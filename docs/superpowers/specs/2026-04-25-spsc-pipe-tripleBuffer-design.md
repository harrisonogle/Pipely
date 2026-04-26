# SPSC Pipe — Design (TripleBuffer-based)

**Date:** 2026-04-25
**Status:** Design (pre-implementation) — revised post-review

## Top-level key takeaways

- A lock-free, single-producer single-consumer (SPSC) implementation of `System.IO.Pipelines.PipeReader` / `PipeWriter`, sharing all cross-thread state through two `TripleBuffer<T>` instances rather than a shared mutable linked list.
- Each side publishes a self-consistent monotonic snapshot of its own state (counters + segment cursor + completion); each side reads the other's latest snapshot opportunistically. Snapshot loss is safe because everything is monotonic.
- The writer is the sole mutator of segments and the chain. Reader holds references but never mutates. Recycling is gated on the reader's published `HeadSegment`, not byte counters, eliminating a class of "still-referenced" bugs.
- The awaiter is a single packed `int` per direction. Every state transition is `Interlocked.CompareExchange` or `Interlocked.Or`. Result construction is **stash-and-construct**: the reader stashes its cursor at park time; the signaler combines the stash with the writer's just-published state to construct the `ReadResult` and pass it to `_core.SetResult`. No thread-affinity constraint on continuations.
- Cancel-from-any-thread (`CancelPending*`) is a sticky bit on the awaiter, naturally coalescing per BCL semantics.
- BCL surface compatibility: `ReadAsync` / `TryRead` / `FlushAsync` / `Advance` / `AdvanceTo` / `Complete` / `CancelPending*`. **Documented divergences from BCL `Pipe`:**
  - No `Reset` (single-use lifecycle; pipe is created → used → both sides `Complete` → `Dispose`).
  - Double-`Complete` on a side coalesces (BCL throws).
  - `IDisposable` added (BCL `Pipe` doesn't implement it; required because there's no `Reset`).
  - `AdvanceTo` argument validation is monotonicity-only — `consumed`/`examined` can't go backwards and `consumed ≤ examined`, but we don't check against the previous `ReadResult.Buffer`'s upper bound (BCL does). Saves a reader-private field; user error becomes silent misbehavior rather than a thrown exception.

## Section 1 — Architecture overview

### Components

- **`SpscPipe`** — owns:
  - `_writerTb : TripleBuffer<WriterState>` — written by writer thread, read by reader thread.
  - `_readerTb : TripleBuffer<ReaderState>` — written by reader thread, read by writer thread.
  - `_readAwaiter : SpscAwaiter<ReadResult>` — woken by writer; awaited by reader.
  - `_flushAwaiter : SpscAwaiter<FlushResult>` — woken by reader; awaited by writer.
  - `_options : SpscPipeOptions` — see Section 6.
- **`SpscPipe.Writer : PipeWriter`** — single producer thread. Owns the segment chain (`_chainHead` → `_writingHead`), a private `BufferSegment` freelist, the `MemoryPool<byte>` reference, and writer-local cursors.
- **`SpscPipe.Reader : PipeReader`** — single consumer thread. Owns reader-local cursors and the most recently acquired `WriterState`.
- **`SpscAwaiter<T>`** — wraps `ManualResetValueTaskSourceCore<T>` with a single packed `int` state field plus a `ParkStash` for `SpscAwaiter<ReadResult>`. Detail in Section 5.

### Local cursors (private to each side)

**Writer-side (touched only by the writer thread):**
- `_chainHead : BufferSegment?` — first segment in the live chain.
- `_writingHead : BufferSegment?` — current tail segment being filled.
- `_writingHeadBytesBuffered : int` — bytes written into `_writingHead` (not yet necessarily published).
- `_totalWritten : long` — monotonic byte counter.
- `_lastPublishedWriterState : WriterState` — read by the reader's signaler under Pattern 2 (Section 5) to construct the `ReadResult`.
- `_lastAcquiredReaderState : ReaderState` — most recent acquire from `_readerTb`.
- `_writerCompleted : bool` — sticky after `Complete`.

**Reader-side (touched only by the reader thread):**
- `_readHead : BufferSegment?`, `_readHeadIdx : int` — head of unconsumed data.
- `_readTail : BufferSegment?`, `_readTailIdx : int` — boundary of the most recently acquired publish; stable until next `TryAcquire`.
- `_totalConsumed : long`, `_totalExamined : long` — monotonic.
- `_lastPublishedReaderState : ReaderState` — read by the writer's signaler under Pattern 2 to construct the `FlushResult` (carries `IsCompleted`/`CompletionException` only; no buffer).
- `_lastAcquiredWriterState : WriterState`.
- `_readerCompleted : bool` — sticky after `Complete`.

### Threading contract (strict SPSC)

- All `Writer` calls (`GetMemory`, `GetSpan`, `Advance`, `FlushAsync`, `Complete`) on a single producer thread.
- All `Reader` calls (`ReadAsync`, `TryRead`, `AdvanceTo`, `Complete`) on a single consumer thread.
- `CancelPendingRead` / `CancelPendingFlush` are explicitly thread-safe (callable from any thread, including the writer or reader thread itself).
- The two threads communicate exclusively via the two `TripleBuffer`s and the two `SpscAwaiter` state machines. No locks. No shared mutable structures outside those two primitives.

### Data flow per cycle (steady state)

1. Writer fills tail segment locally (no cross-thread visibility).
2. `FlushAsync`: builds a `WriterState`, `_writerTb.Publish()`, signals `_readAwaiter`, then `_readerTb.TryAcquire()` for fresh reader state, recycles drained segments, returns or parks on backpressure.
3. `ReadAsync`: `_writerTb.TryAcquire()`, integrates fresh `WriterState` into local cursors, returns synchronously if there's progress; else parks.
4. `AdvanceTo`: updates local cursors, builds `ReaderState`, `_readerTb.Publish()`, signals `_flushAwaiter` **unconditionally** (writer re-checks resume condition on wake; signal-side gating is a future optimization).

### Key takeaways for Section 1

- Two TripleBuffers carry all cross-thread state; two awaiters carry all cross-thread wakeups. That's the entire surface area for synchronization.
- Each side has private local cursors so it can build buffers and decisions without re-reading the TBs every operation. `_lastPublished*State` is read by the *other* side's signaler under Pattern 2.
- The "writer is sole mutator of chain/segments" rule eliminates the class of bugs that drove the prior iteration's restart.

## Section 2 — State shapes

### `WriterState` — published by the writer thread, acquired by the reader

```csharp
internal struct WriterState
{
    public BufferSegment? HeadSegment;     // writer's _chainHead; reader uses this for first-acquire bootstrap only (I10)
    public BufferSegment? TailSegment;     // segment containing the published tail
    public int            TailWritten;     // bytes filled in TailSegment up to the published boundary
    public long           TotalWritten;    // monotonic byte counter across the whole chain
    public bool           IsCompleted;     // sticky once set
    public Exception?     CompletionException;  // set iff IsCompleted with an exception; never changes once set
}
```

### `ReaderState` — published by the reader thread, acquired by the writer

```csharp
internal struct ReaderState
{
    public BufferSegment? HeadSegment;     // first segment still alive on the reader's view; null = pre-bootstrap
    public long           TotalConsumed;   // monotonic — backpressure denominator
    public long           TotalExamined;   // monotonic — flush-awaiter wake gate
    public bool           IsCompleted;     // sticky once set
    public Exception?     CompletionException;
}
```

`HeadConsumed` was deliberately omitted: it would be derivable as `TotalConsumed - HeadSegment.RunningIndex` and is never read by the writer (recycling uses `HeadSegment` reference, backpressure uses `TotalConsumed`). Removing it keeps the snapshot compact.

### Default state semantics

`default(WriterState)` and `default(ReaderState)` represent "nothing yet, not completed" — correct for the pre-first-publish case where `TryAcquire` returns false and the receiving side uses `default` as its `_lastAcquired*` cache.

### Sticky-once-set wording

The `IsCompleted` field on both sides is **sticky once set**: it is `false` initially, transitions to `true` exactly once (when `Complete` is called), and never transitions back. `CompletionException` is set together with `IsCompleted` (iff `IsCompleted = true`) and never changes thereafter. The phrase "monotonic" or "non-decreasing" applied to a `bool` field means exactly this — false-then-true exactly once.

### Cancellation is *not* in either snapshot

`CancelPending*` are documented as thread-safe (callable from any thread), so they can't be folded into the strictly-single-writer/single-reader TBs without extra synchronization. Each awaiter carries its own atomic cancel flag instead — see Section 5.

### Head==tail discipline (the load-bearing invariant for safety)

When the chain has one segment (`_chainHead == _writingHead`), the reader's `_readHead` and `_readTail` both point at that segment while the writer is concurrently appending bytes into it. Three rules keep this safe:

**Reader's tail-bound discipline (I3).** The reader uses its locally cached `_readTailIdx` (set from `WriterState.TailWritten` at `TryAcquire` time) as the ROS endIndex. It *never* reads `BufferSegment.End` or `BufferSegment.Memory.Length` to determine how far the published data extends.

**Writer's mutation discipline on the live tail (I4).** While `S` is the active tail, the writer only appends bytes into `S.AvailableMemory[TailWritten..]`. It does not mutate `S.End`, `S.Memory`, or `S.Next` until the freeze step at segment transition.

**Freeze step (transitioning from `S` to a new tail `S'`).**

1. Rent and initialize `S'` (set `RunningIndex`, `AvailableMemory`).
2. Set `S.End = bytes_filled_in_S`.
3. Set `S.Memory = S.AvailableMemory.Slice(0, S.End)`.
4. Set `S.Next = S'`.
5. Begin writing into `S'`.
6. `Publish` `W' = {TailSegment: S', TailWritten: bytes_in_S', ...}`.

Steps 2–4 are unordered; only the position of *all of them* before step 6 matters. Step 6's `Interlocked.Exchange` is a full barrier, propagating all freeze writes to readers that subsequently `TryAcquire` (I5, I9).

**Benign-torn-read note on `S.Memory`.** During step 3, a concurrent reader iterating an ROS bounded at `_readTailIdx` may observe a torn `Memory<byte>` struct. Only `_length` changes (`_object` and `_index` are stable per segment because `Slice(0, n)` preserves them); both pre-freeze (`capacity`) and post-freeze (`S.End`) values are ≥ `_readTailIdx`, so the resulting slice `[0.._readTailIdx]` is valid in either case. This relies on `Memory<T>`'s internal layout — see Section 3 note (Nit-5).

### Key takeaways for Section 2

- Each snapshot is self-contained and monotonic; intermediate snapshot loss is safe.
- `HeadSegment` field is needed only for first-acquire bootstrap; subsequent acquires the reader uses local cursors.
- `HeadConsumed` is intentionally absent from `ReaderState` (derivable; never read).
- The reader's tail-bound is `_readTailIdx`, not `BufferSegment.End`. This single discipline eliminates the head==tail race that bit prior implementations.
- All freeze writes are ordered before the publish; the publish's full-barrier semantics propagate them to the reader.

## Section 3 — Segment ownership, freelist, BufferSegment, reader bootstrap

### Ownership

| Resource | Owner | Other side's access |
|---|---|---|
| Linked list (`Next` pointers) | Writer | Reader walks but never mutates |
| `BufferSegment` objects | Writer (allocated via freelist) | Reader holds references via `_readHead`, `_readTail`, and acquired `WriterState`s |
| `IMemoryOwner<byte>` per segment | Writer (rents from `_options.Pool`) | None directly; reader sees buffer via `BufferSegment.Memory` |
| Freelist of recyclable segments | Writer-private | None |
| `_chainHead` / `_writingHead` | Writer-private | None |
| `_readHead` / `_readTail` cursors | Reader-private | None |

### `BufferSegment` definition

```csharp
internal sealed class BufferSegment : ReadOnlySequenceSegment<byte>
{
    private IMemoryOwner<byte>? _memoryOwner;

    public Memory<byte>      AvailableMemory { get; private set; }   // set on rent, immutable for segment's lifetime
    public int               End             { get; private set; }   // 0 while segment is the active tail; set on freeze
    public new BufferSegment? Next           { get; private set; }   // null while active tail; set on freeze; reused for freelist link

    // RunningIndex (inherited): set on rent, immutable thereafter
    // base.Memory  (inherited): set on rent (= AvailableMemory) and on freeze (= AvailableMemory.Slice(0, End))

    public void RentFrom(MemoryPool<byte> pool, int sizeHint, long runningIndex)
    {
        _memoryOwner      = pool.Rent(sizeHint);
        AvailableMemory   = _memoryOwner.Memory;
        base.Memory       = AvailableMemory;
        base.RunningIndex = runningIndex;
        End               = 0;
        Next              = null;
        base.Next         = null;
    }

    public void Freeze(int bytesFilled, BufferSegment? next)
    {
        End         = bytesFilled;
        base.Memory = AvailableMemory.Slice(0, bytesFilled);
        Next        = next;
        base.Next   = next;
    }

    public void RecycleReset(long runningIndex)
    {
        base.RunningIndex = runningIndex;
        base.Memory       = AvailableMemory;        // restore to full slice
        End               = 0;
        Next              = null;
        base.Next         = null;
    }

    public void DisposeOwned()
    {
        _memoryOwner?.Dispose();
        _memoryOwner    = null;
        AvailableMemory = default;
    }
}
```

**Note on `Next` semantics.** `BufferSegment.Next` is overloaded to serve both the live chain (when the segment is in `_chainHead..._writingHead`) and the writer-private freelist (when sitting on the freelist). Its semantics are well-defined only conditional on which list the segment is currently in. Recycling clears `Next` (`RecycleReset`); freelist push sets `Next` to the freelist's previous head; allocation pop reads it; `Freeze` sets `Next` to the new tail.

### Allocation path (writer rents a new tail)

1. Pop from freelist if non-empty *and* the popped segment's `AvailableMemory.Length ≥ sizeHint`.
2. **Freelist behavior on size mismatch (N5).** If the popped segment is too small, dispose it (`DisposeOwned`) and allocate fresh. Avoids stranding small segments at the head; simpler than peek-and-pop-conditionally.
3. Fresh: `new BufferSegment().RentFrom(_options.Pool, max(sizeHint, _options.MinimumSegmentSize), runningIndex)`.
4. Reused: `segment.RecycleReset(runningIndex)`.
5. `runningIndex = _writingHead == null ? 0 : _writingHead.RunningIndex + _writingHead.End` (requires the existing tail to be frozen first).
6. Wire into chain: freeze old tail with `next = newTail`. Update `_writingHead = newTail`.

**Freelist size cap (N4).** The freelist is bounded at `_options.MaxFreelistSegments` (default 256, matching BCL's `Pipe` segment-pool default). When `RecycleDrainedSegments` would push to a full freelist, it calls `DisposeOwned()` on the excess segment instead.

**Note on `MemoryPool<byte>.Rent`.** `pool.Rent(sizeHint)` returns a buffer of length **at least** `sizeHint`, not exactly `sizeHint`. Tests, freelist size comparisons, and any code reasoning about rented capacity must use `>=`, never `==`.

### Recycling path

Called at the end of `FlushAsync`, after the writer has refreshed `_lastAcquiredReaderState` via `_readerTb.TryAcquire()`:

```csharp
void RecycleDrainedSegments()
{
    var readerHead = _lastAcquiredReaderState.HeadSegment;
    if (readerHead is null) return;             // reader hasn't bootstrapped (I10)

    while (_chainHead != _writingHead && _chainHead != readerHead)
    {
        var recycled = _chainHead;
        _chainHead   = _chainHead.Next!;

        if (_freelistCount < _options.MaxFreelistSegments)
        {
            PushFreelist(recycled);             // I6
            _freelistCount++;
        }
        else
        {
            recycled.DisposeOwned();            // freelist full; release back to pool
        }
    }
}
```

The predicate uses `HeadSegment` reference comparison rather than byte arithmetic.

**Why reference comparison vs byte arithmetic (Nit-4).** The byte-offset form `_lastAcquiredReaderState.TotalConsumed >= _chainHead.RunningIndex + _chainHead.End` has an off-by-one at the boundary `HeadConsumed == End`: a reader at `(S, S.End)` (end of segment but not yet advanced to `S.Next`, because reader hasn't acquired a state with `TailSegment > S` yet) has `TotalConsumed == S.RunningIndex + S.End`, which the byte predicate (with `>=`) would fire on — but `S` is still the reader's `_readHead`. Reference comparison naturally distinguishes "reader-still-on-this-segment" from "reader-moved-off."

### TripleBuffer slot retention pins recycled segments slightly (Nit-6)

After a segment is recycled to the freelist, it is still reachable via the unused TripleBuffer slot of `_writerTb` until the next publish overwrites that slot's `WriterState.HeadSegment` or `TailSegment` reference. Benign — the recycled segment is alive on the freelist anyway — but means the segment's `IMemoryOwner` is pinned for slightly longer than strictly necessary. Not worth additional complexity to address.

### `MemoryPool` integration and segment sizing

- `_options.Pool` (default `MemoryPool<byte>.Shared`) supplies `IMemoryOwner<byte>`s.
- `_options.MinimumSegmentSize` (default 4096) is the floor for `Pool.Rent(sizeHint)` calls.
- Upper bound on segment size is governed by the underlying `MemoryPool<byte>.MaxBufferSize`; we don't impose a separate cap.
- `GetMemory(sizeHint)`: if `sizeHint > 0` and the current tail can't satisfy it, transition to a new tail of size `Max(sizeHint, MinimumSegmentSize)`. Otherwise return remaining capacity in the current tail.

### `Memory<T>` benign-torn-read assumption (Nit-5)

The Section 2 head==tail discipline relies on the layout of `Memory<byte>`'s internal fields (`_object`, `_index`, `_length`) and on `Slice(0, n)` preserving `_object` and `_index`. This is true today but is an implementation detail of the BCL. **Implementation should add a startup-time assertion or boot test** verifying the assumption (e.g., that `pool.Rent(1024).Memory.Slice(0, 100)` has the same `_object` reference and `_index` value as the original) so a future BCL change is caught at boot rather than as a heisenbug.

### Reader bootstrap

The reader's `_readHead` starts as `null`. On the first successful `_writerTb.TryAcquire()`:

```csharp
_readHead    = acquired.HeadSegment;
_readHeadIdx = 0;
_readTail    = acquired.TailSegment;
_readTailIdx = acquired.TailWritten;
```

After bootstrap, the reader ignores `WriterState.HeadSegment` on subsequent acquires — its local `_readHead` is authoritative.

**Why bootstrap is safe (I10).** The recycle predicate (I6) cannot fire until `_lastAcquiredReaderState.HeadSegment != null`, which requires the reader to have bootstrapped and published. So the writer's `_chainHead` does not advance until *after* the reader's first successful read — at the moment of bootstrap, `WriterState.HeadSegment == _chainHead == start of the live chain`.

Edge cases:
- **Writer completes without writing.** `WriterState{HeadSegment: null, TailSegment: null, IsCompleted: true}`. Reader's first `TryAcquire` gets this; `_readHead` stays null. `ReadResult` is empty with `IsCompleted: true`.
- **Writer publishes first via `FlushAsync`.** `FlushAsync` always publishes (per Section 4), even if `_writingHeadBytesBuffered == 0`. Harmless; reader gets a `WriterState` with `TailWritten = 0` but valid `HeadSegment` once data has been written.

### Lifecycle: cleanup on Dispose

`SpscPipe.Dispose()` walks the chain and the freelist, calling `BufferSegment.DisposeOwned()` on each to release `IMemoryOwner` rentals.

**Precondition:** no operation is currently in flight on either side, **and no `ReadResult.Buffer` references are still held by the user**. The buffer references segments whose `IMemoryOwner` will be released; accessing them after `Dispose` is use-after-free. This is a strengthening of "no in-flight ops" — the user must have called `AdvanceTo` past every `ReadResult.Buffer` they received before `Dispose`. After `Dispose`, the pipe is unusable.

`SpscPipe` implements `IDisposable`; `BCL.Pipe` does not. This is a documented divergence (justified because we don't expose `Reset` and segments need explicit memory release).

### Key takeaways for Section 3

- Writer is the sole mutator of segments and the chain; reader walks but doesn't mutate.
- Recycling reuses both the `BufferSegment` object and its `IMemoryOwner` — only `Dispose()` (or freelist overflow) releases memory back to the pool.
- The recycle predicate is a single reference comparison: `_chainHead != ReaderState.HeadSegment`.
- Reader bootstrap rides on `WriterState.HeadSegment`, used only on first acquire.
- `BufferSegment.Next` is overloaded across chain and freelist; semantics are well-defined only conditional on which list the segment is in.

## Section 4 — Hot paths (steady-state pseudocode)

This section assumes the happy path: no parking, no cancellation. Section 5 layers awaiter parking and cancellation on top. Each method's entry guard and post-Complete throw paths are shown.

### Writer: `GetMemory(int sizeHint)`

```csharp
Memory<byte> GetMemory(int sizeHint = 0)
{
    if (_writerCompleted) throw new InvalidOperationException("Writing is completed.");    // S8
    if (sizeHint < 0) throw new ArgumentOutOfRangeException(nameof(sizeHint));
    if (sizeHint == 0) sizeHint = 1;

    if (_writingHead == null)
    {
        _writingHead = RentSegment(sizeHint, runningIndex: 0);
        _chainHead   = _writingHead;
    }
    else
    {
        int remaining = _writingHead.AvailableMemory.Length - _writingHeadBytesBuffered;
        if (remaining < sizeHint)
        {
            int filled  = _writingHeadBytesBuffered;
            long newRI  = _writingHead.RunningIndex + filled;
            var newTail = RentSegment(Max(sizeHint, _options.MinimumSegmentSize), newRI);

            _writingHead.Freeze(filled, newTail);          // I5
            _writingHead             = newTail;
            _writingHeadBytesBuffered = 0;
        }
    }

    return _writingHead.AvailableMemory.Slice(_writingHeadBytesBuffered);
}
```

Purely writer-local (I2). No TB interaction.

### Writer: `GetSpan(int sizeHint)` (N1)

```csharp
Span<byte> GetSpan(int sizeHint = 0) => GetMemory(sizeHint).Span;
```

### Writer: `Advance(int bytes)`

```csharp
void Advance(int bytes)
{
    if (_writerCompleted) throw new InvalidOperationException("Writing is completed.");    // S8
    Debug.Assert(_writingHead != null);
    Debug.Assert(_writingHeadBytesBuffered + bytes <= _writingHead.AvailableMemory.Length);
    _writingHeadBytesBuffered += bytes;
    _totalWritten             += bytes;
}
```

Local only (I4). `_writingHead.End` and `.Memory` are NOT updated; `TailWritten` in the published `WriterState` carries the figure across.

### Writer: `FlushAsync(CancellationToken ct)` (synchronous fast path)

```csharp
ValueTask<FlushResult> FlushAsync(CancellationToken ct)
{
    if (_writerCompleted) throw new InvalidOperationException("Writing is completed.");    // S8

    var snapshot = new WriterState
    {
        HeadSegment        = _chainHead,
        TailSegment        = _writingHead,
        TailWritten        = _writingHeadBytesBuffered,
        TotalWritten       = _totalWritten,
        IsCompleted        = false,
        CompletionException = null,
    };
    _writerTb.ProducerSlot() = snapshot;
    _writerTb.Publish();                              // R1; I9 release fence
    _lastPublishedWriterState = snapshot;

    SignalReadAwaiterIfPending();                     // R1

    if (_readerTb.TryAcquire())                       // I9 acquire fence
        _lastAcquiredReaderState = _readerTb.ConsumerSlot();
    RecycleDrainedSegments();                         // I6

    // M2 + M4 (Option A): if reader completed with exception, every FlushAsync throws.
    if (_lastAcquiredReaderState.IsCompleted && _lastAcquiredReaderState.CompletionException != null)
        ExceptionDispatchInfo.Throw(_lastAcquiredReaderState.CompletionException);

    long unconsumed = _totalWritten - _lastAcquiredReaderState.TotalConsumed;
    bool readerDone = _lastAcquiredReaderState.IsCompleted;
    bool needsPark = _options.PauseWriterThreshold > 0
                     && unconsumed >= _options.PauseWriterThreshold     // S1: BCL-match >=
                     && !readerDone;                                    // M2

    if (!needsPark)
        return new ValueTask<FlushResult>(BuildFlushResult(isCanceled: false));    // M1

    return ParkFlushAwaiter(ct);                       // see Section 5
}

FlushResult BuildFlushResult(bool isCanceled)
{
    return new FlushResult(isCanceled, isCompleted: _lastAcquiredReaderState.IsCompleted);
}
```

The mid-method `_readerTb.TryAcquire()` is the documented exception to "TryAcquire-once-per-method": acquiring after the publish gives the freshest reader state for both recycling and the backpressure decision.

### Reader: `ReadAsync(CancellationToken ct)` (synchronous fast path)

```csharp
ValueTask<ReadResult> ReadAsync(CancellationToken ct)
{
    if (_readerCompleted) throw new InvalidOperationException("Reading is completed.");    // S8

    // Sync entry: consume sticky cancel flag if present. (See Section 5.)
    while (true)
    {
        int oldV = _readAwaiter._state;
        if ((oldV & SpscAwaiter.CancelFlag) == 0) break;
        int desired = oldV & ~SpscAwaiter.CancelFlag;
        if (Interlocked.CompareExchange(ref _readAwaiter._state, desired, oldV) == oldV)
            return new ValueTask<ReadResult>(BuildReadResult(isCanceled: true));
    }

    if (ct.IsCancellationRequested)
        return ValueTask.FromCanceled<ReadResult>(ct);

    if (_writerTb.TryAcquire())                       // I9 acquire fence
    {
        _lastAcquiredWriterState = _writerTb.ConsumerSlot();
        IntegrateAcquiredWriterState();
    }

    // M3 + M4 (Option A, BCL-strict): every ReadAsync after Writer.Complete(ex) throws.
    if (_lastAcquiredWriterState.IsCompleted && _lastAcquiredWriterState.CompletionException != null)
        ExceptionDispatchInfo.Throw(_lastAcquiredWriterState.CompletionException);

    if (HasReadableProgress() || _lastAcquiredWriterState.IsCompleted)
        return new ValueTask<ReadResult>(BuildReadResult(isCanceled: false));

    return ParkReadAwaiter(ct);                       // see Section 5
}

bool HasReadableProgress() => _lastAcquiredWriterState.TotalWritten > _totalExamined;     // M7

void IntegrateAcquiredWriterState()
{
    var w = _lastAcquiredWriterState;

    if (_readHead == null)                            // I10 bootstrap
    {
        _readHead    = w.HeadSegment;
        _readHeadIdx = 0;
    }

    _readTail    = w.TailSegment;                     // I3 source of truth
    _readTailIdx = w.TailWritten;
}
```

**Precedence at sync entry:** sticky cancel first (consumes flag, surfaces `IsCanceled = true` exactly once per I12), then `ct.IsCancellationRequested` (throws OCE), then writer-completion exception (throws — matches BCL's `IsCompletedOrThrow`), then normal data/IsCompleted path.

### Reader: `TryRead(out ReadResult result)` (S7)

```csharp
bool TryRead(out ReadResult result)
{
    if (_readerCompleted) throw new InvalidOperationException("Reading is completed.");    // S8

    // Sync entry: consume sticky cancel flag if present.
    while (true)
    {
        int oldV = _readAwaiter._state;
        if ((oldV & SpscAwaiter.CancelFlag) == 0) break;
        int desired = oldV & ~SpscAwaiter.CancelFlag;
        if (Interlocked.CompareExchange(ref _readAwaiter._state, desired, oldV) == oldV)
        {
            result = BuildReadResult(isCanceled: true);
            return true;
        }
    }

    if (_writerTb.TryAcquire())
    {
        _lastAcquiredWriterState = _writerTb.ConsumerSlot();
        IntegrateAcquiredWriterState();
    }

    if (_lastAcquiredWriterState.IsCompleted && _lastAcquiredWriterState.CompletionException != null)
        ExceptionDispatchInfo.Throw(_lastAcquiredWriterState.CompletionException);

    if (HasReadableProgress() || _lastAcquiredWriterState.IsCompleted)
    {
        result = BuildReadResult(isCanceled: false);
        return true;
    }

    result = default;
    return false;
}
```

### Reader: `AdvanceTo(SequencePosition consumed)` (single-arg overload, N2)

```csharp
void AdvanceTo(SequencePosition consumed) => AdvanceTo(consumed, consumed);
```

### Reader: `AdvanceTo(SequencePosition consumed, SequencePosition examined)`

```csharp
void AdvanceTo(SequencePosition consumed, SequencePosition examined)
{
    if (_readerCompleted) throw new InvalidOperationException("Reading is completed.");    // S8

    var consumedSeg = consumed.GetObject() as BufferSegment;
    var examinedSeg = examined.GetObject() as BufferSegment;

    // N6: empty-buffer fast path (e.g., AdvanceTo on Empty ROS after IsCompleted).
    if (consumedSeg == null && examinedSeg == null)
    {
        PublishReaderState();
        return;
    }
    if (consumedSeg == null || examinedSeg == null)
        throw new InvalidOperationException("AdvanceTo: mixed null/non-null SequencePositions");

    int consumedIdx = consumed.GetInteger();
    int examinedIdx = examined.GetInteger();

    long consumedAbs = consumedSeg.RunningIndex + consumedIdx;       // O(1) via I2, I9
    long examinedAbs = examinedSeg.RunningIndex + examinedIdx;

    // R8: monotonicity validation. (Upper-bound check is not done — see Top-level takeaways re: BCL divergence.)
    if (consumedAbs < _totalConsumed
        || examinedAbs < _totalExamined
        || consumedAbs > examinedAbs)
    {
        throw new InvalidOperationException("AdvanceTo position out of range");
    }

    _readHead       = consumedSeg;
    _readHeadIdx    = consumedIdx;
    _totalConsumed  = consumedAbs;
    _totalExamined  = examinedAbs;

    PublishReaderState();
}

void PublishReaderState()
{
    var snapshot = new ReaderState
    {
        HeadSegment   = _readHead,
        TotalConsumed = _totalConsumed,
        TotalExamined = _totalExamined,
        IsCompleted   = false,
        CompletionException = null,
    };
    _readerTb.ProducerSlot() = snapshot;
    _readerTb.Publish();                              // R1
    _lastPublishedReaderState = snapshot;

    SignalFlushAwaiterIfPending();                    // R1; unconditional per S2
}
```

### Reader: `BuildReadResult` (R9 — both flags can be set)

```csharp
ReadResult BuildReadResult(bool isCanceled)
{
    bool isCompleted = _lastAcquiredWriterState.IsCompleted;
    var buffer = isCanceled
        ? ReadOnlySequence<byte>.Empty
        : (_readHead == null
            ? ReadOnlySequence<byte>.Empty
            : new ReadOnlySequence<byte>(_readHead, _readHeadIdx, _readTail!, _readTailIdx));   // I3, I7
    return new ReadResult(buffer, isCanceled, isCompleted);
}
```

`isCompleted` reflects writer-completion alone — there is no `_totalExamined >= TotalWritten` gate. Under Option A (M4), the writer-completion-with-exception case is intercepted earlier (the sync-entry throw in `ReadAsync`/`TryRead`) and never reaches `BuildReadResult`. For `Writer.Complete(null)`, the reader sees `IsCompleted = true` from the moment it acquires the completed `WriterState` — buffer may still be non-empty (drain proceeds normally for non-faulted completion).

### Conventions

- **TryAcquire-once-per-method default.** Each method does at most one `TryAcquire` of each TripleBuffer, ideally near the top. Documented exception: `FlushAsync` acquires `_readerTb` mid-method (after publish) for tighter backpressure responsiveness.
- **R1 (publish-before-signal).** Any code path that both publishes a state and signals the other side's awaiter publishes first, signals second.
- **R8 (AdvanceTo validation).** Monotonicity-only: `consumed`/`examined` must not go backwards; `consumed ≤ examined`.

### Key takeaways for Section 4

- `GetMemory` / `Advance` are purely writer-local — no fences, no TB ops. All visibility happens at `FlushAsync`.
- `FlushAsync` ordering: publish → signal → acquire → recycle → completion check → backpressure check.
- `ReadAsync` ordering: sync-cancel → `ct` check → acquire → integrate (bootstrap on first call) → completion-exception check → return-or-park.
- `AdvanceTo` is O(1) via `RunningIndex`; always publishes (cheap; keeps writer's view of progress fresh).
- Every public method has an entry guard for its side's `_*Completed` flag.
- Reader's tail-bound (`_readTailIdx`) is set only when integrating a freshly-acquired `WriterState` — the single point where I3 is established for each subsequent `ReadResult`.

## Section 5 — Awaiter coordination

### Awaiter shape (single packed `int` + Pattern 2 stash)

```csharp
internal sealed class SpscAwaiter<T> : IValueTaskSource<T>
{
    public ManualResetValueTaskSourceCore<T> _core;
    public int _state;                          // packed: bit 0 = state, bit 1 = cancel flag
    public CancellationTokenRegistration _ctr;
    public CancellationToken _token;            // cached for OCE construction; set at park, read by token callback

    // Stash for Pattern 2 (used by SpscAwaiter<ReadResult> only; SpscAwaiter<FlushResult> ignores).
    // Set by reader at park time, read by signaler after CASing out of Pending.
    public BufferSegment? _stashHead;
    public int _stashHeadIdx;

    public const int Inactive   = 0b00;
    public const int Pending    = 0b01;
    public const int StateMask  = 0b01;
    public const int CancelFlag = 0b10;

    public short Version => _core.Version;
    public T GetResult(short token) => _core.GetResult(token);
    public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);
    public void OnCompleted(Action<object?> c, object? s, short token, ValueTaskSourceOnCompletedFlags f)
        => _core.OnCompleted(c, s, token, f);
}
```

Two states (`Inactive` / `Pending`) suffice; "how was this completed" lives in `_core`'s status. Every `_state` mutation is `Interlocked.CompareExchange` or `Interlocked.Or`.

**Field-access discipline.** `_state` is the only field touched by all four actors (owner / signaler / canceler / token callback). `_core`, `_ctr`, `_token`, and the stash fields are accessed under these orderings:
- `_token` is written by the owner before CAS Inactive→Pending; read by the token callback after CAS Pending→Inactive succeeds. Synchronization rides on `_state`'s CAS. (Field reuse across cycles is safe because `CancellationTokenRegistration.Dispose()` blocks on in-flight callbacks, so the prior cycle's callback has finished before the next cycle's `_token = ct` write — see N9.)
- `_ctr` is written by the owner after CAS Inactive→Pending; read by the signaler/canceler/token callback after CAS Pending→Inactive succeeds. Per R5, the owner re-checks `_state` after registering and disposes if the awaiter is no longer Pending.
- Stash fields (`_stashHead`, `_stashHeadIdx`) are written by the owner before CAS Inactive→Pending; read by the signaler after CAS Pending→Inactive succeeds. Same release/acquire pattern as `_token`.

**On Pattern 2 (stash-and-construct).** The signaler runs on the writer thread; constructing a `ReadResult` requires reader-private cursors (`_readHead`, `_readHeadIdx`) per I3. To avoid Pattern 1's thread-affinity constraint (which forces continuations to run on the reader thread, broken by typical `ConfigureAwait(false)` and missing-`SynchronizationContext` cases), the reader stashes its cursor at park time. The signaler combines the stash with `_lastPublishedWriterState` (writer-private, freshest just before signaling) to construct the `ReadResult` and pass it to `_core.SetResult`. No thread-affinity constraint on the continuation.

**Implementation note on `RunContinuationsAsynchronously` (N3).** Set `_core.RunContinuationsAsynchronously = true`. Avoids inline continuation execution on the signaler thread (which can lead to unbounded stack depth and reentrancy hazards under bursty workloads). Pattern 2 makes this choice cleanly orthogonal to correctness (Pattern 1 entangles it).

### Reader's park / wake

```csharp
ValueTask<ReadResult> ParkReadAwaiter(CancellationToken ct)
{
    _readAwaiter._core.Reset();
    _readAwaiter._token = ct;

    // Pattern 2 stash: written before the CAS to Pending; signaler reads after CAS Pending→Inactive.
    _readAwaiter._stashHead    = _readHead;
    _readAwaiter._stashHeadIdx = _readHeadIdx;

    // 1. CAS Inactive → Pending, preserving any flag set concurrently.
    while (true)
    {
        int oldV = _readAwaiter._state;
        Debug.Assert((oldV & StateMask) == Inactive, "SPSC violation: concurrent ReadAsync");
        int desired = (oldV & CancelFlag) | Pending;
        if (Interlocked.CompareExchange(ref _readAwaiter._state, desired, oldV) == oldV)
            break;
    }

    // 2. Lost-cancel re-check (R4).
    int v = _readAwaiter._state;
    if ((v & CancelFlag) != 0
        && Interlocked.CompareExchange(ref _readAwaiter._state, Inactive, Pending | CancelFlag) == (Pending | CancelFlag))
    {
        _readAwaiter._core.SetResult(BuildReadResult(isCanceled: true));
        return new ValueTask<ReadResult>(_readAwaiter, _readAwaiter.Version);
    }

    // 3. Lost-wakeup re-check.
    if (_writerTb.TryAcquire())
    {
        _lastAcquiredWriterState = _writerTb.ConsumerSlot();
        IntegrateAcquiredWriterState();

        // Re-check throw condition under fresh state.
        if (_lastAcquiredWriterState.IsCompleted && _lastAcquiredWriterState.CompletionException != null)
        {
            // Un-park and throw on the awaiter via SetException.
            while (true)
            {
                int oldV = _readAwaiter._state;
                if ((oldV & StateMask) != Pending) break;
                int desired = oldV & ~StateMask;
                if (Interlocked.CompareExchange(ref _readAwaiter._state, desired, oldV) == oldV)
                {
                    _readAwaiter._core.SetException(_lastAcquiredWriterState.CompletionException);
                    return new ValueTask<ReadResult>(_readAwaiter, _readAwaiter.Version);
                }
            }
        }

        if (HasReadableProgress() || _lastAcquiredWriterState.IsCompleted)
        {
            while (true)
            {
                int oldV = _readAwaiter._state;
                if ((oldV & StateMask) != Pending) break;
                int desired = oldV & ~StateMask;
                if (Interlocked.CompareExchange(ref _readAwaiter._state, desired, oldV) == oldV)
                    return new ValueTask<ReadResult>(BuildReadResult(isCanceled: false));
            }
        }
    }

    // 4. Register token; clean up CTR if someone completed during register (R5).
    _readAwaiter._ctr = ct.UnsafeRegister(static p => ((SpscPipe)p!).OnReadAwaiterTokenCancel(), this);
    if ((_readAwaiter._state & StateMask) != Pending)
        _readAwaiter._ctr.Dispose();
    return new ValueTask<ReadResult>(_readAwaiter, _readAwaiter.Version);
}

void SignalReadAwaiterIfPending()
{
    while (true)
    {
        int oldV = _readAwaiter._state;
        if ((oldV & StateMask) != Pending) return;
        int desired = oldV & ~StateMask;                 // state → Inactive, flag preserved
        if (Interlocked.CompareExchange(ref _readAwaiter._state, desired, oldV) == oldV)
        {
            _readAwaiter._ctr.Dispose();

            // Pattern 2: construct ReadResult from stash + just-published WriterState.
            var w = _lastPublishedWriterState;

            // Under Option A (M4): if writer-completed-with-exception, deliver as exception not result.
            if (w.IsCompleted && w.CompletionException != null)
            {
                _readAwaiter._core.SetException(w.CompletionException);
                return;
            }

            var head    = _readAwaiter._stashHead ?? w.HeadSegment;     // bootstrap fallback
            var headIdx = _readAwaiter._stashHead == null ? 0 : _readAwaiter._stashHeadIdx;

            var buffer = head == null
                ? ReadOnlySequence<byte>.Empty
                : new ReadOnlySequence<byte>(head, headIdx, w.TailSegment!, w.TailWritten);

            var result = new ReadResult(buffer, isCanceled: false, isCompleted: w.IsCompleted);
            _readAwaiter._core.SetResult(result);
            return;
        }
    }
}

void OnReadAwaiterTokenCancel()
{
    while (true)
    {
        int oldV = _readAwaiter._state;
        if ((oldV & StateMask) != Pending) return;
        int desired = oldV & ~StateMask;
        if (Interlocked.CompareExchange(ref _readAwaiter._state, desired, oldV) == oldV)
        {
            _readAwaiter._core.SetException(new OperationCanceledException(_readAwaiter._token));
            return;
        }
    }
}
```

**Note on `_readTail`/`_readTailIdx` after a Pattern-2 signaled delivery.** The reader's local `_readTail`/`_readTailIdx` may briefly lag the buffer the user just received (the buffer was constructed with the writer's just-published `TailSegment`/`TailWritten`, but reader's local fields aren't updated). On the reader's next `ReadAsync`/`TryRead`, the entry-side `TryAcquire` refreshes them. AdvanceTo doesn't reference them (it only updates `_readHead`/`_readHeadIdx` and the byte counters). So lag is bounded to "the next reader call" and benign.

### Writer's park / wake

Symmetric to the reader's park/wake but against `_flushAwaiter`. Differences: wake condition is `unconsumed < ResumeWriterThreshold` (BCL hysteresis, S1) OR reader-completed (M2); result construction is simpler (no buffer, just `IsCompleted` and `IsCanceled`); no Pattern-2 stash (FlushResult has no buffer).

```csharp
ValueTask<FlushResult> ParkFlushAwaiter(CancellationToken ct)
{
    _flushAwaiter._core.Reset();
    _flushAwaiter._token = ct;

    // 1. CAS Inactive → Pending, preserving flag.
    while (true)
    {
        int oldV = _flushAwaiter._state;
        Debug.Assert((oldV & StateMask) == Inactive, "SPSC violation: concurrent FlushAsync");
        int desired = (oldV & CancelFlag) | Pending;
        if (Interlocked.CompareExchange(ref _flushAwaiter._state, desired, oldV) == oldV)
            break;
    }

    // 2. Lost-cancel re-check.
    int v = _flushAwaiter._state;
    if ((v & CancelFlag) != 0
        && Interlocked.CompareExchange(ref _flushAwaiter._state, Inactive, Pending | CancelFlag) == (Pending | CancelFlag))
    {
        _flushAwaiter._core.SetResult(BuildFlushResult(isCanceled: true));
        return new ValueTask<FlushResult>(_flushAwaiter, _flushAwaiter.Version);
    }

    // 3. Lost-wakeup re-check.
    if (_readerTb.TryAcquire())
    {
        _lastAcquiredReaderState = _readerTb.ConsumerSlot();

        // Throw if reader-completed-with-exception (M4 Option A).
        if (_lastAcquiredReaderState.IsCompleted && _lastAcquiredReaderState.CompletionException != null)
        {
            while (true)
            {
                int oldV = _flushAwaiter._state;
                if ((oldV & StateMask) != Pending) break;
                int desired = oldV & ~StateMask;
                if (Interlocked.CompareExchange(ref _flushAwaiter._state, desired, oldV) == oldV)
                {
                    _flushAwaiter._core.SetException(_lastAcquiredReaderState.CompletionException);
                    return new ValueTask<FlushResult>(_flushAwaiter, _flushAwaiter.Version);
                }
            }
        }

        long unconsumed = _totalWritten - _lastAcquiredReaderState.TotalConsumed;
        bool releasable = unconsumed < _options.ResumeWriterThreshold       // S1: BCL-match <
                          || _lastAcquiredReaderState.IsCompleted;          // M2

        if (releasable)
        {
            while (true)
            {
                int oldV = _flushAwaiter._state;
                if ((oldV & StateMask) != Pending) break;
                int desired = oldV & ~StateMask;
                if (Interlocked.CompareExchange(ref _flushAwaiter._state, desired, oldV) == oldV)
                    return new ValueTask<FlushResult>(BuildFlushResult(isCanceled: false));
            }
        }
    }

    // 4. Register token.
    _flushAwaiter._ctr = ct.UnsafeRegister(static p => ((SpscPipe)p!).OnFlushAwaiterTokenCancel(), this);
    if ((_flushAwaiter._state & StateMask) != Pending)
        _flushAwaiter._ctr.Dispose();
    return new ValueTask<FlushResult>(_flushAwaiter, _flushAwaiter.Version);
}

void SignalFlushAwaiterIfPending()
{
    while (true)
    {
        int oldV = _flushAwaiter._state;
        if ((oldV & StateMask) != Pending) return;
        int desired = oldV & ~StateMask;
        if (Interlocked.CompareExchange(ref _flushAwaiter._state, desired, oldV) == oldV)
        {
            _flushAwaiter._ctr.Dispose();

            var r = _lastPublishedReaderState;

            if (r.IsCompleted && r.CompletionException != null)
            {
                _flushAwaiter._core.SetException(r.CompletionException);
                return;
            }

            var result = new FlushResult(isCanceled: false, isCompleted: r.IsCompleted);
            _flushAwaiter._core.SetResult(result);
            return;
        }
    }
}

void OnFlushAwaiterTokenCancel()
{
    while (true)
    {
        int oldV = _flushAwaiter._state;
        if ((oldV & StateMask) != Pending) return;
        int desired = oldV & ~StateMask;
        if (Interlocked.CompareExchange(ref _flushAwaiter._state, desired, oldV) == oldV)
        {
            _flushAwaiter._core.SetException(new OperationCanceledException(_flushAwaiter._token));
            return;
        }
    }
}
```

### Cross-thread cancel (any thread)

Under M5 (synthesis option (a)): cancel constructs a minimal `ReadResult`/`FlushResult` directly to avoid touching reader/writer-private state from an arbitrary thread.

```csharp
public void CancelPendingRead()
{
    int oldV = Interlocked.Or(ref _readAwaiter._state, CancelFlag);
    if ((oldV & StateMask) == Pending
        && Interlocked.CompareExchange(
               ref _readAwaiter._state,
               Inactive,
               Pending | CancelFlag) == (Pending | CancelFlag))
    {
        _readAwaiter._ctr.Dispose();
        // Construct minimal cancel result directly — no access to reader-private cursors.
        _readAwaiter._core.SetResult(new ReadResult(ReadOnlySequence<byte>.Empty, isCanceled: true, isCompleted: false));
    }
    // CAS-fail (raced with signaler/token): flag remains set; consumed by next ReadAsync.
}

public void CancelPendingFlush()
{
    int oldV = Interlocked.Or(ref _flushAwaiter._state, CancelFlag);
    if ((oldV & StateMask) == Pending
        && Interlocked.CompareExchange(
               ref _flushAwaiter._state,
               Inactive,
               Pending | CancelFlag) == (Pending | CancelFlag))
    {
        _flushAwaiter._ctr.Dispose();
        _flushAwaiter._core.SetResult(new FlushResult(isCanceled: true, isCompleted: false));
    }
}
```

The `isCompleted: false` in the cancel result is conservative — it doesn't reflect potential post-cancel-call completion of the opposite side. Subsequent `ReadAsync`/`FlushAsync` calls will report the correct `IsCompleted` once the user calls them. (Cancel from any thread is an anomalous interruption; users typically call it then check completion separately.)

### Cancellation semantics summary

| Path | Trigger | Observable result |
|---|---|---|
| `CancellationToken` cancel | `ct` fires (sync at entry, or async via registration) | `OperationCanceledException` thrown from the `await` |
| `CancelPendingRead`/`CancelPendingFlush` | Method call from any thread | Result with `IsCanceled = true`, no exception |
| **Writer-completion exception** (after `Writer.Complete(ex)`) | Reader's `ReadAsync`/`TryRead` | Throws `ex` on every call (M4 Option A) |
| **Reader-completion exception** (after `Reader.Complete(ex)`) | Writer's `FlushAsync` | Throws `ex` on every call |

### Outcome × path matrix

|                          | Sync entry | Park re-check | Park: registered & waiting |
|--------------------------|:---:|:---:|:---:|
| Returns `IsCanceled=false` data | ✓ (1) | ✓ (5) — lost-wakeup defense | ✓ (8) — signaled by writer |
| Returns `IsCanceled=true`        | ✓ (2) — sticky flag at entry | ✓ (6) — lost-cancel defense | ✓ (9) — `CancelPending*` while parked |
| Throws `OperationCanceledException` | ✓ (3) — `ct.IsCancellationRequested` at entry | n/a (token not yet registered) | ✓ (10) — token fires while parked |
| Throws **writer-completion exception** | ✓ (3') — sync entry, post-acquire | ✓ (7) — lost-wakeup defense delivered Complete-with-ex | ✓ (8') — signaler delivers via `SetException` |
| Returns `IsCompleted=true` (Complete(null)) | ✓ (4) — writer already completed | (subsumed by 7) | (subsumed by 8) |

12 distinct internal paths produce 5 user-observable outcomes (4 from Section 5 prior + 1 for writer-completion-throw). During the parked phase, three actors race for the CAS out of `Pending` (signaler, canceler, token); whichever commits first wins, others back off silently. (Note: under M2, a parked writer can also be released by reader-completion via the signaler path — same shape.)

### Concrete trace examples

`_state` is shown in binary (`SF` where `S` = state bit, `F` = flag bit). `Inactive` = `00`, `Pending` = `01`, `Inactive+Flag` = `10`, `Pending+Flag` = `11`.

#### Example A — Happy path (path 8)

Initial: `_state = 00`.

| t | Actor | Op | Before | After |
|---|---|---|---|---|
| 1–4 | Reader | sync entry, no flag, no data; stash; CAS `00 → 01` | `00` | `01` |
| 5 | Reader | re-check flag: clear, skip | `01` | `01` |
| 6 | Reader | re-check data: false | `01` | `01` |
| 7 | Reader | register CTR, return parked task | `01` | `01` |
| 8 | Writer | publishes WriterState | `01` | `01` |
| 9 | Writer | `Signal`: CAS `01 → 00`; constructs ReadResult from stash + W; SetResult | `01` | `00` |

#### Example B — Sticky cancel (path 2)

| t | Actor | Op | Before | After |
|---|---|---|---|---|
| 1 | Canceler | `Or(_state, 10)` (oldV `00`, not Pending — no further work) | `00` | `10` |
| 2 | Reader | sync top: CAS `10 → 00` | `10` | `00` — returns canceled sync |

#### Example C — Lost-cancel defense (path 6)

| t | Actor | Op | Before | After |
|---|---|---|---|---|
| 1 | Reader | sync top: read | `00` | `00` |
| 2 | Reader | sync data: false | `00` | `00` |
| 3 | Canceler | `Or(_state, 10)` | `00` | `10` |
| 4 | Reader | CAS `10 → 11` (preserving flag) | `10` | `11` |
| 5 | Reader | re-check flag: CAS `11 → 00` | `11` | `00` — `SetResult(canceled)` into _core |

#### Example D — Three-way race (paths 8/9/10)

Initial: `_state = 01` (parked).

| t | Actor | Op | Before | After | Note |
|---|---|---|---|---|---|
| 1 | Canceler | `Or(_state, 10)` | `01` | `11` | returns `01` — will attempt CAS |
| 2 | Token cb | read `_state` | `11` | `11` | sees Pending |
| 3 | Signaler | read `_state` | `11` | `11` | sees Pending |
| 4 | Token cb | CAS `11 → 10` | `11` | `10` | wins; `SetException(OCE)` |
| 5 | Signaler | CAS `11 → 10` (expected `11`) | `10` | `10` | fails; returns silently |
| 6 | Canceler | CAS `11 → 00` (expected `11`) | `10` | `10` | fails; returns silently |
| 7 | Reader | continuation | `10` | `10` | `await` throws OCE |
| 8 | Reader | next `ReadAsync`: CAS `10 → 00` | `10` | `00` | deferred cancel surfaces here |

#### Example E — Lost-wakeup defense delivers Complete (path 7)

Reader is mid-park; writer publishes a `Complete(null)` state in the small window between reader's sync data check and reader's CAS to Pending.

| t | Actor | Op | Before | After |
|---|---|---|---|---|
| 1 | Reader | sync top: read | `00` | `00` |
| 2 | Reader | sync `TryAcquire`: false (no publish yet) | `00` | `00` |
| 3 | Writer | `Writer.Complete(null)` publishes terminal `WriterState` | `00` | `00` |
| 4 | Writer | `SignalReadAwaiterIfPending`: read `_state = 00`, not Pending, return silently | `00` | `00` |
| 5 | Reader | stash; CAS `00 → 01` | `00` | `01` |
| 6 | Reader | re-check flag: clear, skip | `01` | `01` |
| 7 | Reader | re-check data: `TryAcquire` succeeds; integrate; `IsCompleted = true` | `01` | `01` |
| 8 | Reader | un-park: CAS `01 → 00`; build ReadResult with `IsCompleted=true`; return sync | `01` | `00` |

Without step 7's re-check, the reader would be parked indefinitely — writer's signal at step 4 was a no-op, and `Writer.Complete` won't publish again.

### Key takeaways for Section 5

- One packed `_state` int per awaiter; every mutation is `Interlocked.*`. No mixed primitives.
- Pattern 2 (stash-and-construct) eliminates the thread-affinity constraint on continuations. Stash is read by the signaler under release/acquire ordering on `_state`'s CAS.
- Lost-wakeup, lost-cancel, and writer-completion-exception defenses are the same shape: park, then re-check the relevant condition, then CAS out of `Pending` if the wake/throw condition holds.
- BCL-style cancel coalescing falls out of the sticky single-bit flag.
- `CancelPending*` constructs minimal results inline (no reader/writer-private state access from an arbitrary thread).
- 12 internal paths to 5 user-observable outcomes; race winners deterministic per CAS.

## Section 6 — Lifecycle and edge cases

Per the brainstorm decision (compatible BCL subset), no `Reset` is exposed. The lifecycle surface is `Complete(ex?)` per side, plus `Dispose()` for memory release. The completion-exception model follows BCL strictly (Option A from the M4 review).

### `SpscPipeOptions` (S9)

```csharp
public sealed class SpscPipeOptions
{
    public MemoryPool<byte> Pool { get; }                       // default: MemoryPool<byte>.Shared
    public int MinimumSegmentSize { get; }                      // default: 4096
    public long PauseWriterThreshold { get; }                   // default: 65536 (64 KB)
    public long ResumeWriterThreshold { get; }                  // default: 32768 (32 KB)
    public int MaxFreelistSegments { get; }                     // default: 256

    public SpscPipeOptions(
        MemoryPool<byte>? pool = null,
        int minimumSegmentSize = 4096,
        long pauseWriterThreshold = 65536,
        long resumeWriterThreshold = 32768,
        int maxFreelistSegments = 256)
    {
        if (minimumSegmentSize <= 0) throw new ArgumentOutOfRangeException(nameof(minimumSegmentSize));
        if (pauseWriterThreshold < 0) throw new ArgumentOutOfRangeException(nameof(pauseWriterThreshold));
        if (resumeWriterThreshold < 0) throw new ArgumentOutOfRangeException(nameof(resumeWriterThreshold));
        if (pauseWriterThreshold > 0 && resumeWriterThreshold > pauseWriterThreshold)
            throw new ArgumentException("ResumeWriterThreshold must be ≤ PauseWriterThreshold.");
        if (maxFreelistSegments < 0) throw new ArgumentOutOfRangeException(nameof(maxFreelistSegments));

        Pool                  = pool ?? MemoryPool<byte>.Shared;
        MinimumSegmentSize    = minimumSegmentSize;
        PauseWriterThreshold  = pauseWriterThreshold;
        ResumeWriterThreshold = resumeWriterThreshold;
        MaxFreelistSegments   = maxFreelistSegments;
    }

    public static SpscPipeOptions Default { get; } = new();
}

public sealed class SpscPipe : IDisposable
{
    public SpscPipe() : this(SpscPipeOptions.Default) { }
    public SpscPipe(SpscPipeOptions options) { _options = options; /* ... */ }

    public PipeReader Reader { get; }
    public PipeWriter Writer { get; }

    public void Dispose() { /* see Section 3 */ }
}
```

`PauseWriterThreshold = 0` means unbounded (writer never parks). Validation matches BCL's `PipeOptions` shape; defaults match BCL's defaults. `MaxFreelistSegments` is our addition (N4).

### Completion overview

| Caller | Effect on the opposite side |
|---|---|
| `Writer.Complete(null)` | Reader sees `ReadResult.IsCompleted = true` from the moment it acquires the completed `WriterState`. Buffered data drains normally; subsequent reads after drain return `(Empty, IsCompleted=true)`. |
| `Writer.Complete(ex)` | **Every** subsequent `ReadAsync`/`TryRead` throws `ex`. Buffered data is unreachable (BCL-strict, M4 Option A). |
| `Reader.Complete(null)` | Writer's `FlushAsync` returns `FlushResult.IsCompleted = true` (synchronously if no backpressure; via signaler if parked). |
| `Reader.Complete(ex)` | **Every** subsequent `FlushAsync` throws `ex`. |

### `Writer.Complete` pseudocode

```csharp
void Complete(Exception? exception = null)
{
    if (_writerCompleted) return;            // double-Complete coalesces (BCL throws; documented divergence)
    _writerCompleted = true;

    var snapshot = new WriterState
    {
        HeadSegment        = _chainHead,
        TailSegment        = _writingHead,
        TailWritten        = _writingHeadBytesBuffered,
        TotalWritten       = _totalWritten,
        IsCompleted        = true,
        CompletionException = exception,
    };
    _writerTb.ProducerSlot() = snapshot;
    _writerTb.Publish();                     // R1, R6
    _lastPublishedWriterState = snapshot;

    SignalReadAwaiterIfPending();            // R1
}
```

After completion, `GetMemory` / `Advance` / `FlushAsync` throw `InvalidOperationException` (entry guards in Section 4).

### `Reader.Complete` pseudocode (S4)

```csharp
void Complete(Exception? exception = null)
{
    if (_readerCompleted) return;
    _readerCompleted = true;

    var snapshot = new ReaderState
    {
        // S4: HeadSegment = null on terminal publish so the writer's recycle predicate (I6)
        // sweeps the entire chain on its next FlushAsync, returning all segments to the freelist.
        HeadSegment   = null,
        TotalConsumed = _totalConsumed,
        TotalExamined = _totalExamined,
        IsCompleted   = true,
        CompletionException = exception,
    };
    _readerTb.ProducerSlot() = snapshot;
    _readerTb.Publish();                     // R1, R6
    _lastPublishedReaderState = snapshot;

    SignalFlushAwaiterIfPending();           // R1
}
```

After completion, `ReadAsync` / `TryRead` / `AdvanceTo` throw `InvalidOperationException` (entry guards in Section 4).

**HeadSegment = null on terminal publish.** The writer's recycle loop is `_chainHead != _writingHead && _chainHead != readerHead` (Section 3). With `readerHead = null`, the loop's `!= readerHead` always holds, so the writer recycles every segment except the active tail. This avoids pinning the chain until `Dispose` and is safe under I6's null-check (`if (readerHead is null) return;` was the *pre-bootstrap* guard — once the reader has bootstrapped, `HeadSegment != null` until terminal publish; we adjust I6 to read "if `readerHead is null` AND `_lastPublishedReaderState.IsCompleted` is false, return" so the new behavior is captured).

Specifically, the corrected recycle loop is:
```csharp
void RecycleDrainedSegments()
{
    var r = _lastAcquiredReaderState;
    if (r.HeadSegment is null && !r.IsCompleted) return;        // pre-bootstrap (I10)
    var readerHead = r.HeadSegment;                              // null iff Reader.Complete'd

    while (_chainHead != _writingHead && _chainHead != readerHead)
    {
        var recycled = _chainHead;
        _chainHead   = _chainHead.Next!;
        if (_freelistCount < _options.MaxFreelistSegments) { PushFreelist(recycled); _freelistCount++; }
        else recycled.DisposeOwned();
    }
}
```

**Parked `ReadAsync` at moment of `Reader.Complete`.** `Reader.Complete` is on the reader thread; `ReadAsync` is also on the reader thread. By the SPSC contract, they don't overlap — there can't be a parked `ReadAsync` *and* a `Reader.Complete` from the same thread simultaneously. (If user code awaits `ReadAsync` and another reader-thread codepath calls `Complete`, that's a contract violation — undocumented behavior.)

### Exception propagation (R7, M4 Option A, BCL-strict)

After `Writer.Complete(ex)`:
- The next `_writerTb.TryAcquire()` on the reader side picks up the `IsCompleted=true, CompletionException=ex` state.
- `ReadAsync`/`TryRead`'s entry-side check throws `ex` immediately, before any `BuildReadResult` call.
- This repeats on every subsequent `ReadAsync`/`TryRead` — `_lastAcquiredWriterState` retains the completed state forever.
- A parked `ReadAsync` is woken by the signaler's `SetException(ex)` path (or the lost-wakeup defense's `SetException` if the publish raced with the park).

Symmetrically for `Reader.Complete(ex)` and the writer's `FlushAsync`.

No `_exceptionAlreadySurfaced` flag is needed under Option A — every relevant call throws; the user is expected to handle the exception and stop calling.

### `BuildReadResult` with both flags (R9)

If a sticky `CancelPending*` is consumed on a post-`Writer.Complete(null)` read, the result has *both* `IsCanceled = true` and `IsCompleted = true`. BCL allows this. `BuildReadResult` sets both flags independently. (For `Writer.Complete(ex)` the exception path fires first; the canceled-sync path doesn't trigger because the entry-guard throws.)

### Edge cases

- **Double `Complete`.** Coalesces (no-op on second call). BCL throws on second; we relax for SPSC simplicity. **Documented divergence.**
- **`CancelPending*` after `Complete`.** Safe (cross-thread by contract). Sets the flag; the next op observes `_*Completed` and either short-circuits to throw (writer-side after `Writer.Complete(ex)`, reader-side after `Reader.Complete`) or returns canceled then completed. Flag is consumed on the next non-throwing call.
- **`Complete` while opposite side parked.** R1 (publish before signal) covers it.
- **`Complete` while a `CancelPending*` race is in flight.** Standard awaiter race; either signaler or canceler wins the CAS. Both orderings reach a consistent terminal state.
- **Both sides `Complete`.** Either order. Pipe is in terminal state from both perspectives.
- **`Dispose` on a never-used pipe.** Safe; the chain and freelist are empty.
- **`AdvanceTo` argument validation (R8).** Monotonicity-only: `consumed`/`examined` can't go backwards; `consumed ≤ examined`. No upper-bound check (documented BCL divergence).
- **`AdvanceTo` on `default(SequencePosition)`.** Treated as a no-op (with publish), per N6 — handles the "AdvanceTo after empty `IsCompleted=true` ReadResult" case without NPE.
- **`CancelPendingRead` + token cancellation interaction.** If both fire concurrently, they race on the CAS out of `Pending`. The winner's path is delivered (canceled result vs. OCE throw). The loser's intent is preserved as the sticky cancel flag (if canceler lost) — surfaces on next call. Token cancellation's "loss" is just lost (the token has already fired; no sticky equivalent).
- **`RunningIndex` overflow.** `RunningIndex` is `long`; overflow at ~9 EB. Out of scope.

### Cleanup / Dispose

`SpscPipe.Dispose()` walks the chain and freelist, disposing each `BufferSegment`'s `IMemoryOwner`. Precondition: no operation is currently in flight on either side, and no `ReadResult.Buffer` references are still held by the user (use-after-free risk on segment memory). After `Dispose`, the pipe is unusable.

### Key takeaways for Section 6

- `Complete(ex?)` is a final `Publish` plus a wake of the opposite side's awaiter.
- Under Option A (BCL-strict), every call after `Complete(ex)` throws on the opposite side. No `_exceptionAlreadySurfaced` tracking needed.
- `Reader.Complete` publishes `HeadSegment = null` so the writer recycles the entire chain on its next `FlushAsync`.
- `IsCanceled` and `IsCompleted` flags are independent in `ReadResult`.
- Double-`Complete` coalesces; `IDisposable` is added; `AdvanceTo` upper-bound check omitted — three documented divergences from BCL.
- `Dispose()` releases segment + freelist memory; precondition is no in-flight ops AND no outstanding buffer refs.

## Section 7 — Verifiability

The design's correctness rests on a small number of state machines and invariants that are explicitly enumerable. This section names what must be verified; the implementation plan describes how.

**Correctness properties to verify** (full list in the invariants and rules tables below):

| Property | Invariant/rule reference |
|---|---|
| No double-completion of `_core` per park cycle | I11 |
| No lost wakeup: every published wake-condition eventually leads to a parked owner observing it | R1, R4 |
| No lost cancel: every `CancelPending*` produces exactly one `IsCanceled=true` result, accounting for races | I12 |
| Writer-completion exception thrown on every `ReadAsync`/`TryRead` after `Writer.Complete(ex)`, never silently dropped | R7 (revised under Option A) |
| Reader-completion exception thrown on every `FlushAsync` after `Reader.Complete(ex)` | R7 |
| CTR registered ⟺ disposed (no leaked registrations) | R5 |
| Recycle never premature: a segment is never returned to the freelist while reader's `HeadSegment` references it | I6 |
| Reader never reads past published `TailWritten` | I3 |
| Pattern 2 stash read by signaler reflects the reader's cursor at park time (no torn read) | I9 + R2/R3 ordering on `_state` CAS |

**Performance goals.** This design exists because BCL's `Pipe` is too slow under SPSC scheduling — its central `lock` serializes producer and consumer. The implementation must demonstrate, on a benchmark harness (`IPipeAdapter` + `Spsc`/`Bcl` adapters; **to be (re)built as a prior implementation step** — the prior iteration's harness was removed in the Restart):

- Materially higher throughput (bytes/sec) than BCL Pipe under steady-state SPSC workloads.
- Comparable or better p50 latency; materially better tail (p99/p99.9) due to no lock contention.
- Comparable allocations per op (segment freelist matches BCL's segment pool).

If these benchmark targets are not met, the design has failed its purpose and must be revisited before merge.

**Tractability.** The state space is small enough for mechanized verification (TLA+) if pursued:

- The cross-thread state-exchange primitive (TripleBuffer) is a well-known wait-free pattern; it can be modeled and verified once and treated as a primitive thereafter.
- The awaiter is a 2-state + 1-flag machine (4 reachable states) with 3 concurrent actors plus the owner.
- The recycle predicate is local and based on a single reference comparison.
- The lifecycle/completion lives in the same awaiter state machine plus the published `IsCompleted` flag.

Whether to actually pursue TLA+ verification is an implementation-plan decision; the design's tractability for it is a deliberate property.

## Consolidated invariants

| # | Invariant |
|---|---|
| **I1** | Successive `WriterState` publishes have `TotalWritten` non-decreasing and `IsCompleted` sticky-once-set. Same for `ReaderState` (`TotalConsumed`, `TotalExamined` monotonic and `TotalConsumed ≤ TotalExamined`; `IsCompleted` sticky). `CompletionException` is set iff `IsCompleted=true` and never changes once set. |
| **I2** | The writer is the sole mutator of `BufferSegment` fields, the chain (`_chainHead`/`_writingHead`), and the freelist. The reader is the sole mutator of `_readHead`, `_readTail`, byte counters. |
| **I3** | Reader's tail-bound is `_readTailIdx` (locally cached from `WriterState.TailWritten`). Reader never reads `BufferSegment.End` or `BufferSegment.Memory.Length` to determine the tail boundary. |
| **I4** | While `S == _writingHead`, the writer mutates only `S.AvailableMemory[TailWritten..]`. It does not mutate `S.End`, `S.Memory`, or `S.Next`. |
| **I5** | All freeze writes on `S` (`End`, `Memory`, `Next`) complete before the `Publish` of any `WriterState` with `TailSegment ≠ S`. |
| **I6** | A segment `S` is recycled only when `S != _writingHead` ∧ `_lastAcquiredReaderState.HeadSegment != S`. The pre-bootstrap guard (`HeadSegment is null` AND `!IsCompleted`) prevents recycling before reader has bootstrapped. The `Reader.Complete` terminal publish sets `HeadSegment = null` AND `IsCompleted = true`, allowing the writer to sweep the entire chain. |
| **I7** | Reader's `_readHead` is at-or-before `_readTail` in chain order. Reader only advances `_readHead` past `_readTail`'s segment when it has acquired a fresher `WriterState`. |
| **I8** | TripleBuffer slot **data** is mutated only by the producer side. Role rotations on `Publish`/`TryAcquire` change the **role** of a slot but never its data. |
| **I9** | Each `Publish` is a release fence; each **successful** `TryAcquire` is an acquire fence (both via `Interlocked.Exchange`). Writes preceding a `Publish` are observable to the consumer side after the matching successful `TryAcquire`. |
| **I10** | At the reader's first successful `TryAcquire`, `WriterState.HeadSegment == writer's _chainHead == start of the live chain`. Justification: the recycle predicate (I6) cannot fire until the reader has bootstrapped and published. |
| **I11** | `_state` transitions out of `Pending` exactly once per park cycle. `_core` enforces single-completion underneath. |
| **I12** | `CancelPending*` calls coalesce: any number of calls between two Inactive→Pending→Inactive cycles deliver at most one `IsCanceled = true` result. |
| **I13** | After `Writer.Complete(_)`, the writer publishes nothing further (R6). The published `WriterState` is sticky with `IsCompleted=true` and `CompletionException` set if the completion was faulted. |
| **I14** | After `Reader.Complete(_)`, the reader publishes nothing further. The published `ReaderState` has `HeadSegment = null` (allowing full chain recycle), `IsCompleted = true`, and `CompletionException` set if faulted. |
| **I15** | `SpscPipe.Dispose()` precondition: no operation is currently in flight on either side, and no `ReadResult.Buffer` references are still held by the user. Violation is undefined behavior. |

## Consolidated rules

| # | Rule |
|---|---|
| **R1** | Publish before signal. In any code path that both publishes a state and signals the other side's awaiter, publish first, signal second. |
| **R2** | All `_state` mutations on `SpscAwaiter` use `Interlocked.CompareExchange` or `Interlocked.Or`. No plain or `Volatile` writes. |
| **R3** | Transitions out of `Pending` (signaler / canceler / token) atomically clear the state bits. The flag bit is preserved on signaler/token paths, cleared on the canceler-delivered-via-state path. The actor that wins the CAS calls the appropriate `_core.Set*`. |
| **R4** | After CASing `Inactive → Pending`, the owner re-checks (a) the cancel flag, (b) the writer-completion exception (reader side) or reader-completion exception (writer side), and (c) the data state / wake condition. Each re-check attempts a CAS out of `Pending` if its condition holds. |
| **R5** | After registering a `CancellationTokenRegistration` on `_ctr`, the registering actor must re-check `_state` and dispose `_ctr` if the awaiter is no longer in `Pending`. CTR disposal is idempotent. |
| **R6** | `Complete(ex?)` on either side is the terminal publish; no further `Publish` happens after it on the completing TripleBuffer. |
| **R7** | **(Option A, BCL-strict.)** After `Writer.Complete(ex)`, every `ReadAsync` / `TryRead` on the reader throws `ex`. After `Reader.Complete(ex)`, every `FlushAsync` on the writer throws `ex`. No drain semantics; buffered data after a faulted writer is unreachable. No `_exceptionAlreadySurfaced` flag needed. |
| **R8** | `AdvanceTo` validates monotonicity only: `consumed`/`examined` must not go backwards relative to `_totalConsumed`/`_totalExamined`, and `consumed ≤ examined`. Out-of-range positions throw `InvalidOperationException`. Upper-bound (within previous buffer) is **not** validated — documented BCL divergence. |
| **R9** | `BuildReadResult` produces `(IsCanceled, IsCompleted)` independently — both flags can be true simultaneously when a sticky cancel is consumed on a post-`Writer.Complete(null)` read. (Post-`Writer.Complete(ex)` case: throw fires first; canceled path doesn't trigger.) |

## Conventions

- **TryAcquire-once-per-method default.** Each method does at most one `TryAcquire` of each TripleBuffer, ideally near the top. Documented exception: `FlushAsync` acquires `_readerTb` mid-method (after publish) for tighter backpressure responsiveness.
- **No `Reset`.** The lifecycle surface is single-use; the pipe is created, used, both sides `Complete`, then `Dispose`.
- **Thread contract.** Strict SPSC for normal ops; `CancelPending*` is the only thread-safe-from-anywhere operation.
- **`MemoryPool<byte>.Rent` returns `≥ sizeHint`.** Code reasoning about rented capacity must use `>=`, never `==`.
- **Sticky once set.** Used consistently for `IsCompleted` (false→true exactly once); avoid "monotonic"/"non-decreasing" for boolean fields.
