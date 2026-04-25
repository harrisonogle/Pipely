# SPSC Pipe — Design (TripleBuffer-based)

**Date:** 2026-04-25
**Status:** Design (pre-implementation)

## Top-level key takeaways

- A lock-free, single-producer single-consumer (SPSC) implementation of `System.IO.Pipelines.PipeReader` / `PipeWriter`, sharing all cross-thread state through two `TripleBuffer<T>` instances rather than a shared mutable linked list.
- Each side publishes a self-consistent monotonic snapshot of its own state (counters + segment cursor + completion); each side reads the other's latest snapshot opportunistically. Snapshot loss is safe because everything is monotonic.
- The writer is the sole mutator of segments and the chain. Reader holds references but never mutates. Recycling is gated on the reader's published `HeadSegment`, not byte counters, eliminating a class of "still-referenced" bugs.
- The awaiter is a single packed `int` per direction. Every state transition is `Interlocked.CompareExchange` or `Interlocked.Or`. No mixed `Volatile`/`Interlocked` patterns; no inter-field ordering to reason about.
- Cancel-from-any-thread (`CancelPending*`) is a sticky bit on the awaiter, naturally coalescing per BCL semantics.
- Surface is a compatible BCL subset (no `Reset`, no deprecated callbacks): `ReadAsync` / `FlushAsync` / `Advance` / `AdvanceTo` / `Complete` / `CancelPending*` with full BCL-matching `PauseWriterThreshold` / `ResumeWriterThreshold` semantics.

## Section 1 — Architecture overview

### Components

- **`SpscPipe`** — owns:
  - `_writerTb : TripleBuffer<WriterState>` — written by writer thread, read by reader thread.
  - `_readerTb : TripleBuffer<ReaderState>` — written by reader thread, read by writer thread.
  - `_readAwaiter : SpscAwaiter<ReadResult>` — woken by writer; awaited by reader.
  - `_flushAwaiter : SpscAwaiter<FlushResult>` — woken by reader; awaited by writer.
- **`SpscPipe.Writer : PipeWriter`** — single producer thread. Owns the segment chain (`_chainHead` → `_writingHead`), a private `BufferSegment` freelist, the `MemoryPool<byte>` reference, and writer-local cursors.
- **`SpscPipe.Reader : PipeReader`** — single consumer thread. Owns reader-local cursors and the most recently acquired `WriterState`.
- **`SpscAwaiter<T>`** — wraps `ManualResetValueTaskSourceCore<T>` with a single packed `int` state field. Detail in Section 5.

### Local cursors (private to each side)

**Writer-side (touched only by the writer thread):**
- `_chainHead : BufferSegment?` — first segment in the live chain.
- `_writingHead : BufferSegment?` — current tail segment being filled.
- `_writingHeadBytesBuffered : int` — bytes written into `_writingHead` (not yet necessarily published).
- `_totalWritten : long` — monotonic byte counter.
- `_lastPublishedWriterState : WriterState` — what the reader will see; supports short-circuit decisions.
- `_lastAcquiredReaderState : ReaderState` — most recent acquire from `_readerTb`.
- `_writerCompleted : bool` — sticky after `Complete`.

**Reader-side (touched only by the reader thread):**
- `_readHead : BufferSegment?`, `_readHeadIdx : int` — head of unconsumed data.
- `_readTail : BufferSegment?`, `_readTailIdx : int` — boundary of the most recently acquired publish; stable until next `TryAcquire`.
- `_totalConsumed : long`, `_totalExamined : long` — monotonic.
- `_readResultBufferLength : long` — length of the most recently produced `ReadResult.Buffer`; used by `AdvanceTo` argument validation (R8).
- `_lastPublishedReaderState : ReaderState`.
- `_lastAcquiredWriterState : WriterState`.
- `_exceptionAlreadySurfaced : bool` — ensures writer-completion-with-exception is thrown at most once (R7).
- `_readerCompleted : bool` — sticky after `Complete`.

### Threading contract (strict SPSC)

- All `Writer` calls (`GetMemory`, `GetSpan`, `Advance`, `FlushAsync`, `Complete`) on a single producer thread.
- All `Reader` calls (`ReadAsync`, `TryRead`, `AdvanceTo`, `Complete`) on a single consumer thread.
- `CancelPendingRead` / `CancelPendingFlush` are explicitly thread-safe (callable from any thread).
- The two threads communicate exclusively via the two `TripleBuffer`s and the two `SpscAwaiter` state machines. No locks. No shared mutable structures outside those two primitives.

### Data flow per cycle (steady state)

1. Writer fills tail segment locally (no cross-thread visibility).
2. `FlushAsync`: builds a `WriterState`, `_writerTb.Publish()`, signals `_readAwaiter`, then `_readerTb.TryAcquire()` for fresh reader state, recycles drained segments, returns or parks on backpressure.
3. `ReadAsync`: `_writerTb.TryAcquire()`, integrates fresh `WriterState` into local cursors, returns synchronously if there's progress; else parks.
4. `AdvanceTo`: updates local cursors, builds `ReaderState`, `_readerTb.Publish()`, signals `_flushAwaiter` if backpressure has relieved.

### Key takeaways for Section 1

- Two TripleBuffers carry all cross-thread state; two awaiters carry all cross-thread wakeups. That's the entire surface area for synchronization.
- Each side has private local cursors (caches of last-published / last-acquired state) so it can build buffers and decisions without re-reading the TBs every operation.
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
    public bool           IsCompleted;     // sticky once true
    public Exception?     CompletionException;  // set iff IsCompleted with an exception
}
```

### `ReaderState` — published by the reader thread, acquired by the writer

```csharp
internal struct ReaderState
{
    public BufferSegment? HeadSegment;     // first segment still alive on the reader's view
    public int            HeadConsumed;    // bytes consumed within HeadSegment
    public long           TotalConsumed;   // monotonic — backpressure denominator
    public long           TotalExamined;   // monotonic — flush-awaiter wake gate
    public bool           IsCompleted;
    public Exception?     CompletionException;
}
```

### Default state semantics

`default(WriterState)` and `default(ReaderState)` represent "nothing yet, not completed" — correct for the pre-first-publish case where `TryAcquire` returns false and the receiving side uses `default` as its `_lastAcquired*` cache.

### Cancellation is *not* in either snapshot

`CancelPending*` are documented as thread-safe (callable from any thread), so they can't be folded into the strictly-single-writer/single-reader TBs without extra synchronization. Each awaiter carries its own atomic cancel flag instead — see Section 5.

### Head==tail discipline (the load-bearing invariant for safety)

When the chain has one segment (`_chainHead == _writingHead`), the reader's `_readHead` and `_readTail` both point at that segment while the writer is concurrently appending bytes into it. Three rules keep this safe:

**Reader's tail-bound discipline.** The reader uses its locally cached `_readTailIdx` (set from `WriterState.TailWritten` at `TryAcquire` time) as the ROS endIndex. It *never* reads `BufferSegment.End` or `BufferSegment.Memory.Length` to determine how far the published data extends.

**Writer's mutation discipline on the live tail.** While `S` is the active tail, the writer only appends bytes into `S.AvailableMemory[TailWritten..]`. It does not mutate `S.End`, `S.Memory`, or `S.Next` until the freeze step at segment transition.

**Freeze step (transitioning from `S` to a new tail `S'`).**

1. Rent and initialize `S'` (set `RunningIndex`, `AvailableMemory`).
2. Set `S.End = bytes_filled_in_S`.
3. Set `S.Memory = S.AvailableMemory.Slice(0, S.End)`.
4. Set `S.Next = S'`.
5. Begin writing into `S'`.
6. `Publish` `W' = {TailSegment: S', TailWritten: bytes_in_S', ...}`.

Steps 2–4 are unordered; only the position of *all of them* before step 6 matters. Step 6's `Interlocked.Exchange` is a full barrier, propagating all freeze writes to readers that subsequently `TryAcquire`.

**Benign-torn-read note on `S.Memory`.** During step 3, a concurrent reader iterating an ROS bounded at `_readTailIdx` may observe a torn `Memory<byte>` struct. Only `_length` changes (`_object` and `_index` are stable per segment because `Slice(0, n)` preserves them); both pre-freeze (`capacity`) and post-freeze (`S.End`) values are ≥ `_readTailIdx`, so the resulting slice `[0.._readTailIdx]` is valid in either case.

### Key takeaways for Section 2

- Each snapshot is self-contained and monotonic; intermediate snapshot loss is safe.
- `HeadSegment` field is needed only for first-acquire bootstrap; subsequent acquires the reader uses local cursors.
- The reader's tail-bound is `_readTailIdx`, not `BufferSegment.End`. This single discipline eliminates the head==tail race that bit prior implementations.
- All freeze writes are ordered before the publish; the publish's full-barrier semantics propagate them to the reader.

## Section 3 — Segment ownership, freelist, BufferSegment, reader bootstrap

### Ownership

| Resource | Owner | Other side's access |
|---|---|---|
| Linked list (`Next` pointers) | Writer | Reader walks but never mutates |
| `BufferSegment` objects | Writer (allocated via freelist) | Reader holds references via `_readHead`, `_readTail`, and acquired `WriterState`s |
| `IMemoryOwner<byte>` per segment | Writer (rents from `PipeOptions.Pool`) | None directly; reader sees buffer via `BufferSegment.Memory` |
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
    public new BufferSegment? Next           { get; private set; }   // null while active tail; set on freeze

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

### Allocation path (writer rents a new tail)

1. Pop from freelist if non-empty *and* the popped segment's `AvailableMemory.Length ≥ sizeHint`. Otherwise allocate fresh.
2. Fresh: `new BufferSegment().RentFrom(_pool, max(sizeHint, _options.MinimumSegmentSize), runningIndex)`.
3. Reused: `segment.RecycleReset(runningIndex)`.
4. `runningIndex = _writingHead == null ? 0 : _writingHead.RunningIndex + _writingHead.End` (requires the existing tail to be frozen first).
5. Wire into chain: freeze old tail with `next = newTail`. Update `_writingHead = newTail`.

The freelist is a writer-private singly-linked-list head pointer chained via `Next` while sitting on the freelist; cleared in `RecycleReset`.

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
        PushFreelist(recycled);                 // I6
    }
}
```

The predicate uses `HeadSegment` reference comparison rather than byte arithmetic. This matches the original "S ≺ HeadSegment in chain order" intuition exactly and avoids a subtle off-by-one that the byte-offset form had at the boundary `HeadConsumed == End`.

### `MemoryPool` integration and segment sizing

- `PipeOptions.Pool` (default `MemoryPool<byte>.Shared`) supplies `IMemoryOwner<byte>`s.
- `PipeOptions.MinimumSegmentSize` (default 4096) is the floor for `Pool.Rent(sizeHint)` calls.
- Upper bound on segment size is governed by the underlying `MemoryPool<byte>.MaxBufferSize`; we don't impose a separate cap.
- `GetMemory(sizeHint)`: if `sizeHint > 0` and the current tail can't satisfy it, transition to a new tail of size `Max(sizeHint, MinimumSegmentSize)`. Otherwise return remaining capacity in the current tail.

### Reader bootstrap

The reader's `_readHead` starts as `null`. On the first successful `_writerTb.TryAcquire()`:

```csharp
_readHead    = acquired.HeadSegment;
_readHeadIdx = 0;
_readTail    = acquired.TailSegment;
_readTailIdx = acquired.TailWritten;
```

After bootstrap, the reader ignores `WriterState.HeadSegment` on subsequent acquires — its local `_readHead` is authoritative.

**Why bootstrap is safe (I10).** The recycle predicate cannot fire until `_lastAcquiredReaderState.HeadSegment != null`, which requires the reader to have bootstrapped and published. So the writer's `_chainHead` does not advance until *after* the reader's first successful read — at the moment of bootstrap, `WriterState.HeadSegment == _chainHead == start of the live chain`.

Edge cases:
- **Writer completes without writing.** `WriterState{HeadSegment: null, TailSegment: null, IsCompleted: true}`. Reader's first `TryAcquire` gets this; `_readHead` stays null. `ReadResult` is empty with `IsCompleted: true`.
- **Writer publishes before any data.** Doesn't happen in normal flow; `FlushAsync` only publishes when there's something to publish or on `Complete`.

### Lifecycle: cleanup on Dispose

`SpscPipe.Dispose()` walks the chain and the freelist, calling `BufferSegment.DisposeOwned()` on each to release `IMemoryOwner` rentals. Precondition: no operation is currently in-flight on either side (typically, both completed). After Dispose, the pipe object should not be used.

### Key takeaways for Section 3

- Writer is the sole mutator of segments and the chain; reader walks but doesn't mutate.
- Recycling reuses both the `BufferSegment` object and its `IMemoryOwner` — only `Dispose()` releases memory back to the pool.
- The recycle predicate is a single reference comparison: `_chainHead != ReaderState.HeadSegment`.
- Reader bootstrap rides on `WriterState.HeadSegment`, used only on first acquire; subsequent acquires the reader uses its local `_readHead`.

## Section 4 — Hot paths (steady-state pseudocode)

This section assumes the happy path: no parking, no cancellation, no completion. Section 5 layers awaiter parking, cancellation, and completion on top. Each step references invariants from the consolidated table at the bottom.

### Writer: `GetMemory(int sizeHint)`

```csharp
Memory<byte> GetMemory(int sizeHint = 0)
{
    if (sizeHint < 0) throw new ArgumentOutOfRangeException(nameof(sizeHint));
    if (sizeHint == 0) sizeHint = 1;
    // Upper bound is governed by the underlying MemoryPool; no separate cap.

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
            // Transition: freeze current tail (I5), rent new tail.
            int filled  = _writingHeadBytesBuffered;
            long newRI  = _writingHead.RunningIndex + filled;
            var newTail = RentSegment(Max(sizeHint, _options.MinimumSegmentSize), newRI);

            _writingHead.Freeze(filled, newTail);
            _writingHead             = newTail;
            _writingHeadBytesBuffered = 0;
        }
    }

    return _writingHead.AvailableMemory.Slice(_writingHeadBytesBuffered);
}
```

Purely writer-local (I2). No TB interaction.

### Writer: `Advance(int bytes)`

```csharp
void Advance(int bytes)
{
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
    _writerTb.Publish();                              // R1: publish before signal; I9 release fence
    _lastPublishedWriterState = snapshot;

    SignalReadAwaiterIfPending();                     // R1

    if (_readerTb.TryAcquire())                       // I9 acquire fence
        _lastAcquiredReaderState = _readerTb.ConsumerSlot();
    RecycleDrainedSegments();                         // I6

    long unconsumed = _totalWritten - _lastAcquiredReaderState.TotalConsumed;
    if (_options.PauseWriterThreshold == 0 || unconsumed <= _options.PauseWriterThreshold)
        return new ValueTask<FlushResult>(new FlushResult(isCanceled: false, isCompleted: false));

    return ParkFlushAwaiter(ct);                       // see Section 5
}
```

### Reader: `ReadAsync(CancellationToken ct)` (synchronous fast path)

```csharp
ValueTask<ReadResult> ReadAsync(CancellationToken ct)
{
    if (_writerTb.TryAcquire())                       // I9 acquire fence
    {
        _lastAcquiredWriterState = _writerTb.ConsumerSlot();
        IntegrateAcquiredWriterState();
    }

    if (HasReadableProgress() || _lastAcquiredWriterState.IsCompleted)
        return new ValueTask<ReadResult>(BuildReadResult(isCanceled: false));

    return ParkReadAwaiter(ct);                       // see Section 5
}

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

### Reader: `AdvanceTo(SequencePosition consumed, SequencePosition examined)`

```csharp
void AdvanceTo(SequencePosition consumed, SequencePosition examined)
{
    var consumedSeg = (BufferSegment)consumed.GetObject()!;
    var examinedSeg = (BufferSegment)examined.GetObject()!;
    int consumedIdx = consumed.GetInteger();
    int examinedIdx = examined.GetInteger();

    long consumedAbs = consumedSeg.RunningIndex + consumedIdx;       // O(1) via I2, I9
    long examinedAbs = examinedSeg.RunningIndex + examinedIdx;

    // R8: validate against the previously-returned buffer's bounds.
    if (consumedAbs < _totalConsumed
        || examinedAbs < _totalExamined
        || consumedAbs > examinedAbs
        || examinedAbs > _totalConsumed + _readResultBufferLength)
    {
        throw new InvalidOperationException("AdvanceTo position out of range");
    }

    _readHead       = consumedSeg;
    _readHeadIdx    = consumedIdx;
    _totalConsumed  = consumedAbs;
    _totalExamined  = examinedAbs;

    var snapshot = new ReaderState
    {
        HeadSegment   = _readHead,
        HeadConsumed  = _readHeadIdx,
        TotalConsumed = _totalConsumed,
        TotalExamined = _totalExamined,
        IsCompleted   = false,
        CompletionException = null,
    };
    _readerTb.ProducerSlot() = snapshot;
    _readerTb.Publish();                              // R1
    _lastPublishedReaderState = snapshot;

    SignalFlushAwaiterIfPending();                    // R1
}
```

### Reader: `BuildReadResult` (R9 — both flags can be set)

```csharp
ReadResult BuildReadResult(bool isCanceled)
{
    bool isCompleted = _lastAcquiredWriterState.IsCompleted
                       && _totalExamined >= _lastAcquiredWriterState.TotalWritten;
    var buffer = isCanceled
        ? ReadOnlySequence<byte>.Empty
        : (_readHead == null
            ? ReadOnlySequence<byte>.Empty
            : new ReadOnlySequence<byte>(_readHead, _readHeadIdx, _readTail!, _readTailIdx));   // I3, I7
    _readResultBufferLength = buffer.Length;
    return new ReadResult(buffer, isCanceled, isCompleted);
}
```

### Conventions

- **TryAcquire-once-per-method default.** Each method does at most one `TryAcquire` of each TripleBuffer, ideally near the top. Documented exceptions: `FlushAsync` acquires `_readerTb` mid-method (after publish) for tighter backpressure responsiveness.
- **R1 (publish-before-signal).** Any code path that both publishes a state and signals the other side's awaiter publishes first, signals second.

### Key takeaways for Section 4

- `GetMemory` / `Advance` are purely writer-local — no fences, no TB ops. All visibility happens at `FlushAsync`.
- `FlushAsync` ordering: publish → signal → acquire → recycle → backpressure check.
- `ReadAsync` ordering: acquire → integrate (bootstrap on first call) → return-or-park.
- `AdvanceTo` is O(1) via `RunningIndex`; always publishes (cheap; keeps writer's view of progress fresh).
- Reader's tail-bound (`_readTailIdx`) is set only when integrating a freshly-acquired `WriterState` — the single point where I3 is established for each subsequent `ReadResult`.

## Section 5 — Awaiter coordination

### Awaiter shape (single packed `int`)

```csharp
internal sealed class SpscAwaiter<T> : IValueTaskSource<T>
{
    private ManualResetValueTaskSourceCore<T> _core;
    private int _state;                          // packed: bit 0 = state, bit 1 = cancel flag
    private CancellationTokenRegistration _ctr;
    private CancellationToken _token;            // cached for OCE construction

    private const int Inactive   = 0b00;
    private const int Pending    = 0b01;
    private const int StateMask  = 0b01;
    private const int CancelFlag = 0b10;

    public short Version => _core.Version;
    public T GetResult(short token) => _core.GetResult(token);
    public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);
    public void OnCompleted(Action<object?> c, object? s, short token, ValueTaskSourceOnCompletedFlags f)
        => _core.OnCompleted(c, s, token, f);
}
```

Two states (`Inactive` / `Pending`) suffice; "how was this completed" lives in `_core`'s status. Every `_state` mutation is `Interlocked.CompareExchange` or `Interlocked.Or` — no mixed `Volatile`/`Interlocked` patterns, no inter-field ordering rules.

### Reader's park / wake

```csharp
ValueTask<ReadResult> ReadAsync(CancellationToken ct)
{
    // Sync entry: consume sticky cancel flag if present.
    while (true)
    {
        int oldV = _readAwaiter._state;
        if ((oldV & CancelFlag) == 0) break;
        int desired = oldV & ~CancelFlag;
        if (Interlocked.CompareExchange(ref _readAwaiter._state, desired, oldV) == oldV)
            return new ValueTask<ReadResult>(BuildReadResult(isCanceled: true));
    }

    if (ct.IsCancellationRequested)
        return ValueTask.FromCanceled<ReadResult>(ct);

    if (_writerTb.TryAcquire())
    {
        _lastAcquiredWriterState = _writerTb.ConsumerSlot();
        IntegrateAcquiredWriterState();
    }

    if (HasReadableProgress() || _lastAcquiredWriterState.IsCompleted)
        return new ValueTask<ReadResult>(BuildReadResult(isCanceled: false));

    return ParkReadAwaiter(ct);
}

ValueTask<ReadResult> ParkReadAwaiter(CancellationToken ct)
{
    _readAwaiter._core.Reset();
    _readAwaiter._token = ct;

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
            _readAwaiter._core.SetResult(default);       // sentinel; see "Result construction" note below
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

### Writer's park / wake

`FlushAsync` / `ParkFlushAwaiter` / `SignalFlushAwaiterIfPending` / `OnFlushAwaiterTokenCancel` mirror the reader-side pseudocode against `_flushAwaiter`. The wake condition swaps from "readable progress" to `unconsumed ≤ ResumeWriterThreshold` (BCL hysteresis).

### Cross-thread cancel (any thread)

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
        _readAwaiter._core.SetResult(BuildReadResult(isCanceled: true));
    }
}

public void CancelPendingFlush() { /* symmetric on _flushAwaiter */ }
```

### Result construction across thread boundaries (implementation note)

The signaler runs on the writer thread; `ReadResult` construction needs reader-private cursors (`_readHead`, `_readTail`, etc.) per I3. The signaler therefore can't construct the `ReadResult` directly. Two viable patterns; the implementation will pick one:

1. **Lazy construction in `GetResult`.** `SpscAwaiter<T>` overrides `IValueTaskSource<T>.GetResult`. The signaler completes `_core` with `default(T)` (a sentinel). When the awaiter's continuation runs, `GetResult` calls `_core.GetResult(token)` to consume the completion (and propagate any exception via `SetException`), then calls back into the pipe to integrate state and build the real `ReadResult`. Requires the continuation to run on the reader thread (enforced via `ValueTaskSourceOnCompletedFlags.UseSchedulingContext` plus reader-side discipline about `ConfigureAwait`).
2. **Stash-and-construct.** When the reader parks, it stashes its current cursor on the awaiter. The signaler reads the stash plus its own just-published `WriterState` to construct the `ReadResult` and pass it to `_core.SetResult`. No thread-affinity constraint on the continuation. Requires the awaiter to carry the stash field.

Both work; the choice is performance vs. simplicity and will be made in the implementation phase. The spec's pseudocode uses `default` as a sentinel for clarity.

### Cancellation semantics summary

| Path | Trigger | Observable result |
|---|---|---|
| `CancellationToken` cancel | `ct` fires (sync at entry, or async via registration) | `OperationCanceledException` thrown from the `await` |
| `CancelPendingRead`/`CancelPendingFlush` | Method call from any thread | Result with `IsCanceled = true`, no exception |

### Outcome × path matrix (case enumeration)

|                          | Sync entry | Park re-check | Park: registered & waiting |
|--------------------------|:---:|:---:|:---:|
| Returns `IsCanceled=false` data | ✓ (1) | ✓ (5) — lost-wakeup defense | ✓ (8) — signaled by writer |
| Returns `IsCanceled=true`        | ✓ (2) — sticky flag at entry | ✓ (6) — lost-cancel defense | ✓ (9) — `CancelPending*` while parked |
| Throws `OperationCanceledException` | ✓ (3) — `ct.IsCancellationRequested` at entry | n/a (token not yet registered) | ✓ (10) — token fires while parked |
| Returns `IsCompleted=true` (drained) | ✓ (4) — writer already completed | ✓ (7) — lost-wakeup defense delivered Complete | (subsumed by 8) |

10 distinct internal paths produce 4 user-observable outcomes. During the parked phase, three actors race for the CAS out of `Pending` (signaler, canceler, token); whichever commits first wins, others back off silently.

### Concrete trace examples

`_state` is shown in binary (`SF` where `S` = state bit, `F` = flag bit). `Inactive` = `00`, `Pending` = `01`, `Inactive+Flag` = `10`, `Pending+Flag` = `11`.

#### Example A — Happy path (path 8)

Initial: `_state = 00`.

| t | Actor | Op | Before | After |
|---|---|---|---|---|
| 1–4 | Reader | sync entry, no flag, no data; `_core.Reset()`; CAS `00 → 01` | `00` | `01` |
| 5 | Reader | re-check flag: clear, skip | `01` | `01` |
| 6 | Reader | re-check data: false | `01` | `01` |
| 7 | Reader | register CTR, return parked task | `01` | `01` |
| 8 | Writer | publishes WriterState | `01` | `01` |
| 9 | Writer | `Signal`: CAS `01 → 00` | `01` | `00` — disposes CTR, `SetResult(data)` |

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

### Key takeaways for Section 5

- One packed `_state` int per awaiter; every mutation is `Interlocked.*`. No mixed primitives.
- Lost-wakeup and lost-cancel defenses are the same shape: park, then re-check the relevant condition, then CAS out of `Pending` if the wake condition holds.
- BCL-style cancel coalescing falls out of the sticky single-bit flag.
- 10 internal paths to 4 user-observable outcomes; race winners deterministic per CAS.

## Section 6 — Lifecycle and edge cases

Per the brainstorm decision (compatible BCL subset), no `Reset` is exposed. The lifecycle surface is `Complete(ex?)` per side, plus `Dispose()` for memory release.

### Completion overview

| Caller | Effect |
|---|---|
| `Writer.Complete(null)` | Reader will see `ReadResult.IsCompleted = true` after draining all published bytes. |
| `Writer.Complete(ex)` | Reader's `ReadAsync` returns drained data with `IsCompleted = true`; on the *next* `ReadAsync` after draining, throws `ex` (R7). |
| `Reader.Complete(null)` | Writer's next `FlushAsync` returns `FlushResult.IsCompleted = true`. |
| `Reader.Complete(ex)` | Writer's next (or current parked) `FlushAsync` throws `ex`. |

### `Writer.Complete` pseudocode

```csharp
void Complete(Exception? exception = null)
{
    if (_writerCompleted) return;            // double-Complete coalesces
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

    SignalReadAwaiterIfPending();
}
```

After completion, `GetMemory` / `Advance` throw `InvalidOperationException`. `FlushAsync` short-circuits to a synchronous result.

### `Reader.Complete` pseudocode

Symmetric: publish a final `ReaderState{IsCompleted: true, CompletionException: ex, ...}`, signal `_flushAwaiter`. After completion, `ReadAsync` / `TryRead` / `AdvanceTo` throw.

### Exception propagation (R7)

The reader's `BuildReadResult` returns `IsCompleted = true` only when `_totalExamined >= TotalWritten`. The writer-completion exception is thrown from the `ReadAsync` call that occurs *after* the final `IsCompleted = true` `ReadResult` has been observed. A reader-private bool `_exceptionAlreadySurfaced` ensures the exception is thrown exactly once.

### `BuildReadResult` with both flags (R9)

If a sticky `CancelPending*` is consumed on a post-Complete read, the result has *both* `IsCanceled = true` and `IsCompleted = true`. BCL allows this. The pseudocode in Section 4 builds both flags independently rather than choosing one.

### Edge cases

- **Double `Complete`.** Coalesces (no-op on second call). BCL throws on second; we relax for SPSC simplicity. **Documented divergence.**
- **`CancelPending*` after `Complete`.** Safe (cross-thread by contract). Sets the flag, but the next op observes `IsCompleted` and short-circuits; the flag never produces an `IsCanceled = true` observable.
- **`Complete` while opposite side parked.** R1 (publish before signal) covers it.
- **`Complete` while a `CancelPending*` race is in flight.** Standard awaiter race; either signaler or canceler wins the CAS. Both orderings reach a consistent terminal state (the loser's intent is reflected in subsequent calls).
- **Both sides `Complete`.** Either order. Pipe is in terminal state from both perspectives.
- **`Dispose` on a never-used pipe.** Safe; the chain and freelist are empty.
- **`AdvanceTo` argument validation (R8).** `consumed`/`examined` validated against `[_totalConsumed, _totalConsumed + _readResultBufferLength]` in absolute byte offsets; out-of-range positions throw `InvalidOperationException`.

### Cleanup / Dispose

`SpscPipe.Dispose()` walks the chain and freelist, disposing each `BufferSegment`'s `IMemoryOwner`. Precondition: no operation is currently in flight on either side. After Dispose, the pipe is unusable.

### Key takeaways for Section 6

- `Complete(ex?)` is a final `Publish` plus a wake of the opposite side's awaiter.
- Reader holds writer-completion-exception until drained, then throws once (`_exceptionAlreadySurfaced`).
- `IsCanceled` and `IsCompleted` flags are independent in `ReadResult`.
- Double-Complete is a no-op coalesce (small documented BCL divergence).
- `Dispose()` releases segment+freelist memory; precondition is no in-flight ops.

## Section 7 — Verifiability

The design's correctness rests on a small number of state machines and invariants that are explicitly enumerable. This section names what must be verified; the implementation plan describes how.

**Correctness properties to verify** (full list in the invariants and rules tables below):

- No double-completion of `_core` per park cycle.
- No lost wakeup: every published wake-condition eventually leads to a parked owner observing it.
- No lost cancel: every `CancelPending*` produces exactly one `IsCanceled = true` result, accounting for races (I12).
- Exactly-once exception surface (R7).
- CTR registered ⟺ disposed (R5).
- Recycle never premature: a segment is never returned to the freelist while reader's `HeadSegment` references it (I6).
- Reader never reads past published `TailWritten` (I3).

**Performance goals.** This design exists because BCL's `Pipe` is too slow under SPSC scheduling — its central `lock` serializes producer and consumer. The implementation must demonstrate, on the existing `tests/SpscPipe.Benchmarks` infrastructure (`IPipeAdapter` + `Spsc`/`Bcl` adapters):

- Materially higher throughput (bytes/sec) than BCL Pipe under steady-state SPSC workloads.
- Comparable or better p50 latency; materially better tail (p99/p99.9) due to no lock contention.
- Comparable allocations per op (segment freelist matches BCL's segment pool).

If these benchmark targets are not met, the design has failed its purpose and must be revisited before merge.

**Tractability.** The state space is small enough for mechanized verification (TLA+):

- The cross-thread state-exchange primitive (TripleBuffer) is a well-known wait-free pattern; it is modeled and verified once and treated as a primitive thereafter.
- The awaiter is a 2-state + 1-flag machine (4 reachable states) with 3 concurrent actors (signaler, canceler, token callback).
- The recycle predicate is local and based on a single reference comparison.
- The lifecycle/completion lives in the same awaiter state machine plus the published `IsCompleted` flag.

This compresses the verification model significantly compared to a shared-mutable-chain design and is a deliberate design property, not an accident.

## Consolidated invariants

| # | Invariant |
|---|---|
| **I1** | Successive `WriterState` publishes have `TotalWritten` and `IsCompleted` non-decreasing. Same for `ReaderState` (`TotalConsumed ≤ TotalExamined`, both monotonic; `IsCompleted` sticky). `CompletionException` is set iff `IsCompleted=true` and never changes once set. |
| **I2** | The writer is the sole mutator of `BufferSegment` fields, the chain (`_chainHead`/`_writingHead`), and the freelist. The reader is the sole mutator of `_readHead`, `_readTail`, byte counters. |
| **I3** | Reader's tail-bound is `_readTailIdx` (locally cached from `WriterState.TailWritten`). Reader never reads `BufferSegment.End` or `BufferSegment.Memory.Length` to determine the tail boundary. |
| **I4** | While `S == _writingHead`, the writer mutates only `S.AvailableMemory[TailWritten..]`. It does not mutate `S.End`, `S.Memory`, or `S.Next`. |
| **I5** | All freeze writes on `S` (`End`, `Memory`, `Next`) complete before the `Publish` of any `WriterState` with `TailSegment ≠ S`. |
| **I6** | A segment `S` is recycled only when `S != _writingHead` ∧ `_lastAcquiredReaderState.HeadSegment != null` ∧ `_lastAcquiredReaderState.HeadSegment != S`. |
| **I7** | Reader's `_readHead` is at-or-before `_readTail` in chain order. Reader only advances `_readHead` past `_readTail`'s segment when it has acquired a fresher `WriterState`. |
| **I8** | TripleBuffer slot **data** is mutated only by the producer side. Role rotations on `Publish`/`TryAcquire` change the **role** of a slot but never its data. |
| **I9** | Each `Publish` is a release fence; each `TryAcquire` is an acquire fence (both via `Interlocked.Exchange`). Writes preceding a `Publish` are observable to the consumer side after the matching `TryAcquire`. |
| **I10** | At the reader's first successful `TryAcquire`, `WriterState.HeadSegment == writer's _chainHead == start of the live chain`. Justification: the recycle predicate (I6) cannot fire until the reader has bootstrapped and published. |
| **I11** | `_state` transitions out of `Pending` exactly once per park cycle. `_core` enforces single-completion underneath. |
| **I12** | `CancelPending*` calls coalesce: any number of calls between two Inactive→Pending→Inactive cycles deliver at most one `IsCanceled = true` result. |
| **I13** | After `Writer.Complete(ex)`, the reader's published counters may continue advancing as the reader drains, but `_writerCompleted` is sticky-true and the writer publishes nothing further (R6). |
| **I14** | After `Reader.Complete(_)`, the reader publishes nothing further. |
| **I15** | `SpscPipe.Dispose()` precondition: no operation is currently in flight on either side. Violation is undefined behavior. |

## Consolidated rules

| # | Rule |
|---|---|
| **R1** | Publish before signal. In any code path that both publishes a state and signals the other side's awaiter, publish first, signal second. |
| **R2** | All `_state` mutations on `SpscAwaiter` use `Interlocked.CompareExchange` or `Interlocked.Or`. No plain or `Volatile` writes. |
| **R3** | Transitions out of `Pending` (signaler / canceler / token) atomically clear the state bits. The flag bit is preserved on signaler/token paths, cleared on the canceler-delivered-via-state path. The actor that wins the CAS calls the appropriate `_core.Set*`. |
| **R4** | After CASing `Inactive → Pending`, the owner re-checks the cancel flag and then the data state. Both re-checks attempt a CAS out of `Pending` if the wake condition holds. |
| **R5** | After registering a `CancellationTokenRegistration` on `_ctr`, the registering actor must re-check `_state` and dispose `_ctr` if the awaiter is no longer in `Pending`. CTR disposal is idempotent. |
| **R6** | `Complete(ex?)` on either side is the terminal publish; no further `Publish` happens after it on the completing TripleBuffer. |
| **R7** | The completion exception is surfaced to the opposite side at most once per pipe (`_exceptionAlreadySurfaced` for the reader; trivial for the writer since `FlushAsync` throws and the user is expected not to retry). |
| **R8** | `AdvanceTo` validates `(consumed, examined)` against `[_totalConsumed, _totalConsumed + _readResultBufferLength]` in absolute byte offsets, rejecting out-of-range positions with `InvalidOperationException`. |
| **R9** | `BuildReadResult` produces `(IsCanceled, IsCompleted)` independently — both flags can be true simultaneously when a sticky cancel is consumed on a post-Complete read. |

## Conventions

- **TryAcquire-once-per-method default.** Each method does at most one `TryAcquire` of each TripleBuffer, ideally near the top. Documented exception: `FlushAsync` acquires `_readerTb` mid-method (after publish) for tighter backpressure responsiveness.
- **No `Reset`.** The lifecycle surface is single-use; the pipe is created, used, both sides `Complete`, then `Dispose`. (Compatible BCL subset, per brainstorm decision Q1=B.)
- **Thread contract.** Strict SPSC for normal ops; `CancelPending*` is the only thread-safe-from-anywhere operation.
