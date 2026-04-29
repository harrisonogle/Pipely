# SPSC Pipe — Design (TripleBuffer-based)

**Date:** 2026-04-25
**Status:** Design (pre-implementation) — round-4 revision (gated for implementation plan)

## Top-level key takeaways

- A lock-free, single-producer single-consumer (SPSC) implementation of `System.IO.Pipelines.PipeReader` / `PipeWriter`, sharing all cross-thread state through two `TripleBuffer<T>` instances rather than a shared mutable linked list.
- Each side publishes a self-consistent monotonic snapshot of its own state (counters + segment cursor + completion); each side reads the other's latest snapshot opportunistically. Snapshot loss is safe because everything is monotonic.
- The writer is the sole mutator of segments and the chain. Reader holds references but never mutates. Recycling is gated on the reader's published `HeadSegment`, not byte counters, eliminating a class of "still-referenced" bugs.
- The awaiter is a single packed `int` per direction. Every state transition is `Interlocked.CompareExchange` or `Interlocked.Or`. Result construction is **stash-and-construct**: the reader stashes its head + tail cursor at park time; the signaler combines the stash with the writer's just-published state to construct the `ReadResult`, then dispatches the continuation via `IContinuationDispatcher` (default = ThreadPool, customizable per-pipe). No thread-affinity constraint on continuations; continuation-thread choice is pluggable.
- Cancel-from-any-thread (`CancelPending*`) is a sticky bit on the awaiter, naturally coalescing per BCL semantics. The cancel-while-parked path constructs the `ReadResult.Buffer` from the stash so the reader observes the same data BCL would have given them.
- BCL surface compatibility: `ReadAsync` / `TryRead` / `FlushAsync` / `Advance` / `AdvanceTo` / `Complete` / `CancelPending*`. Read/flush precedence is **throw-first** (matches BCL `IsCompletedOrThrow`): `Complete(ex)` throws on every subsequent call, taking precedence over both sticky cancel and cancellation-token surfacing.
- **Documented divergences from BCL `Pipe`:**
  - No `Reset` (single-use lifecycle; pipe is created → used → both sides `Complete` → `Dispose`).
  - `IDisposable` added (BCL `Pipe` doesn't implement it; required because there's no `Reset`).
  - `AdvanceTo` argument validation rejects positions past the latest known `TotalWritten`, but does **not** verify positions came from the user's most-recent `ReadResult.Buffer` specifically (BCL does). Catches silent-hang failure mode but not stale-`SequencePosition`-from-recycled-segment corruption.
  - `CancelPending*`-from-third-thread always returns `IsCompleted = false` even if the opposite side has just completed (one-call lag). The next non-cancel call surfaces the correct `IsCompleted` via `TryAcquire`. Only affects the parked-and-cancelled-from-third-thread path; sync-entry sticky-cancel consume sees fresh state via the throw-first `TryAcquire`.
  - `CancelPendingRead` while the reader is parked returns the buffer **as-of park time** (constructed from the awaiter stash), not the current pipe state. BCL constructs from the current committed state. Difference is observable when the writer publishes between the reader's park and the cancel; the data is not lost — it surfaces on the next `ReadAsync`. The non-parked sync-entry sticky-cancel consume *is* BCL-accurate (uses freshly-acquired writer state).

## Section 1 — Architecture overview

### Components

- **`Pipe`** — owns:
  - `_writerTb : TripleBuffer<WriterState>` — written by writer thread, read by reader thread.
  - `_readerTb : TripleBuffer<ReaderState>` — written by reader thread, read by writer thread.
  - `_readAwaiter : PipelyAwaiter<ReadResult>` — woken by writer; awaited by reader. Carries Pattern-2 stash.
  - `_flushAwaiter : PipelyAwaiter<FlushResult>` — woken by reader; awaited by writer. No stash (FlushResult has no buffer).
  - `_options : PipeOptions` — see Section 6.
- **`Pipe.Writer : PipeWriter`** — single producer thread. Owns the segment chain (`_chainHead` → `_writingHead`), a private `BufferSegment` freelist, the `MemoryPool<byte>` reference, and writer-local cursors.
- **`Pipe.Reader : PipeReader`** — single consumer thread. Owns reader-local cursors and the most recently acquired `WriterState`.
- **`PipelyAwaiter<T>`** — wraps `ManualResetValueTaskSourceCore<T>` (configured with `RunContinuationsAsynchronously = false`) with a single packed `int` state field plus a `ParkStash` and a source-side EC-capture stash (`_realContinuation` / `_realState` / `_capturedEC`). Detail in Section 5.
- **`IContinuationDispatcher`** (public interface, §6) — pluggable thread-routing primitive for awaiter continuations. Default = `ThreadPoolContinuationDispatcher` (forwards to `ThreadPool.UnsafeQueueUserWorkItem`). User-supplied implementations let callers escape TP wake-gap latency for high-frequency workloads.

### Local cursors (private to each side)

**Writer-side (touched only by the writer thread):**
- `_chainHead : BufferSegment?` — first segment in the live chain.
- `_writingHead : BufferSegment?` — current tail segment being filled.
- `_writingHeadBytesBuffered : int` — bytes written into `_writingHead` (not yet necessarily published).
- `_totalWritten : long` — monotonic byte counter.
- `_freelistHead : BufferSegment?` — top of writer-private LIFO freelist (chained via `BufferSegment.Next`).
- `_freelistCount : int` — current freelist size; bounded by `_options.MaxFreelistSegments`.
- `_lastPublishedWriterState : WriterState` — read by the reader's signaler under Pattern 2 to construct the `ReadResult`.
- `_lastAcquiredReaderState : ReaderState` — most recent acquire from `_readerTb`.
- `_writerCompleted : bool` — sticky after `Complete`.

**Pipe-level (single field; mutated by `Dispose` only):**
- `_disposed : bool` — sticky after `Dispose`. Every public method's entry guard checks this first and throws `ObjectDisposedException` if set.

**Reader-side (touched only by the reader thread):**
- `_readHead : BufferSegment?`, `_readHeadIdx : int` — head of unconsumed data.
- `_readTail : BufferSegment?`, `_readTailIdx : int` — boundary of the most recently acquired publish; stable until next `TryAcquire`.
- `_totalConsumed : long`, `_totalExamined : long` — monotonic.
- `_lastPublishedReaderState : ReaderState` — read by the writer's signaler under Pattern 2 to construct `FlushResult` (carries `IsCompleted`/`CompletionException` only).
- `_lastAcquiredWriterState : WriterState`.
- `_readerCompleted : bool` — sticky after `Complete`.

### Threading contract (strict SPSC)

- All `Writer` calls (`GetMemory`, `GetSpan`, `Advance`, `FlushAsync`, `Complete`) on a single producer thread.
- All `Reader` calls (`ReadAsync`, `TryRead`, `AdvanceTo`, `Complete`) on a single consumer thread.
- `CancelPendingRead` / `CancelPendingFlush` are explicitly thread-safe (callable from any thread).
- The two threads communicate exclusively via the two `TripleBuffer`s and the two `PipelyAwaiter` state machines. No locks. No shared mutable structures outside those two primitives.
- `WriteAsync`/`CompleteAsync`/`AsStream` and other `PipeReader`/`PipeWriter` extension methods inherit BCL's default implementations on top of the methods above.

### TripleBuffer contract (consolidated; relied on by §1–§6)

The design's correctness depends on these specific guarantees from `TripleBuffer<T>`:

1. **Producer-side mutation only.** Slot data is mutated only by the producer (the side calling `Publish`). Consumers (`TryAcquire` callers) read but don't mutate slot data. Role rotations on `Publish`/`TryAcquire` change which slot is published / consumer / scratch but never touch slot contents.
2. **Release fence on `Publish`.** `Publish` performs `Interlocked.Exchange` on the state field, providing a release fence that publishes all preceding writes (slot contents, segment fields, byte counters) to threads that subsequently observe the publish.
3. **Acquire fence on successful `TryAcquire`.** When `TryAcquire` returns `true`, it has performed `Interlocked.Exchange`, providing an acquire fence such that all writes preceding the matching `Publish` are visible.
4. **Initial-state convention: `dirty = 0`.** The current `TripleBuffer.cs` ctor sets `_state.Value = 1 << 1` (slot 1 published, dirty bit clear). The reader's first `TryAcquire` must return `false` until the writer has called `Publish` at least once. I10 (reader bootstrap safety) depends on this; if TripleBuffer's initial-state convention changes, the bootstrap argument must be re-derived.

These map onto invariants I8 (mutation), I9 (fences), I10 (bootstrap). They are stated here in one place so the spec is self-contained without requiring inspection of `TripleBuffer.cs`.

### Data flow per cycle (steady state)

1. Writer fills tail segment locally (no cross-thread visibility).
2. `FlushAsync`: acquires fresh `ReaderState` to check throw/sticky-cancel; builds `WriterState`, `_writerTb.Publish()`, signals `_readAwaiter`; re-acquires `ReaderState` for backpressure freshness; recycles drained segments; returns or parks on backpressure.
3. `ReadAsync`: `_writerTb.TryAcquire()` and integrate; throw on writer-completion-exception; consume sticky cancel; `ct` check; return synchronously if there's progress; else park.
4. `AdvanceTo`: refreshes writer state via `TryAcquire`; validates positions; updates local cursors; builds `ReaderState`, `_readerTb.Publish()`; signals `_flushAwaiter` **only if backpressure has relieved** — `unconsumed < ResumeWriterThreshold` or reader-completed (the writer cannot re-check the wake condition after `_core.SetResult` because the `await` resumes immediately; signal-side gating is required, not optional).

### Key takeaways for Section 1

- Two TripleBuffers carry all cross-thread state; two awaiters carry all cross-thread wakeups. That's the entire surface area for synchronization.
- Each side has private local cursors so it can build buffers and decisions without re-reading the TBs every operation. `_lastPublished*State` is read by the *other* side's signaler under Pattern 2.
- `AdvanceTo`'s flush-awaiter signal is **gated** by the resume condition (correction to round-1 wording); the unconditional variant is used only by `Reader.Complete`.
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
    public BufferSegment? HeadSegment;     // first segment still alive on the reader's view; null = pre-bootstrap OR Reader.Complete'd
    public long           TotalConsumed;   // monotonic — backpressure denominator
    public long           TotalExamined;   // monotonic
    public bool           IsCompleted;     // sticky once set
    public Exception?     CompletionException;
}
```

`HeadConsumed` was deliberately omitted: derivable as `TotalConsumed - HeadSegment.RunningIndex` and never read by the writer (recycling uses `HeadSegment` reference, backpressure uses `TotalConsumed`).

`HeadSegment = null` is overloaded: it means "pre-bootstrap" if `IsCompleted = false`, and "reader has completed and surrendered the chain" if `IsCompleted = true`. The writer's recycle predicate (Section 3) distinguishes via the `IsCompleted` flag.

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
- `ReaderState.HeadSegment = null` is overloaded: pre-bootstrap (with `IsCompleted=false`) vs. post-`Reader.Complete` (with `IsCompleted=true`).
- The reader's tail-bound is `_readTailIdx`, not `BufferSegment.End`. This single discipline eliminates the head==tail race that bit prior implementations.
- All freeze writes are ordered before the publish; the publish's full-barrier semantics propagate them to the reader.

## Section 3 — Segment ownership, freelist, BufferSegment, reader bootstrap

### Ownership

| Resource | Owner | Other side's access |
|---|---|---|
| Linked list (`Next` pointers) | Writer | Reader walks but never mutates |
| `BufferSegment` objects | Writer (allocated via freelist) | Reader holds references via `_readHead`, `_readTail`, and acquired `WriterState`s |
| `IMemoryOwner<byte>` per segment | Writer (rents from `_options.Pool`) | None directly; reader sees buffer via `BufferSegment.Memory` |
| `IMemoryOwner<byte>` (donated, post-2026-04-28-Append) | Writer (adopted from caller via `Append`) | None directly; reader sees buffer via `BufferSegment.Memory`. Released on recycle (`DisposeOwned`) or `Pipe.Dispose`. See `2026-04-28-spsc-pipe-buffer-ownership-transfer-design.md` |
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

**Buffer-ownership transfer (post-2026-04-28).** `BufferSegment` gained an `IsDonated : bool` field and an `AdoptFrom(IMemoryOwner<byte>, Memory<byte>, long, object)` initializer to support `PipeWriter.Append`'s buffer-ownership-transfer path. See `2026-04-28-spsc-pipe-buffer-ownership-transfer-design.md` §3 for the full definition. The recycle path (this section) branches on `IsDonated`: rented → `PushFreelist` (existing); donated → `DisposeOwned()` and discard.

**Note on `Next` semantics.** `BufferSegment.Next` is overloaded to serve both the live chain (when the segment is in `_chainHead..._writingHead`) and the writer-private freelist (when sitting on the freelist). Its semantics are well-defined only conditional on which list the segment is currently in. Recycling clears `Next` (`RecycleReset`); freelist push sets `Next` to the freelist's previous head; allocation pop reads it; `Freeze` sets `Next` to the new tail.

### Allocation path (writer rents a new tail)

The freelist is a writer-private LIFO stack: head pointer in `_freelistHead : BufferSegment?`, links via `BufferSegment.Next`. Push: `recycled.Next = _freelistHead; _freelistHead = recycled; _freelistCount++`. Pop: `var s = _freelistHead; _freelistHead = s.Next; s.Next = null; _freelistCount--; return s`.

1. Pop from freelist if non-empty *and* the popped segment's `AvailableMemory.Length ≥ sizeHint`.
2. **Freelist behavior on size mismatch (N5).** If the popped segment is too small, dispose it (`DisposeOwned`) and allocate fresh. Avoids stranding small segments at the head; simpler than peek-and-pop-conditionally.
3. Fresh: `new BufferSegment().RentFrom(_options.Pool, max(sizeHint, _options.MinimumSegmentSize), runningIndex)`.
4. Reused: `segment.RecycleReset(runningIndex)`.
5. `runningIndex` for the new tail is computed from the *current* `_writingHead`'s position (`_writingHead.RunningIndex + _writingHead.End` if the existing tail's `End` has been set, or `_writingHead.RunningIndex + _writingHeadBytesBuffered` if computing pre-freeze). The `GetMemory` pseudocode (Section 4) computes `newRI` before calling `Freeze`, using the buffered byte count directly.
6. Wire into chain: freeze old tail with `next = newTail`. Update `_writingHead = newTail`.

**Freelist size cap (N4).** The freelist is bounded at `_options.MaxFreelistSegments` (default 256, matching BCL's `Pipe` segment-pool default). Tracked via `_freelistCount`. When `RecycleDrainedSegments` would push to a full freelist, it calls `DisposeOwned()` on the excess segment instead.

**Note on `MemoryPool<byte>.Rent`.** `pool.Rent(sizeHint)` returns a buffer of length **at least** `sizeHint`, not exactly `sizeHint`. Tests, freelist size comparisons, and any code reasoning about rented capacity must use `>=`, never `==`.

**Exception-safety on allocation failure.** `pool.Rent` may throw `OutOfMemoryException` (or `ArgumentOutOfRangeException` for size-capped pools). The `GetMemory` pseudocode in Section 4 calls `RentSegment` *before* mutating `_writingHead`/`_chainHead`, so an exception leaves writer-private state untouched and the call is safely retryable. Matches BCL `PipeWriter.GetMemory` behavior. An implementer who refactors the order risks leaving the chain in an inconsistent state — exception-safety is sequence-dependent.

### Recycling path (canonical version — see also §6 `Reader.Complete` interaction)

Called at the end of `FlushAsync`, after the writer has refreshed `_lastAcquiredReaderState` via `_readerTb.TryAcquire()`:

```csharp
void RecycleDrainedSegments()
{
    var r = _lastAcquiredReaderState;
    // Pre-bootstrap guard: HeadSegment is null AND reader hasn't completed yet (I10).
    // Post-Reader.Complete: HeadSegment is null AND r.IsCompleted is true → sweep entire chain.
    if (r.HeadSegment is null && !r.IsCompleted) return;

    var readerHead = r.HeadSegment;     // null iff Reader.Complete'd

    while (_chainHead != _writingHead && _chainHead != readerHead)
    {
        var recycled = _chainHead;
        _chainHead   = _chainHead.Next!;

        if (recycled.IsDonated)
        {
            recycled.DisposeOwned();      // foreign owner: release; drop the BufferSegment shell (post-2026-04-28)
        }
        else if (_freelistCount < _options.MaxFreelistSegments)
        {
            PushFreelist(recycled);
            _freelistCount++;
        }
        else
        {
            recycled.DisposeOwned();
        }
    }
}
```

The predicate uses `HeadSegment` reference comparison rather than byte arithmetic. With `readerHead = null` (post-`Reader.Complete`), the loop's `!= readerHead` always holds, so the writer recycles every segment except the active tail.

**Why reference comparison vs byte arithmetic (Nit-4).** The byte-offset form `_lastAcquiredReaderState.TotalConsumed >= _chainHead.RunningIndex + _chainHead.End` has an off-by-one at the boundary `HeadConsumed == End`: a reader at `(S, S.End)` (end of segment but not yet advanced to `S.Next`, because reader hasn't acquired a state with `TailSegment > S` yet) has `TotalConsumed == S.RunningIndex + S.End`, which the byte predicate (with `>=`) would fire on — but `S` is still the reader's `_readHead`. Reference comparison naturally distinguishes "reader-still-on-this-segment" from "reader-moved-off."

### TripleBuffer slot retention pins recycled segments slightly (Nit-6)

After a segment is recycled to the freelist, it is still reachable via the unused TripleBuffer slot of `_writerTb` until the next publish overwrites that slot's `WriterState.HeadSegment` or `TailSegment` reference. Benign — the recycled segment is alive on the freelist anyway — but means the segment's `IMemoryOwner` is pinned for slightly longer than strictly necessary. Not worth additional complexity to address.

### `MemoryPool` integration and segment sizing

- `_options.Pool` (default `MemoryPool<byte>.Shared`) supplies `IMemoryOwner<byte>`s.
- `_options.MinimumSegmentSize` (default 4096) is the floor for `Pool.Rent(sizeHint)` calls.
- Upper bound on segment size is governed by the underlying `MemoryPool<byte>.MaxBufferSize`; we don't impose a separate cap.
- `GetMemory(sizeHint)`: if `sizeHint > 0` and the current tail can't satisfy it, transition to a new tail of size `Max(sizeHint, MinimumSegmentSize)`. Otherwise return remaining capacity in the current tail.
- **Donated segments (post-2026-04-28-Append) bypass `_options.Pool` and `MinimumSegmentSize` entirely.** Size is donor-decided. See `2026-04-28-spsc-pipe-buffer-ownership-transfer-design.md`.

### `Memory<T>` benign-torn-read assumption (Nit-5)

The Section 2 head==tail discipline relies on the layout of `Memory<byte>`'s internal fields (`_object`, `_index`, `_length`) and on `Slice(0, n)` preserving `_object` and `_index`. This is true today but is an implementation detail of the BCL. **Implementation should add a startup-time assertion or boot test** verifying the assumption (e.g., that `pool.Rent(1024).Memory.Slice(0, 100)` has the same `_object` reference and `_index` value as the original) so a future BCL change is caught at boot rather than as a heisenbug.

**Donated segments are immune (post-2026-04-28-Append).** `BufferSegment.AdoptFrom` writes `base.Memory` to the donated slice once at adoption; subsequent `Freeze` calls during chain-link transitions re-write `base.Memory` to the same value (idempotent — `End == AvailableMemory.Length` already). No torn-read concern because both pre and post values are identical. Rented segments retain the existing benign-torn-read property unchanged. See `2026-04-28-spsc-pipe-buffer-ownership-transfer-design.md` §3.3.

### Reader bootstrap

The reader's `_readHead` starts as `null`. On the first successful `_writerTb.TryAcquire()`:

```csharp
_readHead    = acquired.HeadSegment;
_readHeadIdx = 0;
_readTail    = acquired.TailSegment;
_readTailIdx = acquired.TailWritten;
```

After bootstrap, the reader ignores `WriterState.HeadSegment` on subsequent acquires — its local `_readHead` is authoritative.

**Why bootstrap is safe (I10).** The recycle predicate (I6) cannot fire until `_lastAcquiredReaderState.HeadSegment != null` (which requires the reader to have bootstrapped and published) OR `_lastAcquiredReaderState.IsCompleted = true` (which requires `Reader.Complete`). Neither of these can happen pre-bootstrap. So the writer's `_chainHead` does not advance until *after* the reader's first successful read — at the moment of bootstrap, `WriterState.HeadSegment == _chainHead == start of the live chain`.

**Tied to TripleBuffer initial state (R2-14).** The bootstrap argument relies on TripleBuffer's initial state having `dirty = 0` so the reader's first `TryAcquire` returns `false` until the writer has published *something*. The current `TripleBuffer.cs` ctor satisfies this: `_state.Value = 1 << 1` (slot 1 published, dirty bit clear). If TripleBuffer's initial state changed to dirty=1, the reader's first `TryAcquire` would acquire `default(WriterState)` (with `HeadSegment = null`) — which the bootstrap path handles correctly anyway (reader stays unbootstrap'd until next acquire), but the chain of reasoning is brittle. Implementation should treat TripleBuffer's initial-state convention as part of the contract.

Edge cases:
- **Writer completes without writing.** `WriterState{HeadSegment: null, TailSegment: null, IsCompleted: true}`. Reader's first `TryAcquire` gets this; `_readHead` stays null. `ReadResult` is empty with `IsCompleted: true`.
- **Writer publishes first via `FlushAsync`.** `FlushAsync` always publishes (per Section 4), even if `_writingHeadBytesBuffered == 0`. Harmless; reader gets a `WriterState` with `TailWritten = 0` but valid `HeadSegment` once data has been written.

### Lifecycle: cleanup on Dispose

`Pipe.Dispose()` walks the chain and the freelist, calling `BufferSegment.DisposeOwned()` on each to release `IMemoryOwner` rentals. Idempotent: a `_disposed` flag at the top of `Dispose` short-circuits subsequent calls. Also disposes leftover awaiter `_ctr` registrations (R4-1: closes a CTR-rooting leak when a token-cancelled awaiter is never followed by another park to clean up — `Pipe` is otherwise rooted by the `CancellationTokenSource`'s callback list for the lifetime of the CTS).

```csharp
public void Dispose()
{
    if (_disposed) return;
    _disposed = true;
    _readAwaiter._ctr.Dispose();        // R4-1: idempotent; safe on default value
    _flushAwaiter._ctr.Dispose();
    DisposeChain(_chainHead);
    DisposeFreelist(_freelistHead);
}
```

**Precondition:** no operation is currently in flight on either side, **and no `ReadResult.Buffer` references are still held by the user**. The buffer references segments whose `IMemoryOwner` will be released; accessing them after `Dispose` is use-after-free. After `Dispose`, the pipe is unusable: every public method's entry guard checks `_disposed` first (after the `_writerCompleted`/`_readerCompleted` check is the wrong order — `_disposed` should be first). Calls after `Dispose` throw `ObjectDisposedException`. Implementations should add this guard to every public method shown in Section 4 (omitted from the pseudocode samples for brevity, but load-bearing for the contract).

`Pipe` implements `IDisposable`; `BCL.Pipe` does not. This is a documented divergence (justified because we don't expose `Reset` and segments need explicit memory release).

### Key takeaways for Section 3

- Writer is the sole mutator of segments and the chain; reader walks but doesn't mutate.
- Recycling reuses both the `BufferSegment` object and its `IMemoryOwner` — only `Dispose()` (or freelist overflow) releases memory back to the pool.
- The recycle predicate is a single reference comparison: `_chainHead != ReaderState.HeadSegment`, with a pre-bootstrap guard that distinguishes "reader hasn't started" from "reader completed."
- `Reader.Complete` publishes `HeadSegment = null, IsCompleted = true` so the writer sweeps the entire chain on its next `FlushAsync` (see Section 6).
- Reader bootstrap rides on `WriterState.HeadSegment`, used only on first acquire; safety relies on TripleBuffer's initial-state convention.
- `BufferSegment.Next` is overloaded across chain and freelist; semantics are well-defined only conditional on which list the segment is in.

## Section 4 — Hot paths (steady-state pseudocode)

This section covers all public methods. Each method has an entry guard for its side's `_*Completed` flag and follows the **throw-first** precedence for completion-exception handling: writer-completion-exception throws before sticky-cancel consumption, before `ct.IsCancellationRequested`, before normal data path. This matches BCL's `IsCompletedOrThrow` semantics.

**Buffer-ownership transfer (post-2026-04-28).** Section 4 was extended with a `Writer: Append(IMemoryOwner<byte> buffer[, int start, int length])` subsection. Defined fully in `2026-04-28-spsc-pipe-buffer-ownership-transfer-design.md` §4. `Append` is purely writer-thread-local — same publication boundary as `GetMemory`/`Advance`; nothing becomes visible to the reader until the next `FlushAsync`. It interacts cleanly with the rest of Section 4 without modifying any existing pseudocode.

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

    // R2-3 throw-first: refresh reader state, throw if reader-completed-with-ex.
    if (_readerTb.TryAcquire())                       // I9 acquire fence
        _lastAcquiredReaderState = _readerTb.ConsumerSlot();

    if (_lastAcquiredReaderState.IsCompleted && _lastAcquiredReaderState.CompletionException != null)
        ExceptionDispatchInfo.Throw(_lastAcquiredReaderState.CompletionException);    // R7 (Option A)

    // R2-2 sync entry: consume sticky CancelPendingFlush flag.
    while (true)
    {
        int oldV = _flushAwaiter._state;
        if ((oldV & PipelyAwaiter.CancelFlag) == 0) break;
        int desired = oldV & ~PipelyAwaiter.CancelFlag;
        if (Interlocked.CompareExchange(ref _flushAwaiter._state, desired, oldV) == oldV)
            return new ValueTask<FlushResult>(BuildFlushResult(isCanceled: true));
    }

    if (ct.IsCancellationRequested)
        return ValueTask.FromCanceled<FlushResult>(ct);

    // Build state, publish, signal reader.
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

    // Note: FlushAsync publishes and signals unconditionally even when no bytes have been written
    // since the last flush (zero-byte flush). May cause a spurious reader wakeup; the reader handles
    // it correctly (returns ReadResult with no new bytes; reader re-parks). Optional optimization:
    // skip publish+signal when _writingHeadBytesBuffered is unchanged. BCL guards via _unflushedBytes > 0.
    SignalReadAwaiterIfPending();                     // R1

    // Re-acquire reader state for backpressure freshness (catches reader's response to our signal).
    if (_readerTb.TryAcquire())
        _lastAcquiredReaderState = _readerTb.ConsumerSlot();
    RecycleDrainedSegments();                         // I6

    // Re-check reader-completion-exception (reader may have completed in response to our publish).
    if (_lastAcquiredReaderState.IsCompleted && _lastAcquiredReaderState.CompletionException != null)
        ExceptionDispatchInfo.Throw(_lastAcquiredReaderState.CompletionException);

    long unconsumed = _totalWritten - _lastAcquiredReaderState.TotalConsumed;
    bool readerDone = _lastAcquiredReaderState.IsCompleted;
    bool needsPark = _options.PauseWriterThreshold > 0
                     && unconsumed >= _options.PauseWriterThreshold     // S1: BCL-match >=
                     && !readerDone;                                    // M2

    if (!needsPark)
        return new ValueTask<FlushResult>(BuildFlushResult(isCanceled: false));

    return ParkFlushAwaiter(ct);                       // see Section 5
}

FlushResult BuildFlushResult(bool isCanceled)
{
    return new FlushResult(isCanceled, isCompleted: _lastAcquiredReaderState.IsCompleted);
}
```

The two `_readerTb.TryAcquire()` calls are intentional: the first gives a fresh state for the throw/sticky-cancel/`ct` checks; the second catches any reader response to the just-emitted signal, for tighter backpressure decisions. Each `TryAcquire` is cheap (`Volatile.Read`; `Interlocked.Exchange` only if dirty).

### Reader: `ReadAsync(CancellationToken ct)` (synchronous fast path)

```csharp
ValueTask<ReadResult> ReadAsync(CancellationToken ct)
{
    if (_readerCompleted) throw new InvalidOperationException("Reading is completed.");    // S8

    // R2-3 throw-first: refresh writer state, throw if writer-completed-with-ex.
    if (_writerTb.TryAcquire())                       // I9 acquire fence
    {
        _lastAcquiredWriterState = _writerTb.ConsumerSlot();
        IntegrateAcquiredWriterState();
    }

    if (_lastAcquiredWriterState.IsCompleted && _lastAcquiredWriterState.CompletionException != null)
        ExceptionDispatchInfo.Throw(_lastAcquiredWriterState.CompletionException);    // R7 (Option A)

    // Sync entry: consume sticky CancelPendingRead flag.
    while (true)
    {
        int oldV = _readAwaiter._state;
        if ((oldV & PipelyAwaiter.CancelFlag) == 0) break;
        int desired = oldV & ~PipelyAwaiter.CancelFlag;
        if (Interlocked.CompareExchange(ref _readAwaiter._state, desired, oldV) == oldV)
            return new ValueTask<ReadResult>(BuildReadResult(isCanceled: true));
    }

    if (ct.IsCancellationRequested)
        return ValueTask.FromCanceled<ReadResult>(ct);

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

**Precedence at sync entry (R9 revised):** writer-completion-exception throws first (matches BCL `IsCompletedOrThrow`); then sticky cancel consumes; then `ct.IsCancellationRequested` fires; then normal data/IsCompleted path. Under M4 Option A, after `Writer.Complete(ex)` every call throws — sticky cancel is "dropped" (flag remains set forever; never observable). Acceptable: BCL-strict.

### Reader: `TryRead(out ReadResult result)` (S7)

```csharp
bool TryRead(out ReadResult result)
{
    if (_readerCompleted) throw new InvalidOperationException("Reading is completed.");    // S8

    if (_writerTb.TryAcquire())
    {
        _lastAcquiredWriterState = _writerTb.ConsumerSlot();
        IntegrateAcquiredWriterState();
    }

    if (_lastAcquiredWriterState.IsCompleted && _lastAcquiredWriterState.CompletionException != null)
        ExceptionDispatchInfo.Throw(_lastAcquiredWriterState.CompletionException);

    // Sync entry: consume sticky cancel flag.
    while (true)
    {
        int oldV = _readAwaiter._state;
        if ((oldV & PipelyAwaiter.CancelFlag) == 0) break;
        int desired = oldV & ~PipelyAwaiter.CancelFlag;
        if (Interlocked.CompareExchange(ref _readAwaiter._state, desired, oldV) == oldV)
        {
            result = BuildReadResult(isCanceled: true);
            return true;
        }
    }

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

    // R2-7: refresh writer state for upper-bound validation. Catches Pattern-2-delivered buffers
    // whose tail is past _lastAcquiredWriterState.TailSegment (signaler used a fresher publish).
    if (_writerTb.TryAcquire())
    {
        _lastAcquiredWriterState = _writerTb.ConsumerSlot();
        IntegrateAcquiredWriterState();
    }

    // R8 revised: monotonicity + upper-bound against latest known TotalWritten.
    if (consumedAbs < _totalConsumed
        || examinedAbs < _totalExamined
        || consumedAbs > examinedAbs
        || examinedAbs > _lastAcquiredWriterState.TotalWritten)
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

    SignalFlushIfBackpressureRelieved();              // R1; gated per R2-1
}
```

### Reader: `BuildReadResult` (R9 — both flags can be set)

```csharp
// Call sites:
//   - ReadAsync sync entry: data return AND sticky-cancel consume.
//   - TryRead sync entry: data return AND sticky-cancel consume.
//   - ParkReadAwaiter lost-wakeup re-check (data return) AND lost-cancel re-check (canceled return).
// NOT used by SignalReadAwaiterIfPending — the signaler constructs its result inline using stash + _lastPublishedWriterState.
ReadResult BuildReadResult(bool isCanceled)
{
    bool isCompleted = _lastAcquiredWriterState.IsCompleted;
    // R3-1 (BCL parity): construct the buffer from local cursors regardless of `isCanceled`.
    // BCL Pipe.GetReadResult builds the ROS unconditionally; `isCanceled` only affects the result flag.
    // This also matches the cross-thread CancelPendingRead path (which builds from stash) — both
    // cancel paths now return the data the reader has, not an empty buffer.
    var buffer = _readHead == null
        ? ReadOnlySequence<byte>.Empty
        : new ReadOnlySequence<byte>(_readHead, _readHeadIdx, _readTail!, _readTailIdx);   // I3, I7
    return new ReadResult(buffer, isCanceled, isCompleted);
}
```

`isCompleted` reflects writer-completion alone — there is no `_totalExamined >= TotalWritten` gate. Under Option A (M4), writer-completion-with-exception throws at the entry guard and never reaches `BuildReadResult`. For `Writer.Complete(null)`, the reader sees `IsCompleted = true` from the moment it acquires the completed `WriterState` — buffer may still be non-empty (drain proceeds normally for non-faulted completion).

### Conventions

- **TryAcquire-once-per-method default with documented exceptions.** `FlushAsync` does two `_readerTb.TryAcquire`s (top for throw/cancel checks; mid-method for backpressure freshness). `AdvanceTo` does one `_writerTb.TryAcquire` at the top, then `SignalFlushIfBackpressureRelieved` may do another (both reader-thread-local; second is fast-path-skipped if no parked writer).
- **R1 (publish-before-signal).** Any code path that both publishes a state and signals the other side's awaiter publishes first, signals second.
- **R8 (AdvanceTo validation, revised).** Monotonicity + upper-bound against the latest known `_lastAcquiredWriterState.TotalWritten` (after `TryAcquire`). Catches the silent-hang failure mode (`examined > TotalWritten`). Does not catch stale-`SequencePosition`-from-recycled-segment corruption — documented BCL divergence.

### Key takeaways for Section 4

- `GetMemory` / `Advance` are purely writer-local — no fences, no TB ops. All visibility happens at `FlushAsync`.
- `FlushAsync` ordering: throw-first (TryAcquire → throw) → sticky cancel consume → `ct` check → publish → signal → re-acquire → recycle → throw re-check → backpressure check.
- `ReadAsync` ordering: throw-first (TryAcquire → integrate → throw) → sticky cancel consume → `ct` check → return-or-park.
- `AdvanceTo` is O(1) via `RunningIndex`; refreshes writer state for validation; signals flush awaiter only if backpressure has relieved.
- Every public method has an entry guard for its side's `_*Completed` flag.
- Reader's tail-bound (`_readTailIdx`) is set only when integrating a freshly-acquired `WriterState` — the single point where I3 is established for each subsequent `ReadResult`.

## Section 5 — Awaiter coordination

### Awaiter shape (single packed `int` + Pattern 2 stash)

```csharp
internal sealed class PipelyAwaiter<T> : IValueTaskSource<T>
{
    public ManualResetValueTaskSourceCore<T> _core;
    public int _state;                          // packed: bit 0 = state, bit 1 = cancel flag
    public CancellationTokenRegistration _ctr;
    public CancellationToken _token;            // cached for OCE construction; set at park, read by token callback

    // Stash for Pattern 2. Used by PipelyAwaiter<ReadResult>; ignored by PipelyAwaiter<FlushResult>
    // (FlushResult has no buffer; ~24 bytes wasted per pipe — implementation may split the type
    // into two non-generic awaiter classes if the savings matter).
    // Set by reader at park time, read by signaler/canceler after CASing out of Pending.
    public BufferSegment? _stashHead;
    public int            _stashHeadIdx;
    public BufferSegment? _stashTail;       // R2-5: extended for cancel-while-parked buffer reconstruction
    public int            _stashTailIdx;

    // Source-side EC-capture stash. Set by PipelyAwaiter.OnCompleted on the consumer's thread
    // BEFORE delegating to _core.OnCompleted (visible by the time s_dispatch reads them, in
    // both the OnCompleted-fires-first and SetResult-fires-first races; see §4 publication
    // ordering in the source-side EC-capture spec). Read by s_invokeWithEc on the dispatcher's
    // chosen thread; cleared after read.
    private Action<object?>? _realContinuation;
    private object?          _realState;
    private ExecutionContext? _capturedEC;

    // Worker-thread-only scratch fields for the allocation-free ExecutionContext.Run pattern.
    private Action<object?>? _runCb;
    private object? _runState;

    // Dispatcher for routing continuations to the consumer's chosen thread.
    private readonly IContinuationDispatcher _dispatcher;

    public const int Inactive   = 0b00;
    public const int Pending    = 0b01;
    public const int StateMask  = 0b01;
    public const int CancelFlag = 0b10;

    public PipelyAwaiter(IContinuationDispatcher dispatcher) => _dispatcher = dispatcher;

    public short Version => _core.Version;
    public T GetResult(short token) => _core.GetResult(token);
    public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);
    // OnCompleted: captures EC, forwards (s_dispatch, this) to _core with FlowExecutionContext+UseSchedulingContext stripped — full body in source-side EC-capture spec §2.2.
}
```

Two states (`Inactive` / `Pending`) suffice; "how was this completed" lives in `_core`'s status. Every `_state` mutation is `Interlocked.CompareExchange` or `Interlocked.Or`. (Note: `public` fields with leading underscore is unusual for C# — implementation may make them `internal` with `InternalsVisibleTo` to `Pipe`, or merge the classes; cosmetic.)

**Field-access discipline.** `_state` is the only field touched by all four actors (owner / signaler / canceler / token callback). Other fields:
- `_token` is written by the owner before CAS Inactive→Pending; read by the token callback after CAS Pending→Inactive succeeds. Synchronization rides on `_state`'s CAS. Field reuse across cycles is safe because `CancellationTokenRegistration.Dispose()` blocks on in-flight callbacks, so the prior cycle's callback completes before the next cycle's `_token = ct` write.
- `_ctr` is written by the owner *after* CAS Inactive→Pending. **Important: the CAS does NOT release-publish `_ctr`** — there is no synchronizes-with edge between the owner's `_ctr = …` write and a signaler/canceler that wins CAS Pending→Inactive between the owner's CAS and the owner's `_ctr` write. Safety rests on two distinct properties: (a) `CancellationTokenRegistration` is a small struct and `Dispose()` is **idempotent and tolerant of default/torn input** — disposing a default or partially-published value at worst no-ops; (b) the owner's R5 re-check (`if (_state != Pending) _ctr.Dispose()`) ensures the owner disposes the by-then-fully-written value if a signaler/canceler beat it to the CAS. This guarantees at-least-once Dispose. The R5b (start-of-next-park dispose) handles the orthogonal token-callback-wins case where the callback won the CAS but didn't dispose. Future maintainers must not "optimize away" the R5 re-check on the assumption that `_state` publishes `_ctr` — it doesn't.
- Stash fields (`_stashHead`, `_stashHeadIdx`, `_stashTail`, `_stashTailIdx`) are written by the owner *before* CAS Inactive→Pending; read by the signaler or canceler after CAS Pending→Inactive succeeds. The CAS's release/acquire ordering does cover these. Stash fields are not cleared after read; they retain references to BufferSegments until the next park overwrites them (one-cycle pin, parallel to TripleBuffer slot retention; benign).
- Source-side EC stash fields (`_realContinuation`, `_realState`, `_capturedEC`) are written by `PipelyAwaiter.OnCompleted` on the **consumer's thread** before delegating to `_core.OnCompleted`. They are read by `s_invokeWithEc` on the **dispatcher's chosen thread** (after `s_dispatch` queued the work item via `IContinuationDispatcher.UnsafeQueueUserWorkItem`). Visibility chain: `OnCompleted`'s `Volatile.Write` ⟹ `_core.OnCompleted`'s register/queue ⟹ (in the OnCompleted-fires-first race) `_core.SetResult`/`SetException` invoking `s_dispatch` inline ⟹ dispatcher's `UnsafeQueueUserWorkItem` happens-before to dequeued `s_invokeWithEc`; or (in the SetResult-fires-first race) `MRVTSC` queues `s_dispatch` to TP, TP dequeue HB to dispatcher's queue, dispatcher's HB to `s_invokeWithEc`. Either path makes the writes visible to the reads. `s_invokeWithEc` clears the fields after reading (hygiene — releases references to delivered `ReadResult`/exception/EC between park cycles).

**On Pattern 2 (stash-and-construct).** The signaler runs on the writer thread (for the read awaiter); constructing a `ReadResult` requires reader-private cursors per I3. To avoid Pattern 1's thread-affinity constraint, the reader stashes its full cursor (head + tail) at park time. The signaler combines the stash with `_lastPublishedWriterState` (writer-private, freshest just before signaling) to construct the `ReadResult` and hand it off via the stash-and-dispatch sequence (see `### Continuation dispatch` below; per R10). The canceler-while-parked path uses the same stash to construct a buffer reflecting **park-time** contents — bytes the writer publishes between the reader's park and the cancel-from-third-thread are *not* in the cancel result; they surface on the next `ReadAsync` (documented top-level divergence from BCL, which constructs from current committed state). The stash is also not refreshed by `ParkReadAwaiter`'s lost-wakeup integrate; refreshing it would introduce a torn-stash window for a concurrent canceler.

### Continuation dispatch

`_core.RunContinuationsAsynchronously` is set to `false` **once at construction** (not per-`Reset` — `ManualResetValueTaskSourceCore<T>.Reset` does not reset this flag). With RCA = false, `_core.SetResult` / `_core.SetException` invokes the registered continuation **inline on whatever thread calls it**. Pipe never calls `_core.SetResult` / `_core.SetException` directly from a signaler / canceler / token-callback / park re-check; instead, every signal-path site that decides "the parked awaiter is being released" hands off through the configured `IContinuationDispatcher`. This dispatcher hop replaces MRVTSC's internal TP queueing (which RCA = true would otherwise perform). The default dispatcher forwards to `ThreadPool.UnsafeQueueUserWorkItem`, preserving the prior RCA = true configuration's observable behavior: no inline execution on the signaler thread, no unbounded stack depth, no reentrancy hazard under bursty workloads. A custom dispatcher can route the callback to a user-controlled thread (see §6 `IContinuationDispatcher`).

The protocol is:

1. **Consumer's `await` reaches `OnCompleted`.** `PipelyAwaiter.OnCompleted` runs on the consumer's thread, captures `ExecutionContext` (gated by `FlowExecutionContext`), writes `_realContinuation` / `_realState` / `_capturedEC` via `Volatile.Write`, and forwards `(s_dispatch, this, token, flags & ~suppressed)` to `_core.OnCompleted` — where `suppressed = FlowExecutionContext | UseSchedulingContext`.
2. **Producer signals.** Some actor (signaler / canceler / token callback / park re-check) wins CAS Pending→Inactive and calls `_xxxAwaiter._core.SetResult(value)` or `_core.SetException(ex)` directly on the producer's thread (no stashing).
3. **MRVTSC dispatches.** `_core.RunContinuationsAsynchronously = false`, so `_core.SetResult` / `_core.SetException` invokes the registered `s_dispatch` callback inline on the producer's thread.
4. **`s_dispatch` routes through the dispatcher.** It reads `awaiter._dispatcher` (set at construction) and calls `dispatcher.UnsafeQueueUserWorkItem(s_invokeWithEc, awaiter)`. The dispatcher hands the work item to its chosen thread.
5. **`s_invokeWithEc` runs the continuation.** It reads and clears `_realContinuation` / `_realState` / `_capturedEC`. If `_capturedEC` is non-null, it stages cont/state via worker-thread-only scratch fields (`_runCb`, `_runState`) and calls `ExecutionContext.Run(_capturedEC, s_runContinuation, awaiter)`. If null (consumer suppressed flow), it invokes the continuation directly.

In the **`SetResult`-fires-first race** — producer signals before consumer has called `OnCompleted` — `MRVTSC` records the result, finds `_continuation` null, and queues `s_dispatch` to the ThreadPool when `OnCompleted` is later called. The TP-dispatched `s_dispatch` then performs steps 4-5 above. Correctness rests on publication ordering in `OnCompleted`: `_realContinuation` / `_realState` / `_capturedEC` are written via `Volatile.Write` BEFORE `_core.OnCompleted`, so the TP-dispatched `s_dispatch` reads them post-publication.

The two static delegates (per `PipelyAwaiter<T>`'s type parameter) are:

- `s_dispatch` — registered with `_core.OnCompleted`; routes through the dispatcher.
- `s_invokeWithEc` — registered with the dispatcher; applies EC and invokes the real continuation.

Plus a `ContextCallback` (`s_runContinuation`) used by `ExecutionContext.Run` inside `s_invokeWithEc` for the allocation-free pattern.

Lost-wakeup re-check paths inside `ParkReadAwaiter` / `ParkFlushAwaiter` that **don't** go through `_core.Set*` (the data-return paths that build a `ValueTask<T>` directly via `BuildReadResult` / `BuildFlushResult`) are *not* dispatched — there is no continuation, the result is returned synchronously to the caller's await machinery.

**Allocation cost.** Zero allocations per dispatch: `s_dispatch` and `s_invokeWithEc` are allocated once at type-init; `_realContinuation` / `_realState` / `_capturedEC` are direct field writes on the existing awaiter object; the dispatcher receives `(Action<object?>, object?)` with `state = awaiter`. The `ExecutionContext.Run` invocation in `s_invokeWithEc` uses worker-thread-only scratch fields (`_runCb` / `_runState`) plus the static `s_runContinuation` `ContextCallback`, so no per-dispatch closure or tuple is allocated. Per-dispatch overhead vs. the prior wiring: one extra virtual call into the dispatcher (~1-2 ns); should be invisible in throughput benchmarks.

**Source-side EC capture / apply discipline (I16).** ExecutionContext capture for await continuations occurs in `PipelyAwaiter.OnCompleted` on the **consumer's** thread, gated by `FlowExecutionContext`. The capture is stored on the awaiter (`_capturedEC`) and applied in `s_invokeWithEc` via `ExecutionContext.Run` on the dispatcher's chosen thread. `IContinuationDispatcher` implementations MUST NOT capture or apply an ExecutionContext themselves; their role is purely to route the callback to a thread. Implementations using `ThreadPool.UnsafeQueueUserWorkItem` satisfy this trivially; implementations using `ThreadPool.QueueUserWorkItem` or `Task.Run` capture EC redundantly (wasteful, but does not break the consumer's EC because `s_invokeWithEc` applies the source-side captured EC anyway). The contract is convention rather than statically enforced; tests should include an EC-leak guard against a "buggy" dispatcher that captures EC.

**SynchronizationContext / TaskScheduler interaction.** See §3.3 of the source-side EC-capture spec for the public contract — both SC and TaskScheduler captured by the consumer's await are stripped from the flags forwarded to `_core.OnCompleted`, so the dispatcher's chosen thread is always honored.

### Cross-thread visibility

**Cross-thread visibility of `_lastPublishedWriterState` under Pattern 2.** The reader's signaler runs on the writer thread; it reads `_lastPublishedWriterState` (a writer-private field set on the writer thread, immediately before `SignalReadAwaiterIfPending` is called). Since both writes are on the writer thread, no cross-thread synchronization is needed for that read. The constructed `ReadResult` is then stashed on the awaiter and dispatched; the dispatched callback's `_core.SetResult` provides the release/acquire fence to the reader's continuation thread (regardless of which thread the dispatcher routes the callback to).

**Symmetric for `_lastPublishedReaderState`.** The writer's signaler (called from `AdvanceTo` via `SignalFlushIfBackpressureRelieved` and from `Reader.Complete` via `SignalFlushAwaiterIfPending`) runs on the **reader** thread; it reads `_lastPublishedReaderState`, which is reader-private and was set on the reader thread immediately before the signaler call. No cross-thread sync needed for that read. The constructed `FlushResult` is then stashed on the awaiter and dispatched; the dispatched callback's `_core.SetResult` provides release/acquire to the writer's continuation. Future maintainers adding additional signaler call sites must place them on the reader thread to preserve this property.

**Cancel-via-stash transitive-fence visibility.** The cross-thread `CancelPendingRead` path (canceler on a third thread) constructs an ROS from segments reachable through the stash. Visibility chain: writer's freeze writes → (writer's `Publish` release fence) → reader's `TryAcquire` acquire fence → reader's stash writes (same thread, sequenced) → reader's CAS Inactive→Pending release → canceler's CAS Pending→Inactive acquire → canceler's reads of stash + segment fields → canceler's `_core.SetResult` call → `MRVTSC` invokes `s_dispatch` inline → `s_dispatch`'s call to `dispatcher.UnsafeQueueUserWorkItem` (which provides a happens-before edge into the dispatched callback) → canceler's _core.SetResult release (which invokes s_dispatch inline; s_dispatch enqueues s_invokeWithEc on the dispatcher; s_invokeWithEc invokes the user continuation under the captured EC) → continuation's `GetResult` acquire. All segment fields the canceler reads (`RunningIndex`, `Memory`, etc.) were established by the writer's freeze before the writer published, and the chain transitively carries the writes through the canceler thread, the dispatcher's hand-off, and into the continuation.

**`Interlocked.Or` portability note.** `Interlocked.Or(ref int, int)` was added in .NET 7; this project targets net10.0, so it's available. If back-porting to older targets, fall back to a CAS loop.

### Reader's park / wake

```csharp
ValueTask<ReadResult> ParkReadAwaiter(CancellationToken ct)
{
    // R2-11: clean up any leftover CTR from a prior cycle (token-callback-wins case).
    // CancellationTokenRegistration.Dispose is idempotent and safe on default value.
    _readAwaiter._ctr.Dispose();

    _readAwaiter._core.Reset();
    _readAwaiter._token = ct;

    // Pattern 2 stash: written before the CAS to Pending; signaler reads after CAS Pending→Inactive.
    _readAwaiter._stashHead    = _readHead;
    _readAwaiter._stashHeadIdx = _readHeadIdx;
    _readAwaiter._stashTail    = _readTail;
    _readAwaiter._stashTailIdx = _readTailIdx;

    // 1. CAS Inactive → Pending, preserving any flag set concurrently.
    while (true)
    {
        int oldV = _readAwaiter._state;
        Debug.Assert((oldV & StateMask) == Inactive, "SPSC violation: concurrent ReadAsync");
        int desired = (oldV & CancelFlag) | Pending;
        if (Interlocked.CompareExchange(ref _readAwaiter._state, desired, oldV) == oldV)
            break;
    }

    // 2. Lost-wakeup re-check — throw-first per R2-3.
    if (_writerTb.TryAcquire())
    {
        _lastAcquiredWriterState = _writerTb.ConsumerSlot();
        IntegrateAcquiredWriterState();

        if (_lastAcquiredWriterState.IsCompleted && _lastAcquiredWriterState.CompletionException != null)
        {
            // Un-park and deliver exception via SetException.
            while (true)
            {
                int oldV = _readAwaiter._state;
                if ((oldV & StateMask) != Pending) break;
                int desired = oldV & ~StateMask;
                if (Interlocked.CompareExchange(ref _readAwaiter._state, desired, oldV) == oldV)
                {
                    // Shorthand: stash exception + dispatch via IContinuationDispatcher (see "Continuation dispatch" above).
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
                    // Sync data return — no continuation, no dispatcher hop.
                    return new ValueTask<ReadResult>(BuildReadResult(isCanceled: false));
            }
        }
    }

    // 3. Lost-cancel re-check (R4).
    int v = _readAwaiter._state;
    if ((v & CancelFlag) != 0
        && Interlocked.CompareExchange(ref _readAwaiter._state, Inactive, Pending | CancelFlag) == (Pending | CancelFlag))
    {
        // Shorthand: stash canceled ReadResult + dispatch via IContinuationDispatcher.
        _readAwaiter._core.SetResult(BuildReadResult(isCanceled: true));
        return new ValueTask<ReadResult>(_readAwaiter, _readAwaiter.Version);
    }

    // 4. Register token; clean up CTR if someone completed during register (R5).
    _readAwaiter._ctr = ct.UnsafeRegister(static p => ((Pipe)p!).OnReadAwaiterTokenCancel(), this);
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

            // Throw-first: writer-completed-with-ex delivered as exception.
            if (w.IsCompleted && w.CompletionException != null)
            {
                // Shorthand: stash exception + dispatch via IContinuationDispatcher.
                _readAwaiter._core.SetException(w.CompletionException);
                return;
            }

            var head    = _readAwaiter._stashHead ?? w.HeadSegment;     // bootstrap fallback
            var headIdx = _readAwaiter._stashHead == null ? 0 : _readAwaiter._stashHeadIdx;

            var buffer = head == null
                ? ReadOnlySequence<byte>.Empty
                : new ReadOnlySequence<byte>(head, headIdx, w.TailSegment!, w.TailWritten);

            var result = new ReadResult(buffer, isCanceled: false, isCompleted: w.IsCompleted);
            // Shorthand: stash result + dispatch via IContinuationDispatcher.
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
            // Don't dispose _ctr here (running inside it); next ParkReadAwaiter cleans up.
            // Shorthand: stash OCE + dispatch via IContinuationDispatcher.
            _readAwaiter._core.SetException(new OperationCanceledException(_readAwaiter._token));
            return;
        }
    }
}
```

**Note on Pattern 2 cursor lag.** After a Pattern-2-delivered ReadResult, the reader's local `_readTail`/`_readTailIdx` may briefly lag the buffer the user just received — the buffer was constructed with the writer's just-published `TailSegment`/`TailWritten`, but reader's local fields aren't updated by the signaler. The signaler does **not** update `_readHead` either; that's the user's responsibility via `AdvanceTo`. AdvanceTo's top-of-method `TryAcquire` (R2-7) refreshes `_readTail`/`_readTailIdx` on the next reader call. Sticky-cancel sync return after a Pattern-2 delivery similarly refreshes via the entry-side `TryAcquire` (R2-3 throw-first ordering puts TryAcquire before sticky-cancel consumption).

**Bootstrap-via-signaler delays full bootstrap until first `AdvanceTo`.** If the reader's first read hits the Pattern-2 signaler path (rather than the synchronous `TryAcquire`-and-integrate path), `_readHead` remains null until the user's `AdvanceTo` extracts the head segment from the delivered buffer's `consumed` position. During that window, the reader's published `_lastPublishedReaderState.HeadSegment` is also null — but `IsCompleted = false`, so the writer's recycle predicate's pre-bootstrap guard (`if (HeadSegment is null && !IsCompleted) return;`) holds, blocking recycling. Self-correcting: once the user calls `AdvanceTo`, `_readHead` is set from `consumed.GetObject()` and the next `PublishReaderState` carries a non-null `HeadSegment`.

**Lost-wakeup integrate variant of the same case.** `ParkReadAwaiter`'s lost-wakeup re-check (step 2) calls `IntegrateAcquiredWriterState`. If `_readHead` was null pre-park (first read) and the integrate sets `_readHead` from the just-acquired `WriterState.HeadSegment` but the un-park condition (`HasReadableProgress` / `IsCompleted` / writer-completion-exception) does not fire, `_readHead` becomes non-null while the reader stays parked. The reader's published `_lastPublishedReaderState.HeadSegment` is still null (no `PublishReaderState` happens during park), so the same pre-bootstrap recycle guard protects the segment.

A future maintainer must not "fix" either recycle guard variant to allow null `HeadSegment` without `IsCompleted`, or both safety properties break.

### Writer's park / wake

Symmetric to the reader's park/wake. Differences: no Pattern-2 buffer construction (FlushResult has no buffer; signaler reads `_lastPublishedReaderState.IsCompleted`/`CompletionException` only); wake condition is `unconsumed < ResumeWriterThreshold` (BCL hysteresis, S1) OR reader-completed (M2).

```csharp
ValueTask<FlushResult> ParkFlushAwaiter(CancellationToken ct)
{
    _flushAwaiter._ctr.Dispose();    // R2-11 cleanup
    _flushAwaiter._core.Reset();
    _flushAwaiter._token = ct;

    // 1. CAS Inactive → Pending.
    while (true)
    {
        int oldV = _flushAwaiter._state;
        Debug.Assert((oldV & StateMask) == Inactive, "SPSC violation: concurrent FlushAsync");
        int desired = (oldV & CancelFlag) | Pending;
        if (Interlocked.CompareExchange(ref _flushAwaiter._state, desired, oldV) == oldV)
            break;
    }

    // 2. Lost-wakeup re-check — throw-first.
    if (_readerTb.TryAcquire())
    {
        _lastAcquiredReaderState = _readerTb.ConsumerSlot();

        if (_lastAcquiredReaderState.IsCompleted && _lastAcquiredReaderState.CompletionException != null)
        {
            while (true)
            {
                int oldV = _flushAwaiter._state;
                if ((oldV & StateMask) != Pending) break;
                int desired = oldV & ~StateMask;
                if (Interlocked.CompareExchange(ref _flushAwaiter._state, desired, oldV) == oldV)
                {
                    // Shorthand: stash exception + dispatch via IContinuationDispatcher.
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
                    // Sync data return — no continuation, no dispatcher hop.
                    return new ValueTask<FlushResult>(BuildFlushResult(isCanceled: false));
            }
        }
    }

    // 3. Lost-cancel re-check.
    int v = _flushAwaiter._state;
    if ((v & CancelFlag) != 0
        && Interlocked.CompareExchange(ref _flushAwaiter._state, Inactive, Pending | CancelFlag) == (Pending | CancelFlag))
    {
        // Shorthand: stash canceled FlushResult + dispatch via IContinuationDispatcher.
        _flushAwaiter._core.SetResult(BuildFlushResult(isCanceled: true));
        return new ValueTask<FlushResult>(_flushAwaiter, _flushAwaiter.Version);
    }

    // 4. Register token.
    _flushAwaiter._ctr = ct.UnsafeRegister(static p => ((Pipe)p!).OnFlushAwaiterTokenCancel(), this);
    if ((_flushAwaiter._state & StateMask) != Pending)
        _flushAwaiter._ctr.Dispose();
    return new ValueTask<FlushResult>(_flushAwaiter, _flushAwaiter.Version);
}

// R2-1: gated signaler — only wakes the parked writer when backpressure has relieved.
// Called from AdvanceTo. This is the critical fix for round-1's S2 mis-disposition
// (the writer cannot re-check the wake condition after _core.SetResult).
//
// Side effect: the inner TryAcquire mutates _readTail/_readTailIdx via IntegrateAcquiredWriterState.
// Benign — keeps reader's view of the writer's tail fresh as a no-op-or-better.
void SignalFlushIfBackpressureRelieved()
{
    // Fast path: no parked writer.
    if ((_flushAwaiter._state & PipelyAwaiter.StateMask) != PipelyAwaiter.Pending) return;

    // Refresh writer state to compute unconsumed accurately.
    if (_writerTb.TryAcquire())
    {
        _lastAcquiredWriterState = _writerTb.ConsumerSlot();
        IntegrateAcquiredWriterState();
    }

    long unconsumed = _lastAcquiredWriterState.TotalWritten - _totalConsumed;
    // Note: no `|| _readerCompleted` clause — AdvanceTo's entry guard throws if _readerCompleted,
    // so this code path never runs post-completion. Reader.Complete uses SignalFlushAwaiterIfPending (unconditional).
    if (unconsumed >= _options.ResumeWriterThreshold) return;

    while (true)
    {
        int oldV = _flushAwaiter._state;
        if ((oldV & PipelyAwaiter.StateMask) != PipelyAwaiter.Pending) return;
        int desired = oldV & ~PipelyAwaiter.StateMask;
        if (Interlocked.CompareExchange(ref _flushAwaiter._state, desired, oldV) == oldV)
        {
            _flushAwaiter._ctr.Dispose();
            DeliverFlushResult();
            return;
        }
    }
}

// Unconditional signaler — used by Reader.Complete only (completion is always a wake reason).
void SignalFlushAwaiterIfPending()
{
    while (true)
    {
        int oldV = _flushAwaiter._state;
        if ((oldV & PipelyAwaiter.StateMask) != PipelyAwaiter.Pending) return;
        int desired = oldV & ~PipelyAwaiter.StateMask;
        if (Interlocked.CompareExchange(ref _flushAwaiter._state, desired, oldV) == oldV)
        {
            _flushAwaiter._ctr.Dispose();
            DeliverFlushResult();
            return;
        }
    }
}

// Helper called after a successful CAS in either signaler. Both arms are shorthand:
// stash result/exception + dispatch via IContinuationDispatcher.
void DeliverFlushResult()
{
    var r = _lastPublishedReaderState;
    if (r.IsCompleted && r.CompletionException != null)
    {
        _flushAwaiter._core.SetException(r.CompletionException);
    }
    else
    {
        _flushAwaiter._core.SetResult(new FlushResult(isCanceled: false, isCompleted: r.IsCompleted));
    }
}

void OnFlushAwaiterTokenCancel()
{
    while (true)
    {
        int oldV = _flushAwaiter._state;
        if ((oldV & PipelyAwaiter.StateMask) != PipelyAwaiter.Pending) return;
        int desired = oldV & ~PipelyAwaiter.StateMask;
        if (Interlocked.CompareExchange(ref _flushAwaiter._state, desired, oldV) == oldV)
        {
            // Shorthand: stash OCE + dispatch via IContinuationDispatcher.
            _flushAwaiter._core.SetException(new OperationCanceledException(_flushAwaiter._token));
            return;
        }
    }
}
```

### Cross-thread cancel (any thread)

R2-5: cancel-while-parked constructs the buffer from the awaiter stash to match BCL's "give me the buffer you have right now" semantics. Cancel-while-not-parked sets the sticky flag; the next sync entry consumes the flag and calls `BuildReadResult(isCanceled: true)`, which constructs the buffer from the reader's local cursors (R3-1 fix — same shape as BCL).

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

        // Construct ReadResult from stash (R2-5). Stash captures the reader's cursor at park time;
        // building ROS from frozen segment refs is safe from any thread (no cursor mutation).
        var head = _readAwaiter._stashHead;
        var tail = _readAwaiter._stashTail;
        var buffer = head == null
            ? ReadOnlySequence<byte>.Empty
            : new ReadOnlySequence<byte>(head, _readAwaiter._stashHeadIdx, tail!, _readAwaiter._stashTailIdx);

        // isCompleted: false is conservative — canceler doesn't have access to writer state.
        // The next non-cancel call surfaces correct IsCompleted via TryAcquire.
        // Shorthand: stash canceled ReadResult + dispatch via IContinuationDispatcher.
        // The dispatch hop is critical here: the canceler runs on a third thread, and inline
        // continuation execution would otherwise run user code on the canceler's thread.
        _readAwaiter._core.SetResult(new ReadResult(buffer, isCanceled: true, isCompleted: false));
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
        // FlushResult has no buffer; minimal construction.
        // Shorthand: stash canceled FlushResult + dispatch via IContinuationDispatcher.
        _flushAwaiter._core.SetResult(new FlushResult(isCanceled: true, isCompleted: false));
    }
}
```

### Cancellation semantics summary

| Path | Trigger | Observable result |
|---|---|---|
| Writer-completion exception (after `Writer.Complete(ex)`) | Reader's `ReadAsync`/`TryRead` | Throws `ex` on every call (M4 Option A); takes precedence over sticky cancel and `ct` |
| Reader-completion exception (after `Reader.Complete(ex)`) | Writer's `FlushAsync` | Throws `ex` on every call |
| `CancellationToken` cancel | `ct` fires (sync at entry, or async via registration) | `OperationCanceledException` thrown from the `await` |
| `CancelPendingRead`/`CancelPendingFlush` | Method call from any thread | Result with `IsCanceled = true`, no exception |

### Outcome × path matrix

|                          | Sync entry | Park re-check | Park: registered & waiting |
|--------------------------|:---:|:---:|:---:|
| Throws **writer-completion exception** | ✓ (1) — entry-side throw | ✓ (5) — lost-wakeup defense delivered Complete-with-ex | ✓ (8) — signaler delivers via `SetException` |
| Returns `IsCanceled=false` data | ✓ (2) | ✓ (6) — lost-wakeup defense | ✓ (9) — signaled by writer |
| Returns `IsCanceled=true`        | ✓ (3) — sticky flag at entry | ✓ (7) — lost-cancel defense | ✓ (10) — `CancelPending*` while parked |
| Throws `OperationCanceledException` | ✓ (4) — `ct.IsCancellationRequested` at entry | n/a (token not yet registered) | ✓ (11) — token fires while parked |
| Returns `IsCompleted=true` (Complete(null)) | (subsumed by 2) | (subsumed by 6) | (subsumed by 9) |

11 distinct internal paths produce 4 user-observable outcomes (`IsCanceled`/`IsCompleted` are independent flags on the data return; the `IsCompleted=true` row in older versions of this matrix is subsumed by the data row with `IsCompleted` orthogonal). During the parked phase, three actors race for the CAS out of `Pending` (signaler, canceler, token); whichever commits first wins, others back off silently. Under M2, a parked writer can also be released by reader-completion via the signaler path — same shape.

### Concrete trace examples

`_state` is shown in binary (`SF` where `S` = state bit, `F` = flag bit). `Inactive` = `00`, `Pending` = `01`, `Inactive+Flag` = `10`, `Pending+Flag` = `11`.

#### Example A — Happy path (path 9)

Initial: `_state = 00`.

| t | Actor | Op | Before | After |
|---|---|---|---|---|
| 1–4 | Reader | sync entry, no flag, no data; stash; CAS `00 → 01` | `00` | `01` |
| 5 | Reader | re-check data: false | `01` | `01` |
| 6 | Reader | re-check flag: clear, skip | `01` | `01` |
| 7 | Reader | register CTR, return parked task | `01` | `01` |
| 8 | Writer | publishes WriterState | `01` | `01` |
| 9 | Writer | `Signal`: CAS `01 → 00`; constructs ReadResult from stash + W; SetResult | `01` | `00` |

#### Example B — Sticky cancel (path 3)

| t | Actor | Op | Before | After |
|---|---|---|---|---|
| 1 | Canceler | `Or(_state, 10)` (oldV `00`, not Pending — no further work) | `00` | `10` |
| 2 | Reader | sync entry: CAS `10 → 00` | `10` | `00` — returns canceled sync |

#### Example C — Lost-cancel defense (path 7)

| t | Actor | Op | Before | After |
|---|---|---|---|---|
| 1 | Reader | sync entry: TryAcquire returns false; no throw; flag clear; ct OK | `00` | `00` |
| 2 | Canceler | `Or(_state, 10)` | `00` | `10` |
| 3 | Reader | CAS `10 → 11` (preserving flag) | `10` | `11` |
| 4 | Reader | re-check data: false | `11` | `11` |
| 5 | Reader | re-check flag: CAS `11 → 00` | `11` | `00` — `SetResult(canceled)` |

#### Example D — Three-way race (paths 9/10/11)

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
| 8 | Reader | next `ReadAsync`: CAS `10 → 00` | `10` | `00` | deferred cancel surfaces here (assuming no new throw) |

#### Example E — Lost-wakeup defense delivers Complete (path 5/6)

Reader is mid-park; writer publishes a `Complete(null)` state in the small window between reader's sync data check and reader's CAS to Pending.

| t | Actor | Op | Before | After |
|---|---|---|---|---|
| 1 | Reader | sync entry: TryAcquire false; no throw; no flag; ct OK; HasReadableProgress false | `00` | `00` |
| 2 | Writer | `Writer.Complete(null)` publishes terminal `WriterState` | `00` | `00` |
| 3 | Writer | `SignalReadAwaiterIfPending`: read `_state = 00`, not Pending, return | `00` | `00` |
| 4 | Reader | stash; CAS `00 → 01` | `00` | `01` |
| 5 | Reader | re-check data: `TryAcquire` succeeds; `IsCompleted=true`; un-park CAS `01 → 00`; build ReadResult with `IsCompleted=true`; return sync | `01` | `00` |

Without step 5's re-check, the reader would be parked indefinitely.

### Key takeaways for Section 5

- One packed `_state` int per awaiter; every mutation is `Interlocked.*`. No mixed primitives.
- Pattern 2 (stash-and-construct) eliminates the thread-affinity constraint on continuations. Stash carries head + tail (R2-5) so cancel-while-parked can construct the BCL-equivalent buffer.
- Lost-wakeup, throw, and lost-cancel re-checks happen in that order in `ParkReadAwaiter`/`ParkFlushAwaiter` — matching the throw-first precedence of the sync entries.
- BCL-style cancel coalescing falls out of the sticky single-bit flag.
- `SignalFlushIfBackpressureRelieved` (gated) and `SignalFlushAwaiterIfPending` (unconditional) are split: AdvanceTo uses gated (R2-1), Reader.Complete uses unconditional.
- `CancelPendingRead` constructs `ReadResult` from stash; `CancelPendingFlush` constructs minimal `FlushResult` (no buffer).
- 11 internal paths to 4 user-observable outcomes (data return / canceled return / OCE throw / completion-exception throw, with `IsCompleted` orthogonal on data and canceled returns); race winners deterministic per CAS.
- `_core.RunContinuationsAsynchronously = false`. Every `_core.Set*` call is shorthand for "stash result/exception on the awaiter, then dispatch via `IContinuationDispatcher.UnsafeQueueUserWorkItem`"; the dispatched callback calls `_core.Set*` on the dispatcher's chosen thread. Default dispatcher = `ThreadPool.UnsafeQueueUserWorkItem`, preserving the prior RCA = true behavior. Custom dispatchers must not capture/apply ExecutionContext (EC capture happens on the consumer's thread at `OnCompleted` time and is applied by `_core.Set*`).

## Section 6 — Lifecycle and edge cases

Per the brainstorm decision (compatible BCL subset), no `Reset` is exposed. The lifecycle surface is `Complete(ex?)` per side, plus `Dispose()` for memory release. The completion-exception model follows BCL strictly (Option A from the M4 review).

### `PipeOptions` (S9)

```csharp
public sealed class PipeOptions
{
    public MemoryPool<byte> Pool { get; }                       // default: MemoryPool<byte>.Shared
    public int MinimumSegmentSize { get; }                      // default: 4096
    public long PauseWriterThreshold { get; }                   // default: 65536 (64 KB)
    public long ResumeWriterThreshold { get; }                  // default: 32768 (32 KB)
    public int MaxFreelistSegments { get; }                     // default: 256
    public IContinuationDispatcher? ContinuationDispatcher { get; init; }
        // default: null → uses ThreadPoolContinuationDispatcher.Instance (TP, identical to BCL behavior)

    public PipeOptions(
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

    public static PipeOptions Default { get; } = new();
}

public sealed class Pipe : IDisposable
{
    public Pipe() : this(PipeOptions.Default) { }
    public Pipe(PipeOptions options) { _options = options; /* ... */ }

    public PipeReader Reader { get; }
    public PipeWriter Writer { get; }

    public void Dispose() { /* see Section 3 */ }
}
```

`PauseWriterThreshold = 0` means unbounded (writer never parks). Validation matches BCL's `PipeOptions` shape; defaults match BCL's defaults. `MaxFreelistSegments` is our addition (N4). `ContinuationDispatcher` is `init`-only (per-pipe, immutable after construction); leaving it null preserves the prior RCA = true configuration's observable behavior — every awaited continuation runs on a ThreadPool worker.

**Visibility revision (post-2026-04-28).** `PipeWriter` is now `public` (was `internal`) and is no longer nested inside `Pipe`. `Pipe.Writer`'s declared return type widens to `PipeWriter`. Source-compatible with existing `PipeWriter w = pipe.Writer;` callers via implicit upcast. `PipeReader` stays `internal` — no reader-side surface addition motivates exposing it. See `2026-04-28-spsc-pipe-buffer-ownership-transfer-design.md` §6.

### `IContinuationDispatcher`

`Pipe` parks awaiters when the wake condition is unmet (no published data for `ReadAsync`, backpressure unrelieved for `FlushAsync`). When a producer subsequently signals the parked side, the *continuation* registered on that awaiter must run somewhere. By default that "somewhere" is the .NET ThreadPool, via `ThreadPool.UnsafeQueueUserWorkItem`. The pluggable `IContinuationDispatcher` interface lets a user supply a different routing — most commonly, a hot-handoff to a dedicated busy-spinning thread on a pinned core, escaping the TP wake-gap latency (~390-500 ns at P50, multi-µs at P99) for high-frequency single-stream workloads.

```csharp
namespace Pipely;

public interface IContinuationDispatcher
{
    /// <summary>
    /// Queue the callback for invocation on a thread of the implementation's choosing.
    /// Mirrors the contract of System.Threading.ThreadPool.UnsafeQueueUserWorkItem:
    ///
    /// 1. The callback MUST be invoked exactly once.
    /// 2. The implementation MUST NOT capture or apply an ExecutionContext.
    ///    Pipe captures EC in PipelyAwaiter.OnCompleted (source-side) and applies it
    ///    in s_invokeWithEc via ExecutionContext.Run on the dispatcher's chosen thread.
    ///    Adding EC manipulation in the dispatcher is redundant (wasteful) but does not
    ///    break correctness because s_invokeWithEc applies the source-side captured EC
    ///    regardless.
    /// 3. The implementation MUST be thread-safe; concurrent calls from multiple
    ///    producer threads are permitted (one dispatcher may serve multiple pipes).
    /// 4. The implementation MUST NOT throw from UnsafeQueueUserWorkItem itself.
    ///    Failure to invoke the callback hangs the consumer's await indefinitely;
    ///    a dispatcher in a failed state should still attempt to invoke the callback
    ///    (e.g., fall back to TP) rather than throw.
    /// 5. Implementations SHOULD wrap the callback invocation in try/catch so a
    ///    throwing continuation doesn't kill the dispatcher's worker thread(s).
    /// </summary>
    void UnsafeQueueUserWorkItem(Action<object?> callback, object? state);
}
```

**Default implementation (internal).** A stateless singleton forwarder:

```csharp
internal sealed class ThreadPoolContinuationDispatcher : IContinuationDispatcher
{
    public static readonly ThreadPoolContinuationDispatcher Instance = new();

    public void UnsafeQueueUserWorkItem(Action<object?> callback, object? state)
        => ThreadPool.UnsafeQueueUserWorkItem(callback, state, preferLocal: false);
}
```

When `PipeOptions.ContinuationDispatcher` is `null`, the pipe uses `ThreadPoolContinuationDispatcher.Instance`. See §5 "Continuation dispatch" and invariant I16 for the stash-and-dispatch protocol and EC discipline.

**Why `Action<object?>` and `UnsafeQueueUserWorkItem`-shaped naming.** The signature exactly matches `ThreadPool.UnsafeQueueUserWorkItem`'s, so the canonical implementation is a one-line forwarder and the EC-non-capture contract is explicit in the method name (mirroring the BCL primitive that has the matching semantics). An alternative `IThreadPoolWorkItem`-style struct API would avoid an `Action<object?>` allocation, but PipelyAwaiter<T>'s static delegates (`s_dispatch`, `s_invokeWithEc`, `s_runContinuation`) are allocated once at type-init, so there is no per-dispatch delegate alloc to optimize away.

### Scheduler bypass

`Pipe`'s `Reader.ReadAsync` and `Writer.FlushAsync` continuations do **not** honor the consumer's captured `SynchronizationContext` or `TaskScheduler`. The continuation runs on the thread chosen by the configured `IContinuationDispatcher` (default: the .NET `ThreadPool` via `ThreadPoolContinuationDispatcher`). This is independent of the consumer's `ConfigureAwait(true|false)` choice — both produce identical observable behavior. The implementation enforces this by stripping `ValueTaskSourceOnCompletedFlags.UseSchedulingContext` from the flags forwarded to `_core.OnCompleted` in `PipelyAwaiter<T>.OnCompleted` (see the source-side EC-capture spec, §2.2 and §3.3).

Consumers requiring continuation on a specific scheduler should either: (a) post explicitly via `SynchronizationContext.Post` / `TaskScheduler.FromCurrentSynchronizationContext().StartNew` after the `await`, or (b) wrap the awaitable in a `Task.Run` to capture context boundaries.

This is a deliberate contract choice, not an implementation accident. `Pipe` is a high-throughput primitive aimed at server-side workloads where consumer-side scheduler capture is not the desired routing.

### Completion overview

| Caller | Effect on the opposite side |
|---|---|
| `Writer.Complete(null)` | Reader sees `ReadResult.IsCompleted = true` from the moment it acquires the completed `WriterState`. Buffered data drains normally; subsequent reads after drain return `(Empty, IsCompleted=true)`. |
| `Writer.Complete(ex)` | **Every** subsequent `ReadAsync`/`TryRead` throws `ex`. Buffered data is unreachable (BCL-strict, M4 Option A). |
| `Reader.Complete(null)` | Writer's `FlushAsync` returns `FlushResult.IsCompleted = true` (synchronously if no backpressure; via signaler if parked). Writer's chain is fully recyclable on next `FlushAsync` (via `HeadSegment = null` terminal publish). |
| `Reader.Complete(ex)` | **Every** subsequent `FlushAsync` throws `ex`. |

### `Writer.Complete` pseudocode

```csharp
void Complete(Exception? exception = null)
{
    if (_writerCompleted) return;            // double-Complete coalesces (BCL net10.0 also coalesces — parity)
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

    SignalReadAwaiterIfPending();            // R1; unconditional (completion is always a wake reason)
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
        // S4: HeadSegment = null on terminal publish so the writer's recycle predicate sweeps
        // the entire chain on its next FlushAsync. Distinguished from pre-bootstrap by IsCompleted=true.
        HeadSegment   = null,
        TotalConsumed = _totalConsumed,
        TotalExamined = _totalExamined,
        IsCompleted   = true,
        CompletionException = exception,
    };
    _readerTb.ProducerSlot() = snapshot;
    _readerTb.Publish();                     // R1, R6
    _lastPublishedReaderState = snapshot;

    SignalFlushAwaiterIfPending();           // R1; unconditional (completion is always a wake reason)
}
```

After completion, `ReadAsync` / `TryRead` / `AdvanceTo` throw `InvalidOperationException` (entry guards in Section 4).

**Parked operations at moment of `Complete`.** `Reader.Complete` is on the reader thread; `ReadAsync` is also on the reader thread. By the SPSC contract, they don't overlap — there can't be a parked `ReadAsync` *and* a `Reader.Complete` from the same thread simultaneously. Symmetric for the writer side. (If user code awaits `ReadAsync` and another reader-thread codepath calls `Complete`, that's a contract violation — undefined behavior.)

### Exception propagation (R7, M4 Option A, BCL-strict)

After `Writer.Complete(ex)`:
- The next `_writerTb.TryAcquire()` on the reader side picks up the `IsCompleted=true, CompletionException=ex` state.
- `ReadAsync`/`TryRead`'s entry-side check throws `ex` immediately, before any `BuildReadResult` call.
- This repeats on every subsequent `ReadAsync`/`TryRead` — `_lastAcquiredWriterState` retains the completed state forever.
- A parked `ReadAsync` is woken by the signaler's `SetException(ex)` path (or the lost-wakeup defense's `SetException` if the publish raced with the park).

Symmetrically for `Reader.Complete(ex)` and the writer's `FlushAsync`.

No `_exceptionAlreadySurfaced` flag is needed under Option A — every relevant call throws; the user is expected to handle the exception and stop calling.

### `BuildReadResult` with both flags (R9)

If a sticky `CancelPending*` is consumed on a post-`Writer.Complete(null)` read, the result has *both* `IsCanceled = true` and `IsCompleted = true`. BCL allows this. `BuildReadResult` sets both flags independently. (For `Writer.Complete(ex)` the throw path fires first; the canceled-sync path doesn't trigger because the entry-guard throws.)

### Edge cases

- **Double `Complete`.** Coalesces (no-op on second call). BCL net10.0 also coalesces — parity, not a divergence.
- **`CancelPending*` after `Complete`.** Safe (cross-thread by contract). Sets the flag; the next op's entry guard either short-circuits to throw (after `Writer.Complete(ex)` / `Reader.Complete(ex)`) or returns sticky-canceled then completion. After `*Complete(ex)`, throw-first means cancel never observable.
- **`Complete` while opposite side parked.** R1 (publish before signal) covers it.
- **`Complete` while a `CancelPending*` race is in flight.** Standard awaiter race; either signaler or canceler wins the CAS. Both orderings reach a consistent terminal state.
- **Both sides `Complete`.** Either order. Pipe is in terminal state from both perspectives.
- **`Dispose` on a never-used pipe.** Safe; the chain and freelist are empty.
- **`AdvanceTo` argument validation (R8).** Monotonicity + upper bound against `_lastAcquiredWriterState.TotalWritten`. No buffer-specific validation (documented BCL divergence; catches silent-hang failure mode).
- **`AdvanceTo` on `default(SequencePosition)`.** Treated as a no-op (with publish), per N6 — handles the "AdvanceTo after empty `IsCompleted=true` ReadResult" case without NPE.
- **`AdvanceTo` with a `SequencePosition` from a different `Pipe` instance.** The upper-bound check (`examined ≤ TotalWritten`) catches obvious cases but does not detect cross-pipe references in general. Implementation should consider adding a per-segment back-reference to the owning `Pipe` (or a pipe-identity tag) so `AdvanceTo` can throw `InvalidOperationException` rather than silently corrupting another pipe's state. Treated as a defense-in-depth opportunity for the implementation plan, not a spec-level requirement.
- **`Reader.Complete` while a `ReadResult.Buffer` is outstanding.** The writer's next `FlushAsync` sweeps the entire chain (because `Reader.Complete` publishes `HeadSegment = null, IsCompleted = true`). Segments returned to the freelist may be re-rented and overwritten. Outstanding `ReadResult.Buffer` references must therefore not be accessed after `Reader.Complete` returns (parallel to the `Dispose` precondition). The implementation's public XML doc on `Reader.Complete` should state this explicitly.
- **`CancelPendingRead` + token cancellation interaction.** If both fire concurrently, they race on the CAS out of `Pending`. The winner's path is delivered (canceled result vs. OCE throw). The loser's intent is preserved as the sticky cancel flag (if canceler lost) — surfaces on next call.
- **`RunningIndex` overflow.** `RunningIndex` is `long`; overflow at ~9 EB. Out of scope.

### Cleanup / Dispose

`Pipe.Dispose()` walks the chain and freelist, disposing each `BufferSegment`'s `IMemoryOwner`. Precondition: no operation is currently in flight on either side, and no `ReadResult.Buffer` references are still held by the user (use-after-free risk on segment memory). After `Dispose`, the pipe is unusable.

### Key takeaways for Section 6

- `Complete(ex?)` is a final `Publish` plus an unconditional wake of the opposite side's awaiter.
- Under Option A (BCL-strict), every call after `Complete(ex)` throws on the opposite side. No `_exceptionAlreadySurfaced` tracking needed.
- `Reader.Complete` publishes `HeadSegment = null, IsCompleted = true` so the writer recycles the entire chain on its next `FlushAsync`.
- `IsCanceled` and `IsCompleted` flags are independent in `ReadResult` for `Writer.Complete(null)` cases; for `Writer.Complete(ex)` the throw fires first and cancel is dropped.
- Five documented divergences from BCL (see top-level takeaways): no `Reset`; `IDisposable` added; `AdvanceTo` no buffer-specific upper-bound check; cancel-from-third-thread `IsCompleted=false` lag; cancel-while-parked stash-time buffer.
- `Dispose()` releases segment + freelist memory; precondition is no in-flight ops AND no outstanding buffer refs.
- `PipeOptions.ContinuationDispatcher` is `init`-only and defaults to null (TP). User-supplied dispatchers must obey the five-item contract on `IContinuationDispatcher.UnsafeQueueUserWorkItem`; primary use case is hot-handoff to a dedicated thread for sub-µs continuation latency.

## Section 7 — Verifiability

The design's correctness rests on a small number of state machines and invariants that are explicitly enumerable. This section names what must be verified; the implementation plan describes how.

**Correctness properties to verify** (full list in the invariants and rules tables below):

| Property | Invariant/rule reference |
|---|---|
| No double-completion of `_core` per park cycle | I11 |
| No lost wakeup: every published wake-condition eventually leads to a parked owner observing it | R1, R4 |
| No spurious wake of parked writer: gated signaler (`SignalFlushIfBackpressureRelieved`) only fires when wake condition holds | R2-1 fix |
| No lost cancel: every `CancelPending*` produces exactly one `IsCanceled=true` result, accounting for races | I12 |
| Writer-completion exception thrown on every `ReadAsync`/`TryRead` after `Writer.Complete(ex)`, never silently dropped | R7 (Option A) |
| Reader-completion exception thrown on every `FlushAsync` after `Reader.Complete(ex)` | R7 |
| CTR registered ⟺ disposed (no leaked registrations) | R5 |
| Recycle never premature: a segment is never returned to the freelist while reader's `HeadSegment` references it | I6 |
| Reader never reads past published `TailWritten` | I3 |
| Pattern 2 stash read by signaler/canceler reflects the reader's cursor at park time (no torn read) | I9 + R2/R3 ordering on `_state` CAS |
| Cancel-while-parked delivers the parked buffer (BCL parity) | R2-5 stash extension |
| ExecutionContext flow correctness across the dispatcher hop: continuation observes consumer-captured EC, not producer's; dispatcher's own EC is preserved across the dispatched callback | I16, R10 |
| Continuation runs exactly once per park cycle regardless of dispatcher choice | I11, R10 |

**BCL parity tests** required for the implementation:
- Throw timing on `Writer.Complete(ex)` — every read throws; buffered data unreachable. Verifies M4 Option A claim.
- Throw timing on `Reader.Complete(ex)` — every flush throws.
- Cancel-while-parked buffer contents match BCL `Pipe.CancelPendingRead`.
- Backpressure operators: pause `>=`, resume `<` (verified against BCL source 2026-04-25).

**Performance goals.** This design exists because BCL's `Pipe` is too slow under SPSC scheduling — its central `lock` serializes producer and consumer. The implementation must demonstrate, on a benchmark harness (`IPipeAdapter` + `Pipely`/`Bcl` adapters; **to be (re)built as a prior implementation step** — the prior iteration's harness was removed in the Restart):

- Materially higher throughput (bytes/sec) than BCL Pipe under steady-state SPSC workloads.
- Comparable or better p50 latency; materially better tail (p99/p99.9) due to no lock contention.
- Comparable allocations per op (segment freelist matches BCL's segment pool).

If these benchmark targets are not met, the design has failed its purpose and must be revisited before merge.

**Tractability.** The state space is small enough for mechanized verification (TLA+) if pursued:

- The cross-thread state-exchange primitive (TripleBuffer) is a well-known wait-free pattern; it can be modeled and verified once and treated as a primitive thereafter.
- The awaiter is a 2-state + 1-flag machine (4 reachable states) with 3 concurrent actors plus the owner.
- The recycle predicate is local and based on a single reference comparison (with a special case for `Reader.Complete`'s null `HeadSegment`).
- The lifecycle/completion lives in the same awaiter state machine plus the published `IsCompleted` flag.

Whether to actually pursue TLA+ verification is an implementation-plan decision; the design's tractability for it is a deliberate property.

## Consolidated invariants

| # | Invariant |
|---|---|
| **I1** | Successive `WriterState` publishes have `TotalWritten` non-decreasing and `IsCompleted` sticky-once-set. Same for `ReaderState` (`TotalConsumed`, `TotalExamined` monotonic and `TotalConsumed ≤ TotalExamined`; `IsCompleted` sticky). `CompletionException` is set iff `IsCompleted=true` and never changes once set. |
| **I2** | The writer is the sole mutator of `BufferSegment` fields, the chain (`_chainHead`/`_writingHead`), the freelist, and `_freelistCount`. The reader is the sole mutator of `_readHead`, `_readTail`, byte counters. |
| **I3** | Reader's bound for the *active tail* (the published `WriterState.TailSegment`) is `_readTailIdx`. Reader never reads `BufferSegment.End` or `BufferSegment.Memory.Length` to determine the *active tail's* boundary. (Intermediate frozen segments' `Memory` is read during ROS iteration; safe by I5: those segments' `Memory` was set before the publish whose acquire fence the reader observed.) |
| **I4** | While `S == _writingHead`, the writer mutates only `S.AvailableMemory[TailWritten..]`. It does not mutate `S.End`, `S.Memory`, or `S.Next`. |
| **I5** | All freeze writes on `S` (`End`, `Memory`, `Next`) complete before the `Publish` of any `WriterState` with `TailSegment ≠ S`. |
| **I6** | A segment `S` is recycled only when `S != _writingHead` ∧ `_lastAcquiredReaderState.HeadSegment != S`. The pre-bootstrap guard (`HeadSegment is null` AND `!IsCompleted`) prevents recycling before reader has bootstrapped. The `Reader.Complete` terminal publish sets `HeadSegment = null` AND `IsCompleted = true`, allowing the writer to sweep the entire chain. |
| **I7** | Reader's `_readHead` is at-or-before `_readTail` in chain order, **except** transiently after a Pattern-2-delivered ReadResult — in which case `_readHead` may be past `_readTail`'s segment until the next `TryAcquire` refreshes `_readTail`. The reader-private `_readTail` is purely a hint for `BuildReadResult`'s sync path; AdvanceTo doesn't reference it. |
| **I8** | TripleBuffer slot **data** is mutated only by the producer side. Role rotations on `Publish`/`TryAcquire` change the **role** of a slot but never its data. |
| **I9** | Each `Publish` is a release fence; each **successful** `TryAcquire` is an acquire fence (both via `Interlocked.Exchange`). Writes preceding a `Publish` are observable to the consumer side after the matching successful `TryAcquire`. |
| **I10** | At the reader's first successful `TryAcquire`, `WriterState.HeadSegment == writer's _chainHead == start of the live chain`. Justification: the recycle predicate (I6) cannot fire until the reader has bootstrapped and published, OR `Reader.Complete` has run (which can't happen pre-bootstrap). Bootstrap relies on TripleBuffer's initial state having `dirty = 0` (current `TripleBuffer.cs` ctor satisfies this; treat as part of the contract). |
| **I11** | `_state` transitions out of `Pending` exactly once per park cycle. `_core` enforces single-completion underneath. |
| **I12** | `CancelPending*` calls coalesce: any number of calls between two Inactive→Pending→Inactive cycles deliver at most one `IsCanceled = true` result. |
| **I13** | After `Writer.Complete(_)`, the writer publishes nothing further (R6). The published `WriterState` is sticky with `IsCompleted=true` and `CompletionException` set if the completion was faulted. |
| **I14** | After `Reader.Complete(_)`, the reader publishes nothing further. The published `ReaderState` has `HeadSegment = null` (allowing full chain recycle), `IsCompleted = true`, and `CompletionException` set if faulted. |
| **I15** | `Pipe.Dispose()` precondition: no operation is currently in flight on either side, and no `ReadResult.Buffer` references are still held by the user. Violation is undefined behavior. |
| **I16** | **Source-side EC capture / apply discipline.** ExecutionContext capture for await continuations occurs in `PipelyAwaiter.OnCompleted` on the consumer's thread, gated by `FlowExecutionContext`. The capture is stored on the awaiter (`_capturedEC`) and applied in `s_invokeWithEc` via `ExecutionContext.Run` on the dispatcher's chosen thread. `IContinuationDispatcher` implementations MUST NOT capture or apply an ExecutionContext themselves; their role is purely to route the callback to a thread. Implementations using `ThreadPool.UnsafeQueueUserWorkItem` satisfy this trivially; implementations using `ThreadPool.QueueUserWorkItem` or `Task.Run` capture EC redundantly (wasteful, but does not break the consumer's EC because `s_invokeWithEc` applies the source-side captured EC anyway). |

## Consolidated rules

| # | Rule |
|---|---|
| **R1** | Publish before signal. In any code path that both publishes a state and signals the other side's awaiter, publish first, signal second. |
| **R2** | All `_state` mutations on `PipelyAwaiter` use `Interlocked.CompareExchange` or `Interlocked.Or`. No plain or `Volatile` writes. |
| **R3** | Transitions out of `Pending` (signaler / canceler / token) atomically clear the state bits. The flag bit is preserved on signaler/token paths, cleared on the canceler-delivered-via-state path. The actor that wins the CAS calls the appropriate `_core.Set*`. |
| **R4** | After CASing `Inactive → Pending`, the owner re-checks (a) the writer-completion exception (reader side) or reader-completion exception (writer side), then (b) the data state / wake condition, then (c) the cancel flag. Each re-check attempts a CAS out of `Pending` if its condition holds. **Throw-first ordering** matches the sync-entry precedence (R9). |
| **R5** | After registering a `CancellationTokenRegistration` on `_ctr`, the registering actor must re-check `_state` and dispose `_ctr` if the awaiter is no longer in `Pending`. CTR disposal is idempotent. The owner additionally disposes `_ctr` at the start of the next `Park*Awaiter` call to clean up the token-callback-wins case (callback consumes its own registration but doesn't dispose). |
| **R6** | `Complete(ex?)` on either side is the terminal publish; no further `Publish` happens after it on the completing TripleBuffer. |
| **R7** | **(Option A, BCL-strict.)** After `Writer.Complete(ex)`, every `ReadAsync` / `TryRead` on the reader throws `ex`. After `Reader.Complete(ex)`, every `FlushAsync` on the writer throws `ex`. No drain semantics; buffered data after a faulted side is unreachable. No `_exceptionAlreadySurfaced` flag needed. |
| **R8** | `AdvanceTo` validates monotonicity and upper bound: `consumed`/`examined` must not go backwards relative to `_totalConsumed`/`_totalExamined`; `consumed ≤ examined`; `examined ≤ _lastAcquiredWriterState.TotalWritten` (after `TryAcquire`). Out-of-range positions throw `InvalidOperationException`. The buffer-specific upper bound (within previous `ReadResult.Buffer`) is **not** validated — documented BCL divergence; catches silent-hang but not stale-`SequencePosition` corruption. |
| **R9** | **Throw-first precedence.** Sync entry order in `ReadAsync`/`TryRead`: (1) `_readerCompleted` entry guard, (2) `TryAcquire` + integrate, (3) writer-completion-exception throw, (4) sticky cancel consume, (5) `ct.IsCancellationRequested`, (6) data/IsCompleted return, (7) park. Symmetric in `FlushAsync` (with the dual `TryAcquire`s for backpressure freshness). `BuildReadResult` produces `(IsCanceled, IsCompleted)` independently — both flags can be true simultaneously for `Writer.Complete(null)` reads. For `Writer.Complete(ex)` reads, the throw at step 3 fires first; cancel is permanently dropped. |
| **R10** | **Continuation dispatch.** `_core.RunContinuationsAsynchronously = false`. After winning CAS Pending→Inactive, every signaler / canceler / token-callback / park re-check (both lost-wakeup and lost-cancel) that needs to deliver a result via the awaiter calls `_core.SetResult` / `_core.SetException` directly on the producer's thread. `MRVTSC` invokes the registered `s_dispatch` callback inline (because RCA=false); `s_dispatch` routes through the configured `IContinuationDispatcher`; `s_invokeWithEc` reads the awaiter's source-side-captured `_realContinuation` / `_realState` / `_capturedEC`, applies EC if non-null via `ExecutionContext.Run`, and invokes the user's continuation. Sync data returns built directly into a `ValueTask<T>` via `BuildReadResult` / `BuildFlushResult` are exempt (no continuation, no dispatch). |

## Conventions

- **TryAcquire-twice in `FlushAsync`.** Top-of-method for throw/cancel-check freshness; mid-method (after publish/signal) for backpressure-decision freshness. Each `TryAcquire` is cheap (`Volatile.Read`; `Interlocked.Exchange` only if dirty).
- **No `Reset`.** The lifecycle surface is single-use; the pipe is created, used, both sides `Complete`, then `Dispose`.
- **Thread contract.** Strict SPSC for normal ops; `CancelPending*` is the only thread-safe-from-anywhere operation.
- **`MemoryPool<byte>.Rent` returns `≥ sizeHint`.** Code reasoning about rented capacity must use `>=`, never `==`.
- **Sticky once set.** Used consistently for `IsCompleted` (false→true exactly once); avoid "monotonic"/"non-decreasing" for boolean fields.
