# SpscPipe — Lock-Free SPSC Pipe Specification

**Status:** Draft v2 (supersedes the ring-of-descriptors draft)
**Target framework:** .NET 10+, 64-bit process (`Volatile.Write` on `long` fields requires 64-bit atomicity)
**Scope:** A lock-free, single-producer / single-consumer replacement for `System.IO.Pipelines.Pipe`, preserving the public contracts of `PipeReader` and `PipeWriter`. Uses an unbounded linked list of pooled segments with byte-count backpressure.

---

## 1. Goals and non-goals

### 1.1 Goals

- Preserve the public contracts of `PipeReader` and `PipeWriter` as exposed by `System.IO.Pipelines.Pipe`.
- Lock-free coordination with no `Monitor` and no `lock`. Per-byte read/write paths use only release/acquire semantics on naturally-aligned word-sized fields. Interlocked operations are permitted but confined to segment boundaries (buffer refcounting, §6.5) and awaiter arming (§8); they do not occur on per-byte operations.
- No slot exhaustion under chatty flushes. A caller doing `Write(1); Flush()` in a tight loop must not dead-end the pipe for any reason other than the standard byte-based pause threshold.
- Zero steady-state allocation after warmup. Segment objects and backing buffers are pooled.
- Full `ReadOnlySequence<byte>` support including multi-segment sequences and `examined > consumed` advance patterns.

### 1.2 Non-goals

- Multi-producer or multi-consumer. Any violation of the SPSC invariant is undefined behavior (§10.1).
- Cancellation of partially-written flushes. `FlushAsync`'s `CancellationToken` cancels the wait for backpressure relief, not the publication of already-written bytes. Matches BCL behavior.
- Duplex pipes. `SpscPipe` is one-way.
- Preservation of BCL internal types (`BufferSegment`, `Pipe.State`, etc.). Only the public `PipeReader` / `PipeWriter` contracts are preserved.

### 1.3 Relationship to `System.IO.Pipelines.Pipe`

The BCL `Pipe` uses a linked list of `BufferSegment`s protected by a `SyncObject` lock. `SpscPipe` uses the same structural approach — linked list of segments — but removes the lock by exploiting the SPSC invariant to reduce all cross-thread coordination to release/acquire on fields with a single writer per field.

---

## 2. Terminology

- **Segment**: a node in a singly-linked list, holding a range of bytes (`{ buffer, start, writtenLength, runningIndex, next }`) and inheriting from `ReadOnlySequenceSegment<byte>`.
- **Active buffer**: the pooled backing buffer the writer is currently filling. Not yet visible to the reader.
- **Active segment**: the segment being constructed from the active buffer, not yet linked into the list. Optional — may be deferred until publication.
- **Published segment**: a segment that has been linked into the list and is visible to the reader.
- **Retired segment**: a published segment whose bytes are fully consumed and whose buffer has been returned to the pool; the segment object has been returned to the segment pool.
- **`head`**: pointer to the first non-retired segment. Reader-owned.
- **`tail`**: pointer to the most recently published segment. Writer-owned.
- **`_unpublishedTail`**: pointer to the in-progress segment being grown by the writer before publication. Writer-local; not visible to the reader.

---

## 3. Public API

```csharp
public sealed class SpscPipe : IDisposable
{
    public SpscPipe();
    public SpscPipe(SpscPipeOptions options);

    public PipeReader Reader { get; }
    public PipeWriter Writer { get; }

    public void Reset();
    public void Dispose();
}

public sealed class SpscPipeOptions
{
    public int MinimumSegmentSize { get; init; } = 4096;
    public long PauseWriterThreshold { get; init; } = 65536;
    public long ResumeWriterThreshold { get; init; } = 32768;
    public MemoryPool<byte>? Pool { get; init; } = null;
    public PipeScheduler? ReaderScheduler { get; init; } = PipeScheduler.ThreadPool;
    public PipeScheduler? WriterScheduler { get; init; } = PipeScheduler.ThreadPool;
    public bool UseSynchronizationContext { get; init; } = true;
}
```

Invariants: `0 ≤ ResumeWriterThreshold ≤ PauseWriterThreshold`, `MinimumSegmentSize ≥ 1`. Validated in the constructor.

---

## 4. Data structures

### 4.1 Segment

```csharp
internal sealed class Segment : ReadOnlySequenceSegment<byte>
{
    internal BufferHolder? Holder;        // shared buffer container; null after retirement
    internal int BufferStart;             // start offset within Holder.Owner.Memory
    internal int WrittenLength;           // bytes written into this segment
    // inherited: Memory<byte> Memory, long RunningIndex
    // inherited: ReadOnlySequenceSegment<byte>? Next

    internal void Reset() { /* clear all fields for pool return */ }
}
```

`Memory` is set to a slice of `Holder.Owner.Memory` covering `[BufferStart, BufferStart + WrittenLength)` at publication time. `BufferHolder` is defined in §6.5.1.

`Next` is the base-class property; writes to it use `Volatile.Write` for release semantics (§7.2).

### 4.2 Shared state layout

All cross-thread fields live in a `State` object with explicit cache-line padding. A 64-byte cache line is assumed; on ARM64 platforms with 128-byte lines the layout is wasteful but correct. The `FieldOffset` layout prevents false sharing between groups *within* the struct. However, it does not guarantee 64-byte alignment of the struct itself relative to physical cache lines — the CLR aligns heap objects to pointer boundaries (8 bytes on 64-bit), not cache-line boundaries. In practice, the padding between groups absorbs misalignment; worst case, one cache line straddles two groups, but no two groups share a single cache line.

```csharp
[StructLayout(LayoutKind.Explicit, Size = 384)]
internal struct State
{
    // ---- Cache line 0: writer-published fields ----
    [FieldOffset(64)]  internal Segment? Tail;                // writer writes, reader acquires
    [FieldOffset(72)]  internal long BytesWrittenPublished;   // writer writes, reader acquires
    [FieldOffset(80)]  internal int WriterCompletionState;    // 0=active, 2=completed
    [FieldOffset(88)]  internal ExceptionDispatchInfo? WriterException;
    [FieldOffset(96)]  internal Segment? Head;                // first segment ever published; writer writes once, reader acquires once

    // ---- Cache line 1: reader-published fields ----
    [FieldOffset(128)] internal long BytesReadPublished;      // reader writes, writer acquires
    [FieldOffset(136)] internal int ReaderCompletionState;
    [FieldOffset(144)] internal ExceptionDispatchInfo? ReaderException;

    // ---- Cache line 2: awaiter coordination ----
    [FieldOffset(192)] internal int ReaderAwaiterState;       // see §8
    [FieldOffset(200)] internal int WriterAwaiterState;       // see §8

    // ---- Cache line 3: trailing pad to reduce false-sharing with adjacent heap allocations ----
}
```

Fields on cache line 0 are written only by the writer. Fields on cache line 1 are written only by the reader. Cache line 2 is shared but accessed via release/acquire with the double-check pattern (§8).

### 4.3 Writer-local state

Not in `State`; lives on the `PipeWriter` subclass:

```csharp
internal sealed class SpscPipeWriter : PipeWriter
{
    private Segment? _unpublishedTail;    // in-progress segment (not yet linked)
    private BufferHolder? _activeBufferHolder;  // holder for the buffer currently being filled
    private int _activeBufferWritten;     // write offset within active buffer
    private int _activeBufferCapacity;    // cached Memory.Length
    private int _unflushedStart;          // start offset of unflushed bytes within active buffer
    private long _bytesWritten;           // mirror of BytesWrittenPublished, pre-release

    // Awaiter for FlushAsync backpressure:
    private ManualResetValueTaskSourceCore<FlushResult> _flushAwaiter;
    private CancellationTokenRegistration _flushCtr;
}
```

### 4.4 Reader-local state

```csharp
internal sealed class SpscPipeReader : PipeReader
{
    private Segment? _head;               // first non-retired segment
    private int _headConsumedOffset;      // bytes consumed within _head beyond BufferStart
    private long _examinedPosition;       // absolute byte position of last examined boundary
    private long _bytesRead;              // mirror of BytesReadPublished, pre-release

    // State tracking for AdvanceTo validation:
    private bool _readInProgress;
    private ReadOnlySequence<byte> _lastReturnedBuffer;

    // Awaiter for ReadAsync (carries ReadSignal, not ReadResult — see below):
    private ManualResetValueTaskSourceCore<ReadSignal> _readAwaiter;
    private CancellationTokenRegistration _readCtr;
}

// Signal struct for the read awaiter. The writer must not build a ReadResult
// (which requires reader-local state); instead it signals with minimal flags.
// SpscPipeReader implements IValueTaskSource<ReadResult> and translates the
// signal into a real ReadResult on the reader's continuation thread (§8.7).
internal readonly struct ReadSignal
{
    internal bool IsCanceled { get; init; }
    internal bool IsCompleted { get; init; }
}
```

---

## 5. The fundamental ordering problem

Before describing the operations, this section states the memory-ordering problem that shapes everything below. Implementers should keep this in mind throughout §6–§8.

**The problem.** The writer wants to make bytes visible to the reader. The reader wants to retire segments (return buffers to the pool) after consuming them. Both sides need to observe each other's state without locks. The SPSC invariant gives us a critical property: **no field has more than one writer**. This is what makes release/acquire sufficient.

**The solution pattern.** For every piece of shared state X:

- Designate exactly one thread as the writer of X.
- The writer updates X using release semantics (`Volatile.Write` on the last store, or on a companion "published" field if X is multi-word).
- Any reader of X uses acquire semantics (`Volatile.Read`).
- If both sides need to observe "is the other side waiting?" (for awaiter coordination), they use the double-check pattern (§8.3) to close the standard lost-wakeup race.

**What release/acquire guarantees.** A release-store of X to value `v` followed by the other thread's acquire-load of X observing `v` establishes a happens-before edge: all stores on the releasing thread that are program-order before the release-store are visible to all loads on the acquiring thread that are program-order after the acquire-load.

**What it does not guarantee.** Release/acquire does *not* provide StoreLoad ordering. A release-store followed by an acquire-load on the same thread can be reordered relative to unrelated memory operations. This matters exactly once, in §8.3, where we need an explicit `Interlocked.MemoryBarrier()` (equivalent to `std::atomic_thread_fence(memory_order_seq_cst)`) for the awaiter handshake.

**Interlocked operations on .NET.** Every `Interlocked.*` method (including `Increment`, `Decrement`, `CompareExchange`, `Exchange`, and `MemoryBarrier`) implies a full sequentially-consistent fence. That is: all loads and stores program-order before an `Interlocked` call are globally ordered before all loads and stores program-order after it, observable to every other thread. This is strictly stronger than release-acquire and provides both directions of ordering (in particular, StoreLoad). The spec invokes this axiom explicitly wherever it is relied upon (§6.5.3, §8.2, §8.3, §8.4).

---

## 6. Writer operations

### 6.1 `Memory<byte> GetMemory(int sizeHint = 0)`

Contract: returns contiguous writable memory of length at least `max(sizeHint, 1)`. If `sizeHint == 0`, returns at least some "reasonable" amount (we use `MinimumSegmentSize - _activeBufferWritten` if positive, else `MinimumSegmentSize`).

Algorithm:

```
desiredSize = max(sizeHint, 1)

if _activeBufferHolder == null:
    // First write, or just after a buffer was fully released.
    RentActiveBuffer(max(desiredSize, MinimumSegmentSize))
    return _activeBufferHolder.Owner.Memory

remaining = _activeBufferCapacity - _activeBufferWritten
if remaining >= desiredSize:
    return _activeBufferHolder.Owner.Memory.Slice(_activeBufferWritten)

// Not enough room. Flush any unflushed bytes, then rotate to a new buffer.
if _unflushedStart < _activeBufferWritten:
    PublishActiveSegment()

// Release the writer's reference to the current holder. Outstanding segments
// retain their own references via the refcount (§6.5); the buffer returns to
// the pool when the last of them is retired.
ReleaseHolder(_activeBufferHolder)
_activeBufferHolder = null

RentActiveBuffer(max(desiredSize, MinimumSegmentSize))
return _activeBufferHolder.Owner.Memory
```

Where `RentActiveBuffer(size)` is:

```
owner  = Pool.Rent(size)
holder = BufferHolderPool.Rent()
holder.Owner    = owner
holder.Refcount = 1              // writer's own reference (§6.5.2)
_activeBufferHolder = holder
_activeBufferCapacity = owner.Memory.Length
_activeBufferWritten  = 0
_unflushedStart       = 0
```

**Sub-call: `PublishActiveSegment()`** (§6.4).

Note that `GetMemory` never blocks and never touches shared state. It can allocate (buffer rental, segment rental) but in steady state all of these come from pools. It does not check backpressure; backpressure is entirely a `FlushAsync` concern.

### 6.2 `void Advance(int bytes)`

```
if bytes < 0: throw
if bytes > _activeBufferCapacity - _activeBufferWritten: throw
_activeBufferWritten += bytes
```

Plain operation on writer-local state. No synchronization.

### 6.3 `ValueTask<FlushResult> FlushAsync(CancellationToken ct)`

Algorithm:

```
1. If ct.IsCancellationRequested:
       return ValueTask from FlushResult { IsCanceled = true, IsCompleted = readerCompleted }

2. If _unflushedStart < _activeBufferWritten:
       PublishActiveSegment()

3. Load reader completion state (acquire):
       readerDone = Volatile.Read(ref state.ReaderCompletionState) == 2

4. If readerDone:
       return ValueTask from FlushResult { IsCanceled = false, IsCompleted = true }

5. Backpressure check:
       bytesRead = Volatile.Read(ref state.BytesReadPublished)  // acquire
       outstanding = _bytesWritten - bytesRead
       if outstanding < PauseWriterThreshold:
           return ValueTask from FlushResult { IsCanceled = false, IsCompleted = false }

6. Slow path: need to wait. See §8.4 for the exact awaiter arming sequence.
```

Step 5's single acquire-load is the only synchronization on the fast path. No lock, no CAS.

### 6.4 `PublishActiveSegment()` — the publication protocol

This is the critical operation. It makes unflushed bytes visible to the reader.

```
// Preconditions:
//   _activeBufferHolder != null
//   _unflushedStart < _activeBufferWritten
//   _unpublishedTail is null (we publish eagerly in this algorithm)

// Step 1: Rent a segment and initialize it from writer-local state.
//         All writes here are to writer-local or not-yet-published fields,
//         except the Interlocked.Increment on the shared Refcount. That
//         increment must happen before the segment becomes reachable, so
//         that a reader observing the segment also observes the matching
//         increment (§6.5.3). Interlocked.Increment is a full fence (§5),
//         so it is ordered globally before the release-stores in Step 2.
Interlocked.Increment(ref _activeBufferHolder.Refcount)
seg = RentSegment()
seg.Holder = _activeBufferHolder       // buffer may be shared with sibling segments
seg.BufferStart = _unflushedStart
seg.WrittenLength = _activeBufferWritten - _unflushedStart
seg.Memory = _activeBufferHolder.Owner.Memory.Slice(seg.BufferStart, seg.WrittenLength)
seg.RunningIndex = _bytesWritten       // absolute byte position of this segment's start
seg.Next = null                        // will never be rewritten to non-null by anyone
                                       //   except us, below

// Step 2: Link into the list.
//         state.Tail is writer-owned. There is no concurrent writer of state.Tail
//         or of tail.Next, because SPSC.
prevTail = state.Tail                  // plain read; we are the only writer
if prevTail is null:
    // First segment ever. state.Tail was null; _head (reader-side) is also null.
    // Publish seg as the new tail. Also record it as the permanent head so the
    // reader can find the start of the list on first ReadAsync (§7.1 step 4).
    //
    // CRITICAL ORDERING: all writes to seg.* above must be visible to the reader
    // before the reader observes state.Head or state.Tail = seg.
    //
    // Head MUST be written before Tail. The reader reads Tail first (§7.1 step 3),
    // then reads Head (§7.1 step 4). The reader's acquire-load of Tail seeing
    // this segment establishes a happens-before edge that makes the preceding
    // release-store of Head visible. Reversing this order would allow the reader
    // to observe Tail = seg while Head is still null.
    Volatile.Write(ref state.Head, seg)    // release-store #0 (first segment pointer)
    Volatile.Write(ref state.Tail, seg)    // release-store #1
else:
    // Not first segment. Link prevTail.Next -> seg, then advance Tail.
    //
    // CRITICAL ORDERING:
    //   (a) All writes to seg.* must be visible to the reader before the reader
    //       observes prevTail.Next = seg.
    //   (b) All writes to seg.* must ALSO be visible before state.Tail = seg.
    //
    // The reader can reach seg via two paths:
    //   - Traversing from _head following Next pointers, where it will eventually
    //     do Volatile.Read(prevTail.Next) and observe seg.
    //   - Reading state.Tail directly (we use this as the upper bound for
    //     traversal — see §7.1).
    //
    // Both paths need seg.* to be fully initialized before they see seg. We achieve
    // this with release-stores on BOTH prevTail.Next and state.Tail.
    Volatile.Write(ref prevTail.Next, seg)    // release-store #1 (Next pointer)
    Volatile.Write(ref state.Tail, seg)       // release-store #2 (Tail pointer)

// Step 3: Update byte accounting.
_bytesWritten += seg.WrittenLength
//
// The reader observes new bytes by one of:
//   (i)  Acquire-loading state.Tail and traversing. The segment lengths carry
//        the byte count; the reader doesn't strictly need BytesWrittenPublished
//        for availability, only for backpressure signaling (telling us it's
//        drained).
//   (ii) Acquire-loading state.BytesWrittenPublished to know how much total
//        data is available without walking segments.
//
// We publish BytesWrittenPublished so path (ii) works, used by the awaiter
// coordination in §8:
Volatile.Write(ref state.BytesWrittenPublished, _bytesWritten)    // release-store #3

// Step 4: Update writer-local accounting.
_unflushedStart = _activeBufferWritten

// Step 5: Signal the reader if it's waiting. See §8.2.
MaybeSignalReaderAwaiter()
```

**Why three release-stores?** They ensure two distinct happens-before edges:

1. `seg.*` writes happen-before any reader observation of `seg` (via `state.Head`, `prevTail.Next`, or `state.Tail`).
2. `seg.WrittenLength` (and transitively all byte contents via `seg.Memory`) happen-before the reader's observation of `state.BytesWrittenPublished`.

Edge (1) is needed for readers that walk the list. Edge (2) is needed for readers that shortcut via the byte counter (used in the awaiter-signal decision).

**Why is release-store #2 necessary if #1 already published `seg`?** Because the reader may have been parked with a cached `state.Tail` from *before* this publication, and on wakeup it needs a fresh upper bound. Setting `state.Tail` to `seg` is how the reader learns "there is at least this much available." Without it, a reader that woke up and traversed `_head.Next → ... → prevTail` would see `prevTail.Next == seg` (good) but would have no signal that `seg` is the *new* tail — it would have to follow `seg.Next` and get `null`, at which point it concludes `seg` is the tail. That actually works, but only if we can guarantee `seg.Next` is `null` at the time of the read, which requires us to have not yet started a next publication. To avoid depending on that timing, we publish `state.Tail` explicitly.

(There is a defensible alternative design where `state.Tail` is omitted and the reader always traverses to `next == null`. We keep `state.Tail` because it's also the upper bound used by `TryRead` to decide "is there new data?" without traversal.)

**Ordering constraint between release-stores.** For the non-first case, release-stores #1 and #2 can be in either order, but both must precede release-store #3, because a reader observing `BytesWrittenPublished >= new value` must be able to find segments accounting for those bytes. The order shown (Next first, Tail second, Bytes third) is the conservative one. For the first publication, the same constraint holds: release-stores #0 (Head) and #1 (Tail) must both precede #3.

### 6.5 Buffer lifetime management

The publication algorithm permits multiple segments to share a single backing buffer: after `PublishActiveSegment`, `_activeBufferWritten < _activeBufferCapacity`, and the next `Advance`/`FlushAsync` cycle produces another segment drawing from the same buffer. This is essential for avoiding pool churn under small flushes — without sharing, a 100-byte flush with a 4 KiB `MinimumSegmentSize` would consume an entire buffer.

Because multiple segments may hold references to the same buffer, the buffer cannot be returned to the pool when any one segment is retired. Its lifetime is the union of the lifetimes of all referencing segments plus the writer's active use.

#### 6.5.1 `BufferHolder`

Each rented buffer is wrapped in a shared, pooled container:

```csharp
internal sealed class BufferHolder
{
    internal IMemoryOwner<byte>? Owner;
    internal int Refcount;    // interlocked on both sides
}
```

Every segment drawn from a given buffer stores a reference to the same `BufferHolder`. The refcount tracks how many holders of the buffer exist — segments plus (while active) the writer itself.

#### 6.5.2 Refcount protocol

Both sides use `Interlocked` on `Refcount`. This is the one concession to interlocked operations outside the awaiter coordination; see §6.5.4 for the rationale.

**Buffer rental (writer, in `GetMemory`):**
```
owner  = Pool.Rent(size)
holder = BufferHolderPool.Rent()
holder.Owner    = owner
holder.Refcount = 1                   // writer's own reference
_activeBufferHolder = holder
```

**Segment creation (writer, in `PublishActiveSegment`, before the release-stores that publish it):**
```
Interlocked.Increment(ref _activeBufferHolder.Refcount)
seg.Holder = _activeBufferHolder
```

**Buffer rotation (writer, in `GetMemory` when rolling to a new buffer):**
```
// Drop the writer's own reference. If all segments from this buffer have
// already been retired, this decrement returns it to the pool.
ReleaseHolder(_activeBufferHolder)
_activeBufferHolder = null
// ... rent new buffer/holder as above ...
```

**Segment retirement (reader, in `RetireSegment`):**
```
ReleaseHolder(seg.Holder)
seg.Holder = null
```

**`ReleaseHolder(holder)`** (shared helper):
```
if Interlocked.Decrement(ref holder.Refcount) == 0:
    Pool.Return(holder.Owner)
    holder.Owner = null
    BufferHolderPool.Return(holder)
```

#### 6.5.3 Correctness

We show that the refcount protocol never permits a buffer to be returned to the pool while either the writer or any non-retired segment still references it, and that it always returns the buffer exactly once.

**Visibility of the writer's refcount increment.** In `PublishActiveSegment` (§6.4), the writer executes `Interlocked.Increment(ref _activeBufferHolder.Refcount)` in step 1, before the release-stores in step 2 that make the segment reachable to the reader (via `prevTail.Next` or `state.Tail`). By the `Interlocked` full-fence axiom (§5), every memory operation program-order before the `Interlocked.Increment` is globally ordered before every operation program-order after it. In particular, the updated `Refcount` value is globally ordered before the subsequent release-stores of `prevTail.Next` and `state.Tail`. Therefore any reader thread that observes the segment as reachable also observes the already-incremented `Refcount`.

**Invariant: `Refcount ≥ 1` while the buffer is writer-active.** Let "writer-active" mean the interval between `RentActiveBuffer` setting `_activeBufferHolder = holder` and `ReleaseHolder(holder)` dropping the writer's own reference. At rental, `Refcount` is initialized to 1 (the writer's own reference). During writer-active time:

- Every `Interlocked.Increment` in `PublishActiveSegment` raises `Refcount` by 1 *before* the segment it creates is reachable by the reader.
- Every reader `ReleaseHolder(seg.Holder)` decrements `Refcount` by 1. The reader only reaches this code via `RetireSegment`, which only runs on segments that are reachable from `_head` via the singly-linked chain — i.e., segments that have been published. By the visibility argument above, the matching increment has already executed and is globally visible.
- Therefore every decrement is preceded (in the happens-before order) by a matching increment on the same holder, so the sum `1 + (increments) - (decrements)` observed at any point during writer-active time is at least 1.

**Visibility of the reader's refcount decrement.** Before the reader calls `ReleaseHolder(seg.Holder)`, it must first hold a reference to `seg.Holder`. The reader obtains this reference by a plain load of `seg.Holder` inside `RetireSegment`. This plain load is valid — that is, it observes the value written by the writer during segment initialization in §6.4 step 1 — because the segment `seg` was reached by an earlier acquire-load: either `Volatile.Read(ref state.Head)` in `ReadAsync` (for the first segment, §7.1 step 4) or `Volatile.Read(ref prevSeg.Next)` in sequence traversal (for subsequent segments). Both acquire-loads are paired with writer-side release-stores that were program-order after the writer's initialization of `seg.Holder`; the resulting happens-before edge makes the plain load of `seg.Holder` well-defined.

The reader's `Interlocked.Decrement(ref seg.Holder.Refcount)` is, by the §5 axiom, a full fence. It is globally ordered after the reader's plain load of `seg.Holder` (which is program-order before it). It is also globally ordered after all of the reader's prior consumption of `seg`'s bytes (same reason). The reader only calls `RetireSegment` on segments it has fully consumed per `AdvanceTo` — the caller's `consumed` position has moved past them, meaning the caller's contract obligation (to not access the retired portion of the prior `ReadOnlySequence<byte>`) has taken effect. Therefore a reader that decrements `Refcount` to 0 and proceeds to `Pool.Return(holder.Owner)` has demonstrably finished all reads of the buffer contents before returning the buffer. The pool-return does not race with in-flight reader access to the buffer.

On the writer side, a writer executing `ReleaseHolder(_activeBufferHolder)` (during buffer rotation in `GetMemory`, or during `Complete`) has, by that point, already finished all writes to the buffer — the writer sets `_activeBufferHolder = null` immediately after, and no subsequent writer operation touches the buffer. The writer's `Interlocked.Decrement` is similarly a full fence, ordered after all of its prior writes to the buffer contents. If this decrement drives `Refcount` to 0, the writer's subsequent `Pool.Return` runs after all writer-side writes to the buffer, so it also does not race with in-flight writer access.

**Invariant preservation at buffer rotation.** `ReleaseHolder` decrements `Refcount`. By the axiom, the decrement is a full fence, so it is atomic with respect to concurrent decrements by the reader (there are no other writers). At the moment of the writer's decrement:

- If no segments from this buffer are outstanding (all have been retired), the balance `1 + increments - decrements` has been maintained by the reader's decrements catching up with the writer's increments, leaving `Refcount = 1` at the moment the writer enters `ReleaseHolder`. The writer's decrement drives it to 0; the writer performs the pool-return.
- If some segments are outstanding, `Refcount > 1`. The writer's decrement leaves it `> 0`; the writer does not return the buffer. Each subsequent reader retirement of a still-outstanding segment decrements by 1. The last of them drives `Refcount` to 0 and performs the pool-return.

**No double-free, no leak.** `Refcount` starts at 1 and receives exactly one increment per segment creation and one decrement per retirement plus one decrement from the writer's own `ReleaseHolder`. The total number of decrements equals the total number of increments plus the initial +1, so `Refcount` reaches 0 exactly once, and the pool-return runs exactly once. No segment is ever retired without a matching prior increment (segments are only reachable after the fence-ordered increment), so `Refcount` never goes negative and never reaches 0 while references remain.

**Why `Interlocked` is required on both sides.** The writer's `Interlocked.Increment` (creating segment N+1) can race with the reader's `Interlocked.Decrement` (retiring segment N) on the same `Refcount`. A non-atomic read-modify-write on either side would permit lost updates — the classic `x++` race. Both sides must use atomic RMW; `Interlocked.Increment`/`Decrement` provide this. The full-fence property is also needed on the writer side for the visibility argument above; a hypothetical weaker atomic increment with only acquire semantics would not suffice.

#### 6.5.4 Cost and rationale

`Interlocked.Increment`/`Decrement` execute once per segment boundary, not once per byte. For a workload writing in ~`MinimumSegmentSize`-sized chunks, this is roughly one pair per `MinimumSegmentSize` bytes — orders of magnitude less frequent than per-byte operations. The hot `Advance`/`GetMemory`/`ReadAsync`/`AdvanceTo` path remains free of interlocked operations.

Goal §1.1 bullet 2 is read as: **no interlocked operations on per-byte hot paths**. Interlocked is permitted at segment boundaries (buffer refcounting) and on awaiter arming (§8). This is consistent with what lock-free production SPSC implementations typically accept.

### 6.6 `void Complete(Exception? exception)`

```
1. If _unflushedStart < _activeBufferWritten:
       PublishActiveSegment()

2. If _activeBufferHolder != null:
       ReleaseHolder(_activeBufferHolder)       // §6.5.2; frees buffer if no
       _activeBufferHolder = null               //   segments still reference it

3. Set exception if provided:
       state.WriterException = ExceptionDispatchInfo.Capture(exception)
       (plain write; completion state release-store below establishes visibility)

4. Release-store completion:
       Volatile.Write(ref state.WriterCompletionState, 2)    // completed

5. Signal reader awaiter (§8.2).
```

### 6.7 `void CancelPendingFlush()`

Signals the flush awaiter to complete with `IsCanceled = true`. See §8.4 for the signaling protocol.

---

## 7. Reader operations

### 7.1 `ValueTask<ReadResult> ReadAsync(CancellationToken ct)` / `bool TryRead(out ReadResult)`

Algorithm (shared core, with `TryRead` skipping the await):

```
1. If ct.IsCancellationRequested: return canceled result.

2. If _readInProgress: throw (AdvanceTo not called).

3. Acquire-load tail and completion:
       tail = Volatile.Read(ref state.Tail)
       writerDone = Volatile.Read(ref state.WriterCompletionState) == 2

4. If _head == null and tail != null:
       // First read ever. Acquire-load the permanent head pointer set by the
       // writer's first publication (§6.4).
       _head = Volatile.Read(ref state.Head)
       _headConsumedOffset = 0

5. Determine the end of the available sequence:
       endSeg = tail
       endIdx = (tail != null) ? tail.BufferStart + tail.WrittenLength : 0

6. Determine whether there is new data past _examinedPosition:
       availableEndPosition = (endSeg != null)
           ? endSeg.RunningIndex + endSeg.WrittenLength
           : 0
       hasNewData = availableEndPosition > _examinedPosition

7. If hasNewData:
       buffer = new ReadOnlySequence<byte>(_head, _headConsumedOffset,
                                           endSeg, endIdx)
       result = new ReadResult(buffer, isCanceled: false,
                               isCompleted: writerDone)
       _readInProgress = true
       _lastReturnedBuffer = buffer
       return ValueTask.FromResult(result)

8. If writerDone:
       return ReadResult with empty buffer, IsCompleted = true

9. TryRead: return false.
   ReadAsync: arm the read awaiter (§8.3) and return its ValueTask.
```

**First-read initialization (step 4):** the writer publishes `state.Head` exactly once, during the first `PublishActiveSegment` call (§6.4). It is a permanent pointer to the first segment ever published and is never updated again. On the reader's first `ReadAsync`, `_head` is null and `tail` is non-null (at least one segment exists). The reader initializes `_head` by acquire-loading `state.Head`.

This is correct regardless of how many segments the writer has published before the reader's first read: `state.Head` always points to the first segment, and the reader traverses the complete chain from there. The earlier design of setting `_head = tail` was incorrect when the writer published multiple segments before the first read — it lost all segments except the most recent.

The release-acquire edge (writer's `Volatile.Write(ref state.Head, seg)` in §6.4 paired with the reader's `Volatile.Read(ref state.Head)`) ensures that all of the first segment's fields are visible to the reader. The reader then traverses forward from `_head` via `Volatile.Read(ref seg.Next)` as usual (§7.2).

### 7.2 The traversal invariant

When the reader holds `_head` and `tail = Volatile.Read(ref state.Tail)`, the reader may walk from `_head` forward via `Volatile.Read(ref seg.Next)` up to and including `tail`. Beyond `tail`, even if `seg.Next != null`, the reader must not traverse — those segments may have been published *after* the acquire-load of `tail` and are not part of the current `ReadOnlySequence`.

The `ReadOnlySequence<byte>` constructor with `endSegment`/`endIndex` enforces this automatically: traversal via the sequence's enumerator stops at `endSegment` regardless of its `Next`. That's what makes the design safe.

**Acquire semantics on `seg.Next`:** strictly required. A reader that plain-reads `prevSeg.Next` might observe the writer's pointer store before observing the writes that initialized the new segment. With `Volatile.Read(ref prevSeg.Next)` and the writer's `Volatile.Write(ref prevSeg.Next, newSeg)`, the release-acquire pair establishes visibility of `newSeg.*` fields.

### 7.3 `void AdvanceTo(SequencePosition consumed, SequencePosition examined)`

```
1. Validate _readInProgress, positions lie within _lastReturnedBuffer,
   and consumed <= examined. Throw InvalidOperationException or
   ArgumentOutOfRangeException on violation.

2. retiredBytes = 0
   current = _head
   consumedSeg = (Segment)consumed.GetObject()
   consumedIdx = consumed.GetInteger()
   currentTail = Volatile.Read(ref state.Tail)    // acquire; §10.7 invariant

   while current != consumedSeg:
       retiredBytes += current.WrittenLength - _headConsumedOffset
                       // first iteration uses _headConsumedOffset;
                       // subsequent iterations treat full segment
       next = Volatile.Read(ref current.Next)    // acquire
       if current != currentTail:                // §10.7: never retire state.Tail
           RetireSegment(current)
       current = next
       _headConsumedOffset = 0

   // current == consumedSeg. It is partially (or fully) consumed.
   offsetInCurrent = consumedIdx - current.BufferStart
   retiredBytes += offsetInCurrent - _headConsumedOffset
   _head = current
   _headConsumedOffset = offsetInCurrent

3. _bytesRead += retiredBytes

4. Compute new examined position:
   examinedSeg = (Segment)examined.GetObject()
   examinedIdx = examined.GetInteger()
   _examinedPosition = examinedSeg.RunningIndex
                       + (examinedIdx - examinedSeg.BufferStart)

5. Release-store the updated byte count:
   Volatile.Write(ref state.BytesReadPublished, _bytesRead)

6. Signal writer awaiter if it's waiting and we've drained below
   ResumeWriterThreshold. See §8.4.

7. _readInProgress = false
```

**`RetireSegment(seg)`:**
```
ReleaseHolder(seg.Holder)        // §6.5.2
seg.Holder = null
seg.Reset()
SegmentPool.Return(seg)
```

### 7.4 `void Complete(Exception? exception)`

```
1. Set state.ReaderException if provided:
       state.ReaderException = ExceptionDispatchInfo.Capture(exception)
       (plain write; completion state release-store below establishes visibility)

2. Release-store completion:
       Volatile.Write(ref state.ReaderCompletionState, 2)

3. Signal writer awaiter (§8.4).
```

Reader `Complete` does not retire segments or release buffer holders. All segment and holder cleanup is delegated to `SpscPipe.Dispose()` or `Reset()` (§10.6), which walk the segment chain after no operations are in flight. This avoids retiring segments that `state.Tail` still references (§10.7) and eliminates the race window where the writer publishes between a reader-side retirement walk and the writer observing `ReaderCompletionState`.

### 7.5 `void CancelPendingRead()`

Signals the read awaiter to complete with `IsCanceled = true`. See §8.3.

---

## 8. Awaiter coordination — the hard part

This section specifies how the writer waits for backpressure relief, and how the reader waits for new data, without locks. The challenge is closing the lost-wakeup race in both directions.

### 8.1 Awaiter state machine

Each awaiter (one per side) is an `int` with three states:

- `0 = Idle`: no wait in progress.
- `1 = Armed`: the waiter is parked; the signaler must wake it.
- `2 = Signaled`: the signaler has posted a wake; the waiter will observe it on next check.

Transitions are via `Interlocked.CompareExchange` in the arming and signaling paths. The waiter transitions `Idle → Armed → Idle`; the signaler transitions `Armed → Signaled`. The intermediate "arming" phase (deciding to wait, then re-checking for data before committing to park) is handled procedurally in §8.3 steps A–C, not as a discrete state value.

### 8.2 Writer signaling reader (`MaybeSignalReaderAwaiter`)

Called from `PublishActiveSegment` step 5 and `Complete` step 5. Preceded (in program order) by the release-stores that publish the segment (`Volatile.Write(ref state.Tail, seg)` and `Volatile.Write(ref state.BytesWrittenPublished, ...)`).

```csharp
// StoreLoad fence. Without this, the load of ReaderAwaiterState below could
// be reordered before the preceding release-stores of state.Tail and
// state.BytesWrittenPublished, which would permit the lost-wakeup race
// analyzed in §8.3.
Interlocked.MemoryBarrier();

// Fast path: if reader is not waiting, nothing to do.
var awaiterState = Volatile.Read(ref state.ReaderAwaiterState);
if (awaiterState == Idle) return;

// Try to transition Armed -> Signaled.
var prev = Interlocked.CompareExchange(
    ref state.ReaderAwaiterState, Signaled, Armed);
if (prev == Armed):
    // We won the race. Signal the reader with completion flags only.
    // The reader builds the real ReadResult from reader-local state when
    // the continuation resumes on the reader's scheduled thread (§8.7).
    var writerDone = Volatile.Read(ref state.WriterCompletionState) == 2;
    _reader._readAwaiter.SetResult(new ReadSignal(
        IsCanceled: false, IsCompleted: writerDone));
```

The `Interlocked.MemoryBarrier()` by the §5 axiom is a full sequentially-consistent fence: every store program-order before it is globally ordered before every load program-order after it. This orders the publication stores globally before the awaiter-state load, which is what the double-check protocol requires on this side.

The `Interlocked.CompareExchange` is on the signaling path only — rare, only when the reader was actually parked. Not on the per-byte hot path.

Cost of `Interlocked.MemoryBarrier()`: on x86 it lowers to a locked instruction (~20–30 cycles); on ARM64 to `dmb ish` (~10–20 cycles). Executed once per publication, not per byte.

### 8.3 Reader arming the read awaiter

Called from `ReadAsync` step 9.

This is where the double-check lives.

```csharp
// We've already checked (in step 3–7) that there's no data. We're about to wait.

// Step A: Transition Idle -> Armed.
var prev = Interlocked.CompareExchange(
    ref state.ReaderAwaiterState, Armed, Idle);
if (prev != Idle):
    // Signaler already fired (state was Signaled) or we were already armed
    // (shouldn't happen under SPSC invariant; the reader is single-threaded).
    // Reset to Idle and return a completed ValueTask by re-running the read
    // path (data is now available or writer is complete).
    Volatile.Write(ref state.ReaderAwaiterState, Idle);
    return RedoRead();

// Step B: StoreLoad fence (see rationale below). By the §5 axiom,
// Interlocked.MemoryBarrier() orders the preceding release-store of
// ReaderAwaiterState globally before the subsequent acquire-loads of
// state.Tail and state.WriterCompletionState. Without this fence,
// the two loads could observe values that pre-date the store, enabling
// the lost-wakeup race shown below.
Interlocked.MemoryBarrier();

// Step C: Re-check whether data arrived or writer completed.
tail = Volatile.Read(ref state.Tail);    // acquire
writerDone = Volatile.Read(ref state.WriterCompletionState) == 2;    // acquire

if (TailIndicatesNewDataPast(_examinedPosition, tail) || writerDone):
    // Data is actually available. Try to un-arm.
    var prev2 = Interlocked.CompareExchange(
        ref state.ReaderAwaiterState, Idle, Armed);
    if (prev2 == Armed):
        // Successfully unarmed. Return a synchronous result.
        return RedoRead();
    // prev2 == Signaled: the writer signaled between our Armed store and now.
    // The writer's signal already called SetResult. Our ValueTask will
    // complete synchronously from the ValueTaskSource.
    Volatile.Write(ref state.ReaderAwaiterState, Idle);    // reset for next time

// Step D: Register cancellation and return the awaiter's ValueTask.
_readCtr = ct.UnsafeRegister(static (s, t) =>
{
    var r = (SpscPipeReader)s!;
    var prev = Interlocked.CompareExchange(
        ref r.state.ReaderAwaiterState, Signaled, Armed);
    if (prev == Armed):
        r._readAwaiter.SetResult(new ReadSignal(IsCanceled: true, IsCompleted: false));
}, this);

// SpscPipeReader implements IValueTaskSource<ReadResult>; its GetResult
// retrieves the ReadSignal from _readAwaiter and builds the real ReadResult
// from reader-local state (safe: continuation runs on the reader's scheduler).
return new ValueTask<ReadResult>(this, _readAwaiter.Version);
```

**Why `Interlocked.MemoryBarrier()` in step B?** The double-check protocol requires that on both sides, the "publish my state" store happens globally before the "check other side's state" load. Release-acquire is not enough: a release-store followed by an acquire-load on the same thread may be reordered relative to other memory operations (StoreLoad reordering is permitted by release/acquire alone). Without the fence:

```
Thread A (reader arming):      Thread B (writer publishing):
  store.release Armed            store.release Tail = newSeg
  load.acquire  Tail             load.acquire  AwaiterState
```

On TSO hardware (x86/x64), stores and loads to different addresses can reorder (StoreLoad is the only direction not provided by TSO). On weaker architectures (ARM64) without an explicit fence, the stores can be globally observed in either order relative to the loads.

The race: both threads do their store, then their load. Both observe the *old* value of the other's store. Writer sees `AwaiterState == Idle`, doesn't signal. Reader sees `Tail == oldTail`, no new data, parks. Permanent sleep.

`Interlocked.MemoryBarrier()`, by the §5 axiom, is a full sequentially-consistent fence and thus orders the preceding store globally before the subsequent load. Both sides need their own fence; the writer's symmetric fence is in §8.2.

Note that the `Interlocked.CompareExchange` in Step A is also a full fence by the same axiom. One could in principle rely on that fence to cover Step C's load instead of inserting an additional barrier. We keep the explicit `Interlocked.MemoryBarrier()` in Step B for two reasons: (1) making the fence explicit at the point of need aids local reasoning; (2) the cost is a single fence either way, since a CAS on a just-modified cache line is approximately as expensive as a standalone fence on modern hardware.

### 8.4 Writer arming the flush awaiter — symmetric

`FlushAsync` step 6, after determining `outstanding >= PauseWriterThreshold`:

```csharp
// Step A: Idle -> Armed.
var prev = Interlocked.CompareExchange(
    ref state.WriterAwaiterState, Armed, Idle);
// ... (same structure as reader)

// Step B: full fence. By the §5 axiom, Interlocked.MemoryBarrier() orders the
// preceding release-store of WriterAwaiterState globally before the subsequent
// acquire-loads of BytesReadPublished and ReaderCompletionState. Same rationale
// as §8.3 Step B, symmetric.
Interlocked.MemoryBarrier();

// Step C: re-check backpressure.
bytesRead = Volatile.Read(ref state.BytesReadPublished);
readerDone = Volatile.Read(ref state.ReaderCompletionState) == 2;
outstanding = _bytesWritten - bytesRead;

if (outstanding < PauseWriterThreshold || readerDone):
    // Drained, or reader completed. Un-arm and return synchronously.
    var prev2 = Interlocked.CompareExchange(
        ref state.WriterAwaiterState, Idle, Armed);
    if (prev2 == Armed):
        return ValueTask.FromResult(new FlushResult(
            isCanceled: false, isCompleted: readerDone));
    // prev2 == Signaled: reader signaled. Fall through to the awaiter's ValueTask.
    Volatile.Write(ref state.WriterAwaiterState, Idle);

// Step D: register cancellation, return ValueTask.
// ...
```

Reader's `MaybeSignalWriterAwaiter` (called from `AdvanceTo` step 6 and `Complete`). Preceded in program order by `Volatile.Write(ref state.BytesReadPublished, _bytesRead)`:

```csharp
// StoreLoad fence. By the §5 axiom, orders the preceding release-store of
// BytesReadPublished globally before the subsequent awaiter-state load, so
// that a writer whose backpressure check missed our drain is guaranteed to
// have published its Armed state where we can see it. Symmetric to §8.2.
Interlocked.MemoryBarrier();

var awaiterState = Volatile.Read(ref state.WriterAwaiterState);
if (awaiterState == Idle) return;

// Verify we're actually below the resume threshold before waking (§8.5).
outstanding = Volatile.Read(ref state.BytesWrittenPublished) - _bytesRead;
if (outstanding >= ResumeWriterThreshold) return;

var prev = Interlocked.CompareExchange(
    ref state.WriterAwaiterState, Signaled, Armed);
if (prev == Armed):
    _writer._flushAwaiter.SetResult(new FlushResult(
        isCanceled: false,
        isCompleted: Volatile.Read(ref state.ReaderCompletionState) == 2));
```

### 8.5 Why check the resume threshold before signaling?

If the reader retires 1 byte and the writer is parked at `PauseWriterThreshold = 65536`, waking the writer now would cause it to immediately re-check, see `outstanding = 65535`, and re-park. Waste.

The resume threshold (`ResumeWriterThreshold ≤ PauseWriterThreshold`) gives hysteresis. The reader signals only when it has drained below resume. The writer is guaranteed, on wake, to see `outstanding < ResumeWriterThreshold` (modulo further writer activity, which in SPSC means no concurrent writer — so actually guaranteed).

Corresponding logic on the reader-wake side (`MaybeSignalReaderAwaiter`): just check that there's data or writer completion. No hysteresis needed on that side because the reader has no equivalent of "minimum batch size" in the API.

### 8.6 Cancellation integration

The awaiter state machine already handles cancellation as just another signaler. The cancellation callback does the same `Armed → Signaled` CAS, with a different result payload. The only subtlety: the callback might fire concurrently with the regular signaler. CAS resolves the race: one succeeds, the other is a no-op.

`ct.UnsafeRegister` is used instead of `Register` to avoid capturing ExecutionContext on the hot path.

### 8.7 `ManualResetValueTaskSourceCore` interaction

The awaiter state we maintain (`ReaderAwaiterState`, `WriterAwaiterState`) is a separate coordination layer *on top* of `ManualResetValueTaskSourceCore<T>`. The latter handles the continuation delivery (running the `await` continuation with the configured scheduler); our state machine handles the "do I need to signal?" decision.

Specifically:
- When the waiter transitions `Idle → Armed`, it calls `_awaiter.Reset()` and prepares to return `new ValueTask<T>(source, _awaiter.Version)`, where `source` is the containing class implementing `IValueTaskSource<T>`.
- When the signaler wins the CAS `Armed → Signaled`, it calls `_awaiter.SetResult(...)`, which schedules the continuation.
- The waiter's `ValueTask` completes when the continuation runs.
- `Version` disambiguates reuse of the `ValueTaskSource` — each arm cycle increments it via `Reset()`.

**Reader-side translation.** The reader's `ManualResetValueTaskSourceCore` carries `ReadSignal` (§4.4), not `ReadResult`. `SpscPipeReader` implements `IValueTaskSource<ReadResult>` and bridges the gap: its `GetResult` retrieves the `ReadSignal` from the core, resets `ReaderAwaiterState` to `Idle`, disposes the cancellation registration, and — for non-canceled signals — builds the real `ReadResult` by re-executing the read logic (§7.1 steps 3–7) from reader-local state. This runs on the reader's scheduled continuation thread, so accessing reader-local fields is safe under the SPSC invariant.

The writer-side flush awaiter does not need this treatment: `FlushResult` depends only on writer-local and shared state that the reader (the signaler) can safely read.

`ManualResetValueTaskSourceCore` is itself using `Interlocked.CompareExchange` internally for its continuation slot. That's unavoidable for any `IValueTaskSource`-backed awaiter and is considered acceptable; the "no Interlocked on hot path" goal applies to our bespoke coordination, not to the .NET async infrastructure.

### 8.8 Scheduler integration

`PipeScheduler.Schedule` is invoked by `ManualResetValueTaskSourceCore` when delivering the continuation. Configuration (`ReaderScheduler`, `WriterScheduler`, `UseSynchronizationContext`) determines which scheduler is used. Matches BCL behavior; no new design.

---

## 9. Pools

Three pools are used:

- **Segment pool**: a bounded concurrent pool (e.g., `Microsoft.Extensions.ObjectPool.DefaultObjectPool<Segment>`). Rented by the writer during publication; returned by the reader on retirement. Both sides contend, so the pool's own synchronization applies.
- **`BufferHolder` pool**: same structure. Rented by the writer during buffer rental; returned by whichever side drives the refcount to zero (§6.5.2).
- **Buffer pool**: `MemoryPool<byte>` supplied via `SpscPipeOptions.Pool`, defaulting to an adapter over `ArrayPool<byte>.Shared`.

Default pool sizing targets `2 × (PauseWriterThreshold / MinimumSegmentSize)` concurrent outstanding segments/holders. Over-sizing wastes memory; under-sizing causes allocations on bursty workloads. Tunable if profiling justifies it.

---

## 10. Correctness notes

### 10.1 SPSC invariant

At most one thread may enter any `PipeWriter` method at a time; same for `PipeReader`. Violations are undefined behavior. Implementation does not check in release builds.

Debug builds may include a `[Conditional("DEBUG")]` thread-identity check at method entry.

### 10.2 `AdvanceTo` validation

Positions must come from the most recent `ReadAsync` result. Tracked via `_readInProgress` flag and `_lastReturnedBuffer`. Violations throw.

### 10.3 Completion ordering

Writer completes → reader sees `WriterCompletionState == 2` on next `ReadAsync`'s acquire-load of that field. Bytes published before completion are visible via the normal mechanism (release-store of `Tail` and `BytesWrittenPublished`).

Reader completes → writer sees `ReaderCompletionState == 2` on next `FlushAsync`'s acquire-load. Reader `Complete` does not retire segments (§7.4); all segment and holder cleanup is performed by `SpscPipe.Dispose()` or, as a safety net, the finalizer (§10.6).

### 10.4 Exception propagation

Writer completion with exception: the `WriterException` field is written before the release-store of `WriterCompletionState`. Reader, on acquiring `WriterCompletionState == 2`, reads `WriterException` (plain read is sufficient due to the happens-before edge from the completion state's release-acquire).

Reader's `ReadAsync`, when returning a `ReadResult` with `IsCompleted = true`, does not throw. The reader surfaces the exception by throwing on the *next* `ReadAsync` after all bytes are drained. Matches BCL behavior (`ReadResult` is the normal path; exceptions surface only when there's nothing left to deliver).

Symmetric for reader-to-writer exception propagation on `FlushAsync`.

### 10.5 Reset

`Reset()` requires both ends completed, with no operations in flight. Performs the same segment cleanup walk as `Dispose()` (§10.6): starts from `_head` if non-null, otherwise from `state.Head` (§10.6 step 1). Releases holders and returns buffers and segments to their pools. Zeroes all fields including `state.Head`. Re-initializes both awaiters (incrementing `Version`).

### 10.6 Disposal and finalization

`SpscPipe` implements `IDisposable`. `Dispose()` performs a deterministic cleanup of any segments and buffers still outstanding — including segments the reader never retired (reader `Complete` delegates all cleanup here, §7.4) and any orphaned segments published after reader completion.

```
Dispose():
1. Determine walk root:
       start = _head ?? state.Head
   _head is non-null if the reader ever called ReadAsync. It points to the
   first non-retired segment — the chain from here through state.Tail and
   beyond covers all live segments, including any orphans the writer published
   after reader completion. If _head is null (reader never read), state.Head
   is still valid (never retired) and serves as the fallback.

2. Walk from start forward, following Next pointers, until null.
   For each segment:
       if seg.Holder != null:
           ReleaseHolder(seg.Holder)    // §6.5.2
           seg.Holder = null
       seg.Reset()
       SegmentPool.Return(seg)

3. If _activeBufferHolder != null (writer never rotated/completed):
       ReleaseHolder(_activeBufferHolder)
       _activeBufferHolder = null

4. Null out state.Head, state.Tail. Zero byte counters.
```

`Dispose()` is safe to call after both sides have completed, or after only one side has completed (e.g., the writer completed but the reader abandoned). It is **not** safe to call concurrently with active reader or writer operations — the caller must ensure no operations are in flight.

**Finalizer.** `SpscPipe` includes a weak safety-net finalizer that calls the same cleanup walk. For `ArrayPool<byte>.Shared`-backed buffers this prevents pool pressure under abandoned pipes. For custom `MemoryPool<byte>` implementations backed by pinned or native memory (§13), it prevents genuine resource leaks. The finalizer is suppressed by `Dispose()` via `GC.SuppressFinalize(this)`. Note: the finalizer reads `_head` (reader-local) and `_activeBufferHolder` (writer-local) without explicit `Volatile.Read`. This relies on the GC's stop-the-world phase inducing a full memory barrier before the finalizer thread runs — a property held in practice by the .NET runtime but not formally documented. If this reliance is unacceptable, the finalizer should use `Volatile.Read` for its initial traversal loads.

**`Reset()` vs `Dispose()`:** `Reset()` (§10.5) performs the same segment/holder cleanup walk (starting from `_head ?? state.Head`), then re-initializes the pipe for reuse. `Dispose()` performs cleanup only and leaves the pipe in a terminal state. Both share the same internal walk logic.

### 10.7 Tail-segment retirement invariant

The segment referenced by `state.Tail` is never returned to the segment pool while `state.Tail` references it. The writer relies on `prevTail = state.Tail` followed by `Volatile.Write(ref prevTail.Next, newSeg)` during publication (§6.4); if the reader had retired that segment and returned it to the pool, the write would corrupt whatever consumer now holds the recycled object.

This invariant is maintained by:

- **`AdvanceTo` (§7.3):** acquires `state.Tail` and does not retire any segment identity-equal to it. In practice, `consumedSeg` is always at or before `state.Tail` in the chain, so the loop body never reaches the tail — the check is a mechanical safety net.
- **Reader `Complete` (§7.4):** does not retire segments at all; cleanup is delegated to `Dispose` / `Reset`.
- **`Dispose` / `Reset` (§10.6, §10.5):** run only when no operations are in flight, so `state.Tail` is never concurrently accessed by the writer.

---

## 11. Memory ordering summary table

| Location | Writer | Reader | Ordering |
|---|---|---|---|
| `state.Tail` | W | R | release-store (writer) / acquire-load (reader) |
| `state.Head` | W (once per lifecycle) | R (once per lifecycle) | release-store (writer, first publication) / acquire-load (reader, first read); zeroed by `Reset()` |
| `seg.Next` (for any seg) | W (once) | R | release-store (writer) / acquire-load (reader) |
| `seg.*` initialization fields | W | R | plain; visibility covered by release-store of `Tail` or `prevSeg.Next` |
| `state.BytesWrittenPublished` | W | R | release-store / acquire-load |
| `state.BytesReadPublished` | R | W | release-store / acquire-load |
| `state.WriterCompletionState` | W | R | release-store / acquire-load |
| `state.ReaderCompletionState` | R | W | release-store / acquire-load |
| `state.WriterException` | W | R | plain; visibility covered by completion-state release-store |
| `state.ReaderException` | R | W | plain; visibility covered by completion-state release-store |
| `state.ReaderAwaiterState` | CAS / full fence | CAS / full fence | `Interlocked`; full fence required between arm and re-check (§8.3) and between publish and awaiter-read (§8.2) |
| `state.WriterAwaiterState` | CAS / full fence | CAS / full fence | `Interlocked`; symmetric to `ReaderAwaiterState` (§8.4) |
| `BufferHolder.Refcount` | `Interlocked.Increment` | `Interlocked.Decrement` | `Interlocked` supplies atomicity and full fence (§5 axiom); writer's increment is ordered globally before its subsequent release-stores that publish the segment (§6.5.3) |
| `seg.Holder` | W | R | plain; visibility covered by release-store of `prevSeg.Next` or `state.Tail` |

Explicit full-fence requirements (`Interlocked.MemoryBarrier()` or equivalent Interlocked op, by the §5 axiom) are at:

- Writer after publication, before reading reader-awaiter state (§8.2).
- Reader after arming read-awaiter, before re-checking `state.Tail` / `state.WriterCompletionState` (§8.3).
- Reader after publishing `BytesReadPublished`, before reading writer-awaiter state (§8.4).
- Writer after arming write-awaiter, before re-checking `BytesReadPublished` / `state.ReaderCompletionState` (§8.4).

---

## 12. Performance targets

Benchmarked against `System.IO.Pipelines.Pipe`:

| Metric | Target |
|---|---|
| Steady-state allocations per round-trip | 0 bytes |
| Throughput (single-core, pinned threads) | ≥ 2.5× BCL Pipe |
| `GetMemory` → `Advance` → `FlushAsync` uncontended | ≤ 25 ns |
| `ReadAsync` → `AdvanceTo` uncontended | ≤ 30 ns |
| Cross-core throughput (producer/consumer on sibling cores) | ≥ 2× BCL Pipe |
| Pathological small-flush case (1-byte writes + flush) | no slot exhaustion; throughput ≥ 1× BCL Pipe |

The slow path (with awaiter round-trip) is not a performance target — it only fires under backpressure or empty reads, where the cost is dominated by the `ValueTaskSource` delivery and scheduler hop.

---

## 13. Open questions

1. **Writer `GetMemory` adaptive sizing.** The BCL grows segment size based on prior utilization. `SpscPipe` uses a flat `MinimumSegmentSize`. For v1, keep flat. Revisit if profiles show consistent waste.

2. **Native memory backing.** The `MemoryPool<byte>` extension point supports this directly. Pinned native buffers for AF_XDP adjacency work without spec changes.

3. **Cross-NUMA behavior.** Unmeasured. If the reader and writer land on different NUMA nodes, the cache-line layout still applies but inter-socket latency dominates. Out of scope for v1.

4. **Segment pool contention.** The segment pool is accessed concurrently. If profiling shows this as a bottleneck, split into two pools: "writer-rent, reader-return" via an SPSC queue. Measure before committing.

5. **Eliding Interlocked on the refcount.** Under very high publication rates the per-segment `Interlocked.Increment` on `BufferHolder.Refcount` may be visible in profiles. A design that partitions the counter into writer-local and reader-local halves (exchanging deltas at buffer rotation) would remove it from the hot path at the cost of additional complexity. Measure before committing.

---

## 14. Out of scope for v1

- `Stream` adapter (works unchanged via `PipeReader.AsStream` / `PipeWriter.AsStream`).
- `Socket.SendAsync(ReadOnlySequence<byte>)` integration (works unchanged; `ReadOnlySequence<byte>` is the boundary).
- Duplex pipe as a single object (compose two `SpscPipe`s).
- Multi-producer or multi-consumer variants. Require CAS on `Tail`/`Head`; entirely different design.
