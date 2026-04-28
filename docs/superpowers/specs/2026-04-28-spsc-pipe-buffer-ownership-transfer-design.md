# SpscPipe Buffer Ownership Transfer (`SpscPipeWriter.Append`) — Design

**Date:** 2026-04-28
**Status:** Spec (pre-implementation). Adds zero-copy buffer donation to the writer surface; amends `2026-04-25-spsc-pipe-tripleBuffer-design.md` §3 / §4 / §X.

## Top-level key takeaways

- New writer-side method `SpscPipeWriter.Append(IMemoryOwner<byte> buffer[, int start, int length])`. Caller transfers ownership of an already-filled buffer; the pipe takes responsibility for `Dispose`.
- Donated buffers ride the existing segment chain. A donated `BufferSegment` is constructed with `End == length` and is spliced in as the new tail via the existing `Freeze`/transition machinery. No parallel state, no new chain.
- `Append` is writer-thread-local — same publication boundary as `GetMemory`/`Advance`. Nothing becomes visible to the reader until the next `FlushAsync` (or `Complete`).
- Recycle path forks on a new `BufferSegment.IsDonated` discriminator: rented segments → `PushFreelist` (existing); donated → `DisposeOwned()` and discard. Foreign `IMemoryOwner`s are never reused.
- Public surface impact: `SpscPipeWriter` becomes `public`; `SpscPipe.Writer`'s declared return type widens from `PipeWriter` to `SpscPipeWriter`. Source-compatible with existing `PipeWriter w = pipe.Writer;` call sites. `SpscPipeReader` remains internal.
- Ownership contract: ownership of `buffer` transfers to the pipe **iff** `Append` returns normally. On any throw (argument, disposed, completed), the caller still owns and must `Dispose`.

## Section 1 — Background and motivation

The current `PipeWriter` surface (`GetMemory(int sizeHint)` + `Advance(int bytes)`) requires the caller to copy bytes into a pipe-rented segment before they are visible. For workloads that already have data in their own `IMemoryOwner<byte>` (e.g., a parser that owns a frame, a pre-built protocol message in a pooled buffer), this copy is pure overhead. The cost shows up as both bandwidth (copy throughput) and tail-latency jitter (extra L1/L2 pressure).

`Append` adds a zero-copy alternative. The caller offers an already-filled buffer along with its `IMemoryOwner<byte>`; the pipe splices the buffer into the chain in O(1) without copying. Ownership of the `IMemoryOwner` transfers to the pipe, which disposes it when the reader has drained past or when the pipe itself is disposed.

The design intentionally complements `GetMemory`/`Advance` rather than replaces it. Callers that build messages incrementally still use the existing path; callers that already have a complete buffer use `Append`. The two are interleavable in any order.

## Section 2 — Architecture and the splice

### 2.1 — The donated segment is a chain segment

A donated buffer becomes a `BufferSegment` like any other, with three differences from a rented one:

| Aspect | Rented (today) | Donated (new) |
|---|---|---|
| `_memoryOwner` source | `_options.Pool.Rent(sizeHint)` | Caller-supplied `IMemoryOwner<byte>` |
| Size | `≥ MinimumSegmentSize` | Donor-decided (any positive size) |
| `AvailableMemory` | Full pool rental | `owner.Memory.Slice(start, length)` |
| `End` at link time | Set by `Freeze(bytesFilled, ...)` at transition | Set to `length` immediately at `AdoptFrom` |
| Lifecycle on recycle | `PushFreelist` (or `DisposeOwned` if cap-overflow) | Always `DisposeOwned`; `BufferSegment` shell discarded |
| `OwnerToken` | `pipe` | `pipe` (same — keeps R4-7 pipe-identity check working) |

The reader's view of a donated segment is identical to any other chain segment: it appears as `WriterState.HeadSegment` or `TailSegment`, contributes its bytes to the `ReadOnlySequence<byte>`, and supports `AdvanceTo` to a `SequencePosition` inside it.

### 2.2 — The splice into `_writingHead`

`Append` performs the same kind of tail transition that `GetMemory` does when its `sizeHint` exceeds the current tail's remaining capacity:

1. After validation, capture `slice = buffer.Memory.Slice(start, length)` once (see §4 for the rationale on caching `buffer.Memory`).
2. Compute `runningIndex = _writingHead.RunningIndex + _writingHeadBytesBuffered` (or `0` on the bootstrap path with `_writingHead == null`).
3. Allocate a fresh `BufferSegment`; call `AdoptFrom(buffer, slice, runningIndex, pipeOwner: pipe)`.
4. If `_writingHead != null`, call `_writingHead.Freeze(_writingHeadBytesBuffered, donated)` to finalize the previous tail and link it to the donated segment.
5. Update `_writingHead = donated`, `_writingHeadBytesBuffered = length`, `_totalWritten += length`.

The `Freeze` call in step 3 works uniformly whether the previous `_writingHead` was rented (its `End`/`base.Memory` are being set for the first time) or donated from a prior `Append` (its `End`/`base.Memory` are written to the same values they already held — the writes are idempotent; only `Next` materially changes). The benign-torn-read note from §2 of the base spec applies unchanged: a reader iterating during the redundant `base.Memory` write observes the same `_object`/`_index` and only `_length` could in principle tear, but pre and post values are identical.

No publish, no signal, no awaiter interaction — `Append` is purely writer-thread-local. The new state becomes visible at the next `FlushAsync`.

### 2.3 — Why a chain segment, not a side-channel

A side-channel that walked donated buffers separately during reads (e.g., interleaving them with chain segments via a parallel queue) would require new state, new read-path logic, and new `WriterState` shape — for no observable benefit. The chain already gives ordered iteration, `RunningIndex` arithmetic for `SequencePosition`, and a single recycle predicate. Donated buffers fit the chain naturally; there is no reason to introduce a parallel structure.

## Section 3 — `BufferSegment` changes

```csharp
internal sealed class BufferSegment : ReadOnlySequenceSegment<byte>
{
    private IMemoryOwner<byte>? _memoryOwner;

    public Memory<byte>      AvailableMemory { get; private set; }
    public int               End             { get; private set; }
    public new BufferSegment? Next           { get; private set; }
    public object?           OwnerToken      { get; private set; }
    public bool              IsDonated       { get; private set; }   // NEW

    public void RentFrom(MemoryPool<byte> pool, int sizeHint, long runningIndex, object owner)
    {
        _memoryOwner      = pool.Rent(sizeHint);
        AvailableMemory   = _memoryOwner.Memory;
        base.Memory       = AvailableMemory;
        base.RunningIndex = runningIndex;
        End               = 0;
        Next              = null;
        base.Next         = null;
        OwnerToken        = owner;
        IsDonated         = false;          // NEW: explicit reset on every rent
    }

    // NEW. Caller passes a pre-computed slice — keeps `AdoptFrom` allocation- and throw-free
    // and avoids re-querying `owner.Memory` (which is not contractually stable across calls
    // for arbitrary `IMemoryOwner` implementations).
    // Preconditions (caller-validated; asserted in DEBUG):
    //   owner != null, slice.Length > 0, slice originates from owner.Memory.
    public void AdoptFrom(IMemoryOwner<byte> owner, Memory<byte> slice, long runningIndex, object pipeOwner)
    {
        _memoryOwner      = owner;
        AvailableMemory   = slice;          // donated capacity == published slice; no spare
        base.Memory       = slice;
        base.RunningIndex = runningIndex;
        End               = slice.Length;
        Next              = null;
        base.Next         = null;
        OwnerToken        = pipeOwner;
        IsDonated         = true;
    }

    public void Freeze(int bytesFilled, BufferSegment? next)
    {
        End         = bytesFilled;
        base.Memory = AvailableMemory.Slice(0, bytesFilled);
        Next        = next;
        base.Next   = next;
        // No IsDonated touch — Freeze on a donated segment is idempotent on End/Memory
        // (length == AvailableMemory.Length already), only Next changes.
    }

    public void RecycleReset(long runningIndex)
    {
        Debug.Assert(!IsDonated, "RecycleReset must not be called on donated segments.");
        // ...existing body unchanged...
    }

    public void DisposeOwned()
    {
        _memoryOwner?.Dispose();
        _memoryOwner    = null;
        AvailableMemory = default;
        // Works uniformly: _memoryOwner is the rented or adopted owner depending on path.
    }

    public void SetFreelistNext(BufferSegment? next) { /* unchanged */ }
}
```

### 3.1 — On `IsDonated`

The flag's only purpose is to discriminate the recycle path: donated segments must dispose their `IMemoryOwner` (foreign; cannot pool) and the `BufferSegment` shell is discarded with them. It is not exposed publicly and is not consulted on read-side paths — readers see donated segments as ordinary chain segments and need no awareness of their origin.

### 3.2 — `AdoptFrom` precondition and `length == 0`

`AdoptFrom`'s precondition is `slice.Length > 0`. The `length == 0` case is intercepted in `Append` before `AdoptFrom` is called: the buffer is disposed synchronously and the method returns. This keeps `AdoptFrom`'s state-shape invariants clean (every donated segment in the chain contributes ≥1 byte and has a non-empty `Memory`).

### 3.3 — Memory torn-read note (§3 Nit-5 of base spec)

Donated segments are immune to the documented benign-torn-read concern. `base.Memory` is set once at `AdoptFrom` to the slice and never re-written by `Freeze` (the same value would be re-written, which is also benign — same `_object`/`_index`). Rented segments retain the existing concern unchanged.

## Section 4 — `Append` flow

### 4.1 — Pseudocode

```csharp
public void Append(IMemoryOwner<byte> buffer)
{
    if (buffer is null) throw new ArgumentNullException(nameof(buffer));
    Append(buffer, 0, buffer.Memory.Length);
}

public void Append(IMemoryOwner<byte> buffer, int start, int length)
{
    if (buffer is null) throw new ArgumentNullException(nameof(buffer));

    // State-precondition checks. On throw, caller still owns `buffer`.
    if (_pipe._disposed) throw new ObjectDisposedException(nameof(SpscPipe));
    if (_pipe._writerCompleted) throw new InvalidOperationException("Writing is completed.");

    // Range validation against `buffer.Memory.Length`.
    // Capture `buffer.Memory` once; reuse for validation and slicing. `IMemoryOwner.Memory`
    // is not contractually stable across calls for arbitrary implementations, so re-querying
    // would risk validate/adopt divergence.
    var mem = buffer.Memory;
    if ((uint)start > (uint)mem.Length) throw new ArgumentOutOfRangeException(nameof(start));
    if ((uint)length > (uint)(mem.Length - start)) throw new ArgumentOutOfRangeException(nameof(length));

    // Q5: zero-length is accept-and-dispose, no chain mutation.
    if (length == 0)
    {
        buffer.Dispose();
        return;
    }

    var slice = mem.Slice(start, length);   // guaranteed to succeed after validation above

    // Bootstrap: pipe has no writing head yet.
    if (_pipe._writingHead == null)
    {
        var donated = new BufferSegment();
        donated.AdoptFrom(buffer, slice, runningIndex: 0, pipeOwner: _pipe);
        _pipe._chainHead   = donated;
        _pipe._writingHead = donated;
        _pipe._writingHeadBytesBuffered = length;
        _pipe._totalWritten += length;
        return;
    }

    // Steady state: freeze current tail with whatever's buffered, splice donated as new tail.
    int filled = _pipe._writingHeadBytesBuffered;
    long newRI = _pipe._writingHead.RunningIndex + filled;

    var newDonated = new BufferSegment();
    newDonated.AdoptFrom(buffer, slice, newRI, pipeOwner: _pipe);

    _pipe._writingHead.Freeze(filled, newDonated);
    _pipe._writingHead = newDonated;
    _pipe._writingHeadBytesBuffered = length;
    _pipe._totalWritten += length;
}
```

### 4.2 — Validation order and ownership transfer

All validation runs **before** any mutation of `_chainHead`, `_writingHead`, `_writingHeadBytesBuffered`, or `_totalWritten`. Combined with the contract that ownership transfers iff the call returns normally, this means:

- `ArgumentNullException` (null buffer): nothing was passed; nothing transferred.
- `ObjectDisposedException` / `InvalidOperationException` (state precondition): caller still owns `buffer`; pipe state unchanged.
- `ArgumentOutOfRangeException` (range): caller still owns `buffer`; pipe state unchanged.
- Normal return: pipe owns `buffer` and will dispose it on recycle or `SpscPipe.Dispose`.

After validation, the body's only operations are `new BufferSegment()` (a parameterless constructor that doesn't throw under normal conditions) and `AdoptFrom` (allocation- and throw-free by construction — see §3). The chain pointer updates and scalar increments that follow cannot throw. So the post-validation body has no observable mid-state: either we return normally and ownership has transferred, or validation threw and the caller's buffer is untouched.

### 4.3 — Post-Append state and natural protections

After `Append`, `_writingHead.AvailableMemory.Length == _writingHeadBytesBuffered == length`. Two consequences fall out without code changes:

- **Subsequent `GetMemory(N)` transitions** because `remaining = AvailableMemory.Length - _writingHeadBytesBuffered = 0 < N`. The existing transition path rents a fresh tail and freezes the donated segment as a non-tail chain segment.
- **Subsequent `Advance(N>0)` throws** via the existing bounds check at `SpscPipe.Writer.cs:52` (`_writingHeadBytesBuffered + bytes > _writingHead.AvailableMemory.Length`). Callers cannot accidentally over-write into a donated segment.

### 4.4 — Backpressure interaction

`Append` increments `_totalWritten` without consulting `_options.PauseWriterThreshold`. A pathological caller could `Append` arbitrarily many bytes before flushing. This matches the existing semantic for `GetMemory(huge)` + `Advance(huge)` — backpressure is a `FlushAsync`-time concern, not a staging-time one. The next `FlushAsync` observes the boosted `_totalWritten - _lastAcquiredReaderState.TotalConsumed` and parks if needed.

## Section 5 — Recycle path

```csharp
internal void RecycleDrainedSegments()
{
    var r = _lastAcquiredReaderState;
    if (r.HeadSegment is null && !r.IsCompleted) return;     // pre-bootstrap (I10)

    var readerHead = r.HeadSegment;
    while (_chainHead != _writingHead && _chainHead != readerHead)
    {
        var recycled = _chainHead!;
        _chainHead   = recycled.Next!;

        if (recycled.IsDonated)
            recycled.DisposeOwned();         // foreign owner: release; drop the shell
        else
            PushFreelist(recycled);          // existing pool-rented path (with cap-overflow handling inside)
    }
}
```

The walk predicate is unchanged — `IsDonated` only affects what happens after the predicate has decided to advance past a segment. The reference comparison rationale (§3 Nit-4 of base spec) applies uniformly to both kinds of segment.

`PushFreelist` itself is unchanged. By the time we reach it, the segment is rented; its existing cap-overflow disposal branch continues to work.

The TripleBuffer slot-pinning caveat (§3 Nit-6 of base spec) carries over: a recycled donated segment may stay reachable via the unused `_writerTb` slot until the next publish overwrites that slot's `WriterState.HeadSegment`/`TailSegment` references. The foreign owner is alive slightly longer than the recycle moment, but never past the next publish. Same caveat as today's rented segments; not worth additional complexity.

`SpscPipe.Dispose()` (`SpscPipe.cs:71-90`) walks both the chain and the freelist calling `DisposeOwned()` on each. The path is correct for donated segments without modification — `DisposeOwned` releases whichever `IMemoryOwner` the segment holds, regardless of how it was acquired.

## Section 6 — Public API surface

```csharp
namespace SpscPipelines;

public sealed class SpscPipeWriter : System.IO.Pipelines.PipeWriter
{
    internal SpscPipeWriter(SpscPipe pipe);   // ctor stays internal — only SpscPipe constructs it

    // Existing PipeWriter overrides — unchanged:
    public override Memory<byte> GetMemory(int sizeHint = 0);
    public override Span<byte>   GetSpan(int sizeHint = 0);
    public override void         Advance(int bytes);
    public override ValueTask<FlushResult> FlushAsync(CancellationToken ct = default);
    public override void         Complete(Exception? exception = null);
    public override void         CancelPendingFlush();

    // New:
    public void Append(IMemoryOwner<byte> buffer);
    public void Append(IMemoryOwner<byte> buffer, int start, int length);
}

public sealed partial class SpscPipe : IDisposable
{
    public SpscPipeWriter Writer => _writerInstance;   // was: PipeWriter
    public PipeReader     Reader => _readerInstance;   // unchanged
}
```

### 6.1 — Source compatibility

`SpscPipeWriter : PipeWriter`, so existing call sites that bind the result of `pipe.Writer` to a `PipeWriter` variable continue to compile via implicit upcast:
```csharp
PipeWriter w = pipe.Writer;          // still works — implicit upcast
var w2 = pipe.Writer;                // now SpscPipeWriter; .Append available
SpscPipeWriter w3 = pipe.Writer;     // explicit binding
```

### 6.2 — Why `SpscPipeReader` stays internal

The asymmetry is intentional. `Append` is the only addition beyond the BCL `PipeWriter` surface; the reader has no analogous addition. Exposing `SpscPipeReader` for symmetry alone would commit us to maintaining a public type with no extra public methods on it — pure surface area for no benefit. If a reader-side feature is ever motivated (none currently is), the type can be made public at that point.

### 6.3 — `Append` ownership contract

> `Append` takes ownership of `buffer` if and only if the call returns normally.
> On any exception thrown by `Append`, the caller still owns `buffer` and is responsible for disposing it.

This matches the standard C# resource-handoff idiom and aligns with `Stream.Write` (which does not consume a buffer it failed to write) and `Channel.Writer.WriteAsync` (which does not consume on validation failure). Idiomatic caller code:
```csharp
var buf = MemoryPool<byte>.Shared.Rent(N);
try
{
    Fill(buf.Memory.Span);
    writer.Append(buf, 0, N);    // ownership transferred on success
    buf = null;                  // null the local so the catch doesn't double-dispose
}
finally
{
    buf?.Dispose();              // disposes only on failure; no-op on success
}
```

A simpler "always-take-ownership-even-on-throw" alternative was considered and rejected: silently disposing a buffer the user passed to a method that then threw `InvalidOperationException` is surprising and invites bug classes (e.g., post-`Complete` Append silently destroying a buffer the caller intended to use elsewhere).

## Section 7 — Interactions with existing invariants

| Invariant / mechanism | Effect of `Append` |
|---|---|
| Head==tail discipline (§2 I3/I4) | Unaffected. A donated segment is never the live tail being written into. The previously-rented tail's freeze during splice follows the existing freeze rules (set `End`/`Memory`/`Next` before `Publish`). |
| `WriterState` shape (§2) | Unchanged. Donated segments are addressable by `WriterState.HeadSegment`/`TailSegment` like any chain segment. |
| `RunningIndex` monotonicity (§2 I2) | Maintained. `donated.RunningIndex = previousTail.RunningIndex + previousTail._writingHeadBytesBuffered`. |
| Reader bootstrap (§3 I10) | Unaffected. If `Append` is the first writer op, the donated segment becomes `WriterState.HeadSegment` and the reader bootstraps to it on first `TryAcquire`. |
| Recycle predicate (§3 Nit-4) | Unaffected. Reference comparison `_chainHead != readerHead` is independent of `IsDonated`. |
| `OwnerToken` / R4-7 pipe-identity check | Maintained. `AdoptFrom` sets `OwnerToken = pipe`, so cross-pipe `SequencePosition` detection in `AdvanceTo` works on donated segments identically to rented ones. |
| `Memory<T>` benign-torn-read (§3 Nit-5) | Donated segments are immune (`base.Memory` is written once at `AdoptFrom`, then idempotently by `Freeze` if at all). Rented segments retain the existing benign-torn-read property. |
| Awaiter coordination (§5) | Unaffected. `Append` does not signal, park, or interact with awaiters. |
| Completion / cancellation (§6) | Unaffected. `Writer.Complete` after `Append` is identical to `Writer.Complete` after `GetMemory`/`Advance`. |
| TripleBuffer slot pinning (§3 Nit-6) | Same caveat applies. A recently-recycled donated segment may stay reachable via the unused `_writerTb` slot until the next publish overwrites it. |

## Section 8 — Edge cases (catalog)

| Scenario | Expected behavior |
|---|---|
| `Append(null)` (either overload) | `ArgumentNullException`; pipe state unchanged. |
| `Append(buf)` with `buf.Memory.Length == 0` | `length == 0` after defaulting; goto zero-length branch → `buf.Dispose()`, return. |
| `Append(buf, start, 0)` | Zero-length branch → `buf.Dispose()`, return. No chain mutation. |
| `Append(buf, -1, ...)` / `Append(buf, ..., -1)` / `Append(buf, start, length)` with `start + length > buf.Memory.Length` | `ArgumentOutOfRangeException`; caller still owns `buf`; pipe state unchanged. |
| `Append` on a pipe with `_disposed == true` | `ObjectDisposedException`; caller still owns; pipe state unchanged. |
| `Append` after `Writer.Complete(...)` | `InvalidOperationException`; caller still owns; pipe state unchanged. |
| `Append` on bootstrap (empty pipe) | Donated becomes `_chainHead == _writingHead`. `_totalWritten = length`. `RunningIndex = 0`. |
| `Append` after `GetMemory` + `Advance(0)` (zero buffered) | Freezes empty rented segment with `End == 0`, splices donated. The empty rented segment is harmless in the chain and recycles into the freelist normally on drain. |
| Three back-to-back `Append`s with no intervening `GetMemory` | Chain becomes `[..., previousTail-frozen, d1, d2, d3]` with `_writingHead = d3`. No empty rented tails between donations. Freezes are idempotent on `End`/`Memory` for donated previous tails. |
| `GetMemory(N)` after `Append` | Forces transition (since `remaining == 0`). Rents a fresh tail. Freezes donated as a non-tail chain segment. |
| `Advance(N>0)` after `Append` | Throws `ArgumentOutOfRangeException` via existing bounds check at `SpscPipe.Writer.cs:52`. |
| `FlushAsync` after `Append` | Publishes new `WriterState` with `TailSegment == donated`, `TailWritten == donated.End`. Backpressure check sees boosted `_totalWritten`; parks if `unconsumed >= PauseWriterThreshold`. |
| Reader drains past donated segment | `RecycleDrainedSegments` calls `donated.DisposeOwned()` (releasing the foreign `IMemoryOwner`); the `BufferSegment` shell is GC'd. Freelist count unchanged. |
| `AdvanceTo` to a `SequencePosition` inside a donated segment of the same pipe | Passes pipe-identity check (`OwnerToken == pipe`). Standard `AdvanceTo` semantics apply. |
| `AdvanceTo` to a `SequencePosition` from a *different* pipe's donated segment | Throws `InvalidOperationException` ("SequencePosition is from a different pipe.") via the existing R4-7 check. |
| `SpscPipe.Dispose` with un-flushed donated segments in chain | Existing chain walk calls `DisposeOwned()` on every segment; works uniformly for donated. |
| Pathological large Append (many GB) without intervening flush | Allowed at `Append` time. Next `FlushAsync` parks if backpressure threshold exceeded. Same as `GetMemory(huge)` + `Advance(huge)`. |

## Section 9 — Spec amendments (against `2026-04-25-spsc-pipe-tripleBuffer-design.md`)

| Section | Amendment |
|---|---|
| §3 Ownership table | Add row: foreign `IMemoryOwner<byte>` (donated) — owned by writer, released on recycle (`DisposeOwned`) or `SpscPipe.Dispose`. |
| §3 `BufferSegment` definition | Add `IsDonated` field, `AdoptFrom` initializer; clarify that `Freeze` on a donated segment is idempotent on `End`/`Memory`. |
| §3 Allocation path | Reference new sibling subsection "Donation path" (this design's §4). |
| §3 Recycling path | Update pseudocode to branch on `IsDonated`: dispose-and-drop vs `PushFreelist`. |
| §3 `MemoryPool` integration | Note: donated segments bypass `_options.Pool` and `MinimumSegmentSize`; donor-decided sizing. |
| §3 `Memory<T>` torn-read note (Nit-5) | Note: donated segments are immune; `base.Memory` is not re-sliced post-adoption. |
| §3 Lifecycle / Dispose | Note: `DisposeOwned` walk is uniform across donated and rented. |
| §4 Hot paths | New subsection: "Writer: `Append`" with the pseudocode from §4 of this document. |
| §X Public API | Note `SpscPipeWriter` visibility change and the two `Append` overloads. |

No changes to §2 (state shapes), §5 (awaiter coordination), or §6 (completion/cancellation) — `Append` does not interact with awaiters or completion mechanisms.

## Section 10 — Testing strategy

Three groups, mirroring the existing test layout under `tests/`.

### 10.1 — `BufferSegmentTests` additions

- `AdoptFrom_SetsFieldsCorrectly` — calls `AdoptFrom(owner, slice, runningIndex, pipeOwner)`; verifies `AvailableMemory == slice`, `base.Memory == slice`, `End == slice.Length`, `RunningIndex == passed`, `Next == null`, `OwnerToken == pipeOwner`, `IsDonated == true`.
- `AdoptFrom_ThenDisposeOwned_DisposesOriginalOwner` — uses a tracking `IMemoryOwner` mock; verifies `Dispose` count == 1 after `DisposeOwned`.
- `RentFrom_ResetsIsDonated` — segment recycled from a previous donate-or-rent path with `IsDonated == true`-or-`false` via `RentFrom` ends up with `IsDonated == false`.
- ~~`RecycleReset_AssertsNotDonated`~~ — deferred. The `Debug.Assert` is documentation of the invariant; testing it requires `#if DEBUG`-guarded trace-listener swapping which is brittle and offers little value over the inline assertion itself.
- `Freeze_OnDonatedSegment_IsIdempotent` — pin the documented "End/Memory writes are idempotent" property: `donated.Freeze(donated.End, next)` produces same `End`/`base.Memory` and updated `Next`.

### 10.2 — `SpscPipeWriterAppendTests` (new file)

Argument validation + ownership-on-throw:
- `Append_NullBuffer_Throws_ArgumentNull` (both overloads).
- `Append_DisposedPipe_Throws_ObjectDisposed_CallerStillOwns`.
- `Append_CompletedWriter_Throws_InvalidOp_CallerStillOwns`.
- `Append_NegativeStart_Throws_OutOfRange_CallerStillOwns`.
- `Append_NegativeLength_Throws_OutOfRange_CallerStillOwns`.
- `Append_StartPlusLengthOverflowsBuffer_Throws_OutOfRange_CallerStillOwns`.

The "caller still owns" verifications use a mock `IMemoryOwner<byte>` that increments a counter on `Dispose`; the test asserts `count == 0` after the throw.

Bootstrap and steady-state splicing:
- `Append_OnEmptyPipe_Bootstraps` — verifies `_chainHead == _writingHead == donated`, `_totalWritten == length`, `donated.RunningIndex == 0`.
- `Append_AfterPartialFill_FreezesAndSplices` — calls `GetMemory(64) + Advance(40)` then `Append(buf2)`. Verifies previous tail is frozen with `End == 40`, donated's `RunningIndex == previousTail.RunningIndex + 40`, `_totalWritten == 40 + length`.
- `Append_BackToBack_NoEmptyTailsBetween` — three `Append`s in a row; chain has exactly three donated segments after the entry point, no rented filler.

Post-Append behavior:
- `GetMemory_AfterAppend_TransitionsToFreshTail` — `_writingHead != donated` after the call; new tail's `RunningIndex == donated.RunningIndex + donated.End`.
- `Advance_AfterAppend_Throws` — `Advance(1)` throws `ArgumentOutOfRangeException`.
- `FlushAsync_AfterAppend_PublishesCorrectWriterState` — published `WriterState.TailSegment == donated`, `TailWritten == length`, `TotalWritten == previous + length`.

Zero-length:
- `Append_ZeroLength_DisposesAndReturns` — buffer is disposed, `_totalWritten` unchanged, `_chainHead`/`_writingHead` unchanged.
- `Append_BufferWithMemoryLengthZero_DisposesAndReturns` — same outcome via the no-arg overload.

### 10.3 — Reader / recycle / Dispose integration tests

- `Reader_ReadsDonatedSegmentContent` — `ReadResult.Buffer` includes the donated bytes in the right position.
- `AdvanceTo_PositionInsideDonatedSegment_Succeeds`.
- `AdvanceTo_DonatedSegmentFromOtherPipe_Throws` — pipe-identity (R4-7) check fires for cross-pipe `SequencePosition`.
- `Recycle_DonatedSegmentDisposesOwner` — after the reader drains past a donated segment and writer flushes again, the mock owner's `Dispose` count == 1.
- `Recycle_DonatedSegmentNotPushedToFreelist` — freelist count unchanged across a donated drain.
- `Recycle_RentedSegmentStillFreelisted` — existing rented-recycle behavior preserved (regression guard).
- `Dispose_WithMixedChain_DisposesAllOwners` — chain has `[rented, donated, rented, donated]`; `SpscPipe.Dispose` results in all four `DisposeOwned` calls.
- `Backpressure_AppendDoesNotPark` — `Append(huge)` returns synchronously even when `huge >= PauseWriterThreshold`; subsequent `FlushAsync` parks.

### 10.4 — Stress

One new scenario in `tests/SpscPipe.Stress/` that interleaves `Append` with `GetMemory`/`Advance` while the reader concurrently drains. Uses tracking `IMemoryOwner` mocks and verifies at end-of-test that total `Dispose` count equals total `Append` count plus zero-length-skipped count. Catches both leaks (under-dispose) and double-disposes (over-dispose).

## Section 11 — Out of scope

- A reader-side counterpart that "claims ownership" of a donated segment from the read sequence. Not currently motivated; would require `SpscPipeReader` to become public and `BufferSegment.IsDonated` to be exposed. Deferred until a concrete use case arises.
- Helper extensions like `static class SpscPipeWriterExtensions { public static void Append(this SpscPipeWriter w, byte[] array) { ... } }` that wrap byte arrays into ad-hoc `IMemoryOwner`s. Easy to add later as nice-to-haves; not part of this design.
- Diagnostic counters specifically for donated segments (e.g., `_donatedAppendCount`, `_donatedDisposeCount`). Could be added at implementation time if observability needs surface; not required by the design.
- Pooling of `BufferSegment` shell objects allocated for donated segments. The `BufferSegment` object is small and donations are expected to be relatively infrequent compared to byte volume; pooling adds complexity without clear payoff. Deferred.
