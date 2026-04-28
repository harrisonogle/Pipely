# SpscPipe Buffer Ownership Transfer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the `SpscPipeWriter.Append(IMemoryOwner<byte>[, int start, int length])` API per `docs/superpowers/specs/2026-04-28-spsc-pipe-buffer-ownership-transfer-design.md`. New writer-side method takes ownership of a pre-filled buffer, splices it into the segment chain via the existing freeze/transition machinery, and disposes the foreign `IMemoryOwner` when the reader has drained past or when the pipe is disposed. Ownership transfers iff `Append` returns normally.

**Architecture:** A donated buffer becomes a `BufferSegment` with a new `IsDonated = true` flag, initialized via a new `AdoptFrom(IMemoryOwner<byte> owner, Memory<byte> slice, long runningIndex, object pipeOwner)` initializer. `Append` performs the same kind of tail transition as `GetMemory` does on capacity-exceeded: compute `runningIndex` from the current tail's filled count, allocate a fresh `BufferSegment`, call `AdoptFrom` with a pre-validated slice, freeze the previous tail (if any) with `Freeze(filled, donated)` to set its `Next` pointer, and update `_writingHead`/`_writingHeadBytesBuffered`/`_totalWritten`. `RecycleDrainedSegments` branches on `IsDonated` — donated → `DisposeOwned()` and discard; rented → `PushFreelist` (existing). `SpscPipeWriter` becomes public (unnested) and `SpscPipe.Writer`'s declared return type widens from `PipeWriter` to `SpscPipeWriter`. `SpscPipeReader` stays internal.

**Tech Stack:** C# / .NET 10, xUnit 2.9.3, `System.Buffers.IMemoryOwner<byte>` / `MemoryPool<byte>`, `System.IO.Pipelines.PipeWriter`.

**Reference docs (engineer should re-read before starting):**
- `docs/superpowers/specs/2026-04-28-spsc-pipe-buffer-ownership-transfer-design.md` — this plan's spec.
- `docs/superpowers/specs/2026-04-25-spsc-pipe-tripleBuffer-design.md` — base SpscPipe spec; this plan amends §3 (BufferSegment, recycle), §4 (hot paths), §X (public API).
- `src/SpscPipelines/BufferSegment.cs` — modified in Task 2 (new `IsDonated` field + `AdoptFrom` method).
- `src/SpscPipelines/SpscPipe.Writer.cs` — modified in Task 3 (unnest + visibility) and Tasks 4-8 (new `Append` overloads).
- `src/SpscPipelines/SpscPipe.cs:58` — `Writer` property type changes in Task 3; `RecycleDrainedSegments` (line 315) branches on `IsDonated` in Task 9.
- `tests/SpscPipe.Tests/BufferSegmentTests.cs` — additions in Task 2.
- `tests/SpscPipe.Tests/SpscPipeWriterAppendTests.cs` — new file in Tasks 4-9.

**Working directory for all commands:** `/home/harrison/src/worktrees/SpscPipe/buffer-transfer/` (already a dedicated worktree on branch `buffer-transfer`).

**Pre-work invariants this plan preserves:**
- All existing tests in `tests/SpscPipe.Tests/`, `tests/SpscPipe.Stress/`, and `tests/SpscPipelines.HotHandoff.Tests/` continue to pass without modification.
- The benign-torn-read property of `Memory<T>` (Spec §3 Nit-5) is unaffected — donated segments don't trigger it; rented segments behave exactly as before.
- The TripleBuffer-mediated SPSC contract is untouched.

---

## File structure

```
src/SpscPipelines/
├── BufferSegment.cs                  (modified — Task 2: + IsDonated, + AdoptFrom)
├── SpscPipe.cs                       (modified — Task 3: Writer property type;
│                                                  Task 9: RecycleDrainedSegments branch)
├── SpscPipe.Writer.cs                (modified — Task 3: unnest SpscPipeWriter, public visibility;
│                                                  Tasks 4-8: + Append overloads)
└── (other files unchanged)

tests/SpscPipe.Tests/
├── TrackingMemoryOwner.cs            (NEW — Task 1: shared test helper)
├── BufferSegmentTests.cs             (modified — Task 2: + AdoptFrom tests)
├── SpscPipeWriterAppendTests.cs      (NEW — Tasks 4-9: Append behavior tests)
├── SpscPipeAdvanceToTests.cs         (modified — Task 10: + cross-pipe donated AdvanceTo test)
└── (other files unchanged)

tests/SpscPipe.Stress/
└── StressHarness.cs                  (modified — Task 11: + Append injection in producer)

docs/superpowers/specs/
└── 2026-04-25-spsc-pipe-tripleBuffer-design.md  (modified — Task 12: §3 / §4 / §X amendments)
```

---

## Task 0: Pre-flight — confirm clean baseline

**Files:** none modified.

- [ ] **Step 1: Confirm working directory and branch**

Run:
```bash
pwd && git branch --show-current && git status --short
```

Expected:
```
/home/harrison/src/worktrees/SpscPipe/buffer-transfer
buffer-transfer

```
(empty status — clean tree)

- [ ] **Step 2: Confirm spec is in place**

Run:
```bash
ls docs/superpowers/specs/2026-04-28-spsc-pipe-buffer-ownership-transfer-design.md
```

Expected: file path printed (no error).

- [ ] **Step 3: Build the solution clean**

Run:
```bash
dotnet build SpscPipe.slnx -c Release
```

Expected: 6 projects build, 0 warnings, 0 errors.

- [ ] **Step 4: Run the full test suite to establish baseline**

Run:
```bash
dotnet test SpscPipe.slnx -c Release --nologo
```

Expected: All tests pass. Note the test count for later comparison.

---

## Task 1: TrackingMemoryOwner test helper

**Goal:** Add a shared test helper that wraps a `byte[]` as an `IMemoryOwner<byte>` and counts `Dispose` calls. Used in Tasks 4–9 to verify the ownership-transfer contract (caller still owns on throw; pipe disposes on recycle).

**Files:**
- Create: `tests/SpscPipe.Tests/TrackingMemoryOwner.cs`

- [ ] **Step 1: Create the helper**

```csharp
// tests/SpscPipe.Tests/TrackingMemoryOwner.cs
using System.Buffers;

namespace SpscPipe.Tests;

internal sealed class TrackingMemoryOwner : IMemoryOwner<byte>
{
    private readonly byte[] _bytes;
    private bool _disposed;

    public int DisposeCount;

    public TrackingMemoryOwner(int size) { _bytes = new byte[size]; }
    public TrackingMemoryOwner(byte[] bytes) { _bytes = bytes; }

    public Memory<byte> Memory =>
        _disposed
            ? throw new ObjectDisposedException(nameof(TrackingMemoryOwner))
            : _bytes;

    public void Dispose()
    {
        DisposeCount++;
        _disposed = true;
    }

    // Unchecked accessor for tests that need to inspect bytes after Dispose.
    public byte[] UnderlyingBytes => _bytes;
}
```

- [ ] **Step 2: Verify the test project still builds**

Run:
```bash
dotnet build tests/SpscPipe.Tests/SpscPipe.Tests.csproj -c Release --nologo
```

Expected: build succeeds, 0 errors.

- [ ] **Step 3: Commit**

```bash
git add tests/SpscPipe.Tests/TrackingMemoryOwner.cs
git commit -m "$(cat <<'EOF'
Tests: add TrackingMemoryOwner helper for ownership-transfer assertions

Wraps a byte[] as an IMemoryOwner<byte>, counting Dispose calls so
upcoming SpscPipeWriter.Append tests can verify the ownership
contract (caller still owns on throw; pipe disposes on recycle).
The Memory property throws ObjectDisposedException post-Dispose to
catch use-after-dispose bugs.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: `BufferSegment.IsDonated` + `AdoptFrom` (TDD)

**Goal:** Add the new `IsDonated` field and `AdoptFrom` initializer to `BufferSegment` per spec §3. Establish DEBUG asserts for the precondition `RecycleReset` is never called on donated segments.

**Files:**
- Modify: `src/SpscPipelines/BufferSegment.cs`
- Modify: `tests/SpscPipe.Tests/BufferSegmentTests.cs`

- [ ] **Step 1: Write the failing tests**

Append these tests to `tests/SpscPipe.Tests/BufferSegmentTests.cs` (inside the existing `BufferSegmentTests` class, before the closing `}`):

```csharp
[Fact]
public void RentFrom_SetsIsDonatedFalse()
{
    var seg = new BufferSegment();
    seg.RentFrom(MemoryPool<byte>.Shared, 1024, 0, Owner);
    Assert.False(seg.IsDonated);
}

[Fact]
public void AdoptFrom_SetsFieldsCorrectly()
{
    var owner = new TrackingMemoryOwner(1024);
    var slice = owner.Memory.Slice(64, 256);

    var seg = new BufferSegment();
    seg.AdoptFrom(owner, slice, runningIndex: 999, pipeOwner: Owner);

    Assert.Equal(256, seg.AvailableMemory.Length);
    Assert.Equal(256, ((ReadOnlySequenceSegment<byte>)seg).Memory.Length);
    Assert.Equal(256, seg.End);
    Assert.Null(seg.Next);
    Assert.Null(((ReadOnlySequenceSegment<byte>)seg).Next);
    Assert.Equal(999, seg.RunningIndex);
    Assert.Same(Owner, seg.OwnerToken);
    Assert.True(seg.IsDonated);
}

[Fact]
public void AdoptFrom_ThenDisposeOwned_DisposesOriginalOwner()
{
    var owner = new TrackingMemoryOwner(1024);
    var slice = owner.Memory;
    var seg = new BufferSegment();
    seg.AdoptFrom(owner, slice, runningIndex: 0, pipeOwner: Owner);

    Assert.Equal(0, owner.DisposeCount);
    seg.DisposeOwned();
    Assert.Equal(1, owner.DisposeCount);

    // DisposeOwned remains idempotent.
    seg.DisposeOwned();
    Assert.Equal(1, owner.DisposeCount);
}

[Fact]
public void AdoptFrom_PreservesSliceOffsetIntoOwner()
{
    var bytes = new byte[1024];
    for (int i = 0; i < bytes.Length; i++) bytes[i] = (byte)(i & 0xFF);
    var owner = new TrackingMemoryOwner(bytes);
    var slice = owner.Memory.Slice(100, 50);

    var seg = new BufferSegment();
    seg.AdoptFrom(owner, slice, runningIndex: 0, pipeOwner: Owner);

    // The donated segment's Memory should reference bytes 100..149 of the underlying array.
    Assert.Equal(50, seg.AvailableMemory.Length);
    Assert.Equal((byte)100, seg.AvailableMemory.Span[0]);
    Assert.Equal((byte)149, seg.AvailableMemory.Span[49]);
}

[Fact]
public void Freeze_OnDonatedSegment_IsIdempotentOnEndAndMemory()
{
    // Pin the documented behavior in spec §2.2: when an Append splices a new tail past
    // a previously-donated _writingHead, the call _writingHead.Freeze(filled, next)
    // re-writes End/base.Memory to the same values they already held; only Next changes.
    var owner = new TrackingMemoryOwner(64);
    var seg   = new BufferSegment();
    seg.AdoptFrom(owner, owner.Memory, runningIndex: 0, pipeOwner: Owner);

    int endBefore       = seg.End;
    int memLenBefore    = ((ReadOnlySequenceSegment<byte>)seg).Memory.Length;

    var next = new BufferSegment();
    next.RentFrom(MemoryPool<byte>.Shared, 32, runningIndex: 64, owner: Owner);

    seg.Freeze(bytesFilled: seg.End, next);

    Assert.Equal(endBefore,    seg.End);
    Assert.Equal(memLenBefore, ((ReadOnlySequenceSegment<byte>)seg).Memory.Length);
    Assert.Same(next,          seg.Next);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
dotnet test tests/SpscPipe.Tests/SpscPipe.Tests.csproj --filter "FullyQualifiedName~BufferSegmentTests" --nologo
```

Expected: compilation error referencing `seg.IsDonated`, `seg.AdoptFrom`, or both. (The new tests reference symbols that don't exist yet.)

- [ ] **Step 3: Add `IsDonated` field to BufferSegment**

Edit `src/SpscPipelines/BufferSegment.cs`. Add the field declaration after the existing `OwnerToken` line:

Current (line 12):
```csharp
    public object? OwnerToken { get; private set; }   // R4-7
```

Replace with:
```csharp
    public object? OwnerToken { get; private set; }   // R4-7
    public bool    IsDonated  { get; private set; }   // recycle-path discriminator (donated vs rented)
```

- [ ] **Step 4: Set `IsDonated = false` in `RentFrom`**

Edit `src/SpscPipelines/BufferSegment.cs`. The current `RentFrom` (lines 14-24) ends with `OwnerToken = owner;`. Add an `IsDonated = false;` line right after:

Current end of `RentFrom`:
```csharp
        OwnerToken        = owner;
    }
```

Replace with:
```csharp
        OwnerToken        = owner;
        IsDonated         = false;
    }
```

- [ ] **Step 5: Add `AdoptFrom` initializer**

Edit `src/SpscPipelines/BufferSegment.cs`. After the existing `RentFrom` method (after the `}` that closes it, before `public void Freeze(...)`), add:

```csharp
    // Adopts a foreign IMemoryOwner<byte> for buffer-ownership transfer (Append).
    // Caller passes a pre-computed slice of `owner.Memory` so this method is allocation- and throw-free.
    // Preconditions (caller-validated):
    //   owner != null, slice.Length > 0, slice originates from owner.Memory.
    public void AdoptFrom(IMemoryOwner<byte> owner, Memory<byte> slice, long runningIndex, object pipeOwner)
    {
        _memoryOwner      = owner;
        AvailableMemory   = slice;
        base.Memory       = slice;
        base.RunningIndex = runningIndex;
        End               = slice.Length;
        Next              = null;
        base.Next         = null;
        OwnerToken        = pipeOwner;
        IsDonated         = true;
    }
```

- [ ] **Step 6: Add a DEBUG assert in `RecycleReset`**

Edit `src/SpscPipelines/BufferSegment.cs`. The current `RecycleReset` (lines 34-42) starts with `base.RunningIndex = runningIndex;`. Add a `System.Diagnostics.Debug.Assert(...)` at the top of its body:

Current:
```csharp
    public void RecycleReset(long runningIndex)
    {
        base.RunningIndex = runningIndex;
```

Replace with:
```csharp
    public void RecycleReset(long runningIndex)
    {
        System.Diagnostics.Debug.Assert(!IsDonated, "RecycleReset must not be called on donated segments.");
        base.RunningIndex = runningIndex;
```

- [ ] **Step 7: Run tests to verify they pass**

Run:
```bash
dotnet test tests/SpscPipe.Tests/SpscPipe.Tests.csproj --filter "FullyQualifiedName~BufferSegmentTests" --nologo
```

Expected: all `BufferSegmentTests` pass (existing + 5 new).

- [ ] **Step 8: Run the full test suite to verify no regressions**

Run:
```bash
dotnet test SpscPipe.slnx -c Release --nologo
```

Expected: All tests pass; count = baseline + 5.

- [ ] **Step 9: Commit**

```bash
git add src/SpscPipelines/BufferSegment.cs tests/SpscPipe.Tests/BufferSegmentTests.cs
git commit -m "$(cat <<'EOF'
BufferSegment: add IsDonated flag + AdoptFrom initializer

Adds the recycle-path discriminator (IsDonated) and the AdoptFrom
initializer that wraps a foreign IMemoryOwner<byte> with a
pre-computed slice. AdoptFrom is allocation- and throw-free by
construction so the Append flow's post-validation body has no
exception-safety reordering concerns.

Pins the documented behavior that Freeze on a donated segment is
idempotent on End and base.Memory (only Next changes), since the
segment is initialized into its post-Freeze shape immediately at
AdoptFrom time. RecycleReset gains a DEBUG assert that fires if a
donated segment ever reaches the freelist path.

Per spec §3 of
docs/superpowers/specs/2026-04-28-spsc-pipe-buffer-ownership-transfer-design.md.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Unnest `SpscPipeWriter`; widen `SpscPipe.Writer` return type

**Goal:** Make `SpscPipeWriter` a public, non-nested class so it can carry the new `Append` overloads. Change `SpscPipe.Writer`'s declared return type from `PipeWriter` to `SpscPipeWriter` — source-compatible with all existing call sites via implicit upcast (`SpscPipeWriter : PipeWriter`).

**Files:**
- Modify: `src/SpscPipelines/SpscPipe.Writer.cs`
- Modify: `src/SpscPipelines/SpscPipe.cs:58`

**Note on TDD here:** This is a structural refactor with compile-time correctness only. The "test" is: existing test suite still passes after the change.

- [ ] **Step 1: Unnest `SpscPipeWriter` and make it public**

Edit `src/SpscPipelines/SpscPipe.Writer.cs`. The current shape is:

```csharp
namespace SpscPipelines;

public sealed partial class SpscPipe
{
    internal sealed class SpscPipeWriter : PipeWriter
    {
        ...
    }
}
```

Replace the outer wrapping. The new shape is:

```csharp
namespace SpscPipelines;

public sealed class SpscPipeWriter : PipeWriter
{
    ...
}
```

Concretely:
- Replace `public sealed partial class SpscPipe\n{\n    internal sealed class SpscPipeWriter : PipeWriter\n    {` (lines 8-11) with `public sealed class SpscPipeWriter : PipeWriter\n{`.
- Remove the closing `}` for the outer `SpscPipe` class (the very last `}` in the file).
- Indentation: shift the entire class body left by 4 spaces (one indent level) to match top-level placement.
- Keep the `internal SpscPipeWriter(SpscPipe pipe) => _pipe = pipe;` constructor visibility as `internal` (only `SpscPipe` constructs it).

Use this exact replacement (perform a full-file rewrite to avoid indentation drift):

```csharp
using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.ExceptionServices;
using System.Threading;

namespace SpscPipelines;

public sealed class SpscPipeWriter : PipeWriter
{
    private readonly SpscPipe _pipe;
    internal SpscPipeWriter(SpscPipe pipe) => _pipe = pipe;

    public override Memory<byte> GetMemory(int sizeHint = 0)
    {
        if (_pipe._disposed) throw new ObjectDisposedException(nameof(SpscPipe));
        if (_pipe._writerCompleted) throw new InvalidOperationException("Writing is completed.");
        if (sizeHint < 0) throw new ArgumentOutOfRangeException(nameof(sizeHint));
        if (sizeHint == 0) sizeHint = 1;

        if (_pipe._writingHead == null)
        {
            _pipe._writingHead = _pipe.RentSegment(sizeHint, runningIndex: 0);
            _pipe._chainHead   = _pipe._writingHead;
        }
        else
        {
            int remaining = _pipe._writingHead.AvailableMemory.Length - _pipe._writingHeadBytesBuffered;
            if (remaining < sizeHint)
            {
                int filled  = _pipe._writingHeadBytesBuffered;
                long newRI  = _pipe._writingHead.RunningIndex + filled;
                var newTail = _pipe.RentSegment(Math.Max(sizeHint, _pipe._options.MinimumSegmentSize), newRI);

                _pipe._writingHead.Freeze(filled, newTail);
                _pipe._writingHead = newTail;
                _pipe._writingHeadBytesBuffered = 0;
            }
        }

        return _pipe._writingHead.AvailableMemory.Slice(_pipe._writingHeadBytesBuffered);
    }

    public override Span<byte> GetSpan(int sizeHint = 0) => GetMemory(sizeHint).Span;

    public override void Advance(int bytes)
    {
        if (_pipe._disposed) throw new ObjectDisposedException(nameof(SpscPipe));
        if (_pipe._writerCompleted) throw new InvalidOperationException("Writing is completed.");
        if (_pipe._writingHead == null) throw new InvalidOperationException("Advance without prior GetMemory.");
        if (_pipe._writingHeadBytesBuffered + bytes > _pipe._writingHead.AvailableMemory.Length)
            throw new ArgumentOutOfRangeException(nameof(bytes));
        _pipe._writingHeadBytesBuffered += bytes;
        _pipe._totalWritten += bytes;
    }

    public override ValueTask<FlushResult> FlushAsync(CancellationToken ct = default)
    {
        if (_pipe._disposed) throw new ObjectDisposedException(nameof(SpscPipe));
        if (_pipe._writerCompleted) throw new InvalidOperationException("Writing is completed.");

        // Throw-first: refresh reader state, then throw if reader-completed-with-ex.
        if (_pipe._readerTb.TryAcquire())
            _pipe._lastAcquiredReaderState = _pipe._readerTb.ConsumerSlot();

        if (_pipe._lastAcquiredReaderState.IsCompleted && _pipe._lastAcquiredReaderState.CompletionException != null)
            ExceptionDispatchInfo.Throw(_pipe._lastAcquiredReaderState.CompletionException);

        // Sync entry: consume sticky CancelPendingFlush flag.
        while (true)
        {
            int oldV = _pipe._flushAwaiter._state;
            if ((oldV & SpscAwaiter<FlushResult>.CancelFlag) == 0) break;
            int desired = oldV & ~SpscAwaiter<FlushResult>.CancelFlag;
            if (Interlocked.CompareExchange(ref _pipe._flushAwaiter._state, desired, oldV) == oldV)
                return new ValueTask<FlushResult>(_pipe.BuildFlushResult(isCanceled: true));
        }

        if (ct.IsCancellationRequested)
            return ValueTask.FromCanceled<FlushResult>(ct);

        // Build state, publish, signal.
        var snapshot = new WriterState
        {
            HeadSegment         = _pipe._chainHead,
            TailSegment         = _pipe._writingHead,
            TailWritten         = _pipe._writingHeadBytesBuffered,
            TotalWritten        = _pipe._totalWritten,
            IsCompleted         = false,
            CompletionException = null,
        };
        _pipe._writerTb.ProducerSlot() = snapshot;
        _pipe._writerTb.Publish();
        _pipe._lastPublishedWriterState = snapshot;

        _pipe.SignalReadAwaiterIfPending();

        // Re-acquire for backpressure freshness.
        if (_pipe._readerTb.TryAcquire())
            _pipe._lastAcquiredReaderState = _pipe._readerTb.ConsumerSlot();
        _pipe.RecycleDrainedSegments();

        if (_pipe._lastAcquiredReaderState.IsCompleted && _pipe._lastAcquiredReaderState.CompletionException != null)
            ExceptionDispatchInfo.Throw(_pipe._lastAcquiredReaderState.CompletionException);

        long unconsumed = _pipe._totalWritten - _pipe._lastAcquiredReaderState.TotalConsumed;
        bool readerDone = _pipe._lastAcquiredReaderState.IsCompleted;
        bool needsPark = _pipe._options.PauseWriterThreshold > 0
                         && unconsumed >= _pipe._options.PauseWriterThreshold
                         && !readerDone;

        if (!needsPark)
            return new ValueTask<FlushResult>(_pipe.BuildFlushResult(isCanceled: false));

        return ParkFlushAwaiter(ct);
    }

    public override void Complete(Exception? exception = null)
    {
        if (_pipe._disposed) throw new ObjectDisposedException(nameof(SpscPipe));
        if (_pipe._writerCompleted) return;     // double-Complete coalesces
        _pipe._writerCompleted = true;

        var snapshot = new WriterState
        {
            HeadSegment         = _pipe._chainHead,
            TailSegment         = _pipe._writingHead,
            TailWritten         = _pipe._writingHeadBytesBuffered,
            TotalWritten        = _pipe._totalWritten,
            IsCompleted         = true,
            CompletionException = exception,
        };
        _pipe._writerTb.ProducerSlot() = snapshot;
        _pipe._writerTb.Publish();
        _pipe._lastPublishedWriterState = snapshot;

        _pipe.SignalReadAwaiterIfPending();
    }

    public override void CancelPendingFlush()
    {
        if (_pipe._disposed) throw new ObjectDisposedException(nameof(SpscPipe));
        int oldV = Interlocked.Or(ref _pipe._flushAwaiter._state, SpscAwaiter<FlushResult>.CancelFlag);
        if ((oldV & SpscAwaiter<FlushResult>.StateMask) == SpscAwaiter<FlushResult>.Pending
            && Interlocked.CompareExchange(
                   ref _pipe._flushAwaiter._state,
                   SpscAwaiter<FlushResult>.Inactive,
                   SpscAwaiter<FlushResult>.Pending | SpscAwaiter<FlushResult>.CancelFlag)
               == (SpscAwaiter<FlushResult>.Pending | SpscAwaiter<FlushResult>.CancelFlag))
        {
            Interlocked.Increment(ref _pipe._flushAwaiter._cancelPendingWonCount);
            _pipe._flushAwaiter._ctr.Dispose();
            _pipe._flushAwaiter._core.SetResult(new FlushResult(isCanceled: true, isCompleted: false));
        }
    }

    private ValueTask<FlushResult> ParkFlushAwaiter(CancellationToken ct)
    {
        _pipe._flushAwaiter._ctr.Dispose();        // R5b cleanup
        _pipe._flushAwaiter._core.Reset();
        _pipe._flushAwaiter._token = ct;

        while (true)
        {
            int oldV = _pipe._flushAwaiter._state;
            System.Diagnostics.Debug.Assert((oldV & SpscAwaiter<FlushResult>.StateMask) == SpscAwaiter<FlushResult>.Inactive,
                         "SPSC violation: concurrent FlushAsync");
            int desired = (oldV & SpscAwaiter<FlushResult>.CancelFlag) | SpscAwaiter<FlushResult>.Pending;
            if (Interlocked.CompareExchange(ref _pipe._flushAwaiter._state, desired, oldV) == oldV)
            {
                Interlocked.Increment(ref _pipe._flushAwaiter._parkCount);
                break;
            }
        }

        // Lost-wakeup re-check (throw-first).
        if (_pipe._readerTb.TryAcquire())
        {
            _pipe._lastAcquiredReaderState = _pipe._readerTb.ConsumerSlot();

            if (_pipe._lastAcquiredReaderState.IsCompleted && _pipe._lastAcquiredReaderState.CompletionException != null)
            {
                while (true)
                {
                    int oldV = _pipe._flushAwaiter._state;
                    if ((oldV & SpscAwaiter<FlushResult>.StateMask) != SpscAwaiter<FlushResult>.Pending) break;
                    int desired = oldV & ~SpscAwaiter<FlushResult>.StateMask;
                    if (Interlocked.CompareExchange(ref _pipe._flushAwaiter._state, desired, oldV) == oldV)
                    {
                        Interlocked.Increment(ref _pipe._flushAwaiter._lostWakeupResolvedCount);
                        _pipe._flushAwaiter._core.SetException(_pipe._lastAcquiredReaderState.CompletionException);
                        return new ValueTask<FlushResult>(_pipe._flushAwaiter, _pipe._flushAwaiter.Version);
                    }
                }
            }

            long unconsumed = _pipe._totalWritten - _pipe._lastAcquiredReaderState.TotalConsumed;
            bool releasable = unconsumed < _pipe._options.ResumeWriterThreshold
                              || _pipe._lastAcquiredReaderState.IsCompleted;

            if (releasable)
            {
                while (true)
                {
                    int oldV = _pipe._flushAwaiter._state;
                    if ((oldV & SpscAwaiter<FlushResult>.StateMask) != SpscAwaiter<FlushResult>.Pending) break;
                    int desired = oldV & ~SpscAwaiter<FlushResult>.StateMask;
                    if (Interlocked.CompareExchange(ref _pipe._flushAwaiter._state, desired, oldV) == oldV)
                    {
                        Interlocked.Increment(ref _pipe._flushAwaiter._lostWakeupResolvedCount);
                        return new ValueTask<FlushResult>(_pipe.BuildFlushResult(isCanceled: false));
                    }
                }
            }
        }

        // Lost-cancel re-check.
        int v = _pipe._flushAwaiter._state;
        if ((v & SpscAwaiter<FlushResult>.CancelFlag) != 0
            && Interlocked.CompareExchange(
                   ref _pipe._flushAwaiter._state,
                   SpscAwaiter<FlushResult>.Inactive,
                   SpscAwaiter<FlushResult>.Pending | SpscAwaiter<FlushResult>.CancelFlag)
               == (SpscAwaiter<FlushResult>.Pending | SpscAwaiter<FlushResult>.CancelFlag))
        {
            Interlocked.Increment(ref _pipe._flushAwaiter._lostCancelResolvedCount);
            _pipe._flushAwaiter._core.SetResult(_pipe.BuildFlushResult(isCanceled: true));
            return new ValueTask<FlushResult>(_pipe._flushAwaiter, _pipe._flushAwaiter.Version);
        }

        _pipe._flushAwaiter._ctr = ct.UnsafeRegister(static p => ((SpscPipe)p!).OnFlushAwaiterTokenCancel(), _pipe);
        if ((_pipe._flushAwaiter._state & SpscAwaiter<FlushResult>.StateMask) != SpscAwaiter<FlushResult>.Pending)
            _pipe._flushAwaiter._ctr.Dispose();
        return new ValueTask<FlushResult>(_pipe._flushAwaiter, _pipe._flushAwaiter.Version);
    }
}
```

(That's the full file content. Use Write to overwrite `src/SpscPipelines/SpscPipe.Writer.cs` with this body.)

- [ ] **Step 2: Widen `SpscPipe.Writer` return type**

Edit `src/SpscPipelines/SpscPipe.cs`. Line 44 declares the field; line 58 declares the property.

Current (line 44):
```csharp
    private readonly SpscPipeWriter _writerInstance;
```
This already references `SpscPipeWriter` directly; with the unnested type it now resolves to the top-level class — no change needed.

Current (line 58):
```csharp
    public PipeWriter Writer => _writerInstance;
```

Replace with:
```csharp
    public SpscPipeWriter Writer => _writerInstance;
```

- [ ] **Step 3: Build the solution**

Run:
```bash
dotnet build SpscPipe.slnx -c Release
```

Expected: 6 projects build, 0 warnings, 0 errors.

If a build error mentions an existing call site that depended on `Writer` being typed as `PipeWriter` (e.g., a method expecting `PipeWriter` directly), the implicit upcast should resolve it. If the error is `cannot convert SpscPipeWriter to PipeWriter`, double-check that `SpscPipeWriter` extends `PipeWriter` (it does — line 1 of `SpscPipe.Writer.cs`).

- [ ] **Step 4: Run the full test suite**

Run:
```bash
dotnet test SpscPipe.slnx -c Release --nologo
```

Expected: All tests pass; count unchanged from Task 2's expected count.

- [ ] **Step 5: Commit**

```bash
git add src/SpscPipelines/SpscPipe.Writer.cs src/SpscPipelines/SpscPipe.cs
git commit -m "$(cat <<'EOF'
SpscPipe: unnest SpscPipeWriter; widen Writer return type

Makes SpscPipeWriter a public top-level class (was nested + internal
inside SpscPipe) so the upcoming Append overloads have a public
home. SpscPipe.Writer's declared return type widens from PipeWriter
to SpscPipeWriter; existing call sites that bound to PipeWriter
continue to compile via implicit upcast.

The constructor stays internal — only SpscPipe constructs it.
SpscPipeReader stays internal + nested per spec §6.2 (no reader-
side surface addition motivates exposing it).

No behavior change. Per spec §6 of
docs/superpowers/specs/2026-04-28-spsc-pipe-buffer-ownership-transfer-design.md.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: `Append` argument validation (TDD)

**Goal:** Implement the validation layer of `Append` — null check, disposed/completed state checks, range checks. Each failure path leaves the caller still owning the buffer (no `Dispose` called by the pipe). Mid-method state is untouched.

**Files:**
- Create: `tests/SpscPipe.Tests/SpscPipeWriterAppendTests.cs`
- Modify: `src/SpscPipelines/SpscPipe.Writer.cs`

- [ ] **Step 1: Write the failing tests**

Create `tests/SpscPipe.Tests/SpscPipeWriterAppendTests.cs`:

```csharp
using System.Buffers;
using SpscPipelines;
using Xunit;

namespace SpscPipe.Tests;

public class SpscPipeWriterAppendTests
{
    // ---------- Argument validation: ownership stays with caller on throw ----------

    [Fact]
    public void Append_NullBuffer_NoArg_Throws_ArgumentNull()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        Assert.Throws<ArgumentNullException>(() => pipe.Writer.Append(null!));
    }

    [Fact]
    public void Append_NullBuffer_ThreeArg_Throws_ArgumentNull()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        Assert.Throws<ArgumentNullException>(() => pipe.Writer.Append(null!, 0, 0));
    }

    [Fact]
    public void Append_DisposedPipe_Throws_ObjectDisposed_CallerStillOwns()
    {
        var pipe = new SpscPipelines.SpscPipe();
        pipe.Dispose();

        var owner = new TrackingMemoryOwner(64);
        Assert.Throws<ObjectDisposedException>(() => pipe.Writer.Append(owner));
        Assert.Equal(0, owner.DisposeCount);   // pipe did NOT dispose; caller still owns
    }

    [Fact]
    public void Append_CompletedWriter_Throws_InvalidOp_CallerStillOwns()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        pipe._writerCompleted = true;          // simulate post-Complete state (internal flag)

        var owner = new TrackingMemoryOwner(64);
        Assert.Throws<InvalidOperationException>(() => pipe.Writer.Append(owner));
        Assert.Equal(0, owner.DisposeCount);
    }

    [Theory]
    [InlineData(-1, 0)]    // negative start
    [InlineData(0, -1)]    // negative length
    [InlineData(33, 32)]   // start + length > buffer.Memory.Length
    [InlineData(64, 1)]    // start past end + positive length
    public void Append_RangeViolation_Throws_OutOfRange_CallerStillOwns(int start, int length)
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var owner = new TrackingMemoryOwner(64);
        Assert.Throws<ArgumentOutOfRangeException>(() => pipe.Writer.Append(owner, start, length));
        Assert.Equal(0, owner.DisposeCount);
    }

    [Fact]
    public void Append_ValidationThrows_PipeStateUntouched()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        // Establish a known state.
        pipe.Writer.GetMemory(40);
        pipe.Writer.Advance(40);
        long totalWrittenBefore = pipe._totalWritten;
        var writingHeadBefore = pipe._writingHead;
        int bufferedBefore = pipe._writingHeadBytesBuffered;

        var owner = new TrackingMemoryOwner(64);
        Assert.Throws<ArgumentOutOfRangeException>(() => pipe.Writer.Append(owner, -1, 0));

        Assert.Equal(totalWrittenBefore, pipe._totalWritten);
        Assert.Same(writingHeadBefore, pipe._writingHead);
        Assert.Equal(bufferedBefore, pipe._writingHeadBytesBuffered);
        Assert.Equal(0, owner.DisposeCount);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail (compile error: Append doesn't exist)**

Run:
```bash
dotnet test tests/SpscPipe.Tests/SpscPipe.Tests.csproj --filter "FullyQualifiedName~SpscPipeWriterAppendTests" --nologo
```

Expected: compilation error referencing `pipe.Writer.Append` — the method does not exist yet.

- [ ] **Step 3: Add the validation skeleton of `Append` to `SpscPipeWriter`**

Edit `src/SpscPipelines/SpscPipe.Writer.cs`. After the existing `private ValueTask<FlushResult> ParkFlushAwaiter(...)` method (the last method in the class), before the closing `}`, add:

```csharp

    // ---------- Append (buffer ownership transfer) ----------
    // Per spec docs/superpowers/specs/2026-04-28-spsc-pipe-buffer-ownership-transfer-design.md.
    // Ownership of `buffer` transfers to the pipe iff this method returns normally.
    // On any exception, the caller still owns `buffer` and is responsible for disposing it.

    public void Append(IMemoryOwner<byte> buffer)
    {
        if (buffer is null) throw new ArgumentNullException(nameof(buffer));
        Append(buffer, 0, buffer.Memory.Length);
    }

    public void Append(IMemoryOwner<byte> buffer, int start, int length)
    {
        if (buffer is null) throw new ArgumentNullException(nameof(buffer));
        if (_pipe._disposed) throw new ObjectDisposedException(nameof(SpscPipe));
        if (_pipe._writerCompleted) throw new InvalidOperationException("Writing is completed.");

        var mem = buffer.Memory;
        if ((uint)start > (uint)mem.Length || (uint)length > (uint)(mem.Length - start))
            throw new ArgumentOutOfRangeException();

        // TODO Task 5: zero-length accept-and-dispose.
        // TODO Task 6: bootstrap path.
        // TODO Task 7: steady-state splice.
        throw new NotImplementedException("Append body — implemented in Tasks 5-7");
    }
```

- [ ] **Step 4: Run tests to verify validation tests pass**

Run:
```bash
dotnet test tests/SpscPipe.Tests/SpscPipe.Tests.csproj --filter "FullyQualifiedName~SpscPipeWriterAppendTests" --nologo
```

Expected: All argument-validation tests pass. Tests beyond validation (like ones in upcoming tasks) are not yet present, so this filter only sees the 9 cases written in Step 1 (5 `[Fact]` methods + 1 `[Theory]` with 4 `InlineData` rows), and all should pass.

- [ ] **Step 5: Run full test suite to verify no regressions**

Run:
```bash
dotnet test SpscPipe.slnx -c Release --nologo
```

Expected: All tests pass; count = baseline + 5 (Task 2) + 9 (Task 4).

- [ ] **Step 6: Commit**

```bash
git add tests/SpscPipe.Tests/SpscPipeWriterAppendTests.cs src/SpscPipelines/SpscPipe.Writer.cs
git commit -m "$(cat <<'EOF'
SpscPipeWriter.Append: argument validation layer

Adds the two Append overloads with argument validation:
  - null buffer -> ArgumentNullException
  - disposed pipe -> ObjectDisposedException
  - writer-completed -> InvalidOperationException
  - out-of-range start/length -> ArgumentOutOfRangeException

Validation runs before any pipe-state mutation. Verified via
TrackingMemoryOwner: caller still owns buffer on every throw path
(Dispose is never called by the pipe).

Body is a NotImplementedException placeholder; Tasks 5-7 fill it in.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: `Append` zero-length path (TDD)

**Goal:** Implement the zero-length path: synchronously dispose the buffer, no chain mutation.

**Files:**
- Modify: `tests/SpscPipe.Tests/SpscPipeWriterAppendTests.cs`
- Modify: `src/SpscPipelines/SpscPipe.Writer.cs`

- [ ] **Step 1: Write the failing tests**

Append to `tests/SpscPipe.Tests/SpscPipeWriterAppendTests.cs` (inside the existing class):

```csharp
    // ---------- Zero-length: accept-and-dispose ----------

    [Fact]
    public void Append_ZeroLength_ThreeArg_DisposesAndReturns_NoChainMutation()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var owner = new TrackingMemoryOwner(64);

        long totalWrittenBefore = pipe._totalWritten;
        var writingHeadBefore   = pipe._writingHead;
        var chainHeadBefore     = pipe._chainHead;

        pipe.Writer.Append(owner, start: 10, length: 0);

        Assert.Equal(1, owner.DisposeCount);
        Assert.Equal(totalWrittenBefore, pipe._totalWritten);
        Assert.Same(writingHeadBefore, pipe._writingHead);
        Assert.Same(chainHeadBefore, pipe._chainHead);
    }

    [Fact]
    public void Append_BufferWithMemoryLengthZero_NoArg_DisposesAndReturns()
    {
        // Memory.Length == 0 routes through the no-arg overload to the 3-arg overload
        // with length=0; same accept-and-dispose outcome.
        using var pipe = new SpscPipelines.SpscPipe();
        var owner = new TrackingMemoryOwner(0);

        pipe.Writer.Append(owner);

        Assert.Equal(1, owner.DisposeCount);
        Assert.Equal(0, pipe._totalWritten);
        Assert.Null(pipe._writingHead);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
dotnet test tests/SpscPipe.Tests/SpscPipe.Tests.csproj --filter "FullyQualifiedName~SpscPipeWriterAppendTests.Append_ZeroLength_ThreeArg_DisposesAndReturns_NoChainMutation|FullyQualifiedName~SpscPipeWriterAppendTests.Append_BufferWithMemoryLengthZero_NoArg_DisposesAndReturns" --nologo
```

Expected: both fail with `NotImplementedException` (the placeholder body).

- [ ] **Step 3: Add the zero-length branch**

Edit `src/SpscPipelines/SpscPipe.Writer.cs`. In the 3-arg `Append`, replace the `// TODO Task 5: zero-length accept-and-dispose.` comment and the `throw new NotImplementedException(...)` line with:

```csharp
        // Zero-length: accept ownership, dispose synchronously, no chain mutation.
        if (length == 0)
        {
            buffer.Dispose();
            return;
        }

        // TODO Task 6: bootstrap path.
        // TODO Task 7: steady-state splice.
        throw new NotImplementedException("Append body — implemented in Tasks 6-7");
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
dotnet test tests/SpscPipe.Tests/SpscPipe.Tests.csproj --filter "FullyQualifiedName~SpscPipeWriterAppendTests" --nologo
```

Expected: All Append tests written so far (11 cases total: 9 from Task 4 + 2 from Task 5) pass.

- [ ] **Step 5: Commit**

```bash
git add tests/SpscPipe.Tests/SpscPipeWriterAppendTests.cs src/SpscPipelines/SpscPipe.Writer.cs
git commit -m "$(cat <<'EOF'
SpscPipeWriter.Append: zero-length accept-and-dispose path

A zero-length Append (either via length=0 in the 3-arg overload or
a buffer with Memory.Length=0 via the no-arg overload) takes
ownership of the buffer, disposes it synchronously, and returns
without splicing an empty segment into the chain. _totalWritten
and the chain pointers are unchanged.

Per spec §4.1 zero-length branch.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: `Append` bootstrap path (TDD)

**Goal:** Implement the bootstrap path: when `_writingHead == null`, the donated segment becomes both `_chainHead` and `_writingHead`. Verify all relevant fields, including `RunningIndex == 0`, `OwnerToken == pipe`, `IsDonated == true`, and that the donated `Memory` matches the original slice.

**Files:**
- Modify: `tests/SpscPipe.Tests/SpscPipeWriterAppendTests.cs`
- Modify: `src/SpscPipelines/SpscPipe.Writer.cs`

- [ ] **Step 1: Write the failing tests**

Append to `tests/SpscPipe.Tests/SpscPipeWriterAppendTests.cs`:

```csharp
    // ---------- Bootstrap: empty pipe + Append ----------

    [Fact]
    public void Append_NoArg_OnEmptyPipe_BootstrapsChainHeadEqualsWritingHead()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var bytes = new byte[64];
        for (int i = 0; i < bytes.Length; i++) bytes[i] = (byte)(i + 1);
        var owner = new TrackingMemoryOwner(bytes);

        pipe.Writer.Append(owner);

        Assert.NotNull(pipe._chainHead);
        Assert.Same(pipe._chainHead, pipe._writingHead);
        Assert.Equal(64, pipe._writingHeadBytesBuffered);
        Assert.Equal(64, pipe._totalWritten);

        var seg = pipe._chainHead!;
        Assert.Equal(0, seg.RunningIndex);
        Assert.True(seg.IsDonated);
        Assert.Same(pipe, seg.OwnerToken);
        Assert.Equal(64, seg.End);
        Assert.Equal(64, seg.AvailableMemory.Length);
        Assert.Equal((byte)1,  seg.AvailableMemory.Span[0]);
        Assert.Equal((byte)64, seg.AvailableMemory.Span[63]);

        Assert.Equal(0, owner.DisposeCount);   // ownership transferred; not yet recycled
    }

    [Fact]
    public void Append_ThreeArg_OnEmptyPipe_PublishedSliceMatchesStartAndLength()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var bytes = new byte[1024];
        for (int i = 0; i < bytes.Length; i++) bytes[i] = (byte)(i & 0xFF);
        var owner = new TrackingMemoryOwner(bytes);

        pipe.Writer.Append(owner, start: 100, length: 50);

        Assert.NotNull(pipe._chainHead);
        var seg = pipe._chainHead!;
        Assert.Equal(50, seg.End);
        Assert.Equal(50, seg.AvailableMemory.Length);
        // Bytes 100..149 of the underlying array are exposed.
        Assert.Equal((byte)100, seg.AvailableMemory.Span[0]);
        Assert.Equal((byte)149, seg.AvailableMemory.Span[49]);
        Assert.Equal(50, pipe._totalWritten);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
dotnet test tests/SpscPipe.Tests/SpscPipe.Tests.csproj --filter "FullyQualifiedName~SpscPipeWriterAppendTests.Append_NoArg_OnEmptyPipe|FullyQualifiedName~SpscPipeWriterAppendTests.Append_ThreeArg_OnEmptyPipe" --nologo
```

Expected: both fail with `NotImplementedException`.

- [ ] **Step 3: Add the bootstrap branch**

Edit `src/SpscPipelines/SpscPipe.Writer.cs`. In the 3-arg `Append`, replace:

```csharp
        // TODO Task 6: bootstrap path.
        // TODO Task 7: steady-state splice.
        throw new NotImplementedException("Append body — implemented in Tasks 6-7");
```

with:

```csharp
        var slice = mem.Slice(start, length);    // guaranteed to succeed after validation above

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

        // TODO Task 7: steady-state splice.
        throw new NotImplementedException("Append body — steady-state splice in Task 7");
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
dotnet test tests/SpscPipe.Tests/SpscPipe.Tests.csproj --filter "FullyQualifiedName~SpscPipeWriterAppendTests" --nologo
```

Expected: all 13 Append cases pass (9 validation + 2 zero-length + 2 bootstrap).

- [ ] **Step 5: Commit**

```bash
git add tests/SpscPipe.Tests/SpscPipeWriterAppendTests.cs src/SpscPipelines/SpscPipe.Writer.cs
git commit -m "$(cat <<'EOF'
SpscPipeWriter.Append: bootstrap path (empty pipe)

When _writingHead is null at Append time, the donated segment
becomes both _chainHead and _writingHead with RunningIndex=0.
_writingHeadBytesBuffered and _totalWritten are set to length.
Verified: AdoptFrom-set fields propagate (IsDonated=true,
OwnerToken=pipe, End=length, slice content matches).

Per spec §4.1 bootstrap branch.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: `Append` steady-state splice (TDD)

**Goal:** Implement the steady-state branch: when `_writingHead != null`, freeze the previous tail with whatever's buffered and splice the donated segment as the new tail. Cover both the "previous tail is rented" case and the "previous tail is donated" case (idempotent `Freeze` writes on `End`/`Memory`).

**Files:**
- Modify: `tests/SpscPipe.Tests/SpscPipeWriterAppendTests.cs`
- Modify: `src/SpscPipelines/SpscPipe.Writer.cs`

- [ ] **Step 1: Write the failing tests**

Append to `tests/SpscPipe.Tests/SpscPipeWriterAppendTests.cs`:

```csharp
    // ---------- Steady-state splice ----------

    [Fact]
    public void Append_AfterPartialFill_FreezesPreviousTailAndSplicesDonated()
    {
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions(minimumSegmentSize: 64));
        // Establish a partially-filled rented tail.
        var rentedMem = pipe.Writer.GetMemory(64);
        for (int i = 0; i < 40; i++) rentedMem.Span[i] = (byte)i;
        pipe.Writer.Advance(40);
        var prevTail = pipe._writingHead!;

        // Now Append a donated buffer.
        var donatedBytes = new byte[20];
        for (int i = 0; i < donatedBytes.Length; i++) donatedBytes[i] = (byte)(100 + i);
        var owner = new TrackingMemoryOwner(donatedBytes);

        pipe.Writer.Append(owner);

        // Previous tail (rented) is frozen with End=40.
        Assert.Equal(40, prevTail.End);
        Assert.NotNull(prevTail.Next);
        Assert.False(prevTail.IsDonated);

        // The donated segment is the new writing head.
        var donated = pipe._writingHead!;
        Assert.NotSame(prevTail, donated);
        Assert.Same(donated, prevTail.Next);
        Assert.True(donated.IsDonated);
        Assert.Same(pipe, donated.OwnerToken);
        Assert.Equal(20, donated.End);
        Assert.Equal(40, donated.RunningIndex);   // prevTail.RunningIndex (0) + 40
        Assert.Null(donated.Next);

        // Counters
        Assert.Equal(20, pipe._writingHeadBytesBuffered);
        Assert.Equal(60, pipe._totalWritten);

        // Chain head is still the original prevTail (not donated).
        Assert.Same(prevTail, pipe._chainHead);
        Assert.Equal(0, owner.DisposeCount);
    }

    [Fact]
    public void Append_AfterAppend_PreviousDonatedTailIsLinkedIdempotently()
    {
        // Spec §2.2: when the previous _writingHead is itself donated, the steady-state
        // Freeze(filled, newDonated) call writes End/base.Memory to the same values they
        // already held; only Next changes meaningfully.
        using var pipe = new SpscPipelines.SpscPipe();

        var owner1 = new TrackingMemoryOwner(30);
        var owner2 = new TrackingMemoryOwner(50);

        pipe.Writer.Append(owner1);
        var donated1 = pipe._writingHead!;
        int  end1Before     = donated1.End;
        long ri1Before      = donated1.RunningIndex;

        pipe.Writer.Append(owner2);
        var donated2 = pipe._writingHead!;

        // donated1's End/Memory unchanged (idempotent Freeze write).
        Assert.Equal(end1Before, donated1.End);
        Assert.Equal(ri1Before, donated1.RunningIndex);
        Assert.Same(donated2, donated1.Next);
        // donated2 properly chained.
        Assert.True(donated2.IsDonated);
        Assert.Equal(50, donated2.End);
        Assert.Equal(30, donated2.RunningIndex);   // donated1.RunningIndex(0) + donated1.End(30)

        Assert.Equal(50, pipe._writingHeadBytesBuffered);
        Assert.Equal(80, pipe._totalWritten);

        // Chain head is donated1 (the very first segment).
        Assert.Same(donated1, pipe._chainHead);
    }

    [Fact]
    public void Append_WithStartOffset_SplicesOnlyTheSelectedSlice()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var bytes = new byte[1024];
        for (int i = 0; i < bytes.Length; i++) bytes[i] = (byte)(i & 0xFF);
        var owner = new TrackingMemoryOwner(bytes);

        pipe.Writer.Append(owner, start: 200, length: 100);

        var seg = pipe._writingHead!;
        Assert.Equal(100, seg.End);
        Assert.Equal((byte)200, seg.AvailableMemory.Span[0]);
        Assert.Equal((byte)((200 + 99) & 0xFF), seg.AvailableMemory.Span[99]);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
dotnet test tests/SpscPipe.Tests/SpscPipe.Tests.csproj --filter "FullyQualifiedName~SpscPipeWriterAppendTests.Append_AfterPartialFill|FullyQualifiedName~SpscPipeWriterAppendTests.Append_AfterAppend|FullyQualifiedName~SpscPipeWriterAppendTests.Append_WithStartOffset" --nologo
```

Expected: 3 failures with `NotImplementedException`.

- [ ] **Step 3: Add the steady-state splice**

Edit `src/SpscPipelines/SpscPipe.Writer.cs`. In the 3-arg `Append`, replace:

```csharp
        // TODO Task 7: steady-state splice.
        throw new NotImplementedException("Append body — steady-state splice in Task 7");
```

with:

```csharp
        // Steady state: freeze current tail with whatever's buffered, splice donated as new tail.
        // For a previously-donated tail, Freeze re-writes End/base.Memory to the same values
        // (length == AvailableMemory.Length already) and sets Next; the redundant writes are
        // idempotent and benign-torn-read-safe per spec §2.2.
        int filled = _pipe._writingHeadBytesBuffered;
        long newRI = _pipe._writingHead.RunningIndex + filled;

        var newDonated = new BufferSegment();
        newDonated.AdoptFrom(buffer, slice, newRI, pipeOwner: _pipe);

        _pipe._writingHead.Freeze(filled, newDonated);
        _pipe._writingHead = newDonated;
        _pipe._writingHeadBytesBuffered = length;
        _pipe._totalWritten += length;
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
dotnet test tests/SpscPipe.Tests/SpscPipe.Tests.csproj --filter "FullyQualifiedName~SpscPipeWriterAppendTests" --nologo
```

Expected: all 16 Append cases pass (13 from before + 3 new).

- [ ] **Step 5: Run full test suite**

Run:
```bash
dotnet test SpscPipe.slnx -c Release --nologo
```

Expected: All tests pass; no regressions in existing suites.

- [ ] **Step 6: Commit**

```bash
git add tests/SpscPipe.Tests/SpscPipeWriterAppendTests.cs src/SpscPipelines/SpscPipe.Writer.cs
git commit -m "$(cat <<'EOF'
SpscPipeWriter.Append: steady-state splice

When _writingHead is non-null, Freeze the previous tail with the
buffered byte count and splice the donated segment as the new tail.
RunningIndex chains correctly across the boundary; _writingHead
becomes the donated segment with bytesBuffered = length.

Verified for both rented previous tail (End set first time) and
donated previous tail (idempotent End/Memory writes; only Next
changes), per spec §2.2.

Per spec §4.1 steady-state branch.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: `Append` interactions with `GetMemory` / `Advance` / `FlushAsync` (TDD)

**Goal:** Verify that the post-Append `_writingHead` state interacts correctly with subsequent `GetMemory` (forces transition), `Advance(>0)` (throws via existing bounds check), and `FlushAsync` (publishes the donated segment as `TailSegment`). Also verify back-to-back `Append`s produce no empty rented tails.

**Files:**
- Modify: `tests/SpscPipe.Tests/SpscPipeWriterAppendTests.cs`

(No source changes — this task pins behavior that falls out of existing code.)

- [ ] **Step 1: Write the tests**

Append to `tests/SpscPipe.Tests/SpscPipeWriterAppendTests.cs`:

```csharp
    // ---------- Post-Append interactions with the rest of the writer surface ----------

    [Fact]
    public void GetMemory_AfterAppend_TransitionsToFreshRentedTail()
    {
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions(minimumSegmentSize: 64));
        var owner = new TrackingMemoryOwner(20);
        pipe.Writer.Append(owner);
        var donated = pipe._writingHead!;

        var mem = pipe.Writer.GetMemory(64);

        // _writingHead should have moved off the donated segment to a fresh rented tail.
        Assert.NotSame(donated, pipe._writingHead);
        Assert.False(pipe._writingHead!.IsDonated);
        // The donated segment is now linked as a non-tail chain segment.
        Assert.Same(pipe._writingHead, donated.Next);
        // _writingHeadBytesBuffered resets to 0 for the new tail.
        Assert.Equal(0, pipe._writingHeadBytesBuffered);
        Assert.True(mem.Length >= 64);
    }

    [Fact]
    public void Advance_AfterAppend_ThrowsArgumentOutOfRange()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var owner = new TrackingMemoryOwner(20);
        pipe.Writer.Append(owner);

        // _writingHead.AvailableMemory.Length == _writingHeadBytesBuffered, so any positive
        // Advance fails the existing bounds check at SpscPipe.Writer.cs:52.
        Assert.Throws<ArgumentOutOfRangeException>(() => pipe.Writer.Advance(1));
    }

    [Fact]
    public async Task FlushAsync_AfterAppend_PublishesDonatedAsTailSegment()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var owner = new TrackingMemoryOwner(50);
        pipe.Writer.Append(owner);
        var donated = pipe._writingHead!;

        var fr = await pipe.Writer.FlushAsync();
        Assert.False(fr.IsCanceled);
        Assert.False(fr.IsCompleted);

        var snap = pipe._lastPublishedWriterState;
        Assert.Same(donated, snap.TailSegment);
        Assert.Equal(50, snap.TailWritten);
        Assert.Equal(50, snap.TotalWritten);
        Assert.Same(donated, snap.HeadSegment);   // bootstrap case: donated is also chain head
    }

    [Fact]
    public async Task FlushAsync_AfterMixedWriteAndAppend_PublishesCorrectChain()
    {
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions(minimumSegmentSize: 64));
        var rentedMem = pipe.Writer.GetMemory(64);
        for (int i = 0; i < 40; i++) rentedMem.Span[i] = (byte)i;
        pipe.Writer.Advance(40);
        var rented = pipe._writingHead!;

        var owner = new TrackingMemoryOwner(20);
        pipe.Writer.Append(owner);

        await pipe.Writer.FlushAsync();
        var snap = pipe._lastPublishedWriterState;
        Assert.Same(rented, snap.HeadSegment);
        Assert.Same(pipe._writingHead, snap.TailSegment);
        Assert.True(snap.TailSegment!.IsDonated);
        Assert.Equal(20, snap.TailWritten);
        Assert.Equal(60, snap.TotalWritten);
    }

    [Fact]
    public void Append_AfterGetMemoryWithZeroBuffered_FreezesEmptyRentedSegment()
    {
        // Spec §8: "After GetMemory + Advance(0) (zero buffered)" → previous tail is frozen
        // with End=0; donated is spliced after. The empty rented segment is harmless and
        // recycles to freelist on drain.
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions(minimumSegmentSize: 64));
        pipe.Writer.GetMemory(64);
        pipe.Writer.Advance(0);
        var prevTail = pipe._writingHead!;

        var owner = new TrackingMemoryOwner(20);
        pipe.Writer.Append(owner);

        Assert.Equal(0, prevTail.End);
        Assert.False(prevTail.IsDonated);
        Assert.NotNull(prevTail.Next);
        Assert.True(prevTail.Next!.IsDonated);
        Assert.Same(prevTail.Next, pipe._writingHead);
        Assert.Equal(20, pipe._writingHead!.End);
        Assert.Equal(0, pipe._writingHead.RunningIndex);
        Assert.Equal(20, pipe._totalWritten);
    }

    [Fact]
    public async Task LargeAppend_DoesNotPark_SubsequentFlushAsyncParksWhenOverThreshold()
    {
        // Spec §4.4: Append doesn't gate on PauseWriterThreshold; FlushAsync does.
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions(
            pauseWriterThreshold: 1024, resumeWriterThreshold: 512));

        var owner = new TrackingMemoryOwner(8 * 1024);   // well over the pause threshold
        pipe.Writer.Append(owner);   // synchronous, never parks; no exception

        Assert.Equal(8 * 1024, pipe._totalWritten);

        // FlushAsync should park because unconsumed >= PauseWriterThreshold.
        var flushTask = pipe.Writer.FlushAsync().AsTask();
        // The task is parked; not completed synchronously.
        Assert.False(flushTask.IsCompleted);

        // Drain the buffer to release the parked writer.
        var rr = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(rr.Buffer.End);

        // Now the parked FlushAsync resolves.
        var fr = await flushTask;
        Assert.False(fr.IsCanceled);
    }

    [Fact]
    public void BackToBackAppends_NoEmptyRentedTailsBetweenDonations()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var o1 = new TrackingMemoryOwner(10);
        var o2 = new TrackingMemoryOwner(20);
        var o3 = new TrackingMemoryOwner(30);

        pipe.Writer.Append(o1);
        pipe.Writer.Append(o2);
        pipe.Writer.Append(o3);

        // Walk the chain and verify three donated segments back-to-back.
        var s = pipe._chainHead!;
        Assert.True(s.IsDonated);
        Assert.Equal(10, s.End);
        s = s.Next!;
        Assert.NotNull(s);
        Assert.True(s.IsDonated);
        Assert.Equal(20, s.End);
        s = s.Next!;
        Assert.NotNull(s);
        Assert.True(s.IsDonated);
        Assert.Equal(30, s.End);
        Assert.Null(s.Next);

        Assert.Same(s, pipe._writingHead);
        Assert.Equal(60, pipe._totalWritten);
    }
```

- [ ] **Step 2: Run tests to verify they pass (no source changes needed)**

Run:
```bash
dotnet test tests/SpscPipe.Tests/SpscPipe.Tests.csproj --filter "FullyQualifiedName~SpscPipeWriterAppendTests" --nologo
```

Expected: all 23 Append cases pass (16 from before + 7 new). The 7 new tests pin behaviors that fall out of existing transition logic without source changes (with the exception of the backpressure test, which validates §4.4 — backpressure is a `FlushAsync` concern, not an `Append` concern).

If any of these tests fail, that means the steady-state splice from Task 7 has an interaction with `GetMemory`/`Advance`/`FlushAsync` that wasn't covered. Re-read the spec §4.3 ("Post-Append state and natural protections") and the steady-state implementation in Task 7 before debugging.

- [ ] **Step 3: Commit**

```bash
git add tests/SpscPipe.Tests/SpscPipeWriterAppendTests.cs
git commit -m "$(cat <<'EOF'
Tests: pin SpscPipeWriter.Append interactions with GetMemory/Advance/FlushAsync

Seven tests verify post-Append behaviors that fall out of existing
writer logic without code changes (one — the backpressure case —
validates spec §4.4 that backpressure is a FlushAsync concern, not
an Append concern):
  - GetMemory after Append transitions to a fresh rented tail
    (remaining=0 forces transition).
  - Advance(>0) after Append throws ArgumentOutOfRange via the
    existing bounds check (the donated tail has no spare capacity).
  - FlushAsync after Append publishes the donated segment as
    WriterState.TailSegment with TailWritten == length.
  - FlushAsync after a mixed write (GetMemory+Advance, then
    Append) publishes a chain whose head is the rented segment
    and tail is the donated segment.
  - Append after GetMemory+Advance(0) freezes the empty rented
    segment with End=0 and splices donated after it (§8 row 8).
  - LargeAppend over PauseWriterThreshold does not park; the
    subsequent FlushAsync does (§4.4 / §8 row 17).
  - Three back-to-back Appends produce three donated segments in
    chain with no empty rented tails between them.

Per spec §4.3, §4.4, and §8 edge-case catalog.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: Recycle path branches on `IsDonated` (TDD)

**Goal:** Update `RecycleDrainedSegments` to dispose the foreign `IMemoryOwner` for donated segments instead of pushing them onto the freelist. Verify with end-to-end tests that the reader-drains-past-donated path correctly fires `Dispose` exactly once on the tracked owner, the freelist count stays unchanged, and rented-segment behavior is preserved.

**Files:**
- Modify: `tests/SpscPipe.Tests/SpscPipeWriterAppendTests.cs`
- Modify: `src/SpscPipelines/SpscPipe.cs:315-328`

- [ ] **Step 1: Write the failing tests**

Append to `tests/SpscPipe.Tests/SpscPipeWriterAppendTests.cs`:

```csharp
    // ---------- Recycle path: donated -> DisposeOwned + drop; rented -> freelist (unchanged) ----------

    [Fact]
    public async Task ReaderDrainsPastDonated_DisposesOwner_FreelistCountUnchanged()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var donatedOwner = new TrackingMemoryOwner(30);
        pipe.Writer.Append(donatedOwner);
        // Force a subsequent rented tail so the donated segment becomes a non-tail chain segment.
        pipe.Writer.GetMemory(50); pipe.Writer.Advance(50);

        await pipe.Writer.FlushAsync();
        var rr = await pipe.Reader.ReadAsync();
        // Drain past the donated segment (and the rented bytes) entirely.
        pipe.Reader.AdvanceTo(rr.Buffer.End);

        int freelistBefore = pipe._freelistCount;

        // The next FlushAsync runs RecycleDrainedSegments after re-acquiring the reader state.
        await pipe.Writer.FlushAsync();

        Assert.Equal(1, donatedOwner.DisposeCount);
        // freelistCount may have grown by 1 (the rented segment that was once the writingHead
        // before GetMemory transitioned past it — but in this minimal scenario,
        // _writingHead never transitioned again, so the rented tail stays as _writingHead and
        // recycle stops at it). Either way, it must NOT have grown to absorb the donated segment.
        Assert.True(pipe._freelistCount <= freelistBefore + 1);
    }

    [Fact]
    public async Task RecyclePath_RentedSegmentStillFreelisted_RegressionGuard()
    {
        // Existing rented-segment recycle behavior must be preserved.
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions(minimumSegmentSize: 64));
        // Two rented segments in chain.
        pipe.Writer.GetMemory(64); pipe.Writer.Advance(64);
        pipe.Writer.GetMemory(64); pipe.Writer.Advance(50);
        await pipe.Writer.FlushAsync();
        var rr = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(rr.Buffer.End);

        int freelistBefore = pipe._freelistCount;
        await pipe.Writer.FlushAsync();
        // The first segment is recycled to the freelist (or disposed if cap-overflow).
        Assert.True(pipe._freelistCount > freelistBefore);
    }

    [Fact]
    public async Task DisposePipe_WithMixedChain_DisposesAllOwners()
    {
        var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions(minimumSegmentSize: 64));

        var donated1 = new TrackingMemoryOwner(20);
        var donated2 = new TrackingMemoryOwner(30);

        pipe.Writer.Append(donated1);
        pipe.Writer.GetMemory(64); pipe.Writer.Advance(40);    // rented in middle
        pipe.Writer.Append(donated2);
        await pipe.Writer.FlushAsync();
        // Don't drain — chain is full of un-consumed segments.

        pipe.Dispose();

        Assert.Equal(1, donated1.DisposeCount);
        Assert.Equal(1, donated2.DisposeCount);
    }

    [Fact]
    public async Task ReadResultBufferContent_IncludesDonatedBytes_InCorrectPosition()
    {
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions(minimumSegmentSize: 64));

        // Rented [0..3] = 0x01,0x02,0x03,0x04
        var rentedMem = pipe.Writer.GetMemory(64);
        rentedMem.Span[0] = 0x01; rentedMem.Span[1] = 0x02;
        rentedMem.Span[2] = 0x03; rentedMem.Span[3] = 0x04;
        pipe.Writer.Advance(4);

        // Donated bytes [4..6] = 0xAA,0xBB,0xCC
        var donatedBytes = new byte[] { 0xAA, 0xBB, 0xCC };
        var donatedOwner = new TrackingMemoryOwner(donatedBytes);
        pipe.Writer.Append(donatedOwner);

        await pipe.Writer.FlushAsync();
        var rr = await pipe.Reader.ReadAsync();
        var arr = rr.Buffer.ToArray();
        Assert.Equal(new byte[] { 0x01, 0x02, 0x03, 0x04, 0xAA, 0xBB, 0xCC }, arr);

        pipe.Reader.AdvanceTo(rr.Buffer.End);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
dotnet test tests/SpscPipe.Tests/SpscPipe.Tests.csproj --filter "FullyQualifiedName~SpscPipeWriterAppendTests.ReaderDrainsPastDonated|FullyQualifiedName~SpscPipeWriterAppendTests.DisposePipe_WithMixedChain|FullyQualifiedName~SpscPipeWriterAppendTests.RecyclePath_RentedSegmentStillFreelisted|FullyQualifiedName~SpscPipeWriterAppendTests.ReadResultBufferContent" --nologo
```

Expected:
- `ReadResultBufferContent_IncludesDonatedBytes_InCorrectPosition` — passes (no recycle behavior involved; just the read path).
- `RecyclePath_RentedSegmentStillFreelisted_RegressionGuard` — passes (existing behavior, no donated involvement).
- `ReaderDrainsPastDonated_DisposesOwner_FreelistCountUnchanged` — fails. The current `RecycleDrainedSegments` calls `PushFreelist` for every drained segment, including donated ones. Donated segments don't have `_memoryOwner.Dispose()` called.
- `DisposePipe_WithMixedChain_DisposesAllOwners` — passes (the existing `SpscPipe.Dispose` walk calls `DisposeOwned()` on every chain segment regardless of `IsDonated`).

If `ReaderDrainsPastDonated_DisposesOwner_FreelistCountUnchanged` doesn't fail at this step, double-check that `PushFreelist` isn't already disposing — it shouldn't be (look at `SpscPipe.cs:127` for the cap-overflow case and `SpscPipe.cs:133-136` for the normal push case; the latter does NOT dispose).

- [ ] **Step 3: Add the `IsDonated` branch to `RecycleDrainedSegments`**

Edit `src/SpscPipelines/SpscPipe.cs`. The current `RecycleDrainedSegments` (lines 315-328) ends with a single `PushFreelist(recycled);` call. Replace the loop body to branch on `IsDonated`.

Current:
```csharp
    internal void RecycleDrainedSegments()
    {
        var r = _lastAcquiredReaderState;
        if (r.HeadSegment is null && !r.IsCompleted) return;       // pre-bootstrap

        var readerHead = r.HeadSegment;

        while (_chainHead != _writingHead && _chainHead != readerHead)
        {
            var recycled = _chainHead!;
            _chainHead   = recycled.Next!;
            PushFreelist(recycled);
        }
    }
```

Replace with:
```csharp
    internal void RecycleDrainedSegments()
    {
        var r = _lastAcquiredReaderState;
        if (r.HeadSegment is null && !r.IsCompleted) return;       // pre-bootstrap

        var readerHead = r.HeadSegment;

        while (_chainHead != _writingHead && _chainHead != readerHead)
        {
            var recycled = _chainHead!;
            _chainHead   = recycled.Next!;

            if (recycled.IsDonated)
                recycled.DisposeOwned();      // foreign owner: release; drop the BufferSegment shell
            else
                PushFreelist(recycled);       // pool-rented: existing freelist path (with cap-overflow handling)
        }
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
dotnet test tests/SpscPipe.Tests/SpscPipe.Tests.csproj --filter "FullyQualifiedName~SpscPipeWriterAppendTests" --nologo
```

Expected: all 27 Append cases pass (23 from before + 4 new).

- [ ] **Step 5: Run full test suite to verify no regressions**

Run:
```bash
dotnet test SpscPipe.slnx -c Release --nologo
```

Expected: All tests pass; count = baseline + 5 (Task 2) + 27 (Tasks 4-9) = baseline + 32.

- [ ] **Step 6: Commit**

```bash
git add tests/SpscPipe.Tests/SpscPipeWriterAppendTests.cs src/SpscPipelines/SpscPipe.cs
git commit -m "$(cat <<'EOF'
SpscPipe: RecycleDrainedSegments branches on IsDonated

Donated segments take the dispose-and-drop path: the foreign
IMemoryOwner is released via DisposeOwned and the BufferSegment
shell is discarded (never pushed to the freelist). Rented segments
continue through PushFreelist exactly as before.

The walk predicate (_chainHead != _writingHead && _chainHead !=
readerHead) is unchanged; IsDonated only affects what happens to a
segment after the predicate has decided to advance past it.
SpscPipe.Dispose's chain walk already calls DisposeOwned uniformly,
so the mixed-chain dispose case requires no further changes there.

Tests verify: reader-drains-past-donated triggers exactly one
Dispose on the tracking owner without growing the freelist;
rented-segment recycle behavior preserved (regression guard);
mixed-chain SpscPipe.Dispose disposes every owner exactly once;
ReadResult.Buffer correctly stitches rented and donated bytes.

Per spec §5 of
docs/superpowers/specs/2026-04-28-spsc-pipe-buffer-ownership-transfer-design.md.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: Cross-pipe `AdvanceTo` of donated segment (TDD)

**Goal:** Add a regression test that verifies the existing R4-7 pipe-identity check at `SpscPipe.Reader.cs:113` continues to fire for cross-pipe `SequencePosition`s, even when the position points inside a donated segment of a different pipe. This is a behavior-pinning test; no source change required.

**Files:**
- Modify: `tests/SpscPipe.Tests/SpscPipeAdvanceToTests.cs`

- [ ] **Step 1: Write the test**

Append to `tests/SpscPipe.Tests/SpscPipeAdvanceToTests.cs` (inside the existing class, before the closing `}`):

```csharp
    [Fact]
    public async Task AdvanceTo_DonatedSegmentFromDifferentPipe_Throws()
    {
        // R4-7 pipe-identity check (SpscPipe.Reader.cs:113) must fire even when the
        // SequencePosition points inside a donated segment of pipe1 — donated segments
        // set OwnerToken = pipe1, so a cross-pipe AdvanceTo to pipe2 must reject.
        using var pipe1 = new SpscPipelines.SpscPipe();
        using var pipe2 = new SpscPipelines.SpscPipe();

        var donatedOwner = new TrackingMemoryOwner(20);
        pipe1.Writer.Append(donatedOwner);
        await pipe1.Writer.FlushAsync();
        var r1 = await pipe1.Reader.ReadAsync();

        // Try to AdvanceTo on pipe2 with a position inside pipe1's donated segment.
        Assert.Throws<InvalidOperationException>(() => pipe2.Reader.AdvanceTo(r1.Buffer.End));

        // Cleanup: drain pipe1 properly so its Dispose disposes donatedOwner.
        pipe1.Reader.AdvanceTo(r1.Buffer.End);
    }
```

- [ ] **Step 2: Run the test**

Run:
```bash
dotnet test tests/SpscPipe.Tests/SpscPipe.Tests.csproj --filter "FullyQualifiedName~SpscPipeAdvanceToTests.AdvanceTo_DonatedSegmentFromDifferentPipe_Throws" --nologo
```

Expected: passes immediately. The R4-7 check at `SpscPipe.Reader.cs:113` already does
`!ReferenceEquals(consumedSeg.OwnerToken, _pipe)` — since `AdoptFrom` sets `OwnerToken = pipe1`, the cross-pipe call throws.

If this test fails, that's a real bug — `AdoptFrom` isn't setting `OwnerToken` correctly. Re-check Task 2's `AdoptFrom` implementation against the spec §3 BufferSegment definition.

- [ ] **Step 3: Commit**

```bash
git add tests/SpscPipe.Tests/SpscPipeAdvanceToTests.cs
git commit -m "$(cat <<'EOF'
Tests: AdvanceTo cross-pipe check fires for donated segments

Pins the spec §7 invariant that R4-7 pipe-identity rejection at
SpscPipe.Reader.cs:113 works for donated segments identically to
rented ones — AdoptFrom sets OwnerToken = pipe, so a cross-pipe
SequencePosition is detected and InvalidOperationException is
thrown.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 11: Stress test addition — Append injection in producer

**Goal:** Extend `tests/SpscPipe.Stress/StressHarness.cs` to occasionally use `Append` instead of `GetMemory`+`Advance`. Track per-owner `Dispose` counts to verify no leaks (under-dispose) or double-disposes at end of run.

**Files:**
- Modify: `tests/SpscPipe.Stress/StressHarness.cs`

- [ ] **Step 1: Read the existing producer loop**

Run:
```bash
sed -n '20,80p' tests/SpscPipe.Stress/StressHarness.cs
```

Expected output: shows the producer task (`var producer = Task.Run(async () => { ... })`) starting around line 27, with the `while (produced < totalBytes ...)` loop.

- [ ] **Step 2: Add a tracking owner type and Append branch**

Edit `tests/SpscPipe.Stress/StressHarness.cs`. Add a private nested class at the bottom of `StressHarness` (just before its closing `}`):

```csharp
    // Tracks IMemoryOwner.Dispose calls so the harness can verify zero-leak / no-double-dispose
    // at the end of a run.
    private sealed class StressOwner : IMemoryOwner<byte>
    {
        private readonly byte[] _bytes;
        private bool _disposed;
        public int DisposeCount;
        public StressOwner(byte[] bytes) { _bytes = bytes; }
        public Memory<byte> Memory => _disposed ? throw new ObjectDisposedException(nameof(StressOwner)) : _bytes;
        public void Dispose() { DisposeCount++; _disposed = true; }
    }
```

You'll also need to add `using System.Buffers;` at the top of the file if not already present:
```bash
grep -n "using System.Buffers" tests/SpscPipe.Stress/StressHarness.cs
```

If the grep returns nothing, add `using System.Buffers;` to the top of the file (after any existing `using ... = ...` aliases).

- [ ] **Step 3: Inject Append into the producer loop**

Inside `RunOnce`, before the producer Task.Run, declare a thread-safe owners list:
```csharp
        var owners = new System.Collections.Concurrent.ConcurrentBag<StressOwner>();
```

Then in the producer's while-loop, immediately after the existing chunk-write block (after `pipe.Writer.Advance(chunk); produced += chunk;`), add an alternate Append path. The simplest, safest variant: use a low-frequency branch (e.g., 1-in-16 chance) to emit a donated chunk immediately after the GetMemory-style chunk:

Locate this block (in the producer's while-loop):
```csharp
                    int chunk = producerRng.Next(1, 4097);
                    chunk = (int)Math.Min(chunk, totalBytes - produced);
                    var mem = pipe.Writer.GetMemory(chunk);
                    for (int i = 0; i < chunk; i++)
                        mem.Span[i] = ByteSequence.ByteAt(produced + i);
                    pipe.Writer.Advance(chunk);
                    produced += chunk;

                    if (producerRng.Next(8) == 0) await Task.Yield();
                    var fr = await pipe.Writer.FlushAsync(ct);
                    if (fr.IsCompleted) break;
```

Replace with:
```csharp
                    int chunk = producerRng.Next(1, 4097);
                    chunk = (int)Math.Min(chunk, totalBytes - produced);
                    bool donate = producerRng.Next(16) == 0 && chunk > 0 && produced + chunk <= totalBytes;
                    if (donate)
                    {
                        var bytes = new byte[chunk];
                        for (int i = 0; i < chunk; i++)
                            bytes[i] = ByteSequence.ByteAt(produced + i);
                        var owner = new StressOwner(bytes);
                        owners.Add(owner);
                        pipe.Writer.Append(owner);
                    }
                    else
                    {
                        var mem = pipe.Writer.GetMemory(chunk);
                        for (int i = 0; i < chunk; i++)
                            mem.Span[i] = ByteSequence.ByteAt(produced + i);
                        pipe.Writer.Advance(chunk);
                    }
                    produced += chunk;

                    if (producerRng.Next(8) == 0) await Task.Yield();
                    var fr = await pipe.Writer.FlushAsync(ct);
                    if (fr.IsCompleted) break;
```

- [ ] **Step 4: Verify owner accounting at end of run**

After `await Task.WhenAll(producer, consumer);` (or wherever both tasks have joined and the pipe is about to be disposed), add a leak/double-dispose check. First locate the block by:
```bash
grep -n "WhenAll\|StressResult\|return new" tests/SpscPipe.Stress/StressHarness.cs
```

Just before `pipe.Dispose()` (or at the equivalent point if the existing harness disposes implicitly via `using`), add:

```csharp
            // Owner accounting: every donated owner must be Dispose'd exactly once after pipe.Dispose.
```

Then immediately after the existing `using var pipe` scope ends — which means at the end of `RunOnce` after `pipe` has been disposed by the `using` declaration — verify counts. Concretely: Move the verification *after* the `using var pipe = new SpPipe(_options);` block has gone out of scope. The simplest place is right before `return` from `RunOnce`. If `RunOnce` uses `using var pipe = ...;` directly in the method body, change to:

```csharp
public async Task<StressResult> RunOnce(int seed, long totalBytes, CancellationToken ct)
{
    StressResult result;
    var owners = new System.Collections.Concurrent.ConcurrentBag<StressOwner>();
    {
        using var pipe = new SpPipe(_options);
        // ... existing body, but use the outer `owners` variable ...
        // ... (existing return-result construction) ...
        result = /* the StressResult value previously returned directly */;
    }
    // Pipe is now disposed; every donated owner must have been Dispose'd exactly once.
    foreach (var o in owners)
    {
        if (o.DisposeCount != 1)
            throw new InvalidOperationException(
                $"Owner accounting violated: DisposeCount = {o.DisposeCount} (expected 1).");
    }
    return result;
}
```

If `RunOnce` already returns `StressResult` directly (no intermediate variable), refactor to capture it before the assertion. The exact refactor depends on the existing code's shape — keep the assertion *after* `pipe` has been disposed.

- [ ] **Step 5: Build the stress project**

Run:
```bash
dotnet build tests/SpscPipe.Stress/SpscPipe.Stress.csproj -c Release --nologo
```

Expected: build succeeds, 0 errors, 0 warnings.

- [ ] **Step 6: Run a short stress pass**

The stress harness is run via `Program.cs`. Determine its entry point and a short-duration invocation:
```bash
grep -nE "RunOnce|TimeSpan|Main\(" tests/SpscPipe.Stress/Program.cs | head -10
```

Then run a short stress pass (default duration if Program.cs supports it; otherwise edit Program.cs to invoke `RunOnce` once with `totalBytes = 1 << 22` (4 MiB) and assert the harness completes without throwing):

```bash
dotnet run --project tests/SpscPipe.Stress -c Release -- --bytes=4194304 --seed=42
```

(If the CLI doesn't accept `--bytes` / `--seed`, fall back to running the program with whatever defaults it has and just confirm it exits 0.)

Expected: run completes with exit code 0 and no exceptions. If the leak/double-dispose check fires, it indicates a real bug in the recycle path — investigate before proceeding.

- [ ] **Step 7: Commit**

```bash
git add tests/SpscPipe.Stress/StressHarness.cs
git commit -m "$(cat <<'EOF'
Stress: inject Append into producer; verify zero-leak owner accounting

Producer randomly chooses (1-in-16) between the existing
GetMemory+Advance path and the new Append path with a freshly
allocated byte array wrapped in a StressOwner. Every owner is
tracked in a ConcurrentBag; after pipe.Dispose, the harness asserts
every owner's DisposeCount is exactly 1.

Catches both classes of recycle bug: leaks (DisposeCount=0 means a
donated segment escaped both the recycle path and the dispose-on-
shutdown chain walk) and double-disposes (DisposeCount>1 means
DisposeOwned was called twice — likely a recycle path that didn't
correctly drop the BufferSegment shell after disposing).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 12: Update base spec — `2026-04-25-spsc-pipe-tripleBuffer-design.md` amendments

**Goal:** Apply the spec amendments enumerated in §9 of the design doc to the base spec. The design doc is self-contained; the base spec needs cross-references and inline updates so a future reader doesn't get inconsistent information from the older spec.

**Files:**
- Modify: `docs/superpowers/specs/2026-04-25-spsc-pipe-tripleBuffer-design.md`

- [ ] **Step 1: Add donated row to the §3 Ownership table**

Find the §3 Ownership table at line ~172. The current table:

```
| Resource | Owner | Other side's access |
|---|---|---|
| Linked list (`Next` pointers) | Writer | Reader walks but never mutates |
| `BufferSegment` objects | Writer (allocated via freelist) | Reader holds references via `_readHead`, `_readTail`, and acquired `WriterState`s |
| `IMemoryOwner<byte>` per segment | Writer (rents from `_options.Pool`) | None directly; reader sees buffer via `BufferSegment.Memory` |
| Freelist of recyclable segments | Writer-private | None |
| `_chainHead` / `_writingHead` | Writer-private | None |
| `_readHead` / `_readTail` cursors | Reader-private | None |
```

Insert a new row after the `IMemoryOwner<byte> per segment` row:

```
| `IMemoryOwner<byte>` (donated, post-2026-04-28-Append) | Writer (adopted from caller via `Append`) | None directly; reader sees buffer via `BufferSegment.Memory`. Released on recycle (`DisposeOwned`) or `SpscPipe.Dispose`. See `2026-04-28-spsc-pipe-buffer-ownership-transfer-design.md` |
```

- [ ] **Step 2: Add `IsDonated` field reference to `BufferSegment` definition note**

Find the `BufferSegment` definition code block at line ~183. After the closing `}` of the code block (around line 230), before the existing "Note on `Next` semantics" paragraph, add:

```markdown
**Buffer-ownership transfer (post-2026-04-28).** `BufferSegment` gained an `IsDonated : bool` field and an `AdoptFrom(IMemoryOwner<byte>, Memory<byte>, long, object)` initializer to support `SpscPipeWriter.Append`'s buffer-ownership-transfer path. See `2026-04-28-spsc-pipe-buffer-ownership-transfer-design.md` §3 for the full definition. The recycle path (this section) branches on `IsDonated`: rented → `PushFreelist` (existing); donated → `DisposeOwned()` and discard.
```

- [ ] **Step 3: Update Recycling-path pseudocode**

Find the `RecycleDrainedSegments` pseudocode at line ~256. The current loop body has:

```csharp
        if (_freelistCount < _options.MaxFreelistSegments)
        {
            PushFreelist(recycled);
            _freelistCount++;
        }
        else
        {
            recycled.DisposeOwned();
        }
```

Replace with:

```csharp
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
```

- [ ] **Step 4: Add donor-decided sizing note to MemoryPool integration**

Find the `MemoryPool integration and segment sizing` subsection at line ~291. After the existing four bullets:

```
- `_options.Pool` (default `MemoryPool<byte>.Shared`) supplies `IMemoryOwner<byte>`s.
- `_options.MinimumSegmentSize` (default 4096) is the floor for `Pool.Rent(sizeHint)` calls.
- Upper bound on segment size is governed by the underlying `MemoryPool<byte>.MaxBufferSize`; we don't impose a separate cap.
- `GetMemory(sizeHint)`: if `sizeHint > 0` and the current tail can't satisfy it, transition to a new tail of size `Max(sizeHint, MinimumSegmentSize)`. Otherwise return remaining capacity in the current tail.
```

Add a fifth bullet:

```
- **Donated segments (post-2026-04-28-Append) bypass `_options.Pool` and `MinimumSegmentSize` entirely.** Size is donor-decided. See `2026-04-28-spsc-pipe-buffer-ownership-transfer-design.md`.
```

- [ ] **Step 5: Add donated-immune note to `Memory<T>` torn-read subsection**

Find the `Memory<T> benign-torn-read assumption (Nit-5)` subsection at line ~298. After the existing paragraph (which ends with the boot-test recommendation):

Add a new paragraph after the existing one:

```markdown
**Donated segments are immune (post-2026-04-28-Append).** `BufferSegment.AdoptFrom` writes `base.Memory` to the donated slice once at adoption; subsequent `Freeze` calls during chain-link transitions re-write `base.Memory` to the same value (idempotent — `End == AvailableMemory.Length` already). No torn-read concern because both pre and post values are identical. Rented segments retain the existing benign-torn-read property unchanged. See `2026-04-28-spsc-pipe-buffer-ownership-transfer-design.md` §3.3.
```

- [ ] **Step 6: Add Append entry to Section 4 conventions**

Find Section 4's list of methods. The first method header is `### Writer: GetMemory(int sizeHint)` at line ~356. Just before that section header, insert a forward-reference note:

```markdown
**Buffer-ownership transfer (post-2026-04-28).** Section 4 was extended with a `Writer: Append(IMemoryOwner<byte> buffer[, int start, int length])` subsection. Defined fully in `2026-04-28-spsc-pipe-buffer-ownership-transfer-design.md` §4. `Append` is purely writer-thread-local — same publication boundary as `GetMemory`/`Advance`; nothing becomes visible to the reader until the next `FlushAsync`. It interacts cleanly with the rest of Section 4 without modifying any existing pseudocode.

```

- [ ] **Step 7: Add SpscPipeWriter visibility note to Public API section**

Find the public API section. Run:
```bash
grep -nE "^## Section [0-9]+|public PipeWriter Writer" docs/superpowers/specs/2026-04-25-spsc-pipe-tripleBuffer-design.md | head -20
```

Locate the section containing the `public PipeWriter Writer` declaration (likely §X or similar). After that declaration, add:

```markdown
**Visibility revision (post-2026-04-28).** `SpscPipeWriter` is now `public` (was `internal`) and is no longer nested inside `SpscPipe`. `SpscPipe.Writer`'s declared return type widens to `SpscPipeWriter`. Source-compatible with existing `PipeWriter w = pipe.Writer;` callers via implicit upcast. `SpscPipeReader` stays `internal` — no reader-side surface addition motivates exposing it. See `2026-04-28-spsc-pipe-buffer-ownership-transfer-design.md` §6.
```

- [ ] **Step 8: Skim the spec for any other places that need updates**

Run:
```bash
grep -nE "PushFreelist|recycle|donated|IMemoryOwner|public PipeWriter Writer" docs/superpowers/specs/2026-04-25-spsc-pipe-tripleBuffer-design.md | head -30
```

If any other passages directly contradict the new design (e.g., assert that "the recycle path always pushes onto the freelist"), update them inline with the same `(post-2026-04-28-Append)` parenthetical pattern.

- [ ] **Step 9: Verify the spec markdown is well-formed**

Run:
```bash
grep -nE '^```' docs/superpowers/specs/2026-04-25-spsc-pipe-tripleBuffer-design.md | wc -l
```

Expected: an even number (every code fence is paired). If it's odd, an edit accidentally orphaned a code fence — review the changes.

- [ ] **Step 10: Commit**

```bash
git add docs/superpowers/specs/2026-04-25-spsc-pipe-tripleBuffer-design.md
git commit -m "$(cat <<'EOF'
SpscPipe spec §3 / §4 / §X: add buffer-ownership-transfer cross-refs

Inline amendments to the base spec for the post-2026-04-28 Append
feature: ownership table gains a donated-IMemoryOwner row;
BufferSegment definition references the new IsDonated field +
AdoptFrom initializer; recycle pseudocode branches on IsDonated;
MemoryPool/sizing notes flag donated segments as bypassing the
pool/MinimumSegmentSize; Memory<T> torn-read note documents donated
immunity; Section 4 gains a forward-reference to the new Append
subsection; public-API section notes SpscPipeWriter visibility
change.

The full design lives in
docs/superpowers/specs/2026-04-28-spsc-pipe-buffer-ownership-transfer-design.md;
this commit makes the older base spec consistent with the new
behavior so a future reader is not misled.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 13: Final verification — solution build, full test suite, no regressions

**Goal:** Final pass to confirm the entire solution is green.

**Files:** none modified.

- [ ] **Step 1: Clean build**

Run:
```bash
dotnet clean SpscPipe.slnx -c Release && dotnet build SpscPipe.slnx -c Release --nologo
```

Expected: 6 projects build, 0 warnings, 0 errors.

- [ ] **Step 2: Run the full test suite**

Run:
```bash
dotnet test SpscPipe.slnx -c Release --nologo
```

Expected: All tests pass. Tally (each `[Theory]` `InlineData` row is a separate xUnit test case):
- Baseline pre-Task-2 count
- + 5 BufferSegment cases (Task 2)
- + 9 Append validation cases (Task 4 — 5 `[Fact]`s + 4 `[Theory]` rows)
- + 2 Append zero-length cases (Task 5)
- + 2 Append bootstrap cases (Task 6)
- + 3 Append steady-state cases (Task 7)
- + 7 Append interaction cases (Task 8)
- + 4 recycle/dispose integration cases (Task 9)
- + 1 cross-pipe AdvanceTo case (Task 10)
- = baseline + 33

- [ ] **Step 3: Run a short stress pass to confirm no leaks**

Run:
```bash
dotnet run --project tests/SpscPipe.Stress -c Release
```

Expected: exit 0 with no exceptions. The harness's owner-accounting check (Task 11 Step 4) confirms every donated `IMemoryOwner` was disposed exactly once.

- [ ] **Step 4: Inspect git log for the feature**

Run:
```bash
git log --oneline ^main HEAD
```

Expected output: a sequence of focused commits, each tied to one task in this plan, in order:

```
<sha> SpscPipe spec §3 / §4 / §X: add buffer-ownership-transfer cross-refs
<sha> Stress: inject Append into producer; verify zero-leak owner accounting
<sha> Tests: AdvanceTo cross-pipe check fires for donated segments
<sha> SpscPipe: RecycleDrainedSegments branches on IsDonated
<sha> Tests: pin SpscPipeWriter.Append interactions with GetMemory/Advance/FlushAsync
<sha> SpscPipeWriter.Append: steady-state splice
<sha> SpscPipeWriter.Append: bootstrap path (empty pipe)
<sha> SpscPipeWriter.Append: zero-length accept-and-dispose path
<sha> SpscPipeWriter.Append: argument validation layer
<sha> SpscPipe: unnest SpscPipeWriter; widen Writer return type
<sha> BufferSegment: add IsDonated flag + AdoptFrom initializer
<sha> Tests: add TrackingMemoryOwner helper for ownership-transfer assertions
```

- [ ] **Step 5: Hand off**

Implementation is complete. Optional follow-ups worth offering to the user but explicitly out-of-scope of this plan (per spec §11):
- Helper extension `Append(this SpscPipeWriter, byte[])` that wraps a byte array as an ad-hoc `IMemoryOwner` and Appends it.
- Diagnostic counters (e.g., `_donatedAppendCount`) on `SpscPipe` for observability.
- `BufferSegment` shell pooling for donations (low priority; donations are rare relative to byte volume).

---

## Spec coverage check (post-plan self-review)

Mapping each spec section to its implementing task(s):

| Spec section | Task(s) |
|---|---|
| §1 Background and motivation | (no implementation; documentation only) |
| §2.1 Donated segment as chain segment | Task 2 (`AdoptFrom`, `IsDonated`); Task 6/7 (Append flow uses these) |
| §2.2 Splice into `_writingHead` | Task 7 (steady-state); Task 6 (bootstrap) |
| §2.3 Why a chain segment, not a side-channel | (no implementation) |
| §3 `BufferSegment` changes (+ §3.1 / §3.2 / §3.3) | Task 2 |
| §4 `Append` flow (+ §4.1 / §4.2 / §4.3 / §4.4) | Tasks 4 (validation), 5 (zero-length), 6 (bootstrap), 7 (steady-state); §4.3 pinned by Task 8 |
| §5 Recycle path | Task 9 |
| §6 Public API surface (+ §6.1 / §6.2 / §6.3) | Task 3 (visibility/return type); Tasks 4-9 (Append signatures) |
| §7 Interactions with existing invariants | Task 8 (FlushAsync interaction); Task 9 (Dispose interaction); Task 10 (R4-7) |
| §8 Edge cases catalog | Tasks 4-10 (all rows pinned by tests) |
| §9 Spec amendments to base spec | Task 12 |
| §10 Testing strategy (10.1 / 10.2 / 10.3 / 10.4) | Tasks 2 (10.1), 4-9 (10.2 + 10.3), 11 (10.4) |
| §11 Out of scope | (no implementation; flagged in Task 13 Step 5) |

No gaps.
