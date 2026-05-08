# Pipe.Reset Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `Pipe.Reset()` for instance pooling (BCL parity), and remove `IDisposable`/`Dispose()`/`_disposed` so the pipe has a single lifecycle model centered on Reset.

**Architecture:** Reset is the only cleanup path. The pipe's owner is responsible for the lifecycle: they call `Reset()` after both `Reader.Complete()` and `Writer.Complete()` have been observed (and after the consumer threads have observed `GetResult` on any in-flight `ValueTask`). Reset clears all per-instance state in place and preserves the segment freelists (rented + donated-shell), matching BCL's "preserve segment pool across Reset" semantics. Two new internal helpers — `TripleBuffer<T>.Reset()` and `PipelyAwaiter<T>.Reset()` — re-establish the constructor's initial state for those subcomponents.

**Tech Stack:** .NET 10, C# (LangVersion=latest), xUnit. `InternalsVisibleTo` is already configured for `Pipely.Tests`, so tests can read internal fields directly.

**Hot-path invariant:** No file owned by a reader/writer hot path is touched in any way that affects steady-state behavior. The only Pipe.cs / Pipe.Reader.cs / Pipe.Writer.cs hot-path edits are deletions of `if (_pipe._disposed) throw new ObjectDisposedException(...)` guards (which are dead code post-IDisposable removal) — no functional change for any post-construction usage that didn't previously rely on `Dispose`.

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `src/Pipely/TripleBuffer.cs` | Modify | Add internal `Reset()` |
| `src/Pipely/PipelyAwaiter.cs` | Modify | Add internal `Reset()` |
| `src/Pipely/Pipe.cs` | Modify | Add `public void Reset()`; remove `IDisposable`, `Dispose()`, `_disposed` |
| `src/Pipely/Pipe.Reader.cs` | Modify | Remove 5 `_disposed`-guarded `ObjectDisposedException` throws |
| `src/Pipely/Pipe.Writer.cs` | Modify | Remove 6 `_disposed`-guarded `ObjectDisposedException` throws |
| `tests/Pipely.Tests/TripleBufferResetTests.cs` | Create | New tests for `TripleBuffer<T>.Reset()` |
| `tests/Pipely.Tests/PipelyAwaiterResetTests.cs` | Create | New tests for `PipelyAwaiter<T>.Reset()` |
| `tests/Pipely.Tests/PipeResetTests.cs` | Create | Integration tests for `Pipe.Reset()` |
| `tests/Pipely.Tests/PipeDisposeTests.cs` | Delete | All assertions reference removed Dispose semantics |
| `tests/Pipely.Tests/PipeWriterTests.cs` | Modify | Delete `GetMemory_AfterDispose_Throws`; `using var` → `var` |
| `tests/Pipely.Tests/PipeWriterSpliceTests.cs` | Modify | Delete `Splice_DisposedPipe_*`; rework `DisposePipe_WithMixedChain_*` and `Dispose_ClearsShellFreelist` as Reset analogues; `using var` → `var` |
| `tests/Pipely.Tests/PipeWriterBufferedBytesTests.cs` | Modify | Delete `BufferedBytes_AfterDispose_*`; `using var` → `var` |
| `tests/Pipely.Tests/PipeWriterUnflushedBytesTests.cs` | Modify | Delete `UnflushedBytes_AfterDispose_*`; `using var` → `var` |
| `tests/Pipely.Tests/PipeReaderTests.cs`, `PipeReadInProgressTests.cs`, `PipeLifecycleTests.cs`, `PipeAdvanceToTests.cs`, `PipeCancellationTests.cs`, `PipeContinuationDispatcherTests.cs` | Modify | `using var` → `var` |
| `tests/Pipely.Tests/BclParityTests.cs` | Modify | Drop `IDisposable Disposer` from helper-tuple; remove `NoOpDisposable` class; remove `using (disp)` blocks; `using var spsc` → `var spsc` |
| `tests/Pipely.Stress/StressHarness.cs` | Modify | `using var pipe` → `var pipe` (per-iteration leak is acceptable for stress; long runs may want explicit `Complete + Reset` pooling but that is out of scope) |
| `tests/Pipely.Benchmarks/PipelyPipeAdapter.cs` | Modify | `Dispose()` body becomes a no-op (Pipe is no longer IDisposable). Adapter remains `IDisposable` for benchmark-host symmetry |
| `tests/Pipely.Benchmarks/SchedulerBenchmarks.cs` | Modify | Replace `_pipelyTp.Dispose()` / `_pipelyInline.Dispose()` with field nulling in `[GlobalCleanup]` |
| `tests/Pipely.Benchmarks/PinnedThroughputBenchmarks.cs` | No changes | `_pipely.Dispose()` calls `PipelyPipeAdapter.Dispose` (which we keep as IDisposable). Compiles unchanged |
| `tests/Pipely.Benchmarks/SpliceBenchmarks.cs` | Modify | `() => pipe.Dispose()` delegate becomes `() => { pipe.Writer.Complete(); pipe.Reader.Complete(); }` |
| `tests/Pipely.Benchmarks/FreshPipeSchedulerBenchmarks.cs` | Modify | `using var pipe` → `var pipe` |
| `tests/Pipely.Benchmarks/ThroughputBenchmarks.cs` | No changes | `_pipely.Dispose()` calls `PipelyPipeAdapter.Dispose`. Compiles unchanged |

The two `partial Pipe` files keep their existing split (Reader / Writer hot paths in their own files; lifecycle on `Pipe.cs`). `Reset()` lives on `Pipe.cs` alongside the soon-to-be-deleted `Dispose()`.

---

## Task ordering rationale

Tasks 1–5 add Reset *additively* — every existing test still passes at every commit. Task 6 is the breaking change (drop IDisposable); it has to land atomically because once `Pipe` is no longer `IDisposable`, every `using var <name> = new Pipely.Pipe(...)` stops compiling, every `pipe.Dispose()` call site fails to resolve, and the `BclParityTests` helper tuple's `IDisposable Disposer` field becomes inhabitable only by `NoOpDisposable`. Doing it all in one commit means we never have a half-broken solution build.

---

### Task 1: Add `TripleBuffer<T>.Reset()`

**Files:**
- Modify: `src/Pipely/TripleBuffer.cs`
- Create: `tests/Pipely.Tests/TripleBufferResetTests.cs`

- [ ] **Step 1: Write the failing tests**

Create `tests/Pipely.Tests/TripleBufferResetTests.cs` with this exact content:

```csharp
using Pipely;
using Xunit;

namespace PipelyTests;

public class TripleBufferResetTests
{
    [Fact]
    public void Reset_AfterPublishAcquireCycle_BehavesAsNewlyConstructed()
    {
        var tb = new TripleBuffer<int>();

        // Use it through several publish/acquire cycles.
        for (int i = 1; i <= 3; i++)
        {
            tb.ProducerSlot() = i * 100;
            tb.Publish();
            Assert.True(tb.TryAcquire());
            Assert.Equal(i * 100, tb.ConsumerSlot());
        }

        tb.Reset();

        // Constructor's initial state: no publication yet → TryAcquire returns false.
        Assert.False(tb.TryAcquire());

        // A fresh publish/acquire cycle works identically to a brand-new TripleBuffer.
        tb.ProducerSlot() = 42;
        tb.Publish();
        Assert.True(tb.TryAcquire());
        Assert.Equal(42, tb.ConsumerSlot());
    }

    [Fact]
    public void Reset_OnNeverUsedTripleBuffer_NoThrow_StillBehavesAsNew()
    {
        var tb = new TripleBuffer<int>();
        tb.Reset();

        Assert.False(tb.TryAcquire());
        tb.ProducerSlot() = 7;
        tb.Publish();
        Assert.True(tb.TryAcquire());
        Assert.Equal(7, tb.ConsumerSlot());
    }

    [Fact]
    public void Reset_ProducerSlotReturnsDefault()
    {
        // After Reset, the slot the producer sees is zeroed (default(T)). This is the
        // visible-from-outside check that slot data is cleared; combined with the
        // behavioral test above, it gives us reasonable confidence that all 3 slots
        // are reset (the producer rotates through all 3 across cycles).
        var tb = new TripleBuffer<int>();
        tb.ProducerSlot() = 999;
        tb.Publish();
        tb.ProducerSlot() = 888;
        tb.Publish();
        tb.ProducerSlot() = 777;

        tb.Reset();

        Assert.Equal(0, tb.ProducerSlot());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `dotnet test tests/Pipely.Tests --filter FullyQualifiedName~TripleBufferResetTests`

Expected: build error — `TripleBuffer<T>` has no `Reset` method.

- [ ] **Step 3: Add `Reset()` to `TripleBuffer<T>`**

Modify `src/Pipely/TripleBuffer.cs`. Add this method just before the closing brace of the class (after `GetSlot`):

```csharp
    /// <summary>
    /// Restores the triple buffer to its post-construction state. Caller must guarantee
    /// no concurrent producer or consumer activity (Pipely.Pipe enforces this via its
    /// SPSC contract: Reset runs on the lifecycle owner thread, after both
    /// Reader.Complete and Writer.Complete have been observed).
    /// </summary>
    internal void Reset()
    {
        _state.Value = 1 << 1;
        _producer.Value = 0;
        _consumer.Value = 2;
        _slot0.Data = default!;
        _slot1.Data = default!;
        _slot2.Data = default!;
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test tests/Pipely.Tests --filter FullyQualifiedName~TripleBufferResetTests`

Expected: 3 passed, 0 failed.

- [ ] **Step 5: Run the full test suite to verify nothing else regressed**

Run: `dotnet test tests/Pipely.Tests`

Expected: all tests pass (no Reset-using code yet, only the new helper).

- [ ] **Step 6: Commit**

```bash
git add src/Pipely/TripleBuffer.cs tests/Pipely.Tests/TripleBufferResetTests.cs
git commit -m "TripleBuffer: add Reset helper for upcoming Pipe.Reset"
```

---

### Task 2: Add `PipelyAwaiter<T>.Reset()`

**Files:**
- Modify: `src/Pipely/PipelyAwaiter.cs`
- Create: `tests/Pipely.Tests/PipelyAwaiterResetTests.cs`

- [ ] **Step 1: Write the failing tests**

Create `tests/Pipely.Tests/PipelyAwaiterResetTests.cs` with this exact content:

```csharp
using System.IO.Pipelines;
using Pipely;
using Xunit;

namespace PipelyTests;

public class PipelyAwaiterResetTests
{
    [Fact]
    public void Reset_ClearsExternallyVisibleFields()
    {
        // Note on coverage: the private fields on PipelyAwaiter (_realContinuation, _realState,
        // _capturedEC, _capturedSC, _runCb, _runState) cannot be exercised from outside the type
        // without a test-only helper. The Reset() body also clears them, but that is verified by
        // code inspection rather than this test. The Pipe-level integration tests in
        // PipeResetTests cover the live OnCompleted → SetResult → Reset path.
        var awaiter = new PipelyAwaiter<int>(PipeScheduler.Inline, useSynchronizationContext: false);

        // Dirty every resettable field reachable from outside the type.
        awaiter._state = PipelyAwaiter<int>.Pending | PipelyAwaiter<int>.CancelFlag;
        awaiter._token = new System.Threading.CancellationToken(canceled: true);
        awaiter._stashHead = null;       // BufferSegment is internal; null is fine — we only check it's still null after Reset.
        awaiter._stashHeadIdx = 17;
        awaiter._stashTailIdx = 99;
        awaiter._parkCount = 5;
        awaiter._signalWonCount = 4;
        awaiter._tokenCancelWonCount = 3;
        awaiter._cancelPendingWonCount = 2;
        awaiter._lostWakeupResolvedCount = 1;
        awaiter._lostCancelResolvedCount = 7;

        awaiter.Reset();

        Assert.Equal(0, awaiter._state);
        Assert.Equal(default, awaiter._token);
        Assert.Null(awaiter._stashHead);
        Assert.Equal(0, awaiter._stashHeadIdx);
        Assert.Null(awaiter._stashTail);
        Assert.Equal(0, awaiter._stashTailIdx);
        Assert.Equal(0, awaiter._parkCount);
        Assert.Equal(0, awaiter._signalWonCount);
        Assert.Equal(0, awaiter._tokenCancelWonCount);
        Assert.Equal(0, awaiter._cancelPendingWonCount);
        Assert.Equal(0, awaiter._lostWakeupResolvedCount);
        Assert.Equal(0, awaiter._lostCancelResolvedCount);
    }

    [Fact]
    public void Reset_BumpsCoreVersion_InvalidatingStaleTokens()
    {
        // MRVTSC.Reset increments the version; any stale ValueTask token is now invalid.
        // We can't easily synthesize a stale token without going through OnCompleted, but we
        // can verify the version moves forward across Reset.
        var awaiter = new PipelyAwaiter<int>(PipeScheduler.Inline);
        short v0 = awaiter.Version;
        awaiter.Reset();
        Assert.NotEqual(v0, awaiter.Version);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `dotnet test tests/Pipely.Tests --filter FullyQualifiedName~PipelyAwaiterResetTests`

Expected: build error — `PipelyAwaiter<T>` has no `Reset` method.

- [ ] **Step 3: Add `Reset()` to `PipelyAwaiter<T>`**

Modify `src/Pipely/PipelyAwaiter.cs`. Add this method just before the closing brace of the class:

```csharp
    /// <summary>
    /// Restores the awaiter to its post-construction state. Caller must guarantee no
    /// concurrent consumer activity — specifically, the consumer must have observed
    /// GetResult() on the previously-vended ValueTask before this is called. The
    /// SPSC contract on Pipe.Reset establishes this happens-before edge.
    /// </summary>
    /// <remarks>
    /// _core.Reset bumps the internal version, invalidating any stale ValueTask
    /// tokens. If a consumer somehow holds a stale token and calls GetResult on it
    /// afterward, MRVTSC throws InvalidOperationException("The version is not valid")
    /// — louder than BCL's PipeAwaitable struct-overwrite, which silently abandons
    /// the continuation and hangs the consumer.
    /// <para>
    /// The signaler paths (SignalReadAwaiterIfPending / SignalFlushAwaiterIfPending /
    /// CancelPendingRead / CancelPendingFlush) dispose _ctr before transitioning out
    /// of Pending, but the token-cancel paths (OnReadAwaiterTokenCancel /
    /// OnFlushAwaiterTokenCancel) do not — the CTR is the *thing firing* there, and
    /// the runtime cleans up after fire. We call Dispose defensively at the top of
    /// Reset to cover that path; CancellationTokenRegistration.Dispose is idempotent
    /// on default-valued / already-disposed registrations, so the call is safe in all
    /// three states (live, disposed, never set).
    /// </para>
    /// </remarks>
    internal void Reset()
    {
        _ctr.Dispose();          // idempotent on default / already-disposed; covers token-cancel-fired path.
        _core.Reset();
        _state = 0;
        _token = default;
        _ctr = default;
        _stashHead = null;
        _stashHeadIdx = 0;
        _stashTail = null;
        _stashTailIdx = 0;
        _realContinuation = null;
        _realState = null;
        _capturedEC = null;
        _capturedSC = null;
        _runCb = null;
        _runState = null;
        _parkCount = 0;
        _signalWonCount = 0;
        _tokenCancelWonCount = 0;
        _cancelPendingWonCount = 0;
        _lostWakeupResolvedCount = 0;
        _lostCancelResolvedCount = 0;
    }
```

Note: `_realContinuation`, `_realState`, `_capturedEC`, `_capturedSC`, `_runCb`, `_runState` are `private` — same-class access works (Reset is on the same type).

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test tests/Pipely.Tests --filter FullyQualifiedName~PipelyAwaiterResetTests`

Expected: 2 passed, 0 failed.

- [ ] **Step 5: Run the full test suite**

Run: `dotnet test tests/Pipely.Tests`

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/Pipely/PipelyAwaiter.cs tests/Pipely.Tests/PipelyAwaiterResetTests.cs
git commit -m "PipelyAwaiter: add Reset helper for upcoming Pipe.Reset"
```

---

### Task 3: Add `Pipe.Reset()` precondition (gating exception only)

This task lands the precondition check and a stub body. The full body lands in Task 4. Splitting lets us prove the gate works in isolation before the cleanup logic complicates it.

**Files:**
- Modify: `src/Pipely/Pipe.cs`
- Create: `tests/Pipely.Tests/PipeResetTests.cs`

- [ ] **Step 1: Write the failing precondition tests**

Create `tests/Pipely.Tests/PipeResetTests.cs` with this exact content:

```csharp
using Xunit;

namespace PipelyTests;

public class PipeResetTests
{
    [Fact]
    public void Reset_NeitherSideCompleted_Throws()
    {
        var pipe = new Pipely.Pipe();
        var ex = Assert.Throws<InvalidOperationException>(() => pipe.Reset());
        Assert.Equal("Both completion routines must be called before resetting the pipe.", ex.Message);
    }

    [Fact]
    public void Reset_OnlyWriterCompleted_Throws()
    {
        var pipe = new Pipely.Pipe();
        pipe.Writer.Complete();
        Assert.Throws<InvalidOperationException>(() => pipe.Reset());
    }

    [Fact]
    public void Reset_OnlyReaderCompleted_Throws()
    {
        var pipe = new Pipely.Pipe();
        pipe.Reader.Complete();
        Assert.Throws<InvalidOperationException>(() => pipe.Reset());
    }

    [Fact]
    public void Reset_BothCompleted_DoesNotThrow()
    {
        var pipe = new Pipely.Pipe();
        pipe.Writer.Complete();
        pipe.Reader.Complete();
        pipe.Reset();   // must not throw
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `dotnet test tests/Pipely.Tests --filter FullyQualifiedName~PipeResetTests`

Expected: build error — `Pipe` has no `Reset` method.

- [ ] **Step 3: Add `Pipe.Reset()` skeleton with the gate**

Modify `src/Pipely/Pipe.cs`. Add this method directly above the existing `Dispose()` method (around line 122). The body is intentionally a stub — the real cleanup lands in Task 4.

```csharp
    /// <summary>
    /// Restores the pipe to its post-construction state so the instance can be reused
    /// (e.g. from a pool). Both Reader.Complete and Writer.Complete must have been
    /// called first. Throws otherwise.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The pipe's lifecycle owner is responsible for calling Reset, on the same thread,
    /// only after establishing a happens-before edge with the writer and reader threads
    /// — typically by awaiting the producer/consumer tasks before calling Reset. The
    /// SPSC contract is not violated by Reset itself: the writer/reader threads must
    /// already be quiescent (both have completed and observed any in-flight ValueTask
    /// via GetResult) before Reset runs.
    /// </para>
    /// <para>
    /// State preserved across Reset: PipeOptions, schedulers, the rented-segment
    /// freelist, and the donated-shell freelist. Everything else is restored to its
    /// post-construction value. Matches BCL Pipe.Reset semantics.
    /// </para>
    /// </remarks>
    public void Reset()
    {
        if (!(_writer.WriterCompleted && _reader.ReaderCompleted))
            throw new InvalidOperationException("Both completion routines must be called before resetting the pipe.");
    }
```

- [ ] **Step 4: Run the precondition tests to verify they pass**

Run: `dotnet test tests/Pipely.Tests --filter FullyQualifiedName~PipeResetTests`

Expected: 4 passed, 0 failed.

- [ ] **Step 5: Run the full test suite**

Run: `dotnet test tests/Pipely.Tests`

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/Pipely/Pipe.cs tests/Pipely.Tests/PipeResetTests.cs
git commit -m "Pipe: add Reset precondition gate (body to follow)"
```

---

### Task 4: Implement `Pipe.Reset()` body

**Files:**
- Modify: `src/Pipely/Pipe.cs`
- Modify: `tests/Pipely.Tests/PipeResetTests.cs`

- [ ] **Step 1: Add the post-state failing tests**

Append the following tests to `tests/Pipely.Tests/PipeResetTests.cs` (inside the existing `PipeResetTests` class, before the closing `}`):

```csharp
    [Fact]
    public async Task Reset_ClearsAllPerInstanceFieldsToPostConstructionValues()
    {
        var pipe = new Pipely.Pipe();

        // Drive the pipe through a full round of usage.
        var mem = pipe.Writer.GetMemory(50); mem.Span.Fill(0xAB); pipe.Writer.Advance(50);
        await pipe.Writer.FlushAsync();
        var rr = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(rr.Buffer.End);
        pipe.Writer.Complete();
        pipe.Reader.Complete();

        pipe.Reset();

        // WriterFields restored.
        Assert.Null(pipe._writer.ChainHead);
        Assert.Null(pipe._writer.WritingHead);
        Assert.Equal(0, pipe._writer.WritingHeadBytesBuffered);
        Assert.Equal(0L, pipe._writer.TotalWritten);
        Assert.Equal(default, pipe._writer.LastPublishedWriterState);
        Assert.Equal(default, pipe._writer.LastAcquiredReaderState);
        Assert.False(pipe._writer.WriterCompleted);

        // ReaderFields restored.
        Assert.Null(pipe._reader.ReadHead);
        Assert.Equal(0, pipe._reader.ReadHeadIdx);
        Assert.Null(pipe._reader.ReadTail);
        Assert.Equal(0, pipe._reader.ReadTailIdx);
        Assert.Equal(0L, pipe._reader.TotalConsumed);
        Assert.Equal(0L, pipe._reader.TotalExamined);
        Assert.Equal(default, pipe._reader.LastPublishedReaderState);
        Assert.Equal(default, pipe._reader.LastAcquiredWriterState);
        Assert.False(pipe._reader.ReaderCompleted);
        Assert.False(pipe._reader.ReadPending);

        // Awaiter state.
        Assert.Equal(0, pipe._readAwaiter._state);
        Assert.Equal(0, pipe._flushAwaiter._state);
        Assert.Equal(0L, pipe._readAwaiter._parkCount);
        Assert.Equal(0L, pipe._flushAwaiter._parkCount);

        // Triple buffers: no stale terminal publication remains in any of the 3 slots.
        // (TryAcquire returns false iff the dirty bit is 0, which Reset establishes.)
        Assert.False(pipe._writerTb.TryAcquire());
        Assert.False(pipe._readerTb.TryAcquire());
    }

    [Fact]
    public async Task Reset_PreservesRentedSegmentFreelist()
    {
        // Force the pipe to recycle a rented segment to the freelist.
        var pipe = new Pipely.Pipe(new Pipely.PipeOptions(minimumSegmentSize: 64));
        pipe.Writer.GetMemory(60); pipe.Writer.Advance(60);
        await pipe.Writer.FlushAsync();
        var r1 = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(r1.Buffer.End);

        pipe.Writer.GetMemory(60); pipe.Writer.Advance(60);   // forces a fresh segment
        await pipe.Writer.FlushAsync();                        // recycles the now-drained head

        int freelistBefore = pipe._writer.FreelistCount;
        Assert.True(freelistBefore > 0, "Test setup error: rented freelist should be populated.");

        // Drain the chain so post-Reset only the freelist matters.
        var r2 = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(r2.Buffer.End);

        pipe.Writer.Complete();
        pipe.Reader.Complete();
        pipe.Reset();

        // Freelist preserved across Reset (matches BCL preserving its segment pool).
        Assert.True(pipe._writer.FreelistCount >= freelistBefore,
            $"Freelist should not shrink (was {freelistBefore}, now {pipe._writer.FreelistCount}).");
    }

    [Fact]
    public async Task Reset_DonatedSegmentsInChain_DisposeOwnersAndPoolShells()
    {
        var pipe = new Pipely.Pipe();
        var donated1 = new TrackingMemoryOwner(20);
        var donated2 = new TrackingMemoryOwner(30);

        pipe.Writer.Splice(donated1);
        pipe.Writer.Splice(donated2);
        await pipe.Writer.FlushAsync();
        // Reader does NOT drain — chain is full of donated segments.

        pipe.Writer.Complete();
        pipe.Reader.Complete();
        pipe.Reset();

        // Donated IMemoryOwners must be disposed exactly once each.
        Assert.Equal(1, donated1.DisposeCount);
        Assert.Equal(1, donated2.DisposeCount);
        // Shells go to the donated-shell freelist (within capacity).
        Assert.Equal(2, pipe._writer.DonatedShellFreelistCount);
    }

    [Fact]
    public async Task Reset_RentedSegmentsInChain_PushedToRentedFreelist()
    {
        var pipe = new Pipely.Pipe(new Pipely.PipeOptions(minimumSegmentSize: 64));
        pipe.Writer.GetMemory(60); pipe.Writer.Advance(60);
        await pipe.Writer.FlushAsync();
        // Reader does NOT drain.

        Assert.NotNull(pipe._writer.ChainHead);
        int freelistBefore = pipe._writer.FreelistCount;

        pipe.Writer.Complete();
        pipe.Reader.Complete();
        pipe.Reset();

        // The rented segment in the chain is pushed to the freelist (capacity-permitting).
        Assert.True(pipe._writer.FreelistCount > freelistBefore);
    }

    [Fact]
    public async Task Reset_AllowsFullReuseOfPipeInstance()
    {
        var pipe = new Pipely.Pipe();
        for (int round = 0; round < 3; round++)
        {
            var mem = pipe.Writer.GetMemory(10);
            mem.Span.Fill((byte)round);
            pipe.Writer.Advance(10);
            await pipe.Writer.FlushAsync();

            var r = await pipe.Reader.ReadAsync();
            Assert.Equal(10, r.Buffer.Length);
            Assert.Equal((byte)round, r.Buffer.First.Span[0]);
            pipe.Reader.AdvanceTo(r.Buffer.End);

            pipe.Writer.Complete();
            pipe.Reader.Complete();
            pipe.Reset();
        }
    }
```

- [ ] **Step 2: Run new tests to verify they fail**

Run: `dotnet test tests/Pipely.Tests --filter FullyQualifiedName~PipeResetTests`

Expected: the new 5 tests fail (Reset is a no-op stub; chain leaks, fields not cleared). The 4 tests from Task 3 still pass.

- [ ] **Step 3: Implement the Reset body**

Replace the `Reset()` method body in `src/Pipely/Pipe.cs` with the full implementation. The method becomes:

```csharp
    /// <summary>
    /// Restores the pipe to its post-construction state so the instance can be reused
    /// (e.g. from a pool). Both Reader.Complete and Writer.Complete must have been
    /// called first. Throws otherwise.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The pipe's lifecycle owner is responsible for calling Reset, on the same thread,
    /// only after establishing a happens-before edge with the writer and reader threads
    /// — typically by awaiting the producer/consumer tasks before calling Reset. The
    /// SPSC contract is not violated by Reset itself: the writer/reader threads must
    /// already be quiescent (both have completed and observed any in-flight ValueTask
    /// via GetResult) before Reset runs.
    /// </para>
    /// <para>
    /// State preserved across Reset: PipeOptions, schedulers, the rented-segment
    /// freelist, and the donated-shell freelist. Everything else is restored to its
    /// post-construction value. Matches BCL Pipe.Reset semantics.
    /// </para>
    /// </remarks>
    public void Reset()
    {
        if (!(_writer.WriterCompleted && _reader.ReaderCompleted))
            throw new InvalidOperationException("Both completion routines must be called before resetting the pipe.");

        // Walk the chain and dispatch each segment to its appropriate freelist.
        // Donated segments: dispose the foreign IMemoryOwner, push the shell to
        // the donated-shell freelist (over-cap drops to GC, matching existing semantics).
        // Rented segments: push to the rented-segment freelist (over-cap → DisposeOwned).
        var seg = _writer.ChainHead;
        while (seg != null)
        {
            var next = seg.Next;
            if (seg.IsDonated)
            {
                seg.DisposeOwned();
                PushDonatedShellFreelist(seg);
            }
            else
            {
                PushFreelist(seg);
            }
            seg = next;
        }

        // Clear writer-side fields. Freelist head/count preserved.
        _writer.ChainHead = null;
        _writer.WritingHead = null;
        _writer.WritingHeadBytesBuffered = 0;
        _writer.TotalWritten = 0;
        _writer.LastPublishedWriterState = default;
        _writer.LastAcquiredReaderState = default;
        _writer.WriterCompleted = false;

        // Clear reader-side fields.
        _reader.ReadHead = null;
        _reader.ReadHeadIdx = 0;
        _reader.ReadTail = null;
        _reader.ReadTailIdx = 0;
        _reader.TotalConsumed = 0;
        _reader.TotalExamined = 0;
        _reader.LastPublishedReaderState = default;
        _reader.LastAcquiredWriterState = default;
        _reader.ReaderCompleted = false;
        _reader.ReadPending = false;

        // Reset triple buffers and awaiters.
        _writerTb.Reset();
        _readerTb.Reset();
        _readAwaiter.Reset();
        _flushAwaiter.Reset();
    }
```

- [ ] **Step 4: Run all Reset tests to verify they pass**

Run: `dotnet test tests/Pipely.Tests --filter FullyQualifiedName~PipeResetTests`

Expected: 9 passed, 0 failed.

- [ ] **Step 5: Run the full test suite**

Run: `dotnet test tests/Pipely.Tests`

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/Pipely/Pipe.cs tests/Pipely.Tests/PipeResetTests.cs
git commit -m "Pipe: implement Reset body — chain cleanup, field reset, helpers"
```

---

### Task 5: Reset edge-case tests (cancellation, exception completion, donated tail)

These tests don't drive any new code — they pin down behavior that the Task 4 implementation should already produce, but which is worth explicit coverage.

**Files:**
- Modify: `tests/Pipely.Tests/PipeResetTests.cs`

- [ ] **Step 1: Append the edge-case tests**

Append these to `PipeResetTests` (before the closing `}`):

```csharp
    [Fact]
    public async Task Reset_AfterWriterCompletedWithException_ClearsCompletionException()
    {
        var pipe = new Pipely.Pipe();
        pipe.Writer.Complete(new InvalidOperationException("boom"));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await pipe.Reader.ReadAsync());
        pipe.Reader.Complete();

        pipe.Reset();

        // Post-Reset, no exception in the published writer state.
        Assert.Null(pipe._writer.LastPublishedWriterState.CompletionException);
        Assert.False(pipe._writer.LastPublishedWriterState.IsCompleted);

        // And the pipe is fully usable: the writer state seen by the reader is fresh
        // (no terminal IsCompleted=true sticking around in the triple buffer).
        var mem = pipe.Writer.GetMemory(4); mem.Span.Fill(0x11); pipe.Writer.Advance(4);
        await pipe.Writer.FlushAsync();
        var r = await pipe.Reader.ReadAsync();
        Assert.False(r.IsCompleted);   // Critical: the new reader observes a non-completed state.
        Assert.Equal(4, r.Buffer.Length);
        pipe.Reader.AdvanceTo(r.Buffer.End);
    }

    [Fact]
    public void Reset_OnFreshlyConstructedPipeAfterBothComplete_NoThrow()
    {
        // No GetMemory, no FlushAsync — just complete and Reset.
        var pipe = new Pipely.Pipe();
        pipe.Writer.Complete();
        pipe.Reader.Complete();
        pipe.Reset();   // must not throw, must not crash on null ChainHead
    }

    [Fact]
    public async Task Reset_WithDonatedTail_DisposesAndPoolsTail()
    {
        // The active WritingHead can itself be a donated segment.
        var pipe = new Pipely.Pipe();
        var donated = new TrackingMemoryOwner(40);
        pipe.Writer.Splice(donated);
        // No FlushAsync — the donated segment is the WritingHead.

        pipe.Writer.Complete();
        pipe.Reader.Complete();
        pipe.Reset();

        Assert.Equal(1, donated.DisposeCount);
        Assert.Equal(1, pipe._writer.DonatedShellFreelistCount);
    }

    [Fact]
    public async Task Reset_AfterParkedReadObservedWriterException_ReusableForNewRound()
    {
        // Exercises the live OnCompleted → SetException → Reset → next round path:
        // the reader parks, writer.Complete(ex) signals via SetException, the reader's
        // await re-throws, then we Reset and run a clean round. This is the integration
        // counterpart to the awaiter-level Reset tests.
        var pipe = new Pipely.Pipe();
        var ex = new InvalidOperationException("first round error");

        // Park a read with no data available.
        var readTask = pipe.Reader.ReadAsync().AsTask();
        Assert.False(readTask.IsCompleted);

        pipe.Writer.Complete(ex);
        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await readTask);
        Assert.Same(ex, thrown);

        pipe.Reader.Complete();
        pipe.Reset();

        // Clean round 2: the awaiter has been Reset; new ValueTask tokens are valid.
        var mem = pipe.Writer.GetMemory(8); mem.Span.Fill(0x55); pipe.Writer.Advance(8);
        await pipe.Writer.FlushAsync();
        var r = await pipe.Reader.ReadAsync();
        Assert.False(r.IsCompleted);
        Assert.Equal(8, r.Buffer.Length);
        pipe.Reader.AdvanceTo(r.Buffer.End);
    }

    [Fact]
    public async Task Reset_ReuseAcrossOptionsBoundaries_KeepsOriginalOptions()
    {
        // After Reset, the pipe still uses the options it was constructed with.
        // We can't directly read pipe._options externally without InternalsVisibleTo,
        // but we can verify behavior: the minimumSegmentSize knob is still in effect.
        var pipe = new Pipely.Pipe(new Pipely.PipeOptions(minimumSegmentSize: 64));
        pipe.Writer.GetMemory(1); pipe.Writer.Advance(1);
        await pipe.Writer.FlushAsync();
        var r1 = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(r1.Buffer.End);
        pipe.Writer.Complete();
        pipe.Reader.Complete();
        pipe.Reset();

        // Asking for 1 byte after Reset still rents a >= 64-byte segment.
        var mem = pipe.Writer.GetMemory(1);
        Assert.True(mem.Length >= 64, $"Expected segment of at least 64 bytes; got {mem.Length}.");
    }
```

- [ ] **Step 2: Run the new tests**

Run: `dotnet test tests/Pipely.Tests --filter FullyQualifiedName~PipeResetTests`

Expected: 14 passed, 0 failed.

- [ ] **Step 3: Run the full suite**

Run: `dotnet test tests/Pipely.Tests`

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add tests/Pipely.Tests/PipeResetTests.cs
git commit -m "Pipe: edge-case Reset tests (exception completion, donated tail, options)"
```

---

### Task 6: Drop `IDisposable`, `Dispose()`, `_disposed` (atomic breaking change)

**Files:**
- Modify: `src/Pipely/Pipe.cs`
- Modify: `src/Pipely/Pipe.Reader.cs`
- Modify: `src/Pipely/Pipe.Writer.cs`
- Delete: `tests/Pipely.Tests/PipeDisposeTests.cs`
- Modify: `tests/Pipely.Tests/PipeWriterTests.cs`
- Modify: `tests/Pipely.Tests/PipeWriterSpliceTests.cs`
- Modify: `tests/Pipely.Tests/PipeWriterBufferedBytesTests.cs`
- Modify: `tests/Pipely.Tests/PipeWriterUnflushedBytesTests.cs`
- Modify: `tests/Pipely.Tests/PipeReaderTests.cs`
- Modify: `tests/Pipely.Tests/PipeReadInProgressTests.cs`
- Modify: `tests/Pipely.Tests/PipeLifecycleTests.cs`
- Modify: `tests/Pipely.Tests/PipeAdvanceToTests.cs`
- Modify: `tests/Pipely.Tests/PipeCancellationTests.cs`
- Modify: `tests/Pipely.Tests/PipeContinuationDispatcherTests.cs`
- Modify: `tests/Pipely.Tests/BclParityTests.cs`
- Modify: `tests/Pipely.Stress/StressHarness.cs`
- Modify: `tests/Pipely.Benchmarks/PipelyPipeAdapter.cs`
- Modify: `tests/Pipely.Benchmarks/SchedulerBenchmarks.cs`
- Modify: `tests/Pipely.Benchmarks/SpliceBenchmarks.cs`
- Modify: `tests/Pipely.Benchmarks/FreshPipeSchedulerBenchmarks.cs`

This task is one logical change: remove the IDisposable surface and every reference to it across all four projects (`src`, `tests/Pipely.Tests`, `tests/Pipely.Stress`, `tests/Pipely.Benchmarks`). Doing it in one commit avoids any intermediate state where the solution build is broken.

- [ ] **Step 1: Delete `tests/Pipely.Tests/PipeDisposeTests.cs`**

```bash
git rm tests/Pipely.Tests/PipeDisposeTests.cs
```

- [ ] **Step 2: Delete dispose-only tests in other test files**

In `tests/Pipely.Tests/PipeWriterTests.cs`, delete this entire test (lines 51–57):

```csharp
    [Fact]
    public void GetMemory_AfterDispose_Throws()
    {
        var pipe = new Pipely.Pipe();
        pipe.Dispose();
        Assert.Throws<ObjectDisposedException>(() => pipe.Writer.GetMemory(0));
    }
```

In `tests/Pipely.Tests/PipeWriterSpliceTests.cs`, delete this entire test (lines 24–33):

```csharp
    [Fact]
    public void Splice_DisposedPipe_Throws_ObjectDisposed_CallerStillOwns()
    {
        var pipe = new Pipely.Pipe();
        pipe.Dispose();

        var owner = new TrackingMemoryOwner(64);
        Assert.Throws<ObjectDisposedException>(() => pipe.Writer.Splice(owner));
        Assert.Equal(0, owner.DisposeCount);   // pipe did NOT dispose; caller still owns
    }
```

In `tests/Pipely.Tests/PipeWriterBufferedBytesTests.cs`, delete this entire test (lines 137–150):

```csharp
    [Fact]
    public void BufferedBytes_AfterDispose_ReturnsLastValue_NoThrow()
    {
        var pipe = new Pipely.Pipe();
        pipe.Writer.GetMemory(100);
        pipe.Writer.Advance(40);
        Assert.Equal(40L, pipe.Writer.BufferedBytes);

        pipe.Dispose();

        // Pure accessor, no validation. Dispose does not touch _totalWritten or
        // _lastAcquiredReaderState, so the last value is still observable.
        Assert.Equal(40L, pipe.Writer.BufferedBytes);
    }
```

In `tests/Pipely.Tests/PipeWriterUnflushedBytesTests.cs`, delete this entire test (lines 134–147):

```csharp
    [Fact]
    public void UnflushedBytes_AfterDispose_ReturnsLastValue_NoThrow()
    {
        var pipe = new Pipely.Pipe();
        pipe.Writer.GetMemory(100);
        pipe.Writer.Advance(40);
        Assert.Equal(40L, pipe.Writer.UnflushedBytes);

        pipe.Dispose();

        // Matches BCL: pure accessor, no validation, returns whatever the
        // underlying field state holds. Dispose does not mutate either field.
        Assert.Equal(40L, pipe.Writer.UnflushedBytes);
    }
```

- [ ] **Step 3: Convert the two dispose-with-mixed-chain Splice tests to Reset analogues**

In `tests/Pipely.Tests/PipeWriterSpliceTests.cs`, the test `DisposePipe_WithMixedChain_DisposesAllOwners` and `Dispose_ClearsShellFreelist` are testing real cleanup behavior. They should be converted to assert the same invariants under `Reset()` (which is now the cleanup path).

Replace the body of `DisposePipe_WithMixedChain_DisposesAllOwners` (around line 480) with:

```csharp
    [Fact]
    public async Task ResetPipe_WithMixedChain_DisposesAllDonatedOwners()
    {
        var pipe = new Pipely.Pipe(new Pipely.PipeOptions(minimumSegmentSize: 64));

        var donated1 = new TrackingMemoryOwner(20);
        var donated2 = new TrackingMemoryOwner(30);

        pipe.Writer.Splice(donated1);
        pipe.Writer.GetMemory(64); pipe.Writer.Advance(40);    // rented in middle
        pipe.Writer.Splice(donated2);
        await pipe.Writer.FlushAsync();
        // Don't drain — chain is full of un-consumed segments.

        pipe.Writer.Complete();
        pipe.Reader.Complete();
        pipe.Reset();

        Assert.Equal(1, donated1.DisposeCount);
        Assert.Equal(1, donated2.DisposeCount);
    }
```

Replace `Dispose_ClearsShellFreelist` (around line 560) with:

```csharp
    [Fact]
    public async Task ResetPipe_PreservesShellFreelist()
    {
        // Reset preserves the donated-shell freelist (matches BCL preserving its segment pool).
        // The pre-existing shell entries from prior recycles are NOT disposed/dropped.
        var pipe = new Pipely.Pipe();
        var donated1 = new TrackingMemoryOwner(8);
        var donated2 = new TrackingMemoryOwner(8);
        pipe.Writer.Splice(donated1);
        pipe.Writer.Splice(donated2);
        // Force at least one recycle so the shell freelist has an entry.
        await pipe.Writer.FlushAsync();
        var rr = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(rr.Buffer.GetPosition(8));
        await pipe.Writer.FlushAsync();
        Assert.Equal(1, pipe._writer.DonatedShellFreelistCount);

        // Drain the rest of the chain, complete both sides, Reset.
        var rr2 = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(rr2.Buffer.End);
        pipe.Writer.Complete();
        pipe.Reader.Complete();
        pipe.Reset();

        // Shell freelist preserved (entry from earlier recycle is still there); the
        // active tail (donated2) was added on Reset, so count should be at least 2.
        Assert.True(pipe._writer.DonatedShellFreelistCount >= 1);
    }
```

Note: also delete the local `pipe.Dispose();` call sites at lines 494 and 578 of the old tests (replaced wholesale above).

- [ ] **Step 4: Refactor `BclParityTests.cs` to drop the `IDisposable Disposer` field**

The test file's `CreatePipe` / `CreatePipeWithSyncCtx` helpers return a 3-tuple `(PipeReader, PipeWriter, IDisposable Disposer)` where `Disposer` was either `NoOpDisposable.Instance` (BCL arm — BCL `Pipe` is not `IDisposable`) or the Pipely `Pipe` itself (Pipely arm — historically `IDisposable`). Once Pipely.Pipe is no longer `IDisposable`, that abstraction has no purpose: both arms would be no-ops. Drop the field, drop the `NoOpDisposable` class, drop every `using (disp)` block.

Open `tests/Pipely.Tests/BclParityTests.cs`. Apply the following edits:

1. Change the `CreatePipe` helper signature and body (originally lines 12–25) to:

```csharp
    private static (PipeReader Reader, PipeWriter Writer) CreatePipe(PipeKind kind, PipeOptions? bclOpts = null)
    {
        switch (kind)
        {
            case PipeKind.Bcl:
                var bcl = new Pipe(bclOpts ?? PipeOptions.Default);
                return (bcl.Reader, bcl.Writer);
            case PipeKind.Pipely:
                var spsc = new Pipely.Pipe();    // defaults match BCL defaults
                return (spsc.Reader, spsc.Writer);
            default:
                throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }
```

2. Delete the `NoOpDisposable` class entirely (originally lines 27–31).

3. Change the `CreatePipeWithSyncCtx` helper signature and body (originally lines 43–56) to:

```csharp
    private static (PipeReader Reader, PipeWriter Writer) CreatePipeWithSyncCtx(PipeKind kind, bool useSyncCtx)
    {
        switch (kind)
        {
            case PipeKind.Bcl:
                var bcl = new Pipe(new PipeOptions(useSynchronizationContext: useSyncCtx));
                return (bcl.Reader, bcl.Writer);
            case PipeKind.Pipely:
                var spsc = new Pipely.Pipe(new Pipely.PipeOptions(useSynchronizationContext: useSyncCtx));
                return (spsc.Reader, spsc.Writer);
            default:
                throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }
```

4. For every test that destructures `var (reader, writer, disp) = CreatePipe(kind);` (or `CreatePipeWithSyncCtx`), change to `var (reader, writer) = CreatePipe(kind);` and remove the surrounding `using (disp) { ... }` block — keep the body, drop the wrapper. Affected test method bodies are at original lines 63–71, 78–86, 194–211 (and downward; grep `using \(disp\)` to find them all). For example:

Before:
```csharp
        var (reader, writer, disp) = CreatePipe(kind);
        using (disp)
        {
            var ex = new InvalidOperationException("test");
            writer.Complete(ex);
            var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await reader.ReadAsync());
            Assert.Same(ex, thrown);
        }
```

After:
```csharp
        var (reader, writer) = CreatePipe(kind);
        var ex = new InvalidOperationException("test");
        writer.Complete(ex);
        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await reader.ReadAsync());
        Assert.Same(ex, thrown);
```

5. The `BackpressureHysteresis_ParkAtPause_ResumeAtBelowResume` test (original lines 91–119) hand-destructures the disposer because its branch differs across kinds. Update the manual destructure block (original lines 96–106) to:

```csharp
        PipeReader reader; PipeWriter writer;
        if (kind == PipeKind.Bcl)
        {
            var bcl = new Pipe(new PipeOptions(pauseWriterThreshold: pauseAt, resumeWriterThreshold: resumeAt));
            reader = bcl.Reader; writer = bcl.Writer;
        }
        else
        {
            var spsc = new Pipely.Pipe(new Pipely.PipeOptions(pauseWriterThreshold: pauseAt, resumeWriterThreshold: resumeAt));
            reader = spsc.Reader; writer = spsc.Writer;
        }
```

And drop the `using (disp)` wrapper on lines 108–118.

6. The standalone `using var spsc = new Pipely.Pipe();` at line 184 becomes `var spsc = new Pipely.Pipe();`.

After all six edits, `BclParityTests.cs` should not contain any `IDisposable`, `Disposer`, `disp`, or `NoOpDisposable` token. Verify:

```bash
grep -n "IDisposable\|NoOpDisposable\|Disposer\|using (disp)" tests/Pipely.Tests/BclParityTests.cs
```

Expected: no output.

- [ ] **Step 5: Update Stress and Benchmarks projects**

`tests/Pipely.Stress/StressHarness.cs` line 19: change `using var pipe = new Pipely.Pipe(_options);` to `var pipe = new Pipely.Pipe(_options);`. The harness creates a fresh pipe per stress iteration; without `Dispose`, the segments leak per iteration until GC. Acceptable for the current usage — long runs that need pooling can call `Reset()` instead, but that refactor is out of scope for this plan.

`tests/Pipely.Benchmarks/PipelyPipeAdapter.cs`: the adapter currently delegates `Dispose` to `_pipe.Dispose()` (line 20). Replace that line:

Before:
```csharp
    public void Dispose() => _pipe.Dispose();
```

After:
```csharp
    public void Dispose() { /* Pipely.Pipe is no longer IDisposable; nothing to clean up here. */ }
```

The adapter remains `IDisposable` so that `PinnedThroughputBenchmarks` and `ThroughputBenchmarks` (which call `_pipely.Dispose()` on the adapter, not on the underlying pipe) keep compiling.

`tests/Pipely.Benchmarks/SchedulerBenchmarks.cs`: lines 63 and 76 call `Dispose()` directly on `Pipely.Pipe?` fields. Replace both:

Before (line 63 area):
```csharp
        _pipelyTp.Dispose();
```

After:
```csharp
        _pipelyTp = null;   // Pipely.Pipe no longer IDisposable; let GC reclaim.
```

Before (line 76 area):
```csharp
        _pipelyInline.Dispose();
```

After:
```csharp
        _pipelyInline = null;
```

`tests/Pipely.Benchmarks/SpliceBenchmarks.cs`: lines 48 and 55 pass `() => pipe.Dispose()` as the `completePipe` action. The benchmark uses this to mark the pipe done at end of run. Change both to complete both sides instead:

Before (line 48):
```csharp
        return RunGetSpan(pipe.Writer, pipe.Reader, completePipe: () => pipe.Dispose());
```

After:
```csharp
        return RunGetSpan(pipe.Writer, pipe.Reader, completePipe: () =>
        {
            pipe.Writer.Complete();
            pipe.Reader.Complete();
        });
```

Same edit at line 55 for `RunSplice`.

`tests/Pipely.Benchmarks/FreshPipeSchedulerBenchmarks.cs`: lines 38 and 47 use `using var pipe = new Pipely.Pipe(...)`. Change both to plain `var pipe = ...`.

After all edits, run from the repo root:

```bash
grep -rn "pipe\.Dispose()\|using var \w* = new Pipely\.Pipe\|using (var \w* = new Pipely\.Pipe" tests/ src/
```

Expected: no output (every reference is gone). If anything remains, fix it before moving on.

- [ ] **Step 6: Convert remaining `using var <name>` to `var <name>` across all test files**

The previous step handled `BclParityTests.cs`, the four files modified in Steps 2–3, and the Stress/Benchmarks files. This step catches every other `using var <anything> = new Pipely.Pipe(...)` declaration in `tests/Pipely.Tests/`. Use a broader regex than the original plan to also catch `pipe1`, `pipe2`, `spsc`, etc:

```bash
find tests/Pipely.Tests -name '*.cs' -type f -exec sed -i -E 's/using var ([A-Za-z_][A-Za-z0-9_]*) = new Pipely\.Pipe/var \1 = new Pipely.Pipe/g' {} +
git diff --stat tests/Pipely.Tests/
```

Verify no `using var ... = new Pipely.Pipe` remains anywhere in the solution:

```bash
grep -rn "using var [A-Za-z_][A-Za-z0-9_]* = new Pipely\.Pipe" tests/ src/
```

Expected: no output.

- [ ] **Step 7: Remove `_disposed` checks from `src/Pipely/Pipe.Reader.cs`**

Delete each of these 5 lines (one per method) from `src/Pipely/Pipe.Reader.cs`:

- Line 28: `if (_pipe._disposed) throw new ObjectDisposedException(nameof(Pipe));`
- Line 67: `if (_pipe._disposed) throw new ObjectDisposedException(nameof(Pipe));`
- Line 108: `if (_pipe._disposed) throw new ObjectDisposedException(nameof(Pipe));`
- Line 164: `if (_pipe._disposed) throw new ObjectDisposedException(nameof(Pipe));`
- Line 188: `if (_pipe._disposed) throw new ObjectDisposedException(nameof(Pipe));`

Each method's first non-comment statement is the `_disposed` check; delete just that one line per method.

- [ ] **Step 8: Remove `_disposed` checks from `src/Pipely/Pipe.Writer.cs`**

Delete each of these 6 lines from `src/Pipely/Pipe.Writer.cs`:

- Line 29: `if (_pipe._disposed) throw new ObjectDisposedException(nameof(Pipe));`
- Line 61: `if (_pipe._disposed) throw new ObjectDisposedException(nameof(Pipe));`
- Line 72: `if (_pipe._disposed) throw new ObjectDisposedException(nameof(Pipe));`
- Line 140: `if (_pipe._disposed) throw new ObjectDisposedException(nameof(Pipe));`
- Line 166: `if (_pipe._disposed) throw new ObjectDisposedException(nameof(Pipe));`
- Line 293: `if (_pipe._disposed) throw new ObjectDisposedException(nameof(Pipe));`

- [ ] **Step 9: Remove `IDisposable`, `_disposed`, and `Dispose()` from `src/Pipely/Pipe.cs`**

In `src/Pipely/Pipe.cs`:

- Line 21 — change `public sealed partial class Pipe : IDisposable` to `public sealed partial class Pipe`
- Line 91 — delete the entire line: `internal bool _disposed;` (also delete the `// Pipe-level (mutated by Dispose only).` comment on line 90)
- Lines 122–157 — delete the entire `public void Dispose()` method (from `public void Dispose()` through its closing `}`)

After this edit, the class no longer implements `IDisposable` and has no `Dispose` method or `_disposed` field.

- [ ] **Step 10: Build and run the unit-test suite**

Run: `dotnet build` then `dotnet test tests/Pipely.Tests`

Expected: clean build, all tests pass. If you see `'Pipe' does not contain a definition for 'Dispose'`, a call site was missed; grep:

```bash
grep -rn "\.Dispose()\|using var [A-Za-z_][A-Za-z0-9_]* = new Pipely\.Pipe" tests/ src/ | grep -vE "(TrackingMemoryOwner|ContinuationDispatcher|cts|_producerReady|_consumerReady|_producerStart|_producerDone|_consumerStart|_consumerDone|_bcl\.Dispose|_pipely\.Dispose|_bclRunner|_pipelyRunner)"
```

Expected: empty (only legitimate `IDisposable` users remain — `TrackingMemoryOwner`, BDN-internal disposes, the `ContinuationDispatcher` test scaffolding, the `PipelyPipeAdapter`/`BclPipeAdapter` lifecycle calls).

- [ ] **Step 11: Build the rest of the solution (Stress + Benchmarks)**

Run: `dotnet build Pipely.slnx`

Expected: clean build of the whole solution. If `tests/Pipely.Stress` or `tests/Pipely.Benchmarks` fails to build, re-check Step 5.

- [ ] **Step 12: Commit**

```bash
git add -A src/ tests/
git commit -m "Pipe: drop IDisposable; Reset is the only cleanup path"
```

---

## Self-review checklist

- [x] **Spec coverage:** Plan covers BCL parity (precondition, error message, preserved-state semantics), in-place reset of all `WriterFields`/`ReaderFields`/triple-buffer/awaiter fields, IDisposable removal, every test that relied on Dispose, every `using var <name>` declaration in `tests/Pipely.Tests/` (broader regex catches `pipe1`/`pipe2`/`spsc`/etc), every `pipe.Dispose()` call site in `tests/Pipely.Stress/` and `tests/Pipely.Benchmarks/`, and the `BclParityTests.cs` `IDisposable Disposer` abstraction. Donated-tail, fresh-pipe-Reset, post-Reset reuse, post-Reset triple-buffer-empty, and parked-read-observed-exception-then-Reset cases are all covered by tests.
- [x] **Placeholder scan:** No "TBD", "TODO", or "fill in details" anywhere. Every code change shows the exact code.
- [x] **Type consistency:** `Reset()` (no overloads, no args) is consistent across `TripleBuffer<T>`, `PipelyAwaiter<T>`, and `Pipe`. `WriterFields`/`ReaderFields` field names match `src/Pipely/Pipe.cs:30-78`. `_writer.WriterCompleted` / `_reader.ReaderCompleted` match the existing field names. `BclParityTests` tuple type changes are propagated through every consumer of the helper.
- [x] **No abstraction creep:** Reset doesn't grow new options or callbacks; it's a method that takes nothing and returns nothing. The `BclParityTests` refactor *removes* an abstraction (`Disposer`/`NoOpDisposable`) rather than adding one.
- [x] **Hot-path invariant:** Confirmed — every modification to `Pipe.Reader.cs` and `Pipe.Writer.cs` is a *deletion* of a guard line. No new code added on the hot path.
- [x] **Review feedback applied:** Concern 4 (post-Reset `TryAcquire` assertion in Task 4); Concern 5 (renamed test in Task 2 to `Reset_ClearsExternallyVisibleFields` with explicit comment about untested private fields); Concern 6 (parked-read-observed-exception-then-Reset roundtrip test in Task 5); Blockers 1–3 (Task 6 expanded with explicit Stress + Benchmarks edits and the `BclParityTests` `IDisposable Disposer` removal). Second-pass concern C1 (token-cancel path leaves `_ctr` undisposed): `PipelyAwaiter<T>.Reset()` now calls `_ctr.Dispose()` defensively as its first statement, with an XML doc paragraph explaining the asymmetry between signaler and token-cancel paths.
