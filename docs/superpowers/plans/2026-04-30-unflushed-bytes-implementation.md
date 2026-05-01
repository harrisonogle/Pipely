# UnflushedBytes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Override `PipeWriter.CanGetUnflushedBytes` and `PipeWriter.UnflushedBytes` on `Pipely.PipeWriter` to match the BCL contract bit-for-bit.

**Architecture:** Two property overrides, both pure expression-bodied accessors. `CanGetUnflushedBytes` returns the constant `true`. `UnflushedBytes` returns `_pipe._totalWritten - _pipe._lastPublishedWriterState.TotalWritten` — Pipely already maintains both longs (`_totalWritten` is bumped by `Advance` and `Splice`; `_lastPublishedWriterState.TotalWritten` is unconditionally written to the current `_totalWritten` snapshot in both `FlushAsync` and `Complete`). No new state, no synchronization, no validation — exactly matching BCL's `DefaultPipeWriter`.

**Tech Stack:** C#, .NET 10, xunit 2.x, `System.IO.Pipelines`.

---

## File Structure

- **Modify:** `src/Pipely/Pipe.Writer.cs` — add two property overrides on the `Pipely.PipeWriter` class, just after `CancelPendingFlush` and before the private `ParkFlushAwaiter` helper.
- **Create:** `tests/Pipely.Tests/PipeWriterUnflushedBytesTests.cs` — focused tests for the two new properties across all writer-side lifecycle transitions.
- **Modify:** `tests/Pipely.Tests/BclParityTests.cs` — add a `[Theory]` that runs the same Advance/Flush/Complete script through both BCL and Pipely and asserts `UnflushedBytes` matches at each checkpoint.

No changes to `Pipe.cs`, `Pipe.Reader.cs`, `WriterState.cs`, `BufferSegment.cs`, or any options file.

---

## Task 1: Add `CanGetUnflushedBytes` override

**Files:**
- Modify: `src/Pipely/Pipe.Writer.cs:154` (insert after `CancelPendingFlush`)
- Create: `tests/Pipely.Tests/PipeWriterUnflushedBytesTests.cs`

- [ ] **Step 1: Write the failing test**

Create `tests/Pipely.Tests/PipeWriterUnflushedBytesTests.cs`:

```csharp
using Xunit;

namespace PipelyTests;

public class PipeWriterUnflushedBytesTests
{
    [Fact]
    public void CanGetUnflushedBytes_IsTrue()
    {
        using var pipe = new Pipely.Pipe();
        Assert.True(pipe.Writer.CanGetUnflushedBytes);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `dotnet test tests/Pipely.Tests --filter "FullyQualifiedName~PipeWriterUnflushedBytesTests.CanGetUnflushedBytes_IsTrue"`

Expected: FAIL. `Pipely.PipeWriter` does not override `CanGetUnflushedBytes`, so the abstract base default of `false` is returned and `Assert.True` fails.

- [ ] **Step 3: Add the override**

In `src/Pipely/Pipe.Writer.cs`, insert immediately after the closing `}` of `CancelPendingFlush` (currently at line 154) and before the `private ValueTask<FlushResult> ParkFlushAwaiter(...)` helper:

```csharp
    // ---------- UnflushedBytes (BCL parity) ----------
    // See docs/superpowers/plans/2026-04-30-unflushed-bytes-implementation.md.
    // Mirrors System.IO.Pipelines.Pipe.DefaultPipeWriter: pure accessors, no
    // validation, no synchronization. Writer-thread-only by SPSC contract.

    public override bool CanGetUnflushedBytes => true;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `dotnet test tests/Pipely.Tests --filter "FullyQualifiedName~PipeWriterUnflushedBytesTests.CanGetUnflushedBytes_IsTrue"`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/Pipely/Pipe.Writer.cs tests/Pipely.Tests/PipeWriterUnflushedBytesTests.cs
git commit -m "PipeWriter: override CanGetUnflushedBytes to true"
```

---

## Task 2: Add `UnflushedBytes` override (TDD-driven by Advance scenario)

**Files:**
- Modify: `src/Pipely/Pipe.Writer.cs` (add `UnflushedBytes` property next to `CanGetUnflushedBytes`)
- Modify: `tests/Pipely.Tests/PipeWriterUnflushedBytesTests.cs` (add Advance test)

- [ ] **Step 1: Write the failing test**

Append to `tests/Pipely.Tests/PipeWriterUnflushedBytesTests.cs` inside the existing class:

```csharp
    [Fact]
    public void UnflushedBytes_ZeroOnFreshPipe()
    {
        using var pipe = new Pipely.Pipe();
        Assert.Equal(0L, pipe.Writer.UnflushedBytes);
    }

    [Fact]
    public void UnflushedBytes_ReflectsAdvanceCount()
    {
        using var pipe = new Pipely.Pipe();
        pipe.Writer.GetMemory(100);
        pipe.Writer.Advance(40);
        Assert.Equal(40L, pipe.Writer.UnflushedBytes);

        pipe.Writer.Advance(10);
        Assert.Equal(50L, pipe.Writer.UnflushedBytes);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `dotnet test tests/Pipely.Tests --filter "FullyQualifiedName~PipeWriterUnflushedBytesTests.UnflushedBytes_"`

Expected: FAIL. The abstract base's `UnflushedBytes` throws `NotImplementedException`, so even the `_ZeroOnFreshPipe` test fails with an exception (not an assertion failure).

- [ ] **Step 3: Add the override**

In `src/Pipely/Pipe.Writer.cs`, immediately below the `CanGetUnflushedBytes` line added in Task 1:

```csharp
    public override long UnflushedBytes
        => _pipe._totalWritten - _pipe._lastPublishedWriterState.TotalWritten;
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test tests/Pipely.Tests --filter "FullyQualifiedName~PipeWriterUnflushedBytesTests.UnflushedBytes_"`

Expected: PASS for both `_ZeroOnFreshPipe` and `_ReflectsAdvanceCount`.

- [ ] **Step 5: Commit**

```bash
git add src/Pipely/Pipe.Writer.cs tests/Pipely.Tests/PipeWriterUnflushedBytesTests.cs
git commit -m "PipeWriter: override UnflushedBytes (computed from snapshot delta)"
```

---

## Task 3: Lifecycle coverage — FlushAsync and Complete reset to zero

**Files:**
- Modify: `tests/Pipely.Tests/PipeWriterUnflushedBytesTests.cs`

These tests validate that the existing `FlushAsync` and `Complete` snapshot-publish paths correctly drive `UnflushedBytes` to zero. They are post-implementation regression coverage — they exercise the contract on already-working code.

- [ ] **Step 1: Write the FlushAsync test**

Append to `PipeWriterUnflushedBytesTests`:

```csharp
    [Fact]
    public async Task UnflushedBytes_ZeroAfterFlushAsync()
    {
        using var pipe = new Pipely.Pipe();
        pipe.Writer.GetMemory(100);
        pipe.Writer.Advance(40);
        Assert.Equal(40L, pipe.Writer.UnflushedBytes);

        await pipe.Writer.FlushAsync();
        Assert.Equal(0L, pipe.Writer.UnflushedBytes);
    }

    [Fact]
    public async Task UnflushedBytes_ResumesAfterPostFlushAdvance()
    {
        using var pipe = new Pipely.Pipe();
        pipe.Writer.GetMemory(100);
        pipe.Writer.Advance(40);
        await pipe.Writer.FlushAsync();
        Assert.Equal(0L, pipe.Writer.UnflushedBytes);

        pipe.Writer.GetMemory(100);
        pipe.Writer.Advance(15);
        Assert.Equal(15L, pipe.Writer.UnflushedBytes);
    }
```

- [ ] **Step 2: Run the FlushAsync tests**

Run: `dotnet test tests/Pipely.Tests --filter "FullyQualifiedName~PipeWriterUnflushedBytesTests"`

Expected: PASS for both new tests (and the previous tests still pass).

- [ ] **Step 3: Write the Complete tests**

Append to `PipeWriterUnflushedBytesTests`:

```csharp
    [Fact]
    public void UnflushedBytes_ZeroAfterCompleteWithoutFlush()
    {
        using var pipe = new Pipely.Pipe();
        pipe.Writer.GetMemory(100);
        pipe.Writer.Advance(40);
        Assert.Equal(40L, pipe.Writer.UnflushedBytes);

        pipe.Writer.Complete();
        // BCL parity: CompleteWriter's snapshot publish (analogous to BCL's
        // CommitUnsynchronized) drives the count to zero even without a Flush.
        Assert.Equal(0L, pipe.Writer.UnflushedBytes);
    }

    [Fact]
    public void UnflushedBytes_ZeroAfterCompleteWithException()
    {
        using var pipe = new Pipely.Pipe();
        pipe.Writer.GetMemory(100);
        pipe.Writer.Advance(40);

        pipe.Writer.Complete(new InvalidOperationException("boom"));
        Assert.Equal(0L, pipe.Writer.UnflushedBytes);
    }
```

- [ ] **Step 4: Run the Complete tests**

Run: `dotnet test tests/Pipely.Tests --filter "FullyQualifiedName~PipeWriterUnflushedBytesTests"`

Expected: PASS for all tests.

- [ ] **Step 5: Commit**

```bash
git add tests/Pipely.Tests/PipeWriterUnflushedBytesTests.cs
git commit -m "PipeWriter: cover UnflushedBytes Flush/Complete reset to zero"
```

---

## Task 4: Lifecycle coverage — Splice contributes to UnflushedBytes

**Files:**
- Modify: `tests/Pipely.Tests/PipeWriterUnflushedBytesTests.cs`

`Splice` bumps `_totalWritten` (see `Pipe.Writer.cs:274` for the bootstrap path and `:291` for the steady-state path), so it must show up in `UnflushedBytes` until the next Flush/Complete. These tests validate that.

- [ ] **Step 1: Add the Splice helper using directive at the top of the file**

If not already present at the top of `tests/Pipely.Tests/PipeWriterUnflushedBytesTests.cs`, add `using System.Buffers;` (needed if any future test references `IMemoryOwner<byte>` directly; `TrackingMemoryOwner` already lives in `PipelyTests`). The current file only needs `Xunit`, but `TrackingMemoryOwner` is in the same namespace so no extra using is required for the type itself. Skip this step if the existing file already compiles for the tests below.

- [ ] **Step 2: Write the Splice tests**

Append to `PipeWriterUnflushedBytesTests`:

```csharp
    [Fact]
    public void UnflushedBytes_ReflectsSpliceLength_NoArg_OnEmptyPipe()
    {
        using var pipe = new Pipely.Pipe();
        var owner = new TrackingMemoryOwner(64);

        pipe.Writer.Splice(owner);
        Assert.Equal(64L, pipe.Writer.UnflushedBytes);
    }

    [Fact]
    public void UnflushedBytes_ReflectsSpliceLength_ThreeArg_OnEmptyPipe()
    {
        using var pipe = new Pipely.Pipe();
        var owner = new TrackingMemoryOwner(256);

        pipe.Writer.Splice(owner, start: 100, length: 50);
        Assert.Equal(50L, pipe.Writer.UnflushedBytes);
    }

    [Fact]
    public void UnflushedBytes_AccumulatesAcrossAdvanceAndSplice()
    {
        using var pipe = new Pipely.Pipe();
        pipe.Writer.GetMemory(100);
        pipe.Writer.Advance(20);
        Assert.Equal(20L, pipe.Writer.UnflushedBytes);

        var owner = new TrackingMemoryOwner(64);
        pipe.Writer.Splice(owner);
        Assert.Equal(20L + 64L, pipe.Writer.UnflushedBytes);

        pipe.Writer.GetMemory(100);
        pipe.Writer.Advance(5);
        Assert.Equal(20L + 64L + 5L, pipe.Writer.UnflushedBytes);
    }

    [Fact]
    public async Task UnflushedBytes_ZeroAfterFlush_FollowingSplice()
    {
        using var pipe = new Pipely.Pipe();
        var owner = new TrackingMemoryOwner(64);

        pipe.Writer.Splice(owner);
        Assert.Equal(64L, pipe.Writer.UnflushedBytes);

        await pipe.Writer.FlushAsync();
        Assert.Equal(0L, pipe.Writer.UnflushedBytes);
    }
```

- [ ] **Step 3: Run the Splice tests**

Run: `dotnet test tests/Pipely.Tests --filter "FullyQualifiedName~PipeWriterUnflushedBytesTests"`

Expected: PASS for all tests in the class.

- [ ] **Step 4: Commit**

```bash
git add tests/Pipely.Tests/PipeWriterUnflushedBytesTests.cs
git commit -m "PipeWriter: cover UnflushedBytes Splice contribution"
```

---

## Task 5: Dispose preserves the value (BCL parity — no state mutation)

**Files:**
- Modify: `tests/Pipely.Tests/PipeWriterUnflushedBytesTests.cs`

BCL never resets `_unflushedBytes` outside `CommitUnsynchronized` and `ResetState`. Pipely's `Dispose` (`Pipe.cs:64-99`) doesn't touch `_totalWritten` or `_lastPublishedWriterState` either. Reading `UnflushedBytes` after `Dispose` should therefore return whatever the count was just before Dispose — no exception.

- [ ] **Step 1: Write the test**

Append to `PipeWriterUnflushedBytesTests`:

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

- [ ] **Step 2: Run the test**

Run: `dotnet test tests/Pipely.Tests --filter "FullyQualifiedName~PipeWriterUnflushedBytesTests.UnflushedBytes_AfterDispose"`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/Pipely.Tests/PipeWriterUnflushedBytesTests.cs
git commit -m "PipeWriter: cover UnflushedBytes survives Dispose without throwing"
```

---

## Task 6: BCL parity test — same script through both pipes

**Files:**
- Modify: `tests/Pipely.Tests/BclParityTests.cs`

A single script of `Advance` / `FlushAsync` / `Advance` / `Complete` is run through both BCL `Pipe` and Pipely `Pipe`; `UnflushedBytes` is asserted equal at each checkpoint. Splice is excluded — BCL has no Splice, so it is not part of the parity surface.

- [ ] **Step 1: Write the parity test**

Append to `BclParityTests` (just before the closing `}` of the class):

```csharp
    [Theory]
    [InlineData(PipeKind.Bcl)]
    [InlineData(PipeKind.Pipely)]
    public async Task UnflushedBytes_ParityScript_AdvanceFlushCompleteSequence(PipeKind kind)
    {
        var (reader, writer, disp) = CreatePipe(kind);
        using (disp)
        {
            // Both BCL and Pipely override these to true.
            Assert.True(writer.CanGetUnflushedBytes);

            // Fresh pipe.
            Assert.Equal(0L, writer.UnflushedBytes);

            // Advance accumulates.
            writer.GetMemory(100);
            writer.Advance(40);
            Assert.Equal(40L, writer.UnflushedBytes);

            writer.Advance(10);
            Assert.Equal(50L, writer.UnflushedBytes);

            // FlushAsync resets to 0. Drain on the reader so backpressure
            // is irrelevant and the flush completes synchronously.
            var flushTask = writer.FlushAsync();
            var read = await reader.ReadAsync();
            reader.AdvanceTo(read.Buffer.End);
            await flushTask;
            Assert.Equal(0L, writer.UnflushedBytes);

            // Post-flush Advance accumulates from 0.
            writer.GetMemory(100);
            writer.Advance(7);
            Assert.Equal(7L, writer.UnflushedBytes);

            // Complete resets to 0 (BCL: CommitUnsynchronized inside CompleteWriter;
            // Pipely: snapshot publish in Complete writes _lastPublishedWriterState
            // to current _totalWritten).
            writer.Complete();
            Assert.Equal(0L, writer.UnflushedBytes);
        }
    }
```

- [ ] **Step 2: Run the parity test**

Run: `dotnet test tests/Pipely.Tests --filter "FullyQualifiedName~BclParityTests.UnflushedBytes_ParityScript"`

Expected: PASS for both `PipeKind.Bcl` and `PipeKind.Pipely`.

- [ ] **Step 3: Run the full test suite as a regression check**

Run: `dotnet test tests/Pipely.Tests`

Expected: all tests pass (no regressions in any existing suite).

- [ ] **Step 4: Commit**

```bash
git add tests/Pipely.Tests/BclParityTests.cs
git commit -m "BclParityTests: assert UnflushedBytes parity across Advance/Flush/Complete"
```

---

## Self-Review Notes

- **Spec coverage.** The design called for: (1) `CanGetUnflushedBytes => true` — Task 1. (2) `UnflushedBytes` computed via subtraction — Task 2. (3) Tests for fresh pipe / Advance / Flush / Complete (no-ex and with-ex) / Splice / mixed / Dispose — Tasks 2–5. (4) BCL parity test — Task 6. All covered.
- **No placeholders.** Each step has either exact code, an exact `dotnet test` command with `--filter`, or an exact `git` command.
- **Type / signature consistency.** The two override signatures (`public override bool CanGetUnflushedBytes` and `public override long UnflushedBytes`) match `System.IO.Pipelines.PipeWriter`'s base. The `_pipe._totalWritten` and `_pipe._lastPublishedWriterState.TotalWritten` references are visible from inside the same assembly (`PipeWriter` and `Pipe` are both in `Pipely`, both internal-friendly to each other).
- **No new public API beyond the two overrides.** No XML doc comments are needed beyond the existing inheritdoc semantics — the BCL base class already documents both members.
