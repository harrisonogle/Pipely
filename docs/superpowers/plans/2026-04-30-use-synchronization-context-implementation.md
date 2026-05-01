# UseSynchronizationContext Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `PipeOptions.UseSynchronizationContext` (default `true`, mirroring BCL) so that parked `ReadAsync` / `FlushAsync` continuations honor a non-default `SynchronizationContext` captured at the await site.

**Architecture:** Capture `SynchronizationContext.Current` on the consumer thread inside `PipelyAwaiter.OnCompleted` (alongside the existing `ExecutionContext` capture). When set, `s_dispatch` routes via `sc.Post(...)` instead of `_dispatcher.Schedule(...)`; the existing `s_invokeWithEc` callback still applies the captured EC, so the EC contract is preserved across the SC.Post hop. The pipe option is forwarded to both awaiters (read + flush) once at construction.

**Tech Stack:** C# / .NET 10, `System.IO.Pipelines`, xUnit. Lock-free SPSC primitives. Source-side EC capture pattern (spec: `docs/superpowers/specs/2026-04-28-source-side-ec-capture.md` if present, else inline in `PipelyAwaiter.cs`).

---

## File Structure

| File | Change | Responsibility |
|---|---|---|
| `src/Pipely/PipeOptions.cs` | modify | Add `UseSynchronizationContext` init-only property (default `true`). |
| `src/Pipely/PipelyAwaiter.cs` | modify | Add `_useSyncContext` ctor param + field; add `_capturedSC` field; capture in `OnCompleted`; branch in `s_dispatch` on captured SC. |
| `src/Pipely/Pipe.cs` | modify | Forward `options.UseSynchronizationContext` to both `PipelyAwaiter` constructors. |
| `tests/Pipely.Tests/PipeContinuationDispatcherTests.cs` | modify | Update existing D.1, D.2, D.3 tests (set `UseSynchronizationContext = false` so their assertions still hold). Add new D.4–D.9 tests for the new behavior. |
| `tests/Pipely.Tests/BclParityTests.cs` | modify | Add two `Theory` tests covering BCL/Pipely parity for both `UseSynchronizationContext = true` and `false`. |

Acceptance criteria (derived from spec discussion):

1. `PipeOptions.Default.UseSynchronizationContext` is `true`.
2. With option `true` and a non-default SC at the await site, parked read/flush continuations dispatch via `SC.Post`, not via the configured PipeScheduler.
3. With option `false`, SC is never consulted; configured PipeScheduler always runs the continuation.
4. The default base `SynchronizationContext` (i.e. `sc.GetType() == typeof(SynchronizationContext)`) is treated as "no SC" — falls through to PipeScheduler.
5. When the consumer suppresses `UseSchedulingContext` in the await flags, SC is not captured even with the pipe option enabled.
6. Captured `ExecutionContext` (e.g. `AsyncLocal<T>` values) is still applied inside the continuation when it runs on the SC's thread.
7. For both `UseSynchronizationContext = true` and `false`, Pipely and BCL behave identically with respect to whether SC is honored.

---

## Task 1: Add `UseSynchronizationContext` to `PipeOptions`

**Files:**
- Modify: `src/Pipely/PipeOptions.cs:23-34` (add new init-only property after `WriterScheduler`)

- [ ] **Step 1: Write the failing test**

Add this test to `tests/Pipely.Tests/BclParityTests.cs` (anywhere inside the `BclParityTests` class):

```csharp
    [Fact]
    public void UseSynchronizationContext_DefaultIsTrue()
    {
        Assert.True(Pipely.PipeOptions.Default.UseSynchronizationContext);
        Assert.True(new Pipely.PipeOptions().UseSynchronizationContext);
        Assert.True(new Pipely.PipeOptions { UseSynchronizationContext = true }.UseSynchronizationContext);
        Assert.False(new Pipely.PipeOptions { UseSynchronizationContext = false }.UseSynchronizationContext);
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `dotnet test tests/Pipely.Tests/Pipely.Tests.csproj --filter "FullyQualifiedName~UseSynchronizationContext_DefaultIsTrue"`
Expected: FAIL — "PipeOptions does not contain a definition for 'UseSynchronizationContext'" (compile error).

- [ ] **Step 3: Add the property to `PipeOptions`**

Edit `src/Pipely/PipeOptions.cs`. After the existing `WriterScheduler` property (around line 34), add:

```csharp
    /// <summary>
    /// When true (the default), parked read/flush continuations honor a non-default
    /// <see cref="SynchronizationContext"/> captured at the await site, dispatching the
    /// continuation via <see cref="SynchronizationContext.Post"/> instead of the configured
    /// <see cref="ReaderScheduler"/>/<see cref="WriterScheduler"/>. When false, the configured
    /// PipeScheduler always runs the continuation. The default base SynchronizationContext
    /// (i.e. one whose runtime type is exactly <see cref="SynchronizationContext"/>) is treated
    /// as "no SC" and falls through to the PipeScheduler. Init-only: chosen once at pipe
    /// construction. Mirrors <see cref="System.IO.Pipelines.PipeOptions.UseSynchronizationContext"/>.
    /// </summary>
    public bool UseSynchronizationContext { get; init; } = true;
```

You'll also need to add `using System.Threading;` at the top of the file for the SC type reference in the doc comment to resolve. Check the current `using` block (around line 1-2):

```csharp
using System.Buffers;
using System.IO.Pipelines;
using System.Threading;

namespace Pipely;
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `dotnet test tests/Pipely.Tests/Pipely.Tests.csproj --filter "FullyQualifiedName~UseSynchronizationContext_DefaultIsTrue"`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/Pipely/PipeOptions.cs tests/Pipely.Tests/BclParityTests.cs
git commit -m "PipeOptions: add UseSynchronizationContext (default true)"
```

---

## Task 2: Update existing D.1/D.2/D.3 tests to opt out via `UseSynchronizationContext = false`

The existing tests assert "SC is NOT honored" — that was correct under the old hard-stripping behavior. Under the new default (`true`), those assertions invert. Update each test to construct the pipe with `UseSynchronizationContext = false` so the assertions still hold and the test continues to lock in the opt-out behavior.

**Files:**
- Modify: `tests/Pipely.Tests/PipeContinuationDispatcherTests.cs:613-728` (D.1 + D.2 + D.3)

- [ ] **Step 1: Update D.1 (`SynchronizationContext_AtAwait_NotHonored_ContinuationOnDispatcherThread`)**

In `PipeContinuationDispatcherTests.cs` around line 624, change the pipe construction:

From:
```csharp
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions { ReaderScheduler = dispatcher, WriterScheduler = dispatcher });
```
To:
```csharp
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions { ReaderScheduler = dispatcher, WriterScheduler = dispatcher, UseSynchronizationContext = false });
```

Also update the XML doc comment above the test (lines 605-611). Replace it with:

```csharp
    /// <summary>
    /// With <see cref="Pipely.PipeOptions.UseSynchronizationContext"/> = false, a non-default
    /// SynchronizationContext set at the await site is NOT honored: the continuation runs on
    /// the dispatcher's chosen thread, NOT on the SC's thread. The captured SC's PostCount
    /// stays 0; the continuation thread name is the dispatcher's thread.
    /// </summary>
```

Also update the inline comment inside the test body (around line 651):

From:
```csharp
        // Captured SC was bypassed — PostCount stays 0.
```
To:
```csharp
        // UseSynchronizationContext = false → captured SC is bypassed. PostCount stays 0.
```

- [ ] **Step 2: Update D.2 (`TaskScheduler_AtAwait_NotHonored_ContinuationOnDispatcherThread`)**

Around line 683, change:
```csharp
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions { ReaderScheduler = dispatcher, WriterScheduler = dispatcher });
```
To:
```csharp
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions { ReaderScheduler = dispatcher, WriterScheduler = dispatcher, UseSynchronizationContext = false });
```

Also update the XML doc comment above the test (lines 660-674). Replace the first paragraph with:

```csharp
    /// <summary>
    /// With <see cref="Pipely.PipeOptions.UseSynchronizationContext"/> = false, a non-default
    /// TaskScheduler captured by the consumer's await (here, via
    /// TaskScheduler.FromCurrentSynchronizationContext on a custom SC) is NOT honored.
    /// The continuation runs on the dispatcher's chosen thread, not the scheduler's thread.
    ///
    /// (Even with the option set to true, Pipely never captures TaskScheduler.Current —
    /// only SynchronizationContext.Current. This test happens to also verify that side
    /// of the design via the SC.Post-based TaskScheduler.FromCurrentSynchronizationContext.)
    ///
    /// Note on test structure: ...
    ///   (keep the rest of the existing comment unchanged)
```

(Preserve the existing "Note on test structure" paragraph that follows — only replace the first paragraph.)

- [ ] **Step 3: Update D.3 (`ConfigureAwait_TrueAndFalse_BothRunOnDispatcherThread`)**

Around line 753, change:
```csharp
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions { ReaderScheduler = dispatcher, WriterScheduler = dispatcher });
```
To:
```csharp
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions { ReaderScheduler = dispatcher, WriterScheduler = dispatcher, UseSynchronizationContext = false });
```

Also update the XML doc above the test (lines 732-739). Replace with:

```csharp
    /// <summary>
    /// With <see cref="Pipely.PipeOptions.UseSynchronizationContext"/> = false,
    /// ConfigureAwait(true) and ConfigureAwait(false) produce identical observable behavior on
    /// a Pipe await: both run the continuation on the dispatcher's chosen thread regardless of
    /// the consumer's captured SC. The SC is never captured (UseSynchronizationContext = false
    /// suppresses it), so the ConfigureAwait flag has no effect on dispatch.
    /// </summary>
```

- [ ] **Step 4: Run the three tests to verify they still pass**

Run:
```bash
dotnet test tests/Pipely.Tests/Pipely.Tests.csproj --filter "FullyQualifiedName~SynchronizationContext_AtAwait_NotHonored|FullyQualifiedName~TaskScheduler_AtAwait_NotHonored|FullyQualifiedName~ConfigureAwait_TrueAndFalse"
```
Expected: 4 passed (D.1, D.2, D.3 with `[InlineData(true)]`, D.3 with `[InlineData(false)]`).

Note: at this point the property is defined and read by tests, but the awaiter doesn't yet use it — the tests pass because the awaiter's *current* behavior happens to be what `UseSynchronizationContext = false` should do. They will still pass after Task 3 because Task 3 makes that behavior conditional on the option being false.

- [ ] **Step 5: Commit**

```bash
git add tests/Pipely.Tests/PipeContinuationDispatcherTests.cs
git commit -m "tests: opt out of UseSynchronizationContext in D.1/D.2/D.3"
```

---

## Task 3: Wire `UseSynchronizationContext` through `PipelyAwaiter` and `Pipe`

This task implements the actual behavior. New tests come in Tasks 4–8. We add the field plumbing and the capture/dispatch logic here, then verify that the existing test suite still passes (it should — we've only added a new code path, gated on `_useSyncContext = true`, but no test exercises that path yet).

**Files:**
- Modify: `src/Pipely/PipelyAwaiter.cs:47-65` (ctor + new field), 71-98 (OnCompleted), 104-108 (s_dispatch)
- Modify: `src/Pipely/Pipe.cs:50-59` (ctor — pass option to both awaiters)

- [ ] **Step 1: Add `_useSyncContext` field, `_capturedSC` field, and update the ctor in `PipelyAwaiter`**

Edit `src/Pipely/PipelyAwaiter.cs`. Find the existing block around lines 44-65:

```csharp
    // The scheduler this awaiter routes continuations through. Set once at construction;
    // immutable for the awaiter's lifetime. Stored on the awaiter so s_dispatch can reach it
    // without a back-pointer to Pipe.
    private readonly PipeScheduler _dispatcher;

    public const int Inactive   = 0b00;
    public const int Pending    = 0b01;
    public const int StateMask  = 0b01;
    public const int CancelFlag = 0b10;
```

Replace the `_dispatcher` declaration with:

```csharp
    // The scheduler this awaiter routes continuations through. Set once at construction;
    // immutable for the awaiter's lifetime. Stored on the awaiter so s_dispatch can reach it
    // without a back-pointer to Pipe.
    private readonly PipeScheduler _dispatcher;

    // When true, OnCompleted captures a non-default SynchronizationContext from the consumer
    // thread; s_dispatch then routes the continuation via SC.Post instead of _dispatcher.Schedule.
    // Set once at construction from PipeOptions.UseSynchronizationContext.
    private readonly bool _useSyncContext;

    // Source-side SC capture stash. Written in OnCompleted on the consumer thread (with
    // Volatile.Write, ordered alongside _capturedEC / _realState / _realContinuation). Read by
    // s_dispatch with a plain read; the publication happens-before edge is provided by MRVTSC's
    // interlocked-on-_continuation (OnCompleted-fires-first path) or by the TP queue→dequeue
    // (SetResult-fires-first race). Cleared in s_invokeWithEc on the dispatcher's thread.
    private SynchronizationContext? _capturedSC;
```

Then update the constructor (currently at line 65):

From:
```csharp
    public PipelyAwaiter(PipeScheduler dispatcher) => _dispatcher = dispatcher;
```
To:
```csharp
    public PipelyAwaiter(PipeScheduler dispatcher, bool useSynchronizationContext = false)
    {
        _dispatcher     = dispatcher;
        _useSyncContext = useSynchronizationContext;
    }
```

The default of `false` for the new ctor parameter preserves source compatibility for any test that constructs `PipelyAwaiter<T>` directly (e.g. the E.1 stress test at line 815). `Pipe` will pass `true` by default since `PipeOptions.UseSynchronizationContext` defaults to `true`.

- [ ] **Step 2: Update `OnCompleted` to capture SC**

In `src/Pipely/PipelyAwaiter.cs`, find the existing `OnCompleted` method (lines 71-98). Replace it with:

```csharp
    public void OnCompleted(
        Action<object?> continuation, object? state,
        short token, ValueTaskSourceOnCompletedFlags flags)
    {
        // Capture EC on the awaiter thread (the consumer's), before MRVTSC's barrier. Capturing
        // inside s_dispatch instead would get the producer thread's EC in the SetResult-fires-first
        // race — wrong; would silently leak AsyncLocal<T> values across requests. See spec §2.2.
        ExecutionContext? ec =
            (flags & ValueTaskSourceOnCompletedFlags.FlowExecutionContext) != 0
                ? ExecutionContext.Capture()
                : null;

        // Capture SynchronizationContext on the awaiter thread (the consumer's), under the same
        // rationale as EC capture — capturing inside s_dispatch would observe the producer
        // thread's SC, which is not what the consumer's await semantics promise. The SC is
        // captured iff (a) the pipe was constructed with UseSynchronizationContext = true,
        // (b) the consumer didn't suppress UseSchedulingContext in the await flags
        // (e.g. ConfigureAwait(false) strips it), and (c) SC.Current is non-default
        // (non-null and not exactly the base SynchronizationContext type — matches BCL's
        // PipeAwaitable.OnCompleted check at runtime/PipeAwaitable.cs:115-127).
        SynchronizationContext? sc = null;
        if (_useSyncContext &&
            (flags & ValueTaskSourceOnCompletedFlags.UseSchedulingContext) != 0)
        {
            var current = SynchronizationContext.Current;
            if (current is not null && current.GetType() != typeof(SynchronizationContext))
                sc = current;
        }

        // Volatile.Write publication order: cap EC, cap SC, then state, then continuation. The
        // TP-dispatched s_dispatch in the SetResult-fires-first race reads these post-publication
        // via the happens-before edge from queue-call to dequeued callback. See spec §4.
        Volatile.Write(ref _capturedEC, ec);
        Volatile.Write(ref _capturedSC, sc);
        Volatile.Write(ref _realState, state);
        Volatile.Write(ref _realContinuation, continuation);

        // Strip both EC and SchedulingContext flags before forwarding. EC is captured by us;
        // leaving the flag on would have MRVTSC capture again (wasteful, unused). SchedulingContext
        // is stripped because routing is decided by us in s_dispatch (either through the captured
        // SC or through the configured dispatcher); we don't want MRVTSC to also try to honor it.
        // See spec §2.2 and §3.3.
        const ValueTaskSourceOnCompletedFlags suppressed =
            ValueTaskSourceOnCompletedFlags.FlowExecutionContext |
            ValueTaskSourceOnCompletedFlags.UseSchedulingContext;
        _core.OnCompleted(s_dispatch, this, token, flags & ~suppressed);
    }
```

- [ ] **Step 3: Update `s_dispatch` to route via SC when captured**

In `src/Pipely/PipelyAwaiter.cs`, find the existing `s_dispatch` (lines 100-108). Replace it with:

```csharp
    // Registered with _core via OnCompleted. Invoked inline by MRVTSC.SetResult on the producer
    // thread (RCA=false), or — in the rare SetResult-fires-first race — queued to TP by MRVTSC
    // and invoked there. In either case, routes the work item (the awaiter itself, as state)
    // through the captured SC if present, else through the configured dispatcher. Either path
    // ends in s_invokeWithEc, which applies the captured EC and invokes the consumer's continuation.
    private static readonly Action<object?> s_dispatch = static state =>
    {
        var awaiter = (PipelyAwaiter<T>)state!;
        var sc = awaiter._capturedSC;
        if (sc is not null)
            sc.Post(s_invokeWithEcSendOrPost, awaiter);
        else
            awaiter._dispatcher.Schedule(s_invokeWithEc!, awaiter);
    };

    // SendOrPostCallback adapter for the SC.Post path. Forwards to s_invokeWithEc, which has
    // the (object? state) signature already; SendOrPostCallback's signature is identical
    // (it's also `void(object?)`), so the adapter is just a static delegate cache to avoid
    // re-allocating per dispatch.
    private static readonly SendOrPostCallback s_invokeWithEcSendOrPost = static state => s_invokeWithEc(state);
```

The captured SC is *not* cleared in `s_dispatch` — it's cleared in `s_invokeWithEc` along with the other awaiter fields, so the cleanup is centralized.

- [ ] **Step 4: Update `s_invokeWithEc` to clear `_capturedSC`**

In `src/Pipely/PipelyAwaiter.cs`, find `s_invokeWithEc` (lines 113-139). The current body reads `_realContinuation`, `_realState`, `_capturedEC` and clears them. Add a clear for `_capturedSC` in the same block. Replace the field-clearing block (currently lines 116-121) with:

```csharp
        var awaiter = (PipelyAwaiter<T>)state!;
        var cont = awaiter._realContinuation;
        var st   = awaiter._realState;
        var ec   = awaiter._capturedEC;
        awaiter._realContinuation = null;
        awaiter._realState = null;
        awaiter._capturedEC = null;
        awaiter._capturedSC = null;
```

- [ ] **Step 5: Forward the option to both awaiters in `Pipe` ctor**

Edit `src/Pipely/Pipe.cs`. Find the constructor body (lines 50-59):

```csharp
    public Pipe(PipeOptions options)
    {
        _options = options;
        var readerScheduler = options.ReaderScheduler ?? PipeScheduler.ThreadPool;
        var writerScheduler = options.WriterScheduler ?? PipeScheduler.ThreadPool;
        _readAwaiter    = new PipelyAwaiter<ReadResult>(readerScheduler);
        _flushAwaiter   = new PipelyAwaiter<FlushResult>(writerScheduler);
        _writerInstance = new PipeWriter(this);
        _readerInstance = new PipeReader(this);
    }
```

Change the two awaiter constructions to pass the option:

```csharp
    public Pipe(PipeOptions options)
    {
        _options = options;
        var readerScheduler = options.ReaderScheduler ?? PipeScheduler.ThreadPool;
        var writerScheduler = options.WriterScheduler ?? PipeScheduler.ThreadPool;
        _readAwaiter    = new PipelyAwaiter<ReadResult>(readerScheduler, options.UseSynchronizationContext);
        _flushAwaiter   = new PipelyAwaiter<FlushResult>(writerScheduler, options.UseSynchronizationContext);
        _writerInstance = new PipeWriter(this);
        _readerInstance = new PipeReader(this);
    }
```

- [ ] **Step 6: Build and run the entire existing test suite**

Run:
```bash
dotnet test tests/Pipely.Tests/Pipely.Tests.csproj
```
Expected: ALL tests pass.

This step verifies that:
1. The new code paths don't break any existing test (the existing tests either don't install an SC, or they explicitly opt out via Task 2's changes, so the new SC-honoring path is never exercised yet).
2. The build succeeds — no compile errors from the new field, ctor parameter, or `s_invokeWithEcSendOrPost` delegate.
3. The E.1 stress test at line 805 still passes; it constructs `PipelyAwaiter<int>` directly via the single-arg ctor (now using the `useSynchronizationContext = false` default), which is the same behavior as before.

If any existing test fails: stop and diagnose. Do not proceed to Task 4.

- [ ] **Step 7: Commit**

```bash
git add src/Pipely/PipelyAwaiter.cs src/Pipely/Pipe.cs
git commit -m "PipelyAwaiter: route continuations through captured SC when enabled"
```

---

## Task 4: Test — SC honored on `ReadAsync` parking (default options)

**Files:**
- Modify: `tests/Pipely.Tests/PipeContinuationDispatcherTests.cs` (add new test in section D)

- [ ] **Step 1: Write the failing test**

Add this test to `PipeContinuationDispatcherTests.cs` immediately after the D.3 test (around line 788, before E.1 starts):

```csharp
    // ---------- D.4 — SC honored when UseSynchronizationContext = true (default, ReadAsync) ----------

    /// <summary>
    /// With <see cref="Pipely.PipeOptions.UseSynchronizationContext"/> = true (the default),
    /// a non-default SynchronizationContext set at the await site IS honored: the parked
    /// ReadAsync continuation dispatches via SC.Post (PostCount > 0) rather than through the
    /// configured ReaderScheduler. The continuation observes the SC's chosen thread, not the
    /// dispatcher's worker thread.
    /// </summary>
    [Fact]
    public async Task SynchronizationContext_AtAwait_Honored_ContinuationViaSCPost_ReadAsync()
    {
        int testThreadId = Environment.CurrentManagedThreadId;

        // Use a recording dispatcher with a counter so we can assert it was NOT consulted
        // for the parked-read continuation.
        int dispatcherScheduleCount = 0;
        var dispatcher = new RecordingDispatcher(_ => Interlocked.Increment(ref dispatcherScheduleCount));

        // Default options: UseSynchronizationContext = true.
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions { ReaderScheduler = dispatcher, WriterScheduler = dispatcher });

        var sc = new CapturingSynchronizationContext();
        var prev = SynchronizationContext.Current;
        SynchronizationContext.SetSynchronizationContext(sc);

        try
        {
            _ = Task.Run(async () =>
            {
                await Task.Delay(50);
                var mem = pipe.Writer.GetMemory(5);
                mem.Span.Clear();
                pipe.Writer.Advance(5);
                await pipe.Writer.FlushAsync();
            });

            var rr = await pipe.Reader.ReadAsync();
            pipe.Reader.AdvanceTo(rr.Buffer.End);
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(prev);
        }

        // SC.Post was called exactly once for the parked-read continuation.
        Assert.Equal(1, Volatile.Read(ref sc.PostCount));
        // The configured dispatcher was NOT consulted for that continuation.
        Assert.Equal(0, Volatile.Read(ref dispatcherScheduleCount));
    }
```

- [ ] **Step 2: Run the test to verify it passes**

Run:
```bash
dotnet test tests/Pipely.Tests/Pipely.Tests.csproj --filter "FullyQualifiedName~SynchronizationContext_AtAwait_Honored_ContinuationViaSCPost_ReadAsync"
```
Expected: PASS.

(If the awaiter wiring from Task 3 is correct, this test exercises the new SC-honoring path end-to-end.)

- [ ] **Step 3: Commit**

```bash
git add tests/Pipely.Tests/PipeContinuationDispatcherTests.cs
git commit -m "tests: assert SC honored on ReadAsync parking (default options)"
```

---

## Task 5: Test — SC honored on `FlushAsync` parking (writer side)

**Files:**
- Modify: `tests/Pipely.Tests/PipeContinuationDispatcherTests.cs` (add new test after D.4)

This locks in that the new behavior applies symmetrically to the write-side awaiter (`_flushAwaiter`), not just `_readAwaiter`.

- [ ] **Step 1: Write the failing test**

Add immediately after the D.4 test:

```csharp
    // ---------- D.5 — SC honored on FlushAsync parking ----------

    /// <summary>
    /// Symmetric to D.4 but for the writer-side awaiter: with the default
    /// UseSynchronizationContext = true, a parked FlushAsync continuation also honors a
    /// non-default SynchronizationContext at the await site. Verifies that the wiring in
    /// Pipe.cs forwards the option to both awaiters (read + flush).
    /// </summary>
    [Fact]
    public async Task SynchronizationContext_AtAwait_Honored_ContinuationViaSCPost_FlushAsync()
    {
        int dispatcherScheduleCount = 0;
        var dispatcher = new RecordingDispatcher(_ => Interlocked.Increment(ref dispatcherScheduleCount));

        // Pause threshold is small so the FlushAsync parks deterministically.
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions
        {
            ReaderScheduler = dispatcher,
            WriterScheduler = dispatcher,
            // UseSynchronizationContext defaults to true.
        });

        // Pre-fill the pipe so the next Write+Flush will park at the pause threshold.
        // PipeOptions.PauseWriterThreshold default is 65536; write that much to push the
        // writer over the pause line on the next flush.
        var firstMem = pipe.Writer.GetMemory(70_000);
        firstMem.Span.Clear();
        pipe.Writer.Advance(70_000);

        var sc = new CapturingSynchronizationContext();
        var prev = SynchronizationContext.Current;
        SynchronizationContext.SetSynchronizationContext(sc);

        try
        {
            // The reader will drain enough to drop us below ResumeWriterThreshold (default 32768),
            // releasing the parked flush. Run after a small delay to guarantee the flush parks first.
            _ = Task.Run(async () =>
            {
                await Task.Delay(50);
                var rr = await pipe.Reader.ReadAsync();
                pipe.Reader.AdvanceTo(rr.Buffer.End);  // consume everything → unblock writer
            });

            // This FlushAsync parks (we're over PauseWriterThreshold). Continuation will resume
            // on the SC's chosen thread.
            await pipe.Writer.FlushAsync();
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(prev);
        }

        Assert.Equal(1, Volatile.Read(ref sc.PostCount));
        Assert.Equal(0, Volatile.Read(ref dispatcherScheduleCount));
    }
```

- [ ] **Step 2: Run the test to verify it passes**

Run:
```bash
dotnet test tests/Pipely.Tests/Pipely.Tests.csproj --filter "FullyQualifiedName~SynchronizationContext_AtAwait_Honored_ContinuationViaSCPost_FlushAsync"
```
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/Pipely.Tests/PipeContinuationDispatcherTests.cs
git commit -m "tests: assert SC honored on FlushAsync parking"
```

---

## Task 6: Test — Default base `SynchronizationContext` is treated as "no SC"

This locks in the BCL `sc.GetType() != typeof(SynchronizationContext)` check from `PipelyAwaiter.OnCompleted` Step 2 of Task 3.

**Files:**
- Modify: `tests/Pipely.Tests/PipeContinuationDispatcherTests.cs` (add new test after D.5)

- [ ] **Step 1: Write the failing test**

Add immediately after D.5:

```csharp
    // ---------- D.6 — default base SynchronizationContext is NOT honored ----------

    /// <summary>
    /// Even with <see cref="Pipely.PipeOptions.UseSynchronizationContext"/> = true, an SC whose
    /// runtime type is exactly <see cref="SynchronizationContext"/> (the default base type) is
    /// treated as "no SC" and does NOT cause SC.Post routing. Matches BCL's runtime check at
    /// PipeAwaitable.cs:115-127. The continuation falls through to the configured dispatcher.
    /// </summary>
    [Fact]
    public async Task SynchronizationContext_DefaultBaseType_NotHonored_FallsThroughToDispatcher()
    {
        using var dispatcher = new DedicatedThreadDispatcher();
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions { ReaderScheduler = dispatcher, WriterScheduler = dispatcher });

        // Install the default base SynchronizationContext (NOT a derived type). The
        // OnCompleted SC-capture branch sees this and rejects it via the GetType() check.
        var defaultSc = new SynchronizationContext();
        var prev = SynchronizationContext.Current;
        SynchronizationContext.SetSynchronizationContext(defaultSc);

        string? observedThreadName = null;
        try
        {
            _ = Task.Run(async () =>
            {
                await Task.Delay(50);
                var mem = pipe.Writer.GetMemory(5);
                mem.Span.Clear();
                pipe.Writer.Advance(5);
                await pipe.Writer.FlushAsync();
            });

            var rr = await pipe.Reader.ReadAsync();
            pipe.Reader.AdvanceTo(rr.Buffer.End);
            observedThreadName = Thread.CurrentThread.Name;
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(prev);
        }

        // Continuation ran on the dispatcher's worker thread, NOT under the base SC.
        Assert.Equal(nameof(DedicatedThreadDispatcher), observedThreadName);
    }
```

- [ ] **Step 2: Run the test to verify it passes**

Run:
```bash
dotnet test tests/Pipely.Tests/Pipely.Tests.csproj --filter "FullyQualifiedName~SynchronizationContext_DefaultBaseType_NotHonored"
```
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/Pipely.Tests/PipeContinuationDispatcherTests.cs
git commit -m "tests: assert default base SC is treated as no SC"
```

---

## Task 7: Test — `UseSchedulingContext`-flag suppression bypasses SC capture

This locks in the per-await opt-out: even with the pipe option `true`, if the consumer suppresses `UseSchedulingContext` in the await flags (e.g. `ConfigureAwait(false)` strips it), the SC is not captured. This is the moral analogue of `ConfigureAwait(false)` for our awaiter.

**Files:**
- Modify: `tests/Pipely.Tests/PipeContinuationDispatcherTests.cs` (add new test after D.6)

- [ ] **Step 1: Write the failing test**

Add immediately after D.6:

```csharp
    // ---------- D.7 — UseSchedulingContext stripped at await site → SC not captured ----------

    /// <summary>
    /// Even with <see cref="Pipely.PipeOptions.UseSynchronizationContext"/> = true, if the
    /// consumer's await suppresses <see cref="ValueTaskSourceOnCompletedFlags.UseSchedulingContext"/>
    /// (which is what <c>ConfigureAwait(false)</c> does on a ValueTask-returning method), the SC is
    /// NOT captured and the continuation falls through to the configured dispatcher.
    /// Drives the awaiter directly to control the flags precisely (the high-level
    /// <c>ConfigureAwait(false)</c>-via-Pipe path is also covered indirectly by D.3).
    /// </summary>
    [Fact]
    public async Task SynchronizationContext_FlagSuppressed_NotCaptured()
    {
        using var dispatcher = new ForwardingDispatcher();
        var awaiter = new Pipely.PipelyAwaiter<int>(dispatcher, useSynchronizationContext: true);

        var sc = new CapturingSynchronizationContext();
        var prev = SynchronizationContext.Current;
        SynchronizationContext.SetSynchronizationContext(sc);

        try
        {
            // Producer signals first; consumer registers without UseSchedulingContext flag.
            // s_dispatch fires (via TP queue, since SetResult was first), reads _capturedSC,
            // sees null (because we suppressed the flag), and routes through dispatcher → TP.
            // SC.Post is NEVER called.
            awaiter._core.SetResult(42);

            var tcs = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
            awaiter.OnCompleted(_ =>
            {
                try { tcs.SetResult(awaiter._core.GetResult(awaiter.Version)); }
                catch (Exception ex) { tcs.SetException(ex); }
            },
            state: null,
            awaiter.Version,
            // FlowExecutionContext is set, but UseSchedulingContext is NOT — this is the
            // moral equivalent of ConfigureAwait(false) at the awaiter level.
            ValueTaskSourceOnCompletedFlags.FlowExecutionContext);

            int result = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(42, result);
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(prev);
        }

        Assert.Equal(0, Volatile.Read(ref sc.PostCount));
    }
```

- [ ] **Step 2: Run the test to verify it passes**

Run:
```bash
dotnet test tests/Pipely.Tests/Pipely.Tests.csproj --filter "FullyQualifiedName~SynchronizationContext_FlagSuppressed_NotCaptured"
```
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/Pipely.Tests/PipeContinuationDispatcherTests.cs
git commit -m "tests: assert UseSchedulingContext-suppression bypasses SC capture"
```

---

## Task 8: Test — EC is preserved across the SC.Post hop

This locks in that `AsyncLocal<T>` values captured at the await site are visible inside the continuation when it runs on the SC's chosen thread. The EC capture/apply is the existing `_capturedEC` machinery; this test verifies it composes correctly with the new SC.Post dispatch path.

**Files:**
- Modify: `tests/Pipely.Tests/PipeContinuationDispatcherTests.cs` (add new test after D.7)

- [ ] **Step 1: Write the failing test**

Add immediately after D.7:

```csharp
    // ---------- D.8 — EC preserved across the SC.Post hop ----------

    /// <summary>
    /// AsyncLocal&lt;T&gt; values set at the await site are visible inside the continuation
    /// even when the continuation is dispatched via SC.Post (rather than the configured
    /// dispatcher). This exercises the composition of two source-side captures: EC (existing)
    /// and SC (new). The capturing SC runs the posted callback on a TP worker thread with
    /// no inherited EC of its own — if the EC apply step were skipped on the SC.Post path,
    /// AsyncLocal would not propagate.
    /// </summary>
    [Fact]
    public async Task EC_PreservedAcrossSCPost_AsyncLocalVisibleInContinuation()
    {
        using var pipe = new Pipely.Pipe(); // default options → UseSynchronizationContext = true

        var local = new AsyncLocal<int>();
        local.Value = 0;

        var sc = new CapturingSynchronizationContext();
        var prev = SynchronizationContext.Current;
        SynchronizationContext.SetSynchronizationContext(sc);

        int observedAsyncLocal = -1;
        try
        {
            local.Value = 12345;  // set on the await thread, AFTER the SC is installed

            _ = Task.Run(async () =>
            {
                await Task.Delay(50);
                var mem = pipe.Writer.GetMemory(5);
                mem.Span.Clear();
                pipe.Writer.Advance(5);
                await pipe.Writer.FlushAsync();
            });

            var rr = await pipe.Reader.ReadAsync();
            pipe.Reader.AdvanceTo(rr.Buffer.End);

            // Read AsyncLocal from inside the continuation context. With EC capture +
            // ExecutionContext.Run inside s_invokeWithEc, this should observe 12345.
            observedAsyncLocal = local.Value;
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(prev);
        }

        Assert.Equal(1, Volatile.Read(ref sc.PostCount));    // SC.Post was used
        Assert.Equal(12345, observedAsyncLocal);             // EC propagated across the hop
    }
```

- [ ] **Step 2: Run the test to verify it passes**

Run:
```bash
dotnet test tests/Pipely.Tests/Pipely.Tests.csproj --filter "FullyQualifiedName~EC_PreservedAcrossSCPost_AsyncLocalVisibleInContinuation"
```
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/Pipely.Tests/PipeContinuationDispatcherTests.cs
git commit -m "tests: assert EC preserved across SC.Post hop"
```

---

## Task 9: BCL parity tests — both pipes honor / opt out identically

Two `Theory` tests in `BclParityTests.cs` that run the same scenario against `PipeKind.Bcl` and `PipeKind.Pipely`, asserting identical observable behavior for the SC-honor and SC-opt-out cases.

**Files:**
- Modify: `tests/Pipely.Tests/BclParityTests.cs`

- [ ] **Step 1: Write the failing tests**

Add a small SC-recording helper near the top of the `BclParityTests` class (if a CapturingSynchronizationContext is not already accessible from this file):

```csharp
    private sealed class CapturingSynchronizationContext : SynchronizationContext
    {
        public int PostCount;
        public override void Post(SendOrPostCallback d, object? state)
        {
            Interlocked.Increment(ref PostCount);
            ThreadPool.UnsafeQueueUserWorkItem(_ => d(state), null);
        }
    }
```

(If the file doesn't already import `System.Threading`, add `using System.Threading;` at the top.)

Then add these two tests:

```csharp
    private static (PipeReader Reader, PipeWriter Writer, IDisposable Disposer) CreatePipeWithSyncCtx(PipeKind kind, bool useSyncCtx)
    {
        switch (kind)
        {
            case PipeKind.Bcl:
                var bcl = new Pipe(new PipeOptions(useSynchronizationContext: useSyncCtx));
                return (bcl.Reader, bcl.Writer, NoOpDisposable.Instance);
            case PipeKind.Pipely:
                var spsc = new Pipely.Pipe(new Pipely.PipeOptions { UseSynchronizationContext = useSyncCtx });
                return (spsc.Reader, spsc.Writer, spsc);
            default:
                throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }

    [Theory]
    [InlineData(PipeKind.Bcl)]
    [InlineData(PipeKind.Pipely)]
    public async Task UseSynchronizationContext_True_SCIsHonored(PipeKind kind)
    {
        var (reader, writer, disp) = CreatePipeWithSyncCtx(kind, useSyncCtx: true);
        using (disp)
        {
            var sc = new CapturingSynchronizationContext();
            var prev = SynchronizationContext.Current;
            SynchronizationContext.SetSynchronizationContext(sc);
            try
            {
                _ = Task.Run(async () =>
                {
                    await Task.Delay(50);
                    var mem = writer.GetMemory(5);
                    mem.Span.Clear();
                    writer.Advance(5);
                    await writer.FlushAsync();
                });

                var rr = await reader.ReadAsync();
                reader.AdvanceTo(rr.Buffer.End);
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(prev);
            }

            Assert.True(Volatile.Read(ref sc.PostCount) > 0,
                $"{kind}: expected SC.Post to be called when UseSynchronizationContext = true");
        }
    }

    [Theory]
    [InlineData(PipeKind.Bcl)]
    [InlineData(PipeKind.Pipely)]
    public async Task UseSynchronizationContext_False_SCIsBypassed(PipeKind kind)
    {
        var (reader, writer, disp) = CreatePipeWithSyncCtx(kind, useSyncCtx: false);
        using (disp)
        {
            var sc = new CapturingSynchronizationContext();
            var prev = SynchronizationContext.Current;
            SynchronizationContext.SetSynchronizationContext(sc);
            try
            {
                _ = Task.Run(async () =>
                {
                    await Task.Delay(50);
                    var mem = writer.GetMemory(5);
                    mem.Span.Clear();
                    writer.Advance(5);
                    await writer.FlushAsync();
                });

                var rr = await reader.ReadAsync();
                reader.AdvanceTo(rr.Buffer.End);
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(prev);
            }

            Assert.Equal(0, Volatile.Read(ref sc.PostCount));
        }
    }
```

Note: BCL's `PipeOptions` constructor uses positional `useSynchronizationContext: useSyncCtx` (it's the last parameter; check `System.IO.Pipelines.PipeOptions` if your IDE flags this). Our `Pipely.PipeOptions` uses initializer syntax because it's `init`-only. Both forms produce equivalent behavior.

- [ ] **Step 2: Run the tests to verify both parity tests pass**

Run:
```bash
dotnet test tests/Pipely.Tests/Pipely.Tests.csproj --filter "FullyQualifiedName~UseSynchronizationContext_True_SCIsHonored|FullyQualifiedName~UseSynchronizationContext_False_SCIsBypassed"
```
Expected: 4 passed (each Theory runs against both `PipeKind.Bcl` and `PipeKind.Pipely`).

- [ ] **Step 3: Commit**

```bash
git add tests/Pipely.Tests/BclParityTests.cs
git commit -m "tests: BCL/Pipely parity for UseSynchronizationContext (true and false)"
```

---

## Task 10: Update README to document the new option

**Files:**
- Modify: `README.md:58` (the "mirrors the BCL options" sentence)

- [ ] **Step 1: Update the option list in `README.md`**

In `README.md`, find the existing line (around line 58):

```markdown
`Pipely.PipeOptions` mirrors the BCL options (`MinimumSegmentSize`, `PauseWriterThreshold`, `ResumeWriterThreshold`, `ReaderScheduler`, `WriterScheduler`); the defaults match `PipeOptions.Default`.
```

Replace it with:

```markdown
`Pipely.PipeOptions` mirrors the BCL options (`MinimumSegmentSize`, `PauseWriterThreshold`, `ResumeWriterThreshold`, `ReaderScheduler`, `WriterScheduler`, `UseSynchronizationContext`); the defaults match `PipeOptions.Default`.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "README: list UseSynchronizationContext in the mirrored options"
```

---

## Task 11: Final verification — full test suite + build

- [ ] **Step 1: Run the full test suite**

Run:
```bash
dotnet test
```
Expected: ALL tests pass. Pay particular attention to:
- The `BclParityTests` (both old and new theories).
- The full `PipeContinuationDispatcherTests` D-section (D.1–D.8).
- The E.1 stress test (`SetResultBeforeOnCompleted_DirectAwaiter_Race_StressN_AllResultsDelivered`) — uses the single-arg `PipelyAwaiter` ctor and would catch any issue with the default of `useSynchronizationContext: false` for the direct-awaiter path.

- [ ] **Step 2: Build Release configuration**

Run:
```bash
dotnet build -c Release
```
Expected: clean build, no warnings.

- [ ] **Step 3: Run the throughput benchmark briefly to sanity-check no regression**

Run (this takes a few minutes — limit to a single benchmark for sanity):
```bash
dotnet run -c Release --project tests/Pipely.Benchmarks -- --filter '*ThroughputBenchmarks*' --warmupCount 1 --iterationCount 3
```
Expected: numbers within ±10% of the README's published `Pipely_ThreadPool` figure (~68 μs / 1MiB). If significantly worse, the additional branch in `s_dispatch` may be a hot-path issue — investigate before merging.

- [ ] **Step 4: Final commit (if anything was tweaked above) — otherwise skip**

If any task above produced a fix in this verification step, commit it with a `fixup:`-prefixed message. Otherwise, this task creates no commit.

---

## Self-Review Notes

- **Spec coverage** — all 7 acceptance criteria from the spec discussion are mapped:
  1. Default = true → Task 1.
  2. SC honored when enabled → Tasks 4 (read) + 5 (flush).
  3. Opt-out works → Tasks 2 (existing tests now pass under opt-out) + 9 (false parity).
  4. Default base SC ignored → Task 6.
  5. Per-await opt-out via flags → Task 7.
  6. EC preserved across SC.Post → Task 8.
  7. BCL parity → Task 9.
- **Type consistency** — `PipelyAwaiter<T>(PipeScheduler, bool)` is consistent across Pipe.cs, the existing E.1 stress test (uses default of `false`), and the new D.7 test (passes explicitly). `_capturedSC` field name and `s_invokeWithEcSendOrPost` delegate name used identically in Tasks 3 and any test that introspects (none currently do).
- **No placeholders** — every code block above contains the actual code; no "TBD" or "similar to Task N" without inlining.
- **Order discipline** — Task 3 makes the production change; Task 2 happens *before* Task 3 (it converts existing tests to the future-correct form so they still pass after Task 3). Tasks 4-9 add new tests against the production behavior introduced in Task 3.
