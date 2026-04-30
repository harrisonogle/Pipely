# FastScheduler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `FastScheduler` — a custom `IContinuationDispatcher` that routes `Pipe`'s parked-awaiter continuations to a dedicated busy-spinning thread, with `ThreadPool` overflow — in a separate project, with full test coverage and a benchmark project that measures it against the default TP dispatcher.

**Architecture:** Single packed `int _state` with two bit flags (`Busy=1`, `ShutdownRequested=2`). All cross-thread synchronization through `Interlocked.{CompareExchange, Or, And, Exchange}` on this one word. One slot (`_pending` callback + `_pendingState` payload). One dedicated worker thread. Dispose via `Or` + `Join`. See `docs/superpowers/specs/2026-04-27-fast-scheduler-design.md` for the full spec, including the four-races correctness argument.

**Tech Stack:** C# / .NET 10, `Interlocked` (no `Volatile`, no `volatile` keyword), xUnit 2.9.3, BenchmarkDotNet 0.15.8.

**Reference docs (engineer should re-read before starting):**
- `docs/superpowers/specs/2026-04-27-fast-scheduler-design.md` — spec (architecture, invariants, correctness argument).
- `docs/IContinuationDispatcher.md` — public-surface description and contract items.
- `tests/Pipe.Tests/PipeContinuationDispatcherTests.cs` — existing dispatcher tests at the Pipe level (useful patterns for AsyncLocal flow tests in tasks 14-16).
- `src/Pipely/IContinuationDispatcher.cs` — the interface this implementation realizes.
- `src/Pipely/PipeOptions.cs` — the `ContinuationDispatcher` option that pipes use to plug us in.

**Working directory for all commands:** `/home/harrison/src/worktrees/Pipe/fast-scheduler/`

---

## Task 1: Create the Pipely project skeleton

**Files:**
- Create: `src/Pipely/Pipely.csproj`
- Create: `src/Pipely/FastScheduler.cs` (stub — full impl comes in Task 5)

- [ ] **Step 1: Create the directory**

```bash
mkdir -p src/Pipely
```

- [ ] **Step 2: Create the csproj**

Write `src/Pipely/Pipely.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
    <LangVersion>latest</LangVersion>
  </PropertyGroup>

  <ItemGroup>
    <ProjectReference Include="..\Pipely\Pipely.csproj" />
  </ItemGroup>

</Project>
```

- [ ] **Step 3: Create a stub for the type so the project compiles**

Write `src/Pipely/FastScheduler.cs`:

```csharp
using Pipely;

namespace Pipely;

public sealed class FastScheduler : IContinuationDispatcher, IDisposable
{
    public void UnsafeQueueUserWorkItem(Action<object?> callback, object? state)
        => throw new NotImplementedException();

    public void Dispose() => throw new NotImplementedException();
}
```

- [ ] **Step 4: Verify project compiles**

Run: `dotnet build src/Pipely/Pipely.csproj`
Expected: build succeeds with 0 warnings.

- [ ] **Step 5: Commit**

```bash
git add src/Pipely
git commit -m "$(cat <<'EOF'
FastScheduler: scaffold Pipely project

Empty project + stub FastScheduler type. Real
implementation lands in the next-task TDD step driven by test A.1.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Create the Pipely.Tests project skeleton

**Files:**
- Create: `tests/Pipely.Tests/Pipely.Tests.csproj`
- Create: `tests/Pipely.Tests/FastSchedulerTests.cs` (empty test class)

- [ ] **Step 1: Create the directory**

```bash
mkdir -p tests/Pipely.Tests
```

- [ ] **Step 2: Create the csproj**

Write `tests/Pipely.Tests/Pipely.Tests.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
    <IsPackable>false</IsPackable>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="coverlet.collector" Version="6.0.4" />
    <PackageReference Include="Microsoft.NET.Test.Sdk" Version="17.14.1" />
    <PackageReference Include="xunit" Version="2.9.3" />
    <PackageReference Include="xunit.runner.visualstudio" Version="3.1.4" />
  </ItemGroup>

  <ItemGroup>
    <Using Include="Xunit" />
  </ItemGroup>

  <ItemGroup>
    <ProjectReference Include="..\..\src\Pipely\Pipely.csproj" />
    <ProjectReference Include="..\..\src\Pipely\Pipely.csproj" />
  </ItemGroup>

</Project>
```

- [ ] **Step 3: Create the empty test class**

Write `tests/Pipely.Tests/FastSchedulerTests.cs`:

```csharp
using Pipely;

namespace Pipely.Tests;

public class FastSchedulerTests
{
    // Tests added in subsequent tasks.
}
```

- [ ] **Step 4: Verify the test project compiles**

Run: `dotnet build tests/Pipely.Tests/Pipely.Tests.csproj`
Expected: build succeeds with 0 warnings.

- [ ] **Step 5: Commit**

```bash
git add tests/Pipely.Tests
git commit -m "$(cat <<'EOF'
FastScheduler: scaffold Pipely.Tests project

xUnit-based test project; empty test class. Tests added per-task in
subsequent steps following the spec's required test surface (§7).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Add both new projects to the solution

**Files:**
- Modify: `Pipe.slnx`

- [ ] **Step 1: Read the current slnx**

Run: `cat Pipe.slnx`
Expected output:

```xml
<Solution>
  <Folder Name="/src/">
    <Project Path="src/Pipely/Pipely.csproj" />
  </Folder>
  <Folder Name="/tests/">
    <Project Path="tests/Pipe.Benchmarks/Pipe.Benchmarks.csproj" />
    <Project Path="tests/Pipe.Stress/Pipe.Stress.csproj" />
    <Project Path="tests/Pipe.Tests/Pipe.Tests.csproj" />
  </Folder>
</Solution>
```

- [ ] **Step 2: Edit `Pipe.slnx` to add both projects**

Replace its contents with:

```xml
<Solution>
  <Folder Name="/src/">
    <Project Path="src/Pipely/Pipely.csproj" />
    <Project Path="src/Pipely/Pipely.csproj" />
  </Folder>
  <Folder Name="/tests/">
    <Project Path="tests/Pipe.Benchmarks/Pipe.Benchmarks.csproj" />
    <Project Path="tests/Pipe.Stress/Pipe.Stress.csproj" />
    <Project Path="tests/Pipe.Tests/Pipe.Tests.csproj" />
    <Project Path="tests/Pipely.Tests/Pipely.Tests.csproj" />
  </Folder>
</Solution>
```

- [ ] **Step 3: Build the whole solution**

Run: `dotnet build Pipe.slnx`
Expected: all 5 projects build (the original 4 + the new 2 visible to the solution; the new Tests project depends on the new FastScheduler project, so both must compile clean).

- [ ] **Step 4: Run all tests, verify the new test project is discovered**

Run: `dotnet test Pipe.slnx --nologo`
Expected: all existing tests pass; the new `Pipely.Tests` project shows "0 tests run" (no tests yet).

- [ ] **Step 5: Commit**

```bash
git add Pipe.slnx
git commit -m "$(cat <<'EOF'
FastScheduler: add new projects to Pipe.slnx

Solution now references Pipely and its test project.
dotnet build and dotnet test both green.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Test A.1 + full state-machine implementation (TDD)

This is the architecture-establishing task. Write test A.1 (callback runs on dedicated thread), watch it fail, then implement the full state machine per `docs/superpowers/specs/2026-04-27-fast-scheduler-design.md` §3 in one go.

**Files:**
- Modify: `tests/Pipely.Tests/FastSchedulerTests.cs`
- Modify: `src/Pipely/FastScheduler.cs`

- [ ] **Step 1: Write the failing test**

Add this test method to `FastSchedulerTests`:

```csharp
[Fact]
public void Dispatch_InvokesCallbackOnDedicatedThread()
{
    using var dispatcher = new FastScheduler();
    int? observedThreadId = null;
    string? observedThreadName = null;
    using var done = new ManualResetEventSlim(false);

    dispatcher.UnsafeQueueUserWorkItem(_ =>
    {
        observedThreadId = Environment.CurrentManagedThreadId;
        observedThreadName = Thread.CurrentThread.Name;
        done.Set();
    }, null);

    Assert.True(done.Wait(TimeSpan.FromSeconds(5)),
        "Callback was not invoked within 5 seconds.");
    Assert.NotEqual(Environment.CurrentManagedThreadId, observedThreadId);
    Assert.Equal("Pipe FastScheduler", observedThreadName);
}
```

- [ ] **Step 2: Run the test — expect it to throw `NotImplementedException`**

Run: `dotnet test tests/Pipely.Tests --nologo --filter "Dispatch_InvokesCallbackOnDedicatedThread"`
Expected: test fails — `UnsafeQueueUserWorkItem` throws `NotImplementedException`.

- [ ] **Step 3: Implement the full state machine per spec §3**

Replace the contents of `src/Pipely/FastScheduler.cs` with:

```csharp
using Pipely;

namespace Pipely;

/// <summary>
/// <see cref="IContinuationDispatcher"/> implementation that routes the first hop of
/// each Pipe continuation to a dedicated busy-spinning thread, with ThreadPool
/// overflow when the dedicated thread is already invoking another continuation.
///
/// <para>
/// All cross-thread synchronization runs through a single packed <see cref="int"/>
/// (<c>_state</c>) using <c>Interlocked.{CompareExchange, Or, And, Exchange}</c>:
/// </para>
///
/// <list type="bullet">
/// <item>Bit 0 (<c>Busy</c>): slot has an in-flight callback.</item>
/// <item>Bit 1 (<c>ShutdownRequested</c>): <see cref="Dispose"/> has run; monotonic.</item>
/// </list>
///
/// <para>
/// State <c>Busy | ShutdownRequested</c> = 2 (Vacant + ShutdownRequested) is terminal:
/// no Dispatch can claim, the loop exits, no callback is dropped. See
/// <c>docs/superpowers/specs/2026-04-27-fast-scheduler-design.md</c> for the
/// full spec and four-races correctness argument.
/// </para>
/// </summary>
public sealed class FastScheduler : IContinuationDispatcher, IDisposable
{
    private const int Busy              = 1;
    private const int ShutdownRequested = 2;

    // Tunable. Open per spec §9 — closed by the benchmark project's measurement loop.
    private const int SpinIterations = 10;

    private int _state;
    private Action<object?>? _pending;
    private object? _pendingState;
    private readonly Thread _thread;

    public FastScheduler()
    {
        _thread = new Thread(Loop)
        {
            IsBackground = true,
            Name = "Pipe FastScheduler",
        };
        _thread.Start();
    }

    public void UnsafeQueueUserWorkItem(Action<object?> callback, object? state)
    {
        // Conditional claim: succeeds only when state == 0 (Vacant, no shutdown).
        if (Interlocked.CompareExchange(ref _state, Busy, 0) == 0)
        {
            _pendingState = state;                            // plain
            Interlocked.Exchange(ref _pending, callback);     // full fence: publishes both fields
            return;
        }

        // Slot busy or shutdown — fall through to TP. UnsafeQueueUserWorkItem
        // (not QueueUserWorkItem or Task.Run) — IContinuationDispatcher contract item #2.
        ThreadPool.UnsafeQueueUserWorkItem(callback, state, preferLocal: false);
    }

    private void Loop()
    {
        while (true)
        {
            // Atomic read-and-clear: returns the previously-stored callback (or null).
            var cb = Interlocked.Exchange(ref _pending, null);
            if (cb != null)
            {
                var st = _pendingState;
                _pendingState = null;
                try { cb(st); }
                catch { /* contract item #5: dispatcher thread survives a throwing continuation */ }

                // Clear Busy bit; preserve ShutdownRequested if Dispose has set it.
                Interlocked.And(ref _state, ~Busy);
            }
            else
            {
                // Fenced read of state. State 2 (Vacant + ShutdownRequested) is terminal.
                var s = Interlocked.CompareExchange(ref _state, 0, 0);
                if (s == ShutdownRequested) return;
                Thread.SpinWait(SpinIterations);
            }
        }
    }

    public void Dispose()
    {
        Interlocked.Or(ref _state, ShutdownRequested);
        _thread.Join();
    }
}
```

- [ ] **Step 4: Run the test — expect it to pass**

Run: `dotnet test tests/Pipely.Tests --nologo --filter "Dispatch_InvokesCallbackOnDedicatedThread"`
Expected: 1 test passed.

- [ ] **Step 5: Commit**

```bash
git add src/Pipely/FastScheduler.cs \
        tests/Pipely.Tests/FastSchedulerTests.cs
git commit -m "$(cat <<'EOF'
FastScheduler: implement state-machine + first dispatched-thread test

Implements the full FastScheduler per spec §3:
single-int _state with Busy/ShutdownRequested bits, atomic slot
publication via Interlocked.Exchange, Loop drains and terminalizes
on state 2, Dispose Ors the bit and Joins. SpinIterations = 10
(tunable per spec §9).

Test A.1 confirms a Dispatched callback runs on the named worker
thread, not the caller's.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Test A.2 — overflow falls back to ThreadPool

**Files:**
- Modify: `tests/Pipely.Tests/FastSchedulerTests.cs`

- [ ] **Step 1: Add the test**

Append to `FastSchedulerTests`:

```csharp
[Fact]
public void Dispatch_OverflowFallsBackToThreadPool()
{
    using var dispatcher = new FastScheduler();
    using var firstStarted = new ManualResetEventSlim(false);
    using var firstRelease = new ManualResetEventSlim(false);
    using var secondDone   = new ManualResetEventSlim(false);
    bool secondOnTpThread = false;

    // First dispatch: claim the slot and hold it until released.
    dispatcher.UnsafeQueueUserWorkItem(_ =>
    {
        firstStarted.Set();
        firstRelease.Wait(TimeSpan.FromSeconds(5));
    }, null);

    Assert.True(firstStarted.Wait(TimeSpan.FromSeconds(5)),
        "First callback never started — slot was never claimed.");

    // Second dispatch: slot is occupied; should overflow to TP.
    dispatcher.UnsafeQueueUserWorkItem(_ =>
    {
        secondOnTpThread = Thread.CurrentThread.IsThreadPoolThread;
        secondDone.Set();
    }, null);

    Assert.True(secondDone.Wait(TimeSpan.FromSeconds(5)),
        "Second (overflow) callback was not invoked.");
    Assert.True(secondOnTpThread,
        "Overflow callback should have run on a ThreadPool thread.");

    firstRelease.Set();
}
```

- [ ] **Step 2: Run the test**

Run: `dotnet test tests/Pipely.Tests --nologo --filter "Dispatch_OverflowFallsBackToThreadPool"`
Expected: PASS — the implementation from Task 4 already covers this.

If the test fails, debug: a CAS-loss in `UnsafeQueueUserWorkItem` should fall through to `ThreadPool.UnsafeQueueUserWorkItem(...)`. Re-read `FastScheduler.UnsafeQueueUserWorkItem`.

- [ ] **Step 3: Commit**

```bash
git add tests/Pipely.Tests/FastSchedulerTests.cs
git commit -m "$(cat <<'EOF'
FastScheduler tests: A.2 — overflow falls back to ThreadPool

Holds the slot via a slow first callback; second concurrent Dispatch
must run on a TP thread (slot was Busy → CAS lost → TP fallback path).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Test A.3 — exactly-once invocation under stress

**Files:**
- Modify: `tests/Pipely.Tests/FastSchedulerTests.cs`

- [ ] **Step 1: Add the test**

Append to `FastSchedulerTests`:

```csharp
[Fact]
public void Dispatch_InvokesEachCallbackExactlyOnce()
{
    using var dispatcher = new FastScheduler();
    const int totalDispatches = 10_000;
    int invocationCount = 0;
    var allDone = new CountdownEvent(totalDispatches);

    Action<object?> cb = _ =>
    {
        Interlocked.Increment(ref invocationCount);
        allDone.Signal();
    };

    // Submit from multiple producer threads to exercise concurrent CAS losers
    // (which fall through to TP).
    Parallel.For(0, totalDispatches, _ => dispatcher.UnsafeQueueUserWorkItem(cb, null));

    Assert.True(allDone.Wait(TimeSpan.FromSeconds(30)),
        $"Not all callbacks ran. Got {invocationCount} of {totalDispatches}.");
    Assert.Equal(totalDispatches, Volatile.Read(ref invocationCount));
}
```

Note: `Volatile.Read` on the assert is only used in the *test* — it's the convention for reading a counter on the asserting thread after a synchronizing event. The dispatcher implementation itself remains `Interlocked`-only.

- [ ] **Step 2: Run the test**

Run: `dotnet test tests/Pipely.Tests --nologo --filter "Dispatch_InvokesEachCallbackExactlyOnce"`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/Pipely.Tests/FastSchedulerTests.cs
git commit -m "$(cat <<'EOF'
FastScheduler tests: A.3 — every callback invoked exactly once under stress

10k Dispatches from Parallel.For; sum of invocation counter == 10k.
Mix of slot-path and TP-overflow paths under contention.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: Test A.4 — UnsafeQueueUserWorkItem never throws

**Files:**
- Modify: `tests/Pipely.Tests/FastSchedulerTests.cs`

- [ ] **Step 1: Add the test**

Append to `FastSchedulerTests`:

```csharp
[Fact]
public void Dispatch_NeverThrowsFromUnsafeQueueUserWorkItem()
{
    using var dispatcher = new FastScheduler();
    const int totalDispatches = 5_000;
    int dispatchExceptions = 0;
    var allDispatched = new CountdownEvent(totalDispatches);

    Parallel.For(0, totalDispatches, _ =>
    {
        try
        {
            dispatcher.UnsafeQueueUserWorkItem(static _ => { }, null);
        }
        catch
        {
            Interlocked.Increment(ref dispatchExceptions);
        }
        finally
        {
            allDispatched.Signal();
        }
    });

    Assert.True(allDispatched.Wait(TimeSpan.FromSeconds(30)));
    Assert.Equal(0, Volatile.Read(ref dispatchExceptions));
}
```

- [ ] **Step 2: Run the test**

Run: `dotnet test tests/Pipely.Tests --nologo --filter "Dispatch_NeverThrowsFromUnsafeQueueUserWorkItem"`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/Pipely.Tests/FastSchedulerTests.cs
git commit -m "$(cat <<'EOF'
FastScheduler tests: A.4 — UnsafeQueueUserWorkItem never throws

5k concurrent dispatches; assert no exception ever escapes
UnsafeQueueUserWorkItem (contract item #4). Failure mode would be
a producer thread crash, hanging the awaiter indefinitely.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: Test A.5 — throwing callback doesn't kill the dispatcher thread

**Files:**
- Modify: `tests/Pipely.Tests/FastSchedulerTests.cs`

- [ ] **Step 1: Add the test**

Append to `FastSchedulerTests`:

```csharp
[Fact]
public void ThrowingCallback_DoesNotKillDispatcherThread()
{
    using var dispatcher = new FastScheduler();
    using var firstDone  = new ManualResetEventSlim(false);
    using var secondDone = new ManualResetEventSlim(false);
    string? secondThreadName = null;

    // First slot-path dispatch throws.
    dispatcher.UnsafeQueueUserWorkItem(_ =>
    {
        firstDone.Set();
        throw new InvalidOperationException("intentional");
    }, null);

    Assert.True(firstDone.Wait(TimeSpan.FromSeconds(5)),
        "First (throwing) callback never ran.");

    // Give the dispatcher thread a moment to finish processing the throw + re-loop.
    Thread.Sleep(50);

    // Second slot-path dispatch must run on the same dedicated thread —
    // the worker survived the throw.
    dispatcher.UnsafeQueueUserWorkItem(_ =>
    {
        secondThreadName = Thread.CurrentThread.Name;
        secondDone.Set();
    }, null);

    Assert.True(secondDone.Wait(TimeSpan.FromSeconds(5)),
        "Second callback after throwing first never ran — dispatcher thread may have died.");
    Assert.Equal("Pipe FastScheduler", secondThreadName);
}
```

- [ ] **Step 2: Run the test**

Run: `dotnet test tests/Pipely.Tests --nologo --filter "ThrowingCallback_DoesNotKillDispatcherThread"`
Expected: PASS — the `try { cb(st); } catch { }` in `Loop` swallows the exception (contract item #5).

- [ ] **Step 3: Commit**

```bash
git add tests/Pipely.Tests/FastSchedulerTests.cs
git commit -m "$(cat <<'EOF'
FastScheduler tests: A.5 — throwing callback doesn't kill dispatcher thread

First slot-path callback throws; second slot-path callback still runs
on the same named dedicated thread. Validates the try/catch in Loop
(contract item #5).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: Test A.6 — Dispatch races Dispose, callback still invoked exactly once

**Files:**
- Modify: `tests/Pipely.Tests/FastSchedulerTests.cs`

- [ ] **Step 1: Add the test**

Append to `FastSchedulerTests`:

```csharp
[Fact]
public void Dispatch_RacingDispose_InvokesCallbackExactlyOnce()
{
    // Repeat to flush out the race: the Dispatcher CAS and Dispose's Or both
    // target _state; the spec's Race 1 / Race 4 cases must close every interleaving.
    const int trials = 200;

    for (int trial = 0; trial < trials; trial++)
    {
        var dispatcher = new FastScheduler();
        int invocationCount = 0;
        using var done = new ManualResetEventSlim(false);

        // Two threads racing: one Dispatches, the other Disposes.
        var dispatchTask = Task.Run(() =>
        {
            dispatcher.UnsafeQueueUserWorkItem(_ =>
            {
                Interlocked.Increment(ref invocationCount);
                done.Set();
            }, null);
        });
        var disposeTask = Task.Run(() => dispatcher.Dispose());

        Task.WaitAll(new[] { dispatchTask, disposeTask }, TimeSpan.FromSeconds(5));

        Assert.True(done.Wait(TimeSpan.FromSeconds(5)),
            $"Trial {trial}: callback never ran.");
        Assert.Equal(1, Volatile.Read(ref invocationCount));
    }
}
```

- [ ] **Step 2: Run the test**

Run: `dotnet test tests/Pipely.Tests --nologo --filter "Dispatch_RacingDispose_InvokesCallbackExactlyOnce"`
Expected: PASS — pins the spec's Race 1, 2, 4 closure.

If this test fails intermittently, that is a critical correctness bug. Debug starting from `FastScheduler.UnsafeQueueUserWorkItem` and `Loop` — the four-races argument is in spec §5.

- [ ] **Step 3: Commit**

```bash
git add tests/Pipely.Tests/FastSchedulerTests.cs
git commit -m "$(cat <<'EOF'
FastScheduler tests: A.6 — Dispatch racing Dispose invokes callback exactly once

200 trials of concurrent UnsafeQueueUserWorkItem + Dispose on fresh
dispatchers; assert callback ran exactly once in every trial. Pins
the spec §5 Race 1 / Race 2 / Race 4 closures.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: Test A.7 — Dispose blocks until in-flight callback completes

**Files:**
- Modify: `tests/Pipely.Tests/FastSchedulerTests.cs`

- [ ] **Step 1: Add the test**

Append to `FastSchedulerTests`:

```csharp
[Fact]
public void Dispose_BlocksUntilInFlightCallbackCompletes()
{
    var dispatcher = new FastScheduler();
    using var callbackStarted = new ManualResetEventSlim(false);
    using var callbackRelease = new ManualResetEventSlim(false);
    int callbackCompleted = 0;

    dispatcher.UnsafeQueueUserWorkItem(_ =>
    {
        callbackStarted.Set();
        callbackRelease.Wait(TimeSpan.FromSeconds(5));
        Interlocked.Increment(ref callbackCompleted);
    }, null);

    Assert.True(callbackStarted.Wait(TimeSpan.FromSeconds(5)),
        "Callback never started.");

    var disposeTask = Task.Run(() => dispatcher.Dispose());

    // Briefly verify Dispose has not yet returned — the callback is still gated.
    Assert.False(disposeTask.Wait(TimeSpan.FromMilliseconds(200)),
        "Dispose returned before the in-flight callback finished.");
    Assert.Equal(0, Volatile.Read(ref callbackCompleted));

    callbackRelease.Set();

    Assert.True(disposeTask.Wait(TimeSpan.FromSeconds(5)),
        "Dispose did not return after callback was released.");
    Assert.Equal(1, Volatile.Read(ref callbackCompleted));
}
```

- [ ] **Step 2: Run the test**

Run: `dotnet test tests/Pipely.Tests --nologo --filter "Dispose_BlocksUntilInFlightCallbackCompletes"`
Expected: PASS — `Thread.Join` in Dispose waits for Loop to exit, which only happens after the callback finishes and the Loop terminalizes.

- [ ] **Step 3: Commit**

```bash
git add tests/Pipely.Tests/FastSchedulerTests.cs
git commit -m "$(cat <<'EOF'
FastScheduler tests: A.7 — Dispose blocks until in-flight callback completes

Slot-path callback gated on a release event; Dispose must not return
until after the callback's gate is released. Pins the Thread.Join
semantics in Dispose and the spec §5 Race 1 closure.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 11: Test A.8 — Dispatch after Dispose always goes to ThreadPool

**Files:**
- Modify: `tests/Pipely.Tests/FastSchedulerTests.cs`

- [ ] **Step 1: Add the test**

Append to `FastSchedulerTests`:

```csharp
[Fact]
public void Dispatch_AfterDispose_AlwaysRunsOnThreadPool()
{
    var dispatcher = new FastScheduler();
    dispatcher.Dispose();

    const int total = 100;
    int onTpThread = 0;
    int notOnTpThread = 0;
    var allDone = new CountdownEvent(total);

    for (int i = 0; i < total; i++)
    {
        dispatcher.UnsafeQueueUserWorkItem(_ =>
        {
            if (Thread.CurrentThread.IsThreadPoolThread)
                Interlocked.Increment(ref onTpThread);
            else
                Interlocked.Increment(ref notOnTpThread);
            allDone.Signal();
        }, null);
    }

    Assert.True(allDone.Wait(TimeSpan.FromSeconds(10)));
    Assert.Equal(total, Volatile.Read(ref onTpThread));
    Assert.Equal(0,     Volatile.Read(ref notOnTpThread));
}
```

- [ ] **Step 2: Run the test**

Run: `dotnet test tests/Pipely.Tests --nologo --filter "Dispatch_AfterDispose_AlwaysRunsOnThreadPool"`
Expected: PASS — after Dispose, `_state` is 2 (ShutdownRequested + Vacant), so every Dispatcher CAS expecting 0 fails and falls through to TP.

- [ ] **Step 3: Commit**

```bash
git add tests/Pipely.Tests/FastSchedulerTests.cs
git commit -m "$(cat <<'EOF'
FastScheduler tests: A.8 — Dispatch after Dispose runs on ThreadPool

100 dispatches after Dispose; every callback runs on a TP thread
(state == ShutdownRequested + Vacant; Dispatcher CAS always fails;
TP fallback). Pins the spec §5 Race 3 closure.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 12: Test A.9 — one dispatcher serves multiple Pipes correctly

**Files:**
- Modify: `tests/Pipely.Tests/FastSchedulerTests.cs`

- [ ] **Step 1: Add the test**

Append to `FastSchedulerTests`:

```csharp
[Fact]
public async Task SingleDispatcher_ServingMultiplePipes_CompletesAllAwaiters()
{
    using var dispatcher = new FastScheduler();
    using var pipeA = new Pipely.Pipe(new PipeOptions { ContinuationDispatcher = dispatcher });
    using var pipeB = new Pipely.Pipe(new PipeOptions { ContinuationDispatcher = dispatcher });

    static async Task Roundtrip(Pipely.Pipe pipe, int payloadBytes)
    {
        var readTask = pipe.Reader.ReadAsync().AsTask();
        await Task.Run(async () =>
        {
            var mem = pipe.Writer.GetMemory(payloadBytes);
            mem.Span.Clear();
            pipe.Writer.Advance(payloadBytes);
            await pipe.Writer.FlushAsync();
        });
        var rr = await readTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(payloadBytes, rr.Buffer.Length);
        pipe.Reader.AdvanceTo(rr.Buffer.End);
    }

    // Run both round-trips concurrently — exercises contract item #3
    // (one dispatcher, multiple producer threads).
    await Task.WhenAll(Roundtrip(pipeA, 7), Roundtrip(pipeB, 11));
}
```

- [ ] **Step 2: Run the test**

Run: `dotnet test tests/Pipely.Tests --nologo --filter "SingleDispatcher_ServingMultiplePipes_CompletesAllAwaiters"`
Expected: PASS — the dispatcher's CAS on `_state` is per-instance, not per-pipe; concurrent producers from different pipes contest the same slot, with overflow to TP.

- [ ] **Step 3: Commit**

```bash
git add tests/Pipely.Tests/FastSchedulerTests.cs
git commit -m "$(cat <<'EOF'
FastScheduler tests: A.9 — one dispatcher serves multiple pipes correctly

Two Pipes share one FastScheduler; concurrent
ReadAsync/FlushAsync round-trips on both pipes complete. Pins contract
item #3 (thread-safety across pipes).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 13: Test B.1 — Pipe round-trip via the dispatcher

**Files:**
- Modify: `tests/Pipely.Tests/FastSchedulerTests.cs`

- [ ] **Step 1: Add the test**

Append to `FastSchedulerTests`:

```csharp
[Fact]
public async Task Pipe_WithFastScheduler_BasicReadFlush_RoundTrip()
{
    using var dispatcher = new FastScheduler();
    using var pipe = new Pipely.Pipe(new PipeOptions { ContinuationDispatcher = dispatcher });

    var readTask = pipe.Reader.ReadAsync().AsTask();
    Assert.False(readTask.IsCompleted, "Reader should park on the empty pipe.");

    await Task.Run(async () =>
    {
        var mem = pipe.Writer.GetMemory(5);
        mem.Span.Clear();
        pipe.Writer.Advance(5);
        await pipe.Writer.FlushAsync();
    });

    var rr = await readTask.WaitAsync(TimeSpan.FromSeconds(5));
    Assert.Equal(5, rr.Buffer.Length);
    pipe.Reader.AdvanceTo(rr.Buffer.End);
}
```

- [ ] **Step 2: Run the test**

Run: `dotnet test tests/Pipely.Tests --nologo --filter "Pipe_WithFastScheduler_BasicReadFlush_RoundTrip"`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/Pipely.Tests/FastSchedulerTests.cs
git commit -m "$(cat <<'EOF'
FastScheduler tests: B.1 — Pipe golden-path round-trip via dispatcher

Reader parks on empty pipe; Writer flushes; Read completes through the
fast-scheduler dispatcher. Validates the dispatcher integrates cleanly
with PipeOptions.ContinuationDispatcher.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 14: Test B.2 — consumer's AsyncLocal flows to continuation

**Files:**
- Modify: `tests/Pipely.Tests/FastSchedulerTests.cs`

- [ ] **Step 1: Add the test**

Append to `FastSchedulerTests`:

```csharp
[Fact]
public async Task Pipe_WithFastScheduler_AsyncLocalFlowsToContinuation()
{
    var asyncLocal = new AsyncLocal<int>();
    using var dispatcher = new FastScheduler();
    using var pipe = new Pipely.Pipe(new PipeOptions { ContinuationDispatcher = dispatcher });

    asyncLocal.Value = 42;

    // Producer fires after a short delay so the consumer parks first.
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

    // Continuation runs on the dispatcher's worker thread; the captured EC
    // (consumer's, with asyncLocal.Value = 42) is restored by MRVTSC's
    // RunInternal regardless of dispatcher choice.
    Assert.Equal(5, rr.Buffer.Length);
    Assert.Equal(42, asyncLocal.Value);
}
```

- [ ] **Step 2: Run the test**

Run: `dotnet test tests/Pipely.Tests --nologo --filter "Pipe_WithFastScheduler_AsyncLocalFlowsToContinuation"`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/Pipely.Tests/FastSchedulerTests.cs
git commit -m "$(cat <<'EOF'
FastScheduler tests: B.2 — consumer's AsyncLocal flows to the continuation

AsyncLocal set before await pipe.Reader.ReadAsync(); after the
continuation runs (on the dispatcher's worker thread), the value is
still observed. Pins EC contract item #2 — the dispatcher does not
disturb MRVTSC's RunInternal-based EC restoration.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 15: Test B.3 — dispatcher thread's AsyncLocal does NOT leak into continuation

**Files:**
- Modify: `tests/Pipely.Tests/FastSchedulerTests.cs`

- [ ] **Step 1: Add the test**

The test pattern follows the existing `tests/Pipe.Tests/PipeContinuationDispatcherTests.cs::CustomDispatcher_DispatcherThreadAsyncLocal_NotObservedInContinuation` test. We replicate it through `FastScheduler` directly. Because that dispatcher's worker thread does not expose a "set this AsyncLocal on the worker thread" hook, we use a one-shot dispatch to set the AsyncLocal *on* the worker thread before the parked-await scenario runs.

Append to `FastSchedulerTests`:

```csharp
[Fact]
public async Task Pipe_WithFastScheduler_DispatcherThreadAsyncLocal_NotObservedInContinuation()
{
    var consumerLocal   = new AsyncLocal<int>();
    var dispatcherLocal = new AsyncLocal<int>();

    using var dispatcher = new FastScheduler();

    // Set dispatcherLocal on the worker thread by dispatching a one-shot through the slot.
    using var setupDone = new ManualResetEventSlim(false);
    dispatcher.UnsafeQueueUserWorkItem(_ =>
    {
        dispatcherLocal.Value = 999;
        setupDone.Set();
    }, null);
    Assert.True(setupDone.Wait(TimeSpan.FromSeconds(5)));

    using var pipe = new Pipely.Pipe(new PipeOptions { ContinuationDispatcher = dispatcher });

    consumerLocal.Value = 42;

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

    // Continuation runs on the dispatcher's worker thread under the consumer's
    // captured EC. consumerLocal.Value (42) is observed; dispatcherLocal.Value
    // (999, set on the worker thread above) is NOT observed.
    Assert.Equal(5, rr.Buffer.Length);
    Assert.Equal(42, consumerLocal.Value);
    Assert.Equal(0,  dispatcherLocal.Value);
}
```

- [ ] **Step 2: Run the test**

Run: `dotnet test tests/Pipely.Tests --nologo --filter "Pipe_WithFastScheduler_DispatcherThreadAsyncLocal_NotObservedInContinuation"`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/Pipely.Tests/FastSchedulerTests.cs
git commit -m "$(cat <<'EOF'
FastScheduler tests: B.3 — dispatcher's AsyncLocal does not leak into continuation

The worker thread's own AsyncLocal value (set via a one-shot dispatch)
is NOT observed by the continuation that resumes the consumer's await.
RunInternal saves/restores the worker thread's EC around the consumer's
captured EC. Pins the EC isolation property of contract item #2.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 16: Test B.4 — rapid park/resume cycles, no version mismatch

**Files:**
- Modify: `tests/Pipely.Tests/FastSchedulerTests.cs`

- [ ] **Step 1: Add the test**

Append to `FastSchedulerTests`:

```csharp
[Fact]
public async Task Pipe_WithFastScheduler_RapidParkResumeCycles_NoVersionMismatch()
{
    using var dispatcher = new FastScheduler();
    using var pipe = new Pipely.Pipe(new PipeOptions { ContinuationDispatcher = dispatcher });

    const int totalCycles  = 1000;
    const int messageBytes = 8;

    var producer = Task.Run(async () =>
    {
        for (int i = 0; i < totalCycles; i++)
        {
            var mem = pipe.Writer.GetMemory(messageBytes);
            mem.Span.Clear();
            pipe.Writer.Advance(messageBytes);
            await pipe.Writer.FlushAsync();
        }
        pipe.Writer.Complete();
    });

    var consumer = Task.Run(async () =>
    {
        long bytesRead = 0;
        long target    = (long)totalCycles * messageBytes;
        while (bytesRead < target)
        {
            var rr = await pipe.Reader.ReadAsync();
            bytesRead += rr.Buffer.Length;
            pipe.Reader.AdvanceTo(rr.Buffer.End);
            if (rr.IsCompleted) break;
        }
        pipe.Reader.Complete();
    });

    await Task.WhenAll(producer, consumer).WaitAsync(TimeSpan.FromSeconds(30));
}
```

- [ ] **Step 2: Run the test**

Run: `dotnet test tests/Pipely.Tests --nologo --filter "Pipe_WithFastScheduler_RapidParkResumeCycles_NoVersionMismatch"`
Expected: PASS — completes within timeout, no exception thrown.

- [ ] **Step 3: Commit**

```bash
git add tests/Pipely.Tests/FastSchedulerTests.cs
git commit -m "$(cat <<'EOF'
FastScheduler tests: B.4 — rapid park/resume cycles, no version mismatch

1000 producer flush + consumer ReadAsync cycles via the fast-scheduler
dispatcher; assert no version-mismatch exceptions and completion within
30s. Stress test against the dispatch hop allowing stale awaiter
version observations.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 17: Run the full test suite

Sanity check: every test we wrote passes, plus all existing tests still pass.

- [ ] **Step 1: Run all tests across the whole solution**

Run: `dotnet test Pipe.slnx --nologo`
Expected: every test in every project passes. The new project should report 13 tests passed (A.1-A.9 + B.1-B.4 = 9 + 4 = 13).

- [ ] **Step 2: If anything fails, debug**

Each failing test maps to a contract item or invariant from spec §4 / §5. Use the failure to localize:

| Failure pattern | Likely invariant / rule violated |
|---|---|
| Dispatch_InvokesCallbackOnDedicatedThread | Constructor doesn't start named thread, or Loop never reads `_pending`. |
| Dispatch_OverflowFallsBackToThreadPool | TP fallback path missing or wrong primitive (must be `UnsafeQueueUserWorkItem`). |
| Dispatch_InvokesEachCallbackExactlyOnce | Loop's `Interlocked.Exchange` claim not atomic, or And-clear-Busy uses wrong mask. |
| Dispatch_RacingDispose_InvokesCallbackExactlyOnce | Spec §5 Race 1/2/4 — likely missing `Or` for shutdown bit or wrong CAS expected-value. |
| Dispose_BlocksUntilInFlightCallbackCompletes | Loop's terminate condition not on state == 2, or `Thread.Join` not called. |
| Dispatch_AfterDispose_AlwaysRunsOnThreadPool | Dispatcher CAS expecting `Vacant` (0) — must reject states 2 and 3. |
| AsyncLocal flow tests | Likely an EC capture inserted somewhere (closure, `QueueUserWorkItem` instead of `UnsafeQueueUserWorkItem`). |

- [ ] **Step 3: No commit needed** — this is a validation step.

---

## Task 18: Create the benchmark project skeleton

**Files:**
- Create: `tests/Pipely.Benchmarks/Pipely.Benchmarks.csproj`
- Create: `tests/Pipely.Benchmarks/Program.cs` (stub)

- [ ] **Step 1: Create the directory**

```bash
mkdir -p tests/Pipely.Benchmarks
```

- [ ] **Step 2: Create the csproj**

Write `tests/Pipely.Benchmarks/Pipely.Benchmarks.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <ItemGroup>
    <ProjectReference Include="..\..\src\Pipely\Pipely.csproj" />
    <ProjectReference Include="..\..\src\Pipely\Pipely.csproj" />
  </ItemGroup>

  <ItemGroup>
    <PackageReference Include="BenchmarkDotNet" Version="0.15.8" />
    <PackageReference Include="System.CommandLine" Version="2.0.7" />
  </ItemGroup>

  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <LangVersion>latest</LangVersion>
    <Nullable>enable</Nullable>
    <ServerGarbageCollection>true</ServerGarbageCollection>
    <ConcurrentGarbageCollection>true</ConcurrentGarbageCollection>
  </PropertyGroup>

</Project>
```

- [ ] **Step 3: Create a placeholder Program.cs**

Write `tests/Pipely.Benchmarks/Program.cs`:

```csharp
// CLI dispatch added in a later task. Sub-commands:
//   latency    — DispatcherLatencyHarness.Run
//   throughput — BenchmarkSwitcher → DispatcherThroughputBench
Console.WriteLine("Pipely.Benchmarks — pass `latency` or `throughput`.");
return 0;
```

- [ ] **Step 4: Verify it compiles**

Run: `dotnet build tests/Pipely.Benchmarks/Pipely.Benchmarks.csproj`
Expected: build succeeds with 0 warnings.

- [ ] **Step 5: Commit**

```bash
git add tests/Pipely.Benchmarks
git commit -m "$(cat <<'EOF'
FastScheduler: scaffold Pipely.Benchmarks project

BDN + System.CommandLine, Server+Concurrent GC, Exe output. Latency
harness and throughput benchmark added in subsequent tasks.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 19: Add the benchmark project to the solution

**Files:**
- Modify: `Pipe.slnx`

- [ ] **Step 1: Edit `Pipe.slnx`**

Add the benchmark project under the `/tests/` folder. The full file should be:

```xml
<Solution>
  <Folder Name="/src/">
    <Project Path="src/Pipely/Pipely.csproj" />
    <Project Path="src/Pipely/Pipely.csproj" />
  </Folder>
  <Folder Name="/tests/">
    <Project Path="tests/Pipe.Benchmarks/Pipe.Benchmarks.csproj" />
    <Project Path="tests/Pipe.Stress/Pipe.Stress.csproj" />
    <Project Path="tests/Pipe.Tests/Pipe.Tests.csproj" />
    <Project Path="tests/Pipely.Tests/Pipely.Tests.csproj" />
    <Project Path="tests/Pipely.Benchmarks/Pipely.Benchmarks.csproj" />
  </Folder>
</Solution>
```

- [ ] **Step 2: Build the solution**

Run: `dotnet build Pipe.slnx`
Expected: all projects build.

- [ ] **Step 3: Commit**

```bash
git add Pipe.slnx
git commit -m "$(cat <<'EOF'
FastScheduler: add benchmarks project to Pipe.slnx

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 20: Implement the dispatcher latency harness

**Files:**
- Create: `tests/Pipely.Benchmarks/DispatcherLatencyHarness.cs`

The harness measures producer→consumer message latency under sustained throughput, parameterized by an optional `IContinuationDispatcher`. Mirrors the percentile-by-sort approach of `tests/Pipe.Benchmarks/LatencyHarness.cs` but is leaner — no wake-gap traces, no TP correlation, no awaiter counters. Just message latency for the dispatcher comparison.

- [ ] **Step 1: Write the harness**

Write `tests/Pipely.Benchmarks/DispatcherLatencyHarness.cs`:

```csharp
using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using Pipely;

namespace Pipely.Benchmarks;

internal sealed record LatencyStats(
    long Count,
    long MinNs,
    long P50Ns,
    long P90Ns,
    long P99Ns,
    long P999Ns,
    long MaxNs,
    double MeanNs);

internal static class DispatcherLatencyHarness
{
    // Producer writes fixed-size messages prefixed with a Stopwatch timestamp.
    // Consumer reads each message and records (now - timestamp). After both sides
    // finish, samples are sorted and exact percentiles are computed by index.
    //
    // dispatcher = null → Pipe uses the default ThreadPoolContinuationDispatcher.
    public static async Task<LatencyStats> Run(IContinuationDispatcher? dispatcher, int messageCount, int messageBytes)
    {
        if (messageBytes < 8) throw new ArgumentException("messageBytes must be >= 8 (timestamp prefix)");

        using var pipe = new Pipely.Pipe(new PipeOptions
        {
            ContinuationDispatcher = dispatcher,
        });

        var samples = new long[messageCount];
        // Pre-touch every 4 KiB page to commit physical memory before the timed run.
        for (int i = 0; i < samples.Length; i += 512) samples[i] = 1;
        Array.Clear(samples);

        long bytesTotal = (long)messageCount * messageBytes;
        int messageIdx = 0;

        var producer = Task.Run(async () =>
        {
            for (int i = 0; i < messageCount; i++)
            {
                var mem = pipe.Writer.GetMemory(messageBytes);
                long t = Stopwatch.GetTimestamp();
                MemoryMarshal.Write(mem.Span, in t);
                pipe.Writer.Advance(messageBytes);
                var fr = await pipe.Writer.FlushAsync();
                if (fr.IsCompleted) break;
            }
            pipe.Writer.Complete();
        });

        var consumer = Task.Run(async () =>
        {
            long consumed = 0;
            byte[] tsBuf = new byte[8];
            while (consumed < bytesTotal)
            {
                var rr = await pipe.Reader.ReadAsync();
                var buf = rr.Buffer;
                while (buf.Length >= messageBytes)
                {
                    buf.Slice(0, 8).CopyTo(tsBuf);
                    long sentTicks = MemoryMarshal.Read<long>(tsBuf);
                    long now = Stopwatch.GetTimestamp();
                    samples[messageIdx++] = now - sentTicks;
                    consumed += messageBytes;
                    buf = buf.Slice(messageBytes);
                }
                long consumedThisRead = rr.Buffer.Length - buf.Length;
                pipe.Reader.AdvanceTo(rr.Buffer.GetPosition(consumedThisRead), rr.Buffer.End);
                if (rr.IsCompleted && consumed >= bytesTotal) break;
            }
            pipe.Reader.Complete();
        });

        await Task.WhenAll(producer, consumer);

        var span = samples.AsSpan(0, messageIdx);
        span.Sort();
        long freq = Stopwatch.Frequency;
        double meanTicks = 0;
        for (int i = 0; i < span.Length; i++) meanTicks += span[i];
        meanTicks /= span.Length;

        return new LatencyStats(
            Count:  span.Length,
            MinNs:  TicksToNs(span[0],                   freq),
            P50Ns:  TicksToNs(Percentile(span, 0.50),    freq),
            P90Ns:  TicksToNs(Percentile(span, 0.90),    freq),
            P99Ns:  TicksToNs(Percentile(span, 0.99),    freq),
            P999Ns: TicksToNs(Percentile(span, 0.999),   freq),
            MaxNs:  TicksToNs(span[^1],                  freq),
            MeanNs: TicksToNs((long)meanTicks,           freq));
    }

    public static void PrintComparison(string label, LatencyStats baseline, LatencyStats compare)
    {
        Console.WriteLine();
        Console.WriteLine($"=== {label} ===");
        Console.WriteLine($"| {"Stat",-8} | {"tp-default",12} | {"fast-scheduler",12} |  Ratio |");
        Console.WriteLine($"|:---------|-------------:|-------------:|-------:|");
        PrintRow("Count", baseline.Count,  compare.Count);
        PrintRow("Min",   baseline.MinNs,  compare.MinNs);
        PrintRow("P50",   baseline.P50Ns,  compare.P50Ns);
        PrintRow("P90",   baseline.P90Ns,  compare.P90Ns);
        PrintRow("P99",   baseline.P99Ns,  compare.P99Ns);
        PrintRow("P99.9", baseline.P999Ns, compare.P999Ns);
        PrintRow("Max",   baseline.MaxNs,  compare.MaxNs);
        PrintRow("Mean",  baseline.MeanNs, compare.MeanNs);
    }

    private static void PrintRow(string label, double baseline, double compare)
    {
        string ratio = baseline <= 0 ? "N/A" :
            string.Format(CultureInfo.InvariantCulture, "{0,6:F2}", compare / baseline);
        Console.WriteLine($"| {label,-8} | {baseline,11:N0}  | {compare,11:N0}  | {ratio} |");
    }

    private static long Percentile(Span<long> sorted, double p)
    {
        int n = sorted.Length;
        int idx = Math.Min(n - 1, Math.Max(0, (int)Math.Ceiling(p * n) - 1));
        return sorted[idx];
    }

    private static long TicksToNs(long ticks, long freq) => (long)(ticks * 1_000_000_000.0 / freq);
}
```

- [ ] **Step 2: Verify it compiles**

Run: `dotnet build tests/Pipely.Benchmarks/Pipely.Benchmarks.csproj`
Expected: build succeeds with 0 warnings.

- [ ] **Step 3: Commit**

```bash
git add tests/Pipely.Benchmarks/DispatcherLatencyHarness.cs
git commit -m "$(cat <<'EOF'
FastScheduler bench: implement DispatcherLatencyHarness

Producer→consumer message-latency harness, parameterized by an optional
IContinuationDispatcher. Records exact percentiles by sort-and-index.
Same shape as tests/Pipe.Benchmarks/LatencyHarness.cs, leaner
(focused on the dispatcher comparison axis only).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 21: Implement the dispatcher throughput benchmark

**Files:**
- Create: `tests/Pipely.Benchmarks/DispatcherThroughputBench.cs`

- [ ] **Step 1: Write the BDN benchmark class**

Write `tests/Pipely.Benchmarks/DispatcherThroughputBench.cs`:

```csharp
using BenchmarkDotNet.Attributes;
using Pipely;

namespace Pipely.Benchmarks;

[MemoryDiagnoser]
public class DispatcherThroughputBench
{
    private const int TotalBytes = 1 << 20;        // 1 MiB per iteration
    private const int ChunkSize  = 4096;

    [Benchmark(Baseline = true)]
    public async Task TpDefault_ProduceAndDrain()
    {
        using var pipe = new Pipely.Pipe(new PipeOptions
        {
            ContinuationDispatcher = null,
        });
        await ProduceAndDrain(pipe);
    }

    [Benchmark]
    public async Task FastScheduler_ProduceAndDrain()
    {
        using var dispatcher = new FastScheduler();
        using var pipe = new Pipely.Pipe(new PipeOptions
        {
            ContinuationDispatcher = dispatcher,
        });
        await ProduceAndDrain(pipe);
    }

    private static async Task ProduceAndDrain(Pipely.Pipe pipe)
    {
        var producer = Task.Run(async () =>
        {
            int written = 0;
            var chunk = new byte[ChunkSize];
            while (written < TotalBytes)
            {
                var memory = pipe.Writer.GetMemory(chunk.Length);
                chunk.CopyTo(memory);
                pipe.Writer.Advance(chunk.Length);
                await pipe.Writer.FlushAsync();
                written += chunk.Length;
            }
            pipe.Writer.Complete();
        });

        var consumer = Task.Run(async () =>
        {
            while (true)
            {
                var result = await pipe.Reader.ReadAsync();
                pipe.Reader.AdvanceTo(result.Buffer.End);
                if (result.IsCompleted) break;
            }
            pipe.Reader.Complete();
        });

        await Task.WhenAll(producer, consumer);
    }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `dotnet build tests/Pipely.Benchmarks/Pipely.Benchmarks.csproj`
Expected: build succeeds with 0 warnings.

- [ ] **Step 3: Commit**

```bash
git add tests/Pipely.Benchmarks/DispatcherThroughputBench.cs
git commit -m "$(cat <<'EOF'
FastScheduler bench: implement DispatcherThroughputBench

BDN + MemoryDiagnoser; two configurations (tp-default baseline vs
fast-scheduler). 1 MiB ProduceAndDrain at 4 KiB chunks — same workload
as tests/Pipe.Benchmarks/ThroughputBenchmarks.cs.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 22: Wire the Program.cs CLI

**Files:**
- Modify: `tests/Pipely.Benchmarks/Program.cs`

Pattern matches `tests/Pipe.Benchmarks/Program.cs`: when `args[0] == "latency"`, run our latency harness with System.CommandLine-parsed options; otherwise pass all args to BDN's `BenchmarkSwitcher` (which handles `--filter`, `--job`, etc., for the throughput benchmark).

- [ ] **Step 1: Replace Program.cs**

Write `tests/Pipely.Benchmarks/Program.cs`:

```csharp
using BenchmarkDotNet.Running;
using Pipely;
using Pipely.Benchmarks;
using System.CommandLine;

// Anything that isn't the latency sub-command (including no args, or BDN args
// like --filter / --job) is forwarded to BenchmarkSwitcher.
if (args.Length == 0 || args[0] != "latency")
{
    BenchmarkSwitcher.FromTypes(new[] { typeof(DispatcherThroughputBench) }).Run(args);
    return 0;
}

var countOption = new Option<int>("--count")
{
    Description = "Message count per latency trial",
    DefaultValueFactory = _ => 100_000,
};

var sizeOption = new Option<int>("--size")
{
    Description = "Message size in bytes (>= 8)",
    DefaultValueFactory = _ => 256,
};

var trialsOption = new Option<int>("--trials")
{
    Description = "Number of latency trials (each trial runs both configurations)",
    DefaultValueFactory = _ => 3,
};

var warmupOption = new Option<int>("--warmup")
{
    Description = "Warmup trials before recording (not included in results)",
    DefaultValueFactory = _ => 1,
};

var latencyCommand = new Command("latency", "Run the latency comparison (tp-default vs fast-scheduler)")
{
    countOption, sizeOption, trialsOption, warmupOption,
};
latencyCommand.SetAction(async parseResult =>
{
    int count   = parseResult.GetValue(countOption);
    int size    = parseResult.GetValue(sizeOption);
    int trials  = parseResult.GetValue(trialsOption);
    int warmup  = parseResult.GetValue(warmupOption);
    await RunLatency(count, size, trials, warmup);
    return 0;
});

var rootCommand = new RootCommand("Pipely benchmark harness")
{
    latencyCommand,
};

return await rootCommand.Parse(args).InvokeAsync();

static async Task RunLatency(int count, int size, int trials, int warmup)
{
    Console.WriteLine($"Latency comparison: {count:N0} messages × {size} B, {trials} trials, {warmup} warmup");

    for (int w = 0; w < warmup; w++)
    {
        Console.WriteLine($"  Warmup trial {w + 1}/{warmup} (not recorded)");
        _ = await DispatcherLatencyHarness.Run(null, count, size);
        using var dispatcher = new FastScheduler();
        _ = await DispatcherLatencyHarness.Run(dispatcher, count, size);
    }

    for (int t = 0; t < trials; t++)
    {
        Console.WriteLine();
        Console.WriteLine($"=== Trial {t + 1}/{trials} ===");

        var tpStats = await DispatcherLatencyHarness.Run(null, count, size);

        LatencyStats hhStats;
        using (var dispatcher = new FastScheduler())
            hhStats = await DispatcherLatencyHarness.Run(dispatcher, count, size);

        DispatcherLatencyHarness.PrintComparison("Message latency (ns)", tpStats, hhStats);
    }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `dotnet build tests/Pipely.Benchmarks/Pipely.Benchmarks.csproj`
Expected: build succeeds with 0 warnings.

- [ ] **Step 3: Commit**

```bash
git add tests/Pipely.Benchmarks/Program.cs
git commit -m "$(cat <<'EOF'
FastScheduler bench: wire Program.cs CLI (latency + throughput sub-commands)

`latency` runs DispatcherLatencyHarness for both configurations N times
with M warmup trials. `throughput` defers to BenchmarkSwitcher.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 23: Smoke-test the benchmark binaries

A quick run to confirm both sub-commands launch and produce output. We are not committing measurements yet — that comes from a Release-build run on the final hardware once `RESULTS.md` is in place.

- [ ] **Step 1: Smoke-test the latency sub-command (Debug, small count)**

Run: `dotnet run --project tests/Pipely.Benchmarks -- latency --count 1000 --trials 1 --warmup 0`
Expected: prints a "=== Trial 1/1 ===" header followed by a "Message latency (ns)" comparison table with `tp-default`, `fast-scheduler`, and a Ratio column. No exceptions.

- [ ] **Step 2: Smoke-test the throughput benchmark (Release)**

Run: `dotnet run --project tests/Pipely.Benchmarks -c Release -- --filter '*'`
Expected: BDN prints its job header and begins running both `TpDefault_ProduceAndDrain` and `FastScheduler_ProduceAndDrain`. You can Ctrl+C after seeing both benchmarks appear in BDN's "Found benchmarks" / running output — this step only verifies that the binary launches, BDN discovers the two benchmarks, and they begin executing without error. A full BDN run is part of the post-implementation measurement loop.

BDN refuses to run Debug builds; the `-c Release` flag is required.

- [ ] **Step 3: No commit needed** — this is a validation step.

---

## Task 24: Write the RESULTS.md scaffold

**Files:**
- Create: `tests/Pipely.Benchmarks/RESULTS.md`

This file documents the methodology, the comparison context, and the design-completion criterion from spec §8.3. Measurement rows are blank — they are filled in as the benchmark loop runs (see post-implementation steps below).

- [ ] **Step 1: Write the scaffold**

Write `tests/Pipely.Benchmarks/RESULTS.md`:

```markdown
# FastScheduler Dispatcher Benchmark Results

**Compared:** `tp-default` (no `ContinuationDispatcher` set; Pipe uses
`ThreadPoolContinuationDispatcher.Instance`) vs `fast-scheduler`
(`Pipely.FastScheduler`).

**Spec reference:** `docs/superpowers/specs/2026-04-27-fast-scheduler-design.md` §8.

## Design-completion criterion

> The implementation is finalized when measurements either justify a tuned
> configuration that beats `tp-default` at the percentiles that matter
> (P50, P90, P99) under reasonable CPU cost, or demonstrate that no
> reasonable configuration does. Each iteration of the implementation lands
> the change with the measurement that justified it.

The architecture in spec §3 is fixed. The tunable surface in spec §9
(`SpinIterations`, backoff body, CPU pinning, mailbox depth) is open. Every
change to a tuning constant in source must be committed alongside the
measurement that drove it.

## Methodology

- Latency: `dotnet run -c Release --project tests/Pipely.Benchmarks -- latency --count 100000 --size 256 --trials 3 --warmup 1`
- Throughput: `dotnet run -c Release --project tests/Pipely.Benchmarks -- --filter '*'`
- Three latency trials per recorded run; warmup trial not recorded.
- Hardware/build details captured at the top of each results section.
- The fast-scheduler worker thread sits at ~100% on its core during the busy-spin
  loop. Latency wins must be read against this CPU cost.

## Starting tunables

Recorded here verbatim so that any tuning iteration is auditable against
the prior baseline.

| Tunable | Starting value | Notes |
|---|---|---|
| `SpinIterations` | 10 | `private const int` in `FastScheduler.cs` |
| Backoff body | `Thread.SpinWait(SpinIterations)` | The entire idle-loop body |
| CPU pinning | none | Worker thread is unpinned in the starting configuration |
| Mailbox depth | 1 | Single-slot with TP overflow on contention |

## Run 1 — starting configuration

**Date:** _(record the date of the run)_
**Hardware:** _(record CPU, RAM, OS)_
**Build:** _(record .NET SDK / runtime versions, GC mode)_
**Commit:** _(record the git SHA at run time)_
**Tunables:** as in "Starting tunables" above; no overrides.

### Latency (ns)

_(Paste the comparison tables from `dotnet run ... latency` here, one block per trial.)_

### Throughput

_(Paste the BDN summary table from `dotnet run ... throughput` here.)_

### Observations

_(Brief honest read of the data. Did fast-scheduler win at P50/P90/P99? At what
CPU cost? Any anomalies? This section commits to a numerical conclusion;
subsequent runs document tuning iterations.)_

## Subsequent runs

Each tuning iteration adds a new section ("Run 2 — pinned worker", "Run 3 —
SpinIterations 50", etc.) capturing the same fields. The implementation is
not finalized until either:

- A configuration is justified by the data and locked in source (with the
  measurement linked from the source comment), or
- Reasonable variants are exhausted and the dispatcher does not earn its
  complexity — in which case this project is removed from the solution.
```

- [ ] **Step 2: Commit**

```bash
git add tests/Pipely.Benchmarks/RESULTS.md
git commit -m "$(cat <<'EOF'
FastScheduler bench: scaffold RESULTS.md

Methodology, design-completion criterion (spec §8.3), and starting tunables
table. Measurement sections are blank — filled in by Release-build runs
on the chosen hardware as the benchmark loop drives implementation
finalization.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 25: Final verification

- [ ] **Step 1: Build the entire solution clean**

Run: `dotnet build Pipe.slnx --nologo -warnaserror`
Expected: 0 errors, 0 warnings, all projects build.

- [ ] **Step 2: Run all tests**

Run: `dotnet test Pipe.slnx --nologo`
Expected: all existing tests pass; the new project reports 13 tests passed.

- [ ] **Step 3: No commit needed** — this is a final smoke-check.

---

## Post-implementation: the measurement loop

The plan above produces an implementation paired with a benchmark project; it does not produce a finalized design. The remaining work — running the benchmark, iterating on the open tunables, committing each tuning change with the measurement that justifies it — happens after the plan executes. Per spec §8.3, the implementation is finalized only when measurements close the design-completion criterion in `RESULTS.md`.

**Suggested first iteration** (not part of this plan, listed here for orientation):

1. Run `latency` and `throughput` commands on the target hardware in Release with no source changes; record results in `RESULTS.md` "Run 1 — starting configuration".
2. Read the data: does `fast-scheduler` beat `tp-default` at P50/P90/P99 under reasonable CPU cost? If yes, evaluate whether further tuning is worth it. If no, identify the bottleneck (TP overflow rate too high? worker thread spinning too cold? cache-line contention?) and pick one open tunable from spec §9 to vary.
3. Edit the source to vary the tunable; re-run; record in "Run 2 — ...". Commit the source change with the measurement output as the commit body.
4. Repeat until either the criterion closes positively (configuration justified) or negatively (reasonable variants exhausted).
