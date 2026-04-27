# Hot-Handoff Continuation Dispatcher — Design

**Date:** 2026-04-27
**Status:** Spec (pre-implementation). The design described here covers the **fixed architecture** — state machine, contract, correctness argument, and tests. The **tunable surface** (spin count, backoff body, CPU pinning, mailbox depth) is explicitly open and will be closed by the benchmark project that ships alongside the implementation. The implementation is finalized only when measurements either justify a tuned configuration that beats the default `ThreadPool` dispatcher at the percentiles that matter, or demonstrate that no reasonable configuration does — at which point we either ship the measured configuration or do not ship the dispatcher at all.

## Top-level key takeaways

- A user-side implementation of `IContinuationDispatcher` that routes the first hop of each `SpscPipe` continuation to a dedicated, busy-spinning thread, with `ThreadPool.UnsafeQueueUserWorkItem` as the overflow path when the dedicated thread is already invoking another continuation.
- All cross-thread synchronization runs through a **single packed `int`** with two bit-flags (`Busy`, `ShutdownRequested`). Every transition is `Interlocked.{CompareExchange, Or, And, Exchange}` on that one word. No `Volatile.*`, no `volatile` field modifier, no second sync field whose interaction with the first needs argument.
- Slot is single-occupancy: at most one continuation may be staged for the dedicated thread at any time. Concurrent Dispatchers race on a single CAS; losers fall through to TP. This bounds the dispatcher's memory and avoids any queue/mailbox machinery in the architecture.
- Lifecycle is a three-state monotonic transition: `Vacant` → optional `Busy` → optional `ShutdownRequested` (terminal once slot empties). `Dispose` is `Or` + `Join`, and the four observable producer/disposer races are closed without a second sync variable.
- The dispatcher upholds all five items of the `IContinuationDispatcher` contract: exactly-once invocation (race-checked), no `ExecutionContext` capture (structural), thread-safe (single-int CAS), never throws from `UnsafeQueueUserWorkItem`, and survives throwing continuations via `try`/`catch`.
- Lives in a separate project, `src/SpscPipelines.HotHandoff/`, with its own test project and its own benchmark project. `SpscPipelines` itself gains zero new public API. Whether the dispatcher ships as a supported library type, ships as a sample, or doesn't ship at all is a deferred decision tied to the benchmark project's findings.

## Section 1 — Goal and motivation

`SpscPipe` ships a pluggable `IContinuationDispatcher` (`docs/IContinuationDispatcher.md`, §6 of the SPSC pipe design spec). The default forwards to `ThreadPool.UnsafeQueueUserWorkItem`, which carries a fixed per-signal cost — typically a few hundred ns at P50, multi-µs at P99 — to dispatch the continuation onto a TP worker. For a single high-frequency producer/consumer stream this fixed cost is the worst case for the TP scheduler architecture: it does not amortize across multiple workloads, and it scales linearly with signal rate.

The hot-handoff dispatcher is the canonical custom implementation that escapes this cost. It maintains one dedicated thread that busy-spins on a one-slot mailbox, ready to invoke the very next continuation without a kernel wake hop. When the dedicated thread is already running a continuation, subsequent continuations spill to the ThreadPool — i.e., the dispatcher degrades to the default behavior under burst, never refusing work and never violating the "callback invoked exactly once" contract item.

The design's purpose is to be the dispatcher we measure to determine whether the wake-gap escape hatch is worth providing as a supported component. The architecture in this spec is fixed; the implementation's tunable surface is closed by measurement (Section 7).

## Section 2 — Project layout

Three new projects under the existing `SpscPipe.slnx`:

- `src/SpscPipelines.HotHandoff/` — the dispatcher implementation. References `SpscPipelines`. Public surface: exactly one type, `HotHandoffContinuationDispatcher`, implementing `IContinuationDispatcher` and `IDisposable`. No options class. No constructor parameters. No public knobs of any kind. Tunables live as `private const` adjacent to the loop body and are edited between benchmark runs.
- `tests/SpscPipelines.HotHandoff.Tests/` — unit and integration tests for the dispatcher. References `SpscPipelines.HotHandoff` and `SpscPipelines`. xUnit-based, conventions matching the existing `tests/SpscPipe.Tests/`.
- `tests/SpscPipelines.HotHandoff.Benchmarks/` — BenchmarkDotNet-based comparison harness measuring `HotHandoffContinuationDispatcher` against the default `ThreadPoolContinuationDispatcher` under identical workloads. Owns its own `RESULTS.md` so the existing `tests/SpscPipe.Benchmarks/RESULTS.md` (the SpscPipe-vs-BCL comparison) is not muddied with a different axis.

`SpscPipelines.csproj` is unchanged. The dispatcher plugs in via the existing `SpscPipeOptions.ContinuationDispatcher` slot.

## Section 3 — Architecture

The dispatcher is one type with the following internal state:

- `_state : int` — packed bit-flags. **The sole synchronizing field.** Bit 0 is `Busy`; bit 1 is `ShutdownRequested`. Reachable values: 0 (Vacant), 1 (Busy), 2 (ShutdownRequested + Vacant — terminal), 3 (ShutdownRequested + Busy — transient, drains to 2). All transitions go through `Interlocked.{CompareExchange, Or, And}` on this field.
- `_pending : Action<object?>?` — the slot's callback while `Busy`. Atomically published by the producing Dispatcher via `Interlocked.Exchange`; atomically claimed by the loop via `Interlocked.Exchange` (read-and-clear).
- `_pendingState : object?` — the callback's `state` argument. Plain reads/writes; visibility is anchored by the `Interlocked.Exchange` on `_pending`. The Dispatcher writes `_pendingState` before publishing `_pending`; the loop reads `_pendingState` after observing a non-null `_pending`. The full-fence semantics of `Interlocked.Exchange` guarantee the Dispatcher's `_pendingState` write is visible whenever the loop observes a non-null `_pending`.
- `_thread : Thread` — the dedicated worker. `IsBackground = true`, `Name = "SpscPipe HotHandoff"`. Started in the dispatcher's constructor; joined in `Dispose`.

Public surface, exhaustively:

```csharp
public sealed class HotHandoffContinuationDispatcher : IContinuationDispatcher, IDisposable
{
    public HotHandoffContinuationDispatcher();
    public void UnsafeQueueUserWorkItem(Action<object?> callback, object? state);
    public void Dispose();
}
```

### Section 3.1 — State machine

```
Reachable state values:
  0  =  Vacant                       (resting; dispatcher available)
  1  =  Busy                         (slot has work; loop will drain)
  2  =  ShutdownRequested + Vacant   (TERMINAL — loop exits, no Dispatcher can claim)
  3  =  ShutdownRequested + Busy     (transient — drains to 2, then loop exits)

Transitions (only these are reachable):
  Vacant   →  Busy                 :  Dispatcher CAS(_state, Busy, Vacant)
  Busy     →  Vacant               :  Loop  Interlocked.And(_state, ~Busy)        (when ShutdownRequested not set)
  Busy     →  Vacant+Shut          :  Loop  Interlocked.And(_state, ~Busy)        (when ShutdownRequested set)
  Vacant   →  Vacant+Shut          :  Dispose Interlocked.Or(_state, ShutdownRequested)
  Busy     →  Busy+Shut            :  Dispose Interlocked.Or(_state, ShutdownRequested)

State 2 (Vacant + ShutdownRequested) is terminal: every subsequent Dispatcher CAS
finds state ≥ 2 and fails (the CAS expects Vacant); no one clears the ShutdownRequested
bit; the loop's only reachable read of state == 2 leads to a return.
```

### Section 3.2 — Dispatch path

```csharp
public void UnsafeQueueUserWorkItem(Action<object?> callback, object? state)
{
    // Conditional claim: succeeds only when state == Vacant (no Busy bit, no ShutdownRequested bit).
    // A losing CAS — slot busy, shutdown requested, or shutdown final — falls through to TP.
    if (Interlocked.CompareExchange(ref _state, Busy, Vacant) == Vacant)
    {
        _pendingState = state;                              // plain
        Interlocked.Exchange(ref _pending, callback);       // full fence: publishes both fields
        return;
    }

    // Overflow path. Use UnsafeQueueUserWorkItem (not the EC-capturing QueueUserWorkItem
    // or Task.Run) — IContinuationDispatcher contract item #2.
    ThreadPool.UnsafeQueueUserWorkItem(callback, state, preferLocal: false);
}
```

The Dispatch path never reads `_pending` — it only writes after winning the state CAS. A successful CAS confers exclusive write access to `_pending` and `_pendingState` until the loop transitions the Busy bit back off.

### Section 3.3 — Loop

```csharp
private void Loop()
{
    while (true)
    {
        // Atomic read-and-clear of the slot. Returns the previously-stored callback (or null).
        var cb = Interlocked.Exchange(ref _pending, null);
        if (cb != null)
        {
            var st = _pendingState;
            _pendingState = null;
            try { cb(st); } catch { /* contract item #5: dispatcher thread survives */ }

            // Clear the Busy bit. Preserves ShutdownRequested if Dispose has set it.
            // Loop will observe state == 2 on next iteration and return.
            Interlocked.And(ref _state, ~Busy);
        }
        else
        {
            // Fenced read of state. State 2 is terminal — no further transitions possible.
            var s = Interlocked.CompareExchange(ref _state, 0, 0);
            if (s == ShutdownRequested) return;
            Thread.SpinWait(SpinIterations);
        }
    }
}
```

### Section 3.4 — Dispose path

```csharp
public void Dispose()
{
    Interlocked.Or(ref _state, ShutdownRequested);   // sets bit, never disturbs Busy
    _thread.Join();                                  // returns only after loop terminates
}
```

`Or` is the right primitive because it is unconditional with respect to `Busy`: if a Dispatcher has won the slot CAS (state == 1), the `Or` produces state 3 (Busy + ShutdownRequested), and the loop will drain the in-flight callback and then transition to state 2 by clearing the Busy bit. If state was already Vacant (0), the `Or` produces state 2 directly and the loop's next idle observation exits.

## Section 4 — Invariants and rules

I1. **Single sync field.** `_state` is the *only* field that synchronizes producer/dispatcher/disposer threads. Any future change that introduces a second cross-thread synchronizing field invalidates this invariant and requires a re-derivation of correctness.

I2. **State transition closure.** Only the five transitions in §3.1 are reachable. The loop is the unique writer that clears the `Busy` bit. The Dispatcher is the unique writer that sets the `Busy` bit. Dispose is the unique writer that sets the `ShutdownRequested` bit. The `ShutdownRequested` bit is monotonic — once set, never cleared.

I3. **Slot publication ordering.** A successful Dispatcher CAS (`Vacant` → `Busy`) confers exclusive write access to `_pending` and `_pendingState` for the dispatching thread. The Dispatcher writes `_pendingState` (plain) before publishing `_pending` (`Interlocked.Exchange`). The full-fence semantics of the `Exchange` guarantee `_pendingState` is visible to any thread that subsequently observes a non-null `_pending` via `Interlocked.Exchange(ref _pending, null)`.

I4. **Slot claim ordering.** The loop's `Interlocked.Exchange(ref _pending, null)` is the unique reader of `_pending`. A claimed callback is invoked at most once because the claim is atomic and clears the slot in the same operation.

I5. **No EC capture in the dispatcher.** The dispatcher hands the delegate to the loop's thread (slot path) or forwards it to `ThreadPool.UnsafeQueueUserWorkItem` (overflow path) without wrapping it in a closure or queuing primitive that captures `ExecutionContext`. Contract item #2 holds by construction — there is no point in either code path where `ExecutionContext.Capture` could occur.

I6. **Terminal state is unreachable for `Dispatch`.** Once `_state` carries the `ShutdownRequested` bit (states 2 and 3), every Dispatcher CAS expecting `Vacant` (state 0) fails, and every callback is routed to the TP overflow path. State 2 is additionally terminal in the sense that no transition out of it exists once the loop has observed it and returned.

I7. **`Dispose` returns implies loop terminated.** `Thread.Join` blocks the disposing thread until the worker has returned from `Loop()`. The loop returns only after observing `_state == ShutdownRequested` (state 2). After `Dispose` returns, no callback is in flight on the worker thread, and any subsequent Dispatcher call routes to TP.

R1. **Dispatcher CAS shape.** The Dispatch path uses `Interlocked.CompareExchange(ref _state, Busy, Vacant)`. The expected value is `Vacant` (0); any other state, including the transient state 3 or the terminal state 2, must cause CAS failure and TP fallback.

R2. **Loop release shape.** After invoking a callback, the loop clears `Busy` via `Interlocked.And(ref _state, ~Busy)`. The And primitive must be used (not `Exchange`-to-`Vacant`) so that a concurrently-set `ShutdownRequested` bit is preserved.

R3. **Loop terminate shape.** The loop returns when `Interlocked.CompareExchange(ref _state, 0, 0)` returns `ShutdownRequested` (state 2). The CAS-with-self is used as a fenced read of `_state`, not as a state-changing transition. No state-changing CAS is required for terminate, because state 2 is terminal (Section 3.1) and cannot be exited: there is no transition out of state 2 in the closed transition set, so observing state == 2 is sufficient to safely return.

R4. **Dispose shape.** Dispose writes the bit unconditionally via `Interlocked.Or(ref _state, ShutdownRequested)`. It does not CAS — the producer's CAS already guarantees the `Busy` bit is preserved, and the `Or` is idempotent across multiple Dispose calls.

R5. **Throwing-continuation containment.** Every callback invocation is wrapped in `try`/`catch` (contract item #5). The catch is empty — the contract is "the dispatcher thread must survive a throwing continuation," not "the dispatcher must surface the exception somewhere." Surfacing is the consumer's responsibility via the awaiter that scheduled the continuation.

R6. **No heap allocations in the steady-state hot path.** The Dispatch path performs at most one `Interlocked.CompareExchange`, two field writes, and one `Interlocked.Exchange`. The loop's hot path performs one `Interlocked.Exchange` plus either a callback invocation or one `Interlocked.CompareExchange`-as-fenced-read plus `Thread.SpinWait`. None of these allocate.

## Section 5 — Correctness argument: the four Dispose races

The `IContinuationDispatcher` contract requires that every successfully-Dispatched callback is invoked exactly once (item #1). Dispose introduces the four races below; all four are closed by I1–I7 and the single-int state machine.

**Race 1 — Dispatcher CAS-wins, then Dispose runs.** Producer's CAS `Vacant`→`Busy` succeeds, then writes `_pendingState`, then `Interlocked.Exchange`-publishes `_pending`. State is `Busy` (1). Dispose's `Or` produces state 3 (`Busy + ShutdownRequested`). The loop's next iteration claims the callback via `Interlocked.Exchange(ref _pending, null)`, invokes it, then `And ~Busy` produces state 2. The loop's following iteration reads state == 2 and returns. `Join` unblocks. Callback invoked exactly once.

**Race 2 — Dispatcher CAS-wins during Dispose's terminate window.** Loop reads `_pending == null`, then reads `_state` via `CompareExchange(_state, 0, 0)`. Suppose state is observed as `Vacant` (0). Loop spins. Concurrently, Dispose `Or`s `ShutdownRequested` and a separate Dispatcher CAS-wins `Vacant`→`Busy`. The `Or` and CAS are atomic on the same word; hardware serializes them. Either:
- CAS happens first: state goes `Vacant → Busy`, then `Or` produces state 3. Loop's next iteration claims and invokes (Race 1 outcome).
- `Or` happens first: state goes `Vacant → Vacant + ShutdownRequested` (state 2). The Dispatcher CAS expecting `Vacant` (0) finds 2 and fails — Dispatcher routes to TP. Loop's next iteration reads state == 2 and returns.

Either ordering invokes the callback exactly once.

**Race 3 — Dispatcher CAS attempt after the loop has already returned.** State is `ShutdownRequested + Vacant` (2). Dispatcher CAS expecting `Vacant` (0) finds 2, fails, routes to TP. Callback invoked exactly once via the TP overflow path.

**Race 4 — Dispatcher CAS races Dispose's `Or` directly.** Same as Race 2, generalized: the two writes to `_state` are atomic on the same int, hardware-serialized into a single global order. Whichever completes first determines which path the other takes (Race 1 outcome if CAS-first; Race 3 outcome if `Or`-first). No interleaving is possible because both operations target the same machine word.

The proof obligation reduces to "all transitions on `_state` are atomic on the same int" — true by construction — and the case analysis above. There is no two-field synchronization argument required, because there is no second sync field.

## Section 6 — `ExecutionContext` contract

Restated for this implementation:

The `IContinuationDispatcher` contract item #2 forbids EC capture in the dispatcher. This is a structural property of the implementation:

- **Slot path.** The dispatcher writes `callback` (the un-wrapped `Action<object?>` passed to `UnsafeQueueUserWorkItem`) directly into `_pending`. The dedicated thread reads it and invokes it directly. No closure is allocated, no EC primitive is touched, no `ThreadPool.QueueUserWorkItem` (which captures EC) is called. The continuation runs on the dedicated thread under whatever EC `MRVTSC.RunInternal` restores from the consumer's captured EC (set at `OnCompleted` time on the consumer's thread). The dedicated thread's own EC is saved and restored by `RunInternal` around the continuation invocation, so AsyncLocal state on the dispatcher thread is *isolated from* the continuation, not leaked into it.
- **Overflow path.** The dispatcher forwards to `ThreadPool.UnsafeQueueUserWorkItem(callback, state, preferLocal: false)` — the unsafe variant, which does not capture EC. The same `MRVTSC.RunInternal` restoration applies on whatever TP worker picks up the work item.

Both paths satisfy contract item #2 without dispatcher-side EC manipulation. The existing tests in `tests/SpscPipe.Tests/SpscPipeContinuationDispatcherTests.cs` (specifically `CustomDispatcher_AsyncLocalFlowsToContinuation`, `CustomDispatcher_DispatcherThreadAsyncLocal_NotObservedInContinuation`, `CustomDispatcher_DispatcherThreadAsyncLocal_RestoredAfterContinuation`) already pin the EC behavior at the SpscPipe level for any conforming dispatcher; the corresponding tests in this project pin it specifically through `HotHandoffContinuationDispatcher`.

## Section 7 — Required test surface

Tests pin observable behavior, not internal sequencing. Two layers:

**Layer A — Dispatcher in isolation** (no `SpscPipe`; the dispatcher is exercised directly via `UnsafeQueueUserWorkItem`):

- A.1 The dispatcher's first slot-path Dispatch invokes the callback on a thread *other than* the calling thread, and *that thread* is consistently the same across slot-path Dispatches (the dedicated worker).
- A.2 When the slot is occupied, a second concurrent Dispatch invokes its callback on a `ThreadPool` thread (`Thread.CurrentThread.IsThreadPoolThread == true`).
- A.3 Across N Dispatches under concurrent submission, the total invocation count equals N — every callback runs exactly once.
- A.4 `UnsafeQueueUserWorkItem` never propagates an exception to its caller, even under concurrent submission stress.
- A.5 A throwing slot-path callback does not kill the dedicated thread: subsequent slot-path Dispatches still run on the dedicated thread.
- A.6 If a Dispatch CAS-wins concurrent with a Dispose call, the callback is invoked exactly once (either on the dedicated thread before its loop terminates, or via TP if the Dispose `Or` won the race). This pins Race 1, 2, and 4 from Section 5.
- A.7 Dispose blocks the disposing thread until any slot-path callback in flight has completed (verified by an in-callback gate).
- A.8 After Dispose returns, every subsequent Dispatch's callback runs on a `ThreadPool` thread. This pins Race 3 from Section 5.
- A.9 A single `HotHandoffContinuationDispatcher` instance, configured into multiple `SpscPipe` instances simultaneously, services every pipe's awaiter completions correctly. This pins contract item #3 (thread-safety across pipes).

**Layer B — Dispatcher through `SpscPipe`** (plugged into `SpscPipeOptions.ContinuationDispatcher`):

- B.1 A producer-flush + consumer-`ReadAsync` round trip completes correctly when the dispatcher is configured.
- B.2 Consumer-side `AsyncLocal<T>` set before `await Reader.ReadAsync()` is observed in the continuation that runs on the dispatcher's dedicated thread (mirrors `CustomDispatcher_AsyncLocalFlowsToContinuation` from the existing `SpscPipeContinuationDispatcherTests.cs`).
- B.3 The dedicated thread's own `AsyncLocal<T>` is *not* observed in the continuation that runs through the dispatcher (mirrors `CustomDispatcher_DispatcherThreadAsyncLocal_NotObservedInContinuation`).
- B.4 Rapid park/resume cycles (≥1000) under the dispatcher complete without version-mismatch exceptions.

Tests use deterministic synchronization primitives (e.g., `ManualResetEventSlim`, `CountdownEvent`) for in-callback gating. No `Thread.Sleep` for synchronization. All tests have bounded timeouts (≤ 5s).

## Section 8 — Benchmark methodology and design-completion criterion

`tests/SpscPipelines.HotHandoff.Benchmarks/` is a first-class part of the design — it is what closes the open tunables in Section 9.

### Section 8.1 — Comparison configurations

Two configurations, identical in everything else:

- **`tp-default`** — `SpscPipeOptions.ContinuationDispatcher` is `null`; the pipe uses the internal `ThreadPoolContinuationDispatcher.Instance`.
- **`hot-handoff`** — `SpscPipeOptions.ContinuationDispatcher` is a fresh `HotHandoffContinuationDispatcher` per pipe.

Same chunk size, same total bytes, same hardware/build/runtime, same Server GC, same `MemoryDiagnoser`. Only the dispatcher differs.

### Section 8.2 — Measurements

- **Latency:** producer→consumer hand-off latency under sustained throughput, recorded into a flat `long[]` and read by exact percentile rank (the same method the existing `tests/SpscPipe.Benchmarks/LatencyHarness.cs` uses). Reported: Min, P50, P90, P99, P99.9, Max, Mean. Three independent runs per configuration; run-to-run variance reported honestly.
- **Throughput:** sustained 1 MiB / 4 KiB-chunk ProduceAndDrain via BenchmarkDotNet, with `MemoryDiagnoser`. Reported: mean, error, std-dev, allocations-per-op.
- **CPU cost context:** the hot-handoff dispatcher's worker thread sits at ~100% on its core during the busy-spin loop. `RESULTS.md` calls this out so that any latency win is read against the cost: hot-handoff can buy lower wake gap *at the cost of one continuously hot core*.

### Section 8.3 — Design-completion criterion

> The implementation is finalized when measurements either justify a tuned configuration that beats `tp-default` at the percentiles that matter (P50, P90, P99) under reasonable CPU cost, or demonstrate that no reasonable configuration does. Each iteration of the implementation lands the change with the measurement that justified it.

The architecture (Sections 3, 4, 5, 6) is fixed and does not iterate. What iterates is the tunable surface (Section 9). Every tuning change in source is committed alongside the `RESULTS.md` line that justifies it; no tuning constant is changed without a measured rationale.

If the criterion does not close — i.e., no reasonable configuration of the architecture beats TP at the percentiles that matter — the dispatcher does not earn its complexity and we do not ship it. Either outcome (ship a measured configuration, or do not ship) is the design "finished."

## Section 9 — Open tunables

These are deliberately left open by the spec; they will be closed by the benchmark project per Section 8.3.

- **`SpinIterations`** — the integer constant passed to `Thread.SpinWait(...)` in the loop's idle branch. Starts at the doc-sketch value of 10; will be swept against latency percentiles.
- **Backoff body** — currently `Thread.SpinWait(SpinIterations)` only. May become spin-then-`Thread.Yield`, spin-then-park-on-semaphore, or an adaptive variant if measurements indicate. A change to a parking variant reintroduces a kernel-wake hop in the cold-path and must be justified against the latency percentile it's intended to improve.
- **CPU pinning** — the dedicated thread is unpinned in the starting configuration. If measurements show that pinning materially improves percentile latency, the dispatcher will pin its thread (Linux: `sched_setaffinity` via P/Invoke; Windows: `Thread.BeginThreadAffinity` + `ProcessorAffinity`). The pin target (specific core, NUMA-aware, or simply "exclusive core") is itself a measurement-driven decision.
- **Mailbox depth** — single-slot in the starting configuration, with TP overflow on contention. May become a small bounded MPSC queue if measurements show that bursts spilling to TP defeat the dispatcher's purpose for the target workload. A multi-slot variant introduces new ordering and visibility surface and would require an extension of the correctness argument in Section 5.

The starting configuration is not "v1." It is the first measurement point. Each closed tunable is committed with the data that closed it.

## Section 10 — Out of scope (this design)

- **Telemetry counters** (dispatched, fell-back-to-TP, slot-occupancy histogram). May be added if measurements indicate a need for tuning visibility — but there is no claim that they are required.
- **Lifecycle plumbing for in-flight drain across pipe lifetimes.** The contract is "Dispose blocks until the worker thread terminates"; explicit cooperative drain APIs are out of scope.
- **A pluggable `IBackoffStrategy` or similar abstraction.** Discussed and rejected: the tunable surface lives as `private const` and is changed by editing source, not by exposing public knobs that may not survive the benchmark process.
- **Multiple dispatcher threads.** The architecture is one thread per dispatcher instance. A hypothetical multi-threaded variant is out of scope; users wanting more parallelism can construct multiple dispatchers and assign them to different pipes.

## Section 11 — Spec references

- `docs/IContinuationDispatcher.md` — public-surface description of `IContinuationDispatcher`, the canonical hot-handoff sketch, the EC contract, and the doc's enumeration of "production-grade enhancements." This spec inherits the contract verbatim and corrects a Dispose race in the doc's sketch (the loop in the doc-sketch can drop a pending callback if `_shutdown` is set between the slot CAS and the loop's next iteration; this design closes that race via the single-int state machine in Section 5).
- `docs/superpowers/specs/2026-04-25-spsc-pipe-tripleBuffer-design.md` — §5 "Continuation dispatch," I16 (EC discipline), R10 (continuation dispatch protocol), §6 (`IContinuationDispatcher` public contract). This spec implements the public contract that document specifies.
- `docs/superpowers/plans/2026-04-27-pluggable-continuation-dispatch.md` — implementation plan for the `IContinuationDispatcher` mechanism in SpscPipe itself; this spec is the implementation of the canonical custom dispatcher described in that plan's "User-side example" section, with the open question "should we expose a built-in `HotHandoffContinuationDispatcher` in the `SpscPipelines` package?" answered by the project layout in Section 2 (separate project, ship-decision deferred to benchmark findings).
