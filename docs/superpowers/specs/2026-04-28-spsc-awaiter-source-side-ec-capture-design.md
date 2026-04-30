# `PipelyAwaiter` Source-Side EC Capture — Design

**Date:** 2026-04-28
**Status:** Spec (pre-implementation). Supersedes the partial measure in commit `020d770` (which captured `ExecutionContext` at the dispatcher's construction time and wrapped every `cb` invocation in `ExecutionContext.Run`); reverted in `cea6d49` to clear the slate for this design.

## Top-level key takeaways

- Move `ExecutionContext` capture and continuation routing from the consumer-side flag-honoring path (current `ManualResetValueTaskSourceCore<T>` behavior) to the source-side `PipelyAwaiter<T>`, following the pattern established by OpenTcp's `DispatchedValueTaskSource<T>`.
- Fixes two distinct manifestations of "dispatcher ambient state leak" with one mechanism:
  - **Mechanism A** — consumer-side `SynchronizationContext` / `TaskScheduler` capture by `await pipe.Reader.ReadAsync()` silently overrides the dispatcher's chosen routing; in BDN's harness this manifested as a deadlock + 3.5× perf regression that `ConfigureAwait(false)` masked.
  - **Mechanism B** — when the consumer's awaiter has no captured `ExecutionContext` (e.g., `FlowExecutionContext` was suppressed at the consumer's `OnCompleted` time), `MRVTSC.SetResult` skips its `RunInternal` and invokes the continuation under whatever EC the calling thread has — which on a long-lived dispatcher worker thread is the EC drifted by prior continuations' AsyncLocal mutations. Worst-case consequence: cross-tenant `AsyncLocal<T>` leak in a request path.
- `IContinuationDispatcher` contract item #2 ("MUST NOT capture EC") stays literally true. EC handling moves entirely into `PipelyAwaiter`. The dispatcher is genuinely a thread router that takes `(Action<object?>, object?)` work items and delivers them. Existing dispatcher implementations (`FastScheduler`, `ThreadPoolContinuationDispatcher`) require **no source changes**.
- The packed-`int` state machine, `ParkStash`, the four-races correctness argument, R5/R5b CAS protocols, version/token invariants, the cycle-protocol — all unchanged.
- Net effect on `Pipe.cs` / `Pipe.Reader.cs` / `Pipe.Writer.cs`: signal sites *simplify*. The four `s_dispatch*` static delegates and the `DispatchVia` helper are deleted. Signal sites call `_core.SetResult(...)` / `_core.SetException(...)` directly on the producer thread. The dispatcher hop is encapsulated inside `PipelyAwaiter`'s `OnCompleted` + `s_dispatch` flow rather than spread across signal sites.

## Section 1 — Background and motivation

A multi-day investigation in the `fast-scheduler` branch surfaced two distinct correctness gaps in the `IContinuationDispatcher` contract as currently specified:

**Mechanism A (perf cliff + deadlock).** The current dispatcher contract item #2 says the dispatcher must not capture `ExecutionContext` because `MRVTSC` handles EC restoration via the consumer-captured EC at `OnCompleted` time. That is true for `EC`. But `SynchronizationContext` and `TaskScheduler` are captured *separately* by the consumer's `await` (via `ValueTaskSourceOnCompletedFlags.UseSchedulingContext`), stored in `MRVTSC`'s awaiter, and applied at `SetResult` time — overriding whichever thread the dispatcher chose. In BDN's harness, the captured scheduler tied continuations back to a thread blocked on `Task.GetResult()`: classic sync-over-async deadlock when the per-message-processing consumer was added; with simple-drain consumer, a 3.5× iteration-time inflation that disappeared when `ConfigureAwait(false)` was added to the awaits. The dispatcher's stated purpose ("control where continuations run") is silently overridden by ambient `SynchronizationContext` / `TaskScheduler`. This is a real contract gap: the dispatcher promises something it cannot deliver under standard `IValueTaskSource` semantics.

**Mechanism B (cross-tenant AsyncLocal leak).** The dispatcher contract relies on `MRVTSC.RunInternal` to apply the consumer's captured EC to the continuation. `RunInternal` only fires when `MRVTSC._executionContext != null` — i.e., when the consumer's `OnCompleted` was called with `FlowExecutionContext` set. When suppressed (which the standard library permits), `MRVTSC.SetResult` invokes the continuation directly under the calling thread's current EC. On a long-lived dispatcher worker thread, that EC accumulates `AsyncLocal<T>` mutations across cb invocations whenever `MRVTSC` doesn't apply a captured EC of its own. A future continuation under that drifted EC inherits whatever pollution prior continuations left behind. Concretely: continuation A in iteration N sets `currentTenant.Value = "tenant-A"`; iteration N+1's continuation arrives with no captured EC, runs under the worker's drifted EC, observes `currentTenant.Value == "tenant-A"`. Silent data corruption / authorization leak in any code that uses `AsyncLocal` for request scoping.

Neither gap is fixable from inside an `IContinuationDispatcher` implementation. Both are mediated by `MRVTSC`'s flag-honoring behavior, which sits between the source and the dispatcher. The fix has to live at the source layer: `PipelyAwaiter`.

OpenTcp's `DispatchedValueTaskSource<T>` (a parallel project, design referenced in the conversation that produced this spec) demonstrates the correct pattern: capture `ExecutionContext` ourselves at `OnCompleted` time, strip both `FlowExecutionContext` and `UseSchedulingContext` from the flags forwarded to `MRVTSC`, and route the continuation invocation (with attached EC) through the dispatcher ourselves. This subsumes both mechanisms: the dispatcher genuinely controls routing (no scheduler override), and per-await EC propagates correctly regardless of `FlowExecutionContext`.

A previous partial measure (commit `020d770`) captured EC at the dispatcher's *construction* time and wrapped every `cb` invocation in `ExecutionContext.Run` on the worker thread. That defended against Mechanism B's worker-EC drift but used the wrong EC scope: the construction-time EC has no relation to the consumer's per-await EC, so AsyncLocal values from the consumer's await context don't propagate to the continuation. Reverted in `cea6d49`.

## Section 2 — Architecture changes

### Section 2.1 — `PipelyAwaiter<T>` field changes

**Added:**
- `private Action<object?>? _realContinuation` — the user's continuation captured at `OnCompleted` time, written before delegating to `_core.OnCompleted` so the storage is visible by the time `s_dispatch` reads it (including in the SetResult-fires-first race; see §4 publication ordering).
- `private object? _realState` — the user's continuation state, paired with `_realContinuation`.
- `private ExecutionContext? _capturedEC` — `ExecutionContext.Capture()` result if `FlowExecutionContext` was set at the consumer's `OnCompleted`; `null` otherwise (consumer suppressed flow → user explicitly opted out of EC propagation).

**Removed:**
- `internal T? _dispatchResult` — superseded. Signal sites now call `_core.SetResult(value)` directly on the producer thread; no need to stash the result for a deferred `SetResult` call.
- `internal Exception? _dispatchException` — superseded. Same reason.

The diagnostic counters (`_parkCount`, `_signalWonCount`, etc., used by `tests/Pipe.Benchmarks/PipeAdapter.cs`) are unaffected and remain. The packed-`int` state field, `ParkStash`, and all cycle-protocol fields are unaffected.

### Section 2.2 — `PipelyAwaiter<T>.OnCompleted` override

`PipelyAwaiter` currently passes `OnCompleted` through to `_core.OnCompleted` unchanged. Replace with:

```csharp
public void OnCompleted(
    Action<object?> continuation, object? state,
    short token, ValueTaskSourceOnCompletedFlags flags)
{
    // Capture EC on the awaiter thread (the consumer's), before MRVTSC's
    // barrier. Capturing inside s_dispatch instead would get the producer
    // thread's EC in the SetResult-fires-first race — wrong; would silently
    // leak AsyncLocal<T> values across requests.
    ExecutionContext? ec =
        (flags & ValueTaskSourceOnCompletedFlags.FlowExecutionContext) != 0
            ? ExecutionContext.Capture()
            : null;

    Volatile.Write(ref _capturedEC, ec);
    Volatile.Write(ref _realState, state);
    Volatile.Write(ref _realContinuation, continuation);

    // Strip both EC and SchedulingContext flags before forwarding. EC is
    // captured by us; leaving the flag on would have MRVTSC capture again
    // (wasteful, unused). SchedulingContext is stripped to honor the
    // dispatcher's contract — see §3 (Contract revisions) for the public-
    // contract phrasing.
    const ValueTaskSourceOnCompletedFlags suppressed =
        ValueTaskSourceOnCompletedFlags.FlowExecutionContext |
        ValueTaskSourceOnCompletedFlags.UseSchedulingContext;
    _core.OnCompleted(s_dispatch, this, token, flags & ~suppressed);
}
```

### Section 2.3 — Two new static delegates

Replacing the four `s_dispatchReadSetResult` / `s_dispatchReadSetException` / `s_dispatchFlushSetResult` / `s_dispatchFlushSetException` delegates currently in `Pipe.cs`:

```csharp
// Registered with _core via OnCompleted. Invoked inline by MRVTSC.SetResult
// on the producer thread (RCA=false), or — in the rare SetResult-fires-
// first race — queued to TP by MRVTSC and invoked there. In either case,
// reads the awaiter's stored continuation/state/EC and routes the work
// item through the dispatcher.
private static readonly Action<object?> s_dispatch = static state =>
{
    var awaiter = (PipelyAwaiter<T>)state!;
    var dispatcher = awaiter.GetDispatcher();
    dispatcher.UnsafeQueueUserWorkItem(s_invokeWithEc, awaiter);
};

// Invoked by the dispatcher's chosen thread (FastScheduler worker, TP
// worker for overflow, or TP for ThreadPoolContinuationDispatcher).
// Reads the awaiter's fields, applies the consumer-captured EC if any,
// and invokes the continuation.
private static readonly Action<object?> s_invokeWithEc = static state =>
{
    var awaiter = (PipelyAwaiter<T>)state!;
    var cont = awaiter._realContinuation;
    var st   = awaiter._realState;
    var ec   = awaiter._capturedEC;
    awaiter._realContinuation = null;
    awaiter._realState = null;
    awaiter._capturedEC = null;
    if (ec is not null)
        ExecutionContext.Run(ec, s_runContinuation, /* cont/state pair */);
    else
        cont!(st);
};
```

The `ExecutionContext.Run` arm needs to invoke `cont(st)` under the captured EC. Allocation-free patterns are available (e.g., a static `ContextCallback` plus a temporary boxed `(cont, st)` reference in a worker-thread-only field, similar to commit `020d770`'s pattern). Implementation detail; the spec invariant is "if `ec != null`, the continuation runs under that EC."

### Section 2.4 — Signal-site simplification

In `Pipe.cs` / `Pipe.Reader.cs` / `Pipe.Writer.cs`, the existing pattern at every signal site:

```csharp
_readAwaiter._dispatchResult = readResult;
DispatchVia(s_dispatchReadSetResult, _readAwaiter);
```

becomes:

```csharp
_readAwaiter._core.SetResult(readResult);
```

The `DispatchVia` helper, the four `s_dispatch*` delegates, and the `_dispatchResult` / `_dispatchException` field references are deleted. The producer thread calls `SetResult` (or `SetException`) directly; `MRVTSC.SetResult` invokes the registered `s_dispatch` callback inline (because `RunContinuationsAsynchronously = false`); `s_dispatch` packages the work item and hands it to the dispatcher; the dispatcher's chosen thread invokes `s_invokeWithEc`.

The signal sites in scope (per the existing dispatcher implementation plan):
- `Pipe.SignalReadAwaiterIfPending` — `SetResult` (data) and `SetException` (writer-completion-exception).
- `Pipe.SignalFlushIfBackpressureRelieved` → `DeliverFlushResult` — `SetResult` / `SetException`.
- `Pipe.SignalFlushAwaiterIfPending` → `DeliverFlushResult`.
- `Pipe.OnReadAwaiterTokenCancel` — `SetException` with `OperationCanceledException`.
- `Pipe.OnFlushAwaiterTokenCancel` — `SetException` with `OperationCanceledException`.
- `Pipe.Reader.CancelPendingRead` — `SetResult` (canceled `ReadResult`).
- `Pipe.Reader.ParkReadAwaiter` lost-wakeup throw path — `SetException`.
- `Pipe.Reader.ParkReadAwaiter` lost-cancel path — `SetResult` (canceled).
- `Pipe.Writer.CancelPendingFlush` — `SetResult` (canceled `FlushResult`).
- `Pipe.Writer.ParkFlushAwaiter` lost-wakeup throw path — `SetException`.
- `Pipe.Writer.ParkFlushAwaiter` lost-cancel path — `SetResult` (canceled).

Every site that currently performs `_dispatchResult/_dispatchException` stash + `DispatchVia` call becomes a direct `_core.SetResult/SetException` call.

### Section 2.5 — `RunContinuationsAsynchronously = false` is still load-bearing

The dispatcher-level routing relies on `s_dispatch` being invoked **inline** by `SetResult` (so we get a synchronous opportunity to package the work item and hand it to the dispatcher). With `RunContinuationsAsynchronously = true`, `MRVTSC` would queue `s_dispatch` to the ThreadPool itself before invoking it — adding a redundant TP hop and breaking the dispatcher's thread-routing guarantee. The `PipelyAwaiter` constructor's `_core.RunContinuationsAsynchronously = false` setting is unchanged.

Note: `RunContinuationsAsynchronously = false` controls only the `OnCompleted`-fires-first race (the dominant production path). For the `SetResult`-fires-first race, `MRVTSC` unconditionally queues the registered callback to the ThreadPool regardless of the flag — see §4.

## Section 3 — Contract revisions

### Section 3.1 — `IContinuationDispatcher.md` contract item #2

**Current text** (in `docs/IContinuationDispatcher.md`):

> The implementation MUST NOT capture or apply an `ExecutionContext`. Pipe relies on `ManualResetValueTaskSourceCore<T>`'s internal EC restoration (using the consumer-captured EC from `OnCompleted` time) to scope the continuation correctly. Adding EC manipulation in the dispatcher will leak the *producer's* EC into the continuation in the rare case where the consumer's `await` suppressed `FlowExecutionContext`.

**Revised text:**

> The implementation MUST NOT capture or apply an `ExecutionContext`. EC handling for `Pipe`'s awaitable continuations is performed by `PipelyAwaiter<T>`: it captures the consumer's `ExecutionContext` at `OnCompleted` time (per the consumer's `FlowExecutionContext` flag), passes the dispatcher a work item that carries the captured EC alongside the continuation, and applies the EC via `ExecutionContext.Run` at invoke time. The dispatcher is purely a thread router. Adding EC manipulation in the dispatcher would interfere with the source-side capture/apply protocol and is forbidden.

### Section 3.2 — `IContinuationDispatcher.md` "EC contract" section

The current section explains how `MRVTSC.RunInternal` handles EC. Update to explain the new flow (source-side capture, dispatcher-side application) and remove the now-obsolete discussion of `MRVTSC`'s internal EC flow.

The EC isolation guarantee Pipe gives the consumer is now: "regardless of the `IContinuationDispatcher` configured, your `await pipe.Reader.ReadAsync()` continuation runs under the `ExecutionContext` your code had at the `await` — same as standard `Task.Run` / `await` semantics — provided `FlowExecutionContext` was set at `OnCompleted` (the default). This guarantee is robust against worker-thread-EC drift in dispatchers with long-lived worker threads (e.g., `FastScheduler`)."

### Section 3.3 — `Pipe` public contract — scheduler / TaskScheduler

A new clause documenting the scheduler bypass:

> `Pipe`'s `Reader.ReadAsync` and `Writer.FlushAsync` continuations do **not** honor the consumer's captured `SynchronizationContext` or `TaskScheduler`. The continuation runs on the thread chosen by the configured `IContinuationDispatcher` (default: the .NET `ThreadPool` via `ThreadPoolContinuationDispatcher`). This is independent of the consumer's `ConfigureAwait(true|false)` choice. Consumers requiring continuation on a specific scheduler should either: (a) post explicitly via `SynchronizationContext.Post` / `TaskScheduler.FromCurrentSynchronizationContext().StartNew` after the `await`, or (b) wrap the awaitable in a `Task.Run` to capture context boundaries.

This is a deliberate contract choice, not an implementation accident. `Pipe` is a high-throughput primitive aimed at server-side workloads where consumer-side scheduler capture is not the desired routing. The explicit contract clause prevents surprise.

The spec for `IContinuationDispatcher` (`docs/IContinuationDispatcher.md`) should cross-reference this clause.

### Section 3.4 — Existing dispatcher spec (`2026-04-27-fast-scheduler-design.md`)

§6 "EC contract" needs revision to match the new flow. `FastScheduler` does not directly capture or apply EC; the EC is stored on the work item (passed via `awaiter` as state) and applied by the dispatcher's chosen thread when invoking `s_invokeWithEc`. The four-races correctness argument (§5) is unaffected — it concerns slot/dispatch-state synchronization, not EC.

## Section 4 — Publication ordering for the `SetResult`-fires-first race

The dominant production path is `OnCompleted` *before* `SetResult`: consumer awaits → MRVTSC stores `_continuation` and our delegate (`s_dispatch`) → producer signals → `SetResult` invokes `s_dispatch` inline → `s_dispatch` reads the awaiter's `_realContinuation`/`_realState`/`_capturedEC` (which were written by `OnCompleted` before `_core.OnCompleted` was called).

The rare path is `SetResult` *before* `OnCompleted`: producer signals (via `_core.SetResult(value)`) before the consumer has called `OnCompleted`. `_core` records the result; `_continuation` is `null`. Later, the consumer calls `OnCompleted`. `MRVTSC` sees a completed source and **unconditionally** queues the callback to the ThreadPool (this is `MRVTSC`'s behavior regardless of `RunContinuationsAsynchronously`).

Correctness in this race depends on **publication ordering**: `OnCompleted` writes `_capturedEC`/`_realState`/`_realContinuation` **before** calling `_core.OnCompleted`. The TP-dispatched `s_dispatch` therefore reads them post-publication. The `Volatile.Write` calls in `OnCompleted` make this explicit. There is no synchronization gap; the race is closed by the write order.

Cost: one extra TP hop in this rare race (versus the dominant path's inline invocation). Acceptable, and it matches OpenTcp's design choice. A test should pin this race explicitly (see §6).

## Section 5 — Allocation discipline

The OpenTcp pattern as designed is allocation-free per dispatch:
- The work item is the `PipelyAwaiter<T>` instance itself, passed as `object?` state. Reused per cycle (one awaiter per direction per pipe).
- Field writes/clears (`_realContinuation`, `_realState`, `_capturedEC`) are direct.
- The two static delegates (`s_dispatch`, `s_invokeWithEc`) are allocated once at type-init.

The only subtlety is the `ExecutionContext.Run` invocation in `s_invokeWithEc`. The `ContextCallback` signature is `void(object?)`. To pass `(cont, st)` without per-call allocation, the implementation can:
- Stash `cont`/`st` into worker-thread-only scratch fields on the awaiter and pass the awaiter as `state` to `ExecutionContext.Run` (similar to commit `020d770`'s `_runCb`/`_runState` pattern). Zero allocation.
- Or accept a single `(cont, st)` boxed pair per dispatch (small, but allocates).

The spec invariant is "no more than one allocation per dispatch in any path"; the implementation chooses the specific pattern.

## Section 6 — Required test surface

Tests pin observable behavior, not internal sequencing. Categorized by what they pin:

**EC propagation correctness:**

- C.1 — Per-await `ExecutionContext` is propagated to the continuation. Set `AsyncLocal<int>` to 42 before `await pipe.Reader.ReadAsync()`; assert `AsyncLocal<int>.Value == 42` in the continuation. (Mirrors existing `Pipe_WithFastScheduler_AsyncLocalFlowsToContinuation`; the new wiring must preserve this.)
- C.2 — Cross-cycle isolation: cycle 1's `AsyncLocal<int>` mutation does NOT leak to cycle 2's continuation. Set `AsyncLocal<int>` to 42 → first `await` → continuation mutates to 999 → second `await` (different cycle) → assert `AsyncLocal<int>.Value == 42` (or whatever the consumer's then-current value is; not 999).
- C.3 — `FlowExecutionContext` suppression: surround the `await` in an `ExecutionContext.SuppressFlow()` block. Continuation runs (no `ExecutionContext.Run` because `_capturedEC == null`); no AsyncLocal pollution from prior cb leaks in. (This is the Mechanism B regression test.)
- C.4 — Worker-thread `AsyncLocal` not observed in continuation: existing `Pipe_WithFastScheduler_DispatcherThreadAsyncLocal_NotObservedInContinuation` test pattern, must still pass under the new wiring.

**Scheduler bypass:**

- D.1 — A non-default `SynchronizationContext` set at the `await` site is not honored. Set a custom `SC` on the consumer thread; `await pipe.Reader.ReadAsync()`; assert the continuation runs on the dispatcher's chosen thread, NOT on the SC's thread. (This pins the "scheduler bypass" public-contract clause.)
- D.2 — A non-default `TaskScheduler` set via `Task.Factory.StartNew(..., TaskScheduler.FromCurrentSynchronizationContext())` is not honored. Same check as D.1 with `TaskScheduler` instead of `SC`.
- D.3 — Consumer's explicit `ConfigureAwait(true)` does not change behavior vs `ConfigureAwait(false)`. Both run on the dispatcher's chosen thread.

**`SetResult`-fires-first race:**

- E.1 — Producer signals before consumer has called `OnCompleted`. Consumer subsequently awaits; the result is delivered, the continuation runs, and EC is correctly applied (per §4 publication ordering). Stress with N iterations.

**Existing test surface from the dispatcher spec (§7) must continue to pass unchanged.**

## Section 7 — Out of scope

- **Dispatcher implementation changes.** `FastScheduler` and `ThreadPoolContinuationDispatcher` require zero source changes for this design. The dispatcher's role is unchanged: deliver a `(callback, state)` pair to its chosen thread.
- **`IContinuationDispatcher` interface signature.** Stays `void UnsafeQueueUserWorkItem(Action<object?>, object?)`. Work item carrying EC is passed via `state` (the awaiter), not as a richer signature.
- **The benchmark project's TEMP commits** (`c1ba6b9` / `99293c1` / `02a6dd1`). Reverting and restoring the throughput-shape benchmark belongs in the implementation plan, not this spec.
- **Performance characterization of the new wiring.** Pre-ship benchmark pass measures the impact; not part of the design.

## Section 8 — Spec references

- `docs/superpowers/specs/2026-04-27-fast-scheduler-design.md` — existing dispatcher spec. §5 (four-races correctness) unaffected; §6 (EC contract) needs the revision sketched in §3.4 above.
- `docs/superpowers/specs/2026-04-25-spsc-pipe-tripleBuffer-design.md` — `Pipe` design. §5 (awaiter state machine) needs the `_dispatchResult`/`_dispatchException` field references replaced with the new `_realContinuation`/`_realState`/`_capturedEC` triple. §6 (`IContinuationDispatcher` public contract) gets the §3.3 scheduler-bypass clause added.
- `docs/IContinuationDispatcher.md` — public-facing dispatcher contract. Item #2 and "EC contract" section revised per §3.1 / §3.2.
- OpenTcp's `DispatchedValueTaskSource<T>` — design pattern source. The `internal sealed`-with-explicit-contract argument from that design's commentary applies here: `PipelyAwaiter` is internal to `Pipely`; the public-facing `await pipe.Reader.ReadAsync()` is what consumers see, and the new public contract clause (§3.3) defines its semantics.

## Section 9 — Implementation note

A separate implementation plan (`docs/superpowers/plans/2026-04-28-spsc-awaiter-source-side-ec-capture-implementation.md`) will translate this design into a TDD-disciplined task list with file-by-file changes, test method names, and per-task commits. The implementation work touches `PipelyAwaiter.cs`, `Pipe.cs`, `Pipe.Reader.cs`, `Pipe.Writer.cs`, the existing dispatcher and pipe specs, the `IContinuationDispatcher.md` contract doc, and the test files. Estimated scope: ~80 lines of source changes (mostly *deletions* — the four `s_dispatch*` delegates and `DispatchVia` go away), ~150 lines of new tests, ~50 lines of spec/contract doc updates.
