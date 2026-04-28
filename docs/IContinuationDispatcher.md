# `IContinuationDispatcher`

Pluggable continuation dispatch for `SpscPipe`. Lets users override where async continuations run when a parked awaiter is signaled.

## Why this exists

`SpscPipe` is a lock-free single-producer/single-consumer pipe. When the consumer awaits an empty pipe, its `await` registers a continuation and suspends. When the producer signals (after its next flush), the continuation runs.

By default, the runtime queues that continuation to the .NET ThreadPool. There's a fixed per-signal cost — typically **a few hundred ns at P50, multiple µs at P99** — for the TP scheduler to dispatch the continuation onto a worker thread, including waking the worker from `LowLevelLifoSemaphore.Wait` if it had parked between work items.

For high-frequency producer/consumer pairs (say, MHz signal rates), this fixed cost scales linearly:

> 100 ns wake gap × 1,000,000 signals/sec = 10% of one CPU core

That's significant. The TP architecture is designed for shared, continuously busy workloads where the per-event cost amortizes well; a single low-frequency-bursty stream is the worst case for it, and a single high-frequency stream pays the fixed cost on every event.

`IContinuationDispatcher` is the escape hatch. By supplying a custom dispatcher, users can route signaled continuations to a thread *they* keep hot — typically a dedicated thread pinned to a specific core, busy-spinning or with adaptive backoff — completely bypassing the TP for the wake hop. Users who don't opt in keep the default TP behavior unchanged.

## API

```csharp
public interface IContinuationDispatcher
{
    void UnsafeQueueUserWorkItem(Action<object?> callback, object? state);
}

// Set on SpscPipeOptions; null = default ThreadPool dispatch (current behavior).
public sealed class SpscPipeOptions
{
    public IContinuationDispatcher? ContinuationDispatcher { get; init; }
}
```

The method is named `UnsafeQueueUserWorkItem` deliberately — it mirrors `ThreadPool.UnsafeQueueUserWorkItem`'s contract, signaling that the caller is responsible for `ExecutionContext` handling. See the **EC contract** section below.

## Contract (must be followed by all implementations)

1. **The callback MUST be invoked exactly once.** Failing to invoke it hangs the consumer's `await` indefinitely.
2. **The implementation MUST NOT capture or apply an `ExecutionContext`.** EC handling for `SpscPipe`'s awaitable continuations is performed by `SpscAwaiter<T>`: it captures the consumer's `ExecutionContext` at `OnCompleted` time (per the consumer's `FlowExecutionContext` flag), passes the dispatcher a work item that carries the captured EC alongside the continuation (via fields on the awaiter — the awaiter is the `state` argument), and applies the EC via `ExecutionContext.Run` at invoke time. The dispatcher is purely a thread router. Adding EC manipulation in the dispatcher would interfere with the source-side capture/apply protocol and is forbidden. For TP-based dispatchers, use `ThreadPool.UnsafeQueueUserWorkItem` (NOT the safe `QueueUserWorkItem` or `Task.Run`, both of which capture EC implicitly). For dedicated-thread dispatchers, hand off the callback delegate as-is.
3. **The implementation MUST be thread-safe.** A single dispatcher instance may be shared across multiple pipes; `UnsafeQueueUserWorkItem` may be called concurrently from multiple producer threads.
4. **The implementation MUST NOT throw.** A throwing dispatcher will crash the producer's signal path. A dispatcher in a failed state should still attempt to invoke the callback (e.g., fall back to TP) rather than throw.
5. **The implementation SHOULD wrap the callback invocation in `try/catch`** so a throwing continuation doesn't kill the dispatcher's worker thread.

## Default behavior

If `SpscPipeOptions.ContinuationDispatcher` is `null` (default), SpscPipe uses an internal `ThreadPoolContinuationDispatcher` that forwards every callback to `ThreadPool.UnsafeQueueUserWorkItem(callback, state, preferLocal: false)`. This preserves the observable behavior of prior SpscPipe versions: continuations run on TP worker threads. There is no measurable performance regression vs. the prior `MRVTSC.RunContinuationsAsynchronously = true` path (one extra virtual call, ~1-2 ns per signal).

## Usage: hot-handoff dispatcher

The canonical custom dispatcher routes the FIRST hop of each continuation to a dedicated thread (kept hot via busy-spin or adaptive backoff), with TP fallback when the dedicated thread is already running another continuation. This avoids the TP wake-gap for the common case while preserving correctness under burst.

```csharp
public sealed class HotHandoffContinuationDispatcher : IContinuationDispatcher, IDisposable
{
    private const int Vacant = 0;
    private const int Busy = 1;
    private int _state;
    private Action<object?>? _pending;
    private object? _pendingState;
    private readonly Thread _thread;
    private volatile bool _shutdown;

    public HotHandoffContinuationDispatcher()
    {
        _thread = new Thread(Loop) { IsBackground = true, Name = "SpscPipe HotHandoff" };
        _thread.Start();
    }

    public void UnsafeQueueUserWorkItem(Action<object?> callback, object? state)
    {
        // Try to claim the dedicated thread's single mailbox slot.
        if (Interlocked.CompareExchange(ref _state, Busy, Vacant) == Vacant)
        {
            _pendingState = state;
            Volatile.Write(ref _pending, callback);
        }
        else
        {
            // Mailbox occupied — fall back to TP. Use the *unsafe* variant to satisfy
            // contract item #2 (no EC capture).
            ThreadPool.UnsafeQueueUserWorkItem(callback, state, preferLocal: false);
        }
    }

    private void Loop()
    {
        while (!_shutdown)
        {
            var cb = Volatile.Read(ref _pending);
            if (cb != null)
            {
                var st = _pendingState;
                _pending = null;
                _pendingState = null;
                try { cb(st); } catch { /* log; satisfies contract item #5 */ }
                Volatile.Write(ref _state, Vacant);
            }
            else
            {
                Thread.SpinWait(10);  // tunable: trade CPU for wake latency
            }
        }
    }

    public void Dispose()
    {
        _shutdown = true;
        _thread.Join();
    }
}

// Usage:
using var dispatcher = new HotHandoffContinuationDispatcher();
using var pipe = new SpscPipe(new SpscPipeOptions { ContinuationDispatcher = dispatcher });
```

### Tuning the "hotness" knob

The dispatcher's `Loop` controls how aggressively the dedicated thread stays hot — i.e., how quickly it can dispatch a freshly-queued callback. Tradeoffs:

- **`Thread.SpinWait(small)` only**: ~100% core utilization, sub-µs dispatch latency from queue to running. Best when you have spare cores to dedicate.
- **Spin briefly, then `Thread.Yield()`**: less CPU, slightly higher dispatch latency. Reasonable middle ground.
- **Spin briefly, then block on a semaphore**: lowest CPU, but reintroduces the kernel-wake cost we set out to avoid. Only useful at very low signal rates.

Pin the thread to a dedicated core for tightest cache-locality (P/Invoke `sched_setaffinity` on Linux, `Thread.BeginThreadAffinity` + `ProcessorAffinity` on Windows). Avoid colocating with the producer's busy-spinning thread (if any) — they should be on separate physical cores to prevent SMT contention.

### Production-grade enhancements not shown above

- **Multi-slot mailbox** (e.g., a small lock-free MPSC queue) so bursts don't immediately spill to TP.
- **Adaptive backoff** that ramps spin intensity up/down based on observed signal rate.
- **Telemetry counters** (dispatched, fell-back-to-TP, mailbox depth) for tuning visibility.
- **Lifecycle plumbing** for clean shutdown that drains in-flight work.

## EC contract: how it works (source-side capture)

When the consumer's `await pipe.Reader.ReadAsync()` suspends, **`SpscAwaiter<T>.OnCompleted`** captures the consumer's current `ExecutionContext` (which carries `AsyncLocal<T>` values, ambient diagnostic state, etc.) on the consumer's thread, at the moment of the `await`. The capture is gated by the `ValueTaskSourceOnCompletedFlags.FlowExecutionContext` flag the consumer's await machinery passed in: if the flag is set (the default), `_capturedEC = ExecutionContext.Capture()`; if the flag is cleared (the consumer is inside an `ExecutionContext.SuppressFlow()` block), `_capturedEC = null` — the consumer has explicitly opted out of EC propagation.

`SpscAwaiter<T>` then writes the consumer-supplied `(continuation, state)` pair to its `_realContinuation` / `_realState` fields and forwards a different `(callback, state)` pair to `_core.OnCompleted` — specifically, its own internal `s_dispatch` delegate paired with `this` (the awaiter). It also strips both `FlowExecutionContext` and `UseSchedulingContext` from the flags forwarded to `_core.OnCompleted`:

- **`FlowExecutionContext` is stripped** because `SpscAwaiter` already captured the EC; leaving the flag on would have `MRVTSC` capture again (redundant).
- **`UseSchedulingContext` is stripped** because `SpscPipe`'s public contract is that the configured `IContinuationDispatcher` controls continuation routing — the consumer's captured `SynchronizationContext` / `TaskScheduler` is intentionally ignored.

When the producer signals (`_core.SetResult` / `_core.SetException`), `MRVTSC` invokes the registered `s_dispatch` callback inline on the producer's thread (because `RunContinuationsAsynchronously = false`). `s_dispatch` calls `dispatcher.UnsafeQueueUserWorkItem(s_invokeWithEc, awaiter)` — handing the work item to the configured dispatcher. The dispatcher's chosen thread invokes `s_invokeWithEc`, which:

1. Reads and clears `_realContinuation`, `_realState`, `_capturedEC`.
2. If `_capturedEC` is non-null, calls `ExecutionContext.Run(_capturedEC, s_runContinuation, awaiter)` — applying the consumer's captured EC for the duration of the continuation invocation; the dispatcher thread's pre-call EC is automatically saved and restored by `ExecutionContext.Run`.
3. If `_capturedEC` is null (consumer suppressed flow), invokes the continuation directly on the dispatcher's chosen thread — the consumer explicitly opted out of EC propagation and accepts whatever EC that thread has.

The EC isolation guarantee `SpscPipe` gives the consumer is therefore: **regardless of the `IContinuationDispatcher` configured, your `await pipe.Reader.ReadAsync()` continuation runs under the `ExecutionContext` your code had at the `await` — same as standard `Task.Run` / `await` semantics — provided `FlowExecutionContext` was set at `OnCompleted` (the default). This guarantee is robust against worker-thread-EC drift in dispatchers with long-lived worker threads (e.g., `HotHandoffContinuationDispatcher`).**

A dispatcher that captures EC itself (e.g., uses the EC-capturing `ThreadPool.QueueUserWorkItem` instead of the recommended `UnsafeQueueUserWorkItem`) does NOT break the consumer's EC guarantee — `s_invokeWithEc` applies the consumer-captured EC after the dispatcher's hop — but it DOES introduce wasteful capture/apply overhead and violates contract item #2.

## Scheduler bypass

`SpscPipe`'s `Reader.ReadAsync` and `Writer.FlushAsync` continuations do **not** honor the consumer's captured `SynchronizationContext` or `TaskScheduler`. The continuation runs on the thread chosen by the configured `IContinuationDispatcher` (default: the .NET `ThreadPool` via `ThreadPoolContinuationDispatcher`). This is independent of the consumer's `ConfigureAwait(true|false)` choice — both produce identical observable behavior. Consumers requiring continuation on a specific scheduler should either:

- (a) post explicitly via `SynchronizationContext.Post` / `TaskScheduler.FromCurrentSynchronizationContext().StartNew` after the `await`, or
- (b) wrap the awaitable in a `Task.Run` to capture context boundaries.

This is a deliberate contract choice, not an implementation accident. `SpscPipe` is a high-throughput primitive aimed at server-side workloads where consumer-side scheduler capture is not the desired routing. The explicit contract clause prevents surprise.

## Where to look in the code

| What | Where |
|------|-------|
| Interface + default impl | `src/SpscPipelines/IContinuationDispatcher.cs` |
| Options field | `src/SpscPipelines/SpscPipeOptions.cs` |
| Source-side EC capture (consumer-thread) | `src/SpscPipelines/SpscAwaiter.cs` (`OnCompleted` override; `_realContinuation` / `_realState` / `_capturedEC` fields) |
| Source-side EC application (dispatcher-thread) | `src/SpscPipelines/SpscAwaiter.cs` (`s_dispatch`, `s_invokeWithEc`, `s_runContinuation` static delegates) |
| Signal-path SetResult/SetException sites | All in `SpscPipe.cs`, `SpscPipe.Reader.cs`, `SpscPipe.Writer.cs` — direct `_core.SetResult` / `_core.SetException` calls; the dispatcher hop is encapsulated inside `SpscAwaiter`'s `OnCompleted` + `s_dispatch` flow |
| Tests (EC flow, isolation, scheduler bypass, race) | `tests/SpscPipe.Tests/SpscPipeContinuationDispatcherTests.cs` |

## Spec references

The mechanism is normatively specified in:

- `docs/superpowers/specs/2026-04-25-spsc-pipe-tripleBuffer-design.md`
  - **§5 "Continuation dispatch"** — the stash-and-dispatch protocol and shorthand convention used in the awaiter pseudocode.
  - **Invariant I16** — EC capture / restoration discipline.
  - **Rule R10** — continuation dispatch protocol (CAS-win → stash → dispatch ordering at every signal site).
  - **§6 IContinuationDispatcher** — public-surface description and the five-item contract.

The implementation plan and design rationale (with empirical wake-gap measurements that motivated the feature) are in:

- `docs/superpowers/plans/2026-04-27-pluggable-continuation-dispatch.md`
