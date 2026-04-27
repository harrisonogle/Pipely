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
2. **The implementation MUST NOT capture or apply an `ExecutionContext`.** SpscPipe relies on `ManualResetValueTaskSourceCore`'s internal EC restoration (using the consumer-captured EC from `OnCompleted` time) to scope the continuation correctly. Adding EC manipulation in the dispatcher will leak the *producer's* EC into the continuation in the rare case where the consumer's `await` suppressed `FlowExecutionContext`. For TP-based dispatchers, use `ThreadPool.UnsafeQueueUserWorkItem` (NOT the safe `QueueUserWorkItem` or `Task.Run`, both of which capture EC implicitly). For dedicated-thread dispatchers, hand off the callback delegate as-is.
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

## EC contract: why it matters

When the consumer's `await pipe.Reader.ReadAsync()` suspends, the runtime captures the consumer's current `ExecutionContext` (which carries `AsyncLocal<T>` values, ambient diagnostic state, etc.) **on the consumer's thread, at `OnCompleted` time**. That captured EC is stored on the awaiter's `_core` field.

When the producer eventually signals via SetResult, the inner `ExecutionContext.RunInternal(captured, callback, state)` saves the *current* (calling) thread's EC, applies the captured (consumer's) EC for the duration of the callback, then restores the calling thread's EC. The continuation runs under the *consumer's* EC — correctly observing the consumer's `AsyncLocal` values, not the producer's or the dispatcher thread's.

This works because the *captured* EC is what's applied — not whatever the dispatcher does at queue time. **A dispatcher that adds its own EC capture/apply layer (e.g., via `ThreadPool.QueueUserWorkItem` or `Task.Run`, both of which capture EC implicitly) introduces a subtle hazard:** in the rare case where the consumer's `await` suppressed `FlowExecutionContext` (so the awaiter has no captured EC to restore), the dispatcher's captured EC — *the producer's EC* — would be applied to the continuation. This silently leaks producer-side `AsyncLocal` values into the consumer's continuation.

For typical `await` (with default flags), this hazard is masked by the inner `RunInternal`. But the contract forbids it because:
1. There's no benefit — MRVTSC will override the EC anyway.
2. The capture-apply overhead is wasted CPU.
3. The exotic-edge-case correctness gap is real and hard to debug.

The fix is the same as the recommendation: **dispatchers must use `ThreadPool.UnsafeQueueUserWorkItem` or hand off the callback as-is to a dedicated thread**. The naming of the interface method (`UnsafeQueueUserWorkItem`) communicates this contract by convention.

## Where to look in the code

| What | Where |
|------|-------|
| Interface + default impl | `src/SpscPipelines/IContinuationDispatcher.cs` |
| Options field | `src/SpscPipelines/SpscPipeOptions.cs` |
| Stash fields on awaiter | `src/SpscPipelines/SpscAwaiter.cs` (`_dispatchResult`, `_dispatchException`) |
| Static dispatch delegates + helper | `src/SpscPipelines/SpscPipe.cs` (`s_dispatch*`, `DispatchVia`) |
| Signal-path SetResult/SetException sites | All in `SpscPipe.cs`, `SpscPipe.Reader.cs`, `SpscPipe.Writer.cs` — every `_core.SetResult`/`SetException` runs inside one of the four `s_dispatch*` static delegates |
| Tests (EC flow, isolation, contract guard, exception, version safety) | `tests/SpscPipe.Tests/SpscPipeContinuationDispatcherTests.cs` |

## Spec references

The mechanism is normatively specified in:

- `docs/superpowers/specs/2026-04-25-spsc-pipe-tripleBuffer-design.md`
  - **§5 "Continuation dispatch"** — the stash-and-dispatch protocol and shorthand convention used in the awaiter pseudocode.
  - **Invariant I16** — EC capture / restoration discipline.
  - **Rule R10** — continuation dispatch protocol (CAS-win → stash → dispatch ordering at every signal site).
  - **§6 IContinuationDispatcher** — public-surface description and the five-item contract.

The implementation plan and design rationale (with empirical wake-gap measurements that motivated the feature) are in:

- `docs/superpowers/plans/2026-04-27-pluggable-continuation-dispatch.md`
