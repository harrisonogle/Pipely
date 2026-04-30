# Pluggable continuation dispatch for Pipe

**Date:** 2026-04-27
**Status:** Plan, pending implementation

## Goal

Let users configure where async continuations run when `Pipe`'s parked awaiters are signaled. Default = ThreadPool (preserves current behavior). Optional override = user-supplied dispatcher (e.g., fast-scheduler to a dedicated thread on a pinned core, with TP fallback when busy).

Primary use case: custom Kestrel transports where the producer side is owned by the user but the consumer side (Kestrel) uses async/await on TP and is sensitive to wake-gap latency.

## Why now

- For high-frequency single-stream workloads, the TP wake gap (~390-500 ns at P50, multi-µs at P99) is a fixed per-event cost that scales linearly with frequency. At MHz rates, this can consume tens of percent of a CPU core just on wake overhead.
- Users who own one side and need ultra-low latency can already escape this on their own side via `TryRead`-busy-spin. They cannot escape it on the side they don't own.
- A pluggable dispatcher provides the escape hatch: redirect the *other* side's continuations to a thread the user keeps hot, without requiring changes to the consumer's code.

## API design

**Public interface (added to `Pipely`):**

```csharp
namespace Pipely;

public interface IContinuationDispatcher
{
    /// <summary>
    /// Queue the callback for invocation on a thread of the implementation's choosing.
    /// Mirrors the contract of <see cref="System.Threading.ThreadPool.UnsafeQueueUserWorkItem"/>:
    ///
    /// 1. The callback MUST be invoked exactly once.
    /// 2. The implementation MUST NOT capture or apply an ExecutionContext.
    ///    Pipe relies on MRVTSC's internal EC restoration (via the consumer-captured
    ///    EC from OnCompleted) to scope the continuation correctly. Adding EC manipulation
    ///    in the dispatcher will either leak the producer's EC into the continuation
    ///    (when FlowExecutionContext is absent on the original await) or introduce
    ///    wasteful capture/apply overhead.
    /// 3. The implementation MUST be thread-safe; concurrent calls from multiple
    ///    producer threads are permitted (one dispatcher may serve multiple pipes).
    /// 4. The implementation MUST NOT throw from UnsafeQueueUserWorkItem itself.
    ///    Failure to invoke the callback hangs the consumer's await indefinitely;
    ///    a dispatcher in a failed state should still attempt to invoke the callback
    ///    (e.g., fall back to TP) rather than throw.
    /// 5. Implementations SHOULD wrap the callback invocation in try/catch so a
    ///    throwing continuation doesn't kill the dispatcher's worker thread(s).
    /// </summary>
    void UnsafeQueueUserWorkItem(Action<object?> callback, object? state);
}
```

**Default implementation (internal):**

```csharp
internal sealed class ThreadPoolContinuationDispatcher : IContinuationDispatcher
{
    public static readonly ThreadPoolContinuationDispatcher Instance = new();

    public void UnsafeQueueUserWorkItem(Action<object?> callback, object? state)
        => ThreadPool.UnsafeQueueUserWorkItem(callback, state, preferLocal: false);
}
```

**Added to `PipeOptions`:**

```csharp
public IContinuationDispatcher? ContinuationDispatcher { get; init; }
// null = use ThreadPoolContinuationDispatcher.Instance (preserves current behavior)
// non-null = user-supplied, must obey the contract above
```

`init`-only — dispatcher choice is per-pipe-instance, immutable after construction.

## Mechanical change

The current code uses `ManualResetValueTaskSourceCore<T>` (MRVTSC) with `RunContinuationsAsynchronously = true`, which hardcodes "TP via UnsafeQueueUserWorkItem inside MRVTSC." We can't intercept that; we have to flip the flag.

Change: `_core.RunContinuationsAsynchronously = false`. With this, `_core.SetResult` invokes the continuation **inline on whatever thread calls SetResult**. We then route every SetResult call through our dispatcher, which picks the thread.

For the default user (no custom dispatcher), our default dispatcher forwards to `ThreadPool.UnsafeQueueUserWorkItem` — observable behavior identical to today's RCA=true setup.

## Allocation pattern (zero-alloc per dispatch)

Instead of packaging `(awaiter, value)` into a `Tuple<,>` per dispatch, stash the result on the awaiter itself:

```csharp
internal sealed class PipelyAwaiter<T> : IValueTaskSource<T>
{
    // ... existing fields ...

    // Held during the window between CAS-out-of-Pending and SetResult execution on
    // the dispatcher thread. Producer's signal CAS sets this before calling
    // dispatcher.UnsafeQueueUserWorkItem; the dispatched callback reads it and
    // calls _core.SetResult / SetException.
    internal T? _dispatchResult;
    internal Exception? _dispatchException;
}
```

Dispatch from the producer's signal path looks like:

```csharp
// _state CAS Pending→Inactive succeeded; producer-side counter Interlocked.Increment'd; etc.
_readAwaiter._dispatchResult = readResult;
var dispatcher = _options.ContinuationDispatcher ?? ThreadPoolContinuationDispatcher.Instance;
dispatcher.UnsafeQueueUserWorkItem(s_dispatchReadSetResult, _readAwaiter);

// Where s_dispatchReadSetResult is a static delegate:
private static readonly Action<object?> s_dispatchReadSetResult = static state =>
{
    var awaiter = (PipelyAwaiter<ReadResult>)state!;
    var result = awaiter._dispatchResult;
    awaiter._dispatchResult = default;
    awaiter._core.SetResult(result);
};
```

Zero allocations per dispatch (the static delegates are allocated once at type-init).

Symmetric pattern for `SetException` and for `PipelyAwaiter<FlushResult>`.

## Where the wrapping goes

Every site that currently calls `_core.SetResult(...)` or `_core.SetException(...)` needs to instead stash the value/exception on the awaiter and route through the dispatcher:

**`Pipe.cs`:**
- `SignalReadAwaiterIfPending` — SetResult (data) and SetException (writer-completion-exception)
- `SignalFlushIfBackpressureRelieved` → `DeliverFlushResult` — SetResult / SetException
- `SignalFlushAwaiterIfPending` → `DeliverFlushResult`
- `OnReadAwaiterTokenCancel` — SetException with OCE
- `OnFlushAwaiterTokenCancel` — SetException with OCE

**`Pipe.Reader.cs`:**
- `CancelPendingRead` — SetResult (canceled ReadResult)
- `ParkReadAwaiter` lost-wakeup throw path — SetException
- `ParkReadAwaiter` lost-cancel path — SetResult (canceled)

**`Pipe.Writer.cs`:**
- `CancelPendingFlush` — SetResult (canceled FlushResult)
- `ParkFlushAwaiter` lost-wakeup throw path — SetException
- `ParkFlushAwaiter` lost-cancel path — SetResult (canceled)

**Not changed:** the `ParkReadAwaiter`/`ParkFlushAwaiter` lost-wakeup *data* return paths, which return a sync `ValueTask<T>` directly via `BuildReadResult`/`BuildFlushResult`. No continuation involved, no dispatcher hop needed.

## ExecutionContext story (the critical part)

**EC capture happens on the consumer's thread, at `OnCompleted` time.** This is the consumer's EC at the moment its `await` suspended — which is the EC the continuation should resume under (Kestrel's `HttpContext`, logging scopes, ActivitySource, etc., all flow through here).

MRVTSC handles this internally: `OnCompleted` captures EC into `_core._executionContext` synchronously, before returning. By the time SetResult fires (whether from the producer's thread, our dispatcher's thread, or anywhere), the captured EC is already sealed in.

**EC restoration happens inside SetResult, on whatever thread SetResult runs on.** MRVTSC calls `ExecutionContext.RunInternal(captured, continuation, state)`, which:

1. Saves the calling thread's current EC into a local variable (for restoration later).
2. Applies the captured EC for the duration of the continuation.
3. Runs the continuation under the captured EC.
4. Restores the calling thread's saved EC.

The dispatcher thread's own EC is *preserved across* the call (saved/restored), not *used* by the continuation. The continuation runs under the consumer's captured EC. AsyncLocal flow works correctly.

**The hazard we MUST avoid:** if the dispatcher itself called `ExecutionContext.Capture()` (e.g., via `ThreadPool.QueueUserWorkItem` or `Task.Run`), the producer's EC would be captured and applied around the callback. In the rare case where the consumer's await suppressed `FlowExecutionContext`, MRVTSC's inner RunInternal wouldn't fire and the continuation would run under the producer's EC — leaking AsyncLocal values across requests. This is why contract item #2 prohibits EC capture in dispatcher implementations and why we name the method `UnsafeQueueUserWorkItem` (mirroring the BCL primitive that has the right semantics).

**SyncContext / TaskScheduler:** if the consumer's await captured a non-null SC or non-default TaskScheduler, MRVTSC's inner dispatch posts to those, overriding our dispatcher's thread choice. For ASP.NET Core / Kestrel this is not the typical case (SC is null by convention, and Kestrel uses `ConfigureAwait(false)` extensively). Document as a known constraint: "if your consumer's await captured a SyncContext, the continuation runs on that SC, not on the dispatcher's chosen thread."

## Default behavior preservation

With `RCA = false` + default dispatcher = `ThreadPoolContinuationDispatcher.Instance`:

- Continuation runs on a TP thread (via `UnsafeQueueUserWorkItem`)
- Producer doesn't block waiting for the continuation
- ExecutionContext / SyncContext capture & restore work identically to today

The only difference vs today's RCA=true path: there's one extra `IContinuationDispatcher.UnsafeQueueUserWorkItem` virtual call between MRVTSC's internal logic and the TP queue. Cost: one virtual dispatch (~1-2 ns) per signal. Should be invisible in benchmarks; verify with the throughput run.

## Tests

**Existing test suite must pass unchanged** with the default dispatcher. Run all 70 unit tests + stress harness.

**New tests (add to `Pipe.Tests`):**

1. Custom dispatcher receives the callback for each signal site:
   - Data-path SetResult: Read awaiter via `SignalReadAwaiterIfPending`, Flush awaiter via gated and unconditional flush signalers.
   - Token-cancel SetException: Read and Flush awaiters via `OnReadAwaiterTokenCancel` / `OnFlushAwaiterTokenCancel`.
   - CancelPending* SetResult: Read via `CancelPendingRead`, Flush via `CancelPendingFlush`.
   - Lost-wakeup paths: writer-completion-exception throw on Read side, reader-completion-exception throw on Flush side.

2. **EC flow correctness:**
   - `AsyncLocal<T>` set before `await Reader.ReadAsync()`; assert continuation observes the same value.
   - Custom dispatcher's hot thread sets its own `AsyncLocal` value; assert continuation does NOT see it (sees only consumer's captured value).
   - After continuation completes, assert dispatcher's thread `AsyncLocal` value is unchanged.
   - With `ConfigureAwait(false)`: same EC flow assertions still pass.

3. **EC contract guard:** a dispatcher implementation that incorrectly captures EC (uses `ThreadPool.QueueUserWorkItem` instead of `UnsafeQueueUserWorkItem`) is detectable — write a test where the producer thread sets a sentinel `AsyncLocal` value and the continuation asserts it is absent. Failing implementations would surface here.

4. **Exception in continuation:** dispatcher's worker thread survives a throwing continuation (verify fast-scheduler dispatcher implementation specifically).

5. **Version safety under rapid park/resume:** loop awaiting + processing many cycles with a custom dispatcher; assert no version-mismatch exceptions (validates dispatch hop doesn't allow stale version observations).

6. **Race: SetResult fires before OnCompleted called:** producer signals very quickly so `_completed=1` before consumer's `await` registers; verify continuation runs correctly with custom dispatcher.

## Spec update

Sections affected:

- §5 (Awaiter state machine): note that `_core.RunContinuationsAsynchronously = false`, with continuation dispatch routed through `IContinuationDispatcher` (default = TP).
- §5 (Signal paths): each SetResult/SetException site description should reference "dispatched via `IContinuationDispatcher.UnsafeQueueUserWorkItem`" instead of "queued to TP via MRVTSC".
- §6 (Options): document `PipeOptions.ContinuationDispatcher` and the contract.
- New invariant: "EC capture for await continuations occurs on the consumer's thread at `OnCompleted` time and is applied via MRVTSC's `RunInternal` inside `SetResult`. Dispatcher implementations must not interpose EC manipulation."

Probably a single PR ~30-50 lines of spec changes, alongside the code change.

## Effort estimate

- ~80 lines new code (interface, default impl, helper static delegates, dispatcher field, awaiter fields for stash)
- ~40 lines mechanical edits across `Pipe.cs`, `Reader.cs`, `Writer.cs` (replacing direct `_core.SetResult/SetException` calls with the dispatched form)
- ~150 lines of new tests
- Spec update ~30-50 lines
- Re-run unit tests, stress harness, latency benchmark to confirm no regression
- **Total: ~half-day of focused work, plus spec PR**

## Open questions

1. **Should we expose a built-in `FastScheduler` in the `Pipely` package?** Tempting (it's the canonical use case) but it has its own lifecycle (thread start/stop), tuning concerns (spin intensity), and exception handling. Better as a documentation example than a library type — keeps Pipe's public surface minimal. Users implementing the interface get full control.

2. **Should the default dispatcher be a singleton (`Instance`) or instantiated per pipe?** Singleton is fine — it's stateless. Cheaper than per-pipe construction.

3. **Is `Action<object?>` the right callback shape?** It matches `ThreadPool.UnsafeQueueUserWorkItem`'s signature exactly. Alternative would be a struct-based work-item interface (`IThreadPoolWorkItem`) which avoids the delegate allocation. But our `s_dispatch*` are static delegates (allocated once at type-init), so there's no per-dispatch delegate alloc. `Action<object?>` is fine and matches user expectations.

## Risks

1. **Subtle perf regression on default path.** One extra virtual call per dispatch. Should be ~1-2 ns; verify with throughput benchmark (currently ~74 µs / 1 MiB; expect within noise after change).

2. **Concurrency edge cases at signal sites.** Each signal-path SetResult site has its own CAS protocol (R5/R5b). The dispatcher hop happens *after* the CAS to Inactive succeeds, so it doesn't change the awaiter state machine. Should be safe; reviewer should verify that no signal path could call SetResult before the corresponding CAS succeeds.

3. **The stash field (`_dispatchResult` / `_dispatchException`) lifetime.** Set after CAS-out-of-Pending succeeds (only one signaler can win), read inside the dispatched callback. Between those two events, no other code path touches the field. Safe by the existing CAS protocol, but it's a new piece of state that the spec needs to mention.

4. **EC contract enforcement is convention, not enforcement.** A buggy custom dispatcher that captures EC won't be caught by the type system. Mitigated by: clear contract docs, EC guard test (#3 above), and documentation pointing users at `UnsafeQueueUserWorkItem` as the canonical implementation primitive.

## User-side example: fast-scheduler dispatcher

For reference, what a user implementing the canonical "fast-scheduler with TP fallback" dispatcher might look like:

```csharp
public sealed class FastScheduler : IContinuationDispatcher, IDisposable
{
    private const int Vacant = 0;
    private const int Busy = 1;
    private int _state;
    private Action<object?>? _pending;
    private object? _pendingState;
    private readonly Thread _thread;
    private volatile bool _shutdown;

    public FastScheduler()
    {
        _thread = new Thread(Loop) { IsBackground = true, Name = "Pipe FastScheduler" };
        _thread.Start();
    }

    public void UnsafeQueueUserWorkItem(Action<object?> callback, object? state)
    {
        if (Interlocked.CompareExchange(ref _state, Busy, Vacant) == Vacant)
        {
            _pendingState = state;
            Volatile.Write(ref _pending, callback);
        }
        else
        {
            // Hot thread busy — fall back to TP (UnsafeQueueUserWorkItem; no EC capture).
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
                try { cb(st); } catch { /* log */ }
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
```

About 30 lines for the basic shape. Real implementations would add: pinning to a specific core, configurable spin/yield/block backoff strategies, telemetry counters, lifecycle plumbing.
