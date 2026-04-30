
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
/// State 2 (<c>ShutdownRequested</c> set, <c>Busy</c> clear — i.e., Vacant + ShutdownRequested)
/// is terminal: no Dispatch can claim, the loop exits, no callback is dropped. See
/// <c>docs/superpowers/specs/2026-04-27-fast-scheduler-design.md</c> for the
/// full spec and four-races correctness argument.
/// </para>
/// </summary>
public sealed class FastScheduler : IContinuationDispatcher, IDisposable
{
    private const int Vacant            = 0;
    private const int Busy              = 1;
    private const int ShutdownRequested = 2;

    // Tunable. Open per spec §9 — closed by the benchmark project's measurement loop.
    private const int SpinIterations = 10;

    private int _state;
    private Action<object?>? _pending;
    private object? _pendingState;
    private readonly Thread _thread;

    // Diagnostic-only telemetry (spec §10 "may be added if measurements indicate
    // a need"). Cumulative since dispatcher construction. Not part of the public
    // contract; exposed via internal accessors for the benchmark project.
    private long _slotDispatchedCount;
    private long _tpOverflowedCount;

    /// <summary>
    /// Cumulative count of dispatches whose slot CAS won and ran on the dedicated
    /// worker thread. Internal — for benchmark diagnostics only.
    /// </summary>
    internal long SlotDispatchedCount => Interlocked.Read(ref _slotDispatchedCount);

    /// <summary>
    /// Cumulative count of dispatches whose slot CAS lost and were forwarded to
    /// <see cref="ThreadPool.UnsafeQueueUserWorkItem(Action{object?}, object?, bool)"/>.
    /// Internal — for benchmark diagnostics only.
    /// </summary>
    internal long TpOverflowedCount => Interlocked.Read(ref _tpOverflowedCount);

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
        // Conditional claim: succeeds only when state == Vacant (no Busy, no ShutdownRequested).
        if (Interlocked.CompareExchange(ref _state, Busy, Vacant) == Vacant)
        {
            _pendingState = state;                            // plain
            Interlocked.Exchange(ref _pending, callback);     // full fence: publishes both fields
            Interlocked.Increment(ref _slotDispatchedCount);  // diagnostic — see §10 / SlotDispatchedCount
            return;
        }

        // Slot busy or shutdown — fall through to TP. UnsafeQueueUserWorkItem
        // (not QueueUserWorkItem or Task.Run) — IContinuationDispatcher contract item #2.
        ThreadPool.UnsafeQueueUserWorkItem(callback, state, preferLocal: false);
        Interlocked.Increment(ref _tpOverflowedCount);        // diagnostic — see §10 / TpOverflowedCount
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
                // Fenced read of state via CAS-with-self: comparand == new-value, so the
                // store is a no-op (writes the same value when state == Vacant; doesn't write
                // otherwise). The return value is the read with full memory ordering.
                // State == ShutdownRequested (== 2: Vacant + ShutdownRequested bit) is terminal.
                var s = Interlocked.CompareExchange(ref _state, Vacant, Vacant);
                if (s == ShutdownRequested) return;
                Thread.SpinWait(SpinIterations);
            }
        }
    }

    public void Dispose()
    {
        Interlocked.Or(ref _state, ShutdownRequested);

        // If Dispose is called from within a callback the dispatcher routed
        // (i.e., the current thread IS the worker thread), Joining would
        // self-deadlock. The worker observes ShutdownRequested when the cb
        // returns to Loop and exits naturally. In that case, Dispose returns
        // before the worker terminates; the dispatcher is functionally
        // shutdown either way (every subsequent Dispatch CAS sees state ≥ 2
        // and routes to TP).
        if (Thread.CurrentThread != _thread)
            _thread.Join();
    }
}
