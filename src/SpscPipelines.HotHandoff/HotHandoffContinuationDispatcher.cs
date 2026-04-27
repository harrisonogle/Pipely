using SpscPipelines;

namespace SpscPipelines.HotHandoff;

/// <summary>
/// <see cref="IContinuationDispatcher"/> implementation that routes the first hop of
/// each SpscPipe continuation to a dedicated busy-spinning thread, with ThreadPool
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
/// <c>docs/superpowers/specs/2026-04-27-hot-handoff-dispatcher-design.md</c> for the
/// full spec and four-races correctness argument.
/// </para>
/// </summary>
public sealed class HotHandoffContinuationDispatcher : IContinuationDispatcher, IDisposable
{
    private const int Busy              = 1;
    private const int ShutdownRequested = 2;

    // Tunable. Open per spec §9 — closed by the benchmark project's measurement loop.
    private const int SpinIterations = 10;

    private int _state;
    private Action<object?>? _pending;
    private object? _pendingState;
    private readonly Thread _thread;

    public HotHandoffContinuationDispatcher()
    {
        _thread = new Thread(Loop)
        {
            IsBackground = true,
            Name = "SpscPipe HotHandoff",
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
