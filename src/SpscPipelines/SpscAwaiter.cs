using System.Threading;
using System.Threading.Tasks.Sources;

namespace SpscPipelines;

internal sealed class SpscAwaiter<T> : IValueTaskSource<T>
{
    // RCA = false: with pluggable IContinuationDispatcher, every signal-path SetResult/SetException
    // is routed through the configured dispatcher (default = TP via ThreadPoolContinuationDispatcher).
    // Set once at construction; ManualResetValueTaskSourceCore<T>.Reset does NOT reset this flag,
    // so it remains correct across park cycles. See spec §5 "Continuation dispatch" and R10.
    public ManualResetValueTaskSourceCore<T> _core = new() { RunContinuationsAsynchronously = false };
    public int _state;
    public CancellationTokenRegistration _ctr;
    public CancellationToken _token;

    // Pattern 2 stash (used by SpscAwaiter<ReadResult>; ignored by SpscAwaiter<FlushResult>).
    public BufferSegment? _stashHead;
    public int _stashHeadIdx;
    public BufferSegment? _stashTail;
    public int _stashTailIdx;

    // Dispatch stash (R10). Set by the actor that wins CAS Pending→Inactive (signaler / canceler /
    // token callback / lost-wakeup re-check / lost-cancel re-check) AFTER the CAS but BEFORE the
    // call to IContinuationDispatcher.UnsafeQueueUserWorkItem. Read inside the dispatched callback,
    // which clears the field (sets it back to default/null) and then invokes _core.SetResult /
    // _core.SetException. Because exactly one actor wins the CAS per park cycle (I11), there is no
    // concurrent writer; the read inside the callback synchronizes with that single writer through
    // the dispatcher's own happens-before edge from queue-call to dequeued callback.
    // Clearing on read releases references to the just-delivered ReadResult (and its segments) /
    // exception so the awaiter does not retain those references between park cycles.
    public T? _dispatchResult;
    public Exception? _dispatchException;

    public const int Inactive   = 0b00;
    public const int Pending    = 0b01;
    public const int StateMask  = 0b01;
    public const int CancelFlag = 0b10;

    // Diagnostic counters (Interlocked-incremented at each CAS resolution site).
    // ParkCount = sum of all resolution counters + any leftover unresolved parks (should be 0
    // at end of any well-behaved run). Cost: ~5-10 ns per increment, only on park/signal paths
    // that are off the synchronous hot path.
    public long _parkCount;
    public long _signalWonCount;
    public long _tokenCancelWonCount;
    public long _cancelPendingWonCount;
    public long _lostWakeupResolvedCount;
    public long _lostCancelResolvedCount;

    public short Version => _core.Version;
    public T GetResult(short token) => _core.GetResult(token);
    public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);
    public void OnCompleted(Action<object?> c, object? s, short token, ValueTaskSourceOnCompletedFlags f)
        => _core.OnCompleted(c, s, token, f);
}
