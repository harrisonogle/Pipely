using System.Threading;
using System.Threading.Tasks.Sources;

namespace SpscPipelines;

internal sealed class SpscAwaiter<T> : IValueTaskSource<T>
{
    public ManualResetValueTaskSourceCore<T> _core = new() { RunContinuationsAsynchronously = true };
    public int _state;
    public CancellationTokenRegistration _ctr;
    public CancellationToken _token;

    // Pattern 2 stash (used by SpscAwaiter<ReadResult>; ignored by SpscAwaiter<FlushResult>).
    public BufferSegment? _stashHead;
    public int _stashHeadIdx;
    public BufferSegment? _stashTail;
    public int _stashTailIdx;

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
