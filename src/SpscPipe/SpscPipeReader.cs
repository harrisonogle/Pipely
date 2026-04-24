using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Tasks.Sources;

namespace SpscPipe;

// PipeReader subclass backing SpscPipe.Reader.  Spec §7.
// Implements IValueTaskSource<ReadResult> to bridge ReadSignal (the
// awaiter payload) to ReadResult (what the caller expects).  See §8.7.
internal sealed class SpscPipeReader : PipeReader, IValueTaskSource<ReadResult>
{
    private readonly SpscPipe _pipe;

    // Reader-local state (§4.4).
    private Segment? _head;
    private int _headConsumedOffset;
    private long _examinedPosition;
    private long _bytesRead;

    private bool _readInProgress;
    private ReadOnlySequence<byte> _lastReturnedBuffer;

    // Read awaiter carries ReadSignal (§4.4 / §8.7).
    private ManualResetValueTaskSourceCore<ReadSignal> _readAwaiter;
    private CancellationTokenRegistration _readCtr;

    internal SpscPipeReader(SpscPipe pipe)
    {
        _pipe = pipe;
        _readAwaiter = new ManualResetValueTaskSourceCore<ReadSignal>
        {
            RunContinuationsAsynchronously = true,
        };
    }

    // §7.1
    public override ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
        => throw new NotImplementedException("§7.1 — checkpoint 2/3");

    public override bool TryRead(out ReadResult result)
        => throw new NotImplementedException("§7.1 — checkpoint 2");

    // §7.3
    public override void AdvanceTo(SequencePosition consumed)
        => AdvanceTo(consumed, consumed);

    public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
        => throw new NotImplementedException("§7.3 — checkpoint 2");

    // §7.4
    public override void Complete(Exception? exception = null)
        => throw new NotImplementedException("§7.4 — checkpoint 4");

    // §7.5
    public override void CancelPendingRead()
        => throw new NotImplementedException("§7.5 — checkpoint 3");

    // IValueTaskSource<ReadResult> bridge (§8.7).
    ReadResult IValueTaskSource<ReadResult>.GetResult(short token)
        => throw new NotImplementedException("§8.7 — checkpoint 3");

    ValueTaskSourceStatus IValueTaskSource<ReadResult>.GetStatus(short token)
        => _readAwaiter.GetStatus(token);

    void IValueTaskSource<ReadResult>.OnCompleted(Action<object?> continuation,
        object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _readAwaiter.OnCompleted(continuation, state, token, flags);
}
