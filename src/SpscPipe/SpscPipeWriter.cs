using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Tasks.Sources;

namespace SpscPipe;

// PipeWriter subclass backing SpscPipe.Writer.  Spec §6.
internal sealed class SpscPipeWriter : PipeWriter
{
    private readonly SpscPipe _pipe;

    // Unpublished chain (writer-local per §4.3).
    private Segment? _unpublishedHead;
    private Segment? _unpublishedTail;
    private long _unpublishedBytes;

    // Active buffer state (§4.3).
    private BufferHolder? _activeBufferHolder;
    private int _activeBufferWritten;
    private int _activeBufferCapacity;
    private int _unflushedStart;

    // Byte accounting (§4.3).
    private long _bytesWritten;

    // Flush awaiter (§4.3, §8.4).
    private ManualResetValueTaskSourceCore<FlushResult> _flushAwaiter;
    private CancellationTokenRegistration _flushCtr;

    internal SpscPipeWriter(SpscPipe pipe)
    {
        _pipe = pipe;
        _flushAwaiter = new ManualResetValueTaskSourceCore<FlushResult>
        {
            RunContinuationsAsynchronously = true,
        };
    }

    // §6.1
    public override Memory<byte> GetMemory(int sizeHint = 0)
        => throw new NotImplementedException("§6.1 — checkpoint 2");

    public override Span<byte> GetSpan(int sizeHint = 0) => GetMemory(sizeHint).Span;

    // §6.2
    public override void Advance(int bytes)
        => throw new NotImplementedException("§6.2 — checkpoint 2");

    // §6.3
    public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
        => throw new NotImplementedException("§6.3 — checkpoint 2/3");

    // §6.6
    public override void Complete(Exception? exception = null)
        => throw new NotImplementedException("§6.6 — checkpoint 4");

    // §6.7
    public override void CancelPendingFlush()
        => throw new NotImplementedException("§6.7 — checkpoint 3");
}
