using System.IO.Pipelines;

namespace SpscPipe;

// §6 Writer facade.  PipeWriter implementation bound to an SpscPipe.
// All methods are stubbed in checkpoint 1; behavior lands in later
// checkpoints (§6.1/§6.2/§6.3/§6.4 in checkpoint 2; §6.5/§8.2/§8.4 in
// checkpoint 3; §6.6/§6.7 in checkpoint 4).
internal sealed class SpscPipeWriter : PipeWriter
{
    private readonly SpscPipe _pipe;

    internal SpscPipeWriter(SpscPipe pipe) => _pipe = pipe;

    // §6.2
    public override void Advance(int bytes)
        => throw new NotImplementedException();

    // §6.7
    public override void CancelPendingFlush()
        => throw new NotImplementedException();

    // §6.6
    public override void Complete(Exception? exception = null)
        => throw new NotImplementedException();

    // §6.3
    public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
        => throw new NotImplementedException();

    // §6.1
    public override Memory<byte> GetMemory(int sizeHint = 0)
        => throw new NotImplementedException();

    public override Span<byte> GetSpan(int sizeHint = 0)
        => throw new NotImplementedException();
}
