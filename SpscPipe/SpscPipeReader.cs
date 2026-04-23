using System.IO.Pipelines;

namespace SpscPipe;

// §7 Reader facade.  PipeReader implementation bound to an SpscPipe.
// All methods are stubbed in checkpoint 1; behavior lands in later
// checkpoints (§7.1/§7.2/§7.3 in checkpoint 2; §8.3/§8.7 in checkpoint 3;
// §7.4/§7.5 in checkpoint 4).
internal sealed class SpscPipeReader : PipeReader
{
    private readonly SpscPipe _pipe;

    internal SpscPipeReader(SpscPipe pipe) => _pipe = pipe;

    // §7.3
    public override void AdvanceTo(SequencePosition consumed)
        => throw new NotImplementedException();

    public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
        => throw new NotImplementedException();

    // §7.5
    public override void CancelPendingRead()
        => throw new NotImplementedException();

    // §7.4
    public override void Complete(Exception? exception = null)
        => throw new NotImplementedException();

    // §7.1
    public override ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
        => throw new NotImplementedException();

    public override bool TryRead(out ReadResult result)
        => throw new NotImplementedException();
}
