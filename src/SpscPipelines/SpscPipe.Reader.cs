using System.IO.Pipelines;

namespace SpscPipelines;

public sealed partial class SpscPipe
{
    internal sealed class SpscPipeReader : PipeReader
    {
        private readonly SpscPipe _pipe;
        public SpscPipeReader(SpscPipe pipe) => _pipe = pipe;

        public override ValueTask<ReadResult> ReadAsync(CancellationToken ct = default) => throw new NotImplementedException();
        public override bool TryRead(out ReadResult result) => throw new NotImplementedException();
        public override void AdvanceTo(SequencePosition consumed) => throw new NotImplementedException();
        public override void AdvanceTo(SequencePosition consumed, SequencePosition examined) => throw new NotImplementedException();
        public override void Complete(Exception? ex = null) => throw new NotImplementedException();
        public override void CancelPendingRead() => throw new NotImplementedException();
    }
}
