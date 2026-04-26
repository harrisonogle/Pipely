using System.Buffers;
using System.IO.Pipelines;

namespace SpscPipelines;

public sealed partial class SpscPipe
{
    internal sealed class SpscPipeWriter : PipeWriter
    {
        private readonly SpscPipe _pipe;
        public SpscPipeWriter(SpscPipe pipe) => _pipe = pipe;

        public override Memory<byte> GetMemory(int sizeHint = 0)
        {
            if (_pipe._disposed) throw new ObjectDisposedException(nameof(SpscPipe));
            if (_pipe._writerCompleted) throw new InvalidOperationException("Writing is completed.");
            if (sizeHint < 0) throw new ArgumentOutOfRangeException(nameof(sizeHint));
            if (sizeHint == 0) sizeHint = 1;

            if (_pipe._writingHead == null)
            {
                _pipe._writingHead = _pipe.RentSegment(sizeHint, runningIndex: 0);
                _pipe._chainHead   = _pipe._writingHead;
            }
            else
            {
                int remaining = _pipe._writingHead.AvailableMemory.Length - _pipe._writingHeadBytesBuffered;
                if (remaining < sizeHint)
                {
                    int filled  = _pipe._writingHeadBytesBuffered;
                    long newRI  = _pipe._writingHead.RunningIndex + filled;
                    var newTail = _pipe.RentSegment(Math.Max(sizeHint, _pipe._options.MinimumSegmentSize), newRI);

                    _pipe._writingHead.Freeze(filled, newTail);
                    _pipe._writingHead = newTail;
                    _pipe._writingHeadBytesBuffered = 0;
                }
            }

            return _pipe._writingHead.AvailableMemory.Slice(_pipe._writingHeadBytesBuffered);
        }

        public override Span<byte> GetSpan(int sizeHint = 0) => GetMemory(sizeHint).Span;

        public override void Advance(int bytes)
        {
            if (_pipe._disposed) throw new ObjectDisposedException(nameof(SpscPipe));
            if (_pipe._writerCompleted) throw new InvalidOperationException("Writing is completed.");
            if (_pipe._writingHead == null) throw new InvalidOperationException("Advance without prior GetMemory.");
            if (_pipe._writingHeadBytesBuffered + bytes > _pipe._writingHead.AvailableMemory.Length)
                throw new ArgumentOutOfRangeException(nameof(bytes));
            _pipe._writingHeadBytesBuffered += bytes;
            _pipe._totalWritten += bytes;
        }

        // FlushAsync, Complete, CancelPendingFlush — implemented in later tasks.
        public override ValueTask<FlushResult> FlushAsync(CancellationToken ct = default) => throw new NotImplementedException();
        public override void Complete(Exception? ex = null) => throw new NotImplementedException();
        public override void CancelPendingFlush() => throw new NotImplementedException();
    }
}
