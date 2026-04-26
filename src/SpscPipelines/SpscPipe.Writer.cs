using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.ExceptionServices;
using System.Threading;

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
        public override ValueTask<FlushResult> FlushAsync(CancellationToken ct = default)
        {
            if (_pipe._disposed) throw new ObjectDisposedException(nameof(SpscPipe));
            if (_pipe._writerCompleted) throw new InvalidOperationException("Writing is completed.");

            // Throw-first: refresh reader state, then throw if reader-completed-with-ex.
            if (_pipe._readerTb.TryAcquire())
                _pipe._lastAcquiredReaderState = _pipe._readerTb.ConsumerSlot();

            if (_pipe._lastAcquiredReaderState.IsCompleted && _pipe._lastAcquiredReaderState.CompletionException != null)
                ExceptionDispatchInfo.Throw(_pipe._lastAcquiredReaderState.CompletionException);

            // Sync entry: consume sticky CancelPendingFlush flag.
            while (true)
            {
                int oldV = _pipe._flushAwaiter._state;
                if ((oldV & SpscAwaiter<FlushResult>.CancelFlag) == 0) break;
                int desired = oldV & ~SpscAwaiter<FlushResult>.CancelFlag;
                if (Interlocked.CompareExchange(ref _pipe._flushAwaiter._state, desired, oldV) == oldV)
                    return new ValueTask<FlushResult>(_pipe.BuildFlushResult(isCanceled: true));
            }

            if (ct.IsCancellationRequested)
                return ValueTask.FromCanceled<FlushResult>(ct);

            // Build state, publish, signal.
            var snapshot = new WriterState
            {
                HeadSegment         = _pipe._chainHead,
                TailSegment         = _pipe._writingHead,
                TailWritten         = _pipe._writingHeadBytesBuffered,
                TotalWritten        = _pipe._totalWritten,
                IsCompleted         = false,
                CompletionException = null,
            };
            _pipe._writerTb.ProducerSlot() = snapshot;
            _pipe._writerTb.Publish();
            _pipe._lastPublishedWriterState = snapshot;

            _pipe.SignalReadAwaiterIfPending();

            // Re-acquire for backpressure freshness.
            if (_pipe._readerTb.TryAcquire())
                _pipe._lastAcquiredReaderState = _pipe._readerTb.ConsumerSlot();
            _pipe.RecycleDrainedSegments();

            if (_pipe._lastAcquiredReaderState.IsCompleted && _pipe._lastAcquiredReaderState.CompletionException != null)
                ExceptionDispatchInfo.Throw(_pipe._lastAcquiredReaderState.CompletionException);

            long unconsumed = _pipe._totalWritten - _pipe._lastAcquiredReaderState.TotalConsumed;
            bool readerDone = _pipe._lastAcquiredReaderState.IsCompleted;
            bool needsPark = _pipe._options.PauseWriterThreshold > 0
                             && unconsumed >= _pipe._options.PauseWriterThreshold
                             && !readerDone;

            if (!needsPark)
                return new ValueTask<FlushResult>(_pipe.BuildFlushResult(isCanceled: false));

            // Parking implemented in Task 8 — for now, throw to make the unimplemented path explicit.
            throw new NotImplementedException("Flush parking implemented in Task 8.");
        }
        public override void Complete(Exception? ex = null) => throw new NotImplementedException();
        public override void CancelPendingFlush() => throw new NotImplementedException();
    }
}
