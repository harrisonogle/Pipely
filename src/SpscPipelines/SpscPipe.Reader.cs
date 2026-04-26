using System.IO.Pipelines;
using System.Runtime.ExceptionServices;
using System.Threading;

namespace SpscPipelines;

public sealed partial class SpscPipe
{
    internal sealed class SpscPipeReader : PipeReader
    {
        private readonly SpscPipe _pipe;
        public SpscPipeReader(SpscPipe pipe) => _pipe = pipe;

        public override ValueTask<ReadResult> ReadAsync(CancellationToken ct = default)
        {
            if (_pipe._disposed) throw new ObjectDisposedException(nameof(SpscPipe));
            if (_pipe._readerCompleted) throw new InvalidOperationException("Reading is completed.");

            if (_pipe._writerTb.TryAcquire())
            {
                _pipe._lastAcquiredWriterState = _pipe._writerTb.ConsumerSlot();
                _pipe.IntegrateAcquiredWriterState();
            }

            if (_pipe._lastAcquiredWriterState.IsCompleted && _pipe._lastAcquiredWriterState.CompletionException != null)
                ExceptionDispatchInfo.Throw(_pipe._lastAcquiredWriterState.CompletionException);

            while (true)
            {
                int oldV = _pipe._readAwaiter._state;
                if ((oldV & SpscAwaiter<ReadResult>.CancelFlag) == 0) break;
                int desired = oldV & ~SpscAwaiter<ReadResult>.CancelFlag;
                if (Interlocked.CompareExchange(ref _pipe._readAwaiter._state, desired, oldV) == oldV)
                    return new ValueTask<ReadResult>(_pipe.BuildReadResult(isCanceled: true));
            }

            if (ct.IsCancellationRequested)
                return ValueTask.FromCanceled<ReadResult>(ct);

            if (_pipe.HasReadableProgress() || _pipe._lastAcquiredWriterState.IsCompleted)
                return new ValueTask<ReadResult>(_pipe.BuildReadResult(isCanceled: false));

            // Parking implemented in Task 8.
            throw new NotImplementedException("Read parking implemented in Task 8.");
        }

        public override bool TryRead(out ReadResult result)
        {
            if (_pipe._disposed) throw new ObjectDisposedException(nameof(SpscPipe));
            if (_pipe._readerCompleted) throw new InvalidOperationException("Reading is completed.");

            if (_pipe._writerTb.TryAcquire())
            {
                _pipe._lastAcquiredWriterState = _pipe._writerTb.ConsumerSlot();
                _pipe.IntegrateAcquiredWriterState();
            }

            if (_pipe._lastAcquiredWriterState.IsCompleted && _pipe._lastAcquiredWriterState.CompletionException != null)
                ExceptionDispatchInfo.Throw(_pipe._lastAcquiredWriterState.CompletionException);

            while (true)
            {
                int oldV = _pipe._readAwaiter._state;
                if ((oldV & SpscAwaiter<ReadResult>.CancelFlag) == 0) break;
                int desired = oldV & ~SpscAwaiter<ReadResult>.CancelFlag;
                if (Interlocked.CompareExchange(ref _pipe._readAwaiter._state, desired, oldV) == oldV)
                {
                    result = _pipe.BuildReadResult(isCanceled: true);
                    return true;
                }
            }

            if (_pipe.HasReadableProgress() || _pipe._lastAcquiredWriterState.IsCompleted)
            {
                result = _pipe.BuildReadResult(isCanceled: false);
                return true;
            }

            result = default;
            return false;
        }

        public override void AdvanceTo(SequencePosition consumed) => AdvanceTo(consumed, consumed);

        public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
        {
            if (_pipe._disposed) throw new ObjectDisposedException(nameof(SpscPipe));
            if (_pipe._readerCompleted) throw new InvalidOperationException("Reading is completed.");

            var consumedSeg = consumed.GetObject() as BufferSegment;
            var examinedSeg = examined.GetObject() as BufferSegment;

            if (consumedSeg == null && examinedSeg == null)
            {
                _pipe.PublishReaderState();
                return;
            }
            if (consumedSeg == null || examinedSeg == null)
                throw new InvalidOperationException("AdvanceTo: mixed null/non-null SequencePositions");

            // R4-7: pipe-identity check.
            if (!ReferenceEquals(consumedSeg.OwnerToken, _pipe) || !ReferenceEquals(examinedSeg.OwnerToken, _pipe))
                throw new InvalidOperationException("AdvanceTo: SequencePosition is from a different pipe.");

            int consumedIdx = consumed.GetInteger();
            int examinedIdx = examined.GetInteger();

            long consumedAbs = consumedSeg.RunningIndex + consumedIdx;
            long examinedAbs = examinedSeg.RunningIndex + examinedIdx;

            // Refresh writer state for upper-bound validation.
            if (_pipe._writerTb.TryAcquire())
            {
                _pipe._lastAcquiredWriterState = _pipe._writerTb.ConsumerSlot();
                _pipe.IntegrateAcquiredWriterState();
            }

            if (consumedAbs < _pipe._totalConsumed
                || examinedAbs < _pipe._totalExamined
                || consumedAbs > examinedAbs
                || examinedAbs > _pipe._lastAcquiredWriterState.TotalWritten)
            {
                throw new InvalidOperationException("AdvanceTo position out of range");
            }

            _pipe._readHead      = consumedSeg;
            _pipe._readHeadIdx   = consumedIdx;
            _pipe._totalConsumed = consumedAbs;
            _pipe._totalExamined = examinedAbs;

            _pipe.PublishReaderState();
        }
        public override void Complete(Exception? ex = null) => throw new NotImplementedException();
        public override void CancelPendingRead() => throw new NotImplementedException();
    }
}
