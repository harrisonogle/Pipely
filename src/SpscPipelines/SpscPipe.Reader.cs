using System.Buffers;
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

            return ParkReadAwaiter(ct);
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
        public override void Complete(Exception? exception = null)
        {
            if (_pipe._disposed) throw new ObjectDisposedException(nameof(SpscPipe));
            if (_pipe._readerCompleted) return;
            _pipe._readerCompleted = true;

            var snapshot = new ReaderState
            {
                HeadSegment         = null,           // S4: terminal publish
                TotalConsumed       = _pipe._totalConsumed,
                TotalExamined       = _pipe._totalExamined,
                IsCompleted         = true,
                CompletionException = exception,
            };
            _pipe._readerTb.ProducerSlot() = snapshot;
            _pipe._readerTb.Publish();
            _pipe._lastPublishedReaderState = snapshot;

            _pipe.SignalFlushAwaiterIfPending();
        }
        public override void CancelPendingRead()
        {
            if (_pipe._disposed) throw new ObjectDisposedException(nameof(SpscPipe));
            int oldV = Interlocked.Or(ref _pipe._readAwaiter._state, SpscAwaiter<ReadResult>.CancelFlag);
            if ((oldV & SpscAwaiter<ReadResult>.StateMask) == SpscAwaiter<ReadResult>.Pending
                && Interlocked.CompareExchange(
                       ref _pipe._readAwaiter._state,
                       SpscAwaiter<ReadResult>.Inactive,
                       SpscAwaiter<ReadResult>.Pending | SpscAwaiter<ReadResult>.CancelFlag)
                   == (SpscAwaiter<ReadResult>.Pending | SpscAwaiter<ReadResult>.CancelFlag))
            {
                _pipe._readAwaiter._ctr.Dispose();

                var head = _pipe._readAwaiter._stashHead;
                var tail = _pipe._readAwaiter._stashTail;
                var buffer = head == null
                    ? ReadOnlySequence<byte>.Empty
                    : new ReadOnlySequence<byte>(head, _pipe._readAwaiter._stashHeadIdx, tail!, _pipe._readAwaiter._stashTailIdx);

                _pipe._readAwaiter._core.SetResult(new ReadResult(buffer, isCanceled: true, isCompleted: false));
            }
        }

        private ValueTask<ReadResult> ParkReadAwaiter(CancellationToken ct)
        {
            _pipe._readAwaiter._ctr.Dispose();        // R5b cleanup
            _pipe._readAwaiter._core.Reset();
            _pipe._readAwaiter._token = ct;

            _pipe._readAwaiter._stashHead    = _pipe._readHead;
            _pipe._readAwaiter._stashHeadIdx = _pipe._readHeadIdx;
            _pipe._readAwaiter._stashTail    = _pipe._readTail;
            _pipe._readAwaiter._stashTailIdx = _pipe._readTailIdx;

            while (true)
            {
                int oldV = _pipe._readAwaiter._state;
                System.Diagnostics.Debug.Assert((oldV & SpscAwaiter<ReadResult>.StateMask) == SpscAwaiter<ReadResult>.Inactive);
                int desired = (oldV & SpscAwaiter<ReadResult>.CancelFlag) | SpscAwaiter<ReadResult>.Pending;
                if (Interlocked.CompareExchange(ref _pipe._readAwaiter._state, desired, oldV) == oldV) break;
            }

            // Lost-wakeup re-check (throw-first).
            if (_pipe._writerTb.TryAcquire())
            {
                _pipe._lastAcquiredWriterState = _pipe._writerTb.ConsumerSlot();
                _pipe.IntegrateAcquiredWriterState();

                if (_pipe._lastAcquiredWriterState.IsCompleted && _pipe._lastAcquiredWriterState.CompletionException != null)
                {
                    while (true)
                    {
                        int oldV = _pipe._readAwaiter._state;
                        if ((oldV & SpscAwaiter<ReadResult>.StateMask) != SpscAwaiter<ReadResult>.Pending) break;
                        int desired = oldV & ~SpscAwaiter<ReadResult>.StateMask;
                        if (Interlocked.CompareExchange(ref _pipe._readAwaiter._state, desired, oldV) == oldV)
                        {
                            _pipe._readAwaiter._core.SetException(_pipe._lastAcquiredWriterState.CompletionException);
                            return new ValueTask<ReadResult>(_pipe._readAwaiter, _pipe._readAwaiter.Version);
                        }
                    }
                }

                if (_pipe.HasReadableProgress() || _pipe._lastAcquiredWriterState.IsCompleted)
                {
                    while (true)
                    {
                        int oldV = _pipe._readAwaiter._state;
                        if ((oldV & SpscAwaiter<ReadResult>.StateMask) != SpscAwaiter<ReadResult>.Pending) break;
                        int desired = oldV & ~SpscAwaiter<ReadResult>.StateMask;
                        if (Interlocked.CompareExchange(ref _pipe._readAwaiter._state, desired, oldV) == oldV)
                            return new ValueTask<ReadResult>(_pipe.BuildReadResult(isCanceled: false));
                    }
                }
            }

            // Lost-cancel re-check.
            int v = _pipe._readAwaiter._state;
            if ((v & SpscAwaiter<ReadResult>.CancelFlag) != 0
                && Interlocked.CompareExchange(
                       ref _pipe._readAwaiter._state,
                       SpscAwaiter<ReadResult>.Inactive,
                       SpscAwaiter<ReadResult>.Pending | SpscAwaiter<ReadResult>.CancelFlag)
                   == (SpscAwaiter<ReadResult>.Pending | SpscAwaiter<ReadResult>.CancelFlag))
            {
                _pipe._readAwaiter._core.SetResult(_pipe.BuildReadResult(isCanceled: true));
                return new ValueTask<ReadResult>(_pipe._readAwaiter, _pipe._readAwaiter.Version);
            }

            _pipe._readAwaiter._ctr = ct.UnsafeRegister(static p => ((SpscPipe)p!).OnReadAwaiterTokenCancel(), _pipe);
            if ((_pipe._readAwaiter._state & SpscAwaiter<ReadResult>.StateMask) != SpscAwaiter<ReadResult>.Pending)
                _pipe._readAwaiter._ctr.Dispose();
            return new ValueTask<ReadResult>(_pipe._readAwaiter, _pipe._readAwaiter.Version);
        }
    }
}
