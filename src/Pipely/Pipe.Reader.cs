using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.ExceptionServices;
using System.Threading;

namespace Pipely;

/// <summary>Reader side of a Pipely <see cref="Pipe"/>.</summary>
/// <remarks>
/// All members must be invoked on a single consumer thread, including <see cref="Complete"/>.
/// The sole exception is <see cref="CancelPendingRead"/>, which is callable from any thread.
/// <para>
/// This is stricter than <see cref="System.IO.Pipelines.PipeReader"/>. The BCL implementation
/// is incidentally robust against cross-thread <c>Complete</c> because of an internal lock on
/// its hot path; Pipely is lock-free and offers no such fallback. To trigger completion from a
/// non-reader thread (timeout, cancellation token, upstream error), call
/// <see cref="CancelPendingRead"/> from that thread and let the reader thread observe the
/// cancellation and call <see cref="Complete"/> itself.
/// </para>
/// </remarks>
public sealed class PipeReader : System.IO.Pipelines.PipeReader
{
    private readonly Pipe _pipe;
    internal PipeReader(Pipe pipe) => _pipe = pipe;

    public override ValueTask<ReadResult> ReadAsync(CancellationToken ct = default)
    {
        if (_pipe._reader.ReaderCompleted) throw new InvalidOperationException("Reading is completed.");
        if (_pipe._reader.ReadPending) throw new InvalidOperationException("Reading is in progress.");

        if (_pipe._writerTb.TryAcquire())
        {
            _pipe._reader.LastAcquiredWriterState = _pipe._writerTb.ConsumerSlot();
            _pipe.IntegrateAcquiredWriterState();
        }

        if (_pipe._reader.LastAcquiredWriterState.IsCompleted && _pipe._reader.LastAcquiredWriterState.CompletionException != null)
            ExceptionDispatchInfo.Throw(_pipe._reader.LastAcquiredWriterState.CompletionException);

        while (true)
        {
            int oldV = _pipe._readAwaiter._state;
            if ((oldV & PipelyAwaiter<ReadResult>.CancelFlag) == 0) break;
            int desired = oldV & ~PipelyAwaiter<ReadResult>.CancelFlag;
            if (Interlocked.CompareExchange(ref _pipe._readAwaiter._state, desired, oldV) == oldV)
            {
                _pipe._reader.ReadPending = true;
                return new ValueTask<ReadResult>(_pipe.BuildReadResult(isCanceled: true));
            }
        }

        if (ct.IsCancellationRequested)
            return ValueTask.FromCanceled<ReadResult>(ct);

        if (_pipe.HasReadableProgress() || _pipe._reader.LastAcquiredWriterState.IsCompleted)
        {
            _pipe._reader.ReadPending = true;
            return new ValueTask<ReadResult>(_pipe.BuildReadResult(isCanceled: false));
        }

        return ParkReadAwaiter(ct);
    }

    public override bool TryRead(out ReadResult result)
    {
        if (_pipe._reader.ReaderCompleted) throw new InvalidOperationException("Reading is completed.");
        if (_pipe._reader.ReadPending) throw new InvalidOperationException("Reading is in progress.");

        if (_pipe._writerTb.TryAcquire())
        {
            _pipe._reader.LastAcquiredWriterState = _pipe._writerTb.ConsumerSlot();
            _pipe.IntegrateAcquiredWriterState();
        }

        if (_pipe._reader.LastAcquiredWriterState.IsCompleted && _pipe._reader.LastAcquiredWriterState.CompletionException != null)
            ExceptionDispatchInfo.Throw(_pipe._reader.LastAcquiredWriterState.CompletionException);

        while (true)
        {
            int oldV = _pipe._readAwaiter._state;
            if ((oldV & PipelyAwaiter<ReadResult>.CancelFlag) == 0) break;
            int desired = oldV & ~PipelyAwaiter<ReadResult>.CancelFlag;
            if (Interlocked.CompareExchange(ref _pipe._readAwaiter._state, desired, oldV) == oldV)
            {
                _pipe._reader.ReadPending = true;
                result = _pipe.BuildReadResult(isCanceled: true);
                return true;
            }
        }

        if (_pipe.HasReadableProgress() || _pipe._reader.LastAcquiredWriterState.IsCompleted)
        {
            _pipe._reader.ReadPending = true;
            result = _pipe.BuildReadResult(isCanceled: false);
            return true;
        }

        result = default;
        return false;
    }

    public override void AdvanceTo(SequencePosition consumed) => AdvanceTo(consumed, consumed);

    public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
    {
        if (_pipe._reader.ReaderCompleted) throw new InvalidOperationException("Reading is completed.");
        _pipe._reader.ReadPending = false;

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
            _pipe._reader.LastAcquiredWriterState = _pipe._writerTb.ConsumerSlot();
            _pipe.IntegrateAcquiredWriterState();
        }

        if (consumedAbs < _pipe._reader.TotalConsumed
            || examinedAbs < _pipe._reader.TotalExamined
            || consumedAbs > examinedAbs
            || examinedAbs > _pipe._reader.LastAcquiredWriterState.TotalWritten)
        {
            throw new InvalidOperationException("AdvanceTo position out of range");
        }

        _pipe._reader.ReadHead      = consumedSeg;
        _pipe._reader.ReadHeadIdx   = consumedIdx;
        _pipe._reader.TotalConsumed = consumedAbs;
        _pipe._reader.TotalExamined = examinedAbs;

        _pipe.PublishReaderState();
    }
    /// <summary>Marks reading as complete and publishes the terminal reader state to the writer.</summary>
    /// <remarks>
    /// Must be called on the reader thread. Calling from any other thread is a contract violation;
    /// see the type-level remarks on <see cref="PipeReader"/> for the recommended migration from
    /// BCL idioms that complete the reader from a timeout or cancellation handler. Repeat calls
    /// coalesce — the second and subsequent calls are no-ops.
    /// </remarks>
    public override void Complete(Exception? exception = null)
    {
        if (_pipe._reader.ReaderCompleted) return;
        _pipe._reader.ReaderCompleted = true;

        var snapshot = new ReaderState
        {
            HeadSegment         = null,           // S4: terminal publish
            TotalConsumed       = _pipe._reader.TotalConsumed,
            TotalExamined       = _pipe._reader.TotalExamined,
            IsCompleted         = true,
            CompletionException = exception,
        };
        _pipe._readerTb.ProducerSlot() = snapshot;
        _pipe._readerTb.Publish();
        _pipe._reader.LastPublishedReaderState = snapshot;

        _pipe.SignalFlushAwaiterIfPending();
    }
    /// <summary>
    /// Cancels a pending or future <see cref="ReadAsync"/>. Safe to call from any thread — this
    /// is the only reader-side method that may be invoked outside the consumer thread.
    /// </summary>
    public override void CancelPendingRead()
    {
        int oldV = Interlocked.Or(ref _pipe._readAwaiter._state, PipelyAwaiter<ReadResult>.CancelFlag);
        if ((oldV & PipelyAwaiter<ReadResult>.StateMask) == PipelyAwaiter<ReadResult>.Pending
            && Interlocked.CompareExchange(
                   ref _pipe._readAwaiter._state,
                   PipelyAwaiter<ReadResult>.Inactive,
                   PipelyAwaiter<ReadResult>.Pending | PipelyAwaiter<ReadResult>.CancelFlag)
               == (PipelyAwaiter<ReadResult>.Pending | PipelyAwaiter<ReadResult>.CancelFlag))
        {
            _pipe._readAwaiter._ctr.Dispose();

            var head = _pipe._readAwaiter._stashHead;
            var tail = _pipe._readAwaiter._stashTail;
            var buffer = head == null
                ? ReadOnlySequence<byte>.Empty
                : new ReadOnlySequence<byte>(head, _pipe._readAwaiter._stashHeadIdx, tail!, _pipe._readAwaiter._stashTailIdx);

            Interlocked.Increment(ref _pipe._readAwaiter._cancelPendingWonCount);
            _pipe._reader.ReadPending = true;
            _pipe._readAwaiter._core.SetResult(new ReadResult(buffer, isCanceled: true, isCompleted: false));
        }
    }

    private ValueTask<ReadResult> ParkReadAwaiter(CancellationToken ct)
    {
        _pipe._readAwaiter._ctr.Dispose();        // R5b cleanup
        _pipe._readAwaiter._core.Reset();
        _pipe._readAwaiter._token = ct;

        _pipe._readAwaiter._stashHead    = _pipe._reader.ReadHead;
        _pipe._readAwaiter._stashHeadIdx = _pipe._reader.ReadHeadIdx;
        _pipe._readAwaiter._stashTail    = _pipe._reader.ReadTail;
        _pipe._readAwaiter._stashTailIdx = _pipe._reader.ReadTailIdx;

        while (true)
        {
            int oldV = _pipe._readAwaiter._state;
            System.Diagnostics.Debug.Assert((oldV & PipelyAwaiter<ReadResult>.StateMask) == PipelyAwaiter<ReadResult>.Inactive);
            int desired = (oldV & PipelyAwaiter<ReadResult>.CancelFlag) | PipelyAwaiter<ReadResult>.Pending;
            if (Interlocked.CompareExchange(ref _pipe._readAwaiter._state, desired, oldV) == oldV)
            {
                Interlocked.Increment(ref _pipe._readAwaiter._parkCount);
                break;
            }
        }

        // Lost-wakeup re-check (throw-first).
        if (_pipe._writerTb.TryAcquire())
        {
            _pipe._reader.LastAcquiredWriterState = _pipe._writerTb.ConsumerSlot();
            _pipe.IntegrateAcquiredWriterState();

            if (_pipe._reader.LastAcquiredWriterState.IsCompleted && _pipe._reader.LastAcquiredWriterState.CompletionException != null)
            {
                while (true)
                {
                    int oldV = _pipe._readAwaiter._state;
                    if ((oldV & PipelyAwaiter<ReadResult>.StateMask) != PipelyAwaiter<ReadResult>.Pending) break;
                    int desired = oldV & ~PipelyAwaiter<ReadResult>.StateMask;
                    if (Interlocked.CompareExchange(ref _pipe._readAwaiter._state, desired, oldV) == oldV)
                    {
                        Interlocked.Increment(ref _pipe._readAwaiter._lostWakeupResolvedCount);
                        _pipe._readAwaiter._core.SetException(_pipe._reader.LastAcquiredWriterState.CompletionException);
                        return new ValueTask<ReadResult>(_pipe._readAwaiter, _pipe._readAwaiter.Version);
                    }
                }
            }

            if (_pipe.HasReadableProgress() || _pipe._reader.LastAcquiredWriterState.IsCompleted)
            {
                while (true)
                {
                    int oldV = _pipe._readAwaiter._state;
                    if ((oldV & PipelyAwaiter<ReadResult>.StateMask) != PipelyAwaiter<ReadResult>.Pending) break;
                    int desired = oldV & ~PipelyAwaiter<ReadResult>.StateMask;
                    if (Interlocked.CompareExchange(ref _pipe._readAwaiter._state, desired, oldV) == oldV)
                    {
                        Interlocked.Increment(ref _pipe._readAwaiter._lostWakeupResolvedCount);
                        _pipe._reader.ReadPending = true;
                        return new ValueTask<ReadResult>(_pipe.BuildReadResult(isCanceled: false));
                    }
                }
            }
        }

        // Lost-cancel re-check.
        int v = _pipe._readAwaiter._state;
        if ((v & PipelyAwaiter<ReadResult>.CancelFlag) != 0
            && Interlocked.CompareExchange(
                   ref _pipe._readAwaiter._state,
                   PipelyAwaiter<ReadResult>.Inactive,
                   PipelyAwaiter<ReadResult>.Pending | PipelyAwaiter<ReadResult>.CancelFlag)
               == (PipelyAwaiter<ReadResult>.Pending | PipelyAwaiter<ReadResult>.CancelFlag))
        {
            Interlocked.Increment(ref _pipe._readAwaiter._lostCancelResolvedCount);
            _pipe._reader.ReadPending = true;
            _pipe._readAwaiter._core.SetResult(_pipe.BuildReadResult(isCanceled: true));
            return new ValueTask<ReadResult>(_pipe._readAwaiter, _pipe._readAwaiter.Version);
        }

        _pipe._readAwaiter._ctr = ct.UnsafeRegister(static p => ((Pipe)p!).OnReadAwaiterTokenCancel(), _pipe);
        if ((_pipe._readAwaiter._state & PipelyAwaiter<ReadResult>.StateMask) != PipelyAwaiter<ReadResult>.Pending)
            _pipe._readAwaiter._ctr.Dispose();
        return new ValueTask<ReadResult>(_pipe._readAwaiter, _pipe._readAwaiter.Version);
    }
}
