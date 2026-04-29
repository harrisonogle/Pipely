using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.ExceptionServices;
using System.Threading;

namespace SpscPipelines;

public sealed class SpscPipeWriter : PipeWriter
{
    private readonly SpscPipe _pipe;
    internal SpscPipeWriter(SpscPipe pipe) => _pipe = pipe;

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

        return ParkFlushAwaiter(ct);
    }

    public override void Complete(Exception? exception = null)
    {
        if (_pipe._disposed) throw new ObjectDisposedException(nameof(SpscPipe));
        if (_pipe._writerCompleted) return;     // double-Complete coalesces
        _pipe._writerCompleted = true;

        var snapshot = new WriterState
        {
            HeadSegment         = _pipe._chainHead,
            TailSegment         = _pipe._writingHead,
            TailWritten         = _pipe._writingHeadBytesBuffered,
            TotalWritten        = _pipe._totalWritten,
            IsCompleted         = true,
            CompletionException = exception,
        };
        _pipe._writerTb.ProducerSlot() = snapshot;
        _pipe._writerTb.Publish();
        _pipe._lastPublishedWriterState = snapshot;

        _pipe.SignalReadAwaiterIfPending();
    }

    public override void CancelPendingFlush()
    {
        if (_pipe._disposed) throw new ObjectDisposedException(nameof(SpscPipe));
        int oldV = Interlocked.Or(ref _pipe._flushAwaiter._state, SpscAwaiter<FlushResult>.CancelFlag);
        if ((oldV & SpscAwaiter<FlushResult>.StateMask) == SpscAwaiter<FlushResult>.Pending
            && Interlocked.CompareExchange(
                   ref _pipe._flushAwaiter._state,
                   SpscAwaiter<FlushResult>.Inactive,
                   SpscAwaiter<FlushResult>.Pending | SpscAwaiter<FlushResult>.CancelFlag)
               == (SpscAwaiter<FlushResult>.Pending | SpscAwaiter<FlushResult>.CancelFlag))
        {
            Interlocked.Increment(ref _pipe._flushAwaiter._cancelPendingWonCount);
            _pipe._flushAwaiter._ctr.Dispose();
            _pipe._flushAwaiter._core.SetResult(new FlushResult(isCanceled: true, isCompleted: false));
        }
    }

    private ValueTask<FlushResult> ParkFlushAwaiter(CancellationToken ct)
    {
        _pipe._flushAwaiter._ctr.Dispose();        // R5b cleanup
        _pipe._flushAwaiter._core.Reset();
        _pipe._flushAwaiter._token = ct;

        while (true)
        {
            int oldV = _pipe._flushAwaiter._state;
            System.Diagnostics.Debug.Assert((oldV & SpscAwaiter<FlushResult>.StateMask) == SpscAwaiter<FlushResult>.Inactive,
                         "SPSC violation: concurrent FlushAsync");
            int desired = (oldV & SpscAwaiter<FlushResult>.CancelFlag) | SpscAwaiter<FlushResult>.Pending;
            if (Interlocked.CompareExchange(ref _pipe._flushAwaiter._state, desired, oldV) == oldV)
            {
                Interlocked.Increment(ref _pipe._flushAwaiter._parkCount);
                break;
            }
        }

        // Lost-wakeup re-check (throw-first).
        if (_pipe._readerTb.TryAcquire())
        {
            _pipe._lastAcquiredReaderState = _pipe._readerTb.ConsumerSlot();

            if (_pipe._lastAcquiredReaderState.IsCompleted && _pipe._lastAcquiredReaderState.CompletionException != null)
            {
                while (true)
                {
                    int oldV = _pipe._flushAwaiter._state;
                    if ((oldV & SpscAwaiter<FlushResult>.StateMask) != SpscAwaiter<FlushResult>.Pending) break;
                    int desired = oldV & ~SpscAwaiter<FlushResult>.StateMask;
                    if (Interlocked.CompareExchange(ref _pipe._flushAwaiter._state, desired, oldV) == oldV)
                    {
                        Interlocked.Increment(ref _pipe._flushAwaiter._lostWakeupResolvedCount);
                        _pipe._flushAwaiter._core.SetException(_pipe._lastAcquiredReaderState.CompletionException);
                        return new ValueTask<FlushResult>(_pipe._flushAwaiter, _pipe._flushAwaiter.Version);
                    }
                }
            }

            long unconsumed = _pipe._totalWritten - _pipe._lastAcquiredReaderState.TotalConsumed;
            bool releasable = unconsumed < _pipe._options.ResumeWriterThreshold
                              || _pipe._lastAcquiredReaderState.IsCompleted;

            if (releasable)
            {
                while (true)
                {
                    int oldV = _pipe._flushAwaiter._state;
                    if ((oldV & SpscAwaiter<FlushResult>.StateMask) != SpscAwaiter<FlushResult>.Pending) break;
                    int desired = oldV & ~SpscAwaiter<FlushResult>.StateMask;
                    if (Interlocked.CompareExchange(ref _pipe._flushAwaiter._state, desired, oldV) == oldV)
                    {
                        Interlocked.Increment(ref _pipe._flushAwaiter._lostWakeupResolvedCount);
                        return new ValueTask<FlushResult>(_pipe.BuildFlushResult(isCanceled: false));
                    }
                }
            }
        }

        // Lost-cancel re-check.
        int v = _pipe._flushAwaiter._state;
        if ((v & SpscAwaiter<FlushResult>.CancelFlag) != 0
            && Interlocked.CompareExchange(
                   ref _pipe._flushAwaiter._state,
                   SpscAwaiter<FlushResult>.Inactive,
                   SpscAwaiter<FlushResult>.Pending | SpscAwaiter<FlushResult>.CancelFlag)
               == (SpscAwaiter<FlushResult>.Pending | SpscAwaiter<FlushResult>.CancelFlag))
        {
            Interlocked.Increment(ref _pipe._flushAwaiter._lostCancelResolvedCount);
            _pipe._flushAwaiter._core.SetResult(_pipe.BuildFlushResult(isCanceled: true));
            return new ValueTask<FlushResult>(_pipe._flushAwaiter, _pipe._flushAwaiter.Version);
        }

        _pipe._flushAwaiter._ctr = ct.UnsafeRegister(static p => ((SpscPipe)p!).OnFlushAwaiterTokenCancel(), _pipe);
        if ((_pipe._flushAwaiter._state & SpscAwaiter<FlushResult>.StateMask) != SpscAwaiter<FlushResult>.Pending)
            _pipe._flushAwaiter._ctr.Dispose();
        return new ValueTask<FlushResult>(_pipe._flushAwaiter, _pipe._flushAwaiter.Version);
    }

    // ---------- Splice (buffer ownership transfer) ----------
    // Per spec docs/superpowers/specs/2026-04-28-spsc-pipe-buffer-ownership-transfer-design.md.
    // Ownership of `buffer` transfers to the pipe iff this method returns normally.
    // On any exception, the caller still owns `buffer` and is responsible for disposing it.

    public void Splice(IMemoryOwner<byte> buffer)
    {
        if (buffer is null) throw new ArgumentNullException(nameof(buffer));
        Splice(buffer, 0, buffer.Memory.Length);
    }

    public void Splice(IMemoryOwner<byte> buffer, int start, int length)
    {
        if (buffer is null) throw new ArgumentNullException(nameof(buffer));
        if (_pipe._disposed) throw new ObjectDisposedException(nameof(SpscPipe));
        if (_pipe._writerCompleted) throw new InvalidOperationException("Writing is completed.");

        var mem = buffer.Memory;
        if ((uint)start > (uint)mem.Length) throw new ArgumentOutOfRangeException(nameof(start));
        if ((uint)length > (uint)(mem.Length - start)) throw new ArgumentOutOfRangeException(nameof(length));

        // Zero-length: accept ownership, dispose synchronously, no chain mutation.
        if (length == 0)
        {
            buffer.Dispose();
            return;
        }

        var slice = mem.Slice(start, length);    // guaranteed to succeed after validation above

        // Bootstrap: pipe has no writing head yet.
        if (_pipe._writingHead == null)
        {
            var donated = _pipe.PopDonatedShellFreelist() ?? new BufferSegment();
            donated.AdoptFrom(buffer, slice, runningIndex: 0, pipeOwner: _pipe);
            _pipe._chainHead   = donated;
            _pipe._writingHead = donated;
            _pipe._writingHeadBytesBuffered = length;
            _pipe._totalWritten += length;
            return;
        }

        // Steady state: freeze current tail with whatever's buffered, splice donated as new tail.
        // For a previously-donated tail, Freeze re-writes End/base.Memory to the same values
        // (length == AvailableMemory.Length already) and sets Next; the redundant writes are
        // idempotent and benign-torn-read-safe per spec §2.2.
        int filled = _pipe._writingHeadBytesBuffered;
        long newRI = _pipe._writingHead.RunningIndex + filled;

        var newDonated = _pipe.PopDonatedShellFreelist() ?? new BufferSegment();
        newDonated.AdoptFrom(buffer, slice, newRI, pipeOwner: _pipe);

        _pipe._writingHead.Freeze(filled, newDonated);
        _pipe._writingHead = newDonated;
        _pipe._writingHeadBytesBuffered = length;
        _pipe._totalWritten += length;
    }
}
