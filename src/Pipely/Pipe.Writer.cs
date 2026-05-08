using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.ExceptionServices;
using System.Threading;

namespace Pipely;

/// <summary>Writer side of a Pipely <see cref="Pipe"/>.</summary>
/// <remarks>
/// All members must be invoked on a single producer thread, including
/// <see cref="Complete"/> and <see cref="Splice(System.Buffers.IMemoryOwner{byte})"/>.
/// The sole exception is <see cref="CancelPendingFlush"/>, which is callable from any thread.
/// <para>
/// This is stricter than <see cref="System.IO.Pipelines.PipeWriter"/>. The BCL implementation
/// is incidentally robust against cross-thread <c>Complete</c> because of an internal lock on
/// its hot path; Pipely is lock-free and offers no such fallback. To trigger completion from a
/// non-writer thread (timeout, cancellation token, downstream error), call
/// <see cref="CancelPendingFlush"/> from that thread and let the writer thread observe the
/// cancellation and call <see cref="Complete"/> itself.
/// </para>
/// </remarks>
public sealed class PipeWriter : System.IO.Pipelines.PipeWriter
{
    private readonly Pipe _pipe;
    internal PipeWriter(Pipe pipe) => _pipe = pipe;

    public override Memory<byte> GetMemory(int sizeHint = 0)
    {
        if (_pipe._disposed) throw new ObjectDisposedException(nameof(Pipe));
        if (_pipe._writer.WriterCompleted) throw new InvalidOperationException("Writing is completed.");
        if (sizeHint < 0) throw new ArgumentOutOfRangeException(nameof(sizeHint));
        if (sizeHint == 0) sizeHint = 1;

        if (_pipe._writer.WritingHead == null)
        {
            _pipe._writer.WritingHead = _pipe.RentSegment(sizeHint, runningIndex: 0);
            _pipe._writer.ChainHead   = _pipe._writer.WritingHead;
        }
        else
        {
            int remaining = _pipe._writer.WritingHead.AvailableMemory.Length - _pipe._writer.WritingHeadBytesBuffered;
            if (remaining < sizeHint)
            {
                int filled  = _pipe._writer.WritingHeadBytesBuffered;
                long newRI  = _pipe._writer.WritingHead.RunningIndex + filled;
                var newTail = _pipe.RentSegment(Math.Max(sizeHint, _pipe._options.MinimumSegmentSize), newRI);

                _pipe._writer.WritingHead.Freeze(filled, newTail);
                _pipe._writer.WritingHead = newTail;
                _pipe._writer.WritingHeadBytesBuffered = 0;
            }
        }

        return _pipe._writer.WritingHead.AvailableMemory.Slice(_pipe._writer.WritingHeadBytesBuffered);
    }

    public override Span<byte> GetSpan(int sizeHint = 0) => GetMemory(sizeHint).Span;

    public override void Advance(int bytes)
    {
        if (_pipe._disposed) throw new ObjectDisposedException(nameof(Pipe));
        if (_pipe._writer.WriterCompleted) throw new InvalidOperationException("Writing is completed.");
        if (_pipe._writer.WritingHead == null) throw new InvalidOperationException("Advance without prior GetMemory.");
        if (_pipe._writer.WritingHeadBytesBuffered + bytes > _pipe._writer.WritingHead.AvailableMemory.Length)
            throw new ArgumentOutOfRangeException(nameof(bytes));
        _pipe._writer.WritingHeadBytesBuffered += bytes;
        _pipe._writer.TotalWritten += bytes;
    }

    public override ValueTask<FlushResult> FlushAsync(CancellationToken ct = default)
    {
        if (_pipe._disposed) throw new ObjectDisposedException(nameof(Pipe));
        if (_pipe._writer.WriterCompleted) throw new InvalidOperationException("Writing is completed.");

        // Throw-first: refresh reader state, then throw if reader-completed-with-ex.
        if (_pipe._readerTb.TryAcquire())
            _pipe._writer.LastAcquiredReaderState = _pipe._readerTb.ConsumerSlot();

        if (_pipe._writer.LastAcquiredReaderState.IsCompleted && _pipe._writer.LastAcquiredReaderState.CompletionException != null)
            ExceptionDispatchInfo.Throw(_pipe._writer.LastAcquiredReaderState.CompletionException);

        // Sync entry: consume sticky CancelPendingFlush flag.
        while (true)
        {
            int oldV = _pipe._flushAwaiter._state;
            if ((oldV & PipelyAwaiter<FlushResult>.CancelFlag) == 0) break;
            int desired = oldV & ~PipelyAwaiter<FlushResult>.CancelFlag;
            if (Interlocked.CompareExchange(ref _pipe._flushAwaiter._state, desired, oldV) == oldV)
                return new ValueTask<FlushResult>(_pipe.BuildFlushResult(isCanceled: true));
        }

        if (ct.IsCancellationRequested)
            return ValueTask.FromCanceled<FlushResult>(ct);

        // Build state, publish, signal.
        var snapshot = new WriterState
        {
            HeadSegment         = _pipe._writer.ChainHead,
            TailSegment         = _pipe._writer.WritingHead,
            TailWritten         = _pipe._writer.WritingHeadBytesBuffered,
            TotalWritten        = _pipe._writer.TotalWritten,
            IsCompleted         = false,
            CompletionException = null,
        };
        _pipe._writerTb.ProducerSlot() = snapshot;
        _pipe._writerTb.Publish();
        _pipe._writer.LastPublishedWriterState = snapshot;

        _pipe.SignalReadAwaiterIfPending();

        // Re-acquire for backpressure freshness.
        if (_pipe._readerTb.TryAcquire())
            _pipe._writer.LastAcquiredReaderState = _pipe._readerTb.ConsumerSlot();
        _pipe.RecycleDrainedSegments();

        if (_pipe._writer.LastAcquiredReaderState.IsCompleted && _pipe._writer.LastAcquiredReaderState.CompletionException != null)
            ExceptionDispatchInfo.Throw(_pipe._writer.LastAcquiredReaderState.CompletionException);

        long unconsumed = _pipe._writer.TotalWritten - _pipe._writer.LastAcquiredReaderState.TotalConsumed;
        bool readerDone = _pipe._writer.LastAcquiredReaderState.IsCompleted;
        bool needsPark = _pipe._options.PauseWriterThreshold > 0
                         && unconsumed >= _pipe._options.PauseWriterThreshold
                         && !readerDone;

        if (!needsPark)
            return new ValueTask<FlushResult>(_pipe.BuildFlushResult(isCanceled: false));

        return ParkFlushAwaiter(ct);
    }

    /// <summary>Marks writing as complete and publishes the terminal writer state to the reader.</summary>
    /// <remarks>
    /// Must be called on the writer thread. Calling from any other thread is a contract violation;
    /// see the type-level remarks on <see cref="PipeWriter"/> for the recommended migration from
    /// BCL idioms that complete the writer from a timeout or cancellation handler. Repeat calls
    /// coalesce — the second and subsequent calls are no-ops.
    /// </remarks>
    public override void Complete(Exception? exception = null)
    {
        if (_pipe._disposed) throw new ObjectDisposedException(nameof(Pipe));
        if (_pipe._writer.WriterCompleted) return;     // double-Complete coalesces
        _pipe._writer.WriterCompleted = true;

        var snapshot = new WriterState
        {
            HeadSegment         = _pipe._writer.ChainHead,
            TailSegment         = _pipe._writer.WritingHead,
            TailWritten         = _pipe._writer.WritingHeadBytesBuffered,
            TotalWritten        = _pipe._writer.TotalWritten,
            IsCompleted         = true,
            CompletionException = exception,
        };
        _pipe._writerTb.ProducerSlot() = snapshot;
        _pipe._writerTb.Publish();
        _pipe._writer.LastPublishedWriterState = snapshot;

        _pipe.SignalReadAwaiterIfPending();
    }

    /// <summary>
    /// Cancels a pending or future <see cref="FlushAsync"/>. Safe to call from any thread — this
    /// is the only writer-side method that may be invoked outside the producer thread.
    /// </summary>
    public override void CancelPendingFlush()
    {
        if (_pipe._disposed) throw new ObjectDisposedException(nameof(Pipe));
        int oldV = Interlocked.Or(ref _pipe._flushAwaiter._state, PipelyAwaiter<FlushResult>.CancelFlag);
        if ((oldV & PipelyAwaiter<FlushResult>.StateMask) == PipelyAwaiter<FlushResult>.Pending
            && Interlocked.CompareExchange(
                   ref _pipe._flushAwaiter._state,
                   PipelyAwaiter<FlushResult>.Inactive,
                   PipelyAwaiter<FlushResult>.Pending | PipelyAwaiter<FlushResult>.CancelFlag)
               == (PipelyAwaiter<FlushResult>.Pending | PipelyAwaiter<FlushResult>.CancelFlag))
        {
            Interlocked.Increment(ref _pipe._flushAwaiter._cancelPendingWonCount);
            _pipe._flushAwaiter._ctr.Dispose();
            _pipe._flushAwaiter._core.SetResult(new FlushResult(isCanceled: true, isCompleted: false));
        }
    }

    // ---------- UnflushedBytes (BCL parity) ----------
    // Mirrors System.IO.Pipelines.Pipe.DefaultPipeWriter: pure accessors, no
    // validation, no synchronization. Writer-thread-only by SPSC contract.

    public override bool CanGetUnflushedBytes => true;

    public override long UnflushedBytes
        => _pipe._writer.TotalWritten - _pipe._writer.LastPublishedWriterState.TotalWritten;

    // ---------- BufferedBytes (Pipely extension) ----------
    // Everything currently held in pipe buffers: staging (unflushed) plus
    // published-but-not-yet-consumed by the reader. UnflushedBytes ⊂ BufferedBytes.
    // Writer-thread-only, pure accessor with no acquire side-effect — TotalConsumed
    // freshness is bounded by flush cadence (refreshed inside FlushAsync).

    public long BufferedBytes
        => _pipe._writer.TotalWritten - _pipe._writer.LastAcquiredReaderState.TotalConsumed;

    private ValueTask<FlushResult> ParkFlushAwaiter(CancellationToken ct)
    {
        _pipe._flushAwaiter._ctr.Dispose();        // R5b cleanup
        _pipe._flushAwaiter._core.Reset();
        _pipe._flushAwaiter._token = ct;

        while (true)
        {
            int oldV = _pipe._flushAwaiter._state;
            System.Diagnostics.Debug.Assert((oldV & PipelyAwaiter<FlushResult>.StateMask) == PipelyAwaiter<FlushResult>.Inactive,
                         "SPSC violation: concurrent FlushAsync");
            int desired = (oldV & PipelyAwaiter<FlushResult>.CancelFlag) | PipelyAwaiter<FlushResult>.Pending;
            if (Interlocked.CompareExchange(ref _pipe._flushAwaiter._state, desired, oldV) == oldV)
            {
                Interlocked.Increment(ref _pipe._flushAwaiter._parkCount);
                break;
            }
        }

        // Lost-wakeup re-check (throw-first).
        if (_pipe._readerTb.TryAcquire())
        {
            _pipe._writer.LastAcquiredReaderState = _pipe._readerTb.ConsumerSlot();

            if (_pipe._writer.LastAcquiredReaderState.IsCompleted && _pipe._writer.LastAcquiredReaderState.CompletionException != null)
            {
                while (true)
                {
                    int oldV = _pipe._flushAwaiter._state;
                    if ((oldV & PipelyAwaiter<FlushResult>.StateMask) != PipelyAwaiter<FlushResult>.Pending) break;
                    int desired = oldV & ~PipelyAwaiter<FlushResult>.StateMask;
                    if (Interlocked.CompareExchange(ref _pipe._flushAwaiter._state, desired, oldV) == oldV)
                    {
                        Interlocked.Increment(ref _pipe._flushAwaiter._lostWakeupResolvedCount);
                        _pipe._flushAwaiter._core.SetException(_pipe._writer.LastAcquiredReaderState.CompletionException);
                        return new ValueTask<FlushResult>(_pipe._flushAwaiter, _pipe._flushAwaiter.Version);
                    }
                }
            }

            long unconsumed = _pipe._writer.TotalWritten - _pipe._writer.LastAcquiredReaderState.TotalConsumed;
            bool releasable = unconsumed < _pipe._options.ResumeWriterThreshold
                              || _pipe._writer.LastAcquiredReaderState.IsCompleted;

            if (releasable)
            {
                while (true)
                {
                    int oldV = _pipe._flushAwaiter._state;
                    if ((oldV & PipelyAwaiter<FlushResult>.StateMask) != PipelyAwaiter<FlushResult>.Pending) break;
                    int desired = oldV & ~PipelyAwaiter<FlushResult>.StateMask;
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
        if ((v & PipelyAwaiter<FlushResult>.CancelFlag) != 0
            && Interlocked.CompareExchange(
                   ref _pipe._flushAwaiter._state,
                   PipelyAwaiter<FlushResult>.Inactive,
                   PipelyAwaiter<FlushResult>.Pending | PipelyAwaiter<FlushResult>.CancelFlag)
               == (PipelyAwaiter<FlushResult>.Pending | PipelyAwaiter<FlushResult>.CancelFlag))
        {
            Interlocked.Increment(ref _pipe._flushAwaiter._lostCancelResolvedCount);
            _pipe._flushAwaiter._core.SetResult(_pipe.BuildFlushResult(isCanceled: true));
            return new ValueTask<FlushResult>(_pipe._flushAwaiter, _pipe._flushAwaiter.Version);
        }

        _pipe._flushAwaiter._ctr = ct.UnsafeRegister(static p => ((Pipe)p!).OnFlushAwaiterTokenCancel(), _pipe);
        if ((_pipe._flushAwaiter._state & PipelyAwaiter<FlushResult>.StateMask) != PipelyAwaiter<FlushResult>.Pending)
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
        if (_pipe._disposed) throw new ObjectDisposedException(nameof(Pipe));
        if (_pipe._writer.WriterCompleted) throw new InvalidOperationException("Writing is completed.");

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
        if (_pipe._writer.WritingHead == null)
        {
            var donated = _pipe.PopDonatedShellFreelist() ?? new BufferSegment();
            donated.AdoptFrom(buffer, slice, runningIndex: 0, pipeOwner: _pipe);
            _pipe._writer.ChainHead   = donated;
            _pipe._writer.WritingHead = donated;
            _pipe._writer.WritingHeadBytesBuffered = length;
            _pipe._writer.TotalWritten += length;
            return;
        }

        // Steady state: freeze current tail with whatever's buffered, splice donated as new tail.
        // For a previously-donated tail, Freeze re-writes End/base.Memory to the same values
        // (length == AvailableMemory.Length already) and sets Next; the redundant writes are
        // idempotent and benign-torn-read-safe per spec §2.2.
        int filled = _pipe._writer.WritingHeadBytesBuffered;
        long newRI = _pipe._writer.WritingHead.RunningIndex + filled;

        var newDonated = _pipe.PopDonatedShellFreelist() ?? new BufferSegment();
        newDonated.AdoptFrom(buffer, slice, newRI, pipeOwner: _pipe);

        _pipe._writer.WritingHead.Freeze(filled, newDonated);
        _pipe._writer.WritingHead = newDonated;
        _pipe._writer.WritingHeadBytesBuffered = length;
        _pipe._writer.TotalWritten += length;
    }
}
