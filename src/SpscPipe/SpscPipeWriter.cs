using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Tasks.Sources;

namespace SpscPipe;

// PipeWriter subclass backing SpscPipe.Writer.  Spec §6.  Implements
// IValueTaskSource<FlushResult> directly — §8.7 notes the writer does
// not need a signal bridge because FlushResult depends only on writer-
// local and shared state the reader (signaler) can safely read.
internal sealed class SpscPipeWriter : PipeWriter, IValueTaskSource<FlushResult>
{
    private readonly SpscPipe _pipe;

    // Unpublished chain (writer-local per §4.3).
    private Segment? _unpublishedHead;
    private Segment? _unpublishedTail;
    private long _unpublishedBytes;

    // Active buffer state (§4.3).
    private BufferHolder? _activeBufferHolder;
    private int _activeBufferWritten;
    private int _activeBufferCapacity;
    private int _unflushedStart;

    // Byte accounting (§4.3).
    private long _bytesWritten;

    // Flush awaiter (§4.3, §8.4).
    private ManualResetValueTaskSourceCore<FlushResult> _flushAwaiter;
    private CancellationTokenRegistration _flushCtr;

    internal SpscPipeWriter(SpscPipe pipe)
    {
        _pipe = pipe;
        _flushAwaiter = new ManualResetValueTaskSourceCore<FlushResult>
        {
            RunContinuationsAsynchronously = true,
        };
    }

    // Exposes the writer's _bytesWritten for Complete's splice ordering
    // (checkpoint 4).  Not used outside internal helpers.
    internal long BytesWritten => _bytesWritten;

    // ------------------------------------------------------------------
    //  §6.1  GetMemory
    // ------------------------------------------------------------------

    public override Memory<byte> GetMemory(int sizeHint = 0)
    {
        var desiredSize = Math.Max(sizeHint, 1);

        if (_activeBufferHolder is null)
        {
            RentActiveBuffer(Math.Max(desiredSize, _pipe.Options.MinimumSegmentSize));
            return _activeBufferHolder!.Owner!.Memory;
        }

        var remaining = _activeBufferCapacity - _activeBufferWritten;
        if (remaining >= desiredSize)
        {
            return _activeBufferHolder.Owner!.Memory.Slice(_activeBufferWritten);
        }

        if (_unflushedStart < _activeBufferWritten)
        {
            AppendActiveSegmentToUnpublished();
        }
        _pipe.ReleaseHolder(_activeBufferHolder);
        _activeBufferHolder = null;

        RentActiveBuffer(Math.Max(desiredSize, _pipe.Options.MinimumSegmentSize));
        return _activeBufferHolder!.Owner!.Memory;
    }

    public override Span<byte> GetSpan(int sizeHint = 0) => GetMemory(sizeHint).Span;

    private void RentActiveBuffer(int size)
    {
        var owner = _pipe.BufferPool.Rent(size);
        var holder = _pipe.HolderPool.Rent();
        holder.Owner = owner;
        holder.Refcount = 1;                // writer's own reference (§6.5.2)
        _activeBufferHolder = holder;
        _activeBufferCapacity = owner.Memory.Length;
        _activeBufferWritten = 0;
        _unflushedStart = 0;
    }

    // ------------------------------------------------------------------
    //  §6.2  Advance
    // ------------------------------------------------------------------

    public override void Advance(int bytes)
    {
        if ((uint)bytes > (uint)(_activeBufferCapacity - _activeBufferWritten))
            throw new ArgumentOutOfRangeException(nameof(bytes));
        _activeBufferWritten += bytes;
    }

    // ------------------------------------------------------------------
    //  §6.4.1  AppendActiveSegmentToUnpublished  (writer-local only)
    // ------------------------------------------------------------------

    private void AppendActiveSegmentToUnpublished()
    {
        Interlocked.Increment(ref _activeBufferHolder!.Refcount);  // §6.5.2 refcount+; §6.5.3 full fence

        var seg = _pipe.SegmentPool.Rent();
        seg.Holder = _activeBufferHolder;
        seg.BufferStart = _unflushedStart;
        seg.WrittenLength = _activeBufferWritten - _unflushedStart;
        seg.SetMemory(_activeBufferHolder.Owner!.Memory.Slice(seg.BufferStart, seg.WrittenLength));
        seg.SetRunningIndex(_bytesWritten + _unpublishedBytes);
        seg.SetNextPlain(null);    // §6.4.1: terminal until a later Append/splice

        if (_unpublishedTail is null)
        {
            _unpublishedHead = seg;
        }
        else
        {
            _unpublishedTail.SetNextPlain(seg);  // §6.4.1 step 2: plain write; visibility deferred to splice
        }
        _unpublishedTail = seg;
        _unpublishedBytes += seg.WrittenLength;

        _unflushedStart = _activeBufferWritten;
    }

    // ------------------------------------------------------------------
    //  §6.4.2  SpliceUnpublishedChain  (the publication point)
    // ------------------------------------------------------------------

    private void SpliceUnpublishedChain()
    {
        var prevTail = _pipe._state.Tail;    // plain read; writer is the sole writer of state.Tail

        if (prevTail is null)
        {
            Volatile.Write(ref _pipe._state.Head, _unpublishedHead);   // §6.4.2 release-store #0 (first Head)
            Volatile.Write(ref _pipe._state.Tail, _unpublishedTail);   // §6.4.2 release-store #1 (Tail)
        }
        else
        {
            prevTail.SetNextRelease(_unpublishedHead);                  // §6.4.2 release-store #1 (prevTail.Next)
            Volatile.Write(ref _pipe._state.Tail, _unpublishedTail);   // §6.4.2 release-store #2 (Tail)
        }

        _bytesWritten += _unpublishedBytes;
        Volatile.Write(ref _pipe._state.BytesWrittenPublished, _bytesWritten);  // §6.4.2 release-store #3 (BWP)

        _unpublishedHead = null;
        _unpublishedTail = null;
        _unpublishedBytes = 0;

        MaybeSignalReaderAwaiter();
    }

    // ------------------------------------------------------------------
    //  §8.2  MaybeSignalReaderAwaiter
    // ------------------------------------------------------------------

    private void MaybeSignalReaderAwaiter()
    {
        // §8.2 StoreLoad fence.
        Interlocked.MemoryBarrier();                                                 // §8.2 StoreLoad fence

        var awaiterState = Volatile.Read(ref _pipe._state.ReaderAwaiterState);       // §8.2 acquire
        if (awaiterState == AwaiterState.Idle)
        {
            if (_pipe.Diag is not null)
            {
                var tail = _pipe._state.Tail;
                _pipe.Diag.Log("W.SignalSkipIdle",
                    tail is null ? -1 : tail.RunningIndex,
                    tail is null ? 0 : tail.WrittenLength,
                    _pipe._state.BytesWrittenPublished,
                    _bytesWritten);
            }
            return;
        }

        var prev = Interlocked.CompareExchange(
            ref _pipe._state.ReaderAwaiterState,
            AwaiterState.Signaled, AwaiterState.Armed);                               // §8.2 signal CAS (full fence)
        if (prev == AwaiterState.Armed)
        {
            if (_pipe.Diag is not null)
            {
                var tail = _pipe._state.Tail;
                _pipe.Diag.Log("W.SignalFire",
                    tail is null ? -1 : tail.RunningIndex,
                    tail is null ? 0 : tail.WrittenLength,
                    _pipe._state.BytesWrittenPublished,
                    _bytesWritten);
            }
            _pipe.ReaderInternal.SetSignal(new ReadSignal { IsCanceled = false });   // §8.2 SetResult via bridge
        }
        else if (_pipe.Diag is not null)
        {
            var tail = _pipe._state.Tail;
            _pipe.Diag.Log("W.SignalRace",
                tail is null ? -1 : tail.RunningIndex,
                tail is null ? 0 : tail.WrittenLength,
                _pipe._state.BytesWrittenPublished,
                prev);
        }
    }

    // ------------------------------------------------------------------
    //  §6.3  FlushAsync
    // ------------------------------------------------------------------

    public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            var readerDoneCanceled =
                Volatile.Read(ref _pipe._state.ReaderCompletionState)                 // §6.3 step 1 acquire
                == CompletionState.Completed;
            return new ValueTask<FlushResult>(new FlushResult(
                isCanceled: true, isCompleted: readerDoneCanceled));
        }

        // Step 2: append + splice.
        if (_unflushedStart < _activeBufferWritten)
            AppendActiveSegmentToUnpublished();
        if (_unpublishedHead is not null)
            SpliceUnpublishedChain();

        // Step 3-4: observe reader completion.
        var readerDone =
            Volatile.Read(ref _pipe._state.ReaderCompletionState)                     // §6.3 step 3 acquire
            == CompletionState.Completed;
        if (readerDone)
        {
            // §10.4 symmetric: throw the reader's exception if it completed
            // with one.  Plain read of ReaderException is covered by the
            // ReaderCompletionState release-acquire edge we just observed.
            var exInfo = _pipe._state.ReaderException;
            if (exInfo is not null)
            {
                exInfo.Throw();   // §10.4 symmetric
            }
            return new ValueTask<FlushResult>(new FlushResult(isCanceled: false, isCompleted: true));
        }

        // Step 5: backpressure check.
        var bytesRead = Volatile.Read(ref _pipe._state.BytesReadPublished);          // §6.3 step 5 acquire
        var outstanding = _bytesWritten - bytesRead;
        if (outstanding < _pipe.Options.PauseWriterThreshold)
        {
            return new ValueTask<FlushResult>(new FlushResult(isCanceled: false, isCompleted: false));
        }

        // §6.3 step 6 / §8.4 slow path.
        return ArmFlushAndAwait(cancellationToken);
    }

    private ValueTask<FlushResult> ArmFlushAndAwait(CancellationToken ct)
    {
        _flushAwaiter.Reset();

        // §8.4 step A: CAS Idle -> Armed.
        var prev = Interlocked.CompareExchange(
            ref _pipe._state.WriterAwaiterState,
            AwaiterState.Armed, AwaiterState.Idle);                                   // §8.4 step A CAS (full fence)
        if (prev != AwaiterState.Idle)
        {
            Volatile.Write(ref _pipe._state.WriterAwaiterState, AwaiterState.Idle);  // §8.4 reset defense
            return new ValueTask<FlushResult>(new FlushResult(false, false));
        }

        // §8.4 step B: fence.  Symmetric to §8.3 step B.
        Interlocked.MemoryBarrier();                                                  // §8.4 step B fence

        // §8.4 step C: re-check backpressure.
        var bytesRead = Volatile.Read(ref _pipe._state.BytesReadPublished);           // §8.4 step C acquire
        var readerDone = Volatile.Read(ref _pipe._state.ReaderCompletionState)        // §8.4 step C acquire
                         == CompletionState.Completed;
        var outstanding = _bytesWritten - bytesRead;

        if (outstanding < _pipe.Options.PauseWriterThreshold || readerDone)
        {
            var prev2 = Interlocked.CompareExchange(
                ref _pipe._state.WriterAwaiterState,
                AwaiterState.Idle, AwaiterState.Armed);                                // §8.4 step C un-arm CAS
            if (prev2 == AwaiterState.Armed)
            {
                return new ValueTask<FlushResult>(new FlushResult(
                    isCanceled: false, isCompleted: readerDone));
            }
            // prev2 == Signaled: reader signaled in the window.  Fall through.
        }

        // §8.4 step D: register cancellation.
        _flushCtr = ct.UnsafeRegister(static (s, _) =>
        {
            var w = (SpscPipeWriter)s!;
            var prev = Interlocked.CompareExchange(
                ref w._pipe._state.WriterAwaiterState,
                AwaiterState.Signaled, AwaiterState.Armed);                            // §8.6 cancel CAS (full fence)
            if (prev == AwaiterState.Armed)
            {
                var readerDone2 =
                    Volatile.Read(ref w._pipe._state.ReaderCompletionState)            // §8.6 acquire (for FlushResult.IsCompleted)
                    == CompletionState.Completed;
                w._flushAwaiter.SetResult(new FlushResult(
                    isCanceled: true, isCompleted: readerDone2));                      // §8.6 SetResult cancel
            }
        }, this);

        return new ValueTask<FlushResult>(this, _flushAwaiter.Version);
    }

    // Called by SpscPipeReader.MaybeSignalWriterAwaiter when hysteresis +
    // awaiter-state CAS succeed (§8.4 / §8.5).
    internal void SetFlushSignal(FlushResult result) => _flushAwaiter.SetResult(result);

    // ------------------------------------------------------------------
    //  §6.7  CancelPendingFlush
    // ------------------------------------------------------------------

    public override void CancelPendingFlush()
    {
        var prev = Interlocked.CompareExchange(
            ref _pipe._state.WriterAwaiterState,
            AwaiterState.Signaled, AwaiterState.Armed);                                // §6.7 / §8.6 cancel CAS (full fence)
        if (prev == AwaiterState.Armed)
        {
            var readerDone =
                Volatile.Read(ref _pipe._state.ReaderCompletionState)                 // §6.7 acquire (for FlushResult.IsCompleted)
                == CompletionState.Completed;
            _flushAwaiter.SetResult(new FlushResult(isCanceled: true, isCompleted: readerDone));
        }
    }

    // ------------------------------------------------------------------
    //  §6.6  Complete
    // ------------------------------------------------------------------

    public override void Complete(Exception? exception = null)
    {
        // Step 1: flush any pending bytes so the reader can drain them
        // before seeing the completion signal.
        if (_unflushedStart < _activeBufferWritten)
            AppendActiveSegmentToUnpublished();
        if (_unpublishedHead is not null)
            SpliceUnpublishedChain();

        // Step 2: release active buffer holder (§6.5.2).
        if (_activeBufferHolder is not null)
        {
            _pipe.ReleaseHolder(_activeBufferHolder);
            _activeBufferHolder = null;
        }

        // Step 3: capture exception.  Plain write; visibility carried by
        // the completion-state release-store below (§10.4).
        if (exception is not null)
        {
            _pipe._state.WriterException =
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(exception);
        }

        // Step 4: release-store completion.
        Volatile.Write(ref _pipe._state.WriterCompletionState, CompletionState.Completed);  // §6.6 step 4 release

        // Step 5: signal reader awaiter.
        MaybeSignalReaderAwaiter();
    }

    // Cleanup helpers used by SpscPipe.Dispose (§10.6).
    internal void WalkUnpublishedChain(Action<Segment> onSegment)
    {
        var current = _unpublishedHead;
        while (current is not null)
        {
            var next = current.TypedNext;
            onSegment(current);
            current = next;
        }
        _unpublishedHead = null;
        _unpublishedTail = null;
        _unpublishedBytes = 0;
    }

    internal void DisposeActiveBuffer()
    {
        if (_activeBufferHolder is not null)
        {
            _pipe.ReleaseHolder(_activeBufferHolder);
            _activeBufferHolder = null;
        }
    }

    // §10.5 Reset: re-init writer-local state after SpscPipe.Reset.
    internal void Reset()
    {
        _unpublishedHead = null;
        _unpublishedTail = null;
        _unpublishedBytes = 0;
        _activeBufferHolder = null;
        _activeBufferWritten = 0;
        _activeBufferCapacity = 0;
        _unflushedStart = 0;
        _bytesWritten = 0;
        _flushAwaiter.Reset();
        _flushCtr.Dispose();
        _flushCtr = default;
    }

    // ------------------------------------------------------------------
    //  IValueTaskSource<FlushResult> (§8.7 — writer-side, no bridge)
    // ------------------------------------------------------------------

    FlushResult IValueTaskSource<FlushResult>.GetResult(short token)
    {
        var result = _flushAwaiter.GetResult(token);

        // Clear awaiter state for the next cycle.
        Volatile.Write(ref _pipe._state.WriterAwaiterState, AwaiterState.Idle);       // §8.7 reset
        _flushCtr.Dispose();
        _flushCtr = default;

        return result;
    }

    ValueTaskSourceStatus IValueTaskSource<FlushResult>.GetStatus(short token)
        => _flushAwaiter.GetStatus(token);

    void IValueTaskSource<FlushResult>.OnCompleted(Action<object?> continuation,
        object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _flushAwaiter.OnCompleted(continuation, state, token, flags);
}
