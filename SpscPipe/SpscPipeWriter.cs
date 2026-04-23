using System.Diagnostics;
using System.IO.Pipelines;
using System.Threading.Tasks.Sources;
using SpscPipe.Internal;

namespace SpscPipe;

// §6 Writer facade.  Also implements IValueTaskSource<FlushResult> — the
// FlushResult payload is buildable from shared state (unlike ReadResult),
// so no ReadSignal-style bridge is required.
internal sealed class SpscPipeWriter : PipeWriter, IValueTaskSource<FlushResult>
{
    private readonly SpscPipe _pipe;

    // §4.3 writer-local state ----------------------------------------------------

    private Segment? _unpublishedHead;
    private Segment? _unpublishedTail;
    private long     _unpublishedBytes;

    private BufferHolder? _activeBufferHolder;
    private int           _activeBufferWritten;
    private int           _activeBufferCapacity;
    private int           _unflushedStart;

    private long _bytesWritten;

    // §8 awaiter state.
    internal ManualResetValueTaskSourceCore<FlushResult> _flushAwaiter =
        new() { RunContinuationsAsynchronously = true };

    private CancellationTokenRegistration _flushCtr;

    private static readonly Action<object?, CancellationToken> s_cancelCallback =
        static (state, _) =>
        {
            var w = (SpscPipeWriter)state!;
            var prev = Interlocked.CompareExchange(
                ref w._pipe._state.WriterAwaiterState,
                AwaiterStates.Signaled,
                AwaiterStates.Armed);
            if (prev == AwaiterStates.Armed)
                w._flushAwaiter.SetResult(new FlushResult(isCanceled: true, isCompleted: false));
        };

    internal SpscPipeWriter(SpscPipe pipe) => _pipe = pipe;

    // §6.1 -----------------------------------------------------------------------
    public override Memory<byte> GetMemory(int sizeHint = 0)
    {
        if (sizeHint < 0)
            throw new ArgumentOutOfRangeException(nameof(sizeHint));

        var options = _pipe.Options;
        var desiredSize = Math.Max(sizeHint, 1);

        if (_activeBufferHolder is null)
        {
            RentActiveBuffer(Math.Max(desiredSize, options.MinimumSegmentSize));
            return _activeBufferHolder!.Owner!.Memory;
        }

        var remaining = _activeBufferCapacity - _activeBufferWritten;
        if (remaining >= desiredSize)
            return _activeBufferHolder.Owner!.Memory.Slice(_activeBufferWritten);

        if (_unflushedStart < _activeBufferWritten)
            AppendActiveSegmentToUnpublished();

        _pipe.ReleaseHolder(_activeBufferHolder);
        _activeBufferHolder = null;

        RentActiveBuffer(Math.Max(desiredSize, options.MinimumSegmentSize));
        return _activeBufferHolder!.Owner!.Memory;
    }

    public override Span<byte> GetSpan(int sizeHint = 0) => GetMemory(sizeHint).Span;

    private void RentActiveBuffer(int size)
    {
        var owner = _pipe._memoryPool.Rent(size);
        var holder = _pipe._bufferHolderPool.Rent();
        holder.Owner = owner;
        holder.Refcount = 1;
        _activeBufferHolder   = holder;
        _activeBufferCapacity = owner.Memory.Length;
        _activeBufferWritten  = 0;
        _unflushedStart       = 0;
    }

    // §6.2 -----------------------------------------------------------------------
    public override void Advance(int bytes)
    {
        if (bytes < 0)
            throw new ArgumentOutOfRangeException(nameof(bytes));
        if (bytes > _activeBufferCapacity - _activeBufferWritten)
            throw new InvalidOperationException("Advance past the end of the active buffer.");
        _activeBufferWritten += bytes;
    }

    // §6.4.1 ---------------------------------------------------------------------
    private void AppendActiveSegmentToUnpublished()
    {
        Debug.Assert(_activeBufferHolder is not null);
        Debug.Assert(_unflushedStart < _activeBufferWritten);

        var holder = _activeBufferHolder!;

        // §6.5.2 + §6.5.3: full-fence increment, ordered globally before
        // the release-stores in SpliceUnpublishedChain that make the
        // segment reader-reachable.
        Interlocked.Increment(ref holder.Refcount);

        var seg = _pipe._segmentPool.Rent();
        seg.Initialize(
            holder:        holder,
            bufferStart:   _unflushedStart,
            writtenLength: _activeBufferWritten - _unflushedStart,
            runningIndex:  _bytesWritten + _unpublishedBytes);

        if (_unpublishedTail is null)
            _unpublishedHead = seg;
        else
            _unpublishedTail.SetNextPlain(seg);

        _unpublishedTail = seg;
        _unpublishedBytes += seg.WrittenLength;

        _unflushedStart = _activeBufferWritten;
    }

    // §6.4.2 ---------------------------------------------------------------------
    private void SpliceUnpublishedChain()
    {
        Debug.Assert(_unpublishedHead is not null);

        ref var state = ref _pipe._state;

        var prevTail = state.Tail;

        if (prevTail is null)
        {
            Volatile.Write(ref state.Head, _unpublishedHead);   // §6.4.2 release-store #0 (first-ever head)
            Volatile.Write(ref state.Tail, _unpublishedTail);   // §6.4.2 release-store #1
        }
        else
        {
            prevTail.SetNextRelease(_unpublishedHead);          // §6.4.2 release-store #1 (prevTail.Next)
            Volatile.Write(ref state.Tail, _unpublishedTail);   // §6.4.2 release-store #2
        }

        _bytesWritten += _unpublishedBytes;
        Volatile.Write(ref state.BytesWrittenPublished, _bytesWritten);   // §6.4.2 release-store #3

        _unpublishedHead = null;
        _unpublishedTail = null;
        _unpublishedBytes = 0;

        // Step 5: signal the reader if it's parked.
        MaybeSignalReaderAwaiter();
    }

    // §8.2 writer-signals-reader.  Called from SpliceUnpublishedChain.
    // Preceded in program order by the release-stores of state.Tail and
    // state.BytesWrittenPublished; the MemoryBarrier below is the StoreLoad
    // fence that prevents the reader's arm from being unseen by this load
    // of ReaderAwaiterState.  Confirmed load-bearing by
    // AwaiterHandshake.tla's ENABLE_WRITER_FENCE=FALSE counterexample.
    private void MaybeSignalReaderAwaiter()
    {
        Interlocked.MemoryBarrier();

        ref var state = ref _pipe._state;

        if (Volatile.Read(ref state.ReaderAwaiterState) == AwaiterStates.Idle)
            return;

        var prev = Interlocked.CompareExchange(
            ref state.ReaderAwaiterState,
            AwaiterStates.Signaled,
            AwaiterStates.Armed);

        if (prev == AwaiterStates.Armed)
            _pipe._reader._readAwaiter.SetResult(new ReadSignal { IsCanceled = false });
    }

    // §6.3 + §8.4 ---------------------------------------------------------------
    public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromResult(new FlushResult(isCanceled: true, isCompleted: false));

        // Step 2: publish pending bytes.
        if (_activeBufferHolder is not null && _unflushedStart < _activeBufferWritten)
            AppendActiveSegmentToUnpublished();

        if (_unpublishedHead is not null)
            SpliceUnpublishedChain();

        ref var state = ref _pipe._state;

        // Step 3–4: reader completion shortcuts.
        var readerDone = Volatile.Read(ref state.ReaderCompletionState) == 2;
        if (readerDone)
            return ValueTask.FromResult(new FlushResult(isCanceled: false, isCompleted: true));

        // Step 5: backpressure check.
        var bytesRead = Volatile.Read(ref state.BytesReadPublished);
        var outstanding = _bytesWritten - bytesRead;
        if (outstanding < _pipe.Options.PauseWriterThreshold)
            return ValueTask.FromResult(new FlushResult(isCanceled: false, isCompleted: false));

        // Step 6: slow path — arm the flush awaiter.  §8.4.
        return ArmFlushAwaiter(cancellationToken);
    }

    private ValueTask<FlushResult> ArmFlushAwaiter(CancellationToken cancellationToken)
    {
        while (true)
        {
            _flushAwaiter.Reset();

            ref var state = ref _pipe._state;

            // Step A: CAS Idle → Armed.  Full fence by §5 axiom.
            var prev = Interlocked.CompareExchange(
                ref state.WriterAwaiterState,
                AwaiterStates.Armed,
                AwaiterStates.Idle);

            if (prev != AwaiterStates.Idle)
            {
                Volatile.Write(ref state.WriterAwaiterState, AwaiterStates.Idle);
                // Retry fast-path check.
                var readerDone = Volatile.Read(ref state.ReaderCompletionState) == 2;
                if (readerDone)
                    return ValueTask.FromResult(new FlushResult(isCanceled: false, isCompleted: true));

                var bytesRead = Volatile.Read(ref state.BytesReadPublished);
                if (_bytesWritten - bytesRead < _pipe.Options.PauseWriterThreshold)
                    return ValueTask.FromResult(new FlushResult(isCanceled: false, isCompleted: false));
                continue;
            }

            // Step B: full fence.  Redundant with Step A's CAS per §8.3
            // footnote — elided for the correct-code path.

            // Step C: re-check backpressure.
            var readerDone2 = Volatile.Read(ref state.ReaderCompletionState) == 2;
            var bytesRead2 = Volatile.Read(ref state.BytesReadPublished);
            var outstanding = _bytesWritten - bytesRead2;

            if (outstanding < _pipe.Options.PauseWriterThreshold || readerDone2)
            {
                var prev2 = Interlocked.CompareExchange(
                    ref state.WriterAwaiterState,
                    AwaiterStates.Idle,
                    AwaiterStates.Armed);

                if (prev2 == AwaiterStates.Armed)
                {
                    // Successfully un-armed.
                    return ValueTask.FromResult(
                        new FlushResult(isCanceled: false, isCompleted: readerDone2));
                }

                Volatile.Write(ref state.WriterAwaiterState, AwaiterStates.Idle);
            }

            // Step D: register cancellation, return ValueTask.
            _flushCtr = cancellationToken.UnsafeRegister(s_cancelCallback, this);

            return new ValueTask<FlushResult>(this, _flushAwaiter.Version);
        }
    }

    // §6.7
    public override void CancelPendingFlush()
    {
        ref var state = ref _pipe._state;
        var prev = Interlocked.CompareExchange(
            ref state.WriterAwaiterState,
            AwaiterStates.Signaled,
            AwaiterStates.Armed);
        if (prev == AwaiterStates.Armed)
            _flushAwaiter.SetResult(new FlushResult(isCanceled: true, isCompleted: false));
    }

    // §6.6 — checkpoint 4
    public override void Complete(Exception? exception = null)
        => throw new NotImplementedException();

    // IValueTaskSource<FlushResult> (§8.7 symmetric — no ReadSignal bridge
    // needed since FlushResult is buildable from shared state) --------------

    ValueTaskSourceStatus IValueTaskSource<FlushResult>.GetStatus(short token)
        => _flushAwaiter.GetStatus(token);

    void IValueTaskSource<FlushResult>.OnCompleted(
        Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _flushAwaiter.OnCompleted(continuation, state, token, flags);

    FlushResult IValueTaskSource<FlushResult>.GetResult(short token)
    {
        try
        {
            return _flushAwaiter.GetResult(token);
        }
        finally
        {
            _flushCtr.Dispose();
            _flushCtr = default;
            Volatile.Write(ref _pipe._state.WriterAwaiterState, AwaiterStates.Idle);
        }
    }
}
