using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Tasks.Sources;

namespace SpscPipe;

// PipeWriter subclass backing SpscPipe.Writer.  Spec §6.
internal sealed class SpscPipeWriter : PipeWriter
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

    // Flush awaiter (§4.3, §8.4).  Populated in checkpoint 3.
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

        // Rotate: flush any unflushed bytes into the unpublished chain,
        // then release the writer's own reference to the active holder
        // (its refcount survives via in-chain segments if any).
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
        // §6.5.3: full-fence increment must happen before the splice's
        // release-stores so any reader that observes the segment also
        // observes the incremented refcount.
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
            // First splice.  Publish state.Head once (permanent), then state.Tail.
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

        // §8.2 signalling is wired in checkpoint 3.
    }

    // ------------------------------------------------------------------
    //  §6.3  FlushAsync  (happy path; slow path in checkpoint 3)
    // ------------------------------------------------------------------

    public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            var readerDoneCanceled =
                Volatile.Read(ref _pipe._state.ReaderCompletionState) == CompletionState.Completed;  // §6.3 step 1
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
            Volatile.Read(ref _pipe._state.ReaderCompletionState) == CompletionState.Completed;  // §6.3 step 3 acquire
        if (readerDone)
            return new ValueTask<FlushResult>(new FlushResult(isCanceled: false, isCompleted: true));

        // Step 5: backpressure check.
        var bytesRead = Volatile.Read(ref _pipe._state.BytesReadPublished);  // §6.3 step 5 acquire
        var outstanding = _bytesWritten - bytesRead;
        if (outstanding < _pipe.Options.PauseWriterThreshold)
        {
            return new ValueTask<FlushResult>(new FlushResult(isCanceled: false, isCompleted: false));
        }

        // Slow path (§6.3 step 6) — checkpoint 3.
        throw new NotImplementedException("§6.3 step 6 / §8.4 — checkpoint 3");
    }

    // ------------------------------------------------------------------
    //  §6.6  Complete — checkpoint 4
    // ------------------------------------------------------------------

    public override void Complete(Exception? exception = null)
        => throw new NotImplementedException("§6.6 — checkpoint 4");

    // ------------------------------------------------------------------
    //  §6.7  CancelPendingFlush — checkpoint 3
    // ------------------------------------------------------------------

    public override void CancelPendingFlush()
        => throw new NotImplementedException("§6.7 — checkpoint 3");
}
