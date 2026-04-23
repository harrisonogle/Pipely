using System.Diagnostics;
using System.IO.Pipelines;
using SpscPipe.Internal;

namespace SpscPipe;

// §6 Writer facade.  PipeWriter implementation bound to an SpscPipe.
// Checkpoint 2 scope: happy-path GetMemory/Advance/FlushAsync and the
// publication protocol (§6.4.1, §6.4.2) + §10.7-aware retirement on
// the reader side.  No backpressure, no awaiter coordination, no
// cancellation — all of those arrive in checkpoint 3.
internal sealed class SpscPipeWriter : PipeWriter
{
    private readonly SpscPipe _pipe;

    // §4.3 writer-local state ----------------------------------------------------

    // Unpublished chain (writer-local; reader cannot see until splice).
    private Segment? _unpublishedHead;
    private Segment? _unpublishedTail;
    private long     _unpublishedBytes;

    // Active buffer (currently being filled).
    private BufferHolder? _activeBufferHolder;
    private int           _activeBufferWritten;
    private int           _activeBufferCapacity;
    private int           _unflushedStart;

    // Mirror of state.BytesWrittenPublished, advanced at each splice.
    private long _bytesWritten;

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

        // Not enough room.  Any unflushed bytes on the current buffer become
        // a segment in the unpublished chain; then rotate to a new buffer.
        if (_unflushedStart < _activeBufferWritten)
            AppendActiveSegmentToUnpublished();

        // Drop the writer's own reference to the current holder.  Segments
        // appended to the unpublished chain retain their own references
        // (§6.5); the buffer returns to the pool when the last of them is
        // retired (or at Dispose, if the chain never gets spliced).
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
        holder.Refcount = 1;            // writer's own reference (§6.5.2)
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
    // Writer-local only.  Builds a Segment for the current active buffer's
    // unflushed bytes and appends it to the unpublished chain.  The reader
    // cannot observe this segment until SpliceUnpublishedChain makes the
    // chain reachable via a release-store.
    private void AppendActiveSegmentToUnpublished()
    {
        Debug.Assert(_activeBufferHolder is not null);
        Debug.Assert(_unflushedStart < _activeBufferWritten);

        var holder = _activeBufferHolder!;

        // §6.5.2 + §6.5.3: the Interlocked.Increment is a full fence by the
        // §5 axiom, so it is globally ordered before any subsequent release-
        // store in SpliceUnpublishedChain that makes this segment reader-
        // reachable.  Any reader that later observes the segment also
        // observes the incremented Refcount.
        Interlocked.Increment(ref holder.Refcount);

        var seg = _pipe._segmentPool.Rent();
        seg.Initialize(
            holder:        holder,
            bufferStart:   _unflushedStart,
            writtenLength: _activeBufferWritten - _unflushedStart,
            runningIndex:  _bytesWritten + _unpublishedBytes);

        if (_unpublishedTail is null)
        {
            _unpublishedHead = seg;
        }
        else
        {
            _unpublishedTail.SetNextPlain(seg);  // §6.4.1 step 2: plain intra-chain write
        }
        _unpublishedTail = seg;
        _unpublishedBytes += seg.WrittenLength;

        _unflushedStart = _activeBufferWritten;
    }

    // §6.4.2 ---------------------------------------------------------------------
    // The publication point.  A single invocation publishes every segment
    // currently in the unpublished chain to the reader atomically (in
    // happens-before terms).  The three release-stores in the order below
    // are LOAD-BEARING on weak memory (ARM64); verified by TLC in
    // spec/tla/Publication.tla.  Do not reorder.
    private void SpliceUnpublishedChain()
    {
        Debug.Assert(_unpublishedHead is not null);

        ref var state = ref _pipe._state;

        // Step 1: snapshot current tail.  Plain read — the writer is the
        // sole writer of state.Tail, so buffer-forwarding delivers the
        // latest value without a Volatile.Read.
        var prevTail = state.Tail;

        if (prevTail is null)
        {
            // First splice ever.
            // CRITICAL ORDERING: Head MUST be written before Tail.  The
            // reader reads Tail before Head (§7.1 step 3–4); the acquire-
            // load of Tail seeing a chain segment establishes a happens-
            // before edge that makes the preceding release-store of Head
            // visible.  Reversing the order would permit the reader to
            // observe Tail = chainTail while Head is still null.
            Volatile.Write(ref state.Head, _unpublishedHead);   // §6.4.2 release-store #0 (first-ever head)
            Volatile.Write(ref state.Tail, _unpublishedTail);   // §6.4.2 release-store #1
        }
        else
        {
            // Subsequent splice.  Link prevTail.Next to chain head first,
            // then advance state.Tail.  Order matters for the same reason:
            // Publication.tla fence-flip experiment produces a
            // ChainConsistent counterexample in 4 steps when reversed.
            prevTail.SetNextRelease(_unpublishedHead);          // §6.4.2 release-store #1 (prevTail.Next)
            Volatile.Write(ref state.Tail, _unpublishedTail);   // §6.4.2 release-store #2
        }

        // Step 3: publish BytesWrittenPublished.  Must come after the chain
        // pointers so a reader observing the counter can find segments
        // accounting for the bytes.
        _bytesWritten += _unpublishedBytes;
        Volatile.Write(ref state.BytesWrittenPublished, _bytesWritten);   // §6.4.2 release-store #3

        // Step 4: clear the unpublished chain — its segments are now
        // reader-reachable.
        _unpublishedHead = null;
        _unpublishedTail = null;
        _unpublishedBytes = 0;

        // Step 5: MaybeSignalReaderAwaiter — arrives in checkpoint 3.
    }

    // §6.3 -----------------------------------------------------------------------
    public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromResult(new FlushResult(isCanceled: true, isCompleted: false));

        // Step 2: fold the current active buffer's unflushed bytes into the
        // chain, then splice.
        if (_activeBufferHolder is not null && _unflushedStart < _activeBufferWritten)
            AppendActiveSegmentToUnpublished();

        if (_unpublishedHead is not null)
            SpliceUnpublishedChain();

        // Step 3–4: completion check (reader side — always false in this
        // checkpoint, the reader can't Complete yet).  Checkpoint 4.

        // Step 5–6: backpressure check + slow path — checkpoint 3.
        return ValueTask.FromResult(new FlushResult(isCanceled: false, isCompleted: false));
    }

    // §6.7 — checkpoint 3
    public override void CancelPendingFlush()
        => throw new NotImplementedException();

    // §6.6 — checkpoint 4
    public override void Complete(Exception? exception = null)
        => throw new NotImplementedException();
}
