using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using SpscPipe.Internal;

namespace SpscPipe;

// §7 Reader facade.  PipeReader implementation bound to an SpscPipe.
// Checkpoint 2 scope: happy-path ReadAsync/TryRead/AdvanceTo with
// §10.7-aware retirement.  No awaiter, no cancellation parking — a
// ReadAsync that finds no data and no completion throws (tests must
// produce before consuming in checkpoint 2).  Awaiter arrives in
// checkpoint 3, lifecycle in checkpoint 4.
internal sealed class SpscPipeReader : PipeReader
{
    private readonly SpscPipe _pipe;

    // §4.4 reader-local state ----------------------------------------------------

    private Segment? _head;
    private int      _headConsumedOffset;
    private long     _examinedPosition;
    private long     _bytesRead;

    private bool                   _readInProgress;
    private ReadOnlySequence<byte> _lastReturnedBuffer;

    internal SpscPipeReader(SpscPipe pipe) => _pipe = pipe;

    // §7.1 -----------------------------------------------------------------------
    public override ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromResult(new ReadResult(default, isCanceled: true, isCompleted: false));

        if (TryReadCore(out var result))
            return ValueTask.FromResult(result);

        // Checkpoint 3 replaces this with awaiter arming.
        throw new NotImplementedException(
            "Awaiter coordination lands in checkpoint 3; until then, ReadAsync requires " +
            "data to have been flushed (or the writer completed) before the call.");
    }

    public override bool TryRead(out ReadResult result) => TryReadCore(out result);

    // Shared ReadAsync / TryRead core.  Returns true if a non-empty ReadResult
    // (data or completion) is available synchronously.
    private bool TryReadCore(out ReadResult result)
    {
        if (_readInProgress)
            throw new InvalidOperationException("AdvanceTo must be called before the next ReadAsync/TryRead.");

        ref var state = ref _pipe._state;

        // §7.1 step 3 — order matters: completion before Tail, so a reader
        // that observes "writer completed" is guaranteed to also see the
        // writer's final Tail publication (no dropped final segment).
        // (WriterCompletionState stays 0 throughout checkpoint 2; the
        // ordering rule still applies once checkpoint 4 lands Complete.)
        var writerDone = Volatile.Read(ref state.WriterCompletionState) == 2;
        var tail = Volatile.Read(ref state.Tail);

        // §7.1 step 4: first-read initialization.  state.Head is set once
        // by the writer's first splice and never updated again for this
        // lifecycle.
        if (_head is null && tail is not null)
        {
            _head = Volatile.Read(ref state.Head);
            _headConsumedOffset = 0;
        }

        // No tail means the writer has not yet spliced anything.
        if (tail is null)
        {
            if (writerDone)
            {
                result = new ReadResult(default, isCanceled: false, isCompleted: true);
                _readInProgress = true;
                _lastReturnedBuffer = default;
                return true;
            }
            result = default;
            return false;
        }

        Debug.Assert(_head is not null);

        // §7.1 step 5–6: determine whether there is new data past
        // _examinedPosition.
        var availableEndPosition = tail.RunningIndex + tail.WrittenLength;
        var hasNewData = availableEndPosition > _examinedPosition;

        if (!hasNewData && !writerDone)
        {
            result = default;
            return false;
        }

        // §7.1 step 7 / step 8: build the ReadResult.
        var buffer = new ReadOnlySequence<byte>(_head!, _headConsumedOffset, tail, tail.WrittenLength);

        result = new ReadResult(buffer, isCanceled: false, isCompleted: writerDone);
        _readInProgress = true;
        _lastReturnedBuffer = buffer;
        return true;
    }

    // §7.3 -----------------------------------------------------------------------
    public override void AdvanceTo(SequencePosition consumed)
        => AdvanceTo(consumed, consumed);

    public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
    {
        if (!_readInProgress)
            throw new InvalidOperationException("AdvanceTo called without a matching ReadAsync/TryRead.");

        // §10.7: acquire state.Tail once at entry.  This anchors the
        // retirement check to a consistent snapshot for the whole walk.
        ref var state = ref _pipe._state;
        var currentTail = Volatile.Read(ref state.Tail);

        var consumedSeg = consumed.GetObject() as Segment;
        var consumedIdx = consumed.GetInteger();
        if (consumedSeg is null)
            throw new ArgumentException("SequencePosition.GetObject() did not return a Segment.", nameof(consumed));
        if (consumedIdx < 0 || consumedIdx > consumedSeg.WrittenLength)
            throw new ArgumentOutOfRangeException(nameof(consumed));

        // Step 2: walk _head → consumedSeg, retiring segments that pass §10.7.
        long retiredBytes = 0;
        var current = _head;

        while (current is not null && !ReferenceEquals(current, consumedSeg))
        {
            retiredBytes += current.WrittenLength - _headConsumedOffset;
            var next = current.AcquireNext();    // §7.2 acquire-load

            // §10.7: retire iff current != acquired tail OR current.Next != null.
            // The second disjunct catches the transient mid-splice window
            // where state.Tail still points to current but its successor
            // has already been published — see spec §10.7 "Anchor point".
            if (!ReferenceEquals(current, currentTail) || next is not null)
                RetireSegment(current);

            current = next;
            _headConsumedOffset = 0;
        }

        if (current is null)
            throw new InvalidOperationException(
                "AdvanceTo's consumed position is not reachable from the current _head. " +
                "Positions must come from the most recent Read result.");

        // current == consumedSeg.
        if (consumedIdx == consumedSeg.WrittenLength)
        {
            // Fully consumed current segment — retire if a successor is published.
            var nextAfter = consumedSeg.AcquireNext();   // §7.2 acquire-load
            retiredBytes += consumedSeg.WrittenLength - _headConsumedOffset;

            if (nextAfter is not null)
            {
                RetireSegment(consumedSeg);
                _head = nextAfter;
                _headConsumedOffset = 0;
            }
            else
            {
                // consumedSeg may be state.Tail; keep it.
                _head = consumedSeg;
                _headConsumedOffset = consumedIdx;   // == WrittenLength
            }
        }
        else
        {
            // Partially consumed — must keep.
            retiredBytes += consumedIdx - _headConsumedOffset;
            _head = consumedSeg;
            _headConsumedOffset = consumedIdx;
        }

        _bytesRead += retiredBytes;

        // Step 4: update examined position.
        var examinedSeg = examined.GetObject() as Segment;
        var examinedIdx = examined.GetInteger();
        if (examinedSeg is null)
            throw new ArgumentException("SequencePosition.GetObject() did not return a Segment.", nameof(examined));
        _examinedPosition = examinedSeg.RunningIndex + examinedIdx;

        // Step 5: publish BytesReadPublished.  Acquire-acquire pairing on
        // the writer side (§8.4) when backpressure arrives in checkpoint 3.
        Volatile.Write(ref state.BytesReadPublished, _bytesRead);

        // Step 6: signal writer awaiter — checkpoint 3.

        _readInProgress = false;
        _lastReturnedBuffer = default;
    }

    // §10.7 retirement.  Retires the segment object and releases the
    // writer-side refcount on its buffer holder.
    private void RetireSegment(Segment seg)
    {
        var holder = seg.Holder!;
        _pipe._segmentPool.Return(seg);   // resets all fields and returns to pool
        _pipe.ReleaseHolder(holder);
    }

    // §7.5 — checkpoint 3
    public override void CancelPendingRead()
        => throw new NotImplementedException();

    // §7.4 — checkpoint 4
    public override void Complete(Exception? exception = null)
        => throw new NotImplementedException();
}
