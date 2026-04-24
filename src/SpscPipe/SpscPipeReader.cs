using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Tasks.Sources;

namespace SpscPipe;

// PipeReader subclass backing SpscPipe.Reader.  Spec §7.
// Implements IValueTaskSource<ReadResult> to bridge ReadSignal (the
// awaiter payload) to ReadResult (what the caller expects).  See §8.7.
internal sealed class SpscPipeReader : PipeReader, IValueTaskSource<ReadResult>
{
    private readonly SpscPipe _pipe;

    // Reader-local state (§4.4).
    private Segment? _head;
    private int _headConsumedOffset;
    private long _examinedPosition;
    private long _bytesRead;

    private bool _readInProgress;
    private ReadOnlySequence<byte> _lastReturnedBuffer;

    // Read awaiter carries ReadSignal (§4.4 / §8.7).  Populated in checkpoint 3.
    private ManualResetValueTaskSourceCore<ReadSignal> _readAwaiter;
    private CancellationTokenRegistration _readCtr;

    internal SpscPipeReader(SpscPipe pipe)
    {
        _pipe = pipe;
        _readAwaiter = new ManualResetValueTaskSourceCore<ReadSignal>
        {
            RunContinuationsAsynchronously = true,
        };
    }

    // ------------------------------------------------------------------
    //  §7.1  ReadAsync / TryRead
    // ------------------------------------------------------------------

    public override bool TryRead(out ReadResult result)
    {
        if (_readInProgress)
            throw new InvalidOperationException("AdvanceTo must be called before the next ReadAsync/TryRead (§10.2).");

        // §7.1 step 3: acquire completion before tail.  The writer's
        // Complete release-stores state.Tail (via the final splice) before
        // release-storing WriterCompletionState = 2; reversing the order
        // would let the reader observe a stale tail paired with fresh
        // completion and silently drop bytes.
        var writerDone = Volatile.Read(ref _pipe._state.WriterCompletionState)    // §7.1 step 3 acquire completion
                         == CompletionState.Completed;
        var tail = Volatile.Read(ref _pipe._state.Tail);                          // §7.1 step 3 acquire tail

        // §7.1 step 4: first-read head init.
        if (_head is null && tail is not null)
        {
            _head = Volatile.Read(ref _pipe._state.Head);                          // §7.1 step 4 acquire (once per lifecycle)
            _headConsumedOffset = 0;
        }

        // §7.1 step 5-6.
        var endSeg = tail;
        var endIdx = tail is null ? 0 : tail.WrittenLength;
        var availableEndPosition = tail is null ? 0 : tail.RunningIndex + tail.WrittenLength;
        var hasNewData = availableEndPosition > _examinedPosition;

        if (hasNewData)
        {
            // §7.1 step 7: return buffer.
            var buffer = new ReadOnlySequence<byte>(_head!, _headConsumedOffset, endSeg!, endIdx);
            result = new ReadResult(buffer, isCanceled: false, isCompleted: writerDone);
            _readInProgress = true;
            _lastReturnedBuffer = buffer;
            return true;
        }

        if (writerDone)
        {
            // §7.1 step 8.
            result = new ReadResult(default, isCanceled: false, isCompleted: true);
            _readInProgress = true;
            _lastReturnedBuffer = default;
            return true;
        }

        // §7.1 step 9 for TryRead.
        result = default;
        return false;
    }

    public override ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromResult(new ReadResult(default, isCanceled: true, isCompleted: false));
        }

        if (TryRead(out var result))
        {
            return ValueTask.FromResult(result);
        }

        // Slow path (§7.1 step 9 / §8.3) — checkpoint 3.
        throw new NotImplementedException("§7.1 step 9 / §8.3 — checkpoint 3");
    }

    // ------------------------------------------------------------------
    //  §7.3  AdvanceTo
    // ------------------------------------------------------------------

    public override void AdvanceTo(SequencePosition consumed) => AdvanceTo(consumed, consumed);

    public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
    {
        if (!_readInProgress)
            throw new InvalidOperationException("AdvanceTo called without a matching ReadAsync/TryRead (§10.2).");

        // §7.3 step 1 validation: light for checkpoint 2; full in checkpoint 4.

        // §7.3 step 2 (LOAD-BEARING): snapshot examined position BEFORE any
        // retirement.  The retire walk in step 3 may return examinedSeg to
        // the segment pool, after which reading examinedSeg.RunningIndex
        // would observe fields from a different logical segment (§4.1,
        // §10.7 reader-side obligation).
        var examinedSeg = examined.GetObject() as Segment;
        var examinedIdx = examined.GetInteger();
        var newExaminedPosition = examinedSeg is null
            ? _examinedPosition
            : examinedSeg.RunningIndex + examinedIdx;

        // §7.3 step 3: walk _head -> consumedSeg, retiring as §10.7 permits.
        var consumedSeg = consumed.GetObject() as Segment;
        var consumedIdx = consumed.GetInteger();
        var currentTail = Volatile.Read(ref _pipe._state.Tail);                    // §7.3 step 3 acquire; §10.7 invariant

        long retiredBytes = 0;
        var current = _head;

        if (current is null && consumedSeg is null)
        {
            // Empty buffer path (e.g., writer done with no data).
            _readInProgress = false;
            _lastReturnedBuffer = default;
            return;
        }

        while (current != consumedSeg)
        {
            retiredBytes += current!.WrittenLength - _headConsumedOffset;
            var next = current.AcquireNext();                                       // §7.2 / §7.3 acquire

            // §10.7: retire when current is proven not to be state.Tail.
            if (current != currentTail || next is not null)
            {
                RetireSegment(current);
            }
            current = next;
            _headConsumedOffset = 0;
        }

        // current == consumedSeg.  Handle partial/full consumption.
        if (consumedSeg is not null && consumedIdx == consumedSeg.WrittenLength)
        {
            var nextAfterConsumed = consumedSeg.AcquireNext();                      // §7.3 acquire
            if (nextAfterConsumed is not null)
            {
                retiredBytes += consumedSeg.WrittenLength - _headConsumedOffset;
                RetireSegment(consumedSeg);
                _head = nextAfterConsumed;
                _headConsumedOffset = 0;
            }
            else
            {
                retiredBytes += consumedSeg.WrittenLength - _headConsumedOffset;
                _head = consumedSeg;
                _headConsumedOffset = consumedIdx;                                   // == WrittenLength
            }
        }
        else if (consumedSeg is not null)
        {
            retiredBytes += consumedIdx - _headConsumedOffset;
            _head = consumedSeg;
            _headConsumedOffset = consumedIdx;
        }

        _bytesRead += retiredBytes;
        _examinedPosition = newExaminedPosition;    // §7.3 step 4: commit snapshotted examined

        Volatile.Write(ref _pipe._state.BytesReadPublished, _bytesRead);           // §7.3 step 5 release

        // §7.3 step 6: signal writer awaiter — checkpoint 3.

        _readInProgress = false;
        _lastReturnedBuffer = default;
    }

    private void RetireSegment(Segment seg)
    {
        if (seg.Holder is not null)
        {
            _pipe.ReleaseHolder(seg.Holder);   // §6.5.2 (§6.5.3 full fence)
            seg.Holder = null;
        }
        seg.Reset();
        _pipe.SegmentPool.Return(seg);
    }

    // ------------------------------------------------------------------
    //  §7.4  Complete — checkpoint 4
    // ------------------------------------------------------------------

    public override void Complete(Exception? exception = null)
        => throw new NotImplementedException("§7.4 — checkpoint 4");

    // ------------------------------------------------------------------
    //  §7.5  CancelPendingRead — checkpoint 3
    // ------------------------------------------------------------------

    public override void CancelPendingRead()
        => throw new NotImplementedException("§7.5 — checkpoint 3");

    // ------------------------------------------------------------------
    //  IValueTaskSource<ReadResult> bridge (§8.7) — checkpoint 3
    // ------------------------------------------------------------------

    ReadResult IValueTaskSource<ReadResult>.GetResult(short token)
        => throw new NotImplementedException("§8.7 — checkpoint 3");

    ValueTaskSourceStatus IValueTaskSource<ReadResult>.GetStatus(short token)
        => _readAwaiter.GetStatus(token);

    void IValueTaskSource<ReadResult>.OnCompleted(Action<object?> continuation,
        object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _readAwaiter.OnCompleted(continuation, state, token, flags);
}
