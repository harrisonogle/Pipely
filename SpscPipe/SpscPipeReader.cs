using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks.Sources;
using SpscPipe.Internal;

namespace SpscPipe;

// §7 Reader facade.  Implements IValueTaskSource<ReadResult> so the
// awaiter's internal payload (ReadSignal) can be translated to a real
// ReadResult on the reader's scheduled continuation thread (§8.7).
internal sealed class SpscPipeReader : PipeReader, IValueTaskSource<ReadResult>
{
    private readonly SpscPipe _pipe;

    // §4.4 reader-local state ----------------------------------------------------

    private Segment? _head;
    private int      _headConsumedOffset;
    private long     _examinedPosition;
    private long     _bytesRead;

    private bool                   _readInProgress;
    private ReadOnlySequence<byte> _lastReturnedBuffer;

    // §8.7 awaiter carries ReadSignal, not ReadResult.  Continuations run
    // asynchronously (ThreadPool) so the writer's SetResult does not block
    // on the reader's continuation.
    internal ManualResetValueTaskSourceCore<ReadSignal> _readAwaiter =
        new() { RunContinuationsAsynchronously = true };

    private CancellationTokenRegistration _readCtr;

    // Static cancel callback to avoid per-arm-cycle delegate allocation.
    private static readonly Action<object?, CancellationToken> s_cancelCallback =
        static (state, _) =>
        {
            var r = (SpscPipeReader)state!;
            var prev = Interlocked.CompareExchange(
                ref r._pipe._state.ReaderAwaiterState,
                AwaiterStates.Signaled,
                AwaiterStates.Armed);
            if (prev == AwaiterStates.Armed)
                r._readAwaiter.SetResult(new ReadSignal { IsCanceled = true });
        };

    internal SpscPipeReader(SpscPipe pipe) => _pipe = pipe;

    // §7.1 + §8.3 ---------------------------------------------------------------
    public override ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
    {
        while (true)
        {
            if (cancellationToken.IsCancellationRequested)
                return ValueTask.FromResult(new ReadResult(default, isCanceled: true, isCompleted: false));

            // Sync fast path (§7.1 steps 3–8).
            if (TryReadCore(out var syncResult))
                return ValueTask.FromResult(syncResult);

            // Slow path: arm the read awaiter.  §8.3.
            _readAwaiter.Reset();

            ref var state = ref _pipe._state;

            // Step A: CAS Idle → Armed.  Also a full fence by §5 axiom,
            // which subsumes Step B below.
            var prev = Interlocked.CompareExchange(
                ref state.ReaderAwaiterState,
                AwaiterStates.Armed,
                AwaiterStates.Idle);

            if (prev != AwaiterStates.Idle)
            {
                // A prior signal left the state as Signaled (a stale signal
                // not yet consumed), or an illegal concurrent reader violated
                // SPSC.  Reset to Idle and re-run the sync path — under
                // correct signaling, data should now be available.
                Volatile.Write(ref state.ReaderAwaiterState, AwaiterStates.Idle);
                continue;
            }

            // Step B: redundant MemoryBarrier per §8.3 footnote — Step A CAS
            // already provides the full fence.  Keeping it elided for clarity
            // in the correct-code path; the TLA+ model (AwaiterHandshake.tla)
            // confirms Step B is unnecessary given Step A.

            // Step C: re-check whether data arrived or writer completed.
            // Order: completion before Tail, per §7.1 step 3.
            var writerDone = Volatile.Read(ref state.WriterCompletionState) == 2;
            var tail = Volatile.Read(ref state.Tail);
            var hasNewData = tail is not null &&
                             tail.RunningIndex + tail.WrittenLength > _examinedPosition;

            if (hasNewData || writerDone)
            {
                var prev2 = Interlocked.CompareExchange(
                    ref state.ReaderAwaiterState,
                    AwaiterStates.Idle,
                    AwaiterStates.Armed);

                if (prev2 == AwaiterStates.Armed)
                {
                    // Successfully un-armed.  Redo sync path.
                    continue;
                }

                // prev2 == Signaled: signaler beat us between Step A and now.
                // SetResult has run; fall through and return the ValueTask,
                // which will complete synchronously.
                Volatile.Write(ref state.ReaderAwaiterState, AwaiterStates.Idle);
            }

            // Step D: register cancellation and return the awaiter's
            // ValueTask.  UnsafeRegister avoids capturing ExecutionContext
            // on the hot path.
            _readCtr = cancellationToken.UnsafeRegister(s_cancelCallback, this);

            return new ValueTask<ReadResult>(this, _readAwaiter.Version);
        }
    }

    public override bool TryRead(out ReadResult result) => TryReadCore(out result);

    private bool TryReadCore(out ReadResult result)
    {
        if (_readInProgress)
            throw new InvalidOperationException("AdvanceTo must be called before the next ReadAsync/TryRead.");

        ref var state = ref _pipe._state;

        // §7.1 step 3: completion before Tail.
        var writerDone = Volatile.Read(ref state.WriterCompletionState) == 2;
        var tail = Volatile.Read(ref state.Tail);

        // §7.1 step 4: first-read head initialization.
        if (_head is null && tail is not null)
        {
            _head = Volatile.Read(ref state.Head);
            _headConsumedOffset = 0;
        }

        // §7.1 step 5–6.
        var availableEndPosition = tail is null ? 0L : tail.RunningIndex + tail.WrittenLength;
        var hasNewData = availableEndPosition > _examinedPosition;

        // §7.1 step 7: new data — return with the full available buffer.
        if (hasNewData)
        {
            Debug.Assert(_head is not null && tail is not null);
            var buffer = new ReadOnlySequence<byte>(_head!, _headConsumedOffset, tail!, tail!.WrittenLength);
            result = new ReadResult(buffer, isCanceled: false, isCompleted: writerDone);
            _readInProgress = true;
            _lastReturnedBuffer = buffer;
            return true;
        }

        // §7.1 step 8: no new data, writer completed.  §10.4: if the writer
        // completed with an exception, throw it now (after all bytes are
        // drained from the reader's view).  Otherwise return empty+completed.
        if (writerDone)
        {
            var ex = state.WriterException;
            if (ex is not null)
                ex.Throw();   // throws; does not return

            result = new ReadResult(default, isCanceled: false, isCompleted: true);
            _readInProgress = true;
            _lastReturnedBuffer = default;
            return true;
        }

        // §7.1 step 9: no data, no completion — sync path fails; caller arms.
        result = default;
        return false;
    }

    // §7.3 + §8.4 signal ---------------------------------------------------------
    public override void AdvanceTo(SequencePosition consumed)
        => AdvanceTo(consumed, consumed);

    public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
    {
        if (!_readInProgress)
            throw new InvalidOperationException("AdvanceTo called without a matching ReadAsync/TryRead.");

        // Empty-completed case: the last ReadResult was an empty completion
        // marker (no segments).  Both consumed and examined are default
        // SequencePositions; there is nothing to retire or examine.
        if (_lastReturnedBuffer.IsEmpty)
        {
            _readInProgress = false;
            _lastReturnedBuffer = default;
            return;
        }

        ref var state = ref _pipe._state;
        var currentTail = Volatile.Read(ref state.Tail);

        var consumedSeg = consumed.GetObject() as Segment;
        var consumedIdx = consumed.GetInteger();
        if (consumedSeg is null)
            throw new ArgumentException("SequencePosition.GetObject() did not return a Segment.", nameof(consumed));
        if (consumedIdx < 0 || consumedIdx > consumedSeg.WrittenLength)
            throw new ArgumentOutOfRangeException(nameof(consumed));

        long retiredBytes = 0;
        var current = _head;

        while (current is not null && !ReferenceEquals(current, consumedSeg))
        {
            retiredBytes += current.WrittenLength - _headConsumedOffset;
            var next = current.AcquireNext();

            if (!ReferenceEquals(current, currentTail) || next is not null)
                RetireSegment(current);

            current = next;
            _headConsumedOffset = 0;
        }

        if (current is null)
            throw new InvalidOperationException(
                "AdvanceTo's consumed position is not reachable from the current _head. " +
                "Positions must come from the most recent Read result.");

        if (consumedIdx == consumedSeg.WrittenLength)
        {
            var nextAfter = consumedSeg.AcquireNext();
            retiredBytes += consumedSeg.WrittenLength - _headConsumedOffset;

            if (nextAfter is not null)
            {
                RetireSegment(consumedSeg);
                _head = nextAfter;
                _headConsumedOffset = 0;
            }
            else
            {
                _head = consumedSeg;
                _headConsumedOffset = consumedIdx;
            }
        }
        else
        {
            retiredBytes += consumedIdx - _headConsumedOffset;
            _head = consumedSeg;
            _headConsumedOffset = consumedIdx;
        }

        _bytesRead += retiredBytes;

        var examinedSeg = examined.GetObject() as Segment;
        var examinedIdx = examined.GetInteger();
        if (examinedSeg is null)
            throw new ArgumentException("SequencePosition.GetObject() did not return a Segment.", nameof(examined));
        _examinedPosition = examinedSeg.RunningIndex + examinedIdx;

        Volatile.Write(ref state.BytesReadPublished, _bytesRead);

        _readInProgress = false;
        _lastReturnedBuffer = default;

        // §8.4 reader-signals-writer.  Deferred to after publishing
        // _bytesRead so the writer's wake-up check sees the fresh value.
        MaybeSignalWriterAwaiter();
    }

    private void RetireSegment(Segment seg)
    {
        var holder = seg.Holder!;
        _pipe._segmentPool.Return(seg);
        _pipe.ReleaseHolder(holder);
    }

    // §8.4 reader-signals-writer.  Called after AdvanceTo publishes
    // BytesReadPublished.  Matches §8.2 structure but with a hysteresis
    // check (§8.5) so the writer is only woken once drained below the
    // resume threshold — avoids wake/park thrash on single-byte retires.
    private void MaybeSignalWriterAwaiter()
    {
        // StoreLoad fence.  Without this, the load of WriterAwaiterState
        // below could reorder before the release-store of BytesReadPublished
        // in AdvanceTo, permitting the lost-wakeup race §8.3 analyzes.
        // Confirmed load-bearing by the AwaiterHandshake.tla fence-removal
        // experiment (symmetric to §8.2).
        Interlocked.MemoryBarrier();

        ref var state = ref _pipe._state;

        if (Volatile.Read(ref state.WriterAwaiterState) == AwaiterStates.Idle)
            return;

        // §8.5 hysteresis applies only to normal drain signals.  When the
        // reader has completed, bypass the hysteresis — the writer should
        // wake regardless of how much is outstanding so it can observe
        // ReaderCompletionState and return/throw from FlushAsync.
        var readerDone = Volatile.Read(ref state.ReaderCompletionState) == 2;
        if (!readerDone)
        {
            var outstanding = Volatile.Read(ref state.BytesWrittenPublished) - _bytesRead;
            if (outstanding >= _pipe.Options.ResumeWriterThreshold)
                return;
        }

        var prev = Interlocked.CompareExchange(
            ref state.WriterAwaiterState,
            AwaiterStates.Signaled,
            AwaiterStates.Armed);

        if (prev == AwaiterStates.Armed)
        {
            _pipe._writer._flushAwaiter.SetResult(
                new FlushResult(isCanceled: false, isCompleted: readerDone));
        }
    }

    // §7.5
    public override void CancelPendingRead()
    {
        // Same signal semantics as the token-registered callback: Armed → Signaled.
        ref var state = ref _pipe._state;
        var prev = Interlocked.CompareExchange(
            ref state.ReaderAwaiterState,
            AwaiterStates.Signaled,
            AwaiterStates.Armed);
        if (prev == AwaiterStates.Armed)
            _readAwaiter.SetResult(new ReadSignal { IsCanceled = true });
    }

    // §7.4.  Reader.Complete does NOT retire segments — cleanup is delegated
    // to SpscPipe.Dispose or SpscPipe.Reset.  This avoids retiring segments
    // that state.Tail still references (§10.7) and eliminates the race
    // window where the writer publishes between a reader-side retirement
    // walk and the writer observing ReaderCompletionState.
    public override void Complete(Exception? exception = null)
    {
        ref var state = ref _pipe._state;

        // Plain write of exception; visible via the release-acquire edge on
        // ReaderCompletionState below (§10.4).
        if (exception is not null)
            state.ReaderException = ExceptionDispatchInfo.Capture(exception);

        Volatile.Write(ref state.ReaderCompletionState, 2);

        // Wake the writer if parked.
        MaybeSignalWriterAwaiter();
    }

    // Cleanup walk used by SpscPipe.Dispose and SpscPipe.Reset.  Walks the
    // reader-visible published chain starting from _head (if read has
    // begun) or state.Head (if not), releasing each segment's holder and
    // returning the segment to the pool.  Must only run when no reader
    // operations are in flight.
    internal void DoCleanup()
    {
        var current = _head ?? _pipe._state.Head;
        while (current is not null)
        {
            var next = current._next;   // plain read — no concurrent access at cleanup
            if (current.Holder is not null)
            {
                _pipe.ReleaseHolder(current.Holder);
                current.Holder = null;
            }
            _pipe._segmentPool.Return(current);
            current = next;
        }

        _head = null;
        _headConsumedOffset = 0;
        _examinedPosition = 0;
        _bytesRead = 0;
        _readInProgress = false;
        _lastReturnedBuffer = default;
    }

    // Re-initialize reader for reuse after Reset.
    internal void ReInitForReuse()
    {
        _readCtr.Dispose();
        _readCtr = default;
        _readAwaiter = new ManualResetValueTaskSourceCore<ReadSignal>
        {
            RunContinuationsAsynchronously = true,
        };
    }

    // IValueTaskSource<ReadResult> bridge (§8.7) ---------------------------------

    ValueTaskSourceStatus IValueTaskSource<ReadResult>.GetStatus(short token)
        => _readAwaiter.GetStatus(token);

    void IValueTaskSource<ReadResult>.OnCompleted(
        Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _readAwaiter.OnCompleted(continuation, state, token, flags);

    ReadResult IValueTaskSource<ReadResult>.GetResult(short token)
    {
        ReadSignal signal;
        try
        {
            signal = _readAwaiter.GetResult(token);
        }
        finally
        {
            // Clean up cancellation registration and reset awaiter state for
            // the next arm cycle.  Done before we re-run TryReadCore so the
            // awaiter is ready whether we return sync-completed or the caller
            // immediately re-arms.
            _readCtr.Dispose();
            _readCtr = default;
            Volatile.Write(ref _pipe._state.ReaderAwaiterState, AwaiterStates.Idle);
        }

        if (signal.IsCanceled)
            return new ReadResult(default, isCanceled: true, isCompleted: false);

        // Non-canceled signal: re-execute read logic from reader-local state.
        // Safe under SPSC — this continuation runs on the reader's scheduler
        // thread, not concurrent with the reader's main flow.
        if (TryReadCore(out var result))
            return result;

        // A signal must correspond to "data available or writer completed";
        // TryReadCore not returning true here means either: (a) the data was
        // consumed between signal and resume (impossible under SPSC), or
        // (b) a spurious signal was delivered (also impossible under the
        // protocol verified in AwaiterHandshake.tla).
        throw new InvalidOperationException("Spurious wake in ReadAsync — awaiter protocol invariant violated.");
    }
}
