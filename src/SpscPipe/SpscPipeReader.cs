using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Tasks.Sources;

namespace SpscPipe;

// PipeReader subclass backing SpscPipe.Reader.  Spec §7.
//
// Implements IValueTaskSource<ReadResult> to bridge ReadSignal (the awaiter
// payload) to ReadResult.  §8.7 explains why this bridge is needed: the
// writer, as signaler, cannot build a ReadResult (which requires reader-
// local state), so it signals with a minimal ReadSignal; the reader's
// GetResult on its own scheduled thread rebuilds the real ReadResult by
// re-running the sync path.
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

    // Read awaiter carries ReadSignal (§4.4 / §8.7).
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

    internal long _diag_lastTryRead_examinedSeen;
    internal long _diag_lastTryRead_availableEndSeen;
    internal bool _diag_lastTryRead_writerDoneSeen;
    internal Segment? _diag_lastTryRead_tailSeen;

    public override bool TryRead(out ReadResult result)
    {
        if (_readInProgress)
            throw new InvalidOperationException("AdvanceTo must be called before the next ReadAsync/TryRead (§10.2).");

        // §7.1 step 3: acquire completion before tail (rationale: if loads
        // were reversed the reader could observe a stale tail paired with
        // fresh completion and drop bytes).
        var writerDone = Volatile.Read(ref _pipe._state.WriterCompletionState)    // §7.1 step 3 acquire completion
                         == CompletionState.Completed;
        var tail = Volatile.Read(ref _pipe._state.Tail);                          // §7.1 step 3 acquire tail
        _diag_lastTryRead_tailSeen = tail;
        _diag_lastTryRead_writerDoneSeen = writerDone;
        _diag_lastTryRead_examinedSeen = _examinedPosition;
        _diag_lastTryRead_availableEndSeen = tail is null ? 0 : tail.RunningIndex + tail.WrittenLength;

        // §7.1 step 4: first-read head init.
        if (_head is null && tail is not null)
        {
            _head = Volatile.Read(ref _pipe._state.Head);                          // §7.1 step 4 acquire (once per lifecycle)
            _headConsumedOffset = 0;
        }

        // §7.1 steps 5-6.
        var endSeg = tail;
        var endIdx = tail is null ? 0 : tail.WrittenLength;
        var availableEndPosition = tail is null ? 0 : tail.RunningIndex + tail.WrittenLength;
        var hasNewData = availableEndPosition > _examinedPosition;

        if (hasNewData)
        {
            var buffer = new ReadOnlySequence<byte>(_head!, _headConsumedOffset, endSeg!, endIdx);
            result = new ReadResult(buffer, isCanceled: false, isCompleted: writerDone);
            _readInProgress = true;
            _lastReturnedBuffer = buffer;
            return true;
        }

        if (writerDone)
        {
            // §10.4: if the writer completed with an exception and the
            // reader has drained all available bytes, throw on this
            // ReadAsync.  WriterException is a plain read whose visibility
            // is carried by the release-acquire edge on
            // WriterCompletionState that we just observed.
            var exInfo = _pipe._state.WriterException;
            if (exInfo is not null)
            {
                exInfo.Throw();   // §10.4
            }
            result = new ReadResult(default, isCanceled: false, isCompleted: true);
            _readInProgress = true;
            _lastReturnedBuffer = default;
            return true;
        }

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

        return ArmAndAwait(cancellationToken);
    }

    private ValueTask<ReadResult> ArmAndAwait(CancellationToken ct)
    {
        _pipe.Diag?.Log("R.ArmStart", _examinedPosition, _bytesRead, 0, 0);

        // Prepare MRVTS for the new cycle.  Reset bumps Version, so any
        // signal from a prior (un-consumed) cycle cannot bleed into this
        // one (§8.7 Version discipline).
        _readAwaiter.Reset();

        // §8.3 step A: CAS Idle -> Armed.
        var prev = Interlocked.CompareExchange(
            ref _pipe._state.ReaderAwaiterState,
            AwaiterState.Armed, AwaiterState.Idle);                                // §8.3 step A CAS (full fence)
        if (prev != AwaiterState.Idle)
        {
            // Unexpected: previous cycle leaked a Signaled state, or reader
            // was already Armed.  Reset to Idle and retry the sync path.
            Volatile.Write(ref _pipe._state.ReaderAwaiterState, AwaiterState.Idle); // §8.3 reset defense
            return TryRead(out var r)
                ? ValueTask.FromResult(r)
                : ValueTask.FromResult(new ReadResult(default, false, false));
        }

        // §8.3 step B: StoreLoad fence.  The step A CAS is itself a full
        // fence (§5 axiom) but we emit an explicit MemoryBarrier here for
        // local reasoning per §8.3 rationale.
        Interlocked.MemoryBarrier();                                                // §8.3 step B fence

        // §8.3 step C: re-check.  Completion before tail, same order as §7.1.
        var writerDone = Volatile.Read(ref _pipe._state.WriterCompletionState)      // §8.3 step C acquire completion
                         == CompletionState.Completed;
        var tail = Volatile.Read(ref _pipe._state.Tail);                            // §8.3 step C acquire tail

        // First-read init inside arm: if readerHead is still null, we must
        // initialise it now so that TryRead (via GetResult) can build a
        // ReadOnlySequence.
        if (_head is null && tail is not null)
        {
            _head = Volatile.Read(ref _pipe._state.Head);                            // §7.1 step 4 acquire
            _headConsumedOffset = 0;
        }

        var availableEndPosition = tail is null ? 0 : tail.RunningIndex + tail.WrittenLength;
        var hasNewData = availableEndPosition > _examinedPosition;

        _pipe.Diag?.Log("R.ArmReCheck", availableEndPosition, _examinedPosition,
            writerDone ? 1 : 0, hasNewData ? 1 : 0);

        if (hasNewData || writerDone)
        {
            // Try to un-arm.
            var prev2 = Interlocked.CompareExchange(
                ref _pipe._state.ReaderAwaiterState,
                AwaiterState.Idle, AwaiterState.Armed);                              // §8.3 step C un-arm CAS
            if (prev2 == AwaiterState.Armed)
            {
                _pipe.Diag?.Log("R.ArmUnArm", 0, 0, 0, 0);
                // Un-armed successfully; return sync.
                return TryRead(out var r)
                    ? ValueTask.FromResult(r)
                    : ValueTask.FromResult(new ReadResult(default, false, writerDone));
            }
            _pipe.Diag?.Log("R.ArmUnArmFail", prev2, 0, 0, 0);
            // prev2 == Signaled: signaler already fired SetResult.  Fall
            // through to return the VT so the caller awaits GetResult.
        }

        _pipe.Diag?.Log("R.ArmPark", 0, 0, 0, 0);

        // §8.3 step D: register cancellation and return the VT.
        _readCtr = ct.UnsafeRegister(static (s, _) =>
        {
            var r = (SpscPipeReader)s!;
            var prev = Interlocked.CompareExchange(
                ref r._pipe._state.ReaderAwaiterState,
                AwaiterState.Signaled, AwaiterState.Armed);                          // §8.6 cancel CAS (full fence)
            if (prev == AwaiterState.Armed)
            {
                r._readAwaiter.SetResult(new ReadSignal { IsCanceled = true });      // §8.6 SetResult cancel
            }
        }, this);

        return new ValueTask<ReadResult>(this, _readAwaiter.Version);
    }

    // Called by SpscPipeWriter when it signals the reader (§8.2) and by
    // the cancellation callback (§8.6) through a shared payload type.
    internal void SetSignal(ReadSignal signal) => _readAwaiter.SetResult(signal);

    // ------------------------------------------------------------------
    //  §7.3  AdvanceTo
    // ------------------------------------------------------------------

    public override void AdvanceTo(SequencePosition consumed) => AdvanceTo(consumed, consumed);

    public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
    {
        if (!_readInProgress)
            throw new InvalidOperationException("AdvanceTo called without a matching ReadAsync/TryRead (§10.2).");

        // §7.3 step 2 (LOAD-BEARING): snapshot examined BEFORE retirement.
        // Retirement of examinedSeg would let the writer re-rent it and
        // overwrite its fields (§4.1), leaving a later read of RunningIndex
        // pointing at a different logical segment.
        var examinedSeg = examined.GetObject() as Segment;
        var examinedIdx = examined.GetInteger();
        var newExaminedPosition = examinedSeg is null
            ? _examinedPosition
            : examinedSeg.RunningIndex + examinedIdx;

        // §7.3 step 3: walk _head -> consumedSeg, retiring as §10.7 permits.
        var consumedSeg = consumed.GetObject() as Segment;
        var consumedIdx = consumed.GetInteger();
        var currentTail = Volatile.Read(ref _pipe._state.Tail);                      // §7.3 acquire; §10.7 invariant

        long retiredBytes = 0;
        var current = _head;

        if (current is null && consumedSeg is null)
        {
            _readInProgress = false;
            _lastReturnedBuffer = default;
            return;
        }

        while (current != consumedSeg)
        {
            retiredBytes += current!.WrittenLength - _headConsumedOffset;
            var next = current.AcquireNext();                                         // §7.2 / §7.3 acquire

            if (current != currentTail || next is not null)                          // §10.7 retirement rule
            {
                RetireSegment(current);
            }
            current = next;
            _headConsumedOffset = 0;
        }

        if (consumedSeg is not null && consumedIdx == consumedSeg.WrittenLength)
        {
            var nextAfterConsumed = consumedSeg.AcquireNext();                        // §7.3 acquire
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
                _headConsumedOffset = consumedIdx;
            }
        }
        else if (consumedSeg is not null)
        {
            retiredBytes += consumedIdx - _headConsumedOffset;
            _head = consumedSeg;
            _headConsumedOffset = consumedIdx;
        }

        _bytesRead += retiredBytes;
        _examinedPosition = newExaminedPosition;

        Volatile.Write(ref _pipe._state.BytesReadPublished, _bytesRead);             // §7.3 step 5 release

        _readInProgress = false;
        _lastReturnedBuffer = default;

        // §7.3 step 6 / §8.4 / §8.5: signal writer if it's parked and we've
        // drained below Resume.
        MaybeSignalWriterAwaiter();
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
    //  §8.4 / §8.5  MaybeSignalWriterAwaiter
    // ------------------------------------------------------------------

    private void MaybeSignalWriterAwaiter()
    {
        // §8.4 reader-side fence.  Mirror of §8.2: the reader's release-
        // store of BytesReadPublished (just above) must be globally ordered
        // before this load of WriterAwaiterState so the double-check
        // protocol is closed.
        Interlocked.MemoryBarrier();                                                 // §8.4 reader-side StoreLoad fence

        var awaiterState = Volatile.Read(ref _pipe._state.WriterAwaiterState);       // §8.4 acquire
        if (awaiterState == AwaiterState.Idle) return;

        // §8.5 hysteresis: only signal once Outstanding has drained below
        // Resume (not just below Pause).  Without this, the writer wakes at
        // outstanding = Pause-1 and immediately re-parks — wake/park thrash.
        var outstanding =
            Volatile.Read(ref _pipe._state.BytesWrittenPublished) - _bytesRead;      // §8.5 acquire
        if (outstanding >= _pipe.Options.ResumeWriterThreshold) return;

        var prev = Interlocked.CompareExchange(
            ref _pipe._state.WriterAwaiterState,
            AwaiterState.Signaled, AwaiterState.Armed);                               // §8.4 signal CAS (full fence)
        if (prev == AwaiterState.Armed)
        {
            var readerDone =
                Volatile.Read(ref _pipe._state.ReaderCompletionState) == CompletionState.Completed;  // §8.4 acquire for FlushResult
            _pipe.WriterInternal.SetFlushSignal(new FlushResult(
                isCanceled: false, isCompleted: readerDone));                         // §8.4 SetResult
        }
    }

    // ------------------------------------------------------------------
    //  §7.4  Complete
    // ------------------------------------------------------------------

    public override void Complete(Exception? exception = null)
    {
        // Step 1: capture exception (plain write; visibility via completion).
        if (exception is not null)
        {
            _pipe._state.ReaderException =
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(exception);
        }

        // Step 2: release-store completion.
        Volatile.Write(ref _pipe._state.ReaderCompletionState, CompletionState.Completed);  // §7.4 step 2 release

        // Step 3: signal writer awaiter, BYPASSING hysteresis (§8.5).  On
        // reader completion the writer should wake regardless of
        // Outstanding; otherwise a writer parked with outstanding in
        // [Resume, Pause) stays asleep forever.
        SignalWriterAwaiterOnComplete();

        // Per §7.4: reader Complete does NOT retire segments.  All segment
        // and holder cleanup is delegated to SpscPipe.Dispose or Reset.
    }

    private void SignalWriterAwaiterOnComplete()
    {
        Interlocked.MemoryBarrier();                                                  // §8.4 reader-side fence

        var awaiterState = Volatile.Read(ref _pipe._state.WriterAwaiterState);        // §8.4 acquire
        if (awaiterState == AwaiterState.Idle) return;

        // §8.5 bypass: on reader-complete, skip the outstanding < Resume
        // check.  Otherwise a writer parked at Outstanding ≥ Resume stays
        // asleep.
        var prev = Interlocked.CompareExchange(
            ref _pipe._state.WriterAwaiterState,
            AwaiterState.Signaled, AwaiterState.Armed);                                // §8.5 bypass signal CAS (full fence)
        if (prev == AwaiterState.Armed)
        {
            _pipe.WriterInternal.SetFlushSignal(new FlushResult(
                isCanceled: false, isCompleted: true));
        }
    }

    // Internal accessor for lifecycle cleanup (§10.6).
    internal Segment? Head => _head;

    // §10.5 Reset: re-init reader-local state for reuse after SpscPipe.Reset.
    internal void Reset()
    {
        _head = null;
        _headConsumedOffset = 0;
        _examinedPosition = 0;
        _bytesRead = 0;
        _readInProgress = false;
        _lastReturnedBuffer = default;
        _readAwaiter.Reset();
        _readCtr.Dispose();
        _readCtr = default;
    }

    // ------------------------------------------------------------------
    //  §7.5  CancelPendingRead
    // ------------------------------------------------------------------

    public override void CancelPendingRead()
    {
        var prev = Interlocked.CompareExchange(
            ref _pipe._state.ReaderAwaiterState,
            AwaiterState.Signaled, AwaiterState.Armed);                               // §7.5 / §8.6 cancel CAS (full fence)
        if (prev == AwaiterState.Armed)
        {
            _readAwaiter.SetResult(new ReadSignal { IsCanceled = true });
        }
    }

    // ------------------------------------------------------------------
    //  IValueTaskSource<ReadResult> bridge (§8.7)
    // ------------------------------------------------------------------

    ReadResult IValueTaskSource<ReadResult>.GetResult(short token)
    {
        _pipe.Diag?.Log("R.GetResultEntry", token, 0, 0, 0);
        var signal = _readAwaiter.GetResult(token);

        // §8.7 bridge: clear awaiter state for the next cycle.
        Volatile.Write(ref _pipe._state.ReaderAwaiterState, AwaiterState.Idle);      // §8.7 reset
        _readCtr.Dispose();
        _readCtr = default;

        if (signal.IsCanceled)
        {
            _pipe.Diag?.Log("R.GetResultCanceled", 0, 0, 0, 0);
            return new ReadResult(default, isCanceled: true, isCompleted: false);
        }

        // Re-run sync path to produce the real ReadResult (§8.7 rationale).
        if (TryRead(out var result))
        {
            _pipe.Diag?.Log("R.GetResultOK",
                (long)result.Buffer.Length,
                result.IsCompleted ? 1 : 0,
                0, 0);
            return result;
        }

        _pipe.Diag?.Log("R.GetResultSpurious",
            _diag_lastTryRead_examinedSeen,
            _diag_lastTryRead_availableEndSeen,
            _diag_lastTryRead_writerDoneSeen ? 1 : 0,
            _diag_lastTryRead_tailSeen is null ? -1 : _diag_lastTryRead_tailSeen.RunningIndex);

        var tryReadObs =
            $"tryRead-observed: examined={_diag_lastTryRead_examinedSeen} availableEnd={_diag_lastTryRead_availableEndSeen} " +
            $"tail={(_diag_lastTryRead_tailSeen is null ? "null" : $"seg@{_diag_lastTryRead_tailSeen.RunningIndex}+{_diag_lastTryRead_tailSeen.WrittenLength}")} writerDone={_diag_lastTryRead_writerDoneSeen}";
        var snapshot =
            $"now: examined={_examinedPosition} bytesRead={_bytesRead} " +
            $"readerHead={(_head is null ? "null" : $"seg@{_head.RunningIndex}+{_head.WrittenLength}")} " +
            $"memTail={(_pipe._state.Tail is null ? "null" : $"seg@{_pipe._state.Tail.RunningIndex}+{_pipe._state.Tail.WrittenLength}")} " +
            $"memBWP={_pipe._state.BytesWrittenPublished} " +
            $"memWriterCompletion={_pipe._state.WriterCompletionState} " +
            $"memReaderAwaiterState={_pipe._state.ReaderAwaiterState} ";
        throw new InvalidOperationException(
            $"SpscPipe: spurious wake (§8.3). {tryReadObs} | {snapshot}");
    }

    ValueTaskSourceStatus IValueTaskSource<ReadResult>.GetStatus(short token)
        => _readAwaiter.GetStatus(token);

    void IValueTaskSource<ReadResult>.OnCompleted(Action<object?> continuation,
        object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _readAwaiter.OnCompleted(continuation, state, token, flags);
}
