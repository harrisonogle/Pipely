----------------------------- MODULE Backpressure -----------------------------
(***************************************************************************)
(* Flush-side awaiter with resume-threshold hysteresis.  Covers spec §6.3   *)
(* (FlushAsync backpressure path), §8.4 (writer arms flush awaiter), §8.5  *)
(* (resume-threshold hysteresis subtlety).                                  *)
(*                                                                          *)
(* Symmetric to Awaiter.tla but for the writer's _flushAwaiter.  The         *)
(* distinguishing feature is numerical:                                     *)
(*                                                                          *)
(*   outstanding = BytesWritten - BytesRead                                 *)
(*                                                                          *)
(*   The reader signals the parked writer only when                         *)
(*   outstanding < ResumeWriterThreshold (hysteresis) OR readerDone         *)
(*   (bypass).  Signalling on bare < PauseWriterThreshold would produce     *)
(*   wake-park thrash.                                                      *)
(*                                                                          *)
(* Differential experiments:                                                *)
(*   - EnableHysteresisBypass = FALSE: drop the readerDone bypass in        *)
(*     R_MaybeSignalWriter.  NoLostWakeupOnReaderComplete expected to fail. *)
(*   - EnableResumeHysteresis = FALSE: signal when outstanding < Pause      *)
(*     instead of < Resume.  HysteresisCorrectness expected to fail.        *)
(***************************************************************************)

EXTENDS MRVTS, Integers, Sequences, TLC

CONSTANTS
    Pause,                      \* PauseWriterThreshold (integer > Resume)
    Resume,                     \* ResumeWriterThreshold (integer >= 0)
    ChunkSize,                  \* bytes per Produce / Consume
    MaxProduces,                \* bound on W_Produce invocations
    MaxConsumes,                \* bound on R_Consume invocations
    EnableReaderFence,          \* TRUE (correct) | FALSE (drops R-side fence)
    EnableHysteresisBypass,     \* TRUE (correct) | FALSE (readerDone bypass off)
    EnableResumeHysteresis      \* TRUE (correct, use Resume) | FALSE (use Pause)

ASSUME Pause > Resume /\ Resume >= 0
ASSUME ChunkSize > 0

AwaiterStates == {"Idle", "Armed", "Signaled"}
NoneEx == NoException

\* --------------------------------------------------------------------------
\* Variables
\* --------------------------------------------------------------------------

VARIABLES
    memBytesWritten,         \* writer's release-stored produce counter
    memBytesRead,            \* reader's release-stored consume counter
    memReaderCompletionState,\* 0 active | 2 completed
    memReaderException,      \* NoneEx | exception tag
    memWriterAwaiterState    \* flush awaiter state

\* Reader's store buffer (for its release-store of bytesRead and completion)
VARIABLES bufR

\* Writer's MRVTS (flush awaiter)
VARIABLES writerMrvts

VARIABLES
    writerParked,            \* TRUE while writer is blocked in FlushAsync await
    writerGotException,      \* TRUE iff GetResult (would have) thrown reader ex
    producedCount,
    consumedCount,
    readerCompleted,
    writerMrvtsLastSignal    \* last FlushResult { IsCompleted } the writer saw

VARIABLES wpc, rpc, dpc

vars == <<memBytesWritten, memBytesRead, memReaderCompletionState,
          memReaderException, memWriterAwaiterState, bufR,
          writerMrvts, writerParked, writerGotException,
          producedCount, consumedCount, readerCompleted,
          writerMrvtsLastSignal,
          wpc, rpc, dpc>>

\* --------------------------------------------------------------------------
\* Init
\* --------------------------------------------------------------------------

Init ==
    /\ memBytesWritten          = 0
    /\ memBytesRead             = 0
    /\ memReaderCompletionState = 0
    /\ memReaderException       = NoneEx
    /\ memWriterAwaiterState    = "Idle"
    /\ bufR                     = <<>>
    /\ writerMrvts              = MrvtsNew
    /\ writerParked             = FALSE
    /\ writerGotException       = FALSE
    /\ producedCount            = 0
    /\ consumedCount            = 0
    /\ readerCompleted          = FALSE
    /\ writerMrvtsLastSignal    = [IsCompleted |-> FALSE]
    /\ wpc                      = "Ready"
    /\ rpc                      = "Ready"
    /\ dpc                      = "Idle"

\* --------------------------------------------------------------------------
\* Drain of reader's buffer
\* --------------------------------------------------------------------------

DrainR ==
    /\ Len(bufR) > 0
    /\ LET e == Head(bufR) IN
         \/ /\ e.addr = "BytesRead"
            /\ memBytesRead' = e.val
            /\ UNCHANGED <<memReaderCompletionState, memReaderException>>
         \/ /\ e.addr = "ReaderCompletion"
            /\ memReaderCompletionState' = e.val
            /\ UNCHANGED <<memBytesRead, memReaderException>>
         \/ /\ e.addr = "ReaderException"
            /\ memReaderException' = e.val
            /\ UNCHANGED <<memBytesRead, memReaderCompletionState>>
    /\ bufR' = Tail(bufR)
    /\ UNCHANGED <<memBytesWritten, memWriterAwaiterState,
                   writerMrvts, writerParked, writerGotException,
                   producedCount, consumedCount, readerCompleted,
                   writerMrvtsLastSignal,
                   wpc, rpc, dpc>>

\* --------------------------------------------------------------------------
\* Writer actions
\* --------------------------------------------------------------------------

\* Writer's view of outstanding.  Writer uses its OWN memBytesWritten
\* (buffer-forwarded — writer knows its latest release-store value) and
\* acquires memBytesRead from memory.
Outstanding == memBytesWritten - memBytesRead

\* W_Produce — advance bytesWritten.  Models §6.1–§6.3 up to FlushAsync's
\* backpressure decision.  Writer's release-store is atomic here because
\* we abstract over the release mechanism (the chain-structure detail is
\* in Publication.tla).  The writer then decides whether to Flush.
W_Produce ==
    /\ wpc = "Ready"
    /\ ~readerCompleted
    /\ producedCount < MaxProduces
    /\ memBytesWritten' = memBytesWritten + ChunkSize
    /\ producedCount' = producedCount + 1
    /\ UNCHANGED <<memBytesRead, memReaderCompletionState, memReaderException,
                   memWriterAwaiterState, bufR,
                   writerMrvts, writerParked, writerGotException,
                   consumedCount, readerCompleted, writerMrvtsLastSignal,
                   wpc, rpc, dpc>>

\* W_FlushAndMaybeArm — §6.3 + §8.4.  Writer's FlushAsync flow: check reader
\* done; check backpressure; if Outstanding >= Pause and not done, arm.
\* Otherwise return sync result.
\*
\* Fused for the same reason as Awaiter's W_PublishAndSignal: the reader's
\* release-stores and CAS happen without writer yielding.  The writer's
\* atomic action captures the whole flush decision.
W_FlushAndMaybeArm ==
    /\ wpc = "Ready"
    /\ writerMrvts.continuation = NoContinuation
    /\ \/ \* Fast path: outstanding below Pause OR reader done.
          /\ Outstanding < Pause \/ memReaderCompletionState = 2
          /\ UNCHANGED <<memWriterAwaiterState, writerMrvts, writerParked,
                          writerGotException,
                          writerMrvtsLastSignal, wpc, rpc>>
       \/ \* Slow path: Outstanding >= Pause and not done → arm.
          /\ Outstanding >= Pause
          /\ memReaderCompletionState /= 2
          /\ writerMrvts' = MrvtsReset(writerMrvts)
          /\ memWriterAwaiterState' = "Armed"
          /\ wpc' = "WtrArmed"
          /\ UNCHANGED <<writerParked, writerGotException,
                          writerMrvtsLastSignal, rpc>>
    /\ UNCHANGED <<memBytesWritten, memBytesRead, memReaderCompletionState,
                   memReaderException, bufR,
                   producedCount, consumedCount, readerCompleted, dpc>>

\* W_ArmedReCheck — §8.4 step C.  After Armed, re-check backpressure.  If
\* drained OR reader done, try to un-arm.  Else proceed to park.
W_ArmedReCheck ==
    /\ wpc = "WtrArmed"
    /\ IF Outstanding < Pause \/ memReaderCompletionState = 2
       THEN \* Try to un-arm (CAS Armed -> Idle).
            \/ /\ memWriterAwaiterState = "Armed"
               /\ memWriterAwaiterState' = "Idle"
               /\ wpc' = "Ready"
               /\ UNCHANGED <<writerMrvts, writerParked, writerGotException,
                               writerMrvtsLastSignal>>
            \/ /\ memWriterAwaiterState = "Signaled"
               \* Reader already signaled; proceed to parked + eventual GetResult.
               /\ wpc' = "WtrParked"
               /\ writerParked' = TRUE
               /\ UNCHANGED <<memWriterAwaiterState, writerMrvts,
                               writerGotException, writerMrvtsLastSignal>>
       ELSE /\ wpc' = "WtrParked"
            /\ writerParked' = TRUE
            /\ UNCHANGED <<memWriterAwaiterState, writerMrvts,
                            writerGotException, writerMrvtsLastSignal>>
    /\ UNCHANGED <<memBytesWritten, memBytesRead, memReaderCompletionState,
                   memReaderException, bufR,
                   producedCount, consumedCount, readerCompleted, rpc, dpc>>

\* W_RegisterOnCompleted — runtime registers the writer's resumption callback
\* on writerMrvts.  Inline dispatch if terminal, else store.
W_RegisterOnCompleted ==
    /\ wpc = "WtrParked"
    /\ writerMrvts.continuation = NoContinuation
    /\ dpc = "Idle"
    /\ CanOnCompleted(writerMrvts, writerMrvts.version)
    /\ IF MrvtsIsCompleted(writerMrvts)
       THEN /\ UNCHANGED writerMrvts
            /\ dpc' = "RanContinuation"
       ELSE /\ writerMrvts' = MrvtsStoreContinuation(writerMrvts, "writerCb",
                                                      writerMrvts.version)
            /\ UNCHANGED dpc
    /\ UNCHANGED <<memBytesWritten, memBytesRead, memReaderCompletionState,
                   memReaderException, memWriterAwaiterState, bufR,
                   writerParked, writerGotException,
                   producedCount, consumedCount, readerCompleted,
                   writerMrvtsLastSignal,
                   wpc, rpc>>

\* D_Dispatch — MRVTS dispatcher fires.
D_Dispatch ==
    /\ dpc \in {"Idle", "PendingDispatch"}
    /\ MrvtsIsCompleted(writerMrvts)
    /\ writerMrvts.continuation /= NoContinuation
    /\ writerMrvts' = MrvtsAfterSchedule(writerMrvts)
    /\ dpc' = "RanContinuation"
    /\ UNCHANGED <<memBytesWritten, memBytesRead, memReaderCompletionState,
                   memReaderException, memWriterAwaiterState, bufR,
                   writerParked, writerGotException,
                   producedCount, consumedCount, readerCompleted,
                   writerMrvtsLastSignal,
                   wpc, rpc>>

\* W_GetResultFlush — retrieves FlushResult and clears state.  Also checks
\* for reader-side exception (§10.4 symmetric).
W_GetResultFlush ==
    /\ wpc = "WtrParked"
    /\ dpc = "RanContinuation"
    /\ CanGetResult(writerMrvts, writerMrvts.version)
    /\ memWriterAwaiterState' = "Idle"
    /\ writerParked' = FALSE
    /\ writerGotException' = (memReaderCompletionState = 2
                              /\ memReaderException /= NoneEx)
    /\ wpc' = "Ready"
    /\ UNCHANGED <<memBytesWritten, memBytesRead, memReaderCompletionState,
                   memReaderException, bufR,
                   writerMrvts, writerMrvtsLastSignal,
                   producedCount, consumedCount, readerCompleted,
                   rpc, dpc>>

D_PostGet ==
    /\ dpc = "RanContinuation"
    /\ wpc = "Ready"
    /\ dpc' = "Idle"
    /\ UNCHANGED <<memBytesWritten, memBytesRead, memReaderCompletionState,
                   memReaderException, memWriterAwaiterState, bufR,
                   writerMrvts, writerParked, writerGotException,
                   producedCount, consumedCount, readerCompleted,
                   writerMrvtsLastSignal,
                   wpc, rpc>>

W_Halt ==
    /\ wpc = "Ready"
    /\ (producedCount >= MaxProduces \/ readerCompleted)
    /\ ~writerParked
    /\ wpc' = "Halt"
    /\ UNCHANGED <<memBytesWritten, memBytesRead, memReaderCompletionState,
                   memReaderException, memWriterAwaiterState, bufR,
                   writerMrvts, writerParked, writerGotException,
                   producedCount, consumedCount, readerCompleted,
                   writerMrvtsLastSignal,
                   rpc, dpc>>

\* --------------------------------------------------------------------------
\* Reader actions
\* --------------------------------------------------------------------------

\* R_ConsumeAndSignal — reader's equivalent of §7.3 step 5 (release-store
\* BytesReadPublished) and step 6 (MaybeSignalWriterAwaiter, §8.4/§8.5).
\* Fused for the same atomicity reasons as W_PublishAndSignal in Awaiter.tla.
\*
\* Hysteresis: signal only when outstanding < Resume (correct) or < Pause
\* (differential EnableResumeHysteresis=FALSE).
R_ConsumeAndSignal ==
    /\ rpc = "Ready"
    /\ consumedCount < MaxConsumes
    /\ memBytesRead + ChunkSize <= memBytesWritten
    /\ LET newBR == memBytesRead + ChunkSize
           signalThreshold == IF EnableResumeHysteresis THEN Resume ELSE Pause
           outstandingAfter == memBytesWritten - newBR
       IN
       /\ consumedCount' = consumedCount + 1
       /\ IF EnableReaderFence
          THEN /\ memBytesRead' = newBR
               /\ UNCHANGED bufR
          ELSE /\ bufR' = Append(bufR, [addr |-> "BytesRead", val |-> newBR])
               /\ UNCHANGED memBytesRead
       /\ \* Signal-check.  §8.5: reader signals only when outstanding <
          \* Resume (or bypassed by readerCompletion).  Here the reader
          \* hasn't completed, so only the drain-threshold path applies.
          IF memWriterAwaiterState = "Armed"
             /\ outstandingAfter < signalThreshold
          THEN /\ memWriterAwaiterState' = "Signaled"
               /\ CanSetResult(writerMrvts)
               /\ writerMrvts' = MrvtsSetResult(writerMrvts, "FlushRes")
               /\ writerMrvtsLastSignal' =
                    [IsCompleted |-> memReaderCompletionState = 2]
          ELSE UNCHANGED <<memWriterAwaiterState, writerMrvts,
                            writerMrvtsLastSignal>>
    /\ UNCHANGED <<memBytesWritten, memReaderCompletionState, memReaderException,
                   writerParked, writerGotException,
                   producedCount, readerCompleted,
                   wpc, rpc, dpc>>

\* R_Complete — §7.4 / §8.5 bypass.  Reader sets ReaderCompletionState = 2
\* (optionally with exception), then signals writer regardless of
\* hysteresis (the bypass).  Differential: when EnableHysteresisBypass =
\* FALSE, only signal if outstanding < Resume, losing the reader-complete
\* wakeup.
R_Complete ==
    /\ rpc = "Ready"
    /\ ~readerCompleted
    /\ readerCompleted' = TRUE
    /\ \E ex \in {NoneEx} \cup Exceptions :
         IF EnableReaderFence
         THEN /\ memReaderException' = ex
              /\ memReaderCompletionState' = 2
              /\ UNCHANGED bufR
         ELSE /\ bufR' = Append(Append(bufR,
                              [addr |-> "ReaderException", val |-> ex]),
                              [addr |-> "ReaderCompletion", val |-> 2])
              /\ UNCHANGED <<memReaderException, memReaderCompletionState>>
    /\ \* Signal writer.  Correct behaviour: bypass hysteresis when reader
       \* completed (§8.5).  Differential: condition only on outstanding <
       \* Resume, losing the completion wakeup.
       LET signalCond ==
            IF EnableHysteresisBypass
            THEN TRUE   \* unconditional wake on completion
            ELSE Outstanding <
                   (IF EnableResumeHysteresis THEN Resume ELSE Pause)
       IN
       IF memWriterAwaiterState = "Armed" /\ signalCond
       THEN /\ memWriterAwaiterState' = "Signaled"
            /\ CanSetResult(writerMrvts)
            /\ writerMrvts' = MrvtsSetResult(writerMrvts, "FlushRes")
            /\ writerMrvtsLastSignal' = [IsCompleted |-> TRUE]
       ELSE UNCHANGED <<memWriterAwaiterState, writerMrvts,
                         writerMrvtsLastSignal>>
    /\ UNCHANGED <<memBytesWritten, memBytesRead,
                   writerParked, writerGotException,
                   producedCount, consumedCount,
                   wpc, rpc, dpc>>

R_Halt ==
    /\ rpc = "Ready"
    /\ (consumedCount >= MaxConsumes \/ readerCompleted)
    /\ wpc = "Halt"
    /\ rpc' = "Halt"
    /\ UNCHANGED <<memBytesWritten, memBytesRead, memReaderCompletionState,
                   memReaderException, memWriterAwaiterState, bufR,
                   writerMrvts, writerParked, writerGotException,
                   producedCount, consumedCount, readerCompleted,
                   writerMrvtsLastSignal,
                   wpc, dpc>>

\* --------------------------------------------------------------------------
\* Next
\* --------------------------------------------------------------------------

Next ==
    \/ DrainR
    \/ W_Produce
    \/ W_FlushAndMaybeArm
    \/ W_ArmedReCheck
    \/ W_RegisterOnCompleted
    \/ W_GetResultFlush
    \/ D_Dispatch
    \/ D_PostGet
    \/ W_Halt
    \/ R_ConsumeAndSignal
    \/ R_Complete
    \/ R_Halt

Fairness ==
    /\ WF_vars(DrainR)
    /\ WF_vars(W_ArmedReCheck)
    /\ WF_vars(W_RegisterOnCompleted)
    /\ WF_vars(D_Dispatch)
    /\ WF_vars(D_PostGet)
    /\ WF_vars(W_GetResultFlush)
    /\ WF_vars(W_Halt)
    /\ WF_vars(R_Halt)

Spec == Init /\ [][Next]_vars /\ Fairness

\* --------------------------------------------------------------------------
\* Invariants
\* --------------------------------------------------------------------------

TypeOK ==
    /\ memBytesWritten \in 0..(MaxProduces * ChunkSize)
    /\ memBytesRead \in 0..(MaxConsumes * ChunkSize)
    /\ memReaderCompletionState \in {0, 2}
    /\ memReaderException \in Exceptions \cup {NoneEx}
    /\ memWriterAwaiterState \in AwaiterStates
    /\ writerMrvts \in MrvtsSchema
    /\ writerParked \in BOOLEAN
    /\ writerGotException \in BOOLEAN
    /\ producedCount \in 0..MaxProduces
    /\ consumedCount \in 0..MaxConsumes
    /\ readerCompleted \in BOOLEAN
    /\ wpc \in {"Ready", "WtrArmed", "WtrParked", "Halt"}
    /\ rpc \in {"Ready", "Halt"}
    /\ dpc \in {"Idle", "PendingDispatch", "RanContinuation"}

\* HysteresisCorrectness: the writer is never woken by an Armed -> Signaled
\* transition when outstanding ∈ [Resume, Pause) AND reader has not
\* completed.  Violation manifests as the reader signaling prematurely,
\* producing wake-park thrash.
\*
\* We state this as: at any state where writerMrvtsLastSignal is set and
\* writer is Parked / past-GetResult, outstanding (at signal time) was
\* either < Resume (correct) or memReaderCompletionState = 2 (bypass).
\* Capture by tracking a separate variable... but to keep this simple, we
\* restate: whenever memWriterAwaiterState transitions Armed -> Signaled
\* via a reader action, the trigger must be outstanding < Resume OR
\* readerDone.  Encoded by: if ever signaled via reader path with
\* Outstanding >= Resume AND readerCompleted = FALSE, it's a bug.
HysteresisCorrectness ==
    \* At each state, if the writer's flush-awaiter is Signaled, either:
    \*   - the signal matches the correct condition (out < Resume or done), OR
    \*   - writerGotException is the terminal state we care about.
    \* The approximation: memWriterAwaiterState = Signaled /\ ~readerCompleted
    \* /\ writerParked /\ outstanding >= Resume is the bug state.
    ~(memWriterAwaiterState = "Signaled"
      /\ writerParked
      /\ ~readerCompleted
      /\ Outstanding >= Resume
      /\ \* ignore the transient between Armed->Signaled and GetResult
         dpc = "Idle")
      \* When dpc /= Idle, we're mid-dispatch — that's fine because the
      \* writer is about to GetResult and (possibly) re-park if spurious.

\* --------------------------------------------------------------------------
\* Temporal properties
\* --------------------------------------------------------------------------

\* NoLostWakeupOnDrain: if writer parked (writerParked=TRUE) and reader
\* drains below Resume, writer eventually unparks (writerParked=FALSE).
NoLostWakeupOnDrain ==
    (writerParked /\ Outstanding < Resume)
        ~> (~writerParked)

\* NoLostWakeupOnReaderComplete: if writer parked and reader completes,
\* writer eventually unparks.  Differential experiment (bypass off)
\* breaks this.
NoLostWakeupOnReaderComplete ==
    (writerParked /\ memReaderCompletionState = 2)
        ~> (~writerParked)

EventualProgress == <>(wpc = "Halt" /\ rpc = "Halt")

==============================================================================
