------------------------------- MODULE Awaiter -------------------------------
(***************************************************************************)
(* Awaiter handshake, cancellation, and exception propagation on the        *)
(* reader side.  Covers spec §6.6 (writer Complete), §7.4 (reader Complete),*)
(* §8.1 (awaiter states), §8.2 (writer signals reader, including the        *)
(* §8.2.1 reader-caught-up skip), §8.3 (reader arms + double-check fence),  *)
(* §8.6 (cancellation), §10.4 (exception propagation on the reader side).   *)
(*                                                                          *)
(* Uses MRVTS.tla's axioms for the reader's _readAwaiter.                   *)
(*                                                                          *)
(* W_Publish and W_Signal are separate actions — they execute sequentially  *)
(* on the writer thread but reader-thread actions can interleave between    *)
(* them.  An earlier revision fused them; that fusion hid the race §8.2.1   *)
(* closes.                                                                  *)
(*                                                                          *)
(* Reader-side releases (memExaminedPublished) are modeled as atomic        *)
(* updates rather than through an explicit store buffer.  Rationale:        *)
(* the reader's arm CAS in §8.3 is a full fence that drains prior reader    *)
(* releases; since the writer's §8.2 signal-check loads AwaiterState        *)
(* before ExaminedPublished (TSO load-load ordering), the writer observes   *)
(* the reader's post-fence ExaminedPublished whenever it observes Armed.    *)
(* Modeling a reader-side buffer adds state without adding coverage for     *)
(* the race §8.2.1 closes.                                                  *)
(*                                                                          *)
(* Differential experiments:                                                *)
(*   - EnableWriterFence = FALSE: drop the writer-side fence between        *)
(*     W_Publish's release-store drain and W_Signal's AwaiterState load.    *)
(*   - EnableReaderCaughtUpCheck = FALSE: W_Signal skips the                *)
(*     memExaminedPublished check.  Expected: NoSpuriousWake violated.      *)
(***************************************************************************)

EXTENDS MRVTS, Integers, Sequences, TLC

CONSTANTS
    MaxProduces,                    \* bound on W_Publish invocations
    MaxCancels,                     \* bound on cancellation triggers
    EnableWriterFence,              \* TRUE = correct; FALSE = drop writer-side fence
    EnableReaderCaughtUpCheck       \* TRUE = §8.2.1 check; FALSE = differential

AwaiterStates == {"Idle", "Armed", "Signaled"}
NoneEx == NoException

\* Writer-side store-buffer entry types.
StoreTypes == {"TailPublished", "WriterCompletion", "WriterException"}

\* --------------------------------------------------------------------------
\* Variables
\* --------------------------------------------------------------------------

VARIABLES
    memTailPublished,           \* writer's monotonic byte count (committed)
    memWriterCompletionState,   \* 0 = active, 2 = completed
    memWriterException,         \* NoneEx | exc tag
    memAwaiterState,            \* Idle | Armed | Signaled
    memExaminedPublished        \* reader's examined byte position (committed, §8.2.1)

\* Writer store buffer (writer-side release-stores awaiting drain).
VARIABLES bufW

\* Reader's MRVTS instance (MrvtsSchema record).
VARIABLES readerMrvts

\* Writer-local state.
VARIABLES
    writerBytesWritten,         \* writer's synchronous byte counter (the spec's
                                 \* _bytesWritten, maintained on the writer thread)
    writerCompleted             \* TRUE iff W_Complete has fired

\* Reader local state.
VARIABLES
    readerExamined,             \* reader's _examinedPosition
    readerArmedVersion,         \* MRVTS version when last armed, or -1
    readerCycleConsumed,        \* §8.7 MRVTS reset-discipline mirror
    readerLastSignal            \* last signal value from GetResult: [IsCanceled]

\* Cancellation state.
VARIABLES
    ctRegistered,
    ctTriggered

\* Counters.
VARIABLES
    producedCount,
    armsDone,
    cancelsFired

\* Control.
VARIABLES wpc, rpc, dpc

vars == <<memTailPublished, memWriterCompletionState, memWriterException,
          memAwaiterState, memExaminedPublished, bufW,
          readerMrvts,
          writerBytesWritten, writerCompleted,
          readerExamined, readerArmedVersion,
          readerCycleConsumed, readerLastSignal,
          ctRegistered, ctTriggered,
          producedCount, armsDone, cancelsFired,
          wpc, rpc, dpc>>

\* --------------------------------------------------------------------------
\* Init
\* --------------------------------------------------------------------------

Init ==
    /\ memTailPublished         = 0
    /\ memWriterCompletionState = 0
    /\ memWriterException       = NoneEx
    /\ memAwaiterState          = "Idle"
    /\ memExaminedPublished     = 0
    /\ bufW                     = <<>>
    /\ readerMrvts              = MrvtsNew
    /\ writerBytesWritten       = 0
    /\ writerCompleted          = FALSE
    /\ readerExamined           = 0
    /\ readerArmedVersion       = -1
    /\ readerCycleConsumed      = TRUE
    /\ readerLastSignal         = [IsCanceled |-> FALSE]
    /\ ctRegistered             = FALSE
    /\ ctTriggered              = FALSE
    /\ producedCount            = 0
    /\ armsDone                 = 0
    /\ cancelsFired             = 0
    /\ wpc                      = "Ready"
    /\ rpc                      = "Ready"
    /\ dpc                      = "Idle"

\* --------------------------------------------------------------------------
\* Writer store-buffer drain
\* --------------------------------------------------------------------------

DrainW ==
    /\ Len(bufW) > 0
    /\ LET e == Head(bufW) IN
         \/ /\ e.addr = "TailPublished"
            /\ memTailPublished' = e.val
            /\ UNCHANGED <<memWriterCompletionState, memWriterException,
                           memAwaiterState, memExaminedPublished>>
         \/ /\ e.addr = "WriterCompletion"
            /\ memWriterCompletionState' = e.val
            /\ UNCHANGED <<memTailPublished, memWriterException,
                           memAwaiterState, memExaminedPublished>>
         \/ /\ e.addr = "WriterException"
            /\ memWriterException' = e.val
            /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                           memAwaiterState, memExaminedPublished>>
    /\ bufW' = Tail(bufW)
    /\ UNCHANGED <<readerMrvts,
                   writerBytesWritten, writerCompleted,
                   readerExamined, readerArmedVersion,
                   readerCycleConsumed, readerLastSignal,
                   ctRegistered, ctTriggered,
                   producedCount, armsDone, cancelsFired,
                   wpc, rpc, dpc>>

\* --------------------------------------------------------------------------
\* Writer actions — un-fused Publish + Signal
\* --------------------------------------------------------------------------

W_Publish ==
    /\ wpc = "Ready"
    /\ producedCount < MaxProduces
    /\ ~writerCompleted
    /\ producedCount' = producedCount + 1
    /\ writerBytesWritten' = writerBytesWritten + 1
    /\ LET newTP == memTailPublished + 1 IN
         IF EnableWriterFence
         THEN /\ memTailPublished' = newTP
              /\ UNCHANGED bufW
         ELSE /\ bufW' = Append(bufW,
                                  [addr |-> "TailPublished", val |-> newTP])
              /\ UNCHANGED memTailPublished
    /\ wpc' = "WtrSignaling"
    /\ UNCHANGED <<memWriterCompletionState, memWriterException,
                   memAwaiterState, memExaminedPublished,
                   readerMrvts, writerCompleted,
                   readerExamined, readerArmedVersion,
                   readerCycleConsumed, readerLastSignal,
                   ctRegistered, ctTriggered,
                   armsDone, cancelsFired, rpc, dpc>>

W_Signal ==
    /\ wpc = "WtrSignaling"
    /\ \/ /\ memAwaiterState = "Idle"
          /\ wpc' = "Ready"
          /\ UNCHANGED <<memAwaiterState, readerMrvts,
                          readerCycleConsumed, readerLastSignal>>
       \/ /\ memAwaiterState = "Signaled"
          /\ wpc' = "Ready"
          /\ UNCHANGED <<memAwaiterState, readerMrvts,
                          readerCycleConsumed, readerLastSignal>>
       \/ /\ memAwaiterState = "Armed"
          /\ LET skip == EnableReaderCaughtUpCheck
                         /\ ~writerCompleted
                         /\ memExaminedPublished >= writerBytesWritten
             IN IF skip
                THEN /\ wpc' = "Ready"
                     /\ UNCHANGED <<memAwaiterState, readerMrvts,
                                     readerCycleConsumed, readerLastSignal>>
                ELSE /\ memAwaiterState' = "Signaled"
                     /\ CanSetResult(readerMrvts)
                     /\ readerMrvts' = MrvtsSetResult(readerMrvts, "ReadSig")
                     /\ readerCycleConsumed' = FALSE
                     /\ readerLastSignal' = [IsCanceled |-> FALSE]
                     /\ wpc' = "Ready"
    /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                   memWriterException, memExaminedPublished, bufW,
                   writerBytesWritten, writerCompleted,
                   readerExamined, readerArmedVersion,
                   ctRegistered, ctTriggered,
                   producedCount, armsDone, cancelsFired, rpc, dpc>>

W_Complete ==
    /\ wpc = "Ready"
    /\ ~writerCompleted
    /\ writerCompleted' = TRUE
    /\ \E ex \in {NoneEx} \cup Exceptions :
         IF EnableWriterFence
         THEN /\ memWriterException' = ex
              /\ memWriterCompletionState' = 2
              /\ UNCHANGED bufW
         ELSE /\ bufW' = Append(Append(bufW,
                              [addr |-> "WriterException", val |-> ex]),
                              [addr |-> "WriterCompletion", val |-> 2])
              /\ UNCHANGED <<memWriterException, memWriterCompletionState>>
    /\ IF memAwaiterState = "Armed"
       THEN /\ memAwaiterState' = "Signaled"
            /\ CanSetResult(readerMrvts)
            /\ readerMrvts' = MrvtsSetResult(readerMrvts, "ReadSig")
            /\ readerCycleConsumed' = FALSE
            /\ readerLastSignal' = [IsCanceled |-> FALSE]
       ELSE UNCHANGED <<memAwaiterState, readerMrvts,
                         readerCycleConsumed, readerLastSignal>>
    /\ UNCHANGED <<memTailPublished, memExaminedPublished,
                   writerBytesWritten,
                   readerExamined, readerArmedVersion,
                   ctRegistered, ctTriggered,
                   producedCount, armsDone, cancelsFired, wpc, rpc, dpc>>

W_Halt ==
    /\ wpc = "Ready"
    /\ (producedCount >= MaxProduces \/ writerCompleted)
    /\ wpc' = "Halt"
    /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                   memWriterException, memAwaiterState, memExaminedPublished,
                   bufW, readerMrvts,
                   writerBytesWritten, writerCompleted,
                   readerExamined, readerArmedVersion,
                   readerCycleConsumed, readerLastSignal,
                   ctRegistered, ctTriggered,
                   producedCount, armsDone, cancelsFired, rpc, dpc>>

\* --------------------------------------------------------------------------
\* Reader actions
\* --------------------------------------------------------------------------

HasNewDataOrDone ==
    memTailPublished > readerExamined \/ memWriterCompletionState = 2

R_TryRead ==
    /\ rpc = "Ready"
    /\ armsDone + producedCount + cancelsFired < MaxProduces + MaxCancels + 2
    /\ readerCycleConsumed
    /\ IF HasNewDataOrDone
       THEN /\ readerExamined' = memTailPublished
            /\ memExaminedPublished' = memTailPublished   \* §7.3 step 5 (atomic)
            /\ rpc' = "Ready"
            /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                            memWriterException, memAwaiterState, bufW,
                            readerMrvts, readerArmedVersion,
                            readerCycleConsumed, readerLastSignal,
                            ctRegistered, ctTriggered,
                            producedCount, armsDone, cancelsFired,
                            writerBytesWritten, writerCompleted, wpc, dpc>>
       ELSE /\ rpc' = "RdrArming"
            /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                            memWriterException, memAwaiterState,
                            memExaminedPublished, bufW,
                            readerMrvts, readerExamined, readerArmedVersion,
                            readerCycleConsumed, readerLastSignal,
                            ctRegistered, ctTriggered,
                            producedCount, armsDone, cancelsFired,
                            writerBytesWritten, writerCompleted, wpc, dpc>>

R_Arm ==
    /\ rpc = "RdrArming"
    /\ readerMrvts.continuation = NoContinuation
    /\ memAwaiterState = "Idle"
    /\ readerMrvts'       = MrvtsReset(readerMrvts)
    /\ readerCycleConsumed' = TRUE
    /\ memAwaiterState'   = "Armed"
    /\ readerArmedVersion'= readerMrvts.version + 1
    /\ armsDone'          = armsDone + 1
    /\ rpc' = "RdrArmed"
    /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                   memWriterException, memExaminedPublished, bufW,
                   readerExamined, readerLastSignal,
                   ctRegistered, ctTriggered,
                   producedCount, cancelsFired, writerBytesWritten,
                   writerCompleted, wpc, dpc>>

R_ReCheck ==
    /\ rpc = "RdrArmed"
    /\ IF HasNewDataOrDone
       THEN \/ /\ memAwaiterState = "Armed"
               /\ memAwaiterState' = "Idle"
               /\ readerArmedVersion' = -1
               /\ readerExamined' = memTailPublished
               /\ memExaminedPublished' = memTailPublished
               /\ rpc' = "Ready"
               /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                               memWriterException, bufW,
                               readerMrvts, readerCycleConsumed, readerLastSignal,
                               ctRegistered, ctTriggered,
                               producedCount, armsDone, cancelsFired,
                               writerBytesWritten, writerCompleted, wpc, dpc>>
            \/ /\ memAwaiterState = "Signaled"
               /\ rpc' = "RdrParked"
               /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                               memWriterException, memAwaiterState,
                               memExaminedPublished, bufW,
                               readerMrvts, readerExamined, readerArmedVersion,
                               readerCycleConsumed, readerLastSignal,
                               ctRegistered, ctTriggered,
                               producedCount, armsDone, cancelsFired,
                               writerBytesWritten, writerCompleted, wpc, dpc>>
       ELSE /\ rpc' = "RdrParked"
            /\ ctRegistered' = TRUE
            /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                            memWriterException, memAwaiterState,
                            memExaminedPublished, bufW,
                            readerMrvts, readerExamined, readerArmedVersion,
                            readerCycleConsumed, readerLastSignal,
                            ctTriggered,
                            producedCount, armsDone, cancelsFired,
                            writerBytesWritten, writerCompleted, wpc, dpc>>

R_RegisterOnCompleted ==
    /\ rpc = "RdrParked"
    /\ readerMrvts.continuation = NoContinuation
    /\ dpc = "Idle"
    /\ CanOnCompleted(readerMrvts, readerMrvts.version)
    /\ IF MrvtsIsCompleted(readerMrvts)
       THEN /\ UNCHANGED readerMrvts
            /\ dpc' = "RanContinuation"
       ELSE /\ readerMrvts' = MrvtsStoreContinuation(readerMrvts, "readerCb",
                                                      readerMrvts.version)
            /\ UNCHANGED dpc
    /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                   memWriterException, memAwaiterState, memExaminedPublished,
                   bufW,
                   readerExamined, readerArmedVersion,
                   readerCycleConsumed, readerLastSignal,
                   ctRegistered, ctTriggered,
                   producedCount, armsDone, cancelsFired,
                   writerBytesWritten, writerCompleted,
                   wpc, rpc>>

D_Dispatch ==
    /\ dpc \in {"Idle", "PendingDispatch"}
    /\ MrvtsIsCompleted(readerMrvts)
    /\ readerMrvts.continuation /= NoContinuation
    /\ readerMrvts' = MrvtsAfterSchedule(readerMrvts)
    /\ dpc' = "RanContinuation"
    /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                   memWriterException, memAwaiterState, memExaminedPublished,
                   bufW,
                   readerExamined, readerArmedVersion,
                   readerCycleConsumed, readerLastSignal,
                   ctRegistered, ctTriggered,
                   producedCount, armsDone, cancelsFired,
                   writerBytesWritten, writerCompleted,
                   wpc, rpc>>

R_GetResult ==
    /\ rpc = "RdrParked"
    /\ dpc = "RanContinuation"
    /\ CanGetResult(readerMrvts, readerMrvts.version)
    /\ readerCycleConsumed' = TRUE
    /\ memAwaiterState' = "Idle"
    /\ ctRegistered' = FALSE
    /\ rpc' = "Ready"
    /\ readerExamined' = IF readerLastSignal.IsCanceled
                          THEN readerExamined
                          ELSE memTailPublished
    /\ memExaminedPublished' = IF readerLastSignal.IsCanceled
                                THEN memExaminedPublished
                                ELSE memTailPublished
    /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                   memWriterException, bufW,
                   readerMrvts, readerArmedVersion, readerLastSignal,
                   ctTriggered,
                   producedCount, armsDone, cancelsFired,
                   writerBytesWritten, writerCompleted,
                   wpc, dpc>>

D_PostGet ==
    /\ dpc = "RanContinuation"
    /\ rpc = "Ready"
    /\ dpc' = "Idle"
    /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                   memWriterException, memAwaiterState, memExaminedPublished,
                   bufW,
                   readerMrvts, readerExamined, readerArmedVersion,
                   readerCycleConsumed, readerLastSignal,
                   ctRegistered, ctTriggered,
                   producedCount, armsDone, cancelsFired,
                   writerBytesWritten, writerCompleted,
                   wpc, rpc>>

R_Halt ==
    /\ rpc = "Ready"
    /\ wpc = "Halt"
    /\ rpc' = "Halt"
    /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                   memWriterException, memAwaiterState, memExaminedPublished,
                   bufW, readerMrvts,
                   readerExamined, readerArmedVersion,
                   readerCycleConsumed, readerLastSignal,
                   ctRegistered, ctTriggered,
                   producedCount, armsDone, cancelsFired,
                   writerBytesWritten, writerCompleted, wpc, dpc>>

\* --------------------------------------------------------------------------
\* Cancellation actions
\* --------------------------------------------------------------------------

C_TriggerToken ==
    /\ cancelsFired < MaxCancels
    /\ ~ctTriggered
    /\ ctTriggered' = TRUE
    /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                   memWriterException, memAwaiterState, memExaminedPublished,
                   bufW, readerMrvts,
                   writerBytesWritten, writerCompleted,
                   readerExamined, readerArmedVersion,
                   readerCycleConsumed, readerLastSignal,
                   ctRegistered,
                   producedCount, armsDone, cancelsFired,
                   wpc, rpc, dpc>>

C_CallbackFire ==
    /\ ctRegistered
    /\ ctTriggered
    /\ cancelsFired < MaxCancels
    /\ \/ /\ memAwaiterState = "Armed"
          /\ memAwaiterState' = "Signaled"
          /\ CanSetResult(readerMrvts)
          /\ readerMrvts' = MrvtsSetResult(readerMrvts, "ReadSig")
          /\ readerCycleConsumed' = FALSE
          /\ readerLastSignal' = [IsCanceled |-> TRUE]
          /\ cancelsFired' = cancelsFired + 1
          /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                          memWriterException, memExaminedPublished, bufW,
                          writerBytesWritten, writerCompleted,
                          readerExamined, readerArmedVersion,
                          ctRegistered, ctTriggered,
                          producedCount, armsDone, wpc, rpc, dpc>>
       \/ /\ memAwaiterState /= "Armed"
          /\ cancelsFired' = cancelsFired + 1
          /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                          memWriterException, memAwaiterState,
                          memExaminedPublished, bufW,
                          readerMrvts, readerExamined, readerArmedVersion,
                          readerCycleConsumed, readerLastSignal,
                          ctRegistered, ctTriggered,
                          producedCount, armsDone,
                          writerBytesWritten, writerCompleted,
                          wpc, rpc, dpc>>

\* --------------------------------------------------------------------------
\* Next / Spec
\* --------------------------------------------------------------------------

Next ==
    \/ DrainW
    \/ W_Publish
    \/ W_Signal
    \/ W_Complete
    \/ W_Halt
    \/ R_TryRead
    \/ R_Arm
    \/ R_ReCheck
    \/ R_RegisterOnCompleted
    \/ R_GetResult
    \/ D_Dispatch
    \/ D_PostGet
    \/ R_Halt
    \/ C_TriggerToken
    \/ C_CallbackFire

Fairness ==
    /\ WF_vars(DrainW)
    /\ WF_vars(W_Signal)
    /\ WF_vars(D_Dispatch)
    /\ WF_vars(D_PostGet)
    /\ WF_vars(R_GetResult)
    /\ WF_vars(R_ReCheck)
    /\ WF_vars(R_RegisterOnCompleted)
    /\ WF_vars(W_Halt)
    /\ WF_vars(R_Halt)

Spec == Init /\ [][Next]_vars /\ Fairness

\* --------------------------------------------------------------------------
\* Invariants
\* --------------------------------------------------------------------------

TypeOK ==
    /\ memTailPublished \in 0..MaxProduces
    /\ memWriterCompletionState \in {0, 2}
    /\ memWriterException \in Exceptions \cup {NoneEx}
    /\ memAwaiterState \in AwaiterStates
    /\ memExaminedPublished \in 0..MaxProduces
    /\ readerMrvts \in MrvtsSchema
    /\ writerBytesWritten \in 0..MaxProduces
    /\ writerCompleted \in BOOLEAN
    /\ readerExamined \in 0..MaxProduces
    /\ readerArmedVersion \in -1..MaxVersion
    /\ readerCycleConsumed \in BOOLEAN
    /\ readerLastSignal \in [IsCanceled : BOOLEAN]
    /\ ctRegistered \in BOOLEAN
    /\ ctTriggered \in BOOLEAN
    /\ producedCount \in 0..MaxProduces
    /\ armsDone \in 0..(MaxProduces + MaxCancels)
    /\ cancelsFired \in 0..MaxCancels
    /\ wpc \in {"Ready", "WtrSignaling", "Halt"}
    /\ rpc \in {"Ready", "RdrArming", "RdrArmed", "RdrParked", "Halt"}
    /\ dpc \in {"Idle", "PendingDispatch", "RanContinuation"}

\* NoSpuriousWake: when R_GetResult runs, the signal corresponds to new
\* data past examined, or completion, or cancellation.  Holds under §8.2.1;
\* differential EnableReaderCaughtUpCheck=FALSE violates.
NoSpuriousWake ==
    (rpc = "RdrParked" /\ dpc = "RanContinuation" /\ MrvtsIsCompleted(readerMrvts))
        =>
            \/ memTailPublished > readerExamined
            \/ memWriterCompletionState = 2
            \/ readerLastSignal.IsCanceled

\* --------------------------------------------------------------------------
\* Temporal properties
\* --------------------------------------------------------------------------

EventuallyReaderFinishesArm ==
    (rpc = "RdrArmed") ~> (rpc \in {"Ready", "RdrParked"})

EventuallyResumed ==
    /\ (rpc = "RdrParked" /\ memTailPublished > readerExamined)
         ~> (rpc = "Ready")
    /\ (rpc = "RdrParked" /\ memWriterCompletionState = 2)
         ~> (rpc = "Ready")

EventualProgress == <>(wpc = "Halt" /\ rpc = "Halt")

==============================================================================
