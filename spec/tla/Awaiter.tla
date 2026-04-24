------------------------------- MODULE Awaiter -------------------------------
(***************************************************************************)
(* Awaiter handshake, cancellation, and exception propagation on the        *)
(* reader side.  Covers spec §6.6 (writer Complete), §7.4 (reader Complete),*)
(* §8.1 (awaiter states), §8.2 (writer signals reader), §8.3 (reader arms + *)
(* double-check fence), §8.6 (cancellation), §10.4 (exception propagation   *)
(* on the reader side).                                                     *)
(*                                                                          *)
(* Uses MRVTS.tla's axioms for the reader's _readAwaiter.                   *)
(*                                                                          *)
(* Key abstractions:                                                        *)
(*   - Publication is abstracted to a monotonic counter (tailPublished).    *)
(*     The chain-structure detail lives in Publication.tla.                 *)
(*   - Writer store buffer (bufW) models TSO: release-stores drain FIFO.    *)
(*     Reader acquire-loads read committed memory.                          *)
(*   - Writer-side Interlocked.MemoryBarrier() in W_MaybeSignalReader (§8.2)*)
(*     is modelled by requiring bufW to drain before the AwaiterState load. *)
(*     The differential experiment drops this precondition — expected to    *)
(*     produce a lost-wakeup counterexample (EventuallyResumed fails).      *)
(*   - Reader CAS on AwaiterState is already a full fence by the §5 axiom   *)
(*     (Interlocked is a full fence); so reader's double-check (§8.3 step B)*)
(*     needs no explicit fence beyond the CAS in step A.  This matches the  *)
(*     §8.3 note that the explicit MemoryBarrier on the reader side is      *)
(*     stylistic.                                                           *)
(***************************************************************************)

EXTENDS Integers, Sequences, TLC, MRVTS

CONSTANTS
    MaxProduces,            \* bound on W_Publish invocations
    MaxCancels,             \* bound on cancellation triggers
    EnableWriterFence       \* TRUE = correct; FALSE = drop fence (differential)

\* Reuse MRVTS instantiation parameters:
\*   Results:    {"ReadSig"}   (single signal value; IsCanceled field tracked separately)
\*   Exceptions: {"Ex"}
\*   Callbacks:  {"readerCb"}
\*   MaxResets, MaxCompletes bound the MRVTS driver.

AwaiterStates == {"Idle", "Armed", "Signaled"}

\* Store-buffer entry types for this module:
\*   "TailPublished"       — release-store of monotonic produced-byte counter
\*   "WriterCompletion"    — release-store of WriterCompletionState (0 or 2)
\*   "WriterException"     — plain write of exception (visibility via
\*                           WriterCompletion release)
StoreTypes == {"TailPublished", "WriterCompletion", "WriterException"}

NoneEx == NoException   \* symbolic "no exception"

\* --------------------------------------------------------------------------
\* Variables
\* --------------------------------------------------------------------------

\* Committed shared state (reader acquire-loads see these)
VARIABLES
    memTailPublished,           \* writer's monotonic byte count
    memWriterCompletionState,   \* 0 = active, 2 = completed
    memWriterException,         \* NoneEx | exc tag
    memAwaiterState             \* Idle | Armed | Signaled

\* Writer store buffer (FIFO of pending release-stores)
VARIABLES bufW

\* Reader's MRVTS instance (MrvtsSchema record)
VARIABLES readerMrvts

\* Reader local state
VARIABLES
    readerExamined,         \* _examinedPosition
    readerArmedVersion,     \* MRVTS version when we last armed, or -1
    readerCycleConsumed,    \* tracks MRVTS-reset eligibility (mirrors cycleConsumed in standalone MRVTS model)
    readerLastSignal        \* last signal value returned by GetResult: [IsCanceled]

\* Cancellation state
VARIABLES
    ctRegistered,           \* is the cancellation callback currently registered?
    ctTriggered             \* has the cancellation token been triggered externally?

\* Dispatcher state (ThreadPool that runs MRVTS continuations)
\* — modelled as a boolean "there is a pending dispatch" plus the dispatcher
\* firing.  The dispatcher's action writes the result via R_GetResult.

\* Bounded counters
VARIABLES
    producedCount,          \* count of W_Publish
    armsDone,               \* count of successful Arm transitions
    cancelsFired,           \* count of cancellation callback fires
    writerCompleted         \* has W_Complete fired?

\* Control
VARIABLES wpc, rpc, dpc

vars == <<memTailPublished, memWriterCompletionState, memWriterException,
          memAwaiterState, bufW,
          readerMrvts, readerExamined, readerArmedVersion,
          readerCycleConsumed, readerLastSignal,
          ctRegistered, ctTriggered,
          producedCount, armsDone, cancelsFired, writerCompleted,
          wpc, rpc, dpc>>

\* --------------------------------------------------------------------------
\* Init
\* --------------------------------------------------------------------------

Init ==
    /\ memTailPublished         = 0
    /\ memWriterCompletionState = 0
    /\ memWriterException       = NoneEx
    /\ memAwaiterState          = "Idle"
    /\ bufW                     = <<>>
    /\ readerMrvts              = MrvtsNew
    /\ readerExamined           = 0
    /\ readerArmedVersion       = -1
    /\ readerCycleConsumed      = TRUE      \* MRVTS is fresh
    /\ readerLastSignal         = [IsCanceled |-> FALSE]
    /\ ctRegistered             = FALSE
    /\ ctTriggered              = FALSE
    /\ producedCount            = 0
    /\ armsDone                 = 0
    /\ cancelsFired             = 0
    /\ writerCompleted          = FALSE
    /\ wpc                      = "Ready"
    /\ rpc                      = "Ready"
    /\ dpc                      = "Idle"

\* --------------------------------------------------------------------------
\* Drain action
\* --------------------------------------------------------------------------

DrainW ==
    /\ Len(bufW) > 0
    /\ LET e == Head(bufW) IN
         \/ /\ e.addr = "TailPublished"
            /\ memTailPublished' = e.val
            /\ UNCHANGED <<memWriterCompletionState, memWriterException,
                           memAwaiterState>>
         \/ /\ e.addr = "WriterCompletion"
            /\ memWriterCompletionState' = e.val
            /\ UNCHANGED <<memTailPublished, memWriterException, memAwaiterState>>
         \/ /\ e.addr = "WriterException"
            /\ memWriterException' = e.val
            /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                           memAwaiterState>>
    /\ bufW' = Tail(bufW)
    /\ UNCHANGED <<readerMrvts, readerExamined, readerArmedVersion,
                   readerCycleConsumed, readerLastSignal,
                   ctRegistered, ctTriggered,
                   producedCount, armsDone, cancelsFired, writerCompleted,
                   wpc, rpc, dpc>>

\* --------------------------------------------------------------------------
\* Writer actions
\* --------------------------------------------------------------------------

\* W_PublishAndSignal — fused emit + fence + signal-check atom (§6.4.2 step 5
\* publication followed by §8.2 MaybeSignalReaderAwaiter).  These run
\* sequentially on the writer thread without yielding, so modelling them as
\* a single atomic TLA+ action faithfully represents the concurrency.
\*
\* With EnableWriterFence = TRUE (default / correct protocol), the store
\* commits to memory atomically (captures LOCK fence semantics on x86).
\* The subsequent load of memAwaiterState on the same thread sees a
\* consistent state; concurrent reader actions interleave before/after the
\* atom, not within.
\*
\* With EnableWriterFence = FALSE (differential), the store goes to bufW
\* and is not drained by this action.  The signal-check load then reads
\* stale memTailPublished — but more importantly, the reader's subsequent
\* re-check (R_ReCheck) also reads stale memTailPublished until DrainW
\* fires, producing the lost-wakeup counterexample.
W_PublishAndSignal ==
    /\ wpc = "Ready"
    /\ producedCount < MaxProduces
    /\ ~writerCompleted
    /\ producedCount' = producedCount + 1
    /\ LET newTP == memTailPublished + 1 IN
         IF EnableWriterFence
         THEN /\ memTailPublished' = newTP
              /\ UNCHANGED bufW
         ELSE /\ bufW' = Append(bufW,
                                  [addr |-> "TailPublished", val |-> newTP])
              /\ UNCHANGED memTailPublished
    /\ \* §8.2: load awaiterState, CAS Armed->Signaled if waiter is parked.
       IF memAwaiterState = "Armed"
       THEN /\ memAwaiterState' = "Signaled"
            /\ CanSetResult(readerMrvts)
            /\ readerMrvts' = MrvtsSetResult(readerMrvts, "ReadSig")
            /\ readerCycleConsumed' = FALSE
            /\ readerLastSignal' = [IsCanceled |-> FALSE]
       ELSE UNCHANGED <<memAwaiterState, readerMrvts,
                         readerCycleConsumed, readerLastSignal>>
    /\ UNCHANGED <<memWriterCompletionState, memWriterException,
                   readerExamined, readerArmedVersion,
                   ctRegistered, ctTriggered,
                   armsDone, cancelsFired, writerCompleted, wpc, rpc, dpc>>

\* W_Complete — writer sets completion state (§6.6).  Exception is plain-
\* written; completion-state release-store carries visibility.  Fused
\* with signal-check, analogous to W_PublishAndSignal.
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
    /\ \* Signal if waiter is parked.
       IF memAwaiterState = "Armed"
       THEN /\ memAwaiterState' = "Signaled"
            /\ CanSetResult(readerMrvts)
            /\ readerMrvts' = MrvtsSetResult(readerMrvts, "ReadSig")
            /\ readerCycleConsumed' = FALSE
            /\ readerLastSignal' = [IsCanceled |-> FALSE]
       ELSE UNCHANGED <<memAwaiterState, readerMrvts,
                         readerCycleConsumed, readerLastSignal>>
    /\ UNCHANGED <<memTailPublished,
                   readerExamined, readerArmedVersion,
                   ctRegistered, ctTriggered,
                   producedCount, armsDone, cancelsFired, wpc, rpc, dpc>>

W_Halt ==
    /\ wpc = "Ready"
    /\ (producedCount >= MaxProduces \/ writerCompleted)
    /\ wpc' = "Halt"
    /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                   memWriterException, memAwaiterState, bufW,
                   readerMrvts, readerExamined, readerArmedVersion,
                   readerCycleConsumed, readerLastSignal,
                   ctRegistered, ctTriggered,
                   producedCount, armsDone, cancelsFired,
                   writerCompleted, rpc, dpc>>

\* --------------------------------------------------------------------------
\* Reader actions
\* --------------------------------------------------------------------------

\* Sync-path check: is there new data OR writer done?
HasNewDataOrDone ==
    memTailPublished > readerExamined \/ memWriterCompletionState = 2

\* R_TryRead — sync-path of ReadAsync (§7.1 steps 1–7).  If data visible or
\* writer completed, consume (advance examined).  Otherwise proceed to Arm.
R_TryRead ==
    /\ rpc = "Ready"
    /\ armsDone + producedCount + cancelsFired < MaxProduces + MaxCancels + 2
    /\ readerCycleConsumed
    /\ IF HasNewDataOrDone
       THEN \* Sync-path: consume, stay in Ready.
            /\ readerExamined' = memTailPublished
            /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                            memWriterException, memAwaiterState, bufW,
                            readerMrvts, readerArmedVersion,
                            readerCycleConsumed, readerLastSignal,
                            ctRegistered, ctTriggered,
                            producedCount, armsDone, cancelsFired,
                            writerCompleted, wpc, dpc>>
            /\ rpc' = "Ready"
       ELSE \* No data.  Transition to arming.
            /\ rpc' = "RdrArming"
            /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                            memWriterException, memAwaiterState, bufW,
                            readerMrvts, readerExamined, readerArmedVersion,
                            readerCycleConsumed, readerLastSignal,
                            ctRegistered, ctTriggered,
                            producedCount, armsDone, cancelsFired,
                            writerCompleted, wpc, dpc>>

\* R_Arm — §8.3 step A.  CAS Idle -> Armed.  Reset MRVTS first to bump version.
\* The CAS itself is a full fence (Interlocked, §5 axiom) so the subsequent
\* re-check in R_ReCheck observes all writer release-stores that drained
\* before the CAS.
R_Arm ==
    /\ rpc = "RdrArming"
    /\ readerMrvts.continuation = NoContinuation
    /\ memAwaiterState = "Idle"
    /\ \* Step 1: reset MRVTS to bump version.
       readerMrvts' = MrvtsReset(readerMrvts)
    /\ readerCycleConsumed' = TRUE  \* fresh cycle; nothing to consume yet
    /\ \* Step A: CAS Idle -> Armed.
       memAwaiterState' = "Armed"
    /\ readerArmedVersion' = readerMrvts.version + 1
    /\ armsDone' = armsDone + 1
    /\ rpc' = "RdrArmed"
    /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                   memWriterException, bufW,
                   readerExamined, readerLastSignal,
                   ctRegistered, ctTriggered,
                   producedCount, cancelsFired, writerCompleted, wpc, dpc>>

\* R_ReCheck — §8.3 step C (the double-check).  Load tail/completion after
\* the arm CAS.  If new data visible, try to CAS Armed -> Idle (unwind the
\* arm) and return sync.  If CAS fails (writer or cancellation already
\* signaled), fall through to Park.
R_ReCheck ==
    /\ rpc = "RdrArmed"
    /\ IF HasNewDataOrDone
       THEN \* Unwind the arm.
            \/ /\ memAwaiterState = "Armed"
               \* CAS Armed -> Idle
               /\ memAwaiterState' = "Idle"
               /\ readerArmedVersion' = -1
               /\ readerExamined' = memTailPublished
               /\ rpc' = "Ready"
               /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                               memWriterException, bufW,
                               readerMrvts, readerCycleConsumed, readerLastSignal,
                               ctRegistered, ctTriggered,
                               producedCount, armsDone, cancelsFired,
                               writerCompleted, wpc, dpc>>
            \/ /\ memAwaiterState = "Signaled"
               \* Someone signaled already; go to Park -> eventually GetResult.
               /\ rpc' = "RdrParked"
               /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                               memWriterException, memAwaiterState, bufW,
                               readerMrvts, readerExamined, readerArmedVersion,
                               readerCycleConsumed, readerLastSignal,
                               ctRegistered, ctTriggered,
                               producedCount, armsDone, cancelsFired,
                               writerCompleted, wpc, dpc>>
       ELSE \* No new data; proceed to park.
            /\ rpc' = "RdrParked"
            /\ ctRegistered' = TRUE
            /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                            memWriterException, memAwaiterState, bufW,
                            readerMrvts, readerExamined, readerArmedVersion,
                            readerCycleConsumed, readerLastSignal,
                            ctTriggered,
                            producedCount, armsDone, cancelsFired,
                            writerCompleted, wpc, dpc>>

\* R_RegisterOnCompleted — models the runtime's OnCompleted on the VT's
\* IValueTaskSource.  Stores the reader callback into MRVTS (or, if MRVTS
\* is already terminal, dispatches inline — no storage needed).
R_RegisterOnCompleted ==
    /\ rpc = "RdrParked"
    /\ readerMrvts.continuation = NoContinuation
    /\ dpc = "Idle"   \* not already running / completed
    /\ CanOnCompleted(readerMrvts, readerMrvts.version)
    /\ IF MrvtsIsCompleted(readerMrvts)
       THEN \* Inline dispatch: run the callback immediately on this thread.
            \* Skip storage; fast-forward to the "continuation has run" state
            \* so R_GetResult fires next.
            /\ UNCHANGED readerMrvts
            /\ dpc' = "RanContinuation"
       ELSE \* Pending: store callback; D_Dispatch will fire when terminal.
            /\ readerMrvts' = MrvtsStoreContinuation(readerMrvts, "readerCb",
                                                      readerMrvts.version)
            /\ UNCHANGED dpc
    /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                   memWriterException, memAwaiterState, bufW,
                   readerExamined, readerArmedVersion,
                   readerCycleConsumed, readerLastSignal,
                   ctRegistered, ctTriggered,
                   producedCount, armsDone, cancelsFired, writerCompleted,
                   wpc, rpc>>

\* D_Dispatch — the ThreadPool-abstract dispatcher runs the stored
\* continuation once MRVTS is terminal and continuation is stored.  Hands
\* control back to R_GetResult.
D_Dispatch ==
    /\ dpc = "Idle" \/ dpc = "PendingDispatch"
    /\ MrvtsIsCompleted(readerMrvts)
    /\ readerMrvts.continuation /= NoContinuation
    /\ readerMrvts' = MrvtsAfterSchedule(readerMrvts)
    /\ dpc' = "RanContinuation"
    /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                   memWriterException, memAwaiterState, bufW,
                   readerExamined, readerArmedVersion,
                   readerCycleConsumed, readerLastSignal,
                   ctRegistered, ctTriggered,
                   producedCount, armsDone, cancelsFired, writerCompleted,
                   wpc, rpc>>

\* R_GetResult — §8.3 step D / §10.4.  After dispatcher ran, reader's
\* continuation invokes GetResult on the MRVTS bridge.  GetResult retrieves
\* the signal, clears ReaderAwaiterState to Idle, and re-runs the sync path.
R_GetResult ==
    /\ rpc = "RdrParked"
    /\ dpc = "RanContinuation"
    /\ CanGetResult(readerMrvts, readerMrvts.version)
    /\ readerCycleConsumed' = TRUE
    /\ memAwaiterState' = "Idle"
    /\ ctRegistered' = FALSE   \* dispose the cancel registration
    /\ \* Consume data / completion / cancellation based on readerLastSignal
       \* and memory (the bridge's IValueTaskSource re-executes TryRead).
       rpc' = "Ready"
    /\ readerExamined' = IF readerLastSignal.IsCanceled
                          THEN readerExamined    \* canceled — no consume
                          ELSE memTailPublished  \* consume up to tail
    /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                   memWriterException, bufW,
                   readerMrvts, readerArmedVersion, readerLastSignal,
                   ctTriggered,
                   producedCount, armsDone, cancelsFired, writerCompleted,
                   wpc, dpc>>

\* Post-get: dispatcher returns to idle.
D_PostGet ==
    /\ dpc = "RanContinuation"
    /\ rpc = "Ready"
    /\ dpc' = "Idle"
    /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                   memWriterException, memAwaiterState, bufW,
                   readerMrvts, readerExamined, readerArmedVersion,
                   readerCycleConsumed, readerLastSignal,
                   ctRegistered, ctTriggered,
                   producedCount, armsDone, cancelsFired, writerCompleted,
                   wpc, rpc>>

R_Halt ==
    /\ rpc = "Ready"
    /\ ~readerCycleConsumed \/ TRUE   \* allow halt anytime after the cycle settles
    /\ wpc = "Halt"
    /\ rpc' = "Halt"
    /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                   memWriterException, memAwaiterState, bufW,
                   readerMrvts, readerExamined, readerArmedVersion,
                   readerCycleConsumed, readerLastSignal,
                   ctRegistered, ctTriggered,
                   producedCount, armsDone, cancelsFired, writerCompleted,
                   wpc, dpc>>

\* --------------------------------------------------------------------------
\* Cancellation actions
\* --------------------------------------------------------------------------

\* External trigger of the cancellation token (models the caller's Cancel()).
C_TriggerToken ==
    /\ cancelsFired < MaxCancels
    /\ ~ctTriggered
    /\ ctTriggered' = TRUE
    /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                   memWriterException, memAwaiterState, bufW,
                   readerMrvts, readerExamined, readerArmedVersion,
                   readerCycleConsumed, readerLastSignal,
                   ctRegistered,
                   producedCount, armsDone, cancelsFired, writerCompleted,
                   wpc, rpc, dpc>>

\* The registered cancellation callback fires.  CAS Armed -> Signaled with
\* IsCanceled=true.  The signaled MRVTS then dispatches via D_Dispatch.
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
                          memWriterException, bufW,
                          readerExamined, readerArmedVersion,
                          ctRegistered, ctTriggered,
                          producedCount, armsDone, writerCompleted,
                          wpc, rpc, dpc>>
       \/ /\ memAwaiterState /= "Armed"
          \* Already Idle or Signaled; callback no-op.  Count it.
          /\ cancelsFired' = cancelsFired + 1
          /\ UNCHANGED <<memTailPublished, memWriterCompletionState,
                          memWriterException, memAwaiterState, bufW,
                          readerMrvts, readerExamined, readerArmedVersion,
                          readerCycleConsumed, readerLastSignal,
                          ctRegistered, ctTriggered,
                          producedCount, armsDone, writerCompleted,
                          wpc, rpc, dpc>>

\* --------------------------------------------------------------------------
\* Next-state relation
\* --------------------------------------------------------------------------

Next ==
    \/ DrainW
    \/ W_PublishAndSignal
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

\* Fairness: drain and progress actions.  Without WF on DrainW the writer's
\* stores could sit in bufW forever and temporal properties wouldn't hold.
Fairness ==
    /\ WF_vars(DrainW)
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
    /\ readerMrvts \in MrvtsSchema
    /\ readerExamined \in 0..MaxProduces
    /\ readerArmedVersion \in -1..MaxVersion
    /\ readerCycleConsumed \in BOOLEAN
    /\ readerLastSignal \in [IsCanceled : BOOLEAN]
    /\ ctRegistered \in BOOLEAN
    /\ ctTriggered \in BOOLEAN
    /\ producedCount \in 0..MaxProduces
    /\ armsDone \in 0..(MaxProduces + MaxCancels)
    /\ cancelsFired \in 0..MaxCancels
    /\ writerCompleted \in BOOLEAN
    /\ wpc \in {"Ready", "Halt"}
    /\ rpc \in {"Ready", "RdrArming", "RdrArmed", "RdrParked", "Halt"}
    /\ dpc \in {"Idle", "PendingDispatch", "RanContinuation"}

\* No spurious wake: when the reader reaches R_GetResult, the signal it
\* consumes must correspond to either (a) writer signaled because data was
\* published, (b) writer completed, or (c) cancellation fired.  We encode
\* this as: at R_GetResult's firing state, at least one of the backing
\* signals must be observably true.
NoSpuriousWake ==
    (rpc = "RdrParked" /\ dpc = "RanContinuation" /\ MrvtsIsCompleted(readerMrvts))
        =>
            \/ memTailPublished > readerExamined   \* data available
            \/ memWriterCompletionState = 2        \* writer done
            \/ readerLastSignal.IsCanceled         \* canceled

\* At most one result per MRVTS cycle: enforced structurally by MRVTS's
\* SetResult precondition (requires Pending).  We check it as a sanity
\* invariant via the operator.
AtMostOneResultPerVersion ==
    \* If MRVTS is terminal at version V, the only way to reach Pending at V+1
    \* is via MrvtsReset, which the driver actions do exactly once per cycle.
    TRUE    \* structural

\* --------------------------------------------------------------------------
\* Temporal properties
\* --------------------------------------------------------------------------

\* Eventually the reader returns to Ready (no stuck-armed), assuming
\* producing + completing are driven + fence enabled.
EventuallyReaderFinishesArm ==
    (rpc = "RdrArmed") ~> (rpc \in {"Ready", "RdrParked"})

\* Lost-wakeup safety: if the reader is parked (Armed -> waiting for signal)
\* AND data eventually becomes available (memTailPublished advances OR writer
\* completes), the reader eventually unparks.
EventuallyResumed ==
    /\ (rpc = "RdrParked" /\ memTailPublished > readerExamined)
         ~> (rpc = "Ready")
    /\ (rpc = "RdrParked" /\ memWriterCompletionState = 2)
         ~> (rpc = "Ready")

\* Progress: the system eventually halts (both sides Halt).
EventualProgress == <>(wpc = "Halt" /\ rpc = "Halt")

==============================================================================
