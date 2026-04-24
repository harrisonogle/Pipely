-------------------------- MODULE MRVTSStandalone --------------------------
(***************************************************************************)
(* Standalone state machine verifying the MRVTS operators' axiom            *)
(* consistency.  Actors:                                                    *)
(*                                                                          *)
(*   - User: drives Reset / SetResult / SetException / OnCompleted /        *)
(*           GetResult in any legal order.                                  *)
(*                                                                          *)
(*   - Dispatcher: runs scheduled continuations (models                     *)
(*           RunContinuationsAsynchronously = true + ThreadPool).           *)
(*                                                                          *)
(* Invariants verified:                                                     *)
(*                                                                          *)
(*   - TypeOK:  state is well-typed.                                        *)
(*   - VersionBounded:  mrvts.version never exceeds VersionRange.           *)
(*   - ResetDiscipline:  Reset is attempted only when previous cycle is     *)
(*                       consumed and no continuation is registered.        *)
(*                       Load-bearing: differential experiment removes it.  *)
(*   - NoOrphanDispatch:  every dispatched pair originated from a legal     *)
(*                        OnCompleted call.                                 *)
(***************************************************************************)

EXTENDS MRVTS, Integers

CONSTANTS
    MaxCompletes,           \* bound on SetResult/SetException actions
    EnforceResetDiscipline  \* TRUE (default) or FALSE (differential)

VARIABLES
    mrvts,            \* the MRVTS instance under test
    resetsDone,       \* count of Reset actions fired
    completesDone,    \* count of SetResult/SetException actions fired
    cycleConsumed,    \* TRUE iff current cycle has been consumed by GetResult
    registered,       \* set of <cb,token> pairs registered while Pending
    dispatched        \* set of <cb,token> pairs run (inline or by dispatcher)

vars == <<mrvts, resetsDone, completesDone, cycleConsumed,
          registered, dispatched>>

Init ==
    /\ mrvts         = MrvtsNew
    /\ resetsDone    = 0
    /\ completesDone = 0
    /\ cycleConsumed = TRUE
    /\ registered    = {}
    /\ dispatched    = {}

A_Reset ==
    /\ resetsDone < MaxResets
    /\ (EnforceResetDiscipline =>
           /\ cycleConsumed
           /\ mrvts.continuation = NoContinuation)
    /\ mrvts'         = MrvtsReset(mrvts)
    /\ resetsDone'    = resetsDone + 1
    /\ cycleConsumed' = TRUE
    /\ UNCHANGED <<completesDone, registered, dispatched>>

A_SetResult == \E r \in Results :
    /\ CanSetResult(mrvts)
    /\ completesDone < MaxCompletes
    /\ mrvts'         = MrvtsSetResult(mrvts, r)
    /\ completesDone' = completesDone + 1
    /\ cycleConsumed' = FALSE
    /\ UNCHANGED <<resetsDone, registered, dispatched>>

A_SetException == \E ex \in Exceptions :
    /\ CanSetException(mrvts)
    /\ completesDone < MaxCompletes
    /\ mrvts'         = MrvtsSetException(mrvts, ex)
    /\ completesDone' = completesDone + 1
    /\ cycleConsumed' = FALSE
    /\ UNCHANGED <<resetsDone, registered, dispatched>>

A_OnCompletedRegister == \E cb \in Callbacks :
    /\ mrvts.status        = "Pending"
    /\ mrvts.continuation  = NoContinuation
    /\ CanOnCompleted(mrvts, mrvts.version)
    /\ mrvts'       = MrvtsStoreContinuation(mrvts, cb, mrvts.version)
    /\ registered'  = registered \cup {<<cb, mrvts.version>>}
    /\ UNCHANGED <<resetsDone, completesDone, cycleConsumed, dispatched>>

A_OnCompletedInline == \E cb \in Callbacks :
    /\ MrvtsIsCompleted(mrvts)
    /\ CanOnCompleted(mrvts, mrvts.version)
    /\ dispatched' = dispatched \cup {<<cb, mrvts.version>>}
    /\ UNCHANGED <<mrvts, resetsDone, completesDone, cycleConsumed, registered>>

A_DispatcherRun ==
    /\ MrvtsIsCompleted(mrvts)
    /\ mrvts.continuation /= NoContinuation
    /\ dispatched' = dispatched \cup {mrvts.continuation}
    /\ mrvts'      = MrvtsAfterSchedule(mrvts)
    /\ UNCHANGED <<resetsDone, completesDone, cycleConsumed, registered>>

A_GetResult ==
    /\ CanGetResult(mrvts, mrvts.version)
    /\ cycleConsumed' = TRUE
    /\ UNCHANGED <<mrvts, resetsDone, completesDone, registered, dispatched>>

Next ==
    \/ A_Reset
    \/ A_SetResult
    \/ A_SetException
    \/ A_OnCompletedRegister
    \/ A_OnCompletedInline
    \/ A_DispatcherRun
    \/ A_GetResult

Fairness ==
    /\ WF_vars(A_DispatcherRun)
    /\ WF_vars(A_GetResult)

Spec == Init /\ [][Next]_vars /\ Fairness

TypeOK ==
    /\ mrvts \in MrvtsSchema
    /\ resetsDone    \in 0..MaxResets
    /\ completesDone \in 0..MaxCompletes
    /\ cycleConsumed \in BOOLEAN
    /\ registered    \subseteq RegisteredContinuation
    /\ dispatched    \subseteq RegisteredContinuation

VersionBounded == mrvts.version \in VersionRange

ResetDiscipline ==
    \A pair \in registered :
        pair[2] < mrvts.version => pair \in dispatched

NoOrphanDispatch == dispatched \subseteq RegisteredContinuation

==============================================================================
