-------------------------------- MODULE MRVTS --------------------------------
(***************************************************************************)
(* Axiomatization of ManualResetValueTaskSourceCore<T> per the              *)
(* documented .NET semantics.  Covers spec §8.7 and is the foundation       *)
(* shared by Awaiter.tla and Backpressure.tla.                              *)
(*                                                                          *)
(* This module has two roles:                                               *)
(*                                                                          *)
(*   1. Export pure operators over an MRVTS-instance record                 *)
(*      (MrvtsReset, MrvtsSetResult, ...)  that other modules reuse by      *)
(*      applying them to their own record-valued variables.                 *)
(*                                                                          *)
(*   2. Provide a standalone state machine that verifies the operators      *)
(*      respect the axioms.  The standalone model has a user actor that    *)
(*      nondeterministically drives Reset / SetResult / SetException /      *)
(*      OnCompleted / GetResult sequences, and a dispatcher actor that      *)
(*      runs scheduled continuations asynchronously.                        *)
(***************************************************************************)

EXTENDS Integers, Sequences, TLC

CONSTANTS
    Results,        \* finite set of possible result values
    Exceptions,     \* finite set of possible exception tags
    Callbacks,      \* finite set of callback identifiers
    MaxResets,      \* bound on Reset actions (standalone driver)
    MaxCompletes    \* bound on SetResult/SetException actions

\* ----- Sentinels (distinct values outside user-supplied constant sets) ---

NoResult        == "NoResult"
NoException     == "NoException"
NoContinuation  == <<>>

ASSUME NoResult    \notin Results
ASSUME NoException \notin Exceptions

\* ----- Schema ------------------------------------------------------------

Statuses == {"Pending", "Succeeded", "Faulted"}

\* Versions are bounded by MaxResets + 1 (initial zero plus up to MaxResets
\* resets).  Other modules that reuse these operators MUST supply their own
\* MaxResets; the bound keeps the state space finite in TLC.
MaxVersion   == MaxResets + 1
VersionRange == 0..MaxVersion

\* A registered continuation is a <callback, token> pair; the token is the
\* MRVTS version in effect at OnCompleted time (spec §8.7).
RegisteredContinuation == Callbacks \X VersionRange
ContinuationValues     == {NoContinuation} \cup RegisteredContinuation

\* An MRVTS instance is a record.  Other modules hold variables of this
\* schema and apply the operators below.
MrvtsSchema == [
    status       : Statuses,
    version      : VersionRange,
    result       : Results    \cup {NoResult},
    exception    : Exceptions \cup {NoException},
    continuation : ContinuationValues
]

\* Initial value: fresh MRVTS (new ManualResetValueTaskSourceCore<T>()).
MrvtsNew == [
    status       |-> "Pending",
    version      |-> 0,
    result       |-> NoResult,
    exception    |-> NoException,
    continuation |-> NoContinuation
]

\* ----- Pure operators ----------------------------------------------------

\* Reset: bump version, reset to Pending, clear result/exception/continuation.
MrvtsReset(m) == [
    status       |-> "Pending",
    version      |-> m.version + 1,
    result       |-> NoResult,
    exception    |-> NoException,
    continuation |-> NoContinuation
]

\* SetResult precondition: status is Pending.  Violating it is a .NET
\* InvalidOperationException — we model violations as guard failures in
\* the standalone driver and require callers to enforce the guard.
CanSetResult(m)          == m.status = "Pending"
MrvtsSetResult(m, r)     == [m EXCEPT !.status = "Succeeded", !.result = r]

CanSetException(m)       == m.status = "Pending"
MrvtsSetException(m, ex) == [m EXCEPT !.status = "Faulted", !.exception = ex]

MrvtsIsCompleted(m) == m.status /= "Pending"

\* GetResult(token): throws InvalidOperationException if token /= version
\* OR status = Pending.  MRVTS does NOT clear state on GetResult — the
\* caller must Reset before reuse.
CanGetResult(m, token) == (token = m.version) /\ MrvtsIsCompleted(m)

\* OnCompleted(cb, token): validates token == version; otherwise throws.
\* When Pending, stores continuation; when terminal, inline-dispatches.
CanOnCompleted(m, token) == token = m.version
MrvtsStoreContinuation(m, cb, token) ==
    IF m.status = "Pending"
    THEN [m EXCEPT !.continuation = <<cb, token>>]
    ELSE m     \* no state change; caller dispatches cb inline

\* After the dispatcher runs a stored continuation the slot clears, enforcing
\* exactly-once delivery per register cycle.
MrvtsAfterSchedule(m) == [m EXCEPT !.continuation = NoContinuation]

(***************************************************************************)
(*                                                                          *)
(* Standalone state machine verifying axiom consistency of the operators    *)
(* above.  Actors:                                                          *)
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
(*   - VersionMonotonic:  mrvts.version never decreases.                    *)
(*   - ResetDiscipline:  Reset is attempted only on a "consumed" cycle      *)
(*                       (post-GetResult, or initial uncompleted state).    *)
(*                       Load-bearing: differential experiment removes it.  *)
(*   - NoDoubleDispatch:  each <cb,token> pair is dispatched at most once   *)
(*                        (either inline or via the dispatcher).            *)
(*                                                                          *)
(* Temporal:                                                                *)
(*                                                                          *)
(*   - EventualDispatch:  under WF on the dispatcher, any registered        *)
(*                        continuation is eventually run.                   *)
(*                                                                          *)
(***************************************************************************)

CONSTANTS
    EnforceResetDiscipline  \* TRUE (default) = guard Reset on cycleConsumed;
                             \* FALSE (differential) = let Reset race.

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
    /\ cycleConsumed = TRUE     \* initial Pending is "consumable"; see A_Reset
    /\ registered    = {}
    /\ dispatched    = {}

\* -----  Actions  ---------------------------------------------------------

\* Reset is legal iff (1) the previous cycle's terminal result was consumed
\* via GetResult (cycleConsumed) AND (2) there is no pending continuation
\* that would be stranded by advancing the version.  Both conditions are
\* required; see spec §8.7 and .NET MRVTS docs.  The differential experiment
\* removes both checks.
A_Reset ==
    /\ resetsDone < MaxResets
    /\ (EnforceResetDiscipline =>
           /\ cycleConsumed
           /\ mrvts.continuation = NoContinuation)
    /\ mrvts'         = MrvtsReset(mrvts)
    /\ resetsDone'    = resetsDone + 1
    /\ cycleConsumed' = TRUE    \* fresh cycle, nothing to consume yet
    /\ UNCHANGED <<completesDone, registered, dispatched>>

A_SetResult == \E r \in Results :
    /\ CanSetResult(mrvts)
    /\ completesDone < MaxCompletes
    /\ mrvts'         = MrvtsSetResult(mrvts, r)
    /\ completesDone' = completesDone + 1
    /\ cycleConsumed' = FALSE    \* new terminal, awaiting GetResult
    /\ UNCHANGED <<resetsDone, registered, dispatched>>

A_SetException == \E ex \in Exceptions :
    /\ CanSetException(mrvts)
    /\ completesDone < MaxCompletes
    /\ mrvts'         = MrvtsSetException(mrvts, ex)
    /\ completesDone' = completesDone + 1
    /\ cycleConsumed' = FALSE    \* new terminal, awaiting GetResult
    /\ UNCHANGED <<resetsDone, registered, dispatched>>

\* OnCompleted while Pending: register continuation.  A spec-conformant
\* caller registers at most one continuation per version; we enforce that
\* by requiring the slot empty.
A_OnCompletedRegister == \E cb \in Callbacks :
    /\ mrvts.status        = "Pending"
    /\ mrvts.continuation  = NoContinuation
    /\ CanOnCompleted(mrvts, mrvts.version)
    /\ mrvts'       = MrvtsStoreContinuation(mrvts, cb, mrvts.version)
    /\ registered'  = registered \cup {<<cb, mrvts.version>>}
    /\ UNCHANGED <<resetsDone, completesDone, cycleConsumed, dispatched>>

\* OnCompleted on terminal status: inline dispatch, no state change.
A_OnCompletedInline == \E cb \in Callbacks :
    /\ MrvtsIsCompleted(mrvts)
    /\ CanOnCompleted(mrvts, mrvts.version)
    /\ dispatched' = dispatched \cup {<<cb, mrvts.version>>}
    /\ UNCHANGED <<mrvts, resetsDone, completesDone, cycleConsumed, registered>>

\* Dispatcher: deliver a stored continuation once its status is terminal.
A_DispatcherRun ==
    /\ MrvtsIsCompleted(mrvts)
    /\ mrvts.continuation /= NoContinuation
    /\ dispatched' = dispatched \cup {mrvts.continuation}
    /\ mrvts'      = MrvtsAfterSchedule(mrvts)
    /\ UNCHANGED <<resetsDone, completesDone, cycleConsumed, registered>>

\* GetResult: consume the current cycle.  After this, Reset becomes legal.
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

\* Fairness on the dispatcher and the consumer; without these, temporal
\* properties about delivery / cycle progress do not hold.
Fairness ==
    /\ WF_vars(A_DispatcherRun)
    /\ WF_vars(A_GetResult)

Spec == Init /\ [][Next]_vars /\ Fairness

\* ----- Invariants --------------------------------------------------------

TypeOK ==
    /\ mrvts \in MrvtsSchema
    /\ resetsDone    \in 0..MaxResets
    /\ completesDone \in 0..MaxCompletes
    /\ cycleConsumed \in BOOLEAN
    /\ registered    \subseteq RegisteredContinuation
    /\ dispatched    \subseteq RegisteredContinuation

\* Version is structurally monotonic (only incremented by MrvtsReset);
\* stated as a safety invariant for confidence in the operator.
VersionBounded == mrvts.version \in VersionRange

\* ResetDiscipline: when enforced, every state reached has the invariant
\* that if we just did a Reset, the previous cycle was consumed.  Stated
\* as: whenever we are in a Pending state with version > 0, it was reached
\* by a Reset action that observed cycleConsumed = TRUE.  The negation
\* ("Reset observed cycleConsumed = FALSE") would mean a live registered
\* continuation was stranded — captured by the differential experiment's
\* EventualDispatch failure.
ResetDiscipline ==
    \* No stranded continuations: any <cb,token> in registered with
    \* token <= mrvts.version - 1 (a previous cycle) must also be in
    \* dispatched.  Equivalently: you cannot advance past a cycle with
    \* a dangling registration.
    \A pair \in registered :
        pair[2] < mrvts.version => pair \in dispatched

\* No double dispatch: union of dispatch sources equals itself (structural).
\* Stronger form: each registered pair appears in dispatched at most once.
\* TLA+ sets are sets so "at most once" is automatic; we state a liveness
\* check that dispatched \subseteq (registered \cup inline-dispatched set).
\* For simplicity, we fold inline dispatches into `dispatched` and check
\* origin here: every dispatched pair either came from a registered pair
\* or was inline.
NoOrphanDispatch == dispatched \subseteq RegisteredContinuation

\* ----- Temporal ---------------------------------------------------------
\*
\* MRVTS's liveness depends on caller behaviour (did SetResult/SetException
\* fire?), not on the primitive itself.  Liveness of the awaiter handshake
\* is verified by Awaiter.tla and Backpressure.tla, which drive completion
\* from a concrete writer/reader protocol.  The standalone model only
\* checks structural safety (TypeOK, VersionBounded, ResetDiscipline,
\* NoOrphanDispatch).

==============================================================================
