-------------------------------- MODULE MRVTS --------------------------------
(***************************************************************************)
(* Axiomatization of ManualResetValueTaskSourceCore<T> per the              *)
(* documented .NET semantics.  Covers spec §8.7.                            *)
(*                                                                          *)
(* This module is pure operators — no VARIABLES, no state-machine.  Other   *)
(* modules (Awaiter.tla, Backpressure.tla) EXTEND MRVTS to reuse the        *)
(* operators on their own MRVTS-record-valued variables.                    *)
(*                                                                          *)
(* The standalone state machine that verifies these operators' axiom        *)
(* consistency lives in MRVTSStandalone.tla.                                *)
(***************************************************************************)

EXTENDS Integers, Sequences, TLC

CONSTANTS
    Results,        \* finite set of possible result values
    Exceptions,     \* finite set of possible exception tags
    Callbacks,      \* finite set of callback identifiers
    MaxResets       \* upper bound on MRVTS version (for TLC finite state)

\* ----- Sentinels (distinct values outside user-supplied constant sets) ---

NoResult        == "NoResult"
NoException     == "NoException"
NoContinuation  == <<>>

ASSUME NoResult    \notin Results
ASSUME NoException \notin Exceptions

\* ----- Schema ------------------------------------------------------------

Statuses == {"Pending", "Succeeded", "Faulted"}

\* Versions are bounded by MaxResets + 1 (initial zero plus up to MaxResets
\* resets).  Callers that use these operators MUST supply their own
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

==============================================================================
