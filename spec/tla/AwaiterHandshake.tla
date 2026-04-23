---------------------------- MODULE AwaiterHandshake ----------------------------
(***************************************************************************)
(* Model of SpscPipe §8 awaiter handshake, reader-awaiter side.            *)
(*                                                                         *)
(* Scope (refers to docs/spscpipe-spec.md):                                *)
(*   §8.2  Writer signaling reader (MaybeSignalReaderAwaiter)              *)
(*   §8.3  Reader arming read awaiter (double-check with StoreLoad fence)  *)
(*   §8.6  Cancellation as a third concurrent signaler                     *)
(*   §6.6  Writer Complete (final publish of writerDone + signal)          *)
(*                                                                         *)
(* The writer-awaiter side (§8.4) is structurally symmetric and covered    *)
(* by inspection.                                                          *)
(*                                                                         *)
(* Memory model (TSO-like; sufficient for AArch64 + release/acquire since  *)
(* this protocol does not rely on non-multi-copy-atomic behavior):         *)
(*                                                                         *)
(*   * Release-stores (Volatile.Write) enter the issuing thread's store    *)
(*     buffer (bufW).                                                      *)
(*   * Acquire-loads (Volatile.Read) read from global committed memory.    *)
(*     StoreLoad reordering is modeled by the fact that a subsequent       *)
(*     acquire-load of a different variable may fire before the preceding  *)
(*     release-store has been drained.                                     *)
(*   * Buffered stores propagate nondeterministically via DrainW.          *)
(*   * A full fence (Interlocked.*, MemoryBarrier) is modeled by guarding  *)
(*     the fenced action with bufW = <<>>.  In concert with weak fairness  *)
(*     on DrainW this forces the buffer to drain before the fenced action  *)
(*     executes.                                                           *)
(*   * CAS operates on global committed memory atomically and is a full    *)
(*     fence (guarded on bufW = <<>> for the writer; reader/cancel have    *)
(*     no store buffer so no guard needed).                                *)
(*                                                                         *)
(* Observation: in this protocol only the writer issues release-stores.    *)
(* The reader's only writes to shared state are CAS (atomic/fenced).  The  *)
(* cancel actor likewise only CASes.  Hence bufR and bufC would be         *)
(* structurally empty and are omitted.                                     *)
(*                                                                         *)
(* Fence-removal experiment:                                               *)
(*   ENABLE_WRITER_FENCE = FALSE removes the fence that §8.2 places before *)
(*   the writer's acquire-load of ReaderAwaiterState.  TLC should then     *)
(*   produce a counterexample to NoLostWakeup — the canonical double-check *)
(*   race.                                                                 *)
(***************************************************************************)

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    MaxProduces,            \* bound on publication events
    MaxArms,                \* bound on reader arm cycles
    MaxCancels,             \* bound on cancellation events
    ENABLE_WRITER_FENCE     \* TRUE for correct model; FALSE for fence-removal

ASSUME /\ MaxProduces \in Nat \ {0}
       /\ MaxArms     \in Nat \ {0}
       /\ MaxCancels  \in Nat
       /\ ENABLE_WRITER_FENCE \in BOOLEAN

(* -------- Awaiter states (§8.1) -------- *)
Idle     == "Idle"
Armed    == "Armed"
Signaled == "Signaled"

AwaiterStates == {Idle, Armed, Signaled}

(* -------- Writer PC labels -------- *)
W_Ready  == "W.Ready"       \* can produce, or call Complete, or halt
W_Signal == "W.Signal"      \* just published; about to fence + signal
W_Halt   == "W.Halt"        \* completed

WriterPCs == {W_Ready, W_Signal, W_Halt}

(* -------- Reader PC labels -------- *)
R_Ready      == "R.Ready"       \* about to call ReadAsync
R_Arm        == "R.Arm"         \* no data in initial check; about to arm
R_Recheck    == "R.Recheck"     \* armed; about to re-check (§8.3 Step C)
R_Park       == "R.Park"        \* parked on awaiter, waiting for SetResult
R_Halt       == "R.Halt"        \* out of arm budget

ReaderPCs == {R_Ready, R_Arm, R_Recheck, R_Park, R_Halt}

(* -------- Cancel PC labels -------- *)
C_Ready == "C.Ready"
C_Halt  == "C.Halt"

CancelPCs == {C_Ready, C_Halt}

(* -------- State -------- *)
VARIABLES
    mem,                 \* [readerAwaiter, tailPub, writerDone]
    bufW,                \* writer's store buffer: Seq of [var, val]
    wpc, rpc, cpc,       \* program counters
    armVersion,          \* version of the currently-active arm cycle
    parkedVersion,       \* version reader is parked on, or -1
    setResults,          \* Seq of [version, reason] ; reason \in {"signal", "cancel"}
    producedCount,
    armsCompleted,       \* # of arm cycles that have resolved
    readerExamined,      \* largest tail value the reader has successfully consumed
    cancelsFired

vars == << mem, bufW, wpc, rpc, cpc, armVersion, parkedVersion, setResults,
           producedCount, armsCompleted, readerExamined, cancelsFired >>

(* -------- Type invariant -------- *)
\* Two disjoint record shapes: tailPub carries a Nat, writerDone carries a Bool.
\* Encoded as a union of records (rather than a single record with a
\* heterogeneous `val` field) because TLC evaluates set membership on the
\* val-position eagerly and refuses to compare a Bool against 0..MaxProduces.
StoreEntry ==
        [var: {"tailPub"},    val: 0..MaxProduces]
    \cup [var: {"writerDone"}, val: BOOLEAN]

TypeOK ==
    /\ mem \in [readerAwaiter: AwaiterStates,
                tailPub:       0..MaxProduces,
                writerDone:    BOOLEAN]
    /\ bufW \in Seq(StoreEntry)
    /\ wpc \in WriterPCs
    /\ rpc \in ReaderPCs
    /\ cpc \in CancelPCs
    /\ armVersion     \in 0..MaxArms
    /\ parkedVersion  \in (-1)..MaxArms
    /\ setResults     \in Seq([version: 0..MaxArms, reason: {"signal", "cancel"}])
    /\ producedCount  \in 0..MaxProduces
    /\ armsCompleted  \in 0..MaxArms
    /\ readerExamined \in 0..MaxProduces
    /\ cancelsFired   \in 0..MaxCancels

(* -------- Initial state -------- *)
Init ==
    /\ mem = [readerAwaiter |-> Idle, tailPub |-> 0, writerDone |-> FALSE]
    /\ bufW = << >>
    /\ wpc = W_Ready
    /\ rpc = R_Ready
    /\ cpc = C_Ready
    /\ armVersion     = 0
    /\ parkedVersion  = -1
    /\ setResults     = << >>
    /\ producedCount  = 0
    /\ armsCompleted  = 0
    /\ readerExamined = 0
    /\ cancelsFired   = 0

(* =================================================================== *)
(*  Helpers                                                             *)
(* =================================================================== *)

\* Fence predicate: TRUE if the writer's buffer is empty (fence condition met).
\* When ENABLE_WRITER_FENCE is FALSE, the predicate is identically TRUE — the
\* fence is elided.
WriterFenceOK ==
    IF ENABLE_WRITER_FENCE THEN bufW = << >> ELSE TRUE

\* Data is "available" to the reader iff the published tail advances beyond
\* what it has examined.  Abstracts the §7.1 step 6 check.
DataAvailable(tailObs) == tailObs > readerExamined

(* =================================================================== *)
(*  Memory-model action: drain one entry from writer's store buffer     *)
(* =================================================================== *)

DrainW ==
    /\ bufW # << >>
    /\ LET entry == Head(bufW) IN
        mem' = [mem EXCEPT ![entry.var] = entry.val]
    /\ bufW' = Tail(bufW)
    /\ UNCHANGED << wpc, rpc, cpc, armVersion, parkedVersion, setResults,
                    producedCount, armsCompleted, readerExamined, cancelsFired >>

(* =================================================================== *)
(*  Writer actions                                                      *)
(* =================================================================== *)

\* Publish one chunk: release-store tailPub (§6.4.2 splice — abstracted
\* to the single release-store that advances BytesWrittenPublished).
\* Moves wpc to W_Signal so the writer must attempt to signal before
\* publishing again.  This mirrors §8.2 being called from §6.4.2 step 5.
W_DoPublish ==
    /\ wpc = W_Ready
    /\ producedCount < MaxProduces
    /\ producedCount' = producedCount + 1
    /\ bufW' = Append(bufW, [var |-> "tailPub", val |-> producedCount + 1])
    /\ wpc' = W_Signal
    /\ UNCHANGED << mem, rpc, cpc, armVersion, parkedVersion, setResults,
                    armsCompleted, readerExamined, cancelsFired >>

\* MaybeSignalReaderAwaiter (§8.2):
\*   [fence (guarded on bufW empty when enabled)]
\*   acquire-load readerAwaiter
\*   if Armed: CAS Armed -> Signaled, SetResult(signal)
\* Modeled as one atomic action because the fence + load + CAS forms a
\* single logical decision — interleaving between them would not change
\* outcomes (CAS itself is atomic and no other writer action intervenes).
\* The fence is the key ordering constraint: with ENABLE_WRITER_FENCE=TRUE
\* the writer cannot proceed while bufW is non-empty, forcing DrainW to
\* run first (weak fairness on DrainW guarantees eventual drain).
W_MaybeSignal ==
    /\ wpc = W_Signal
    /\ WriterFenceOK
    /\ LET aw == mem.readerAwaiter IN
        IF aw = Armed
            THEN /\ mem' = [mem EXCEPT !.readerAwaiter = Signaled]
                 /\ setResults' = Append(setResults,
                                         [version |-> parkedVersion,
                                          reason  |-> "signal"])
            ELSE /\ UNCHANGED mem
                 /\ UNCHANGED setResults
    /\ wpc' = W_Ready
    /\ UNCHANGED << bufW, rpc, cpc, armVersion, parkedVersion,
                    producedCount, armsCompleted, readerExamined, cancelsFired >>

\* Writer Complete (§6.6): publish writerDone, then signal.  Modeled in two
\* steps so the release-store of writerDone can be interleaved with reader
\* activity, matching the release of WriterCompletionState=2 in §6.6 step 4.
W_DoComplete ==
    /\ wpc = W_Ready
    /\ ~ mem.writerDone
    /\ \A i \in 1..Len(bufW) : bufW[i].var # "writerDone"   \* not already pending
    /\ bufW' = Append(bufW, [var |-> "writerDone", val |-> TRUE])
    /\ wpc' = W_Signal
    /\ UNCHANGED << mem, rpc, cpc, armVersion, parkedVersion, setResults,
                    producedCount, armsCompleted, readerExamined, cancelsFired >>

\* Writer halts only after Complete has been published AND drained to global
\* memory.  This mirrors the SpscPipe contract: the writer must call
\* Complete() before abandoning the pipe so the reader can observe
\* writerDone and make progress.  Requiring mem.writerDone (not merely
\* bufW containing the writerDone store) also ensures any signaling
\* attempt in W_MaybeSignal has already run (it fenced on bufW = <<>>).
W_Stop ==
    /\ wpc = W_Ready
    /\ mem.writerDone
    /\ wpc' = W_Halt
    /\ UNCHANGED << mem, bufW, rpc, cpc, armVersion, parkedVersion, setResults,
                    producedCount, armsCompleted, readerExamined, cancelsFired >>

WriterAction == W_DoPublish \/ W_MaybeSignal \/ W_DoComplete \/ W_Stop

(* =================================================================== *)
(*  Reader actions                                                      *)
(* =================================================================== *)

\* Start a read: acquire-load tailPub and writerDone (§7.1 step 3).
\* If data is available or writer has completed, resolve synchronously
\* (Go directly back to R_Ready, bumping armsCompleted, updating examined).
\* Otherwise, prepare to arm.
R_StartRead ==
    /\ rpc = R_Ready
    /\ armsCompleted < MaxArms
    /\ LET tailObs == mem.tailPub
           doneObs == mem.writerDone IN
        IF DataAvailable(tailObs) \/ doneObs
            THEN /\ armsCompleted' = armsCompleted + 1
                 /\ readerExamined' = IF DataAvailable(tailObs) THEN tailObs
                                       ELSE readerExamined
                 /\ rpc' = R_Ready
                 /\ UNCHANGED << armVersion, parkedVersion >>
            ELSE /\ rpc' = R_Arm
                 /\ UNCHANGED << armsCompleted, readerExamined,
                                 armVersion, parkedVersion >>
    /\ UNCHANGED << mem, bufW, wpc, cpc, setResults,
                    producedCount, cancelsFired >>

\* Arm CAS (§8.3 Step A): CAS readerAwaiter Idle -> Armed.  Atomic + full
\* fence (covers Step B's MemoryBarrier by the §8.3 footnote).
\* Under the SPSC invariant the CAS always succeeds — preceding signals
\* have already been cleaned up by R_Resume (readerAwaiter reset to Idle).
\* If that assumption is wrong in the model, TLC will flag AtMostOneResult
\* or TypeOK via Signaled being observed here.
R_ArmCAS ==
    /\ rpc = R_Arm
    /\ mem.readerAwaiter = Idle
    /\ mem' = [mem EXCEPT !.readerAwaiter = Armed]
    /\ parkedVersion' = armVersion
    /\ rpc' = R_Recheck
    /\ UNCHANGED << bufW, wpc, cpc, armVersion, setResults,
                    producedCount, armsCompleted, readerExamined, cancelsFired >>

\* Synchronous-resolution body shared by the two R_DoRecheck success branches.
ResolveSync(tailObs, doneObs) ==
    /\ armsCompleted' = armsCompleted + 1
    /\ readerExamined' = IF DataAvailable(tailObs) THEN tailObs
                          ELSE readerExamined
    /\ parkedVersion' = -1
    /\ armVersion' = armVersion + 1
    /\ rpc' = R_Ready

\* Re-check (§8.3 Step C): acquire-load tailPub and writerDone.  If either
\* indicates work is available, attempt un-arm CAS Armed -> Idle.  If that
\* CAS wins, resolve synchronously.  If it loses (state is Signaled — i.e.
\* writer or cancel already delivered), the VTS already has a result;
\* still resolve synchronously (reset awaiter to Idle for next cycle).
\* If no work is available, commit to parking.
R_DoRecheck ==
    /\ rpc = R_Recheck
    /\ LET tailObs == mem.tailPub
           doneObs == mem.writerDone IN
        IF DataAvailable(tailObs) \/ doneObs
            THEN \/ \* Un-arm CAS wins
                    /\ mem.readerAwaiter = Armed
                    /\ mem' = [mem EXCEPT !.readerAwaiter = Idle]
                    /\ ResolveSync(tailObs, doneObs)
               \/ \* Un-arm CAS loses: state is Signaled
                    /\ mem.readerAwaiter = Signaled
                    /\ mem' = [mem EXCEPT !.readerAwaiter = Idle]
                    /\ ResolveSync(tailObs, doneObs)
            ELSE /\ rpc' = R_Park
                 /\ UNCHANGED << mem, armVersion, parkedVersion,
                                 armsCompleted, readerExamined >>
    /\ UNCHANGED << bufW, wpc, cpc, setResults,
                    producedCount, cancelsFired >>

\* Parked reader is resumed when SetResult has been delivered for its version.
\* We detect delivery by the presence of an entry with version = parkedVersion
\* in setResults.  On resume, consume the entry (conceptually), reset the
\* awaiter state to Idle, and advance to the next arm cycle.
R_Resume ==
    /\ rpc = R_Park
    /\ parkedVersion >= 0
    /\ \E i \in 1..Len(setResults) : setResults[i].version = parkedVersion
    /\ mem' = [mem EXCEPT !.readerAwaiter = Idle]
    /\ armsCompleted' = armsCompleted + 1
    /\ readerExamined' = IF mem.tailPub > readerExamined THEN mem.tailPub
                          ELSE readerExamined
    /\ parkedVersion' = -1
    /\ armVersion' = armVersion + 1
    /\ rpc' = R_Ready
    /\ UNCHANGED << bufW, wpc, cpc, setResults, producedCount, cancelsFired >>

\* Reader halts when either its arm budget is exhausted or the writer has
\* completed and the reader has caught up.  The latter matches real usage:
\* once the reader observes IsCompleted=TRUE with an empty buffer it stops,
\* rather than loop forever on synchronous-complete ReadAsync calls.
R_Stop ==
    /\ rpc = R_Ready
    /\ \/ armsCompleted = MaxArms
       \/ mem.writerDone
    /\ rpc' = R_Halt
    /\ UNCHANGED << mem, bufW, wpc, cpc, armVersion, parkedVersion, setResults,
                    producedCount, armsCompleted, readerExamined, cancelsFired >>

ReaderAction == R_StartRead \/ R_ArmCAS \/ R_DoRecheck \/ R_Resume \/ R_Stop

(* =================================================================== *)
(*  Cancel action (§8.6)                                                *)
(* =================================================================== *)

\* Cancel fires nondeterministically.  Like the writer's signal, it CASes
\* Armed -> Signaled with SetResult(cancel).  If the awaiter is not Armed,
\* the CAS fails and cancel is a no-op (matches §8.6 "CAS resolves the race").
C_Fire ==
    /\ cpc = C_Ready
    /\ cancelsFired < MaxCancels
    /\ IF mem.readerAwaiter = Armed
            THEN /\ mem' = [mem EXCEPT !.readerAwaiter = Signaled]
                 /\ setResults' = Append(setResults,
                                         [version |-> parkedVersion,
                                          reason  |-> "cancel"])
            ELSE /\ UNCHANGED mem
                 /\ UNCHANGED setResults
    /\ cancelsFired' = cancelsFired + 1
    /\ UNCHANGED << bufW, wpc, rpc, cpc, armVersion, parkedVersion,
                    producedCount, armsCompleted, readerExamined >>

C_Stop ==
    /\ cpc = C_Ready
    /\ cancelsFired = MaxCancels
    /\ cpc' = C_Halt
    /\ UNCHANGED << mem, bufW, wpc, rpc, armVersion, parkedVersion, setResults,
                    producedCount, armsCompleted, readerExamined, cancelsFired >>

CancelAction == C_Fire \/ C_Stop

(* =================================================================== *)
(*  Next-state and spec                                                 *)
(* =================================================================== *)

Next == WriterAction \/ ReaderAction \/ CancelAction \/ DrainW

\* Weak fairness on every action.  DrainW needs WF so the writer's fence
\* can make progress (a pending store eventually drains).  Other actions
\* need WF so bounded counters eventually saturate and we don't stutter.
Fairness ==
    /\ WF_vars(DrainW)
    /\ WF_vars(W_DoPublish)
    /\ WF_vars(W_MaybeSignal)
    /\ WF_vars(W_DoComplete)
    /\ WF_vars(W_Stop)
    /\ WF_vars(R_StartRead)
    /\ WF_vars(R_ArmCAS)
    /\ WF_vars(R_DoRecheck)
    /\ WF_vars(R_Resume)
    /\ WF_vars(R_Stop)
    /\ WF_vars(C_Fire)
    /\ WF_vars(C_Stop)

Spec == Init /\ [][Next]_vars /\ Fairness

(* =================================================================== *)
(*  Safety invariants                                                   *)
(* =================================================================== *)

\* Each arm-cycle version receives at most one SetResult.
AtMostOneResult ==
    \A v \in 0..MaxArms :
        Cardinality({i \in 1..Len(setResults) : setResults[i].version = v}) <= 1

\* The awaiter, when not Idle, has a parked version to match it against.
AwaiterHasVersion ==
    (mem.readerAwaiter \in {Armed, Signaled}) => (parkedVersion >= 0)

\* A reader cannot be parked unless the awaiter is Armed or Signaled.
\* (The Signaled case means the signal arrived between park and the scheduler
\* delivering the continuation.)
ParkedImpliesNonIdle ==
    (rpc = R_Park) => (mem.readerAwaiter \in {Armed, Signaled})

(* =================================================================== *)
(*  Temporal properties                                                 *)
(* =================================================================== *)

\* Every armed awaiter is eventually non-Armed (either un-armed by the
\* reader's Step C CAS, or transitioned to Signaled by writer/cancel).
\* Captures lost-wakeup freedom: an Armed state that lingers forever is
\* precisely the deadlock.
EventuallyUnArmed ==
    [](mem.readerAwaiter = Armed => <>(mem.readerAwaiter # Armed))

\* Every parked reader eventually resumes (completes its arm cycle).
\* Complements EventuallyUnArmed: after Signaled, the reader's continuation
\* must run.
EventuallyResumed ==
    [](rpc = R_Park => <>(rpc # R_Park))

\* Reader eventually uses all its arm budget, given that the writer keeps
\* producing or completes and the cancel actor runs.  This is the strongest
\* progress property: the handshake never deadlocks the protocol as a whole.
EventualProgress ==
    <>(rpc = R_Halt)

=============================================================================
