------------------------------- MODULE Publication -------------------------------
(***************************************************************************)
(* Model of SpscPipe §6.4.2 (SpliceUnpublishedChain), §7.1/§7.2 (reader    *)
(* acquire-loads and traversal invariant), and §10.7 (tail retirement).    *)
(*                                                                         *)
(* Scope (refers to docs/spscpipe-spec.md):                                *)
(*   §6.4.1  Unpublished chain construction — writer-local intra-chain    *)
(*           Next links (plain writes before publication).                *)
(*   §6.4.2  SpliceUnpublishedChain — the three release-stores that make  *)
(*           an unpublished chain reader-visible.  Ordering of the first  *)
(*           two stores (Head/Next before Tail) is load-bearing on weak   *)
(*           memory.  BWP is always last.                                  *)
(*   §7.1    First-read initialization — reader acquire-loads state.Head  *)
(*           once and walks from there.                                    *)
(*   §7.2    Traversal invariant — reader never traverses past the        *)
(*           acquired state.Tail; intermediate segments via acquire-load  *)
(*           of seg.Next.                                                  *)
(*   §10.7   Tail retirement — a segment may be retired when seg !=       *)
(*           state.Tail (observed) OR seg.Next is observed non-null.      *)
(*                                                                         *)
(* Memory model: same TSO-like store-buffer as AwaiterHandshake.           *)
(* Only cross-thread pointer writes (state.Head, state.Tail, prevTail.Next,*)
(* state.BytesWrittenPublished) go through the buffer.  Segment field     *)
(* initialization and intra-chain Next links are modeled as direct-to-mem *)
(* writes; they are writer-local until a splice makes them reachable, and *)
(* FIFO drain of bufW preserves the release-store ordering needed for the *)
(* reader to see initialized fields whenever it sees a pointer to a seg.  *)
(*                                                                         *)
(* Key properties:                                                        *)
(*   ChainConsistent     — if memTail is non-null, it is reachable from   *)
(*                          memHead via committed seg.Next.               *)
(*   NoRetiredAsPrevTail — the writer never uses a retired segment as    *)
(*                          prevTail when entering a subsequent splice.   *)
(*                          This is the §10.7 safety invariant.           *)
(*   BWPNotAheadOfTail   — state.BytesWrittenPublished never indicates    *)
(*                          bytes past what the reachable chain accounts  *)
(*                          for.                                           *)
(*                                                                         *)
(* Splice-ordering experiment: ENABLE_ORDERED_SPLICE = FALSE flips the    *)
(* splice's first two release-stores so state.Tail is published before    *)
(* state.Head / prevTail.Next.  TLC should then produce a ChainConsistent *)
(* counterexample, confirming §6.4.2's "CRITICAL ORDERING" argument.      *)
(***************************************************************************)

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    MaxSegments,            \* max total segments allocated
    MaxChainLen,            \* max segments per unpublished chain
    ENABLE_ORDERED_SPLICE   \* TRUE = correct; FALSE = experiment

ASSUME /\ MaxSegments \in Nat \ {0}
       /\ MaxChainLen \in Nat \ {0}
       /\ MaxChainLen <= MaxSegments
       /\ ENABLE_ORDERED_SPLICE \in BOOLEAN

NULL == 0
SegIds == 1..MaxSegments

(* -------- PC labels -------- *)
W_Ready  == "W.Ready"
W_Part2  == "W.SplicePart2"
W_Bwp    == "W.SpliceBwp"
W_Halt   == "W.Halt"

WriterPCs == {W_Ready, W_Part2, W_Bwp, W_Halt}

R_Ready  == "R.Ready"
R_Halt   == "R.Halt"

ReaderPCs == {R_Ready, R_Halt}

(* -------- Store-buffer entries -------- *)
\* kind:   "head" | "tail" | "segNext" | "bwp"
\* tgt:    segment id for "segNext" (the segment whose .Next is being set);
\*         unused otherwise (0).
\* val:    segment id for "head" / "tail" / "segNext"; byte count for "bwp".
StoreEntryShape ==
        [kind: {"head"},    tgt: {0},    val: 0..MaxSegments]
    \cup [kind: {"tail"},    tgt: {0},    val: 0..MaxSegments]
    \cup [kind: {"segNext"}, tgt: SegIds, val: 0..MaxSegments]
    \cup [kind: {"bwp"},     tgt: {0},    val: 0..MaxSegments]

(* -------- State variables -------- *)
VARIABLES
    \* Committed shared memory
    memHead,                 \* state.Head
    memTail,                 \* state.Tail
    memBWP,                  \* state.BytesWrittenPublished
    segNextCommitted,        \* committed seg.Next for each seg id

    \* Segment plain data (initialized at allocation, never changed)
    segWrittenLen,           \* 0 for unallocated; writtenLen \in 1..1 otherwise
    segRunningIdx,           \* cumulative byte start for the seg
    segAllocated,            \* BOOLEAN — writer has rented this seg
    segRetired,              \* BOOLEAN — reader has retired this seg

    \* Writer store buffer (FIFO)
    bufW,

    \* Writer local (unpublished chain pointers; byte count; published mirror)
    unpubHead,
    unpubTail,
    unpubBytes,
    writerBWP,
    writerTailView,          \* writer's buffer-forwarded view of state.Tail
    segsAllocated,           \* count, for bounding

    \* Reader local.  We deliberately do not model a cached "acquired tail";
    \* §7.3 performs a fresh Volatile.Read(ref state.Tail) at AdvanceTo entry,
    \* which is modeled by using memTail directly at R_RetireAndAdvance time
    \* (each retire action corresponds to one AdvanceTo call).
    readerHead,
    readerHeadInit,          \* TRUE after the one-time first-read head load
    readerExamined,

    \* Control
    wpc,
    rpc,
    splicesComplete

vars == << memHead, memTail, memBWP, segNextCommitted,
           segWrittenLen, segRunningIdx, segAllocated, segRetired,
           bufW,
           unpubHead, unpubTail, unpubBytes, writerBWP, writerTailView,
           segsAllocated,
           readerHead, readerHeadInit, readerExamined,
           wpc, rpc, splicesComplete >>

(* -------- Type invariant -------- *)
TypeOK ==
    /\ memHead          \in 0..MaxSegments
    /\ memTail          \in 0..MaxSegments
    /\ memBWP           \in 0..MaxSegments
    /\ segNextCommitted \in [SegIds -> 0..MaxSegments]
    /\ segWrittenLen    \in [SegIds -> 0..1]
    /\ segRunningIdx    \in [SegIds -> 0..MaxSegments]
    /\ segAllocated     \in [SegIds -> BOOLEAN]
    /\ segRetired       \in [SegIds -> BOOLEAN]
    /\ bufW             \in Seq(StoreEntryShape)
    /\ unpubHead        \in 0..MaxSegments
    /\ unpubTail        \in 0..MaxSegments
    /\ unpubBytes       \in 0..MaxSegments
    /\ writerBWP        \in 0..MaxSegments
    /\ writerTailView   \in 0..MaxSegments
    /\ segsAllocated    \in 0..MaxSegments
    /\ readerHead       \in 0..MaxSegments
    /\ readerHeadInit   \in BOOLEAN
    /\ readerExamined   \in 0..MaxSegments
    /\ wpc              \in WriterPCs
    /\ rpc              \in ReaderPCs
    /\ splicesComplete  \in 0..MaxSegments

(* -------- Initial state -------- *)
Init ==
    /\ memHead = NULL
    /\ memTail = NULL
    /\ memBWP  = 0
    /\ segNextCommitted = [s \in SegIds |-> NULL]
    /\ segWrittenLen    = [s \in SegIds |-> 0]
    /\ segRunningIdx    = [s \in SegIds |-> 0]
    /\ segAllocated     = [s \in SegIds |-> FALSE]
    /\ segRetired       = [s \in SegIds |-> FALSE]
    /\ bufW  = << >>
    /\ unpubHead = NULL
    /\ unpubTail = NULL
    /\ unpubBytes = 0
    /\ writerBWP = 0
    /\ writerTailView = NULL
    /\ segsAllocated = 0
    /\ readerHead = NULL
    /\ readerHeadInit = FALSE
    /\ readerExamined = 0
    /\ wpc = W_Ready
    /\ rpc = R_Ready
    /\ splicesComplete = 0

(* =================================================================== *)
(*  Helpers                                                             *)
(* =================================================================== *)

\* Unpublished chain length.  Used to bound the chain to MaxChainLen.
UnpubChainLen ==
    IF unpubHead = NULL THEN 0 ELSE unpubBytes
    \* Uses the fact that writtenLen is 1 per segment — unpubBytes = chain length.

\* Whether any segment in the chain has been allocated but not yet added.
\* (Used to decide if writer can allocate more.)
CanAllocateMore == segsAllocated < MaxSegments /\ UnpubChainLen < MaxChainLen

IsFirstSplice == writerTailView = NULL

\* First release-store of the splice — the one that is NOT state.Tail.
NonTailStoreEntry ==
    IF IsFirstSplice
        THEN [kind |-> "head",    tgt |-> 0,              val |-> unpubHead]
        ELSE [kind |-> "segNext", tgt |-> writerTailView, val |-> unpubHead]

\* Second release-store of the splice — state.Tail.
TailStoreEntry == [kind |-> "tail", tgt |-> 0, val |-> unpubTail]

\* Ordered vs. reversed dispatch.
SpliceFirstEntry  ==
    IF ENABLE_ORDERED_SPLICE THEN NonTailStoreEntry ELSE TailStoreEntry
SpliceSecondEntry ==
    IF ENABLE_ORDERED_SPLICE THEN TailStoreEntry ELSE NonTailStoreEntry

\* Chain reachability under committed seg.Next.  Budgeted to MaxSegments so
\* TLC can evaluate it without infinite recursion.
RECURSIVE ReachableRec(_, _, _)
ReachableRec(s, t, budget) ==
    \/ s = t
    \/ /\ budget > 0
       /\ s # NULL
       /\ segNextCommitted[s] # NULL
       /\ ReachableRec(segNextCommitted[s], t, budget - 1)

Reachable(s, t) == s # NULL /\ ReachableRec(s, t, MaxSegments)

(* =================================================================== *)
(*  Memory-model action                                                  *)
(* =================================================================== *)

\* Apply the head of bufW to committed memory.  Separate cases for each
\* store kind so TLC can type-check the target.
ApplyStore(e) ==
    CASE e.kind = "head"    -> /\ memHead' = e.val
                               /\ UNCHANGED << memTail, memBWP, segNextCommitted >>
      [] e.kind = "tail"    -> /\ memTail' = e.val
                               /\ UNCHANGED << memHead, memBWP, segNextCommitted >>
      [] e.kind = "segNext" -> /\ segNextCommitted' = [segNextCommitted EXCEPT ![e.tgt] = e.val]
                               /\ UNCHANGED << memHead, memTail, memBWP >>
      [] e.kind = "bwp"     -> /\ memBWP' = e.val
                               /\ UNCHANGED << memHead, memTail, segNextCommitted >>

DrainW ==
    /\ bufW # << >>
    /\ ApplyStore(Head(bufW))
    /\ bufW' = Tail(bufW)
    /\ UNCHANGED << segWrittenLen, segRunningIdx, segAllocated, segRetired,
                    unpubHead, unpubTail, unpubBytes, writerBWP, writerTailView,
                    segsAllocated, readerHead, readerHeadInit, readerExamined,
                    wpc, rpc, splicesComplete >>

(* =================================================================== *)
(*  Writer actions                                                       *)
(* =================================================================== *)

\* Rent a new segment and append it to the unpublished chain.
\* Segment fields are initialized as plain writes; the intra-chain
\* seg.Next link (updating the previous unpub tail) is also a plain write,
\* modeled as a direct update to segNextCommitted since the reader cannot
\* observe it until a splice makes the chain reader-reachable.
W_AllocAndAppend ==
    /\ wpc = W_Ready
    /\ CanAllocateMore
    /\ LET newSeg  == segsAllocated + 1
           newRIdx == writerBWP + unpubBytes IN
        /\ segAllocated'  = [segAllocated  EXCEPT ![newSeg] = TRUE]
        /\ segWrittenLen' = [segWrittenLen EXCEPT ![newSeg] = 1]
        /\ segRunningIdx' = [segRunningIdx EXCEPT ![newSeg] = newRIdx]
        \* Intra-chain link (plain write): only if the chain is non-empty.
        /\ segNextCommitted' =
            IF unpubTail = NULL
                THEN segNextCommitted
                ELSE [segNextCommitted EXCEPT ![unpubTail] = newSeg]
        /\ unpubHead' = IF unpubHead = NULL THEN newSeg ELSE unpubHead
        /\ unpubTail' = newSeg
        /\ unpubBytes' = unpubBytes + 1
        /\ segsAllocated' = newSeg
    /\ UNCHANGED << memHead, memTail, memBWP, segRetired, bufW,
                    writerBWP, writerTailView,
                    readerHead, readerHeadInit, readerExamined,
                    wpc, rpc, splicesComplete >>

\* Splice part 1: append the first of the two pointer release-stores.
\* Under ordered (correct) semantics this is the NonTail store
\* (state.Head on first splice, or prevTail.Next on subsequent).
\* Under reversed semantics it is state.Tail.
W_SplicePart1 ==
    /\ wpc = W_Ready
    /\ unpubHead # NULL
    /\ bufW' = Append(bufW, SpliceFirstEntry)
    /\ wpc' = W_Part2
    /\ UNCHANGED << memHead, memTail, memBWP, segNextCommitted,
                    segWrittenLen, segRunningIdx, segAllocated, segRetired,
                    unpubHead, unpubTail, unpubBytes, writerBWP, writerTailView,
                    segsAllocated,
                    readerHead, readerHeadInit, readerExamined,
                    rpc, splicesComplete >>

\* Splice part 2: append the second of the two pointer release-stores.
\* Synchronously updates writerTailView (buffer-forwarded plain read of
\* state.Tail returns the new value to the writer immediately, regardless
\* of drain status).
W_SplicePart2 ==
    /\ wpc = W_Part2
    /\ bufW' = Append(bufW, SpliceSecondEntry)
    /\ writerTailView' = unpubTail
    /\ wpc' = W_Bwp
    /\ UNCHANGED << memHead, memTail, memBWP, segNextCommitted,
                    segWrittenLen, segRunningIdx, segAllocated, segRetired,
                    unpubHead, unpubTail, unpubBytes, writerBWP,
                    segsAllocated,
                    readerHead, readerHeadInit, readerExamined,
                    rpc, splicesComplete >>

\* Splice part 3: release-store BytesWrittenPublished.  Always last (§6.4.2
\* ordering constraint: BWP must follow the pointer stores).  Clears the
\* unpublished chain — its segments are now reader-reachable.
W_SpliceBwp ==
    /\ wpc = W_Bwp
    /\ LET newBWP == writerBWP + unpubBytes IN
        /\ bufW' = Append(bufW, [kind |-> "bwp", tgt |-> 0, val |-> newBWP])
        /\ writerBWP' = newBWP
    /\ unpubHead' = NULL
    /\ unpubTail' = NULL
    /\ unpubBytes' = 0
    /\ wpc' = W_Ready
    /\ splicesComplete' = splicesComplete + 1
    /\ UNCHANGED << memHead, memTail, memBWP, segNextCommitted,
                    segWrittenLen, segRunningIdx, segAllocated, segRetired,
                    writerTailView, segsAllocated,
                    readerHead, readerHeadInit, readerExamined, rpc >>

\* Writer halts when there's nothing left to do: segment budget exhausted,
\* no pending chain, no in-flight splice, and the writer's published state
\* has been fully drained.
W_Stop ==
    /\ wpc = W_Ready
    /\ segsAllocated = MaxSegments
    /\ unpubHead = NULL
    /\ bufW = << >>
    /\ wpc' = W_Halt
    /\ UNCHANGED << memHead, memTail, memBWP, segNextCommitted,
                    segWrittenLen, segRunningIdx, segAllocated, segRetired,
                    bufW, unpubHead, unpubTail, unpubBytes, writerBWP,
                    writerTailView, segsAllocated,
                    readerHead, readerHeadInit, readerExamined,
                    rpc, splicesComplete >>

WriterAction ==
    \/ W_AllocAndAppend
    \/ W_SplicePart1
    \/ W_SplicePart2
    \/ W_SpliceBwp
    \/ W_Stop

(* =================================================================== *)
(*  Reader actions                                                       *)
(* =================================================================== *)

\* §7.1 step 4: first-read initialization.  Acquire-load state.Head exactly
\* once, when state.Tail has become non-null.  readerHeadInit guards
\* against re-firing if the action ever happened to produce readerHead = 0
\* (which would be a ChainConsistent violation and caught separately).
R_FirstAcquireHead ==
    /\ rpc = R_Ready
    /\ ~readerHeadInit
    /\ memTail # NULL
    /\ readerHead' = memHead
    /\ readerHeadInit' = TRUE
    /\ UNCHANGED << memHead, memTail, memBWP, segNextCommitted,
                    segWrittenLen, segRunningIdx, segAllocated, segRetired,
                    bufW, unpubHead, unpubTail, unpubBytes, writerBWP,
                    writerTailView, segsAllocated, readerExamined,
                    wpc, rpc, splicesComplete >>

\* Consume one segment's worth of bytes at readerHead.  Abstracts advancing
\* examined to the end of the current head segment.
R_Consume ==
    /\ rpc = R_Ready
    /\ readerHead # NULL
    /\ segAllocated[readerHead]
    /\ readerExamined < segRunningIdx[readerHead] + segWrittenLen[readerHead]
    /\ readerExamined' = segRunningIdx[readerHead] + segWrittenLen[readerHead]
    /\ UNCHANGED << memHead, memTail, memBWP, segNextCommitted,
                    segWrittenLen, segRunningIdx, segAllocated, segRetired,
                    bufW, unpubHead, unpubTail, unpubBytes, writerBWP,
                    writerTailView, segsAllocated,
                    readerHead, readerHeadInit,
                    wpc, rpc, splicesComplete >>

\* §10.7 tail-retirement rule: retire readerHead (and advance) when the
\* segment is fully consumed AND at least one of the two safety conditions
\* holds — it is not the current tail (via a fresh acquire-load of memTail,
\* matching §7.3's Volatile.Read at AdvanceTo entry), OR its Next has been
\* published.  The first disjunct is redundant under a consistent chain
\* (readerHead != memTail implies segNext != NULL), but we keep it explicit
\* to match the spec.  The extra guard segNext != NULL ensures we never
\* advance readerHead into NULL, so R_FirstAcquireHead fires at most once.
R_RetireAndAdvance ==
    /\ rpc = R_Ready
    /\ readerHead # NULL
    /\ readerExamined >= segRunningIdx[readerHead] + segWrittenLen[readerHead]
    /\ segNextCommitted[readerHead] # NULL
    /\ \/ readerHead # memTail
       \/ segNextCommitted[readerHead] # NULL
    /\ segRetired' = [segRetired EXCEPT ![readerHead] = TRUE]
    /\ readerHead' = segNextCommitted[readerHead]
    /\ UNCHANGED << memHead, memTail, memBWP, segNextCommitted,
                    segWrittenLen, segRunningIdx, segAllocated,
                    bufW, unpubHead, unpubTail, unpubBytes, writerBWP,
                    writerTailView, segsAllocated,
                    readerHeadInit, readerExamined,
                    wpc, rpc, splicesComplete >>

\* Reader halts.  Permitted whenever the writer has halted and the reader
\* has caught up (readerHead = NULL or reader has consumed everything it
\* can reach).  We use a lenient guard to avoid spurious deadlocks.
R_Stop ==
    /\ rpc = R_Ready
    /\ wpc = W_Halt
    /\ bufW = << >>
    /\ rpc' = R_Halt
    /\ UNCHANGED << memHead, memTail, memBWP, segNextCommitted,
                    segWrittenLen, segRunningIdx, segAllocated, segRetired,
                    bufW, unpubHead, unpubTail, unpubBytes, writerBWP,
                    writerTailView, segsAllocated,
                    readerHead, readerHeadInit, readerExamined,
                    wpc, splicesComplete >>

ReaderAction ==
    \/ R_FirstAcquireHead
    \/ R_Consume
    \/ R_RetireAndAdvance
    \/ R_Stop

(* =================================================================== *)
(*  Next-state and spec                                                  *)
(* =================================================================== *)

Next == WriterAction \/ ReaderAction \/ DrainW

Fairness ==
    /\ WF_vars(DrainW)
    /\ WF_vars(W_AllocAndAppend)
    /\ WF_vars(W_SplicePart1)
    /\ WF_vars(W_SplicePart2)
    /\ WF_vars(W_SpliceBwp)
    /\ WF_vars(W_Stop)
    /\ WF_vars(R_FirstAcquireHead)
    /\ WF_vars(R_Consume)
    /\ WF_vars(R_RetireAndAdvance)
    /\ WF_vars(R_Stop)

Spec == Init /\ [][Next]_vars /\ Fairness

(* =================================================================== *)
(*  Safety invariants                                                    *)
(* =================================================================== *)

\* §6.4.2 chain consistency: whenever memTail is non-null, memHead is
\* non-null and memTail is reachable from memHead via committed seg.Next
\* pointers.  The ordering of the splice's release-stores is what
\* maintains this invariant on weak memory.
ChainConsistent ==
    \/ memTail = NULL
    \/ (memHead # NULL /\ Reachable(memHead, memTail))

\* §10.7 tail-retirement safety: whenever the writer is at a splice entry
\* point (wpc = W_Ready), its view of state.Tail — the value that will be
\* used as prevTail if the writer starts a splice next — is not a retired
\* segment.  A plain "writerTailView is never retired" is too strict: in
\* ordered mode, between Part1 and Part2 of a splice the view briefly lags
\* the Part1 store (prevTail.Next is now published, so the reader can
\* validly retire that segment by the §10.7 second disjunct), then Part2
\* updates the view to the new tail before the writer returns to W_Ready.
\* This transient is safe in the real implementation because the writer
\* completes its splice before reading state.Tail again.
NoRetiredAsPrevTail ==
    (wpc = W_Ready /\ writerTailView # NULL) => ~segRetired[writerTailView]

\* Committed BWP never exceeds the end-position of the committed tail.
\* Because bufW is FIFO and BWP is always appended after the tail store,
\* memBWP can lag memTail but never lead it.
BWPNotAheadOfTail ==
    memBWP <= IF memTail = NULL
                THEN 0
                ELSE segRunningIdx[memTail] + segWrittenLen[memTail]

\* The reader never observes a segment via its local head that has been
\* retired.  Retirement advances readerHead to the successor in the same
\* atomic action, so readerHead is never in the retired set.
ReaderHeadNotRetired ==
    readerHead # NULL => ~segRetired[readerHead]

\* Examined position is bounded by writerBWP plus any unpublished bytes
\* the writer has accumulated; i.e., the reader never reads past what
\* the writer has committed plus what's currently in flight.
ExaminedNotAhead ==
    readerExamined <= writerBWP + unpubBytes

(* =================================================================== *)
(*  Temporal properties                                                 *)
(* =================================================================== *)

\* Both sides eventually halt.
EventualWriterHalt == <>(wpc = W_Halt)
EventualReaderHalt == <>(rpc = R_Halt)

=============================================================================
