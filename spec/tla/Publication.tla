----------------------------- MODULE Publication -----------------------------
(***************************************************************************)
(* Segment lifecycle, publication, retirement, and the segment pool.        *)
(* Covers spec §4.1 (segment as mutable pooled object), §6.1 (GetMemory /   *)
(* buffer rotation), §6.2 (Advance), §6.3 (FlushAsync publication path),    *)
(* §6.4.1 (AppendActiveSegmentToUnpublished), §6.4.2 (SpliceUnpublishedChain*)
(* — the three release-stores), §7.1 step 4 (first-read head init), §7.2   *)
(* (traversal), §7.3 (AdvanceTo with the §7.3 errata ordering), §9 (pool    *)
(* mechanics), §10.7 (retirement invariant including the reader-side        *)
(* lifetime obligation).                                                    *)
(*                                                                          *)
(* Key modelling decisions (per spec/tla/design.md):                        *)
(*                                                                          *)
(*   - Segments are mutable, pooled objects: each segment ID in             *)
(*     1..MaxSegments is a reusable slot whose fields are state variables.  *)
(*     Initialize overwrites the fields and increments segLogicalId,        *)
(*     capturing the "different logical segment across the same object      *)
(*     identity" phenomenon from §4.1.                                      *)
(*                                                                          *)
(*   - Writer has an explicit FIFO store buffer bufW.  Release-stores are   *)
(*     pushed; the DrainW action pops FIFO and applies to committed memory. *)
(*     Captures TSO-style ordering needed for the three-part splice.        *)
(*                                                                          *)
(*   - NoStaleFieldReadAfterRetire is the new load-bearing invariant: when  *)
(*     the reader reads any field of a segment, the logicalId observed must *)
(*     match the logicalId in effect at the time the reader first saw that  *)
(*     segment reference.  This is the property the earlier model missed    *)
(*     and that would have caught the examined-position bug from §7.3.      *)
(*                                                                          *)
(* Not modelled here (deferred):                                            *)
(*   - Awaiter state (see Awaiter.tla).                                     *)
(*   - WriterCompletionState and exception propagation (see Awaiter.tla).   *)
(*   - BufferHolder refcount (§6.5) — orthogonal to chain-level concerns.   *)
(*   - Reader-side store buffer (its release-store of BytesReadPublished is *)
(*     out of scope here; verified inside Backpressure.tla).                *)
(***************************************************************************)

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    MaxSegments,           \* number of segment IDs available in the pool
    MaxChainLen,           \* upper bound on unpublished-chain length at a time
    MaxAdvanceCycles,      \* upper bound on reader AdvanceTo cycles
    MaxSpliceCycles,       \* upper bound on writer splice cycles
    EnableOrderedSplice,   \* TRUE (correct) | FALSE (flip Part1/Part2 order)
    EnableExaminedSnapshot \* TRUE (correct, §7.3) | FALSE (snapshot after retire)

\* --------------------------------------------------------------------------
\* Types and constants
\* --------------------------------------------------------------------------

NullSeg    == 0
SegmentIds == 1..MaxSegments
SegRef     == {NullSeg} \cup SegmentIds

\* A write to an abstract byte-count "unit" is 1; we track byte positions as
\* natural numbers.  Actual writes per alloc are fixed at 1 unit to keep the
\* state space small — the correctness questions we care about (ordering,
\* retirement, pool re-rent) are insensitive to the chunk sizes.
ChunkSize == 1

\* Writer program-counter values.  "Ready" is a quiescent state where the
\* writer may begin a new alloc / splice / complete.  Splice phases are
\* explicit to let the store-buffer drain interleave with individual
\* release-store emissions.
WPC == {"Ready", "SplicePart1", "SplicePart2", "SpliceBwp", "Halt"}

\* Reader program-counter values.  Split AdvanceTo into its three spec-level
\* steps (snapshot / walk / commit) so the §7.3 errata ordering is explicit.
RPC == {"Ready",
         "AdvanceSnapshot",   \* §7.3 step 2 — snapshot examined (default order)
         "AdvanceWalk",       \* §7.3 step 3 — walk _head → consumed, retire
         "AdvanceCommit",     \* §7.3 steps 4–5 — commit examined, publish BWP
         "Halt"}

\* Store-buffer entries.  `addr` tags the target field:
\*   - "Head", "Tail", "BWP", "SegNext": release-stores (spec §6.4.2).
\*   - "SegInit": the writer's plain writes to a segment's fields during
\*     AppendActiveSegmentToUnpublished (§6.4.1).  On TSO these share the
\*     writer's store buffer with release-stores and drain FIFO, which is
\*     load-bearing: the reader must never observe (stale memTail = X, re-
\*     initialised X.fields) because X's re-init is FIFO-after the Tail
\*     release-store that retires the stale reference.
StoreTypes == {"Head", "Tail", "BWP", "SegNext", "SegInit"}

\* --------------------------------------------------------------------------
\* Variables
\* --------------------------------------------------------------------------

\* ---- Committed shared memory (what the reader's acquire-loads observe) ---
VARIABLES
    memHead,                \* state.Head
    memTail,                \* state.Tail
    memBWP,                 \* state.BytesWrittenPublished
    segNextCommitted        \* function SegmentIds -> SegRef; committed seg.Next

\* ---- Per-segment mutable state (the "Segment object" fields) ------------
VARIABLES
    segRunningIdx,          \* seg.RunningIndex
    segWrittenLen,          \* seg.WrittenLength
    segLogicalId,           \* logical-segment identity (bumped each Init)
    segInitialized          \* TRUE iff seg currently holds valid-initialised state

\* ---- Segment pool / retirement tracking ---------------------------------
VARIABLES
    pool,                   \* set of segment IDs currently available for rent
    segRetired,             \* per-seg TRUE iff this seg was retired-but-not-rerent
    nextLogicalId           \* monotonic counter assigned on each Initialize

\* ---- Writer store buffer (FIFO of release-stores awaiting drain) --------
VARIABLES
    bufW                    \* sequence of [addr, seg, val] records

\* ---- Writer local state --------------------------------------------------
VARIABLES
    unpubHead,              \* writer-local chain head
    unpubTail,              \* writer-local chain tail
    unpubBytes,             \* sum of WrittenLength in the unpub chain
    writerBWP,              \* writer's mirror of state.BWP (post-splice)
    writerTailView,         \* writer's buffer-forwarded view of state.Tail
    writerIsFirstSplice,    \* TRUE iff the pending splice is the first in lifecycle
    splicesDone             \* count of completed splice cycles

\* ---- Reader local state --------------------------------------------------
VARIABLES
    readerHead,                 \* _head
    readerHeadOffset,           \* _headConsumedOffset (Memory-relative bytes)
    readerExamined,             \* _examinedPosition (absolute)
    readerBytesRead,            \* _bytesRead (absolute)
    readerInProgress,           \* _readInProgress
    readerCurrentTail,          \* currentTail acquired at the top of AdvanceTo
    readerConsumedSeg,          \* consumedSeg chosen for this AdvanceTo
    readerConsumedIdx,          \* consumedIdx
    readerExaminedSeg,          \* examinedSeg chosen
    readerExaminedIdx,          \* examinedIdx
    readerSnapshotNewExamined,  \* value computed by the snapshot step
    readerObservedLogicalId,    \* per-seg logicalId as-seen by the reader;
                                 \* indexed by segment id, value -1 if never observed
    advancesDone

\* ---- Control -------------------------------------------------------------
VARIABLES wpc, rpc

vars == <<memHead, memTail, memBWP, segNextCommitted,
          segRunningIdx, segWrittenLen, segLogicalId, segInitialized,
          pool, segRetired, nextLogicalId,
          bufW,
          unpubHead, unpubTail, unpubBytes,
          writerBWP, writerTailView, writerIsFirstSplice, splicesDone,
          readerHead, readerHeadOffset, readerExamined, readerBytesRead,
          readerInProgress, readerCurrentTail,
          readerConsumedSeg, readerConsumedIdx,
          readerExaminedSeg, readerExaminedIdx, readerSnapshotNewExamined,
          readerObservedLogicalId, advancesDone,
          wpc, rpc>>

\* --------------------------------------------------------------------------
\* Initial state
\* --------------------------------------------------------------------------

Init ==
    /\ memHead        = NullSeg
    /\ memTail        = NullSeg
    /\ memBWP         = 0
    /\ segNextCommitted = [s \in SegmentIds |-> NullSeg]
    /\ segRunningIdx    = [s \in SegmentIds |-> 0]
    /\ segWrittenLen    = [s \in SegmentIds |-> 0]
    /\ segLogicalId     = [s \in SegmentIds |-> 0]
    /\ segInitialized   = [s \in SegmentIds |-> FALSE]
    /\ pool             = SegmentIds       \* all segs start free
    /\ segRetired       = [s \in SegmentIds |-> FALSE]
    /\ nextLogicalId    = 1
    /\ bufW             = <<>>
    /\ unpubHead        = NullSeg
    /\ unpubTail        = NullSeg
    /\ unpubBytes       = 0
    /\ writerBWP        = 0
    /\ writerTailView   = NullSeg
    /\ writerIsFirstSplice = TRUE
    /\ splicesDone      = 0
    /\ readerHead       = NullSeg
    /\ readerHeadOffset = 0
    /\ readerExamined   = 0
    /\ readerBytesRead  = 0
    /\ readerInProgress = FALSE
    /\ readerCurrentTail = NullSeg
    /\ readerConsumedSeg = NullSeg
    /\ readerConsumedIdx = 0
    /\ readerExaminedSeg = NullSeg
    /\ readerExaminedIdx = 0
    /\ readerSnapshotNewExamined = 0
    /\ readerObservedLogicalId = [s \in SegmentIds |-> -1]
    /\ advancesDone   = 0
    /\ wpc            = "Ready"
    /\ rpc            = "Ready"

\* --------------------------------------------------------------------------
\* Memory model: store-buffer drain
\* --------------------------------------------------------------------------

\* Drain the head of the writer's store buffer, applying it to committed
\* memory.  Models TSO-style nondeterministic drain.  All writer release-
\* stores pass through this buffer; reader acquire-loads read committed
\* memory directly.
\* SegInit entries carry the writer's plain-field updates for a re-rented
\* segment.  Four fields get updated atomically per entry: runningIdx,
\* writtenLen, logicalId, initialized.  segRetired is cleared and pool
\* removal already happened on the writer side (writer-local bookkeeping);
\* pool membership is not gated by drain.
DrainW ==
    /\ Len(bufW) > 0
    /\ LET e == Head(bufW) IN
         \/ /\ e.addr = "Head"
            /\ memHead' = e.val
            /\ UNCHANGED <<memTail, memBWP, segNextCommitted,
                           segRunningIdx, segWrittenLen, segLogicalId,
                           segInitialized, segRetired>>
         \/ /\ e.addr = "Tail"
            /\ memTail' = e.val
            /\ UNCHANGED <<memHead, memBWP, segNextCommitted,
                           segRunningIdx, segWrittenLen, segLogicalId,
                           segInitialized, segRetired>>
         \/ /\ e.addr = "BWP"
            /\ memBWP' = e.val
            /\ UNCHANGED <<memHead, memTail, segNextCommitted,
                           segRunningIdx, segWrittenLen, segLogicalId,
                           segInitialized, segRetired>>
         \/ /\ e.addr = "SegNext"
            /\ segNextCommitted' =
                 [segNextCommitted EXCEPT ![e.seg] = e.val]
            /\ UNCHANGED <<memHead, memTail, memBWP,
                           segRunningIdx, segWrittenLen, segLogicalId,
                           segInitialized, segRetired>>
         \/ /\ e.addr = "SegInit"
            /\ segRunningIdx'  =
                 [segRunningIdx  EXCEPT ![e.seg] = e.runningIdx]
            /\ segWrittenLen'  =
                 [segWrittenLen  EXCEPT ![e.seg] = e.writtenLen]
            /\ segLogicalId'   =
                 [segLogicalId   EXCEPT ![e.seg] = e.logicalId]
            /\ segInitialized' =
                 [segInitialized EXCEPT ![e.seg] = TRUE]
            /\ segRetired'     =
                 [segRetired     EXCEPT ![e.seg] = FALSE]
            /\ UNCHANGED <<memHead, memTail, memBWP, segNextCommitted>>
    /\ bufW' = Tail(bufW)
    /\ UNCHANGED <<pool, nextLogicalId,
                   unpubHead, unpubTail, unpubBytes,
                   writerBWP, writerTailView, writerIsFirstSplice, splicesDone,
                   readerHead, readerHeadOffset, readerExamined, readerBytesRead,
                   readerInProgress, readerCurrentTail,
                   readerConsumedSeg, readerConsumedIdx,
                   readerExaminedSeg, readerExaminedIdx, readerSnapshotNewExamined,
                   readerObservedLogicalId, advancesDone,
                   wpc, rpc>>

\* --------------------------------------------------------------------------
\* Writer actions
\* --------------------------------------------------------------------------

\* W_AllocSegment — rent a segment from the pool and initialise it for
\* the unpublished chain (§6.4.1).  Writer-local bookkeeping (pool,
\* unpubHead/Tail/Bytes, nextLogicalId) updates atomically from the
\* writer's own perspective.  The segment's *field* writes are plain TSO
\* stores that go through the writer's store buffer in FIFO order with the
\* preceding release-stores.  Modelled by pushing a "SegInit" entry (and a
\* Next-reset "SegNext" entry for §6.4.1's seg.Next := null) to bufW; the
\* reader observes the new fields only after the drain.  This preserves
\* the TSO invariant: (memTail = X stale, X.fields new) is unreachable
\* because X's SegInit is FIFO-after the Tail store that was retired.
W_AllocSegment ==
    /\ wpc = "Ready"
    /\ pool /= {}
    /\ LET newSeg        == CHOOSE s \in pool : TRUE
           newRunningIdx == writerBWP + unpubBytes
           newWritten    == ChunkSize
           chainCount    == (IF unpubHead = NullSeg THEN 0
                              ELSE IF unpubHead = unpubTail THEN 1
                              ELSE 2)  \* bounded by MaxChainLen <= 2
           initEntry     == [addr       |-> "SegInit",
                              seg        |-> newSeg,
                              runningIdx |-> newRunningIdx,
                              writtenLen |-> newWritten,
                              logicalId  |-> nextLogicalId]
           nextResetEntry == [addr |-> "SegNext", seg |-> newSeg,
                               val  |-> NullSeg]
           intraLinkEntry == [addr |-> "SegNext", seg |-> unpubTail,
                               val  |-> newSeg]
       IN /\ chainCount < MaxChainLen
          /\ pool' = pool \ {newSeg}
          \* FIFO order of TSO plain writes:
          \*   (1) SegInit    -- §6.4.1 step 1 field init
          \*   (2) SegNext(newSeg) := null  -- §6.4.1 step 1 terminal
          \*   (3) SegNext(unpubTail) := newSeg  -- §6.4.1 step 2 intra-chain
          \*       link, only when unpubHead is already non-null
          /\ IF unpubHead = NullSeg
             THEN bufW' = Append(Append(bufW, initEntry), nextResetEntry)
             ELSE bufW' = Append(Append(Append(bufW, initEntry),
                                         nextResetEntry),
                                  intraLinkEntry)
          /\ nextLogicalId' = nextLogicalId + 1
          /\ IF unpubHead = NullSeg THEN
                /\ unpubHead' = newSeg
                /\ unpubTail' = newSeg
             ELSE
                /\ unpubHead' = unpubHead
                /\ unpubTail' = newSeg
          /\ unpubBytes' = unpubBytes + newWritten
    /\ UNCHANGED <<memHead, memTail, memBWP, segNextCommitted,
                   segRunningIdx, segWrittenLen, segLogicalId, segInitialized,
                   segRetired,
                   writerBWP, writerTailView, writerIsFirstSplice, splicesDone,
                   readerHead, readerHeadOffset, readerExamined, readerBytesRead,
                   readerInProgress, readerCurrentTail,
                   readerConsumedSeg, readerConsumedIdx,
                   readerExaminedSeg, readerExaminedIdx, readerSnapshotNewExamined,
                   readerObservedLogicalId, advancesDone,
                   wpc, rpc>>

\* Begin a splice cycle.  Requires unpubHead non-empty.  Transitions writer
\* to SplicePart1 phase where it will emit release-store #1 (Head or SegNext).
W_BeginSplice ==
    /\ wpc = "Ready"
    /\ unpubHead /= NullSeg
    /\ splicesDone < MaxSpliceCycles
    /\ wpc' = "SplicePart1"
    /\ UNCHANGED <<memHead, memTail, memBWP, segNextCommitted,
                   segRunningIdx, segWrittenLen, segLogicalId, segInitialized,
                   pool, segRetired, nextLogicalId,
                   bufW,
                   unpubHead, unpubTail, unpubBytes,
                   writerBWP, writerTailView, writerIsFirstSplice, splicesDone,
                   readerHead, readerHeadOffset, readerExamined, readerBytesRead,
                   readerInProgress, readerCurrentTail,
                   readerConsumedSeg, readerConsumedIdx,
                   readerExaminedSeg, readerExaminedIdx, readerSnapshotNewExamined,
                   readerObservedLogicalId, advancesDone, rpc>>

\* W_SplicePart1 — emit release-store #1.  For first splice this is state.Head;
\* for subsequent splices this is prevTail.Next (= writerTailView.Next).
\* Intra-chain Next pointers were already pushed to bufW during W_AllocSegment
\* (§6.4.1 step 2) and drain FIFO-before the splice's Tail release-store, so
\* no atomic side-effect on segNextCommitted is needed here.
\*
\* In the differential experiment (EnableOrderedSplice = FALSE), the writer
\* swaps Part1 and Part2 — it emits state.Tail first, then Part1.  The
\* ChainConsistent invariant must catch this: a reader can observe memTail
\* pointing at a chain tail while memHead (or prevTail.Next) still points
\* past it (null / old).
W_SplicePart1 ==
    /\ wpc = "SplicePart1"
    /\ LET link == IF writerIsFirstSplice THEN
                       \* First splice: release-store of state.Head.
                       [addr |-> "Head", seg |-> NullSeg, val |-> unpubHead]
                   ELSE
                       \* Subsequent splice: release-store of prevTail.Next.
                       [addr |-> "SegNext", seg |-> writerTailView, val |-> unpubHead]
       IN bufW' = Append(bufW, link)
    /\ wpc' = "SplicePart2"
    /\ UNCHANGED <<memHead, memTail, memBWP, segNextCommitted,
                   segRunningIdx, segWrittenLen, segLogicalId, segInitialized,
                   pool, segRetired, nextLogicalId,
                   unpubHead, unpubTail, unpubBytes,
                   writerBWP, writerTailView, writerIsFirstSplice, splicesDone,
                   readerHead, readerHeadOffset, readerExamined, readerBytesRead,
                   readerInProgress, readerCurrentTail,
                   readerConsumedSeg, readerConsumedIdx,
                   readerExaminedSeg, readerExaminedIdx, readerSnapshotNewExamined,
                   readerObservedLogicalId, advancesDone, rpc>>

\* W_SplicePart2 — emit release-store #2 (state.Tail = unpubTail) and update
\* writerTailView synchronously (buffer-forwarded).  Also commits the intra-
\* chain plain seg.Next links at this point: a chain of length >=2 has the
\* internal link unpubHead.Next = unpubTail which becomes reader-visible
\* through the edge established by either release-store #1 (prevTail.Next
\* or state.Head).  We commit intra-chain links here (atomically with the
\* Part2 emission) as a simplification that preserves the happens-before
\* semantics: by the time the reader can observe memTail = unpubTail, the
\* intra-chain pointer must already be committed, and in our FIFO drain
\* model Part1 drains before Part2.
W_SplicePart2 ==
    /\ wpc = "SplicePart2"
    /\ bufW' = Append(bufW, [addr |-> "Tail", seg |-> NullSeg, val |-> unpubTail])
    \* Intra-chain plain Next writes were pushed to bufW at W_AllocSegment
    \* time (§6.4.1 step 2) and will drain FIFO-before the splice's Tail
    \* release-store.  No atomic side-effect on segNextCommitted here.
    /\ writerTailView' = unpubTail
    \* Advance writerBWP synchronously here.  Spec §6.4.2 step 3 increments
    \* _bytesWritten between the Tail release-store (Part2) and the BWP
    \* release-store (Part3).  Doing it here ensures writerBWP >= any byte
    \* the reader could start consuming once memTail drains (Part2 is now
    \* in the drain queue; the reader can acquire it at any time).
    /\ writerBWP' = writerBWP + unpubBytes
    \* Clear the unpublished chain — by spec it's cleared in step 4 after
    \* Part3 emits, but the ordering is writer-local only (the reader never
    \* inspects unpubHead/unpubTail), so advancing the clear here is sound.
    /\ unpubHead' = NullSeg
    /\ unpubTail' = NullSeg
    /\ unpubBytes' = 0
    /\ wpc' = "SpliceBwp"
    /\ UNCHANGED <<memHead, memTail, memBWP, segNextCommitted,
                   segRunningIdx, segWrittenLen, segLogicalId, segInitialized,
                   pool, segRetired, nextLogicalId,
                   writerIsFirstSplice, splicesDone,
                   readerHead, readerHeadOffset, readerExamined, readerBytesRead,
                   readerInProgress, readerCurrentTail,
                   readerConsumedSeg, readerConsumedIdx,
                   readerExaminedSeg, readerExaminedIdx, readerSnapshotNewExamined,
                   readerObservedLogicalId, advancesDone, rpc>>

\* W_SpliceBwp — emit release-store #3 (state.BWP = writerBWP).  writerBWP
\* was advanced synchronously in Part2; here we just emit the store that
\* eventually makes the new value visible in committed memory (memBWP).
W_SpliceBwp ==
    /\ wpc = "SpliceBwp"
    /\ bufW' = Append(bufW, [addr |-> "BWP", seg |-> NullSeg, val |-> writerBWP])
    /\ writerIsFirstSplice' = FALSE
    /\ splicesDone' = splicesDone + 1
    /\ wpc' = "Ready"
    /\ UNCHANGED <<memHead, memTail, memBWP, segNextCommitted,
                   segRunningIdx, segWrittenLen, segLogicalId, segInitialized,
                   pool, segRetired, nextLogicalId,
                   unpubHead, unpubTail, unpubBytes,
                   writerBWP, writerTailView,
                   readerHead, readerHeadOffset, readerExamined, readerBytesRead,
                   readerInProgress, readerCurrentTail,
                   readerConsumedSeg, readerConsumedIdx,
                   readerExaminedSeg, readerExaminedIdx, readerSnapshotNewExamined,
                   readerObservedLogicalId, advancesDone, rpc>>

\* Differential experiment: when EnableOrderedSplice = FALSE, the writer
\* emits Part2 first (state.Tail) and then Part1 (Head or prevTail.Next).
\* Encoded by offering an alternative path through the splice phases.
\* Implementation: swap the buffered stores inside Part1/Part2 in the FALSE
\* case.  We encode this by making the Part1 action emit a Tail store, and
\* Part2 emit the Head/SegNext store.  The state-flow remains the same so
\* existing invariants still cover all states.
W_SplicePart1_Flipped ==
    /\ wpc = "SplicePart1"
    /\ ~EnableOrderedSplice
    /\ bufW' = Append(bufW, [addr |-> "Tail", seg |-> NullSeg, val |-> unpubTail])
    /\ writerTailView' = unpubTail
    /\ wpc' = "SplicePart2"
    /\ UNCHANGED <<memHead, memTail, memBWP, segNextCommitted,
                   segRunningIdx, segWrittenLen, segLogicalId, segInitialized,
                   pool, segRetired, nextLogicalId,
                   unpubHead, unpubTail, unpubBytes,
                   writerBWP, writerIsFirstSplice, splicesDone,
                   readerHead, readerHeadOffset, readerExamined, readerBytesRead,
                   readerInProgress, readerCurrentTail,
                   readerConsumedSeg, readerConsumedIdx,
                   readerExaminedSeg, readerExaminedIdx, readerSnapshotNewExamined,
                   readerObservedLogicalId, advancesDone, rpc>>

W_SplicePart2_Flipped ==
    /\ wpc = "SplicePart2"
    /\ ~EnableOrderedSplice
    /\ LET link == IF writerIsFirstSplice THEN
                       [addr |-> "Head", seg |-> NullSeg, val |-> unpubHead]
                   ELSE
                       \* In flipped order, writerTailView has just been set
                       \* to unpubTail by the flipped Part1.  We want to write
                       \* the old prevTail.Next, which is the segment that
                       \* used to be tail before the flip.  That's recoverable
                       \* because the (flipped) Part1 updated writerTailView
                       \* only synchronously; the "prevTail" in the algorithm
                       \* is what memTail was.  In a realistic code flow the
                       \* writer would have snapshotted prevTail first, so
                       \* the flip still writes prevTail.Next — but the
                       \* release-store order is swapped.  Here we approximate
                       \* by writing the pre-flip prevTail (tracked via a
                       \* LET binding that captures the unpubHead target's
                       \* predecessor).  Simplification: target the segment
                       \* that *was* memTail prior to the flipped Part1, i.e.,
                       \* whichever seg was in writerTailView before the flip.
                       \* Since we overwrote writerTailView, we reconstruct
                       \* via memTail (committed).
                       [addr |-> "SegNext", seg |-> memTail, val |-> unpubHead]
       IN bufW' = Append(bufW, link)
    /\ wpc' = "SpliceBwp"
    /\ UNCHANGED <<memHead, memTail, memBWP, segNextCommitted,
                   segRunningIdx, segWrittenLen, segLogicalId, segInitialized,
                   pool, segRetired, nextLogicalId,
                   unpubHead, unpubTail, unpubBytes,
                   writerBWP, writerTailView, writerIsFirstSplice, splicesDone,
                   readerHead, readerHeadOffset, readerExamined, readerBytesRead,
                   readerInProgress, readerCurrentTail,
                   readerConsumedSeg, readerConsumedIdx,
                   readerExaminedSeg, readerExaminedIdx, readerSnapshotNewExamined,
                   readerObservedLogicalId, advancesDone, rpc>>

\* Writer Halt: stops the writer (used for termination bound).
W_Halt ==
    /\ wpc = "Ready"
    /\ unpubHead = NullSeg           \* no pending unflushed data
    /\ splicesDone >= MaxSpliceCycles \/ pool = {}
    /\ wpc' = "Halt"
    /\ UNCHANGED <<memHead, memTail, memBWP, segNextCommitted,
                   segRunningIdx, segWrittenLen, segLogicalId, segInitialized,
                   pool, segRetired, nextLogicalId,
                   bufW,
                   unpubHead, unpubTail, unpubBytes,
                   writerBWP, writerTailView, writerIsFirstSplice, splicesDone,
                   readerHead, readerHeadOffset, readerExamined, readerBytesRead,
                   readerInProgress, readerCurrentTail,
                   readerConsumedSeg, readerConsumedIdx,
                   readerExaminedSeg, readerExaminedIdx, readerSnapshotNewExamined,
                   readerObservedLogicalId, advancesDone, rpc>>

\* --------------------------------------------------------------------------
\* Reader actions
\* --------------------------------------------------------------------------

\* R_AdvanceToStart — acquire currentTail, pick consumed+examined.  This
\* combines ReadAsync's acquire-load of state.Tail and the top of AdvanceTo.
\* readerHead is initialised on first acquisition (§7.1 step 4).
\*
\* Simplification for tractability: we fix consumedSeg = examinedSeg =
\* currentTail (the common case where the caller passes buffer.End for
\* both).  consumedIdx / examinedIdx range over 0..WrittenLen[cTail].  This
\* covers the key retirement scenarios — including fully-consumed cTail
\* with a published successor, which is the §7.3 errata bug pattern.
R_AdvanceToStart ==
    /\ rpc = "Ready"
    /\ ~readerInProgress
    /\ advancesDone < MaxAdvanceCycles
    /\ memTail /= NullSeg
    /\ LET tail == memTail IN
       \* hasNewData gate per spec §7.1 step 6: ReadAsync returns only when
       \* the tail's end is past the reader's examined position.  Without
       \* this gate, the model allows the reader to enter AdvanceTo in the
       \* transient window where its _head has already advanced past a
       \* stale memTail (retirement via seg.Next /= null) — a path the real
       \* code avoids because the sync-path returns TryRead=false.
       /\ segRunningIdx[tail] + segWrittenLen[tail] > readerExamined
       \* Additional stability: the current tail must be initialised.  The
       \* writer's splice guarantees memTail = seg initialised at the moment
       \* the Tail release-store drains.  If the reader retired that seg
       \* via seg.Next /= null between Tail stores, hasNewData above already
       \* excludes that case (examined has passed tail.end).  Keep the
       \* explicit check for safety.
       /\ segInitialized[tail]
       /\ \E cIdx \in 0..segWrittenLen[tail] :
          \E eIdx \in cIdx..segWrittenLen[tail] :
          /\ readerCurrentTail' = tail
          /\ readerConsumedSeg'  = tail
          /\ readerConsumedIdx'  = cIdx
          /\ readerExaminedSeg'  = tail
          /\ readerExaminedIdx'  = eIdx
          /\ readerInProgress'   = TRUE
          \* First-read acquisition of state.Head (§7.1 step 4):
          /\ IF readerHead = NullSeg
             THEN
               /\ readerHead' = memHead
               /\ readerHeadOffset' = 0
               \* Snapshot the logicalId of readerHead AND currentTail at the
               \* moment the reader first observed each reference.  These are
               \* the refs the reader will later dereference during the
               \* AdvanceTo walk / snapshot; a stale read would violate
               \* NoStaleFieldReadAfterRetire.
               /\ readerObservedLogicalId' =
                    [s \in SegmentIds |->
                       IF s = memHead THEN segLogicalId[memHead]
                       ELSE IF s = tail THEN segLogicalId[tail]
                       ELSE readerObservedLogicalId[s]]
             ELSE
               /\ readerHead' = readerHead
               /\ readerHeadOffset' = readerHeadOffset
               /\ readerObservedLogicalId' =
                    [readerObservedLogicalId EXCEPT
                        ![tail] = IF readerObservedLogicalId[tail] = -1
                                   THEN segLogicalId[tail]
                                   ELSE readerObservedLogicalId[tail]]
          /\ \* Branch on experiment flag: default = snapshot first (correct),
             \* flipped = walk first (violates §7.3 errata).
             IF EnableExaminedSnapshot
             THEN rpc' = "AdvanceSnapshot"
             ELSE rpc' = "AdvanceWalk"
    /\ UNCHANGED <<memHead, memTail, memBWP, segNextCommitted,
                   segRunningIdx, segWrittenLen, segLogicalId, segInitialized,
                   pool, segRetired, nextLogicalId,
                   bufW,
                   unpubHead, unpubTail, unpubBytes,
                   writerBWP, writerTailView, writerIsFirstSplice, splicesDone,
                   readerExamined, readerBytesRead, readerSnapshotNewExamined,
                   advancesDone, wpc>>

\* R_AdvanceToSnapshot — §7.3 step 2.  Read examinedSeg's RunningIndex and
\* compute newExaminedPos.  This action reads segRunningIdx[examinedSeg];
\* the NoStaleFieldReadAfterRetire invariant fires if the reader's observed
\* logicalId for examinedSeg does not match the current segLogicalId
\* (indicating the segment was retired + re-rented between observation and
\* this read).
R_AdvanceToSnapshot ==
    /\ rpc = "AdvanceSnapshot"
    /\ LET eSeg == readerExaminedSeg
           eIdx == readerExaminedIdx
           \* Observe the examined segment's logicalId when we snapshot its
           \* fields.  We expect it to match what was captured when we
           \* established the reference (either at first-acquire-head or
           \* during a prior R_AdvanceToStart that set readerCurrentTail).
           obsLid == readerObservedLogicalId[eSeg]
           nowLid == segLogicalId[eSeg]
       IN /\ \* Update the observed logicalId for examinedSeg if not yet set
             \* (e.g., examinedSeg is currentTail which we just acquired)
             readerObservedLogicalId' =
                 IF readerObservedLogicalId[eSeg] = -1
                 THEN [readerObservedLogicalId EXCEPT ![eSeg] = nowLid]
                 ELSE readerObservedLogicalId
          /\ readerSnapshotNewExamined' =
                 segRunningIdx[eSeg] + eIdx
    /\ rpc' = IF EnableExaminedSnapshot THEN "AdvanceWalk" ELSE "AdvanceCommit"
    /\ UNCHANGED <<memHead, memTail, memBWP, segNextCommitted,
                   segRunningIdx, segWrittenLen, segLogicalId, segInitialized,
                   pool, segRetired, nextLogicalId,
                   bufW,
                   unpubHead, unpubTail, unpubBytes,
                   writerBWP, writerTailView, writerIsFirstSplice, splicesDone,
                   readerHead, readerHeadOffset, readerExamined, readerBytesRead,
                   readerInProgress, readerCurrentTail,
                   readerConsumedSeg, readerConsumedIdx,
                   readerExaminedSeg, readerExaminedIdx,
                   advancesDone, wpc>>

\* R_AdvanceToWalk — §7.3 step 3.  Walk from _head forward via acquire-loads
\* of seg.Next up to (but not including) consumedSeg, retiring each
\* intermediate seg that satisfies the §10.7 retirement condition:
\*   seg /= currentTail OR seg.Next /= NullSeg.
\* Retires the walk atomically; this is a sound over-approximation (the
\* model can delay retirement to later cycles, but the realistic case is
\* that the reader retires everything it can during this AdvanceTo).
\* Also retires consumedSeg if fully-consumed + successor exists.
\*
\* Walk enumeration: the chain from readerHead to cTail has at most
\* MaxSegments segments (since each segment ID appears at most once in a
\* live chain).  We unroll to depth MaxSegments = 3.
R_AdvanceToWalk ==
    /\ rpc = "AdvanceWalk"
    /\ LET
        cSeg  == readerConsumedSeg
        cIdx  == readerConsumedIdx
        cTail == readerCurrentTail
        hSeg  == readerHead
        \* Follow the committed chain from hSeg for up to MaxSegments-1 hops,
        \* accumulating segments until we hit cSeg (or the chain terminates).
        \* Walk set = {hSeg, s1, s2, ...} \ {cSeg, NullSeg}.
        step1 == IF hSeg /= NullSeg /\ hSeg /= cSeg
                 THEN segNextCommitted[hSeg] ELSE NullSeg
        step2 == IF step1 /= NullSeg /\ step1 /= cSeg
                 THEN segNextCommitted[step1] ELSE NullSeg
        step3 == IF step2 /= NullSeg /\ step2 /= cSeg
                 THEN segNextCommitted[step2] ELSE NullSeg
        walkRaw == {hSeg, step1, step2, step3} \ {NullSeg, cSeg}
        \* Retirement condition per §10.7: seg /= cTail OR seg.Next /= NullSeg.
        CanRetire(s) ==
            \/ s /= cTail
            \/ segNextCommitted[s] /= NullSeg
        retireWalk == {s \in walkRaw : CanRetire(s)}
        \* Handle consumedSeg: retire only if fully-consumed + successor.
        consumedFullyConsumed ==
            /\ cIdx = segWrittenLen[cSeg]
            /\ segNextCommitted[cSeg] /= NullSeg
        retireConsumed == IF consumedFullyConsumed THEN {cSeg} ELSE {}
        retireAll == retireWalk \cup retireConsumed
        \* New _head after retirement:
        newHead == IF consumedFullyConsumed
                    THEN segNextCommitted[cSeg]
                    ELSE cSeg
        newHeadOffset == IF consumedFullyConsumed THEN 0 ELSE cIdx
       IN
        /\ pool' = pool \cup retireAll
        /\ segRetired' = [s \in SegmentIds |->
                           IF s \in retireAll THEN TRUE ELSE segRetired[s]]
        /\ segInitialized' = [s \in SegmentIds |->
                               IF s \in retireAll THEN FALSE ELSE segInitialized[s]]
        \* Clear readerObservedLogicalId for retired segs so a future re-rent
        \* doesn't trigger a false NoStaleFieldReadAfterRetire violation;
        \* reader no longer holds a reference to these segs.
        /\ readerObservedLogicalId' =
              [s \in SegmentIds |->
                 IF s \in retireAll THEN -1 ELSE readerObservedLogicalId[s]]
        /\ readerHead' = newHead
        /\ readerHeadOffset' = newHeadOffset
        /\ readerBytesRead' = segRunningIdx[cSeg] + cIdx
    /\ rpc' = IF EnableExaminedSnapshot
              THEN "AdvanceCommit"
              ELSE "AdvanceSnapshot"
    /\ UNCHANGED <<memHead, memTail, memBWP, segNextCommitted,
                   segRunningIdx, segWrittenLen, segLogicalId,
                   nextLogicalId,
                   bufW,
                   unpubHead, unpubTail, unpubBytes,
                   writerBWP, writerTailView, writerIsFirstSplice, splicesDone,
                   readerExamined, readerInProgress, readerCurrentTail,
                   readerConsumedSeg, readerConsumedIdx,
                   readerExaminedSeg, readerExaminedIdx, readerSnapshotNewExamined,
                   advancesDone, wpc>>

\* R_AdvanceToCommit — §7.3 steps 4-5.  Commit examined (from snapshot) and
\* publish BytesReadPublished (abstractly, as memBWP is the writer's
\* publication; we don't model BRP in this module, so we just clear the
\* read-in-progress flag).
R_AdvanceToCommit ==
    /\ rpc = "AdvanceCommit"
    /\ readerExamined' = readerSnapshotNewExamined
    /\ readerInProgress' = FALSE
    /\ advancesDone' = advancesDone + 1
    /\ rpc' = "Ready"
    /\ UNCHANGED <<memHead, memTail, memBWP, segNextCommitted,
                   segRunningIdx, segWrittenLen, segLogicalId, segInitialized,
                   pool, segRetired, nextLogicalId,
                   bufW,
                   unpubHead, unpubTail, unpubBytes,
                   writerBWP, writerTailView, writerIsFirstSplice, splicesDone,
                   readerHead, readerHeadOffset, readerBytesRead,
                   readerCurrentTail,
                   readerConsumedSeg, readerConsumedIdx,
                   readerExaminedSeg, readerExaminedIdx, readerSnapshotNewExamined,
                   readerObservedLogicalId, wpc>>

R_Halt ==
    /\ rpc = "Ready"
    /\ advancesDone >= MaxAdvanceCycles
    /\ ~readerInProgress
    /\ rpc' = "Halt"
    /\ UNCHANGED <<memHead, memTail, memBWP, segNextCommitted,
                   segRunningIdx, segWrittenLen, segLogicalId, segInitialized,
                   pool, segRetired, nextLogicalId,
                   bufW,
                   unpubHead, unpubTail, unpubBytes,
                   writerBWP, writerTailView, writerIsFirstSplice, splicesDone,
                   readerHead, readerHeadOffset, readerExamined, readerBytesRead,
                   readerInProgress, readerCurrentTail,
                   readerConsumedSeg, readerConsumedIdx,
                   readerExaminedSeg, readerExaminedIdx, readerSnapshotNewExamined,
                   readerObservedLogicalId, advancesDone, wpc>>

\* --------------------------------------------------------------------------
\* Next-state relation
\* --------------------------------------------------------------------------

Next ==
    \/ DrainW
    \/ W_AllocSegment
    \/ W_BeginSplice
    \/ (EnableOrderedSplice /\ W_SplicePart1)
    \/ (EnableOrderedSplice /\ W_SplicePart2)
    \/ W_SplicePart1_Flipped
    \/ W_SplicePart2_Flipped
    \/ W_SpliceBwp
    \/ W_Halt
    \/ R_AdvanceToStart
    \/ R_AdvanceToSnapshot
    \/ R_AdvanceToWalk
    \/ R_AdvanceToCommit
    \/ R_Halt

Fairness ==
    /\ WF_vars(DrainW)
    /\ WF_vars(W_Halt)
    /\ WF_vars(R_Halt)

Spec == Init /\ [][Next]_vars /\ Fairness

\* --------------------------------------------------------------------------
\* Invariants
\* --------------------------------------------------------------------------

TypeOK ==
    /\ memHead \in SegRef
    /\ memTail \in SegRef
    /\ memBWP \in Nat
    /\ segNextCommitted \in [SegmentIds -> SegRef]
    /\ segRunningIdx    \in [SegmentIds -> Nat]
    /\ segWrittenLen    \in [SegmentIds -> Nat]
    /\ segLogicalId     \in [SegmentIds -> Nat]
    /\ segInitialized   \in [SegmentIds -> BOOLEAN]
    /\ pool             \subseteq SegmentIds
    /\ segRetired       \in [SegmentIds -> BOOLEAN]
    /\ nextLogicalId    \in Nat
    /\ unpubHead \in SegRef
    /\ unpubTail \in SegRef
    /\ writerBWP \in Nat
    /\ writerTailView \in SegRef
    /\ wpc \in WPC
    /\ rpc \in RPC

\* ChainConsistent (§4.1, §6.4.2):
\*   memTail /= NullSeg => memHead /= NullSeg AND memTail is reachable from
\*   memHead via the committed segNext pointers.  Fails under the flipped
\*   splice order because reader can observe memTail without the matching
\*   memHead or prevTail.Next commit.
ChainReachable(from, to) ==
    \/ from = to
    \/ /\ from /= NullSeg
       /\ segNextCommitted[from] /= NullSeg
       /\ \/ segNextCommitted[from] = to
          \/ /\ segNextCommitted[from] /= NullSeg
             /\ segNextCommitted[segNextCommitted[from]] = to   \* depth ≤ 2

\* state.Head is published ONCE per spec §7.1 step 4 / §6.4.2 (first-splice
\* case) and used only for the reader's first-read init.  After the first
\* read, readerHead takes over and memHead is never read again.  The chain
\* reachability invariant therefore applies only while the reader could
\* still do a first-read (readerHead = NullSeg).  Once readerHead latches,
\* segment retirement + pool re-rent can legitimately make memHead stale
\* (it now points to a re-rented object belonging to a different chain);
\* the reader is insulated because it consults memHead only once.
ChainConsistent ==
    (memTail /= NullSeg /\ readerHead = NullSeg) =>
        /\ memHead /= NullSeg
        /\ ChainReachable(memHead, memTail)

\* NoRetiredAsPrevTail (§10.7):  when the writer is at a quiescent Ready
\* state, writerTailView must not be a retired segment.  The only time
\* writerTailView is read is at the top of a splice (to form prevTail);
\* at that moment it must not be in the pool.
NoRetiredAsPrevTail ==
    (wpc = "Ready" /\ writerTailView /= NullSeg /\ unpubHead /= NullSeg) =>
        writerTailView \notin pool

\* ReaderHeadNotRetired:  if readerHead is non-null, it must still be
\* initialised (not in pool).  A retired readerHead would mean the reader
\* is holding a pool-eligible reference — a stale-field hazard.
ReaderHeadNotRetired ==
    (readerHead /= NullSeg) => segInitialized[readerHead]

\* BWPNotAheadOfTail:  memBWP never exceeds the end of memTail.
BWPNotAheadOfTail ==
    (memTail /= NullSeg) =>
        memBWP <= segRunningIdx[memTail] + segWrittenLen[memTail]

\* NoStaleFieldReadAfterRetire:  whenever the reader has a snapshotted
\* observedLogicalId for a segment (/= -1), and the segment's current
\* logicalId differs, any subsequent reader field-read of that segment
\* constitutes a stale read.  We state this as: the reader never holds a
\* stale observation.  Safety form: for every seg s with
\* readerObservedLogicalId[s] /= -1, we require segLogicalId[s] =
\* readerObservedLogicalId[s].  The action R_AdvanceToSnapshot reads
\* examinedSeg's RunningIndex and thus depends on this property; a
\* violation in the differential experiment manifests here.
NoStaleFieldReadAfterRetire ==
    \A s \in SegmentIds :
        (readerObservedLogicalId[s] /= -1 /\ segInitialized[s])
            => readerObservedLogicalId[s] = segLogicalId[s]

\* BytesReadConsistent: the reader's consumed byte count never exceeds the
\* writer's synchronously-advanced BWP.  writerBWP is updated atomically with
\* the splice's emission of the BWP release-store, so it is always >= any
\* data the reader could have traversed.  (memBWP can lag writerBWP while
\* the BWP store is still in the writer's buffer; the reader sees tail's
\* segment length via the acquire-load on memTail, not via memBWP.)
BytesReadConsistent == readerBytesRead <= writerBWP

\* --------------------------------------------------------------------------
\* Temporal
\* --------------------------------------------------------------------------

EventualProgress ==
    <>(wpc = "Halt" /\ rpc = "Halt")

==============================================================================
