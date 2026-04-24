# SpscPipe TLA+ verification design

Design for a verification-led redo of SpscPipe's TLA+ models. This document
specifies which modules exist, what each covers, which runtime primitives are
axiomatized, and per module the variables, actions, invariants, and liveness
properties to encode.

**The design doc is prescriptive for the next session's TLA+ work.** The goal
is that every correctness claim in `docs/spscpipe-spec.md` either (a) falls
within the scope of one of these modules and gets mechanically verified, or
(b) is explicitly listed in the Out-of-scope section below with the rationale.
Nothing in the spec should be implicitly assumed correct.

---

## Philosophy

**Verification-led.** The spec asserts correctness claims. The TLA+ models
verify them. Where a claim involves a runtime primitive whose correctness we
do not control (MRVTS, ArrayPool, CancellationToken, ThreadPool), we
*axiomatize* the primitive — model its documented semantics as given — rather
than attempt to reconstruct them from lower-level operations. The models then
verify the SpscPipe protocol against those axioms.

**Abstraction must match the spec's abstraction.** If the spec says "segments
are mutable pooled objects with observable field reinitialization" (§4.1),
the model must represent them that way. Previous modeling chose a coarser
abstraction (segments as immutable values identified by ID, no pool reuse),
which let a real bug through. The new design resists that temptation.

**Scope is declared, not discovered.** Out-of-scope concerns are listed
up-front with rationale. A bug found at runtime that falls inside a declared
scope is a model-level bug (the model should have caught it; extend or fix).
A bug found at runtime that falls in a declared out-of-scope area is a known
gap (documented and accepted, or triggers a follow-up to expand scope).

---

## Module breakdown

Four modules plus one axiom module. Rough sizes given; actual line counts
will vary.

| Module | Spec sections | Role | Est. size |
|---|---|---|---|
| `MRVTS.tla` | §8.7 | Axiom: MRVTS semantics | ~100–150 |
| `Publication.tla` | §4.1, §6.1–§6.4, §7.1–§7.3, §9, §10.7 | Segment lifecycle, publish, retire, pool | ~600–800 |
| `Awaiter.tla` | §6.6, §7.4, §8.1–§8.6, §10.4 | Awaiter handshake, cancellation, completion, exception propagation | ~500–700 |
| `Backpressure.tla` | §6.3, §8.4, §8.5 | Flush-side awaiter with hysteresis | ~300–400 |

**Rationale for the split:**

- Publication and Awaiter are separable: the former models *what data flows
  through the pipe*, the latter models *when threads wait on each other*.
  They touch different invariants (`ChainConsistent`, `NoRetiredAsPrevTail`
  vs. `AtMostOneResult`, `EventuallyUnArmed`). Keeping them separate keeps
  each TLC state space tractable.
- Backpressure is split from Awaiter because it adds a numerical threshold
  dimension (Pause/Resume counters) that the basic reader-signal-reader
  handshake doesn't need. Combining would roughly multiply state spaces;
  splitting lets us verify the hysteresis claim (§8.5) separately with a
  tight state space.
- MRVTS is an axiom module because its correctness isn't ours to verify,
  but its semantics are load-bearing for our code. Separating the axioms
  makes the assumption surface explicit.

Memory-model machinery (TSO-like store buffer with per-thread buffers and
nondeterministic drain, fence actions that flush) is factored into a small
internal helper rather than a module, included by `Publication.tla` and
`Awaiter.tla` directly. It's only ~30 lines and doesn't deserve its own
file.

---

## Axiomatized primitives

| Primitive | Axiomatized in | Rationale |
|---|---|---|
| `ManualResetValueTaskSourceCore<T>` | `MRVTS.tla` | Not ours to verify; documented semantics. Load-bearing for awaiter coordination. |
| `ArrayPool<byte>` / `MemoryPool<byte>` | implicit | Buffer rental/return modeled as nondeterministic choice from a bag of outstanding buffers. Specific pool behavior (sizing, tiers) not modeled; any buffer Rent returns a buffer of at least the requested size. |
| `CancellationToken` + `UnsafeRegister` | implicit | Modeled as an actor that may transition token from "not triggered" to "triggered" at any point, firing registered callback once. |
| `ThreadPool` / `PipeScheduler` | implicit | Modeled as "continuation eventually runs after SetResult." Specific scheduler identity not modeled; `RunContinuationsAsynchronously = true` is the assumed config. |
| `Interlocked.*` | inline | Modeled as atomic operation with full-fence semantics (§5 axiom). Not abstracted further. |
| `Volatile.Read` / `Volatile.Write` | inline | Release/acquire semantics. Modeled via the store-buffer machinery. |

---

## `MRVTS.tla` — axiom module

### Scope

Covers §8.7 of the spec. Provides the MRVTS state machine and its operations
as axioms. Other modules `EXTENDS MRVTS` (or instantiate via `INSTANCE`) to
use them.

### State

```
MrvtsStatus == {"Pending", "Succeeded", "Faulted"}

VARIABLES
    mrvts_status,           \* per-instance Pending | Succeeded | Faulted
    mrvts_version,           \* per-instance monotonic short counter
    mrvts_result,            \* stored result value (for Succeeded)
    mrvts_exception,         \* stored exception (for Faulted)
    mrvts_continuation       \* registered continuation (callback + state + token), or None
```

All five are functions keyed by MRVTS instance ID (we will have two: one for
the reader, one for the writer).

### Actions (as operator definitions callable by other modules)

```
MrvtsReset(inst) ==
    /\ mrvts_status'[inst] = "Pending"
    /\ mrvts_version'[inst] = mrvts_version[inst] + 1
    /\ mrvts_result'[inst] = Default
    /\ mrvts_exception'[inst] = None
    /\ mrvts_continuation'[inst] = None

MrvtsSetResult(inst, result) ==
    /\ mrvts_status[inst] = "Pending"          \* precondition (throws otherwise)
    /\ mrvts_status'[inst] = "Succeeded"
    /\ mrvts_result'[inst] = result
    /\ (if mrvts_continuation[inst] != None, schedule it)

MrvtsSetException(inst, ex) ==
    /\ mrvts_status[inst] = "Pending"
    /\ mrvts_status'[inst] = "Faulted"
    /\ mrvts_exception'[inst] = ex
    /\ (if continuation != None, schedule it)

MrvtsGetResult(inst, token) ==
    /\ token = mrvts_version[inst]             \* precondition
    /\ mrvts_status[inst] != "Pending"
    /\ (returns mrvts_result[inst] or throws mrvts_exception[inst])

MrvtsOnCompleted(inst, cb, state, token, flags) ==
    /\ token = mrvts_version[inst]
    /\ if mrvts_status[inst] = "Pending"
           then store (cb, state) as mrvts_continuation
           else schedule (cb, state) immediately
```

### Invariants

- `MrvtsSetResultPendingPrecondition`: `SetResult` is never called on a
  non-Pending instance. (Violations represent illegal usage.)
- `MrvtsVersionMonotonic`: `mrvts_version` only increases.
- `MrvtsNoSpontaneousTransition`: status transitions only via the defined
  actions.

### Usage from other modules

```
INSTANCE MRVTS WITH mrvts_* <- our_local_instance_vars
```

Or simpler: the other modules manipulate these variables directly as
operators, since TLA+'s `INSTANCE` parameterization is awkward for this
pattern. The next session can pick whichever composition mechanism is
cleanest.

---

## `Publication.tla` — publication, retirement, and pool

### Scope

Covers §4.1 (segment as mutable pooled object), §6.1 (GetMemory + buffer
rotation), §6.2 (Advance), §6.3 (FlushAsync publication), §6.4.1 (append),
§6.4.2 (splice), §7.1 step 4 (first-read head init), §7.2 (traversal),
§7.3 (AdvanceTo with corrected step 2 ordering), §9 (pool mechanics),
§10.7 (retirement invariant including the newly-added reader-side lifetime
obligation).

### Key modeling decisions

- **Segments are mutable, pooled objects.** Each segment ID from
  `1..MaxSegments` represents a Segment *object*. Its fields (RunningIndex,
  WrittenLength, Holder, Next) are state variables. Initialize is an action
  that overwrites these fields. Retirement returns the ID to a pool; a
  subsequent Append can re-rent and re-initialize the same ID.
- **Logical segment vs. object identity.** Model a `logicalSegment` counter
  that increments on every Initialize. This lets invariants reference "the
  segment as of Initialize N" vs. "as of Initialize N+1" even though the
  object ID is the same.
- **Pool is modeled explicitly.** `pool` is a set of segment IDs available
  for rent. `PoolRent` is a nondeterministic action that picks any element
  of `pool` to return to the writer.
- **Store buffer for writer's release-stores.** Captures the TSO-style
  ordering needed for splice's three release-stores.
- **Reader and writer threads modeled as separate actors.** Writer actions:
  allocate, append, splice (three-part), rotate, complete. Reader actions:
  first-read, consume, advance (with snapshot-then-retire ordering from
  errata §7.3).

### Variables

```
\* Committed shared memory
memHead, memTail, memBWP             \* state.Head, state.Tail, BytesWrittenPublished
segNextCommitted                     \* committed seg.Next per segment ID

\* Per-segment fields (mutable, indexed by segment ID)
segRunningIdx, segWrittenLen,
segHolder, segAllocated, segRetired,
segLogicalId                         \* incremented on each Initialize for that seg

\* Writer's store buffer (FIFO)
bufW

\* Writer local state
unpubHead, unpubTail, unpubBytes,
writerBWP,                           \* mirror of committed BWP
writerTailView,                      \* buffer-forwarded view of state.Tail
activeBufferBytes,                   \* bytes in active buffer (for rotation decisions)

\* Reader local state
readerHead,                          \* _head
readerHeadConsumedOffset,            \* _headConsumedOffset
readerExaminedPosition,              \* _examinedPosition
readerBytesRead,                     \* _bytesRead
readerInProgress,                    \* _readInProgress

\* Segment pool
pool                                 \* set of retired segment IDs available for rent

\* Logical timeline
nextLogicalId                        \* monotonic counter for segLogicalId assignment

\* Control (PC for each actor)
wpc, rpc
```

### Actions

**Memory model.** `DrainW`: pop head of `bufW`, apply to committed memory
(FIFO).

**Writer actions.**

- `W_AllocSegment`: rent a segment from `pool` (or allocate a fresh ID if
  pool is empty and count < MaxSegments). Initialize via `SegInit`:
  overwrite all segment fields with new values, increment `segLogicalId`.
  Append to unpub chain.
- `W_Rotate`: models `GetMemory`'s rotation case. Calls `W_AllocSegment`
  behavior for the rotation-out, plus a separate rent for the new active
  buffer. (Active buffer is also abstract; not modeled as a segment.)
- `W_SplicePart1`: release-store `prevTail.Next` or `state.Head` (first
  splice). Buffered.
- `W_SplicePart2`: release-store `state.Tail`. Updates `writerTailView`
  synchronously (buffer-forwarded).
- `W_SpliceBwp`: release-store BWP. Advances `writerBWP`. Clears unpub
  chain.
- `W_Complete`: may splice if unpub non-empty, releases active buffer
  holder, release-stores WriterCompletionState (deferred to Awaiter.tla —
  this module doesn't model completion, only publication).

**Reader actions.**

- `R_FirstAcquireHead`: §7.1 step 4. Acquire-load `state.Head` if not yet
  initialized.
- `R_AcquireTail`: acquire-load `state.Tail` for walking. (In spec's §7.3,
  done at AdvanceTo entry; this action models that.)
- `R_Consume`: advance examined position through a chain segment.
  Abstracted: examined jumps to `seg.RunningIndex + seg.WrittenLength`.
- `R_AdvanceTo`: implements the §7.3 algorithm with corrected step 2
  ordering. In order:
  1. Snapshot examined position (`examinedSeg.RunningIndex + examinedIdx`).
  2. Walk _head → consumedSeg, retiring as §10.7 allows. Retirement puts
     segment ID back in `pool`.
  3. Handle consumedSeg (retire if fully-consumed + successor; keep
     otherwise).
  4. Commit snapshotted examined position.
  5. Release-store BytesReadPublished.

**Pool actions.**

- `PoolReturn(seg)`: called from retire path. Adds seg to `pool`,
  marks `segRetired[seg] = TRUE`. Does *not* clear fields — that happens
  on next Initialize (this is the realistic behavior; fields stay with
  whatever values they had at retirement).

### Invariants

- `TypeOK`: well-typed state.
- `ChainConsistent`: `memTail != NULL => memHead != NULL && memTail
  reachable from memHead via committed segNext`.
- `NoRetiredAsPrevTail`: at `wpc = W_Ready`, `writerTailView` is not in
  `pool` (not retired). Matches the clarified §10.7 anchor-point rule.
- `ReaderHeadNotRetired`: `readerHead != NULL => !segRetired[readerHead]`.
- `BWPNotAheadOfTail`: `memBWP <= memTail.End` (by FIFO drain order).
- **`NoStaleFieldReadAfterRetire`** (new, compared to old model): if the
  reader performs any action that reads `seg.RunningIndex` or
  `seg.WrittenLength` or `seg.Next`, the `segLogicalId` observed by that
  read must match the `segLogicalId` in effect at the time the reader
  first observed the segment reference. Encoded by snapshotting
  `segLogicalId` when the reader first sees the ref and asserting the
  invariant at each subsequent field-read action. This is the property
  that would have caught the examined-position bug.
- `ExaminedPositionMonotonic`: `readerExaminedPosition` only advances.
- `BytesReadMatchesConsumption`: reader-side byte accounting is consistent
  with the segments it has advanced past.

### Temporal

- `EventualProgress`: `<>(wpc = W_Halt /\ rpc = R_Halt)`.

### Differential experiments

To confirm load-bearing properties:

- Flip the ordering in `W_SplicePart1` / `W_SplicePart2`. Expect
  `ChainConsistent` to fail (reversed-splice experiment from old model —
  still load-bearing here, re-verify).
- Reorder `R_AdvanceTo` to compute examined *after* retirement (the bug
  fixed in §7.3 errata). Expect `NoStaleFieldReadAfterRetire` to fail.

---

## `Awaiter.tla` — handshake, cancellation, completion

### Scope

Covers §6.6 (writer Complete + exception propagation), §7.4 (reader
Complete), §8.1 (awaiter states), §8.2 (writer signals reader), §8.3 (reader
arms, double-check with fence), §8.5 hysteresis subtlety for Reader.Complete
(bypass), §8.6 (cancellation), §10.4 (exception propagation semantics on the
reader side only; writer-side symmetric in `Backpressure.tla`).

Uses `MRVTS.tla` axioms for the reader's `_readAwaiter`.

### Key modeling decisions

- **Four actors:** writer, reader, cancellation callback, ThreadPool
  continuation dispatcher. The latter models the "MRVTS schedules
  continuation; ThreadPool eventually runs GetResult" path.
- **Cancellation as an actor.** Non-deterministically fires once, CAS
  Armed→Signaled with `IsCanceled=true`.
- **Publication abstracted.** Unlike `Publication.tla`, we don't model
  segment chains here. "Data was published" is represented as a monotonic
  counter `tailPublished`, matching the old `AwaiterHandshake.tla`. The
  two modules together cover both dimensions.
- **Reader's IValueTaskSource bridge.** GetResult retrieves ReadSignal,
  then calls TryReadCore. The bridge is explicitly modeled.

### Variables

```
\* Shared committed memory
memReaderAwaiterState,                \* Idle=0, Armed=1, Signaled=2
memTailPublished,                     \* abstract: monotonic counter of published bytes
memWriterCompletionState,             \* 0 | 2
memWriterException,                   \* None | "ex"

\* Writer's store buffer
bufW

\* MRVTS for the reader (from MRVTS.tla)
reader_mrvts_*                        \* see MRVTS.tla

\* MRVTS result payload: ReadSignal
\* Modeled as { IsCanceled: BOOLEAN }

\* Reader local
readerExaminedPosition,               \* _examinedPosition
readerInProgress,                     \* _readInProgress
readerPendingArmVersion,              \* version of the currently-awaited VT, or -1

\* Actor PCs
wpc, rpc, cpc, schedPc

\* Bounds
producedCount, armsCompleted, cancelsFired
```

### Actions

**Writer.**  Publish and signal are separate actions (`W_Publish` and
`W_Signal`, gated by `wpc ∈ {Ready, WtrSignaling}`) — they execute
sequentially on the writer thread but reader-thread actions can
interleave between them.  An earlier revision fused them into one
atomic `W_PublishAndSignal`; that fusion hid the race spec §8.2.1
closes.

- `W_Publish`: advance `writerBytesWritten` synchronously; emit the
  TailPublished release-store either as a direct commit (with writer
  fence) or via `bufW` (differential). Transition `wpc` to
  `WtrSignaling`.
- `W_Signal`: fence; load `memAwaiterState`; apply the §8.2.1 reader-
  caught-up skip (`memExaminedPublished ≥ writerBytesWritten`, bypassed
  on `writerCompleted`); CAS `Armed → Signaled` and call SetResult.
- `W_Complete`: release-store WriterCompletionState = 2 and
  (optionally) WriterException. Signal unconditionally (the §8.2.1
  skip's `~writerCompleted` guard ensures Complete always wakes the
  parked reader).

**Reader.**
- `R_ReadAsync`: sync-path (TryReadCore). If data or completion, return
  sync result.
- `R_Arm`: MRVTS.Reset. CAS Idle→Armed. Re-check. If data/completion
  visible, CAS Armed→Idle. Otherwise register cancel callback, return VT.
- `R_ResumeContinuation`: modeled as a separate actor (ThreadPool
  dispatcher): when `MRVTS.Succeeded` and there's a pending reader, fire
  the continuation. This calls `R_GetResult`.
- `R_GetResult`: MRVTS.GetResult. Clear ReaderAwaiterState to Idle.
  If IsCanceled, return canceled. Else re-run TryReadCore; if data or
  completion found (with exception check — §10.4), return or throw; else
  throw spurious-wake.
- `R_Complete`: set WriterCompletionState analogue on reader side (covered
  in reverse-direction for writer awaiter, see Backpressure.tla).
- `R_CancelPendingRead`: CAS Armed→Signaled with IsCanceled=true.

**Cancel callback.**
- `C_Fire`: fires nondeterministically (when CT is triggered externally
  for the arm period). CAS Armed→Signaled with IsCanceled=true.

### Invariants

- `TypeOK`.
- `AtMostOneResultPerVersion` (implied by MRVTS axioms but cross-checked).
- `NoStuckArmed`: eventually, Armed → non-Armed (temporal, under fairness).
- `NoSpuriousWake`: if `R_GetResult` is invoked with token=V, the signal
  delivered for V must correspond to either (a) data past
  `readerExaminedPosition` actually being visible, (b) completion state
  being 2, or (c) IsCanceled=true. Throwing the "spurious wake"
  InvalidOperationException is a model-level invariant violation (we
  should never reach that branch in correct execution). The §8.2.1
  reader-caught-up skip in `W_Signal` is the protocol feature that
  makes this invariant hold; the `EnableReaderCaughtUpCheck=FALSE`
  differential produces a counterexample.
- `ExceptionPropagation`: if writer completes with exception and reader
  drains all data, reader's next `R_GetResult` throws that exception
  (bridge contract from §10.4).

### Temporal

- `EventuallyUnArmed`.
- `EventuallyResumed`.
- `EventualProgress`.

### Differential experiments

- `EnableWriterFence = FALSE`: drop the writer-side fence between
  `W_Publish`'s release-store drain and `W_Signal`'s AwaiterState
  load. Expect `NoSpuriousWake` to fail (the writer's signal-check
  observes an Armed reader while the writer's own stores remain
  undrained).
- `EnableReaderCaughtUpCheck = FALSE`: `W_Signal` skips the §8.2.1
  reader-caught-up check. Expect `NoSpuriousWake` to fail — this is
  the race uncovered by Phase 3 stress testing, where the reader's
  sync-path consumes a publish and then arms, and the writer's
  delayed signal for that same publish is stale.
- Skip the §7.3 step-2 snapshot (compute examined after retire) in
  tandem with pool re-rent in Publication.tla. (Cross-module;
  encoded in Publication.tla alone.)

---

## `Backpressure.tla` — flush-side awaiter with hysteresis

### Scope

Covers §6.3 (FlushAsync backpressure path), §8.4 (writer arms flush
awaiter), §8.5 (resume-threshold hysteresis).

Symmetric to `Awaiter.tla` but for the writer's `_flushAwaiter`.

### Key modeling decisions

- **Numerical thresholds are the point.** Pause and Resume are constants;
  `outstanding = bytesWritten - bytesRead`. Hysteresis is enforced in
  the reader's signaling path: signal writer only when
  `outstanding < Resume`, not just `< Pause`.
- **Reader.Complete bypasses hysteresis.** As clarified during
  implementation: when the reader completes, the writer should wake
  regardless of `outstanding`. Explicitly modeled as a conditional
  bypass in `R_MaybeSignalWriter`.
- **FlushResult exception propagation.** When the writer's `GetResult`
  returns `IsCompleted=true`, the reader-side exception is thrown
  (§10.4 symmetric).

### Variables (abstract; overlap with Awaiter.tla)

```
memWriterAwaiterState, memBytesWritten, memBytesRead,
memReaderCompletionState, memReaderException,
writer_mrvts_*,
producedOutstanding, consumedTotal,
wpc, rpc, cpc
```

### Actions

- `W_Produce`: bytesWritten += chunkSize. Publish.
- `W_Flush`: check readerDone. Check backpressure. If outstanding ≥ Pause
  and not done, enter `ArmFlushAwaiter`.
- `W_ArmFlush`: MRVTS.Reset. CAS Idle→Armed. Re-check. Etc.
- `W_GetResultFlush`: MRVTS.GetResult. If `IsCompleted && ReaderException`,
  throw.
- `R_Consume`: bytesRead += chunkSize. Release-store.
- `R_MaybeSignalWriter`: fence. Load WriterAwaiterState. If Armed, check
  `readerDone || outstanding < Resume`. If signal condition met, CAS
  Armed→Signaled and MRVTS.SetResult(FlushResult(IsCompleted=readerDone)).
- `R_Complete`: release-store ReaderCompletionState = 2 and (optionally)
  ReaderException. Then `R_MaybeSignalWriter` (hysteresis bypassed due to
  readerDone path).

### Invariants

- `HysteresisCorrectness`: the writer is not signaled when
  `outstanding ∈ [Resume, Pause)` unless the reader has completed.
- `NoLostWakeupOnDrain`: if writer parks with `outstanding ≥ Pause` and
  reader drains to `outstanding < Resume`, the writer eventually wakes
  (temporal).
- `NoLostWakeupOnReaderComplete`: if writer parks and reader completes,
  the writer eventually wakes regardless of `outstanding`.

### Temporal

- `EventualWriterProgress`.

### Differential experiments

- Remove hysteresis bypass in `R_MaybeSignalWriter` when
  `readerDone=true`. Expect `NoLostWakeupOnReaderComplete` to fail.
- Weaken the hysteresis to `outstanding < Pause` (instead of `< Resume`).
  Expect `HysteresisCorrectness` to fail — writer woken prematurely,
  wake/park thrash observable.

---

## In-scope vs. out-of-scope

### In-scope (will be verified)

- **Publication ordering** (§6.4.2): three release-stores in the correct
  order, load-bearing.
- **Retirement safety** (§10.7): writer-side + reader-side lifetime
  requirements. Pool re-rent invalidating stale field reads is modeled.
- **Awaiter handshake** (§8.2, §8.3, §8.6): no lost wakeups, no double
  SetResult, cancellation correctness.
- **Backpressure** (§6.3, §8.4, §8.5): hysteresis correctness, bypass on
  reader completion.
- **Exception propagation** (§10.4): both directions.
- **MRVTS contract adherence** (§8.7): our usage of MRVTS respects its
  axioms.
- **First-read head init** (§7.1 step 4).
- **Traversal invariant** (§7.2).
- **SPSC invariant enforcement at action level**: only one writer action
  at a time, only one reader action at a time (modeled by PC separation).

### Out-of-scope (documented, not verified)

- **Internals of MRVTS, ArrayPool, CancellationToken, ThreadPool.** Axioms,
  not re-derived.
- **Reset/Dispose state transitions** (§10.5, §10.6): require "no
  operations in flight," which is a precondition the model does not
  check; the spec assigns this to the caller.
- **Finalizer behavior** (§10.6): modeling finalizer ordering against the
  GC is out of scope; rely on `GC.SuppressFinalize` + the stop-the-world
  barrier argument in the spec.
- **Cache-line layout** (§4.2): a performance concern, not correctness.
- **`ReadOnlySequence<byte>` external enumerator behavior**: the consumer
  walks `base.Next`; contract is that they do so only within the returned
  `ReadResult.Buffer`. Not modeled.
- **Scheduler semantics beyond "continuation eventually runs"**
  (§8.8): `PipeScheduler.Schedule` configurations are not modeled in
  detail.
- **Native-memory `MemoryPool<byte>` implementations** (§13): axiomatized
  the same as array-backed; specific quirks not modeled.
- **Performance targets** (§12).

### Declared gaps (acknowledged, not planned for the redo)

- **Real `Segment.Initialize` implementation details.** Our model says
  "Initialize overwrites all fields as a single action." Actual C#
  writes fields sequentially. If there's a race concern specifically
  about *partial* Initialize (writer has written RunningIndex but not
  WrittenLength when reader reads), the model doesn't catch it — we
  treat Initialize as atomic. Mitigated in implementation by the
  snapshot-before-retire ordering; the reader only reads fields of a
  segment while its holder refcount is held (not yet retired), so
  Initialize never races with reads.

---

## Verification approach

### TLC configuration

Each module has its own `.cfg`. Bounds are small to keep state space
tractable:

- `Publication.tla`: `MaxSegments = 3`, `MaxChainLen = 2`,
  `MaxAdvanceCycles = 3`. Expected: millions of states, seconds to
  minutes.
- `Awaiter.tla`: `MaxProduces = 2`, `MaxArms = 2`, `MaxCancels = 1`.
  Same as the old model; tens of thousands of states.
- `Backpressure.tla`: small integer thresholds (`Pause = 4`, `Resume = 2`,
  `ProduceChunk = 1`). `MaxProduces = 4`. Keeps numeric state space small.

### Differential experiments

Each module should include a boolean constant (`ENABLE_FENCE`,
`ENABLE_ORDERED_SPLICE`, `ENABLE_HYSTERESIS_BYPASS`, `ENABLE_EXAMINED_SNAPSHOT`)
that can flip the load-bearing behavior. The default config has them all
`TRUE` (correct); alternate configs flip each one and expect a specific
invariant or temporal property to fail. This matches the pattern
established in the old models and is the single most important
verification technique — it proves the design choices are load-bearing,
not superstitious.

### When TLC finds a counterexample

1. If the counterexample is in a declared in-scope area, it's a model bug
   *or* a spec bug. Fix the model or the spec; re-run.
2. If the counterexample is in an out-of-scope area (e.g., pool
   mechanics we axiomatized), extend the scope if the bug matters, or
   document the gap.
3. Differential-experiment counterexamples are expected; they validate
   the experiment.

---

## Dependencies between modules

```
    MRVTS.tla
      ↑     ↑
      |     |
  Awaiter.tla   Backpressure.tla
      |     |
      ↓     ↓
  (Publication.tla is independent — no MRVTS usage;
   models chain structure and retirement without awaiter)
```

`Publication.tla` doesn't need MRVTS because it doesn't model waits.
Awaiter and Backpressure share MRVTS.tla. The three non-axiom modules
can be verified independently.

The three's *correctness assumptions* compose: if Publication.tla verifies
publication/retirement under a memory model, and Awaiter.tla verifies
awaiter handshake assuming data publication happens correctly (via the
`tailPublished` abstraction), then the composition should cover the real
protocol. The composition is argued informally, not mechanically verified
(cross-module composition in TLA+ is hard).

---

## Relationship to the (deleted) earlier TLA+ work

The previous `AwaiterHandshake.tla` and `Publication.tla` modules verified
narrower scopes that didn't capture segment pool mutability or MRVTS's
specific semantics. Their key techniques — TSO store-buffer encoding,
differential fence experiments, weak fairness for liveness — transfer
directly. The new modules will reuse those techniques while expanding the
covered abstraction.

The old modules were deleted along with the first implementation attempt
(see the accompanying commit) to avoid biasing the next session's work
toward "transform the old model" rather than "build from the design."
Git history preserves both.
