# SpscPipe TLA+ models

Formal models of the `SpscPipe` specification in `docs/spscpipe-spec.md`.
Scope is intentionally narrow: model only the parts of the spec where
reordering-sensitive reasoning is load-bearing (see the main spec's §5
and the fence-placement arguments in §6.4.2, §8, §10.7).

## Modules

| File | Scope | Spec sections |
|---|---|---|
| `AwaiterHandshake.tla` | Reader-awaiter handshake, fence + double-check, cancellation | §8.2, §8.3, §8.6, §6.6 |
| `Publication.tla` | Splice ordering, reader traversal, tail retirement | §6.4.1, §6.4.2, §7.1, §7.2, §10.7 |

The writer-awaiter side (§8.4) is structurally symmetric to §8.2/§8.3 and
is covered by inspection rather than a dedicated model.

## Memory model

A TSO-like model with per-thread release-store buffers:

- Release-stores (`Volatile.Write`) enter the issuing thread's buffer.
- Acquire-loads (`Volatile.Read`) read from globally committed memory.
- Buffers drain nondeterministically via a `DrainW` action. Weak fairness
  on `DrainW` ensures pending stores eventually propagate.
- Full fences (`Interlocked.*`, `MemoryBarrier`) are modeled by guarding
  the fenced action on `bufW = <<>>`; the thread cannot proceed until
  the scheduler has drained its buffer.
- CAS operates atomically on committed memory and is a full fence.

This captures StoreLoad reordering, which is the only reordering the §8
handshake depends on.  It is sufficient for AArch64 because the spec does
not rely on non-multi-copy-atomic behavior (POWER-style ordering) — every
shared field has a single writer.

## Running TLC

Default configuration (correct model, writer fence enabled):

```bash
cd spec/tla
tlc AwaiterHandshake.tla
```

Expected output: `Model checking completed. No error has been found.`

## Fence-removal experiment

A dedicated config runs the model with the writer's fence elided:

```bash
tlc -config AwaiterHandshake_NoFence.cfg AwaiterHandshake.tla
```

Expected: TLC produces a counterexample to the temporal properties
(`EventuallyUnArmed` / `EventuallyResumed` / `EventualProgress`) showing
the classic lost-wakeup race.  Condensed:

1. Writer publishes a chunk — release-store `tailPub` enters `bufW`.
2. Writer executes `MaybeSignalReaderAwaiter` **without** the fence:
   acquire-loads `readerAwaiter` from committed memory while its own
   `tailPub` store is still buffered.  Reader hadn't yet armed, so it
   reads `Idle` and does not signal.
3. Reader then runs: acquire-loads `tailPub` — reads the *previous*
   committed value (writer's new store is still in `bufW`).  No data
   visible.
4. Reader CASes `readerAwaiter: Idle -> Armed` and parks.
5. `DrainW` fires, propagating the new `tailPub` value to committed
   memory.  Too late — the writer has already moved on.
6. Reader is parked, awaiter is `Armed`, data is visible in memory, and
   no further signaler exists.  Liveness violated.

This confirms §8.2's `Interlocked.MemoryBarrier()` is load-bearing on
weak-memory hardware (ARM64), not superstition — the fence is what
orders the writer's `tailPub` store globally before its subsequent read
of `readerAwaiter`.

## Publication module

`Publication.tla` models `SpliceUnpublishedChain` (§6.4.2), reader
traversal (§7.1/§7.2), and the tail-retirement rule (§10.7).

### Run

```bash
tlc Publication.tla                                            # default — ordered splice, expect pass
tlc -config Publication_Reversed.cfg Publication.tla           # reversed splice, expect counterexample
```

### Key invariants

- **`ChainConsistent`** — if `state.Tail` is non-null, `state.Head` is
  non-null and `state.Tail` is reachable from `state.Head` via committed
  `seg.Next` pointers.  Captures §6.4.2's "CRITICAL ORDERING" requirement.
- **`NoRetiredAsPrevTail`** — at every splice-entry point, the writer's
  view of `state.Tail` (which will be used as `prevTail`) is not a
  retired segment.  Captures §10.7.
- **`BWPNotAheadOfTail`** — `BytesWrittenPublished` never exceeds the
  end position of the committed tail (FIFO store-buffer property).

### Splice-ordering experiment

Flipping `ENABLE_ORDERED_SPLICE` to `FALSE` reverses the order of the
splice's first two release-stores so that `state.Tail` is published
before `state.Head` (first splice) or `prevTail.Next` (subsequent).

TLC produces a 4-state counterexample:

1. Writer allocates segment 1 (writer-local, not reader-visible).
2. Writer's Part1 under reversed semantics appends `[tail, S1]` to the
   store buffer.
3. `DrainW` propagates the tail store: `memTail = S1`, but `memHead = 0`
   (nothing published yet).  `ChainConsistent` is violated — reader
   observing `memTail = S1` has no head from which to reach it.

This is exactly §6.4.2's "reversing the writer-side order would allow
the reader to observe `Tail = chainTail` while `Head` is still null".

### Modeled / not modeled

The model covers:
- Unpublished-chain construction with plain intra-chain `Next` links
  (§6.4.1).
- The three release-stores of the splice with per-store drain.
- Reader first-read head initialization (§7.1 step 4).
- Reader traversal and tail-retirement decision (§10.7).

Deliberately not modeled:
- Refcounted `BufferHolder` lifecycle (§6.5).  The spec's own proof is
  an SC-equivalent argument that TLC would not find anything new in.
- Byte-level content of segments.  `segWrittenLen` is fixed at 1 per
  segment; `segRunningIdx` is the cumulative count.  Preserves all the
  interesting ordering and reachability behavior while keeping the
  state space small.
- Actual `ReadOnlySequence<byte>` construction.  The reader's consume
  and retire actions abstract this.

## Bounds

Defaults (`MaxProduces = 2`, `MaxArms = 2`, `MaxCancels = 1`) are small
enough to explore in seconds.  The race surfaces at these bounds; larger
values mostly increase state-space size without exposing new behaviors.
See the spec's §10 for why the small-model hypothesis is adequate here.

## Known model limitations

- **`ValueTaskSource.Version` reuse across cycles is not modeled.**
  `setResults` is appended per arm cycle keyed by `parkedVersion`; signals
  cannot be delivered to a stale arm cycle because the CAS that transitions
  `Armed -> Signaled` is atomic with the result-append.  A bug in the
  actual `ManualResetValueTaskSourceCore` version logic would not be caught
  here; it must be covered by inspection or a separate model.
- **Reader and cancel have no store buffers** because they never issue
  release-stores in this protocol (only CAS).  The reader's Step B
  `MemoryBarrier` is redundant by the spec's own footnote (Step A CAS is
  already a full fence), so it is not parameterized.
