# SpscPipe follow-ups

Items identified at the end of the initial verification-led redo that
are out of scope for that session but worth tracking.

## 1. Scheduler options are declared but not consumed

`SpscPipeOptions.ReaderScheduler`, `WriterScheduler`, and
`UseSynchronizationContext` are part of the public API (matching BCL
`PipeOptions`) but the implementation never reads them. MRVTS is
constructed with `RunContinuationsAsynchronously = true` and
continuations always dispatch on the default ThreadPool.

A caller that sets `ReaderScheduler = PipeScheduler.Inline` expecting
inline continuation dispatch gets ThreadPool dispatch instead. That's
a silent behavior divergence from BCL `Pipe`.

Two acceptable resolutions:
- **Implement**: route MRVTS continuations through
  `options.ReaderScheduler`/`options.WriterScheduler`. Likely via a
  custom `IValueTaskSource<T>` wrapper that intercepts `OnCompleted`
  and schedules the continuation on the caller-specified
  `PipeScheduler`. Mirrors BCL behavior.
- **Reject**: throw in the constructor if a non-`ThreadPool` scheduler
  is specified, with a clear message that v1 only supports ThreadPool
  dispatch. Surfaces the divergence rather than hiding it.

Spec §8.8 currently says "matches BCL behavior; no new design." The
impl doesn't.

## 2. Unit-test coverage gaps

- No test asserts that `PauseWriterThreshold = 0` prevents parking.
  Add a test that fills well past a "normal" pause threshold's worth
  of data under `Pause=0` and observes that every `FlushAsync`
  completes synchronously.
- No test asserts that `new SpscPipeOptions { PauseWriterThreshold = -1 }`
  throws `ArgumentOutOfRangeException`. Same for
  `ResumeWriterThreshold = -1`.
- No test asserts the `Resume=0 → 1` coercion is observable on the
  `SpscPipeOptions` instance (`opts.ResumeWriterThreshold == 1` after
  `new SpscPipeOptions { ResumeWriterThreshold = 0 }`).

## 3. ARM64 CI wiring

x86_64 stress runs locally and is covered by `SpscPipe.Stress`. The
protocol uses release/acquire + full fences that are load-bearing on
weaker memory models (ARM64), but those paths haven't been exercised
on real ARM64 hardware yet. Plan:

- Add a CI job (GitHub Actions, `runs-on: ubuntu-latest-arm64` or
  equivalent) that runs the stress harness at 100k+ iterations.
- Seed with the current failing-prone configurations found on x86
  (Resume=0-coerced-to-1, small segments, high chaos budget).
- If ARM64 surfaces a counterexample x86 misses, it is almost
  certainly in the fence placement — the §8.3 MemoryBarrier and the
  §8.2 MemoryBarrier are the first places to audit.

## 4. Cross-module TLA+ composition gaps

`spec/tla/design.md` notes:

> The composition is argued informally, not mechanically verified
> (cross-module composition in TLA+ is hard).

Phase 3 stress found one such composition gap: `Awaiter.tla` fused
`W_Publish` and `W_Signal` into a single atomic action, hiding the
race that §8.2.1 closes. The gap was repaired by un-fusing + adding
`memExaminedPublished`.

Likely-analogous places worth auditing:

- **Publication.tla ↔ Awaiter.tla.** The abstract `memTailPublished`
  counter in `Awaiter.tla` doesn't model segment-chain retirement +
  pool re-rent. A scenario where the reader retires a segment the
  writer is about to signal on could produce a new race. Stress
  hasn't surfaced one, but the model doesn't rule it out either.
- **Backpressure.tla ↔ Publication.tla.** `Backpressure.tla` uses
  `writerBytesWritten` / `readerBytesRead` as scalar counters without
  modeling segment-level publication. Analogous to the Awaiter.tla
  gap: the real writer's bytesWritten advances across multiple
  release-stores (Tail, BWP) that can interleave with reader-side
  observations.
- **The assumption that `Backpressure.tla` and `Awaiter.tla` can be
  verified independently** — they share the awaiter-state machine
  and MRVTS instance semantics, and both modify reader-visible
  published counters. A composed model would be large but possibly
  tractable with aggressive state-space pruning.

No stress counterexamples currently open; this is proactive hardening.

## 5. Test code quality (xUnit1031 warnings)

Three test methods use blocking `.Result` / `.Wait()` on tasks. xUnit
flags them as deadlock risks (they aren't in these specific tests,
but the warnings should be resolved for cleanliness). Convert to
`async Task` test methods.

- `HappyPathTests` (2 sites)
- `LifecycleTests` (1 site)

## 6. Spec prose polish

The §8.2.1 rationale in `docs/spscpipe-spec.md` still references the
`outstanding < Resume` signal condition as if `Resume = 0` were a
user-reachable value. With the §3 coercion (`Resume = 0 → 1`) landed
in `SpscPipeOptions`, that case no longer reaches pipe internals.
Minor rewording, not a correctness issue.

---

## Reference: verified reproducers

Preserving for future regression-testing.

### §8.2.1 spurious-wake race (pre-fix)

To observe the race that commit `c6fe04f` fixes, revert that commit
and run:

```bash
dotnet run --project tests/SpscPipe.Stress/SpscPipe.Stress.csproj -c Release \
    -- --iterations 100000 --seed 2 --timeout 30
```

Prior to the fix this reproduced within the first ~20 seconds
(observed at iteration 19,752 and 63,307 on different runs — timing-
dependent but reliable). The stress harness in commit `53fbf16`
included diagnostic instrumentation (`DiagLog`) that captured the
writer-signal / reader-park / GetResult-spurious sequence; that
instrumentation was removed in commit `c4f12de` but is recoverable
from git if you want the timeline again.

The specific stress config that tripped it: random MinimumSegmentSize
in [4,63], random PauseWriterThreshold in [8,255], random
ResumeWriterThreshold in [1,Pause], random totalBytes in [0,4095],
chaos budget [0,15]. No single "minimal" reproducer — the race is
inherent to the protocol scheduling.

### Differential TLA+ configs that demonstrate load-bearing behaviour

Each produces its documented counterexample in <1s:

```bash
cd spec/tla
tlc -config MRVTS_ResetRace.cfg MRVTSStandalone.tla          # ResetDiscipline
tlc -config Publication_SpliceReorder.cfg Publication.tla    # ChainConsistent
tlc -config Publication_ExaminedAfterRetire.cfg Publication.tla   # NoStaleFieldReadAfterRetire
tlc -config Awaiter_NoFence.cfg Awaiter.tla                  # NoSpuriousWake (fence)
tlc -config Awaiter_NoCaughtUpCheck.cfg Awaiter.tla          # NoSpuriousWake (§8.2.1)
tlc -config Backpressure_NoBypass.cfg Backpressure.tla       # NoLostWakeupOnReaderComplete
tlc -config Backpressure_WeakHysteresis.cfg Backpressure.tla # HysteresisCorrectness
```

## TLA+ modelling notes

Gotchas that bit this project and would bite anyone reusing the
pattern.

### Module composition with `EXTENDS`

`EXTENDS M` pulls in *all* of M's operators *and* `VARIABLES`.  If
M has a standalone state machine, extenders inherit those variables
and must include them in their own `vars` tuple and `Init` / `Next`
relations, or TLC fails with "variable not assigned."

Solution: split M into an operators-only module and a separate
standalone-driver module that `EXTENDS` it.  `Awaiter.tla` and
`Backpressure.tla` EXTEND `MRVTS.tla` (pure operators);
`MRVTSStandalone.tla` is the separate driver that EXTENDS `MRVTS.tla`.

### Quantifier binding order

```tla
\E x \in S, y \in T : P(x, y)   (* y's set cannot reference x *)
```

vs.

```tla
\E x \in S : \E y \in f(x) : P(x, y)   (* y's set can reference x *)
```

TLC's SANY parser does not propagate the leftmost binding into
subsequent set expressions in the comma form.  Use nested `\E`.

### `Nat` in schema records

`[field: Nat, ...]` works at the schema level for `\in`-checks if TLC
can decide membership without enumerating the set.  But if a
`CHOOSE` or similar primitive tries to materialize the set, TLC errs.
Use bounded ranges (`field: 0..MaxN`) in any schema TLC might
enumerate.

### State-space blow-up from atomic vs. buffered releases

Adding a reader-side store buffer (`bufR`) to `Awaiter.tla` increased
the verified state space by ~100× for no added coverage of the race
under study.  Atomic releases are sound where a fence on the writer
side (or an arm-CAS fence on the reader side) orders them before
subsequent loads that establish happens-before.  Documented in
`Awaiter.tla`'s header; keep that decision if you extend the model.

### Plain writes + pooled objects

When modelling a pooled mutable object (e.g., `Segment`), plain
writes to its fields from the writer must go through the same FIFO
store buffer as release-stores to observable fields that point at it.
Omitting this produces the spurious "stale tail with new fields"
counterexample that `Publication.tla` documents in comments on
`W_AllocSegment`.
