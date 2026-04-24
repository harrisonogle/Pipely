# SpscPipe TLA+ verification

Formal models of the `SpscPipe` specification in `docs/spscpipe-spec.md`.

## Status

Phase 1 (TLA+ verification) complete. Four modules plus one axiom split:
`MRVTS.tla` (operators), `MRVTSStandalone.tla` (standalone driver),
`Publication.tla`, `Awaiter.tla`, `Backpressure.tla`. Each has a default
config that passes all invariants and temporal properties, and one or
more differential-experiment configs that flip a load-bearing behaviour
and produce the expected counterexample.

## Files

| File | Purpose |
|---|---|
| `design.md` | Design document for the expanded verification scope. |
| `MRVTS.tla` | Pure operators (no state) for `ManualResetValueTaskSourceCore<T>` (spec §8.7). Extended by Awaiter and Backpressure. |
| `MRVTSStandalone.tla` | Standalone state machine verifying MRVTS operator consistency. |
| `Publication.tla` | §4.1, §6.1–§6.4, §7.1–§7.3, §9, §10.7 — publication, retirement, segment pool (mutable objects), TSO store buffer. |
| `Awaiter.tla` | §6.6, §7.4, §8.1–§8.6, §10.4 — reader-side awaiter handshake, cancellation, exception propagation. |
| `Backpressure.tla` | §6.3, §8.4, §8.5 — writer-side flush awaiter with hysteresis + reader-complete bypass. |

## Results

| Config | States | Outcome |
|---|---|---|
| `MRVTS.cfg` | 640 | All invariants hold |
| `MRVTS_ResetRace.cfg` | 316 | `ResetDiscipline` violated (expected) |
| `Publication.cfg` | 10,417 | All invariants hold |
| `Publication_SpliceReorder.cfg` | 114 | `ChainConsistent` violated (expected) |
| `Publication_ExaminedAfterRetire.cfg` | 6,729 | `NoStaleFieldReadAfterRetire` violated (expected) |
| `Awaiter.cfg` | 2,817 | All invariants + temporal hold |
| `Awaiter_NoFence.cfg` | 625 | `NoSpuriousWake` violated (expected) |
| `Backpressure.cfg` | 826 | All invariants + temporal hold |
| `Backpressure_NoBypass.cfg` | 704 | `NoLostWakeupOnReaderComplete` violated (expected, temporal) |
| `Backpressure_WeakHysteresis.cfg` | 503 | `HysteresisCorrectness` violated (expected) |

## Running TLC

```bash
cd spec/tla
tlc -config MRVTS.cfg MRVTSStandalone.tla
tlc -config Publication.cfg Publication.tla
tlc -config Awaiter.cfg Awaiter.tla
tlc -config Backpressure.cfg Backpressure.tla
```

Each default config takes seconds.  The differential-experiment configs
produce their expected counterexamples in milliseconds-to-seconds.

## History

The earlier TLA+ modules are preserved in git history (see commits prior
to `648d534`, the verification-led redo). The old modules were deleted
along with the first implementation attempt to avoid biasing this
session's work toward "transform the old model" rather than "build from
the design."
