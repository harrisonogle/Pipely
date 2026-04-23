# SpscPipe TLA+ verification

Formal models of the `SpscPipe` specification in `docs/spscpipe-spec.md`.

## Status

**Verification-led redo in progress.** The initial TLA+ modules and C#
implementation were removed after stress testing revealed that the modeled
scope was narrower than the spec's actual abstraction (specifically: the
old models treated segments as immutable values identified by ID, whereas
the spec and implementation treat them as mutable pooled objects with
observable field reinitialization).

The next session's work is guided by `design.md`, which specifies the
module breakdown, variables, actions, invariants, and axiomatized
primitives for the expanded scope.

## Files

| File | Purpose |
|---|---|
| `design.md` | Design document for the expanded verification scope. Prescriptive for the next session's TLA+ work. |
| *(future)* `MRVTS.tla` | Axiom module: MRVTS semantics. |
| *(future)* `Publication.tla` | §4.1, §6.1–§6.4, §7.1–§7.3, §9, §10.7 — publication, retirement, segment pool. |
| *(future)* `Awaiter.tla` | §6.6, §7.4, §8.1–§8.6, §10.4 — awaiter handshake, cancellation, completion, exception propagation. |
| *(future)* `Backpressure.tla` | §6.3, §8.4, §8.5 — flush-side awaiter with hysteresis. |

## History

The earlier TLA+ modules (`AwaiterHandshake.tla`, `Publication.tla`) are
preserved in git history. See `docs/spscpipe-spec.md` for the spec and
the `design.md` for the plan going forward.

## Running TLC (once modules are implemented)

```bash
cd spec/tla
tlc Publication.tla
tlc Awaiter.tla
tlc Backpressure.tla
```

Each module will include a default `.cfg` (correct protocol) and one or
more differential-experiment `.cfg`s that flip a load-bearing behavior
(fence, ordering, hysteresis) and expect TLC to produce a counterexample
to a specific property.
