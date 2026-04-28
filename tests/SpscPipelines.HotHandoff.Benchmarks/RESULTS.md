# HotHandoff Dispatcher Benchmark Results

**Compared:** `tp-default` (no `ContinuationDispatcher` set; SpscPipe uses
`ThreadPoolContinuationDispatcher.Instance`) vs `hot-handoff`
(`SpscPipelines.HotHandoff.HotHandoffContinuationDispatcher`).

**Spec reference:** `docs/superpowers/specs/2026-04-27-hot-handoff-dispatcher-design.md` §8.

## Design-completion criterion

> The implementation is finalized when measurements either justify a tuned
> configuration that beats `tp-default` at the percentiles that matter
> (P50, P90, P99) under reasonable CPU cost, or demonstrate that no
> reasonable configuration does. Each iteration of the implementation lands
> the change with the measurement that justified it.

The architecture in spec §3 is fixed. The tunable surface in spec §9
(`SpinIterations`, backoff body, CPU pinning, mailbox depth) is open. Every
change to a tuning constant in source must be committed alongside the
measurement that drove it.

## Methodology

- Latency: `dotnet run -c Release --project tests/SpscPipelines.HotHandoff.Benchmarks -- latency --count 100000 --size 256 --trials 3 --warmup 1`
- Throughput: `dotnet run -c Release --project tests/SpscPipelines.HotHandoff.Benchmarks -- --filter '*'`
- Three latency trials per recorded run; warmup trial not recorded.
- Hardware/build details captured at the top of each results section.
- The hot-handoff worker thread sits at ~100% on its core during the busy-spin
  loop. Latency wins must be read against this CPU cost.
- **Throughput-benchmark caveat:** Each BDN iteration constructs and disposes a fresh `HotHandoffContinuationDispatcher`, so iteration time includes thread-startup (~30-100 µs on Linux) and `Thread.Join` cost. The throughput number is therefore a conservative lower bound for steady-state hot-handoff use, not a steady-state ceiling. The latency comparison (which constructs one dispatcher per recorded trial, not per message) is unaffected.

## Starting tunables

Recorded here verbatim so that any tuning iteration is auditable against
the prior baseline.

| Tunable | Starting value | Notes |
|---|---|---|
| `SpinIterations` | 10 | `private const int` in `HotHandoffContinuationDispatcher.cs` |
| Backoff body | `Thread.SpinWait(SpinIterations)` | The entire idle-loop body |
| CPU pinning | none | Worker thread is unpinned in the starting configuration |
| Mailbox depth | 1 | Single-slot with TP overflow on contention |

## Run 1 — starting configuration

**Date:** _(record the date of the run)_
**Hardware:** _(record CPU, RAM, OS)_
**Build:** _(record .NET SDK / runtime versions, GC mode)_
**Commit:** _(record the git SHA at run time)_
**Tunables:** as in "Starting tunables" above; no overrides.

### Latency (ns)

_(Paste the comparison tables from `dotnet run ... latency` here, one block per trial.)_

### Throughput

_(Paste the BDN summary table from `dotnet run ... throughput` here.)_

### Observations

_(Brief honest read of the data. Did hot-handoff win at P50/P90/P99? At what
CPU cost? Any anomalies? This section commits to a numerical conclusion;
subsequent runs document tuning iterations.)_

## Subsequent runs

Each tuning iteration adds a new section ("Run 2 — pinned worker", "Run 3 —
SpinIterations 50", etc.) capturing the same fields. The implementation is
not finalized until either:

- A configuration is justified by the data and locked in source (with the
  measurement linked from the source comment), or
- Reasonable variants are exhausted and the dispatcher does not earn its
  complexity — in which case this project is removed from the solution.
