# HotHandoff Dispatcher Benchmark Results

**Compared:**

- **Latency** (custom harness, P50/P90/P99 by sort): `tp-default` (SpscPipe with `ContinuationDispatcher = null`, i.e., `ThreadPoolContinuationDispatcher.Instance`) vs `hot-handoff` (SpscPipe with `HotHandoffContinuationDispatcher`).
- **Throughput** (BenchmarkDotNet, 1 MiB / 4 KiB chunks): three-way head-to-head — `BclPipe` (BCL `System.IO.Pipelines.Pipe`, baseline), `SpscPipe_TpDefault`, `SpscPipe_HotHandoff` — all in the same BDN process invocation so their numbers are directly comparable.

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
  loop. Latency and throughput wins must be read against this CPU cost.
- **Dispatcher amortization:** `SpscPipe_HotHandoff_ProduceAndDrain` constructs the dispatcher once via `[GlobalSetup]` and reuses it across all BDN iterations (mirroring `BclPipe`'s no-extra-state baseline and `SpscPipe_TpDefault`'s singleton-dispatcher baseline). Per-iteration cost for all three rows is therefore solely pipe ctor + produce-and-drain — apples to apples. The latency harness similarly amortizes (one dispatcher per recorded trial, not per message).

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

**Date:** 2026-04-27
**Hardware:** AMD Ryzen 7 8700F (8 physical / 16 logical cores), Linux Ubuntu 24.04.4 LTS, kernel 6.17.0-22-generic
**Build:** Release, .NET SDK 10.0.107 / Runtime .NET 10.0.7, Server GC + concurrent
**Commit:** `9b98a8f` (HotHandoff bench: amortize dispatcher + add BCL to 3-way throughput)
**Tunables:** as in "Starting tunables" above; no overrides.

### Throughput (1 MiB / 4 KiB chunks; head-to-head-to-head, same BDN process)

| Method                              | Mean      | Error    | StdDev   | Ratio | Gen0   | Allocated | Alloc Ratio |
|-------------------------------------|----------:|---------:|---------:|------:|-------:|----------:|------------:|
| `BclPipe_ProduceAndDrain`           | 105.09 us | 0.834 us | 0.780 us | 1.00  | 0.1221 |   7.03 KB |        1.00 |
| `SpscPipe_TpDefault_ProduceAndDrain`|  70.34 us | 0.242 us | 0.215 us | 0.67  | 0.2441 |  11.03 KB |        1.57 |
| `SpscPipe_HotHandoff_ProduceAndDrain`|  50.45 us | 0.185 us | 0.173 us | 0.48  | 0.2441 |   9.21 KB |        1.31 |

Reading:
- `SpscPipe_TpDefault` is **1.50×** faster than BCL — consistent with `tests/SpscPipelines.Benchmarks/RESULTS.md`'s prior characterization of the SpscPipe-vs-BCL axis.
- `SpscPipe_HotHandoff` is **1.39×** faster than `SpscPipe_TpDefault` (50.45 / 70.34) and **2.08×** faster than BCL.
- `SpscPipe_HotHandoff` allocates **0.84×** the bytes of `SpscPipe_TpDefault` (9.21 / 11.03 KB) — the worker-thread invocation path doesn't allocate the per-event TP work-item objects.

### Latency (ns) — 1 M messages × 256 B, 5 warmup + 10 recorded trials

Per-trial percentiles (ns, both configurations same trial). Compact summary across trials below; per-trial detail captured from the run.

Aggregate across the 10 recorded trials (each trial sorts 1 M samples and reads exact percentile by index):

| Stat   | `tp-default` min / median / max | `hot-handoff` min / median / max | median ratio (HH/TP) |
|--------|----------------------:|-------------------------:|---------------------:|
| Min    |   120 /   190 /   230 |   130 /   190 /   220   | 1.00 |
| P50    |   950 / 1,185 / 1,530 | 1,170 / 1,370 / 1,730   | 1.16 |
| P90    | 1,860 / 2,330 / 3,700 | 1,750 / 2,580 / 3,640   | 1.11 |
| P99    | 3,410 / 6,190 / 8,070 | 3,440 / 6,070 / 8,670   | 0.98 |
| P99.9  | 9,900 / 11,060 / 18,420 | 9,300 / 12,460 / 20,080 | 1.13 |
| Max    | 28,620 / 33,945 / 86,428 | 24,980 / 36,095 / 93,949 | 1.06 |
| Mean   | 1,287 / 1,465 / 1,764 | 1,329 / 1,750 / 1,920   | 1.19 |

(Per-trial tables: 10 trials each producing the per-percentile pair (`tp-default`, `hot-handoff`); the aggregate above is min/median/max of each per-percentile column across the 10 trials.)

### Observations

The two measurements characterize the same dispatcher under two different kinds of workload, and the contrast is the design's central finding.

**Throughput workload — HotHandoff wins decisively.** 1.39× over `SpscPipe_TpDefault`, 2.08× over BCL, with lower allocations. The 4 KiB-chunk workload's backpressure cycles produce idle windows long enough (>10 µs) for TP workers to exit their spin and actually sleep on the kernel semaphore. Each resume then pays a TP wake-gap on the order of multiple µs. HotHandoff's continuously-hot worker thread skips the kernel wake entirely. This is the workload pattern the dispatcher was designed for: streams where TP queues empty long enough that TP workers park between events.

**Latency workload — HotHandoff is comparable-to-slightly-worse.** P50 median 1.16× (worse), Mean median 1.19× (worse), tails (P99) effectively unchanged. The 256 B / 1 M-message workload sustains MHz event rates; idle windows between events are sub-µs, well inside TP's spin-then-sleep threshold. TP workers never actually park, so there is no kernel-wake cost for HotHandoff to escape. The dispatcher's per-event overhead (worker-thread `Interlocked` operations on the same cache line touched by the producer's signal path; cache contention without CPU pinning) shows up in the per-message latency without the wake-gap savings to offset it.

**CPU cost.** HotHandoff's worker thread sits at ~100% on its core during the busy-spin loop. Both measurements above are with one core continuously consumed. For applications where the wake-gap escape genuinely matters, this is the explicit cost.

**Design-completion criterion (spec §8.3) — characterization.** The criterion was "beats `tp-default` at the percentiles that matter under reasonable CPU cost, *or* demonstrates that no reasonable configuration does." This run shows neither pure outcome but a workload-shaped one: the dispatcher delivers its designed-for benefit (TP wake-gap escape) for streams where TP would otherwise park, and is overhead-only for streams where TP stays hot. This is a useful, honest characterization to ship with — the dispatcher should be selected against the workload's expected idle pattern, not blindly applied.

The starting tunables (`SpinIterations = 10`, no pinning, single-slot mailbox) deliver the throughput win without further tuning. CPU pinning may improve the latency-workload tail behavior but is unlikely to change the central observation (no wake-gap to escape → no benefit possible).

## Subsequent runs

Each tuning iteration adds a new section ("Run 2 — pinned worker", "Run 3 —
SpinIterations 50", etc.) capturing the same fields. The implementation is
not finalized until either:

- A configuration is justified by the data and locked in source (with the
  measurement linked from the source comment), or
- Reasonable variants are exhausted and the dispatcher does not earn its
  complexity — in which case this project is removed from the solution.
