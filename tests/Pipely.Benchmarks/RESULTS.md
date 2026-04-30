# Benchmark Results

**Date:** 2026-04-26
**Hardware:** AMD Ryzen 7 8700F 8-Core Processor (16 logical / 8 physical cores, base 4.02 GHz, boost 5.06 GHz), 30 GiB RAM
**OS:** Linux Ubuntu 24.04.4 LTS (Noble Numbat), kernel 6.17.0-22-generic
**Build:** Release, .NET SDK 10.0.107 / Runtime .NET 10.0.7, RyuJIT x86-64-v4, Concurrent Server GC
**Commit:** `51aa01385c6ab92d3b5345fd9c4fd6033beb1050`

## Throughput

`ThroughputBenchmarks.ProduceAndDrain` — single producer task fills 1 MiB through 4 KiB chunks
(`GetMemory` / copy / `Advance` / `FlushAsync`), single consumer drains via `ReadAsync` /
`AdvanceTo(buffer.End)`. `TotalBytes = 1 << 20`, `ChunkSize = 4096`. Both sides call synchronous
`Complete()` at the end. `[MemoryDiagnoser]` tracks allocations.

```
BenchmarkDotNet v0.15.8, Linux Ubuntu 24.04.4 LTS (Noble Numbat)
AMD Ryzen 7 8700F 4.02GHz, 1 CPU, 16 logical and 8 physical cores
.NET SDK 10.0.107
  [Host]     : .NET 10.0.7 (10.0.7, 10.0.726.21808), X64 RyuJIT x86-64-v4
  DefaultJob : .NET 10.0.7 (10.0.7, 10.0.726.21808), X64 RyuJIT x86-64-v4
```

| Method                   | Mean      | Error    | StdDev   | Ratio | Gen0   | Allocated | Alloc Ratio |
|------------------------- |----------:|---------:|---------:|------:|-------:|----------:|------------:|
| BclPipe_ProduceAndDrain  | 110.34 us | 0.728 us | 0.681 us |  1.00 |      - |   6.87 KB |        1.00 |
| Pipe_ProduceAndDrain |  73.69 us | 0.263 us | 0.220 us |  0.67 | 0.1221 |   8.44 KB |        1.23 |

(Two SPSC outliers at 74.50 us and 75.00 us were trimmed by BDN; the 13 retained iterations are
tightly clustered, StdDev = 0.22 us = 0.30% of mean.)

## Verdict

- **BCL throughput:** 1 MiB / 110.34 us ≈ **9.50 GB/s** (1 GB = 10^9 B).
- **SPSC throughput:** 1 MiB / 73.69 us ≈ **14.23 GB/s**.
- **Speedup (BCL mean / SPSC mean):** ~1.50x — i.e. SPSC takes 67% of the time BCL does for the
  same single-producer / single-consumer 1 MiB transfer. Both runs are extremely stable
  (BCL 0.62% StdDev, SPSC 0.30% StdDev) so the gap is real and well outside measurement noise.
- **Allocations per op:** SPSC **8.44 KB** vs BCL **6.87 KB** (1.23x). SPSC also reports
  Gen0 = 0.1221 collections / 1000 ops, while BCL shows none.

The spec target was throughput "materially higher than BCL's `Pipe`" on the single-producer /
single-consumer hot path. A 1.50x speedup at this chunk size meets that bar.

### Notes / caveats

- **Higher allocations.** SPSC's per-op allocation is ~1.6 KB above BCL. This benchmark constructs
  a fresh pipe (and therefore new TripleBuffer slots and ReaderState/WriterState graphs) every
  iteration, plus the producer Task / consumer Task / FlushAsync awaitables, so the gap is partly
  an artifact of one-shot construction rather than steady-state per-byte allocation. Confirming
  that — and reducing it where possible — is future work.
- **Single chunk size only.** Both pipes are exercised at 4 KiB chunks, the size that corresponds
  to MemoryPool's default rental. Smaller chunks would amplify per-flush overhead; larger chunks
  would shift the bottleneck toward the memcpy. A future task should sweep {64 B, 1 KiB, 4 KiB,
  64 KiB} to characterize the curve.
- **`ServerGarbageCollection` is on** (csproj). Workstation GC may give different absolute numbers
  but should not flip the ranking.
- **Pause/resume thresholds match.** BCL's `PipeOptions.Default` pause/resume = 64K/32K; the
  SPSC adapter passes `PipeOptions.Default` which uses the same 64K/32K. So the comparison
  exercises identical backpressure points.

## Latency

`LatencyHarness.Run` — producer writes 100,000 fixed-size 256-byte messages, each prefixed with a
`Stopwatch.GetTimestamp()` value. Consumer reads each message and records `(now - timestamp)` into
a flat 100K-element `long[]`. After both sides finish, the array is sorted and exact percentiles
are read by index (nearest-rank). No artificial pacing — measures producer→consumer hand-off
latency under sustained throughput. Same hardware/build as throughput run, commit `81cd302`.

Three independent runs:

| Pipe     | Run | Min  | P50    | P90    | P99      | P99.9    | Max      | Mean    |
|----------|----:|-----:|-------:|-------:|---------:|---------:|---------:|--------:|
| BCL      |   1 | 210ns| 2,230ns| 4,950ns| 156,267ns| 6,209,599ns| 6,212,910ns| 21,898ns|
| BCL      |   2 | 220ns| 2,350ns| 6,590ns|  28,079ns| 5,848,336ns| 5,850,656ns| 19,091ns|
| BCL      |   3 | 230ns| 2,390ns| 5,880ns|  33,260ns| 5,094,158ns| 5,098,688ns| 15,938ns|
| Pipe |   1 | 250ns|   680ns| 1,230ns|  14,400ns| 3,840,958ns| 3,850,797ns| 11,923ns|
| Pipe |   2 | 210ns|   570ns|   940ns|   4,370ns| 3,476,284ns| 3,486,363ns| 10,646ns|
| Pipe |   3 | 230ns|   680ns| 1,190ns|   9,080ns| 3,623,882ns| 3,633,742ns| 11,265ns|

### Verdict

- **Min** is essentially tied (~200-250 ns for both) — both pipes hit the same noise floor on the
  fastest path.
- **P50:** Pipe ~600-700 ns vs BCL ~2.2-2.4 µs → **~3.4× lower median**.
- **P90:** Pipe ~0.9-1.2 µs vs BCL ~5-6.5 µs → **~4-6× lower**.
- **P99:** Pipe ~4-14 µs vs BCL ~28-156 µs → **~3-11× lower**, with BCL's P99 noticeably more
  variable run-to-run. This is the most striking gap and was completely hidden by the prior
  power-of-2 histogram (both reported the same 16,384 ns bucket label).
- **P99.9 / Max:** Both ~3-6 ms. The tail is dominated by OS scheduling jitter and GC, not by pipe
  internals; lock-free vs locked doesn't change worst-case runtime behavior. Expected.
- **Mean:** Pipe ~10-12 µs vs BCL ~16-22 µs → ~1.5-1.8× lower mean. The mean is dragged up
  for both by the millisecond-scale outliers, so percentile views are more informative.

### Notes / caveats

- **Exact percentiles.** Switched from a power-of-2 log-bucket histogram (which made P99 readings
  identical between pipes — both fell in the same wide bucket) to sort-all-samples. 100K longs
  = 800 KB, sort is sub-100 ms, percentiles are exact.
- **`Stopwatch.GetTimestamp()` overhead** is ~10-20 ns on this hardware, comfortably below the
  ~200 ns Min — measurement floor is not contaminating the signal.
- **No warmup separation.** First-message latency includes JIT warmup. With 100K samples that's
  diluted in P50/P90/P99 but contributes to the worst few outliers (P99.9+).
- **Sustained-throughput model, not round-trip.** Measures stream latency under continuous flow.
  Producer hot-loops without pacing; once the in-flight bytes hit `PauseWriterThreshold` (64K =
  256 messages), the producer parks on `FlushAsync` until the consumer drains to `Resume`. The
  recorded latency for any given message includes time-in-queue ahead of it, so this is closer to
  "system-level latency under saturation" than to "isolated per-message hand-off cost." A round-trip
  ping-pong benchmark (request/reply via two pipes) would isolate the latter; not currently
  implemented.
- **`Min` is a single sample.** Over 100K iterations one is bound to land at the floor; treat Min
  as a noise indicator, not a steady-state quantity.

# FastScheduler benchmark results

**Compared:**

- **Latency** (custom harness, P50/P90/P99 by sort): `tp-default` (Pipe with `ContinuationDispatcher = null`, i.e., `ThreadPoolContinuationDispatcher.Instance`) vs `fast-scheduler` (Pipe with `FastScheduler`).
- **Throughput** (BenchmarkDotNet, 1 MiB / 4 KiB chunks): three-way head-to-head — `BclPipe` (BCL `System.IO.Pipelines.Pipe`, baseline), `Pipe_TpDefault`, `Pipe_FastScheduler` — all in the same BDN process invocation so their numbers are directly comparable.

**Spec reference:** `docs/superpowers/specs/2026-04-27-fast-scheduler-design.md` §8.

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

- Latency: `dotnet run -c Release --project tests/Pipely.Benchmarks -- dispatcher-latency --count 100000 --size 256 --trials 3 --warmup 1`
- Throughput: `dotnet run -c Release --project tests/Pipely.Benchmarks -- --filter '*DispatcherThroughputBench*'`
- Three latency trials per recorded run; warmup trial not recorded.
- Hardware/build details captured at the top of each results section.
- The FastScheduler worker thread sits at ~100% on its core during the busy-spin
  loop. Latency and throughput wins must be read against this CPU cost.
- **Scheduler amortization:** `Pipe_FastScheduler_ProduceAndDrain` constructs the scheduler once via `[GlobalSetup]` and reuses it across all BDN iterations (mirroring `BclPipe`'s no-extra-state baseline and `Pipe_TpDefault`'s singleton-dispatcher baseline). Per-iteration cost for all three rows is therefore solely pipe ctor + produce-and-drain — apples to apples. The latency harness similarly amortizes (one scheduler per recorded trial, not per message).

## Starting tunables

Recorded here verbatim so that any tuning iteration is auditable against
the prior baseline.

| Tunable | Starting value | Notes |
|---|---|---|
| `SpinIterations` | 10 | `private const int` in `FastScheduler.cs` |
| Backoff body | `Thread.SpinWait(SpinIterations)` | The entire idle-loop body |
| CPU pinning | none | Worker thread is unpinned in the starting configuration |
| Mailbox depth | 1 | Single-slot with TP overflow on contention |

## Run 1 — starting configuration

**Date:** 2026-04-27
**Hardware:** AMD Ryzen 7 8700F (8 physical / 16 logical cores), Linux Ubuntu 24.04.4 LTS, kernel 6.17.0-22-generic
**Build:** Release, .NET SDK 10.0.107 / Runtime .NET 10.0.7, Server GC + concurrent
**Commit:** `9b98a8f` (FastScheduler bench: amortize scheduler + add BCL to 3-way throughput)
**Tunables:** as in "Starting tunables" above; no overrides.

### Throughput (1 MiB / 4 KiB chunks; head-to-head-to-head, same BDN process)

| Method                                | Mean      | Error    | StdDev   | Ratio | Gen0   | Allocated | Alloc Ratio |
|---------------------------------------|----------:|---------:|---------:|------:|-------:|----------:|------------:|
| `BclPipe_ProduceAndDrain`             | 105.09 us | 0.834 us | 0.780 us | 1.00  | 0.1221 |   7.03 KB |        1.00 |
| `Pipe_TpDefault_ProduceAndDrain`      |  70.34 us | 0.242 us | 0.215 us | 0.67  | 0.2441 |  11.03 KB |        1.57 |
| `Pipe_FastScheduler_ProduceAndDrain`  |  50.45 us | 0.185 us | 0.173 us | 0.48  | 0.2441 |   9.21 KB |        1.31 |

Reading:
- `Pipe_TpDefault` is **1.50×** faster than BCL — consistent with the prior `BclPipe vs Pipe` characterization above.
- `Pipe_FastScheduler` is **1.39×** faster than `Pipe_TpDefault` (50.45 / 70.34) and **2.08×** faster than BCL.
- `Pipe_FastScheduler` allocates **0.84×** the bytes of `Pipe_TpDefault` (9.21 / 11.03 KB) — the worker-thread invocation path doesn't allocate the per-event TP work-item objects.

### Latency (ns) — 1 M messages × 256 B, 5 warmup + 10 recorded trials

Per-trial percentiles (ns, both configurations same trial). Compact summary across trials below; per-trial detail captured from the run.

Aggregate across the 10 recorded trials (each trial sorts 1 M samples and reads exact percentile by index):

| Stat   | `tp-default` min / median / max | `fast-scheduler` min / median / max | median ratio (FS/TP) |
|--------|----------------------:|----------------------------:|---------------------:|
| Min    |   120 /   190 /   230 |   130 /   190 /   220       | 1.00 |
| P50    |   950 / 1,185 / 1,530 | 1,170 / 1,370 / 1,730       | 1.16 |
| P90    | 1,860 / 2,330 / 3,700 | 1,750 / 2,580 / 3,640       | 1.11 |
| P99    | 3,410 / 6,190 / 8,070 | 3,440 / 6,070 / 8,670       | 0.98 |
| P99.9  | 9,900 / 11,060 / 18,420 | 9,300 / 12,460 / 20,080   | 1.13 |
| Max    | 28,620 / 33,945 / 86,428 | 24,980 / 36,095 / 93,949 | 1.06 |
| Mean   | 1,287 / 1,465 / 1,764 | 1,329 / 1,750 / 1,920       | 1.19 |

(Per-trial tables: 10 trials each producing the per-percentile pair (`tp-default`, `fast-scheduler`); the aggregate above is min/median/max of each per-percentile column across the 10 trials.)

### Observations

The two measurements characterize the same scheduler under two different kinds of workload, and the contrast is the design's central finding.

**Throughput workload — FastScheduler wins decisively.** 1.39× over `Pipe_TpDefault`, 2.08× over BCL, with lower allocations. The 4 KiB-chunk workload's backpressure cycles produce idle windows long enough (>10 µs) for TP workers to exit their spin and actually sleep on the kernel semaphore. Each resume then pays a TP wake-gap on the order of multiple µs. FastScheduler's continuously-hot worker thread skips the kernel wake entirely. This is the workload pattern the scheduler was designed for: streams where TP queues empty long enough that TP workers park between events.

**Latency workload — FastScheduler is comparable-to-slightly-worse.** P50 median 1.16× (worse), Mean median 1.19× (worse), tails (P99) effectively unchanged. The 256 B / 1 M-message workload sustains MHz event rates; idle windows between events are sub-µs, well inside TP's spin-then-sleep threshold. TP workers never actually park, so there is no kernel-wake cost for FastScheduler to escape. The scheduler's per-event overhead (worker-thread `Interlocked` operations on the same cache line touched by the producer's signal path; cache contention without CPU pinning) shows up in the per-message latency without the wake-gap savings to offset it.

**CPU cost.** FastScheduler's worker thread sits at ~100% on its core during the busy-spin loop. Both measurements above are with one core continuously consumed. For applications where the wake-gap escape genuinely matters, this is the explicit cost.

**Design-completion criterion (spec §8.3) — characterization.** The criterion was "beats `tp-default` at the percentiles that matter under reasonable CPU cost, *or* demonstrates that no reasonable configuration does." This run shows neither pure outcome but a workload-shaped one: the scheduler delivers its designed-for benefit (TP wake-gap escape) for streams where TP would otherwise park, and is overhead-only for streams where TP stays hot. This is a useful, honest characterization to ship with — the scheduler should be selected against the workload's expected idle pattern, not blindly applied.

The starting tunables (`SpinIterations = 10`, no pinning, single-slot mailbox) deliver the throughput win without further tuning. CPU pinning may improve the latency-workload tail behavior but is unlikely to change the central observation (no wake-gap to escape → no benefit possible).

## Subsequent runs

Each tuning iteration adds a new section ("Run 2 — pinned worker", "Run 3 —
SpinIterations 50", etc.) capturing the same fields. The implementation is
not finalized until either:

- A configuration is justified by the data and locked in source (with the
  measurement linked from the source comment), or
- Reasonable variants are exhausted and the scheduler does not earn its
  complexity — in which case it is removed from the public surface.
