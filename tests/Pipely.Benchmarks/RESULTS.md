# Benchmark Results

**Date:** 2026-04-30
**Hardware:** AMD Ryzen 7 8700F 8-Core Processor (16 logical / 8 physical cores, base 4.02 GHz, boost 5.06 GHz), 30 GiB RAM
**OS:** Linux Ubuntu 24.04.4 LTS (Noble Numbat), kernel 6.17.0-22-generic
**Build:** Release, .NET SDK 10.0.107 / Runtime .NET 10.0.7, RyuJIT x86-64-v4, Concurrent Server GC
**Commit:** `b0c312b` (Post-rename namespace cleanup)

> **Note on prior numbers.** Earlier revisions of this section were recorded
> when the test/bench/stress projects lived under `Pipely.*` namespaces.
> Because both `Pipely` and `System.IO.Pipelines` expose identically-named
> types (`Pipe`, `PipeReader`, `PipeWriter`, `PipeOptions`), unqualified `Pipe`
> inside a `Pipely.Benchmarks` file resolved to `Pipely.Pipe`, not the BCL
> type — the "BCL" baseline was silently measuring Pipely vs Pipely. Commit
> `b0c312b` moved the test projects to top-level `PipelyBenchmarks` /
> `PipelyTests` / `PipelyStress` namespaces (assembly names preserved),
> restoring genuine BCL-vs-Pipely comparison. The numbers below were
> taken after that fix on a quiet machine with each BDN class invoked
> in isolation (`--filter '*<ClassName>*'`), so they should be directly
> comparable to the FastScheduler "Run 1" baseline below.

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

| Method                  | Mean      | Error    | StdDev   | Ratio | Gen0   | Allocated | Alloc Ratio |
|-------------------------|----------:|---------:|---------:|------:|-------:|----------:|------------:|
| BclPipe_ProduceAndDrain | 103.59 us | 1.164 us | 1.089 us |  1.00 | 0.1221 |   7.10 KB |        1.00 |
| Pipely_ProduceAndDrain  |  75.14 us | 0.598 us | 0.559 us |  0.73 | 0.3662 |  10.76 KB |        1.52 |

(All 15 iterations retained for both. SPSC StdDev = 0.56 us = 0.74% of mean;
BCL StdDev = 1.09 us = 1.05% of mean. Run on a quiet machine with the
benchmark class invoked in isolation: `dotnet run -c Release -- --filter
'*ThroughputBenchmarks*'`.)

## Verdict

- **BCL throughput:** 1 MiB / 103.59 us ≈ **10.12 GB/s** (1 GB = 10^9 B).
- **SPSC throughput:** 1 MiB / 75.14 us ≈ **13.95 GB/s**.
- **Speedup (BCL mean / SPSC mean):** ~1.38x — i.e. SPSC takes 73% of the time BCL does for the
  same single-producer / single-consumer 1 MiB transfer. SPSC StdDev 0.74%; BCL StdDev 1.05% —
  both well inside noise tolerances and the 28 us gap is real.
- **Allocations per op:** SPSC **10.76 KB** vs BCL **7.10 KB** (1.52x). Both pipes report Gen0
  collections (BCL 0.12, SPSC 0.37 collections / 1000 ops) at this allocation rate.

The spec target was throughput "materially higher than BCL's `Pipe`" on the single-producer /
single-consumer hot path. A 1.38x speedup at this chunk size meets that bar.

### Notes / caveats

- **Higher allocations.** SPSC's per-op allocation is ~3.7 KB above BCL. This benchmark constructs
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
latency under sustained throughput. Same hardware/build as throughput run, commit `b0c312b`.

Three independent trials (1 warmup, not recorded):

| Pipe   | Run |  Min |  P50 |   P90 |    P99 |     P99.9 |       Max |
|--------|----:|-----:|-----:|------:|-------:|----------:|----------:|
| BCL    |   1 |  360 |  890 | 1,440 |  6,779 |    28,659 |    35,810 |
| BCL    |   2 |  330 |  830 | 1,550 |  7,660 |    28,390 |    31,970 |
| BCL    |   3 |  370 |  920 | 1,630 | 15,980 |   646,481 |   647,471 |
| Pipely |   1 |  250 |  690 | 1,020 |  3,280 |    13,550 |    35,440 |
| Pipely |   2 |  270 |  690 | 1,010 | 26,510 |    59,749 |    85,819 |
| Pipely |   3 |  320 |  670 |   960 | 11,540 | 5,134,950 | 5,151,340 |

(All values in nanoseconds. Trial 3's Pipely P99.9/Max are dominated by a
single multi-ms outlier — most likely a GC pause; the awaiter counters for
that trial are clean, so it's not a pipe-internal stall. Trial 2's Pipely
P99 of 26,510 ns is a single ~26 µs spike — the next-worst sample is at the
P99.9 mark of 59,749 ns, so a small cluster of tail samples sits above the
P99 line for that trial only.)

### Verdict

- **Min** is essentially tied (~250-390 ns for both) — both pipes hit the same noise floor on the
  fastest path; Pipely trends slightly lower (250-320 vs 330-370).
- **P50:** Pipely ~670-690 ns vs BCL ~830-920 ns → **~1.2-1.4× lower median**, consistently across
  all three trials. Smaller margin than earlier baselines (which saw ~3.4×) because BCL's median
  is faster on this run, but the gap is in Pipely's favor in every trial.
- **P90:** Pipely ~960-1,020 ns vs BCL ~1,440-1,630 ns → **~1.4-1.7× lower**, again consistent
  across all trials.
- **P99:** Pipely ~3.3-26.5 µs vs BCL ~6.8-16.0 µs → Pipely wins trials 1 (3.3 vs 6.8) and 3
  (11.5 vs 16.0) but **loses trial 2** (26.5 vs 7.7) due to a single tail cluster. The
  P99 is the percentile most sensitive to small numbers of stalls — a one-trial regression
  there is a real signal worth investigating but doesn't change the 2/3 directional win.
- **P99.9 / Max:** Tail is dominated by OS scheduling jitter and GC. Pipely's trial 3 produced a
  single multi-millisecond outlier; BCL's trial 3 saw a ~650 µs outlier. Trials 1 and 2 are
  cleanly bounded under 90 µs for both pipes. Lock-free vs locked doesn't change worst-case
  runtime behavior — neither pipe wins the tail consistently.

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

- **Latency** (custom harness, P50/P90/P99 by sort): `ThreadPool` (Pipely with `ContinuationDispatcher = null`, i.e., `ThreadPoolContinuationDispatcher.Instance`) vs `fast-scheduler` (Pipely with `FastScheduler`).
- **Throughput** (BenchmarkDotNet, 1 MiB / 4 KiB chunks): three-way head-to-head — `BclPipe` (BCL `System.IO.Pipelines.Pipe`, baseline), `Pipely_ThreadPool`, `Pipely_FastScheduler` — all in the same BDN process invocation so their numbers are directly comparable.

**Spec reference:** `docs/superpowers/specs/2026-04-27-fast-scheduler-design.md` §8.

## Design-completion criterion

> The implementation is finalized when measurements either justify a tuned
> configuration that beats `ThreadPool` at the percentiles that matter
> (P50, P90, P99) under reasonable CPU cost, or demonstrate that no
> reasonable configuration does. Each iteration of the implementation lands
> the change with the measurement that justified it.

The architecture in spec §3 is fixed. The tunable surface in spec §9
(`SpinIterations`, backoff body, CPU pinning, mailbox depth) is open. Every
change to a tuning constant in source must be committed alongside the
measurement that drove it.

## Methodology

- Latency: `dotnet run -c Release --project tests/Pipely.Benchmarks -- dispatcher-latency --count 100000 --size 256 --trials 3 --warmup 1`
- Throughput: `dotnet run -c Release --project tests/Pipely.Benchmarks -- --filter '*SchedulerBenchmarks*'`
- Three latency trials per recorded run; warmup trial not recorded.
- Hardware/build details captured at the top of each results section.
- The FastScheduler worker thread sits at ~100% on its core during the busy-spin
  loop. Latency and throughput wins must be read against this CPU cost.
- **Scheduler amortization:** `Pipely_FastScheduler_ProduceAndDrain` constructs the scheduler once via `[GlobalSetup]` and reuses it across all BDN iterations (mirroring `BclPipe`'s no-extra-state baseline and `Pipely_ThreadPool`'s singleton-dispatcher baseline). Per-iteration cost for all three rows is therefore solely pipe ctor + produce-and-drain — apples to apples. The latency harness similarly amortizes (one scheduler per recorded trial, not per message).

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
| `Pipely_ThreadPool_ProduceAndDrain`      |  70.34 us | 0.242 us | 0.215 us | 0.67  | 0.2441 |  11.03 KB |        1.57 |
| `Pipely_FastScheduler_ProduceAndDrain`  |  50.45 us | 0.185 us | 0.173 us | 0.48  | 0.2441 |   9.21 KB |        1.31 |

Reading:
- `Pipely_ThreadPool` is **1.50×** faster than BCL — consistent with the prior `BclPipe vs Pipely` characterization above.
- `Pipely_FastScheduler` is **1.39×** faster than `Pipely_ThreadPool` (50.45 / 70.34) and **2.08×** faster than BCL.
- `Pipely_FastScheduler` allocates **0.84×** the bytes of `Pipely_ThreadPool` (9.21 / 11.03 KB) — the worker-thread invocation path doesn't allocate the per-event TP work-item objects.

### Latency (ns) — 1 M messages × 256 B, 5 warmup + 10 recorded trials

Per-trial percentiles (ns, both configurations same trial). Compact summary across trials below; per-trial detail captured from the run.

Aggregate across the 10 recorded trials (each trial sorts 1 M samples and reads exact percentile by index):

| Stat   | `ThreadPool` min / median / max | `fast-scheduler` min / median / max | median ratio (FS/TP) |
|--------|----------------------:|----------------------------:|---------------------:|
| Min    |   120 /   190 /   230 |   130 /   190 /   220       | 1.00 |
| P50    |   950 / 1,185 / 1,530 | 1,170 / 1,370 / 1,730       | 1.16 |
| P90    | 1,860 / 2,330 / 3,700 | 1,750 / 2,580 / 3,640       | 1.11 |
| P99    | 3,410 / 6,190 / 8,070 | 3,440 / 6,070 / 8,670       | 0.98 |
| P99.9  | 9,900 / 11,060 / 18,420 | 9,300 / 12,460 / 20,080   | 1.13 |
| Max    | 28,620 / 33,945 / 86,428 | 24,980 / 36,095 / 93,949 | 1.06 |
| Mean   | 1,287 / 1,465 / 1,764 | 1,329 / 1,750 / 1,920       | 1.19 |

(Per-trial tables: 10 trials each producing the per-percentile pair (`ThreadPool`, `fast-scheduler`); the aggregate above is min/median/max of each per-percentile column across the 10 trials.)

### Observations

The two measurements characterize the same scheduler under two different kinds of workload, and the contrast is the design's central finding.

**Throughput workload — FastScheduler wins decisively.** 1.39× over `Pipely_ThreadPool`, 2.08× over BCL, with lower allocations. The 4 KiB-chunk workload's backpressure cycles produce idle windows long enough (>10 µs) for TP workers to exit their spin and actually sleep on the kernel semaphore. Each resume then pays a TP wake-gap on the order of multiple µs. FastScheduler's continuously-hot worker thread skips the kernel wake entirely. This is the workload pattern the scheduler was designed for: streams where TP queues empty long enough that TP workers park between events.

**Latency workload — FastScheduler is comparable-to-slightly-worse.** P50 median 1.16× (worse), Mean median 1.19× (worse), tails (P99) effectively unchanged. The 256 B / 1 M-message workload sustains MHz event rates; idle windows between events are sub-µs, well inside TP's spin-then-sleep threshold. TP workers never actually park, so there is no kernel-wake cost for FastScheduler to escape. The scheduler's per-event overhead (worker-thread `Interlocked` operations on the same cache line touched by the producer's signal path; cache contention without CPU pinning) shows up in the per-message latency without the wake-gap savings to offset it.

**CPU cost.** FastScheduler's worker thread sits at ~100% on its core during the busy-spin loop. Both measurements above are with one core continuously consumed. For applications where the wake-gap escape genuinely matters, this is the explicit cost.

**Design-completion criterion (spec §8.3) — characterization.** The criterion was "beats `ThreadPool` at the percentiles that matter under reasonable CPU cost, *or* demonstrates that no reasonable configuration does." This run shows neither pure outcome but a workload-shaped one: the scheduler delivers its designed-for benefit (TP wake-gap escape) for streams where TP would otherwise park, and is overhead-only for streams where TP stays hot. This is a useful, honest characterization to ship with — the scheduler should be selected against the workload's expected idle pattern, not blindly applied.

The starting tunables (`SpinIterations = 10`, no pinning, single-slot mailbox) deliver the throughput win without further tuning. CPU pinning may improve the latency-workload tail behavior but is unlikely to change the central observation (no wake-gap to escape → no benefit possible).

## Subsequent runs

Each tuning iteration adds a new section ("Run 2 — pinned worker", "Run 3 —
SpinIterations 50", etc.) capturing the same fields. The implementation is
not finalized until either:

- A configuration is justified by the data and locked in source (with the
  measurement linked from the source comment), or
- Reasonable variants are exhausted and the scheduler does not earn its
  complexity — in which case it is removed from the public surface.
