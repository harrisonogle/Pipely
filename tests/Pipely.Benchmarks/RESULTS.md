# Benchmark Results

**Date:** 2026-04-30
**Hardware:** AMD Ryzen 7 8700F 8-Core Processor (16 logical / 8 physical cores, base 4.02 GHz, boost 5.06 GHz), 30 GiB RAM
**OS:** Linux Ubuntu 24.04.4 LTS (Noble Numbat), kernel 6.17.0-22-generic
**Build:** Release, .NET SDK 10.0.107 / Runtime .NET 10.0.7, RyuJIT x86-64-v4, Concurrent Server GC
**Commit:** post-`5a572b8` (re-measured after the headline benchmarks were flipped to steady-state shape; `Pipe` reused across iterations)

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
`AdvanceTo(buffer.End)`. `TotalBytes = 1 << 20`, `ChunkSize = 4096`. The `Pipe` is constructed
once in `[GlobalSetup]` and reused across BDN iterations; neither side calls `Complete()`
between iterations. Reported allocations therefore exclude per-`Pipe` construction and
characterize the production shape (long-lived pipes). `[MemoryDiagnoser]` tracks allocations.

```
BenchmarkDotNet v0.15.8, Linux Ubuntu 24.04.4 LTS (Noble Numbat)
AMD Ryzen 7 8700F 4.00GHz, 1 CPU, 16 logical and 8 physical cores
.NET SDK 10.0.107
  [Host]     : .NET 10.0.7 (10.0.7, 10.0.726.21808), X64 RyuJIT x86-64-v4
  DefaultJob : .NET 10.0.7 (10.0.7, 10.0.726.21808), X64 RyuJIT x86-64-v4
```

| Method                  | Mean      | Error    | StdDev   | Ratio | Allocated | Alloc Ratio |
|-------------------------|----------:|---------:|---------:|------:|----------:|------------:|
| BclPipe_ProduceAndDrain | 109.78 us | 0.483 us | 0.428 us |  1.00 |     926 B |        1.00 |
| Pipely_ProduceAndDrain  |  74.64 us | 0.371 us | 0.309 us |  0.68 |     969 B |        1.05 |

(Run on a quiet machine with the benchmark class invoked in isolation:
`dotnet run -c Release --project tests/Pipely.Benchmarks -- --filter
'PipelyBenchmarks.ThroughputBenchmarks.*'`.)

## Verdict

- **BCL throughput:** 1 MiB / 109.78 us ≈ **9.55 GB/s** (1 GB = 10^9 B).
- **Pipely throughput:** 1 MiB / 74.64 us ≈ **14.05 GB/s**.
- **Speedup (BCL mean / Pipely mean):** ~1.47x — i.e. Pipely takes 68% of the time BCL does for the
  same single-producer / single-consumer 1 MiB transfer.
- **Allocations per op:** Pipely **969 B** vs BCL **926 B** (1.05x). With the `Pipe` reused
  across iterations, both pipes' per-op cost is essentially the harness floor (`Task.Run` /
  `Task.WhenAll` state machines), and Pipely's awaiter machinery adds only ~40 B over BCL's.
  Per-`Pipe` construction itself is ~1.7 KB heavier in Pipely — see "Per-`Pipe` construction cost"
  below.

### Notes / caveats

- **Single chunk size only.** Both pipes are exercised at 4 KiB chunks, the size that corresponds
  to MemoryPool's default rental. Smaller chunks would amplify per-flush overhead; larger chunks
  would shift the bottleneck toward the memcpy. A future task should sweep {64 B, 1 KiB, 4 KiB,
  64 KiB} to characterize the curve.
- **`ServerGarbageCollection` is on** (csproj). Workstation GC may give different absolute numbers
  but should not flip the ranking.
- **Pause/resume thresholds match.** BCL's `PipeOptions.Default` pause/resume = 64K/32K; the
  SPSC adapter passes `PipeOptions.Default` which uses the same 64K/32K. So the comparison
  exercises identical backpressure points.
- **TP fast-path attribution.** Before commit `af5f617` (`IThreadPoolWorkItem` TP fast-path in
  `PipelyAwaiter`), `Pipely_ThreadPool` paid an extra ~2.2 KB per op from
  `PipeScheduler.ThreadPool.Schedule(action, state)` wrapper allocations. Queuing the awaiter as
  a work item directly removed them; this is why the steady-state allocation now ties BCL
  within harness noise.

## Per-`Pipe` construction cost (fresh-`Pipe` companion)

`FreshPipeThroughputBenchmarks` and `FreshPipeSchedulerBenchmarks` are the fresh-`Pipe`
companions to the headline benchmarks above. Each constructs a new `Pipe` per iteration and
calls `Complete()` on both sides at the end. Reported allocations therefore include per-`Pipe`
construction; the per-row delta versus the headline isolates that construction cost.

### Fresh-`Pipe` throughput

| Method                  | Mean      | Allocated | Alloc Ratio |
|-------------------------|----------:|----------:|------------:|
| BclPipe_ProduceAndDrain | 106.83 us |   7.09 KB |        1.00 |
| Pipely_ProduceAndDrain  |  76.20 us |   8.79 KB |        1.24 |

### Fresh-`Pipe`, five-way scheduler matrix

| Method               | Mean      | Allocated | Alloc Ratio |
|----------------------|----------:|----------:|------------:|
| BCL_ThreadPool       | 109.10 us |   6.91 KB |        1.00 |
| BCL_Inline           |  49.46 us |   6.08 KB |        0.88 |
| Pipely_ThreadPool    |  71.99 us |   8.85 KB |        1.28 |
| Pipely_Inline        |  41.73 us |   8.65 KB |        1.25 |
| Pipely_FastScheduler |  49.95 us |   9.29 KB |        1.34 |

### Construction-cost delta

The per-row delta (fresh-`Pipe` − headline) isolates the per-`Pipe` construction component:

| Method               | Fresh-`Pipe` | Headline | Per-`Pipe` ctor |
|----------------------|-------------:|---------:|----------------:|
| BCL Pipe (Throughput)|      7.09 KB |    926 B |        ~6.2 KB  |
| Pipely Pipe (TP)     |      8.79 KB |    969 B |        ~7.8 KB  |
| BCL_ThreadPool       |      6.91 KB |    816 B |        ~6.1 KB  |
| BCL_Inline           |      6.08 KB |    743 B |        ~5.4 KB  |
| Pipely_ThreadPool    |      8.85 KB |    889 B |        ~7.9 KB  |
| Pipely_Inline        |      8.65 KB |    750 B |        ~7.9 KB  |
| Pipely_FastScheduler |      9.29 KB |  1,158 B |        ~8.2 KB  |

Pipely's per-`Pipe` construction is ~1.7 KB heavier than BCL's at every variant. The structural
source is that Pipely allocates two `TripleBuffer<T>` (`sealed class`, ~512 B each, three
cache-line-padded slots) and two `PipelyAwaiter<T>` (`sealed class`, holds the awaiter state)
where BCL embeds the equivalent state directly in `Pipe` (`PipeAwaitable` is a `struct`). For
long-lived pipes — the production use case captured by the headline rows — this is amortized to
near-zero per op. For very short-lived pipes the extra ~1.7 KB shows up as a one-shot
construction cost.

### Caveat — fresh-`Pipe` `Pipely_FastScheduler` mean

`Pipely_FastScheduler`'s mean is faster in the fresh-`Pipe` row (49.95 us) than in the headline
steady-state row (63.86 us). This reproduces across runs and is FastScheduler-specific: the
worker thread's mailbox / CAS path interacts with always-warm pipe state differently than with
freshly-constructed pipes. The other four variants behave conventionally (steady-state ≤
fresh-`Pipe` mean within run noise). The mean delta does not affect the allocation analysis
above.

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

- **Latency** (custom harness, P50/P90/P99 by sort): `ThreadPool` (Pipely with `ReaderScheduler = WriterScheduler = PipeScheduler.ThreadPool`, the default) vs `FastScheduler` (Pipely with both schedulers set to a `Pipely.FastScheduler` instance).
- **Throughput** (BenchmarkDotNet, 1 MiB / 4 KiB chunks): five-way head-to-head — `BCL_ThreadPool`, `BCL_Inline`, `Pipely_ThreadPool`, `Pipely_Inline`, `Pipely_FastScheduler` — all in the same BDN process invocation so their numbers are directly comparable. The 2×2 BCL × Pipely × {ThreadPool, Inline} matrix isolates "scheduler" from "pipe internals"; the FastScheduler row is Pipely-only since BCL has no analogue.

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
- Throughput (headline, steady-state): `dotnet run -c Release --project tests/Pipely.Benchmarks -- --filter 'PipelyBenchmarks.SchedulerBenchmarks.*'`
- Throughput (fresh-`Pipe` companion): `dotnet run -c Release --project tests/Pipely.Benchmarks -- --filter 'PipelyBenchmarks.FreshPipeSchedulerBenchmarks.*'`
- Three latency trials per recorded run; warmup trial not recorded.
- Hardware/build details captured at the top of each results section.
- The FastScheduler worker thread sits at ~100% on its core during the busy-spin
  loop. Latency and throughput wins must be read against this CPU cost.
- **Pipe + scheduler amortization:** the headline `SchedulerBenchmarks` reuses the `Pipe` across iterations (production shape: long-lived pipes); `Pipely_FastScheduler` likewise constructs its scheduler once via `[GlobalSetup]`, mirroring the `ThreadPool` / `Inline` rows that use BCL singletons. Per-iteration cost for all five rows is therefore solely produce-and-drain on a warm `Pipe` — apples to apples. The fresh-`Pipe` companion (`FreshPipeSchedulerBenchmarks`) constructs a new `Pipe` per iteration; the per-row delta is the per-`Pipe` construction cost. The latency harness similarly amortizes the scheduler (one per recorded trial, not per message).

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

**Date:** 2026-04-27 (latency); 2026-04-30 (throughput re-measured under headline-flipped (steady-state) `SchedulerBenchmarks`)
**Hardware:** AMD Ryzen 7 8700F (8 physical / 16 logical cores), Linux Ubuntu 24.04.4 LTS, kernel 6.17.0-22-generic
**Build:** Release, .NET SDK 10.0.107 / Runtime .NET 10.0.7, Server GC + concurrent
**Commit:** `9b98a8f` (latency); post-`5a572b8` (throughput — `Pipe` reused across iterations; supersedes `af5f617` fresh-`Pipe` numbers, which now live under `FreshPipeSchedulerBenchmarks`)
**Tunables:** as in "Starting tunables" above; no overrides.

### Throughput (1 MiB / 4 KiB chunks; five-way head-to-head, same BDN process; `Pipe` reused across iterations)

| Method                 | Mean      | Error    | StdDev   | Ratio | Allocated | Alloc Ratio |
|------------------------|----------:|---------:|---------:|------:|----------:|------------:|
| `BCL_ThreadPool`       | 103.64 us | 0.553 us | 0.517 us |  1.00 |     816 B |        1.00 |
| `BCL_Inline`           |  48.47 us | 0.308 us | 0.273 us |  0.47 |     743 B |        0.91 |
| `Pipely_ThreadPool`    |  71.24 us | 0.291 us | 0.273 us |  0.69 |     889 B |        1.09 |
| `Pipely_Inline`        |  41.70 us | 0.769 us | 0.719 us |  0.40 |     750 B |        0.92 |
| `Pipely_FastScheduler` |  63.86 us | 0.996 us | 0.932 us |  0.62 |   1,158 B |        1.42 |

Reading:
- **Inline ≈ Inline.** `BCL_Inline` 48.47 us, `Pipely_Inline` 41.70 us — Pipely is ~14% faster on the inline path. Once the TP wake-gap is removed, per-event work is dominated by pipe internals; the gap reflects Pipely's lighter awaiter / signaling path. (At the matched-pipe-internals comparison, the two are close — the variance comes from inline-path differences in awaiter structure.)
- **ThreadPool: Pipely beats BCL by ~1.45×.** `BCL_ThreadPool` 103.64 us vs `Pipely_ThreadPool` 71.24 us (32 us absolute gap). At the same scheduler, Pipely's awaiter machinery is materially leaner per-event than BCL's.
- **FastScheduler beats ThreadPool by ~1.12×** within Pipely (`Pipely_FastScheduler` 63.86 vs `Pipely_ThreadPool` 71.24). Most of the TP wake-gap is escaped by the busy-spinning worker thread; the steady-state mean shows a smaller margin than the fresh-`Pipe` companion (~1.44×) because reused-`Pipe` state reduces FastScheduler's per-iter advantage.
- **Inline beats FastScheduler by ~1.53×** (`Pipely_Inline` 41.70 vs `Pipely_FastScheduler` 63.86). FastScheduler still pays a slot-CAS + worker-thread coordination cost on each dispatch; Inline pays nothing. FastScheduler's role is to escape TP without forcing continuations onto the producer's thread (the price of Inline) — the comparison to make is FastScheduler vs ThreadPool, not FastScheduler vs Inline.
- **Allocations.** With the `Pipe` reused across iterations, all five rows are within ~450 B of each other — per-op cost is essentially the harness floor. `Pipely_FastScheduler` (1,158 B) carries an extra ~340 B over `Pipely_ThreadPool` because FastScheduler still routes some signals through TP overflow when the worker's mailbox is full, and that overflow path allocates per-signal wrappers. The fresh-`Pipe` companion (`FreshPipeSchedulerBenchmarks`) shows the per-`Pipe` construction component — see the "Per-`Pipe` construction cost" section earlier in this doc for the delta breakdown.

### Latency (ns) — 1 M messages × 256 B, 5 warmup + 10 recorded trials

> **Provenance:** the latency aggregate below is the original 2026-04-27
> measurement at commit `9b98a8f`, prior to the BCL_Inline / scheduler-matrix
> work. The throughput re-measurement above did not include latency. A
> latency re-measurement against the expanded matrix would be a useful
> follow-up but is not in this run.

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

**Throughput workload — FastScheduler wins over ThreadPool, but Inline wins overall.** Within Pipely, FastScheduler is 1.44× over `Pipely_ThreadPool`. The 4 KiB-chunk workload's backpressure cycles produce idle windows long enough (>10 µs) for TP workers to exit their spin and actually sleep on the kernel semaphore; each resume then pays a TP wake-gap on the order of multiple µs. FastScheduler's continuously-hot worker thread skips the kernel wake entirely. The expanded matrix above shows that `Pipely_Inline` is faster still (1.16× over FastScheduler) — Inline pays no scheduling cost at all because continuations run synchronously on the signaling thread. **FastScheduler's niche is therefore narrower than "fastest scheduler": it is the fastest scheduler that still runs continuations on a separate thread.** Choose Inline when continuation-on-signal-thread is acceptable; choose FastScheduler when continuations need an off-thread context but TP wake-gap costs too much.

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
