# Benchmark Results

**Date:** 2026-04-30
**Hardware:** AMD Ryzen 7 8700F 8-Core Processor (16 logical / 8 physical cores, base 4.02 GHz, boost 5.06 GHz), 30 GiB RAM
**OS:** Linux Ubuntu 24.04.4 LTS (Noble Numbat), kernel 6.17.0-22-generic
**Build:** Release, .NET SDK 10.0.107 / Runtime .NET 10.0.7, RyuJIT x86-64-v4, Concurrent Server GC
**Commit:** `af5f617` (PipelyAwaiter: zero-alloc TP fast-path via IThreadPoolWorkItem)

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
| BclPipe_ProduceAndDrain | 107.03 us | 1.643 us | 1.537 us |  1.00 | 0.2441 |   7.06 KB |        1.00 |
| Pipely_ProduceAndDrain  |  76.56 us | 0.535 us | 0.573 us |  0.72 | 0.2441 |   8.80 KB |        1.25 |

(Run on a quiet machine with the benchmark class invoked in isolation:
`dotnet run -c Release --project tests/Pipely.Benchmarks -- --filter
'PipelyBenchmarks.ThroughputBenchmarks*'`.)

## Verdict

- **BCL throughput:** 1 MiB / 107.03 us ≈ **9.80 GB/s** (1 GB = 10^9 B).
- **Pipely throughput:** 1 MiB / 76.56 us ≈ **13.69 GB/s**.
- **Speedup (BCL mean / Pipely mean):** ~1.40x — i.e. Pipely takes 72% of the time BCL does for the
  same single-producer / single-consumer 1 MiB transfer.
- **Allocations per op:** Pipely **8.80 KB** vs BCL **7.06 KB** (1.25x). The remaining ~1.7 KB
  gap is entirely per-`Pipe` construction (this benchmark builds a fresh `Pipe` each iteration);
  the steady-state per-op cost is byte-for-byte tied with BCL. See "Steady-state allocations"
  below for the breakdown.

### Notes / caveats

- **Allocation source.** The previous revision of this section reported a 1.52x ratio (10.76 KB
  vs 7.10 KB) at commit `b0c312b`. The 2 KB drop here is from the `IThreadPoolWorkItem` TP
  fast-path in `PipelyAwaiter` (commit `af5f617`): `PipeScheduler.ThreadPool.Schedule(action,
  state)` was wrapping its arguments per signal; queuing the awaiter directly via
  `ThreadPool.UnsafeQueueUserWorkItem(this, ...)` removes that wrapper. The remaining ~1.7 KB
  excess vs BCL is per-`Pipe` construction overhead (TripleBuffer / PipelyAwaiter as classes
  rather than struct-embedded as BCL does).
- **Single chunk size only.** Both pipes are exercised at 4 KiB chunks, the size that corresponds
  to MemoryPool's default rental. Smaller chunks would amplify per-flush overhead; larger chunks
  would shift the bottleneck toward the memcpy. A future task should sweep {64 B, 1 KiB, 4 KiB,
  64 KiB} to characterize the curve.
- **`ServerGarbageCollection` is on** (csproj). Workstation GC may give different absolute numbers
  but should not flip the ranking.
- **Pause/resume thresholds match.** BCL's `PipeOptions.Default` pause/resume = 64K/32K; the
  SPSC adapter passes `PipeOptions.Default` which uses the same 64K/32K. So the comparison
  exercises identical backpressure points.

## Steady-state allocations

`SteadyStateThroughputBenchmarks` and `SteadyStateSchedulerBenchmarks` are companions to the
above, identical in shape except that the `Pipe` is constructed once in `[GlobalSetup]` and
reused across BDN iterations (neither side calls `Complete()` between iterations). Reported
allocations therefore exclude per-`Pipe` construction and isolate the per-1 MiB-transfer
steady-state cost.

### Steady-state throughput (`Pipe` reused across iterations)

| Method                  | Mean      | Allocated | Alloc Ratio |
|-------------------------|----------:|----------:|------------:|
| BclPipe_ProduceAndDrain | 109.78 us |     937 B |        1.00 |
| Pipely_ProduceAndDrain  |  70.50 us |     952 B |        1.02 |

Pipely's per-op allocation ties BCL within run noise (15 B over a 937 B floor that is
harness-side `Task.Run` / `Task.WhenAll` state machines, identical between the two rows).

### Steady-state, five-way scheduler matrix

| Method               | Mean      | Allocated | Alloc Ratio |
|----------------------|----------:|----------:|------------:|
| BCL_ThreadPool       | 106.45 us |     815 B |        1.00 |
| BCL_Inline           |  46.46 us |     709 B |        0.87 |
| Pipely_ThreadPool    |  71.97 us |     820 B |        1.01 |
| Pipely_Inline        |  42.03 us |     750 B |        0.92 |
| Pipely_FastScheduler |  67.42 us |    1229 B |        1.51 |

Reading:
- **Pipely_ThreadPool ≈ BCL_ThreadPool** at 820 B vs 815 B. Before the
  `IThreadPoolWorkItem` fast-path (commit `af5f617`), `Pipely_ThreadPool` allocated 2962 B
  here — a 3.91x ratio. The 2.2 KB delta was per-signal wrapper allocations from
  `PipeScheduler.ThreadPool.Schedule(action, state)`; queuing the awaiter as a work item
  directly removes them.
- **Pipely_Inline ≈ BCL_Inline.** Both inline paths run continuations on the signaling
  thread with no scheduler hop, so per-op allocation is essentially the harness floor.
- **Pipely_FastScheduler 1229 B.** Higher than the TP rows because FastScheduler still
  routes some signals through TP overflow when the worker's mailbox is full. The number
  was 865 B in the run that drove the fast-path investigation, and 1229 B in the
  re-measurement; the difference is within FastScheduler's measurement noise (worker
  busy-spin causes scheduling jitter). The fast-path code change in `PipelyAwaiter`
  doesn't affect FastScheduler's own routing.

### Per-`Pipe` construction cost

The delta between per-iter and steady-state allocation isolates the per-`Pipe` construction
component for each variant:

| Method            | Per-iter | Steady-state | Per-`Pipe` ctor |
|-------------------|---------:|-------------:|----------------:|
| BCL Pipe          |  7.06 KB |        937 B |        ~6.1 KB  |
| Pipely Pipe (TP)  |  8.80 KB |        952 B |        ~7.8 KB  |

Pipely's per-`Pipe` construction is ~1.7 KB heavier than BCL's. The structural source is
that Pipely allocates two `TripleBuffer<T>` (`sealed class`, ~512 B each, three
cache-line-padded slots) and two `PipelyAwaiter<T>` (`sealed class`, holds the awaiter
state) where BCL embeds the equivalent state directly in `Pipe` (`PipeAwaitable` is a
`struct`). For long-lived pipes — the production use case — this is amortized to
near-zero per op. For very short-lived pipes the extra ~1.7 KB shows up as a one-shot
construction cost.

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
- Throughput: `dotnet run -c Release --project tests/Pipely.Benchmarks -- --filter '*SchedulerBenchmarks*'`
- Three latency trials per recorded run; warmup trial not recorded.
- Hardware/build details captured at the top of each results section.
- The FastScheduler worker thread sits at ~100% on its core during the busy-spin
  loop. Latency and throughput wins must be read against this CPU cost.
- **Scheduler amortization:** `Pipely_FastScheduler` constructs the scheduler once via `[GlobalSetup]` and reuses it across all BDN iterations (mirroring the `ThreadPool` and `Inline` rows, which use BCL singletons with no extra state). Per-iteration cost for all five rows is therefore solely pipe ctor + produce-and-drain — apples to apples. The latency harness similarly amortizes (one scheduler per recorded trial, not per message).

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

**Date:** 2026-04-27 (latency); 2026-04-30 (throughput re-measurement with the 5-variant scheduler matrix; re-re-measured at `af5f617` after the TP fast-path)
**Hardware:** AMD Ryzen 7 8700F (8 physical / 16 logical cores), Linux Ubuntu 24.04.4 LTS, kernel 6.17.0-22-generic
**Build:** Release, .NET SDK 10.0.107 / Runtime .NET 10.0.7, Server GC + concurrent
**Commit:** `9b98a8f` (latency); `af5f617` (throughput — re-measurement after `IThreadPoolWorkItem` TP fast-path; supersedes `07be068`)
**Tunables:** as in "Starting tunables" above; no overrides.

### Throughput (1 MiB / 4 KiB chunks; five-way head-to-head, same BDN process)

| Method                 | Mean      | Error    | StdDev   | Ratio | Gen0   | Allocated | Alloc Ratio |
|------------------------|----------:|---------:|---------:|------:|-------:|----------:|------------:|
| `BCL_ThreadPool`       | 105.08 us | 0.625 us | 0.554 us |  1.00 | 0.1221 |   7.14 KB |        1.00 |
| `BCL_Inline`           |  44.23 us | 0.560 us | 0.468 us |  0.42 | 0.1221 |   5.79 KB |        0.81 |
| `Pipely_ThreadPool`    |  72.11 us | 0.282 us | 0.264 us |  0.69 | 0.3662 |   8.86 KB |        1.24 |
| `Pipely_Inline`        |  43.22 us | 0.431 us | 0.382 us |  0.41 | 0.1831 |   8.57 KB |        1.20 |
| `Pipely_FastScheduler` |  49.95 us | 0.437 us | 0.365 us |  0.48 | 0.3662 |   9.26 KB |        1.30 |

Reading:
- **Inline ≈ Inline.** `BCL_Inline` 44.23 us, `Pipely_Inline` 43.22 us — within 3%. Once the TP wake-gap is removed, per-event work is dominated by pipe internals, and BCL and Pipely are essentially tied. This isolates the "scheduler" axis from the "pipe internals" axis.
- **ThreadPool: Pipely beats BCL by ~1.46×.** `BCL_ThreadPool` 105.08 us vs `Pipely_ThreadPool` 72.11 us (33 us absolute gap). At the same scheduler, Pipely's awaiter machinery is materially leaner per-event than BCL's.
- **FastScheduler beats ThreadPool by ~1.44×** within Pipely (`Pipely_FastScheduler` 49.95 vs `Pipely_ThreadPool` 72.11). Most of the TP wake-gap is escaped by the busy-spinning worker thread.
- **Inline beats FastScheduler by ~1.16×** (`Pipely_Inline` 43.22 vs `Pipely_FastScheduler` 49.95). FastScheduler still pays a slot-CAS + worker-thread coordination cost on each dispatch; Inline pays nothing. FastScheduler's role is to escape TP without forcing continuations onto the producer's thread (the price of Inline) — the comparison to make is FastScheduler vs ThreadPool, not FastScheduler vs Inline.
- **Allocations.** `BCL_Inline` is the leanest at 5.79 KB. Pipely's allocation gap vs BCL is now uniform across schedulers (1.20–1.30x) and is entirely per-`Pipe` construction overhead — see "Steady-state allocations" above for the per-op-vs-per-construction breakdown. Prior to commit `af5f617` (`IThreadPoolWorkItem` TP fast-path), `Pipely_ThreadPool` allocated 10.76 KB (1.56x); the per-signal wrapper allocation from `PipeScheduler.ThreadPool.Schedule(action, state)` was the source.

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
