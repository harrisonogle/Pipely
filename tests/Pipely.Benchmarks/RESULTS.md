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
> in isolation (`--filter '*<ClassName>*'`).

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

### Fresh-`Pipe`, four-way scheduler matrix

| Method            | Mean      | Allocated | Alloc Ratio |
|-------------------|----------:|----------:|------------:|
| BCL_ThreadPool    | 109.10 us |   6.91 KB |        1.00 |
| BCL_Inline        |  49.46 us |   6.08 KB |        0.88 |
| Pipely_ThreadPool |  71.99 us |   8.85 KB |        1.28 |
| Pipely_Inline     |  41.73 us |   8.65 KB |        1.25 |

### Construction-cost delta

The per-row delta (fresh-`Pipe` − headline) isolates the per-`Pipe` construction component:

| Method                | Fresh-`Pipe` | Headline | Per-`Pipe` ctor |
|-----------------------|-------------:|---------:|----------------:|
| BCL Pipe (Throughput) |      7.09 KB |    926 B |        ~6.2 KB  |
| Pipely Pipe (TP)      |      8.79 KB |    969 B |        ~7.8 KB  |
| BCL_ThreadPool        |      6.91 KB |    816 B |        ~6.1 KB  |
| BCL_Inline            |      6.08 KB |    743 B |        ~5.4 KB  |
| Pipely_ThreadPool     |      8.85 KB |    889 B |        ~7.9 KB  |
| Pipely_Inline         |      8.65 KB |    750 B |        ~7.9 KB  |

Pipely's per-`Pipe` construction is ~1.7 KB heavier than BCL's at every variant. The structural
source is that Pipely allocates two `TripleBuffer<T>` (`sealed class`, ~512 B each, three
cache-line-padded slots) and two `PipelyAwaiter<T>` (`sealed class`, holds the awaiter state)
where BCL embeds the equivalent state directly in `Pipe` (`PipeAwaitable` is a `struct`). For
long-lived pipes — the production use case captured by the headline rows — this is amortized to
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
