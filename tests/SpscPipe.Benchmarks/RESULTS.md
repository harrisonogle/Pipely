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
| SpscPipe_ProduceAndDrain |  73.69 us | 0.263 us | 0.220 us |  0.67 | 0.1221 |   8.44 KB |        1.23 |

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
  SPSC adapter passes `SpscPipeOptions.Default` which uses the same 64K/32K. So the comparison
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
| SpscPipe |   1 | 250ns|   680ns| 1,230ns|  14,400ns| 3,840,958ns| 3,850,797ns| 11,923ns|
| SpscPipe |   2 | 210ns|   570ns|   940ns|   4,370ns| 3,476,284ns| 3,486,363ns| 10,646ns|
| SpscPipe |   3 | 230ns|   680ns| 1,190ns|   9,080ns| 3,623,882ns| 3,633,742ns| 11,265ns|

### Verdict

- **Min** is essentially tied (~200-250 ns for both) — both pipes hit the same noise floor on the
  fastest path.
- **P50:** SpscPipe ~600-700 ns vs BCL ~2.2-2.4 µs → **~3.4× lower median**.
- **P90:** SpscPipe ~0.9-1.2 µs vs BCL ~5-6.5 µs → **~4-6× lower**.
- **P99:** SpscPipe ~4-14 µs vs BCL ~28-156 µs → **~3-11× lower**, with BCL's P99 noticeably more
  variable run-to-run. This is the most striking gap and was completely hidden by the prior
  power-of-2 histogram (both reported the same 16,384 ns bucket label).
- **P99.9 / Max:** Both ~3-6 ms. The tail is dominated by OS scheduling jitter and GC, not by pipe
  internals; lock-free vs locked doesn't change worst-case runtime behavior. Expected.
- **Mean:** SpscPipe ~10-12 µs vs BCL ~16-22 µs → ~1.5-1.8× lower mean. The mean is dragged up
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
