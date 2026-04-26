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

- **BCL throughput:** 1 MiB / 110.34 us ≈ **9.50 GB/s** (1 KB = 1024 B; 1 GB = 1024^3 B).
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
- **Pause/resume thresholds differ.** BCL's `PipeOptions.Default` pause/resume = 64K/32K; the
  SPSC adapter passes `SpscPipeOptions.Default` which uses the same 64K/32K. So the comparison
  exercises identical backpressure points.
- **Latency benchmark is out of scope here.** Only `*ProduceAndDrain*` was filtered; the
  `LatencyHarness` ping-pong remains hooked up for future characterization.
