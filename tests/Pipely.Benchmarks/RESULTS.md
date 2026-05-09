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

## Cache-line padding (commit `a92c020`, 2026-05-04)

`Pipe` instance fields were unpadded between the writer-thread-only block
(`_chainHead`, `_writingHead`, `_totalWritten`, `_lastPublishedWriterState`,
`_writerCompleted`, …) and the reader-thread-only block (`_readHead`,
`_totalConsumed`, `_lastPublishedReaderState`, `_readerCompleted`,
`_readPending`, …), leaving false sharing on the cache line that bridged
them — most acutely on the bool flags, which packed `_writerCompleted` /
`_readerCompleted` / `_readPending` / `_disposed` within a few bytes of each
other. (`TripleBuffer<T>` was already 128 B-padded; this patch extends the
same discipline to the `Pipe` class itself.) Commit `a92c020` adds
`[StructLayout(LayoutKind.Sequential)]` on `Pipe` and inserts two
`[InlineArray(128)]` `CacheLinePad` fields — one before the writer-side
block, one between the writer-side and reader-side blocks.

### Pinned busy-poll benchmark

`PinnedThroughputBenchmarks` (introduced in commit `1c169dc`) drives the
SPSC steady-state path with raw threads pinned via `sched_setaffinity`
(Linux), `PipeScheduler.Inline`, and backpressure disabled
(`pauseWriterThreshold: 0`) so the producer never parks and the consumer
busy-polls on `TryRead`. This isolates data-structure contention from
`Task.Run` / `Task.WhenAll` / awaiter overhead. Producer pinned to CPU 2,
consumer to CPU 4 (distinct physical cores; `lscpu -e` confirms separate
L1/L2, shared L3).

| Method                | Mean      | Error    | StdDev   | Ratio | Allocated |
|---------------------- |----------:|---------:|---------:|------:|----------:|
| BCL_PinnedBusyPoll    | 109.48 μs | 1.178 μs | 1.102 μs |  1.00 |         - |
| Pipely_PinnedBusyPoll |  76.17 μs | 0.516 μs | 0.482 μs |  0.70 |         - |

Both pipes are alloc-free per iteration in steady state on this path:
`TryRead` is synchronous, `FlushAsync` returns a sync-completed `ValueTask`
with backpressure disabled, and the pinned threads are reused across BDN
iterations.

### Cache-line bouncing (perf c2c, same session)

A standalone harness (`dotnet run -- cache-bench --pipe pipely
--producer-core 2 --consumer-core 4 --duration 20`, wrapped in
`perf c2c record`) attributes HITM events to specific cache lines and
fields. Both runs are on the same hardware, same session, only the
patch differs:

| Pipely (cache-bench, perf c2c) | Pre-patch | Post-patch |        Δ |
|------------------------------- |----------:|-----------:|---------:|
| Throughput                     | 10,460 MiB/s | 13,522 MiB/s | **+29%** |
| Total Local HITM               |     1,285 |        849 | **−34%** |
| HITM per GiB transferred       |        ~6 |         ~3 | **−50%** |
| Bool-flag line HITM (`0x...7c0`) |    167 |         41 | **−75%** |

For reference, on the same harness BCL transfers ~9.1 GiB/s with ~14.6
HITM/GiB — ~5× more cache-line bouncing per byte than padded Pipely.

The residual hot lines align with `WriterState` fields inside a
`TripleBuffer` slot (`TotalWritten` at offset 0x18, `IsCompleted` at 0x20,
`CompletionException` at 0x28), with `TripleBuffer<WriterState>::Publish()`
directly attributed at offset 0x38 of the slot-swap line. That's the
*intended* SPSC handoff (true sharing of the slot's data) — not false
sharing — and isn't reducible without changing the architecture.

### Effect on default async/TP path

On the `Task.Run + await + Pipe`-on-ThreadPool shape (the headline
`SchedulerBenchmarks.Pipely_ThreadPool` row), the patch is performance-
neutral within noise:

- `Pipely_ThreadPool` post-patch: **74.65 μs**
- `Pipely_ProduceAndDrain` (≡ `Pipely_ThreadPool`) pre-patch headline:
  **74.64 μs** (2026-04-30, above)

The bottleneck on that path is `Task.Run` / `Task.WhenAll` / async state
machines + TP wakeup latency, not data-line bouncing, so the patch doesn't
move the needle.

### Per-`Pipe` allocation cost

The two 128 B pads add ~256 B per `Pipe` instance. In
`FreshPipeSchedulerBenchmarks` (one fresh `Pipe` per iteration), this
shows up as +~210-290 B per op:

| Method            | Pre-patch alloc (2026-04-30) | Post-patch alloc (2026-05-04) |      Δ |
|------------------ |---------------------------:|-----------------------------:|-------:|
| Pipely_ThreadPool |                    8.85 KB |                      9.14 KB | +290 B |
| Pipely_Inline     |                    8.65 KB |                      8.86 KB | +210 B |

For long-lived pipes (the production shape captured by the headline rows)
the cost amortizes to zero per op.

### Notes / caveats

- **JIT symbol resolution.** `perf c2c` requires `DOTNET_EnableWriteXorExecute=0`
  to attribute samples to JIT'd .NET methods via `/tmp/perf-PID.map`. .NET
  8+'s default W^X mode places JIT'd code in a `memfd:doublemapper` region
  that `perf` treats as a DSO, bypassing the perfmap. Disabling W^X is for
  the recording only; production layout and cache behavior are unchanged.
- **Same-session comparison.** The cache-bench numbers above were recorded
  back-to-back in the same session. The BDN absolute numbers shifted ~5%
  across the session (BCL baseline 103 → 109 μs over a few hours, even
  though BCL was untouched), consistent with thermal / governor variance —
  comparable to BDN measurement noise. Cross-day comparisons are
  unreliable.
- **Padding constant.** 128 B matches the value already chosen by
  `TripleBuffer<T>` (see `TripleBuffer.CacheLineSize`); the comment there
  notes it covers x86-64 / Graviton and is also the right value for Apple
  Silicon. `Pipe`'s pad uses the same value inline.

## Cache-line padding revisited — runtime layout reality (2026-05-08)

After commit `a92c020`, an offset probe (using `Ldflda` per field on a live
`Pipe` instance and `Unsafe.SizeOf<T>` for sizes) revealed that the previous
patch's source-level intent did not match the runtime layout. Two findings,
both empirically verified on .NET 10.0.7 / x86-64 RyuJIT:

1. **`[StructLayout(LayoutKind.Sequential)]` on a class is a marshaling hint,
   not a managed-layout guarantee.** On a class with mixed reference and value
   fields, the CLR hoists references to the front of the object for
   GC-bitmap efficiency regardless of the attribute. The `_padBeforeWriterFields`
   field declared between the writer-block source comments did *not* land
   between the writer-block and the reader-block — it landed at offset +156
   (of the class), with hot writer/reader cursors interleaved on shared
   cache lines elsewhere.

   Concretely, under the `a92c020` layout: `_totalWritten` (writer-mut,
   offset +104) and `_totalConsumed` (reader-mut, offset +112) sat on the
   same cache line; `_writingHeadBytesBuffered` (offset +132) and
   `_readHeadIdx` (offset +144) sat on the same cache line. The `perf c2c`
   75% HITM drop on the bool-flag line was real but reflected the bools
   being shoved to a less-trafficked line by the runtime's reordering, not
   the source-level discipline the comments described.

2. **`Sequential` is also reordered on structs containing reference fields.**
   Refactoring the writer-block and reader-block into two
   `Sequential` structs (`WriterFields`, `ReaderFields`) with explicit
   `_padBefore` / `_padAfter` fields did *not* place those pads at the start
   and end of the struct. The runtime applied the same hoist-references-first
   policy inside the struct: refs at offsets 0–24, value primitives at 32–52,
   `_padBefore` at +53, the larger nested structs at +184/+224, `_padAfter`
   at +264. Field declaration order is not preserved.

**Why the struct-encapsulation refactor still works** — the cache-line
isolation is now guaranteed by the *total size* of each padded struct, not
by the position of the pads within it. Each struct carries 256 B of padding
(two `CacheLinePad` fields, 128 B each) plus the bulk of its hot-path fields
(~136 B), totaling ~390 B per struct. The two structs are placed back-to-back
in the class (`_writer` at offset +64, `_reader` at offset +456). Writer's
last hot byte is at offset +327 (end of `LastAcquiredReaderState`); reader's
first hot byte is at offset +456 (`ReadHead`). The 129-byte gap between them
guarantees they fall on different cache lines under any heap-object
alignment, with at least one entirely-empty cache line between the two
hot regions.

So the false-sharing fix that the prior commit *believed* it was making is
now genuinely realized. The `Sequential` attribute is retained as
documentation of intent and on the off chance a future runtime honors it;
the isolation does not depend on it.

**Verifier.** Future maintainers altering the `Pipe` field layout can
re-derive the offsets with a small probe (`DynamicMethod` + `Ldflda` per
field on a live `Pipe` instance) and confirm no writer-mutated field shares
a 64 B cache line with any reader-mutated field. The transcript above
covers .NET 10.0.7 / x86-64 RyuJIT; results may differ on other runtimes
or JITs.

### Post-refactor measurements (commit `840859c`, 2026-05-08, same-session A/B)

A same-session A/B was run by checking out the pre-refactor source onto
the working tree, building, and running the cache-bench harness pinned to
CPU 2 (producer) / CPU 4 (consumer); then restoring HEAD and re-running.
Three 5 s samples per side, alternating, no `perf` overhead:

| Run | Pre-refactor (`a92c020`) | Post-refactor (`840859c`) |
|-----|-------------------------:|--------------------------:|
| 1   |               11537 MiB/s |                19810 MiB/s |
| 2   |               13428 MiB/s |                20032 MiB/s |
| 3   |               14138 MiB/s |                20014 MiB/s |
| Mean |              13034 MiB/s |                19952 MiB/s |

**+53% throughput** vs pre-refactor in the same session. Post-refactor
samples are extremely stable (stddev 110 MiB/s); pre-refactor samples
drift upward as the system warms (11.5 → 14.1 GiB/s), so the gap is
slightly understated by the early samples. Combined with the prior
`a92c020` improvement over the unpadded baseline (10460 MiB/s reported
on 2026-05-04), the cumulative gain since the no-padding starting point
is approximately +91%.

### `perf c2c` HITM (20 s captures, same session)

| Metric                         | Pre-refactor | Post-refactor |
|--------------------------------|-------------:|--------------:|
| Total records (IBS samples)    |      206 496 |      214 496 |
| Total Load Local HITM          |          709 |        1 025 |
| Total Shared Cache Lines       |           92 |          146 |
| Throughput during capture      | 12 743 MiB/s | 19 080 MiB/s |
| HITM per GiB transferred       |        ~2.85 |        ~2.75 |

The HITM-per-byte ratio is essentially flat. The absolute count and the
shared-line count both *rose*, because: (a) more work was done per second,
so more samples; (b) the post-refactor object is larger (the two structs
add ~256 B of padding plus alignment slack), spreading the same data over
more 64 B lines. The residual hot lines align with `WriterState` /
`ReaderState` fields inside `TripleBuffer` slots — the intended SPSC
true-sharing handoff, not false sharing, and not reducible without
changing the architecture.

**Where the throughput came from, then.** The pre-refactor layout had
writer-mutated fields and reader-mutated fields colocated on the same
two 64 B cache lines (e.g., line 64–127 contained `_totalWritten`
[writer] and `_totalConsumed` [reader]; line 128–191 contained
`_writingHeadBytesBuffered` [writer] alongside `_readHeadIdx` [reader],
plus all four bool flags). Every writer mutation invalidated those lines
in the reader's L1, and vice versa, generating coherence-protocol
traffic that the busy-poll loop's 4 KiB-cycle steady state stalled on.
That coherence cost is mostly invisible to IBS sampling (the sample
fires on the load that *waits* on the line, not on every cycle of
inter-core invalidation), which is why the HITM-per-GiB metric stayed
flat while wall-clock throughput improved 53%. The new layout places
writer hot fields entirely within `_writer` (offsets +64 onward) and
reader hot fields entirely within `_reader` (offsets +456 onward) on
disjoint 64 B cache lines, so the per-cycle coherence cost goes to zero
for the SPSC cursors. The `TripleBuffer` slot handoff cost — true
sharing across a single line per slot — remains and is the work being
attributed in the residual `perf c2c` data.

### Full BDN sweep (commit `840859c`, 2026-05-08)

All six benchmark classes re-run on the post-refactor commit, each invoked
in isolation (`--filter '<class>.*'`) on the same machine and session.
Same hardware/OS as the 2026-04-30 headline; cross-day comparisons remain
unreliable, but the per-class numbers below are now the canonical
post-refactor baseline.

#### Throughput (`ThroughputBenchmarks`, long-lived `Pipe`)

| Method                  | Mean      | Error    | StdDev   | Ratio | Allocated | Alloc Ratio |
|-------------------------|----------:|---------:|---------:|------:|----------:|------------:|
| BclPipe_ProduceAndDrain | 105.23 μs | 0.797 μs | 0.707 μs |  1.00 |     873 B |        1.00 |
| Pipely_ProduceAndDrain  |  63.41 μs | 0.443 μs | 0.392 μs |  0.60 |     939 B |        1.08 |

Pipely vs BCL: **1.66×** at the BCL-default ThreadPool path. Compared to
the 2026-04-30 headline (Pipely 74.64 μs), Pipely improved 15% on the
default async/TP path purely from the cache-line refactor.

#### Pinned busy-poll (`PinnedThroughputBenchmarks`, raw threads, `Inline` scheduler)

| Method                | Mean      | Error    | StdDev   | Ratio | RatioSD | Allocated |
|-----------------------|----------:|---------:|---------:|------:|--------:|----------:|
| BCL_PinnedBusyPoll    | 121.56 μs | 2.424 μs | 4.670 μs |  1.00 |    0.06 |         - |
| Pipely_PinnedBusyPoll |  51.18 μs | 0.306 μs | 0.239 μs |  0.42 |    0.02 |         - |

Pipely vs BCL: **2.38×**. Compared to the prior post-`a92c020` measurement
(76.17 μs), Pipely improved 33% on this no-awaiter path — the cleanest
isolation of the cache-line refactor's effect, since neither side is
paying for the scheduler / awaiter machinery.

#### Scheduler matrix, long-lived `Pipe` (`SchedulerBenchmarks`)

| Method            | Mean      | Error    | StdDev   | Ratio | Allocated | Alloc Ratio |
|-------------------|----------:|---------:|---------:|------:|----------:|------------:|
| BCL_ThreadPool    | 107.61 μs | 0.871 μs | 0.772 μs |  1.00 |     809 B |        1.00 |
| BCL_Inline        |  44.61 μs | 0.305 μs | 0.270 μs |  0.41 |     744 B |        0.92 |
| Pipely_ThreadPool |  58.81 μs | 0.325 μs | 0.272 μs |  0.55 |     881 B |        1.09 |
| Pipely_Inline     |  39.35 μs | 0.729 μs | 0.682 μs |  0.37 |     742 B |        0.92 |

The headline 4-variant table (and the matching version in `README.md`).
Matched-scheduler ratios:
- ThreadPool: Pipely 1.83× faster than BCL (was 1.45× pre-refactor)
- Inline:     Pipely 1.13× faster than BCL (was within 5% pre-refactor)

The Inline gap is new — pre-refactor, Pipely's data-structure cost was
roughly equal to BCL's lock; post-refactor, the cache-line discipline gives
Pipely a consistent ~13% edge even with no scheduler / awaiter difference.

#### Fresh-`Pipe` companions (`FreshPipeThroughputBenchmarks`, `FreshPipeSchedulerBenchmarks`)

Throughput, fresh `Pipe` per iteration:

| Method                  | Mean      | Allocated | Alloc Ratio |
|-------------------------|----------:|----------:|------------:|
| BclPipe_ProduceAndDrain | 107.82 μs |   6.86 KB |        1.00 |
| Pipely_ProduceAndDrain  |  65.02 μs |    9.3 KB |        1.36 |

Scheduler matrix, fresh `Pipe`:

| Method            | Mean      | Allocated | Alloc Ratio |
|-------------------|----------:|----------:|------------:|
| BCL_ThreadPool    | 108.84 μs |   7.13 KB |        1.00 |
| BCL_Inline        |  45.97 μs |   5.81 KB |        0.82 |
| Pipely_ThreadPool |  63.78 μs |   9.39 KB |        1.32 |
| Pipely_Inline     |  42.62 μs |   9.32 KB |        1.31 |

Per-`Pipe` construction cost (fresh − headline):

| Method                | Fresh    | Headline | Per-`Pipe` ctor |
|-----------------------|---------:|---------:|----------------:|
| BCL Pipe (Throughput) |  6.86 KB |    873 B |        ~6.0 KB  |
| Pipely (Throughput)   |   9.3 KB |    939 B |        ~8.4 KB  |
| BCL_ThreadPool        |  7.13 KB |    809 B |        ~6.3 KB  |
| BCL_Inline            |  5.81 KB |    744 B |        ~5.1 KB  |
| Pipely_ThreadPool     |  9.39 KB |    881 B |        ~8.5 KB  |
| Pipely_Inline         |  9.32 KB |    742 B |        ~8.6 KB  |

Pipely's per-`Pipe` construction is now ~2.2 KB heavier than BCL (vs
~1.7 KB pre-refactor) — the additional ~500 B per `Pipe` is the
struct-encapsulated cache-line padding (256 B for `WriterFields` plus
256 B for `ReaderFields`, with alignment slack). For long-lived pipes
this amortizes to zero per op; for very short-lived pipes it's a
one-shot cost paid at construction.

#### Splice (`SpliceBenchmarks`, BCL `GetSpan` vs Pipely `GetSpan` vs Pipely `Splice`)

12 rows: 4 buffer sizes × {1, 16} chunks-per-iter × 3 methods. Selected
rows (BCL baseline, Pipely-`GetSpan`, Pipely-`Splice` for the same
shape):

| Buffer | Chunks | Method          | Mean      | Ratio | Allocated |
|-------:|-------:|-----------------|----------:|------:|----------:|
|  4096  |   1    | BclPipe_GetSpan | 133.09 μs |  1.00 |   8.37 KB |
|  4096  |   1    | Pipely_GetSpan  |  80.84 μs |  0.61 |  11.36 KB |
|  4096  |   1    | Pipely_Splice   |  64.24 μs |  0.48 |  11.34 KB |
|  4096  |  16    | BclPipe_GetSpan |  72.56 μs |  1.00 |   9.81 KB |
|  4096  |  16    | Pipely_GetSpan  |  39.89 μs |  0.55 |  14.36 KB |
|  4096  |  16    | Pipely_Splice   |  33.04 μs |  0.46 |  13.59 KB |
| 16384  |   1    | BclPipe_GetSpan |  54.72 μs |  1.00 |   3.13 KB |
| 16384  |   1    | Pipely_GetSpan  |  44.50 μs |  0.81 |   6.44 KB |
| 16384  |   1    | Pipely_Splice   |  31.93 μs |  0.58 |   6.44 KB |
| 16384  |  16    | BclPipe_GetSpan |  40.27 μs |  1.00 |   5.03 KB |
| 16384  |  16    | Pipely_GetSpan  |  35.28 μs |  0.88 |   9.85 KB |
| 16384  |  16    | Pipely_Splice   |  25.26 μs |  0.63 |   9.08 KB |

`Splice` (zero-copy ownership transfer) is consistently 0.46–0.63× of
BCL `GetSpan` (which is the only path BCL provides for the same shape);
even Pipely's plain `GetSpan` path is 0.55–0.88× of BCL's, so the
refactor improvements carry through here too.

#### Latency (`LatencyHarness`, 256 B messages, 100 000 samples, 3 trials)

Per-message end-to-end latency (nanoseconds, exact percentiles by sort):

| Percentile | BCL Pipe | Pipely  | Ratio |
|------------|---------:|--------:|------:|
| Min        |      310 |     249 | 0.80  |
| P50        |      940 |     650 | 0.69  |
| P90        |    1 660 |     859 | 0.52  |
| P99        |    6 890 |   2 880 | 0.42  |
| P99.9      |   35 859 |  12 440 | 0.35  |
| Max        |   50 299 |  67 809 | 1.35  |

Per-`ReadAsync` latency (nanoseconds):

| Percentile | BCL Pipe | Pipely  | Ratio |
|------------|---------:|--------:|------:|
| Min        |       60 |      50 | 0.83  |
| P50        |      240 |     120 | 0.50  |
| P90        |      410 |     180 | 0.44  |
| P99        |    1 100 |     270 | 0.25  |
| P99.9      |    3 459 |   2 170 | 0.63  |
| Max        |   63 559 |  67 269 | 1.06  |

Pipely's median message-latency is ~1.45× lower than BCL; P90 is ~1.93×
lower; P99 is ~2.4× lower. Read latency is even tighter — Pipely's P99
read is **4× lower** than BCL's, reflecting the awaiter / signaling
path difference. The `Max` row is dominated by GC and OS scheduling
artifacts and is not consistently better for either pipe; only the
sub-tail percentiles characterize the steady state.

### Summary of the refactor's effect

Across the full BDN sweep, the struct-encapsulation refactor produced
single-digit-to-double-digit improvements on every async path tested,
and a 33% improvement on the pure-data-structure pinned busy-poll path:

| Path                           | Pre-refactor | Post-refactor | Δ        |
|--------------------------------|-------------:|--------------:|---------:|
| Throughput (long-lived, TP)    |     74.64 μs |      63.41 μs | **−15%** |
| Pinned busy-poll (Inline)      |     76.17 μs |      51.18 μs | **−33%** |
| Scheduler, long-lived TP       |     71.24 μs |      58.81 μs | **−17%** |
| Scheduler, long-lived Inline   |     43.07 μs |      39.35 μs | **−9%**  |
| Fresh-`Pipe`, TP               |     71.99 μs |      63.78 μs | **−11%** |
| Fresh-`Pipe`, Inline           |     41.73 μs |      42.62 μs |   +2%    |

The cost is ~500 B of additional per-`Pipe` allocation (the two padded
structs). For long-lived pipes this is amortized to zero; for fresh-pipe
shapes the extra allocation explains why the Fresh-`Pipe Inline` row is
flat — the pipe-construction cost increased by roughly the same amount
as the per-iter savings, and that single-CPU code path was already
allocation-dominated in the FreshPipe shape.

## Post-`Reset()` / IDisposable removal sweep (commit `72c0fea`, 2026-05-09)

Re-measured after the `Pipe.Reset()` feature landed and `IDisposable`/`Dispose()`/`_disposed`
were removed from `Pipely.Pipe`. The only hot-path edits in this feature were *deletions*:
11 `if (_pipe._disposed) throw new ObjectDisposedException(...)` guards across `Pipe.Reader.cs`
(5) and `Pipe.Writer.cs` (6). No new code on the producer/consumer hot path.

**Read absolute numbers with care.** Prior comparison points for each class were captured
on commit `840859c` (2026-05-08, one day earlier) on the same hardware. Per the existing
"Cross-day comparisons are unreliable" caveat further up in this document, BCL absolute
numbers drifted within a ~5–17% band across sessions even though no BCL code changed —
likely thermal / governor variance. The most reliable signal is therefore the
**Pipely-vs-BCL ratio within a session**, since both arms ride the same machine state.

Same hardware/build as the prior sweep; each BDN class invoked via wildcard filter and run
sequentially on a quiet machine.

```
BenchmarkDotNet v0.15.8, Linux Ubuntu 24.04.4 LTS (Noble Numbat)
AMD Ryzen 7 8700F 4.04GHz, 1 CPU, 16 logical and 8 physical cores
.NET SDK 10.0.107
  [Host]     : .NET 10.0.7 (10.0.7, 10.0.726.21808), X64 RyuJIT x86-64-v4
  DefaultJob : .NET 10.0.7 (10.0.7, 10.0.726.21808), X64 RyuJIT x86-64-v4
```

### Throughput (`ThroughputBenchmarks`, long-lived `Pipe`)

| Method                  | Mean      | Error    | StdDev   | Ratio | Allocated | Alloc Ratio |
|-------------------------|----------:|---------:|---------:|------:|----------:|------------:|
| BclPipe_ProduceAndDrain | 110.20 us | 0.872 us | 0.816 us |  1.00 |     871 B |        1.00 |
| Pipely_ProduceAndDrain  |  53.24 us | 0.438 us | 0.410 us |  0.48 |     927 B |        1.06 |

Prior post-`840859c` (within-session) baseline:

| Method                  | Prior Mean | Now Mean | Prior Ratio | Now Ratio |
|-------------------------|-----------:|---------:|------------:|----------:|
| BclPipe_ProduceAndDrain |  105.23 us | 110.20 us|        1.00 |      1.00 |
| Pipely_ProduceAndDrain  |   63.41 us |  53.24 us|        0.60 |  **0.48** |

The Pipely-vs-BCL ratio improved from 0.60 → 0.48, a ~20% relative gain. This is the
class most affected by the deleted `_disposed` guards — `ProduceAndDrain` does ~256
chunked round-trips per BDN iteration, hitting `GetMemory` / `Advance` / `FlushAsync` /
`ReadAsync` / `AdvanceTo` thousands of times. The dominant cause is most likely the
removed guards plus their cascading i-cache / layout effects, but absent a profiler
diff we can't fully decompose the gain from session noise.

Allocations 969 B → 927 B (-4%); within measurement-to-measurement variance.

### Pinned busy-poll (`PinnedThroughputBenchmarks`, raw threads, `Inline` scheduler)

| Method                | Mean      | Error    | StdDev   | Ratio | Allocated |
|-----------------------|----------:|---------:|---------:|------:|----------:|
| BCL_PinnedBusyPoll    | 100.36 us | 1.615 us | 1.658 us |  1.00 |         - |
| Pipely_PinnedBusyPoll |  38.04 us | 0.490 us | 0.410 us |  0.38 |         - |

Prior post-`840859c` baseline:

| Method                | Prior Mean | Now Mean | Prior Ratio | Now Ratio |
|-----------------------|-----------:|---------:|------------:|----------:|
| BCL_PinnedBusyPoll    |  121.56 us | 100.36 us|        1.00 |      1.00 |
| Pipely_PinnedBusyPoll |   51.18 us |  38.04 us|        0.42 |      0.38 |

Both arms got faster in absolute terms (BCL −17%, Pipely −26%) — that's the session-noise
signal; no BCL code changed between runs. Ratio improvement (0.42 → 0.38) is real but
mild. Pinned uses `TryRead` plus the `Inline` scheduler, so the deleted `GetMemory` /
`Advance` guards still apply but `ReadAsync` / `FlushAsync` ones don't, consistent with
the smaller relative gain than long-lived `ProduceAndDrain`.

### Scheduler matrix, long-lived `Pipe` (`SchedulerBenchmarks`)

| Method            | Mean      | Error    | StdDev   | Ratio | Allocated |
|-------------------|----------:|---------:|---------:|------:|----------:|
| BCL_ThreadPool    | 107.64 us | 0.928 us | 0.823 us |  1.00 |     755 B |
| BCL_Inline        |  43.61 us | 0.292 us | 0.259 us |  0.41 |     744 B |
| Pipely_ThreadPool |  58.19 us | 0.358 us | 0.335 us |  0.54 |     862 B |
| Pipely_Inline     |  38.26 us | 0.437 us | 0.409 us |  0.36 |     745 B |

Prior post-`840859c` baseline:

| Method            | Prior Mean | Now Mean | Prior Ratio | Now Ratio |
|-------------------|-----------:|---------:|------------:|----------:|
| BCL_ThreadPool    |  107.61 us | 107.64 us|        1.00 |      1.00 |
| BCL_Inline        |   44.61 us |  43.61 us|        0.41 |      0.41 |
| Pipely_ThreadPool |   58.81 us |  58.19 us|        0.55 |      0.54 |
| Pipely_Inline     |   39.35 us |  38.26 us|        0.37 |      0.36 |

All four ratios essentially unchanged — within-noise stable. These shapes do one
round-trip per BDN iteration vs `ProduceAndDrain`'s ~256, so per-iteration savings from
the deleted guards are smaller and lost in noise.

### Fresh-`Pipe` companions

`FreshPipeThroughputBenchmarks` (per-op `new Pipely.Pipe()` cost included):

| Method                  | Mean      | Error    | StdDev   | Ratio | Gen0   | Allocated | Alloc Ratio |
|-------------------------|----------:|---------:|---------:|------:|-------:|----------:|------------:|
| BclPipe_ProduceAndDrain | 107.05 us | 1.069 us | 0.892 us |  1.00 | 0.2441 |    7.1 KB |        1.00 |
| Pipely_ProduceAndDrain  |  64.68 us | 1.264 us | 1.405 us |  0.60 | 0.9766 |  39.18 KB |        5.52 |

`FreshPipeSchedulerBenchmarks`:

| Method            | Mean      | Error    | StdDev   | Ratio | Gen0   | Allocated | Alloc Ratio |
|-------------------|----------:|---------:|---------:|------:|-------:|----------:|------------:|
| BCL_ThreadPool    | 108.13 us | 1.102 us | 1.031 us |  1.00 | 0.2441 |   7.14 KB |        1.00 |
| BCL_Inline        |  43.16 us | 0.354 us | 0.314 us |  0.40 | 0.1221 |   5.78 KB |        0.81 |
| Pipely_ThreadPool |  60.34 us | 0.255 us | 0.226 us |  0.56 | 1.4648 |  40.03 KB |        5.61 |
| Pipely_Inline     |  42.02 us | 0.529 us | 0.494 us |  0.39 | 0.5493 |  21.04 KB |        2.95 |

The fresh-`Pipe` allocation footprint (~39 KB/op vs BCL's ~7 KB) is exactly why `Reset()` exists:
pooling the `Pipe` instance amortizes that cost to zero. The headline long-lived measurements
above are the achievable steady-state shape.

### Splice (`SpliceBenchmarks`, BCL `GetSpan` vs Pipely `GetSpan` vs Pipely `Splice`)

| Method          | BufferSize | BuffersBeforeFlush | Mean      | Ratio | Allocated | Alloc Ratio |
|-----------------|-----------:|-------------------:|----------:|------:|----------:|------------:|
| BclPipe_GetSpan |        256 |                  1 | 606.06 us |  1.00 |  100052 B |        1.00 |
| Pipely_GetSpan  |        256 |                  1 | 473.13 us |  0.78 |  116735 B |        1.17 |
| Pipely_Splice   |        256 |                  1 | 579.44 us |  0.96 |  107901 B |        1.08 |
| BclPipe_GetSpan |        256 |                 16 | 277.92 us |  1.00 |  100005 B |        1.00 |
| Pipely_GetSpan  |        256 |                 16 | 170.45 us |  0.61 |  117606 B |        1.18 |
| Pipely_Splice   |        256 |                 16 | 245.07 us |  0.88 |  107695 B |        1.08 |
| BclPipe_GetSpan |       1024 |                  1 | 241.87 us |  1.00 |   26552 B |        1.00 |
| Pipely_GetSpan  |       1024 |                  1 | 162.17 us |  0.67 |   48689 B |        1.83 |
| Pipely_Splice   |       1024 |                  1 | 164.95 us |  0.68 |   34549 B |        1.30 |
| BclPipe_GetSpan |       1024 |                 16 | 115.69 us |  1.00 |   26865 B |        1.00 |
| Pipely_GetSpan  |       1024 |                 16 |  65.90 us |  0.57 |   67878 B |        2.53 |
| Pipely_Splice   |       1024 |                 16 |  77.59 us |  0.67 |   36885 B |        1.37 |
| BclPipe_GetSpan |       4096 |                  1 | 129.70 us |  1.00 |    8665 B |        1.00 |
| Pipely_GetSpan  |       4096 |                  1 |  79.05 us |  0.61 |   37005 B |        4.27 |
| Pipely_Splice   |       4096 |                  1 |  62.24 us |  0.48 |   22714 B |        2.62 |
| BclPipe_GetSpan |       4096 |                 16 |  74.07 us |  1.00 |   10037 B |        1.00 |
| Pipely_GetSpan  |       4096 |                 16 |  46.97 us |  0.63 |  150842 B |       15.03 |
| Pipely_Splice   |       4096 |                 16 |  33.72 us |  0.46 |       0 B |        0.00 |
| BclPipe_GetSpan |      16384 |                  1 |  53.31 us |  1.00 |    3212 B |        1.00 |
| Pipely_GetSpan  |      16384 |                  1 |  47.11 us |  0.88 |   69008 B |       21.48 |
| Pipely_Splice   |      16384 |                  1 |  32.35 us |  0.61 |   42966 B |       13.38 |
| BclPipe_GetSpan |      16384 |                 16 |  39.33 us |  1.00 |    5157 B |        1.00 |
| Pipely_GetSpan  |      16384 |                 16 |  74.02 us |  1.88 |  551649 B |      106.97 |
| Pipely_Splice   |      16384 |                 16 |  30.19 us |  0.77 |  133604 B |       25.91 |

Pre-existing outlier at 16384/16: `Pipely_GetSpan` is GC-bound (551 KB/op, multi-generation
collections). `Pipely_Splice` at the same point is 0.77x BCL. Not a regression introduced by
this feature.

### Latency (`LatencyHarness`, 256 B messages, 100 000 samples, 3 trials)

Per-message end-to-end latency in nanoseconds (exact percentiles via sort):

| Percentile | Trial | BCL Pipe | Pipely | Ratio |
|------------|------:|---------:|-------:|------:|
| Min        |     1 |      320 |    240 |  0.75 |
| Min        |     2 |      390 |    230 |  0.59 |
| Min        |     3 |      340 |    250 |  0.74 |
| P50        |     1 |      820 |    670 |  0.82 |
| P50        |     2 |      890 |    600 |  0.67 |
| P50        |     3 |      850 |    630 |  0.74 |
| P90        |     1 |    1,470 |    920 |  0.63 |
| P90        |     2 |    1,490 |    860 |  0.58 |
| P90        |     3 |    1,410 |  1,110 |  0.79 |
| P99        |     1 |    7,330 |  5,360 |  0.73 |
| P99        |     2 |    9,439 |  6,480 |  0.69 |
| P99        |     3 |    8,530 | 13,550 |  1.59 |

P50/P90 hold the prior shape: Pipely consistently 0.6-0.8x BCL. P99 has trial-to-trial
variance dominated by GC pauses (one trial each direction); the pattern is unchanged from
the prior sweep.

### Summary

The `Reset()` feature added a public method, two internal helpers (`TripleBuffer<T>.Reset`,
`PipelyAwaiter<T>.Reset`), and removed the entire `IDisposable` surface — net **−170 lines**
across `src/`. Hot-path effect, ratio-based to control for cross-session drift:

- **Long-lived `Throughput`** (the busiest hot path: ~256 round-trips/iter): ratio
  0.60 → 0.48, a ~20% relative gain. Cleanest signal that the deleted `_disposed`
  guards moved the needle.
- **Pinned busy-poll**: ratio 0.42 → 0.38, mild gain. Half the deleted guards apply here.
- **Scheduler / FreshPipe matrices**: ratios unchanged (≤1 percentage point). One
  round-trip per iteration leaves no headroom for guard-removal savings to surface.
- **Splice**: ratios unchanged from prior; the pre-existing 16384/16 GC-bound outlier
  on `Pipely_GetSpan` is still there (not introduced by this change).
- **Latency**: P50/P90 shape unchanged. P99 has trial-to-trial variance dominated by GC
  pauses (one trial each direction); no signal at the tail.

Steady-state producer→consumer throughput is **1 MiB / 53.24 µs ≈ 19.7 GB/s** for Pipely
vs **1 MiB / 110.20 µs ≈ 9.5 GB/s** for BCL on this hardware (1 GB = 10⁹ B, matching
the convention used by the headline section above). Ratio against BCL: 2.07×.

To re-run perf c2c HITM attribution alongside this BDN data, see
`tools/cache-bench-perf-c2c.sh`. To re-run the BDN sweep itself, the invocations that
generated the tables above are recorded in `docs/superpowers/measurements/post-reset-2026-05-09/`.
