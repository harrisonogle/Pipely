# SpscPipe.Benchmarks

Compares `SpscPipe` against `System.IO.Pipelines.Pipe` on throughput,
allocation/GC pressure, and per-flush latency.

Design: [`docs/superpowers/specs/2026-04-23-spscpipe-benchmarks-design.md`](../../docs/superpowers/specs/2026-04-23-spscpipe-benchmarks-design.md)

## Running

Build everything in Release before running benchmarks:

```bash
dotnet build -c Release
```

### BenchmarkDotNet (throughput + allocation)

```bash
dotnet run -c Release --project tests/SpscPipe.Benchmarks -- bdn
```

Add any BDN flag after `bdn`:

```bash
# List all benchmarks BDN sees (method-level)
dotnet run -c Release --project tests/SpscPipe.Benchmarks -- bdn --list flat

# Run only the chatty workload
dotnet run -c Release --project tests/SpscPipe.Benchmarks -- bdn --filter '*Chatty*'

# Single param slice (note: filter matches "ChunkSize: 4096" not "ChunkSize=4096")
dotnet run -c Release --project tests/SpscPipe.Benchmarks -- bdn \
    --filter '*Chatty*ChunkSize: 4096*'

# Faster iteration (single warmup, single iteration; lower-quality numbers)
dotnet run -c Release --project tests/SpscPipe.Benchmarks -- bdn \
    --warmupCount 1 --iterationCount 1 --invocationCount 1 --unrollFactor 1
```

Reports land in `BenchmarkDotNet.Artifacts/results/` (markdown, html, csv).

#### Reading the output

- **Mean** is the per-op time. One op transfers `1 MiB` total bytes through
  the pipe, so throughput is `1 MiB / Mean`. (BDN doesn't compute this for
  you — divide manually.)
- **Allocated** (from `[MemoryDiagnoser]`) is bytes/op. SpscPipe targets
  zero steady-state allocation (spec §1.1). Caveat: when running with
  `InvocationCount=1` (BDN's default for fast benchmarks), each op starts
  with a freshly-built pipe — the pool is cold, so first-touch buffer
  rentals show up in `Allocated`. To see steady-state numbers, increase
  invocation count or warm with a longer iteration.
- **Lock Contentions** (from `[ThreadingDiagnoser]`) — expected 0 for
  SpscPipe, non-zero for BCL `Pipe` (which uses `SyncObject` per spec §1.3).
- **Backpressure** rows use Pause=4096 / Resume=2048 to force frequent
  park/wake cycles; the other rows use BCL defaults (Pause=65536,
  Resume=32768).

### Latency harness

```bash
dotnet run -c Release --project tests/SpscPipe.Benchmarks -- latency
```

Defaults: 1 000 000 messages per cell, both impls, both modes (`normal`
and `backpressure`), all five MessageSizes. Total runtime: ~1 minute.

Flags:

```
--messages N                          messages per cell (default 1_000_000)
--mode normal|backpressure|both       default both
--impl spsc|bcl|both                  default both
--sizes 16,64,256,4096,65536          default all (each must be >= 8)
```

Output is markdown tables on stdout — pipe to a file with `> results.md`
if you want to keep them.

#### Reading the output

- All numbers are nanoseconds.
- "Latency" = time from `Stopwatch.GetTimestamp()` at the producer's
  Advance/Flush boundary to `Stopwatch.GetTimestamp()` at the consumer's
  per-message read.
- Under `normal` mode, recorded latency includes queueing time when the
  producer outpaces the consumer (low MessageSize). The number is real
  but it measures queueing+handoff, not handoff alone.
- Under `backpressure` mode, the tight Pause threshold limits queue
  depth; recorded latency reflects the park/wake round-trip cost.
- The `max` column may be slightly *less* than `p99.9` — `max` is the
  exact recorded value, while `p99.9` is the upper edge of the bucket
  containing the 99.9th percentile (a HdrHistogram-style quirk; not a bug).

## Known limitations

- **No bytes written into buffers.** Both impls are byte-agnostic on the
  write path, so omitting writes is fair, but a memory-bound consumer
  in production would see different relative numbers.
- **Single scheduler.** Only the default `ThreadPool` scheduler is
  exercised. SpscPipe does not yet honor `ReaderScheduler`/`WriterScheduler`
  (see `FOLLOWUPS.md` item 1).
- **Single threshold matrix.** Apples-to-apples on BCL defaults; the
  threshold-related `[Params]` are declared with single values so adding
  more is a one-line change.
- **Single machine.** No CI publication or cross-platform results.
