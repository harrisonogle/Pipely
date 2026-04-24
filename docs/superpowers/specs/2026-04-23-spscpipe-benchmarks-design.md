# SpscPipe vs. `System.IO.Pipelines.Pipe` — Benchmark Design

**Date:** 2026-04-23
**Status:** Design — pending implementation plan
**Scope:** New `tests/SpscPipe.Benchmarks` project comparing `SpscPipe` against `System.IO.Pipelines.Pipe` along throughput, allocation, and per-flush latency axes.

---

## 1. Goals

- Compare `SpscPipe` against the BCL `Pipe` on three axes: steady-state throughput, allocation/GC pressure, and per-flush latency (including tail percentiles).
- Cover three workload shapes that exercise distinct paths: bulk streaming, chatty small flushes, and backpressure-bound (writer parks frequently).
- Apples-to-apples by default: both pipes configured with the BCL `PipeOptions` defaults (`MinimumSegmentSize=4096`, `PauseWriterThreshold=65536`, `ResumeWriterThreshold=32768`), default `ThreadPool` scheduler.
- Reproducible: deterministic per-iteration setup, fixed total work per benchmark op, fixed message count per latency cell.

## 2. Non-goals

- Multi-producer / multi-consumer scenarios. SpscPipe is SPSC by contract (spec §1.2).
- Comparing against alternative scheduler configurations (`PipeScheduler.Inline`, custom). SpscPipe's options exist but aren't yet wired to a scheduler (FOLLOWUPS.md item 1) — Inline comparison would not be apples-to-apples.
- Cross-platform numbers. We target the developer's local environment; CI publication is out of scope.
- Tuning recommendations. Benchmarks measure; they do not prescribe.
- Microbenchmarking individual methods (`GetMemory`, `Advance`, etc.) in isolation — we measure end-to-end producer/consumer paths.

## 3. Project layout

A single new project: **`tests/SpscPipe.Benchmarks`**, added to `SpscPipe.slnx` under the existing `tests/` folder.

```
tests/SpscPipe.Benchmarks/
  SpscPipe.Benchmarks.csproj      # net10.0, refs SpscPipe + BenchmarkDotNet
  Program.cs                       # mode dispatch on args[0]
  PipeAdapter.cs                   # IPipeAdapter + Spsc/Bcl impls
  Workloads.cs                     # producer/consumer task helpers
  ThroughputBenchmarks.cs          # BDN class
  LatencyHarness.cs                # console latency runner
  Histogram.cs                     # log-bucket percentile recorder
  README.md                        # how to run, what each metric means
```

`SpscPipe.Benchmarks.csproj`:

- TargetFramework `net10.0` (matches the rest of the solution).
- ImplicitUsings, Nullable enabled (matches the rest).
- PackageReference `BenchmarkDotNet` (latest stable).
- ProjectReference to `src/SpscPipe/SpscPipe.csproj`.
- `<OutputType>Exe</OutputType>`.

## 4. Entry point and modes

`Program.cs` dispatches on the first arg:

```
dotnet run -c Release --project tests/SpscPipe.Benchmarks -- bdn      # BenchmarkDotNet
dotnet run -c Release --project tests/SpscPipe.Benchmarks -- latency  # latency harness
dotnet run -c Release --project tests/SpscPipe.Benchmarks              # prints usage
```

`bdn` mode forwards remaining args to `BenchmarkSwitcher.FromAssembly(...).Run(args)` so BDN's own filters/flags work (`--filter`, `--list`, etc.).

`latency` mode parses its own flags (see §6.3).

## 5. Shared model

### 5.1 `IPipeAdapter`

Both implementations are accessed through a tiny adapter interface so workload code is implementation-agnostic.

```csharp
internal interface IPipeAdapter : IDisposable
{
    PipeReader Reader { get; }
    PipeWriter Writer { get; }
    void ResetOrRebuild();   // SpscPipe.Reset() for Spsc; rebuild for BCL
}
```

Two concrete adapters:

- `SpscPipeAdapter` — wraps a `SpscPipe.SpscPipe` with the configured `SpscPipeOptions`. `ResetOrRebuild` calls `pipe.Reset()`.
- `BclPipeAdapter` — wraps a `System.IO.Pipelines.Pipe` with the configured `PipeOptions`. `ResetOrRebuild` disposes (where applicable; BCL `Pipe` has no public Reset that mirrors SpscPipe's contract — we rebuild a new instance and let the previous get GC'd between iterations) and constructs a new instance.

The adapter takes its config from a small struct populated by the BDN/latency caller:

```csharp
internal readonly record struct PipeConfig(
    int MinimumSegmentSize,
    long PauseWriterThreshold,
    long ResumeWriterThreshold);
```

### 5.2 Workload helpers

`Workloads.cs` provides three async producer/consumer pairs, parameterized by the adapter and per-workload knobs. They write *no* data bytes — only timestamps in the latency mode (§6.3). The throughput workloads `Advance(ChunkSize)` past an unwritten span.

**Bulk producer:**
```csharp
internal static async Task BulkProducer(IPipeAdapter pipe, int chunkSize,
                                         int batchPerFlush, long totalBytes)
{
    long written = 0;
    while (written < totalBytes) {
        for (int i = 0; i < batchPerFlush && written < totalBytes; i++) {
            pipe.Writer.GetMemory(chunkSize);
            pipe.Writer.Advance(chunkSize);
            written += chunkSize;
        }
        var fr = await pipe.Writer.FlushAsync();
        if (fr.IsCompleted || fr.IsCanceled) break;
    }
    pipe.Writer.Complete();
}
```

**Chatty producer:** identical but `batchPerFlush = 1`.

**Common consumer:**
```csharp
internal static async Task DrainConsumer(IPipeAdapter pipe)
{
    while (true) {
        var rr = await pipe.Reader.ReadAsync();
        if (rr.IsCanceled) break;
        pipe.Reader.AdvanceTo(rr.Buffer.End);
        if (rr.IsCompleted && rr.Buffer.IsEmpty) break;
    }
    pipe.Reader.Complete();
}
```

A "no bytes are written" choice is deliberate: both implementations are byte-agnostic on the write path, so writing bytes adds the same memcpy cost on both sides and compresses the *relative* gap. We record this as a known limitation (§9): real-world workloads do touch bytes; pure-overhead numbers may overstate the relative SpscPipe advantage on memory-bound consumers. A future `TouchBytes` knob can be added without disturbing the rest of the design.

## 6. Benchmarks

### 6.1 BDN throughput + allocation matrix

`ThroughputBenchmarks.cs`:

```csharp
[MemoryDiagnoser]
[ThreadingDiagnoser]
[SimpleJob(RuntimeMoniker.Net100, warmupCount: 3, iterationCount: 5)]
public class ThroughputBenchmarks
{
    public enum Impl { SpscPipe, BclPipe }

    [Params(Impl.SpscPipe, Impl.BclPipe)] public Impl Pipe;
    [Params(1, 16, 256, 4096, 65536)]      public int ChunkSize;
    [Params(4096)]                         public int MinimumSegmentSize;
    [Params(65536)]                        public long PauseWriterThreshold;
    [Params(32768)]                        public long ResumeWriterThreshold;

    private const long TotalBytesPerOp = 1L << 20;   // 1 MiB
    private const int  BatchPerFlushBulk = 16;

    private IPipeAdapter _pipe = null!;

    [IterationSetup(Targets = new[] { nameof(Bulk), nameof(Chatty) })]
    public void SetupNormal() => _pipe = BuildAdapter(new PipeConfig(
        MinimumSegmentSize, PauseWriterThreshold, ResumeWriterThreshold));

    [IterationSetup(Target = nameof(Backpressure))]
    public void SetupBackpressure() => _pipe = BuildAdapter(new PipeConfig(
        MinimumSegmentSize, pauseWriterThreshold: 4096, resumeWriterThreshold: 2048));

    [IterationCleanup] public void Cleanup() { _pipe.Dispose(); }

    [Benchmark] public Task Bulk()         => Run(BatchPerFlushBulk);
    [Benchmark] public Task Chatty()       => Run(batchPerFlush: 1);
    [Benchmark] public Task Backpressure() => Run(BatchPerFlushBulk);  // pipe was built with tight thresholds in SetupBackpressure

    private async Task Run(int batchPerFlush)
    {
        var producer = Workloads.BulkProducer(_pipe, ChunkSize, batchPerFlush, TotalBytesPerOp);
        var consumer = Workloads.DrainConsumer(_pipe);
        await Task.WhenAll(producer, consumer);
    }
}
```

**Notes:**

- `Backpressure` reuses the bulk producer but with `Pause=4096, Resume=2048` so the writer parks frequently. The override happens in `SetupBackpressure` (untimed `[IterationSetup]`); the `[Params]` for Pause/Resume still reflect the BCL defaults used by `Bulk` and `Chatty`. The four `[Params]`-declared options are listed as `[Params]` with a single value each so it is trivial to widen later by adding values to the attribute.
- `BatchPerFlush=16` for `Bulk` — chosen so each flush carries `16 × ChunkSize` bytes; for `ChunkSize=4096` that's 64 KiB per flush, which is a realistic "fill a TCP send buffer then flush" cadence.
- `TotalBytesPerOp = 1 MiB` keeps the per-op time bounded across the full chunk-size sweep. At `ChunkSize=1`, that's 1M chunks per op — slow but tractable. At `ChunkSize=65536`, it's 16 chunks per op.
- `ThreadingDiagnoser` should report 0 lock contention for `SpscPipe` and non-zero for `BclPipe` (BCL `Pipe` uses a `SyncObject` lock per spec §1.3).

**Result count:** `2 × 5 × 1 × 1 × 1 × 3 = 30` rows. Total run time estimate: 5–15 minutes on a developer machine.

### 6.2 What BDN actually measures

- The `[Benchmark]` body runs the full producer + consumer to completion. BDN times the body, not individual pipe operations.
- Per-op time → throughput is `TotalBytesPerOp / mean op time`. We do this conversion in the README rather than a custom `[Column]` to keep the BDN class small.
- `[MemoryDiagnoser]` reports `Allocated` (bytes/op) and Gen0/1/2 collection counts. SpscPipe's "zero steady-state allocation after warmup" goal (spec §1.1) is directly visible: after the BDN warmup iterations, `Allocated` should be ~0 bytes/op for SpscPipe. BCL `Pipe`'s steady-state allocation cost shows in its own row.

### 6.3 Latency harness

`LatencyHarness.cs`. Console runner — no BDN. Output is a markdown table per scenario printed to stdout.

**Unit:** per-flush latency = `Stopwatch.GetTimestamp() at consumer - Stopwatch.GetTimestamp() at producer`. Each flush carries one logical message: an 8-byte timestamp prefix followed by `MessageSize - 8` bytes of unwritten payload (the producer `Advance`s past them; the consumer reads them only to skip over them).

**Producer:**
```csharp
while (count++ < N) {
    var span = pipe.Writer.GetSpan(MessageSize);
    BinaryPrimitives.WriteInt64LittleEndian(span, Stopwatch.GetTimestamp());
    pipe.Writer.Advance(MessageSize);
    var fr = await pipe.Writer.FlushAsync();
    if (fr.IsCompleted || fr.IsCanceled) break;
}
pipe.Writer.Complete();
```

**Consumer:**
```csharp
while (true) {
    var rr = await pipe.Reader.ReadAsync();
    var buffer = rr.Buffer;
    while (buffer.Length >= MessageSize) {
        var first = buffer.First.Span;   // may not contain full 8 bytes if mid-segment
        long ts = first.Length >= 8
            ? BinaryPrimitives.ReadInt64LittleEndian(first)
            : ReadAcrossSegments(buffer);  // fallback that copies into an 8-byte stackalloc
        histogram.Record(Stopwatch.GetTimestamp() - ts);
        buffer = buffer.Slice(MessageSize);
    }
    pipe.Reader.AdvanceTo(buffer.Start, rr.Buffer.End);
    if (rr.IsCompleted && buffer.IsEmpty) break;
}
pipe.Reader.Complete();
```

`ReadAcrossSegments` handles the case where the 8-byte timestamp spans two `ReadOnlySequence` segments — likely rare with `MinimumSegmentSize=4096` but must not crash.

**Cells:** for each `(Impl × MessageSize × Mode)` triple, record `N = 1_000_000` messages and compute p50/p90/p99/p99.9/max.

- `Impl ∈ {SpscPipe, BclPipe}`
- `MessageSize ∈ {16, 64, 256, 4096, 65536}` (drops `1` from the throughput matrix because the timestamp needs ≥8 bytes)
- `Mode ∈ {normal, backpressure}` — `normal` uses BCL defaults `Pause=65536, Resume=32768`; `backpressure` uses `Pause=4096, Resume=2048` to force frequent park/wake cycles

**Total cells:** `2 × 5 × 2 = 20`. At 1M messages/cell with ~1µs steady-state latency, each cell takes ~1s of measurement plus warmup; total runtime well under 1 minute.

**Histogram:** `Histogram.cs` is a simple log-bucket recorder: 64 buckets per power-of-2 from 1ns to 1s. No external dependency. Sufficient for percentile resolution at the scales we care about.

**Per-cell warmup:** before recording, run 10 000 messages through the pipe and discard the timestamps. Pays JIT and pool-rent costs out of band so the measured 1M-message window is steady-state.

**Producer pacing:** producer runs flat-out in both modes. Under `normal` with small `MessageSize`, the producer outpaces the consumer until the pipe holds many in-flight messages; recorded latency includes queueing time. This is documented in the README as load-induced latency, not a bug — the cell still reports a real number, it just measures queueing+handoff rather than handoff alone. Under `backpressure`, recorded latency reflects the park/wake round-trip cost.

**CLI flags:**
```
--messages N            messages per cell (default 1_000_000)
--mode normal|backpressure|both    (default both)
--impl  spsc|bcl|both              (default both)
--sizes 16,64,256,4096,65536       (default all)
```

**Output (one table per Mode):**

```
## Latency — normal (Pause=65536, Resume=32768)

| Impl     | MessageSize | p50 (ns) | p90  | p99   | p99.9  | max     |
| -------- | ----------: | -------: | ---: | ----: | -----: | ------: |
| SpscPipe |          16 |      230 |  410 | 1 200 |  9 400 | 142 000 |
| BclPipe  |          16 |      ... |  ... |   ... |    ... |     ... |
| ...
```

## 7. Reporting

- BDN writes its standard report (markdown + html + csv) to `BenchmarkDotNet.Artifacts/results/` — this is BDN's default and we do not customize it.
- Latency harness writes markdown to stdout and (with `--out path.md`) optionally to a file.
- A short `tests/SpscPipe.Benchmarks/README.md` explains: how to run each mode, how to interpret each column, and the known limitations from §9.

## 8. Risks

- **JIT / warmup variability.** BDN's warmup handles JIT for the throughput benchmarks. The latency harness runs its own warmup pass (per-cell, see §6.3).
- **ThreadPool starvation.** Both implementations dispatch continuations on the ThreadPool. On a busy machine, latency tails will reflect ThreadPool scheduling, not pipe overhead. Documented in the README; running in isolation is recommended.
- **GC interference in latency tails.** SpscPipe should produce ~0 allocations steady-state; BCL `Pipe` allocates more. Long-tail latency may correlate with Gen0 collections. Histogram captures this faithfully — we don't try to suppress GC.
- **`Reset` vs rebuild between iterations.** SpscPipe has a public `Reset()` (spec §10.5); BCL `Pipe`'s `Reset()` is restricted (both sides must have Completed first). Per-iteration teardown happens in `[IterationCleanup]` and rebuild in `[IterationSetup]`, both *outside* the BDN-timed body, so the choice doesn't affect the measured throughput numbers. SpscPipe uses `Reset()`; BCL builds a fresh `Pipe`. Documented here only because if either path leaks (e.g., a bug in `Reset` causes accumulating segment retention), it would skew steady-state allocation diagnostics across iterations — the `MemoryDiagnoser` numbers would still be per-op-honest but trends across runs could mislead.

## 9. Known limitations

- **No bytes written into buffers.** Both impls are byte-agnostic on the write path, so omitting writes is fair, but a memory-bound consumer in production would see different relative numbers. A `TouchBytes` knob can be added later without disturbing this design.
- **No `Pause=0` (unlimited) sweep.** Excluded for apples-to-apples; both impls support it (it is part of the BCL `PipeOptions` contract). The four threshold-related `[Params]` are declared so adding a second value is a one-line change.
- **No scheduler comparison.** Only the default `ThreadPool` scheduler is exercised. SpscPipe does not yet honor `ReaderScheduler`/`WriterScheduler` (FOLLOWUPS.md item 1); comparing against `Pipe` with `PipeScheduler.Inline` would not be apples-to-apples.
- **Single-machine, single-OS results.** No CI publication or cross-platform matrix; this is a developer-facing benchmark project, not a continuously-published dashboard.
- **Latency under `normal` mode includes queueing.** Producer is unpaced; under low `MessageSize` the queue fills and recorded latency is queueing+handoff. The `backpressure` mode constrains queue depth via the threshold and yields handoff-dominated numbers.

## 10. Out-of-scope future work

- `TouchBytes` knob as `[Params(false, true)]`.
- `Pause=0` sweep (add a second value to the `PauseWriterThreshold` `[Params]`).
- `MinimumSegmentSize` sweep (4096, 16384) once a baseline exists.
- Inline-scheduler comparison once SpscPipe wires up `ReaderScheduler`/`WriterScheduler` per FOLLOWUPS.md item 1.
- ARM64 numbers (FOLLOWUPS.md item 3 — separate effort).
