# SpscPipe Benchmarks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `tests/SpscPipe.Benchmarks` project that compares `SpscPipe` against `System.IO.Pipelines.Pipe` on throughput, allocation/GC, and per-flush latency (with tail percentiles).

**Architecture:** One new console project with two modes dispatched on `args[0]`. `bdn` runs BenchmarkDotNet with `[MemoryDiagnoser]` + `[ThreadingDiagnoser]` for throughput and allocation. `latency` runs a custom harness that embeds an 8-byte `Stopwatch.GetTimestamp()` in each flush and records p50/p90/p99/p99.9 with a log-bucket histogram. Both impls are accessed through a small `IPipeAdapter` so workload code is implementation-agnostic.

**Tech Stack:** .NET 10, C# 12, BenchmarkDotNet, `System.IO.Pipelines`, `System.Buffers.Binary`, `System.Diagnostics.Stopwatch`.

**Spec:** `docs/superpowers/specs/2026-04-23-spscpipe-benchmarks-design.md`

---

## File Structure

All files live under `tests/SpscPipe.Benchmarks/` unless noted.

| File | Responsibility |
|---|---|
| `SpscPipe.Benchmarks.csproj` | net10.0 console project; refs SpscPipe + BenchmarkDotNet |
| `Program.cs` | Mode dispatch on `args[0]` (`bdn` / `latency`); usage on no args |
| `PipeAdapter.cs` | `IPipeAdapter` interface + `SpscPipeAdapter` and `BclPipeAdapter`; `PipeConfig` record struct; `BuildAdapter(Impl, PipeConfig)` factory |
| `Workloads.cs` | `BulkProducer` (parameterized by batchPerFlush) and `DrainConsumer` async helpers |
| `ThroughputBenchmarks.cs` | BDN class with three `[Benchmark]` methods (Bulk/Chatty/Backpressure) and the params matrix |
| `LatencyHarness.cs` | Console runner: per-cell warmup, measurement loop, percentile reporter |
| `Histogram.cs` | Log-bucket histogram (64 buckets per power-of-2, 1ns–1s) with `Record`/`Percentile` |
| `README.md` | How to run each mode; how to read each column; documented limitations |
| `SpscPipe.slnx` *(repo root, modified)* | Add the new project under the `tests/` folder |

---

## Conventions used by tasks

- All commands are run from the repo root: `/home/harrison/src/sandbox/SpscPipe`.
- Tasks are TDD-shaped where a unit test makes sense (adapter, histogram, helpers). Pure plumbing (csproj, Program.cs dispatch) and end-to-end-only code (BDN class, latency harness) are smoke-tested by running them.
- Every task ends in a commit. Commit messages use the existing repo style (no body needed for trivial; body for meaty changes — see `git log --oneline` for tone). All commits include `Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>`.
- C# style: file-scoped namespaces (`namespace SpscPipe.Benchmarks;`), `internal` types unless they need to be public for BDN (BDN requires `[Benchmark]` methods and their params on `public` classes/properties).
- The `SpscPipe.Tests` project already references xunit. We will add tests for the new project's pure-logic helpers there to avoid adding a second test project. Use `internal` + `[InternalsVisibleTo]` from the benchmarks project to expose what tests need.

---

## Task 1: Create the benchmarks project skeleton and wire it into the solution

**Files:**
- Create: `tests/SpscPipe.Benchmarks/SpscPipe.Benchmarks.csproj`
- Create: `tests/SpscPipe.Benchmarks/Program.cs`
- Modify: `SpscPipe.slnx`

- [ ] **Step 1: Create the csproj**

Create `tests/SpscPipe.Benchmarks/SpscPipe.Benchmarks.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
    <ServerGarbageCollection>true</ServerGarbageCollection>
    <ConcurrentGarbageCollection>true</ConcurrentGarbageCollection>
    <TieredCompilation>true</TieredCompilation>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="BenchmarkDotNet" Version="0.14.0" />
  </ItemGroup>

  <ItemGroup>
    <ProjectReference Include="..\..\src\SpscPipe\SpscPipe.csproj" />
  </ItemGroup>

  <ItemGroup>
    <InternalsVisibleTo Include="SpscPipe.Tests" />
  </ItemGroup>

</Project>
```

- [ ] **Step 2: Create the placeholder Program.cs**

Create `tests/SpscPipe.Benchmarks/Program.cs`:

```csharp
namespace SpscPipe.Benchmarks;

internal static class Program
{
    private static int Main(string[] args)
    {
        if (args.Length == 0)
        {
            PrintUsage();
            return 1;
        }

        return args[0] switch
        {
            "bdn"     => RunBdn(args.AsSpan(1).ToArray()),
            "latency" => RunLatency(args.AsSpan(1).ToArray()),
            _         => Unknown(args[0]),
        };

        static int Unknown(string mode)
        {
            Console.Error.WriteLine($"unknown mode: {mode}");
            PrintUsage();
            return 1;
        }
    }

    private static int RunBdn(string[] args)
    {
        // Filled in by Task 7.
        Console.WriteLine("bdn mode: not yet implemented");
        return 0;
    }

    private static int RunLatency(string[] args)
    {
        // Filled in by Task 9.
        Console.WriteLine("latency mode: not yet implemented");
        return 0;
    }

    private static void PrintUsage() =>
        Console.WriteLine("""
            SpscPipe.Benchmarks
              bdn      [BenchmarkDotNet flags...]   throughput + allocation benchmarks
              latency  [--messages N] [--mode normal|backpressure|both]
                       [--impl spsc|bcl|both] [--sizes 16,64,...]
                                                    per-flush latency harness
            """);
}
```

- [ ] **Step 3: Add the project to the solution**

Read the current `SpscPipe.slnx`. Add a `<Project Path="tests/SpscPipe.Benchmarks/SpscPipe.Benchmarks.csproj" />` line inside the `<Folder Name="/tests/">` element so the file becomes:

```xml
<Solution>
  <Folder Name="/src/">
    <Project Path="src/SpscPipe/SpscPipe.csproj" />
  </Folder>
  <Folder Name="/tests/">
    <Project Path="tests/SpscPipe.Stress/SpscPipe.Stress.csproj" />
    <Project Path="tests/SpscPipe.Tests/SpscPipe.Tests.csproj" />
    <Project Path="tests/SpscPipe.Benchmarks/SpscPipe.Benchmarks.csproj" />
  </Folder>
</Solution>
```

- [ ] **Step 4: Verify it builds**

Run: `dotnet build tests/SpscPipe.Benchmarks/SpscPipe.Benchmarks.csproj -c Release`
Expected: `Build succeeded.` with 0 errors. Warnings about BenchmarkDotNet versions are fine. If the BenchmarkDotNet 0.14.0 package fails to restore for net10.0, try `0.15.2` or the latest BDN that targets net9.0+ (BDN supports later TFMs even if its own target is older). Pin whatever version restores.

- [ ] **Step 5: Verify it runs and prints usage**

Run: `dotnet run -c Release --project tests/SpscPipe.Benchmarks/SpscPipe.Benchmarks.csproj`
Expected: prints the usage block, exits with code 1.

Run: `dotnet run -c Release --project tests/SpscPipe.Benchmarks/SpscPipe.Benchmarks.csproj -- bdn`
Expected: prints `bdn mode: not yet implemented`, exits with code 0.

- [ ] **Step 6: Commit**

```bash
git add tests/SpscPipe.Benchmarks/SpscPipe.Benchmarks.csproj \
        tests/SpscPipe.Benchmarks/Program.cs \
        SpscPipe.slnx
git commit -m "$(cat <<'EOF'
Benchmarks: scaffold tests/SpscPipe.Benchmarks

Empty BenchmarkDotNet console project wired into SpscPipe.slnx; mode
dispatch on args[0] with placeholder bdn/latency handlers filled in
by later tasks.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: PipeAdapter — interface, config record, factory

**Files:**
- Create: `tests/SpscPipe.Benchmarks/PipeAdapter.cs`
- Test: `tests/SpscPipe.Tests/PipeAdapterTests.cs`

- [ ] **Step 1: Write the failing test**

Create `tests/SpscPipe.Tests/PipeAdapterTests.cs`:

```csharp
using SpscPipe.Benchmarks;

namespace SpscPipe.Tests;

public class PipeAdapterTests
{
    [Theory]
    [InlineData(Impl.SpscPipe)]
    [InlineData(Impl.BclPipe)]
    public async Task RoundTrip_Smoke_BothImpls(Impl impl)
    {
        var cfg = new PipeConfig(MinimumSegmentSize: 4096,
                                 PauseWriterThreshold: 65536,
                                 ResumeWriterThreshold: 32768);
        using var pipe = AdapterFactory.Build(impl, cfg);

        // Producer writes one chunk, completes.
        var producer = Task.Run(async () =>
        {
            pipe.Writer.GetMemory(64);
            pipe.Writer.Advance(64);
            await pipe.Writer.FlushAsync();
            pipe.Writer.Complete();
        });

        // Consumer drains until completion.
        long total = 0;
        var consumer = Task.Run(async () =>
        {
            while (true)
            {
                var rr = await pipe.Reader.ReadAsync();
                total += rr.Buffer.Length;
                pipe.Reader.AdvanceTo(rr.Buffer.End);
                if (rr.IsCompleted && rr.Buffer.IsEmpty) break;
            }
            pipe.Reader.Complete();
        });

        await Task.WhenAll(producer, consumer).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(64, total);
    }

    [Fact]
    public void Reset_Or_Rebuild_Returns_Usable_Pipe_For_Both_Impls()
    {
        var cfg = new PipeConfig(4096, 65536, 32768);

        foreach (var impl in new[] { Impl.SpscPipe, Impl.BclPipe })
        {
            using var pipe = AdapterFactory.Build(impl, cfg);
            // First round.
            pipe.Writer.GetMemory(8); pipe.Writer.Advance(8);
            pipe.Writer.FlushAsync().AsTask().Wait();
            pipe.Writer.Complete();
            // Drain and complete reader so BCL Pipe.Reset() is legal.
            var rr = pipe.Reader.ReadAsync().AsTask().Result;
            pipe.Reader.AdvanceTo(rr.Buffer.End);
            pipe.Reader.Complete();

            pipe.ResetOrRebuild();

            // After reset, the adapter is usable again.
            pipe.Writer.GetMemory(8); pipe.Writer.Advance(8);
            pipe.Writer.FlushAsync().AsTask().Wait();
            pipe.Writer.Complete();
            var rr2 = pipe.Reader.ReadAsync().AsTask().Result;
            Assert.Equal(8, rr2.Buffer.Length);
            pipe.Reader.AdvanceTo(rr2.Buffer.End);
            pipe.Reader.Complete();
        }
    }
}
```

Add a `ProjectReference` to the benchmarks project from `tests/SpscPipe.Tests/SpscPipe.Tests.csproj`:

```xml
<ItemGroup>
  <ProjectReference Include="..\..\src\SpscPipe\SpscPipe.csproj" />
  <ProjectReference Include="..\SpscPipe.Benchmarks\SpscPipe.Benchmarks.csproj" />
</ItemGroup>
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `dotnet test tests/SpscPipe.Tests/SpscPipe.Tests.csproj --filter "FullyQualifiedName~PipeAdapterTests"`
Expected: FAIL with "Impl/PipeConfig/AdapterFactory not found in namespace SpscPipe.Benchmarks" (or similar — types don't exist yet).

- [ ] **Step 3: Implement PipeAdapter.cs**

Create `tests/SpscPipe.Benchmarks/PipeAdapter.cs`:

```csharp
using System.IO.Pipelines;

namespace SpscPipe.Benchmarks;

public enum Impl { SpscPipe, BclPipe }

public readonly record struct PipeConfig(
    int  MinimumSegmentSize,
    long PauseWriterThreshold,
    long ResumeWriterThreshold);

internal interface IPipeAdapter : IDisposable
{
    PipeReader Reader { get; }
    PipeWriter Writer { get; }
    void ResetOrRebuild();
}

internal static class AdapterFactory
{
    public static IPipeAdapter Build(Impl impl, PipeConfig cfg) => impl switch
    {
        Impl.SpscPipe => new SpscPipeAdapter(cfg),
        Impl.BclPipe  => new BclPipeAdapter(cfg),
        _             => throw new ArgumentOutOfRangeException(nameof(impl)),
    };
}

internal sealed class SpscPipeAdapter : IPipeAdapter
{
    private readonly PipeConfig _cfg;
    private SpscPipe.SpscPipe _pipe;

    public SpscPipeAdapter(PipeConfig cfg)
    {
        _cfg = cfg;
        _pipe = Build(cfg);
    }

    public PipeReader Reader => _pipe.Reader;
    public PipeWriter Writer => _pipe.Writer;

    public void ResetOrRebuild() => _pipe.Reset();

    public void Dispose() => _pipe.Dispose();

    private static SpscPipe.SpscPipe Build(PipeConfig cfg) =>
        new SpscPipe.SpscPipe(new SpscPipe.SpscPipeOptions
        {
            MinimumSegmentSize    = cfg.MinimumSegmentSize,
            PauseWriterThreshold  = cfg.PauseWriterThreshold,
            ResumeWriterThreshold = cfg.ResumeWriterThreshold,
        });
}

internal sealed class BclPipeAdapter : IPipeAdapter
{
    private readonly PipeConfig _cfg;
    private Pipe _pipe;

    public BclPipeAdapter(PipeConfig cfg)
    {
        _cfg = cfg;
        _pipe = Build(cfg);
    }

    public PipeReader Reader => _pipe.Reader;
    public PipeWriter Writer => _pipe.Writer;

    // BCL Pipe.Reset() requires both sides to have completed. Caller (test or
    // [IterationCleanup]) must Complete reader+writer before calling. We do
    // an explicit Reset rather than allocating a new Pipe so per-iteration
    // teardown cost is comparable to SpscPipeAdapter.ResetOrRebuild.
    public void ResetOrRebuild() => _pipe.Reset();

    public void Dispose()
    {
        // Pipe is not IDisposable; nothing to do beyond letting GC reclaim it.
    }

    private static Pipe Build(PipeConfig cfg) =>
        new Pipe(new PipeOptions(
            pool: null,
            readerScheduler: PipeScheduler.ThreadPool,
            writerScheduler: PipeScheduler.ThreadPool,
            pauseWriterThreshold: cfg.PauseWriterThreshold,
            resumeWriterThreshold: cfg.ResumeWriterThreshold,
            minimumSegmentSize: cfg.MinimumSegmentSize,
            useSynchronizationContext: false));
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test tests/SpscPipe.Tests/SpscPipe.Tests.csproj --filter "FullyQualifiedName~PipeAdapterTests"`
Expected: 3 tests pass (2 theory cases + 1 fact). Total test run also passes (no regressions in existing tests).

- [ ] **Step 5: Commit**

```bash
git add tests/SpscPipe.Benchmarks/PipeAdapter.cs \
        tests/SpscPipe.Tests/PipeAdapterTests.cs \
        tests/SpscPipe.Tests/SpscPipe.Tests.csproj
git commit -m "$(cat <<'EOF'
Benchmarks: IPipeAdapter + Spsc/Bcl impls

Single interface so workload code is implementation-agnostic.
Smoke tests cover the round-trip and the reset/rebuild path for
both impls.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Workload helpers — BulkProducer + DrainConsumer

**Files:**
- Create: `tests/SpscPipe.Benchmarks/Workloads.cs`
- Test: `tests/SpscPipe.Tests/WorkloadsTests.cs`

- [ ] **Step 1: Write the failing test**

Create `tests/SpscPipe.Tests/WorkloadsTests.cs`:

```csharp
using SpscPipe.Benchmarks;

namespace SpscPipe.Tests;

public class WorkloadsTests
{
    [Theory]
    [InlineData(Impl.SpscPipe, 16,    1,     4096)]
    [InlineData(Impl.SpscPipe, 256,   16,    65536)]
    [InlineData(Impl.SpscPipe, 4096,  4,     65536)]
    [InlineData(Impl.BclPipe,  16,    1,     4096)]
    [InlineData(Impl.BclPipe,  256,   16,    65536)]
    [InlineData(Impl.BclPipe,  4096,  4,     65536)]
    public async Task BulkProducer_DrainConsumer_TransfersExactByteCount(
        Impl impl, int chunkSize, int batchPerFlush, long totalBytes)
    {
        var cfg = new PipeConfig(MinimumSegmentSize: 4096,
                                 PauseWriterThreshold: 65536,
                                 ResumeWriterThreshold: 32768);
        using var pipe = AdapterFactory.Build(impl, cfg);

        long observed = 0;
        var consumer = Task.Run(async () =>
        {
            while (true)
            {
                var rr = await pipe.Reader.ReadAsync();
                observed += rr.Buffer.Length;
                pipe.Reader.AdvanceTo(rr.Buffer.End);
                if (rr.IsCompleted && rr.Buffer.IsEmpty) break;
            }
            pipe.Reader.Complete();
        });

        var producer = Task.Run(() =>
            Workloads.BulkProducer(pipe, chunkSize, batchPerFlush, totalBytes));

        await Task.WhenAll(producer, consumer).WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(totalBytes, observed);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `dotnet test tests/SpscPipe.Tests/SpscPipe.Tests.csproj --filter "FullyQualifiedName~WorkloadsTests"`
Expected: FAIL with "Workloads not found".

- [ ] **Step 3: Implement Workloads.cs**

Create `tests/SpscPipe.Benchmarks/Workloads.cs`:

```csharp
namespace SpscPipe.Benchmarks;

internal static class Workloads
{
    // Producer writes `chunkSize` bytes per Advance, batches `batchPerFlush`
    // chunks per FlushAsync, and stops once `totalBytes` have been written.
    // No bytes are written into the buffer — both pipe impls are byte-agnostic
    // on the write path, so the omission preserves apples-to-apples while
    // avoiding memcpy noise. See spec §5.2 / §9.
    internal static async Task BulkProducer(
        IPipeAdapter pipe, int chunkSize, int batchPerFlush, long totalBytes)
    {
        long written = 0;
        while (written < totalBytes)
        {
            var batchTarget = written + (long)chunkSize * batchPerFlush;
            if (batchTarget > totalBytes) batchTarget = totalBytes;

            while (written < batchTarget)
            {
                var n = (int)Math.Min(chunkSize, batchTarget - written);
                pipe.Writer.GetMemory(n);
                pipe.Writer.Advance(n);
                written += n;
            }

            var fr = await pipe.Writer.FlushAsync();
            if (fr.IsCompleted || fr.IsCanceled) break;
        }
        pipe.Writer.Complete();
    }

    // Consumer reads everything until IsCompleted with empty buffer; advances
    // past every byte. Records nothing — used by throughput benchmarks where
    // we only care about wall time.
    internal static async Task DrainConsumer(IPipeAdapter pipe)
    {
        while (true)
        {
            var rr = await pipe.Reader.ReadAsync();
            if (rr.IsCanceled) break;
            pipe.Reader.AdvanceTo(rr.Buffer.End);
            if (rr.IsCompleted && rr.Buffer.IsEmpty) break;
        }
        pipe.Reader.Complete();
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test tests/SpscPipe.Tests/SpscPipe.Tests.csproj --filter "FullyQualifiedName~WorkloadsTests"`
Expected: 6 tests pass. Full test run still green.

- [ ] **Step 5: Commit**

```bash
git add tests/SpscPipe.Benchmarks/Workloads.cs \
        tests/SpscPipe.Tests/WorkloadsTests.cs
git commit -m "$(cat <<'EOF'
Benchmarks: BulkProducer + DrainConsumer helpers

Bulk producer batches chunks per flush; drain consumer advances past
every byte. No bytes written into buffers (byte-agnostic write path
on both impls). Theory tests cover Spsc + Bcl across small/medium/
large chunk sizes.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Histogram — log-bucket percentile recorder

**Files:**
- Create: `tests/SpscPipe.Benchmarks/Histogram.cs`
- Test: `tests/SpscPipe.Tests/HistogramTests.cs`

- [ ] **Step 1: Write the failing tests**

Create `tests/SpscPipe.Tests/HistogramTests.cs`:

```csharp
using SpscPipe.Benchmarks;

namespace SpscPipe.Tests;

public class HistogramTests
{
    [Fact]
    public void Records_Linear_Range_Percentiles_Within_Bucket_Resolution()
    {
        var h = new Histogram();
        // Record 1..1000 (linearly).
        for (long v = 1; v <= 1000; v++) h.Record(v);

        Assert.Equal(1000, h.Count);
        // p50 should be near 500. Allow ~5% slack for log-bucket coarseness
        // at this scale.
        var p50 = h.Percentile(0.50);
        Assert.InRange(p50, 475, 525);

        var p99 = h.Percentile(0.99);
        Assert.InRange(p99, 975, 1024);   // 1024 = next bucket boundary above 1000

        Assert.True(h.Max >= 1000);
    }

    [Fact]
    public void Empty_Histogram_Returns_Zero_For_Percentiles()
    {
        var h = new Histogram();
        Assert.Equal(0, h.Count);
        Assert.Equal(0, h.Percentile(0.5));
        Assert.Equal(0, h.Max);
    }

    [Fact]
    public void Clamps_Negative_And_Zero_To_Bucket_Zero()
    {
        var h = new Histogram();
        h.Record(0);
        h.Record(-5);
        h.Record(1);
        Assert.Equal(3, h.Count);
        // p99 should not be negative.
        Assert.True(h.Percentile(0.99) >= 0);
    }

    [Fact]
    public void Records_Wide_Range_Without_Overflow()
    {
        var h = new Histogram();
        for (int i = 0; i < 1_000_000; i++) h.Record(100);
        h.Record(1_000_000_000L);   // 1 second in ns
        Assert.Equal(1_000_001, h.Count);
        Assert.True(h.Max >= 1_000_000_000L);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `dotnet test tests/SpscPipe.Tests/SpscPipe.Tests.csproj --filter "FullyQualifiedName~HistogramTests"`
Expected: FAIL with "Histogram not found".

- [ ] **Step 3: Implement Histogram.cs**

Create `tests/SpscPipe.Benchmarks/Histogram.cs`:

```csharp
using System.Numerics;

namespace SpscPipe.Benchmarks;

// Log-linear histogram: 64 sub-buckets per power-of-2, covering 0..2^40 ns
// (~18 minutes). Resolution at any value v is v / 64. Sufficient for
// per-flush latency percentiles where v ranges from ~100ns to ~10ms.
internal sealed class Histogram
{
    private const int SubBucketCount = 64;
    private const int PowerOfTwoCount = 41;   // covers 0 .. 2^40 ns
    private readonly long[] _buckets = new long[SubBucketCount * PowerOfTwoCount];

    public long Count { get; private set; }
    public long Max { get; private set; }

    public void Record(long valueNs)
    {
        if (valueNs < 1) valueNs = 1;
        // Bucket index = floor(log2(v)) * SubBucketCount + linear sub-index.
        int msb = 63 - BitOperations.LeadingZeroCount((ulong)valueNs);
        if (msb >= PowerOfTwoCount) msb = PowerOfTwoCount - 1;

        // Sub-bucket: take the next 6 bits below the MSB (0..63).
        // For v < 64 (msb < 6), shift left to fill the sub-bucket evenly.
        int subIndex = msb >= 6
            ? (int)((valueNs >> (msb - 6)) & (SubBucketCount - 1))
            : (int)(valueNs & (SubBucketCount - 1));

        int bucket = msb * SubBucketCount + subIndex;
        _buckets[bucket]++;
        Count++;
        if (valueNs > Max) Max = valueNs;
    }

    // Returns the upper edge of the bucket containing the requested
    // percentile. Returns 0 if the histogram is empty.
    public long Percentile(double p)
    {
        if (Count == 0) return 0;
        if (p < 0) p = 0;
        if (p > 1) p = 1;

        long target = (long)Math.Ceiling(p * Count);
        if (target < 1) target = 1;

        long acc = 0;
        for (int i = 0; i < _buckets.Length; i++)
        {
            acc += _buckets[i];
            if (acc >= target) return BucketUpperBoundNs(i);
        }
        return Max;
    }

    private static long BucketUpperBoundNs(int bucket)
    {
        int msb = bucket / SubBucketCount;
        int sub = bucket % SubBucketCount;
        // Bucket [msb, sub] covers values whose MSB == msb and whose next
        // 6 bits == sub. Upper edge = ((sub + 1) << (msb - 6)) for msb >= 6,
        // else (sub + 1).
        return msb >= 6 ? (long)(sub + 1) << (msb - 6) : sub + 1;
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test tests/SpscPipe.Tests/SpscPipe.Tests.csproj --filter "FullyQualifiedName~HistogramTests"`
Expected: 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add tests/SpscPipe.Benchmarks/Histogram.cs \
        tests/SpscPipe.Tests/HistogramTests.cs
git commit -m "$(cat <<'EOF'
Benchmarks: log-bucket Histogram for latency percentiles

64 sub-buckets per power-of-2, ~v/64 resolution at any value v.
Covers 1ns..2^40ns; sufficient for per-flush latency reporting at
p50/p90/p99/p99.9.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Wire BDN dispatch in Program.cs

**Files:**
- Modify: `tests/SpscPipe.Benchmarks/Program.cs`

- [ ] **Step 1: Replace the placeholder RunBdn with the real dispatcher**

Edit `tests/SpscPipe.Benchmarks/Program.cs`. Replace the existing `RunBdn` method with:

```csharp
    private static int RunBdn(string[] args)
    {
        var summary = BenchmarkDotNet.Running.BenchmarkSwitcher
            .FromAssembly(typeof(Program).Assembly)
            .Run(args);
        // Switcher returns one Summary per type run; non-zero exit if any errored.
        return summary.Any(s => s.HasCriticalValidationErrors) ? 2 : 0;
    }
```

Add the using directive at the top (alphabetical with the implicit usings — file-scoped namespace remains unchanged):

```csharp
using BenchmarkDotNet.Running;
```

(If existing usings already cover `System.Linq`, no extra `using`.)

- [ ] **Step 2: Verify it builds**

Run: `dotnet build tests/SpscPipe.Benchmarks/SpscPipe.Benchmarks.csproj -c Release`
Expected: builds. The switcher will report "no benchmarks found in assembly" if invoked because `ThroughputBenchmarks` doesn't exist yet — we wire that in Task 6.

- [ ] **Step 3: Smoke-run the BDN dispatcher**

Run: `dotnet run -c Release --project tests/SpscPipe.Benchmarks/SpscPipe.Benchmarks.csproj -- bdn --list flat`
Expected: BDN prints "No benchmarks found" or an empty list. Exit code 0.

- [ ] **Step 4: Commit**

```bash
git add tests/SpscPipe.Benchmarks/Program.cs
git commit -m "$(cat <<'EOF'
Benchmarks: wire BenchmarkSwitcher for bdn mode

Forwards args after 'bdn' to BenchmarkDotNet so its filters/list/etc
work as documented. ThroughputBenchmarks added in the next commit.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: ThroughputBenchmarks — BDN class

**Files:**
- Create: `tests/SpscPipe.Benchmarks/ThroughputBenchmarks.cs`

- [ ] **Step 1: Implement ThroughputBenchmarks.cs**

Create `tests/SpscPipe.Benchmarks/ThroughputBenchmarks.cs`:

```csharp
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;

namespace SpscPipe.Benchmarks;

[MemoryDiagnoser]
[ThreadingDiagnoser]
[SimpleJob(RuntimeMoniker.Net100, warmupCount: 3, iterationCount: 5)]
public class ThroughputBenchmarks
{
    // Total bytes per timed op. Chosen so the smallest chunk (1 byte) still
    // completes in reasonable time and the largest (65536) doesn't finish in
    // microseconds. 1 MiB = 1M ops at chunk=1 (slow but tractable), 16 ops at
    // chunk=65536.
    private const long TotalBytesPerOp = 1L << 20;
    private const int  BatchPerFlushBulk = 16;

    // Tight thresholds for the Backpressure scenario. Producer/consumer both
    // run flat-out; small Pause forces frequent park/wake cycles. Spec §6.3.
    private const long BackpressurePause  = 4096;
    private const long BackpressureResume = 2048;

    [Params(Impl.SpscPipe, Impl.BclPipe)] public Impl Pipe;
    [Params(1, 16, 256, 4096, 65536)]      public int ChunkSize;

    // Declared as [Params] with a single value each so adding values to widen
    // the matrix is a one-line change. Currently locked to BCL PipeOptions
    // defaults for apples-to-apples comparison. Spec §6.1 notes.
    [Params(4096)]   public int  MinimumSegmentSize;
    [Params(65536)]  public long PauseWriterThreshold;
    [Params(32768)]  public long ResumeWriterThreshold;

    private IPipeAdapter _pipe = null!;

    [IterationSetup(Targets = new[] { nameof(Bulk), nameof(Chatty) })]
    public void SetupNormal() =>
        _pipe = AdapterFactory.Build(Pipe, new PipeConfig(
            MinimumSegmentSize, PauseWriterThreshold, ResumeWriterThreshold));

    [IterationSetup(Target = nameof(Backpressure))]
    public void SetupBackpressure() =>
        _pipe = AdapterFactory.Build(Pipe, new PipeConfig(
            MinimumSegmentSize, BackpressurePause, BackpressureResume));

    [IterationCleanup]
    public void Cleanup() => _pipe.Dispose();

    [Benchmark] public Task Bulk()         => Run(BatchPerFlushBulk);
    [Benchmark] public Task Chatty()       => Run(batchPerFlush: 1);
    [Benchmark] public Task Backpressure() => Run(BatchPerFlushBulk);

    private Task Run(int batchPerFlush)
    {
        var producer = Workloads.BulkProducer(_pipe, ChunkSize, batchPerFlush, TotalBytesPerOp);
        var consumer = Workloads.DrainConsumer(_pipe);
        return Task.WhenAll(producer, consumer);
    }
}
```

- [ ] **Step 2: Verify it builds**

Run: `dotnet build tests/SpscPipe.Benchmarks/SpscPipe.Benchmarks.csproj -c Release`
Expected: builds.

- [ ] **Step 3: Verify BDN sees the benchmarks**

Run: `dotnet run -c Release --project tests/SpscPipe.Benchmarks/SpscPipe.Benchmarks.csproj -- bdn --list flat`
Expected: prints rows for `ThroughputBenchmarks.Bulk`, `ThroughputBenchmarks.Chatty`, `ThroughputBenchmarks.Backpressure` cross-multiplied with the params (30 entries).

- [ ] **Step 4: Run a single tiny case end-to-end as a smoke test**

Run: `dotnet run -c Release --project tests/SpscPipe.Benchmarks/SpscPipe.Benchmarks.csproj -- bdn --filter '*Bulk*' --warmupCount 1 --iterationCount 1 --maxIterationCount 1 --invocationCount 1 --unrollFactor 1`
Expected: BDN runs the smallest matrix slice for `Bulk`, completes in a few minutes, prints a summary table with rows for Spsc + Bcl × the chunk-size sweep. No errors. The `BenchmarkDotNet.Artifacts/` directory is created with markdown/csv reports.

If this is too slow for an iterative dev loop, restrict further with `--filter '*ChunkSize=4096,Pipe=SpscPipe*'` (BDN supports param filters in the filter string).

- [ ] **Step 5: Commit**

```bash
git add tests/SpscPipe.Benchmarks/ThroughputBenchmarks.cs
git commit -m "$(cat <<'EOF'
Benchmarks: ThroughputBenchmarks (BDN, MemoryDiagnoser)

Three [Benchmark] methods (Bulk/Chatty/Backpressure) cross with
{Spsc, Bcl} x ChunkSize {1,16,256,4096,65536}. Backpressure uses
its own [IterationSetup] with tight Pause/Resume to force park/wake
cycles. Threshold params declared with single values (BCL defaults)
so widening is a one-line change.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: LatencyHarness — argument parsing + scenario shell

**Files:**
- Create: `tests/SpscPipe.Benchmarks/LatencyHarness.cs`
- Modify: `tests/SpscPipe.Benchmarks/Program.cs`

- [ ] **Step 1: Implement the LatencyHarness skeleton (parsing + dispatch only)**

Create `tests/SpscPipe.Benchmarks/LatencyHarness.cs`:

```csharp
namespace SpscPipe.Benchmarks;

internal static class LatencyHarness
{
    private static readonly int[] DefaultSizes = [16, 64, 256, 4096, 65536];
    private static readonly Mode[] AllModes = [Mode.Normal, Mode.Backpressure];
    private static readonly Impl[] AllImpls = [Impl.SpscPipe, Impl.BclPipe];

    internal enum Mode { Normal, Backpressure }

    internal sealed record Cli(
        long      Messages,
        Mode[]    Modes,
        Impl[]    Impls,
        int[]     Sizes);

    internal static int Run(string[] args)
    {
        if (!TryParse(args, out var cli, out var error))
        {
            Console.Error.WriteLine(error);
            return 1;
        }

        foreach (var mode in cli.Modes)
        {
            PrintHeader(mode);
            foreach (var impl in cli.Impls)
                foreach (var size in cli.Sizes)
                    RunCell(impl, size, mode, cli.Messages);
        }
        return 0;
    }

    private static void PrintHeader(Mode mode)
    {
        var (pause, resume) = ConfigFor(mode);
        Console.WriteLine();
        Console.WriteLine($"## Latency — {mode.ToString().ToLowerInvariant()} (Pause={pause}, Resume={resume})");
        Console.WriteLine();
        Console.WriteLine("| Impl     | MessageSize | p50 (ns) | p90 (ns) | p99 (ns) | p99.9 (ns) | max (ns) |");
        Console.WriteLine("| -------- | ----------: | -------: | -------: | -------: | ---------: | -------: |");
    }

    // Filled in by Task 8.
    private static void RunCell(Impl impl, int size, Mode mode, long messages)
    {
        Console.WriteLine($"| {impl,-8} | {size,11} | {0,8} | {0,8} | {0,8} | {0,10} | {0,8} |");
    }

    internal static (long Pause, long Resume) ConfigFor(Mode mode) => mode switch
    {
        Mode.Normal        => (65536, 32768),
        Mode.Backpressure  => (4096,  2048),
        _                  => throw new ArgumentOutOfRangeException(nameof(mode)),
    };

    private static bool TryParse(string[] args, out Cli cli, out string error)
    {
        long messages = 1_000_000;
        Mode[] modes = AllModes;
        Impl[] impls = AllImpls;
        int[]  sizes = DefaultSizes;

        for (var i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--messages":
                    if (++i >= args.Length || !long.TryParse(args[i], out messages) || messages < 1)
                        return Fail(out cli, out error, "--messages requires a positive integer");
                    break;
                case "--mode":
                    if (++i >= args.Length) return Fail(out cli, out error, "--mode requires a value");
                    modes = args[i] switch
                    {
                        "normal"        => [Mode.Normal],
                        "backpressure"  => [Mode.Backpressure],
                        "both"          => AllModes,
                        _               => null!,
                    };
                    if (modes is null) return Fail(out cli, out error, $"unknown --mode: {args[i]}");
                    break;
                case "--impl":
                    if (++i >= args.Length) return Fail(out cli, out error, "--impl requires a value");
                    impls = args[i] switch
                    {
                        "spsc" => [Impl.SpscPipe],
                        "bcl"  => [Impl.BclPipe],
                        "both" => AllImpls,
                        _      => null!,
                    };
                    if (impls is null) return Fail(out cli, out error, $"unknown --impl: {args[i]}");
                    break;
                case "--sizes":
                    if (++i >= args.Length) return Fail(out cli, out error, "--sizes requires a comma-separated list");
                    var parts = args[i].Split(',', StringSplitOptions.RemoveEmptyEntries);
                    sizes = new int[parts.Length];
                    for (var j = 0; j < parts.Length; j++)
                    {
                        if (!int.TryParse(parts[j], out var s) || s < 8)
                            return Fail(out cli, out error, $"invalid --sizes entry '{parts[j]}' (must be int >= 8 to fit timestamp)");
                        sizes[j] = s;
                    }
                    break;
                case "--help" or "-h":
                    return Fail(out cli, out error,
                        "latency [--messages N] [--mode normal|backpressure|both] [--impl spsc|bcl|both] [--sizes 16,64,...]");
                default:
                    return Fail(out cli, out error, $"unknown flag: {args[i]}");
            }
        }

        cli = new Cli(messages, modes, impls, sizes);
        error = "";
        return true;

        static bool Fail(out Cli cli, out string error, string message)
        {
            cli = default!;
            error = message;
            return false;
        }
    }
}
```

- [ ] **Step 2: Wire it into Program.RunLatency**

Edit `tests/SpscPipe.Benchmarks/Program.cs`. Replace the existing `RunLatency` method body with:

```csharp
    private static int RunLatency(string[] args) => LatencyHarness.Run(args);
```

- [ ] **Step 3: Verify it builds and prints empty tables**

Run: `dotnet run -c Release --project tests/SpscPipe.Benchmarks/SpscPipe.Benchmarks.csproj -- latency --messages 1`
Expected: prints two markdown table headers (one for `normal`, one for `backpressure`) with rows of zeros (RunCell stub). Exit 0.

Run: `dotnet run -c Release --project tests/SpscPipe.Benchmarks/SpscPipe.Benchmarks.csproj -- latency --mode normal --impl spsc --sizes 16,64`
Expected: one table with `normal` heading, two rows. Exit 0.

Run: `dotnet run -c Release --project tests/SpscPipe.Benchmarks/SpscPipe.Benchmarks.csproj -- latency --bogus`
Expected: stderr `unknown flag: --bogus`, exit 1.

- [ ] **Step 4: Commit**

```bash
git add tests/SpscPipe.Benchmarks/LatencyHarness.cs \
        tests/SpscPipe.Benchmarks/Program.cs
git commit -m "$(cat <<'EOF'
Benchmarks: latency harness skeleton (CLI + scenario dispatch)

Argument parsing for --messages / --mode / --impl / --sizes; per-mode
markdown table headers; RunCell is a stub that emits zero rows.
Measurement loop lands in the next commit.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: LatencyHarness — measurement loop

**Files:**
- Modify: `tests/SpscPipe.Benchmarks/LatencyHarness.cs`

- [ ] **Step 1: Implement the producer/consumer measurement loop**

Edit `tests/SpscPipe.Benchmarks/LatencyHarness.cs`. Replace the stub `RunCell` (and add helpers below it) with:

```csharp
    private const int WarmupMessages = 10_000;

    private static readonly double NsPerTick = 1_000_000_000.0 / System.Diagnostics.Stopwatch.Frequency;

    private static void RunCell(Impl impl, int size, Mode mode, long messages)
    {
        var (pause, resume) = ConfigFor(mode);
        var cfg = new PipeConfig(MinimumSegmentSize: 4096,
                                 PauseWriterThreshold: pause,
                                 ResumeWriterThreshold: resume);

        // Two passes: a warmup that records into a throwaway histogram,
        // then the real measured pass.
        var warmupHist = new Histogram();
        RunOne(impl, size, cfg, WarmupMessages, warmupHist);

        var hist = new Histogram();
        RunOne(impl, size, cfg, messages, hist);

        var p50    = hist.Percentile(0.50);
        var p90    = hist.Percentile(0.90);
        var p99    = hist.Percentile(0.99);
        var p999   = hist.Percentile(0.999);
        var max    = hist.Max;
        Console.WriteLine($"| {impl,-8} | {size,11} | {p50,8} | {p90,8} | {p99,8} | {p999,10} | {max,8} |");
    }

    private static void RunOne(Impl impl, int messageSize, PipeConfig cfg, long messages, Histogram hist)
    {
        using var pipe = AdapterFactory.Build(impl, cfg);

        var producer = Task.Run(() => Producer(pipe, messageSize, messages));
        var consumer = Task.Run(() => Consumer(pipe, messageSize, hist));

        Task.WhenAll(producer, consumer).GetAwaiter().GetResult();
    }

    private static async Task Producer(IPipeAdapter pipe, int messageSize, long messages)
    {
        for (long i = 0; i < messages; i++)
        {
            var span = pipe.Writer.GetSpan(messageSize);
            System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(
                span, System.Diagnostics.Stopwatch.GetTimestamp());
            pipe.Writer.Advance(messageSize);
            var fr = await pipe.Writer.FlushAsync();
            if (fr.IsCompleted || fr.IsCanceled) break;
        }
        pipe.Writer.Complete();
    }

    private static async Task Consumer(IPipeAdapter pipe, int messageSize, Histogram hist)
    {
        Span<byte> tsScratch = stackalloc byte[8];
        while (true)
        {
            var rr = await pipe.Reader.ReadAsync();
            if (rr.IsCanceled) break;
            var buffer = rr.Buffer;

            while (buffer.Length >= messageSize)
            {
                long ts = ReadTimestamp(buffer, tsScratch);
                long now = System.Diagnostics.Stopwatch.GetTimestamp();
                long ns = (long)((now - ts) * NsPerTick);
                hist.Record(ns);
                buffer = buffer.Slice(messageSize);
            }

            pipe.Reader.AdvanceTo(buffer.Start, rr.Buffer.End);
            if (rr.IsCompleted && buffer.IsEmpty) break;
        }
        pipe.Reader.Complete();
    }

    private static long ReadTimestamp(System.Buffers.ReadOnlySequence<byte> buffer, Span<byte> scratch)
    {
        var first = buffer.First.Span;
        if (first.Length >= 8)
            return System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(first);

        // Slow path: timestamp straddles two segments. Copy the first 8 bytes
        // into the stackalloc scratch via the sequence reader.
        buffer.Slice(0, 8).CopyTo(scratch);
        return System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(scratch);
    }
```

Note the `using System.Buffers;` and `using System.Buffers.Binary;` need to be at the top of the file (or use fully-qualified names as shown). Using fully-qualified names keeps the diff small; either is fine.

Important: `Consumer` uses `Span<byte> tsScratch = stackalloc byte[8];` *outside* the `await` boundary — `tsScratch` is only touched in the synchronous body between `ReadAsync` calls, which is legal (the span doesn't cross an await). This is the only fiddly correctness point in the file.

Wait — the `while (true)` loop has an `await` inside, and `tsScratch` is declared outside that loop. Span locals can't survive across awaits. Move the `stackalloc` *inside* the loop, after the `await`:

Replace the `Consumer` body with:

```csharp
    private static async Task Consumer(IPipeAdapter pipe, int messageSize, Histogram hist)
    {
        while (true)
        {
            var rr = await pipe.Reader.ReadAsync();
            if (rr.IsCanceled) break;
            var buffer = rr.Buffer;

            while (buffer.Length >= messageSize)
            {
                long ts = ReadTimestamp(buffer);
                long now = System.Diagnostics.Stopwatch.GetTimestamp();
                long ns = (long)((now - ts) * NsPerTick);
                hist.Record(ns);
                buffer = buffer.Slice(messageSize);
            }

            pipe.Reader.AdvanceTo(buffer.Start, rr.Buffer.End);
            if (rr.IsCompleted && buffer.IsEmpty) break;
        }
        pipe.Reader.Complete();
    }

    private static long ReadTimestamp(System.Buffers.ReadOnlySequence<byte> buffer)
    {
        var first = buffer.First.Span;
        if (first.Length >= 8)
            return System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(first);

        Span<byte> scratch = stackalloc byte[8];
        buffer.Slice(0, 8).CopyTo(scratch);
        return System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(scratch);
    }
```

`stackalloc` lives inside `ReadTimestamp` which is fully synchronous — safe.

- [ ] **Step 2: Verify it builds**

Run: `dotnet build tests/SpscPipe.Benchmarks/SpscPipe.Benchmarks.csproj -c Release`
Expected: builds.

- [ ] **Step 3: Smoke-run a tiny cell**

Run: `dotnet run -c Release --project tests/SpscPipe.Benchmarks/SpscPipe.Benchmarks.csproj -- latency --messages 10000 --mode normal --impl spsc --sizes 64`
Expected: prints the Normal table header, one data row with non-zero p50/p90/p99/p99.9/max numbers in the hundreds-to-thousands of nanoseconds range. Exit 0. Total runtime well under 10 seconds.

- [ ] **Step 4: Smoke-run the full sweep at reduced N**

Run: `dotnet run -c Release --project tests/SpscPipe.Benchmarks/SpscPipe.Benchmarks.csproj -- latency --messages 50000`
Expected: two tables (normal + backpressure), each with 10 rows (2 impls × 5 sizes). All cells non-zero, monotonically: p50 ≤ p90 ≤ p99 ≤ p99.9 ≤ max. Exit 0.

- [ ] **Step 5: Commit**

```bash
git add tests/SpscPipe.Benchmarks/LatencyHarness.cs
git commit -m "$(cat <<'EOF'
Benchmarks: latency measurement loop

Per-cell warmup (10k messages discarded) followed by the recorded
pass. Producer embeds a Stopwatch tick in the first 8 bytes of each
flush; consumer parses message-by-message and records (now - ts)
into the log-bucket Histogram. Slow-path ReadTimestamp handles
8-byte values that straddle two ReadOnlySequence segments.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: README

**Files:**
- Create: `tests/SpscPipe.Benchmarks/README.md`

- [ ] **Step 1: Write the README**

Create `tests/SpscPipe.Benchmarks/README.md`:

```markdown
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
# List all benchmarks BDN sees
dotnet run -c Release --project tests/SpscPipe.Benchmarks -- bdn --list flat

# Run only the chatty workload
dotnet run -c Release --project tests/SpscPipe.Benchmarks -- bdn --filter '*Chatty*'

# Faster iteration (single warmup, single iteration; lower-quality numbers)
dotnet run -c Release --project tests/SpscPipe.Benchmarks -- bdn \
    --warmupCount 1 --iterationCount 1
```

Reports land in `BenchmarkDotNet.Artifacts/results/` (markdown, html, csv).

#### Reading the output

- **Mean** is the per-op time. One op transfers `1 MiB` total bytes through
  the pipe, so throughput is `1 MiB / Mean`. (BDN doesn't compute this for
  you — divide manually.)
- **Allocated** (from `[MemoryDiagnoser]`) is bytes/op. SpscPipe targets
  zero steady-state allocation (spec §1.1) — after warmup, expect ~0
  bytes/op. BCL `Pipe` allocates per-op.
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
```

- [ ] **Step 2: Commit**

```bash
git add tests/SpscPipe.Benchmarks/README.md
git commit -m "$(cat <<'EOF'
Benchmarks: README with usage and result-interpretation notes

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: End-to-end verification + final commit

**Files:**
- (No file changes; verification only.)

- [ ] **Step 1: Confirm the full solution builds**

Run: `dotnet build SpscPipe.slnx -c Release`
Expected: 3 projects build (`SpscPipe`, `SpscPipe.Stress`, `SpscPipe.Tests`, `SpscPipe.Benchmarks`). 0 errors.

- [ ] **Step 2: Confirm all tests pass (no regressions)**

Run: `dotnet test SpscPipe.slnx -c Release`
Expected: all existing tests pass plus the new `PipeAdapterTests`, `WorkloadsTests`, `HistogramTests`. Zero failures.

- [ ] **Step 3: BDN smoke run on a single param slice**

Run:
```bash
dotnet run -c Release --project tests/SpscPipe.Benchmarks -- bdn \
    --filter '*ThroughputBenchmarks.Bulk*' \
    --warmupCount 1 --iterationCount 1
```
Expected: BDN runs the Bulk benchmark across all 10 param combinations (Spsc + Bcl × 5 chunk sizes), prints a summary table, exit 0. Should take a few minutes.

Inspect the table: SpscPipe's `Allocated` column should be ~0 B/op for chunk sizes ≥ 16; BCL Pipe's `Allocated` should be non-zero (likely a few hundred bytes per op).

If SpscPipe shows non-zero steady-state allocations: this contradicts spec §1.1 and is worth investigating before declaring the benchmark valid. Either the benchmark itself is allocating (e.g., the workload helper) or there's a real regression in SpscPipe. Triage with `--profiler EP` (BDN's EventPipe profiler) — but that's outside this plan; flag it back to the user.

- [ ] **Step 4: Latency harness smoke run**

Run:
```bash
dotnet run -c Release --project tests/SpscPipe.Benchmarks -- latency --messages 100000
```
Expected: two markdown tables (normal + backpressure), each with 10 rows. All percentiles non-zero, ordered. Total runtime < 30 seconds.

- [ ] **Step 5: Final commit**

If any small fix-up was needed in Steps 3-4 (e.g., a typo, an off-by-one), commit it now. If everything was clean, no commit needed — the project is ready.

```bash
# Only if there are uncommitted fixes from this task:
git add -p   # stage fixes
git commit -m "$(cat <<'EOF'
Benchmarks: end-to-end smoke fixes (if any)

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Self-review

**Spec coverage:**

- §1 Goals — covered by Tasks 6 (BDN throughput + allocation) and 8 (latency).
- §2 Non-goals — respected (no multi-scheduler, no multi-producer scenarios, no CI).
- §3 Project layout — Task 1 (csproj + slnx wiring).
- §4 Entry point and modes — Tasks 1 (Program shell), 5 (BDN dispatch), 7 (latency dispatch).
- §5.1 IPipeAdapter — Task 2.
- §5.2 Workload helpers — Task 3.
- §6.1 BDN throughput matrix — Task 6.
- §6.2 What BDN measures — covered by README in Task 9 (this lives in user docs, not code).
- §6.3 Latency harness — Tasks 7 (CLI + scenario shell) + 8 (measurement loop).
- §7 Reporting — covered by Task 9 (README documents BDN report location and latency stdout output).
- §8 Risks — covered in design doc; no code change required.
- §9 Known limitations — explicitly documented in Task 9 README.
- §10 Out-of-scope future work — not implemented (correct).

**Placeholder scan:** No `TBD`, `TODO`, "implement later", "fill in details" appear in code blocks or step instructions. The `RunCell` stub in Task 7 is intentional: Task 8 names it explicitly and replaces the body.

**Type consistency check:**
- `Impl` enum: `SpscPipe, BclPipe` — consistent across Tasks 2, 3, 6, 7.
- `PipeConfig(int MinimumSegmentSize, long PauseWriterThreshold, long ResumeWriterThreshold)` — order and types consistent across Tasks 2, 6, 8.
- `IPipeAdapter` members `Reader`, `Writer`, `ResetOrRebuild`, `Dispose` — used identically in Tasks 3, 6, 8.
- `Workloads.BulkProducer(IPipeAdapter, int chunkSize, int batchPerFlush, long totalBytes)` — signature consistent across Tasks 3 and 6.
- `Histogram.Record(long)` / `Percentile(double)` / `Count` / `Max` — consistent across Tasks 4 and 8.
- `LatencyHarness.Mode` enum and `ConfigFor` — consistent across Tasks 7 and 8.
