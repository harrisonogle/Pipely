# SPSC Pipe Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a lock-free single-producer single-consumer (SPSC) `PipeReader`/`PipeWriter` per the design at `docs/superpowers/specs/2026-04-25-spsc-pipe-tripleBuffer-design.md`, demonstrably faster than BCL `Pipe` under SPSC scheduling.

**Architecture:** Two `TripleBuffer<T>` instances carry cross-thread state snapshots; two single-packed-int `SpscAwaiter<T>` instances handle wakeups via a stash-and-construct (Pattern 2) pattern. Writer is sole mutator of segments and chain. Recycle predicate is reference comparison. Read/flush precedence is throw-first (BCL-strict per `IsCompletedOrThrow`).

**Tech Stack:** C# / .NET 10, `System.IO.Pipelines`, `System.Buffers`, `System.Threading`, `ManualResetValueTaskSourceCore<T>`, `ReadOnlySequence<byte>`. Tests: xUnit. Benchmarks: BenchmarkDotNet + `MemoryDiagnoser`.

**Spec reference convention:** "Spec §X" or "Spec line N" refers to `docs/superpowers/specs/2026-04-25-spsc-pipe-tripleBuffer-design.md` at commit `2ca40e9` (round 4 lock-in). Invariants (I1–I15) and rules (R1–R9) are stable; pseudocode line numbers may shift if the spec is later edited.

**Decisions baked into this plan:**
- TLA+: skipped for v1. The design has been review-stabilized across 4 rounds; the awaiter's 4-state machine is small enough that hand-enumeration suffices. Stress harness catches what TLA+ would catch for our scope.
- Worktree-based: implementation runs in a dedicated git worktree (Task 0).
- TDD per checkpoint: every public-API task pairs implementation with tests in the same commit.
- R4-7 (pipe-identity check): included in Task 2 via per-segment back-reference.
- R4-13 (zero-byte flush optimization): deferred. Spec acknowledges as optional.
- R4-15 (`SpscAwaiter<FlushResult>` stash-field savings): deferred. Spec acknowledges as cosmetic.

---

## File structure

```
SpscPipe/                                          (existing root)
├── SpscPipe.sln                                   (NEW)
├── src/
│   └── SpscPipelines/
│       ├── SpscPipelines.csproj                   (existing — minor edit for InternalsVisibleTo)
│       ├── TripleBuffer.cs                        (existing — unchanged)
│       ├── BufferSegment.cs                       (NEW)
│       ├── SpscAwaiter.cs                         (NEW)
│       ├── SpscPipeOptions.cs                     (NEW)
│       ├── WriterState.cs                         (NEW — internal struct)
│       ├── ReaderState.cs                         (NEW — internal struct)
│       ├── SpscPipe.cs                            (NEW — main class + nested Reader/Writer)
│       ├── SpscPipe.Reader.cs                     (NEW — partial; nested SpscPipeReader)
│       └── SpscPipe.Writer.cs                     (NEW — partial; nested SpscPipeWriter)
└── tests/
    ├── SpscPipe.Tests/                            (NEW)
    │   ├── SpscPipe.Tests.csproj
    │   ├── BufferSegmentTests.cs
    │   ├── SpscAwaiterTests.cs
    │   ├── SpscPipeWriterTests.cs
    │   ├── SpscPipeReaderTests.cs
    │   ├── SpscPipeAdvanceToTests.cs
    │   ├── SpscPipeLifecycleTests.cs
    │   ├── SpscPipeCancellationTests.cs
    │   ├── SpscPipeDisposeTests.cs
    │   └── BclParityTests.cs
    ├── SpscPipe.Stress/                           (NEW)
    │   ├── SpscPipe.Stress.csproj
    │   ├── ByteSequence.cs
    │   ├── StressHarness.cs
    │   └── Program.cs
    └── SpscPipe.Benchmarks/                       (NEW)
        ├── SpscPipe.Benchmarks.csproj
        ├── IPipeAdapter.cs
        ├── BclPipeAdapter.cs
        ├── SpscPipeAdapter.cs                     (added in Task 13)
        ├── ThroughputBenchmarks.cs
        ├── LatencyHarness.cs
        ├── Histogram.cs
        └── Program.cs
```

---

## Task 0: Worktree setup

**Files:**
- N/A — this is a one-shot environment setup before any task starts.

- [ ] **Step 1: Create the implementation worktree from master**

```bash
git worktree add /home/harrison/src/worktrees/SpscPipe/spsc-impl -b spsc-impl
cd /home/harrison/src/worktrees/SpscPipe/spsc-impl
```

Expected: new worktree on branch `spsc-impl` with all current files present.

- [ ] **Step 2: Verify spec is present and current**

```bash
ls docs/superpowers/specs/2026-04-25-spsc-pipe-tripleBuffer-design.md
git log --oneline -1 docs/superpowers/specs/2026-04-25-spsc-pipe-tripleBuffer-design.md
```

Expected: spec file exists; latest commit is `2ca40e9` (round-4 lock-in) or later.

- [ ] **Step 3: Verify existing TripleBuffer compiles**

```bash
dotnet build src/SpscPipelines/SpscPipelines.csproj
```

Expected: build succeeds with no errors.

---

## Task 1: Solution + benchmark infrastructure (recreate prior infra)

**Goal:** Establish a solution, a benchmark project against BCL `Pipe`, and verify it runs end-to-end before writing any new SPSC code. This is Section 7's prerequisite — without baseline benchmarks, "materially higher than BCL" can't be measured.

**Files:**
- Create: `SpscPipe.sln` (root)
- Create: `tests/SpscPipe.Benchmarks/SpscPipe.Benchmarks.csproj`
- Create: `tests/SpscPipe.Benchmarks/IPipeAdapter.cs`
- Create: `tests/SpscPipe.Benchmarks/BclPipeAdapter.cs`
- Create: `tests/SpscPipe.Benchmarks/Histogram.cs`
- Create: `tests/SpscPipe.Benchmarks/ThroughputBenchmarks.cs`
- Create: `tests/SpscPipe.Benchmarks/LatencyHarness.cs`
- Create: `tests/SpscPipe.Benchmarks/Program.cs`

- [ ] **Step 1: Create the solution and add the existing project**

```bash
cd /home/harrison/src/worktrees/SpscPipe/spsc-impl
dotnet new sln -n SpscPipe
dotnet sln add src/SpscPipelines/SpscPipelines.csproj
```

Expected: `SpscPipe.sln` created; project added.

- [ ] **Step 2: Create the benchmarks project**

```bash
mkdir -p tests/SpscPipe.Benchmarks
dotnet new console -n SpscPipe.Benchmarks -o tests/SpscPipe.Benchmarks --framework net10.0
dotnet sln add tests/SpscPipe.Benchmarks/SpscPipe.Benchmarks.csproj
dotnet add tests/SpscPipe.Benchmarks/SpscPipe.Benchmarks.csproj reference src/SpscPipelines/SpscPipelines.csproj
dotnet add tests/SpscPipe.Benchmarks/SpscPipe.Benchmarks.csproj package BenchmarkDotNet
```

Expected: project created, BenchmarkDotNet referenced.

- [ ] **Step 3: Edit `tests/SpscPipe.Benchmarks/SpscPipe.Benchmarks.csproj` to set release config defaults**

Set `<PropertyGroup>` to include:
```xml
<TargetFramework>net10.0</TargetFramework>
<LangVersion>latest</LangVersion>
<Nullable>enable</Nullable>
<ServerGarbageCollection>true</ServerGarbageCollection>
<ConcurrentGarbageCollection>true</ConcurrentGarbageCollection>
```

Verify: `dotnet build tests/SpscPipe.Benchmarks/ -c Release` succeeds.

- [ ] **Step 4: Write `IPipeAdapter.cs`**

```csharp
using System.IO.Pipelines;

namespace SpscPipe.Benchmarks;

internal interface IPipeAdapter : IDisposable
{
    PipeReader Reader { get; }
    PipeWriter Writer { get; }
}
```

- [ ] **Step 5: Write `BclPipeAdapter.cs`**

```csharp
using System.IO.Pipelines;

namespace SpscPipe.Benchmarks;

internal sealed class BclPipeAdapter : IPipeAdapter
{
    private readonly Pipe _pipe;
    public BclPipeAdapter(PipeOptions? options = null) => _pipe = new Pipe(options ?? PipeOptions.Default);
    public PipeReader Reader => _pipe.Reader;
    public PipeWriter Writer => _pipe.Writer;
    public void Dispose() { /* BCL Pipe is not IDisposable; no-op */ }
}
```

- [ ] **Step 6: Write `Histogram.cs` (log-bucket histogram for latency)**

```csharp
namespace SpscPipe.Benchmarks;

internal sealed class Histogram
{
    private const int BucketCount = 64;            // covers ~1ns..~10s in log buckets
    private readonly long[] _buckets = new long[BucketCount];
    private long _count;

    public void Record(long elapsedTicks)
    {
        if (elapsedTicks <= 0) elapsedTicks = 1;
        int bucket = 63 - System.Numerics.BitOperations.LeadingZeroCount((ulong)elapsedTicks);
        if (bucket >= BucketCount) bucket = BucketCount - 1;
        _buckets[bucket]++;
        _count++;
    }

    public long Percentile(double p)
    {
        long target = (long)(_count * p);
        long acc = 0;
        for (int i = 0; i < BucketCount; i++)
        {
            acc += _buckets[i];
            if (acc >= target) return 1L << i;
        }
        return 1L << (BucketCount - 1);
    }

    public long Count => _count;
}
```

- [ ] **Step 7: Write `ThroughputBenchmarks.cs` (BDN, BCL only for now)**

```csharp
using BenchmarkDotNet.Attributes;
using System.IO.Pipelines;

namespace SpscPipe.Benchmarks;

[MemoryDiagnoser]
public class ThroughputBenchmarks
{
    private const int TotalBytes = 1 << 20;        // 1 MiB per iteration
    private const int ChunkSize = 4096;

    [Benchmark(Baseline = true)]
    public async Task BclPipe_ProduceAndDrain()
    {
        var pipe = new Pipe();
        var producer = Task.Run(async () =>
        {
            int written = 0;
            var chunk = new byte[ChunkSize];
            while (written < TotalBytes)
            {
                var memory = pipe.Writer.GetMemory(chunk.Length);
                chunk.CopyTo(memory);
                pipe.Writer.Advance(chunk.Length);
                await pipe.Writer.FlushAsync();
                written += chunk.Length;
            }
            await pipe.Writer.CompleteAsync();
        });

        var consumer = Task.Run(async () =>
        {
            while (true)
            {
                var result = await pipe.Reader.ReadAsync();
                pipe.Reader.AdvanceTo(result.Buffer.End);
                if (result.IsCompleted) break;
            }
            await pipe.Reader.CompleteAsync();
        });

        await Task.WhenAll(producer, consumer);
    }
}
```

- [ ] **Step 8: Write `LatencyHarness.cs` (skeleton; populated in Task 13)**

```csharp
namespace SpscPipe.Benchmarks;

internal static class LatencyHarness
{
    public static async Task Run(IPipeAdapter adapter, int messages, int messageBytes)
    {
        var hist = new Histogram();
        // Producer pauses briefly between writes; consumer measures end-to-end latency.
        // Full implementation deferred to Task 13.
        Console.WriteLine($"Latency harness skeleton: {messages} msgs of {messageBytes}B");
        Console.WriteLine($"Buckets recorded: {hist.Count}");
    }
}
```

- [ ] **Step 9: Write `Program.cs` (BDN switcher + scenario dispatch)**

```csharp
using BenchmarkDotNet.Running;
using SpscPipe.Benchmarks;

if (args.Length > 0 && args[0] == "latency")
{
    using var adapter = new BclPipeAdapter();
    await LatencyHarness.Run(adapter, messages: 100_000, messageBytes: 256);
    return;
}

BenchmarkSwitcher.FromAssembly(typeof(ThroughputBenchmarks).Assembly).Run(args);
```

- [ ] **Step 10: Smoke-test the benchmark infrastructure**

```bash
dotnet run --project tests/SpscPipe.Benchmarks -c Release -- --filter "*BclPipe_ProduceAndDrain*" --maxIterationCount 5 --warmupCount 2 --invocationCount 1
```

Expected: BDN runs, completes 5 iterations of `BclPipe_ProduceAndDrain`, prints throughput numbers. No exceptions.

- [ ] **Step 11: Verify latency harness skeleton runs**

```bash
dotnet run --project tests/SpscPipe.Benchmarks -c Release -- latency
```

Expected: Prints the skeleton message; no exceptions.

- [ ] **Step 12: Commit**

```bash
git add SpscPipe.sln tests/SpscPipe.Benchmarks/
git commit -m "Benchmarks: project + IPipeAdapter + BCL adapter + throughput skeleton"
```

---

## Task 2: BufferSegment (with R4-7 pipe-identity tag)

**Goal:** Implement the segment type per Spec §3, including the R4-7 `OwnerToken` for cross-pipe `SequencePosition` detection.

**Files:**
- Create: `src/SpscPipelines/BufferSegment.cs`
- Create: `tests/SpscPipe.Tests/SpscPipe.Tests.csproj`
- Create: `tests/SpscPipe.Tests/BufferSegmentTests.cs`
- Modify: `src/SpscPipelines/SpscPipelines.csproj` (add `InternalsVisibleTo` for tests)

- [ ] **Step 1: Create the test project and add references**

```bash
mkdir -p tests/SpscPipe.Tests
dotnet new xunit -n SpscPipe.Tests -o tests/SpscPipe.Tests --framework net10.0
dotnet sln add tests/SpscPipe.Tests/SpscPipe.Tests.csproj
dotnet add tests/SpscPipe.Tests/SpscPipe.Tests.csproj reference src/SpscPipelines/SpscPipelines.csproj
```

Verify: `dotnet build tests/SpscPipe.Tests/` succeeds with stub test passing.

- [ ] **Step 2: Add `InternalsVisibleTo` to the main project**

Edit `src/SpscPipelines/SpscPipelines.csproj`, add inside `<Project>`:

```xml
<ItemGroup>
  <InternalsVisibleTo Include="SpscPipe.Tests" />
  <InternalsVisibleTo Include="SpscPipe.Stress" />
  <InternalsVisibleTo Include="SpscPipe.Benchmarks" />
</ItemGroup>
```

Verify: `dotnet build src/SpscPipelines/` still succeeds.

- [ ] **Step 3: Write the failing rent test in `tests/SpscPipe.Tests/BufferSegmentTests.cs`**

```csharp
using System.Buffers;
using SpscPipelines;
using Xunit;

namespace SpscPipe.Tests;

public class BufferSegmentTests
{
    private static readonly object Owner = new();

    [Fact]
    public void RentFrom_SetsAvailableMemoryAndOwnerToken()
    {
        var seg = new BufferSegment();
        seg.RentFrom(MemoryPool<byte>.Shared, sizeHint: 1024, runningIndex: 0, owner: Owner);

        Assert.True(seg.AvailableMemory.Length >= 1024);
        Assert.Equal(0, seg.End);
        Assert.Null(seg.Next);
        Assert.Equal(0, seg.RunningIndex);
        Assert.Same(Owner, seg.OwnerToken);
        Assert.Same(seg.AvailableMemory, ((ReadOnlySequenceSegment<byte>)seg).Memory);
    }
}
```

- [ ] **Step 4: Run the failing test**

```bash
dotnet test tests/SpscPipe.Tests/ --filter "FullyQualifiedName~BufferSegmentTests"
```

Expected: FAIL with "BufferSegment not defined" or "RentFrom not defined".

- [ ] **Step 5: Implement `BufferSegment.cs`**

Create `src/SpscPipelines/BufferSegment.cs`:

```csharp
using System.Buffers;

namespace SpscPipelines;

internal sealed class BufferSegment : ReadOnlySequenceSegment<byte>
{
    private IMemoryOwner<byte>? _memoryOwner;

    public Memory<byte> AvailableMemory { get; private set; }
    public int End { get; private set; }
    public new BufferSegment? Next { get; private set; }
    public object? OwnerToken { get; private set; }   // R4-7

    public void RentFrom(MemoryPool<byte> pool, int sizeHint, long runningIndex, object owner)
    {
        _memoryOwner       = pool.Rent(sizeHint);
        AvailableMemory    = _memoryOwner.Memory;
        base.Memory        = AvailableMemory;
        base.RunningIndex  = runningIndex;
        End                = 0;
        Next               = null;
        base.Next          = null;
        OwnerToken         = owner;
    }

    public void Freeze(int bytesFilled, BufferSegment? next)
    {
        End         = bytesFilled;
        base.Memory = AvailableMemory.Slice(0, bytesFilled);
        Next        = next;
        base.Next   = next;
    }

    public void RecycleReset(long runningIndex)
    {
        base.RunningIndex = runningIndex;
        base.Memory       = AvailableMemory;
        End               = 0;
        Next              = null;
        base.Next         = null;
        // OwnerToken intentionally retained — segment stays bound to the same pipe across recycles.
    }

    public void DisposeOwned()
    {
        _memoryOwner?.Dispose();
        _memoryOwner    = null;
        AvailableMemory = default;
    }

    // Used by SpscPipe's freelist to link recycled segments without affecting End/Memory.
    // (Avoids overloading Freeze/RecycleReset for the freelist-link use case.)
    public void SetFreelistNext(BufferSegment? next)
    {
        Next = next;
        base.Next = next;
    }
}
```

- [ ] **Step 6: Run test, expect pass**

```bash
dotnet test tests/SpscPipe.Tests/ --filter "FullyQualifiedName~RentFrom_SetsAvailableMemoryAndOwnerToken"
```

Expected: PASS.

- [ ] **Step 7: Add Freeze, RecycleReset, DisposeOwned tests**

Append to `BufferSegmentTests.cs`:

```csharp
[Fact]
public void Freeze_SetsEndAndMemoryAndNext()
{
    var seg = new BufferSegment();
    seg.RentFrom(MemoryPool<byte>.Shared, 1024, 0, Owner);

    var next = new BufferSegment();
    next.RentFrom(MemoryPool<byte>.Shared, 1024, 256, Owner);

    seg.Freeze(bytesFilled: 256, next);

    Assert.Equal(256, seg.End);
    Assert.Equal(256, ((ReadOnlySequenceSegment<byte>)seg).Memory.Length);
    Assert.Same(next, seg.Next);
    Assert.Same(next, ((ReadOnlySequenceSegment<byte>)seg).Next);
}

[Fact]
public void RecycleReset_RestoresFullMemoryAndPreservesOwner()
{
    var seg = new BufferSegment();
    seg.RentFrom(MemoryPool<byte>.Shared, 1024, 0, Owner);
    int capacity = seg.AvailableMemory.Length;
    seg.Freeze(256, null);

    seg.RecycleReset(runningIndex: 999);

    Assert.Equal(0, seg.End);
    Assert.Null(seg.Next);
    Assert.Equal(999, seg.RunningIndex);
    Assert.Equal(capacity, ((ReadOnlySequenceSegment<byte>)seg).Memory.Length);
    Assert.Same(Owner, seg.OwnerToken);
}

[Fact]
public void DisposeOwned_ReleasesMemoryOwner()
{
    var seg = new BufferSegment();
    seg.RentFrom(MemoryPool<byte>.Shared, 1024, 0, Owner);

    seg.DisposeOwned();

    Assert.Equal(0, seg.AvailableMemory.Length);
    // Calling DisposeOwned again should be a no-op; not throw.
    seg.DisposeOwned();
}
```

- [ ] **Step 8: Run all `BufferSegmentTests`**

```bash
dotnet test tests/SpscPipe.Tests/ --filter "FullyQualifiedName~BufferSegmentTests"
```

Expected: all 4 tests PASS.

- [ ] **Step 9: Verify `Memory<T>` benign-torn-read assumption (Spec Nit-5)**

Append to `BufferSegmentTests.cs`:

```csharp
[Fact]
public void MemoryLayout_SlicePreservesObjectAndIndex_Assumption()
{
    // Spec Nit-5: the head==tail safety argument relies on Slice(0, n) preserving
    // _object and _index of Memory<byte>. If a future BCL change breaks this, we
    // want to find out at boot time, not as a heisenbug.
    using var owner = MemoryPool<byte>.Shared.Rent(1024);
    var full = owner.Memory;
    var sliced = full.Slice(0, 100);

    // We can't access _object/_index directly, but we can verify functional equivalence:
    // sliced[0] should refer to the same byte as full[0].
    full.Span[0] = 0xAB;
    Assert.Equal(0xAB, sliced.Span[0]);

    // And length differs as expected.
    Assert.Equal(1024, full.Length);
    Assert.Equal(100, sliced.Length);
}
```

Run; expect pass. This is a regression sentry — if it ever fails, the spec's Section 2 head==tail discipline needs re-derivation.

- [ ] **Step 10: Commit**

```bash
git add src/SpscPipelines/BufferSegment.cs src/SpscPipelines/SpscPipelines.csproj tests/SpscPipe.Tests/
git commit -m "BufferSegment: implementation + unit tests + Memory<T> sentinel"
```

---

## Task 3: SpscAwaiter<T> + state machine

**Goal:** Implement the awaiter per Spec §5 — single packed `int` state, Pattern 2 stash, IValueTaskSource<T>. Test the state machine via direct CAS sequences (state-machine in isolation; integration tested later via SpscPipe).

**Files:**
- Create: `src/SpscPipelines/SpscAwaiter.cs`
- Create: `tests/SpscPipe.Tests/SpscAwaiterTests.cs`

- [ ] **Step 1: Write the failing initial-state test**

```csharp
using System.Threading;
using System.Threading.Tasks.Sources;
using SpscPipelines;
using Xunit;

namespace SpscPipe.Tests;

public class SpscAwaiterTests
{
    [Fact]
    public void NewAwaiter_StateIsInactive()
    {
        var a = new SpscAwaiter<int>();
        Assert.Equal(SpscAwaiter<int>.Inactive, a._state);
    }
}
```

- [ ] **Step 2: Run, expect fail (`SpscAwaiter` not defined)**

```bash
dotnet test tests/SpscPipe.Tests/ --filter "FullyQualifiedName~SpscAwaiterTests"
```

Expected: FAIL.

- [ ] **Step 3: Implement `SpscAwaiter.cs` (skeleton + state constants)**

Create `src/SpscPipelines/SpscAwaiter.cs`:

```csharp
using System.Threading;
using System.Threading.Tasks.Sources;

namespace SpscPipelines;

internal sealed class SpscAwaiter<T> : IValueTaskSource<T>
{
    public ManualResetValueTaskSourceCore<T> _core = new() { RunContinuationsAsynchronously = true };
    public int _state;
    public CancellationTokenRegistration _ctr;
    public CancellationToken _token;

    // Pattern 2 stash (used by SpscAwaiter<ReadResult>; ignored by SpscAwaiter<FlushResult>).
    public BufferSegment? _stashHead;
    public int _stashHeadIdx;
    public BufferSegment? _stashTail;
    public int _stashTailIdx;

    public const int Inactive   = 0b00;
    public const int Pending    = 0b01;
    public const int StateMask  = 0b01;
    public const int CancelFlag = 0b10;

    public short Version => _core.Version;
    public T GetResult(short token) => _core.GetResult(token);
    public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);
    public void OnCompleted(Action<object?> c, object? s, short token, ValueTaskSourceOnCompletedFlags f)
        => _core.OnCompleted(c, s, token, f);
}
```

- [ ] **Step 4: Run; expect pass**

```bash
dotnet test tests/SpscPipe.Tests/ --filter "FullyQualifiedName~NewAwaiter_StateIsInactive"
```

Expected: PASS.

- [ ] **Step 5: Add cancel-flag tests (Or sets, owner CAS clears)**

Append to `SpscAwaiterTests.cs`:

```csharp
[Fact]
public void Or_CancelFlag_SetsBitAndReturnsOldValue()
{
    var a = new SpscAwaiter<int>();
    int old = Interlocked.Or(ref a._state, SpscAwaiter<int>.CancelFlag);

    Assert.Equal(SpscAwaiter<int>.Inactive, old);
    Assert.Equal(SpscAwaiter<int>.CancelFlag, a._state);
}

[Fact]
public void OwnerCanClearCancelFlagViaCAS()
{
    var a = new SpscAwaiter<int>();
    a._state = SpscAwaiter<int>.CancelFlag;     // simulate canceler's Or

    int prior = Interlocked.CompareExchange(ref a._state, SpscAwaiter<int>.Inactive, SpscAwaiter<int>.CancelFlag);

    Assert.Equal(SpscAwaiter<int>.CancelFlag, prior);
    Assert.Equal(SpscAwaiter<int>.Inactive, a._state);
}
```

Run; expect pass. (Tests state semantics only; no parking yet.)

- [ ] **Step 6: Write the park-and-signal test**

```csharp
[Fact]
public async Task ParkThenSignal_DeliversResult()
{
    var a = new SpscAwaiter<int>();
    a._core.Reset();

    // Owner: CAS Inactive → Pending.
    int prior = Interlocked.CompareExchange(ref a._state, SpscAwaiter<int>.Pending, SpscAwaiter<int>.Inactive);
    Assert.Equal(SpscAwaiter<int>.Inactive, prior);

    var task = new ValueTask<int>(a, a.Version);

    // Signaler: CAS Pending → Inactive (state cleared, flag preserved). Then SetResult.
    int oldV = a._state;
    int desired = oldV & ~SpscAwaiter<int>.StateMask;
    int seen = Interlocked.CompareExchange(ref a._state, desired, oldV);
    Assert.Equal(SpscAwaiter<int>.Pending, seen);

    a._core.SetResult(42);

    Assert.Equal(42, await task);
}
```

Run; expect PASS.

- [ ] **Step 7: Write the signal-with-flag-preserved test**

```csharp
[Fact]
public async Task ParkThenCancelerSetsFlagThenSignaler_FlagPreservedAfterDelivery()
{
    var a = new SpscAwaiter<int>();
    a._core.Reset();

    Interlocked.CompareExchange(ref a._state, SpscAwaiter<int>.Pending, SpscAwaiter<int>.Inactive);

    // Canceler races first: Or flag.
    Interlocked.Or(ref a._state, SpscAwaiter<int>.CancelFlag);
    Assert.Equal(SpscAwaiter<int>.Pending | SpscAwaiter<int>.CancelFlag, a._state);

    var task = new ValueTask<int>(a, a.Version);

    // Signaler wins CAS Pending|Flag → Inactive|Flag (state cleared, flag preserved).
    int oldV = a._state;
    int desired = oldV & ~SpscAwaiter<int>.StateMask;
    int seen = Interlocked.CompareExchange(ref a._state, desired, oldV);
    Assert.Equal(SpscAwaiter<int>.Pending | SpscAwaiter<int>.CancelFlag, seen);
    a._core.SetResult(7);

    Assert.Equal(7, await task);
    // Flag remains set; next Park's R4 lost-cancel re-check picks it up.
    Assert.Equal(SpscAwaiter<int>.CancelFlag, a._state);
}
```

Run; expect PASS.

- [ ] **Step 8: Write the canceler-wins-CAS test**

```csharp
[Fact]
public async Task CancelerWinsCAS_DeliversResultAndClearsFlag()
{
    var a = new SpscAwaiter<int>();
    a._core.Reset();

    Interlocked.CompareExchange(ref a._state, SpscAwaiter<int>.Pending, SpscAwaiter<int>.Inactive);
    Interlocked.Or(ref a._state, SpscAwaiter<int>.CancelFlag);

    var task = new ValueTask<int>(a, a.Version);

    // Canceler CAS Pending|Flag → Inactive (flag cleared by this CAS).
    int seen = Interlocked.CompareExchange(
        ref a._state,
        SpscAwaiter<int>.Inactive,
        SpscAwaiter<int>.Pending | SpscAwaiter<int>.CancelFlag);
    Assert.Equal(SpscAwaiter<int>.Pending | SpscAwaiter<int>.CancelFlag, seen);
    a._core.SetResult(99);

    Assert.Equal(99, await task);
    Assert.Equal(SpscAwaiter<int>.Inactive, a._state);
}
```

Run; expect PASS.

- [ ] **Step 9: Write the stash round-trip test**

```csharp
[Fact]
public void StashFields_AreReadableAfterAssignment()
{
    var a = new SpscAwaiter<int>();
    var head = new BufferSegment();
    head.RentFrom(System.Buffers.MemoryPool<byte>.Shared, 1024, 0, this);
    var tail = new BufferSegment();
    tail.RentFrom(System.Buffers.MemoryPool<byte>.Shared, 1024, 1024, this);

    a._stashHead    = head;
    a._stashHeadIdx = 100;
    a._stashTail    = tail;
    a._stashTailIdx = 200;

    Assert.Same(head, a._stashHead);
    Assert.Equal(100, a._stashHeadIdx);
    Assert.Same(tail, a._stashTail);
    Assert.Equal(200, a._stashTailIdx);
}
```

Run; expect PASS.

- [ ] **Step 10: Commit**

```bash
git add src/SpscPipelines/SpscAwaiter.cs tests/SpscPipe.Tests/SpscAwaiterTests.cs
git commit -m "SpscAwaiter<T>: state machine + Pattern 2 stash + unit tests"
```

---

## Task 4: SpscPipeOptions + SpscPipe scaffolding + GetMemory/GetSpan/Advance

**Goal:** Define the public API shell, options, and the writer-local `GetMemory`/`Advance` methods. No cross-thread interaction yet.

**Files:**
- Create: `src/SpscPipelines/SpscPipeOptions.cs`
- Create: `src/SpscPipelines/WriterState.cs`
- Create: `src/SpscPipelines/ReaderState.cs`
- Create: `src/SpscPipelines/SpscPipe.cs`
- Create: `src/SpscPipelines/SpscPipe.Writer.cs`
- Create: `src/SpscPipelines/SpscPipe.Reader.cs`
- Create: `tests/SpscPipe.Tests/SpscPipeWriterTests.cs`

- [ ] **Step 1: Write `SpscPipeOptions.cs` per Spec §6**

```csharp
using System.Buffers;

namespace SpscPipelines;

public sealed class SpscPipeOptions
{
    public MemoryPool<byte> Pool { get; }
    public int  MinimumSegmentSize    { get; }
    public long PauseWriterThreshold  { get; }
    public long ResumeWriterThreshold { get; }
    public int  MaxFreelistSegments   { get; }

    public SpscPipeOptions(
        MemoryPool<byte>? pool = null,
        int  minimumSegmentSize    = 4096,
        long pauseWriterThreshold  = 65536,
        long resumeWriterThreshold = 32768,
        int  maxFreelistSegments   = 256)
    {
        if (minimumSegmentSize <= 0) throw new ArgumentOutOfRangeException(nameof(minimumSegmentSize));
        if (pauseWriterThreshold < 0) throw new ArgumentOutOfRangeException(nameof(pauseWriterThreshold));
        if (resumeWriterThreshold < 0) throw new ArgumentOutOfRangeException(nameof(resumeWriterThreshold));
        if (pauseWriterThreshold > 0 && resumeWriterThreshold > pauseWriterThreshold)
            throw new ArgumentException("ResumeWriterThreshold must be <= PauseWriterThreshold.", nameof(resumeWriterThreshold));
        if (maxFreelistSegments < 0) throw new ArgumentOutOfRangeException(nameof(maxFreelistSegments));

        Pool = pool ?? MemoryPool<byte>.Shared;
        MinimumSegmentSize    = minimumSegmentSize;
        PauseWriterThreshold  = pauseWriterThreshold;
        ResumeWriterThreshold = resumeWriterThreshold;
        MaxFreelistSegments   = maxFreelistSegments;
    }

    public static SpscPipeOptions Default { get; } = new();
}
```

- [ ] **Step 2: Write `WriterState.cs` and `ReaderState.cs` per Spec §2**

`WriterState.cs`:
```csharp
namespace SpscPipelines;

internal struct WriterState
{
    public BufferSegment? HeadSegment;
    public BufferSegment? TailSegment;
    public int            TailWritten;
    public long           TotalWritten;
    public bool           IsCompleted;
    public Exception?     CompletionException;
}
```

`ReaderState.cs`:
```csharp
namespace SpscPipelines;

internal struct ReaderState
{
    public BufferSegment? HeadSegment;
    public long           TotalConsumed;
    public long           TotalExamined;
    public bool           IsCompleted;
    public Exception?     CompletionException;
}
```

- [ ] **Step 3: Write `SpscPipe.cs` (main class, options, fields, nested Reader/Writer placeholders)**

```csharp
using System.IO.Pipelines;

namespace SpscPipelines;

public sealed partial class SpscPipe : IDisposable
{
    internal readonly SpscPipeOptions _options;
    internal readonly TripleBuffer<WriterState> _writerTb = new();
    internal readonly TripleBuffer<ReaderState> _readerTb = new();
    internal readonly SpscAwaiter<ReadResult>  _readAwaiter  = new();
    internal readonly SpscAwaiter<FlushResult> _flushAwaiter = new();

    // Writer-side cursors (writer thread only).
    internal BufferSegment? _chainHead;
    internal BufferSegment? _writingHead;
    internal int  _writingHeadBytesBuffered;
    internal long _totalWritten;
    internal BufferSegment? _freelistHead;
    internal int  _freelistCount;
    internal WriterState _lastPublishedWriterState;
    internal ReaderState _lastAcquiredReaderState;
    internal bool _writerCompleted;

    // Reader-side cursors (reader thread only).
    internal BufferSegment? _readHead;
    internal int _readHeadIdx;
    internal BufferSegment? _readTail;
    internal int _readTailIdx;
    internal long _totalConsumed;
    internal long _totalExamined;
    internal ReaderState _lastPublishedReaderState;
    internal WriterState _lastAcquiredWriterState;
    internal bool _readerCompleted;

    // Pipe-level (mutated by Dispose only).
    internal bool _disposed;

    private readonly SpscPipeWriter _writerInstance;
    private readonly SpscPipeReader _readerInstance;

    public SpscPipe() : this(SpscPipeOptions.Default) { }
    public SpscPipe(SpscPipeOptions options)
    {
        _options       = options;
        _writerInstance = new SpscPipeWriter(this);
        _readerInstance = new SpscPipeReader(this);
    }

    public PipeWriter Writer => _writerInstance;
    public PipeReader Reader => _readerInstance;

    public void Dispose()
    {
        // Full implementation in Task 10.
        if (_disposed) return;
        _disposed = true;
    }
}
```

- [ ] **Step 4: Write `SpscPipe.Writer.cs` (nested SpscPipeWriter with GetMemory/GetSpan/Advance)**

```csharp
using System.Buffers;
using System.IO.Pipelines;

namespace SpscPipelines;

public sealed partial class SpscPipe
{
    internal sealed class SpscPipeWriter : PipeWriter
    {
        private readonly SpscPipe _pipe;
        public SpscPipeWriter(SpscPipe pipe) => _pipe = pipe;

        public override Memory<byte> GetMemory(int sizeHint = 0)
        {
            if (_pipe._disposed) throw new ObjectDisposedException(nameof(SpscPipe));
            if (_pipe._writerCompleted) throw new InvalidOperationException("Writing is completed.");
            if (sizeHint < 0) throw new ArgumentOutOfRangeException(nameof(sizeHint));
            if (sizeHint == 0) sizeHint = 1;

            if (_pipe._writingHead == null)
            {
                _pipe._writingHead = _pipe.RentSegment(sizeHint, runningIndex: 0);
                _pipe._chainHead   = _pipe._writingHead;
            }
            else
            {
                int remaining = _pipe._writingHead.AvailableMemory.Length - _pipe._writingHeadBytesBuffered;
                if (remaining < sizeHint)
                {
                    int filled  = _pipe._writingHeadBytesBuffered;
                    long newRI  = _pipe._writingHead.RunningIndex + filled;
                    var newTail = _pipe.RentSegment(Math.Max(sizeHint, _pipe._options.MinimumSegmentSize), newRI);

                    _pipe._writingHead.Freeze(filled, newTail);
                    _pipe._writingHead = newTail;
                    _pipe._writingHeadBytesBuffered = 0;
                }
            }

            return _pipe._writingHead.AvailableMemory.Slice(_pipe._writingHeadBytesBuffered);
        }

        public override Span<byte> GetSpan(int sizeHint = 0) => GetMemory(sizeHint).Span;

        public override void Advance(int bytes)
        {
            if (_pipe._disposed) throw new ObjectDisposedException(nameof(SpscPipe));
            if (_pipe._writerCompleted) throw new InvalidOperationException("Writing is completed.");
            if (_pipe._writingHead == null) throw new InvalidOperationException("Advance without prior GetMemory.");
            if (_pipe._writingHeadBytesBuffered + bytes > _pipe._writingHead.AvailableMemory.Length)
                throw new ArgumentOutOfRangeException(nameof(bytes));
            _pipe._writingHeadBytesBuffered += bytes;
            _pipe._totalWritten += bytes;
        }

        // FlushAsync, Complete, CancelPendingFlush — implemented in later tasks.
        public override ValueTask<FlushResult> FlushAsync(CancellationToken ct = default) => throw new NotImplementedException();
        public override void Complete(Exception? ex = null) => throw new NotImplementedException();
        public override void CancelPendingFlush() => throw new NotImplementedException();
    }
}
```

- [ ] **Step 5: Write `SpscPipe.Reader.cs` (nested placeholder)**

```csharp
using System.IO.Pipelines;

namespace SpscPipelines;

public sealed partial class SpscPipe
{
    internal sealed class SpscPipeReader : PipeReader
    {
        private readonly SpscPipe _pipe;
        public SpscPipeReader(SpscPipe pipe) => _pipe = pipe;

        public override ValueTask<ReadResult> ReadAsync(CancellationToken ct = default) => throw new NotImplementedException();
        public override bool TryRead(out ReadResult result) => throw new NotImplementedException();
        public override void AdvanceTo(SequencePosition consumed) => throw new NotImplementedException();
        public override void AdvanceTo(SequencePosition consumed, SequencePosition examined) => throw new NotImplementedException();
        public override void Complete(Exception? ex = null) => throw new NotImplementedException();
        public override void CancelPendingRead() => throw new NotImplementedException();
    }
}
```

- [ ] **Step 6: Add `RentSegment` and freelist helpers to `SpscPipe.Writer.cs` (or back on SpscPipe.cs)**

Add inside `SpscPipe`:

```csharp
internal BufferSegment RentSegment(int sizeHint, long runningIndex)
{
    var s = PopFreelist(minSize: sizeHint);
    if (s != null)
    {
        s.RecycleReset(runningIndex);
        return s;
    }
    s = new BufferSegment();
    s.RentFrom(_options.Pool, Math.Max(sizeHint, _options.MinimumSegmentSize), runningIndex, owner: this);
    return s;
}

private BufferSegment? PopFreelist(int minSize)
{
    var head = _freelistHead;
    if (head == null) return null;
    if (head.AvailableMemory.Length < minSize)
    {
        // Drop and dispose; per Spec §3 N5 (avoid stranding small segments).
        _freelistHead = head.Next;
        head.DisposeOwned();
        _freelistCount--;
        return null;
    }
    _freelistHead = head.Next;
    _freelistCount--;
    // Don't RecycleReset here — RentSegment does it with the correct runningIndex,
    // which also clears the freelist-link Next set by PushFreelist.
    return head;
}

internal void PushFreelist(BufferSegment s)
{
    if (_freelistCount >= _options.MaxFreelistSegments)
    {
        s.DisposeOwned();
        return;
    }
    // Reset to clean state, then link into the freelist via SetFreelistNext.
    s.RecycleReset(runningIndex: 0);
    s.SetFreelistNext(_freelistHead);
    _freelistHead = s;
    _freelistCount++;
}
```

(Uses `SetFreelistNext` from Task 2 — keeps the freelist-link concern out of `Freeze`/`RecycleReset`, both of which are about the live chain.)

- [ ] **Step 7: Write writer tests**

Create `tests/SpscPipe.Tests/SpscPipeWriterTests.cs`:

```csharp
using SpscPipelines;
using Xunit;

namespace SpscPipe.Tests;

public class SpscPipeWriterTests
{
    [Fact]
    public void GetMemory_ReturnsAtLeastSizeHint_AndAdvanceTracksBytes()
    {
        using var pipe = new SpscPipe();
        var mem = pipe.Writer.GetMemory(100);
        Assert.True(mem.Length >= 100);
        for (int i = 0; i < 100; i++) mem.Span[i] = (byte)i;
        pipe.Writer.Advance(100);

        // Subsequent GetMemory returns the next slice.
        var mem2 = pipe.Writer.GetMemory(0);
        Assert.True(mem2.Length >= 1);
    }

    [Fact]
    public void GetMemory_TransitionsToNewSegmentWhenSizeHintExceedsRemaining()
    {
        using var pipe = new SpscPipe(new SpscPipeOptions(minimumSegmentSize: 64));
        var mem1 = pipe.Writer.GetMemory(64);
        pipe.Writer.Advance(50);

        var mem2 = pipe.Writer.GetMemory(64);   // forces new segment
        Assert.True(mem2.Length >= 64);
    }

    [Fact]
    public void Advance_BeyondCapacity_Throws()
    {
        using var pipe = new SpscPipe();
        pipe.Writer.GetMemory(1);
        Assert.Throws<ArgumentOutOfRangeException>(() => pipe.Writer.Advance(int.MaxValue));
    }

    [Fact]
    public void GetMemory_AfterComplete_Throws()
    {
        using var pipe = new SpscPipe();
        // Manually set the internal flag to test the entry guard. Complete is wired in Task 9.
        // SpscPipe.Tests has InternalsVisibleTo, so direct field access works.
        pipe._writerCompleted = true;

        Assert.Throws<InvalidOperationException>(() => pipe.Writer.GetMemory(0));
    }

    [Fact]
    public void GetMemory_AfterDispose_Throws()
    {
        var pipe = new SpscPipe();
        pipe.Dispose();
        Assert.Throws<ObjectDisposedException>(() => pipe.Writer.GetMemory(0));
    }
}
```

- [ ] **Step 8: Run all writer tests; expect PASS**

```bash
dotnet test tests/SpscPipe.Tests/ --filter "FullyQualifiedName~SpscPipeWriterTests"
```

- [ ] **Step 9: Commit**

```bash
git add src/SpscPipelines/ tests/SpscPipe.Tests/
git commit -m "SpscPipe: scaffolding + GetMemory/GetSpan/Advance + writer entry guards"
```

---

## Task 5: FlushAsync sync path + SignalReadAwaiterIfPending

**Goal:** Implement `FlushAsync` per Spec §4 (sync fast path with throw-first ordering, sticky cancel consume, publish, signal). The signaler stub returns immediately if no parked reader (no full Pattern 2 construction yet — Task 8 adds that).

**Files:**
- Modify: `src/SpscPipelines/SpscPipe.Writer.cs`
- Modify: `src/SpscPipelines/SpscPipe.cs` (add SignalReadAwaiterIfPending; BuildFlushResult)

- [ ] **Step 1: Add `BuildFlushResult` and `SignalReadAwaiterIfPending` to `SpscPipe.cs`**

```csharp
internal FlushResult BuildFlushResult(bool isCanceled)
    => new(isCanceled, isCompleted: _lastAcquiredReaderState.IsCompleted);

internal void SignalReadAwaiterIfPending()
{
    while (true)
    {
        int oldV = _readAwaiter._state;
        if ((oldV & SpscAwaiter<ReadResult>.StateMask) != SpscAwaiter<ReadResult>.Pending) return;
        int desired = oldV & ~SpscAwaiter<ReadResult>.StateMask;
        if (Interlocked.CompareExchange(ref _readAwaiter._state, desired, oldV) == oldV)
        {
            _readAwaiter._ctr.Dispose();
            // Pattern 2 construction happens here in Task 8. For now, deliver default.
            // This intermediate behavior won't be exposed to users until ReadAsync is wired (Task 6),
            // and the parking path is wired (Task 8). The sync fast path doesn't reach here.
            _readAwaiter._core.SetResult(default);
            return;
        }
    }
}
```

(Implementer: Task 8 replaces the `SetResult(default)` placeholder with the full Pattern 2 construction.)

- [ ] **Step 2: Implement `FlushAsync` in `SpscPipe.Writer.cs`**

Replace the `NotImplementedException` body:

```csharp
public override ValueTask<FlushResult> FlushAsync(CancellationToken ct = default)
{
    if (_pipe._disposed) throw new ObjectDisposedException(nameof(SpscPipe));
    if (_pipe._writerCompleted) throw new InvalidOperationException("Writing is completed.");

    // Throw-first: refresh reader state, then throw if reader-completed-with-ex.
    if (_pipe._readerTb.TryAcquire())
        _pipe._lastAcquiredReaderState = _pipe._readerTb.ConsumerSlot();

    if (_pipe._lastAcquiredReaderState.IsCompleted && _pipe._lastAcquiredReaderState.CompletionException != null)
        System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(_pipe._lastAcquiredReaderState.CompletionException);

    // Sync entry: consume sticky CancelPendingFlush flag.
    while (true)
    {
        int oldV = _pipe._flushAwaiter._state;
        if ((oldV & SpscAwaiter<FlushResult>.CancelFlag) == 0) break;
        int desired = oldV & ~SpscAwaiter<FlushResult>.CancelFlag;
        if (Interlocked.CompareExchange(ref _pipe._flushAwaiter._state, desired, oldV) == oldV)
            return new ValueTask<FlushResult>(_pipe.BuildFlushResult(isCanceled: true));
    }

    if (ct.IsCancellationRequested)
        return ValueTask.FromCanceled<FlushResult>(ct);

    // Build state, publish, signal.
    var snapshot = new WriterState
    {
        HeadSegment        = _pipe._chainHead,
        TailSegment        = _pipe._writingHead,
        TailWritten        = _pipe._writingHeadBytesBuffered,
        TotalWritten       = _pipe._totalWritten,
        IsCompleted        = false,
        CompletionException = null,
    };
    _pipe._writerTb.ProducerSlot() = snapshot;
    _pipe._writerTb.Publish();
    _pipe._lastPublishedWriterState = snapshot;

    _pipe.SignalReadAwaiterIfPending();

    // Re-acquire for backpressure freshness.
    if (_pipe._readerTb.TryAcquire())
        _pipe._lastAcquiredReaderState = _pipe._readerTb.ConsumerSlot();
    _pipe.RecycleDrainedSegments();

    if (_pipe._lastAcquiredReaderState.IsCompleted && _pipe._lastAcquiredReaderState.CompletionException != null)
        System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(_pipe._lastAcquiredReaderState.CompletionException);

    long unconsumed = _pipe._totalWritten - _pipe._lastAcquiredReaderState.TotalConsumed;
    bool readerDone = _pipe._lastAcquiredReaderState.IsCompleted;
    bool needsPark = _pipe._options.PauseWriterThreshold > 0
                     && unconsumed >= _pipe._options.PauseWriterThreshold
                     && !readerDone;

    if (!needsPark)
        return new ValueTask<FlushResult>(_pipe.BuildFlushResult(isCanceled: false));

    // Parking implemented in Task 8 — for now, throw to make the unimplemented path explicit.
    throw new NotImplementedException("Flush parking implemented in Task 8.");
}
```

- [ ] **Step 3: Add `RecycleDrainedSegments` to `SpscPipe.cs`**

```csharp
internal void RecycleDrainedSegments()
{
    var r = _lastAcquiredReaderState;
    if (r.HeadSegment is null && !r.IsCompleted) return;       // pre-bootstrap

    var readerHead = r.HeadSegment;

    while (_chainHead != _writingHead && _chainHead != readerHead)
    {
        var recycled = _chainHead!;
        _chainHead   = recycled.Next!;
        PushFreelist(recycled);
    }
}
```

- [ ] **Step 4: Write FlushAsync tests**

Append to `SpscPipeWriterTests.cs`:

```csharp
[Fact]
public async Task FlushAsync_NoBackpressure_ReturnsImmediatelyNotCompleted()
{
    using var pipe = new SpscPipe();
    pipe.Writer.GetMemory(10);
    pipe.Writer.Advance(10);

    var result = await pipe.Writer.FlushAsync();
    Assert.False(result.IsCanceled);
    Assert.False(result.IsCompleted);
}

[Fact]
public async Task FlushAsync_AfterReaderCompletedNull_ReturnsIsCompletedTrue()
{
    using var pipe = new SpscPipe();
    // Manually publish a reader-completed state via the readerTb (proxy for Reader.Complete which is Task 9).
    pipe.Writer.GetMemory(10); pipe.Writer.Advance(10);
    var readerSnap = new ReaderState { IsCompleted = true, CompletionException = null };
    pipe._readerTb.ProducerSlot() = readerSnap;
    pipe._readerTb.Publish();

    var result = await pipe.Writer.FlushAsync();
    Assert.True(result.IsCompleted);
    Assert.False(result.IsCanceled);
}

[Fact]
public async Task FlushAsync_AfterReaderCompletedException_Throws()
{
    using var pipe = new SpscPipe();
    pipe.Writer.GetMemory(10); pipe.Writer.Advance(10);
    var ex = new InvalidOperationException("from reader");
    pipe._readerTb.ProducerSlot() = new ReaderState { IsCompleted = true, CompletionException = ex };
    pipe._readerTb.Publish();

    var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await pipe.Writer.FlushAsync());
    Assert.Same(ex, thrown);
}
```

- [ ] **Step 5: Run; expect PASS**

```bash
dotnet test tests/SpscPipe.Tests/ --filter "FullyQualifiedName~SpscPipeWriterTests"
```

- [ ] **Step 6: Commit**

```bash
git add src/SpscPipelines/ tests/SpscPipe.Tests/
git commit -m "SpscPipe.Writer: FlushAsync sync path + signaler stub + recycle"
```

---

## Task 6: ReadAsync/TryRead sync path + IntegrateAcquiredWriterState + bootstrap

**Goal:** Implement reader sync paths per Spec §4. Throw-first precedence; sticky cancel consume; bootstrap from `WriterState.HeadSegment`.

**Files:**
- Modify: `src/SpscPipelines/SpscPipe.Reader.cs`
- Modify: `src/SpscPipelines/SpscPipe.cs` (add IntegrateAcquiredWriterState, HasReadableProgress, BuildReadResult)
- Create: `tests/SpscPipe.Tests/SpscPipeReaderTests.cs`

- [ ] **Step 1: Add reader helpers to `SpscPipe.cs`**

```csharp
internal bool HasReadableProgress() => _lastAcquiredWriterState.TotalWritten > _totalExamined;

internal void IntegrateAcquiredWriterState()
{
    var w = _lastAcquiredWriterState;
    if (_readHead == null)                  // I10 bootstrap
    {
        _readHead    = w.HeadSegment;
        _readHeadIdx = 0;
    }
    _readTail    = w.TailSegment;
    _readTailIdx = w.TailWritten;
}

internal ReadResult BuildReadResult(bool isCanceled)
{
    bool isCompleted = _lastAcquiredWriterState.IsCompleted;
    var buffer = _readHead == null
        ? ReadOnlySequence<byte>.Empty
        : new ReadOnlySequence<byte>(_readHead, _readHeadIdx, _readTail!, _readTailIdx);
    return new ReadResult(buffer, isCanceled, isCompleted);
}
```

- [ ] **Step 2: Implement `ReadAsync` in `SpscPipe.Reader.cs`**

Replace the `NotImplementedException`:

```csharp
public override ValueTask<ReadResult> ReadAsync(CancellationToken ct = default)
{
    if (_pipe._disposed) throw new ObjectDisposedException(nameof(SpscPipe));
    if (_pipe._readerCompleted) throw new InvalidOperationException("Reading is completed.");

    if (_pipe._writerTb.TryAcquire())
    {
        _pipe._lastAcquiredWriterState = _pipe._writerTb.ConsumerSlot();
        _pipe.IntegrateAcquiredWriterState();
    }

    if (_pipe._lastAcquiredWriterState.IsCompleted && _pipe._lastAcquiredWriterState.CompletionException != null)
        System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(_pipe._lastAcquiredWriterState.CompletionException);

    while (true)
    {
        int oldV = _pipe._readAwaiter._state;
        if ((oldV & SpscAwaiter<ReadResult>.CancelFlag) == 0) break;
        int desired = oldV & ~SpscAwaiter<ReadResult>.CancelFlag;
        if (Interlocked.CompareExchange(ref _pipe._readAwaiter._state, desired, oldV) == oldV)
            return new ValueTask<ReadResult>(_pipe.BuildReadResult(isCanceled: true));
    }

    if (ct.IsCancellationRequested)
        return ValueTask.FromCanceled<ReadResult>(ct);

    if (_pipe.HasReadableProgress() || _pipe._lastAcquiredWriterState.IsCompleted)
        return new ValueTask<ReadResult>(_pipe.BuildReadResult(isCanceled: false));

    // Parking implemented in Task 8.
    throw new NotImplementedException("Read parking implemented in Task 8.");
}
```

- [ ] **Step 3: Implement `TryRead`**

```csharp
public override bool TryRead(out ReadResult result)
{
    if (_pipe._disposed) throw new ObjectDisposedException(nameof(SpscPipe));
    if (_pipe._readerCompleted) throw new InvalidOperationException("Reading is completed.");

    if (_pipe._writerTb.TryAcquire())
    {
        _pipe._lastAcquiredWriterState = _pipe._writerTb.ConsumerSlot();
        _pipe.IntegrateAcquiredWriterState();
    }

    if (_pipe._lastAcquiredWriterState.IsCompleted && _pipe._lastAcquiredWriterState.CompletionException != null)
        System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(_pipe._lastAcquiredWriterState.CompletionException);

    while (true)
    {
        int oldV = _pipe._readAwaiter._state;
        if ((oldV & SpscAwaiter<ReadResult>.CancelFlag) == 0) break;
        int desired = oldV & ~SpscAwaiter<ReadResult>.CancelFlag;
        if (Interlocked.CompareExchange(ref _pipe._readAwaiter._state, desired, oldV) == oldV)
        {
            result = _pipe.BuildReadResult(isCanceled: true);
            return true;
        }
    }

    if (_pipe.HasReadableProgress() || _pipe._lastAcquiredWriterState.IsCompleted)
    {
        result = _pipe.BuildReadResult(isCanceled: false);
        return true;
    }

    result = default;
    return false;
}
```

- [ ] **Step 4: Write reader tests**

Create `tests/SpscPipe.Tests/SpscPipeReaderTests.cs`:

```csharp
using System.Buffers;
using SpscPipelines;
using Xunit;

namespace SpscPipe.Tests;

public class SpscPipeReaderTests
{
    [Fact]
    public async Task ReadAsync_AfterFlushedData_ReturnsBuffer()
    {
        using var pipe = new SpscPipe();
        var mem = pipe.Writer.GetMemory(5);
        mem.Span[0] = 1; mem.Span[1] = 2; mem.Span[2] = 3; mem.Span[3] = 4; mem.Span[4] = 5;
        pipe.Writer.Advance(5);
        await pipe.Writer.FlushAsync();

        var result = await pipe.Reader.ReadAsync();
        Assert.False(result.IsCanceled);
        Assert.False(result.IsCompleted);
        Assert.Equal(5, result.Buffer.Length);
        var arr = result.Buffer.ToArray();
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5 }, arr);
    }

    [Fact]
    public void TryRead_NoData_ReturnsFalse()
    {
        using var pipe = new SpscPipe();
        Assert.False(pipe.Reader.TryRead(out var result));
    }

    [Fact]
    public async Task ReadAsync_AfterWriterCompletedNull_ReturnsIsCompletedTrue()
    {
        using var pipe = new SpscPipe();
        // Simulate Writer.Complete(null) by direct WriterState publish (real Complete in Task 9).
        pipe._writerTb.ProducerSlot() = new WriterState { IsCompleted = true };
        pipe._writerTb.Publish();

        var result = await pipe.Reader.ReadAsync();
        Assert.True(result.IsCompleted);
        Assert.True(result.Buffer.IsEmpty);
    }

    [Fact]
    public async Task ReadAsync_AfterWriterCompletedWithEx_Throws()
    {
        using var pipe = new SpscPipe();
        var ex = new InvalidOperationException("from writer");
        pipe._writerTb.ProducerSlot() = new WriterState { IsCompleted = true, CompletionException = ex };
        pipe._writerTb.Publish();

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await pipe.Reader.ReadAsync());
        Assert.Same(ex, thrown);
    }
}
```

- [ ] **Step 5: Run; expect PASS**

```bash
dotnet test tests/SpscPipe.Tests/ --filter "FullyQualifiedName~SpscPipeReaderTests"
```

- [ ] **Step 6: Commit**

```bash
git add src/SpscPipelines/ tests/SpscPipe.Tests/
git commit -m "SpscPipe.Reader: ReadAsync/TryRead sync paths + bootstrap + throw-first"
```

---

## Task 7: AdvanceTo + PublishReaderState + SignalFlushIfBackpressureRelieved

**Goal:** Close the cycle — reader's `AdvanceTo` updates state, publishes, gates the flush signal per Spec §4 / R2-1.

**Files:**
- Modify: `src/SpscPipelines/SpscPipe.Reader.cs`
- Modify: `src/SpscPipelines/SpscPipe.cs`
- Create: `tests/SpscPipe.Tests/SpscPipeAdvanceToTests.cs`

- [ ] **Step 1: Implement `AdvanceTo` (both overloads) in `SpscPipe.Reader.cs`**

```csharp
public override void AdvanceTo(SequencePosition consumed) => AdvanceTo(consumed, consumed);

public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
{
    if (_pipe._disposed) throw new ObjectDisposedException(nameof(SpscPipe));
    if (_pipe._readerCompleted) throw new InvalidOperationException("Reading is completed.");

    var consumedSeg = consumed.GetObject() as BufferSegment;
    var examinedSeg = examined.GetObject() as BufferSegment;

    if (consumedSeg == null && examinedSeg == null)
    {
        _pipe.PublishReaderState();
        return;
    }
    if (consumedSeg == null || examinedSeg == null)
        throw new InvalidOperationException("AdvanceTo: mixed null/non-null SequencePositions");

    // R4-7: pipe-identity check.
    if (!ReferenceEquals(consumedSeg.OwnerToken, _pipe) || !ReferenceEquals(examinedSeg.OwnerToken, _pipe))
        throw new InvalidOperationException("AdvanceTo: SequencePosition is from a different pipe.");

    int consumedIdx = consumed.GetInteger();
    int examinedIdx = examined.GetInteger();

    long consumedAbs = consumedSeg.RunningIndex + consumedIdx;
    long examinedAbs = examinedSeg.RunningIndex + examinedIdx;

    // Refresh writer state for upper-bound validation.
    if (_pipe._writerTb.TryAcquire())
    {
        _pipe._lastAcquiredWriterState = _pipe._writerTb.ConsumerSlot();
        _pipe.IntegrateAcquiredWriterState();
    }

    if (consumedAbs < _pipe._totalConsumed
        || examinedAbs < _pipe._totalExamined
        || consumedAbs > examinedAbs
        || examinedAbs > _pipe._lastAcquiredWriterState.TotalWritten)
    {
        throw new InvalidOperationException("AdvanceTo position out of range");
    }

    _pipe._readHead       = consumedSeg;
    _pipe._readHeadIdx    = consumedIdx;
    _pipe._totalConsumed  = consumedAbs;
    _pipe._totalExamined  = examinedAbs;

    _pipe.PublishReaderState();
}
```

- [ ] **Step 2: Add `PublishReaderState` and `SignalFlushIfBackpressureRelieved` to `SpscPipe.cs`**

```csharp
internal void PublishReaderState()
{
    var snapshot = new ReaderState
    {
        HeadSegment   = _readHead,
        TotalConsumed = _totalConsumed,
        TotalExamined = _totalExamined,
        IsCompleted   = false,
        CompletionException = null,
    };
    _readerTb.ProducerSlot() = snapshot;
    _readerTb.Publish();
    _lastPublishedReaderState = snapshot;

    SignalFlushIfBackpressureRelieved();
}

internal void SignalFlushIfBackpressureRelieved()
{
    if ((_flushAwaiter._state & SpscAwaiter<FlushResult>.StateMask) != SpscAwaiter<FlushResult>.Pending) return;

    if (_writerTb.TryAcquire())
    {
        _lastAcquiredWriterState = _writerTb.ConsumerSlot();
        IntegrateAcquiredWriterState();
    }

    long unconsumed = _lastAcquiredWriterState.TotalWritten - _totalConsumed;
    if (unconsumed >= _options.ResumeWriterThreshold) return;

    while (true)
    {
        int oldV = _flushAwaiter._state;
        if ((oldV & SpscAwaiter<FlushResult>.StateMask) != SpscAwaiter<FlushResult>.Pending) return;
        int desired = oldV & ~SpscAwaiter<FlushResult>.StateMask;
        if (Interlocked.CompareExchange(ref _flushAwaiter._state, desired, oldV) == oldV)
        {
            _flushAwaiter._ctr.Dispose();
            DeliverFlushResult();
            return;
        }
    }
}

internal void SignalFlushAwaiterIfPending()       // unconditional; called from Reader.Complete
{
    while (true)
    {
        int oldV = _flushAwaiter._state;
        if ((oldV & SpscAwaiter<FlushResult>.StateMask) != SpscAwaiter<FlushResult>.Pending) return;
        int desired = oldV & ~SpscAwaiter<FlushResult>.StateMask;
        if (Interlocked.CompareExchange(ref _flushAwaiter._state, desired, oldV) == oldV)
        {
            _flushAwaiter._ctr.Dispose();
            DeliverFlushResult();
            return;
        }
    }
}

private void DeliverFlushResult()
{
    var r = _lastPublishedReaderState;
    if (r.IsCompleted && r.CompletionException != null)
        _flushAwaiter._core.SetException(r.CompletionException);
    else
        _flushAwaiter._core.SetResult(new FlushResult(isCanceled: false, isCompleted: r.IsCompleted));
}
```

- [ ] **Step 3: Write `SpscPipeAdvanceToTests.cs`**

```csharp
using SpscPipelines;
using Xunit;

namespace SpscPipe.Tests;

public class SpscPipeAdvanceToTests
{
    [Fact]
    public async Task AdvanceTo_PartialConsume_PreservesRemainder()
    {
        using var pipe = new SpscPipe();
        var mem = pipe.Writer.GetMemory(10);
        for (int i = 0; i < 10; i++) mem.Span[i] = (byte)i;
        pipe.Writer.Advance(10);
        await pipe.Writer.FlushAsync();

        var r1 = await pipe.Reader.ReadAsync();
        var consumed = r1.Buffer.GetPosition(5);
        pipe.Reader.AdvanceTo(consumed);

        var r2 = await pipe.Reader.ReadAsync();
        Assert.Equal(5, r2.Buffer.Length);
        Assert.Equal(new byte[] { 5, 6, 7, 8, 9 }, r2.Buffer.ToArray());
    }

    [Fact]
    public async Task AdvanceTo_FullConsume_NextReadHasEmptyBuffer_IfNoData()
    {
        using var pipe = new SpscPipe();
        var mem = pipe.Writer.GetMemory(10);
        for (int i = 0; i < 10; i++) mem.Span[i] = (byte)i;
        pipe.Writer.Advance(10);
        await pipe.Writer.FlushAsync();

        var r1 = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(r1.Buffer.End);

        Assert.False(pipe.Reader.TryRead(out _));
    }

    [Fact]
    public async Task AdvanceTo_BackwardsConsumed_Throws()
    {
        using var pipe = new SpscPipe();
        var mem = pipe.Writer.GetMemory(10); pipe.Writer.Advance(10);
        await pipe.Writer.FlushAsync();
        var r1 = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(r1.Buffer.GetPosition(5));

        var r2 = await pipe.Reader.ReadAsync();
        // Try to "advance" backwards (consumed before _totalConsumed).
        Assert.Throws<InvalidOperationException>(() => pipe.Reader.AdvanceTo(r1.Buffer.Start));
    }

    [Fact]
    public async Task AdvanceTo_OnEmptyBuffer_DoesNotThrow_WithDefaultPositions()
    {
        using var pipe = new SpscPipe();
        // Simulate empty IsCompleted=true ReadResult by direct publish.
        pipe._writerTb.ProducerSlot() = new WriterState { IsCompleted = true };
        pipe._writerTb.Publish();

        var r = await pipe.Reader.ReadAsync();
        Assert.True(r.IsCompleted);
        Assert.True(r.Buffer.IsEmpty);

        // Should not throw.
        pipe.Reader.AdvanceTo(r.Buffer.Start, r.Buffer.End);
    }

    [Fact]
    public async Task AdvanceTo_FromDifferentPipe_Throws()
    {
        using var pipe1 = new SpscPipe();
        using var pipe2 = new SpscPipe();

        pipe1.Writer.GetMemory(5); pipe1.Writer.Advance(5);
        await pipe1.Writer.FlushAsync();
        var r1 = await pipe1.Reader.ReadAsync();

        // Try to AdvanceTo on pipe2 with positions from pipe1 — should throw (R4-7).
        Assert.Throws<InvalidOperationException>(() => pipe2.Reader.AdvanceTo(r1.Buffer.End));
    }
}
```

- [ ] **Step 4: Run; expect PASS**

```bash
dotnet test tests/SpscPipe.Tests/ --filter "FullyQualifiedName~SpscPipeAdvanceToTests"
```

- [ ] **Step 5: Commit**

```bash
git add src/SpscPipelines/ tests/SpscPipe.Tests/
git commit -m "SpscPipe.Reader: AdvanceTo + PublishReaderState + gated flush signaler + R4-7"
```

---

## Task 8: Park paths (ParkReadAwaiter + ParkFlushAwaiter) + Pattern 2 completion

**Goal:** Implement parking and the full Pattern 2 stash-and-construct signaler completion.

**Files:**
- Modify: `src/SpscPipelines/SpscPipe.Reader.cs`
- Modify: `src/SpscPipelines/SpscPipe.Writer.cs`
- Modify: `src/SpscPipelines/SpscPipe.cs`

- [ ] **Step 1: Replace `SignalReadAwaiterIfPending` placeholder with full Pattern 2 construction**

In `SpscPipe.cs`:

```csharp
internal void SignalReadAwaiterIfPending()
{
    while (true)
    {
        int oldV = _readAwaiter._state;
        if ((oldV & SpscAwaiter<ReadResult>.StateMask) != SpscAwaiter<ReadResult>.Pending) return;
        int desired = oldV & ~SpscAwaiter<ReadResult>.StateMask;
        if (Interlocked.CompareExchange(ref _readAwaiter._state, desired, oldV) == oldV)
        {
            _readAwaiter._ctr.Dispose();

            var w = _lastPublishedWriterState;

            if (w.IsCompleted && w.CompletionException != null)
            {
                _readAwaiter._core.SetException(w.CompletionException);
                return;
            }

            var head    = _readAwaiter._stashHead ?? w.HeadSegment;
            var headIdx = _readAwaiter._stashHead == null ? 0 : _readAwaiter._stashHeadIdx;

            var buffer = head == null
                ? ReadOnlySequence<byte>.Empty
                : new ReadOnlySequence<byte>(head, headIdx, w.TailSegment!, w.TailWritten);

            _readAwaiter._core.SetResult(new ReadResult(buffer, isCanceled: false, isCompleted: w.IsCompleted));
            return;
        }
    }
}
```

- [ ] **Step 2: Add `ParkReadAwaiter` to `SpscPipe.Reader.cs`**

```csharp
private ValueTask<ReadResult> ParkReadAwaiter(CancellationToken ct)
{
    _pipe._readAwaiter._ctr.Dispose();        // R5b cleanup
    _pipe._readAwaiter._core.Reset();
    _pipe._readAwaiter._token = ct;

    _pipe._readAwaiter._stashHead    = _pipe._readHead;
    _pipe._readAwaiter._stashHeadIdx = _pipe._readHeadIdx;
    _pipe._readAwaiter._stashTail    = _pipe._readTail;
    _pipe._readAwaiter._stashTailIdx = _pipe._readTailIdx;

    while (true)
    {
        int oldV = _pipe._readAwaiter._state;
        Debug.Assert((oldV & SpscAwaiter<ReadResult>.StateMask) == SpscAwaiter<ReadResult>.Inactive);
        int desired = (oldV & SpscAwaiter<ReadResult>.CancelFlag) | SpscAwaiter<ReadResult>.Pending;
        if (Interlocked.CompareExchange(ref _pipe._readAwaiter._state, desired, oldV) == oldV) break;
    }

    // Lost-wakeup re-check (throw-first).
    if (_pipe._writerTb.TryAcquire())
    {
        _pipe._lastAcquiredWriterState = _pipe._writerTb.ConsumerSlot();
        _pipe.IntegrateAcquiredWriterState();

        if (_pipe._lastAcquiredWriterState.IsCompleted && _pipe._lastAcquiredWriterState.CompletionException != null)
        {
            while (true)
            {
                int oldV = _pipe._readAwaiter._state;
                if ((oldV & SpscAwaiter<ReadResult>.StateMask) != SpscAwaiter<ReadResult>.Pending) break;
                int desired = oldV & ~SpscAwaiter<ReadResult>.StateMask;
                if (Interlocked.CompareExchange(ref _pipe._readAwaiter._state, desired, oldV) == oldV)
                {
                    _pipe._readAwaiter._core.SetException(_pipe._lastAcquiredWriterState.CompletionException);
                    return new ValueTask<ReadResult>(_pipe._readAwaiter, _pipe._readAwaiter.Version);
                }
            }
        }

        if (_pipe.HasReadableProgress() || _pipe._lastAcquiredWriterState.IsCompleted)
        {
            while (true)
            {
                int oldV = _pipe._readAwaiter._state;
                if ((oldV & SpscAwaiter<ReadResult>.StateMask) != SpscAwaiter<ReadResult>.Pending) break;
                int desired = oldV & ~SpscAwaiter<ReadResult>.StateMask;
                if (Interlocked.CompareExchange(ref _pipe._readAwaiter._state, desired, oldV) == oldV)
                    return new ValueTask<ReadResult>(_pipe.BuildReadResult(isCanceled: false));
            }
        }
    }

    // Lost-cancel re-check.
    int v = _pipe._readAwaiter._state;
    if ((v & SpscAwaiter<ReadResult>.CancelFlag) != 0
        && Interlocked.CompareExchange(
               ref _pipe._readAwaiter._state,
               SpscAwaiter<ReadResult>.Inactive,
               SpscAwaiter<ReadResult>.Pending | SpscAwaiter<ReadResult>.CancelFlag)
           == (SpscAwaiter<ReadResult>.Pending | SpscAwaiter<ReadResult>.CancelFlag))
    {
        _pipe._readAwaiter._core.SetResult(_pipe.BuildReadResult(isCanceled: true));
        return new ValueTask<ReadResult>(_pipe._readAwaiter, _pipe._readAwaiter.Version);
    }

    _pipe._readAwaiter._ctr = ct.UnsafeRegister(static p => ((SpscPipe)p!).OnReadAwaiterTokenCancel(), _pipe);
    if ((_pipe._readAwaiter._state & SpscAwaiter<ReadResult>.StateMask) != SpscAwaiter<ReadResult>.Pending)
        _pipe._readAwaiter._ctr.Dispose();
    return new ValueTask<ReadResult>(_pipe._readAwaiter, _pipe._readAwaiter.Version);
}
```

Replace the `throw new NotImplementedException("Read parking implemented in Task 8.")` in `ReadAsync` with `return ParkReadAwaiter(ct);`.

- [ ] **Step 3: Add `OnReadAwaiterTokenCancel` to `SpscPipe.cs`**

```csharp
internal void OnReadAwaiterTokenCancel()
{
    while (true)
    {
        int oldV = _readAwaiter._state;
        if ((oldV & SpscAwaiter<ReadResult>.StateMask) != SpscAwaiter<ReadResult>.Pending) return;
        int desired = oldV & ~SpscAwaiter<ReadResult>.StateMask;
        if (Interlocked.CompareExchange(ref _readAwaiter._state, desired, oldV) == oldV)
        {
            _readAwaiter._core.SetException(new OperationCanceledException(_readAwaiter._token));
            return;
        }
    }
}
```

- [ ] **Step 4: Add `ParkFlushAwaiter` to `SpscPipe.Writer.cs`**

```csharp
private ValueTask<FlushResult> ParkFlushAwaiter(CancellationToken ct)
{
    _pipe._flushAwaiter._ctr.Dispose();        // R5b cleanup
    _pipe._flushAwaiter._core.Reset();
    _pipe._flushAwaiter._token = ct;

    while (true)
    {
        int oldV = _pipe._flushAwaiter._state;
        Debug.Assert((oldV & SpscAwaiter<FlushResult>.StateMask) == SpscAwaiter<FlushResult>.Inactive,
                     "SPSC violation: concurrent FlushAsync");
        int desired = (oldV & SpscAwaiter<FlushResult>.CancelFlag) | SpscAwaiter<FlushResult>.Pending;
        if (Interlocked.CompareExchange(ref _pipe._flushAwaiter._state, desired, oldV) == oldV) break;
    }

    // Lost-wakeup re-check (throw-first).
    if (_pipe._readerTb.TryAcquire())
    {
        _pipe._lastAcquiredReaderState = _pipe._readerTb.ConsumerSlot();

        if (_pipe._lastAcquiredReaderState.IsCompleted && _pipe._lastAcquiredReaderState.CompletionException != null)
        {
            while (true)
            {
                int oldV = _pipe._flushAwaiter._state;
                if ((oldV & SpscAwaiter<FlushResult>.StateMask) != SpscAwaiter<FlushResult>.Pending) break;
                int desired = oldV & ~SpscAwaiter<FlushResult>.StateMask;
                if (Interlocked.CompareExchange(ref _pipe._flushAwaiter._state, desired, oldV) == oldV)
                {
                    _pipe._flushAwaiter._core.SetException(_pipe._lastAcquiredReaderState.CompletionException);
                    return new ValueTask<FlushResult>(_pipe._flushAwaiter, _pipe._flushAwaiter.Version);
                }
            }
        }

        long unconsumed = _pipe._totalWritten - _pipe._lastAcquiredReaderState.TotalConsumed;
        bool releasable = unconsumed < _pipe._options.ResumeWriterThreshold
                          || _pipe._lastAcquiredReaderState.IsCompleted;

        if (releasable)
        {
            while (true)
            {
                int oldV = _pipe._flushAwaiter._state;
                if ((oldV & SpscAwaiter<FlushResult>.StateMask) != SpscAwaiter<FlushResult>.Pending) break;
                int desired = oldV & ~SpscAwaiter<FlushResult>.StateMask;
                if (Interlocked.CompareExchange(ref _pipe._flushAwaiter._state, desired, oldV) == oldV)
                    return new ValueTask<FlushResult>(_pipe.BuildFlushResult(isCanceled: false));
            }
        }
    }

    // Lost-cancel re-check.
    int v = _pipe._flushAwaiter._state;
    if ((v & SpscAwaiter<FlushResult>.CancelFlag) != 0
        && Interlocked.CompareExchange(
               ref _pipe._flushAwaiter._state,
               SpscAwaiter<FlushResult>.Inactive,
               SpscAwaiter<FlushResult>.Pending | SpscAwaiter<FlushResult>.CancelFlag)
           == (SpscAwaiter<FlushResult>.Pending | SpscAwaiter<FlushResult>.CancelFlag))
    {
        _pipe._flushAwaiter._core.SetResult(_pipe.BuildFlushResult(isCanceled: true));
        return new ValueTask<FlushResult>(_pipe._flushAwaiter, _pipe._flushAwaiter.Version);
    }

    _pipe._flushAwaiter._ctr = ct.UnsafeRegister(static p => ((SpscPipe)p!).OnFlushAwaiterTokenCancel(), _pipe);
    if ((_pipe._flushAwaiter._state & SpscAwaiter<FlushResult>.StateMask) != SpscAwaiter<FlushResult>.Pending)
        _pipe._flushAwaiter._ctr.Dispose();
    return new ValueTask<FlushResult>(_pipe._flushAwaiter, _pipe._flushAwaiter.Version);
}
```

Replace the `throw new NotImplementedException("Flush parking implemented in Task 8.")` in `FlushAsync` (Task 5 final line) with `return ParkFlushAwaiter(ct);`.

- [ ] **Step 4b: Add `OnFlushAwaiterTokenCancel` to `SpscPipe.cs`**

```csharp
internal void OnFlushAwaiterTokenCancel()
{
    while (true)
    {
        int oldV = _flushAwaiter._state;
        if ((oldV & SpscAwaiter<FlushResult>.StateMask) != SpscAwaiter<FlushResult>.Pending) return;
        int desired = oldV & ~SpscAwaiter<FlushResult>.StateMask;
        if (Interlocked.CompareExchange(ref _flushAwaiter._state, desired, oldV) == oldV)
        {
            _flushAwaiter._core.SetException(new OperationCanceledException(_flushAwaiter._token));
            return;
        }
    }
}
```

- [ ] **Step 5: Add park/wake tests**

Append to `SpscPipeReaderTests.cs`:

```csharp
[Fact]
public async Task ReadAsync_ParksWhenNoData_ResumesOnFlush()
{
    using var pipe = new SpscPipe();
    var readTask = pipe.Reader.ReadAsync().AsTask();
    Assert.False(readTask.IsCompleted);

    // Writer side on a different thread.
    await Task.Run(async () =>
    {
        var mem = pipe.Writer.GetMemory(3);
        mem.Span[0] = 7; mem.Span[1] = 8; mem.Span[2] = 9;
        pipe.Writer.Advance(3);
        await pipe.Writer.FlushAsync();
    });

    var result = await readTask.WaitAsync(TimeSpan.FromSeconds(5));
    Assert.Equal(new byte[] { 7, 8, 9 }, result.Buffer.ToArray());
}
```

Append to `SpscPipeWriterTests.cs`:

```csharp
[Fact]
public async Task FlushAsync_ParksOnBackpressure_ResumesOnAdvance()
{
    using var pipe = new SpscPipe(new SpscPipeOptions(
        pauseWriterThreshold: 100,
        resumeWriterThreshold: 50));

    // Fill above pause threshold.
    var mem = pipe.Writer.GetMemory(150);
    pipe.Writer.Advance(150);

    var flushTask = pipe.Writer.FlushAsync().AsTask();
    Assert.False(flushTask.IsCompleted);

    // Reader drains enough to drop below resume threshold.
    await Task.Run(async () =>
    {
        var r = await pipe.Reader.ReadAsync();
        // Consume 110 bytes (leaves 40 unconsumed; below resume threshold of 50).
        pipe.Reader.AdvanceTo(r.Buffer.GetPosition(110));
    });

    var result = await flushTask.WaitAsync(TimeSpan.FromSeconds(5));
    Assert.False(result.IsCanceled);
}
```

Run; expect PASS.

- [ ] **Step 6: Commit**

```bash
git add src/SpscPipelines/ tests/SpscPipe.Tests/
git commit -m "SpscPipe: park paths + Pattern 2 signaler completion"
```

---

## Task 9: Complete (both sides) + R7 throw integration

**Goal:** Implement `Reader.Complete(ex?)` and `Writer.Complete(ex?)` per Spec §6.

**Files:**
- Modify: `src/SpscPipelines/SpscPipe.Writer.cs`
- Modify: `src/SpscPipelines/SpscPipe.Reader.cs`
- Create: `tests/SpscPipe.Tests/SpscPipeLifecycleTests.cs`

- [ ] **Step 1: Implement `Writer.Complete`**

Replace the `NotImplementedException` in `SpscPipe.Writer.cs`:

```csharp
public override void Complete(Exception? exception = null)
{
    if (_pipe._disposed) throw new ObjectDisposedException(nameof(SpscPipe));
    if (_pipe._writerCompleted) return;     // double-Complete coalesces
    _pipe._writerCompleted = true;

    var snapshot = new WriterState
    {
        HeadSegment        = _pipe._chainHead,
        TailSegment        = _pipe._writingHead,
        TailWritten        = _pipe._writingHeadBytesBuffered,
        TotalWritten       = _pipe._totalWritten,
        IsCompleted        = true,
        CompletionException = exception,
    };
    _pipe._writerTb.ProducerSlot() = snapshot;
    _pipe._writerTb.Publish();
    _pipe._lastPublishedWriterState = snapshot;

    _pipe.SignalReadAwaiterIfPending();
}
```

- [ ] **Step 2: Implement `Reader.Complete`**

```csharp
public override void Complete(Exception? exception = null)
{
    if (_pipe._disposed) throw new ObjectDisposedException(nameof(SpscPipe));
    if (_pipe._readerCompleted) return;
    _pipe._readerCompleted = true;

    var snapshot = new ReaderState
    {
        HeadSegment   = null,           // S4: terminal publish
        TotalConsumed = _pipe._totalConsumed,
        TotalExamined = _pipe._totalExamined,
        IsCompleted   = true,
        CompletionException = exception,
    };
    _pipe._readerTb.ProducerSlot() = snapshot;
    _pipe._readerTb.Publish();
    _pipe._lastPublishedReaderState = snapshot;

    _pipe.SignalFlushAwaiterIfPending();
}
```

- [ ] **Step 3: Write lifecycle tests**

Create `tests/SpscPipe.Tests/SpscPipeLifecycleTests.cs`:

```csharp
using SpscPipelines;
using Xunit;

namespace SpscPipe.Tests;

public class SpscPipeLifecycleTests
{
    [Fact]
    public async Task WriterCompleteNull_ReaderSeesIsCompletedAfterDrain()
    {
        using var pipe = new SpscPipe();
        var mem = pipe.Writer.GetMemory(5); mem.Span.Fill(0xAA); pipe.Writer.Advance(5);
        await pipe.Writer.FlushAsync();
        pipe.Writer.Complete();

        var r1 = await pipe.Reader.ReadAsync();
        Assert.True(r1.IsCompleted);
        Assert.Equal(5, r1.Buffer.Length);

        pipe.Reader.AdvanceTo(r1.Buffer.End);

        var r2 = await pipe.Reader.ReadAsync();
        Assert.True(r2.IsCompleted);
        Assert.True(r2.Buffer.IsEmpty);
    }

    [Fact]
    public async Task WriterCompleteEx_EveryReadAsyncThrows()
    {
        using var pipe = new SpscPipe();
        var ex = new InvalidOperationException("writer error");
        pipe.Writer.Complete(ex);

        var t1 = await Assert.ThrowsAsync<InvalidOperationException>(async () => await pipe.Reader.ReadAsync());
        Assert.Same(ex, t1);

        var t2 = await Assert.ThrowsAsync<InvalidOperationException>(async () => await pipe.Reader.ReadAsync());
        Assert.Same(ex, t2);
    }

    [Fact]
    public async Task ReaderComplete_WriterFlushReturnsIsCompleted()
    {
        using var pipe = new SpscPipe();
        pipe.Reader.Complete();

        var r = await pipe.Writer.FlushAsync();
        Assert.True(r.IsCompleted);
    }

    [Fact]
    public async Task ReaderCompleteEx_WriterFlushThrows()
    {
        using var pipe = new SpscPipe();
        var ex = new InvalidOperationException("reader error");
        pipe.Reader.Complete(ex);

        var t = await Assert.ThrowsAsync<InvalidOperationException>(async () => await pipe.Writer.FlushAsync());
        Assert.Same(ex, t);
    }

    [Fact]
    public void DoubleComplete_NoOp()
    {
        using var pipe = new SpscPipe();
        pipe.Writer.Complete();
        pipe.Writer.Complete(new Exception("ignored"));         // no-op coalesce
        pipe.Reader.Complete();
        pipe.Reader.Complete(new Exception("ignored"));
    }

    [Fact]
    public async Task ReaderComplete_AllChainSegmentsRecycledOnNextFlush()
    {
        using var pipe = new SpscPipe(new SpscPipeOptions(minimumSegmentSize: 64));
        // Fill two segments.
        for (int i = 0; i < 2; i++)
        {
            pipe.Writer.GetMemory(60); pipe.Writer.Advance(60);
            await pipe.Writer.FlushAsync();
        }
        pipe.Reader.Complete();

        // Writer's next FlushAsync should recycle the entire chain (HeadSegment=null + IsCompleted=true).
        await pipe.Writer.FlushAsync();

        // _chainHead should equal _writingHead (only the active tail remains).
        Assert.Same(pipe._writingHead, pipe._chainHead);
    }
}
```

- [ ] **Step 4: Run; expect PASS**

```bash
dotnet test tests/SpscPipe.Tests/ --filter "FullyQualifiedName~SpscPipeLifecycleTests"
```

- [ ] **Step 5: Commit**

```bash
git add src/SpscPipelines/ tests/SpscPipe.Tests/
git commit -m "SpscPipe: Complete (both sides) + R7 throw + chain sweep on Reader.Complete"
```

---

## Task 10: CancelPending* + Dispose

**Goal:** Implement cross-thread `CancelPendingRead`/`CancelPendingFlush` per Spec §5 (with R2-5 stash construction) and `Dispose` per Spec §3 (with R4-1 CTR cleanup).

**Files:**
- Modify: `src/SpscPipelines/SpscPipe.Reader.cs`
- Modify: `src/SpscPipelines/SpscPipe.Writer.cs`
- Modify: `src/SpscPipelines/SpscPipe.cs`
- Create: `tests/SpscPipe.Tests/SpscPipeCancellationTests.cs`
- Create: `tests/SpscPipe.Tests/SpscPipeDisposeTests.cs`

- [ ] **Step 1: Implement `CancelPendingRead`**

In `SpscPipe.Reader.cs`:

```csharp
public override void CancelPendingRead()
{
    int oldV = Interlocked.Or(ref _pipe._readAwaiter._state, SpscAwaiter<ReadResult>.CancelFlag);
    if ((oldV & SpscAwaiter<ReadResult>.StateMask) == SpscAwaiter<ReadResult>.Pending
        && Interlocked.CompareExchange(
               ref _pipe._readAwaiter._state,
               SpscAwaiter<ReadResult>.Inactive,
               SpscAwaiter<ReadResult>.Pending | SpscAwaiter<ReadResult>.CancelFlag)
           == (SpscAwaiter<ReadResult>.Pending | SpscAwaiter<ReadResult>.CancelFlag))
    {
        _pipe._readAwaiter._ctr.Dispose();

        var head = _pipe._readAwaiter._stashHead;
        var tail = _pipe._readAwaiter._stashTail;
        var buffer = head == null
            ? ReadOnlySequence<byte>.Empty
            : new ReadOnlySequence<byte>(head, _pipe._readAwaiter._stashHeadIdx, tail!, _pipe._readAwaiter._stashTailIdx);

        _pipe._readAwaiter._core.SetResult(new ReadResult(buffer, isCanceled: true, isCompleted: false));
    }
}
```

- [ ] **Step 2: Implement `CancelPendingFlush`**

In `SpscPipe.Writer.cs`:

```csharp
public override void CancelPendingFlush()
{
    int oldV = Interlocked.Or(ref _pipe._flushAwaiter._state, SpscAwaiter<FlushResult>.CancelFlag);
    if ((oldV & SpscAwaiter<FlushResult>.StateMask) == SpscAwaiter<FlushResult>.Pending
        && Interlocked.CompareExchange(
               ref _pipe._flushAwaiter._state,
               SpscAwaiter<FlushResult>.Inactive,
               SpscAwaiter<FlushResult>.Pending | SpscAwaiter<FlushResult>.CancelFlag)
           == (SpscAwaiter<FlushResult>.Pending | SpscAwaiter<FlushResult>.CancelFlag))
    {
        _pipe._flushAwaiter._ctr.Dispose();
        _pipe._flushAwaiter._core.SetResult(new FlushResult(isCanceled: true, isCompleted: false));
    }
}
```

- [ ] **Step 3: Implement full `Dispose` per R4-1**

Replace the placeholder Dispose in `SpscPipe.cs`:

```csharp
public void Dispose()
{
    if (_disposed) return;
    _disposed = true;

    // R4-1: dispose leftover CTRs (idempotent on default).
    _readAwaiter._ctr.Dispose();
    _flushAwaiter._ctr.Dispose();

    // Walk the chain.
    var seg = _chainHead;
    while (seg != null)
    {
        var next = seg.Next;
        seg.DisposeOwned();
        seg = next;
    }
    _chainHead = null;
    _writingHead = null;

    // Walk the freelist.
    var fl = _freelistHead;
    while (fl != null)
    {
        var next = fl.Next;
        fl.DisposeOwned();
        fl = next;
    }
    _freelistHead = null;
    _freelistCount = 0;
}
```

- [ ] **Step 4: Write `SpscPipeCancellationTests.cs`**

```csharp
using SpscPipelines;
using Xunit;

namespace SpscPipe.Tests;

public class SpscPipeCancellationTests
{
    [Fact]
    public async Task CancelPendingRead_WhileNotParked_NextReadReturnsCanceled()
    {
        using var pipe = new SpscPipe();
        pipe.Reader.CancelPendingRead();

        var result = await pipe.Reader.ReadAsync();
        Assert.True(result.IsCanceled);
    }

    [Fact]
    public async Task CancelPendingRead_WhileParked_DeliversCanceled()
    {
        using var pipe = new SpscPipe();
        var readTask = pipe.Reader.ReadAsync().AsTask();

        // No Task.Delay needed: ParkReadAwaiter CASes to Pending synchronously before returning,
        // so by the time AsTask() returns the awaiter is parked. If readTask is already complete,
        // ReadAsync took the sync path (un-parked via re-check), which would mean the test's
        // precondition (no data, no completion) is violated.
        Assert.False(readTask.IsCompleted);

        pipe.Reader.CancelPendingRead();
        var result = await readTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(result.IsCanceled);
    }

    [Fact]
    public async Task CancelPendingRead_FromThirdThread_DeliversStashBuffer()
    {
        using var pipe = new SpscPipe();
        var mem = pipe.Writer.GetMemory(3);
        mem.Span[0] = 1; mem.Span[1] = 2; mem.Span[2] = 3;
        pipe.Writer.Advance(3);
        await pipe.Writer.FlushAsync();

        // Read once + AdvanceTo with examined=buffer.End so HasReadableProgress is false on next read.
        // (If we used single-arg AdvanceTo, examined would stay at consumed=1, and the next ReadAsync
        //  would return sync with the remaining 2 bytes — no park.)
        var r1 = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(r1.Buffer.GetPosition(1), r1.Buffer.End);

        // Park reader; cancel from a third thread.
        var readTask = pipe.Reader.ReadAsync().AsTask();
        Assert.False(readTask.IsCompleted);
        await Task.Run(() => pipe.Reader.CancelPendingRead());

        var result = await readTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(result.IsCanceled);
        // Buffer should be the bytes after the AdvanceTo's consumed position (at park time).
        Assert.Equal(2, result.Buffer.Length);
        Assert.Equal(new byte[] { 2, 3 }, result.Buffer.ToArray());
    }

    [Fact]
    public async Task ReadAsync_WithCanceledToken_Throws()
    {
        using var pipe = new SpscPipe();
        var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAsync<OperationCanceledException>(async () => await pipe.Reader.ReadAsync(cts.Token));
    }

    [Fact]
    public async Task ReadAsync_TokenCancelsWhileParked_Throws()
    {
        using var pipe = new SpscPipe();
        var cts = new CancellationTokenSource();
        var readTask = pipe.Reader.ReadAsync(cts.Token).AsTask();
        Assert.False(readTask.IsCompleted);

        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(async () => await readTask.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task CancelPendingFlush_WhileParked_DeliversCanceled()
    {
        using var pipe = new SpscPipe(new SpscPipeOptions(pauseWriterThreshold: 50, resumeWriterThreshold: 25));
        pipe.Writer.GetMemory(100); pipe.Writer.Advance(100);
        var flushTask = pipe.Writer.FlushAsync().AsTask();
        Assert.False(flushTask.IsCompleted);

        await Task.Run(() => pipe.Writer.CancelPendingFlush());

        var result = await flushTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(result.IsCanceled);
    }
}
```

- [ ] **Step 5: Write `SpscPipeDisposeTests.cs`**

```csharp
using SpscPipelines;
using Xunit;

namespace SpscPipe.Tests;

public class SpscPipeDisposeTests
{
    [Fact]
    public void Dispose_NeverUsed_NoThrow()
    {
        var pipe = new SpscPipe();
        pipe.Dispose();
    }

    [Fact]
    public void DoubleDispose_NoThrow()
    {
        var pipe = new SpscPipe();
        pipe.Dispose();
        pipe.Dispose();
    }

    [Fact]
    public async Task Dispose_AfterUse_ReleasesSegments()
    {
        var pipe = new SpscPipe();
        pipe.Writer.GetMemory(100); pipe.Writer.Advance(100);
        await pipe.Writer.FlushAsync();
        var r = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(r.Buffer.End);

        pipe.Writer.Complete();
        pipe.Reader.Complete();
        pipe.Dispose();

        // Subsequent calls throw ObjectDisposedException.
        Assert.Throws<ObjectDisposedException>(() => pipe.Writer.GetMemory(0));
        Assert.Throws<ObjectDisposedException>(() => pipe.Reader.TryRead(out _));
    }
}
```

- [ ] **Step 6: Run all cancellation + dispose tests; expect PASS**

```bash
dotnet test tests/SpscPipe.Tests/ --filter "FullyQualifiedName~SpscPipeCancellationTests|FullyQualifiedName~SpscPipeDisposeTests"
```

- [ ] **Step 7: Commit**

```bash
git add src/SpscPipelines/ tests/SpscPipe.Tests/
git commit -m "SpscPipe: CancelPending* + Dispose with CTR cleanup"
```

---

## Task 11: Stress harness (concurrent randomized workloads)

**Goal:** Build a stress harness that runs producer + consumer threads with PRNG-driven workloads, verifying byte-sequence integrity, no deadlocks, and conservation of bytes.

**Files:**
- Create: `tests/SpscPipe.Stress/SpscPipe.Stress.csproj`
- Create: `tests/SpscPipe.Stress/ByteSequence.cs`
- Create: `tests/SpscPipe.Stress/StressHarness.cs`
- Create: `tests/SpscPipe.Stress/Program.cs`

- [ ] **Step 1: Create stress project**

```bash
mkdir -p tests/SpscPipe.Stress
dotnet new console -n SpscPipe.Stress -o tests/SpscPipe.Stress --framework net10.0
dotnet sln add tests/SpscPipe.Stress/SpscPipe.Stress.csproj
dotnet add tests/SpscPipe.Stress/SpscPipe.Stress.csproj reference src/SpscPipelines/SpscPipelines.csproj
```

- [ ] **Step 2: Write `ByteSequence.cs`**

```csharp
namespace SpscPipe.Stress;

internal static class ByteSequence
{
    // Deterministic, O(1)-per-byte, allocation-free. Producer and consumer compute the
    // same expected byte from the absolute offset alone — no shared RNG state needed.
    // Uses Knuth's multiplicative hash; quality is sufficient for byte-integrity checks.
    public static byte ByteAt(long offset)
    {
        ulong x = unchecked((ulong)offset);
        x = unchecked(x * 2654435761UL);
        x ^= x >> 16;
        return (byte)x;
    }
}
```

(`new Random(seed)` per byte is far too slow for streaming MiB-scale workloads. This hash is one mul + one shift per byte and produces a deterministic-but-distinct-enough sequence for catching off-by-one or torn-write bugs.)

- [ ] **Step 3: Write `StressHarness.cs`**

```csharp
using SpscPipelines;

namespace SpscPipe.Stress;

internal sealed class StressHarness
{
    private readonly SpscPipeOptions _options;
    private readonly TimeSpan _duration;

    public StressHarness(SpscPipeOptions options, TimeSpan duration)
    {
        _options  = options;
        _duration = duration;
    }

    public async Task<StressResult> RunOnce(int seed, long totalBytes, CancellationToken ct)
    {
        using var pipe = new SpscPipe(_options);
        // Two independent RNGs for producer/consumer so timing/yielding decisions don't synchronize.
        var producerRng = new Random(seed);
        var consumerRng = new Random(seed ^ 0x5A5A_5A5A);

        long produced = 0;
        long consumed = 0;
        Exception? producerEx = null, consumerEx = null;

        var producer = Task.Run(async () =>
        {
            try
            {
                while (produced < totalBytes && !ct.IsCancellationRequested)
                {
                    int chunk = producerRng.Next(1, 4097);
                    chunk = (int)Math.Min(chunk, totalBytes - produced);
                    var mem = pipe.Writer.GetMemory(chunk);
                    for (int i = 0; i < chunk; i++)
                        mem.Span[i] = ByteSequence.ByteAt(produced + i);
                    pipe.Writer.Advance(chunk);
                    produced += chunk;

                    if (producerRng.Next(8) == 0) await Task.Yield();
                    var fr = await pipe.Writer.FlushAsync(ct);
                    if (fr.IsCompleted) break;
                }
                pipe.Writer.Complete();
            }
            catch (Exception e) { producerEx = e; }
        }, ct);

        var consumer = Task.Run(async () =>
        {
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    var rr = await pipe.Reader.ReadAsync(ct);
                    long bufferStart = consumed;
                    long offset = bufferStart;
                    foreach (var memory in rr.Buffer)
                    {
                        for (int i = 0; i < memory.Length; i++)
                        {
                            byte expected = ByteSequence.ByteAt(offset + i);
                            if (memory.Span[i] != expected)
                                throw new InvalidOperationException(
                                    $"Byte mismatch at offset {offset + i}: expected 0x{expected:X2}, got 0x{memory.Span[i]:X2}");
                        }
                        offset += memory.Length;
                    }
                    consumed = offset;

                    // Sometimes only AdvanceTo a prefix to exercise the partial-consume path.
                    if (consumerRng.Next(4) == 0 && rr.Buffer.Length > 1)
                    {
                        long takeBytes = consumerRng.Next(1, (int)Math.Min(rr.Buffer.Length, int.MaxValue));
                        consumed = bufferStart + takeBytes;
                        pipe.Reader.AdvanceTo(rr.Buffer.GetPosition(takeBytes));
                    }
                    else
                    {
                        pipe.Reader.AdvanceTo(rr.Buffer.End);
                    }

                    if (rr.IsCompleted && consumed >= produced) break;
                }
                pipe.Reader.Complete();
            }
            catch (Exception e) { consumerEx = e; }
        }, ct);

        try
        {
            await Task.WhenAll(producer, consumer).WaitAsync(_duration);
        }
        catch (TimeoutException)
        {
            return new StressResult(seed, produced, consumed, "timeout — possible deadlock", null, null);
        }

        if (producerEx != null || consumerEx != null)
            return new StressResult(seed, produced, consumed, "exception", producerEx, consumerEx);

        return new StressResult(seed, produced, consumed,
            consumed == produced ? "ok" : "byte-count mismatch",
            null, null);
    }
}

internal record StressResult(int Seed, long Produced, long Consumed, string Status, Exception? ProducerEx, Exception? ConsumerEx);
```

Note the partial-consume path: 25% of reads `AdvanceTo` a random prefix instead of `Buffer.End`, exercising the "examined > consumed" path that's easy to miss with always-drain stress patterns.

- [ ] **Step 4: Write `Program.cs`**

```csharp
using SpscPipelines;
using SpscPipe.Stress;

int seedCount = args.Length > 0 ? int.Parse(args[0]) : 10;
long bytesPerSeed = args.Length > 1 ? long.Parse(args[1]) : 1L << 22;   // 4 MiB

var harness = new StressHarness(SpscPipeOptions.Default, TimeSpan.FromSeconds(30));
int failures = 0;

for (int i = 0; i < seedCount; i++)
{
    int seed = i + 1;
    using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(1));
    var result = await harness.RunOnce(seed, bytesPerSeed, cts.Token);
    Console.WriteLine($"seed={seed,4} status={result.Status,-30} produced={result.Produced,12} consumed={result.Consumed,12}");
    if (result.Status != "ok") { failures++; if (result.ProducerEx != null) Console.WriteLine($"  producer: {result.ProducerEx}"); if (result.ConsumerEx != null) Console.WriteLine($"  consumer: {result.ConsumerEx}"); }
}

Console.WriteLine($"\n{seedCount - failures}/{seedCount} seeds passed.");
return failures == 0 ? 0 : 1;
```

- [ ] **Step 5: Run a smoke stress (10 seeds × 4 MiB)**

```bash
dotnet run --project tests/SpscPipe.Stress -c Release -- 10 4194304
```

Expected: 10/10 seeds pass, output looks like `seed=   1 status=ok ...`. If a seed fails, the harness prints the seed for reproduction.

- [ ] **Step 6: Commit**

```bash
git add tests/SpscPipe.Stress/
git commit -m "Stress harness: PRNG-driven byte-integrity + deadlock detection"
```

---

## Task 12: BCL parity tests

**Goal:** Validate the documented divergences from BCL `Pipe` are exactly the documented set, and that everything else matches BCL exactly.

**Files:**
- Create: `tests/SpscPipe.Tests/BclParityTests.cs`

- [ ] **Step 1: Write `BclParityTests.cs`**

```csharp
using System.IO.Pipelines;
using SpscPipelines;
using Xunit;

namespace SpscPipe.Tests;

public enum PipeKind { Bcl, Spsc }

public class BclParityTests
{
    // Each parametrized test runs against both BCL Pipe and SpscPipe; verify outcomes match.

    private static (PipeReader Reader, PipeWriter Writer, IDisposable Disposer) CreatePipe(PipeKind kind, PipeOptions? bclOpts = null)
    {
        switch (kind)
        {
            case PipeKind.Bcl:
                var bcl = new Pipe(bclOpts ?? PipeOptions.Default);
                return (bcl.Reader, bcl.Writer, NoOpDisposable.Instance);
            case PipeKind.Spsc:
                var spsc = new SpscPipe();    // defaults match BCL defaults
                return (spsc.Reader, spsc.Writer, spsc);
            default:
                throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }

    private sealed class NoOpDisposable : IDisposable
    {
        public static readonly NoOpDisposable Instance = new();
        public void Dispose() { }
    }

    [Theory]
    [InlineData(PipeKind.Bcl)]
    [InlineData(PipeKind.Spsc)]
    public async Task WriterCompleteEx_NextReadAsyncThrows(PipeKind kind)
    {
        var (reader, writer, disp) = CreatePipe(kind);
        using (disp)
        {
            var ex = new InvalidOperationException("test");
            writer.Complete(ex);
            var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await reader.ReadAsync());
            Assert.Same(ex, thrown);
        }
    }

    [Theory]
    [InlineData(PipeKind.Bcl)]
    [InlineData(PipeKind.Spsc)]
    public async Task ReaderCompleteEx_NextFlushAsyncThrows(PipeKind kind)
    {
        var (reader, writer, disp) = CreatePipe(kind);
        using (disp)
        {
            var ex = new InvalidOperationException("test");
            reader.Complete(ex);
            var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.FlushAsync());
            Assert.Same(ex, thrown);
        }
    }

    [Theory]
    [InlineData(PipeKind.Bcl)]
    [InlineData(PipeKind.Spsc)]
    public async Task BackpressureHysteresis_ParkAtPause_ResumeAtBelowResume(PipeKind kind)
    {
        // Both pipes use defaults (Pause=64K, Resume=32K).
        var (reader, writer, disp) = CreatePipe(kind);
        using (disp)
        {
            // Fill above pause threshold.
            var mem = writer.GetMemory(70_000); writer.Advance(70_000);
            var flushTask = writer.FlushAsync().AsTask();
            Assert.False(flushTask.IsCompleted);

            // Drain enough to drop below resume.
            var r = await reader.ReadAsync();
            // Consume so unconsumed = 70_000 - 40_000 = 30_000 < 32_000 (resume threshold).
            reader.AdvanceTo(r.Buffer.GetPosition(40_000));

            var result = await flushTask.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.False(result.IsCanceled);
        }
    }

    [Theory]
    [InlineData(PipeKind.Bcl)]
    [InlineData(PipeKind.Spsc)]
    public async Task EmptyPipe_TryReadReturnsFalse_ReadAsyncParks(PipeKind kind)
    {
        var (reader, writer, disp) = CreatePipe(kind);
        using (disp)
        {
            Assert.False(reader.TryRead(out _));
            var t = reader.ReadAsync().AsTask();
            Assert.False(t.IsCompleted);
            // Cleanup: complete the writer so the reader unparks.
            writer.Complete();
            await t.WaitAsync(TimeSpan.FromSeconds(5));
        }
    }

    [Theory]
    [InlineData(PipeKind.Bcl)]
    [InlineData(PipeKind.Spsc)]
    public async Task RoundTripBytes_PreservesContent(PipeKind kind)
    {
        var (reader, writer, disp) = CreatePipe(kind);
        using (disp)
        {
            var data = Enumerable.Range(0, 1000).Select(i => (byte)i).ToArray();
            var mem = writer.GetMemory(data.Length);
            data.CopyTo(mem);
            writer.Advance(data.Length);
            await writer.FlushAsync();
            writer.Complete();

            var rr = await reader.ReadAsync();
            Assert.Equal(data, rr.Buffer.ToArray());
        }
    }

    // SPSC-only divergence: this test specifically validates that the documented divergence holds.
    [Fact]
    public void DoubleComplete_SpscCoalesces_BclThrows()
    {
        // BCL: throws on second Complete.
        var bcl = new Pipe();
        bcl.Writer.Complete();
        Assert.Throws<InvalidOperationException>(() => bcl.Writer.Complete());

        // SPSC: coalesces.
        using var spsc = new SpscPipe();
        spsc.Writer.Complete();
        spsc.Writer.Complete();   // no throw
    }
}
```

- [ ] **Step 2: Run; expect PASS (with parity-divergence test passing on both pipes)**

```bash
dotnet test tests/SpscPipe.Tests/ --filter "FullyQualifiedName~BclParityTests"
```

Expected: all tests pass. Some tests parametrize over both pipes (verifying parity); the `DoubleComplete` test validates the documented divergence holds.

- [ ] **Step 3: Commit**

```bash
git add tests/SpscPipe.Tests/BclParityTests.cs
git commit -m "BCL parity tests: round-trip + completion + backpressure + documented divergences"
```

---

## Task 13: Benchmark validation (SpscPipe vs BCL)

**Goal:** Add an SPSC adapter to the benchmarks project, run head-to-head against BCL, validate the design's performance goals.

**Files:**
- Create: `tests/SpscPipe.Benchmarks/SpscPipeAdapter.cs`
- Modify: `tests/SpscPipe.Benchmarks/ThroughputBenchmarks.cs`

- [ ] **Step 1: Write `SpscPipeAdapter.cs`**

```csharp
using System.IO.Pipelines;
using SpscPipelines;

namespace SpscPipe.Benchmarks;

internal sealed class SpscPipeAdapter : IPipeAdapter
{
    private readonly SpscPipe _pipe;
    public SpscPipeAdapter(SpscPipeOptions? options = null) => _pipe = new SpscPipe(options ?? SpscPipeOptions.Default);
    public PipeReader Reader => _pipe.Reader;
    public PipeWriter Writer => _pipe.Writer;
    public void Dispose() => _pipe.Dispose();
}
```

- [ ] **Step 2: Add SPSC throughput benchmark**

Edit `ThroughputBenchmarks.cs`, add:

```csharp
[Benchmark]
public async Task SpscPipe_ProduceAndDrain()
{
    using var adapter = new SpscPipeAdapter();
    await ProduceAndDrain(adapter);
}

private static async Task ProduceAndDrain(IPipeAdapter adapter)
{
    var producer = Task.Run(async () =>
    {
        int written = 0;
        var chunk = new byte[ChunkSize];
        while (written < TotalBytes)
        {
            var memory = adapter.Writer.GetMemory(chunk.Length);
            chunk.CopyTo(memory);
            adapter.Writer.Advance(chunk.Length);
            await adapter.Writer.FlushAsync();
            written += chunk.Length;
        }
        adapter.Writer.Complete();
    });

    var consumer = Task.Run(async () =>
    {
        while (true)
        {
            var result = await adapter.Reader.ReadAsync();
            adapter.Reader.AdvanceTo(result.Buffer.End);
            if (result.IsCompleted) break;
        }
        adapter.Reader.Complete();
    });

    await Task.WhenAll(producer, consumer);
}
```

(Implementer: refactor `BclPipe_ProduceAndDrain` to use `ProduceAndDrain(new BclPipeAdapter())` for fair comparison.)

- [ ] **Step 3: Run head-to-head benchmarks**

```bash
dotnet run --project tests/SpscPipe.Benchmarks -c Release -- --filter "*ProduceAndDrain*"
```

Expected: BDN reports both BCL and SPSC results. SPSC should be materially faster (target: at least 2× under SPSC contention; pause to consider what "materially" means for this hardware if it's less).

- [ ] **Step 4: Document the benchmark results**

Create `tests/SpscPipe.Benchmarks/RESULTS.md`:

```markdown
# Benchmark Results

**Date:** YYYY-MM-DD
**Hardware:** [CPU model, cores, RAM, OS]
**Build:** Release, .NET 10
**Commit:** [git rev-parse HEAD]

## Throughput

[BDN output table — paste actual results]

## Verdict

- SPSC throughput (bytes/sec): [number]
- BCL throughput (bytes/sec): [number]
- Speedup: [factor]
- Allocations per op: SPSC [number] vs BCL [number]

[Discussion: did we meet the spec's "materially higher than BCL" target?]
```

- [ ] **Step 5: Commit**

```bash
git add tests/SpscPipe.Benchmarks/
git commit -m "Benchmarks: SpscPipe adapter + head-to-head + RESULTS.md"
```

---

## Spec coverage check (post-plan self-review)

Per the writing-plans skill, reviewing the plan against the spec:

| Spec section | Implementing task(s) |
|---|---|
| §1 Architecture, components, threading contract, TripleBuffer contract | Tasks 4 (scaffolding), 1 (TripleBuffer existing) |
| §1 Local cursors | Task 4 |
| §2 WriterState, ReaderState, head==tail discipline | Tasks 4, 5, 6 |
| §3 BufferSegment, freelist, allocation, recycle, bootstrap, Dispose | Tasks 2, 4, 5, 7, 10 |
| §3 Memory<T> torn-read sentinel | Task 2 step 9 |
| §3 R4-7 pipe-identity | Task 2 (field) + Task 7 (validation) |
| §4 GetMemory/Span/Advance | Task 4 |
| §4 FlushAsync | Tasks 5 (sync), 8 (park) |
| §4 ReadAsync/TryRead | Tasks 6 (sync), 8 (park) |
| §4 AdvanceTo (both overloads, throw-first, R8 validation) | Task 7 |
| §4 BuildReadResult / BuildFlushResult | Tasks 5, 6 |
| §5 SpscAwaiter<T> + Pattern 2 stash | Tasks 3, 8 |
| §5 ParkReadAwaiter / SignalReadAwaiterIfPending / OnReadAwaiterTokenCancel | Task 8 |
| §5 ParkFlushAwaiter / signaler split (gated + unconditional) | Tasks 7 (gated), 8 (parking), 9 (unconditional via Reader.Complete) |
| §5 CancelPending* | Task 10 |
| §6 SpscPipeOptions | Task 4 |
| §6 Writer.Complete / Reader.Complete | Task 9 |
| §6 R7 throw integration | Tasks 5, 6, 8, 9 (entry guards + signaler exception path) |
| §6 R4-1 Dispose CTR cleanup | Task 10 |
| §6 Edge cases (double Complete, empty AdvanceTo, etc.) | Tasks 7, 9 |
| §7 BCL parity tests | Task 12 |
| §7 Stress harness | Task 11 |
| §7 Performance goals | Task 13 |
| Invariant I1 (monotonicity) | Tasks 5, 9 (snapshot construction) |
| Invariant I2 (sole mutator) | Tasks 4–10 (architectural — enforced by code structure) |
| Invariant I3 (reader tail-bound) | Task 6 (BuildReadResult uses _readTailIdx) |
| Invariants I4–I5 (head==tail discipline + freeze ordering) | Task 4 (Freeze in BufferSegment + GetMemory transition) |
| Invariant I6 (recycle predicate) | Task 5 (RecycleDrainedSegments) |
| Invariant I7 (read head ≤ tail) | Task 7 (AdvanceTo validation) |
| Invariants I8–I9 (TripleBuffer contract) | Existing TripleBuffer (Task 0 verifies) |
| Invariant I10 (bootstrap) | Task 6 (IntegrateAcquiredWriterState) |
| Invariants I11–I12 (awaiter state, cancel coalesce) | Tasks 3, 8, 10 |
| Invariants I13–I14 (post-Complete no further publish) | Task 9 |
| Invariant I15 (Dispose precondition) | Task 10 |
| Rules R1, R2, R3 (publish-before-signal, Interlocked-only, transitions) | Architectural — Tasks 5, 7, 8, 10 |
| Rule R4 (re-checks after CAS) | Task 8 |
| Rule R5 (CTR re-check + R5b cleanup at park entry + R4-1 Dispose cleanup) | Tasks 8, 10 |
| Rule R6 (Complete is terminal) | Task 9 |
| Rule R7 (Option A throw) | Tasks 5, 6, 8, 9 |
| Rule R8 (AdvanceTo validation) | Task 7 |
| Rule R9 (throw-first precedence) | Tasks 5, 6, 8 |

**Identified gaps:** none. Every spec section has at least one implementing task.

**Type/method consistency check:** types and method signatures used in pseudocode (e.g., `BuildReadResult`, `IntegrateAcquiredWriterState`, `RecycleDrainedSegments`, `SignalFlushIfBackpressureRelieved`, `SignalFlushAwaiterIfPending`, `DeliverFlushResult`, `OnReadAwaiterTokenCancel`, `OnFlushAwaiterTokenCancel`, `RentSegment`, `PushFreelist`, `PopFreelist`) are introduced consistently across tasks.
