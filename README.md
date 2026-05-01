# Pipely

Same usage and behavior as `System.IO.Pipelines.Pipe` — but lock-free, and with a `Splice` API for zero-copy buffer ownership transfer.

> Status: fully tested and benchmarked. Not yet published to NuGet.

## Benchmarks

Headline numbers on a single-producer / single-consumer 1 MiB transfer through 4 KiB chunks (AMD Ryzen 7 8700F, .NET 10, Server GC). The `Pipe` is constructed once and reused across iterations — the production shape for long-lived pipes. All four rows are from the same BenchmarkDotNet process invocation, so they are directly comparable.

| Method              | Mean (μs) | Ratio | Allocated |
|---------------------|----------:|------:|----------:|
| `BCL_ThreadPool`    |    103.64 |  1.00 |     816 B |
| `BCL_Inline`        |     48.47 |  0.47 |     743 B |
| `Pipely_ThreadPool` |     71.24 |  0.69 |     889 B |
| `Pipely_Inline`     |     41.70 |  0.40 |     750 B |

At the same scheduler, Pipely is **~1.45× faster than BCL** (`Pipely_ThreadPool` vs `BCL_ThreadPool`); with continuations inlined the two implementations are within 5%, so the gap is in the awaiter / signaling path rather than in the rest of the pipe.

Allocations above are per-1 MiB-transfer with the `Pipe` reused across iterations, so they exclude one-shot per-`Pipe` construction cost. Pipely matches BCL within ~10% at the matched-scheduler rows; per-`Pipe` construction itself is ~1.7 KB heavier than BCL because Pipely uses two `TripleBuffer<T>` instances and two `PipelyAwaiter<T>` instances where BCL embeds awaitable state directly in `Pipe`. The companion `FreshPipeSchedulerBenchmarks` measures fresh-`Pipe` per iteration; the per-row delta isolates that construction cost. See [`RESULTS.md`](tests/Pipely.Benchmarks/RESULTS.md) for the full breakdown.

For latency under sustained throughput (256-byte messages, 100K samples, exact percentiles by sort), Pipely's P50 is ~1.2–1.4× lower and P90 is ~1.4–1.7× lower than BCL across all measured trials. Tail behavior (P99.9, Max) is dominated by GC and OS scheduling and is not consistently better for either pipe.

Full methodology, caveats, and per-trial percentiles live in [`tests/Pipely.Benchmarks/RESULTS.md`](tests/Pipely.Benchmarks/RESULTS.md).

## Getting started

Add a project reference to `src/Pipely/Pipely.csproj`. The public surface mirrors `System.IO.Pipelines`, so existing code reads the same:

```csharp
using Pipely;

var pipe = new Pipe();

var producer = Task.Run(async () =>
{
    var memory = pipe.Writer.GetMemory(4096);
    payload.CopyTo(memory.Span);
    pipe.Writer.Advance(payload.Length);
    await pipe.Writer.FlushAsync();
    pipe.Writer.Complete();
});

var consumer = Task.Run(async () =>
{
    while (true)
    {
        var result = await pipe.Reader.ReadAsync();
        Process(result.Buffer);
        pipe.Reader.AdvanceTo(result.Buffer.End);
        if (result.IsCompleted) break;
    }
    pipe.Reader.Complete();
});

await Task.WhenAll(producer, consumer);
```

`Pipely.PipeOptions` extends `System.IO.Pipelines.PipeOptions` — it inherits every BCL knob (`Pool`, `MinimumSegmentSize`, `PauseWriterThreshold`, `ResumeWriterThreshold`, `ReaderScheduler`, `WriterScheduler`, `UseSynchronizationContext`) with the same defaults, and adds `MaxFreelistSegments` for Pipely's per-pipe segment freelist.

## Design

Pipely is lock-free by construction. It contains no `lock` statements, no monitors, no semaphores, no mutexes — only single-producer / single-consumer patterns synchronized with atomics. The BCL's `Pipe` has the same SPSC contract but uses a `lock` on its hot path; Pipely keeps the contract and removes the lock.

The entire concurrency surface is:

- **Two `TripleBuffer<T>`s** — a lock-free primitive that publishes a value from one thread to another through three padded slots and atomic indices. One carries the writer's published state to the reader; the other carries the reader's published state to the writer. Each side reads the other's most-recently-published state without blocking.
- **Two awaiter state machines** (`PipelyAwaiter<ReadResult>` for the parked reader; `PipelyAwaiter<FlushResult>` for the back-pressured writer), each synchronized through `Interlocked` operations on a single `int` state field.

## Splice — zero-copy ownership transfer

When you already hold a rented or pooled buffer (e.g. a payload received from a socket, a frame produced by another component), `Splice` hands it to the pipe without copying. The name is borrowed from Linux's [`splice(2)`](https://man7.org/linux/man-pages/man2/splice.2.html) — conceptually the same operation: transfer ownership of an existing buffer rather than copy bytes into a new one.

```csharp
// `frame` was rented from a pool by upstream code; we own it.
IMemoryOwner<byte> frame = ReceiveFrame();

pipe.Writer.Splice(frame);              // ownership transfers to the pipe
await pipe.Writer.FlushAsync();
// Do not touch or dispose `frame` after this point — the pipe owns it.
```

There is also a `Splice(IMemoryOwner<byte>, int start, int length)` overload for handing over a slice of a larger buffer.

**Ownership rule.** Ownership transfers to the pipe **iff `Splice` returns normally**. If `Splice` throws (argument validation, pipe disposed, writer completed), the caller still owns the buffer and is responsible for disposing it. The pipe disposes spliced buffers when the reader advances past them, when the pipe itself is disposed, or when the writer is completed.

## Schedulers

`PipeOptions.ReaderScheduler` and `PipeOptions.WriterScheduler` accept any `System.IO.Pipelines.PipeScheduler` and route parked `ReadAsync` / `FlushAsync` continuations the same way the BCL pipe does. `PipeScheduler.ThreadPool` is the default; `PipeScheduler.Inline` runs continuations on the signaling thread.

## Building, testing, benchmarks

Requires the .NET 10 SDK.

```bash
dotnet build -c Release
dotnet test
```

Benchmarks (BenchmarkDotNet, run from the repo root):

```bash
# Throughput: BCL vs Pipely, single-producer / single-consumer 1 MiB transfer
dotnet run -c Release --project tests/Pipely.Benchmarks -- --filter 'PipelyBenchmarks.ThroughputBenchmarks.*'

# Four-variant scheduler matrix (BCL × Pipely × {ThreadPool, Inline})
dotnet run -c Release --project tests/Pipely.Benchmarks -- --filter 'PipelyBenchmarks.SchedulerBenchmarks.*'

# Fresh-Pipe companions (per-Pipe construction cost = FreshPipe per-iter alloc - headline per-iter alloc)
dotnet run -c Release --project tests/Pipely.Benchmarks -- --filter 'PipelyBenchmarks.FreshPipe*'

# Latency harness (sort-all-samples, exact percentiles)
dotnet run -c Release --project tests/Pipely.Benchmarks -- \
    latency --count 100000 --size 256 --trials 3 --warmup 1
```

## License

MIT — see [LICENSE](LICENSE).
