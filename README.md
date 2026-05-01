# Pipely

Same usage and behavior as `System.IO.Pipelines.Pipe` — but lock-free, and with a `Splice` API for zero-copy buffer ownership transfer.

> Status: fully tested and benchmarked. Not yet published to NuGet.

## Benchmarks

Headline numbers on a single-producer / single-consumer 1 MiB transfer through 4 KiB chunks (AMD Ryzen 7 8700F, .NET 10, Server GC). All five rows are from the same BenchmarkDotNet process invocation, so they are directly comparable.

| Method                 | Mean (μs) | Ratio | Allocated |
|------------------------|----------:|------:|----------:|
| `BCL_ThreadPool`       |    107.02 |  1.00 |   6.88 KB |
| `BCL_Inline`           |     43.43 |  0.41 |   5.77 KB |
| `Pipely_ThreadPool`    |     68.68 |  0.64 |  10.76 KB |
| `Pipely_Inline`        |     41.85 |  0.39 |   8.55 KB |
| `Pipely_FastScheduler` |     49.45 |  0.46 |   9.21 KB |

At the same scheduler, Pipely is **~1.56× faster than BCL** (`Pipely_ThreadPool` vs `BCL_ThreadPool`); with continuations inlined the two implementations are within 4%, so the gap is in the awaiter / signaling path rather than in the rest of the pipe.

For latency under sustained throughput (256-byte messages, 100K samples, exact percentiles by sort), Pipely's P50 is ~1.2–1.4× lower and P90 is ~1.4–1.7× lower than BCL across all measured trials. Tail behavior (P99.9, Max) is dominated by GC and OS scheduling and is not consistently better for either pipe.

Full methodology, caveats, per-trial percentiles, and the FastScheduler workload analysis live in [`tests/Pipely.Benchmarks/RESULTS.md`](tests/Pipely.Benchmarks/RESULTS.md).

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

`Pipely.PipeOptions` mirrors the BCL options (`MinimumSegmentSize`, `PauseWriterThreshold`, `ResumeWriterThreshold`, `ReaderScheduler`, `WriterScheduler`, `UseSynchronizationContext`); the defaults match `PipeOptions.Default`.

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

Pipely also ships `Pipely.FastScheduler`, a busy-spinning `PipeScheduler` for streams where the ThreadPool's spin-then-sleep wake-gap is the dominant latency cost. It runs a dedicated worker thread at ~100% on its core, so it's a niche tool — useful when you need an off-thread continuation context but the TP wake-gap is too expensive, and not appropriate when continuations can run inline. See the FastScheduler section of [`RESULTS.md`](tests/Pipely.Benchmarks/RESULTS.md) for the workload analysis.

## Building, testing, benchmarks

Requires the .NET 10 SDK.

```bash
dotnet build -c Release
dotnet test
```

Benchmarks (BenchmarkDotNet, run from the repo root):

```bash
# Throughput: BCL vs Pipely, single-producer / single-consumer 1 MiB transfer
dotnet run -c Release --project tests/Pipely.Benchmarks -- --filter '*ThroughputBenchmarks*'

# Five-variant scheduler matrix (BCL × Pipely × {ThreadPool, Inline} + Pipely_FastScheduler)
dotnet run -c Release --project tests/Pipely.Benchmarks -- --filter '*SchedulerBenchmarks*'

# Latency harness (sort-all-samples, exact percentiles)
dotnet run -c Release --project tests/Pipely.Benchmarks -- \
    dispatcher-latency --count 100000 --size 256 --trials 3 --warmup 1
```

## License

MIT — see [LICENSE](LICENSE).
