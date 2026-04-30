using System.IO.Pipelines;
using BenchmarkDotNet.Attributes;

namespace PipelyBenchmarks;

[MemoryDiagnoser]
public class SchedulerBenchmarks
{
    private const int TotalBytes = 1 << 20;        // 1 MiB per iteration
    private const int ChunkSize  = 4096;

    // Constructed once per benchmark run, reused across all iterations. This
    // matches the apples-to-apples comparison shape: BCL Pipe and Pipely with
    // PipeScheduler.ThreadPool both have zero per-iteration "scheduler" startup
    // cost (BCL Pipe uses TP directly; Pipely-ThreadPool uses the singleton
    // PipeScheduler.ThreadPool). The FastScheduler equivalent must also
    // amortize its thread-startup cost across iterations rather than pay it
    // per measurement. Per-iteration cost is now solely pipe ctor +
    // produce-and-drain on both sides.
    private Pipely.FastScheduler? _scheduler;

    [GlobalSetup(Target = nameof(Pipely_FastScheduler_ProduceAndDrain))]
    public void SetupFastScheduler() => _scheduler = new Pipely.FastScheduler();

    [GlobalCleanup(Target = nameof(Pipely_FastScheduler_ProduceAndDrain))]
    public void CleanupFastScheduler() => _scheduler?.Dispose();

    // BCL System.IO.Pipelines.Pipe — TP-driven continuations, default options
    // (64K pause / 32K resume — same thresholds as PipeOptions.Default).
    [Benchmark(Baseline = true)]
    public async Task BclPipe_ProduceAndDrain()
    {
        var pipe = new Pipe();
        await ProduceAndDrain(pipe.Reader, pipe.Writer);
    }

    // Pipely with PipeScheduler.ThreadPool (the default, set explicitly here
    // for clarity; null and PipeScheduler.ThreadPool are equivalent).
    [Benchmark]
    public async Task Pipely_ThreadPool_ProduceAndDrain()
    {
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions
        {
            ReaderScheduler = PipeScheduler.ThreadPool,
            WriterScheduler = PipeScheduler.ThreadPool,
        });
        await ProduceAndDrain(pipe.Reader, pipe.Writer);
    }

    // Pipely with PipeScheduler.Inline — continuations run synchronously on the
    // signaling thread (the producer for the read awaiter, the reader for the
    // flush awaiter). No thread hop on the signal path.
    [Benchmark]
    public async Task Pipely_Inline_ProduceAndDrain()
    {
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions
        {
            ReaderScheduler = PipeScheduler.Inline,
            WriterScheduler = PipeScheduler.Inline,
        });
        await ProduceAndDrain(pipe.Reader, pipe.Writer);
    }

    // Pipely with FastScheduler (constructed once in [GlobalSetup], reused
    // across all iterations of this benchmark).
    [Benchmark]
    public async Task Pipely_FastScheduler_ProduceAndDrain()
    {
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions
        {
            ReaderScheduler = _scheduler,
            WriterScheduler = _scheduler,
        });
        await ProduceAndDrain(pipe.Reader, pipe.Writer);
    }

    private static async Task ProduceAndDrain(PipeReader reader, PipeWriter writer)
    {
        var producer = Task.Run(async () =>
        {
            int written = 0;
            var chunk = new byte[ChunkSize];
            while (written < TotalBytes)
            {
                var memory = writer.GetMemory(chunk.Length);
                chunk.CopyTo(memory);
                writer.Advance(chunk.Length);
                await writer.FlushAsync();
                written += chunk.Length;
            }
            writer.Complete();
        });

        var consumer = Task.Run(async () =>
        {
            while (true)
            {
                var result = await reader.ReadAsync();
                reader.AdvanceTo(result.Buffer.End);
                if (result.IsCompleted) break;
            }
            reader.Complete();
        });

        await Task.WhenAll(producer, consumer);
    }
}
