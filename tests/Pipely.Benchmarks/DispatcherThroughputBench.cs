using System.IO.Pipelines;
using BenchmarkDotNet.Attributes;

namespace PipelyBenchmarks;

[MemoryDiagnoser]
public class DispatcherThroughputBench
{
    private const int TotalBytes = 1 << 20;        // 1 MiB per iteration
    private const int ChunkSize  = 4096;

    // Constructed once per benchmark run, reused across all iterations. This
    // matches the apples-to-apples comparison shape: BCL Pipe and Pipely's
    // default TP dispatcher both have zero per-iteration "dispatcher" startup
    // cost (BCL Pipe uses TP directly; Pipely-TP uses the singleton
    // ThreadPoolContinuationDispatcher.Instance). The FastScheduler equivalent
    // must also amortize its thread-startup cost across iterations rather
    // than pay it per measurement. Per-iteration cost is now solely
    // pipe ctor + produce-and-drain on both sides.
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

    // Pipely with the default ThreadPoolContinuationDispatcher (no override).
    [Benchmark]
    public async Task Pipely_TpDefault_ProduceAndDrain()
    {
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions
        {
            ContinuationDispatcher = null,
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
            ContinuationDispatcher = _scheduler,
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
