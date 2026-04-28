using System.IO.Pipelines;
using BenchmarkDotNet.Attributes;
using SpscPipelines;

namespace SpscPipelines.HotHandoff.Benchmarks;

[MemoryDiagnoser]
public class DispatcherThroughputBench
{
    private const int TotalBytes = 1 << 20;        // 1 MiB per iteration
    private const int ChunkSize  = 4096;

    // Constructed once per benchmark run, reused across all iterations. This
    // matches the apples-to-apples comparison shape: BCL Pipe and SpscPipe's
    // default TP dispatcher both have zero per-iteration "dispatcher" startup
    // cost (the BCL pipe uses TP directly; SpscPipe-TP uses the singleton
    // ThreadPoolContinuationDispatcher.Instance). The HotHandoff equivalent
    // must also amortize its thread-startup cost across iterations rather
    // than pay it per measurement. Per-iteration cost is now solely
    // pipe ctor + produce-and-drain on both sides.
    private HotHandoffContinuationDispatcher? _dispatcher;

    [GlobalSetup(Target = nameof(SpscPipe_HotHandoff_ProduceAndDrain))]
    public void SetupHotHandoff() => _dispatcher = new HotHandoffContinuationDispatcher();

    [GlobalCleanup(Target = nameof(SpscPipe_HotHandoff_ProduceAndDrain))]
    public void CleanupHotHandoff() => _dispatcher?.Dispose();

    // BCL System.IO.Pipelines.Pipe — TP-driven continuations, default options
    // (64K pause / 32K resume — same thresholds as SpscPipeOptions.Default).
    [Benchmark(Baseline = true)]
    public async Task BclPipe_ProduceAndDrain()
    {
        var pipe = new Pipe();
        await ProduceAndDrain(pipe.Reader, pipe.Writer);
    }

    // SpscPipe with the default ThreadPoolContinuationDispatcher (no override).
    [Benchmark]
    public async Task SpscPipe_TpDefault_ProduceAndDrain()
    {
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions
        {
            ContinuationDispatcher = null,
        });
        await ProduceAndDrain(pipe.Reader, pipe.Writer);
    }

    // SpscPipe with the HotHandoffContinuationDispatcher (constructed once
    // in [GlobalSetup], reused across all iterations of this benchmark).
    [Benchmark]
    public async Task SpscPipe_HotHandoff_ProduceAndDrain()
    {
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions
        {
            ContinuationDispatcher = _dispatcher,
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
