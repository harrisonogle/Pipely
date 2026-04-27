using BenchmarkDotNet.Attributes;
using SpscPipelines;

namespace SpscPipelines.HotHandoff.Benchmarks;

[MemoryDiagnoser]
public class DispatcherThroughputBench
{
    private const int TotalBytes = 1 << 20;        // 1 MiB per iteration
    private const int ChunkSize  = 4096;

    [Benchmark(Baseline = true)]
    public async Task TpDefault_ProduceAndDrain()
    {
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions
        {
            ContinuationDispatcher = null,
        });
        await ProduceAndDrain(pipe);
    }

    [Benchmark]
    public async Task HotHandoff_ProduceAndDrain()
    {
        using var dispatcher = new HotHandoffContinuationDispatcher();
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions
        {
            ContinuationDispatcher = dispatcher,
        });
        await ProduceAndDrain(pipe);
    }

    private static async Task ProduceAndDrain(SpscPipelines.SpscPipe pipe)
    {
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
            pipe.Writer.Complete();
        });

        var consumer = Task.Run(async () =>
        {
            while (true)
            {
                var result = await pipe.Reader.ReadAsync();
                pipe.Reader.AdvanceTo(result.Buffer.End);
                if (result.IsCompleted) break;
            }
            pipe.Reader.Complete();
        });

        await Task.WhenAll(producer, consumer);
    }
}
