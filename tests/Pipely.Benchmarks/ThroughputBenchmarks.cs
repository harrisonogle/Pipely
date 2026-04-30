using BenchmarkDotNet.Attributes;

namespace PipelyBenchmarks;

[MemoryDiagnoser]
public class ThroughputBenchmarks
{
    private const int TotalBytes = 1 << 20;        // 1 MiB per iteration
    private const int ChunkSize = 4096;

    [Benchmark(Baseline = true)]
    public async Task BclPipe_ProduceAndDrain()
    {
        using var adapter = new BclPipeAdapter();
        await ProduceAndDrain(adapter);
    }

    [Benchmark]
    public async Task Pipely_ProduceAndDrain()
    {
        using var adapter = new PipelyPipeAdapter();
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
}
