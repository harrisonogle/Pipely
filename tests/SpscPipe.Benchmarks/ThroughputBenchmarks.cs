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
