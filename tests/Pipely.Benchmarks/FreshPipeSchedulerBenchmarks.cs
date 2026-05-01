using System.IO.Pipelines;
using BenchmarkDotNet.Attributes;

namespace PipelyBenchmarks;

// Fresh-Pipe companion to SchedulerBenchmarks. Constructs a new Pipe per
// BDN iteration and calls Complete() on both sides at the end. Reported
// allocations therefore include per-Pipe construction; the delta versus
// the headline SchedulerBenchmarks row is the per-Pipe construction cost
// for that variant.
[MemoryDiagnoser]
public class FreshPipeSchedulerBenchmarks
{
    private const int TotalBytes = 1 << 20;        // 1 MiB per iteration
    private const int ChunkSize  = 4096;

    [Benchmark(Baseline = true)]
    public async Task BCL_ThreadPool()
    {
        var pipe = new Pipe(new PipeOptions(
            readerScheduler: PipeScheduler.ThreadPool,
            writerScheduler: PipeScheduler.ThreadPool));
        await ProduceAndDrain(pipe.Reader, pipe.Writer);
    }

    [Benchmark]
    public async Task BCL_Inline()
    {
        var pipe = new Pipe(new PipeOptions(
            readerScheduler: PipeScheduler.Inline,
            writerScheduler: PipeScheduler.Inline));
        await ProduceAndDrain(pipe.Reader, pipe.Writer);
    }

    [Benchmark]
    public async Task Pipely_ThreadPool()
    {
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions(
            readerScheduler: PipeScheduler.ThreadPool,
            writerScheduler: PipeScheduler.ThreadPool));
        await ProduceAndDrain(pipe.Reader, pipe.Writer);
    }

    [Benchmark]
    public async Task Pipely_Inline()
    {
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions(
            readerScheduler: PipeScheduler.Inline,
            writerScheduler: PipeScheduler.Inline));
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
