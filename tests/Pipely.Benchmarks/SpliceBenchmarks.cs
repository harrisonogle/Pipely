using System.Buffers;
using System.IO.Pipelines;
using BenchmarkDotNet.Attributes;

namespace PipelyBenchmarks;

// Benchmarks PipeWriter.Splice against GetSpan+Advance under the realistic scenario
// Splice was designed for: an upstream component (network RX, parser, reorder buffer, etc.)
// hands the producer an IMemoryOwner<byte> with data already written into it. The producer
// must publish that data into a pipe. Per iteration, every variant:
//
//   1. Rents a buffer from MemoryPool<byte>.Shared.
//   2. Writes data to it (simulates the upstream fill we cannot avoid).
//   3. Hands it off to the pipe.
//        BCL Pipe / Pipely (GetSpan): GetSpan + CopyTo + Advance + Dispose source.
//        Pipely (Splice): Splice(source) — ownership transferred; no CopyTo, no Dispose.
//
// Consumer drains identically across variants. Backpressure disabled. TotalBytes is fixed
// at 1 MiB per iteration so wall-clock is comparable across configs.
[MemoryDiagnoser]
public class SpliceBenchmarks
{
    private const int TotalBytes = 1 << 20;     // 1 MiB per iteration

    [Params(256, 1024, 4096, 16384)]
    public int BufferSize { get; set; }

    [Params(1, 16)]
    public int BuffersBeforeFlush { get; set; }

    private static readonly PipeOptions BclOptions =
        new(pauseWriterThreshold: long.MaxValue, resumeWriterThreshold: long.MaxValue / 2);

    private static readonly Pipely.PipeOptions PipelyOptions =
        new(pauseWriterThreshold: 0);   // 0 = unbounded per PipeOptions contract

    [Benchmark(Baseline = true)]
    public Task BclPipe_GetSpan()
    {
        var pipe = new Pipe(BclOptions);
        return RunGetSpan(pipe.Writer, pipe.Reader, completePipe: () => { });
    }

    [Benchmark]
    public Task Pipely_GetSpan()
    {
        var pipe = new Pipely.Pipe(PipelyOptions);
        return RunGetSpan(pipe.Writer, pipe.Reader, completePipe: () =>
        {
            pipe.Writer.Complete();
            pipe.Reader.Complete();
        });
    }

    [Benchmark]
    public Task Pipely_Splice()
    {
        var pipe = new Pipely.Pipe(PipelyOptions);
        return RunSplice(pipe.Writer, pipe.Reader, completePipe: () =>
        {
            pipe.Writer.Complete();
            pipe.Reader.Complete();
        });
    }

    private async Task RunGetSpan(PipeWriter writer, PipeReader reader, Action completePipe)
    {
        int bufferSize = BufferSize;
        int flushEvery = BuffersBeforeFlush;
        int totalBuffers = TotalBytes / bufferSize;

        var producer = Task.Run(async () =>
        {
            for (int i = 0; i < totalBuffers; i++)
            {
                using var source = MemoryPool<byte>.Shared.Rent(bufferSize);
                source.Memory.Span.Slice(0, bufferSize).Fill(0x42);   // upstream fill

                var span = writer.GetSpan(bufferSize);
                source.Memory.Span.Slice(0, bufferSize).CopyTo(span); // copy into pipe-rented memory
                writer.Advance(bufferSize);

                if ((i + 1) % flushEvery == 0)
                    await writer.FlushAsync();
            }
            await writer.FlushAsync();
            await writer.CompleteAsync();
        });

        await DrainAndComplete(reader, producer, completePipe);
    }

    private async Task RunSplice(Pipely.PipeWriter writer, PipeReader reader, Action completePipe)
    {
        int bufferSize = BufferSize;
        int flushEvery = BuffersBeforeFlush;
        int totalBuffers = TotalBytes / bufferSize;

        var producer = Task.Run(async () =>
        {
            for (int i = 0; i < totalBuffers; i++)
            {
                var source = MemoryPool<byte>.Shared.Rent(bufferSize);
                source.Memory.Span.Slice(0, bufferSize).Fill(0x42);   // upstream fill

                writer.Splice(source, 0, bufferSize);                 // ownership transfer

                if ((i + 1) % flushEvery == 0)
                    await writer.FlushAsync();
            }
            await writer.FlushAsync();
            await writer.CompleteAsync();
        });

        await DrainAndComplete(reader, producer, completePipe);
    }

    private static async Task DrainAndComplete(PipeReader reader, Task producer, Action completePipe)
    {
        var consumer = Task.Run(async () =>
        {
            while (true)
            {
                var rr = await reader.ReadAsync();
                reader.AdvanceTo(rr.Buffer.End);
                if (rr.IsCompleted) break;
            }
            await reader.CompleteAsync();
        });

        await Task.WhenAll(producer, consumer);
        completePipe();
    }
}
