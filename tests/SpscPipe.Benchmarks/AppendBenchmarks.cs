using System.Buffers;
using System.IO.Pipelines;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using SpscPipelines;

namespace SpscPipe.Benchmarks;

// Compares SpscPipeWriter.Append against GetSpan+Advance on BCL Pipe and SpscPipe under two profiles:
//   "nocopy"  — Variant A: producer generates N bytes via Span.Fill directly into target memory.
//               No source buffer, no copy. Measures pure per-call API overhead.
//   "copy"    — Variant B: producer simulates having received an IMemoryOwner<byte> (rent + fill).
//               GetSpan path CopyTo's into pipe memory and disposes the source; Append transfers
//               ownership. Measures the copy savings Append delivers in the realistic zero-copy
//               scenario it was designed for.
//
// Backpressure is disabled across all variants so the benchmark measures throughput, not park/unpark
// coordination. TotalBytes is fixed at 1 MiB per BDN iteration to keep the wall-clock unit consistent.
[MemoryDiagnoser]
[CategoriesColumn]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class AppendBenchmarks
{
    private const int TotalBytes = 1 << 20;     // 1 MiB per iteration

    [Params(256, 1024, 4096, 16384)]
    public int BufferSize { get; set; }

    [Params(1, 16)]
    public int BuffersBeforeFlush { get; set; }

    private static readonly PipeOptions BclOptions =
        new(pauseWriterThreshold: long.MaxValue, resumeWriterThreshold: long.MaxValue / 2);

    private static readonly SpscPipeOptions SpscOptions =
        new(pauseWriterThreshold: 0);   // 0 = unbounded per SpscPipeOptions contract

    // ---------- Variant A — "nocopy" (pure API overhead) ----------

    [Benchmark(Baseline = true), BenchmarkCategory("nocopy")]
    public Task BclPipe_GetSpan_NoCopy()
    {
        var pipe = new Pipe(BclOptions);
        return RunGetSpanNoCopy(pipe.Writer, pipe.Reader, completePipe: () => { });
    }

    [Benchmark, BenchmarkCategory("nocopy")]
    public Task SpscPipe_GetSpan_NoCopy()
    {
        var pipe = new SpscPipelines.SpscPipe(SpscOptions);
        return RunGetSpanNoCopy(pipe.Writer, pipe.Reader, completePipe: () => pipe.Dispose());
    }

    [Benchmark, BenchmarkCategory("nocopy")]
    public Task SpscPipe_Append_NoCopy()
    {
        var pipe = new SpscPipelines.SpscPipe(SpscOptions);
        return RunAppendNoCopy(pipe.Writer, pipe.Reader, completePipe: () => pipe.Dispose());
    }

    // ---------- Variant B — "copy" (zero-copy realistic) ----------

    [Benchmark(Baseline = true), BenchmarkCategory("copy")]
    public Task BclPipe_GetSpan_Copy()
    {
        var pipe = new Pipe(BclOptions);
        return RunGetSpanCopy(pipe.Writer, pipe.Reader, completePipe: () => { });
    }

    [Benchmark, BenchmarkCategory("copy")]
    public Task SpscPipe_GetSpan_Copy()
    {
        var pipe = new SpscPipelines.SpscPipe(SpscOptions);
        return RunGetSpanCopy(pipe.Writer, pipe.Reader, completePipe: () => pipe.Dispose());
    }

    [Benchmark, BenchmarkCategory("copy")]
    public Task SpscPipe_Append()
    {
        var pipe = new SpscPipelines.SpscPipe(SpscOptions);
        return RunAppend(pipe.Writer, pipe.Reader, completePipe: () => pipe.Dispose());
    }

    // ---------- Producer/consumer harnesses ----------

    private async Task RunGetSpanNoCopy(PipeWriter writer, PipeReader reader, Action completePipe)
    {
        int bufferSize = BufferSize;
        int flushEvery = BuffersBeforeFlush;
        int totalBuffers = TotalBytes / bufferSize;

        var producer = Task.Run(async () =>
        {
            for (int i = 0; i < totalBuffers; i++)
            {
                var span = writer.GetSpan(bufferSize);
                span.Slice(0, bufferSize).Fill(0x42);
                writer.Advance(bufferSize);
                if ((i + 1) % flushEvery == 0)
                    await writer.FlushAsync();
            }
            await writer.FlushAsync();
            await writer.CompleteAsync();
        });

        await DrainAndDispose(reader, producer, completePipe);
    }

    private async Task RunAppendNoCopy(SpscPipeWriter writer, PipeReader reader, Action completePipe)
    {
        int bufferSize = BufferSize;
        int flushEvery = BuffersBeforeFlush;
        int totalBuffers = TotalBytes / bufferSize;

        var producer = Task.Run(async () =>
        {
            for (int i = 0; i < totalBuffers; i++)
            {
                var owner = MemoryPool<byte>.Shared.Rent(bufferSize);
                owner.Memory.Span.Slice(0, bufferSize).Fill(0x42);
                writer.Append(owner, 0, bufferSize);
                if ((i + 1) % flushEvery == 0)
                    await writer.FlushAsync();
            }
            await writer.FlushAsync();
            await writer.CompleteAsync();
        });

        await DrainAndDispose(reader, producer, completePipe);
    }

    private async Task RunGetSpanCopy(PipeWriter writer, PipeReader reader, Action completePipe)
    {
        int bufferSize = BufferSize;
        int flushEvery = BuffersBeforeFlush;
        int totalBuffers = TotalBytes / bufferSize;

        var producer = Task.Run(async () =>
        {
            for (int i = 0; i < totalBuffers; i++)
            {
                // Simulate "received an IMemoryOwner from elsewhere".
                using var source = MemoryPool<byte>.Shared.Rent(bufferSize);
                source.Memory.Span.Slice(0, bufferSize).Fill(0x42);

                var span = writer.GetSpan(bufferSize);
                source.Memory.Span.Slice(0, bufferSize).CopyTo(span);
                writer.Advance(bufferSize);
                if ((i + 1) % flushEvery == 0)
                    await writer.FlushAsync();
            }
            await writer.FlushAsync();
            await writer.CompleteAsync();
        });

        await DrainAndDispose(reader, producer, completePipe);
    }

    private async Task RunAppend(SpscPipeWriter writer, PipeReader reader, Action completePipe)
    {
        int bufferSize = BufferSize;
        int flushEvery = BuffersBeforeFlush;
        int totalBuffers = TotalBytes / bufferSize;

        var producer = Task.Run(async () =>
        {
            for (int i = 0; i < totalBuffers; i++)
            {
                // Simulate "received an IMemoryOwner from elsewhere"; transfer ownership to pipe.
                var source = MemoryPool<byte>.Shared.Rent(bufferSize);
                source.Memory.Span.Slice(0, bufferSize).Fill(0x42);
                writer.Append(source, 0, bufferSize);
                if ((i + 1) % flushEvery == 0)
                    await writer.FlushAsync();
            }
            await writer.FlushAsync();
            await writer.CompleteAsync();
        });

        await DrainAndDispose(reader, producer, completePipe);
    }

    private static async Task DrainAndDispose(PipeReader reader, Task producer, Action completePipe)
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
