using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;

namespace SpscPipe.Benchmarks;

internal static class LatencyHarness
{
    private static readonly int[]  DefaultSizes = [16, 64, 256, 4096, 65536];
    private static readonly Mode[] AllModes     = [Mode.Normal, Mode.Backpressure];
    private static readonly Impl[] AllImpls     = [Impl.SpscPipe, Impl.BclPipe];

    internal enum Mode { Normal, Backpressure }

    internal sealed record Cli(
        long      Messages,
        Mode[]    Modes,
        Impl[]    Impls,
        int[]     Sizes);

    internal static int Run(string[] args)
    {
        if (!TryParse(args, out var cli, out var error))
        {
            Console.Error.WriteLine(error);
            return 1;
        }

        foreach (var mode in cli.Modes)
        {
            PrintHeader(mode);
            foreach (var impl in cli.Impls)
                foreach (var size in cli.Sizes)
                    RunCell(impl, size, mode, cli.Messages);
        }
        return 0;
    }

    private static void PrintHeader(Mode mode)
    {
        var (pause, resume) = ConfigFor(mode);
        Console.WriteLine();
        Console.WriteLine($"## Latency — {mode.ToString().ToLowerInvariant()} (Pause={pause}, Resume={resume})");
        Console.WriteLine();
        Console.WriteLine("| Impl     | MessageSize | p50 (ns) | p90 (ns) | p99 (ns) | p99.9 (ns) | max (ns) |");
        Console.WriteLine("| -------- | ----------: | -------: | -------: | -------: | ---------: | -------: |");
    }

    private const int    WarmupMessages = 10_000;
    private static readonly double NsPerTick = 1_000_000_000.0 / Stopwatch.Frequency;

    private static void RunCell(Impl impl, int size, Mode mode, long messages)
    {
        var (pause, resume) = ConfigFor(mode);
        var cfg = new PipeConfig(MinimumSegmentSize: 4096,
                                 PauseWriterThreshold: pause,
                                 ResumeWriterThreshold: resume);

        // Warmup: pay JIT and pool-rent costs out of band.
        var warmupHist = new Histogram();
        RunOne(impl, size, cfg, WarmupMessages, warmupHist);

        var hist = new Histogram();
        RunOne(impl, size, cfg, messages, hist);

        var p50  = hist.Percentile(0.50);
        var p90  = hist.Percentile(0.90);
        var p99  = hist.Percentile(0.99);
        var p999 = hist.Percentile(0.999);
        var max  = hist.Max;
        Console.WriteLine($"| {impl,-8} | {size,11} | {p50,8} | {p90,8} | {p99,8} | {p999,10} | {max,8} |");
    }

    private static void RunOne(Impl impl, int messageSize, PipeConfig cfg, long messages, Histogram hist)
    {
        using var pipe = AdapterFactory.Build(impl, cfg);

        var producer = Task.Run(() => Producer(pipe, messageSize, messages));
        var consumer = Task.Run(() => Consumer(pipe, messageSize, hist));

        Task.WhenAll(producer, consumer).GetAwaiter().GetResult();
    }

    private static async Task Producer(IPipeAdapter pipe, int messageSize, long messages)
    {
        for (long i = 0; i < messages; i++)
        {
            var span = pipe.Writer.GetSpan(messageSize);
            BinaryPrimitives.WriteInt64LittleEndian(span, Stopwatch.GetTimestamp());
            pipe.Writer.Advance(messageSize);
            var fr = await pipe.Writer.FlushAsync();
            if (fr.IsCompleted || fr.IsCanceled) break;
        }
        pipe.Writer.Complete();
    }

    private static async Task Consumer(IPipeAdapter pipe, int messageSize, Histogram hist)
    {
        while (true)
        {
            var rr = await pipe.Reader.ReadAsync();
            if (rr.IsCanceled) break;
            var buffer = rr.Buffer;

            while (buffer.Length >= messageSize)
            {
                long ts  = ReadTimestamp(buffer);
                long now = Stopwatch.GetTimestamp();
                long ns  = (long)((now - ts) * NsPerTick);
                hist.Record(ns);
                buffer = buffer.Slice(messageSize);
            }

            pipe.Reader.AdvanceTo(buffer.Start, rr.Buffer.End);
            if (rr.IsCompleted && buffer.IsEmpty) break;
        }
        pipe.Reader.Complete();
    }

    private static long ReadTimestamp(ReadOnlySequence<byte> buffer)
    {
        var first = buffer.First.Span;
        if (first.Length >= 8)
            return BinaryPrimitives.ReadInt64LittleEndian(first);

        Span<byte> scratch = stackalloc byte[8];
        buffer.Slice(0, 8).CopyTo(scratch);
        return BinaryPrimitives.ReadInt64LittleEndian(scratch);
    }

    internal static (long Pause, long Resume) ConfigFor(Mode mode) => mode switch
    {
        Mode.Normal       => (65536, 32768),
        Mode.Backpressure => (4096,  2048),
        _                 => throw new ArgumentOutOfRangeException(nameof(mode)),
    };

    private static bool TryParse(string[] args, out Cli cli, out string error)
    {
        long messages = 1_000_000;
        Mode[] modes  = AllModes;
        Impl[] impls  = AllImpls;
        int[]  sizes  = DefaultSizes;

        for (var i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--messages":
                    if (++i >= args.Length || !long.TryParse(args[i], out messages) || messages < 1)
                        return Fail(out cli, out error, "--messages requires a positive integer");
                    break;
                case "--mode":
                    if (++i >= args.Length) return Fail(out cli, out error, "--mode requires a value");
                    modes = args[i] switch
                    {
                        "normal"       => [Mode.Normal],
                        "backpressure" => [Mode.Backpressure],
                        "both"         => AllModes,
                        _              => null!,
                    };
                    if (modes is null) return Fail(out cli, out error, $"unknown --mode: {args[i]}");
                    break;
                case "--impl":
                    if (++i >= args.Length) return Fail(out cli, out error, "--impl requires a value");
                    impls = args[i] switch
                    {
                        "spsc" => [Impl.SpscPipe],
                        "bcl"  => [Impl.BclPipe],
                        "both" => AllImpls,
                        _      => null!,
                    };
                    if (impls is null) return Fail(out cli, out error, $"unknown --impl: {args[i]}");
                    break;
                case "--sizes":
                    if (++i >= args.Length) return Fail(out cli, out error, "--sizes requires a comma-separated list");
                    var parts = args[i].Split(',', StringSplitOptions.RemoveEmptyEntries);
                    sizes = new int[parts.Length];
                    for (var j = 0; j < parts.Length; j++)
                    {
                        if (!int.TryParse(parts[j], out var s) || s < 8)
                            return Fail(out cli, out error, $"invalid --sizes entry '{parts[j]}' (must be int >= 8 to fit timestamp)");
                        sizes[j] = s;
                    }
                    break;
                case "--help" or "-h":
                    return Fail(out cli, out error,
                        "latency [--messages N] [--mode normal|backpressure|both] [--impl spsc|bcl|both] [--sizes 16,64,...]");
                default:
                    return Fail(out cli, out error, $"unknown flag: {args[i]}");
            }
        }

        cli = new Cli(messages, modes, impls, sizes);
        error = "";
        return true;

        static bool Fail(out Cli cli, out string error, string message)
        {
            cli = default!;
            error = message;
            return false;
        }
    }
}
