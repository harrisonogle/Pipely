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

    // Filled in by Task 8.
    private static void RunCell(Impl impl, int size, Mode mode, long messages)
    {
        Console.WriteLine($"| {impl,-8} | {size,11} | {0,8} | {0,8} | {0,8} | {0,10} | {0,8} |");
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
