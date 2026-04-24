namespace SpscPipe.Benchmarks;

internal static class Program
{
    private static int Main(string[] args)
    {
        if (args.Length == 0)
        {
            PrintUsage();
            return 1;
        }

        return args[0] switch
        {
            "bdn"     => RunBdn(args.AsSpan(1).ToArray()),
            "latency" => RunLatency(args.AsSpan(1).ToArray()),
            _         => Unknown(args[0]),
        };

        static int Unknown(string mode)
        {
            Console.Error.WriteLine($"unknown mode: {mode}");
            PrintUsage();
            return 1;
        }
    }

    private static int RunBdn(string[] args)
    {
        // Filled in by Task 5.
        Console.WriteLine("bdn mode: not yet implemented");
        return 0;
    }

    private static int RunLatency(string[] args)
    {
        // Filled in by Task 7.
        Console.WriteLine("latency mode: not yet implemented");
        return 0;
    }

    private static void PrintUsage() =>
        Console.WriteLine("""
            SpscPipe.Benchmarks
              bdn      [BenchmarkDotNet flags...]   throughput + allocation benchmarks
              latency  [--messages N] [--mode normal|backpressure|both]
                       [--impl spsc|bcl|both] [--sizes 16,64,...]
                                                    per-flush latency harness
            """);
}
