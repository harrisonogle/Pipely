using BenchmarkDotNet.Running;
using SpscPipe.Benchmarks;
using System.CommandLine;

if (args.Length == 0 || args[0] != "latency")
{
    BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    return 0;
}

var countOption = new Option<int>("--count")
{
    Description = "Message count",
    DefaultValueFactory = _ => 100_000,
};

var sizeOption = new Option<int>("--size")
{
    Description = "Message size in bytes",
    DefaultValueFactory = _ => 256,
};

var trialsOption = new Option<int>("--trials")
{
    Description = "Trial count",
    DefaultValueFactory = _ => 1,
};

var latencyCommand = new Command("latency", "Run the latency benchmark")
{
    countOption,
    sizeOption,
    trialsOption,
};

latencyCommand.SetAction(async parseResult =>
{
    int count = parseResult.GetValue(countOption);
    int size = parseResult.GetValue(sizeOption);
    int trials = parseResult.GetValue(trialsOption);

    await RunLatencyBenchmarks(count, size, trials);

    return 0;
});

var rootCommand = new RootCommand("Benchmark harness")
{
    latencyCommand,
};

return await rootCommand.Parse(args).InvokeAsync();

static async Task RunLatencyBenchmarks(int count, int size, int trials)
{
    var samples = new LatencySamples(count);

    if (trials == 1)
    {
        Console.WriteLine($"Running BCL Pipe ({count:N0} × {size} B).");
        LatencyStats bclStats;
        using (var bcl = new BclPipeAdapter())
            bclStats = await LatencyHarness.Run(bcl, samples, size);

        Console.WriteLine($"Running SpscPipe ({count:N0} × {size} B).");
        LatencyStats spscStats;
        using (var spsc = new SpscPipeAdapter())
            spscStats = await LatencyHarness.Run(spsc, samples, size);

        LatencyHarness.PrintComparison("Transfer", "BCL Pipe", bclStats.Message, "SpscPipe", spscStats.Message);
        LatencyHarness.PrintComparison("FlushAsync", "BCL Pipe", bclStats.Flush, "SpscPipe", spscStats.Flush);
        LatencyHarness.PrintComparison("ReadAsync", "BCL Pipe", bclStats.Read, "SpscPipe", spscStats.Read);
    }
    else
    {
        Console.WriteLine($"Running {trials} trials of {count:N0} × {size} B per trial (BCL then SpscPipe each trial).");

        for (int t = 1; t <= trials; t++)
        {
            LatencyStats bclStats;
            using (var bcl = new BclPipeAdapter())
                bclStats = await LatencyHarness.Run(bcl, samples, size);

            LatencyStats spscStats;
            using (var spsc = new SpscPipeAdapter())
                spscStats = await LatencyHarness.Run(spsc, samples, size);

            Console.WriteLine();
            Console.WriteLine($"=== Trial {t}/{trials} ===");
            LatencyHarness.PrintComparison(nameof(bclStats.Message), "BCL Pipe", bclStats.Message, "SpscPipe", spscStats.Message);
            LatencyHarness.PrintComparison(nameof(bclStats.Flush), "BCL Pipe", bclStats.Flush, "SpscPipe", spscStats.Flush);
            LatencyHarness.PrintComparison(nameof(bclStats.SyncFlush), "BCL Pipe", bclStats.SyncFlush, "SpscPipe", spscStats.SyncFlush);
            LatencyHarness.PrintComparison(nameof(bclStats.AsyncFlush), "BCL Pipe", bclStats.AsyncFlush, "SpscPipe", spscStats.AsyncFlush);
            LatencyHarness.PrintComparison(nameof(bclStats.Read), "BCL Pipe", bclStats.Read, "SpscPipe", spscStats.Read);
            LatencyHarness.PrintComparison(nameof(bclStats.SyncRead), "BCL Pipe", bclStats.SyncRead, "SpscPipe", spscStats.SyncRead);
            LatencyHarness.PrintComparison(nameof(bclStats.AsyncRead), "BCL Pipe", bclStats.AsyncRead, "SpscPipe", spscStats.AsyncRead);
        }
    }
}
