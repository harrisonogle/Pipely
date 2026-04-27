using BenchmarkDotNet.Running;
using SpscPipelines.HotHandoff;
using SpscPipelines.HotHandoff.Benchmarks;
using System.CommandLine;

// Anything that isn't the latency sub-command (including no args, or BDN args
// like --filter / --job) is forwarded to BenchmarkSwitcher.
if (args.Length == 0 || args[0] != "latency")
{
    BenchmarkSwitcher.FromTypes(new[] { typeof(DispatcherThroughputBench) }).Run(args);
    return 0;
}

var countOption = new Option<int>("--count")
{
    Description = "Message count per latency trial",
    DefaultValueFactory = _ => 100_000,
};

var sizeOption = new Option<int>("--size")
{
    Description = "Message size in bytes (>= 8)",
    DefaultValueFactory = _ => 256,
};

var trialsOption = new Option<int>("--trials")
{
    Description = "Number of latency trials (each trial runs both configurations)",
    DefaultValueFactory = _ => 3,
};

var warmupOption = new Option<int>("--warmup")
{
    Description = "Warmup trials before recording (not included in results)",
    DefaultValueFactory = _ => 1,
};

var latencyCommand = new Command("latency", "Run the latency comparison (tp-default vs hot-handoff)")
{
    countOption, sizeOption, trialsOption, warmupOption,
};
latencyCommand.SetAction(async parseResult =>
{
    int count   = parseResult.GetValue(countOption);
    int size    = parseResult.GetValue(sizeOption);
    int trials  = parseResult.GetValue(trialsOption);
    int warmup  = parseResult.GetValue(warmupOption);
    await RunLatency(count, size, trials, warmup);
    return 0;
});

var rootCommand = new RootCommand("SpscPipelines.HotHandoff benchmark harness")
{
    latencyCommand,
};

return await rootCommand.Parse(args).InvokeAsync();

static async Task RunLatency(int count, int size, int trials, int warmup)
{
    Console.WriteLine($"Latency comparison: {count:N0} messages × {size} B, {trials} trials, {warmup} warmup");

    for (int w = 0; w < warmup; w++)
    {
        Console.WriteLine($"  Warmup trial {w + 1}/{warmup} (not recorded)");
        _ = await DispatcherLatencyHarness.Run(null, count, size);
        using var dispatcher = new HotHandoffContinuationDispatcher();
        _ = await DispatcherLatencyHarness.Run(dispatcher, count, size);
    }

    for (int t = 0; t < trials; t++)
    {
        Console.WriteLine();
        Console.WriteLine($"=== Trial {t + 1}/{trials} ===");

        var tpStats = await DispatcherLatencyHarness.Run(null, count, size);

        LatencyStats hhStats;
        using (var dispatcher = new HotHandoffContinuationDispatcher())
            hhStats = await DispatcherLatencyHarness.Run(dispatcher, count, size);

        DispatcherLatencyHarness.PrintComparison("Message latency (ns)", tpStats, hhStats);
    }
}
