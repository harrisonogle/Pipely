using BenchmarkDotNet.Running;
using SpscPipelines.HotHandoff;
using SpscPipelines.HotHandoff.Benchmarks;
using System.CommandLine;

// Anything that isn't the latency sub-command (including no args, or BDN args
// like --filter / --job) is forwarded to BenchmarkSwitcher. FromAssembly
// auto-discovers every public [Benchmark] class in the project, so adding a
// second BDN class later requires no Program.cs edit (matches the pattern in
// tests/SpscPipe.Benchmarks/Program.cs).
if (args.Length == 0 || args[0] != "latency")
{
    BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
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

var copyChunkOption = new Option<bool>("--copy-chunk")
{
    Description = "Producer copies a full message-sized zero-filled chunk into each rented buffer (matching DispatcherThroughputBench.ProduceAndDrain's `chunk.CopyTo(memory)` per-event memcpy). When omitted (default), only the 8-byte timestamp is written and remaining bytes are uninitialized — the high-rate per-message latency workload. Pair with --count 256 --size 4096 for apples-to-apples with the BDN throughput row.",
    DefaultValueFactory = _ => false,
};

var latencyCommand = new Command("latency", "Run the latency comparison (tp-default vs hot-handoff)")
{
    countOption, sizeOption, trialsOption, warmupOption, copyChunkOption,
};
latencyCommand.SetAction(async parseResult =>
{
    int count      = parseResult.GetValue(countOption);
    int size       = parseResult.GetValue(sizeOption);
    int trials     = parseResult.GetValue(trialsOption);
    int warmup     = parseResult.GetValue(warmupOption);
    bool copyChunk = parseResult.GetValue(copyChunkOption);
    await RunLatency(count, size, trials, warmup, copyChunk);
    return 0;
});

var rootCommand = new RootCommand("SpscPipelines.HotHandoff benchmark harness")
{
    latencyCommand,
};

return await rootCommand.Parse(args).InvokeAsync();

static async Task RunLatency(int count, int size, int trials, int warmup, bool copyChunk)
{
    string writeMode = copyChunk ? "full chunk copy" : "timestamp-only writes";
    Console.WriteLine($"Latency comparison: {count:N0} messages × {size} B, {trials} trials, {warmup} warmup, {writeMode}");

    for (int w = 0; w < warmup; w++)
    {
        Console.WriteLine($"  Warmup trial {w + 1}/{warmup} (not recorded)");
        _ = await DispatcherLatencyHarness.Run(null, count, size, copyChunk);
        using var dispatcher = new HotHandoffContinuationDispatcher();
        _ = await DispatcherLatencyHarness.Run(dispatcher, count, size, copyChunk);
    }

    for (int t = 0; t < trials; t++)
    {
        Console.WriteLine();
        Console.WriteLine($"=== Trial {t + 1}/{trials} ===");

        var tpStats = await DispatcherLatencyHarness.Run(null, count, size, copyChunk);

        LatencyStats hhStats;
        long slotDispatched, tpOverflowed;
        using (var dispatcher = new HotHandoffContinuationDispatcher())
        {
            hhStats        = await DispatcherLatencyHarness.Run(dispatcher, count, size, copyChunk);
            slotDispatched = dispatcher.SlotDispatchedCount;
            tpOverflowed   = dispatcher.TpOverflowedCount;
        }

        DispatcherLatencyHarness.PrintComparison("Message latency (ns)", tpStats, hhStats);
        PrintDispatchBreakdown(slotDispatched, tpOverflowed);
    }
}

static void PrintDispatchBreakdown(long slot, long tp)
{
    long total = slot + tp;
    if (total == 0) return;
    double slotPct = 100.0 * slot / total;
    double tpPct   = 100.0 * tp   / total;
    Console.WriteLine();
    Console.WriteLine("=== HotHandoff dispatch breakdown ===");
    Console.WriteLine($"| Path                 |       Count |    %    |");
    Console.WriteLine($"|:---------------------|------------:|--------:|");
    Console.WriteLine($"| Slot (worker thread) | {slot,11:N0} | {slotPct,6:F2}% |");
    Console.WriteLine($"| TP overflow          | {tp,11:N0} | {tpPct,6:F2}% |");
    Console.WriteLine($"| Total                | {total,11:N0} | 100.00% |");
}
