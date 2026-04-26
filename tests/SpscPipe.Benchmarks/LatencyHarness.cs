namespace SpscPipe.Benchmarks;

internal static class LatencyHarness
{
    public static async Task Run(IPipeAdapter adapter, int messages, int messageBytes)
    {
        var hist = new Histogram();
        // Producer pauses briefly between writes; consumer measures end-to-end latency.
        // Full implementation deferred to Task 13.
        Console.WriteLine($"Latency harness skeleton: {messages} msgs of {messageBytes}B");
        Console.WriteLine($"Buckets recorded: {hist.Count}");
        await Task.CompletedTask;
    }
}
