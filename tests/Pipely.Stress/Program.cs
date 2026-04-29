using Pipely.Stress;

int seedCount = args.Length > 0 ? int.Parse(args[0]) : 10;
long bytesPerSeed = args.Length > 1 ? long.Parse(args[1]) : 1L << 22;   // 4 MiB

var harness = new StressHarness(Pipely.PipeOptions.Default, TimeSpan.FromSeconds(30));
int failures = 0;

for (int i = 0; i < seedCount; i++)
{
    int seed = i + 1;
    using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(1));
    var result = await harness.RunOnce(seed, bytesPerSeed, cts.Token);
    Console.WriteLine($"seed={seed,4} status={result.Status,-30} produced={result.Produced,12} consumed={result.Consumed,12}");
    if (result.Status != "ok") { failures++; if (result.ProducerEx != null) Console.WriteLine($"  producer: {result.ProducerEx}"); if (result.ConsumerEx != null) Console.WriteLine($"  consumer: {result.ConsumerEx}"); }
}

Console.WriteLine($"\n{seedCount - failures}/{seedCount} seeds passed.");
return failures == 0 ? 0 : 1;
