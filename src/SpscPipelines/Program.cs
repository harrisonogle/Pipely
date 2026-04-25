using SpscPipelines;

var example = new MarketDataExample();

using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));

Console.WriteLine("Starting.");

await Task.WhenAll(
    Task.Run(() => example.RunProducer(cancellation.Token)),
    Task.Run(() => example.RunConsumer(cancellation.Token)));

Console.WriteLine("Done.");


public struct MarketTick
{
    public double Bid;
    public double Ask;
    public long TimestampTicks;
    public long Symbol;
}

public class MarketDataExample
{
    private readonly TripleBuffer<MarketTick> _buffer = new();
    
    public void RunProducer(CancellationToken ct)
    {
        // Producer thread — receives market data and publishes snapshots
        long symbol = 12345;
        var rng = new Random();
        
        while (!ct.IsCancellationRequested)
        {
            // Get a writable ref to the producer's currently-owned slot
            ref MarketTick slot = ref _buffer.ProducerSlot();
            
            // Mutate fields directly. No allocation.
            slot.Bid = 100.0 + rng.NextDouble();
            slot.Ask = slot.Bid + 0.01;
            slot.TimestampTicks = DateTime.UtcNow.Ticks;
            slot.Symbol = symbol;
            
            // Publish: atomically swap the producer's slot with the published slot.
            _buffer.Publish();
        }
    }
    
    public void RunConsumer(CancellationToken ct)
    {
        // Consumer thread — reads the latest available snapshot
        while (!ct.IsCancellationRequested)
        {
            if (_buffer.TryAcquire())
            {
                ref readonly MarketTick snap = ref _buffer.ConsumerSlot();
                
                Console.WriteLine(
                    $"Symbol={snap.Symbol} Bid={snap.Bid:F4} Ask={snap.Ask:F4}");
            }
            else
            {
                // No new data since last acquire. Sleep, spin, or do other work.
                Thread.SpinWait(100);
                Console.WriteLine("Spinning");
            }
        }
    }
}