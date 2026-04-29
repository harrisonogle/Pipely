using SpPipe = SpscPipelines.SpscPipe;
using SpscPipelines;

namespace SpscPipe.Stress;

internal sealed class StressHarness
{
    private readonly SpscPipeOptions _options;
    private readonly TimeSpan _duration;

    public StressHarness(SpscPipeOptions options, TimeSpan duration)
    {
        _options  = options;
        _duration = duration;
    }

    public async Task<StressResult> RunOnce(int seed, long totalBytes, CancellationToken ct)
    {
        var owners = new System.Collections.Concurrent.ConcurrentBag<StressOwner>();
        StressResult result;
        {
            using var pipe = new SpPipe(_options);
            var producerRng = new Random(seed);
            var consumerRng = new Random(seed ^ 0x5A5A_5A5A);

            long produced = 0;
            long consumed = 0;
            Exception? producerEx = null, consumerEx = null;

            var producer = Task.Run(async () =>
            {
                try
                {
                    while (produced < totalBytes && !ct.IsCancellationRequested)
                    {
                        int chunk = producerRng.Next(1, 4097);
                        chunk = (int)Math.Min(chunk, totalBytes - produced);
                        bool donate = producerRng.Next(16) == 0 && chunk > 0;
                        if (donate)
                        {
                            var bytes = new byte[chunk];
                            for (int i = 0; i < chunk; i++)
                                bytes[i] = ByteSequence.ByteAt(produced + i);
                            var owner = new StressOwner(bytes);
                            owners.Add(owner);
                            pipe.Writer.Splice(owner);
                        }
                        else
                        {
                            var mem = pipe.Writer.GetMemory(chunk);
                            for (int i = 0; i < chunk; i++)
                                mem.Span[i] = ByteSequence.ByteAt(produced + i);
                            pipe.Writer.Advance(chunk);
                        }
                        produced += chunk;

                        if (producerRng.Next(8) == 0) await Task.Yield();
                        var fr = await pipe.Writer.FlushAsync(ct);
                        if (fr.IsCompleted) break;
                    }
                }
                catch (Exception e) { producerEx = e; }
                finally
                {
                    try { pipe.Writer.Complete(producerEx); } catch { }
                }
            }, ct);

            var consumer = Task.Run(async () =>
            {
                try
                {
                    while (!ct.IsCancellationRequested)
                    {
                        var rr = await pipe.Reader.ReadAsync(ct);
                        long bufferStart = consumed;
                        long offset = bufferStart;
                        foreach (var memory in rr.Buffer)
                        {
                            for (int i = 0; i < memory.Length; i++)
                            {
                                byte expected = ByteSequence.ByteAt(offset + i);
                                if (memory.Span[i] != expected)
                                    throw new InvalidOperationException(
                                        $"Byte mismatch at offset {offset + i}: expected 0x{expected:X2}, got 0x{memory.Span[i]:X2}");
                            }
                            offset += memory.Length;
                        }
                        consumed = offset;

                        if (consumerRng.Next(4) == 0 && rr.Buffer.Length > 1)
                        {
                            long takeBytes = consumerRng.Next(1, (int)Math.Min(rr.Buffer.Length, int.MaxValue));
                            consumed = bufferStart + takeBytes;
                            pipe.Reader.AdvanceTo(rr.Buffer.GetPosition(takeBytes));
                        }
                        else
                        {
                            pipe.Reader.AdvanceTo(rr.Buffer.End);
                        }

                        if (rr.IsCompleted && consumed >= produced) break;
                    }
                }
                catch (Exception e) { consumerEx = e; }
                finally
                {
                    try { pipe.Reader.Complete(consumerEx); } catch { }
                }
            }, ct);

            try
            {
                await Task.WhenAll(producer, consumer).WaitAsync(_duration);
                if (producerEx != null || consumerEx != null)
                    result = new StressResult(seed, produced, consumed, "exception", producerEx, consumerEx);
                else
                    result = new StressResult(seed, produced, consumed,
                        consumed == produced ? "ok" : "byte-count mismatch", null, null);
            }
            catch (TimeoutException)
            {
                result = new StressResult(seed, produced, consumed, "timeout — possible deadlock", producerEx, consumerEx);
            }
        }
        // pipe disposed here (using-scope ended). Now verify owner accounting.
        foreach (var o in owners)
        {
            if (o.DisposeCount != 1)
                throw new InvalidOperationException(
                    $"Owner accounting violated: DisposeCount = {o.DisposeCount} (expected 1).");
        }
        return result;
    }

    // Tracks IMemoryOwner.Dispose calls so the harness can verify zero-leak / no-double-dispose
    // at the end of a run.
    private sealed class StressOwner : System.Buffers.IMemoryOwner<byte>
    {
        private readonly byte[] _bytes;
        private bool _disposed;
        public int DisposeCount;
        public StressOwner(byte[] bytes) { _bytes = bytes; }
        public Memory<byte> Memory => _disposed ? throw new ObjectDisposedException(nameof(StressOwner)) : _bytes;
        public void Dispose() { DisposeCount++; _disposed = true; }
    }
}

internal record StressResult(int Seed, long Produced, long Consumed, string Status, Exception? ProducerEx, Exception? ConsumerEx);
