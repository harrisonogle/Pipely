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
                    var mem = pipe.Writer.GetMemory(chunk);
                    for (int i = 0; i < chunk; i++)
                        mem.Span[i] = ByteSequence.ByteAt(produced + i);
                    pipe.Writer.Advance(chunk);
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
        }
        catch (TimeoutException)
        {
            return new StressResult(seed, produced, consumed, "timeout — possible deadlock", producerEx, consumerEx);
        }

        if (producerEx != null || consumerEx != null)
            return new StressResult(seed, produced, consumed, "exception", producerEx, consumerEx);

        return new StressResult(seed, produced, consumed,
            consumed == produced ? "ok" : "byte-count mismatch",
            null, null);
    }
}

internal record StressResult(int Seed, long Produced, long Consumed, string Status, Exception? ProducerEx, Exception? ConsumerEx);
