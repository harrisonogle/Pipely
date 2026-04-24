namespace SpscPipe.Benchmarks;

internal static class Workloads
{
    // Producer writes `chunkSize` bytes per Advance, batches `batchPerFlush`
    // chunks per FlushAsync, and stops once `totalBytes` have been written.
    // No bytes are written into the buffer — both pipe impls are byte-agnostic
    // on the write path, so the omission preserves apples-to-apples while
    // avoiding memcpy noise. See spec §5.2 / §9.
    internal static async Task BulkProducer(
        IPipeAdapter pipe, int chunkSize, int batchPerFlush, long totalBytes)
    {
        long written = 0;
        while (written < totalBytes)
        {
            var batchTarget = written + (long)chunkSize * batchPerFlush;
            if (batchTarget > totalBytes) batchTarget = totalBytes;

            while (written < batchTarget)
            {
                var n = (int)Math.Min(chunkSize, batchTarget - written);
                pipe.Writer.GetMemory(n);
                pipe.Writer.Advance(n);
                written += n;
            }

            var fr = await pipe.Writer.FlushAsync();
            if (fr.IsCompleted || fr.IsCanceled) break;
        }
        pipe.Writer.Complete();
    }

    // Consumer reads everything until IsCompleted with empty buffer; advances
    // past every byte. Records nothing — used by throughput benchmarks where
    // we only care about wall time.
    internal static async Task DrainConsumer(IPipeAdapter pipe)
    {
        while (true)
        {
            var rr = await pipe.Reader.ReadAsync();
            if (rr.IsCanceled) break;
            pipe.Reader.AdvanceTo(rr.Buffer.End);
            if (rr.IsCompleted && rr.Buffer.IsEmpty) break;
        }
        pipe.Reader.Complete();
    }
}
