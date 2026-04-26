namespace SpscPipe.Benchmarks;

internal sealed class Histogram
{
    private const int BucketCount = 64;            // covers ~1ns..~10s in log buckets
    private readonly long[] _buckets = new long[BucketCount];
    private long _count;

    public void Record(long elapsedTicks)
    {
        if (elapsedTicks <= 0) elapsedTicks = 1;
        int bucket = 63 - System.Numerics.BitOperations.LeadingZeroCount((ulong)elapsedTicks);
        if (bucket >= BucketCount) bucket = BucketCount - 1;
        _buckets[bucket]++;
        _count++;
    }

    public long Percentile(double p)
    {
        long target = (long)(_count * p);
        long acc = 0;
        for (int i = 0; i < BucketCount; i++)
        {
            acc += _buckets[i];
            if (acc >= target) return 1L << i;
        }
        return 1L << (BucketCount - 1);
    }

    public long Count => _count;
}
