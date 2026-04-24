using System.Numerics;

namespace SpscPipe.Benchmarks;

// Log-linear histogram: 64 sub-buckets per power-of-2, covering 1..2^41 ns
// (~36 minutes). Resolution at any value v is v / 64. Sufficient for
// per-flush latency percentiles where v ranges from ~100ns to ~10ms.
internal sealed class Histogram
{
    private const int SubBucketCount   = 64;
    private const int PowerOfTwoCount  = 41;
    private readonly long[] _buckets   = new long[SubBucketCount * PowerOfTwoCount];

    public long Count { get; private set; }
    public long Max { get; private set; }

    public void Record(long valueNs)
    {
        if (valueNs < 1) valueNs = 1;
        int msb = 63 - BitOperations.LeadingZeroCount((ulong)valueNs);
        if (msb >= PowerOfTwoCount) msb = PowerOfTwoCount - 1;

        // Sub-bucket: take the next 6 bits below the MSB (0..63). For v < 64
        // (msb < 6) the value itself fits in 6 bits — index by it directly.
        int subIndex = msb >= 6
            ? (int)((valueNs >> (msb - 6)) & (SubBucketCount - 1))
            : (int)(valueNs & (SubBucketCount - 1));

        int bucket = msb * SubBucketCount + subIndex;
        _buckets[bucket]++;
        Count++;
        if (valueNs > Max) Max = valueNs;
    }

    // Returns the upper edge of the bucket containing the requested
    // percentile. Returns 0 if the histogram is empty.
    public long Percentile(double p)
    {
        if (Count == 0) return 0;
        if (p < 0) p = 0;
        if (p > 1) p = 1;

        long target = (long)Math.Ceiling(p * Count);
        if (target < 1) target = 1;

        long acc = 0;
        for (int i = 0; i < _buckets.Length; i++)
        {
            acc += _buckets[i];
            if (acc >= target) return BucketUpperBoundNs(i);
        }
        return Max;
    }

    private static long BucketUpperBoundNs(int bucket)
    {
        int msb = bucket / SubBucketCount;
        int sub = bucket % SubBucketCount;
        if (msb < 6) return sub + 1;
        // Sub-bucket s within power-of-2 group msb covers values
        // [2^msb + s * 2^(msb-6), 2^msb + (s+1) * 2^(msb-6)). Upper edge = end.
        return (1L << msb) + ((long)(sub + 1) << (msb - 6));
    }
}
