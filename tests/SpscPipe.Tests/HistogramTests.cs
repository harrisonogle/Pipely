using SpscPipe.Benchmarks;

namespace SpscPipe.Tests;

public class HistogramTests
{
    [Fact]
    public void Records_Linear_Range_Percentiles_Within_Bucket_Resolution()
    {
        var h = new Histogram();
        for (long v = 1; v <= 1000; v++) h.Record(v);

        Assert.Equal(1000, h.Count);
        // p50 should be near 500. Allow ~5% slack for log-bucket coarseness
        // at this scale.
        var p50 = h.Percentile(0.50);
        Assert.InRange(p50, 475, 525);

        var p99 = h.Percentile(0.99);
        Assert.InRange(p99, 975, 1024);

        Assert.True(h.Max >= 1000);
    }

    [Fact]
    public void Empty_Histogram_Returns_Zero_For_Percentiles()
    {
        var h = new Histogram();
        Assert.Equal(0, h.Count);
        Assert.Equal(0, h.Percentile(0.5));
        Assert.Equal(0, h.Max);
    }

    [Fact]
    public void Clamps_Negative_And_Zero_To_Bucket_One()
    {
        var h = new Histogram();
        h.Record(0);
        h.Record(-5);
        h.Record(1);
        Assert.Equal(3, h.Count);
        Assert.True(h.Percentile(0.99) >= 0);
    }

    [Fact]
    public void Records_Wide_Range_Without_Overflow()
    {
        var h = new Histogram();
        for (int i = 0; i < 1_000_000; i++) h.Record(100);
        h.Record(1_000_000_000L);
        Assert.Equal(1_000_001, h.Count);
        Assert.True(h.Max >= 1_000_000_000L);
    }
}
