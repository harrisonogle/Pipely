using Pipely;
using Xunit;

namespace PipelyTests;

public class TripleBufferResetTests
{
    [Fact]
    public void Reset_AfterPublishAcquireCycle_BehavesAsNewlyConstructed()
    {
        var tb = new TripleBuffer<int>();

        // Use it through several publish/acquire cycles.
        for (int i = 1; i <= 3; i++)
        {
            tb.ProducerSlot() = i * 100;
            tb.Publish();
            Assert.True(tb.TryAcquire());
            Assert.Equal(i * 100, tb.ConsumerSlot());
        }

        tb.Reset();

        // Constructor's initial state: no publication yet → TryAcquire returns false.
        Assert.False(tb.TryAcquire());

        // A fresh publish/acquire cycle works identically to a brand-new TripleBuffer.
        tb.ProducerSlot() = 42;
        tb.Publish();
        Assert.True(tb.TryAcquire());
        Assert.Equal(42, tb.ConsumerSlot());
    }

    [Fact]
    public void Reset_OnNeverUsedTripleBuffer_NoThrow_StillBehavesAsNew()
    {
        var tb = new TripleBuffer<int>();
        tb.Reset();

        Assert.False(tb.TryAcquire());
        tb.ProducerSlot() = 7;
        tb.Publish();
        Assert.True(tb.TryAcquire());
        Assert.Equal(7, tb.ConsumerSlot());
    }

    [Fact]
    public void Reset_ProducerSlotReturnsDefault()
    {
        // After Reset, the slot the producer sees is zeroed (default(T)). This is the
        // visible-from-outside check that slot data is cleared; combined with the
        // behavioral test above, it gives us reasonable confidence that all 3 slots
        // are reset (the producer rotates through all 3 across cycles).
        var tb = new TripleBuffer<int>();
        tb.ProducerSlot() = 999;
        tb.Publish();
        tb.ProducerSlot() = 888;
        tb.Publish();
        tb.ProducerSlot() = 777;

        tb.Reset();

        Assert.Equal(0, tb.ProducerSlot());
    }
}
