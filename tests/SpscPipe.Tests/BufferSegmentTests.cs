using System.Buffers;
using SpscPipelines;
using Xunit;

namespace SpscPipe.Tests;

public class BufferSegmentTests
{
    private static readonly object Owner = new();

    [Fact]
    public void RentFrom_SetsAvailableMemoryAndOwnerToken()
    {
        var seg = new BufferSegment();
        seg.RentFrom(MemoryPool<byte>.Shared, sizeHint: 1024, runningIndex: 0, owner: Owner);

        Assert.True(seg.AvailableMemory.Length >= 1024);
        Assert.Equal(0, seg.End);
        Assert.Null(seg.Next);
        Assert.Equal(0, seg.RunningIndex);
        Assert.Same(Owner, seg.OwnerToken);
        // Memory<byte> is a struct; use length comparison instead of Assert.Same.
        Assert.Equal(seg.AvailableMemory.Length, ((ReadOnlySequenceSegment<byte>)seg).Memory.Length);
    }

    [Fact]
    public void Freeze_SetsEndAndMemoryAndNext()
    {
        var seg = new BufferSegment();
        seg.RentFrom(MemoryPool<byte>.Shared, 1024, 0, Owner);

        var next = new BufferSegment();
        next.RentFrom(MemoryPool<byte>.Shared, 1024, 256, Owner);

        seg.Freeze(bytesFilled: 256, next);

        Assert.Equal(256, seg.End);
        Assert.Equal(256, ((ReadOnlySequenceSegment<byte>)seg).Memory.Length);
        Assert.Same(next, seg.Next);
        Assert.Same(next, ((ReadOnlySequenceSegment<byte>)seg).Next);
    }

    [Fact]
    public void RecycleReset_RestoresFullMemoryAndPreservesOwner()
    {
        var seg = new BufferSegment();
        seg.RentFrom(MemoryPool<byte>.Shared, 1024, 0, Owner);
        int capacity = seg.AvailableMemory.Length;
        seg.Freeze(256, null);

        seg.RecycleReset(runningIndex: 999);

        Assert.Equal(0, seg.End);
        Assert.Null(seg.Next);
        Assert.Equal(999, seg.RunningIndex);
        Assert.Equal(capacity, ((ReadOnlySequenceSegment<byte>)seg).Memory.Length);
        Assert.Same(Owner, seg.OwnerToken);
    }

    [Fact]
    public void DisposeOwned_ReleasesMemoryOwner()
    {
        var seg = new BufferSegment();
        seg.RentFrom(MemoryPool<byte>.Shared, 1024, 0, Owner);

        seg.DisposeOwned();

        Assert.Equal(0, seg.AvailableMemory.Length);
        // Calling DisposeOwned again should be a no-op; not throw.
        seg.DisposeOwned();
    }

    [Fact]
    public void MemoryLayout_SlicePreservesObjectAndIndex_Assumption()
    {
        // Spec Nit-5: the head==tail safety argument relies on Slice(0, n) preserving
        // _object and _index of Memory<byte>. If a future BCL change breaks this, we
        // want to find out at boot time, not as a heisenbug.
        using var owner = MemoryPool<byte>.Shared.Rent(1024);
        var full = owner.Memory;
        var sliced = full.Slice(0, 100);

        // We can't access _object/_index directly, but we can verify functional equivalence:
        // sliced[0] should refer to the same byte as full[0].
        full.Span[0] = 0xAB;
        Assert.Equal(0xAB, sliced.Span[0]);

        // And length differs as expected.
        Assert.Equal(1024, full.Length);
        Assert.Equal(100, sliced.Length);
    }
}
