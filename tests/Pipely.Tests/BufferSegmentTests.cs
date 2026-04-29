using System.Buffers;
using Xunit;

namespace Pipely.Tests;

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

    [Fact]
    public void RentFrom_SetsIsDonatedFalse()
    {
        var seg = new BufferSegment();
        seg.RentFrom(MemoryPool<byte>.Shared, 1024, 0, Owner);
        Assert.False(seg.IsDonated);
    }

    [Fact]
    public void AdoptFrom_SetsFieldsCorrectly()
    {
        var owner = new TrackingMemoryOwner(1024);
        var slice = owner.Memory.Slice(64, 256);

        var seg = new BufferSegment();
        seg.AdoptFrom(owner, slice, runningIndex: 999, pipeOwner: Owner);

        Assert.Equal(256, seg.AvailableMemory.Length);
        Assert.Equal(256, ((ReadOnlySequenceSegment<byte>)seg).Memory.Length);
        Assert.Equal(256, seg.End);
        Assert.Null(seg.Next);
        Assert.Null(((ReadOnlySequenceSegment<byte>)seg).Next);
        Assert.Equal(999, seg.RunningIndex);
        Assert.Same(Owner, seg.OwnerToken);
        Assert.True(seg.IsDonated);
    }

    [Fact]
    public void AdoptFrom_ThenDisposeOwned_DisposesOriginalOwner()
    {
        var owner = new TrackingMemoryOwner(1024);
        var slice = owner.Memory;
        var seg = new BufferSegment();
        seg.AdoptFrom(owner, slice, runningIndex: 0, pipeOwner: Owner);

        Assert.Equal(0, owner.DisposeCount);
        seg.DisposeOwned();
        Assert.Equal(1, owner.DisposeCount);

        // DisposeOwned remains idempotent.
        seg.DisposeOwned();
        Assert.Equal(1, owner.DisposeCount);
    }

    [Fact]
    public void AdoptFrom_PreservesSliceOffsetIntoOwner()
    {
        var bytes = new byte[1024];
        for (int i = 0; i < bytes.Length; i++) bytes[i] = (byte)(i & 0xFF);
        var owner = new TrackingMemoryOwner(bytes);
        var slice = owner.Memory.Slice(100, 50);

        var seg = new BufferSegment();
        seg.AdoptFrom(owner, slice, runningIndex: 0, pipeOwner: Owner);

        // The donated segment's Memory should reference bytes 100..149 of the underlying array.
        Assert.Equal(50, seg.AvailableMemory.Length);
        Assert.Equal((byte)100, seg.AvailableMemory.Span[0]);
        Assert.Equal((byte)149, seg.AvailableMemory.Span[49]);
    }

    [Fact]
    public void Freeze_OnDonatedSegment_IsIdempotentOnEndAndMemory()
    {
        // Pin the documented behavior in spec §2.2: when an Splice splices a new tail past
        // a previously-donated _writingHead, the call _writingHead.Freeze(filled, next)
        // re-writes End/base.Memory to the same values they already held; only Next changes.
        var owner = new TrackingMemoryOwner(64);
        var seg   = new BufferSegment();
        seg.AdoptFrom(owner, owner.Memory, runningIndex: 0, pipeOwner: Owner);

        int endBefore       = seg.End;
        int memLenBefore    = ((ReadOnlySequenceSegment<byte>)seg).Memory.Length;

        var next = new BufferSegment();
        next.RentFrom(MemoryPool<byte>.Shared, 32, runningIndex: 64, owner: Owner);

        seg.Freeze(bytesFilled: seg.End, next);

        Assert.Equal(endBefore,    seg.End);
        Assert.Equal(memLenBefore, ((ReadOnlySequenceSegment<byte>)seg).Memory.Length);
        Assert.Same(next,          seg.Next);
    }
}
