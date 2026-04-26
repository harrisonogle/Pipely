using System.Buffers;

namespace SpscPipelines;

internal sealed class BufferSegment : ReadOnlySequenceSegment<byte>
{
    private IMemoryOwner<byte>? _memoryOwner;

    public Memory<byte> AvailableMemory { get; private set; }
    public int End { get; private set; }
    public new BufferSegment? Next { get; private set; }
    public object? OwnerToken { get; private set; }   // R4-7

    public void RentFrom(MemoryPool<byte> pool, int sizeHint, long runningIndex, object owner)
    {
        _memoryOwner      = pool.Rent(sizeHint);
        AvailableMemory   = _memoryOwner.Memory;
        base.Memory       = AvailableMemory;
        base.RunningIndex = runningIndex;
        End               = 0;
        Next              = null;
        base.Next         = null;
        OwnerToken        = owner;
    }

    public void Freeze(int bytesFilled, BufferSegment? next)
    {
        End         = bytesFilled;
        base.Memory = AvailableMemory.Slice(0, bytesFilled);
        Next        = next;
        base.Next   = next;
    }

    public void RecycleReset(long runningIndex)
    {
        base.RunningIndex = runningIndex;
        base.Memory       = AvailableMemory;
        End               = 0;
        Next              = null;
        base.Next         = null;
        // OwnerToken intentionally retained — segment stays bound to the same pipe across recycles.
    }

    public void DisposeOwned()
    {
        _memoryOwner?.Dispose();
        _memoryOwner    = null;
        AvailableMemory = default;
    }

    // Used by SpscPipe's freelist to link recycled segments without affecting End/Memory.
    // (Avoids overloading Freeze/RecycleReset for the freelist-link use case.)
    public void SetFreelistNext(BufferSegment? next)
    {
        Next      = next;
        base.Next = next;
    }
}
