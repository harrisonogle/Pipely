using System.Buffers;

namespace SpscPipelines;

internal sealed class BufferSegment : ReadOnlySequenceSegment<byte>
{
    private IMemoryOwner<byte>? _memoryOwner;

    public Memory<byte> AvailableMemory { get; private set; }
    public int End { get; private set; }
    public new BufferSegment? Next { get; private set; }
    public object? OwnerToken { get; private set; }   // R4-7
    public bool    IsDonated  { get; private set; }   // recycle-path discriminator (donated vs rented)

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
        IsDonated         = false;
    }

    // Adopts a foreign IMemoryOwner<byte> for buffer-ownership transfer (Splice).
    // Caller passes a pre-computed slice of `owner.Memory` so this method is allocation- and throw-free.
    // Preconditions (caller-validated):
    //   owner != null, slice.Length > 0, slice originates from owner.Memory.
    public void AdoptFrom(IMemoryOwner<byte> owner, Memory<byte> slice, long runningIndex, object pipeOwner)
    {
        _memoryOwner      = owner;
        AvailableMemory   = slice;
        base.Memory       = slice;
        base.RunningIndex = runningIndex;
        End               = slice.Length;
        Next              = null;
        base.Next         = null;
        OwnerToken        = pipeOwner;
        IsDonated         = true;
    }

    public void Freeze(int bytesFilled, BufferSegment? next)
    {
        End         = bytesFilled;
        base.Memory = AvailableMemory.Slice(0, bytesFilled);
        Next        = next;
        base.Next   = next;
        // IsDonated not touched: Freeze on a donated segment writes End/base.Memory to the
        // same values they already hold (AdoptFrom sets End == slice.Length immediately).
        // Only Next materially changes. See spec §2.2.
    }

    public void RecycleReset(long runningIndex)
    {
        System.Diagnostics.Debug.Assert(!IsDonated, "RecycleReset must not be called on donated segments.");
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
