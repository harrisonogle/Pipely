using System.Buffers;

namespace SpscPipe.Internal;

// §6.5.1 BufferHolder — shared refcounted container for a rented buffer.
//
// Multiple segments may reference the same buffer (after buffer rotation
// within an unpublished chain, small flushes share the backing buffer
// across several segment objects).  The buffer returns to the pool when
// the last segment referencing it retires AND the writer has released
// its own reference.  See §6.5 for the full refcount protocol.
//
// Refcount is incremented/decremented via Interlocked on both sides
// (writer at segment creation; reader at segment retirement; writer at
// buffer rotation).  Interlocked on a segment-boundary cadence is
// acceptable; it does not appear on the per-byte hot path.
internal sealed class BufferHolder
{
    internal IMemoryOwner<byte>? Owner;
    internal int Refcount;
}
