using System.Buffers;

namespace SpscPipe;

// Per spec §6.5.1.  Shared buffer container with refcount; segment +
// writer hold references.  Refcount is Interlocked on both sides
// (§6.5.2, §6.5.3).
internal sealed class BufferHolder
{
    internal IMemoryOwner<byte>? Owner;
    internal int Refcount;
}
