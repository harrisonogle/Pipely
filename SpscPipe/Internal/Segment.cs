using System.Buffers;

namespace SpscPipe.Internal;

// §4.1 Segment — a node in the published/unpublished singly-linked list.
//
// Inherits from ReadOnlySequenceSegment<byte> so that ReadOnlySequence<byte>
// enumerators (the external consumer's view) can walk from a start segment
// to an end segment via the base class Next property.
//
// Internal traversal (§7.2) reads the successor via _next with
// Volatile.Read/Volatile.Write for release/acquire semantics.  base.Next
// is kept in sync so external consumers see the same linkage.  We can't
// Volatile.Write directly to the base property because it has no
// addressable backing field we can pass by ref.
internal sealed class Segment : ReadOnlySequenceSegment<byte>
{
    // Shared buffer container; null after retirement.
    internal BufferHolder? Holder;

    // Offset within Holder.Owner.Memory where this segment's bytes start.
    internal int BufferStart;

    // Bytes written into this segment.  Plain int; set during
    // AppendActiveSegmentToUnpublished before publication.
    internal int WrittenLength;

    // Internal Next pointer; accessed via Volatile.Read/Write in checkpoint 2
    // (§7.2 acquire, §6.4.2 release-store).  base.Next is kept in sync for
    // ReadOnlySequence<byte> external consumers.
    internal Segment? _next;

    // Resets all fields for pool return.  Called from SegmentPool.Return.
    internal void Reset()
    {
        Holder = null;
        BufferStart = 0;
        WrittenLength = 0;
        _next = null;
        Memory = default;
        RunningIndex = 0;
        base.Next = null;
    }
}
