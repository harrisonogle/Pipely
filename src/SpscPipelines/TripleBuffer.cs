using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace SpscPipelines;

public struct Snapshot
{
    public double Bid;
    public double Ask;
    public long TimestampTicks;
    public long Symbol;
}

public sealed class TripleBuffer
{
    [StructLayout(LayoutKind.Explicit, Size = 192)]
    private struct PaddedSlot
    {
        [FieldOffset(0)] public Snapshot Data;
    }

    private PaddedSlot _slot0;
    private PaddedSlot _slot1;
    private PaddedSlot _slot2;

    // State word: bit 0 = dirty, bits 1-2 = published slot index.
    [StructLayout(LayoutKind.Explicit, Size = 128)]
    private struct PaddedInt
    {
        [FieldOffset(0)] public int Value;
    }
    private PaddedInt _state;

    [StructLayout(LayoutKind.Explicit, Size = 128)]
    private struct ProducerState
    {
        [FieldOffset(0)] public int OwnedIndex;
    }
    private ProducerState _producer;

    [StructLayout(LayoutKind.Explicit, Size = 128)]
    private struct ConsumerState
    {
        [FieldOffset(0)] public int OwnedIndex;
    }
    private ConsumerState _consumer;

    public TripleBuffer()
    {
        _producer.OwnedIndex = 0;
        _consumer.OwnedIndex = 2;
        _state.Value = 1 << 1;  // slot 1 published, dirty = 0
    }

    public ref Snapshot ProducerSlot() => ref GetSlot(_producer.OwnedIndex);

    public void Publish()
    {
        int newlyPublished = _producer.OwnedIndex;
        int newState = (newlyPublished << 1) | 1;  // set dirty
        int oldState = Interlocked.Exchange(ref _state.Value, newState);
        _producer.OwnedIndex = (oldState >> 1) & 0b11;
    }

    public bool TryAcquire()
    {
        int current = Volatile.Read(ref _state.Value);
        if ((current & 1) == 0)
            return false;  // not dirty

        int ours = _consumer.OwnedIndex << 1;  // dirty = 0
        int acquired = Interlocked.Exchange(ref _state.Value, ours);
        _consumer.OwnedIndex = (acquired >> 1) & 0b11;
        return true;
    }

    public ref Snapshot ConsumerSlot() => ref GetSlot(_consumer.OwnedIndex);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ref Snapshot GetSlot(int index)
    {
        switch (index)
        {
            case 0: return ref _slot0.Data;
            case 1: return ref _slot1.Data;
            default: return ref _slot2.Data;
        }
    }
}