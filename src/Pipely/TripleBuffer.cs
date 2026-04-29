using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Pipely;

public sealed class TripleBuffer<T> where T : struct
{
    private const int CacheLineSize = 128;  // x86-64 / Graviton; use 128 if targeting Apple Silicon production
    
    [InlineArray(CacheLineSize)]
    private struct CacheLinePad { private byte _b; }
    
    [StructLayout(LayoutKind.Sequential)]
    private struct PaddedSlot
    {
        public T Data;
        private CacheLinePad _pad;
    }
    
    private PaddedSlot _slot0;
    private PaddedSlot _slot1;
    private PaddedSlot _slot2;
    
    [StructLayout(LayoutKind.Sequential)]
    private struct PaddedInt
    {
        public int Value;
        private CacheLinePad _padAfter;
    }
    
#pragma warning disable CS0169
    private CacheLinePad _padBeforeState;
#pragma warning restore CS0169
    private PaddedInt _state;
    private PaddedInt _producer;     // OwnedIndex packed in Value
    private PaddedInt _consumer;     // OwnedIndex packed in Value
    
    public TripleBuffer()
    {
        Debug.Assert(Unsafe.SizeOf<T>() > 0);
        // Initial state: slot 1 published, dirty = 0
        _state.Value = 1 << 1;
        _producer.Value = 0;
        _consumer.Value = 2;
    }
    
    public ref T ProducerSlot() => ref GetSlot(_producer.Value);
    
    public void Publish()
    {
        int newlyPublished = _producer.Value;
        int newState = (newlyPublished << 1) | 1;
        int oldState = Interlocked.Exchange(ref _state.Value, newState);
        _producer.Value = (oldState >> 1) & 0b11;
    }
    
    public bool TryAcquire()
    {
        int current = Volatile.Read(ref _state.Value);
        if ((current & 1) == 0) return false;
        
        int ours = _consumer.Value << 1;
        int acquired = Interlocked.Exchange(ref _state.Value, ours);
        _consumer.Value = (acquired >> 1) & 0b11;
        return true;
    }
    
    public ref T ConsumerSlot() => ref GetSlot(_consumer.Value);
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ref T GetSlot(int index)
    {
        switch (index)
        {
            case 0: return ref _slot0.Data;
            case 1: return ref _slot1.Data;
            default: return ref _slot2.Data;
        }
    }
}