using System.Buffers;
using System.IO.Pipelines;

namespace SpscPipe;

// Public entry point.  Spec §3.
public sealed class SpscPipe : IDisposable
{
    private readonly SpscPipeReader _reader;
    private readonly SpscPipeWriter _writer;

    // Mutable shared state.  Padded per §4.2.
    internal State _state;

    // Pools.  Per §9.
    internal readonly MemoryPool<byte> BufferPool;
    internal readonly DefaultObjectPool<Segment> SegmentPool;
    internal readonly DefaultObjectPool<BufferHolder> HolderPool;

    internal readonly SpscPipeOptions Options;

    public SpscPipe() : this(new SpscPipeOptions()) { }

    public SpscPipe(SpscPipeOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        Options = options;
        BufferPool = options.Pool ?? new ArrayMemoryPool();
        SegmentPool = new DefaultObjectPool<Segment>();
        HolderPool = new DefaultObjectPool<BufferHolder>();
        _reader = new SpscPipeReader(this);
        _writer = new SpscPipeWriter(this);
    }

    public PipeReader Reader => _reader;
    public PipeWriter Writer => _writer;

    public void Reset() => throw new NotImplementedException("§10.5");

    // §10.6 cleanup implemented in checkpoint 4.  For checkpoints 2-3
    // Dispose is a no-op; this lets `using` patterns in tests work
    // without triggering NotImplementedException before the lifecycle
    // checkpoint is reached.
    public void Dispose() { /* §10.6 — checkpoint 4 */ }

    // ----- Shared helpers ------------------------------------------------

    // §6.5.2 ReleaseHolder.  Called from both writer (on buffer rotation +
    // Complete) and reader (on RetireSegment).  Interlocked.Decrement is a
    // full fence by the §5 axiom; its ordering with prior reads/writes on
    // each side is argued in §6.5.3.
    internal void ReleaseHolder(BufferHolder holder)
    {
        if (Interlocked.Decrement(ref holder.Refcount) == 0)   // §6.5.2 refcount-; §6.5.3 full fence
        {
            holder.Owner?.Dispose();
            holder.Owner = null;
            HolderPool.Return(holder);
        }
    }
}

// Adapts ArrayPool<byte>.Shared to the MemoryPool<byte> API.
internal sealed class ArrayMemoryPool : MemoryPool<byte>
{
    public override int MaxBufferSize => int.MaxValue;

    public override IMemoryOwner<byte> Rent(int minBufferSize = -1)
    {
        var size = minBufferSize < 0 ? 4096 : minBufferSize;
        return new ArrayMemoryOwner(ArrayPool<byte>.Shared.Rent(size));
    }

    protected override void Dispose(bool disposing) { }

    private sealed class ArrayMemoryOwner : IMemoryOwner<byte>
    {
        private byte[]? _array;
        internal ArrayMemoryOwner(byte[] array) => _array = array;
        public Memory<byte> Memory => _array ?? Array.Empty<byte>();
        public void Dispose()
        {
            var arr = Interlocked.Exchange(ref _array, null);
            if (arr is not null) ArrayPool<byte>.Shared.Return(arr);
        }
    }
}

// Minimal lock-backed object pool.  Sufficient for the v1 contract
// (§9: "a bounded concurrent pool").  Can be swapped for
// Microsoft.Extensions.ObjectPool later if profiles demand it.
internal sealed class DefaultObjectPool<T> where T : class, new()
{
    private readonly object _gate = new();
    private readonly Stack<T> _items = new();

    internal T Rent()
    {
        lock (_gate)
        {
            if (_items.Count > 0) return _items.Pop();
        }
        return new T();
    }

    internal void Return(T item)
    {
        lock (_gate)
        {
            _items.Push(item);
        }
    }
}
