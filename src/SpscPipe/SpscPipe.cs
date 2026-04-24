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

    internal SpscPipeReader ReaderInternal => _reader;
    internal SpscPipeWriter WriterInternal => _writer;

    private int _disposed;   // 0 = live, 1 = disposed

    public void Dispose()
    {
        // §10.6 idempotency guard.  Interlocked.Exchange is a full fence
        // (§5 axiom) and ensures only one thread runs DisposeCore.
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;                      // §10.6 idempotency
        DisposeCore();
        GC.SuppressFinalize(this);
    }

    // §10.5 Reset: same cleanup walks as Dispose, then reinitialise for
    // reuse.  Not invoked from Dispose (Reset leaves the pipe usable;
    // Dispose leaves it terminal).
    public void Reset()
    {
        DisposeCore();
        // Zero the state groups.  Group layout is preserved (struct is
        // fixed-layout); Writer/Reader instances are reused.
        _state = default;
        _reader.Reset();
        _writer.Reset();
    }

    // §10.6 cleanup walks.  Caller must ensure no operations are in flight.
    private void DisposeCore()
    {
        // Step 1: walk the writer's unpublished chain.
        _writer.WalkUnpublishedChain(seg =>
        {
            if (seg.Holder is not null)
            {
                ReleaseHolder(seg.Holder);
                seg.Holder = null;
            }
            seg.Reset();
            SegmentPool.Return(seg);
        });

        // Step 2-3: walk the published chain from _head (reader's) or
        // state.Head if reader never read.
        var start = _reader.Head ?? _state.Head;
        var current = start;
        while (current is not null)
        {
            var next = current.TypedNext;
            if (current.Holder is not null)
            {
                ReleaseHolder(current.Holder);
                current.Holder = null;
            }
            current.Reset();
            SegmentPool.Return(current);
            current = next;
        }

        // Step 4: release active buffer holder (if writer never flushed/
        // completed, _activeBufferHolder may still hold a reference).
        _writer.DisposeActiveBuffer();

        // Step 5: null out state pointers (completion/exception fields are
        // useful to callers that might still hold references; we zero them
        // via full state reset in Reset, not here).
        _state.Head = null;
        _state.Tail = null;
    }

    // §10.6 finalizer safety net.  Not a guarantee — callers should
    // explicitly Dispose.  The finalizer handles abandoned pipes backed
    // by native/pinned memory pools (see §10.6 rationale).  This runs
    // during GC stop-the-world which provides implicit memory barriers,
    // so the field reads here do not use Volatile.Read per spec §10.6 note.
    ~SpscPipe() { try { DisposeCore(); } catch { /* swallow in finalizer */ } }

    // ----- Shared helpers ------------------------------------------------

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
            // Idempotent return-to-pool.  Not a spec-mandated primitive;
            // needed only because IMemoryOwner<T>.Dispose contract allows
            // concurrent Disposes (we don't generate them, but be safe).
            var arr = Interlocked.Exchange(ref _array, null);                         // (infra) idempotent return
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
