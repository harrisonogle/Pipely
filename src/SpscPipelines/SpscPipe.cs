using System.Buffers;
using System.IO.Pipelines;
using System.Threading;

namespace SpscPipelines;

public sealed partial class SpscPipe : IDisposable
{
    internal readonly SpscPipeOptions _options;
    internal readonly TripleBuffer<WriterState> _writerTb = new();
    internal readonly TripleBuffer<ReaderState> _readerTb = new();
    internal readonly SpscAwaiter<ReadResult>  _readAwaiter  = new();
    internal readonly SpscAwaiter<FlushResult> _flushAwaiter = new();

    // Writer-side cursors (writer thread only).
    internal BufferSegment? _chainHead;
    internal BufferSegment? _writingHead;
    internal int  _writingHeadBytesBuffered;
    internal long _totalWritten;
    internal BufferSegment? _freelistHead;
    internal int  _freelistCount;
    internal WriterState _lastPublishedWriterState;
    internal ReaderState _lastAcquiredReaderState;
    internal bool _writerCompleted;

    // Reader-side cursors (reader thread only).
    internal BufferSegment? _readHead;
    internal int _readHeadIdx;
    internal BufferSegment? _readTail;
    internal int _readTailIdx;
    internal long _totalConsumed;
    internal long _totalExamined;
    internal ReaderState _lastPublishedReaderState;
    internal WriterState _lastAcquiredWriterState;
    internal bool _readerCompleted;

    // Pipe-level (mutated by Dispose only).
    internal bool _disposed;

    private readonly SpscPipeWriter _writerInstance;
    private readonly SpscPipeReader _readerInstance;

    public SpscPipe() : this(SpscPipeOptions.Default) { }
    public SpscPipe(SpscPipeOptions options)
    {
        _options        = options;
        _writerInstance = new SpscPipeWriter(this);
        _readerInstance = new SpscPipeReader(this);
    }

    public PipeWriter Writer => _writerInstance;
    public PipeReader Reader => _readerInstance;

    public void Dispose()
    {
        // Full implementation in Task 10.
        if (_disposed) return;
        _disposed = true;
    }

    internal BufferSegment RentSegment(int sizeHint, long runningIndex)
    {
        var s = PopFreelist(minSize: sizeHint);
        if (s != null)
        {
            s.RecycleReset(runningIndex);
            return s;
        }
        s = new BufferSegment();
        s.RentFrom(_options.Pool, Math.Max(sizeHint, _options.MinimumSegmentSize), runningIndex, owner: this);
        return s;
    }

    private BufferSegment? PopFreelist(int minSize)
    {
        var head = _freelistHead;
        if (head == null) return null;
        if (head.AvailableMemory.Length < minSize)
        {
            // Drop and dispose; per Spec §3 N5 (avoid stranding small segments).
            _freelistHead = head.Next;
            head.DisposeOwned();
            _freelistCount--;
            return null;
        }
        _freelistHead = head.Next;
        _freelistCount--;
        // Don't RecycleReset here — RentSegment does it with the correct runningIndex,
        // which also clears the freelist-link Next set by PushFreelist.
        return head;
    }

    internal void PushFreelist(BufferSegment s)
    {
        if (_freelistCount >= _options.MaxFreelistSegments)
        {
            s.DisposeOwned();
            return;
        }
        // Reset to clean state, then link into the freelist via SetFreelistNext.
        s.RecycleReset(runningIndex: 0);
        s.SetFreelistNext(_freelistHead);
        _freelistHead = s;
        _freelistCount++;
    }

    internal bool HasReadableProgress() => _lastAcquiredWriterState.TotalWritten > _totalExamined;

    internal void IntegrateAcquiredWriterState()
    {
        var w = _lastAcquiredWriterState;
        if (_readHead == null)                  // I10 bootstrap
        {
            _readHead    = w.HeadSegment;
            _readHeadIdx = 0;
        }
        _readTail    = w.TailSegment;
        _readTailIdx = w.TailWritten;
    }

    internal ReadResult BuildReadResult(bool isCanceled)
    {
        bool isCompleted = _lastAcquiredWriterState.IsCompleted;
        var buffer = _readHead == null
            ? ReadOnlySequence<byte>.Empty
            : new ReadOnlySequence<byte>(_readHead, _readHeadIdx, _readTail!, _readTailIdx);
        return new ReadResult(buffer, isCanceled, isCompleted);
    }

    internal FlushResult BuildFlushResult(bool isCanceled)
        => new(isCanceled, isCompleted: _lastAcquiredReaderState.IsCompleted);

    internal void SignalReadAwaiterIfPending()
    {
        while (true)
        {
            int oldV = _readAwaiter._state;
            if ((oldV & SpscAwaiter<ReadResult>.StateMask) != SpscAwaiter<ReadResult>.Pending) return;
            int desired = oldV & ~SpscAwaiter<ReadResult>.StateMask;
            if (Interlocked.CompareExchange(ref _readAwaiter._state, desired, oldV) == oldV)
            {
                _readAwaiter._ctr.Dispose();
                // Pattern 2 construction happens here in Task 8. For now, deliver default.
                // This intermediate behavior won't be exposed to users until ReadAsync is wired (Task 6),
                // and the parking path is wired (Task 8). The sync fast path doesn't reach here.
                _readAwaiter._core.SetResult(default);
                return;
            }
        }
    }

    internal void RecycleDrainedSegments()
    {
        var r = _lastAcquiredReaderState;
        if (r.HeadSegment is null && !r.IsCompleted) return;       // pre-bootstrap

        var readerHead = r.HeadSegment;

        while (_chainHead != _writingHead && _chainHead != readerHead)
        {
            var recycled = _chainHead!;
            _chainHead   = recycled.Next!;
            PushFreelist(recycled);
        }
    }
}
