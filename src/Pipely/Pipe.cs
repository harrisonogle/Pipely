using System.Buffers;
using System.IO.Pipelines;
using System.Threading;

namespace Pipely;

public sealed partial class Pipe : IDisposable
{
    internal readonly System.IO.Pipelines.PipeOptions _options;
    internal readonly int _maxFreelistSegments;
    internal readonly TripleBuffer<WriterState> _writerTb = new();
    internal readonly TripleBuffer<ReaderState> _readerTb = new();
    internal readonly PipelyAwaiter<ReadResult>  _readAwaiter;
    internal readonly PipelyAwaiter<FlushResult> _flushAwaiter;

    // Writer-side cursors (writer thread only).
    internal BufferSegment? _chainHead;
    internal BufferSegment? _writingHead;
    internal int  _writingHeadBytesBuffered;
    internal long _totalWritten;
    internal BufferSegment? _freelistHead;
    internal int  _freelistCount;
    internal BufferSegment? _donatedShellFreelistHead;
    internal int  _donatedShellFreelistCount;
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
    // True when a ReadResult has been delivered to the user but not yet AdvanceTo'd.
    // Set on every ReadResult-delivery site (sync return + park SetResult); cleared in AdvanceTo.
    // Cross-thread sets (signaler, canceler) ride on _core's SetResult/await synchronization edge.
    internal bool _readPending;

    // Pipe-level (mutated by Dispose only).
    internal bool _disposed;

    private readonly PipeWriter _writerInstance;
    private readonly PipeReader _readerInstance;

    public Pipe() : this(PipeOptions.Default) { }

    public Pipe(PipeOptions options) : this(options, options.MaxFreelistSegments) { }

    public Pipe(System.IO.Pipelines.PipeOptions options, int? maxFreelistSegments = null)
        : this(
            options,
            maxFreelistSegments
                ?? (options as PipeOptions)?.MaxFreelistSegments
                ?? PipeOptions.Default.MaxFreelistSegments)
    { }

    private Pipe(System.IO.Pipelines.PipeOptions options, int maxFreelistSegments)
    {
        if (maxFreelistSegments < 0) throw new ArgumentOutOfRangeException(nameof(maxFreelistSegments));
        _options = options;
        _maxFreelistSegments = maxFreelistSegments;
        _readAwaiter    = new PipelyAwaiter<ReadResult>(options.ReaderScheduler, options.UseSynchronizationContext);
        _flushAwaiter   = new PipelyAwaiter<FlushResult>(options.WriterScheduler, options.UseSynchronizationContext);
        _writerInstance = new PipeWriter(this);
        _readerInstance = new PipeReader(this);
    }

    public PipeWriter Writer => _writerInstance;
    public PipeReader Reader => _readerInstance;

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        // R4-1: dispose leftover CTRs (idempotent on default).
        _readAwaiter._ctr.Dispose();
        _flushAwaiter._ctr.Dispose();

        // Walk the chain.
        var seg = _chainHead;
        while (seg != null)
        {
            var next = seg.Next;
            seg.DisposeOwned();
            seg = next;
        }
        _chainHead = null;
        _writingHead = null;

        // Walk the freelist.
        var fl = _freelistHead;
        while (fl != null)
        {
            var next = fl.Next;
            fl.DisposeOwned();
            fl = next;
        }
        _freelistHead = null;
        _freelistCount = 0;

        // Donated-shell freelist: shells have no IMemoryOwner (released in RecycleDrainedSegments
        // before pooling). Just clear the head and count; nothing to dispose.
        _donatedShellFreelistHead = null;
        _donatedShellFreelistCount = 0;
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
        if (_freelistCount >= _maxFreelistSegments)
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

    internal void PushDonatedShellFreelist(BufferSegment shell)
    {
        if (_donatedShellFreelistCount >= _maxFreelistSegments)
            return;     // cap exceeded; drop the shell to GC
        shell.SetFreelistNext(_donatedShellFreelistHead);
        _donatedShellFreelistHead = shell;
        _donatedShellFreelistCount++;
    }

    internal BufferSegment? PopDonatedShellFreelist()
    {
        var head = _donatedShellFreelistHead;
        if (head == null) return null;
        _donatedShellFreelistHead = head.Next;
        head.SetFreelistNext(null);   // detach from the freelist link
        _donatedShellFreelistCount--;
        return head;
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
            if ((oldV & PipelyAwaiter<ReadResult>.StateMask) != PipelyAwaiter<ReadResult>.Pending) return;
            int desired = oldV & ~PipelyAwaiter<ReadResult>.StateMask;
            if (Interlocked.CompareExchange(ref _readAwaiter._state, desired, oldV) == oldV)
            {
                Interlocked.Increment(ref _readAwaiter._signalWonCount);
                _readAwaiter._ctr.Dispose();

                // Pattern 2: construct ReadResult from stash + just-published WriterState.
                var w = _lastPublishedWriterState;

                // Throw-first: writer-completed-with-ex delivered as exception.
                if (w.IsCompleted && w.CompletionException != null)
                {
                    _readAwaiter._core.SetException(w.CompletionException);
                    return;
                }

                var head    = _readAwaiter._stashHead ?? w.HeadSegment;     // bootstrap fallback
                var headIdx = _readAwaiter._stashHead == null ? 0 : _readAwaiter._stashHeadIdx;

                var buffer = head == null
                    ? ReadOnlySequence<byte>.Empty
                    : new ReadOnlySequence<byte>(head, headIdx, w.TailSegment!, w.TailWritten);

                _readPending = true;
                _readAwaiter._core.SetResult(new ReadResult(buffer, isCanceled: false, isCompleted: w.IsCompleted));
                return;
            }
        }
    }

    internal void OnReadAwaiterTokenCancel()
    {
        while (true)
        {
            int oldV = _readAwaiter._state;
            if ((oldV & PipelyAwaiter<ReadResult>.StateMask) != PipelyAwaiter<ReadResult>.Pending) return;
            int desired = oldV & ~PipelyAwaiter<ReadResult>.StateMask;
            if (Interlocked.CompareExchange(ref _readAwaiter._state, desired, oldV) == oldV)
            {
                Interlocked.Increment(ref _readAwaiter._tokenCancelWonCount);
                _readAwaiter._core.SetException(new OperationCanceledException(_readAwaiter._token));
                return;
            }
        }
    }

    internal void OnFlushAwaiterTokenCancel()
    {
        while (true)
        {
            int oldV = _flushAwaiter._state;
            if ((oldV & PipelyAwaiter<FlushResult>.StateMask) != PipelyAwaiter<FlushResult>.Pending) return;
            int desired = oldV & ~PipelyAwaiter<FlushResult>.StateMask;
            if (Interlocked.CompareExchange(ref _flushAwaiter._state, desired, oldV) == oldV)
            {
                Interlocked.Increment(ref _flushAwaiter._tokenCancelWonCount);
                _flushAwaiter._core.SetException(new OperationCanceledException(_flushAwaiter._token));
                return;
            }
        }
    }

    internal void PublishReaderState()
    {
        var snapshot = new ReaderState
        {
            HeadSegment         = _readHead,
            TotalConsumed       = _totalConsumed,
            TotalExamined       = _totalExamined,
            IsCompleted         = false,
            CompletionException = null,
        };
        _readerTb.ProducerSlot() = snapshot;
        _readerTb.Publish();
        _lastPublishedReaderState = snapshot;

        SignalFlushIfBackpressureRelieved();
    }

    // R2-1: gated signaler — only wakes the parked writer when backpressure has relieved.
    // Called from AdvanceTo. The writer cannot re-check the wake condition after _core.SetResult,
    // so signal-side gating is required (not optional).
    //
    // Side effect: the inner TryAcquire mutates _readTail/_readTailIdx via IntegrateAcquiredWriterState.
    // Benign — keeps reader's view of the writer's tail fresh as a no-op-or-better.
    internal void SignalFlushIfBackpressureRelieved()
    {
        // Fast path: no parked writer.
        if ((_flushAwaiter._state & PipelyAwaiter<FlushResult>.StateMask) != PipelyAwaiter<FlushResult>.Pending) return;

        // Refresh writer state to compute unconsumed accurately.
        if (_writerTb.TryAcquire())
        {
            _lastAcquiredWriterState = _writerTb.ConsumerSlot();
            IntegrateAcquiredWriterState();
        }

        long unconsumed = _lastAcquiredWriterState.TotalWritten - _totalConsumed;
        // Note: no `|| _readerCompleted` clause — AdvanceTo's entry guard throws if _readerCompleted,
        // so this code path never runs post-completion. Reader.Complete uses SignalFlushAwaiterIfPending (unconditional).
        if (unconsumed >= _options.ResumeWriterThreshold) return;

        while (true)
        {
            int oldV = _flushAwaiter._state;
            if ((oldV & PipelyAwaiter<FlushResult>.StateMask) != PipelyAwaiter<FlushResult>.Pending) return;
            int desired = oldV & ~PipelyAwaiter<FlushResult>.StateMask;
            if (Interlocked.CompareExchange(ref _flushAwaiter._state, desired, oldV) == oldV)
            {
                Interlocked.Increment(ref _flushAwaiter._signalWonCount);
                _flushAwaiter._ctr.Dispose();
                DeliverFlushResult();
                return;
            }
        }
    }

    // Unconditional signaler — used by Reader.Complete only (completion is always a wake reason).
    internal void SignalFlushAwaiterIfPending()
    {
        while (true)
        {
            int oldV = _flushAwaiter._state;
            if ((oldV & PipelyAwaiter<FlushResult>.StateMask) != PipelyAwaiter<FlushResult>.Pending) return;
            int desired = oldV & ~PipelyAwaiter<FlushResult>.StateMask;
            if (Interlocked.CompareExchange(ref _flushAwaiter._state, desired, oldV) == oldV)
            {
                Interlocked.Increment(ref _flushAwaiter._signalWonCount);
                _flushAwaiter._ctr.Dispose();
                DeliverFlushResult();
                return;
            }
        }
    }

    private void DeliverFlushResult()
    {
        var r = _lastPublishedReaderState;
        if (r.IsCompleted && r.CompletionException != null)
            _flushAwaiter._core.SetException(r.CompletionException);
        else
            _flushAwaiter._core.SetResult(new FlushResult(isCanceled: false, isCompleted: r.IsCompleted));
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

            if (recycled.IsDonated)
            {
                recycled.DisposeOwned();              // foreign owner: release the IMemoryOwner
                PushDonatedShellFreelist(recycled);   // shell pooled for re-use; over-cap drops to GC
            }
            else
            {
                PushFreelist(recycled);               // pool-rented: existing freelist path (with cap-overflow handling)
            }
        }
    }
}
