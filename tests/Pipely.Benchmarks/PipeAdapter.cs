using System.IO.Pipelines;

namespace Pipely.Benchmarks;

internal sealed record AwaiterCounters(
    long ParkCount,
    long SignalWonCount,
    long TokenCancelWonCount,
    long CancelPendingWonCount,
    long LostWakeupResolvedCount,
    long LostCancelResolvedCount
);

internal sealed class PipeAdapter : IPipeAdapter
{
    private readonly Pipely.Pipe _pipe;
    public PipeAdapter(Pipely.PipeOptions? options = null) => _pipe = new Pipely.Pipe(options ?? Pipely.PipeOptions.Default);
    public PipeReader Reader => _pipe.Reader;
    public PipeWriter Writer => _pipe.Writer;
    public void Dispose() => _pipe.Dispose();

    // Snapshot of read-awaiter counters (read after Run completes — no concurrent writers,
    // so plain reads of the long fields are sufficient on 64-bit platforms).
    public AwaiterCounters GetReadAwaiterCounters() => new(
        ParkCount:               _pipe._readAwaiter._parkCount,
        SignalWonCount:          _pipe._readAwaiter._signalWonCount,
        TokenCancelWonCount:     _pipe._readAwaiter._tokenCancelWonCount,
        CancelPendingWonCount:   _pipe._readAwaiter._cancelPendingWonCount,
        LostWakeupResolvedCount: _pipe._readAwaiter._lostWakeupResolvedCount,
        LostCancelResolvedCount: _pipe._readAwaiter._lostCancelResolvedCount);

    public AwaiterCounters GetFlushAwaiterCounters() => new(
        ParkCount:               _pipe._flushAwaiter._parkCount,
        SignalWonCount:          _pipe._flushAwaiter._signalWonCount,
        TokenCancelWonCount:     _pipe._flushAwaiter._tokenCancelWonCount,
        CancelPendingWonCount:   _pipe._flushAwaiter._cancelPendingWonCount,
        LostWakeupResolvedCount: _pipe._flushAwaiter._lostWakeupResolvedCount,
        LostCancelResolvedCount: _pipe._flushAwaiter._lostCancelResolvedCount);
}
