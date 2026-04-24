namespace SpscPipe;

// Per spec §4.4.  Signal carried on the reader's _readAwaiter (MRVTS).
// Only IsCanceled is needed; the reader re-executes the sync path for
// non-canceled signals to obtain the real ReadResult from reader-local
// state (§8.7).
internal readonly struct ReadSignal
{
    internal bool IsCanceled { get; init; }
}
