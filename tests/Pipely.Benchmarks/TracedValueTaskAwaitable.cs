using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Pipely.Benchmarks;

// Wraps a ValueTask<T> so we can measure the time from "continuation scheduled"
// (OnCompleted invoked) to "continuation actually resumed" — the runtime's wake-gap
// for this specific await. Captured deltas are written into the caller-provided
// callback. Use only on the async branch (where IsCompleted is already false at
// the wrap site); sync awaits don't register a continuation and have no wake gap.
//
// Allocation cost: one closure per use (captures the Stopwatch start-tick + the
// wakeup callback). Significant under heavy use; acceptable for diagnostic runs.
internal readonly struct TracedValueTaskAwaitable<T>
{
    private readonly ValueTask<T> _task;
    private readonly Action<long> _recordWakeGap;

    public TracedValueTaskAwaitable(ValueTask<T> task, Action<long> recordWakeGap)
    {
        _task = task;
        _recordWakeGap = recordWakeGap;
    }

    public TracedValueTaskAwaiter<T> GetAwaiter() => new(_task.GetAwaiter(), _recordWakeGap);
}

internal readonly struct TracedValueTaskAwaiter<T> : ICriticalNotifyCompletion
{
    private readonly ValueTaskAwaiter<T> _inner;
    private readonly Action<long> _recordWakeGap;

    public TracedValueTaskAwaiter(ValueTaskAwaiter<T> inner, Action<long> recordWakeGap)
    {
        _inner = inner;
        _recordWakeGap = recordWakeGap;
    }

    public bool IsCompleted => _inner.IsCompleted;

    public T GetResult() => _inner.GetResult();

    public void OnCompleted(Action continuation)
    {
        long scheduledAt = Stopwatch.GetTimestamp();
        var record = _recordWakeGap;
        _inner.OnCompleted(() =>
        {
            record(Stopwatch.GetTimestamp() - scheduledAt);
            continuation();
        });
    }

    public void UnsafeOnCompleted(Action continuation)
    {
        long scheduledAt = Stopwatch.GetTimestamp();
        var record = _recordWakeGap;
        _inner.UnsafeOnCompleted(() =>
        {
            record(Stopwatch.GetTimestamp() - scheduledAt);
            continuation();
        });
    }
}
