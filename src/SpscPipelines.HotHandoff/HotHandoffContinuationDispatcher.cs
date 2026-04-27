using SpscPipelines;

namespace SpscPipelines.HotHandoff;

public sealed class HotHandoffContinuationDispatcher : IContinuationDispatcher, IDisposable
{
    public void UnsafeQueueUserWorkItem(Action<object?> callback, object? state)
        => throw new NotImplementedException();

    public void Dispose() => throw new NotImplementedException();
}
