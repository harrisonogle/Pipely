using System.Threading;
using SpscPipelines;
using Xunit;

namespace SpscPipe.Tests;

public class SpscPipeContinuationDispatcherTests
{
    private sealed class RecordingDispatcher : IContinuationDispatcher
    {
        private readonly Action<Action<object?>>? _onDispatch;
        public RecordingDispatcher(Action<Action<object?>>? onDispatch = null) => _onDispatch = onDispatch;

        public void UnsafeQueueUserWorkItem(Action<object?> callback, object? state)
        {
            _onDispatch?.Invoke(callback);
            // Forward to TP so the await completes.
            ThreadPool.UnsafeQueueUserWorkItem(callback, state, preferLocal: false);
        }
    }

    [Fact]
    public async Task CustomDispatcher_ReceivesContinuationCallback_ForReadAsync()
    {
        int dispatchCount = 0;
        var dispatcher = new RecordingDispatcher(_ => Interlocked.Increment(ref dispatchCount));
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions { ContinuationDispatcher = dispatcher });

        var readTask = pipe.Reader.ReadAsync().AsTask();
        Assert.False(readTask.IsCompleted);

        await Task.Run(async () =>
        {
            var mem = pipe.Writer.GetMemory(5);
            mem.Span.Clear();
            pipe.Writer.Advance(5);
            await pipe.Writer.FlushAsync();
        });

        var result = await readTask;
        Assert.Equal(5, result.Buffer.Length);
        Assert.Equal(1, dispatchCount);
    }
}
