using System.Threading;

namespace SpscPipelines;

/// <summary>
/// Routes <see cref="SpscPipe"/>'s parked-awaiter continuations to a thread of the
/// implementation's choosing. Configured via <see cref="SpscPipeOptions.ContinuationDispatcher"/>;
/// the default (when the option is null) forwards to <see cref="ThreadPool.UnsafeQueueUserWorkItem(Action{object?}, object?, bool)"/>,
/// preserving the prior <c>RunContinuationsAsynchronously = true</c> configuration's observable behavior.
///
/// <para>
/// The primary motivation for plugging in a custom dispatcher is to escape the .NET ThreadPool
/// wake-gap latency (~390-500 ns at P50, multi-µs at P99) for high-frequency single-stream
/// workloads — typically by routing the consumer-side continuation to a busy-spinning thread
/// on a pinned core.
/// </para>
/// </summary>
public interface IContinuationDispatcher
{
    /// <summary>
    /// Queue the callback for invocation on a thread of the implementation's choosing.
    /// Mirrors the contract of <see cref="ThreadPool.UnsafeQueueUserWorkItem(Action{object?}, object?, bool)"/>:
    ///
    /// <list type="number">
    /// <item>
    /// The callback MUST be invoked exactly once.
    /// </item>
    /// <item>
    /// The implementation MUST NOT capture or apply an <see cref="System.Threading.ExecutionContext"/>.
    /// EC handling for SpscPipe's awaitable continuations is performed by <c>SpscAwaiter&lt;T&gt;</c>:
    /// it captures the consumer's <see cref="System.Threading.ExecutionContext"/> at
    /// <see cref="System.Threading.Tasks.Sources.IValueTaskSource.OnCompleted"/> time (per the
    /// consumer's <c>FlowExecutionContext</c> flag), passes the dispatcher a work item that carries
    /// the captured EC alongside the continuation (via fields on the awaiter — the awaiter is the
    /// <c>state</c> argument), and applies the EC via <see cref="System.Threading.ExecutionContext.Run"/>
    /// at invoke time. The dispatcher is purely a thread router. Adding EC manipulation in the
    /// dispatcher would interfere with the source-side capture/apply protocol and is forbidden.
    /// </item>
    /// <item>
    /// The implementation MUST be thread-safe; concurrent calls from multiple producer threads
    /// are permitted (one dispatcher may serve multiple pipes).
    /// </item>
    /// <item>
    /// The implementation MUST NOT throw from <c>UnsafeQueueUserWorkItem</c> itself.
    /// Failure to invoke the callback hangs the consumer's await indefinitely; a dispatcher in
    /// a failed state should still attempt to invoke the callback (e.g., fall back to TP) rather
    /// than throw.
    /// </item>
    /// <item>
    /// Implementations SHOULD wrap the callback invocation in <c>try</c>/<c>catch</c> so a
    /// throwing continuation doesn't kill the dispatcher's worker thread(s).
    /// </item>
    /// </list>
    /// </summary>
    /// <param name="callback">The continuation callback to invoke. Must not be null.</param>
    /// <param name="state">Opaque state to pass to <paramref name="callback"/>.</param>
    void UnsafeQueueUserWorkItem(Action<object?> callback, object? state);
}

/// <summary>
/// Default <see cref="IContinuationDispatcher"/>: forwards to
/// <see cref="ThreadPool.UnsafeQueueUserWorkItem(Action{object?}, object?, bool)"/> with
/// <c>preferLocal: false</c>. Stateless singleton; used when
/// <see cref="SpscPipeOptions.ContinuationDispatcher"/> is null.
/// </summary>
internal sealed class ThreadPoolContinuationDispatcher : IContinuationDispatcher
{
    public static readonly ThreadPoolContinuationDispatcher Instance = new();

    private ThreadPoolContinuationDispatcher() { }

    public void UnsafeQueueUserWorkItem(Action<object?> callback, object? state)
        => ThreadPool.UnsafeQueueUserWorkItem(callback, state, preferLocal: false);
}
