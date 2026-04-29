using System.IO.Pipelines;

namespace SpscPipelines.Benchmarks;

internal sealed class BclPipeAdapter : IPipeAdapter
{
    private readonly Pipe _pipe;
    public BclPipeAdapter(PipeOptions? options = null) => _pipe = new Pipe(options ?? PipeOptions.Default);
    public PipeReader Reader => _pipe.Reader;
    public PipeWriter Writer => _pipe.Writer;
    public void Dispose() { /* BCL Pipe is not IDisposable; no-op */ }
}
