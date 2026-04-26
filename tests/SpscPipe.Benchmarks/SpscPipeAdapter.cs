using System.IO.Pipelines;
using SpscPipelines;
using SpPipe = SpscPipelines.SpscPipe;

namespace SpscPipe.Benchmarks;

internal sealed class SpscPipeAdapter : IPipeAdapter
{
    private readonly SpPipe _pipe;
    public SpscPipeAdapter(SpscPipeOptions? options = null) => _pipe = new SpPipe(options ?? SpscPipeOptions.Default);
    public PipeReader Reader => _pipe.Reader;
    public PipeWriter Writer => _pipe.Writer;
    public void Dispose() => _pipe.Dispose();
}
