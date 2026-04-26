using System.IO.Pipelines;

namespace SpscPipe.Benchmarks;

internal interface IPipeAdapter : IDisposable
{
    PipeReader Reader { get; }
    PipeWriter Writer { get; }
}
