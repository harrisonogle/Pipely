using System.IO.Pipelines;

namespace SpscPipelines.Benchmarks;

internal interface IPipeAdapter : IDisposable
{
    PipeReader Reader { get; }
    PipeWriter Writer { get; }
}
