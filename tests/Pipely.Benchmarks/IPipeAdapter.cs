using System.IO.Pipelines;

namespace Pipely.Benchmarks;

internal interface IPipeAdapter : IDisposable
{
    PipeReader Reader { get; }
    PipeWriter Writer { get; }
}
