using System.IO.Pipelines;

namespace PipelyBenchmarks;

internal interface IPipeAdapter : IDisposable
{
    PipeReader Reader { get; }
    PipeWriter Writer { get; }
}
