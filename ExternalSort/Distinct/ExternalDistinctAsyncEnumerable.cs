using ExternalSort.Shared;

namespace ExternalSort.Distinct;

internal class ExternalDistinctAsyncEnumerable<T> : IExternalDistinctAsyncEnumerable<T> where T : IComparable<T>, new()
{
    private readonly Func<T, long> _calculateBytesInRam = r => 300L;
    private readonly int _mbLimit = 200;
    private readonly int _openFilesLimit = 10;
    private readonly IAsyncEnumerable<T> _source;
    private readonly CancellationToken _abort;

    public ExternalDistinctAsyncEnumerable(IAsyncEnumerable<T> source, CancellationToken abort)
    {
        _source = source;
        _abort = abort;
    }
    
    private ExternalDistinctAsyncEnumerable(Func<T, long> calculateBytesInRam, int mbLimit, int openFilesLimit, IAsyncEnumerable<T> source, CancellationToken abort)
    {
        _calculateBytesInRam = calculateBytesInRam;
        _mbLimit = mbLimit;
        _openFilesLimit = openFilesLimit;
        _source = source;
        _abort = abort;
    }


    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = new CancellationToken())
    {
        return _source
            .OrderByExternal(_abort)
            .OptimiseFor(_calculateBytesInRam, _mbLimit, _openFilesLimit)
            .DistinctUsingOrderedInput(_abort)
            .GetAsyncEnumerator(cancellationToken);
    }

    public IExternalDistinctAsyncEnumerable<T> OptimiseFor(Func<T, long>? calculateBytesInRam = null, int? mbLimit = null,
        int? openFilesLimit = null)
    {
        return new ExternalDistinctAsyncEnumerable<T>(calculateBytesInRam ?? _calculateBytesInRam, mbLimit ?? _mbLimit, openFilesLimit ?? _openFilesLimit, _source, _abort);

    }
}