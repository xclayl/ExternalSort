using ExternalSort.Shared;

namespace ExternalSort.ExceptBy;

internal class ExternalExceptByAsyncEnumerable<TSource, TKey> : IExternalExceptByAsyncEnumerable<TSource> where TSource : new() where TKey : new()
{
    private readonly IAsyncEnumerable<TSource> _first;
    private readonly IAsyncEnumerable<TKey> _second;
    private readonly Func<TSource, TKey> _keySelector;
    
    private readonly Func<TSource, long> _calculateBytesInRam = o => 300;
    private readonly int _mbLimit = 200;
    private readonly int _openFilesLimit = 10;
    private readonly IComparer<TKey> _keyComparer;
    
    public ExternalExceptByAsyncEnumerable(IAsyncEnumerable<TSource> first, IAsyncEnumerable<TKey> second, Func<TSource,TKey> keySelector)
    {
        _first = first;
        _second = second;
        _keySelector = keySelector;
        _keyComparer =  Comparer<TKey>.Default;
    }

    private ExternalExceptByAsyncEnumerable(
        Func<TSource, long> calculateBytesInRam,
        int mbLimit,
        int openFilesLimit,
        IComparer<TKey> keyComparer,
        IAsyncEnumerable<TSource> first, IAsyncEnumerable<TKey> second, Func<TSource,TKey> keySelector
        )
    {
        _first = first;
        _second = second;
        _keySelector = keySelector;

        _calculateBytesInRam = calculateBytesInRam;
        _mbLimit = mbLimit;
        _openFilesLimit = openFilesLimit;
        _keyComparer = keyComparer;
    }

    private readonly record struct SelectorPair<T>(T Item, bool HasAnyFromSecond);
    
    public IAsyncEnumerator<TSource> GetAsyncEnumerator(CancellationToken cancellationToken = new CancellationToken())
    {
        var pairRows = _first
            .GroupJoinExternal(_second, _keySelector, s => s, (f, s) => new SelectorPair<TSource>(f, s.Any()));

        return pairRows
            .Where(r => !r.HasAnyFromSecond)
            .Select(r => r.Item)
            .WhenKeyChanges(_keySelector, _keyComparer)
            .GetAsyncEnumerator(cancellationToken);
    }

    public IExternalExceptByAsyncEnumerable<TSource> OptimiseFor(Func<TSource, long>? calculateBytesInRam = null, int? mbLimit = null,
        int? openFilesLimit = null)
    {
        return new ExternalExceptByAsyncEnumerable<TSource, TKey>(calculateBytesInRam ?? _calculateBytesInRam,
            mbLimit ?? _mbLimit, openFilesLimit ?? _openFilesLimit,
            _keyComparer, _first, _second, _keySelector);
    }
}