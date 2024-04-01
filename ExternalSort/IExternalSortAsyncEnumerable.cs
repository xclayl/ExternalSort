namespace ExternalSort;

public interface IExternalSortAsyncEnumerable<out T> : IAsyncEnumerable<T> where T : new()
{
    IExternalSortAsyncEnumerable<T> OptimiseFor(Func<T, long>? calculateBytesInRam = null, int? mbLimit = null, int? openFilesLimit = null);
}

internal enum OrderBy
{
    Asc,
    Desc
}

internal class ExternalSortAsyncEnumerable<T, TK> : IExternalSortAsyncEnumerable<T> where T : new()
{
    private readonly Func<T, long> _calculateBytesInRam = r => 300L;
    private readonly int _mbLimit = 200;
    private readonly int _openFilesLimit = 10;
    private readonly OrderByPair<T, TK> _orderByPair;
    private readonly IAsyncEnumerable<T> _src;

    
    internal ExternalSortAsyncEnumerable(IAsyncEnumerable<T> src, Func<T, TK> keySelector, OrderBy orderBy)
    {
        _src = src;
        _orderByPair = new(keySelector, orderBy);
    }
    
    private ExternalSortAsyncEnumerable(Func<T, long> calculateBytesInRam, int mbLimit, int openFilesLimit, IAsyncEnumerable<T> src, OrderByPair<T, TK> orderByPair)
    {
        _calculateBytesInRam = calculateBytesInRam;
        _mbLimit = mbLimit;
        _openFilesLimit = openFilesLimit;
        _src = src;
        _orderByPair = orderByPair;
    }
    
    public IExternalSortAsyncEnumerable<T> OptimiseFor(Func<T, long>? calculateBytesInRam = null, int? mbLimit = null, int? openFilesLimit = null)
    {
        return new ExternalSortAsyncEnumerable<T, TK>(calculateBytesInRam ?? _calculateBytesInRam, mbLimit ?? _mbLimit, openFilesLimit ?? _openFilesLimit, _src, _orderByPair);
    }

    public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = new())
    {
        using var sorter = new ExternalSorter<T, TK>(_calculateBytesInRam, _mbLimit, _openFilesLimit, _orderByPair);
        await sorter.SplitAndSortEach(_src);
        await sorter.MergeSortFiles();
        await foreach (var row in sorter.MergeRead().WithCancellation(cancellationToken))
            yield return row;
    }
}