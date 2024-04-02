namespace ExternalSort;

public interface IExternalSortAsyncEnumerable<out T> : IAsyncEnumerable<T> where T : new()
{
    IExternalSortAsyncEnumerable<T> OptimiseFor(Func<T, long>? calculateBytesInRam = null, int? mbLimit = null, int? openFilesLimit = null);
    IExternalSortAsyncEnumerable<T> ThenBy<TK>(Func<T, TK> keySelector);
    IExternalSortAsyncEnumerable<T> ThenByDescending<TK>(Func<T, TK> keySelector);
    
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
    private readonly List<IComparer<T>> _orderByPairs;
    private readonly IAsyncEnumerable<T> _src;

    
    internal ExternalSortAsyncEnumerable(IAsyncEnumerable<T> src, Func<T, TK> keySelector, OrderBy orderBy)
    {
        _src = src;
        _orderByPairs = [new ObjKeyComparer<T,TK>(new(keySelector, orderBy))];
    }
    
    private ExternalSortAsyncEnumerable(Func<T, long> calculateBytesInRam, int mbLimit, int openFilesLimit, IAsyncEnumerable<T> src, List<IComparer<T>> orderByPairs)
    {
        _calculateBytesInRam = calculateBytesInRam;
        _mbLimit = mbLimit;
        _openFilesLimit = openFilesLimit;
        _src = src;
        _orderByPairs = orderByPairs;
    }
    
    public IExternalSortAsyncEnumerable<T> OptimiseFor(Func<T, long>? calculateBytesInRam = null, int? mbLimit = null, int? openFilesLimit = null)
    {
        if (mbLimit <= 0)
            throw new ArgumentException($"{nameof(mbLimit)} is an invalid non-positive number: {mbLimit}");
        if (openFilesLimit <= 1)
            throw new ArgumentException($"{nameof(openFilesLimit)} must be 2 or greater.  Found: {openFilesLimit}");
        return new ExternalSortAsyncEnumerable<T, TK>(calculateBytesInRam ?? _calculateBytesInRam, mbLimit ?? _mbLimit, openFilesLimit ?? _openFilesLimit, _src, _orderByPairs);
    }

    public IExternalSortAsyncEnumerable<T> ThenBy<TK1>(Func<T, TK1> keySelector)
    {
        return new ExternalSortAsyncEnumerable<T, TK>(_calculateBytesInRam, _mbLimit, _openFilesLimit, _src,
            _orderByPairs.Concat([new ObjKeyComparer<T,TK1>(new (keySelector, OrderBy.Asc))]).ToList());
    }

    public IExternalSortAsyncEnumerable<T> ThenByDescending<TK1>(Func<T, TK1> keySelector)
    {
        return new ExternalSortAsyncEnumerable<T, TK>(_calculateBytesInRam, _mbLimit, _openFilesLimit, _src,
            _orderByPairs.Concat([new ObjKeyComparer<T,TK1>(new (keySelector, OrderBy.Desc))]).ToList());
    }

    public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = new())
    {
        using var sorter = new ExternalSorter<T, TK>(_calculateBytesInRam, _mbLimit, _openFilesLimit, _orderByPairs.AsReadOnly());
        await sorter.SplitAndSortEach(_src);
        await sorter.MergeSortFiles();
        await foreach (var row in sorter.MergeRead().WithCancellation(cancellationToken))
            yield return row;
    }
}