using ExternalSort.Scalars;
using ExternalSort.Shared;

namespace ExternalSort.OrderBy;


internal enum OrderBy
{
    Asc,
    Desc
}

internal class ExternalOrderByAsyncEnumerable<T, TK> : IExternalOrderByAsyncEnumerable<T> 
    where T : new()
    where TK : IComparable<TK>
{
    private readonly Func<T, long> _calculateBytesInRam = r => 300L;
    private readonly int _mbLimit = 200;
    private readonly int _openFilesLimit = 10;
    private readonly List<IComparer<T>> _orderByPairs;
    private readonly IAsyncEnumerable<T> _src;
    private readonly string? _tempDir;
    private readonly CancellationToken _abort;

    
    internal ExternalOrderByAsyncEnumerable(IAsyncEnumerable<T> src, Func<T, TK> keySelector, OrderBy orderBy, CancellationToken abort)
    {
        _src = src;
        _orderByPairs = [new ObjKeyComparer<T,TK>(new(keySelector, orderBy))];
        _abort = abort;
    }
    
    private ExternalOrderByAsyncEnumerable(Func<T, long> calculateBytesInRam, int mbLimit, int openFilesLimit, IAsyncEnumerable<T> src, List<IComparer<T>> orderByPairs, string? tempDir, CancellationToken abort)
    {
        _calculateBytesInRam = calculateBytesInRam;
        _mbLimit = mbLimit;
        _openFilesLimit = openFilesLimit;
        _src = src;
        _orderByPairs = orderByPairs;
        _tempDir = tempDir;
        _abort = abort;
    }
    
    public IExternalOrderByAsyncEnumerable<T> OptimiseFor(Func<T, long>? calculateBytesInRam = null, int? mbLimit = null, int? openFilesLimit = null)
    {
        if (mbLimit <= 0)
            throw new ArgumentException($"{nameof(mbLimit)} is an invalid non-positive number: {mbLimit}");
        if (openFilesLimit <= 1)
            throw new ArgumentException($"{nameof(openFilesLimit)} must be 2 or greater.  Found: {openFilesLimit}");
        return new ExternalOrderByAsyncEnumerable<T, TK>(calculateBytesInRam ?? _calculateBytesInRam, mbLimit ?? _mbLimit, openFilesLimit ?? _openFilesLimit, _src, _orderByPairs, _tempDir, _abort);
    }

    public IExternalOrderByAsyncEnumerable<T> UseTempDir(string dir)
    {
        return new ExternalOrderByAsyncEnumerable<T, TK>(_calculateBytesInRam, _mbLimit, _openFilesLimit, _src, _orderByPairs, dir, _abort);
    }

    public IExternalOrderByAsyncEnumerable<T> ThenBy<TK1>(Func<T, TK1> keySelector) where TK1 : IComparable<TK1>
    {
        return new ExternalOrderByAsyncEnumerable<T, TK>(_calculateBytesInRam, _mbLimit, _openFilesLimit, _src,
            _orderByPairs.Concat([new ObjKeyComparer<T,TK1>(new (keySelector, OrderBy.Asc))]).ToList(), _tempDir, _abort);
    }

    public IExternalOrderByAsyncEnumerable<T> ThenByDescending<TK1>(Func<T, TK1> keySelector) where TK1 : IComparable<TK1>
    {
        return new ExternalOrderByAsyncEnumerable<T, TK>(_calculateBytesInRam, _mbLimit, _openFilesLimit, _src,
            _orderByPairs.Concat([new ObjKeyComparer<T,TK1>(new (keySelector, OrderBy.Desc))]).ToList(), _tempDir, _abort);
    }

    public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = new())
    {
        using var sorter = new ExternalSorter<T>(_calculateBytesInRam, _mbLimit, _openFilesLimit, _orderByPairs.AsReadOnly(), _tempDir, _abort);
        await sorter.SplitAndSortEach(_src);
        await sorter.MergeSortFiles();
        await foreach (var row in sorter.MergeRead().WithCancellation(cancellationToken))
            yield return row;
    }
}


internal class ExternalOrderByScalarAsyncEnumerable<T, TK> : IExternalOrderByAsyncEnumerable<T> 
    where T : new() 
    where TK : IComparable<TK>
{
    private readonly Func<T, long> _calculateBytesInRam = r => 300L;
    private readonly int _mbLimit = 200;
    private readonly int _openFilesLimit = 10;
    private readonly List<IComparer<Scalar<T>>> _orderByPairs;
    private readonly IAsyncEnumerable<Scalar<T>> _src;
    private readonly string? _tempDir;
    private readonly CancellationToken _abort;


    internal ExternalOrderByScalarAsyncEnumerable(IAsyncEnumerable<T> src, Func<T, TK> keySelector, OrderBy orderBy, CancellationToken abort)
    {
        _src = src.Select(i => new Scalar<T>(i));
        _orderByPairs = [new ObjKeyComparer<Scalar<T>,TK>(new(o => keySelector(o.Value), orderBy))];
        _abort = abort;
    }
    
    private ExternalOrderByScalarAsyncEnumerable(Func<T, long> calculateBytesInRam, int mbLimit, int openFilesLimit, IAsyncEnumerable<Scalar<T>> src, List<IComparer<Scalar<T>>> orderByPairs, string? tempDir, CancellationToken abort)
    {
        _calculateBytesInRam = calculateBytesInRam;
        _mbLimit = mbLimit;
        _openFilesLimit = openFilesLimit;
        _src = src;
        _orderByPairs = orderByPairs;
        _tempDir = tempDir;
        _abort = abort;
    }
    
    public IExternalOrderByAsyncEnumerable<T> OptimiseFor(Func<T, long>? calculateBytesInRam = null, int? mbLimit = null, int? openFilesLimit = null)
    {
        if (mbLimit <= 0)
            throw new ArgumentException($"{nameof(mbLimit)} is an invalid non-positive number: {mbLimit}");
        if (openFilesLimit <= 1)
            throw new ArgumentException($"{nameof(openFilesLimit)} must be 2 or greater.  Found: {openFilesLimit}");
        return new ExternalOrderByScalarAsyncEnumerable<T, TK>(calculateBytesInRam ?? _calculateBytesInRam, mbLimit ?? _mbLimit, openFilesLimit ?? _openFilesLimit, _src, _orderByPairs, _tempDir, _abort);
    }

    public IExternalOrderByAsyncEnumerable<T> UseTempDir(string dir)
    {
        return new ExternalOrderByScalarAsyncEnumerable<T, TK>(_calculateBytesInRam, _mbLimit, _openFilesLimit, _src, _orderByPairs, _tempDir, _abort);
    }

    public IExternalOrderByAsyncEnumerable<T> ThenBy<TK1>(Func<T, TK1> keySelector) where TK1 : IComparable<TK1>
    {
        return new ExternalOrderByScalarAsyncEnumerable<T, TK>(_calculateBytesInRam, _mbLimit, _openFilesLimit, _src,
            _orderByPairs.Concat([new ObjKeyComparer<Scalar<T>,TK1>(new (o => keySelector(o.Value), OrderBy.Asc))]).ToList(), _tempDir, _abort);
    }

    public IExternalOrderByAsyncEnumerable<T> ThenByDescending<TK1>(Func<T, TK1> keySelector) where TK1 : IComparable<TK1>
    {
        return new ExternalOrderByScalarAsyncEnumerable<T, TK>(_calculateBytesInRam, _mbLimit, _openFilesLimit, _src,
            _orderByPairs.Concat([new ObjKeyComparer<Scalar<T>,TK1>(new (o => keySelector(o.Value), OrderBy.Desc))]).ToList(), _tempDir, _abort);
    }

    public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = new())
    {
        using var sorter = new ExternalSorter<Scalar<T>>(v => _calculateBytesInRam(v.Value), _mbLimit, _openFilesLimit, _orderByPairs.AsReadOnly(), _tempDir, _abort);
        await sorter.SplitAndSortEach(_src);
        await sorter.MergeSortFiles();
        await foreach (var row in sorter.MergeRead().WithCancellation(cancellationToken))
            yield return row.Value;
    }
}


