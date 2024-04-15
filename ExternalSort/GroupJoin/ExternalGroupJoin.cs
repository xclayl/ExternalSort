namespace ExternalSort.GroupJoin;

internal class ExternalGroupJoin<TOuter, TInner, TKey, TResult> : IAsyncDisposable where TOuter : new() where TInner : new()
{
    private readonly Func<TOuter, long> _calculateOuterBytesInRam;
    private readonly Func<TInner, long> _calculateInnerBytesInRam;
    private readonly int _mbLimit;
    private readonly int _openFilesLimit;
    private readonly IComparer<TKey> _keyComparer;
    private readonly IAsyncEnumerable<TOuter> _outerSource;
    private readonly Func<TOuter, TKey> _outerKeySelector;
    private readonly Func<TInner, TKey> _innerKeySelector;
    private readonly IAsyncEnumerator<TInner> _innerReader;
    private bool _innerReaderInit;
    private bool _innerReaderHasNext;
    private TInner? _next;
    private TKey? _nextKey;
    private readonly CancellationToken _abort;

    public ExternalGroupJoin(Func<TOuter,long> calculateOuterBytesInRam, Func<TInner, long> calculateInnerBytesInRam, 
        int mbLimit, int openFilesLimit, IComparer<TKey> keyComparer, IAsyncEnumerable<TOuter> outerSource,
        IAsyncEnumerable<TInner> innerSource, Func<TOuter, TKey> outerKeySelector, Func<TInner, TKey> innerKeySelector,
        CancellationToken abort)
    {
        _calculateOuterBytesInRam = calculateOuterBytesInRam;
        _calculateInnerBytesInRam = calculateInnerBytesInRam;
        _mbLimit = mbLimit;
        _openFilesLimit = openFilesLimit;
        _keyComparer = keyComparer;
        _outerSource = outerSource;
        _outerKeySelector = outerKeySelector;
        _innerKeySelector = innerKeySelector;

        _innerReader = innerSource.OrderByExternal(_innerKeySelector, abort)
            .OptimiseFor(_calculateInnerBytesInRam, Math.Max(_mbLimit / 2, 1), Math.Max(_openFilesLimit / 2, 2))
            .GetAsyncEnumerator();


        _innerReaderInit = false;
        _innerReaderHasNext = false;

        _next = default;
        _nextKey = default;

        _abort = abort;
    }


    public IAsyncEnumerable<TOuter> ReadOuter()
    {
        return _outerSource.OrderByExternal(_outerKeySelector, _abort)
            .OptimiseFor(_calculateOuterBytesInRam, Math.Max(_mbLimit / 2, 1), Math.Max(_openFilesLimit / 2, 2));
    }

    public async ValueTask ReadInner(TKey outerKey, List<TInner> destInnerList)
    {
        if (!_innerReaderInit)
        {
            await ReadNextInner();
            _innerReaderInit = true;
        }
        
        if (!_innerReaderHasNext)
            return;


        while (_innerReaderHasNext && _keyComparer.Compare(outerKey, _nextKey!) > 0)
            await ReadNextInner();
     

        while (_innerReaderHasNext && _keyComparer.Compare(outerKey, _nextKey) == 0)
        {
            destInnerList.Add(_next!);
            await ReadNextInner();
        }
    }

    private async ValueTask ReadNextInner()
    {
        _innerReaderHasNext = await _innerReader.MoveNextAsync();
        if (_innerReaderHasNext)
        {
            _next = _innerReader.Current;
            _nextKey = _innerKeySelector(_next!);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _innerReader.DisposeAsync();
    }
}