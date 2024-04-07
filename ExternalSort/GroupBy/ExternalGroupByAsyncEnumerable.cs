namespace ExternalSort.GroupBy;

internal class ExternalGroupByAsyncEnumerable<TOuter, TInner, TKey, TResult> : IExternalGroupJoinAsyncEnumerable<TOuter, TInner, TResult> 
    where TOuter : new() where TInner : new()
{
    private readonly Func<TOuter, long> _calculateOuterBytesInRam = o => 300;
    private readonly Func<TInner, long> _calculateInnerBytesInRam = o => 300;
    private readonly int _mbLimit = 200;
    private readonly int _openFilesLimit = 10;
    private readonly IComparer<TKey> _keyComparer;
    private readonly IAsyncEnumerable<TOuter> _outerSource;
    private readonly IAsyncEnumerable<TInner> _innerSource;
    private readonly Func<TOuter, TKey> _outerKeySelector;
    private readonly Func<TInner, TKey> _innerKeySelector;
    private readonly Func<TOuter, IEnumerable<TInner>, TResult> _resultSelector;

    
    public ExternalGroupByAsyncEnumerable(
        IAsyncEnumerable<TOuter> outerSource,
        IAsyncEnumerable<TInner> innerSource,
        Func<TOuter, TKey> outerKeySelector,
        Func<TInner, TKey> innerKeySelector,
        Func<TOuter, IEnumerable<TInner>, TResult> resultSelector)
    {
        _keyComparer =  Comparer<TKey>.Default;
        _outerSource = outerSource;
        _innerSource = innerSource;
        _outerKeySelector = outerKeySelector;
        _innerKeySelector = innerKeySelector;
        _resultSelector = resultSelector;
    }

    private ExternalGroupByAsyncEnumerable(Func<TOuter, long> calculateOuterBytesInRam, 
        Func<TInner, long> calculateInnerBytesInRam,
        int mbLimit,
        int openFilesLimit,
        IComparer<TKey> keyComparer,
        IAsyncEnumerable<TOuter> outerSource,
        IAsyncEnumerable<TInner> innerSource,
        Func<TOuter, TKey> outerKeySelector,
        Func<TInner, TKey> innerKeySelector,
        Func<TOuter, IEnumerable<TInner>, TResult> resultSelector)
    {
        _calculateOuterBytesInRam = calculateOuterBytesInRam;
        _calculateInnerBytesInRam = calculateInnerBytesInRam;
        _mbLimit = mbLimit;
        _openFilesLimit = openFilesLimit;
        
        _keyComparer = keyComparer;
        _outerSource = outerSource;
        _innerSource = innerSource;
        _outerKeySelector = outerKeySelector;
        _innerKeySelector = innerKeySelector;
        _resultSelector = resultSelector;

    }
    
    public IExternalGroupJoinAsyncEnumerable<TOuter, TInner, TResult> OptimiseFor(Func<TOuter, long>? calculateOuterBytesInRam = null,
        Func<TInner, long>? calculateInnerBytesInRam = null, int? mbLimit = null, int? openFilesLimit = null)
    {
        return new ExternalGroupByAsyncEnumerable<TOuter, TInner, TKey, TResult>(calculateOuterBytesInRam ?? _calculateOuterBytesInRam,
            calculateInnerBytesInRam ?? _calculateInnerBytesInRam,
            mbLimit ?? _mbLimit,
            openFilesLimit ?? _openFilesLimit,
            _keyComparer,
            _outerSource,
            _innerSource,
            _outerKeySelector,
            _innerKeySelector,
            _resultSelector
            );
    }
    
    
    public async IAsyncEnumerator<TResult> GetAsyncEnumerator(CancellationToken cancellationToken = new CancellationToken())
    {
       
        await using var groupBy = new ExternalGroupBy<TOuter, TInner, TKey, TResult>(_calculateOuterBytesInRam,
            _calculateInnerBytesInRam,
            _mbLimit,
            _openFilesLimit,
            _keyComparer,
            _outerSource,
            _innerSource,
            _outerKeySelector,
            _innerKeySelector);

        var innerList = new List<TInner>();
        TKey previousOuterKey = default!;
        var first = true;
        await foreach (var outer in groupBy.ReadOuter().WithCancellation(cancellationToken))
        {
            innerList.Clear();

            var outerKey = _outerKeySelector(outer);
            if (!first && _keyComparer.Compare(previousOuterKey, outerKey) == 0)
                throw new Exception($"Duplicate outer key value {outerKey}");

            previousOuterKey = outerKey;
            
            await groupBy.ReadInner(outerKey, innerList);
            yield return _resultSelector(outer, innerList);
            first = false;
        }
    }

}