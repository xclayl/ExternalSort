using ExternalSort.Shared;

namespace ExternalSort.GroupJoin;

internal class ExternalGroupJoinAsyncEnumerable<TOuter, TInner, TKey, TResult> : IExternalGroupJoinAsyncEnumerable<TOuter, TInner, TResult> 
    where TOuter : new() where TInner : new() where TKey : IComparable<TKey>
{
    private readonly Func<TOuter, long> _calculateOuterBytesInRam = o => 300;
    private readonly Func<TInner, long> _calculateInnerBytesInRam = o => 300;
    private readonly int _mbLimit = 200;
    private readonly int _openFilesLimit = 10;
    private readonly IAsyncEnumerable<TOuter> _outerSource;
    private readonly IAsyncEnumerable<TInner> _innerSource;
    private readonly Func<TOuter, TKey> _outerKeySelector;
    private readonly Func<TInner, TKey> _innerKeySelector;
    private readonly Func<TOuter, IEnumerable<TInner>, TResult> _resultSelector;
    private readonly CancellationToken _abort;

    
    public ExternalGroupJoinAsyncEnumerable(
        IAsyncEnumerable<TOuter> outerSource,
        IAsyncEnumerable<TInner> innerSource,
        Func<TOuter, TKey> outerKeySelector,
        Func<TInner, TKey> innerKeySelector,
        Func<TOuter, IEnumerable<TInner>, TResult> resultSelector, 
        CancellationToken abort)
    {
        _outerSource = outerSource;
        _innerSource = innerSource;
        _outerKeySelector = outerKeySelector;
        _innerKeySelector = innerKeySelector;
        _resultSelector = resultSelector;
        _abort = abort;
    }

    private ExternalGroupJoinAsyncEnumerable(Func<TOuter, long> calculateOuterBytesInRam, 
        Func<TInner, long> calculateInnerBytesInRam,
        int mbLimit,
        int openFilesLimit,
        IAsyncEnumerable<TOuter> outerSource,
        IAsyncEnumerable<TInner> innerSource,
        Func<TOuter, TKey> outerKeySelector,
        Func<TInner, TKey> innerKeySelector,
        Func<TOuter, IEnumerable<TInner>, TResult> resultSelector, 
        CancellationToken abort)
    {
        _calculateOuterBytesInRam = calculateOuterBytesInRam;
        _calculateInnerBytesInRam = calculateInnerBytesInRam;
        _mbLimit = mbLimit;
        _openFilesLimit = openFilesLimit;
        
        _outerSource = outerSource;
        _innerSource = innerSource;
        _outerKeySelector = outerKeySelector;
        _innerKeySelector = innerKeySelector;
        _resultSelector = resultSelector;
        _abort = abort;
    }
    
    public IExternalGroupJoinAsyncEnumerable<TOuter, TInner, TResult> OptimiseFor(Func<TOuter, long>? calculateOuterBytesInRam = null,
        Func<TInner, long>? calculateInnerBytesInRam = null, int? mbLimit = null, int? openFilesLimit = null)
    {
        return new ExternalGroupJoinAsyncEnumerable<TOuter, TInner, TKey, TResult>(calculateOuterBytesInRam ?? _calculateOuterBytesInRam,
            calculateInnerBytesInRam ?? _calculateInnerBytesInRam,
            mbLimit ?? _mbLimit,
            openFilesLimit ?? _openFilesLimit,
            _outerSource,
            _innerSource,
            _outerKeySelector,
            _innerKeySelector,
            _resultSelector,
            _abort
            );
    }
    
    
    public async IAsyncEnumerator<TResult> GetAsyncEnumerator(CancellationToken cancellationToken = new CancellationToken())
    {
       
        await using var groupBy = new ExternalGroupJoin<TOuter, TInner, TKey, TResult>(_calculateOuterBytesInRam,
            _calculateInnerBytesInRam,
            _mbLimit,
            _openFilesLimit,
            _outerSource,
            _innerSource,
            _outerKeySelector,
            _innerKeySelector,
            _abort);

        var innerList = new List<TInner>();
        TKey previousOuterKey = default!;
        var first = true;
        await foreach (var outer in groupBy.ReadOuter().WithCancellation(cancellationToken))
        {
            _abort.ThrowIfCancellationRequested();
            
            var outerKey = _outerKeySelector(outer);
            
            if (!first && CompareUtil.Compare(previousOuterKey, outerKey) == 0)
            {
                yield return _resultSelector(outer, innerList);
                continue;
            }

            innerList.Clear();
            previousOuterKey = outerKey;
            
            await groupBy.ReadInner(outerKey, innerList);
            yield return _resultSelector(outer, innerList);
            first = false;
        }
    }

}