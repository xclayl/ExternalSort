using ExternalSort.GroupBy;

namespace ExternalSort;

public static class ExternalGroupByExtensions
{
    /// <summary>
    /// Performs a GroupJoin on data that is too large for RAM.  This works like a 'left join', where 'outer' is the left side.
    /// The Outer data must have unique keys, or an Exception is thrown.  This is to keep the implementation efficient by
    /// avoiding re-reading the Input data.
    /// Internally, it uses OrderByExternal() to sort both inputs and perform the group-join.
    /// </summary>
    public static IExternalGroupJoinAsyncEnumerable<TOuter, TInner, TResult> GroupJoinExternal<TOuter, TInner, TKey, TResult>(
        this IAsyncEnumerable<TOuter> outer,
        IAsyncEnumerable<TInner> inner,
        Func<TOuter, TKey> outerKeySelector,
        Func<TInner, TKey> innerKeySelector,
        Func<TOuter, IEnumerable<TInner>, TResult> resultSelector) where TOuter : new() where TInner : new()
    {
        return new ExternalGroupByAsyncEnumerable<TOuter, TInner, TKey, TResult>(outer, inner, outerKeySelector,
            innerKeySelector, resultSelector);
    }
}