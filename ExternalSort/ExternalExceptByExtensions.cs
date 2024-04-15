using ExternalSort.ExceptBy;

namespace ExternalSort;

public static class ExternalExceptByExtensions
{
    /// <summary>
    /// Performs a ExceptBy on data that is too large for RAM.  This works like a 'DISTINCT' + 'NOT IN' SQL clause.
    /// Internally, it uses OrderByExternal() to sort both inputs and perform the except-by.
    /// Duplicate keys in the first input are deduplicated, in which case a random row is selected. (in order to match ExceptBy() behaviour)
    /// </summary>
    public static IExternalExceptByAsyncEnumerable<TSource> ExceptByExternal<TSource, TKey>(
        this IAsyncEnumerable<TSource> first,
        IAsyncEnumerable<TKey> second,
        Func<TSource, TKey> keySelector,
        CancellationToken? abort = null) where TSource : new() where TKey : new()
    {
        return new ExternalExceptByAsyncEnumerable<TSource, TKey>(first, second, keySelector, abort ?? CancellationToken.None);
    }
}