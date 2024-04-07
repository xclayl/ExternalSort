using ExternalSort.ExceptBy;

namespace ExternalSort;

public static class ExternalExceptByExtensions
{
    /// <summary>
    /// Performs a ExceptBy on data that is too large for RAM.  This works like a 'NOT IN' SQL clause.
    /// The Outer data must have unique keys, or an Exception is thrown.  This is to keep the implementation efficient by
    /// avoiding re-reading the Input data.
    /// Internally, it uses OrderByExternal() to sort both inputs and perform the group-join.
    /// </summary>
    public static IExternalExceptByAsyncEnumerable<TSource> GroupJoinExternal<TSource, TKey>(
        this IAsyncEnumerable<TSource> first,
        IAsyncEnumerable<TKey> second,
        Func<TSource, TKey> keySelector) where TSource : new() where TKey : new()
    {
        return new ExternalExceptByAsyncEnumerable<TSource, TKey>(first, second, keySelector);
    }
}