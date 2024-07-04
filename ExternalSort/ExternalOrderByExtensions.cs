using ExternalSort.OrderBy;
using ExternalSort.Scalars;
using ExternalSort.Shared;

namespace ExternalSort;

public static class ExternalOrderByExtensions
{

    
    /// <summary>
    /// Sorts data that occupies more RAM than is available, by using temporary files.
    /// See https://josef.codes/sorting-really-large-files-with-c-sharp/ and https://en.wikipedia.org/wiki/External_sorting
    /// Parquet files are used for temporarily persisting to disk.  See https://github.com/aloneguid/parquet-dotnet for class serialisation options.
    /// Rows with duplicate keys are preserved. The order between them will be random.
    /// </summary>
    public static IExternalOrderByAsyncEnumerable<T> OrderByExternal<T, TK>(this IAsyncEnumerable<T> src, Func<T, TK> keySelector, CancellationToken? abort = null) 
        where T : new()
        where TK : IComparable<TK>
    {
        abort ??= CancellationToken.None;
        return typeof(T).IsValueType switch
        {
            true => new ExternalOrderByScalarAsyncEnumerable<T, TK>(src, keySelector, OrderBy.OrderBy.Asc, abort.Value),
            _ => new ExternalOrderByAsyncEnumerable<T, TK>(src, keySelector, OrderBy.OrderBy.Asc, abort.Value)
        };
    }

        
    /// <summary>
    /// Sorts data that occupies more RAM than is available, by using temporary files.
    /// See https://josef.codes/sorting-really-large-files-with-c-sharp/ and https://en.wikipedia.org/wiki/External_sorting
    /// Parquet files are used for temporarily persisting to disk.  See https://github.com/aloneguid/parquet-dotnet for class serialisation options.
    /// Rows with duplicate keys are preserved. The order between them will be random.
    /// </summary>
    public static IExternalOrderByAsyncEnumerable<T> OrderByDescendingExternal<T, TK>(this IAsyncEnumerable<T> src, Func<T, TK> keySelector, CancellationToken? abort = null) 
        where T : new() 
        where TK : IComparable<TK>
    {
        abort ??= CancellationToken.None;
        return typeof(T).IsValueType switch
        {
            true => new ExternalOrderByScalarAsyncEnumerable<T, TK>(src, keySelector, OrderBy.OrderBy.Desc, abort.Value),
            _ => new ExternalOrderByAsyncEnumerable<T, TK>(src, keySelector, OrderBy.OrderBy.Desc, abort.Value)
        };
        
    }

    
    /// <summary>
    /// Sorts data that occupies more RAM than is available, by using temporary files.
    /// See https://josef.codes/sorting-really-large-files-with-c-sharp/ and https://en.wikipedia.org/wiki/External_sorting
    /// Parquet files are used for temporarily persisting to disk.  See https://github.com/aloneguid/parquet-dotnet for class serialisation options.
    /// Rows with equal order are preserved. The order between them will be random.
    /// </summary>
    public static IExternalOrderByAsyncEnumerable<T> OrderByExternal<T>(this IAsyncEnumerable<T> src, CancellationToken? abort = null) where T : IComparable<T>, new()
    {
        abort ??= CancellationToken.None;
        return typeof(T).IsValueType switch
        {
            true => new ExternalOrderByScalarAsyncEnumerable<T, T>(src, r => r, OrderBy.OrderBy.Asc, abort.Value),
            _ => new ExternalOrderByAsyncEnumerable<T, T>(src, r => r, OrderBy.OrderBy.Asc, abort.Value)
        };
    }

}