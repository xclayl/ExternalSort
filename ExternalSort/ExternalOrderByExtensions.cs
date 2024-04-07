using System.Runtime.CompilerServices;
using ExternalSort.OrderBy;

namespace ExternalSort;

public static class ExternalOrderByExtensions
{

    
    /// <summary>
    /// Sorts data that occupies more RAM than is available, by using temporary files.
    /// See https://josef.codes/sorting-really-large-files-with-c-sharp/ and https://en.wikipedia.org/wiki/External_sorting
    /// Parquet files are used for temporarily persisting to disk.  See https://github.com/aloneguid/parquet-dotnet for class serialisation options.
    /// Rows with duplicate keys are preserved. The order between them will be random.
    /// </summary>
    public static IExternalOrderByAsyncEnumerable<T> OrderByExternal<T, TK>(this IAsyncEnumerable<T> src, Func<T, TK> keySelector) where T : new() 
    {
        return new ExternalOrderByAsyncEnumerable<T, TK>(src, keySelector, OrderBy.OrderBy.Asc);
    }
    
        
    /// <summary>
    /// Sorts data that occupies more RAM than is available, by using temporary files.
    /// See https://josef.codes/sorting-really-large-files-with-c-sharp/ and https://en.wikipedia.org/wiki/External_sorting
    /// Parquet files are used for temporarily persisting to disk.  See https://github.com/aloneguid/parquet-dotnet for class serialisation options.
    /// Rows with duplicate keys are preserved. The order between them will be random.
    /// </summary>
    public static IExternalOrderByAsyncEnumerable<T> OrderByDescendingExternal<T, TK>(this IAsyncEnumerable<T> src, Func<T, TK> keySelector) where T : new() 
    {
        return new ExternalOrderByAsyncEnumerable<T, TK>(src, keySelector, OrderBy.OrderBy.Desc);
    }



}