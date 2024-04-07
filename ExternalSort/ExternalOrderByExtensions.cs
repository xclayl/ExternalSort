using System.Runtime.CompilerServices;
using ExternalSort.OrderBy;

namespace ExternalSort;

public static class ExternalOrderByExtensions
{

    
    /// <summary>
    /// Sorts data that occupies more RAM than is available, by using temporary files.
    /// See https://josef.codes/sorting-really-large-files-with-c-sharp/ and https://en.wikipedia.org/wiki/External_sorting
    /// Parquet files are used for temporarily persisting to disk.  See https://github.com/aloneguid/parquet-dotnet for class serialisation options.
    /// </summary>
    public static IExternalOrderByAsyncEnumerable<T> OrderByExternal<T, TK>(this IAsyncEnumerable<T> src, Func<T, TK> keySelector) where T : new() 
    {
        return new ExternalOrderByAsyncEnumerable<T, TK>(src, keySelector, OrderBy.OrderBy.Asc);
    }
    
        
    /// <summary>
    /// Sorts data that occupies more RAM than is available, by using temporary files.
    /// See https://josef.codes/sorting-really-large-files-with-c-sharp/ and https://en.wikipedia.org/wiki/External_sorting
    /// Parquet files are used for temporarily persisting to disk.  See https://github.com/aloneguid/parquet-dotnet for class serialisation options.
    /// </summary>
    public static IExternalOrderByAsyncEnumerable<T> OrderByDescendingExternal<T, TK>(this IAsyncEnumerable<T> src, Func<T, TK> keySelector) where T : new() 
    {
        return new ExternalOrderByAsyncEnumerable<T, TK>(src, keySelector, OrderBy.OrderBy.Desc);
    }


    internal static IEnumerable<T[]> Batch<T>(this IEnumerable<T> src, int batchSize)
    {
        var currentBatch = new List<T>(batchSize);
        foreach (var row in src)
        {
            currentBatch.Add(row);
            if (currentBatch.Count >= batchSize)
            {
                yield return currentBatch.ToArray();
                currentBatch.Clear();
            }
        }
        
        if (currentBatch.Count > 0)
            yield return currentBatch.ToArray();
    }
    
    internal static async IAsyncEnumerable<T[]> BatchAsync<T>(this IAsyncEnumerable<T> src, int batchSize,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var currentBatch = new List<T>(batchSize);

        await foreach (var item in src.WithCancellation(cancellationToken))
        {
            if (cancellationToken.IsCancellationRequested)
                yield break;

            currentBatch.Add(item);
            if (currentBatch.Count >= batchSize)
            {
                yield return currentBatch.ToArray();
                currentBatch.Clear();
            }
        }

        if (currentBatch.Count > 0)
            yield return currentBatch.ToArray();
    }
}