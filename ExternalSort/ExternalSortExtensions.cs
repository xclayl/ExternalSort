using System.Runtime.CompilerServices;

namespace ExternalSort;

public static class ExternalSortExtensions
{

    
    /// <summary>
    /// Sorts data that occupies more RAM than is available, by using temporary files.
    /// See https://josef.codes/sorting-really-large-files-with-c-sharp/ and https://en.wikipedia.org/wiki/External_sorting
    /// Parquet files are used for temporarily persisting to disk.  See https://github.com/aloneguid/parquet-dotnet for class serialisation options.
    /// </summary>
    public static async IAsyncEnumerable<T> OrderByExternal<T, TK>(this IAsyncEnumerable<T> src, Func<T, TK> keySelector, Func<T, long> calculateBytesInRam) where T : new() // where TK : IComparable
    {
        using var sorter = new ExternalSorter<T, TK>(calculateBytesInRam, keySelector);
        await sorter.SplitAndSortEach(src);
        await sorter.MergeSortFiles();
        await foreach (var row in sorter.MergeRead())
            yield return row;
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