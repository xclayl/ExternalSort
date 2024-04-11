using System.Runtime.CompilerServices;

namespace ExternalSort.Shared;

internal static class SharedExtensionMethods
{
    
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
    
    
    internal static async IAsyncEnumerable<T> Where<T>(this IAsyncEnumerable<T> source, Func<T, bool> predicate)
    {
        await foreach (var item in source)
        {
            if (predicate(item))
                yield return item;
        }
    }


    internal static async IAsyncEnumerable<TResult> Select<T, TResult>(this IAsyncEnumerable<T> source, Func<T, TResult> project)
    {
        await foreach (var item in source)
        {
            yield return project(item);
        }
    }
    
    
    
    internal static async IAsyncEnumerable<T> DistinctUsingOrderedInput<T, TKey>(this IAsyncEnumerable<T> source, Func<T, TKey> keySelector, IComparer<TKey> comparer)
    {
        var first = true;
        TKey previousKey = default;
        
        await foreach (var item in source)
        {
            var key = keySelector(item);
            if (first || comparer.Compare(previousKey, key) != 0)
                yield return item;

            previousKey = key;
            first = false;
        }
    }
    
    
    internal static async IAsyncEnumerable<T> DistinctUsingOrderedInput<T>(this IAsyncEnumerable<T> source) where T : IComparable<T>
    {
        var first = true;
        T previousItem = default;

        var comparer = Comparer<T>.Default;
        
        await foreach (var item in source)
        {
            if (first || comparer.Compare(previousItem, item) != 0)
                yield return item;

            previousItem = item;
            first = false;
        }
    }


}