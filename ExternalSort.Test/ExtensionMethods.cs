namespace ExternalSort.Test;

public static class ExtensionMethods
{
    public static async ValueTask<List<T>> ToListAsync<T>(this IAsyncEnumerable<T> items)
    {
        var evaluatedItems = new List<T>();
        await foreach (var item in items)
            evaluatedItems.Add(item);
        return evaluatedItems;
    }
    
    public static async IAsyncEnumerable<T> ToAsyncList<T>(this IEnumerable<T> items)
    {
        foreach (var item in items)
            yield return item;
    }

    
}