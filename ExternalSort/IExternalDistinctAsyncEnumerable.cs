namespace ExternalSort;

public interface IExternalDistinctAsyncEnumerable<out T> : IAsyncEnumerable<T> where T : IComparable<T>, new()
{
    IExternalDistinctAsyncEnumerable<T> OptimiseFor(Func<T, long>? calculateBytesInRam = null, int? mbLimit = null, int? openFilesLimit = null);
}