using ExternalSort.Distinct;

namespace ExternalSort;

public static class ExternalDistinctExtensions
{
    /// <summary>
    /// Removes duplicate rows, by using temporary files.
    /// See https://josef.codes/sorting-really-large-files-with-c-sharp/ and https://en.wikipedia.org/wiki/External_sorting
    /// Parquet files are used for temporarily persisting to disk.  See https://github.com/aloneguid/parquet-dotnet for class serialisation options.
    /// </summary>
    public static IExternalDistinctAsyncEnumerable<T> DistinctExternal<T>(this IAsyncEnumerable<T> src, CancellationToken? abort = null) where T : IComparable<T>, new()
    {
        return new ExternalDistinctAsyncEnumerable<T>(src, abort ?? CancellationToken.None);
    }

}