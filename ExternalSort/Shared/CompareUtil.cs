namespace ExternalSort.Shared;

internal static class CompareUtil
{
    public static int Compare<T>(T? a, T? b) where T : IComparable<T>
    {
        if (a == null && b == null)
            return 0;
        if (a == null)
            return -1;
        return a.CompareTo(b);
    }
}