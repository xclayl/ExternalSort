namespace ExternalSort;

internal class TempDir : IDisposable
{
    private DirectoryInfo _dir;
    
    public TempDir()
    {
        var dir = new DirectoryInfo(Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString()));
        dir.Create();
        _dir = dir;
    }

    public string FullName => _dir.FullName;

    public void Dispose()
    {
        _dir.Delete(true);
    }
}