namespace ExternalSort.Shared;

internal class TempDir : IDisposable
{
    private DirectoryInfo _dir;
    
    public TempDir(string? tempDir)
    {
        var tempPath = tempDir ?? Path.Combine(Path.GetTempPath(), "ExternalSort_" + Guid.NewGuid());
        
        var dir = new DirectoryInfo(tempPath);
        if (dir.Exists)
            throw new Exception(
                $"Temp directory, {tempPath}, exists.  Provide a path that doesn't exist, and this code will delete the directory when it finishes");
        dir.Create();
        _dir = dir;
    }

    public string FullName => _dir.FullName;

    public void Dispose()
    {
        _dir.Delete(true);
    }
}