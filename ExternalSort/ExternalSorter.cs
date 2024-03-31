using System.IO.Compression;
using Parquet;
using Parquet.Serialization;

namespace ExternalSort;

public class ExternalSorter<T, TK> : IDisposable where T : new() // where TK : IComparable
{
    private const int MbLimit = 200;
    private const int OpenFilesLimit = 10;

    private readonly TempDir _tempDir = new();
    private readonly List<FileInfo> _tempFiles = new();
    private readonly Func<T, long> _calculateBytesInRam;
    private int _batchRowLimit;
    private int _createdFiles;
    private readonly IComparer<T> _comparer;

    public ExternalSorter(Func<T,long> calculateBytesInRam, Func<T, TK> keySelector)
    {
        _calculateBytesInRam = calculateBytesInRam;
        _comparer =  new ObjComparer(keySelector);
    }

    private class ObjComparer : IComparer<T>
    {
        private readonly Func<T, TK> _keySelector;
        private readonly Comparer<TK> _keyComparer = Comparer<TK>.Default;
    
        public ObjComparer(Func<T, TK> keySelector)
        {
            _keySelector = keySelector;
        }
    
        public int Compare(T? x, T? y)
        {
            var xKey = _keySelector(x);
            var yKey = _keySelector(y);

            return _keyComparer.Compare(xKey, yKey);
        }
    }

    public void Dispose()
    {
        _tempDir.Dispose();
    }

    
    private int RowGroupSize => _batchRowLimit / OpenFilesLimit;
    
    public async ValueTask SplitAndSortEach(IAsyncEnumerable<T> src)
    {
        var isFirstBatch = true;
        var batchSizeInBytes = 0L;
        
        var currentBatch = new List<T>();
        var byteLimit = MbLimit * 1024 * 1024;
        await foreach (var row in src)
        {
            if (isFirstBatch)
            {
                batchSizeInBytes += _calculateBytesInRam(row);
                if (batchSizeInBytes > byteLimit)
                    _batchRowLimit = currentBatch.Count;
            }
            
            currentBatch.Add(row);

            if (_batchRowLimit != 0 && _batchRowLimit <= currentBatch.Count)
            {
                currentBatch.Sort(_comparer);
                
                var currentFile = new FileInfo(Path.Combine(_tempDir.FullName, $"{_createdFiles++}.parquet"));
                _tempFiles.Add(currentFile);
                isFirstBatch = false;
                await ParquetSerializer.SerializeAsync(currentBatch, currentFile.FullName, new ParquetSerializerOptions
                {
                    CompressionLevel = CompressionLevel.Fastest,
                    CompressionMethod = CompressionMethod.Snappy, // chosen for speed 
                    RowGroupSize = RowGroupSize
                });
                currentBatch.Clear();
            }
        }

        if (isFirstBatch)
            _batchRowLimit = int.MaxValue;
        
        if (currentBatch.Any())
        {
            currentBatch.Sort(_comparer);
                
            var currentFile = new FileInfo(Path.Combine(_tempDir.FullName, $"{_createdFiles++}.parquet"));
            _tempFiles.Add(currentFile);
            isFirstBatch = false;
            await ParquetSerializer.SerializeAsync(currentBatch, currentFile.FullName, new ParquetSerializerOptions
            {
                CompressionLevel = CompressionLevel.Fastest,
                CompressionMethod = CompressionMethod.Snappy, // chosen for speed 
                RowGroupSize = RowGroupSize
            });
            currentBatch.Clear();
        }
    }

    public async ValueTask MergeSortFiles()
    {
        while (_tempFiles.Count > OpenFilesLimit)
        {
            var fileGroups = _tempFiles.Batch(OpenFilesLimit).ToList();
            var oldTempFiles = _tempFiles.ToArray();
            _tempFiles.Clear();
            foreach (var fileGroup in fileGroups)
            {
                var destFile = new FileInfo(Path.Combine(_tempDir.FullName, $"{_createdFiles++}.parquet"));
                _tempFiles.Add(destFile);

                await MergeSortFiles(fileGroup, destFile);
            }

            foreach (var oldTempFile in oldTempFiles)
            {
                oldTempFile.Delete();
            }
        }
    }

    private async ValueTask MergeSortFiles(FileInfo[] srcFiles, FileInfo destFile)
    {
        var batches = MergeReader(srcFiles).BatchAsync(RowGroupSize);

        var isFirst = true;

        await foreach (var batch in batches)
        {
            await ParquetSerializer.SerializeAsync(batch, destFile.FullName, new ParquetSerializerOptions
            {
                CompressionLevel = CompressionLevel.Fastest,
                CompressionMethod = CompressionMethod.Snappy, // chosen for speed 
                Append = !isFirst
            });
            isFirst = false;
        }
    }

    private async IAsyncEnumerable<T> MergeReader(FileInfo[] srcFiles)
    {
        var readers = srcFiles.Select(Reader).ToList();
        try
        {
            var readersActive = readers.Select(r => true).ToList();

            var headRows = new T[readers.Count];
            
            foreach (var (reader, index) in readers.Select((r, i) => (r, i)))
            {
                readersActive[index] = await reader.MoveNextAsync();
                if (readersActive[index])
                {
                    headRows[index] = reader.Current;
                }
            }
            
            while (readersActive.Any(r => r))
            {
                var minPos = headRows
                    .Select((r, i) => (r, i))
                    .Where(r => readersActive[r.i])
                    .MinBy(a => a.r, _comparer)
                    .i;

                yield return headRows[minPos];

                readersActive[minPos] = await readers[minPos].MoveNextAsync();
                if (readersActive[minPos])
                    headRows[minPos] = readers[minPos].Current;
            }


        }
        finally
        {
            foreach (var reader in readers)
                await reader.DisposeAsync();
        }


    }

    private async IAsyncEnumerator<T> Reader(FileInfo srcFile)
    {
        await using var stream = srcFile.OpenRead();
        await foreach (var row in ParquetSerializer.DeserializeAllAsync<T>(stream))
            yield return row;
    }

    public IAsyncEnumerable<T> MergeRead()
    {
        return MergeReader(_tempFiles.ToArray());
    }
}