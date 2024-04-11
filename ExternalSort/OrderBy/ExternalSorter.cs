using System.Collections.ObjectModel;
using System.IO.Compression;
using ExternalSort.Shared;
using Parquet;
using Parquet.Serialization;

namespace ExternalSort.OrderBy;

internal readonly record struct OrderByPair<T, TK>(Func<T, TK> KeySelector, OrderBy OrderBy);

internal class ExternalSorter<T> : IDisposable where T : new() // where TK : IComparable
{
    private readonly int _mbLimit;
    private readonly int _openFilesLimit;

    private readonly TempDir _tempDir;
    private readonly List<FileInfo> _tempFiles = new();
    private readonly Func<T, long> _calculateBytesInRam;
    private int _batchRowLimit;
    private int _createdFiles;
    private readonly IComparer<T> _comparer;

    private List<T>? _singleBatchShortcut;

    public ExternalSorter(Func<T, long> calculateBytesInRam, int mbLimit, int openFilesLimit, ReadOnlyCollection<IComparer<T>> orderByPairs,
        string? tempDir)
    {
        _calculateBytesInRam = calculateBytesInRam;
        _mbLimit = mbLimit;
        _openFilesLimit = openFilesLimit;
        _comparer =  new ObjComparer(orderByPairs);
        _tempDir = new TempDir(tempDir);
    }

    private class ObjComparer : IComparer<T>
    {
        private readonly ReadOnlyCollection<IComparer<T>> _orderByPairs;
    
        public ObjComparer(ReadOnlyCollection<IComparer<T>> orderByPairs)
        {
            _orderByPairs = orderByPairs;
        }
    
        public int Compare(T? x, T? y)
        {
            for (var i = 0; i < _orderByPairs.Count; i++)
            {
                var orderByPair = _orderByPairs[i];
                var val = orderByPair.Compare(x, y);

                if (val != 0)
                    return val;
            }

            return 0;
        }
    }

    public void Dispose()
    {
        _tempDir.Dispose();
    }

    
    private int RowGroupSize => _batchRowLimit / _openFilesLimit;
    
    public async ValueTask SplitAndSortEach(IAsyncEnumerable<T> src)
    {
        var isFirstBatch = true;
        var batchSizeInBytes = 0L;
        
        var currentBatch = new List<T>();
        var byteLimit = _mbLimit * 1024 * 1024;
        await foreach (var row in src)
        {
            if (isFirstBatch)
            {
                var itemBytes = _calculateBytesInRam(row);
                if (itemBytes <= 0)
                    throw new ArgumentException($"calculateBytesInRam() returned an invalid non-positive number: {itemBytes}");
                batchSizeInBytes += itemBytes;
                
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
            if (!_tempFiles.Any())
            {
                _singleBatchShortcut = currentBatch;
                return;
            }

            
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
        if (_singleBatchShortcut != null)
            return;
        
        while (_tempFiles.Count > _openFilesLimit)
        {
            var fileGroups = _tempFiles.Batch(_openFilesLimit).ToList();
         
            _tempFiles.Clear();
            foreach (var fileGroup in fileGroups)
            {
                var destFile = new FileInfo(Path.Combine(_tempDir.FullName, $"{_createdFiles++}.parquet"));
                _tempFiles.Add(destFile);

                await MergeSortFiles(fileGroup, destFile);
                
                foreach (var oldTempFile in fileGroup)
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

    private record struct ReaderInfo(IAsyncEnumerator<T> Reader, bool Active, T Head);

    private static bool AnyActive(ReaderInfo[] readers)
    {
        for(int i = 0; i < readers.Length; i++)
            if (readers[i].Active)
                return true;

        return false;
    }

    private int FindMinHeadPos(ReaderInfo[] readers)
    {
        // Easy way:
        // var minPos = headRows
        //     .Select((r, i) => (r, i))
        //     .Where(r => readers[r.i].Active)
        //     .MinBy(a => a.r.Head, _comparer)
        //     .i;
        
        // Fast way:
        var minPos = -1;
        var minVal = default(T);
        for (int i = 0; i < readers.Length; i++)
        {
            if (readers[i].Active)
                if (minPos == -1)
                {
                    minPos = i;
                    minVal = readers[i].Head;
                }
                else
                    if (_comparer.Compare(minVal, readers[i].Head) > 0)
                    {
                        minPos = i;
                        minVal = readers[i].Head;
                    }
        }

        return minPos;
    }
    
    private async IAsyncEnumerable<T> MergeReader(FileInfo[] srcFiles)
    {
        var readers = new ReaderInfo[srcFiles.Length];
        for (int i = 0; i < srcFiles.Length; i++)
            readers[i] = new(CreateReader(srcFiles[i]), false, default(T));
        
        try
        {
            for(int i = 0; i < readers.Length; i++)
            {
                readers[i].Active = await readers[i].Reader.MoveNextAsync();
                if (readers[i].Active)
                    readers[i].Head = readers[i].Reader.Current;
            }
            
            while (AnyActive(readers))
            {
                var minPos = FindMinHeadPos(readers);

                yield return readers[minPos].Head;

                readers[minPos].Active = await readers[minPos].Reader.MoveNextAsync();
                if (readers[minPos].Active)
                    readers[minPos].Head = readers[minPos].Reader.Current;
            }
            
        }
        finally
        {
            foreach (var reader in readers)
                await reader.Reader.DisposeAsync();
        }


    }

    private async IAsyncEnumerator<T> CreateReader(FileInfo srcFile)
    {
        await using var stream = srcFile.OpenRead();
        await foreach (var row in ParquetSerializer.DeserializeAllAsync<T>(stream))
            yield return row;
    }

    public IAsyncEnumerable<T> MergeRead()
    {
        if (_singleBatchShortcut != null)
            return ToAsyncList(_singleBatchShortcut);
        return MergeReader(_tempFiles.ToArray());
    }

    private static async IAsyncEnumerable<T> ToAsyncList(IEnumerable<T> src)
    {
        foreach (var item in src)
        {
            yield return item;
        }
    }
}