## External Sort

Sorts data (IAsyncEnumerable&lt;T>) that is larger than the amount of RAM available.

basic example

```
  IAsyncEnumerable<User> myUsersToSort = ...
  var sortedUsers = myUsersToSort
            .OrderByExternal(u => u.Email);
```

### Behaviour

 * *Unstable sort* - the rows that 'tie' will be returned in an undefined way
 * *O(n log n)* - best case performance when the data fits in RAM.  The more RAM you give it, the faster it will sort. More performance details here https://en.wikipedia.org/wiki/External_sorting
 * No disk activity when data fits in RAM

### Options / Features

Sort Descending

```
  IAsyncEnumerable<User> myUsersToSort = ...
  var sortedUsers = myUsersToSort
            .OrderByDescendingExternal(u => u.Email);
```

Sort on multiple properties

```
  IAsyncEnumerable<User> myUsersToSort = ...
  var sortedUsers = myUsersToSort
            .OrderByExternal(u => u.Email)
            .ThenBy(u => u.FirstName)
            .ThenByDescending(u => u.UserId);
```

Control the amount of RAM used
```
  IAsyncEnumerable<User> myUsersToSort = ...
  var sortedUsers = myUsersToSort
            .OrderByExternal(u => u.Email)
            .OptimiseFor(calculateBytesInRam: u => u.CalculateBytesInRam(), mbLimit: 1_000, openFilesLimit: 10);
```

* *calculateBytesInRam* - Calculates how much ram the row occupies in RAM.  Not calculated for every row, but enough to get an average. Default = 300 bytes
* *mbLimit* - The rough limit how much RAM to use to sort the data.  In practice this is not exact, but it will never be unlimited. Default = 200 MB
* *openFilesLimit* - The number of files open at one time.  A larger number can reduce the number of files needed to sort, but then each file has a smaller buffer and can become too "chatty" with the disk.  Buffer size for one temp file = mbLimit / openFileLimit.  Default = 10

Internally, ExternalSort uses Parquet temp files.   See https://github.com/aloneguid/parquet-dotnet for class serialisation options.

Thanks to https://josef.codes/sorting-really-large-files-with-c-sharp/ for the inspiration.