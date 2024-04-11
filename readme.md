## External Sort

Contains various utilities to sort and join large amounts of data that is larger than the amount of available RAM.

[Documentation](https://github.com/xclayl/ExternalSort/blob/master/docs/Index.md)

### OrderByExternal()

Sort IAsyncEnumerable&lt;T> data.

```
  IAsyncEnumerable<User> myUsersToSort = ...
  var sortedUsers = myUsersToSort
    .OrderByExternal(u => u.Email);
```

### GroupJoinExternal()

GroupJoin (left outer join) on IAsyncEnumerable&lt;T> data.

```
  IAsyncEnumerable<User> myUsers = ...
  IAsyncEnumerable<UserComments> myUserComments = ...
  
  var joinedUsersAndComments = myUsersToSort
    .GroupJoinExternal(myUserComments, u => u.UserId, uc => uc.UserId, (user, comments) => new
    {
        User = user,
        Comments = comments.ToList()
    });
```


### ExceptByExternal()
ExceptBy (NOT IN) on IAsyncEnumerable&lt;T> data.

```
  IAsyncEnumerable<User> myUsers = ...
  IAsyncEnumerable<int> myStaffUserIds = ...
  
  var nonStaffUsers = myUsers
    .ExceptByExternal(myStaffUserIds, u => u.UserId);
```


### DistinctExternal()
Distinct on IAsyncEnumerable&lt;T> data.

```
  IAsyncEnumerable<User> myUsers = ...
  IAsyncEnumerable<int> myUserAges = myUsers.Select(u => u.Age)
  
  var distinctAges = myUserAges
    .DistinctExternal();
```



Internally, ExternalSort uses Parquet temp files.   See https://github.com/aloneguid/parquet-dotnet for class serialisation options.

Thanks to https://josef.codes/sorting-really-large-files-with-c-sharp/ for the inspiration.