## External Sort

Contains various utilities to sort and join large amounts of data that is larger than the amount of RAM available.

[Documentation]

### OrderByExternal()

Sort IAsyncEnumerable&lt;T> data.

```
  IAsyncEnumerable<User> myUsersToSort = ...
  var sortedUsers = myUsersToSort
    .OrderByExternal(u => u.Email);
```


GroupJoin (left outer join) IAsyncEnumerable&lt;T> data.

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


ExceptBy (NOT IN) IAsyncEnumerable&lt;T> data.

```
  IAsyncEnumerable<User> myUsers = ...
  IAsyncEnumerable<int> myStaffUserIds = ...
  
  var nonStaffUsers = myUsers
    .ExceptByExternal(myStaffUserIds, u => u.UserId);
```



Internally, ExternalSort uses Parquet temp files.   See https://github.com/aloneguid/parquet-dotnet for class serialisation options.

Thanks to https://josef.codes/sorting-really-large-files-with-c-sharp/ for the inspiration.