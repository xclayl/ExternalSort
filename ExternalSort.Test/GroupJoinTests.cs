using System.ComponentModel;
using System.Runtime.InteropServices.Marshalling;

namespace ExternalSort.Test;

public class GroupJoinTests
{
    
    [Fact]
    public void GroupJoinBehaviour()
    {
        List<int> parents = [1, 3];
        List<(int parent, int val)> children = [(1, 100), (1, 101), (2, 200)];

        var actual = parents
            .GroupJoin(children, u => u, uc => uc.parent, (u, ucs) => new
            {
                Parent = u,
                Children = ucs.ToList()
            })
            .ToList();

        actual.Should().HaveCount(2);
        actual.Select(a => a.Parent).Should().BeEquivalentTo([1, 3]);
        
        actual.Single(a => a.Parent == 1).Children.Should().BeEquivalentTo([(1, 100), (1, 101)]);
        actual.Where(a => a.Parent == 2).Should().BeEmpty();
        actual.Single(a => a.Parent == 3).Children.Should().BeEmpty();
    }
    
    
    
    [Fact]
    public void GroupJoinBehaviourDuplicates()
    {
        List<int> parents = [1, 1, 3];
        List<(int parent, int val)> children = [(1, 100), (1, 101), (2, 200)];

        var actual = parents
            .GroupJoin(children, u => u, uc => uc.parent, (u, ucs) => new
            {
                Parent = u,
                Children = ucs.ToList()
            })
            .ToList();

        actual.Should().HaveCount(3);
        actual.Select(a => a.Parent).Should().BeEquivalentTo([1, 1, 3]);
        
        actual[0].Children.Should().BeEquivalentTo([(1, 100), (1, 101)]);
        actual[1].Children.Should().BeEquivalentTo([(1, 100), (1, 101)]);
        actual[2].Children.Should().BeEmpty();
    }
    
    [Fact]
    public async Task GroupJoinExternalBehaviour()
    {
        List<int> parents = [1, 3];
        List<(int parent, int val)> children = [(1, 100), (1, 101), (2, 200)];

        var actual = await parents.ToAsyncList()
            .GroupJoinExternal(children.ToAsyncList(), u => u, uc => uc.parent, (u, ucs) => new
            {
                Parent = u,
                Children = ucs.ToList()
            })
            .ToListAsync();

        actual.Should().HaveCount(2);
        actual.Select(a => a.Parent).Should().BeEquivalentTo([1, 3]);
        
        actual.Single(a => a.Parent == 1).Children.Should().BeEquivalentTo([(1, 100), (1, 101)]);
        actual.Where(a => a.Parent == 2).Should().BeEmpty();
        actual.Single(a => a.Parent == 3).Children.Should().BeEmpty();
    }
    
    
    [Fact]
    public async Task GroupJoinExternalBehaviour2()
    {
        List<int> parents = [1, 1, 3];
        List<(int parent, int val)> children = [(1, 100), (1, 101), (2, 200)];

        var actual = await parents.ToAsyncList()
            .GroupJoinExternal(children.ToAsyncList(), u => u, uc => uc.parent, (u, ucs) => new
            {
                Parent = u,
                Children = ucs.ToList()
            })
            .ToListAsync();

        actual.Should().HaveCount(3);
        actual.Select(a => a.Parent).Should().BeEquivalentTo([1, 1, 3]);
        
        actual[0].Children.Should().BeEquivalentTo([(1, 100), (1, 101)]);
        actual[1].Children.Should().BeEquivalentTo([(1, 100), (1, 101)]);
        actual[2].Children.Should().BeEmpty();
    }


    
    [Fact]
    public async Task HappyPath_1TempFile()
    {
        var users = await RowGenerator.GenerateUsers(10).ToListAsync();
        var userComments = await RowGenerator.GenerateUserComments(100, users).ToListAsync();

        var actual = await users.ToAsyncList()
            .GroupJoinExternal(userComments.ToAsyncList(), u => u.UserGuid, uc => uc.UserGuid, (u, ucs) => new
            {
                User = u,
                UserComments = ucs.ToList()
            })
            .ToListAsync();

        actual.Should().HaveCount(users.Count);
        actual.SelectMany(u => u.UserComments).Should().HaveCount(userComments.Count);

        actual.Select(u => u.User.UserGuid).Should().Equal(users.Select(u => u.UserGuid).OrderBy(u => u).ToList());
    }
    
    [Fact]
    public async Task HappyPath_2TempFiles()
    {
        var users = await RowGenerator.GenerateUsers(10_000).ToListAsync();
        var userComments = await RowGenerator.GenerateUserComments(10_000, users).ToListAsync();

        var actual = await users.ToAsyncList()
            .GroupJoinExternal(userComments.ToAsyncList(), u => u.UserGuid, uc => uc.UserGuid, (u, ucs) => new
            {
                User = u,
                UserComments = ucs.ToList()
            })
            .OptimiseFor(calculateInnerBytesInRam: u => u.CalculateSize() * 100 /* pretend these use more RAM */,
                calculateOuterBytesInRam: u => u.CalculateSize() * 100 /* pretend these use more RAM */)
            .ToListAsync();

        actual.Should().HaveCount(users.Count);
        actual.SelectMany(u => u.UserComments).Should().HaveCount(userComments.Count);

        actual.Select(u => u.User.UserGuid).Should().Equal(users.Select(u => u.UserGuid).OrderBy(u => u).ToList());
    }

    
    
    
}


/*
 * * GroupJoin (left outer join) 
 * * ExceptBy (not in)
 * IntersectBy (inner join)
 * not sure how: (full outer join). possible if both have unique keys
 * DistinctBy
 * GroupBy
 * Implicit scalars: string, "where T : IBinaryInteger<T>", Guid
*/