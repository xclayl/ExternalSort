using Parquet.Serialization.Attributes;

namespace ExternalSort.Test;

public class RowGenerator
{
    public static async IAsyncEnumerable<User> GenerateUsers(int count)
    {
        var userGenerator = new Faker<User>()
            .RuleFor(u => u.Firstname, f => f.Name.FirstName())
            .RuleFor(u => u.Lastname, f => f.Name.LastName())
            .RuleFor(u => u.Avatar, f => f.Internet.Avatar())
            .RuleFor(u => u.Username, (f, u) => f.Internet.UserName(u.Firstname, u.Lastname))
            .RuleFor(u => u.Email, (f, u) => $"{f.Internet.Email(u.Firstname, u.Lastname)} {f.UniqueIndex}")
            .RuleFor(u => u.SomethingUnique, f => $"Value {f.UniqueIndex}")
            .RuleFor(u => u.UserGuid, Guid.NewGuid);
        
        for (var i = 0; i < count; i++)
        {
            yield return userGenerator.Generate();
        }
    }
    
    public static async IAsyncEnumerable<UserComment> GenerateUserComments(int count, IReadOnlyList<User> users)
    {
        var userList = users.ToList();
        
        var userGenerator = new Faker<UserComment>()
            .RuleFor(u => u.CreatedAt, f => f.Date.Recent(100))
            .RuleFor(u => u.Comment, f => f.Lorem.Paragraph())
            .RuleFor(u => u.UserGuid, f => f.PickRandom(userList).UserGuid);
        
        for (var i = 0; i < count; i++)
        {
            yield return userGenerator.Generate();
        }
    }
}

public record User(string Firstname, string Lastname, string Username, string Email, string SomethingUnique, Guid UserGuid, string Avatar)
{
    public User() : this(String.Empty, String.Empty, String.Empty, String.Empty, String.Empty, Guid.Empty, String.Empty)
    {
    }

    public long CalculateSize()
        => Firstname.Length * 2
           + Lastname.Length * 2
           + Username.Length * 2
           + Email.Length * 2
           + SomethingUnique.Length * 2
           + 16
           + Avatar.Length * 2;

}


public record UserComment(DateTime CreatedAt, Guid UserGuid, string Comment)
{
    public UserComment() : this(DateTime.MinValue, Guid.Empty, String.Empty)
    {
    }

    public long CalculateSize()
        => 16
           + 16
           + Comment.Length * 2;

}