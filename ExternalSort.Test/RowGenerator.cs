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
            .RuleFor(u => u.Email, (f, u) => f.Internet.Email(u.Firstname, u.Lastname))
            .RuleFor(u => u.SomethingUnique, f => $"Value {f.UniqueIndex}")
            .RuleFor(u => u.SomeGuid, Guid.NewGuid);
        
        for (var i = 0; i < count; i++)
        {
            yield return userGenerator.Generate();
        }
    }
}

public record User(string Firstname, string Lastname, string Username, string Email, string SomethingUnique, Guid SomeGuid, string Avatar)
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
