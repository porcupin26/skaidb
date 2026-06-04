// Minimal end-to-end example for the skaidb C# driver.
//
//   dotnet run --project drivers/dotnet/example -- 192.168.7.117 7000 skaidb secret
//
// args: [0]=host [1]=port [2]=user [3]=password (all optional).

using System;
using System.Globalization;
using Skaidb;

string host = args.Length > 0 ? args[0] : "localhost";
int port = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 7000;
string user = args.Length > 2 ? args[2] : "anonymous";
string password = args.Length > 3 ? args[3] : "";

string connString =
    $"Host={host};Port={port};User={user};Password={password};Consistency=Quorum";

using var conn = new SkaidbConnection(connString);
conn.Open();
Console.WriteLine($"Connected to {host}:{port} as {user}.");

// DDL: create a table.
using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "CREATE TABLE people (PRIMARY KEY (id))";
    cmd.ExecuteNonQuery();
    Console.WriteLine("Created table people.");
}

// Insert a few rows with positional ? parameters.
var seed = new (long Id, string Name, long Age)[]
{
    (1, "Ada", 36),
    (2, "Grace", 45),
    (3, "O'Brien", 52), // apostrophe — exercises SQL quote-escaping
};
foreach (var (id, name, age) in seed)
{
    using var cmd = conn.CreateCommand();
    cmd.CommandText = "INSERT INTO people (id, name, age) VALUES (?, ?, ?)";
    cmd.Parameters.Add(id);
    cmd.Parameters.Add(name);
    cmd.Parameters.Add(age);
    int affected = cmd.ExecuteNonQuery();
    Console.WriteLine($"Inserted {name} (affected={affected}).");
}

// Query with a ? parameter and read rows via the reader.
using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "SELECT id, name, age FROM people WHERE age >= ?";
    cmd.Parameters.Add(40L);

    using var reader = cmd.ExecuteReader();
    Console.WriteLine($"Columns: {reader.FieldCount}");
    while (reader.Read())
    {
        long id = reader.GetInt64(0);
        string name = reader.GetString(1);
        long age = reader.IsDBNull(2) ? -1 : reader.GetInt64(2);
        Console.WriteLine($"  {id}  {name}  {age}");
    }
}

// ExecuteScalar example.
using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "SELECT name FROM people WHERE id = ?";
    cmd.Parameters.Add(3L);
    object? name = cmd.ExecuteScalar();
    Console.WriteLine($"Scalar lookup id=3 -> {name}");
}

// Clean up.
using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "DROP TABLE people";
    cmd.ExecuteNonQuery();
    Console.WriteLine("Dropped table people.");
}

Console.WriteLine("Done.");
