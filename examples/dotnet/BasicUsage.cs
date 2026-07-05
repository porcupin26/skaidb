// How to use skaidb from C# — an ADO.NET provider (DbConnection/DbCommand).
//
//   dotnet run --project examples/dotnet -- [host] [port] [user] [password]

using System;
using System.Globalization;
using Skaidb;

string host = args.Length > 0 ? args[0] : "localhost";
int port = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 7000;
string user = args.Length > 2 ? args[2] : "anonymous";
string password = args.Length > 3 ? args[3] : "";

string connString = $"Host={host};Port={port};User={user};Password={password};Consistency=Quorum";

using var conn = new SkaidbConnection(connString);
conn.Open();

// --- DDL ---
using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "DROP TABLE IF EXISTS people";
    cmd.ExecuteNonQuery();
}
using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "CREATE TABLE people (PRIMARY KEY (id))";
    cmd.ExecuteNonQuery();
}

// --- Batch insert with bound parameters (`?`) ---
var seed = new (long Id, string Name, long Age)[]
{
    (1, "Ada", 36),
    (2, "Linus", 54),
    (3, "Margaret", 80),
};
foreach (var (id, name, age) in seed)
{
    using var cmd = conn.CreateCommand();
    cmd.CommandText = "INSERT INTO people (id, name, age) VALUES (?, ?, ?)";
    cmd.Parameters.Add(id);
    cmd.Parameters.Add(name);
    cmd.Parameters.Add(age);
    cmd.ExecuteNonQuery();
}

// --- Query ---
Console.WriteLine("age > 40:");
using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "SELECT id, name, age FROM people WHERE age > ? ORDER BY id";
    cmd.Parameters.Add(40L);
    using var reader = cmd.ExecuteReader();
    while (reader.Read())
    {
        Console.WriteLine($"  {reader.GetInt64(0)}  {reader.GetString(1)}  {reader.GetInt64(2)}");
    }
}

// --- Update ---
int updated;
using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "UPDATE people SET age = ? WHERE id = ?";
    cmd.Parameters.Add(37L);
    cmd.Parameters.Add(1L);
    updated = cmd.ExecuteNonQuery();
}
Console.WriteLine($"updated {updated} row(s)");

// --- Point read by primary key ---
using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "SELECT name, age FROM people WHERE id = ?";
    cmd.Parameters.Add(1L);
    using var reader = cmd.ExecuteReader();
    reader.Read();
    Console.WriteLine($"id=1: {reader.GetString(0)} {reader.GetInt64(1)}");
}

// --- Error handling ---
try
{
    using var cmd = conn.CreateCommand();
    cmd.CommandText = "SELECT * FROM does_not_exist";
    cmd.ExecuteNonQuery();
}
catch (SkaidbException e)
{
    Console.WriteLine($"expected error: {e.Message}");
}

// --- Delete + cleanup ---
int deleted;
using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "DELETE FROM people WHERE id = ?";
    cmd.Parameters.Add(2L);
    deleted = cmd.ExecuteNonQuery();
}
Console.WriteLine($"deleted {deleted} row(s)");
using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "DROP TABLE people";
    cmd.ExecuteNonQuery();
}
