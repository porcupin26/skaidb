# skaidb — C# / .NET driver

An [ADO.NET](https://learn.microsoft.com/dotnet/framework/data/adonet/)-shaped
driver. If you've used `SqlConnection`/`SqlCommand`/`SqlDataReader`, you already
know this API — just with concrete `Skaidb*` types. **Pure BCL** — no NuGet
dependencies. Targets `net6.0`+.

## Install

```sh
# Add a project reference to the driver from the repo:
dotnet add reference path/to/drivers/dotnet/Skaidb.csproj
# or just drop Skaidb.cs into your project.
```

## Use

```csharp
using Skaidb;

using var conn = new SkaidbConnection(
    "Host=localhost;Port=7000;User=skaidb;Password=secret;Consistency=Quorum");
conn.Open();   // runs the SCRAM-SHA-256 handshake

using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "CREATE TABLE users (PRIMARY KEY (id))";
    cmd.ExecuteNonQuery();
}

using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "INSERT INTO users (id, name) VALUES (?, ?)";
    cmd.Parameters.Add(1);
    cmd.Parameters.Add("Ada");
    cmd.ExecuteNonQuery();   // affected rows
}

using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "SELECT id, name FROM users WHERE id = ?";
    cmd.Parameters.Add(1);
    using var reader = cmd.ExecuteReader();
    while (reader.Read())
        Console.WriteLine($"{reader.GetInt64(0)} {reader.GetString(1)}");
}
```

- **Placeholders** use `?`, bound positionally from `cmd.Parameters` (add values
  in order). Parameters are safely quoted client-side, so `"O'Brien"` just works.
- You can also construct with named arguments:
  `new SkaidbConnection(host: "localhost", port: 7000, user: "skaidb", password: "secret")`.
- `Open()` is idempotent and runs the SCRAM-SHA-256 handshake. Omit
  `User`/`Password` (or use `User=anonymous`) for a server with auth disabled.
- `SkaidbConnection`, `SkaidbCommand`, and `SkaidbDataReader` are all
  `IDisposable` — wrap them in `using`.

### API surface

| Type | Members |
|------|---------|
| `SkaidbConnection` | `Open()`, `Close()`, `Dispose()`, `CreateCommand()`, `Consistency`, `Timeout`, `IsOpen` |
| `SkaidbCommand` | `CommandText`, `Parameters` (`List<object?>`), `Consistency`, `ExecuteReader()`, `ExecuteNonQuery()` (int affected), `ExecuteScalar()` |
| `SkaidbDataReader` | `Read()`, `FieldCount`, `GetName(i)`, `GetOrdinal(name)`, `IsDBNull(i)`, `GetValue(i)`, `GetBoolean/GetInt32/GetInt64/GetDouble/GetDecimal/GetString/GetGuid/GetBytes/GetDateTimeOffset(i)`, `this[int]`, `this[string]` |
| `SkaidbException` | thrown on driver and server (query) errors |

### Connection string

Parsed case-insensitively; `;`-separated `Key=Value` pairs:

| Key | Aliases | Default |
|-----|---------|---------|
| `Host` | `Server`, `Data Source` | `localhost` |
| `Port` | | `7000` |
| `User` | `Username`, `User Id`, `Uid` | `anonymous` |
| `Password` | `Pwd` | (empty) |
| `Consistency` | | `Quorum` |
| `Timeout` | `Connect Timeout` | `10` (seconds) |

### Consistency

skaidb is leaderless with tunable consistency. Default is `Quorum`:

```csharp
// connection string: Consistency=One | Quorum | All
conn.Consistency = SkaidbConsistency.All;     // connection default
cmd.Consistency  = SkaidbConsistency.One;     // per-command override
```

### Types

Values returned by `GetValue(i)` (and the typed getters) map as follows:

| skaidb     | .NET                                   |
|------------|----------------------------------------|
| Null       | `null` (`DBNull.Value` from `GetValue`) |
| Bool       | `bool`                                 |
| Int        | `long`                                 |
| Float      | `double`                               |
| Decimal    | `decimal` (falls back to `string` if it overflows `System.Decimal`) |
| String     | `string`                               |
| Bytes      | `byte[]`                               |
| Uuid       | `System.Guid`                          |
| Timestamp  | `System.DateTimeOffset` (UTC)          |
| Array      | `object?[]`                            |
| Document   | `Dictionary<string, object?>` (insertion order preserved) |

Parameter binding accepts `bool`, all integer types, `float`/`double`
(NaN/Infinity rejected), `decimal`, `BigInteger`, `string`/`char`, `byte[]`
(hex literal), `Guid`, `DateTime`/`DateTimeOffset` (unix milliseconds), and
`null`/`DBNull` (→ `NULL`).

> skaidb auto-commits each statement (non-transactional). There is no
> multi-statement transaction.

## Run the example

```sh
dotnet run --project drivers/dotnet/example -- 192.168.7.117 7000 skaidb secret
```
