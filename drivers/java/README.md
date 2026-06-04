# skaidb — Java driver

A JDBC-flavored client: `connect`, `prepare`, `setInt`/`setString`,
`executeQuery`/`executeUpdate`, and a `ResultSet` with `next()`/`getInt`/
`getString`. If you've used JDBC, this is the same shape. **Pure JDK** — no
dependencies, single source file.

## Build

No build tool required:

```sh
javac -d out src/main/java/com/skaidb/Skaidb.java
# add out/ to your classpath, or jar it:
jar cf skaidb.jar -C out .
```

(Or drop `src/main/java/com/skaidb/Skaidb.java` into your project.)

## Use

```java
import com.skaidb.Skaidb;

try (Skaidb.Connection conn = Skaidb.connect("skaidb://user:pass@localhost:7000")) {
    conn.execute("CREATE TABLE users (PRIMARY KEY (id))");

    try (Skaidb.Query q = conn.prepare("INSERT INTO users (id, name) VALUES (?, ?)")) {
        q.setInt(1, 1).setString(2, "Ada").executeUpdate();
    }

    Skaidb.ResultSet rs = conn.prepare("SELECT id, name FROM users WHERE id = ?")
                              .setInt(1, 1).executeQuery();
    while (rs.next()) {
        System.out.println(rs.getInt("id") + " " + rs.getString("name"));
    }
}
```

- **Placeholders** use `?` with 1-based `setX(index, value)`, exactly like JDBC
  `PreparedStatement`. Parameters are quoted safely client-side.
- `connect(String dsn)` accepts `skaidb://user:pass@host:port/?consistency=quorum`;
  `connect(host, port, user, password)` is the explicit overload.
- `Connection`, `Query`, and `ResultSet` mirror JDBC ergonomics without the full
  `java.sql.*` surface. Errors throw `Skaidb.SkaidbException` (unchecked).
- skaidb is non-transactional — every statement auto-commits.

### Consistency

```java
conn.setConsistency(Skaidb.CONSISTENCY_ONE);   // or _QUORUM (default) / _ALL
```

### Types (`ResultSet` getters / `getObject`)

| skaidb     | Java                         |
|------------|------------------------------|
| Null       | `null`                       |
| Bool       | `Boolean`                    |
| Int        | `Long`                       |
| Float      | `Double`                     |
| Decimal    | `java.math.BigDecimal`       |
| String     | `String`                     |
| Bytes      | `byte[]`                     |
| Uuid       | `java.util.UUID`             |
| Timestamp  | `java.time.Instant`          |
| Array      | `java.util.List`             |
| Document   | `java.util.LinkedHashMap`    |

`getInt`/`getLong`/`getDouble` coerce numeric values; `getString` works on any
non-null value.

## Run the example

```sh
javac -d out src/main/java/com/skaidb/Skaidb.java Example.java
java -cp out Example 192.168.7.117 7000 skaidb secret
```
