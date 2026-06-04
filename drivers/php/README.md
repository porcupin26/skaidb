# skaidb — PHP driver

A **pure-PHP** client for skaidb with a **PDO-shaped** API. If you've used
`PDO`/`PDOStatement`, you already know this driver — `prepare()`, `execute()`,
`fetch()`, `fetchAll()`, `rowCount()`. No Composer or PECL dependencies (just
the bundled `hash` extension). Targets PHP 8.0+.

> A *real* PDO driver needs a C extension; this is a pure-PHP library that
> mirrors the PDO shape so the learning curve is near zero.

## Install

```sh
composer require skaidb/skaidb        # from a registry/path repo
# or just require the single file:
require '/path/to/drivers/php/src/Skaidb.php';
```

## Use

```php
<?php
require 'src/Skaidb.php';

use Skaidb\Connection;

$db = new Connection('localhost', 7000, 'skaidb', 'secret');

$db->exec('CREATE TABLE users (PRIMARY KEY (id))');

$stmt = $db->prepare('INSERT INTO users (id, name) VALUES (?, ?)');
$stmt->execute([1, 'Ada']);

$stmt = $db->prepare('SELECT id, name FROM users WHERE id = ?');
$stmt->execute([1]);
print_r($stmt->fetch());          // ['id' => 1, 'name' => 'Ada']

$db->close();
```

- **Placeholders** use `?` (PDO's default positional style). Parameters passed
  to `execute([...])` are safely quoted/escaped **client-side**, so values like
  `"O'Brien"` just work — the wire protocol has no server-side bind params.
- The `Connection` constructor runs the SCRAM-SHA-256 handshake automatically.
  Omit `user`/`password` (or use `anonymous` / `''`) for a server with auth
  disabled.

## API

```php
use Skaidb\Connection;
use Skaidb\Statement;
use Skaidb\SkaidbException;

// Connection
new Connection(string $host = 'localhost', int $port = 7000,
               string $user = 'anonymous', string $password = '',
               int|string $consistency = 'QUORUM', float $timeout = 10.0)
$db->prepare(string $sql): Statement
$db->query(string $sql): Statement      // no-param convenience (PDO::query)
$db->exec(string $sql): int             // returns affected rows (PDO::exec)
$db->setConsistency(int|string $c): void
$db->close(): void

// Statement
$stmt->execute(array $params = []): bool
$stmt->fetch(): ?array                  // one assoc row, or null
$stmt->fetchAll(): array                // array of assoc rows
$stmt->fetchColumn(int $i = 0): mixed   // one column, or false when exhausted
$stmt->rowCount(): int                  // affected (mutations) / row count (SELECT)
$stmt->columnCount(): int
$stmt->columns(): array                 // column names
$stmt->setConsistency(int|string $c): static
```

Every error — connect failure, auth denial, or a server statement error —
throws `Skaidb\SkaidbException` (modelled on `PDOException`).

### Consistency

skaidb is leaderless with tunable consistency. Default is `QUORUM`:

```php
$db = new Connection(/* ... */, consistency: 'ONE');   // or 'QUORUM' / 'ALL', or 0 / 1 / 2
$stmt->setConsistency('ALL');                          // per-statement override
```

### Types

| skaidb     | PHP                                            |
|------------|------------------------------------------------|
| Null       | `null`                                         |
| Bool       | `bool`                                         |
| Int        | `int` (64-bit on 64-bit builds)                |
| Float      | `float`                                        |
| Decimal    | `string` (exact)                               |
| String     | `string`                                       |
| Bytes      | `string` (binary)                              |
| Uuid       | `string` (canonical lowercase 8-4-4-4-12)      |
| Timestamp  | `\DateTimeImmutable` (UTC)                      |
| Array      | `array` (list)                                 |
| Document   | `array` (associative, insertion order)         |

Notes:

- **Decimal** is surfaced as an exact decimal **string** to avoid precision
  loss. Full 128-bit mantissas are exact when `ext-bcmath` or `ext-gmp` is
  installed; without either, mantissas beyond 64 bits may be approximate
  (the common case fits in 64 bits and is always exact).
- **Timestamp** decodes to a `\DateTimeImmutable` in UTC. When binding a
  `\DateTimeInterface` parameter it is sent as Unix **milliseconds**.

> skaidb auto-commits each statement (non-transactional). There are no
> multi-statement transactions, so there is no `beginTransaction`/`commit`.

## Run the example

```sh
php example.php 192.168.7.117 7000 skaidb secret
```
