# skaidb — Node.js / TypeScript driver

API modeled on [node-postgres (`pg`)](https://node-postgres.com/). If you've
used `pg`, this is the same shape: `new Client(...)`, `await client.query(...)`,
`{ rows, rowCount, fields }`. **Zero dependencies** — pure `net` + `crypto`.
Ships with TypeScript types.

## Install

```sh
npm install ./drivers/nodejs      # from the repo
# or copy skaidb.js (+ skaidb.d.ts) into your project
```

## Use

```js
const { Client } = require('skaidb');

const client = new Client({
  host: 'localhost', port: 7000,
  user: 'skaidb', password: 'secret',
});
await client.connect();

await client.query('CREATE TABLE users (PRIMARY KEY (id))');
await client.query('INSERT INTO users (id, name) VALUES ($1, $2)', [1, 'Ada']);

const res = await client.query('SELECT id, name FROM users WHERE id = $1', [1]);
console.log(res.rows);      // [ { id: 1, name: 'Ada' } ]
console.log(res.rowCount);  // 1

await client.end();
```

TypeScript works out of the box:

```ts
import { Client, QueryResult } from 'skaidb';
const client = new Client({ host, port, user, password });
```

- **Placeholders** use `$1, $2, …` (pg style). Parameters are safely quoted
  client-side, so `"O'Brien"` is fine.
- Rows are objects keyed by column name by default. Pass
  `{ text, rowMode: 'array' }` for array rows (like `pg`).
- Queries on one client are serialized (one request/response in flight); open
  multiple `Client`s for concurrency, or reconnect per worker.

### Consistency

```js
new Client({ ..., consistency: 'ONE' });                 // default 'QUORUM'
client.query({ text: 'SELECT ...', consistency: 'ALL' }); // per-query
```

### Types

| skaidb     | JavaScript                              |
|------------|-----------------------------------------|
| Null       | `null`                                  |
| Bool       | `boolean`                               |
| Int        | `number` (or `bigint` if > 2^53)        |
| Float      | `number`                                |
| Decimal    | `string` (exact)                        |
| String     | `string`                                |
| Bytes      | `Buffer`                                |
| Uuid       | `string` (canonical)                    |
| Timestamp  | `Date`                                  |
| Array      | `Array`                                 |
| Document   | `object`                                |

## Run the example

```sh
node example.js 192.168.7.117 7000 skaidb secret
```
