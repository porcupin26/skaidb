// How to use skaidb from Node.js / TypeScript — modeled on `pg` (node-postgres).
//
//   node basic_usage.js [host] [port] [user] [password]
//
// Uses the driver at ../../drivers/nodejs. In a real project, `npm install`
// it (or vendor it) instead of the relative require below, which exists only
// so this example runs straight out of the repo.
const { Client } = require('../../drivers/nodejs/skaidb');

const [host = 'localhost', port = '7000', user = 'anonymous', password = ''] = process.argv.slice(2);

(async () => {
  const client = new Client({ host, port: Number(port), user, password });
  await client.connect();

  // --- DDL ---
  await client.query('DROP TABLE IF EXISTS people');
  await client.query('CREATE TABLE people (PRIMARY KEY (id))');

  // --- Batch insert with bound parameters ($1, $2, ... like node-postgres) ---
  for (const [id, name, age] of [[1, 'Ada', 36], [2, 'Linus', 54], [3, 'Margaret', 80]]) {
    await client.query('INSERT INTO people (id, name, age) VALUES ($1, $2, $3)', [id, name, age]);
  }

  // --- Query ---
  const res = await client.query('SELECT id, name, age FROM people WHERE age > $1 ORDER BY id', [40]);
  console.log('age > 40:', res.rows);

  // --- Update ---
  const upd = await client.query('UPDATE people SET age = $1 WHERE id = $2', [37, 1]);
  console.log(`updated ${upd.rowCount} row(s)`);

  // --- Point read by primary key ---
  const one = await client.query('SELECT name, age FROM people WHERE id = $1', [1]);
  console.log('id=1:', one.rows[0]);

  // --- Error handling ---
  try {
    await client.query('SELECT * FROM does_not_exist');
  } catch (e) {
    console.log('expected error:', e.message);
  }

  // --- Delete + cleanup ---
  const del = await client.query('DELETE FROM people WHERE id = $1', [2]);
  console.log(`deleted ${del.rowCount} row(s)`);
  await client.query('DROP TABLE people');

  await client.end();
})().catch((e) => { console.error(e); process.exit(1); });
