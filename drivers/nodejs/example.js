// Runnable example: node example.js [host] [port] [user] [password]
const { Client } = require('./skaidb');

const [host = 'localhost', port = '7000', user = 'anonymous', password = ''] = process.argv.slice(2);

(async () => {
  const client = new Client({ host, port: Number(port), user, password });
  await client.connect();

  await client.query('CREATE TABLE people (PRIMARY KEY (id))');
  for (const [id, name, age] of [[1, 'Ada', 36], [2, 'Linus', 54], [3, 'Margaret', 80]]) {
    await client.query('INSERT INTO people (id, name, age) VALUES ($1, $2, $3)', [id, name, age]);
  }

  const res = await client.query('SELECT id, name, age FROM people WHERE age > $1', [40]);
  console.log(res.rows);          // [ { id: 2, name: 'Linus', age: 54 }, ... ]

  await client.query('DROP TABLE people');
  await client.end();
})().catch((e) => { console.error(e); process.exit(1); });
