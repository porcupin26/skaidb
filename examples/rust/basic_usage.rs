//! How to use skaidb from Rust — the native, in-tree driver.
//!
//!   cargo run --bin basic_usage -- [host:port] [user] [password]
//!
//! Uses `skaidb-driver` via a path dependency (this example lives outside
//! the workspace). In your own project, depend on the published crate.

use skaidb_driver::Client;
use skaidb_proto::Response;
use skaidb_types::Value;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let addr = args.get(1).map(String::as_str).unwrap_or("localhost:7000");
    let user = args.get(2).map(String::as_str).unwrap_or("anonymous");
    let pass = args.get(3).map(String::as_str).unwrap_or("");

    let mut client = Client::connect_with(addr, user, pass).expect("connect");

    // --- DDL ---
    client.execute("DROP TABLE IF EXISTS people").unwrap();
    client.execute("CREATE TABLE people (PRIMARY KEY (id))").unwrap();

    // --- Prepared statement: parse once on the server, bind per row ---
    let mut insert = client
        .prepare("INSERT INTO people (id, name, age) VALUES (?, ?, ?)")
        .expect("prepare");
    for (id, name, age) in [(1, "Ada", 36), (2, "Linus", 54), (3, "Margaret", 80)] {
        client
            .execute_prepared(
                &mut insert,
                &[Value::Int(id), Value::String(name.into()), Value::Int(age)],
            )
            .unwrap();
    }

    // --- Query (one-shot SQL text) ---
    match client
        .execute("SELECT id, name, age FROM people WHERE age > 40 ORDER BY id")
        .unwrap()
    {
        Response::Rows { rows, .. } => {
            println!("age > 40:");
            for row in rows {
                println!("  {row:?}");
            }
        }
        other => panic!("expected rows, got {other:?}"),
    }

    // --- Update, via a prepared statement ---
    let mut update = client.prepare("UPDATE people SET age = ? WHERE id = ?").unwrap();
    match client
        .execute_prepared(&mut update, &[Value::Int(37), Value::Int(1)])
        .unwrap()
    {
        Response::Mutation { affected } => println!("updated {affected} row(s)"),
        other => panic!("expected a mutation, got {other:?}"),
    }

    // --- Point read by primary key ---
    match client.execute("SELECT name, age FROM people WHERE id = 1").unwrap() {
        Response::Rows { rows, .. } => println!("id=1: {:?}", rows[0]),
        other => panic!("expected rows, got {other:?}"),
    }

    // --- Error handling ---
    if let Err(e) = client.execute("SELECT * FROM does_not_exist") {
        println!("expected error: {e}");
    }

    // --- Delete + cleanup ---
    match client.execute("DELETE FROM people WHERE id = 2").unwrap() {
        Response::Mutation { affected } => println!("deleted {affected} row(s)"),
        other => panic!("expected a mutation, got {other:?}"),
    }
    client.execute("DROP TABLE people").unwrap();
}
