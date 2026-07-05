// How to use skaidb from Go — a database/sql driver, so it works with the
// standard library's Query/Exec/Scan idioms.
//
//   go run . "skaidb://user:pass@host:7000/?consistency=quorum"
//
// Uses the driver at ../../drivers/go via a `replace` in go.mod (this
// example lives outside that module). In a real project, `go get` the
// driver's real module path instead.
package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/porcupin26/skaidb/drivers/go"
)

func main() {
	dsn := "skaidb://localhost:7000/"
	if len(os.Args) > 1 {
		dsn = os.Args[1]
	}

	db, err := sql.Open("skaidb", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	must(db.Exec("DROP TABLE IF EXISTS people"))
	must(db.Exec("CREATE TABLE people (PRIMARY KEY (id))"))

	// Batch insert with bound parameters (`?`).
	for _, p := range []struct {
		id, age int
		name    string
	}{{1, 36, "Ada"}, {2, 54, "Linus"}, {3, 80, "Margaret"}} {
		must(db.Exec("INSERT INTO people (id, name, age) VALUES (?, ?, ?)", p.id, p.name, p.age))
	}

	// Query.
	rows, err := db.Query("SELECT id, name, age FROM people WHERE age > ? ORDER BY id", 40)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("age > 40:")
	for rows.Next() {
		var id, age int
		var name string
		if err := rows.Scan(&id, &name, &age); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  %d %s %d\n", id, name, age)
	}
	rows.Close()

	// Update.
	res := must(db.Exec("UPDATE people SET age = ? WHERE id = ?", 37, 1))
	n, _ := res.RowsAffected()
	fmt.Printf("updated %d row(s)\n", n)

	// Point read by primary key.
	var name string
	var age int
	if err := db.QueryRow("SELECT name, age FROM people WHERE id = ?", 1).Scan(&name, &age); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("id=1: %s %d\n", name, age)

	// Error handling: a database error surfaces as a normal Go error.
	if _, err := db.Exec("SELECT * FROM does_not_exist"); err != nil {
		fmt.Println("expected error:", err)
	}

	// Delete + cleanup.
	res = must(db.Exec("DELETE FROM people WHERE id = ?", 2))
	n, _ = res.RowsAffected()
	fmt.Printf("deleted %d row(s)\n", n)
	must(db.Exec("DROP TABLE people"))
}

func must(res sql.Result, err error) sql.Result {
	if err != nil {
		log.Fatal(err)
	}
	return res
}
