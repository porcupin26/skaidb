// Runnable example: go run . "skaidb://user:pass@host:7000/?consistency=quorum"
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

	must(db.Exec("CREATE TABLE people (PRIMARY KEY (id))"))
	for _, p := range []struct {
		id, age int
		name    string
	}{{1, 36, "Ada"}, {2, 54, "Linus"}, {3, 80, "Margaret"}} {
		must(db.Exec("INSERT INTO people (id, name, age) VALUES (?, ?, ?)", p.id, p.name, p.age))
	}

	rows, err := db.Query("SELECT id, name, age FROM people WHERE age > ?", 40)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, age int
		var name string
		if err := rows.Scan(&id, &name, &age); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%d  %-10s %d\n", id, name, age)
	}

	must(db.Exec("DROP TABLE people"))
}

func must(_ sql.Result, err error) {
	if err != nil {
		log.Fatal(err)
	}
}
