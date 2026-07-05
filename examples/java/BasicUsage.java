import com.skaidb.Skaidb;

/**
 * How to use skaidb from Java — modeled on JDBC.
 *
 * Compile & run (from this directory):
 *   javac -d out ../../drivers/java/src/main/java/com/skaidb/Skaidb.java BasicUsage.java
 *   java -cp out BasicUsage [host] [port] [user] [password]
 */
public class BasicUsage {
    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 7000;
        String user = args.length > 2 ? args[2] : "anonymous";
        String pass = args.length > 3 ? args[3] : "";

        try (Skaidb.Connection conn = Skaidb.connect(host, port, user, pass)) {
            // --- DDL ---
            conn.execute("DROP TABLE IF EXISTS people");
            conn.execute("CREATE TABLE people (PRIMARY KEY (id))");

            // --- Prepared statement: parse once, bind (1-based, JDBC-style) per row ---
            try (Skaidb.Query ins = conn.prepare("INSERT INTO people (id, name, age) VALUES (?, ?, ?)")) {
                ins.setInt(1, 1).setString(2, "Ada").setInt(3, 36).executeUpdate();
                ins.setInt(1, 2).setString(2, "Linus").setInt(3, 54).executeUpdate();
                ins.setInt(1, 3).setString(2, "Margaret").setInt(3, 80).executeUpdate();
            }

            // --- Query ---
            System.out.println("age > 40:");
            try (Skaidb.Query q = conn.prepare("SELECT id, name, age FROM people WHERE age > ? ORDER BY id")) {
                Skaidb.ResultSet rs = q.setInt(1, 40).executeQuery();
                while (rs.next()) {
                    System.out.println("  " + rs.getObject("id") + " " + rs.getObject("name") + " " + rs.getObject("age"));
                }
            }

            // --- Update ---
            long updated;
            try (Skaidb.Query q = conn.prepare("UPDATE people SET age = ? WHERE id = ?")) {
                updated = q.setInt(1, 37).setInt(2, 1).executeUpdate();
            }
            System.out.println("updated " + updated + " row(s)");

            // --- Point read by primary key ---
            try (Skaidb.Query q = conn.prepare("SELECT name, age FROM people WHERE id = ?")) {
                Skaidb.ResultSet rs = q.setInt(1, 1).executeQuery();
                rs.next();
                System.out.println("id=1: " + rs.getObject("name") + " " + rs.getObject("age"));
            }

            // --- Error handling ---
            try {
                conn.execute("SELECT * FROM does_not_exist");
            } catch (Skaidb.SkaidbException e) {
                System.out.println("expected error: " + e.getMessage());
            }

            // --- Delete + cleanup ---
            long deleted;
            try (Skaidb.Query q = conn.prepare("DELETE FROM people WHERE id = ?")) {
                deleted = q.setInt(1, 2).executeUpdate();
            }
            System.out.println("deleted " + deleted + " row(s)");
            conn.execute("DROP TABLE people");
        }
    }
}
