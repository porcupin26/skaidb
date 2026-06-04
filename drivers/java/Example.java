import com.skaidb.Skaidb;

// Compile & run:
//   javac -d out src/main/java/com/skaidb/Skaidb.java Example.java
//   java -cp out Example [host] [port] [user] [password]
public class Example {
    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 7000;
        String user = args.length > 2 ? args[2] : "anonymous";
        String pass = args.length > 3 ? args[3] : "";

        try (Skaidb.Connection conn = Skaidb.connect(host, port, user, pass)) {
            conn.execute("CREATE TABLE people (PRIMARY KEY (id))");

            try (Skaidb.Query q = conn.prepare("INSERT INTO people (id, name, age) VALUES (?, ?, ?)")) {
                q.setInt(1, 1).setString(2, "Ada").setInt(3, 36).executeUpdate();
            }
            conn.prepare("INSERT INTO people (id, name, age) VALUES (?, ?, ?)")
                .setInt(1, 2).setString(2, "Linus").setInt(3, 54).executeUpdate();

            Skaidb.ResultSet rs = conn.prepare("SELECT id, name, age FROM people WHERE age > ?")
                                      .setInt(1, 40).executeQuery();
            while (rs.next()) {
                System.out.println(rs.getInt("id") + "  " + rs.getString("name") + "  " + rs.getInt("age"));
            }

            conn.execute("DROP TABLE people");
        }
    }
}
