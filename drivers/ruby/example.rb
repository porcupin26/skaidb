# frozen_string_literal: true

# Runnable example for the skaidb Ruby driver.
#
#   ruby example.rb [host] [port] [user] [password]
$LOAD_PATH.unshift(File.expand_path("lib", __dir__))
require "skaidb"

host = ARGV[0] || "localhost"
port = (ARGV[1] || 7000).to_i
user = ARGV[2] || "anonymous"
pw   = ARGV[3] || ""

Skaidb.connect(host: host, port: port, user: user, password: pw) do |conn|
  conn.exec("CREATE TABLE people (PRIMARY KEY (id))")

  people = [[1, "Ada", 36], [2, "Linus", 54], [3, "Margaret", 80]]
  people.each do |id, name, age|
    conn.exec_params(
      "INSERT INTO people (id, name, age) VALUES ($1, $2, $3)",
      [id, name, age]
    )
  end

  res = conn.exec_params("SELECT id, name, age FROM people WHERE age > $1", [40])
  puts "columns: #{res.fields.join(', ')}"
  res.each do |row|
    puts "#{row['id']}\t#{row['name']}\t#{row['age']}"
  end
  puts "(#{res.ntuples} rows)"

  conn.exec("DROP TABLE people")
end
