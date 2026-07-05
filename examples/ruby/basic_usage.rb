# frozen_string_literal: true

# How to use skaidb from Ruby — modeled on the `pg` gem.
#
#   ruby basic_usage.rb [host] [port] [user] [password]
#
# Uses the driver at ../../drivers/ruby/lib. In a real project, add the gem
# (or vendor it) instead of the $LOAD_PATH hack below, which exists only so
# this example runs straight out of the repo.
$LOAD_PATH.unshift(File.expand_path("../../drivers/ruby/lib", __dir__))
require "skaidb"

host = ARGV[0] || "localhost"
port = (ARGV[1] || 7000).to_i
user = ARGV[2] || "anonymous"
pw   = ARGV[3] || ""

Skaidb.connect(host: host, port: port, user: user, password: pw) do |conn|
  # --- DDL ---
  conn.exec("DROP TABLE IF EXISTS people")
  conn.exec("CREATE TABLE people (PRIMARY KEY (id))")

  # --- Batch insert with bound parameters ($1, $2, ... like the pg gem) ---
  [[1, "Ada", 36], [2, "Linus", 54], [3, "Margaret", 80]].each do |id, name, age|
    conn.exec_params("INSERT INTO people (id, name, age) VALUES ($1, $2, $3)", [id, name, age])
  end

  # --- Query ---
  res = conn.exec_params("SELECT id, name, age FROM people WHERE age > $1 ORDER BY id", [40])
  puts "age > 40:"
  res.each { |row| puts "  #{row}" } # each row as a Hash keyed by column name

  # --- Update ---
  upd = conn.exec_params("UPDATE people SET age = $1 WHERE id = $2", [37, 1])
  puts "updated #{upd.cmd_tuples} row(s)"

  # --- Point read by primary key ---
  one = conn.exec_params("SELECT name, age FROM people WHERE id = $1", [1])
  puts "id=1: #{one[0]}"

  # --- Error handling ---
  begin
    conn.exec("SELECT * FROM does_not_exist")
  rescue Skaidb::Error => e
    puts "expected error: #{e.message}"
  end

  # --- Delete + cleanup ---
  del = conn.exec_params("DELETE FROM people WHERE id = $1", [2])
  puts "deleted #{del.cmd_tuples} row(s)"
  conn.exec("DROP TABLE people")
end
