# frozen_string_literal: true

# skaidb — Ruby driver.
#
# A small, dependency-free client for skaidb's binary wire protocol. The API is
# modelled on the {https://github.com/ged/ruby-pg ruby-pg} gem (`PG`), so if you
# have used Postgres from Ruby it should feel familiar: +Skaidb.connect+ returns
# a connection, +exec+ / +exec_params+ run statements, and the result behaves
# like +PG::Result+ (Enumerable of row Hashes, plus +rows+, +fields+,
# +ntuples+, +cmd_tuples+).
#
# Pure standard library — only +socket+, +openssl+, +securerandom+ and
# +bigdecimal+. No gems.
#
#   require "skaidb"
#
#   conn = Skaidb.connect(host: "localhost", port: 7000,
#                         user: "skaidb", password: "secret")
#   conn.exec("CREATE TABLE users (PRIMARY KEY (id))")
#   conn.exec_params("INSERT INTO users (id, name) VALUES ($1, $2)", [1, "Ada"])
#   res = conn.exec_params("SELECT id, name FROM users WHERE id = $1", [1])
#   res.each { |row| puts row["name"] }    # => "Ada"
#   conn.close
#
# Placeholders use the pg-style +$1+, +$2+, ... and are interpolated into the
# SQL client-side with correct quoting (the protocol has no server-side bind
# parameters).
require "socket"
require "openssl"
require "securerandom"
require "bigdecimal"

module Skaidb
  VERSION = "0.1.0"

  # Base class for every error raised by this driver.
  class Error < StandardError; end

  # A connection / transport problem (socket, framing, handshake). When this is
  # raised mid-stream the connection is dead and must be discarded.
  class ConnectionError < Error; end

  # A statement failed (bad SQL, constraint violation, ...). The connection
  # stays usable for the next query.
  class QueryError < Error; end

  # Consistency levels — how many replicas must acknowledge/answer.
  module Consistency
    ONE = 0
    QUORUM = 1
    ALL = 2

    BY_NAME = { "one" => 0, "quorum" => 1, "all" => 2 }.freeze

    # Resolve a symbol/string/integer into the 0/1/2 wire value.
    def self.resolve(value)
      case value
      when Integer
        return value if [0, 1, 2].include?(value)

        raise Error, "invalid consistency #{value.inspect}"
      when String, Symbol
        v = BY_NAME[value.to_s.downcase]
        return v if v

        raise Error, "invalid consistency #{value.inspect}"
      else
        raise Error, "invalid consistency #{value.inspect}"
      end
    end
  end

  # Value type tags (§4 of PROTOCOL.md).
  module Tags
    NULL = 0
    BOOL = 1
    INT = 2
    FLOAT = 3
    DECIMAL = 4
    STRING = 5
    BYTES = 6
    UUID = 7
    TIMESTAMP = 8
    ARRAY = 9
    DOCUMENT = 10
  end

  # ---- byte reader ---------------------------------------------------------
  #
  # All integers here are little-endian (the frame length prefix, handled in
  # Connection, is the sole big-endian field).
  class Reader
    def initialize(buf)
      @buf = buf
      @pos = 0
    end

    def take(n)
      raise ConnectionError, "truncated server message" if @pos + n > @buf.bytesize

      s = @buf.byteslice(@pos, n)
      @pos += n
      s
    end

    def u8
      take(1).unpack1("C")
    end

    def u32
      take(4).unpack1("V")
    end

    def i64
      take(8).unpack1("q<")
    end

    def u64
      take(8).unpack1("Q<")
    end

    def f64
      take(8).unpack1("E")
    end

    # signed little-endian 128-bit integer (16 bytes), assembled manually.
    def i128
      bytes = take(16)
      lo = bytes.byteslice(0, 8).unpack1("Q<")
      hi = bytes.byteslice(8, 8).unpack1("q<") # high half is signed
      (hi << 64) | lo
    end

    def blob
      take(u32)
    end

    def text
      blob.force_encoding("UTF-8")
    end
  end

  # Decode one self-describing Value (§4) from the reader.
  def self.decode_value(r)
    tag = r.u8
    case tag
    when Tags::NULL then nil
    when Tags::BOOL then r.u8 != 0
    when Tags::INT then r.i64
    when Tags::FLOAT then r.f64
    when Tags::DECIMAL
      mantissa = r.i128
      scale = r.u32
      # value = mantissa / 10^scale, built exactly via BigDecimal("<mantissa>e-<scale>")
      BigDecimal("#{mantissa}e-#{scale}")
    when Tags::STRING then r.text
    when Tags::BYTES then r.blob.force_encoding("BINARY")
    when Tags::UUID then format_uuid(r.take(16))
    when Tags::TIMESTAMP
      ms = r.i64
      # preserve millisecond precision; Time in UTC
      Time.at(ms / 1000, (ms % 1000) * 1000, :usec).utc
    when Tags::ARRAY
      Array.new(r.u32) { decode_value(r) }
    when Tags::DOCUMENT
      out = {}
      r.u32.times do
        key = r.text
        out[key] = decode_value(r)
      end
      out
    else
      raise ConnectionError, "unknown value tag #{tag}"
    end
  end

  # 16 raw bytes -> canonical lowercase 8-4-4-4-12 UUID string.
  def self.format_uuid(bytes)
    hex = bytes.unpack1("H*")
    "#{hex[0, 8]}-#{hex[8, 4]}-#{hex[12, 4]}-#{hex[16, 4]}-#{hex[20, 12]}"
  end

  # ---- client-side parameter binding (§5) ----------------------------------

  # Quote a single Ruby value into a SQL literal.
  def self.quote(arg)
    case arg
    when nil
      "NULL"
    when true
      "TRUE"
    when false
      "FALSE"
    when Integer
      arg.to_s
    when Float
      raise QueryError, "cannot bind NaN/Infinity" if arg.nan? || arg.infinite?

      # round-trip-safe representation
      arg.to_s
    when BigDecimal
      arg.to_s("F")
    when String
      if arg.encoding == Encoding::BINARY
        # raw binary bytes (ASCII-8BIT) -> hex literal, mirroring Python's bytes
        "'" + arg.unpack1("H*") + "'"
      else
        # text string -> single-quoted, doubling embedded quotes
        "'" + arg.gsub("'", "''") + "'"
      end
    when Symbol
      "'" + arg.to_s.gsub("'", "''") + "'"
    when Time
      ms = (arg.to_r * 1000).round
      ms.to_s
    else
      raise QueryError, "cannot bind value of type #{arg.class}"
    end
  end

  # Interpolate +params+ into +sql+, replacing $1, $2, ... placeholders that
  # appear outside single-quoted string literals.
  def self.bind(sql, params)
    params ||= []
    out = +""
    in_str = false
    i = 0
    n = sql.length
    max_used = 0
    while i < n
      ch = sql[i]
      if in_str
        out << ch
        if ch == "'"
          if i + 1 < n && sql[i + 1] == "'"
            out << "'"
            i += 2
            next
          end
          in_str = false
        end
        i += 1
        next
      end

      if ch == "'"
        in_str = true
        out << ch
        i += 1
        next
      end

      if ch == "$" && i + 1 < n && sql[i + 1] =~ /[0-9]/
        j = i + 1
        j += 1 while j < n && sql[j] =~ /[0-9]/
        idx = sql[(i + 1)...j].to_i
        raise QueryError, "invalid placeholder $0" if idx < 1
        raise QueryError, "placeholder $#{idx} has no parameter" if idx > params.length

        out << quote(params[idx - 1])
        max_used = idx if idx > max_used
        i = j
        next
      end

      out << ch
      i += 1
    end

    if params.length > max_used
      raise QueryError, "more parameters (#{params.length}) than placeholders ($#{max_used})"
    end

    out
  end

  # ---- SCRAM-SHA-256 (§2) --------------------------------------------------

  # Compute the client proof and the expected server signature.
  def self.scram(password, salt, iterations, auth_message)
    digest = OpenSSL::Digest::SHA256
    salted = OpenSSL::KDF.pbkdf2_hmac(
      password, salt: salt, iterations: iterations, length: 32, hash: "sha256"
    )
    client_key = OpenSSL::HMAC.digest(digest.new, salted, "Client Key")
    stored_key = OpenSSL::Digest::SHA256.digest(client_key)
    client_sig = OpenSSL::HMAC.digest(digest.new, stored_key, auth_message)
    proof = xor_bytes(client_key, client_sig)
    server_key = OpenSSL::HMAC.digest(digest.new, salted, "Server Key")
    server_sig = OpenSSL::HMAC.digest(digest.new, server_key, auth_message)
    [proof, server_sig]
  end

  def self.xor_bytes(a, b)
    a.bytes.zip(b.bytes).map { |x, y| x ^ y }.pack("C*")
  end

  # Constant-time comparison for the server signature.
  def self.secure_compare(a, b)
    return false unless a.bytesize == b.bytesize

    res = 0
    a.bytes.zip(b.bytes) { |x, y| res |= x ^ y }
    res.zero?
  end

  # encode a string field: u32 LE length + UTF-8 bytes
  def self.enc_str(str)
    b = str.to_s.dup.force_encoding("UTF-8")
    bytes = b.bytesize
    [bytes].pack("V") + b.b
  end

  # ---- Result --------------------------------------------------------------

  # A query result, shaped like +PG::Result+. Enumerable over row Hashes
  # (String column-name keys); also exposes positional +rows+, +fields+,
  # +ntuples+ and +cmd_tuples+.
  class Result
    include Enumerable

    # @return [Array<String>] column names, in order
    attr_reader :fields
    # @return [Array<Array>] rows as arrays of values
    attr_reader :rows
    # @return [Integer] number of rows affected by a mutation (0 otherwise)
    attr_reader :cmd_tuples

    def initialize(fields:, rows:, cmd_tuples: 0)
      @fields = fields
      @rows = rows
      @cmd_tuples = cmd_tuples
    end

    # pg alias for the column names.
    alias columns fields

    # @return [Integer] number of result rows
    def ntuples
      @rows.length
    end
    alias num_tuples ntuples

    # @return [Integer] number of columns
    def nfields
      @fields.length
    end
    alias num_fields nfields

    # Yield each row as a Hash keyed by column name (String keys).
    def each
      return enum_for(:each) unless block_given?

      @rows.each do |row|
        h = {}
        @fields.each_with_index { |name, idx| h[name] = row[idx] }
        yield h
      end
      self
    end

    # Row +i+ as a Hash keyed by column name.
    def [](i)
      row = @rows[i]
      return nil if row.nil?

      h = {}
      @fields.each_with_index { |name, idx| h[name] = row[idx] }
      h
    end

    # A single field value by row index and column (name or index).
    def getvalue(row, col)
      r = @rows[row]
      return nil if r.nil?

      col = @fields.index(col) if col.is_a?(String)
      col.nil? ? nil : r[col]
    end

    # All rows as Hashes.
    def values
      to_a
    end
  end

  # ---- Connection ----------------------------------------------------------

  @nonce_counter = 0
  @nonce_mutex = Mutex.new

  # Internal: a process-wide monotonic counter for client nonces.
  def self.next_nonce_id
    @nonce_mutex.synchronize { @nonce_counter += 1 }
  end

  # A connection to one skaidb node. Modelled on +PG::Connection+.
  class Connection
    # @return [Boolean] whether the connection has been closed
    attr_reader :closed

    def initialize(host:, port:, user:, password:, consistency:, timeout:)
      @consistency = Consistency.resolve(consistency)
      @mutex = Mutex.new
      @closed = false
      begin
        @sock = Socket.tcp(host, port, connect_timeout: timeout)
        @sock.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
      rescue StandardError => e
        raise ConnectionError, "connect failed: #{e.message}"
      end
      begin
        handshake(user.to_s, password.to_s)
      rescue ConnectionError
        close
        raise
      end
    end

    # Current default consistency level (0/1/2).
    attr_reader :consistency

    # Override the default consistency level for subsequent queries.
    def consistency=(value)
      @consistency = Consistency.resolve(value)
    end

    # Execute a statement with no parameters.
    # @return [Result]
    def exec(sql)
      run(sql.to_s, @consistency)
    end
    alias query exec

    # Execute a statement, interpolating +params+ at +$1+, +$2+, ... .
    # @param params [Array] positional parameters
    # @return [Result]
    def exec_params(sql, params = [], consistency: nil)
      bound = Skaidb.bind(sql.to_s, params)
      level = consistency.nil? ? @consistency : Consistency.resolve(consistency)
      run(bound, level)
    end

    # Close the connection. Idempotent.
    def close
      return if @closed

      @closed = true
      begin
        @sock&.close
      rescue StandardError
        # ignore — socket already gone
      end
      nil
    end

    def finished?
      @closed
    end

    private

    # -- framing --

    def write_frame(payload)
      payload = payload.b
      @sock.write([payload.bytesize].pack("N") + payload) # length is BE
    rescue StandardError => e
      raise ConnectionError, "write failed: #{e.message}"
    end

    def read_frame
      head = read_exact(4)
      length = head.unpack1("N") # BE
      read_exact(length)
    end

    def read_exact(n)
      return "".b if n.zero?

      buf = +""
      while buf.bytesize < n
        chunk = begin
          @sock.read(n - buf.bytesize)
        rescue StandardError => e
          raise ConnectionError, "read failed: #{e.message}"
        end
        raise ConnectionError, "connection closed by server" if chunk.nil? || chunk.empty?

        buf << chunk
      end
      buf
    end

    # -- handshake --

    def handshake(user, password)
      counter = Skaidb.next_nonce_id
      client_nonce = "rb#{Process.pid}.#{counter}.#{SecureRandom.hex(4)}"

      start = [10].pack("C") + Skaidb.enc_str(user) + Skaidb.enc_str(client_nonce)
      write_frame(start)

      r = Reader.new(read_frame)
      raise ConnectionError, "bad handshake challenge" unless r.u8 == 11

      salt = r.blob
      iterations = r.u32
      server_nonce = r.text

      salt_hex = salt.unpack1("H*") # lowercase hex
      auth_message = [user, client_nonce, server_nonce, salt_hex, iterations.to_s]
                     .join("\0").dup.force_encoding("UTF-8")
      proof, expected_sig = Skaidb.scram(password, salt, iterations, auth_message)

      write_frame([12].pack("C") + proof) # 32 raw bytes, not length-prefixed

      r = Reader.new(read_frame)
      raise ConnectionError, "bad handshake outcome" unless r.u8 == 13

      if r.u8 == 1
        server_sig = r.take(32)
        if !password.empty? && !Skaidb.secure_compare(server_sig, expected_sig)
          raise ConnectionError, "server signature mismatch (mutual auth failed)"
        end
      else
        raise ConnectionError, "authentication denied: #{r.text}"
      end
    end

    # -- query --

    def run(sql, consistency)
      raise ConnectionError, "connection is closed" if @closed

      sql_bytes = sql.dup.force_encoding("UTF-8").b
      req = [1, consistency].pack("CC") + [sql_bytes.bytesize].pack("V") + sql_bytes
      reader = nil
      @mutex.synchronize do
        write_frame(req)
        reader = Reader.new(read_frame)
      end
      parse_response(reader)
    end

    def parse_response(r)
      tag = r.u8
      case tag
      when 0 # Rows
        ncols = r.u32
        columns = Array.new(ncols) { r.text }
        nrows = r.u32
        rows = Array.new(nrows) do
          ncells = r.u32
          Array.new(ncells) { Skaidb.decode_value(Reader.new(r.blob)) }
        end
        Result.new(fields: columns, rows: rows, cmd_tuples: 0)
      when 1 # Mutation
        Result.new(fields: [], rows: [], cmd_tuples: r.u64)
      when 2 # Ddl
        Result.new(fields: [], rows: [], cmd_tuples: 0)
      when 3 # Error — a statement error, connection stays usable
        raise QueryError, r.text
      else
        raise ConnectionError, "unknown response tag #{tag}"
      end
    end
  end

  # ---- module entry point --------------------------------------------------

  # Open a connection to a skaidb node and run the SCRAM-SHA-256 handshake.
  #
  # @param host [String]
  # @param port [Integer]
  # @param user [String]
  # @param password [String]
  # @param consistency [Symbol, String, Integer] :one / :quorum / :all (or 0/1/2)
  # @param timeout [Numeric, nil] connect/IO timeout in seconds
  # @yield [conn] optional block; the connection is closed when it returns
  # @return [Connection] (or the block's value when a block is given)
  def self.connect(host: "localhost", port: 7000, user: "anonymous",
                   password: "", consistency: :quorum, timeout: 10.0)
    conn = Connection.new(host: host, port: port, user: user,
                          password: password, consistency: consistency,
                          timeout: timeout)
    return conn unless block_given?

    begin
      yield conn
    ensure
      conn.close
    end
  end
end
