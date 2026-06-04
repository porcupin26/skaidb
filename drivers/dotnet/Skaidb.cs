// skaidb — official C# / .NET driver.
//
// Pure BCL (no NuGet packages). ADO.NET-shaped API with concrete Skaidb* types:
//
//     using var conn = new SkaidbConnection("Host=localhost;Port=7000;User=skaidb;Password=secret");
//     conn.Open();
//     using var cmd = conn.CreateCommand();
//     cmd.CommandText = "INSERT INTO users (id, name) VALUES (?, ?)";
//     cmd.Parameters.Add(1);
//     cmd.Parameters.Add("Ada");
//     cmd.ExecuteNonQuery();
//
//     cmd = conn.CreateCommand();
//     cmd.CommandText = "SELECT id, name FROM users WHERE id = ?";
//     cmd.Parameters.Add(1);
//     using var reader = cmd.ExecuteReader();
//     while (reader.Read())
//         Console.WriteLine($"{reader.GetInt64(0)} {reader.GetString(1)}");
//
// Placeholders use the `?` (qmark) style, bound positionally from Parameters.
// The wire protocol is documented in ../PROTOCOL.md; this driver is verified
// against the live-tested Python reference in ../python/skaidb/__init__.py and
// is byte-for-byte compatible with it.

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.Net.Sockets;
using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace Skaidb;

/// <summary>Consistency level for a query (how many replicas must agree).</summary>
public enum SkaidbConsistency : byte
{
    One = 0,
    Quorum = 1,
    All = 2,
}

/// <summary>Thrown on any driver or server-reported error.</summary>
public class SkaidbException : Exception
{
    public SkaidbException(string message) : base(message) { }
    public SkaidbException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>
/// A connection to one skaidb node. Construct with a connection string or named
/// arguments, then call <see cref="Open"/> (idempotent) to run the SCRAM handshake.
/// </summary>
public sealed class SkaidbConnection : IDisposable
{
    public string Host { get; }
    public int Port { get; }
    public string User { get; }
    private readonly string _password;

    /// <summary>Default consistency for commands created from this connection.</summary>
    public SkaidbConsistency Consistency { get; set; }

    /// <summary>Connect/read timeout. Default 10s. Set before <see cref="Open"/>.</summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(10);

    private TcpClient? _tcp;
    private NetworkStream? _stream;
    private bool _open;
    private bool _disposed;
    private readonly object _lock = new();

    private static int _nonceCounter;

    /// <summary>Parse a connection string such as
    /// "Host=localhost;Port=7000;User=skaidb;Password=secret;Consistency=Quorum".
    /// Keys are case-insensitive.</summary>
    public SkaidbConnection(string connectionString)
    {
        if (connectionString is null) throw new ArgumentNullException(nameof(connectionString));

        string host = "localhost";
        int port = 7000;
        string user = "anonymous";
        string password = "";
        SkaidbConsistency consistency = SkaidbConsistency.Quorum;
        TimeSpan? timeout = null;

        foreach (var rawPart in connectionString.Split(';'))
        {
            var part = rawPart.Trim();
            if (part.Length == 0) continue;
            int eq = part.IndexOf('=');
            if (eq < 0) throw new SkaidbException($"malformed connection string segment: '{part}'");
            string key = part.Substring(0, eq).Trim();
            string val = part.Substring(eq + 1).Trim();
            switch (key.ToLowerInvariant())
            {
                case "host":
                case "server":
                case "datasource":
                case "data source":
                    host = val;
                    break;
                case "port":
                    port = int.Parse(val, CultureInfo.InvariantCulture);
                    break;
                case "user":
                case "username":
                case "userid":
                case "user id":
                case "uid":
                    user = val;
                    break;
                case "password":
                case "pwd":
                    password = val;
                    break;
                case "consistency":
                    consistency = ParseConsistency(val);
                    break;
                case "timeout":
                case "connecttimeout":
                case "connect timeout":
                    timeout = TimeSpan.FromSeconds(double.Parse(val, CultureInfo.InvariantCulture));
                    break;
                default:
                    throw new SkaidbException($"unknown connection string key: '{key}'");
            }
        }

        Host = host;
        Port = port;
        User = user;
        _password = password;
        Consistency = consistency;
        if (timeout.HasValue) Timeout = timeout.Value;
    }

    /// <summary>Construct from named arguments.</summary>
    public SkaidbConnection(
        string host = "localhost",
        int port = 7000,
        string user = "anonymous",
        string password = "",
        SkaidbConsistency consistency = SkaidbConsistency.Quorum)
    {
        Host = host;
        Port = port;
        User = user;
        _password = password;
        Consistency = consistency;
    }

    private static SkaidbConsistency ParseConsistency(string value)
    {
        switch (value.Trim().ToUpperInvariant())
        {
            case "ONE":
            case "0":
                return SkaidbConsistency.One;
            case "QUORUM":
            case "1":
                return SkaidbConsistency.Quorum;
            case "ALL":
            case "2":
                return SkaidbConsistency.All;
            default:
                throw new SkaidbException($"invalid consistency '{value}'");
        }
    }

    /// <summary>True once <see cref="Open"/> has completed successfully.</summary>
    public bool IsOpen => _open;

    /// <summary>Connect and run the SCRAM-SHA-256 handshake. Idempotent.</summary>
    public void Open()
    {
        if (_disposed) throw new ObjectDisposedException(nameof(SkaidbConnection));
        if (_open) return;

        try
        {
            var tcp = new TcpClient { NoDelay = true };
            tcp.SendTimeout = (int)Timeout.TotalMilliseconds;
            tcp.ReceiveTimeout = (int)Timeout.TotalMilliseconds;
            // Connect with timeout.
            var connectTask = tcp.ConnectAsync(Host, Port);
            if (!connectTask.Wait(Timeout))
            {
                tcp.Dispose();
                throw new SkaidbException($"connect to {Host}:{Port} timed out");
            }
            tcp.NoDelay = true;
            var stream = tcp.GetStream();
            stream.ReadTimeout = (int)Timeout.TotalMilliseconds;
            stream.WriteTimeout = (int)Timeout.TotalMilliseconds;

            _tcp = tcp;
            _stream = stream;
            Handshake(User, _password);
            _open = true;
        }
        catch (SkaidbException)
        {
            CleanupSocket();
            throw;
        }
        catch (Exception e)
        {
            CleanupSocket();
            throw new SkaidbException($"connect failed: {e.Message}", e);
        }
    }

    private void CleanupSocket()
    {
        try { _stream?.Dispose(); } catch { /* ignore */ }
        try { _tcp?.Dispose(); } catch { /* ignore */ }
        _stream = null;
        _tcp = null;
    }

    /// <summary>Create a command bound to this connection.</summary>
    public SkaidbCommand CreateCommand() => new SkaidbCommand(this);

    public void Close() => Dispose();

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _open = false;
        CleanupSocket();
    }

    // ---- framing -----------------------------------------------------------

    private void WriteFrame(byte[] payload)
    {
        if (_stream is null) throw new SkaidbException("connection is not open");
        Span<byte> head = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(head, (uint)payload.Length); // length is BIG-endian
        _stream.Write(head);
        _stream.Write(payload, 0, payload.Length);
        _stream.Flush();
    }

    private byte[] ReadFrame()
    {
        byte[] head = ReadExact(4);
        uint length = BinaryPrimitives.ReadUInt32BigEndian(head); // length is BIG-endian
        if (length > 64 * 1024 * 1024)
            throw new SkaidbException($"frame too large: {length} bytes");
        return ReadExact((int)length);
    }

    // NetworkStream.Read can return short; loop until N bytes are read.
    private byte[] ReadExact(int n)
    {
        if (_stream is null) throw new SkaidbException("connection is not open");
        var buf = new byte[n];
        int got = 0;
        while (got < n)
        {
            int read = _stream.Read(buf, got, n - got);
            if (read <= 0) throw new SkaidbException("connection closed by server");
            got += read;
        }
        return buf;
    }

    // ---- handshake ---------------------------------------------------------

    private void Handshake(string user, string password)
    {
        int counter = Interlocked.Increment(ref _nonceCounter);
        string clientNonce = $"cs{Environment.CurrentManagedThreadId}.{counter}.{Environment.TickCount64}";

        // AuthStart: u8 10, str username, str client_nonce
        {
            var w = new BinWriter();
            w.U8(10);
            w.Str(user);
            w.Str(clientNonce);
            WriteFrame(w.ToArray());
        }

        // AuthChallenge: u8 11, blob salt, u32 iterations, str server_nonce
        byte[] salt;
        uint iterations;
        string serverNonce;
        {
            var r = new BinReader(ReadFrame());
            if (r.U8() != 11) throw new SkaidbException("bad handshake challenge");
            salt = r.Blob();
            iterations = r.U32();
            serverNonce = r.Text();
        }

        string saltHex = ToLowerHex(salt);
        // auth_message = user \0 client_nonce \0 server_nonce \0 salt_hex \0 iterations
        string authMessageStr = string.Join(
            "\0",
            user, clientNonce, serverNonce, saltHex, iterations.ToString(CultureInfo.InvariantCulture));
        byte[] authMessage = Encoding.UTF8.GetBytes(authMessageStr);

        var (proof, expectedServerSig) = ScramProof(password, salt, (int)iterations, authMessage);

        // AuthFinish: u8 12, 32 raw proof bytes (NOT length-prefixed)
        {
            var payload = new byte[1 + 32];
            payload[0] = 12;
            Array.Copy(proof, 0, payload, 1, 32);
            WriteFrame(payload);
        }

        // AuthOutcome: u8 13, u8 ok; if ok: 32 raw server_sig; else str reason
        {
            var r = new BinReader(ReadFrame());
            if (r.U8() != 13) throw new SkaidbException("bad handshake outcome");
            if (r.U8() == 1)
            {
                byte[] serverSig = r.Take(32);
                if (password.Length > 0 && !FixedTimeEquals(serverSig, expectedServerSig))
                    throw new SkaidbException("server signature mismatch (mutual auth failed)");
            }
            else
            {
                throw new SkaidbException($"authentication denied: {r.Text()}");
            }
        }
    }

    private static (byte[] proof, byte[] serverSig) ScramProof(
        string password, byte[] salt, int iterations, byte[] authMessage)
    {
        byte[] pw = Encoding.UTF8.GetBytes(password);
        byte[] salted;
        using (var pbkdf2 = new Rfc2898DeriveBytes(pw, salt, iterations, HashAlgorithmName.SHA256))
        {
            salted = pbkdf2.GetBytes(32); // dkLen = 32
        }

        byte[] clientKey = HmacSha256(salted, Encoding.ASCII.GetBytes("Client Key"));
        byte[] storedKey = Sha256(clientKey);
        byte[] clientSig = HmacSha256(storedKey, authMessage);

        var proof = new byte[32];
        for (int i = 0; i < 32; i++) proof[i] = (byte)(clientKey[i] ^ clientSig[i]);

        byte[] serverKey = HmacSha256(salted, Encoding.ASCII.GetBytes("Server Key"));
        byte[] serverSig = HmacSha256(serverKey, authMessage);
        return (proof, serverSig);
    }

    private static byte[] HmacSha256(byte[] key, byte[] message)
    {
        using var h = new HMACSHA256(key); // key first, then message
        return h.ComputeHash(message);
    }

    private static byte[] Sha256(byte[] data)
    {
        using var h = SHA256.Create();
        return h.ComputeHash(data);
    }

    private static bool FixedTimeEquals(byte[] a, byte[] b)
    {
        if (a.Length != b.Length) return false;
        int diff = 0;
        for (int i = 0; i < a.Length; i++) diff |= a[i] ^ b[i];
        return diff == 0;
    }

    private static string ToLowerHex(byte[] bytes)
    {
        var sb = new StringBuilder(bytes.Length * 2);
        foreach (byte b in bytes) sb.Append(b.ToString("x2", CultureInfo.InvariantCulture));
        return sb.ToString();
    }

    // ---- query (internal) --------------------------------------------------

    internal QueryResult Query(string sql, SkaidbConsistency consistency)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(SkaidbConnection));
        if (!_open) throw new SkaidbException("connection is not open");

        byte[] sqlBytes = Encoding.UTF8.GetBytes(sql);
        var w = new BinWriter();
        w.U8(1);                  // OP_QUERY
        w.U8((byte)consistency);  // consistency
        w.U32((uint)sqlBytes.Length); // sql_len, LITTLE-endian
        w.Raw(sqlBytes);

        BinReader r;
        lock (_lock)
        {
            WriteFrame(w.ToArray());
            r = new BinReader(ReadFrame());
        }

        byte tag = r.U8();
        switch (tag)
        {
            case 0: // Rows
            {
                uint ncols = r.U32();
                var columns = new string[ncols];
                for (int i = 0; i < ncols; i++) columns[i] = r.Text();
                uint nrows = r.U32();
                var rows = new List<object?[]>((int)nrows);
                for (int ri = 0; ri < nrows; ri++)
                {
                    uint ncells = r.U32();
                    var row = new object?[ncells];
                    for (int ci = 0; ci < ncells; ci++)
                    {
                        // each cell is a length-prefixed self-describing Value
                        byte[] cellBytes = r.Blob();
                        var cr = new BinReader(cellBytes);
                        row[ci] = DecodeValue(cr);
                    }
                    rows.Add(row);
                }
                return new QueryResult(QueryResultKind.Rows, columns, rows, 0);
            }
            case 1: // Mutation
                return new QueryResult(QueryResultKind.Mutation, null, null, r.U64());
            case 2: // Ddl
                return new QueryResult(QueryResultKind.Ddl, null, null, 0);
            case 3: // Error
                throw new SkaidbException(r.Text());
            default:
                throw new SkaidbException($"unknown response tag {tag}");
        }
    }

    // ---- value codec (§4) --------------------------------------------------

    private const byte TagNull = 0;
    private const byte TagBool = 1;
    private const byte TagInt = 2;
    private const byte TagFloat = 3;
    private const byte TagDecimal = 4;
    private const byte TagString = 5;
    private const byte TagBytes = 6;
    private const byte TagUuid = 7;
    private const byte TagTimestamp = 8;
    private const byte TagArray = 9;
    private const byte TagDocument = 10;

    internal static object? DecodeValue(BinReader r)
    {
        byte tag = r.U8();
        switch (tag)
        {
            case TagNull:
                return null;
            case TagBool:
                return r.U8() != 0;
            case TagInt:
                return r.I64();
            case TagFloat:
                return BitConverter.Int64BitsToDouble(r.I64());
            case TagDecimal:
            {
                byte[] mantissaBytes = r.Take(16); // i128 LE
                uint scale = r.U32();
                return DecodeDecimal(mantissaBytes, scale);
            }
            case TagString:
                return r.Text();
            case TagBytes:
                return r.Blob();
            case TagUuid:
                return GuidFromRfc4122(r.Take(16));
            case TagTimestamp:
                return DateTimeOffset.FromUnixTimeMilliseconds(r.I64()); // UTC
            case TagArray:
            {
                uint count = r.U32();
                var arr = new object?[count];
                for (int i = 0; i < count; i++) arr[i] = DecodeValue(r);
                return arr;
            }
            case TagDocument:
            {
                uint count = r.U32();
                // Preserve insertion order. Dictionary preserves insertion order
                // for enumeration as long as nothing is removed.
                var doc = new Dictionary<string, object?>((int)count);
                for (int i = 0; i < count; i++)
                {
                    string key = r.Text();
                    doc[key] = DecodeValue(r);
                }
                return doc;
            }
            default:
                throw new SkaidbException($"unknown value tag {tag}");
        }
    }

    // Decimal: value = mantissa / 10^scale. System.Decimal when it fits, else a string.
    private static object DecodeDecimal(byte[] mantissaLeBytes, uint scale)
    {
        // i128 little-endian, signed.
        var mantissa = new BigInteger(mantissaLeBytes, isUnsigned: false, isBigEndian: false);

        // System.Decimal holds a 96-bit unsigned integer with scale 0..28.
        // Try to use it when both the magnitude and the scale fit; otherwise
        // fall back to a lossless decimal string.
        if (scale <= 28)
        {
            BigInteger mag = BigInteger.Abs(mantissa);
            // 96-bit unsigned max.
            BigInteger max96 = (BigInteger.One << 96) - 1;
            if (mag <= max96)
            {
                // Build the int parts of the 96-bit magnitude.
                uint lo = (uint)(mag & 0xFFFFFFFF);
                uint mid = (uint)((mag >> 32) & 0xFFFFFFFF);
                uint hi = (uint)((mag >> 64) & 0xFFFFFFFF);
                return new decimal((int)lo, (int)mid, (int)hi, mantissa.Sign < 0, (byte)scale);
            }
        }

        // Fallback: format mantissa/10^scale as a plain decimal string.
        return FormatBigDecimalString(mantissa, scale);
    }

    private static string FormatBigDecimalString(BigInteger mantissa, uint scale)
    {
        if (scale == 0) return mantissa.ToString(CultureInfo.InvariantCulture);

        bool negative = mantissa.Sign < 0;
        string digits = BigInteger.Abs(mantissa).ToString(CultureInfo.InvariantCulture);
        if (digits.Length <= scale)
            digits = new string('0', (int)scale - digits.Length + 1) + digits;

        int pointPos = digits.Length - (int)scale;
        string intPart = digits.Substring(0, pointPos);
        string fracPart = digits.Substring(pointPos);
        string result = intPart + "." + fracPart;
        return negative ? "-" + result : result;
    }

    // The 16 bytes are RFC-4122 / big-endian order. Guid(byte[]) interprets the
    // first 3 fields as little-endian, so build the Guid from its canonical
    // 8-4-4-4-12 string form to avoid byte reordering.
    private static Guid GuidFromRfc4122(byte[] b)
    {
        var sb = new StringBuilder(36);
        AppendHex(sb, b, 0, 4);
        sb.Append('-');
        AppendHex(sb, b, 4, 2);
        sb.Append('-');
        AppendHex(sb, b, 6, 2);
        sb.Append('-');
        AppendHex(sb, b, 8, 2);
        sb.Append('-');
        AppendHex(sb, b, 10, 6);
        return new Guid(sb.ToString());
    }

    private static void AppendHex(StringBuilder sb, byte[] b, int offset, int count)
    {
        for (int i = 0; i < count; i++)
            sb.Append(b[offset + i].ToString("x2", CultureInfo.InvariantCulture));
    }
}

// ---- internal result carrier ----------------------------------------------

internal enum QueryResultKind { Rows, Mutation, Ddl }

internal sealed class QueryResult
{
    public QueryResultKind Kind { get; }
    public string[]? Columns { get; }
    public List<object?[]>? Rows { get; }
    public ulong Affected { get; }

    public QueryResult(QueryResultKind kind, string[]? columns, List<object?[]>? rows, ulong affected)
    {
        Kind = kind;
        Columns = columns;
        Rows = rows;
        Affected = affected;
    }
}

/// <summary>
/// A SQL command. Set <see cref="CommandText"/> with `?` placeholders and add
/// values to <see cref="Parameters"/> in order, then execute.
/// </summary>
public sealed class SkaidbCommand : IDisposable
{
    private readonly SkaidbConnection _connection;

    public string CommandText { get; set; } = "";

    /// <summary>Positional parameters, bound to `?` placeholders in order.</summary>
    public List<object?> Parameters { get; } = new();

    /// <summary>Per-command consistency override. Defaults to the connection's.</summary>
    public SkaidbConsistency Consistency { get; set; }

    public SkaidbCommand(SkaidbConnection connection)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        Consistency = connection.Consistency;
    }

    /// <summary>Execute and return a reader over the result rows. One reader per execution.</summary>
    public SkaidbDataReader ExecuteReader()
    {
        var result = Run();
        if (result.Kind == QueryResultKind.Rows)
            return new SkaidbDataReader(result.Columns!, result.Rows!);
        // Non-row results expose an empty reader (FieldCount 0, Read() false).
        return new SkaidbDataReader(Array.Empty<string>(), new List<object?[]>());
    }

    /// <summary>Execute a statement and return the number of affected rows (0 for DDL/SELECT).</summary>
    public int ExecuteNonQuery()
    {
        var result = Run();
        if (result.Kind == QueryResultKind.Mutation)
        {
            ulong n = result.Affected;
            return n > int.MaxValue ? int.MaxValue : (int)n;
        }
        return 0;
    }

    /// <summary>Execute and return the first column of the first row (or null).</summary>
    public object? ExecuteScalar()
    {
        var result = Run();
        if (result.Kind == QueryResultKind.Rows && result.Rows is { Count: > 0 } rows)
        {
            var first = rows[0];
            return first.Length > 0 ? first[0] : null;
        }
        return null;
    }

    private QueryResult Run()
    {
        string sql = ParameterBinder.Bind(CommandText, Parameters);
        return _connection.Query(sql, Consistency);
    }

    public void Dispose() { /* nothing owned */ }
}

/// <summary>Forward-only reader over a result set. Created by <see cref="SkaidbCommand.ExecuteReader"/>.</summary>
public sealed class SkaidbDataReader : IDisposable
{
    private readonly string[] _columns;
    private readonly List<object?[]> _rows;
    private int _pos = -1;

    internal SkaidbDataReader(string[] columns, List<object?[]> rows)
    {
        _columns = columns;
        _rows = rows;
    }

    /// <summary>Number of columns in the result.</summary>
    public int FieldCount => _columns.Length;

    /// <summary>Advance to the next row. Returns false when exhausted.</summary>
    public bool Read()
    {
        if (_pos + 1 >= _rows.Count) { _pos = _rows.Count; return false; }
        _pos++;
        return true;
    }

    private object?[] Current
    {
        get
        {
            if (_pos < 0) throw new SkaidbException("call Read() before accessing fields");
            if (_pos >= _rows.Count) throw new SkaidbException("no current row");
            return _rows[_pos];
        }
    }

    public string GetName(int i) => _columns[i];

    public int GetOrdinal(string name)
    {
        for (int i = 0; i < _columns.Length; i++)
            if (string.Equals(_columns[i], name, StringComparison.Ordinal))
                return i;
        for (int i = 0; i < _columns.Length; i++)
            if (string.Equals(_columns[i], name, StringComparison.OrdinalIgnoreCase))
                return i;
        throw new SkaidbException($"no column named '{name}'");
    }

    public bool IsDBNull(int i) => Current[i] is null;

    /// <summary>Raw value; null maps to <see cref="DBNull.Value"/>.</summary>
    public object GetValue(int i) => Current[i] ?? DBNull.Value;

    public bool GetBoolean(int i) => Convert.ToBoolean(NonNull(i), CultureInfo.InvariantCulture);

    public long GetInt64(int i) => Convert.ToInt64(NonNull(i), CultureInfo.InvariantCulture);

    public int GetInt32(int i) => Convert.ToInt32(NonNull(i), CultureInfo.InvariantCulture);

    public double GetDouble(int i) => Convert.ToDouble(NonNull(i), CultureInfo.InvariantCulture);

    public decimal GetDecimal(int i) => Convert.ToDecimal(NonNull(i), CultureInfo.InvariantCulture);

    public string GetString(int i)
    {
        object v = NonNull(i);
        return v as string ?? Convert.ToString(v, CultureInfo.InvariantCulture) ?? "";
    }

    public Guid GetGuid(int i)
    {
        object v = NonNull(i);
        return v is Guid g ? g : Guid.Parse(Convert.ToString(v, CultureInfo.InvariantCulture)!);
    }

    public DateTimeOffset GetDateTimeOffset(int i) => (DateTimeOffset)NonNull(i);

    public byte[] GetBytes(int i) => (byte[])NonNull(i);

    private object NonNull(int i)
    {
        object? v = Current[i];
        if (v is null) throw new SkaidbException($"column {i} is NULL");
        return v;
    }

    /// <summary>Value by ordinal (DBNull for null).</summary>
    public object this[int i] => GetValue(i);

    /// <summary>Value by column name (DBNull for null).</summary>
    public object this[string column] => GetValue(GetOrdinal(column));

    public void Dispose() { /* nothing owned */ }
}

// ---- client-side parameter binding (§5) ------------------------------------

internal static class ParameterBinder
{
    public static string Bind(string sql, IReadOnlyList<object?> parameters)
    {
        if (parameters is null || parameters.Count == 0)
        {
            if (HasPlaceholderOutsideStrings(sql))
                throw new SkaidbException("query has placeholders but no parameters given");
            return sql;
        }

        var sb = new StringBuilder(sql.Length + 16);
        bool inStr = false;
        int i = 0;
        int n = sql.Length;
        int used = 0;
        while (i < n)
        {
            char ch = sql[i];
            if (inStr)
            {
                sb.Append(ch);
                if (ch == '\'')
                {
                    // A doubled '' is an escaped quote and stays inside the string.
                    if (i + 1 < n && sql[i + 1] == '\'')
                    {
                        sb.Append('\'');
                        i += 2;
                        continue;
                    }
                    inStr = false;
                }
                i++;
                continue;
            }
            if (ch == '\'')
            {
                inStr = true;
                sb.Append(ch);
                i++;
                continue;
            }
            if (ch == '?')
            {
                if (used >= parameters.Count)
                    throw new SkaidbException("more placeholders than parameters");
                sb.Append(Quote(parameters[used]));
                used++;
                i++;
                continue;
            }
            sb.Append(ch);
            i++;
        }
        if (used < parameters.Count)
            throw new SkaidbException("more parameters than placeholders");
        return sb.ToString();
    }

    private static bool HasPlaceholderOutsideStrings(string sql)
    {
        bool inStr = false;
        int i = 0, n = sql.Length;
        while (i < n)
        {
            char ch = sql[i];
            if (inStr)
            {
                if (ch == '\'')
                {
                    if (i + 1 < n && sql[i + 1] == '\'') { i += 2; continue; }
                    inStr = false;
                }
                i++;
                continue;
            }
            if (ch == '\'') { inStr = true; i++; continue; }
            if (ch == '?') return true;
            i++;
        }
        return false;
    }

    private static string Quote(object? arg)
    {
        switch (arg)
        {
            case null:
            case DBNull:
                return "NULL";
            case bool b:
                return b ? "TRUE" : "FALSE";
            case byte v:
                return v.ToString(CultureInfo.InvariantCulture);
            case sbyte v:
                return v.ToString(CultureInfo.InvariantCulture);
            case short v:
                return v.ToString(CultureInfo.InvariantCulture);
            case ushort v:
                return v.ToString(CultureInfo.InvariantCulture);
            case int v:
                return v.ToString(CultureInfo.InvariantCulture);
            case uint v:
                return v.ToString(CultureInfo.InvariantCulture);
            case long v:
                return v.ToString(CultureInfo.InvariantCulture);
            case ulong v:
                return v.ToString(CultureInfo.InvariantCulture);
            case BigInteger v:
                return v.ToString(CultureInfo.InvariantCulture);
            case float f:
                if (float.IsNaN(f) || float.IsInfinity(f))
                    throw new SkaidbException("cannot bind NaN/Infinity");
                return f.ToString("R", CultureInfo.InvariantCulture);
            case double d:
                if (double.IsNaN(d) || double.IsInfinity(d))
                    throw new SkaidbException("cannot bind NaN/Infinity");
                return d.ToString("R", CultureInfo.InvariantCulture);
            case decimal m:
                return m.ToString(CultureInfo.InvariantCulture);
            case string s:
                return "'" + s.Replace("'", "''") + "'";
            case char c:
                return "'" + (c == '\'' ? "''" : c.ToString()) + "'";
            case byte[] bytes:
                return "'" + ToLowerHex(bytes) + "'";
            case Guid g:
                return "'" + g.ToString("D", CultureInfo.InvariantCulture) + "'";
            case DateTimeOffset dto:
                return dto.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);
            case DateTime dt:
            {
                var utc = dt.Kind == DateTimeKind.Unspecified
                    ? DateTime.SpecifyKind(dt, DateTimeKind.Utc)
                    : dt.ToUniversalTime();
                long ms = new DateTimeOffset(utc).ToUnixTimeMilliseconds();
                return ms.ToString(CultureInfo.InvariantCulture);
            }
            default:
                throw new SkaidbException($"cannot bind value of type {arg.GetType().Name}");
        }
    }

    private static string ToLowerHex(byte[] bytes)
    {
        var sb = new StringBuilder(bytes.Length * 2);
        foreach (byte b in bytes) sb.Append(b.ToString("x2", CultureInfo.InvariantCulture));
        return sb.ToString();
    }
}

// ---- binary read/write helpers ---------------------------------------------
// All length/integer fields here are LITTLE-endian (the frame length prefix,
// handled in SkaidbConnection, is the only big-endian field in the protocol).

internal sealed class BinReader
{
    private readonly byte[] _buf;
    private int _pos;

    public BinReader(byte[] buf) { _buf = buf; _pos = 0; }

    public byte[] Take(int n)
    {
        int end = _pos + n;
        if (n < 0 || end > _buf.Length) throw new SkaidbException("truncated server message");
        var slice = new byte[n];
        Array.Copy(_buf, _pos, slice, 0, n);
        _pos = end;
        return slice;
    }

    private ReadOnlySpan<byte> Span(int n)
    {
        int end = _pos + n;
        if (n < 0 || end > _buf.Length) throw new SkaidbException("truncated server message");
        var span = new ReadOnlySpan<byte>(_buf, _pos, n);
        _pos = end;
        return span;
    }

    public byte U8()
    {
        if (_pos >= _buf.Length) throw new SkaidbException("truncated server message");
        return _buf[_pos++];
    }

    public uint U32() => BinaryPrimitives.ReadUInt32LittleEndian(Span(4));
    public long I64() => BinaryPrimitives.ReadInt64LittleEndian(Span(8));
    public ulong U64() => BinaryPrimitives.ReadUInt64LittleEndian(Span(8));

    public byte[] Blob() => Take((int)U32());
    public string Text() => Encoding.UTF8.GetString(Blob());
}

internal sealed class BinWriter
{
    private readonly List<byte> _buf = new();

    public void U8(byte b) => _buf.Add(b);

    public void U32(uint v)
    {
        Span<byte> tmp = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(tmp, v);
        _buf.AddRange(tmp.ToArray());
    }

    public void Raw(byte[] bytes) => _buf.AddRange(bytes);

    public void Str(string s)
    {
        byte[] b = Encoding.UTF8.GetBytes(s);
        U32((uint)b.Length);
        _buf.AddRange(b);
    }

    public byte[] ToArray() => _buf.ToArray();
}
