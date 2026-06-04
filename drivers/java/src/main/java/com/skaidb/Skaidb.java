package com.skaidb;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.MessageDigest;

/**
 * Official skaidb driver for Java. The API is modeled on JDBC — {@code connect},
 * {@code prepare}, {@code setInt}/{@code setString}, {@code executeQuery}/
 * {@code executeUpdate}, and a {@code ResultSet} with {@code next()}/{@code getX}
 * — so a JDBC user has essentially nothing new to learn. Pure JDK: no
 * third-party dependencies.
 *
 * <pre>{@code
 * try (Skaidb.Connection conn = Skaidb.connect("skaidb://user:pass@localhost:7000")) {
 *     conn.execute("CREATE TABLE users (PRIMARY KEY (id))");
 *     try (Skaidb.Query q = conn.prepare("INSERT INTO users (id, name) VALUES (?, ?)")) {
 *         q.setInt(1, 1).setString(2, "Ada").executeUpdate();
 *     }
 *     Skaidb.ResultSet rs = conn.prepare("SELECT id, name FROM users WHERE id = ?")
 *                               .setInt(1, 1).executeQuery();
 *     while (rs.next()) System.out.println(rs.getInt("id") + " " + rs.getString("name"));
 * }
 * }</pre>
 */
public final class Skaidb {

    private Skaidb() {}

    public static final int CONSISTENCY_ONE = 0;
    public static final int CONSISTENCY_QUORUM = 1;
    public static final int CONSISTENCY_ALL = 2;

    /** Connect using a {@code skaidb://user:pass@host:port/?consistency=quorum} URL. */
    public static Connection connect(String dsn) {
        try {
            URI u = URI.create(dsn);
            if (!"skaidb".equals(u.getScheme()))
                throw new SkaidbException("DSN scheme must be skaidb://");
            String user = "anonymous", pass = "";
            if (u.getUserInfo() != null) {
                String[] up = u.getUserInfo().split(":", 2);
                user = up[0];
                if (up.length > 1) pass = up[1];
            }
            int port = u.getPort() == -1 ? 7000 : u.getPort();
            int consistency = CONSISTENCY_QUORUM;
            String q = u.getQuery();
            if (q != null) {
                for (String part : q.split("&")) {
                    if (part.startsWith("consistency=")) {
                        consistency = parseConsistency(part.substring("consistency=".length()));
                    }
                }
            }
            return new Connection(u.getHost(), port, user, pass, consistency);
        } catch (IllegalArgumentException e) {
            throw new SkaidbException("bad DSN: " + e.getMessage());
        }
    }

    public static Connection connect(String host, int port, String user, String password) {
        return new Connection(host, port, user, password, CONSISTENCY_QUORUM);
    }

    private static int parseConsistency(String s) {
        switch (s.toLowerCase()) {
            case "one": return CONSISTENCY_ONE;
            case "all": return CONSISTENCY_ALL;
            case "":
            case "quorum": return CONSISTENCY_QUORUM;
            default: throw new SkaidbException("bad consistency " + s);
        }
    }

    /** Thrown on connection, protocol, or statement errors. */
    public static final class SkaidbException extends RuntimeException {
        public SkaidbException(String message) { super(message); }
        public SkaidbException(String message, Throwable cause) { super(message, cause); }
    }

    // ---- Connection --------------------------------------------------------

    public static final class Connection implements AutoCloseable {
        private static int nonceCounter = 0;
        private final Socket socket;
        private final DataInputStream in;
        private final OutputStream out;
        private int consistency;
        private boolean closed = false;

        Connection(String host, int port, String user, String password, int consistency) {
            this.consistency = consistency;
            try {
                socket = new Socket();
                socket.connect(new InetSocketAddress(host, port), 10_000);
                socket.setTcpNoDelay(true);
                in = new DataInputStream(socket.getInputStream());
                out = socket.getOutputStream();
                handshake(user, password);
            } catch (IOException e) {
                throw new SkaidbException("connect failed: " + e.getMessage(), e);
            }
        }

        /** Override the consistency level for subsequent statements. */
        public Connection setConsistency(int level) {
            if (level < 0 || level > 2) throw new SkaidbException("bad consistency " + level);
            this.consistency = level;
            return this;
        }

        /** Prepare a parameterized statement (use {@code ?} placeholders). */
        public Query prepare(String sql) { return new Query(this, sql); }

        /** Run a statement with no parameters; returns affected rows, or -1 for DDL. */
        public long execute(String sql) { return new Query(this, sql).executeUpdate(); }

        /** Run a SELECT with no parameters. */
        public ResultSet query(String sql) { return new Query(this, sql).executeQuery(); }

        @Override public void close() {
            if (closed) return;
            closed = true;
            try { socket.close(); } catch (IOException ignored) {}
        }

        // -- framing --
        private void writeFrame(byte[] payload) throws IOException {
            out.write(new byte[]{
                (byte) (payload.length >>> 24), (byte) (payload.length >>> 16),
                (byte) (payload.length >>> 8), (byte) payload.length});
            out.write(payload);
            out.flush();
        }

        private byte[] readFrame() throws IOException {
            int len = in.readInt(); // big-endian
            byte[] buf = new byte[len];
            in.readFully(buf);
            return buf;
        }

        // -- handshake --
        private void handshake(String user, String password) throws IOException {
            String clientNonce;
            synchronized (Connection.class) { clientNonce = "jv" + (++nonceCounter) + "." + System.nanoTime(); }

            Buf start = new Buf();
            start.u8(10).str(user).str(clientNonce);
            writeFrame(start.toBytes());

            Reader r = new Reader(readFrame());
            if (r.u8() != 11) throw new SkaidbException("bad handshake challenge");
            byte[] salt = r.blob();
            int iterations = r.u32();
            String serverNonce = r.text();

            byte[] authMessage = String.join("\0",
                user, clientNonce, serverNonce, hex(salt), Integer.toString(iterations))
                .getBytes(StandardCharsets.UTF_8);
            byte[] salted = pbkdf2(password.getBytes(StandardCharsets.UTF_8), salt, iterations, 32);
            byte[] clientKey = hmac(salted, "Client Key".getBytes(StandardCharsets.UTF_8));
            byte[] storedKey = sha256(clientKey);
            byte[] clientSig = hmac(storedKey, authMessage);
            byte[] proof = new byte[32];
            for (int i = 0; i < 32; i++) proof[i] = (byte) (clientKey[i] ^ clientSig[i]);

            Buf finish = new Buf();
            finish.u8(12).raw(proof);
            writeFrame(finish.toBytes());

            Reader r2 = new Reader(readFrame());
            if (r2.u8() != 13) throw new SkaidbException("bad handshake outcome");
            if (r2.u8() == 1) {
                byte[] serverSig = r2.take(32);
                if (!password.isEmpty()) {
                    byte[] serverKey = hmac(salted, "Server Key".getBytes(StandardCharsets.UTF_8));
                    byte[] expected = hmac(serverKey, authMessage);
                    if (!MessageDigest.isEqual(serverSig, expected))
                        throw new SkaidbException("server signature mismatch (mutual auth failed)");
                }
            } else {
                throw new SkaidbException("authentication denied: " + r2.text());
            }
        }

        // -- query: returns the raw response Reader positioned after the tag --
        synchronized Object run(String sql, boolean wantRows) {
            if (closed) throw new SkaidbException("connection is closed");
            try {
                byte[] body = sql.getBytes(StandardCharsets.UTF_8);
                Buf req = new Buf();
                req.u8(1).u8(consistency).u32(body.length).raw(body);
                writeFrame(req.toBytes());

                Reader r = new Reader(readFrame());
                int tag = r.u8();
                if (tag == 0) {            // Rows
                    int ncols = r.u32();
                    String[] cols = new String[ncols];
                    for (int i = 0; i < ncols; i++) cols[i] = r.text();
                    int nrows = r.u32();
                    List<Object[]> data = new ArrayList<>(nrows);
                    for (int i = 0; i < nrows; i++) {
                        int ncells = r.u32();
                        Object[] row = new Object[ncells];
                        for (int j = 0; j < ncells; j++) row[j] = decodeValue(new Reader(r.blob()));
                        data.add(row);
                    }
                    return new ResultSet(cols, data);
                } else if (tag == 1) {     // Mutation
                    return r.u64();
                } else if (tag == 2) {     // Ddl
                    return -1L;
                } else if (tag == 3) {     // Error
                    throw new SkaidbException(r.text());
                }
                throw new SkaidbException("unknown response tag " + tag);
            } catch (IOException e) {
                throw new SkaidbException("query failed: " + e.getMessage(), e);
            }
        }
    }

    // ---- Query (prepared statement) ---------------------------------------

    public static final class Query implements AutoCloseable {
        private final Connection conn;
        private final String sql;
        private final Object[] params;

        Query(Connection conn, String sql) {
            this.conn = conn;
            this.sql = sql;
            this.params = new Object[countPlaceholders(sql)];
        }

        // JDBC-style 1-based parameter setters (all funnel through setObject).
        public Query setInt(int i, int v)        { return setObject(i, (long) v); }
        public Query setLong(int i, long v)      { return setObject(i, v); }
        public Query setDouble(int i, double v)  { return setObject(i, v); }
        public Query setBoolean(int i, boolean v){ return setObject(i, v); }
        public Query setString(int i, String v)  { return setObject(i, v); }
        public Query setNull(int i)              { return setObject(i, null); }

        public Query setObject(int i, Object v) {
            if (i < 1 || i > params.length)
                throw new SkaidbException("parameter index " + i + " out of range 1.." + params.length);
            params[i - 1] = v;
            return this;
        }

        public ResultSet executeQuery() {
            Object res = conn.run(bind(sql, params), true);
            if (res instanceof ResultSet) return (ResultSet) res;
            return new ResultSet(new String[0], new ArrayList<>()); // mutation/ddl: empty set
        }

        public long executeUpdate() {
            Object res = conn.run(bind(sql, params), false);
            return (res instanceof Long) ? (Long) res : 0L;
        }

        @Override public void close() {}
    }

    // ---- ResultSet --------------------------------------------------------

    public static final class ResultSet {
        private final String[] columns;
        private final List<Object[]> rows;
        private int pos = -1;

        ResultSet(String[] columns, List<Object[]> rows) {
            this.columns = columns;
            this.rows = rows;
        }

        public boolean next() { return ++pos < rows.size(); }
        public int getRowCount() { return rows.size(); }
        public String[] getColumnNames() { return columns.clone(); }

        public Object getObject(int col) { return rows.get(pos)[col - 1]; } // 1-based
        public Object getObject(String name) { return rows.get(pos)[colIndex(name)]; }

        public String getString(String name)  { Object v = getObject(name); return v == null ? null : String.valueOf(v); }
        public int getInt(String name)         { return ((Number) req(name)).intValue(); }
        public long getLong(String name)       { return ((Number) req(name)).longValue(); }
        public double getDouble(String name)   { return ((Number) req(name)).doubleValue(); }
        public boolean getBoolean(String name) { return (Boolean) req(name); }
        public boolean isNull(String name)     { return getObject(name) == null; }

        public String getString(int col)  { Object v = getObject(col); return v == null ? null : String.valueOf(v); }
        public int getInt(int col)         { return ((Number) getObject(col)).intValue(); }
        public long getLong(int col)       { return ((Number) getObject(col)).longValue(); }
        public double getDouble(int col)   { return ((Number) getObject(col)).doubleValue(); }
        public boolean getBoolean(int col) { return (Boolean) getObject(col); }

        private Object req(String name) {
            Object v = getObject(name);
            if (v == null) throw new SkaidbException("column " + name + " is NULL");
            return v;
        }

        private int colIndex(String name) {
            for (int i = 0; i < columns.length; i++) if (columns[i].equals(name)) return i;
            throw new SkaidbException("no such column: " + name);
        }
    }

    // ---- value decoding (§4) ----------------------------------------------

    static Object decodeValue(Reader r) {
        int tag = r.u8();
        switch (tag) {
            case 0:  return null;
            case 1:  return r.u8() != 0;
            case 2:  return r.i64();
            case 3:  return Double.longBitsToDouble(r.i64());
            case 4: {                                  // Decimal -> BigDecimal
                BigInteger mant = signedLE(r.take(16));
                int scale = r.u32();
                return new BigDecimal(mant, scale);
            }
            case 5:  return r.text();
            case 6:  return r.blob();                  // Bytes -> byte[]
            case 7: {                                  // Uuid
                byte[] b = r.take(16);
                long hi = 0, lo = 0;
                for (int i = 0; i < 8; i++) hi = (hi << 8) | (b[i] & 0xff);
                for (int i = 8; i < 16; i++) lo = (lo << 8) | (b[i] & 0xff);
                return new UUID(hi, lo);
            }
            case 8:  return Instant.ofEpochMilli(r.i64());   // Timestamp
            case 9: {                                  // Array -> List
                int n = r.u32();
                List<Object> list = new ArrayList<>(n);
                for (int i = 0; i < n; i++) list.add(decodeValue(r));
                return list;
            }
            case 10: {                                 // Document -> ordered Map
                int n = r.u32();
                Map<String, Object> m = new LinkedHashMap<>();
                for (int i = 0; i < n; i++) { String k = r.text(); m.put(k, decodeValue(r)); }
                return m;
            }
            default: throw new SkaidbException("unknown value tag " + tag);
        }
    }

    private static BigInteger signedLE(byte[] le) {
        byte[] be = new byte[le.length];
        for (int i = 0; i < le.length; i++) be[i] = le[le.length - 1 - i];
        return new BigInteger(be); // two's-complement big-endian
    }

    // ---- client-side parameter binding (§5) -------------------------------

    static int countPlaceholders(String sql) {
        int n = 0;
        boolean inStr = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (inStr) {
                if (c == '\'') {
                    if (i + 1 < sql.length() && sql.charAt(i + 1) == '\'') { i++; } else inStr = false;
                }
            } else if (c == '\'') {
                inStr = true;
            } else if (c == '?') {
                n++;
            }
        }
        return n;
    }

    static String bind(String sql, Object[] params) {
        if (params.length == 0) return sql;
        StringBuilder b = new StringBuilder(sql.length() + 16);
        boolean inStr = false;
        int idx = 0;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (inStr) {
                b.append(c);
                if (c == '\'') {
                    if (i + 1 < sql.length() && sql.charAt(i + 1) == '\'') { b.append('\''); i++; }
                    else inStr = false;
                }
                continue;
            }
            if (c == '\'') { inStr = true; b.append(c); continue; }
            if (c == '?') {
                if (idx >= params.length) throw new SkaidbException("more placeholders than parameters");
                b.append(quote(params[idx++]));
                continue;
            }
            b.append(c);
        }
        return b.toString();
    }

    static String quote(Object v) {
        if (v == null) return "NULL";
        if (v instanceof Boolean) return ((Boolean) v) ? "TRUE" : "FALSE";
        if (v instanceof Float || v instanceof Double) {
            double d = ((Number) v).doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d)) throw new SkaidbException("cannot bind NaN/Infinity");
            return Double.toString(d);
        }
        if (v instanceof Number) return v.toString();
        if (v instanceof byte[]) return "'" + hex((byte[]) v) + "'";
        if (v instanceof Instant) return Long.toString(((Instant) v).toEpochMilli());
        // strings, UUID, anything else -> quoted string with '' escaping
        return "'" + v.toString().replace("'", "''") + "'";
    }

    // ---- crypto + small helpers -------------------------------------------

    private static byte[] hmac(byte[] key, byte[] msg) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            // SecretKeySpec rejects a zero-length key; the empty-password
            // (anonymous) path only needs a well-defined value the server ignores.
            mac.init(new SecretKeySpec(key.length == 0 ? new byte[1] : key, "HmacSHA256"));
            return mac.doFinal(msg);
        } catch (Exception e) {
            throw new SkaidbException("hmac failed: " + e.getMessage(), e);
        }
    }

    private static byte[] sha256(byte[] data) {
        try { return MessageDigest.getInstance("SHA-256").digest(data); }
        catch (Exception e) { throw new SkaidbException("sha256 failed", e); }
    }

    // PBKDF2-HMAC-SHA256 (hand-rolled to avoid empty-password provider quirks).
    private static byte[] pbkdf2(byte[] password, byte[] salt, int iterations, int dkLen) {
        int hLen = 32;
        int blocks = (dkLen + hLen - 1) / hLen;
        byte[] out = new byte[blocks * hLen];
        byte[] key = password.length == 0 ? new byte[0] : password;
        for (int block = 1; block <= blocks; block++) {
            byte[] in = new byte[salt.length + 4];
            System.arraycopy(salt, 0, in, 0, salt.length);
            in[salt.length] = (byte) (block >>> 24);
            in[salt.length + 1] = (byte) (block >>> 16);
            in[salt.length + 2] = (byte) (block >>> 8);
            in[salt.length + 3] = (byte) block;
            byte[] u = hmac(key, in);
            byte[] t = u.clone();
            for (int i = 1; i < iterations; i++) {
                u = hmac(key, u);
                for (int j = 0; j < t.length; j++) t[j] ^= u[j];
            }
            System.arraycopy(t, 0, out, (block - 1) * hLen, hLen);
        }
        byte[] dk = new byte[dkLen];
        System.arraycopy(out, 0, dk, 0, dkLen);
        return dk;
    }

    private static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) sb.append(Character.forDigit((x >> 4) & 0xf, 16)).append(Character.forDigit(x & 0xf, 16));
        return sb.toString();
    }

    // ---- little binary helpers --------------------------------------------

    static final class Buf {
        private final java.io.ByteArrayOutputStream b = new java.io.ByteArrayOutputStream();
        Buf u8(int v) { b.write(v & 0xff); return this; }
        Buf u32(int v) { b.write(v & 0xff); b.write((v >>> 8) & 0xff); b.write((v >>> 16) & 0xff); b.write((v >>> 24) & 0xff); return this; }
        Buf raw(byte[] x) { b.write(x, 0, x.length); return this; }
        Buf str(String s) { byte[] x = s.getBytes(StandardCharsets.UTF_8); u32(x.length); return raw(x); }
        byte[] toBytes() { return b.toByteArray(); }
    }

    static final class Reader {
        private final byte[] buf;
        private int pos = 0;
        Reader(byte[] buf) { this.buf = buf; }
        byte[] take(int n) {
            if (pos + n > buf.length) throw new SkaidbException("truncated server message");
            byte[] s = new byte[n];
            System.arraycopy(buf, pos, s, 0, n);
            pos += n;
            return s;
        }
        int u8() { return take(1)[0] & 0xff; }
        int u32() {
            byte[] b = take(4);
            return (b[0] & 0xff) | ((b[1] & 0xff) << 8) | ((b[2] & 0xff) << 16) | ((b[3] & 0xff) << 24);
        }
        long i64() {
            byte[] b = take(8);
            long v = 0;
            for (int i = 7; i >= 0; i--) v = (v << 8) | (b[i] & 0xffL);
            return v;
        }
        long u64() { return i64(); } // affected counts fit in a signed long
        byte[] blob() { return take(u32()); }
        String text() { return new String(blob(), StandardCharsets.UTF_8); }
    }
}
