// Package skaidb is a database/sql driver for skaidb.
//
// Import it for its side effect (it registers the "skaidb" driver) and use the
// standard database/sql API — there is nothing new to learn:
//
//	import (
//		"database/sql"
//		_ "github.com/porcupin26/skaidb/drivers/go"
//	)
//
//	db, _ := sql.Open("skaidb", "skaidb://user:pass@localhost:7000/?consistency=quorum")
//	rows, _ := db.Query("SELECT id, name FROM users WHERE id = ?", 1)
//
// Placeholders use "?" (the database/sql norm). Pure standard library: no
// third-party dependencies.
package skaidb

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

func init() { sql.Register("skaidb", &drv{}) }

const (
	consistencyOne    = 0
	consistencyQuorum = 1
	consistencyAll    = 2
)

var nonceCounter uint64

// ---- driver registration --------------------------------------------------

type drv struct{}

func (d *drv) Open(dsn string) (driver.Conn, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return dial(cfg)
}

type config struct {
	addr        string
	user        string
	password    string
	consistency byte
}

func parseDSN(dsn string) (config, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return config{}, fmt.Errorf("skaidb: bad DSN: %w", err)
	}
	if u.Scheme != "skaidb" {
		return config{}, fmt.Errorf("skaidb: DSN scheme must be skaidb://")
	}
	cfg := config{addr: u.Host, user: "anonymous", consistency: consistencyQuorum}
	if u.User != nil {
		cfg.user = u.User.Username()
		if p, ok := u.User.Password(); ok {
			cfg.password = p
		}
	}
	if !strings.Contains(cfg.addr, ":") {
		cfg.addr += ":7000"
	}
	switch strings.ToLower(u.Query().Get("consistency")) {
	case "one":
		cfg.consistency = consistencyOne
	case "all":
		cfg.consistency = consistencyAll
	case "", "quorum":
		cfg.consistency = consistencyQuorum
	default:
		return config{}, fmt.Errorf("skaidb: bad consistency %q", u.Query().Get("consistency"))
	}
	return cfg, nil
}

// ---- connection ------------------------------------------------------------

type conn struct {
	nc          net.Conn
	consistency byte
	closed      bool
}

func dial(cfg config) (*conn, error) {
	nc, err := net.DialTimeout("tcp", cfg.addr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("skaidb: connect failed: %w", err)
	}
	if tcp, ok := nc.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	c := &conn{nc: nc, consistency: cfg.consistency}
	if err := c.handshake(cfg.user, cfg.password); err != nil {
		nc.Close()
		return nil, err
	}
	return c, nil
}

func (c *conn) writeFrame(payload []byte) error {
	var head [4]byte
	binary.BigEndian.PutUint32(head[:], uint32(len(payload)))
	if _, err := c.nc.Write(head[:]); err != nil {
		return err
	}
	_, err := c.nc.Write(payload)
	return err
}

func (c *conn) readFrame() ([]byte, error) {
	var head [4]byte
	if _, err := io.ReadFull(c.nc, head[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(head[:])
	buf := make([]byte, n)
	if _, err := io.ReadFull(c.nc, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func (c *conn) handshake(user, password string) error {
	clientNonce := fmt.Sprintf("go%d.%d", time.Now().UnixNano()&0xffff, atomic.AddUint64(&nonceCounter, 1))

	start := []byte{10}
	start = appendStr(start, user)
	start = appendStr(start, clientNonce)
	if err := c.writeFrame(start); err != nil {
		return err
	}

	r := &reader{buf: mustFrame(c.readFrame())}
	if r.err == nil && r.u8() != 11 {
		return fmt.Errorf("skaidb: bad handshake challenge")
	}
	salt := r.blob()
	iterations := r.u32()
	serverNonce := r.text()
	if r.err != nil {
		return fmt.Errorf("skaidb: handshake decode: %w", r.err)
	}

	authMessage := []byte(strings.Join(
		[]string{user, clientNonce, serverNonce, hex.EncodeToString(salt), strconv.FormatUint(uint64(iterations), 10)},
		"\x00"))
	salted := pbkdf2SHA256([]byte(password), salt, int(iterations), 32)
	clientKey := hmacSHA256(salted, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)
	clientSig := hmacSHA256(storedKey[:], authMessage)
	proof := make([]byte, 32)
	for i := range proof {
		proof[i] = clientKey[i] ^ clientSig[i]
	}
	if err := c.writeFrame(append([]byte{12}, proof...)); err != nil {
		return err
	}

	r = &reader{buf: mustFrame(c.readFrame())}
	if r.err != nil {
		return r.err
	}
	if r.u8() != 13 {
		return fmt.Errorf("skaidb: bad handshake outcome")
	}
	if r.u8() == 1 {
		serverSig := r.take(32)
		if password != "" {
			serverKey := hmacSHA256(salted, []byte("Server Key"))
			expected := hmacSHA256(serverKey, authMessage)
			if subtle.ConstantTimeCompare(serverSig, expected) != 1 {
				return fmt.Errorf("skaidb: server signature mismatch (mutual auth failed)")
			}
		}
		return nil
	}
	return fmt.Errorf("skaidb: authentication denied: %s", r.text())
}

// ---- database/sql/driver interfaces ---------------------------------------

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{c: c, query: query, n: strings.Count(stripStrings(query), "?")}, nil
}

func (c *conn) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	return c.nc.Close()
}

// skaidb is non-transactional; expose that honestly.
func (c *conn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("skaidb: transactions are not supported")
}

func (c *conn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	sqlText, err := bind(query, args)
	if err != nil {
		return nil, err
	}
	return c.query(sqlText)
}

func (c *conn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	sqlText, err := bind(query, args)
	if err != nil {
		return nil, err
	}
	return c.exec(sqlText)
}

func (c *conn) sendQuery(sqlText string) (*reader, error) {
	if c.closed {
		return nil, fmt.Errorf("skaidb: connection closed")
	}
	body := []byte(sqlText)
	req := make([]byte, 0, 6+len(body))
	req = append(req, 1, c.consistency)
	req = binary.LittleEndian.AppendUint32(req, uint32(len(body)))
	req = append(req, body...)
	if err := c.writeFrame(req); err != nil {
		return nil, err
	}
	frame, err := c.readFrame()
	if err != nil {
		return nil, err
	}
	return &reader{buf: frame}, nil
}

func (c *conn) query(sqlText string) (driver.Rows, error) {
	r, err := c.sendQuery(sqlText)
	if err != nil {
		return nil, err
	}
	switch tag := r.u8(); tag {
	case 0: // Rows
		ncols := int(r.u32())
		cols := make([]string, ncols)
		for i := range cols {
			cols[i] = r.text()
		}
		nrows := int(r.u32())
		data := make([][]driver.Value, nrows)
		for i := 0; i < nrows; i++ {
			ncells := int(r.u32())
			row := make([]driver.Value, ncells)
			for j := 0; j < ncells; j++ {
				row[j] = decodeValue(&reader{buf: r.blob()})
			}
			data[i] = row
		}
		if r.err != nil {
			return nil, r.err
		}
		return &rows{cols: cols, data: data}, nil
	case 1: // Mutation: a SELECT-less result; surface an empty row set
		return &rows{cols: []string{}, data: nil}, nil
	case 2: // Ddl
		return &rows{cols: []string{}, data: nil}, nil
	case 3:
		return nil, fmt.Errorf("skaidb: %s", r.text())
	default:
		return nil, fmt.Errorf("skaidb: unknown response tag %d", tag)
	}
}

func (c *conn) exec(sqlText string) (driver.Result, error) {
	r, err := c.sendQuery(sqlText)
	if err != nil {
		return nil, err
	}
	switch tag := r.u8(); tag {
	case 0: // Rows returned to Exec — discard, report 0 affected
		return result{affected: 0}, nil
	case 1:
		return result{affected: int64(r.u64())}, nil
	case 2:
		return result{affected: 0}, nil
	case 3:
		return nil, fmt.Errorf("skaidb: %s", r.text())
	default:
		return nil, fmt.Errorf("skaidb: unknown response tag %d", tag)
	}
}

// ---- stmt / rows / result --------------------------------------------------

type stmt struct {
	c     *conn
	query string
	n     int
}

func (s *stmt) Close() error  { return nil }
func (s *stmt) NumInput() int { return s.n }
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	sqlText, err := bind(s.query, named(args))
	if err != nil {
		return nil, err
	}
	return s.c.query(sqlText)
}
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	sqlText, err := bind(s.query, named(args))
	if err != nil {
		return nil, err
	}
	return s.c.exec(sqlText)
}

type rows struct {
	cols []string
	data [][]driver.Value
	pos  int
}

func (r *rows) Columns() []string { return r.cols }
func (r *rows) Close() error      { return nil }
func (r *rows) Next(dest []driver.Value) error {
	if r.pos >= len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.pos])
	r.pos++
	return nil
}

type result struct{ affected int64 }

func (r result) LastInsertId() (int64, error) { return 0, fmt.Errorf("skaidb: no LastInsertId") }
func (r result) RowsAffected() (int64, error) { return r.affected, nil }

// ---- value decoding (§4) ---------------------------------------------------

// decodeValue returns a database/sql-compatible driver.Value. Composite values
// (Array, Document) are surfaced as a JSON string so they can be Scanned into a
// string or []byte.
func decodeValue(r *reader) driver.Value {
	v := decodeNative(r)
	switch v.(type) {
	case []interface{}, map[string]interface{}:
		return jsonString(v)
	default:
		return v
	}
}

// decodeNative decodes into a plain Go value, recursing into composites.
func decodeNative(r *reader) interface{} {
	switch tag := r.u8(); tag {
	case 0:
		return nil
	case 1:
		return r.u8() != 0
	case 2:
		return int64(r.u64())
	case 3:
		return math.Float64frombits(r.u64())
	case 4: // Decimal -> string
		mant := r.take(16)
		scale := r.u32()
		return decimalString(mant, scale)
	case 5:
		return r.text()
	case 6:
		return append([]byte(nil), r.blob()...)
	case 7: // Uuid -> canonical string
		b := r.take(16)
		return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
	case 8: // Timestamp(ms) -> time.Time
		return time.UnixMilli(int64(r.u64())).UTC()
	case 9: // Array
		n := int(r.u32())
		arr := make([]interface{}, n)
		for i := range arr {
			arr[i] = decodeNative(r)
		}
		return arr
	case 10: // Document (insertion order not preserved in a Go map)
		n := int(r.u32())
		m := make(map[string]interface{}, n)
		for i := 0; i < n; i++ {
			k := r.text()
			m[k] = decodeNative(r)
		}
		return m
	default:
		r.err = fmt.Errorf("skaidb: unknown value tag %d", tag)
		return nil
	}
}

type reader struct {
	buf []byte
	pos int
	err error
}

func (r *reader) take(n int) []byte {
	if r.err != nil {
		return make([]byte, n)
	}
	if r.pos+n > len(r.buf) {
		r.err = fmt.Errorf("skaidb: truncated server message")
		return make([]byte, n)
	}
	b := r.buf[r.pos : r.pos+n]
	r.pos += n
	return b
}
func (r *reader) u8() byte    { return r.take(1)[0] }
func (r *reader) u32() uint32 { return binary.LittleEndian.Uint32(r.take(4)) }
func (r *reader) u64() uint64 { return binary.LittleEndian.Uint64(r.take(8)) }
func (r *reader) blob() []byte {
	n := int(r.u32())
	return r.take(n)
}
func (r *reader) text() string { return string(r.blob()) }

// ---- parameter binding (§5) ------------------------------------------------

func named(args []driver.Value) []driver.NamedValue {
	out := make([]driver.NamedValue, len(args))
	for i, a := range args {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: a}
	}
	return out
}

func bind(query string, args []driver.NamedValue) (string, error) {
	if len(args) == 0 {
		return query, nil
	}
	var b strings.Builder
	inStr := false
	idx := 0
	for i := 0; i < len(query); i++ {
		ch := query[i]
		if inStr {
			b.WriteByte(ch)
			if ch == '\'' {
				if i+1 < len(query) && query[i+1] == '\'' {
					b.WriteByte('\'')
					i++
				} else {
					inStr = false
				}
			}
			continue
		}
		if ch == '\'' {
			inStr = true
			b.WriteByte(ch)
			continue
		}
		if ch == '?' {
			if idx >= len(args) {
				return "", fmt.Errorf("skaidb: more placeholders than parameters")
			}
			s, err := quote(args[idx].Value)
			if err != nil {
				return "", err
			}
			b.WriteString(s)
			idx++
			continue
		}
		b.WriteByte(ch)
	}
	if idx != len(args) {
		return "", fmt.Errorf("skaidb: more parameters than placeholders")
	}
	return b.String(), nil
}

func quote(v driver.Value) (string, error) {
	switch x := v.(type) {
	case nil:
		return "NULL", nil
	case bool:
		if x {
			return "TRUE", nil
		}
		return "FALSE", nil
	case int64:
		return strconv.FormatInt(x, 10), nil
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return "", fmt.Errorf("skaidb: cannot bind NaN/Infinity")
		}
		return strconv.FormatFloat(x, 'g', -1, 64), nil
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'", nil
	case []byte:
		return "'" + hex.EncodeToString(x) + "'", nil
	case time.Time:
		return strconv.FormatInt(x.UnixMilli(), 10), nil
	default:
		return "", fmt.Errorf("skaidb: cannot bind value of type %T", v)
	}
}

func stripStrings(s string) string {
	var b strings.Builder
	inStr := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inStr {
			if ch == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++
					continue
				}
				inStr = false
			}
			continue
		}
		if ch == '\'' {
			inStr = true
			continue
		}
		b.WriteByte(ch)
	}
	return b.String()
}

// ---- small crypto + helpers ------------------------------------------------

func hmacSHA256(key, msg []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(msg)
	return m.Sum(nil)
}

// pbkdf2SHA256 implements PBKDF2-HMAC-SHA256 (kept local to avoid x/crypto).
func pbkdf2SHA256(password, salt []byte, iter, keyLen int) []byte {
	hashLen := sha256.Size
	numBlocks := (keyLen + hashLen - 1) / hashLen
	var out []byte
	for block := 1; block <= numBlocks; block++ {
		var idx [4]byte
		binary.BigEndian.PutUint32(idx[:], uint32(block))
		u := hmacSHA256(password, append(append([]byte(nil), salt...), idx[:]...))
		t := append([]byte(nil), u...)
		for i := 1; i < iter; i++ {
			u = hmacSHA256(password, u)
			for j := range t {
				t[j] ^= u[j]
			}
		}
		out = append(out, t...)
	}
	return out[:keyLen]
}

func appendStr(b []byte, s string) []byte {
	b = binary.LittleEndian.AppendUint32(b, uint32(len(s)))
	return append(b, s...)
}

func mustFrame(b []byte, err error) []byte {
	if err != nil {
		return nil
	}
	return b
}

func decimalString(mant []byte, scale uint32) string {
	// little-endian signed 16-byte integer -> decimal string with scale
	v := bigIntFromLE(mant)
	s := v.String()
	if scale == 0 {
		return s
	}
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	for uint32(len(s)) <= scale {
		s = "0" + s
	}
	point := uint32(len(s)) - scale
	res := s[:point] + "." + s[point:]
	if neg {
		res = "-" + res
	}
	return res
}

// bigIntFromLE interprets b as a little-endian two's-complement integer.
func bigIntFromLE(b []byte) *big.Int {
	le := make([]byte, len(b))
	for i := range b {
		le[i] = b[len(b)-1-i] // reverse to big-endian
	}
	v := new(big.Int).SetBytes(le)
	if len(b) > 0 && b[len(b)-1]&0x80 != 0 { // negative
		v.Sub(v, new(big.Int).Lsh(big.NewInt(1), uint(8*len(b))))
	}
	return v
}

func jsonString(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "null"
	}
	return string(b)
}
