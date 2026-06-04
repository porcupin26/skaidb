<?php

declare(strict_types=1);

/**
 * skaidb — PHP driver.
 *
 * A pure-PHP client for the skaidb binary wire protocol. The public API is
 * modelled on PDO/PDOStatement so it should feel immediately familiar:
 *
 *     use Skaidb\Connection;
 *
 *     $db = new Connection('localhost', 7000, 'skaidb', 'secret');
 *
 *     $db->exec('CREATE TABLE users (PRIMARY KEY (id))');
 *
 *     $stmt = $db->prepare('INSERT INTO users (id, name) VALUES (?, ?)');
 *     $stmt->execute([1, 'Ada']);
 *
 *     $stmt = $db->prepare('SELECT id, name FROM users WHERE id = ?');
 *     $stmt->execute([1]);
 *     foreach ($stmt->fetchAll() as $row) {
 *         print_r($row);     // ['id' => 1, 'name' => 'Ada']
 *     }
 *
 * No Composer or PECL dependencies — only the bundled `hash` extension (and
 * optionally `sockets`/`bcmath`/`gmp`). Target PHP 8.0+.
 *
 * The protocol uses '?' positional placeholders. Parameters passed to
 * `execute()` are quoted/escaped client-side (the wire protocol has no
 * server-side bind parameters), so values like "O'Brien" are safe.
 */

namespace Skaidb;

use DateTimeImmutable;
use DateTimeZone;
use Exception;

/**
 * Thrown for every skaidb error: connection/transport failures, handshake
 * denials, and server-side statement errors. Modelled on PDOException.
 */
class SkaidbException extends Exception
{
}

/**
 * A connection to one skaidb node. Runs the SCRAM-SHA-256 handshake in the
 * constructor, then exposes a PDO-shaped query API.
 */
class Connection
{
    public const ONE = 0;
    public const QUORUM = 1;
    public const ALL = 2;

    /** Value type tags (§4 of PROTOCOL.md). */
    private const TAG_NULL = 0;
    private const TAG_BOOL = 1;
    private const TAG_INT = 2;
    private const TAG_FLOAT = 3;
    private const TAG_DECIMAL = 4;
    private const TAG_STRING = 5;
    private const TAG_BYTES = 6;
    private const TAG_UUID = 7;
    private const TAG_TIMESTAMP = 8;
    private const TAG_ARRAY = 9;
    private const TAG_DOCUMENT = 10;

    private const CONSISTENCY_BY_NAME = ['ONE' => 0, 'QUORUM' => 1, 'ALL' => 2];

    /** @var resource|null the TCP stream */
    private $sock;

    private int $consistency;

    private bool $closed = false;

    private static int $nonceCounter = 0;

    /**
     * Open a connection and complete the SCRAM-SHA-256 handshake.
     *
     * @param string     $host        node hostname
     * @param int        $port        binary protocol port (default 7000)
     * @param string     $user        username ("anonymous" for auth-disabled servers)
     * @param string     $password    password (empty for anonymous)
     * @param int|string $consistency 'ONE'/'QUORUM'/'ALL' or 0/1/2 (default QUORUM)
     * @param float      $timeout     connect/read timeout in seconds
     *
     * @throws SkaidbException on connect or auth failure
     */
    public function __construct(
        string $host = 'localhost',
        int $port = 7000,
        string $user = 'anonymous',
        string $password = '',
        $consistency = 'QUORUM',
        float $timeout = 10.0
    ) {
        $this->consistency = self::resolveConsistency($consistency);

        $errno = 0;
        $errstr = '';
        // TCP_NODELAY via stream context where supported (PHP 7.1+).
        $ctx = stream_context_create(['socket' => ['tcp_nodelay' => true]]);
        $sock = @stream_socket_client(
            "tcp://{$host}:{$port}",
            $errno,
            $errstr,
            $timeout,
            STREAM_CLIENT_CONNECT,
            $ctx
        );
        if ($sock === false) {
            throw new SkaidbException("connect failed: {$errstr} ({$errno})");
        }
        $this->sock = $sock;
        // Read timeout for fread loops.
        stream_set_timeout($this->sock, (int) $timeout, (int) (($timeout - (int) $timeout) * 1_000_000));

        try {
            $this->handshake($user, $password);
        } catch (SkaidbException $e) {
            $this->close();
            throw $e;
        }
    }

    public function __destruct()
    {
        $this->close();
    }

    /**
     * Resolve a consistency level (name or 0/1/2) to its integer code.
     *
     * @param int|string $value
     */
    public static function resolveConsistency($value): int
    {
        if (is_int($value)) {
            if (!in_array($value, [0, 1, 2], true)) {
                throw new SkaidbException("invalid consistency {$value}");
            }
            return $value;
        }
        $key = strtoupper((string) $value);
        if (!isset(self::CONSISTENCY_BY_NAME[$key])) {
            throw new SkaidbException("invalid consistency {$value}");
        }
        return self::CONSISTENCY_BY_NAME[$key];
    }

    /** Set the default consistency level for subsequent queries. */
    public function setConsistency($consistency): void
    {
        $this->consistency = self::resolveConsistency($consistency);
    }

    /** Current default consistency code (0/1/2). */
    public function getConsistency(): int
    {
        return $this->consistency;
    }

    /**
     * Prepare a statement with '?' positional placeholders. Returns a
     * Statement you can `execute()` with parameters.
     */
    public function prepare(string $sql): Statement
    {
        return new Statement($this, $sql);
    }

    /**
     * Run SQL with no parameters and return a Statement holding the result.
     * Convenience for SELECTs without binds (mirrors PDO::query).
     */
    public function query(string $sql): Statement
    {
        $stmt = new Statement($this, $sql);
        $stmt->execute();
        return $stmt;
    }

    /**
     * Execute a statement and return the number of affected rows (PDO::exec
     * semantics). For DDL or non-mutation statements this is 0.
     */
    public function exec(string $sql): int
    {
        $stmt = new Statement($this, $sql);
        $stmt->execute();
        return $stmt->rowCount();
    }

    public function close(): void
    {
        if (!$this->closed) {
            $this->closed = true;
            if (is_resource($this->sock)) {
                @fclose($this->sock);
            }
            $this->sock = null;
        }
    }

    // ---- internal: query execution (called by Statement) ------------------

    /**
     * Send one query frame and decode the response.
     *
     * @internal
     *
     * @return array{kind:string, columns:array<int,string>, rows:array<int,array<int,mixed>>, affected:int}
     */
    public function runQuery(string $sql, int $consistency): array
    {
        if ($this->closed) {
            throw new SkaidbException('connection is closed');
        }
        $sqlBytes = $sql; // already a UTF-8 byte string
        // OP_QUERY=1, consistency u8, u32 LE sql_len, sql bytes.
        $req = pack('C', 1) . pack('C', $consistency)
            . pack('V', strlen($sqlBytes)) . $sqlBytes;
        $this->writeFrame($req);

        $r = new Reader($this->readFrame());
        $tag = $r->u8();
        if ($tag === 0) { // Rows
            $ncols = $r->u32();
            $columns = [];
            for ($i = 0; $i < $ncols; $i++) {
                $columns[] = $r->text();
            }
            $nrows = $r->u32();
            $rows = [];
            for ($i = 0; $i < $nrows; $i++) {
                $ncells = $r->u32();
                $row = [];
                for ($c = 0; $c < $ncells; $c++) {
                    // Each cell is a length-prefixed blob holding one Value.
                    $cell = new Reader($r->blob());
                    $row[] = $this->decodeValue($cell);
                }
                $rows[] = $row;
            }
            return ['kind' => 'rows', 'columns' => $columns, 'rows' => $rows, 'affected' => 0];
        }
        if ($tag === 1) { // Mutation
            $affected = $r->u64();
            return ['kind' => 'mutation', 'columns' => [], 'rows' => [], 'affected' => $affected];
        }
        if ($tag === 2) { // Ddl
            return ['kind' => 'ddl', 'columns' => [], 'rows' => [], 'affected' => 0];
        }
        if ($tag === 3) { // Error
            throw new SkaidbException($r->text());
        }
        throw new SkaidbException("unknown response tag {$tag}");
    }

    // ---- framing ----------------------------------------------------------

    private function writeFrame(string $payload): void
    {
        // u32 BE length prefix, then payload.
        $frame = pack('N', strlen($payload)) . $payload;
        $total = strlen($frame);
        $written = 0;
        while ($written < $total) {
            $n = @fwrite($this->sock, substr($frame, $written));
            if ($n === false || $n === 0) {
                throw new SkaidbException('connection closed by server (write)');
            }
            $written += $n;
        }
    }

    private function readFrame(): string
    {
        $head = $this->readExact(4);
        $len = unpack('N', $head)[1]; // u32 BE
        if ($len === 0) {
            return '';
        }
        return $this->readExact($len);
    }

    /** Read exactly $n bytes; fread may return short, so loop. */
    private function readExact(int $n): string
    {
        $buf = '';
        $remaining = $n;
        while ($remaining > 0) {
            $chunk = fread($this->sock, $remaining);
            if ($chunk === false || $chunk === '') {
                $meta = is_resource($this->sock) ? stream_get_meta_data($this->sock) : ['timed_out' => false];
                if (!empty($meta['timed_out'])) {
                    throw new SkaidbException('connection timed out');
                }
                throw new SkaidbException('connection closed by server');
            }
            $buf .= $chunk;
            $remaining -= strlen($chunk);
        }
        return $buf;
    }

    // ---- handshake (§2) ---------------------------------------------------

    private function handshake(string $user, string $password): void
    {
        self::$nonceCounter++;
        $clientNonce = 'php' . getmypid() . '.' . self::$nonceCounter . '.' . spl_object_id($this);

        // AuthStart: tag 10, str username, str client_nonce.
        $start = pack('C', 10) . self::encStr($user) . self::encStr($clientNonce);
        $this->writeFrame($start);

        // AuthChallenge: tag 11, blob salt, u32 LE iterations, str server_nonce.
        $r = new Reader($this->readFrame());
        if ($r->u8() !== 11) {
            throw new SkaidbException('bad handshake challenge');
        }
        $salt = $r->blob();
        $iterations = $r->u32();
        $serverNonce = $r->text();

        $saltHex = bin2hex($salt); // lowercase hex
        $authMessage = implode("\0", [$user, $clientNonce, $serverNonce, $saltHex, (string) $iterations]);

        [$proof, $expectedServerSig] = self::scram($password, $salt, $iterations, $authMessage);

        // AuthFinish: tag 12, 32 raw proof bytes (NOT length-prefixed).
        $this->writeFrame(pack('C', 12) . $proof);

        // AuthOutcome: tag 13, u8 ok_flag, then 32-byte sig or str reason.
        $r = new Reader($this->readFrame());
        if ($r->u8() !== 13) {
            throw new SkaidbException('bad handshake outcome');
        }
        if ($r->u8() === 1) {
            $serverSig = $r->take(32);
            if ($password !== '' && !hash_equals($expectedServerSig, $serverSig)) {
                throw new SkaidbException('server signature mismatch (mutual auth failed)');
            }
        } else {
            throw new SkaidbException('authentication denied: ' . $r->text());
        }
    }

    /**
     * Compute the SCRAM-SHA-256 client proof and expected server signature.
     * All crypto outputs are RAW bytes.
     *
     * @return array{0:string,1:string} [proof, expectedServerSig]
     */
    private static function scram(string $password, string $salt, int $iterations, string $authMessage): array
    {
        $salted = hash_pbkdf2('sha256', $password, $salt, $iterations, 32, true); // 32 raw bytes
        $clientKey = hash_hmac('sha256', 'Client Key', $salted, true);            // HMAC(key=salted, msg)
        $storedKey = hash('sha256', $clientKey, true);
        $clientSig = hash_hmac('sha256', $authMessage, $storedKey, true);
        $proof = $clientKey ^ $clientSig; // PHP XORs byte strings of equal length

        $serverKey = hash_hmac('sha256', 'Server Key', $salted, true);
        $serverSig = hash_hmac('sha256', $authMessage, $serverKey, true);

        return [$proof, $serverSig];
    }

    /** Encode a str field: u32 LE length + UTF-8 bytes. */
    private static function encStr(string $s): string
    {
        return pack('V', strlen($s)) . $s;
    }

    // ---- value decoding (§4) ----------------------------------------------

    /**
     * @return mixed
     */
    private function decodeValue(Reader $r)
    {
        $tag = $r->u8();
        switch ($tag) {
            case self::TAG_NULL:
                return null;
            case self::TAG_BOOL:
                return $r->u8() !== 0;
            case self::TAG_INT:
                return $r->i64();
            case self::TAG_FLOAT:
                return $r->f64();
            case self::TAG_DECIMAL:
                $mantissa = self::i128ToString($r->take(16));
                $scale = $r->u32();
                return self::scaleDecimal($mantissa, $scale);
            case self::TAG_STRING:
                return $r->text();
            case self::TAG_BYTES:
                return $r->blob();
            case self::TAG_UUID:
                return self::uuidToString($r->take(16));
            case self::TAG_TIMESTAMP:
                $ms = $r->i64();
                return self::msToDateTime($ms);
            case self::TAG_ARRAY:
                $count = $r->u32();
                $out = [];
                for ($i = 0; $i < $count; $i++) {
                    $out[] = $this->decodeValue($r);
                }
                return $out;
            case self::TAG_DOCUMENT:
                $count = $r->u32();
                $out = [];
                for ($i = 0; $i < $count; $i++) {
                    $key = $r->text();
                    $out[$key] = $this->decodeValue($r);
                }
                return $out;
        }
        throw new SkaidbException("unknown value tag {$tag}");
    }

    /** Convert a 16-byte little-endian two's-complement integer to a decimal string. */
    private static function i128ToString(string $bytes): string
    {
        // Determine sign from the most-significant byte (last byte, LE).
        $negative = (ord($bytes[15]) & 0x80) !== 0;

        if (function_exists('gmp_init')) {
            $hex = bin2hex(strrev($bytes)); // big-endian hex
            $val = gmp_init($hex, 16);
            if ($negative) {
                // two's complement: subtract 2^128
                $val = gmp_sub($val, gmp_pow(2, 128));
            }
            return gmp_strval($val);
        }

        if (function_exists('bcadd')) {
            // Build magnitude via bcmath from big-endian bytes.
            $beBytes = strrev($bytes);
            $val = '0';
            for ($i = 0; $i < 16; $i++) {
                $val = bcadd(bcmul($val, '256'), (string) ord($beBytes[$i]));
            }
            if ($negative) {
                // two's complement: value - 2^128
                $two128 = bcpow('2', '128');
                $val = bcsub($val, $two128);
            }
            return $val;
        }

        // Fallback: no bigint library. Interpret the low 64 bits as a signed
        // i64 (correct for any mantissa that fits in 64 bits — the common case;
        // larger mantissas may be approximate). 'P' = u64 LE, and on 64-bit PHP
        // a set top bit already yields the correct signed two's-complement int.
        $low = substr($bytes, 0, 8);
        return (string) unpack('P', $low)[1];
    }

    /**
     * Apply a base-10 scale to an integer mantissa string, returning an exact
     * decimal string. value = mantissa / 10^scale.
     */
    private static function scaleDecimal(string $mantissa, int $scale): string
    {
        if ($scale === 0) {
            return $mantissa;
        }
        $neg = false;
        if ($mantissa !== '' && $mantissa[0] === '-') {
            $neg = true;
            $mantissa = substr($mantissa, 1);
        }
        $mantissa = ltrim($mantissa, '0');
        if ($mantissa === '') {
            $mantissa = '0';
        }
        if (strlen($mantissa) <= $scale) {
            $mantissa = str_repeat('0', $scale - strlen($mantissa) + 1) . $mantissa;
        }
        $point = strlen($mantissa) - $scale;
        $result = substr($mantissa, 0, $point) . '.' . substr($mantissa, $point);
        return ($neg ? '-' : '') . $result;
    }

    /** Format 16 raw UUID bytes as canonical lowercase 8-4-4-4-12. */
    private static function uuidToString(string $bytes): string
    {
        $hex = bin2hex($bytes);
        return substr($hex, 0, 8) . '-' . substr($hex, 8, 4) . '-'
            . substr($hex, 12, 4) . '-' . substr($hex, 16, 4) . '-' . substr($hex, 20, 12);
    }

    /** Unix milliseconds → DateTimeImmutable in UTC. */
    private static function msToDateTime(int $ms): DateTimeImmutable
    {
        $sec = intdiv($ms, 1000);
        $remMs = $ms - $sec * 1000; // keep sign consistent with $sec for negatives
        if ($remMs < 0) {
            $sec -= 1;
            $remMs += 1000;
        }
        $micros = $remMs * 1000;
        $str = sprintf('%d.%06d', $sec, $micros);
        $dt = DateTimeImmutable::createFromFormat('U.u', $str, new DateTimeZone('UTC'));
        if ($dt === false) {
            // Fallback for environments rejecting negative U: build from epoch.
            $dt = (new DateTimeImmutable('@0'))->modify("{$sec} seconds");
        }
        return $dt->setTimezone(new DateTimeZone('UTC'));
    }

    // ---- client-side parameter binding (§5) -------------------------------

    /**
     * Interpolate '?' placeholders in $sql with quoted $params.
     *
     * @internal
     *
     * @param array<int,mixed> $params
     */
    public static function bindParams(string $sql, array $params): string
    {
        // Re-index in case of associative input.
        $params = array_values($params);

        if (count($params) === 0) {
            if (strpos(self::stripStrings($sql), '?') !== false) {
                throw new SkaidbException('query has placeholders but no parameters given');
            }
            return $sql;
        }

        $out = '';
        $inStr = false;
        $i = 0;
        $n = strlen($sql);
        $used = 0;
        $total = count($params);
        while ($i < $n) {
            $ch = $sql[$i];
            if ($inStr) {
                $out .= $ch;
                if ($ch === "'") {
                    if ($i + 1 < $n && $sql[$i + 1] === "'") {
                        $out .= "'";
                        $i += 2;
                        continue;
                    }
                    $inStr = false;
                }
                $i++;
                continue;
            }
            if ($ch === "'") {
                $inStr = true;
                $out .= $ch;
                $i++;
                continue;
            }
            if ($ch === '?') {
                if ($used >= $total) {
                    throw new SkaidbException('more placeholders than parameters');
                }
                $out .= self::quote($params[$used]);
                $used++;
                $i++;
                continue;
            }
            $out .= $ch;
            $i++;
        }
        if ($used < $total) {
            throw new SkaidbException('more parameters than placeholders');
        }
        return $out;
    }

    /** SQL-quote a single bound value. */
    private static function quote($arg): string
    {
        if ($arg === null) {
            return 'NULL';
        }
        if (is_bool($arg)) {
            return $arg ? 'TRUE' : 'FALSE';
        }
        if (is_int($arg)) {
            return (string) $arg;
        }
        if (is_float($arg)) {
            if (is_nan($arg) || is_infinite($arg)) {
                throw new SkaidbException('cannot bind NaN/Infinity');
            }
            // Round-trip-safe float formatting. PHP's (string) cast honours the
            // `precision` ini (default 14) and can lose digits; json_encode uses
            // serialize_precision=-1 (shortest round-trippable form) by default.
            $s = json_encode($arg);
            if ($s === false) {
                throw new SkaidbException('cannot bind float value');
            }
            return $s;
        }
        if (is_string($arg)) {
            return "'" . str_replace("'", "''", $arg) . "'";
        }
        if ($arg instanceof DateTimeImmutable || $arg instanceof \DateTimeInterface) {
            // Bind as unix milliseconds.
            $ms = (int) round((float) $arg->format('U.u') * 1000);
            return (string) $ms;
        }
        if (is_object($arg) && method_exists($arg, '__toString')) {
            return "'" . str_replace("'", "''", (string) $arg) . "'";
        }
        $type = is_object($arg) ? get_class($arg) : gettype($arg);
        throw new SkaidbException("cannot bind value of type {$type}");
    }

    /** Return $sql with single-quoted literals blanked, for placeholder counting. */
    private static function stripStrings(string $sql): string
    {
        $out = '';
        $inStr = false;
        $i = 0;
        $n = strlen($sql);
        while ($i < $n) {
            $ch = $sql[$i];
            if ($inStr) {
                if ($ch === "'") {
                    if ($i + 1 < $n && $sql[$i + 1] === "'") {
                        $i += 2;
                        continue;
                    }
                    $inStr = false;
                }
                $i++;
                continue;
            }
            if ($ch === "'") {
                $inStr = true;
                $i++;
                continue;
            }
            $out .= $ch;
            $i++;
        }
        return $out;
    }
}

/**
 * A prepared/executed statement, modelled on PDOStatement. Build it via
 * Connection::prepare(); call execute([...]) then fetch results.
 */
class Statement
{
    private Connection $conn;

    private string $sql;

    private int $consistency;

    /** @var array<int,string> */
    private array $columns = [];

    /** @var array<int,array<int,mixed>> */
    private array $rows = [];

    private int $affected = 0;

    private int $pos = 0;

    private bool $isRows = false;

    public function __construct(Connection $conn, string $sql)
    {
        $this->conn = $conn;
        $this->sql = $sql;
        // Inherit the connection's default consistency; override per-statement.
        $this->consistency = $conn->getConsistency();
    }

    /** Override consistency for this statement ('ONE'/'QUORUM'/'ALL' or 0/1/2). */
    public function setConsistency($consistency): self
    {
        $this->consistency = Connection::resolveConsistency($consistency);
        return $this;
    }

    /**
     * Bind $params into the SQL and run it. Returns true on success (mirrors
     * PDOStatement::execute, which returns bool). Throws on error.
     *
     * @param array<int,mixed> $params positional values for '?' placeholders
     */
    public function execute(array $params = []): bool
    {
        $bound = Connection::bindParams($this->sql, $params);
        $res = $this->conn->runQuery($bound, $this->consistency);
        $this->pos = 0;
        if ($res['kind'] === 'rows') {
            $this->isRows = true;
            $this->columns = $res['columns'];
            $this->rows = $res['rows'];
            $this->affected = 0;
        } else {
            $this->isRows = false;
            $this->columns = [];
            $this->rows = [];
            $this->affected = $res['affected'];
        }
        return true;
    }

    /**
     * Fetch the next row as an associative array (column name => value), or
     * null when exhausted.
     *
     * @return array<string,mixed>|null
     */
    public function fetch(): ?array
    {
        if ($this->pos >= count($this->rows)) {
            return null;
        }
        $row = $this->rows[$this->pos];
        $this->pos++;
        return $this->assoc($row);
    }

    /**
     * Fetch all remaining rows as associative arrays.
     *
     * @return array<int,array<string,mixed>>
     */
    public function fetchAll(): array
    {
        $out = [];
        $count = count($this->rows);
        for (; $this->pos < $count; $this->pos++) {
            $out[] = $this->assoc($this->rows[$this->pos]);
        }
        return $out;
    }

    /**
     * Fetch a single column from the next row (default the first column),
     * or false when exhausted (mirrors PDOStatement::fetchColumn).
     *
     * @return mixed
     */
    public function fetchColumn(int $column = 0)
    {
        if ($this->pos >= count($this->rows)) {
            return false;
        }
        $row = $this->rows[$this->pos];
        $this->pos++;
        return $row[$column] ?? null;
    }

    /**
     * For mutations: the number of affected rows. For SELECTs: the number of
     * rows in the result set (note: PDO leaves SELECT rowCount() driver-defined;
     * we return the row count for convenience).
     */
    public function rowCount(): int
    {
        return $this->isRows ? count($this->rows) : $this->affected;
    }

    /** Number of columns in the result set. */
    public function columnCount(): int
    {
        return count($this->columns);
    }

    /** @return array<int,string> the column names */
    public function columns(): array
    {
        return $this->columns;
    }

    /**
     * Zip a positional row with column names into an associative array.
     *
     * @param array<int,mixed> $row
     *
     * @return array<string,mixed>
     */
    private function assoc(array $row): array
    {
        $out = [];
        foreach ($this->columns as $i => $name) {
            $out[$name] = $row[$i] ?? null;
        }
        return $out;
    }
}

/**
 * A cursor over a byte string, decoding little-endian protocol fields.
 *
 * @internal
 */
class Reader
{
    private string $buf;

    private int $pos = 0;

    private int $len;

    public function __construct(string $buf)
    {
        $this->buf = $buf;
        $this->len = strlen($buf);
    }

    public function take(int $n): string
    {
        $end = $this->pos + $n;
        if ($end > $this->len) {
            throw new SkaidbException('truncated server message');
        }
        $s = substr($this->buf, $this->pos, $n);
        $this->pos = $end;
        return $s;
    }

    public function u8(): int
    {
        return ord($this->take(1));
    }

    public function u32(): int
    {
        return unpack('V', $this->take(4))[1]; // u32 LE
    }

    /** Read an i64 LE, returning a PHP int (64-bit on 64-bit builds). */
    public function i64(): int
    {
        $bytes = $this->take(8);
        // 'P' = u64 LE. On 64-bit PHP this yields a signed int already when the
        // top bit is set (PHP ints are signed 64-bit), giving correct two's
        // complement values. unpack('q') would depend on machine endianness, so
        // we use 'P' explicitly to guarantee little-endian interpretation.
        return unpack('P', $bytes)[1];
    }

    /** Read a u64 LE. Returned as PHP int; values > PHP_INT_MAX wrap negative. */
    public function u64(): int
    {
        return unpack('P', $this->take(8))[1];
    }

    /** Read an f64 LE (IEEE-754). */
    public function f64(): float
    {
        // 'e' = double, little-endian (PHP 7.0.15+/7.1+). Guaranteed LE.
        return unpack('e', $this->take(8))[1];
    }

    /** Read a length-prefixed blob (u32 LE len + bytes). */
    public function blob(): string
    {
        return $this->take($this->u32());
    }

    /** Read a length-prefixed UTF-8 string. */
    public function text(): string
    {
        return $this->blob();
    }
}
