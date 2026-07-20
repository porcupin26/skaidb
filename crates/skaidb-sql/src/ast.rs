//! Abstract syntax tree for the skaidb SQL subset (SPEC §3).

use skaidb_types::Value;

/// A top-level SQL statement.
// `Select` (many optional clauses) is inherently larger than the DDL
// variants; boxing it would touch every match site in the engine for no
// runtime benefit (statements aren't stored in bulk — one per parse).
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    CreateTable(CreateTable),
    /// `CREATE TIMESERIES TABLE` — a table whose rows are samples, stored in
    /// the time-series engine. Dropped with plain `DROP TABLE`.
    CreateTimeseriesTable(CreateTimeseriesTable),
    /// `CREATE ROLLUP` — a downsampled companion of a time-series table,
    /// maintained automatically. Dropped with plain `DROP TABLE`.
    CreateRollup(CreateRollup),
    DropTable { name: String, if_exists: bool },
    CreateIndex(CreateIndex),
    DropIndex { name: String, if_exists: bool },
    CreateVectorIndex(CreateVectorIndex),
    DropVectorIndex { name: String, if_exists: bool },
    /// `CREATE GEO INDEX` — a Morton/Z-order index over a `{lat, lon}` point
    /// column that transparently accelerates `geo_distance` / `geo_bbox`.
    CreateGeoIndex(CreateGeoIndex),
    /// `DROP GEO INDEX [IF EXISTS] name`.
    DropGeoIndex { name: String, if_exists: bool },
    /// `CREATE SEARCH INDEX` — a full-text index over one or more text
    /// columns, queried with `MATCH()`/`SEARCH()` predicates.
    CreateSearchIndex(CreateSearchIndex),
    /// `DROP SEARCH INDEX [IF EXISTS] name`.
    DropSearchIndex { name: String, if_exists: bool },
    /// `REBUILD SEARCH INDEX name` — discard the index data and re-index
    /// every row of the table (recovery / anti-entropy escape hatch).
    RebuildSearchIndex { name: String },
    /// `ALTER SEARCH INDEX <name> SET (<option> = <literal>, ...)` —
    /// change **query-time** index options (synonyms, search_analyzer,
    /// boost, refresh_ms) in place, no reindex.
    AlterSearchIndex {
        name: String,
        /// Raw options, validated by the engine like the CREATE options.
        options: Vec<(String, String)>,
    },
    /// `ALTER VECTOR INDEX <name> SET (<option> = <literal>, ...)` —
    /// search-time tuning (`ef`); index-time parameters need a rebuild.
    AlterVectorIndex {
        name: String,
        options: Vec<(String, String)>,
    },
    /// `SUGGEST '<text>' ON <index> [COLUMN <col>] [LIMIT n]` — term
    /// suggestions ("did you mean") from the index's term dictionary.
    Suggest {
        text: String,
        index: String,
        column: Option<String>,
        limit: u64,
    },
    /// `EXPLAIN SCORE SELECT ... WHERE <search> FOR <pk literal>` — the
    /// BM25 breakdown of how one row (by primary-key value) scored
    /// against the SELECT's search predicates. One `explanation` row of
    /// JSON when the row matches; zero rows when it does not.
    ExplainScore {
        select: Box<Select>,
        key: skaidb_types::Value,
    },
    /// `EXPLAIN <statement>` — the plan the executor would choose (access
    /// path, pushdown/fallback decisions, cluster fan-out), as
    /// `(aspect, decision)` rows. Advisory: it mirrors the planner's
    /// decision logic without executing.
    Explain { statement: Box<Statement> },
    /// `SHOW CLUSTER` — ring/membership detail (needs ADMIN; served by the
    /// server layer, not the engine).
    ShowCluster,
    /// `SHOW CONFIG [LIKE '<pattern>']` — flattened `(key, value)` config
    /// rows, secrets masked (`%`/`_` wildcards; needs ADMIN).
    ShowConfig { like: Option<String> },
    /// `SET CONFIG <section.field> = '<value>'` — live-mutable keys apply
    /// instantly; everything persists to the config file (needs ADMIN).
    SetConfig { key: String, value: String },
    /// `SHOW SLOW QUERIES [LIMIT n]` — the in-memory slow-query sample
    /// (masked SQL; needs ADMIN).
    ShowSlowQueries { limit: Option<u64> },
    /// `SET CONSISTENCY { ONE | QUORUM | ALL }` — per-connection override
    /// on binary-protocol sessions.
    SetConsistency { level: String },
    /// `REPAIR CLUSTER` — one anti-entropy pass, cluster-wide (ADMIN).
    RepairCluster,
    /// `RECLAIM` — drop data this cluster's nodes no longer own (ADMIN).
    Reclaim,
    /// `ALTER CLUSTER ADD NODE '<addr>'` / `ALTER CLUSTER REMOVE NODE
    /// '<id>'` (ADMIN).
    AlterCluster { add: bool, node: String },
    /// `ALTER CLUSTER SET NAME '<name>'` — rename the cluster (Admin;
    /// refused on witness nodes, whose identity mirrors the primary).
    AlterClusterName { name: String },
    /// `ALTER NODE '<alias|dotted|id>' SET NAME '<name>'` — rename a
    /// member or witness (Admin; refused on witness nodes).
    AlterNodeName { node: String, name: String },
    /// `BACKUP TO '<path>'` — a crash-consistent copy of this node's data
    /// directory (tables + WALs + catalog + search/time-series stores)
    /// taken under the exclusive lock (ADMIN).
    Backup { path: String },
    /// `RESTORE FROM '<path>'` — replace this instance's data with a
    /// backup and reopen. Embedded / single-node only (ADMIN).
    Restore { path: String },
    AlterTable(AlterTable),
    Insert(Insert),
    Select(Select),
    Update(Update),
    Delete(Delete),
    /// `BEGIN [TRANSACTION]` — start a transaction (embedded engine only).
    Begin,
    /// `COMMIT [TRANSACTION]` — durably apply the open transaction.
    Commit,
    /// `ROLLBACK [TRANSACTION]` — discard the open transaction.
    Rollback,
    /// `SHOW TABLES` — list the tables in the catalog (read-only introspection).
    ShowTables,
    /// `SHOW INDEXES` — list secondary and vector indexes in the catalog.
    ShowIndexes,
    /// `SHOW STATUS` — storage/runtime statistics for the current database.
    ShowStatus,
    /// `DESCRIBE <table> [FULL [SAMPLE n | EXACT]]` (or `DESC …`) — the
    /// table's structure. Without `FULL` it is catalog-only: one row per column
    /// that is part of the primary key or an index, answered from the catalog
    /// with no data access. `FULL` additionally samples rows to surface
    /// **every** field (schema-less, so otherwise-invisible non-key fields)
    /// with its inferred type; `SAMPLE n` caps the rows scanned (default
    /// otherwise), while `EXACT` scans all rows and caches the result in RAM,
    /// revalidated against the table's write stamp on each call.
    Describe {
        table: String,
        /// `FULL` — sample rows to include non-key/non-indexed fields + types.
        full: bool,
        /// `SAMPLE n` (only with `FULL`) — max rows to scan; `None` = default.
        sample: Option<usize>,
        /// `EXACT` (only with `FULL`) — scan all rows; RAM-cached by write stamp.
        exact: bool,
    },
    /// `SHOW DATABASES` — list the databases in this data directory.
    ///
    /// The four database statements below operate across databases, so a single
    /// `Database` cannot execute them; they are handled by the multi-database
    /// session layer in `skaidb-engine`.
    ShowDatabases,
    /// `CREATE DATABASE [IF NOT EXISTS] <name>`.
    CreateDatabase { name: String, if_not_exists: bool },
    /// `DROP DATABASE [IF EXISTS] <name>`.
    DropDatabase { name: String, if_exists: bool },
    /// `USE [DATABASE] <name>` — switch the session's current database.
    UseDatabase { name: String },
    /// `CREATE USER [IF NOT EXISTS] u PASSWORD '...'` (or the internal
    /// `VERIFIER '...'` replication form). A user acts as its own-named
    /// role; grant other roles to it for more.
    CreateUser(CreateUser),
    /// `ALTER USER u PASSWORD '...'`.
    AlterUser { name: String, password: String },
    /// `DROP USER [IF EXISTS] u`.
    DropUser { name: String, if_exists: bool },
    /// `CREATE ROLE [IF NOT EXISTS] r` (internal `GRANTS '...'` form carries
    /// whole-role state for replication).
    CreateRole {
        name: String,
        if_not_exists: bool,
        state: Option<String>,
    },
    /// `DROP ROLE [IF EXISTS] r`.
    DropRole { name: String, if_exists: bool },
    /// `GRANT <privilege> ON <table | DATABASE db | *> TO <role>`.
    Grant {
        privilege: String,
        object: GrantObject,
        to: String,
    },
    /// `REVOKE <privilege> ON <table | DATABASE db | *> FROM <role>`.
    Revoke {
        privilege: String,
        object: GrantObject,
        from: String,
    },
    /// `GRANT ROLE r TO u` — role inheritance.
    GrantRole { role: String, to: String },
    /// `REVOKE ROLE r FROM u`.
    RevokeRole { role: String, from: String },
    /// `SHOW GRANTS [FOR <role>]`.
    ShowGrants { role: Option<String> },
}

/// What a `GRANT`/`REVOKE` applies to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantObject {
    /// `ON *` — the whole cluster.
    Global,
    /// `ON <table>`.
    Table(String),
    /// `ON DATABASE <db>` — every table in that database.
    Database(String),
}

/// `CREATE USER` payload.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateUser {
    pub name: String,
    pub if_not_exists: bool,
    /// Plaintext (client-facing form) — the engine derives and stores a
    /// SCRAM verifier; the coordinator rewrites to `verifier` form before
    /// broadcasting, so plaintext never crosses internode links.
    pub password: Option<String>,
    /// Encoded verifier (internal replication/replay form).
    pub verifier: Option<String>,
    /// External (passwordless) Kerberos/GSSAPI user: the name is the principal
    /// and there is no local secret. Mutually exclusive with password/verifier
    /// (the parser enforces exactly one form).
    pub gssapi: bool,
}

impl Statement {
    /// Visit every **table reference** in the statement (each possibly written
    /// as `db.table`) so a caller can rewrite it to an internal, database-resolved
    /// name. Centralizes the set of table positions for namespace resolution.
    pub fn for_each_table_mut(&mut self, mut f: impl FnMut(&mut String)) {
        match self {
            Statement::CreateTable(c) => f(&mut c.name),
            Statement::CreateTimeseriesTable(c) => f(&mut c.name),
            Statement::CreateRollup(c) => {
                f(&mut c.name);
                f(&mut c.table);
            }
            Statement::DropTable { name, .. } => f(name),
            Statement::CreateIndex(c) => f(&mut c.table),
            Statement::CreateVectorIndex(c) => f(&mut c.table),
            Statement::CreateGeoIndex(c) => f(&mut c.table),
            Statement::CreateSearchIndex(c) => f(&mut c.table),
            Statement::AlterTable(a) => {
                f(&mut a.name);
                if let AlterAction::RenameTable { new_name } = &mut a.action {
                    f(new_name);
                }
            }
            Statement::Insert(i) => f(&mut i.table),
            Statement::Update(u) => f(&mut u.table),
            Statement::Delete(d) => f(&mut d.table),
            Statement::Select(s) => s.for_each_table_mut(&mut f),
            Statement::Describe { table, .. } => f(table),
            Statement::Explain { statement } => statement.for_each_table_mut(f),
            _ => {}
        }
    }

    /// Visit every **local object name** — one that lives inside the current
    /// database and is never `db.`-qualified by the user: secondary and vector
    /// index names.
    pub fn for_each_local_name_mut(&mut self, mut f: impl FnMut(&mut String)) {
        match self {
            Statement::CreateIndex(c) => f(&mut c.name),
            Statement::DropIndex { name, .. } => f(name),
            Statement::CreateVectorIndex(c) => f(&mut c.name),
            Statement::DropVectorIndex { name, .. } => f(name),
            Statement::CreateGeoIndex(c) => f(&mut c.name),
            Statement::DropGeoIndex { name, .. } => f(name),
            Statement::CreateSearchIndex(c) => f(&mut c.name),
            Statement::DropSearchIndex { name, .. } => f(name),
            Statement::RebuildSearchIndex { name } => f(name),
            Statement::AlterSearchIndex { name, .. } => f(name),
            Statement::Suggest { index, .. } => f(index),
            Statement::ExplainScore { select, .. } => f(&mut select.from),
            Statement::Explain { statement } => statement.for_each_local_name_mut(f),
            _ => {}
        }
    }
}

impl Select {
    /// Visit the `FROM` table, every joined table, and the tables of any
    /// trailing `UNION` branches.
    pub fn for_each_table_mut(&mut self, f: &mut impl FnMut(&mut String)) {
        // An empty `from` is the FROM-less constant-select sentinel
        // (`SELECT 1`), not a table reference: database resolution must not
        // qualify it into `db.<nothing>` (which broke `SELECT 1` for every
        // session outside the default database).
        if !self.from.is_empty() {
            f(&mut self.from);
        }
        for join in &mut self.joins {
            f(&mut join.table);
        }
        for set_op in &mut self.set_ops {
            set_op.select.for_each_table_mut(f);
        }
    }
}

/// `ALTER TABLE name <action>`.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTable {
    pub name: String,
    pub action: AlterAction,
}

/// What an `ALTER TABLE` does. The store is schema-less, so only structural
/// renames are meaningful (there is no column list to add/drop columns from).
#[derive(Debug, Clone, PartialEq)]
pub enum AlterAction {
    /// `RENAME TO <new_name>`.
    RenameTable { new_name: String },
    /// `RENAME COLUMN <from> TO <to>` — rewrites the field in every row.
    RenameColumn { from: String, to: String },
    /// `SET (<option> = <value>, ...)` — placement/witness options. The
    /// engine validates which options are mutable (witness: freely;
    /// nodes: shrink-only until the placement-transition work lands;
    /// replication: rejected until then).
    SetOptions { options: Vec<(String, String)> },
}

/// `CREATE TABLE [IF NOT EXISTS] name (PRIMARY KEY (cols...))`.
///
/// Schema-less: only the primary key is declared, never a column list.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTable {
    pub name: String,
    pub if_not_exists: bool,
    pub primary_key: Vec<String>,
    /// `WITH (ttl = <duration>)` — rows expire (become invisible, then
    /// reclaimed) after this age. `None` = no expiry.
    pub ttl_ms: Option<i64>,
    /// `WITH (memory = true)`: RAM-resident, never flushed, empty on restart.
    pub memory: bool,
    /// `WITH (replication = N)`: this table's ring-placed copy count
    /// (None = cluster default). Mutually exclusive with `nodes`.
    pub replication: Option<u32>,
    /// `WITH (nodes = ['alias'|'id', ...])`: pinned placement — replicas
    /// live on exactly these members (each a full copy). Aliases resolve
    /// to stable ids at execution. Mutually exclusive with `replication`.
    pub nodes: Vec<String>,
    /// `WITH (witness = false)`: excluded from witness mirrors (default
    /// mirrored).
    pub witness: bool,
}

/// `CREATE TIMESERIES TABLE [IF NOT EXISTS] name (SERIES KEY (l1 [, ...])
/// [, RETENTION <duration>])`. Series-key columns are string labels; the
/// implicit sample key is `(series key, ts)`.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTimeseriesTable {
    pub name: String,
    pub if_not_exists: bool,
    pub series_key: Vec<String>,
    /// Milliseconds; `None` keeps data forever.
    pub retention_ms: Option<i64>,
    /// Out-of-order acceptance window in ms; `None`/0 = strict monotonic.
    pub ooo_ms: Option<i64>,
}

/// `CREATE ROLLUP [IF NOT EXISTS] name ON table BUCKET <duration>
/// [RETENTION <duration>]`: a derived time-series table holding per-bucket
/// partials (`<field>_count/_sum/_min/_max/_first/_last`).
#[derive(Debug, Clone, PartialEq)]
pub struct CreateRollup {
    pub name: String,
    pub if_not_exists: bool,
    pub table: String,
    pub bucket_ms: i64,
    pub retention_ms: Option<i64>,
}

/// `CREATE INDEX [IF NOT EXISTS] name ON table (path1 [, path2, ...])`.
/// Multiple paths form a composite index, ordered left-to-right.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndex {
    pub name: String,
    pub if_not_exists: bool,
    pub table: String,
    pub paths: Vec<String>,
    /// `WITH (global = true)`: entries are placed on the ring by indexed
    /// value (an internal replicated table) instead of indexing each node's
    /// local shard. See docs/GLOBAL_INDEXES.md.
    pub global: bool,
    /// Internal (schema-replay only): `WITH (global = true, ready = true)`
    /// marks a global index whose cluster-wide backfill already completed,
    /// so a node importing the definition (rejoin catch-up, fresh
    /// bootstrap) starts routing probes instead of waiting for a readiness
    /// broadcast it can no longer receive.
    pub ready: bool,
}

/// `CREATE VECTOR INDEX [IF NOT EXISTS] name ON table (path) DIM n [USING metric]`.
/// An HNSW index for approximate nearest-neighbor search over the float array
/// at `path`.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateVectorIndex {
    pub name: String,
    pub if_not_exists: bool,
    pub table: String,
    pub path: String,
    pub dim: usize,
    /// `cosine` (default), `l2`, or `dot`.
    pub metric: String,
    /// `EMBED` — a **managed** index: `path` names a TEXT column, and skaidb
    /// embeds it via the configured inference provider (rather than reading a
    /// pre-computed vector array from the row). A string `NEAREST` query on such
    /// an index is auto-embedded too.
    pub embed: bool,
    /// `QUANTIZED` — the in-RAM graph stores int8 scalar-quantized vectors
    /// (4× less RAM); searches over-fetch and rescore the top-k against the
    /// exact vectors re-read from the rows. Incompatible with `EMBED`.
    pub quantized: bool,
}

/// `CREATE GEO INDEX [IF NOT EXISTS] name ON table (path)`. A Morton/Z-order
/// spatial index over the `{lat, lon}` point at `path`.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateGeoIndex {
    pub name: String,
    pub if_not_exists: bool,
    pub table: String,
    pub path: String,
}

/// `CREATE SEARCH INDEX [IF NOT EXISTS] name ON table (path1 [, path2, ...])
/// [WITH (option = value, ...)]`. A full-text (BM25) index over the text at
/// the given document paths. Options: `analyzer` (string, default
/// `'standard'`), `refresh_ms` (integer, default 1000).
#[derive(Debug, Clone, PartialEq)]
pub struct CreateSearchIndex {
    pub name: String,
    pub if_not_exists: bool,
    pub table: String,
    pub paths: Vec<String>,
    /// Raw `WITH (...)` options in declaration order; values are kept as
    /// written (strings unquoted, numbers/bools as their literal text) and
    /// validated by the engine.
    pub options: Vec<(String, String)>,
}

/// `INSERT INTO table (cols...) VALUES (..), (..)`.
#[derive(Debug, Clone, PartialEq)]
pub struct Insert {
    pub table: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Expr>>,
}

/// `SELECT [DISTINCT] items FROM table [JOIN ...] [WHERE expr] [GROUP BY ..]
/// [HAVING expr] [UNION [ALL] select] [ORDER BY ..] [LIMIT n] [OFFSET m]`.
///
/// `joins`, `having`, and `set_ops` are empty/`None` for the simple single-table
/// query that the rest of the engine has always handled. When `set_ops` is
/// non-empty, `order_by`/`limit`/`offset`/`distinct` here apply to the **whole**
/// combined result (standard SQL set-query semantics); the chained selects carry
/// only their own projection/source/filter/grouping.
#[derive(Debug, Clone, PartialEq)]
pub struct Select {
    pub distinct: bool,
    /// `NEAREST (<path>, <query>, <k>)` — approximate nearest-neighbor clause:
    /// return the `k` rows whose vector at `path` is closest to `query`
    /// (which must evaluate to a numeric array), ordered nearest-first, with
    /// the distance exposed as a `_distance` field. Requires a vector index
    /// on `(table, path)`. Mutually exclusive with joins, grouping, set ops,
    /// and `ORDER BY`.
    pub nearest: Option<Box<Nearest>>,
    /// `RANK BY RRF [(<k>)]` — hybrid retrieval: fuse the vector leg (the
    /// `NEAREST` clause) and the text leg (the search predicate in `WHERE`) by
    /// Reciprocal Rank Fusion. `rrf_score()` exposes the fused score; the
    /// residual (non-search) part of `WHERE` filters both legs. Requires both a
    /// `NEAREST` clause and a search predicate in `WHERE`.
    pub rrf: Option<Rrf>,
    /// `RERANK [ON <col>] [WITH '<model>'] [QUERY '<text>'] [TOP <n>]` —
    /// second-stage reranking: the top `n` candidates of the search (or
    /// hybrid) retrieval are re-scored by an external cross-encoder reranker
    /// and returned in the reranker's order. Requires a search predicate in
    /// `WHERE`; incompatible with `ORDER BY` and grouping.
    pub rerank: Option<Rerank>,
    pub items: Vec<SelectItem>,
    pub from: String,
    /// Alias for the `FROM` table (defaults to the table name).
    pub from_alias: String,
    pub joins: Vec<Join>,
    pub filter: Option<Expr>,
    pub group_by: Vec<Expr>,
    /// `GROUP BY ... TOP k BY <expr> [ASC|DESC]` — per-group top-k rows:
    /// instead of one aggregated row per group, each group contributes its
    /// `k` best rows ranked by the expression (`DESC` — best-first — by
    /// default). Works with `score()` under a search predicate.
    pub group_top: Option<GroupTopK>,
    pub having: Option<Expr>,
    pub set_ops: Vec<SetOp>,
    pub order_by: Vec<OrderKey>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    /// `AFTER (<sort-value>, <pk-value>)` — deep-pagination keyset cursor
    /// (ES `search_after`): resume the page sequence strictly after the row
    /// identified by the previous page's last sort value plus its primary-key
    /// value (the tie-break, always ascending). Requires a search query with
    /// `ORDER BY score() DESC` or `ORDER BY <col>` plus `LIMIT`; mutually
    /// exclusive with `OFFSET`.
    pub after: Option<Vec<Expr>>,
    /// `LIMIT ?` — the bind-parameter position when the limit is a
    /// placeholder. `bind` substitutes the bound value into `limit` and
    /// clears this; one surviving to execution is an unbound-parameter error.
    pub limit_param: Option<u16>,
    /// `OFFSET ?`, same contract as [`Select::limit_param`].
    pub offset_param: Option<u16>,
}

/// The `TOP k BY <expr> [ASC|DESC]` clause of a `GROUP BY`.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupTopK {
    /// Rows kept per group.
    pub k: u64,
    /// Ranking expression, evaluated per row (aggregates not allowed).
    pub by: Expr,
    /// `true` for `ASC` (smallest first); default `DESC`.
    pub ascending: bool,
}

/// The `RANK BY RRF [(<k>)]` clause of a [`Select`] — Reciprocal Rank Fusion
/// of the `NEAREST` (vector) and `WHERE`-search (text) legs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rrf {
    /// The RRF rank constant `k` (default 60): a hit at 1-based rank `r` in a
    /// leg contributes `1 / (k + r)` to its fused score.
    pub constant: u32,
}

/// The default RRF rank constant (the value TREC/Elasticsearch use).
pub const DEFAULT_RRF_CONSTANT: u32 = 60;

/// The `RERANK [ON <col>] [WITH '<model>'] [QUERY '<text>'] [TOP <n>]` clause
/// of a [`Select`] — second-stage reranking of a search/hybrid retrieval by an
/// external cross-encoder model (docs/SEARCH.md "Reranking").
#[derive(Debug, Clone, PartialEq)]
pub struct Rerank {
    /// `ON <col>` — the column whose text is sent as each candidate's
    /// document. Default: the columns the search predicate targets (all
    /// string fields for a field-less `SEARCH()`).
    pub column: Option<String>,
    /// `WITH '<model>'` — the model name sent to the rerank endpoint.
    /// Default: the configured `inference.rerank_model`.
    pub model: Option<String>,
    /// `QUERY '<text>'` — the query text the reranker scores documents
    /// against. Default: the text of the search predicate.
    pub query: Option<String>,
    /// `TOP <n>` — how many top-ranked candidates are sent to the reranker
    /// (the rerank window; ES `rank_window_size`). Default
    /// [`DEFAULT_RERANK_TOP`], capped by the engine.
    pub top: u64,
}

/// The default `RERANK … TOP` candidate window.
pub const DEFAULT_RERANK_TOP: u64 = 100;

/// The `NEAREST (<path>, <query>, <k>)` clause of a [`Select`].
#[derive(Debug, Clone, PartialEq)]
pub struct Nearest {
    /// Document path of the indexed vector field (e.g. `embedding`).
    pub path: String,
    /// The query vector: an array literal or a bind parameter.
    pub query: Expr,
    /// How many neighbors to return: an integer literal or a bind parameter.
    pub k: Expr,
}

/// A joined table: `[<kind>] JOIN <table> [AS alias] [ON <expr>]`.
#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    pub kind: JoinKind,
    pub table: String,
    pub alias: String,
    /// The `ON` predicate; `None` for a `CROSS JOIN` (full cartesian product).
    pub on: Option<Expr>,
}

/// The join flavor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Cross,
}

/// A trailing `UNION [ALL] <select>` combined into the query.
#[derive(Debug, Clone, PartialEq)]
pub struct SetOp {
    /// `true` for `UNION ALL` (keep duplicates); `false` for `UNION` (dedup).
    pub all: bool,
    pub select: Select,
}

/// A projected output column.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    /// `*` — all fields present in each row.
    Wildcard,
    /// An expression with an optional `AS` alias.
    Expr { expr: Expr, alias: Option<String> },
}

/// An `ORDER BY` sort key.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderKey {
    pub expr: Expr,
    pub descending: bool,
}

/// `UPDATE table SET col = expr, .. [WHERE expr]`.
#[derive(Debug, Clone, PartialEq)]
pub struct Update {
    pub table: String,
    pub assignments: Vec<(String, Expr)>,
    pub filter: Option<Expr>,
}

/// `DELETE FROM table [WHERE expr]`.
#[derive(Debug, Clone, PartialEq)]
pub struct Delete {
    pub table: String,
    pub filter: Option<Expr>,
}

/// A scalar expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A literal value.
    Literal(Value),
    /// A positional bind parameter (`?`), 0-indexed in order of appearance.
    /// Present only in prepared statements; `bind` replaces every parameter
    /// with a literal before execution.
    Parameter(u16),
    /// A column / field path like `a.b.c`.
    Column(String),
    /// Unary operator application.
    Unary { op: UnaryOp, expr: Box<Expr> },
    /// Binary operator application.
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// `expr IS [NOT] NULL`.
    IsNull { expr: Box<Expr>, negated: bool },
    /// `expr [NOT] IN (list)` — set membership. An element that evaluates to
    /// an array is flattened (each of its elements becomes a candidate), so a
    /// bound array parameter — `col IN (?)` with `?` = `['a','b']` — tests
    /// membership in that array. `negated` is `NOT IN`.
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// `expr [NOT] BETWEEN lo AND hi` — inclusive range, sugar for
    /// `expr >= lo AND expr <= hi` under three-valued logic.
    Between {
        expr: Box<Expr>,
        lo: Box<Expr>,
        hi: Box<Expr>,
        negated: bool,
    },
    /// `expr [NOT] LIKE/ILIKE pattern` — SQL pattern match (`%` = any run,
    /// `_` = any one char; no escape sequence). `case_insensitive` is `ILIKE`.
    /// Non-string operands compare as unknown (`NULL`), matching how
    /// incomparable types behave in ordinary comparisons.
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        case_insensitive: bool,
        negated: bool,
    },
    /// An aggregate function call (`COUNT(*)`, `SUM(x)`, ...).
    Aggregate { func: AggFunc, arg: AggArg },
    /// A scalar function call (`time_bucket(5m, ts)`, `now()`). Names are
    /// resolved at evaluation; unknown functions are execution errors.
    Func { name: String, args: Vec<Expr> },
}

/// Argument to an aggregate.
#[derive(Debug, Clone, PartialEq)]
pub enum AggArg {
    /// `COUNT(*)`.
    Star,
    /// An expression argument.
    Expr(Box<Expr>),
    /// `COUNT(DISTINCT expr)` — distinct non-null values of the expression.
    Distinct(Box<Expr>),
    /// `APPROX_COUNT_DISTINCT(expr)` — the caller opts into an approximate
    /// distinct count: the search-index pushdown may answer with an
    /// HLL-style sketch instead of bailing on wide term sets; paths without
    /// a sketch answer exactly (an exact answer is a valid approximation).
    ApproxDistinct(Box<Expr>),
}

/// Aggregate functions (SPEC §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    /// Per-second counter rate over the group, reset-aware, computed per
    /// series then summed (time-series tables only).
    Rate,
    /// Counter-reset-aware total increase (time-series tables only).
    Increase,
    /// `last - first` per series, summed (time-series tables only).
    Delta,
    /// Value at the earliest timestamp in the group (time-series tables only).
    First,
    /// Value at the latest timestamp in the group (time-series tables only).
    Last,
}

/// Unary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
}

/// Binary operators, comparison and arithmetic and logical.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    Add,
    Sub,
    Mul,
    Div,
}
