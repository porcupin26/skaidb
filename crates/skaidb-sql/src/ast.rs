//! Abstract syntax tree for the skaidb SQL subset (SPEC §3).

use skaidb_types::Value;

/// A top-level SQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    CreateTable(CreateTable),
    DropTable { name: String, if_exists: bool },
    CreateIndex(CreateIndex),
    DropIndex { name: String, if_exists: bool },
    CreateVectorIndex(CreateVectorIndex),
    DropVectorIndex { name: String, if_exists: bool },
    Insert(Insert),
    Select(Select),
    Update(Update),
    Delete(Delete),
}

/// `CREATE TABLE [IF NOT EXISTS] name (PRIMARY KEY (cols...))`.
///
/// Schema-less: only the primary key is declared, never a column list.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTable {
    pub name: String,
    pub if_not_exists: bool,
    pub primary_key: Vec<String>,
}

/// `CREATE INDEX [IF NOT EXISTS] name ON table (path1 [, path2, ...])`.
/// Multiple paths form a composite index, ordered left-to-right.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndex {
    pub name: String,
    pub if_not_exists: bool,
    pub table: String,
    pub paths: Vec<String>,
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
}

/// `INSERT INTO table (cols...) VALUES (..), (..)`.
#[derive(Debug, Clone, PartialEq)]
pub struct Insert {
    pub table: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Expr>>,
}

/// `SELECT items FROM table [WHERE expr] [ORDER BY ..] [LIMIT n] [OFFSET m]`.
#[derive(Debug, Clone, PartialEq)]
pub struct Select {
    pub items: Vec<SelectItem>,
    pub from: String,
    pub filter: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub order_by: Vec<OrderKey>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
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
    /// An aggregate function call (`COUNT(*)`, `SUM(x)`, ...).
    Aggregate { func: AggFunc, arg: AggArg },
}

/// Argument to an aggregate.
#[derive(Debug, Clone, PartialEq)]
pub enum AggArg {
    /// `COUNT(*)`.
    Star,
    /// An expression argument.
    Expr(Box<Expr>),
}

/// Aggregate functions (SPEC §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
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
