//! Recursive-descent / precedence-climbing parser for the skaidb SQL subset.

use skaidb_types::Value;

use crate::ast::*;
use crate::token::{tokenize, Keyword, LexError, Token};

/// Errors raised while parsing.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ParseError {
    #[error("lex error: {0}")]
    Lex(#[from] LexError),
    #[error("unexpected token {found:?}, expected {expected}")]
    Unexpected { found: String, expected: String },
    #[error("unexpected end of input, expected {0}")]
    UnexpectedEof(String),
    #[error("{0}")]
    Other(String),
}

/// Parse a single SQL statement (a trailing semicolon is permitted).
pub fn parse(sql: &str) -> Result<Statement, ParseError> {
    let tokens = tokenize(sql)?;
    let mut p = Parser { tokens, pos: 0 };
    let stmt = p.parse_statement()?;
    p.eat(&Token::Semicolon);
    p.expect_eof()?;
    Ok(stmt)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> &Token {
        &self.tokens[self.pos]
    }

    fn advance(&mut self) -> Token {
        let t = self.tokens[self.pos].clone();
        if self.pos < self.tokens.len() - 1 {
            self.pos += 1;
        }
        t
    }

    /// Consume `tok` if it is next; report whether it matched.
    fn eat(&mut self, tok: &Token) -> bool {
        if self.peek() == tok {
            self.advance();
            true
        } else {
            false
        }
    }

    fn eat_keyword(&mut self, kw: Keyword) -> bool {
        if self.peek() == &Token::Keyword(kw) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, tok: &Token) -> Result<(), ParseError> {
        if self.eat(tok) {
            Ok(())
        } else {
            Err(self.unexpected(format!("{tok:?}")))
        }
    }

    fn expect_keyword(&mut self, kw: Keyword) -> Result<(), ParseError> {
        if self.eat_keyword(kw) {
            Ok(())
        } else {
            Err(self.unexpected(format!("{kw:?}")))
        }
    }

    fn expect_eof(&mut self) -> Result<(), ParseError> {
        if self.peek() == &Token::Eof {
            Ok(())
        } else {
            Err(self.unexpected("end of input".into()))
        }
    }

    fn unexpected(&self, expected: String) -> ParseError {
        match self.peek() {
            Token::Eof => ParseError::UnexpectedEof(expected),
            other => ParseError::Unexpected {
                found: format!("{other:?}"),
                expected,
            },
        }
    }

    fn expect_ident(&mut self) -> Result<String, ParseError> {
        match self.advance() {
            Token::Ident(s) => Ok(s),
            other => Err(ParseError::Unexpected {
                found: format!("{other:?}"),
                expected: "identifier".into(),
            }),
        }
    }

    fn expect_u64(&mut self) -> Result<u64, ParseError> {
        match self.advance() {
            Token::Int(i) if i >= 0 => Ok(i as u64),
            other => Err(ParseError::Unexpected {
                found: format!("{other:?}"),
                expected: "non-negative integer".into(),
            }),
        }
    }

    // ---- statements ----

    fn parse_statement(&mut self) -> Result<Statement, ParseError> {
        match self.peek() {
            Token::Keyword(Keyword::Select) => self.parse_select().map(Statement::Select),
            Token::Keyword(Keyword::Insert) => self.parse_insert().map(Statement::Insert),
            Token::Keyword(Keyword::Update) => self.parse_update().map(Statement::Update),
            Token::Keyword(Keyword::Delete) => self.parse_delete().map(Statement::Delete),
            Token::Keyword(Keyword::Create) => self.parse_create(),
            Token::Keyword(Keyword::Drop) => self.parse_drop(),
            Token::Keyword(Keyword::Alter) => self.parse_alter(),
            Token::Keyword(Keyword::Begin) => {
                self.advance();
                self.eat_keyword(Keyword::Transaction);
                Ok(Statement::Begin)
            }
            Token::Keyword(Keyword::Commit) => {
                self.advance();
                self.eat_keyword(Keyword::Transaction);
                Ok(Statement::Commit)
            }
            Token::Keyword(Keyword::Rollback) => {
                self.advance();
                self.eat_keyword(Keyword::Transaction);
                Ok(Statement::Rollback)
            }
            Token::Keyword(Keyword::Show) => {
                self.advance();
                if self.eat_keyword(Keyword::Tables) {
                    Ok(Statement::ShowTables)
                } else if self.eat_keyword(Keyword::Indexes) {
                    Ok(Statement::ShowIndexes)
                } else {
                    Err(self.unexpected("TABLES or INDEXES after SHOW".into()))
                }
            }
            _ => Err(self.unexpected("a statement".into())),
        }
    }

    /// `ALTER TABLE <name> RENAME { TO <new> | COLUMN <from> TO <to> }`.
    fn parse_alter(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Alter)?;
        self.expect_keyword(Keyword::Table)?;
        let name = self.expect_ident()?;
        self.expect_keyword(Keyword::Rename)?;
        let action = if self.eat_keyword(Keyword::Column) {
            let from = self.parse_path()?;
            self.expect_keyword(Keyword::To)?;
            let to = self.parse_path()?;
            AlterAction::RenameColumn { from, to }
        } else {
            self.expect_keyword(Keyword::To)?;
            let new_name = self.expect_ident()?;
            AlterAction::RenameTable { new_name }
        };
        Ok(Statement::AlterTable(AlterTable { name, action }))
    }

    fn parse_create(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Create)?;
        if self.eat_keyword(Keyword::Table) {
            let if_not_exists = self.parse_if_not_exists()?;
            let name = self.expect_ident()?;
            self.expect(&Token::LParen)?;
            self.expect_keyword(Keyword::Primary)?;
            self.expect_keyword(Keyword::Key)?;
            self.expect(&Token::LParen)?;
            let primary_key = self.parse_ident_list()?;
            self.expect(&Token::RParen)?;
            self.expect(&Token::RParen)?;
            Ok(Statement::CreateTable(CreateTable {
                name,
                if_not_exists,
                primary_key,
            }))
        } else if self.eat_keyword(Keyword::Vector) {
            self.expect_keyword(Keyword::Index)?;
            let if_not_exists = self.parse_if_not_exists()?;
            let name = self.expect_ident()?;
            self.expect_keyword(Keyword::On)?;
            let table = self.expect_ident()?;
            self.expect(&Token::LParen)?;
            let path = self.parse_path()?;
            self.expect(&Token::RParen)?;
            // `DIM <n>` (required) and `USING <metric>` (optional), either order.
            let mut dim = None;
            let mut metric = None;
            loop {
                if self.eat_keyword(Keyword::Dim) {
                    dim = Some(self.expect_u64()? as usize);
                } else if self.eat_keyword(Keyword::Using) {
                    metric = Some(self.expect_ident()?);
                } else {
                    break;
                }
            }
            let dim = dim
                .ok_or_else(|| ParseError::Other("CREATE VECTOR INDEX requires DIM <n>".into()))?;
            Ok(Statement::CreateVectorIndex(CreateVectorIndex {
                name,
                if_not_exists,
                table,
                path,
                dim,
                metric: metric.unwrap_or_else(|| "cosine".into()),
            }))
        } else if self.eat_keyword(Keyword::Index) {
            let if_not_exists = self.parse_if_not_exists()?;
            let name = self.expect_ident()?;
            self.expect_keyword(Keyword::On)?;
            let table = self.expect_ident()?;
            self.expect(&Token::LParen)?;
            let mut paths = vec![self.parse_path()?];
            while self.eat(&Token::Comma) {
                paths.push(self.parse_path()?);
            }
            self.expect(&Token::RParen)?;
            Ok(Statement::CreateIndex(CreateIndex {
                name,
                if_not_exists,
                table,
                paths,
            }))
        } else {
            Err(self.unexpected("TABLE or INDEX".into()))
        }
    }

    fn parse_drop(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Drop)?;
        if self.eat_keyword(Keyword::Table) {
            let if_exists = self.parse_if_exists()?;
            let name = self.expect_ident()?;
            Ok(Statement::DropTable { name, if_exists })
        } else if self.eat_keyword(Keyword::Vector) {
            self.expect_keyword(Keyword::Index)?;
            let if_exists = self.parse_if_exists()?;
            let name = self.expect_ident()?;
            Ok(Statement::DropVectorIndex { name, if_exists })
        } else if self.eat_keyword(Keyword::Index) {
            let if_exists = self.parse_if_exists()?;
            let name = self.expect_ident()?;
            Ok(Statement::DropIndex { name, if_exists })
        } else {
            Err(self.unexpected("TABLE, INDEX, or VECTOR INDEX".into()))
        }
    }

    fn parse_if_not_exists(&mut self) -> Result<bool, ParseError> {
        if self.eat_keyword(Keyword::If) {
            self.expect_keyword(Keyword::Not)?;
            self.expect_keyword(Keyword::Exists)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn parse_if_exists(&mut self) -> Result<bool, ParseError> {
        if self.eat_keyword(Keyword::If) {
            self.expect_keyword(Keyword::Exists)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn parse_insert(&mut self) -> Result<Insert, ParseError> {
        self.expect_keyword(Keyword::Insert)?;
        self.expect_keyword(Keyword::Into)?;
        let table = self.expect_ident()?;
        self.expect(&Token::LParen)?;
        let columns = self.parse_ident_list()?;
        self.expect(&Token::RParen)?;
        self.expect_keyword(Keyword::Values)?;

        let mut rows = Vec::new();
        loop {
            self.expect(&Token::LParen)?;
            let mut row = Vec::new();
            if self.peek() != &Token::RParen {
                loop {
                    row.push(self.parse_expr()?);
                    if !self.eat(&Token::Comma) {
                        break;
                    }
                }
            }
            self.expect(&Token::RParen)?;
            if row.len() != columns.len() {
                return Err(ParseError::Other(format!(
                    "INSERT row has {} values but {} columns",
                    row.len(),
                    columns.len()
                )));
            }
            rows.push(row);
            if !self.eat(&Token::Comma) {
                break;
            }
        }
        Ok(Insert {
            table,
            columns,
            rows,
        })
    }

    /// A full query: one or more `SELECT` cores chained by `UNION [ALL]`, with a
    /// trailing `ORDER BY`/`LIMIT`/`OFFSET` that applies to the whole result.
    fn parse_select(&mut self) -> Result<Select, ParseError> {
        let mut query = self.parse_select_core()?;

        let mut set_ops = Vec::new();
        while self.eat_keyword(Keyword::Union) {
            let all = self.eat_keyword(Keyword::All);
            let select = self.parse_select_core()?;
            set_ops.push(SetOp { all, select });
        }
        query.set_ops = set_ops;

        // Whole-query ORDER BY / LIMIT / OFFSET.
        query.order_by = self.parse_order_by()?;
        query.limit = if self.eat_keyword(Keyword::Limit) {
            Some(self.expect_u64()?)
        } else {
            None
        };
        query.offset = if self.eat_keyword(Keyword::Offset) {
            Some(self.expect_u64()?)
        } else {
            None
        };
        Ok(query)
    }

    /// One `SELECT` body: `SELECT [DISTINCT] items FROM t [joins] [WHERE]
    /// [GROUP BY] [HAVING]` — without the query-level `ORDER BY`/`LIMIT`/`UNION`.
    fn parse_select_core(&mut self) -> Result<Select, ParseError> {
        self.expect_keyword(Keyword::Select)?;
        let distinct = self.eat_keyword(Keyword::Distinct);
        let mut items = Vec::new();
        loop {
            if self.eat(&Token::Star) {
                items.push(SelectItem::Wildcard);
            } else {
                let expr = self.parse_expr()?;
                let alias = if self.eat_keyword(Keyword::As) {
                    Some(self.expect_ident()?)
                } else if let Token::Ident(_) = self.peek() {
                    Some(self.expect_ident()?)
                } else {
                    None
                };
                items.push(SelectItem::Expr { expr, alias });
            }
            if !self.eat(&Token::Comma) {
                break;
            }
        }

        self.expect_keyword(Keyword::From)?;
        let from = self.expect_ident()?;
        let from_alias = self.parse_table_alias(&from);

        let mut joins = Vec::new();
        while let Some(kind) = self.peek_join_kind() {
            joins.push(self.parse_join(kind)?);
        }

        let filter = if self.eat_keyword(Keyword::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let mut group_by = Vec::new();
        if self.eat_keyword(Keyword::Group) {
            self.expect_keyword(Keyword::By)?;
            loop {
                group_by.push(self.parse_expr()?);
                if !self.eat(&Token::Comma) {
                    break;
                }
            }
        }

        let having = if self.eat_keyword(Keyword::Having) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Select {
            distinct,
            items,
            from,
            from_alias,
            joins,
            filter,
            group_by,
            having,
            set_ops: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
        })
    }

    /// An optional table alias after a table reference: `[AS] <ident>`. Returns
    /// `default` (the table name) when none is present.
    fn parse_table_alias(&mut self, default: &str) -> String {
        if self.eat_keyword(Keyword::As) {
            return self.expect_ident().unwrap_or_else(|_| default.to_string());
        }
        if let Token::Ident(_) = self.peek() {
            if let Ok(a) = self.expect_ident() {
                return a;
            }
        }
        default.to_string()
    }

    /// If the next token begins a join clause, the join flavor it introduces.
    fn peek_join_kind(&self) -> Option<JoinKind> {
        match self.peek() {
            Token::Keyword(Keyword::Join) => Some(JoinKind::Inner),
            Token::Keyword(Keyword::Inner) => Some(JoinKind::Inner),
            Token::Keyword(Keyword::Left) => Some(JoinKind::Left),
            Token::Keyword(Keyword::Right) => Some(JoinKind::Right),
            Token::Keyword(Keyword::Cross) => Some(JoinKind::Cross),
            _ => None,
        }
    }

    fn parse_join(&mut self, kind: JoinKind) -> Result<Join, ParseError> {
        // Consume the flavor keyword(s) up to and including JOIN.
        match self.peek() {
            Token::Keyword(Keyword::Join) => {
                self.advance();
            }
            Token::Keyword(Keyword::Inner) | Token::Keyword(Keyword::Cross) => {
                self.advance();
                self.expect_keyword(Keyword::Join)?;
            }
            Token::Keyword(Keyword::Left) | Token::Keyword(Keyword::Right) => {
                self.advance();
                self.eat_keyword(Keyword::Outer); // optional
                self.expect_keyword(Keyword::Join)?;
            }
            _ => return Err(self.unexpected("JOIN".into())),
        }
        let table = self.expect_ident()?;
        let alias = self.parse_table_alias(&table);
        let on = if self.eat_keyword(Keyword::On) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Join {
            kind,
            table,
            alias,
            on,
        })
    }

    fn parse_order_by(&mut self) -> Result<Vec<OrderKey>, ParseError> {
        let mut order_by = Vec::new();
        if self.eat_keyword(Keyword::Order) {
            self.expect_keyword(Keyword::By)?;
            loop {
                let expr = self.parse_expr()?;
                let descending = if self.eat_keyword(Keyword::Desc) {
                    true
                } else {
                    self.eat_keyword(Keyword::Asc);
                    false
                };
                order_by.push(OrderKey { expr, descending });
                if !self.eat(&Token::Comma) {
                    break;
                }
            }
        }
        Ok(order_by)
    }

    fn parse_update(&mut self) -> Result<Update, ParseError> {
        self.expect_keyword(Keyword::Update)?;
        let table = self.expect_ident()?;
        self.expect_keyword(Keyword::Set)?;
        let mut assignments = Vec::new();
        loop {
            let col = self.parse_path()?;
            self.expect(&Token::Eq)?;
            let expr = self.parse_expr()?;
            assignments.push((col, expr));
            if !self.eat(&Token::Comma) {
                break;
            }
        }
        let filter = if self.eat_keyword(Keyword::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Update {
            table,
            assignments,
            filter,
        })
    }

    fn parse_delete(&mut self) -> Result<Delete, ParseError> {
        self.expect_keyword(Keyword::Delete)?;
        self.expect_keyword(Keyword::From)?;
        let table = self.expect_ident()?;
        let filter = if self.eat_keyword(Keyword::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Delete { table, filter })
    }

    // ---- helpers ----

    fn parse_ident_list(&mut self) -> Result<Vec<String>, ParseError> {
        let mut out = vec![self.expect_ident()?];
        while self.eat(&Token::Comma) {
            out.push(self.expect_ident()?);
        }
        Ok(out)
    }

    /// Parse a dotted path `a.b.c` into the string `"a.b.c"`.
    fn parse_path(&mut self) -> Result<String, ParseError> {
        let mut path = self.expect_ident()?;
        while self.eat(&Token::Dot) {
            path.push('.');
            path.push_str(&self.expect_ident()?);
        }
        Ok(path)
    }

    // ---- expressions (precedence climbing) ----

    fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_and()?;
        while self.eat_keyword(Keyword::Or) {
            let right = self.parse_and()?;
            left = bin(BinaryOp::Or, left, right);
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_not()?;
        while self.eat_keyword(Keyword::And) {
            let right = self.parse_not()?;
            left = bin(BinaryOp::And, left, right);
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Expr, ParseError> {
        if self.eat_keyword(Keyword::Not) {
            let expr = self.parse_not()?;
            Ok(Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(expr),
            })
        } else {
            self.parse_comparison()
        }
    }

    fn parse_comparison(&mut self) -> Result<Expr, ParseError> {
        let left = self.parse_additive()?;

        // Postfix IS [NOT] NULL.
        if self.eat_keyword(Keyword::Is) {
            let negated = self.eat_keyword(Keyword::Not);
            self.expect_keyword(Keyword::Null)?;
            return Ok(Expr::IsNull {
                expr: Box::new(left),
                negated,
            });
        }

        let op = match self.peek() {
            Token::Eq => BinaryOp::Eq,
            Token::NotEq => BinaryOp::NotEq,
            Token::Lt => BinaryOp::Lt,
            Token::LtEq => BinaryOp::LtEq,
            Token::Gt => BinaryOp::Gt,
            Token::GtEq => BinaryOp::GtEq,
            _ => return Ok(left),
        };
        self.advance();
        let right = self.parse_additive()?;
        Ok(bin(op, left, right))
    }

    fn parse_additive(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_multiplicative()?;
        loop {
            let op = match self.peek() {
                Token::Plus => BinaryOp::Add,
                Token::Minus => BinaryOp::Sub,
                _ => break,
            };
            self.advance();
            let right = self.parse_multiplicative()?;
            left = bin(op, left, right);
        }
        Ok(left)
    }

    fn parse_multiplicative(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_unary()?;
        loop {
            let op = match self.peek() {
                Token::Star => BinaryOp::Mul,
                Token::Slash => BinaryOp::Div,
                _ => break,
            };
            self.advance();
            let right = self.parse_unary()?;
            left = bin(op, left, right);
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr, ParseError> {
        if self.eat(&Token::Minus) {
            let expr = self.parse_unary()?;
            Ok(Expr::Unary {
                op: UnaryOp::Neg,
                expr: Box::new(expr),
            })
        } else {
            self.parse_primary()
        }
    }

    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        match self.peek().clone() {
            Token::Int(i) => {
                self.advance();
                Ok(Expr::Literal(Value::Int(i)))
            }
            Token::Float(f) => {
                self.advance();
                Ok(Expr::Literal(Value::Float(f)))
            }
            Token::Str(s) => {
                self.advance();
                Ok(Expr::Literal(Value::String(s)))
            }
            Token::Keyword(Keyword::True) => {
                self.advance();
                Ok(Expr::Literal(Value::Bool(true)))
            }
            Token::Keyword(Keyword::False) => {
                self.advance();
                Ok(Expr::Literal(Value::Bool(false)))
            }
            Token::Keyword(Keyword::Null) => {
                self.advance();
                Ok(Expr::Literal(Value::Null))
            }
            Token::LParen => {
                self.advance();
                let e = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                Ok(e)
            }
            // Array literal `[a, b, c]` (e.g. an embedding vector). Elements must
            // be constant: a literal, or a negated numeric literal.
            Token::LBracket => {
                self.advance();
                let mut items = Vec::new();
                if !self.eat(&Token::RBracket) {
                    loop {
                        let e = self.parse_expr()?;
                        items.push(const_value(&e).ok_or_else(|| {
                            ParseError::Other("array elements must be constant literals".into())
                        })?);
                        if !self.eat(&Token::Comma) {
                            break;
                        }
                    }
                    self.expect(&Token::RBracket)?;
                }
                Ok(Expr::Literal(Value::Array(items)))
            }
            Token::Keyword(kw) if agg_func(kw).is_some() => {
                self.advance();
                let func = agg_func(kw).unwrap();
                self.expect(&Token::LParen)?;
                let arg = if self.eat(&Token::Star) {
                    AggArg::Star
                } else {
                    AggArg::Expr(Box::new(self.parse_expr()?))
                };
                self.expect(&Token::RParen)?;
                Ok(Expr::Aggregate { func, arg })
            }
            Token::Ident(_) => {
                let path = self.parse_path()?;
                Ok(Expr::Column(path))
            }
            _ => Err(self.unexpected("an expression".into())),
        }
    }
}

fn bin(op: BinaryOp, left: Expr, right: Expr) -> Expr {
    Expr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

fn agg_func(kw: Keyword) -> Option<AggFunc> {
    match kw {
        Keyword::Count => Some(AggFunc::Count),
        Keyword::Sum => Some(AggFunc::Sum),
        Keyword::Avg => Some(AggFunc::Avg),
        Keyword::Min => Some(AggFunc::Min),
        Keyword::Max => Some(AggFunc::Max),
        _ => None,
    }
}

/// Fold a parse-time-constant expression — a literal, or a negated numeric
/// literal — into a `Value`, for array-literal elements.
fn const_value(e: &Expr) -> Option<Value> {
    match e {
        Expr::Literal(v) => Some(v.clone()),
        Expr::Unary {
            op: UnaryOp::Neg,
            expr,
        } => match expr.as_ref() {
            Expr::Literal(Value::Int(i)) => Some(Value::Int(-i)),
            Expr::Literal(Value::Float(f)) => Some(Value::Float(-f)),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_table() {
        let s = parse("CREATE TABLE users (PRIMARY KEY (id))").unwrap();
        assert_eq!(
            s,
            Statement::CreateTable(CreateTable {
                name: "users".into(),
                if_not_exists: false,
                primary_key: vec!["id".into()],
            })
        );
    }

    #[test]
    fn parse_insert_multi_row() {
        let s = parse("INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b')").unwrap();
        match s {
            Statement::Insert(i) => {
                assert_eq!(i.columns, vec!["id", "name"]);
                assert_eq!(i.rows.len(), 2);
            }
            _ => panic!("expected insert"),
        }
    }

    #[test]
    fn insert_arity_mismatch_errors() {
        assert!(parse("INSERT INTO t (a, b) VALUES (1)").is_err());
    }

    #[test]
    fn parse_select_full() {
        let s = parse(
            "SELECT id, name AS n, COUNT(*) FROM t WHERE a >= 3 AND b IS NOT NULL \
             ORDER BY id DESC LIMIT 10 OFFSET 5",
        )
        .unwrap();
        match s {
            Statement::Select(sel) => {
                assert_eq!(sel.from, "t");
                assert_eq!(sel.items.len(), 3);
                assert_eq!(sel.limit, Some(10));
                assert_eq!(sel.offset, Some(5));
                assert_eq!(sel.order_by.len(), 1);
                assert!(sel.order_by[0].descending);
                assert!(sel.filter.is_some());
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn operator_precedence() {
        // a + b * c  ==  a + (b * c)
        let s = parse("SELECT x FROM t WHERE a = b + c * d").unwrap();
        let Statement::Select(sel) = s else {
            panic!("expected select")
        };
        let Expr::Binary { op, right, .. } = sel.filter.unwrap() else {
            panic!("expected binary")
        };
        assert_eq!(op, BinaryOp::Eq);
        // RHS is b + (c*d)
        let Expr::Binary {
            op: add,
            right: mul,
            ..
        } = *right
        else {
            panic!("expected add")
        };
        assert_eq!(add, BinaryOp::Add);
        let Expr::Binary { op: mul_op, .. } = *mul else {
            panic!("expected mul")
        };
        assert_eq!(mul_op, BinaryOp::Mul);
    }

    #[test]
    fn parse_update_and_delete() {
        assert!(matches!(
            parse("UPDATE t SET a = 1, b = 'x' WHERE id = 5").unwrap(),
            Statement::Update(_)
        ));
        assert!(matches!(
            parse("DELETE FROM t WHERE id = 5").unwrap(),
            Statement::Delete(_)
        ));
    }

    #[test]
    fn nested_path_column() {
        let s = parse("SELECT a.b.c FROM t").unwrap();
        let Statement::Select(sel) = s else { panic!() };
        match &sel.items[0] {
            SelectItem::Expr {
                expr: Expr::Column(p),
                ..
            } => assert_eq!(p, "a.b.c"),
            _ => panic!("expected column path"),
        }
    }

    #[test]
    fn trailing_semicolon_ok() {
        assert!(parse("SELECT 1 FROM t;").is_ok());
    }

    #[test]
    fn parse_distinct_having() {
        let Statement::Select(sel) =
            parse("SELECT DISTINCT g, COUNT(*) FROM t GROUP BY g HAVING COUNT(*) > 1").unwrap()
        else {
            panic!("expected select")
        };
        assert!(sel.distinct);
        assert!(sel.having.is_some());
        assert_eq!(sel.group_by.len(), 1);
    }

    #[test]
    fn parse_joins_with_aliases() {
        let Statement::Select(sel) = parse(
            "SELECT u.name, o.amt FROM users u \
             JOIN orders o ON u.id = o.uid \
             LEFT JOIN refunds r ON r.oid = o.oid",
        )
        .unwrap() else {
            panic!("expected select")
        };
        assert_eq!(sel.from, "users");
        assert_eq!(sel.from_alias, "u");
        assert_eq!(sel.joins.len(), 2);
        assert_eq!(sel.joins[0].kind, JoinKind::Inner);
        assert_eq!(sel.joins[0].alias, "o");
        assert_eq!(sel.joins[1].kind, JoinKind::Left);
        assert!(sel.joins[1].on.is_some());
    }

    #[test]
    fn parse_union_all_with_trailing_order() {
        let Statement::Select(sel) =
            parse("SELECT id FROM a UNION ALL SELECT id FROM b ORDER BY id LIMIT 5").unwrap()
        else {
            panic!("expected select")
        };
        assert_eq!(sel.set_ops.len(), 1);
        assert!(sel.set_ops[0].all);
        assert_eq!(sel.set_ops[0].select.from, "b");
        // Trailing ORDER BY / LIMIT bind to the whole query, not the last leg.
        assert_eq!(sel.order_by.len(), 1);
        assert_eq!(sel.limit, Some(5));
        assert!(sel.set_ops[0].select.order_by.is_empty());
    }

    #[test]
    fn parse_alter_and_transactions() {
        assert_eq!(
            parse("ALTER TABLE t RENAME TO t2").unwrap(),
            Statement::AlterTable(AlterTable {
                name: "t".into(),
                action: AlterAction::RenameTable {
                    new_name: "t2".into()
                },
            })
        );
        assert_eq!(
            parse("ALTER TABLE t RENAME COLUMN a TO b").unwrap(),
            Statement::AlterTable(AlterTable {
                name: "t".into(),
                action: AlterAction::RenameColumn {
                    from: "a".into(),
                    to: "b".into()
                },
            })
        );
        assert_eq!(parse("BEGIN").unwrap(), Statement::Begin);
        assert_eq!(parse("COMMIT TRANSACTION").unwrap(), Statement::Commit);
        assert_eq!(parse("ROLLBACK").unwrap(), Statement::Rollback);
    }
}
