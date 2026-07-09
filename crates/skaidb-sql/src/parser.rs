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
    let mut p = Parser { tokens, pos: 0, params: 0 };
    let stmt = p.parse_statement()?;
    p.eat(&Token::Semicolon);
    p.expect_eof()?;
    Ok(stmt)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    /// Bind parameters (`?`) seen so far; assigns each its positional index.
    params: u16,
}

/// The bare table name from a (possibly `db.table`-qualified) reference — used
/// as the default alias, which is matched against unqualified column prefixes.
fn bare_table(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}

impl Parser {
    fn peek(&self) -> &Token {
        &self.tokens[self.pos]
    }

    fn advance(&mut self) -> Token {
        // Tokens are consumed strictly left-to-right (the parser never looks
        // back), so the consumed slot can be hollowed out instead of cloned.
        let t = std::mem::replace(&mut self.tokens[self.pos], Token::Eof);
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

    /// True if the next token is an identifier equal to `word` (case-insensitive).
    ///
    /// Used for *contextual* keywords (`STATUS`, `DATABASE`, `DATABASES`, `USE`):
    /// they only act as keywords in specific positions, so they stay usable as
    /// ordinary column/table names everywhere else.
    fn peek_ident_ci(&self, word: &str) -> bool {
        matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case(word))
    }

    /// Consume the next token if it is an identifier equal to `word` (ci).
    fn eat_ident_ci(&mut self, word: &str) -> bool {
        if self.peek_ident_ci(word) {
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
        // `USE <name>` — `use` is a contextual keyword (still a valid identifier
        // elsewhere), so it is matched here rather than in the lexer.
        if self.peek_ident_ci("use") {
            self.advance();
            // Optional `USE DATABASE <name>` — only treat a leading "database" as
            // the keyword when another identifier follows, so a database actually
            // named "database" can still be selected with `USE database`.
            if self.peek_ident_ci("database")
                && matches!(self.tokens.get(self.pos + 1), Some(Token::Ident(_)))
            {
                self.advance();
            }
            let name = self.expect_ident()?;
            return Ok(Statement::UseDatabase { name });
        }
        // `REBUILD SEARCH INDEX <name>` — both words contextual.
        if self.peek_ident_ci("rebuild") {
            self.advance();
            if !self.eat_ident_ci("search") {
                return Err(self.unexpected("SEARCH INDEX after REBUILD".into()));
            }
            self.expect_keyword(Keyword::Index)?;
            let name = self.expect_ident()?;
            return Ok(Statement::RebuildSearchIndex { name });
        }
        // Statement-level SET: `SET CONFIG k = 'v'` / `SET CONSISTENCY L`.
        if self.peek() == &Token::Keyword(Keyword::Set) {
            self.advance();
            if self.eat_ident_ci("config") {
                let key = self.parse_path()?;
                self.expect(&Token::Eq)?;
                let value = match self.advance() {
                    Token::Str(v) => v,
                    Token::Int(n) => n.to_string(),
                    Token::Float(f) => f.to_string(),
                    Token::Keyword(Keyword::True) => "true".into(),
                    Token::Keyword(Keyword::False) => "false".into(),
                    other => {
                        return Err(ParseError::Other(format!(
                            "SET CONFIG expects a literal value, found {other:?}"
                        )))
                    }
                };
                return Ok(Statement::SetConfig { key, value });
            }
            if self.eat_ident_ci("consistency") {
                let level = match self.advance() {
                    Token::Ident(s) => s.to_ascii_uppercase(),
                    // ALL lexes as the UNION ALL keyword.
                    Token::Keyword(Keyword::All) => "ALL".to_string(),
                    other => {
                        return Err(ParseError::Other(format!(
                            "SET CONSISTENCY takes ONE, QUORUM, or ALL — found {other:?}"
                        )))
                    }
                };
                if !matches!(level.as_str(), "ONE" | "QUORUM" | "ALL") {
                    return Err(ParseError::Other(
                        "SET CONSISTENCY takes ONE, QUORUM, or ALL".into(),
                    ));
                }
                return Ok(Statement::SetConsistency { level });
            }
            return Err(self.unexpected("CONFIG or CONSISTENCY after SET".into()));
        }
        // `REPAIR CLUSTER` / `RECLAIM` — cluster maintenance.
        if self.peek_ident_ci("repair") {
            self.advance();
            if !self.eat_ident_ci("cluster") {
                return Err(self.unexpected("CLUSTER after REPAIR".into()));
            }
            return Ok(Statement::RepairCluster);
        }
        if self.peek_ident_ci("reclaim") {
            self.advance();
            return Ok(Statement::Reclaim);
        }
        // `EXPLAIN SCORE <select> FOR <literal>` — per-row BM25 breakdown
        // (EXPLAIN/SCORE/FOR are contextual identifiers).
        if self.peek_ident_ci("explain") {
            self.advance();
            if !self.eat_ident_ci("score") {
                return Err(ParseError::Other(
                    "EXPLAIN supports only EXPLAIN SCORE <select> FOR <pk>".into(),
                ));
            }
            let select = self.parse_select()?;
            self.expect_keyword(Keyword::For).map_err(|_| {
                ParseError::Other("EXPLAIN SCORE needs `FOR <primary-key literal>`".into())
            })?;
            let key = match self.parse_expr()? {
                Expr::Literal(v) => v,
                Expr::Unary {
                    op: UnaryOp::Neg,
                    expr,
                } => match *expr {
                    Expr::Literal(skaidb_types::Value::Int(n)) => skaidb_types::Value::Int(-n),
                    Expr::Literal(skaidb_types::Value::Float(f)) => {
                        skaidb_types::Value::Float(-f)
                    }
                    _ => {
                        return Err(ParseError::Other(
                            "EXPLAIN SCORE FOR takes a literal primary-key value".into(),
                        ))
                    }
                },
                _ => {
                    return Err(ParseError::Other(
                        "EXPLAIN SCORE FOR takes a literal primary-key value".into(),
                    ))
                }
            };
            return Ok(Statement::ExplainScore {
                select: Box::new(select),
                key,
            });
        }
        // `SUGGEST '<text>' ON <index> [COLUMN <col>] [LIMIT n]` — term
        // suggestions from a search index (SUGGEST is contextual).
        if self.peek_ident_ci("suggest") {
            self.advance();
            let text = match self.advance() {
                Token::Str(s) => s,
                other => {
                    return Err(ParseError::Other(format!(
                        "SUGGEST expects a quoted input string, found {other:?}"
                    )))
                }
            };
            self.expect_keyword(Keyword::On)?;
            let index = self.expect_ident()?;
            let column = if self.eat_keyword(Keyword::Column) {
                Some(self.parse_path()?)
            } else {
                None
            };
            let limit = if self.eat_keyword(Keyword::Limit) {
                match self.advance() {
                    Token::Int(n) if n > 0 => n as u64,
                    other => {
                        return Err(ParseError::Other(format!(
                            "SUGGEST LIMIT expects a positive integer, found {other:?}"
                        )))
                    }
                }
            } else {
                5
            };
            return Ok(Statement::Suggest {
                text,
                index,
                column,
                limit,
            });
        }
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
                } else if self.eat_keyword(Keyword::Grants) {
                    let role = if self.eat_keyword(Keyword::For) {
                        Some(self.expect_ident()?)
                    } else {
                        None
                    };
                    Ok(Statement::ShowGrants { role })
                } else if self.eat_ident_ci("status") {
                    Ok(Statement::ShowStatus)
                } else if self.eat_ident_ci("databases") {
                    Ok(Statement::ShowDatabases)
                } else if self.eat_ident_ci("cluster") {
                    Ok(Statement::ShowCluster)
                } else if self.eat_ident_ci("config") {
                    let like = if self.eat_ident_ci("like") {
                        match self.advance() {
                            Token::Str(p) => Some(p),
                            other => {
                                return Err(ParseError::Other(format!(
                                    "SHOW CONFIG LIKE expects a quoted pattern, found {other:?}"
                                )))
                            }
                        }
                    } else {
                        None
                    };
                    Ok(Statement::ShowConfig { like })
                } else if self.eat_ident_ci("slow") {
                    if !self.eat_ident_ci("queries") {
                        return Err(self.unexpected("QUERIES after SHOW SLOW".into()));
                    }
                    let limit = if self.eat_keyword(Keyword::Limit) {
                        match self.advance() {
                            Token::Int(n) if n > 0 => Some(n as u64),
                            other => {
                                return Err(ParseError::Other(format!(
                                    "SHOW SLOW QUERIES LIMIT expects a positive integer,                                      found {other:?}"
                                )))
                            }
                        }
                    } else {
                        None
                    };
                    Ok(Statement::ShowSlowQueries { limit })
                } else {
                    Err(self.unexpected(
                        "TABLES, INDEXES, GRANTS, STATUS, DATABASES, CLUSTER, CONFIG,                          or SLOW QUERIES after SHOW"
                            .into(),
                    ))
                }
            }
            Token::Keyword(Keyword::Grant) => self.parse_grant(),
            Token::Keyword(Keyword::Revoke) => self.parse_revoke(),
            _ => Err(self.unexpected("a statement".into())),
        }
    }

    /// `GRANT ROLE <r> TO <u>` | `GRANT <priv> ON <table|*> TO <role>`.
    fn parse_grant(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Grant)?;
        if self.eat_keyword(Keyword::Role) {
            let role = self.expect_ident()?;
            self.expect_keyword(Keyword::To)?;
            let to = self.expect_ident()?;
            return Ok(Statement::GrantRole { role, to });
        }
        let privilege = self.parse_privilege()?;
        self.expect_keyword(Keyword::On)?;
        let object = self.parse_grant_object()?;
        self.expect_keyword(Keyword::To)?;
        let to = self.expect_ident()?;
        Ok(Statement::Grant {
            privilege,
            object,
            to,
        })
    }

    /// `REVOKE ROLE <r> FROM <u>` | `REVOKE <priv> ON <table|*> FROM <role>`.
    fn parse_revoke(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Revoke)?;
        if self.eat_keyword(Keyword::Role) {
            let role = self.expect_ident()?;
            self.expect_keyword(Keyword::From)?;
            let from = self.expect_ident()?;
            return Ok(Statement::RevokeRole { role, from });
        }
        let privilege = self.parse_privilege()?;
        self.expect_keyword(Keyword::On)?;
        let object = self.parse_grant_object()?;
        self.expect_keyword(Keyword::From)?;
        let from = self.expect_ident()?;
        Ok(Statement::Revoke {
            privilege,
            object,
            from,
        })
    }

    /// A grantable privilege keyword, canonicalized to lowercase.
    fn parse_privilege(&mut self) -> Result<String, ParseError> {
        let name = match self.advance() {
            Token::Keyword(Keyword::Select) => "select",
            Token::Keyword(Keyword::Insert) => "insert",
            Token::Keyword(Keyword::Update) => "update",
            Token::Keyword(Keyword::Delete) => "delete",
            Token::Keyword(Keyword::Create) => "create",
            Token::Keyword(Keyword::Drop) => "drop",
            Token::Keyword(Keyword::Grant) => "grant",
            Token::Keyword(Keyword::Admin) => "admin",
            other => {
                return Err(ParseError::Other(format!(
                    "expected a privilege (SELECT/INSERT/UPDATE/DELETE/CREATE/DROP/GRANT/ADMIN), found {other:?}"
                )))
            }
        };
        Ok(name.to_string())
    }

    /// `ON <table>`, `ON DATABASE <db>`, or `ON *` (global).
    fn parse_grant_object(&mut self) -> Result<GrantObject, ParseError> {
        if self.eat(&Token::Star) {
            Ok(GrantObject::Global)
        } else if self.eat_ident_ci("database") {
            Ok(GrantObject::Database(self.expect_ident()?))
        } else {
            Ok(GrantObject::Table(self.parse_table_name()?))
        }
    }

    fn expect_string(&mut self) -> Result<String, ParseError> {
        match self.advance() {
            Token::Str(s) => Ok(s),
            other => Err(ParseError::Other(format!(
                "expected a quoted string, found {other:?}"
            ))),
        }
    }

    /// `ALTER TABLE <name> RENAME { TO <new> | COLUMN <from> TO <to> }`.
    fn parse_alter(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Alter)?;
        // `ALTER CLUSTER ADD NODE '<addr>'` / `REMOVE NODE '<id>'`.
        if self.eat_ident_ci("cluster") {
            let add = if self.eat_ident_ci("add") {
                true
            } else if self.eat_ident_ci("remove") {
                false
            } else {
                return Err(self.unexpected("ADD or REMOVE after ALTER CLUSTER".into()));
            };
            if !self.eat_ident_ci("node") {
                return Err(self.unexpected("NODE after ALTER CLUSTER ADD/REMOVE".into()));
            }
            let node = self.expect_string().map_err(|_| {
                ParseError::Other(
                    "ALTER CLUSTER ADD/REMOVE NODE takes a quoted 'host:port' / id".into(),
                )
            })?;
            return Ok(Statement::AlterCluster { add, node });
        }
        if self.eat_keyword(Keyword::User) {
            let name = self.expect_ident()?;
            self.expect_keyword(Keyword::Password)?;
            let password = self.expect_string()?;
            return Ok(Statement::AlterUser { name, password });
        }
        // `ALTER VECTOR INDEX <name> SET (<option> = <literal>, ...)`.
        if self.eat_keyword(Keyword::Vector) {
            self.expect_keyword(Keyword::Index)?;
            let name = self.expect_ident()?;
            self.expect_keyword(Keyword::Set)?;
            self.expect(&Token::LParen)?;
            let options = self.parse_option_list()?;
            return Ok(Statement::AlterVectorIndex { name, options });
        }
        // `ALTER SEARCH INDEX <name> SET (<option> = <literal>, ...)` —
        // SEARCH stays contextual.
        if self.eat_ident_ci("search") {
            self.expect_keyword(Keyword::Index)?;
            let name = self.expect_ident()?;
            self.expect_keyword(Keyword::Set)?;
            self.expect(&Token::LParen)?;
            let options = self.parse_option_list()?;
            return Ok(Statement::AlterSearchIndex { name, options });
        }
        self.expect_keyword(Keyword::Table)?;
        let name = self.parse_table_name()?;
        self.expect_keyword(Keyword::Rename)?;
        let action = if self.eat_keyword(Keyword::Column) {
            let from = self.parse_path()?;
            self.expect_keyword(Keyword::To)?;
            let to = self.parse_path()?;
            AlterAction::RenameColumn { from, to }
        } else {
            self.expect_keyword(Keyword::To)?;
            let new_name = self.parse_table_name()?;
            AlterAction::RenameTable { new_name }
        };
        Ok(Statement::AlterTable(AlterTable { name, action }))
    }

    fn parse_create(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Create)?;
        if self.eat_keyword(Keyword::Table) {
            let if_not_exists = self.parse_if_not_exists()?;
            let name = self.parse_table_name()?;
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
        } else if self.eat_keyword(Keyword::Timeseries) {
            // CREATE TIMESERIES TABLE [IF NOT EXISTS] name
            //   (SERIES KEY (l1 [, ...]) [, RETENTION <duration>])
            self.expect_keyword(Keyword::Table)?;
            let if_not_exists = self.parse_if_not_exists()?;
            let name = self.parse_table_name()?;
            self.expect(&Token::LParen)?;
            self.expect_keyword(Keyword::Series)?;
            self.expect_keyword(Keyword::Key)?;
            self.expect(&Token::LParen)?;
            let series_key = self.parse_ident_list()?;
            self.expect(&Token::RParen)?;
            let mut retention_ms = None;
            let mut ooo_ms = None;
            while self.eat(&Token::Comma) {
                let target = if self.eat_keyword(Keyword::Retention) {
                    &mut retention_ms
                } else if self.eat_keyword(Keyword::Ooo) {
                    &mut ooo_ms
                } else {
                    return Err(self.unexpected("RETENTION or OOO".into()));
                };
                *target = Some(match self.advance() {
                    Token::Duration(ms) => ms,
                    other => {
                        return Err(ParseError::Other(format!(
                            "expected a duration like 30d or 12h, found {other:?}"
                        )))
                    }
                });
            }
            self.expect(&Token::RParen)?;
            Ok(Statement::CreateTimeseriesTable(CreateTimeseriesTable {
                name,
                if_not_exists,
                series_key,
                retention_ms,
                ooo_ms,
            }))
        } else if self.eat_keyword(Keyword::User) {
            let if_not_exists = self.parse_if_not_exists()?;
            let name = self.expect_ident()?;
            let (mut password, mut verifier) = (None, None);
            if self.eat_keyword(Keyword::Password) {
                password = Some(self.expect_string()?);
            } else if self.eat_keyword(Keyword::Verifier) {
                verifier = Some(self.expect_string()?);
            } else {
                return Err(self.unexpected("PASSWORD or VERIFIER".into()));
            }
            Ok(Statement::CreateUser(CreateUser {
                name,
                if_not_exists,
                password,
                verifier,
            }))
        } else if self.eat_keyword(Keyword::Role) {
            let if_not_exists = self.parse_if_not_exists()?;
            let name = self.expect_ident()?;
            let state = if self.eat_keyword(Keyword::Grants) {
                Some(self.expect_string()?)
            } else {
                None
            };
            Ok(Statement::CreateRole {
                name,
                if_not_exists,
                state,
            })
        } else if self.eat_keyword(Keyword::Rollup) {
            // CREATE ROLLUP [IF NOT EXISTS] name ON table BUCKET <dur>
            //   [RETENTION <dur>]
            let if_not_exists = self.parse_if_not_exists()?;
            let name = self.parse_table_name()?;
            self.expect_keyword(Keyword::On)?;
            let table = self.parse_table_name()?;
            self.expect_keyword(Keyword::Bucket)?;
            let bucket_ms = match self.advance() {
                Token::Duration(ms) => ms,
                other => {
                    return Err(ParseError::Other(format!(
                        "BUCKET expects a duration like 5m, found {other:?}"
                    )))
                }
            };
            let mut retention_ms = None;
            if self.eat_keyword(Keyword::Retention) {
                retention_ms = Some(match self.advance() {
                    Token::Duration(ms) => ms,
                    other => {
                        return Err(ParseError::Other(format!(
                            "RETENTION expects a duration like 90d, found {other:?}"
                        )))
                    }
                });
            }
            Ok(Statement::CreateRollup(CreateRollup {
                name,
                if_not_exists,
                table,
                bucket_ms,
                retention_ms,
            }))
        } else if self.eat_keyword(Keyword::Vector) {
            self.expect_keyword(Keyword::Index)?;
            let if_not_exists = self.parse_if_not_exists()?;
            let name = self.expect_ident()?;
            self.expect_keyword(Keyword::On)?;
            let table = self.parse_table_name()?;
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
        } else if self.peek_ident_ci("search") {
            // `SEARCH` is contextual so `SEARCH('...')` stays a valid
            // function call in expressions.
            self.advance();
            self.expect_keyword(Keyword::Index)?;
            let if_not_exists = self.parse_if_not_exists()?;
            let name = self.expect_ident()?;
            self.expect_keyword(Keyword::On)?;
            let table = self.parse_table_name()?;
            self.expect(&Token::LParen)?;
            let mut paths = vec![self.parse_path()?];
            while self.eat(&Token::Comma) {
                paths.push(self.parse_path()?);
            }
            self.expect(&Token::RParen)?;
            let options = self.parse_with_options()?;
            Ok(Statement::CreateSearchIndex(CreateSearchIndex {
                name,
                if_not_exists,
                table,
                paths,
                options,
            }))
        } else if self.eat_keyword(Keyword::Index) {
            let if_not_exists = self.parse_if_not_exists()?;
            let name = self.expect_ident()?;
            self.expect_keyword(Keyword::On)?;
            let table = self.parse_table_name()?;
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
        } else if self.eat_ident_ci("database") {
            let if_not_exists = self.parse_if_not_exists()?;
            let name = self.expect_ident()?;
            Ok(Statement::CreateDatabase {
                name,
                if_not_exists,
            })
        } else {
            Err(self.unexpected("TABLE, INDEX, or DATABASE".into()))
        }
    }

    fn parse_drop(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Drop)?;
        if self.eat_keyword(Keyword::Table) {
            let if_exists = self.parse_if_exists()?;
            let name = self.parse_table_name()?;
            Ok(Statement::DropTable { name, if_exists })
        } else if self.eat_keyword(Keyword::User) {
            let if_exists = self.parse_if_exists()?;
            let name = self.expect_ident()?;
            Ok(Statement::DropUser { name, if_exists })
        } else if self.eat_keyword(Keyword::Role) {
            let if_exists = self.parse_if_exists()?;
            let name = self.expect_ident()?;
            Ok(Statement::DropRole { name, if_exists })
        } else if self.eat_keyword(Keyword::Vector) {
            self.expect_keyword(Keyword::Index)?;
            let if_exists = self.parse_if_exists()?;
            let name = self.expect_ident()?;
            Ok(Statement::DropVectorIndex { name, if_exists })
        } else if self.peek_ident_ci("search") {
            self.advance();
            self.expect_keyword(Keyword::Index)?;
            let if_exists = self.parse_if_exists()?;
            let name = self.expect_ident()?;
            Ok(Statement::DropSearchIndex { name, if_exists })
        } else if self.eat_keyword(Keyword::Index) {
            let if_exists = self.parse_if_exists()?;
            let name = self.expect_ident()?;
            Ok(Statement::DropIndex { name, if_exists })
        } else if self.eat_ident_ci("database") {
            let if_exists = self.parse_if_exists()?;
            let name = self.expect_ident()?;
            Ok(Statement::DropDatabase { name, if_exists })
        } else {
            Err(self.unexpected("TABLE, INDEX, VECTOR INDEX, SEARCH INDEX, or DATABASE".into()))
        }
    }

    /// `[WITH (name = value, ...)]` — an optional options list. Names may be
    /// dotted (per-column options like `title.boost`); values may be string,
    /// integer, float, or boolean literals. Each is captured as its literal
    /// text and validated by the consumer of the statement.
    fn parse_with_options(&mut self) -> Result<Vec<(String, String)>, ParseError> {
        // `WITH` is contextual: only an options list when followed by `(`.
        if !(self.peek_ident_ci("with")
            && matches!(self.tokens.get(self.pos + 1), Some(Token::LParen)))
        {
            return Ok(Vec::new());
        }
        self.advance();
        self.expect(&Token::LParen)?;
        self.parse_option_list()
    }

    /// The body of a parenthesized option list, after the `(`:
    /// `name = literal [, ...] )`.
    fn parse_option_list(&mut self) -> Result<Vec<(String, String)>, ParseError> {
        let mut options = Vec::new();
        loop {
            let name = self.parse_path()?.to_ascii_lowercase();
            self.expect(&Token::Eq)?;
            let value = match self.advance() {
                Token::Str(s) => s,
                Token::Int(i) => i.to_string(),
                Token::Float(x) => x.to_string(),
                Token::Keyword(Keyword::True) => "true".to_string(),
                Token::Keyword(Keyword::False) => "false".to_string(),
                other => {
                    return Err(ParseError::Other(format!(
                        "expected a literal value for option {name}, found {other:?}"
                    )))
                }
            };
            options.push((name, value));
            if !self.eat(&Token::Comma) {
                break;
            }
        }
        self.expect(&Token::RParen)?;
        Ok(options)
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
        let table = self.parse_table_name()?;
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
        let from = self.parse_table_name()?;
        let from_alias = self
            .parse_table_alias()
            .unwrap_or_else(|| bare_table(&from).to_string());

        let mut joins = Vec::new();
        while let Some(kind) = self.peek_join_kind() {
            joins.push(self.parse_join(kind)?);
        }

        // `NEAREST (<path>, <query>, <k>)` — ANN clause (vector search).
        let nearest = if self.eat_keyword(Keyword::Nearest) {
            self.expect(&Token::LParen)?;
            let path = self.parse_path()?;
            self.expect(&Token::Comma)?;
            let query = self.parse_expr()?;
            self.expect(&Token::Comma)?;
            let k = self.parse_expr()?;
            self.expect(&Token::RParen)?;
            Some(Box::new(Nearest { path, query, k }))
        } else {
            None
        };

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
            nearest,
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
    /// `None` when absent so the caller only allocates the default when needed.
    fn parse_table_alias(&mut self) -> Option<String> {
        if self.eat_keyword(Keyword::As) {
            return self.expect_ident().ok();
        }
        if let Token::Ident(_) = self.peek() {
            if let Ok(a) = self.expect_ident() {
                return Some(a);
            }
        }
        None
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
        let table = self.parse_table_name()?;
        let alias = self
            .parse_table_alias()
            .unwrap_or_else(|| bare_table(&table).to_string());
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
        let table = self.parse_table_name()?;
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
        let table = self.parse_table_name()?;
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

    /// Parse a table reference: `<table>` or `<database> . <table>`. A database
    /// qualifier is preserved verbatim as the string `"db.table"`; the engine
    /// resolves it (or a bare name) against the connection's current database.
    /// Table and database identifiers never contain `.`, so the join is
    /// unambiguous.
    fn parse_table_name(&mut self) -> Result<String, ParseError> {
        let mut first = self.expect_ident()?;
        if self.eat(&Token::Dot) {
            let table = self.expect_ident()?;
            first.reserve(1 + table.len());
            first.push('.');
            first.push_str(&table);
        }
        Ok(first)
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
        match self.peek() {
            Token::Int(i) => {
                let i = *i;
                self.advance();
                Ok(Expr::Literal(Value::Int(i)))
            }
            Token::Float(f) => {
                let f = *f;
                self.advance();
                Ok(Expr::Literal(Value::Float(f)))
            }
            // Duration literals (`5m`, `2h`, `30d`) are millisecond integers.
            Token::Duration(ms) => {
                let ms = *ms;
                self.advance();
                Ok(Expr::Literal(Value::Int(ms)))
            }
            Token::Str(_) => match self.advance() {
                Token::Str(s) => Ok(Expr::Literal(Value::String(s))),
                _ => unreachable!(),
            },
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
            Token::Question => {
                self.advance();
                let idx = self.params;
                self.params = self.params.checked_add(1).ok_or_else(|| {
                    ParseError::Other("too many bind parameters".into())
                })?;
                Ok(Expr::Parameter(idx))
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
                        items.push(const_value(e).ok_or_else(|| {
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
            Token::Keyword(kw) if agg_func(*kw).is_some() => {
                let func = agg_func(*kw).unwrap();
                self.advance();
                self.expect(&Token::LParen)?;
                let arg = if self.eat(&Token::Star) {
                    AggArg::Star
                } else if self.eat_keyword(Keyword::Distinct) {
                    AggArg::Distinct(Box::new(self.parse_expr()?))
                } else {
                    AggArg::Expr(Box::new(self.parse_expr()?))
                };
                self.expect(&Token::RParen)?;
                Ok(Expr::Aggregate { func, arg })
            }
            Token::Ident(_) => {
                let path = self.parse_path()?;
                // A bare name directly followed by `(` is a function call:
                // the time-series aggregates get `Expr::Aggregate` (the
                // executor treats them like other aggregates), everything
                // else becomes a scalar `Expr::Func` resolved at evaluation.
                if !path.contains('.') && self.peek() == &Token::LParen {
                    self.advance();
                    let mut args = Vec::new();
                    if !self.eat(&Token::RParen) {
                        loop {
                            args.push(self.parse_expr()?);
                            if !self.eat(&Token::Comma) {
                                break;
                            }
                        }
                        self.expect(&Token::RParen)?;
                    }
                    if path.eq_ignore_ascii_case("approx_count_distinct") {
                        if args.len() != 1 {
                            return Err(ParseError::Other(
                                "APPROX_COUNT_DISTINCT(expr) takes exactly one argument".into(),
                            ));
                        }
                        return Ok(Expr::Aggregate {
                            func: AggFunc::Count,
                            arg: AggArg::ApproxDistinct(Box::new(
                                args.into_iter().next().unwrap(),
                            )),
                        });
                    }
                    if let Some(func) = ts_agg_func(&path) {
                        if args.len() != 1 {
                            return Err(ParseError::Other(format!(
                                "{path}() takes exactly one argument"
                            )));
                        }
                        return Ok(Expr::Aggregate {
                            func,
                            arg: AggArg::Expr(Box::new(args.into_iter().next().unwrap())),
                        });
                    }
                    return Ok(Expr::Func {
                        name: path.to_ascii_lowercase(),
                        args,
                    });
                }
                Ok(Expr::Column(path))
            }
            _ => Err(self.unexpected("an expression".into())),
        }
    }
}

/// Time-series aggregate functions, spelled as ordinary identifiers so the
/// names stay usable as column names when not called.
fn ts_agg_func(name: &str) -> Option<AggFunc> {
    match name.to_ascii_lowercase().as_str() {
        "rate" => Some(AggFunc::Rate),
        "increase" => Some(AggFunc::Increase),
        "delta" => Some(AggFunc::Delta),
        "first" => Some(AggFunc::First),
        "last" => Some(AggFunc::Last),
        _ => None,
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
fn const_value(e: Expr) -> Option<Value> {
    match e {
        Expr::Literal(v) => Some(v),
        Expr::Unary {
            op: UnaryOp::Neg,
            expr,
        } => match *expr {
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
    fn parse_search_index_ddl() {
        assert_eq!(
            parse("CREATE SEARCH INDEX articles_fts ON articles (title, body) \
                   WITH (analyzer = 'english', refresh_ms = 500)")
            .unwrap(),
            Statement::CreateSearchIndex(CreateSearchIndex {
                name: "articles_fts".into(),
                if_not_exists: false,
                table: "articles".into(),
                paths: vec!["title".into(), "body".into()],
                options: vec![
                    ("analyzer".into(), "english".into()),
                    ("refresh_ms".into(), "500".into()),
                ],
            })
        );
        // WITH is optional; IF NOT EXISTS and dotted paths work.
        assert_eq!(
            parse("CREATE SEARCH INDEX IF NOT EXISTS s ON t (meta.title)").unwrap(),
            Statement::CreateSearchIndex(CreateSearchIndex {
                name: "s".into(),
                if_not_exists: true,
                table: "t".into(),
                paths: vec!["meta.title".into()],
                options: vec![],
            })
        );
        assert_eq!(
            parse("DROP SEARCH INDEX IF EXISTS articles_fts").unwrap(),
            Statement::DropSearchIndex {
                name: "articles_fts".into(),
                if_exists: true,
            }
        );
        assert_eq!(
            parse("REBUILD SEARCH INDEX articles_fts").unwrap(),
            Statement::RebuildSearchIndex {
                name: "articles_fts".into(),
            }
        );
        // Options must be literals.
        assert!(parse("CREATE SEARCH INDEX s ON t (a) WITH (analyzer = english)").is_err());
        assert!(parse("REBUILD INDEX x").is_err());
    }

    #[test]
    fn search_functions_parse_as_plain_functions() {
        // `SEARCH`/`MATCH`/`score` stay contextual: they parse as ordinary
        // function calls in expressions, and the search-index DDL does not
        // reserve them.
        let s = parse(
            "SELECT id, score() FROM articles \
             WHERE MATCH(body, 'quick fox') AND published = true \
             ORDER BY score() DESC LIMIT 10",
        )
        .unwrap();
        let Statement::Select(sel) = s else {
            panic!("expected select");
        };
        assert_eq!(sel.limit, Some(10));
        assert_eq!(sel.order_by.len(), 1);
        assert!(sel.order_by[0].descending);
        assert_eq!(
            sel.order_by[0].expr,
            Expr::Func {
                name: "score".into(),
                args: vec![],
            }
        );
        // The MATCH predicate is the left arm of the AND.
        let Some(Expr::Binary { .. }) = sel.filter else {
            panic!("expected AND filter");
        };
        // A table named `search` can still be queried.
        assert!(parse("SELECT * FROM search WHERE SEARCH('title:rust +db')").is_ok());
    }

    #[test]
    fn parse_grant_objects() {
        assert_eq!(
            parse("GRANT SELECT ON t TO reader").unwrap(),
            Statement::Grant {
                privilege: "select".into(),
                object: GrantObject::Table("t".into()),
                to: "reader".into(),
            }
        );
        assert_eq!(
            parse("GRANT insert ON DATABASE sales TO writer").unwrap(),
            Statement::Grant {
                privilege: "insert".into(),
                object: GrantObject::Database("sales".into()),
                to: "writer".into(),
            }
        );
        assert_eq!(
            parse("REVOKE ADMIN ON * FROM ops").unwrap(),
            Statement::Revoke {
                privilege: "admin".into(),
                object: GrantObject::Global,
                from: "ops".into(),
            }
        );
        // In grant position `DATABASE` is a contextual keyword, so it needs
        // a database name after it (a table named `database` can't be the
        // grant object — same tradeoff as the other contextual keywords).
        assert!(parse("GRANT SELECT ON database TO reader").is_err());
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

    #[test]
    fn parse_status_and_database_statements() {
        assert_eq!(parse("SHOW STATUS").unwrap(), Statement::ShowStatus);
        assert_eq!(parse("SHOW DATABASES").unwrap(), Statement::ShowDatabases);
        assert_eq!(
            parse("CREATE DATABASE shop").unwrap(),
            Statement::CreateDatabase {
                name: "shop".into(),
                if_not_exists: false,
            }
        );
        assert_eq!(
            parse("CREATE DATABASE IF NOT EXISTS shop").unwrap(),
            Statement::CreateDatabase {
                name: "shop".into(),
                if_not_exists: true,
            }
        );
        assert_eq!(
            parse("DROP DATABASE IF EXISTS shop").unwrap(),
            Statement::DropDatabase {
                name: "shop".into(),
                if_exists: true,
            }
        );
        assert_eq!(
            parse("USE shop").unwrap(),
            Statement::UseDatabase { name: "shop".into() }
        );
        assert_eq!(
            parse("USE DATABASE shop").unwrap(),
            Statement::UseDatabase { name: "shop".into() }
        );
    }

    #[test]
    fn contextual_keywords_remain_usable_as_identifiers() {
        // `status`, `database`, `databases`, and `use` are contextual keywords:
        // they must still parse as ordinary column/table names.
        let stmt = parse("SELECT status, use, database FROM databases WHERE status = 'x'").unwrap();
        match stmt {
            Statement::Select(s) => assert_eq!(s.from, "databases"),
            other => panic!("expected select, got {other:?}"),
        }
        assert_eq!(
            parse("CREATE TABLE status (PRIMARY KEY (use))").unwrap(),
            Statement::CreateTable(CreateTable {
                name: "status".into(),
                if_not_exists: false,
                primary_key: vec!["use".into()],
            })
        );
        // A database genuinely named "database" is still selectable.
        assert_eq!(
            parse("USE database").unwrap(),
            Statement::UseDatabase {
                name: "database".into()
            }
        );
    }

    #[test]
    fn parse_qualified_table_names() {
        match parse("SELECT id FROM shop.orders").unwrap() {
            Statement::Select(s) => {
                assert_eq!(s.from, "shop.orders");
                assert_eq!(s.from_alias, "orders"); // alias defaults to the bare name
            }
            other => panic!("{other:?}"),
        }
        match parse("INSERT INTO shop.orders (id) VALUES (1)").unwrap() {
            Statement::Insert(i) => assert_eq!(i.table, "shop.orders"),
            other => panic!("{other:?}"),
        }
        assert_eq!(
            parse("CREATE TABLE shop.orders (PRIMARY KEY (id))").unwrap(),
            Statement::CreateTable(CreateTable {
                name: "shop.orders".into(),
                if_not_exists: false,
                primary_key: vec!["id".into()],
            })
        );
        // Joins and the ON-table of an index can be qualified too.
        match parse("SELECT * FROM shop.a JOIN shop.b ON a.id = b.id").unwrap() {
            Statement::Select(s) => {
                assert_eq!(s.from, "shop.a");
                assert_eq!(s.joins[0].table, "shop.b");
            }
            other => panic!("{other:?}"),
        }
    }
}
