//! skaidb SQL frontend: lexer, AST, and parser.
//!
//! Targets a named subset of SQL:2016 core (SPEC §3). [`parse`] turns a SQL
//! string into a [`Statement`]; the query engine consumes the AST and executes
//! it against the storage layer.

pub mod ast;
mod parser;
mod token;

pub use ast::*;
pub use parser::{parse, ParseError};
pub use token::{tokenize, Keyword, LexError, Token};
