//! skaidb SQL frontend: lexer, AST, and parser.
//!
//! Targets a named subset of SQL:2016 core (SPEC §3). [`parse`] turns a SQL
//! string into a [`Statement`]; the query engine consumes the AST and executes
//! it against the storage layer.

pub mod ast;
pub mod bind;
mod parser;
mod token;

pub use ast::*;
pub use bind::{bind, param_count, BindError};
pub use parser::{parse, ParseError};
pub use token::{tokenize, Keyword, LexError, Token};
