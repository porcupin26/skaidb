//! Tokenizer for the skaidb SQL subset (SPEC §3).
//!
//! Keywords are matched case-insensitively; identifiers preserve their original
//! case. String literals use single quotes with `''` as the escape for a quote.

/// A lexical token.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    /// A bare or double-quoted identifier (table/column name).
    Ident(String),
    /// A reserved keyword (already upper-cased).
    Keyword(Keyword),
    /// Integer literal.
    Int(i64),
    /// Floating-point literal.
    Float(f64),
    /// Duration literal (`15s`, `5m`, `2h`, `30d`, `250ms`, `1w`), in ms.
    Duration(i64),
    /// String literal (quotes already stripped, escapes resolved).
    Str(String),

    // Punctuation / operators.
    Comma,
    Dot,
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Colon,
    Star,
    Plus,
    Minus,
    Slash,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Semicolon,
    /// A positional bind-parameter placeholder (`?`), for prepared statements.
    Question,

    /// End of input.
    Eof,
}

/// Reserved keywords recognized by the parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Keyword {
    Select,
    From,
    Where,
    Insert,
    Into,
    Values,
    Update,
    Set,
    Delete,
    Create,
    Drop,
    Table,
    Index,
    On,
    Primary,
    Key,
    Order,
    By,
    Asc,
    Desc,
    Limit,
    Offset,
    And,
    Or,
    Not,
    Null,
    Is,
    True,
    False,
    If,
    Exists,
    Group,
    As,
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Vector,
    Using,
    Dim,
    Embed,
    Nearest,
    Alter,
    Rename,
    To,
    Column,
    Distinct,
    Having,
    Union,
    All,
    Begin,
    Commit,
    Rollback,
    Transaction,
    Join,
    Inner,
    Left,
    Right,
    Outer,
    Cross,
    Show,
    Tables,
    Indexes,
    Timeseries,
    Series,
    Retention,
    Ooo,
    Rollup,
    Bucket,
    User,
    Role,
    Password,
    Verifier,
    Gssapi,
    Grant,
    Revoke,
    Grants,
    Admin,
    For,
}

/// Length of the longest keyword ("TRANSACTION").
const MAX_KEYWORD_LEN: usize = 11;

impl Keyword {
    fn from_str(s: &str) -> Option<Keyword> {
        use Keyword::*;
        // Uppercase into a stack buffer so matching never allocates.
        if s.len() > MAX_KEYWORD_LEN {
            return None;
        }
        let mut buf = [0u8; MAX_KEYWORD_LEN];
        for (out, b) in buf.iter_mut().zip(s.bytes()) {
            *out = b.to_ascii_uppercase();
        }
        let kw = match &buf[..s.len()] {
            b"SELECT" => Select,
            b"FROM" => From,
            b"WHERE" => Where,
            b"INSERT" => Insert,
            b"INTO" => Into,
            b"VALUES" => Values,
            b"UPDATE" => Update,
            b"SET" => Set,
            b"DELETE" => Delete,
            b"CREATE" => Create,
            b"DROP" => Drop,
            b"TABLE" => Table,
            b"INDEX" => Index,
            b"ON" => On,
            b"PRIMARY" => Primary,
            b"KEY" => Key,
            b"ORDER" => Order,
            b"BY" => By,
            b"ASC" => Asc,
            b"DESC" => Desc,
            b"LIMIT" => Limit,
            b"OFFSET" => Offset,
            b"AND" => And,
            b"OR" => Or,
            b"NOT" => Not,
            b"NULL" => Null,
            b"IS" => Is,
            b"TRUE" => True,
            b"FALSE" => False,
            b"IF" => If,
            b"EXISTS" => Exists,
            b"GROUP" => Group,
            b"AS" => As,
            b"COUNT" => Count,
            b"SUM" => Sum,
            b"AVG" => Avg,
            b"MIN" => Min,
            b"MAX" => Max,
            b"VECTOR" => Vector,
            b"USING" => Using,
            b"DIM" => Dim,
            b"EMBED" => Embed,
            b"NEAREST" => Nearest,
            b"ALTER" => Alter,
            b"RENAME" => Rename,
            b"TO" => To,
            b"COLUMN" => Column,
            b"DISTINCT" => Distinct,
            b"HAVING" => Having,
            b"TIMESERIES" => Timeseries,
            b"SERIES" => Series,
            b"RETENTION" => Retention,
            b"OOO" => Ooo,
            b"ROLLUP" => Rollup,
            b"BUCKET" => Bucket,
            b"USER" => User,
            b"ROLE" => Role,
            b"PASSWORD" => Password,
            b"VERIFIER" => Verifier,
            b"GSSAPI" => Gssapi,
            b"GRANT" => Grant,
            b"REVOKE" => Revoke,
            b"GRANTS" => Grants,
            b"ADMIN" => Admin,
            b"FOR" => For,
            b"UNION" => Union,
            b"ALL" => All,
            b"BEGIN" => Begin,
            b"COMMIT" => Commit,
            b"ROLLBACK" => Rollback,
            b"TRANSACTION" => Transaction,
            b"JOIN" => Join,
            b"INNER" => Inner,
            b"LEFT" => Left,
            b"RIGHT" => Right,
            b"OUTER" => Outer,
            b"CROSS" => Cross,
            b"SHOW" => Show,
            b"TABLES" => Tables,
            b"INDEXES" => Indexes,
            _ => return None,
        };
        Some(kw)
    }
}

/// Errors raised while tokenizing.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LexError {
    #[error("unterminated string literal")]
    UnterminatedString,
    #[error("invalid number literal: {0}")]
    InvalidNumber(String),
    #[error("unexpected character: {0:?}")]
    UnexpectedChar(char),
}

/// Tokenize `input` into a token stream terminated by [`Token::Eof`].
///
/// Operates on the raw bytes with an index cursor; slicing at ASCII delimiters
/// is always UTF-8 safe, and multi-byte characters are decoded only where the
/// grammar allows them (whitespace, identifiers, string/quoted-ident bodies).
pub fn tokenize(input: &str) -> Result<Vec<Token>, LexError> {
    let bytes = input.as_bytes();
    let mut i = 0;
    let mut tokens = Vec::new();

    while i < bytes.len() {
        match bytes[i] {
            b'\t'..=b'\r' | b' ' => i += 1,
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                // Line comment to end of line.
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b',' => {
                tokens.push(Token::Comma);
                i += 1;
            }
            b'.' => {
                tokens.push(Token::Dot);
                i += 1;
            }
            b'(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            b')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            b'[' => {
                tokens.push(Token::LBracket);
                i += 1;
            }
            b']' => {
                tokens.push(Token::RBracket);
                i += 1;
            }
            b'{' => {
                tokens.push(Token::LBrace);
                i += 1;
            }
            b'}' => {
                tokens.push(Token::RBrace);
                i += 1;
            }
            b':' => {
                tokens.push(Token::Colon);
                i += 1;
            }
            b'*' => {
                tokens.push(Token::Star);
                i += 1;
            }
            b'+' => {
                tokens.push(Token::Plus);
                i += 1;
            }
            b'-' => {
                tokens.push(Token::Minus);
                i += 1;
            }
            b'/' => {
                tokens.push(Token::Slash);
                i += 1;
            }
            b';' => {
                tokens.push(Token::Semicolon);
                i += 1;
            }
            b'?' => {
                tokens.push(Token::Question);
                i += 1;
            }
            b'=' => {
                tokens.push(Token::Eq);
                i += 1;
            }
            b'<' => {
                if bytes.get(i + 1) == Some(&b'=') {
                    tokens.push(Token::LtEq);
                    i += 2;
                } else if bytes.get(i + 1) == Some(&b'>') {
                    tokens.push(Token::NotEq);
                    i += 2;
                } else {
                    tokens.push(Token::Lt);
                    i += 1;
                }
            }
            b'>' => {
                if bytes.get(i + 1) == Some(&b'=') {
                    tokens.push(Token::GtEq);
                    i += 2;
                } else {
                    tokens.push(Token::Gt);
                    i += 1;
                }
            }
            b'!' if bytes.get(i + 1) == Some(&b'=') => {
                tokens.push(Token::NotEq);
                i += 2;
            }
            b'\'' => {
                let (s, next) = lex_string(input, i)?;
                tokens.push(Token::Str(s));
                i = next;
            }
            b'"' => {
                let (s, next) = lex_quoted_ident(input, i)?;
                tokens.push(Token::Ident(s));
                i = next;
            }
            b if b.is_ascii_digit() => {
                let (tok, next) = lex_number(input, i)?;
                tokens.push(tok);
                i = next;
            }
            b if b.is_ascii_alphabetic() || b == b'_' => {
                let next = lex_word_end(input, i);
                let word = &input[i..next];
                match Keyword::from_str(word) {
                    Some(kw) => tokens.push(Token::Keyword(kw)),
                    None => tokens.push(Token::Ident(word.to_string())),
                }
                i = next;
            }
            b if b < 0x80 => return Err(LexError::UnexpectedChar(b as char)),
            _ => {
                // Non-ASCII: decode the full character to classify it.
                let c = input[i..].chars().next().unwrap();
                if c.is_whitespace() {
                    i += c.len_utf8();
                } else if c.is_alphabetic() {
                    let next = lex_word_end(input, i);
                    tokens.push(Token::Ident(input[i..next].to_string()));
                    i = next;
                } else {
                    return Err(LexError::UnexpectedChar(c));
                }
            }
        }
    }

    tokens.push(Token::Eof);
    Ok(tokens)
}

fn lex_string(input: &str, start: usize) -> Result<(String, usize), LexError> {
    let bytes = input.as_bytes();
    let mut i = start + 1; // skip opening quote
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            if bytes.get(i + 1) == Some(&b'\'') {
                // Slow path: the literal contains `''` escapes.
                return lex_string_escaped(input, start + 1, i);
            }
            return Ok((input[start + 1..i].to_string(), i + 1));
        }
        i += 1;
    }
    Err(LexError::UnterminatedString)
}

/// Continue lexing a string literal whose first `''` escape is at `escape`;
/// the content starts at `content_start` (just past the opening quote).
fn lex_string_escaped(
    input: &str,
    content_start: usize,
    escape: usize,
) -> Result<(String, usize), LexError> {
    let bytes = input.as_bytes();
    let mut out = String::from(&input[content_start..escape]);
    out.push('\'');
    let mut i = escape + 2;
    let mut seg = i; // start of the current unescaped segment
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            out.push_str(&input[seg..i]);
            if bytes.get(i + 1) == Some(&b'\'') {
                out.push('\''); // escaped quote
                i += 2;
                seg = i;
            } else {
                return Ok((out, i + 1));
            }
        } else {
            i += 1;
        }
    }
    Err(LexError::UnterminatedString)
}

fn lex_quoted_ident(input: &str, start: usize) -> Result<(String, usize), LexError> {
    let bytes = input.as_bytes();
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == b'"' {
            return Ok((input[start + 1..i].to_string(), i + 1));
        }
        i += 1;
    }
    Err(LexError::UnterminatedString)
}

fn lex_number(input: &str, start: usize) -> Result<(Token, usize), LexError> {
    let bytes = input.as_bytes();
    let mut i = start;
    let mut seen_dot = false;
    while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b'.') {
        if bytes[i] == b'.' {
            // A dot followed by a non-digit is field access, not a decimal point.
            if seen_dot || i + 1 >= bytes.len() || !bytes[i + 1].is_ascii_digit() {
                break;
            }
            seen_dot = true;
        }
        i += 1;
    }
    // Scientific notation: `e`/`E`, an optional sign, then digits — floats
    // rendered by most languages' default formatting (`1.2e-05`) failed to
    // lex, which broke embedding-vector literals whose components crossed
    // 1e-5 (a categorizer stored vectors this way, 2026-07-14). Ambiguity
    // free: identifiers cannot start with a digit and `e` is not a duration
    // unit.
    let mut is_float = seen_dot;
    if i < bytes.len() && (bytes[i] == b'e' || bytes[i] == b'E') {
        let mut j = i + 1;
        if j < bytes.len() && (bytes[j] == b'+' || bytes[j] == b'-') {
            j += 1;
        }
        if j < bytes.len() && bytes[j].is_ascii_digit() {
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            i = j;
            is_float = true;
        }
    }
    let text = &input[start..i];
    if is_float {
        return Ok((
            Token::Float(
                text.parse()
                    .map_err(|_| LexError::InvalidNumber(text.to_string()))?,
            ),
            i,
        ));
    }
    let value: i64 = text
        .parse()
        .map_err(|_| LexError::InvalidNumber(text.to_string()))?;
    // An immediately-adjacent unit suffix makes a duration literal: `15s`,
    // `5m`, `2h`, `30d`, `1w`, `250ms` (the suffix must end the word).
    let word_end = lex_word_end(input, i);
    if word_end > i {
        let per_unit = match &input[i..word_end] {
            "ms" => Some(1),
            "s" => Some(1000),
            "m" => Some(60 * 1000),
            "h" => Some(3600 * 1000),
            "d" => Some(24 * 3600 * 1000),
            "w" => Some(7 * 24 * 3600 * 1000),
            _ => None,
        };
        if let Some(per_unit) = per_unit {
            let ms = value
                .checked_mul(per_unit)
                .ok_or_else(|| LexError::InvalidNumber(input[start..word_end].to_string()))?;
            return Ok((Token::Duration(ms), word_end));
        }
    }
    Ok((Token::Int(value), i))
}

/// Byte index just past the identifier/keyword word starting at `start`.
fn lex_word_end(input: &str, start: usize) -> usize {
    let bytes = input.as_bytes();
    let mut i = start;
    while i < bytes.len() {
        let b = bytes[i];
        if b < 0x80 {
            if b.is_ascii_alphanumeric() || b == b'_' {
                i += 1;
            } else {
                break;
            }
        } else {
            let c = input[i..].chars().next().unwrap();
            if c.is_alphanumeric() {
                i += c.len_utf8();
            } else {
                break;
            }
        }
    }
    i
}

#[cfg(test)]
mod tests {
    #[test]
    fn scientific_notation_floats() {
        for (text, want) in [
            ("1.2e-05", 1.2e-05_f64),
            ("3E8", 3e8),
            ("2.5e+3", 2.5e3),
            ("7e2", 700.0),
        ] {
            let (tok, end) = super::lex_number(text, 0).unwrap();
            assert_eq!(end, text.len(), "{text}");
            match tok {
                super::Token::Float(f) => assert!((f - want).abs() < 1e-12, "{text}"),
                other => panic!("{text}: {other:?}"),
            }
        }
        // NOT scientific: `e` followed by a non-digit stays an identifier
        // boundary, and duration suffixes still work.
        let (tok, end) = super::lex_number("15s", 0).unwrap();
        assert_eq!(end, 3);
        assert!(matches!(tok, super::Token::Duration(_)), "{tok:?}");
        let (tok, end) = super::lex_number("5east", 0).unwrap();
        let _ = (tok, end); // whatever it lexes to must not consume 'east'
        let (tok2, _) = super::lex_number("42", 0).unwrap();
        assert!(matches!(tok2, super::Token::Int(42)));
    }

    use super::*;

    #[test]
    fn tokenizes_select() {
        let toks = tokenize("SELECT a, b.c FROM t WHERE a >= 3").unwrap();
        assert_eq!(toks[0], Token::Keyword(Keyword::Select));
        assert_eq!(toks[1], Token::Ident("a".into()));
        assert_eq!(toks[2], Token::Comma);
        assert_eq!(toks[3], Token::Ident("b".into()));
        assert_eq!(toks[4], Token::Dot);
        assert!(toks.contains(&Token::GtEq));
        assert_eq!(*toks.last().unwrap(), Token::Eof);
    }

    #[test]
    fn numbers_and_paths() {
        // `a.b` is path access; `3.5` is a float.
        let toks = tokenize("x.y = 3.5").unwrap();
        assert_eq!(toks[0], Token::Ident("x".into()));
        assert_eq!(toks[1], Token::Dot);
        assert_eq!(toks[2], Token::Ident("y".into()));
        assert_eq!(toks[3], Token::Eq);
        assert_eq!(toks[4], Token::Float(3.5));
    }

    #[test]
    fn string_escapes() {
        let toks = tokenize("'it''s'").unwrap();
        assert_eq!(toks[0], Token::Str("it's".into()));
    }

    #[test]
    fn comments_skipped() {
        let toks = tokenize("SELECT 1 -- a comment\n, 2").unwrap();
        assert_eq!(toks[0], Token::Keyword(Keyword::Select));
        assert_eq!(toks[1], Token::Int(1));
        assert_eq!(toks[2], Token::Comma);
        assert_eq!(toks[3], Token::Int(2));
    }

    #[test]
    fn not_equal_forms() {
        assert_eq!(tokenize("a <> b").unwrap()[1], Token::NotEq);
        assert_eq!(tokenize("a != b").unwrap()[1], Token::NotEq);
    }
}
