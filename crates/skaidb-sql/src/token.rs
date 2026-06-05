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
    /// String literal (quotes already stripped, escapes resolved).
    Str(String),

    // Punctuation / operators.
    Comma,
    Dot,
    LParen,
    RParen,
    LBracket,
    RBracket,
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
}

impl Keyword {
    fn from_str(s: &str) -> Option<Keyword> {
        use Keyword::*;
        let kw = match s.to_ascii_uppercase().as_str() {
            "SELECT" => Select,
            "FROM" => From,
            "WHERE" => Where,
            "INSERT" => Insert,
            "INTO" => Into,
            "VALUES" => Values,
            "UPDATE" => Update,
            "SET" => Set,
            "DELETE" => Delete,
            "CREATE" => Create,
            "DROP" => Drop,
            "TABLE" => Table,
            "INDEX" => Index,
            "ON" => On,
            "PRIMARY" => Primary,
            "KEY" => Key,
            "ORDER" => Order,
            "BY" => By,
            "ASC" => Asc,
            "DESC" => Desc,
            "LIMIT" => Limit,
            "OFFSET" => Offset,
            "AND" => And,
            "OR" => Or,
            "NOT" => Not,
            "NULL" => Null,
            "IS" => Is,
            "TRUE" => True,
            "FALSE" => False,
            "IF" => If,
            "EXISTS" => Exists,
            "GROUP" => Group,
            "AS" => As,
            "COUNT" => Count,
            "SUM" => Sum,
            "AVG" => Avg,
            "MIN" => Min,
            "MAX" => Max,
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
pub fn tokenize(input: &str) -> Result<Vec<Token>, LexError> {
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;
    let mut tokens = Vec::new();

    while i < chars.len() {
        let c = chars[i];
        match c {
            c if c.is_whitespace() => i += 1,
            '-' if i + 1 < chars.len() && chars[i + 1] == '-' => {
                // Line comment to end of line.
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
            }
            ',' => {
                tokens.push(Token::Comma);
                i += 1;
            }
            '.' => {
                tokens.push(Token::Dot);
                i += 1;
            }
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            '[' => {
                tokens.push(Token::LBracket);
                i += 1;
            }
            ']' => {
                tokens.push(Token::RBracket);
                i += 1;
            }
            '*' => {
                tokens.push(Token::Star);
                i += 1;
            }
            '+' => {
                tokens.push(Token::Plus);
                i += 1;
            }
            '-' => {
                tokens.push(Token::Minus);
                i += 1;
            }
            '/' => {
                tokens.push(Token::Slash);
                i += 1;
            }
            ';' => {
                tokens.push(Token::Semicolon);
                i += 1;
            }
            '=' => {
                tokens.push(Token::Eq);
                i += 1;
            }
            '<' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Token::LtEq);
                    i += 2;
                } else if i + 1 < chars.len() && chars[i + 1] == '>' {
                    tokens.push(Token::NotEq);
                    i += 2;
                } else {
                    tokens.push(Token::Lt);
                    i += 1;
                }
            }
            '>' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Token::GtEq);
                    i += 2;
                } else {
                    tokens.push(Token::Gt);
                    i += 1;
                }
            }
            '!' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                tokens.push(Token::NotEq);
                i += 2;
            }
            '\'' => {
                let (s, next) = lex_string(&chars, i)?;
                tokens.push(Token::Str(s));
                i = next;
            }
            '"' => {
                let (s, next) = lex_quoted_ident(&chars, i)?;
                tokens.push(Token::Ident(s));
                i = next;
            }
            c if c.is_ascii_digit() => {
                let (tok, next) = lex_number(&chars, i)?;
                tokens.push(tok);
                i = next;
            }
            c if c.is_alphabetic() || c == '_' => {
                let (word, next) = lex_word(&chars, i);
                match Keyword::from_str(&word) {
                    Some(kw) => tokens.push(Token::Keyword(kw)),
                    None => tokens.push(Token::Ident(word)),
                }
                i = next;
            }
            other => return Err(LexError::UnexpectedChar(other)),
        }
    }

    tokens.push(Token::Eof);
    Ok(tokens)
}

fn lex_string(chars: &[char], start: usize) -> Result<(String, usize), LexError> {
    let mut i = start + 1; // skip opening quote
    let mut out = String::new();
    while i < chars.len() {
        let c = chars[i];
        if c == '\'' {
            if i + 1 < chars.len() && chars[i + 1] == '\'' {
                out.push('\''); // escaped quote
                i += 2;
            } else {
                return Ok((out, i + 1));
            }
        } else {
            out.push(c);
            i += 1;
        }
    }
    Err(LexError::UnterminatedString)
}

fn lex_quoted_ident(chars: &[char], start: usize) -> Result<(String, usize), LexError> {
    let mut i = start + 1;
    let mut out = String::new();
    while i < chars.len() {
        if chars[i] == '"' {
            return Ok((out, i + 1));
        }
        out.push(chars[i]);
        i += 1;
    }
    Err(LexError::UnterminatedString)
}

fn lex_number(chars: &[char], start: usize) -> Result<(Token, usize), LexError> {
    let mut i = start;
    let mut seen_dot = false;
    while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
        if chars[i] == '.' {
            // A dot followed by a non-digit is field access, not a decimal point.
            if seen_dot || i + 1 >= chars.len() || !chars[i + 1].is_ascii_digit() {
                break;
            }
            seen_dot = true;
        }
        i += 1;
    }
    let text: String = chars[start..i].iter().collect();
    let token = if seen_dot {
        Token::Float(
            text.parse()
                .map_err(|_| LexError::InvalidNumber(text.clone()))?,
        )
    } else {
        Token::Int(
            text.parse()
                .map_err(|_| LexError::InvalidNumber(text.clone()))?,
        )
    };
    Ok((token, i))
}

fn lex_word(chars: &[char], start: usize) -> (String, usize) {
    let mut i = start;
    while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
        i += 1;
    }
    (chars[start..i].iter().collect(), i)
}

#[cfg(test)]
mod tests {
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
