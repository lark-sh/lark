//! Token types for the expression lexer.

use std::fmt;

/// Token kinds for the expression language.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenKind {
    // Literals
    Ident,
    String,
    Number,
    True,
    False,
    Null,

    // Operators
    EqEqEq,  // ===
    NeqEqEq, // !==
    EqEq,    // ==
    Neq,     // !=
    And,     // &&
    Or,      // ||
    Gt,      // >
    Lt,      // <
    Gte,     // >=
    Lte,     // <=
    Not,     // !
    Plus,    // +
    Minus,   // -
    Star,    // *
    Slash,   // /
    Percent, // %

    // Punctuation
    LParen,   // (
    RParen,   // )
    LBracket, // [
    RBracket, // ]
    Dot,      // .
    Comma,    // ,
    Question, // ?
    Colon,    // :

    // End of input
    Eof,
}

impl fmt::Display for TokenKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TokenKind::Ident => write!(f, "identifier"),
            TokenKind::String => write!(f, "string"),
            TokenKind::Number => write!(f, "number"),
            TokenKind::True => write!(f, "true"),
            TokenKind::False => write!(f, "false"),
            TokenKind::Null => write!(f, "null"),
            TokenKind::EqEqEq => write!(f, "==="),
            TokenKind::NeqEqEq => write!(f, "!=="),
            TokenKind::EqEq => write!(f, "=="),
            TokenKind::Neq => write!(f, "!="),
            TokenKind::And => write!(f, "&&"),
            TokenKind::Or => write!(f, "||"),
            TokenKind::Gt => write!(f, ">"),
            TokenKind::Lt => write!(f, "<"),
            TokenKind::Gte => write!(f, ">="),
            TokenKind::Lte => write!(f, "<="),
            TokenKind::Not => write!(f, "!"),
            TokenKind::Plus => write!(f, "+"),
            TokenKind::Minus => write!(f, "-"),
            TokenKind::Star => write!(f, "*"),
            TokenKind::Slash => write!(f, "/"),
            TokenKind::Percent => write!(f, "%"),
            TokenKind::LParen => write!(f, "("),
            TokenKind::RParen => write!(f, ")"),
            TokenKind::LBracket => write!(f, "["),
            TokenKind::RBracket => write!(f, "]"),
            TokenKind::Dot => write!(f, "."),
            TokenKind::Comma => write!(f, ","),
            TokenKind::Question => write!(f, "?"),
            TokenKind::Colon => write!(f, ":"),
            TokenKind::Eof => write!(f, "EOF"),
        }
    }
}

/// A token with its kind, value, and position.
#[derive(Debug, Clone)]
pub struct Token {
    pub kind: TokenKind,
    pub value: String,
    pub num_val: f64,
    pub pos: usize,
}

impl Token {
    pub fn new(kind: TokenKind, pos: usize) -> Self {
        Self {
            kind,
            value: String::new(),
            num_val: 0.0,
            pos,
        }
    }

    pub fn with_value(kind: TokenKind, value: String, pos: usize) -> Self {
        Self {
            kind,
            value,
            num_val: 0.0,
            pos,
        }
    }

    pub fn with_number(num: f64, pos: usize) -> Self {
        Self {
            kind: TokenKind::Number,
            value: String::new(),
            num_val: num,
            pos,
        }
    }
}
