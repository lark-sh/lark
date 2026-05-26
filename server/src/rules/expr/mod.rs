//! Expression parsing and evaluation for security rules.
//!
//! This module provides a pure Rust expression evaluator for
//! security rule expressions like `auth.uid === $uid`.

mod ast;
mod eval;
mod lexer;
mod parser;
mod token;
mod value;

pub use ast::*;
pub use eval::{EvalContext, EvalError, eval, eval_bool};
pub use lexer::Lexer;
pub use parser::parse;
pub use token::{Token, TokenKind};
pub use value::{OBJECT_SENTINEL_MARKER, Snapshot, Value, ValueKind};
