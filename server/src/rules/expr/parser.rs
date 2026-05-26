//! Parser for rule expressions.

use super::ast::{BinaryOp, Expr, LiteralValue, UnaryOp};
use super::lexer::Lexer;
use super::token::{Token, TokenKind};

/// Maximum AST depth to prevent stack overflow during parsing.
pub const MAX_AST_DEPTH: usize = 50;

/// Maximum number of AST nodes allowed in a single expression.
/// Checked at compile time to prevent complex rules from slowing down evaluation.
pub const MAX_AST_NODES: usize = 1000;

/// Parse an expression string into an AST.
pub fn parse(src: &str) -> Result<Expr, String> {
    let tokens = Lexer::tokenize(src)?;
    let mut parser = Parser::new(tokens);
    let mut expr = parser.parse_expr()?;

    // Ensure we consumed all tokens
    if parser.current().kind != TokenKind::Eof {
        return Err(format!(
            "unexpected token {} at position {}",
            parser.current().kind,
            parser.current().pos
        ));
    }

    // Check expression complexity (compile-time budget)
    let node_count = expr.count_nodes();
    if node_count > MAX_AST_NODES {
        return Err(format!(
            "expression too complex: {} nodes exceeds maximum {}",
            node_count, MAX_AST_NODES
        ));
    }

    // Pre-compile regex patterns in matches() calls
    precompile_regexes(&mut expr);

    Ok(expr)
}

/// Walk the AST and pre-compile string literal arguments to matches() calls.
/// This avoids compiling the regex on every evaluation.
fn precompile_regexes(expr: &mut Expr) {
    match expr {
        Expr::Call { callee, args } => {
            // Check if this is a .matches(string_literal) call
            if let Expr::Member { property, .. } = callee.as_ref()
                && property == "matches"
                && args.len() == 1
                && let Expr::Literal(LiteralValue::String(pattern)) = &args[0]
                && let Ok(re) = regex::Regex::new(pattern)
            {
                args[0] = Expr::Literal(LiteralValue::Regex(re));
            }
            // If regex compilation fails, leave as string — runtime will return false
            // Recurse into callee and args
            precompile_regexes(callee);
            for arg in args.iter_mut() {
                precompile_regexes(arg);
            }
        }
        Expr::Binary { left, right, .. } => {
            precompile_regexes(left);
            precompile_regexes(right);
        }
        Expr::Unary { operand, .. } => {
            precompile_regexes(operand);
        }
        Expr::Ternary {
            condition,
            then_branch,
            else_branch,
        } => {
            precompile_regexes(condition);
            precompile_regexes(then_branch);
            precompile_regexes(else_branch);
        }
        Expr::Member { object, .. } => {
            precompile_regexes(object);
        }
        Expr::Array(elements) => {
            for el in elements.iter_mut() {
                precompile_regexes(el);
            }
        }
        Expr::Literal(_) | Expr::Ident(_) => {}
    }
}

/// Parser for rule expressions.
struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    depth: usize,
    eof_token: Token,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            pos: 0,
            depth: 0,
            eof_token: Token {
                kind: TokenKind::Eof,
                value: String::new(),
                num_val: 0.0,
                pos: 0,
            },
        }
    }

    /// Check and increment depth, returning error if too deep.
    fn enter_depth(&mut self) -> Result<(), String> {
        self.depth += 1;
        if self.depth > MAX_AST_DEPTH {
            return Err(format!(
                "expression too complex: nesting depth {} exceeds maximum {}",
                self.depth, MAX_AST_DEPTH
            ));
        }
        Ok(())
    }

    /// Decrement depth when leaving a nested context.
    fn leave_depth(&mut self) {
        self.depth = self.depth.saturating_sub(1);
    }

    fn current(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&self.eof_token)
    }

    fn advance(&mut self) -> Token {
        let tok = self.current().clone();
        self.pos += 1;
        tok
    }

    fn matches(&self, kinds: &[TokenKind]) -> bool {
        kinds.contains(&self.current().kind)
    }

    fn expect(&mut self, kind: TokenKind) -> Result<Token, String> {
        if self.current().kind != kind {
            return Err(format!(
                "expected {}, got {} at position {}",
                kind,
                self.current().kind,
                self.current().pos
            ));
        }
        Ok(self.advance())
    }

    /// Entry point - parses a full expression.
    fn parse_expr(&mut self) -> Result<Expr, String> {
        self.parse_ternary()
    }

    /// Parse ternary: expr ? expr : expr
    fn parse_ternary(&mut self) -> Result<Expr, String> {
        let cond = self.parse_or()?;

        if self.matches(&[TokenKind::Question]) {
            self.advance();
            self.enter_depth()?;
            let then_branch = self.parse_expr()?;
            self.expect(TokenKind::Colon)?;
            let else_branch = self.parse_expr()?;
            self.leave_depth();
            return Ok(Expr::Ternary {
                condition: Box::new(cond),
                then_branch: Box::new(then_branch),
                else_branch: Box::new(else_branch),
            });
        }

        Ok(cond)
    }

    /// Parse or: and (|| and)*
    fn parse_or(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_and()?;

        while self.matches(&[TokenKind::Or]) {
            self.advance();
            let right = self.parse_and()?;
            left = Expr::Binary {
                op: BinaryOp::Or,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse and: equality (&& equality)*
    fn parse_and(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_equality()?;

        while self.matches(&[TokenKind::And]) {
            self.advance();
            let right = self.parse_equality()?;
            left = Expr::Binary {
                op: BinaryOp::And,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse equality: compare ((=== | !== | == | !=) compare)*
    fn parse_equality(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_compare()?;

        while self.matches(&[
            TokenKind::EqEqEq,
            TokenKind::NeqEqEq,
            TokenKind::EqEq,
            TokenKind::Neq,
        ]) {
            let op_tok = self.advance();
            let op = match op_tok.kind {
                TokenKind::EqEqEq => BinaryOp::StrictEq,
                TokenKind::NeqEqEq => BinaryOp::StrictNotEq,
                TokenKind::EqEq => BinaryOp::Eq,
                TokenKind::Neq => BinaryOp::NotEq,
                _ => unreachable!(),
            };
            let right = self.parse_compare()?;
            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse compare: additive ((> | < | >= | <=) additive)*
    fn parse_compare(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_additive()?;

        while self.matches(&[TokenKind::Gt, TokenKind::Lt, TokenKind::Gte, TokenKind::Lte]) {
            let op_tok = self.advance();
            let op = match op_tok.kind {
                TokenKind::Gt => BinaryOp::Gt,
                TokenKind::Lt => BinaryOp::Lt,
                TokenKind::Gte => BinaryOp::Gte,
                TokenKind::Lte => BinaryOp::Lte,
                _ => unreachable!(),
            };
            let right = self.parse_additive()?;
            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse additive: multiplicative ((+ | -) multiplicative)*
    fn parse_additive(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_multiplicative()?;

        while self.matches(&[TokenKind::Plus, TokenKind::Minus]) {
            let op_tok = self.advance();
            let op = match op_tok.kind {
                TokenKind::Plus => BinaryOp::Add,
                TokenKind::Minus => BinaryOp::Sub,
                _ => unreachable!(),
            };
            let right = self.parse_multiplicative()?;
            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse multiplicative: unary ((* | / | %) unary)*
    fn parse_multiplicative(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_unary()?;

        while self.matches(&[TokenKind::Star, TokenKind::Slash, TokenKind::Percent]) {
            let op_tok = self.advance();
            let op = match op_tok.kind {
                TokenKind::Star => BinaryOp::Mul,
                TokenKind::Slash => BinaryOp::Div,
                TokenKind::Percent => BinaryOp::Mod,
                _ => unreachable!(),
            };
            let right = self.parse_unary()?;
            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse unary: (! | -) unary | postfix
    fn parse_unary(&mut self) -> Result<Expr, String> {
        if self.matches(&[TokenKind::Not]) {
            self.advance();
            self.enter_depth()?;
            let operand = self.parse_unary()?;
            self.leave_depth();
            return Ok(Expr::Unary {
                op: UnaryOp::Not,
                operand: Box::new(operand),
            });
        }

        if self.matches(&[TokenKind::Minus]) {
            self.advance();
            self.enter_depth()?;
            let operand = self.parse_unary()?;
            self.leave_depth();
            return Ok(Expr::Unary {
                op: UnaryOp::Neg,
                operand: Box::new(operand),
            });
        }

        self.parse_postfix()
    }

    /// Parse postfix: primary (. IDENT | ( args ))*
    fn parse_postfix(&mut self) -> Result<Expr, String> {
        let mut expr = self.parse_primary()?;

        loop {
            if self.matches(&[TokenKind::Dot]) {
                self.advance();
                let ident = self.expect(TokenKind::Ident)?;
                expr = Expr::Member {
                    object: Box::new(expr),
                    property: ident.value,
                };
            } else if self.matches(&[TokenKind::LParen]) {
                self.advance();
                let args = self.parse_args()?;
                self.expect(TokenKind::RParen)?;
                expr = Expr::Call {
                    callee: Box::new(expr),
                    args,
                };
            } else {
                break;
            }
        }

        Ok(expr)
    }

    /// Parse args: (expr (, expr)*)?
    fn parse_args(&mut self) -> Result<Vec<Expr>, String> {
        let mut args = Vec::new();

        if self.matches(&[TokenKind::RParen]) {
            return Ok(args);
        }

        self.enter_depth()?;
        args.push(self.parse_expr()?);

        while self.matches(&[TokenKind::Comma]) {
            self.advance();
            args.push(self.parse_expr()?);
        }
        self.leave_depth();

        Ok(args)
    }

    /// Parse primary: IDENT | STRING | NUMBER | true | false | null | ( expr ) | [ elements ]
    fn parse_primary(&mut self) -> Result<Expr, String> {
        match self.current().kind {
            TokenKind::Ident => {
                let tok = self.advance();
                Ok(Expr::Ident(tok.value))
            }

            TokenKind::String => {
                let tok = self.advance();
                Ok(Expr::Literal(LiteralValue::String(tok.value)))
            }

            TokenKind::Number => {
                let tok = self.advance();
                Ok(Expr::Literal(LiteralValue::Number(tok.num_val)))
            }

            TokenKind::True => {
                self.advance();
                Ok(Expr::Literal(LiteralValue::Bool(true)))
            }

            TokenKind::False => {
                self.advance();
                Ok(Expr::Literal(LiteralValue::Bool(false)))
            }

            TokenKind::Null => {
                self.advance();
                Ok(Expr::Literal(LiteralValue::Null))
            }

            TokenKind::LParen => {
                self.advance();
                self.enter_depth()?;
                let expr = self.parse_expr()?;
                self.leave_depth();
                self.expect(TokenKind::RParen)?;
                Ok(expr)
            }

            TokenKind::LBracket => {
                self.advance();
                self.enter_depth()?;
                let elements = self.parse_array_elements()?;
                self.leave_depth();
                self.expect(TokenKind::RBracket)?;
                Ok(Expr::Array(elements))
            }

            _ => Err(format!(
                "unexpected token {} at position {}",
                self.current().kind,
                self.current().pos
            )),
        }
    }

    /// Parse array elements: (expr (, expr)*)?
    fn parse_array_elements(&mut self) -> Result<Vec<Expr>, String> {
        let mut elements = Vec::new();

        if self.matches(&[TokenKind::RBracket]) {
            return Ok(elements);
        }

        elements.push(self.parse_expr()?);

        while self.matches(&[TokenKind::Comma]) {
            self.advance();
            elements.push(self.parse_expr()?);
        }

        Ok(elements)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_literal_true() {
        let expr = parse("true").unwrap();
        match expr {
            Expr::Literal(LiteralValue::Bool(true)) => {}
            _ => panic!("expected true literal"),
        }
    }

    #[test]
    fn test_parse_literal_null() {
        let expr = parse("null").unwrap();
        match expr {
            Expr::Literal(LiteralValue::Null) => {}
            _ => panic!("expected null literal"),
        }
    }

    #[test]
    fn test_parse_literal_number() {
        let expr = parse("42").unwrap();
        match expr {
            Expr::Literal(LiteralValue::Number(n)) => assert_eq!(n, 42.0),
            _ => panic!("expected number literal"),
        }
    }

    #[test]
    fn test_parse_literal_string() {
        let expr = parse("'hello'").unwrap();
        match expr {
            Expr::Literal(LiteralValue::String(s)) => assert_eq!(s, "hello"),
            _ => panic!("expected string literal"),
        }
    }

    #[test]
    fn test_parse_ident() {
        let expr = parse("auth").unwrap();
        match expr {
            Expr::Ident(name) => assert_eq!(name, "auth"),
            _ => panic!("expected identifier"),
        }
    }

    #[test]
    fn test_parse_wildcard() {
        let expr = parse("$userId").unwrap();
        match expr {
            Expr::Ident(name) => assert_eq!(name, "$userId"),
            _ => panic!("expected identifier"),
        }
    }

    #[test]
    fn test_parse_member_access() {
        let expr = parse("auth.uid").unwrap();
        match expr {
            Expr::Member { object, property } => {
                match *object {
                    Expr::Ident(name) => assert_eq!(name, "auth"),
                    _ => panic!("expected identifier"),
                }
                assert_eq!(property, "uid");
            }
            _ => panic!("expected member expression"),
        }
    }

    #[test]
    fn test_parse_method_call() {
        let expr = parse("data.val()").unwrap();
        match expr {
            Expr::Call { callee, args } => {
                assert!(args.is_empty());
                match *callee {
                    Expr::Member { property, .. } => assert_eq!(property, "val"),
                    _ => panic!("expected member expression"),
                }
            }
            _ => panic!("expected call expression"),
        }
    }

    #[test]
    fn test_parse_method_call_with_args() {
        let expr = parse("data.child('foo')").unwrap();
        match expr {
            Expr::Call { callee, args } => {
                assert_eq!(args.len(), 1);
                match &args[0] {
                    Expr::Literal(LiteralValue::String(s)) => assert_eq!(s, "foo"),
                    _ => panic!("expected string argument"),
                }
                match *callee {
                    Expr::Member { property, .. } => assert_eq!(property, "child"),
                    _ => panic!("expected member expression"),
                }
            }
            _ => panic!("expected call expression"),
        }
    }

    #[test]
    fn test_parse_binary_strict_eq() {
        let expr = parse("a === b").unwrap();
        match expr {
            Expr::Binary { op, .. } => assert_eq!(op, BinaryOp::StrictEq),
            _ => panic!("expected binary expression"),
        }
    }

    #[test]
    fn test_parse_binary_and() {
        let expr = parse("a && b").unwrap();
        match expr {
            Expr::Binary { op, .. } => assert_eq!(op, BinaryOp::And),
            _ => panic!("expected binary expression"),
        }
    }

    #[test]
    fn test_parse_unary_not() {
        let expr = parse("!a").unwrap();
        match expr {
            Expr::Unary { op, .. } => assert_eq!(op, UnaryOp::Not),
            _ => panic!("expected unary expression"),
        }
    }

    #[test]
    fn test_parse_ternary() {
        let expr = parse("a ? b : c").unwrap();
        match expr {
            Expr::Ternary { .. } => {}
            _ => panic!("expected ternary expression"),
        }
    }

    #[test]
    fn test_parse_array() {
        let expr = parse("['a', 'b']").unwrap();
        match expr {
            Expr::Array(elements) => assert_eq!(elements.len(), 2),
            _ => panic!("expected array expression"),
        }
    }

    #[test]
    fn test_parse_complex_expression() {
        // auth.uid !== null && data.child('owner').val() === auth.uid
        let expr = parse("auth.uid !== null && data.child('owner').val() === auth.uid").unwrap();
        match expr {
            Expr::Binary {
                op: BinaryOp::And, ..
            } => {}
            _ => panic!("expected && at top level"),
        }
    }

    #[test]
    fn test_parse_nested_method_calls() {
        let expr = parse("root.child('users').child($uid).val()").unwrap();
        match expr {
            Expr::Call { .. } => {}
            _ => panic!("expected call expression"),
        }
    }

    #[test]
    fn test_parse_parentheses() {
        let expr = parse("(a || b) && c").unwrap();
        match expr {
            Expr::Binary {
                op: BinaryOp::And,
                left,
                ..
            } => match *left {
                Expr::Binary {
                    op: BinaryOp::Or, ..
                } => {}
                _ => panic!("expected || in parentheses"),
            },
            _ => panic!("expected && at top level"),
        }
    }

    #[test]
    fn test_parse_depth_limit_parentheses() {
        // Create deeply nested parentheses exceeding MAX_AST_DEPTH
        let mut expr = "a".to_string();
        for _ in 0..MAX_AST_DEPTH + 1 {
            expr = format!("({})", expr);
        }
        let result = parse(&expr);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("too complex"));
    }

    #[test]
    fn test_parse_depth_limit_unary() {
        // Create deeply nested unary operators
        let mut expr = "a".to_string();
        for _ in 0..MAX_AST_DEPTH + 1 {
            expr = format!("!{}", expr);
        }
        let result = parse(&expr);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("too complex"));
    }

    #[test]
    fn test_parse_depth_at_limit() {
        // Create nesting exactly at MAX_AST_DEPTH - should succeed
        let mut expr = "a".to_string();
        for _ in 0..MAX_AST_DEPTH {
            expr = format!("({})", expr);
        }
        let result = parse(&expr);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_depth_limit_method_chain() {
        // Method chains like data.child('a').child('b')... don't increase depth
        // (they're parsed iteratively in parse_postfix)
        // But nested method args do increase depth
        let mut expr = "x".to_string();
        for _ in 0..MAX_AST_DEPTH + 1 {
            expr = format!("f({})", expr);
        }
        let result = parse(&expr);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("too complex"));
    }

    #[test]
    fn test_parse_node_count_limit() {
        // Create an expression with more nodes than MAX_AST_NODES
        // Each "true && " adds 2 nodes (Binary + Literal), plus the final "true"
        // So we need (MAX_AST_NODES / 2) + 1 "true" literals
        let count = (MAX_AST_NODES / 2) + 10;
        let expr_str = vec!["true"; count].join(" && ");
        let result = parse(&expr_str);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("too complex"), "error was: {}", err);
        assert!(err.contains("nodes"), "error was: {}", err);
    }

    #[test]
    fn test_parse_node_count_normal() {
        // Normal expressions should be well under the limit
        let expr = parse("auth.uid === $userId && data.exists() && newData.val() !== null");
        assert!(expr.is_ok());
        // This expression has about 15 nodes - well under 1000
        assert!(expr.unwrap().count_nodes() < 50);
    }

    #[test]
    fn test_parse_utf8_after_number_dot() {
        // Regression test: fuzzer found crash with "5.Ҹ" (number, dot, cyrillic char)
        // Should return an error, not crash
        let input = "5.\u{04B8}"; // "5.Ҹ"
        let result = parse(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_non_ascii_identifier_rejected() {
        // Non-ASCII identifiers should be rejected (only ASCII alphanumeric + _ + $ allowed)
        // But non-ASCII in string literals is fine: data.child('日本語') works
        let result = parse("αβγ");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_string_with_utf8() {
        // UTF-8 characters in string literals should work
        let result = parse("'日本語'");
        assert!(result.is_ok());
        match result.unwrap() {
            Expr::Literal(LiteralValue::String(s)) => assert_eq!(s, "日本語"),
            _ => panic!("expected string literal"),
        }
    }

    #[test]
    fn test_parse_string_with_escapes_and_utf8() {
        // Regression test: fuzzer found crash with escape sequences + UTF-8
        // Input: '\<ESC>$!ue=\unllפ'
        let input = "'\x1b$!ue=\\unll\u{05E4}'";
        let result = parse(input);
        // Should either parse successfully or return an error, but not crash
        let _ = result;
    }

    #[test]
    fn test_parse_string_backslash_utf8() {
        // Regression test: fuzzer found crash with backslash + multi-byte UTF-8
        // Input: '\ن'' (backslash followed by Arabic noon)
        let input = "'\\ن''";
        let result = parse(input);
        // Should parse - the \ن is an unknown escape, treated literally
        let _ = result;
    }
}
