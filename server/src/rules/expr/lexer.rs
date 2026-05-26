//! Lexer for rule expressions.

use super::token::{Token, TokenKind};

/// Lexer tokenizes a rule expression string.
pub struct Lexer<'a> {
    src: &'a str,
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Lexer<'a> {
    /// Create a new lexer for the given source string.
    pub fn new(src: &'a str) -> Self {
        Self {
            src,
            bytes: src.as_bytes(),
            pos: 0,
        }
    }

    /// Tokenize the entire source and return all tokens.
    pub fn tokenize(src: &str) -> Result<Vec<Token>, String> {
        let mut lexer = Lexer::new(src);
        let mut tokens = Vec::new();
        loop {
            // Before calling next_token, check if we're at a `/` that should be a regex literal.
            // A `/` is a regex literal when preceded by an operator or opening punctuation
            // (expression context). It's division when preceded by a value-like token.
            lexer.skip_whitespace();
            if lexer.pos < lexer.bytes.len() && lexer.bytes[lexer.pos] == b'/' {
                let is_regex = match tokens.last().map(|t: &Token| t.kind) {
                    // Division follows value-like tokens
                    Some(
                        TokenKind::Ident
                        | TokenKind::Number
                        | TokenKind::String
                        | TokenKind::True
                        | TokenKind::False
                        | TokenKind::Null
                        | TokenKind::RParen
                        | TokenKind::RBracket,
                    ) => false,
                    // Regex follows everything else (operators, `(`, `,`, start of input)
                    _ => true,
                };

                if is_regex {
                    let start_pos = lexer.pos;
                    let (pattern, end_pos) = Self::scan_regex_literal(src, start_pos)?;
                    lexer.pos = end_pos;
                    tokens.push(Token::with_value(TokenKind::String, pattern, start_pos));
                    continue;
                }
            }

            let tok = lexer.next_token()?;
            let is_eof = tok.kind == TokenKind::Eof;
            tokens.push(tok);
            if is_eof {
                break;
            }
        }
        Ok(tokens)
    }

    /// Scan a regex literal starting at the opening `/`.
    /// Returns (pattern_string, end_position_after_flags).
    /// If the `i` flag is present, prepends `(?i)` to the pattern.
    fn scan_regex_literal(src: &str, start: usize) -> Result<(String, usize), String> {
        let bytes = src.as_bytes();
        let mut pos = start + 1; // skip opening /
        let mut pattern = String::new();

        while pos < bytes.len() {
            let ch = bytes[pos];
            if ch == b'/' {
                pos += 1; // skip closing /
                // Check for flags
                let mut case_insensitive = false;
                while pos < bytes.len() && bytes[pos].is_ascii_alphabetic() {
                    if bytes[pos] == b'i' {
                        case_insensitive = true;
                    }
                    // Ignore unknown flags
                    pos += 1;
                }
                if case_insensitive {
                    return Ok((format!("(?i){}", pattern), pos));
                }
                return Ok((pattern, pos));
            }
            if ch == b'\\' && pos + 1 < bytes.len() {
                let next = bytes[pos + 1];
                if next == b'/' {
                    // \/ in regex literal = literal /
                    pattern.push('/');
                    pos += 2;
                } else {
                    // Pass other escapes through to the regex engine (e.g., \d, \s, \\)
                    pattern.push('\\');
                    pos += 1;
                    if next < 0x80 {
                        pattern.push(next as char);
                        pos += 1;
                    } else {
                        // Multi-byte UTF-8 after backslash
                        let mut end = pos + 1;
                        while end < bytes.len() && !src.is_char_boundary(end) {
                            end += 1;
                        }
                        pattern.push_str(&src[pos..end]);
                        pos = end;
                    }
                }
            } else if ch < 0x80 {
                pattern.push(ch as char);
                pos += 1;
            } else {
                // Multi-byte UTF-8
                let mut end = pos + 1;
                while end < bytes.len() && !src.is_char_boundary(end) {
                    end += 1;
                }
                pattern.push_str(&src[pos..end]);
                pos = end;
            }
        }

        Err(format!("unterminated regex literal at position {}", start))
    }

    /// Get the next token.
    pub fn next_token(&mut self) -> Result<Token, String> {
        self.skip_whitespace();

        if self.pos >= self.bytes.len() {
            return Ok(Token::new(TokenKind::Eof, self.pos));
        }

        let start_pos = self.pos;
        let ch = self.bytes[self.pos];

        // Identifiers and keywords
        if is_ident_start(ch) {
            return self.scan_ident();
        }

        // Numbers
        if is_digit(ch) || (ch == b'.' && self.peek_next().is_some_and(is_digit)) {
            return self.scan_number();
        }

        // Strings
        if ch == b'"' || ch == b'\'' {
            return self.scan_string(ch);
        }

        // Multi-character operators (3 chars)
        // Only slice if end position is a valid UTF-8 char boundary
        if self.pos + 3 <= self.bytes.len() && self.src.is_char_boundary(self.pos + 3) {
            let three = &self.src[self.pos..self.pos + 3];
            match three {
                "===" => {
                    self.pos += 3;
                    return Ok(Token::new(TokenKind::EqEqEq, start_pos));
                }
                "!==" => {
                    self.pos += 3;
                    return Ok(Token::new(TokenKind::NeqEqEq, start_pos));
                }
                _ => {}
            }
        }

        // Multi-character operators (2 chars)
        // Only slice if end position is a valid UTF-8 char boundary
        if self.pos + 2 <= self.bytes.len() && self.src.is_char_boundary(self.pos + 2) {
            let two = &self.src[self.pos..self.pos + 2];
            match two {
                "==" => {
                    self.pos += 2;
                    return Ok(Token::new(TokenKind::EqEq, start_pos));
                }
                "!=" => {
                    self.pos += 2;
                    return Ok(Token::new(TokenKind::Neq, start_pos));
                }
                "&&" => {
                    self.pos += 2;
                    return Ok(Token::new(TokenKind::And, start_pos));
                }
                "||" => {
                    self.pos += 2;
                    return Ok(Token::new(TokenKind::Or, start_pos));
                }
                ">=" => {
                    self.pos += 2;
                    return Ok(Token::new(TokenKind::Gte, start_pos));
                }
                "<=" => {
                    self.pos += 2;
                    return Ok(Token::new(TokenKind::Lte, start_pos));
                }
                _ => {}
            }
        }

        // Single-character tokens
        let kind = match ch {
            b'(' => TokenKind::LParen,
            b')' => TokenKind::RParen,
            b'[' => TokenKind::LBracket,
            b']' => TokenKind::RBracket,
            b'.' => TokenKind::Dot,
            b',' => TokenKind::Comma,
            b'?' => TokenKind::Question,
            b':' => TokenKind::Colon,
            b'!' => TokenKind::Not,
            b'>' => TokenKind::Gt,
            b'<' => TokenKind::Lt,
            b'+' => TokenKind::Plus,
            b'-' => TokenKind::Minus,
            b'*' => TokenKind::Star,
            b'/' => TokenKind::Slash,
            b'%' => TokenKind::Percent,
            _ => {
                // Handle non-ASCII characters: find the full UTF-8 character for error message
                // and advance past it properly
                let char_start = self.pos;
                // Find the next char boundary to get the full character
                let mut end = self.pos + 1;
                while end < self.bytes.len() && !self.src.is_char_boundary(end) {
                    end += 1;
                }
                self.pos = end;
                let bad_char = &self.src[char_start..end];
                return Err(format!(
                    "unexpected character {:?} at position {}",
                    bad_char, start_pos
                ));
            }
        };

        self.pos += 1;
        Ok(Token::new(kind, start_pos))
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn peek_next(&self) -> Option<u8> {
        if self.pos + 1 < self.bytes.len() {
            Some(self.bytes[self.pos + 1])
        } else {
            None
        }
    }

    fn scan_ident(&mut self) -> Result<Token, String> {
        let start_pos = self.pos;
        while self.pos < self.bytes.len() && is_ident_char(self.bytes[self.pos]) {
            self.pos += 1;
        }
        let value = &self.src[start_pos..self.pos];

        // Check for keywords
        let kind = match value {
            "true" => TokenKind::True,
            "false" => TokenKind::False,
            "null" => TokenKind::Null,
            _ => TokenKind::Ident,
        };

        Ok(Token::with_value(kind, value.to_string(), start_pos))
    }

    fn scan_number(&mut self) -> Result<Token, String> {
        let start_pos = self.pos;
        let mut has_decimal = false;

        while self.pos < self.bytes.len() {
            let ch = self.bytes[self.pos];
            if is_digit(ch) {
                self.pos += 1;
            } else if ch == b'.' && !has_decimal {
                // Check it's not a method call like 123.toString()
                if self.peek_next().is_some_and(is_digit) {
                    has_decimal = true;
                    self.pos += 1;
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        let num_str = &self.src[start_pos..self.pos];
        let num: f64 = num_str
            .parse()
            .map_err(|_| format!("invalid number {:?} at position {}", num_str, start_pos))?;

        Ok(Token::with_number(num, start_pos))
    }

    fn scan_string(&mut self, quote: u8) -> Result<Token, String> {
        let start_pos = self.pos;
        self.pos += 1; // skip opening quote

        let mut result = String::new();
        while self.pos < self.bytes.len() {
            let ch = self.bytes[self.pos];
            if ch == quote {
                self.pos += 1; // skip closing quote
                return Ok(Token::with_value(TokenKind::String, result, start_pos));
            }
            if ch == b'\\' && self.pos + 1 < self.bytes.len() {
                self.pos += 1;
                let escaped = self.bytes[self.pos];
                match escaped {
                    b'n' => {
                        result.push('\n');
                        self.pos += 1;
                    }
                    b't' => {
                        result.push('\t');
                        self.pos += 1;
                    }
                    b'r' => {
                        result.push('\r');
                        self.pos += 1;
                    }
                    b'\\' => {
                        result.push('\\');
                        self.pos += 1;
                    }
                    b'"' => {
                        result.push('"');
                        self.pos += 1;
                    }
                    b'\'' => {
                        result.push('\'');
                        self.pos += 1;
                    }
                    b'u' => {
                        // Unicode escape: \uXXXX
                        // Check we have 4 more bytes AND they're valid char boundaries
                        let end_pos = self.pos + 5;
                        if end_pos <= self.bytes.len()
                            && self.src.is_char_boundary(self.pos + 1)
                            && self.src.is_char_boundary(end_pos)
                        {
                            let hex = &self.src[self.pos + 1..end_pos];
                            if let Ok(code) = u32::from_str_radix(hex, 16) {
                                if let Some(c) = char::from_u32(code) {
                                    result.push(c);
                                    self.pos += 4;
                                } else {
                                    result.push(escaped as char);
                                }
                            } else {
                                result.push(escaped as char);
                            }
                        } else {
                            result.push(escaped as char);
                        }
                        self.pos += 1;
                    }
                    _ => {
                        // Unknown escape - include the escaped character literally
                        // Handle multi-byte UTF-8 characters properly
                        if escaped < 0x80 {
                            result.push(escaped as char);
                            self.pos += 1;
                        } else {
                            // Multi-byte UTF-8 character after backslash
                            let char_start = self.pos;
                            let mut end = self.pos + 1;
                            while end < self.bytes.len() && !self.src.is_char_boundary(end) {
                                end += 1;
                            }
                            let char_str = &self.src[char_start..end];
                            result.push_str(char_str);
                            self.pos = end;
                        }
                    }
                }
            } else if ch < 0x80 {
                // ASCII character - handle directly
                result.push(ch as char);
                self.pos += 1;
            } else {
                // Multi-byte UTF-8 character - find the full character
                let char_start = self.pos;
                let mut end = self.pos + 1;
                while end < self.bytes.len() && !self.src.is_char_boundary(end) {
                    end += 1;
                }
                // Extract the full UTF-8 character
                let char_str = &self.src[char_start..end];
                result.push_str(char_str);
                self.pos = end;
            }
        }

        Err(format!("unterminated string at position {}", start_pos))
    }
}

fn is_ident_start(ch: u8) -> bool {
    ch.is_ascii_alphabetic() || ch == b'_' || ch == b'$'
}

fn is_ident_char(ch: u8) -> bool {
    is_ident_start(ch) || ch.is_ascii_digit()
}

fn is_digit(ch: u8) -> bool {
    ch.is_ascii_digit()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_simple_ident() {
        let tokens = Lexer::tokenize("auth").unwrap();
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].kind, TokenKind::Ident);
        assert_eq!(tokens[0].value, "auth");
        assert_eq!(tokens[1].kind, TokenKind::Eof);
    }

    #[test]
    fn test_tokenize_member_access() {
        let tokens = Lexer::tokenize("auth.uid").unwrap();
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens[0].kind, TokenKind::Ident);
        assert_eq!(tokens[1].kind, TokenKind::Dot);
        assert_eq!(tokens[2].kind, TokenKind::Ident);
        assert_eq!(tokens[2].value, "uid");
    }

    #[test]
    fn test_tokenize_operators() {
        let tokens = Lexer::tokenize("a === b").unwrap();
        assert_eq!(tokens[1].kind, TokenKind::EqEqEq);

        let tokens = Lexer::tokenize("a !== b").unwrap();
        assert_eq!(tokens[1].kind, TokenKind::NeqEqEq);

        let tokens = Lexer::tokenize("a && b").unwrap();
        assert_eq!(tokens[1].kind, TokenKind::And);

        let tokens = Lexer::tokenize("a || b").unwrap();
        assert_eq!(tokens[1].kind, TokenKind::Or);
    }

    #[test]
    fn test_tokenize_string() {
        let tokens = Lexer::tokenize("\"hello world\"").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::String);
        assert_eq!(tokens[0].value, "hello world");

        let tokens = Lexer::tokenize("'single quotes'").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::String);
        assert_eq!(tokens[0].value, "single quotes");
    }

    #[test]
    #[allow(clippy::approx_constant)] // "3.14" is lexer test input, not PI
    fn test_tokenize_number() {
        let tokens = Lexer::tokenize("42").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::Number);
        assert_eq!(tokens[0].num_val, 42.0);

        let tokens = Lexer::tokenize("3.14").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::Number);
        assert_eq!(tokens[0].num_val, 3.14);
    }

    #[test]
    fn test_tokenize_keywords() {
        let tokens = Lexer::tokenize("true false null").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::True);
        assert_eq!(tokens[1].kind, TokenKind::False);
        assert_eq!(tokens[2].kind, TokenKind::Null);
    }

    #[test]
    fn test_tokenize_wildcard() {
        let tokens = Lexer::tokenize("$userId").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::Ident);
        assert_eq!(tokens[0].value, "$userId");
    }

    #[test]
    fn test_tokenize_method_call() {
        let tokens = Lexer::tokenize("data.child('foo').val()").unwrap();
        // data . child ( 'foo' ) . val ( ) EOF = 11 tokens
        assert_eq!(tokens.len(), 11);
        assert_eq!(tokens[0].value, "data");
        assert_eq!(tokens[2].value, "child");
        assert_eq!(tokens[4].value, "foo");
        assert_eq!(tokens[7].value, "val");
    }

    #[test]
    fn test_tokenize_complex_expression() {
        let tokens =
            Lexer::tokenize("auth.uid !== null && data.child('owner').val() === auth.uid").unwrap();
        assert!(tokens.len() > 10);
    }

    #[test]
    fn test_tokenize_escape_sequences() {
        let tokens = Lexer::tokenize(r#""hello\nworld""#).unwrap();
        assert_eq!(tokens[0].value, "hello\nworld");

        let tokens = Lexer::tokenize(r#""tab\there""#).unwrap();
        assert_eq!(tokens[0].value, "tab\there");
    }

    #[test]
    fn test_tokenize_ternary() {
        let tokens = Lexer::tokenize("a ? b : c").unwrap();
        assert_eq!(tokens[1].kind, TokenKind::Question);
        assert_eq!(tokens[3].kind, TokenKind::Colon);
    }

    #[test]
    fn test_tokenize_array() {
        let tokens = Lexer::tokenize("['a', 'b', 'c']").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::LBracket);
        assert_eq!(tokens[tokens.len() - 2].kind, TokenKind::RBracket);
    }

    // =========================================================================
    // Regex Literal Tests
    // =========================================================================

    #[test]
    fn test_regex_literal_basic() {
        // matches(/^foo/) → the /^foo/ becomes a String token with value "^foo"
        let tokens = Lexer::tokenize("x.matches(/^foo/)").unwrap();
        // x . matches ( "^foo" ) EOF
        let regex_tok = tokens.iter().find(|t| t.kind == TokenKind::String).unwrap();
        assert_eq!(regex_tok.value, "^foo");
    }

    #[test]
    fn test_regex_literal_case_insensitive() {
        let tokens = Lexer::tokenize("x.matches(/^[A-Z]+$/i)").unwrap();
        let regex_tok = tokens.iter().find(|t| t.kind == TokenKind::String).unwrap();
        assert_eq!(regex_tok.value, "(?i)^[A-Z]+$");
    }

    #[test]
    fn test_regex_literal_escaped_slash() {
        // \/ in regex literal = literal /
        let tokens = Lexer::tokenize(r"x.matches(/https?:\/\//)").unwrap();
        let regex_tok = tokens.iter().find(|t| t.kind == TokenKind::String).unwrap();
        assert_eq!(regex_tok.value, "https?://");
    }

    #[test]
    fn test_regex_literal_with_backslash_sequences() {
        let tokens = Lexer::tokenize(r"x.matches(/^\d+\.\d+$/)").unwrap();
        let regex_tok = tokens.iter().find(|t| t.kind == TokenKind::String).unwrap();
        assert_eq!(regex_tok.value, r"^\d+\.\d+$");
    }

    #[test]
    fn test_regex_literal_firebase_date_example() {
        // Firebase docs example: YYYY-MM-DD validation
        let tokens = Lexer::tokenize(
            r"x.matches(/^(19|20)[0-9][0-9][-\/. ](0[1-9]|1[012])[-\/. ](0[1-9]|[12][0-9]|3[01])$/)"
        ).unwrap();
        let regex_tok = tokens.iter().find(|t| t.kind == TokenKind::String).unwrap();
        assert_eq!(
            regex_tok.value,
            "^(19|20)[0-9][0-9][-/. ](0[1-9]|1[012])[-/. ](0[1-9]|[12][0-9]|3[01])$"
        );
    }

    #[test]
    fn test_regex_literal_firebase_email_example() {
        // Firebase docs example: email validation (case insensitive)
        let tokens =
            Lexer::tokenize(r"x.matches(/^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}$/i)").unwrap();
        let regex_tok = tokens.iter().find(|t| t.kind == TokenKind::String).unwrap();
        assert_eq!(
            regex_tok.value,
            r"(?i)^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}$"
        );
    }

    #[test]
    fn test_slash_as_division_after_value() {
        // After a number or ident, / is division, not regex
        let tokens = Lexer::tokenize("a / b").unwrap();
        assert_eq!(tokens[1].kind, TokenKind::Slash);

        let tokens = Lexer::tokenize("10 / 2").unwrap();
        assert_eq!(tokens[1].kind, TokenKind::Slash);
    }

    #[test]
    fn test_unterminated_regex_literal() {
        let result = Lexer::tokenize("x.matches(/^foo)");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unterminated regex"));
    }
}
