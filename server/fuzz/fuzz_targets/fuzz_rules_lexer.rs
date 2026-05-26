#![no_main]

use libfuzzer_sys::fuzz_target;
use lark_server::rules::expr::{Lexer, TokenKind};

fuzz_target!(|data: &[u8]| {
    // Try to parse as UTF-8 string, skip invalid UTF-8
    if let Ok(s) = std::str::from_utf8(data) {
        // Property: Lexer should never panic, always return Ok or Err
        let mut lexer = Lexer::new(s);
        loop {
            match lexer.next_token() {
                Ok(token) => {
                    if token.kind == TokenKind::Eof {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    }
});
