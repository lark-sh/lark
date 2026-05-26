#![no_main]

use libfuzzer_sys::fuzz_target;
use lark_server::rules::expr::parse;

fuzz_target!(|data: &[u8]| {
    // Try to parse as UTF-8 string, skip invalid UTF-8
    if let Ok(s) = std::str::from_utf8(data) {
        // Property: parse() should never panic, always return Ok or Err
        let _ = parse(s);
    }
});
