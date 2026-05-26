#![no_main]

use libfuzzer_sys::fuzz_target;
use lark_server::db::{validate_path, Path};

fuzz_target!(|data: &[u8]| {
    // Try to parse as UTF-8 string, skip invalid UTF-8
    if let Ok(s) = std::str::from_utf8(data) {
        // Property: Path::parse and validate_path should never panic
        let path = Path::parse(s);
        let _ = path.len();
        let _ = path.is_root();
        let _ = path.parent();

        // validate_path does the actual validation with error checking
        let _ = validate_path(s);
    }
});
