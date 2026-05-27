#![no_main]

use libfuzzer_sys::fuzz_target;
use lark_blob::dictionary::Dictionary;

fuzz_target!(|data: &[u8]| {
    // Property: Dictionary::from_bytes should never panic, always return Ok or Err,
    // on arbitrary (possibly truncated or corrupt) dictionary bytes. field_count
    // and the per-name lengths are read from the input and drive both slicing and
    // Vec capacity, so this exercises the allocation-sizing path.
    let _ = Dictionary::from_bytes(data);
});
