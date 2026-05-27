#![no_main]

use libfuzzer_sys::fuzz_target;
use lark_blob::segment::Sidecar;

fuzz_target!(|data: &[u8]| {
    // Property: Sidecar::from_bytes should never panic, always return Ok or Err,
    // on arbitrary (possibly truncated or corrupt) sidecar bytes. The region
    // count and per-key lengths are read straight from the input, so this
    // exercises the length/offset arithmetic on the deserialization path.
    let _ = Sidecar::from_bytes(data);
});
