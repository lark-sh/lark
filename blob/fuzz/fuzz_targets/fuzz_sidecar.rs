#![no_main]

use libfuzzer_sys::fuzz_target;
use lark_blob::segment::Sidecar;
use xxhash_rust::xxh64::xxh64;

fuzz_target!(|data: &[u8]| {
    // Property: Sidecar::from_bytes should never panic, always return Ok or Err,
    // on arbitrary (possibly truncated or corrupt) sidecar bytes. The region
    // count and per-key lengths are read straight from the input, so this
    // exercises the length/offset arithmetic on the deserialization path.
    let _ = Sidecar::from_bytes(data);

    // Most inputs stop at the checksum, so also seal the input with a valid one
    // to reach the parser behind it.
    let mut sealed = data.to_vec();
    if sealed.len() >= 24 {
        let main_len = (sealed.len() + 8) as u64;
        sealed[16..24].copy_from_slice(&main_len.to_le_bytes());
    }
    let checksum = xxh64(&sealed, 0);
    sealed.extend_from_slice(&checksum.to_le_bytes());
    let _ = Sidecar::from_bytes(&sealed);
});
