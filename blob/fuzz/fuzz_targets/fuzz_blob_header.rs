#![no_main]

use libfuzzer_sys::fuzz_target;
use lark_blob::format::{BlobHeader, HEADER_SIZE};

fuzz_target!(|data: &[u8]| {
    // BlobHeader::from_bytes wants a fixed HEADER_SIZE-byte array; feed it the
    // first HEADER_SIZE bytes whenever the input is long enough.
    // Property: parsing arbitrary header bytes should never panic.
    if data.len() >= HEADER_SIZE {
        let buf: &[u8; HEADER_SIZE] = data[..HEADER_SIZE].try_into().unwrap();
        let _ = BlobHeader::from_bytes(buf);
    }
});
