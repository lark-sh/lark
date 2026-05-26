#![no_main]

use libfuzzer_sys::fuzz_target;
use lark_server::transport::FirebaseAdapter;

fuzz_target!(|data: &[u8]| {
    // Create a fresh adapter for each fuzz run
    let mut adapter = FirebaseAdapter::new("test-project", "localhost");

    // Property: handle_incoming_frame should never panic, always return Ok or Err
    // Feed the data as a single frame
    let _ = adapter.handle_incoming_frame(data);

    // Also test translate_incoming directly (for already-reassembled messages)
    let _ = adapter.translate_incoming(data);
});
