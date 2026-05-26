#![no_main]

use libfuzzer_sys::fuzz_target;
use lark_server::transport::{
    HelloMessage, HelloAckMessage, HeartbeatMessage, DatabaseLoadedMessage,
    DatabaseUnloadedMessage, ConfigPushMessage, ConfigRequestMessage, ShutdownMessage,
};

fuzz_target!(|data: &[u8]| {
    // Property: All decode functions should never panic, always return Some or None

    // Test all the proxy protocol message decoders
    let _ = HelloMessage::decode(data);
    let _ = HelloAckMessage::decode(data);
    let _ = HeartbeatMessage::decode(data);
    let _ = DatabaseLoadedMessage::decode(data);
    let _ = DatabaseUnloadedMessage::decode(data);
    let _ = ConfigPushMessage::decode(data);
    let _ = ConfigRequestMessage::decode(data);
    let _ = ShutdownMessage::decode(data);
});
