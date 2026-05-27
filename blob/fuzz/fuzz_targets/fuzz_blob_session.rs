#![no_main]

use libfuzzer_sys::fuzz_target;
use lark_blob::arc_value::ArcValue;
use lark_blob::io::MemBlobIO;
use lark_blob::session::BlobSession;
use lark_blob::writer::write_blob;
use std::sync::OnceLock;

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    futures::executor::block_on(f)
}

/// A valid blob, built once. It contains nested collections and arrays of mixed
/// leaf types so that corrupting it can drive every branch of the node reader
/// (collection header/child-index, array header, and leaf parsing).
fn base_blob() -> &'static [u8] {
    static BASE: OnceLock<Vec<u8>> = OnceLock::new();
    BASE.get_or_init(|| {
        let tree = ArcValue::from_value(serde_json::json!({
            "users": {
                "alice": { "age": 30, "name": "Alice", "admin": true },
                "bob": { "age": 25, "name": "Bob", "tags": ["x", "y", "z"] }
            },
            "rooms": [
                { "id": 1, "members": ["alice", "bob"] },
                { "id": 2, "members": [] }
            ],
            "counters": [0, 1, 2, 3, 4, 5],
            "flag": false,
            "title": "fuzz seed"
        }));
        let io = MemBlobIO::new();
        block_on(write_blob(&io, &tree)).expect("seed blob must serialize");
        io.data().to_vec()
    })
}

fuzz_target!(|data: &[u8]| {
    // Property: reading a corrupted blob must never panic — only Ok or Err.
    //
    // Random bytes almost never parse as a blob header, so feeding raw `data`
    // would only exercise `open()`'s early rejection. Instead we corrupt a known
    // valid blob: interpret `data` as a stream of (u32 offset, u8 value) patches
    // and apply each. Single-byte patches keep the surrounding structure intact,
    // so corruption of a length/count field inside a node header still reaches
    // the node reader (where the count drives slicing and Vec allocation).
    let mut bytes = base_blob().to_vec();
    for chunk in data.chunks_exact(5) {
        let off = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]) as usize % bytes.len();
        bytes[off] = chunk[4];
    }

    let _ = block_on(async {
        let session = BlobSession::open(MemBlobIO::from_bytes(bytes)).await?;
        // Drive the read paths: a full deep read plus shallow/key reads at a few
        // points, so collection, array, and leaf decoders all run.
        let _ = session.read_subtree(&[]).await;
        let _ = session.read_keys(&[]).await;
        let _ = session.read_shallow(&["users"]).await;
        let _ = session.read_subtree(&["rooms"]).await;
        let _ = session.read_subtree(&["users", "bob"]).await;
        Ok::<(), lark_blob::error::BlobError>(())
    });
});
