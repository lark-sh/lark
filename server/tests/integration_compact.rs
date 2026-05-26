//! End-to-end tests for the `lark-compact` binary.
//!
//! lark-compact is a one-shot blob compactor:
//!
//!   lark-compact <database-dir>
//!
//! These tests build small blob fixtures, run the binary against them, and
//! verify the resulting tree + generation counter.

use lark_blob::ArcValue;
use lark_blob::incremental::apply_updates;
use lark_blob::io::StdBlobIO;
use lark_blob::session::BlobSession;
use lark_blob::writer::write_blob;

use serde_json::json;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::TempDir;

/// Minimal single-poll block_on for sync-returning async fns (StdBlobIO).
fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    fn noop_raw_waker() -> RawWaker {
        fn no_op(_: *const ()) {}
        fn clone(_: *const ()) -> RawWaker {
            noop_raw_waker()
        }
        RawWaker::new(
            std::ptr::null(),
            &RawWakerVTable::new(clone, no_op, no_op, no_op),
        )
    }

    let waker = unsafe { Waker::from_raw(noop_raw_waker()) };
    let mut cx = Context::from_waker(&waker);
    let mut fut = std::pin::pin!(fut);
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(val) => val,
        Poll::Pending => panic!("block_on: unexpected Pending from sync BlobIO"),
    }
}

/// Find the `lark-compact` binary built by cargo. It lives next to the test
/// binary in `target/debug/`. The caller (`make test-all`) is expected to
/// have built it; otherwise this assert fires with a hint.
fn compact_binary() -> PathBuf {
    let mut path = std::env::current_exe()
        .expect("current_exe")
        .parent()
        .expect("parent of test binary")
        .parent()
        .expect("parent of deps dir")
        .to_path_buf();
    path.push("lark-compact");
    assert!(
        path.exists(),
        "lark-compact binary not found at {}. Build it first: `cargo build -p lark-compact`.",
        path.display()
    );
    path
}

/// Write a blob file at `{data_dir}/{project}/{database}/blob.lark`.
fn write_test_blob(data_dir: &Path, project: &str, database: &str, tree: &ArcValue) {
    let db_dir = data_dir.join(project).join(database);
    std::fs::create_dir_all(&db_dir).unwrap();
    let blob_path = db_dir.join("blob.lark");

    block_on(async {
        let io = StdBlobIO::create(&blob_path).unwrap();
        write_blob(&io, tree).await.unwrap();
    });
}

/// Apply incremental updates to the blob at `{data_dir}/{project}/{database}/blob.lark`.
fn apply_test_updates(
    data_dir: &Path,
    project: &str,
    database: &str,
    updates: &[(Vec<String>, Option<ArcValue>)],
) {
    let blob_path = data_dir.join(project).join(database).join("blob.lark");

    block_on(async {
        let io = StdBlobIO::open(&blob_path).unwrap();
        apply_updates(&io, updates).await.unwrap();
    });
}

/// Check if blob.lark exists and return (generation, "blob.lark") if so.
fn list_blobs(data_dir: &Path, project: &str, database: &str) -> Vec<(u64, String)> {
    let db_dir = data_dir.join(project).join(database);
    let bp = db_dir.join("blob.lark");
    if bp.exists() {
        let generation = match std::fs::read_to_string(db_dir.join("blob.generation")) {
            Ok(s) => s.trim().parse::<u64>().unwrap_or(0),
            Err(_) => 0,
        };
        vec![(generation, "blob.lark".to_string())]
    } else {
        vec![]
    }
}

/// Read the full tree from a blob file and return it as ArcValue.
fn read_full_tree(blob_path: &Path) -> ArcValue {
    block_on(async {
        let io = StdBlobIO::open(blob_path).unwrap();
        let session = BlobSession::open(io).await.unwrap();
        session.read_subtree(&[]).await.unwrap()
    })
}

/// Read a value at a specific path from a blob file.
fn read_blob_path(blob_path: &Path, path: &[&str]) -> ArcValue {
    block_on(async {
        let io = StdBlobIO::open(blob_path).unwrap();
        let session = BlobSession::open(io).await.unwrap();
        session.read_subtree(path).await.unwrap()
    })
}

/// Run `lark-compact <db-dir>`.
fn run_compact(db_dir: &Path) -> std::process::Output {
    Command::new(compact_binary())
        .arg(db_dir.to_str().unwrap())
        .output()
        .expect("failed to execute lark-compact")
}

// =============================================================================
// Tests
// =============================================================================

#[test]
fn test_compact_clean_blob() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path();

    let tree = ArcValue::from_value(json!({
        "users": {
            "alice": {"name": "Alice", "score": 100},
            "bob": {"name": "Bob", "score": 200}
        },
        "config": {"mode": "dark"}
    }));
    write_test_blob(data_dir, "myproject", "mydb", &tree);

    let db_dir = data_dir.join("myproject").join("mydb");
    let output = run_compact(&db_dir);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "lark-compact failed: {}", stderr);

    let blobs = list_blobs(data_dir, "myproject", "mydb");
    assert_eq!(blobs.len(), 1, "expected exactly 1 blob, got: {:?}", blobs);
    assert_eq!(blobs[0].0, 1, "expected generation 1");

    let new_blob = db_dir.join("blob.lark");
    let result = read_full_tree(&new_blob);
    assert_eq!(result, tree);
}

#[test]
fn test_compact_dirty_blob() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path();

    let tree = ArcValue::from_value(json!({
        "users": {
            "alice": {"name": "Alice", "score": 100},
            "bob": {"name": "Bob", "score": 200}
        }
    }));
    write_test_blob(data_dir, "proj", "db1", &tree);

    // Apply updates: change Alice's score, add a new user
    let updates = vec![
        (
            vec!["users".into(), "alice".into(), "score".into()],
            Some(ArcValue::from_value(json!(999))),
        ),
        (
            vec!["users".into(), "charlie".into()],
            Some(ArcValue::from_value(
                json!({"name": "Charlie", "score": 300}),
            )),
        ),
    ];
    apply_test_updates(data_dir, "proj", "db1", &updates);

    let db_dir = data_dir.join("proj").join("db1");
    let output = run_compact(&db_dir);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "lark-compact failed: {}", stderr);

    let new_blob = db_dir.join("blob.lark");
    assert!(new_blob.exists(), "blob.lark should exist");

    let alice_score = read_blob_path(&new_blob, &["users", "alice", "score"]);
    assert_eq!(
        alice_score,
        ArcValue::from_value(json!(999)),
        "alice's score should be updated"
    );

    let charlie = read_blob_path(&new_blob, &["users", "charlie", "name"]);
    assert_eq!(charlie, ArcValue::from("Charlie"), "charlie should exist");

    let bob = read_blob_path(&new_blob, &["users", "bob", "name"]);
    assert_eq!(bob, ArcValue::from("Bob"), "bob should still exist");
}

#[test]
fn test_compact_missing_dir() {
    let temp = TempDir::new().unwrap();
    let db_dir = temp.path().join("nonexistent").join("db");

    let output = run_compact(&db_dir);
    assert!(!output.status.success(), "should fail for missing dir");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("does not exist"),
        "should report missing dir: {}",
        stderr
    );
}

#[test]
fn test_compact_no_blob_in_database() {
    let temp = TempDir::new().unwrap();
    let db_dir = temp.path().join("proj").join("empty_db");
    std::fs::create_dir_all(&db_dir).unwrap();

    let output = run_compact(&db_dir);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "should succeed: {}", stderr);
    assert!(
        stderr.contains("skipped"),
        "should report skipped: {}",
        stderr
    );
}

#[test]
fn test_compact_with_deletes() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path();

    let tree = ArcValue::from_value(json!({
        "users": {
            "alice": {"name": "Alice"},
            "bob": {"name": "Bob"},
            "charlie": {"name": "Charlie"}
        }
    }));
    write_test_blob(data_dir, "proj", "db1", &tree);

    // Delete bob
    let updates = vec![(vec!["users".into(), "bob".into()], None::<ArcValue>)];
    apply_test_updates(data_dir, "proj", "db1", &updates);

    let db_dir = data_dir.join("proj").join("db1");
    let output = run_compact(&db_dir);
    assert!(output.status.success(), "lark-compact failed");

    let new_blob = db_dir.join("blob.lark");
    let result = read_full_tree(&new_blob);

    assert!(result.get("users").unwrap().get("alice").is_some());
    assert!(result.get("users").unwrap().get("charlie").is_some());
    assert!(
        result.get("users").unwrap().get("bob").is_none(),
        "bob should be deleted after compaction"
    );
}

#[test]
fn test_no_args_shows_usage() {
    let output = Command::new(compact_binary())
        .output()
        .expect("failed to execute lark-compact");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "should fail without args");
    assert!(stderr.contains("Usage:"), "should show usage: {}", stderr);
}
