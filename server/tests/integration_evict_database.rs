//! Integration tests for database-level eviction (EVICT_DATABASE).
//!
//! Covers the rename-on-purge behaviour wired up in
//! `CoreHandler::handle_evict_database`. The test server's `evict_database`
//! helper mirrors that production path.

mod common;

use common::{TestServer, run_test};
use serde_json::json;
use std::path::Path;
use std::time::Duration;
use tempfile::TempDir;

// WAL flush interval is 2 seconds; wait a bit longer so data is actually
// on disk before we evict.
const WAL_FLUSH_WAIT: Duration = Duration::from_millis(2500);

#[test]
fn test_evict_with_purge_renames_data_dir() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "evict-proj",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("evict-proj/room-1").await;
        client.set("/data/key", "value").await.unwrap();

        // Make sure the WAL actually hits disk so the dir has real contents.
        glommio::timer::sleep(WAL_FLUSH_WAIT).await;

        let db_path = Path::new(data_dir).join("evict-proj").join("room-1");
        assert!(db_path.exists(), "db dir should exist after writes");

        // Evict with purge.
        let renamed = server
            .evict_database("evict-proj", "room-1", true)
            .expect("purge should have renamed the dir");

        // Original path is gone.
        assert!(!db_path.exists(), "original path must be gone after rename");

        // Renamed path follows the `{db_path}-deleted-{ts}` pattern and exists.
        let renamed_str = renamed.to_string_lossy();
        let expected_prefix = db_path.to_string_lossy().to_string() + "-deleted-";
        assert!(
            renamed_str.starts_with(&expected_prefix),
            "renamed path {renamed_str} should start with {expected_prefix}"
        );
        assert!(renamed.exists(), "renamed path must exist on disk");
    });
}

#[test]
fn test_evict_without_purge_leaves_data_dir() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "evict-proj",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("evict-proj/keep-me").await;
        client.set("/x", 1).await.unwrap();
        glommio::timer::sleep(WAL_FLUSH_WAIT).await;

        let db_path = Path::new(data_dir).join("evict-proj").join("keep-me");
        assert!(db_path.exists());

        // Evict without purge — dir must be untouched.
        let renamed = server.evict_database("evict-proj", "keep-me", false);
        assert!(renamed.is_none(), "no rename when purge=false");
        assert!(db_path.exists(), "data dir preserved when purge=false");
    });
}

#[test]
fn test_evict_with_purge_when_never_loaded_still_renames_orphan() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        // Seed an orphan directory directly on disk — simulates a DB whose
        // files are present but that was never loaded on this core.
        let orphan = Path::new(data_dir).join("evict-proj").join("orphan-db");
        std::fs::create_dir_all(&orphan).unwrap();
        std::fs::write(orphan.join("blob.lark"), b"garbage").unwrap();

        let server = TestServer::with_persistence(data_dir);

        let renamed = server
            .evict_database("evict-proj", "orphan-db", true)
            .expect("purge must still rename orphan dir even if never loaded");

        assert!(!orphan.exists());
        assert!(renamed.exists());
        assert!(
            renamed.join("blob.lark").exists(),
            "contents survive rename"
        );
    });
}

#[test]
fn test_evict_is_idempotent_and_tolerates_missing_dir() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let server = TestServer::with_persistence(data_dir);

        // Dir doesn't exist, DB never loaded → purge is a no-op (not an error).
        let first = server.evict_database("nobody", "nothing", true);
        assert!(first.is_none());

        // Repeated eviction is equally fine.
        let second = server.evict_database("nobody", "nothing", true);
        assert!(second.is_none());

        // And without purge.
        let third = server.evict_database("nobody", "nothing", false);
        assert!(third.is_none());
    });
}
