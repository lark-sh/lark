//! Persistence tests.
//!
//! These test WAL, disk persistence, and data recovery:
//! - Full persistence cycle (write → flush → restart → verify)
//! - WAL replay on load
//! - Graceful shutdown flushes WAL
//! - Deletions are properly persisted
//! - Nested updates persist correctly
//! - Multiple writes to same path coalesce correctly
//! - Ephemeral databases don't persist

mod common;

use common::{TestServer, TransactionOp, run_test};
use serde_json::json;
use std::path::Path;
use std::time::Duration;
use tempfile::TempDir;

// WAL flush interval is 2 seconds, so we wait 2.5 seconds to ensure flush
const WAL_FLUSH_WAIT: Duration = Duration::from_millis(2500);

// =============================================================================
// WAL Replay Tests
// =============================================================================

#[test]
fn test_persistence_wal_replay() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        // Create server with persistence
        let server = TestServer::with_persistence(data_dir);

        // Configure project as non-ephemeral
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/replay-db").await;

        // Write data that will be in WAL
        client.set("/data/key1", "value1").await.unwrap();
        client.set("/data/key2", "value2").await.unwrap();
        client.set("/data/key3", "value3").await.unwrap();

        // Wait for WAL flush
        glommio::timer::sleep(WAL_FLUSH_WAIT).await;

        // Verify WAL directory was created
        let wal_dir = Path::new(data_dir)
            .join("test-project")
            .join("replay-db")
            .join("wal");
        assert!(wal_dir.exists(), "WAL directory should exist");

        // Disconnect client and shutdown server
        client.disconnect().await;
        server.shutdown().await;

        // Small delay to ensure shutdown completes
        glommio::timer::sleep(Duration::from_millis(100)).await;

        // Restart server with same data directory
        let server2 = TestServer::restart_with_persistence(data_dir);
        server2
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client2 = server2.client();
        client2.connect("test-project/replay-db").await;

        // Give time for database to load from WAL
        glommio::timer::sleep(Duration::from_millis(500)).await;

        // Read data back - should be recovered from WAL
        let data = client2.once("/data").await.unwrap();
        let data_map = data.as_object().expect("expected map");

        // Verify all three keys were recovered from WAL
        assert_eq!(
            data_map.get("key1"),
            Some(&json!("value1")),
            "key1 should be recovered"
        );
        assert_eq!(
            data_map.get("key2"),
            Some(&json!("value2")),
            "key2 should be recovered"
        );
        assert_eq!(
            data_map.get("key3"),
            Some(&json!("value3")),
            "key3 should be recovered"
        );

        client2.disconnect().await;
        server2.shutdown().await;
    });
}

#[test]
fn test_persistence_graceful_shutdown_flushes_wal() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/graceful-db").await;

        // Write data that would normally be in dirty tracker (not yet flushed)
        client.set("/data1", "first write").await.unwrap();
        client.set("/data2", "second write").await.unwrap();

        // Don't wait for flush - immediately do graceful shutdown
        // The graceful shutdown should flush the WAL
        glommio::timer::sleep(Duration::from_millis(100)).await;

        // Graceful shutdown
        client.disconnect().await;
        server.shutdown().await;
        // Wait for database to notice handles dropped and flush WAL
        glommio::timer::sleep(Duration::from_millis(100)).await;

        // Restart server
        let server2 = TestServer::restart_with_persistence(data_dir);
        server2
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client2 = server2.client();
        client2.connect("test-project/graceful-db").await;
        glommio::timer::sleep(Duration::from_millis(500)).await;

        // Both writes should be recovered (graceful shutdown flushes WAL)
        let data1 = client2.once("/data1").await.unwrap();
        assert_eq!(
            data1,
            json!("first write"),
            "data1 should be recovered after graceful shutdown"
        );

        let data2 = client2.once("/data2").await.unwrap();
        assert_eq!(
            data2,
            json!("second write"),
            "data2 should be recovered after graceful shutdown"
        );

        client2.disconnect().await;
        server2.shutdown().await;
    });
}

// =============================================================================
// Deletion Persistence Tests
// =============================================================================

#[test]
fn test_persistence_deletion() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/delete-db").await;

        // Write then delete
        client.set("/to-delete", "temporary").await.unwrap();
        client.set("/to-keep", "permanent").await.unwrap();

        // Wait for WAL flush
        glommio::timer::sleep(WAL_FLUSH_WAIT).await;

        // Delete
        client.remove("/to-delete").await.unwrap();

        // Wait for second flush
        glommio::timer::sleep(WAL_FLUSH_WAIT).await;

        // Restart
        client.disconnect().await;
        server.shutdown().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;

        let server2 = TestServer::restart_with_persistence(data_dir);
        server2
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client2 = server2.client();
        client2.connect("test-project/delete-db").await;
        glommio::timer::sleep(Duration::from_millis(500)).await;

        // Deleted data should not exist
        let deleted_data = client2.once("/to-delete").await.unwrap();
        assert_eq!(
            deleted_data,
            serde_json::Value::Null,
            "deleted data should not exist"
        );

        // Kept data should exist
        let kept_data = client2.once("/to-keep").await.unwrap();
        assert_eq!(kept_data, json!("permanent"), "kept data should exist");

        client2.disconnect().await;
        server2.shutdown().await;
    });
}

// =============================================================================
// Nested Update Tests
// =============================================================================

#[test]
fn test_persistence_nested_update() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/nested-db").await;

        // Create nested structure
        client.set("/user/profile/name", "John").await.unwrap();
        client.set("/user/profile/age", 30).await.unwrap();
        client.set("/user/settings/theme", "dark").await.unwrap();

        // Wait for WAL flush
        glommio::timer::sleep(WAL_FLUSH_WAIT).await;

        // Restart
        client.disconnect().await;
        server.shutdown().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;

        let server2 = TestServer::restart_with_persistence(data_dir);
        server2
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client2 = server2.client();
        client2.connect("test-project/nested-db").await;
        glommio::timer::sleep(Duration::from_millis(500)).await;

        // Verify nested structure
        let user_data = client2.once("/user").await.unwrap();
        let user_map = user_data.as_object().expect("expected map");

        let profile = user_map
            .get("profile")
            .and_then(|v| v.as_object())
            .expect("expected profile map");
        assert_eq!(
            profile.get("name"),
            Some(&json!("John")),
            "name should be John"
        );
        assert_eq!(profile.get("age"), Some(&json!(30)), "age should be 30");

        let settings = user_map
            .get("settings")
            .and_then(|v| v.as_object())
            .expect("expected settings map");
        assert_eq!(
            settings.get("theme"),
            Some(&json!("dark")),
            "theme should be dark"
        );

        client2.disconnect().await;
        server2.shutdown().await;
    });
}

// =============================================================================
// Multiple Writes Coalescing Tests
// =============================================================================

#[test]
fn test_persistence_multiple_writes() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/coalesce-db").await;

        // Write to the same path many times rapidly
        for i in 0..100 {
            client.set("/counter", i).await.unwrap();
        }

        // Wait for WAL flush
        glommio::timer::sleep(WAL_FLUSH_WAIT).await;

        // Close and restart to verify final value was persisted
        client.disconnect().await;
        server.shutdown().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;

        let server2 = TestServer::restart_with_persistence(data_dir);
        server2
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client2 = server2.client();
        client2.connect("test-project/coalesce-db").await;
        glommio::timer::sleep(Duration::from_millis(500)).await;

        let val = client2.once("/counter").await.unwrap();

        // Should have the final value (99)
        assert_eq!(val, json!(99), "expected final value 99, got {:?}", val);

        client2.disconnect().await;
        server2.shutdown().await;
    });
}

// =============================================================================
// Ephemeral Database Tests
// =============================================================================

#[test]
fn test_persistence_ephemeral_not_persisted() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let server = TestServer::with_persistence(data_dir);

        // Configure project as EPHEMERAL
        server
            .set_rules_with_ephemeral(
                "ephemeral-project",
                json!({"rules": {".read": true, ".write": true}}),
                true,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("ephemeral-project/ephemeral-db").await;

        // Write data
        client.set("/data", "should not persist").await.unwrap();

        // Wait for what would be WAL flush time
        glommio::timer::sleep(WAL_FLUSH_WAIT).await;

        // Verify NO files were created for ephemeral database
        let db_dir = Path::new(data_dir)
            .join("ephemeral-project")
            .join("ephemeral-db");
        assert!(
            !db_dir.exists(),
            "ephemeral database should not create persistence files"
        );

        client.disconnect().await;
        server.shutdown().await;
    });
}

// =============================================================================
// Full Persistence Cycle Tests
// =============================================================================

#[test]
fn test_persistence_full_cycle() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        // Create server with persistence
        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/test-db").await;

        // Write some data
        client
            .set("/users/alice", json!({"name": "Alice", "score": 100}))
            .await
            .unwrap();
        client
            .set("/users/bob", json!({"name": "Bob", "score": 200}))
            .await
            .unwrap();
        client.set("/config/version", 1).await.unwrap();

        // Wait for WAL flush
        glommio::timer::sleep(WAL_FLUSH_WAIT).await;

        // Verify WAL files were created
        let wal_dir = Path::new(data_dir)
            .join("test-project")
            .join("test-db")
            .join("wal");
        assert!(wal_dir.exists(), "WAL directory should be created");

        let wal_files: Vec<_> = std::fs::read_dir(&wal_dir)
            .map(|rd| rd.filter_map(|e| e.ok()).collect())
            .unwrap_or_default();
        assert!(!wal_files.is_empty(), "expected WAL files to be created");

        // Disconnect client and shutdown server
        client.disconnect().await;
        server.shutdown().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;

        // Start new server with same data directory
        let server2 = TestServer::restart_with_persistence(data_dir);
        server2
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client2 = server2.client();
        client2.connect("test-project/test-db").await;

        // Give time for database to load
        glommio::timer::sleep(Duration::from_millis(500)).await;

        // Read data back
        let alice_data = client2.once("/users/alice").await.unwrap();
        let alice_map = alice_data.as_object().expect("expected map");
        assert_eq!(
            alice_map.get("name"),
            Some(&json!("Alice")),
            "alice name should be recovered"
        );
        assert_eq!(
            alice_map.get("score"),
            Some(&json!(100)),
            "alice score should be recovered"
        );

        let bob_data = client2.once("/users/bob").await.unwrap();
        let bob_map = bob_data.as_object().expect("expected map");
        assert_eq!(
            bob_map.get("name"),
            Some(&json!("Bob")),
            "bob name should be recovered"
        );
        assert_eq!(
            bob_map.get("score"),
            Some(&json!(200)),
            "bob score should be recovered"
        );

        let config_version = client2.once("/config/version").await.unwrap();
        assert_eq!(
            config_version,
            json!(1),
            "config version should be recovered"
        );

        client2.disconnect().await;
        server2.shutdown().await;
    });
}

// =============================================================================
// UPDATE Persistence Tests
// =============================================================================

#[test]
fn test_persistence_update_shallow_merge() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/update-db").await;

        // SET initial data, then UPDATE a subset of fields
        client
            .set(
                "/users/alice",
                json!({"name": "Alice", "score": 100, "level": 1}),
            )
            .await
            .unwrap();
        client
            .update("/users/alice", json!({"score": 150}))
            .await
            .unwrap();

        glommio::timer::sleep(WAL_FLUSH_WAIT).await;

        client.disconnect().await;
        server.shutdown().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;

        let server2 = TestServer::restart_with_persistence(data_dir);
        server2
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client2 = server2.client();
        client2.connect("test-project/update-db").await;
        glommio::timer::sleep(Duration::from_millis(500)).await;

        let alice = client2.once("/users/alice").await.unwrap();
        assert_eq!(alice["name"], "Alice", "name should be preserved");
        assert_eq!(alice["level"], 1, "level should be preserved");
        assert_eq!(alice["score"], 150, "score should be updated");

        client2.disconnect().await;
        server2.shutdown().await;
    });
}

#[test]
fn test_persistence_set_then_update() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/set-update-db").await;

        client
            .set("/user", json!({"name": "Alice", "age": 30}))
            .await
            .unwrap();
        client
            .update("/user", json!({"age": 31, "city": "NYC"}))
            .await
            .unwrap();

        glommio::timer::sleep(WAL_FLUSH_WAIT).await;

        client.disconnect().await;
        server.shutdown().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;

        let server2 = TestServer::restart_with_persistence(data_dir);
        server2
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client2 = server2.client();
        client2.connect("test-project/set-update-db").await;
        glommio::timer::sleep(Duration::from_millis(500)).await;

        let user = client2.once("/user").await.unwrap();
        assert_eq!(user["name"], "Alice", "name should be preserved");
        assert_eq!(user["age"], 31, "age should be updated");
        assert_eq!(user["city"], "NYC", "city should be added");

        client2.disconnect().await;
        server2.shutdown().await;
    });
}

#[test]
fn test_persistence_multiple_updates_same_path() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/multi-update-db").await;

        client
            .set("/stats", json!({"a": 1, "b": 2, "c": 3}))
            .await
            .unwrap();
        client.update("/stats", json!({"a": 10})).await.unwrap();
        client.update("/stats", json!({"b": 20})).await.unwrap();
        client
            .update("/stats", json!({"c": 30, "d": 40}))
            .await
            .unwrap();

        glommio::timer::sleep(WAL_FLUSH_WAIT).await;

        client.disconnect().await;
        server.shutdown().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;

        let server2 = TestServer::restart_with_persistence(data_dir);
        server2
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client2 = server2.client();
        client2.connect("test-project/multi-update-db").await;
        glommio::timer::sleep(Duration::from_millis(500)).await;

        let stats = client2.once("/stats").await.unwrap();
        assert_eq!(stats["a"], 10);
        assert_eq!(stats["b"], 20);
        assert_eq!(stats["c"], 30);
        assert_eq!(stats["d"], 40);

        client2.disconnect().await;
        server2.shutdown().await;
    });
}

#[test]
fn test_persistence_update_then_set_replaces() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/update-set-db").await;

        client
            .set("/user", json!({"name": "Alice", "age": 30}))
            .await
            .unwrap();
        client
            .update("/user", json!({"city": "NYC"}))
            .await
            .unwrap();
        // SET replaces everything
        client.set("/user", json!({"name": "Bob"})).await.unwrap();

        glommio::timer::sleep(WAL_FLUSH_WAIT).await;

        client.disconnect().await;
        server.shutdown().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;

        let server2 = TestServer::restart_with_persistence(data_dir);
        server2
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client2 = server2.client();
        client2.connect("test-project/update-set-db").await;
        glommio::timer::sleep(Duration::from_millis(500)).await;

        let user = client2.once("/user").await.unwrap();
        assert_eq!(user["name"], "Bob", "name should be Bob");
        assert!(
            user.get("age").is_none() || user["age"].is_null(),
            "age should not exist (SET replaces)"
        );
        assert!(
            user.get("city").is_none() || user["city"].is_null(),
            "city should not exist (SET replaces)"
        );

        client2.disconnect().await;
        server2.shutdown().await;
    });
}

// =============================================================================
// DELETE + SET Ordering Tests
// =============================================================================

#[test]
fn test_persistence_delete_then_set() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/delete-set-db").await;

        client.set("/user", json!({"name": "Alice"})).await.unwrap();
        client.remove("/user").await.unwrap();
        client.set("/user", json!({"name": "Bob"})).await.unwrap();

        glommio::timer::sleep(WAL_FLUSH_WAIT).await;

        client.disconnect().await;
        server.shutdown().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;

        let server2 = TestServer::restart_with_persistence(data_dir);
        server2
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client2 = server2.client();
        client2.connect("test-project/delete-set-db").await;
        glommio::timer::sleep(Duration::from_millis(500)).await;

        let user = client2.once("/user").await.unwrap();
        assert_eq!(user["name"], "Bob", "name should be Bob after delete+set");

        client2.disconnect().await;
        server2.shutdown().await;
    });
}

#[test]
fn test_persistence_set_then_delete() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/set-delete-db").await;

        client.set("/user", json!({"name": "Alice"})).await.unwrap();
        client.remove("/user").await.unwrap();

        glommio::timer::sleep(WAL_FLUSH_WAIT).await;

        client.disconnect().await;
        server.shutdown().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;

        let server2 = TestServer::restart_with_persistence(data_dir);
        server2
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client2 = server2.client();
        client2.connect("test-project/set-delete-db").await;
        glommio::timer::sleep(Duration::from_millis(500)).await;

        let user = client2.once("/user").await.unwrap();
        assert_eq!(user, serde_json::Value::Null, "user should be deleted");

        client2.disconnect().await;
        server2.shutdown().await;
    });
}

#[test]
fn test_persistence_delete_nested_path() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/nested-delete-db").await;

        client
            .set(
                "/users/alice",
                json!({
                    "profile": {"name": "Alice", "email": "alice@example.com", "phone": "555-1234"}
                }),
            )
            .await
            .unwrap();
        client.remove("/users/alice/profile/email").await.unwrap();

        glommio::timer::sleep(WAL_FLUSH_WAIT).await;

        client.disconnect().await;
        server.shutdown().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;

        let server2 = TestServer::restart_with_persistence(data_dir);
        server2
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client2 = server2.client();
        client2.connect("test-project/nested-delete-db").await;
        glommio::timer::sleep(Duration::from_millis(500)).await;

        let profile = client2.once("/users/alice/profile").await.unwrap();
        assert_eq!(profile["name"], "Alice", "name should be preserved");
        assert!(
            profile.get("email").is_none() || profile["email"].is_null(),
            "email should be deleted"
        );
        assert_eq!(profile["phone"], "555-1234", "phone should be preserved");

        client2.disconnect().await;
        server2.shutdown().await;
    });
}

// =============================================================================
// Root Write Override Test
// =============================================================================

#[test]
fn test_persistence_root_write_overrides_all() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/root-write-db").await;

        // Set up some data, then overwrite root
        client
            .set("/users/alice", json!({"name": "Alice"}))
            .await
            .unwrap();
        client.set("/settings/theme", "dark").await.unwrap();
        client
            .set("/", json!({"newData": "fresh start"}))
            .await
            .unwrap();

        glommio::timer::sleep(WAL_FLUSH_WAIT).await;

        client.disconnect().await;
        server.shutdown().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;

        let server2 = TestServer::restart_with_persistence(data_dir);
        server2
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client2 = server2.client();
        client2.connect("test-project/root-write-db").await;
        glommio::timer::sleep(Duration::from_millis(500)).await;

        let users = client2.once("/users").await.unwrap();
        assert_eq!(
            users,
            serde_json::Value::Null,
            "old /users should be gone after root write"
        );

        let settings = client2.once("/settings").await.unwrap();
        assert_eq!(
            settings,
            serde_json::Value::Null,
            "old /settings should be gone after root write"
        );

        let new_data = client2.once("/newData").await.unwrap();
        assert_eq!(new_data, "fresh start", "new data should exist");

        client2.disconnect().await;
        server2.shutdown().await;
    });
}

// =============================================================================
// Transaction Persistence Tests
// =============================================================================

#[test]
fn test_persistence_transaction() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/transaction-db").await;

        // Set up initial data
        client.set("/counter", 5).await.unwrap();
        client
            .set("/users/alice", json!({"balance": 100}))
            .await
            .unwrap();

        // Transaction: set counter, update alice, add log entry
        client
            .transaction(vec![
                TransactionOp {
                    op: "s".to_string(),
                    path: "/counter".to_string(),
                    value: Some(json!(6)),
                    hash: None,
                },
                TransactionOp {
                    op: "u".to_string(),
                    path: "/users/alice".to_string(),
                    value: Some(json!({"balance": 150})),
                    hash: None,
                },
                TransactionOp {
                    op: "s".to_string(),
                    path: "/log/tx1".to_string(),
                    value: Some(json!("completed")),
                    hash: None,
                },
            ])
            .await
            .unwrap();

        glommio::timer::sleep(WAL_FLUSH_WAIT).await;

        client.disconnect().await;
        server.shutdown().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;

        let server2 = TestServer::restart_with_persistence(data_dir);
        server2
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client2 = server2.client();
        client2.connect("test-project/transaction-db").await;
        glommio::timer::sleep(Duration::from_millis(500)).await;

        let counter = client2.once("/counter").await.unwrap();
        assert_eq!(counter, 6, "counter should be 6");

        let balance = client2.once("/users/alice/balance").await.unwrap();
        assert_eq!(balance, 150, "alice balance should be 150");

        let log = client2.once("/log/tx1").await.unwrap();
        assert_eq!(log, "completed", "log entry should exist");

        client2.disconnect().await;
        server2.shutdown().await;
    });
}

#[test]
fn test_persistence_transaction_mixed_ops() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/mixed-tx-db").await;

        // Initial state
        client
            .set("/users/alice", json!({"name": "Alice", "balance": 100}))
            .await
            .unwrap();
        client
            .set("/users/bob", json!({"name": "Bob", "balance": 50}))
            .await
            .unwrap();
        client
            .set("/users/charlie", json!({"name": "Charlie"}))
            .await
            .unwrap();

        // Transaction: update alice, delete charlie, add dave
        client
            .transaction(vec![
                TransactionOp {
                    op: "u".to_string(),
                    path: "/users/alice".to_string(),
                    value: Some(json!({"balance": 150})),
                    hash: None,
                },
                TransactionOp {
                    op: "d".to_string(),
                    path: "/users/charlie".to_string(),
                    value: None,
                    hash: None,
                },
                TransactionOp {
                    op: "s".to_string(),
                    path: "/users/dave".to_string(),
                    value: Some(json!({"name": "Dave", "balance": 0})),
                    hash: None,
                },
            ])
            .await
            .unwrap();

        glommio::timer::sleep(WAL_FLUSH_WAIT).await;

        client.disconnect().await;
        server.shutdown().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;

        let server2 = TestServer::restart_with_persistence(data_dir);
        server2
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client2 = server2.client();
        client2.connect("test-project/mixed-tx-db").await;
        glommio::timer::sleep(Duration::from_millis(500)).await;

        let alice = client2.once("/users/alice").await.unwrap();
        assert_eq!(alice["balance"], 150, "Alice balance should be 150");
        assert_eq!(alice["name"], "Alice", "Alice name should be preserved");

        let bob = client2.once("/users/bob").await.unwrap();
        assert_eq!(bob["balance"], 50, "Bob should be untouched");

        let charlie = client2.once("/users/charlie").await.unwrap();
        assert_eq!(
            charlie,
            serde_json::Value::Null,
            "Charlie should be deleted"
        );

        let dave = client2.once("/users/dave").await.unwrap();
        assert_eq!(dave["name"], "Dave", "Dave should be added");

        client2.disconnect().await;
        server2.shutdown().await;
    });
}

// =============================================================================
// Server Value Persistence Tests
// =============================================================================

#[test]
fn test_persistence_server_value_timestamp() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let before = chrono::Utc::now().timestamp_millis();

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/timestamp-db").await;

        client
            .set_raw(
                "/post",
                json!({"title": "Hello", "createdAt": {".sv": "timestamp"}}),
            )
            .await
            .unwrap();

        let after = chrono::Utc::now().timestamp_millis();

        glommio::timer::sleep(WAL_FLUSH_WAIT).await;

        client.disconnect().await;
        server.shutdown().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;

        let server2 = TestServer::restart_with_persistence(data_dir);
        server2
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client2 = server2.client();
        client2.connect("test-project/timestamp-db").await;
        glommio::timer::sleep(Duration::from_millis(500)).await;

        let post = client2.once("/post").await.unwrap();
        assert_eq!(post["title"], "Hello");

        let created_at = post["createdAt"]
            .as_i64()
            .expect("createdAt should be a number");
        assert!(
            created_at >= before,
            "createdAt {} should be >= before {}",
            created_at,
            before
        );
        assert!(
            created_at <= after,
            "createdAt {} should be <= after {}",
            created_at,
            after
        );

        client2.disconnect().await;
        server2.shutdown().await;
    });
}

#[test]
fn test_persistence_server_value_increment() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/increment-db").await;

        client.set("/counter", 10).await.unwrap();
        client
            .set_raw("/counter", json!({".sv": {"increment": 5}}))
            .await
            .unwrap();

        glommio::timer::sleep(WAL_FLUSH_WAIT).await;

        client.disconnect().await;
        server.shutdown().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;

        let server2 = TestServer::restart_with_persistence(data_dir);
        server2
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client2 = server2.client();
        client2.connect("test-project/increment-db").await;
        glommio::timer::sleep(Duration::from_millis(500)).await;

        let counter = client2.once("/counter").await.unwrap();
        assert_eq!(counter, 15, "counter should be 15 (10+5)");

        client2.disconnect().await;
        server2.shutdown().await;
    });
}

// =============================================================================
// Multiple WAL Files at Startup Test
// =============================================================================

#[test]
fn test_persistence_multiple_wal_files_at_startup() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/multi-wal-db").await;

        // Write + flush several times to create multiple WAL files
        client
            .set("/phase1", json!({"data": "first"}))
            .await
            .unwrap();
        client.set("/counter", 1).await.unwrap();
        glommio::timer::sleep(WAL_FLUSH_WAIT).await;

        client
            .set("/phase2", json!({"data": "second"}))
            .await
            .unwrap();
        client.set("/counter", 2).await.unwrap();
        glommio::timer::sleep(WAL_FLUSH_WAIT).await;

        client
            .set("/phase3", json!({"data": "third"}))
            .await
            .unwrap();
        client.set("/counter", 3).await.unwrap();
        glommio::timer::sleep(WAL_FLUSH_WAIT).await;

        client.disconnect().await;
        server.shutdown().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;

        // Restart — should load and replay all WAL files
        let server2 = TestServer::restart_with_persistence(data_dir);
        server2
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client2 = server2.client();
        client2.connect("test-project/multi-wal-db").await;
        glommio::timer::sleep(Duration::from_millis(500)).await;

        // Data from all phases should be present
        let phase1 = client2.once("/phase1").await.unwrap();
        assert_eq!(phase1["data"], "first", "phase1 data should be recovered");

        let phase2 = client2.once("/phase2").await.unwrap();
        assert_eq!(phase2["data"], "second", "phase2 data should be recovered");

        let phase3 = client2.once("/phase3").await.unwrap();
        assert_eq!(phase3["data"], "third", "phase3 data should be recovered");

        // Counter should have the final value (last write wins across WAL files)
        let counter = client2.once("/counter").await.unwrap();
        assert_eq!(counter, 3, "counter should be 3 (from last write)");

        client2.disconnect().await;
        server2.shutdown().await;
    });
}
