//! OnDisconnect integration tests.
//!
//! Tests for disconnect hooks (onDisconnect set/remove/cancel).

mod common;

use common::{TestServer, run_test};
use lark_blob::{ArcValue, StdBlobIO, write_blob};
use serde_json::{Value, json};
use std::time::Duration;
use tempfile::TempDir;

/// Helper: write a blob file at the expected path for a given project/database.
fn write_test_blob(data_dir: &str, project: &str, db_name: &str, tree: &ArcValue) {
    let db_dir = format!("{}/{}/{}", data_dir, project, db_name);
    std::fs::create_dir_all(&db_dir).unwrap();
    let blob_path = format!("{}/blob.lark", db_dir);
    futures::executor::block_on(async {
        let io = StdBlobIO::create(std::path::Path::new(&blob_path)).unwrap();
        write_blob(&io, tree).await.unwrap();
    });
}

// =============================================================================
// OnDisconnect Tests
// =============================================================================

#[test]
fn test_on_disconnect_remove() {
    run_test(|| async {
        let server = TestServer::new();

        // Client 1 will set data and register disconnect hook
        let mut client1 = server.client();
        client1.connect("ondisconnect-db").await;

        // Set player data
        client1
            .set("/players/abc", json!({"name": "Alice"}))
            .await
            .expect("Failed to set");

        // Register disconnect hook to remove player
        client1
            .on_disconnect_remove("/players/abc")
            .await
            .expect("Failed to register ondisconnect");

        // Verify player exists
        let value = client1.once("/players/abc").await.expect("Failed to once");
        assert!(
            value != Value::Null,
            "Player should exist before disconnect"
        );

        // Disconnect
        client1.disconnect().await;

        // Give server time to process disconnect
        glommio::timer::sleep(Duration::from_millis(100)).await;

        // Connect new client to verify data was removed
        let mut client2 = server.client();
        client2.connect("ondisconnect-db").await;

        // Player should be gone
        let value = client2.once("/players/abc").await.expect("Failed to once");
        assert_eq!(
            value,
            Value::Null,
            "Player should have been removed on disconnect"
        );
    });
}

#[test]
fn test_on_disconnect_set() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client1 = server.client();
        client1.connect("ondisconnect-set-db").await;

        // Set player as online
        client1
            .set("/players/abc/status", "online")
            .await
            .expect("Failed to set");

        // Register disconnect hook to set status to offline
        client1
            .on_disconnect_set("/players/abc/status", "offline")
            .await
            .expect("Failed to register ondisconnect");

        // Disconnect
        client1.disconnect().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;

        // Verify status changed
        let mut client2 = server.client();
        client2.connect("ondisconnect-set-db").await;

        let value = client2
            .once("/players/abc/status")
            .await
            .expect("Failed to once");
        assert_eq!(value, json!("offline"));
    });
}

#[test]
fn test_on_disconnect_notifies_subscribers() {
    run_test(|| async {
        let server = TestServer::new();

        // Client 1: will disconnect
        let mut client1 = server.client();
        // Client 2: subscriber
        let mut client2 = server.client();

        client1.connect("ondisconnect-events-db").await;
        client2.connect("ondisconnect-events-db").await;

        // Client 1 sets their player data
        client1
            .set("/players/abc", json!({"name": "Alice"}))
            .await
            .expect("Failed to set");

        // Client 1 registers disconnect hook
        client1
            .on_disconnect_remove("/players/abc")
            .await
            .expect("Failed to register ondisconnect");

        // Client 2 subscribes to players
        client2
            .subscribe("/players", &["value"])
            .await
            .expect("Failed to subscribe");

        // Consume initial put event
        let _ = client2
            .wait_for_event(Duration::from_secs(2))
            .await
            .expect("Failed to receive initial event");

        // Clear any pending events
        client2.clear_events().await;

        // Client 1 disconnects
        client1.disconnect().await;

        // Client 2 should receive a put event about the removal
        let event = client2
            .wait_for_event(Duration::from_secs(2))
            .await
            .expect("Failed to receive disconnect event");

        // Should be a put event
        assert_eq!(event.event.as_deref(), Some("put"));
    });
}

#[test]
fn test_on_disconnect_cancel() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client1 = server.client();
        client1.connect("ondisconnect-cancel-db").await;

        // Set player data
        client1
            .set("/players/abc", json!({"name": "Alice"}))
            .await
            .expect("Failed to set");

        // Register disconnect hook
        client1
            .on_disconnect_remove("/players/abc")
            .await
            .expect("Failed to register ondisconnect");

        // Cancel the hook
        client1
            .on_disconnect_cancel("/players/abc")
            .await
            .expect("Failed to cancel ondisconnect");

        // Disconnect
        client1.disconnect().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;

        // Verify player still exists (hook was cancelled)
        let mut client2 = server.client();
        client2.connect("ondisconnect-cancel-db").await;

        let value = client2.once("/players/abc").await.expect("Failed to once");
        assert!(
            value != Value::Null,
            "Player should still exist after cancelled ondisconnect"
        );
    });
}

// =============================================================================
// Additional OnDisconnect Tests
// =============================================================================

#[test]
fn test_on_disconnect_update() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client1 = server.client();
        client1.connect("ondisconnect-update-db").await;

        // Set player data
        client1
            .set(
                "/players/abc",
                json!({"name": "Alice", "status": "online", "score": 100}),
            )
            .await
            .expect("Failed to set");

        // Register disconnect hook to update status only
        client1
            .on_disconnect_update("/players/abc", json!({"status": "offline"}))
            .await
            .expect("Failed to register ondisconnect update");

        // Disconnect
        client1.disconnect().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;

        // Verify only status changed, name and score preserved
        let mut client2 = server.client();
        client2.connect("ondisconnect-update-db").await;

        let value = client2.once("/players/abc").await.expect("Failed to once");

        let obj = value.as_object().expect("Expected object");
        assert_eq!(obj.get("name"), Some(&json!("Alice")));
        assert_eq!(obj.get("status"), Some(&json!("offline")));
        assert_eq!(obj.get("score"), Some(&json!(100)));
    });
}

#[test]
fn test_multiple_on_disconnect_hooks() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client1 = server.client();
        client1.connect("multi-ondisconnect-db").await;

        // Set multiple pieces of data
        client1
            .set("/players/abc", json!({"name": "Alice"}))
            .await
            .expect("Failed to set player");
        client1
            .set("/presence/abc", "online")
            .await
            .expect("Failed to set presence");

        // Register multiple disconnect hooks
        client1
            .on_disconnect_remove("/players/abc")
            .await
            .expect("Failed to register hook 1");
        client1
            .on_disconnect_set("/presence/abc", "offline")
            .await
            .expect("Failed to register hook 2");

        // Disconnect
        client1.disconnect().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;

        // Verify both hooks executed
        let mut client2 = server.client();
        client2.connect("multi-ondisconnect-db").await;

        let player = client2
            .once("/players/abc")
            .await
            .expect("Failed to once player");
        assert_eq!(player, Value::Null, "Player should be removed");

        let presence = client2
            .once("/presence/abc")
            .await
            .expect("Failed to once presence");
        assert_eq!(presence, json!("offline"), "Presence should be offline");
    });
}

// Regression: handle_disconnect's UPDATE arm uses tree.update unconditionally,
// which on a blob-backed Sentinel-rooted tree creates empty Object intermediates
// that lie about being fully loaded. A subsequent once() short-circuits promotion
// and returns only what the onDisconnect UPDATE wrote.
#[test]
fn test_on_disconnect_update_blob_backed_preserves_existing_fields() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        // Blob has the player with all 3 fields.
        let tree = ArcValue::from_value(json!({
            "players": {
                "abc": {"name": "Alice", "status": "online", "score": 100}
            }
        }));
        write_test_blob(
            data_dir,
            "test-project",
            "ondisconnect-blob-update-db",
            &tree,
        );

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        // Client1: register onDisconnect UPDATE that flips status, then disconnect.
        let mut client1 = server.client();
        client1
            .connect("test-project/ondisconnect-blob-update-db")
            .await;
        client1
            .on_disconnect_update("/players/abc", json!({"status": "offline"}))
            .await
            .expect("register ondisconnect update");
        client1.disconnect().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;

        // Client2: read back. Must see all 3 fields with status flipped.
        let mut client2 = server.client();
        client2
            .connect("test-project/ondisconnect-blob-update-db")
            .await;
        let value = client2
            .once("/players/abc")
            .await
            .expect("once /players/abc");
        let obj = value.as_object().expect("expected object");
        assert_eq!(obj.get("name"), Some(&json!("Alice")), "name from blob");
        assert_eq!(obj.get("score"), Some(&json!(100)), "score from blob");
        assert_eq!(
            obj.get("status"),
            Some(&json!("offline")),
            "status from onDisconnect UPDATE"
        );
    });
}
