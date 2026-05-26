//! Connection ID and write deduplication tests.
//!
//! These test connection IDs and write deduplication functionality.

mod common;

use common::{TestServer, run_test};
use serde_json::json;

// =============================================================================
// Connection ID Tests
// =============================================================================

#[test]
fn test_connection_id_exists() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("conn-id-db").await;

        // Connection ID should exist and be non-empty
        let conn_id = client.get_connection_id();
        assert!(conn_id.is_some(), "expected connection ID");
        let conn_id = conn_id.unwrap();
        assert!(!conn_id.is_empty(), "expected non-empty connection ID");

        // Connection ID should be a push ID format (starts with -)
        assert!(
            conn_id.starts_with('-'),
            "connection ID should be a push ID format (start with -), got {}",
            conn_id
        );
    });
}

// =============================================================================
// Write Deduplication Tests
// =============================================================================

#[test]
fn test_write_deduplication() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("dedup-db").await;

        // First write with a specific request ID
        client
            .set_with_request_id("/counter", 1, "req-123")
            .await
            .expect("failed to set");

        // Verify the value
        let val1 = client.once("/counter").await.expect("failed to read");
        assert_eq!(val1, json!(1));

        // Second write with same request ID should be deduplicated (value stays at 1)
        client
            .set_with_request_id("/counter", 2, "req-123")
            .await
            .expect("failed to set");

        // Value should still be 1 because the second write was skipped
        let val2 = client.once("/counter").await.expect("failed to read");
        assert_eq!(
            val2,
            json!(1),
            "expected counter=1 (deduplicated), got {:?}",
            val2
        );

        // Third write with different request ID should succeed
        client
            .set_with_request_id("/counter", 3, "req-456")
            .await
            .expect("failed to set");

        let val3 = client.once("/counter").await.expect("failed to read");
        assert_eq!(val3, json!(3), "expected counter=3, got {:?}", val3);
    });
}

#[test]
fn test_write_deduplication_different_paths() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("dedup-paths-db").await;

        // Write to path1 with request ID
        client
            .set_with_request_id("/path1", "value1", "req-abc")
            .await
            .expect("failed to set path1");

        // Try to write to path2 with the SAME request ID
        // This should also be deduplicated since request ID is the key
        client
            .set_with_request_id("/path2", "value2", "req-abc")
            .await
            .expect("failed to set path2");

        // path1 should have value1
        let val1 = client.once("/path1").await.expect("failed to read path1");
        assert_eq!(val1, json!("value1"));

        // path2 should NOT be written (deduplicated)
        let val2 = client.once("/path2").await.expect("failed to read path2");
        assert_eq!(
            val2,
            serde_json::Value::Null,
            "path2 should not exist (deduplicated)"
        );
    });
}

#[test]
fn test_write_deduplication_without_request_id() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("dedup-no-req-db").await;

        // Writes without request ID should NOT be deduplicated
        // (they're considered independent operations)
        client.set("/value", 1).await.expect("failed to set");
        let val1 = client.once("/value").await.expect("failed to read");
        assert_eq!(val1, json!(1));

        // Second write should succeed (not deduplicated)
        client.set("/value", 2).await.expect("failed to set");
        let val2 = client.once("/value").await.expect("failed to read");
        assert_eq!(val2, json!(2));
    });
}

// =============================================================================
// Reconnect Deduplication Tests
// =============================================================================

#[test]
fn test_reconnect_with_previous_connection_id() {
    run_test(|| async {
        let server = TestServer::new();

        // First connection
        let mut client1 = server.client();
        client1.connect("reconnect-db").await;

        let connection_id = client1
            .get_connection_id()
            .expect("expected connection ID")
            .to_string();

        // Write with specific request ID
        client1
            .set_with_request_id("/data", "original", "reconnect-req-1")
            .await
            .expect("failed to set");

        // Disconnect first client (drop it)
        client1.disconnect().await;
        drop(client1);

        // Second connection using the SAME connection ID (simulating reconnect)
        let mut client2 = server.client();
        client2
            .connect_with_connection_id("reconnect-db", &connection_id)
            .await;

        // Retry the write with same request ID - should be deduplicated
        client2
            .set_with_request_id("/data", "retry-value", "reconnect-req-1")
            .await
            .expect("failed to set");

        // Value should still be "original" because the retry was deduplicated
        let val = client2.once("/data").await.expect("failed to read");
        assert_eq!(
            val,
            json!("original"),
            "expected 'original' (deduplicated), got {:?}",
            val
        );

        // A write with a DIFFERENT request ID should succeed
        client2
            .set_with_request_id("/data", "new-value", "reconnect-req-2")
            .await
            .expect("failed to set");

        let val2 = client2.once("/data").await.expect("failed to read");
        assert_eq!(val2, json!("new-value"));
    });
}
