//! Server values tests (timestamps, increments).
//!
//! These test the {".sv": "timestamp"} and {".sv": {"increment": delta}} functionality.

mod common;

use common::{TestServer, TransactionOp, run_test};
use serde_json::json;
use std::time::{SystemTime, UNIX_EPOCH};

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

// =============================================================================
// Timestamp Tests
// =============================================================================

#[test]
fn test_server_value_timestamp() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("test-sv-timestamp").await;

        // Get time before write
        let before_ms = now_millis();

        // Set with server timestamp
        client
            .set("/createdAt", json!({".sv": "timestamp"}))
            .await
            .expect("Failed to set");

        // Get time after write
        let after_ms = now_millis();

        // Read back the value
        let value = client.once("/createdAt").await.expect("Failed to once");

        // Should be a number (timestamp in ms)
        let ts = value.as_i64().expect("Expected integer timestamp");

        // Check timestamp is within expected range
        assert!(
            ts >= before_ms && ts <= after_ms,
            "timestamp {} not in range [{}, {}]",
            ts,
            before_ms,
            after_ms
        );
    });
}

#[test]
fn test_server_value_timestamp_nested() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("test-sv-timestamp-nested").await;

        // Get time before write
        let before_ms = now_millis();

        // Set object with nested timestamp
        client
            .set(
                "/post",
                json!({
                    "title": "Hello",
                    "createdAt": {".sv": "timestamp"}
                }),
            )
            .await
            .expect("Failed to set");

        // Get time after write
        let after_ms = now_millis();

        // Read back the value
        let value = client.once("/post").await.expect("Failed to once");
        let post = value.as_object().expect("Expected object");

        // Check title is preserved
        assert_eq!(post.get("title"), Some(&json!("Hello")));

        // Check timestamp
        let ts = post
            .get("createdAt")
            .expect("Expected createdAt")
            .as_i64()
            .expect("Expected integer timestamp");

        assert!(
            ts >= before_ms && ts <= after_ms,
            "timestamp {} not in range [{}, {}]",
            ts,
            before_ms,
            after_ms
        );
    });
}

// =============================================================================
// Increment Tests
// =============================================================================

#[test]
fn test_server_value_increment() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("test-sv-increment").await;

        // Set initial value
        client
            .set("/score", 50)
            .await
            .expect("Failed to set initial");

        // Increment by 10
        client
            .set("/score", json!({".sv": {"increment": 10}}))
            .await
            .expect("Failed to increment");

        // Read back
        let value = client.once("/score").await.expect("Failed to once");

        // Should be 60
        assert_eq!(value, json!(60));
    });
}

#[test]
fn test_server_value_increment_null_starts_at_zero() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("test-sv-increment-null").await;

        // Increment non-existent path (should treat as 0)
        client
            .set("/newCounter", json!({".sv": {"increment": 5}}))
            .await
            .expect("Failed to increment");

        // Read back
        let value = client.once("/newCounter").await.expect("Failed to once");

        // Should be 5
        assert_eq!(value, json!(5));
    });
}

#[test]
fn test_server_value_increment_negative() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("test-sv-decrement").await;

        // Set initial value
        client
            .set("/score", 100)
            .await
            .expect("Failed to set initial");

        // Decrement by 30 (negative increment)
        client
            .set("/score", json!({".sv": {"increment": -30}}))
            .await
            .expect("Failed to decrement");

        // Read back
        let value = client.once("/score").await.expect("Failed to once");

        // Should be 70
        assert_eq!(value, json!(70));
    });
}

#[test]
fn test_server_value_increment_non_numeric_treated_as_zero() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("test-sv-increment-nonnumeric").await;

        // Set string value
        client
            .set("/value", "hello")
            .await
            .expect("Failed to set initial");

        // Increment non-numeric (should treat as 0)
        client
            .set("/value", json!({".sv": {"increment": 7}}))
            .await
            .expect("Failed to increment");

        // Read back
        let value = client.once("/value").await.expect("Failed to once");

        // Should be 7 (0 + 7)
        assert_eq!(value, json!(7));
    });
}

// =============================================================================
// Server Values in Update
// =============================================================================

#[test]
fn test_server_value_in_update() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("test-sv-update").await;

        // Set initial data
        client
            .set(
                "/user",
                json!({
                    "name": "Alice",
                    "score": 10
                }),
            )
            .await
            .expect("Failed to set initial");

        // Get time before update
        let before_ms = now_millis();

        // Update with server values
        client
            .update(
                "/user",
                json!({
                    "lastSeen": {".sv": "timestamp"},
                    "score": {".sv": {"increment": 5}}
                }),
            )
            .await
            .expect("Failed to update");

        // Get time after update
        let after_ms = now_millis();

        // Read back
        let value = client.once("/user").await.expect("Failed to once");
        let user = value.as_object().expect("Expected object");

        // Check name is preserved
        assert_eq!(user.get("name"), Some(&json!("Alice")));

        // Check score is incremented
        assert_eq!(user.get("score"), Some(&json!(15)));

        // Check timestamp
        let ts = user
            .get("lastSeen")
            .expect("Expected lastSeen")
            .as_i64()
            .expect("Expected integer timestamp");

        assert!(
            ts >= before_ms && ts <= after_ms,
            "timestamp {} not in range [{}, {}]",
            ts,
            before_ms,
            after_ms
        );
    });
}

// =============================================================================
// Server Values in Transaction
// =============================================================================

#[test]
fn test_server_value_in_transaction() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("test-sv-transaction").await;

        // Set initial score
        client
            .set("/score", 100)
            .await
            .expect("Failed to set initial");

        // Get time before transaction
        let before_ms = now_millis();

        // Transaction with server values
        client
            .transaction(vec![
                TransactionOp {
                    op: "s".to_string(),
                    path: "/score".to_string(),
                    value: Some(json!({".sv": {"increment": 50}})),
                    hash: None,
                },
                TransactionOp {
                    op: "s".to_string(),
                    path: "/lastUpdate".to_string(),
                    value: Some(json!({".sv": "timestamp"})),
                    hash: None,
                },
            ])
            .await
            .expect("Transaction failed");

        // Get time after transaction
        let after_ms = now_millis();

        // Check score
        let score_val = client.once("/score").await.expect("Failed to once score");
        assert_eq!(score_val, json!(150));

        // Check timestamp
        let ts_val = client
            .once("/lastUpdate")
            .await
            .expect("Failed to once lastUpdate");
        let ts = ts_val.as_i64().expect("Expected integer timestamp");

        assert!(
            ts >= before_ms && ts <= after_ms,
            "timestamp {} not in range [{}, {}]",
            ts,
            before_ms,
            after_ms
        );
    });
}
