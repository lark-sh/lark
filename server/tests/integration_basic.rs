//! Basic integration tests for connection and CRUD operations.
//!
//! These tests use a proxy-style test harness that mimics how real clients
//! connect through the proxy layer.

// 3.14 appears as test data, not as an approximation of PI.
#![allow(clippy::approx_constant)]

mod common;

use common::{TestServer, run_test};
use serde_json::{Value, json};

// =============================================================================
// Connection Tests
// =============================================================================

#[test]
fn test_client_connects_to_database() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        // Connect to a database
        client.connect("test-db").await;

        // Verify database was created
        assert_eq!(server.database_count(), 1);
    });
}

#[test]
fn test_multiple_clients_same_database() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client1 = server.client();
        let mut client2 = server.client();

        // Both connect to the same database
        client1.connect("shared-db").await;
        client2.connect("shared-db").await;

        // Should still be just one database
        assert_eq!(server.database_count(), 1);
    });
}

#[test]
fn test_clients_different_databases() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client1 = server.client();
        let mut client2 = server.client();

        // Connect to different databases
        client1.connect("db-1").await;
        client2.connect("db-2").await;

        // Should be two databases
        assert_eq!(server.database_count(), 2);
    });
}

// =============================================================================
// Set and Once Tests
// =============================================================================

#[test]
fn test_set_and_once() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("test-db").await;

        // Set some data
        client
            .set("/players/abc/name", "Alice")
            .await
            .expect("Failed to set");

        // Read it back
        let value = client
            .once("/players/abc/name")
            .await
            .expect("Failed to once");

        assert_eq!(value, json!("Alice"));
    });
}

#[test]
fn test_set_nested_object_and_once() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("test-db").await;

        // Set an object
        let player = json!({
            "name": "Alice",
            "score": 100
        });
        client
            .set("/players/abc", player)
            .await
            .expect("Failed to set");

        // Read it back
        let value = client.once("/players/abc").await.expect("Failed to once");

        let value_map = value.as_object().expect("Expected object");
        assert_eq!(value_map.get("name"), Some(&json!("Alice")));
        assert_eq!(value_map.get("score"), Some(&json!(100)));
    });
}

#[test]
fn test_once_nonexistent_path() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("test-db").await;

        // Read a path that doesn't exist
        let value = client
            .once("/does/not/exist")
            .await
            .expect("Failed to once");

        assert_eq!(value, Value::Null);
    });
}

#[test]
fn test_set_overwrites_previous_value() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("test-db").await;

        // Set initial value
        client.set("/path", "first").await.expect("Failed to set");

        // Overwrite with new value
        client.set("/path", "second").await.expect("Failed to set");

        // Read back - should be second value
        let value = client.once("/path").await.expect("Failed to once");
        assert_eq!(value, json!("second"));
    });
}

#[test]
fn test_set_various_types() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("test-db").await;

        // String
        client.set("/types/string", "hello").await.unwrap();
        assert_eq!(client.once("/types/string").await.unwrap(), json!("hello"));

        // Number (integer)
        client.set("/types/int", 42).await.unwrap();
        assert_eq!(client.once("/types/int").await.unwrap(), json!(42));

        // Number (float)
        client.set("/types/float", 3.14).await.unwrap();
        assert_eq!(client.once("/types/float").await.unwrap(), json!(3.14));

        // Boolean
        client.set("/types/bool", true).await.unwrap();
        assert_eq!(client.once("/types/bool").await.unwrap(), json!(true));

        // Null
        client.set("/types/null", Value::Null).await.unwrap();
        assert_eq!(client.once("/types/null").await.unwrap(), Value::Null);

        // Array (preserved as array)
        client.set("/types/array", json!([1, 2, 3])).await.unwrap();
        assert_eq!(client.once("/types/array").await.unwrap(), json!([1, 2, 3]));
    });
}

// =============================================================================
// Update (Merge) Tests
// =============================================================================

#[test]
fn test_update() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("test-db").await;

        // Set initial data
        client
            .set(
                "/players/abc",
                json!({
                    "name": "Alice",
                    "score": 100,
                    "hp": 50
                }),
            )
            .await
            .expect("Failed to set");

        // Update just the score
        client
            .update("/players/abc", json!({"score": 200}))
            .await
            .expect("Failed to update");

        // Verify
        let value = client.once("/players/abc").await.expect("Failed to once");
        let value_map = value.as_object().expect("Expected object");

        assert_eq!(value_map.get("name"), Some(&json!("Alice")));
        assert_eq!(value_map.get("score"), Some(&json!(200)));
        assert_eq!(value_map.get("hp"), Some(&json!(50)));
    });
}

#[test]
fn test_update_creates_missing_fields() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("test-db").await;

        // Set initial data
        client
            .set("/players/abc", json!({"name": "Alice"}))
            .await
            .expect("Failed to set");

        // Update with new field
        client
            .update("/players/abc", json!({"score": 100}))
            .await
            .expect("Failed to update");

        // Verify both fields exist
        let value = client.once("/players/abc").await.expect("Failed to once");
        let value_map = value.as_object().expect("Expected object");

        assert_eq!(value_map.get("name"), Some(&json!("Alice")));
        assert_eq!(value_map.get("score"), Some(&json!(100)));
    });
}

#[test]
fn test_update_on_nonexistent_path() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("test-db").await;

        // Update on a path that doesn't exist yet
        client
            .update("/new/path", json!({"key": "value"}))
            .await
            .expect("Failed to update");

        // Should have created the path
        let value = client.once("/new/path").await.expect("Failed to once");
        assert_eq!(value, json!({"key": "value"}));
    });
}

// =============================================================================
// Remove Tests
// =============================================================================

#[test]
fn test_remove() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("test-db").await;

        // Set data
        client
            .set("/players/abc", "data")
            .await
            .expect("Failed to set");

        // Verify it exists
        let value = client.once("/players/abc").await.expect("Failed to once");
        assert_eq!(value, json!("data"));

        // Remove it
        client
            .remove("/players/abc")
            .await
            .expect("Failed to remove");

        // Verify it's gone
        let value = client.once("/players/abc").await.expect("Failed to once");
        assert_eq!(value, Value::Null);
    });
}

#[test]
fn test_remove_subtree() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("test-db").await;

        // Set nested data
        client.set("/players/abc/name", "Alice").await.unwrap();
        client.set("/players/abc/score", 100).await.unwrap();
        client.set("/players/xyz/name", "Alex").await.unwrap();

        // Remove abc subtree
        client
            .remove("/players/abc")
            .await
            .expect("Failed to remove");

        // abc should be gone
        let value = client.once("/players/abc").await.expect("Failed to once");
        assert_eq!(value, Value::Null);

        // xyz should still exist
        let value = client.once("/players/xyz").await.expect("Failed to once");
        assert_eq!(value, json!({"name": "Alex"}));
    });
}

#[test]
fn test_remove_nonexistent_path() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("test-db").await;

        // Remove a path that doesn't exist - should succeed silently
        let result = client.remove("/does/not/exist").await;
        assert!(result.is_ok());
    });
}

// =============================================================================
// Multi-Client Tests
// =============================================================================

#[test]
fn test_client_sees_other_clients_writes() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client1 = server.client();
        let mut client2 = server.client();

        // Both connect to the same database
        client1.connect("shared-db").await;
        client2.connect("shared-db").await;

        // Client 1 sets data
        client1
            .set("/shared/data", "from client 1")
            .await
            .expect("client1 failed to set");

        // Client 2 reads it
        let value = client2
            .once("/shared/data")
            .await
            .expect("client2 failed to once");
        assert_eq!(value, json!("from client 1"));
    });
}

#[test]
fn test_concurrent_writes_to_different_paths() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client1 = server.client();
        let mut client2 = server.client();

        // Both connect to the same database
        client1.connect("concurrent-db").await;
        client2.connect("concurrent-db").await;

        // Both write to different paths concurrently
        // Note: In Glommio, we use futures::future::join instead of tokio::join!
        let write1 = client1.set("/path1", "value1");
        let write2 = client2.set("/path2", "value2");

        let (r1, r2) = futures::future::join(write1, write2).await;
        r1.expect("client1 write failed");
        r2.expect("client2 write failed");

        // Both values should be present
        let v1 = client1.once("/path1").await.unwrap();
        let v2 = client1.once("/path2").await.unwrap();
        assert_eq!(v1, json!("value1"));
        assert_eq!(v2, json!("value2"));
    });
}

// =============================================================================
// Edge Cases
// =============================================================================

#[test]
fn test_deep_nesting() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("test-db").await;

        // Set deeply nested data
        client
            .set("/a/b/c/d/e/f/g/h/i/j", "deep")
            .await
            .expect("Failed to set deeply nested path");

        // Read it back
        let value = client
            .once("/a/b/c/d/e/f/g/h/i/j")
            .await
            .expect("Failed to once");
        assert_eq!(value, json!("deep"));

        // Read partial path
        let value = client.once("/a/b/c").await.expect("Failed to once");
        assert!(value.is_object());
    });
}

#[test]
fn test_special_characters_in_keys() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("test-db").await;

        // Keys with special characters (but not disallowed ones)
        client
            .set("/users/user-123/email", "test@example.com")
            .await
            .unwrap();
        client
            .set("/paths/path_with_underscore/value", 1)
            .await
            .unwrap();

        let value = client.once("/users/user-123/email").await.unwrap();
        assert_eq!(value, json!("test@example.com"));
    });
}

#[test]
fn test_empty_object() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("test-db").await;

        // Set an empty object - Firebase treats this as deletion
        client.set("/path", json!({})).await.unwrap();

        // Should be null (empty objects are pruned)
        let value = client.once("/path").await.unwrap();
        assert_eq!(value, Value::Null);
    });
}

#[test]
fn test_large_value() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("test-db").await;

        // Create a large string (1KB)
        let large_string = "x".repeat(1024);

        client.set("/large", json!(large_string)).await.unwrap();

        let value = client.once("/large").await.unwrap();
        assert_eq!(value.as_str().unwrap().len(), 1024);
    });
}
