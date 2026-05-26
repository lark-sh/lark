//! Transaction integration tests.
//!
//! Tests for atomic multi-path transactions with conditions, updates, and deletes.

mod common;

use common::{TestServer, compute_jcs_hash, run_test};
use lark_server::protocol::TransactionOp;
use serde_json::json;
use std::time::Duration;

// =============================================================================
// Basic Transaction Tests
// =============================================================================

#[test]
fn test_transaction_atomic_multi_path() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client = server.client();
        client.connect("tx-atomic-db").await;

        // Execute a transaction with multiple paths
        client
            .transaction(vec![
                TransactionOp {
                    op: "s".to_string(),
                    path: "/users/alice/name".to_string(),
                    value: Some(json!("Alice")),
                    hash: None,
                },
                TransactionOp {
                    op: "s".to_string(),
                    path: "/users/alice/score".to_string(),
                    value: Some(json!(100)),
                    hash: None,
                },
                TransactionOp {
                    op: "s".to_string(),
                    path: "/posts/post1/author".to_string(),
                    value: Some(json!("alice")),
                    hash: None,
                },
            ])
            .await
            .expect("Transaction failed");

        // Verify all values were set
        let name = client
            .once("/users/alice/name")
            .await
            .expect("Failed to read name");
        assert_eq!(name, json!("Alice"));

        let score = client
            .once("/users/alice/score")
            .await
            .expect("Failed to read score");
        assert_eq!(score, json!(100));

        let author = client
            .once("/posts/post1/author")
            .await
            .expect("Failed to read author");
        assert_eq!(author, json!("alice"));
    });
}

#[test]
fn test_transaction_with_mixed_operations() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client = server.client();
        client.connect("tx-mixed-db").await;

        // Set up initial data
        client
            .set("/data/a", "initial-a")
            .await
            .expect("Failed to set");
        client
            .set("/data/b", json!({"x": 1, "y": 2}))
            .await
            .expect("Failed to set");
        client
            .set("/data/c", "to-be-deleted")
            .await
            .expect("Failed to set");

        // Execute transaction with set, update, and delete
        client
            .transaction(vec![
                TransactionOp {
                    op: "s".to_string(),
                    path: "/data/a".to_string(),
                    value: Some(json!("updated-a")),
                    hash: None,
                },
                TransactionOp {
                    op: "u".to_string(),
                    path: "/data/b".to_string(),
                    value: Some(json!({"z": 3})),
                    hash: None,
                },
                TransactionOp {
                    op: "d".to_string(),
                    path: "/data/c".to_string(),
                    value: None,
                    hash: None,
                },
            ])
            .await
            .expect("Transaction failed");

        // Verify set
        let a = client.once("/data/a").await.expect("Failed to read");
        assert_eq!(a, json!("updated-a"));

        // Verify update (should merge, not replace)
        let b = client.once("/data/b").await.expect("Failed to read");
        let b_obj = b.as_object().expect("Expected object");
        assert_eq!(b_obj.get("x"), Some(&json!(1)));
        assert_eq!(b_obj.get("y"), Some(&json!(2)));
        assert_eq!(b_obj.get("z"), Some(&json!(3)));

        // Verify delete
        let c = client.once("/data/c").await.expect("Failed to read");
        assert_eq!(c, json!(null));
    });
}

#[test]
fn test_transaction_partial_permission_denied() {
    run_test(|| async {
        let server = TestServer::new();

        // Set up rules where only /allowed is writable
        server
            .set_rules(
                "tx-perm-db",
                json!({
                    "rules": {
                        ".read": true,
                        "allowed": {
                            ".write": true
                        },
                        "denied": {
                            ".write": false
                        }
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("tx-perm-db").await;

        // Try a transaction where one path is allowed and one is denied
        // The ENTIRE transaction should be rejected
        let result = client
            .transaction(vec![
                TransactionOp {
                    op: "s".to_string(),
                    path: "/allowed/data".to_string(),
                    value: Some(json!("ok")),
                    hash: None,
                },
                TransactionOp {
                    op: "s".to_string(),
                    path: "/denied/data".to_string(),
                    value: Some(json!("should fail")),
                    hash: None,
                },
            ])
            .await;

        assert!(
            result.is_err(),
            "Transaction should fail due to permission denied"
        );

        // Verify that NEITHER path was written (atomic rollback)
        let allowed = client.once("/allowed/data").await.expect("Failed to read");
        assert_eq!(
            allowed,
            json!(null),
            "Allowed path should not have been written due to atomic transaction failure"
        );
    });
}

#[test]
fn test_transaction_empty_fails() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client = server.client();
        client.connect("tx-empty-db").await;

        // Empty transaction should fail
        let result = client.transaction(vec![]).await;
        assert!(result.is_err(), "Empty transaction should fail");
    });
}

#[test]
fn test_transaction_notifies_subscribers() {
    run_test(|| async {
        let server = TestServer::new();

        // Client 1: writer
        let mut client1 = server.client();
        client1.connect("tx-events-db").await;

        // Client 2: subscriber
        let mut client2 = server.client();
        client2.connect("tx-events-db").await;

        // Client 2 subscribes
        client2
            .subscribe("/users", &["value"])
            .await
            .expect("Failed to subscribe");

        // Drain initial value event
        let _ = client2.wait_for_event(Duration::from_secs(1)).await;
        client2.clear_events().await;

        // Client 1 executes transaction
        client1
            .transaction(vec![
                TransactionOp {
                    op: "s".to_string(),
                    path: "/users/alice".to_string(),
                    value: Some(json!({"name": "Alice"})),
                    hash: None,
                },
                TransactionOp {
                    op: "s".to_string(),
                    path: "/users/bob".to_string(),
                    value: Some(json!({"name": "Bob"})),
                    hash: None,
                },
            ])
            .await
            .expect("Transaction failed");

        // Wait for events
        glommio::timer::sleep(Duration::from_millis(200)).await;

        // Client 2 should have received events for the changes
        let events = client2.events().await;
        assert!(
            !events.is_empty(),
            "Subscriber should have received events from transaction"
        );
    });
}

// =============================================================================
// Condition Tests (Value-based)
// =============================================================================

#[test]
fn test_transaction_condition_success() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client = server.client();
        client.connect("tx-condition-db").await;

        // Set initial value
        client.set("/counter", 5).await.expect("Failed to set");

        // Transaction with condition that should succeed
        client
            .transaction(vec![
                TransactionOp {
                    op: "c".to_string(),
                    path: "/counter".to_string(),
                    value: Some(json!(5)), // condition: must equal 5
                    hash: None,
                },
                TransactionOp {
                    op: "s".to_string(),
                    path: "/counter".to_string(),
                    value: Some(json!(6)), // set to 6
                    hash: None,
                },
            ])
            .await
            .expect("Transaction with valid condition should succeed");

        // Verify the value was updated
        let val = client.once("/counter").await.expect("Failed to read");
        assert_eq!(val, json!(6));
    });
}

#[test]
fn test_transaction_condition_fails() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client = server.client();
        client.connect("tx-condition-fail-db").await;

        // Set initial value
        client.set("/counter", 5).await.expect("Failed to set");

        // Transaction with condition that should fail (expecting 10, but it's 5)
        let result = client
            .transaction(vec![
                TransactionOp {
                    op: "c".to_string(),
                    path: "/counter".to_string(),
                    value: Some(json!(10)), // condition: must equal 10 (WRONG)
                    hash: None,
                },
                TransactionOp {
                    op: "s".to_string(),
                    path: "/counter".to_string(),
                    value: Some(json!(11)), // would set to 11
                    hash: None,
                },
            ])
            .await;

        assert!(
            result.is_err(),
            "Transaction with invalid condition should fail"
        );

        // Verify the value was NOT updated
        let val = client.once("/counter").await.expect("Failed to read");
        assert_eq!(val, json!(5), "Counter should still be 5 (unchanged)");
    });
}

#[test]
fn test_transaction_condition_with_nil() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client = server.client();
        client.connect("tx-condition-nil-db").await;

        // Transaction with condition on non-existent path (should be nil)
        client
            .transaction(vec![
                TransactionOp {
                    op: "c".to_string(),
                    path: "/new-key".to_string(),
                    value: None, // condition: must be nil (doesn't exist)
                    hash: None,
                },
                TransactionOp {
                    op: "s".to_string(),
                    path: "/new-key".to_string(),
                    value: Some(json!(1)), // create with value 1
                    hash: None,
                },
            ])
            .await
            .expect("Transaction with nil condition should succeed");

        // Verify the value was created
        let val = client.once("/new-key").await.expect("Failed to read");
        assert_eq!(val, json!(1));

        // Now try again - should fail because value exists
        let result = client
            .transaction(vec![
                TransactionOp {
                    op: "c".to_string(),
                    path: "/new-key".to_string(),
                    value: None, // condition: must be nil (but it's 1 now)
                    hash: None,
                },
                TransactionOp {
                    op: "s".to_string(),
                    path: "/new-key".to_string(),
                    value: Some(json!(2)), // would set to 2
                    hash: None,
                },
            ])
            .await;

        assert!(
            result.is_err(),
            "Transaction should fail when condition expects nil but value exists"
        );
    });
}

#[test]
fn test_transaction_condition_with_object() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client = server.client();
        client.connect("tx-condition-obj-db").await;

        // Set initial object value
        client
            .set("/user", json!({"name": "Alice", "score": 100}))
            .await
            .expect("Failed to set");

        // Small delay to ensure Set is fully processed
        glommio::timer::sleep(Duration::from_millis(100)).await;

        // Transaction with condition on object
        client
            .transaction(vec![
                TransactionOp {
                    op: "c".to_string(),
                    path: "/user".to_string(),
                    value: Some(json!({"name": "Alice", "score": 100})),
                    hash: None,
                },
                TransactionOp {
                    op: "s".to_string(),
                    path: "/user".to_string(),
                    value: Some(json!({"name": "Alice", "score": 150})),
                    hash: None,
                },
            ])
            .await
            .expect("Transaction with matching object condition should succeed");

        // Verify the value was updated
        let val = client.once("/user/score").await.expect("Failed to read");
        assert_eq!(val, json!(150));
    });
}

#[test]
fn test_transaction_multiple_conditions() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client = server.client();
        client.connect("tx-multi-cond-db").await;

        // Set initial values
        client.set("/a", 1).await.expect("Failed to set");
        client.set("/b", 2).await.expect("Failed to set");

        // Transaction with multiple conditions - all must pass
        client
            .transaction(vec![
                TransactionOp {
                    op: "c".to_string(),
                    path: "/a".to_string(),
                    value: Some(json!(1)),
                    hash: None,
                },
                TransactionOp {
                    op: "c".to_string(),
                    path: "/b".to_string(),
                    value: Some(json!(2)),
                    hash: None,
                },
                TransactionOp {
                    op: "s".to_string(),
                    path: "/a".to_string(),
                    value: Some(json!(10)),
                    hash: None,
                },
                TransactionOp {
                    op: "s".to_string(),
                    path: "/b".to_string(),
                    value: Some(json!(20)),
                    hash: None,
                },
            ])
            .await
            .expect("Transaction with all valid conditions should succeed");

        // Verify both were updated
        let a = client.once("/a").await.expect("Failed to read");
        let b = client.once("/b").await.expect("Failed to read");
        assert_eq!(a, json!(10));
        assert_eq!(b, json!(20));

        // Transaction where one condition fails
        let result = client
            .transaction(vec![
                TransactionOp {
                    op: "c".to_string(),
                    path: "/a".to_string(),
                    value: Some(json!(10)), // correct
                    hash: None,
                },
                TransactionOp {
                    op: "c".to_string(),
                    path: "/b".to_string(),
                    value: Some(json!(2)), // WRONG - it's 20 now
                    hash: None,
                },
                TransactionOp {
                    op: "s".to_string(),
                    path: "/a".to_string(),
                    value: Some(json!(100)),
                    hash: None,
                },
                TransactionOp {
                    op: "s".to_string(),
                    path: "/b".to_string(),
                    value: Some(json!(200)),
                    hash: None,
                },
            ])
            .await;

        assert!(
            result.is_err(),
            "Transaction should fail if any condition fails"
        );

        // Verify neither was updated
        let a = client.once("/a").await.expect("Failed to read");
        let b = client.once("/b").await.expect("Failed to read");
        assert_eq!(a, json!(10), "a should be unchanged");
        assert_eq!(b, json!(20), "b should be unchanged");
    });
}

// =============================================================================
// Hash-based Condition Tests
// =============================================================================

#[test]
fn test_transaction_condition_with_hash() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client = server.client();
        client.connect("tx-hash-cond-db").await;

        // Set initial value (a complex object with an array)
        let initial_value = json!({
            "name": "Alice",
            "score": 100,
            "items": ["sword", "shield"]
        });
        client
            .set("/user", initial_value.clone())
            .await
            .expect("Failed to set");

        // Compute hash of the value (arrays are preserved as arrays)
        let hash = compute_jcs_hash(&initial_value);

        // Transaction with hash-based condition should succeed
        client
            .transaction(vec![
                TransactionOp {
                    op: "c".to_string(),
                    path: "/user".to_string(),
                    value: None,
                    hash: Some(hash),
                },
                TransactionOp {
                    op: "s".to_string(),
                    path: "/user/score".to_string(),
                    value: Some(json!(200)),
                    hash: None,
                },
            ])
            .await
            .expect("Transaction with valid hash condition should succeed");

        // Verify the update was applied
        let score = client.once("/user/score").await.expect("Failed to read");
        assert_eq!(score, json!(200));
    });
}

#[test]
fn test_transaction_condition_with_hash_fails() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client = server.client();
        client.connect("tx-hash-fail-db").await;

        // Set initial value
        client.set("/counter", 5).await.expect("Failed to set");

        // Compute hash of DIFFERENT value (wrong hash)
        let wrong_hash = compute_jcs_hash(&json!(10)); // hash of 10, but actual value is 5

        // Transaction with wrong hash should fail
        let result = client
            .transaction(vec![
                TransactionOp {
                    op: "c".to_string(),
                    path: "/counter".to_string(),
                    value: None,
                    hash: Some(wrong_hash),
                },
                TransactionOp {
                    op: "s".to_string(),
                    path: "/counter".to_string(),
                    value: Some(json!(6)),
                    hash: None,
                },
            ])
            .await;

        assert!(result.is_err(), "Transaction with wrong hash should fail");

        // Verify value was not changed
        let value = client.once("/counter").await.expect("Failed to read");
        assert_eq!(value, json!(5), "Value should be unchanged");
    });
}

#[test]
fn test_transaction_condition_with_hash_on_object() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client = server.client();
        client.connect("tx-hash-obj-db").await;

        // Set a large nested object
        let large_object = json!({
            "users": {
                "alice": {"score": 100, "level": 5},
                "bob": {"score": 200, "level": 10}
            },
            "metadata": {
                "version": 1,
                "updated": "2024-01-01"
            }
        });
        client
            .set("/data", large_object.clone())
            .await
            .expect("Failed to set");

        // Compute hash of the object
        let hash = compute_jcs_hash(&large_object);

        // Transaction using hash (saves bandwidth vs sending full object)
        client
            .transaction(vec![
                TransactionOp {
                    op: "c".to_string(),
                    path: "/data".to_string(),
                    value: None,
                    hash: Some(hash),
                },
                TransactionOp {
                    op: "s".to_string(),
                    path: "/data/users/alice/score".to_string(),
                    value: Some(json!(150)),
                    hash: None,
                },
            ])
            .await
            .expect("Transaction with hash condition on object should succeed");

        // Verify update
        let score = client
            .once("/data/users/alice/score")
            .await
            .expect("Failed to read");
        assert_eq!(score, json!(150));
    });
}

#[test]
fn test_transaction_condition_with_hash_on_null() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client = server.client();
        client.connect("tx-hash-null-db").await;

        // Compute hash of null (path doesn't exist)
        let null_hash = compute_jcs_hash(&json!(null));

        // Transaction to create new value - condition checks path doesn't exist
        client
            .transaction(vec![
                TransactionOp {
                    op: "c".to_string(),
                    path: "/newpath".to_string(),
                    value: None,
                    hash: Some(null_hash.clone()),
                },
                TransactionOp {
                    op: "s".to_string(),
                    path: "/newpath".to_string(),
                    value: Some(json!("created")),
                    hash: None,
                },
            ])
            .await
            .expect("Transaction with null hash condition should succeed");

        // Verify creation
        let value = client.once("/newpath").await.expect("Failed to read");
        assert_eq!(value, json!("created"));

        // Now the same transaction should fail (path exists)
        let result = client
            .transaction(vec![
                TransactionOp {
                    op: "c".to_string(),
                    path: "/newpath".to_string(),
                    value: None,
                    hash: Some(null_hash),
                },
                TransactionOp {
                    op: "s".to_string(),
                    path: "/newpath".to_string(),
                    value: Some(json!("overwrite")),
                    hash: None,
                },
            ])
            .await;

        assert!(
            result.is_err(),
            "Transaction should fail when path exists but null hash expected"
        );
    });
}
