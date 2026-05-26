//! Priority and .value validation tests.
//!
//! These test the Firebase .value and .priority special key validation:
//! - .value with only .priority is valid (wrapped primitive with priority)
//! - .value with other keys is invalid
//! - Regular objects with .priority are valid

mod common;

use common::{TestServer, TransactionOp, run_test};
use serde_json::json;

// =============================================================================
// Valid .value/.priority Patterns
// =============================================================================

#[test]
fn test_set_value_priority_valid() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("value-priority-db").await;

        // Valid: .value with only .priority (wrapped primitive)
        let valid_wrapped = json!({
            ".value": 42,
            ".priority": 5
        });
        client
            .set("/wrapped", valid_wrapped)
            .await
            .expect("valid wrapped primitive should succeed");

        // Valid: .value alone
        let valid_value_only = json!({
            ".value": "hello"
        });
        client
            .set("/valueonly", valid_value_only)
            .await
            .expect("valid .value only should succeed");

        // Valid: Regular object with .priority
        let valid_object_with_priority = json!({
            "name": "Alice",
            "score": 100,
            ".priority": 5
        });
        client
            .set("/object", valid_object_with_priority)
            .await
            .expect("valid object with .priority should succeed");

        // Valid: Regular data without special keys
        let valid_regular = json!({
            "name": "Bob",
            "score": 200
        });
        client
            .set("/regular", valid_regular)
            .await
            .expect("valid regular data should succeed");
    });
}

// =============================================================================
// Invalid .value Patterns
// =============================================================================

#[test]
fn test_set_value_priority_invalid() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("value-priority-invalid-db").await;

        // Invalid: .value with other children
        let invalid_data = json!({
            ".value": 42,
            ".priority": 5,
            "foo": "bar"
        });
        let result = client.set("/invalid", invalid_data).await;
        assert!(
            result.is_err(),
            "expected error for .value with other children"
        );
        let err = result.unwrap_err();
        assert!(
            err.contains("invalid_data") || err.contains(".value"),
            "expected invalid_data error, got: {}",
            err
        );

        // Invalid: .value with extra key (no priority)
        let invalid_data2 = json!({
            ".value": 42,
            "extra": "data"
        });
        let result2 = client.set("/invalid2", invalid_data2).await;
        assert!(result2.is_err(), "expected error for .value with extra key");
    });
}

#[test]
fn test_set_value_priority_nested_invalid() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("value-priority-nested-db").await;

        // Invalid: Nested object has invalid .value pattern
        let nested_invalid = json!({
            "users": {
                "alice": {
                    ".value": 42,
                    ".priority": 5,
                    "extra": "bad"
                }
            }
        });
        let result = client.set("/data", nested_invalid).await;
        assert!(
            result.is_err(),
            "expected error for nested invalid .value pattern"
        );
        let err = result.unwrap_err();
        assert!(
            err.contains("invalid_data") || err.contains(".value"),
            "expected invalid_data error, got: {}",
            err
        );
    });
}

#[test]
fn test_update_value_priority_invalid() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("value-priority-update-db").await;

        // Invalid update: one of the values has invalid .value pattern
        let invalid_update = json!({
            "alice": {
                ".value": 42,
                ".priority": 5,
                "extra": "bad"
            }
        });
        let result = client.update("/users", invalid_update).await;
        assert!(
            result.is_err(),
            "expected error for update with invalid .value pattern"
        );
        let err = result.unwrap_err();
        assert!(
            err.contains("invalid_data") || err.contains(".value"),
            "expected invalid_data error, got: {}",
            err
        );
    });
}

#[test]
fn test_transaction_value_priority_invalid() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("value-priority-tx-db").await;

        // Transaction with invalid .value in set operation
        let result = client
            .transaction(vec![TransactionOp {
                op: "s".to_string(),
                path: "/data".to_string(),
                value: Some(json!({
                    ".value": 42,
                    ".priority": 5,
                    "extra": "bad"
                })),
                hash: None,
            }])
            .await;

        assert!(
            result.is_err(),
            "expected error for transaction with invalid .value pattern"
        );
        let err = result.unwrap_err();
        assert!(
            err.contains("invalid_data") || err.contains(".value"),
            "expected invalid_data error, got: {}",
            err
        );
    });
}
