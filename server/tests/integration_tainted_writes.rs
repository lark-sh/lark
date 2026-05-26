//! Tainted write tests.
//!
//! These test tainted write detection - if a write fails (e.g., permission denied),
//! subsequent writes that depend on it (specified in pending_writes list) should be
//! silently rejected.

mod common;

use common::{TestServer, TransactionOp, run_test};
use serde_json::json;
use std::time::Duration;

// =============================================================================
// Tainted Write Detection Tests
// =============================================================================

#[test]
fn test_tainted_write_detection() {
    run_test(|| async {
        let server = TestServer::new();

        // Set up rules where /denied is not writable
        // Note: project_id is extracted from database_id (before the '/')
        server
            .set_rules(
                "tainted-db",
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
            .unwrap();

        let mut client = server.client();
        client.connect("tainted-db").await;

        // First write to /denied should fail with permission_denied
        let result = client
            .set_with_pending_writes("/denied/data", "should fail", "req-1", None)
            .await;

        assert!(
            result.is_err(),
            "expected first write to fail with permission_denied"
        );
        let err = result.unwrap_err();
        assert!(
            err.contains("permission_denied"),
            "expected permission_denied error, got: {}",
            err
        );

        // Second write with pw:[req-1] is tainted - server silently ignores it
        client
            .set_with_pending_writes_fire_and_forget(
                "/allowed/data",
                "should also fail",
                "req-2",
                vec!["req-1".to_string()],
            )
            .await
            .unwrap();

        // Give server time to process (and ignore) the tainted write
        glommio::timer::sleep(Duration::from_millis(100)).await;

        // Verify nothing was written
        let val = client.once("/allowed/data").await.unwrap();
        assert_eq!(val, serde_json::Value::Null, "expected nil, got {:?}", val);
    });
}

#[test]
fn test_non_tainted_write() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("non-tainted-db").await;

        // First write succeeds
        let result = client
            .set_with_pending_writes("/data", "first", "req-1", None)
            .await;
        assert!(result.is_ok(), "first write failed: {:?}", result);

        // Second write with pw:[req-1] should succeed (req-1 was acked, not nacked)
        let result = client
            .set_with_pending_writes("/data", "second", "req-2", Some(vec!["req-1".to_string()]))
            .await;
        assert!(result.is_ok(), "second write failed: {:?}", result);

        // Verify the value
        let val = client.once("/data").await.unwrap();
        assert_eq!(val, json!("second"), "expected 'second', got {:?}", val);
    });
}

#[test]
fn test_condition_failed_does_not_taint() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("condition-failed-db").await;

        // Set initial value
        client.set("/counter", 5).await.unwrap();

        // Transaction with wrong condition should fail with condition_failed
        let result = client
            .transaction(vec![
                TransactionOp {
                    op: "c".to_string(),
                    path: "/counter".to_string(),
                    value: Some(json!(999)), // Wrong expected value
                    hash: None,
                },
                TransactionOp {
                    op: "s".to_string(),
                    path: "/counter".to_string(),
                    value: Some(json!(6)),
                    hash: None,
                },
            ])
            .await;

        assert!(
            result.is_err(),
            "expected transaction to fail with condition_failed"
        );
        let err = result.unwrap_err();
        assert!(
            err.contains("condition_failed"),
            "expected condition_failed error, got: {}",
            err
        );

        // A subsequent write should NOT be tainted (condition_failed is not recorded)
        client.set("/counter", 10).await.unwrap();

        let val = client.once("/counter").await.unwrap();
        assert_eq!(val, json!(10), "expected 10, got {:?}", val);
    });
}

#[test]
fn test_tainted_write_with_multiple_pending_writes() {
    run_test(|| async {
        let server = TestServer::new();

        // Set up rules where /denied is not writable
        // Note: project_id is extracted from database_id (before the '/')
        server
            .set_rules(
                "multi-tainted-db",
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
            .unwrap();

        let mut client = server.client();
        client.connect("multi-tainted-db").await;

        // req-1: succeeds
        let result = client
            .set_with_pending_writes("/allowed/a", "ok", "req-1", None)
            .await;
        assert!(result.is_ok(), "req-1 failed: {:?}", result);

        // req-2: fails (permission denied)
        let result = client
            .set_with_pending_writes(
                "/denied/b",
                "fail",
                "req-2",
                Some(vec!["req-1".to_string()]),
            )
            .await;
        assert!(result.is_err(), "expected req-2 to fail");

        // req-3: succeeds (only depends on req-1 which was acked)
        let result = client
            .set_with_pending_writes("/allowed/c", "ok", "req-3", Some(vec!["req-1".to_string()]))
            .await;
        assert!(result.is_ok(), "req-3 failed: {:?}", result);

        // req-4 with pw:[req-1, req-2, req-3] is tainted because req-2 was nacked
        client
            .set_with_pending_writes_fire_and_forget(
                "/allowed/d",
                "should fail",
                "req-4",
                vec![
                    "req-1".to_string(),
                    "req-2".to_string(),
                    "req-3".to_string(),
                ],
            )
            .await
            .unwrap();

        // Give server time to process (and ignore) the tainted write
        glommio::timer::sleep(Duration::from_millis(100)).await;

        // Verify /allowed/d was NOT written (tainted write was ignored)
        let val = client.once("/allowed/d").await.unwrap();
        assert_eq!(
            val,
            serde_json::Value::Null,
            "expected nil at /allowed/d, got {:?}",
            val
        );
    });
}

#[test]
fn test_empty_pending_writes_succeeds() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("empty-pw-db").await;

        // Write with empty pw should succeed
        let result = client
            .set_with_pending_writes("/data", "value", "req-1", Some(vec![]))
            .await;
        assert!(result.is_ok(), "write with empty pw failed: {:?}", result);

        let val = client.once("/data").await.unwrap();
        assert_eq!(val, json!("value"), "expected 'value', got {:?}", val);
    });
}
