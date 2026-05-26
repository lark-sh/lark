//! Number preservation tests.
//!
//! These test that numbers are correctly preserved through the wire format:
//! - Integers should not gain decimal points
//! - Floats should preserve precision
//! - Large integers beyond float64 precision should be preserved

// 3.14159 appears as test data, not as an approximation of PI.
#![allow(clippy::approx_constant)]

mod common;

use common::{TestServer, run_test};
use serde_json::json;

// =============================================================================
// Number Preservation Tests
// =============================================================================

#[test]
fn test_number_preservation_integer() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("number-int-db").await;

        // Set an integer value
        client.set("/test/score", 42).await.expect("set failed");

        // Read it back via once - should still be an integer
        let value = client.once("/test/score").await.expect("once failed");

        // Should be 42, not 42.0
        assert_eq!(value, json!(42));

        // Check it's represented as integer, not float
        assert!(
            value.is_i64() || value.is_u64(),
            "expected integer type, got {:?}",
            value
        );
    });
}

#[test]
fn test_number_preservation_float() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("number-float-db").await;

        // Set a float value
        client
            .set("/test/position", 3.14159)
            .await
            .expect("set failed");

        // Read it back
        let value = client.once("/test/position").await.expect("once failed");

        // Check it preserved the float value
        let f = value.as_f64().expect("expected float");
        assert!((f - 3.14159).abs() < 0.00001, "expected 3.14159, got {}", f);
    });
}

#[test]
fn test_number_preservation_large_integer() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("number-large-db").await;

        // Set a large integer that would lose precision as float64
        // 9007199254740993 is 2^53 + 1, which cannot be exactly represented as float64
        let large_int: i64 = 9007199254740993;
        client
            .set("/test/bignum", large_int)
            .await
            .expect("set failed");

        // Read it back
        let value = client.once("/test/bignum").await.expect("once failed");

        // Should preserve the exact value
        let read_val = value.as_i64().expect("expected i64");
        assert_eq!(
            read_val, large_int,
            "expected {}, got {}",
            large_int, read_val
        );
    });
}
