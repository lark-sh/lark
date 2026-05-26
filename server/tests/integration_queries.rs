//! Query integration tests.
//!
//! Tests for query operations (orderBy, limitToFirst, limitToLast, equalTo, etc.)

mod common;

use common::{QueryOptions, TestServer, run_test};
use serde_json::json;
use std::time::Duration;

// =============================================================================
// Query and Priority Tests
// =============================================================================

#[test]
fn test_set_with_priority() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client = server.client();
        client.connect("priority-db").await;

        // Set items with different priorities
        client
            .set_with_priority("/items/c", json!({"name": "item-c"}), 3.0)
            .await
            .expect("Failed to set item c");
        client
            .set_with_priority("/items/a", json!({"name": "item-a"}), 1.0)
            .await
            .expect("Failed to set item a");
        client
            .set_with_priority("/items/b", json!({"name": "item-b"}), 2.0)
            .await
            .expect("Failed to set item b");

        // Query with orderByPriority
        let result = client
            .once_query(
                "/items",
                QueryOptions {
                    order_by: Some("priority".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("Failed to query");

        // Result should be a map with all items
        let result_map = result.as_object().expect("Expected object");
        assert_eq!(result_map.len(), 3, "Expected 3 items");

        // Verify .priority is included in the data
        let item_a = result_map.get("a").expect("Expected item a");
        let item_a_obj = item_a.as_object().expect("Expected item a to be object");
        assert_eq!(
            item_a_obj.get(".priority"),
            Some(&json!(1.0)),
            "Expected item a to have .priority=1.0"
        );
    });
}

#[test]
fn test_query_order_by_key() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client = server.client();
        client.connect("orderby-key-db").await;

        // Set items (out of order)
        client
            .set("/items/zebra", "z")
            .await
            .expect("Failed to set");
        client
            .set("/items/apple", "a")
            .await
            .expect("Failed to set");
        client
            .set("/items/mango", "m")
            .await
            .expect("Failed to set");

        // Query with orderByKey
        let result = client
            .once_query(
                "/items",
                QueryOptions {
                    order_by: Some("key".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("Failed to query");

        let result_map = result.as_object().expect("Expected object");
        assert_eq!(result_map.len(), 3, "Expected 3 items");
    });
}

#[test]
fn test_query_limit_to_first() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client = server.client();
        client.connect("limit-first-db").await;

        // Set 5 items
        for i in 1..=5 {
            client
                .set(&format!("/items/item{}", i), format!("value{}", i))
                .await
                .expect("Failed to set");
        }

        // Query with limitToFirst(2)
        let result = client
            .once_query(
                "/items",
                QueryOptions {
                    order_by: Some("key".to_string()),
                    limit_to_first: Some(2),
                    ..Default::default()
                },
            )
            .await
            .expect("Failed to query");

        let result_map = result.as_object().expect("Expected object");
        assert_eq!(
            result_map.len(),
            2,
            "Expected 2 items with limitToFirst(2), got {}",
            result_map.len()
        );

        // Should have item1 and item2 (first two alphabetically)
        assert!(
            result_map.contains_key("item1"),
            "Expected item1 to be in result"
        );
        assert!(
            result_map.contains_key("item2"),
            "Expected item2 to be in result"
        );
    });
}

#[test]
fn test_query_limit_to_last() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client = server.client();
        client.connect("limit-last-db").await;

        // Set 5 items
        for i in 1..=5 {
            client
                .set(&format!("/items/item{}", i), format!("value{}", i))
                .await
                .expect("Failed to set");
        }

        // Query with limitToLast(2)
        let result = client
            .once_query(
                "/items",
                QueryOptions {
                    order_by: Some("key".to_string()),
                    limit_to_last: Some(2),
                    ..Default::default()
                },
            )
            .await
            .expect("Failed to query");

        let result_map = result.as_object().expect("Expected object");
        assert_eq!(
            result_map.len(),
            2,
            "Expected 2 items with limitToLast(2), got {}",
            result_map.len()
        );

        // Should have item4 and item5 (last two alphabetically)
        assert!(
            result_map.contains_key("item4"),
            "Expected item4 to be in result"
        );
        assert!(
            result_map.contains_key("item5"),
            "Expected item5 to be in result"
        );
    });
}

#[test]
fn test_query_order_by_priority_with_limit() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client = server.client();
        client.connect("priority-limit-db").await;

        // Set items with priorities (lower = higher priority)
        client
            .set_with_priority(
                "/scores/player_c",
                json!({"name": "Charlie", "score": 300}),
                3.0,
            )
            .await
            .expect("Failed to set");
        client
            .set_with_priority(
                "/scores/player_a",
                json!({"name": "Alice", "score": 100}),
                1.0,
            )
            .await
            .expect("Failed to set");
        client
            .set_with_priority(
                "/scores/player_d",
                json!({"name": "Dave", "score": 400}),
                4.0,
            )
            .await
            .expect("Failed to set");
        client
            .set_with_priority(
                "/scores/player_b",
                json!({"name": "Bob", "score": 200}),
                2.0,
            )
            .await
            .expect("Failed to set");

        // Get top 2 by priority
        let result = client
            .once_query(
                "/scores",
                QueryOptions {
                    order_by: Some("priority".to_string()),
                    limit_to_first: Some(2),
                    ..Default::default()
                },
            )
            .await
            .expect("Failed to query");

        let result_map = result.as_object().expect("Expected object");
        assert_eq!(result_map.len(), 2, "Expected 2 items");

        // Should have player_a (priority 1) and player_b (priority 2)
        assert!(
            result_map.contains_key("player_a"),
            "Expected player_a (priority 1) to be in top 2"
        );
        assert!(
            result_map.contains_key("player_b"),
            "Expected player_b (priority 2) to be in top 2"
        );
    });
}

#[test]
fn test_number_query_equal_to() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client = server.client();
        client.connect("number-query-db").await;

        // Set up some data with numeric values
        client
            .set("/items/a", json!({"name": "alice", "score": 100}))
            .await
            .expect("Failed to set");
        client
            .set("/items/b", json!({"name": "bob", "score": 200}))
            .await
            .expect("Failed to set");
        client
            .set("/items/c", json!({"name": "charlie", "score": 100}))
            .await
            .expect("Failed to set");

        // Subscribe with equalTo query
        client
            .subscribe_with_query(
                "/items",
                &["value"],
                QueryOptions {
                    order_by_child: Some("score".to_string()),
                    equal_to: Some(json!(100)),
                    ..Default::default()
                },
            )
            .await
            .expect("Subscribe failed");

        // Wait for initial value
        let event = client
            .wait_for_event(Duration::from_secs(2))
            .await
            .expect("Failed to receive event");

        let value = event.value.expect("Expected value").to_value();
        let data = value.as_object().expect("Expected object");

        // Should have alice and charlie (score=100), not bob (score=200)
        assert_eq!(
            data.len(),
            2,
            "Expected 2 items with score=100, got {}: {:?}",
            data.len(),
            data
        );
        assert!(data.contains_key("a"), "Expected 'a' (alice) in results");
        assert!(data.contains_key("c"), "Expected 'c' (charlie) in results");
        assert!(!data.contains_key("b"), "Unexpected 'b' (bob) in results");
    });
}
