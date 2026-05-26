//! Query view tests.
//!
//! These test query subscriptions with limits, ranges, and ordering:
//! - limitToFirst/limitToLast with updates entering/exiting view
//! - startAt/endAt/equalTo range filtering
//! - orderByChild/orderByValue/orderByPriority/orderByKey

mod common;

use common::{QueryOptions, TestServer, run_test};
use serde_json::json;
use std::time::Duration;

// =============================================================================
// Basic Query View Tests
// =============================================================================

#[test]
fn test_query_view_initial_value() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("query-view-db").await;

        // Set up data with 5 items
        client
            .set("/items/a", json!({"name": "alpha"}))
            .await
            .unwrap();
        client
            .set("/items/b", json!({"name": "bravo"}))
            .await
            .unwrap();
        client
            .set("/items/c", json!({"name": "charlie"}))
            .await
            .unwrap();
        client
            .set("/items/d", json!({"name": "delta"}))
            .await
            .unwrap();
        client
            .set("/items/e", json!({"name": "echo"}))
            .await
            .unwrap();

        // Subscribe with limitToFirst(2)
        client
            .subscribe_with_query(
                "/items",
                &["value"],
                QueryOptions {
                    order_by: Some("key".to_string()),
                    limit_to_first: Some(2),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Should receive initial value with only 2 items (a and b, sorted by key)
        let event = client.wait_for_event(Duration::from_secs(2)).await.unwrap();

        assert_eq!(event.event.as_deref(), Some("put"));

        // Check that only 2 items were returned
        let data: serde_json::Value = event.value.expect("expected value").to_value();
        let result_map = data.as_object().expect("expected object");

        assert_eq!(result_map.len(), 2, "expected 2 items with limitToFirst(2)");
        assert!(result_map.contains_key("a"), "expected item 'a' in result");
        assert!(result_map.contains_key("b"), "expected item 'b' in result");
    });
}

#[test]
fn test_query_view_updates_in_view() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client1 = server.client();
        let mut client2 = server.client();

        client1.connect("query-updates-db").await;
        client2.connect("query-updates-db").await;

        // Set up data with 5 items
        client2
            .set("/items/a", json!({"name": "alpha"}))
            .await
            .unwrap();
        client2
            .set("/items/b", json!({"name": "bravo"}))
            .await
            .unwrap();
        client2
            .set("/items/c", json!({"name": "charlie"}))
            .await
            .unwrap();
        client2
            .set("/items/d", json!({"name": "delta"}))
            .await
            .unwrap();
        client2
            .set("/items/e", json!({"name": "echo"}))
            .await
            .unwrap();

        // Client 1 subscribes with limitToFirst(2)
        client1
            .subscribe_with_query(
                "/items",
                &["value"],
                QueryOptions {
                    order_by: Some("key".to_string()),
                    limit_to_first: Some(2),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Consume initial event
        client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        // Client 2 updates item 'a' (which is IN the view)
        client2.set("/items/a/name", "ALPHA UPDATED").await.unwrap();

        // Client 1 should receive the update
        let event = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        assert_eq!(event.event.as_deref(), Some("put"));

        // The path should be relative: /a/name
        assert_eq!(event.path.as_deref(), Some("/a/name"));
    });
}

#[test]
fn test_query_view_updates_outside_view() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client1 = server.client();
        let mut client2 = server.client();

        client1.connect("query-outside-db").await;
        client2.connect("query-outside-db").await;

        // Set up data with 5 items
        client2
            .set("/items/a", json!({"name": "alpha"}))
            .await
            .unwrap();
        client2
            .set("/items/b", json!({"name": "bravo"}))
            .await
            .unwrap();
        client2
            .set("/items/c", json!({"name": "charlie"}))
            .await
            .unwrap();
        client2
            .set("/items/d", json!({"name": "delta"}))
            .await
            .unwrap();
        client2
            .set("/items/e", json!({"name": "echo"}))
            .await
            .unwrap();

        // Client 1 subscribes with limitToFirst(2) - will see 'a' and 'b'
        client1
            .subscribe_with_query(
                "/items",
                &["value"],
                QueryOptions {
                    order_by: Some("key".to_string()),
                    limit_to_first: Some(2),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Consume initial event
        client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        // Client 2 updates item 'e' (which is OUTSIDE the view)
        client2.set("/items/e/name", "ECHO UPDATED").await.unwrap();

        // Client 1 should NOT receive this update (timeout expected)
        let result = client1.wait_for_event(Duration::from_millis(500)).await;
        assert!(
            result.is_err(),
            "expected timeout - should not receive update for item outside view"
        );
    });
}

#[test]
fn test_query_view_item_enters_view() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client1 = server.client();
        let mut client2 = server.client();

        client1.connect("query-enter-db").await;
        client2.connect("query-enter-db").await;

        // Set up data with 3 items: b, c, d
        client2
            .set("/items/b", json!({"name": "bravo"}))
            .await
            .unwrap();
        client2
            .set("/items/c", json!({"name": "charlie"}))
            .await
            .unwrap();
        client2
            .set("/items/d", json!({"name": "delta"}))
            .await
            .unwrap();

        // Client 1 subscribes with limitToFirst(2) - will see 'b' and 'c'
        client1
            .subscribe_with_query(
                "/items",
                &["value"],
                QueryOptions {
                    order_by: Some("key".to_string()),
                    limit_to_first: Some(2),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Consume initial event
        client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        // Client 2 adds item 'a' (which should enter the view, pushing 'c' out)
        client2
            .set("/items/a", json!({"name": "alpha"}))
            .await
            .unwrap();

        // Client 1 should receive an atomic patch event with the swap
        let event = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        assert_eq!(
            event.event.as_deref(),
            Some("patch"),
            "expected patch event"
        );
        assert_eq!(event.path.as_deref(), Some("/"), "expected patch at '/'");

        let data: serde_json::Value = event.value.expect("expected value").to_value();
        let value_map = data.as_object().expect("expected object");

        // Patch should contain: exited 'c' as null, entered 'a' with data
        assert_eq!(
            value_map.get("/c"),
            Some(&serde_json::Value::Null),
            "'c' should be removed (null)"
        );
        assert!(value_map.contains_key("/a"), "expected 'a' to enter view");
    });
}

#[test]
fn test_query_view_item_exits_view() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client1 = server.client();
        let mut client2 = server.client();

        client1.connect("query-exit-db").await;
        client2.connect("query-exit-db").await;

        // Set up data with 3 items: a, b, c
        client2
            .set("/items/a", json!({"name": "alpha"}))
            .await
            .unwrap();
        client2
            .set("/items/b", json!({"name": "bravo"}))
            .await
            .unwrap();
        client2
            .set("/items/c", json!({"name": "charlie"}))
            .await
            .unwrap();

        // Client 1 subscribes with limitToFirst(2) - will see 'a' and 'b'
        client1
            .subscribe_with_query(
                "/items",
                &["value"],
                QueryOptions {
                    order_by: Some("key".to_string()),
                    limit_to_first: Some(2),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Consume initial event
        client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        // Client 2 deletes item 'a' (which is in view, 'c' should now enter)
        client2.remove("/items/a").await.unwrap();

        // Client 1 should receive an atomic patch event with the swap
        let event = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        assert_eq!(
            event.event.as_deref(),
            Some("patch"),
            "expected patch event"
        );
        assert_eq!(event.path.as_deref(), Some("/"), "expected patch at '/'");

        let data: serde_json::Value = event.value.expect("expected value").to_value();
        let value_map = data.as_object().expect("expected object");

        // Patch should contain: exited 'a' as null, entered 'c' with data
        assert_eq!(
            value_map.get("/a"),
            Some(&serde_json::Value::Null),
            "'a' should be removed (null)"
        );
        assert!(value_map.contains_key("/c"), "expected 'c' to enter view");
    });
}

// =============================================================================
// Range Query Tests
// =============================================================================

#[test]
fn test_query_view_start_at() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("query-startat-db").await;

        // Set up data with 5 items: a, b, c, d, e
        client
            .set("/items/a", json!({"name": "alpha"}))
            .await
            .unwrap();
        client
            .set("/items/b", json!({"name": "bravo"}))
            .await
            .unwrap();
        client
            .set("/items/c", json!({"name": "charlie"}))
            .await
            .unwrap();
        client
            .set("/items/d", json!({"name": "delta"}))
            .await
            .unwrap();
        client
            .set("/items/e", json!({"name": "echo"}))
            .await
            .unwrap();

        // Subscribe with orderByKey + startAt("c") - should see c, d, e
        client
            .subscribe_with_query(
                "/items",
                &["value"],
                QueryOptions {
                    order_by: Some("key".to_string()),
                    start_at: Some(json!("c")),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let event = client.wait_for_event(Duration::from_secs(2)).await.unwrap();

        let data: serde_json::Value = event.value.expect("expected value").to_value();
        let result_map = data.as_object().expect("expected object");

        // Should have c, d, e (3 items starting from "c")
        assert_eq!(result_map.len(), 3, "expected 3 items with startAt('c')");
        assert!(result_map.contains_key("c"), "expected item 'c' in result");
        assert!(result_map.contains_key("d"), "expected item 'd' in result");
        assert!(result_map.contains_key("e"), "expected item 'e' in result");
    });
}

#[test]
fn test_query_view_end_at() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("query-endat-db").await;

        // Set up data with 5 items: a, b, c, d, e
        client
            .set("/items/a", json!({"name": "alpha"}))
            .await
            .unwrap();
        client
            .set("/items/b", json!({"name": "bravo"}))
            .await
            .unwrap();
        client
            .set("/items/c", json!({"name": "charlie"}))
            .await
            .unwrap();
        client
            .set("/items/d", json!({"name": "delta"}))
            .await
            .unwrap();
        client
            .set("/items/e", json!({"name": "echo"}))
            .await
            .unwrap();

        // Subscribe with orderByKey + endAt("c") - should see a, b, c
        client
            .subscribe_with_query(
                "/items",
                &["value"],
                QueryOptions {
                    order_by: Some("key".to_string()),
                    end_at: Some(json!("c")),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let event = client.wait_for_event(Duration::from_secs(2)).await.unwrap();

        let data: serde_json::Value = event.value.expect("expected value").to_value();
        let result_map = data.as_object().expect("expected object");

        // Should have a, b, c (3 items ending at "c")
        assert_eq!(result_map.len(), 3, "expected 3 items with endAt('c')");
        assert!(result_map.contains_key("a"), "expected item 'a' in result");
        assert!(result_map.contains_key("b"), "expected item 'b' in result");
        assert!(result_map.contains_key("c"), "expected item 'c' in result");
    });
}

#[test]
fn test_query_view_start_at_end_at() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("query-range-db").await;

        // Set up data with 5 items: a, b, c, d, e
        client
            .set("/items/a", json!({"name": "alpha"}))
            .await
            .unwrap();
        client
            .set("/items/b", json!({"name": "bravo"}))
            .await
            .unwrap();
        client
            .set("/items/c", json!({"name": "charlie"}))
            .await
            .unwrap();
        client
            .set("/items/d", json!({"name": "delta"}))
            .await
            .unwrap();
        client
            .set("/items/e", json!({"name": "echo"}))
            .await
            .unwrap();

        // Subscribe with orderByKey + startAt("b") + endAt("d") - should see b, c, d
        client
            .subscribe_with_query(
                "/items",
                &["value"],
                QueryOptions {
                    order_by: Some("key".to_string()),
                    start_at: Some(json!("b")),
                    end_at: Some(json!("d")),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let event = client.wait_for_event(Duration::from_secs(2)).await.unwrap();

        let data: serde_json::Value = event.value.expect("expected value").to_value();
        let result_map = data.as_object().expect("expected object");

        // Should have b, c, d (3 items in range)
        assert_eq!(
            result_map.len(),
            3,
            "expected 3 items with startAt('b')+endAt('d')"
        );
        assert!(result_map.contains_key("b"), "expected item 'b' in result");
        assert!(result_map.contains_key("c"), "expected item 'c' in result");
        assert!(result_map.contains_key("d"), "expected item 'd' in result");
    });
}

#[test]
fn test_query_view_equal_to() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("query-equalto-db").await;

        // Set up data with 5 items: a, b, c, d, e
        client
            .set("/items/a", json!({"name": "alpha"}))
            .await
            .unwrap();
        client
            .set("/items/b", json!({"name": "bravo"}))
            .await
            .unwrap();
        client
            .set("/items/c", json!({"name": "charlie"}))
            .await
            .unwrap();
        client
            .set("/items/d", json!({"name": "delta"}))
            .await
            .unwrap();
        client
            .set("/items/e", json!({"name": "echo"}))
            .await
            .unwrap();

        // Subscribe with orderByKey + equalTo("c") - should see only c
        client
            .subscribe_with_query(
                "/items",
                &["value"],
                QueryOptions {
                    order_by: Some("key".to_string()),
                    equal_to: Some(json!("c")),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let event = client.wait_for_event(Duration::from_secs(2)).await.unwrap();

        let data: serde_json::Value = event.value.expect("expected value").to_value();
        let result_map = data.as_object().expect("expected object");

        // Should have only c
        assert_eq!(result_map.len(), 1, "expected 1 item with equalTo('c')");
        assert!(result_map.contains_key("c"), "expected item 'c' in result");
    });
}

// =============================================================================
// orderByChild Tests
// =============================================================================

#[test]
fn test_query_view_order_by_child() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("query-orderbychild-db").await;

        // Set up data with different scores
        client
            .set("/players/alice", json!({"name": "Alice", "score": 100}))
            .await
            .unwrap();
        client
            .set("/players/bob", json!({"name": "Bob", "score": 50}))
            .await
            .unwrap();
        client
            .set("/players/charlie", json!({"name": "Charlie", "score": 150}))
            .await
            .unwrap();
        client
            .set("/players/dave", json!({"name": "Dave", "score": 75}))
            .await
            .unwrap();

        // Subscribe with orderByChild("score") + limitToFirst(2) - should get bob (50) and dave (75)
        client
            .subscribe_with_query(
                "/players",
                &["value"],
                QueryOptions {
                    order_by_child: Some("score".to_string()),
                    limit_to_first: Some(2),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let event = client.wait_for_event(Duration::from_secs(2)).await.unwrap();

        let data: serde_json::Value = event.value.expect("expected value").to_value();
        let result_map = data.as_object().expect("expected object");

        // Should have bob and dave (lowest 2 scores)
        assert_eq!(result_map.len(), 2, "expected 2 items with limitToFirst(2)");
        assert!(
            result_map.contains_key("bob"),
            "expected 'bob' (score 50) in result"
        );
        assert!(
            result_map.contains_key("dave"),
            "expected 'dave' (score 75) in result"
        );
    });
}

#[test]
fn test_query_view_order_by_child_with_start_at() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("query-orderbychild-startat-db").await;

        // Set up data with different scores
        client
            .set("/players/alice", json!({"name": "Alice", "score": 100}))
            .await
            .unwrap();
        client
            .set("/players/bob", json!({"name": "Bob", "score": 50}))
            .await
            .unwrap();
        client
            .set("/players/charlie", json!({"name": "Charlie", "score": 150}))
            .await
            .unwrap();
        client
            .set("/players/dave", json!({"name": "Dave", "score": 75}))
            .await
            .unwrap();

        // Subscribe with orderByChild("score") + startAt(100) - should get alice (100) and charlie (150)
        client
            .subscribe_with_query(
                "/players",
                &["value"],
                QueryOptions {
                    order_by_child: Some("score".to_string()),
                    start_at: Some(json!(100)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let event = client.wait_for_event(Duration::from_secs(2)).await.unwrap();

        let data: serde_json::Value = event.value.expect("expected value").to_value();
        let result_map = data.as_object().expect("expected object");

        // Should have alice and charlie (scores >= 100)
        assert_eq!(result_map.len(), 2, "expected 2 items with startAt(100)");
        assert!(
            result_map.contains_key("alice"),
            "expected 'alice' (score 100) in result"
        );
        assert!(
            result_map.contains_key("charlie"),
            "expected 'charlie' (score 150) in result"
        );
    });
}

#[test]
fn test_query_view_item_enters_between() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client1 = server.client();
        let mut client2 = server.client();

        client1.connect("query-enter-between-db").await;
        client2.connect("query-enter-between-db").await;

        // Set up initial data: b, d
        client2
            .set("/items/b", json!({"name": "bravo"}))
            .await
            .unwrap();
        client2
            .set("/items/d", json!({"name": "delta"}))
            .await
            .unwrap();

        // Subscribe with limitToFirst(3) - will see b, d
        client1
            .subscribe_with_query(
                "/items",
                &["value"],
                QueryOptions {
                    order_by: Some("key".to_string()),
                    limit_to_first: Some(3),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Consume initial event
        client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        // Add 'c' which should enter between 'b' and 'd'
        client2
            .set("/items/c", json!({"name": "charlie"}))
            .await
            .unwrap();

        // Client 1 should receive event for 'c' entering
        let event = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        assert_eq!(event.path.as_deref(), Some("/c"), "expected path '/c'");

        // Verify the data is included
        let data: serde_json::Value = event.value.expect("expected value").to_value();
        let data_map = data.as_object().expect("expected object");
        assert_eq!(
            data_map.get("name").and_then(|v| v.as_str()),
            Some("charlie")
        );
    });
}

// =============================================================================
// limitToLast Tests
// =============================================================================

#[test]
fn test_query_view_limit_to_last() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("query-limittolast-db").await;

        // Set up data with 5 items
        client
            .set("/items/a", json!({"name": "alpha"}))
            .await
            .unwrap();
        client
            .set("/items/b", json!({"name": "bravo"}))
            .await
            .unwrap();
        client
            .set("/items/c", json!({"name": "charlie"}))
            .await
            .unwrap();
        client
            .set("/items/d", json!({"name": "delta"}))
            .await
            .unwrap();
        client
            .set("/items/e", json!({"name": "echo"}))
            .await
            .unwrap();

        // Subscribe with limitToLast(2) - should see 'd' and 'e'
        client
            .subscribe_with_query(
                "/items",
                &["value"],
                QueryOptions {
                    order_by: Some("key".to_string()),
                    limit_to_last: Some(2),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let event = client.wait_for_event(Duration::from_secs(2)).await.unwrap();

        let data: serde_json::Value = event.value.expect("expected value").to_value();
        let result_map = data.as_object().expect("expected object");

        // Should have d and e (last 2 by key)
        assert_eq!(result_map.len(), 2, "expected 2 items with limitToLast(2)");
        assert!(result_map.contains_key("d"), "expected item 'd' in result");
        assert!(result_map.contains_key("e"), "expected item 'e' in result");
    });
}

// =============================================================================
// Range Enter/Exit Tests
// =============================================================================

#[test]
fn test_query_view_range_enter_exit() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client1 = server.client();
        let mut client2 = server.client();

        client1.connect("query-range-enter-exit-db").await;
        client2.connect("query-range-enter-exit-db").await;

        // Set up data with scores
        client2
            .set("/players/alice", json!({"name": "Alice", "score": 50}))
            .await
            .unwrap();
        client2
            .set("/players/bob", json!({"name": "Bob", "score": 100}))
            .await
            .unwrap();

        // Subscribe with orderByChild("score") + startAt(75) - should only see bob (100)
        client1
            .subscribe_with_query(
                "/players",
                &["value"],
                QueryOptions {
                    order_by_child: Some("score".to_string()),
                    start_at: Some(json!(75)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Consume initial event
        let initial_event = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        let initial_data: serde_json::Value =
            initial_event.value.expect("expected value").to_value();
        let initial_map = initial_data.as_object().expect("expected object");
        assert_eq!(initial_map.len(), 1, "expected 1 item initially (bob)");

        // Update alice's score to 80 - should now enter the view
        client2.set("/players/alice/score", 80).await.unwrap();

        // Should receive event for alice entering
        let event = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        assert_eq!(
            event.path.as_deref(),
            Some("/alice"),
            "expected path '/alice' for entering item"
        );

        // Verify alice's data is included
        let data: serde_json::Value = event.value.expect("expected value").to_value();
        let data_map = data.as_object().expect("expected object");
        assert_eq!(data_map.get("name").and_then(|v| v.as_str()), Some("Alice"));
    });
}

// =============================================================================
// PATCH Event Tests
// =============================================================================

#[test]
fn test_patch_event_on_update() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client1 = server.client();
        let mut client2 = server.client();

        client1.connect("patch-update-db").await;
        client2.connect("patch-update-db").await;

        // Set up initial data
        client2
            .set(
                "/players/alice",
                json!({"name": "Alice", "score": 100, "level": 1}),
            )
            .await
            .unwrap();

        // Subscribe to /players
        client1.subscribe("/players", &["value"]).await.unwrap();

        // Consume initial event
        client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        // Do an update on alice with multiple fields
        client2
            .update(
                "/players/alice",
                json!({"score": 150, "reaction": {"emoji": "thumbsup", "timestamp": 12345}}),
            )
            .await
            .unwrap();

        // Should receive a PATCH event with only the changed fields
        let event = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        // Verify it's a PATCH event
        assert_eq!(
            event.event.as_deref(),
            Some("patch"),
            "expected event type 'patch'"
        );

        // Verify path is "/"
        assert_eq!(event.path.as_deref(), Some("/"), "expected path '/'");

        // Parse the value and verify it contains only the changed fields
        let patch_data: serde_json::Value = event.value.expect("expected value").to_value();
        let patch_map = patch_data.as_object().expect("expected object");

        // Should have two entries: /alice/score and /alice/reaction
        assert_eq!(patch_map.len(), 2, "expected 2 fields in patch data");

        // Check score was updated
        assert!(
            patch_map.contains_key("/alice/score"),
            "expected /alice/score in patch data"
        );
        assert_eq!(patch_map.get("/alice/score"), Some(&json!(150)));

        // Check reaction was updated
        assert!(
            patch_map.contains_key("/alice/reaction"),
            "expected /alice/reaction in patch data"
        );
        let reaction = patch_map.get("/alice/reaction").unwrap();
        assert_eq!(
            reaction.get("emoji").and_then(|v| v.as_str()),
            Some("thumbsup")
        );
    });
}

// =============================================================================
// Empty Results Tests
// =============================================================================

#[test]
fn test_set_empty_deletes_data() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("set-empty-db").await;

        // Set initial data
        client.set("/foo/bar", "hello").await.unwrap();
        glommio::timer::sleep(Duration::from_millis(50)).await;

        // Verify it exists
        let val = client.once("/foo/bar").await.unwrap();
        assert_eq!(val, json!("hello"));

        // Now set it to empty object - should delete
        client.set("/foo/bar", json!({})).await.unwrap();
        glommio::timer::sleep(Duration::from_millis(50)).await;

        // Verify it's deleted
        let val = client.once("/foo/bar").await.unwrap();
        assert_eq!(
            val,
            serde_json::Value::Null,
            "expected nil after setting {{}}"
        );

        // Parent should also be auto-pruned
        let val = client.once("/foo").await.unwrap();
        assert_eq!(
            val,
            serde_json::Value::Null,
            "expected /foo to be auto-pruned"
        );
    });
}

#[test]
fn test_auto_prune_notifies_subscribers() {
    run_test(|| async {
        let server = TestServer::new();
        let mut writer = server.client();
        let mut subscriber = server.client();

        writer.connect("autoprune-notify-db").await;
        subscriber.connect("autoprune-notify-db").await;

        // Set initial data
        writer.set("/users/alice/name", "Alice").await.unwrap();
        glommio::timer::sleep(Duration::from_millis(50)).await;

        // Subscriber subscribes to /users
        subscriber.subscribe("/users", &["value"]).await.unwrap();

        // Get initial snapshot
        let event = subscriber
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();
        assert!(event.value.is_some(), "expected initial data");

        // Now delete the only child, causing auto-prune
        writer.remove("/users/alice/name").await.unwrap();

        // Wait for the event notifying us that /users is now null
        let event = subscriber
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        // The subscriber should receive notification that /users is now null
        let deleted_data: serde_json::Value = event
            .value
            .map(|v| v.to_value())
            .unwrap_or(serde_json::Value::Null);
        assert_eq!(
            deleted_data,
            serde_json::Value::Null,
            "expected null after auto-prune"
        );
    });
}

// =============================================================================
// Move Event Tests
// =============================================================================

#[test]
fn test_query_view_move_event() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client1 = server.client();
        let mut client2 = server.client();

        client1.connect("query-move-db").await;
        client2.connect("query-move-db").await;

        // Set up data with scores
        client2
            .set("/players/alice", json!({"name": "Alice", "score": 100}))
            .await
            .unwrap();
        client2
            .set("/players/bob", json!({"name": "Bob", "score": 200}))
            .await
            .unwrap();
        client2
            .set("/players/charlie", json!({"name": "Charlie", "score": 300}))
            .await
            .unwrap();

        // Subscribe with orderByChild("score") + limitToFirst(3)
        // Order should be: alice (100), bob (200), charlie (300)
        client1
            .subscribe_with_query(
                "/players",
                &["value"],
                QueryOptions {
                    order_by_child: Some("score".to_string()),
                    limit_to_first: Some(3),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Consume initial event
        client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        // Change alice's score to 250 - should move between bob and charlie
        // New order: bob (200), alice (250), charlie (300)
        client2.set("/players/alice/score", 250).await.unwrap();

        // Server sends data change so client can re-sort and detect the move
        let event = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        // Should be a PATCH with the score change
        assert_eq!(event.event.as_deref(), Some("patch"));

        // Parse the patch data
        let patch_data: serde_json::Value = event.value.expect("expected value").to_value();
        let patch_map = patch_data.as_object().expect("expected object");

        // Should contain alice's new score
        assert!(
            patch_map.contains_key("/alice/score"),
            "expected /alice/score in patch data"
        );
        assert_eq!(patch_map.get("/alice/score"), Some(&json!(250)));
    });
}

// =============================================================================
// Once Query Tests
// =============================================================================

#[test]
fn test_once_query_with_range_filters() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("once-range-filter-db").await;

        // Set up data with 5 items: a, b, c, d, e
        client
            .set("/items/a", json!({"name": "alpha", "score": 10}))
            .await
            .unwrap();
        client
            .set("/items/b", json!({"name": "bravo", "score": 20}))
            .await
            .unwrap();
        client
            .set("/items/c", json!({"name": "charlie", "score": 30}))
            .await
            .unwrap();
        client
            .set("/items/d", json!({"name": "delta", "score": 40}))
            .await
            .unwrap();
        client
            .set("/items/e", json!({"name": "echo", "score": 50}))
            .await
            .unwrap();

        glommio::timer::sleep(Duration::from_millis(100)).await;

        // Test 1: orderByKey + startAt("c") - should return c, d, e
        let result = client
            .once_query(
                "/items",
                QueryOptions {
                    order_by: Some("key".to_string()),
                    start_at: Some(json!("c")),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let result_map = result.as_object().expect("expected map result");
        assert_eq!(result_map.len(), 3, "startAt('c'): expected 3 items");
        assert!(
            !result_map.contains_key("a"),
            "startAt('c'): should not have 'a'"
        );
        assert!(
            !result_map.contains_key("b"),
            "startAt('c'): should not have 'b'"
        );
        assert!(
            result_map.contains_key("c"),
            "startAt('c'): should have 'c'"
        );

        // Test 2: orderByKey + endAt("c") - should return a, b, c
        let result = client
            .once_query(
                "/items",
                QueryOptions {
                    order_by: Some("key".to_string()),
                    end_at: Some(json!("c")),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let result_map = result.as_object().expect("expected map result");
        assert_eq!(result_map.len(), 3, "endAt('c'): expected 3 items");
        assert!(
            !result_map.contains_key("d"),
            "endAt('c'): should not have 'd'"
        );
        assert!(
            !result_map.contains_key("e"),
            "endAt('c'): should not have 'e'"
        );

        // Test 3: orderByKey + startAt("b") + endAt("d") - should return b, c, d
        let result = client
            .once_query(
                "/items",
                QueryOptions {
                    order_by: Some("key".to_string()),
                    start_at: Some(json!("b")),
                    end_at: Some(json!("d")),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let result_map = result.as_object().expect("expected map result");
        assert_eq!(
            result_map.len(),
            3,
            "startAt('b')+endAt('d'): expected 3 items"
        );

        // Test 4: orderByKey + equalTo("c") - should return only c
        let result = client
            .once_query(
                "/items",
                QueryOptions {
                    order_by: Some("key".to_string()),
                    equal_to: Some(json!("c")),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let result_map = result.as_object().expect("expected map result");
        assert_eq!(result_map.len(), 1, "equalTo('c'): expected 1 item");
        assert!(
            result_map.contains_key("c"),
            "equalTo('c'): should have 'c'"
        );

        // Test 5: orderByChild("score") + startAt(25) - should return c, d, e (scores 30, 40, 50)
        let result = client
            .once_query(
                "/items",
                QueryOptions {
                    order_by_child: Some("score".to_string()),
                    start_at: Some(json!(25)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let result_map = result.as_object().expect("expected map result");
        assert_eq!(
            result_map.len(),
            3,
            "orderByChild('score')+startAt(25): expected 3 items"
        );
        assert!(
            !result_map.contains_key("a"),
            "orderByChild('score')+startAt(25): should not have 'a' (score 10)"
        );
        assert!(
            !result_map.contains_key("b"),
            "orderByChild('score')+startAt(25): should not have 'b' (score 20)"
        );

        // Test 6: orderByKey + startAt("b") + limitToFirst(2) - should return b, c (not a, b!)
        let result = client
            .once_query(
                "/items",
                QueryOptions {
                    order_by: Some("key".to_string()),
                    start_at: Some(json!("b")),
                    limit_to_first: Some(2),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let result_map = result.as_object().expect("expected map result");
        assert_eq!(
            result_map.len(),
            2,
            "startAt('b')+limitToFirst(2): expected 2 items"
        );
        assert!(
            !result_map.contains_key("a"),
            "startAt('b')+limitToFirst(2): should not have 'a' - filter should apply before limit"
        );
        assert!(
            result_map.contains_key("b"),
            "startAt('b')+limitToFirst(2): should have 'b'"
        );
        assert!(
            result_map.contains_key("c"),
            "startAt('b')+limitToFirst(2): should have 'c'"
        );
    });
}

// =============================================================================
// Set/Remove at Subscription Path Tests
// =============================================================================

#[test]
fn test_query_view_set_at_subscription_path() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("query-set-at-path-db").await;

        // Subscribe with orderByChild('score') on an empty path
        client
            .subscribe_with_query(
                "/items",
                &["value"],
                QueryOptions {
                    order_by_child: Some("score".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // First event is the initial snapshot (null for empty path)
        let initial_event = client.wait_for_event(Duration::from_secs(2)).await.unwrap();
        assert_eq!(initial_event.event.as_deref(), Some("put"));
        assert_eq!(initial_event.path.as_deref(), Some("/"));

        // Set multiple children at the subscription path - should send ONE event, not multiple
        client
            .set(
                "/items",
                json!({
                    "alex": {"score": 60},
                    "greg": {"score": 52},
                    "tony": {"score": 52},
                    "vassili": {"score": 55.5},
                    "rob": {"score": 56}
                }),
            )
            .await
            .unwrap();

        // Should receive a single PUT event with the full value at path "/"
        let event = client.wait_for_event(Duration::from_secs(2)).await.unwrap();

        // The key assertion: path should be "/" (full snapshot), not "/alex", "/greg", etc.
        assert_eq!(
            event.path.as_deref(),
            Some("/"),
            "expected single PUT at '/' with full value"
        );

        // Verify the value contains all 5 items
        let data: serde_json::Value = event.value.expect("expected value").to_value();
        let result_map = data.as_object().expect("expected map result");

        assert_eq!(result_map.len(), 5, "expected 5 items in result");

        // Verify all expected keys are present
        for key in &["alex", "greg", "tony", "vassili", "rob"] {
            assert!(
                result_map.contains_key(*key),
                "expected key '{}' in result",
                key
            );
        }
    });
}

#[test]
fn test_query_view_remove_at_subscription_path() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("query-remove-at-path-db").await;

        // Set up initial data
        client
            .set(
                "/items",
                json!({
                    "alex": {"score": 60},
                    "greg": {"score": 52},
                    "tony": {"score": 52}
                }),
            )
            .await
            .unwrap();

        // Subscribe with orderByChild('score')
        client
            .subscribe_with_query(
                "/items",
                &["value"],
                QueryOptions {
                    order_by_child: Some("score".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // First event is the initial snapshot
        let initial_event = client.wait_for_event(Duration::from_secs(2)).await.unwrap();
        assert_eq!(initial_event.event.as_deref(), Some("put"));
        assert_eq!(initial_event.path.as_deref(), Some("/"));

        // Remove the entire path - should send ONE event with null, not individual removals
        client.remove("/items").await.unwrap();

        // Should receive a single PUT event with null at path "/"
        let event = client.wait_for_event(Duration::from_secs(2)).await.unwrap();

        // The key assertion: path should be "/" with null value
        assert_eq!(
            event.path.as_deref(),
            Some("/"),
            "expected single PUT at '/' with null"
        );

        // Verify the value is null
        let data: serde_json::Value = event
            .value
            .map(|v| v.to_value())
            .unwrap_or(serde_json::Value::Null);
        assert_eq!(
            data,
            serde_json::Value::Null,
            "expected null value for removal"
        );
    });
}

// =============================================================================
// Nested orderByChild Tests
// =============================================================================

#[test]
fn test_query_view_nested_order_by_child() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("nested-orderby-db").await;

        // Set up players with nested stats
        client
            .set(
                "/players/alice",
                json!({"name": "Alice", "stats": {"score": 150, "wins": 10}}),
            )
            .await
            .unwrap();
        client
            .set(
                "/players/bob",
                json!({"name": "Bob", "stats": {"score": 50, "wins": 5}}),
            )
            .await
            .unwrap();
        client
            .set(
                "/players/charlie",
                json!({"name": "Charlie", "stats": {"score": 100, "wins": 8}}),
            )
            .await
            .unwrap();

        // Subscribe with orderByChild("stats/score") + limitToFirst(2)
        client
            .subscribe_with_query(
                "/players",
                &["value"],
                QueryOptions {
                    order_by_child: Some("stats/score".to_string()),
                    limit_to_first: Some(2),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let event = client.wait_for_event(Duration::from_secs(2)).await.unwrap();

        // Should get bob (50) and charlie (100)
        let data: serde_json::Value = event.value.expect("expected value").to_value();
        let result_map = data.as_object().expect("expected object");

        assert_eq!(result_map.len(), 2, "expected 2 items");
        assert!(result_map.contains_key("bob"), "expected 'bob' in result");
        assert!(
            result_map.contains_key("charlie"),
            "expected 'charlie' in result"
        );
    });
}

// =============================================================================
// Sort Field Change Entering Limit Tests
// =============================================================================

#[test]
fn test_query_view_sort_field_change_enters_limit() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client1 = server.client();
        let mut client2 = server.client();

        client1.connect("sort-enters-limit-db").await;
        client2.connect("sort-enters-limit-db").await;

        // Set up 4 players - limit will be 2
        client2
            .set("/players/alice", json!({"name": "Alice", "score": 100}))
            .await
            .unwrap();
        client2
            .set("/players/bob", json!({"name": "Bob", "score": 200}))
            .await
            .unwrap();
        client2
            .set("/players/charlie", json!({"name": "Charlie", "score": 300}))
            .await
            .unwrap(); // outside limit
        client2
            .set("/players/dave", json!({"name": "Dave", "score": 400}))
            .await
            .unwrap(); // outside limit

        // Subscribe with limitToFirst(2) - should see alice and bob
        client1
            .subscribe_with_query(
                "/players",
                &["value"],
                QueryOptions {
                    order_by_child: Some("score".to_string()),
                    limit_to_first: Some(2),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Consume initial
        let initial = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();
        let initial_data: serde_json::Value = initial.value.expect("expected value").to_value();
        let initial_map = initial_data.as_object().expect("expected object");
        assert_eq!(initial_map.len(), 2, "expected 2 items in initial");

        // Change charlie's score to 50 - should now enter view, pushing bob out
        // New order in view: charlie (50), alice (100)
        client2.set("/players/charlie/score", 50).await.unwrap();

        // Should receive an atomic patch event with the swap
        let event = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        assert_eq!(
            event.event.as_deref(),
            Some("patch"),
            "expected patch event"
        );
        assert_eq!(event.path.as_deref(), Some("/"), "expected patch at '/'");

        let event_data: serde_json::Value = event.value.expect("expected value").to_value();
        let event_map = event_data.as_object().expect("expected object");

        // Patch should contain: exited 'bob' as null, entered 'charlie' with data
        assert_eq!(
            event_map.get("/bob"),
            Some(&serde_json::Value::Null),
            "'bob' should be removed (null)"
        );
        assert!(
            event_map.contains_key("/charlie"),
            "expected 'charlie' to enter view"
        );
    });
}

// =============================================================================
// Empty Query Results Tests
// =============================================================================

#[test]
fn test_query_view_empty_results_return_null() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("empty-query-db").await;

        // Set up data where no children will match the query
        client
            .set("/players/alice", json!({"name": "Alice", "score": 10}))
            .await
            .unwrap();
        client
            .set("/players/bob", json!({"name": "Bob", "score": 20}))
            .await
            .unwrap();

        // Subscribe with startAt(100) - no children have score >= 100
        client
            .subscribe_with_query(
                "/players",
                &["value"],
                QueryOptions {
                    order_by_child: Some("score".to_string()),
                    start_at: Some(json!(100)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let event = client.wait_for_event(Duration::from_secs(2)).await.unwrap();

        // Value should be null, not {}
        let value: serde_json::Value = event
            .value
            .map(|v| v.to_value())
            .unwrap_or(serde_json::Value::Null);
        assert_eq!(
            value,
            serde_json::Value::Null,
            "expected null for empty query results"
        );
    });
}

#[test]
fn test_query_view_empty_results_return_null_with_equal_to() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("empty-equalto-db").await;

        // Set up data
        client
            .set("/players/alice", json!({"name": "Alice", "team": "red"}))
            .await
            .unwrap();
        client
            .set("/players/bob", json!({"name": "Bob", "team": "blue"}))
            .await
            .unwrap();

        // Subscribe with equalTo("green") - no children have team == "green"
        client
            .subscribe_with_query(
                "/players",
                &["value"],
                QueryOptions {
                    order_by_child: Some("team".to_string()),
                    equal_to: Some(json!("green")),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let event = client.wait_for_event(Duration::from_secs(2)).await.unwrap();

        // Value should be null
        let value: serde_json::Value = event
            .value
            .map(|v| v.to_value())
            .unwrap_or(serde_json::Value::Null);
        assert_eq!(
            value,
            serde_json::Value::Null,
            "expected null for empty equalTo results"
        );
    });
}
