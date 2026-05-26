//! Incremental sort optimization integration tests.
//!
//! Tests for the sort key caching and boundary tracking optimizations
//! that enable O(1) or O(log N) updates for limited query views.
//!
//! Note: Query view boundary swaps produce atomic patch events at "/" containing
//! only the changed children: exited items as null, entered items with data.

mod common;

use common::{QueryOptions, TestServer, run_test};
use serde_json::json;
use std::time::Duration;

// =============================================================================
// Direct Swap Tests (Item enters view, boundary exits)
// =============================================================================

/// Test limitToLast direct swap: new highest item enters, oldest exits.
/// This is the common "add new chat message" case.
#[test]
fn test_limit_to_last_direct_swap_new_highest() {
    run_test(|| async {
        let server = TestServer::new();
        let mut writer = server.client();
        let mut subscriber = server.client();

        writer.connect("swap-last-db").await;
        subscriber.connect("swap-last-db").await;

        // Set up initial data with timestamps (higher = newer)
        writer
            .set("/messages/msg1", json!({"text": "first", "ts": 100}))
            .await
            .expect("Failed to set msg1");
        writer
            .set("/messages/msg2", json!({"text": "second", "ts": 200}))
            .await
            .expect("Failed to set msg2");
        writer
            .set("/messages/msg3", json!({"text": "third", "ts": 300}))
            .await
            .expect("Failed to set msg3");

        // Subscribe with limitToLast(2) orderByChild("ts") - should get msg2, msg3
        subscriber
            .subscribe_with_query(
                "/messages",
                &["value"],
                QueryOptions {
                    order_by_child: Some("ts".to_string()),
                    limit_to_last: Some(2),
                    ..Default::default()
                },
            )
            .await
            .expect("Failed to subscribe");

        // Consume initial snapshot event
        let initial = subscriber
            .wait_for_event(Duration::from_secs(2))
            .await
            .expect("Initial event");
        let initial_data = initial.value.expect("expected value").to_value();
        let initial_map = initial_data.as_object().expect("expected object");

        // Initial view should have msg2 and msg3
        assert!(
            initial_map.contains_key("msg2"),
            "Initial view should have msg2"
        );
        assert!(
            initial_map.contains_key("msg3"),
            "Initial view should have msg3"
        );
        assert!(
            !initial_map.contains_key("msg1"),
            "Initial view should NOT have msg1"
        );

        // Add a new message with highest timestamp - should enter view, msg2 should exit
        writer
            .set("/messages/msg4", json!({"text": "fourth", "ts": 400}))
            .await
            .expect("Failed to set msg4");

        // Should receive atomic patch with exited item as null, entered item with data
        let event = subscriber
            .wait_for_event(Duration::from_secs(2))
            .await
            .expect("Expected update event");

        assert_eq!(
            event.event.as_deref(),
            Some("patch"),
            "Expected patch event"
        );
        assert_eq!(event.path.as_deref(), Some("/"), "Expected patch at '/'");

        let data = event.value.expect("expected value").to_value();
        let value_map = data.as_object().expect("expected object");

        // Patch should contain: exited msg2 as null, entered msg4 with data
        assert_eq!(
            value_map.get("/msg2"),
            Some(&serde_json::Value::Null),
            "msg2 should be removed (null)"
        );
        assert!(value_map.contains_key("/msg4"), "msg4 should enter view");
        assert!(
            value_map.get("/msg4").unwrap().is_object(),
            "msg4 should have data"
        );
    });
}

/// Test limitToFirst direct swap: new lowest item enters, highest exits.
#[test]
fn test_limit_to_first_direct_swap_new_lowest() {
    run_test(|| async {
        let server = TestServer::new();
        let mut writer = server.client();
        let mut subscriber = server.client();

        writer.connect("swap-first-db").await;
        subscriber.connect("swap-first-db").await;

        // Set up initial data with scores (lower = better)
        writer
            .set("/scores/p1", json!({"name": "Player1", "score": 100}))
            .await
            .expect("Failed to set p1");
        writer
            .set("/scores/p2", json!({"name": "Player2", "score": 200}))
            .await
            .expect("Failed to set p2");
        writer
            .set("/scores/p3", json!({"name": "Player3", "score": 300}))
            .await
            .expect("Failed to set p3");

        // Subscribe with limitToFirst(2) orderByChild("score") - should get p1, p2
        subscriber
            .subscribe_with_query(
                "/scores",
                &["value"],
                QueryOptions {
                    order_by_child: Some("score".to_string()),
                    limit_to_first: Some(2),
                    ..Default::default()
                },
            )
            .await
            .expect("Failed to subscribe");

        // Consume initial event
        let initial = subscriber
            .wait_for_event(Duration::from_secs(2))
            .await
            .expect("Initial event");
        let initial_data = initial.value.expect("expected value").to_value();
        let initial_map = initial_data.as_object().expect("expected object");

        // Initial view should have p1 and p2
        assert!(
            initial_map.contains_key("p1"),
            "Initial view should have p1"
        );
        assert!(
            initial_map.contains_key("p2"),
            "Initial view should have p2"
        );

        // Add a new player with lowest score - should enter view, p2 should exit
        writer
            .set("/scores/p0", json!({"name": "Player0", "score": 50}))
            .await
            .expect("Failed to set p0");

        // Should receive atomic patch with exited item as null, entered item with data
        let event = subscriber
            .wait_for_event(Duration::from_secs(2))
            .await
            .expect("Expected update event");

        assert_eq!(
            event.event.as_deref(),
            Some("patch"),
            "Expected patch event"
        );

        let data = event.value.expect("expected value").to_value();
        let value_map = data.as_object().expect("expected object");

        // Patch should contain: exited p2 as null, entered p0 with data
        assert_eq!(
            value_map.get("/p2"),
            Some(&serde_json::Value::Null),
            "p2 should be removed (null)"
        );
        assert!(value_map.contains_key("/p0"), "p0 should enter view");
        assert!(
            value_map.get("/p0").unwrap().is_object(),
            "p0 should have data"
        );
    });
}

// =============================================================================
// Item Outside View Tests (O(1) fast path)
// =============================================================================

/// Test that changing an item outside view that doesn't beat boundary produces no events.
#[test]
fn test_item_outside_view_no_events() {
    run_test(|| async {
        let server = TestServer::new();
        let mut writer = server.client();
        let mut subscriber = server.client();

        writer.connect("outside-view-db").await;
        subscriber.connect("outside-view-db").await;

        // Set up data: msg1 (oldest), msg2, msg3 (newest)
        writer
            .set("/messages/msg1", json!({"text": "first", "ts": 100}))
            .await
            .expect("Failed to set");
        writer
            .set("/messages/msg2", json!({"text": "second", "ts": 200}))
            .await
            .expect("Failed to set");
        writer
            .set("/messages/msg3", json!({"text": "third", "ts": 300}))
            .await
            .expect("Failed to set");

        // Subscribe with limitToLast(2) - view contains msg2, msg3
        subscriber
            .subscribe_with_query(
                "/messages",
                &["value"],
                QueryOptions {
                    order_by_child: Some("ts".to_string()),
                    limit_to_last: Some(2),
                    ..Default::default()
                },
            )
            .await
            .expect("Failed to subscribe");

        // Consume initial event
        subscriber
            .wait_for_event(Duration::from_secs(2))
            .await
            .expect("Initial event");

        // Change msg1 (outside view) but keep ts low - still doesn't beat boundary
        writer
            .set(
                "/messages/msg1",
                json!({"text": "updated first", "ts": 150}),
            )
            .await
            .expect("Failed to update");

        // Should NOT receive any events since msg1 is still outside view
        let result = subscriber.wait_for_event(Duration::from_millis(500)).await;
        assert!(
            result.is_err(),
            "Expected timeout - item outside view changing shouldn't produce events"
        );
    });
}

/// Test that an item outside view can enter when it beats the boundary.
#[test]
fn test_item_outside_view_enters_when_beats_boundary() {
    run_test(|| async {
        let server = TestServer::new();
        let mut writer = server.client();
        let mut subscriber = server.client();

        writer.connect("enters-view-db").await;
        subscriber.connect("enters-view-db").await;

        // Set up data
        writer
            .set("/messages/msg1", json!({"text": "first", "ts": 100}))
            .await
            .expect("Failed to set");
        writer
            .set("/messages/msg2", json!({"text": "second", "ts": 200}))
            .await
            .expect("Failed to set");
        writer
            .set("/messages/msg3", json!({"text": "third", "ts": 300}))
            .await
            .expect("Failed to set");

        // Subscribe with limitToLast(2) - view contains msg2, msg3
        subscriber
            .subscribe_with_query(
                "/messages",
                &["value"],
                QueryOptions {
                    order_by_child: Some("ts".to_string()),
                    limit_to_last: Some(2),
                    ..Default::default()
                },
            )
            .await
            .expect("Failed to subscribe");

        // Consume initial event
        subscriber
            .wait_for_event(Duration::from_secs(2))
            .await
            .expect("Initial event");

        // Update msg1 to have highest timestamp - should now beat boundary and enter view
        writer
            .set(
                "/messages/msg1",
                json!({"text": "updated first", "ts": 400}),
            )
            .await
            .expect("Failed to update");

        // Should receive atomic patch with exited item as null, entered item with data
        let event = subscriber
            .wait_for_event(Duration::from_secs(2))
            .await
            .expect("Expected update event");

        assert_eq!(
            event.event.as_deref(),
            Some("patch"),
            "Expected patch event"
        );

        let data = event.value.expect("expected value").to_value();
        let value_map = data.as_object().expect("expected object");

        // Patch should contain: exited msg2 as null, entered msg1 with data
        assert_eq!(
            value_map.get("/msg2"),
            Some(&serde_json::Value::Null),
            "msg2 should be removed (null)"
        );
        assert!(value_map.contains_key("/msg1"), "msg1 should enter view");
        assert!(
            value_map.get("/msg1").unwrap().is_object(),
            "msg1 should have data"
        );
    });
}

// =============================================================================
// Range Constraint Tests
// =============================================================================

/// Test that items outside range constraints never produce events.
#[test]
fn test_range_constraint_item_outside_range_no_events() {
    run_test(|| async {
        let server = TestServer::new();
        let mut writer = server.client();
        let mut subscriber = server.client();

        writer.connect("range-outside-db").await;
        subscriber.connect("range-outside-db").await;

        // Set up data with scores
        writer
            .set("/scores/p1", json!({"name": "P1", "score": 50}))
            .await
            .expect("Failed to set");
        writer
            .set("/scores/p2", json!({"name": "P2", "score": 100}))
            .await
            .expect("Failed to set");
        writer
            .set("/scores/p3", json!({"name": "P3", "score": 150}))
            .await
            .expect("Failed to set");
        writer
            .set("/scores/p4", json!({"name": "P4", "score": 200}))
            .await
            .expect("Failed to set");

        // Subscribe with startAt(100) endAt(200) - p1 is out of range
        subscriber
            .subscribe_with_query(
                "/scores",
                &["value"],
                QueryOptions {
                    order_by_child: Some("score".to_string()),
                    start_at: Some(json!(100)),
                    end_at: Some(json!(200)),
                    limit_to_first: Some(2),
                    ..Default::default()
                },
            )
            .await
            .expect("Failed to subscribe");

        // Consume initial event
        subscriber
            .wait_for_event(Duration::from_secs(2))
            .await
            .expect("Initial event");

        // Change p1 (score 50, outside range) - should never produce events
        writer
            .set("/scores/p1", json!({"name": "P1 Updated", "score": 60}))
            .await
            .expect("Failed to update");

        let result = subscriber.wait_for_event(Duration::from_millis(500)).await;
        assert!(
            result.is_err(),
            "Expected timeout - item outside range shouldn't produce events"
        );
    });
}

/// Test that an item can enter the view when it comes into range and beats boundary.
#[test]
fn test_range_constraint_item_enters_range_and_view() {
    run_test(|| async {
        let server = TestServer::new();
        let mut writer = server.client();
        let mut subscriber = server.client();

        writer.connect("range-enters-db").await;
        subscriber.connect("range-enters-db").await;

        // Set up data
        writer
            .set("/scores/p1", json!({"name": "P1", "score": 50}))
            .await
            .expect("Failed to set"); // Out of range
        writer
            .set("/scores/p2", json!({"name": "P2", "score": 150}))
            .await
            .expect("Failed to set");
        writer
            .set("/scores/p3", json!({"name": "P3", "score": 200}))
            .await
            .expect("Failed to set");

        // Subscribe with startAt(100) limitToFirst(2)
        subscriber
            .subscribe_with_query(
                "/scores",
                &["value"],
                QueryOptions {
                    order_by_child: Some("score".to_string()),
                    start_at: Some(json!(100)),
                    limit_to_first: Some(2),
                    ..Default::default()
                },
            )
            .await
            .expect("Failed to subscribe");

        // Consume initial event
        let initial = subscriber
            .wait_for_event(Duration::from_secs(2))
            .await
            .expect("Initial event");
        let initial_data = initial.value.expect("expected value").to_value();
        let initial_map = initial_data.as_object().expect("expected object");

        // Initial view should have p2 and p3 (p1 is out of range)
        assert!(
            initial_map.contains_key("p2"),
            "Initial view should have p2"
        );
        assert!(
            initial_map.contains_key("p3"),
            "Initial view should have p3"
        );
        assert!(!initial_map.contains_key("p1"), "p1 should be out of range");

        // Update p1 to be in range AND beat the boundary
        writer
            .set("/scores/p1", json!({"name": "P1 Updated", "score": 120}))
            .await
            .expect("Failed to update");

        // Should receive atomic patch with exited item as null, entered item with data
        let event = subscriber
            .wait_for_event(Duration::from_secs(2))
            .await
            .expect("Expected update event");

        assert_eq!(
            event.event.as_deref(),
            Some("patch"),
            "Expected patch event"
        );

        let data = event.value.expect("expected value").to_value();
        let value_map = data.as_object().expect("expected object");

        // Patch should contain: exited p3 as null, entered p1 with data
        assert_eq!(
            value_map.get("/p3"),
            Some(&serde_json::Value::Null),
            "p3 should be removed (null)"
        );
        assert!(value_map.contains_key("/p1"), "p1 should enter view");
        assert!(
            value_map.get("/p1").unwrap().is_object(),
            "p1 should have data"
        );
    });
}

// =============================================================================
// View Not Full Tests
// =============================================================================

/// Test that items can enter when view is not yet at capacity.
#[test]
fn test_view_not_full_item_enters() {
    run_test(|| async {
        let server = TestServer::new();
        let mut writer = server.client();
        let mut subscriber = server.client();

        writer.connect("not-full-db").await;
        subscriber.connect("not-full-db").await;

        // Set up only 1 item
        writer
            .set("/items/a", json!({"val": 1}))
            .await
            .expect("Failed to set");

        // Subscribe with limit 3 (view not full)
        subscriber
            .subscribe_with_query(
                "/items",
                &["value"],
                QueryOptions {
                    order_by_child: Some("val".to_string()),
                    limit_to_first: Some(3),
                    ..Default::default()
                },
            )
            .await
            .expect("Failed to subscribe");

        // Consume initial event
        let initial = subscriber
            .wait_for_event(Duration::from_secs(2))
            .await
            .expect("Initial event");
        let initial_data = initial.value.expect("expected value").to_value();
        let initial_map = initial_data.as_object().expect("expected object");
        assert_eq!(initial_map.len(), 1, "Initial view should have 1 item");

        // Add a second item - should enter view (no swap needed, just add)
        writer
            .set("/items/b", json!({"val": 2}))
            .await
            .expect("Failed to set");

        // Should receive update with new item in view
        let event = subscriber
            .wait_for_event(Duration::from_secs(2))
            .await
            .expect("Expected add event");

        // Could be a delta update at /b or a full snapshot
        let data = event.value.expect("expected value").to_value();

        // If it's a full snapshot at "/"
        if event.path.as_deref() == Some("/") {
            let value_map = data.as_object().expect("expected object");
            assert!(value_map.contains_key("a"), "a should be in view");
            assert!(value_map.contains_key("b"), "b should be added to view");
        } else {
            // Delta update - just verify we got something for /b
            assert_eq!(event.path.as_deref(), Some("/b"), "Expected update for /b");
        }
    });
}

// =============================================================================
// Multiple Subscribers Tests
// =============================================================================

/// Test that multiple subscribers to the same query view all receive correct events.
#[test]
fn test_multiple_subscribers_receive_swap_events() {
    run_test(|| async {
        let server = TestServer::new();
        let mut writer = server.client();
        let mut sub1 = server.client();
        let mut sub2 = server.client();

        writer.connect("multi-sub-db").await;
        sub1.connect("multi-sub-db").await;
        sub2.connect("multi-sub-db").await;

        // Set up initial data
        writer
            .set("/messages/m1", json!({"ts": 100}))
            .await
            .expect("Failed to set");
        writer
            .set("/messages/m2", json!({"ts": 200}))
            .await
            .expect("Failed to set");
        writer
            .set("/messages/m3", json!({"ts": 300}))
            .await
            .expect("Failed to set");

        // Both subscribers subscribe with same query
        let query = QueryOptions {
            order_by_child: Some("ts".to_string()),
            limit_to_last: Some(2),
            ..Default::default()
        };

        sub1.subscribe_with_query("/messages", &["value"], query.clone())
            .await
            .expect("Failed to subscribe");
        sub2.subscribe_with_query("/messages", &["value"], query)
            .await
            .expect("Failed to subscribe");

        // Consume initial events for both
        sub1.wait_for_event(Duration::from_secs(2))
            .await
            .expect("Initial event");
        sub2.wait_for_event(Duration::from_secs(2))
            .await
            .expect("Initial event");

        // Writer adds new highest item
        writer
            .set("/messages/m4", json!({"ts": 400}))
            .await
            .expect("Failed to set");

        // Both subscribers should receive the update
        let event1 = sub1
            .wait_for_event(Duration::from_secs(2))
            .await
            .expect("Sub1 should receive event");
        let event2 = sub2
            .wait_for_event(Duration::from_secs(2))
            .await
            .expect("Sub2 should receive event");

        // Both should receive patch: m2 removed (null), m4 entered (data)
        let data1 = event1.value.expect("expected value").to_value();
        let data2 = event2.value.expect("expected value").to_value();

        let map1 = data1.as_object().expect("expected object");
        let map2 = data2.as_object().expect("expected object");

        assert_eq!(
            map1.get("/m2"),
            Some(&serde_json::Value::Null),
            "Sub1: m2 should be removed"
        );
        assert!(map1.contains_key("/m4"), "Sub1: m4 should enter view");
        assert_eq!(
            map2.get("/m2"),
            Some(&serde_json::Value::Null),
            "Sub2: m2 should be removed"
        );
        assert!(map2.contains_key("/m4"), "Sub2: m4 should enter view");
    });
}
