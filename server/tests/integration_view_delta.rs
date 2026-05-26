//! View delta tests.
//!
//! These test delta event generation for subscriptions:
//! - Delta-only verification (sends only changed data)
//! - Priority and value ordering
//! - Score/sort field change events
//! - Event type filtering (child_added, child_changed, child_removed)

mod common;

use common::{QueryOptions, TestServer, run_test};
use serde_json::json;
use std::time::Duration;

// =============================================================================
// Delta-Only Verification Tests
// =============================================================================

#[test]
fn test_simple_view_delta_value_only() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client1 = server.client();
        let mut client2 = server.client();

        client1.connect("delta-simple-db").await;
        client2.connect("delta-simple-db").await;

        // Set up initial data with multiple fields
        client2
            .set(
                "/players/alice",
                json!({
                    "name": "Alice",
                    "score": 100,
                    "level": 5,
                    "stats": {"wins": 10, "losses": 2}
                }),
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

        // Update just the score field
        client2.set("/players/alice/score", 150).await.unwrap();

        // Should receive delta event
        let event = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        // Verify it's a PUT event with relative path
        assert_eq!(event.event.as_deref(), Some("put"));
        assert_eq!(event.path.as_deref(), Some("/alice/score"));

        // Value should be ONLY the score (150), not the full alice object
        let value = event.value.expect("expected value").to_value();
        assert_eq!(
            value,
            json!(150),
            "expected delta value only (150), got {:?}",
            value
        );
    });
}

#[test]
fn test_query_view_delta_value_only() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client1 = server.client();
        let mut client2 = server.client();

        client1.connect("delta-query-db").await;
        client2.connect("delta-query-db").await;

        // Set up data - alice and bob with scores
        client2
            .set(
                "/players/alice",
                json!({"name": "Alice", "score": 100, "level": 5}),
            )
            .await
            .unwrap();
        client2
            .set(
                "/players/bob",
                json!({"name": "Bob", "score": 200, "level": 3}),
            )
            .await
            .unwrap();

        // Subscribe with orderByChild("score") limitToFirst(2)
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

        // Consume initial event
        client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        // Update alice's NAME (not the sort field) - should just send the delta
        client2
            .set("/players/alice/name", "ALICE UPDATED")
            .await
            .unwrap();

        let event = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        // Should be PUT with delta path and delta value
        assert_eq!(event.event.as_deref(), Some("put"));
        assert_eq!(event.path.as_deref(), Some("/alice/name"));

        // Value should be just "ALICE UPDATED", not the full alice object
        let value = event.value.expect("expected value").to_value();
        assert_eq!(
            value,
            json!("ALICE UPDATED"),
            "expected delta value only, got {:?}",
            value
        );
    });
}

// =============================================================================
// Priority Ordering Tests
// =============================================================================

#[test]
fn test_query_view_order_by_priority() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("priority-order-db").await;

        // Set up items with priorities
        client
            .set_with_priority("/items/a", json!({"name": "alpha"}), 3.0)
            .await
            .unwrap();
        client
            .set_with_priority("/items/b", json!({"name": "bravo"}), 1.0)
            .await
            .unwrap();
        client
            .set_with_priority("/items/c", json!({"name": "charlie"}), 2.0)
            .await
            .unwrap();

        // Subscribe with orderByPriority + limitToFirst(2)
        // Should get b (priority 1) and c (priority 2)
        client
            .subscribe_with_query(
                "/items",
                &["value"],
                QueryOptions {
                    order_by: Some("priority".to_string()),
                    limit_to_first: Some(2),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let event = client.wait_for_event(Duration::from_secs(2)).await.unwrap();

        let data = event.value.expect("expected value").to_value();
        let data_map = data.as_object().expect("expected object");

        assert_eq!(data_map.len(), 2, "expected 2 items");
        assert!(
            data_map.contains_key("b"),
            "expected 'b' (priority 1) in result"
        );
        assert!(
            data_map.contains_key("c"),
            "expected 'c' (priority 2) in result"
        );
    });
}

#[test]
fn test_query_view_priority_change_causes_data_event() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client1 = server.client();
        let mut client2 = server.client();

        client1.connect("priority-move-db").await;
        client2.connect("priority-move-db").await;

        // Set up items with priorities
        client2
            .set_with_priority("/items/a", json!({"name": "alpha"}), 1.0)
            .await
            .unwrap();
        client2
            .set_with_priority("/items/b", json!({"name": "bravo"}), 2.0)
            .await
            .unwrap();
        client2
            .set_with_priority("/items/c", json!({"name": "charlie"}), 3.0)
            .await
            .unwrap();

        // Subscribe with orderByPriority
        client1
            .subscribe_with_query(
                "/items",
                &["value"],
                QueryOptions {
                    order_by: Some("priority".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Consume initial event - should have all 3 items
        let initial = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();
        let initial_data = initial.value.expect("expected value");
        let initial_map = initial_data.as_object().expect("expected object");
        assert_eq!(initial_map.len(), 3, "expected 3 items initially");

        // Change 'a' priority to 2.5 - should now be between b and c
        client2
            .set_with_priority("/items/a", json!({"name": "alpha"}), 2.5)
            .await
            .unwrap();

        // Should receive a PATCH or PUT with the data change so client can re-sort
        let event = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        // Verify the event contains the changed data (client uses this to re-sort)
        assert!(
            event.event.as_deref() == Some("patch") || event.event.as_deref() == Some("put"),
            "expected patch or put event, got {:?}",
            event.event
        );
    });
}

// =============================================================================
// Value Ordering Tests
// =============================================================================

#[test]
fn test_query_view_order_by_value() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("value-order-db").await;

        // Set up items as direct values (not objects)
        client.set("/scores/alice", 150).await.unwrap();
        client.set("/scores/bob", 50).await.unwrap();
        client.set("/scores/charlie", 100).await.unwrap();

        // Subscribe with orderByValue + limitToFirst(2)
        // Should get bob (50) and charlie (100)
        client
            .subscribe_with_query(
                "/scores",
                &["value"],
                QueryOptions {
                    order_by: Some("value".to_string()),
                    limit_to_first: Some(2),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let event = client.wait_for_event(Duration::from_secs(2)).await.unwrap();

        let data = event.value.expect("expected value").to_value();
        let data_map = data.as_object().expect("expected object");

        assert_eq!(data_map.len(), 2, "expected 2 items");
        assert!(
            data_map.contains_key("bob"),
            "expected 'bob' (value 50) in result"
        );
        assert!(
            data_map.contains_key("charlie"),
            "expected 'charlie' (value 100) in result"
        );
    });
}

// =============================================================================
// Score Change / Data Event Tests
// =============================================================================

#[test]
fn test_query_view_score_change_sends_data() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client1 = server.client();
        let mut client2 = server.client();

        client1.connect("shuffle-db").await;
        client2.connect("shuffle-db").await;

        // Set up 4 players with scores
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
        client2
            .set("/players/dave", json!({"name": "Dave", "score": 400}))
            .await
            .unwrap();

        // Subscribe with orderByChild("score")
        client1
            .subscribe_with_query(
                "/players",
                &["value"],
                QueryOptions {
                    order_by_child: Some("score".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Consume initial event
        let initial = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();
        let initial_data = initial.value.expect("expected value");
        let initial_map = initial_data.as_object().expect("expected object");
        assert_eq!(initial_map.len(), 4, "expected 4 players initially");

        // Change alice's score to 350 - client will re-sort to detect move
        client2.set("/players/alice/score", 350).await.unwrap();

        // Should receive PATCH with the score change
        let event = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        assert_eq!(event.event.as_deref(), Some("patch"));

        // Parse the patch data - should contain alice's new score
        let patch_data = event.value.expect("expected value").to_value();
        let patch_map = patch_data.as_object().expect("expected object");

        assert!(
            patch_map.contains_key("/alice/score"),
            "expected /alice/score in patch data"
        );
        assert_eq!(patch_map.get("/alice/score"), Some(&json!(350)));
    });
}

#[test]
fn test_query_view_move_data_only_for_trigger() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client1 = server.client();
        let mut client2 = server.client();

        client1.connect("move-data-db").await;
        client2.connect("move-data-db").await;

        // Set up players
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

        // Subscribe
        client1
            .subscribe_with_query(
                "/players",
                &["value"],
                QueryOptions {
                    order_by_child: Some("score".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Consume initial
        client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        // Change alice's score - she moves
        client2.set("/players/alice/score", 250).await.unwrap();

        // Find the PATCH event with alice's data
        let event = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        assert_eq!(event.event.as_deref(), Some("patch"));

        // Parse the patch data
        let patch_data = event.value.expect("expected value").to_value();
        let patch_map = patch_data.as_object().expect("expected object");

        // Should have /alice/score
        assert!(
            patch_map.contains_key("/alice/score"),
            "expected /alice/score in patch data"
        );

        // Should NOT have bob or charlie data
        for key in patch_map.keys() {
            assert!(
                key != "/bob/score" && key != "/charlie/score",
                "unexpected data for non-trigger item: {}",
                key
            );
        }
    });
}

// =============================================================================
// Update on Query View Tests
// =============================================================================

#[test]
fn test_query_view_update_generates_patch() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client1 = server.client();
        let mut client2 = server.client();

        client1.connect("query-update-patch-db").await;
        client2.connect("query-update-patch-db").await;

        // Set up data
        client2
            .set(
                "/players/alice",
                json!({"name": "Alice", "score": 100, "level": 1}),
            )
            .await
            .unwrap();
        client2
            .set(
                "/players/bob",
                json!({"name": "Bob", "score": 200, "level": 2}),
            )
            .await
            .unwrap();

        // Subscribe with query
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
        client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        // Update alice with multiple non-sort fields
        client2
            .update("/players/alice", json!({"level": 5, "badge": "gold"}))
            .await
            .unwrap();

        // Should receive PATCH event
        let event = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        assert_eq!(
            event.event.as_deref(),
            Some("patch"),
            "expected 'patch' event for update()"
        );

        // Parse and verify only the updated fields are present
        let patch_data = event.value.expect("expected value").to_value();
        let patch_map = patch_data.as_object().expect("expected object");

        assert_eq!(patch_map.len(), 2, "expected 2 fields in patch");
        assert!(
            patch_map.contains_key("/alice/level"),
            "expected /alice/level in patch data"
        );
        assert!(
            patch_map.contains_key("/alice/badge"),
            "expected /alice/badge in patch data"
        );
    });
}

#[test]
fn test_query_view_sort_field_update_sends_data() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client1 = server.client();
        let mut client2 = server.client();

        client1.connect("sort-update-move-db").await;
        client2.connect("sort-update-move-db").await;

        // Set up players
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

        // Subscribe
        client1
            .subscribe_with_query(
                "/players",
                &["value"],
                QueryOptions {
                    order_by_child: Some("score".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Consume initial
        client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        // Use update() to change alice's score (sort field)
        client2
            .update("/players/alice", json!({"score": 250}))
            .await
            .unwrap();

        // Should receive PATCH with alice's new score
        let event = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        assert_eq!(event.event.as_deref(), Some("patch"));

        // Verify alice's score is in the patch
        let patch_data = event.value.expect("expected value").to_value();
        let patch_map = patch_data.as_object().expect("expected object");

        assert!(
            patch_map.contains_key("/alice/score"),
            "expected /alice/score in patch data"
        );
        assert_eq!(patch_map.get("/alice/score"), Some(&json!(250)));
    });
}

// =============================================================================
// Nested orderByChild Tests
// =============================================================================

#[test]
fn test_query_view_nested_order_by_child_move() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client1 = server.client();
        let mut client2 = server.client();

        client1.connect("nested-move-db").await;
        client2.connect("nested-move-db").await;

        // Set up players with nested stats
        client2
            .set(
                "/players/alice",
                json!({"name": "Alice", "stats": {"score": 100}}),
            )
            .await
            .unwrap();
        client2
            .set(
                "/players/bob",
                json!({"name": "Bob", "stats": {"score": 200}}),
            )
            .await
            .unwrap();

        // Subscribe with orderByChild("stats/score")
        client1
            .subscribe_with_query(
                "/players",
                &["value"],
                QueryOptions {
                    order_by_child: Some("stats/score".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Consume initial - should have alice and bob
        client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        // Change alice's nested score to 300 - should move after bob
        client2
            .set("/players/alice/stats/score", 300)
            .await
            .unwrap();

        // Should receive event with alice's new score (client re-sorts to detect move)
        let event = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        // Should be a PATCH or PUT with the score change
        assert!(
            event.event.as_deref() == Some("patch") || event.event.as_deref() == Some("put"),
            "expected patch or put event, got {:?}",
            event.event
        );
    });
}

// =============================================================================
// Event Type Filtering Tests
// Note: Server-side event filtering has been removed. Clients now filter events
// locally based on their registered callback.
// =============================================================================

#[test]
fn test_event_type_filtering_set_null_is_removal() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client1 = server.client();
        let mut client2 = server.client();

        client1.connect("set-null-remove-db").await;
        client2.connect("set-null-remove-db").await;

        // Set up initial data
        client2
            .set("/items/toRemove", json!({"data": "test"}))
            .await
            .unwrap();
        client2
            .set("/items/toKeep", json!({"data": "keep"}))
            .await
            .unwrap();

        // Client 1 subscribes to child_removed ONLY
        client1
            .subscribe("/items", &["child_removed"])
            .await
            .unwrap();

        // Consume initial event
        client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        // Use set(null) to delete - this should be treated as a removal
        client2
            .set("/items/toRemove", serde_json::Value::Null)
            .await
            .unwrap();

        // Client 1 SHOULD receive this as a child_removed event
        let event = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(event.path.as_deref(), Some("/toRemove"));

        // Value should be null
        let value = event
            .value
            .map(|v| v.to_value())
            .unwrap_or(serde_json::Value::Null);
        assert_eq!(value, serde_json::Value::Null);
    });
}

#[test]
fn test_event_type_filtering_value_gets_everything() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client1 = server.client();
        let mut client2 = server.client();

        client1.connect("event-filter-value-db").await;
        client2.connect("event-filter-value-db").await;

        // Set up initial data
        client2
            .set("/messages/msg1", json!({"text": "Hello"}))
            .await
            .unwrap();

        // Client 1 subscribes to "value" (should get everything)
        client1.subscribe("/messages", &["value"]).await.unwrap();

        // Consume initial event
        client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        // Add a new message
        client2
            .set("/messages/msg2", json!({"text": "New"}))
            .await
            .unwrap();
        let event1 = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(event1.path.as_deref(), Some("/msg2"));

        // Change existing message
        client2.set("/messages/msg1/text", "Updated").await.unwrap();
        let event2 = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(event2.path.as_deref(), Some("/msg1/text"));

        // Remove message
        client2.remove("/messages/msg2").await.unwrap();
        let event3 = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(event3.path.as_deref(), Some("/msg2"));
    });
}

// =============================================================================
// Sort Field Change Without Position Change Tests
// =============================================================================

#[test]
fn test_query_view_sort_field_change_no_move_still_fires_change() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client1 = server.client();
        let mut client2 = server.client();

        client1.connect("sort-no-move-db").await;
        client2.connect("sort-no-move-db").await;

        // Set up players with scores far apart so small changes don't cause moves
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

        // Subscribe with orderByChild("score")
        client1
            .subscribe_with_query(
                "/players",
                &["value"],
                QueryOptions {
                    order_by_child: Some("score".to_string()),
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

        // Change bob's score from 200 to 210 - order stays the same (still between alice and charlie)
        client2.set("/players/bob/score", 210).await.unwrap();

        // Should receive a PATCH event with the score change (child_changed)
        // even though the order didn't change
        let event = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        // Should be a PATCH with the score update
        assert_eq!(event.event.as_deref(), Some("patch"));

        // Parse patch data - should contain /bob/score
        let patch_data = event.value.expect("expected value").to_value();
        let patch_map = patch_data.as_object().expect("expected object");

        assert!(
            patch_map.contains_key("/bob/score"),
            "expected /bob/score in patch data, got {:?}",
            patch_map
        );
    });
}

#[test]
fn test_query_view_sort_field_change_no_move_with_event_filter() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client1 = server.client();
        let mut client2 = server.client();

        client1.connect("sort-no-move-filter-db").await;
        client2.connect("sort-no-move-filter-db").await;

        // Set up players
        client2
            .set("/players/alice", json!({"name": "Alice", "score": 100}))
            .await
            .unwrap();
        client2
            .set("/players/bob", json!({"name": "Bob", "score": 200}))
            .await
            .unwrap();

        // Subscribe with orderByChild("score") and only child_changed event
        client1
            .subscribe_with_query(
                "/players",
                &["child_changed"],
                QueryOptions {
                    order_by_child: Some("score".to_string()),
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

        // Change bob's score slightly - no position change
        client2.set("/players/bob/score", 205).await.unwrap();

        // Should receive the change event
        let event = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .unwrap();

        // Verify it's about bob's score
        let patch_data = event.value.expect("expected value").to_value();
        let patch_map = patch_data.as_object().expect("expected object");

        assert!(
            patch_map.contains_key("/bob/score"),
            "expected /bob/score in change event, got {:?}",
            patch_map
        );
    });
}
