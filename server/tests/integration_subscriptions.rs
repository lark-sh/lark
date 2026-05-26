//! Subscription integration tests.
//!
//! Tests for subscribe, events, and real-time updates.

mod common;

use common::{TestServer, run_test};
use serde_json::json;
use std::time::Duration;

// =============================================================================
// Basic Subscription Tests
// =============================================================================

#[test]
fn test_subscribe_receives_value_event() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("test-db").await;

        // Set some initial data
        client
            .set("/players/abc", json!({"name": "Alice"}))
            .await
            .expect("Failed to set");

        // Subscribe
        client
            .subscribe("/players/abc", &["value"])
            .await
            .expect("Failed to subscribe");

        // Should receive initial put event with full snapshot
        let event = client
            .wait_for_event(Duration::from_secs(2))
            .await
            .expect("Failed to receive event");

        assert_eq!(event.event.as_deref(), Some("put"));
        assert_eq!(event.subscription_path.as_deref(), Some("/players/abc"));

        // Check the value
        let value = event.value.expect("Expected value in event").to_value();
        assert_eq!(value.get("name"), Some(&json!("Alice")));
    });
}

#[test]
fn test_subscriber_receives_updates() {
    run_test(|| async {
        let server = TestServer::new();

        // Client 1: subscriber
        let mut client1 = server.client();
        // Client 2: writer
        let mut client2 = server.client();

        // Both join the same database
        client1.connect("shared-db").await;
        client2.connect("shared-db").await;

        // Client 1 subscribes
        client1
            .subscribe("/players", &["value"])
            .await
            .expect("Failed to subscribe");

        // Consume the initial put event (null since path doesn't exist yet)
        let _initial = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .expect("Failed to receive initial event");

        // Client 2 sets data
        client2
            .set("/players/xyz", json!({"name": "Alex"}))
            .await
            .expect("Client2 failed to set");

        // Client 1 should receive a put event with delta
        let event = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .expect("Client1 failed to receive event");

        // Should be a put event
        assert_eq!(event.event.as_deref(), Some("put"));
    });
}

#[test]
fn test_write_echo_arrives_before_ack() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("echo-ordering-test").await;

        // Subscribe to a path
        client
            .subscribe("/test", &["value"])
            .await
            .expect("Failed to subscribe");

        // Wait for initial snapshot
        let _ = client.wait_for_event(Duration::from_secs(1)).await;

        // Clear raw messages buffer to start fresh
        client.clear_raw_messages().await;

        // Now write to the path we're subscribed to
        client
            .set("/test", json!({"value": 42}))
            .await
            .expect("Failed to set");

        // Small delay to ensure all messages arrive
        glommio::timer::sleep(Duration::from_millis(50)).await;

        // Get raw messages in order
        let msgs = client.get_raw_messages().await;

        // Find the PUT event and ACK for our write
        let mut put_index: Option<usize> = None;
        let mut ack_index: Option<usize> = None;

        for (i, msg) in msgs.iter().enumerate() {
            if msg.event.as_deref() == Some("put")
                && msg.subscription_path.as_deref() == Some("/test")
            {
                put_index = Some(i);
            }
            if msg.ack.is_some() {
                ack_index = Some(i);
            }
        }

        let put_idx = put_index.expect("Did not receive PUT event for write echo");
        let ack_idx = ack_index.expect("Did not receive ACK for write");

        // The PUT event must arrive BEFORE the ACK
        assert!(
            put_idx < ack_idx,
            "Write echo arrived AFTER ACK (put index={}, ack index={}). \
             Expected event before ACK for correct optimistic UI behavior.",
            put_idx,
            ack_idx
        );
    });
}

// =============================================================================
// Subscription Edge Cases
// =============================================================================

#[test]
fn test_subscribe_to_nonexistent_path() {
    run_test(|| async {
        let server = TestServer::new();
        let mut client = server.client();

        client.connect("test-db").await;

        // Subscribe to a path that doesn't exist
        client
            .subscribe("/does/not/exist", &["value"])
            .await
            .expect("Failed to subscribe");

        // Should receive initial put event with null value
        let event = client
            .wait_for_event(Duration::from_secs(2))
            .await
            .expect("Failed to receive event");

        assert_eq!(event.event.as_deref(), Some("put"));
        // Value should be null for nonexistent path
        assert!(
            event.value.is_none() || event.value.as_ref().map(|v| v.is_null()).unwrap_or(false)
        );
    });
}

#[test]
fn test_multiple_subscribers_same_path() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client1 = server.client();
        let mut client2 = server.client();
        let mut writer = server.client();

        client1.connect("multi-sub-db").await;
        client2.connect("multi-sub-db").await;
        writer.connect("multi-sub-db").await;

        // Both subscribe to the same path
        client1
            .subscribe("/data", &["value"])
            .await
            .expect("Client1 failed to subscribe");
        client2
            .subscribe("/data", &["value"])
            .await
            .expect("Client2 failed to subscribe");

        // Consume initial events
        let _ = client1.wait_for_event(Duration::from_secs(1)).await;
        let _ = client2.wait_for_event(Duration::from_secs(1)).await;

        // Writer sets data
        writer
            .set("/data", json!({"test": true}))
            .await
            .expect("Writer failed to set");

        // Both subscribers should receive the update
        let event1 = client1
            .wait_for_event(Duration::from_secs(2))
            .await
            .expect("Client1 failed to receive update");
        let event2 = client2
            .wait_for_event(Duration::from_secs(2))
            .await
            .expect("Client2 failed to receive update");

        assert_eq!(event1.event.as_deref(), Some("put"));
        assert_eq!(event2.event.as_deref(), Some("put"));
    });
}

#[test]
fn test_unsubscribe_stops_events() {
    run_test(|| async {
        let server = TestServer::new();

        let mut subscriber = server.client();
        let mut writer = server.client();

        subscriber.connect("unsub-db").await;
        writer.connect("unsub-db").await;

        // Subscribe
        subscriber
            .subscribe("/data", &["value"])
            .await
            .expect("Failed to subscribe");

        // Consume initial event
        let _ = subscriber.wait_for_event(Duration::from_secs(1)).await;

        // Unsubscribe
        subscriber
            .unsubscribe("/data")
            .await
            .expect("Failed to unsubscribe");

        // Clear any pending events
        subscriber.clear_events().await;

        // Writer sets data
        writer
            .set("/data", json!({"after": "unsubscribe"}))
            .await
            .expect("Writer failed to set");

        // Wait a bit for any potential events
        glommio::timer::sleep(Duration::from_millis(100)).await;

        // Subscriber should NOT have received the event
        let events = subscriber.events().await;
        assert!(
            events.is_empty(),
            "Should not receive events after unsubscribe, got {} events",
            events.len()
        );
    });
}

#[test]
fn test_subscribe_nested_paths() {
    run_test(|| async {
        let server = TestServer::new();

        let mut parent_sub = server.client();
        let mut child_sub = server.client();
        let mut writer = server.client();

        parent_sub.connect("nested-db").await;
        child_sub.connect("nested-db").await;
        writer.connect("nested-db").await;

        // Subscribe to parent path
        parent_sub
            .subscribe("/users", &["value"])
            .await
            .expect("Failed to subscribe to parent");

        // Subscribe to child path
        child_sub
            .subscribe("/users/alice", &["value"])
            .await
            .expect("Failed to subscribe to child");

        // Consume initial events
        let _ = parent_sub.wait_for_event(Duration::from_secs(1)).await;
        let _ = child_sub.wait_for_event(Duration::from_secs(1)).await;

        // Write to a different child
        writer
            .set("/users/bob", json!({"name": "Bob"}))
            .await
            .expect("Writer failed to set");

        // Parent subscriber should receive update (since it's watching /users)
        let parent_event = parent_sub
            .wait_for_event(Duration::from_secs(2))
            .await
            .expect("Parent subscriber failed to receive update");
        assert_eq!(parent_event.event.as_deref(), Some("put"));

        // Child subscriber should NOT receive update (watching /users/alice, not /users/bob)
        glommio::timer::sleep(Duration::from_millis(100)).await;
        let child_events = child_sub.events().await;
        assert!(
            child_events.is_empty(),
            "Child subscriber should not receive events for sibling path"
        );
    });
}

#[test]
fn test_subscriber_receives_remove_event() {
    run_test(|| async {
        let server = TestServer::new();

        let mut subscriber = server.client();
        let mut writer = server.client();

        subscriber.connect("remove-event-db").await;
        writer.connect("remove-event-db").await;

        // Set initial data
        writer
            .set("/data", json!({"value": 123}))
            .await
            .expect("Failed to set initial data");

        // Subscribe
        subscriber
            .subscribe("/data", &["value"])
            .await
            .expect("Failed to subscribe");

        // Consume initial event
        let initial = subscriber
            .wait_for_event(Duration::from_secs(1))
            .await
            .expect("Failed to receive initial event");
        assert_eq!(initial.event.as_deref(), Some("put"));

        // Remove the data
        writer.remove("/data").await.expect("Failed to remove");

        // Subscriber should receive a put event with null value
        let remove_event = subscriber
            .wait_for_event(Duration::from_secs(2))
            .await
            .expect("Failed to receive remove event");

        assert_eq!(remove_event.event.as_deref(), Some("put"));
        // Value should be null after removal
        assert!(
            remove_event.value.is_none()
                || remove_event
                    .value
                    .as_ref()
                    .map(|v| v.is_null())
                    .unwrap_or(false),
            "Expected null value after remove, got {:?}",
            remove_event.value
        );
    });
}
