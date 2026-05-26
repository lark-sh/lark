//! Volatile path integration tests.
//!
//! Tests for volatile writes (high-frequency, non-persisted updates).

mod common;

use common::{TestServer, run_test};
use serde_json::{Value, json};
use std::time::Duration;

// =============================================================================
// Volatile Write Tests
// =============================================================================

#[test]
fn test_volatile_write_does_not_persist() {
    run_test(|| async {
        let server = TestServer::new();

        // Set up rules with volatile path
        server
            .set_rules(
                "volatile-db",
                json!({
                    "rules": {
                        "players": {
                            "$uid": {
                                "position": {
                                    ".read": true,
                                    ".write": true,
                                    ".volatile": true
                                }
                            }
                        },
                        ".read": true,
                        ".write": true
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("volatile-db").await;

        // Volatile write should succeed
        client
            .set_volatile("/players/p1/position", json!({"x": 1, "y": 2, "z": 3}))
            .await
            .expect("Volatile write should succeed");

        // Give it time to process
        glommio::timer::sleep(Duration::from_millis(50)).await;

        // Volatile writes do NOT persist to tree - they are pure relay/pub-sub.
        // Reading the value back should return null (no data stored).
        let value = client
            .once("/players/p1/position")
            .await
            .expect("Failed to read");

        assert_eq!(
            value,
            Value::Null,
            "Volatile writes should not persist to tree"
        );
    });
}

#[test]
fn test_volatile_write_received_by_subscribers() {
    run_test(|| async {
        let server = TestServer::new();

        // Set up rules with volatile path
        server
            .set_rules(
                "volatile-sub-db",
                json!({
                    "rules": {
                        "players": {
                            "$uid": {
                                "position": {
                                    ".read": true,
                                    ".write": true,
                                    ".volatile": true
                                }
                            }
                        },
                        ".read": true,
                        ".write": true
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut writer = server.client();
        let mut subscriber = server.client();

        writer.connect("volatile-sub-db").await;
        subscriber.connect("volatile-sub-db").await;

        // Subscriber subscribes to the volatile path
        subscriber
            .subscribe("/players/p1/position", &["value"])
            .await
            .expect("Failed to subscribe");

        // Consume initial event (null since path doesn't exist)
        let _ = subscriber.wait_for_event(Duration::from_secs(1)).await;
        subscriber.clear_events().await;

        // Writer sends volatile update
        writer
            .set_volatile("/players/p1/position", json!({"x": 10, "y": 20}))
            .await
            .expect("Volatile write should succeed");

        // Wait for volatile batch (slow clients flush every ~333ms)
        // Need to wait at least 400ms to ensure the flush happens
        glommio::timer::sleep(Duration::from_millis(400)).await;

        // Subscriber should receive the volatile update
        let events = subscriber.events().await;

        // Should have received at least one event (volatile batch)
        assert!(
            !events.is_empty(),
            "Subscriber should receive volatile updates"
        );
    });
}

#[test]
fn test_volatile_write_not_echoed_to_sender() {
    run_test(|| async {
        let server = TestServer::new();

        // Set up rules with volatile path
        server
            .set_rules(
                "volatile-echo-db",
                json!({
                    "rules": {
                        "players": {
                            "$uid": {
                                "position": {
                                    ".read": true,
                                    ".write": true,
                                    ".volatile": true
                                }
                            }
                        },
                        ".read": true,
                        ".write": true
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut client = server.client();
        client.connect("volatile-echo-db").await;

        // Subscribe to the path we'll write to
        client
            .subscribe("/players/me/pos", &["value"])
            .await
            .expect("Failed to subscribe");

        // Consume initial event
        let _ = client.wait_for_event(Duration::from_secs(1)).await;
        client.clear_events().await;

        // Send volatile update to the path we're subscribed to
        client
            .set_volatile("/players/me/pos", json!({"x": 10}))
            .await
            .expect("Volatile write should succeed");

        // Wait for any batched volatile events (slow clients flush every ~333ms)
        glommio::timer::sleep(Duration::from_millis(400)).await;

        // We should NOT receive our own volatile event back
        let events = client.events().await;

        // Filter for volatile patch events that contain our path
        // New format: {"ev": "patch", "sp": "/players/me/pos", "p": "/", "v": {...}, "x": true}
        let own_echoes: Vec<_> = events
            .iter()
            .filter(|e| {
                // Check if this is a volatile patch event for our subscription path
                e.event.as_deref() == Some("patch")
                    && e.volatile.unwrap_or(false)
                    && e.subscription_path.as_deref() == Some("/players/me/pos")
            })
            .collect();

        assert!(
            own_echoes.is_empty(),
            "Should not receive own volatile event back, got {} echoes",
            own_echoes.len()
        );
    });
}

#[test]
fn test_volatile_write_to_child_received_by_parent_subscriber() {
    run_test(|| async {
        let server = TestServer::new();

        // Set up rules with volatile path at the child level
        server
            .set_rules(
                "volatile-parent-db",
                json!({
                    "rules": {
                        "cursors": {
                            "$playerId": {
                                ".read": true,
                                ".write": true,
                                ".volatile": true
                            }
                        },
                        ".read": true,
                        ".write": true
                    }
                }),
            )
            .expect("Failed to set rules");

        let mut writer = server.client();
        let mut subscriber = server.client();

        writer.connect("volatile-parent-db").await;
        subscriber.connect("volatile-parent-db").await;

        // Subscriber subscribes to the PARENT path (like loadtest does with /cursors)
        subscriber
            .subscribe("/cursors", &["value"])
            .await
            .expect("Failed to subscribe");

        // Consume initial event (null since path doesn't exist)
        let _ = subscriber.wait_for_event(Duration::from_secs(1)).await;
        subscriber.clear_events().await;

        // Writer sends volatile update to a CHILD path
        writer
            .set_volatile("/cursors/player1", json!({"x": 100, "y": 200}))
            .await
            .expect("Volatile write should succeed");

        // Wait for volatile batch flush (slow clients flush every ~333ms)
        glommio::timer::sleep(Duration::from_millis(400)).await;

        // Subscriber should receive the volatile update even though they subscribed to parent
        let events = subscriber.events().await;

        assert!(
            !events.is_empty(),
            "Subscriber to parent path should receive volatile writes to child paths"
        );

        // Verify the event contains the cursor data
        // New format: {"ev": "patch", "sp": "/cursors", "p": "/", "v": {"/player1": {x, y}}, "x": true}
        let has_cursor_event = events.iter().any(|e| {
            // Check if this is a volatile patch event for /cursors containing /player1
            e.event.as_deref() == Some("patch")
                && e.volatile.unwrap_or(false)
                && e.subscription_path.as_deref() == Some("/cursors")
                && e.value.as_ref().is_some_and(|v| {
                    v.as_value()
                        .and_then(|v| v.as_object())
                        .is_some_and(|obj| obj.contains_key("/player1"))
                })
        });

        assert!(
            has_cursor_event,
            "Should receive cursor update for player1, got events: {:?}",
            events
        );
    });
}

#[test]
fn test_regular_write_size_limit() {
    run_test(|| async {
        let server = TestServer::new();

        let mut client = server.client();
        client.connect("size-limit-db").await;

        // Normal-sized write should succeed
        client
            .set("/test", "small value")
            .await
            .expect("Small write should succeed");

        // Verify it worked
        let value = client.once("/test").await.expect("Failed to read");
        assert_eq!(value, json!("small value"));
    });
}
