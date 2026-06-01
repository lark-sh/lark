use super::*;
use crate::db::database::SendError;
use bytes::Bytes;
use serde_json::json;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

// Counter for generating unique client IDs in tests
static MOCK_CLIENT_COUNTER: AtomicU32 = AtomicU32::new(1);

// Mock connection for testing - tracks send count
struct MockConnection {
    send_count: AtomicUsize,
    id: u32,
}

impl MockConnection {
    fn new() -> Self {
        Self {
            send_count: AtomicUsize::new(0),
            id: MOCK_CLIENT_COUNTER.fetch_add(1, Ordering::Relaxed),
        }
    }

    fn count(&self) -> usize {
        self.send_count.load(Ordering::Relaxed)
    }
}

impl ConnectionSender for MockConnection {
    fn send(
        &self,
        _data: Bytes,
        _volatile: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), SendError>> + '_>> {
        self.send_count.fetch_add(1, Ordering::Relaxed);
        Box::pin(async { Ok(()) })
    }

    fn try_send(
        &self,
        _data: Bytes,
        _volatile: bool,
        _skip_translation: bool,
    ) -> Result<(), SendError> {
        self.send_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn outbox_id(&self) -> usize {
        // All mock connections share the same "outbox" for testing
        1
    }

    fn client_id(&self) -> u32 {
        self.id
    }

    fn send_broadcast_raw(&self, payload: &[u8], _flags: u8) -> Result<(), SendError> {
        // Parse client count from payload header
        if payload.len() >= 4 {
            let client_count =
                u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
            self.send_count.fetch_add(client_count, Ordering::Relaxed);
        }
        Ok(())
    }
}

fn mock_conn() -> Arc<MockConnection> {
    Arc::new(MockConnection::new())
}

// ==========================================================================
// Basic Subscription Tests
// ==========================================================================

#[test]
fn test_subscribe_and_unsubscribe() {
    let mut vm = ViewManager::new();

    let query_id = vm
        .subscribe("client1", "/messages", None, mock_conn())
        .unwrap();
    assert_eq!(query_id, "default");
    assert_eq!(vm.view_count(), 1);

    vm.unsubscribe("client1", "/messages");
    assert_eq!(vm.view_count(), 0);
}

#[test]
fn test_subscribe_with_query() {
    let mut vm = ViewManager::new();

    let params = QueryParams {
        order_by_child: Some("score".to_string()),
        limit_to_first: Some(10),
        ..Default::default()
    };

    let query_id = vm
        .subscribe("client1", "/players", Some(&params), mock_conn())
        .unwrap();
    assert_ne!(query_id, "default");
    assert_eq!(vm.view_count(), 1);

    let view = vm.get_view("client1", "/players", &query_id).unwrap();
    assert!(view.has_query());
}

#[test]
fn test_unsubscribe_all() {
    let mut vm = ViewManager::new();

    vm.subscribe("client1", "/a", None, mock_conn()).unwrap();
    vm.subscribe("client1", "/b", None, mock_conn()).unwrap();
    vm.subscribe("client2", "/a", None, mock_conn()).unwrap();

    // With shared views: 2 views (one for /a shared by client1+client2, one for /b)
    assert_eq!(vm.view_count(), 2);
    assert_eq!(vm.subscription_count(), 3);

    vm.unsubscribe_all("client1");
    // After unsubscribe: 1 view (/a still has client2)
    assert_eq!(vm.view_count(), 1);
    assert_eq!(vm.subscription_count(), 1);
}

#[test]
fn test_subscription_cap_per_client() {
    let mut vm = ViewManager::new();

    // Fill client1 to the cap with distinct paths.
    for i in 0..MAX_SUBSCRIPTIONS_PER_CLIENT {
        let path = format!("/p{}", i);
        assert!(vm.subscribe("client1", &path, None, mock_conn()).is_ok());
    }

    // One more distinct path is rejected.
    let err = vm
        .subscribe("client1", "/overflow", None, mock_conn())
        .unwrap_err();
    assert_eq!(
        err,
        SubscribeError::TooManySubscriptions {
            limit: MAX_SUBSCRIPTIONS_PER_CLIENT
        }
    );

    // Re-subscribing to a view the client already holds is idempotent and
    // must still succeed even at the cap.
    assert!(vm.subscribe("client1", "/p0", None, mock_conn()).is_ok());

    // The cap is per-client: a different connection is unaffected.
    assert!(
        vm.subscribe("client2", "/overflow", None, mock_conn())
            .is_ok()
    );
}

// ==========================================================================
// Find Affected Views Tests
// ==========================================================================

#[test]
fn test_find_affected_views_exact_match() {
    let mut vm = ViewManager::new();
    vm.subscribe("client1", "/messages", None, mock_conn())
        .unwrap();

    let views = vm.find_affected_views("/messages", false);
    assert_eq!(views.len(), 1);
    assert_eq!(views[0].path, "/messages");
}

#[test]
fn test_find_affected_views_ancestor() {
    let mut vm = ViewManager::new();
    vm.subscribe("client1", "/messages", None, mock_conn())
        .unwrap();

    let views = vm.find_affected_views("/messages/abc/text", false);
    assert_eq!(views.len(), 1);
    assert_eq!(views[0].path, "/messages");
}

#[test]
fn test_find_affected_views_descendant() {
    let mut vm = ViewManager::new();
    vm.subscribe("client1", "/messages/abc", None, mock_conn())
        .unwrap();

    let views = vm.find_affected_views("/messages", false);
    assert_eq!(views.len(), 1);
    assert_eq!(views[0].path, "/messages/abc");
}

#[test]
fn test_find_affected_views_no_match() {
    let mut vm = ViewManager::new();
    vm.subscribe("client1", "/messages", None, mock_conn())
        .unwrap();

    let views = vm.find_affected_views("/players", false);
    assert_eq!(views.len(), 0);
}

// ==========================================================================
// Event Sending Tests
// ==========================================================================

#[test]
fn test_send_simple_put_event() {
    let mut vm = ViewManager::new();
    let conn = mock_conn();
    vm.subscribe("client1", "/messages", None, conn.clone())
        .unwrap();

    let mut tree = Tree::new();
    tree.set_str("/messages/abc", json!({"text": "hello"}));

    let event = MutationEvent {
        mutation_type: "set".to_string(),
        path: "/messages/abc".to_string(),
        old_value: None,
        new_value: Some(json!({"text": "hello"})),
        updates: None,
        volatile: false,
        writer_client_id: None,
    };

    let sent_count = vm.send_events(&event, &tree);
    assert_eq!(sent_count, 1);
    assert_eq!(conn.count(), 1);
}

// ==========================================================================
// Query View Tests
// ==========================================================================

#[test]
fn test_query_view_initialization() {
    let mut vm = ViewManager::new();

    let params = QueryParams {
        order_by_child: Some("score".to_string()),
        limit_to_first: Some(3),
        ..Default::default()
    };

    let query_id = vm
        .subscribe("client1", "/players", Some(&params), mock_conn())
        .unwrap();

    // Initialize with ordered keys
    vm.initialize_query_view(
        "client1",
        "/players",
        &query_id,
        vec!["a".to_string(), "b".to_string()],
    );

    let view = vm.get_view("client1", "/players", &query_id).unwrap();
    assert_eq!(view.ordered_keys(), vec!["a", "b"]);
}

// ==========================================================================
// Volatile Path Tests
// ==========================================================================

#[test]
fn test_volatile_path_detection() {
    let mut vm = ViewManager::new();
    vm.set_volatile_paths(vec!["cursors/$playerId".to_string()]);

    vm.subscribe("client1", "/cursors/player1", None, mock_conn())
        .unwrap();

    let view = vm
        .get_view("client1", "/cursors/player1", "default")
        .unwrap();
    assert!(view.is_volatile());
}

#[test]
fn test_non_volatile_path() {
    let mut vm = ViewManager::new();
    vm.set_volatile_paths(vec!["cursors/$playerId".to_string()]);

    vm.subscribe("client1", "/messages", None, mock_conn())
        .unwrap();

    let view = vm.get_view("client1", "/messages", "default").unwrap();
    assert!(!view.is_volatile());
}

// ==========================================================================
// QueryIdentifier Tests (ported from Go)
// ==========================================================================

#[test]
fn test_query_identifier_default() {
    // Empty query params should return "default"
    let params = QueryParams::default();
    assert_eq!(params.identifier(), "default");
}

#[test]
fn test_query_identifier_limit_to_first() {
    let params = QueryParams {
        limit_to_first: Some(10),
        ..Default::default()
    };
    let id = params.identifier();
    assert!(id.contains("\"l\":10"));
    assert!(id.contains("\"vf\":\"l\""));
}

#[test]
fn test_query_identifier_limit_to_last() {
    let params = QueryParams {
        limit_to_last: Some(5),
        ..Default::default()
    };
    let id = params.identifier();
    assert!(id.contains("\"l\":5"));
    assert!(id.contains("\"vf\":\"r\""));
}

#[test]
fn test_query_identifier_order_by_child() {
    let params = QueryParams {
        order_by_child: Some("score".to_string()),
        limit_to_last: Some(5),
        ..Default::default()
    };
    let id = params.identifier();
    assert!(id.contains("\"i\":\".score\""));
    assert!(id.contains("\"l\":5"));
}

#[test]
fn test_query_identifier_order_by_key() {
    let params = QueryParams {
        order_by: Some("key".to_string()),
        ..Default::default()
    };
    let id = params.identifier();
    assert!(id.contains("\"i\":\".key\""));
}

#[test]
fn test_query_identifier_order_by_value() {
    let params = QueryParams {
        order_by: Some("value".to_string()),
        ..Default::default()
    };
    let id = params.identifier();
    assert!(id.contains("\"i\":\".value\""));
}

#[test]
fn test_query_identifier_start_at() {
    let params = QueryParams {
        start_at: Some(json!("w")),
        ..Default::default()
    };
    let id = params.identifier();
    assert!(id.contains("\"sin\":true"));
    assert!(id.contains("\"sp\":\"w\""));
}

#[test]
fn test_query_identifier_end_at() {
    let params = QueryParams {
        end_at: Some(json!("y")),
        ..Default::default()
    };
    let id = params.identifier();
    assert!(id.contains("\"ein\":true"));
    assert!(id.contains("\"ep\":\"y\""));
}

#[test]
fn test_query_identifier_equal_to() {
    let params = QueryParams {
        equal_to: Some(json!("exact")),
        ..Default::default()
    };
    let id = params.identifier();
    // equalTo sets both start and end to the same value
    assert!(id.contains("\"sp\":\"exact\""));
    assert!(id.contains("\"ep\":\"exact\""));
}

// ==========================================================================
// Multiple Views Same Path Tests (ported from Go)
// ==========================================================================

#[test]
fn test_multiple_views_same_path() {
    let mut vm = ViewManager::new();

    // Subscribe to same path with different queries
    vm.subscribe("client1", "/users", None, mock_conn())
        .unwrap();
    vm.subscribe(
        "client1",
        "/users",
        Some(&QueryParams {
            limit_to_first: Some(5),
            ..Default::default()
        }),
        mock_conn(),
    )
    .unwrap();
    vm.subscribe(
        "client1",
        "/users",
        Some(&QueryParams {
            limit_to_last: Some(5),
            ..Default::default()
        }),
        mock_conn(),
    )
    .unwrap();

    // Should have 3 distinct views
    assert_eq!(vm.view_count(), 3);
}

#[test]
fn test_multiple_views_same_path_different_clients() {
    let mut vm = ViewManager::new();

    let query = QueryParams {
        limit_to_first: Some(10),
        ..Default::default()
    };

    // Two clients subscribe to same path with same query
    vm.subscribe("client1", "/users", Some(&query), mock_conn())
        .unwrap();
    vm.subscribe("client2", "/users", Some(&query), mock_conn())
        .unwrap();

    // With shared views: 1 view (shared by both clients)
    assert_eq!(vm.view_count(), 1);
    // But 2 total subscriptions
    assert_eq!(vm.subscription_count(), 2);
}

#[test]
fn test_unsubscribe_with_query_specific() {
    let mut vm = ViewManager::new();

    // Subscribe with multiple queries
    vm.subscribe("client1", "/users", None, mock_conn())
        .unwrap();
    let params1 = QueryParams {
        limit_to_first: Some(5),
        ..Default::default()
    };
    let query_id1 = vm
        .subscribe("client1", "/users", Some(&params1), mock_conn())
        .unwrap();
    vm.subscribe(
        "client1",
        "/users",
        Some(&QueryParams {
            limit_to_last: Some(5),
            ..Default::default()
        }),
        mock_conn(),
    )
    .unwrap();

    assert_eq!(vm.view_count(), 3);

    // Unsubscribe only the limitToFirst query
    vm.unsubscribe_with_query("client1", "/users", &query_id1);

    // Should have 2 views remaining
    assert_eq!(vm.view_count(), 2);
}

#[test]
fn test_unsubscribe_default_does_not_affect_query_views() {
    let mut vm = ViewManager::new();

    // Subscribe with default and query
    vm.subscribe("client1", "/users", None, mock_conn())
        .unwrap();
    vm.subscribe(
        "client1",
        "/users",
        Some(&QueryParams {
            limit_to_first: Some(5),
            ..Default::default()
        }),
        mock_conn(),
    )
    .unwrap();

    assert_eq!(vm.view_count(), 2);

    // Unsubscribe default (no query) using the default query ID
    vm.unsubscribe_with_query("client1", "/users", "default");

    // Should have 1 view remaining (the query view)
    assert_eq!(vm.view_count(), 1);
}

#[test]
fn test_find_affected_views_multi_query() {
    let mut vm = ViewManager::new();

    // Multiple views on same path with different queries
    vm.subscribe("client1", "/users", None, mock_conn())
        .unwrap();
    vm.subscribe(
        "client1",
        "/users",
        Some(&QueryParams {
            limit_to_first: Some(5),
            ..Default::default()
        }),
        mock_conn(),
    )
    .unwrap();
    vm.subscribe(
        "client2",
        "/users",
        Some(&QueryParams {
            limit_to_last: Some(5),
            ..Default::default()
        }),
        mock_conn(),
    )
    .unwrap();

    // Change at /users/alice should affect all 3 views
    let affected = vm.find_affected_views("/users/alice", false);
    assert_eq!(affected.len(), 3);
}

#[test]
fn test_unsubscribe_cleans_up_view() {
    let mut vm = ViewManager::new();
    vm.subscribe("client1", "/test/path", None, mock_conn())
        .unwrap();
    assert_eq!(vm.view_count(), 1);

    // Unsubscribe
    vm.unsubscribe("client1", "/test/path");

    // View should be cleaned up
    assert_eq!(vm.view_count(), 0);
}

#[test]
fn test_unsubscribe_all_cleans_up_views() {
    let mut vm = ViewManager::new();
    vm.subscribe("client1", "/test/path1", None, mock_conn())
        .unwrap();
    vm.subscribe("client1", "/test/path2", None, mock_conn())
        .unwrap();

    // Unsubscribe all
    vm.unsubscribe_all("client1");

    // All rate limit states should be cleaned up
    assert_eq!(vm.view_count(), 0);
}

// ==========================================================================
// Tag Routing Tests (ported from Go)
// ==========================================================================

#[test]
fn test_tag_stored_on_view() {
    let mut vm = ViewManager::new();

    let params = QueryParams {
        limit_to_first: Some(5),
        tag: Some(42),
        ..Default::default()
    };
    let query_id = vm
        .subscribe("client1", "/users", Some(&params), mock_conn())
        .unwrap();

    let view = vm.get_view("client1", "/users", &query_id).unwrap();
    assert_eq!(view.tag(), Some(42));
}

#[test]
fn test_tag_not_in_query_identifier() {
    // Tag should NOT affect queryIdentifier - it's just metadata for routing
    let params1 = QueryParams {
        limit_to_first: Some(5),
        tag: Some(1),
        ..Default::default()
    };
    let params2 = QueryParams {
        limit_to_first: Some(5),
        tag: Some(2),
        ..Default::default()
    };

    // Same query params with different tags should have same identifier
    assert_eq!(params1.identifier(), params2.identifier());
}

#[test]
fn test_view_without_tag() {
    let mut vm = ViewManager::new();

    let params = QueryParams {
        limit_to_first: Some(5),
        ..Default::default()
    };
    let query_id = vm
        .subscribe("client1", "/users", Some(&params), mock_conn())
        .unwrap();

    let view = vm.get_view("client1", "/users", &query_id).unwrap();
    assert_eq!(view.tag(), None);
}

// ==========================================================================
// Volatile Path Pattern Matching Tests (ported from Go)
// ==========================================================================

#[test]
fn test_matches_pattern_wildcard() {
    let mut vm = ViewManager::new();
    vm.set_volatile_paths(vec!["players/*/position".to_string()]);

    // Should match
    vm.subscribe("client1", "/players/abc/position", None, mock_conn())
        .unwrap();
    let view = vm
        .get_view("client1", "/players/abc/position", "default")
        .unwrap();
    assert!(view.is_volatile());

    // Should also match different player ID
    vm.subscribe("client2", "/players/xyz/position", None, mock_conn())
        .unwrap();
    let view2 = vm
        .get_view("client2", "/players/xyz/position", "default")
        .unwrap();
    assert!(view2.is_volatile());
}

#[test]
fn test_matches_pattern_no_match_different_end() {
    let mut vm = ViewManager::new();
    vm.set_volatile_paths(vec!["players/*/position".to_string()]);

    // Should NOT match - different ending
    vm.subscribe("client1", "/players/abc/name", None, mock_conn())
        .unwrap();
    let view = vm
        .get_view("client1", "/players/abc/name", "default")
        .unwrap();
    assert!(!view.is_volatile());
}

#[test]
fn test_matches_pattern_no_match_different_start() {
    let mut vm = ViewManager::new();
    vm.set_volatile_paths(vec!["players/*/position".to_string()]);

    // Should NOT match - different starting segment
    vm.subscribe("client1", "/other/abc/position", None, mock_conn())
        .unwrap();
    let view = vm
        .get_view("client1", "/other/abc/position", "default")
        .unwrap();
    assert!(!view.is_volatile());
}

#[test]
fn test_matches_pattern_no_match_too_short() {
    let mut vm = ViewManager::new();
    vm.set_volatile_paths(vec!["players/*/position".to_string()]);

    // Should NOT match - too few segments
    vm.subscribe("client1", "/players/abc", None, mock_conn())
        .unwrap();
    let view = vm.get_view("client1", "/players/abc", "default").unwrap();
    assert!(!view.is_volatile());
}

#[test]
fn test_matches_pattern_child_of_volatile() {
    let mut vm = ViewManager::new();
    vm.set_volatile_paths(vec!["players/*/position".to_string()]);

    // Should match - child of a volatile path (volatile cascades down)
    vm.subscribe("client1", "/players/abc/position/x", None, mock_conn())
        .unwrap();
    let view = vm
        .get_view("client1", "/players/abc/position/x", "default")
        .unwrap();
    assert!(view.is_volatile());
}

#[test]
fn test_is_fast_client() {
    // WebTransport (protocol_id 1) = fast
    assert!(Subscriber::is_fast_client("proxy_1_127.0.0.1:8080_0_42"));
    assert!(Subscriber::is_fast_client("proxy_1_10.0.0.1:443_3_1"));

    // WebSocket (protocol_id 0) = slow
    assert!(!Subscriber::is_fast_client("proxy_0_127.0.0.1:8080_0_42"));
    // REST (protocol_id 2) = slow
    assert!(!Subscriber::is_fast_client("proxy_2_127.0.0.1:8080_0_42"));
    // Unknown format = slow
    assert!(!Subscriber::is_fast_client("client1"));
}

#[test]
fn test_subscriber_fast_slow_tracking() {
    let mut vm = ViewManager::new();
    vm.set_volatile_paths(vec!["cursors/*".to_string()]);

    // Subscribe with a slow client (WebSocket, protocol 0)
    vm.subscribe("proxy_0_127.0.0.1_0_1", "/cursors", None, mock_conn())
        .unwrap();

    // Subscribe with a fast client (WebTransport, protocol 1)
    vm.subscribe("proxy_1_127.0.0.1_0_2", "/cursors", None, mock_conn())
        .unwrap();

    let view_key = ViewKey::new("/cursors", "default");
    let view = vm.shared_views.get(&view_key).unwrap();

    // Check fast/slow sets
    assert!(view.slow_subscribers.contains("proxy_0_127.0.0.1_0_1"));
    assert!(!view.fast_subscribers.contains("proxy_0_127.0.0.1_0_1"));
    assert!(view.fast_subscribers.contains("proxy_1_127.0.0.1_0_2"));
    assert!(!view.slow_subscribers.contains("proxy_1_127.0.0.1_0_2"));
}

#[test]
fn test_buffer_volatile() {
    let mut vm = ViewManager::new();
    vm.set_volatile_paths(vec!["cursors/*".to_string()]);

    // Subscribe to /cursors (parent path watching children)
    vm.subscribe("client1", "/cursors", None, mock_conn())
        .unwrap();

    // Buffer a volatile write to /cursors/player1
    let value = Bytes::from(r#"{"x": 100, "y": 200}"#);
    vm.buffer_volatile("/cursors/player1", value, "client2");

    // Check that the batch is pending
    assert!(vm.has_pending_volatile());

    // Check the view has pending data
    let view_key = ViewKey::new("/cursors", "default");
    let view = vm.shared_views.get(&view_key).unwrap();
    assert!(view.has_pending_volatile());
    assert!(view.pending_volatile_batch.contains_key("/player1"));
}

#[test]
fn test_clear_volatile_for_path_prevents_stale_flush() {
    // Simulates onDisconnect().remove() on a volatile path:
    // After clearing, the next volatile flush should NOT send stale data.
    let mut vm = ViewManager::new();
    vm.set_volatile_paths(vec!["cursors/*".to_string()]);

    let conn = mock_conn();
    vm.subscribe("proxy_1_127.0.0.1_0_1", "/cursors", None, conn.clone())
        .unwrap();

    // Buffer volatile writes for two cursors
    vm.buffer_volatile(
        "/cursors/player1",
        Bytes::from(r#"{"x":10,"y":20}"#),
        "writer1",
    );
    vm.buffer_volatile(
        "/cursors/player2",
        Bytes::from(r#"{"x":30,"y":40}"#),
        "writer2",
    );

    // Verify both are in the batch
    let view_key = ViewKey::new("/cursors", "default");
    let view = vm.shared_views.get(&view_key).unwrap();
    assert_eq!(view.pending_volatile_batch.len(), 2);
    assert!(view.pending_volatile_batch.contains_key("/player1"));
    assert!(view.pending_volatile_batch.contains_key("/player2"));

    // player1 disconnects — clear their entry from the volatile batch
    vm.clear_volatile_for_path("/cursors/player1");

    // Only player2's data should remain
    let view = vm.shared_views.get(&view_key).unwrap();
    assert_eq!(view.pending_volatile_batch.len(), 1);
    assert!(!view.pending_volatile_batch.contains_key("/player1"));
    assert!(view.pending_volatile_batch.contains_key("/player2"));

    // Flush — only player2's data should be sent, not player1's stale cursor
    let (sent, _bytes) = vm.flush_volatile_fast();
    assert_eq!(sent, 1);
    assert_eq!(conn.count(), 1);
}

#[test]
fn test_volatile_coalescing() {
    let mut vm = ViewManager::new();
    vm.set_volatile_paths(vec!["cursors/*".to_string()]);

    vm.subscribe("client1", "/cursors", None, mock_conn())
        .unwrap();

    // Multiple writes to the same path - should coalesce (latest wins)
    vm.buffer_volatile("/cursors/player1", Bytes::from(r#"{"x": 1}"#), "client2");
    vm.buffer_volatile("/cursors/player1", Bytes::from(r#"{"x": 2}"#), "client2");
    vm.buffer_volatile("/cursors/player1", Bytes::from(r#"{"x": 3}"#), "client2");

    let view_key = ViewKey::new("/cursors", "default");
    let view = vm.shared_views.get(&view_key).unwrap();
    let value = view.pending_volatile_batch.get("/player1").unwrap();
    assert_eq!(value.as_ref(), b"{\"x\": 3}");
}

#[test]
fn test_flush_volatile_fast_sends_to_fast_clients() {
    let mut vm = ViewManager::new();
    vm.set_volatile_paths(vec!["cursors/*".to_string()]);

    let slow_conn = mock_conn();
    let fast_conn = mock_conn();

    // Subscribe with slow (WebSocket, protocol 0) and fast (WebTransport, protocol 1) clients
    vm.subscribe("proxy_0_127.0.0.1_0_1", "/cursors", None, slow_conn.clone())
        .unwrap();
    vm.subscribe("proxy_1_127.0.0.1_0_2", "/cursors", None, fast_conn.clone())
        .unwrap();

    // Buffer a volatile write
    vm.buffer_volatile("/cursors/player1", Bytes::from(r#"{"x": 100}"#), "client3");

    // Flush to fast clients only
    let (sent, bytes) = vm.flush_volatile_fast();
    assert_eq!(sent, 1); // Only fast client
    assert!(bytes > 0); // Egress metered for the fast recipient

    // Fast client received, slow did not
    assert_eq!(fast_conn.count(), 1);
    assert_eq!(slow_conn.count(), 0);

    // Batch is NOT cleared (slow clients still need it)
    assert!(vm.has_pending_volatile());
}

#[test]
fn test_flush_volatile_slow_sends_and_clears() {
    let mut vm = ViewManager::new();
    vm.set_volatile_paths(vec!["cursors/*".to_string()]);

    let slow_conn = mock_conn();

    // Subscribe with slow client
    vm.subscribe("client1", "/cursors", None, slow_conn.clone())
        .unwrap();

    // Buffer a volatile write
    vm.buffer_volatile("/cursors/player1", Bytes::from(r#"{"x": 100}"#), "client2");

    // Flush to slow clients
    let (sent, bytes) = vm.flush_volatile_slow();
    assert_eq!(sent, 1);
    assert!(bytes > 0); // Egress metered for the slow recipient
    assert_eq!(slow_conn.count(), 1);

    // Batch is cleared
    assert!(!vm.has_pending_volatile());
}

#[test]
fn test_flush_volatile_encode_once() {
    let mut vm = ViewManager::new();
    vm.set_volatile_paths(vec!["cursors/*".to_string()]);

    // Subscribe with 100 fast clients
    let conns: Vec<_> = (0..100)
        .map(|i| {
            let conn = mock_conn();
            vm.subscribe(
                &format!("proxy_1_127.0.0.1_0_{}", i),
                "/cursors",
                None,
                conn.clone(),
            )
            .unwrap();
            conn
        })
        .collect();

    // Buffer a volatile write
    vm.buffer_volatile("/cursors/player1", Bytes::from(r#"{"x": 100}"#), "writer");

    // Flush to fast clients
    let (sent, bytes) = vm.flush_volatile_fast();
    assert_eq!(sent, 100);
    // Egress is metered per recipient: one encoded payload × 100 clients.
    assert!(bytes > 0);

    // With BROADCAST, one connection sends the payload with all client IDs.
    // The mock's send_broadcast_raw increments count by the number of clients.
    // Total across all connections should equal the client count.
    let total: usize = conns.iter().map(|c| c.count()).sum();
    assert_eq!(total, 100);
}
