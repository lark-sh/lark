use super::*;
use serde_json::json;
use std::sync::Mutex;

#[test]
fn test_validate_value_keys() {
    // Plain nested data is fine.
    assert!(validate_value_keys(&json!({"a": {"b": [1, 2, {"c": 3}]}})).is_ok());
    // Server-value / priority sentinels pass (leading-dot keys allowed).
    assert!(validate_value_keys(&json!({"createdAt": {".sv": "timestamp"}})).is_ok());
    assert!(validate_value_keys(&json!({".priority": 5, "name": "x"})).is_ok());
    // A literal slash in an object key would become an unaddressable storage
    // key — reject it (Firebase rejects it too).
    assert!(validate_value_keys(&json!({"a/b": 1})).is_err());
    // Other forbidden key chars, nested, are caught by the recursion.
    assert!(validate_value_keys(&json!({"ok": {"bad$key": 1}})).is_err());
    assert!(validate_value_keys(&json!({"arr": [{"in.mid": 1}]})).is_err());
    assert!(validate_value_keys(&json!({"": 1})).is_err());
}

#[test]
fn test_convert_auth_to_rules_normalizes_empty_uid() {
    // Firebase Legacy Tokens authenticate with uid == "" and carry identity
    // in their claims. The principal must stay authenticated (auth != null),
    // but auth.uid must read as absent so `auth.uid === $uid` can't match an
    // empty captured path segment.
    let legacy = AuthInfo {
        uid: String::new(),
        provider: "custom".to_string(),
        token: HashMap::from([("role".to_string(), json!("editor"))]),
        is_admin: false,
    };
    let rules_auth = Database::convert_auth_to_rules(&legacy);
    let map = rules_auth
        .to_json()
        .expect("legacy token with claims must be authenticated (auth != null)");
    assert!(
        !map.contains_key("uid"),
        "empty uid must not appear as auth.uid"
    );
    assert_eq!(map.get("role"), Some(&json!("editor")));

    // A normal authenticated user keeps its uid verbatim.
    let normal = AuthInfo {
        uid: "user-123".to_string(),
        provider: "google".to_string(),
        token: HashMap::new(),
        is_admin: false,
    };
    let rules_auth = Database::convert_auth_to_rules(&normal);
    let map = rules_auth.to_json().expect("auth != null");
    assert_eq!(map.get("uid"), Some(&json!("user-123")));

    // A claimless empty-uid token has no identity at all → auth == null.
    let identityless = AuthInfo {
        uid: String::new(),
        provider: "custom".to_string(),
        token: HashMap::new(),
        is_admin: false,
    };
    assert!(
        Database::convert_auth_to_rules(&identityless)
            .to_json()
            .is_none(),
        "an empty-uid, claimless token carries no identity and must be auth == null"
    );
}

#[test]
fn test_path_matches_pattern() {
    // Exact matches
    assert!(path_matches_pattern("/cursors/test", "cursors/*"));
    assert!(path_matches_pattern("/cursors/abc", "cursors/*"));
    assert!(path_matches_pattern(
        "/players/p1/position",
        "players/*/position"
    ));

    // Children of volatile paths (volatile cascades down)
    assert!(path_matches_pattern("/cursors/a/b", "cursors/*"));
    assert!(path_matches_pattern(
        "/players/p1/position/x",
        "players/*/position"
    ));
    assert!(path_matches_pattern("/cursors/a/b/c", "cursors"));

    // Non-matches
    assert!(!path_matches_pattern("/cursors", "cursors/*")); // Too short
    assert!(!path_matches_pattern("/other/test", "cursors/*")); // Wrong prefix
}

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    let local_ex = glommio::LocalExecutor::default();
    local_ex.run(f)
}

// Mock connection for testing
struct MockConnection {
    messages: Arc<Mutex<Vec<Vec<u8>>>>,
    closed: Arc<std::sync::atomic::AtomicBool>,
}

impl MockConnection {
    #[allow(clippy::type_complexity)] // test helper: (conn, captured-writes)
    fn new() -> (Arc<Self>, Arc<Mutex<Vec<Vec<u8>>>>) {
        let messages = Arc::new(Mutex::new(Vec::new()));
        let conn = Arc::new(Self {
            messages: messages.clone(),
            closed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        });
        (conn, messages)
    }
}

impl ConnectionSender for MockConnection {
    fn send(
        &self,
        data: Bytes,
        _volatile: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), SendError>> + '_>> {
        self.messages.lock().unwrap().push(data.to_vec());
        Box::pin(async { Ok(()) })
    }

    fn try_send(
        &self,
        data: Bytes,
        _volatile: bool,
        _skip_translation: bool,
    ) -> Result<(), SendError> {
        self.messages.lock().unwrap().push(data.to_vec());
        Ok(())
    }

    fn send_broadcast_raw(&self, payload: &[u8], _flags: u8) -> Result<(), SendError> {
        // Parse broadcast format: [ClientCount:4][[ClientID:4][Tag:4]...][MsgLen:4][MsgBytes...]
        if payload.len() < 4 {
            return Ok(());
        }
        let client_count = u32::from_be_bytes(payload[0..4].try_into().unwrap()) as usize;
        let header_size = 4 + client_count * 8; // 4 (count) + N * (4 clientID + 4 tag)
        if payload.len() < header_size + 4 {
            return Ok(());
        }
        let msg_len =
            u32::from_be_bytes(payload[header_size..header_size + 4].try_into().unwrap()) as usize;
        let msg_start = header_size + 4;
        if payload.len() >= msg_start + msg_len {
            let msg_bytes = &payload[msg_start..msg_start + msg_len];
            self.messages.lock().unwrap().push(msg_bytes.to_vec());
        }
        Ok(())
    }

    fn close(&self) {
        self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

#[test]
fn test_database_set_and_get() {
    let db = Database::new("test".to_string(), "test-project".to_string(), true);

    // Manually set a value
    let path = Path::parse("/players/abc/name");
    db.tree.write().unwrap().set(&path, json!("Alice"));

    // Get it back
    let value = db.tree.read().unwrap().get_value(&path);
    assert_eq!(value, Some(json!("Alice")));
}

#[test]
fn test_handle_set() {
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        let (conn, _messages) = MockConnection::new();

        // Add client
        db.add_client_internal("client1", None, "conn1", conn);

        // Handle set message
        let msg = ClientMessage {
            op: "s".to_string(),
            path: Some("/foo".to_string()),
            value: Some(json!("bar")),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };

        let response = db.handle_set("client1", &msg, false).await;
        assert!(response.is_some());

        // Verify data was set
        let value = db.tree.read().unwrap().get_value_str("/foo");
        assert_eq!(value, Some(json!("bar")));
    })
}

#[test]
fn test_write_handlers_reject_invalid_paths_and_keys() {
    // End-to-end: drive real SET/UPDATE messages through the single-op
    // handlers (not just the validator functions) so the dispatch path is
    // covered. Security audit finding #3: these handlers, not just
    // handle_transaction, must enforce the key invariant.
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Empty path segment (the confused-deputy input) → NACK, nothing written.
        let msg = ClientMessage {
            op: "s".to_string(),
            path: Some("/users//abc".to_string()),
            value: Some(json!("x")),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        let resp = db
            .handle_set("client1", &msg, false)
            .await
            .expect("response");
        assert_eq!(resp.error.as_deref(), Some(error::INVALID_DATA));
        assert!(resp.nack.is_some());
        assert_eq!(db.tree.read().unwrap().get_value_str("/users"), None);

        // Literal-slash key inside a SET value → NACK, nothing written.
        let msg = ClientMessage {
            op: "s".to_string(),
            path: Some("/ok".to_string()),
            value: Some(json!({"a/b": 1})),
            request_id: Some("r2".to_string()),
            ..Default::default()
        };
        let resp = db
            .handle_set("client1", &msg, false)
            .await
            .expect("response");
        assert!(resp.nack.is_some());
        assert_eq!(db.tree.read().unwrap().get_value_str("/ok"), None);

        // UPDATE with a forbidden key → NACK.
        let msg = ClientMessage {
            op: "u".to_string(),
            path: Some("/acct".to_string()),
            value: Some(json!({"bal$ance": 5})),
            request_id: Some("r3".to_string()),
            ..Default::default()
        };
        let resp = db
            .handle_update("client1", &msg, false)
            .await
            .expect("response");
        assert!(resp.nack.is_some());

        // A well-formed write still succeeds — no false positives.
        let msg = ClientMessage {
            op: "s".to_string(),
            path: Some("/users/abc".to_string()),
            value: Some(json!({"name": "Alice"})),
            request_id: Some("r4".to_string()),
            ..Default::default()
        };
        let resp = db
            .handle_set("client1", &msg, false)
            .await
            .expect("response");
        assert!(resp.nack.is_none(), "valid write must not be nacked");
        assert_eq!(
            db.tree.read().unwrap().get_value_str("/users/abc"),
            Some(json!({"name": "Alice"}))
        );
    })
}

/// Nest `levels` objects (`{d0: {d1: ... {d{levels-1}: "leaf"}}}`).
/// `json_value_depth` of the result is `levels`.
fn nest_value(levels: usize) -> Value {
    let mut value = json!("leaf");
    for i in (0..levels).rev() {
        let mut m = serde_json::Map::new();
        m.insert(format!("d{i}"), value);
        value = Value::Object(m);
    }
    value
}

#[test]
fn test_set_rejects_total_depth_over_cap() {
    // The chaos-monkey shape: a deeply-nested value at a shallow path. The
    // path passes validate_path, but path + value nesting exceeds the depth
    // cap, so the leaf would be unreadable by path — reject the write.
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // path "/deep" = 1 segment; value nested MAX_PATH_DEPTH deep →
        // total MAX_PATH_DEPTH + 1, one past the cap.
        let msg = ClientMessage {
            op: "s".to_string(),
            path: Some("/deep".to_string()),
            value: Some(nest_value(crate::db::MAX_PATH_DEPTH)),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        let resp = db.handle_set("client1", &msg, false).await.expect("resp");
        assert_eq!(resp.error.as_deref(), Some(error::INVALID_DATA));
        assert_eq!(
            db.tree.read().unwrap().get_value_str("/deep"),
            None,
            "over-deep write must not commit"
        );
    })
}

#[test]
fn test_set_accepts_total_depth_at_cap() {
    // path(1) + value(MAX_PATH_DEPTH - 1) == MAX_PATH_DEPTH exactly → allowed.
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        let msg = ClientMessage {
            op: "s".to_string(),
            path: Some("/deep".to_string()),
            value: Some(nest_value(crate::db::MAX_PATH_DEPTH - 1)),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        let resp = db.handle_set("client1", &msg, false).await.expect("resp");
        assert!(
            resp.nack.is_none(),
            "a write landing exactly at the depth cap must be accepted"
        );
        assert!(db.tree.read().unwrap().get_value_str("/deep").is_some());
    })
}

#[test]
fn test_update_rejects_total_depth_over_cap() {
    // UPDATE composes base + child key + value nesting; the sum must respect
    // the cap just like SET.
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // base "/a" (1) + key "b" (1) + value nested (MAX_PATH_DEPTH - 1) →
        // total MAX_PATH_DEPTH + 1.
        let mut map = serde_json::Map::new();
        map.insert("b".to_string(), nest_value(crate::db::MAX_PATH_DEPTH - 1));
        let msg = ClientMessage {
            op: "u".to_string(),
            path: Some("/a".to_string()),
            value: Some(Value::Object(map)),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        let resp = db
            .handle_update("client1", &msg, false)
            .await
            .expect("resp");
        assert_eq!(resp.error.as_deref(), Some(error::INVALID_DATA));
        assert_eq!(db.tree.read().unwrap().get_value_str("/a/b"), None);
    })
}

#[test]
fn test_on_disconnect_enforces_rules_and_validation() {
    // Security audit follow-up: onDisconnect deferred writes are applied
    // directly to the tree/WAL on disconnect, so they must be rules-checked
    // AND path/key-validated at registration — not left as a write-anywhere
    // primitive that bypasses security rules.
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        let rules = crate::rules::parse_rules(&json!({
            "rules": {
                "locked": { ".write": false },
                "open":   { ".write": true }
            }
        }))
        .unwrap();
        db.set_rules(rules);

        // 1. Deferred write to a rules-denied path → NACK, not registered.
        let msg = ClientMessage {
            path: Some("/locked".to_string()),
            action: Some("s".to_string()),
            value: Some(json!("x")),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        let resp = db
            .handle_on_disconnect("client1", &msg)
            .await
            .expect("resp");
        assert_eq!(resp.error.as_deref(), Some(error::PERMISSION_DENIED));

        // 2. Deferred write with a malformed path → NACK INVALID_DATA.
        let msg = ClientMessage {
            path: Some("/open//x".to_string()),
            action: Some("s".to_string()),
            value: Some(json!("x")),
            request_id: Some("r2".to_string()),
            ..Default::default()
        };
        let resp = db
            .handle_on_disconnect("client1", &msg)
            .await
            .expect("resp");
        assert_eq!(resp.error.as_deref(), Some(error::INVALID_DATA));

        // 3. An allowed, well-formed deferred write → ACK, and it fires.
        let msg = ClientMessage {
            path: Some("/open/ok".to_string()),
            action: Some("s".to_string()),
            value: Some(json!("v")),
            request_id: Some("r3".to_string()),
            ..Default::default()
        };
        let resp = db
            .handle_on_disconnect("client1", &msg)
            .await
            .expect("resp");
        assert!(resp.nack.is_none(), "allowed onDisconnect should ack");

        // Fire deferred actions; only the allowed one should have been kept.
        db.handle_disconnect("client1").await;
        assert_eq!(
            db.tree.read().unwrap().get_value_str("/open/ok"),
            Some(json!("v"))
        );
        assert_eq!(db.tree.read().unwrap().get_value_str("/locked"), None);
    })
}

#[test]
fn test_on_disconnect_rejects_total_depth_over_cap() {
    // onDisconnect applies deferred writes directly to the tree, so it must
    // enforce the same path+value depth cap as the live write handlers —
    // otherwise it could register a write that lands data deeper than a read
    // can address.
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // path "/deep" (1) + value nested MAX_PATH_DEPTH deep → one past cap.
        let msg = ClientMessage {
            path: Some("/deep".to_string()),
            action: Some("s".to_string()),
            value: Some(nest_value(crate::db::MAX_PATH_DEPTH)),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        let resp = db
            .handle_on_disconnect("client1", &msg)
            .await
            .expect("resp");
        assert_eq!(resp.error.as_deref(), Some(error::INVALID_DATA));

        // Firing disconnect must apply nothing (the action was never registered).
        db.handle_disconnect("client1").await;
        assert_eq!(db.tree.read().unwrap().get_value_str("/deep"), None);
    })
}

#[test]
fn test_on_disconnect_caps_per_client() {
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);
        db.set_rules(
            crate::rules::parse_rules(&json!({"rules": {".write": true, ".read": true}})).unwrap(),
        );

        // Register up to the per-client action-count cap — all accepted.
        for i in 0..MAX_ON_DISCONNECT_ACTIONS_PER_CLIENT {
            let msg = ClientMessage {
                path: Some(format!("/p{}", i)),
                action: Some("s".to_string()),
                value: Some(json!("v")),
                request_id: Some(format!("r{}", i)),
                ..Default::default()
            };
            let resp = db
                .handle_on_disconnect("client1", &msg)
                .await
                .expect("resp");
            assert!(resp.nack.is_none(), "action {} within cap should ack", i);
        }

        // One more action exceeds the count cap → NACK PAYLOAD_TOO_LARGE.
        let msg = ClientMessage {
            path: Some("/overflow".to_string()),
            action: Some("s".to_string()),
            value: Some(json!("v")),
            request_id: Some("rovf".to_string()),
            ..Default::default()
        };
        let resp = db
            .handle_on_disconnect("client1", &msg)
            .await
            .expect("resp");
        assert_eq!(resp.error.as_deref(), Some(error::PAYLOAD_TOO_LARGE));

        // A fresh client with a single oversized value trips the byte cap.
        let (conn2, _m2) = MockConnection::new();
        db.add_client_internal("client2", None, "conn2", conn2);
        let big = "x".repeat(MAX_ON_DISCONNECT_BYTES_PER_CLIENT + 1);
        let msg = ClientMessage {
            path: Some("/big".to_string()),
            action: Some("s".to_string()),
            value: Some(json!(big)),
            request_id: Some("rbig".to_string()),
            ..Default::default()
        };
        let resp = db
            .handle_on_disconnect("client2", &msg)
            .await
            .expect("resp");
        assert_eq!(resp.error.as_deref(), Some(error::PAYLOAD_TOO_LARGE));
    })
}

/// Build a non-admin authenticated identity with the given uid.
fn authed(uid: &str) -> AuthInfo {
    AuthInfo {
        uid: uid.to_string(),
        provider: "password".to_string(),
        token: HashMap::new(),
        is_admin: false,
    }
}

/// Per-user read rule used by the revocation tests: a client may read
/// `/private/<uid>` only when `auth.uid` matches that uid.
fn private_per_user_rules() -> crate::rules::RuleSet {
    crate::rules::parse_rules(&json!({
        "rules": { "private": { "$uid": {
            ".read": "auth.uid === $uid",
            ".write": true
        }}}
    }))
    .unwrap()
}

#[test]
fn test_auth_change_revokes_now_unauthorized_subscription() {
    // H-2: a subscription authorized under one auth must be torn down
    // server-side when the connection's auth changes to one the read rule
    // denies — not left streaming for the connection's lifetime.
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        let (conn, _messages) = MockConnection::new();
        db.set_rules(private_per_user_rules());

        // Alice connects and subscribes to her own private path — allowed.
        db.add_client_internal("client1", Some(authed("alice")), "conn1", conn);
        db.tree
            .write()
            .unwrap()
            .set_str("/private/alice", json!({"secret": 1}));

        let sub = ClientMessage {
            op: "sb".to_string(),
            path: Some("/private/alice".to_string()),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        db.handle_subscribe("client1", &sub).await;
        assert_eq!(
            db.view_manager.subscription_count(),
            1,
            "alice's subscribe to her own path should be accepted"
        );

        // Connection's auth is swapped to a different uid (sign-out, or a
        // token refresh to bob). Alice's old subscription no longer passes.
        db.handle_auth_update("client1", Some(authed("bob"))).await;
        assert_eq!(
            db.view_manager.subscription_count(),
            0,
            "subscription must be revoked once auth changes to a uid the rule denies"
        );
    })
}

#[test]
fn test_auth_change_keeps_still_authorized_subscription() {
    // A re-auth that still satisfies the rule (e.g. token refresh, same uid)
    // must NOT drop the subscription.
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        let (conn, _messages) = MockConnection::new();
        db.set_rules(private_per_user_rules());
        db.add_client_internal("client1", Some(authed("alice")), "conn1", conn);
        db.tree
            .write()
            .unwrap()
            .set_str("/private/alice", json!({"secret": 1}));

        let sub = ClientMessage {
            op: "sb".to_string(),
            path: Some("/private/alice".to_string()),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        db.handle_subscribe("client1", &sub).await;
        assert_eq!(db.view_manager.subscription_count(), 1);

        // Re-auth, still alice → still authorized, subscription survives.
        db.handle_auth_update("client1", Some(authed("alice")))
            .await;
        assert_eq!(
            db.view_manager.subscription_count(),
            1,
            "a still-authorized re-auth must not revoke the subscription"
        );
    })
}

#[test]
fn test_query_subscription_revoked_on_auth_change() {
    // Exercises the query path: the stored rules-query is carried through
    // list_client_subscriptions and the revoke uses the right query_id.
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        let (conn, _messages) = MockConnection::new();
        db.set_rules(private_per_user_rules());
        db.add_client_internal("client1", Some(authed("alice")), "conn1", conn);
        {
            let mut tree = db.tree.write().unwrap();
            tree.set_str("/private/alice/a", json!({"n": 1}));
            tree.set_str("/private/alice/b", json!({"n": 2}));
        }

        let sub = ClientMessage {
            op: "sb".to_string(),
            path: Some("/private/alice".to_string()),
            request_id: Some("r1".to_string()),
            order_by_child: Some("n".to_string()),
            limit_to_first: Some(1),
            ..Default::default()
        };
        db.handle_subscribe("client1", &sub).await;
        assert_eq!(db.view_manager.subscription_count(), 1);

        db.handle_auth_update("client1", Some(authed("bob"))).await;
        assert_eq!(
            db.view_manager.subscription_count(),
            0,
            "query subscription must be revoked on auth change too"
        );
    })
}

#[test]
fn test_rules_change_revokes_now_unauthorized_subscription() {
    // A tightened ruleset (CONFIG_PUSH) must revoke subscriptions it now
    // forbids. Drives the all-clients re-check helper.
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        let (conn, _messages) = MockConnection::new();

        // Start fully open: alice subscribes to /room successfully.
        db.set_rules(
            crate::rules::parse_rules(&json!({"rules": {".read": true, ".write": true}})).unwrap(),
        );
        db.add_client_internal("client1", Some(authed("alice")), "conn1", conn);
        db.tree
            .write()
            .unwrap()
            .set_str("/room", json!({"msg": "hi"}));

        let sub = ClientMessage {
            op: "sb".to_string(),
            path: Some("/room".to_string()),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        db.handle_subscribe("client1", &sub).await;
        assert_eq!(db.view_manager.subscription_count(), 1);

        // Admin tightens rules: /room is no longer readable.
        db.set_rules(
            crate::rules::parse_rules(&json!({
                "rules": { ".write": true, "room": { ".read": false } }
            }))
            .unwrap(),
        );
        db.revoke_all_unauthorized_subscriptions().await;
        assert_eq!(
            db.view_manager.subscription_count(),
            0,
            "subscription must be revoked once new rules deny the read"
        );
    })
}

#[test]
fn test_handle_subscribe() {
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        let (conn, messages) = MockConnection::new();

        // Add client
        db.add_client_internal("client1", None, "conn1", conn);

        // Set some data first
        db.tree
            .write()
            .unwrap()
            .set_str("/players/abc", json!({"name": "Alice"}));

        // Subscribe
        let msg = ClientMessage {
            op: "sb".to_string(),
            path: Some("/players/abc".to_string()),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };

        db.handle_subscribe("client1", &msg).await;

        // Should have received initial snapshot (ack may be combined or omitted)
        let msgs = messages.lock().unwrap();
        assert!(
            !msgs.is_empty(),
            "Expected at least 1 message, got {}",
            msgs.len()
        );

        // Verify view was created
        assert_eq!(db.view_count(), 1);
    })
}

#[test]
fn test_handle_unsubscribe() {
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        let (conn, _messages) = MockConnection::new();

        // Add client
        db.add_client_internal("client1", None, "conn1", conn);

        // Subscribe
        let sub_msg = ClientMessage {
            op: "sb".to_string(),
            path: Some("/players".to_string()),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        db.handle_subscribe("client1", &sub_msg).await;
        assert_eq!(db.view_count(), 1);

        // Unsubscribe
        let unsub_msg = ClientMessage {
            op: "us".to_string(),
            path: Some("/players".to_string()),
            request_id: Some("r2".to_string()),
            ..Default::default()
        };
        db.handle_unsubscribe("client1", &unsub_msg);
        assert_eq!(db.view_count(), 0);
    })
}

#[test]
fn test_subscribe_with_query() {
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        let (conn, messages) = MockConnection::new();

        // Add client
        db.add_client_internal("client1", None, "conn1", conn);

        // Set some data
        {
            let mut tree = db.tree.write().unwrap();
            tree.set_str("/players/alice", json!({"name": "Alice", "score": 200}));
            tree.set_str("/players/bob", json!({"name": "Bob", "score": 100}));
            tree.set_str("/players/charlie", json!({"name": "Charlie", "score": 300}));
        }

        // Subscribe with query (orderByChild score, limitToFirst 2)
        let msg = ClientMessage {
            op: "sb".to_string(),
            path: Some("/players".to_string()),
            request_id: Some("r1".to_string()),
            order_by_child: Some("score".to_string()),
            limit_to_first: Some(2),
            ..Default::default()
        };

        db.handle_subscribe("client1", &msg).await;

        // Should have received filtered snapshot (ack may be combined or omitted)
        let msgs = messages.lock().unwrap();
        assert!(
            !msgs.is_empty(),
            "Expected at least 1 message, got {}",
            msgs.len()
        );

        // Parse the snapshot to verify filtering (last message is the snapshot)
        let snapshot_data = &msgs[msgs.len() - 1];
        let snapshot: Value = serde_json::from_slice(snapshot_data).unwrap();

        // The value should only have 2 entries (bob: 100, alice: 200)
        if let Some(value) = snapshot.get("v")
            && let Some(obj) = value.as_object()
        {
            assert_eq!(obj.len(), 2);
            assert!(obj.contains_key("bob")); // score 100
            assert!(obj.contains_key("alice")); // score 200
            assert!(!obj.contains_key("charlie")); // score 300 (filtered out)
        }
    })
}

#[test]
fn test_disconnect_removes_subscriptions() {
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        let (conn, _messages) = MockConnection::new();

        // Add client
        db.add_client_internal("client1", None, "conn1", conn);

        // Subscribe to multiple paths
        let msg1 = ClientMessage {
            op: "sb".to_string(),
            path: Some("/a".to_string()),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        let msg2 = ClientMessage {
            op: "sb".to_string(),
            path: Some("/b".to_string()),
            request_id: Some("r2".to_string()),
            ..Default::default()
        };

        db.handle_subscribe("client1", &msg1).await;
        db.handle_subscribe("client1", &msg2).await;
        assert_eq!(db.view_count(), 2);

        // Disconnect
        db.handle_disconnect("client1").await;
        assert_eq!(db.view_count(), 0);
        assert_eq!(db.client_count(), 0);
    })
}

// Persistence tests removed — WAL replay is now handled by lark-blob.
// New blob-backed tests will be added when BlobSession integration is complete.

// =========================================================================
// WAL Failure Recovery Tests
// =========================================================================

#[test]
fn test_wal_failed_flag_nacks_set() {
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Simulate WAL failure
        db.wal_failed = true;

        // Attempt a SET write — should be NACKed
        let msg = ClientMessage {
            op: "s".to_string(),
            path: Some("/foo".to_string()),
            value: Some(json!("bar")),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        let response = db.handle_set("client1", &msg, false).await;

        // Should get a NACK with "unavailable"
        let resp = response.expect("Expected a NACK response");
        assert!(resp.nack.is_some(), "Expected NACK, got: {:?}", resp);
        assert_eq!(resp.error.as_deref(), Some("unavailable"));

        // Tree should NOT have the value (write was rejected before tree mutation)
        let value = db.tree.read().unwrap().get_value_str("/foo");
        assert!(
            value.is_none(),
            "Tree should not have value after WAL-failed NACK"
        );
    })
}

#[test]
fn test_wal_failed_flag_nacks_update() {
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Set some initial data
        db.tree
            .write()
            .unwrap()
            .set_str("/users/1", json!({"name": "Alice"}));

        // Simulate WAL failure
        db.wal_failed = true;

        // Attempt an UPDATE — should be NACKed
        let msg = ClientMessage {
            op: "u".to_string(),
            path: Some("/users/1".to_string()),
            value: Some(json!({"age": 30})),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        let response = db.handle_update("client1", &msg, false).await;

        let resp = response.expect("Expected a NACK response");
        assert!(resp.nack.is_some(), "Expected NACK, got: {:?}", resp);
        assert_eq!(resp.error.as_deref(), Some("unavailable"));

        // Tree should NOT have the update applied
        let tree = db.tree.read().unwrap();
        let val = tree.get_value_str("/users/1").unwrap();
        assert!(
            val.get("age").is_none(),
            "Update should not have been applied"
        );
    })
}

#[test]
fn test_wal_failed_flag_nacks_remove() {
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Set initial data
        db.tree
            .write()
            .unwrap()
            .set_str("/data", json!("important"));

        // Simulate WAL failure
        db.wal_failed = true;

        // Attempt a REMOVE — should be NACKed
        let msg = ClientMessage {
            op: "r".to_string(),
            path: Some("/data".to_string()),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        let response = db.handle_remove("client1", &msg, false).await;

        let resp = response.expect("Expected a NACK response");
        assert!(resp.nack.is_some(), "Expected NACK, got: {:?}", resp);
        assert_eq!(resp.error.as_deref(), Some("unavailable"));

        // Data should still exist (removal was rejected)
        let value = db.tree.read().unwrap().get_value_str("/data");
        assert_eq!(value, Some(json!("important")));
    })
}

#[test]
fn test_wal_failed_flag_nacks_transaction() {
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Simulate WAL failure
        db.wal_failed = true;

        // Attempt a TRANSACTION — should be NACKed
        let msg = ClientMessage {
            op: "t".to_string(),
            path: Some("/counter".to_string()),
            request_id: Some("r1".to_string()),
            operations: Some(vec![crate::protocol::TransactionOp {
                op: "s".to_string(),
                path: "/counter".to_string(),
                value: Some(json!(42)),
                hash: None,
            }]),
            ..Default::default()
        };
        let response = db.handle_transaction("client1", &msg).await;

        let resp = response.expect("Expected a NACK response");
        assert!(resp.nack.is_some(), "Expected NACK, got: {:?}", resp);
        assert_eq!(resp.error.as_deref(), Some("unavailable"));
    })
}

#[test]
fn test_transaction_op_count_cap() {
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // A transaction at the cap is accepted (open rules by default).
        let ops_at_cap: Vec<_> = (0..MAX_TRANSACTION_OPS)
            .map(|i| crate::protocol::TransactionOp {
                op: "s".to_string(),
                path: format!("/k{}", i),
                value: Some(json!(i)),
                hash: None,
            })
            .collect();
        let msg = ClientMessage {
            op: "t".to_string(),
            request_id: Some("r1".to_string()),
            operations: Some(ops_at_cap),
            ..Default::default()
        };
        let resp = db.handle_transaction("client1", &msg).await.expect("resp");
        assert!(
            resp.nack.is_none(),
            "transaction at the cap should not be rejected for size, got: {:?}",
            resp
        );

        // One more op exceeds the cap → NACK PAYLOAD_TOO_LARGE.
        let too_many: Vec<_> = (0..=MAX_TRANSACTION_OPS)
            .map(|i| crate::protocol::TransactionOp {
                op: "s".to_string(),
                path: format!("/k{}", i),
                value: Some(json!(i)),
                hash: None,
            })
            .collect();
        let msg = ClientMessage {
            op: "t".to_string(),
            request_id: Some("r2".to_string()),
            operations: Some(too_many),
            ..Default::default()
        };
        let resp = db.handle_transaction("client1", &msg).await.expect("resp");
        assert_eq!(resp.error.as_deref(), Some(error::PAYLOAD_TOO_LARGE));
    })
}

#[test]
fn test_write_rate_limiter_burst_and_refill() {
    let t0 = Instant::now();
    let mut rl = WriteRateLimiter {
        tokens: WRITE_RATE_BURST_BYTES,
        last_refill: t0,
    };

    let mb = 1024 * 1024;

    // Can spend the full burst capacity immediately.
    assert!(rl.try_consume_at(512 * mb, t0));
    // Bucket now empty: a 1-byte write is rejected at the same instant.
    assert!(!rl.try_consume_at(1, t0));

    // After 15s, ~64MB has refilled: a 64MB write succeeds, a hair more fails.
    let t1 = t0 + Duration::from_secs(15);
    assert!(rl.try_consume_at(64 * mb, t1));
    assert!(!rl.try_consume_at(mb, t1));

    // Refill is capped at burst capacity: idling a long time can't exceed 512MB.
    let t2 = t1 + Duration::from_secs(3600);
    assert!(rl.try_consume_at(512 * mb, t2));
    assert!(!rl.try_consume_at(1, t2));
}

#[test]
fn test_database_size_cap_rejects_growth_allows_delete() {
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Drive the (normally periodically-refreshed) size gauge to the cap.
        db.metrics.set_data_size(MAX_DATABASE_SIZE_BYTES);

        // SET (growth) → DATABASE_FULL.
        let resp = db
            .handle_set(
                "client1",
                &ClientMessage {
                    op: "s".to_string(),
                    path: Some("/a".to_string()),
                    value: Some(json!("v")),
                    request_id: Some("r1".to_string()),
                    ..Default::default()
                },
                false,
            )
            .await
            .expect("resp");
        assert_eq!(resp.error.as_deref(), Some(error::DATABASE_FULL));

        // UPDATE (growth) → DATABASE_FULL.
        let resp = db
            .handle_update(
                "client1",
                &ClientMessage {
                    op: "u".to_string(),
                    path: Some("/a".to_string()),
                    value: Some(json!({"k": "v"})),
                    request_id: Some("r2".to_string()),
                    ..Default::default()
                },
                false,
            )
            .await
            .expect("resp");
        assert_eq!(resp.error.as_deref(), Some(error::DATABASE_FULL));

        // TRANSACTION → DATABASE_FULL.
        let resp = db
            .handle_transaction(
                "client1",
                &ClientMessage {
                    op: "t".to_string(),
                    request_id: Some("r3".to_string()),
                    operations: Some(vec![crate::protocol::TransactionOp {
                        op: "s".to_string(),
                        path: "/a".to_string(),
                        value: Some(json!(1)),
                        hash: None,
                    }]),
                    ..Default::default()
                },
            )
            .await
            .expect("resp");
        assert_eq!(resp.error.as_deref(), Some(error::DATABASE_FULL));

        // REMOVE is still allowed at the cap so the owner can recover.
        let resp = db
            .handle_remove(
                "client1",
                &ClientMessage {
                    op: "d".to_string(),
                    path: Some("/a".to_string()),
                    request_id: Some("r4".to_string()),
                    ..Default::default()
                },
                false,
            )
            .await;
        if let Some(r) = resp {
            assert_ne!(
                r.error.as_deref(),
                Some(error::DATABASE_FULL),
                "remove must not be size-rejected: {:?}",
                r
            );
        }

        // Volatile writes are NOT exempt — they're rejected at the cap too
        // (we intentionally don't carve them out).
        db.set_volatile_paths(vec!["cursors/*".to_string()]);
        let resp = db
            .handle_set(
                "client1",
                &ClientMessage {
                    op: "s".to_string(),
                    path: Some("/cursors/p1".to_string()),
                    value: Some(json!({"x": 1})),
                    request_id: Some("r5".to_string()),
                    volatile: Some(true),
                    ..Default::default()
                },
                true,
            )
            .await
            .expect("resp");
        assert_eq!(resp.error.as_deref(), Some(error::DATABASE_FULL));
    })
}

#[test]
fn test_wal_failed_allows_volatile_writes() {
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Set volatile paths
        db.set_volatile_paths(vec!["cursors/*".to_string()]);

        // Simulate WAL failure
        db.wal_failed = true;

        // Volatile writes should still go through (they bypass WAL)
        let msg = ClientMessage {
            op: "s".to_string(),
            path: Some("/cursors/player1".to_string()),
            value: Some(json!({"x": 100, "y": 200})),
            request_id: Some("r1".to_string()),
            volatile: Some(true),
            ..Default::default()
        };

        // Volatile writes return None (no ack)
        let response = db.handle_set("client1", &msg, true).await;
        assert!(
            response.is_none(),
            "Volatile writes should not be NACKed even when WAL failed"
        );
    })
}

#[test]
fn test_wal_recovery_clears_failed_flag() {
    block_on(async {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_path_buf();

        let mut db = Database::new_with_persistence(
            "test".to_string(),
            "test-project".to_string(),
            data_dir.clone(),
        );
        db.init_wal_writer().await;

        // Simulate WAL failure
        db.wal_failed = true;
        assert!(db.is_wal_failed());

        // Attempt recovery — WAL writer is functional (disk is fine),
        // so recovery should succeed
        db.try_recover_wal().await;
        assert!(
            !db.is_wal_failed(),
            "WAL should have recovered since disk is fine"
        );

        // Now writes should work again
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        let msg = ClientMessage {
            op: "s".to_string(),
            path: Some("/test".to_string()),
            value: Some(json!("recovered")),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        let response = db.handle_set("client1", &msg, false).await;

        // Should get ACK, not NACK
        let resp = response.expect("Expected ACK response");
        assert!(
            resp.nack.is_none(),
            "Should not be NACKed after recovery, got: {:?}",
            resp
        );

        // Value should be in tree
        let value = db.tree.read().unwrap().get_value_str("/test");
        assert_eq!(value, Some(json!("recovered")));
    })
}

#[test]
fn test_wal_recovery_no_op_when_not_failed() {
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        assert!(!db.is_wal_failed());

        // Recovery should be a no-op
        db.try_recover_wal().await;
        assert!(!db.is_wal_failed());
    })
}

#[test]
fn test_init_wal_writer_returns_true_for_ephemeral() {
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        let result = db.init_wal_writer().await;
        assert!(
            result,
            "init_wal_writer should return true for ephemeral databases"
        );
    })
}

#[test]
fn test_init_wal_writer_returns_true_for_valid_dir() {
    block_on(async {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_path_buf();

        let mut db = Database::new_with_persistence(
            "test".to_string(),
            "test-project".to_string(),
            data_dir.clone(),
        );

        let result = db.init_wal_writer().await;
        assert!(
            result,
            "init_wal_writer should return true for valid directory"
        );
        assert!(db.wal_writer.is_some());
    })
}

// =========================================================================
// Eviction Tests
// =========================================================================

/// Helper: create a blob-backed database with known data in the blob.
/// Returns (db, _temp_dir) — keep _temp_dir alive for the test duration.
async fn make_blob_backed_db(data: Value) -> (Database, tempfile::TempDir) {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_path_buf();

    // Write blob with known data
    let blob_path = data_dir.join("blob.lark");
    let arc_value = ArcValue::from_value(data);
    let io = GlommioBlobIO::create(&blob_path).await.unwrap();
    lark_blob::write_blob(&io, &arc_value).await.unwrap();
    lark_blob::BlobIO::sync(&io).await.unwrap();
    drop(io);

    // Create database pointing at this dir
    let mut db = Database::new_with_persistence(
        "test/evict".to_string(),
        "test".to_string(),
        data_dir.clone(),
    );
    db.load_from_disk().await.unwrap();

    // Initialize WAL writer so writes add entries to pending_wal_entries
    db.init_wal_writer().await;

    // Verify it's blob-backed
    assert!(db.is_blob_backed(), "Database should be blob-backed");

    (db, temp_dir)
}

/// Helper: directly evict a path (simulates what evict_idle_paths does).
fn force_evict(db: &mut Database, path: &str) {
    let path_obj = Path::parse(path);
    db.tree
        .write()
        .unwrap()
        .set_arc_uncleaned_lazy(&path_obj, ArcValue::empty_sentinel());
    db.remove_sentinel_paths_below(path);
    db.sentinel_paths.insert(path.to_string());
    db.promoted_paths.remove(path);
}

// --- Test 1: Basic eviction via timer ---
#[test]
fn test_eviction_basic_timer() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "users": {"alice": {"name": "Alice"}}
        }))
        .await;

        // Promote the path
        let loaded = db.promote_path("/users/alice").await.unwrap();
        assert!(loaded, "Should have loaded from blob");
        assert!(db.promoted_paths.contains_key("/users/alice"));

        // Verify data is there
        let val = db
            .tree
            .read()
            .unwrap()
            .get_value(&Path::parse("/users/alice"));
        assert_eq!(val, Some(json!({"name": "Alice"})));

        // Backdate the promoted timestamp to simulate idle time
        db.promoted_paths.insert(
            "/users/alice".to_string(),
            Instant::now() - Duration::from_secs(600),
        );

        // Evict idle paths
        db.evict_idle_paths();

        // Path should be evicted
        assert!(!db.promoted_paths.contains_key("/users/alice"));

        // Tree should have a Sentinel there now
        let tree = db.tree.read().unwrap();
        let node = tree.get(&Path::parse("/users/alice"));
        assert!(
            node.is_none() || node.unwrap().is_sentinel(),
            "Evicted path should be Sentinel or absent"
        );
    })
}

// --- Test 2: Re-promotion resets timer ---
#[test]
fn test_eviction_repromotion_resets_timer() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "users": {"alice": {"name": "Alice"}}
        }))
        .await;

        // Promote the path
        db.promote_path("/users/alice").await.unwrap();

        // Evict it
        force_evict(&mut db, "/users/alice");
        assert!(!db.promoted_paths.contains_key("/users/alice"));

        // Re-promote — should reload from blob
        let loaded = db.promote_path("/users/alice").await.unwrap();
        assert!(loaded, "Should have re-loaded from blob after eviction");
        assert!(db.promoted_paths.contains_key("/users/alice"));

        // Timestamp should be fresh — evict_idle_paths should NOT evict
        db.evict_idle_paths();
        assert!(
            db.promoted_paths.contains_key("/users/alice"),
            "Freshly re-promoted path should not be evicted"
        );
    })
}

// --- Test 3: Evict then read with once() ---
#[test]
fn test_eviction_then_once_read() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "users": {"alice": {"name": "Alice", "score": 100}}
        }))
        .await;
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Promote, then evict
        db.promote_path("/users/alice").await.unwrap();
        force_evict(&mut db, "/users/alice");

        // once() read should re-promote and return correct data
        let msg = ClientMessage {
            op: "o".to_string(),
            path: Some("/users/alice".to_string()),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        let response = db.handle_once("client1", &msg).await;
        let resp = response.expect("Expected response from once()");
        assert!(
            resp.nack.is_none(),
            "once() should succeed, got: {:?}",
            resp
        );

        // Data should match (once response uses once_value, not value)
        let val = resp.once_value.map(|v| v.to_value());
        assert_eq!(val, Some(json!({"name": "Alice", "score": 100})));
    })
}

// --- Test 4: Evict then subscribe ---
#[test]
fn test_eviction_then_subscribe() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "users": {"alice": {"name": "Alice"}}
        }))
        .await;
        let (conn, messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Promote, then evict
        db.promote_path("/users/alice").await.unwrap();
        force_evict(&mut db, "/users/alice");

        // Subscribe should re-promote and send correct initial snapshot
        let msg = ClientMessage {
            op: "sb".to_string(),
            path: Some("/users/alice".to_string()),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        db.handle_subscribe("client1", &msg).await;

        // View should be created
        assert_eq!(db.view_count(), 1);

        // Should have received messages
        let msgs = messages.lock().unwrap();
        assert!(!msgs.is_empty(), "Should have received initial snapshot");

        // The last message should be the snapshot with correct data
        let last_msg: Value = serde_json::from_slice(&msgs[msgs.len() - 1]).unwrap();
        if let Some(v) = last_msg.get("v") {
            assert_eq!(v, &json!({"name": "Alice"}));
        }
    })
}

// --- Test 5: Evict then SET via set_lazy ---
#[test]
fn test_eviction_then_set() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "chat": {
                "msg1": {"text": "hello"},
                "msg2": {"text": "world"}
            }
        }))
        .await;
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Promote, then evict /chat
        db.promote_path("/chat").await.unwrap();
        force_evict(&mut db, "/chat");

        // SET to /chat/msg3 — should work through Sentinel via set_lazy
        let msg = ClientMessage {
            op: "s".to_string(),
            path: Some("/chat/msg3".to_string()),
            value: Some(json!({"text": "new message"})),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        let response = db.handle_set("client1", &msg, false).await;
        let resp = response.expect("Expected ACK");
        assert!(
            resp.nack.is_none(),
            "SET should succeed after eviction, got: {:?}",
            resp
        );

        // The new data should be in the tree (set_lazy writes through Sentinels)
        let val = db
            .tree
            .read()
            .unwrap()
            .get_value(&Path::parse("/chat/msg3"));
        assert_eq!(val, Some(json!({"text": "new message"})));

        // Now deep-promote /chat to verify all data is correct (blob + WAL replay)
        db.promote_path_deep("/chat").await.unwrap();
        let val = db.tree.read().unwrap().get_value(&Path::parse("/chat"));
        let obj = val.unwrap();
        assert_eq!(obj.get("msg1"), Some(&json!({"text": "hello"})));
        assert_eq!(obj.get("msg2"), Some(&json!({"text": "world"})));
        assert_eq!(obj.get("msg3"), Some(&json!({"text": "new message"})));
    })
}

// --- Test 6: Evict then UPDATE ---
#[test]
fn test_eviction_then_update() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "users": {"alice": {"name": "Alice", "score": 100}}
        }))
        .await;
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Promote, then evict
        db.promote_path("/users/alice").await.unwrap();
        force_evict(&mut db, "/users/alice");

        // UPDATE on an evicted path. After the lazy-newData refactor,
        // handle_update no longer eagerly promotes — it just writes
        // through Sentinel intermediates via update_lazy. The tree
        // immediately after the UPDATE may still be Sentinel-rooted
        // at this path; correct merged data appears once anything
        // reads the path and triggers promote_path_deep + WAL replay.
        let msg = ClientMessage {
            op: "u".to_string(),
            path: Some("/users/alice".to_string()),
            value: Some(json!({"badge": "gold"})),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        let response = db.handle_update("client1", &msg, false).await;
        let resp = response.expect("Expected ACK");
        assert!(
            resp.nack.is_none(),
            "UPDATE should succeed after eviction, got: {:?}",
            resp
        );

        // Read via promote_path_deep (the documented read path) —
        // this loads the blob, replays WAL (which has the badge
        // write), and produces the merged view.
        db.promote_path_deep("/users/alice").await.unwrap();
        let val = db
            .tree
            .read()
            .unwrap()
            .get_value(&Path::parse("/users/alice"))
            .unwrap();
        assert_eq!(val.get("name"), Some(&json!("Alice")));
        assert_eq!(val.get("score"), Some(&json!(100)));
        assert_eq!(val.get("badge"), Some(&json!("gold")));
    })
}

// --- Test 7: Evict then DELETE ---
#[test]
fn test_eviction_then_delete() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "users": {
                "alice": {"name": "Alice"},
                "bob": {"name": "Bob"}
            }
        }))
        .await;
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Promote, then evict /users
        db.promote_path("/users").await.unwrap();
        force_evict(&mut db, "/users");

        // DELETE /users/alice — should work (delete doesn't need existing data)
        let msg = ClientMessage {
            op: "r".to_string(),
            path: Some("/users/alice".to_string()),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        let response = db.handle_remove("client1", &msg, false).await;
        let resp = response.expect("Expected ACK");
        assert!(
            resp.nack.is_none(),
            "DELETE should succeed after eviction, got: {:?}",
            resp
        );

        // After deep-promoting /users, alice should be gone, bob should still be there
        db.promote_path_deep("/users").await.unwrap();
        let val = db
            .tree
            .read()
            .unwrap()
            .get_value(&Path::parse("/users"))
            .unwrap();
        assert!(val.get("alice").is_none(), "alice should be deleted");
        assert_eq!(val.get("bob"), Some(&json!({"name": "Bob"})));
    })
}

// --- Test 8: Subscribe, evict, then SET — verify delta event ---
#[test]
fn test_eviction_subscription_receives_delta_event() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "chat": {"msg1": {"text": "hello"}}
        }))
        .await;
        let (conn, messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Promote and subscribe to /chat
        db.promote_path("/chat").await.unwrap();
        let msg = ClientMessage {
            op: "sb".to_string(),
            path: Some("/chat".to_string()),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        db.handle_subscribe("client1", &msg).await;

        // Clear initial messages
        messages.lock().unwrap().clear();

        // Now evict /chat
        force_evict(&mut db, "/chat");

        // SET a new child — subscriber should get a delta event
        let set_msg = ClientMessage {
            op: "s".to_string(),
            path: Some("/chat/msg2".to_string()),
            value: Some(json!({"text": "new"})),
            request_id: Some("r2".to_string()),
            ..Default::default()
        };
        db.handle_set("client1", &set_msg, false).await;

        // Events are sent directly during broadcast_mutation via try_send
        let msgs = messages.lock().unwrap();
        let found_event = msgs.iter().any(|m| {
            if let Ok(v) = serde_json::from_slice::<Value>(m) {
                // Look for event containing msg2

                v.to_string().contains("msg2")
            } else {
                false
            }
        });
        assert!(
            found_event,
            "Subscriber should receive delta event for new child after eviction"
        );
    })
}

// --- Test 9: Subscribe with query, evict, trigger recompute ---
#[test]
fn test_eviction_query_view_recompute() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "players": {
                "alice": {"name": "Alice", "score": 300},
                "bob": {"name": "Bob", "score": 100},
                "charlie": {"name": "Charlie", "score": 200}
            }
        }))
        .await;
        let (conn, messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Promote /players and subscribe with limitToFirst(2) orderByChild(score)
        db.promote_path("/players").await.unwrap();
        let msg = ClientMessage {
            op: "sb".to_string(),
            path: Some("/players".to_string()),
            request_id: Some("r1".to_string()),
            order_by_child: Some("score".to_string()),
            limit_to_first: Some(2),
            ..Default::default()
        };
        db.handle_subscribe("client1", &msg).await;
        messages.lock().unwrap().clear();

        // Evict /players
        force_evict(&mut db, "/players");

        // Remove bob (score: 100) — this triggers a query recompute
        // because a removal from a limited query needs to check if a
        // previously-excluded item should now enter the result set.
        let del_msg = ClientMessage {
            op: "r".to_string(),
            path: Some("/players/bob".to_string()),
            request_id: Some("r2".to_string()),
            ..Default::default()
        };
        let response = db.handle_remove("client1", &del_msg, false).await;
        assert!(
            response.is_none() || response.as_ref().unwrap().nack.is_none(),
            "DELETE should succeed: {:?}",
            response
        );

        // Events are sent directly during broadcast_mutation via try_send.
        // Verify the subscriber received events (removal + potentially an add).
        let msgs = messages.lock().unwrap();
        assert!(
            !msgs.is_empty(),
            "Should have received query recompute events after eviction"
        );
    })
}

// --- Test 10: WAL replay correctness after eviction ---
#[test]
fn test_eviction_wal_replay_preserves_all_writes() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "chat": {"msg1": {"text": "from blob"}}
        }))
        .await;
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Write /chat/msg2 (goes to WAL + tree via set_lazy)
        let msg = ClientMessage {
            op: "s".to_string(),
            path: Some("/chat/msg2".to_string()),
            value: Some(json!({"text": "from wal 1"})),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        db.handle_set("client1", &msg, false).await;

        // Evict /chat
        force_evict(&mut db, "/chat");

        // Write /chat/msg3 (also goes to WAL + tree via set_lazy)
        let msg2 = ClientMessage {
            op: "s".to_string(),
            path: Some("/chat/msg3".to_string()),
            value: Some(json!({"text": "from wal 2"})),
            request_id: Some("r2".to_string()),
            ..Default::default()
        };
        db.handle_set("client1", &msg2, false).await;

        // Now deep-read /chat — should promote from blob + replay ALL WAL entries
        db.promote_path_deep("/chat").await.unwrap();
        let val = db
            .tree
            .read()
            .unwrap()
            .get_value(&Path::parse("/chat"))
            .unwrap();

        // All three messages should be present
        assert_eq!(
            val.get("msg1"),
            Some(&json!({"text": "from blob"})),
            "blob data preserved"
        );
        assert_eq!(
            val.get("msg2"),
            Some(&json!({"text": "from wal 1"})),
            "first WAL write preserved"
        );
        assert_eq!(
            val.get("msg3"),
            Some(&json!({"text": "from wal 2"})),
            "second WAL write preserved"
        );
    })
}

// --- Test 11: Descendants of evicted nodes ---
#[test]
fn test_eviction_orphaned_descendants_replaced_on_promote() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "chat": {
                "msg1": {"text": "original"}
            }
        }))
        .await;
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Evict /chat (it was never promoted, tree has Sentinel root)
        // Write /chat/msg2 — creates orphan real data under Sentinel
        let msg = ClientMessage {
            op: "s".to_string(),
            path: Some("/chat/msg2".to_string()),
            value: Some(json!({"text": "orphan"})),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        db.handle_set("client1", &msg, false).await;

        // Verify msg2 exists in tree (it was set via set_lazy)
        let val = db
            .tree
            .read()
            .unwrap()
            .get_value(&Path::parse("/chat/msg2"));
        assert_eq!(val, Some(json!({"text": "orphan"})));

        // Now deep-promote /chat — should read blob + replay WAL (including msg2 write)
        db.promote_path_deep("/chat").await.unwrap();
        let val = db
            .tree
            .read()
            .unwrap()
            .get_value(&Path::parse("/chat"))
            .unwrap();

        // Both original and orphan data should be present
        assert_eq!(
            val.get("msg1"),
            Some(&json!({"text": "original"})),
            "blob data present"
        );
        assert_eq!(
            val.get("msg2"),
            Some(&json!({"text": "orphan"})),
            "WAL orphan data present"
        );
    })
}

// --- Test 12: Multiple paths, only idle ones evicted ---
#[test]
fn test_eviction_selective_only_idle_paths() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "users": {"alice": {"name": "Alice"}},
            "config": {"theme": "dark"},
            "stats": {"views": 42}
        }))
        .await;

        // Promote all three paths
        db.promote_path("/users").await.unwrap();
        db.promote_path("/config").await.unwrap();
        db.promote_path("/stats").await.unwrap();

        // Backdate /users and /stats (idle), keep /config fresh
        db.promoted_paths.insert(
            "/users".to_string(),
            Instant::now() - Duration::from_secs(600),
        );
        db.promoted_paths.insert(
            "/stats".to_string(),
            Instant::now() - Duration::from_secs(600),
        );
        // /config stays at its current (recent) timestamp

        // Evict
        db.evict_idle_paths();

        // /users and /stats should be evicted
        assert!(
            !db.promoted_paths.contains_key("/users"),
            "/users should be evicted"
        );
        assert!(
            !db.promoted_paths.contains_key("/stats"),
            "/stats should be evicted"
        );

        // /config should still be promoted
        assert!(
            db.promoted_paths.contains_key("/config"),
            "/config should stay"
        );

        // /config data should still be readable without re-promotion
        let val = db.tree.read().unwrap().get_value(&Path::parse("/config"));
        assert_eq!(val, Some(json!({"theme": "dark"})));
    })
}

// --- Test 13: once() read, evict, once() again ---
#[test]
fn test_eviction_repeated_once_reads() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "data": {"key": "value", "count": 42}
        }))
        .await;
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // First once() read
        let msg = ClientMessage {
            op: "o".to_string(),
            path: Some("/data".to_string()),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        let resp1 = db.handle_once("client1", &msg).await.unwrap();
        assert!(resp1.nack.is_none());
        assert_eq!(
            resp1.once_value.map(|v| v.to_value()),
            Some(json!({"key": "value", "count": 42}))
        );

        // Evict
        force_evict(&mut db, "/data");

        // Second once() read — should re-promote and return same data
        let msg2 = ClientMessage {
            op: "o".to_string(),
            path: Some("/data".to_string()),
            request_id: Some("r2".to_string()),
            ..Default::default()
        };
        let resp2 = db.handle_once("client1", &msg2).await.unwrap();
        assert!(resp2.nack.is_none());
        assert_eq!(
            resp2.once_value.map(|v| v.to_value()),
            Some(json!({"key": "value", "count": 42}))
        );
    })
}

// --- Test 14: Rules evaluation after eviction ---
#[test]
fn test_eviction_rules_evaluation_promotes() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "config": {"public": true},
            "data": {"secret": "value"}
        }))
        .await;
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Set rules that read from root.child('config').child('public')
        let rules = crate::rules::parse_rules(&json!({
            "rules": {
                "data": {
                    ".read": "root.child('config').child('public').val() === true",
                    ".write": "root.child('config').child('public').val() === true"
                }
            }
        }))
        .unwrap();
        db.set_rules(rules);

        // First: promote /config so rules can evaluate, then read /data
        db.promote_path("/config").await.unwrap();
        let msg = ClientMessage {
            op: "o".to_string(),
            path: Some("/data".to_string()),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        let resp1 = db.handle_once("client1", &msg).await.unwrap();
        assert!(
            resp1.nack.is_none(),
            "First read should succeed (config promoted)"
        );

        // Now evict /config — rules will need to re-promote it
        force_evict(&mut db, "/config");

        // Write to /data — rules evaluation needs /config, which is now Sentinel
        // The NeedsPromotion retry loop should handle this.
        let set_msg = ClientMessage {
            op: "s".to_string(),
            path: Some("/data/new_key".to_string()),
            value: Some(json!("new_value")),
            request_id: Some("r2".to_string()),
            ..Default::default()
        };
        let resp2 = db.handle_set("client1", &set_msg, false).await.unwrap();
        assert!(
            resp2.nack.is_none(),
            "Write should succeed — rules should re-promote /config via NeedsPromotion loop. Got: {:?}",
            resp2
        );

        // Verify the write went through
        let val = db
            .tree
            .read()
            .unwrap()
            .get_value(&Path::parse("/data/new_key"));
        assert_eq!(val, Some(json!("new_value")));
    })
}

// --- Test 15: Transaction condition check after eviction ---
#[test]
fn test_eviction_transaction_condition_promotes() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "counter": 42
        }))
        .await;
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Promote /counter, then evict
        db.promote_path("/counter").await.unwrap();
        let val = db.tree.read().unwrap().get_value(&Path::parse("/counter"));
        assert_eq!(val, Some(json!(42)));
        force_evict(&mut db, "/counter");

        // Transaction: condition check on /counter (expecting 42), then set to 43.
        // The promote_path in handle_transaction should re-load the data.
        let msg = ClientMessage {
            op: "t".to_string(),
            path: Some("/counter".to_string()),
            request_id: Some("r1".to_string()),
            operations: Some(vec![
                crate::protocol::TransactionOp {
                    op: "c".to_string(),
                    path: "/counter".to_string(),
                    value: Some(json!(42)),
                    hash: None,
                },
                crate::protocol::TransactionOp {
                    op: "s".to_string(),
                    path: "/counter".to_string(),
                    value: Some(json!(43)),
                    hash: None,
                },
            ]),
            ..Default::default()
        };
        let response = db.handle_transaction("client1", &msg).await;
        let resp = response.expect("Expected response from transaction");
        assert!(
            resp.nack.is_none(),
            "Transaction should succeed — condition check should promote from blob. Got: {:?}",
            resp
        );

        // Verify the value was updated
        let val = db.tree.read().unwrap().get_value(&Path::parse("/counter"));
        assert_eq!(val, Some(json!(43)));
    })
}

// --- handle_compaction_complete tests ---

#[test]
fn test_compaction_complete_trims_old_entries() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({"x": 1})).await;

        // Manually populate pending_wal_entries with entries at different sequences
        db.pending_wal_entries = vec![
            {
                let mut e = WalEntry::set("/a", json!(1));
                e.sequence = 1;
                e
            },
            {
                let mut e = WalEntry::set("/b", json!(2));
                e.sequence = 2;
                e
            },
            {
                let mut e = WalEntry::set("/c", json!(3));
                e.sequence = 3;
                e
            },
            {
                let mut e = WalEntry::set("/d", json!(4));
                e.sequence = 4;
                e
            },
            {
                let mut e = WalEntry::set("/e", json!(5));
                e.sequence = 5;
                e
            },
        ];

        // Compact through sequence 3 — entries 1, 2, 3 should be trimmed
        db.handle_compaction_complete(CompactionComplete {
            sequence: 3,
            blob_generation: 0,
            cached_io: None,
        })
        .await;

        assert_eq!(db.pending_wal_entries.len(), 2);
        assert_eq!(db.pending_wal_entries[0].sequence, 4);
        assert_eq!(db.pending_wal_entries[1].sequence, 5);
    })
}

#[test]
fn test_compaction_complete_updates_blob_sequence() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({"x": 1})).await;

        assert_eq!(db.blob_sequence, 0);

        db.handle_compaction_complete(CompactionComplete {
            sequence: 42,
            blob_generation: 0,
            cached_io: None,
        })
        .await;
        assert_eq!(db.blob_sequence, 42);

        db.handle_compaction_complete(CompactionComplete {
            sequence: 100,
            blob_generation: 0,
            cached_io: None,
        })
        .await;
        assert_eq!(db.blob_sequence, 100);
    })
}

#[test]
fn test_compaction_complete_no_entries_is_noop() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({"x": 1})).await;

        // No pending_wal_entries at all
        assert!(db.pending_wal_entries.is_empty());

        // Should not panic or fail
        db.handle_compaction_complete(CompactionComplete {
            sequence: 10,
            blob_generation: 0,
            cached_io: None,
        })
        .await;

        assert_eq!(db.blob_sequence, 10);
        assert!(db.pending_wal_entries.is_empty());
    })
}

#[test]
fn test_compaction_complete_all_entries_trimmed() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({"x": 1})).await;

        db.pending_wal_entries = vec![
            {
                let mut e = WalEntry::set("/a", json!(1));
                e.sequence = 1;
                e
            },
            {
                let mut e = WalEntry::set("/b", json!(2));
                e.sequence = 2;
                e
            },
        ];

        // Compact through sequence 5 — all entries should be trimmed
        db.handle_compaction_complete(CompactionComplete {
            sequence: 5,
            blob_generation: 0,
            cached_io: None,
        })
        .await;

        assert!(db.pending_wal_entries.is_empty());
        assert_eq!(db.blob_sequence, 5);
    })
}

#[test]
fn test_compaction_complete_none_trimmed() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({"x": 1})).await;

        db.pending_wal_entries = vec![
            {
                let mut e = WalEntry::set("/a", json!(10));
                e.sequence = 10;
                e
            },
            {
                let mut e = WalEntry::set("/b", json!(11));
                e.sequence = 11;
                e
            },
        ];

        // Compact through sequence 5 — no entries should be trimmed (all > 5)
        db.handle_compaction_complete(CompactionComplete {
            sequence: 5,
            blob_generation: 0,
            cached_io: None,
        })
        .await;

        assert_eq!(db.pending_wal_entries.len(), 2);
        assert_eq!(db.blob_sequence, 5);
    })
}

#[test]
fn test_compaction_complete_progressive_trimming() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({"x": 1})).await;

        db.pending_wal_entries = vec![
            {
                let mut e = WalEntry::set("/a", json!(1));
                e.sequence = 1;
                e
            },
            {
                let mut e = WalEntry::set("/b", json!(2));
                e.sequence = 2;
                e
            },
            {
                let mut e = WalEntry::set("/c", json!(3));
                e.sequence = 3;
                e
            },
            {
                let mut e = WalEntry::set("/d", json!(4));
                e.sequence = 4;
                e
            },
        ];

        // First compaction: trim through seq 1
        db.handle_compaction_complete(CompactionComplete {
            sequence: 1,
            blob_generation: 0,
            cached_io: None,
        })
        .await;
        assert_eq!(db.pending_wal_entries.len(), 3);
        assert_eq!(db.blob_sequence, 1);

        // Second compaction: trim through seq 3
        db.handle_compaction_complete(CompactionComplete {
            sequence: 3,
            blob_generation: 0,
            cached_io: None,
        })
        .await;
        assert_eq!(db.pending_wal_entries.len(), 1);
        assert_eq!(db.pending_wal_entries[0].sequence, 4);
        assert_eq!(db.blob_sequence, 3);

        // Third compaction: trim through seq 4
        db.handle_compaction_complete(CompactionComplete {
            sequence: 4,
            blob_generation: 0,
            cached_io: None,
        })
        .await;
        assert!(db.pending_wal_entries.is_empty());
        assert_eq!(db.blob_sequence, 4);
    })
}

#[test]
fn test_compaction_complete_promotion_uses_remaining_entries() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "users": {"alice": {"name": "Alice"}}
        }))
        .await;

        // Simulate WAL entries: seq 1 sets alice's score, seq 2 sets bob
        db.pending_wal_entries = vec![
            {
                let mut e = WalEntry::set("/users/alice/score", json!(100));
                e.sequence = 1;
                e
            },
            {
                let mut e = WalEntry::set("/users/bob", json!({"name": "Bob"}));
                e.sequence = 2;
                e
            },
        ];

        // Compact through seq 1 — alice/score is now in blob, bob entry remains
        db.handle_compaction_complete(CompactionComplete {
            sequence: 1,
            blob_generation: 0,
            cached_io: None,
        })
        .await;
        assert_eq!(db.pending_wal_entries.len(), 1);
        assert_eq!(db.pending_wal_entries[0].path, "/users/bob");

        // Now promote /users — the blob has the original data,
        // and only the remaining WAL entry (bob) should be replayed
        let loaded = db.promote_path("/users").await.unwrap();
        assert!(loaded);

        // Alice should exist from blob (score was compacted into blob already)
        let alice = db
            .tree
            .read()
            .unwrap()
            .get_value(&Path::parse("/users/alice"));
        assert!(alice.is_some(), "Alice should exist from blob");

        // Bob should exist from remaining WAL entry replay
        let bob = db
            .tree
            .read()
            .unwrap()
            .get_value(&Path::parse("/users/bob"));
        assert_eq!(bob, Some(json!({"name": "Bob"})));
    })
}

// =========================================================================
// Shallow Read Tests
// =========================================================================

/// Helper: build a shallow once request for a path.
fn shallow_once_msg(path: &str, request_id: &str) -> ClientMessage {
    ClientMessage {
        op: "o".to_string(),
        path: Some(path.to_string()),
        request_id: Some(request_id.to_string()),
        shallow: Some(true),
        ..Default::default()
    }
}

/// Extract the once_value from a response as serde_json::Value.
fn extract_once_value(resp: &ServerMessage) -> Option<Value> {
    resp.once_value.as_ref().map(|v| v.to_value())
}

/// Assert a shallow child is a container marker ({".sz": <positive int>}).
fn assert_is_size_marker(val: &Value, context: &str) {
    let obj = val
        .as_object()
        .unwrap_or_else(|| panic!("{}: expected object, got {:?}", context, val));
    assert!(
        obj.contains_key(".sz"),
        "{}: expected .sz key, got {:?}",
        context,
        obj
    );
    let sz = obj[".sz"]
        .as_i64()
        .unwrap_or_else(|| panic!("{}: .sz should be integer", context));
    assert!(
        sz >= 0,
        "{}: .sz should be non-negative, got {}",
        context,
        sz
    );
}

// --- Shallow Test 1: Basic shallow read returns container markers from blob ---
#[test]
fn test_shallow_once_basic_blob() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "characters": {"alice": {"hp": 100}},
            "chat": {"msg1": {"text": "hello"}},
            "config": {"mode": "dark"}
        }))
        .await;
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        let msg = shallow_once_msg("/", "r1");
        let resp = db.handle_once("client1", &msg).await.unwrap();
        assert!(resp.nack.is_none(), "Shallow once should succeed");

        let val = extract_once_value(&resp).unwrap();
        let obj = val.as_object().unwrap();
        assert_eq!(obj.len(), 3);
        assert_is_size_marker(&obj["characters"], "characters");
        assert_is_size_marker(&obj["chat"], "chat");
        assert_is_size_marker(&obj["config"], "config");
    })
}

// --- Shallow Test 2: Shallow read at nested path ---
#[test]
fn test_shallow_once_nested_path() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "characters": {
                "alice": {"hp": 100, "name": "Alice"},
                "bob": {"hp": 50, "name": "Bob"}
            }
        }))
        .await;
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        let msg = shallow_once_msg("/characters", "r1");
        let resp = db.handle_once("client1", &msg).await.unwrap();
        assert!(resp.nack.is_none());

        let val = extract_once_value(&resp).unwrap();
        let obj = val.as_object().unwrap();
        assert_eq!(obj.len(), 2);
        assert_is_size_marker(&obj["alice"], "alice");
        assert_is_size_marker(&obj["bob"], "bob");
    })
}

// --- Shallow Test 3: Shallow read on non-existent path returns null ---
#[test]
fn test_shallow_once_nonexistent_path() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "users": {"alice": {"hp": 100}}
        }))
        .await;
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        let msg = shallow_once_msg("/nonexistent", "r1");
        let resp = db.handle_once("client1", &msg).await.unwrap();
        assert!(resp.nack.is_none());

        let val = extract_once_value(&resp).unwrap();
        assert_eq!(val, json!(null));
    })
}

// --- Shallow Test 4: Shallow read on a leaf returns the leaf value ---
#[test]
fn test_shallow_once_leaf_value() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "users": {"alice": {"hp": 100, "name": "Alice"}}
        }))
        .await;
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        let msg = shallow_once_msg("/users/alice/hp", "r1");
        let resp = db.handle_once("client1", &msg).await.unwrap();
        assert!(resp.nack.is_none());

        let val = extract_once_value(&resp).unwrap();
        assert_eq!(val, json!(100));
    })
}

// --- Shallow Test 5: Shallow read with WAL entries adding children ---
#[test]
fn test_shallow_once_wal_adds_children() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "characters": {
                "alice": {"hp": 100}
            }
        }))
        .await;
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Write a new child via SET — this goes into pending_wal_entries
        let set_msg = ClientMessage {
            op: "s".to_string(),
            path: Some("/characters/bob".to_string()),
            value: Some(json!({"hp": 50})),
            request_id: Some("w1".to_string()),
            ..Default::default()
        };
        db.handle_set("client1", &set_msg, false).await;

        // Shallow read should include both blob key (alice) and WAL key (bob)
        let msg = shallow_once_msg("/characters", "r1");
        let resp = db.handle_once("client1", &msg).await.unwrap();
        assert!(resp.nack.is_none());

        let val = extract_once_value(&resp).unwrap();
        let obj = val.as_object().unwrap();
        assert_eq!(obj.len(), 2);
        assert_is_size_marker(&obj["alice"], "alice from blob");
        // bob from WAL: SET to {"hp": 50} which is an object → size marker
        assert_is_size_marker(&obj["bob"], "bob from WAL");
    })
}

// --- Shallow Test 6: Shallow read with WAL entry deleting a child ---
#[test]
fn test_shallow_once_wal_deletes_child() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "characters": {
                "alice": {"hp": 100},
                "bob": {"hp": 50}
            }
        }))
        .await;
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Delete bob via REMOVE
        let del_msg = ClientMessage {
            op: "r".to_string(),
            path: Some("/characters/bob".to_string()),
            request_id: Some("w1".to_string()),
            ..Default::default()
        };
        db.handle_remove("client1", &del_msg, false).await;

        // Shallow read should only have alice
        let msg = shallow_once_msg("/characters", "r1");
        let resp = db.handle_once("client1", &msg).await.unwrap();
        assert!(resp.nack.is_none());

        let val = extract_once_value(&resp).unwrap();
        let obj = val.as_object().unwrap();
        assert_eq!(obj.len(), 1);
        assert_is_size_marker(&obj["alice"], "alice");
    })
}

// --- Shallow Test 7: Shallow read with data already promoted in tree ---
#[test]
fn test_shallow_once_data_already_in_tree() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "users": {
                "alice": {"name": "Alice"},
                "bob": {"name": "Bob"}
            }
        }))
        .await;
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Promote the data into the tree first
        db.promote_path_deep("/users").await.unwrap();

        // Shallow read should use the tree path (already loaded)
        let msg = shallow_once_msg("/users", "r1");
        let resp = db.handle_once("client1", &msg).await.unwrap();
        assert!(resp.nack.is_none());

        let val = extract_once_value(&resp).unwrap();
        let obj = val.as_object().unwrap();
        assert_eq!(obj.len(), 2);
        // alice and bob are objects in the tree → size markers
        assert_is_size_marker(&obj["alice"], "alice");
        assert_is_size_marker(&obj["bob"], "bob");
    })
}

// --- Shallow Test 8: Shallow read on non-blob-backed (ephemeral) database ---
#[test]
fn test_shallow_once_ephemeral_db() {
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Set some data directly in the tree
        {
            let mut tree = db.tree.write().unwrap();
            tree.set(&Path::parse("/a"), json!(1));
            tree.set(&Path::parse("/b"), json!("hello"));
            tree.set(&Path::parse("/c"), json!({"nested": true}));
        }

        let msg = shallow_once_msg("/", "r1");
        let resp = db.handle_once("client1", &msg).await.unwrap();
        assert!(resp.nack.is_none());

        let val = extract_once_value(&resp).unwrap();
        let obj = val.as_object().unwrap();
        assert_eq!(obj.len(), 3);
        // a and b are primitives → actual values
        assert_eq!(obj["a"], json!(1));
        assert_eq!(obj["b"], json!("hello"));
        // c is an object → size marker
        assert_is_size_marker(&obj["c"], "c");
    })
}

// --- Shallow Test 9: WAL deep descendant write implies child key exists ---
#[test]
fn test_shallow_once_wal_deep_descendant() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "data": {}
        }))
        .await;
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Write to a deep path — /data/users/alice/score
        // This should make "users" appear as a child key of /data
        let set_msg = ClientMessage {
            op: "s".to_string(),
            path: Some("/data/users/alice/score".to_string()),
            value: Some(json!(100)),
            request_id: Some("w1".to_string()),
            ..Default::default()
        };
        db.handle_set("client1", &set_msg, false).await;

        let msg = shallow_once_msg("/data", "r1");
        let resp = db.handle_once("client1", &msg).await.unwrap();
        assert!(resp.nack.is_none());

        let val = extract_once_value(&resp).unwrap();
        let obj = val.as_object().unwrap();
        assert_eq!(obj.len(), 1);
        // users is implied container from deep descendant write → size marker (size=0)
        assert_is_size_marker(&obj["users"], "users");
    })
}

// --- Shallow Test 10: WAL SET at exact path replaces children ---
#[test]
fn test_shallow_once_wal_set_replaces_node() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "config": {
                "old_key1": "a",
                "old_key2": "b"
            }
        }))
        .await;
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // SET /config to a completely new object
        let set_msg = ClientMessage {
            op: "s".to_string(),
            path: Some("/config".to_string()),
            value: Some(json!({"new_key": "value"})),
            request_id: Some("w1".to_string()),
            ..Default::default()
        };
        db.handle_set("client1", &set_msg, false).await;

        let msg = shallow_once_msg("/config", "r1");
        let resp = db.handle_once("client1", &msg).await.unwrap();
        assert!(resp.nack.is_none());

        let val = extract_once_value(&resp).unwrap();
        // Old keys gone, new_key is a string primitive → actual value
        assert_eq!(val, json!({"new_key": "value"}));
    })
}

// --- Shallow Test 11: Mixed primitive and container children ---
#[test]
fn test_shallow_once_mixed_children() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "game": {
                "title": "My Game",
                "version": 2,
                "active": true,
                "characters": {"alice": {"hp": 100}},
                "settings": {"volume": 80}
            }
        }))
        .await;
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        let msg = shallow_once_msg("/game", "r1");
        let resp = db.handle_once("client1", &msg).await.unwrap();
        assert!(resp.nack.is_none());

        let val = extract_once_value(&resp).unwrap();
        let obj = val.as_object().unwrap();
        assert_eq!(obj.len(), 5);
        // Primitives → actual values
        assert_eq!(obj["title"], json!("My Game"));
        assert_eq!(obj["version"], json!(2));
        assert_eq!(obj["active"], json!(true));
        // Containers → size markers
        assert_is_size_marker(&obj["characters"], "characters");
        assert_is_size_marker(&obj["settings"], "settings");
    })
}

// --- Shallow Test 12: Shallow read on string leaf ---
#[test]
fn test_shallow_once_string_leaf() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "message": {
                "body": "Hello!"
            }
        }))
        .await;
        let (conn, _messages) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Shallow read at the leaf string — should return the string directly
        let msg = shallow_once_msg("/message/body", "r1");
        let resp = db.handle_once("client1", &msg).await.unwrap();
        assert!(resp.nack.is_none());

        let val = extract_once_value(&resp).unwrap();
        assert_eq!(val, json!("Hello!"));
    })
}

// --- Repro: TRANSACTION (multi-path PATCH) on blob-backed DB uses tree.set
// instead of tree.set_lazy, so Sentinel-root walks create EMPTY OBJECT
// intermediates that lie about being fully loaded. ---
//
// Production access pattern (wastingtime-server/src/db.rs:handle_save_character):
//   PATCH at root with leaf paths:
//     accounts/<acct>/characters/<cid>/level
//     accounts/<acct>/characters/<cid>/zone_id
//     accounts/<acct>/characters/<cid>/last_played_ms
//     character_names/<name> = <char_id>
//
// The Firebase adapter translates this to a TRANSACTION with individual
// SET ops (firebase_adapter.rs translate_merge with has_path_keys=true).
//
// After the transaction runs on a fresh (Sentinel-rooted) blob-backed DB,
// a once() at /accounts/<acct>/characters should return the FULL data
// (8 chars × 5 fields each, from the blob) — not just whatever leaves the
// transaction wrote.
//
// This test FAILS today: once() returns only the c1 character with only
// the 3 fields the transaction wrote.
#[test]
fn test_repro_transaction_then_once_returns_partial() {
    block_on(async {
        // Seed blob with full character data.
        let mut chars = serde_json::Map::new();
        for id in &["c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"] {
            chars.insert(
                id.to_string(),
                json!({
                    "class_id": "sorcerer",
                    "character_name": format!("Char-{}", id),
                    "last_played_ms": 1000_i64,
                    "zone_id": "greenhollow",
                    "level": 30,
                }),
            );
        }
        let (mut db, _dir) = make_blob_backed_db(json!({
            "accounts": {"A": {"characters": Value::Object(chars)}},
            "character_names": {
                "sorcerertest": "c1"
            }
        }))
        .await;
        let (conn, _msgs) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Simulate the multi-path PATCH from handle_save_character — a
        // TRANSACTION with leaf-path SETs. (No prior once() / subscribe;
        // the DB is fresh so the tree is Sentinel-rooted.)
        let now_ms = 9999_i64;
        let tx_msg = ClientMessage {
            op: "t".to_string(),
            request_id: Some("tx1".to_string()),
            operations: Some(vec![
                crate::protocol::TransactionOp {
                    op: "s".to_string(),
                    path: "/accounts/A/characters/c1/level".to_string(),
                    value: Some(json!(99)),
                    hash: None,
                },
                crate::protocol::TransactionOp {
                    op: "s".to_string(),
                    path: "/accounts/A/characters/c1/zone_id".to_string(),
                    value: Some(json!("newzone")),
                    hash: None,
                },
                crate::protocol::TransactionOp {
                    op: "s".to_string(),
                    path: "/accounts/A/characters/c1/last_played_ms".to_string(),
                    value: Some(json!(now_ms)),
                    hash: None,
                },
            ]),
            ..Default::default()
        };
        let resp = db
            .handle_transaction("client1", &tx_msg)
            .await
            .expect("transaction should respond");
        assert!(resp.nack.is_none(), "tx should ack: {:?}", resp);

        // Inspect raw tree.
        {
            let tree = db.tree.read().unwrap();
            let p = tree.get(&Path::parse("/accounts/A/characters")).cloned();
            eprintln!(
                "/accounts/A/characters variant: {:?}",
                p.as_ref().map(|v| match v {
                    ArcValue::Object(_) => "Object",
                    ArcValue::Sentinel(_) => "Sentinel",
                    _ => "other",
                })
            );
            if let Some(ArcValue::Object(map)) | Some(ArcValue::Sentinel(map)) = &p {
                eprintln!(
                    "  has {} children: {:?}",
                    map.len(),
                    map.keys().collect::<Vec<_>>()
                );
            }
            eprintln!("sentinel_paths = {:?}", db.sentinel_paths);
        }

        // once() should return ALL 8 chars with ALL 5 fields each.
        let msg = ClientMessage {
            op: "o".to_string(),
            path: Some("/accounts/A/characters".to_string()),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        let response = db
            .handle_once("client1", &msg)
            .await
            .expect("expected response");
        assert!(
            response.nack.is_none(),
            "once() should succeed: {:?}",
            response
        );
        let val = response
            .once_value
            .map(|v| v.to_value())
            .unwrap_or(Value::Null);
        eprintln!(
            "once(/accounts/A/characters) = {}",
            serde_json::to_string_pretty(&val).unwrap()
        );

        let obj = val.as_object().expect("expected object response");
        assert_eq!(
            obj.len(),
            8,
            "should have 8 chars from blob, got {}: {}",
            obj.len(),
            val
        );
        for id in &["c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"] {
            let c = obj
                .get(*id)
                .unwrap_or_else(|| panic!("missing char {}", id));
            for f in &[
                "class_id",
                "character_name",
                "last_played_ms",
                "zone_id",
                "level",
            ] {
                assert!(c.get(f).is_some(), "char {} missing {}: {}", id, f, c);
            }
        }
    })
}

// --- Repro: TRANSACTION UPDATE on a blob-backed Sentinel-rooted DB
// creates Object intermediates and only writes the updated keys, losing
// the other fields from the blob. ---
#[test]
fn test_repro_transaction_update_loses_blob_fields() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "characters": {
                "c1": {
                    "class_id": "sorcerer",
                    "character_name": "Alice",
                    "level": 30,
                    "zone_id": "greenhollow",
                    "last_played_ms": 1000_i64,
                }
            }
        }))
        .await;
        let (conn, _msgs) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // UPDATE /characters/c1 with a subset of fields, in a transaction.
        // (Multi-path PATCH could route through here too, but explicit
        // UPDATE is the more direct exercise.)
        let tx_msg = ClientMessage {
            op: "t".to_string(),
            request_id: Some("tx1".to_string()),
            operations: Some(vec![crate::protocol::TransactionOp {
                op: "u".to_string(),
                path: "/characters/c1".to_string(),
                value: Some(json!({
                    "level": 99,
                    "zone_id": "newzone",
                })),
                hash: None,
            }]),
            ..Default::default()
        };
        let resp = db
            .handle_transaction("client1", &tx_msg)
            .await
            .expect("tx response");
        assert!(resp.nack.is_none(), "tx should ack: {:?}", resp);

        // once() should return all 5 fields, with level/zone_id updated.
        let msg = ClientMessage {
            op: "o".to_string(),
            path: Some("/characters/c1".to_string()),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        let response = db
            .handle_once("client1", &msg)
            .await
            .expect("expected response");
        assert!(response.nack.is_none());
        let val = response
            .once_value
            .map(|v| v.to_value())
            .unwrap_or(Value::Null);
        eprintln!("once(/characters/c1) = {}", val);

        assert_eq!(val.get("class_id"), Some(&json!("sorcerer")));
        assert_eq!(val.get("character_name"), Some(&json!("Alice")));
        assert_eq!(val.get("last_played_ms"), Some(&json!(1000)));
        assert_eq!(val.get("level"), Some(&json!(99)));
        assert_eq!(val.get("zone_id"), Some(&json!("newzone")));
    })
}

// --- Repro: TRANSACTION DELETE doesn't clean sentinel_paths — leaves
// stale entries that match nothing in the tree. ---
#[test]
fn test_repro_transaction_delete_leaks_sentinel_tracking() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "users": {
                "alice": {"name": "Alice", "score": 100},
                "bob": {"name": "Bob", "score": 200}
            }
        }))
        .await;
        let (conn, _msgs) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Promote /users via shallow read so sentinel_paths gets populated
        // with the container children's paths.
        db.promote_path("/users").await.unwrap();
        assert!(
            db.sentinel_paths.contains("/users/alice"),
            "shallow promote should track alice as Sentinel child"
        );
        assert!(
            db.sentinel_paths.contains("/users/bob"),
            "shallow promote should track bob as Sentinel child"
        );

        // Now delete /users/alice via a transaction.
        let tx_msg = ClientMessage {
            op: "t".to_string(),
            request_id: Some("tx1".to_string()),
            operations: Some(vec![crate::protocol::TransactionOp {
                op: "d".to_string(),
                path: "/users/alice".to_string(),
                value: None,
                hash: None,
            }]),
            ..Default::default()
        };
        let resp = db
            .handle_transaction("client1", &tx_msg)
            .await
            .expect("tx response");
        assert!(resp.nack.is_none(), "delete tx should ack");

        // After delete, /users/alice should NOT be tracked as a Sentinel
        // anymore — the path doesn't exist.
        assert!(
            !db.sentinel_paths.contains("/users/alice"),
            "DELETE should remove sentinel_paths entry, but found stale: {:?}",
            db.sentinel_paths
        );
    })
}

// --- Repro: TRANSACTION condition check on a container path uses shallow
// promotion, which leaves container children as Sentinels. They serialize
// to null, breaking value-equality and hash comparisons. ---
#[test]
fn test_repro_transaction_condition_on_container_path_fails() {
    block_on(async {
        // Blob has /config = { feature_a: { enabled: true }, theme: "dark" }
        // The condition check expects an exact match on /config.
        let (mut db, _dir) = make_blob_backed_db(json!({
            "config": {
                "feature_a": {"enabled": true},
                "theme": "dark"
            }
        }))
        .await;
        let (conn, _msgs) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Transaction: condition that /config equals its actual value, then SET something.
        let expected_config = json!({
            "feature_a": {"enabled": true},
            "theme": "dark"
        });
        let tx_msg = ClientMessage {
            op: "t".to_string(),
            request_id: Some("tx1".to_string()),
            operations: Some(vec![
                crate::protocol::TransactionOp {
                    op: "c".to_string(),
                    path: "/config".to_string(),
                    value: Some(expected_config),
                    hash: None,
                },
                crate::protocol::TransactionOp {
                    op: "s".to_string(),
                    path: "/marker".to_string(),
                    value: Some(json!("did_run")),
                    hash: None,
                },
            ]),
            ..Default::default()
        };
        let resp = db
            .handle_transaction("client1", &tx_msg)
            .await
            .expect("tx response");

        // Condition should pass — config in blob matches expected.
        // With the shallow-promote bug, the condition compares against
        // {feature_a: null, theme: "dark"} which fails.
        assert!(
            resp.nack.is_none(),
            "condition on container path should pass, got: {:?}",
            resp
        );
        assert_eq!(resp.error.as_deref(), None);
    })
}

// --- Repro: TRANSACTION at /character_names/foo creates empty-Object
// /character_names intermediate, then once() at /character_names/sorcerertest
// returns null (instead of reading from blob). ---
#[test]
fn test_repro_transaction_then_sibling_once_returns_null() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "character_names": {
                "sorcerertest": "c1",
                "alice": "c2",
                "bob": "c3"
            }
        }))
        .await;
        let (conn, _msgs) = MockConnection::new();
        db.add_client_internal("client1", None, "conn1", conn);

        // Transaction writes a NEW name reservation. Should not affect
        // existing /character_names/sorcerertest.
        let tx_msg = ClientMessage {
            op: "t".to_string(),
            request_id: Some("tx1".to_string()),
            operations: Some(vec![crate::protocol::TransactionOp {
                op: "s".to_string(),
                path: "/character_names/newchar".to_string(),
                value: Some(json!("c99")),
                hash: None,
            }]),
            ..Default::default()
        };
        let resp = db
            .handle_transaction("client1", &tx_msg)
            .await
            .expect("tx response");
        assert!(resp.nack.is_none(), "tx should ack");

        // Inspect tree.
        {
            let tree = db.tree.read().unwrap();
            let p = tree.get(&Path::parse("/character_names")).cloned();
            eprintln!(
                "/character_names variant: {:?}",
                p.as_ref().map(|v| match v {
                    ArcValue::Object(_) => "Object",
                    ArcValue::Sentinel(_) => "Sentinel",
                    _ => "other",
                })
            );
            if let Some(ArcValue::Object(map)) | Some(ArcValue::Sentinel(map)) = &p {
                eprintln!("  keys: {:?}", map.keys().collect::<Vec<_>>());
            }
        }

        // Read the EXISTING sorcerertest entry — should return "c1" from blob.
        let msg = ClientMessage {
            op: "o".to_string(),
            path: Some("/character_names/sorcerertest".to_string()),
            request_id: Some("r1".to_string()),
            ..Default::default()
        };
        let response = db
            .handle_once("client1", &msg)
            .await
            .expect("expected response");
        assert!(response.nack.is_none(), "once() should succeed");
        let val = response
            .once_value
            .map(|v| v.to_value())
            .unwrap_or(Value::Null);
        eprintln!("once(/character_names/sorcerertest) = {}", val);

        assert_eq!(val, json!("c1"), "should read 'c1' from blob, got {}", val);
    })
}

// Sanity tests for `find_sentinel_tracking_violations`: the helper must
// return empty when the tree's Sentinels are correctly tracked, and must
// report violations when they're not. Real-scenario coverage lives in
// integration tests / chaos-monkey.
#[test]
fn test_find_sentinel_tracking_violations_clean_tree() {
    block_on(async {
        // Fresh blob-backed DB: root is empty Sentinel and "/" is in
        // sentinel_paths (per load_from_disk init). Invariant holds.
        let (db, _dir) = make_blob_backed_db(json!({"a": 1})).await;
        let violations = db.find_sentinel_tracking_violations();
        assert!(
            violations.is_empty(),
            "fresh DB must have all Sentinels tracked, got violations: {:?}",
            violations
        );
    })
}

#[test]
fn test_find_sentinel_tracking_violations_after_promote() {
    block_on(async {
        let (mut db, _dir) = make_blob_backed_db(json!({
            "users": {"alice": {"name": "Alice"}}
        }))
        .await;
        db.promote_path_deep("/users/alice").await.unwrap();
        // After deep promotion at /users/alice:
        //   - /users/alice = Object (loaded)
        //   - /users = Sentinel-with-children {alice: Object}
        //   - / = Sentinel-with-children {users: Sentinel{...}}
        // Both Sentinels must be tracked in sentinel_paths.
        let violations = db.find_sentinel_tracking_violations();
        assert!(
            violations.is_empty(),
            "all Sentinels must be tracked after deep promotion: {:?}",
            violations
        );
    })
}

#[test]
fn test_find_sentinel_tracking_violations_detects_stale_missing() {
    block_on(async {
        let (db, _dir) = make_blob_backed_db(json!({"a": 1})).await;
        // Inject an in-tree Sentinel at /a/b without adding to the set.
        // (Simulates a buggy code path that creates a Sentinel-with-children
        // and forgets to call track_sentinels_after_write.)
        db.tree
            .write()
            .unwrap()
            .set_arc_uncleaned_lazy(&Path::parse("/a/b"), ArcValue::empty_sentinel());
        // Note: parent /a is also now a Sentinel-with-children (set_path_mut_sentinel
        // walks through and creates Sentinel intermediates). And root contains
        // /a as a Sentinel too. Only /a and /a/b are NEW Sentinels in this DB
        // (root was already Sentinel from init and "/" is tracked).

        let violations = db.find_sentinel_tracking_violations();
        assert!(
            !violations.is_empty(),
            "untracked in-tree Sentinel must be reported as a violation"
        );
        assert!(
            violations.iter().any(|p| p == "/a/b"),
            "/a/b must appear in violations, got: {:?}",
            violations
        );
    })
}

// `promote_path_shallow`'s `Err(BlobError::PathNotFound)` branch writes a
// Null marker via `set_arc_uncleaned_lazy` without checking that the
// parent path is an Object container. If the in-memory parent is a
// primitive (reachable via a race with a concurrent SET that turns the
// parent into a primitive between `promote_path`'s tree-state check and
// `promote_path_shallow`'s blob-read await point), the marker write walks
// through the primitive and Sentinel-clobbers it — losing the primitive's
// value in memory.
//
// This test simulates the post-race state directly: install a primitive
// at the parent path, call `promote_path_shallow` on a descendant whose
// blob path doesn't exist, and assert the primitive is preserved.
//
// Mirrors the parent-Object guard already present in `promote_path` and
// `promote_path_deep`.
#[test]
fn test_promote_path_shallow_pathnotfound_preserves_primitive_parent() {
    block_on(async {
        // Blob has no /a — read_shallow at /a/b will return PathNotFound.
        let (mut db, _dir) = make_blob_backed_db(json!({
            "unrelated": "value"
        }))
        .await;

        // Simulate the post-race state: in-memory tree has /a as a
        // primitive (e.g., a concurrent SET /a = 5 turned the Sentinel
        // parent into a Number between the parent check and the blob read).
        db.tree
            .write()
            .unwrap()
            .set_lazy(&Path::parse("/a"), json!(5));

        assert_eq!(
            db.tree.read().unwrap().get_value(&Path::parse("/a")),
            Some(json!(5)),
            "precondition: /a is the primitive 5",
        );

        // Invoke the broken branch directly. The pre-fix code calls
        // `set_arc_uncleaned_lazy(/a/b, Null)` which walks /a (primitive)
        // and clobbers it into a Sentinel container.
        let _ = db.promote_path_shallow("/a/b").await;

        // Post-fix: /a is still the primitive 5.
        // Pre-fix: /a is a Sentinel{b: Null}, primitive value lost.
        assert_eq!(
            db.tree.read().unwrap().get_value(&Path::parse("/a")),
            Some(json!(5)),
            "primitive /a must be preserved — promote_path_shallow's PathNotFound branch \
                 must not write a Null marker through a primitive parent",
        );
    })
}

/// Regression: rules-eval retry loop used to spin to exhaustion when a
/// rule referenced a path that didn't exist in the blob AND the path's
/// parent wasn't loaded in the in-memory tree. Old `promote_path_shallow`
/// PathNotFound branch would skip the marker write (parent absent →
/// `parent_is_container = false`), leaving the tree state unchanged —
/// every iteration re-asked for the same path. This test exercises
/// that scenario directly: blob has no `/a/b/c/d`, `/a` is loaded as
/// a Sentinel from a shallow promote, and `/b`/`/c` are absent. After
/// `promote_path_shallow`, the leaf must be marked as Null so the
/// retry loop can make progress.
#[test]
fn test_promote_path_shallow_pathnotfound_writes_marker_through_absent_ancestors() {
    block_on(async {
        // Blob has /a (with some other key) but /a/b/c/d doesn't exist.
        let (mut db, _dir) = make_blob_backed_db(json!({
            "a": {"x": 1}
        }))
        .await;

        // Force-promote root so `/a` ends up as a Sentinel-style child
        // of the root Object (this is the typical state after a
        // shallow root promote). `promote_path` shallow-promotes `/a`
        // proper as a real Object since we ask for `/a` itself —
        // which is what we want as the loaded ancestor.
        db.promote_path("/a").await.unwrap();

        // Sanity: precondition. /a is loaded, /a/b is None.
        assert!(db.tree.read().unwrap().node_is_loaded("/a"));
        assert_eq!(
            db.tree.read().unwrap().get_value(&Path::parse("/a/b")),
            None,
        );

        // Promote a deep path that doesn't exist in blob and whose
        // parents aren't in the tree.
        db.promote_path_shallow("/a/b/c/d").await.unwrap();

        // After promotion, the leaf must carry a Null marker so the
        // rules retry loop terminates on the next eval.
        assert_eq!(
            db.tree.read().unwrap().get_value(&Path::parse("/a/b/c/d")),
            Some(Value::Null),
            "leaf must be marked as Null so node_is_loaded returns true \
                 on the next iteration"
        );

        // /a/x — the unrelated existing key — must NOT have been touched.
        assert_eq!(
            db.tree.read().unwrap().get_value(&Path::parse("/a/x")),
            Some(json!(1)),
            "unrelated existing data under /a must be preserved"
        );
    })
}

// Invariant: a path that's "hot" (in promoted_paths and not idle) must be
// preserved bit-for-bit by selective eviction. The recursion into hot
// children should only walk *ancestors* of deeper hot paths — when a hot
// path itself is reached, the subtree at that path should be left alone.
//
// This catches the primitive-clobber bug in selective_evict_children where
// recursing into a hot leaf container would reach its primitive fields,
// classify them as "cold" (no further hot descendants), and Sentinel-clobber
// them via set_arc_uncleaned_lazy.
#[test]
fn test_selective_eviction_preserves_hot_subtree() {
    block_on(async {
        let mut chars = serde_json::Map::new();
        let char_ids = ["a", "b", "c", "d", "e", "f", "g", "h"];
        for id in char_ids.iter() {
            chars.insert(
                id.to_string(),
                json!({
                    "class_id": "sorcerer",
                    "character_name": format!("Char-{}", id),
                    "last_played_ms": 1000_i64,
                    "zone_id": "greenhollow",
                    "level": 30,
                }),
            );
        }
        let (mut db, _dir) = make_blob_backed_db(json!({
            "accounts": {"a1": {"characters": Value::Object(chars)}}
        }))
        .await;

        let parent = "/accounts/a1/characters";
        db.promote_path_deep(parent).await.unwrap();

        // Half of the chars are idle, half are hot. The parent itself is idle.
        let stale = Instant::now() - Duration::from_secs(600);
        let fresh = Instant::now();
        db.promoted_paths.insert(parent.to_string(), stale);
        for id in &char_ids[..4] {
            db.promoted_paths
                .insert(format!("{}/{}", parent, id), stale);
        }
        for id in &char_ids[4..] {
            db.promoted_paths
                .insert(format!("{}/{}", parent, id), fresh);
        }

        // Snapshot each hot path's tree state BEFORE eviction.
        let before: Vec<(String, ArcValue)> = char_ids[4..]
            .iter()
            .map(|id| {
                let p = format!("{}/{}", parent, id);
                let v = db
                    .tree
                    .read()
                    .unwrap()
                    .get(&Path::parse(&p))
                    .cloned()
                    .expect("hot path must exist before eviction");
                (p, v)
            })
            .collect();

        db.evict_idle_paths();

        // Each hot path's tree state must equal what it was before.
        for (p, expected) in &before {
            let after = db
                .tree
                .read()
                .unwrap()
                .get(&Path::parse(p))
                .cloned()
                .unwrap_or_else(|| panic!("hot path {} disappeared", p));
            assert_eq!(
                &after, expected,
                "selective eviction corrupted hot path {}: before={:?}, after={:?}",
                p, expected, after
            );
        }

        // Sanity: cold paths should now be Sentinel(empty).
        for id in &char_ids[..4] {
            let p = format!("{}/{}", parent, id);
            let v = db
                .tree
                .read()
                .unwrap()
                .get(&Path::parse(&p))
                .cloned()
                .expect("cold path should still exist as Sentinel");
            assert!(
                matches!(&v, ArcValue::Sentinel(m) if m.is_empty()),
                "cold path {} should be empty Sentinel, got: {:?}",
                p,
                v
            );
        }
    })
}

#[test]
fn test_drain_inbox_with_error_disconnects_pending_clients() {
    block_on(async {
        let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
        let handle = db.handle();

        // Queue two add_client messages into the inbox
        let (conn1, messages1) = MockConnection::new();
        let closed1 = conn1.closed.clone();
        let (conn2, messages2) = MockConnection::new();
        let closed2 = conn2.closed.clone();

        handle.add_client("client1".to_string(), None, "conn1".to_string(), conn1);
        handle.add_client("client2".to_string(), None, "conn2".to_string(), conn2);

        // Drain with error (simulating startup failure)
        db.drain_inbox_with_error("Database failed to initialize")
            .await;

        // Both clients should have received a nack message
        let msgs1 = messages1.lock().unwrap();
        assert_eq!(
            msgs1.len(),
            1,
            "client1 should have received exactly one message"
        );
        let parsed1: ServerMessage = serde_json::from_slice(&msgs1[0]).unwrap();
        assert_eq!(parsed1.error.as_deref(), Some("unavailable"));

        let msgs2 = messages2.lock().unwrap();
        assert_eq!(
            msgs2.len(),
            1,
            "client2 should have received exactly one message"
        );
        let parsed2: ServerMessage = serde_json::from_slice(&msgs2[0]).unwrap();
        assert_eq!(parsed2.error.as_deref(), Some("unavailable"));

        // Both connections should have been closed
        assert!(
            closed1.load(std::sync::atomic::Ordering::SeqCst),
            "client1 connection should be closed"
        );
        assert!(
            closed2.load(std::sync::atomic::Ordering::SeqCst),
            "client2 connection should be closed"
        );
    })
}
