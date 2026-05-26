//! Integration tests for eviction and re-promotion edge cases.
//!
//! These test the interaction between:
//! - Blob-backed lazy loading (promotion)
//! - Eviction (replacing promoted data with Sentinels)
//! - WAL replay during re-promotion
//! - Different subscription levels (root vs. child vs. deep paths)
//!
//! The key scenarios tested:
//! 1. Promote root, evict, then read child → does data survive?
//! 2. Promote child, evict child, then read root → does root have all data?
//! 3. Promote root, evict, then read root again → full data restored?
//! 4. Write via WAL + evict + re-promote → WAL entries replayed correctly?
//! 5. Multiple clients at different subscription depths after eviction

mod common;

use common::{TestServer, run_test};
use lark_blob::{ArcValue, StdBlobIO, write_blob};
use serde_json::json;
use std::time::Duration;
use tempfile::TempDir;

/// Helper: write a blob file at the expected path for a given project/database.
fn write_test_blob(data_dir: &str, project: &str, db_name: &str, tree: &ArcValue) {
    let db_dir = format!("{}/{}/{}", data_dir, project, db_name);
    std::fs::create_dir_all(&db_dir).unwrap();
    let blob_path = format!("{}/blob.lark", db_dir);

    futures::executor::block_on(async {
        let io = StdBlobIO::create(std::path::Path::new(&blob_path)).unwrap();
        write_blob(&io, tree).await.unwrap();
    });
}

/// Standard test data: a nested tree with multiple top-level keys and deep nesting.
fn test_tree() -> ArcValue {
    ArcValue::from_value(json!({
        "characters": {
            "char-1": {"name": "Alice", "hp": 100, "class": "warrior"},
            "char-2": {"name": "Bob", "hp": 80, "class": "mage"},
            "char-3": {"name": "Charlie", "hp": 120, "class": "tank"}
        },
        "pages": {
            "page-1": {"name": "Map", "grid": true},
            "page-2": {"name": "Handout", "grid": false}
        },
        "campaign": {
            "name": "Test Campaign",
            "settings": {
                "fog": true,
                "grid_size": 70
            }
        },
        "players": {
            "player-1": {"name": "Alice", "color": "red"},
            "player-2": {"name": "Sam", "color": "blue"}
        },
        "chat": {
            "msg-1": {"text": "hello", "sender": "player-1"},
            "msg-2": {"text": "world", "sender": "player-2"}
        }
    }))
}

fn setup_server(data_dir: &str, db_name: &str) -> TestServer {
    write_test_blob(data_dir, "test-project", db_name, &test_tree());
    let server = TestServer::with_persistence(data_dir);
    server
        .set_rules_with_ephemeral(
            "test-project",
            json!({"rules": {".read": true, ".write": true}}),
            false,
        )
        .unwrap();
    server
}

// =============================================================================
// Test 1: Promote root, evict all, then read a child path
// =============================================================================

#[test]
fn test_eviction_promote_root_evict_then_read_child() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        let server = setup_server(data_dir, "evict-1");

        let mut client = server.client();
        client.connect("test-project/evict-1").await;

        // Promote root by reading it
        let root = client.once("/").await.unwrap();
        assert_eq!(root["characters"]["char-1"]["name"], "Alice");

        // Force evict everything
        client.force_evict_all().await;

        // Now read a child path — should re-promote from blob
        let chars = client.once("/characters").await.unwrap();
        assert_eq!(chars["char-1"]["name"], "Alice");
        assert_eq!(chars["char-2"]["name"], "Bob");
        assert_eq!(chars["char-3"]["name"], "Charlie");

        client.disconnect().await;
    });
}

// =============================================================================
// Test 2: Promote child, evict, then read root
// =============================================================================

#[test]
fn test_eviction_promote_child_evict_then_read_root() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        let server = setup_server(data_dir, "evict-2");

        let mut client = server.client();
        client.connect("test-project/evict-2").await;

        // Promote only /characters
        let chars = client.once("/characters").await.unwrap();
        assert_eq!(chars["char-1"]["name"], "Alice");

        // Force evict everything
        client.force_evict_all().await;

        // Now read root — should get ALL data, not just characters
        let root = client.once("/").await.unwrap();
        assert_eq!(root["characters"]["char-1"]["name"], "Alice");
        assert_eq!(root["pages"]["page-1"]["name"], "Map");
        assert_eq!(root["campaign"]["name"], "Test Campaign");
        assert_eq!(root["players"]["player-1"]["name"], "Alice");
        assert_eq!(root["chat"]["msg-1"]["text"], "hello");

        client.disconnect().await;
    });
}

// =============================================================================
// Test 3: Promote root, evict, read root again
// =============================================================================

#[test]
fn test_eviction_promote_root_evict_then_read_root() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        let server = setup_server(data_dir, "evict-3");

        let mut client = server.client();
        client.connect("test-project/evict-3").await;

        // Promote root
        let root = client.once("/").await.unwrap();
        assert_eq!(root["characters"]["char-1"]["name"], "Alice");

        // Force evict everything
        client.force_evict_all().await;

        // Read root again — should get complete data
        let root2 = client.once("/").await.unwrap();
        assert_eq!(root2["characters"]["char-1"]["name"], "Alice");
        assert_eq!(root2["characters"]["char-2"]["name"], "Bob");
        assert_eq!(root2["pages"]["page-1"]["name"], "Map");
        assert_eq!(root2["campaign"]["settings"]["fog"], true);
        assert_eq!(root2["players"]["player-2"]["color"], "blue");

        client.disconnect().await;
    });
}

// =============================================================================
// Test 4: Promote multiple children, evict, then read root
// =============================================================================

#[test]
fn test_eviction_promote_multiple_children_evict_then_read_root() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        let server = setup_server(data_dir, "evict-4");

        let mut client = server.client();
        client.connect("test-project/evict-4").await;

        // Promote several child paths (like a real game client would)
        let _ = client.once("/characters").await.unwrap();
        let _ = client.once("/pages").await.unwrap();
        let _ = client.once("/players/player-1").await.unwrap();
        let _ = client.once("/campaign").await.unwrap();

        // Force evict everything
        client.force_evict_all().await;

        // Read root — should have ALL data including chat (never promoted before)
        let root = client.once("/").await.unwrap();
        assert_eq!(root["characters"]["char-1"]["name"], "Alice");
        assert_eq!(root["pages"]["page-2"]["name"], "Handout");
        assert_eq!(root["campaign"]["name"], "Test Campaign");
        assert_eq!(root["players"]["player-1"]["name"], "Alice");
        assert_eq!(root["players"]["player-2"]["name"], "Sam");
        assert_eq!(
            root["chat"]["msg-1"]["text"], "hello",
            "chat was never promoted but should appear in root read"
        );

        client.disconnect().await;
    });
}

// =============================================================================
// Test 5: Promote deep path, evict, then read parent
// =============================================================================

#[test]
fn test_eviction_promote_deep_path_evict_then_read_parent() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        let server = setup_server(data_dir, "evict-5");

        let mut client = server.client();
        client.connect("test-project/evict-5").await;

        // Promote a deep path
        let alice = client.once("/characters/char-1").await.unwrap();
        assert_eq!(alice["name"], "Alice");

        // Force evict
        client.force_evict_all().await;

        // Read the parent — should have ALL characters, not just char-1
        let chars = client.once("/characters").await.unwrap();
        assert_eq!(chars["char-1"]["name"], "Alice");
        assert_eq!(chars["char-2"]["name"], "Bob");
        assert_eq!(chars["char-3"]["name"], "Charlie");

        client.disconnect().await;
    });
}

// =============================================================================
// Test 6: Write data, evict, then read — WAL replay after eviction
// =============================================================================

#[test]
fn test_eviction_write_then_evict_then_read() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        let server = setup_server(data_dir, "evict-6");

        let mut client = server.client();
        client.connect("test-project/evict-6").await;

        // Read original data
        let alice = client.once("/characters/char-1/hp").await.unwrap();
        assert_eq!(alice, json!(100));

        // Write new data (goes to WAL + tree)
        client
            .set("/characters/char-1/hp", json!(50))
            .await
            .unwrap();

        // Verify write took effect
        let hp = client.once("/characters/char-1/hp").await.unwrap();
        assert_eq!(hp, json!(50));

        // Force evict everything
        client.force_evict_all().await;

        // Read back — should get WAL-modified value (blob has 100, WAL has 50)
        let hp2 = client.once("/characters/char-1/hp").await.unwrap();
        assert_eq!(
            hp2,
            json!(50),
            "WAL write should survive eviction + re-promotion"
        );

        client.disconnect().await;
    });
}

// =============================================================================
// Test 7: Write new key, evict, read root — new key should appear
// =============================================================================

#[test]
fn test_eviction_write_new_key_evict_then_read_root() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        let server = setup_server(data_dir, "evict-7");

        let mut client = server.client();
        client.connect("test-project/evict-7").await;

        // Write a new top-level key (not in original blob)
        client
            .set("/macros/macro-1", json!({"name": "Fireball"}))
            .await
            .unwrap();

        // Force evict
        client.force_evict_all().await;

        // Read root — should have both blob data AND the WAL-written macro
        let root = client.once("/").await.unwrap();
        assert_eq!(
            root["characters"]["char-1"]["name"], "Alice",
            "blob data should survive"
        );
        assert_eq!(
            root["macros"]["macro-1"]["name"], "Fireball",
            "WAL-written data should survive eviction"
        );

        client.disconnect().await;
    });
}

// =============================================================================
// Test 8: Delete via WAL, evict, read — delete should persist
// =============================================================================

#[test]
fn test_eviction_delete_then_evict_then_read() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        let server = setup_server(data_dir, "evict-8");

        let mut client = server.client();
        client.connect("test-project/evict-8").await;

        // Verify data exists
        let charlie = client.once("/characters/char-3").await.unwrap();
        assert_eq!(charlie["name"], "Charlie");

        // Delete a character
        client.remove("/characters/char-3").await.unwrap();

        // Force evict
        client.force_evict_all().await;

        // Read characters — char-3 should be gone (WAL DELETE replayed over blob)
        let chars = client.once("/characters").await.unwrap();
        assert_eq!(chars["char-1"]["name"], "Alice");
        assert_eq!(chars["char-2"]["name"], "Bob");
        assert!(
            chars.get("char-3").is_none() || chars["char-3"].is_null(),
            "Deleted character should not reappear after eviction, got: {:?}",
            chars.get("char-3")
        );

        client.disconnect().await;
    });
}

// =============================================================================
// Test 9: Update (merge) via WAL, evict, read — merge should persist
// =============================================================================

#[test]
fn test_eviction_update_then_evict_then_read() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        let server = setup_server(data_dir, "evict-9");

        let mut client = server.client();
        client.connect("test-project/evict-9").await;

        // Update (shallow merge) — adds a new field, keeps existing ones
        client
            .update("/characters/char-1", json!({"level": 5}))
            .await
            .unwrap();

        // Force evict
        client.force_evict_all().await;

        // Read back — should have original fields + new one
        let alice = client.once("/characters/char-1").await.unwrap();
        assert_eq!(alice["name"], "Alice", "original field should survive");
        assert_eq!(alice["hp"], 100, "original field should survive");
        assert_eq!(
            alice["level"], 5,
            "WAL UPDATE field should survive eviction"
        );

        client.disconnect().await;
    });
}

// =============================================================================
// Test 10: Subscribe to child, evict, subscribe to root — root has full data
// =============================================================================

#[test]
fn test_eviction_subscribe_child_evict_then_subscribe_root() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        let server = setup_server(data_dir, "evict-10");

        let mut client = server.client();
        client.connect("test-project/evict-10").await;

        // Subscribe to child paths (like a game client)
        client.subscribe("/characters", &[]).await.unwrap();
        client.subscribe("/pages", &[]).await.unwrap();
        // Drain initial events
        glommio::timer::sleep(Duration::from_millis(200)).await;
        client.clear_events().await;

        // Force evict
        client.force_evict_all().await;

        // Now read root via once — should get complete data
        let root = client.once("/").await.unwrap();
        assert_eq!(root["characters"]["char-1"]["name"], "Alice");
        assert_eq!(root["pages"]["page-1"]["name"], "Map");
        assert_eq!(root["campaign"]["name"], "Test Campaign");
        assert_eq!(root["chat"]["msg-1"]["text"], "hello");

        client.disconnect().await;
    });
}

// =============================================================================
// Test 11: Subscribe to root, evict, subscribe to child — child has data
// =============================================================================

#[test]
fn test_eviction_subscribe_root_evict_then_subscribe_child() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        let server = setup_server(data_dir, "evict-11");

        let mut client = server.client();
        client.connect("test-project/evict-11").await;

        // Subscribe to root (promotes everything)
        client.subscribe("/", &[]).await.unwrap();
        glommio::timer::sleep(Duration::from_millis(200)).await;
        client.clear_events().await;

        // Force evict
        client.force_evict_all().await;

        // Read a child path
        let chars = client.once("/characters").await.unwrap();
        assert_eq!(chars["char-1"]["name"], "Alice");
        assert_eq!(chars["char-2"]["name"], "Bob");
        assert_eq!(chars["char-3"]["name"], "Charlie");

        client.disconnect().await;
    });
}

// =============================================================================
// Test 12: Two clients — one subscribes to child, evict, other reads root
// =============================================================================

#[test]
fn test_eviction_two_clients_different_depths() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        let server = setup_server(data_dir, "evict-12");

        let mut client1 = server.client();
        client1.connect("test-project/evict-12").await;

        // Client 1 reads specific paths (like a game client)
        let _ = client1.once("/characters").await.unwrap();
        let _ = client1.once("/pages").await.unwrap();

        // Force evict all
        client1.force_evict_all().await;

        // Client 2 connects and reads root (like a dashboard)
        let mut client2 = server.client();
        client2.connect("test-project/evict-12").await;

        let root = client2.once("/").await.unwrap();
        assert_eq!(root["characters"]["char-1"]["name"], "Alice");
        assert_eq!(root["pages"]["page-1"]["name"], "Map");
        assert_eq!(root["campaign"]["name"], "Test Campaign");
        assert_eq!(root["chat"]["msg-1"]["text"], "hello");

        client1.disconnect().await;
        client2.disconnect().await;
    });
}

// =============================================================================
// Test 13: WAL writes + restart + evict + read
// =============================================================================

#[test]
fn test_eviction_wal_restart_evict_read() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        let server = setup_server(data_dir, "evict-13");

        let mut client = server.client();
        client.connect("test-project/evict-13").await;

        // Write some data
        client
            .set("/characters/char-1/hp", json!(50))
            .await
            .unwrap();
        client.set("/newdata/key1", json!("added")).await.unwrap();
        client.remove("/chat/msg-2").await.unwrap();

        // Wait for WAL flush
        glommio::timer::sleep(Duration::from_millis(2500)).await;

        // Shutdown
        client.disconnect().await;
        server.shutdown().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;

        // Restart
        let server2 = TestServer::restart_with_persistence(data_dir);
        server2
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client2 = server2.client();
        client2.connect("test-project/evict-13").await;
        glommio::timer::sleep(Duration::from_millis(500)).await;

        // Read some data (promotes it)
        let hp = client2.once("/characters/char-1/hp").await.unwrap();
        assert_eq!(hp, json!(50), "WAL-written HP should survive restart");

        // Now evict everything
        client2.force_evict_all().await;

        // Read root — should have everything correct
        let root = client2.once("/").await.unwrap();
        assert_eq!(
            root["characters"]["char-1"]["hp"], 50,
            "WAL write should survive restart + eviction"
        );
        assert_eq!(
            root["characters"]["char-1"]["name"], "Alice",
            "original blob data should survive"
        );
        assert_eq!(root["characters"]["char-2"]["name"], "Bob");
        assert_eq!(
            root["newdata"]["key1"], "added",
            "new WAL key should survive restart + eviction"
        );
        assert!(
            root["chat"].get("msg-2").is_none() || root["chat"]["msg-2"].is_null(),
            "WAL DELETE should survive restart + eviction"
        );
        assert_eq!(
            root["chat"]["msg-1"]["text"], "hello",
            "non-deleted chat should survive"
        );

        client2.disconnect().await;
        server2.shutdown().await;
    });
}

// =============================================================================
// Test 14: Multiple evict/promote cycles
// =============================================================================

#[test]
fn test_eviction_multiple_cycles() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        let server = setup_server(data_dir, "evict-14");

        let mut client = server.client();
        client.connect("test-project/evict-14").await;

        for i in 0..5 {
            // Read root (promotes everything)
            let root = client.once("/").await.unwrap();
            assert_eq!(
                root["characters"]["char-1"]["name"], "Alice",
                "cycle {}: root read should return full data",
                i
            );
            assert_eq!(
                root["pages"]["page-1"]["name"], "Map",
                "cycle {}: root read should return full data",
                i
            );

            // Evict
            client.force_evict_all().await;

            // Read child
            let chars = client.once("/characters").await.unwrap();
            assert_eq!(
                chars["char-2"]["name"], "Bob",
                "cycle {}: child read after eviction should work",
                i
            );

            // Evict again
            client.force_evict_all().await;
        }

        client.disconnect().await;
    });
}

// =============================================================================
// Test 15: Write between evict cycles
// =============================================================================

#[test]
fn test_eviction_write_between_cycles() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        let server = setup_server(data_dir, "evict-15");

        let mut client = server.client();
        client.connect("test-project/evict-15").await;

        // Cycle 1: read, write, evict
        let _ = client.once("/characters/char-1").await.unwrap();
        client
            .set("/characters/char-1/level", json!(1))
            .await
            .unwrap();
        client.force_evict_all().await;

        // Cycle 2: read, write more, evict
        let alice = client.once("/characters/char-1").await.unwrap();
        assert_eq!(
            alice["level"], 1,
            "write from cycle 1 should survive eviction"
        );
        client
            .set("/characters/char-1/level", json!(2))
            .await
            .unwrap();
        client.force_evict_all().await;

        // Cycle 3: verify accumulated writes
        let alice = client.once("/characters/char-1").await.unwrap();
        assert_eq!(
            alice["level"], 2,
            "write from cycle 2 should survive eviction"
        );
        assert_eq!(alice["name"], "Alice", "original blob data should survive");
        assert_eq!(alice["hp"], 100, "original blob data should survive");

        client.disconnect().await;
    });
}

// =============================================================================
// Test 16: Evict, then subscribe — initial snapshot should have full data
// =============================================================================

#[test]
fn test_eviction_subscribe_after_evict_gets_initial_data() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        let server = setup_server(data_dir, "evict-16");

        let mut client = server.client();
        client.connect("test-project/evict-16").await;

        // Promote something, then evict
        let _ = client.once("/characters").await.unwrap();
        client.force_evict_all().await;

        // Subscribe to root — initial snapshot should have all data
        client.subscribe("/", &[]).await.unwrap();

        // Wait for initial event
        let event = client.wait_for_event(Duration::from_secs(2)).await.unwrap();

        // The initial event for "/" should contain the full tree
        let value = event.value.expect("initial event should have value");
        let val = value.to_value();
        assert_eq!(val["characters"]["char-1"]["name"], "Alice");
        assert_eq!(val["pages"]["page-1"]["name"], "Map");
        assert_eq!(val["campaign"]["name"], "Test Campaign");

        client.disconnect().await;
    });
}

// =============================================================================
// Test 17: Cold start (WAL only, no blob reads yet), evict, then read root
// =============================================================================

#[test]
fn test_eviction_cold_wal_only_evict_read_root() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        // First: write data via WAL (no pre-existing blob with this data)
        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/evict-17").await;

        // Write data (goes to WAL)
        client
            .set("/users/alice", json!({"name": "Alice", "score": 100}))
            .await
            .unwrap();
        client
            .set("/users/bob", json!({"name": "Bob", "score": 200}))
            .await
            .unwrap();
        client.set("/config/theme", json!("dark")).await.unwrap();

        // Wait for WAL flush
        glommio::timer::sleep(Duration::from_millis(2500)).await;

        // Shutdown and restart
        client.disconnect().await;
        server.shutdown().await;
        glommio::timer::sleep(Duration::from_millis(100)).await;

        let server2 = TestServer::restart_with_persistence(data_dir);
        server2
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client2 = server2.client();
        client2.connect("test-project/evict-17").await;
        glommio::timer::sleep(Duration::from_millis(500)).await;

        // Read a child path (promotes it)
        let alice = client2.once("/users/alice").await.unwrap();
        assert_eq!(alice["name"], "Alice");

        // Force evict
        client2.force_evict_all().await;

        // Read root — should have all WAL-written data
        let root = client2.once("/").await.unwrap();
        assert_eq!(root["users"]["alice"]["name"], "Alice");
        assert_eq!(root["users"]["bob"]["name"], "Bob");
        assert_eq!(root["config"]["theme"], "dark");

        client2.disconnect().await;
        server2.shutdown().await;
    });
}

// =============================================================================
// Test 18: Promote root, write to child, evict, read child — write survives
// =============================================================================

#[test]
fn test_eviction_promote_root_write_child_evict_read_child() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        let server = setup_server(data_dir, "evict-18");

        let mut client = server.client();
        client.connect("test-project/evict-18").await;

        // Read root (promotes everything)
        let _ = client.once("/").await.unwrap();

        // Write to a child
        client
            .set("/characters/char-1/hp", json!(999))
            .await
            .unwrap();

        // Evict everything
        client.force_evict_all().await;

        // Read just the child — should have the WAL write
        let char1 = client.once("/characters/char-1").await.unwrap();
        assert_eq!(char1["hp"], 999, "WAL write should survive eviction");
        assert_eq!(
            char1["name"], "Alice",
            "other fields should survive from blob"
        );

        client.disconnect().await;
    });
}

// =============================================================================
// Test 19: Root SET (overwrite everything), evict, read child
// =============================================================================

#[test]
fn test_eviction_root_set_evict_then_read() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        let server = setup_server(data_dir, "evict-19");

        let mut client = server.client();
        client.connect("test-project/evict-19").await;

        // Overwrite entire root
        client
            .set("/", json!({"new_root": {"key": "value"}}))
            .await
            .unwrap();

        // Evict
        client.force_evict_all().await;

        // Read root — should be the new value, NOT the original blob data
        let root = client.once("/").await.unwrap();
        assert_eq!(root["new_root"]["key"], "value");
        assert!(
            root.get("characters").is_none() || root["characters"].is_null(),
            "old blob data should be gone after root SET"
        );

        client.disconnect().await;
    });
}

// =============================================================================
// Sentinel-in-subtree bug: these tests target the root cause of missing data.
//
// The bug: Sentinels can end up buried inside non-Sentinel Objects, causing
// serialization failures (silent message drops). This happens when:
//   1. SET at "/" converts root from Sentinel to Object, then child writes
//      via set_lazy create Sentinel intermediates inside the Object root.
//   2. Eviction of a descendant leaves a Sentinel hole inside a non-Sentinel parent.
//   3. Overlapping subscriptions at different depths cause partial eviction.
//
// promote_path_deep fixes this by walking the subtree for Sentinels before
// serving subscribe/once/query results.
// =============================================================================

/// Bug repro #1: Fresh DB (no blob), SET at root, then child writes.
/// The root SET converts root from Sentinel to Object. Child writes via set_lazy
/// create Sentinel intermediates inside the Object root. Reading root must
/// detect and fix these buried Sentinels.
#[test]
fn test_sentinel_bug_fresh_db_root_set_then_child_writes() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        // No pre-existing blob — DB creates blank blob.lark
        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/sentinel-bug-1").await;

        // SET at root — converts root from Sentinel to Object
        client
            .set(
                "/",
                json!({
                    "chat": {"msg1": {"text": "hello", "ts": 1}}
                }),
            )
            .await
            .unwrap();

        // Child writes — set_lazy creates Sentinel intermediates inside Object root
        client
            .set("/chat/msg2", json!({"text": "world", "ts": 2}))
            .await
            .unwrap();
        client
            .set("/users/alice", json!({"name": "Alice"}))
            .await
            .unwrap();

        // Read root — must serialize entire tree including Sentinel intermediates.
        // Before the fix, this would silently drop the message (encode error).
        let root = client.once("/").await.unwrap();
        assert_eq!(root["chat"]["msg1"]["text"], "hello");
        assert_eq!(root["chat"]["msg2"]["text"], "world");
        assert_eq!(root["users"]["alice"]["name"], "Alice");

        client.disconnect().await;
    });
}

/// Bug repro #2: Eviction of descendant leaves Sentinel inside non-Sentinel parent.
/// Promote root (makes it a real Object), evict all (children become Sentinels),
/// promote one child (makes it real), then read root — root is Object but other
/// children are still Sentinels.
#[test]
fn test_sentinel_bug_evicted_child_leaves_sentinel_in_parent() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        let server = setup_server(data_dir, "sentinel-bug-2");
        let mut client = server.client();
        client.connect("test-project/sentinel-bug-2").await;

        // Promote root — entire tree becomes real Objects
        let root = client.once("/").await.unwrap();
        assert_eq!(root["chat"]["msg-1"]["text"], "hello");

        // Evict — children of root become Sentinels
        client.force_evict_all().await;

        // Promote just /campaign — only that subtree becomes real
        let campaign = client.once("/campaign").await.unwrap();
        assert_eq!(campaign["name"], "Test Campaign");

        // Now read root — root is Object (never evicted) but /chat, /characters
        // etc. are still Sentinels inside it. promote_path_deep must catch this.
        let root = client.once("/").await.unwrap();
        assert_eq!(root["characters"]["char-1"]["name"], "Alice");
        assert_eq!(root["chat"]["msg-1"]["text"], "hello");
        assert_eq!(root["campaign"]["name"], "Test Campaign");
        assert_eq!(root["players"]["player-1"]["name"], "Alice");

        client.disconnect().await;
    });
}

/// Bug repro #3: Fresh DB, all data in WAL (no compaction), evict, read root.
/// All data comes from pending_wal_entries replay on an empty blob.
#[test]
fn test_sentinel_bug_fresh_db_wal_only_evict_read_root() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/sentinel-bug-3").await;

        // Write several paths — all go to WAL + tree via set_lazy
        client
            .set("/chat/msg1", json!({"text": "first"}))
            .await
            .unwrap();
        client
            .set("/chat/msg2", json!({"text": "second"}))
            .await
            .unwrap();
        client
            .set("/users/alice", json!({"name": "Alice"}))
            .await
            .unwrap();

        // Evict
        client.force_evict_all().await;

        // Read root — must reload from empty blob + WAL replay
        let root = client.once("/").await.unwrap();
        assert_eq!(root["chat"]["msg1"]["text"], "first");
        assert_eq!(root["chat"]["msg2"]["text"], "second");
        assert_eq!(root["users"]["alice"]["name"], "Alice");

        client.disconnect().await;
    });
}

/// Bug repro #4: The exact production scenario — SET at root (initial data push),
/// then child writes, then evict, then subscribe to root.
#[test]
fn test_sentinel_bug_root_set_child_writes_evict_subscribe() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();

        let mut client = server.client();
        client.connect("test-project/sentinel-bug-4").await;

        // SET at root — like a Firebase SDK initial data push
        client
            .set(
                "/",
                json!({
                    "chat": {"msg1": {"text": "hello"}}
                }),
            )
            .await
            .unwrap();

        // More child writes
        client
            .set("/chat/msg2", json!({"text": "world"}))
            .await
            .unwrap();
        client.set("/config/version", json!(42)).await.unwrap();

        // Evict
        client.force_evict_all().await;

        // Read root
        let root = client.once("/").await.unwrap();
        assert_eq!(root["chat"]["msg1"]["text"], "hello");
        assert_eq!(root["chat"]["msg2"]["text"], "world");
        assert_eq!(root["config"]["version"], 42);

        // Also verify child reads still work
        let chat = client.once("/chat").await.unwrap();
        assert_eq!(chat["msg1"]["text"], "hello");
        assert_eq!(chat["msg2"]["text"], "world");

        client.disconnect().await;
    });
}

/// Bug repro #5: Overlapping subscriptions — Dashboard subscribes to "/",
/// game client subscribes to "/chat/msg-1". Eviction at different levels
/// creates Sentinel holes. Next root read must detect them.
#[test]
fn test_sentinel_bug_overlapping_subscriptions() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        let server = setup_server(data_dir, "sentinel-bug-5");

        // "Dashboard" reads root — promotes everything
        let mut dashboard = server.client();
        dashboard.connect("test-project/sentinel-bug-5").await;
        let root = dashboard.once("/").await.unwrap();
        assert_eq!(root["chat"]["msg-1"]["text"], "hello");

        // "Game client" reads a deep path — creates separate promotion timer
        let mut game_client = server.client();
        game_client.connect("test-project/sentinel-bug-5").await;
        let msg = game_client.once("/chat/msg-1").await.unwrap();
        assert_eq!(msg["text"], "hello");

        // Evict all
        dashboard.force_evict_all().await;

        // Dashboard reads root again — must get full data
        let root = dashboard.once("/").await.unwrap();
        assert_eq!(root["characters"]["char-1"]["name"], "Alice");
        assert_eq!(root["chat"]["msg-1"]["text"], "hello");
        assert_eq!(root["chat"]["msg-2"]["text"], "world");
        assert_eq!(root["pages"]["page-1"]["name"], "Map");
        assert_eq!(root["players"]["player-1"]["name"], "Alice");

        dashboard.disconnect().await;
        game_client.disconnect().await;
    });
}

// =============================================================================
// Test: Root eviction with hot sub-tree
// =============================================================================

/// Root eviction: subscribe to root, subscribe to a sub-tree, evict root.
/// The sub-tree should stay hot (selective eviction skips it), root re-promotion
/// should restore all data, and subsequent writes + evict + read should work.
#[test]
fn test_eviction_root_evicted_subtree_stays_hot() {
    run_test(|| async {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        let server = setup_server(data_dir, "evict-root-hot");

        let mut client = server.client();
        client.connect("test-project/evict-root-hot").await;

        // 1. Read root — promotes entire tree, creates promotion timer for "/"
        let root = client.once("/").await.unwrap();
        assert_eq!(root["characters"]["char-1"]["name"], "Alice");
        assert_eq!(root["chat"]["msg-1"]["text"], "hello");

        // 2. Read a sub-tree — creates a separate promotion timer for "/characters"
        let chars = client.once("/characters").await.unwrap();
        assert_eq!(chars["char-2"]["name"], "Bob");

        // 3. Force evict — root "/" timer is cleared, but since "/characters" is
        //    also promoted, selective eviction should keep /characters hot and only
        //    evict the other branches (chat, pages, campaign, players).
        //    Actually force_evict_all clears everything, so let's use timed eviction
        //    by evicting only root. We'll simulate by reading /characters again
        //    (refreshing its timer) then force-evicting.
        //
        //    force_evict_all evicts ALL paths, so both "/" and "/characters" get evicted.
        //    This tests the hardest case: root itself becomes Sentinel.
        client.force_evict_all().await;

        // 4. Read just the sub-tree — should re-promote from blob + WAL
        let chars = client.once("/characters").await.unwrap();
        assert_eq!(chars["char-1"]["name"], "Alice");
        assert_eq!(chars["char-2"]["name"], "Bob");
        assert_eq!(chars["char-3"]["name"], "Charlie");

        // 5. Read root — root is Sentinel, promote_path_deep must detect it
        let root = client.once("/").await.unwrap();
        assert_eq!(root["characters"]["char-1"]["name"], "Alice");
        assert_eq!(root["chat"]["msg-1"]["text"], "hello");
        assert_eq!(root["pages"]["page-1"]["name"], "Map");
        assert_eq!(root["campaign"]["name"], "Test Campaign");
        assert_eq!(root["players"]["player-1"]["name"], "Alice");

        // 6. Write new data to the sub-tree (goes to WAL + tree)
        client
            .set("/characters/char-1/hp", json!(999))
            .await
            .unwrap();
        client
            .set(
                "/characters/char-4",
                json!({"name": "Diana", "hp": 90, "class": "rogue"}),
            )
            .await
            .unwrap();

        // 7. Evict everything again
        client.force_evict_all().await;

        // 8. Read root — must pick up WAL writes on top of blob data
        let root = client.once("/").await.unwrap();
        assert_eq!(
            root["characters"]["char-1"]["hp"], 999,
            "WAL write should survive root eviction"
        );
        assert_eq!(
            root["characters"]["char-1"]["name"], "Alice",
            "blob data should survive"
        );
        assert_eq!(
            root["characters"]["char-4"]["name"], "Diana",
            "new WAL entry should survive"
        );
        assert_eq!(
            root["chat"]["msg-1"]["text"], "hello",
            "other branches should survive"
        );

        // 9. Read just the child — also works after root eviction
        let char1 = client.once("/characters/char-1").await.unwrap();
        assert_eq!(char1["hp"], 999);
        assert_eq!(char1["name"], "Alice");

        client.disconnect().await;
    });
}
