//! Writes that leave the data as it was send no events, and updates send only the
//! keys that changed. Each test has a control write that does change something,
//! so a listener that has simply stopped working can't pass.

mod common;

use common::{QueryOptions, TestClient, TestServer, run_test};
use lark_blob::{ArcValue, StdBlobIO, write_blob};
use lark_server::protocol::{ServerMessage, TransactionOp};
use serde_json::{Value, json};
use std::time::Duration;
use tempfile::TempDir;

const SETTLE: Duration = Duration::from_millis(50);

fn sheet() -> Value {
    json!({
        "hp": 1,
        "ratio": 1.5,
        "sheet": {"name": "Ayla", "stats": {"str": 10, "dex": 12}, "tags": ["a", "b"]},
    })
}

async fn listen(server: &TestServer, db: &str, path: &str) -> TestClient {
    let mut client = server.client();
    client.connect(db).await;
    client.subscribe(path, &["value"]).await.unwrap();
    settle(&client).await;
    client
}

/// Waits for delivery, then forgets what the client has been sent.
async fn settle(client: &TestClient) {
    received(client).await;
}

/// The events a client has been sent since this was last called.
async fn received(client: &TestClient) -> Vec<ServerMessage> {
    glommio::timer::sleep(SETTLE).await;
    let events = client.events().await;
    client.clear_events().await;
    events
}

fn value_of(event: &ServerMessage) -> Value {
    event.value.as_ref().map_or(Value::Null, |v| v.to_value())
}

fn set_op(path: &str, value: Value) -> TransactionOp {
    TransactionOp {
        op: "s".to_string(),
        path: path.to_string(),
        value: Some(value),
        hash: None,
    }
}

fn delete_op(path: &str) -> TransactionOp {
    TransactionOp {
        op: "d".to_string(),
        path: path.to_string(),
        value: None,
        hash: None,
    }
}

#[test]
fn test_identical_set_reaches_nobody() {
    run_test(|| async {
        let server = TestServer::new();
        let mut writer = server.client();
        writer.connect("db").await;
        writer.set("/game", sheet()).await.unwrap();

        writer.subscribe("/game", &["value"]).await.unwrap();
        settle(&writer).await;
        let other = listen(&server, "db", "/game").await;
        let child = listen(&server, "db", "/game/sheet").await;

        // `set` returning Ok means the writer received its ack.
        writer.set("/game", sheet()).await.unwrap();
        assert!(received(&writer).await.is_empty());
        assert!(received(&other).await.is_empty());
        assert!(received(&child).await.is_empty());

        let reordered = json!({
            "sheet": sheet()["sheet"],
            "ratio": 1.5,
            "hp": 1,
        });
        writer.set("/game", reordered).await.unwrap();
        assert!(received(&other).await.is_empty());

        let mut edited = sheet();
        edited["sheet"]["stats"]["str"] = json!(11);
        writer.set("/game", edited.clone()).await.unwrap();
        let events = received(&other).await;
        assert_eq!(events.len(), 1);
        assert_eq!(value_of(&events[0]), edited, "a change sends the whole set");
        assert_eq!(received(&child).await.len(), 1);
        assert_eq!(received(&writer).await.len(), 1);
    });
}

#[test]
fn test_update_sends_only_changed_keys() {
    run_test(|| async {
        let server = TestServer::new();
        let mut writer = server.client();
        writer.connect("db").await;
        writer.set("/game", sheet()).await.unwrap();
        let other = listen(&server, "db", "/game").await;

        writer
            .update("/game", json!({"hp": 1, "sheet": sheet()["sheet"]}))
            .await
            .unwrap();
        assert!(received(&other).await.is_empty());

        writer
            .update("/game", json!({"hp": 2, "sheet": sheet()["sheet"]}))
            .await
            .unwrap();
        let events = received(&other).await;
        assert_eq!(events.len(), 1);
        assert_eq!(value_of(&events[0]), json!({"/hp": 2}));

        // Keys are compared whole: one nested change sends all of that key.
        let mut edited = sheet()["sheet"].clone();
        edited["stats"]["str"] = json!(11);
        writer
            .update("/game", json!({"sheet": edited.clone()}))
            .await
            .unwrap();
        let events = received(&other).await;
        assert_eq!(value_of(&events[0]), json!({"/sheet": edited}));

        assert_eq!(
            writer.once("/game/sheet/stats/str").await.unwrap(),
            json!(11)
        );
    });
}

#[test]
fn test_update_spares_listeners_it_does_not_reach() {
    run_test(|| async {
        let server = TestServer::new();
        let mut writer = server.client();
        writer.connect("db").await;
        writer.set("/game", sheet()).await.unwrap();
        let sheet_listener = listen(&server, "db", "/game/sheet").await;
        let stats_listener = listen(&server, "db", "/game/sheet/stats").await;
        let name_listener = listen(&server, "db", "/game/sheet/name").await;

        // Sent to /game, but the only key that changed is elsewhere.
        writer
            .update("/game", json!({"hp": 2, "sheet": sheet()["sheet"]}))
            .await
            .unwrap();
        assert!(received(&sheet_listener).await.is_empty());

        // Paths in keys reach listeners above, at and below them.
        writer
            .update("/game", json!({"sheet/stats/str": 11}))
            .await
            .unwrap();
        assert_eq!(received(&sheet_listener).await.len(), 1);
        assert_eq!(received(&stats_listener).await.len(), 1);
        assert!(received(&name_listener).await.is_empty());

        writer
            .update("/game", json!({"sheet": {"name": "Bo"}}))
            .await
            .unwrap();
        let events = received(&name_listener).await;
        assert_eq!(value_of(&events[0]), json!("Bo"));
        let events = received(&stats_listener).await;
        assert_eq!(
            value_of(&events[0]),
            Value::Null,
            "replaced with its parent"
        );
    });
}

#[test]
fn test_transaction_sends_only_operations_that_changed_something() {
    run_test(|| async {
        let server = TestServer::new();
        let mut writer = server.client();
        writer.connect("db").await;
        writer.set("/game", sheet()).await.unwrap();
        let other = listen(&server, "db", "/game").await;

        writer
            .transaction(vec![
                set_op("/game/sheet/stats/str", json!(10)),
                delete_op("/game/never/written"),
                set_op("/game/hp", json!(1)),
            ])
            .await
            .unwrap();
        assert!(received(&other).await.is_empty());

        writer
            .transaction(vec![
                set_op("/game/sheet/stats/str", json!(10)),
                set_op("/game/hp", json!(2)),
            ])
            .await
            .unwrap();
        let events = received(&other).await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].path.as_deref(), Some("/hp"));
    });
}

#[test]
fn test_removing_nothing_sends_nothing() {
    run_test(|| async {
        let server = TestServer::new();
        let mut writer = server.client();
        writer.connect("db").await;
        writer.set("/game", sheet()).await.unwrap();
        let other = listen(&server, "db", "/game").await;

        writer.remove("/game/never/written").await.unwrap();
        writer
            .set("/game/never/written", Value::Null)
            .await
            .unwrap();
        assert!(received(&other).await.is_empty());

        writer.remove("/game/hp").await.unwrap();
        assert_eq!(received(&other).await.len(), 1);
    });
}

#[test]
fn test_on_disconnect_sends_only_changes() {
    run_test(|| async {
        let server = TestServer::new();
        let mut leaver = server.client();
        leaver.connect("db").await;
        leaver.set("/game", sheet()).await.unwrap();
        leaver
            .on_disconnect_set("/game/hp", json!(1))
            .await
            .unwrap();
        leaver
            .on_disconnect_update("/game", json!({"ratio": 1.5, "online": false}))
            .await
            .unwrap();
        leaver.on_disconnect_remove("/game/missing").await.unwrap();
        let other = listen(&server, "db", "/game").await;

        leaver.disconnect().await;
        let events = received(&other).await;
        assert_eq!(events.len(), 1);
        assert_eq!(value_of(&events[0]), json!({"/online": false}));
    });
}

#[test]
fn test_query_listener_skips_unchanged_writes() {
    run_test(|| async {
        let server = TestServer::new();
        let mut writer = server.client();
        writer.connect("db").await;
        writer
            .set("/scores", json!({"a": {"n": 1}, "b": {"n": 2}}))
            .await
            .unwrap();

        let mut other = server.client();
        other.connect("db").await;
        other
            .subscribe_with_query(
                "/scores",
                &["value"],
                QueryOptions {
                    order_by_child: Some("n".to_string()),
                    limit_to_last: Some(1),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        settle(&other).await;

        writer.set("/scores/b", json!({"n": 2})).await.unwrap();
        writer.update("/scores/b", json!({"n": 2})).await.unwrap();
        assert!(received(&other).await.is_empty());

        writer.set("/scores/a", json!({"n": 3})).await.unwrap();
        assert!(!received(&other).await.is_empty());
    });
}

/// A database that keeps its data across restarts.
const DURABLE_DB: &str = "test-project/durable";

/// Writes `sheet()` to a persisted database and restarts, so it's loaded from
/// storage from then on.
async fn restarted_server(dir: &str) -> TestServer {
    let open_rules = json!({"rules": {".read": true, ".write": true}});
    let server = TestServer::with_persistence(dir);
    server
        .set_rules_with_ephemeral("test-project", open_rules.clone(), false)
        .unwrap();
    let mut writer = server.client();
    writer.connect(DURABLE_DB).await;
    writer.set("/game", sheet()).await.unwrap();
    glommio::timer::sleep(Duration::from_millis(2500)).await;
    writer.disconnect().await;
    server.shutdown().await;
    let server = TestServer::restart_with_persistence(dir);
    server
        .set_rules_with_ephemeral("test-project", open_rules, false)
        .unwrap();
    server
}

#[test]
fn test_data_loaded_from_storage_compares_equal() {
    run_test(|| async {
        let dir = TempDir::new().unwrap();
        let server = restarted_server(dir.path().to_str().unwrap()).await;

        // Subscribing loads the data, whole numbers and fractions included.
        let other = listen(&server, DURABLE_DB, "/game").await;
        let mut writer = server.client();
        writer.connect(DURABLE_DB).await;
        writer.set("/game", sheet()).await.unwrap();
        let events = received(&other).await;
        assert!(events.is_empty(), "{events:?}");

        writer
            .update("/game", json!({"sheet": sheet()["sheet"]}))
            .await
            .unwrap();
        let events = received(&other).await;
        assert!(events.is_empty(), "{events:?}");
    });
}

#[test]
fn test_data_read_from_a_blob_compares_equal() {
    run_test(|| async {
        let dir = TempDir::new().unwrap();
        let data_dir = dir.path().to_str().unwrap();
        let db_dir = format!("{data_dir}/{DURABLE_DB}");
        std::fs::create_dir_all(&db_dir).unwrap();
        let blob = format!("{db_dir}/blob.lark");
        let io = StdBlobIO::create(std::path::Path::new(&blob)).unwrap();
        write_blob(&io, &ArcValue::from_value(json!({"game": sheet()})))
            .await
            .unwrap();

        let server = TestServer::with_persistence(data_dir);
        server
            .set_rules_with_ephemeral(
                "test-project",
                json!({"rules": {".read": true, ".write": true}}),
                false,
            )
            .unwrap();
        let other = listen(&server, DURABLE_DB, "/game").await;
        let mut writer = server.client();
        writer.connect(DURABLE_DB).await;
        assert_eq!(writer.once("/game").await.unwrap(), sheet());

        writer.set("/game", sheet()).await.unwrap();
        let events = received(&other).await;
        assert!(events.is_empty(), "{events:?}");
    });
}

#[test]
fn test_unloaded_data_is_sent_without_loading_it() {
    run_test(|| async {
        let dir = TempDir::new().unwrap();
        let server = restarted_server(dir.path().to_str().unwrap()).await;

        // Loads /game/sheet/name and leaves the rest of /game/sheet in storage.
        let other = listen(&server, DURABLE_DB, "/game/sheet/name").await;
        let mut writer = server.client();
        writer.connect(DURABLE_DB).await;

        // The server can't tell this is unchanged, so it's sent.
        writer
            .set("/game/sheet", sheet()["sheet"].clone())
            .await
            .unwrap();
        assert_eq!(received(&other).await.len(), 1);

        // That write put the value in memory, so the same one again is skipped.
        writer
            .set("/game/sheet", sheet()["sheet"].clone())
            .await
            .unwrap();
        assert!(received(&other).await.is_empty());
    });
}

#[test]
fn test_evicted_data_is_sent() {
    run_test(|| async {
        let dir = TempDir::new().unwrap();
        let server = restarted_server(dir.path().to_str().unwrap()).await;
        let mut writer = server.client();
        writer.connect(DURABLE_DB).await;
        assert_eq!(writer.once("/game").await.unwrap(), sheet());

        writer.force_evict_all().await;
        let other = listen(&server, DURABLE_DB, "/game/hp").await;
        writer.set("/game", sheet()).await.unwrap();
        assert_eq!(received(&other).await.len(), 1);
    });
}

fn cursor_rules() -> Value {
    json!({
        "rules": {
            ".read": true,
            ".write": true,
            "rooms": {"$room": {"cursors": {"$id": {".volatile": true}}}},
        }
    })
}

#[test]
fn test_clearing_volatile_data_reaches_listeners() {
    run_test(|| async {
        let server = TestServer::new();
        server.set_rules("db", cursor_rules()).unwrap();
        let mut mover = server.client();
        mover.connect("db").await;
        mover
            .on_disconnect_remove("/rooms/r1/cursors/me")
            .await
            .unwrap();
        let other = listen(&server, "db", "/rooms/r1").await;

        // This goes straight to listeners, so the tree has nothing at this path.
        mover
            .set_volatile("/rooms/r1/cursors/me", json!({"x": 1}))
            .await
            .unwrap();
        glommio::timer::sleep(Duration::from_millis(250)).await;
        received(&other).await;
        other.clear_raw_messages().await;

        mover.disconnect().await;
        let events = received(&other).await;
        assert_eq!(events.len(), 1, "the cursor must be cleared for listeners");
        assert_eq!(value_of(&events[0]), Value::Null);
    });
}

#[test]
fn test_writes_above_volatile_data_reach_listeners() {
    run_test(|| async {
        let server = TestServer::new();
        server.set_rules("db", cursor_rules()).unwrap();
        let mut writer = server.client();
        writer.connect("db").await;
        writer.set("/rooms/r1/name", json!("Hall")).await.unwrap();
        let other = listen(&server, "db", "/rooms/r1").await;

        writer
            .set_volatile("/rooms/r1/cursors/me", json!({"x": 1}))
            .await
            .unwrap();
        glommio::timer::sleep(Duration::from_millis(250)).await;
        received(&other).await;

        // Unchanged in the tree, but it's how a client clears those cursors.
        writer
            .set("/rooms/r1", json!({"name": "Hall"}))
            .await
            .unwrap();
        assert_eq!(received(&other).await.len(), 1);

        // Elsewhere in the database, the comparison still applies.
        let elsewhere = listen(&server, "db", "/lobby").await;
        writer.set("/lobby/a", json!(1)).await.unwrap();
        settle(&elsewhere).await;
        writer.set("/lobby/a", json!(1)).await.unwrap();
        assert!(received(&elsewhere).await.is_empty());
    });
}
