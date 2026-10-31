//! Differential test of the server against an in-memory model.
//!
//! Random scripts of writes, reads of present and missing paths, queries and
//! evictions run through a real database, and every result must match a model
//! that applies the same writes to plain JSON. `blob/tests/compaction_model.rs`
//! does this for the blob alone; this test covers what sits above it: promotion,
//! the in-memory tree, rules and subscriptions. Modes add rules that read data, a
//! live subscriber, restarts and compaction.
//!
//! Run more cases with `PROPTEST_CASES=1000 cargo test -p lark-server --test integration_read_model`.

mod common;

use common::{QueryOptions, TestClient, TestServer, run_test};
use lark_blob::{ArcValue, StdBlobIO, write_blob};
use lark_server::protocol::{ClientMessage, TransactionOp, op};
use proptest::prelude::*;
use serde_json::{Map, Value, json};
use std::time::Duration;
use tempfile::TempDir;

const ROOT: &str = "m";
const MISSING_KEY: &str = "missing";
const WAL_SYNC_WAIT: Duration = Duration::from_millis(2500);

/// A small key set so paths collide constantly. The numeric keys are timestamps,
/// too sparse to read back as arrays.
const KEYS: &[&str] = &["alpha", "beta", "gamma", "1000000", "2000000"];

fn key() -> impl Strategy<Value = String> {
    prop::sample::select(KEYS).prop_map(String::from)
}

fn path() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(key(), 1..=3)
}

fn maybe_root() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(key(), 0..=3)
}

/// A primitive or nested object. Objects may hold nulls, which storage drops.
fn value() -> BoxedStrategy<Value> {
    let leaf = prop_oneof![
        (0..100i64).prop_map(|n| json!(n)),
        "[a-z]{1,4}".prop_map(Value::String),
        any::<bool>().prop_map(Value::Bool),
    ];
    leaf.prop_recursive(3, 12, 3, |inner| {
        let child = prop_oneof![4 => inner, 1 => Just(Value::Null)];
        prop::collection::btree_map(key(), child, 1..4)
            .prop_map(|m| Value::Object(m.into_iter().collect()))
    })
    .boxed()
}

fn update_value() -> BoxedStrategy<Value> {
    prop_oneof![Just(Value::Null), value()].boxed()
}

#[derive(Debug, Clone)]
enum Step {
    /// Multi-path update: base path, then relative paths where no key is a
    /// prefix of another, matching what clients send. A base of `None` sends it
    /// from the database root, above the model.
    Update(Option<Vec<String>>, Vec<(Vec<String>, Value)>),
    Set(Vec<String>, Value),
    Delete(Vec<String>),
    /// A transaction that deletes one path and sets another.
    Transaction(Vec<String>, Vec<String>, Value),
    Read(Vec<String>),
    ReadMissing(Vec<String>),
    Query(Vec<String>, usize),
    Shallow(Vec<String>),
    Evict,
    Compact,
    Restart,
}

fn no_prefix_overlap(rels: &[(Vec<String>, Value)]) -> bool {
    rels.iter().enumerate().all(|(i, (a, _))| {
        rels.iter()
            .enumerate()
            .all(|(j, (b, _))| i == j || !b.starts_with(a))
    })
}

fn step(mode: Mode) -> BoxedStrategy<Step> {
    let update = (
        prop::option::weighted(0.7, prop::collection::vec(key(), 0..=1)),
        prop::collection::vec((path(), update_value()), 1..4),
    )
        .prop_filter("no overlapping update keys", |(_, rels)| {
            no_prefix_overlap(rels)
        })
        .prop_map(|(base, rels)| Step::Update(base, rels));

    let mut steps: Vec<(u32, BoxedStrategy<Step>)> = vec![
        (6, update.boxed()),
        (
            2,
            (path(), value()).prop_map(|(p, v)| Step::Set(p, v)).boxed(),
        ),
        (2, path().prop_map(Step::Delete).boxed()),
        (
            1,
            (path(), path(), value())
                .prop_map(|(d, s, v)| Step::Transaction(d, s, v))
                .boxed(),
        ),
        (4, maybe_root().prop_map(Step::Read).boxed()),
        (2, path().prop_map(Step::ReadMissing).boxed()),
        (
            2,
            (maybe_root(), 1..4usize)
                .prop_map(|(p, n)| Step::Query(p, n))
                .boxed(),
        ),
        (2, Just(Step::Evict).boxed()),
    ];
    if mode.shallow {
        steps.push((2, maybe_root().prop_map(Step::Shallow).boxed()));
    }
    if mode.compaction {
        steps.push((1, Just(Step::Compact).boxed()));
    }
    if mode.restart {
        steps.push((1, Just(Step::Restart).boxed()));
    }
    prop::strategy::Union::new_weighted(steps).boxed()
}

fn script(mode: Mode) -> impl Strategy<Value = (Value, Vec<Step>)> {
    (value(), prop::collection::vec(step(mode), 1..60))
}

/// Removes nulls nested in a written value, and containers they leave empty.
fn clean(value: &Value) -> Value {
    let Some(map) = value.as_object() else {
        return value.clone();
    };
    let kept: Map<String, Value> = map
        .iter()
        .map(|(k, v)| (k.clone(), clean(v)))
        .filter(|(_, v)| !v.is_null())
        .collect();
    if kept.is_empty() {
        Value::Null
    } else {
        Value::Object(kept)
    }
}

/// Applies a write to the model. Null deletes, and a node left with no children
/// is removed along with it. Writing below a primitive replaces it; deleting
/// below one changes nothing.
fn model_write(node: &mut Value, segments: &[String], value: &Value) {
    let Some((first, rest)) = segments.split_first() else {
        *node = value.clone();
        return;
    };
    if !node.is_object() {
        if value.is_null() {
            return;
        }
        *node = json!({});
    }
    let map = node.as_object_mut().unwrap();
    let child = map.entry(first.clone()).or_insert(Value::Null);
    model_write(child, rest, value);
    if child.is_null() {
        map.remove(first);
    }
    if map.is_empty() {
        *node = Value::Null;
    }
}

fn model_read(root: &Value, segments: &[String]) -> Value {
    let mut node = root;
    for segment in segments {
        match node.get(segment) {
            Some(child) => node = child,
            None => return Value::Null,
        }
    }
    node.clone()
}

fn wire_path(segments: &[String]) -> String {
    if segments.is_empty() {
        format!("/{ROOT}")
    } else {
        format!("/{ROOT}/{}", segments.join("/"))
    }
}

/// Children in query order: integer-like keys numerically, then strings.
fn keys_in_order(value: &Value) -> Vec<String> {
    let mut keys: Vec<String> = value
        .as_object()
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default();
    keys.sort_by_key(|k| match k.parse::<i64>() {
        Ok(n) => (0, n, String::new()),
        Err(_) => (1, 0, k.clone()),
    });
    keys
}

fn keep(value: &Value, keys: &[String]) -> Value {
    if keys.is_empty() {
        return Value::Null;
    }
    Value::Object(keys.iter().map(|k| (k.clone(), value[k].clone())).collect())
}

/// Which features a run exercises.
#[derive(Clone, Copy)]
struct Mode {
    /// No blob or WAL: the whole tree lives in memory.
    ephemeral: bool,
    /// Rules that read existing data, so writes and reads promote through them.
    data_rules: bool,
    /// A second client subscribed to the model root, mirroring what it's sent.
    subscriber: bool,
    /// Compact the WAL into the blob during the run.
    compaction: bool,
    /// Include shallow reads.
    shallow: bool,
    /// Restart the server during the run, replaying from WAL and blob.
    restart: bool,
}

fn rules_for(mode: Mode) -> Value {
    if mode.data_rules {
        json!({"rules": {
            ".read": "data.exists() || !data.exists()",
            ".write": "!data.hasChild('never') || data.exists()",
            "m": {
                "$a": {
                    ".read": "data.child('never').exists() || true",
                    ".write": "data.exists() || !data.exists()",
                    "$b": {
                        ".write": "data.parent().child('never').exists() || true"
                    }
                }
            }
        }})
    } else {
        json!({"rules": {".read": true, ".write": true}})
    }
}

async fn check_read(client: &TestClient, model: &Value, segments: &[String]) {
    let path = wire_path(segments);
    let actual = client.once(&path).await.unwrap();
    assert_eq!(actual, model_read(model, segments), "read {path}");
}

async fn check_query(client: &TestClient, model: &Value, segments: &[String], limit: usize) {
    let path = wire_path(segments);
    let expected = model_read(model, segments);
    let query = QueryOptions {
        order_by: Some("key".to_string()),
        limit_to_first: Some(limit as i32),
        ..Default::default()
    };
    let actual = client.once_query(&path, query).await.unwrap();
    let wanted = if expected.is_object() {
        let keys = keys_in_order(&expected);
        keep(&expected, &keys[..limit.min(keys.len())])
    } else {
        expected
    };
    assert_eq!(actual, wanted, "limitToFirst({limit}) at {path}");
}

/// A shallow read returns primitives as they are and containers as size markers.
async fn check_shallow(client: &TestClient, model: &Value, segments: &[String]) {
    let path = wire_path(segments);
    let response = client
        .send_and_wait(ClientMessage {
            op: op::ONCE.to_string(),
            path: Some(path.clone()),
            request_id: Some("shallow".to_string()),
            shallow: Some(true),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        response.nack.is_none(),
        "shallow {path} was refused: {:?} {:?}",
        response.error,
        response.message
    );
    let actual = response
        .once_value
        .map(|v| v.to_value())
        .unwrap_or(Value::Null);
    let expected = model_read(model, segments);
    let Some(children) = expected.as_object() else {
        assert_eq!(actual, expected, "shallow {path}");
        return;
    };
    let mut actual_keys = keys_in_order(&actual);
    actual_keys.sort();
    let mut wanted_keys: Vec<String> = children.keys().cloned().collect();
    wanted_keys.sort();
    assert_eq!(actual_keys, wanted_keys, "shallow keys of {path}: {actual}");
    for (key, child) in children {
        if child.is_object() {
            assert!(
                actual[key].get(".sz").is_some(),
                "shallow {path}/{key}: {actual}"
            );
        } else {
            assert_eq!(&actual[key], child, "shallow {path}/{key}");
        }
    }
}

/// Applies every event the subscriber has been sent to its mirror.
async fn drain_events(subscriber: &TestClient, mirror: &mut Value) {
    glommio::timer::sleep(Duration::from_millis(5)).await;
    let events = subscriber.events().await;
    subscriber.clear_events().await;
    let root_prefix = format!("/{ROOT}");
    for event in events {
        let path = event.path.as_deref().unwrap_or("/");
        let segments: Vec<String> = path
            .strip_prefix(root_prefix.as_str())
            .unwrap_or(path)
            .split('/')
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        let value = event.value.map(|v| v.to_value()).unwrap_or(Value::Null);
        match event.event.as_deref() {
            Some("patch") => {
                for (key, child) in value.as_object().unwrap() {
                    let mut target = segments.clone();
                    target.extend(key.split('/').filter(|s| !s.is_empty()).map(str::to_string));
                    model_write(mirror, &target, &clean(child));
                }
            }
            _ => model_write(mirror, &segments, &clean(&value)),
        }
    }
}

/// Writes past the WAL rotation size, then waits for the storage worker.
async fn compact(client: &TestClient) {
    let chunk = "x".repeat(600_000);
    for i in 0..10 {
        client
            .set(&format!("/bulk/item_{i}"), json!(&chunk))
            .await
            .unwrap();
    }
    glommio::timer::sleep(Duration::from_millis(3500)).await;
}

async fn check_step(client: &TestClient, model: &Value, step: &Step) {
    match step {
        Step::Read(segments) => check_read(client, model, segments).await,
        Step::ReadMissing(segments) => {
            let mut segments = segments.clone();
            segments.push(MISSING_KEY.to_string());
            check_read(client, model, &segments).await;
        }
        Step::Query(segments, limit) => check_query(client, model, segments, *limit).await,
        Step::Shallow(segments) => check_shallow(client, model, segments).await,
        _ => {}
    }
}

/// Applies a write to the database and the model.
async fn apply_write(client: &TestClient, model: &mut Value, step: &Step) {
    match step {
        Step::Update(base, rels) => {
            let model_base = base.clone().unwrap_or_default();
            let mut updates = Map::new();
            for (rel, value) in rels {
                let key = rel.join("/");
                let key = match base {
                    Some(_) => key,
                    None => format!("{ROOT}/{key}"),
                };
                updates.insert(key, value.clone());
                let full = [model_base.as_slice(), rel].concat();
                model_write(model, &full, &clean(value));
            }
            let path = base.as_ref().map_or("/".to_string(), |b| wire_path(b));
            client.update(&path, Value::Object(updates)).await.unwrap();
        }
        Step::Set(segments, value) => {
            client
                .set(&wire_path(segments), value.clone())
                .await
                .unwrap();
            model_write(model, segments, &clean(value));
        }
        Step::Delete(segments) => {
            client.remove(&wire_path(segments)).await.unwrap();
            model_write(model, segments, &Value::Null);
        }
        Step::Transaction(deleted, written, value) => {
            client
                .transaction(vec![
                    TransactionOp {
                        op: "d".to_string(),
                        path: wire_path(deleted),
                        value: None,
                        hash: None,
                    },
                    TransactionOp {
                        op: "s".to_string(),
                        path: wire_path(written),
                        value: Some(value.clone()),
                        hash: None,
                    },
                ])
                .await
                .unwrap();
            model_write(model, deleted, &Value::Null);
            model_write(model, written, &clean(value));
        }
        _ => {}
    }
}

async fn run_script(initial: Value, steps: Vec<Step>, mode: Mode) {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap().to_string();
    let db_id = "test-project/model".to_string();

    let mut model = Value::Null;
    if !mode.ephemeral {
        let initial = clean(&initial);
        model_write(&mut model, &[], &initial);
        let db_dir = format!("{data_dir}/{db_id}");
        std::fs::create_dir_all(&db_dir).unwrap();
        let blob_path = format!("{db_dir}/blob.lark");
        let io = StdBlobIO::create(std::path::Path::new(&blob_path)).unwrap();
        let mut contents = json!({"seed": 1});
        if !initial.is_null() {
            contents[ROOT] = initial;
        }
        write_blob(&io, &ArcValue::from_value(contents))
            .await
            .unwrap();
    }

    let mut server = if mode.ephemeral {
        TestServer::new()
    } else {
        TestServer::with_persistence(&data_dir)
    };
    server
        .set_rules_with_ephemeral("test-project", rules_for(mode), mode.ephemeral)
        .unwrap();
    let mut client = server.client();
    client.connect(&db_id).await;

    let mut mirror = Value::Null;
    let mut subscriber = server.client();
    if mode.subscriber {
        subscriber.connect(&db_id).await;
        subscriber
            .subscribe(&format!("/{ROOT}"), &["value"])
            .await
            .unwrap();
    }

    for step in &steps {
        match step {
            Step::Evict => client.force_evict_all().await,
            Step::Compact => compact(&client).await,
            Step::Restart => {
                // Default durability acks before the WAL sync; let it land first.
                glommio::timer::sleep(WAL_SYNC_WAIT).await;
                subscriber.disconnect().await;
                client.disconnect().await;
                glommio::timer::sleep(Duration::from_millis(50)).await;
                server.shutdown().await;
                server = TestServer::restart_with_persistence(&data_dir);
                server
                    .set_rules_with_ephemeral("test-project", rules_for(mode), false)
                    .unwrap();
                client = server.client();
                client.connect(&db_id).await;
                subscriber = server.client();
                if mode.subscriber {
                    mirror = Value::Null;
                    subscriber.connect(&db_id).await;
                    subscriber
                        .subscribe(&format!("/{ROOT}"), &["value"])
                        .await
                        .unwrap();
                }
            }
            _ => {
                apply_write(&client, &mut model, step).await;
                check_step(&client, &model, step).await;
            }
        }
    }

    check_read(&client, &model, &[]).await;
    if mode.subscriber {
        drain_events(&subscriber, &mut mirror).await;
        assert_eq!(mirror, model, "subscriber mirror");
    }
}

fn run((initial, steps): (Value, Vec<Step>), mode: Mode) {
    run_test(move || run_script(initial, steps, mode));
}

/// Runs PROPTEST_CASES, or `default` when it isn't set. Failures are shrunk to a
/// minimal script.
fn config(default: u32) -> ProptestConfig {
    ProptestConfig {
        cases: std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|n| n.parse().ok())
            .unwrap_or(default),
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

const BLOB_BACKED: Mode = Mode {
    ephemeral: false,
    data_rules: false,
    subscriber: false,
    compaction: false,
    restart: false,
    shallow: false,
};

proptest! {
    #![proptest_config(config(32))]

    #[test]
    fn test_model_blob_backed(script in script(BLOB_BACKED)) {
        run(script, BLOB_BACKED);
    }

    #[test]
    fn test_model_ephemeral(script in script(BLOB_BACKED)) {
        run(script, Mode { ephemeral: true, ..BLOB_BACKED });
    }

    #[test]
    fn test_model_data_reading_rules(script in script(BLOB_BACKED)) {
        run(script, Mode { data_rules: true, ..BLOB_BACKED });
    }

    #[test]
    fn test_model_live_subscriber(script in script(BLOB_BACKED)) {
        run(script, Mode { subscriber: true, ..BLOB_BACKED });
    }

    #[test]
    #[ignore = "known issue: shallow reads fail on unloaded children and miss ancestor writes"]
    fn test_model_shallow_reads(script in script(Mode { shallow: true, ..BLOB_BACKED })) {
        run(script, Mode { shallow: true, ..BLOB_BACKED });
    }
}

proptest! {
    #![proptest_config(config(4))]

    #[test]
    #[ignore] // Slow test — restarts the server, waiting out a WAL sync each time
    fn test_model_restart(script in script(Mode { restart: true, ..BLOB_BACKED })) {
        run(script, Mode { restart: true, ..BLOB_BACKED });
    }

    #[test]
    #[ignore] // Slow test — writes 5MB+ per compaction
    fn test_model_compaction(script in script(Mode { compaction: true, ..BLOB_BACKED })) {
        run(script, Mode { compaction: true, ..BLOB_BACKED });
    }
}
