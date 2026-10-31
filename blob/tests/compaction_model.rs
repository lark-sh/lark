//! Differential fuzz test for incremental compaction.
//!
//! Random WAL entries go two ways:
//! - the production path: written as WAL lines, read back with
//!   `parse_wal_lines`, turned into updates by `coalesce_wal_entries`, and
//!   applied with `BlobSession::apply_updates_with_sidecar`;
//! - an independent model that applies each entry in order to an in-memory
//!   tree with the same steps as the server's `Tree` (`from_value_cleaned`,
//!   then `set_path_mut` / `remove_path_mut`). It never uses
//!   `coalesce_wal_entries` or the batch tree.
//!
//! After every batch the blob must read back what the model holds: the whole
//! root, point reads, and shallow reads. Batch boundaries are random, and full
//! compactions and reopens are mixed in. Values are compared the way a client
//! sees them: arrays as index-keyed objects, nulls and empty objects absent,
//! numbers as f64.
//!
//! Run more cases with `PROPTEST_CASES=10000 cargo test -p lark-blob --test compaction_model`.
//!
//! The batch tree applies a batch's updates in `HashMap` order, which changes
//! from run to run, so a failure that depends on file layout may not repeat
//! with the same input. Rerun with more cases, or replay the shrunk script in a
//! loop, to reproduce one.

use std::collections::BTreeSet;
use std::path::Path;

use lark_blob::wal::{WalEntry, WalOp, coalesce_wal_entries, parse_wal_lines};
use lark_blob::{ArcValue, BlobError, BlobSession, MemBlobIO, ShallowValue};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use serde_json::{Map, Number, Value, json};

fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    futures::executor::block_on(fut)
}

// ---------------------------------------------------------------------------
// Generators
// ---------------------------------------------------------------------------

/// A small key set so paths collide constantly. "0" and "1" make objects that
/// read back as arrays; "-P…" keys look like push IDs.
const KEYS: &[&str] = &["a", "b", "c", "0", "1", "-Pa", "-Pb"];

fn key() -> impl Strategy<Value = String> {
    prop::sample::select(KEYS).prop_map(String::from)
}

fn path(min: usize, max: usize) -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(key(), min..=max)
}

fn leaf() -> BoxedStrategy<Value> {
    prop_oneof![
        1 => Just(Value::Null),
        2 => any::<bool>().prop_map(Value::Bool),
        3 => (-1000i64..1000).prop_map(|n| json!(n)),
        2 => any::<f64>()
            .prop_filter("finite", |f| f.is_finite())
            .prop_map(|f| Value::Number(Number::from_f64(f).unwrap())),
        3 => "[a-z,]{0,8}".prop_map(Value::String),
        // Large strings force values to move instead of being overwritten in place.
        1 => "[a-z]{50,400}".prop_map(Value::String),
    ]
    .boxed()
}

fn value() -> BoxedStrategy<Value> {
    leaf()
        .prop_recursive(3, 24, 4, |inner| {
            prop_oneof![
                prop::collection::btree_map(key(), inner.clone(), 0..4)
                    .prop_map(|m| Value::Object(m.into_iter().collect())),
                prop::collection::vec(inner, 0..4).prop_map(Value::Array),
            ]
        })
        .boxed()
}

fn object_value() -> BoxedStrategy<Value> {
    prop::collection::btree_map(key(), value(), 0..4)
        .prop_map(|m| Value::Object(m.into_iter().collect()))
        .boxed()
}

#[derive(Debug, Clone)]
enum Op {
    Set(Vec<String>, Value),
    /// Multi-path update: base path plus relative paths (no key is a prefix of
    /// another, matching what clients send).
    Update(Vec<String>, Vec<(Vec<String>, Value)>),
    Delete(Vec<String>),
    /// A child of a large push-ID collection, so collections grow to hundreds
    /// of children (reserved slots running out, collection copies).
    Push(String, u32, Value),
    /// A write immediately followed by a delete of the same path, in one batch.
    /// The batch tree merges the two, which is where several bugs have hidden.
    SetThenDelete(Vec<String>, Value),
}

fn no_prefix_overlap(rels: &[(Vec<String>, Value)]) -> bool {
    rels.iter().enumerate().all(|(i, (a, _))| {
        rels.iter()
            .enumerate()
            .all(|(j, (b, _))| i == j || !(b.len() >= a.len() && b[..a.len()] == a[..]))
    })
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => (path(1, 4), value()).prop_map(|(p, v)| Op::Set(p, v)),
        4 => (path(0, 2), prop::collection::vec((path(1, 2), value()), 1..4))
            .prop_filter("no overlapping update keys", |(_, rels)| no_prefix_overlap(rels))
            .prop_map(|(p, rels)| Op::Update(p, rels)),
        3 => path(1, 4).prop_map(Op::Delete),
        4 => (key(), 0u32..300, value()).prop_map(|(k, n, v)| Op::Push(k, n, v)),
        3 => (path(1, 4), value()).prop_map(|(p, v)| Op::SetThenDelete(p, v)),
        // Whole-database writes.
        1 => object_value().prop_map(|v| Op::Set(Vec::new(), v)),
        1 => Just(Op::Delete(Vec::new())),
    ]
}

#[derive(Debug, Clone)]
enum Step {
    Batch(Vec<Op>),
    /// Full compaction (`root_compact`), as the daily pass does.
    FullCompact,
    /// Reopen the session from its bytes, as after a restart.
    Reopen,
}

fn script() -> impl Strategy<Value = Vec<Step>> {
    let step = prop_oneof![
        12 => prop::collection::vec(op(), 1..8).prop_map(Step::Batch),
        1 => Just(Step::FullCompact),
        1 => Just(Step::Reopen),
    ];
    prop::collection::vec(step, 1..40)
}

// ---------------------------------------------------------------------------
// WAL entries and the model
// ---------------------------------------------------------------------------

fn wal_path(p: &[String]) -> String {
    format!("/{}", p.join("/"))
}

fn to_wal(op: &Op) -> Vec<WalEntry> {
    match op {
        Op::Set(p, v) => vec![WalEntry::set(&wal_path(p), v.clone())],
        Op::Update(p, rels) => {
            let map: Map<String, Value> =
                rels.iter().map(|(r, v)| (r.join("/"), v.clone())).collect();
            vec![WalEntry::update(&wal_path(p), Value::Object(map))]
        }
        Op::Delete(p) => vec![WalEntry::delete(&wal_path(p))],
        Op::Push(k, n, v) => vec![WalEntry::set(&format!("/{k}/-Q{n:04}"), v.clone())],
        Op::SetThenDelete(p, v) => vec![
            WalEntry::set(&wal_path(p), v.clone()),
            WalEntry::delete(&wal_path(p)),
        ],
    }
}

fn segments(path: &str) -> Vec<String> {
    path.split('/')
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect()
}

fn model_remove(root: &mut ArcValue, path: &[String]) {
    if path.is_empty() {
        *root = ArcValue::empty_object();
    } else {
        let refs: Vec<&str> = path.iter().map(String::as_str).collect();
        root.remove_path_mut(&refs);
    }
}

fn model_set(root: &mut ArcValue, path: &[String], value: Value) {
    match ArcValue::from_value_cleaned(value) {
        None => model_remove(root, path),
        Some(v) if path.is_empty() => *root = v,
        Some(v) => {
            let refs: Vec<&str> = path.iter().map(String::as_str).collect();
            root.set_path_mut(&refs, v);
        }
    }
}

fn model_apply(root: &mut ArcValue, entry: &WalEntry) {
    let base = segments(&entry.path);
    match entry.op {
        WalOp::Set => model_set(root, &base, entry.value.clone().unwrap_or(Value::Null)),
        WalOp::Delete => model_remove(root, &base),
        WalOp::Update => {
            if let Some(Value::Object(map)) = &entry.value {
                for (k, v) in map {
                    let mut p = base.clone();
                    p.extend(segments(k));
                    model_set(root, &p, v.clone());
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Comparison
// ---------------------------------------------------------------------------

/// A value as a client sees it: arrays become index-keyed objects, nulls and
/// empty objects don't exist, numbers are f64.
fn normalize(value: Value) -> Option<Value> {
    let object: Map<String, Value> = match value {
        Value::Null => return None,
        Value::Number(n) => {
            return Some(Value::Number(
                n.as_f64().and_then(Number::from_f64).unwrap_or(n),
            ));
        }
        Value::Array(items) => items
            .into_iter()
            .enumerate()
            .filter_map(|(i, v)| normalize(v).map(|v| (i.to_string(), v)))
            .collect(),
        Value::Object(map) => map
            .into_iter()
            .filter_map(|(k, v)| normalize(v).map(|v| (k, v)))
            .collect(),
        other => return Some(other),
    };
    (!object.is_empty()).then_some(Value::Object(object))
}

fn norm(v: &ArcValue) -> Option<Value> {
    normalize(v.to_value())
}

fn blob_read(session: &BlobSession<MemBlobIO>, path: &[&str]) -> Result<ArcValue, TestCaseError> {
    match block_on(session.read_subtree(path)) {
        Ok(v) => Ok(v),
        Err(BlobError::PathNotFound(_)) => Ok(ArcValue::Null),
        Err(e) => Err(TestCaseError::fail(format!(
            "read_subtree /{} failed: {e}",
            path.join("/")
        ))),
    }
}

/// The first path where two normalized values differ, for failure messages.
fn first_diff(a: &Option<Value>, b: &Option<Value>, path: &str) -> Option<String> {
    if a == b {
        return None;
    }
    if let (Some(Value::Object(ma)), Some(Value::Object(mb))) = (a, b) {
        let keys: BTreeSet<&String> = ma.keys().chain(mb.keys()).collect();
        for k in keys {
            if let Some(d) = first_diff(
                &ma.get(k).cloned(),
                &mb.get(k).cloned(),
                &format!("{path}/{k}"),
            ) {
                return Some(d);
            }
        }
    }
    let show = |v: &Option<Value>| v.as_ref().map_or("(absent)".to_string(), |v| v.to_string());
    Some(format!("{path}: blob={} model={}", show(a), show(b)))
}

/// Paths probed with point and shallow reads: the root and every one- and
/// two-segment path over the key set.
fn probe_paths() -> Vec<Vec<&'static str>> {
    let mut out = vec![vec![]];
    for a in KEYS {
        out.push(vec![*a]);
        for b in KEYS {
            out.push(vec![*a, *b]);
        }
    }
    out
}

fn check(
    session: &BlobSession<MemBlobIO>,
    model: &ArcValue,
    at: &str,
) -> Result<(), TestCaseError> {
    // Whole root.
    let root = blob_read(session, &[])?;
    if let Some(d) = first_diff(&norm(&root), &norm(model), "") {
        return Err(TestCaseError::fail(format!("{at}: root differs at {d}")));
    }

    for p in probe_paths() {
        let shown = format!("/{}", p.join("/"));
        let expected = model.get_path(&p).cloned().unwrap_or(ArcValue::Null);

        // Point read.
        let got = blob_read(session, &p)?;
        if let Some(d) = first_diff(&norm(&got), &norm(&expected), &shown) {
            return Err(TestCaseError::fail(format!(
                "{at}: point read {shown} differs at {d}"
            )));
        }

        // Shallow read: every child the model has must be listed, primitives
        // must match, and nothing the model lacks may be listed.
        let shallow = match block_on(session.read_shallow(&p)) {
            Ok(s) => s,
            Err(BlobError::PathNotFound(_)) => {
                if norm(&expected).is_some() {
                    return Err(TestCaseError::fail(format!(
                        "{at}: shallow {shown} not found, model has data"
                    )));
                }
                continue;
            }
            Err(e) => {
                return Err(TestCaseError::fail(format!(
                    "{at}: shallow {shown} failed: {e}"
                )));
            }
        };
        match shallow {
            ShallowValue::Primitive(v) => {
                if norm(&v) != norm(&expected) {
                    return Err(TestCaseError::fail(format!(
                        "{at}: shallow {shown} primitive {} vs model {}",
                        v.to_value(),
                        expected.to_value()
                    )));
                }
            }
            ShallowValue::Children(children) => {
                let listed: BTreeSet<&str> = children.iter().map(|c| c.key.as_str()).collect();
                for (k, v) in expected.iter() {
                    if norm(v).is_some() && !listed.contains(k) {
                        return Err(TestCaseError::fail(format!(
                            "{at}: shallow {shown} is missing child {k}"
                        )));
                    }
                }
                for c in &children {
                    let model_child = expected.get(&c.key).cloned().unwrap_or(ArcValue::Null);
                    match &c.value {
                        Some(v) => {
                            if norm(v) != norm(&model_child) {
                                return Err(TestCaseError::fail(format!(
                                    "{at}: shallow {shown}/{} is {} vs model {}",
                                    c.key,
                                    v.to_value(),
                                    model_child.to_value()
                                )));
                            }
                        }
                        None => {
                            if norm(&model_child).is_none() {
                                return Err(TestCaseError::fail(format!(
                                    "{at}: shallow {shown} lists {}, which the model doesn't have",
                                    c.key
                                )));
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The test
// ---------------------------------------------------------------------------

fn run(script: &[Step]) -> Result<(), TestCaseError> {
    let mut io = MemBlobIO::new();
    let sidecar = MemBlobIO::new();
    let mut session =
        block_on(BlobSession::init(io.clone())).map_err(|e| TestCaseError::fail(e.to_string()))?;
    let mut model = ArcValue::empty_object();

    for (i, step) in script.iter().enumerate() {
        let at = format!("step {i}");
        match step {
            Step::Batch(ops) => {
                let entries: Vec<WalEntry> = ops.iter().flat_map(to_wal).collect();
                for e in &entries {
                    model_apply(&mut model, e);
                }
                // Through a WAL file's text, as the StorageWorker reads it.
                let text: String = entries
                    .iter()
                    .map(|e| serde_json::to_string(e).unwrap() + "\n")
                    .collect();
                let parsed = parse_wal_lines(&text, Path::new("fuzz.wal"), false)
                    .map_err(|e| TestCaseError::fail(format!("{at}: parse: {e}")))?;
                let updates = coalesce_wal_entries(parsed);
                block_on(session.apply_updates_with_sidecar(&updates, Some(&sidecar)))
                    .map_err(|e| TestCaseError::fail(format!("{at}: apply: {e}")))?;
            }
            Step::FullCompact => {
                let dst = MemBlobIO::new();
                block_on(session.root_compact(dst.clone()))
                    .map_err(|e| TestCaseError::fail(format!("{at}: root_compact: {e}")))?;
                block_on(session.apply_updates_with_sidecar(&[], Some(&sidecar)))
                    .map_err(|e| TestCaseError::fail(format!("{at}: sidecar: {e}")))?;
                io = dst;
            }
            Step::Reopen => {
                session = block_on(BlobSession::open_with_sidecar(io.clone(), Some(&sidecar)))
                    .map_err(|e| TestCaseError::fail(format!("{at}: reopen: {e}")))?;
            }
        }
        check(&session, &model, &at)?;
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig {
        // Runs PROPTEST_CASES (default 256). Failures are shrunk to a minimal script.
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn compaction_matches_in_order_model(script in script()) {
        run(&script)?;
    }
}
