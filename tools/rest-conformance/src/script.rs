//! Random scripts of REST requests, checked against the model.
//!
//! Between requests the database is evicted, restarted and compacted, none of
//! which a client should be able to see.

use crate::model::{self, OrderBy, Query};
use crate::stack::{Response, Stack};
use proptest::prelude::*;
use proptest::strategy::Union;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::thread::sleep;
use std::time::Duration;

/// The low integers read back as arrays and the large ones are too sparse to. The
/// rest need URL encoding.
const KEYS: &[&str] = &[
    "alpha", "beta", "gamma", "0", "1", "2", "3", "1000000", "2000000", "a b", "é", "x:y", "a+b",
    "50%", "日本", "😀",
];
const COMMON_KEYS: &[&str] = &["alpha", "beta", "0", "1"];
const CHILD_KEYS: &[&str] = &["alpha", "beta", "0"];
const MISSING_KEY: &str = "missing";

/// Writes past the WAL rotation size, so the storage worker compacts.
const BULK_ITEMS: usize = 10;
const BULK_ITEM_BYTES: usize = 600_000;
const BULK_SETTLE: Duration = Duration::from_millis(3500);

type Path = Vec<String>;

/// Mostly a few keys, so paths collide constantly.
fn key() -> impl Strategy<Value = String> {
    prop_oneof![
        3 => prop::sample::select(COMMON_KEYS),
        1 => prop::sample::select(KEYS),
    ]
    .prop_map(String::from)
}

fn path() -> impl Strategy<Value = Path> {
    prop::collection::vec(key(), 1..=3)
}

fn maybe_root() -> impl Strategy<Value = Path> {
    prop::collection::vec(key(), 0..=3)
}

fn leaf() -> BoxedStrategy<Value> {
    prop_oneof![
        (-100..100i64).prop_map(|n| json!(n)),
        Just(json!(1.5)),
        Just(json!(-0.25)),
        Just(json!(9007199254740991i64)),
        "[a-z]{1,4}".prop_map(Value::String),
        Just(json!("quo\"te\\é日本")),
        any::<bool>().prop_map(Value::Bool),
    ]
    .boxed()
}

/// JSON a client could send, including what storage drops or restructures.
fn value() -> BoxedStrategy<Value> {
    leaf()
        .prop_recursive(3, 12, 3, |inner| {
            let child = prop_oneof![
                6 => inner,
                1 => Just(Value::Null),
                1 => Just(json!({})),
                1 => Just(json!([])),
            ];
            prop_oneof![
                4 => prop::collection::btree_map(key(), child.clone(), 1..4)
                    .prop_map(|m| Value::Object(m.into_iter().collect())),
                1 => prop::collection::vec(child, 1..4).prop_map(Value::Array),
            ]
        })
        .boxed()
}

fn bound(order: &OrderBy) -> BoxedStrategy<Value> {
    match order {
        OrderBy::Key => key().prop_map(Value::String).boxed(),
        _ => leaf(),
    }
}

fn query() -> BoxedStrategy<Query> {
    let order = prop_oneof![
        Just(OrderBy::Key),
        Just(OrderBy::Value),
        prop::sample::select(CHILD_KEYS).prop_map(|k| OrderBy::Child(k.to_string())),
    ];
    let limit = prop::option::of((any::<bool>(), 1..4usize));
    (order, limit)
        .prop_flat_map(|(order, limit)| {
            let bounds = prop_oneof![
                Just((None, None, None)),
                prop::option::of(bound(&order)).prop_map(|b| (b, None, None)),
                prop::option::of(bound(&order)).prop_map(|b| (None, b, None)),
                prop::option::of(bound(&order)).prop_map(|b| (None, None, b)),
            ];
            (Just(order), Just(limit), bounds)
        })
        .prop_map(|(order, limit, (start, end, equal))| Query {
            order,
            start,
            end,
            equal,
            limit,
        })
        .boxed()
}

/// Where a read goes. Most reads pick from the objects in the model when they
/// run, so they land on data.
#[derive(Debug, Clone)]
pub enum Target {
    Existing(usize),
    Path(Path),
}

fn target() -> BoxedStrategy<Target> {
    prop_oneof![
        3 => any::<usize>().prop_map(Target::Existing),
        1 => maybe_root().prop_map(Target::Path),
    ]
    .boxed()
}

fn collect_objects(node: &Value, path: &mut Path, found: &mut Vec<Path>) {
    if let Some(map) = node.as_object() {
        found.push(path.clone());
        for (key, child) in map {
            path.push(key.clone());
            collect_objects(child, path, found);
            path.pop();
        }
    }
}

#[derive(Debug, Clone)]
pub enum Step {
    Put {
        path: Path,
        value: Value,
        silent: bool,
    },
    /// Multi-path update: base path, then relative paths where no key is a
    /// prefix of another. A base of `None` sends it from the database root.
    Patch(Option<Path>, Vec<(Path, Value)>),
    Post(Path, Value),
    Delete(Path),
    Get(Target),
    GetMissing(Target),
    Shallow(Target),
    Query(Target, Query),
    /// Reads an ETag and writes with it, or with a stale one.
    ConditionalPut {
        path: Path,
        value: Value,
        stale: bool,
    },
    Evict,
    Restart,
    FullCompact,
    LiveCompact,
    BulkCompact,
}

fn no_prefix_overlap(rels: &[(Path, Value)]) -> bool {
    rels.iter().enumerate().all(|(i, (a, _))| {
        rels.iter()
            .enumerate()
            .all(|(j, (b, _))| i == j || !b.starts_with(a))
    })
}

fn step(slow: bool) -> BoxedStrategy<Step> {
    let patch = (
        prop::option::weighted(0.7, prop::collection::vec(key(), 0..=1)),
        prop::collection::vec((path(), prop_oneof![Just(Value::Null), value()]), 1..4),
    )
        .prop_filter("no overlapping keys", |(_, rels)| no_prefix_overlap(rels))
        .prop_map(|(base, rels)| Step::Patch(base, rels));

    let mut steps: Vec<(u32, BoxedStrategy<Step>)> = vec![
        (6, patch.boxed()),
        (
            3,
            (path(), value(), prop::bool::weighted(0.2))
                .prop_map(|(path, value, silent)| Step::Put {
                    path,
                    value,
                    silent,
                })
                .boxed(),
        ),
        (
            1,
            (maybe_root(), value())
                .prop_filter("POST needs a value", |(_, v)| !model::store(v).is_null())
                .prop_map(|(p, v)| Step::Post(p, v))
                .boxed(),
        ),
        (2, path().prop_map(Step::Delete).boxed()),
        (4, target().prop_map(Step::Get).boxed()),
        (2, target().prop_map(Step::GetMissing).boxed()),
        (3, target().prop_map(Step::Shallow).boxed()),
        (
            4,
            (target(), query())
                .prop_map(|(t, q)| Step::Query(t, q))
                .boxed(),
        ),
        (
            1,
            (path(), value(), any::<bool>())
                .prop_map(|(path, value, stale)| Step::ConditionalPut { path, value, stale })
                .boxed(),
        ),
        (2, Just(Step::Evict).boxed()),
    ];
    if slow {
        steps.push((1, Just(Step::Restart).boxed()));
        steps.push((1, Just(Step::FullCompact).boxed()));
        steps.push((1, Just(Step::LiveCompact).boxed()));
        steps.push((1, Just(Step::BulkCompact).boxed()));
    }
    Union::new_weighted(steps).boxed()
}

pub fn script(max_steps: usize, slow: bool) -> impl Strategy<Value = (Value, Vec<Step>)> {
    (value(), prop::collection::vec(step(slow), 1..max_steps))
}

/// Percent-encodes everything but unreserved characters, as a client would.
fn encode(text: &str) -> String {
    text.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

fn describe(response: &Response) -> String {
    let text = response.text();
    let cut = text
        .char_indices()
        .nth(600)
        .map_or(text.as_str(), |(i, _)| &text[..i]);
    format!("HTTP {}: {cut}", response.status)
}

/// Requires a status, a JSON content type and a body that parses.
pub fn parse(response: &Response, status: u16) -> Result<Value, String> {
    if response.status != status {
        return Err(format!("wanted HTTP {status}, got {}", describe(response)));
    }
    let content_type = response.header("content-type").unwrap_or("");
    if !content_type.starts_with("application/json") {
        return Err(format!("content type {content_type:?}"));
    }
    response.json()
}

/// Writes past the WAL rotation size, then waits for the storage worker's batch.
pub fn bulk_compact(stack: &Stack) -> Result<(), String> {
    let chunk = json!("x".repeat(BULK_ITEM_BYTES)).to_string();
    for i in 0..BULK_ITEMS {
        let url = format!("/bulk/item_{i}.json?print=silent");
        let response = stack.request("PUT", &url, &[], Some(chunk.as_bytes()))?;
        if response.status != 204 {
            return Err(format!("PUT {url}: {}", describe(&response)));
        }
    }
    sleep(BULK_SETTLE);
    Ok(())
}

pub struct Run<'a> {
    stack: &'a mut Stack,
    /// Top-level key this script owns, so scripts don't see each other.
    root: String,
    model: Value,
    /// Last ETag seen for each path, with what it was read from.
    etags: HashMap<Path, (Value, String)>,
}

impl<'a> Run<'a> {
    pub fn new(stack: &'a mut Stack, root: String) -> Self {
        Self {
            stack,
            root,
            model: Value::Null,
            etags: HashMap::new(),
        }
    }

    fn url(&self, segments: &[String], query: &str) -> String {
        let mut url = format!("/{}", self.root);
        for segment in segments {
            url.push('/');
            url.push_str(&encode(segment));
        }
        url.push_str(".json");
        url.push_str(query);
        url
    }

    fn resolve(&self, target: &Target) -> Path {
        match target {
            Target::Path(path) => path.clone(),
            Target::Existing(pick) => {
                let mut found = Vec::new();
                collect_objects(&self.model, &mut Vec::new(), &mut found);
                if found.is_empty() {
                    return Vec::new();
                }
                found[pick % found.len()].clone()
            }
        }
    }

    fn read_model(&self, segments: &[String]) -> Value {
        model::read(&self.model, segments).clone()
    }

    pub fn seed(&mut self, value: &Value) -> Result<(), String> {
        self.apply(&Step::Put {
            path: vec![],
            value: value.clone(),
            silent: false,
        })
    }

    pub fn check_all(&self) -> Result<(), String> {
        self.check_read(&[])
    }

    fn check_read(&self, segments: &[String]) -> Result<(), String> {
        let url = self.url(segments, "");
        let actual = parse(&self.stack.request("GET", &url, &[], None)?, 200)
            .map_err(|e| format!("GET {url}: {e}"))?;
        let wanted = model::render(&self.read_model(segments));
        if actual != wanted {
            return Err(format!("GET {url}\n  wanted {wanted}\n  got    {actual}"));
        }
        Ok(())
    }

    /// PUT and PATCH respond with the data sent. The docs don't say whether
    /// that's before or after storage drops nulls, so either passes.
    fn check_echo(&self, what: &str, response: &Response, sent: &Value) -> Result<(), String> {
        let actual = parse(response, 200).map_err(|e| format!("{what}: {e}"))?;
        if actual != *sent && actual != model::render(&model::store(sent)) {
            return Err(format!("{what}: sent {sent}, response was {actual}"));
        }
        Ok(())
    }

    fn check_etag(&mut self, segments: &[String], response: &Response) -> Result<String, String> {
        let etag = response
            .header("etag")
            .ok_or_else(|| format!("no ETag in {}", describe(response)))?
            .to_string();
        let value = self.read_model(segments);
        if let Some((seen_value, seen_etag)) = self.etags.get(segments) {
            let same_value = *seen_value == value;
            if same_value != (*seen_etag == etag) {
                return Err(format!(
                    "ETag at {segments:?}: {seen_etag} for {seen_value}, then {etag} for {value}"
                ));
            }
        }
        self.etags.insert(segments.to_vec(), (value, etag.clone()));
        Ok(etag)
    }

    fn apply_put(&mut self, path: &Path, value: &Value, silent: bool) -> Result<(), String> {
        let query = if silent { "?print=silent" } else { "" };
        let url = self.url(path, query);
        let body = value.to_string();
        let response = self
            .stack
            .request("PUT", &url, &[], Some(body.as_bytes()))?;
        if silent {
            if response.status != 204 || !response.body.is_empty() {
                return Err(format!("PUT {url}: {}", describe(&response)));
            }
        } else {
            self.check_echo(&format!("PUT {url}"), &response, value)?;
        }
        model::write(&mut self.model, path, &model::store(value));
        Ok(())
    }

    fn apply_patch(&mut self, base: &Option<Path>, rels: &[(Path, Value)]) -> Result<(), String> {
        let model_base = base.clone().unwrap_or_default();
        let mut body = Map::new();
        for (rel, value) in rels {
            let key = rel.join("/");
            let key = match base {
                Some(_) => key,
                None => format!("{}/{key}", self.root),
            };
            body.insert(key, value.clone());
            let full = [model_base.as_slice(), rel].concat();
            model::write(&mut self.model, &full, &model::store(value));
        }
        let body = Value::Object(body);
        let url = match base {
            Some(base) => self.url(base, ""),
            None => "/.json".to_string(),
        };
        let response = self
            .stack
            .request("PATCH", &url, &[], Some(body.to_string().as_bytes()))?;
        let actual = parse(&response, 200).map_err(|e| format!("PATCH {url}: {e}"))?;
        if actual != body {
            return Err(format!("PATCH {url}: sent {body}, response was {actual}"));
        }
        Ok(())
    }

    fn apply_post(&mut self, path: &Path, value: &Value) -> Result<(), String> {
        let url = self.url(path, "");
        let response = self
            .stack
            .request("POST", &url, &[], Some(value.to_string().as_bytes()))?;
        let actual = parse(&response, 200).map_err(|e| format!("POST {url}: {e}"))?;
        let name = actual["name"]
            .as_str()
            .ok_or_else(|| format!("POST {url}: response was {actual}"))?
            .to_string();
        let full = [path.as_slice(), &[name]].concat();
        model::write(&mut self.model, &full, &model::store(value));
        self.check_read(&full)
    }

    fn check_shallow(&self, segments: &[String]) -> Result<(), String> {
        let url = self.url(segments, "?shallow=true");
        let actual = parse(&self.stack.request("GET", &url, &[], None)?, 200)
            .map_err(|e| format!("GET {url}: {e}"))?;
        let node = self.read_model(segments);
        let Some(children) = node.as_object() else {
            if actual != node {
                return Err(format!("GET {url}\n  wanted {node}\n  got    {actual}"));
            }
            return Ok(());
        };
        // The docs only describe objects, so an array-like node may come back
        // either way, as long as it names exactly the keys that exist.
        let wanted = Value::Object(children.keys().map(|k| (k.clone(), json!(true))).collect());
        if model::as_object(&actual) != wanted {
            return Err(format!("GET {url}\n  wanted {wanted}\n  got    {actual}"));
        }
        Ok(())
    }

    fn check_query(&self, segments: &[String], query: &Query) -> Result<(), String> {
        let parameters: Vec<String> = query
            .parameters()
            .iter()
            .map(|(name, value)| format!("{name}={}", encode(value)))
            .collect();
        let url = self.url(segments, &format!("?{}", parameters.join("&")));
        let actual = parse(&self.stack.request("GET", &url, &[], None)?, 200)
            .map_err(|e| format!("GET {url}: {e}"))?;
        let node = self.read_model(segments);
        // The docs don't say what a query on a primitive returns.
        if !node.is_object() && !node.is_null() {
            return Ok(());
        }
        let wanted = query.select(&node);
        // Nor what shape an empty result takes.
        if wanted == json!({}) && (actual.is_null() || actual == json!({})) {
            return Ok(());
        }
        if model::as_object(&actual) != wanted {
            return Err(format!("GET {url}\n  wanted {wanted}\n  got    {actual}"));
        }
        Ok(())
    }

    fn apply_conditional_put(
        &mut self,
        path: &Path,
        value: &Value,
        stale: bool,
    ) -> Result<(), String> {
        let url = self.url(path, "");
        let response = self
            .stack
            .request("GET", &url, &[("X-Firebase-ETag", "true")], None)?;
        let etag = self.check_etag(path, &response)?;
        let sent = if stale { "c3RhbGU=" } else { etag.as_str() };
        let body = value.to_string();
        let response =
            self.stack
                .request("PUT", &url, &[("if-match", sent)], Some(body.as_bytes()))?;
        if stale {
            let wanted = model::render(&self.read_model(path));
            let actual = parse(&response, 412).map_err(|e| format!("stale PUT {url}: {e}"))?;
            if actual != wanted {
                return Err(format!("stale PUT {url}: wanted {wanted}, got {actual}"));
            }
            if response.header("etag") != Some(etag.as_str()) {
                return Err(format!(
                    "stale PUT {url}: ETag {:?}",
                    response.header("etag")
                ));
            }
            return self.check_read(path);
        }
        self.check_echo(&format!("conditional PUT {url}"), &response, value)?;
        model::write(&mut self.model, path, &model::store(value));
        Ok(())
    }

    fn bulk_compact(&self) -> Result<(), String> {
        bulk_compact(self.stack)
    }

    pub fn apply(&mut self, step: &Step) -> Result<(), String> {
        match step {
            Step::Put {
                path,
                value,
                silent,
            } => self.apply_put(path, value, *silent),
            Step::Patch(base, rels) => self.apply_patch(base, rels),
            Step::Post(path, value) => self.apply_post(path, value),
            Step::Delete(path) => {
                let url = self.url(path, "");
                let response = self.stack.request("DELETE", &url, &[], None)?;
                let actual = parse(&response, 200).map_err(|e| format!("DELETE {url}: {e}"))?;
                if !actual.is_null() {
                    return Err(format!("DELETE {url}: response was {actual}"));
                }
                model::write(&mut self.model, path, &Value::Null);
                Ok(())
            }
            Step::Get(target) => self.check_read(&self.resolve(target)),
            Step::GetMissing(target) => {
                let mut path = self.resolve(target);
                path.push(MISSING_KEY.to_string());
                self.check_read(&path)
            }
            Step::Shallow(target) => self.check_shallow(&self.resolve(target)),
            Step::Query(target, query) => self.check_query(&self.resolve(target), query),
            Step::ConditionalPut { path, value, stale } => {
                self.apply_conditional_put(path, value, *stale)
            }
            Step::Evict => self.stack.evict(),
            Step::Restart => self.stack.restart(),
            Step::FullCompact => self.stack.full_compact(),
            Step::LiveCompact => self.stack.live_compact(),
            Step::BulkCompact => self.bulk_compact(),
        }
    }
}
