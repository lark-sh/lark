//! Random operation generator for chaos testing.
//!
//! Generates weighted random operations that exercise all major storage paths:
//! - Normal writes (33%)
//! - Collection pushes (15%) - generates data for blob compaction
//! - Deletes (10%)
//! - Edge case writes (10%) - unicode, emoji, deep nesting, large values
//! - Burst writes (10%) - triggers WAL rotation
//! - Delete collection (5%)
//! - Deep nesting (5%)
//! - Updates (5%) - shallow merge at single path
//! - Multi-path UPDATE at root (2%) - mimics Firebase REST PATCH at "/"
//!   with slash-keyed values (e.g. `{"users/alice": ..., "names/foo": "id"}`).
//!   This is what hit a real production bug: WalIndex didn't normalize the
//!   empty/root path, so descendant queries missed these entries on replay.
//! - Multi-path UPDATE at subpath (2%) - same shape but at a non-root base.
//! - TRANSACTION (3%) - atomic batch of 2-5 SET/UPDATE/DELETE sub-ops at
//!   distinct deep paths. This is the wire format the Firebase REST adapter
//!   produces for a multi-path PATCH with slash-keyed values, and it goes
//!   through `handle_transaction` (which had multiple bugs around blob-backed
//!   writes — set_lazy/update_lazy/remove_sentinel_paths_below).

use rand::Rng;
use serde_json::{json, Value};

/// A generated operation to execute.
///
/// For `OpType::Set` / `OpType::Update` the `path` and `value` fields are the
/// targets; `tx_ops` is `None`.
///
/// For `OpType::Transaction` the `tx_ops` field carries the batch of sub-ops
/// to send atomically; `path` and `value` are unused (set to `""` and
/// `Value::Null`).
#[derive(Debug, Clone)]
pub struct Operation {
    pub path: String,
    pub op_type: OpType,
    pub value: Value,
    pub tx_ops: Option<Vec<TxOp>>,
}

#[derive(Debug, Clone)]
pub enum OpType {
    Set,
    Update,
    Transaction,
}

/// A single op within a transaction.
#[derive(Debug, Clone)]
pub struct TxOp {
    pub kind: TxOpKind,
    pub path: String,
    /// For Set/Update this is the value being written. For Delete it's `None`.
    pub value: Option<Value>,
}

#[derive(Debug, Clone)]
pub enum TxOpKind {
    Set,
    Update,
    Delete,
}

/// Generates random operations for chaos testing.
pub struct OperationGenerator {
    /// Counter for unique push IDs
    push_counter: u64,
    /// Collection names used
    collections: Vec<String>,
    /// Item keys used (for targeted deletes/updates)
    written_paths: Vec<String>,
    /// Paths that currently hold an array, as (path, element_count, is_object_array).
    /// Used to target valid indices for partial-element writes.
    array_paths: Vec<(String, usize, bool)>,
}

impl OperationGenerator {
    pub fn new() -> Self {
        Self {
            push_counter: 0,
            collections: vec![
                "players".to_string(),
                "messages".to_string(),
                "scores".to_string(),
                "inventory".to_string(),
                "events".to_string(),
            ],
            written_paths: Vec::new(),
            array_paths: Vec::new(),
        }
    }

    /// Remember an array we just wrote so later ops can target its elements.
    /// Bounded so the list can't grow without limit.
    fn record_array(&mut self, path: String, len: usize, is_objects: bool) {
        const MAX_TRACKED: usize = 64;
        self.array_paths.push((path, len, is_objects));
        if self.array_paths.len() > MAX_TRACKED {
            self.array_paths.remove(0);
        }
    }

    /// Generate a random operation.
    pub fn generate<R: Rng>(&mut self, rng: &mut R) -> Operation {
        let roll: u32 = rng.gen_range(0..100);

        match roll {
            0..=28 => self.normal_write(rng),
            29..=43 => self.collection_push(rng),
            44..=52 => self.delete(rng),
            53..=61 => self.edge_case(rng),
            62..=70 => self.burst_value(rng),
            71..=74 => self.delete_collection(rng),
            75..=78 => self.deep_nesting(rng),
            79..=81 => self.update(rng),
            82..=83 => self.update_at_fresh_pid(rng),
            84..=85 => self.multi_path_update_at_root(rng),
            86..=87 => self.multi_path_update_at_subpath(rng),
            88..=90 => self.transaction(rng),
            91..=95 => self.set_array(rng),
            96..=99 => self.array_element_write(rng),
            _ => unreachable!(),
        }
    }

    /// Generate a write that the `lookup` rules ruleset will deny — a SET
    /// at a chaos-rule-deny path with the literal deny-marker value. The
    /// rule expression `newData.val() !== '__chaos_deny__'` rejects this,
    /// the server NACKs, ground-truth marks it Rejected, and verify
    /// confirms the path doesn't carry the deny value after restart.
    ///
    /// Caller is responsible for only invoking this when running in
    /// `lookup` rules mode — under `open` rules the write would succeed
    /// and corrupt ground truth.
    pub fn generate_deny<R: Rng>(&mut self, rng: &mut R) -> Operation {
        let item_id: u32 = rng.gen_range(0..1000);
        // Use a distinct path prefix so denied writes can't collide with
        // legitimate written_paths bookkeeping.
        let path = format!("/chaos-deny/-item-{}", item_id);
        Operation {
            path,
            op_type: OpType::Set,
            value: serde_json::Value::String("__chaos_deny__".to_string()),
            tx_ops: None,
        }
    }

    /// Generate a batch of burst writes (for WAL rotation testing).
    pub fn generate_burst<R: Rng>(&mut self, rng: &mut R) -> Vec<Operation> {
        let count = rng.gen_range(50..200);
        let mut ops = Vec::with_capacity(count);
        for _ in 0..count {
            ops.push(self.burst_value(rng));
        }
        ops
    }

    /// Normal write: SET a simple value at /data/-item-abcdefg-{N}
    fn normal_write<R: Rng>(&mut self, rng: &mut R) -> Operation {
        let item_id: u32 = rng.gen_range(0..1000);
        let path = format!("/data/-item-abcdefg-{}", item_id);
        let name = format!("-item-abcdefg-{}", item_id);
        let val: i32 = rng.gen_range(0..10000);
        let active = rng.gen_bool(0.5);
        let ts = chrono_like_timestamp();
        let value = json!({
            "name": name,
            "value": val,
            "active": active,
            "ts": ts,
        });
        self.written_paths.push(path.clone());
        Operation {
            path,
            op_type: OpType::Set,
            value,
            tx_ops: None,
        }
    }

    /// Collection push: SET to /collections/{name}/{push-id}
    /// Generates enough data to trigger WAL rotation and blob compaction.
    ///
    /// Keys use Firebase push ID format (start with '-', 20 chars).
    fn collection_push<R: Rng>(&mut self, rng: &mut R) -> Operation {
        let coll = &self.collections[rng.gen_range(0..self.collections.len())];
        self.push_counter += 1;
        let push_id = generate_push_id(self.push_counter, rng);
        let path = format!("/collections/{}/{}", coll, push_id);

        // ~5KB per push so ~200 pushes to one collection reaches the 1MB threshold
        let content = generate_text(rng, 3000, 5000);
        let author = format!("user-{}", rng.gen_range(0..50));
        let ts = chrono_like_timestamp();
        let types = ["message", "event", "action"];
        let msg_type = types[rng.gen_range(0..3)];
        let priority: u32 = rng.gen_range(0..5);
        let value = json!({
            "id": push_id,
            "content": content,
            "author": author,
            "timestamp": ts,
            "metadata": {
                "type": msg_type,
                "priority": priority,
            }
        });
        self.written_paths.push(path.clone());
        Operation {
            path,
            op_type: OpType::Set,
            value,
            tx_ops: None,
        }
    }

    /// Delete: SET null to a previously written path.
    fn delete<R: Rng>(&mut self, rng: &mut R) -> Operation {
        let path = if !self.written_paths.is_empty() && rng.gen_bool(0.8) {
            let idx = rng.gen_range(0..self.written_paths.len());
            self.written_paths[idx].clone()
        } else {
            let item_id: u32 = rng.gen_range(0..1000);
            format!("/data/-item-abcdefg-{}", item_id)
        };
        Operation {
            path,
            op_type: OpType::Set,
            value: Value::Null,
            tx_ops: None,
        }
    }

    /// Edge case writes: unicode, emoji, deep nesting, large values, empty strings.
    #[allow(clippy::approx_constant)] // 3.14159… is edge-case test data, not PI
    fn edge_case<R: Rng>(&mut self, rng: &mut R) -> Operation {
        let variant = rng.gen_range(0..6);
        let item_id = rng.gen_range(0..100);
        let path = format!("/edge/-case-abcdefg-{}", item_id);

        let value = match variant {
            0 => {
                // Unicode strings
                json!({
                    "text": "Hello 世界 مرحبا Привет こんにちは",
                    "emoji": "🎮🎯🏆🔥💎",
                    "mixed": "café résumé naïve"
                })
            }
            1 => {
                // Large string value (50KB - approaching but not exceeding limits)
                let big = "x".repeat(50_000);
                json!({"large": big})
            }
            2 => {
                // Empty string and empty object
                json!({"empty_str": "", "empty_obj": {}, "zero": 0, "false": false})
            }
            3 => {
                // Numeric edge cases
                json!({
                    "max_safe": 9007199254740991_i64,
                    "min_safe": -9007199254740991_i64,
                    "float": 3.141592653589793,
                    "neg_zero": -0.0,
                    "small": 0.000001,
                })
            }
            4 => {
                // Deeply nested (10 levels)
                let mut v = json!("leaf");
                for i in (0..10).rev() {
                    let mut map = serde_json::Map::new();
                    map.insert(format!("level-{}", i), v);
                    v = Value::Object(map);
                }
                v
            }
            5 => {
                // Special characters in string values
                json!({
                    "quotes": "he said \"hello\"",
                    "newlines": "line1\nline2\nline3",
                    "tabs": "col1\tcol2\tcol3",
                    "backslash": "path\\to\\file",
                    "null_char": "before\x00after",
                })
            }
            _ => unreachable!(),
        };

        self.written_paths.push(path.clone());
        Operation {
            path,
            op_type: OpType::Set,
            value,
            tx_ops: None,
        }
    }

    /// Burst value: large write to trigger WAL rotation (10-50KB).
    fn burst_value<R: Rng>(&mut self, rng: &mut R) -> Operation {
        let item_id = rng.gen_range(0..500);
        let path = format!("/burst/-item-abcdefg-{}", item_id);
        let size = rng.gen_range(10_000..50_000);
        let data = generate_text(rng, size, size);
        let seq = self.push_counter;
        let value = json!({
            "data": data,
            "seq": seq,
        });
        self.push_counter += 1;
        self.written_paths.push(path.clone());
        Operation {
            path,
            op_type: OpType::Set,
            value,
            tx_ops: None,
        }
    }

    /// SET an array at /arrays/-item-{N}. Mixes arrays of primitives and arrays
    /// of objects so partial-element writes can exercise both paths.
    fn set_array<R: Rng>(&mut self, rng: &mut R) -> Operation {
        let item_id: u32 = rng.gen_range(0..200);
        let path = format!("/arrays/-item-abcdefg-{}", item_id);
        let len = rng.gen_range(2..6);
        let is_objects = rng.gen_bool(0.5);
        let value = if is_objects {
            Value::Array(
                (0..len)
                    .map(|i| {
                        json!({
                            "x": rng.gen_range(0..10000),
                            "label": format!("o-{}-{}", item_id, i),
                        })
                    })
                    .collect(),
            )
        } else {
            Value::Array(
                (0..len)
                    .map(|i| json!(format!("elem-{}-{}", item_id, i)))
                    .collect(),
            )
        };
        self.written_paths.push(path.clone());
        self.record_array(path.clone(), len, is_objects);
        Operation {
            path,
            op_type: OpType::Set,
            value,
            tx_ops: None,
        }
    }

    /// Write into an existing array element: either overwrite a bare index with
    /// a primitive (`/arr/{i}`) or set a field on an object element
    /// (`/arr/{i}/x`). The latter mirrors the partial-write-into-array case —
    /// the other elements and the element's other fields must survive. Falls
    /// back to creating an array if none are tracked yet.
    fn array_element_write<R: Rng>(&mut self, rng: &mut R) -> Operation {
        if self.array_paths.is_empty() {
            return self.set_array(rng);
        }
        let pick = rng.gen_range(0..self.array_paths.len());
        let (base, len, is_objects) = self.array_paths[pick].clone();
        let idx = rng.gen_range(0..len);

        let (path, value) = if is_objects && rng.gen_bool(0.6) {
            (
                format!("{}/{}/x", base, idx),
                json!(rng.gen_range(0..10000)),
            )
        } else {
            (format!("{}/{}", base, idx), json!(rng.gen_range(0..10000)))
        };
        self.written_paths.push(path.clone());
        Operation {
            path,
            op_type: OpType::Set,
            value,
            tx_ops: None,
        }
    }

    /// Delete an entire collection root.
    fn delete_collection<R: Rng>(&mut self, rng: &mut R) -> Operation {
        let coll = &self.collections[rng.gen_range(0..self.collections.len())];
        let path = format!("/collections/{}", coll);
        Operation {
            path,
            op_type: OpType::Set,
            value: Value::Null,
            tx_ops: None,
        }
    }

    /// Deep nesting, right up to the server's depth cap.
    ///
    /// Total tree depth = path segments + value nesting, and the server rejects
    /// writes past `MAX_PATH_DEPTH` (32). The path here is two
    /// segments (`/deep/-test-…`), leaving room for `max_value_depth` levels of
    /// value nesting and still committing. Stay within that so we exercise the
    /// deep path without generating writes the server (correctly) rejects.
    fn deep_nesting<R: Rng>(&mut self, rng: &mut R) -> Operation {
        const MAX_PATH_DEPTH: usize = 32; // mirrors server/src/db/path.rs
        let item_id = rng.gen_range(0..20);
        let path = format!("/deep/-test-abcdefg-{}", item_id);
        let path_segments = 2; // "deep" + "-test-abcdefg-N"
        let max_value_depth = MAX_PATH_DEPTH - path_segments;
        let depth = rng.gen_range((max_value_depth / 2)..=max_value_depth);
        let leaf: i32 = rng.gen_range(0..10000);
        let mut value = json!(leaf);
        for i in (0..depth).rev() {
            let mut map = serde_json::Map::new();
            map.insert(format!("d{}", i), value);
            value = Value::Object(map);
        }
        self.written_paths.push(path.clone());
        Operation {
            path,
            op_type: OpType::Set,
            value,
            tx_ops: None,
        }
    }

    /// Update: shallow merge at an existing path.
    fn update<R: Rng>(&mut self, rng: &mut R) -> Operation {
        let path = if !self.written_paths.is_empty() && rng.gen_bool(0.7) {
            let idx = rng.gen_range(0..self.written_paths.len());
            self.written_paths[idx].clone()
        } else {
            let item_id: u32 = rng.gen_range(0..1000);
            format!("/data/-item-abcdefg-{}", item_id)
        };
        let updated_field: i32 = rng.gen_range(0..10000);
        let updated_at = chrono_like_timestamp();
        let value = json!({
            "updated_field": updated_field,
            "updated_at": updated_at,
        });
        Operation {
            path,
            op_type: OpType::Update,
            value,
            tx_ops: None,
        }
    }

    /// UPDATE at a fresh deep path `/coll/{new-pid}` with `{n, active}`.
    ///
    /// Mirrors the *exact same shape* as the TX UPDATE sub-op produced by
    /// `transaction()`, but as a plain (non-transactional) UPDATE. We use
    /// this to isolate whether the chaos pre-kill violations are
    /// transaction-specific, or whether the same shape via plain UPDATE
    /// also surfaces them — which would point at general WAL replay /
    /// promotion logic rather than anything in `handle_transaction`.
    fn update_at_fresh_pid<R: Rng>(&mut self, rng: &mut R) -> Operation {
        let coll = self.collections[rng.gen_range(0..self.collections.len())].clone();
        self.push_counter += 1;
        let pid = generate_push_id(self.push_counter, rng);
        let path = format!("/{}/{}", coll, pid);
        let value = json!({
            "n": rng.gen_range(0..1000),
            "active": rng.gen_bool(0.5),
        });
        self.written_paths.push(path.clone());
        Operation {
            path,
            op_type: OpType::Update,
            value,
            tx_ops: None,
        }
    }

    /// Multi-path UPDATE at root (path=""). The value is an object with
    /// slash-keyed keys, where each key is a sub-path under root. This is the
    /// shape the Firebase REST adapter writes for a multi-path PATCH at root,
    /// and it's what wastingtime-server's `multipath_update("", ...)` produces.
    ///
    /// Tree::update interprets each slash-keyed entry via `path.join(key)`, so
    /// `{"users/alice": v1, "names/foo": v2}` at root becomes equivalent to
    /// SET /users/alice = v1 + SET /names/foo = v2. Ground truth matches via
    /// `format!("{}/{}", record.path, key)` which produces the correct leaf
    /// paths.
    ///
    /// Use path="" (not "/") to avoid double-slash issues in the path
    /// concatenation on the verifier side.
    fn multi_path_update_at_root<R: Rng>(&mut self, rng: &mut R) -> Operation {
        let mut updates = serde_json::Map::new();
        let entry_count = rng.gen_range(2..=5);
        for _ in 0..entry_count {
            let (key, value) = self.random_multi_path_entry(rng, /* base */ "");
            insert_non_overlapping(&mut updates, key, value);
        }
        // Track each leaf path as written so deletes/updates can target them.
        for key in updates.keys() {
            self.written_paths.push(format!("/{}", key));
        }
        Operation {
            path: "".to_string(),
            op_type: OpType::Update,
            value: Value::Object(updates),
            tx_ops: None,
        }
    }

    /// Multi-path UPDATE at a non-root path. The base path is one of the known
    /// collections (e.g. /players, /messages); each key in the value is a
    /// slash-keyed sub-path within that collection.
    fn multi_path_update_at_subpath<R: Rng>(&mut self, rng: &mut R) -> Operation {
        let coll = self.collections[rng.gen_range(0..self.collections.len())].clone();
        let base = format!("/{}", coll);
        let mut updates = serde_json::Map::new();
        let entry_count = rng.gen_range(2..=4);
        for _ in 0..entry_count {
            let (key, value) = self.random_multi_path_entry(rng, &base);
            insert_non_overlapping(&mut updates, key, value);
        }
        for key in updates.keys() {
            self.written_paths.push(format!("{}/{}", base, key));
        }
        Operation {
            path: base,
            op_type: OpType::Update,
            value: Value::Object(updates),
            tx_ops: None,
        }
    }

    /// Generate a single (key, value) entry for a multi-path UPDATE.
    /// `key` will contain at least one '/' so it exercises Tree::update's
    /// `path.join(key)` sub-path interpretation. `_base` is the UPDATE path —
    /// caller uses it to track the resulting leaf path for later ops.
    fn random_multi_path_entry<R: Rng>(&mut self, rng: &mut R, _base: &str) -> (String, Value) {
        // Pick a depth — 2 segments (e.g. "users/alice") or 3 (e.g. "users/alice/score").
        let depth = rng.gen_range(2..=3);
        let mut segs = Vec::with_capacity(depth);
        for _ in 0..depth {
            // Mix of named keys and push IDs to exercise both shapes.
            if rng.gen_bool(0.3) {
                self.push_counter += 1;
                segs.push(generate_push_id(self.push_counter, rng));
            } else {
                let names = [
                    "users", "scores", "names", "items", "alice", "bob", "carol", "level", "score",
                    "name", "active",
                ];
                segs.push(names[rng.gen_range(0..names.len())].to_string());
            }
        }
        let key = segs.join("/");

        // Value is one of: primitive, small object, push-id-keyed object.
        let variant = rng.gen_range(0..3);
        let value = match variant {
            0 => json!(rng.gen_range(0..10000)),
            1 => json!({
                "n": rng.gen_range(0..1000),
                "active": rng.gen_bool(0.5),
                "name": format!("entry-{}", rng.gen_range(0..100)),
            }),
            _ => {
                self.push_counter += 1;
                let pid = generate_push_id(self.push_counter, rng);
                json!({ pid: { "v": rng.gen_range(0..1000) } })
            }
        };
        (key, value)
    }

    /// TRANSACTION op — atomic batch of SET/UPDATE/DELETE sub-ops at distinct
    /// deep paths. This is what the Firebase REST adapter writes for a
    /// multi-path PATCH whose keys contain "/" (the most common shape: e.g.
    /// `multipath_update` in wastingtime-server's character create/save).
    ///
    /// Exercises `handle_transaction` end-to-end including its blob-backed
    /// write path (`set_lazy` / `update_lazy` / `remove_sentinel_paths_below`)
    /// and the WAL replay of TRANSACTION-derived entries.
    fn transaction<R: Rng>(&mut self, rng: &mut R) -> Operation {
        let op_count = rng.gen_range(2..=5);
        let mut tx = Vec::with_capacity(op_count);
        for _ in 0..op_count {
            // Mostly SETs (matches the multi-path-PATCH-at-root pattern), some
            // UPDATEs, a few DELETEs against previously-written paths.
            let kind_roll: u32 = rng.gen_range(0..100);
            let (kind, path, value) = if kind_roll < 70 {
                // SET at a fresh deep leaf path
                let coll = self.collections[rng.gen_range(0..self.collections.len())].clone();
                self.push_counter += 1;
                let pid = generate_push_id(self.push_counter, rng);
                let leaf = ["level", "score", "name", "active", "ts"];
                let leaf_key = leaf[rng.gen_range(0..leaf.len())];
                let path = format!("/{}/{}/{}", coll, pid, leaf_key);
                let value = match rng.gen_range(0..3) {
                    0 => json!(rng.gen_range(0..10000)),
                    1 => json!(format!("v-{}", rng.gen_range(0..1000))),
                    _ => json!(rng.gen_bool(0.5)),
                };
                self.written_paths.push(path.clone());
                (TxOpKind::Set, path, Some(value))
            } else if kind_roll < 90 {
                // UPDATE at a deep path (writes to a previously-set or new container)
                let coll = self.collections[rng.gen_range(0..self.collections.len())].clone();
                self.push_counter += 1;
                let pid = generate_push_id(self.push_counter, rng);
                let path = format!("/{}/{}", coll, pid);
                let value = json!({
                    "n": rng.gen_range(0..1000),
                    "active": rng.gen_bool(0.5),
                });
                self.written_paths.push(path.clone());
                (TxOpKind::Update, path, Some(value))
            } else {
                // DELETE at a previously-written path (or a fresh one)
                let path = if !self.written_paths.is_empty() && rng.gen_bool(0.7) {
                    let idx = rng.gen_range(0..self.written_paths.len());
                    self.written_paths[idx].clone()
                } else {
                    let item_id: u32 = rng.gen_range(0..1000);
                    format!("/data/-item-abcdefg-{}", item_id)
                };
                (TxOpKind::Delete, path, None)
            };
            tx.push(TxOp { kind, path, value });
        }
        Operation {
            path: String::new(),
            op_type: OpType::Transaction,
            value: Value::Null,
            tx_ops: Some(tx),
        }
    }

    /// Generate a batch of collection pushes to seed data for blob compaction.
    /// Call at the start of a cycle to ensure WAL rotation and compaction happen
    /// before the kill.
    ///
    /// Targets 2 collections with ~250 pushes each (~5KB each = ~1.25MB per collection).
    pub fn seed_collections<R: Rng>(&mut self, rng: &mut R) -> Vec<Operation> {
        let mut ops = Vec::new();
        // Seed the first 2 collections past the 1MB threshold
        for coll_idx in 0..2.min(self.collections.len()) {
            let coll = self.collections[coll_idx].clone();
            for _ in 0..250 {
                self.push_counter += 1;
                let push_id = generate_push_id(self.push_counter, rng);
                let path = format!("/collections/{}/{}", coll, push_id);
                let content = generate_text(rng, 3000, 5000);
                let ts = chrono_like_timestamp();
                let value = json!({
                    "id": push_id,
                    "content": content,
                    "timestamp": ts,
                });
                self.written_paths.push(path.clone());
                ops.push(Operation {
                    path,
                    op_type: OpType::Set,
                    value,
                    tx_ops: None,
                });
            }
        }
        ops
    }

    /// Get the collection names (for cold eviction testing).
    pub fn collection_names(&self) -> &[String] {
        &self.collections
    }
}

/// Insert `key` → `value` into `map` only if no existing key is a path-prefix
/// of `key` and `key` isn't a path-prefix of any existing key. This avoids
/// undefined-but-server-deterministic behavior when one key is `"a/b"` and
/// another is `"a/b/c"` in the same multi-path UPDATE — Tree::update would
/// apply them in alphabetical order and the deeper write would clobber the
/// shallower into a container, while a naive ground-truth model keeps both.
/// Real Firebase clients don't send overlapping keys in one PATCH; this match
/// keeps the chaos generator focused on bug classes worth catching.
fn insert_non_overlapping(map: &mut serde_json::Map<String, Value>, key: String, value: Value) {
    let key_with_slash = format!("{}/", key);
    let conflicts = map.keys().any(|existing| {
        existing == &key
            || existing.starts_with(&key_with_slash)
            || key.starts_with(&format!("{}/", existing))
    });
    if !conflicts {
        map.insert(key, value);
    }
}

/// Generate a Firebase-style push ID (starts with '-', 20 chars).
/// The segmentation algorithm's `is_push_id()` requires keys that start with '-'
/// and are at least 10 characters long.
fn generate_push_id<R: Rng>(counter: u64, rng: &mut R) -> String {
    let charset = b"-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";
    let mut id = String::with_capacity(20);
    id.push('-'); // Must start with '-'
                  // Encode counter into base-64-ish chars for the next 8 chars (sortable)
    let mut n = counter;
    let mut counter_chars = [0u8; 8];
    for i in (0..8).rev() {
        counter_chars[i] = charset[(n % 64) as usize];
        n /= 64;
    }
    for &c in &counter_chars {
        id.push(c as char);
    }
    // Fill remaining 11 chars with random
    for _ in 0..11 {
        id.push(charset[rng.gen_range(0..charset.len())] as char);
    }
    id
}

/// Generate random text of approximately the given length.
fn generate_text<R: Rng>(rng: &mut R, min_len: usize, max_len: usize) -> String {
    let len = rng.gen_range(min_len..=max_len);
    let chars: Vec<char> = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
        .chars()
        .collect();
    (0..len)
        .map(|_| chars[rng.gen_range(0..chars.len())])
        .collect()
}

/// Generate a timestamp-like integer (Unix millis).
fn chrono_like_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
