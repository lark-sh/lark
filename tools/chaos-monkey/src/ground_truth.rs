//! Ground truth tracking for chaos testing.
//!
//! Tracks every write operation through its lifecycle:
//!   Sent → Committed (ACK) | Rejected (NACK) | Pending (crash before response)
//!
//! After a crash, the expected state is reconstructed from committed writes only.
//! Pending writes may or may not appear — both outcomes are acceptable.
//!
//! State carries across cycles: each cycle starts from a baseline, the state
//! the previous cycles left that's still known (see `carry_forward`), so data
//! written cycles ago is checked again after later restarts and compactions.

use crate::operations::{TxOp, TxOpKind};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Instant;
use tracing::{debug, trace};

/// The state of a write operation.
#[derive(Debug, Clone)]
pub enum WriteState {
    /// Write was sent but no response received yet.
    Sent,
    /// Server acknowledged the write (must survive restart).
    Committed,
    /// Server rejected the write (must NOT appear after restart).
    Rejected(String),
}

/// A recorded write operation.
#[derive(Debug, Clone)]
pub struct WriteRecord {
    pub request_id: String,
    pub client_id: u32,
    pub path: String,
    pub operation: WriteOp,
    pub state: WriteState,
    pub sequence: u64,
    /// When the ACK was received (set by mark_committed).
    pub committed_at: Option<Instant>,
}

/// The type of write operation.
#[derive(Debug, Clone)]
pub enum WriteOp {
    /// SET: replace value at path
    Set(Value),
    /// UPDATE: shallow merge at path
    Update(Value),
    /// TRANSACTION: atomic batch of sub-ops. On commit all apply in order at a
    /// single sequence number; on reject none apply. Mirrors `handle_transaction`.
    Transaction(Vec<TxOp>),
}

/// Tracks all writes and builds expected state from committed writes.
pub struct GroundTruth {
    /// All writes indexed by request_id
    writes: HashMap<String, WriteRecord>,
    /// Monotonically increasing sequence counter
    next_sequence: u64,
    /// Leaves known from earlier cycles, before this cycle's writes.
    baseline: BTreeMap<String, Value>,
    /// At most this many leaves carried from earlier cycles are verified per
    /// pass, picked at random with `sample_seed`. This cycle's leaves are all
    /// verified.
    carried_sample: usize,
    sample_seed: u64,
}

impl GroundTruth {
    pub fn new() -> Self {
        Self::with_baseline(BTreeMap::new(), 0, usize::MAX)
    }

    /// Ground truth for a cycle that starts from `baseline`, verifying up to
    /// `carried_sample` of its leaves, chosen with `sample_seed`.
    pub fn with_baseline(
        baseline: BTreeMap<String, Value>,
        sample_seed: u64,
        carried_sample: usize,
    ) -> Self {
        Self {
            writes: HashMap::new(),
            next_sequence: 1,
            baseline,
            carried_sample,
            sample_seed,
        }
    }

    /// Leaves carried from earlier cycles.
    pub fn baseline_len(&self) -> usize {
        self.baseline.len()
    }

    /// The state to start the next cycle from: this cycle's expected state
    /// minus anything not known for sure. That's whatever a pending
    /// (unacknowledged) write could have changed, since it may or may not
    /// have landed, and the paths in `drop`, which failed verification and
    /// were reported already.
    pub fn carry_forward(&self, drop: &[String]) -> BTreeMap<String, Value> {
        let mut state = self.build_state();
        let pending = self
            .writes
            .values()
            .filter(|w| matches!(w.state, WriteState::Sent));
        for record in pending {
            // `replaced_roots` leaves out the root; a pending write that may
            // have replaced it leaves nothing known.
            let replaces_root = match &record.operation {
                WriteOp::Set(_) => normalize_path(&record.path) == "/",
                WriteOp::Update(_) => false,
                WriteOp::Transaction(subs) => subs.iter().any(|s| {
                    matches!(s.kind, TxOpKind::Set | TxOpKind::Delete)
                        && normalize_path(&s.path) == "/"
                }),
            };
            if replaces_root {
                return BTreeMap::new();
            }
            for root in replaced_roots(record) {
                forget(&mut state, &root);
            }
        }
        for path in drop {
            forget(&mut state, path);
        }
        state
    }

    /// Record a write that was sent to the server.
    pub fn record_sent(&mut self, request_id: &str, client_id: u32, path: &str, op: WriteOp) {
        let record = WriteRecord {
            request_id: request_id.to_string(),
            client_id,
            path: path.to_string(),
            operation: op,
            state: WriteState::Sent,
            sequence: self.next_sequence,
            committed_at: None,
        };
        self.next_sequence += 1;
        trace!("SENT: {} -> {}", request_id, path);
        self.writes.insert(request_id.to_string(), record);
    }

    /// Mark a write as committed (ACK received).
    pub fn mark_committed(&mut self, request_id: &str) -> bool {
        if let Some(record) = self.writes.get_mut(request_id) {
            record.state = WriteState::Committed;
            record.committed_at = Some(Instant::now());
            trace!("COMMITTED: {} -> {}", request_id, record.path);
            true
        } else {
            debug!("ACK for unknown request: {}", request_id);
            false
        }
    }

    /// Mark a write as rejected (NACK received).
    pub fn mark_rejected(&mut self, request_id: &str, error: &str) -> bool {
        if let Some(record) = self.writes.get_mut(request_id) {
            record.state = WriteState::Rejected(error.to_string());
            trace!("REJECTED: {} -> {} ({})", request_id, record.path, error);
            true
        } else {
            debug!("NACK for unknown request: {}", request_id);
            false
        }
    }

    /// Mark all currently Sent writes as Pending (called before crash).
    /// Returns the count of writes that were pending.
    pub fn mark_all_sent_as_pending(&mut self) -> usize {
        // We don't have an explicit Pending variant; we just leave them as Sent.
        // During verification, Sent writes are treated as "may or may not exist".
        self.writes
            .values()
            .filter(|w| matches!(w.state, WriteState::Sent))
            .count()
    }

    /// Build the expected tree state from committed writes, replayed in sequence order.
    /// Returns a nested JSON value representing the expected state at each path.
    pub fn build_expected_state(&self) -> HashMap<String, Value> {
        self.build_state().into_iter().collect()
    }

    fn build_state(&self) -> BTreeMap<String, Value> {
        let mut committed: Vec<&WriteRecord> = self
            .writes
            .values()
            .filter(|w| matches!(w.state, WriteState::Committed))
            .collect();

        // Sort by sequence to replay in order
        committed.sort_by_key(|w| w.sequence);

        let mut state = self.baseline.clone();

        for record in committed {
            match &record.operation {
                WriteOp::Set(value) => {
                    // Remove existing path and ALL children first (SET replaces the whole subtree)
                    remove_subtree(&mut state, &record.path);

                    if !value.is_null() {
                        // SET: replace value at path
                        flatten_value(&record.path, value, &mut state);
                    }
                }
                WriteOp::Update(value) => {
                    // UPDATE: shallow merge at path — each top-level key replaces its subtree
                    if let Some(obj) = value.as_object() {
                        for (key, val) in obj {
                            let child_path = format!("{}/{}", record.path, key);
                            // Clear existing subtree for this child key
                            remove_subtree(&mut state, &child_path);

                            if !val.is_null() {
                                flatten_value(&child_path, val, &mut state);
                            }
                        }
                    }
                }
                WriteOp::Transaction(tx_ops) => {
                    // Apply each sub-op in order. The whole transaction shares
                    // one sequence number, so this happens atomically (no
                    // other write is interleaved).
                    for sub in tx_ops {
                        match &sub.kind {
                            TxOpKind::Set => {
                                remove_subtree(&mut state, &sub.path);
                                if let Some(v) = &sub.value {
                                    if !v.is_null() {
                                        flatten_value(&sub.path, v, &mut state);
                                    }
                                }
                            }
                            TxOpKind::Update => {
                                if let Some(Value::Object(obj)) = &sub.value {
                                    for (key, val) in obj {
                                        let child_path = format!("{}/{}", sub.path, key);
                                        remove_subtree(&mut state, &child_path);
                                        if !val.is_null() {
                                            flatten_value(&child_path, val, &mut state);
                                        }
                                    }
                                }
                            }
                            TxOpKind::Delete => {
                                remove_subtree(&mut state, &sub.path);
                            }
                        }
                    }
                }
            }
        }

        state
    }

    /// Subtrees whose entire contents this cycle's writes determine, paired
    /// with the leaves expected under each.
    ///
    /// Every SET, DELETE and UPDATE child replaces the subtree at its path, so
    /// once a committed write lands there, everything below comes from writes
    /// ground truth tracked — data from earlier cycles can't survive in it. A
    /// read of such a subtree must hold exactly the expected leaves: an extra
    /// key means deleted data came back. Only the outermost of these roots is
    /// returned, and any root that a pending (unacknowledged) write touches is
    /// skipped, since that write may or may not have landed.
    pub fn get_replaced_subtrees(&self) -> Vec<(String, HashMap<String, Value>)> {
        let mut committed: Vec<&WriteRecord> = self
            .writes
            .values()
            .filter(|w| matches!(w.state, WriteState::Committed))
            .collect();
        committed.sort_by_key(|w| w.sequence);

        let mut roots: Vec<String> = committed.iter().flat_map(|w| replaced_roots(w)).collect();
        roots.sort_by_key(|r| r.len());
        roots.dedup();

        let pending_roots: Vec<String> = self
            .writes
            .values()
            .filter(|w| matches!(w.state, WriteState::Sent))
            .flat_map(replaced_roots)
            .collect();

        let mut outermost: HashSet<String> = HashSet::new();
        for root in roots {
            let covered = ancestors_of(&root).any(|a| outermost.contains(a));
            if !covered {
                outermost.insert(root);
            }
        }

        let state = self.build_state();
        let mut out: Vec<(String, HashMap<String, Value>)> = outermost
            .into_iter()
            .filter(|root| !pending_roots.iter().any(|p| paths_overlap(p, root)))
            .map(|root| {
                let prefix = format!("{}/", root);
                let mut leaves: HashMap<String, Value> = subtree_keys(&state, &prefix)
                    .into_iter()
                    .map(|k| {
                        let v = state[&k].clone();
                        (k, v)
                    })
                    .collect();
                if let Some(v) = state.get(&root) {
                    leaves.insert(root.clone(), v.clone());
                }
                (root, leaves)
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Get the count of writes in each state.
    pub fn stats(&self) -> (usize, usize, usize) {
        let mut committed = 0;
        let mut rejected = 0;
        let mut sent = 0;
        for w in self.writes.values() {
            match w.state {
                WriteState::Committed => committed += 1,
                WriteState::Rejected(_) => rejected += 1,
                WriteState::Sent => sent += 1,
            }
        }
        (committed, rejected, sent)
    }

    /// Get all paths that should exist after restart (committed and not deleted).
    /// Returns (path, expected_value) pairs for verification via ONCE reads.
    pub fn get_verification_paths(&self) -> Vec<(String, Value)> {
        let state = self.build_expected_state();
        state.into_iter().collect()
    }

    /// Like get_verification_paths, but also returns the committed_at timestamp
    /// for each path (from the write that last set it). A leaf carried from
    /// an earlier cycle, and not rewritten in this one, has no timestamp; at
    /// most `carried_sample` of those are returned, picked at random.
    pub fn get_verification_paths_with_timing(&self) -> Vec<(String, Value, Option<Instant>)> {
        let state = self.build_state();

        // Collect committed writes sorted by sequence (latest last)
        let mut committed: Vec<&WriteRecord> = self
            .writes
            .values()
            .filter(|w| matches!(w.state, WriteState::Committed))
            .collect();
        committed.sort_by_key(|w| w.sequence);

        let mut current = Vec::new();
        let mut carried = Vec::new();
        for (path, value) in state {
            if self.baseline.get(&path) == Some(&value) {
                carried.push((path, value, None));
                continue;
            }
            // Find the latest committed write that covers this path
            let committed_at = committed
                .iter()
                .rev()
                .find(|w| path == w.path || path.starts_with(&format!("{}/", w.path)))
                .and_then(|w| w.committed_at);
            current.push((path, value, committed_at));
        }
        if carried.len() > self.carried_sample {
            carried.shuffle(&mut StdRng::seed_from_u64(self.sample_seed));
            carried.truncate(self.carried_sample);
        }
        current.extend(carried);
        current
    }

    /// Get all paths from rejected writes (should NOT exist from those writes).
    pub fn get_rejected_paths(&self) -> Vec<String> {
        self.writes
            .values()
            .filter(|w| matches!(w.state, WriteState::Rejected(_)))
            .map(|w| w.path.clone())
            .collect()
    }

    /// Get paths that were in Sent state (pending) - may or may not exist.
    pub fn get_pending_paths(&self) -> Vec<String> {
        self.writes
            .values()
            .filter(|w| matches!(w.state, WriteState::Sent))
            .map(|w| w.path.clone())
            .collect()
    }

    /// Clear all write records (for starting a fresh cycle without losing state tracking).
    pub fn clear(&mut self) {
        self.writes.clear();
        self.next_sequence = 1;
    }

    /// Total number of tracked writes.
    pub fn total_writes(&self) -> usize {
        self.writes.len()
    }

    /// Return every primitive-leaf op that *actually touches* `target_path`,
    /// sorted by sequence ascending. Each entry is a single `set leaf=value`
    /// or `clear leaf` from a write — Object values are expanded all the way
    /// to their leaves so the diagnostic dump shows the full picture.
    ///
    /// "Touches" = the leaf path equals, is an ancestor of, or is a descendant
    /// of the target. No more "root affects everything" wide net.
    pub fn ops_affecting(&self, target_path: &str) -> Vec<AffectingOp> {
        let target = normalize_path(target_path);
        let mut out: Vec<AffectingOp> = Vec::new();
        for record in self.writes.values() {
            for leaf in effective_leaves(record) {
                if leaf_touches(&leaf, &target) {
                    out.push(AffectingOp {
                        sequence: record.sequence,
                        request_id: record.request_id.clone(),
                        state: record.state.clone(),
                        op_kind: record.operation.kind_label(),
                        leaf,
                    });
                }
            }
        }
        out.sort_by_key(|a| a.sequence);
        out
    }

    /// Return every operation whose path (or any sub-op path for transactions)
    /// is the target itself or an ancestor of the target — even if its leaves
    /// don't directly write to target's line. These are the writes that
    /// trigger `promote_path` / `handle_update` on an ancestor, which can
    /// corrupt in-memory state for descendants without leaving a leaf-level
    /// trace. Use alongside `ops_affecting` to see promotion triggers that
    /// `ops_affecting` filters out.
    pub fn promote_triggers_for(&self, target_path: &str) -> Vec<AncestorOp> {
        let target = normalize_path(target_path);
        let mut out: Vec<AncestorOp> = Vec::new();
        for record in self.writes.values() {
            // Skip records whose leaves already cover the target — those will
            // appear in `ops_affecting` already, no need to also list as a
            // trigger.
            let already_covered = effective_leaves(record)
                .iter()
                .any(|leaf| leaf_touches(leaf, &target));
            if already_covered {
                continue;
            }

            // Collect every internal "operation path" the record exercises.
            let mut op_paths: Vec<String> = Vec::new();
            match &record.operation {
                WriteOp::Set(_) | WriteOp::Update(_) => {
                    op_paths.push(normalize_path(&record.path));
                }
                WriteOp::Transaction(subs) => {
                    for sub in subs {
                        op_paths.push(normalize_path(&sub.path));
                    }
                }
            }

            for op_path in op_paths {
                // Path triggers a promotion on target if it equals target or
                // is an ancestor — i.e. the handler called promote_path on
                // something on target's line.
                let triggers = op_path == target
                    || target.starts_with(&format!("{}/", op_path))
                    || op_path == "/";
                if triggers {
                    out.push(AncestorOp {
                        sequence: record.sequence,
                        state: record.state.clone(),
                        op_kind: record.operation.kind_label(),
                        path: op_path,
                    });
                    break; // one trigger per record is enough for the dump
                }
            }
        }
        out.sort_by_key(|a| a.sequence);
        out
    }
}

/// A write whose operation path is at or above the target — represents a
/// "promotion trigger" on the target's line that doesn't directly write a
/// leaf on target's line.
#[derive(Debug, Clone)]
pub struct AncestorOp {
    pub sequence: u64,
    pub state: WriteState,
    pub op_kind: &'static str,
    pub path: String,
}

impl WriteOp {
    /// Short label describing the op shape, e.g. "SET" / "UPDATE" / "TX".
    fn kind_label(&self) -> &'static str {
        match self {
            WriteOp::Set(_) => "SET",
            WriteOp::Update(_) => "UPDATE",
            WriteOp::Transaction(_) => "TX",
        }
    }
}

/// One primitive-level effect of a write, paired with the originating write's
/// sequence/state for context.
#[derive(Debug, Clone)]
pub struct AffectingOp {
    pub sequence: u64,
    pub request_id: String,
    pub state: WriteState,
    /// The shape of the originating write ("SET", "UPDATE", or "TX") — useful
    /// when several leaves on the same path come from different write shapes.
    pub op_kind: &'static str,
    pub leaf: LeafEffect,
}

/// A single primitive-level effect at a fully-resolved path.
#[derive(Debug, Clone)]
pub enum LeafEffect {
    /// Wrote `value` at `path` (value is a JSON primitive — never an object).
    Set { path: String, value: Value },
    /// Cleared `path` (set null, delete, or DELETE sub-op).
    Clear { path: String },
}

impl LeafEffect {
    pub fn path(&self) -> &str {
        match self {
            LeafEffect::Set { path, .. } | LeafEffect::Clear { path } => path,
        }
    }
}

/// Walk a `WriteRecord` and emit every primitive-leaf effect it would produce
/// when applied to ground-truth state. Object values are recursively flattened;
/// SET-null / DELETE / TX-delete produce a single Clear at the op path.
fn effective_leaves(record: &WriteRecord) -> Vec<LeafEffect> {
    let mut out = Vec::new();
    match &record.operation {
        WriteOp::Set(v) => {
            if v.is_null() {
                out.push(LeafEffect::Clear {
                    path: normalize_path(&record.path),
                });
            } else {
                expand_value(&record.path, v, &mut out);
            }
        }
        WriteOp::Update(v) => {
            if let Some(obj) = v.as_object() {
                for (key, val) in obj {
                    let child = join_path(&record.path, key);
                    if val.is_null() {
                        out.push(LeafEffect::Clear { path: child });
                    } else {
                        expand_value(&child, val, &mut out);
                    }
                }
            }
        }
        WriteOp::Transaction(tx_ops) => {
            for sub in tx_ops {
                match sub.kind {
                    TxOpKind::Set => match &sub.value {
                        Some(v) if !v.is_null() => expand_value(&sub.path, v, &mut out),
                        _ => out.push(LeafEffect::Clear {
                            path: normalize_path(&sub.path),
                        }),
                    },
                    TxOpKind::Update => {
                        if let Some(Value::Object(obj)) = &sub.value {
                            for (key, val) in obj {
                                let child = join_path(&sub.path, key);
                                if val.is_null() {
                                    out.push(LeafEffect::Clear { path: child });
                                } else {
                                    expand_value(&child, val, &mut out);
                                }
                            }
                        }
                    }
                    TxOpKind::Delete => {
                        out.push(LeafEffect::Clear {
                            path: normalize_path(&sub.path),
                        });
                    }
                }
            }
        }
    }
    out
}

/// Recursively walk `value` rooted at `base` and append a Set leaf for each
/// primitive, or a Clear for each explicit null. Empty objects produce no
/// entries (they're effectively no-ops on the tree).
fn expand_value(base: &str, value: &Value, out: &mut Vec<LeafEffect>) {
    match value {
        Value::Object(obj) => {
            for (key, val) in obj {
                let child = join_path(base, key);
                expand_value(&child, val, out);
            }
        }
        // Arrays are stored as integer-keyed maps: each element expands under
        // its index. A null element recurses into the Null arm below (a Clear),
        // i.e. an absent index.
        Value::Array(arr) => {
            for (i, val) in arr.iter().enumerate() {
                let child = join_path(base, &i.to_string());
                expand_value(&child, val, out);
            }
        }
        Value::Null => {
            out.push(LeafEffect::Clear {
                path: normalize_path(base),
            });
        }
        prim => {
            out.push(LeafEffect::Set {
                path: normalize_path(base),
                value: prim.clone(),
            });
        }
    }
}

/// True if a leaf at `leaf_path` touches the line of `target` (== / ancestor / descendant).
fn leaf_touches(leaf: &LeafEffect, target: &str) -> bool {
    let p = leaf.path();
    if p == target {
        return true;
    }
    // leaf is an ancestor of target (clobbers when written, exposes when read)
    let p_slash = format!("{}/", p);
    if target.starts_with(&p_slash) {
        return true;
    }
    // leaf is a descendant of target (visible via subtree read of target)
    let t_slash = format!("{}/", target);
    if p.starts_with(&t_slash) {
        return true;
    }
    false
}

/// Normalize "" / "/" to "/", strip trailing slashes (except for root).
fn normalize_path(path: &str) -> String {
    if path.is_empty() {
        return "/".to_string();
    }
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Join a base path with a child key (which may itself contain "/").
fn join_path(base: &str, key: &str) -> String {
    let b = normalize_path(base);
    if b == "/" {
        format!("/{}", key.trim_start_matches('/'))
    } else {
        format!("{}/{}", b, key.trim_start_matches('/'))
    }
}

/// Paths whose whole subtree `record` replaces, joined the same way
/// `build_expected_state` builds its keys.
fn replaced_roots(record: &WriteRecord) -> Vec<String> {
    let mut out = Vec::new();
    let mut children = |base: &str, value: Option<&Value>| {
        if let Some(Value::Object(obj)) = value {
            for key in obj.keys() {
                out.push(format!("{}/{}", base, key));
            }
        }
    };
    let mut roots = Vec::new();
    match &record.operation {
        WriteOp::Set(_) => roots.push(record.path.clone()),
        WriteOp::Update(v) => children(&record.path, Some(v)),
        WriteOp::Transaction(tx_ops) => {
            for sub in tx_ops {
                match sub.kind {
                    TxOpKind::Set | TxOpKind::Delete => roots.push(sub.path.clone()),
                    TxOpKind::Update => children(&sub.path, sub.value.as_ref()),
                }
            }
        }
    }
    out.extend(roots);
    // The root itself would cover data this cycle never saw.
    out.retain(|r| !r.is_empty() && r != "/");
    out
}

/// Strict ancestors of `path`, nearest first: `/a/b/c` → `/a/b`, `/a`.
fn ancestors_of(path: &str) -> impl Iterator<Item = &str> {
    path.char_indices()
        .rev()
        .filter(|&(i, c)| c == '/' && i > 0)
        .map(move |(i, _)| &path[..i])
}

/// True if `a` and `b` are the same path or one contains the other.
fn paths_overlap(a: &str, b: &str) -> bool {
    a == b || a.starts_with(&format!("{}/", b)) || b.starts_with(&format!("{}/", a))
}

/// Keys strictly below `prefix` (which ends in "/"), in order.
fn subtree_keys(state: &BTreeMap<String, Value>, prefix: &str) -> Vec<String> {
    state
        .range::<str, _>((
            std::ops::Bound::Included(prefix),
            std::ops::Bound::Unbounded,
        ))
        .take_while(|(k, _)| k.starts_with(prefix))
        .map(|(k, _)| k.clone())
        .collect()
}

/// Remove `path` and everything below it.
fn remove_subtree(state: &mut BTreeMap<String, Value>, path: &str) {
    state.remove(path);
    for key in subtree_keys(state, &format!("{}/", path)) {
        state.remove(&key);
    }
}

/// Forget everything a write at `path` could have changed: the path, what's
/// below it, and any primitive ancestor it would turn into a container.
fn forget(state: &mut BTreeMap<String, Value>, path: &str) {
    let path = normalize_path(path);
    remove_subtree(state, &path);
    clear_ancestor_leaves(state, &path);
}

/// Flatten a JSON value into leaf paths.
/// For {"a": {"b": 1, "c": 2}} at path "/x", produces:
///   "/x/a/b" -> 1
///   "/x/a/c" -> 2
fn flatten_value(path: &str, value: &Value, state: &mut BTreeMap<String, Value>) {
    match value {
        // A null anywhere in a written value is absent, never stored.
        Value::Null => {}
        Value::Object(obj) => {
            // Remove the parent path itself (it's not a leaf)
            state.remove(path);
            for (key, val) in obj {
                let child_path = format!("{}/{}", path, key);
                flatten_value(&child_path, val, state);
            }
        }
        // Arrays are stored as integer-keyed maps. Each non-null element is a
        // leaf under its index; null elements are gaps (absent), matching the
        // server which never stores nulls.
        Value::Array(arr) => {
            state.remove(path);
            for (i, val) in arr.iter().enumerate() {
                if val.is_null() {
                    continue;
                }
                let child_path = format!("{}/{}", path, i);
                flatten_value(&child_path, val, state);
            }
        }
        _ => {
            // Leaf value — store it. Also clear any ancestor entry that's
            // currently a leaf in `state`: writing a leaf at /a/b/c on the
            // server clobbers a primitive ancestor (e.g. /a) into a container,
            // so the ancestor's primitive value is no longer reachable.
            clear_ancestor_leaves(state, path);
            state.insert(path.to_string(), value.clone());
        }
    }
}

/// Remove any state entry whose key is a strict prefix of `path` (an ancestor).
/// Mirrors `set_path_mut`'s behavior on the server, which clobbers a primitive
/// ancestor into a container when a child path is set under it.
fn clear_ancestor_leaves(state: &mut BTreeMap<String, Value>, path: &str) {
    let mut cur = path;
    while let Some(idx) = cur.rfind('/') {
        if idx == 0 {
            break; // we've walked up to "/" — nothing above root
        }
        cur = &path[..idx];
        state.remove(cur);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn committed(gt: &mut GroundTruth, id: &str, path: &str, op: WriteOp) {
        gt.record_sent(id, 1, path, op);
        gt.mark_committed(id);
    }

    #[test]
    fn baseline_carries_into_the_next_cycle() {
        let mut first = GroundTruth::new();
        committed(
            &mut first,
            "r1",
            "/a",
            WriteOp::Set(json!({"x": 1, "y": 2})),
        );
        committed(&mut first, "r2", "/b", WriteOp::Set(json!(3)));

        let mut second = GroundTruth::with_baseline(first.carry_forward(&[]), 1, usize::MAX);
        committed(&mut second, "r1", "/a", WriteOp::Update(json!({"y": 5})));
        let state = second.build_expected_state();
        assert_eq!(state.get("/a/x"), Some(&json!(1)));
        assert_eq!(state.get("/a/y"), Some(&json!(5)));
        assert_eq!(state.get("/b"), Some(&json!(3)));
    }

    #[test]
    fn a_set_replaces_carried_leaves() {
        let mut first = GroundTruth::new();
        committed(
            &mut first,
            "r1",
            "/a",
            WriteOp::Set(json!({"x": 1, "y": 2})),
        );
        let mut second = GroundTruth::with_baseline(first.carry_forward(&[]), 1, usize::MAX);
        committed(&mut second, "r1", "/a", WriteOp::Set(json!({"z": 3})));
        let state = second.build_expected_state();
        assert_eq!(state.get("/a/x"), None);
        assert_eq!(state.get("/a/z"), Some(&json!(3)));
    }

    #[test]
    fn carry_forward_forgets_what_a_pending_write_could_change() {
        let mut gt = GroundTruth::new();
        committed(&mut gt, "r1", "/a", WriteOp::Set(json!({"x": 1, "y": 2})));
        committed(&mut gt, "r2", "/b", WriteOp::Set(json!(7)));
        committed(&mut gt, "r3", "/c", WriteOp::Set(json!({"k": 1})));
        // Pending: may or may not have landed.
        gt.record_sent("p1", 1, "/a", WriteOp::Update(json!({"y": null})));
        gt.record_sent("p2", 1, "/b/deeper", WriteOp::Set(json!(1)));

        let carried = gt.carry_forward(&[]);
        assert_eq!(
            carried.get("/a/x"),
            Some(&json!(1)),
            "untouched sibling stays"
        );
        assert_eq!(
            carried.get("/a/y"),
            None,
            "a pending update's key is unknown"
        );
        assert_eq!(
            carried.get("/b"),
            None,
            "a primitive the pending write could replace"
        );
        assert_eq!(carried.get("/c/k"), Some(&json!(1)));
    }

    #[test]
    fn carry_forward_drops_violated_paths() {
        let mut gt = GroundTruth::new();
        committed(&mut gt, "r1", "/a", WriteOp::Set(json!({"x": 1, "y": 2})));
        let carried = gt.carry_forward(&["/a/x".to_string()]);
        assert_eq!(carried.get("/a/x"), None);
        assert_eq!(carried.get("/a/y"), Some(&json!(2)));
    }

    #[test]
    fn a_pending_root_set_forgets_everything() {
        let mut gt = GroundTruth::new();
        committed(&mut gt, "r1", "/a", WriteOp::Set(json!(1)));
        committed(&mut gt, "r2", "/b", WriteOp::Set(json!(1)));
        gt.record_sent("p1", 1, "/", WriteOp::Set(json!({"a": 2})));
        assert!(gt.carry_forward(&[]).is_empty());

        // A pending update at the root only touches its keys.
        let mut gt = GroundTruth::new();
        committed(&mut gt, "r1", "/a", WriteOp::Set(json!(1)));
        committed(&mut gt, "r2", "/b", WriteOp::Set(json!(1)));
        gt.record_sent("p1", 1, "", WriteOp::Update(json!({"a": 2})));
        let carried = gt.carry_forward(&[]);
        assert!(!carried.contains_key("/a"));
        assert_eq!(carried.get("/b"), Some(&json!(1)));
    }

    #[test]
    fn carried_leaves_are_sampled_and_current_ones_are_not() {
        let mut first = GroundTruth::new();
        let many: serde_json::Map<String, Value> =
            (0..50).map(|i| (format!("k{i}"), json!(i))).collect();
        committed(&mut first, "r1", "/old", WriteOp::Set(Value::Object(many)));

        let mut second = GroundTruth::with_baseline(first.carry_forward(&[]), 7, 10);
        committed(
            &mut second,
            "r1",
            "/new",
            WriteOp::Set(json!({"a": 1, "b": 2})),
        );
        let paths = second.get_verification_paths_with_timing();
        let carried = paths
            .iter()
            .filter(|(p, _, _)| p.starts_with("/old/"))
            .count();
        let current = paths
            .iter()
            .filter(|(p, _, _)| p.starts_with("/new/"))
            .count();
        assert_eq!(carried, 10);
        assert_eq!(current, 2);
    }

    #[test]
    fn test_basic_set_and_verify() {
        let mut gt = GroundTruth::new();
        gt.record_sent(
            "r1",
            1,
            "/users/alice",
            WriteOp::Set(json!({"name": "Alice"})),
        );
        gt.mark_committed("r1");

        let state = gt.build_expected_state();
        assert_eq!(state.get("/users/alice/name"), Some(&json!("Alice")));
    }

    #[test]
    fn test_nested_null_is_absent() {
        let mut gt = GroundTruth::new();
        gt.record_sent(
            "r1",
            1,
            "",
            WriteOp::Update(json!({"a/b": {"x": 1, "y": null}})),
        );
        gt.mark_committed("r1");

        let state = gt.build_expected_state();
        assert_eq!(state.get("/a/b/x"), Some(&json!(1)));
        assert!(!state.contains_key("/a/b/y"));
    }

    #[test]
    fn test_replaced_subtrees_outermost_with_expected_leaves() {
        let mut gt = GroundTruth::new();
        gt.record_sent("r1", 1, "/a", WriteOp::Set(json!({"x": 1, "old": 2})));
        gt.record_sent(
            "r2",
            1,
            "",
            WriteOp::Update(json!({"b/c": null, "a/old": null})),
        );
        gt.record_sent("r3", 1, "/a/y", WriteOp::Set(json!(3)));
        for r in ["r1", "r2", "r3"] {
            gt.mark_committed(r);
        }

        let subtrees = gt.get_replaced_subtrees();
        let roots: Vec<&str> = subtrees.iter().map(|(r, _)| r.as_str()).collect();
        assert_eq!(roots, vec!["/a", "/b/c"]);

        let a = &subtrees[0].1;
        assert_eq!(a.len(), 2);
        assert_eq!(a.get("/a/x"), Some(&json!(1)));
        assert_eq!(a.get("/a/y"), Some(&json!(3)));
        assert!(subtrees[1].1.is_empty());
    }

    #[test]
    fn test_replaced_subtrees_skip_pending_overlap() {
        let mut gt = GroundTruth::new();
        gt.record_sent("r1", 1, "/a", WriteOp::Set(json!(null)));
        gt.record_sent("r2", 1, "/b", WriteOp::Set(json!(1)));
        gt.mark_committed("r1");
        gt.mark_committed("r2");
        // Unacknowledged write below /a: it may or may not have landed.
        gt.record_sent("r3", 1, "/a/z", WriteOp::Set(json!(1)));

        let roots: Vec<String> = gt
            .get_replaced_subtrees()
            .into_iter()
            .map(|(r, _)| r)
            .collect();
        assert_eq!(roots, vec!["/b".to_string()]);
    }

    #[test]
    fn test_delete_removes_path() {
        let mut gt = GroundTruth::new();
        gt.record_sent("r1", 1, "/data/x", WriteOp::Set(json!(42)));
        gt.mark_committed("r1");
        gt.record_sent("r2", 1, "/data/x", WriteOp::Set(Value::Null));
        gt.mark_committed("r2");

        let state = gt.build_expected_state();
        assert!(!state.contains_key("/data/x"));
    }

    #[test]
    fn test_update_shallow_merge() {
        let mut gt = GroundTruth::new();
        gt.record_sent(
            "r1",
            1,
            "/user",
            WriteOp::Set(json!({"name": "A", "score": 0})),
        );
        gt.mark_committed("r1");
        gt.record_sent("r2", 1, "/user", WriteOp::Update(json!({"score": 100})));
        gt.mark_committed("r2");

        let state = gt.build_expected_state();
        assert_eq!(state.get("/user/name"), Some(&json!("A")));
        assert_eq!(state.get("/user/score"), Some(&json!(100)));
    }

    #[test]
    fn test_rejected_not_in_expected() {
        let mut gt = GroundTruth::new();
        gt.record_sent("r1", 1, "/secret", WriteOp::Set(json!("hack")));
        gt.mark_rejected("r1", "permission_denied");

        let state = gt.build_expected_state();
        assert!(state.is_empty());
    }

    #[test]
    fn test_set_replaces_entire_subtree() {
        // This is the exact bug pattern from the chaos run:
        // SET an object, then UPDATE adds a field, then SET replaces with new object.
        // The UPDATE'd field should NOT be in the expected state.
        let mut gt = GroundTruth::new();
        gt.record_sent(
            "r1",
            1,
            "/data/item",
            WriteOp::Set(json!({"name": "A", "value": 1})),
        );
        gt.mark_committed("r1");
        gt.record_sent(
            "r2",
            1,
            "/data/item",
            WriteOp::Update(json!({"updated_field": 8578})),
        );
        gt.mark_committed("r2");
        gt.record_sent(
            "r3",
            1,
            "/data/item",
            WriteOp::Set(json!({"name": "B", "value": 2})),
        );
        gt.mark_committed("r3");

        let state = gt.build_expected_state();
        // After the final SET, only name and value should exist
        assert_eq!(state.get("/data/item/name"), Some(&json!("B")));
        assert_eq!(state.get("/data/item/value"), Some(&json!(2)));
        // The updated_field from the UPDATE should be gone (SET replaced the whole subtree)
        assert!(!state.contains_key("/data/item/updated_field"));
    }

    #[test]
    fn test_set_clears_old_children() {
        // SET to an object, then SET to a different object with different keys
        let mut gt = GroundTruth::new();
        gt.record_sent("r1", 1, "/obj", WriteOp::Set(json!({"a": 1, "b": 2})));
        gt.mark_committed("r1");
        gt.record_sent("r2", 1, "/obj", WriteOp::Set(json!({"c": 3})));
        gt.mark_committed("r2");

        let state = gt.build_expected_state();
        assert!(!state.contains_key("/obj/a"));
        assert!(!state.contains_key("/obj/b"));
        assert_eq!(state.get("/obj/c"), Some(&json!(3)));
    }

    #[test]
    fn test_update_replaces_child_subtree() {
        // UPDATE where a child key replaces a nested object with a scalar
        let mut gt = GroundTruth::new();
        gt.record_sent(
            "r1",
            1,
            "/user",
            WriteOp::Set(json!({"profile": {"name": "A", "age": 30}})),
        );
        gt.mark_committed("r1");
        gt.record_sent(
            "r2",
            1,
            "/user",
            WriteOp::Update(json!({"profile": "simple"})),
        );
        gt.mark_committed("r2");

        let state = gt.build_expected_state();
        // profile is now a leaf, old children should be gone
        assert_eq!(state.get("/user/profile"), Some(&json!("simple")));
        assert!(!state.contains_key("/user/profile/name"));
        assert!(!state.contains_key("/user/profile/age"));
    }

    #[test]
    fn test_multi_path_update_at_root() {
        // UPDATE at "" (root) with slash-keyed values — what the Firebase REST
        // adapter writes for a multi-path PATCH. Each slash-keyed entry should
        // become an independent leaf in the expected state.
        let mut gt = GroundTruth::new();
        gt.record_sent(
            "r1",
            1,
            "",
            WriteOp::Update(json!({
                "character_names/sorcerertest": "c1",
                "characters/c1": {
                    "account_id": "a1",
                    "class_id": "sorcerer",
                },
                "accounts/a1/characters/c1/level": 30,
            })),
        );
        gt.mark_committed("r1");

        let state = gt.build_expected_state();
        assert_eq!(
            state.get("/character_names/sorcerertest"),
            Some(&json!("c1"))
        );
        assert_eq!(state.get("/characters/c1/account_id"), Some(&json!("a1")));
        assert_eq!(
            state.get("/characters/c1/class_id"),
            Some(&json!("sorcerer"))
        );
        assert_eq!(
            state.get("/accounts/a1/characters/c1/level"),
            Some(&json!(30))
        );
    }

    #[test]
    fn test_multi_path_update_at_subpath() {
        // UPDATE at /users with slash-keyed values writes leaves under /users.
        let mut gt = GroundTruth::new();
        gt.record_sent(
            "r1",
            1,
            "/users",
            WriteOp::Update(json!({
                "alice/name": "Alice",
                "alice/score": 100,
                "bob/score": 50,
            })),
        );
        gt.mark_committed("r1");

        let state = gt.build_expected_state();
        assert_eq!(state.get("/users/alice/name"), Some(&json!("Alice")));
        assert_eq!(state.get("/users/alice/score"), Some(&json!(100)));
        assert_eq!(state.get("/users/bob/score"), Some(&json!(50)));
    }

    #[test]
    fn test_multi_path_update_then_overwrite_via_set() {
        // Multi-path UPDATE creates leaves; a later SET at one of those leaves'
        // ancestors should replace the whole subtree.
        let mut gt = GroundTruth::new();
        gt.record_sent(
            "r1",
            1,
            "",
            WriteOp::Update(json!({
                "accounts/a1/characters/c1/level": 1,
                "accounts/a1/characters/c1/zone": "start",
            })),
        );
        gt.mark_committed("r1");
        gt.record_sent(
            "r2",
            1,
            "/accounts/a1/characters/c1",
            WriteOp::Set(json!({"level": 10, "name": "X"})),
        );
        gt.mark_committed("r2");

        let state = gt.build_expected_state();
        assert_eq!(
            state.get("/accounts/a1/characters/c1/level"),
            Some(&json!(10))
        );
        assert_eq!(
            state.get("/accounts/a1/characters/c1/name"),
            Some(&json!("X"))
        );
        // The "zone" leaf from the multi-path UPDATE should be gone.
        assert!(!state.contains_key("/accounts/a1/characters/c1/zone"));
    }

    #[test]
    fn test_writing_descendant_clobbers_primitive_ancestor() {
        // Server semantic: SET /a/b = primitive, then SET /a/b/c = primitive
        // clobbers /a/b's primitive into a container. Ground truth must mirror
        // this — otherwise it tracks both /a/b and /a/b/c as leaves and the
        // verifier flags a false-positive when reading /a/b.
        let mut gt = GroundTruth::new();
        gt.record_sent("r1", 1, "/a/b", WriteOp::Set(json!(9709)));
        gt.mark_committed("r1");
        gt.record_sent("r2", 1, "/a/b/c", WriteOp::Set(json!(100)));
        gt.mark_committed("r2");

        let state = gt.build_expected_state();
        // /a/b is gone — the server's primitive at that path got clobbered.
        assert!(
            !state.contains_key("/a/b"),
            "ancestor leaf should be cleared, got: {:?}",
            state.get("/a/b")
        );
        // /a/b/c is the only leaf.
        assert_eq!(state.get("/a/b/c"), Some(&json!(100)));
    }

    #[test]
    fn test_multi_path_update_clobbers_ancestor_within_same_op() {
        // Within a single multi-path UPDATE, having both "a/b" (primitive) and
        // "a/b/c" (primitive) keys causes the deeper key to clobber the
        // shallower (server iterates alphabetically; "a/b" < "a/b/c").
        let mut gt = GroundTruth::new();
        gt.record_sent(
            "r1",
            1,
            "",
            WriteOp::Update(json!({
                "a/b": 1,
                "a/b/c": 2,
            })),
        );
        gt.mark_committed("r1");

        let state = gt.build_expected_state();
        assert!(!state.contains_key("/a/b"), "shallower key got clobbered");
        assert_eq!(state.get("/a/b/c"), Some(&json!(2)));
    }

    #[test]
    fn test_transaction_applies_all_ops_on_commit() {
        // TRANSACTION mimics the Firebase REST adapter's translation of a
        // multi-path PATCH whose keys contain "/" — each key becomes a SET sub-op
        // at the full leaf path. This is what handle_transaction processes.
        let mut gt = GroundTruth::new();
        let tx = vec![
            TxOp {
                kind: TxOpKind::Set,
                path: "/character_names/sorcerertest".to_string(),
                value: Some(json!("c1")),
            },
            TxOp {
                kind: TxOpKind::Set,
                path: "/accounts/a1/characters/c1".to_string(),
                value: Some(json!({"level": 1, "class": "sorcerer"})),
            },
            TxOp {
                kind: TxOpKind::Update,
                path: "/players/p1".to_string(),
                value: Some(json!({"name": "Alice", "score": 100})),
            },
        ];
        gt.record_sent("r1", 1, "", WriteOp::Transaction(tx));
        gt.mark_committed("r1");

        let state = gt.build_expected_state();
        assert_eq!(
            state.get("/character_names/sorcerertest"),
            Some(&json!("c1"))
        );
        assert_eq!(
            state.get("/accounts/a1/characters/c1/level"),
            Some(&json!(1))
        );
        assert_eq!(
            state.get("/accounts/a1/characters/c1/class"),
            Some(&json!("sorcerer"))
        );
        assert_eq!(state.get("/players/p1/name"), Some(&json!("Alice")));
        assert_eq!(state.get("/players/p1/score"), Some(&json!(100)));
    }

    #[test]
    fn test_transaction_no_ops_apply_on_reject() {
        // If the server NACKs the transaction, NONE of its sub-ops apply.
        let mut gt = GroundTruth::new();
        let tx = vec![TxOp {
            kind: TxOpKind::Set,
            path: "/foo".to_string(),
            value: Some(json!("bar")),
        }];
        gt.record_sent("r1", 1, "", WriteOp::Transaction(tx));
        gt.mark_rejected("r1", "permission_denied");

        let state = gt.build_expected_state();
        assert!(state.is_empty());
    }

    #[test]
    fn test_transaction_delete_clears_subtree() {
        // A SET creates leaves, then a TRANSACTION DELETE wipes the subtree.
        let mut gt = GroundTruth::new();
        gt.record_sent(
            "r1",
            1,
            "/users/alice",
            WriteOp::Set(json!({"name": "A", "score": 5})),
        );
        gt.mark_committed("r1");
        let tx = vec![TxOp {
            kind: TxOpKind::Delete,
            path: "/users/alice".to_string(),
            value: None,
        }];
        gt.record_sent("r2", 1, "", WriteOp::Transaction(tx));
        gt.mark_committed("r2");

        let state = gt.build_expected_state();
        assert!(!state.contains_key("/users/alice/name"));
        assert!(!state.contains_key("/users/alice/score"));
    }

    #[test]
    fn test_stats() {
        let mut gt = GroundTruth::new();
        gt.record_sent("r1", 1, "/a", WriteOp::Set(json!(1)));
        gt.record_sent("r2", 1, "/b", WriteOp::Set(json!(2)));
        gt.record_sent("r3", 1, "/c", WriteOp::Set(json!(3)));
        gt.mark_committed("r1");
        gt.mark_rejected("r2", "err");

        let (committed, rejected, sent) = gt.stats();
        assert_eq!(committed, 1);
        assert_eq!(rejected, 1);
        assert_eq!(sent, 1);
    }

    #[test]
    fn test_array_expands_to_index_leaves() {
        let mut gt = GroundTruth::new();
        gt.record_sent("r1", 1, "/arr", WriteOp::Set(json!(["a", "b", "c"])));
        gt.mark_committed("r1");

        let state = gt.build_expected_state();
        assert_eq!(state.get("/arr/0"), Some(&json!("a")));
        assert_eq!(state.get("/arr/1"), Some(&json!("b")));
        assert_eq!(state.get("/arr/2"), Some(&json!("c")));
        // The container path itself is not a leaf.
        assert!(!state.contains_key("/arr"));
    }

    #[test]
    fn test_array_null_element_is_gap() {
        let mut gt = GroundTruth::new();
        gt.record_sent("r1", 1, "/arr", WriteOp::Set(json!(["a", null, "c"])));
        gt.mark_committed("r1");

        let state = gt.build_expected_state();
        assert_eq!(state.get("/arr/0"), Some(&json!("a")));
        assert!(!state.contains_key("/arr/1")); // null element = gap
        assert_eq!(state.get("/arr/2"), Some(&json!("c")));
    }

    #[test]
    fn test_array_partial_element_write_preserves_siblings() {
        // The exact bug pattern: write an array of objects, then set a field on
        // one element. The other elements and the element's other field survive.
        let mut gt = GroundTruth::new();
        gt.record_sent(
            "r1",
            1,
            "/arr",
            WriteOp::Set(json!([{"x": 1, "label": "a"}, {"x": 2, "label": "b"}])),
        );
        gt.mark_committed("r1");
        gt.record_sent("r2", 1, "/arr/0/x", WriteOp::Set(json!(99)));
        gt.mark_committed("r2");

        let state = gt.build_expected_state();
        assert_eq!(state.get("/arr/0/x"), Some(&json!(99))); // updated
        assert_eq!(state.get("/arr/0/label"), Some(&json!("a"))); // sibling field survives
        assert_eq!(state.get("/arr/1/x"), Some(&json!(2))); // sibling element survives
        assert_eq!(state.get("/arr/1/label"), Some(&json!("b")));
    }
}
