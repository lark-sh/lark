//! Database: The core runtime for a single database.
//!
//! Each database runs as an independent Glommio task on a specific core, processing
//! messages from its inbox channel. The database is the unit of state - it owns:
//!
//! - **Tree**: The JSON data structure (using ArcValue for copy-on-write)
//! - **ViewManager**: Subscription tracking, delta events, and volatile batching
//! - **Evaluator**: Security rules evaluation
//! - **WalWriter**: Write-ahead log for durability
//! - **BlobSession**: Blob storage for persistence
//!
//! ## Message Loop
//!
//! The database receives work via `LocalChannel<InboxMessage>` (lock-free, single-consumer).
//! The main loop:
//! 1. Polls inbox with `poll_immediate()` (non-blocking)
//! 2. Batch-processes all ready messages
//! 3. Applies mutations to tree (with rules checks)
//! 4. Generates delta events for subscribed views
//! 5. Sends events to clients (with rate limiting)
//! 6. Flushes WAL periodically (every 2s)
//!
//! ## Rate Limiting
//!
//! Events are rate-limited to prevent overwhelming slow clients:
//! - Volatile paths: 33ms intervals (~30Hz) - for high-frequency game state
//! - Non-volatile: 100ms intervals (~10Hz) - for normal data updates
//!

use indexmap::IndexSet;
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::storage::{CompactionRequest, StorageWorkerMessage};
use bytes::Bytes;
use futures::FutureExt;
use futures::future::poll_immediate;
use glommio::channels::local_channel::{self, LocalReceiver, LocalSender};
use glommio::timer::Timer;
use serde_json::Value;
use tracing::{debug, error, info, trace, warn};

use crate::storage::glommio_blob_io::GlommioBlobIO;
use lark_blob::{BlobError, BlobIO, BlobSession, CachedIO, ReadStats, ShallowValue};

use crate::db::firebase_hash::{compute_firebase_hash, is_firebase_hash};
use crate::db::query::{QueryError, QueryParams};
use crate::db::subscription::{MutationEvent, SubscribeError, ViewManager};
use crate::db::value::ArcValueSortExt;
use crate::db::{ArcValue, Path, Tree};
use crate::protocol::{ClientMessage, ServerMessage, error, op};
use crate::rules::{
    AuthInfo as RulesAuthInfo, Evaluator, NewData, RuleSet, RulesContext, TreeGetter,
};
use crate::storage::{WalEntry, WalOp, WalReader, WalWriter, read_file_async};

/// Wrapper around Arc<RwLock<Tree>> that implements TreeGetter.
/// This allows rules evaluation to access the tree without holding
/// a mutable borrow on Database.
pub struct TreeAccessor {
    tree: Arc<RwLock<Tree>>,
    blob_backed: bool,
}

impl TreeAccessor {
    pub fn new(tree: Arc<RwLock<Tree>>, blob_backed: bool) -> Self {
        Self { tree, blob_backed }
    }
}

impl TreeGetter for TreeAccessor {
    fn get_value(&self, path: &str) -> Option<serde_json::Value> {
        let tree = self.tree.read().unwrap();
        let path = Path::parse(path);
        tree.get(&path).map(|n| n.to_value())
    }

    fn get_node_value(&self, path: &str) -> Option<serde_json::Value> {
        let tree = self.tree.read().unwrap();
        let path = Path::parse(path);
        tree.get(&path).map(|n| n.to_value())
    }

    fn node_exists(&self, path: &str) -> bool {
        let tree = self.tree.read().unwrap();
        let path = Path::parse(path);
        tree.get(&path).map(|n| n.exists()).unwrap_or(false)
    }

    fn node_has_child(&self, path: &str, child_name: &str) -> bool {
        let tree = self.tree.read().unwrap();
        let path = Path::parse(path);
        tree.get(&path)
            .and_then(|n| n.get(child_name))
            .map(|c| c.exists())
            .unwrap_or(false)
    }

    fn node_is_loaded(&self, path: &str) -> bool {
        let tree = self.tree.read().unwrap();
        tree.node_is_loaded(path)
    }

    fn is_blob_backed(&self) -> bool {
        self.blob_backed
    }
}

/// Zero-allocation check: is `child` a path descendant of `parent`?
/// e.g. is_path_descendant("/users", "/users/alice") == true
/// Equivalent to `child.starts_with(&format!("{}/", parent))` but without allocating.
#[inline]
fn is_path_descendant(parent: &str, child: &str) -> bool {
    child.len() > parent.len()
        && child.starts_with(parent)
        && child.as_bytes()[parent.len()] == b'/'
}

/// Tracks promotion statistics for a metrics interval (reset on emit).
struct PromotionStats {
    count: u64,
    total_us: u64,
    total_read_us: u64,
    durations_us: Vec<u64>,
    read_durations_us: Vec<u64>,
    // Accumulated I/O stats across all promotions in this window
    pread_count: u64,
    bytes_read: u64,
    cache_hits: u64,
    cache_hit_bytes: u64,
    cache_header_misses: u64,
}

impl PromotionStats {
    fn new() -> Self {
        Self {
            count: 0,
            total_us: 0,
            total_read_us: 0,
            durations_us: Vec::new(),
            read_durations_us: Vec::new(),
            pread_count: 0,
            bytes_read: 0,
            cache_hits: 0,
            cache_hit_bytes: 0,
            cache_header_misses: 0,
        }
    }

    fn record(&mut self, total: Duration, read: Duration, io_stats: ReadStats) {
        self.count += 1;
        self.total_us += total.as_micros() as u64;
        self.total_read_us += read.as_micros() as u64;
        self.durations_us.push(total.as_micros() as u64);
        self.read_durations_us.push(read.as_micros() as u64);
        self.pread_count += io_stats.pread_count;
        self.bytes_read += io_stats.bytes_read;
        self.cache_hits += io_stats.cache_hits;
        self.cache_hit_bytes += io_stats.cache_hit_bytes;
        self.cache_header_misses += io_stats.cache_header_misses;
    }

    fn percentile(sorted: &[u64], p: f64) -> u64 {
        if sorted.is_empty() {
            return 0;
        }
        let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }

    fn reset(&mut self) -> PromotionSnapshot {
        let mut sorted = std::mem::take(&mut self.durations_us);
        let mut read_sorted = std::mem::take(&mut self.read_durations_us);
        sorted.sort_unstable();
        read_sorted.sort_unstable();
        let snap = PromotionSnapshot {
            count: self.count,
            total_us: self.total_us,
            total_read_us: self.total_read_us,
            p50: Self::percentile(&sorted, 50.0),
            p95: Self::percentile(&sorted, 95.0),
            p99: Self::percentile(&sorted, 99.0),
            read_p50: Self::percentile(&read_sorted, 50.0),
            read_p95: Self::percentile(&read_sorted, 95.0),
            read_p99: Self::percentile(&read_sorted, 99.0),
            pread_count: self.pread_count,
            bytes_read: self.bytes_read,
            cache_hits: self.cache_hits,
            cache_hit_bytes: self.cache_hit_bytes,
            cache_header_misses: self.cache_header_misses,
        };
        self.count = 0;
        self.total_us = 0;
        self.total_read_us = 0;
        self.pread_count = 0;
        self.bytes_read = 0;
        self.cache_hits = 0;
        self.cache_hit_bytes = 0;
        self.cache_header_misses = 0;
        snap
    }
}

struct PromotionSnapshot {
    count: u64,
    total_us: u64,
    total_read_us: u64,
    p50: u64,
    p95: u64,
    p99: u64,
    read_p50: u64,
    read_p95: u64,
    read_p99: u64,
    pread_count: u64,
    bytes_read: u64,
    cache_hits: u64,
    cache_hit_bytes: u64,
    cache_header_misses: u64,
}

/// Canonical form for path-keyed maps inside `Database` (`WalIndex.by_path`,
/// `Database.promoted_paths`, and any future map keyed by tree path).
///
/// Callers reach these maps from two worlds with different leading-slash
/// conventions:
///   - Wire-protocol handlers (`handle_set`/`handle_update`/`handle_remove`,
///     `handle_subscribe`, `handle_once`, `handle_transaction`, query view
///     recompute) pass paths as received from the wire, typically `"/foo/bar"`.
///     Root multi-path PATCH from the Firebase REST adapter passes `""`.
///   - The rules-eval retry loop passes `NeedsPromotion.path`, which comes
///     from `LazySnapshot.path` — and `evaluator.rs`'s `eval_expr` constructs
///     that snapshot via `ctx.path.trim_start_matches('/')` (no leading slash,
///     so `data.foo` and `root.foo` build a consistent join shape).
///
/// Without normalization a BTreeMap stores e.g. `/paths/.../pathA` (writer
/// side) but a lookup with `paths/.../pathA` (reader side) misses — exact
/// match fails, and the descendant range scan also misses because
/// lexicographic order puts `/paths/...` before `paths/...`. The bug surfaced
/// in `WalIndex` first (correctness regression: WAL replay on promotion
/// silently dropped pending writes for the just-created path), and
/// `promoted_paths` had the same shape (a path could end up tracked under
/// both forms — only a perf regression, but worth keeping clean).
///
/// Canonical form: **leading slash present** (root is `"/"`). This matches
/// `sentinel_paths`'s convention and the prefix-based descendant scans that
/// build `format!("{}/", path)` from a key in `promoted_paths` to find
/// related sentinel-tracking entries — those scans assume the key already
/// has a leading slash, so a no-slash canonical form here would silently
/// miss every descendant during eviction cleanup.
fn normalize_path_key(path: &str) -> String {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", trimmed)
    }
}

/// Index over pending_wal_entries for fast path-based lookups.
/// Replaces O(n) linear scan with O(depth + k) where k = matching entries.
struct WalIndex {
    /// Path → indices into pending_wal_entries. BTreeMap for range scans (descendants).
    by_path: std::collections::BTreeMap<String, Vec<usize>>,
}

impl WalIndex {
    fn new() -> Self {
        Self {
            by_path: std::collections::BTreeMap::new(),
        }
    }

    /// Add a single entry at the given index.
    fn add(&mut self, path: &str, index: usize) {
        self.by_path
            .entry(normalize_path_key(path))
            .or_default()
            .push(index);
    }

    /// Rebuild the entire index from scratch (after compaction trim).
    fn rebuild(&mut self, entries: &[WalEntry]) {
        self.by_path.clear();
        for (i, entry) in entries.iter().enumerate() {
            self.by_path
                .entry(normalize_path_key(&entry.path))
                .or_default()
                .push(i);
        }
    }

    /// Find all entry indices that affect a given promotion path.
    /// Returns indices sorted in insertion order (ascending).
    fn find_affecting(&self, path: &str) -> Vec<usize> {
        let path_owned = normalize_path_key(path);
        let path = path_owned.as_str();
        let mut indices = Vec::new();

        if path == "/" {
            // Root promotion — everything matches
            for idxs in self.by_path.values() {
                indices.extend(idxs);
            }
            indices.sort_unstable();
            return indices;
        }

        // 1. Exact match
        if let Some(idxs) = self.by_path.get(path) {
            indices.extend(idxs);
        }

        // 2. Root entries (affect everything)
        if let Some(idxs) = self.by_path.get("/") {
            indices.extend(idxs);
        }

        // 3. Ancestors: walk up the path
        let mut current = path;
        loop {
            match current.rfind('/') {
                None => break,
                Some(0) => break, // we already checked "/"
                Some(pos) => {
                    current = &path[..pos];
                    if let Some(idxs) = self.by_path.get(current) {
                        indices.extend(idxs);
                    }
                }
            }
        }

        // 4. Descendants: BTreeMap range scan starting just past "{path}",
        //    breaking when keys no longer start with "{path}/". Zero allocations.
        use std::ops::Bound;
        for (key, idxs) in self
            .by_path
            .range::<str, _>((Bound::Excluded(path), Bound::Unbounded))
        {
            if !(key.len() > path.len()
                && key.starts_with(path)
                && key.as_bytes()[path.len()] == b'/')
            {
                break;
            }
            indices.extend(idxs);
        }

        indices.sort_unstable();
        indices
    }
}

/// Return the fixed blob path: `data_dir/blob.lark`.
pub fn blob_path(data_dir: &std::path::Path) -> PathBuf {
    data_dir.join("blob.lark")
}

/// Return the fixed sidecar path: `data_dir/sidecar.lark`.
pub fn sidecar_path(data_dir: &std::path::Path) -> PathBuf {
    data_dir.join("sidecar.lark")
}

/// Read the blob generation number from `data_dir/blob.generation`.
/// Returns 0 if the file doesn't exist or can't be parsed.
pub fn read_blob_generation(data_dir: &std::path::Path) -> u64 {
    match std::fs::read_to_string(data_dir.join("blob.generation")) {
        Ok(s) => s.trim().parse::<u64>().unwrap_or(0),
        Err(_) => 0,
    }
}

/// Check if a path matches a volatile path pattern.
/// Patterns use `*` as a wildcard for a single segment.
/// E.g., pattern "players/*/position" matches "/players/abc/position".
/// Volatile cascades: children of volatile paths are also volatile.
/// E.g., pattern "cursors" matches "/cursors/player1" (child of volatile node).
fn path_matches_pattern(path: &str, pattern: &str) -> bool {
    // Normalize: strip leading slash from path for comparison
    let path = path.strip_prefix('/').unwrap_or(path);

    let path_segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let pattern_segments: Vec<&str> = pattern.split('/').filter(|s| !s.is_empty()).collect();

    // Path must have at least as many segments as pattern (exact match or child)
    if path_segments.len() < pattern_segments.len() {
        return false;
    }

    // Check that the pattern segments match the beginning of the path
    for (path_seg, pattern_seg) in path_segments.iter().zip(pattern_segments.iter()) {
        if *pattern_seg != "*" && path_seg != pattern_seg {
            return false;
        }
    }

    true
}

/// Volatile batch flush interval for high-frequency clients (KCP/WebTransport)
const VOLATILE_FAST_FLUSH_INTERVAL: Duration = Duration::from_millis(50); // 20Hz

/// Volatile batch flush interval for slow clients (WebSocket)
const VOLATILE_SLOW_FLUSH_INTERVAL: Duration = Duration::from_millis(150); // ~7Hz

/// WAL sync interval (fsync to disk)
const WAL_SYNC_INTERVAL: Duration = Duration::from_secs(2);

/// Metrics emission interval (only for active databases)
const METRICS_EMIT_INTERVAL: Duration = Duration::from_secs(60);

/// Maximum number of retries when rules evaluation hits unloaded blob data.
/// This handles rules that access many paths - each iteration
/// loads one path from blob and retries evaluation.
/// Max retries for the rules-eval promotion loop. Each iteration loads ONE
/// path that a rule referenced and re-evaluates. Sized to accommodate
/// rules with many distinct `data.*` / `root.*` / `newData.*` accesses.
/// (Lazy `newData` post-refactor can also trigger promotions for
/// tree-side siblings the UPDATE doesn't touch — each such access
/// consumes one iteration.)
const MAX_PROMOTION_RETRIES: usize = 50;

/// How long a promoted path can remain idle before being evicted back to Sentinel.
/// Eviction reclaims memory; re-promotion from blob + WAL replay restores the data.
/// Default 300s; override via `set_eviction_idle_secs` at startup (driven by the
/// `LARK_EVICTION_IDLE_SECS` env var; chaos-monkey uses this to force frequent
/// evictions).
pub static EVICTION_IDLE_SECS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(300);

pub fn set_eviction_idle_secs(secs: u64) {
    EVICTION_IDLE_SECS.store(secs, std::sync::atomic::Ordering::SeqCst);
}

/// Maximum number of operations in a single transaction. Each condition op
/// triggers a `promote_path_deep` (blob read + WAL replay) on the database's
/// single-threaded inbox, so an unbounded count lets one ~16 MB request serialize
/// many disk round trips and stall every client on the database. Firebase
/// documents no transaction op-count limit; this is a generous DoS rail well
/// under the 16 MB message ceiling (audit M-2).
const MAX_TRANSACTION_OPS: usize = 1_000;

/// Maximum number of onDisconnect actions a single client connection may have
/// registered at once. They accumulate in memory until the client disconnects,
/// so an unbounded count is an asymmetric per-connection memory sink whose OOM
/// would abort the whole core (every tenant on the node). See audit M-3.
const MAX_ON_DISCONNECT_ACTIONS_PER_CLIENT: usize = 100;

/// Maximum aggregate payload bytes across a single client's registered
/// onDisconnect actions. Mirrors Firebase's documented 1 MB event-size limit and
/// bounds the memory one connection can pin. See audit M-3.
const MAX_ON_DISCONNECT_BYTES_PER_CLIENT: usize = 1024 * 1024;

/// Rough in-memory byte estimate for a JSON value. Used only to bound aggregate
/// onDisconnect payload per client — approximate is fine, it just needs to be
/// monotonic in actual size so a large value can't slip under the cap.
fn estimate_value_bytes(v: &Value) -> usize {
    match v {
        Value::Null | Value::Bool(_) => 4,
        Value::Number(_) => 8,
        Value::String(s) => s.len(),
        Value::Array(a) => 8 + a.iter().map(estimate_value_bytes).sum::<usize>(),
        Value::Object(m) => {
            8 + m
                .iter()
                .map(|(k, val)| k.len() + estimate_value_bytes(val))
                .sum::<usize>()
        }
    }
}

/// Database state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseState {
    /// Database is loading from storage
    Loading,
    /// Database is serving requests
    Serving,
}

/// Trait for sending data to a client connection.
/// Note: In Glommio, this does NOT require Send + Sync since we're single-threaded per core.
pub trait ConnectionSender {
    /// Send data to the client.
    fn send(
        &self,
        data: Bytes,
        volatile: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), SendError>> + '_>>;

    /// Try to send data without blocking.
    ///
    /// If `skip_translation` is true, the data is assumed to already be in the correct
    /// wire format for this client (e.g., Firebase format for Firebase clients).
    /// This skips the FirebaseAdapter translation but still performs chunking if needed.
    fn try_send(
        &self,
        data: Bytes,
        volatile: bool,
        skip_translation: bool,
    ) -> Result<(), SendError>;

    /// Whether this connection is a Firebase-protocol client (vs native Lark).
    /// Firebase clients receive events in Firebase wire format.
    fn is_firebase(&self) -> bool {
        false
    }

    /// Get a unique identifier for the outbox this connection sends to.
    /// Used to group messages for batch compression - messages going to the same
    /// outbox (same proxy connection) can be compressed together.
    fn outbox_id(&self) -> usize {
        0
    }

    /// Get the numeric client ID for the binary protocol.
    fn client_id(&self) -> u32 {
        0
    }

    /// Send a broadcast message with a pre-built payload.
    /// The payload should already be in wire format:
    /// `[ClientCount:4][[ClientID:4][Tag:4]...][MsgLen:4][MsgBytes...]`
    fn send_broadcast_raw(&self, _payload: &[u8], _flags: u8) -> Result<(), SendError> {
        Err(SendError::Closed)
    }

    /// Close the client connection.
    fn close(&self) {}
}

#[derive(Debug)]
pub enum SendError {
    Closed,
    BufferFull,
}

/// Client info tracked by the database.
pub struct ClientInfo {
    pub id: String,
    pub auth: Option<AuthInfo>,
    /// Cached RulesAuthInfo to avoid repeated conversion on every write.
    /// Wrapped in Arc for O(1) cloning during rules evaluation.
    pub rules_auth: Option<Arc<RulesAuthInfo>>,
    pub connection_id: String,
    pub auth_complete: bool,
    pub conn: Arc<dyn ConnectionSender>,
}

impl std::fmt::Debug for ClientInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientInfo")
            .field("id", &self.id)
            .field("auth", &self.auth)
            .field("rules_auth", &self.rules_auth.is_some())
            .field("connection_id", &self.connection_id)
            .field("auth_complete", &self.auth_complete)
            .field("conn", &"<connection>")
            .finish()
    }
}

/// Authentication info.
#[derive(Debug, Clone)]
pub struct AuthInfo {
    pub uid: String,
    pub provider: String,
    pub token: HashMap<String, Value>,
    pub is_admin: bool,
}

/// Compaction complete notification from StorageWorker to Database.
pub struct CompactionComplete {
    /// WAL entries up to this sequence are now baked into the blob.
    pub sequence: i64,
    /// The blob generation the StorageWorker applied to (from blob.generation file).
    pub blob_generation: u64,
    /// When the StorageWorker opened a new blob (generation change), it sends its
    /// CachedIO so the Database can open a BlobSession that shares the same cache.
    /// None on same-generation compaction (already sharing from the original request).
    pub cached_io: Option<CachedIO<GlommioBlobIO>>,
}

/// Message sent to the database inbox.
pub struct InboxMessage {
    pub client_id: String,
    pub message: Option<ClientMessage>,
    pub volatile: bool,
    pub disconnect: bool,

    // Client join
    pub add_client: bool,
    pub connection_id: String,
    pub conn: Option<Arc<dyn ConnectionSender>>,
    pub auth_info: Option<AuthInfo>,

    // Auth update
    pub auth_update: Option<AuthInfo>,
    pub has_auth: bool,

    // Latency tracking - start_time is always set (for metrics),
    // timestamps is only set when debug sampling (for detailed profiling)
    pub start_time: Instant,
    pub timestamps: Option<crate::metrics::MessageTimestamps>,

    /// Compaction complete notification from the StorageWorker.
    /// Contains the new blob sequence and the blob generation the worker applied to.
    /// Entries with sequence <= this value are now baked into the blob and can be
    /// dropped from pending_wal_entries. If the blob generation differs from the
    /// Database's current generation, the Database must switch to the new blob first.
    pub compaction_complete: Option<CompactionComplete>,

    /// Force-evict all promoted paths immediately (for testing).
    pub force_evict_all: bool,

    /// Rules evaluator update (hot-reload on CONFIG_PUSH).
    /// `has_evaluator_update=true` signals this is an update message;
    /// `evaluator_update=None` clears rules (fully open), `Some(e)` installs `e`.
    pub has_evaluator_update: bool,
    pub evaluator_update: Option<Evaluator>,
}

impl std::fmt::Debug for InboxMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InboxMessage")
            .field("client_id", &self.client_id)
            .field("message", &self.message)
            .field("volatile", &self.volatile)
            .field("disconnect", &self.disconnect)
            .field("add_client", &self.add_client)
            .field("connection_id", &self.connection_id)
            .field("conn", &self.conn.as_ref().map(|_| "<connection>"))
            .field("auth_info", &self.auth_info)
            .field("auth_update", &self.auth_update)
            .field("has_auth", &self.has_auth)
            .field("start_time", &self.start_time)
            .field(
                "timestamps",
                &self.timestamps.as_ref().map(|_| "<timestamps>"),
            )
            .field(
                "compaction_complete",
                &self.compaction_complete.as_ref().map(|c| c.sequence),
            )
            .field("has_evaluator_update", &self.has_evaluator_update)
            .finish()
    }
}

impl Default for InboxMessage {
    fn default() -> Self {
        Self {
            client_id: String::new(),
            message: None,
            volatile: false,
            disconnect: false,
            add_client: false,
            connection_id: String::new(),
            conn: None,
            auth_info: None,
            auth_update: None,
            has_auth: false,
            start_time: Instant::now(),
            timestamps: None,
            compaction_complete: None,
            force_evict_all: false,
            has_evaluator_update: false,
            evaluator_update: None,
        }
    }
}

/// Disconnect action for onDisconnect hooks.
#[derive(Debug, Clone)]
pub struct DisconnectAction {
    pub path: String,
    pub action: String, // "set", "update", "remove"
    pub value: Option<Value>,
}

/// A single database instance.
pub struct Database {
    pub id: String,
    pub project_id: String,
    /// Core ID this database is running on (for metrics aggregation).
    core_id: usize,
    /// Just the database portion of the ID (without project prefix), for rules evaluation.
    pure_database_id: String,
    pub tree: Arc<RwLock<Tree>>,
    pub ephemeral: bool,

    /// Inbox channel receiver (sender is held by DatabaseHandle)
    inbox: LocalReceiver<InboxMessage>,

    /// Inbox sender for creating handles (wrapped in Rc for cloning)
    inbox_sender: Rc<LocalSender<InboxMessage>>,

    /// Client management
    clients: HashMap<String, ClientInfo>,

    /// View manager for subscriptions and volatile batching
    view_manager: ViewManager,

    /// Disconnect hooks: client_id -> actions
    on_disconnect: HashMap<String, Vec<DisconnectAction>>,

    /// State
    state: DatabaseState,
    last_activity: Instant,
    /// Set to true when the Database encounters an unrecoverable error
    /// (e.g., failed to open a new blob after generation change).
    /// The main loop checks this and exits cleanly.
    fatal_error: bool,

    /// Volatile path patterns (from rules)
    volatile_paths: Vec<String>,

    /// Security rules evaluator
    evaluator: Option<Rc<Evaluator>>,

    /// Data directory for persistence on NVMe (None for ephemeral databases)
    data_dir: Option<PathBuf>,

    /// WAL writer for durability (None for ephemeral databases).
    /// Uses Glommio async I/O to avoid blocking other databases on the same core.
    wal_writer: Option<WalWriter>,

    /// Whether WAL has dirty (unflushed) data
    wal_dirty: bool,

    /// Whether WAL I/O has failed (disk full, etc.)
    /// When true, all writes are NACKed until recovery succeeds.
    /// Recovery is attempted every ~10s during housekeeping.
    wal_failed: bool,

    /// WAL stats: entries written since last sync
    wal_pending_entries: usize,

    /// WAL stats: bytes written since last sync
    wal_pending_bytes: u64,

    /// Blob session for reading from blob storage (None for ephemeral databases).
    /// Caches header + dictionary for fast navigation/reads.
    /// Owns the CachedIO handle — access via `session.io()`.
    blob_session: Option<BlobSession<CachedIO<GlommioBlobIO>>>,

    /// Current blob generation number (from blob.generation file).
    /// Used by housekeeping to detect when an external full compaction (lark-compact)
    /// has created a higher-numbered blob file.
    blob_generation: u64,

    /// In-memory WAL entries not yet compacted into the blob.
    /// On promotion (Sentinel → real data), we replay these entries on top of blob data.
    /// Trimmed when the storage worker compacts WAL into the blob and updates the sequence file.
    pending_wal_entries: Vec<WalEntry>,

    /// Path index over pending_wal_entries for O(depth+k) promotion lookups.
    wal_index: WalIndex,

    /// WAL sequence number through which the blob is up to date.
    /// Read from {data_dir}/sequence on startup. Default 0 if file missing.
    /// Used as min_sequence for the WAL writer so it starts after compacted entries.
    blob_sequence: i64,

    /// Tracks promoted paths (blob reads) and when they were last promoted.
    /// Used for eviction: paths idle for >30s are evicted back to Sentinel.
    /// Only populated for blob-backed databases.
    promoted_paths: HashMap<String, Instant>,

    /// Tracks all paths where a Sentinel currently exists in the tree.
    /// Used by `promote_path_deep` to avoid expensive recursive `contains_sentinel()`
    /// tree walks — a BTreeSet range query replaces the O(n) tree scan with O(log n).
    /// Maintained by: set_lazy (add ancestors), eviction (add), promotion (remove).
    sentinel_paths: BTreeSet<String>,

    /// Project secret for token validation (optional - if None, uses emulator mode)
    project_secret: Option<String>,

    /// Write deduplication - tracks connection_id -> set of request_ids (in insertion order)
    /// Used to prevent duplicate writes on reconnect retry
    /// Keeps last MAX_WRITES_PER_CONNECTION entries per connection
    processed_writes: HashMap<String, IndexSet<String>>,

    /// Nacked writes - tracks connection_id -> set of request_ids (in insertion order)
    /// Used to detect tainted writes (writes that depend on a nacked write)
    /// Keeps last MAX_WRITES_PER_CONNECTION entries per connection
    nacked_writes: HashMap<String, IndexSet<String>>,

    /// Template mode - if true, this database loads from a shared template
    /// and should skip compaction/segmentation queues
    template_mode: bool,

    /// Template directory for loading (if using template mode).
    /// Loading happens at the start of run() to allow async segmentation lock wait.
    pending_template_dir: Option<PathBuf>,

    /// Whether disk loading is pending (will be done at start of run())
    pending_disk_load: bool,

    /// Whether startup found many uncompacted WAL files that should be compacted.
    needs_startup_compaction: bool,

    /// Channel to notify the per-core storage worker when a WAL file is rotated.
    /// None for ephemeral databases.
    compaction_tx: Option<Rc<LocalSender<StorageWorkerMessage>>>,

    /// Per-database metrics (writes, reads, CCU, latency, etc.)
    pub metrics: crate::metrics::DatabaseMetrics,

    /// Optional sink for emitted metrics JSON, forwarded to a dedicated
    /// shipper thread that POSTs them to the coordinator's `/internal/metrics`
    /// (enabled by `LARK_METRICS_PUSH`). `None` means metrics are only written
    /// to stdout (the default; e.g. when an external log shipper scrapes them).
    /// `try_send` is non-blocking and drops on a full channel, so a slow or
    /// dead shipper can never stall this core.
    metrics_tx: Option<std::sync::mpsc::SyncSender<String>>,

    /// Promotion stats for the current metrics interval (reset on emit).
    promotion_stats: PromotionStats,
}

/// Handle to interact with a database.
#[derive(Clone)]
pub struct DatabaseHandle {
    pub id: String,
    /// Inbox sender - wrapped in Rc for cloning (LocalSender is !Clone)
    pub inbox: Rc<LocalSender<InboxMessage>>,
}

impl DatabaseHandle {
    /// Send a message to the database (non-blocking).
    /// Returns true if sent, false if channel is full (should be rare with proper sizing).
    pub fn send(&self, msg: InboxMessage) -> bool {
        self.inbox.try_send(msg).is_ok()
    }

    /// Add a client to the database.
    /// Non-blocking.
    pub fn add_client(
        &self,
        client_id: String,
        auth: Option<AuthInfo>,
        connection_id: String,
        conn: Arc<dyn ConnectionSender>,
    ) {
        let msg = InboxMessage {
            client_id,
            add_client: true,
            connection_id,
            conn: Some(conn),
            auth_info: auth,
            ..Default::default()
        };
        let _ = self.inbox.try_send(msg);
    }

    /// Send a protocol message from a client.
    pub fn send_message(&self, client_id: String, message: ClientMessage) {
        self.send_message_with_timestamps(client_id, message, None);
    }

    /// Send a protocol message from a client with latency tracking timestamps.
    /// Non-blocking.
    pub fn send_message_with_timestamps(
        &self,
        client_id: String,
        message: ClientMessage,
        mut timestamps: Option<crate::metrics::MessageTimestamps>,
    ) {
        // Stamp just before pushing to inbox
        if let Some(ref mut ts) = timestamps {
            ts.stamp_db_inbox_push();
        }

        let volatile = message.volatile.unwrap_or(false);
        let msg = InboxMessage {
            client_id,
            message: Some(message),
            volatile,
            timestamps,
            ..Default::default()
        };
        let _ = self.inbox.try_send(msg);
    }

    /// Notify that a client disconnected.
    /// Non-blocking.
    pub fn client_disconnected(&self, client_id: String) {
        let msg = InboxMessage {
            client_id,
            disconnect: true,
            ..Default::default()
        };
        let _ = self.inbox.try_send(msg);
    }

    /// Force-evict all promoted paths (for testing eviction/re-promotion).
    pub fn force_evict_all(&self) {
        let msg = InboxMessage {
            force_evict_all: true,
            ..Default::default()
        };
        let _ = self.inbox.try_send(msg);
    }

    /// Update a client's authentication state.
    /// Non-blocking.
    pub fn update_client_auth(&self, client_id: String, auth: Option<AuthInfo>) {
        let msg = InboxMessage {
            client_id,
            auth_update: auth,
            has_auth: true,
            ..Default::default()
        };
        let _ = self.inbox.try_send(msg);
    }

    /// Hot-reload the rules evaluator (called on CONFIG_PUSH).
    /// `None` clears rules (fully open); `Some(e)` installs the new evaluator.
    /// Non-blocking.
    pub fn update_evaluator(&self, evaluator: Option<Evaluator>) {
        let msg = InboxMessage {
            has_evaluator_update: true,
            evaluator_update: evaluator,
            ..Default::default()
        };
        let _ = self.inbox.try_send(msg);
    }
}

/// Default inbox channel size - large enough to buffer burst traffic
const INBOX_CHANNEL_SIZE: usize = 16384;

/// Maximum number of write request IDs to track per connection for deduplication.
/// After this limit, oldest entries are evicted.
const MAX_WRITES_PER_CONNECTION: usize = 500;

impl Database {
    /// Extract just the database portion from a combined "project/database" ID.
    fn extract_pure_database_id(id: &str) -> String {
        match id.find('/') {
            Some(idx) => id[idx + 1..].to_string(),
            None => id.to_string(),
        }
    }

    /// Create a new database.
    pub fn new(id: String, project_id: String, ephemeral: bool) -> Self {
        let (inbox_sender, inbox) = local_channel::new_bounded(INBOX_CHANNEL_SIZE);

        let tree = Arc::new(RwLock::new(Tree::new()));
        let pure_database_id = Self::extract_pure_database_id(&id);

        Self {
            id,
            project_id,
            core_id: 0, // Set via set_core_id() after creation
            pure_database_id,
            tree,
            ephemeral,
            inbox,
            inbox_sender: Rc::new(inbox_sender),
            clients: HashMap::new(),
            view_manager: ViewManager::new(),
            on_disconnect: HashMap::new(),
            state: DatabaseState::Loading,
            last_activity: Instant::now(),
            fatal_error: false,
            volatile_paths: Vec::new(),
            evaluator: None,
            data_dir: None,
            wal_writer: None,
            wal_dirty: false,
            wal_failed: false,
            wal_pending_entries: 0,
            wal_pending_bytes: 0,
            blob_session: None,
            blob_generation: 0,

            pending_wal_entries: Vec::new(),
            wal_index: WalIndex::new(),
            blob_sequence: 0,
            promoted_paths: HashMap::new(),
            sentinel_paths: BTreeSet::new(),
            project_secret: None,
            processed_writes: HashMap::new(),
            nacked_writes: HashMap::new(),
            template_mode: false,
            pending_template_dir: None,
            pending_disk_load: false,
            needs_startup_compaction: false,
            compaction_tx: None,
            metrics: crate::metrics::DatabaseMetrics::new(),
            metrics_tx: None,
            promotion_stats: PromotionStats::new(),
        }
    }

    /// Create a new database with persistence.
    ///
    /// Note: The WAL writer is initialized asynchronously in `run()` after disk loading,
    /// since it requires async I/O to avoid blocking other databases on the core.
    pub fn new_with_persistence(id: String, project_id: String, data_dir: PathBuf) -> Self {
        let (inbox_sender, inbox) = local_channel::new_bounded(INBOX_CHANNEL_SIZE);

        let tree = Arc::new(RwLock::new(Tree::new()));
        let pure_database_id = Self::extract_pure_database_id(&id);

        Self {
            id,
            project_id,
            core_id: 0, // Set via set_core_id() after creation
            pure_database_id,
            tree,
            ephemeral: false,
            inbox,
            inbox_sender: Rc::new(inbox_sender),
            clients: HashMap::new(),
            view_manager: ViewManager::new(),
            on_disconnect: HashMap::new(),
            state: DatabaseState::Loading,
            last_activity: Instant::now(),
            fatal_error: false,
            volatile_paths: Vec::new(),
            evaluator: None,
            data_dir: Some(data_dir),
            wal_writer: None, // Initialized async in run() after disk loading
            wal_dirty: false,
            wal_failed: false,
            wal_pending_entries: 0,
            wal_pending_bytes: 0,
            blob_session: None, // Initialized in load_from_disk()
            blob_generation: 0, // Initialized in load_from_disk()
            pending_wal_entries: Vec::new(),
            wal_index: WalIndex::new(),
            blob_sequence: 0,
            promoted_paths: HashMap::new(),
            sentinel_paths: BTreeSet::new(),
            project_secret: None,
            processed_writes: HashMap::new(),
            nacked_writes: HashMap::new(),
            template_mode: false,
            pending_template_dir: None,
            pending_disk_load: true, // Persistent databases need to load at start of run()
            needs_startup_compaction: false, // Set by load_wal_entries() if many WAL files
            compaction_tx: None,     // Set via set_compaction_tx() after creation
            metrics: crate::metrics::DatabaseMetrics::new(),
            metrics_tx: None, // Set via set_metrics_tx() after creation
            promotion_stats: PromotionStats::new(),
        }
    }

    /// Set the core ID this database is running on.
    pub fn set_core_id(&mut self, core_id: usize) {
        self.core_id = core_id;
    }

    /// Set the compaction channel sender for notifying the storage worker on WAL rotation.
    pub fn set_compaction_tx(&mut self, tx: Rc<LocalSender<StorageWorkerMessage>>) {
        self.compaction_tx = Some(tx);
    }

    /// Set the metrics sink: a non-blocking channel to the shipper thread that
    /// POSTs emitted metrics to the coordinator. Only set when `LARK_METRICS_PUSH`
    /// is enabled; otherwise metrics are stdout-only.
    pub fn set_metrics_tx(&mut self, tx: std::sync::mpsc::SyncSender<String>) {
        self.metrics_tx = Some(tx);
    }

    /// Set the project secret for token validation.
    pub fn set_project_secret(&mut self, secret: &str) {
        self.project_secret = Some(secret.to_string());
    }

    /// Set template mode - databases in template mode skip compaction/segmentation queues.
    pub fn set_template_mode(&mut self, template_mode: bool) {
        self.template_mode = template_mode;
    }

    /// Check if this database is in template mode.
    pub fn is_template_mode(&self) -> bool {
        self.template_mode
    }

    /// Returns true if this database is backed by blob storage.
    pub fn is_blob_backed(&self) -> bool {
        self.blob_session.is_some()
    }

    /// Set the template directory for loading.
    /// If set, the database will load from this template at the start of run().
    pub fn set_pending_template_dir(&mut self, template_dir: PathBuf) {
        self.pending_template_dir = Some(template_dir);
    }

    /// Initialize the async WAL writer.
    ///
    /// This is called after disk loading in `run()` to get the correct min_sequence
    /// from the manifest. Uses runtime-adaptive async I/O.
    ///
    /// Returns true if initialization succeeded (or was skipped for ephemeral DBs),
    /// false if initialization failed and the database should not serve requests.
    pub async fn init_wal_writer(&mut self) -> bool {
        // Skip for ephemeral databases
        if self.data_dir.is_none() {
            return true; // Ephemeral - no WAL needed
        }

        let wal_dir = match self.wal_dir() {
            Some(dir) => dir,
            None => return true,
        };

        // Use blob_sequence so the WAL writer starts after already-compacted entries.
        let min_sequence = self.blob_sequence;

        match WalWriter::with_min_sequence(&wal_dir, min_sequence).await {
            Ok(writer) => {
                debug!(
                    "[Persistence] {}: Initialized async WAL writer (sequence={})",
                    self.id,
                    writer.sequence()
                );
                self.wal_writer = Some(writer);
                true
            }
            Err(e) => {
                error!(
                    "[STORAGE INTEGRITY] {}: Failed to initialize WAL writer: {}",
                    self.id, e
                );
                false
            }
        }
    }

    /// Read the WAL sequence file from `{data_dir}/sequence`.
    /// Returns the sequence number through which the blob is up to date.
    /// Returns 0 if the file doesn't exist or can't be parsed.
    async fn read_sequence_file(data_dir: &std::path::Path) -> i64 {
        let path = data_dir.join("sequence");
        match read_file_async(&path).await {
            Ok(bytes) => {
                let s = String::from_utf8_lossy(&bytes);
                s.trim().parse::<i64>().unwrap_or(0)
            }
            Err(_) => 0, // File doesn't exist or can't be read
        }
    }

    /// Return the WAL directory for this database.
    fn wal_dir(&self) -> Option<PathBuf> {
        self.data_dir.as_ref().map(|d| d.join("wal"))
    }

    /// Load database state from BlobSession (async).
    /// With the blob model, the Tree starts empty and data is loaded lazily
    /// via navigate() and read_subtree() when accessed.
    async fn load_from_disk(&mut self) -> std::io::Result<()> {
        let data_dir = match &self.data_dir {
            Some(dir) => dir.clone(),
            None => return Ok(()), // Ephemeral — no blob needed
        };

        // Use fixed blob filename: blob.lark
        // If none exists, create it so the storage worker always has one to apply to.
        let bp = blob_path(&data_dir);
        if !bp.exists() {
            std::fs::create_dir_all(&data_dir)
                .map_err(|e| std::io::Error::other(format!("creating data dir: {}", e)))?;

            let io = CachedIO::new(GlommioBlobIO::create(&bp).await?);
            let session = BlobSession::init(io)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            lark_blob::BlobIO::sync(session.io()).await?;

            self.blob_session = Some(session);
            self.blob_generation = 0;
            *self.tree.write().unwrap() = Tree::new_sentinel();
            self.sentinel_paths.insert("/".to_string());

            tracing::debug!("Database {} created blank blob at {:?}", self.id, bp);

            // Continue to WAL replay below
            return self.load_wal_entries().await;
        }

        let blob_gen = read_blob_generation(&data_dir);

        // Open existing blob file via Glommio io_uring — reads yield to scheduler.
        let raw_io = GlommioBlobIO::open(&bp).await?;
        let io = CachedIO::new(raw_io);

        // BlobSession::open reads just the header + dictionary (small, fixed-size)
        let session = BlobSession::open(io)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        self.blob_session = Some(session);
        self.blob_generation = blob_gen;

        // Initialize tree with Sentinel root — data will be promoted on demand.
        *self.tree.write().unwrap() = Tree::new_sentinel();
        self.sentinel_paths.insert("/".to_string());

        tracing::debug!(
            "Database {} opened BlobSession from {:?} (gen {})",
            self.id,
            bp,
            blob_gen
        );

        self.load_wal_entries().await
    }

    /// Load uncompacted WAL entries from disk into pending_wal_entries.
    ///
    /// WAL entries are NOT replayed into the tree here. They are stored in
    /// `pending_wal_entries` and replayed on top of blob data when a path is
    /// promoted via `promote_path()`. This ensures correct ordering — blob data
    /// is read first, then all WAL entries (SET, UPDATE, DELETE) are applied.
    async fn load_wal_entries(&mut self) -> std::io::Result<()> {
        let data_dir = match &self.data_dir {
            Some(dir) => dir.clone(),
            None => return Ok(()),
        };

        // Read sequence file from local data_dir — tells us which WAL entries
        // are already compacted into the blob.
        self.blob_sequence = Self::read_sequence_file(&data_dir).await;

        // Load uncompacted WAL entries (sequence > blob_sequence).
        let wal_dir = match self.wal_dir() {
            Some(dir) => dir,
            None => return Ok(()),
        };
        if wal_dir.exists() {
            let reader = WalReader::new(&wal_dir);
            match reader.read_since(self.blob_sequence + 1).await {
                Ok(entries) => {
                    if !entries.is_empty() {
                        tracing::debug!(
                            "Database {} loaded {} WAL entries (after sequence {})",
                            self.id,
                            entries.len(),
                            self.blob_sequence
                        );

                        self.pending_wal_entries = entries;
                        self.wal_index.rebuild(&self.pending_wal_entries);
                    }

                    // If many small WAL files have accumulated, request compaction
                    // on startup to consolidate them into the blob.
                    let file_count = reader.file_count_since(self.blob_sequence + 1).await;
                    if file_count > 10 {
                        tracing::info!(
                            "Database {} has {} uncompacted WAL files, requesting startup compaction",
                            self.id,
                            file_count
                        );
                        self.needs_startup_compaction = true;
                    }
                }
                Err(e) => {
                    tracing::error!("Database {} failed to read WAL entries: {}", self.id, e);
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    // =========================================================================
    /// Handle a compaction complete notification from the StorageWorker.
    ///
    /// Trims pending_wal_entries to drop entries already baked into the blob,
    /// and updates blob_sequence so future promotions skip those entries.
    async fn handle_compaction_complete(&mut self, cc: CompactionComplete) {
        let new_sequence = cc.sequence;

        // If the StorageWorker is on a different blob generation (e.g., after an
        // external full compaction by lark-compact), we must switch to that blob
        // BEFORE trimming WAL entries. Otherwise we'd lose WAL entries that haven't
        // been applied to our (old) blob yet.
        if cc.blob_generation != self.blob_generation {
            let old_gen = self.blob_generation;

            // Use the StorageWorker's CachedIO if provided (shares its cache),
            // otherwise fall back to opening fresh.
            let io = if let Some(cached_io) = cc.cached_io {
                match cached_io.clone_for_reading().await {
                    Ok(io) => io,
                    Err(e) => {
                        error!(
                            "[Compaction] {}: Failed to clone CachedIO from StorageWorker (gen {}): {} — falling back to fresh open",
                            self.id, cc.blob_generation, e
                        );
                        match self.open_fresh_cached_io(cc.blob_generation).await {
                            Some(io) => io,
                            None => return,
                        }
                    }
                }
            } else {
                match self.open_fresh_cached_io(cc.blob_generation).await {
                    Some(io) => io,
                    None => return,
                }
            };

            match BlobSession::open(io).await {
                Ok(session) => {
                    self.blob_session = Some(session);
                    self.blob_generation = cc.blob_generation;
                    info!(
                        "[Compaction] {}: Switched to blob generation {} (was {})",
                        self.id, cc.blob_generation, old_gen
                    );
                }
                Err(e) => {
                    error!(
                        "[Compaction] {}: Failed to open BlobSession on blob.lark (gen {}): {} — shutting down",
                        self.id, cc.blob_generation, e
                    );
                    self.fatal_error = true;
                    return;
                }
            }
        } else {
            // Same blob generation with shared CachedIO. Container-header writes
            // by the StorageWorker are visible to our reads via the shared
            // Rc-backed `regions` map: `pwrite_deferred` patches the cached
            // region in place, and our `nav_cache` reads see the patched bytes
            // — that's the whole point of the shared cache.
            //
            // The blob header at bytes [0..64] is the exception. It's never
            // populated into CachedIO's `regions` (cache_region is only called
            // for container headers, not the file header), so when
            // `forward_via_parent_index` relocates the root container — writing
            // the new offset to bytes [16..24] via pwrite_deferred — the write
            // bypasses the cache and goes straight to disk. The StorageWorker's
            // `BlobSession.header.root_offset` field gets updated, but ours is
            // a separate copy from when we opened the session, and it's now
            // stale: subsequent reads navigate from the OLD root offset and
            // return whatever lived there before, or PathNotFound, or whatever
            // the free list reused that range for. That was the chaos-monkey
            // bug we hunted down.
            //
            // Fix: re-read the header from disk on every CompactionComplete.
            // Cost is two small preads (header + dictionary) per compaction
            // (so per ~5MB of WAL); the kernel page cache will almost always
            // have the bytes warm because the StorageWorker just wrote them.
            //
            // If this ever shows up in a profile, the next step is to cache
            // [0..HEADER_SIZE] in CachedIO's regions on session open — then
            // the StorageWorker's pwrite_deferred would patch it in place and
            // our pread would be a cache hit. We deferred that because (a) the
            // current cost is negligible at compaction cadence and (b) the
            // BlobSession.header *struct* would still need refreshing
            // separately — the `regions` map holds raw bytes, not the parsed
            // header. The fully-shared alternative (Rc<RefCell<BlobHeader>>
            // between sessions) eliminates the read entirely but is invasive
            // in lark-blob.
            if let Some(session) = self.blob_session.as_mut()
                && let Err(e) = session.refresh().await
            {
                error!(
                    "[Compaction] {}: BlobSession::refresh failed after incremental compaction: {} — reads may return stale data",
                    self.id, e
                );
            }
        }

        let before = self.pending_wal_entries.len();
        self.pending_wal_entries
            .retain(|e| e.sequence > new_sequence);
        let trimmed = before - self.pending_wal_entries.len();
        self.blob_sequence = new_sequence;
        if trimmed > 0 {
            self.wal_index.rebuild(&self.pending_wal_entries);
            debug!(
                "[Compaction] {}: Trimmed {} WAL entries (blob now at seq {}), {} remaining",
                self.id,
                trimmed,
                new_sequence,
                self.pending_wal_entries.len()
            );
        }
    }

    /// Open a fresh (independent) CachedIO on the current blob.lark.
    /// Returns None and sets fatal_error on failure.
    async fn open_fresh_cached_io(&mut self, blob_gen: u64) -> Option<CachedIO<GlommioBlobIO>> {
        let data_dir = self.data_dir.as_ref()?;
        let bp = blob_path(data_dir);
        match GlommioBlobIO::open(&bp).await {
            Ok(raw_io) => Some(CachedIO::new(raw_io)),
            Err(e) => {
                error!(
                    "[Compaction] {}: Failed to open blob.lark (gen {}): {} — shutting down",
                    self.id, blob_gen, e
                );
                self.fatal_error = true;
                None
            }
        }
    }

    // =========================================================================
    // Blob Data Loading
    // =========================================================================

    /// Ensure the data at `path` is materialized (not a Sentinel or unknown).
    ///
    /// If the node at `path` is already real data, this is a no-op.
    /// Otherwise, reads from blob, replays in-memory WAL entries, and inserts
    /// the result into the tree.
    ///
    /// Returns Ok(true) if data was loaded, Ok(false) if no loading needed.
    /// Returns Err if blob I/O fails.
    async fn promote_path(&mut self, path: &str) -> Result<bool, String> {
        // If not blob-backed, nothing to promote
        if self.blob_session.is_none() {
            return Ok(false);
        }

        // Check if this node already has real (non-Sentinel) data in the tree.
        // If so, no promotion needed. (This is a shallow check — only the top node.
        // For full-subtree guarantees, use promote_path_deep.)
        {
            let tree = self.tree.read().unwrap();
            let path_obj = Path::parse(path);
            match tree.get(&path_obj) {
                Some(node) if !node.is_sentinel() => {
                    drop(tree);
                    if let Some(ts) = self.promoted_paths.get_mut(&normalize_path_key(path)) {
                        *ts = Instant::now();
                    }
                    return Ok(false);
                }
                None => {
                    // Node is absent (not even a Sentinel). Check if the parent is
                    // a loaded container (Object). If so, the parent has complete
                    // knowledge of its children — an absent child definitively does
                    // not exist. No blob read needed.
                    //
                    // Why this is safe:
                    // - A non-Sentinel parent was either promoted (all children loaded
                    //   from blob + WAL) or written via SET (full replacement).
                    // - If a child were evicted, it would be a Sentinel, not absent.
                    // - If a child were deleted, it is correctly absent.
                    //
                    // IMPORTANT: only write the Null marker if the parent is an Object.
                    // If the parent is a primitive (Null/Bool/Number/String) or Array,
                    // the child definitively doesn't exist, but `set_arc_uncleaned_lazy`
                    // would clobber the primitive into a Sentinel container (see
                    // ArcValue::set_path_mut_sentinel's primitive branch), corrupting
                    // the tree. Skip the marker write in that case — the next read
                    // will do the same cheap check and arrive at the same answer.
                    if let Some(parent) = path_obj.parent()
                        && let Some(parent_node) = tree.get(&parent)
                    {
                        if parent_node.is_object() {
                            // Drop read lock, insert Null to mark "we checked"
                            drop(tree);
                            let mut tree = self.tree.write().unwrap();
                            tree.set_arc_uncleaned_lazy(&path_obj, ArcValue::Null);
                            return Ok(false);
                        }
                        if !parent_node.is_sentinel() {
                            // Parent is a primitive or Array — child can't exist.
                            // No marker write (would corrupt parent). No blob read.
                            return Ok(false);
                        }
                    }
                    // Parent is Sentinel or absent — need to load from blob
                }
                _ => {
                    // Sentinel — need to load from blob
                }
            }
        }

        self.promote_path_shallow(path).await
    }

    /// Deep promotion: ensures the entire subtree at `path` is Sentinel-free.
    ///
    /// Used by subscribe(), once(), and query view recompute — operations that
    /// Check the blob's on-disk subtree size at a path before promoting.
    /// Returns true if the subtree is too large to serve (exceeds the response size limit).
    /// Uses blob `navigate()` which only reads headers — no data is loaded.
    ///
    /// The blob subtree_size is the binary on-disk size, which is smaller than the
    /// JSON serialization size. We use a 1.5x multiplier on MAX_RESPONSE_SIZE as the
    /// threshold: if the raw blob bytes alone exceed that, the JSON response will
    /// certainly exceed the limit.
    async fn blob_subtree_exceeds_limit(&self, path: &str) -> bool {
        let session = match &self.blob_session {
            Some(s) => s,
            None => return false, // Ephemeral DB, no blob to check
        };

        let path_obj = Path::parse(path);
        let segments: Vec<&str> = path_obj.segments().iter().map(|s| s.as_ref()).collect();
        let blob_path = if path == "/" { vec![] } else { segments };

        match session.navigate(&blob_path).await {
            Ok(location) => {
                let limit = crate::protocol::MAX_RESPONSE_SIZE as u64 * 3 / 2;
                if location.subtree_size > limit {
                    warn!(
                        "[Size Check] {}: blob subtree at {} is {} bytes (limit {}), rejecting before promotion",
                        self.id, path, location.subtree_size, limit
                    );
                    return true;
                }
                false
            }
            Err(BlobError::PathNotFound(_)) => false, // Doesn't exist in blob, can't be too large
            Err(_) => false, // Navigate failed, let promotion handle the error
        }
    }

    /// need to serialize or iterate the full subtree. Unlike `promote_path()`,
    /// this checks for Sentinel descendants (not just the top node) and does a
    /// full blob read + WAL replay if any are found.
    async fn promote_path_deep(&mut self, path: &str) -> Result<bool, String> {
        // If not blob-backed, nothing to promote
        if self.blob_session.is_none() {
            return Ok(false);
        }

        // Check if any Sentinel exists at or below this path — O(log n) BTreeSet
        // range query instead of the old O(tree_size) recursive contains_sentinel() walk.
        let needs_promotion = if self.has_sentinel_at_or_below(path) {
            true
        } else {
            // No sentinels in this subtree. But the node might be absent —
            // check if the parent is loaded so we can definitively say "doesn't exist."
            let tree = self.tree.read().unwrap();
            let path_obj = Path::parse(path);
            match tree.get(&path_obj) {
                Some(node) if !node.is_sentinel() => false, // Node exists and has no sentinels below — fully loaded
                Some(_) => {
                    // I3 invariant violation: the tree has a Sentinel at this
                    // path but `sentinel_paths` doesn't track it. We promote
                    // defensively so the read returns correct data, but warn
                    // loudly — some mutation site is creating a Sentinel
                    // without keeping `sentinel_paths` in sync. Each repeated
                    // hit on the same path means the same upstream bug, so
                    // include enough context to chase it.
                    drop(tree);
                    warn!(
                        db = %self.id,
                        path = %path,
                        "I3 invariant violation: untracked Sentinel at {} (tree has Sentinel, \
                         sentinel_paths does not). Promoting defensively. Find the mutation site \
                         that created this Sentinel without calling track_sentinels_after_write \
                         or otherwise updating sentinel_paths.",
                        path
                    );
                    true
                }
                None => {
                    // Same parent-container check as in promote_path: only write the
                    // Null marker when the parent is an Object. A primitive/Array
                    // parent means the child definitively doesn't exist, but writing
                    // Null through `set_path_mut_sentinel` would clobber the parent
                    // into a Sentinel — see comment in promote_path for the full
                    // story.
                    if let Some(parent) = path_obj.parent()
                        && let Some(parent_node) = tree.get(&parent)
                    {
                        if parent_node.is_object() {
                            drop(tree);
                            let mut tree = self.tree.write().unwrap();
                            tree.set_arc_uncleaned_lazy(&path_obj, ArcValue::Null);
                            return Ok(false);
                        }
                        if !parent_node.is_sentinel() {
                            // Primitive/Array parent — child can't exist. Skip.
                            return Ok(false);
                        }
                    }
                    // Parent is Sentinel or absent — need to load
                    true
                }
            }
        };

        if !needs_promotion {
            if let Some(ts) = self.promoted_paths.get_mut(&normalize_path_key(path)) {
                *ts = Instant::now();
            }
            return Ok(false);
        }

        // Force a full promotion: read from blob + replay WAL, replacing the subtree.
        // This is the same logic as promote_path but without the early bail-out.
        self.promote_path_unchecked(path).await
    }

    /// Unconditional promotion: always reads from blob + replays WAL at the given path.
    /// Used by `promote_path_deep` when Sentinels are detected, and by `promote_path`
    /// when the top-level node is Sentinel.
    async fn promote_path_unchecked(&mut self, path: &str) -> Result<bool, String> {
        let promote_start = Instant::now();

        let session = match &self.blob_session {
            Some(s) => s,
            _ => return Ok(false),
        };

        // Step 1: Read subtree from blob
        let read_start = Instant::now();
        let _ = session.io().take_read_stats(); // reset counters
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        let blob_value = match session.read_subtree(&segments).await {
            Ok(value) => value,
            Err(BlobError::PathNotFound(_)) => {
                // Path doesn't exist in blob — start with Null
                ArcValue::Null
            }
            Err(e) => return Err(format!("Blob read failed at {}: {}", path, e)),
        };
        let read_elapsed = read_start.elapsed();
        let io_stats = session.io().take_read_stats();

        // Step 2: Build a temporary tree from the blob data and replay matching WAL entries
        let path_obj = Path::parse(path);
        let mut temp_tree = Tree::new();
        temp_tree.set_arc_uncleaned(&path_obj, blob_value);

        // Use the indexed lookup instead of scanning all entries
        let matching_indices = self.wal_index.find_affecting(path);
        for &idx in &matching_indices {
            let entry = &self.pending_wal_entries[idx];
            let entry_path = Path::parse(&entry.path);
            match entry.op {
                WalOp::Set => {
                    // `value: None` means SET-to-null (serde collapses
                    // `{"v":null}` into `None`). Modern writers canonicalize
                    // this to `WalOp::Delete` in `wal_write_set`, but old WAL
                    // entries on disk may still have the SET-with-null form;
                    // map None → Null so `tree.set` cleans it to a delete.
                    let value = entry.value.clone().unwrap_or(Value::Null);
                    temp_tree.set(&entry_path, value);
                }
                WalOp::Update => {
                    if let Some(Value::Object(ref updates)) = entry.value {
                        temp_tree.update(&entry_path, updates);
                    }
                }
                WalOp::Delete => {
                    temp_tree.remove(&entry_path);
                }
            }
        }

        // Step 3: Extract the promoted value and set it in the real tree.
        let promoted_value = temp_tree.get_arc(&path_obj).unwrap_or(ArcValue::Null);

        {
            let mut tree = self.tree.write().unwrap();
            tree.set_arc_uncleaned_lazy(&path_obj, promoted_value);
        }

        // set_arc_uncleaned_lazy may have created Sentinel intermediates along the
        // path (via set_path_mut_sentinel). Track those in sentinel_paths.
        self.track_sentinels_after_write(path);

        // Track this promotion for eviction timing
        self.promoted_paths
            .insert(normalize_path_key(path), Instant::now());

        // Remove sentinel tracking for this path and all descendants —
        // the subtree has been fully replaced with real data from blob + WAL.
        self.remove_sentinel_paths_below(path);

        // Record promotion stats
        let total_elapsed = promote_start.elapsed();
        self.promotion_stats
            .record(total_elapsed, read_elapsed, io_stats);

        Ok(true)
    }

    /// Shallow promotion: reads only immediate children from blob, not the full subtree.
    ///
    /// For primitive values at `path`, inserts the value directly.
    /// For containers, inserts primitive children as real values and container
    /// children as Sentinels (to be loaded on demand later).
    ///
    /// WAL entries are replayed on top to ensure the shallow view is up-to-date.
    /// This is much cheaper than `promote_path_unchecked` because it avoids
    /// allocating the full BTreeMap hierarchy for deep subtrees.
    async fn promote_path_shallow(&mut self, path: &str) -> Result<bool, String> {
        let promote_start = Instant::now();

        let session = match &self.blob_session {
            Some(s) => s,
            _ => return Ok(false),
        };

        // Step 1: Shallow read from blob
        let read_start = Instant::now();
        let _ = session.io().take_read_stats(); // reset counters
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        let shallow_result = match session.read_shallow(&segments).await {
            Ok(value) => value,
            Err(BlobError::PathNotFound(_)) => {
                // Path doesn't exist in blob. Delegate to `promote_path_unchecked`,
                // which seeds an empty subtree with `Null` and replays any
                // affecting WAL entries on top. That handles both cases
                // correctly:
                //   - WAL has writes → the in-memory data those writes
                //     produced is preserved (a bare Null marker would
                //     clobber it via `set_path_mut_sentinel`'s leaf
                //     assignment, replacing the WAL-built Sentinel
                //     container with Null).
                //   - WAL has nothing → temp_tree stays Null, and the
                //     install is `tree.set_arc_uncleaned_lazy(path, Null)`
                //     — same effect as the old marker write.
                //
                // Primitive-parent guard: `set_path_mut_sentinel` walks
                // through any non-Object/Sentinel ancestor by replacing it
                // with a fresh Sentinel container. If a concurrent SET has
                // turned an ancestor into a primitive between this path's
                // tree-state check and now, walking through it (whether
                // from `promote_path_unchecked` or a bare marker write)
                // would silently destroy the primitive's value. Walk up
                // first; if any ancestor is primitive/Array, skip the
                // promotion entirely. The next read re-evaluates and
                // arrives at the right answer via the WAL/blob.
                //
                // Regression tests:
                //   - test_blob_update_create_then_update_player_permissions
                //   - test_promote_path_shallow_pathnotfound_preserves_primitive_parent
                let path_obj = Path::parse(path);
                let safe_to_write = match path_obj.parent() {
                    None => false, // root — would replace the whole tree
                    Some(start) => {
                        let tree = self.tree.read().unwrap();
                        let mut current = Some(start);
                        let mut clobber_risk = false;
                        while let Some(p) = current {
                            let node = if p.is_root() {
                                Some(tree.root())
                            } else {
                                tree.get(&p)
                            };
                            if let Some(n) = node {
                                if !(n.is_object() || n.is_sentinel()) {
                                    clobber_risk = true;
                                }
                                break;
                            }
                            current = p.parent();
                        }
                        !clobber_risk
                    }
                };

                if !safe_to_write {
                    // Same as the pre-delegation behavior: skip, let the
                    // next read re-evaluate. The primitive parent edge
                    // case is rare enough that the rules-eval retry loop
                    // can spend a slot here without exhausting.
                    self.promoted_paths
                        .insert(normalize_path_key(path), Instant::now());
                    self.sentinel_paths.remove(path);
                    self.promotion_stats.record(
                        promote_start.elapsed(),
                        read_start.elapsed(),
                        session.io().take_read_stats(),
                    );
                    return Ok(true);
                }

                self.promotion_stats.record(
                    promote_start.elapsed(),
                    read_start.elapsed(),
                    session.io().take_read_stats(),
                );
                return self.promote_path_unchecked(path).await;
            }
            Err(e) => return Err(format!("Blob shallow read failed at {}: {}", path, e)),
        };
        let read_elapsed = read_start.elapsed();
        let io_stats = session.io().take_read_stats();

        // Step 2: Convert shallow result to an ArcValue
        let blob_value = match shallow_result {
            ShallowValue::Primitive(value) => value,
            ShallowValue::Children(children) => {
                let mut map = std::collections::HashMap::new();
                for child in children {
                    match child.value {
                        Some(prim) => {
                            // Primitive child — insert real value
                            map.insert(child.key, prim);
                        }
                        None => {
                            // Container child — insert empty Sentinel
                            map.insert(child.key, ArcValue::empty_sentinel());
                        }
                    }
                }
                ArcValue::Object(Arc::new(map))
            }
        };

        // Step 3: Build temp tree with blob data and replay WAL entries.
        //
        // Use the *lazy* set/update variants here. The shallow blob read seeded
        // `temp_tree` with empty Sentinel children for each container (the
        // "needs promotion" signal). The non-lazy `tree.set` / `tree.update`
        // walk through Sentinels via `set_path_mut`, which inserts plain
        // `empty_object` for any missing intermediate — so a deep WAL write
        // like `accounts/{a}/characters/{c}/last_played_ms` would tunnel
        // through the Sentinel-rooted children and leave a chain of real
        // Objects holding only the keys the WAL touched. The subtree then
        // reports as "fully loaded" to subsequent reads, which return the
        // partial WAL data instead of triggering a fresh blob read.
        //
        // `set_lazy` / `update_lazy` use `set_path_mut_sentinel`, which keeps
        // missing intermediates as `empty_sentinel` so the tree continues to
        // flag every path on the chain as needing promotion. The leaves still
        // get their WAL values, but the Sentinel signal survives.
        //
        // Regression test: tests/integration_blob.rs
        // `test_blob_root_multipath_update_replay_preserves_sentinel_intermediates`.
        let path_obj = Path::parse(path);
        let mut temp_tree = Tree::new();
        temp_tree.set_arc_uncleaned(&path_obj, blob_value);

        let matching_indices = self.wal_index.find_affecting(path);
        for &idx in &matching_indices {
            let entry = &self.pending_wal_entries[idx];
            let entry_path = Path::parse(&entry.path);
            match entry.op {
                WalOp::Set => {
                    // See note in `promote_path_unchecked`: SET with None
                    // is the historical SET-to-null form. `set_lazy` cleans
                    // Null to a delete via `from_value_cleaned`.
                    let value = entry.value.clone().unwrap_or(Value::Null);
                    temp_tree.set_lazy(&entry_path, value);
                }
                WalOp::Update => {
                    if let Some(Value::Object(ref updates)) = entry.value {
                        temp_tree.update_lazy(&entry_path, updates);
                    }
                }
                WalOp::Delete => {
                    temp_tree.remove(&entry_path);
                }
            }
        }

        // Step 4: Extract the promoted value and set it in the real tree
        let promoted_value = temp_tree.get_arc(&path_obj).unwrap_or(ArcValue::Null);

        {
            let mut tree = self.tree.write().unwrap();
            tree.set_arc_uncleaned_lazy(&path_obj, promoted_value.clone());
        }

        // Track Sentinel ancestors above `path` (promotion only replaces the
        // subtree AT path; ancestors keep whatever Sentinel state they had).
        self.track_sentinels_after_write(path);

        // Track this promotion for eviction timing
        self.promoted_paths
            .insert(normalize_path_key(path), Instant::now());

        // `promoted_value` replaces the entire subtree at `path`. WAL replay
        // can create Sentinel intermediates *deeper* than immediate children:
        // e.g. an `update_lazy` at root with key `characters/<cid>/core` walks
        // through the `characters` Sentinel and creates a `<cid>` Sentinel
        // intermediate inside it. Walking only immediate children would miss
        // `<cid>` and violate the I3 invariant (`sentinel_paths` must be a
        // superset of every Sentinel actually in the tree). Clear the old
        // subtree's entries and walk the full new value.
        self.remove_sentinel_paths_below(path);
        let mut prefix = if path == "/" {
            String::new()
        } else {
            path.to_string()
        };
        Self::collect_sentinel_paths(&promoted_value, &mut prefix, &mut self.sentinel_paths);

        // Record promotion stats
        let total_elapsed = promote_start.elapsed();
        self.promotion_stats
            .record(total_elapsed, read_elapsed, io_stats);

        Ok(true)
    }

    /// Legacy load_from_blob — delegates to promote_path.
    async fn load_from_blob(&mut self, path: &str) -> Result<bool, String> {
        self.promote_path(path).await
    }

    // =========================================================================
    // Sentinel Path Tracking
    // =========================================================================

    /// Lazy-tree invariant: `sentinel_paths` must be a superset of every
    /// actual `Sentinel` node in the in-memory tree. Stale-extra entries are
    /// tolerated (waste reads); missing entries cause skipped promotions and
    /// silent wrong reads.
    ///
    /// Walks the tree and returns every path whose tree node is a `Sentinel`
    /// (empty or with-children) but whose path is NOT in `sentinel_paths`.
    /// An empty return value means the invariant holds for this snapshot.
    ///
    /// O(tree size). Intended as a test-only safety net — callers should
    /// invoke it after a mutation sequence and assert the result is empty:
    ///
    /// ```ignore
    /// // somewhere in a test, after a sequence of writes/promotions:
    /// let violations = db.find_sentinel_tracking_violations();
    /// assert!(violations.is_empty(), "sentinel tracking violation: {:?}", violations);
    /// ```
    ///
    /// Exposed unconditionally (rather than gated on `cfg(test)`) so chaos-monkey
    /// and integration tests can call it the same way unit tests do.
    #[doc(hidden)]
    pub fn find_sentinel_tracking_violations(&self) -> Vec<String> {
        let mut violations = Vec::new();
        let tree = self.tree.read().unwrap();
        let mut path_buf = String::new();
        Self::walk_tree_for_sentinels(
            tree.root(),
            &mut path_buf,
            &self.sentinel_paths,
            &mut violations,
        );
        violations
    }

    /// Recursive helper for `find_sentinel_tracking_violations`. Walks the
    /// tree, accumulating the path, and records a violation whenever a
    /// `Sentinel` node's path is missing from `sentinel_paths`. Walks into
    /// both `Object` and `Sentinel` containers (Sentinels-with-children also
    /// have descendants worth checking).
    fn walk_tree_for_sentinels(
        node: &ArcValue,
        path_buf: &mut String,
        sentinel_paths: &BTreeSet<String>,
        violations: &mut Vec<String>,
    ) {
        if matches!(node, ArcValue::Sentinel(_)) {
            let normalized = if path_buf.is_empty() {
                "/".to_string()
            } else {
                path_buf.clone()
            };
            if !sentinel_paths.contains(&normalized) {
                violations.push(normalized);
            }
        }
        if let ArcValue::Object(map) | ArcValue::Sentinel(map) = node {
            let base_len = path_buf.len();
            for (key, child) in map.iter() {
                path_buf.push('/');
                path_buf.push_str(key);
                Self::walk_tree_for_sentinels(child, path_buf, sentinel_paths, violations);
                path_buf.truncate(base_len);
            }
        }
    }

    /// Walk `node` and insert the path of every `Sentinel` found into `out`.
    /// `path_buf` accumulates the path being walked; pass an empty string for
    /// root, or the path to `node` for a subtree. Insertions use the canonical
    /// form: `"/"` for root, `"/a/b/c"` otherwise.
    ///
    /// Used by `promote_path_shallow` to keep `sentinel_paths` a superset of
    /// every Sentinel in the newly-promoted subtree, including deep
    /// intermediates created by lazy WAL replay.
    fn collect_sentinel_paths(node: &ArcValue, path_buf: &mut String, out: &mut BTreeSet<String>) {
        if matches!(node, ArcValue::Sentinel(_)) {
            let canonical = if path_buf.is_empty() {
                "/".to_string()
            } else {
                path_buf.clone()
            };
            out.insert(canonical);
        }
        if let ArcValue::Object(map) | ArcValue::Sentinel(map) = node {
            let base_len = path_buf.len();
            for (key, child) in map.iter() {
                path_buf.push('/');
                path_buf.push_str(key);
                Self::collect_sentinel_paths(child, path_buf, out);
                path_buf.truncate(base_len);
            }
        }
    }

    /// Check if there are any sentinels at or below `path` in the tree.
    /// O(log n) BTreeSet range query instead of O(tree_size) recursive walk.
    fn has_sentinel_at_or_below(&self, path: &str) -> bool {
        // Exact match
        if self.sentinel_paths.contains(path) {
            return true;
        }
        // Check for any descendant: entries starting with "{path}/"
        let prefix = if path == "/" {
            "/".to_string()
        } else {
            format!("{}/", path)
        };
        self.sentinel_paths
            .range::<String, _>(&prefix..)
            .next()
            .is_some_and(|p| p.starts_with(&prefix))
    }

    /// After a `set_lazy` write, walk the ancestors of the written path
    /// and record any that are Sentinels in `sentinel_paths`.
    /// O(depth) — typically 3-5 tree lookups.
    fn track_sentinels_after_write(&mut self, path_str: &str) {
        let tree = self.tree.read().unwrap();
        let segments: Vec<&str> = path_str.split('/').filter(|s| !s.is_empty()).collect();

        // Check each ancestor (not the leaf itself — that has the real value).
        let mut current = String::new();
        for seg in &segments[..segments.len().saturating_sub(1)] {
            current.push('/');
            current.push_str(seg);
            let path_obj = Path::parse(&current);
            if let Some(node) = tree.get(&path_obj)
                && node.is_sentinel()
            {
                self.sentinel_paths.insert(current.clone());
            }
        }

        // Also check root — it may be a Sentinel
        if tree.root().is_sentinel() {
            self.sentinel_paths.insert("/".to_string());
        }
    }

    /// Remove sentinel tracking for a path and all its descendants (range removal).
    /// Used after deep/unchecked promotion replaces a full subtree with real data.
    fn remove_sentinel_paths_below(&mut self, path: &str) {
        self.sentinel_paths.remove(path);
        if path == "/" {
            self.sentinel_paths.clear();
        } else {
            let prefix = format!("{}/", path);
            let to_remove: Vec<String> = self
                .sentinel_paths
                .range::<String, _>(&prefix..)
                .take_while(|p| p.starts_with(&prefix))
                .cloned()
                .collect();
            for p in to_remove {
                self.sentinel_paths.remove(&p);
            }
        }
    }

    /// Evict promoted paths that have been idle for longer than the eviction timeout.
    /// Replaces the node with an empty Sentinel, freeing the in-memory subtree.
    /// Re-promotion from blob + WAL replay restores the data on next access.
    fn evict_idle_paths(&mut self) {
        let now = Instant::now();
        let idle_timeout =
            Duration::from_secs(EVICTION_IDLE_SECS.load(std::sync::atomic::Ordering::Relaxed));

        // Partition into idle and hot
        let mut idle_paths = Vec::new();
        let mut hot_paths = std::collections::HashSet::new();

        for (path, last_promoted) in &self.promoted_paths {
            if now.duration_since(*last_promoted) >= idle_timeout {
                idle_paths.push(path.clone());
            } else {
                hot_paths.insert(path.clone());
            }
        }

        if idle_paths.is_empty() {
            return;
        }

        let mut tree = self.tree.write().unwrap();
        let mut evicted_count = 0usize;

        for path in &idle_paths {
            // Check if any hot path is at or under this path
            let has_hot_descendant = if path == "/" {
                // Any hot path is a descendant of root
                !hot_paths.is_empty()
            } else {
                hot_paths.iter().any(|hp| is_path_descendant(path, hp))
            };

            if !has_hot_descendant {
                // Safe to evict entirely — replace with Sentinel
                let path_obj = Path::parse(path);
                tree.set_arc_uncleaned_lazy(&path_obj, ArcValue::empty_sentinel());
                // Clear stale descendant entries — the entire subtree is now one sentinel
                if path == "/" {
                    self.sentinel_paths.clear();
                } else {
                    let prefix = format!("{}/", path);
                    let stale: Vec<String> = self
                        .sentinel_paths
                        .range::<String, _>(&prefix..)
                        .take_while(|p| p.starts_with(&prefix))
                        .cloned()
                        .collect();
                    for p in stale {
                        self.sentinel_paths.remove(&p);
                    }
                }
                self.sentinel_paths.insert(path.clone());
                evicted_count += 1;
            } else {
                // Has hot descendants — prune only cold branches
                evicted_count += Self::selective_evict_children(
                    &mut tree,
                    &mut path.clone(),
                    &hot_paths,
                    &mut self.sentinel_paths,
                );
            }
        }
        drop(tree);

        // Remove idle paths from tracking
        for path in &idle_paths {
            self.promoted_paths.remove(path);
        }

        if evicted_count > 0 {
            info!(
                "[Eviction] {}: Evicted {} subtree(s) ({} idle path(s) processed)",
                self.id,
                evicted_count,
                idle_paths.len()
            );
        }
    }

    /// Walk children of a node and replace cold branches with Sentinels.
    /// A branch is "hot" if any path in `hot_paths` is at or under it.
    /// Returns the number of subtrees replaced with Sentinels.
    fn selective_evict_children(
        tree: &mut Tree,
        path: &mut String,
        hot_paths: &std::collections::HashSet<String>,
        sentinel_paths: &mut BTreeSet<String>,
    ) -> usize {
        // For each immediate child key, classify it as one of:
        //   - hot leaf:     the child path itself is a hot path → preserve as-is.
        //   - hot ancestor: the child is on the path to a deeper hot path → recurse.
        //   - cold:         neither → replace with empty Sentinel.
        //
        // The distinction matters because recursing into a hot leaf would walk
        // its primitive fields and Sentinel-clobber them (they have no further
        // hot descendants from the recursion's point of view).
        let path_prefix = if path == "/" { "/" } else { path.as_str() };
        let mut hot_leaf_children: std::collections::HashSet<&str> =
            std::collections::HashSet::new();
        let mut hot_ancestor_children: std::collections::HashSet<&str> =
            std::collections::HashSet::new();
        for hp in hot_paths {
            let suffix = if path_prefix == "/" {
                hp.strip_prefix('/')
            } else {
                hp.strip_prefix(path_prefix)
                    .and_then(|s| s.strip_prefix('/'))
            };
            if let Some(rest) = suffix
                && let Some(seg) = rest.split('/').next()
                && !seg.is_empty()
            {
                if rest.len() == seg.len() {
                    // hp is exactly path/seg — child IS the hot path
                    hot_leaf_children.insert(seg);
                } else {
                    // hp is path/seg/... — child is on the way to a deeper hot path
                    hot_ancestor_children.insert(seg);
                }
            }
        }

        // Collect child keys (must drop tree borrow before mutating)
        let path_obj = Path::parse(path);
        let child_keys: Vec<String> = match tree.get(&path_obj) {
            Some(ArcValue::Object(map) | ArcValue::Sentinel(map)) => map.keys().cloned().collect(),
            _ => return 0,
        };

        let base_len = path.len();
        let mut evicted = 0;
        for key in child_keys {
            // Build child path in-place to avoid allocations
            if base_len == 1 {
                // path is "/"
                path.push_str(&key);
            } else {
                path.push('/');
                path.push_str(&key);
            }

            if hot_leaf_children.contains(key.as_str()) {
                // Child IS a hot path — preserve its subtree untouched.
            } else if hot_ancestor_children.contains(key.as_str()) {
                // Hot descendant somewhere below — recurse to prune selectively
                evicted += Self::selective_evict_children(tree, path, hot_paths, sentinel_paths);
            } else {
                // Cold branch — replace with Sentinel (frees all descendants)
                let child_path_obj = Path::parse(path);
                tree.set_arc_uncleaned_lazy(&child_path_obj, ArcValue::empty_sentinel());
                // Clear stale descendant entries before inserting the new one
                let prefix = format!("{}/", &path);
                let stale: Vec<String> = sentinel_paths
                    .range::<String, _>(&prefix..)
                    .take_while(|p| p.starts_with(&prefix))
                    .cloned()
                    .collect();
                for p in stale {
                    sentinel_paths.remove(&p);
                }
                sentinel_paths.insert(path.clone());
                evicted += 1;
            }

            // Restore path buffer for next sibling
            path.truncate(base_len);
        }

        evicted
    }

    /// Force-evict ALL promoted paths immediately (ignoring idle timeout).
    /// Used for testing eviction/re-promotion edge cases.
    fn force_evict_all_paths(&mut self) {
        if self.promoted_paths.is_empty() {
            return;
        }

        let paths: Vec<String> = self.promoted_paths.keys().cloned().collect();
        let mut tree = self.tree.write().unwrap();

        for path in &paths {
            let path_obj = Path::parse(path);
            tree.set_arc_uncleaned_lazy(&path_obj, ArcValue::empty_sentinel());
            // Clear stale descendant entries
            if path == "/" {
                self.sentinel_paths.clear();
            } else {
                let prefix = format!("{}/", path);
                let stale: Vec<String> = self
                    .sentinel_paths
                    .range::<String, _>(&prefix..)
                    .take_while(|p| p.starts_with(&prefix))
                    .cloned()
                    .collect();
                for p in stale {
                    self.sentinel_paths.remove(&p);
                }
            }
            self.sentinel_paths.insert(path.clone());
            debug!("[Eviction] {}: Force-evicted path {}", self.id, path);
        }
        drop(tree);

        self.promoted_paths.clear();

        info!(
            "[Eviction] {}: Force-evicted {} path(s)",
            self.id,
            paths.len()
        );
    }

    /// Get a handle to send messages to this database.
    pub fn handle(&self) -> DatabaseHandle {
        DatabaseHandle {
            id: self.id.clone(),
            inbox: self.inbox_sender.clone(),
        }
    }

    /// Set volatile path patterns (called when rules are loaded).
    pub fn set_volatile_paths(&mut self, patterns: Vec<String>) {
        self.volatile_paths = patterns.clone();
        self.view_manager.set_volatile_paths(patterns);
    }

    /// Set security rules for this database.
    pub fn set_rules(&mut self, rules: RuleSet) {
        // Extract volatile paths from rules
        let volatile_paths = rules.get_volatile_paths();
        self.set_volatile_paths(volatile_paths);

        // Set the evaluator
        self.evaluator = Some(Rc::new(Evaluator::new(rules)));
    }

    /// Set the evaluator directly (used when evaluator already exists in config).
    pub fn set_evaluator(&mut self, evaluator: Evaluator) {
        // Extract volatile paths from the evaluator's rules
        let volatile_paths = evaluator.get_volatile_paths();
        self.set_volatile_paths(volatile_paths);

        // Set the evaluator
        self.evaluator = Some(Rc::new(evaluator));
    }

    /// Check if a read is allowed for the given client and path.
    ///
    /// This is async because rules evaluation may hit sentinel/unloaded data that needs
    /// to be fetched from blob storage. Each fetch is async and yields to other databases.
    ///
    /// The retry loop (MAX_PROMOTION_RETRIES) handles rules that access many
    /// unloaded paths - each path is loaded from blob and we retry evaluation.
    async fn can_read(
        &mut self,
        client_id: &str,
        path: &str,
        query: Option<Arc<HashMap<String, serde_json::Value>>>,
    ) -> bool {
        let evaluator = match self.evaluator.clone() {
            Some(e) => e,
            None => return true, // No rules = allow all
        };

        let client = match self.clients.get(client_id) {
            Some(c) => c,
            None => return false, // Unknown client
        };

        // Use cached rules_auth to avoid repeated conversion
        let auth = client.rules_auth.clone();

        // Create tree accessor for lazy data access in rules (data.*, root.*)
        let tree_accessor: Arc<dyn TreeGetter> =
            Arc::new(TreeAccessor::new(self.tree.clone(), self.is_blob_backed()));

        let ctx = RulesContext {
            auth,
            root_tree: Some(tree_accessor),
            path: path.to_string(),
            new_data: None,
            is_volatile: self.is_volatile_path(path),
            database_id: self.pure_database_id.clone(),
            project_id: self.project_id.clone(),
            query,
        };

        // Retry loop for rules that access unloaded blob data.
        // Each iteration loads one path from blob and retries evaluation.
        for _attempt in 0..MAX_PROMOTION_RETRIES {
            match evaluator.can_read(&ctx) {
                Ok(allowed) => return allowed,
                Err(needs) => {
                    match self.load_from_blob(&needs.path).await {
                        Ok(did_promote) => {
                            if did_promote {
                                trace!(
                                    path = %needs.path,
                                    "Loading blob data for rules eval (read)"
                                );
                            }
                            continue; // Retry evaluation
                        }
                        Err(e) => {
                            warn!(
                                "Failed to load blob data at {} for read {}: {}",
                                needs.path, path, e
                            );
                            return false;
                        }
                    }
                }
            }
        }

        warn!(path, "Rules eval exceeded max retries for read");
        false
    }

    /// Get a human-readable summary of a client's auth state for logging.
    fn get_auth_summary(&self, client_id: &str) -> String {
        let client = match self.clients.get(client_id) {
            Some(c) => c,
            None => return "unknown client".to_string(),
        };

        match &client.rules_auth {
            None => "unauthenticated".to_string(),
            Some(auth) => {
                let mut parts = vec![];

                if let Some(ref uid) = auth.uid {
                    parts.push(format!("uid={}", uid));
                }

                if let Some(ref provider) = auth.provider {
                    parts.push(format!("provider={}", provider));
                }

                if auth.is_true_admin {
                    parts.push("is_admin=true".to_string());
                }

                // Include custom token claims if present
                if let Some(ref token) = auth.token {
                    for (key, value) in token.iter() {
                        // Skip standard JWT claims, only show custom ones
                        if !["uid", "provider", "iat", "exp", "aud", "iss", "sub"]
                            .contains(&key.as_str())
                        {
                            let val_str = match value {
                                serde_json::Value::String(s) => s.clone(),
                                serde_json::Value::Bool(b) => b.to_string(),
                                serde_json::Value::Number(n) => n.to_string(),
                                _ => format!("{}", value),
                            };
                            parts.push(format!("{}={}", key, val_str));
                        }
                    }
                }

                if parts.is_empty() {
                    "authenticated (no claims)".to_string()
                } else {
                    parts.join(", ")
                }
            }
        }
    }

    /// Check if a write is allowed for the given client and path.
    ///
    /// This is async because rules evaluation may hit sentinel/unloaded data that needs
    /// to be fetched from blob storage. Each fetch is async and yields to other databases.
    ///
    /// The retry loop (MAX_PROMOTION_RETRIES) handles rules that access many
    /// unloaded paths - each path is loaded from blob and we retry evaluation.
    async fn can_write(&mut self, client_id: &str, path: &str, new_data: Option<NewData>) -> bool {
        let evaluator = match self.evaluator.clone() {
            Some(e) => e,
            None => return true, // No rules = allow all
        };

        let client = match self.clients.get(client_id) {
            Some(c) => c,
            None => {
                trace!(
                    "can_write DENIED: unknown client {} for path {} in {}",
                    client_id, path, self.id
                );
                return false;
            }
        };

        // Use cached rules_auth to avoid repeated conversion
        let auth = client.rules_auth.clone();

        // Create tree accessor for lazy data access in rules (data.*, root.*)
        let tree_accessor: Arc<dyn TreeGetter> =
            Arc::new(TreeAccessor::new(self.tree.clone(), self.is_blob_backed()));

        let ctx = RulesContext {
            auth: auth.clone(),
            root_tree: Some(tree_accessor),
            path: path.to_string(),
            new_data,
            is_volatile: self.is_volatile_path(path),
            database_id: self.pure_database_id.clone(),
            project_id: self.project_id.clone(),
            query: None, // Writes don't use query-based rules
        };

        // Retry loop for rules that access unloaded blob data.
        // Each iteration loads one path from blob and retries evaluation.
        for _attempt in 0..MAX_PROMOTION_RETRIES {
            match evaluator.can_write(&ctx) {
                Ok(allowed) => {
                    if !allowed {
                        let auth_uid = auth
                            .as_ref()
                            .and_then(|a| a.uid.as_ref())
                            .map(|s| s.as_str())
                            .unwrap_or("<none>");
                        let auth_provider = auth
                            .as_ref()
                            .and_then(|a| a.provider.as_ref())
                            .map(|s| s.as_str())
                            .unwrap_or("<none>");
                        trace!(
                            "can_write DENIED by rules: path={} auth.uid={} auth.provider={} in {}",
                            path, auth_uid, auth_provider, self.id
                        );
                    }
                    return allowed;
                }
                Err(needs) => {
                    match self.load_from_blob(&needs.path).await {
                        Ok(did_promote) => {
                            if did_promote {
                                trace!(
                                    path = %needs.path,
                                    "Loading blob data for rules eval (write)"
                                );
                            }
                            continue; // Retry evaluation
                        }
                        Err(e) => {
                            warn!(
                                "Failed to load blob data at {} for write {}: {}",
                                needs.path, path, e
                            );
                            return false;
                        }
                    }
                }
            }
        }

        warn!(path, "Rules eval exceeded max retries for write");
        false
    }

    /// Convert database AuthInfo to rules AuthInfo.
    /// This is a static function so it can be called when caching rules_auth.
    /// The returned AuthInfo has its JSON representation pre-computed for efficient rules evaluation.
    /// Wrapped in Arc for O(1) cloning during rules evaluation.
    fn convert_auth_to_rules(auth: &AuthInfo) -> Arc<RulesAuthInfo> {
        let mut token = serde_json::Map::new();
        for (k, v) in &auth.token {
            token.insert(k.clone(), v.clone());
        }

        // Normalize an empty uid to absent. Firebase Legacy Tokens authenticate
        // with uid == "" (identity lives in the `d` claims), so the principal is
        // still authenticated via its token, but `auth.uid` must read as null —
        // otherwise a rule like `auth.uid === $uid` would spuriously match an
        // empty captured path segment. See convert_auth (truly anonymous users
        // are already dropped there).
        let uid = if auth.uid.is_empty() {
            None
        } else {
            Some(auth.uid.clone())
        };

        Arc::new(RulesAuthInfo::new(
            uid,
            Some(auth.provider.clone()),
            if token.is_empty() { None } else { Some(token) },
            auth.is_admin,
        ))
    }

    // =========================================================================
    // Write Deduplication
    // =========================================================================

    /// Get the connection ID for a client.
    fn get_client_connection_id(&self, client_id: &str) -> Option<&str> {
        self.clients
            .get(client_id)
            .map(|c| c.connection_id.as_str())
    }

    /// Check if a write with the given request ID was already processed.
    /// Returns true if the write should be skipped (already processed).
    fn is_write_processed(&self, client_id: &str, request_id: &str) -> bool {
        if request_id.is_empty() {
            return false; // No request ID = no deduplication
        }
        let connection_id = match self.get_client_connection_id(client_id) {
            Some(id) if !id.is_empty() => id,
            _ => return false, // No connection ID = no deduplication
        };
        self.processed_writes
            .get(connection_id)
            .is_some_and(|set| set.contains(request_id))
    }

    /// Record that a write was processed for deduplication.
    fn record_processed_write(&mut self, client_id: &str, request_id: &str) {
        if request_id.is_empty() {
            return; // No request ID = no deduplication
        }
        let connection_id = match self.get_client_connection_id(client_id) {
            Some(id) if !id.is_empty() => id.to_string(),
            _ => return, // No connection ID = no deduplication
        };
        let set = self.processed_writes.entry(connection_id).or_default();
        // Only insert if not already present (IndexSet handles this)
        if set.insert(request_id.to_string()) {
            // Evict entries if over limit
            // Use swap_remove_index (O(1)) instead of shift_remove_index (O(n))
            // Order doesn't matter for deduplication - we just check membership
            while set.len() > MAX_WRITES_PER_CONNECTION {
                set.swap_remove_index(0);
            }
        }
    }

    /// Check if a write is tainted (depends on a nacked write).
    /// Returns true if the write should be silently ignored.
    fn is_write_tainted(&self, client_id: &str, pending_writes: &Option<Vec<String>>) -> bool {
        let pending = match pending_writes {
            Some(pw) if !pw.is_empty() => pw,
            _ => return false, // No pending writes = not tainted
        };
        let connection_id = match self.get_client_connection_id(client_id) {
            Some(id) if !id.is_empty() => id,
            _ => return false, // No connection ID = can't check
        };
        let nacked_set = match self.nacked_writes.get(connection_id) {
            Some(set) => set,
            None => return false, // No nacked writes for this connection
        };
        for request_id in pending {
            if nacked_set.contains(request_id) {
                return true; // Found a nacked write = tainted
            }
        }
        false
    }

    /// Record that a write was nacked (for tainted write detection).
    fn record_nacked_write(&mut self, client_id: &str, request_id: &str) {
        if request_id.is_empty() {
            return; // No request ID = no tracking
        }
        let connection_id = match self.get_client_connection_id(client_id) {
            Some(id) if !id.is_empty() => id.to_string(),
            _ => return, // No connection ID = no tracking
        };
        let set = self.nacked_writes.entry(connection_id).or_default();
        if set.insert(request_id.to_string()) {
            // Evict entries if over limit
            // Use swap_remove_index (O(1)) instead of shift_remove_index (O(n))
            while set.len() > MAX_WRITES_PER_CONNECTION {
                set.swap_remove_index(0);
            }
        }
    }

    /// Drain all pending inbox messages and disconnect any clients with an error.
    /// Called when the database fails during initialization (e.g., load_from_disk or WAL init)
    /// to ensure clients don't hang waiting for a response.
    async fn drain_inbox_with_error(&mut self, reason: &str) {
        while let Some(Some(msg)) = poll_immediate(self.inbox.recv()).await {
            if msg.add_client
                && let Some(conn) = &msg.conn
            {
                let nack = ServerMessage::nack("0", error::UNAVAILABLE, reason);
                if let Ok(data) = nack.encode() {
                    let _ = conn.try_send(data.into(), false, false);
                }
                conn.close();
            }
            // Other messages (protocol, disconnect, etc.) are silently dropped
        }
    }

    /// Run the database message loop.
    /// Event-driven using Glommio's local_channel with message batching.
    /// Processes up to 128 messages or 10ms worth of work before yielding.
    pub async fn run(mut self) {
        debug!("Database {} starting", self.id);

        // Initialize database (BlobSession, etc.)
        if self.pending_disk_load {
            let load_failed = match self.load_from_disk().await {
                Ok(()) => false,
                Err(e) => {
                    error!(
                        "[STORAGE INTEGRITY] {}: Failed to initialize: {}. Database will not serve requests.",
                        self.id, e
                    );
                    true
                }
            };
            self.pending_disk_load = false;

            if load_failed {
                // Don't enter Serving state - database will shut down
                // Disconnect any pending clients so they don't hang indefinitely
                self.drain_inbox_with_error("Database failed to initialize")
                    .await;
                return;
            }

            // Initialize WAL writer
            if !self.init_wal_writer().await {
                // WAL writer failed to initialize - can't accept writes durably
                error!(
                    "[STORAGE INTEGRITY] {}: Failed to initialize WAL writer. Database will not serve requests.",
                    self.id
                );
                self.drain_inbox_with_error("Database WAL failed to initialize")
                    .await;
                return;
            }
        }

        // If startup found many uncompacted WAL files, trigger compaction now.
        if self.needs_startup_compaction {
            self.needs_startup_compaction = false;
            self.notify_compaction().await;
        }

        // Transition to serving
        self.state = DatabaseState::Serving;

        // Track periodic task timing
        let mut last_volatile_fast_flush = Instant::now();
        let mut last_volatile_slow_flush = Instant::now();
        let mut last_wal_sync = Instant::now();
        let mut last_housekeeping = Instant::now();
        let mut last_metrics_emit = Instant::now();
        let mut last_promotion_stats_emit = Instant::now();
        let mut last_backup_marker = Instant::now();

        // Batch processing constants
        const MAX_BATCH_SIZE: usize = 128;
        const MAX_BATCH_DURATION: Duration = Duration::from_millis(10);
        const PERIODIC_INTERVAL: Duration = Duration::from_millis(50);

        loop {
            // Wait for first message or periodic timeout
            let timeout = Timer::new(PERIODIC_INTERVAL);
            enum PollResult {
                GotMessage,
                Timeout,
                InboxClosed,
            }
            let poll_result = futures::select! {
                msg = self.inbox.recv().fuse() => {
                    if let Some(mut msg) = msg {
                        // Stamp inbox pop time for latency tracking
                        if let Some(ref mut ts) = msg.timestamps {
                            ts.stamp_db_inbox_pop();
                        }

                        // Handle the first message
                        self.handle_message_internal(&mut msg).await;

                        // Stamp work complete and record latency
                        if let Some(ref mut ts) = msg.timestamps {
                            ts.stamp_work_complete();
                            crate::metrics::record_latency(ts);
                        }
                        PollResult::GotMessage
                    } else {
                        // Inbox closed - all senders dropped, time to shut down
                        PollResult::InboxClosed
                    }
                }
                _ = timeout.fuse() => {
                    PollResult::Timeout
                }
            };

            // Exit if inbox was closed (all handles dropped)
            if matches!(poll_result, PollResult::InboxClosed) {
                debug!(
                    "Database {} inbox closed, shutting down gracefully",
                    self.id
                );
                break;
            }

            let got_message = matches!(poll_result, PollResult::GotMessage);

            // If we got a message, drain any immediately-available messages (batching)
            if got_message {
                let batch_start = Instant::now();
                let mut batch_count = 1;

                while batch_count < MAX_BATCH_SIZE && batch_start.elapsed() < MAX_BATCH_DURATION {
                    // poll_immediate polls once without blocking
                    match poll_immediate(self.inbox.recv()).await {
                        Some(Some(mut msg)) => {
                            if let Some(ref mut ts) = msg.timestamps {
                                ts.stamp_db_inbox_pop();
                            }

                            self.handle_message_internal(&mut msg).await;

                            if let Some(ref mut ts) = msg.timestamps {
                                ts.stamp_work_complete();
                                crate::metrics::record_latency(ts);
                            }
                            batch_count += 1;
                        }
                        _ => break, // No more ready messages
                    }
                }

                if batch_count > 1 {
                    trace!(
                        "Database {} processed batch of {} messages",
                        self.id, batch_count
                    );
                }
            }

            // Yield to scheduler to allow TCP tasks to run
            glommio::yield_if_needed().await;

            // Check for fatal error (e.g., failed blob generation switch)
            if self.fatal_error {
                error!("Database {} shutting down due to fatal error", self.id);
                break;
            }

            // Check if all external handles have been dropped (graceful shutdown)
            // Rc::strong_count() == 1 means only the Database's copy remains
            if Rc::strong_count(&self.inbox_sender) == 1 {
                debug!(
                    "Database {} all handles dropped, shutting down gracefully",
                    self.id
                );
                break;
            }

            // Run periodic tasks based on elapsed time
            let now = Instant::now();

            // Flush volatile batches for high-frequency clients (50ms)
            if now.duration_since(last_volatile_fast_flush) >= VOLATILE_FAST_FLUSH_INTERVAL {
                self.flush_volatile_fast();
                last_volatile_fast_flush = now;
            }

            // Flush volatile batches for low-frequency clients (333ms)
            if now.duration_since(last_volatile_slow_flush) >= VOLATILE_SLOW_FLUSH_INTERVAL {
                self.flush_volatile_slow();
                last_volatile_slow_flush = now;
            }

            // Sync WAL to disk (2s) - async to avoid blocking other DBs
            if now.duration_since(last_wal_sync) >= WAL_SYNC_INTERVAL {
                self.sync_wal().await;
                last_wal_sync = now;
            }

            // Housekeeping (5s)
            if now.duration_since(last_housekeeping) >= Duration::from_secs(5) {
                self.housekeeping().await;
                last_housekeeping = now;

                // Debug: print stats (only at debug level to reduce noise)
                trace!(
                    "Database {} stats: clients={}, views={}",
                    self.id,
                    self.clients.len(),
                    self.view_manager.view_count()
                );

                // Check for idle shutdown
                if self.clients.is_empty() && self.last_activity.elapsed() > Duration::from_secs(60)
                {
                    debug!("Database {} idle, shutting down", self.id);
                    break;
                }
            }

            // Emit metrics to stdout (60s, only if active)
            if now.duration_since(last_metrics_emit) >= METRICS_EMIT_INTERVAL {
                self.refresh_data_size().await;
                self.emit_metrics();
                last_metrics_emit = now;
            }

            // Emit promotion stats (30s)
            if now.duration_since(last_promotion_stats_emit) >= Duration::from_secs(30) {
                self.emit_promotion_stats();
                last_promotion_stats_emit = now;
            }

            // Write backup marker (5 min) so lark-compact syncs WAL files for active databases,
            // even if no WAL rotation has occurred. Written every 5 min to ensure lark-compact's
            // 15-minute WAL sync cycle always finds a fresh marker.
            if now.duration_since(last_backup_marker) >= Duration::from_secs(300) {
                if self.wal_writer.is_some() {
                    self.write_compaction_queue_marker().await;
                }
                last_backup_marker = now;
            }
        }

        // Final WAL sync and close before shutdown
        self.sync_wal().await;
        self.close_wal().await;

        // Write compaction queue marker so lark-compact syncs any unrotated WAL data
        self.write_compaction_queue_marker().await;

        // Tell StorageWorker to clean up cached state (BlobSession, shared CachedIO)
        self.notify_storage_worker_shutdown();

        // Drop blob session (owns the IO handle — closes on drop)
        self.blob_session.take();

        debug!("Database {} stopped", self.id);
    }

    async fn handle_message_internal(&mut self, msg: &mut InboxMessage) {
        self.last_activity = Instant::now();

        // Handle compaction complete from StorageWorker
        if let Some(cc) = msg.compaction_complete.take() {
            self.handle_compaction_complete(cc).await;
            return;
        }

        // Handle force eviction (for testing)
        if msg.force_evict_all {
            self.force_evict_all_paths();
            return;
        }

        // Handle rules hot-reload from CONFIG_PUSH
        if msg.has_evaluator_update {
            match msg.evaluator_update.take() {
                Some(evaluator) => {
                    debug!("Database {} applying new rules from CONFIG_PUSH", self.id);
                    self.set_evaluator(evaluator);
                }
                None => {
                    debug!(
                        "Database {} clearing rules from CONFIG_PUSH (fully open)",
                        self.id
                    );
                    self.evaluator = None;
                    self.set_volatile_paths(Vec::new());
                }
            }
            return;
        }

        // Handle special message types
        if msg.add_client {
            if let Some(ref conn) = msg.conn {
                self.add_client_internal(
                    &msg.client_id,
                    msg.auth_info.clone(),
                    &msg.connection_id,
                    conn.clone(),
                );
            }
            return;
        }

        if msg.disconnect {
            self.handle_disconnect(&msg.client_id).await;
            return;
        }

        if msg.has_auth {
            self.handle_auth_update(&msg.client_id, msg.auth_update.clone());
            return;
        }

        // Handle protocol message
        if let Some(ref protocol_msg) = msg.message {
            let volatile = msg.volatile;
            self.handle_protocol_message(&msg.client_id, protocol_msg.clone(), volatile)
                .await;

            // Record end-to-end latency (TCP receive to processing complete)
            let latency_us = msg.start_time.elapsed().as_micros() as u32;
            self.metrics.record_latency(latency_us);
        }
    }

    async fn handle_protocol_message(
        &mut self,
        client_id: &str,
        msg: ClientMessage,
        volatile: bool,
    ) {
        let response = match msg.op.as_str() {
            op::JOIN => self.handle_join(client_id, &msg),
            op::AUTH => self.handle_auth(client_id, &msg),
            op::UNAUTH => self.handle_unauth(client_id, &msg),
            op::SET => self.handle_set(client_id, &msg, volatile).await,
            op::UPDATE => self.handle_update(client_id, &msg, volatile).await,
            op::REMOVE => self.handle_remove(client_id, &msg, volatile).await,
            op::SUBSCRIBE => self.handle_subscribe(client_id, &msg).await,
            op::UNSUBSCRIBE => self.handle_unsubscribe(client_id, &msg),
            op::ONCE => self.handle_once(client_id, &msg).await,
            op::ON_DISCONNECT => self.handle_on_disconnect(client_id, &msg).await,
            op::TRANSACTION => self.handle_transaction(client_id, &msg).await,
            op::LEAVE => {
                // Leave is a graceful disconnect - trigger ondisconnect hooks
                self.handle_disconnect(client_id).await;
                let request_id = msg.request_id.as_deref().unwrap_or("");
                Some(ServerMessage::ack(request_id))
            }
            op::PING => None, // Client keepalive, swallow without response
            op::PONG => None, // Keepalive response, ignore
            _ => {
                let request_id = msg.request_id.as_deref().unwrap_or("");
                Some(ServerMessage::nack(
                    request_id,
                    error::INVALID_OPERATION,
                    "unknown operation",
                ))
            }
        };

        // Send response
        if let Some(resp) = response {
            self.send_to_client(client_id, &resp, false).await;
        }
    }

    /// Handle JOIN message - acknowledges the join and returns volatile paths.
    fn handle_join(&self, client_id: &str, msg: &ClientMessage) -> Option<ServerMessage> {
        let request_id = msg.request_id.as_deref().unwrap_or("");

        // Get connection ID from client info
        let connection_id = self
            .clients
            .get(client_id)
            .map(|c| c.connection_id.clone())
            .unwrap_or_default();

        Some(ServerMessage::join_ack(
            request_id,
            self.volatile_paths.clone(),
            &connection_id,
        ))
    }

    /// Handle AUTH message - validates token and sets auth state for the client.
    ///
    /// In production, token validation is done by the server/proxy layer which has
    /// access to project secrets. Here we validate HS256 tokens using the project secret
    /// if available, or accept the token in emulator mode.
    fn handle_auth(&mut self, client_id: &str, msg: &ClientMessage) -> Option<ServerMessage> {
        let request_id = msg.request_id.as_deref().unwrap_or("");

        let client = match self.clients.get_mut(client_id) {
            Some(c) => c,
            None => {
                return Some(ServerMessage::nack(
                    request_id,
                    error::NOT_FOUND,
                    "client not found",
                ));
            }
        };

        // Empty token = anonymous auth
        let token = msg.token.as_deref().unwrap_or("");
        if token.is_empty() {
            client.auth_complete = true;
            client.auth = None;
            client.rules_auth = None;
            return Some(ServerMessage::auth_ack(request_id, ""));
        }

        // Validate the token using our auth module
        match validate_auth_token(token, self.project_secret.as_deref()) {
            Ok(auth_info) => {
                let uid = auth_info.uid.clone();
                let auth = AuthInfo {
                    uid: auth_info.uid,
                    provider: auth_info.provider,
                    token: auth_info.token,
                    is_admin: auth_info.is_true_admin,
                };
                client.rules_auth = Some(Self::convert_auth_to_rules(&auth));
                client.auth = Some(auth);
                client.auth_complete = true;
                Some(ServerMessage::auth_ack(request_id, &uid))
            }
            Err(e) => Some(ServerMessage::nack(
                request_id,
                error::PERMISSION_DENIED,
                &format!("invalid token: {}", e),
            )),
        }
    }

    /// Handle UNAUTH message - clears auth state for the client.
    fn handle_unauth(&mut self, client_id: &str, msg: &ClientMessage) -> Option<ServerMessage> {
        let request_id = msg.request_id.as_deref().unwrap_or("");

        if let Some(client) = self.clients.get_mut(client_id) {
            client.auth = None;
            client.rules_auth = None;
            // Keep auth_complete true - just cleared the auth
            return Some(ServerMessage::ack(request_id));
        }

        Some(ServerMessage::nack(
            request_id,
            error::NOT_FOUND,
            "client not found",
        ))
    }

    /// Handle TRANSACTION message - atomic multi-path operations.
    async fn handle_transaction(
        &mut self,
        client_id: &str,
        msg: &ClientMessage,
    ) -> Option<ServerMessage> {
        let request_id = msg.request_id.as_deref().unwrap_or("");

        // Check for tainted write (depends on a nacked write) - silently ignore
        if self.is_write_tainted(client_id, &msg.pending_writes) {
            return None; // Silently ignore tainted writes
        }

        // Check for duplicate write (deduplication)
        if self.is_write_processed(client_id, request_id) {
            // Already processed - return ack without doing anything
            if !request_id.is_empty() {
                return Some(ServerMessage::ack(request_id));
            }
            return None;
        }

        // NACK if WAL I/O has failed
        if self.is_wal_failed() {
            self.record_nacked_write(client_id, request_id);
            return Some(ServerMessage::nack(
                request_id,
                error::UNAVAILABLE,
                "Storage unavailable (WAL I/O failure)",
            ));
        }

        let operations = match &msg.operations {
            Some(ops) => ops,
            None => {
                self.record_nacked_write(client_id, request_id);
                return Some(ServerMessage::nack(
                    request_id,
                    error::INVALID_DATA,
                    "missing operations",
                ));
            }
        };

        if operations.is_empty() {
            self.record_nacked_write(client_id, request_id);
            return Some(ServerMessage::nack(
                request_id,
                error::INVALID_DATA,
                "empty transaction",
            ));
        }

        // Cap operations per transaction. Each condition op below promotes a
        // path (blob read + WAL replay) on this database's single inbox, so an
        // oversized transaction would serialize many disk round trips and stall
        // every client on the database. See audit M-2.
        if operations.len() > MAX_TRANSACTION_OPS {
            debug!(
                "NACK {}: transaction has {} ops, exceeds cap {}",
                self.id,
                operations.len(),
                MAX_TRANSACTION_OPS
            );
            self.record_nacked_write(client_id, request_id);
            return Some(ServerMessage::nack(
                request_id,
                error::PAYLOAD_TOO_LARGE,
                &format!("transaction exceeds {} operations", MAX_TRANSACTION_OPS),
            ));
        }

        // Validate every operation's paths AND the keys inside its value before
        // anything else. Both the op path and the object field-names in the value
        // become storage keys, so the same key rules (validate_key: non-empty, no
        // control chars / `$ # [ ] /`, `.` only as a leading char, ≤768 bytes)
        // must hold for all of them. Rejecting up front means malformed keys can't
        // reach the rules evaluator or the WAL/blob writers, and it closes the
        // rules-vs-storage tokenizer divergence (e.g. `users//abc` has an empty
        // segment → rejected here, before the two tokenizers can disagree about
        // where the write lands).
        for op in operations {
            let check = || -> Result<(), crate::db::KeyError> {
                crate::db::validate_path(&op.path)?;
                match (op.op.as_str(), &op.value) {
                    // SET: the value's object keys become storage keys.
                    ("s" | "set", Some(value)) => validate_value_keys(value)?,
                    // UPDATE: each map key is a relative path appended to op.path
                    // (validate the full landing path), and each update value's
                    // own object keys become storage keys too.
                    ("u" | "update", Some(Value::Object(map))) => {
                        for (key, val) in map {
                            let full = format!("{}/{}", op.path.trim_end_matches('/'), key);
                            crate::db::validate_path(&full)?;
                            validate_value_keys(val)?;
                        }
                    }
                    _ => {}
                }
                Ok(())
            };
            if let Err(e) = check() {
                debug!(
                    "NACK {}: invalid path/key in op at {:?}: {}",
                    self.id, op.path, e
                );
                self.record_nacked_write(client_id, request_id);
                return Some(ServerMessage::nack(
                    request_id,
                    error::INVALID_DATA,
                    "invalid path or key",
                ));
            }
        }

        // First, check permissions for all write operations
        for op in operations {
            if op.op == "c" {
                continue; // Conditions don't need write permission
            }

            // Check write permission. Build the appropriate `NewData` for
            // each op type — SET-style for "s" with a value, UPDATE-style
            // for "u", and None for "d" (delete).
            let new_data = match (op.op.as_str(), op.value.clone()) {
                ("s", Some(v)) => Some(NewData::from_set(op.path.clone(), v)),
                ("u", Some(Value::Object(map))) => Some(NewData::from_update(op.path.clone(), map)),
                _ => None,
            };
            if !self.can_write(client_id, &op.path, new_data).await {
                let auth_summary = self.get_auth_summary(client_id);
                debug!(
                    "NACK {}: TRANSACTION permission denied at {} for client {} | auth: {}",
                    self.id, op.path, client_id, auth_summary
                );
                self.metrics.record_permission_denial();
                self.record_nacked_write(client_id, request_id);
                return Some(ServerMessage::nack(
                    request_id,
                    error::PERMISSION_DENIED,
                    "write permission denied",
                ));
            }
        }

        // Validate all conditions. Promotion is idempotent, so dedup repeated
        // condition paths within the transaction — promoting a path twice is
        // wasted disk work and an avoidable amplification vector (audit M-2).
        let mut promoted: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for op in operations {
            if op.op == "c" {
                // Promote from blob if needed for accurate condition check.
                // Use deep promotion so container values are fully loaded —
                // shallow promotion leaves Sentinel children which would
                // serialize to null and break value/hash comparisons.
                if promoted.insert(op.path.as_str())
                    && let Err(e) = self.promote_path_deep(&op.path).await
                {
                    warn!(
                        "NACK TRANSACTION: promotion failed for condition at {}: {}",
                        op.path, e
                    );
                    return Some(ServerMessage::nack(
                        request_id,
                        error::UNAVAILABLE,
                        &format!("Failed to load data for condition check: {}", e),
                    ));
                }

                let path = Path::parse(&op.path);
                let current_value = self.tree.read().unwrap().get(&path).map(|n| n.to_value());

                if let Some(ref expected) = op.value {
                    // Value-based condition
                    let current_val = current_value.as_ref().unwrap_or(&Value::Null);
                    if current_val != expected {
                        return Some(ServerMessage::nack(
                            request_id,
                            error::CONDITION_FAILED,
                            "condition not met",
                        ));
                    }
                } else if let Some(ref hash) = op.hash {
                    // Hash-based condition check
                    let current_val = current_value.as_ref().unwrap_or(&Value::Null);
                    let current_hash = compute_value_hash(current_val);
                    if &current_hash != hash {
                        return Some(ServerMessage::nack(
                            request_id,
                            error::CONDITION_FAILED,
                            "hash mismatch",
                        ));
                    }
                } else {
                    // No value and no hash means expecting null/non-existent
                    if current_value.is_some() {
                        return Some(ServerMessage::nack(
                            request_id,
                            error::CONDITION_FAILED,
                            "expected null but path exists",
                        ));
                    }
                }
            }
        }

        // Validate .value/.priority patterns for all set/update operations
        for op in operations {
            match op.op.as_str() {
                "s" | "set" => {
                    if let Some(ref value) = op.value
                        && let Err(e) = validate_value_priority(value, &op.path)
                    {
                        return Some(ServerMessage::nack(request_id, error::INVALID_DATA, &e));
                    }
                }
                "u" | "update" => {
                    if let Some(Value::Object(map)) = &op.value {
                        for (key, val) in map {
                            let child_path = format!("{}/{}", op.path.trim_end_matches('/'), key);
                            if let Err(e) = validate_value_priority(val, &child_path) {
                                return Some(ServerMessage::nack(
                                    request_id,
                                    error::INVALID_DATA,
                                    &e,
                                ));
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        // Collect changes for subscriber notifications
        #[allow(clippy::type_complexity)] // (path, key, value, fields) change tuples
        let mut changes: Vec<(
            String,
            String,
            Option<Value>,
            Option<serde_json::Map<String, Value>>,
        )> = Vec::new();

        // Apply all operations
        // Note: We need to collect WAL entries separately because we can't hold the tree lock while writing to WAL.
        // Each arm acquires/releases the tree lock as needed so we can call &mut self
        // helpers (remove_sentinel_paths_below, track_sentinels_after_write) between writes.
        let mut wal_entries: Vec<(String, String, Option<Value>)> = Vec::new();
        let blob_backed = self.is_blob_backed();
        for op in operations {
            let path = Path::parse(&op.path);
            match op.op.as_str() {
                "s" | "set" => {
                    if let Some(ref value) = op.value {
                        let processed =
                            match process_server_values(value.clone(), &op.path, &self.tree) {
                                Ok((v, _)) => v,
                                Err(e) => {
                                    return Some(ServerMessage::nack(
                                        request_id,
                                        error::INVALID_DATA,
                                        &e,
                                    ));
                                }
                            };
                        // For blob-backed DBs, use set_lazy so intermediate nodes
                        // are Sentinels (not empty Objects). Empty Object intermediates
                        // would lie about being "fully loaded" and cause subsequent
                        // reads to short-circuit promotion, returning partial data.
                        if blob_backed {
                            self.remove_sentinel_paths_below(&op.path);
                            self.tree
                                .write()
                                .unwrap()
                                .set_lazy(&path, processed.clone());
                            self.track_sentinels_after_write(&op.path);
                        } else {
                            self.tree.write().unwrap().set(&path, processed.clone());
                        }
                        wal_entries.push((
                            op.path.clone(),
                            "set".to_string(),
                            Some(processed.clone()),
                        ));
                        changes.push((op.path.clone(), "set".to_string(), Some(processed), None));
                    }
                }
                "u" | "update" => {
                    if let Some(Value::Object(map)) = &op.value {
                        let mut processed_map = serde_json::Map::new();
                        for (key, val) in map {
                            let child_path = format!("{}/{}", op.path.trim_end_matches('/'), key);
                            let processed =
                                match process_server_values(val.clone(), &child_path, &self.tree) {
                                    Ok((v, _)) => v,
                                    Err(e) => {
                                        return Some(ServerMessage::nack(
                                            request_id,
                                            error::INVALID_DATA,
                                            &e,
                                        ));
                                    }
                                };
                            processed_map.insert(key.clone(), processed);
                        }
                        // For blob-backed DBs, use update_lazy so the merge writes
                        // through Sentinel intermediates instead of clobbering them
                        // into Objects. Subsequent reads will promote on demand.
                        if blob_backed {
                            self.tree
                                .write()
                                .unwrap()
                                .update_lazy(&path, &processed_map);
                            // Track Sentinels created by each leaf write — track_sentinels_after_write
                            // walks ancestors of its argument, so passing each leaf path catches
                            // the update-path itself (which became a Sentinel container).
                            let base = op.path.trim_end_matches('/');
                            for key in processed_map.keys() {
                                let leaf_path = format!("{}/{}", base, key);
                                self.track_sentinels_after_write(&leaf_path);
                            }
                        } else {
                            self.tree.write().unwrap().update(&path, &processed_map);
                        }
                        wal_entries.push((
                            op.path.clone(),
                            "update".to_string(),
                            Some(Value::Object(processed_map.clone())),
                        ));
                        changes.push((
                            op.path.clone(),
                            "update".to_string(),
                            None,
                            Some(processed_map),
                        ));
                    }
                }
                "d" | "remove" => {
                    self.tree.write().unwrap().remove(&path);
                    // Clear sentinel tracking at and below the deleted path —
                    // those nodes are gone (matches handle_remove).
                    self.remove_sentinel_paths_below(&op.path);
                    wal_entries.push((op.path.clone(), "remove".to_string(), None));
                    changes.push((op.path.clone(), "remove".to_string(), None, None));
                }
                "c" => {
                    // Condition - already checked above
                }
                _ => {}
            }
        }

        // Write to WAL (tree lock is released now) - async to avoid blocking
        for (path, op_type, value) in wal_entries {
            match op_type.as_str() {
                "set" => {
                    if let Some(v) = value {
                        self.wal_write_set(&path, &v);
                    }
                }
                "update" => {
                    if let Some(Value::Object(map)) = value {
                        self.wal_write_update(&path, &map);
                    }
                }
                "remove" => {
                    self.wal_write_delete(&path);
                }
                _ => {}
            }
        }

        // Notify subscribers of changes
        for (path, mutation_type, new_value, updates) in changes {
            self.broadcast_mutation(
                &path,
                &mutation_type,
                new_value,
                updates,
                false,
                Some(client_id),
            )
            .await;
        }

        // Record for deduplication
        self.record_processed_write(client_id, request_id);

        // Record transaction metrics
        self.metrics.record_transaction();

        Some(ServerMessage::ack(request_id))
    }

    // =========================================================================
    // Client Management
    // =========================================================================

    fn add_client_internal(
        &mut self,
        client_id: &str,
        auth: Option<AuthInfo>,
        connection_id: &str,
        conn: Arc<dyn ConnectionSender>,
    ) {
        debug!("Client {} joined database {}", client_id, self.id);
        let rules_auth = auth.as_ref().map(Self::convert_auth_to_rules);
        self.clients.insert(
            client_id.to_string(),
            ClientInfo {
                id: client_id.to_string(),
                auth,
                rules_auth,
                connection_id: connection_id.to_string(),
                auth_complete: false,
                conn,
            },
        );

        // Update CCU metric
        self.metrics.increment_ccu();
    }

    async fn handle_disconnect(&mut self, client_id: &str) {
        debug!(
            "Client {} disconnected from database {}",
            client_id, self.id
        );

        // Execute disconnect hooks
        if let Some(actions) = self.on_disconnect.remove(client_id) {
            for action in actions {
                let path = Path::parse(&action.path);
                match action.action.as_str() {
                    "set" | "s" => {
                        if let Some(value) = action.value {
                            if self.is_volatile_path(&action.path) {
                                self.view_manager.clear_volatile_for_path(&action.path);
                            }
                            self.remove_sentinel_paths_below(&action.path);
                            if self.is_blob_backed() {
                                self.tree.write().unwrap().set_lazy(&path, value.clone());
                                self.track_sentinels_after_write(&action.path);
                            } else {
                                self.tree.write().unwrap().set(&path, value.clone());
                            }
                            self.wal_write_set(&action.path, &value);
                            self.broadcast_mutation(
                                &action.path,
                                "set",
                                Some(value),
                                None,
                                false,
                                None,
                            )
                            .await;
                        }
                    }
                    "update" | "u" => {
                        if let Some(Value::Object(updates)) = action.value {
                            if self.is_volatile_path(&action.path) {
                                self.view_manager.clear_volatile_for_path(&action.path);
                            }
                            // For blob-backed DBs, use update_lazy so the merge writes
                            // through Sentinel intermediates instead of creating empty
                            // Objects that would lie about being fully loaded.
                            if self.is_blob_backed() {
                                self.tree.write().unwrap().update_lazy(&path, &updates);
                                let base = action.path.trim_end_matches('/');
                                for key in updates.keys() {
                                    let leaf_path = format!("{}/{}", base, key);
                                    self.track_sentinels_after_write(&leaf_path);
                                }
                            } else {
                                self.tree.write().unwrap().update(&path, &updates);
                            }
                            self.wal_write_update(&action.path, &updates);
                            self.broadcast_mutation(
                                &action.path,
                                "update",
                                None,
                                Some(updates),
                                false,
                                None,
                            )
                            .await;
                        }
                    }
                    "remove" | "d" => {
                        // Clear from volatile batch first to prevent stale data
                        // from being flushed after the removal event
                        if self.is_volatile_path(&action.path) {
                            self.view_manager.clear_volatile_for_path(&action.path);
                        }
                        self.tree.write().unwrap().remove(&path);
                        self.remove_sentinel_paths_below(&action.path);
                        self.wal_write_delete(&action.path);
                        self.broadcast_mutation(&action.path, "remove", None, None, false, None)
                            .await;
                    }
                    _ => {}
                }
            }
        }

        // Remove all subscriptions for this client
        self.view_manager.unsubscribe_all(client_id);

        // Note: We intentionally keep processed_writes/nacked_writes entries
        // for the connection_id. If the client reconnects with the same
        // connection_id, we need this history for deduplication.
        // Memory is bounded by MAX_WRITES_PER_CONNECTION per connection.

        // Remove client
        self.clients.remove(client_id);

        // Update CCU metric
        self.metrics.decrement_ccu();
    }

    fn handle_auth_update(&mut self, client_id: &str, auth: Option<AuthInfo>) {
        if let Some(client) = self.clients.get_mut(client_id) {
            client.rules_auth = auth.as_ref().map(Self::convert_auth_to_rules);
            client.auth = auth;
            client.auth_complete = true;
        }
    }

    pub fn client_count(&self) -> usize {
        self.clients.len()
    }

    // =========================================================================
    // Write Operations
    // =========================================================================

    /// Record and build the NACK for a write whose path or value keys are
    /// invalid (empty/oversized segment, control char, `$ # [ ] /`,
    /// dot-in-middle, or a literal-slash object key). Shared by every single-op
    /// write handler so the same key invariant is enforced on each entry point,
    /// not just inside `handle_transaction`.
    fn nack_invalid_key(&mut self, client_id: &str, request_id: &str) -> Option<ServerMessage> {
        self.record_nacked_write(client_id, request_id);
        Some(ServerMessage::nack(
            request_id,
            error::INVALID_DATA,
            "invalid path or key",
        ))
    }

    async fn handle_set(
        &mut self,
        client_id: &str,
        msg: &ClientMessage,
        volatile: bool,
    ) -> Option<ServerMessage> {
        let request_id = msg.request_id.as_deref().unwrap_or("");
        let path_str = msg.path.as_deref().unwrap_or("/");

        // Reject malformed paths or value keys before any work, the rules
        // evaluator, or the WAL/blob writers. SET stores its value object as-is,
        // so its field-names become storage keys and must pass validate_key too.
        if crate::db::validate_path(path_str).is_err()
            || msg
                .value
                .as_ref()
                .is_some_and(|v| validate_value_keys(v).is_err())
        {
            return self.nack_invalid_key(client_id, request_id);
        }

        // Check for tainted write (depends on a nacked write) - silently ignore
        if self.is_write_tainted(client_id, &msg.pending_writes) {
            return None; // Silently ignore tainted writes
        }

        // Check for duplicate write (deduplication)
        if self.is_write_processed(client_id, request_id) {
            // Already processed - return ack without doing anything
            if !request_id.is_empty() {
                return Some(ServerMessage::ack(request_id));
            }
            return None;
        }

        // NACK if WAL I/O has failed (non-volatile writes only)
        if !volatile && self.is_wal_failed() {
            self.record_nacked_write(client_id, request_id);
            return Some(ServerMessage::nack(
                request_id,
                error::UNAVAILABLE,
                "Storage unavailable (WAL I/O failure)",
            ));
        }

        let value = match &msg.value {
            Some(v) => v.clone(),
            None => Value::Null,
        };

        // Validate .value/.priority patterns
        if let Err(e) = validate_value_priority(&value, path_str) {
            if !volatile {
                self.record_nacked_write(client_id, request_id);
                return Some(ServerMessage::nack(request_id, error::INVALID_DATA, &e));
            }
            return None; // Swallow NACK for volatile writes
        }

        // Process server values (like {".sv": "timestamp"} or {".sv": {"increment": 10}})
        let value = match process_server_values(value, path_str, &self.tree) {
            Ok((processed, _)) => processed,
            Err(e) => {
                if !volatile {
                    self.record_nacked_write(client_id, request_id);
                    return Some(ServerMessage::nack(request_id, error::INVALID_DATA, &e));
                }
                return None; // Swallow NACK for volatile writes
            }
        };

        // Check write permission
        if !self
            .can_write(
                client_id,
                path_str,
                Some(NewData::from_set(path_str.to_string(), value.clone())),
            )
            .await
        {
            let auth_summary = self.get_auth_summary(client_id);
            debug!(
                "NACK {}: SET permission denied at {} for client {} | auth: {}",
                self.id, path_str, client_id, auth_summary
            );
            self.metrics.record_permission_denial();
            if !volatile {
                self.record_nacked_write(client_id, request_id);
                return Some(ServerMessage::nack(
                    request_id,
                    error::PERMISSION_DENIED,
                    "Permission denied",
                ));
            }
            return None; // Swallow NACK for volatile writes
        }

        // Check compare-and-swap hash if provided (Firebase transaction support)
        let hash = msg.hash.as_deref().unwrap_or("");
        let hash_provided = msg.hash_provided.unwrap_or(false);
        if !hash.is_empty() || hash_provided {
            // Promote path to get accurate data for hash comparison
            if let Err(e) = self.promote_path(path_str).await {
                warn!(
                    "NACK SET {}: promotion failed for hash check: {}",
                    path_str, e
                );
                self.record_nacked_write(client_id, request_id);
                return Some(ServerMessage::nack(
                    request_id,
                    error::UNAVAILABLE,
                    &format!("Failed to load data for hash verification: {}", e),
                ));
            }

            let path_obj = Path::parse(path_str);
            let old_value = self.tree.read().unwrap().get_value(&path_obj);

            if !hash.is_empty() {
                // Compare hash of current value
                let current_hash = if is_firebase_hash(hash) {
                    // Firebase hash: SHA-1 + base64
                    compute_firebase_hash(&old_value.clone().unwrap_or(Value::Null))
                } else {
                    // Lark hash: JCS + SHA-256 + hex
                    compute_value_hash(&old_value.clone().unwrap_or(Value::Null))
                };

                if current_hash != hash {
                    // Hash mismatch - data changed since client read it
                    // Don't record as nacked - condition_failed is retryable
                    return Some(ServerMessage::nack(
                        request_id,
                        error::CONDITION_FAILED,
                        "data changed since read (hash mismatch)",
                    ));
                }
            } else if hash_provided && old_value.as_ref().is_some_and(|v| !v.is_null()) {
                // Empty hash with hash_provided=true means speculative transaction
                // (client has no cached data). Only accept if path has no existing data.
                //
                // `old_value.is_some()` alone is wrong: a path that's been
                // promoted as "loaded, doesn't exist" sits in the tree as
                // `Some(Value::Null)` (the marker `promote_path_unchecked`
                // installs on PathNotFound) — semantics treat null
                // as "doesn't exist," so a speculative write against it must
                // succeed, not fail. The check has to look at the value
                // itself, not just whether the lookup returned Some.
                //
                // Without this, a client whose listener received a
                // null snapshot and then ran `transaction()` got rejected on
                // the speculative first attempt, retried with the same
                // payload, and looped to MAXRETRY without progress.
                return Some(ServerMessage::nack(
                    request_id,
                    error::CONDITION_FAILED,
                    "data exists (speculative write rejected)",
                ));
            }
        }

        // Determine if this path is volatile based on RULES (don't trust client flag)
        let is_volatile = self.is_volatile_path(path_str);

        // Check if this is a volatile path - use ViewManager batching, skip persistence
        if is_volatile {
            // Fast path: buffer in ViewManager for batch sending
            let value_bytes = Bytes::from(serde_json::to_vec(&value).unwrap_or_default());
            self.view_manager
                .buffer_volatile(path_str, value_bytes, client_id);

            // Record write metrics for volatile writes too
            self.metrics.record_write(msg.payload_size);

            // No ack for volatile writes
            return None;
        }

        // Regular write path
        let path = Path::parse(path_str);

        // Set the value in tree. For blob-backed DBs, use set_lazy so
        // intermediate nodes are Sentinels (no eager loading needed for SET).
        if self.is_blob_backed() {
            // Clear stale descendant sentinel entries — set_lazy replaces the subtree
            self.remove_sentinel_paths_below(path_str);
            self.tree.write().unwrap().set_lazy(&path, value.clone());
            self.track_sentinels_after_write(path_str);
        } else {
            self.tree.write().unwrap().set(&path, value.clone());
        }

        // Write to WAL for durability (async)
        self.wal_write_set(path_str, &value);

        // Broadcast to subscribers via ViewManager
        self.broadcast_mutation(
            path_str,
            "set",
            Some(value),
            None,
            is_volatile,
            Some(client_id),
        )
        .await;

        // Record for deduplication (skip volatile writes - they don't need deduplication)
        if !is_volatile {
            self.record_processed_write(client_id, request_id);
        }

        // Record write metrics using raw payload size captured at parse time
        self.metrics.record_write(msg.payload_size);

        // Return ack (only if not volatile and has request_id)
        if !msg.is_volatile() && !request_id.is_empty() {
            Some(ServerMessage::ack(request_id))
        } else {
            None
        }
    }

    async fn handle_update(
        &mut self,
        client_id: &str,
        msg: &ClientMessage,
        volatile: bool,
    ) -> Option<ServerMessage> {
        let request_id = msg.request_id.as_deref().unwrap_or("");
        let path_str = msg.path.as_deref().unwrap_or("/");
        let path = Path::parse(path_str);

        // Reject malformed paths or value keys before any work. Each UPDATE child
        // key is a relative path appended to the base, and each child value's
        // object keys become storage keys — validate the full landing paths and
        // those keys.
        if crate::db::validate_path(path_str).is_err() {
            return self.nack_invalid_key(client_id, request_id);
        }
        if let Some(Value::Object(map)) = &msg.value {
            for (key, val) in map {
                let full = format!("{}/{}", path_str.trim_end_matches('/'), key);
                if crate::db::validate_path(&full).is_err() || validate_value_keys(val).is_err() {
                    return self.nack_invalid_key(client_id, request_id);
                }
            }
        }

        // Check for tainted write (depends on a nacked write) - silently ignore
        if self.is_write_tainted(client_id, &msg.pending_writes) {
            return None; // Silently ignore tainted writes
        }

        // Check for duplicate write (deduplication)
        if self.is_write_processed(client_id, request_id) {
            // Already processed - return ack without doing anything
            if !request_id.is_empty() {
                return Some(ServerMessage::ack(request_id));
            }
            return None;
        }

        // NACK if WAL I/O has failed (non-volatile writes only)
        if !volatile && self.is_wal_failed() {
            self.record_nacked_write(client_id, request_id);
            return Some(ServerMessage::nack(
                request_id,
                error::UNAVAILABLE,
                "Storage unavailable (WAL I/O failure)",
            ));
        }

        let updates = match &msg.value {
            Some(Value::Object(map)) => map.clone(),
            _ => {
                self.record_nacked_write(client_id, request_id);
                return Some(ServerMessage::nack(
                    request_id,
                    error::INVALID_DATA,
                    "update requires an object value",
                ));
            }
        };

        // Validate .value/.priority patterns for each update value
        for (key, value) in &updates {
            let child_path = format!("{}/{}", path_str.trim_end_matches('/'), key);
            if let Err(e) = validate_value_priority(value, &child_path) {
                self.record_nacked_write(client_id, request_id);
                return Some(ServerMessage::nack(request_id, error::INVALID_DATA, &e));
            }
        }

        // Process server values for each update value
        let mut processed_updates = serde_json::Map::new();
        for (key, value) in updates {
            let child_path = format!("{}/{}", path_str.trim_end_matches('/'), key);
            let processed = match process_server_values(value, &child_path, &self.tree) {
                Ok((v, _)) => v,
                Err(e) => {
                    self.record_nacked_write(client_id, request_id);
                    return Some(ServerMessage::nack(request_id, error::INVALID_DATA, &e));
                }
            };
            processed_updates.insert(key, processed);
        }
        let updates = processed_updates;

        // No eager `promote_path` — `can_write` builds a `NewData::Update`
        // and the rules engine constructs `LazyUpdateSnapshot`s on demand.
        // Anything a rule actually reads (`data.*`, sibling-of-write under
        // `newData.*`) goes through `NeedsPromotion` and the retry loop
        // loads exactly that path. Untouched siblings are never fetched.

        // First, check at the UPDATE path level with `NewData::Update` —
        // the snapshot will be built lazily over (tree, path, updates).
        let update_path_allowed = self
            .can_write(
                client_id,
                path_str,
                Some(NewData::from_update(path_str.to_string(), updates.clone())),
            )
            .await;

        if !update_path_allowed {
            // Parent rule didn't grant access, check each child path individually
            // (Firebase allows children to grant their own access even if parent denies)
            for (key, value) in &updates {
                let child_path = format!("{}/{}", path_str.trim_end_matches('/'), key);
                if !self
                    .can_write(
                        client_id,
                        &child_path,
                        Some(NewData::from_set(child_path.clone(), value.clone())),
                    )
                    .await
                {
                    let auth_summary = self.get_auth_summary(client_id);
                    debug!(
                        "NACK {}: UPDATE permission denied at {} for client {} | auth: {}",
                        self.id, child_path, client_id, auth_summary
                    );
                    self.metrics.record_permission_denial();
                    self.record_nacked_write(client_id, request_id);
                    return Some(ServerMessage::nack(
                        request_id,
                        error::PERMISSION_DENIED,
                        "Permission denied",
                    ));
                }
            }
        }

        // Perform update (shallow merge at path).
        //
        // For blob-backed DBs, use `update_lazy` so intermediate nodes that
        // don't yet exist become Sentinels (signal "not loaded") instead of
        // empty Objects (signal "fully loaded"). The non-lazy `tree.update`
        // here would silently turn the parent into a real Object containing
        // only the new keys whenever a prior `promote_path_shallow` had
        // collapsed the parent to Null on PathNotFound — and `promote_path_deep`
        // would then short-circuit reads of the destroyed siblings via its
        // "Object parent → child definitively absent" check, returning Null
        // for data that's still present in the WAL/blob until the next restart.
        //
        // Mirrors the pattern in `handle_set` and `handle_transaction`'s UPDATE
        // arm.
        if self.is_blob_backed() {
            self.tree.write().unwrap().update_lazy(&path, &updates);
            // Track Sentinel intermediates created by each leaf write —
            // `track_sentinels_after_write` walks ancestors of its argument,
            // so passing each leaf path catches the update-path itself
            // (which became a Sentinel container).
            let base = path_str.trim_end_matches('/');
            for key in updates.keys() {
                let leaf_path = format!("{}/{}", base, key);
                self.track_sentinels_after_write(&leaf_path);
            }
        } else {
            self.tree.write().unwrap().update(&path, &updates);
        }

        // Write to WAL for durability (non-volatile writes only, async)
        if !volatile {
            self.wal_write_update(path_str, &updates);
        }

        // Broadcast to subscribers
        self.broadcast_mutation(
            path_str,
            "update",
            None,
            Some(updates),
            volatile,
            Some(client_id),
        )
        .await;

        // Record for deduplication (skip volatile writes)
        if !volatile {
            self.record_processed_write(client_id, request_id);
        }

        // Record write metrics using raw payload size captured at parse time
        self.metrics.record_write(msg.payload_size);

        // Return ack
        if !msg.is_volatile() && !request_id.is_empty() {
            Some(ServerMessage::ack(request_id))
        } else {
            None
        }
    }

    async fn handle_remove(
        &mut self,
        client_id: &str,
        msg: &ClientMessage,
        volatile: bool,
    ) -> Option<ServerMessage> {
        let request_id = msg.request_id.as_deref().unwrap_or("");
        let path_str = msg.path.as_deref().unwrap_or("/");
        let path = Path::parse(path_str);

        // Reject malformed remove paths before any work (same key invariant as
        // SET/UPDATE; a remove can't plant keys but still must not diverge between
        // the rules matcher and storage on empty/odd segments).
        if crate::db::validate_path(path_str).is_err() {
            return self.nack_invalid_key(client_id, request_id);
        }

        // Check for tainted write (depends on a nacked write) - silently ignore
        if self.is_write_tainted(client_id, &msg.pending_writes) {
            return None; // Silently ignore tainted writes
        }

        // Check for duplicate write (deduplication)
        if self.is_write_processed(client_id, request_id) {
            // Already processed - return ack without doing anything
            if !request_id.is_empty() {
                return Some(ServerMessage::ack(request_id));
            }
            return None;
        }

        // NACK if WAL I/O has failed (non-volatile writes only)
        if !volatile && self.is_wal_failed() {
            self.record_nacked_write(client_id, request_id);
            return Some(ServerMessage::nack(
                request_id,
                error::UNAVAILABLE,
                "Storage unavailable (WAL I/O failure)",
            ));
        }

        // Check write permission (remove = write null)
        if !self.can_write(client_id, path_str, None).await {
            let auth_summary = self.get_auth_summary(client_id);
            debug!(
                "NACK {}: DELETE permission denied at {} for client {} | auth: {}",
                self.id, path_str, client_id, auth_summary
            );
            self.metrics.record_permission_denial();
            self.record_nacked_write(client_id, request_id);
            return Some(ServerMessage::nack(
                request_id,
                error::PERMISSION_DENIED,
                "Permission denied",
            ));
        }

        // Remove the value from tree (no need to pre-load from blob for delete)
        self.tree.write().unwrap().remove(&path);
        // Clear sentinel tracking at and below the deleted path — those nodes are gone
        self.remove_sentinel_paths_below(path_str);

        // Write to WAL for durability (non-volatile writes only, async)
        if !volatile {
            self.wal_write_delete(path_str);
        }

        // Broadcast deletion to subscribers
        self.broadcast_mutation(path_str, "remove", None, None, volatile, Some(client_id))
            .await;

        // Record for deduplication (skip volatile writes)
        if !volatile {
            self.record_processed_write(client_id, request_id);
        }

        // Record write metrics (remove is 0 bytes)
        self.metrics.record_write(0);

        // Return ack
        if !request_id.is_empty() {
            Some(ServerMessage::ack(request_id))
        } else {
            None
        }
    }

    // =========================================================================
    // Subscribe/Unsubscribe
    // =========================================================================

    async fn handle_subscribe(
        &mut self,
        client_id: &str,
        msg: &ClientMessage,
    ) -> Option<ServerMessage> {
        let request_id = msg.request_id.as_deref().unwrap_or("");
        let path_str = msg.path.as_deref().unwrap_or("/");
        let path = Path::parse(path_str);

        // Reject malformed read paths so the rules matcher and the tree can't
        // tokenize them differently. (No write impact, but keeps read-side auth
        // consistent with the write paths.)
        if crate::db::validate_path(path_str).is_err() {
            return Some(ServerMessage::nack(
                request_id,
                error::INVALID_DATA,
                "invalid path",
            ));
        }

        // Parse query parameters (only build rules query HashMap if rules use query.*)
        let query_params = QueryParams::from_message(msg);
        let rules_query = if self.rules_use_query() {
            query_params.as_ref().map(|qp| qp.to_rules_query())
        } else {
            None
        };

        // Check read permission (with query context for query-based rules)
        if !self.can_read(client_id, path_str, rules_query).await {
            let auth_summary = self.get_auth_summary(client_id);
            debug!(
                "NACK {}: SUBSCRIBE permission denied at {} for client {} | auth: {}",
                self.id, path_str, client_id, auth_summary
            );
            self.metrics.record_permission_denial();
            return Some(ServerMessage::nack(
                request_id,
                error::PERMISSION_DENIED,
                "Permission denied",
            ));
        }

        // Pre-check: if the path needs promotion and the blob subtree is massive,
        // reject before loading into memory.
        if self.has_sentinel_at_or_below(path_str)
            && self.blob_subtree_exceeds_limit(path_str).await
        {
            self.metrics.record_size_rejection();
            return Some(ServerMessage::nack(
                request_id,
                error::RESPONSE_TOO_LARGE,
                "Subtree too large to read",
            ));
        }

        // Deep promote: ensure the entire subtree is Sentinel-free.
        // Subscribe sends the full snapshot to the client, so all descendants must be real.
        if let Err(e) = self.promote_path_deep(path_str).await {
            warn!("NACK SUBSCRIBE {}: promotion failed: {}", path_str, e);
            return Some(ServerMessage::nack(
                request_id,
                error::UNAVAILABLE,
                &format!("Failed to load data for subscription: {}", e),
            ));
        }

        // Get client connection for storing in the subscription
        let conn = match self.clients.get(client_id) {
            Some(client) => client.conn.clone(),
            None => {
                return Some(ServerMessage::nack(
                    request_id,
                    error::INTERNAL,
                    "Client not found",
                ));
            }
        };

        // Add subscription via view manager
        let query_id =
            match self
                .view_manager
                .subscribe(client_id, path_str, query_params.as_ref(), conn)
            {
                Ok(id) => id,
                Err(SubscribeError::Query(QueryError::LimitTooLarge(n))) => {
                    return Some(ServerMessage::nack(
                        request_id,
                        error::INVALID_DATA,
                        &format!("Query limit {} exceeds maximum allowed (10000)", n),
                    ));
                }
                Err(SubscribeError::TooManySubscriptions { limit }) => {
                    debug!(
                        "NACK {}: SUBSCRIBE rejected for client {} — at subscription cap ({})",
                        self.id, client_id, limit
                    );
                    return Some(ServerMessage::nack(
                        request_id,
                        error::TOO_MANY_SUBSCRIPTIONS,
                        &format!("subscription limit reached ({} per connection)", limit),
                    ));
                }
            };

        // Get initial value for snapshot
        // OPTIMIZATION: Use ArcValue directly to avoid to_value() copy in all cases.
        // OPTIMIZATION: If another client already subscribed to this exact query,
        // reuse the cached ordered_keys instead of re-sorting.
        let (arc_value, tag, keys) = if let Some(params) = &query_params {
            // Query subscription - check if we can reuse cached keys from shared view
            let cached_keys = self
                .view_manager
                .get_shared_view(path_str, &query_id)
                .filter(|v| !v.ordered_keys.is_empty())
                .map(|v| v.ordered_keys.clone());

            if let Some(keys) = cached_keys {
                // Reuse cached keys - skip expensive sorting!
                let arc_value = self.get_result_from_cached_keys(&path, &keys);
                (arc_value, params.tag, None) // None = don't re-initialize
            } else {
                // First subscriber - compute full query result
                let query = match params.to_query() {
                    Ok(q) => q,
                    Err(e) => {
                        self.view_manager
                            .unsubscribe_with_query(client_id, path_str, &query_id);
                        return Some(ServerMessage::nack(
                            request_id,
                            error::INVALID_DATA,
                            &format!("Invalid query: {:?}", e),
                        ));
                    }
                };
                let (arc_value, keys) = self.get_query_result_with_keys(&path, &query);
                (arc_value, params.tag, Some(keys))
            }
        } else {
            // Simple subscription - use ArcValue directly (avoids to_value() conversion)
            let arc_value = self
                .tree
                .read()
                .unwrap()
                .get_arc(&path)
                .unwrap_or(ArcValue::Null);
            (arc_value, None, None)
        };

        // Check response size limit (256MB for all clients)
        let estimated_size = arc_value.estimate_size() as usize;
        if estimated_size > crate::protocol::MAX_RESPONSE_SIZE {
            // Remove the subscription we just added
            self.view_manager
                .unsubscribe_with_query(client_id, path_str, &query_id);
            self.metrics.record_size_rejection();
            return Some(ServerMessage::nack(
                request_id,
                error::RESPONSE_TOO_LARGE,
                &format!(
                    "Initial snapshot size {} exceeds maximum allowed ({} bytes)",
                    estimated_size,
                    crate::protocol::MAX_RESPONSE_SIZE
                ),
            ));
        }

        // Update subscription count metric
        self.metrics
            .set_subscriptions(self.view_manager.subscription_count() as u32);

        // Initialize query view with ordered keys (if query subscription)
        if let Some(keys) = keys {
            self.view_manager
                .initialize_query_view(client_id, path_str, &query_id, keys);
        }

        let mut event_msg = ServerMessage::put_event_arc(path_str, "/", arc_value, false);
        if let Some(tag) = tag {
            event_msg.tag = Some(tag);
        }

        self.send_to_client(client_id, &event_msg, false).await;

        // Now send ack
        Some(ServerMessage::ack(request_id))
    }

    fn handle_unsubscribe(
        &mut self,
        client_id: &str,
        msg: &ClientMessage,
    ) -> Option<ServerMessage> {
        let request_id = msg.request_id.as_deref().unwrap_or("");
        let path_str = msg.path.as_deref().unwrap_or("/");

        // Parse query params to get the correct query ID
        let query_params = QueryParams::from_message(msg);
        let query_id = query_params
            .as_ref()
            .map(|p| p.identifier())
            .unwrap_or_else(|| "default".to_string());

        // Remove subscription from view manager
        self.view_manager
            .unsubscribe_with_query(client_id, path_str, &query_id);

        // Update subscription count metric
        self.metrics
            .set_subscriptions(self.view_manager.subscription_count() as u32);

        Some(ServerMessage::ack(request_id))
    }

    // =========================================================================
    // Once (single read)
    // =========================================================================

    async fn handle_once(&mut self, client_id: &str, msg: &ClientMessage) -> Option<ServerMessage> {
        let request_id = msg.request_id.as_deref().unwrap_or("");
        let path_str = msg.path.as_deref().unwrap_or("/");
        let path = Path::parse(path_str);

        // Reject malformed read paths (see handle_subscribe).
        if crate::db::validate_path(path_str).is_err() {
            return Some(ServerMessage::nack(
                request_id,
                error::INVALID_DATA,
                "invalid path",
            ));
        }

        // Only build query HashMap if rules reference query.*
        let rules_query = if self.rules_use_query() {
            QueryParams::from_message(msg).map(|qp| qp.to_rules_query())
        } else {
            None
        };

        // Check read permission (with query context for query-based rules)
        if !self.can_read(client_id, path_str, rules_query).await {
            let auth_summary = self.get_auth_summary(client_id);
            debug!(
                "NACK {}: ONCE permission denied at {} for client {} | auth: {}",
                self.id, path_str, client_id, auth_summary
            );
            self.metrics.record_permission_denial();
            return Some(ServerMessage::nack(
                request_id,
                error::PERMISSION_DENIED,
                "Permission denied",
            ));
        }

        // Shallow read: return only immediate child keys as {"key": true, ...}
        // without loading any child data from the blob.
        if msg.shallow == Some(true) {
            return self.handle_once_shallow(request_id, path_str, &path).await;
        }

        // Pre-check: if the path needs promotion and the blob subtree is massive,
        // reject before loading into memory.
        if self.has_sentinel_at_or_below(path_str)
            && self.blob_subtree_exceeds_limit(path_str).await
        {
            self.metrics.record_size_rejection();
            return Some(ServerMessage::nack(
                request_id,
                error::RESPONSE_TOO_LARGE,
                "Subtree too large to read",
            ));
        }

        // Deep promote: ensure the entire subtree is Sentinel-free.
        // ONCE sends the full value to the client, so all descendants must be real.
        if let Err(e) = self.promote_path_deep(path_str).await {
            warn!("NACK ONCE {}: promotion failed: {}", path_str, e);
            return Some(ServerMessage::nack(
                request_id,
                error::UNAVAILABLE,
                &format!("Failed to load data for read: {}", e),
            ));
        }

        // Parse query parameters
        let query_params = QueryParams::from_message(msg);

        // OPTIMIZATION: Use ArcValue directly to avoid to_value() copy in all cases.
        let arc_value = if let Some(params) = &query_params {
            // Validate and convert query params
            let query = match params.to_query() {
                Ok(q) => q,
                Err(QueryError::LimitTooLarge(n)) => {
                    return Some(ServerMessage::nack(
                        request_id,
                        error::INVALID_DATA,
                        &format!("Query limit {} exceeds maximum allowed (10000)", n),
                    ));
                }
            };
            // Query read - apply filtering, returns ArcValue directly (O(1) child clones)
            let (arc_value, _keys) = self.get_query_result_with_keys(&path, &query);
            arc_value
        } else {
            // Simple read - use ArcValue directly (avoids to_value() conversion)
            self.tree
                .read()
                .unwrap()
                .get_arc(&path)
                .unwrap_or(ArcValue::Null)
        };

        // Check response size limit (256MB for all clients)
        let estimated_size = arc_value.estimate_size() as usize;
        if estimated_size > crate::protocol::MAX_RESPONSE_SIZE {
            self.metrics.record_size_rejection();
            return Some(ServerMessage::nack(
                request_id,
                error::RESPONSE_TOO_LARGE,
                &format!(
                    "Response size {} exceeds maximum allowed ({} bytes)",
                    estimated_size,
                    crate::protocol::MAX_RESPONSE_SIZE
                ),
            ));
        }

        // Record read operation (bytes tracked in send_to_client)
        self.metrics.record_read();

        Some(ServerMessage::once_response_arc(request_id, arc_value))
    }

    /// Handle a shallow once read.
    ///
    /// Returns a map of immediate children at the given path. Each child value is:
    /// - **Primitive child**: the actual value (string, number, bool, null)
    /// - **Container child**: `{".sz": <byte_size>}` — the proxy can convert this
    ///   to `true` for Firebase REST clients or keep the size for Lark v2 clients.
    ///
    /// If the path itself is a primitive, returns the value directly.
    ///
    /// For blob-backed databases, uses `BlobSession::read_shallow` which reads only
    /// the container header + child index (plus tiny reads for primitive child values).
    /// No full subtree data is ever loaded.
    async fn handle_once_shallow(
        &mut self,
        request_id: &str,
        path_str: &str,
        path: &Path,
    ) -> Option<ServerMessage> {
        // Helper: build {".sz": size} marker for a container child.
        fn size_marker(size: u64) -> ArcValue {
            let mut m = HashMap::new();
            m.insert(".sz".to_string(), ArcValue::from(size as i64));
            ArcValue::Object(Arc::new(m))
        }

        // Helper: convert a serde_json::Value to its shallow representation.
        // Primitives → ArcValue of that primitive. Objects/arrays → size marker.
        fn shallow_from_json(val: &Value) -> ArcValue {
            match val {
                Value::Object(_) | Value::Array(_) => {
                    let arc = ArcValue::from_value(val.clone());
                    size_marker(arc.estimate_size() as u64)
                }
                _ => ArcValue::from_value(val.clone()),
            }
        }

        let mut children: HashMap<String, ArcValue> = HashMap::new();

        // Check if the data is already in the tree (non-Sentinel)
        let tree_has_data = {
            let tree = self.tree.read().unwrap();
            match tree.get(path) {
                Some(node) if !node.is_sentinel() => {
                    if !node.is_object() && !node.is_array() {
                        // Path is a primitive in the tree — return it directly
                        let val = node.clone();
                        drop(tree);
                        self.metrics.record_read();
                        return Some(ServerMessage::once_response_arc(request_id, val));
                    }
                    // Container — build shallow map from children
                    for key in node.keys() {
                        if let Some(child) = node.get(key) {
                            let shallow_val = if child.is_object() || child.is_array() {
                                size_marker(child.estimate_size() as u64)
                            } else {
                                child.clone()
                            };
                            children.insert(key.to_string(), shallow_val);
                        }
                    }
                    true
                }
                _ => false,
            }
        };

        if !tree_has_data {
            if self.blob_session.is_some() {
                let segments: Vec<&str> = path.segments().iter().map(|s| s.as_ref()).collect();
                let blob_path = if path.is_root() { vec![] } else { segments };

                let blob_result = {
                    // Inner scope keeps the borrow short so it drops before the
                    // `&mut self` uses below; an outer `if let` would extend it.
                    #[allow(clippy::unnecessary_unwrap)]
                    let session = self.blob_session.as_ref().unwrap();
                    session.read_shallow(&blob_path).await
                };

                match blob_result {
                    Ok(ShallowValue::Primitive(val)) => {
                        // Path is a primitive in the blob — return directly
                        self.metrics.record_read();
                        return Some(ServerMessage::once_response_arc(request_id, val));
                    }
                    Ok(ShallowValue::Children(blob_children)) => {
                        for child in blob_children {
                            let val = match child.value {
                                Some(prim) => prim,              // primitive value
                                None => size_marker(child.size), // container → {".sz": size}
                            };
                            children.insert(child.key, val);
                        }
                    }
                    Err(BlobError::PathNotFound(_)) => {
                        // Path doesn't exist in blob — children stays empty,
                        // WAL entries below may still add children
                    }
                    Err(e) => {
                        warn!(
                            "NACK shallow ONCE {}: blob read_shallow failed: {}",
                            path_str, e
                        );
                        return Some(ServerMessage::nack(
                            request_id,
                            error::UNAVAILABLE,
                            &format!("Failed to read shallow data: {}", e),
                        ));
                    }
                }

                // Merge with pending WAL entries: find entries that affect direct
                // children of the target path.
                let path_prefix = if path_str == "/" {
                    "/".to_string()
                } else {
                    format!("{}/", path_str)
                };
                for entry in &self.pending_wal_entries {
                    if let Some(remainder) = entry.path.strip_prefix(path_prefix.as_str()) {
                        // Direct child: remainder has no more slashes
                        if !remainder.contains('/') && !remainder.is_empty() {
                            match entry.op {
                                WalOp::Set => {
                                    // SET with None == SET-to-null == delete.
                                    // Modern WALs canonicalize this to
                                    // `WalOp::Delete`; this handles old entries
                                    // and stays defensive against the encoding.
                                    match &entry.value {
                                        Some(val) if !val.is_null() => {
                                            children.insert(
                                                remainder.to_string(),
                                                shallow_from_json(val),
                                            );
                                        }
                                        _ => {
                                            children.remove(remainder);
                                        }
                                    }
                                }
                                WalOp::Update => {
                                    if let Some(val) = &entry.value {
                                        children
                                            .insert(remainder.to_string(), shallow_from_json(val));
                                    }
                                }
                                WalOp::Delete => {
                                    children.remove(remainder);
                                }
                            }
                        }
                        // Deeper descendant (e.g. /users/alice/score): the first
                        // segment is a container child that must exist.
                        else if let Some(child_key) = remainder.split('/').next()
                            && !child_key.is_empty()
                        {
                            match entry.op {
                                WalOp::Set | WalOp::Update => {
                                    // We know this child is a container, but we don't
                                    // have the full size. Use 0 to indicate "container,
                                    // size unknown" — only overwrites if key wasn't
                                    // already present from the blob (which has accurate size).
                                    children
                                        .entry(child_key.to_string())
                                        .or_insert_with(|| size_marker(0));
                                }
                                WalOp::Delete => {
                                    // Deleting a descendant doesn't remove the child —
                                    // it may still have other children.
                                }
                            }
                        }
                    }
                    // An exact-path SET replaces the node entirely.
                    else if entry.path == path_str {
                        match entry.op {
                            WalOp::Set => {
                                children.clear();
                                if let Some(value) = &entry.value {
                                    if let Some(obj) = value.as_object() {
                                        for (key, val) in obj {
                                            children.insert(key.clone(), shallow_from_json(val));
                                        }
                                    }
                                    // SET to a non-object (leaf) — return it directly
                                    if !value.is_object() && !value.is_array() {
                                        self.metrics.record_read();
                                        return Some(ServerMessage::once_response_arc(
                                            request_id,
                                            ArcValue::from_value(value.clone()),
                                        ));
                                    }
                                }
                            }
                            WalOp::Update => {
                                if let Some(value) = &entry.value
                                    && let Some(obj) = value.as_object()
                                {
                                    for (key, val) in obj {
                                        if val.is_null() {
                                            children.remove(key);
                                        } else {
                                            children.insert(key.clone(), shallow_from_json(val));
                                        }
                                    }
                                }
                            }
                            WalOp::Delete => {
                                children.clear();
                            }
                        }
                    }
                }
            } else {
                // Not blob-backed — promote (shallow) and read from tree
                if let Err(e) = self.promote_path(path_str).await {
                    warn!("NACK shallow ONCE {}: promotion failed: {}", path_str, e);
                    return Some(ServerMessage::nack(
                        request_id,
                        error::UNAVAILABLE,
                        &format!("Failed to load data for read: {}", e),
                    ));
                }
                let tree = self.tree.read().unwrap();
                if let Some(node) = tree.get(path) {
                    if !node.is_object() && !node.is_array() {
                        let val = node.clone();
                        drop(tree);
                        self.metrics.record_read();
                        return Some(ServerMessage::once_response_arc(request_id, val));
                    }
                    for key in node.keys() {
                        if let Some(child) = node.get(key) {
                            let shallow_val = if child.is_object() || child.is_array() {
                                size_marker(child.estimate_size() as u64)
                            } else {
                                child.clone()
                            };
                            children.insert(key.to_string(), shallow_val);
                        }
                    }
                }
            }
        }

        // Build the response
        let shallow_value = if children.is_empty() {
            ArcValue::Null
        } else {
            ArcValue::Object(Arc::new(children))
        };

        self.metrics.record_read();
        Some(ServerMessage::once_response_arc(request_id, shallow_value))
    }

    /// Apply a query to get filtered/sorted results and the ordered keys.
    ///
    /// OPTIMIZATION: Uses lightweight SortEntry to filter/sort first, then only
    /// fetches full values for keys that pass the query. This avoids calling
    /// to_value() on children that will be filtered out.
    ///
    /// Returns (value, ordered_keys) where ordered_keys is the list of keys in sorted order.
    /// Get query result with ordered keys.
    /// OPTIMIZATION: Returns ArcValue directly, using O(1) Arc clones for child values.
    fn get_query_result_with_keys(
        &self,
        path: &Path,
        query: &crate::db::query::Query,
    ) -> (ArcValue, Vec<String>) {
        use crate::db::query::{SortEntry, apply_query_to_sort_entries};
        use std::sync::Arc;

        let tree = self.tree.read().unwrap();
        let node = match tree.get(path) {
            Some(n) => n,
            None => return (ArcValue::Null, Vec::new()),
        };

        // Get children keys
        let children_keys: Vec<String> = node.keys().map(|s| s.to_string()).collect();

        if children_keys.is_empty() {
            // Not an object node, return the value directly (O(1) clone)
            return (node.clone(), Vec::new());
        }

        // Build lightweight sort entries (key + sort value only, no full value copy)
        let sort_entries: Vec<SortEntry> = children_keys
            .iter()
            .filter_map(|key| {
                let child = node.get(key)?;
                // Only extract sort value, not full value
                let sort_value = child.get_sort_value(&query.order_by);
                Some(SortEntry::new(key.clone(), sort_value))
            })
            .collect();

        // Apply query to get filtered/sorted keys
        let filtered_keys = apply_query_to_sort_entries(sort_entries, query);

        // Now fetch full values only for keys in the result
        // OPTIMIZATION: Build ArcValue::Object using O(1) child clones instead of to_value()
        if filtered_keys.is_empty() {
            (ArcValue::Null, Vec::new())
        } else {
            let mut result = HashMap::new();
            for key in &filtered_keys {
                if let Some(child) = node.get(key) {
                    // O(1) Arc clone instead of O(n) to_value()
                    result.insert(key.clone(), child.clone());
                }
            }
            (ArcValue::Object(Arc::new(result)), filtered_keys)
        }
    }

    /// Get query result using pre-computed keys (from a shared view).
    /// This avoids re-sorting when another client already computed the result.
    fn get_result_from_cached_keys(&self, path: &Path, keys: &[String]) -> ArcValue {
        use std::sync::Arc;

        if keys.is_empty() {
            return ArcValue::Null;
        }

        let tree = self.tree.read().unwrap();
        let node = match tree.get(path) {
            Some(n) => n,
            None => return ArcValue::Null,
        };

        let mut result = HashMap::new();
        for key in keys {
            if let Some(child) = node.get(key) {
                result.insert(key.clone(), child.clone());
            }
        }

        if result.is_empty() {
            ArcValue::Null
        } else {
            ArcValue::Object(Arc::new(result))
        }
    }

    // =========================================================================
    // OnDisconnect
    // =========================================================================

    async fn handle_on_disconnect(
        &mut self,
        client_id: &str,
        msg: &ClientMessage,
    ) -> Option<ServerMessage> {
        let request_id = msg.request_id.as_deref().unwrap_or("");
        let path_str = msg.path.as_deref().unwrap_or("/");
        let action = msg.action.as_deref().unwrap_or("s");

        match action {
            "s" | "u" | "d" => {
                // Deferred writes are applied directly to the tree + WAL on
                // disconnect (handle_disconnect), bypassing the live write
                // handlers — so the same checks must happen here, at registration:

                // 1. Path/key validity (empty/odd segments, control chars,
                //    `$ # [ ] /`, literal-slash value keys, >768-byte keys).
                let keys_ok = crate::db::validate_path(path_str).is_ok()
                    && match (action, &msg.value) {
                        ("s", Some(v)) => validate_value_keys(v).is_ok(),
                        ("u", Some(Value::Object(map))) => map.iter().all(|(k, val)| {
                            crate::db::validate_path(&format!(
                                "{}/{}",
                                path_str.trim_end_matches('/'),
                                k
                            ))
                            .is_ok()
                                && validate_value_keys(val).is_ok()
                        }),
                        _ => true,
                    };
                if !keys_ok {
                    return Some(ServerMessage::nack(
                        request_id,
                        error::INVALID_DATA,
                        "invalid path or key",
                    ));
                }

                // 2. Security rules. Evaluate onDisconnect writes
                //    against rules when they're established, using the
                //    registering client's auth — do the same so a deferred write
                //    can't reach a path the client isn't allowed to write.
                let new_data = match (action, msg.value.clone()) {
                    ("s", Some(v)) => Some(NewData::from_set(path_str.to_string(), v)),
                    ("u", Some(Value::Object(map))) => {
                        Some(NewData::from_update(path_str.to_string(), map))
                    }
                    _ => None,
                };
                if !self.can_write(client_id, path_str, new_data).await {
                    self.metrics.record_permission_denial();
                    return Some(ServerMessage::nack(
                        request_id,
                        error::PERMISSION_DENIED,
                        "write permission denied",
                    ));
                }

                // Bound the per-client onDisconnect state — both action count
                // and aggregate payload bytes. These live in memory until the
                // client disconnects, so an unbounded client is an asymmetric
                // memory sink whose OOM aborts the whole core (audit M-3).
                let new_bytes = path_str.len()
                    + action.len()
                    + msg.value.as_ref().map_or(0, estimate_value_bytes);
                let (existing_count, existing_bytes) =
                    self.on_disconnect.get(client_id).map_or((0, 0), |actions| {
                        let bytes: usize = actions
                            .iter()
                            .map(|a| {
                                a.path.len()
                                    + a.action.len()
                                    + a.value.as_ref().map_or(0, estimate_value_bytes)
                            })
                            .sum();
                        (actions.len(), bytes)
                    });
                if existing_count >= MAX_ON_DISCONNECT_ACTIONS_PER_CLIENT
                    || existing_bytes + new_bytes > MAX_ON_DISCONNECT_BYTES_PER_CLIENT
                {
                    debug!(
                        "NACK {}: onDisconnect rejected for client {} — at cap ({} actions / {} bytes)",
                        self.id, client_id, existing_count, existing_bytes
                    );
                    return Some(ServerMessage::nack(
                        request_id,
                        error::PAYLOAD_TOO_LARGE,
                        &format!(
                            "onDisconnect limit reached ({} actions or {} bytes per connection)",
                            MAX_ON_DISCONNECT_ACTIONS_PER_CLIENT,
                            MAX_ON_DISCONNECT_BYTES_PER_CLIENT
                        ),
                    ));
                }

                let disconnect_action = DisconnectAction {
                    path: path_str.to_string(),
                    action: action.to_string(),
                    value: msg.value.clone(),
                };

                self.on_disconnect
                    .entry(client_id.to_string())
                    .or_default()
                    .push(disconnect_action);
            }
            "c" => {
                // Cancel - remove disconnect hooks for this path
                if let Some(actions) = self.on_disconnect.get_mut(client_id) {
                    actions.retain(|a| a.path != path_str);
                }
            }
            _ => {}
        }

        Some(ServerMessage::ack(request_id))
    }

    // =========================================================================
    // Event Broadcasting
    // =========================================================================

    /// Broadcast a mutation to all affected views.
    async fn broadcast_mutation(
        &mut self,
        path: &str,
        mutation_type: &str,
        new_value: Option<Value>,
        updates: Option<serde_json::Map<String, Value>>,
        volatile: bool,
        writer_client_id: Option<&str>,
    ) {
        let event = MutationEvent {
            mutation_type: mutation_type.to_string(),
            path: path.to_string(),
            old_value: None, // We don't track old values for now
            new_value,
            updates,
            volatile,
            writer_client_id: writer_client_id.map(|s| s.to_string()),
        };

        // OPTIMIZATION: Send events directly to subscribers without creating ClientEvent objects.
        // This eliminates:
        // - 100k message clones (in high-fanout scenarios)
        // - 100k ClientEvent allocations/deallocations
        // - 100k HashMap lookups (connections are stored in subscribers)
        //
        // Rate limiting is done at the VIEW level inside send_events.
        //
        // FAIRNESS: Process views in batches of 10, yielding between batches.
        // This prevents a database with many unique views (e.g., 200k CCU with different
        // query params) from starving other databases on the same core.
        const VIEWS_PER_BATCH: usize = 10;

        // 1. Collect affected views (quick, needs tree briefly)
        let view_infos = self.view_manager.collect_affected_view_infos(&event);

        // 2. Deep promote view paths for query views that may need to recompute.
        //    recompute_query_view reads all children from the tree, so the entire
        //    subtree must be Sentinel-free.
        for info in &view_infos {
            if info.has_query {
                let _ = self.promote_path_deep(&info.path).await;
            }
        }

        // 3. Process in batches, yielding between
        let mut event_count = 0;
        for (batch_idx, chunk) in view_infos.chunks(VIEWS_PER_BATCH).enumerate() {
            // Acquire lock only for this batch
            let batch_sent = {
                let tree = self.tree.read().unwrap();
                self.view_manager
                    .send_events_for_views(chunk, &event, &tree)
            }; // Lock released before yield
            event_count += batch_sent;

            // Yield after each batch (except the first) to allow other tasks to run
            if batch_idx > 0 {
                glommio::yield_if_needed().await;
            }
        }

        // Record events sent
        if event_count > 0 {
            self.metrics.record_events_sent(event_count as u64);
        }
    }

    async fn send_to_client(&self, client_id: &str, msg: &ServerMessage, volatile: bool) {
        let client = match self.clients.get(client_id) {
            Some(c) => c,
            None => return,
        };

        match msg.encode() {
            Ok(data) => {
                // Record outbound bytes (not read count - this includes events, acks, etc.)
                self.metrics.record_outbound_bytes(data.len());

                // Use try_send to avoid blocking the database task if client is slow
                if let Err(e) = client.conn.try_send(data.into(), volatile, false) {
                    trace!(
                        "Failed to send to client {} (dropping message): {:?}",
                        client_id, e
                    );
                }
            }
            Err(e) => {
                // Encoding failed. The only known trigger is an ArcValue::Sentinel
                // leaking into a response, but treat this as a generic internal
                // error. If the original message was a response to a request (has
                // a request_id), convert to a NACK so the client fails fast rather
                // than waiting for a response that will never come. Pure events
                // (put/patch deltas) have no request_id — log loudly and drop.
                //
                // Diagnostic: if the message carries an ArcValue payload, walk it
                // to find the offending Sentinel's path so we know exactly which
                // node leaked. This is server-side only — the client NACK stays
                // generic.
                let sentinel_path = [&msg.value, &msg.once_value]
                    .iter()
                    .find_map(|opt| match opt.as_ref() {
                        Some(crate::protocol::MessageValue::Arc(v)) => v.find_first_sentinel_path(),
                        _ => None,
                    });
                let req_path = msg.path.as_deref().unwrap_or("");
                let req_id = msg.request_id().unwrap_or("");
                warn!(
                    "Database {} failed to encode message for client {} (req_id={}, req_path={:?}, sentinel_at={:?}): {}",
                    self.id, client_id, req_id, req_path, sentinel_path, e
                );
                if let Some(request_id) = msg.request_id() {
                    let nack =
                        ServerMessage::nack(request_id, error::INTERNAL, "Internal encoding error");
                    match nack.encode() {
                        Ok(data) => {
                            let _ = client.conn.try_send(data.into(), volatile, false);
                        }
                        Err(nack_err) => {
                            warn!(
                                "Database {} also failed to encode NACK for client {}: {}",
                                self.id, client_id, nack_err
                            );
                        }
                    }
                }
            }
        }
    }

    // =========================================================================
    // Volatile Path Helpers
    // =========================================================================

    /// Check if the current rules reference query.* variables.
    fn rules_use_query(&self) -> bool {
        self.evaluator.as_ref().is_some_and(|e| e.uses_query())
    }

    /// Check if a path is configured as volatile.
    fn is_volatile_path(&self, path: &str) -> bool {
        for pattern in &self.volatile_paths {
            if path_matches_pattern(path, pattern) {
                return true;
            }
        }
        false
    }

    /// Flush volatile batches for high-frequency clients (KCP/WebTransport) - 20Hz.
    fn flush_volatile_fast(&mut self) {
        if !self.view_manager.has_pending_volatile() {
            return;
        }

        // Send directly via stored connections
        let event_count = self.view_manager.flush_volatile_fast();

        // Record events sent
        if event_count > 0 {
            self.metrics.record_events_sent(event_count as u64);
        }
    }

    /// Flush volatile batches for slow clients (WebSocket) - 4Hz.
    /// This also clears the batch after sending.
    fn flush_volatile_slow(&mut self) {
        if !self.view_manager.has_pending_volatile() {
            return;
        }

        // Send directly via stored connections and clear batch
        let event_count = self.view_manager.flush_volatile_slow();

        // Record events sent
        if event_count > 0 {
            self.metrics.record_events_sent(event_count as u64);
        }
    }

    // =========================================================================
    // WAL (Write-Ahead Log) Operations - Async
    // =========================================================================

    /// Check if WAL I/O has failed. When true, all writes must be NACKed.
    fn is_wal_failed(&self) -> bool {
        self.wal_failed
    }

    /// Mark WAL as failed. Called on first I/O error.
    fn set_wal_failed(&mut self) {
        if !self.wal_failed {
            self.wal_failed = true;
            error!(
                "[STORAGE INTEGRITY] {}: WAL I/O failure detected. All writes will be NACKed until recovery.",
                self.id
            );
        }
    }

    /// Attempt to recover WAL after failure.
    /// Tries a test write + sync. If successful, clears the failure flag.
    async fn try_recover_wal(&mut self) {
        if !self.wal_failed {
            return;
        }

        if let Some(ref mut writer) = self.wal_writer {
            // Attempt a test write (no-op WAL entry) + sync
            let test_entry = WalEntry::set("/__wal_recovery_test", Value::Bool(true));
            match writer.append_one(&test_entry) {
                Ok(_) => match writer.sync().await {
                    Ok(_) => {
                        self.wal_failed = false;
                        self.wal_dirty = false;
                        self.wal_pending_entries = 0;
                        self.wal_pending_bytes = 0;
                        info!(
                            "[STORAGE INTEGRITY] {}: WAL recovered. Resuming normal write operations.",
                            self.id
                        );
                    }
                    Err(e) => {
                        debug!(
                            "[STORAGE INTEGRITY] {}: WAL recovery sync failed (will retry): {}",
                            self.id, e
                        );
                    }
                },
                Err(e) => {
                    debug!(
                        "[STORAGE INTEGRITY] {}: WAL recovery write failed (will retry): {}",
                        self.id, e
                    );
                }
            }
        }
    }

    /// Notify the per-core storage worker that a WAL file was rotated and is ready for compaction.
    async fn notify_compaction(&self) {
        if let (Some(tx), Some(data_dir), Some(session)) =
            (&self.compaction_tx, &self.data_dir, &self.blob_session)
        {
            // Write .compaction-queue marker for the external compactor binary.
            self.write_compaction_queue_marker().await;

            // Clone the CachedIO via clone_for_reading — shares the Rc-backed byte cache
            // so StorageWorker writes are immediately visible to our reads (write-through).
            let cached_io = match session.io().clone_for_reading().await {
                Ok(io) => io,
                Err(e) => {
                    warn!(
                        "[Persistence] {}: Failed to clone CachedIO for storage worker: {}",
                        self.id, e
                    );
                    return;
                }
            };

            let request = CompactionRequest {
                data_dir: data_dir.clone(),
                database_id: self.id.clone(),
                inbox_sender: self.inbox_sender.clone(),
                cached_io,
            };
            match tx.try_send(StorageWorkerMessage::Compact(request)) {
                Ok(_) => {
                    info!(
                        "[Persistence] {}: Sent compaction request to storage worker",
                        self.id
                    );
                }
                Err(_) => {
                    warn!(
                        "[Persistence] {}: Compaction channel full, skipping notification",
                        self.id
                    );
                }
            }
        }
    }

    /// Notify the StorageWorker to clean up cached state for this database.
    fn notify_storage_worker_shutdown(&self) {
        if let Some(tx) = &self.compaction_tx {
            let _ = tx.try_send(StorageWorkerMessage::Shutdown {
                database_id: self.id.clone(),
            });
        }
    }

    /// Write a SET operation to the WAL (in-memory buffer only, no I/O).
    /// Returns false if serialization failed (caller should NACK).
    fn wal_write_set(&mut self, path: &str, value: &Value) -> bool {
        // Canonicalize SET-to-null as DELETE so the WAL has a single encoding
        // for "this path is gone." Without this, `WalEntry::set(path, Null)`
        // serializes as `{"o":"s","v":null}`; serde then deserializes the null
        // as `Option::None` on read, which the SET arm of the WAL-replay loops
        // silently skipped — so the deletion vanished on restart.
        if value.is_null() {
            return self.wal_write_delete(path);
        }
        if let Some(ref mut writer) = self.wal_writer {
            let mut entry = WalEntry::set(path, value.clone());
            entry.sequence = writer.sequence();
            match writer.append_one(&entry) {
                Ok(_) => {
                    self.wal_dirty = true;
                    self.wal_pending_entries += 1;
                    self.wal_pending_bytes += writer.bytes_written_last_append();
                    let idx = self.pending_wal_entries.len();
                    self.wal_index.add(&entry.path, idx);
                    self.pending_wal_entries.push(entry);
                    true
                }
                Err(e) => {
                    self.set_wal_failed();
                    error!(
                        "[STORAGE INTEGRITY] {}: WAL write failed for SET {}: {}",
                        self.id, path, e
                    );
                    false
                }
            }
        } else {
            true // No WAL writer (ephemeral) - always succeeds
        }
    }

    /// Write an UPDATE operation to the WAL (async).
    /// Write an UPDATE operation to the WAL (in-memory buffer only, no I/O).
    /// Returns false if serialization failed (caller should NACK).
    fn wal_write_update(&mut self, path: &str, updates: &serde_json::Map<String, Value>) -> bool {
        if let Some(ref mut writer) = self.wal_writer {
            let mut entry = WalEntry::update(path, Value::Object(updates.clone()));
            entry.sequence = writer.sequence();
            match writer.append_one(&entry) {
                Ok(_) => {
                    self.wal_dirty = true;
                    self.wal_pending_entries += 1;
                    self.wal_pending_bytes += writer.bytes_written_last_append();
                    let idx = self.pending_wal_entries.len();
                    self.wal_index.add(&entry.path, idx);
                    self.pending_wal_entries.push(entry);
                    true
                }
                Err(e) => {
                    self.set_wal_failed();
                    error!(
                        "[STORAGE INTEGRITY] {}: WAL write failed for UPDATE {}: {}",
                        self.id, path, e
                    );
                    false
                }
            }
        } else {
            true // No WAL writer (ephemeral) - always succeeds
        }
    }

    /// Write a DELETE operation to the WAL (in-memory buffer only, no I/O).
    /// Returns false if serialization failed (caller should NACK).
    fn wal_write_delete(&mut self, path: &str) -> bool {
        if let Some(ref mut writer) = self.wal_writer {
            let mut entry = WalEntry::delete(path);
            entry.sequence = writer.sequence();
            match writer.append_one(&entry) {
                Ok(_) => {
                    self.wal_dirty = true;
                    self.wal_pending_entries += 1;
                    self.wal_pending_bytes += writer.bytes_written_last_append();
                    let idx = self.pending_wal_entries.len();
                    self.wal_index.add(&entry.path, idx);
                    self.pending_wal_entries.push(entry);
                    true
                }
                Err(e) => {
                    self.set_wal_failed();
                    error!(
                        "[STORAGE INTEGRITY] {}: WAL write failed for DELETE {}: {}",
                        self.id, path, e
                    );
                    false
                }
            }
        } else {
            true // No WAL writer (ephemeral) - always succeeds
        }
    }

    /// Sync the WAL to disk (async).
    /// Uses async I/O to avoid blocking other databases on the core.
    async fn sync_wal(&mut self) {
        if !self.wal_dirty {
            return;
        }

        let entries = self.wal_pending_entries;
        let bytes = self.wal_pending_bytes;

        if let Some(ref mut writer) = self.wal_writer {
            let start = Instant::now();
            match writer.sync().await {
                Ok(rotated) => {
                    let duration = start.elapsed();
                    self.wal_dirty = false;
                    self.wal_pending_entries = 0;
                    self.wal_pending_bytes = 0;

                    debug!(
                        "[WAL Sync] {}: flushed {} entries ({} bytes) in {:?}",
                        self.id, entries, bytes, duration
                    );

                    // Record WAL flush stats
                    crate::metrics::record_wal_flush(duration, entries, bytes);

                    if rotated {
                        tracing::debug!("[Persistence] {}: WAL rotated", self.id);
                        self.notify_compaction().await;
                    }
                }
                Err(e) => {
                    self.set_wal_failed();
                    error!("[STORAGE INTEGRITY] {}: WAL sync failed: {}", self.id, e);
                }
            }
        }
    }

    /// Write a .compaction-queue marker so lark-compact knows to sync this DB's WAL files.
    /// Called on shutdown to ensure any unrotated WAL data gets synced offsite.
    async fn write_compaction_queue_marker(&self) {
        let Some(data_dir) = &self.data_dir else {
            return;
        };
        if let Some(root_dir) = data_dir.parent().and_then(|p| p.parent()) {
            let queue_dir = root_dir.join(".compaction-queue");
            let marker_name = self.id.replace('/', "#");
            let marker_path = queue_dir.join(&marker_name);
            let _ = crate::storage::create_dir_all_async(&queue_dir).await;
            let _ = crate::storage::write_file_async(&marker_path, b"").await;
        }
    }

    /// Close the WAL writer (for clean shutdown).
    async fn close_wal(&mut self) {
        if let Some(mut writer) = self.wal_writer.take()
            && let Err(e) = writer.close().await
        {
            warn!("Failed to close WAL: {}", e);
        }
    }

    // =========================================================================
    // Housekeeping
    // =========================================================================

    async fn housekeeping(&mut self) {
        // Keepalive is client-initiated (client sends "pi", server swallows it)
        // View manager handles its own cleanup via unsubscribe

        // Note: processed_writes and nacked_writes are bounded per-connection
        // (MAX_WRITES_PER_CONNECTION entries each). They evict oldest on insert.
        // Entries are kept after disconnect so reconnecting clients still get
        // deduplication protection.

        // Attempt WAL recovery if in failed state
        // This runs every ~5s (housekeeping interval) and is cheap (one small write + sync)
        self.try_recover_wal().await;

        // Evict idle promoted paths back to Sentinel to reclaim memory.
        // Only applies to blob-backed databases with promoted data.
        if self.is_blob_backed() && !self.promoted_paths.is_empty() {
            self.evict_idle_paths();
        }
    }

    /// Get view count (for testing).
    pub fn view_count(&self) -> usize {
        self.view_manager.view_count()
    }

    /// Refresh the per-database on-disk size gauge for billing telemetry.
    ///
    /// We bill on the compacted blob only. `io().size()` reads an in-memory
    /// `tracked_size` cell (no syscall) and is refreshed at the end of every
    /// incremental compaction batch (`BlobSession::apply_updates_with_sidecar`),
    /// so it's current to within one WAL cycle (≤ a few MB) — negligible at
    /// GB-granularity billing. The sidecar and not-yet-compacted WAL are
    /// intentionally excluded. In-memory/ephemeral databases have no blob
    /// session, so the gauge stays at 0.
    async fn refresh_data_size(&self) {
        let Some(session) = self.blob_session.as_ref() else {
            return;
        };
        if let Ok(size) = lark_blob::BlobIO::size(session.io()).await {
            self.metrics.set_data_size(size);
        }
    }

    /// Emit metrics to stdout in JSON format (for Vector to pick up).
    /// Only emits if there was activity since the last emission.
    fn emit_metrics(&mut self) {
        if let Some(snapshot) = self.metrics.emit_and_reset() {
            // Extract just the database name from "project/database" id
            let database_name = self.pure_database_id.clone();

            // Get server ID from environment or use hostname
            let server_id =
                std::env::var("LARK_SERVER_ID").unwrap_or_else(|_| "localhost".to_string());

            let json = snapshot.to_json(&self.project_id, &database_name, &server_id, self.core_id);

            // Forward to the shipper thread when direct push is enabled. Non-blocking:
            // a full channel (slow/dead shipper) drops the sample rather than stalling
            // this core.
            if let Some(tx) = &self.metrics_tx {
                let _ = tx.try_send(json.clone());
            }

            // Always emit to stdout: this is what an external log shipper (e.g. Vector)
            // scrapes, and it keeps the line visible in logs regardless of push.
            println!("{}", json);
        }
    }

    fn emit_promotion_stats(&mut self) {
        let snap = self.promotion_stats.reset();
        if snap.count > 0 {
            info!(
                db = %self.id,
                promotions = snap.count,
                total_ms = format!("{:.1}", snap.total_us as f64 / 1000.0),
                total_read_ms = format!("{:.1}", snap.total_read_us as f64 / 1000.0),
                p50_ms = format!("{:.1}", snap.p50 as f64 / 1000.0),
                p95_ms = format!("{:.1}", snap.p95 as f64 / 1000.0),
                p99_ms = format!("{:.1}", snap.p99 as f64 / 1000.0),
                read_p50_ms = format!("{:.1}", snap.read_p50 as f64 / 1000.0),
                read_p95_ms = format!("{:.1}", snap.read_p95 as f64 / 1000.0),
                read_p99_ms = format!("{:.1}", snap.read_p99 as f64 / 1000.0),
                pread_count = snap.pread_count,
                bytes_read = snap.bytes_read,
                cache_hits = snap.cache_hits,
                cache_hit_bytes = snap.cache_hit_bytes,
                cache_header_misses = snap.cache_header_misses,
                pending_wal = self.pending_wal_entries.len(),
                promoted_paths = self.promoted_paths.len(),
                "Promotion stats"
            );
        }
    }
}

/// Recursively validate every object key in a written value.
///
/// Object field-names become storage keys, so the same restrictions that apply
/// to path segments (`validate_key`: non-empty, no control chars or
/// `$ # [ ] /`, `.` only as a leading char, ≤768 bytes) must hold for them too.
/// Without this, a SET/UPDATE value could plant keys that no path can address
/// (e.g. a literal `a/b` key) and that the rules layer assumes can't exist.
/// Server-value and priority sentinels (`.sv`, `.priority`, `.value`) pass
/// because `validate_key` permits a leading dot. Arrays carry no string keys, so
/// we just recurse into their elements.
fn validate_value_keys(value: &Value) -> Result<(), crate::db::KeyError> {
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                crate::db::validate_key(k)?;
                validate_value_keys(v)?;
            }
        }
        Value::Array(items) => {
            for item in items {
                validate_value_keys(item)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Validate an authentication token and extract auth info.
///
/// Uses the actual JWT validation from our auth module.
/// If no secret is provided (emulator mode), accepts valid-format tokens.
fn validate_auth_token(
    token: &str,
    project_secret: Option<&str>,
) -> Result<crate::auth::jwt::AuthInfo, String> {
    use crate::auth::jwt::{peek_token_header, validate_lark_customer_token};

    // Check if it's a valid JWT format
    let (alg, _kid) = peek_token_header(token).map_err(|e| format!("{:?}", e))?;

    match alg.as_str() {
        "HS256" => {
            // HS256 tokens need validation with the secret
            let secret = project_secret.ok_or_else(|| "no project secret available".to_string())?;
            validate_lark_customer_token(token, secret.as_bytes()).map_err(|e| format!("{:?}", e))
        }
        _ => Err(format!("unsupported algorithm: {}", alg)),
    }
}

/// Compute a hash of a JSON value for transaction conditions.
/// Uses JCS (RFC 8785) canonicalization for consistent hashing.
fn compute_value_hash(value: &Value) -> String {
    use sha2::{Digest, Sha256};

    // Canonicalize to JCS format using the library
    let canonical = serde_json_canonicalizer::to_vec(value).unwrap_or_default();

    // Hash with SHA-256
    let mut hasher = Sha256::new();
    hasher.update(&canonical);
    let result = hasher.finalize();

    // Return as hex string
    hex::encode(result)
}

/// Process server value placeholders in a value.
/// Returns the processed value and whether any server values were resolved.
fn process_server_values(
    value: Value,
    base_path: &str,
    tree: &Arc<RwLock<Tree>>,
) -> Result<(Value, bool), String> {
    process_server_values_recursive(value, base_path, tree)
}

fn process_server_values_recursive(
    value: Value,
    current_path: &str,
    tree: &Arc<RwLock<Tree>>,
) -> Result<(Value, bool), String> {
    match value {
        Value::Object(map) => {
            // Check if this is a server value placeholder: {".sv": ...}
            if map.len() == 1
                && let Some(sv) = map.get(".sv")
            {
                let resolved = resolve_server_value(sv, current_path, tree)?;
                return Ok((resolved, true));
            }

            // Recursively process children
            let mut result = serde_json::Map::new();
            let mut any_modified = false;
            for (key, child) in map {
                let child_path = format!("{}/{}", current_path.trim_end_matches('/'), key);
                let (processed, modified) =
                    process_server_values_recursive(child, &child_path, tree)?;
                if modified {
                    any_modified = true;
                }
                result.insert(key, processed);
            }
            Ok((Value::Object(result), any_modified))
        }
        Value::Array(arr) => {
            // Recursively process array elements
            let mut result = Vec::new();
            let mut any_modified = false;
            for (i, elem) in arr.into_iter().enumerate() {
                let child_path = format!("{}/{}", current_path, i);
                let (processed, modified) =
                    process_server_values_recursive(elem, &child_path, tree)?;
                if modified {
                    any_modified = true;
                }
                result.push(processed);
            }
            Ok((Value::Array(result), any_modified))
        }
        _ => {
            // Primitives pass through unchanged
            Ok((value, false))
        }
    }
}

/// Resolve a server value placeholder.
fn resolve_server_value(sv: &Value, path: &str, tree: &Arc<RwLock<Tree>>) -> Result<Value, String> {
    match sv {
        Value::String(s) if s == "timestamp" => {
            // Return current timestamp in milliseconds
            use std::time::{SystemTime, UNIX_EPOCH};
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            Ok(Value::Number(serde_json::Number::from(now)))
        }
        Value::Object(map) => {
            // Check for increment: {".sv": {"increment": delta}}
            if map.len() == 1
                && let Some(delta) = map.get("increment")
            {
                return resolve_increment(delta, path, tree);
            }
            Err(format!("unknown server value object: {:?}", map))
        }
        _ => Err(format!("unknown server value: {:?}", sv)),
    }
}

/// Validate .value/.priority patterns in a value.
/// Firebase allows `.value` with only `.priority` as the other key.
/// Any other keys alongside `.value` are invalid.
fn validate_value_priority(value: &Value, path: &str) -> Result<(), String> {
    let obj = match value {
        Value::Object(map) => map,
        _ => return Ok(()), // Not an object, nothing to validate
    };

    // Check if this object has .value
    if obj.contains_key(".value") {
        // If .value exists, only .priority is allowed as the other key
        let invalid_keys: Vec<&String> = obj
            .keys()
            .filter(|k| *k != ".value" && *k != ".priority")
            .collect();

        if !invalid_keys.is_empty() {
            return Err(format!(
                "Data at {} contains \".value\" alongside other children ({}). \".value\" can only be used with \".priority\" for primitives with priority",
                path,
                invalid_keys
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
    }

    // Recursively validate children
    for (key, child) in obj {
        let child_path = format!("{}/{}", path.trim_end_matches('/'), key);
        validate_value_priority(child, &child_path)?;
    }

    Ok(())
}

/// Resolve increment server value.
/// Reads current value and adds delta to it.
/// If current value is null or not a number, treats it as 0.
fn resolve_increment(delta: &Value, path: &str, tree: &Arc<RwLock<Tree>>) -> Result<Value, String> {
    // Convert delta to f64
    let delta_float = match delta {
        Value::Number(n) => n
            .as_f64()
            .ok_or_else(|| "invalid increment delta".to_string())?,
        _ => return Err(format!("increment delta must be a number, got {:?}", delta)),
    };

    // Get current value at path
    let current_float = {
        let tree_guard = tree.read().unwrap();
        let parsed_path = Path::parse(path);
        match tree_guard.get(&parsed_path) {
            Some(node) => {
                let val = node.to_value();
                match val {
                    Value::Number(n) => n.as_f64().unwrap_or(0.0),
                    _ => 0.0, // Non-numeric values treated as 0
                }
            }
            None => 0.0, // Null/non-existent treated as 0
        }
    };

    // Return the sum
    let result = current_float + delta_float;

    // Return as integer if it's a whole number to preserve wire format
    if result == (result as i64) as f64 {
        Ok(Value::Number(serde_json::Number::from(result as i64)))
    } else {
        Ok(Value::Number(
            serde_json::Number::from_f64(result).unwrap_or_else(|| serde_json::Number::from(0)),
        ))
    }
}

#[cfg(test)]
mod tests {
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
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), SendError>> + '_>>
        {
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
                u32::from_be_bytes(payload[header_size..header_size + 4].try_into().unwrap())
                    as usize;
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
    fn test_on_disconnect_caps_per_client() {
        block_on(async {
            let mut db = Database::new("test".to_string(), "test-project".to_string(), true);
            let (conn, _messages) = MockConnection::new();
            db.add_client_internal("client1", None, "conn1", conn);
            db.set_rules(
                crate::rules::parse_rules(&json!({"rules": {".write": true, ".read": true}}))
                    .unwrap(),
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
}
