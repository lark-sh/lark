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
use crate::protocol::{ClientMessage, MAX_VOLATILE_WRITE_SIZE, ServerMessage, error, op};
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

/// WAL flush interval, in milliseconds. Controls how often buffered WAL entries
/// are flushed to the WAL file. `0` selects synchronous durability: every write
/// is flushed (and `fdatasync`'d, if `FSYNC_ON_WAL_FLUSH`) before its ACK is
/// sent, so the database waits on each write. Default 2000ms. Override at startup
/// via `set_wal_sync_interval_ms` (driven by the `LARK_WAL_SYNC_INTERVAL_MS` env
/// var). See also [`FSYNC_ON_WAL_FLUSH`].
pub static WAL_SYNC_INTERVAL_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(2000);

pub fn set_wal_sync_interval_ms(ms: u64) {
    WAL_SYNC_INTERVAL_MS.store(ms, std::sync::atomic::Ordering::SeqCst);
}

pub fn wal_sync_interval_ms() -> u64 {
    WAL_SYNC_INTERVAL_MS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Whether each WAL flush issues a real `fdatasync` to force data onto the
/// physical device (`true`), or only flushes to the OS page cache (`false`,
/// the default). Page-cache-only writes survive a process crash (the kernel
/// writes them back) but not a power loss or kernel panic before writeback.
/// Override at startup via `set_fsync_on_wal_flush` (driven by the
/// `LARK_FSYNC_ON_WAL_FLUSH` env var).
pub static FSYNC_ON_WAL_FLUSH: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

pub fn set_fsync_on_wal_flush(enabled: bool) {
    FSYNC_ON_WAL_FLUSH.store(enabled, std::sync::atomic::Ordering::SeqCst);
}

pub fn fsync_on_wal_flush() -> bool {
    FSYNC_ON_WAL_FLUSH.load(std::sync::atomic::Ordering::Relaxed)
}

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

/// Maximum on-disk size of a single database. Growth writes (set/update/
/// transaction) are rejected once the database reaches this; deletes are still
/// allowed so the owner can recover under the cap. Enforcement is approximate —
/// it reads the periodically-refreshed `data_size` gauge (~60s cadence), so a
/// burst can overshoot slightly, which is fine for a 1 TB runaway-cost backstop.
const MAX_DATABASE_SIZE_BYTES: u64 = 1024 * 1024 * 1024 * 1024; // 1 TiB

/// Maximum number of operations in a single transaction. Each condition op
/// triggers a `promote_path_deep` (blob read + WAL replay) on the database's
/// single-threaded inbox, so an unbounded count lets one ~16 MB request serialize
/// many disk round trips and stall every client on the database. This is a generous DoS rail well
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

/// Token-bucket capacity (burst) for the per-database durable-write rate
/// limiter: 512 MB. Must exceed the largest single write (256 MB REST) so one
/// big write can never overflow the bucket and deadlock. Deliberately generous —
/// only sustained abuse should ever hit it. This is a runaway-cost / abuse
/// backstop, not a fairness mechanism (fairness is handled by the thread-per-core
/// batch+yield scheduling).
const WRITE_RATE_BURST_BYTES: f64 = 512.0 * 1024.0 * 1024.0;

/// Sustained refill for the durable-write rate limiter: 64 MB per 15 s
/// (= 256 MB/min). The long-run ceiling a single database's durable writes are
/// held to once the burst budget is spent.
const WRITE_RATE_REFILL_BYTES_PER_SEC: f64 = 64.0 * 1024.0 * 1024.0 / 15.0;

/// Per-database token-bucket limiter for durable write *bytes*. Each database is
/// single-threaded, so no synchronization is needed. See `WRITE_RATE_*`.
struct WriteRateLimiter {
    tokens: f64,
    last_refill: Instant,
}

impl WriteRateLimiter {
    fn new() -> Self {
        Self {
            tokens: WRITE_RATE_BURST_BYTES, // start full so a fresh DB can burst
            last_refill: Instant::now(),
        }
    }

    /// Refill by elapsed time (capped at burst), then consume `bytes` if enough
    /// tokens remain. Returns whether the write is allowed. `now` is injected for
    /// deterministic testing; the public `try_consume` passes `Instant::now()`.
    fn try_consume_at(&mut self, bytes: usize, now: Instant) -> bool {
        let elapsed = now
            .saturating_duration_since(self.last_refill)
            .as_secs_f64();
        self.last_refill = now;
        self.tokens =
            (self.tokens + elapsed * WRITE_RATE_REFILL_BYTES_PER_SEC).min(WRITE_RATE_BURST_BYTES);
        if self.tokens >= bytes as f64 {
            self.tokens -= bytes as f64;
            true
        } else {
            false
        }
    }

    fn try_consume(&mut self, bytes: usize) -> bool {
        self.try_consume_at(bytes, Instant::now())
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

    /// Per-database durable-write byte rate limiter (runaway-cost backstop).
    write_rate_limiter: WriteRateLimiter,
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

mod auth;
mod broadcast;
mod connection;
mod eviction;
mod handlers;
mod housekeeping;
mod lifecycle;
mod persistence;
mod promotion;
mod run;
mod sentinel;
mod subscribe;
mod wal;

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
/// Maximum object/array nesting depth of a JSON value: a scalar is 0,
/// `{"a": 1}` is 1, `{"a": {"b": 1}}` is 2. Added to a write's landing-path
/// depth, this is how deep in the tree the value's deepest leaf will sit.
fn json_value_depth(value: &Value) -> usize {
    match value {
        Value::Object(map) => 1 + map.values().map(json_value_depth).max().unwrap_or(0),
        Value::Array(items) => 1 + items.iter().map(json_value_depth).max().unwrap_or(0),
        _ => 0,
    }
}

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
mod tests;
