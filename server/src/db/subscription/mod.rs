//! Subscription/View management for real-time data synchronization.
//!
//! This module handles:
//! - Tracking active subscriptions (views) for each client
//! - Finding views affected by data mutations
//! - Generating delta events for mutations
//! - Query view state tracking (enter/exit detection)
//! - Rate limiting / send coalescing for high-frequency updates
//! - Volatile batching for high-frequency cursor/position updates
//!
//! ## Shared View Optimization
//!
//! When multiple clients subscribe to the same path with the same query parameters,
//! they share a single `SharedView`. This avoids:
//! - Generating the same event N times
//! - Encoding the same JSON N times
//! - Doing N HashMap lookups to find views
//!
//! Instead, we generate one event per unique (path, query), then iterate subscribers.
//!
//! ## Volatile Batching
//!
//! Paths marked as `.volatile` in security rules receive special handling for
//! high-frequency updates (e.g., cursor positions at 10-20Hz per client).
//!
//! **Key optimizations:**
//! - **Bypass tree storage**: Volatile writes skip Tree mutation, WAL, and persistence
//! - **Coalescing**: Multiple writes to the same path within a batch window keep only
//!   the latest value (e.g., 5 cursor updates become 1 send)
//! - **Tiered flush rates**: Fast clients (KCP/WebTransport) flush at 20Hz,
//!   slow clients (WebSocket) flush at 4Hz
//! - **Encode-once**: Each batch is JSON-encoded once and sent to all subscribers
//!
//! **Flow:**
//! ```text
//! Volatile write arrives
//!   → buffer_volatile() stores in SharedView.pending_volatile_batch
//!   → Latest value wins (coalescing)
//!   → Every 50ms: flush_volatile_fast() sends to KCP/WebTransport clients
//!   → Every 250ms: flush_volatile_slow() sends to WebSocket clients, clears batch
//! ```
//!

use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};

use serde_json::{Map, Value};
use std::sync::Arc;

use crate::db::database::ConnectionSender;
use crate::db::path::Path;
use crate::db::query::{
    Limit, OrderBy, Query, QueryError, QueryParams, SortEntry, apply_query_to_sort_entries,
    is_in_range,
};
use crate::db::tree::Tree;
use crate::db::value::{ArcValueSortExt, SortKey, compare_sort_keys};
use crate::protocol::ServerMessage;
use crate::transport::firebase_adapter::{
    FIREBASE_MAX_FRAME_SIZE, encode_firebase_event, insert_firebase_tag,
};
use crate::transport::protocol::broadcast_flag;
use lark_blob::ArcValue;

// =============================================================================
use std::cell::RefCell;

// =============================================================================
// Broadcast Buffer for Single-Message Fan-Out
// =============================================================================

/// Initial capacity for broadcast buffers.
/// Sized for ~500 clients per group (500 * 8 bytes = 4KB).
const BROADCAST_BUFFER_INITIAL_CAPACITY: usize = 4 * 1024;

thread_local! {
    /// Thread-local broadcast buffers, keyed by (outbox_id, is_firebase).
    /// Reused across event sends to avoid repeated allocation.
    static BROADCAST_BUFFERS: RefCell<HashMap<(usize, bool), BroadcastBuffer>> = RefCell::new(HashMap::new());
}

/// Buffer for collecting client IDs for a broadcast message.
/// Client entries are written directly in binary format during the subscriber loop,
/// eliminating the need for an intermediate Vec<(u32, i32)>.
///
/// Wire format (built incrementally):
/// ```text
/// [client_count: u32]           <- filled in at send time
/// [client_id_1: u32][tag_1: i32]
/// [client_id_2: u32][tag_2: i32]
/// ...
/// [msg_len: u32][msg_bytes...]  <- appended at send time
/// ```
pub struct BroadcastBuffer {
    /// Binary data: [count:4][[client_id:4][tag:4]...]
    /// Message is appended at send time.
    data: Vec<u8>,
    /// Number of clients in the buffer
    client_count: u32,
    /// Connection to use for sending (set on first add_client)
    conn: Option<Arc<dyn ConnectionSender>>,
    /// True if any client needs reliable delivery
    has_reliable: bool,
}

impl BroadcastBuffer {
    /// Create a new empty broadcast buffer with default capacity.
    pub fn new() -> Self {
        let mut data = Vec::with_capacity(BROADCAST_BUFFER_INITIAL_CAPACITY);
        // Reserve space for client count (will be filled in later)
        data.extend_from_slice(&0u32.to_be_bytes());
        Self {
            data,
            client_count: 0,
            conn: None,
            has_reliable: false,
        }
    }

    /// Clear the buffer for reuse, preserving allocated capacity.
    #[inline]
    pub fn clear(&mut self) {
        self.data.clear();
        self.data.extend_from_slice(&0u32.to_be_bytes()); // reserve count header
        self.client_count = 0;
        self.conn = None;
        self.has_reliable = false;
    }

    /// Add a client to the broadcast.
    /// Writes [client_id:4][tag:4] directly to the buffer.
    #[inline]
    pub fn add_client(
        &mut self,
        client_id: u32,
        tag: i32,
        conn: &Arc<dyn ConnectionSender>,
        reliable: bool,
    ) {
        self.data.extend_from_slice(&client_id.to_be_bytes());
        self.data.extend_from_slice(&tag.to_be_bytes());
        self.client_count += 1;
        if self.conn.is_none() {
            self.conn = Some(conn.clone());
        }
        if reliable {
            self.has_reliable = true;
        }
    }

    /// Check if the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.client_count == 0
    }

    /// Send the broadcast via the stored connection.
    /// Returns the number of clients sent to, or 0 if send failed.
    pub fn send(&mut self, message: &[u8], flags: u8) -> usize {
        if self.client_count == 0 {
            return 0;
        }

        // Get values before borrowing self mutably for finalize
        let count = self.client_count as usize;
        let conn = match self.conn.take() {
            Some(c) => c,
            None => return 0,
        };

        // Finalize builds the complete payload in self.data
        // Fill in client count at the beginning
        self.data[0..4].copy_from_slice(&self.client_count.to_be_bytes());
        // Append message length and message
        self.data
            .extend_from_slice(&(message.len() as u32).to_be_bytes());
        self.data.extend_from_slice(message);

        // Send and return count
        if conn.send_broadcast_raw(&self.data, flags).is_ok() {
            count
        } else {
            0
        }
    }
}

/// Access thread-local broadcast buffers for the given operation.
/// The closure receives a mutable reference to the buffer map.
/// Buffers are automatically cleared before use (preserving capacity).
fn with_broadcast_buffers<F, R>(f: F) -> R
where
    F: FnOnce(&mut HashMap<(usize, bool), BroadcastBuffer>) -> R,
{
    BROADCAST_BUFFERS.with(|buffers| {
        let mut buffers = buffers.borrow_mut();
        // Clear all buffers before use (preserves capacity)
        for buf in buffers.values_mut() {
            buf.clear();
        }
        f(&mut buffers)
    })
}

// =============================================================================
// View Types
// =============================================================================

/// One of a client's subscriptions: `(path, query_id, rules_query)`.
/// `rules_query` is the query context captured at subscribe time, for re-running
/// `can_read` on auth/rules change.
pub type ClientSubscription = (String, String, Option<Arc<HashMap<String, Value>>>);

/// A subscription across all clients: `(client_id, path, query_id, rules_query)`.
pub type GlobalSubscription = (String, String, String, Option<Arc<HashMap<String, Value>>>);

/// Key for identifying a shared view: (path, query_id).
/// All clients with the same path and query parameters share a view.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ViewKey {
    pub path: String,
    pub query_id: String,
}

impl ViewKey {
    pub fn new(path: &str, query_id: &str) -> Self {
        Self {
            path: path.to_string(),
            query_id: query_id.to_string(),
        }
    }
}

/// Info about an affected view for batched processing.
/// Used to collect view info upfront, then process in batches with yields.
#[derive(Debug, Clone)]
pub struct AffectedViewInfo {
    pub path: String,
    pub query_id: String,
    pub has_query: bool,
    pub is_volatile: bool,
}

/// Per-subscriber information within a shared view.
pub struct Subscriber {
    pub client_id: String,
    pub tag: Option<i32>,
    pub conn: Arc<dyn ConnectionSender>,
    /// True for KCP/WebTransport clients (20Hz), false for WebSocket (4Hz)
    pub is_fast: bool,
    /// Whether this subscriber is a Firebase-protocol client (cached on subscribe
    /// to avoid virtual dispatch in the hot send loop). Firebase clients receive
    /// events in Firebase wire format.
    pub is_firebase: bool,
    /// Cached outbox_id to avoid virtual dispatch in hot loop.
    pub cached_outbox_id: usize,
    /// Cached numeric client_id to avoid virtual dispatch in hot loop.
    pub cached_client_id: u32,
}

impl Subscriber {
    /// Check if a client supports high-frequency updates.
    /// WebTransport (UDP-based) can handle 20Hz volatile flush.
    /// WebSocket (TCP) is limited to 4Hz due to syscall overhead.
    ///
    /// Client IDs are formatted as "proxy_{protocol}_{addr}_{core}_{id}"
    /// where protocol is a u8: 0=WebSocket, 1=WebTransport, 2=REST.
    pub fn is_fast_client(client_id: &str) -> bool {
        // WebTransport: protocol_id 1 → "proxy_1_..."
        client_id.starts_with("proxy_1_")
    }
}

impl std::fmt::Debug for Subscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscriber")
            .field("client_id", &self.client_id)
            .field("tag", &self.tag)
            .field("is_fast", &self.is_fast)
            .finish()
    }
}

/// A shared view that multiple clients can subscribe to.
/// All clients subscribed to the same (path, query) share this view.
#[derive(Debug)]
pub struct SharedView {
    pub path: String,
    pub query: Query,
    pub query_id: String,
    pub is_volatile: bool,

    /// The rules-query context (`query.*` map) for this view's query, built once
    /// at subscribe time. `None` for simple (non-query) subscriptions. Stored so
    /// a subscription can be re-evaluated against `can_read` after an auth or
    /// rules change without the original SUBSCRIBE message in hand. Identical for
    /// every subscriber of a shared view (it's a pure function of the query), so
    /// it lives on the shared view rather than per-subscriber.
    pub rules_query: Option<Arc<HashMap<String, Value>>>,

    /// For query views: keys currently in the view, in sorted order.
    pub ordered_keys: Vec<String>,

    /// Sort key cache for O(1) lookup during incremental updates.
    pub sort_key_cache: HashMap<String, Option<SortKey>>,

    /// Boundary tracking for limited queries.
    pub boundary: Option<BoundaryItem>,

    /// All subscribers to this view.
    pub subscribers: HashMap<String, Subscriber>, // client_id -> Subscriber

    // --- Volatile batching (for is_volatile views) ---
    /// Fast subscribers (KCP/WebTransport, 20Hz flush rate)
    pub fast_subscribers: HashSet<String>,
    /// Slow subscribers (WebSocket, 4Hz flush rate)
    pub slow_subscribers: HashSet<String>,
    /// Pending volatile batch: relative_path -> raw JSON bytes (latest wins)
    /// E.g., "/player1" -> {"x": 100, "y": 200}
    pub pending_volatile_batch: HashMap<String, Bytes>,
}

impl SharedView {
    /// Create a new shared view.
    pub fn new(
        path: String,
        query: Query,
        is_volatile: bool,
        rules_query: Option<Arc<HashMap<String, Value>>>,
    ) -> Self {
        let query_id = query.identifier();
        Self {
            path,
            query,
            query_id,
            is_volatile,
            rules_query,
            ordered_keys: Vec::new(),
            sort_key_cache: HashMap::new(),
            boundary: None,
            subscribers: HashMap::new(),
            fast_subscribers: HashSet::new(),
            slow_subscribers: HashSet::new(),
            pending_volatile_batch: HashMap::new(),
        }
    }

    /// Add a subscriber to this view.
    pub fn add_subscriber(
        &mut self,
        client_id: String,
        tag: Option<i32>,
        conn: Arc<dyn ConnectionSender>,
    ) {
        let is_fast = Subscriber::is_fast_client(&client_id);
        // Cache values once on subscribe to avoid virtual dispatch in hot loop
        let is_firebase = conn.is_firebase();
        let cached_outbox_id = conn.outbox_id();
        let cached_client_id = conn.client_id();
        self.subscribers.insert(
            client_id.clone(),
            Subscriber {
                client_id: client_id.clone(),
                tag,
                conn,
                is_fast,
                is_firebase,
                cached_outbox_id,
                cached_client_id,
            },
        );
        // Track in fast/slow sets for volatile batching
        if is_fast {
            self.fast_subscribers.insert(client_id);
        } else {
            self.slow_subscribers.insert(client_id);
        }
    }

    /// Remove a subscriber from this view.
    pub fn remove_subscriber(&mut self, client_id: &str) -> bool {
        // Remove from fast/slow sets
        self.fast_subscribers.remove(client_id);
        self.slow_subscribers.remove(client_id);
        self.subscribers.remove(client_id).is_some()
    }

    /// Check if this view has any subscribers.
    pub fn is_empty(&self) -> bool {
        self.subscribers.is_empty()
    }

    /// Check if this view has query constraints.
    pub fn has_query(&self) -> bool {
        self.query.has_constraints()
    }

    /// Check if this view has a limit.
    pub fn has_limit(&self) -> bool {
        self.query.has_limit()
    }

    /// Check if a key is currently in the view.
    pub fn is_key_in_view(&self, key: &str) -> bool {
        self.ordered_keys.iter().any(|k| k == key)
    }

    /// Get the predecessor key for a given key (for previousChildKey).
    pub fn find_predecessor_key(&self, key: &str) -> Option<&str> {
        for (i, k) in self.ordered_keys.iter().enumerate() {
            if k == key {
                return if i == 0 {
                    None
                } else {
                    Some(&self.ordered_keys[i - 1])
                };
            }
        }
        None
    }

    /// Get the ViewKey for this shared view.
    pub fn key(&self) -> ViewKey {
        ViewKey::new(&self.path, &self.query_id)
    }

    // --- Volatile batching methods ---

    /// Buffer a volatile update for this view.
    /// relative_path is the path relative to this view's path (e.g., "/player1")
    /// raw_value is the JSON-encoded value bytes
    pub fn buffer_volatile(&mut self, relative_path: String, raw_value: Bytes) {
        self.pending_volatile_batch.insert(relative_path, raw_value);
    }

    /// Check if there are pending volatile updates.
    pub fn has_pending_volatile(&self) -> bool {
        !self.pending_volatile_batch.is_empty()
    }

    /// Clear the pending volatile batch.
    pub fn clear_volatile_batch(&mut self) {
        self.pending_volatile_batch.clear();
    }
}

/// A reference to a view for a specific client.
/// Provides the same interface as the old View struct for compatibility.
pub struct ViewRef<'a> {
    pub shared_view: &'a SharedView,
    pub subscriber: &'a Subscriber,
}

impl<'a> ViewRef<'a> {
    pub fn client_id(&self) -> &str {
        &self.subscriber.client_id
    }

    pub fn path(&self) -> &str {
        &self.shared_view.path
    }

    pub fn query_id(&self) -> &str {
        &self.shared_view.query_id
    }

    pub fn query(&self) -> &Query {
        &self.shared_view.query
    }

    pub fn is_volatile(&self) -> bool {
        self.shared_view.is_volatile
    }

    pub fn tag(&self) -> Option<i32> {
        self.subscriber.tag
    }

    pub fn has_query(&self) -> bool {
        self.shared_view.has_query()
    }

    pub fn has_limit(&self) -> bool {
        self.shared_view.has_limit()
    }

    pub fn is_key_in_view(&self, key: &str) -> bool {
        self.shared_view.is_key_in_view(key)
    }

    pub fn ordered_keys(&self) -> &[String] {
        &self.shared_view.ordered_keys
    }
}

/// Event categories for filtering based on subscribed event types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventCategory {
    /// Initial snapshot.
    Initial,
    /// New child added (child_added).
    Add,
    /// Existing child changed (child_changed).
    Change,
    /// Child removed (child_removed).
    Remove,
    /// Child moved position (child_moved).
    Move,
}

/// A client event to be sent.
#[derive(Debug)]
pub struct ClientEvent {
    pub client_id: String,
    pub subscription_path: String,
    pub query_id: String,
    pub message: ServerMessage,
    pub volatile: bool,
    pub category: EventCategory,
    /// Pre-encoded message bytes for shared views (avoids re-encoding for each subscriber).
    pub encoded_bytes: Option<Vec<u8>>,
}

impl ClientEvent {
    pub fn new(
        client_id: String,
        subscription_path: String,
        query_id: String,
        message: ServerMessage,
        volatile: bool,
        category: EventCategory,
    ) -> Self {
        Self {
            client_id,
            subscription_path,
            query_id,
            message,
            volatile,
            category,
            encoded_bytes: None,
        }
    }
}

/// A mutation event that views may care about.
#[derive(Debug, Clone)]
pub struct MutationEvent {
    /// Type of mutation: "set", "update", "remove", "push".
    pub mutation_type: String,
    /// Absolute path that was mutated.
    pub path: String,
    /// Value before change (None for adds).
    pub old_value: Option<Value>,
    /// Value after change (None for removes).
    pub new_value: Option<Value>,
    /// For "update" type: the original update paths/values.
    pub updates: Option<Map<String, Value>>,
    /// Whether this is a volatile write.
    pub volatile: bool,
    /// Client that initiated this mutation (their echo bypasses coalescing).
    pub writer_client_id: Option<String>,
}

/// Boundary item for limited queries - tracks the edge of the view window.
/// For limitToFirst: this is the LAST item (highest sort value in view).
/// For limitToLast: this is the FIRST item (lowest sort value in view).
#[derive(Debug, Clone)]
pub struct BoundaryItem {
    pub key: String,
    pub sort_value: Option<SortKey>,
}

/// A subscription view for a client.
#[derive(Debug, Clone)]
pub struct View {
    pub client_id: String,
    pub path: String,
    pub query_id: String,
    pub query: Query,
    pub is_volatile: bool,
    pub tag: Option<i32>,

    /// For query views: keys currently in the view, in sorted order.
    pub ordered_keys: Vec<String>,

    /// Sort key cache for O(1) lookup during incremental updates.
    /// Maps child key -> cached sort value for that child.
    /// Only populated for views with limits (where incremental updates matter).
    pub sort_key_cache: HashMap<String, Option<SortKey>>,

    /// Boundary tracking for limited queries.
    /// For limitToFirst: the last item in view (highest sort value).
    /// For limitToLast: the first item in view (lowest sort value).
    pub boundary: Option<BoundaryItem>,
}

impl View {
    /// Create a new view.
    pub fn new(client_id: String, path: String, query: Query, is_volatile: bool) -> Self {
        let query_id = query.identifier();
        let tag = query.tag;
        Self {
            client_id,
            path,
            query_id,
            query,
            is_volatile,
            tag,
            ordered_keys: Vec::new(),
            sort_key_cache: HashMap::new(),
            boundary: None,
        }
    }

    /// Check if this view has query constraints.
    pub fn has_query(&self) -> bool {
        self.query.has_constraints()
    }

    /// Check if this view has a limit.
    pub fn has_limit(&self) -> bool {
        self.query.has_limit()
    }

    /// Check if a key is currently in the view.
    pub fn is_key_in_view(&self, key: &str) -> bool {
        self.ordered_keys.iter().any(|k| k == key)
    }

    /// Get the predecessor key for a given key (for previousChildKey).
    pub fn find_predecessor_key(&self, key: &str) -> Option<&str> {
        for (i, k) in self.ordered_keys.iter().enumerate() {
            if k == key {
                return if i == 0 {
                    None
                } else {
                    Some(&self.ordered_keys[i - 1])
                };
            }
        }
        None
    }
}

/// Maximum number of distinct subscriptions (views) a single client connection
/// may hold on one database. A subscription amplifies every matching write's
/// fan-out, so an unbounded count lets one cheap connection inflate per-write
/// work for the whole database. This is a generous DoS rail (audit M-3). Re-subscribing to a view the
/// client already holds is idempotent and does not count against this.
const MAX_SUBSCRIPTIONS_PER_CLIENT: usize = 1_000;

/// Why a `subscribe` call was rejected. Distinct from [`QueryError`] (a
/// query-*parsing* failure) so unrelated query-parsing call sites don't have to
/// reason about subscription limits.
#[derive(Debug, PartialEq, Eq)]
pub enum SubscribeError {
    /// The query parameters themselves were invalid.
    Query(QueryError),
    /// The client already holds [`MAX_SUBSCRIPTIONS_PER_CLIENT`] subscriptions.
    TooManySubscriptions { limit: usize },
}

impl From<QueryError> for SubscribeError {
    fn from(e: QueryError) -> Self {
        SubscribeError::Query(e)
    }
}

/// Manages all active views and generates events.
///
/// Uses shared views internally to optimize event generation for high-fanout scenarios.
/// When N clients subscribe to the same (path, query), they share a single SharedView.
pub struct ViewManager {
    /// Primary storage: ViewKey -> SharedView
    /// This is the source of truth for view state.
    shared_views: HashMap<ViewKey, SharedView>,

    /// Index for find_affected_views: path -> set of ViewKeys at that path
    by_path: BTreeMap<String, HashSet<ViewKey>>,

    /// Index for unsubscribe_all: client_id -> set of ViewKeys the client is subscribed to
    by_client: HashMap<String, HashSet<ViewKey>>,

    /// Total subscription count (maintained incrementally for O(1) access)
    total_subscriptions: usize,

    /// Volatile path patterns for checking subscriptions
    volatile_paths: Vec<String>,

    /// Volatile views with pending batches (for efficient flushing)
    pending_volatile_views: HashSet<ViewKey>,
}

mod events;
mod incremental_sort;
mod lifecycle;
mod query_view;
mod volatile;

impl Default for ViewManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
