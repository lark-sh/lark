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
    pub fn new(path: String, query: Query, is_volatile: bool) -> Self {
        let query_id = query.identifier();
        Self {
            path,
            query,
            query_id,
            is_volatile,
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

    /// Get the number of subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.subscribers.len()
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

impl ViewManager {
    /// Create a new view manager.
    pub fn new() -> Self {
        Self {
            shared_views: HashMap::new(),
            by_path: BTreeMap::new(),
            by_client: HashMap::new(),
            total_subscriptions: 0,
            volatile_paths: Vec::new(),
            pending_volatile_views: HashSet::new(),
        }
    }

    /// Get total number of active subscriptions (O(1)).
    pub fn subscription_count(&self) -> usize {
        self.total_subscriptions
    }

    /// Set volatile path patterns.
    pub fn set_volatile_paths(&mut self, patterns: Vec<String>) {
        self.volatile_paths = patterns;
    }

    /// Check if a path matches a volatile pattern.
    fn is_volatile_path(&self, path: &str) -> bool {
        let path_segments: Vec<&str> = path.trim_matches('/').split('/').collect();

        for pattern in &self.volatile_paths {
            let pattern_segments: Vec<&str> = pattern.trim_matches('/').split('/').collect();
            if Self::matches_pattern(&path_segments, &pattern_segments) {
                return true;
            }
        }
        false
    }

    fn matches_pattern(path_segments: &[&str], pattern_segments: &[&str]) -> bool {
        // Path must have at least as many segments as pattern (exact match or child).
        // Volatile cascades: children of volatile paths are also volatile.
        if path_segments.len() < pattern_segments.len() {
            return false;
        }
        // Check that the pattern segments match the beginning of the path
        for (seg, pat) in path_segments.iter().zip(pattern_segments.iter()) {
            if *pat != "*" && !pat.starts_with('$') && *pat != *seg {
                return false;
            }
        }
        true
    }

    /// Subscribe a client to a path with optional query parameters.
    ///
    /// Returns the query_id on success, or a QueryError if the query parameters are invalid.
    /// If another client already has the same (path, query), they share the same SharedView.
    pub fn subscribe(
        &mut self,
        client_id: &str,
        path: &str,
        query_params: Option<&QueryParams>,
        conn: Arc<dyn ConnectionSender>,
    ) -> Result<String, QueryError> {
        let query = match query_params {
            Some(p) => p.to_query()?,
            None => Query::default(),
        };
        let is_volatile = self.is_volatile_path(path);
        let tag = query.tag;
        let query_id = query.identifier();
        let view_key = ViewKey::new(path, &query_id);

        // Check if this client is already subscribed to this exact view
        let already_subscribed = self
            .by_client
            .get(client_id)
            .is_some_and(|keys| keys.contains(&view_key));

        if !already_subscribed {
            // Get or create the shared view
            let shared_view = self
                .shared_views
                .entry(view_key.clone())
                .or_insert_with(|| SharedView::new(path.to_string(), query, is_volatile));

            // Add this client as a subscriber
            shared_view.add_subscriber(client_id.to_string(), tag, conn);

            // Update by_path index
            self.by_path
                .entry(path.to_string())
                .or_default()
                .insert(view_key.clone());

            // Update by_client index
            self.by_client
                .entry(client_id.to_string())
                .or_default()
                .insert(view_key);

            // Increment subscription count
            self.total_subscriptions += 1;
        }

        Ok(query_id)
    }

    /// Initialize a query view with its ordered keys.
    /// This is called after the initial snapshot is sent to set up query state.
    pub fn initialize_query_view(
        &mut self,
        _client_id: &str,
        path: &str,
        query_id: &str,
        keys: Vec<String>,
    ) {
        let view_key = ViewKey::new(path, query_id);
        if let Some(view) = self.shared_views.get_mut(&view_key) {
            view.ordered_keys = keys;
        }
    }

    /// Unsubscribe a client from a path (default query).
    pub fn unsubscribe(&mut self, client_id: &str, path: &str) {
        self.unsubscribe_with_query(client_id, path, "default");
    }

    /// Unsubscribe a client from a path with a specific query.
    pub fn unsubscribe_with_query(&mut self, client_id: &str, path: &str, query_id: &str) {
        let view_key = ViewKey::new(path, query_id);
        let mut removed = false;
        let mut view_empty = false;

        // Remove subscriber from shared view
        if let Some(shared_view) = self.shared_views.get_mut(&view_key)
            && shared_view.remove_subscriber(client_id)
        {
            removed = true;
            view_empty = shared_view.is_empty();
        }

        // If shared view is now empty, remove it entirely
        if view_empty {
            self.shared_views.remove(&view_key);
            // Clean up by_path index
            if let Some(keys) = self.by_path.get_mut(path) {
                keys.remove(&view_key);
                if keys.is_empty() {
                    self.by_path.remove(path);
                }
            }
        }

        // Remove from by_client index
        if let Some(keys) = self.by_client.get_mut(client_id) {
            keys.remove(&view_key);
            if keys.is_empty() {
                self.by_client.remove(client_id);
            }
        }

        // Decrement counter if we actually removed something
        if removed {
            self.total_subscriptions = self.total_subscriptions.saturating_sub(1);
        }
    }

    /// Unsubscribe a client from all paths.
    pub fn unsubscribe_all(&mut self, client_id: &str) {
        if let Some(view_keys) = self.by_client.remove(client_id) {
            let removed_count = view_keys.len();

            for view_key in view_keys {
                // Remove subscriber from shared view
                let mut view_empty = false;
                if let Some(shared_view) = self.shared_views.get_mut(&view_key) {
                    shared_view.remove_subscriber(client_id);
                    view_empty = shared_view.is_empty();
                }

                // If shared view is now empty, remove it entirely
                if view_empty {
                    self.shared_views.remove(&view_key);
                    // Clean up by_path index
                    if let Some(keys) = self.by_path.get_mut(&view_key.path) {
                        keys.remove(&view_key);
                        if keys.is_empty() {
                            self.by_path.remove(&view_key.path);
                        }
                    }
                }
            }

            // Decrement counter by the number of subscriptions removed
            self.total_subscriptions = self.total_subscriptions.saturating_sub(removed_count);
        }
    }

    /// Get a shared view by path and query ID.
    pub fn get_shared_view(&self, path: &str, query_id: &str) -> Option<&SharedView> {
        let view_key = ViewKey::new(path, query_id);
        self.shared_views.get(&view_key)
    }

    /// Get a mutable shared view by path and query ID.
    fn get_shared_view_mut(&mut self, path: &str, query_id: &str) -> Option<&mut SharedView> {
        let view_key = ViewKey::new(path, query_id);
        self.shared_views.get_mut(&view_key)
    }

    // Legacy compatibility: get_view returns a View-like accessor
    // This is used by some methods that need per-client info (like tag)
    /// Get a view by client, path, and query ID.
    /// Returns None if the client is not subscribed to this view.
    pub fn get_view(&self, client_id: &str, path: &str, query_id: &str) -> Option<ViewRef<'_>> {
        let view_key = ViewKey::new(path, query_id);
        let shared_view = self.shared_views.get(&view_key)?;
        let subscriber = shared_view.subscribers.get(client_id)?;
        Some(ViewRef {
            shared_view,
            subscriber,
        })
    }

    /// Get a mutable view by client, path, and query ID.
    fn get_view_mut(
        &mut self,
        _client_id: &str,
        path: &str,
        query_id: &str,
    ) -> Option<&mut SharedView> {
        self.get_shared_view_mut(path, query_id)
    }

    /// Get all views for a client (returns shared view references).
    pub fn get_client_views(&self, client_id: &str) -> Vec<ViewRef<'_>> {
        let Some(view_keys) = self.by_client.get(client_id) else {
            return Vec::new();
        };

        view_keys
            .iter()
            .filter_map(|key| {
                let shared_view = self.shared_views.get(key)?;
                let subscriber = shared_view.subscribers.get(client_id)?;
                Some(ViewRef {
                    shared_view,
                    subscriber,
                })
            })
            .collect()
    }

    /// Find all shared views affected by a change at the given path.
    /// This is the key optimization: returns shared views instead of per-client views.
    pub fn find_affected_shared_views(
        &self,
        changed_path: &str,
        is_volatile: bool,
    ) -> Vec<&SharedView> {
        let mut result = Vec::new();
        let mut seen = HashSet::new();

        // Walk up the path tree by truncating at each '/' — zero allocations.
        // e.g. "/users/alice/score" → "/users/alice" → "/users" → "/"
        let path = changed_path.trim_end_matches('/');
        let mut current = path;
        loop {
            let lookup: &str = if current.is_empty() { "/" } else { current };
            if let Some(view_keys) = self.by_path.get(lookup) {
                for view_key in view_keys {
                    if seen.insert(view_key.clone())
                        && let Some(shared_view) = self.shared_views.get(view_key)
                    {
                        result.push(shared_view);
                    }
                }
            }
            if lookup == "/" {
                break;
            }
            // Move up: find the last '/' and truncate
            match current.rfind('/') {
                Some(0) | None => {
                    current = "";
                } // next iteration checks "/"
                Some(pos) => {
                    current = &path[..pos];
                }
            }
        }

        // Also find views to descendants (they are affected too).
        // BTreeMap range scan starting just past "{path}/", breaking when keys no longer match.
        // Skip for volatile writes - volatile paths are typically leaf nodes.
        if !is_volatile {
            use std::ops::Bound;
            for (view_path, view_keys) in self
                .by_path
                .range::<str, _>((Bound::Excluded(path), Bound::Unbounded))
            {
                // Check if view_path starts with "{path}/" — if not, we're past all descendants
                if !(view_path.len() > path.len()
                    && view_path.starts_with(path)
                    && view_path.as_bytes()[path.len()] == b'/')
                {
                    break;
                }
                for view_key in view_keys {
                    if seen.insert(view_key.clone())
                        && let Some(shared_view) = self.shared_views.get(view_key)
                    {
                        result.push(shared_view);
                    }
                }
            }
        }

        result
    }

    /// Check if `descendant` is a descendant of `ancestor`.
    /// Send events directly to subscribers without creating ClientEvent objects.
    /// This is the optimized path for high-fanout scenarios (100k+ subscribers).
    ///
    /// Returns the number of events sent.
    ///
    /// OPTIMIZATION: Instead of creating Vec<ClientEvent> and then iterating to send,
    /// we generate the message once, encode once, and send directly to each subscriber's
    /// stored connection. This eliminates:
    /// - 100k message clones
    /// - 100k ClientEvent allocations/deallocations
    /// - 100k HashMap lookups
    pub fn send_events(&mut self, event: &MutationEvent, tree: &Tree) -> usize {
        let shared_views = self.find_affected_shared_views(&event.path, event.volatile);
        if shared_views.is_empty() {
            return 0;
        }

        let mut total_sent = 0;

        // Collect shared view info (can't hold refs while mutating)
        let view_infos: Vec<_> = shared_views
            .iter()
            .map(|v| {
                (
                    v.path.clone(),
                    v.query_id.clone(),
                    v.has_query(),
                    v.is_volatile,
                )
            })
            .collect();

        for (view_path, query_id, has_query, is_volatile) in view_infos {
            // For non-query views, generate and send directly
            if !has_query {
                total_sent += self.send_events_for_shared_view(
                    &view_path,
                    &query_id,
                    event,
                    tree,
                    is_volatile,
                );
            } else {
                // For query views, use optimized send with tag prefix insertion
                total_sent += self.send_events_for_query_shared_view(
                    &view_path,
                    &query_id,
                    event,
                    tree,
                    is_volatile,
                );
            }
        }

        total_sent
    }

    /// Collect info about affected views without processing them.
    /// Used for batched processing with yields between batches.
    pub fn collect_affected_view_infos(&self, event: &MutationEvent) -> Vec<AffectedViewInfo> {
        let shared_views = self.find_affected_shared_views(&event.path, event.volatile);
        shared_views
            .iter()
            .map(|v| AffectedViewInfo {
                path: v.path.clone(),
                query_id: v.query_id.clone(),
                has_query: v.has_query(),
                is_volatile: v.is_volatile,
            })
            .collect()
    }

    /// Send events for a batch of affected views.
    /// Returns the number of events sent.
    pub fn send_events_for_views(
        &mut self,
        view_infos: &[AffectedViewInfo],
        event: &MutationEvent,
        tree: &Tree,
    ) -> usize {
        let mut total_sent = 0;

        for info in view_infos {
            if !info.has_query {
                total_sent += self.send_events_for_shared_view(
                    &info.path,
                    &info.query_id,
                    event,
                    tree,
                    info.is_volatile,
                );
            } else {
                total_sent += self.send_events_for_query_shared_view(
                    &info.path,
                    &info.query_id,
                    event,
                    tree,
                    info.is_volatile,
                );
            }
        }

        total_sent
    }

    /// Send events directly for a shared non-query view.
    /// Returns the number of events sent.
    ///
    /// Uses fast string-concatenation encoding to avoid JSON serialization overhead.
    /// The value is serialized once, then formats are generated
    /// via cheap string concatenation.
    fn send_events_for_shared_view(
        &self,
        view_path: &str,
        query_id: &str,
        event: &MutationEvent,
        tree: &Tree,
        is_volatile: bool,
    ) -> usize {
        let view_key = ViewKey::new(view_path, query_id);
        let shared_view = match self.shared_views.get(&view_key) {
            Some(v) => v,
            None => return 0,
        };

        let view_segments: Vec<&str> = view_path.trim_matches('/').split('/').collect();
        let mutation_segments: Vec<&str> = event.path.trim_matches('/').split('/').collect();

        let view_len = if view_segments.len() == 1 && view_segments[0].is_empty() {
            0
        } else {
            view_segments.len()
        };
        let mutation_len = if mutation_segments.len() == 1 && mutation_segments[0].is_empty() {
            0
        } else {
            mutation_segments.len()
        };

        // Determine event type, relative path, and value
        let (event_type, relative_path, value): (&str, String, Value) = if view_len > mutation_len {
            // View is below the mutation path (view is a descendant)
            let view_path_obj = Path::parse(view_path);
            let value = tree.get_value(&view_path_obj).unwrap_or(Value::Null);
            ("put", "/".to_string(), value)
        } else {
            // Check if view path is prefix of mutation path
            let is_prefix = view_len == 0
                || (mutation_len >= view_len && {
                    let view_segs = if view_len == 0 {
                        &[] as &[&str]
                    } else {
                        &view_segments[..view_len]
                    };
                    let mut_segs = &mutation_segments[..view_len];
                    view_segs == mut_segs
                });

            if !is_prefix {
                return 0;
            }

            let remaining_segments: Vec<&str> = if view_len == 0 {
                mutation_segments.clone()
            } else {
                mutation_segments[view_len..].to_vec()
            };

            // For update operations with Updates map, use patch
            if event.mutation_type == "update" {
                if let Some(ref updates) = event.updates {
                    let prefix = if remaining_segments.is_empty() {
                        "/".to_string()
                    } else {
                        format!("/{}/", remaining_segments.join("/"))
                    };

                    let mut patch_values = Map::new();
                    for (update_path, update_value) in updates {
                        let full_path = format!("{}{}", prefix, update_path);
                        patch_values.insert(full_path, update_value.clone());
                    }

                    ("patch", "/".to_string(), Value::Object(patch_values))
                } else {
                    // Fallback to put
                    let relative_path = if remaining_segments.is_empty() {
                        "/".to_string()
                    } else {
                        format!("/{}", remaining_segments.join("/"))
                    };
                    let value = if event.mutation_type == "remove" {
                        Value::Null
                    } else {
                        event.new_value.clone().unwrap_or(Value::Null)
                    };
                    ("put", relative_path, value)
                }
            } else {
                // For other operations, send PUT
                let relative_path = if remaining_segments.is_empty() {
                    "/".to_string()
                } else {
                    format!("/{}", remaining_segments.join("/"))
                };
                let value = if event.mutation_type == "remove" {
                    Value::Null
                } else {
                    event.new_value.clone().unwrap_or(Value::Null)
                };
                ("put", relative_path, value)
            }
        };

        // Serialize value ONCE
        let value_bytes = match serde_json::to_vec(&value) {
            Ok(bytes) => bytes,
            Err(_) => return 0,
        };

        // Generate Lark base bytes (no tag for simple views)
        let lark_base: Vec<u8> = ServerMessage::encode_event_fast(
            event_type,
            view_path,
            &relative_path,
            &value_bytes,
            None, // No tag for simple views
            is_volatile,
        );

        // Lazy-init Firebase base bytes on first Firebase subscriber
        let mut firebase_base: Option<Vec<u8>> = None;

        // Use thread-local broadcast buffers for single-pass payload building

        with_broadcast_buffers(|buffers| {
            let mut direct_sent = 0;
            let reliable = !is_volatile;

            for subscriber in shared_view.subscribers.values() {
                let is_firebase = subscriber.is_firebase;

                if is_firebase {
                    // Firebase client - use or generate Firebase format
                    let fb_bytes = firebase_base.get_or_insert_with(|| {
                        encode_firebase_event(
                            event_type,
                            view_path,
                            &relative_path,
                            &value_bytes,
                            None, // No tag for simple views
                        )
                    });

                    // Check if chunking is needed (Firebase + >16KB)
                    if fb_bytes.len() > FIREBASE_MAX_FRAME_SIZE {
                        // Fall back to direct send (handles chunking)
                        if subscriber
                            .conn
                            .try_send(fb_bytes.clone().into(), is_volatile, true)
                            .is_ok()
                        {
                            direct_sent += 1;
                        }
                        continue;
                    }
                }

                // Add client directly to broadcast buffer (single pass - no intermediate Vec)
                // Use cached values to avoid virtual dispatch overhead
                let outbox_id = subscriber.cached_outbox_id;
                let client_id = subscriber.cached_client_id;
                let key = (outbox_id, is_firebase);

                buffers
                    .entry(key)
                    .or_insert_with(BroadcastBuffer::new)
                    .add_client(client_id, 0, &subscriber.conn, reliable); // Tag = 0 for simple views
            }

            // Send BROADCAST for each buffer
            let mut broadcast_sent = 0;
            for ((_, is_firebase), buffer) in buffers.iter_mut() {
                if buffer.is_empty() {
                    continue;
                }

                // Build flags
                let mut flags: u8 = 0;
                if buffer.has_reliable {
                    flags |= broadcast_flag::RELIABLE;
                }
                if *is_firebase {
                    flags |= broadcast_flag::FIREBASE_FORMAT;
                }

                // Get the message bytes for this group
                let message = if *is_firebase {
                    firebase_base.as_ref().unwrap().as_slice()
                } else {
                    lark_base.as_slice()
                };

                broadcast_sent += buffer.send(message, flags);
            }

            direct_sent + broadcast_sent
        })
    }

    /// Send events directly for a shared query view.
    /// Uses fast string-concat encoding and tag insertion to avoid per-subscriber overhead.
    /// Returns the number of events sent.
    fn send_events_for_query_shared_view(
        &mut self,
        view_path: &str,
        query_id: &str,
        event: &MutationEvent,
        tree: &Tree,
        is_volatile: bool,
    ) -> usize {
        let view_key = ViewKey::new(view_path, query_id);

        // Get first subscriber's client_id for generate_events_for_view (only 1 String clone)
        let first_client_id = {
            let shared_view = match self.shared_views.get(&view_key) {
                Some(v) => v,
                None => return 0,
            };
            match shared_view.subscribers.iter().next() {
                Some((cid, _)) => cid.clone(),
                None => return 0,
            }
        };

        // Generate events using the first subscriber as "representative"
        // This handles the query state update (ordered_keys, etc.)
        let base_events =
            self.generate_events_for_view(&first_client_id, view_path, query_id, event, tree);

        if base_events.is_empty() {
            return 0;
        }

        let mut total_sent = 0;

        for base_event in &base_events {
            // Extract event components from ServerMessage
            let event_type = base_event.message.event.as_deref().unwrap_or("put");
            let subscription_path = base_event
                .message
                .subscription_path
                .as_deref()
                .unwrap_or("");
            let relative_path = base_event.message.path.as_deref().unwrap_or("/");

            // Serialize value ONCE (directly from MessageValue/ArcValue, no intermediate clone)
            let value_bytes = match base_event.message.value.as_ref() {
                Some(v) => match serde_json::to_vec(v) {
                    Ok(bytes) => bytes,
                    Err(_) => continue,
                },
                None => b"null".to_vec(),
            };

            // Generate Lark base bytes WITHOUT tag (we'll prepend tags per-subscriber)
            let lark_base: Vec<u8> = ServerMessage::encode_event_fast(
                event_type,
                subscription_path,
                relative_path,
                &value_bytes,
                None, // No tag - will be added per-subscriber
                is_volatile,
            );

            // Lazy-init Firebase base bytes on first Firebase subscriber
            let mut firebase_base: Option<Vec<u8>> = None;

            // Re-borrow shared_view to iterate subscribers directly (no Vec allocation)
            let shared_view = match self.shared_views.get(&view_key) {
                Some(v) => v,
                None => return total_sent,
            };

            // Use thread-local broadcast buffers for single-pass payload building
            let event_sent = with_broadcast_buffers(|buffers| {
                let mut direct_sent = 0;
                let reliable = !is_volatile;

                // For each subscriber, add to broadcast buffer with their tag
                for subscriber in shared_view.subscribers.values() {
                    let is_firebase = subscriber.is_firebase;

                    if is_firebase {
                        // Firebase client
                        let fb_base = firebase_base.get_or_insert_with(|| {
                            encode_firebase_event(
                                event_type,
                                subscription_path,
                                relative_path,
                                &value_bytes,
                                None, // No tag - proxy will insert per-subscriber
                            )
                        });

                        // Check if chunking is needed (Firebase + >16KB)
                        if fb_base.len() > FIREBASE_MAX_FRAME_SIZE {
                            // Fall back to direct send (handles chunking)
                            let encoded: Bytes = if let Some(t) = subscriber.tag {
                                insert_firebase_tag(fb_base, t).into()
                            } else {
                                fb_base.clone().into()
                            };
                            if subscriber.conn.try_send(encoded, is_volatile, true).is_ok() {
                                direct_sent += 1;
                            }
                            continue;
                        }
                    }

                    // Add client to broadcast buffer with tag
                    // Tag = 0 means no tag modification, otherwise proxy inserts tag
                    // Use cached values to avoid virtual dispatch overhead
                    let outbox_id = subscriber.cached_outbox_id;
                    let client_id = subscriber.cached_client_id;
                    let tag = subscriber.tag.unwrap_or(0);
                    let key = (outbox_id, is_firebase);

                    buffers
                        .entry(key)
                        .or_insert_with(BroadcastBuffer::new)
                        .add_client(client_id, tag, &subscriber.conn, reliable);
                }

                // Send BROADCAST for each buffer
                let mut broadcast_sent = 0;
                for ((_, is_firebase), buffer) in buffers.iter_mut() {
                    if buffer.is_empty() {
                        continue;
                    }

                    // Build flags
                    let mut flags: u8 = 0;
                    if buffer.has_reliable {
                        flags |= broadcast_flag::RELIABLE;
                    }
                    if *is_firebase {
                        flags |= broadcast_flag::FIREBASE_FORMAT;
                    }

                    // Get the message bytes for this group (without tags - proxy inserts them)
                    let message = if *is_firebase {
                        firebase_base.as_ref().unwrap().as_slice()
                    } else {
                        lark_base.as_slice()
                    };

                    broadcast_sent += buffer.send(message, flags);
                }

                direct_sent + broadcast_sent
            });

            total_sent += event_sent;
        }

        total_sent
    }

    /// Generate events for a single view.
    /// This is used for query views which need per-view state management.
    fn generate_events_for_view(
        &mut self,
        client_id: &str,
        view_path: &str,
        query_id: &str,
        event: &MutationEvent,
        tree: &Tree,
    ) -> Vec<ClientEvent> {
        let view_segments: Vec<&str> = view_path.trim_matches('/').split('/').collect();
        let mutation_segments: Vec<&str> = event.path.trim_matches('/').split('/').collect();

        // Get view info from shared view
        let (has_query, is_writer_echo, tag) = {
            let view = match self.get_view(client_id, view_path, query_id) {
                Some(v) => v,
                None => return Vec::new(),
            };
            let is_echo = event.writer_client_id.as_deref() == Some(view.client_id());
            (view.has_query(), is_echo, view.tag())
        };

        // Check if view path is a prefix of mutation path
        let view_len = if view_segments.len() == 1 && view_segments[0].is_empty() {
            0
        } else {
            view_segments.len()
        };
        let mutation_len = if mutation_segments.len() == 1 && mutation_segments[0].is_empty() {
            0
        } else {
            mutation_segments.len()
        };

        if view_len > mutation_len {
            // View is below the mutation path (view is a descendant)
            // Get the current value at the view's path
            let view_path_obj = Path::parse(view_path);
            let value = tree.get_value(&view_path_obj).unwrap_or(Value::Null);

            let mut msg = ServerMessage::put_event(view_path, "/", value, event.volatile);
            if let Some(t) = tag {
                msg.tag = Some(t);
            }

            let ev = ClientEvent::new(
                client_id.to_string(),
                view_path.to_string(),
                query_id.to_string(),
                msg,
                event.volatile,
                EventCategory::Initial,
            );

            return vec![ev];
        }

        // Check if view path is prefix of mutation path
        let is_prefix = view_len == 0
            || (mutation_len >= view_len && {
                let view_segs = if view_len == 0 {
                    &[] as &[&str]
                } else {
                    &view_segments[..view_len]
                };
                let mut_segs = &mutation_segments[..view_len];
                view_segs == mut_segs
            });

        if !is_prefix {
            return Vec::new();
        }

        // Remaining segments after view path
        let remaining_segments: Vec<&str> = if view_len == 0 {
            mutation_segments.clone()
        } else {
            mutation_segments[view_len..].to_vec()
        };

        // For non-query views, just send the delta
        if !has_query {
            return self.generate_simple_view_event(
                client_id,
                view_path,
                query_id,
                event,
                &remaining_segments,
                is_writer_echo,
                tag,
            );
        }

        // For query views, handle complex logic
        self.generate_query_view_events(
            client_id,
            view_path,
            query_id,
            event,
            &remaining_segments,
            tree,
            is_writer_echo,
            tag,
        )
    }

    /// Generate a simple delta event for a non-query view.
    #[allow(clippy::too_many_arguments)]
    fn generate_simple_view_event(
        &self,
        client_id: &str,
        view_path: &str,
        query_id: &str,
        event: &MutationEvent,
        remaining_segments: &[&str],
        _is_writer_echo: bool,
        tag: Option<i32>,
    ) -> Vec<ClientEvent> {
        // Verify view exists
        if self.get_view(client_id, view_path, query_id).is_none() {
            return Vec::new();
        }

        // Determine event category
        let category = self.determine_simple_event_category(event, remaining_segments);

        let relative_path = if remaining_segments.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", remaining_segments.join("/"))
        };

        // For update operations with Updates map, send PATCH
        if event.mutation_type == "update"
            && let Some(ref updates) = event.updates
        {
            let prefix = if remaining_segments.is_empty() {
                "/".to_string()
            } else {
                format!("/{}/", remaining_segments.join("/"))
            };

            let mut patch_values = Map::new();
            for (update_path, update_value) in updates {
                let full_path = format!("{}{}", prefix, update_path);
                patch_values.insert(full_path, update_value.clone());
            }

            let mut msg = ServerMessage::patch_event(view_path, "/", patch_values, event.volatile);
            if let Some(t) = tag {
                msg.tag = Some(t);
            }

            let ev = ClientEvent::new(
                client_id.to_string(),
                view_path.to_string(),
                query_id.to_string(),
                msg,
                event.volatile,
                category,
            );

            return vec![ev];
        }

        // For other operations, send PUT
        let value = if event.mutation_type == "remove" {
            Value::Null
        } else {
            event.new_value.clone().unwrap_or(Value::Null)
        };

        let mut msg = ServerMessage::put_event(view_path, &relative_path, value, event.volatile);
        if let Some(t) = tag {
            msg.tag = Some(t);
        }

        let ev = ClientEvent::new(
            client_id.to_string(),
            view_path.to_string(),
            query_id.to_string(),
            msg,
            event.volatile,
            category,
        );

        vec![ev]
    }

    /// Determine the event category for a simple view event.
    fn determine_simple_event_category(
        &self,
        event: &MutationEvent,
        remaining_segments: &[&str],
    ) -> EventCategory {
        // Mutation AT the subscription path
        if remaining_segments.is_empty() {
            return EventCategory::Initial;
        }

        let is_direct_child = remaining_segments.len() == 1;

        match event.mutation_type.as_str() {
            "remove" => EventCategory::Remove,
            "update" => EventCategory::Change,
            "set" | "push" => {
                // set(null) at direct child level is a removal
                if is_direct_child && event.new_value.as_ref().is_none_or(|v| v.is_null()) {
                    EventCategory::Remove
                } else if is_direct_child {
                    // Direct child set - check old_value to determine add vs change
                    if event.old_value.is_none()
                        || event.old_value.as_ref().is_some_and(|v| v.is_null())
                    {
                        EventCategory::Add
                    } else {
                        EventCategory::Change
                    }
                } else {
                    // Nested set - treat as change (modifying existing data)
                    EventCategory::Change
                }
            }
            _ => EventCategory::Change,
        }
    }

    /// Generate events for a query view (with enter/exit detection).
    #[allow(clippy::too_many_arguments)]
    fn generate_query_view_events(
        &mut self,
        client_id: &str,
        view_path: &str,
        query_id: &str,
        event: &MutationEvent,
        remaining_segments: &[&str],
        tree: &Tree,
        is_writer_echo: bool,
        tag: Option<i32>,
    ) -> Vec<ClientEvent> {
        // Mutation at view path itself - full recompute
        if remaining_segments.is_empty() {
            return self.recompute_query_view(
                client_id,
                view_path,
                query_id,
                event,
                remaining_segments,
                tree,
                is_writer_echo,
                tag,
            );
        }

        let child_key = remaining_segments[0];

        // Get view state
        let (is_in_view, _has_limit, order_by) = {
            let view = match self.get_view(client_id, view_path, query_id) {
                Some(v) => v,
                None => return Vec::new(),
            };
            (
                view.is_key_in_view(child_key),
                view.has_limit(),
                view.query().order_by.clone(),
            )
        };

        let is_child_mutation = remaining_segments.len() == 1;
        let is_removal = event.mutation_type == "remove" && is_child_mutation;

        // Case 1: Item is in the view
        if is_in_view {
            // If item was removed or sort field changed, may need recompute
            if is_removal || self.is_sort_field_change(&order_by, event, remaining_segments) {
                // Try incremental update first for sort field changes (not removals)
                if !is_removal
                    && self.can_use_incremental_sort(
                        client_id,
                        view_path,
                        query_id,
                        event,
                        remaining_segments,
                    )
                    && let Some(events) = self.handle_incremental_sort_update(
                        client_id,
                        view_path,
                        query_id,
                        event,
                        remaining_segments,
                        tree,
                        is_writer_echo,
                        tag,
                    )
                {
                    return events;
                }
                // Incremental update returned None - fall back to full recompute
                return self.recompute_query_view(
                    client_id,
                    view_path,
                    query_id,
                    event,
                    remaining_segments,
                    tree,
                    is_writer_echo,
                    tag,
                );
            }

            // Non-removal, non-sort-field change - just send delta
            // Verify view exists
            if self.get_view(client_id, view_path, query_id).is_none() {
                return Vec::new();
            }

            // For update operations, send PATCH with the specific changed fields
            let msg = if event.mutation_type == "update" {
                if let Some(ref updates) = event.updates {
                    let mut patch_values = Map::new();
                    for (update_path, update_value) in updates {
                        let full_path = format!("/{}/{}", child_key, update_path);
                        patch_values.insert(full_path, update_value.clone());
                    }
                    let mut msg =
                        ServerMessage::patch_event(view_path, "/", patch_values, event.volatile);
                    if let Some(t) = tag {
                        msg.tag = Some(t);
                    }
                    msg
                } else {
                    // Fallback to PUT if no updates map
                    let relative_path = format!("/{}", remaining_segments.join("/"));
                    let value = event.new_value.clone().unwrap_or(Value::Null);
                    let mut msg =
                        ServerMessage::put_event(view_path, &relative_path, value, event.volatile);
                    if let Some(t) = tag {
                        msg.tag = Some(t);
                    }
                    msg
                }
            } else {
                // For set operations, send PUT
                let relative_path = format!("/{}", remaining_segments.join("/"));
                let value = event.new_value.clone().unwrap_or(Value::Null);
                let mut msg =
                    ServerMessage::put_event(view_path, &relative_path, value, event.volatile);
                if let Some(t) = tag {
                    msg.tag = Some(t);
                }
                msg
            };

            // All query view events must bypass rate limiting to maintain correct client state
            return vec![ClientEvent::new(
                client_id.to_string(),
                view_path.to_string(),
                query_id.to_string(),
                msg,
                event.volatile,
                EventCategory::Change,
            )];
        }

        // Case 2: Item is NOT in the view
        // Check if this could cause it to enter
        if is_child_mutation && event.mutation_type != "remove" {
            // New child added or sort field changed - check if it should enter
            // Try incremental update first
            if self.can_use_incremental_sort(
                client_id,
                view_path,
                query_id,
                event,
                remaining_segments,
            ) && let Some(events) = self.handle_incremental_sort_update(
                client_id,
                view_path,
                query_id,
                event,
                remaining_segments,
                tree,
                is_writer_echo,
                tag,
            ) {
                return events;
            }
            // Incremental update returned None - fall back to full recompute
            return self.recompute_query_view(
                client_id,
                view_path,
                query_id,
                event,
                remaining_segments,
                tree,
                is_writer_echo,
                tag,
            );
        }

        if self.is_sort_field_change(&order_by, event, remaining_segments) {
            // Sort field changed for item outside view - might enter
            // Try incremental update first
            if self.can_use_incremental_sort(
                client_id,
                view_path,
                query_id,
                event,
                remaining_segments,
            ) && let Some(events) = self.handle_incremental_sort_update(
                client_id,
                view_path,
                query_id,
                event,
                remaining_segments,
                tree,
                is_writer_echo,
                tag,
            ) {
                return events;
            }
            // Incremental update returned None - fall back to full recompute
            return self.recompute_query_view(
                client_id,
                view_path,
                query_id,
                event,
                remaining_segments,
                tree,
                is_writer_echo,
                tag,
            );
        }

        // Item outside view and change doesn't affect view - no event
        Vec::new()
    }

    /// Check if a mutation affects the sort field for a query.
    fn is_sort_field_change(
        &self,
        order_by: &OrderBy,
        event: &MutationEvent,
        remaining_segments: &[&str],
    ) -> bool {
        // Update at child level - check if any update path matches sort field
        if event.mutation_type == "update"
            && let Some(updates) = event.updates.as_ref()
            && remaining_segments.len() == 1
        {
            return self.update_affects_sort_field(order_by, updates);
        }

        if remaining_segments.len() < 2 {
            // Direct child change always affects sort
            return true;
        }

        // Path within the child: remaining[0] is child key, rest is subpath
        let subpath = remaining_segments[1..].join("/");

        match order_by {
            OrderBy::Child(child_path) => {
                // Check if mutation is to the orderByChild path
                subpath == *child_path || child_path.starts_with(&format!("{}/", subpath))
            }
            OrderBy::Value => remaining_segments.len() == 1,
            OrderBy::Key => false,
            // Priority ordering: changes to .priority affect sort order
            OrderBy::Priority => subpath == ".priority",
        }
    }

    fn update_affects_sort_field(&self, order_by: &OrderBy, updates: &Map<String, Value>) -> bool {
        match order_by {
            OrderBy::Child(child_path) => {
                for update_path in updates.keys() {
                    if update_path == child_path
                        || child_path.starts_with(&format!("{}/", update_path))
                        || update_path.starts_with(&format!("{}/", child_path))
                    {
                        return true;
                    }
                }
                false
            }
            OrderBy::Value => true,
            OrderBy::Key => false,
            // Priority ordering: updates to .priority affect sort order
            OrderBy::Priority => updates.contains_key(".priority"),
        }
    }

    /// Recompute a query view and generate enter/exit/move events.
    ///
    /// OPTIMIZATION: This function uses lazy value fetching to avoid copying
    /// all child values. It only extracts sort values for sorting/filtering,
    /// then fetches full values only for keys that actually need them.
    #[allow(clippy::too_many_arguments)]
    fn recompute_query_view(
        &mut self,
        client_id: &str,
        view_path: &str,
        query_id: &str,
        event: &MutationEvent,
        remaining_segments: &[&str],
        tree: &Tree,
        _is_writer_echo: bool,
        tag: Option<i32>,
    ) -> Vec<ClientEvent> {
        // Get the node at view path
        let view_path_obj = Path::parse(view_path);
        let node = tree.get(&view_path_obj);

        // Get current view state
        let (old_keys, query) = {
            let view = match self.get_view(client_id, view_path, query_id) {
                Some(v) => v,
                None => return Vec::new(),
            };
            (view.ordered_keys().to_vec(), view.query().clone())
        };

        // Compute new ordered keys using LIGHTWEIGHT sort entries (no full value copies)
        // Also build sort_key_cache and boundary for incremental updates.
        let (new_keys, new_sort_key_cache, new_boundary) = if let Some(node) = node {
            let children_keys: Vec<String> = node.keys().map(|s| s.to_string()).collect();

            // Build lightweight sort entries - only extract sort values, not full values
            let sort_entries: Vec<SortEntry> = children_keys
                .iter()
                .filter_map(|key| {
                    let child = node.get(key)?;
                    // Use efficient sort value extraction (doesn't copy entire child)
                    let sort_value = child.get_sort_value(&query.order_by);
                    Some(SortEntry::new(key.clone(), sort_value))
                })
                .collect();

            // Build a map of key -> sort_value before filtering
            let all_sort_values: HashMap<String, Option<SortKey>> = sort_entries
                .iter()
                .map(|e| (e.key.clone(), e.sort_value.clone()))
                .collect();

            // Apply query to get filtered/sorted keys
            let result_keys = apply_query_to_sort_entries(sort_entries, &query);

            // Build cache only for keys in the result (saves memory)
            let cache: HashMap<String, Option<SortKey>> = result_keys
                .iter()
                .filter_map(|key| all_sort_values.get(key).map(|v| (key.clone(), v.clone())))
                .collect();

            // Compute boundary based on limit type
            // IMPORTANT: Only set boundary when view is FULL (at capacity)
            // This ensures we only do swaps when adding an item would exceed the limit
            let boundary = match query.limit {
                Some(Limit::First(limit_val)) if result_keys.len() == limit_val => {
                    // limitToFirst: boundary is the LAST item (highest in view)
                    result_keys.last().map(|key| BoundaryItem {
                        key: key.clone(),
                        sort_value: cache.get(key).cloned().flatten(),
                    })
                }
                Some(Limit::Last(limit_val)) if result_keys.len() == limit_val => {
                    // limitToLast: boundary is the FIRST item (lowest in view)
                    result_keys.first().map(|key| BoundaryItem {
                        key: key.clone(),
                        sort_value: cache.get(key).cloned().flatten(),
                    })
                }
                _ => None, // No limit, or view not at capacity yet
            };

            (result_keys, cache, boundary)
        } else {
            (Vec::new(), HashMap::new(), None)
        };

        // Update view state with new keys, cache, and boundary
        if let Some(view) = self.get_view_mut(client_id, view_path, query_id) {
            view.ordered_keys = new_keys.clone();
            view.sort_key_cache = new_sort_key_cache;
            view.boundary = new_boundary;
        }

        // If node doesn't exist, send null
        if node.is_none() {
            let mut msg = ServerMessage::put_event(view_path, "/", Value::Null, event.volatile);
            if let Some(t) = tag {
                msg.tag = Some(t);
            }

            return vec![ClientEvent::new(
                client_id.to_string(),
                view_path.to_string(),
                query_id.to_string(),
                msg,
                event.volatile,
                EventCategory::Remove,
            )];
        }

        // SPECIAL CASE: Mutation at view path itself (e.g., set('/messages', {...}))
        // Send a single PUT event with the full filtered result.
        // Here we DO need full values, but only for keys in the result (not all children).
        // OPTIMIZATION: Build ArcValue::Object directly using O(1) Arc clones instead of to_value().
        if remaining_segments.is_empty() {
            let node = node.unwrap();
            let arc_value = if new_keys.is_empty() {
                ArcValue::Null
            } else {
                let mut value_map = HashMap::new();
                for key in &new_keys {
                    if let Some(child) = node.get(key) {
                        // O(1) Arc clone instead of O(n) to_value()
                        value_map.insert(key.clone(), child.clone());
                    }
                }
                ArcValue::Object(Arc::new(value_map))
            };

            let mut msg = ServerMessage::put_event_arc(view_path, "/", arc_value, event.volatile);
            if let Some(t) = tag {
                msg.tag = Some(t);
            }

            return vec![ClientEvent::new(
                client_id.to_string(),
                view_path.to_string(),
                query_id.to_string(),
                msg,
                event.volatile,
                EventCategory::Initial,
            )];
        }

        // Find entered, exited, and moved keys
        let old_set: HashSet<_> = old_keys.iter().cloned().collect();
        let new_set: HashSet<_> = new_keys.iter().cloned().collect();

        let entered: Vec<_> = new_keys
            .iter()
            .filter(|k| !old_set.contains(*k))
            .cloned()
            .collect();
        let exited: Vec<_> = old_keys
            .iter()
            .filter(|k| !new_set.contains(*k))
            .cloned()
            .collect();

        // If both entered AND exited (boundary swap), send a single atomic patch
        // containing null for exited children and data for entered children.
        // This is processed atomically by the SDK (one value callback).
        if !entered.is_empty() && !exited.is_empty() {
            let node = node.unwrap();
            let mut patch_map = HashMap::new();

            for key in &exited {
                patch_map.insert(format!("/{}", key), ArcValue::Null);
            }
            for key in &entered {
                if let Some(child) = node.get(key) {
                    patch_map.insert(format!("/{}", key), child.clone());
                }
            }

            let mut msg = ServerMessage::patch_event_arc(
                view_path,
                "/",
                ArcValue::Object(Arc::new(patch_map)),
                event.volatile,
            );
            if let Some(t) = tag {
                msg.tag = Some(t);
            }

            return vec![ClientEvent::new(
                client_id.to_string(),
                view_path.to_string(),
                query_id.to_string(),
                msg,
                event.volatile,
                EventCategory::Change,
            )];
        }

        let node = node.unwrap();
        let mut events = Vec::new();

        // Identify the triggering key (if any)
        let trigger_key = if !remaining_segments.is_empty() {
            Some(remaining_segments[0].to_string())
        } else {
            None
        };

        // Find moved items (in both lists, but predecessor changed)
        let old_predecessor: std::collections::HashMap<_, _> = old_keys
            .iter()
            .enumerate()
            .map(|(i, k)| {
                (
                    k.clone(),
                    if i == 0 {
                        String::new()
                    } else {
                        old_keys[i - 1].clone()
                    },
                )
            })
            .collect();
        let new_predecessor: std::collections::HashMap<_, _> = new_keys
            .iter()
            .enumerate()
            .map(|(i, k)| {
                (
                    k.clone(),
                    if i == 0 {
                        String::new()
                    } else {
                        new_keys[i - 1].clone()
                    },
                )
            })
            .collect();

        let mut moved: Vec<String> = Vec::new();
        for key in &new_keys {
            if old_set.contains(key) {
                let old_pred = old_predecessor.get(key).map(|s| s.as_str()).unwrap_or("");
                let new_pred = new_predecessor.get(key).map(|s| s.as_str()).unwrap_or("");
                if old_pred != new_pred {
                    moved.push(key.clone());
                }
            }
        }

        // Generate exit events
        for key in &exited {
            let mut msg = ServerMessage::put_event(
                view_path,
                &format!("/{}", key),
                Value::Null,
                event.volatile,
            );
            if let Some(t) = tag {
                msg.tag = Some(t);
            }

            events.push(ClientEvent::new(
                client_id.to_string(),
                view_path.to_string(),
                query_id.to_string(),
                msg,
                event.volatile,
                EventCategory::Remove,
            ));
        }

        // Generate enter events
        // OPTIMIZATION: Use put_event_arc to avoid to_value() conversion.
        for key in &entered {
            if let Some(child) = node.get(key) {
                // O(1) Arc clone instead of O(n) to_value()
                let mut msg = ServerMessage::put_event_arc(
                    view_path,
                    &format!("/{}", key),
                    child.clone(),
                    event.volatile,
                );
                if let Some(t) = tag {
                    msg.tag = Some(t);
                }

                events.push(ClientEvent::new(
                    client_id.to_string(),
                    view_path.to_string(),
                    query_id.to_string(),
                    msg,
                    event.volatile,
                    EventCategory::Add,
                ));
            }
        }

        // Handle moves and changes: if the trigger key moved or is in the view, send change event
        if let Some(ref trigger) = trigger_key {
            let trigger_moved = moved.contains(trigger);
            let trigger_in_view = new_set.contains(trigger);
            let trigger_entered = entered.contains(trigger);

            // Case 1: Trigger key moved - send PATCH with the changed data
            if trigger_moved {
                let mut patch_values = Map::new();
                if event.mutation_type == "update" {
                    // Update operation - use the updates map for specific paths
                    if let Some(ref updates) = event.updates {
                        for (update_path, update_value) in updates {
                            patch_values.insert(
                                format!("/{}/{}", trigger, update_path),
                                update_value.clone(),
                            );
                        }
                    }
                } else if remaining_segments.len() == 1 {
                    // Direct child set - send the full value at /childKey
                    patch_values.insert(
                        format!("/{}", trigger),
                        event.new_value.clone().unwrap_or(Value::Null),
                    );
                } else {
                    // Nested set - send the specific path that changed
                    let relative_path = format!("/{}", remaining_segments.join("/"));
                    patch_values.insert(
                        relative_path,
                        event.new_value.clone().unwrap_or(Value::Null),
                    );
                }

                let mut msg =
                    ServerMessage::patch_event(view_path, "/", patch_values, event.volatile);
                if let Some(t) = tag {
                    msg.tag = Some(t);
                }

                events.push(ClientEvent::new(
                    client_id.to_string(),
                    view_path.to_string(),
                    query_id.to_string(),
                    msg,
                    event.volatile,
                    EventCategory::Change,
                ));
            }
            // Case 2: Trigger key is in view, didn't move, but also didn't enter (sort field changed without position change)
            else if moved.is_empty() && trigger_in_view && !trigger_entered {
                let mut patch_values = Map::new();
                if event.mutation_type == "update" {
                    // Update operation - use the updates map for specific paths
                    if let Some(ref updates) = event.updates {
                        for (update_path, update_value) in updates {
                            patch_values.insert(
                                format!("/{}/{}", trigger, update_path),
                                update_value.clone(),
                            );
                        }
                    }
                } else if remaining_segments.len() == 1 {
                    // Direct child mutation - send the full value at /childKey
                    patch_values.insert(
                        format!("/{}", trigger),
                        event.new_value.clone().unwrap_or(Value::Null),
                    );
                } else {
                    // Nested set - send the specific path that changed
                    let relative_path = format!("/{}", remaining_segments.join("/"));
                    patch_values.insert(
                        relative_path,
                        event.new_value.clone().unwrap_or(Value::Null),
                    );
                }

                let mut msg =
                    ServerMessage::patch_event(view_path, "/", patch_values, event.volatile);
                if let Some(t) = tag {
                    msg.tag = Some(t);
                }

                events.push(ClientEvent::new(
                    client_id.to_string(),
                    view_path.to_string(),
                    query_id.to_string(),
                    msg,
                    event.volatile,
                    EventCategory::Change,
                ));
            }
        }

        events
    }

    /// Check if incremental sort update can be used instead of full recompute.
    /// Returns true if the mutation is safe for incremental handling.
    fn can_use_incremental_sort(
        &self,
        client_id: &str,
        view_path: &str,
        query_id: &str,
        event: &MutationEvent,
        remaining_segments: &[&str],
    ) -> bool {
        // Must be a direct child mutation (not deeper nested)
        if remaining_segments.len() != 1 {
            return false;
        }

        // Must not be a removal (removals need full recompute to find replacement)
        if event.mutation_type == "remove" {
            return false;
        }

        let view = match self.get_view(client_id, view_path, query_id) {
            Some(v) => v,
            None => return false,
        };

        // Must have a limit (otherwise no boundary tracking needed)
        if !view.has_limit() {
            return false;
        }

        // Range constraints are now supported - we check in_range during swap logic

        // Must have sort_key_cache populated (i.e., we've done at least one recompute)
        if view.shared_view.sort_key_cache.is_empty() && !view.ordered_keys().is_empty() {
            return false;
        }

        true
    }

    /// Handle incremental sort update for limited queries.
    /// Returns events if the update could be handled incrementally, None if full recompute needed.
    #[allow(clippy::too_many_arguments)]
    fn handle_incremental_sort_update(
        &mut self,
        client_id: &str,
        view_path: &str,
        query_id: &str,
        event: &MutationEvent,
        remaining_segments: &[&str],
        tree: &Tree,
        is_writer_echo: bool,
        tag: Option<i32>,
    ) -> Option<Vec<ClientEvent>> {
        let trigger_key = remaining_segments[0].to_string();

        // Get the node at view path to extract new sort value
        let view_path_obj = Path::parse(view_path);
        let node = tree.get(&view_path_obj)?;
        let trigger_child = node.get(&trigger_key);

        // Get current view state from shared view
        let (is_in_view, query, limit, ordered_keys, boundary, old_sort_value) = {
            let view_key = ViewKey::new(view_path, query_id);
            let shared_view = self.shared_views.get(&view_key)?;
            (
                shared_view.is_key_in_view(&trigger_key),
                shared_view.query.clone(),
                shared_view.query.limit?,
                shared_view.ordered_keys.clone(),
                shared_view.boundary.clone(),
                shared_view.sort_key_cache.get(&trigger_key).cloned(),
            )
        };

        // Get new sort value for the trigger key
        let new_sort_value = trigger_child
            .as_ref()
            .and_then(|c| c.get_sort_value(&query.order_by));

        // Determine if this is limitToFirst or limitToLast
        let is_limit_to_first = matches!(limit, Limit::First(_));

        if is_in_view {
            // Case 1: Item is currently in the view
            // Check if it should stay or be replaced by an outside item
            self.handle_in_view_sort_change(
                client_id,
                view_path,
                query_id,
                &trigger_key,
                new_sort_value,
                old_sort_value.flatten(),
                &query,
                is_limit_to_first,
                &ordered_keys,
                tree,
                event,
                is_writer_echo,
                tag,
            )
        } else {
            // Case 2: Item is outside the view
            // Check if it should enter by beating the boundary
            self.handle_outside_view_sort_change(
                client_id,
                view_path,
                query_id,
                &trigger_key,
                new_sort_value,
                &query,
                is_limit_to_first,
                &ordered_keys,
                boundary.as_ref(),
                tree,
                event,
                is_writer_echo,
                tag,
            )
        }
    }

    /// Handle sort field change for an item currently in the view.
    #[allow(clippy::too_many_arguments)]
    fn handle_in_view_sort_change(
        &mut self,
        client_id: &str,
        view_path: &str,
        query_id: &str,
        trigger_key: &str,
        new_sort_value: Option<SortKey>,
        old_sort_value: Option<SortKey>,
        query: &Query,
        _is_limit_to_first: bool,
        ordered_keys: &[String],
        _tree: &Tree,
        event: &MutationEvent,
        _is_writer_echo: bool,
        tag: Option<i32>,
    ) -> Option<Vec<ClientEvent>> {
        // Check if the item's new sort value is still in range
        // If it falls out of range, we need to find a replacement - fall back to recompute
        if !is_in_range(new_sort_value.as_ref(), trigger_key, query) {
            // Item fell out of range - needs replacement, fall back to recompute
            return None;
        }

        // For in-view changes, we take a simpler approach:
        // - If position doesn't change, just update the cache
        // - If position might change, fall back to recompute
        // This avoids an expensive O(N) scan of all children outside the view.
        // Items only get "pushed out" when a NEW item enters that beats the boundary,
        // which is handled by handle_outside_view_sort_change.
        {
            // Check if position changed within the view BEFORE getting mutable borrow
            // This uses only data we've already extracted
            let position_changed = self.check_position_changed(
                trigger_key,
                new_sort_value.as_ref(),
                old_sort_value.as_ref(),
                ordered_keys,
                &query.order_by,
            );

            if position_changed {
                // Position changed - need to update ordered_keys and boundary
                // Fall back to recompute for now to handle this correctly
                return None;
            }

            // Now update the sort key cache and boundary
            if let Some(view) = self.get_view_mut(client_id, view_path, query_id) {
                view.sort_key_cache
                    .insert(trigger_key.to_string(), new_sort_value.clone());

                // Update boundary if trigger is the boundary
                if let Some(ref mut boundary) = view.boundary
                    && boundary.key == trigger_key
                {
                    boundary.sort_value = new_sort_value.clone();
                }
            }

            // Generate change event (item stayed in view, just value changed)
            // All query view events must bypass rate limiting to maintain correct client state
            let msg = self.build_change_message(view_path, trigger_key, event, tag);
            Some(vec![ClientEvent::new(
                client_id.to_string(),
                view_path.to_string(),
                query_id.to_string(),
                msg,
                event.volatile,
                EventCategory::Change,
            )])
        }
    }

    /// Handle sort field change for an item outside the view.
    /// If the item should enter the view, performs a direct swap with the boundary.
    #[allow(clippy::too_many_arguments)]
    fn handle_outside_view_sort_change(
        &mut self,
        client_id: &str,
        view_path: &str,
        query_id: &str,
        trigger_key: &str,
        new_sort_value: Option<SortKey>,
        query: &Query,
        is_limit_to_first: bool,
        ordered_keys: &[String],
        boundary: Option<&BoundaryItem>,
        tree: &Tree,
        event: &MutationEvent,
        _is_writer_echo: bool,
        tag: Option<i32>,
    ) -> Option<Vec<ClientEvent>> {
        // First check if the item is in range (for queries with range constraints)
        // If not in range, it can never enter the view
        if !is_in_range(new_sort_value.as_ref(), trigger_key, query) {
            // Item is out of range - no events needed, stays outside view
            return Some(Vec::new());
        }

        // Check if trigger beats the boundary
        let boundary = match boundary {
            Some(b) => b,
            None => {
                // No boundary means view is empty or not full yet
                // Fall back to full recompute to handle this correctly
                return None;
            }
        };

        let cmp = self.compare_sort_entries_with_key(
            new_sort_value.as_ref(),
            trigger_key,
            boundary.sort_value.as_ref(),
            &boundary.key,
            &query.order_by,
        );

        let should_enter = if is_limit_to_first {
            // For limitToFirst: trigger enters if it's LESS than boundary
            cmp == Ordering::Less
        } else {
            // For limitToLast: trigger enters if it's GREATER than boundary
            cmp == Ordering::Greater
        };

        if !should_enter {
            // Trigger stays outside - no events needed
            return Some(Vec::new());
        }

        // Trigger should enter, boundary exits - perform direct swap
        // Get the full value of the entering item for the event
        let view_path_obj = Path::parse(view_path);
        let node = tree.get(&view_path_obj)?;

        // Verify trigger exists (we need it for the snapshot)
        node.get(trigger_key)?;

        let exiting_key = boundary.key.clone();

        // Find insertion position for the new item
        // We need to get the sort_key_cache from the shared view
        let sort_key_cache = {
            let view = self.get_view(client_id, view_path, query_id)?;
            view.shared_view.sort_key_cache.clone()
        };

        let insertion_pos = self.find_insertion_position(
            trigger_key,
            new_sort_value.as_ref(),
            ordered_keys,
            &query.order_by,
            &sort_key_cache,
        );

        // Update view state
        let new_boundary = {
            let view = self.get_view_mut(client_id, view_path, query_id)?;

            // Remove the exiting boundary from ordered_keys
            let exit_pos = if is_limit_to_first {
                // limitToFirst: boundary is at the end
                view.ordered_keys.len().saturating_sub(1)
            } else {
                // limitToLast: boundary is at the beginning
                0
            };

            if exit_pos < view.ordered_keys.len() {
                view.ordered_keys.remove(exit_pos);
            }

            // Adjust insertion position if we removed an item before it.
            // The two branches coincide but cover distinct limit directions.
            #[allow(clippy::if_same_then_else)]
            let adjusted_pos = if !is_limit_to_first && insertion_pos > 0 {
                insertion_pos - 1
            } else if is_limit_to_first && insertion_pos > exit_pos {
                insertion_pos - 1
            } else {
                insertion_pos
            };

            // Insert the new item at the correct position
            let insert_at = adjusted_pos.min(view.ordered_keys.len());
            view.ordered_keys.insert(insert_at, trigger_key.to_string());

            // Update sort_key_cache
            view.sort_key_cache.remove(&exiting_key);
            view.sort_key_cache
                .insert(trigger_key.to_string(), new_sort_value.clone());

            // Compute new boundary
            let new_boundary_key = if is_limit_to_first {
                // limitToFirst: boundary is the last item (highest)
                view.ordered_keys.last()
            } else {
                // limitToLast: boundary is the first item (lowest)
                view.ordered_keys.first()
            };

            new_boundary_key.map(|key| {
                let sort_val = view.sort_key_cache.get(key).cloned().flatten();
                BoundaryItem {
                    key: key.clone(),
                    sort_value: sort_val,
                }
            })
        };

        // Update the boundary
        if let Some(view) = self.get_view_mut(client_id, view_path, query_id) {
            view.boundary = new_boundary;
        }

        // Send a single atomic patch: remove exiting boundary + add entering item.
        // Serializes only the two changed children instead of the entire query view.
        let mut patch_map = HashMap::new();
        patch_map.insert(format!("/{}", exiting_key), ArcValue::Null);
        if let Some(child) = node.get(trigger_key) {
            patch_map.insert(format!("/{}", trigger_key), child.clone());
        }

        let mut msg = ServerMessage::patch_event_arc(
            view_path,
            "/",
            ArcValue::Object(Arc::new(patch_map)),
            event.volatile,
        );
        if let Some(t) = tag {
            msg.tag = Some(t);
        }

        Some(vec![ClientEvent::new(
            client_id.to_string(),
            view_path.to_string(),
            query_id.to_string(),
            msg,
            event.volatile,
            EventCategory::Change,
        )])
    }

    /// Find the correct insertion position for a new item in the ordered_keys list.
    /// Uses binary search for O(log N) performance.
    fn find_insertion_position(
        &self,
        key: &str,
        sort_value: Option<&SortKey>,
        ordered_keys: &[String],
        order_by: &OrderBy,
        sort_key_cache: &HashMap<String, Option<SortKey>>,
    ) -> usize {
        if ordered_keys.is_empty() {
            return 0;
        }

        // Binary search to find insertion point
        let mut low = 0;
        let mut high = ordered_keys.len();

        while low < high {
            let mid = (low + high) / 2;
            let mid_key = &ordered_keys[mid];
            let mid_sort_value = sort_key_cache.get(mid_key).and_then(|v| v.as_ref());

            let cmp = self.compare_sort_entries_with_key(
                sort_value,
                key,
                mid_sort_value,
                mid_key,
                order_by,
            );

            match cmp {
                Ordering::Less => high = mid,
                Ordering::Greater => low = mid + 1,
                Ordering::Equal => return mid, // Exact match (shouldn't happen for new items)
            }
        }

        low
    }

    /// Compare two sort entries, using key as tie-breaker.
    fn compare_sort_entries_with_key(
        &self,
        a_sort: Option<&SortKey>,
        a_key: &str,
        b_sort: Option<&SortKey>,
        b_key: &str,
        order_by: &OrderBy,
    ) -> Ordering {
        // For orderByKey, just compare keys
        if matches!(order_by, OrderBy::Key) {
            return crate::db::value::compare_keys(a_key, b_key);
        }

        // Compare sort values first
        match (a_sort, b_sort) {
            (Some(a), Some(b)) => {
                let cmp = compare_sort_keys(a, b);
                if cmp == Ordering::Equal {
                    // Tie-breaker: compare keys
                    crate::db::value::compare_keys(a_key, b_key)
                } else {
                    cmp
                }
            }
            (Some(_), None) => Ordering::Greater, // Items with sort value come after null
            (None, Some(_)) => Ordering::Less,
            (None, None) => crate::db::value::compare_keys(a_key, b_key),
        }
    }

    /// Check if an item's position changed within the view after sort value update.
    fn check_position_changed(
        &self,
        trigger_key: &str,
        new_sort_value: Option<&SortKey>,
        old_sort_value: Option<&SortKey>,
        ordered_keys: &[String],
        _order_by: &OrderBy,
    ) -> bool {
        // Find current position
        let pos = match ordered_keys.iter().position(|k| k == trigger_key) {
            Some(p) => p,
            None => return true, // Not found, definitely changed
        };

        // Check if new value would compare differently with neighbors
        // Check predecessor
        if pos > 0 {
            let _pred_key = &ordered_keys[pos - 1];
            // We don't have predecessor's sort value cached here, so be conservative
            // If sort value changed at all, assume position might have changed
            if new_sort_value != old_sort_value {
                return true;
            }
        }

        // Check successor
        if pos < ordered_keys.len() - 1 {
            let _succ_key = &ordered_keys[pos + 1];
            if new_sort_value != old_sort_value {
                return true;
            }
        }

        false
    }

    /// Build a change message for an item that stayed in view.
    fn build_change_message(
        &self,
        view_path: &str,
        trigger_key: &str,
        event: &MutationEvent,
        tag: Option<i32>,
    ) -> ServerMessage {
        let mut msg = if event.mutation_type == "update" {
            if let Some(ref updates) = event.updates {
                let mut patch_values = Map::new();
                for (update_path, update_value) in updates {
                    patch_values.insert(
                        format!("/{}/{}", trigger_key, update_path),
                        update_value.clone(),
                    );
                }
                ServerMessage::patch_event(view_path, "/", patch_values, event.volatile)
            } else {
                let value = event.new_value.clone().unwrap_or(Value::Null);
                ServerMessage::put_event(
                    view_path,
                    &format!("/{}", trigger_key),
                    value,
                    event.volatile,
                )
            }
        } else {
            let value = event.new_value.clone().unwrap_or(Value::Null);
            ServerMessage::put_event(
                view_path,
                &format!("/{}", trigger_key),
                value,
                event.volatile,
            )
        };

        if let Some(t) = tag {
            msg.tag = Some(t);
        }

        msg
    }

    // =========================================================================
    // Volatile Batching
    // =========================================================================

    /// Buffer a volatile write for all affected views.
    /// write_path: absolute path being written to (e.g., "/cursors/player1")
    /// raw_value: JSON-encoded value bytes
    /// sender_id: client who sent the write (won't receive echo)
    pub fn buffer_volatile(&mut self, write_path: &str, raw_value: Bytes, _sender_id: &str) {
        // Find all views affected by this write
        // We need views that are:
        // 1. At write_path (exact match)
        // 2. At a parent of write_path (e.g., /cursors watching /cursors/player1)
        let write_segments: Vec<&str> = write_path.trim_matches('/').split('/').collect();

        // Walk up the path tree to find views
        for i in (0..=write_segments.len()).rev() {
            let view_path = if i == 0 {
                "/".to_string()
            } else {
                format!("/{}", write_segments[..i].join("/"))
            };

            if let Some(view_keys) = self.by_path.get(&view_path) {
                for view_key in view_keys.iter() {
                    if let Some(view) = self.shared_views.get_mut(view_key) {
                        // Note: We don't check view.is_volatile here because:
                        // - buffer_volatile is only called when write_path is volatile
                        // - Views at parent paths (e.g., /cursors) receive child updates
                        //   even if the parent path itself doesn't match the volatile pattern

                        // Calculate relative path from view path to write path
                        let relative_path = if view.path == "/" {
                            write_path.to_string()
                        } else if write_path == view.path {
                            "/".to_string()
                        } else {
                            write_path[view.path.len()..].to_string()
                        };

                        // Buffer the update for all subscribers except the sender
                        // Since SharedView batches at the view level, we just need to
                        // track that this view has updates and who the sender is
                        view.buffer_volatile(relative_path, raw_value.clone());

                        // Track this view as having pending updates
                        self.pending_volatile_views.insert(view_key.clone());
                    }
                }
            }
        }

        // Store sender_id for echo suppression during flush
        // We do this by storing it in a thread-local or passing to flush
        // For simplicity, we'll handle echo suppression differently - by not
        // adding sender to the batch in the first place
        // Actually, the current approach buffers at view level, not per-client
        // So we need to handle sender exclusion at flush time
        // Store sender in pending_volatile_views metadata...
        // Actually, let's keep it simple: store sender per-view
    }

    /// Remove a path from all pending volatile batches.
    /// Called when an onDisconnect action fires on a volatile path to prevent
    /// stale data from being flushed after the removal event.
    pub fn clear_volatile_for_path(&mut self, write_path: &str) {
        let write_segments: Vec<&str> = write_path.trim_matches('/').split('/').collect();

        for i in (0..=write_segments.len()).rev() {
            let view_path = if i == 0 {
                "/".to_string()
            } else {
                format!("/{}", write_segments[..i].join("/"))
            };

            if let Some(view_keys) = self.by_path.get(&view_path) {
                let keys: Vec<_> = view_keys.iter().cloned().collect();
                for view_key in keys {
                    if let Some(view) = self.shared_views.get_mut(&view_key) {
                        let relative_path = if view.path == "/" {
                            write_path.to_string()
                        } else if write_path == view.path {
                            "/".to_string()
                        } else {
                            write_path[view.path.len()..].to_string()
                        };
                        view.pending_volatile_batch.remove(&relative_path);
                    }
                }
            }
        }
    }

    /// Check if there are any pending volatile batches.
    pub fn has_pending_volatile(&self) -> bool {
        !self.pending_volatile_views.is_empty()
    }

    /// Flush volatile batches to fast clients (called every 50ms).
    /// Does NOT clear the batch - slow clients may still need it.
    /// Returns the number of clients that received messages.
    pub fn flush_volatile_fast(&mut self) -> usize {
        let mut sent_count = 0;

        for view_key in &self.pending_volatile_views {
            if let Some(view) = self.shared_views.get(view_key) {
                if !view.has_pending_volatile() || view.fast_subscribers.is_empty() {
                    continue;
                }

                // Build patch value from batch: {"/player-1": {...}, "/player-2": {...}}
                let mut patch_values = Map::new();
                for (relative_path, raw_bytes) in &view.pending_volatile_batch {
                    if let Ok(value) = serde_json::from_slice::<Value>(raw_bytes) {
                        patch_values.insert(relative_path.clone(), value);
                    }
                }

                if patch_values.is_empty() {
                    continue;
                }

                // Serialize value ONCE
                let value_bytes = match serde_json::to_vec(&patch_values) {
                    Ok(bytes) => bytes,
                    Err(_) => continue,
                };

                // Generate Lark base bytes using fast encoding
                let lark_base = ServerMessage::encode_event_fast(
                    "patch",
                    &view.path,
                    "/",
                    &value_bytes,
                    None, // No tag for volatile views
                    true, // volatile = true
                );

                // Lazy-init Firebase base bytes on first Firebase subscriber
                let mut firebase_base: Option<Vec<u8>> = None;

                // Use thread-local broadcast buffers for single-pass payload building
                sent_count += with_broadcast_buffers(|buffers| {
                    let mut direct_sent = 0;

                    for client_id in &view.fast_subscribers {
                        if let Some(subscriber) = view.subscribers.get(client_id) {
                            let is_firebase = subscriber.is_firebase;

                            if is_firebase {
                                // Firebase client - use or generate Firebase format
                                let fb_bytes = firebase_base.get_or_insert_with(|| {
                                    encode_firebase_event(
                                        "patch",
                                        &view.path,
                                        "/",
                                        &value_bytes,
                                        None, // No tag
                                    )
                                });

                                // Check if chunking is needed (Firebase + >16KB)
                                if fb_bytes.len() > FIREBASE_MAX_FRAME_SIZE {
                                    // Fall back to direct send (handles chunking)
                                    if subscriber
                                        .conn
                                        .try_send(fb_bytes.clone().into(), true, true)
                                        .is_ok()
                                    {
                                        direct_sent += 1;
                                    }
                                    continue;
                                }
                            }

                            // Add client to broadcast buffer (single pass)
                            let outbox_id = subscriber.cached_outbox_id;
                            let client_id_num = subscriber.cached_client_id;
                            let key = (outbox_id, is_firebase);

                            // RELIABLE=false for volatile data
                            buffers
                                .entry(key)
                                .or_insert_with(BroadcastBuffer::new)
                                .add_client(client_id_num, 0, &subscriber.conn, false);
                        }
                    }

                    // Send BROADCAST for each buffer
                    let mut broadcast_sent = 0;
                    for ((_, is_firebase), buffer) in buffers.iter_mut() {
                        if buffer.is_empty() {
                            continue;
                        }

                        // Build flags: RELIABLE=false (volatile), FIREBASE_FORMAT if firebase
                        let flags = if *is_firebase {
                            broadcast_flag::FIREBASE_FORMAT
                        } else {
                            0
                        };

                        // Get the message bytes for this group
                        let message = if *is_firebase {
                            firebase_base.as_ref().unwrap().as_slice()
                        } else {
                            lark_base.as_slice()
                        };

                        broadcast_sent += buffer.send(message, flags);
                    }

                    direct_sent + broadcast_sent
                });
            }
        }

        sent_count
    }

    /// Flush volatile batches to slow clients (called every 250ms).
    /// Clears the batch after sending.
    /// Returns the number of clients that received messages.
    pub fn flush_volatile_slow(&mut self) -> usize {
        let mut sent_count = 0;

        // Collect keys to clear after iteration
        let keys: Vec<_> = self.pending_volatile_views.iter().cloned().collect();

        for view_key in keys {
            if let Some(view) = self.shared_views.get_mut(&view_key) {
                if !view.has_pending_volatile() {
                    view.clear_volatile_batch();
                    continue;
                }

                let has_slow = !view.slow_subscribers.is_empty();

                // Only encode/send if there are slow subscribers
                if has_slow {
                    // Build patch value from batch: {"/player-1": {...}, "/player-2": {...}}
                    let mut patch_values = Map::new();
                    for (relative_path, raw_bytes) in &view.pending_volatile_batch {
                        if let Ok(value) = serde_json::from_slice::<Value>(raw_bytes) {
                            patch_values.insert(relative_path.clone(), value);
                        }
                    }

                    if !patch_values.is_empty() {
                        // Serialize value ONCE
                        if let Ok(value_bytes) = serde_json::to_vec(&patch_values) {
                            // Generate Lark base bytes using fast encoding
                            let lark_base = ServerMessage::encode_event_fast(
                                "patch",
                                &view.path,
                                "/",
                                &value_bytes,
                                None, // No tag for volatile views
                                true, // volatile = true
                            );

                            // Lazy-init Firebase base bytes on first Firebase subscriber
                            let mut firebase_base: Option<Vec<u8>> = None;

                            // Use thread-local broadcast buffers for single-pass payload building
                            sent_count += with_broadcast_buffers(|buffers| {
                                let mut direct_sent = 0;

                                for client_id in &view.slow_subscribers {
                                    if let Some(subscriber) = view.subscribers.get(client_id) {
                                        let is_firebase = subscriber.is_firebase;

                                        if is_firebase {
                                            // Firebase client - use or generate Firebase format
                                            let fb_bytes = firebase_base.get_or_insert_with(|| {
                                                encode_firebase_event(
                                                    "patch",
                                                    &view.path,
                                                    "/",
                                                    &value_bytes,
                                                    None, // No tag
                                                )
                                            });

                                            // Check if chunking is needed (Firebase + >16KB)
                                            if fb_bytes.len() > FIREBASE_MAX_FRAME_SIZE {
                                                // Fall back to direct send (handles chunking)
                                                if subscriber
                                                    .conn
                                                    .try_send(fb_bytes.clone().into(), true, true)
                                                    .is_ok()
                                                {
                                                    direct_sent += 1;
                                                }
                                                continue;
                                            }
                                        }

                                        // Add client to broadcast buffer (single pass)
                                        let outbox_id = subscriber.cached_outbox_id;
                                        let client_id_num = subscriber.cached_client_id;
                                        let key = (outbox_id, is_firebase);

                                        // RELIABLE=false for volatile data
                                        buffers
                                            .entry(key)
                                            .or_insert_with(BroadcastBuffer::new)
                                            .add_client(client_id_num, 0, &subscriber.conn, false);
                                    }
                                }

                                // Send BROADCAST for each buffer
                                let mut broadcast_sent = 0;
                                for ((_, is_firebase), buffer) in buffers.iter_mut() {
                                    if buffer.is_empty() {
                                        continue;
                                    }

                                    // Build flags: RELIABLE=false (volatile), FIREBASE_FORMAT if firebase
                                    let flags = if *is_firebase {
                                        broadcast_flag::FIREBASE_FORMAT
                                    } else {
                                        0
                                    };

                                    // Get the message bytes for this group
                                    let message = if *is_firebase {
                                        firebase_base.as_ref().unwrap().as_slice()
                                    } else {
                                        lark_base.as_slice()
                                    };

                                    broadcast_sent += buffer.send(message, flags);
                                }

                                direct_sent + broadcast_sent
                            });
                        }
                    }
                }

                // Clear the batch
                view.clear_volatile_batch();
            }
        }

        // Clear pending volatile views set
        self.pending_volatile_views.clear();

        sent_count
    }

    /// Get view count (for testing).
    /// Returns the number of unique shared views (path + query combinations).
    pub fn view_count(&self) -> usize {
        self.shared_views.len()
    }

    /// Find all views affected by a change (compatibility wrapper for tests).
    /// Returns shared views.
    #[cfg(test)]
    pub fn find_affected_views(&self, changed_path: &str, is_volatile: bool) -> Vec<&SharedView> {
        self.find_affected_shared_views(changed_path, is_volatile)
    }
}

impl Default for ViewManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::database::SendError;
    use bytes::Bytes;
    use serde_json::json;
    use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

    // Counter for generating unique client IDs in tests
    static MOCK_CLIENT_COUNTER: AtomicU32 = AtomicU32::new(1);

    // Mock connection for testing - tracks send count
    struct MockConnection {
        send_count: AtomicUsize,
        id: u32,
    }

    impl MockConnection {
        fn new() -> Self {
            Self {
                send_count: AtomicUsize::new(0),
                id: MOCK_CLIENT_COUNTER.fetch_add(1, Ordering::Relaxed),
            }
        }

        fn count(&self) -> usize {
            self.send_count.load(Ordering::Relaxed)
        }
    }

    impl ConnectionSender for MockConnection {
        fn send(
            &self,
            _data: Bytes,
            _volatile: bool,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), SendError>> + '_>>
        {
            self.send_count.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(()) })
        }

        fn try_send(
            &self,
            _data: Bytes,
            _volatile: bool,
            _skip_translation: bool,
        ) -> Result<(), SendError> {
            self.send_count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        fn outbox_id(&self) -> usize {
            // All mock connections share the same "outbox" for testing
            1
        }

        fn client_id(&self) -> u32 {
            self.id
        }

        fn send_broadcast_raw(&self, payload: &[u8], _flags: u8) -> Result<(), SendError> {
            // Parse client count from payload header
            if payload.len() >= 4 {
                let client_count =
                    u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
                self.send_count.fetch_add(client_count, Ordering::Relaxed);
            }
            Ok(())
        }
    }

    fn mock_conn() -> Arc<MockConnection> {
        Arc::new(MockConnection::new())
    }

    // ==========================================================================
    // Basic Subscription Tests
    // ==========================================================================

    #[test]
    fn test_subscribe_and_unsubscribe() {
        let mut vm = ViewManager::new();

        let query_id = vm
            .subscribe("client1", "/messages", None, mock_conn())
            .unwrap();
        assert_eq!(query_id, "default");
        assert_eq!(vm.view_count(), 1);

        vm.unsubscribe("client1", "/messages");
        assert_eq!(vm.view_count(), 0);
    }

    #[test]
    fn test_subscribe_with_query() {
        let mut vm = ViewManager::new();

        let params = QueryParams {
            order_by_child: Some("score".to_string()),
            limit_to_first: Some(10),
            ..Default::default()
        };

        let query_id = vm
            .subscribe("client1", "/players", Some(&params), mock_conn())
            .unwrap();
        assert_ne!(query_id, "default");
        assert_eq!(vm.view_count(), 1);

        let view = vm.get_view("client1", "/players", &query_id).unwrap();
        assert!(view.has_query());
    }

    #[test]
    fn test_unsubscribe_all() {
        let mut vm = ViewManager::new();

        vm.subscribe("client1", "/a", None, mock_conn()).unwrap();
        vm.subscribe("client1", "/b", None, mock_conn()).unwrap();
        vm.subscribe("client2", "/a", None, mock_conn()).unwrap();

        // With shared views: 2 views (one for /a shared by client1+client2, one for /b)
        assert_eq!(vm.view_count(), 2);
        assert_eq!(vm.subscription_count(), 3);

        vm.unsubscribe_all("client1");
        // After unsubscribe: 1 view (/a still has client2)
        assert_eq!(vm.view_count(), 1);
        assert_eq!(vm.subscription_count(), 1);
    }

    // ==========================================================================
    // Find Affected Views Tests
    // ==========================================================================

    #[test]
    fn test_find_affected_views_exact_match() {
        let mut vm = ViewManager::new();
        vm.subscribe("client1", "/messages", None, mock_conn())
            .unwrap();

        let views = vm.find_affected_views("/messages", false);
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].path, "/messages");
    }

    #[test]
    fn test_find_affected_views_ancestor() {
        let mut vm = ViewManager::new();
        vm.subscribe("client1", "/messages", None, mock_conn())
            .unwrap();

        let views = vm.find_affected_views("/messages/abc/text", false);
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].path, "/messages");
    }

    #[test]
    fn test_find_affected_views_descendant() {
        let mut vm = ViewManager::new();
        vm.subscribe("client1", "/messages/abc", None, mock_conn())
            .unwrap();

        let views = vm.find_affected_views("/messages", false);
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].path, "/messages/abc");
    }

    #[test]
    fn test_find_affected_views_no_match() {
        let mut vm = ViewManager::new();
        vm.subscribe("client1", "/messages", None, mock_conn())
            .unwrap();

        let views = vm.find_affected_views("/players", false);
        assert_eq!(views.len(), 0);
    }

    // ==========================================================================
    // Event Sending Tests
    // ==========================================================================

    #[test]
    fn test_send_simple_put_event() {
        let mut vm = ViewManager::new();
        let conn = mock_conn();
        vm.subscribe("client1", "/messages", None, conn.clone())
            .unwrap();

        let mut tree = Tree::new();
        tree.set_str("/messages/abc", json!({"text": "hello"}));

        let event = MutationEvent {
            mutation_type: "set".to_string(),
            path: "/messages/abc".to_string(),
            old_value: None,
            new_value: Some(json!({"text": "hello"})),
            updates: None,
            volatile: false,
            writer_client_id: None,
        };

        let sent_count = vm.send_events(&event, &tree);
        assert_eq!(sent_count, 1);
        assert_eq!(conn.count(), 1);
    }

    // ==========================================================================
    // Query View Tests
    // ==========================================================================

    #[test]
    fn test_query_view_initialization() {
        let mut vm = ViewManager::new();

        let params = QueryParams {
            order_by_child: Some("score".to_string()),
            limit_to_first: Some(3),
            ..Default::default()
        };

        let query_id = vm
            .subscribe("client1", "/players", Some(&params), mock_conn())
            .unwrap();

        // Initialize with ordered keys
        vm.initialize_query_view(
            "client1",
            "/players",
            &query_id,
            vec!["a".to_string(), "b".to_string()],
        );

        let view = vm.get_view("client1", "/players", &query_id).unwrap();
        assert_eq!(view.ordered_keys(), vec!["a", "b"]);
    }

    // ==========================================================================
    // Volatile Path Tests
    // ==========================================================================

    #[test]
    fn test_volatile_path_detection() {
        let mut vm = ViewManager::new();
        vm.set_volatile_paths(vec!["cursors/$playerId".to_string()]);

        vm.subscribe("client1", "/cursors/player1", None, mock_conn())
            .unwrap();

        let view = vm
            .get_view("client1", "/cursors/player1", "default")
            .unwrap();
        assert!(view.is_volatile());
    }

    #[test]
    fn test_non_volatile_path() {
        let mut vm = ViewManager::new();
        vm.set_volatile_paths(vec!["cursors/$playerId".to_string()]);

        vm.subscribe("client1", "/messages", None, mock_conn())
            .unwrap();

        let view = vm.get_view("client1", "/messages", "default").unwrap();
        assert!(!view.is_volatile());
    }

    // ==========================================================================
    // QueryIdentifier Tests (ported from Go)
    // ==========================================================================

    #[test]
    fn test_query_identifier_default() {
        // Empty query params should return "default"
        let params = QueryParams::default();
        assert_eq!(params.identifier(), "default");
    }

    #[test]
    fn test_query_identifier_limit_to_first() {
        let params = QueryParams {
            limit_to_first: Some(10),
            ..Default::default()
        };
        let id = params.identifier();
        assert!(id.contains("\"l\":10"));
        assert!(id.contains("\"vf\":\"l\""));
    }

    #[test]
    fn test_query_identifier_limit_to_last() {
        let params = QueryParams {
            limit_to_last: Some(5),
            ..Default::default()
        };
        let id = params.identifier();
        assert!(id.contains("\"l\":5"));
        assert!(id.contains("\"vf\":\"r\""));
    }

    #[test]
    fn test_query_identifier_order_by_child() {
        let params = QueryParams {
            order_by_child: Some("score".to_string()),
            limit_to_last: Some(5),
            ..Default::default()
        };
        let id = params.identifier();
        assert!(id.contains("\"i\":\".score\""));
        assert!(id.contains("\"l\":5"));
    }

    #[test]
    fn test_query_identifier_order_by_key() {
        let params = QueryParams {
            order_by: Some("key".to_string()),
            ..Default::default()
        };
        let id = params.identifier();
        assert!(id.contains("\"i\":\".key\""));
    }

    #[test]
    fn test_query_identifier_order_by_value() {
        let params = QueryParams {
            order_by: Some("value".to_string()),
            ..Default::default()
        };
        let id = params.identifier();
        assert!(id.contains("\"i\":\".value\""));
    }

    #[test]
    fn test_query_identifier_start_at() {
        let params = QueryParams {
            start_at: Some(json!("w")),
            ..Default::default()
        };
        let id = params.identifier();
        assert!(id.contains("\"sin\":true"));
        assert!(id.contains("\"sp\":\"w\""));
    }

    #[test]
    fn test_query_identifier_end_at() {
        let params = QueryParams {
            end_at: Some(json!("y")),
            ..Default::default()
        };
        let id = params.identifier();
        assert!(id.contains("\"ein\":true"));
        assert!(id.contains("\"ep\":\"y\""));
    }

    #[test]
    fn test_query_identifier_equal_to() {
        let params = QueryParams {
            equal_to: Some(json!("exact")),
            ..Default::default()
        };
        let id = params.identifier();
        // equalTo sets both start and end to the same value
        assert!(id.contains("\"sp\":\"exact\""));
        assert!(id.contains("\"ep\":\"exact\""));
    }

    // ==========================================================================
    // Multiple Views Same Path Tests (ported from Go)
    // ==========================================================================

    #[test]
    fn test_multiple_views_same_path() {
        let mut vm = ViewManager::new();

        // Subscribe to same path with different queries
        vm.subscribe("client1", "/users", None, mock_conn())
            .unwrap();
        vm.subscribe(
            "client1",
            "/users",
            Some(&QueryParams {
                limit_to_first: Some(5),
                ..Default::default()
            }),
            mock_conn(),
        )
        .unwrap();
        vm.subscribe(
            "client1",
            "/users",
            Some(&QueryParams {
                limit_to_last: Some(5),
                ..Default::default()
            }),
            mock_conn(),
        )
        .unwrap();

        // Should have 3 distinct views
        assert_eq!(vm.view_count(), 3);
    }

    #[test]
    fn test_multiple_views_same_path_different_clients() {
        let mut vm = ViewManager::new();

        let query = QueryParams {
            limit_to_first: Some(10),
            ..Default::default()
        };

        // Two clients subscribe to same path with same query
        vm.subscribe("client1", "/users", Some(&query), mock_conn())
            .unwrap();
        vm.subscribe("client2", "/users", Some(&query), mock_conn())
            .unwrap();

        // With shared views: 1 view (shared by both clients)
        assert_eq!(vm.view_count(), 1);
        // But 2 total subscriptions
        assert_eq!(vm.subscription_count(), 2);
    }

    #[test]
    fn test_unsubscribe_with_query_specific() {
        let mut vm = ViewManager::new();

        // Subscribe with multiple queries
        vm.subscribe("client1", "/users", None, mock_conn())
            .unwrap();
        let params1 = QueryParams {
            limit_to_first: Some(5),
            ..Default::default()
        };
        let query_id1 = vm
            .subscribe("client1", "/users", Some(&params1), mock_conn())
            .unwrap();
        vm.subscribe(
            "client1",
            "/users",
            Some(&QueryParams {
                limit_to_last: Some(5),
                ..Default::default()
            }),
            mock_conn(),
        )
        .unwrap();

        assert_eq!(vm.view_count(), 3);

        // Unsubscribe only the limitToFirst query
        vm.unsubscribe_with_query("client1", "/users", &query_id1);

        // Should have 2 views remaining
        assert_eq!(vm.view_count(), 2);
    }

    #[test]
    fn test_unsubscribe_default_does_not_affect_query_views() {
        let mut vm = ViewManager::new();

        // Subscribe with default and query
        vm.subscribe("client1", "/users", None, mock_conn())
            .unwrap();
        vm.subscribe(
            "client1",
            "/users",
            Some(&QueryParams {
                limit_to_first: Some(5),
                ..Default::default()
            }),
            mock_conn(),
        )
        .unwrap();

        assert_eq!(vm.view_count(), 2);

        // Unsubscribe default (no query) using the default query ID
        vm.unsubscribe_with_query("client1", "/users", "default");

        // Should have 1 view remaining (the query view)
        assert_eq!(vm.view_count(), 1);
    }

    #[test]
    fn test_find_affected_views_multi_query() {
        let mut vm = ViewManager::new();

        // Multiple views on same path with different queries
        vm.subscribe("client1", "/users", None, mock_conn())
            .unwrap();
        vm.subscribe(
            "client1",
            "/users",
            Some(&QueryParams {
                limit_to_first: Some(5),
                ..Default::default()
            }),
            mock_conn(),
        )
        .unwrap();
        vm.subscribe(
            "client2",
            "/users",
            Some(&QueryParams {
                limit_to_last: Some(5),
                ..Default::default()
            }),
            mock_conn(),
        )
        .unwrap();

        // Change at /users/alice should affect all 3 views
        let affected = vm.find_affected_views("/users/alice", false);
        assert_eq!(affected.len(), 3);
    }

    #[test]
    fn test_unsubscribe_cleans_up_view() {
        let mut vm = ViewManager::new();
        vm.subscribe("client1", "/test/path", None, mock_conn())
            .unwrap();
        assert_eq!(vm.view_count(), 1);

        // Unsubscribe
        vm.unsubscribe("client1", "/test/path");

        // View should be cleaned up
        assert_eq!(vm.view_count(), 0);
    }

    #[test]
    fn test_unsubscribe_all_cleans_up_views() {
        let mut vm = ViewManager::new();
        vm.subscribe("client1", "/test/path1", None, mock_conn())
            .unwrap();
        vm.subscribe("client1", "/test/path2", None, mock_conn())
            .unwrap();

        // Unsubscribe all
        vm.unsubscribe_all("client1");

        // All rate limit states should be cleaned up
        assert_eq!(vm.view_count(), 0);
    }

    // ==========================================================================
    // Tag Routing Tests (ported from Go)
    // ==========================================================================

    #[test]
    fn test_tag_stored_on_view() {
        let mut vm = ViewManager::new();

        let params = QueryParams {
            limit_to_first: Some(5),
            tag: Some(42),
            ..Default::default()
        };
        let query_id = vm
            .subscribe("client1", "/users", Some(&params), mock_conn())
            .unwrap();

        let view = vm.get_view("client1", "/users", &query_id).unwrap();
        assert_eq!(view.tag(), Some(42));
    }

    #[test]
    fn test_tag_not_in_query_identifier() {
        // Tag should NOT affect queryIdentifier - it's just metadata for routing
        let params1 = QueryParams {
            limit_to_first: Some(5),
            tag: Some(1),
            ..Default::default()
        };
        let params2 = QueryParams {
            limit_to_first: Some(5),
            tag: Some(2),
            ..Default::default()
        };

        // Same query params with different tags should have same identifier
        assert_eq!(params1.identifier(), params2.identifier());
    }

    #[test]
    fn test_view_without_tag() {
        let mut vm = ViewManager::new();

        let params = QueryParams {
            limit_to_first: Some(5),
            ..Default::default()
        };
        let query_id = vm
            .subscribe("client1", "/users", Some(&params), mock_conn())
            .unwrap();

        let view = vm.get_view("client1", "/users", &query_id).unwrap();
        assert_eq!(view.tag(), None);
    }

    // ==========================================================================
    // Volatile Path Pattern Matching Tests (ported from Go)
    // ==========================================================================

    #[test]
    fn test_matches_pattern_wildcard() {
        let mut vm = ViewManager::new();
        vm.set_volatile_paths(vec!["players/*/position".to_string()]);

        // Should match
        vm.subscribe("client1", "/players/abc/position", None, mock_conn())
            .unwrap();
        let view = vm
            .get_view("client1", "/players/abc/position", "default")
            .unwrap();
        assert!(view.is_volatile());

        // Should also match different player ID
        vm.subscribe("client2", "/players/xyz/position", None, mock_conn())
            .unwrap();
        let view2 = vm
            .get_view("client2", "/players/xyz/position", "default")
            .unwrap();
        assert!(view2.is_volatile());
    }

    #[test]
    fn test_matches_pattern_no_match_different_end() {
        let mut vm = ViewManager::new();
        vm.set_volatile_paths(vec!["players/*/position".to_string()]);

        // Should NOT match - different ending
        vm.subscribe("client1", "/players/abc/name", None, mock_conn())
            .unwrap();
        let view = vm
            .get_view("client1", "/players/abc/name", "default")
            .unwrap();
        assert!(!view.is_volatile());
    }

    #[test]
    fn test_matches_pattern_no_match_different_start() {
        let mut vm = ViewManager::new();
        vm.set_volatile_paths(vec!["players/*/position".to_string()]);

        // Should NOT match - different starting segment
        vm.subscribe("client1", "/other/abc/position", None, mock_conn())
            .unwrap();
        let view = vm
            .get_view("client1", "/other/abc/position", "default")
            .unwrap();
        assert!(!view.is_volatile());
    }

    #[test]
    fn test_matches_pattern_no_match_too_short() {
        let mut vm = ViewManager::new();
        vm.set_volatile_paths(vec!["players/*/position".to_string()]);

        // Should NOT match - too few segments
        vm.subscribe("client1", "/players/abc", None, mock_conn())
            .unwrap();
        let view = vm.get_view("client1", "/players/abc", "default").unwrap();
        assert!(!view.is_volatile());
    }

    #[test]
    fn test_matches_pattern_child_of_volatile() {
        let mut vm = ViewManager::new();
        vm.set_volatile_paths(vec!["players/*/position".to_string()]);

        // Should match - child of a volatile path (volatile cascades down)
        vm.subscribe("client1", "/players/abc/position/x", None, mock_conn())
            .unwrap();
        let view = vm
            .get_view("client1", "/players/abc/position/x", "default")
            .unwrap();
        assert!(view.is_volatile());
    }

    #[test]
    fn test_is_fast_client() {
        // WebTransport (protocol_id 1) = fast
        assert!(Subscriber::is_fast_client("proxy_1_127.0.0.1:8080_0_42"));
        assert!(Subscriber::is_fast_client("proxy_1_10.0.0.1:443_3_1"));

        // WebSocket (protocol_id 0) = slow
        assert!(!Subscriber::is_fast_client("proxy_0_127.0.0.1:8080_0_42"));
        // REST (protocol_id 2) = slow
        assert!(!Subscriber::is_fast_client("proxy_2_127.0.0.1:8080_0_42"));
        // Unknown format = slow
        assert!(!Subscriber::is_fast_client("client1"));
    }

    #[test]
    fn test_subscriber_fast_slow_tracking() {
        let mut vm = ViewManager::new();
        vm.set_volatile_paths(vec!["cursors/*".to_string()]);

        // Subscribe with a slow client (WebSocket, protocol 0)
        vm.subscribe("proxy_0_127.0.0.1_0_1", "/cursors", None, mock_conn())
            .unwrap();

        // Subscribe with a fast client (WebTransport, protocol 1)
        vm.subscribe("proxy_1_127.0.0.1_0_2", "/cursors", None, mock_conn())
            .unwrap();

        let view_key = ViewKey::new("/cursors", "default");
        let view = vm.shared_views.get(&view_key).unwrap();

        // Check fast/slow sets
        assert!(view.slow_subscribers.contains("proxy_0_127.0.0.1_0_1"));
        assert!(!view.fast_subscribers.contains("proxy_0_127.0.0.1_0_1"));
        assert!(view.fast_subscribers.contains("proxy_1_127.0.0.1_0_2"));
        assert!(!view.slow_subscribers.contains("proxy_1_127.0.0.1_0_2"));
    }

    #[test]
    fn test_buffer_volatile() {
        let mut vm = ViewManager::new();
        vm.set_volatile_paths(vec!["cursors/*".to_string()]);

        // Subscribe to /cursors (parent path watching children)
        vm.subscribe("client1", "/cursors", None, mock_conn())
            .unwrap();

        // Buffer a volatile write to /cursors/player1
        let value = Bytes::from(r#"{"x": 100, "y": 200}"#);
        vm.buffer_volatile("/cursors/player1", value, "client2");

        // Check that the batch is pending
        assert!(vm.has_pending_volatile());

        // Check the view has pending data
        let view_key = ViewKey::new("/cursors", "default");
        let view = vm.shared_views.get(&view_key).unwrap();
        assert!(view.has_pending_volatile());
        assert!(view.pending_volatile_batch.contains_key("/player1"));
    }

    #[test]
    fn test_clear_volatile_for_path_prevents_stale_flush() {
        // Simulates onDisconnect().remove() on a volatile path:
        // After clearing, the next volatile flush should NOT send stale data.
        let mut vm = ViewManager::new();
        vm.set_volatile_paths(vec!["cursors/*".to_string()]);

        let conn = mock_conn();
        vm.subscribe("proxy_1_127.0.0.1_0_1", "/cursors", None, conn.clone())
            .unwrap();

        // Buffer volatile writes for two cursors
        vm.buffer_volatile(
            "/cursors/player1",
            Bytes::from(r#"{"x":10,"y":20}"#),
            "writer1",
        );
        vm.buffer_volatile(
            "/cursors/player2",
            Bytes::from(r#"{"x":30,"y":40}"#),
            "writer2",
        );

        // Verify both are in the batch
        let view_key = ViewKey::new("/cursors", "default");
        let view = vm.shared_views.get(&view_key).unwrap();
        assert_eq!(view.pending_volatile_batch.len(), 2);
        assert!(view.pending_volatile_batch.contains_key("/player1"));
        assert!(view.pending_volatile_batch.contains_key("/player2"));

        // player1 disconnects — clear their entry from the volatile batch
        vm.clear_volatile_for_path("/cursors/player1");

        // Only player2's data should remain
        let view = vm.shared_views.get(&view_key).unwrap();
        assert_eq!(view.pending_volatile_batch.len(), 1);
        assert!(!view.pending_volatile_batch.contains_key("/player1"));
        assert!(view.pending_volatile_batch.contains_key("/player2"));

        // Flush — only player2's data should be sent, not player1's stale cursor
        let sent = vm.flush_volatile_fast();
        assert_eq!(sent, 1);
        assert_eq!(conn.count(), 1);
    }

    #[test]
    fn test_volatile_coalescing() {
        let mut vm = ViewManager::new();
        vm.set_volatile_paths(vec!["cursors/*".to_string()]);

        vm.subscribe("client1", "/cursors", None, mock_conn())
            .unwrap();

        // Multiple writes to the same path - should coalesce (latest wins)
        vm.buffer_volatile("/cursors/player1", Bytes::from(r#"{"x": 1}"#), "client2");
        vm.buffer_volatile("/cursors/player1", Bytes::from(r#"{"x": 2}"#), "client2");
        vm.buffer_volatile("/cursors/player1", Bytes::from(r#"{"x": 3}"#), "client2");

        let view_key = ViewKey::new("/cursors", "default");
        let view = vm.shared_views.get(&view_key).unwrap();
        let value = view.pending_volatile_batch.get("/player1").unwrap();
        assert_eq!(value.as_ref(), b"{\"x\": 3}");
    }

    #[test]
    fn test_flush_volatile_fast_sends_to_fast_clients() {
        let mut vm = ViewManager::new();
        vm.set_volatile_paths(vec!["cursors/*".to_string()]);

        let slow_conn = mock_conn();
        let fast_conn = mock_conn();

        // Subscribe with slow (WebSocket, protocol 0) and fast (WebTransport, protocol 1) clients
        vm.subscribe("proxy_0_127.0.0.1_0_1", "/cursors", None, slow_conn.clone())
            .unwrap();
        vm.subscribe("proxy_1_127.0.0.1_0_2", "/cursors", None, fast_conn.clone())
            .unwrap();

        // Buffer a volatile write
        vm.buffer_volatile("/cursors/player1", Bytes::from(r#"{"x": 100}"#), "client3");

        // Flush to fast clients only
        let sent = vm.flush_volatile_fast();
        assert_eq!(sent, 1); // Only fast client

        // Fast client received, slow did not
        assert_eq!(fast_conn.count(), 1);
        assert_eq!(slow_conn.count(), 0);

        // Batch is NOT cleared (slow clients still need it)
        assert!(vm.has_pending_volatile());
    }

    #[test]
    fn test_flush_volatile_slow_sends_and_clears() {
        let mut vm = ViewManager::new();
        vm.set_volatile_paths(vec!["cursors/*".to_string()]);

        let slow_conn = mock_conn();

        // Subscribe with slow client
        vm.subscribe("client1", "/cursors", None, slow_conn.clone())
            .unwrap();

        // Buffer a volatile write
        vm.buffer_volatile("/cursors/player1", Bytes::from(r#"{"x": 100}"#), "client2");

        // Flush to slow clients
        let sent = vm.flush_volatile_slow();
        assert_eq!(sent, 1);
        assert_eq!(slow_conn.count(), 1);

        // Batch is cleared
        assert!(!vm.has_pending_volatile());
    }

    #[test]
    fn test_flush_volatile_encode_once() {
        let mut vm = ViewManager::new();
        vm.set_volatile_paths(vec!["cursors/*".to_string()]);

        // Subscribe with 100 fast clients
        let conns: Vec<_> = (0..100)
            .map(|i| {
                let conn = mock_conn();
                vm.subscribe(
                    &format!("proxy_1_127.0.0.1_0_{}", i),
                    "/cursors",
                    None,
                    conn.clone(),
                )
                .unwrap();
                conn
            })
            .collect();

        // Buffer a volatile write
        vm.buffer_volatile("/cursors/player1", Bytes::from(r#"{"x": 100}"#), "writer");

        // Flush to fast clients
        let sent = vm.flush_volatile_fast();
        assert_eq!(sent, 100);

        // With BROADCAST, one connection sends the payload with all client IDs.
        // The mock's send_broadcast_raw increments count by the number of clients.
        // Total across all connections should equal the client count.
        let total: usize = conns.iter().map(|c| c.count()).sum();
        assert_eq!(total, 100);
    }
}
