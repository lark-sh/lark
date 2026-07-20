# Write Lifecycle

This document describes the complete lifecycle of a write operation in Lark, from the moment it arrives at the proxy layer until events are delivered to all subscribed clients.

## Two Database Flavors: Ephemeral vs Blob-Backed

Lark databases come in two flavors, and the write lifecycle is largely the same for both, though a handful of steps behave differently. Each section below calls out blob-backed-specific behavior where it diverges.

| | Ephemeral | Blob-backed |
|---|---|---|
| Persistence | None; tree lives only in memory | `blob.lark` + WAL on disk |
| Tree mutations | `tree.set` / `tree.update` (real Object intermediates) | `tree.set_lazy` / `tree.update_lazy` (Sentinel intermediates) |
| `data.*` reads in rules | Direct tree access | `LazySnapshot`; may trigger `NeedsPromotion` if unloaded |
| `newData.*` in rules | `LazyUpdateSnapshot` (overlay over an empty tree) | `LazyUpdateSnapshot` (overlay over the real lazy tree) |
| Eviction | None | Idle promoted paths reverted to Sentinels every ~5s |
| WAL append | Skipped | Every write |
| Use case | Volatile playspaces, throwaway test DBs | Long-lived game/app data |

**Key concept for blob-backed DBs: Sentinels.** A `Sentinel` ArcValue represents "data exists at or below this point in the tree, but it hasn't been loaded from the blob yet." Sentinels make writes free of blob I/O (`set_lazy` walks down the path through Sentinel intermediates and just inserts the leaf), and reads opt-in to loading via `promote_path*`. See the "Storage" and "Data model" sections in the root [CONTRIBUTING](../CONTRIBUTING.md) for the lazy tree design.

## Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              WRITE LIFECYCLE                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Client ──WebSocket/WebTransport──► Proxy ──TCP──► Lark Server              │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ 1. ARRIVAL        Proxy receives, extracts client_id, routes to core   │ │
│  ├────────────────────────────────────────────────────────────────────────┤ │
│  │ 2. ROUTING        CoreHandler wraps in InboxMessage, pushes to DB      │ │
│  ├────────────────────────────────────────────────────────────────────────┤ │
│  │ 3. PROCESSING     Main loop batches messages, dispatches by op type    │ │
│  ├────────────────────────────────────────────────────────────────────────┤ │
│  │ 4. VALIDATION     Deduplication, value processing, server values       │ │
│  ├────────────────────────────────────────────────────────────────────────┤ │
│  │ 5. AUTHORIZATION  Rules evaluation with lazy blob promotion            │ │
│  ├────────────────────────────────────────────────────────────────────────┤ │
│  │ 6. MUTATION       Tree update (ArcValue COW), WAL append               │ │
│  ├────────────────────────────────────────────────────────────────────────┤ │
│  │ 7. BROADCAST      ViewManager finds affected views, generates events   │ │
│  ├────────────────────────────────────────────────────────────────────────┤ │
│  │ 8. DELIVERY       BROADCAST message to proxy for fan-out               │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  Client ◄──event──── Proxy ◄──BROADCAST──── Lark Server                     │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 1. Message Arrival (Transport Layer)

**File:** `edge/*` and `server/src/transport/proxy.rs`

### Proxy Connection

The proxy layer terminates TLS and WebSocket/WebTransport connections, then multiplexes clients over a single TCP connection to each Lark core.

```
ProxyListener (per-core, SO_REUSEPORT)
    │
    ▼
ProxyConnection
    │
    ├── Read TCP stream: [Length:4][Type:1][ClientID:4][Payload...]
    │
    ├── On DATA message (0x02):
    │   ├── Extract client_id from header
    │   ├── Create MessageTimestamps (if sampling enabled)
    │   └── Call handler.on_message(client, data, timestamps)
    │
    └── VirtualClient represents each multiplexed client
        ├── Unique ID: "proxy_{protocol}_{proxyAddr}_{coreId}_{clientID}"
        ├── Protocol: WebSocket (0x00) or WebTransport (0x01)
        └── Outbox sender for async responses
```

### Wire Format (Client → Server)

Clients send JSON messages with shortened keys:

```json
// SET operation
{"o": "s", "p": "/users/alice", "v": {"name": "Alice"}, "r": "req-1"}

// UPDATE operation (shallow merge)
{"o": "u", "p": "/users/alice", "v": {"score": 100}, "r": "req-2"}

// DELETE operation
{"o": "d", "p": "/users/alice", "r": "req-3"}
```

| Key | Meaning |
|-----|---------|
| `o` | Operation: `s` (set), `u` (update), `d` (delete) |
| `p` | Path |
| `v` | Value |
| `r` | Request ID (for ack/nack correlation) |
| `x` | Volatile flag (optional) |

## 2. Routing to Database (Server Layer)

**File:** `server/src/server/core_handler.rs`

### CoreHandler

The `CoreHandler` implements the `ProxyHandler` trait and routes messages to the correct database:

```rust
impl ProxyHandler for CoreHandler {
    async fn on_message(&self, client: &VirtualClient, data: Bytes, timestamps: Option<MessageTimestamps>) {
        // 1. Parse the client message
        let msg: ClientMessage = serde_json::from_slice(&data)?;

        // 2. Get database ID from client metadata
        let database_id = client.database_id();

        // 3. Get or create database handle
        let db_handle = self.get_or_create_database(database_id).await;

        // 4. Build InboxMessage
        let inbox_msg = InboxMessage {
            client_id: client.id().to_string(),
            message: Some(msg),
            volatile: msg.volatile.unwrap_or(false),
            timestamps,
            start_time: Instant::now(),
            // ... other fields
        };

        // 5. Send to database (non-blocking)
        db_handle.send(inbox_msg).await;
    }
}
```

### InboxMessage Structure

**File:** `server/src/db/database/mod.rs`

```rust
pub struct InboxMessage {
    pub client_id: String,
    pub message: Option<ClientMessage>,      // The actual operation
    pub volatile: bool,                       // High-frequency update flag
    pub disconnect: bool,                     // Client disconnection
    pub add_client: bool,                     // New client connection
    pub connection_id: String,
    pub conn: Option<Arc<dyn ConnectionSender>>,
    pub auth_info: Option<AuthInfo>,
    pub auth_update: Option<AuthInfo>,
    pub has_auth: bool,
    pub start_time: Instant,                  // For latency tracking
    pub timestamps: Option<MessageTimestamps>,
}
```

The `InboxMessage` uses flags instead of enum variants for simpler single-threaded processing.

## 3. Database Processing (Main Loop)

**File:** `server/src/db/database/run.rs`

### Message Loop Architecture

Each database runs as an independent Glommio task with its own inbox channel:

```rust
pub async fn run(&mut self) {
    // Load from disk if persistent
    if self.pending_disk_load {
        self.load_from_disk().await;
    }

    loop {
        // Wait for message with 50ms timeout
        select! {
            msg = self.inbox.recv() => {
                self.handle_message_internal(&msg).await;
            }
            _ = Timer::new(PERIODIC_INTERVAL) => {
                self.run_periodic_tasks().await;
            }
        }

        // Batch process immediately-ready messages
        let batch_start = Instant::now();
        let mut batch_count = 0;

        while batch_count < 128 && batch_start.elapsed() < Duration::from_millis(10) {
            match poll_immediate(self.inbox.recv()).await {
                Some(msg) => {
                    self.handle_message_internal(&msg).await;
                    batch_count += 1;
                }
                None => break,
            }
        }

        // Yield for fairness
        glommio::yield_if_needed().await;
    }
}
```

**Key design points:**
- **Batching**: Up to 128 messages or 10ms of work per iteration
- **Non-blocking poll**: `poll_immediate()` checks for ready messages without waiting
- **Cooperative yielding**: Prevents starvation of other databases on same core

### Message Dispatch

```rust
async fn handle_message_internal(&mut self, msg: &InboxMessage) {
    // Handle special message types first
    if msg.add_client {
        self.add_client_internal(msg).await;
    }
    if msg.disconnect {
        self.handle_disconnect(&msg.client_id).await;
    }
    if msg.has_auth {
        self.handle_auth_update(msg).await;
    }

    // Route protocol message by operation
    if let Some(ref protocol_msg) = msg.message {
        match protocol_msg.op.as_str() {
            "s" => self.handle_set(msg).await,
            "u" => self.handle_update(msg).await,
            "d" => self.handle_remove(msg).await,
            "t" | "tx" => self.handle_transaction(msg).await,
            "q" => self.handle_subscribe(msg).await,
            "uq" => self.handle_unsubscribe(msg).await,
            _ => { /* unknown op */ }
        }
    }

    // Record end-to-end latency
    let latency_us = msg.start_time.elapsed().as_micros() as u64;
    self.metrics.record_latency(latency_us);
}
```

## 4. Write Validation

**Files:** `server/src/db/database/handlers.rs`, `server/src/db/database/run.rs`

### Deduplication

Each client connection tracks processed request IDs to handle retries:

```rust
// Check if write depends on a previously nacked write (tainted)
if self.is_write_tainted(&msg.client_id, &msg.pending_writes) {
    return None;  // Silent drop
}

// Check if already processed (idempotent ack)
if self.is_write_processed(&msg.client_id, &request_id) {
    return Some(ServerMessage::ack(&request_id));
}
```

The `processed_writes` HashMap stores an `IndexSet<String>` per connection, bounded to 500 entries with O(1) eviction via `swap_remove`.

### Value Processing

```rust
// Extract value (null if not provided)
let value = msg.value.clone().unwrap_or(Value::Null);

// Validate .value/.priority patterns
self.validate_value_priority(&value, path_str)?;

// Process server values
let value = self.process_server_values(value, path_str);
```

**Server values** are special placeholders that the server replaces:

| Server Value | Replacement |
|--------------|-------------|
| `{".sv": "timestamp"}` | Current server timestamp (ms) |
| `{".sv": {"increment": N}}` | Atomic increment by N |

## 5. Authorization (Rules Evaluation)

**Files:** `server/src/db/database/auth.rs`, `server/src/rules/`

### can_write() with Lazy Promotion

Rules evaluation is synchronous, but accessing data that lives in the blob (or behind a Sentinel) requires async I/O. The system uses a retry loop: the rules engine signals which path it needs via `NeedsPromotion`, the database loads it, and evaluation retries.

```rust
async fn can_write(&mut self, client_id: &str, path: &str, new_data: Option<NewData>) -> bool {
    let evaluator = match &self.evaluator {
        Some(e) => e.clone(),
        None => return true,  // No rules configured = allow all
    };

    let auth = self.clients.get(client_id)?.rules_auth.clone();
    let tree_accessor: Arc<dyn TreeGetter> = Arc::new(TreeAccessor::new(...));

    let ctx = RulesContext {
        auth,
        root_tree: Some(tree_accessor),
        path: path.to_string(),
        new_data,                    // ← lazy: NewData enum, not a JsonValue
        ..Default::default()
    };

    for _attempt in 0..MAX_PROMOTION_RETRIES {
        match evaluator.can_write(&ctx) {
            Ok(allowed) => return allowed,
            Err(NeedsPromotion { path }) => {
                self.load_from_blob(&path).await?;
                // retry
            }
        }
    }
    false
}
```

`new_data` is an `Option<NewData>`: `None` for deletes (no value being written), `Some(NewData::Set { .. })` for SET, `Some(NewData::Update { .. })` for UPDATE. The same `NewData` is reused at every level of the rules cascade; the rules engine builds a level-specific snapshot via `NewData::snapshot_at(tree, ctx.path)` per level.

### Snapshot Types

The rules engine sees three kinds of snapshots, all of which implement the `SnapshotTrait` consumed by the expression evaluator:

| Variable | Snapshot | What it represents | Lazy? |
|---|---|---|---|
| `data.*` | `LazySnapshot` | Existing tree at the rule's path | Yes; `NeedsPromotion` if unloaded |
| `root.*` | `LazySnapshot` (at `""`) | Existing tree at root | Yes; same |
| `newData.*` | `LazyUpdateSnapshot` | Post-write merged view | Yes; overlay of updates onto tree |

`LazySnapshot` and `LazyUpdateSnapshot` both navigate freely (`child()`, `parent()` are O(1) path math) and only trigger `NeedsPromotion` when an accessor (`val()`, `exists()`, `has_child()`, etc.) needs data that isn't loaded.

### Lazy newData Model

`NewData::snapshot_at(view_path)` always returns a `LazyUpdateSnapshot`. The snapshot resolves accesses by classifying `view_path` into one of four regions relative to the write's `(base_path, updates)`:

```
Update at base="/" with updates = {"characters/abc/core": {...}}

view_path classification:
  /characters/abc/core            → AtUpdateLeaf       — value comes from updates map
  /characters/abc/core/level      → InsideUpdateValue  — descend into the update value
  /                               → Overlay            — merged view (tree ∪ updates)
  /characters                     → Overlay            — same
  /unrelated_sibling              → TreeOnly           — defer to tree (LazySnapshot-style)
```

For SET, we synthesize a one-key updates map `{set_path: value}` at root, so the same snapshot type handles both write kinds.

Rules like `auth.token.is_admin === true` (the production "admin only" pattern) therefore never construct a snapshot at all: `eval_expr` checks `uses_new_data` per rule before building anything. For rules that *do* read `newData.x`, only `x`'s path is touched. Untouched siblings stay unloaded, untouched paths in a multi-path UPDATE never get walked, and the eager `merged_data = existing + updates` allocation is gone.

### Validate Children: Only What's Written

`.validate` rules at children of the UPDATE path fire only on children the UPDATE actually writes. `validate_children` walks `NewData::writes_at(ctx.path)`, which yields `(child_name, partial_value)` for each touched leaf, grouping multi-path keys that share a child (`{"a/b": v1, "a/c": v2}` → one entry `("a", {b: v1, c: v2})`). Untouched tree-existing siblings are not validated.

### Volatile Path Fast Path

Paths marked `.volatile` in rules have restricted expressions for performance:

```rust
// Simple rules allowed for volatile paths:
// - auth.* access (already in memory)
// - $wildcard captures (already parsed)
// - newData.* access (resolves from the in-memory updates map)
// - Literals and operators

// Expensive rules denied for volatile paths:
// - data.* access (requires tree lookup, may trigger blob I/O)
// - root.* access (requires cross-path lookup)

if is_volatile && !rule.is_simple {
    return false;  // Deny without evaluation
}
```

`newData.*` is allowed because the `LazyUpdateSnapshot`'s AtUpdateLeaf / InsideUpdateValue regions resolve entirely from the updates map without touching the tree.

### Permission Denied Response

```rust
if !self.can_write(client_id, path_str, Some(NewData::from_set(path_str.into(), value.clone()))).await {
    debug!("NACK SET: permission denied at {} for client {}", path_str, client_id);
    self.metrics.record_permission_denial();
    self.record_nacked_write(client_id, &request_id);

    return Some(ServerMessage::nack(
        &request_id,
        error::PERMISSION_DENIED,
        "Permission denied"
    ));
}
```

## 6. Mutation (Tree & WAL)

### Volatile Write Path (Fast)

**File:** `server/src/db/database/handlers.rs`

Volatile writes skip the tree and WAL entirely:

```rust
if self.is_volatile_path(path_str) {
    // Encode value to bytes once
    let value_bytes = Bytes::from(serde_json::to_vec(&value)?);

    // Buffer for batched sending
    self.view_manager.buffer_volatile(path_str, value_bytes, client_id);

    self.metrics.record_write(msg.payload_size);
    return None;  // No ack for volatile writes
}
```

### Normal Write Path

#### Tree Mutation

**File:** `server/src/db/tree.rs`, `blob/src/arc_value.rs`

The mutation path branches on `is_blob_backed()`. Both branches end up doing the same logical thing (apply the write to the tree), but the blob-backed branch goes through Sentinel-aware lazy variants.

**Ephemeral DB:**
```rust
let path = Path::parse(path_str);
self.tree.write().unwrap().set(&path, value.clone());      // SET
self.tree.write().unwrap().update(&path, &updates);        // UPDATE
self.tree.write().unwrap().remove(&path);                  // DELETE
```

`set` / `update` walk through real Object intermediates and create empty Object intermediates when needed.

**Blob-backed DB:**
```rust
self.tree.write().unwrap().set_lazy(&path, value.clone());     // SET
self.track_sentinels_after_write(path_str);

self.tree.write().unwrap().update_lazy(&path, &updates);       // UPDATE
for key in updates.keys() {
    self.track_sentinels_after_write(&format!("{path_str}/{key}"));
}
```

`set_lazy` / `update_lazy` use `set_path_mut_sentinel` under the hood, which walks the path and creates **Sentinel** intermediates for any key that doesn't yet exist in the tree. This is what makes blob-backed writes free of blob I/O: you can write `/a/b/c/d` even if the path's ancestors have never been loaded, and each missing intermediate becomes a `Sentinel` that signals "this subtree may have data in the blob, load on demand."

**No eager `promote_path`**: neither SET nor UPDATE loads existing data before the write. Earlier versions of this code did an eager `promote_path` in `handle_update` to materialize `merged_data = existing + updates` for rules eval; that's gone. The rules engine now consumes `NewData` lazily (see §5), so any blob path a rule actually reads is loaded inside the rules retry loop, not unconditionally upfront.

The tree itself uses `ArcValue` (from `lark-blob`) for copy-on-write semantics:

```rust
pub enum ArcValue {
    Null,
    Bool(bool),
    Number(f64),
    String(Arc<str>),
    Object(Arc<BTreeMap<Arc<str>, ArcValue>>),
    Sentinel(Arc<BTreeMap<Arc<str>, ArcValue>>),  // Lazy tree structural support
}
```

**Key properties:**
- O(1) cloning via `Arc` reference counting
- In-place mutation when refcount == 1 via `Arc::make_mut()`
- Structural sharing for unmodified subtrees
- Sentinels hold children written before blob data was loaded (invisible to reads: `exists()` → false, `to_value()` → Null)

#### Sentinel Tracking (Blob-backed only)

Every Sentinel in the tree must have its path recorded in the database's `sentinel_paths: BTreeSet<String>` index. The tree itself is the source of truth, and `sentinel_paths` is a derived index that gives `has_sentinel_at_or_below(path)` an O(log n) range query instead of a recursive subtree walk.

The invariant is one-way: `sentinel_paths` must be a **superset** of every actual Sentinel in the tree. Stale-extra entries are tolerated (cause unnecessary promotions); missing entries are catastrophic (cause skipped promotions, returning Sentinels to the encoder).

Every write site that introduces or removes Sentinels must keep `sentinel_paths` in sync:

| Mutation site | Sentinel handling |
|---|---|
| `handle_set` / `handle_update` / `handle_transaction` / `handle_disconnect` | `track_sentinels_after_write(leaf_path)` walks ancestors and inserts any that are now Sentinels |
| `evict_idle_paths` (housekeeping) | clears `sentinel_paths` descendants of the evicted path, inserts the path itself |
| `promote_path_unchecked` (deep) | promoted_value is Sentinel-free (read_subtree + non-lazy WAL replay); `track_sentinels_after_write(path)` walks ancestors only |
| `promote_path_shallow` | promoted_value can have *deep* Sentinel intermediates (lazy WAL replay through Sentinel children); recursive `collect_sentinel_paths` walk inserts every Sentinel found |
| `handle_remove` | `remove_sentinel_paths_below(path)` clears the deleted subtree's tracking |

`promote_path_deep` includes a defensive check + `warn!` if it ever finds an untracked Sentinel. It promotes anyway, but flags the I3 violation in the logs so we can find the offending mutation site. See `Database::find_sentinel_tracking_violations` for a test-time audit hook.

#### WAL Write

**File:** `server/src/storage/wal.rs`

```rust
self.wal_write_set(path_str, &value);
```

Implementation:
1. **Canonicalize SET-to-null**: if `value.is_null()`, route to `wal_write_delete` instead. Without this, `WalEntry::set(path, Null)` serializes as `{"o":"s","v":null}`, which serde reads back as `value: None` on restart, so the SET arm of WAL replay would silently skip the entry. This canonicalization gives the WAL a single encoding for "delete" (`WalOp::Delete`, no `v` field) regardless of which wire shape produced it (Lark `remove()`, Firebase `set(null)`, transaction SET-with-null, on-disconnect SET-with-null).
2. Create `WalEntry::Set { path, value }`
3. Append to WAL buffer (JSONL format)
4. Also append to in-memory `pending_wal_entries` (for replay during future promotions)
5. Check rotation threshold (5MB)
6. Mark `wal_dirty = true` for next sync

Steps 1–6 touch only memory: the entry lands in an in-memory buffer, not the WAL
file. The ACK is returned to the client at this point; the write becomes durable
at the next sync (below).

WAL sync happens in the periodic task at an interval controlled by
`LARK_WAL_SYNC_INTERVAL_MS` (default 2000ms):

```rust
// run.rs — every LARK_WAL_SYNC_INTERVAL_MS
if now.duration_since(last_wal_sync) >= wal_sync_interval {
    self.sync_wal().await;  // flush buffer → WAL file (+ fdatasync if enabled)
}
```

`sync_wal` flushes the buffer to the WAL file and, when `LARK_FSYNC_ON_WAL_FLUSH`
is `true`, issues an `fdatasync` so the data is durable on the physical device.

**Durability window.** With the defaults, a write is ACKed before it is flushed,
so up to `LARK_WAL_SYNC_INTERVAL_MS` of acknowledged writes sit in memory, and
each flush only reaches the OS page cache (no `fdatasync`). That is safe across a
**process** crash, since the kernel writes the page cache back, but a **power
loss** or kernel panic can lose the most recent writes. Two knobs tighten this
(see [DEPLOYMENT.md](DEPLOYMENT.md#durability)):

- `LARK_WAL_SYNC_INTERVAL_MS=0`: flush before every write's ACK (synchronous;
  the database waits, so a delivered ACK means the write is at least in the page
  cache).
- `LARK_FSYNC_ON_WAL_FLUSH=true`: `fdatasync` on every flush (power-safe).

Setting both gives strict per-write durability at the cost of write latency.

On WAL rotation, a `CompactionRequest` is sent to the per-core StorageWorker via `LocalChannel`.

#### Record Processed

```rust
self.record_processed_write(client_id, &request_id);
```

## 7. Event Broadcasting

**File:** `server/src/db/database/broadcast.rs`

### broadcast_mutation()

```rust
async fn broadcast_mutation(
    &mut self,
    path: &str,
    mutation_type: &str,       // "set", "update", "remove"
    new_value: Option<Value>,
    updates: Option<Map<String, Value>>,
    volatile: bool,
    writer_client_id: Option<&str>,
) {
    // Create mutation event
    let event = MutationEvent {
        mutation_type: mutation_type.to_string(),
        path: path.to_string(),
        new_value,
        updates,
        volatile,
        writer_client_id: writer_client_id.map(|s| s.to_string()),
        ..Default::default()
    };

    // Collect affected views
    let view_infos = self.view_manager.collect_affected_view_infos(&event);

    // Process in batches of 10 for fairness
    const VIEWS_PER_BATCH: usize = 10;
    let mut event_count = 0;

    for (batch_idx, chunk) in view_infos.chunks(VIEWS_PER_BATCH).enumerate() {
        let batch_sent = {
            let tree = self.tree.read().unwrap();
            self.view_manager.send_events_for_views(chunk, &event, &tree)
        };

        event_count += batch_sent;

        // Yield between batches for fairness
        if batch_idx > 0 {
            glommio::yield_if_needed().await;
        }
    }

    self.metrics.record_events_sent(event_count);
}
```

### View Identification

**File:** `server/src/db/subscription/events.rs`

The ViewManager finds all subscriptions affected by a mutation:

```
Mutation at: /users/alice/score

Affected subscriptions:
├── /users/alice/score  (exact match)
├── /users/alice        (parent - mutation is under subscription)
├── /users              (ancestor - mutation is under subscription)
└── /                   (root - sees all changes)

NOT affected:
├── /users/bob          (sibling - different subtree)
└── /users/alice/name   (sibling child - different path)
```

For each affected view, returns `AffectedViewInfo`:
- `path`: Subscription path
- `query_id`: Unique subscription identifier
- `has_query`: Whether it has query constraints (orderBy, limit)
- `is_volatile`: Whether marked volatile in rules

## 8. Event Delivery

### Broadcast Buffer Architecture

**File:** `server/src/db/subscription/mod.rs`

Thread-local reusable buffers minimize allocations:

```rust
thread_local! {
    static BROADCAST_BUFFERS: RefCell<HashMap<(usize, bool), BroadcastBuffer>> = ...
}

pub struct BroadcastBuffer {
    data: Vec<u8>,              // Binary: [count:4][[id:4][tag:4]...][msglen:4][msg...]
    client_count: u32,
    conn: Option<Arc<dyn ConnectionSender>>,
    has_reliable: bool,
}
```

### Event Encoding

**File:** `server/src/protocol/messages.rs`

```rust
pub fn encode_event_fast(
    event_type: &str,        // "put" or "patch"
    subscription_path: &str, // "/users"
    relative_path: &str,     // "/alice" or "/"
    value_bytes: &[u8],      // Pre-serialized JSON
    tag: Option<i32>,        // For query views
    volatile: bool,
) -> Vec<u8>
```

Output format:
```json
{"ev":"put","sp":"/users","p":"/alice","v":{"name":"Alice"},"tag":5}
```

| Key | Meaning |
|-----|---------|
| `ev` | Event type: `put` (set/delete) or `patch` (update) |
| `sp` | Subscription path |
| `p` | Relative path within subscription |
| `v` | New value (or null for delete) |
| `tag` | Query tag (for ordered results) |
| `x` | Volatile flag |

### BROADCAST Wire Format

Format sent to proxy for fan-out:

```
[Length:4][Type:0x0B][Flags:1][ClientCount:4][[ClientID:4][Tag:4]...][MsgLen:4][MsgBytes...]
```

| Field | Size | Description |
|-------|------|-------------|
| Length | 4 bytes | Total frame length |
| Type | 1 byte | `0x0B` = BROADCAST |
| Flags | 1 byte | RELIABLE (0x01), UNRELIABLE (0x02), COMPRESSED (0x04) |
| ClientCount | 4 bytes | Number of recipients |
| ClientEntries | 8 bytes each | [ClientID:4][Tag:4] per recipient |
| MsgLen | 4 bytes | Message payload length |
| MsgBytes | variable | The event JSON |

The proxy receives one BROADCAST message and fans out to all listed clients, inserting per-client tags as needed.

### Volatile Batch Flushing

**File:** `server/src/db/database/broadcast.rs`

Volatile writes are batched and flushed at different rates:

```rust
// Fast flush: 50ms interval (20Hz) for KCP/WebTransport clients
fn flush_volatile_fast(&mut self) {
    if !self.view_manager.has_pending_volatile() { return; }
    let event_count = self.view_manager.flush_volatile_fast();
    self.metrics.record_events_sent(event_count);
}

// Slow flush: 250ms interval (4Hz) for WebSocket clients + clear batch
fn flush_volatile_slow(&mut self) {
    if !self.view_manager.has_pending_volatile() { return; }
    let event_count = self.view_manager.flush_volatile_slow();
    self.metrics.record_events_sent(event_count);
}
```

**Coalescing**: Multiple writes to the same path within a batch window → latest value wins.

## Complete Example: Normal Write (Blob-backed DB)

### Client Request

```json
{"o": "s", "p": "/players/alice/score", "v": 100, "r": "req-1"}
```

### Flow

1. **Proxy receives** WebSocket frame, extracts client_id
2. **CoreHandler.on_message** wraps in InboxMessage, sends to database
3. **Database main loop** pops message, calls `handle_set`
4. **Deduplication** checks `processed_writes["client-1"]` → not found, continue
5. **Rules evaluation** `can_write("/players/alice/score", NewData::Set { .. })`
   - Rules eval consumes `newData` via `LazyUpdateSnapshot`. If a rule reads `data.*` or `root.*` and the path isn't loaded, the engine returns `NeedsPromotion` and the retry loop calls `load_from_blob` for that specific path. For a rule that only references `auth.*` / `newData.*`, no tree access happens at all.
   - Rule passes → continue
6. **Tree mutation** `tree.set_lazy(["players", "alice", "score"], 100)`: Sentinel intermediates created for any unloaded ancestor; `track_sentinels_after_write("/players/alice/score")` records them
7. **WAL append** `{"o":"s","p":"/players/alice/score","v":100}` + stored in `pending_wal_entries`
8. **Record processed** `processed_writes["client-1"].insert("req-1")`
9. **Broadcast** finds affected views:
    - `/players` → 3 subscribers
    - `/players/alice` → 1 subscriber
10. **Encode event** `{"ev":"put","sp":"/players","p":"/alice/score","v":100}`
11. **Send BROADCAST** to proxy with client IDs
12. **Send ack** `{"a":"req-1"}` to requesting client

## Complete Example: Volatile Write

### Client Request

```json
{"o": "s", "p": "/cursors/player1", "v": {"x": 100, "y": 200}, "x": true}
```

### Flow

1. **Proxy receives** frame
2. **CoreHandler** routes to database with `volatile: true`
3. **Database** calls `handle_set` with volatile flag
4. **Rules evaluation** (simple rules only for volatile)
5. **Volatile path detected** → skip tree, WAL
6. **Buffer in ViewManager** `pending_volatile_batch["/player1"] = {"x":100,"y":200}`
7. **No ack sent** (volatile writes don't ack)
8. **50ms later** `flush_volatile_fast()` sends to KCP/WebTransport clients
9. **250ms later** `flush_volatile_slow()` sends to WebSocket clients, clears batch

### Event Format

```json
{"ev": "patch", "sp": "/cursors", "p": "/player1", "v": {"x": 100, "y": 200}, "x": true}
```

## Performance Optimizations

| Optimization | Location | Benefit |
|--------------|----------|---------|
| **Arc-wrapped auth** | `database/auth.rs` | O(1) auth cloning during rules eval |
| **ArcValue COW** | `arc_value.rs` | O(1) tree cloning, in-place mutation |
| **Broadcast buffers** | `subscription/mod.rs` | One encode → N client sends |
| **Fast event encoding** | `messages.rs` | Direct string concat, no JSON serialize |
| **Volatile batching** | `subscription/volatile.rs` | 5-25x reduction in event sends |
| **Tiered flush rates** | `database/broadcast.rs` | 20Hz fast / 4Hz slow clients |
| **Lazy blob promotion** | `database/promotion.rs` | Only loads blob data when a read accesses it |
| **Lazy newData** | `rules/snapshot.rs` | UPDATE rules cascade builds snapshots on demand instead of materializing `merged_data` per ancestor, with no eager `tree.get_value` walks for rules that don't read `newData.*` |
| **writes_at validate** | `rules/snapshot.rs` | `.validate` fires only on children being written, not on tree-existing untouched siblings |
| **Message batching** | `proxy.rs` | 256KB or 3ms → reduced syscalls |
| **View batch processing** | `database/broadcast.rs` | 10 views per batch, yield between |
| **Deduplication** | `database/run.rs` | IndexSet with O(1) eviction |

## Latency Tracking

When debug timing is enabled, `MessageTimestamps` tracks checkpoints:

1. **Proxy receive** - TCP frame arrives
2. **DB inbox push** - Message queued to channel
3. **DB inbox pop** - Message dequeued for processing
4. **Work complete** - Processing finished
5. **Event sent** - Events dispatched

Final latency recorded: `start_time.elapsed()` after all processing completes.
