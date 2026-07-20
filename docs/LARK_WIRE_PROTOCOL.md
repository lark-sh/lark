# Lark Wire Protocol

JSON messages with shortened keys for bandwidth (~28% reduction). See `server/src/protocol/messages.rs` for full reference.

## Message Types

**Client → Server**: `j` (join), `au` (auth), `ua` (unauth), `s` (set), `u` (update), `d` (delete), `sb` (subscribe), `us` (unsubscribe), `o` (once), `od` (ondisconnect), `po` (pong), `tx` (transaction)

**Server → Client**: `a` (ack), `n` (nack), `ev` (event), `pi` (ping), `jc` (join complete), `ac` (auth complete), `oc` (once response)

## Connection Flow (Direct WebSocket/KCP)

Auth is validated by the proxy and included in the CONNECT message:

```
1. Client connects to proxy, sends join message (with optional token)
2. Proxy validates token (if provided), looks up backend server
3. Proxy sends CONNECT to backend with auth info in payload:
   {"uid": "user-123", "provider": "google", "claims": {...}, "is_admin": false}
4. Proxy forwards buffered messages (JOIN, etc.) to backend
5. Backend processes JOIN - auth is already complete from CONNECT
6. Client can immediately perform operations (no separate AUTH needed)
```

**Late auth changes**: If a user logs in/out after the connection is established, the proxy sends an `AUTH_CHANGED` (0x04) message to the backend with the new auth state. The backend updates the client's auth and re-validates subscriptions.

## Auth Operations

**Join** - Establishes connection to a database. The database ID uses `project/database` format.

```json
// Join a database (format: project/database)
{"o": "j", "d": "myproject/room-123", "r": "r1"}
{"jc": "r1", "vp": [...], "cid": "...", "st": 1704067200000}
```

**Auth change** - Clients can re-authenticate anytime:

```json
{"o": "au", "t": "<new-jwt-token>", "r": "r3"}
{"ac": "r3", "au": "new-user-456"}
```

**Unauth (sign out)** - Clears auth (becomes anonymous):

```json
{"o": "ua", "r": "r3"}
{"ac": "r3", "au": ""}
```

## Write Response Ordering

When a client performs a write (set/update/delete), the server responds with:
1. **Data event(s) first** - The write is echoed back to the writer (and all other subscribers)
2. **ACK/NACK second** - Confirmation that the write was accepted/rejected

## Compare-and-Swap (CAS) Writes

A SET can carry an optional compare-and-swap hash. Three shapes:

| Fields | Meaning |
|--------|---------|
| `{o:"s", p, v}` | Unconditional SET |
| `{o:"s", p, v, h:"<hash>"}` | CAS: only apply if current value's hash matches `<hash>` |
| `{o:"s", p, v, h:"", hash_provided:true}` | **Speculative SET**: only apply if path has no value (null counts as "no value") |

The hash is computed over the current server-side value. Two formats are accepted: Lark hash (JCS + SHA-256, 64-char lowercase hex) or Firebase hash (SHA-1 + base64, ~28 chars). The server detects the format from the hash string itself.

**Speculative SET semantics**: `h:""` with `hash_provided:true` is the wire shape Firebase clients use when running `transaction()` against a path they have no cached value for. The server accepts the write only if the current value at the path is null or absent, both treated as "doesn't exist," matching Firebase's semantic where null means deletion. A path that's been promoted from a blob-backed DB and is currently `Some(Value::Null)` in the in-memory tree counts as absent for this check.

**Failure mode**: hash mismatch or speculative-set against an existing non-null value returns `condition_failed`. This NACK does NOT taint subsequent writes (the client is expected to retry, typically by re-reading the path and computing a new hash).

```json
// Optimistic increment of a counter:
{"o": "s", "p": "/counter", "v": 6, "h": "<hash of current value 5>", "r": "r1"}
// → ACK if current is still 5, NACK condition_failed if it's been changed
```

## Volatile Writes

Volatile writes (`x: true`) are fire-and-forget with minimal overhead:
- **No request ID required** - `r` field is optional for volatile writes
- **No ack/nack sent** - server never responds to volatile writes
- **Silently dropped on error** - size limit, invalid JSON, or rules failure = silent drop
- **No deduplication tracking** - `pw` field is ignored, no write records kept

```json
{"o": "s", "p": "/players/abc/pos", "v": {"x":1,"y":2}, "x": true}
```

## Event Types (Delta-Based)

Events use a delta-based format with relative paths:

| Field | Description |
|-------|-------------|
| `ev` | Event type: `put` or `patch` |
| `sp` | Subscription path (absolute path client subscribed to) |
| `p` | Delta path (relative to subscription, `/` for full snapshot) |
| `v` | Value (delta data, `null` for deletion) |
| `x` | Volatile flag (true for volatile/ephemeral updates) |
| `ts` | Server timestamp in ms (included when `x` is true) |
| `tag` | Query tag (for tagged query subscriptions) |

**Example: Simple subscription flow**
```json
// Client subscribes
{"o": "sb", "p": "/players/abc", "r": "r1"}
{"a": "r1"}

// Initial snapshot (p="/" means full value at subscription path)
{"ev": "put", "sp": "/players/abc", "p": "/", "v": {"name": "Alice", "score": 100}}

// Delta update (only changed data, relative path)
{"ev": "put", "sp": "/players/abc", "p": "/score", "v": 150}

// Deletion (null value)
{"ev": "put", "sp": "/players/abc", "p": "/score", "v": null}
```

**Example: Query view with ordering**
```json
// Subscribe with orderByChild
{"o": "sb", "p": "/players", "r": "r1", "orderByChild": "score", "limitToFirst": 3}
{"a": "r1"}

// Initial snapshot (client sorts data locally)
{"ev": "put", "sp": "/players", "p": "/", "v": {"alice": {...}, "bob": {...}}}

// Child enters view
{"ev": "put", "sp": "/players", "p": "/charlie", "v": {...}}

// Update generates PATCH with only changed fields
{"ev": "patch", "sp": "/players", "p": "/", "v": {"/alice/score": 250}}

// Sort field change - client re-sorts to detect position changes
{"ev": "patch", "sp": "/players", "p": "/", "v": {"/alice/score": 350}}
```

The client generates `child_added`, `child_changed`, `child_removed`, `child_moved` events locally by diffing and re-sorting its cache.

## Volatile Batching

Volatile updates (cursor positions, etc.) are batched and sent as standard `patch` events with the volatile flag set. This provides compatibility while maintaining efficient batching.

**Protocol-Aware Batch Rates:**

| Transport | Batch Rate | Interval | Reason |
|-----------|------------|----------|--------|
| KCP/WebTransport (UDP) | 20Hz | 50ms | Low overhead, no head-of-line blocking |
| Proxy (fast clients) | 20Hz | 50ms | Proxy handles transport, backend uses fast rate |
| WebSocket (TCP) | 7Hz | 150ms | Higher syscall overhead, TCP HOL blocking |

The server determines transport type from connection ID prefixes (`kcp_`, `wt_`, `proxy_wt_`, or `ws_/fb_`) at subscribe time.

**Batch format (standard patch event):**
```json
{
  "ev": "patch",
  "sp": "/cursors",
  "p": "/",
  "v": {
    "/player1": {"x": 100, "y": 200},
    "/player2": {"x": 300, "y": 400}
  },
  "x": true,
  "ts": 1735945123456
}
```

**Structure:** Standard patch event where:
- `sp` is the subscription path
- `p` is "/" (patch at root of subscription)
- `v` contains `relativePath → value` entries
- `x: true` indicates this is a volatile/ephemeral update
- `ts` is the server timestamp for latency compensation

**Client handling:**
1. Receive standard `patch` event (same handler as regular patches)
2. Check `x: true` to identify volatile updates if needed for special handling
3. Apply patch values to local cache at `sp + relativePath`
4. Use `ts` for latency compensation/interpolation if needed

**Coalescing:** Multiple writes to the same path within a batch window are coalesced (latest value wins).

## Connection and Write Deduplication

On join, the server generates a unique **connection ID** (a push ID) and returns it in the join response:

```json
// Client joins (format: project/database)
{"o": "j", "d": "myproject/room-123", "r": "r1"}

// Server responds with connection ID
{"jc": "r1", "vp": ["players/*/position"], "cid": "-OhgLedGN0vr714Sh6PJ", "st": 1704067200000}
```

On reconnect, clients can pass their **previous connection ID** to enable write deduplication:

```json
{"o": "j", "d": "myproject/room-123", "r": "r1", "pcid": "-OhgLedGN0vr714Sh6PJ"}
```

**Write Deduplication Flow:**
1. Client generates unique request IDs for writes (UUIDs or monotonic counter)
2. Client tracks pending writes by request ID in `pendingWrites` map
3. On ack, client removes from pending
4. On reconnect, client passes previous connection ID and retries unacked writes
5. Server deduplicates using `(connectionID, requestID)` pairs - already-processed writes return ack without re-applying

## Local-First Writes and Tainted Write Detection

For local-first/optimistic updates, clients can include a `pw` (pending writes) field with each write operation:

```json
{"o": "s", "p": "/scores/alice", "v": 42, "r": "r2", "pw": ["r1"]}
```

**Tainted Write Detection:**
1. Client applies writes locally before server confirmation (optimistic updates)
2. Each write includes `pw` - request IDs of unconfirmed writes in the same View
3. If any write in `pw` was previously NACKed, the server silently ignores the write (no response)
4. The client handles cascading failures locally when it receives the first NACK

**Example Flow:**
```
r1: set /foo = 1  (pw: [])      → NACK (permission_denied)
r2: set /foo = 2  (pw: [r1])    → (silently ignored - client marks r2 failed when r1 NACK arrives)
r3: set /bar = 3  (pw: [])      → ACK - different View, no tainted dependency
```

**Error codes that record nacks for taint detection:**
- `permission_denied` - Write denied by rules
- `invalid_data` - Malformed JSON or validation error
- `invalid_path` - Bad path format
- `payload_too_large` - Exceeds size limits
- `internal_error` - Server error

**Error codes that do NOT record nacks:**
- `condition_failed` - Transaction condition not met (client handles retry)

Nacked write records are cleaned up after 5 minutes or on disconnect.
