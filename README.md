# Lark

Interactive everything for the modern era.

## Quick start

On your local machine:

```bash
make up
```

That brings up `lark-server` and
`lark-edge`. The dashboard is at
http://localhost:8080/admin/ — the admin email and one-time password are
printed in the log on first start.

**Want it on the public internet instead?** `deploy/fly/quickstart.sh` stands up
a real, TLS-terminated Lark deployment on [Fly.io](https://fly.io) in a few
minutes (you'll need a domain). See [`deploy/fly/README.md`](deploy/fly/README.md).

## Repo layout

```
server/     The Rust DB engine (lark-server binary).
blob/       The blob storage engine and format (lark-blob crate).
edge/       The Go transport gateway + admin API + embedded SPA (lark-edge).
tools/      Development tools (chaos-monkey, lark-compact).
docs/       Wire-protocol reference + write-lifecycle deep-dive.
deploy/     Deployment configs (e.g. deploy/fly for Fly.io).
```

## Technical Underpinnings

Lark uses a **thread-per-core** model. Each CPU core runs its own Glommio
event loop with no shared mutable state. When a client connects, the
gateway hashes the database ID and routes the connection to the owning core. 
That database's data structures, subscriptions, WAL writer, and blob session all 
live on that one core, which avoids the need for mutexes or locks across cores.

Within a core, each database is a Glommio task with its own inbox
(`LocalChannel<InboxMessage>`). Writes, reads, subscribes, transactions
— all arrive as messages on that inbox and are processed one at a
time.

## Storage

Three layers, all per-database:

- **WAL** — `wal/000001.wal`, JSONL, flushed every 2 seconds, rotates at 5 MB.
- **Blob** — `blob.lark`, single binary file in the `lark-blob` format. Compact on-disk representation. Loaded lazily.
- **Sidecar** — `sidecar.lark`. Free list + pending dictionary keys, written alongside each compaction batch.

```
{data_dir}/{project}/{database}/
├── blob.lark            # Main blob file
├── blob.generation      # u64 in text — bumped on full re-compaction
├── sidecar.lark         # Free list + pending dictionary keys
├── sequence             # Last WAL sequence applied to the blob
└── wal/
    ├── 000001.wal
    ├── 000002.wal       # New file on >5 MB rotation
    └── ...
```

**Lazy tree**: blob-backed databases start with a Sentinel root — no
data is in memory until first access. A read at a path triggers
`promote_path`, which reads the subtree from the blob, replays any
pending WAL entries on top, and inserts the result into the tree.
Writes don't promote — `set_lazy` creates Sentinel intermediates that
hold new leaves without touching surrounding data. Idle promoted paths
get evicted back to Sentinels after ~30 s; re-promotion is
deterministic so no data is ever lost.

**Compaction**: a per-core `StorageWorker` (Glommio task on the lower-
priority queue) incrementally applies completed WAL files into the
blob. The sidecar's free list lets dead bytes get reused for new writes
so the blob stays roughly the size of its working set. Full
re-compaction (rare) runs via the separate `lark-compact` CLI when
`bytes_wasted ≥ 500 MB AND ≥ 20%` of blob size.

See `blob/README.md` for the blob format internals and
`docs/WRITE_LIFECYCLE.md` for what happens end-to-end when a write
arrives.

## Auth tokens

Lark validates JWT tokens at the gateway (`lark-edge`) and passes the
resolved auth into the database engine as part of the wire-protocol
join. Four token formats are supported, picked automatically based on
algorithm + claim shape:

| Format | Algorithm | Source | Typical use |
|---|---|---|---|
| Firebase ID tokens | RS256 | Google's Firebase Auth | Drop-in for apps already using Firebase Auth (must set `firebase-project-id` in the Project Settings) |
| Firebase custom tokens | RS256 | Your Firebase service-account key | Mint via the Firebase Admin SDK |
| Firebase legacy tokens | HS256 | Your project's Firebase legacy secret | The pre-2017 Firebase token format |
| Lark customer tokens | HS256 | Your service signs with the project's secret | The recommended format for new applications |

Rules see the resolved auth as the `auth` object:
- `auth.uid` — the token's `sub` (or `uid`) claim.
- `auth.token.<claim>` — any custom claim on the token.
- `auth == null` — unauthenticated.

Implementation: `server/src/auth/` validates tokens server-side;
`edge/auth/` does the gateway-side JWT verification.

## Data model

If you're contributing, four core types do most of the work:

**`ArcValue`** (`blob/src/arc_value.rs`)

The in-memory JSON representation. Like `serde_json::Value` but with
`Arc<T>` wrapping every container so clones are O(1) and structural
sharing across the tree is automatic:

```rust
pub enum ArcValue {
    Null,
    Bool(bool),
    Number(f64),
    String(Arc<str>),
    Object(Arc<BTreeMap<Arc<str>, ArcValue>>),
    Sentinel(Arc<BTreeMap<Arc<str>, ArcValue>>),  // Lazy tree placeholder
}
```

The `Sentinel` variant is what makes the lazy tree work: it's invisible
to reads (`exists()` → false, `to_value()` → Null), but holds children
that were written to deeper paths before surrounding blob data was
loaded. Reads through a Sentinel always trigger promotion.

Mutations use `Arc::make_mut` — if the refcount is 1, mutate in place;
otherwise clone first. So a SET that touches one node only clones that
one node, not the whole subtree.

**`Path`** (`server/src/db/path.rs`)

A parsed, validated database path (`/users/alice/score`). Validation
enforces key rules (no `.`, `#`, `$`, `[`, `]`, `/`;
max 768 bytes per key). Internally a `Vec<Arc<str>>`, so subpath
operations don't reallocate.

**`Query`** (`server/src/db/query.rs`)

Captures the query parameters (`orderBy`,
`limitToFirst`, `limitToLast`, `startAt`, `endAt`, `equalTo`). The
interesting bit is `SortKey` (`server/src/db/value.rs`), which wraps
`ArcValue` with mixed-type ordering (null < bool < number <
string < object).

**`InboxMessage`** (`server/src/db/database.rs`)

The single enum carrying work into a database. Every wire-protocol op
(set, update, delete, subscribe, transaction, ondisconnect, etc.) plus
internal events (storage-worker completion, eviction tick) becomes one
of these messages on the inbox. The database's `run()` loop is
essentially `while let Some(msg) = inbox.recv() { dispatch(msg) }`.

## Development

Environment setup, build/test commands, the contribution workflow, and the CLA
all live in **[CONTRIBUTING.md](CONTRIBUTING.md)**. The short version:

```bash
make up      # whole stack — dashboard at http://localhost:8080/admin/
make test    # fast lib tests
make help    # all Makefile targets
```

On macOS you build through a Linux dev container (Glommio needs `io_uring`); the
Makefile handles that transparently. See the [Data model](#data-model) and
[Storage](#storage) sections above for the internals you'll most often touch.

## License

AGPL v3. See [LICENSE](LICENSE).
