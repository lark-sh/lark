# Contributing to Lark

Thanks for your interest in contributing! This guide covers how to set up a
development environment, the contribution workflow, and the conventions we follow.

By participating, you agree to abide by our
[Code of Conduct](CODE_OF_CONDUCT.md). To report a **security** vulnerability,
do **not** open an issue — follow [SECURITY.md](SECURITY.md).

## Ways to contribute

- **Bugs & features:** open an issue (templates will guide you). For questions and
  ideas, use [Discussions](https://github.com/lark-sh/lark/discussions).
- **Code & docs:** pull requests welcome. For anything large, please open an issue
  first so we can agree on the approach before you invest the time.

## Contributor License Agreement (CLA)

Lark is AGPL-licensed, and Bag of Holding, Inc. maintains the option to offer it
under commercial terms as well. To keep that possible, **all contributors must
sign a CLA** before their first contribution is merged:

- **Individuals:** the [Individual CLA](docs/cla/individual-cla.md) is signed
  automatically on your first pull request — a bot comments with instructions, and
  you sign by replying with the one-line statement it gives you. One signature
  covers all your future PRs.
- **Contributing as part of your job:** your employer should also have a
  [Corporate CLA](docs/cla/corporate-cla.md) on file (signed once, returned to
  team@lark.sh).

## Development setup

### Prerequisites

- **Rust** — latest stable (1.78+).
- **Docker** — required on macOS: Glommio uses `io_uring` (Linux-only), so the
  Makefile transparently runs Rust commands inside a Linux dev container.
- **Node.js** — for building the dashboard SPA.
- **Go** — for building `lark-edge`.

### Common Makefile targets

The root `Makefile` is the canonical surface; `make help` lists everything. The
ones you'll use most:

```bash
make dev-image     # one-time: build the Linux dev container image
make check         # cargo check --workspace inside the dev container
make test          # cargo test --lib (the fast common case)
make test-all      # full integration suite via test-everything.sh
make up            # docker compose up — brings up lark-server + lark-edge
```

### Building & running

```bash
make build-server  # release lark-server (in the dev container)
make build-edge    # cross-compile lark-edge to Linux
make build         # both
make build-spa     # dashboard SPA only

make up            # whole stack via docker compose (dashboard at :8080/admin/)
make shell         # shell inside the Linux dev container
# from inside the dev container, run the server directly in emulator mode:
cargo run -p lark-server -- --id=local-1 --hostname=localhost --proxy-port=7779 --emulator
```

### Testing

```bash
make test                                   # lib tests (fast)
make test-all                               # full integration suite
# a specific suite, from `make shell`:
cargo test -p lark-server --test integration_rules -j 2
# Firebase SDK wire-compat regression suite:
./test/run-firebase-sdk.sh
```

`-j 2` keeps parallel linking under control so the linker isn't OOM-killed on
memory-constrained machines. The Rust integration test harness
(`TestServer`/`TestClient`) lives in `server/tests/common/mod.rs`. See the
[Data model](#data-model) and [Storage](#storage)
sections for the internals you'll most often touch.

## Making changes

Common extension points (the relevant source files are noted inline):

- **A new wire operation:** define the message in
  `server/src/protocol/messages.rs`, add it to `InboxMessage` and handle it in the
  `run()` loop in `server/src/db/database.rs`, then add tests under
  `server/tests/integration_*.rs`.
- **A rules built-in:** add a method on `DataSnapshot`
  (`server/src/rules/snapshot.rs`) and dispatch it in
  `server/src/rules/expr/eval.rs`.
- **A query feature:** extend `Query` (`server/src/db/query.rs`), parsing
  (`server/src/protocol/messages.rs`), and evaluation.

### Code style

- Use `tracing` macros (`debug!`, `trace!`, `warn!`), not `println!`.
- Prefer `Option::map`/`and_then` over `if let` when transforming.
- Use `?` for error propagation; keep functions short; add `///` doc comments on
  public APIs.
- New behavior needs tests. If it's user-facing, update the docs and add a
  `CHANGELOG.md` entry under `[Unreleased]`.

## Pull request process

1. Fork and branch off `main`.
2. Make your change with tests; run `make test` (and relevant integration suites).
3. Open the PR using the template. Keep it focused — smaller PRs review faster.
4. Sign the CLA when the bot prompts you (first PR only).
5. CI must pass and a maintainer must approve before merge.

## Versioning

Lark follows [Semantic Versioning](https://semver.org). We're currently `0.x`:
while we stabilize the APIs, wire protocol, and on-disk format, minor releases may
include breaking changes (documented in `CHANGELOG.md`). The `1.0.0` line lands at
the public release.

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
