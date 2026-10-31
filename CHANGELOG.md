# Changelog

Notable changes to Lark are documented here. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows [Semantic Versioning](https://semver.org/).

> **Pre-1.0:** Lark is versioned `0.x`. Breaking changes in `0.x` releases are rare and always called out here; if a release changes the on-disk format, it ships with a migration path. Even so, **back up before upgrading** (see [BACKUP.md](docs/BACKUP.md)) — downgrades aren't supported, so the way back from a bad upgrade is a restore. For how we think about `1.0.0`, see [Project status](README.md#project-status) in the README.

## [Unreleased]

### Security

- Updated klauspost/compress to 1.18.7 (GHSA-259r-337f-4rfw, an integer overflow in `s2.NewDict`). lark-edge uses only the package's zstd decoder, so it wasn't affected; the update clears the alert.
- Refreshed the dashboard's and the sticky-notes example's npm lockfiles to pick up fixed versions of vite, source-map-js, browserslist, baseline-browser-mapping, js-yaml and brace-expansion. These are build and dev-server tools; none of them ships in the edge image, which embeds only the built dashboard.
- Updated rustls to 0.23.46 (GHSA-h2v8-394m-p7vv, GHSA-c8m5-c952-6cm4, GHSA-w44r-93j7-qv28, GHSA-vp88-798v-h67f). lark-server uses it only for outbound HTTPS through `ureq`, without brotli certificate compression or ECH, and never Debug-formats TLS sessions; at worst, a later connection could fall back to a classical-only X25519 key exchange.
- lark-edge now builds with Go 1.27.3, and its image uses Alpine 3.24. Go 1.25 and Alpine 3.20 no longer receive security fixes. The move fixes standard library issues the edge could reach, including an HTTP/2 server crash (GO-2026-6617), HTTP/2 memory exhaustion (GO-2026-6603), HTTP/1 connection desynchronization (GO-2026-6613) and trust in compromised intermediate certificates (GO-2026-6809).
- Updated the edge's Go modules, including golang.org/x/text to fix an infinite loop (GO-2026-5970) and a panic on crafted input (GO-2026-6629). quic-go and webtransport-go stay at their current versions.
- Updated rustls-webpki to 0.103.16 and webpki-roots to 1.0.11 (RUSTSEC-2026-0382, RUSTSEC-2026-0381) so that lark-server's outbound HTTPS no longer trusts distrusted intermediate certificates.
- Updated zstd to 0.14 (RUSTSEC-2026-0391, RUSTSEC-2026-0398, RUSTSEC-2026-0403). lark-server uses it only for one-shot compression of outbound messages, so the streaming and seekable issues didn't apply; the update clears the advisories.
- Removed jsonwebtoken, which nothing used, and with it rsa (RUSTSEC-2023-0071) and crypto-bigint (RUSTSEC-2026-0387). Neither crate's vulnerable code was reachable.

### Fixed

- Reading a key that didn't exist, when its parent was already in memory, added that key to the parent with a null value. Any later read of the parent returned the null child, a limited query could return it in place of a real row, and a conditional write on the parent could fail because its hash had changed. This also happened to a parent emptied by a delete, once its path was read. A missing key is now simply absent from its parent.
- Deleting every child of a node left an empty collection in the blob, which reads returned as `{}` after compaction. Reads now skip empty collections at any depth.

### Changed

- The `lark-server` image now builds from `Cargo.lock` with the toolchain pinned in `rust-toolchain.toml`. It previously resolved the newest compatible crates and the current stable Rust at build time, so images could contain versions CI had never tested.
- Updated the pinned Rust toolchain to 1.99.1.

## [0.3.0] — 2026-10-11

### Fixed

- A SET or DELETE of the database root (a client setting or removing the whole database, or a REST PUT or DELETE on `/.json`) never reached the blob: compaction skipped root-path WAL entries, so the server's in-memory data changed but the old data came back after a restart or eviction. Updates at the root were not affected. Compaction now applies a root write by deleting every existing top-level key and then applying the batch's writes.
- Deleting a key that had already been deleted, after its value had earlier moved within the blob, wrote a null byte at a stale offset, which could land inside unrelated live data and corrupt it (a string could come back truncated to null, or a container could become unreadable). A delete that finds an already-deleted key now does nothing.
- When one compaction batch wrote below a primitive value (which replaces it with an object) and then deleted everything it had written there, the blob kept the old primitive, so the deleted value came back after a restart or eviction even though clients had seen it removed. Compaction now applies such a batch the way the writes happened in order: the primitive is replaced, and the node is removed if nothing written under it survives.
- A shallow read of a blob-backed collection that had ever had a child deleted could fail, or list the deleted key with whatever bytes sat at a stale offset as its value. Deleted children stay in a collection's index as zero-size null entries, and the shallow reader treated them as live primitive values. This broke shallow `once` queries (which were NACKed) and shallow promotions on such paths. Shallow reads now skip deleted children, matching how path lookups already treat them.
- Fixed a bug where bursts of data were causing issues when echoing back to the client.
- Firebase-protocol responses over 16 KB were split into frames at byte offsets, so a multi-byte character (an em dash, a bullet, any non-ASCII text) landing on a 16,384-byte boundary produced a frame that was not valid UTF-8 on its own. Browsers fail the whole WebSocket on such a frame (close code 1007), and because the client then reconnected and re-requested the same data, any database containing such a character at that position could never finish loading. Frames are now split only at character boundaries. The edge also logs a WARN when a client closes with a protocol-fault code, since that always indicates a server-side bug.
- The server rejected inbound Firebase frames over 16,384 bytes, but the Firebase JS SDK splits at 16,384 UTF-16 code units, so a large write containing non-ASCII text could be refused as malformed. The inbound cap is now three bytes per unit.
- The edge dropped clients whose per-connection outbox exceeded 1000 messages. Firebase-protocol responses are split into 16 KB frames, so a client loading a large database on an ordinary connection could exhaust the count mid-sync, be disconnected, reconnect, re-issue every query, and loop forever without ever finishing. The outbox is now bounded by bytes (`CLIENT_OUTBOX_MAX_BYTES`, default 256 MB) rather than by message count.
- The edge's flat 10-second write deadline could drop a native-protocol client on a slow link while it was still receiving a single large frame. The deadline now scales with payload size (`CLIENT_WRITE_DEADLINE` + payload / `CLIENT_WRITE_MIN_BYTES_PER_SEC`), so it only fires when a client has stopped taking bytes.
- Compaction stored nulls written by updates instead of treating them as deletes, and when a batch deleted a node and then wrote beneath it, the node's old children came back. Nulls written by updates are now deletes, and a deleted node's old children stay deleted.
- `.validate` rules were evaluated at nodes a write leaves null, so nulling a validated path in a multi-path update was denied, and removing a field its parent requires was allowed. Lark now skips `.validate` at any node a write leaves null and checks it everywhere else, the same way for set, update and remove.
- Paths that arrived without a leading slash or with a trailing slash could leak not-yet-loaded data into reads, and the response then failed to encode. Such paths are now handled like their canonical form, and a response that fails to encode logs the request path.
- The edge could crash when a client sent a second AUTH message while the first was still being validated before its connection finished routing: both auth handlers closed the same channel. A second AUTH in that window is now dropped, and the handler already running picks up the newer token. CI now runs the Go tests with the race detector.
- When a connection between the edge and a server died, messages that needed it were dropped silently. The edge now redials until every core has its full set of connections again, closes the clients that were routed through the dead connection so they reconnect, and fails a message that has no live connection right away, with a WARN.

### Added

- Graceful shutdown: on SIGTERM or SIGINT, lark-server stops its databases so each flushes its WAL before the process exits, giving each core up to 8 seconds (under the 10 seconds Docker and Kubernetes allow before SIGKILL). This works when the server runs as PID 1 in a container.
- Server tunables for the remaining per-client and per-request caps that used to be compile-time constants: `LARK_MAX_TRANSACTION_OPS`, `LARK_MAX_ON_DISCONNECT_ACTIONS_PER_CLIENT`, `LARK_MAX_ON_DISCONNECT_BYTES_PER_CLIENT`, and `LARK_MAX_RESPONSE_SIZE` (each also a `--flag`). Defaults are unchanged.

### Changed

- Every place the edge or server drops a client or a message because a defensive limit was hit now logs at WARN with the reason and context: outbox full, write deadline exceeded, backend disconnected, SSE buffer overflow, subscription cap, onDisconnect cap, transaction op cap, response-size rejections, and reliable messages dropped on a full send buffer (throttled to the first and every 1000th). These used to be at debug or trace, or not logged at all.
- The edge logs `Client outbox approaching limit` at WARN when one client's queue crosses `CLIENT_OUTBOX_WARN_BYTES` (default 64 MB), and `Client outbox usage` at INFO once a minute while anything is queued, so the headroom on the cap is visible before anyone hits it.

### Security

- Updated rustls to 0.23.45 (GHSA-2mjx-qc3c-rqvc: TLS 1.3 handshake messages accepted across encryption-level boundaries). lark-server uses it only for outbound HTTPS through `ureq`; inbound TLS is terminated by lark-edge.
- Updated xxhash-rust to 0.8.19 (GHSA-6g2r-675j-hx59, in the xxh3 custom-secret API). Lark uses only xxh64, so it wasn't affected; the update clears the alert.

## [0.2.0] — 2026-08-11

The first public release of Lark, a realtime database server that is also wire-compatible with the Firebase Realtime Database, comprising the `lark-server` database engine (thread-per-core Rust on Glommio/io_uring, per-database write-ahead log, the `lark-blob` on-disk format with lazy loading and incremental compaction, and tunable durability up to fsync-before-ACK), the `lark-edge` gateway in Go (TLS termination, WebSocket/WebTransport/REST transports, JWT auth across four token formats, an embedded admin dashboard), the Firebase security-rules language and query surface, per-database resource limits, and deployment, backup, and observability guides with a one-command Fly.io quickstart.

For what Lark is and how it's tested, start at the [README](README.md) and [TESTING.md](TESTING.md). Subsequent releases will list their changes here in the usual Added/Changed/Fixed form.
