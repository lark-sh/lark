# Changelog

Notable changes to Lark are documented here. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows [Semantic Versioning](https://semver.org/).

> **Pre-1.0:** Lark is versioned `0.x`. Breaking changes in `0.x` releases are rare and always called out here; if a release changes the on-disk format, it ships with a migration path. Even so, **back up before upgrading** (see [BACKUP.md](docs/BACKUP.md)) — downgrades aren't supported, so the way back from a bad upgrade is a restore. For how we think about `1.0.0`, see [Project status](README.md#project-status) in the README.

## [Unreleased]

### Fixed

- Fixed a bug where bursts of data were causing issues when echoing back to the client.
- Firebase-protocol responses over 16 KB were split into frames at byte offsets, so a multi-byte character (an em dash, a bullet, any non-ASCII text) landing on a 16,384-byte boundary produced a frame that was not valid UTF-8 on its own. Browsers fail the whole WebSocket on such a frame (close code 1007), and because the client then reconnected and re-requested the same data, any database containing such a character at that position could never finish loading. Frames are now split only at character boundaries. The edge also logs a WARN when a client closes with a protocol-fault code, since that always indicates a server-side bug.
- The server rejected inbound Firebase frames over 16,384 bytes, but the Firebase JS SDK splits at 16,384 UTF-16 code units, so a large write containing non-ASCII text could be refused as malformed. The inbound cap is now three bytes per unit.
- The edge dropped clients whose per-connection outbox exceeded 1000 messages. Firebase-protocol responses are split into 16 KB frames, so a client loading a large database on an ordinary connection could exhaust the count mid-sync, be disconnected, reconnect, re-issue every query, and loop forever without ever finishing. The outbox is now bounded by bytes (`CLIENT_OUTBOX_MAX_BYTES`, default 256 MB) rather than by message count.
- The edge's flat 10-second write deadline could drop a native-protocol client on a slow link while it was still receiving a single large frame. The deadline now scales with payload size (`CLIENT_WRITE_DEADLINE` + payload / `CLIENT_WRITE_MIN_BYTES_PER_SEC`), so it only fires when a client has stopped taking bytes.

### Added

- Server tunables for the remaining per-client and per-request caps that used to be compile-time constants: `LARK_MAX_TRANSACTION_OPS`, `LARK_MAX_ON_DISCONNECT_ACTIONS_PER_CLIENT`, `LARK_MAX_ON_DISCONNECT_BYTES_PER_CLIENT`, and `LARK_MAX_RESPONSE_SIZE` (each also a `--flag`). Defaults are unchanged.

### Changed

- Every place the edge or server drops a client or a message because a defensive limit was hit now logs at WARN with the reason and context: outbox full, write deadline exceeded, backend disconnected, SSE buffer overflow, subscription cap, onDisconnect cap, transaction op cap, response-size rejections, and reliable messages dropped on a full send buffer (throttled to the first and every 1000th). These used to be at debug or trace, or not logged at all.
- The edge logs `Client outbox approaching limit` at WARN when one client's queue crosses `CLIENT_OUTBOX_WARN_BYTES` (default 64 MB), and `Client outbox usage` at INFO once a minute while anything is queued, so the headroom on the cap is visible before anyone hits it.

## [0.2.0] — 2026-08-11

The first public release of Lark, a realtime database server that is also wire-compatible with the Firebase Realtime Database, comprising the `lark-server` database engine (thread-per-core Rust on Glommio/io_uring, per-database write-ahead log, the `lark-blob` on-disk format with lazy loading and incremental compaction, and tunable durability up to fsync-before-ACK), the `lark-edge` gateway in Go (TLS termination, WebSocket/WebTransport/REST transports, JWT auth across four token formats, an embedded admin dashboard), the Firebase security-rules language and query surface, per-database resource limits, and deployment, backup, and observability guides with a one-command Fly.io quickstart.

For what Lark is and how it's tested, start at the [README](README.md) and [TESTING.md](TESTING.md). Subsequent releases will list their changes here in the usual Added/Changed/Fixed form.
