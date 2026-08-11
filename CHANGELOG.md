# Changelog

Notable changes to Lark are documented here. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows [Semantic Versioning](https://semver.org/).

> **Pre-1.0:** Lark is versioned `0.x`. Breaking changes in `0.x` releases are rare and always called out here; if a release changes the on-disk format, it ships with a migration path. Even so, **back up before upgrading** (see [BACKUP.md](docs/BACKUP.md)) — downgrades aren't supported, so the way back from a bad upgrade is a restore. For how we think about `1.0.0`, see [Project status](README.md#project-status) in the README.

## [0.2.0] — 2026-08-11

The first public release of Lark, a realtime database server that is also wire-compatible with the Firebase Realtime Database, comprising the `lark-server` database engine (thread-per-core Rust on Glommio/io_uring, per-database write-ahead log, the `lark-blob` on-disk format with lazy loading and incremental compaction, and tunable durability up to fsync-before-ACK), the `lark-edge` gateway in Go (TLS termination, WebSocket/WebTransport/REST transports, JWT auth across four token formats, an embedded admin dashboard), the Firebase security-rules language and query surface, per-database resource limits, and deployment, backup, and observability guides with a one-command Fly.io quickstart.

For what Lark is and how it's tested, start at the [README](README.md) and [TESTING.md](TESTING.md). Subsequent releases will list their changes here in the usual Added/Changed/Fixed form.
