# Changelog

All notable changes to Lark are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/).

> **Pre-1.0:** Lark is under active development and currently versioned `0.x`.
> Per semver, minor versions (`0.x`) may include breaking changes while we
> stabilize the APIs, wire protocol, and on-disk format. If a release changes the
> on-disk format, we'll ship a migration path and call it out here — but **back
> up before upgrading** (see [BACKUP.md](docs/BACKUP.md)), and note that
> downgrades aren't supported (restore from a backup instead). We'll cut `1.0.0`
> at the public release, after which the usual semver guarantees apply.

## [0.1.0] — Unreleased

First public release. Lark is a realtime database that's wire-compatible with the
Firebase Realtime Database.

### Added

- **Database engine** (`lark-server`): thread-per-core on Glommio / io_uring, with
  a per-database write-ahead log plus the `lark-blob` on-disk format, lazy
  on-demand tree loading, and incremental compaction.
- **Gateway** (`lark-edge`): TLS termination, WebSocket and WebTransport
  transports, JWT auth (Firebase ID / custom / legacy tokens and Lark customer
  tokens), consistent-hash routing to database servers, and an embedded admin
  dashboard.
- **Firebase-compatible API**: REST + realtime reads/writes, the security-rules
  language, and queries (`orderBy`, `limitToFirst`/`limitToLast`, `startAt`,
  `endAt`, `equalTo`).
- **`LARK_PROXY_BIND`** option to set the proxy listener's bind host (default
  `0.0.0.0`; use `[::]` for IPv6 networks such as Fly.io's private network).
- **Deployment**: single-host docker-compose stack, a production deployment guide
  (Tiers 1–3), a backup & restore guide, and a one-command Fly.io quick start.

### Fixed

- Removed an unsound `Rc::from_raw` in the proxy handler.
