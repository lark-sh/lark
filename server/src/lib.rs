//! Lark: A Realtime Database written in Rust.
//!
//! Lark provides real-time data synchronization with Firebase SDK compatibility,
//! designed for predictable low-latency performance in multiplayer applications.
//!
//! ## Architecture
//!
//! Lark uses a **thread-per-core** model where each CPU core runs an independent
//! event loop with no shared mutable state between cores. This eliminates
//! cross-core synchronization overhead entirely.
//!
//! Key modules:
//! - [`executor`]: Glommio runtime infrastructure (per-core LocalExecutor)
//! - [`server`]: Per-core request handling (CoreHandler)
//! - [`db`]: Core database logic (Tree, ArcValue, subscriptions, queries)
//! - [`transport`]: Network layer (proxy protocol, Firebase adapter)
//! - [`rules`]: Security rules engine (expression evaluator)
//! - [`storage`]: Persistence layer (WAL, blob storage, in-process compaction)
//! - [`protocol`]: Client/server message types
//!
//! Note: token validation lives entirely in the Go edge. The Rust server trusts
//! the edge's resolved auth (delivered out-of-band via AUTH_CHANGED) and never
//! validates JWTs itself.
//!
//! See the `docs/` directory at the repo root for deeper architecture writeups.

pub mod db;
pub mod executor;
pub mod metrics;
pub mod protocol;
pub mod rules;
pub mod server;
pub mod storage;
pub mod transport;
pub mod util;
