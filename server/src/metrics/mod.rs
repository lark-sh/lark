//! Metrics and observability for Lark.
//!
//! This module provides:
//! - **Database metrics**: Per-database counters (writes, reads, CCU, etc.)
//! - **Core metrics**: Per-core aggregated metrics
//! - **Latency tracking**: Debug-mode latency profiling

pub mod database_metrics;
pub mod latency;

pub use database_metrics::{DatabaseMetrics, DatabaseMetricsSnapshot};
pub use latency::{
    DEBUG_TIMING, MessageTimestamps, STATS_INTERVAL, is_debug_timing_enabled, log_latency_stats,
    log_wal_stats, maybe_create_timestamps, record_latency, record_wal_flush, set_debug_timing,
};
