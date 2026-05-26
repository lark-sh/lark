//! Per-database metrics collection with atomic counters.
//!
//! This module provides lightweight, lock-free metrics tracking for each database.
//! All counters use atomic operations to minimize overhead on the hot path.
//!
//! ## Design
//!
//! - **Counters reset on emit**: writes, reads, bytes, etc. are swapped to 0 when emitted
//! - **Gauges don't reset**: CCU, subscriptions, data_size reflect current state
//! - **Skip inactive databases**: If no activity, emit nothing (saves Postgres writes)
//!
//! ## Usage
//!
//! ```ignore
//! let metrics = DatabaseMetrics::new();
//!
//! // On each operation (fast - just atomic increment)
//! metrics.record_write(1024);  // 1KB write
//! metrics.record_latency(150); // 150µs latency
//! metrics.record_read();            // count a read operation
//! metrics.record_outbound_bytes(4096); // 4KB sent
//!
//! // Periodically (every 60s), emit and reset
//! if let Some(snapshot) = metrics.emit_and_reset() {
//!     // Send snapshot to Vector/Postgres
//! }
//! ```

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// Lightweight metrics counters for a single database.
/// All operations are lock-free atomic increments.
pub struct DatabaseMetrics {
    // Operation counts (reset on emit)
    writes: AtomicU64,
    reads: AtomicU64,
    transactions: AtomicU64,

    // Byte counts (reset on emit)
    write_bytes: AtomicU64,
    read_bytes: AtomicU64,

    // Event counts (reset on emit)
    events_sent: AtomicU64,

    // Error counts (reset on emit)
    permission_denials: AtomicU32,
    size_rejections: AtomicU32,

    // Latency tracking (reset on emit)
    // Tracks end-to-end latency from TCP receive to processing complete
    latency_sum_us: AtomicU64,
    latency_count: AtomicU32,
    latency_max_us: AtomicU32,

    // Point-in-time values (not reset)
    current_ccu: AtomicU32,
    current_subscriptions: AtomicU32,
    data_size_bytes: AtomicU64,
}

impl Default for DatabaseMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl DatabaseMetrics {
    /// Create a new metrics instance with all counters at zero.
    pub fn new() -> Self {
        Self {
            writes: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            transactions: AtomicU64::new(0),
            write_bytes: AtomicU64::new(0),
            read_bytes: AtomicU64::new(0),
            events_sent: AtomicU64::new(0),
            permission_denials: AtomicU32::new(0),
            size_rejections: AtomicU32::new(0),
            latency_sum_us: AtomicU64::new(0),
            latency_count: AtomicU32::new(0),
            latency_max_us: AtomicU32::new(0),
            current_ccu: AtomicU32::new(0),
            current_subscriptions: AtomicU32::new(0),
            data_size_bytes: AtomicU64::new(0),
        }
    }

    // =========================================================================
    // Recording methods (called on every operation - must be fast)
    // =========================================================================

    /// Record a write operation (SET, UPDATE, REMOVE).
    #[inline]
    pub fn record_write(&self, bytes: usize) {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.write_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// Record operation latency (TCP receive to processing complete).
    #[inline]
    pub fn record_latency(&self, latency_us: u32) {
        self.latency_sum_us
            .fetch_add(latency_us as u64, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);
        self.latency_max_us.fetch_max(latency_us, Ordering::Relaxed);
    }

    /// Record a read operation (ONCE queries). Bytes tracked separately via record_outbound_bytes.
    #[inline]
    pub fn record_read(&self) {
        self.reads.fetch_add(1, Ordering::Relaxed);
    }

    /// Record outbound bytes (all data sent to clients: events, reads, subscriptions).
    #[inline]
    pub fn record_outbound_bytes(&self, bytes: usize) {
        self.read_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// Record a successful transaction.
    #[inline]
    pub fn record_transaction(&self) {
        self.transactions.fetch_add(1, Ordering::Relaxed);
    }

    /// Record events sent to subscribers.
    #[inline]
    pub fn record_events_sent(&self, count: u64) {
        self.events_sent.fetch_add(count, Ordering::Relaxed);
    }

    /// Record a permission denial (rules rejected the operation).
    #[inline]
    pub fn record_permission_denial(&self) {
        self.permission_denials.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a size rejection (payload too large, response too large, etc.).
    #[inline]
    pub fn record_size_rejection(&self) {
        self.size_rejections.fetch_add(1, Ordering::Relaxed);
    }

    // =========================================================================
    // Gauge updates (called when state changes)
    // =========================================================================

    /// Update current CCU (called on connect/disconnect).
    #[inline]
    pub fn set_ccu(&self, ccu: u32) {
        self.current_ccu.store(ccu, Ordering::Relaxed);
    }

    /// Increment CCU (client connected).
    #[inline]
    pub fn increment_ccu(&self) {
        self.current_ccu.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement CCU (client disconnected).
    #[inline]
    pub fn decrement_ccu(&self) {
        // Use saturating subtraction to avoid underflow
        self.current_ccu
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            })
            .ok();
    }

    /// Update current subscription count.
    #[inline]
    pub fn set_subscriptions(&self, count: u32) {
        self.current_subscriptions.store(count, Ordering::Relaxed);
    }

    /// Update data size in bytes.
    #[inline]
    pub fn set_data_size(&self, bytes: u64) {
        self.data_size_bytes.store(bytes, Ordering::Relaxed);
    }

    // =========================================================================
    // Emission (called periodically - every 60s for active databases)
    // =========================================================================

    /// Emit metrics and reset counters. Returns None if no activity.
    ///
    /// This atomically swaps all counter values to zero and returns a snapshot
    /// of the previous values. Gauges (CCU, subscriptions, data_size) are read
    /// but not reset.
    pub fn emit_and_reset(&self) -> Option<DatabaseMetricsSnapshot> {
        // Read counters atomically (swap with 0)
        let writes = self.writes.swap(0, Ordering::Relaxed);
        let reads = self.reads.swap(0, Ordering::Relaxed);
        let events = self.events_sent.swap(0, Ordering::Relaxed);

        // Skip if no activity
        if writes == 0 && reads == 0 && events == 0 {
            return None;
        }

        let transactions = self.transactions.swap(0, Ordering::Relaxed);
        let write_bytes = self.write_bytes.swap(0, Ordering::Relaxed);
        let read_bytes = self.read_bytes.swap(0, Ordering::Relaxed);

        let permission_denials = self.permission_denials.swap(0, Ordering::Relaxed);
        let size_rejections = self.size_rejections.swap(0, Ordering::Relaxed);

        let latency_count = self.latency_count.swap(0, Ordering::Relaxed);
        let latency_sum = self.latency_sum_us.swap(0, Ordering::Relaxed);
        let latency_max = self.latency_max_us.swap(0, Ordering::Relaxed);

        // Read gauges (don't reset)
        let current_ccu = self.current_ccu.load(Ordering::Relaxed);
        let current_subscriptions = self.current_subscriptions.load(Ordering::Relaxed);
        let data_size_bytes = self.data_size_bytes.load(Ordering::Relaxed);

        Some(DatabaseMetricsSnapshot {
            writes,
            reads,
            transactions,
            write_bytes,
            read_bytes,
            events_sent: events,
            permission_denials,
            size_rejections,
            latency_avg_us: if latency_count > 0 {
                (latency_sum / latency_count as u64) as u32
            } else {
                0
            },
            latency_max_us: latency_max,
            current_ccu,
            current_subscriptions,
            data_size_bytes,
        })
    }

    /// Get current CCU without resetting.
    #[inline]
    pub fn get_ccu(&self) -> u32 {
        self.current_ccu.load(Ordering::Relaxed)
    }

    /// Get current subscription count without resetting.
    #[inline]
    pub fn get_subscriptions(&self) -> u32 {
        self.current_subscriptions.load(Ordering::Relaxed)
    }
}

/// Snapshot of database metrics at a point in time.
/// This is what gets serialized to JSON and sent to Postgres/Vector.
#[derive(Debug, Clone)]
pub struct DatabaseMetricsSnapshot {
    // Counters (accumulated since last emit)
    pub writes: u64,
    pub reads: u64,
    pub transactions: u64,
    pub write_bytes: u64,
    pub read_bytes: u64,
    pub events_sent: u64,

    // Error counters
    pub permission_denials: u32,
    pub size_rejections: u32,

    // Latency (from this interval) - TCP receive to processing complete
    pub latency_avg_us: u32,
    pub latency_max_us: u32,

    // Gauges (current values)
    pub current_ccu: u32,
    pub current_subscriptions: u32,
    pub data_size_bytes: u64,
}

impl DatabaseMetricsSnapshot {
    /// Serialize to JSON for output.
    pub fn to_json(&self, project: &str, database: &str, server: &str, core_id: usize) -> String {
        use std::time::SystemTime;

        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        format!(
            r#"{{"type":"db_metrics","ts":{},"server":"{}","core":{},"project":"{}","database":"{}","writes":{},"reads":{},"transactions":{},"write_bytes":{},"read_bytes":{},"events_sent":{},"ccu":{},"subscriptions":{},"data_size_bytes":{},"latency_avg_us":{},"latency_max_us":{},"permission_denials":{},"size_rejections":{}}}"#,
            ts,
            server,
            core_id,
            project,
            database,
            self.writes,
            self.reads,
            self.transactions,
            self.write_bytes,
            self.read_bytes,
            self.events_sent,
            self.current_ccu,
            self.current_subscriptions,
            self.data_size_bytes,
            self.latency_avg_us,
            self.latency_max_us,
            self.permission_denials,
            self.size_rejections,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_write() {
        let metrics = DatabaseMetrics::new();

        metrics.record_write(1024);
        metrics.record_write(2048);

        let snapshot = metrics.emit_and_reset().unwrap();

        assert_eq!(snapshot.writes, 2);
        assert_eq!(snapshot.write_bytes, 3072);
    }

    #[test]
    fn test_record_latency() {
        let metrics = DatabaseMetrics::new();

        metrics.record_latency(150);
        metrics.record_latency(200);
        metrics.record_write(100); // Need activity to emit

        let snapshot = metrics.emit_and_reset().unwrap();

        assert_eq!(snapshot.latency_avg_us, 175); // (150 + 200) / 2
        assert_eq!(snapshot.latency_max_us, 200);
    }

    #[test]
    fn test_record_read() {
        let metrics = DatabaseMetrics::new();

        metrics.record_read();
        metrics.record_read();
        metrics.record_outbound_bytes(4096);
        metrics.record_outbound_bytes(8192);

        let snapshot = metrics.emit_and_reset().unwrap();

        assert_eq!(snapshot.reads, 2);
        assert_eq!(snapshot.read_bytes, 12288);
    }

    #[test]
    fn test_skip_inactive() {
        let metrics = DatabaseMetrics::new();

        // No activity - should return None
        let snapshot = metrics.emit_and_reset();
        assert!(snapshot.is_none());
    }

    #[test]
    fn test_ccu_tracking() {
        let metrics = DatabaseMetrics::new();

        metrics.increment_ccu();
        metrics.increment_ccu();
        metrics.increment_ccu();
        assert_eq!(metrics.get_ccu(), 3);

        metrics.decrement_ccu();
        assert_eq!(metrics.get_ccu(), 2);

        // Force some activity so we can emit
        metrics.record_write(100);
        let snapshot = metrics.emit_and_reset().unwrap();
        assert_eq!(snapshot.current_ccu, 2);
    }

    #[test]
    fn test_ccu_no_underflow() {
        let metrics = DatabaseMetrics::new();

        // Should not underflow
        metrics.decrement_ccu();
        metrics.decrement_ccu();
        assert_eq!(metrics.get_ccu(), 0);
    }

    #[test]
    fn test_reset_on_emit() {
        let metrics = DatabaseMetrics::new();

        metrics.record_write(1024);
        metrics.record_read();
        metrics.record_outbound_bytes(2048);

        let snapshot1 = metrics.emit_and_reset().unwrap();
        assert_eq!(snapshot1.writes, 1);
        assert_eq!(snapshot1.reads, 1);
        assert_eq!(snapshot1.read_bytes, 2048);

        // Second emit should be None (no new activity)
        let snapshot2 = metrics.emit_and_reset();
        assert!(snapshot2.is_none());
    }

    #[test]
    fn test_gauges_not_reset() {
        let metrics = DatabaseMetrics::new();

        metrics.set_ccu(10);
        metrics.set_subscriptions(5);
        metrics.set_data_size(1_000_000);

        // Force activity for emit
        metrics.record_write(100);

        let snapshot1 = metrics.emit_and_reset().unwrap();
        assert_eq!(snapshot1.current_ccu, 10);
        assert_eq!(snapshot1.current_subscriptions, 5);
        assert_eq!(snapshot1.data_size_bytes, 1_000_000);

        // Gauges should still be set after emit
        metrics.record_write(100);
        let snapshot2 = metrics.emit_and_reset().unwrap();
        assert_eq!(snapshot2.current_ccu, 10);
        assert_eq!(snapshot2.current_subscriptions, 5);
    }

    #[test]
    fn test_to_json() {
        let snapshot = DatabaseMetricsSnapshot {
            writes: 100,
            reads: 200,
            transactions: 10,
            write_bytes: 50000,
            read_bytes: 100000,
            events_sent: 500,
            permission_denials: 2,
            size_rejections: 1,
            latency_avg_us: 150,
            latency_max_us: 500,
            current_ccu: 25,
            current_subscriptions: 50,
            data_size_bytes: 10_000_000,
        };

        let json = snapshot.to_json("acme-corp", "production", "lark-prod-1", 3);
        assert!(json.contains(r#""type":"db_metrics""#));
        assert!(json.contains(r#""core":3"#));
        assert!(json.contains(r#""project":"acme-corp""#));
        assert!(json.contains(r#""database":"production""#));
        assert!(json.contains(r#""writes":100"#));
        assert!(json.contains(r#""ccu":25"#));
        assert!(json.contains(r#""latency_avg_us":150"#));
        assert!(json.contains(r#""latency_max_us":500"#));
    }
}
