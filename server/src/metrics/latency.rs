//! Latency tracking for message pipeline debugging.
//!
//! When enabled via `set_debug_timing(true)`, this module tracks timestamps
//! at key points in the message pipeline and reports percentile statistics.
//!
//! To minimize overhead:
//! - All tracking is disabled when `DEBUG_TIMING` is false (zero cost)
//! - Only 1 in N messages are sampled to reduce mutex contention
//! - Uses `Instant` for monotonic, low-overhead timestamps

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// Global toggle - when false, ALL latency overhead is eliminated.
/// Check this with `Ordering::Relaxed` for minimal overhead.
pub static DEBUG_TIMING: AtomicBool = AtomicBool::new(false);

/// Sample counter for 1-in-N sampling
static SAMPLE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Sample rate: track 1 in N messages.
/// Higher = less overhead but less granular stats.
const SAMPLE_RATE: u64 = 100;

/// Stats reporting interval
pub const STATS_INTERVAL: Duration = Duration::from_secs(5);

// ============================================================================
// MessageTimestamps
// ============================================================================

/// Timestamps carried through the message pipeline.
///
/// Each field represents a checkpoint in the message's journey:
/// 1. `tcp_read` - When bytes were read from TCP socket
/// 2. `handler_receive` - When ProxyHandler.on_message was called
/// 3. `db_inbox_push` - Just before pushing to database inbox channel
/// 4. `db_inbox_pop` - When message was received from inbox channel
/// 5. `work_complete` - After handle_message finished processing
#[derive(Debug, Clone, Default)]
pub struct MessageTimestamps {
    pub tcp_read: Option<Instant>,
    pub handler_receive: Option<Instant>,
    pub db_inbox_push: Option<Instant>,
    pub db_inbox_pop: Option<Instant>,
    pub work_complete: Option<Instant>,
}

impl MessageTimestamps {
    /// Create new timestamps, stamping tcp_read immediately.
    #[inline]
    pub fn new() -> Self {
        Self {
            tcp_read: Some(Instant::now()),
            ..Default::default()
        }
    }

    /// Stamp when handler receives the message.
    #[inline]
    pub fn stamp_handler_receive(&mut self) {
        self.handler_receive = Some(Instant::now());
    }

    /// Stamp just before pushing to database inbox.
    #[inline]
    pub fn stamp_db_inbox_push(&mut self) {
        self.db_inbox_push = Some(Instant::now());
    }

    /// Stamp when message is popped from database inbox.
    #[inline]
    pub fn stamp_db_inbox_pop(&mut self) {
        self.db_inbox_pop = Some(Instant::now());
    }

    /// Stamp when message handling is complete.
    #[inline]
    pub fn stamp_work_complete(&mut self) {
        self.work_complete = Some(Instant::now());
    }
}

// ============================================================================
// Sampling
// ============================================================================

/// Check if we should sample this message (1 in SAMPLE_RATE).
/// Returns false immediately if DEBUG_TIMING is disabled.
#[inline]
pub fn should_sample() -> bool {
    if !DEBUG_TIMING.load(Ordering::Relaxed) {
        return false;
    }
    // Atomic increment, sample when counter % SAMPLE_RATE == 0
    let count = SAMPLE_COUNTER.fetch_add(1, Ordering::Relaxed);
    count.is_multiple_of(SAMPLE_RATE)
}

/// Create timestamps if sampling this message, None otherwise.
/// This is the main entry point - call at the start of message processing.
#[inline]
pub fn maybe_create_timestamps() -> Option<MessageTimestamps> {
    if should_sample() {
        Some(MessageTimestamps::new())
    } else {
        None
    }
}

// ============================================================================
// Stats Collection
// ============================================================================

/// Per-step latency storage
struct StepLatencies {
    tcp_to_handler: Vec<Duration>,
    handler_to_db_push: Vec<Duration>,
    db_push_to_pop: Vec<Duration>,
    db_pop_to_complete: Vec<Duration>,
    total: Vec<Duration>,
}

impl StepLatencies {
    fn new() -> Self {
        Self {
            tcp_to_handler: Vec::with_capacity(10000),
            handler_to_db_push: Vec::with_capacity(10000),
            db_push_to_pop: Vec::with_capacity(10000),
            db_pop_to_complete: Vec::with_capacity(10000),
            total: Vec::with_capacity(10000),
        }
    }

    fn clear(&mut self) {
        self.tcp_to_handler.clear();
        self.handler_to_db_push.clear();
        self.db_push_to_pop.clear();
        self.db_pop_to_complete.clear();
        self.total.clear();
    }
}

/// Global latency storage (lazily initialized)
static LATENCIES: Mutex<Option<StepLatencies>> = Mutex::new(None);

fn get_latencies() -> std::sync::MutexGuard<'static, Option<StepLatencies>> {
    let mut guard = LATENCIES.lock().unwrap();
    if guard.is_none() {
        *guard = Some(StepLatencies::new());
    }
    guard
}

/// Record latency from a completed message.
/// Call this after stamping `work_complete`.
pub fn record_latency(ts: &MessageTimestamps) {
    if !DEBUG_TIMING.load(Ordering::Relaxed) {
        return;
    }

    let tcp_read = match ts.tcp_read {
        Some(t) => t,
        None => return,
    };

    let mut guard = get_latencies();
    let latencies = guard.as_mut().unwrap();

    // Record each step (only if both timestamps present)
    if let Some(handler) = ts.handler_receive
        && let Some(duration) = handler.checked_duration_since(tcp_read)
    {
        latencies.tcp_to_handler.push(duration);
    }

    if let (Some(handler), Some(push)) = (ts.handler_receive, ts.db_inbox_push)
        && let Some(duration) = push.checked_duration_since(handler)
    {
        latencies.handler_to_db_push.push(duration);
    }

    if let (Some(push), Some(pop)) = (ts.db_inbox_push, ts.db_inbox_pop)
        && let Some(duration) = pop.checked_duration_since(push)
    {
        latencies.db_push_to_pop.push(duration);
    }

    if let (Some(pop), Some(complete)) = (ts.db_inbox_pop, ts.work_complete)
        && let Some(duration) = complete.checked_duration_since(pop)
    {
        latencies.db_pop_to_complete.push(duration);
    }

    if let Some(complete) = ts.work_complete
        && let Some(duration) = complete.checked_duration_since(tcp_read)
    {
        latencies.total.push(duration);
    }
}

// ============================================================================
// Percentile Calculation
// ============================================================================

/// Calculate p50, p90, p99 from a slice of durations.
/// Sorts the slice in place.
fn percentiles(data: &mut [Duration]) -> (Duration, Duration, Duration) {
    if data.is_empty() {
        return (Duration::ZERO, Duration::ZERO, Duration::ZERO);
    }

    data.sort_unstable();
    let len = data.len();

    // Use (len - 1) * p / 100 for percentile index (standard method)
    let p50_idx = (len - 1) * 50 / 100;
    let p90_idx = (len - 1) * 90 / 100;
    let p99_idx = (len - 1) * 99 / 100;

    (data[p50_idx], data[p90_idx], data[p99_idx])
}

/// Format a duration in a human-readable way (µs or ms)
fn format_duration(d: Duration) -> String {
    let micros = d.as_micros();
    if micros < 1000 {
        format!("{}µs", micros)
    } else {
        format!("{:.2}ms", micros as f64 / 1000.0)
    }
}

// ============================================================================
// Reporting
// ============================================================================

/// Log latency stats and clear buffers.
/// Returns true if stats were logged, false if no data or timing disabled.
pub fn log_latency_stats() -> bool {
    if !DEBUG_TIMING.load(Ordering::Relaxed) {
        return false;
    }

    let mut guard = get_latencies();
    let latencies = match guard.as_mut() {
        Some(l) => l,
        None => return false,
    };

    if latencies.total.is_empty() {
        return false;
    }

    let count = latencies.total.len();

    // Calculate and log total
    let (p50, p90, p99) = percentiles(&mut latencies.total);
    info!(
        "[Latency] msgs={} total: p50={} p90={} p99={}",
        count,
        format_duration(p50),
        format_duration(p90),
        format_duration(p99)
    );

    // Per-step breakdown
    if !latencies.tcp_to_handler.is_empty() {
        let (p50, p90, p99) = percentiles(&mut latencies.tcp_to_handler);
        info!(
            "[Latency]   1.tcp→handler:    p50={} p90={} p99={}",
            format_duration(p50),
            format_duration(p90),
            format_duration(p99)
        );
    }

    if !latencies.handler_to_db_push.is_empty() {
        let (p50, p90, p99) = percentiles(&mut latencies.handler_to_db_push);
        info!(
            "[Latency]   2.handler→dbPush: p50={} p90={} p99={}",
            format_duration(p50),
            format_duration(p90),
            format_duration(p99)
        );
    }

    if !latencies.db_push_to_pop.is_empty() {
        let (p50, p90, p99) = percentiles(&mut latencies.db_push_to_pop);
        info!(
            "[Latency]   3.dbPush→dbPop:   p50={} p90={} p99={}",
            format_duration(p50),
            format_duration(p90),
            format_duration(p99)
        );
    }

    if !latencies.db_pop_to_complete.is_empty() {
        let (p50, p90, p99) = percentiles(&mut latencies.db_pop_to_complete);
        info!(
            "[Latency]   4.dbPop→complete: p50={} p90={} p99={}",
            format_duration(p50),
            format_duration(p90),
            format_duration(p99)
        );
    }

    // Clear for next interval
    latencies.clear();

    true
}

/// Enable or disable debug timing.
pub fn set_debug_timing(enabled: bool) {
    DEBUG_TIMING.store(enabled, Ordering::SeqCst);
    if enabled {
        warn!(
            "DEBUG TIMING ENABLED - latency tracking active (1:{} sample rate)",
            SAMPLE_RATE
        );
    } else {
        info!("Debug timing disabled");
    }
}

/// Check if debug timing is enabled.
#[inline]
pub fn is_debug_timing_enabled() -> bool {
    DEBUG_TIMING.load(Ordering::Relaxed)
}

// ============================================================================
// WAL Flush Tracking
// ============================================================================

/// WAL flush statistics storage
struct WalStats {
    flush_durations: Vec<Duration>,
    entry_counts: Vec<usize>,
    bytes_written: Vec<u64>,
}

impl WalStats {
    fn new() -> Self {
        Self {
            flush_durations: Vec::with_capacity(10000),
            entry_counts: Vec::with_capacity(10000),
            bytes_written: Vec::with_capacity(10000),
        }
    }

    fn clear(&mut self) {
        self.flush_durations.clear();
        self.entry_counts.clear();
        self.bytes_written.clear();
    }
}

/// Global WAL stats storage
static WAL_STATS: Mutex<Option<WalStats>> = Mutex::new(None);

fn get_wal_stats() -> std::sync::MutexGuard<'static, Option<WalStats>> {
    let mut guard = WAL_STATS.lock().unwrap();
    if guard.is_none() {
        *guard = Some(WalStats::new());
    }
    guard
}

/// Record a WAL flush operation.
/// Unlike message latency, WAL flushes are recorded for every flush (not sampled).
pub fn record_wal_flush(duration: Duration, entries: usize, bytes: u64) {
    if !DEBUG_TIMING.load(Ordering::Relaxed) {
        return;
    }

    let mut guard = get_wal_stats();
    let stats = guard.as_mut().unwrap();

    stats.flush_durations.push(duration);
    stats.entry_counts.push(entries);
    stats.bytes_written.push(bytes);
}

/// Log WAL flush stats and clear buffers.
/// Returns true if stats were logged, false if no data or timing disabled.
pub fn log_wal_stats() -> bool {
    if !DEBUG_TIMING.load(Ordering::Relaxed) {
        return false;
    }

    let mut guard = get_wal_stats();
    let stats = match guard.as_mut() {
        Some(s) => s,
        None => return false,
    };

    if stats.flush_durations.is_empty() {
        return false;
    }

    let flush_count = stats.flush_durations.len();

    // Calculate duration percentiles
    let (p50, p90, p99) = percentiles(&mut stats.flush_durations);

    // Calculate averages for entries and bytes
    let total_entries: usize = stats.entry_counts.iter().sum();
    let total_bytes: u64 = stats.bytes_written.iter().sum();
    let avg_entries = total_entries as f64 / flush_count as f64;
    let avg_bytes = total_bytes as f64 / flush_count as f64;

    info!(
        "[WAL] flushes={} duration: p50={} p90={} p99={} | avg entries={:.1} avg bytes={:.0}",
        flush_count,
        format_duration(p50),
        format_duration(p90),
        format_duration(p99),
        avg_entries,
        avg_bytes
    );

    // Clear for next interval
    stats.clear();

    true
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamps() {
        let mut ts = MessageTimestamps::new();
        assert!(ts.tcp_read.is_some());
        assert!(ts.handler_receive.is_none());

        ts.stamp_handler_receive();
        assert!(ts.handler_receive.is_some());

        ts.stamp_db_inbox_push();
        assert!(ts.db_inbox_push.is_some());

        ts.stamp_db_inbox_pop();
        assert!(ts.db_inbox_pop.is_some());

        ts.stamp_work_complete();
        assert!(ts.work_complete.is_some());
    }

    // Note: These tests use global state (DEBUG_TIMING, SAMPLE_COUNTER).
    // Run with `cargo test -- --test-threads=1` if tests are flaky.

    #[test]
    fn test_sampling_disabled() {
        // Temporarily disable timing
        DEBUG_TIMING.store(false, Ordering::SeqCst);

        // With timing disabled, should_sample should return false (many times to be sure)
        for _ in 0..200 {
            assert!(!should_sample());
        }
        assert!(maybe_create_timestamps().is_none());
    }

    #[test]
    fn test_sampling_enabled() {
        // Enable timing
        DEBUG_TIMING.store(true, Ordering::SeqCst);

        // Call should_sample many times and count how many return true
        // With 1:100 sampling, roughly 1% should return true
        let mut sampled = 0;
        for _ in 0..1000 {
            if should_sample() {
                sampled += 1;
            }
        }

        // Should have sampled ~10 times (1000/100 = 10), allow wide tolerance
        // since other tests running in parallel might have incremented the global counter
        // The important thing is that sampling is happening at roughly the right rate
        assert!(sampled >= 1, "Expected at least 1 sample, got {}", sampled);
        assert!(
            sampled <= 30,
            "Expected at most 30 samples, got {}",
            sampled
        );

        // Disable for other tests
        DEBUG_TIMING.store(false, Ordering::SeqCst);
    }

    #[test]
    fn test_percentiles() {
        let mut data: Vec<Duration> = (1..=100).map(|i| Duration::from_micros(i * 10)).collect();

        let (p50, p90, p99) = percentiles(&mut data);

        assert_eq!(p50, Duration::from_micros(500)); // 50th element
        assert_eq!(p90, Duration::from_micros(900)); // 90th element
        assert_eq!(p99, Duration::from_micros(990)); // 99th element
    }

    #[test]
    fn test_percentiles_empty() {
        let mut data: Vec<Duration> = vec![];
        let (p50, p90, p99) = percentiles(&mut data);
        assert_eq!(p50, Duration::ZERO);
        assert_eq!(p90, Duration::ZERO);
        assert_eq!(p99, Duration::ZERO);
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(Duration::from_micros(500)), "500µs");
        assert_eq!(format_duration(Duration::from_micros(1500)), "1.50ms");
        assert_eq!(format_duration(Duration::from_millis(10)), "10.00ms");
    }
}
