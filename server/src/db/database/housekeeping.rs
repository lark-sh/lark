use super::*;

impl Database {
    // =========================================================================
    // Housekeeping
    // =========================================================================

    pub(super) async fn housekeeping(&mut self) {
        // Keepalive is client-initiated (client sends "pi", server swallows it)
        // View manager handles its own cleanup via unsubscribe

        // Note: processed_writes and nacked_writes are bounded per-connection
        // (MAX_WRITES_PER_CONNECTION entries each). They evict oldest on insert.
        // Entries are kept after disconnect so reconnecting clients still get
        // deduplication protection.

        // Attempt WAL recovery if in failed state
        // This runs every ~5s (housekeeping interval) and is cheap (one small write + sync)
        self.try_recover_wal().await;

        // Evict idle promoted paths back to Sentinel to reclaim memory.
        // Only applies to blob-backed databases with promoted data.
        if self.is_blob_backed() && !self.promoted_paths.is_empty() {
            self.evict_idle_paths();
        }
    }

    /// Get view count (for testing).
    pub fn view_count(&self) -> usize {
        self.view_manager.view_count()
    }

    /// Refresh the per-database on-disk size gauge for billing telemetry.
    ///
    /// We bill on the compacted blob only. `io().size()` reads an in-memory
    /// `tracked_size` cell (no syscall) and is refreshed at the end of every
    /// incremental compaction batch (`BlobSession::apply_updates_with_sidecar`),
    /// so it's current to within one WAL cycle (≤ a few MB) — negligible at
    /// GB-granularity billing. The sidecar and not-yet-compacted WAL are
    /// intentionally excluded. In-memory/ephemeral databases have no blob
    /// session, so the gauge stays at 0.
    pub(super) async fn refresh_data_size(&self) {
        let Some(session) = self.blob_session.as_ref() else {
            return;
        };
        if let Ok(size) = lark_blob::BlobIO::size(session.io()).await {
            self.metrics.set_data_size(size);
        }
    }

    /// Whether the database has reached its size cap and should reject growth
    /// writes. Reads the periodically-refreshed `data_size` gauge, so it's an
    /// approximate (slightly-stale) check — appropriate for a 1 TB backstop.
    pub(super) fn is_at_size_cap(&self) -> bool {
        self.metrics.data_size() >= MAX_DATABASE_SIZE_BYTES
    }

    /// Charge `bytes` against the durable-write rate limiter, returning a NACK to
    /// send if the write must be rejected (rate exceeded), else `None`. Ephemeral
    /// (in-memory) databases are exempt — they incur no durable storage cost, so
    /// this also leaves tests/emulator/benchmarks on ephemeral DBs unthrottled.
    pub(super) fn check_write_rate(
        &mut self,
        bytes: usize,
        client_id: &str,
        request_id: &str,
    ) -> Option<ServerMessage> {
        if self.ephemeral || self.write_rate_limiter.try_consume(bytes) {
            return None;
        }
        self.record_nacked_write(client_id, request_id);
        Some(ServerMessage::nack(
            request_id,
            error::RATE_LIMITED,
            "write rate limit exceeded; retry shortly",
        ))
    }

    /// Emit metrics to stdout in JSON format (for Vector to pick up).
    /// Only emits if there was activity since the last emission.
    pub(super) fn emit_metrics(&mut self) {
        if let Some(snapshot) = self.metrics.emit_and_reset() {
            // Extract just the database name from "project/database" id
            let database_name = self.pure_database_id.clone();

            // Get server ID from environment or use hostname
            let server_id =
                std::env::var("LARK_SERVER_ID").unwrap_or_else(|_| "localhost".to_string());

            let json = snapshot.to_json(&self.project_id, &database_name, &server_id, self.core_id);

            // Forward to the shipper thread when direct push is enabled. Non-blocking:
            // a full channel (slow/dead shipper) drops the sample rather than stalling
            // this core.
            if let Some(tx) = &self.metrics_tx {
                let _ = tx.try_send(json.clone());
            }

            // Always emit to stdout: this is what an external log shipper (e.g. Vector)
            // scrapes, and it keeps the line visible in logs regardless of push.
            println!("{}", json);
        }
    }

    pub(super) fn emit_promotion_stats(&mut self) {
        let snap = self.promotion_stats.reset();
        if snap.count > 0 {
            info!(
                db = %self.id,
                promotions = snap.count,
                total_ms = format!("{:.1}", snap.total_us as f64 / 1000.0),
                total_read_ms = format!("{:.1}", snap.total_read_us as f64 / 1000.0),
                p50_ms = format!("{:.1}", snap.p50 as f64 / 1000.0),
                p95_ms = format!("{:.1}", snap.p95 as f64 / 1000.0),
                p99_ms = format!("{:.1}", snap.p99 as f64 / 1000.0),
                read_p50_ms = format!("{:.1}", snap.read_p50 as f64 / 1000.0),
                read_p95_ms = format!("{:.1}", snap.read_p95 as f64 / 1000.0),
                read_p99_ms = format!("{:.1}", snap.read_p99 as f64 / 1000.0),
                pread_count = snap.pread_count,
                bytes_read = snap.bytes_read,
                cache_hits = snap.cache_hits,
                cache_hit_bytes = snap.cache_hit_bytes,
                cache_header_misses = snap.cache_header_misses,
                pending_wal = self.pending_wal_entries.len(),
                promoted_paths = self.promoted_paths.len(),
                "Promotion stats"
            );
        }
    }
}
