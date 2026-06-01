use super::*;

impl Database {
    // =========================================================================
    // Write Deduplication
    // =========================================================================

    /// Get the connection ID for a client.
    fn get_client_connection_id(&self, client_id: &str) -> Option<&str> {
        self.clients
            .get(client_id)
            .map(|c| c.connection_id.as_str())
    }

    /// Check if a write with the given request ID was already processed.
    /// Returns true if the write should be skipped (already processed).
    pub(super) fn is_write_processed(&self, client_id: &str, request_id: &str) -> bool {
        if request_id.is_empty() {
            return false; // No request ID = no deduplication
        }
        let connection_id = match self.get_client_connection_id(client_id) {
            Some(id) if !id.is_empty() => id,
            _ => return false, // No connection ID = no deduplication
        };
        self.processed_writes
            .get(connection_id)
            .is_some_and(|set| set.contains(request_id))
    }

    /// Record that a write was processed for deduplication.
    pub(super) fn record_processed_write(&mut self, client_id: &str, request_id: &str) {
        if request_id.is_empty() {
            return; // No request ID = no deduplication
        }
        let connection_id = match self.get_client_connection_id(client_id) {
            Some(id) if !id.is_empty() => id.to_string(),
            _ => return, // No connection ID = no deduplication
        };
        let set = self.processed_writes.entry(connection_id).or_default();
        // Only insert if not already present (IndexSet handles this)
        if set.insert(request_id.to_string()) {
            // Evict entries if over limit
            // Use swap_remove_index (O(1)) instead of shift_remove_index (O(n))
            // Order doesn't matter for deduplication - we just check membership
            while set.len() > MAX_WRITES_PER_CONNECTION {
                set.swap_remove_index(0);
            }
        }
    }

    /// Check if a write is tainted (depends on a nacked write).
    /// Returns true if the write should be silently ignored.
    pub(super) fn is_write_tainted(
        &self,
        client_id: &str,
        pending_writes: &Option<Vec<String>>,
    ) -> bool {
        let pending = match pending_writes {
            Some(pw) if !pw.is_empty() => pw,
            _ => return false, // No pending writes = not tainted
        };
        let connection_id = match self.get_client_connection_id(client_id) {
            Some(id) if !id.is_empty() => id,
            _ => return false, // No connection ID = can't check
        };
        let nacked_set = match self.nacked_writes.get(connection_id) {
            Some(set) => set,
            None => return false, // No nacked writes for this connection
        };
        for request_id in pending {
            if nacked_set.contains(request_id) {
                return true; // Found a nacked write = tainted
            }
        }
        false
    }

    /// Record that a write was nacked (for tainted write detection).
    pub(super) fn record_nacked_write(&mut self, client_id: &str, request_id: &str) {
        if request_id.is_empty() {
            return; // No request ID = no tracking
        }
        let connection_id = match self.get_client_connection_id(client_id) {
            Some(id) if !id.is_empty() => id.to_string(),
            _ => return, // No connection ID = no tracking
        };
        let set = self.nacked_writes.entry(connection_id).or_default();
        if set.insert(request_id.to_string()) {
            // Evict entries if over limit
            // Use swap_remove_index (O(1)) instead of shift_remove_index (O(n))
            while set.len() > MAX_WRITES_PER_CONNECTION {
                set.swap_remove_index(0);
            }
        }
    }

    /// Drain all pending inbox messages and disconnect any clients with an error.
    /// Called when the database fails during initialization (e.g., load_from_disk or WAL init)
    /// to ensure clients don't hang waiting for a response.
    pub(super) async fn drain_inbox_with_error(&mut self, reason: &str) {
        while let Some(Some(msg)) = poll_immediate(self.inbox.recv()).await {
            if msg.add_client
                && let Some(conn) = &msg.conn
            {
                let nack = ServerMessage::nack("0", error::UNAVAILABLE, reason);
                if let Ok(data) = nack.encode() {
                    let _ = conn.try_send(data.into(), false, false);
                }
                conn.close();
            }
            // Other messages (protocol, disconnect, etc.) are silently dropped
        }
    }

    /// Run the database message loop.
    /// Event-driven using Glommio's local_channel with message batching.
    /// Processes up to 128 messages or 10ms worth of work before yielding.
    pub async fn run(mut self) {
        debug!("Database {} starting", self.id);

        // Initialize database (BlobSession, etc.)
        if self.pending_disk_load {
            let load_failed = match self.load_from_disk().await {
                Ok(()) => false,
                Err(e) => {
                    error!(
                        "[STORAGE INTEGRITY] {}: Failed to initialize: {}. Database will not serve requests.",
                        self.id, e
                    );
                    true
                }
            };
            self.pending_disk_load = false;

            if load_failed {
                // Don't enter Serving state - database will shut down
                // Disconnect any pending clients so they don't hang indefinitely
                self.drain_inbox_with_error("Database failed to initialize")
                    .await;
                return;
            }

            // Initialize WAL writer
            if !self.init_wal_writer().await {
                // WAL writer failed to initialize - can't accept writes durably
                error!(
                    "[STORAGE INTEGRITY] {}: Failed to initialize WAL writer. Database will not serve requests.",
                    self.id
                );
                self.drain_inbox_with_error("Database WAL failed to initialize")
                    .await;
                return;
            }
        }

        // If startup found many uncompacted WAL files, trigger compaction now.
        if self.needs_startup_compaction {
            self.needs_startup_compaction = false;
            self.notify_compaction().await;
        }

        // Transition to serving
        self.state = DatabaseState::Serving;

        // Track periodic task timing
        let mut last_volatile_fast_flush = Instant::now();
        let mut last_volatile_slow_flush = Instant::now();
        let mut last_wal_sync = Instant::now();
        let mut last_housekeeping = Instant::now();
        let mut last_metrics_emit = Instant::now();
        let mut last_promotion_stats_emit = Instant::now();
        let mut last_backup_marker = Instant::now();

        // Batch processing constants
        const MAX_BATCH_SIZE: usize = 128;
        const MAX_BATCH_DURATION: Duration = Duration::from_millis(10);
        const PERIODIC_INTERVAL: Duration = Duration::from_millis(50);

        loop {
            // Wait for first message or periodic timeout
            let timeout = Timer::new(PERIODIC_INTERVAL);
            enum PollResult {
                GotMessage,
                Timeout,
                InboxClosed,
            }
            let poll_result = futures::select! {
                msg = self.inbox.recv().fuse() => {
                    if let Some(mut msg) = msg {
                        // Stamp inbox pop time for latency tracking
                        if let Some(ref mut ts) = msg.timestamps {
                            ts.stamp_db_inbox_pop();
                        }

                        // Handle the first message
                        self.handle_message_internal(&mut msg).await;

                        // Stamp work complete and record latency
                        if let Some(ref mut ts) = msg.timestamps {
                            ts.stamp_work_complete();
                            crate::metrics::record_latency(ts);
                        }
                        PollResult::GotMessage
                    } else {
                        // Inbox closed - all senders dropped, time to shut down
                        PollResult::InboxClosed
                    }
                }
                _ = timeout.fuse() => {
                    PollResult::Timeout
                }
            };

            // Exit if inbox was closed (all handles dropped)
            if matches!(poll_result, PollResult::InboxClosed) {
                debug!(
                    "Database {} inbox closed, shutting down gracefully",
                    self.id
                );
                break;
            }

            let got_message = matches!(poll_result, PollResult::GotMessage);

            // If we got a message, drain any immediately-available messages (batching)
            if got_message {
                let batch_start = Instant::now();
                let mut batch_count = 1;

                while batch_count < MAX_BATCH_SIZE && batch_start.elapsed() < MAX_BATCH_DURATION {
                    // poll_immediate polls once without blocking
                    match poll_immediate(self.inbox.recv()).await {
                        Some(Some(mut msg)) => {
                            if let Some(ref mut ts) = msg.timestamps {
                                ts.stamp_db_inbox_pop();
                            }

                            self.handle_message_internal(&mut msg).await;

                            if let Some(ref mut ts) = msg.timestamps {
                                ts.stamp_work_complete();
                                crate::metrics::record_latency(ts);
                            }
                            batch_count += 1;
                        }
                        _ => break, // No more ready messages
                    }
                }

                if batch_count > 1 {
                    trace!(
                        "Database {} processed batch of {} messages",
                        self.id, batch_count
                    );
                }
            }

            // Yield to scheduler to allow TCP tasks to run
            glommio::yield_if_needed().await;

            // Check for fatal error (e.g., failed blob generation switch)
            if self.fatal_error {
                error!("Database {} shutting down due to fatal error", self.id);
                break;
            }

            // Check if all external handles have been dropped (graceful shutdown)
            // Rc::strong_count() == 1 means only the Database's copy remains
            if Rc::strong_count(&self.inbox_sender) == 1 {
                debug!(
                    "Database {} all handles dropped, shutting down gracefully",
                    self.id
                );
                break;
            }

            // Run periodic tasks based on elapsed time
            let now = Instant::now();

            // Flush volatile batches for high-frequency clients (50ms)
            if now.duration_since(last_volatile_fast_flush) >= VOLATILE_FAST_FLUSH_INTERVAL {
                self.flush_volatile_fast();
                last_volatile_fast_flush = now;
            }

            // Flush volatile batches for low-frequency clients (333ms)
            if now.duration_since(last_volatile_slow_flush) >= VOLATILE_SLOW_FLUSH_INTERVAL {
                self.flush_volatile_slow();
                last_volatile_slow_flush = now;
            }

            // Sync WAL to disk (2s) - async to avoid blocking other DBs
            if now.duration_since(last_wal_sync) >= WAL_SYNC_INTERVAL {
                self.sync_wal().await;
                last_wal_sync = now;
            }

            // Housekeeping (5s)
            if now.duration_since(last_housekeeping) >= Duration::from_secs(5) {
                self.housekeeping().await;
                last_housekeeping = now;

                // Debug: print stats (only at debug level to reduce noise)
                trace!(
                    "Database {} stats: clients={}, views={}",
                    self.id,
                    self.clients.len(),
                    self.view_manager.view_count()
                );

                // Check for idle shutdown
                if self.clients.is_empty() && self.last_activity.elapsed() > Duration::from_secs(60)
                {
                    debug!("Database {} idle, shutting down", self.id);
                    break;
                }
            }

            // Emit metrics to stdout (60s, only if active)
            if now.duration_since(last_metrics_emit) >= METRICS_EMIT_INTERVAL {
                self.refresh_data_size().await;
                self.emit_metrics();
                last_metrics_emit = now;
            }

            // Emit promotion stats (30s)
            if now.duration_since(last_promotion_stats_emit) >= Duration::from_secs(30) {
                self.emit_promotion_stats();
                last_promotion_stats_emit = now;
            }

            // Write backup marker (5 min) so lark-compact syncs WAL files for active databases,
            // even if no WAL rotation has occurred. Written every 5 min to ensure lark-compact's
            // 15-minute WAL sync cycle always finds a fresh marker.
            if now.duration_since(last_backup_marker) >= Duration::from_secs(300) {
                if self.wal_writer.is_some() {
                    self.write_compaction_queue_marker().await;
                }
                last_backup_marker = now;
            }
        }

        // Final WAL sync and close before shutdown
        self.sync_wal().await;
        self.close_wal().await;

        // Write compaction queue marker so lark-compact syncs any unrotated WAL data
        self.write_compaction_queue_marker().await;

        // Tell StorageWorker to clean up cached state (BlobSession, shared CachedIO)
        self.notify_storage_worker_shutdown();

        // Drop blob session (owns the IO handle — closes on drop)
        self.blob_session.take();

        debug!("Database {} stopped", self.id);
    }

    async fn handle_message_internal(&mut self, msg: &mut InboxMessage) {
        self.last_activity = Instant::now();

        // Handle compaction complete from StorageWorker
        if let Some(cc) = msg.compaction_complete.take() {
            self.handle_compaction_complete(cc).await;
            return;
        }

        // Handle force eviction (for testing)
        if msg.force_evict_all {
            self.force_evict_all_paths();
            return;
        }

        // Handle rules hot-reload from CONFIG_PUSH
        if msg.has_evaluator_update {
            match msg.evaluator_update.take() {
                Some(evaluator) => {
                    debug!("Database {} applying new rules from CONFIG_PUSH", self.id);
                    self.set_evaluator(evaluator);
                    // Rules just changed. A tightened ruleset must not keep
                    // streaming to listeners it now forbids, so re-check every
                    // active subscription against the new rules and revoke the
                    // ones that no longer pass.
                    self.revoke_all_unauthorized_subscriptions().await;
                }
                None => {
                    debug!(
                        "Database {} clearing rules from CONFIG_PUSH (fully open)",
                        self.id
                    );
                    self.evaluator = None;
                    self.set_volatile_paths(Vec::new());
                    // Rules cleared to fully-open: `can_read` now allows
                    // everything, so no existing subscription can have become
                    // unauthorized. Nothing to revoke.
                }
            }
            return;
        }

        // Handle special message types
        if msg.add_client {
            if let Some(ref conn) = msg.conn {
                self.add_client_internal(
                    &msg.client_id,
                    msg.auth_info.clone(),
                    &msg.connection_id,
                    conn.clone(),
                );
            }
            return;
        }

        if msg.disconnect {
            self.handle_disconnect(&msg.client_id).await;
            return;
        }

        if msg.has_auth {
            self.handle_auth_update(&msg.client_id, msg.auth_update.clone())
                .await;
            return;
        }

        // Handle protocol message
        if let Some(ref protocol_msg) = msg.message {
            let volatile = msg.volatile;
            self.handle_protocol_message(&msg.client_id, protocol_msg.clone(), volatile)
                .await;

            // Record end-to-end latency (TCP receive to processing complete)
            let latency_us = msg.start_time.elapsed().as_micros() as u32;
            self.metrics.record_latency(latency_us);
        }
    }

    async fn handle_protocol_message(
        &mut self,
        client_id: &str,
        msg: ClientMessage,
        volatile: bool,
    ) {
        let response = match msg.op.as_str() {
            op::JOIN => self.handle_join(client_id, &msg),
            op::SET => self.handle_set(client_id, &msg, volatile).await,
            op::UPDATE => self.handle_update(client_id, &msg, volatile).await,
            op::REMOVE => self.handle_remove(client_id, &msg, volatile).await,
            op::SUBSCRIBE => self.handle_subscribe(client_id, &msg).await,
            op::UNSUBSCRIBE => self.handle_unsubscribe(client_id, &msg),
            op::ONCE => self.handle_once(client_id, &msg).await,
            op::ON_DISCONNECT => self.handle_on_disconnect(client_id, &msg).await,
            op::TRANSACTION => self.handle_transaction(client_id, &msg).await,
            op::LEAVE => {
                // Leave is a graceful disconnect - trigger ondisconnect hooks
                self.handle_disconnect(client_id).await;
                let request_id = msg.request_id.as_deref().unwrap_or("");
                Some(ServerMessage::ack(request_id))
            }
            op::PING => None, // Client keepalive, swallow without response
            op::PONG => None, // Keepalive response, ignore
            _ => {
                let request_id = msg.request_id.as_deref().unwrap_or("");
                Some(ServerMessage::nack(
                    request_id,
                    error::INVALID_OPERATION,
                    "unknown operation",
                ))
            }
        };

        // Send response
        if let Some(resp) = response {
            self.send_to_client(client_id, &resp, false).await;
        }
    }
}
