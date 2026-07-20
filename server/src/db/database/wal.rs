use super::*;

impl Database {
    // =========================================================================
    // WAL (Write-Ahead Log) Operations - Async
    // =========================================================================

    /// Check if WAL I/O has failed. When true, all writes must be NACKed.
    pub(super) fn is_wal_failed(&self) -> bool {
        self.wal_failed
    }

    /// Mark WAL as failed. Called on first I/O error.
    fn set_wal_failed(&mut self) {
        if !self.wal_failed {
            self.wal_failed = true;
            error!(
                "[STORAGE INTEGRITY] {}: WAL I/O failure detected. All writes will be NACKed until recovery.",
                self.id
            );
        }
    }

    /// Attempt to recover WAL after failure.
    /// Tries a test write + sync. If successful, clears the failure flag.
    pub(super) async fn try_recover_wal(&mut self) {
        if !self.wal_failed {
            return;
        }

        if let Some(ref mut writer) = self.wal_writer {
            // Attempt a test write (no-op WAL entry) + sync
            let test_entry = WalEntry::set("/__wal_recovery_test", Value::Bool(true));
            match writer.append_one(&test_entry) {
                Ok(_) => match writer.sync(fsync_on_wal_flush()).await {
                    Ok(_) => {
                        self.wal_failed = false;
                        self.wal_dirty = false;
                        self.wal_pending_entries = 0;
                        self.wal_pending_bytes = 0;
                        info!(
                            "[STORAGE INTEGRITY] {}: WAL recovered. Resuming normal write operations.",
                            self.id
                        );
                    }
                    Err(e) => {
                        debug!(
                            "[STORAGE INTEGRITY] {}: WAL recovery sync failed (will retry): {}",
                            self.id, e
                        );
                    }
                },
                Err(e) => {
                    debug!(
                        "[STORAGE INTEGRITY] {}: WAL recovery write failed (will retry): {}",
                        self.id, e
                    );
                }
            }
        }
    }

    /// Notify the per-core storage worker that a WAL file was rotated and is ready for compaction.
    pub(super) async fn notify_compaction(&self) {
        if let (Some(tx), Some(data_dir), Some(session)) =
            (&self.compaction_tx, &self.data_dir, &self.blob_session)
        {
            // Write .compaction-queue marker for the external compactor binary.
            self.write_compaction_queue_marker().await;

            // Clone the CachedIO via clone_for_reading — shares the Rc-backed byte cache
            // so StorageWorker writes are immediately visible to our reads (write-through).
            let cached_io = match session.io().clone_for_reading().await {
                Ok(io) => io,
                Err(e) => {
                    warn!(
                        "[Persistence] {}: Failed to clone CachedIO for storage worker: {}",
                        self.id, e
                    );
                    return;
                }
            };

            let request = CompactionRequest {
                data_dir: data_dir.clone(),
                database_id: self.id.clone(),
                inbox_sender: self.inbox_sender.clone(),
                cached_io,
            };
            match tx.try_send(StorageWorkerMessage::Compact(request)) {
                Ok(_) => {
                    info!(
                        "[Persistence] {}: Sent compaction request to storage worker",
                        self.id
                    );
                }
                Err(_) => {
                    warn!(
                        "[Persistence] {}: Compaction channel full, skipping notification",
                        self.id
                    );
                }
            }
        }
    }

    /// Notify the StorageWorker to clean up cached state for this database.
    pub(super) fn notify_storage_worker_shutdown(&self) {
        if let Some(tx) = &self.compaction_tx {
            let _ = tx.try_send(StorageWorkerMessage::Shutdown {
                database_id: self.id.clone(),
            });
        }
    }

    /// Write a SET operation to the WAL (in-memory buffer only, no I/O).
    /// Returns false if serialization failed (caller should NACK).
    pub(super) fn wal_write_set(&mut self, path: &str, value: &Value) -> bool {
        // Canonicalize SET-to-null as DELETE so the WAL has a single encoding
        // for "this path is gone." Without this, `WalEntry::set(path, Null)`
        // serializes as `{"o":"s","v":null}`; serde then deserializes the null
        // as `Option::None` on read, which the SET arm of the WAL-replay loops
        // silently skipped — so the deletion vanished on restart.
        if value.is_null() {
            return self.wal_write_delete(path);
        }
        if let Some(ref mut writer) = self.wal_writer {
            let mut entry = WalEntry::set(path, value.clone());
            entry.sequence = writer.sequence();
            match writer.append_one(&entry) {
                Ok(_) => {
                    self.wal_dirty = true;
                    self.wal_pending_entries += 1;
                    self.wal_pending_bytes += writer.bytes_written_last_append();
                    let idx = self.pending_wal_entries.len();
                    self.wal_index.add(&entry.path, idx);
                    self.pending_wal_entries.push(entry);
                    true
                }
                Err(e) => {
                    self.set_wal_failed();
                    error!(
                        "[STORAGE INTEGRITY] {}: WAL write failed for SET {}: {}",
                        self.id, path, e
                    );
                    false
                }
            }
        } else {
            true // No WAL writer (ephemeral) - always succeeds
        }
    }

    /// Write an UPDATE operation to the WAL (in-memory buffer only, no I/O).
    /// Returns false if serialization failed (caller should NACK).
    pub(super) fn wal_write_update(
        &mut self,
        path: &str,
        updates: &serde_json::Map<String, Value>,
    ) -> bool {
        if let Some(ref mut writer) = self.wal_writer {
            let mut entry = WalEntry::update(path, Value::Object(updates.clone()));
            entry.sequence = writer.sequence();
            match writer.append_one(&entry) {
                Ok(_) => {
                    self.wal_dirty = true;
                    self.wal_pending_entries += 1;
                    self.wal_pending_bytes += writer.bytes_written_last_append();
                    let idx = self.pending_wal_entries.len();
                    self.wal_index.add(&entry.path, idx);
                    self.pending_wal_entries.push(entry);
                    true
                }
                Err(e) => {
                    self.set_wal_failed();
                    error!(
                        "[STORAGE INTEGRITY] {}: WAL write failed for UPDATE {}: {}",
                        self.id, path, e
                    );
                    false
                }
            }
        } else {
            true // No WAL writer (ephemeral) - always succeeds
        }
    }

    /// Write a DELETE operation to the WAL (in-memory buffer only, no I/O).
    /// Returns false if serialization failed (caller should NACK).
    pub(super) fn wal_write_delete(&mut self, path: &str) -> bool {
        if let Some(ref mut writer) = self.wal_writer {
            let mut entry = WalEntry::delete(path);
            entry.sequence = writer.sequence();
            match writer.append_one(&entry) {
                Ok(_) => {
                    self.wal_dirty = true;
                    self.wal_pending_entries += 1;
                    self.wal_pending_bytes += writer.bytes_written_last_append();
                    let idx = self.pending_wal_entries.len();
                    self.wal_index.add(&entry.path, idx);
                    self.pending_wal_entries.push(entry);
                    true
                }
                Err(e) => {
                    self.set_wal_failed();
                    error!(
                        "[STORAGE INTEGRITY] {}: WAL write failed for DELETE {}: {}",
                        self.id, path, e
                    );
                    false
                }
            }
        } else {
            true // No WAL writer (ephemeral) - always succeeds
        }
    }

    /// Flush the buffered WAL entries to the WAL file (async).
    ///
    /// Whether this also issues an `fdatasync` (durable on the device) or only
    /// writes to the OS page cache is governed by `FSYNC_ON_WAL_FLUSH`. Uses
    /// async I/O to avoid blocking other databases on the core.
    pub(super) async fn sync_wal(&mut self) {
        if !self.wal_dirty {
            return;
        }

        let entries = self.wal_pending_entries;
        let bytes = self.wal_pending_bytes;

        if let Some(ref mut writer) = self.wal_writer {
            let start = Instant::now();
            match writer.sync(fsync_on_wal_flush()).await {
                Ok(rotated) => {
                    let duration = start.elapsed();
                    self.wal_dirty = false;
                    self.wal_pending_entries = 0;
                    self.wal_pending_bytes = 0;

                    debug!(
                        "[WAL Sync] {}: flushed {} entries ({} bytes) in {:?}",
                        self.id, entries, bytes, duration
                    );

                    // Record WAL flush stats
                    crate::metrics::record_wal_flush(duration, entries, bytes);

                    if rotated {
                        tracing::debug!("[Persistence] {}: WAL rotated", self.id);
                        self.notify_compaction().await;
                    }
                }
                Err(e) => {
                    self.set_wal_failed();
                    error!("[STORAGE INTEGRITY] {}: WAL sync failed: {}", self.id, e);
                }
            }
        }
    }

    /// Write a .compaction-queue marker so lark-compact knows to sync this DB's WAL files.
    /// Called on shutdown to ensure any unrotated WAL data gets synced offsite.
    pub(super) async fn write_compaction_queue_marker(&self) {
        let Some(data_dir) = &self.data_dir else {
            return;
        };
        if let Some(root_dir) = data_dir.parent().and_then(|p| p.parent()) {
            let queue_dir = root_dir.join(".compaction-queue");
            let marker_name = self.id.replace('/', "#");
            let marker_path = queue_dir.join(&marker_name);
            let _ = crate::storage::create_dir_all_async(&queue_dir).await;
            let _ = crate::storage::write_file_async(&marker_path, b"").await;
        }
    }

    /// Close the WAL writer (for clean shutdown).
    pub(super) async fn close_wal(&mut self) {
        if let Some(mut writer) = self.wal_writer.take()
            && let Err(e) = writer.close().await
        {
            warn!("Failed to close WAL: {}", e);
        }
    }
}
