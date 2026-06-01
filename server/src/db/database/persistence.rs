use super::*;

impl Database {
    /// Initialize the async WAL writer.
    ///
    /// This is called after disk loading in `run()` to get the correct min_sequence
    /// from the manifest. Uses runtime-adaptive async I/O.
    ///
    /// Returns true if initialization succeeded (or was skipped for ephemeral DBs),
    /// false if initialization failed and the database should not serve requests.
    pub async fn init_wal_writer(&mut self) -> bool {
        // Skip for ephemeral databases
        if self.data_dir.is_none() {
            return true; // Ephemeral - no WAL needed
        }

        let wal_dir = match self.wal_dir() {
            Some(dir) => dir,
            None => return true,
        };

        // Use blob_sequence so the WAL writer starts after already-compacted entries.
        let min_sequence = self.blob_sequence;

        match WalWriter::with_min_sequence(&wal_dir, min_sequence).await {
            Ok(writer) => {
                debug!(
                    "[Persistence] {}: Initialized async WAL writer (sequence={})",
                    self.id,
                    writer.sequence()
                );
                self.wal_writer = Some(writer);
                true
            }
            Err(e) => {
                error!(
                    "[STORAGE INTEGRITY] {}: Failed to initialize WAL writer: {}",
                    self.id, e
                );
                false
            }
        }
    }

    /// Read the WAL sequence file from `{data_dir}/sequence`.
    /// Returns the sequence number through which the blob is up to date.
    /// Returns 0 if the file doesn't exist or can't be parsed.
    async fn read_sequence_file(data_dir: &std::path::Path) -> i64 {
        let path = data_dir.join("sequence");
        match read_file_async(&path).await {
            Ok(bytes) => {
                let s = String::from_utf8_lossy(&bytes);
                s.trim().parse::<i64>().unwrap_or(0)
            }
            Err(_) => 0, // File doesn't exist or can't be read
        }
    }

    /// Return the WAL directory for this database.
    fn wal_dir(&self) -> Option<PathBuf> {
        self.data_dir.as_ref().map(|d| d.join("wal"))
    }

    /// Load database state from BlobSession (async).
    /// With the blob model, the Tree starts empty and data is loaded lazily
    /// via navigate() and read_subtree() when accessed.
    pub(super) async fn load_from_disk(&mut self) -> std::io::Result<()> {
        let data_dir = match &self.data_dir {
            Some(dir) => dir.clone(),
            None => return Ok(()), // Ephemeral — no blob needed
        };

        // Use fixed blob filename: blob.lark
        // If none exists, create it so the storage worker always has one to apply to.
        let bp = blob_path(&data_dir);
        if !bp.exists() {
            std::fs::create_dir_all(&data_dir)
                .map_err(|e| std::io::Error::other(format!("creating data dir: {}", e)))?;

            let io = CachedIO::new(GlommioBlobIO::create(&bp).await?);
            let session = BlobSession::init(io)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            lark_blob::BlobIO::sync(session.io()).await?;

            self.blob_session = Some(session);
            self.blob_generation = 0;
            *self.tree.write().unwrap() = Tree::new_sentinel();
            self.sentinel_paths.insert("/".to_string());

            tracing::debug!("Database {} created blank blob at {:?}", self.id, bp);

            // Continue to WAL replay below
            return self.load_wal_entries().await;
        }

        let blob_gen = read_blob_generation(&data_dir);

        // Open existing blob file via Glommio io_uring — reads yield to scheduler.
        let raw_io = GlommioBlobIO::open(&bp).await?;
        let io = CachedIO::new(raw_io);

        // BlobSession::open reads just the header + dictionary (small, fixed-size)
        let session = BlobSession::open(io)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        self.blob_session = Some(session);
        self.blob_generation = blob_gen;

        // Initialize tree with Sentinel root — data will be promoted on demand.
        *self.tree.write().unwrap() = Tree::new_sentinel();
        self.sentinel_paths.insert("/".to_string());

        tracing::debug!(
            "Database {} opened BlobSession from {:?} (gen {})",
            self.id,
            bp,
            blob_gen
        );

        self.load_wal_entries().await
    }

    /// Load uncompacted WAL entries from disk into pending_wal_entries.
    ///
    /// WAL entries are NOT replayed into the tree here. They are stored in
    /// `pending_wal_entries` and replayed on top of blob data when a path is
    /// promoted via `promote_path()`. This ensures correct ordering — blob data
    /// is read first, then all WAL entries (SET, UPDATE, DELETE) are applied.
    async fn load_wal_entries(&mut self) -> std::io::Result<()> {
        let data_dir = match &self.data_dir {
            Some(dir) => dir.clone(),
            None => return Ok(()),
        };

        // Read sequence file from local data_dir — tells us which WAL entries
        // are already compacted into the blob.
        self.blob_sequence = Self::read_sequence_file(&data_dir).await;

        // Load uncompacted WAL entries (sequence > blob_sequence).
        let wal_dir = match self.wal_dir() {
            Some(dir) => dir,
            None => return Ok(()),
        };
        if wal_dir.exists() {
            let reader = WalReader::new(&wal_dir);
            match reader.read_since(self.blob_sequence + 1).await {
                Ok(entries) => {
                    if !entries.is_empty() {
                        tracing::debug!(
                            "Database {} loaded {} WAL entries (after sequence {})",
                            self.id,
                            entries.len(),
                            self.blob_sequence
                        );

                        self.pending_wal_entries = entries;
                        self.wal_index.rebuild(&self.pending_wal_entries);
                    }

                    // If many small WAL files have accumulated, request compaction
                    // on startup to consolidate them into the blob.
                    let file_count = reader.file_count_since(self.blob_sequence + 1).await;
                    if file_count > 10 {
                        tracing::info!(
                            "Database {} has {} uncompacted WAL files, requesting startup compaction",
                            self.id,
                            file_count
                        );
                        self.needs_startup_compaction = true;
                    }
                }
                Err(e) => {
                    tracing::error!("Database {} failed to read WAL entries: {}", self.id, e);
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    // =========================================================================
    /// Handle a compaction complete notification from the StorageWorker.
    ///
    /// Trims pending_wal_entries to drop entries already baked into the blob,
    /// and updates blob_sequence so future promotions skip those entries.
    pub(super) async fn handle_compaction_complete(&mut self, cc: CompactionComplete) {
        let new_sequence = cc.sequence;

        // If the StorageWorker is on a different blob generation (e.g., after an
        // external full compaction by lark-compact), we must switch to that blob
        // BEFORE trimming WAL entries. Otherwise we'd lose WAL entries that haven't
        // been applied to our (old) blob yet.
        if cc.blob_generation != self.blob_generation {
            let old_gen = self.blob_generation;

            // Use the StorageWorker's CachedIO if provided (shares its cache),
            // otherwise fall back to opening fresh.
            let io = if let Some(cached_io) = cc.cached_io {
                match cached_io.clone_for_reading().await {
                    Ok(io) => io,
                    Err(e) => {
                        error!(
                            "[Compaction] {}: Failed to clone CachedIO from StorageWorker (gen {}): {} — falling back to fresh open",
                            self.id, cc.blob_generation, e
                        );
                        match self.open_fresh_cached_io(cc.blob_generation).await {
                            Some(io) => io,
                            None => return,
                        }
                    }
                }
            } else {
                match self.open_fresh_cached_io(cc.blob_generation).await {
                    Some(io) => io,
                    None => return,
                }
            };

            match BlobSession::open(io).await {
                Ok(session) => {
                    self.blob_session = Some(session);
                    self.blob_generation = cc.blob_generation;
                    info!(
                        "[Compaction] {}: Switched to blob generation {} (was {})",
                        self.id, cc.blob_generation, old_gen
                    );
                }
                Err(e) => {
                    error!(
                        "[Compaction] {}: Failed to open BlobSession on blob.lark (gen {}): {} — shutting down",
                        self.id, cc.blob_generation, e
                    );
                    self.fatal_error = true;
                    return;
                }
            }
        } else {
            // Same blob generation with shared CachedIO. Container-header writes
            // by the StorageWorker are visible to our reads via the shared
            // Rc-backed `regions` map: `pwrite_deferred` patches the cached
            // region in place, and our `nav_cache` reads see the patched bytes
            // — that's the whole point of the shared cache.
            //
            // The blob header at bytes [0..64] is the exception. It's never
            // populated into CachedIO's `regions` (cache_region is only called
            // for container headers, not the file header), so when
            // `forward_via_parent_index` relocates the root container — writing
            // the new offset to bytes [16..24] via pwrite_deferred — the write
            // bypasses the cache and goes straight to disk. The StorageWorker's
            // `BlobSession.header.root_offset` field gets updated, but ours is
            // a separate copy from when we opened the session, and it's now
            // stale: subsequent reads navigate from the OLD root offset and
            // return whatever lived there before, or PathNotFound, or whatever
            // the free list reused that range for. That was the chaos-monkey
            // bug we hunted down.
            //
            // Fix: re-read the header from disk on every CompactionComplete.
            // Cost is two small preads (header + dictionary) per compaction
            // (so per ~5MB of WAL); the kernel page cache will almost always
            // have the bytes warm because the StorageWorker just wrote them.
            //
            // If this ever shows up in a profile, the next step is to cache
            // [0..HEADER_SIZE] in CachedIO's regions on session open — then
            // the StorageWorker's pwrite_deferred would patch it in place and
            // our pread would be a cache hit. We deferred that because (a) the
            // current cost is negligible at compaction cadence and (b) the
            // BlobSession.header *struct* would still need refreshing
            // separately — the `regions` map holds raw bytes, not the parsed
            // header. The fully-shared alternative (Rc<RefCell<BlobHeader>>
            // between sessions) eliminates the read entirely but is invasive
            // in lark-blob.
            if let Some(session) = self.blob_session.as_mut()
                && let Err(e) = session.refresh().await
            {
                error!(
                    "[Compaction] {}: BlobSession::refresh failed after incremental compaction: {} — reads may return stale data",
                    self.id, e
                );
            }
        }

        let before = self.pending_wal_entries.len();
        self.pending_wal_entries
            .retain(|e| e.sequence > new_sequence);
        let trimmed = before - self.pending_wal_entries.len();
        self.blob_sequence = new_sequence;
        if trimmed > 0 {
            self.wal_index.rebuild(&self.pending_wal_entries);
            debug!(
                "[Compaction] {}: Trimmed {} WAL entries (blob now at seq {}), {} remaining",
                self.id,
                trimmed,
                new_sequence,
                self.pending_wal_entries.len()
            );
        }
    }

    /// Open a fresh (independent) CachedIO on the current blob.lark.
    /// Returns None and sets fatal_error on failure.
    async fn open_fresh_cached_io(&mut self, blob_gen: u64) -> Option<CachedIO<GlommioBlobIO>> {
        let data_dir = self.data_dir.as_ref()?;
        let bp = blob_path(data_dir);
        match GlommioBlobIO::open(&bp).await {
            Ok(raw_io) => Some(CachedIO::new(raw_io)),
            Err(e) => {
                error!(
                    "[Compaction] {}: Failed to open blob.lark (gen {}): {} — shutting down",
                    self.id, blob_gen, e
                );
                self.fatal_error = true;
                None
            }
        }
    }
}
