//! Per-core storage worker for WAL compaction into blob storage.
//!
//! Each core gets one `StorageWorker` that runs on the lower-priority `db_tq`
//! task queue. When a database rotates its WAL file, it sends a `CompactionRequest`
//! to the worker. The worker then:
//!
//! 1. Reads the sequence file to find the current blob sequence
//! 2. Lists WAL files and finds completed ones (not the active one)
//! 3. Opens a BlobSession and applies WAL entries to the blob
//! 4. Writes the new sequence file
//!
//! The worker maintains persistent per-database state (BlobSession + IO handle)
//! across compaction requests, lazily initialized on first request for each database.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Instant;

use glommio::channels::local_channel::{LocalReceiver, LocalSender};
use lark_blob::wal::coalesce_wal_entries;
use lark_blob::{BlobIO, BlobSession, CachedIO};
use tracing::{debug, error, info, warn};

use crate::db::{CompactionComplete, InboxMessage};
use crate::db::{blob_path, read_blob_generation, sidecar_path};
use crate::storage::fsync;
use crate::storage::glommio_blob_io::GlommioBlobIO;
use crate::storage::wal::WalReader;

/// Messages sent from Database to StorageWorker.
#[allow(clippy::large_enum_variant)] // Compact dominates; boxing would churn all call sites
pub enum StorageWorkerMessage {
    /// Compact WAL files into the blob.
    Compact(CompactionRequest),
    /// Database is shutting down — clean up cached state.
    Shutdown { database_id: String },
}

/// A request from a Database to compact its WAL into the blob.
pub struct CompactionRequest {
    /// Data directory for this database (e.g., /tank/lark-data/project/database)
    pub data_dir: PathBuf,
    /// Database ID for logging
    pub database_id: String,
    /// Inbox sender for the database — used to send compaction notifications.
    pub inbox_sender: Rc<LocalSender<InboxMessage>>,
    /// Shared CachedIO from the Database's BlobSession.
    /// Cloned via clone_for_reading() — shares the Rc-backed byte cache so
    /// StorageWorker writes are immediately visible to the Database's reads.
    pub cached_io: CachedIO<GlommioBlobIO>,
}

/// Persistent per-database state held by the StorageWorker.
struct WorkerDbState {
    session: BlobSession<CachedIO<GlommioBlobIO>>,
    sidecar_io: GlommioBlobIO,
    /// Blob generation number (from blob.generation file).
    blob_generation: u64,
}

/// Per-core storage worker that processes WAL compaction requests.
pub struct StorageWorker {
    inbox: LocalReceiver<StorageWorkerMessage>,
    /// Persistent per-database state, keyed by database_id.
    db_states: HashMap<String, WorkerDbState>,
}

impl StorageWorker {
    /// Create a new storage worker.
    pub fn new(inbox: LocalReceiver<StorageWorkerMessage>) -> Self {
        Self {
            inbox,
            db_states: HashMap::new(),
        }
    }

    /// Run the storage worker loop forever, processing compaction requests.
    pub async fn run(&mut self) {
        debug!("[StorageWorker] Started, waiting for compaction requests");
        loop {
            let msg = match self.inbox.recv().await {
                Some(msg) => msg,
                None => {
                    // Channel closed — core is shutting down
                    debug!("[StorageWorker] Channel closed, stopping");
                    return;
                }
            };

            match msg {
                StorageWorkerMessage::Shutdown { database_id } => {
                    if self.db_states.remove(&database_id).is_some() {
                        debug!(
                            "[StorageWorker] {}: Cleaned up state (database shutdown)",
                            database_id
                        );
                    }
                }
                StorageWorkerMessage::Compact(request) => {
                    let db_id = request.database_id.clone();
                    match self.process_compaction(request).await {
                        Ok(_) => {}
                        Err(e) => {
                            error!("[StorageWorker] Compaction failed for {}: {}", db_id, e);
                            self.db_states.remove(&db_id);
                        }
                    }
                }
            }
        }
    }

    /// Get or lazily initialize the persistent state for a database.
    /// `blob_gen` and `blob_path` come from the caller's `scan_db_dir` result.
    /// `cached_io` is the shared CachedIO from the Database's BlobSession (via clone_for_reading).
    async fn get_or_init_state(
        &mut self,
        db_id: &str,
        blob_gen: u64,
        cached_io: CachedIO<GlommioBlobIO>,
        blob_path: &std::path::Path,
    ) -> Result<&mut WorkerDbState, String> {
        if !self.db_states.contains_key(db_id) {
            // Open sidecar for free list persistence (sidecar.lark alongside blob.lark)
            let sp = sidecar_path(blob_path.parent().unwrap());
            let sidecar_io = GlommioBlobIO::open_or_create(&sp)
                .await
                .map_err(|e| format!("opening sidecar {:?}: {}", sp, e))?;

            let session = BlobSession::open_with_sidecar(cached_io, Some(&sidecar_io))
                .await
                .map_err(|e| format!("opening blob session {:?}: {}", blob_path, e))?;

            let state = WorkerDbState {
                session,
                sidecar_io,
                blob_generation: blob_gen,
            };

            self.db_states.insert(db_id.to_string(), state);
            info!(
                "[StorageWorker] {}: Initialized state, blob {:?} (generation {})",
                db_id, blob_path, blob_gen
            );
        }

        Ok(self.db_states.get_mut(db_id).unwrap())
    }

    /// Process a single compaction request.
    async fn process_compaction(&mut self, request: CompactionRequest) -> Result<(), String> {
        let data_dir = request.data_dir;
        let db_id = request.database_id;
        let inbox_sender = request.inbox_sender;

        // 1. Single directory scan: check for .compacting marker and find highest blob.
        let (is_compacting, highest_blob) = scan_db_dir(&data_dir);

        if is_compacting {
            debug!(
                "[StorageWorker] {}: .compacting marker present, skipping",
                db_id
            );
            return Ok(());
        }

        let (blob_gen, blob_path) =
            highest_blob.ok_or_else(|| format!("no blob file found in {:?}", data_dir))?;

        // 2. If blob generation changed (lark-compact replaced blob.lark and bumped blob.generation),
        //    drop cached state so get_or_init_state re-opens on the new blob.
        let mut gen_changed = false;
        if let Some(state) = self.db_states.get(&db_id)
            && blob_gen != state.blob_generation
        {
            info!(
                "[StorageWorker] {}: Blob generation changed ({} -> {}), re-opening",
                db_id, state.blob_generation, blob_gen
            );
            gen_changed = true;
            self.db_states.remove(&db_id);
        }

        // 3. Read current blob sequence
        let sequence_path = data_dir.join("sequence");
        let blob_sequence = read_sequence_file(&sequence_path).await;

        // 4. List WAL files
        let wal_dir = data_dir.join("wal");
        let reader = WalReader::new(&wal_dir);
        let highest_sequence = reader.highest_sequence().await;

        if highest_sequence <= blob_sequence {
            debug!("[StorageWorker] {}: No new WAL files to compact", db_id);
            return Ok(());
        }

        // 5. Find completed WAL files: sequence > blob_sequence AND sequence < highest_sequence.
        //    The highest-sequence WAL file is the one the DB is actively writing to.
        let max_completed = highest_sequence - 1;
        if max_completed <= blob_sequence {
            debug!(
                "[StorageWorker] {}: No completed WAL files to compact",
                db_id
            );
            return Ok(());
        }

        let completed_files = reader
            .files_between(blob_sequence + 1, max_completed)
            .await
            .map_err(|e| format!("listing WAL files: {}", e))?;

        if completed_files.is_empty() {
            debug!("[StorageWorker] {}: No completed WAL files found", db_id);
            return Ok(());
        }

        info!(
            "[StorageWorker] {}: Compacting {} WAL file(s) (seq {}-{})",
            db_id,
            completed_files.len(),
            blob_sequence + 1,
            max_completed
        );

        // 6. Get or initialize persistent state for this database.
        //    On generation change, the request's CachedIO may point to the old blob
        //    (Database hasn't switched yet), so open a fresh independent CachedIO.
        //    The Database will switch on CompactionComplete and future requests will
        //    carry the new shared CachedIO.
        let io_for_init = if gen_changed {
            let raw_io = GlommioBlobIO::open(&blob_path)
                .await
                .map_err(|e| format!("opening blob {:?}: {}", blob_path, e))?;
            CachedIO::new(raw_io)
        } else {
            request.cached_io
        };
        let state = self
            .get_or_init_state(&db_id, blob_gen, io_for_init, &blob_path)
            .await?;

        // 7. Read all completed WAL files and collect updates
        let compact_start = Instant::now();
        let mut all_entries = Vec::new();

        for (_seq, filename) in &completed_files {
            let entries = reader
                .read_wal_file(filename)
                .await
                .map_err(|e| format!("reading WAL file {}: {}", filename, e))?;
            all_entries.extend(entries);
        }

        let total_wal_entries = all_entries.len();
        let updates = coalesce_wal_entries(all_entries);
        let total_updates = updates.len();

        // 8. Apply all updates in one call (with sidecar for free list persistence)
        state
            .session
            .apply_updates_with_sidecar(&updates, Some(&state.sidecar_io))
            .await
            .map_err(|e| format!("applying updates: {}", e))?;

        // 9. Write new sequence file (durable)
        //    (Blob is already synced by apply_updates_with_sidecar → flush_write_back.)
        let new_sequence = max_completed;
        fsync::write_file_durable_async(&sequence_path, new_sequence.to_string().as_bytes())
            .await
            .map_err(|e| format!("writing sequence file: {}", e))?;

        // 10. Notify the Database that compaction is complete — it can trim pending_wal_entries.
        //     Include the blob generation so the Database can switch if it's on a different blob
        //     (e.g., after an external full compaction by lark-compact).
        //     On generation change, send our CachedIO so the Database can share our cache
        //     on the new blob (re-establishing shared caching after the switch).
        let shared_io = if gen_changed {
            match state.session.io().clone_for_reading().await {
                Ok(io) => Some(io),
                Err(e) => {
                    warn!(
                        "[StorageWorker] {}: Failed to clone CachedIO for database: {}",
                        db_id, e
                    );
                    None
                }
            }
        } else {
            None
        };
        let compaction_msg = InboxMessage {
            compaction_complete: Some(CompactionComplete {
                sequence: new_sequence,
                blob_generation: state.blob_generation,
                cached_io: shared_io,
            }),
            ..Default::default()
        };
        if inbox_sender.try_send(compaction_msg).is_err() {
            warn!(
                "[StorageWorker] {}: Failed to send compaction_complete to database (inbox full)",
                db_id
            );
        }

        // WAL files are NOT deleted here — they accumulate on disk and are
        // cleaned up by an external process if desired (e.g. lark-compact)

        let blob_size = state.session.io().size().await.unwrap_or(0);
        let elapsed = compact_start.elapsed();

        info!(
            "[StorageWorker] {}: Compacted {} updates ({} WAL entries) from {} WAL file(s) in {:.1}s, sequence now {}, blob {:.1}KB",
            db_id,
            total_updates,
            total_wal_entries,
            completed_files.len(),
            elapsed.as_secs_f64(),
            new_sequence,
            blob_size as f64 / 1024.0,
        );

        Ok(())
    }
}

/// Check for `.compacting` marker and `blob.lark` existence, read `blob.generation`.
fn scan_db_dir(data_dir: &std::path::Path) -> (bool, Option<(u64, PathBuf)>) {
    let is_compacting = data_dir.join(".compacting").exists();
    let bp = blob_path(data_dir);
    if bp.exists() {
        let generation = read_blob_generation(data_dir);
        (is_compacting, Some((generation, bp)))
    } else {
        (is_compacting, None)
    }
}

/// Read the sequence file. Returns 0 if the file doesn't exist or can't be parsed.
async fn read_sequence_file(path: &Path) -> i64 {
    match fsync::read_file_async(path).await {
        Ok(bytes) => {
            let s = String::from_utf8_lossy(&bytes);
            s.trim().parse::<i64>().unwrap_or(0)
        }
        Err(_) => 0,
    }
}
