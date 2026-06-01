use super::*;

impl Database {
    /// Extract just the database portion from a combined "project/database" ID.
    fn extract_pure_database_id(id: &str) -> String {
        match id.find('/') {
            Some(idx) => id[idx + 1..].to_string(),
            None => id.to_string(),
        }
    }

    /// Create a new database.
    pub fn new(id: String, project_id: String, ephemeral: bool) -> Self {
        let (inbox_sender, inbox) = local_channel::new_bounded(INBOX_CHANNEL_SIZE);

        let tree = Arc::new(RwLock::new(Tree::new()));
        let pure_database_id = Self::extract_pure_database_id(&id);

        Self {
            id,
            project_id,
            core_id: 0, // Set via set_core_id() after creation
            pure_database_id,
            tree,
            ephemeral,
            inbox,
            inbox_sender: Rc::new(inbox_sender),
            clients: HashMap::new(),
            view_manager: ViewManager::new(),
            on_disconnect: HashMap::new(),
            state: DatabaseState::Loading,
            last_activity: Instant::now(),
            fatal_error: false,
            volatile_paths: Vec::new(),
            evaluator: None,
            data_dir: None,
            wal_writer: None,
            wal_dirty: false,
            wal_failed: false,
            wal_pending_entries: 0,
            wal_pending_bytes: 0,
            blob_session: None,
            blob_generation: 0,

            pending_wal_entries: Vec::new(),
            wal_index: WalIndex::new(),
            blob_sequence: 0,
            promoted_paths: HashMap::new(),
            sentinel_paths: BTreeSet::new(),
            processed_writes: HashMap::new(),
            nacked_writes: HashMap::new(),
            template_mode: false,
            pending_template_dir: None,
            pending_disk_load: false,
            needs_startup_compaction: false,
            compaction_tx: None,
            metrics: crate::metrics::DatabaseMetrics::new(),
            metrics_tx: None,
            promotion_stats: PromotionStats::new(),
            write_rate_limiter: WriteRateLimiter::new(),
        }
    }

    /// Create a new database with persistence.
    ///
    /// Note: The WAL writer is initialized asynchronously in `run()` after disk loading,
    /// since it requires async I/O to avoid blocking other databases on the core.
    pub fn new_with_persistence(id: String, project_id: String, data_dir: PathBuf) -> Self {
        let (inbox_sender, inbox) = local_channel::new_bounded(INBOX_CHANNEL_SIZE);

        let tree = Arc::new(RwLock::new(Tree::new()));
        let pure_database_id = Self::extract_pure_database_id(&id);

        Self {
            id,
            project_id,
            core_id: 0, // Set via set_core_id() after creation
            pure_database_id,
            tree,
            ephemeral: false,
            inbox,
            inbox_sender: Rc::new(inbox_sender),
            clients: HashMap::new(),
            view_manager: ViewManager::new(),
            on_disconnect: HashMap::new(),
            state: DatabaseState::Loading,
            last_activity: Instant::now(),
            fatal_error: false,
            volatile_paths: Vec::new(),
            evaluator: None,
            data_dir: Some(data_dir),
            wal_writer: None, // Initialized async in run() after disk loading
            wal_dirty: false,
            wal_failed: false,
            wal_pending_entries: 0,
            wal_pending_bytes: 0,
            blob_session: None, // Initialized in load_from_disk()
            blob_generation: 0, // Initialized in load_from_disk()
            pending_wal_entries: Vec::new(),
            wal_index: WalIndex::new(),
            blob_sequence: 0,
            promoted_paths: HashMap::new(),
            sentinel_paths: BTreeSet::new(),
            processed_writes: HashMap::new(),
            nacked_writes: HashMap::new(),
            template_mode: false,
            pending_template_dir: None,
            pending_disk_load: true, // Persistent databases need to load at start of run()
            needs_startup_compaction: false, // Set by load_wal_entries() if many WAL files
            compaction_tx: None,     // Set via set_compaction_tx() after creation
            metrics: crate::metrics::DatabaseMetrics::new(),
            metrics_tx: None, // Set via set_metrics_tx() after creation
            promotion_stats: PromotionStats::new(),
            write_rate_limiter: WriteRateLimiter::new(),
        }
    }

    /// Set the core ID this database is running on.
    pub fn set_core_id(&mut self, core_id: usize) {
        self.core_id = core_id;
    }

    /// Set the compaction channel sender for notifying the storage worker on WAL rotation.
    pub fn set_compaction_tx(&mut self, tx: Rc<LocalSender<StorageWorkerMessage>>) {
        self.compaction_tx = Some(tx);
    }

    /// Set the metrics sink: a non-blocking channel to the shipper thread that
    /// POSTs emitted metrics to the coordinator. Only set when `LARK_METRICS_PUSH`
    /// is enabled; otherwise metrics are stdout-only.
    pub fn set_metrics_tx(&mut self, tx: std::sync::mpsc::SyncSender<String>) {
        self.metrics_tx = Some(tx);
    }

    /// Set template mode - databases in template mode skip compaction/segmentation queues.
    pub fn set_template_mode(&mut self, template_mode: bool) {
        self.template_mode = template_mode;
    }

    /// Returns true if this database is backed by blob storage.
    pub fn is_blob_backed(&self) -> bool {
        self.blob_session.is_some()
    }

    /// Set the template directory for loading.
    /// If set, the database will load from this template at the start of run().
    pub fn set_pending_template_dir(&mut self, template_dir: PathBuf) {
        self.pending_template_dir = Some(template_dir);
    }
}
