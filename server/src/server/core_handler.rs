//! Per-core handler for the Glommio thread-per-core model.
//!
//! Each core runs its own CoreHandler that manages databases, config, and
//! client connections. No shared state between cores - databases are assigned
//! to cores via consistent hashing.

use crate::db::{AuthInfo as DbAuthInfo, ConnectionSender, Database, DatabaseHandle};
use crate::storage::StorageWorkerMessage;
use bytes::Bytes;
use glommio::channels::local_channel::LocalSender;

/// Maximum number of clients allowed per database.
/// Prevents a single database from monopolizing all server connections.
pub const MAX_CLIENTS_PER_DATABASE: usize = 200_000;
use crate::executor::core_for_database;
use crate::protocol::{ClientMessage, ServerMessage, error, op};
use crate::rules::{Evaluator, parse_rules};
use crate::transport::firebase_adapter::FIREBASE_MAX_FRAME_SIZE;
use crate::transport::protocol::ProjectConfig;
use crate::transport::proxy::{
    ConnectResult, ProxyAuthInfo, ProxyHandler, SendError, UnloadNotification, VirtualClient,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, trace, warn};

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for a per-core handler.
#[derive(Clone, Debug)]
pub struct CoreHandlerConfig {
    /// Core ID (0 to nr_cores-1)
    pub core_id: usize,

    /// Total number of cores
    pub nr_cores: usize,

    /// TCP listener port
    pub port: u16,

    /// Emulator mode (accepts "owner" token)
    pub emulator: bool,

    /// Data directory for persistence (None for ephemeral)
    pub data_dir: Option<String>,

    /// Template directory for load testing
    pub template_path: Option<String>,
}

/// Simplified project config for per-core use.
#[derive(Clone)]
pub struct LocalProjectConfig {
    /// Version from the most recent accepted CONFIG_PUSH. 0 for unversioned
    /// configs (legacy proxies). Used to reject stale/duplicate pushes.
    pub config_version: u64,
    pub rules: Option<Arc<Evaluator>>,
    pub secret_key: Option<String>,
    pub admin_secret_key: Option<String>,
    pub ephemeral: bool,
    /// Whether this project may run databases. When `false`, the server refuses
    /// to start its databases and evicts running ones. Defaults to `true`, so a
    /// missing or unversioned config does not disable a project.
    pub enabled: bool,
}

impl Default for LocalProjectConfig {
    fn default() -> Self {
        Self {
            config_version: 0,
            rules: None,
            secret_key: None,
            admin_secret_key: None,
            ephemeral: false,
            enabled: true,
        }
    }
}

// =============================================================================
// Per-Core Handler
// =============================================================================

/// Per-core handler that manages databases and client connections.
pub struct CoreHandler {
    /// Configuration
    config: CoreHandlerConfig,

    /// Active databases on this core: database_id -> DatabaseHandle
    databases: RefCell<HashMap<String, DatabaseHandle>>,

    /// Cached project configs: project_id -> config
    project_configs: RefCell<HashMap<String, LocalProjectConfig>>,

    /// Pending config requests: project_id -> list of buffered connects
    pending_configs: RefCell<HashMap<String, Vec<PendingConnect>>>,

    /// Buffered messages for clients pending config: client_id -> messages
    pending_client_messages: RefCell<HashMap<String, Vec<BufferedMessage>>>,

    /// Latest auth for clients still pending config: client_id -> auth.
    /// An AUTH_CHANGED can arrive before the client's CONNECT is processed
    /// (while it's buffered waiting on project config). Dropping it would leave
    /// the client on its original CONNECT-time auth — a privilege-retention bug
    /// if the change was a downgrade. We stash the newest auth here and
    /// `process_connect` applies it when the client finally connects.
    pending_client_auth: RefCell<HashMap<String, ProxyAuthInfo>>,

    /// Client to database mapping: client_id -> (database_id, virtual client).
    /// We keep the `Rc<VirtualClient>` so explicit eviction can force-close
    /// in-flight clients for a database.
    client_databases: RefCell<HashMap<String, (String, Rc<VirtualClient>)>>,

    /// Databases that were explicitly evicted on this core. The spawned
    /// database task consults this on shutdown to stamp the correct unload
    /// reason on its notification.
    evicted_databases: RefCell<std::collections::HashSet<String>>,

    /// Whether shutdown has been requested
    shutting_down: RefCell<bool>,

    /// Pending database unload notifications to send to proxy
    pending_unloads: RefCell<Vec<UnloadNotification>>,

    /// Compaction channel sender for notifying the per-core storage worker
    compaction_tx: Rc<LocalSender<StorageWorkerMessage>>,

    /// Optional metrics sink, injected into each database it creates. `None`
    /// unless direct metrics push (`LARK_METRICS_PUSH`) is enabled.
    metrics_tx: Option<std::sync::mpsc::SyncSender<String>>,
}

/// A buffered CONNECT waiting for project config.
struct PendingConnect {
    client: Rc<VirtualClient>,
    #[allow(dead_code)] // Reserved for future timeout tracking
    received_at: Instant,
}

/// A buffered message waiting for the client to finish connecting.
struct BufferedMessage {
    data: Vec<u8>,
    timestamps: Option<crate::metrics::MessageTimestamps>,
}

impl CoreHandler {
    /// Create a new per-core handler.
    pub fn new(
        config: CoreHandlerConfig,
        compaction_tx: Rc<LocalSender<StorageWorkerMessage>>,
        metrics_tx: Option<std::sync::mpsc::SyncSender<String>>,
    ) -> Rc<Self> {
        Rc::new(Self {
            config,
            databases: RefCell::new(HashMap::new()),
            project_configs: RefCell::new(HashMap::new()),
            pending_configs: RefCell::new(HashMap::new()),
            pending_client_messages: RefCell::new(HashMap::new()),
            pending_client_auth: RefCell::new(HashMap::new()),
            client_databases: RefCell::new(HashMap::new()),
            evicted_databases: RefCell::new(std::collections::HashSet::new()),
            shutting_down: RefCell::new(false),
            pending_unloads: RefCell::new(Vec::new()),
            compaction_tx,
            metrics_tx,
        })
    }

    /// Get or create a database.
    /// Returns (handle, is_new) where is_new is true if the database was newly created.
    pub fn get_or_create_database(self: &Rc<Self>, database_id: &str) -> (DatabaseHandle, bool) {
        // Check if already exists
        if let Some(handle) = self.databases.borrow().get(database_id) {
            return (handle.clone(), false);
        }

        // Check if this database belongs on this core
        let target_core = core_for_database(database_id, self.config.nr_cores);
        if target_core != self.config.core_id {
            // This can happen if proxy uses different hashing - not a problem
            debug!(
                "Database {} hashes to core {} but requested on core {}",
                database_id, target_core, self.config.core_id
            );
        }

        // Extract project ID
        let project_id = parse_project_id(database_id);

        // Get project config (may be empty)
        let project_config = self
            .project_configs
            .borrow()
            .get(&project_id)
            .cloned()
            .unwrap_or_default();

        // Determine persistence
        let is_persistent = self.config.data_dir.is_some() && !project_config.ephemeral;
        let use_template = project_id == "loadtest" && self.config.template_path.is_some();

        // Create database
        let mut db = if is_persistent {
            if let Some(ref data_dir) = self.config.data_dir {
                let db_data_dir = PathBuf::from(format!("{}/{}", data_dir, database_id));
                Database::new_with_persistence(
                    database_id.to_string(),
                    project_id.clone(),
                    db_data_dir,
                )
            } else {
                Database::new(database_id.to_string(), project_id.clone(), true)
            }
        } else {
            Database::new(database_id.to_string(), project_id.clone(), true)
        };

        // Set template mode if applicable
        if use_template {
            db.set_template_mode(true);
        }

        // Set rules if available
        if let Some(ref evaluator) = project_config.rules {
            db.set_evaluator((**evaluator).clone());
        }

        // Set up template directory if using template mode.
        // Actual loading happens at the start of run() after acquiring the segmentation lock.
        if is_persistent && use_template {
            if let Some(ref template_path) = self.config.template_path {
                let template_dir = PathBuf::from(template_path);
                trace!(
                    "Database {} will load from template {:?} at startup",
                    database_id, template_dir
                );
                db.set_pending_template_dir(template_dir);
            } else {
                warn!(
                    "Template mode requested but no template_path configured for {}",
                    database_id
                );
            }
        }

        // Set core ID for metrics emission
        db.set_core_id(self.config.core_id);

        // Set compaction channel for WAL rotation notifications
        db.set_compaction_tx(self.compaction_tx.clone());

        // Wire up direct metrics push if enabled (LARK_METRICS_PUSH)
        if let Some(tx) = &self.metrics_tx {
            db.set_metrics_tx(tx.clone());
        }

        // Get handle before spawning
        let handle = db.handle();

        // Store in registry
        self.databases
            .borrow_mut()
            .insert(database_id.to_string(), handle.clone());

        // Spawn database task
        let database_id_owned = database_id.to_string();
        let project_id_owned = project_id.clone();
        let db_id_only = if let Some(idx) = database_id.find('/') {
            database_id[idx + 1..].to_string()
        } else {
            database_id.to_string()
        };
        let is_ephemeral = !is_persistent;
        let skip_segmentation = use_template; // Template databases don't need segmentation
        let data_dir_for_marker = self.config.data_dir.clone();
        let self_clone = self.clone();

        glommio::spawn_local(async move {
            db.run().await;
            debug!("Database {} stopped", database_id_owned);

            // Remove from registry (handle_evict_database may have already done
            // this; remove is idempotent).
            self_clone.databases.borrow_mut().remove(&database_id_owned);

            // If handle_evict_database flagged this db as explicitly evicted,
            // stamp the unload reason accordingly. Remove from the set so a
            // later re-create doesn't inherit the flag.
            use crate::transport::protocol::unload_reason;
            let was_evicted = self_clone
                .evicted_databases
                .borrow_mut()
                .remove(&database_id_owned);
            let reason = if was_evicted {
                unload_reason::EXPLICIT_EVICTION
            } else {
                unload_reason::IDLE
            };

            self_clone
                .pending_unloads
                .borrow_mut()
                .push(UnloadNotification {
                    project_id: project_id_owned.clone(),
                    database_id: db_id_only,
                    reason,
                    ephemeral: is_ephemeral,
                });
            debug!(
                "Queued DATABASE_UNLOADED notification for {} (reason={})",
                database_id_owned,
                if was_evicted {
                    "EXPLICIT_EVICTION"
                } else {
                    "IDLE"
                }
            );

            // Note: We no longer create segmentation markers on eviction.
            // On next startup, the database will lazy-load from NAS (manifest + hot.json)
            // and replay the small WAL file (< rotation limit).
            let _ = (is_ephemeral, skip_segmentation, data_dir_for_marker); // Suppress unused warnings
        })
        .detach();

        debug!(
            "Created database {} on core {} (persistent={})",
            database_id, self.config.core_id, is_persistent
        );

        (handle, true)
    }

    /// Handle a config push from the proxy.
    pub fn handle_config_push(self: &Rc<Self>, project_id: &str, config: ProjectConfig) {
        // Reject stale/duplicate pushes. Once we've accepted a versioned config,
        // only strictly-newer versions are accepted. This protects against
        // multi-proxy fan-out duplication and out-of-order delivery. If we've
        // never seen a versioned config (cached.config_version == 0), we accept
        // any push for backwards compat with unversioned proxies.
        if let Some(cached) = self.project_configs.borrow().get(project_id)
            && cached.config_version > 0
            && config.config_version <= cached.config_version
        {
            trace!(
                "Skipping CONFIG_PUSH for {}: incoming version {} <= cached {}",
                project_id, config.config_version, cached.config_version
            );
            return;
        }

        // Parse rules if present (using json5 forcomments and trailing commas)
        let evaluator = if let Some(rules_json) = config.rules.as_ref() {
            let trimmed = rules_json.trim();
            if trimmed.is_empty() {
                None // Empty rules string = intentionally no rules
            } else {
                match json5::from_str::<serde_json::Value>(trimmed) {
                    Ok(rules_value) => match parse_rules(&rules_value) {
                        Ok(ruleset) => Some(Arc::new(Evaluator::new(ruleset))),
                        Err(e) => {
                            error!(
                                "Failed to parse rules for project {}: {}. \
                                 Config will NOT be stored and pending clients will NOT be connected. \
                                 Fix the rules and re-push config.",
                                project_id, e
                            );
                            // Don't store config, don't process pending connects
                            return;
                        }
                    },
                    Err(e) => {
                        error!(
                            "Failed to parse rules JSON for project {}: {}. \
                             Config will NOT be stored and pending clients will NOT be connected. \
                             Fix the rules and re-push config.",
                            project_id, e
                        );
                        // Don't store config, don't process pending connects
                        return;
                    }
                }
            }
        } else {
            None // No rules configured = intentionally open
        };

        // Absent `enabled` (older proxies) is treated as enabled.
        let enabled = config.enabled.unwrap_or(true);
        let cached_config = LocalProjectConfig {
            config_version: config.config_version,
            rules: evaluator,
            secret_key: config.secret_key.clone(),
            admin_secret_key: config.admin_secret_key.clone(),
            ephemeral: config.ephemeral.unwrap_or(false),
            enabled,
        };

        let has_rules = cached_config.rules.is_some();
        let ephemeral = cached_config.ephemeral;
        let new_evaluator = cached_config.rules.clone();

        // Store config
        self.project_configs
            .borrow_mut()
            .insert(project_id.to_string(), cached_config);

        // Push the new rules to any already-running databases for this project.
        // Newly-created databases will pick up the cached config at creation
        // time; this path handles hot-reload for the existing ones.
        let affected_dbs: Vec<(String, DatabaseHandle)> = self
            .databases
            .borrow()
            .iter()
            .filter(|(full_id, _)| parse_project_id(full_id) == project_id)
            .map(|(id, handle)| (id.clone(), handle.clone()))
            .collect();

        if enabled {
            for (db_id, handle) in &affected_dbs {
                let eval = new_evaluator.as_deref().cloned();
                handle.update_evaluator(eval);
                debug!(
                    "  CONFIG_PUSH: pushed new rules to running database {}",
                    db_id
                );
            }
        } else {
            // Project disabled: shut down any running databases for it without
            // purging on-disk data, and NACK their clients. Newly-arriving and
            // pending connects are refused by the startup gate in
            // process_connect.
            for (full_id, _) in &affected_dbs {
                self.shutdown_disabled_database(full_id);
            }
            if !affected_dbs.is_empty() {
                debug!(
                    "  CONFIG_PUSH: project {} disabled, evicted {} running database(s)",
                    project_id,
                    affected_dbs.len()
                );
            }
        }

        debug!(
            "CONFIG_PUSH stored: project={} version={} enabled={} rules={} ephemeral={} core={} affected_dbs={}",
            project_id,
            config.config_version,
            enabled,
            has_rules,
            ephemeral,
            self.config.core_id,
            affected_dbs.len()
        );

        // Process any pending connects for this project
        if let Some(pending) = self.pending_configs.borrow_mut().remove(project_id) {
            trace!(
                "Processing {} pending connects for project {}",
                pending.len(),
                project_id
            );
            for pc in pending {
                let client_id = pc.client.id.clone();

                // Connect the client
                self.process_connect(pc.client.clone());

                // Replay any buffered messages for this client
                if let Some(buffered) = self.pending_client_messages.borrow_mut().remove(&client_id)
                    && !buffered.is_empty()
                {
                    trace!(
                        "Replaying {} buffered messages for client {}",
                        buffered.len(),
                        client_id
                    );
                    for msg in buffered {
                        self.replay_message(pc.client.clone(), msg.data, msg.timestamps);
                    }
                }
            }
        }
    }

    /// Handle a database eviction request from the proxy.
    ///
    /// Evicting works by dropping the `DatabaseHandle` from the registry —
    /// that releases the last external `Rc<inbox_sender>`, causing the
    /// Database task to exit its main loop on the next iteration. In-flight
    /// virtual clients are force-closed so they reconnect and get re-routed.
    ///
    /// If `PURGE_DATA` is set, the persisted data directory is renamed
    /// synchronously to `{dir}-deleted-{unix_ts}` so the data is easy to
    /// recover if the delete was accidental. Space reclamation is deferred
    /// to manual cleanup. The rename happens while the database task may
    /// still be winding down; open file descriptors stay valid across the
    /// rename (Linux), so the task's final WAL sync / close still succeeds.
    ///
    /// Idempotent: evict-already-evicted is a no-op. Safe to call when the
    /// database was never loaded on this core (still triggers the PURGE
    /// rename so offline data gets marked deleted).
    pub fn handle_evict_database(&self, project_id: &str, database_id: &str, flags: u8) {
        use crate::transport::protocol::evict_flag;

        let full_id = if project_id.is_empty() {
            database_id.to_string()
        } else {
            format!("{}/{}", project_id, database_id)
        };

        let purge = flags & evict_flag::PURGE_DATA != 0;

        debug!(
            "EVICT_DATABASE handling {} on core {} (flags=0x{:02x}, purge={})",
            full_id, self.config.core_id, flags, purge
        );

        // Drop the handle. Once our ref is gone the Database's inbox_sender
        // Rc count reaches 1 and run() exits at its next loop iteration. Also
        // mark the DB as explicitly evicted so the spawned task's cleanup
        // reports EXPLICIT_EVICTION in the UnloadNotification.
        let was_loaded = self.databases.borrow_mut().remove(&full_id).is_some();
        if was_loaded {
            self.evicted_databases.borrow_mut().insert(full_id.clone());
            debug!(
                "  dropped DatabaseHandle for {} - DB task will exit on next loop iteration",
                full_id
            );
        } else {
            debug!(
                "  {} was not loaded on this core (nothing to drop)",
                full_id
            );
        }

        // Force-close any in-flight virtual clients attached to this DB.
        // Collect Rc<VirtualClient>s first to avoid borrowing client_databases
        // across client.close() (which only touches the outbox, but keep it
        // clean).
        let mut to_close = Vec::new();
        self.client_databases
            .borrow_mut()
            .retain(|_, (db_id, client)| {
                if db_id == &full_id {
                    to_close.push(client.clone());
                    false
                } else {
                    true
                }
            });
        for client in &to_close {
            client.close();
        }
        if !to_close.is_empty() {
            debug!(
                "  force-closed {} connected virtual clients for {}",
                to_close.len(),
                full_id
            );
        }

        // Close any clients still waiting on config for this database.
        let mut pending_to_remove: Vec<String> = Vec::new();
        {
            let mut pending = self.pending_configs.borrow_mut();
            pending.retain(|_, list| {
                list.retain(|pc| {
                    let matches_db =
                        pc.client.database_id == database_id && pc.client.project_id == project_id;
                    if matches_db {
                        pc.client.close();
                        pending_to_remove.push(pc.client.id.clone());
                    }
                    !matches_db
                });
                !list.is_empty()
            });
        }
        if !pending_to_remove.is_empty() {
            let mut buffers = self.pending_client_messages.borrow_mut();
            let mut deferred_auth = self.pending_client_auth.borrow_mut();
            for id in &pending_to_remove {
                buffers.remove(id);
                deferred_auth.remove(id);
            }
            debug!(
                "  closed {} pending-config clients waiting on {}",
                pending_to_remove.len(),
                full_id
            );
        }

        // Rename the on-disk data directory if requested.
        if purge {
            if let Some(ref data_dir) = self.config.data_dir {
                let db_data_dir = PathBuf::from(format!("{}/{}", data_dir, full_id));
                if !is_safe_database_id(&full_id) {
                    error!(
                        "Refusing PURGE_DATA rename for unsafe database id {:?}",
                        full_id
                    );
                } else if db_data_dir.exists() {
                    let ts = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0);
                    let renamed = PathBuf::from(format!("{}/{}-deleted-{}", data_dir, full_id, ts));
                    debug!(
                        "  PURGE_DATA set, renaming data dir {} -> {}",
                        db_data_dir.display(),
                        renamed.display()
                    );
                    match std::fs::rename(&db_data_dir, &renamed) {
                        Ok(_) => info!(
                            "Purged database {} (renamed {} -> {})",
                            full_id,
                            db_data_dir.display(),
                            renamed.display()
                        ),
                        Err(e) => error!(
                            "Failed to rename {} for purge: {}",
                            db_data_dir.display(),
                            e
                        ),
                    }
                } else {
                    debug!(
                        "  PURGE_DATA set but data dir {} does not exist, nothing to rename",
                        db_data_dir.display()
                    );
                }
            } else {
                debug!("  PURGE_DATA set but no data_dir configured (ephemeral server)");
            }
        }

        info!(
            "Evicted database {} on core {} (loaded={}, clients_closed={}, purge={})",
            full_id,
            self.config.core_id,
            was_loaded,
            to_close.len(),
            purge,
        );
    }

    /// Shut down a running database whose project is disabled.
    ///
    /// Drops the handle so the db task exits, marks it explicitly evicted, and
    /// force-closes attached clients after sending each a `PROJECT_DISABLED`
    /// NACK. Does not purge on-disk data. `full_id` is the `project/database`
    /// key.
    fn shutdown_disabled_database(&self, full_id: &str) {
        let was_loaded = self.databases.borrow_mut().remove(full_id).is_some();
        if was_loaded {
            self.evicted_databases
                .borrow_mut()
                .insert(full_id.to_string());
        }

        let mut to_close = Vec::new();
        self.client_databases
            .borrow_mut()
            .retain(|_, (db_id, client)| {
                if db_id == full_id {
                    to_close.push(client.clone());
                    false
                } else {
                    true
                }
            });
        for client in &to_close {
            if let Ok(data) =
                ServerMessage::nack("", error::PROJECT_DISABLED, "project is disabled").encode()
            {
                let _ = client.try_send(data.into(), false);
            }
            client.close();
        }
        debug!(
            "  shutdown disabled database {} on core {} (loaded={}, clients_closed={})",
            full_id,
            self.config.core_id,
            was_loaded,
            to_close.len()
        );
    }

    /// Handle shutdown request.
    pub fn handle_shutdown(&self, _grace_period_secs: u32) {
        *self.shutting_down.borrow_mut() = true;
        info!("Shutdown requested on core {}", self.config.core_id);
        // Database shutdown will happen via normal idle timeout
    }

    /// Process a client connect after config is available.
    /// Returns (project_id, database_id) if a new database was created.
    fn process_connect(self: &Rc<Self>, client: Rc<VirtualClient>) -> Option<(String, String)> {
        let database_id = if client.project_id.is_empty() {
            client.database_id.clone()
        } else {
            format!("{}/{}", client.project_id, client.database_id)
        };

        // Reject identifiers that could escape the data directory. The edge
        // validates these, but the server must not depend on that — a crafted
        // id like `../../other` would otherwise resolve a data dir in another
        // tenant's tree (defense in depth; also covers any future transport).
        if !is_safe_database_id(&database_id) {
            warn!(
                "Rejecting client {} - unsafe database id {:?}",
                client.id, database_id
            );
            if let Ok(data) =
                ServerMessage::nack("", error::INVALID_DATA, "invalid database id").encode()
            {
                let _ = client.try_send(data.into(), false);
            }
            client.close();
            return None;
        }

        // Check per-database connection limit
        let current_count = self.client_count_for_database(&database_id);
        if current_count >= MAX_CLIENTS_PER_DATABASE {
            warn!(
                "Rejecting client {} - database {} at connection limit ({}/{})",
                client.id, database_id, current_count, MAX_CLIENTS_PER_DATABASE
            );
            // Tell the client *why* before dropping it, rather than a bare close.
            // The NACK is enqueued while the connection is still open; close()
            // then enqueues the CLOSE behind it, so the writer drains the reason
            // first. (No request_id — this is a connect-time rejection.)
            if let Ok(data) = ServerMessage::nack(
                "",
                error::TOO_MANY_CONNECTIONS,
                "database is at its connection limit",
            )
            .encode()
            {
                let _ = client.try_send(data.into(), false);
            }
            client.close();
            return None;
        }

        // Refuse to start or join a database whose project is disabled. Only
        // blocks when a config is cached and explicitly disabled; clients with
        // no cached config (emulator/local) are allowed. This is the single
        // funnel for both immediate and pending-config-drained connects.
        let project_id = parse_project_id(&database_id);
        let project_disabled = self
            .project_configs
            .borrow()
            .get(&project_id)
            .map(|cfg| !cfg.enabled)
            .unwrap_or(false);
        if project_disabled {
            warn!(
                "Rejecting client {} - project {} is disabled",
                client.id, project_id
            );
            if let Ok(data) =
                ServerMessage::nack("", error::PROJECT_DISABLED, "project is disabled").encode()
            {
                let _ = client.try_send(data.into(), false);
            }
            client.close();
            return None;
        }

        // Get or create database
        let (db_handle, is_new) = self.get_or_create_database(&database_id);

        // Convert proxy auth to db auth
        // In emulator mode, all clients get admin access
        let auth = if self.config.emulator {
            Some(DbAuthInfo {
                uid: "emulator".to_string(),
                provider: "emulator".to_string(),
                token: HashMap::new(),
                is_admin: true,
            })
        } else if let Some(deferred) = self.pending_client_auth.borrow_mut().remove(&client.id) {
            // A late AUTH_CHANGED arrived while this client was buffered pending
            // config. Prefer that newer auth over the CONNECT-time auth so a
            // mid-handshake sign-in/out isn't silently lost.
            convert_auth(&deferred)
        } else {
            client.proxy_auth.as_ref().and_then(convert_auth)
        };

        // Log auth info
        if let Some(ref a) = auth {
            debug!(
                "Client {} auth: uid={}, provider={}, is_admin={}",
                client.id, a.uid, a.provider, a.is_admin
            );
        } else {
            debug!("Client {} auth: anonymous", client.id);
        }

        // Generate connection ID
        let connection_id = crate::db::generate_push_id();

        // Create sender adapter. Arc (not Rc) is required by the `add_client`
        // API even though this runs in a single-threaded glommio executor.
        #[allow(clippy::arc_with_non_send_sync)]
        let sender: Arc<dyn ConnectionSender> = Arc::new(VirtualClientSender::new(client.clone()));

        // Add client to database
        db_handle.add_client(client.id.clone(), auth, connection_id, sender);

        // Track client -> database mapping
        self.client_databases
            .borrow_mut()
            .insert(client.id.clone(), (database_id.clone(), client.clone()));

        debug!(
            "Client {} connected to database {} on core {}",
            client.id, database_id, self.config.core_id
        );

        // Return database info if newly created
        if is_new {
            Some((client.project_id.clone(), client.database_id.clone()))
        } else {
            None
        }
    }

    /// Replay a buffered message for a client that is now connected.
    fn replay_message(
        self: &Rc<Self>,
        client: Rc<VirtualClient>,
        data: Vec<u8>,
        timestamps: Option<crate::metrics::MessageTimestamps>,
    ) {
        // Get the database ID (client should be in client_databases now)
        let database_id = match self.client_databases.borrow().get(&client.id) {
            Some((id, _)) => id.clone(),
            None => {
                warn!(
                    "Cannot replay message - client {} not in database mapping",
                    client.id
                );
                return;
            }
        };

        let db_handle = match self.databases.borrow().get(&database_id) {
            Some(h) => h.clone(),
            None => {
                warn!(
                    "Cannot replay message - database {} not found for client {}",
                    database_id, client.id
                );
                return;
            }
        };

        // For Firebase clients, use adapter for translation
        if let Some(adapter_cell) = client.firebase_adapter() {
            let mut adapter = adapter_cell.borrow_mut();

            match adapter.handle_incoming_frame(&data) {
                Ok((Some(message), response)) => {
                    if let Some(resp) = response {
                        let _ = client.try_send(resp.into(), false);
                    }

                    // Skip session-level messages
                    match message.op.as_str() {
                        op::JOIN | op::AUTH | op::UNAUTH | op::LEAVE | op::PONG => {
                            return;
                        }
                        _ => {}
                    }

                    db_handle.send_message_with_timestamps(client.id.clone(), message, timestamps);
                }
                Ok((None, response)) => {
                    if let Some(resp) = response {
                        let _ = client.try_send(resp.into(), false);
                    }
                }
                Err(e) => {
                    warn!(
                        "Firebase frame error replaying message for {}: {}",
                        client.id, e
                    );
                }
            }
        } else {
            // Normal client
            let is_rest = client.protocol == crate::transport::protocol_id::REST;
            let message = match ClientMessage::parse(&data, is_rest) {
                Ok(msg) => msg,
                Err(e) => {
                    warn!("Failed to parse replayed message from {}: {}", client.id, e);
                    return;
                }
            };

            match message.op.as_str() {
                op::JOIN | op::AUTH | op::UNAUTH | op::LEAVE | op::PONG => {
                    return;
                }
                _ => {}
            }

            db_handle.send_message_with_timestamps(client.id.clone(), message, timestamps);
        }
    }

    /// Get database count on this core.
    pub fn database_count(&self) -> usize {
        self.databases.borrow().len()
    }

    /// Get client count on this core.
    pub fn client_count(&self) -> usize {
        self.client_databases.borrow().len()
    }

    /// Check if a project config exists.
    pub fn has_project_config(&self, project_id: &str) -> bool {
        self.project_configs.borrow().contains_key(project_id)
    }

    /// Count connected clients for a specific database.
    fn client_count_for_database(&self, database_id: &str) -> usize {
        self.client_databases
            .borrow()
            .values()
            .filter(|(db_id, _)| db_id == database_id)
            .count()
    }
}

// =============================================================================
// ProxyHandler Implementation
// =============================================================================

impl ProxyHandler for CoreHandler {
    fn on_connect(self: &Rc<Self>, client: Rc<VirtualClient>) -> ConnectResult {
        trace!(
            "Client connected: {} (project={}, db={}, firebase={})",
            client.id,
            client.project_id,
            client.database_id,
            client.is_firebase()
        );

        // Clone project_id to avoid borrow issues
        let project_id = client.project_id.clone();

        // In emulator mode or if config is cached, process immediately
        if self.config.emulator || project_id.is_empty() || self.has_project_config(&project_id) {
            let database_loaded = self.process_connect(client);
            ConnectResult {
                needs_config: false,
                database_loaded,
            }
        } else {
            // Need to wait for config - buffer the connect
            trace!("Buffering connect for {} pending config", project_id);

            // Track this client as pending (for message buffering)
            self.pending_client_messages
                .borrow_mut()
                .insert(client.id.clone(), Vec::new());

            let pending = PendingConnect {
                client,
                received_at: Instant::now(),
            };

            self.pending_configs
                .borrow_mut()
                .entry(project_id)
                .or_default()
                .push(pending);

            ConnectResult {
                needs_config: true,
                database_loaded: None,
            }
        }
    }

    fn on_message(
        &self,
        client: Rc<VirtualClient>,
        data: Vec<u8>,
        mut timestamps: Option<crate::metrics::MessageTimestamps>,
    ) {
        // Stamp handler receive time
        if let Some(ref mut ts) = timestamps {
            ts.stamp_handler_receive();
        }

        // Get the database handle
        let database_id = match self.client_databases.borrow().get(&client.id) {
            Some((id, _)) => id.clone(),
            None => {
                // Check if client is pending config - if so, buffer the message
                let mut pending_messages = self.pending_client_messages.borrow_mut();
                if let Some(buffer) = pending_messages.get_mut(&client.id) {
                    trace!(
                        "Buffering message for pending client {} (buffer size: {})",
                        client.id,
                        buffer.len() + 1
                    );
                    buffer.push(BufferedMessage { data, timestamps });
                    return;
                }

                warn!("Message from unknown client: {}", client.id);
                return;
            }
        };

        let db_handle = match self.databases.borrow().get(&database_id) {
            Some(h) => h.clone(),
            None => {
                warn!("Database not found for client: {}", client.id);
                return;
            }
        };

        // For Firebase clients, use adapter for translation
        if let Some(adapter_cell) = client.firebase_adapter() {
            let mut adapter = adapter_cell.borrow_mut();

            match adapter.handle_incoming_frame(&data) {
                Ok((Some(message), response)) => {
                    // Send any immediate response
                    if let Some(resp) = response {
                        let _ = client.try_send(resp.into(), false);
                    }

                    // Intercept session-level messages
                    match message.op.as_str() {
                        op::JOIN | op::AUTH | op::UNAUTH | op::LEAVE | op::PONG => {
                            // These are handled at handler level, not sent to database
                            return;
                        }
                        _ => {}
                    }

                    // Route to database
                    db_handle.send_message_with_timestamps(client.id.clone(), message, timestamps);
                }
                Ok((None, response)) => {
                    if let Some(resp) = response {
                        let _ = client.try_send(resp.into(), false);
                    }
                }
                Err(e) => {
                    warn!("Firebase frame error for {}: {}", client.id, e);
                }
            }
        } else {
            // Normal client
            let is_rest = client.protocol == crate::transport::protocol_id::REST;
            let message = match ClientMessage::parse(&data, is_rest) {
                Ok(msg) => msg,
                Err(e) => {
                    trace!("Failed to parse message from {}: {}", client.id, e);
                    return;
                }
            };

            // Intercept session-level messages
            match message.op.as_str() {
                op::JOIN | op::AUTH | op::UNAUTH | op::LEAVE | op::PONG => {
                    return;
                }
                _ => {}
            }

            db_handle.send_message_with_timestamps(client.id.clone(), message, timestamps);
        }
    }

    fn on_disconnect(&self, client: Rc<VirtualClient>) {
        debug!("Client disconnected: {}", client.id);

        // Clean up any pending message buffer + deferred auth
        self.pending_client_messages.borrow_mut().remove(&client.id);
        self.pending_client_auth.borrow_mut().remove(&client.id);

        // Clean up from pending configs if client was waiting for config
        let project_id = client.project_id.clone();
        if !project_id.is_empty() {
            let mut pending = self.pending_configs.borrow_mut();
            if let Some(pending_list) = pending.get_mut(&project_id) {
                pending_list.retain(|pc| pc.client.id != client.id);
                // If no more pending connects for this project, remove the entry
                if pending_list.is_empty() {
                    pending.remove(&project_id);
                }
            }
        }

        // Get and remove database mapping
        let database_id = self
            .client_databases
            .borrow_mut()
            .remove(&client.id)
            .map(|(db_id, _)| db_id);

        if let Some(database_id) = database_id
            && let Some(db_handle) = self.databases.borrow().get(&database_id)
        {
            db_handle.client_disconnected(client.id.clone());
        }
    }

    fn on_auth_changed(&self, client: Rc<VirtualClient>, auth: ProxyAuthInfo) {
        info!(
            "Client auth changed: {} (uid={}, provider={}, is_admin={})",
            client.id, auth.uid, auth.provider, auth.is_admin
        );

        let database_id = match self.client_databases.borrow().get(&client.id) {
            Some((id, _)) => id.clone(),
            None => {
                // Client may be pending config (CONNECT buffered, waiting on
                // project config). Stash the newest auth so process_connect
                // applies it on connect rather than dropping it (which would
                // leave the client on its original CONNECT-time auth).
                if self
                    .pending_client_messages
                    .borrow()
                    .contains_key(&client.id)
                {
                    debug!(
                        "AUTH_CHANGED for pending client {} — deferring until connect",
                        client.id
                    );
                    self.pending_client_auth
                        .borrow_mut()
                        .insert(client.id.clone(), auth);
                } else {
                    warn!("AUTH_CHANGED for unknown client {}", client.id);
                }
                return;
            }
        };

        if let Some(db_handle) = self.databases.borrow().get(&database_id) {
            let db_auth = convert_auth(&auth);
            debug!(
                "Updating auth for client {} in database {} (uid={:?})",
                client.id,
                database_id,
                db_auth.as_ref().map(|a| &a.uid)
            );
            db_handle.update_client_auth(client.id.clone(), db_auth);
        }
    }

    fn on_config_push(
        self: &Rc<Self>,
        project_id: &str,
        config: crate::transport::protocol::ProjectConfig,
    ) {
        self.handle_config_push(project_id, config);
    }

    fn on_evict_database(&self, project_id: &str, database_id: &str, flags: u8) {
        self.handle_evict_database(project_id, database_id, flags);
    }

    fn take_pending_unloads(&self) -> Vec<UnloadNotification> {
        std::mem::take(&mut *self.pending_unloads.borrow_mut())
    }
}

// =============================================================================
// Helper Types
// =============================================================================

/// Adapter that makes VirtualClient implement ConnectionSender.
pub struct VirtualClientSender {
    client: Rc<VirtualClient>,
}

impl VirtualClientSender {
    pub fn new(client: Rc<VirtualClient>) -> Self {
        Self { client }
    }
}

impl ConnectionSender for VirtualClientSender {
    fn send(
        &self,
        data: Bytes,
        volatile: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), crate::db::SendError>> + '_>>
    {
        // For Glommio, we use try_send since we're single-threaded
        let result = self.try_send(data, volatile, false);
        Box::pin(async move { result })
    }

    fn try_send(
        &self,
        data: Bytes,
        volatile: bool,
        skip_translation: bool,
    ) -> Result<(), crate::db::SendError> {
        // For Firebase clients, translate outgoing messages
        if let Some(adapter_cell) = self.client.firebase_adapter() {
            // Fast path: if skip_translation=true and no chunking needed, send directly
            // This avoids the data.to_vec() copy in translate_outgoing_chunked
            if skip_translation && data.len() <= FIREBASE_MAX_FRAME_SIZE {
                return self.client.try_send(data, volatile).map_err(|e| match e {
                    SendError::Closed => crate::db::SendError::Closed,
                    SendError::ChannelClosed => crate::db::SendError::Closed,
                    SendError::BufferFull => crate::db::SendError::BufferFull,
                });
            }

            let adapter = adapter_cell.borrow();

            // Use chunked translation to handle large messages
            // If skip_translation is true, data is already in Firebase format
            match adapter.translate_outgoing_chunked(&data, skip_translation) {
                Ok(Some(chunks)) => {
                    // Send all chunks
                    for chunk in chunks {
                        self.client
                            .try_send(chunk.into(), volatile)
                            .map_err(|e| match e {
                                SendError::Closed => crate::db::SendError::Closed,
                                SendError::ChannelClosed => crate::db::SendError::Closed,
                                SendError::BufferFull => crate::db::SendError::BufferFull,
                            })?;
                    }
                    Ok(())
                }
                Ok(None) => {
                    // Message swallowed (e.g., JoinAck)
                    Ok(())
                }
                Err(e) => {
                    warn!("Firebase translate_outgoing error: {}", e);
                    // Send raw data as fallback (shouldn't normally happen)
                    self.client.try_send(data, volatile).map_err(|e| match e {
                        SendError::Closed => crate::db::SendError::Closed,
                        SendError::ChannelClosed => crate::db::SendError::Closed,
                        SendError::BufferFull => crate::db::SendError::BufferFull,
                    })
                }
            }
        } else {
            // Normal client - send raw data
            self.client.try_send(data, volatile).map_err(|e| match e {
                SendError::Closed => crate::db::SendError::Closed,
                SendError::ChannelClosed => crate::db::SendError::Closed,
                SendError::BufferFull => crate::db::SendError::BufferFull,
            })
        }
    }

    fn is_firebase(&self) -> bool {
        self.client.firebase_adapter().is_some()
    }

    fn outbox_id(&self) -> usize {
        self.client.outbox_id()
    }

    fn client_id(&self) -> u32 {
        self.client.client_id
    }

    fn send_broadcast_raw(&self, payload: &[u8], flags: u8) -> Result<(), crate::db::SendError> {
        self.client
            .try_send_broadcast_raw(payload, flags)
            .map_err(|e| match e {
                SendError::Closed => crate::db::SendError::Closed,
                SendError::ChannelClosed => crate::db::SendError::Closed,
                SendError::BufferFull => crate::db::SendError::BufferFull,
            })
    }

    fn close(&self) {
        self.client.close();
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Convert ProxyAuthInfo to database AuthInfo.
/// Returns None only for truly anonymous users (provider == "anonymous").
/// Firebase Legacy Tokens may have empty uid but are still authenticated.
fn convert_auth(proxy_auth: &ProxyAuthInfo) -> Option<DbAuthInfo> {
    if proxy_auth.provider == "anonymous" {
        return None;
    }

    Some(DbAuthInfo {
        uid: proxy_auth.uid.clone(),
        provider: proxy_auth.provider.clone(),
        token: proxy_auth.claims.clone(),
        is_admin: proxy_auth.is_admin,
    })
}

/// Parse project ID from database ID.
fn parse_project_id(database_id: &str) -> String {
    if let Some(idx) = database_id.find('/') {
        database_id[..idx].to_string()
    } else {
        database_id.to_string()
    }
}

/// True if `id` (a `project/database` identifier) is safe to append to the
/// on-disk data directory. Rejects empty / `.` / `..` path segments, NUL bytes,
/// and backslashes so a crafted id can't escape `{data_dir}` into another
/// tenant's tree (e.g. `../../other`). The edge enforces a stricter DNS-safe
/// charset; this is the server-side backstop so tenant isolation never depends
/// on the gateway sanitizing identifiers.
fn is_safe_database_id(id: &str) -> bool {
    if id.is_empty() || id.contains('\0') || id.contains('\\') {
        return false;
    }
    id.split('/')
        .all(|seg| !seg.is_empty() && seg != "." && seg != "..")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_project_id() {
        assert_eq!(parse_project_id("my-project/room-123"), "my-project");
        assert_eq!(parse_project_id("project/db"), "project");
        assert_eq!(parse_project_id("simple-db"), "simple-db");
    }

    #[test]
    fn test_is_safe_database_id() {
        // Normal identifiers.
        assert!(is_safe_database_id("project/database"));
        assert!(is_safe_database_id("simple-db"));
        assert!(is_safe_database_id("p/d"));
        // Traversal / escape attempts.
        assert!(!is_safe_database_id("../../etc"));
        assert!(!is_safe_database_id("project/../other"));
        assert!(!is_safe_database_id("..")); // bare parent
        assert!(!is_safe_database_id(".")); // bare current
        assert!(!is_safe_database_id("/abs/path")); // leading slash -> empty segment
        assert!(!is_safe_database_id("project//database")); // empty segment
        assert!(!is_safe_database_id("project/")); // trailing slash -> empty segment
        assert!(!is_safe_database_id("")); // empty
        assert!(!is_safe_database_id("project\\database")); // backslash
        assert!(!is_safe_database_id("project/db\0")); // NUL
    }

    #[test]
    fn test_convert_auth_anonymous() {
        let proxy_auth = ProxyAuthInfo {
            uid: String::new(),
            provider: "anonymous".to_string(),
            claims: Default::default(),
            is_admin: false,
        };

        assert!(convert_auth(&proxy_auth).is_none());
    }

    #[test]
    fn test_convert_auth_with_uid() {
        let proxy_auth = ProxyAuthInfo {
            uid: "user-123".to_string(),
            provider: "google".to_string(),
            claims: HashMap::new(),
            is_admin: false,
        };

        let db_auth = convert_auth(&proxy_auth).unwrap();
        assert_eq!(db_auth.uid, "user-123");
        assert_eq!(db_auth.provider, "google");
    }

    #[test]
    fn test_json5_parses_firebase_rules() {
        // JSON with comments and trailing commas
        let input = r#"//auth.currentcampaign = player or gm
{
  "rules": {
    ".read": "auth != null", // allow authenticated reads
    ".write": "auth != null",
  }
}"#;
        let parsed: serde_json::Value = json5::from_str(input).unwrap();
        assert_eq!(parsed["rules"][".read"], "auth != null");
        assert_eq!(parsed["rules"][".write"], "auth != null");
    }

    fn make_test_handler() -> Rc<CoreHandler> {
        let (tx, _rx) = glommio::channels::local_channel::new_bounded(16);
        CoreHandler::new(
            CoreHandlerConfig {
                core_id: 0,
                nr_cores: 1,
                port: 7779,
                emulator: true,
                data_dir: None,
                template_path: None,
            },
            Rc::new(tx),
            None,
        )
    }

    #[test]
    fn test_config_push_with_valid_rules_stores_config() {
        let handler = make_test_handler();

        let config = ProjectConfig {
            rules: Some(r#"{"rules": {".read": true, ".write": true}}"#.to_string()),
            secret_key: Some("secret123".to_string()),
            ephemeral: Some(false),
            ..Default::default()
        };

        handler.handle_config_push("my-project", config);

        // Config should be stored
        let configs = handler.project_configs.borrow();
        assert!(
            configs.contains_key("my-project"),
            "Config should be stored for valid rules"
        );
        let cached = configs.get("my-project").unwrap();
        assert!(
            cached.rules.is_some(),
            "Evaluator should be set for valid rules"
        );
    }

    #[test]
    fn test_config_push_with_no_rules_stores_config() {
        let handler = make_test_handler();

        // No rules = intentionally open (evaluator should be None)
        let config = ProjectConfig {
            rules: None,
            ephemeral: Some(true),
            ..Default::default()
        };

        handler.handle_config_push("open-project", config);

        let configs = handler.project_configs.borrow();
        assert!(
            configs.contains_key("open-project"),
            "Config should be stored when no rules"
        );
        let cached = configs.get("open-project").unwrap();
        assert!(
            cached.rules.is_none(),
            "No evaluator when no rules configured"
        );
    }

    #[test]
    fn test_config_push_with_empty_rules_stores_config() {
        let handler = make_test_handler();

        // Empty string rules = intentionally no rules (same as None)
        let config = ProjectConfig {
            rules: Some("   ".to_string()),
            ..Default::default()
        };

        handler.handle_config_push("empty-rules-project", config);

        let configs = handler.project_configs.borrow();
        assert!(
            configs.contains_key("empty-rules-project"),
            "Config should be stored for empty rules"
        );
    }

    #[test]
    fn test_config_push_with_invalid_json_rejects_config() {
        let handler = make_test_handler();

        // Invalid JSON — should NOT store config
        let config = ProjectConfig {
            rules: Some("this is not valid json at all!!!".to_string()),
            secret_key: Some("secret".to_string()),
            ..Default::default()
        };

        handler.handle_config_push("bad-project", config);

        // Config should NOT be stored
        let configs = handler.project_configs.borrow();
        assert!(
            !configs.contains_key("bad-project"),
            "Config should NOT be stored for invalid rules JSON"
        );
    }

    #[test]
    fn test_config_push_with_invalid_rules_structure_rejects_config() {
        let handler = make_test_handler();

        // Valid JSON but missing "rules" key — parse_rules will fail
        let config = ProjectConfig {
            rules: Some(r#"{"not_rules": true}"#.to_string()),
            secret_key: Some("secret".to_string()),
            ..Default::default()
        };

        handler.handle_config_push("bad-structure-project", config);

        // Config should NOT be stored
        let configs = handler.project_configs.borrow();
        assert!(
            !configs.contains_key("bad-structure-project"),
            "Config should NOT be stored for invalid rules structure"
        );
    }

    #[test]
    fn test_config_push_invalid_rules_does_not_clear_existing_config() {
        let handler = make_test_handler();

        // First: push valid config
        let valid_config = ProjectConfig {
            rules: Some(r#"{"rules": {".read": true, ".write": true}}"#.to_string()),
            secret_key: Some("secret".to_string()),
            ephemeral: Some(false),
            ..Default::default()
        };
        handler.handle_config_push("my-project", valid_config);

        // Verify it's stored
        assert!(handler.project_configs.borrow().contains_key("my-project"));

        // Second: push invalid config for the SAME project
        let invalid_config = ProjectConfig {
            rules: Some("broken json !!!".to_string()),
            secret_key: Some("new-secret".to_string()),
            ..Default::default()
        };
        handler.handle_config_push("my-project", invalid_config);

        // Original valid config should still be there (invalid push returned early)
        let configs = handler.project_configs.borrow();
        let cached = configs.get("my-project").unwrap();
        assert!(
            cached.rules.is_some(),
            "Original valid rules should still be in place"
        );
        assert_eq!(
            cached.secret_key.as_deref(),
            Some("secret"),
            "Original secret key should be preserved"
        );
    }

    #[test]
    fn test_config_push_rejects_stale_version() {
        let handler = make_test_handler();

        // v1: establish baseline
        handler.handle_config_push(
            "p",
            ProjectConfig {
                config_version: 1,
                secret_key: Some("v1".to_string()),
                ..Default::default()
            },
        );
        assert_eq!(
            handler
                .project_configs
                .borrow()
                .get("p")
                .unwrap()
                .config_version,
            1
        );

        // v1 again (duplicate) — should be skipped
        handler.handle_config_push(
            "p",
            ProjectConfig {
                config_version: 1,
                secret_key: Some("duplicate".to_string()),
                ..Default::default()
            },
        );
        assert_eq!(
            handler
                .project_configs
                .borrow()
                .get("p")
                .unwrap()
                .secret_key
                .as_deref(),
            Some("v1"),
            "duplicate version should be dropped, original secret retained"
        );

        // v0 (unversioned) after versioned — should be skipped (can't downgrade)
        handler.handle_config_push(
            "p",
            ProjectConfig {
                config_version: 0,
                secret_key: Some("unversioned".to_string()),
                ..Default::default()
            },
        );
        assert_eq!(
            handler
                .project_configs
                .borrow()
                .get("p")
                .unwrap()
                .secret_key
                .as_deref(),
            Some("v1"),
            "unversioned push after versioned should be skipped"
        );

        // v2 — should be accepted
        handler.handle_config_push(
            "p",
            ProjectConfig {
                config_version: 2,
                secret_key: Some("v2".to_string()),
                ..Default::default()
            },
        );
        let cached = handler.project_configs.borrow();
        let cached = cached.get("p").unwrap();
        assert_eq!(cached.config_version, 2);
        assert_eq!(cached.secret_key.as_deref(), Some("v2"));
    }

    #[test]
    fn test_evict_database_idempotent_and_cleans_client_maps() {
        let handler = make_test_handler();

        // Evicting a DB that was never loaded is a no-op (no panic).
        handler.handle_evict_database("ghost-project", "ghost-db", 0);
        assert!(handler.databases.borrow().is_empty());
        assert!(handler.client_databases.borrow().is_empty());

        // Evicting again is also fine.
        handler.handle_evict_database("ghost-project", "ghost-db", 0);
    }

    // The next two tests invoke the `ProxyHandler` trait methods directly
    // (not the inherent `handle_*` methods the other tests use). They guard
    // against drift between the trait signatures in `transport::proxy` and
    // the impl block below — the trait dispatch path is otherwise only
    // exercised end-to-end against a live proxy connection.

    #[test]
    fn test_on_config_push_trait_dispatch_stores_config() {
        let handler = make_test_handler();
        let config = ProjectConfig {
            rules: Some(r#"{"rules": {".read": true, ".write": true}}"#.to_string()),
            secret_key: Some("secret".to_string()),
            ephemeral: Some(false),
            ..Default::default()
        };

        ProxyHandler::on_config_push(&handler, "trait-dispatch-project", config);

        let configs = handler.project_configs.borrow();
        let cached = configs
            .get("trait-dispatch-project")
            .expect("config should be stored via trait dispatch");
        assert!(cached.rules.is_some());
        assert_eq!(cached.secret_key.as_deref(), Some("secret"));
    }

    #[test]
    fn test_on_connect_trait_dispatch_registers_client() {
        // process_connect creates a Database, which spawns tasks via
        // glommio::spawn_local — that requires a LocalExecutor in scope.
        glommio::LocalExecutorBuilder::new(glommio::Placement::Unbound)
            .spawn(|| async {
                let handler = make_test_handler();
                let client = Rc::new(VirtualClient::new_for_test(1, "my-project", "room-a"));

                // Emulator mode (set by make_test_handler) makes on_connect
                // take the immediate-process branch — the one that previously
                // held the unsafe.
                let result = ProxyHandler::on_connect(&handler, client.clone());

                assert!(!result.needs_config);
                assert_eq!(
                    result
                        .database_loaded
                        .as_ref()
                        .map(|(p, d)| (p.as_str(), d.as_str())),
                    Some(("my-project", "room-a")),
                );
                assert!(handler.client_databases.borrow().contains_key(&client.id));
                assert!(handler.databases.borrow().contains_key("my-project/room-a"));
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn test_config_push_absent_enabled_defaults_to_enabled() {
        let handler = make_test_handler();

        // Older proxies omit `enabled`; the server must treat that as enabled.
        let config = ProjectConfig {
            enabled: None,
            ..Default::default()
        };
        handler.handle_config_push("legacy-project", config);

        let configs = handler.project_configs.borrow();
        let cached = configs.get("legacy-project").unwrap();
        assert!(
            cached.enabled,
            "Absent `enabled` must default to enabled, not disabled"
        );
    }

    #[test]
    fn test_config_push_enabled_false_is_stored() {
        let handler = make_test_handler();

        let config = ProjectConfig {
            enabled: Some(false),
            config_version: 1,
            ..Default::default()
        };
        handler.handle_config_push("paused-project", config);

        let configs = handler.project_configs.borrow();
        let cached = configs.get("paused-project").unwrap();
        assert!(!cached.enabled, "enabled=false must be stored as disabled");
    }

    #[test]
    fn test_config_push_disabled_evicts_running_database() {
        // Creating a database spawns tasks via glommio::spawn_local, so the
        // test needs a LocalExecutor in scope.
        glommio::LocalExecutorBuilder::new(glommio::Placement::Unbound)
            .spawn(|| async {
                let handler = make_test_handler();

                // Bring a database online for the project.
                let client = Rc::new(VirtualClient::new_for_test(1, "pay-project", "room-a"));
                ProxyHandler::on_connect(&handler, client.clone());
                assert!(
                    handler
                        .databases
                        .borrow()
                        .contains_key("pay-project/room-a")
                );
                assert!(handler.client_databases.borrow().contains_key(&client.id));

                // A CONFIG_PUSH with enabled=false must evict the running
                // database (without purge) and drop its clients.
                let config = ProjectConfig {
                    enabled: Some(false),
                    config_version: 1,
                    ..Default::default()
                };
                handler.handle_config_push("pay-project", config);

                assert!(
                    !handler
                        .databases
                        .borrow()
                        .contains_key("pay-project/room-a"),
                    "disabled project's running database should be evicted"
                );
                assert!(
                    !handler.client_databases.borrow().contains_key(&client.id),
                    "evicted database's clients should be dropped"
                );
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn test_connect_to_disabled_project_is_refused() {
        glommio::LocalExecutorBuilder::new(glommio::Placement::Unbound)
            .spawn(|| async {
                let handler = make_test_handler();

                // Project is disabled before any client connects.
                let config = ProjectConfig {
                    enabled: Some(false),
                    config_version: 1,
                    ..Default::default()
                };
                handler.handle_config_push("disabled-project", config);

                let client = Rc::new(VirtualClient::new_for_test(1, "disabled-project", "room-a"));
                let result = ProxyHandler::on_connect(&handler, client.clone());

                assert!(
                    result.database_loaded.is_none(),
                    "no database should be created for a disabled project"
                );
                assert!(
                    !handler
                        .databases
                        .borrow()
                        .contains_key("disabled-project/room-a"),
                    "disabled project must not start a database"
                );
                assert!(
                    !handler.client_databases.borrow().contains_key(&client.id),
                    "client connecting to a disabled project must be refused"
                );
            })
            .unwrap()
            .join()
            .unwrap();
    }
}
