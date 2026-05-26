//! Per-core state for the thread-per-core model.
//!
//! Each core maintains its own local state with no sharing between cores.
//! This eliminates all cross-thread synchronization for the hot path.

use crate::db::DatabaseHandle;
use crate::server::config::CachedProjectConfig;
use glommio::TaskQueueHandle;
use std::cell::RefCell;
use std::collections::HashMap;

/// Thread-local state for a single core.
///
/// This replaces the global DashMaps from the Tokio implementation.
/// Since each core is single-threaded, no locks are needed.
pub struct CoreState {
    /// Core ID (0 to nr_cores-1)
    pub core_id: usize,

    /// Total number of cores
    pub nr_cores: usize,

    /// Active databases on this core: database_id -> DatabaseHandle
    pub databases: HashMap<String, DatabaseHandle>,

    /// Cached project configs: project_id -> config
    pub project_configs: HashMap<String, CachedProjectConfig>,

    /// Task queue for TCP I/O (high priority, latency-sensitive)
    pub tcp_tq: Option<TaskQueueHandle>,

    /// Task queue for database processing (lower priority)
    pub db_tq: Option<TaskQueueHandle>,

    /// Pending config requests: project_id -> list of buffered connects waiting for config
    pub pending_config_requests: HashMap<String, Vec<BufferedConnect>>,

    /// Server port for TCP listener
    pub port: u16,

    /// Whether this core is shutting down
    pub shutting_down: bool,
}

/// A buffered CONNECT message waiting for project config.
pub struct BufferedConnect {
    /// Client ID from the proxy
    pub client_id: u32,

    /// Project ID
    pub project_id: String,

    /// Database ID
    pub database_id: String,

    /// Raw CONNECT payload for replay after config arrives
    pub payload: Vec<u8>,

    /// When this connect was received (for timeout)
    pub received_at: std::time::Instant,

    /// Additional DATA messages buffered while waiting (up to 100KB)
    pub buffered_data: Vec<Vec<u8>>,

    /// Total size of buffered data
    pub buffered_size: usize,
}

impl BufferedConnect {
    /// Maximum size of buffered data per client (100KB)
    pub const MAX_BUFFER_SIZE: usize = 100 * 1024;

    /// Timeout for waiting for config (5 seconds)
    pub const CONFIG_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

    pub fn new(client_id: u32, project_id: String, database_id: String, payload: Vec<u8>) -> Self {
        Self {
            client_id,
            project_id,
            database_id,
            payload,
            received_at: std::time::Instant::now(),
            buffered_data: Vec::new(),
            buffered_size: 0,
        }
    }

    /// Try to buffer additional data. Returns false if buffer is full.
    pub fn try_buffer_data(&mut self, data: Vec<u8>) -> bool {
        if self.buffered_size + data.len() > Self::MAX_BUFFER_SIZE {
            return false;
        }
        self.buffered_size += data.len();
        self.buffered_data.push(data);
        true
    }

    /// Check if this buffered connect has timed out.
    pub fn is_timed_out(&self) -> bool {
        self.received_at.elapsed() > Self::CONFIG_TIMEOUT
    }
}

impl CoreState {
    /// Create a new CoreState for the given core.
    pub fn new(core_id: usize, nr_cores: usize, port: u16) -> Self {
        Self {
            core_id,
            nr_cores,
            databases: HashMap::new(),
            project_configs: HashMap::new(),
            tcp_tq: None,
            db_tq: None,
            pending_config_requests: HashMap::new(),
            port,
            shutting_down: false,
        }
    }

    /// Set the task queue handles after creation.
    pub fn set_task_queues(&mut self, tcp_tq: TaskQueueHandle, db_tq: TaskQueueHandle) {
        self.tcp_tq = Some(tcp_tq);
        self.db_tq = Some(db_tq);
    }

    /// Get a database handle, if it exists on this core.
    pub fn get_database(&self, database_id: &str) -> Option<&DatabaseHandle> {
        self.databases.get(database_id)
    }

    /// Insert a database handle.
    pub fn insert_database(&mut self, database_id: String, handle: DatabaseHandle) {
        self.databases.insert(database_id, handle);
    }

    /// Remove a database handle.
    pub fn remove_database(&mut self, database_id: &str) -> Option<DatabaseHandle> {
        self.databases.remove(database_id)
    }

    /// Get project config if cached.
    pub fn get_project_config(&self, project_id: &str) -> Option<&CachedProjectConfig> {
        self.project_configs.get(project_id)
    }

    /// Cache project config.
    pub fn set_project_config(&mut self, project_id: String, config: CachedProjectConfig) {
        self.project_configs.insert(project_id, config);
    }

    /// Check if we're already waiting for config for this project.
    pub fn has_pending_config_request(&self, project_id: &str) -> bool {
        self.pending_config_requests.contains_key(project_id)
    }

    /// Add a buffered connect for a project we're waiting on config for.
    pub fn add_pending_connect(&mut self, project_id: &str, connect: BufferedConnect) {
        self.pending_config_requests
            .entry(project_id.to_string())
            .or_default()
            .push(connect);
    }

    /// Take all pending connects for a project (after config arrives).
    pub fn take_pending_connects(&mut self, project_id: &str) -> Vec<BufferedConnect> {
        self.pending_config_requests
            .remove(project_id)
            .unwrap_or_default()
    }

    /// Clean up timed-out pending connects. Returns client IDs that timed out.
    pub fn cleanup_timed_out_connects(&mut self) -> Vec<(String, u32)> {
        let mut timed_out = Vec::new();

        self.pending_config_requests.retain(|project_id, connects| {
            connects.retain(|c| {
                if c.is_timed_out() {
                    timed_out.push((project_id.clone(), c.client_id));
                    false
                } else {
                    true
                }
            });
            !connects.is_empty()
        });

        timed_out
    }
}

// Thread-local storage for CoreState
thread_local! {
    static CORE_STATE: RefCell<Option<CoreState>> = const { RefCell::new(None) };
}

/// Initialize the thread-local CoreState for this core.
/// Must be called once at the start of each core's executor.
pub fn init_core_state(state: CoreState) {
    CORE_STATE.with(|cs| {
        *cs.borrow_mut() = Some(state);
    });
}

/// Access the thread-local CoreState.
/// Panics if called before init_core_state.
pub fn with_core_state<F, R>(f: F) -> R
where
    F: FnOnce(&CoreState) -> R,
{
    CORE_STATE.with(|cs| {
        let state = cs.borrow();
        let state = state
            .as_ref()
            .expect("CoreState not initialized for this thread");
        f(state)
    })
}

/// Access the thread-local CoreState mutably.
/// Panics if called before init_core_state.
pub fn with_core_state_mut<F, R>(f: F) -> R
where
    F: FnOnce(&mut CoreState) -> R,
{
    CORE_STATE.with(|cs| {
        let mut state = cs.borrow_mut();
        let state = state
            .as_mut()
            .expect("CoreState not initialized for this thread");
        f(state)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffered_connect_timeout() {
        let connect = BufferedConnect::new(1, "project".to_string(), "db".to_string(), vec![]);
        assert!(!connect.is_timed_out());
        // Note: Can't easily test actual timeout without sleeping
    }

    #[test]
    fn test_buffered_connect_buffer_limit() {
        let mut connect = BufferedConnect::new(1, "project".to_string(), "db".to_string(), vec![]);

        // Should accept data up to limit
        let small_data = vec![0u8; 1024];
        assert!(connect.try_buffer_data(small_data.clone()));
        assert_eq!(connect.buffered_size, 1024);

        // Should reject when over limit
        let large_data = vec![0u8; BufferedConnect::MAX_BUFFER_SIZE];
        assert!(!connect.try_buffer_data(large_data));
        assert_eq!(connect.buffered_size, 1024); // Unchanged
    }

    #[test]
    fn test_core_state_databases() {
        let state = CoreState::new(0, 4, 7779);

        assert!(state.get_database("test/db").is_none());

        // Note: Can't easily create a real DatabaseHandle in unit tests
        // Integration tests will cover this
    }
}
