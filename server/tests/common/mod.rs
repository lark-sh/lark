//! Test harness for integration tests using Glommio.
//!
//! This provides an in-memory test client that communicates with the database
//! through local channels, running inside a Glommio LocalExecutor.
//!
//! The test client mimics proxy-style connections:
//! - Auth info is provided at connection time (like proxy CONNECT)
//! - No explicit JOIN/AUTH messages (proxy handles those)
//! - Client is immediately ready to send operations

// Shared test harness: each integration-test binary uses only a subset of these
// helpers, and the single-threaded Glommio executor makes Rc/RefCell-across-await
// patterns safe here.
#![allow(
    dead_code,
    clippy::type_complexity,
    clippy::arc_with_non_send_sync,
    clippy::await_holding_refcell_ref
)]

use glommio::channels::local_channel::{self, LocalReceiver, LocalSender};
use glommio::timer::Timer;
use glommio::{LocalExecutorBuilder, Placement};

use bytes::Bytes;
use lark_server::db::{
    AuthInfo, ConnectionSender, Database, DatabaseHandle, SendError, generate_push_id,
};
pub use lark_server::protocol::{ClientMessage, ServerMessage, TransactionOp, action, op};
use lark_server::rules::{Evaluator, parse_rules};
use lark_server::storage::{StorageWorker, StorageWorkerMessage};

use serde_json::{Value, json};
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Test secret used for JWT signing in auth tests.
pub const TEST_SECRET: &str = "test-secret-key-for-integration-tests";

/// Default timeout for waiting for responses.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

// =============================================================================
// Test Runner
// =============================================================================

/// Run a test inside a Glommio LocalExecutor.
/// This is the main entry point for integration tests.
///
/// Usage:
/// ```ignore
/// #[test]
/// fn test_example() {
///     run_test(|| async {
///         let server = TestServer::new();
///         let mut client = server.client();
///         // ... test body ...
///     });
/// }
/// ```
pub fn run_test<F, Fut>(test_fn: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + 'static,
{
    LocalExecutorBuilder::new(Placement::Unbound)
        .spawn(|| async move {
            test_fn().await;
        })
        .unwrap()
        .join()
        .unwrap();
}

// =============================================================================
// Test Server
// =============================================================================

/// A test server wrapper that manages databases for testing.
/// In Glommio model, this directly manages Database instances.
pub struct TestServer {
    /// Active databases: database_id -> handle
    databases: Rc<RefCell<HashMap<String, DatabaseHandle>>>,
    /// Project rules: project_id -> evaluator
    project_rules: Rc<RefCell<HashMap<String, Arc<Evaluator>>>>,
    /// Project ephemeral flags: project_id -> is_ephemeral
    project_ephemeral: Rc<RefCell<HashMap<String, bool>>>,
    /// Data directory for persistence tests
    data_dir: Option<String>,
    /// Emulator mode (always true for tests)
    emulator: bool,
    /// Compaction channel sender for the per-core storage worker
    compaction_tx: Rc<LocalSender<StorageWorkerMessage>>,
}

impl TestServer {
    /// Create a compaction channel and spawn the storage worker task.
    fn spawn_storage_worker() -> Rc<LocalSender<StorageWorkerMessage>> {
        let (tx, rx) = local_channel::new_bounded(256);
        let tx = Rc::new(tx);

        glommio::spawn_local(async move {
            let mut worker = StorageWorker::new(rx);
            worker.run().await;
        })
        .detach();

        tx
    }

    /// Create a new test server in emulator mode.
    pub fn new() -> Self {
        Self {
            databases: Rc::new(RefCell::new(HashMap::new())),
            project_rules: Rc::new(RefCell::new(HashMap::new())),
            project_ephemeral: Rc::new(RefCell::new(HashMap::new())),
            data_dir: None,
            emulator: true,
            compaction_tx: Self::spawn_storage_worker(),
        }
    }

    /// Create a new test server with persistence enabled.
    pub fn with_persistence(data_dir: &str) -> Self {
        Self {
            databases: Rc::new(RefCell::new(HashMap::new())),
            project_rules: Rc::new(RefCell::new(HashMap::new())),
            project_ephemeral: Rc::new(RefCell::new(HashMap::new())),
            data_dir: Some(data_dir.to_string()),
            emulator: true,
            compaction_tx: Self::spawn_storage_worker(),
        }
    }

    /// Create a new test server with the same data directory (for restart testing).
    pub fn restart_with_persistence(data_dir: &str) -> Self {
        Self::with_persistence(data_dir)
    }

    /// Create a new test server with project secrets configured.
    /// (In Glommio model, secrets are not needed for emulator mode)
    pub fn with_secrets() -> Self {
        Self::new()
    }

    /// Set rules for a project (ephemeral by default).
    pub fn set_rules(&self, project_id: &str, rules: Value) -> Result<(), String> {
        self.set_rules_with_ephemeral(project_id, rules, true)
    }

    /// Push new rules to a project *and* hot-reload any currently-loaded
    /// databases for that project — mirrors what CONFIG_PUSH does in
    /// production (see `CoreHandler::handle_config_push`). Use this when you
    /// want to verify that an already-running database picks up new rules
    /// without being torn down.
    ///
    /// Passing an empty rules object (`json!({})`) or `Value::Null` clears
    /// the evaluator (fully open).
    pub fn push_rules(&self, project_id: &str, rules: Value) -> Result<(), String> {
        let new_eval: Option<Arc<Evaluator>> = if rules.is_null() {
            None
        } else {
            let ruleset = parse_rules(&rules).map_err(|e| e.to_string())?;
            Some(Arc::new(Evaluator::new(ruleset)))
        };

        // Update the cache so newly-created databases pick up the rules.
        match &new_eval {
            Some(e) => {
                self.project_rules
                    .borrow_mut()
                    .insert(project_id.to_string(), e.clone());
            }
            None => {
                self.project_rules.borrow_mut().remove(project_id);
            }
        }

        // Hot-reload every loaded database belonging to this project.
        let targets: Vec<DatabaseHandle> = self
            .databases
            .borrow()
            .iter()
            .filter(|(full_id, _)| {
                let proj = full_id
                    .split_once('/')
                    .map(|(p, _)| p)
                    .unwrap_or(full_id.as_str());
                proj == project_id
            })
            .map(|(_, h)| h.clone())
            .collect();

        for handle in targets {
            handle.update_evaluator(new_eval.as_deref().cloned());
        }

        Ok(())
    }

    /// Set rules for a project with explicit ephemeral flag.
    pub fn set_rules_with_ephemeral(
        &self,
        project_id: &str,
        rules: Value,
        ephemeral: bool,
    ) -> Result<(), String> {
        let ruleset = parse_rules(&rules).map_err(|e| e.to_string())?;
        let evaluator = Arc::new(Evaluator::new(ruleset));
        self.project_rules
            .borrow_mut()
            .insert(project_id.to_string(), evaluator);
        self.project_ephemeral
            .borrow_mut()
            .insert(project_id.to_string(), ephemeral);
        Ok(())
    }

    /// Evict a database: drop the handle (triggers graceful shutdown via
    /// `Rc<inbox_sender>` count → 1) and, if `purge` is set, rename the data
    /// directory to `{dir}-deleted-{unix_ts}`. Mirrors production
    /// `CoreHandler::handle_evict_database`.
    ///
    /// Returns the path the data dir was renamed to (if any) so tests can
    /// assert on it.
    pub fn evict_database(
        &self,
        project_id: &str,
        database_id: &str,
        purge: bool,
    ) -> Option<std::path::PathBuf> {
        let full_id = if project_id.is_empty() {
            database_id.to_string()
        } else {
            format!("{}/{}", project_id, database_id)
        };

        // Drop the handle (idempotent — None if the DB wasn't loaded).
        self.databases.borrow_mut().remove(&full_id);

        if !purge {
            return None;
        }

        let data_dir = self.data_dir.as_ref()?;
        let db_data_dir = std::path::PathBuf::from(format!("{}/{}", data_dir, full_id));
        if !db_data_dir.exists() {
            return None;
        }

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let renamed = std::path::PathBuf::from(format!("{}/{}-deleted-{}", data_dir, full_id, ts));
        std::fs::rename(&db_data_dir, &renamed).expect("rename on purge");
        Some(renamed)
    }

    /// Get the database count.
    pub fn database_count(&self) -> usize {
        self.databases.borrow().len()
    }

    /// Create a new test client (not yet connected to a database).
    pub fn client(&self) -> TestClient {
        TestClient::new(
            self.databases.clone(),
            self.project_rules.clone(),
            self.project_ephemeral.clone(),
            self.data_dir.clone(),
            self.compaction_tx.clone(),
        )
    }

    /// Shutdown the server (for restart testing).
    pub async fn shutdown(&self) {
        // Clear all databases
        self.databases.borrow_mut().clear();
    }

    /// Get the data directory if persistence is enabled.
    pub fn data_dir(&self) -> Option<&str> {
        self.data_dir.as_deref()
    }
}

impl Default for TestServer {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Test Sender (ConnectionSender implementation)
// =============================================================================

/// In-memory connection sender for test clients.
struct TestSender {
    /// Channel to send messages to the client.
    tx: Rc<LocalSender<Vec<u8>>>,
}

impl ConnectionSender for TestSender {
    fn send(
        &self,
        data: Bytes,
        _volatile: bool,
    ) -> Pin<Box<dyn Future<Output = Result<(), SendError>> + '_>> {
        let result = self
            .tx
            .try_send(data.to_vec())
            .map_err(|_| SendError::Closed);
        Box::pin(async move { result })
    }

    fn try_send(
        &self,
        data: Bytes,
        _volatile: bool,
        _skip_translation: bool,
    ) -> Result<(), SendError> {
        self.tx
            .try_send(data.to_vec())
            .map_err(|_| SendError::Closed)
    }

    fn outbox_id(&self) -> usize {
        // Return unique ID based on channel pointer
        Rc::as_ptr(&self.tx) as usize
    }

    fn client_id(&self) -> u32 {
        1 // Tests only have one client per sender
    }

    fn send_broadcast_raw(&self, payload: &[u8], _flags: u8) -> Result<(), SendError> {
        // Parse the payload to extract the message bytes
        // Format: [ClientCount:4][[ClientID:4][Tag:4]...][MsgLen:4][MsgBytes...]
        if payload.len() < 4 {
            return Err(SendError::Closed);
        }
        let client_count =
            u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
        let clients_end = 4 + (client_count * 8);
        if payload.len() < clients_end + 4 {
            return Err(SendError::Closed);
        }
        let msg_len = u32::from_be_bytes([
            payload[clients_end],
            payload[clients_end + 1],
            payload[clients_end + 2],
            payload[clients_end + 3],
        ]) as usize;
        let msg_start = clients_end + 4;
        if payload.len() < msg_start + msg_len {
            return Err(SendError::Closed);
        }
        // Send the message bytes
        self.tx
            .try_send(payload[msg_start..msg_start + msg_len].to_vec())
            .map_err(|_| SendError::Closed)
    }
}

// =============================================================================
// Test Client
// =============================================================================

/// A test client for integration tests.
///
/// This mimics a proxy-style connection:
/// - Connect to a database with auth info provided upfront
/// - No explicit JOIN/AUTH messages needed
/// - Immediately ready to send operations after connect
pub struct TestClient {
    /// Unique client ID.
    pub id: String,

    /// Shared database registry.
    databases: Rc<RefCell<HashMap<String, DatabaseHandle>>>,

    /// Shared project rules.
    project_rules: Rc<RefCell<HashMap<String, Arc<Evaluator>>>>,

    /// Shared project ephemeral flags.
    project_ephemeral: Rc<RefCell<HashMap<String, bool>>>,

    /// Data directory for persistence.
    data_dir: Option<String>,

    /// Compaction channel sender for the storage worker.
    compaction_tx: Rc<LocalSender<StorageWorkerMessage>>,

    /// Database handle (set after connect).
    db_handle: Option<DatabaseHandle>,

    /// Connection ID (set after connect).
    pub connection_id: Option<String>,

    /// Sender for registering with databases.
    sender: Arc<TestSender>,

    /// Receiver for messages from database.
    receiver: Rc<RefCell<LocalReceiver<Vec<u8>>>>,

    /// Request ID counter.
    request_id: AtomicU64,

    /// Pending requests waiting for responses: request_id -> oneshot sender
    pending: Rc<RefCell<HashMap<String, Rc<RefCell<Option<ServerMessage>>>>>>,

    /// Received events (not ack/nack).
    events: Rc<RefCell<Vec<ServerMessage>>>,

    /// Raw messages in receive order (for ordering tests).
    raw_messages: Rc<RefCell<Vec<ServerMessage>>>,
}

impl TestClient {
    /// Create a new test client (not yet connected to a database).
    pub fn new(
        databases: Rc<RefCell<HashMap<String, DatabaseHandle>>>,
        project_rules: Rc<RefCell<HashMap<String, Arc<Evaluator>>>>,
        project_ephemeral: Rc<RefCell<HashMap<String, bool>>>,
        data_dir: Option<String>,
        compaction_tx: Rc<LocalSender<StorageWorkerMessage>>,
    ) -> Self {
        let id = generate_push_id();
        let (tx, rx) = local_channel::new_bounded(1000);
        let sender = Arc::new(TestSender { tx: Rc::new(tx) });

        Self {
            id,
            databases,
            project_rules,
            project_ephemeral,
            data_dir,
            compaction_tx,
            db_handle: None,
            connection_id: None,
            sender,
            receiver: Rc::new(RefCell::new(rx)),
            request_id: AtomicU64::new(0),
            pending: Rc::new(RefCell::new(HashMap::new())),
            events: Rc::new(RefCell::new(Vec::new())),
            raw_messages: Rc::new(RefCell::new(Vec::new())),
        }
    }

    /// Process any pending messages from the database.
    async fn process_pending_messages(&self) {
        use futures::future::poll_immediate;

        loop {
            let msg_data = {
                let rx = self.receiver.borrow_mut();
                match poll_immediate(rx.recv()).await {
                    Some(Some(data)) => data,
                    _ => break,
                }
            };

            // Parse the message
            let msg: ServerMessage = match serde_json::from_slice(&msg_data) {
                Ok(m) => m,
                Err(_) => continue,
            };

            // Record raw message
            self.raw_messages.borrow_mut().push(msg.clone());

            // Check if this is a response to a pending request
            let request_id = msg.ack.as_ref().or(msg.nack.as_ref()).or(msg.once.as_ref());

            if let Some(rid) = request_id {
                let mut pending = self.pending.borrow_mut();
                if let Some(slot) = pending.remove(rid) {
                    *slot.borrow_mut() = Some(msg);
                    continue;
                }
            }

            // This is an event
            self.events.borrow_mut().push(msg);
        }
    }

    /// Generate the next request ID.
    fn next_request_id(&self) -> String {
        let id = self.request_id.fetch_add(1, Ordering::SeqCst);
        format!("r{}", id)
    }

    /// Send a message and wait for a response.
    pub async fn send_and_wait(&self, msg: ClientMessage) -> Result<ServerMessage, String> {
        let request_id = msg.request_id.clone().unwrap_or_default();

        // Create a slot for the response
        let response_slot: Rc<RefCell<Option<ServerMessage>>> = Rc::new(RefCell::new(None));
        self.pending
            .borrow_mut()
            .insert(request_id.clone(), response_slot.clone());

        // Send message
        let db_handle = self
            .db_handle
            .as_ref()
            .ok_or("Not connected to a database")?;
        db_handle.send_message(self.id.clone(), msg);

        // Wait for response with timeout
        let start = Instant::now();
        while start.elapsed() < DEFAULT_TIMEOUT {
            // Process any pending messages
            self.process_pending_messages().await;

            // Check if we got our response
            if response_slot.borrow().is_some() {
                return Ok(response_slot.borrow_mut().take().unwrap());
            }

            // Small delay to avoid busy-waiting
            Timer::new(Duration::from_millis(1)).await;
        }

        // Timeout
        self.pending.borrow_mut().remove(&request_id);
        Err("Timeout waiting for response".to_string())
    }

    /// Get or create a database.
    fn get_or_create_database(&self, database_id: &str) -> DatabaseHandle {
        // Check if already exists
        if let Some(handle) = self.databases.borrow().get(database_id) {
            return handle.clone();
        }

        // Extract project ID
        let project_id = if let Some(idx) = database_id.find('/') {
            &database_id[..idx]
        } else {
            database_id
        };

        // Check if project is ephemeral (default to true if not set)
        let is_ephemeral = self
            .project_ephemeral
            .borrow()
            .get(project_id)
            .copied()
            .unwrap_or(true);

        // Create new database - only use persistence if data_dir is set AND project is not ephemeral
        let mut db = if let Some(ref data_dir) = self.data_dir {
            if is_ephemeral {
                // Ephemeral project - no persistence even if data_dir is set
                Database::new(database_id.to_string(), project_id.to_string(), true)
            } else {
                // Non-ephemeral project with data_dir - use persistence
                let db_data_dir = std::path::PathBuf::from(format!("{}/{}", data_dir, database_id));
                Database::new_with_persistence(
                    database_id.to_string(),
                    project_id.to_string(),
                    db_data_dir,
                )
            }
        } else {
            Database::new(database_id.to_string(), project_id.to_string(), true)
        };

        // Set rules if available
        if let Some(evaluator) = self.project_rules.borrow().get(project_id) {
            db.set_evaluator((**evaluator).clone());
        }

        // Set compaction channel for WAL rotation notifications
        db.set_compaction_tx(self.compaction_tx.clone());

        // Note: Disk loading now happens at the start of run() after acquiring
        // the segmentation lock. No manual load_from_disk() call needed.

        // Get handle before spawning
        let handle = db.handle();

        // Store in registry
        self.databases
            .borrow_mut()
            .insert(database_id.to_string(), handle.clone());

        // Spawn database task
        let database_id_owned = database_id.to_string();
        let databases_clone = self.databases.clone();

        glommio::spawn_local(async move {
            db.run().await;

            // Remove from registry when done
            databases_clone.borrow_mut().remove(&database_id_owned);
        })
        .detach();

        handle
    }

    /// Connect to a database as an anonymous user.
    pub async fn connect(&mut self, database_id: &str) {
        self.connect_with_auth(database_id, None).await;
    }

    /// Connect to a database with specific auth info.
    pub async fn connect_with_auth(&mut self, database_id: &str, auth: Option<AuthInfo>) {
        // Get or create the database
        let db_handle = self.get_or_create_database(database_id);

        // Generate a connection ID
        let connection_id = generate_push_id();
        self.connection_id = Some(connection_id.clone());

        // Add ourselves as a client with auth already set
        db_handle.add_client(self.id.clone(), auth, connection_id, self.sender.clone());

        self.db_handle = Some(db_handle);

        // Give the database a moment to process the add_client
        Timer::new(Duration::from_millis(5)).await;
    }

    /// Connect to a database with a specific connection ID (for reconnect testing).
    pub async fn connect_with_connection_id(&mut self, database_id: &str, connection_id: &str) {
        // Get or create the database
        let db_handle = self.get_or_create_database(database_id);

        // Use the provided connection ID
        self.connection_id = Some(connection_id.to_string());

        // Add ourselves as a client with the provided connection ID
        db_handle.add_client(
            self.id.clone(),
            None,
            connection_id.to_string(),
            self.sender.clone(),
        );

        self.db_handle = Some(db_handle);

        // Give the database a moment to process
        Timer::new(Duration::from_millis(5)).await;
    }

    /// Connect as a specific user (convenience method).
    pub async fn connect_as_user(&mut self, database_id: &str, uid: &str) {
        let auth = AuthInfo {
            uid: uid.to_string(),
            provider: "test".to_string(),
            token: HashMap::new(),
            is_admin: false,
        };
        self.connect_with_auth(database_id, Some(auth)).await;
    }

    /// Connect as an admin user (convenience method).
    pub async fn connect_as_admin(&mut self, database_id: &str, uid: &str) {
        let auth = AuthInfo {
            uid: uid.to_string(),
            provider: "admin".to_string(),
            token: HashMap::new(),
            is_admin: true,
        };
        self.connect_with_auth(database_id, Some(auth)).await;
    }

    /// Update the client's auth (simulates AUTH_CHANGED from proxy).
    pub async fn update_auth(&self, auth: Option<AuthInfo>) {
        if let Some(db_handle) = &self.db_handle {
            db_handle.update_client_auth(self.id.clone(), auth);
        }
    }

    /// Set a value at a path.
    pub async fn set(&self, path: &str, value: impl Into<Value>) -> Result<(), String> {
        let request_id = self.next_request_id();
        let msg = ClientMessage {
            op: op::SET.to_string(),
            path: Some(path.to_string()),
            value: Some(value.into()),
            request_id: Some(request_id),
            ..Default::default()
        };

        let resp = self.send_and_wait(msg).await?;

        if resp.nack.is_some() {
            return Err(format!(
                "{}: {}",
                resp.error.unwrap_or_default(),
                resp.message.unwrap_or_default()
            ));
        }

        Ok(())
    }

    /// Set a value with a specific request ID (for deduplication testing).
    pub async fn set_with_request_id(
        &self,
        path: &str,
        value: impl Into<Value>,
        request_id: &str,
    ) -> Result<(), String> {
        let msg = ClientMessage {
            op: op::SET.to_string(),
            path: Some(path.to_string()),
            value: Some(value.into()),
            request_id: Some(request_id.to_string()),
            ..Default::default()
        };

        let resp = self.send_and_wait(msg).await?;

        if resp.nack.is_some() {
            return Err(format!(
                "{}: {}",
                resp.error.unwrap_or_default(),
                resp.message.unwrap_or_default()
            ));
        }

        Ok(())
    }

    /// Get the connection ID for this client.
    pub fn get_connection_id(&self) -> Option<&str> {
        self.connection_id.as_deref()
    }

    /// Set a value with pending writes list (for tainted write testing).
    pub async fn set_with_pending_writes(
        &self,
        path: &str,
        value: impl Into<Value>,
        request_id: &str,
        pending_writes: Option<Vec<String>>,
    ) -> Result<(), String> {
        let msg = ClientMessage {
            op: op::SET.to_string(),
            path: Some(path.to_string()),
            value: Some(value.into()),
            request_id: Some(request_id.to_string()),
            pending_writes,
            ..Default::default()
        };

        let resp = self.send_and_wait(msg).await?;

        if resp.nack.is_some() {
            return Err(format!(
                "{}: {}",
                resp.error.unwrap_or_default(),
                resp.message.unwrap_or_default()
            ));
        }

        Ok(())
    }

    /// Set a value with pending writes list, without waiting for response.
    pub async fn set_with_pending_writes_fire_and_forget(
        &self,
        path: &str,
        value: impl Into<Value>,
        request_id: &str,
        pending_writes: Vec<String>,
    ) -> Result<(), String> {
        let msg = ClientMessage {
            op: op::SET.to_string(),
            path: Some(path.to_string()),
            value: Some(value.into()),
            request_id: Some(request_id.to_string()),
            pending_writes: Some(pending_writes),
            ..Default::default()
        };

        let db_handle = self.db_handle.as_ref().expect("not connected");
        db_handle.send_message(self.id.clone(), msg);

        Ok(())
    }

    /// Set a value with priority at a path.
    pub async fn set_with_priority(
        &self,
        path: &str,
        value: Value,
        priority: f64,
    ) -> Result<(), String> {
        let value_with_priority = match value {
            Value::Object(mut map) => {
                map.insert(".priority".to_string(), json!(priority));
                Value::Object(map)
            }
            _ => {
                json!({
                    ".value": value,
                    ".priority": priority
                })
            }
        };

        self.set(path, value_with_priority).await
    }

    /// Set a raw value (for server value tests like timestamps).
    pub async fn set_raw(&self, path: &str, value: Value) -> Result<(), String> {
        let request_id = self.next_request_id();
        let msg = ClientMessage {
            op: op::SET.to_string(),
            path: Some(path.to_string()),
            value: Some(value),
            request_id: Some(request_id),
            ..Default::default()
        };

        let resp = self.send_and_wait(msg).await?;

        if resp.nack.is_some() {
            return Err(format!(
                "{}: {}",
                resp.error.unwrap_or_default(),
                resp.message.unwrap_or_default()
            ));
        }

        Ok(())
    }

    /// Set a volatile value (no ack expected).
    pub async fn set_volatile(&self, path: &str, value: impl Into<Value>) -> Result<(), String> {
        let msg = ClientMessage {
            op: op::SET.to_string(),
            path: Some(path.to_string()),
            value: Some(value.into()),
            volatile: Some(true),
            ..Default::default()
        };

        let db_handle = self
            .db_handle
            .as_ref()
            .ok_or("Not connected to a database")?;
        db_handle.send_message(self.id.clone(), msg);

        Ok(())
    }

    /// Update (merge) values at a path.
    pub async fn update(&self, path: &str, value: impl Into<Value>) -> Result<(), String> {
        let request_id = self.next_request_id();
        let msg = ClientMessage {
            op: op::UPDATE.to_string(),
            path: Some(path.to_string()),
            value: Some(value.into()),
            request_id: Some(request_id),
            ..Default::default()
        };

        let resp = self.send_and_wait(msg).await?;

        if resp.nack.is_some() {
            return Err(format!(
                "{}: {}",
                resp.error.unwrap_or_default(),
                resp.message.unwrap_or_default()
            ));
        }

        Ok(())
    }

    /// Remove a value at a path.
    pub async fn remove(&self, path: &str) -> Result<(), String> {
        let request_id = self.next_request_id();
        let msg = ClientMessage {
            op: op::REMOVE.to_string(),
            path: Some(path.to_string()),
            value: Some(Value::Null),
            request_id: Some(request_id),
            ..Default::default()
        };

        let resp = self.send_and_wait(msg).await?;

        if resp.nack.is_some() {
            return Err(format!(
                "{}: {}",
                resp.error.unwrap_or_default(),
                resp.message.unwrap_or_default()
            ));
        }

        Ok(())
    }

    /// Read a value once.
    pub async fn once(&self, path: &str) -> Result<Value, String> {
        let request_id = self.next_request_id();
        let msg = ClientMessage {
            op: op::ONCE.to_string(),
            path: Some(path.to_string()),
            request_id: Some(request_id),
            ..Default::default()
        };

        let resp = self.send_and_wait(msg).await?;

        if resp.nack.is_some() {
            return Err(format!(
                "{}: {}",
                resp.error.unwrap_or_default(),
                resp.message.unwrap_or_default()
            ));
        }

        Ok(resp.once_value.map(|v| v.to_value()).unwrap_or(Value::Null))
    }

    /// Read a value once with query parameters.
    pub async fn once_query(&self, path: &str, query: QueryOptions) -> Result<Value, String> {
        let request_id = self.next_request_id();
        let msg = ClientMessage {
            op: op::ONCE.to_string(),
            path: Some(path.to_string()),
            request_id: Some(request_id),
            order_by: query.order_by,
            order_by_child: query.order_by_child,
            limit_to_first: query.limit_to_first,
            limit_to_last: query.limit_to_last,
            start_at: query.start_at,
            start_at_key: query.start_at_key,
            end_at: query.end_at,
            end_at_key: query.end_at_key,
            equal_to: query.equal_to,
            equal_to_key: query.equal_to_key,
            ..Default::default()
        };

        let resp = self.send_and_wait(msg).await?;

        if resp.nack.is_some() {
            return Err(format!(
                "{}: {}",
                resp.error.unwrap_or_default(),
                resp.message.unwrap_or_default()
            ));
        }

        Ok(resp.once_value.map(|v| v.to_value()).unwrap_or(Value::Null))
    }

    /// Subscribe to events at a path.
    /// Note: The `_events` parameter is ignored (server-side event filtering removed).
    pub async fn subscribe(&self, path: &str, _events: &[&str]) -> Result<(), String> {
        let request_id = self.next_request_id();
        let msg = ClientMessage {
            op: op::SUBSCRIBE.to_string(),
            path: Some(path.to_string()),
            request_id: Some(request_id),
            ..Default::default()
        };

        let resp = self.send_and_wait(msg).await?;

        if resp.nack.is_some() {
            return Err(format!(
                "{}: {}",
                resp.error.unwrap_or_default(),
                resp.message.unwrap_or_default()
            ));
        }

        Ok(())
    }

    /// Subscribe to events with query parameters.
    /// Note: The `_events` parameter is ignored (server-side event filtering removed).
    pub async fn subscribe_with_query(
        &self,
        path: &str,
        _events: &[&str],
        query: QueryOptions,
    ) -> Result<(), String> {
        let request_id = self.next_request_id();
        let msg = ClientMessage {
            op: op::SUBSCRIBE.to_string(),
            path: Some(path.to_string()),
            request_id: Some(request_id),
            order_by: query.order_by,
            order_by_child: query.order_by_child,
            limit_to_first: query.limit_to_first,
            limit_to_last: query.limit_to_last,
            start_at: query.start_at,
            start_at_key: query.start_at_key,
            end_at: query.end_at,
            end_at_key: query.end_at_key,
            equal_to: query.equal_to,
            equal_to_key: query.equal_to_key,
            ..Default::default()
        };

        let resp = self.send_and_wait(msg).await?;

        if resp.nack.is_some() {
            return Err(format!(
                "{}: {}",
                resp.error.unwrap_or_default(),
                resp.message.unwrap_or_default()
            ));
        }

        Ok(())
    }

    /// Unsubscribe from events at a path.
    pub async fn unsubscribe(&self, path: &str) -> Result<(), String> {
        let request_id = self.next_request_id();
        let msg = ClientMessage {
            op: op::UNSUBSCRIBE.to_string(),
            path: Some(path.to_string()),
            request_id: Some(request_id),
            ..Default::default()
        };

        let resp = self.send_and_wait(msg).await?;

        if resp.nack.is_some() {
            return Err(format!(
                "{}: {}",
                resp.error.unwrap_or_default(),
                resp.message.unwrap_or_default()
            ));
        }

        Ok(())
    }

    /// Register an ondisconnect set action.
    pub async fn on_disconnect_set(
        &self,
        path: &str,
        value: impl Into<Value>,
    ) -> Result<(), String> {
        self.on_disconnect(path, action::SET, Some(value.into()))
            .await
    }

    /// Register an ondisconnect update action.
    pub async fn on_disconnect_update(
        &self,
        path: &str,
        value: impl Into<Value>,
    ) -> Result<(), String> {
        self.on_disconnect(path, action::UPDATE, Some(value.into()))
            .await
    }

    /// Register an ondisconnect remove action.
    pub async fn on_disconnect_remove(&self, path: &str) -> Result<(), String> {
        self.on_disconnect(path, action::REMOVE, None).await
    }

    /// Cancel ondisconnect hooks for a path.
    pub async fn on_disconnect_cancel(&self, path: &str) -> Result<(), String> {
        self.on_disconnect(path, action::CANCEL, None).await
    }

    /// Send an ondisconnect message.
    async fn on_disconnect(
        &self,
        path: &str,
        action: &str,
        value: Option<Value>,
    ) -> Result<(), String> {
        let request_id = self.next_request_id();
        let msg = ClientMessage {
            op: op::ON_DISCONNECT.to_string(),
            path: Some(path.to_string()),
            action: Some(action.to_string()),
            value,
            request_id: Some(request_id),
            ..Default::default()
        };

        let resp = self.send_and_wait(msg).await?;

        if resp.nack.is_some() {
            return Err(format!(
                "{}: {}",
                resp.error.unwrap_or_default(),
                resp.message.unwrap_or_default()
            ));
        }

        Ok(())
    }

    /// Execute a transaction.
    pub async fn transaction(&self, ops: Vec<TransactionOp>) -> Result<(), String> {
        let request_id = self.next_request_id();
        let msg = ClientMessage {
            op: op::TRANSACTION.to_string(),
            operations: Some(ops),
            request_id: Some(request_id),
            ..Default::default()
        };

        let resp = self.send_and_wait(msg).await?;

        if resp.nack.is_some() {
            return Err(format!(
                "{}: {}",
                resp.error.unwrap_or_default(),
                resp.message.unwrap_or_default()
            ));
        }

        Ok(())
    }

    /// Wait for an event to arrive.
    pub async fn wait_for_event(
        &self,
        timeout_duration: Duration,
    ) -> Result<ServerMessage, String> {
        let start = Instant::now();

        loop {
            // Process any pending messages
            self.process_pending_messages().await;

            // Check if we have events
            {
                let mut events = self.events.borrow_mut();
                if !events.is_empty() {
                    return Ok(events.remove(0));
                }
            }

            // Check timeout
            if start.elapsed() >= timeout_duration {
                return Err("Timeout waiting for event".to_string());
            }

            // Wait a bit
            Timer::new(Duration::from_millis(10)).await;
        }
    }

    /// Get all received events.
    pub async fn events(&self) -> Vec<ServerMessage> {
        self.process_pending_messages().await;
        self.events.borrow().clone()
    }

    /// Clear the events buffer.
    pub async fn clear_events(&self) {
        self.events.borrow_mut().clear();
    }

    /// Get raw messages in receive order.
    pub async fn get_raw_messages(&self) -> Vec<ServerMessage> {
        self.process_pending_messages().await;
        std::mem::take(&mut *self.raw_messages.borrow_mut())
    }

    /// Clear raw messages.
    pub async fn clear_raw_messages(&self) {
        self.raw_messages.borrow_mut().clear();
    }

    /// Force-evict all promoted paths in the database (for testing eviction edge cases).
    pub async fn force_evict_all(&self) {
        if let Some(db_handle) = &self.db_handle {
            db_handle.force_evict_all();
            // Give the database time to process the eviction
            Timer::new(Duration::from_millis(50)).await;
        }
    }

    /// Disconnect the client gracefully.
    pub async fn disconnect(&mut self) {
        if let Some(db_handle) = self.db_handle.take() {
            db_handle.client_disconnected(self.id.clone());
            // Give the database a moment to process the disconnect
            Timer::new(Duration::from_millis(10)).await;
        }
    }
}

// =============================================================================
// Query Options
// =============================================================================

/// Query options for subscriptions.
#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    pub order_by: Option<String>,
    pub order_by_child: Option<String>,
    pub limit_to_first: Option<i32>,
    pub limit_to_last: Option<i32>,
    pub start_at: Option<Value>,
    pub start_at_key: Option<String>,
    pub end_at: Option<Value>,
    pub end_at_key: Option<String>,
    pub equal_to: Option<Value>,
    pub equal_to_key: Option<String>,
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Generate a test JWT token (for tests that need real token validation).
pub fn generate_test_token(uid: &str, claims: Option<HashMap<String, Value>>) -> String {
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};

    let mut payload = json!({
        "uid": uid,
        "iat": chrono::Utc::now().timestamp(),
        "exp": chrono::Utc::now().timestamp() + 3600,
    });

    if let Some(extra_claims) = claims
        && let Value::Object(ref mut map) = payload
    {
        for (k, v) in extra_claims {
            map.insert(k, v);
        }
    }

    let header = Header::new(Algorithm::HS256);
    encode(
        &header,
        &payload,
        &EncodingKey::from_secret(TEST_SECRET.as_bytes()),
    )
    .unwrap()
}
/// Compute JCS (RFC 8785) hash of a JSON value.
pub fn compute_jcs_hash(value: &Value) -> String {
    use sha2::{Digest, Sha256};

    let canonical = serde_json_canonicalizer::to_vec(value).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(&canonical);
    let result = hasher.finalize();
    hex::encode(result)
}
