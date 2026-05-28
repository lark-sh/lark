//! Proxy transport for Glommio thread-per-core model.
//!
//! Each core runs its own TCP listener with SO_REUSEPORT and handles
//! proxy connections independently with no shared state.

use crate::transport::firebase_adapter::FirebaseAdapter;
use crate::transport::protocol::{
    ConfigPushMessage, ConfigRequestMessage, DatabaseLoadedMessage, DatabaseUnloadedMessage,
    EvictDatabaseMessage, HeartbeatAckMessage, HeartbeatMessage, HelloAckMessage, HelloMessage,
    MAX_MESSAGE_SIZE, PROTOCOL_VERSION, ShutdownMessage, data_flag, disconnect_reason, proxy_msg,
    server_msg,
};
use bytes::{BufMut, Bytes, BytesMut};
use futures::FutureExt;
use futures::future::poll_immediate;
use futures::io::{AsyncReadExt, AsyncWriteExt};
use glommio::channels::local_channel::{self, LocalReceiver, LocalSender};
use glommio::net::TcpListener;
use glommio::timer::Timer;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, error, info, trace, warn};

// =============================================================================
// Constants
// =============================================================================

/// Maximum clients per proxy connection
const MAX_CLIENTS_PER_PROXY: usize = 65536;

/// Outbox buffer size per proxy connection
const OUTBOX_SIZE: usize = 524288;

/// Batch parameters for outgoing writes
const BATCH_MAX_SIZE: usize = 2 * 1024 * 1024; // 2MB
const BATCH_INTERVAL_MS: u64 = 3;

/// Heartbeat interval
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

/// Minimum payload size to consider compression (1KB)
/// Smaller payloads may actually get larger after compression overhead
const COMPRESSION_THRESHOLD: usize = 1024;

// =============================================================================
// Auth Info (from proxy)
// =============================================================================

/// Auth information validated by the proxy.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProxyAuthInfo {
    /// User ID (empty for anonymous)
    pub uid: String,

    /// Provider: "anonymous", "google", "password", "custom", etc.
    pub provider: String,

    /// Custom claims (auth.token in rules)
    #[serde(default)]
    pub claims: HashMap<String, serde_json::Value>,

    /// True for coordinator-signed admin tokens
    #[serde(default)]
    pub is_admin: bool,
}

// =============================================================================
// Error Types
// =============================================================================

/// Error sending to a client
#[derive(Debug, Clone, Copy)]
pub enum SendError {
    Closed,
    ChannelClosed,
    BufferFull,
}

// =============================================================================
// Outgoing Message
// =============================================================================

/// Message queued for sending to proxy
#[derive(Debug)]
pub struct OutgoingMessage {
    pub client_id: u32,
    pub msg_type: u8,
    pub flags: u8,
    pub data: Bytes,
}

// =============================================================================
// Virtual Client
// =============================================================================

/// A virtual client multiplexed through a proxy connection.
pub struct VirtualClient {
    /// Unique ID: proxy_{protocol}_{proxyAddr}_{clientID}
    pub id: String,

    /// Numeric ID within the proxy connection
    pub client_id: u32,

    /// Protocol: WebSocket or WebTransport
    pub protocol: u8,

    /// Project ID from CONNECT
    pub project_id: String,

    /// Database ID from CONNECT
    pub database_id: String,

    /// Metadata from CONNECT
    pub metadata: HashMap<String, serde_json::Value>,

    /// Auth info from proxy
    pub proxy_auth: Option<ProxyAuthInfo>,

    /// Firebase adapter for protocol translation (None if not a Firebase client)
    firebase_adapter: Option<RefCell<FirebaseAdapter>>,

    /// Channel to send messages to this client (wrapped in Rc since LocalSender is !Clone)
    outbox: Rc<LocalSender<OutgoingMessage>>,

    /// Whether the client is closed
    closed: AtomicBool,
}

impl VirtualClient {
    /// Compress data if above threshold, returning (data, compressed_flag)
    fn maybe_compress(data: Bytes) -> (Bytes, bool) {
        if data.len() < COMPRESSION_THRESHOLD {
            return (data, false);
        }

        // Compress with zstd at level 1 (fast compression)
        match zstd::bulk::compress(&data, 1) {
            Ok(compressed) => {
                // Only use compression if it actually reduces size
                if compressed.len() < data.len() {
                    (Bytes::from(compressed), true)
                } else {
                    (data, false)
                }
            }
            Err(_) => {
                // Compression failed, send uncompressed
                (data, false)
            }
        }
    }

    /// Send data to this client.
    pub async fn send(&self, data: Bytes, volatile: bool) -> Result<(), SendError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(SendError::Closed);
        }

        let (data, compressed) = Self::maybe_compress(data);

        let mut flags = if volatile {
            data_flag::UNRELIABLE
        } else {
            data_flag::RELIABLE
        };

        if compressed {
            flags |= data_flag::COMPRESSED;
        }

        let msg = OutgoingMessage {
            client_id: self.client_id,
            msg_type: server_msg::DATA,
            flags,
            data,
        };

        self.outbox
            .try_send(msg)
            .map_err(|_| SendError::ChannelClosed)
    }

    /// Send data synchronously (non-blocking, may drop if full).
    pub fn try_send(&self, data: Bytes, volatile: bool) -> Result<(), SendError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(SendError::Closed);
        }

        let (data, compressed) = Self::maybe_compress(data);

        let mut flags = if volatile {
            data_flag::UNRELIABLE
        } else {
            data_flag::RELIABLE
        };

        if compressed {
            flags |= data_flag::COMPRESSED;
        }

        let msg = OutgoingMessage {
            client_id: self.client_id,
            msg_type: server_msg::DATA,
            flags,
            data,
        };

        // `volatile` has already taken effect via the UNRELIABLE wire flag above.
        // On a full outbox we report BufferFull regardless: every caller drops the
        // message anyway, and reporting the failure keeps drop logging and the
        // "subscribers notified" counts accurate.
        match self.outbox.try_send(msg) {
            Ok(()) => Ok(()),
            Err(_) => Err(SendError::BufferFull),
        }
    }

    /// Close this client connection.
    pub fn close(&self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return; // Already closed
        }

        let msg = OutgoingMessage {
            client_id: self.client_id,
            msg_type: server_msg::CLOSE,
            flags: disconnect_reason::CLEAN,
            data: Bytes::new(),
        };

        let _ = self.outbox.try_send(msg);
    }

    /// Check if this client is closed.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    /// Check if this is a Firebase client.
    pub fn is_firebase(&self) -> bool {
        self.firebase_adapter.is_some()
    }

    /// Get the Firebase adapter if this is a Firebase client.
    pub fn firebase_adapter(&self) -> Option<&RefCell<FirebaseAdapter>> {
        self.firebase_adapter.as_ref()
    }

    /// Get a unique identifier for this client's outbox.
    /// All clients on the same proxy connection share the same outbox.
    pub fn outbox_id(&self) -> usize {
        Rc::as_ptr(&self.outbox) as usize
    }

    /// Test-only constructor. The outbox channel is created but no reader is
    /// attached, so any `send`/`try_send` call will block or fail — tests that
    /// only need to drive handler logic (not message I/O) can use this.
    #[cfg(test)]
    pub(crate) fn new_for_test(
        client_id: u32,
        project_id: impl Into<String>,
        database_id: impl Into<String>,
    ) -> Self {
        let (tx, _rx) = local_channel::new_bounded(1);
        Self {
            id: format!("test_{}", client_id),
            client_id,
            protocol: 0,
            project_id: project_id.into(),
            database_id: database_id.into(),
            metadata: HashMap::new(),
            proxy_auth: None,
            firebase_adapter: None,
            outbox: Rc::new(tx),
            closed: AtomicBool::new(false),
        }
    }

    /// Send a broadcast message with a pre-built payload.
    /// The payload should already be in wire format:
    /// `[ClientCount:4][[ClientID:4][Tag:4]...][MsgLen:4][MsgBytes...]`
    pub fn try_send_broadcast_raw(&self, payload: &[u8], flags: u8) -> Result<(), SendError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(SendError::Closed);
        }

        let msg = OutgoingMessage {
            client_id: 0,
            msg_type: server_msg::BROADCAST,
            flags,
            data: Bytes::copy_from_slice(payload),
        };

        self.outbox
            .try_send(msg)
            .map_err(|_| SendError::ChannelClosed)
    }
}

// =============================================================================
// Connect Result
// =============================================================================

/// Result of handling a client connection.
#[derive(Debug, Default)]
pub struct ConnectResult {
    /// True if we need to request config for this project.
    pub needs_config: bool,
    /// If a new database was loaded, contains (project_id, database_id).
    pub database_loaded: Option<(String, String)>,
}

/// Information about a database that was unloaded.
#[derive(Debug, Clone)]
pub struct UnloadNotification {
    pub project_id: String,
    pub database_id: String,
    pub reason: u8,
    pub ephemeral: bool,
}

// =============================================================================
// Proxy Handler Trait
// =============================================================================

/// Handler for proxy events
pub trait ProxyHandler {
    /// Called when a client connects.
    /// Returns a ConnectResult with information about config needs and database loading.
    fn on_connect(self: &Rc<Self>, client: Rc<VirtualClient>) -> ConnectResult;

    /// Called when a client sends a message
    fn on_message(
        &self,
        client: Rc<VirtualClient>,
        data: Vec<u8>,
        timestamps: Option<crate::metrics::MessageTimestamps>,
    );

    /// Called when a client disconnects
    fn on_disconnect(&self, client: Rc<VirtualClient>);

    /// Called when client auth changes (late auth)
    fn on_auth_changed(&self, client: Rc<VirtualClient>, auth: ProxyAuthInfo);

    /// Called when config is pushed from the proxy
    fn on_config_push(
        self: &Rc<Self>,
        project_id: &str,
        config: crate::transport::protocol::ProjectConfig,
    );

    /// Called when the proxy requests that a database be evicted from this core.
    /// `flags` carries bits from `protocol::evict_flag` (e.g. `PURGE_DATA`).
    fn on_evict_database(&self, _project_id: &str, _database_id: &str, _flags: u8) {
        // Default implementation: no-op.
    }

    /// Get pending database unload notifications.
    /// Called during heartbeat to send notifications to the proxy.
    fn take_pending_unloads(&self) -> Vec<UnloadNotification> {
        Vec::new() // Default implementation
    }
}

// =============================================================================
// Proxy Connection
// =============================================================================

/// A single proxy connection handling multiplexed clients.
pub struct ProxyConnection<H: ProxyHandler> {
    /// Handler for client events
    handler: Rc<H>,

    /// Core ID this connection belongs to
    core_id: usize,

    /// Total number of cores
    nr_cores: usize,

    /// Connected virtual clients: client_id -> VirtualClient
    clients: HashMap<u32, Rc<VirtualClient>>,

    /// Outbox channel for sending messages (wrapped in Rc since LocalSender is !Clone)
    outbox_tx: Rc<LocalSender<OutgoingMessage>>,
    outbox_rx: Option<LocalReceiver<OutgoingMessage>>,

    /// Connection address (for logging)
    remote_addr: String,

    /// Shared secret (SERVER_SECRET) the proxy must prove knowledge of via the
    /// HELLO_AUTH HMAC before this connection is trusted.
    server_secret: Rc<String>,

    /// Random nonce sent in HELLO_ACK; set in `handle_hello`, consumed by
    /// `handle_hello_auth` to verify the proxy's HMAC.
    hello_nonce: Option<[u8; 32]>,

    /// Whether the HELLO/HELLO_AUTH handshake is complete. Until this is true,
    /// every message type other than HELLO/HELLO_AUTH is rejected.
    handshake_complete: bool,

    /// Metrics
    messages_received: u64,
    messages_sent: u64,

    /// Write path metrics (for debug timing)
    total_flushes: u64,
    total_flush_bytes: u64,
    total_flush_msgs: u64,
    max_flush_latency_us: u64,
    total_flush_latency_us: u64,
    last_stats_log: Instant,

    /// Bucket counters for flush sizes
    /// Buckets: <256KB, 256KB-1MB, 1MB-2MB, >=2MB
    bucket_under_256k: u64,
    bucket_256k_1m: u64,
    bucket_1m_2m: u64,
    bucket_over_2m: u64,
}

impl<H: ProxyHandler + 'static> ProxyConnection<H> {
    pub fn new(
        handler: Rc<H>,
        core_id: usize,
        nr_cores: usize,
        remote_addr: String,
        server_secret: Rc<String>,
    ) -> Self {
        let (outbox_tx, outbox_rx) = local_channel::new_bounded(OUTBOX_SIZE);

        Self {
            handler,
            core_id,
            nr_cores,
            clients: HashMap::new(),
            outbox_tx: Rc::new(outbox_tx),
            outbox_rx: Some(outbox_rx),
            remote_addr,
            server_secret,
            hello_nonce: None,
            handshake_complete: false,
            messages_received: 0,
            messages_sent: 0,
            total_flushes: 0,
            total_flush_bytes: 0,
            total_flush_msgs: 0,
            max_flush_latency_us: 0,
            total_flush_latency_us: 0,
            last_stats_log: Instant::now(),
            bucket_under_256k: 0,
            bucket_256k_1m: 0,
            bucket_1m_2m: 0,
            bucket_over_2m: 0,
        }
    }

    /// Run the proxy connection, handling reads and writes with batching.
    ///
    /// Uses interleaved read/write with select! to ensure both directions work.
    /// Outgoing messages are batched up to BATCH_MAX_SIZE or BATCH_INTERVAL_MS
    /// to reduce syscall overhead.
    pub async fn run(mut self, mut stream: glommio::net::TcpStream) -> io::Result<()> {
        trace!(
            "Proxy connection from {} on core {}",
            self.remote_addr, self.core_id
        );

        // Take the outbox receiver for writing
        let outbox_rx = self.outbox_rx.take().unwrap();

        // Buffers
        let mut header_buf = [0u8; 4];
        let mut payload_buf = BytesMut::with_capacity(64 * 1024);
        let mut write_buf = BytesMut::with_capacity(BATCH_MAX_SIZE);
        let mut last_write_flush = Instant::now();
        // Initialize to past so first heartbeat sends immediately after handshake
        let mut last_heartbeat = Instant::now() - HEARTBEAT_INTERVAL;

        // Track partial header read state
        let mut header_read = 0;
        let mut reading_payload = false;
        let mut payload_length = 0;
        let mut payload_read = 0;

        // Track messages in current batch
        let mut batch_msg_count: u64 = 0;

        loop {
            // Collect any immediately-ready outgoing messages into buffer first
            // (ensures HELLO_ACK is queued before HEARTBEAT)
            // Drain immediately-ready messages; stops on None (closed) or
            // Some(None) (nothing pending right now).
            while let Some(Some(msg)) = poll_immediate(outbox_rx.recv()).await {
                self.encode_outgoing(&mut write_buf, &msg);
                batch_msg_count += 1;
                if write_buf.len() >= BATCH_MAX_SIZE {
                    break;
                }
            }

            // Send heartbeat every 10 seconds (only after handshake is complete)
            if self.handshake_complete && last_heartbeat.elapsed() >= HEARTBEAT_INTERVAL {
                // Send pending database unload notifications first
                for unload in self.handler.take_pending_unloads() {
                    trace!(
                        "Sending DATABASE_UNLOADED for {}/{} (reason={}, ephemeral={})",
                        unload.project_id, unload.database_id, unload.reason, unload.ephemeral
                    );
                    let unloaded = DatabaseUnloadedMessage {
                        project_id: unload.project_id,
                        database_id: unload.database_id,
                        reason: unload.reason,
                        ephemeral: unload.ephemeral,
                    };
                    let msg = OutgoingMessage {
                        client_id: 0,
                        msg_type: server_msg::DATABASE_UNLOADED,
                        flags: 0,
                        data: unloaded.encode().to_vec().into(),
                    };
                    self.encode_outgoing(&mut write_buf, &msg);
                }

                let heartbeat = HeartbeatMessage {
                    load: 0, // TODO: compute actual CPU load
                    client_count: self.clients.len() as u32,
                    memory_mb: 0, // TODO: compute actual memory usage
                };
                let msg = OutgoingMessage {
                    client_id: 0,
                    msg_type: server_msg::HEARTBEAT,
                    flags: 0,
                    data: heartbeat.encode().to_vec().into(),
                };
                self.encode_outgoing(&mut write_buf, &msg);
                last_heartbeat = Instant::now();
                trace!("Sent HEARTBEAT to proxy (clients={})", self.clients.len());
            }

            // Determine if we need to flush writes
            let need_flush = !write_buf.is_empty()
                && (write_buf.len() >= BATCH_MAX_SIZE
                    || last_write_flush.elapsed() >= Duration::from_millis(BATCH_INTERVAL_MS));

            // If we have pending writes to flush, do that first
            if need_flush {
                let flush_start = Instant::now();
                let flush_bytes = write_buf.len() as u64;
                stream.write_all(&write_buf).await?;
                let flush_latency_us = flush_start.elapsed().as_micros() as u64;

                // Update stats
                self.total_flushes += 1;
                self.total_flush_bytes += flush_bytes;
                self.total_flush_msgs += batch_msg_count;
                self.messages_sent += batch_msg_count;
                self.total_flush_latency_us += flush_latency_us;
                if flush_latency_us > self.max_flush_latency_us {
                    self.max_flush_latency_us = flush_latency_us;
                }

                // Track bucket sizes
                const KB_256: u64 = 256 * 1024;
                const MB_1: u64 = 1024 * 1024;
                const MB_2: u64 = 2 * 1024 * 1024;
                if flush_bytes < KB_256 {
                    self.bucket_under_256k += 1;
                } else if flush_bytes < MB_1 {
                    self.bucket_256k_1m += 1;
                } else if flush_bytes < MB_2 {
                    self.bucket_1m_2m += 1;
                } else {
                    self.bucket_over_2m += 1;
                }

                write_buf.clear();
                batch_msg_count = 0;
                last_write_flush = Instant::now();
            }

            // Log write stats periodically (every 1 second)
            if self.last_stats_log.elapsed() >= Duration::from_secs(1) {
                let elapsed_secs = self.last_stats_log.elapsed().as_secs_f64();
                if self.total_flushes > 0 {
                    let avg_latency_us =
                        self.total_flush_latency_us as f64 / self.total_flushes as f64;
                    let throughput_mbps =
                        (self.total_flush_bytes as f64 / elapsed_secs) / (1024.0 * 1024.0);
                    trace!(
                        "[Proxy Write Stats] core={} flushes={} buckets=(<256K:{} 256K-1M:{} 1M-2M:{} >=2M:{}) \
                         avg_latency={:.0}µs max_latency={}µs throughput={:.1}MB/s total_bytes={}",
                        self.core_id,
                        self.total_flushes,
                        self.bucket_under_256k,
                        self.bucket_256k_1m,
                        self.bucket_1m_2m,
                        self.bucket_over_2m,
                        avg_latency_us,
                        self.max_flush_latency_us,
                        throughput_mbps,
                        self.total_flush_bytes
                    );
                }
                // Reset stats
                self.total_flushes = 0;
                self.total_flush_bytes = 0;
                self.total_flush_msgs = 0;
                self.max_flush_latency_us = 0;
                self.total_flush_latency_us = 0;
                self.bucket_under_256k = 0;
                self.bucket_256k_1m = 0;
                self.bucket_1m_2m = 0;
                self.bucket_over_2m = 0;
                self.last_stats_log = Instant::now();
            }

            // Set up timer for write batching
            let batch_timer = Timer::new(Duration::from_millis(BATCH_INTERVAL_MS));

            // Use select! to either read from TCP or timeout for write flushing
            futures::select! {
                // Try to read from TCP
                read_result = self.read_one_message(
                    &mut stream,
                    &mut header_buf,
                    &mut header_read,
                    &mut payload_buf,
                    &mut reading_payload,
                    &mut payload_length,
                    &mut payload_read,
                ).fuse() => {
                    match read_result {
                        Ok(Some((msg_type, payload))) => {
                            self.handle_message(msg_type, payload).await?;
                            self.messages_received += 1;

                            // Yield after CONNECT to let spawned database tasks run
                            // (rapid CONNECT handling otherwise starves them), and also
                            // yield periodically for other message types.
                            if msg_type == proxy_msg::CONNECT
                                || self.messages_received.is_multiple_of(100)
                            {
                                glommio::yield_if_needed().await;
                            }
                        }
                        Ok(None) => {
                            // Connection closed
                            break;
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
                }
                // Write batch timer expired
                _ = batch_timer.fuse() => {
                    // Just continue - the loop will flush if needed
                }
            }
        }

        // Flush any remaining writes before closing
        if !write_buf.is_empty() {
            stream.write_all(&write_buf).await?;
        }

        // Cleanup all clients
        for (_, client) in self.clients.drain() {
            self.handler.on_disconnect(client);
        }

        trace!(
            "Proxy connection closed from {} (recv={}, sent={})",
            self.remote_addr, self.messages_received, self.messages_sent
        );

        Ok(())
    }

    /// Read one complete message from the stream.
    /// Returns Ok(Some((type, payload))) on success, Ok(None) on connection close.
    #[allow(clippy::too_many_arguments)]
    async fn read_one_message<'a>(
        &self,
        stream: &mut glommio::net::TcpStream,
        header_buf: &mut [u8; 4],
        header_read: &mut usize,
        payload_buf: &'a mut BytesMut,
        reading_payload: &mut bool,
        payload_length: &mut usize,
        payload_read: &mut usize,
    ) -> io::Result<Option<(u8, &'a [u8])>> {
        // Read header if not already reading payload
        if !*reading_payload {
            while *header_read < 4 {
                let n = stream.read(&mut header_buf[*header_read..]).await?;
                if n == 0 {
                    return Ok(None); // Connection closed
                }
                *header_read += n;
            }

            // Parse length
            let length = u32::from_be_bytes(*header_buf) as usize;

            if length > MAX_MESSAGE_SIZE {
                error!("Message too large: {} bytes", length);
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "message too large",
                ));
            }

            if length < 1 {
                error!("Message too small: {} bytes", length);
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "message too small",
                ));
            }

            // Prepare for payload read
            payload_buf.clear();
            payload_buf.resize(length, 0);
            *payload_length = length;
            *payload_read = 0;
            *reading_payload = true;
            *header_read = 0; // Reset for next message
        }

        // Read payload
        while *payload_read < *payload_length {
            let n = stream.read(&mut payload_buf[*payload_read..]).await?;
            if n == 0 {
                return Ok(None); // Connection closed
            }
            *payload_read += n;
        }

        // Message complete
        *reading_payload = false;
        let msg_type = payload_buf[0];

        Ok(Some((msg_type, &payload_buf[1..])))
    }

    /// Encode an outgoing message into the write buffer.
    fn encode_outgoing(&self, buf: &mut BytesMut, msg: &OutgoingMessage) {
        // Control messages (HELLO_ACK, HEARTBEAT, DATABASE_LOADED, etc.) use a simpler format
        // Data messages (DATA, CLOSE) include ClientID and Flags
        // BROADCAST has Flags but no ClientID
        let is_control_msg = matches!(
            msg.msg_type,
            server_msg::HELLO_ACK
                | server_msg::HEARTBEAT
                | server_msg::DATABASE_LOADED
                | server_msg::DATABASE_UNLOADED
                | server_msg::CONFIG_REQUEST
        );

        if is_control_msg {
            // Control message format: [Length:4][Type:1][Data:variable]
            let payload_len = 1 + msg.data.len();
            buf.put_u32(payload_len as u32);
            buf.put_u8(msg.msg_type);
            buf.extend_from_slice(&msg.data);
        } else if msg.msg_type == server_msg::BROADCAST {
            // BROADCAST format: [Length:4][Type:1][Flags:1][Payload:variable]
            let payload_len = 1 + 1 + msg.data.len();
            buf.put_u32(payload_len as u32);
            buf.put_u8(msg.msg_type);
            buf.put_u8(msg.flags);
            buf.extend_from_slice(&msg.data);
        } else {
            // Data message format: [Length:4][Type:1][ClientID:4][Flags:1][Data:variable]
            let payload_len = 1 + 4 + 1 + msg.data.len();
            buf.put_u32(payload_len as u32);
            buf.put_u8(msg.msg_type);
            buf.put_u32(msg.client_id);
            buf.put_u8(msg.flags);
            buf.extend_from_slice(&msg.data);
        }
    }

    async fn handle_message(&mut self, msg_type: u8, payload: &[u8]) -> io::Result<()> {
        trace!(
            "Received message: type=0x{:02x}, payload_len={}",
            msg_type,
            payload.len()
        );

        // Fail closed: until the proxy has proven knowledge of SERVER_SECRET via
        // HELLO → HELLO_AUTH, the only messages we accept are those two. Anything
        // else (CONNECT, DATA, AUTH_CHANGED, …) from an unauthenticated peer is
        // rejected, which closes the connection. This is what stops an attacker
        // who can merely reach the port from impersonating a trusted gateway.
        if !self.handshake_complete
            && msg_type != proxy_msg::HELLO
            && msg_type != proxy_msg::HELLO_AUTH
        {
            warn!(
                "Rejecting message type 0x{:02x} from {} before authenticated handshake",
                msg_type, self.remote_addr
            );
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "message received before authenticated handshake",
            ));
        }

        match msg_type {
            proxy_msg::HELLO => {
                self.handle_hello(payload).await?;
            }
            proxy_msg::HELLO_AUTH => {
                self.handle_hello_auth(payload).await?;
            }
            proxy_msg::CONNECT => {
                self.handle_connect(payload).await?;
            }
            proxy_msg::DATA => {
                self.handle_data(payload).await?;
            }
            proxy_msg::DISCONNECT => {
                self.handle_disconnect(payload).await?;
            }
            proxy_msg::AUTH_CHANGED => {
                self.handle_auth_changed(payload).await?;
            }
            proxy_msg::HEARTBEAT_ACK => {
                self.handle_heartbeat_ack(payload).await?;
            }
            proxy_msg::CONFIG_PUSH => {
                self.handle_config_push(payload).await?;
            }
            proxy_msg::EVICT_DATABASE => {
                self.handle_evict_database(payload).await?;
            }
            proxy_msg::SHUTDOWN => {
                self.handle_shutdown(payload).await?;
            }
            _ => {
                warn!("Unknown message type: 0x{:02x}", msg_type);
            }
        }
        Ok(())
    }

    async fn handle_hello(&mut self, payload: &[u8]) -> io::Result<()> {
        let hello = HelloMessage::decode(payload)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid HELLO"))?;

        trace!(
            "HELLO from proxy: version={} on core {}",
            hello.proxy_version, self.core_id
        );

        // Generate a fresh per-connection nonce. The proxy must echo back
        // HMAC-SHA256(SERVER_SECRET, nonce) in a HELLO_AUTH before we trust it.
        let mut nonce = [0u8; 32];
        getrandom::getrandom(&mut nonce)
            .map_err(|e| io::Error::other(format!("nonce generation failed: {e}")))?;
        self.hello_nonce = Some(nonce);

        // Send HELLO_ACK (carrying the nonce). Handshake is NOT complete yet —
        // it completes only after a valid HELLO_AUTH.
        let ack = HelloAckMessage {
            core_id: self.core_id as u8,
            nr_cores: self.nr_cores as u8,
            server_version: PROTOCOL_VERSION,
            nonce,
        };

        let msg = OutgoingMessage {
            client_id: 0,
            msg_type: server_msg::HELLO_ACK,
            flags: 0,
            data: ack.encode().to_vec().into(),
        };
        let _ = self.outbox_tx.try_send(msg);

        Ok(())
    }

    /// Verify the proxy's HELLO_AUTH: `HMAC-SHA256(SERVER_SECRET, nonce)` over the
    /// nonce we sent in HELLO_ACK. On success the handshake is complete and the
    /// connection is trusted; on any mismatch we error, which closes the socket.
    async fn handle_hello_auth(&mut self, payload: &[u8]) -> io::Result<()> {
        let nonce = self
            .hello_nonce
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "HELLO_AUTH before HELLO"))?;

        let mut mac = <Hmac<Sha256>>::new_from_slice(self.server_secret.as_bytes())
            .map_err(|e| io::Error::other(format!("hmac init: {e}")))?;
        mac.update(&nonce);

        // verify_slice is constant-time.
        if mac.verify_slice(payload).is_err() {
            warn!(
                "Proxy handshake auth FAILED from {} — rejecting connection (bad SERVER_SECRET?)",
                self.remote_addr
            );
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "proxy handshake authentication failed",
            ));
        }

        self.handshake_complete = true;
        self.hello_nonce = None; // single-use
        trace!("Proxy handshake authenticated from {}", self.remote_addr);
        Ok(())
    }

    async fn handle_connect(&mut self, payload: &[u8]) -> io::Result<()> {
        if payload.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "CONNECT too short",
            ));
        }

        let client_id = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);

        trace!(
            "CONNECT received: client_id={}, payload_len={}, first_bytes={:02x?}",
            client_id,
            payload.len(),
            &payload[..std::cmp::min(32, payload.len())]
        );

        if self.clients.contains_key(&client_id) {
            warn!("Duplicate client ID: {}", client_id);
            return Ok(());
        }

        if self.clients.len() >= MAX_CLIENTS_PER_PROXY {
            warn!("Too many clients on this connection");
            return Ok(());
        }

        // Parse CONNECT payload
        let connect_data = &payload[4..];
        trace!(
            "CONNECT data: len={}, first_bytes={:02x?}",
            connect_data.len(),
            &connect_data[..std::cmp::min(32, connect_data.len())]
        );
        let (protocol, project_id, database_id, metadata, auth) =
            Self::parse_connect_payload(connect_data)?;

        // Determine if this is a Firebase client (check metadata["firebase"] bool)
        let is_firebase = metadata
            .get("firebase")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let firebase_adapter = if is_firebase {
            // Get hostname from metadata (coordinator should provide this)
            let hostname = metadata
                .get("hostname")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            // Create Firebase adapter for protocol translation
            let mut adapter = FirebaseAdapter::new(&project_id, &hostname);

            // The proxy/coordinator already sent the hello and determined the database,
            // so mark as already joined (no lazy join needed)
            adapter.set_joined();

            trace!(
                "Firebase client connected: project={}, db={}",
                project_id, database_id
            );

            Some(RefCell::new(adapter))
        } else {
            None
        };

        // Create unique client ID string
        let client_id_str = format!(
            "proxy_{}_{}_{}_{}",
            protocol, self.remote_addr, self.core_id, client_id
        );

        // Log initial auth
        if let Some(ref a) = auth {
            trace!(
                "CONNECT client {} initial auth: uid={}, provider={}, is_admin={}",
                client_id_str, a.uid, a.provider, a.is_admin
            );
        } else {
            trace!("CONNECT client {} initial auth: anonymous", client_id_str);
        }

        let client = Rc::new(VirtualClient {
            id: client_id_str,
            client_id,
            protocol,
            project_id,
            database_id,
            metadata,
            proxy_auth: auth,
            firebase_adapter,
            outbox: self.outbox_tx.clone(),
            closed: AtomicBool::new(false),
        });

        self.clients.insert(client_id, client.clone());

        let result = self.handler.on_connect(client.clone());

        // Send CONFIG_REQUEST if we don't have config for this project
        if result.needs_config {
            trace!("Sending CONFIG_REQUEST for project {}", client.project_id);
            self.request_config(&client.project_id);
        }

        // Send DATABASE_LOADED if a new database was created
        if let Some((project_id, database_id)) = result.database_loaded {
            trace!("Sending DATABASE_LOADED for {}/{}", project_id, database_id);
            self.notify_database_loaded(&project_id, &database_id);
        }

        Ok(())
    }

    async fn handle_data(&mut self, payload: &[u8]) -> io::Result<()> {
        if payload.len() < 4 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "DATA too short"));
        }

        // Create timestamps for latency tracking (if sampling enabled)
        let timestamps = crate::metrics::maybe_create_timestamps();

        let client_id = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);

        let client = match self.clients.get(&client_id) {
            Some(c) => c.clone(),
            None => {
                trace!("DATA for unknown client: {}", client_id);
                return Ok(());
            }
        };

        let data = payload[4..].to_vec();
        self.handler.on_message(client, data, timestamps);

        Ok(())
    }

    async fn handle_disconnect(&mut self, payload: &[u8]) -> io::Result<()> {
        if payload.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "DISCONNECT too short",
            ));
        }

        let client_id = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);

        if let Some(client) = self.clients.remove(&client_id) {
            client.closed.store(true, Ordering::SeqCst);
            self.handler.on_disconnect(client);
        }

        Ok(())
    }

    async fn handle_auth_changed(&mut self, payload: &[u8]) -> io::Result<()> {
        if payload.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "AUTH_CHANGED too short",
            ));
        }

        let client_id = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);

        let client = match self.clients.get(&client_id) {
            Some(c) => c.clone(),
            None => {
                trace!("AUTH_CHANGED for unknown client: {}", client_id);
                return Ok(());
            }
        };

        // Parse auth JSON
        let auth_json = &payload[4..];
        let auth: ProxyAuthInfo = serde_json::from_slice(auth_json)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        trace!(
            "AUTH_CHANGED for client {}: uid={}, provider={}, is_admin={}",
            client.id, auth.uid, auth.provider, auth.is_admin
        );

        self.handler.on_auth_changed(client, auth);

        Ok(())
    }

    async fn handle_heartbeat_ack(&mut self, payload: &[u8]) -> io::Result<()> {
        let ack = HeartbeatAckMessage::decode(payload);
        if let Some(ack) = ack {
            trace!("HEARTBEAT_ACK: server_time={}", ack.server_time);
        }
        Ok(())
    }

    async fn handle_config_push(&mut self, payload: &[u8]) -> io::Result<()> {
        let push = ConfigPushMessage::decode(payload)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid CONFIG_PUSH"))?;

        debug!(
            "CONFIG_PUSH received: project={} version={} rules={} secret={}",
            push.project_id,
            push.config.config_version,
            push.config.rules.is_some(),
            push.config.secret_key.is_some()
        );

        // Pass config to handler to process pending connects
        self.handler.on_config_push(&push.project_id, push.config);

        Ok(())
    }

    async fn handle_evict_database(&mut self, payload: &[u8]) -> io::Result<()> {
        let evict = EvictDatabaseMessage::decode(payload)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid EVICT_DATABASE"))?;

        info!(
            "EVICT_DATABASE: {}/{} (flags=0x{:02x})",
            evict.project_id, evict.database_id, evict.flags
        );

        self.handler
            .on_evict_database(&evict.project_id, &evict.database_id, evict.flags);

        Ok(())
    }

    async fn handle_shutdown(&mut self, payload: &[u8]) -> io::Result<()> {
        let shutdown = ShutdownMessage::decode(payload);
        let grace_period = shutdown.map(|s| s.grace_period_secs).unwrap_or(30);

        info!("SHUTDOWN received, grace period: {}s", grace_period);

        // Shutdown handling will be done by CoreHandler

        Ok(())
    }

    /// Parse CONNECT payload into (protocol, project_id, database_id, metadata, auth)
    /// Format: [proto:1][proj_len:1][proj][db_len:1][db][meta_len:2][metadata][auth_len:2][auth]
    #[allow(clippy::type_complexity)] // decoded CONNECT fields tuple
    fn parse_connect_payload(
        data: &[u8],
    ) -> io::Result<(
        u8,
        String,
        String,
        HashMap<String, serde_json::Value>,
        Option<ProxyAuthInfo>,
    )> {
        if data.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "CONNECT payload too short",
            ));
        }

        let mut offset = 0;

        // Protocol byte
        let protocol = data[offset];
        offset += 1;

        // Project ID (1-byte length prefix)
        let project_len = data[offset] as usize;
        offset += 1;
        if data.len() < offset + project_len + 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "CONNECT project_id truncated",
            ));
        }
        let project_id = String::from_utf8_lossy(&data[offset..offset + project_len]).to_string();
        offset += project_len;

        // Database ID (1-byte length prefix)
        let database_len = data[offset] as usize;
        offset += 1;
        if data.len() < offset + database_len + 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "CONNECT database_id truncated",
            ));
        }
        let database_id = String::from_utf8_lossy(&data[offset..offset + database_len]).to_string();
        offset += database_len;

        // Metadata (2-byte length prefix)
        let mut metadata = HashMap::new();
        if data.len() >= offset + 2 {
            let meta_len = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
            offset += 2;

            if meta_len > 0 && data.len() >= offset + meta_len {
                let meta_json = &data[offset..offset + meta_len];
                if let Ok(parsed) =
                    serde_json::from_slice::<HashMap<String, serde_json::Value>>(meta_json)
                {
                    metadata = parsed;
                    trace!("CONNECT metadata: {} bytes", meta_len);
                }
                offset += meta_len;
            }
        }

        // Auth (2-byte length prefix)
        let mut auth = None;
        if data.len() >= offset + 2 {
            let auth_len = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
            offset += 2;

            if auth_len > 0 && data.len() >= offset + auth_len {
                let auth_json = &data[offset..offset + auth_len];
                if let Ok(json_str) = std::str::from_utf8(auth_json) {
                    trace!("CONNECT auth JSON: {}", json_str);
                }
                if let Ok(parsed_auth) = serde_json::from_slice::<ProxyAuthInfo>(auth_json) {
                    auth = Some(parsed_auth);
                }
            }
        }

        trace!(
            "Parsed CONNECT: protocol={}, project={}, database={}, has_auth={}",
            protocol,
            project_id,
            database_id,
            auth.is_some()
        );

        Ok((protocol, project_id, database_id, metadata, auth))
    }

    /// Send a config request for a project
    pub fn request_config(&self, project_id: &str) {
        let req = ConfigRequestMessage {
            project_id: project_id.to_string(),
        };

        let msg = OutgoingMessage {
            client_id: 0,
            msg_type: server_msg::CONFIG_REQUEST,
            flags: 0,
            data: req.encode().to_vec().into(),
        };
        let _ = self.outbox_tx.try_send(msg);
    }

    /// Notify that a database was loaded
    pub fn notify_database_loaded(&self, project_id: &str, database_id: &str) {
        let loaded = DatabaseLoadedMessage {
            project_id: project_id.to_string(),
            database_id: database_id.to_string(),
        };

        let msg = OutgoingMessage {
            client_id: 0,
            msg_type: server_msg::DATABASE_LOADED,
            flags: 0,
            data: loaded.encode().to_vec().into(),
        };
        let _ = self.outbox_tx.try_send(msg);
    }

    /// Notify that a database was unloaded
    pub fn notify_database_unloaded(
        &self,
        project_id: &str,
        database_id: &str,
        reason: u8,
        ephemeral: bool,
    ) {
        let unloaded = DatabaseUnloadedMessage {
            project_id: project_id.to_string(),
            database_id: database_id.to_string(),
            reason,
            ephemeral,
        };

        let msg = OutgoingMessage {
            client_id: 0,
            msg_type: server_msg::DATABASE_UNLOADED,
            flags: 0,
            data: unloaded.encode().to_vec().into(),
        };
        let _ = self.outbox_tx.try_send(msg);
    }

    /// Send a heartbeat
    pub fn send_heartbeat(&self, load: u16, client_count: u32, memory_mb: u32) {
        let heartbeat = HeartbeatMessage {
            load,
            client_count,
            memory_mb,
        };

        let msg = OutgoingMessage {
            client_id: 0,
            msg_type: server_msg::HEARTBEAT,
            flags: 0,
            data: heartbeat.encode().to_vec().into(),
        };
        let _ = self.outbox_tx.try_send(msg);
    }
}

// =============================================================================
// Proxy Listener
// =============================================================================

/// TCP listener for proxy connections on a single core.
pub struct ProxyListener<H: ProxyHandler + 'static> {
    handler: Rc<H>,
    core_id: usize,
    nr_cores: usize,
    port: u16,
    /// Host/interface to bind. "0.0.0.0" (IPv4) by default; "[::]" for IPv6.
    bind_host: String,
    /// Shared secret the proxy must authenticate with (HELLO_AUTH HMAC).
    server_secret: Rc<String>,
}

impl<H: ProxyHandler + 'static> ProxyListener<H> {
    pub fn new(
        handler: Rc<H>,
        core_id: usize,
        nr_cores: usize,
        port: u16,
        bind_host: String,
        server_secret: Rc<String>,
    ) -> Self {
        Self {
            handler,
            core_id,
            nr_cores,
            port,
            bind_host,
            server_secret,
        }
    }

    /// Run the listener, accepting connections.
    pub async fn run(self) -> io::Result<()> {
        let addr = format!("{}:{}", self.bind_host, self.port);

        // Create TCP listener with SO_REUSEPORT (implicit in Glommio)
        let listener = TcpListener::bind(&addr)?;

        info!(
            "Core {} listening on {} for proxy connections",
            self.core_id, addr
        );

        loop {
            match listener.accept().await {
                Ok(stream) => {
                    // Disable Nagle's algorithm for lower latency
                    if let Err(e) = stream.set_nodelay(true) {
                        warn!("Failed to set TCP_NODELAY: {}", e);
                    }

                    let remote_addr = stream
                        .peer_addr()
                        .map(|a| a.to_string())
                        .unwrap_or_else(|_| "unknown".to_string());

                    let conn = ProxyConnection::new(
                        self.handler.clone(),
                        self.core_id,
                        self.nr_cores,
                        remote_addr,
                        self.server_secret.clone(),
                    );

                    // Spawn connection handler
                    glommio::spawn_local(async move {
                        if let Err(e) = conn.run(stream).await {
                            // Client-disconnect kinds are expected lifecycle
                            // events (lark-edge cycles connections, clients
                            // restart, etc.) — log at debug so they're
                            // capturable with RUST_LOG=lark_server=debug but
                            // don't spam the default INFO output. Anything
                            // else is a real proxy-protocol bug we want to
                            // hear about.
                            match e.kind() {
                                io::ErrorKind::UnexpectedEof
                                | io::ErrorKind::ConnectionReset
                                | io::ErrorKind::ConnectionAborted
                                | io::ErrorKind::BrokenPipe => {
                                    debug!("Proxy connection closed: {}", e);
                                }
                                _ => {
                                    error!("Proxy connection error: {}", e);
                                }
                            }
                        }
                    })
                    .detach();
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                    Timer::new(Duration::from_millis(100)).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_proxy_auth_info_default() {
        let auth = ProxyAuthInfo::default();
        assert!(auth.uid.is_empty());
        assert!(auth.provider.is_empty());
        assert!(!auth.is_admin);
    }

    /// Locks the HELLO_AUTH HMAC to the same fixed vector asserted on the Go edge
    /// (`wire_test.go: TestServerAuthMACKnownAnswer`), so the two sides can't
    /// silently drift. HMAC-SHA256("lark-test-secret", nonce=bytes 0..31).
    #[test]
    fn test_hello_auth_hmac_known_answer() {
        let mut nonce = [0u8; 32];
        for (i, b) in nonce.iter_mut().enumerate() {
            *b = i as u8;
        }
        let mut mac = <Hmac<Sha256>>::new_from_slice(b"lark-test-secret").unwrap();
        mac.update(&nonce);
        let got = hex::encode(mac.finalize().into_bytes());
        assert_eq!(
            got,
            "d1e6900018c7d50930190b1577cc590f0821354b51afd79df6935cd08a82acbe"
        );
    }

    /// A wrong secret must not verify against the expected MAC (constant-time check).
    #[test]
    fn test_hello_auth_rejects_wrong_secret() {
        let nonce = [7u8; 32];
        let mut good = <Hmac<Sha256>>::new_from_slice(b"correct-secret").unwrap();
        good.update(&nonce);
        let expected = good.finalize().into_bytes();

        let mut bad = <Hmac<Sha256>>::new_from_slice(b"wrong-secret").unwrap();
        bad.update(&nonce);
        assert!(bad.verify_slice(&expected).is_err());
    }

    #[test]
    fn test_proxy_auth_info_serde() {
        let auth = ProxyAuthInfo {
            uid: "user-123".to_string(),
            provider: "google".to_string(),
            claims: HashMap::new(),
            is_admin: false,
        };

        let json = serde_json::to_string(&auth).unwrap();
        let decoded: ProxyAuthInfo = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.uid, "user-123");
        assert_eq!(decoded.provider, "google");
    }

    /// Build a minimal CONNECT payload carrying the given auth JSON, using the
    /// wire layout `parse_connect_payload` expects.
    fn connect_payload_with_auth(auth_json: &str) -> Vec<u8> {
        // protocol=1, project_id="p" (len 1), database_id="d" (len 1)
        let mut buf = vec![1u8, 1u8, b'p', 1u8, b'd'];
        buf.extend_from_slice(&0u16.to_be_bytes()); // metadata len = 0
        let auth = auth_json.as_bytes();
        buf.extend_from_slice(&(auth.len() as u16).to_be_bytes());
        buf.extend_from_slice(auth);
        buf
    }

    /// Regression test for the client-controlled `is_admin` privilege escalation:
    /// a valid customer/Firebase token (gateway sets wire `is_admin = false`) that
    /// smuggles a custom claim `is_admin: true` must NOT be promoted to admin. The
    /// gateway's dedicated wire field is the sole authority; the claim stays inert.
    #[test]
    fn test_claims_is_admin_does_not_grant_admin() {
        let payload = connect_payload_with_auth(
            r#"{"uid":"attacker","provider":"customer","is_admin":false,"claims":{"is_admin":true}}"#,
        );

        let (_proto, _project, _db, _meta, auth) = ProxyConnection::<
            crate::server::core_handler::CoreHandler,
        >::parse_connect_payload(&payload)
        .expect("payload parses");
        let auth = auth.expect("auth present");

        assert!(
            !auth.is_admin,
            "a client-supplied claims.is_admin must never be promoted to the trusted admin flag"
        );
        // The claim itself is preserved as inert app data (Firebase-compatible:
        // rules can still read it as auth.token.is_admin).
        assert_eq!(
            auth.claims.get("is_admin").and_then(|v| v.as_bool()),
            Some(true),
            "the custom claim should pass through untouched for rules to read"
        );
    }

    /// The gateway's authoritative wire field still grants admin (true admins work).
    #[test]
    fn test_wire_is_admin_grants_admin() {
        let payload = connect_payload_with_auth(
            r#"{"uid":"coordinator","provider":"admin","is_admin":true,"claims":{}}"#,
        );
        let (_p, _pr, _d, _m, auth) =
            ProxyConnection::<crate::server::core_handler::CoreHandler>::parse_connect_payload(
                &payload,
            )
            .expect("payload parses");
        assert!(auth.expect("auth present").is_admin);
    }

    #[test]
    fn test_maybe_compress_small_payload_not_compressed() {
        // Payload under threshold (1KB) should not be compressed
        let small_data = Bytes::from(vec![0u8; 500]);
        let (result, compressed) = VirtualClient::maybe_compress(small_data.clone());

        assert!(!compressed, "Small payload should not be compressed");
        assert_eq!(result.len(), small_data.len(), "Data should be unchanged");
    }

    #[test]
    fn test_maybe_compress_large_compressible_payload() {
        // Large repetitive data compresses well
        let large_data = Bytes::from(vec![b'a'; 5000]);
        let (result, compressed) = VirtualClient::maybe_compress(large_data.clone());

        assert!(
            compressed,
            "Large compressible payload should be compressed"
        );
        assert!(
            result.len() < large_data.len(),
            "Compressed size should be smaller"
        );

        // Verify it can be decompressed back
        let decompressed = zstd::bulk::decompress(&result, large_data.len() + 100).unwrap();
        assert_eq!(decompressed, large_data.as_ref());
    }

    #[test]
    fn test_maybe_compress_large_incompressible_payload() {
        // Random data doesn't compress well - compression should be skipped
        // Use a predictable "random" pattern that won't compress
        let mut incompressible = Vec::with_capacity(2000);
        for i in 0..2000u16 {
            incompressible.push((i % 256) as u8);
            incompressible.push(((i * 7) % 256) as u8);
        }
        let data = Bytes::from(incompressible);
        let original_len = data.len();

        let (result, compressed) = VirtualClient::maybe_compress(data);

        // If compression made it larger or same size, it should return uncompressed
        if !compressed {
            assert_eq!(
                result.len(),
                original_len,
                "Uncompressed data should be unchanged"
            );
        } else {
            // If it did compress, verify it's actually smaller
            assert!(
                result.len() < original_len,
                "If compressed flag is set, size must be smaller"
            );
        }
    }

    #[test]
    fn test_maybe_compress_exactly_at_threshold() {
        // Exactly at threshold (1024 bytes) should attempt compression
        let data = Bytes::from(vec![b'x'; COMPRESSION_THRESHOLD]);
        let (result, compressed) = VirtualClient::maybe_compress(data.clone());

        // Repetitive data should compress
        assert!(
            compressed,
            "Data at threshold should be compressed if beneficial"
        );
        assert!(result.len() < data.len());
    }

    #[test]
    fn test_maybe_compress_just_under_threshold() {
        // Just under threshold should not attempt compression
        let data = Bytes::from(vec![b'x'; COMPRESSION_THRESHOLD - 1]);
        let (_, compressed) = VirtualClient::maybe_compress(data);

        assert!(!compressed, "Data under threshold should not be compressed");
    }
}
