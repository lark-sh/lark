//! High-level proxy client for the chaos monkey.
//!
//! Manages the TCP connection to a Lark server, performing the HELLO handshake,
//! connecting virtual clients, sending operations, and receiving responses.

use super::codec::{FrameReader, FrameWriter};
use super::messages::{ClientResponse, ServerMessage};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::io;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};
use tracing::{debug, info, trace, warn};

/// Events received from the server, dispatched to the chaos loop.
#[derive(Debug)]
pub enum ServerEvent {
    /// ACK for a write operation
    Ack { client_id: u32, request_id: String },
    /// NACK for a write operation
    Nack {
        client_id: u32,
        request_id: String,
        error: String,
    },
    /// ONCE read-back response
    Once {
        client_id: u32,
        request_id: String,
        value: serde_json::Value,
    },
    /// Database finished loading
    DatabaseLoaded {
        project_id: String,
        database_id: String,
    },
    /// Server sent a heartbeat
    Heartbeat,
    /// Connection was closed by the server
    Disconnected,
    /// Other data event (subscriptions, etc.)
    Other {
        client_id: u32,
        data: serde_json::Value,
    },
}

/// A connection to a Lark server, pretending to be a proxy.
pub struct ProxyClient {
    writer: FrameWriter,
    event_rx: mpsc::Receiver<ServerEvent>,
    /// Handle to the reader task so we can abort it on drop
    reader_handle: tokio::task::JoinHandle<()>,
    /// Next request ID counter
    next_request_id: u64,
    /// Connected virtual client IDs
    connected_clients: Vec<u32>,
}

impl ProxyClient {
    /// Connect to the server and perform the HELLO handshake.
    pub async fn connect(addr: &str) -> io::Result<Self> {
        info!("Connecting to server at {}", addr);
        let stream = TcpStream::connect(addr).await?;
        let (read_half, write_half) = tokio::io::split(stream);

        let mut writer = FrameWriter::new(write_half);
        let mut reader = FrameReader::new(read_half);

        // Send HELLO
        writer.write_hello().await?;
        debug!("Sent HELLO");

        // Wait for HELLO_ACK
        let (msg_type, payload) = reader.read_frame().await?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "server closed during handshake",
            )
        })?;

        let hello_ack = ServerMessage::decode(msg_type, &payload).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "failed to decode HELLO_ACK")
        })?;

        let nonce = match hello_ack {
            ServerMessage::HelloAck {
                core_id,
                nr_cores,
                server_version,
                nonce,
            } => {
                info!(
                    "Handshake complete: core_id={}, nr_cores={}, server_version={}",
                    core_id, nr_cores, server_version
                );
                nonce
            }
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("expected HELLO_ACK, got {:?}", other),
                ));
            }
        };

        // Prove we're a trusted gateway: HMAC-SHA256(SERVER_SECRET, nonce).
        let mut mac = <Hmac<Sha256>>::new_from_slice(crate::SERVER_SECRET.as_bytes())
            .map_err(|e| io::Error::other(format!("hmac init: {e}")))?;
        mac.update(&nonce);
        let mac_bytes = mac.finalize().into_bytes();
        writer.write_hello_auth(&mac_bytes).await?;
        debug!("Sent HELLO_AUTH");

        // Spawn reader task
        let (event_tx, event_rx) = mpsc::channel(4096);
        let reader_handle = tokio::spawn(Self::reader_loop(reader, event_tx));

        Ok(Self {
            writer,
            event_rx,
            reader_handle,
            next_request_id: 1,
            connected_clients: Vec::new(),
        })
    }

    /// Background reader loop that decodes server messages and sends events.
    async fn reader_loop(mut reader: FrameReader, tx: mpsc::Sender<ServerEvent>) {
        loop {
            match reader.read_frame().await {
                Ok(Some((msg_type, payload))) => {
                    let msg = match ServerMessage::decode(msg_type, &payload) {
                        Some(m) => m,
                        None => {
                            warn!("Failed to decode server message type 0x{:02x}", msg_type);
                            continue;
                        }
                    };

                    match msg {
                        ServerMessage::Data {
                            client_id, payload, ..
                        } => {
                            Self::dispatch_client_data(client_id, &payload, &tx).await;
                        }
                        ServerMessage::Broadcast {
                            client_ids,
                            payload,
                            ..
                        } => {
                            // Dispatch to first client (they all get the same data)
                            if let Some(&cid) = client_ids.first() {
                                Self::dispatch_client_data(cid, &payload, &tx).await;
                            }
                        }
                        ServerMessage::ConfigRequest { project_id } => {
                            debug!("Server requests config for project: {}", project_id);
                            // The main loop will handle sending CONFIG_PUSH
                            // We send a special event for this
                            let _ = tx
                                .send(ServerEvent::DatabaseLoaded {
                                    project_id,
                                    database_id: String::new(),
                                })
                                .await;
                        }
                        ServerMessage::DatabaseLoaded {
                            project_id,
                            database_id,
                        } => {
                            debug!("Database loaded: {}/{}", project_id, database_id);
                            let _ = tx
                                .send(ServerEvent::DatabaseLoaded {
                                    project_id,
                                    database_id,
                                })
                                .await;
                        }
                        ServerMessage::Heartbeat { .. } => {
                            trace!("Heartbeat received");
                            let _ = tx.send(ServerEvent::Heartbeat).await;
                        }
                        ServerMessage::Close { client_id } => {
                            debug!("Server closed client {}", client_id);
                        }
                        ServerMessage::Unknown(t) => {
                            trace!("Unknown server message type: 0x{:02x}", t);
                        }
                        _ => {}
                    }
                }
                Ok(None) => {
                    debug!("Server disconnected");
                    let _ = tx.send(ServerEvent::Disconnected).await;
                    break;
                }
                Err(e) => {
                    debug!("Reader error: {}", e);
                    let _ = tx.send(ServerEvent::Disconnected).await;
                    break;
                }
            }
        }
    }

    /// Parse and dispatch a client data payload (DATA or BROADCAST).
    async fn dispatch_client_data(client_id: u32, payload: &[u8], tx: &mpsc::Sender<ServerEvent>) {
        match ClientResponse::parse(payload) {
            Some(ClientResponse::Ack { request_id }) => {
                trace!("ACK for client {} req {}", client_id, request_id);
                let _ = tx
                    .send(ServerEvent::Ack {
                        client_id,
                        request_id,
                    })
                    .await;
            }
            Some(ClientResponse::Nack { request_id, error }) => {
                trace!(
                    "NACK for client {} req {}: {}",
                    client_id,
                    request_id,
                    error
                );
                let _ = tx
                    .send(ServerEvent::Nack {
                        client_id,
                        request_id,
                        error,
                    })
                    .await;
            }
            Some(ClientResponse::Once { request_id, value }) => {
                trace!("ONCE for client {} req {}", client_id, request_id);
                let _ = tx
                    .send(ServerEvent::Once {
                        client_id,
                        request_id,
                        value,
                    })
                    .await;
            }
            Some(ClientResponse::Other(v)) => {
                trace!("Other data for client {}: {:?}", client_id, v);
                let _ = tx.send(ServerEvent::Other { client_id, data: v }).await;
            }
            None => {
                trace!(
                    "Failed to parse client data for {}: {:?}",
                    client_id,
                    String::from_utf8_lossy(payload)
                );
            }
        }
    }

    /// Connect a virtual client to a database.
    pub async fn connect_client(
        &mut self,
        client_id: u32,
        project_id: &str,
        database_id: &str,
        auth_uid: &str,
    ) -> io::Result<()> {
        self.writer
            .write_connect(client_id, project_id, database_id, auth_uid)
            .await?;
        self.connected_clients.push(client_id);
        debug!(
            "Connected virtual client {} to {}/{}",
            client_id, project_id, database_id
        );
        Ok(())
    }

    /// Send a CONFIG_PUSH for a project with the given rules JSON (persistent).
    pub async fn push_config_with_rules(
        &mut self,
        project_id: &str,
        rules: &str,
    ) -> io::Result<()> {
        self.writer
            .write_config_push(project_id, rules, false)
            .await?;
        debug!("Pushed config for project {}", project_id);
        Ok(())
    }

    /// Send a CONFIG_PUSH for a project (open rules, persistent).
    /// Convenience wrapper preserved for older call sites.
    pub async fn push_config(&mut self, project_id: &str) -> io::Result<()> {
        let rules = r#"{"rules": {".read": true, ".write": true}}"#;
        self.push_config_with_rules(project_id, rules).await
    }

    /// Generate the next unique request ID.
    pub fn next_request_id(&mut self) -> String {
        let id = self.next_request_id;
        self.next_request_id += 1;
        format!("r{}", id)
    }

    /// Send a SET operation.
    pub async fn send_set(
        &mut self,
        client_id: u32,
        path: &str,
        value: serde_json::Value,
        request_id: &str,
    ) -> io::Result<()> {
        let msg = serde_json::json!({
            "o": "s",
            "p": path,
            "v": value,
            "r": request_id,
        });
        let json = serde_json::to_vec(&msg).unwrap();
        self.writer.write_data(client_id, &json).await
    }

    /// Send an UPDATE (shallow merge) operation.
    pub async fn send_update(
        &mut self,
        client_id: u32,
        path: &str,
        value: serde_json::Value,
        request_id: &str,
    ) -> io::Result<()> {
        let msg = serde_json::json!({
            "o": "u",
            "p": path,
            "v": value,
            "r": request_id,
        });
        let json = serde_json::to_vec(&msg).unwrap();
        self.writer.write_data(client_id, &json).await
    }

    /// Send a TRANSACTION operation. Each entry in `ops` is a sub-op encoded
    /// as a JSON object with `o` (kind: "s"/"u"/"d"/"c"), `p` (path), and
    /// optionally `v` (value) / `h` (hash for conditions).
    pub async fn send_transaction(
        &mut self,
        client_id: u32,
        ops: Vec<serde_json::Value>,
        request_id: &str,
    ) -> io::Result<()> {
        let msg = serde_json::json!({
            "o": "tx",
            "r": request_id,
            "ops": ops,
        });
        let json = serde_json::to_vec(&msg).unwrap();
        self.writer.write_data(client_id, &json).await
    }

    /// Send a ONCE (read) operation.
    pub async fn send_once(
        &mut self,
        client_id: u32,
        path: &str,
        request_id: &str,
    ) -> io::Result<()> {
        let msg = serde_json::json!({
            "o": "o",
            "p": path,
            "r": request_id,
        });
        let json = serde_json::to_vec(&msg).unwrap();
        self.writer.write_data(client_id, &json).await
    }

    /// Send a HEARTBEAT_ACK response.
    pub async fn send_heartbeat_ack(&mut self) -> io::Result<()> {
        self.writer.write_heartbeat_ack().await
    }

    /// Disconnect a virtual client.
    pub async fn disconnect_client(&mut self, client_id: u32) -> io::Result<()> {
        self.writer.write_disconnect(client_id).await?;
        self.connected_clients.retain(|&id| id != client_id);
        Ok(())
    }

    /// Receive the next event from the server, with timeout.
    pub async fn recv_event(&mut self, dur: Duration) -> Option<ServerEvent> {
        match timeout(dur, self.event_rx.recv()).await {
            Ok(Some(event)) => Some(event),
            Ok(None) => Some(ServerEvent::Disconnected),
            Err(_) => None, // timeout
        }
    }

    /// Drain all pending events (non-blocking).
    pub fn drain_events(&mut self) -> Vec<ServerEvent> {
        let mut events = Vec::new();
        while let Ok(event) = self.event_rx.try_recv() {
            events.push(event);
        }
        events
    }

    /// Get connected client IDs.
    pub fn connected_clients(&self) -> &[u32] {
        &self.connected_clients
    }
}

impl Drop for ProxyClient {
    fn drop(&mut self) {
        self.reader_handle.abort();
    }
}
