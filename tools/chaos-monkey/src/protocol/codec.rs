//! Binary frame codec for the Lark proxy protocol.
//!
//! Frame format: [Length:4 BE][Payload]
//! Where Length is the size of Payload (does not include the 4-byte length header).
//! Payload[0] is always the message type byte.

use bytes::{BufMut, BytesMut};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Maximum message size (17MB: 16MB data + overhead)
pub const MAX_MESSAGE_SIZE: usize = 17 << 20;

/// Protocol version for HELLO/HELLO_ACK handshake
pub const PROTOCOL_VERSION: u16 = 1;

// =============================================================================
// Proxy -> Server message type constants
// =============================================================================

pub mod proxy_msg {
    pub const CONNECT: u8 = 0x01;
    pub const DATA: u8 = 0x02;
    pub const DISCONNECT: u8 = 0x03;
    pub const HELLO: u8 = 0x04;
    pub const HEARTBEAT_ACK: u8 = 0x06;
    pub const CONFIG_PUSH: u8 = 0x07;
    pub const HELLO_AUTH: u8 = 0x0A;
}

// =============================================================================
// Server -> Proxy message type constants
// =============================================================================

pub mod server_msg {
    pub const DATA: u8 = 0x01;
    pub const CLOSE: u8 = 0x02;
    pub const HELLO_ACK: u8 = 0x03;
    pub const HEARTBEAT: u8 = 0x04;
    pub const DATABASE_LOADED: u8 = 0x05;
    pub const CONFIG_REQUEST: u8 = 0x07;
    pub const BROADCAST: u8 = 0x0B;
}

/// Data flags (Server -> Proxy DATA messages)
pub mod data_flag {
    pub const RELIABLE: u8 = 0x01;
    pub const COMPRESSED: u8 = 0x02;
}

/// Broadcast flags
pub mod broadcast_flag {
    pub const RELIABLE: u8 = 0x01;
    pub const COMPRESSED: u8 = 0x04;
}

/// Protocol identifiers
pub mod protocol_id {
    pub const WEBSOCKET: u8 = 0x00;
}

// =============================================================================
// Frame Reader / Writer
// =============================================================================

/// Reads length-prefixed frames from a TCP stream.
pub struct FrameReader {
    stream: tokio::io::ReadHalf<TcpStream>,
    header_buf: [u8; 4],
}

impl FrameReader {
    pub fn new(stream: tokio::io::ReadHalf<TcpStream>) -> Self {
        Self {
            stream,
            header_buf: [0u8; 4],
        }
    }

    /// Read one complete frame. Returns (msg_type, payload_after_type).
    /// Returns Ok(None) on clean disconnect.
    pub async fn read_frame(&mut self) -> io::Result<Option<(u8, Vec<u8>)>> {
        // Read 4-byte length header
        match self.stream.read_exact(&mut self.header_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }

        let length = u32::from_be_bytes(self.header_buf) as usize;

        if length > MAX_MESSAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("message too large: {} bytes", length),
            ));
        }

        if length < 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "message too small",
            ));
        }

        // Read the full payload
        let mut payload = vec![0u8; length];
        self.stream.read_exact(&mut payload).await?;

        let msg_type = payload[0];
        let rest = payload[1..].to_vec();

        Ok(Some((msg_type, rest)))
    }
}

/// Writes length-prefixed frames to a TCP stream.
pub struct FrameWriter {
    stream: tokio::io::WriteHalf<TcpStream>,
    buf: BytesMut,
}

impl FrameWriter {
    pub fn new(stream: tokio::io::WriteHalf<TcpStream>) -> Self {
        Self {
            stream,
            buf: BytesMut::with_capacity(4096),
        }
    }

    /// Write a raw frame: [Length:4][Type:1][payload]
    pub async fn write_frame(&mut self, msg_type: u8, payload: &[u8]) -> io::Result<()> {
        self.buf.clear();
        let length = 1 + payload.len();
        self.buf.put_u32(length as u32);
        self.buf.put_u8(msg_type);
        self.buf.extend_from_slice(payload);
        self.stream.write_all(&self.buf).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Write a HELLO message
    pub async fn write_hello(&mut self) -> io::Result<()> {
        let mut payload = BytesMut::with_capacity(7);
        payload.put_u16(PROTOCOL_VERSION);
        payload.put_slice(&[0u8; 5]); // Reserved
        self.write_frame(proxy_msg::HELLO, &payload).await
    }

    /// Write a HELLO_AUTH message: HMAC-SHA256(SERVER_SECRET, nonce) over the
    /// nonce from HELLO_ACK, proving we're a trusted gateway.
    pub async fn write_hello_auth(&mut self, mac: &[u8]) -> io::Result<()> {
        self.write_frame(proxy_msg::HELLO_AUTH, mac).await
    }

    /// Write a CONNECT message for a virtual client
    pub async fn write_connect(
        &mut self,
        client_id: u32,
        project_id: &str,
        database_id: &str,
        auth_uid: &str,
    ) -> io::Result<()> {
        let mut payload = BytesMut::with_capacity(256);

        // ClientID (4 bytes)
        payload.put_u32(client_id);

        // Protocol byte (WebSocket)
        payload.put_u8(protocol_id::WEBSOCKET);

        // Project ID (1-byte length prefix)
        let proj_bytes = project_id.as_bytes();
        payload.put_u8(proj_bytes.len() as u8);
        payload.extend_from_slice(proj_bytes);

        // Database ID (1-byte length prefix)
        let db_bytes = database_id.as_bytes();
        payload.put_u8(db_bytes.len() as u8);
        payload.extend_from_slice(db_bytes);

        // Metadata JSON (2-byte length prefix) - empty object
        let meta_json = b"{}";
        payload.put_u16(meta_json.len() as u16);
        payload.extend_from_slice(meta_json);

        // Auth JSON (2-byte length prefix)
        //
        // `is_admin: false` so the rules engine actually evaluates rules
        // for chaos clients — `is_admin: true` would short-circuit every
        // call to `can_write` / `can_read` via the admin-bypass in
        // `Evaluator::can_write` / `can_read`, making rules-mode `lookup`
        // and the deny-op test silently equivalent to `open`. Both
        // current rules modes (`open` = always-true, `lookup` = the
        // chaos test rule) work fine without admin bypass.
        let auth_json = serde_json::json!({
            "uid": auth_uid,
            "provider": "custom",
            "is_admin": false
        });
        let auth_bytes = serde_json::to_vec(&auth_json).unwrap();
        payload.put_u16(auth_bytes.len() as u16);
        payload.extend_from_slice(&auth_bytes);

        self.write_frame(proxy_msg::CONNECT, &payload).await
    }

    /// Write a DATA message (client operation)
    pub async fn write_data(&mut self, client_id: u32, json_msg: &[u8]) -> io::Result<()> {
        let mut payload = BytesMut::with_capacity(4 + json_msg.len());
        payload.put_u32(client_id);
        payload.extend_from_slice(json_msg);
        self.write_frame(proxy_msg::DATA, &payload).await
    }

    /// Write a DISCONNECT message
    pub async fn write_disconnect(&mut self, client_id: u32) -> io::Result<()> {
        let mut payload = BytesMut::with_capacity(4);
        payload.put_u32(client_id);
        self.write_frame(proxy_msg::DISCONNECT, &payload).await
    }

    /// Write a CONFIG_PUSH message
    pub async fn write_config_push(
        &mut self,
        project_id: &str,
        rules_json: &str,
        ephemeral: bool,
    ) -> io::Result<()> {
        let config = serde_json::json!({
            "rules": rules_json,
            "ephemeral": ephemeral,
        });
        let config_bytes = serde_json::to_vec(&config).unwrap();

        let proj_bytes = project_id.as_bytes();
        let mut payload = BytesMut::with_capacity(5 + proj_bytes.len() + config_bytes.len());

        payload.put_u8(proj_bytes.len() as u8);
        payload.extend_from_slice(proj_bytes);
        payload.put_u32(config_bytes.len() as u32);
        payload.extend_from_slice(&config_bytes);

        self.write_frame(proxy_msg::CONFIG_PUSH, &payload).await
    }

    /// Write a HEARTBEAT_ACK message
    pub async fn write_heartbeat_ack(&mut self) -> io::Result<()> {
        let mut payload = BytesMut::with_capacity(12);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        payload.put_u64(now_ms);
        payload.put_slice(&[0u8; 4]); // Reserved
        self.write_frame(proxy_msg::HEARTBEAT_ACK, &payload).await
    }
}
