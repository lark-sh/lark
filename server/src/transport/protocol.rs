//! Extended proxy protocol for Glommio thread-per-core model.
//!
//! This module defines the binary protocol between proxy and server,
//! including new message types for coordinator communication over TCP.

use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// =============================================================================
// Protocol Constants
// =============================================================================

/// Protocol version for HELLO/HELLO_ACK handshake
pub const PROTOCOL_VERSION: u16 = 1;

/// Header size: [Length:4][Type:1][ClientID:4]
pub const HEADER_SIZE: usize = 9;

/// Maximum proxy↔server message size (257MB: 256MB data + overhead).
///
/// This must accommodate the largest write the edge will forward. SDK/WebSocket
/// writes are capped at `MAX_WRITE_SIZE` (16MB), but REST writes are allowed up
/// to 256MB (matching Firebase's REST limit; enforced by the edge's
/// `http.MaxBytesReader` and the Go side's `MaxMessageSize = 256MB`).
pub const MAX_MESSAGE_SIZE: usize = 257 << 20;

// =============================================================================
// Proxy -> Server Message Types
// =============================================================================

pub mod proxy_msg {
    /// Client connected
    pub const CONNECT: u8 = 0x01;
    /// Client message
    pub const DATA: u8 = 0x02;
    /// Client disconnected
    pub const DISCONNECT: u8 = 0x03;
    /// Connection handshake
    pub const HELLO: u8 = 0x04;
    /// Client auth update (late auth)
    pub const AUTH_CHANGED: u8 = 0x05;
    /// Acknowledges heartbeat, provides server time
    pub const HEARTBEAT_ACK: u8 = 0x06;
    /// Pushes project config to cores that need it
    pub const CONFIG_PUSH: u8 = 0x07;
    /// Forces a core to evict a specific database
    pub const EVICT_DATABASE: u8 = 0x08;
    /// Graceful shutdown (sent on all connections)
    pub const SHUTDOWN: u8 = 0x09;
    /// HMAC proof of SERVER_SECRET over the HELLO_ACK nonce. Sent by the proxy
    /// after HELLO_ACK; the server rejects the connection unless it verifies.
    pub const HELLO_AUTH: u8 = 0x0A;
}

// =============================================================================
// Server -> Proxy Message Types
// =============================================================================

pub mod server_msg {
    /// Server message to client
    pub const DATA: u8 = 0x01;
    /// Close client connection
    pub const CLOSE: u8 = 0x02;
    /// Handshake response with core assignment
    pub const HELLO_ACK: u8 = 0x03;
    /// Health metrics, sent every 10s on all connections
    pub const HEARTBEAT: u8 = 0x04;
    /// Notifies proxy that this core loaded a database
    pub const DATABASE_LOADED: u8 = 0x05;
    /// Notifies proxy that this core unloaded a database
    pub const DATABASE_UNLOADED: u8 = 0x06;
    /// Core requests project config (first time seeing project)
    pub const CONFIG_REQUEST: u8 = 0x07;

    /// Broadcast: send one message to multiple clients (proxy fans out)
    /// Format: [Length:4][Type:1=0x0B][Flags:1][ClientCount:4][[ClientID:4][Tag:4]...][MsgLen:4][MsgBytes...]
    /// - Tag=0 means no tag modification (pass through as-is)
    /// - Tag!=0: proxy inserts tag based on FIREBASE_FORMAT flag
    pub const BROADCAST: u8 = 0x0B;
}

/// Flags for BROADCAST message
pub mod broadcast_flag {
    /// Message needs reliable delivery
    pub const RELIABLE: u8 = 0x01;
    /// Message is Firebase format (affects tag insertion position)
    /// If set: insert `{"t":TAG,` after first `{`
    /// If not set (Lark format): replace trailing `}` with `,"t":TAG}`
    pub const FIREBASE_FORMAT: u8 = 0x02;
    /// MsgBytes payload is zstd compressed (proxy decompresses before fan-out)
    pub const COMPRESSED: u8 = 0x04;
}

// =============================================================================
// Other Constants
// =============================================================================

/// Protocol identifiers in CONNECT message
pub mod protocol_id {
    pub const WEBSOCKET: u8 = 0x00;
    pub const WEBTRANSPORT: u8 = 0x01;
    pub const REST: u8 = 0x02;
}

/// Disconnect reasons
pub mod disconnect_reason {
    pub const CLEAN: u8 = 0x00;
    pub const ERROR: u8 = 0x01;
    pub const TIMEOUT: u8 = 0x02;
    pub const CONFIG_TIMEOUT: u8 = 0x03;
}

/// Database unload reasons
pub mod unload_reason {
    pub const IDLE: u8 = 0x00;
    pub const MEMORY_PRESSURE: u8 = 0x01;
    pub const EXPLICIT_EVICTION: u8 = 0x02;
    pub const SHUTDOWN: u8 = 0x03;
}

/// Flags for EVICT_DATABASE messages
pub mod evict_flag {
    /// Delete the database's persisted data (currently: rename the data dir to
    /// `{dir}-deleted-{unix_ts}` so it can be recovered manually if the delete
    /// was accidental).
    pub const PURGE_DATA: u8 = 0x01;
}

/// Data flags (Server -> Proxy)
pub mod data_flag {
    pub const RELIABLE: u8 = 0x01;
    pub const UNRELIABLE: u8 = 0x00;
    /// Payload is zstd compressed (proxy decompresses before forwarding to client)
    pub const COMPRESSED: u8 = 0x02;
}

// =============================================================================
// Message Structs
// =============================================================================

/// HELLO message (Proxy -> Server)
#[derive(Debug, Clone)]
pub struct HelloMessage {
    pub proxy_version: u16,
}

impl HelloMessage {
    pub fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(8);
        // Reserved bytes for future use (5 bytes after version)
        buf.put_u16(self.proxy_version);
        buf.put_slice(&[0u8; 5]); // Reserved
        buf
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 2 {
            return None;
        }
        Some(Self {
            proxy_version: u16::from_be_bytes([data[0], data[1]]),
        })
    }
}

/// HELLO_ACK message (Server -> Proxy).
///
/// Carries a per-connection random `nonce`; the proxy must reply with a
/// HELLO_AUTH containing `HMAC-SHA256(SERVER_SECRET, nonce)` before the server
/// will process any further messages. Wire layout: core_id(1) + nr_cores(1) +
/// server_version(2) + nonce(32).
#[derive(Debug, Clone)]
pub struct HelloAckMessage {
    pub core_id: u8,
    pub nr_cores: u8,
    pub server_version: u16,
    pub nonce: [u8; 32],
}

impl HelloAckMessage {
    pub fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(36);
        buf.put_u8(self.core_id);
        buf.put_u8(self.nr_cores);
        buf.put_u16(self.server_version);
        buf.put_slice(&self.nonce);
        buf
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 36 {
            return None;
        }
        let mut nonce = [0u8; 32];
        nonce.copy_from_slice(&data[4..36]);
        Some(Self {
            core_id: data[0],
            nr_cores: data[1],
            server_version: u16::from_be_bytes([data[2], data[3]]),
            nonce,
        })
    }
}

/// HEARTBEAT message (Server -> Proxy)
#[derive(Debug, Clone)]
pub struct HeartbeatMessage {
    /// Per-core CPU load 0-10000 (0.00%-100.00%)
    pub load: u16,
    /// Number of connected clients
    pub client_count: u32,
    /// Memory usage in MB
    pub memory_mb: u32,
}

impl HeartbeatMessage {
    pub fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(16);
        buf.put_u16(self.load);
        buf.put_u32(self.client_count);
        buf.put_u32(self.memory_mb);
        buf.put_slice(&[0u8; 6]); // Reserved
        buf
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 10 {
            return None;
        }
        Some(Self {
            load: u16::from_be_bytes([data[0], data[1]]),
            client_count: u32::from_be_bytes([data[2], data[3], data[4], data[5]]),
            memory_mb: u32::from_be_bytes([data[6], data[7], data[8], data[9]]),
        })
    }
}

/// HEARTBEAT_ACK message (Proxy -> Server)
#[derive(Debug, Clone)]
pub struct HeartbeatAckMessage {
    /// Server time in unix milliseconds
    pub server_time: u64,
}

impl HeartbeatAckMessage {
    pub fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(12);
        buf.put_u64(self.server_time);
        buf.put_slice(&[0u8; 4]); // Reserved
        buf
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }
        Some(Self {
            server_time: u64::from_be_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]),
        })
    }
}

/// DATABASE_LOADED message (Server -> Proxy)
#[derive(Debug, Clone)]
pub struct DatabaseLoadedMessage {
    pub project_id: String,
    pub database_id: String,
}

impl DatabaseLoadedMessage {
    pub fn encode(&self) -> BytesMut {
        let project_bytes = self.project_id.as_bytes();
        let db_bytes = self.database_id.as_bytes();
        let mut buf = BytesMut::with_capacity(2 + project_bytes.len() + db_bytes.len());

        buf.put_u8(project_bytes.len() as u8);
        buf.put_slice(project_bytes);
        buf.put_u8(db_bytes.len() as u8);
        buf.put_slice(db_bytes);

        buf
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }

        let project_len = data[0] as usize;
        if data.len() < 1 + project_len + 1 {
            return None;
        }

        let project_id = String::from_utf8_lossy(&data[1..1 + project_len]).to_string();
        let db_len = data[1 + project_len] as usize;

        if data.len() < 2 + project_len + db_len {
            return None;
        }

        let database_id =
            String::from_utf8_lossy(&data[2 + project_len..2 + project_len + db_len]).to_string();

        Some(Self {
            project_id,
            database_id,
        })
    }
}

/// DATABASE_UNLOADED message (Server -> Proxy)
#[derive(Debug, Clone)]
pub struct DatabaseUnloadedMessage {
    pub project_id: String,
    pub database_id: String,
    pub reason: u8,
    /// 1 if ephemeral (proxy should delete record), 0 if persistent (proxy marks inactive)
    pub ephemeral: bool,
}

impl DatabaseUnloadedMessage {
    pub fn encode(&self) -> BytesMut {
        let project_bytes = self.project_id.as_bytes();
        let db_bytes = self.database_id.as_bytes();
        let mut buf = BytesMut::with_capacity(4 + project_bytes.len() + db_bytes.len());

        buf.put_u8(project_bytes.len() as u8);
        buf.put_slice(project_bytes);
        buf.put_u8(db_bytes.len() as u8);
        buf.put_slice(db_bytes);
        buf.put_u8(self.reason);
        buf.put_u8(if self.ephemeral { 1 } else { 0 });

        buf
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }

        let project_len = data[0] as usize;
        if data.len() < 1 + project_len + 1 {
            return None;
        }

        let project_id = String::from_utf8_lossy(&data[1..1 + project_len]).to_string();
        let db_len = data[1 + project_len] as usize;

        if data.len() < 2 + project_len + db_len + 2 {
            return None;
        }

        let database_id =
            String::from_utf8_lossy(&data[2 + project_len..2 + project_len + db_len]).to_string();
        let reason = data[2 + project_len + db_len];
        let ephemeral = data[2 + project_len + db_len + 1] != 0;

        Some(Self {
            project_id,
            database_id,
            reason,
            ephemeral,
        })
    }
}

/// CONFIG_REQUEST message (Server -> Proxy)
#[derive(Debug, Clone)]
pub struct ConfigRequestMessage {
    pub project_id: String,
}

impl ConfigRequestMessage {
    pub fn encode(&self) -> BytesMut {
        let project_bytes = self.project_id.as_bytes();
        let mut buf = BytesMut::with_capacity(1 + project_bytes.len());

        buf.put_u8(project_bytes.len() as u8);
        buf.put_slice(project_bytes);

        buf
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }

        let project_len = data[0] as usize;
        if data.len() < 1 + project_len {
            return None;
        }

        let project_id = String::from_utf8_lossy(&data[1..1 + project_len]).to_string();

        Some(Self { project_id })
    }
}

/// Project configuration (sent via CONFIG_PUSH)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProjectConfig {
    /// Monotonically increasing version assigned by lark-admin. Used to
    /// reject out-of-order and duplicate CONFIG_PUSH deliveries (multi-proxy
    /// fan-out). 0 means "unversioned" — we accept these for backwards compat.
    #[serde(default)]
    pub config_version: u64,

    /// Security rules JSON string
    #[serde(default)]
    pub rules: Option<String>,

    /// Project secret key for token validation
    #[serde(default)]
    pub secret_key: Option<String>,

    /// Admin secret key
    #[serde(default)]
    pub admin_secret_key: Option<String>,

    /// Firebase project ID (if applicable)
    #[serde(default)]
    pub firebase_project_id: Option<String>,

    /// Whether databases in this project are ephemeral (no persistence)
    #[serde(default)]
    pub ephemeral: Option<bool>,

    /// Additional settings
    #[serde(default)]
    pub settings: HashMap<String, serde_json::Value>,
}

/// CONFIG_PUSH message (Proxy -> Server)
#[derive(Debug, Clone)]
pub struct ConfigPushMessage {
    pub project_id: String,
    pub config: ProjectConfig,
}

impl ConfigPushMessage {
    pub fn encode(&self) -> BytesMut {
        let project_bytes = self.project_id.as_bytes();
        let config_json = serde_json::to_vec(&self.config).unwrap_or_default();
        let mut buf = BytesMut::with_capacity(5 + project_bytes.len() + config_json.len());

        buf.put_u8(project_bytes.len() as u8);
        buf.put_slice(project_bytes);
        buf.put_u32(config_json.len() as u32);
        buf.put_slice(&config_json);

        buf
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }

        let project_len = data[0] as usize;
        if data.len() < 1 + project_len + 4 {
            return None;
        }

        let project_id = String::from_utf8_lossy(&data[1..1 + project_len]).to_string();
        let config_len = u32::from_be_bytes([
            data[1 + project_len],
            data[2 + project_len],
            data[3 + project_len],
            data[4 + project_len],
        ]) as usize;

        if data.len() < 5 + project_len + config_len {
            return None;
        }

        let config: ProjectConfig =
            serde_json::from_slice(&data[5 + project_len..5 + project_len + config_len]).ok()?;

        Some(Self { project_id, config })
    }
}

/// EVICT_DATABASE message (Proxy -> Server)
///
/// Wire format:
/// `[proj_len:1][proj][db_len:1][db][flags:1]`
///
/// The trailing `flags` byte is optional on decode — older proxies don't send
/// it. See [`evict_flag`] for the flag bits.
#[derive(Debug, Clone)]
pub struct EvictDatabaseMessage {
    pub project_id: String,
    pub database_id: String,
    pub flags: u8,
}

impl EvictDatabaseMessage {
    pub fn encode(&self) -> BytesMut {
        let project_bytes = self.project_id.as_bytes();
        let db_bytes = self.database_id.as_bytes();
        let mut buf = BytesMut::with_capacity(3 + project_bytes.len() + db_bytes.len());

        buf.put_u8(project_bytes.len() as u8);
        buf.put_slice(project_bytes);
        buf.put_u8(db_bytes.len() as u8);
        buf.put_slice(db_bytes);
        buf.put_u8(self.flags);

        buf
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        // Shared prefix with DatabaseLoadedMessage (project + database).
        let loaded = DatabaseLoadedMessage::decode(data)?;

        // Figure out where the project/database prefix ends so we can read
        // the optional trailing flags byte.
        let project_len = data.first().copied()? as usize;
        let db_len_off = 1 + project_len;
        let db_len = data.get(db_len_off).copied()? as usize;
        let flags_off = db_len_off + 1 + db_len;
        let flags = data.get(flags_off).copied().unwrap_or(0);

        Some(Self {
            project_id: loaded.project_id,
            database_id: loaded.database_id,
            flags,
        })
    }
}

/// SHUTDOWN message (Proxy -> Server)
#[derive(Debug, Clone)]
pub struct ShutdownMessage {
    /// Grace period in seconds
    pub grace_period_secs: u32,
}

impl ShutdownMessage {
    pub fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(8);
        buf.put_u32(self.grace_period_secs);
        buf.put_slice(&[0u8; 4]); // Reserved
        buf
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 4 {
            return None;
        }
        Some(Self {
            grace_period_secs: u32::from_be_bytes([data[0], data[1], data[2], data[3]]),
        })
    }
}

// =============================================================================
// Frame Encoding/Decoding Helpers
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hello_encode_decode() {
        let msg = HelloMessage { proxy_version: 1 };
        let encoded = msg.encode();
        let decoded = HelloMessage::decode(&encoded).unwrap();
        assert_eq!(decoded.proxy_version, 1);
    }

    #[test]
    fn test_hello_ack_encode_decode() {
        let mut nonce = [0u8; 32];
        for (i, b) in nonce.iter_mut().enumerate() {
            *b = i as u8;
        }
        let msg = HelloAckMessage {
            core_id: 3,
            nr_cores: 8,
            server_version: 1,
            nonce,
        };
        let encoded = msg.encode();
        assert_eq!(encoded.len(), 36);
        let decoded = HelloAckMessage::decode(&encoded).unwrap();
        assert_eq!(decoded.core_id, 3);
        assert_eq!(decoded.nr_cores, 8);
        assert_eq!(decoded.server_version, 1);
        assert_eq!(decoded.nonce, nonce);
    }

    #[test]
    fn test_heartbeat_encode_decode() {
        let msg = HeartbeatMessage {
            load: 5000,
            client_count: 1234,
            memory_mb: 512,
        };
        let encoded = msg.encode();
        let decoded = HeartbeatMessage::decode(&encoded).unwrap();
        assert_eq!(decoded.load, 5000);
        assert_eq!(decoded.client_count, 1234);
        assert_eq!(decoded.memory_mb, 512);
    }

    #[test]
    fn test_database_loaded_encode_decode() {
        let msg = DatabaseLoadedMessage {
            project_id: "my-project".to_string(),
            database_id: "room-123".to_string(),
        };
        let encoded = msg.encode();
        let decoded = DatabaseLoadedMessage::decode(&encoded).unwrap();
        assert_eq!(decoded.project_id, "my-project");
        assert_eq!(decoded.database_id, "room-123");
    }

    #[test]
    fn test_config_push_encode_decode() {
        let msg = ConfigPushMessage {
            project_id: "test".to_string(),
            config: ProjectConfig {
                config_version: 42,
                rules: Some(r#"{"rules": {".read": true}}"#.to_string()),
                secret_key: Some("secret".to_string()),
                ephemeral: Some(true),
                ..Default::default()
            },
        };
        let encoded = msg.encode();
        let decoded = ConfigPushMessage::decode(&encoded).unwrap();
        assert_eq!(decoded.project_id, "test");
        assert_eq!(decoded.config.config_version, 42);
        assert_eq!(
            decoded.config.rules,
            Some(r#"{"rules": {".read": true}}"#.to_string())
        );
        assert_eq!(decoded.config.secret_key, Some("secret".to_string()));
    }

    #[test]
    fn test_evict_database_encode_decode_with_flags() {
        let msg = EvictDatabaseMessage {
            project_id: "p".to_string(),
            database_id: "room-1".to_string(),
            flags: evict_flag::PURGE_DATA,
        };
        let encoded = msg.encode();
        let decoded = EvictDatabaseMessage::decode(&encoded).unwrap();
        assert_eq!(decoded.project_id, "p");
        assert_eq!(decoded.database_id, "room-1");
        assert_eq!(decoded.flags, evict_flag::PURGE_DATA);
    }

    #[test]
    fn test_evict_database_decode_tolerates_missing_flags() {
        // Old proxy: encodes [proj_len][proj][db_len][db] without trailing flags.
        // Decode should succeed with flags defaulted to 0.
        let legacy = DatabaseLoadedMessage {
            project_id: "p".to_string(),
            database_id: "room-1".to_string(),
        }
        .encode();
        let decoded = EvictDatabaseMessage::decode(&legacy).unwrap();
        assert_eq!(decoded.project_id, "p");
        assert_eq!(decoded.database_id, "room-1");
        assert_eq!(decoded.flags, 0);
    }

    #[test]
    fn test_shutdown_encode_decode() {
        let msg = ShutdownMessage {
            grace_period_secs: 30,
        };
        let encoded = msg.encode();
        let decoded = ShutdownMessage::decode(&encoded).unwrap();
        assert_eq!(decoded.grace_period_secs, 30);
    }
}
