//! Decode server→proxy messages that the chaos monkey receives.
//!
//! We only need to decode the subset of messages relevant to chaos testing:
//! HELLO_ACK, CONFIG_REQUEST, DATABASE_LOADED, DATA, BROADCAST, HEARTBEAT.

use super::codec::{data_flag, server_msg};

/// A decoded server→proxy message.
#[derive(Debug)]
pub enum ServerMessage {
    HelloAck {
        core_id: u8,
        nr_cores: u8,
        server_version: u16,
    },
    ConfigRequest {
        project_id: String,
    },
    DatabaseLoaded {
        project_id: String,
        database_id: String,
    },
    /// Unicast data from server to a specific client
    Data {
        client_id: u32,
        flags: u8,
        payload: Vec<u8>,
    },
    /// Broadcast data to multiple clients
    Broadcast {
        flags: u8,
        client_ids: Vec<u32>,
        payload: Vec<u8>,
    },
    Heartbeat {
        load: u16,
        client_count: u32,
        memory_mb: u32,
    },
    /// Close connection for a client
    Close {
        client_id: u32,
    },
    /// Unknown message type
    Unknown(u8),
}

impl ServerMessage {
    /// Decode a server message from (msg_type, payload_after_type).
    pub fn decode(msg_type: u8, data: &[u8]) -> Option<Self> {
        match msg_type {
            server_msg::HELLO_ACK => Self::decode_hello_ack(data),
            server_msg::CONFIG_REQUEST => Self::decode_config_request(data),
            server_msg::DATABASE_LOADED => Self::decode_database_loaded(data),
            server_msg::DATA => Self::decode_data(data),
            server_msg::BROADCAST => Self::decode_broadcast(data),
            server_msg::HEARTBEAT => Self::decode_heartbeat(data),
            server_msg::CLOSE => Self::decode_close(data),
            _ => Some(ServerMessage::Unknown(msg_type)),
        }
    }

    fn decode_hello_ack(data: &[u8]) -> Option<Self> {
        if data.len() < 4 {
            return None;
        }
        Some(ServerMessage::HelloAck {
            core_id: data[0],
            nr_cores: data[1],
            server_version: u16::from_be_bytes([data[2], data[3]]),
        })
    }

    fn decode_config_request(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }
        let project_len = data[0] as usize;
        if data.len() < 1 + project_len {
            return None;
        }
        let project_id = String::from_utf8_lossy(&data[1..1 + project_len]).to_string();
        Some(ServerMessage::ConfigRequest { project_id })
    }

    fn decode_database_loaded(data: &[u8]) -> Option<Self> {
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
        Some(ServerMessage::DatabaseLoaded {
            project_id,
            database_id,
        })
    }

    fn decode_data(data: &[u8]) -> Option<Self> {
        // DATA format after type byte: [ClientID:4][Flags:1][MsgJSON...]
        if data.len() < 5 {
            return None;
        }
        let client_id = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        let flags = data[4];
        let mut payload = data[5..].to_vec();

        // Decompress if compressed
        if flags & data_flag::COMPRESSED != 0 {
            payload = zstd::decode_all(payload.as_slice()).ok()?;
        }

        Some(ServerMessage::Data {
            client_id,
            flags,
            payload,
        })
    }

    fn decode_broadcast(data: &[u8]) -> Option<Self> {
        // BROADCAST format after type byte: [Flags:1][ClientCount:4][[ClientID:4][Tag:4]...][MsgLen:4][MsgJSON]
        if data.len() < 5 {
            return None;
        }
        let flags = data[0];
        let client_count = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;

        let mut offset = 5;
        let entry_size = 8; // ClientID:4 + Tag:4
        if data.len() < offset + client_count * entry_size + 4 {
            return None;
        }

        let mut client_ids = Vec::with_capacity(client_count);
        for _ in 0..client_count {
            let cid = u32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            client_ids.push(cid);
            offset += entry_size; // skip ClientID + Tag
        }

        let msg_len = u32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        if data.len() < offset + msg_len {
            return None;
        }

        let mut payload = data[offset..offset + msg_len].to_vec();

        // Decompress if compressed (broadcast_flag::COMPRESSED = 0x04)
        if flags & 0x04 != 0 {
            payload = zstd::decode_all(payload.as_slice()).ok()?;
        }

        Some(ServerMessage::Broadcast {
            flags,
            client_ids,
            payload,
        })
    }

    fn decode_heartbeat(data: &[u8]) -> Option<Self> {
        if data.len() < 10 {
            return None;
        }
        Some(ServerMessage::Heartbeat {
            load: u16::from_be_bytes([data[0], data[1]]),
            client_count: u32::from_be_bytes([data[2], data[3], data[4], data[5]]),
            memory_mb: u32::from_be_bytes([data[6], data[7], data[8], data[9]]),
        })
    }

    fn decode_close(data: &[u8]) -> Option<Self> {
        if data.len() < 4 {
            return None;
        }
        let client_id = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        Some(ServerMessage::Close { client_id })
    }
}

/// Parse a client JSON response from the server to extract ACK/NACK/ONCE info.
#[derive(Debug)]
pub enum ClientResponse {
    /// ACK: write was committed
    Ack { request_id: String },
    /// NACK: write was rejected
    Nack { request_id: String, error: String },
    /// ONCE response: read-back value
    Once {
        request_id: String,
        value: serde_json::Value,
    },
    /// Some other message (subscriptions, events, etc.)
    Other(serde_json::Value),
}

impl ClientResponse {
    pub fn parse(json: &[u8]) -> Option<Self> {
        let v: serde_json::Value = serde_json::from_slice(json).ok()?;

        // ACK: {"a": "req-id"}
        if let Some(req_id) = v.get("a").and_then(|v| v.as_str()) {
            return Some(ClientResponse::Ack {
                request_id: req_id.to_string(),
            });
        }

        // NACK: {"n": "req-id", "e": "error-code"}
        if let Some(req_id) = v.get("n").and_then(|v| v.as_str()) {
            let error = v
                .get("e")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            return Some(ClientResponse::Nack {
                request_id: req_id.to_string(),
                error,
            });
        }

        // ONCE response: {"oc": "req-id", "ov": <value>}
        if let Some(req_id) = v.get("oc").and_then(|v| v.as_str()) {
            let value = v.get("ov").cloned().unwrap_or(serde_json::Value::Null);
            return Some(ClientResponse::Once {
                request_id: req_id.to_string(),
                value,
            });
        }

        Some(ClientResponse::Other(v))
    }
}
