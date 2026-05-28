//! Firebase Adapter - Protocol translator for Firebase SDK compatibility.
//!
//! This adapter translates between Firebase's wire protocol and Lark's internal protocol.
//! It's used for connections coming through the proxy layer where the client is using
//! the Firebase SDK.
//!
//! **Proxy mode only**: The proxy handles:
//! - Sending the Firebase hello message
//! - Determining the database from the connection URL
//! - Validating auth tokens (passed as ProxyAuth)
//!
//! The adapter handles:
//! - Frame reassembly for chunked messages (>16KB)
//! - Translating incoming Firebase messages → Lark ClientMessage
//! - Translating outgoing Lark ServerMessage → Firebase format
//! - Path transformation for path-based routing
//! - Swallowing JoinAck (Firebase hello already sent by proxy)

use crate::protocol::ClientMessage;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicI64, Ordering};

// =============================================================================
// Constants
// =============================================================================

/// Maximum size of a single WebSocket frame.
/// Messages larger than this are split into multiple frames with a count prefix.
pub const FIREBASE_MAX_FRAME_SIZE: usize = 16384; // 16KB, matches Firebase SDK

/// Maximum number of frames allowed in a multi-frame message.
/// 16KB per frame × 1024 frames = 16MB max message (matches MAX_WRITE_SIZE).
/// Prevents OOM from malicious frame count like 999999999.
pub const MAX_FRAMES: usize = 1024;

// =============================================================================
// Firebase Wire Protocol Types
// =============================================================================

/// Top-level Firebase message envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirebaseEnvelope {
    /// Message type: "c" = control, "d" = data
    #[serde(rename = "t")]
    pub msg_type: String,

    /// Message data (JSON)
    #[serde(rename = "d")]
    pub data: Value,
}

/// Firebase control message (inside envelope when type="c").
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirebaseControlMessage {
    /// Control type: "h" = hello, "c" = connected, "p" = ping, "o" = pong
    #[serde(rename = "t")]
    pub msg_type: String,

    /// Control data
    #[serde(rename = "d", skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

/// Firebase hello payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirebaseHelloData {
    /// Server timestamp in milliseconds
    #[serde(rename = "ts")]
    pub timestamp: i64,

    /// Protocol version
    #[serde(rename = "v")]
    pub version: String,

    /// Server host
    #[serde(rename = "h")]
    pub host: String,

    /// Session ID
    #[serde(rename = "s")]
    pub session_id: String,
}

/// Firebase data message (inside envelope when type="d").
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirebaseDataMessage {
    /// Request ID (number from client, echoed back by server)
    #[serde(rename = "r", skip_serializing_if = "Option::is_none")]
    pub request_id: Option<Value>,

    /// Action type: "auth", "q" (listen), "n" (unlisten), "g" (get), "p" (put), "m" (merge), etc.
    #[serde(rename = "a", skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,

    /// Message body
    #[serde(rename = "b", skip_serializing_if = "Option::is_none")]
    pub body: Option<Value>,

    /// Tag for subscription routing
    #[serde(rename = "t", skip_serializing_if = "Option::is_none")]
    pub tag: Option<i32>,
}

/// Firebase listen/query body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirebaseListenBody {
    /// Path to subscribe
    #[serde(rename = "p")]
    pub path: String,

    /// Hash for sync (ignored)
    #[serde(rename = "h", skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,

    /// Query parameters - can be {} or [{}]
    #[serde(rename = "q", skip_serializing_if = "Option::is_none")]
    pub query: Option<Value>,

    /// Tag for this subscription
    #[serde(rename = "t", skip_serializing_if = "Option::is_none")]
    pub tag: Option<i32>,
}

/// Firebase query parameters.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FirebaseQuery {
    /// Index: ".key", ".value", ".priority", or child path
    #[serde(rename = "i", skip_serializing_if = "Option::is_none")]
    pub index: Option<String>,

    /// startAt value
    #[serde(rename = "sp", skip_serializing_if = "Option::is_none")]
    pub start_at_val: Option<Value>,

    /// startAt key (tie-breaker)
    #[serde(rename = "sn", skip_serializing_if = "Option::is_none")]
    pub start_at_key: Option<String>,

    /// true = startAt (inclusive), false = startAfter (exclusive)
    #[serde(rename = "sin", skip_serializing_if = "Option::is_none")]
    pub start_inclusive: Option<bool>,

    /// endAt value
    #[serde(rename = "ep", skip_serializing_if = "Option::is_none")]
    pub end_at_val: Option<Value>,

    /// endAt key (tie-breaker)
    #[serde(rename = "en", skip_serializing_if = "Option::is_none")]
    pub end_at_key: Option<String>,

    /// true = endAt (inclusive), false = endBefore (exclusive)
    #[serde(rename = "ein", skip_serializing_if = "Option::is_none")]
    pub end_inclusive: Option<bool>,

    /// Limit count
    #[serde(rename = "l", skip_serializing_if = "Option::is_none")]
    pub limit: Option<i32>,

    /// View from: "l" = limitToFirst, "r" = limitToLast
    #[serde(rename = "vf", skip_serializing_if = "Option::is_none")]
    pub view_from: Option<String>,
}

/// Firebase put body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirebasePutBody {
    /// Path
    #[serde(rename = "p")]
    pub path: String,

    /// Data value
    #[serde(rename = "d")]
    pub data: Value,

    /// Compare-and-swap hash for transactions
    #[serde(rename = "h", skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
}

/// Firebase merge body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirebaseMergeBody {
    /// Path
    #[serde(rename = "p")]
    pub path: String,

    /// Data to merge
    #[serde(rename = "d")]
    pub data: Value,
}

/// Firebase get (once) body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirebaseGetBody {
    /// Path
    #[serde(rename = "p")]
    pub path: String,

    /// Query parameters
    #[serde(rename = "q", skip_serializing_if = "Option::is_none")]
    pub query: Option<Value>,
}

/// Firebase onDisconnect body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirebaseOnDisconnectBody {
    /// Path
    #[serde(rename = "p")]
    pub path: String,

    /// Data value (nil for cancel)
    #[serde(rename = "d", skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

// =============================================================================
// Firebase Adapter
// =============================================================================

/// Project configuration for Firebase compatibility.
#[derive(Debug, Clone, Default)]
pub struct FirebaseConfig {
    /// Database name in simple mode. Typically "default".
    pub default_database: String,

    /// Firebase project ID for RS256 token validation (optional).
    pub firebase_project_id: Option<String>,
}

/// Firebase protocol adapter for translating between Firebase and Lark wire protocols.
///
/// In proxy mode:
/// - `set_joined()` is called immediately (no lazy join)
/// - The proxy sends the Firebase hello
/// - Auth comes from ProxyAuth in CONNECT payload
///
/// In WebSocket mode:
/// - `generate_hello()` is called to send the Firebase hello
/// - `create_join_message()` creates the Lark join message
/// - `create_auto_auth_message()` creates the Lark auth message
pub struct FirebaseAdapter {
    /// Project ID (from ?ns= query param)
    project_id: String,

    /// Server hostname for hello messages
    hostname: String,

    /// Session ID for this connection
    session_id: String,

    /// Database ID (set when joining)
    database_id: Option<String>,

    /// Whether we've joined a Lark database (always true in proxy mode)
    joined: bool,

    /// Track the join request ID so we can swallow JoinAck
    join_request_id: Option<String>,

    /// Track auto-auth request ID so we can swallow its AuthAck
    auto_auth_request_id: Option<String>,

    // Frame reassembly state
    /// Expected number of frames (0 = not in multi-frame mode)
    frame_count: usize,

    /// Accumulated frame data
    frames: Vec<String>,

    /// Total bytes received across frames (for logging)
    frame_bytes_received: usize,
}

impl FirebaseAdapter {
    /// Create a new Firebase adapter.
    ///
    /// For proxy connections, call `set_joined()` immediately after creation.
    pub fn new(project_id: &str, hostname: &str) -> Self {
        Self {
            project_id: project_id.to_string(),
            hostname: hostname.to_string(),
            session_id: generate_session_id(),
            database_id: None,
            joined: false,
            join_request_id: None,
            auto_auth_request_id: None,
            frame_count: 0,
            frames: Vec::new(),
            frame_bytes_received: 0,
        }
    }

    /// Get the project ID.
    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    /// Get the session ID.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Generate the Firebase hello message.
    ///
    /// This should be sent immediately when a WebSocket connection is established,
    /// before any Lark join/auth operations.
    pub fn generate_hello(&self) -> Result<Vec<u8>, String> {
        let hello_payload = FirebaseHelloData {
            timestamp: chrono::Utc::now().timestamp_millis(),
            version: "5".to_string(),
            host: self.hostname.clone(),
            session_id: self.session_id.clone(),
        };

        let control_msg = FirebaseControlMessage {
            msg_type: "h".to_string(),
            data: Some(serde_json::to_value(&hello_payload).map_err(|e| e.to_string())?),
        };

        let envelope = FirebaseEnvelope {
            msg_type: "c".to_string(),
            data: serde_json::to_value(&control_msg).map_err(|e| e.to_string())?,
        };

        serde_json::to_vec(&envelope).map_err(|e| e.to_string())
    }

    /// Create a Lark join message for a specific database.
    ///
    /// The database ID is formatted as "projectID/databaseName" for the Lark protocol.
    pub fn create_join_message(&mut self, database: &str) -> ClientMessage {
        self.join_request_id = Some("fb_join".to_string());
        self.database_id = Some(database.to_string());

        // Format database ID as "projectID/databaseName" for Lark protocol
        let full_database_id = format!("{}/{}", self.project_id, database);

        ClientMessage {
            op: "j".to_string(),
            database: Some(full_database_id),
            request_id: Some("fb_join".to_string()),
            ..Default::default()
        }
    }

    /// Create a Lark auth message for auto-authentication.
    ///
    /// This is sent immediately after join to establish anonymous auth state,
    /// since Firebase SDK doesn't send auth unless the app explicitly authenticates.
    pub fn create_auto_auth_message(&mut self) -> ClientMessage {
        self.auto_auth_request_id = Some("fb_auto_auth".to_string());

        ClientMessage {
            op: "a".to_string(),
            token: Some(String::new()), // Empty token = anonymous
            request_id: Some("fb_auto_auth".to_string()),
            ..Default::default()
        }
    }

    /// Mark the adapter as having joined a Lark database.
    /// Called immediately for proxy connections.
    pub fn set_joined(&mut self) {
        self.joined = true;
    }

    /// Check if we've joined a Lark database.
    pub fn is_joined(&self) -> bool {
        self.joined
    }

    /// Set the join request ID (for swallowing JoinAck).
    pub fn set_join_request_id(&mut self, id: &str) {
        self.join_request_id = Some(id.to_string());
    }

    /// Set the auto-auth request ID (for swallowing AuthAck).
    pub fn set_auto_auth_request_id(&mut self, id: &str) {
        self.auto_auth_request_id = Some(id.to_string());
    }

    // =========================================================================
    // Incoming Message Handling
    // =========================================================================

    /// Process a single WebSocket frame from a Firebase client.
    ///
    /// Firebase SDK splits messages >16KB into multiple frames with a frame count prefix.
    ///
    /// Returns `(message, response)` where:
    /// - `message`: the translated Lark message (None if waiting for more frames or handled internally)
    /// - `response`: optional bytes to send back immediately (keepalive pong)
    pub fn handle_incoming_frame(
        &mut self,
        frame: &[u8],
    ) -> Result<(Option<ClientMessage>, Option<Vec<u8>>), String> {
        let frame_str = String::from_utf8_lossy(frame);

        // Handle keepalive specially - "0" sent as keepalive ping
        // This is NOT a frame count of 0, it's a literal keepalive
        // Firebase servers swallow these without responding
        if frame_str == "0" && self.frame_count == 0 {
            return Ok((None, None));
        }

        // If we're in multi-frame mode, accumulate this frame
        if self.frame_count > 0 {
            self.frames.push(frame_str.to_string());
            self.frame_bytes_received += frame.len();

            // Check if we have all frames
            if self.frames.len() >= self.frame_count {
                // Reassemble the complete message
                let complete_data = self.frames.join("");

                // Reset frame state
                self.frame_count = 0;
                self.frames.clear();
                self.frame_bytes_received = 0;

                // Now translate the complete message
                return self.translate_incoming(complete_data.as_bytes());
            }

            // Still waiting for more frames
            return Ok((None, None));
        }

        // Not in multi-frame mode - check if this is a frame count or a complete message
        // Frame count is a short numeric string (≤6 chars per Firebase SDK)
        if frame_str.len() <= 6
            && let Ok(count) = frame_str.parse::<usize>()
            && count > 0
        {
            // Validate frame count to prevent OOM attacks
            if count > MAX_FRAMES {
                return Err(format!(
                    "frame count {} exceeds maximum allowed ({})",
                    count, MAX_FRAMES
                ));
            }
            // This is a frame count - start multi-frame mode
            self.frame_count = count;
            self.frames = Vec::with_capacity(count);
            self.frame_bytes_received = 0;
            return Ok((None, None));
        }

        // Single-frame message - translate directly
        self.translate_incoming(frame)
    }

    /// Translate a complete Firebase message to Lark format.
    ///
    /// Returns `(message, response)` where:
    /// - `message`: the translated Lark message (None if message should be ignored)
    /// - `response`: optional bytes to send back to the client (e.g., keepalive pong)
    pub fn translate_incoming(
        &self,
        data: &[u8],
    ) -> Result<(Option<ClientMessage>, Option<Vec<u8>>), String> {
        // Handle keepalive (just "0") - Firebase servers swallow these without responding
        if data.len() == 1 && data[0] == b'0' {
            return Ok((None, None));
        }

        let envelope: FirebaseEnvelope =
            serde_json::from_slice(data).map_err(|e| format!("invalid firebase message: {}", e))?;

        // Control messages - handle pings, ignore others
        if envelope.msg_type == "c" {
            let control_msg: FirebaseControlMessage = serde_json::from_value(envelope.data.clone())
                .map_err(|e| format!("invalid control message: {}", e))?;

            if control_msg.msg_type == "p" {
                // Firebase ping - return pong response
                let pong = json!({"t": "c", "d": {"t": "o", "d": {}}});
                return Ok((None, Some(serde_json::to_vec(&pong).unwrap_or_default())));
            }

            // Other control messages (shutdown, reset, etc.) are server-initiated
            return Ok((None, None));
        }

        // Parse data message
        let data_msg: FirebaseDataMessage = serde_json::from_value(envelope.data)
            .map_err(|e| format!("invalid firebase data message: {}", e))?;

        // Convert request ID to string
        let req_id = format_request_id(&data_msg.request_id);

        // Route based on action
        let action = data_msg.action.as_deref().unwrap_or("");
        match action {
            "s" => Ok((None, None)), // Stats - ignore

            // Auth is resolved entirely by the edge, which validates the token and
            // pushes the result to the backend out-of-band (AUTH_CHANGED). The edge
            // also forwards the raw auth frame here and answers the client itself, so
            // the backend must stay silent on it — ignore, don't translate or respond.
            "auth" => Ok((None, None)),

            "q" => self.translate_listen(&req_id, data_msg.body.as_ref(), data_msg.tag),

            "n" => self.translate_unlisten(&req_id, data_msg.body.as_ref()),

            "g" => self.translate_get(&req_id, data_msg.body.as_ref()),

            "p" => self.translate_put(&req_id, data_msg.body.as_ref()),

            "m" => self.translate_merge(&req_id, data_msg.body.as_ref()),

            "o" => self.translate_ondisconnect_put(&req_id, data_msg.body.as_ref()),

            "om" => self.translate_ondisconnect_merge(&req_id, data_msg.body.as_ref()),

            "oc" => self.translate_ondisconnect_cancel(&req_id, data_msg.body.as_ref()),

            _ => Err(format!("unknown firebase action: {}", action)),
        }
    }

    fn translate_listen(
        &self,
        req_id: &str,
        body: Option<&Value>,
        envelope_tag: Option<i32>,
    ) -> Result<(Option<ClientMessage>, Option<Vec<u8>>), String> {
        let body = body.ok_or("missing listen body")?;
        let listen_body: FirebaseListenBody = serde_json::from_value(body.clone())
            .map_err(|e| format!("invalid listen body: {}", e))?;

        let internal_path = ensure_leading_slash(&listen_body.path);

        let mut msg = ClientMessage {
            op: crate::protocol::op::SUBSCRIBE.to_string(),
            path: Some(internal_path),
            request_id: Some(req_id.to_string()),
            ..Default::default()
        };

        // Set tag if provided (from body or envelope)
        let effective_tag = listen_body.tag.or(envelope_tag);
        if let Some(tag) = effective_tag
            && tag != 0
        {
            msg.tag = Some(tag);
        }

        // Parse query parameters
        if let Some(query) = &listen_body.query {
            let firebase_query = parse_firebase_query(query)?;
            apply_query_params(&mut msg, &firebase_query);
        }

        Ok((Some(msg), None))
    }

    fn translate_unlisten(
        &self,
        req_id: &str,
        body: Option<&Value>,
    ) -> Result<(Option<ClientMessage>, Option<Vec<u8>>), String> {
        let body = body.ok_or("missing unlisten body")?;
        let listen_body: FirebaseListenBody = serde_json::from_value(body.clone())
            .map_err(|e| format!("invalid unlisten body: {}", e))?;

        let internal_path = ensure_leading_slash(&listen_body.path);

        let mut msg = ClientMessage {
            op: crate::protocol::op::UNSUBSCRIBE.to_string(),
            path: Some(internal_path),
            request_id: Some(req_id.to_string()),
            ..Default::default()
        };

        // Set tag if provided
        if let Some(tag) = listen_body.tag
            && tag != 0
        {
            msg.tag = Some(tag);
        }

        // Parse query parameters
        if let Some(query) = &listen_body.query {
            let firebase_query = parse_firebase_query(query)?;
            apply_query_params(&mut msg, &firebase_query);
        }

        Ok((Some(msg), None))
    }

    fn translate_get(
        &self,
        req_id: &str,
        body: Option<&Value>,
    ) -> Result<(Option<ClientMessage>, Option<Vec<u8>>), String> {
        let body = body.ok_or("missing get body")?;
        let get_body: FirebaseGetBody =
            serde_json::from_value(body.clone()).map_err(|e| format!("invalid get body: {}", e))?;

        let internal_path = ensure_leading_slash(&get_body.path);

        let mut msg = ClientMessage {
            op: crate::protocol::op::ONCE.to_string(),
            path: Some(internal_path),
            request_id: Some(req_id.to_string()),
            ..Default::default()
        };

        // Parse query parameters
        if let Some(query) = &get_body.query {
            let firebase_query = parse_firebase_query(query)?;
            apply_query_params(&mut msg, &firebase_query);
        }

        Ok((Some(msg), None))
    }

    fn translate_put(
        &self,
        req_id: &str,
        body: Option<&Value>,
    ) -> Result<(Option<ClientMessage>, Option<Vec<u8>>), String> {
        let body = body.ok_or("missing put body")?;
        let put_body: FirebasePutBody =
            serde_json::from_value(body.clone()).map_err(|e| format!("invalid put body: {}", e))?;

        let internal_path = ensure_leading_slash(&put_body.path);

        let mut msg = ClientMessage {
            op: crate::protocol::op::SET.to_string(),
            path: Some(internal_path),
            value: Some(put_body.data),
            request_id: Some(req_id.to_string()),
            ..Default::default()
        };

        // Hash for compare-and-swap
        if let Some(hash) = put_body.hash {
            msg.hash = Some(hash);
            msg.hash_provided = Some(true);
        }

        Ok((Some(msg), None))
    }

    fn translate_merge(
        &self,
        req_id: &str,
        body: Option<&Value>,
    ) -> Result<(Option<ClientMessage>, Option<Vec<u8>>), String> {
        let body = body.ok_or("missing merge body")?;
        let merge_body: FirebaseMergeBody = serde_json::from_value(body.clone())
            .map_err(|e| format!("invalid merge body: {}", e))?;

        let base_path = ensure_leading_slash(&merge_body.path);

        // Check if this is a multi-path update (keys contain slashes)
        if let Value::Object(data) = &merge_body.data {
            let has_path_keys = data.keys().any(|k| k.contains('/'));

            if has_path_keys {
                // Convert to transaction with individual set/delete operations
                let mut ops = Vec::new();

                for (key, value) in data {
                    // Build full path
                    let full_path = if base_path == "/" {
                        format!("/{}", key)
                    } else {
                        format!("{}/{}", base_path, key)
                    };

                    // Check if value is null (delete) or a real value (set)
                    if value.is_null() {
                        ops.push(crate::protocol::TransactionOp {
                            op: "d".to_string(),
                            path: full_path,
                            value: None,
                            hash: None,
                        });
                    } else {
                        ops.push(crate::protocol::TransactionOp {
                            op: "s".to_string(),
                            path: full_path,
                            value: Some(value.clone()),
                            hash: None,
                        });
                    }
                }

                return Ok((
                    Some(ClientMessage {
                        op: crate::protocol::op::TRANSACTION.to_string(),
                        request_id: Some(req_id.to_string()),
                        operations: Some(ops),
                        ..Default::default()
                    }),
                    None,
                ));
            }
        }

        // Regular merge (no path keys)
        Ok((
            Some(ClientMessage {
                op: crate::protocol::op::UPDATE.to_string(),
                path: Some(base_path),
                value: Some(merge_body.data),
                request_id: Some(req_id.to_string()),
                ..Default::default()
            }),
            None,
        ))
    }

    fn translate_ondisconnect_put(
        &self,
        req_id: &str,
        body: Option<&Value>,
    ) -> Result<(Option<ClientMessage>, Option<Vec<u8>>), String> {
        let body = body.ok_or("missing ondisconnect body")?;
        let od_body: FirebaseOnDisconnectBody = serde_json::from_value(body.clone())
            .map_err(|e| format!("invalid ondisconnect body: {}", e))?;

        let internal_path = ensure_leading_slash(&od_body.path);

        Ok((
            Some(ClientMessage {
                op: crate::protocol::op::ON_DISCONNECT.to_string(),
                path: Some(internal_path),
                value: od_body.data,
                request_id: Some(req_id.to_string()),
                action: Some(crate::protocol::action::SET.to_string()),
                ..Default::default()
            }),
            None,
        ))
    }

    fn translate_ondisconnect_merge(
        &self,
        req_id: &str,
        body: Option<&Value>,
    ) -> Result<(Option<ClientMessage>, Option<Vec<u8>>), String> {
        let body = body.ok_or("missing ondisconnect body")?;
        let od_body: FirebaseOnDisconnectBody = serde_json::from_value(body.clone())
            .map_err(|e| format!("invalid ondisconnect body: {}", e))?;

        let internal_path = ensure_leading_slash(&od_body.path);

        Ok((
            Some(ClientMessage {
                op: crate::protocol::op::ON_DISCONNECT.to_string(),
                path: Some(internal_path),
                value: od_body.data,
                request_id: Some(req_id.to_string()),
                action: Some(crate::protocol::action::UPDATE.to_string()),
                ..Default::default()
            }),
            None,
        ))
    }

    fn translate_ondisconnect_cancel(
        &self,
        req_id: &str,
        body: Option<&Value>,
    ) -> Result<(Option<ClientMessage>, Option<Vec<u8>>), String> {
        let body = body.ok_or("missing ondisconnect body")?;
        let od_body: FirebaseOnDisconnectBody = serde_json::from_value(body.clone())
            .map_err(|e| format!("invalid ondisconnect body: {}", e))?;

        let internal_path = ensure_leading_slash(&od_body.path);

        Ok((
            Some(ClientMessage {
                op: crate::protocol::op::ON_DISCONNECT.to_string(),
                path: Some(internal_path),
                request_id: Some(req_id.to_string()),
                action: Some(crate::protocol::action::CANCEL.to_string()),
                ..Default::default()
            }),
            None,
        ))
    }

    // =========================================================================
    // Outgoing Message Handling
    // =========================================================================

    /// Translate a Lark server message to Firebase format, splitting into chunks if >16KB.
    ///
    /// If `skip_translation` is true, the data is assumed to already be in Firebase format
    /// and only chunking is performed. This is used for fast event encoding where the
    /// caller has already generated Firebase-format bytes.
    ///
    /// Returns `None` if the message should be swallowed (e.g., JoinAck).
    pub fn translate_outgoing_chunked(
        &self,
        data: &[u8],
        skip_translation: bool,
    ) -> Result<Option<Vec<Vec<u8>>>, String> {
        // Get the Firebase-format bytes
        let translated = if skip_translation {
            // Data is already in Firebase format, use as-is
            data.to_vec()
        } else {
            // Translate from Lark format to Firebase format
            match self.translate_outgoing(data)? {
                Some(t) => t,
                None => return Ok(None), // Message swallowed
            }
        };

        // Check if chunking is needed
        if translated.len() <= FIREBASE_MAX_FRAME_SIZE {
            return Ok(Some(vec![translated]));
        }

        // Split into chunks
        let chunks = split_into_chunks(&translated, FIREBASE_MAX_FRAME_SIZE);

        // Prepend frame count
        let mut result = Vec::with_capacity(chunks.len() + 1);
        result.push(chunks.len().to_string().into_bytes());
        result.extend(chunks);

        Ok(Some(result))
    }

    /// Translate a Lark server message to Firebase format.
    ///
    /// In proxy mode, JoinAck is swallowed since Firebase hello was sent by the proxy.
    /// For messages >16KB, use `translate_outgoing_chunked` instead.
    pub fn translate_outgoing(&self, data: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let msg: Value =
            serde_json::from_slice(data).map_err(|e| format!("invalid lark message: {}", e))?;

        let msg_obj = msg
            .as_object()
            .ok_or_else(|| "lark message is not an object".to_string())?;

        // Check for JoinAck - always swallow (Firebase hello already sent)
        if msg_obj.contains_key("jc") {
            return Ok(None);
        }

        // Check for AuthAck
        if let Some(ac) = msg_obj.get("ac") {
            let req_id = ac.as_str().unwrap_or("");

            // Swallow auto-auth ack
            if let Some(ref auto_auth_id) = self.auto_auth_request_id
                && req_id == auto_auth_id
            {
                return Ok(None);
            }

            // Real auth from client - translate to Firebase auth response
            return self.translate_auth_ack(req_id, msg_obj);
        }

        // Check for ack
        if let Some(ack) = msg_obj.get("a") {
            let req_id = ack.as_str().unwrap_or("");
            return self.translate_ack(req_id);
        }

        // Check for nack
        if let Some(nack) = msg_obj.get("n") {
            let req_id = nack.as_str().unwrap_or("");
            return self.translate_nack(req_id, msg_obj);
        }

        // Check for event
        if let Some(ev) = msg_obj.get("ev") {
            let event_type = ev.as_str().unwrap_or("");
            return self.translate_event(event_type, msg_obj);
        }

        // Check for once response
        if msg_obj.contains_key("oc") {
            return self.translate_once_response(msg_obj);
        }

        // Check for Lark ping - translate to Firebase pong
        if let Some(op) = msg_obj.get("o")
            && op.as_str() == Some("pi")
        {
            let pong = json!({"t": "c", "d": {"t": "o", "d": {}}});
            return Ok(Some(serde_json::to_vec(&pong).unwrap_or_default()));
        }

        // Unknown message type - pass through wrapped in envelope
        self.wrap_data_message(data)
    }

    fn translate_auth_ack(
        &self,
        req_id: &str,
        msg: &serde_json::Map<String, Value>,
    ) -> Result<Option<Vec<u8>>, String> {
        // Extract auth UID if present
        let mut auth_obj = serde_json::Map::new();
        if let Some(uid) = msg.get("au")
            && let Some(uid_str) = uid.as_str()
            && !uid_str.is_empty()
        {
            auth_obj.insert("uid".to_string(), json!(uid_str));
        }

        // Firebase expects: {"s":"ok","d":{"auth":{...}}}
        let response = json!({
            "s": "ok",
            "d": {
                "auth": auth_obj
            }
        });

        self.wrap_data_response(req_id, &response)
    }

    fn translate_ack(&self, req_id: &str) -> Result<Option<Vec<u8>>, String> {
        // Firebase ack format: {"s":"ok","d":""}
        let response = json!({
            "s": "ok",
            "d": ""
        });

        self.wrap_data_response(req_id, &response)
    }

    fn translate_nack(
        &self,
        req_id: &str,
        msg: &serde_json::Map<String, Value>,
    ) -> Result<Option<Vec<u8>>, String> {
        let mut err_code = "error".to_string();
        let mut err_msg = String::new();

        if let Some(e) = msg.get("e")
            && let Some(s) = e.as_str()
        {
            err_code = s.to_string();
        }
        if let Some(m) = msg.get("m")
            && let Some(s) = m.as_str()
        {
            err_msg = s.to_string();
        }

        // Translate Lark error codes to Firebase equivalents
        if err_code == "condition_failed" {
            err_code = "datastale".to_string();
        }

        let response = json!({
            "s": err_code,
            "d": err_msg
        });

        self.wrap_data_response(req_id, &response)
    }

    fn translate_event(
        &self,
        event_type: &str,
        msg: &serde_json::Map<String, Value>,
    ) -> Result<Option<Vec<u8>>, String> {
        let sub_path = msg.get("sp").and_then(|v| v.as_str()).unwrap_or("");
        let rel_path = msg.get("p").and_then(|v| v.as_str()).unwrap_or("");
        let value = msg.get("v").cloned().unwrap_or(Value::Null);

        // Apply path transformation - add prefix back to external path
        let external_sub_path = sub_path.to_string();

        // Build the full path
        let full_path = if rel_path.is_empty() || rel_path == "/" {
            external_sub_path.clone()
        } else if external_sub_path == "/" {
            rel_path.to_string()
        } else {
            format!("{}{}", external_sub_path, rel_path)
        };

        // Remove leading slash for Firebase format
        let full_path = full_path.strip_prefix('/').unwrap_or(&full_path);

        let action = if event_type == "patch" { "m" } else { "d" };

        let mut body = json!({
            "p": full_path,
            "d": value
        });

        // Include tag if present
        if let Some(tag) = msg.get("tag")
            && let Some(tag_num) = tag.as_i64()
        {
            body["t"] = json!(tag_num);
        }

        let data_msg = json!({
            "a": action,
            "b": body
        });

        self.wrap_data_message_value(&data_msg)
    }

    fn translate_once_response(
        &self,
        msg: &serde_json::Map<String, Value>,
    ) -> Result<Option<Vec<u8>>, String> {
        let req_id = msg.get("oc").and_then(|v| v.as_str()).unwrap_or("");
        let value = msg.get("ov").cloned().unwrap_or(Value::Null);

        let response = json!({
            "s": "ok",
            "d": value
        });

        self.wrap_data_response(req_id, &response)
    }

    fn wrap_data_message(&self, data: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let inner: Value =
            serde_json::from_slice(data).map_err(|e| format!("invalid json: {}", e))?;

        let envelope = json!({
            "t": "d",
            "d": inner
        });

        Ok(Some(
            serde_json::to_vec(&envelope).map_err(|e| format!("json encode error: {}", e))?,
        ))
    }

    fn wrap_data_message_value(&self, data_msg: &Value) -> Result<Option<Vec<u8>>, String> {
        let envelope = json!({
            "t": "d",
            "d": data_msg
        });

        Ok(Some(
            serde_json::to_vec(&envelope).map_err(|e| format!("json encode error: {}", e))?,
        ))
    }

    fn wrap_data_response(
        &self,
        req_id: &str,
        response: &Value,
    ) -> Result<Option<Vec<u8>>, String> {
        // Try to convert reqID back to number if it was originally a number
        let req_id_val: Value = req_id
            .parse::<i64>()
            .map(|n| json!(n))
            .unwrap_or_else(|_| json!(req_id));

        let data_msg = json!({
            "r": req_id_val,
            "b": response
        });

        self.wrap_data_message_value(&data_msg)
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Format a request ID from Firebase (can be number or string) to a string.
fn format_request_id(id: &Option<Value>) -> String {
    match id {
        Some(Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                i.to_string()
            } else if let Some(f) = n.as_f64() {
                (f as i64).to_string()
            } else {
                n.to_string()
            }
        }
        Some(Value::String(s)) => s.clone(),
        Some(v) => v.to_string(),
        None => String::new(),
    }
}

/// Ensure a path has a leading slash.
fn ensure_leading_slash(path: &str) -> String {
    if path.is_empty() {
        return "/".to_string();
    }
    if !path.starts_with('/') {
        return format!("/{}", path);
    }
    path.to_string()
}

/// Parse Firebase query parameters from either object or array format.
fn parse_firebase_query(query: &Value) -> Result<FirebaseQuery, String> {
    match query {
        Value::Array(arr) if !arr.is_empty() => serde_json::from_value(arr[0].clone())
            .map_err(|e| format!("invalid query array: {}", e)),
        Value::Object(_) => serde_json::from_value(query.clone())
            .map_err(|e| format!("invalid query object: {}", e)),
        _ => Ok(FirebaseQuery::default()),
    }
}

/// Apply Firebase query parameters to a Lark ClientMessage.
fn apply_query_params(msg: &mut ClientMessage, q: &FirebaseQuery) {
    // Index/orderBy
    if let Some(ref index) = q.index {
        match index.as_str() {
            ".key" => msg.order_by = Some("key".to_string()),
            ".value" => msg.order_by = Some("value".to_string()),
            ".priority" => msg.order_by = Some("priority".to_string()),
            _ => msg.order_by_child = Some(index.clone()),
        }
    }

    // Limit
    if let Some(limit) = q.limit
        && limit > 0
    {
        if q.view_from.as_deref() == Some("r") {
            msg.limit_to_last = Some(limit);
        } else {
            msg.limit_to_first = Some(limit);
        }
    }

    // Range filters
    // sin: true (or None) = startAt (inclusive), false = startAfter (exclusive)
    // ein: true (or None) = endAt (inclusive), false = endBefore (exclusive)
    if q.start_at_val.is_some() || q.start_at_key.is_some() {
        let is_inclusive = q.start_inclusive.unwrap_or(true);
        if is_inclusive {
            msg.start_at = q.start_at_val.clone();
            if let Some(ref key) = q.start_at_key {
                msg.start_at_key = Some(key.clone());
            }
        } else {
            msg.start_after = q.start_at_val.clone();
            if let Some(ref key) = q.start_at_key {
                msg.start_after_key = Some(key.clone());
            }
        }
    }

    if q.end_at_val.is_some() || q.end_at_key.is_some() {
        let is_inclusive = q.end_inclusive.unwrap_or(true);
        if is_inclusive {
            msg.end_at = q.end_at_val.clone();
            if let Some(ref key) = q.end_at_key {
                msg.end_at_key = Some(key.clone());
            }
        } else {
            msg.end_before = q.end_at_val.clone();
            if let Some(ref key) = q.end_at_key {
                msg.end_before_key = Some(key.clone());
            }
        }
    }
}

/// Split data into chunks of at most chunk_size bytes.
fn split_into_chunks(data: &[u8], chunk_size: usize) -> Vec<Vec<u8>> {
    if data.len() <= chunk_size {
        return vec![data.to_vec()];
    }

    data.chunks(chunk_size).map(|c| c.to_vec()).collect()
}

/// Global counter for session ID generation (fallback).
static SESSION_ID_COUNTER: AtomicI64 = AtomicI64::new(0);

/// Generate a random-ish session ID.
pub fn generate_session_id() -> String {
    const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut result = String::with_capacity(32);

    for i in 0..32 {
        let counter = SESSION_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        let idx = ((counter + i as i64) % CHARS.len() as i64) as usize;
        result.push(CHARS[idx] as char);
    }

    result
}

// =============================================================================
// Fast Event Encoding (string concatenation, no JSON parsing)
// =============================================================================

/// Encode a Firebase event (put or patch) using fast string concatenation.
///
/// This avoids JSON parsing/serialization by directly constructing the Firebase wire format.
/// The `value_bytes` parameter should be pre-serialized JSON bytes of the value.
///
/// # Arguments
/// * `event_type` - Either "put" or "patch"
/// * `subscription_path` - The Lark subscription path (e.g., "/users")
/// * `relative_path` - The relative path within the subscription (e.g., "/alice" or "/")
/// * `value_bytes` - Pre-serialized JSON bytes of the value
/// * `tag` - Optional subscription tag for query views
///
/// # Returns
/// Firebase wire format bytes ready to send to the client.
pub fn encode_firebase_event(
    event_type: &str,
    subscription_path: &str,
    relative_path: &str,
    value_bytes: &[u8],
    tag: Option<i32>,
) -> Vec<u8> {
    // Build the full external path (Firebase format: no leading slash)
    let full_path = if relative_path.is_empty() || relative_path == "/" {
        subscription_path.to_string()
    } else {
        format!("{}{}", subscription_path, relative_path)
    };

    // Strip leading slash for Firebase format
    let full_path = full_path.trim_start_matches('/');

    // Determine action code: "d" for put (data), "m" for patch (merge)
    let action = if event_type == "patch" { "m" } else { "d" };

    // Estimate capacity: envelope + path + value + optional tag
    let capacity = 60 + full_path.len() + value_bytes.len();
    let mut buf = Vec::with_capacity(capacity);

    // Build: {"t":"d","d":{"a":"ACTION","b":{"p":"PATH","d":VALUE}}}
    // or:    {"t":"d","d":{"a":"ACTION","b":{"p":"PATH","d":VALUE,"t":TAG}}}
    buf.extend_from_slice(b"{\"t\":\"d\",\"d\":{\"a\":\"");
    buf.extend_from_slice(action.as_bytes());
    buf.extend_from_slice(b"\",\"b\":{\"p\":\"");
    buf.extend_from_slice(full_path.as_bytes());
    buf.extend_from_slice(b"\",\"d\":");
    buf.extend_from_slice(value_bytes);

    if let Some(t) = tag {
        buf.extend_from_slice(b",\"t\":");
        buf.extend_from_slice(t.to_string().as_bytes());
    }

    buf.extend_from_slice(b"}}}");
    buf
}

/// Insert a tag into pre-encoded Firebase event bytes.
///
/// This is used when we have already encoded a Firebase event without a tag,
/// and need to create a variant with a specific tag for a query subscriber.
///
/// # Arguments
/// * `firebase_bytes` - Pre-encoded Firebase event bytes (without tag)
/// * `tag` - The tag to insert
///
/// # Returns
/// Firebase bytes with the tag inserted, or a clone of the original if tag insertion fails.
pub fn insert_firebase_tag(firebase_bytes: &[u8], tag: i32) -> Vec<u8> {
    // Find the position to insert the tag: before the final "}}}".
    // The format is: {"t":"d","d":{"a":"d","b":{"p":"...","d":...}}}
    // We want to insert ",\"t\":TAG" before the last "}}}".

    if firebase_bytes.len() < 3 {
        return firebase_bytes.to_vec();
    }

    // Check if it ends with }}}
    let end = &firebase_bytes[firebase_bytes.len() - 3..];
    if end != b"}}}" {
        // Unexpected format, return as-is
        return firebase_bytes.to_vec();
    }

    let tag_str = tag.to_string();
    let insert_bytes = format!(",\"t\":{}", tag_str);

    let mut result = Vec::with_capacity(firebase_bytes.len() + insert_bytes.len());
    result.extend_from_slice(&firebase_bytes[..firebase_bytes.len() - 3]);
    result.extend_from_slice(insert_bytes.as_bytes());
    result.extend_from_slice(b"}}}");
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_request_id() {
        assert_eq!(format_request_id(&Some(json!(42))), "42");
        assert_eq!(format_request_id(&Some(json!(42.0))), "42");
        assert_eq!(format_request_id(&Some(json!("req-1"))), "req-1");
        assert_eq!(format_request_id(&None), "");
    }

    #[test]
    fn test_ensure_leading_slash() {
        assert_eq!(ensure_leading_slash(""), "/");
        assert_eq!(ensure_leading_slash("players"), "/players");
        assert_eq!(ensure_leading_slash("/players"), "/players");
    }

    #[test]
    fn test_keepalive() {
        let mut adapter = FirebaseAdapter::new("proj", "host");
        let (msg, response) = adapter.handle_incoming_frame(b"0").unwrap();
        assert!(msg.is_none());
        // Firebase servers swallow keepalives without responding
        assert!(response.is_none());
    }

    #[test]
    fn test_translate_put() {
        let adapter = FirebaseAdapter::new("proj", "host");

        let firebase_msg = json!({
            "t": "d",
            "d": {
                "r": 1,
                "a": "p",
                "b": {
                    "p": "/players/abc",
                    "d": {"name": "Alice"}
                }
            }
        });

        let (msg, _) = adapter
            .translate_incoming(serde_json::to_vec(&firebase_msg).unwrap().as_slice())
            .unwrap();

        let msg = msg.unwrap();
        assert_eq!(msg.op, "s");
        assert_eq!(msg.path, Some("/players/abc".to_string()));
        assert_eq!(msg.value, Some(json!({"name": "Alice"})));
        assert_eq!(msg.request_id, Some("1".to_string()));
    }

    #[test]
    fn test_translate_listen_with_query() {
        let adapter = FirebaseAdapter::new("proj", "host");

        let firebase_msg = json!({
            "t": "d",
            "d": {
                "r": 2,
                "a": "q",
                "b": {
                    "p": "/scores",
                    "q": {
                        "i": "score",
                        "l": 10,
                        "vf": "r"
                    },
                    "t": 5
                }
            }
        });

        let (msg, _) = adapter
            .translate_incoming(serde_json::to_vec(&firebase_msg).unwrap().as_slice())
            .unwrap();

        let msg = msg.unwrap();
        assert_eq!(msg.op, "sb");
        assert_eq!(msg.path, Some("/scores".to_string()));
        assert_eq!(msg.order_by_child, Some("score".to_string()));
        assert_eq!(msg.limit_to_last, Some(10));
        assert_eq!(msg.tag, Some(5));
    }

    #[test]
    fn test_translate_outgoing_ack() {
        let adapter = FirebaseAdapter::new("proj", "host");

        let lark_msg = json!({"a": "1"});
        let result = adapter
            .translate_outgoing(serde_json::to_vec(&lark_msg).unwrap().as_slice())
            .unwrap();

        let result = result.unwrap();
        let parsed: Value = serde_json::from_slice(&result).unwrap();

        assert_eq!(parsed["t"], "d");
        assert_eq!(parsed["d"]["r"], 1); // Numeric request ID
        assert_eq!(parsed["d"]["b"]["s"], "ok");
    }

    #[test]
    fn test_translate_outgoing_event() {
        let adapter = FirebaseAdapter::new("proj", "host");

        let lark_msg = json!({
            "ev": "put",
            "sp": "/players",
            "p": "/abc",
            "v": {"name": "Alice"}
        });

        let result = adapter
            .translate_outgoing(serde_json::to_vec(&lark_msg).unwrap().as_slice())
            .unwrap();

        let result = result.unwrap();
        let parsed: Value = serde_json::from_slice(&result).unwrap();

        assert_eq!(parsed["t"], "d");
        assert_eq!(parsed["d"]["a"], "d"); // "d" = data action
        assert_eq!(parsed["d"]["b"]["p"], "players/abc"); // No leading slash
        assert_eq!(parsed["d"]["b"]["d"]["name"], "Alice");
    }

    #[test]
    fn test_swallow_join_ack() {
        let adapter = FirebaseAdapter::new("proj", "host");

        let lark_msg = json!({
            "jc": "fb_auto_join",
            "vp": [],
            "cid": "conn-123",
            "st": 1234567890
        });

        let result = adapter
            .translate_outgoing(serde_json::to_vec(&lark_msg).unwrap().as_slice())
            .unwrap();

        assert!(result.is_none()); // JoinAck should be swallowed
    }

    #[test]
    fn test_frame_reassembly() {
        let mut adapter = FirebaseAdapter::new("proj", "host");

        // Simulate 3 frames
        let (msg, _) = adapter.handle_incoming_frame(b"3").unwrap();
        assert!(msg.is_none()); // Waiting for frames

        let (msg, _) = adapter.handle_incoming_frame(b"{\"t\":\"d\",").unwrap();
        assert!(msg.is_none()); // Still waiting

        let (msg, _) = adapter
            .handle_incoming_frame(b"\"d\":{\"r\":1,\"a\":\"p\",")
            .unwrap();
        assert!(msg.is_none()); // Still waiting

        let (msg, _) = adapter
            .handle_incoming_frame(b"\"b\":{\"p\":\"/test\",\"d\":1}}}")
            .unwrap();

        // Now we should have the complete message
        let msg = msg.unwrap();
        assert_eq!(msg.op, "s");
        assert_eq!(msg.path, Some("/test".to_string()));
    }

    #[test]
    fn test_frame_count_limit() {
        let mut adapter = FirebaseAdapter::new("proj", "host");

        // Valid frame count should work
        let result = adapter.handle_incoming_frame(b"100");
        assert!(result.is_ok());

        // Reset adapter
        let mut adapter = FirebaseAdapter::new("proj", "host");

        // Frame count exceeding MAX_FRAMES should error
        let huge_count = format!("{}", MAX_FRAMES + 1);
        let result = adapter.handle_incoming_frame(huge_count.as_bytes());
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exceeds maximum allowed"));
    }

    #[test]
    fn test_split_into_chunks() {
        let data = vec![1u8; 40000]; // 40KB
        let chunks = split_into_chunks(&data, FIREBASE_MAX_FRAME_SIZE);

        assert_eq!(chunks.len(), 3); // 16KB + 16KB + 8KB
        assert_eq!(chunks[0].len(), FIREBASE_MAX_FRAME_SIZE);
        assert_eq!(chunks[1].len(), FIREBASE_MAX_FRAME_SIZE);
        assert_eq!(chunks[2].len(), 40000 - 2 * FIREBASE_MAX_FRAME_SIZE);
    }

    // ==========================================================================
    // Additional tests ported from Go
    // ==========================================================================

    #[test]
    fn test_translate_stats_ignored() {
        let adapter = FirebaseAdapter::new("test-db", "host");

        // Stats message should be ignored
        let stats_msg = json!({
            "t": "d",
            "d": {
                "r": 1,
                "a": "s",
                "b": {"c": {"sdk.js.10-12-2": 1}}
            }
        });

        let (msg, _) = adapter
            .translate_incoming(serde_json::to_vec(&stats_msg).unwrap().as_slice())
            .unwrap();

        assert!(msg.is_none(), "expected nil message for stats");
    }

    #[test]
    fn test_translate_auth_is_ignored() {
        // Auth is owned by the edge; the backend must not translate or respond to
        // forwarded auth frames (the edge already answered the client).
        let adapter = FirebaseAdapter::new("test-db", "host");

        let auth_msg = json!({
            "t": "d",
            "d": {
                "r": 2,
                "a": "auth",
                "b": {"cred": "my-jwt-token"}
            }
        });

        let (msg, response) = adapter
            .translate_incoming(serde_json::to_vec(&auth_msg).unwrap().as_slice())
            .unwrap();

        assert!(
            msg.is_none(),
            "auth frame must not produce a backend message"
        );
        assert!(response.is_none(), "auth frame must not produce a response");
    }

    #[test]
    fn test_translate_listen_simple() {
        let adapter = FirebaseAdapter::new("test-db", "host");

        // Simple listen without query
        let listen_msg = json!({
            "t": "d",
            "d": {
                "r": 3,
                "a": "q",
                "b": {"p": "users/abc", "h": ""}
            }
        });

        let (msg, _) = adapter
            .translate_incoming(serde_json::to_vec(&listen_msg).unwrap().as_slice())
            .unwrap();

        let msg = msg.unwrap();
        assert_eq!(msg.op, crate::protocol::op::SUBSCRIBE);
        assert_eq!(msg.path, Some("/users/abc".to_string()));
        assert_eq!(msg.request_id, Some("3".to_string()));
    }

    #[test]
    fn test_translate_listen_order_by_key() {
        let adapter = FirebaseAdapter::new("test-db", "host");

        let listen_msg = json!({
            "t": "d",
            "d": {
                "r": 5,
                "a": "q",
                "b": {"p": "items", "q": [{"i": ".key"}]}
            }
        });

        let (msg, _) = adapter
            .translate_incoming(serde_json::to_vec(&listen_msg).unwrap().as_slice())
            .unwrap();

        let msg = msg.unwrap();
        assert_eq!(msg.order_by, Some("key".to_string()));
    }

    #[test]
    fn test_translate_listen_order_by_value() {
        let adapter = FirebaseAdapter::new("test-db", "host");

        let listen_msg = json!({
            "t": "d",
            "d": {
                "r": 5,
                "a": "q",
                "b": {"p": "scores", "q": [{"i": ".value"}]}
            }
        });

        let (msg, _) = adapter
            .translate_incoming(serde_json::to_vec(&listen_msg).unwrap().as_slice())
            .unwrap();

        let msg = msg.unwrap();
        assert_eq!(msg.order_by, Some("value".to_string()));
    }

    #[test]
    fn test_translate_unlisten() {
        let adapter = FirebaseAdapter::new("test-db", "host");

        let unlisten_msg = json!({
            "t": "d",
            "d": {
                "r": 6,
                "a": "n",
                "b": {"p": "users/abc"}
            }
        });

        let (msg, _) = adapter
            .translate_incoming(serde_json::to_vec(&unlisten_msg).unwrap().as_slice())
            .unwrap();

        let msg = msg.unwrap();
        assert_eq!(msg.op, crate::protocol::op::UNSUBSCRIBE);
        assert_eq!(msg.path, Some("/users/abc".to_string()));
    }

    #[test]
    fn test_translate_merge() {
        let adapter = FirebaseAdapter::new("test-db", "host");

        let merge_msg = json!({
            "t": "d",
            "d": {
                "r": 8,
                "a": "m",
                "b": {
                    "p": "users/abc",
                    "d": {"name": "Alice", "score": 100}
                }
            }
        });

        let (msg, _) = adapter
            .translate_incoming(serde_json::to_vec(&merge_msg).unwrap().as_slice())
            .unwrap();

        let msg = msg.unwrap();
        assert_eq!(msg.op, crate::protocol::op::UPDATE);
        assert_eq!(msg.path, Some("/users/abc".to_string()));
    }

    #[test]
    fn test_translate_ondisconnect_put() {
        let adapter = FirebaseAdapter::new("test-db", "host");

        let od_msg = json!({
            "t": "d",
            "d": {
                "r": 9,
                "a": "o",
                "b": {"p": "users/abc/online", "d": false}
            }
        });

        let (msg, _) = adapter
            .translate_incoming(serde_json::to_vec(&od_msg).unwrap().as_slice())
            .unwrap();

        let msg = msg.unwrap();
        assert_eq!(msg.op, crate::protocol::op::ON_DISCONNECT);
        assert_eq!(msg.action, Some(crate::protocol::action::SET.to_string()));
        assert_eq!(msg.path, Some("/users/abc/online".to_string()));
    }

    #[test]
    fn test_translate_ondisconnect_cancel() {
        let adapter = FirebaseAdapter::new("test-db", "host");

        let od_msg = json!({
            "t": "d",
            "d": {
                "r": 10,
                "a": "oc",
                "b": {"p": "users/abc/online"}
            }
        });

        let (msg, _) = adapter
            .translate_incoming(serde_json::to_vec(&od_msg).unwrap().as_slice())
            .unwrap();

        let msg = msg.unwrap();
        assert_eq!(msg.op, crate::protocol::op::ON_DISCONNECT);
        assert_eq!(
            msg.action,
            Some(crate::protocol::action::CANCEL.to_string())
        );
    }

    #[test]
    fn test_translate_outgoing_nack() {
        let adapter = FirebaseAdapter::new("test-db", "host");

        let lark_nack = json!({
            "n": "6",
            "e": "permission_denied",
            "m": "Access denied"
        });

        let result = adapter
            .translate_outgoing(serde_json::to_vec(&lark_nack).unwrap().as_slice())
            .unwrap();

        let result = result.unwrap();
        let parsed: Value = serde_json::from_slice(&result).unwrap();

        assert_eq!(parsed["d"]["b"]["s"], "permission_denied");
    }

    #[test]
    fn test_translate_outgoing_patch_event() {
        let adapter = FirebaseAdapter::new("test-db", "host");

        let lark_event = json!({
            "ev": "patch",
            "sp": "/users",
            "p": "/",
            "v": {"/abc/name": "Alice"}
        });

        let result = adapter
            .translate_outgoing(serde_json::to_vec(&lark_event).unwrap().as_slice())
            .unwrap();

        let result = result.unwrap();
        let parsed: Value = serde_json::from_slice(&result).unwrap();

        assert_eq!(parsed["d"]["a"], "m"); // "m" = merge action for patch
    }

    #[test]
    fn test_translate_auth_ack() {
        let adapter = FirebaseAdapter::new("test-db", "host");

        let auth_ack = json!({
            "ac": "2",
            "au": "user-123"
        });

        let result = adapter
            .translate_outgoing(serde_json::to_vec(&auth_ack).unwrap().as_slice())
            .unwrap();

        let result = result.unwrap();
        let parsed: Value = serde_json::from_slice(&result).unwrap();

        assert_eq!(parsed["t"], "d");
        assert_eq!(parsed["d"]["r"], 2); // Numeric request ID
        assert_eq!(parsed["d"]["b"]["s"], "ok");
        assert_eq!(parsed["d"]["b"]["d"]["auth"]["uid"], "user-123");
    }

    #[test]
    fn test_translate_listen_with_tag() {
        let adapter = FirebaseAdapter::new("test-db", "host");

        let listen_msg = json!({
            "t": "d",
            "d": {
                "r": 5,
                "a": "q",
                "b": {"p": "users", "t": 42}
            }
        });

        let (msg, _) = adapter
            .translate_incoming(serde_json::to_vec(&listen_msg).unwrap().as_slice())
            .unwrap();

        let msg = msg.unwrap();
        assert_eq!(msg.op, crate::protocol::op::SUBSCRIBE);
        assert_eq!(msg.tag, Some(42));
    }

    #[test]
    fn test_translate_outgoing_event_with_tag() {
        let adapter = FirebaseAdapter::new("test-db", "host");

        let lark_msg = json!({
            "ev": "put",
            "sp": "/users",
            "p": "/alice",
            "v": {"name": "Alice"},
            "tag": 42
        });

        let result = adapter
            .translate_outgoing(serde_json::to_vec(&lark_msg).unwrap().as_slice())
            .unwrap();

        let result = result.unwrap();
        let parsed: Value = serde_json::from_slice(&result).unwrap();

        assert_eq!(parsed["d"]["b"]["t"], 42);
    }

    #[test]
    fn test_translate_outgoing_event_without_tag() {
        let adapter = FirebaseAdapter::new("test-db", "host");

        let lark_msg = json!({
            "ev": "put",
            "sp": "/users",
            "p": "/alice",
            "v": {"name": "Alice"}
        });

        let result = adapter
            .translate_outgoing(serde_json::to_vec(&lark_msg).unwrap().as_slice())
            .unwrap();

        let result = result.unwrap();
        let parsed: Value = serde_json::from_slice(&result).unwrap();

        // Tag should be absent
        assert!(parsed["d"]["b"]["t"].is_null());
    }

    #[test]
    fn test_translate_unlisten_with_query() {
        let adapter = FirebaseAdapter::new("test-db", "host");

        let unlisten_msg = json!({
            "t": "d",
            "d": {
                "r": 8,
                "a": "n",
                "b": {
                    "p": "items",
                    "q": [{"i": ".value", "l": 5}]
                }
            }
        });

        let (msg, _) = adapter
            .translate_incoming(serde_json::to_vec(&unlisten_msg).unwrap().as_slice())
            .unwrap();

        let msg = msg.unwrap();
        assert_eq!(msg.op, crate::protocol::op::UNSUBSCRIBE);
        assert_eq!(msg.order_by, Some("value".to_string()));
        assert_eq!(msg.limit_to_first, Some(5));
    }

    #[test]
    fn test_translate_outgoing_chunked_small_message() {
        let adapter = FirebaseAdapter::new("test-project", "host");

        // Small ack message (well under 16KB)
        let ack = json!({"a": "r1"});

        let chunks = adapter
            .translate_outgoing_chunked(serde_json::to_vec(&ack).unwrap().as_slice(), false)
            .unwrap();

        let chunks = chunks.unwrap();
        assert_eq!(chunks.len(), 1, "expected 1 chunk for small message");
    }

    #[test]
    fn test_translate_outgoing_chunked_large_message() {
        let adapter = FirebaseAdapter::new("test-project", "host");

        // Create a large event message (>16KB)
        let mut large_value = serde_json::Map::new();
        for i in 0..500 {
            let key = format!("key_{:04}", i);
            large_value.insert(
                key,
                json!("some moderately long string value that takes up space"),
            );
        }

        let event = json!({
            "ev": "put",
            "sp": "/data",
            "p": "/",
            "v": large_value
        });

        let chunks = adapter
            .translate_outgoing_chunked(serde_json::to_vec(&event).unwrap().as_slice(), false)
            .unwrap();

        let chunks = chunks.unwrap();

        // Should have frame count + multiple data chunks
        assert!(
            chunks.len() >= 2,
            "expected multiple chunks for large message"
        );

        // Each data chunk should be ≤16KB
        for (i, chunk) in chunks.iter().skip(1).enumerate() {
            assert!(
                chunk.len() <= FIREBASE_MAX_FRAME_SIZE,
                "chunk {} exceeds max frame size: {} > {}",
                i + 1,
                chunk.len(),
                FIREBASE_MAX_FRAME_SIZE
            );
        }
    }

    #[test]
    fn test_translate_get() {
        let adapter = FirebaseAdapter::new("test-db", "host");

        let get_msg = json!({
            "t": "d",
            "d": {
                "r": 11,
                "a": "g",
                "b": {"p": "users/abc"}
            }
        });

        let (msg, _) = adapter
            .translate_incoming(serde_json::to_vec(&get_msg).unwrap().as_slice())
            .unwrap();

        let msg = msg.unwrap();
        assert_eq!(msg.op, crate::protocol::op::ONCE);
        assert_eq!(msg.path, Some("/users/abc".to_string()));
        assert_eq!(msg.request_id, Some("11".to_string()));
    }

    #[test]
    fn test_translate_merge_with_path_keys() {
        let adapter = FirebaseAdapter::new("test-db", "host");

        // Merge with path keys (multi-path update) should become a transaction
        let merge_msg = json!({
            "t": "d",
            "d": {
                "r": 12,
                "a": "m",
                "b": {
                    "p": "/",
                    "d": {
                        "users/abc/name": "Alice",
                        "users/abc/score": 100,
                        "users/def/name": "Other"
                    }
                }
            }
        });

        let (msg, _) = adapter
            .translate_incoming(serde_json::to_vec(&merge_msg).unwrap().as_slice())
            .unwrap();

        let msg = msg.unwrap();
        assert_eq!(msg.op, crate::protocol::op::TRANSACTION);
        assert!(msg.operations.is_some());
        let ops = msg.operations.unwrap();
        assert_eq!(ops.len(), 3);
    }

    #[test]
    fn test_translate_once_response() {
        let adapter = FirebaseAdapter::new("test-db", "host");

        let once_response = json!({
            "oc": "11",
            "ov": {"name": "Alice", "score": 100}
        });

        let result = adapter
            .translate_outgoing(serde_json::to_vec(&once_response).unwrap().as_slice())
            .unwrap();

        let result = result.unwrap();
        let parsed: Value = serde_json::from_slice(&result).unwrap();

        assert_eq!(parsed["t"], "d");
        assert_eq!(parsed["d"]["r"], 11); // Numeric request ID
        assert_eq!(parsed["d"]["b"]["s"], "ok");
        assert_eq!(parsed["d"]["b"]["d"]["name"], "Alice");
    }

    // =========================================================================
    // Fast Event Encoding Tests
    // =========================================================================

    #[test]
    fn test_encode_firebase_event_put() {
        let value_bytes = br#"{"name":"Alice"}"#;
        let result = encode_firebase_event("put", "/users", "/alice", value_bytes, None);

        let parsed: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(parsed["t"], "d");
        assert_eq!(parsed["d"]["a"], "d"); // "d" = data action for put
        assert_eq!(parsed["d"]["b"]["p"], "users/alice"); // no leading slash
        assert_eq!(parsed["d"]["b"]["d"]["name"], "Alice");
        assert!(parsed["d"]["b"].get("t").is_none()); // no tag
    }

    #[test]
    fn test_encode_firebase_event_patch() {
        let value_bytes = br#"{"/name":"Bob"}"#;
        let result = encode_firebase_event("patch", "/users", "/bob", value_bytes, None);

        let parsed: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(parsed["t"], "d");
        assert_eq!(parsed["d"]["a"], "m"); // "m" = merge action for patch
        assert_eq!(parsed["d"]["b"]["p"], "users/bob");
    }

    #[test]
    fn test_encode_firebase_event_with_tag() {
        let value_bytes = br#"100"#;
        let result = encode_firebase_event("put", "/scores", "/player1", value_bytes, Some(7));

        let parsed: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(parsed["d"]["b"]["t"], 7);
    }

    #[test]
    fn test_encode_firebase_event_root_path() {
        // When subscription path is "/" and relative path is "/"
        let value_bytes = br#"{"data":"value"}"#;
        let result = encode_firebase_event("put", "/", "/", value_bytes, None);

        let parsed: Value = serde_json::from_slice(&result).unwrap();
        // Empty path after stripping leading slash
        assert_eq!(parsed["d"]["b"]["p"], "");
    }

    #[test]
    fn test_insert_firebase_tag() {
        // Create a Firebase event without tag
        let base = encode_firebase_event("put", "/users", "/alice", br#"{"name":"Alice"}"#, None);

        // Insert a tag
        let with_tag = insert_firebase_tag(&base, 42);

        let parsed: Value = serde_json::from_slice(&with_tag).unwrap();
        assert_eq!(parsed["t"], "d");
        assert_eq!(parsed["d"]["a"], "d");
        assert_eq!(parsed["d"]["b"]["p"], "users/alice");
        assert_eq!(parsed["d"]["b"]["d"]["name"], "Alice");
        assert_eq!(parsed["d"]["b"]["t"], 42);
    }

    #[test]
    fn test_insert_firebase_tag_negative() {
        let base = encode_firebase_event("put", "/data", "/", br#"null"#, None);

        let with_tag = insert_firebase_tag(&base, -5);

        let parsed: Value = serde_json::from_slice(&with_tag).unwrap();
        assert_eq!(parsed["d"]["b"]["t"], -5);
    }

    #[test]
    fn test_insert_firebase_tag_preserves_content() {
        // Ensure that inserting a tag doesn't corrupt the JSON
        let value_bytes = br#"{"complex":{"nested":true},"array":[1,2,3]}"#;
        let base = encode_firebase_event("patch", "/path", "/sub", value_bytes, None);

        let with_tag = insert_firebase_tag(&base, 999);

        let parsed: Value = serde_json::from_slice(&with_tag).unwrap();
        assert_eq!(parsed["d"]["a"], "m");
        assert_eq!(parsed["d"]["b"]["p"], "path/sub");
        assert_eq!(parsed["d"]["b"]["d"]["complex"]["nested"], true);
        assert_eq!(parsed["d"]["b"]["d"]["array"][0], 1);
        assert_eq!(parsed["d"]["b"]["t"], 999);
    }

    #[test]
    fn test_insert_firebase_tag_empty_object() {
        // Edge case: VALUE is an empty object {}
        // Message ends with }}}} not }}}
        let base = encode_firebase_event("put", "/data", "/", br#"{}"#, None);

        // Verify the base ends with }}}} (object close + 3 envelope closes)
        assert!(base.ends_with(b"}}}}"));

        let with_tag = insert_firebase_tag(&base, 42);

        // Should still parse correctly
        let parsed: Value = serde_json::from_slice(&with_tag).unwrap();
        assert_eq!(parsed["d"]["b"]["t"], 42);
        // The 'd' value should be an empty object
        assert!(parsed["d"]["b"]["d"].is_object());
        assert_eq!(parsed["d"]["b"]["d"].as_object().unwrap().len(), 0);
    }

    #[test]
    fn test_insert_firebase_tag_null_value() {
        // Edge case: VALUE is null
        // Message ends with }}} (null + 3 envelope closes)
        let base = encode_firebase_event("put", "/data", "/", b"null", None);

        // Verify the base ends with }}} but not }}}}
        assert!(base.ends_with(b"}}}"));
        assert!(!base.ends_with(b"}}}}"));

        let with_tag = insert_firebase_tag(&base, 42);

        let parsed: Value = serde_json::from_slice(&with_tag).unwrap();
        assert_eq!(parsed["d"]["b"]["t"], 42);
        assert!(parsed["d"]["b"]["d"].is_null());
    }

    #[test]
    fn test_insert_firebase_tag_array_value() {
        // Edge case: VALUE is an array
        let base = encode_firebase_event("put", "/data", "/", b"[1,2,3]", None);

        let with_tag = insert_firebase_tag(&base, 42);

        let parsed: Value = serde_json::from_slice(&with_tag).unwrap();
        assert_eq!(parsed["d"]["b"]["t"], 42);
        assert!(parsed["d"]["b"]["d"].is_array());
        assert_eq!(parsed["d"]["b"]["d"][0], 1);
    }

    #[test]
    fn test_translate_outgoing_chunked_skip_translation() {
        let adapter = FirebaseAdapter::new("test-project", "host");

        // Pre-encoded Firebase event (what ViewManager would generate)
        let firebase_bytes =
            encode_firebase_event("put", "/users", "/alice", br#"{"name":"Alice"}"#, None);

        // Call with skip_translation=true
        let chunks = adapter
            .translate_outgoing_chunked(&firebase_bytes, true)
            .unwrap();

        let chunks = chunks.unwrap();
        assert_eq!(chunks.len(), 1);

        // Output should be exactly the input (no double-encoding)
        assert_eq!(chunks[0], firebase_bytes);
    }
}
