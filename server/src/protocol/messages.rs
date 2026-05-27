use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::db::ArcValue;

// =============================================================================
// MessageValue - Efficient value wrapper for serialization
// =============================================================================

/// A value that can be serialized efficiently without intermediate copies.
///
/// This allows ServerMessage to hold either:
/// - `Value` (serde_json::Value) - for data that's already in Value form
/// - `Arc` (ArcValue) - for data from the Tree, avoiding the to_value() copy
///
/// Both variants serialize to identical JSON output.
#[derive(Debug, Clone)]
pub enum MessageValue {
    /// Standard serde_json Value
    Json(Value),
    /// ArcValue from the Tree - serializes directly without conversion
    Arc(ArcValue),
}

impl MessageValue {
    /// Create from a serde_json Value
    pub fn from_json(v: Value) -> Self {
        MessageValue::Json(v)
    }

    /// Create from an ArcValue (avoids to_value() conversion)
    pub fn from_arc(v: ArcValue) -> Self {
        MessageValue::Arc(v)
    }

    /// Check if this is null
    pub fn is_null(&self) -> bool {
        match self {
            MessageValue::Json(v) => v.is_null(),
            MessageValue::Arc(v) => matches!(v, ArcValue::Null),
        }
    }

    /// Convert to a serde_json Value.
    /// Note: This allocates for the Arc variant. Prefer serialization when possible.
    pub fn to_value(&self) -> Value {
        match self {
            MessageValue::Json(v) => v.clone(),
            MessageValue::Arc(v) => v.to_value(),
        }
    }

    /// Get a reference as serde_json Value (only works for Json variant).
    pub fn as_value(&self) -> Option<&Value> {
        match self {
            MessageValue::Json(v) => Some(v),
            MessageValue::Arc(_) => None,
        }
    }

    /// Get as object map (converts Arc variant if needed).
    pub fn as_object(&self) -> Option<&serde_json::Map<String, Value>> {
        match self {
            MessageValue::Json(v) => v.as_object(),
            MessageValue::Arc(_) => None, // Would require conversion, use to_value().as_object()
        }
    }
}

impl Default for MessageValue {
    fn default() -> Self {
        MessageValue::Json(Value::Null)
    }
}

impl From<Value> for MessageValue {
    fn from(v: Value) -> Self {
        MessageValue::Json(v)
    }
}

impl From<ArcValue> for MessageValue {
    fn from(v: ArcValue) -> Self {
        MessageValue::Arc(v)
    }
}

// Serialize either variant directly - no intermediate conversion
impl Serialize for MessageValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            MessageValue::Json(v) => v.serialize(serializer),
            MessageValue::Arc(v) => v.serialize(serializer),
        }
    }
}

// Deserialize always to Json variant (for completeness, rarely used)
impl<'de> Deserialize<'de> for MessageValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Value::deserialize(deserializer).map(MessageValue::Json)
    }
}

impl PartialEq for MessageValue {
    fn eq(&self, other: &Self) -> bool {
        // For comparison, convert both to Value
        // This is only used in tests, not hot paths
        match (self, other) {
            (MessageValue::Json(a), MessageValue::Json(b)) => a == b,
            (MessageValue::Arc(a), MessageValue::Arc(b)) => a.to_value() == b.to_value(),
            (MessageValue::Json(a), MessageValue::Arc(b)) => a == &b.to_value(),
            (MessageValue::Arc(a), MessageValue::Json(b)) => &a.to_value() == b,
        }
    }
}

impl PartialEq<Value> for MessageValue {
    fn eq(&self, other: &Value) -> bool {
        match self {
            MessageValue::Json(v) => v == other,
            MessageValue::Arc(v) => &v.to_value() == other,
        }
    }
}

// =============================================================================
// Operation Constants
// =============================================================================

/// Client -> Server operation types
pub mod op {
    pub const JOIN: &str = "j";
    pub const AUTH: &str = "au";
    pub const UNAUTH: &str = "ua";
    pub const SET: &str = "s";
    pub const UPDATE: &str = "u";
    pub const REMOVE: &str = "d";
    pub const SUBSCRIBE: &str = "sb";
    pub const UNSUBSCRIBE: &str = "us";
    pub const ONCE: &str = "o";
    pub const ON_DISCONNECT: &str = "od";
    pub const LEAVE: &str = "l";
    pub const PING: &str = "pi";
    pub const PONG: &str = "po";
    pub const TRANSACTION: &str = "tx";
}

/// Server -> Client operation types
pub mod server_op {
    pub const PING: &str = "pi";
}

/// Event types for server -> client
pub mod event {
    pub const PUT: &str = "put";
    pub const PATCH: &str = "patch";
}

/// OnDisconnect action types
pub mod action {
    pub const SET: &str = "s";
    pub const UPDATE: &str = "u";
    pub const REMOVE: &str = "d";
    pub const CANCEL: &str = "c";
}

/// Error codes for nack messages
pub mod error {
    pub const PERMISSION_DENIED: &str = "permission_denied";
    pub const INVALID_DATA: &str = "invalid_data";
    pub const NOT_FOUND: &str = "not_found";
    pub const INVALID_PATH: &str = "invalid_path";
    pub const INVALID_OPERATION: &str = "invalid_operation";
    pub const INTERNAL: &str = "internal_error";
    pub const PAYLOAD_TOO_LARGE: &str = "payload_too_large";
    pub const RESPONSE_TOO_LARGE: &str = "response_too_large";
    pub const CONDITION_FAILED: &str = "condition_failed";
    pub const TOO_MANY_CONNECTIONS: &str = "too_many_connections";
    /// Client exceeded the per-connection subscription cap.
    pub const TOO_MANY_SUBSCRIPTIONS: &str = "too_many_subscriptions";
    /// Database is at its maximum size; growth writes are rejected (deletes still allowed).
    pub const DATABASE_FULL: &str = "database_full";
    /// Per-database durable-write rate exceeded; retryable.
    pub const RATE_LIMITED: &str = "rate_limited";
    /// Segment or storage unavailable - client should retry
    pub const UNAVAILABLE: &str = "unavailable";
}

// =============================================================================
// Size Limits
// =============================================================================

/// Maximum size for SDK/WebSocket writes (16 MB)
pub const MAX_WRITE_SIZE: usize = 16 * 1024 * 1024;

/// Maximum size for REST API writes (256 MB)
pub const MAX_REST_WRITE_SIZE: usize = 256 * 1024 * 1024;

/// Maximum size for volatile writes (2 KB)
pub const MAX_VOLATILE_WRITE_SIZE: usize = 2 * 1024;

/// Maximum size for a single string value (10 MB)
pub const MAX_STRING_SIZE: usize = 10 * 1024 * 1024;

/// Maximum size for read responses (256 MB)
pub const MAX_RESPONSE_SIZE: usize = 256 * 1024 * 1024;

/// Maximum nesting depth for JSON values (matches Firebase's 32-level limit)
pub const MAX_JSON_DEPTH: usize = 32;

// =============================================================================
// Message Parsing Errors
// =============================================================================

/// Error type for message parsing failures.
#[derive(Debug)]
pub enum MessageError {
    /// Payload exceeds MAX_WRITE_SIZE.
    PayloadTooLarge { size: usize, max: usize },
    /// A string value exceeds MAX_STRING_SIZE.
    StringTooLarge { size: usize, max: usize },
    /// JSON nesting exceeds MAX_JSON_DEPTH.
    NestingTooDeep { depth: usize, max: usize },
    /// JSON parsing failed.
    ParseError(serde_json::Error),
}

impl std::fmt::Display for MessageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MessageError::PayloadTooLarge { size, max } => {
                write!(f, "payload size {} exceeds maximum {} bytes", size, max)
            }
            MessageError::StringTooLarge { size, max } => {
                write!(f, "string size {} exceeds maximum {} bytes", size, max)
            }
            MessageError::NestingTooDeep { depth, max } => {
                write!(f, "JSON nesting depth {} exceeds maximum {}", depth, max)
            }
            MessageError::ParseError(e) => write!(f, "JSON parse error: {}", e),
        }
    }
}

impl std::error::Error for MessageError {}

impl From<serde_json::Error> for MessageError {
    fn from(e: serde_json::Error) -> Self {
        MessageError::ParseError(e)
    }
}

/// Recursively validate a JSON value:
/// - No string exceeds MAX_STRING_SIZE (10MB)
/// - Nesting depth doesn't exceed MAX_JSON_DEPTH (32 levels)
fn validate_value(value: &Value, depth: usize) -> Result<(), MessageError> {
    // Check depth first (bail early)
    if depth > MAX_JSON_DEPTH {
        return Err(MessageError::NestingTooDeep {
            depth,
            max: MAX_JSON_DEPTH,
        });
    }

    match value {
        Value::String(s) => {
            if s.len() > MAX_STRING_SIZE {
                return Err(MessageError::StringTooLarge {
                    size: s.len(),
                    max: MAX_STRING_SIZE,
                });
            }
            Ok(())
        }
        Value::Object(map) => {
            for v in map.values() {
                validate_value(v, depth + 1)?;
            }
            Ok(())
        }
        Value::Array(arr) => {
            for v in arr {
                validate_value(v, depth + 1)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

// =============================================================================
// Client Message
// =============================================================================

/// A single operation within a transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionOp {
    /// Operation: s (set), u (update), d (delete), c (condition)
    #[serde(rename = "o")]
    pub op: String,

    /// Path to operate on
    #[serde(rename = "p")]
    pub path: String,

    /// Value for set/update, or expected value for condition
    #[serde(rename = "v", skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,

    /// SHA-256 hash of JCS-canonicalized value (for condition)
    #[serde(rename = "h", skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
}

/// Message from client to server.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClientMessage {
    /// Operation: j, s, u, d, sb, us, o, od, tx
    #[serde(rename = "o")]
    pub op: String,

    /// Path in database tree
    #[serde(rename = "p", skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,

    /// Value for set/update operations
    #[serde(rename = "v", skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,

    /// Request ID for ack/nack correlation
    #[serde(rename = "r", skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,

    /// Database ID (for join) - format: project/database
    #[serde(rename = "d", skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,

    /// Action for ondisconnect: s, u, d, c
    #[serde(rename = "a", skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,

    /// Volatile flag (no ack/nack sent)
    #[serde(rename = "x", skip_serializing_if = "Option::is_none")]
    pub volatile: Option<bool>,

    /// JWT auth token
    #[serde(rename = "t", skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,

    /// Previous connection ID (for reconnect/deduplication)
    #[serde(rename = "pcid", skip_serializing_if = "Option::is_none")]
    pub previous_connection_id: Option<String>,

    /// Pending write request IDs (for local-first writes)
    #[serde(rename = "pw", skip_serializing_if = "Option::is_none")]
    pub pending_writes: Option<Vec<String>>,

    /// Operations for transaction
    #[serde(rename = "ops", skip_serializing_if = "Option::is_none")]
    pub operations: Option<Vec<TransactionOp>>,

    /// Hash of expected current value (for CAS)
    #[serde(rename = "h", skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,

    /// True if hash was explicitly provided
    #[serde(rename = "hp", skip_serializing_if = "Option::is_none")]
    pub hash_provided: Option<bool>,

    /// Shallow read flag
    #[serde(rename = "sh", skip_serializing_if = "Option::is_none")]
    pub shallow: Option<bool>,

    // Query modifiers
    #[serde(rename = "orderBy", skip_serializing_if = "Option::is_none")]
    pub order_by: Option<String>,

    #[serde(rename = "orderByChild", skip_serializing_if = "Option::is_none")]
    pub order_by_child: Option<String>,

    #[serde(rename = "limitToFirst", skip_serializing_if = "Option::is_none")]
    pub limit_to_first: Option<i32>,

    #[serde(rename = "limitToLast", skip_serializing_if = "Option::is_none")]
    pub limit_to_last: Option<i32>,

    #[serde(rename = "startAt", skip_serializing_if = "Option::is_none")]
    pub start_at: Option<Value>,

    #[serde(rename = "startAtKey", skip_serializing_if = "Option::is_none")]
    pub start_at_key: Option<String>,

    #[serde(rename = "startAfter", skip_serializing_if = "Option::is_none")]
    pub start_after: Option<Value>,

    #[serde(rename = "startAfterKey", skip_serializing_if = "Option::is_none")]
    pub start_after_key: Option<String>,

    #[serde(rename = "endAt", skip_serializing_if = "Option::is_none")]
    pub end_at: Option<Value>,

    #[serde(rename = "endAtKey", skip_serializing_if = "Option::is_none")]
    pub end_at_key: Option<String>,

    #[serde(rename = "endBefore", skip_serializing_if = "Option::is_none")]
    pub end_before: Option<Value>,

    #[serde(rename = "endBeforeKey", skip_serializing_if = "Option::is_none")]
    pub end_before_key: Option<String>,

    #[serde(rename = "equalTo", skip_serializing_if = "Option::is_none")]
    pub equal_to: Option<Value>,

    #[serde(rename = "equalToKey", skip_serializing_if = "Option::is_none")]
    pub equal_to_key: Option<String>,

    /// Client-provided tag for routing events
    #[serde(rename = "tag", skip_serializing_if = "Option::is_none")]
    pub tag: Option<i32>,

    /// Raw payload size in bytes (set during parsing, not serialized)
    #[serde(skip)]
    pub payload_size: usize,
}

impl ClientMessage {
    /// Parse a client message from JSON bytes with size validation.
    ///
    /// For SDK clients (is_rest=false): rejects payloads larger than MAX_WRITE_SIZE (16MB).
    /// For REST clients (is_rest=true): rejects payloads larger than MAX_REST_WRITE_SIZE (256MB).
    /// Also validates that no individual string exceeds MAX_STRING_SIZE (10MB).
    pub fn parse(data: &[u8], is_rest: bool) -> Result<Self, MessageError> {
        let max_size = if is_rest {
            MAX_REST_WRITE_SIZE
        } else {
            MAX_WRITE_SIZE
        };
        if data.len() > max_size {
            return Err(MessageError::PayloadTooLarge {
                size: data.len(),
                max: max_size,
            });
        }
        let mut msg: Self = serde_json::from_slice(data)?;

        // Validate string sizes and nesting depth in the value field
        if let Some(ref value) = msg.value {
            validate_value(value, 0)?;
        }

        // Validate string sizes and nesting depth in transaction operations
        if let Some(ref ops) = msg.operations {
            for op in ops {
                if let Some(ref value) = op.value {
                    validate_value(value, 0)?;
                }
            }
        }

        // Record raw payload size for metrics
        msg.payload_size = data.len();

        Ok(msg)
    }

    /// Parse without size validation (for internal use or when size is pre-validated).
    pub fn parse_unchecked(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(data)
    }

    /// Check if this is a volatile write.
    pub fn is_volatile(&self) -> bool {
        self.volatile.unwrap_or(false)
    }

    /// Check if this is a write operation (set, update, remove).
    pub fn is_write(&self) -> bool {
        matches!(self.op.as_str(), "s" | "u" | "d")
    }

    /// Get the value as a specific type.
    pub fn get_value<T: for<'de> Deserialize<'de>>(&self) -> Option<T> {
        self.value
            .as_ref()
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }
}

// =============================================================================
// Server Message
// =============================================================================

/// Message from server to client.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ServerMessage {
    // Operation (for ping)
    #[serde(rename = "o", skip_serializing_if = "Option::is_none")]
    pub op: Option<String>,

    // Acknowledgments
    #[serde(rename = "a", skip_serializing_if = "Option::is_none")]
    pub ack: Option<String>,

    #[serde(rename = "n", skip_serializing_if = "Option::is_none")]
    pub nack: Option<String>,

    // Error details
    #[serde(rename = "e", skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    #[serde(rename = "m", skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    // Events
    #[serde(rename = "ev", skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,

    #[serde(rename = "sp", skip_serializing_if = "Option::is_none")]
    pub subscription_path: Option<String>,

    #[serde(rename = "p", skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,

    #[serde(rename = "v", skip_serializing_if = "Option::is_none")]
    pub value: Option<MessageValue>,

    #[serde(rename = "tag", skip_serializing_if = "Option::is_none")]
    pub tag: Option<i32>,

    #[serde(rename = "x", skip_serializing_if = "Option::is_none")]
    pub volatile: Option<bool>,

    #[serde(rename = "ts", skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<i64>,

    // Once response
    #[serde(rename = "oc", skip_serializing_if = "Option::is_none")]
    pub once: Option<String>,

    #[serde(rename = "ov", skip_serializing_if = "Option::is_none")]
    pub once_value: Option<MessageValue>,

    // Join response
    #[serde(rename = "jc", skip_serializing_if = "Option::is_none")]
    pub join_ack: Option<String>,

    #[serde(rename = "vp", skip_serializing_if = "Option::is_none")]
    pub volatile_paths: Option<Vec<String>>,

    #[serde(rename = "cid", skip_serializing_if = "Option::is_none")]
    pub connection_id: Option<String>,

    #[serde(rename = "st", skip_serializing_if = "Option::is_none")]
    pub server_time: Option<i64>,

    // Auth response
    #[serde(rename = "ac", skip_serializing_if = "Option::is_none")]
    pub auth_ack: Option<String>,

    #[serde(rename = "au", skip_serializing_if = "Option::is_none")]
    pub auth_uid: Option<String>,
}

impl ServerMessage {
    /// Encode the message to JSON bytes.
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Returns the request id this message is responding to, if any.
    /// Events (put/patch) and pings have no request id.
    pub fn request_id(&self) -> Option<&str> {
        self.ack
            .as_deref()
            .or(self.nack.as_deref())
            .or(self.once.as_deref())
            .or(self.join_ack.as_deref())
            .or(self.auth_ack.as_deref())
    }

    // =========================================================================
    // Fast Event Encoding (string concatenation, no full JSON serialization)
    // =========================================================================

    /// Fast event encoding using string concatenation.
    ///
    /// This avoids full JSON serialization by directly constructing the Lark wire format.
    /// The `value_bytes` parameter should be pre-serialized JSON bytes of the value.
    ///
    /// # Arguments
    /// * `event_type` - Either "put" or "patch"
    /// * `subscription_path` - The subscription path (e.g., "/users")
    /// * `relative_path` - The relative path within the subscription (e.g., "/alice" or "/")
    /// * `value_bytes` - Pre-serialized JSON bytes of the value
    /// * `tag` - Optional subscription tag for query views
    /// * `volatile` - Whether this is a volatile update
    ///
    /// # Returns
    /// Lark wire format bytes ready to send to the client.
    pub fn encode_event_fast(
        event_type: &str,
        subscription_path: &str,
        relative_path: &str,
        value_bytes: &[u8],
        tag: Option<i32>,
        volatile: bool,
    ) -> Vec<u8> {
        // Estimate capacity: envelope + paths + value + optional tag/volatile
        let capacity = 60 + subscription_path.len() + relative_path.len() + value_bytes.len();
        let mut buf = Vec::with_capacity(capacity);

        // Build: {"ev":"EVENT","sp":"PATH","p":"PATH","v":VALUE}
        // Optional: ,"tag":TAG
        // Optional: ,"x":true
        buf.extend_from_slice(b"{\"ev\":\"");
        buf.extend_from_slice(event_type.as_bytes());
        buf.extend_from_slice(b"\",\"sp\":\"");
        buf.extend_from_slice(subscription_path.as_bytes());
        buf.extend_from_slice(b"\",\"p\":\"");
        buf.extend_from_slice(relative_path.as_bytes());
        buf.extend_from_slice(b"\",\"v\":");
        buf.extend_from_slice(value_bytes);

        if let Some(t) = tag {
            buf.extend_from_slice(b",\"tag\":");
            buf.extend_from_slice(t.to_string().as_bytes());
        }

        if volatile {
            buf.extend_from_slice(b",\"x\":true");
        }

        buf.push(b'}');
        buf
    }

    /// Prepend a tag to pre-encoded Lark event bytes.
    ///
    /// This is used when we have already encoded a Lark event without a tag,
    /// and need to create a variant with a specific tag for a query subscriber.
    ///
    /// # Arguments
    /// * `lark_bytes` - Pre-encoded Lark event bytes (without tag)
    /// * `tag` - The tag to prepend
    ///
    /// # Returns
    /// Lark bytes with the tag prepended.
    pub fn prepend_lark_tag(lark_bytes: &[u8], tag: i32) -> Vec<u8> {
        // Format: {"tag":TAG,"ev":... (rest of original message without opening brace)
        let prefix = format!(r#"{{"tag":{},"#, tag);
        let mut result = Vec::with_capacity(prefix.len() + lark_bytes.len() - 1);
        result.extend_from_slice(prefix.as_bytes());
        // Skip the opening '{' of the original message
        if !lark_bytes.is_empty() && lark_bytes[0] == b'{' {
            result.extend_from_slice(&lark_bytes[1..]);
        } else {
            result.extend_from_slice(lark_bytes);
        }
        result
    }

    // =========================================================================
    // Factory Methods
    // =========================================================================

    /// Create an acknowledgment message.
    pub fn ack(request_id: &str) -> Self {
        Self {
            ack: Some(request_id.to_string()),
            ..Default::default()
        }
    }

    /// Create a negative acknowledgment message.
    pub fn nack(request_id: &str, error_code: &str, message: &str) -> Self {
        Self {
            nack: Some(request_id.to_string()),
            error: Some(error_code.to_string()),
            message: Some(message.to_string()),
            ..Default::default()
        }
    }

    /// Create a join acknowledgment.
    pub fn join_ack(request_id: &str, volatile_paths: Vec<String>, connection_id: &str) -> Self {
        let server_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        Self {
            join_ack: Some(request_id.to_string()),
            volatile_paths: Some(volatile_paths),
            connection_id: Some(connection_id.to_string()),
            server_time: Some(server_time),
            ..Default::default()
        }
    }

    /// Create an auth acknowledgment.
    pub fn auth_ack(request_id: &str, uid: &str) -> Self {
        Self {
            auth_ack: Some(request_id.to_string()),
            auth_uid: Some(uid.to_string()),
            ..Default::default()
        }
    }

    /// Create a put event with a serde_json Value.
    pub fn put_event(
        subscription_path: &str,
        relative_path: &str,
        value: Value,
        volatile: bool,
    ) -> Self {
        Self::put_event_impl(
            subscription_path,
            relative_path,
            MessageValue::from(value),
            volatile,
        )
    }

    /// Create a put event with an ArcValue (avoids to_value() conversion).
    pub fn put_event_arc(
        subscription_path: &str,
        relative_path: &str,
        value: ArcValue,
        volatile: bool,
    ) -> Self {
        Self::put_event_impl(
            subscription_path,
            relative_path,
            MessageValue::from(value),
            volatile,
        )
    }

    /// Internal implementation for put_event.
    fn put_event_impl(
        subscription_path: &str,
        relative_path: &str,
        value: MessageValue,
        volatile: bool,
    ) -> Self {
        let mut msg = Self {
            event: Some(event::PUT.to_string()),
            subscription_path: Some(subscription_path.to_string()),
            path: Some(relative_path.to_string()),
            value: Some(value),
            ..Default::default()
        };

        if volatile {
            msg.volatile = Some(true);
            msg.timestamp = Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
            );
        }

        msg
    }

    /// Create a patch event.
    pub fn patch_event(
        subscription_path: &str,
        relative_path: &str,
        values: Map<String, Value>,
        volatile: bool,
    ) -> Self {
        let mut msg = Self {
            event: Some(event::PATCH.to_string()),
            subscription_path: Some(subscription_path.to_string()),
            path: Some(relative_path.to_string()),
            value: Some(MessageValue::from(Value::Object(values))),
            ..Default::default()
        };

        if volatile {
            msg.volatile = Some(true);
            msg.timestamp = Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
            );
        }

        msg
    }

    pub fn patch_event_arc(
        subscription_path: &str,
        relative_path: &str,
        value: ArcValue,
        volatile: bool,
    ) -> Self {
        let mut msg = Self {
            event: Some(event::PATCH.to_string()),
            subscription_path: Some(subscription_path.to_string()),
            path: Some(relative_path.to_string()),
            value: Some(MessageValue::from(value)),
            ..Default::default()
        };

        if volatile {
            msg.volatile = Some(true);
            msg.timestamp = Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
            );
        }

        msg
    }

    /// Create a once response with a serde_json Value.
    pub fn once_response(request_id: &str, value: Value) -> Self {
        Self {
            once: Some(request_id.to_string()),
            once_value: Some(MessageValue::from(value)),
            ..Default::default()
        }
    }

    /// Create a once response with an ArcValue (avoids to_value() conversion).
    pub fn once_response_arc(request_id: &str, value: ArcValue) -> Self {
        Self {
            once: Some(request_id.to_string()),
            once_value: Some(MessageValue::from(value)),
            ..Default::default()
        }
    }

    /// Create a ping message.
    pub fn ping() -> Self {
        Self {
            op: Some(server_op::PING.to_string()),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_join_message() {
        let data = br#"{"o": "j", "d": "myproject/room-123", "r": "r1"}"#;
        let msg = ClientMessage::parse(data, false).unwrap();
        assert_eq!(msg.op, "j");
        assert_eq!(msg.database, Some("myproject/room-123".to_string()));
        assert_eq!(msg.request_id, Some("r1".to_string()));
    }

    #[test]
    fn test_parse_set_message() {
        let data = br#"{"o": "s", "p": "/players/abc/name", "v": "Alice", "r": "r2"}"#;
        let msg = ClientMessage::parse(data, false).unwrap();
        assert_eq!(msg.op, "s");
        assert_eq!(msg.path, Some("/players/abc/name".to_string()));
        assert_eq!(msg.value, Some(json!("Alice")));
        assert_eq!(msg.request_id, Some("r2".to_string()));
    }

    #[test]
    fn test_parse_volatile_set() {
        let data = br#"{"o": "s", "p": "/pos", "v": {"x": 1, "y": 2}, "x": true}"#;
        let msg = ClientMessage::parse(data, false).unwrap();
        assert_eq!(msg.op, "s");
        assert!(msg.is_volatile());
        assert!(msg.request_id.is_none()); // Volatile writes don't need request_id
    }

    #[test]
    fn test_parse_subscribe_message() {
        // Note: "e" field is ignored for backwards compatibility (event filtering removed)
        let data = br#"{"o": "sb", "p": "/players", "e": ["value"], "r": "r3"}"#;
        let msg = ClientMessage::parse(data, false).unwrap();
        assert_eq!(msg.op, "sb");
        assert_eq!(msg.path, Some("/players".to_string()));
    }

    #[test]
    fn test_parse_query_subscribe() {
        let data = br#"{"o": "sb", "p": "/players", "e": ["value"], "r": "r1", "orderByChild": "score", "limitToFirst": 10}"#;
        let msg = ClientMessage::parse(data, false).unwrap();
        assert_eq!(msg.order_by_child, Some("score".to_string()));
        assert_eq!(msg.limit_to_first, Some(10));
    }

    #[test]
    fn test_parse_payload_too_large_sdk() {
        // SDK clients: payload larger than MAX_WRITE_SIZE (16MB) is rejected
        let large_data = vec![b'x'; MAX_WRITE_SIZE + 1];
        let result = ClientMessage::parse(&large_data, false);
        assert!(matches!(result, Err(MessageError::PayloadTooLarge { .. })));
    }

    #[test]
    fn test_parse_payload_rest_higher_limit() {
        // REST clients: payload larger than MAX_WRITE_SIZE but under MAX_REST_WRITE_SIZE is OK
        // Note: This just tests the size check passes - parsing will fail on invalid JSON
        let large_data = vec![b'x'; MAX_WRITE_SIZE + 1];
        let result = ClientMessage::parse(&large_data, true);
        // Should NOT be PayloadTooLarge (will be ParseError instead since it's not valid JSON)
        assert!(matches!(result, Err(MessageError::ParseError(_))));
    }

    #[test]
    fn test_parse_payload_too_large_rest() {
        // REST clients: payload larger than MAX_REST_WRITE_SIZE (256MB) is rejected
        let large_data = vec![b'x'; MAX_REST_WRITE_SIZE + 1];
        let result = ClientMessage::parse(&large_data, true);
        assert!(matches!(result, Err(MessageError::PayloadTooLarge { .. })));
    }

    #[test]
    fn test_encode_ack() {
        let msg = ServerMessage::ack("r1");
        let data = msg.encode().unwrap();
        let parsed: Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(parsed.get("a"), Some(&json!("r1")));
    }

    #[test]
    fn test_encode_nack() {
        let msg = ServerMessage::nack("r1", error::PERMISSION_DENIED, "Access denied");
        let data = msg.encode().unwrap();
        let parsed: Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(parsed.get("n"), Some(&json!("r1")));
        assert_eq!(parsed.get("e"), Some(&json!("permission_denied")));
        assert_eq!(parsed.get("m"), Some(&json!("Access denied")));
    }

    #[test]
    fn test_encode_join_ack() {
        let msg = ServerMessage::join_ack("r1", vec!["players/*/position".to_string()], "conn-123");
        let data = msg.encode().unwrap();
        let parsed: Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(parsed.get("jc"), Some(&json!("r1")));
        assert_eq!(parsed.get("vp"), Some(&json!(["players/*/position"])));
        assert_eq!(parsed.get("cid"), Some(&json!("conn-123")));
        assert!(parsed.get("st").is_some()); // Server time
    }

    #[test]
    fn test_encode_put_event() {
        let msg = ServerMessage::put_event("/players/abc", "/score", json!(150), false);
        let data = msg.encode().unwrap();
        let parsed: Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(parsed.get("ev"), Some(&json!("put")));
        assert_eq!(parsed.get("sp"), Some(&json!("/players/abc")));
        assert_eq!(parsed.get("p"), Some(&json!("/score")));
        assert_eq!(parsed.get("v"), Some(&json!(150)));
    }

    #[test]
    fn test_encode_volatile_put_event() {
        let msg = ServerMessage::put_event("/pos", "/", json!({"x": 1, "y": 2}), true);
        let data = msg.encode().unwrap();
        let parsed: Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(parsed.get("x"), Some(&json!(true)));
        assert!(parsed.get("ts").is_some()); // Timestamp for volatile
    }

    #[test]
    fn test_encode_auth_ack() {
        let msg = ServerMessage::auth_ack("r2", "user-123");
        let data = msg.encode().unwrap();
        let parsed: Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(parsed.get("ac"), Some(&json!("r2")));
        assert_eq!(parsed.get("au"), Some(&json!("user-123")));
    }

    #[test]
    fn test_encode_ping() {
        let msg = ServerMessage::ping();
        let data = msg.encode().unwrap();
        let parsed: Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(parsed.get("o"), Some(&json!("pi")));
    }

    #[test]
    fn test_parse_string_too_large() {
        // Create a message with a string larger than MAX_STRING_SIZE (10MB)
        let large_string = "x".repeat(MAX_STRING_SIZE + 1);
        let msg_json = json!({
            "o": "s",
            "p": "/test",
            "v": large_string,
            "r": "r1"
        });
        let data = serde_json::to_vec(&msg_json).unwrap();
        let result = ClientMessage::parse(&data, false);
        assert!(matches!(result, Err(MessageError::StringTooLarge { .. })));
    }

    #[test]
    fn test_parse_nested_string_too_large() {
        // String inside nested object
        let large_string = "x".repeat(MAX_STRING_SIZE + 1);
        let msg_json = json!({
            "o": "s",
            "p": "/test",
            "v": {"nested": {"deep": large_string}},
            "r": "r1"
        });
        let data = serde_json::to_vec(&msg_json).unwrap();
        let result = ClientMessage::parse(&data, false);
        assert!(matches!(result, Err(MessageError::StringTooLarge { .. })));
    }

    #[test]
    fn test_parse_string_in_array_too_large() {
        // String inside array
        let large_string = "x".repeat(MAX_STRING_SIZE + 1);
        let msg_json = json!({
            "o": "s",
            "p": "/test",
            "v": ["ok", large_string],
            "r": "r1"
        });
        let data = serde_json::to_vec(&msg_json).unwrap();
        let result = ClientMessage::parse(&data, false);
        assert!(matches!(result, Err(MessageError::StringTooLarge { .. })));
    }

    #[test]
    fn test_parse_valid_string_size() {
        // String exactly at MAX_STRING_SIZE should be accepted
        let max_string = "x".repeat(MAX_STRING_SIZE);
        let msg_json = json!({
            "o": "s",
            "p": "/test",
            "v": max_string,
            "r": "r1"
        });
        let data = serde_json::to_vec(&msg_json).unwrap();
        let result = ClientMessage::parse(&data, false);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_nesting_too_deep() {
        // Create a deeply nested structure exceeding MAX_JSON_DEPTH (32)
        // Build {"a":{"a":{"a":...}}} with 33 levels
        let mut value = json!("leaf");
        for _ in 0..33 {
            value = json!({"a": value});
        }
        let msg_json = json!({
            "o": "s",
            "p": "/test",
            "v": value,
            "r": "r1"
        });
        let data = serde_json::to_vec(&msg_json).unwrap();
        let result = ClientMessage::parse(&data, false);
        assert!(matches!(result, Err(MessageError::NestingTooDeep { .. })));
    }

    #[test]
    fn test_parse_nesting_at_limit() {
        // Create a structure exactly at MAX_JSON_DEPTH (32) - should be accepted
        let mut value = json!("leaf");
        for _ in 0..32 {
            value = json!({"a": value});
        }
        let msg_json = json!({
            "o": "s",
            "p": "/test",
            "v": value,
            "r": "r1"
        });
        let data = serde_json::to_vec(&msg_json).unwrap();
        let result = ClientMessage::parse(&data, false);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_array_nesting_too_deep() {
        // Arrays also count toward depth
        let mut value = json!("leaf");
        for _ in 0..33 {
            value = json!([value]);
        }
        let msg_json = json!({
            "o": "s",
            "p": "/test",
            "v": value,
            "r": "r1"
        });
        let data = serde_json::to_vec(&msg_json).unwrap();
        let result = ClientMessage::parse(&data, false);
        assert!(matches!(result, Err(MessageError::NestingTooDeep { .. })));
    }

    // =========================================================================
    // Fast Encoding Tests
    // =========================================================================

    #[test]
    fn test_encode_event_fast_put() {
        let value_bytes = br#"{"name":"Alice"}"#;
        let result =
            ServerMessage::encode_event_fast("put", "/users", "/alice", value_bytes, None, false);

        let parsed: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(parsed["ev"], "put");
        assert_eq!(parsed["sp"], "/users");
        assert_eq!(parsed["p"], "/alice");
        assert_eq!(parsed["v"]["name"], "Alice");
        assert!(parsed.get("tag").is_none());
        assert!(parsed.get("x").is_none());
    }

    #[test]
    fn test_encode_event_fast_patch() {
        let value_bytes = br#"{"/name":"Bob","/age":30}"#;
        let result =
            ServerMessage::encode_event_fast("patch", "/users", "/", value_bytes, None, false);

        let parsed: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(parsed["ev"], "patch");
        assert_eq!(parsed["sp"], "/users");
        assert_eq!(parsed["p"], "/");
        assert_eq!(parsed["v"]["/name"], "Bob");
        assert_eq!(parsed["v"]["/age"], 30);
    }

    #[test]
    fn test_encode_event_fast_with_tag() {
        let value_bytes = br#"42"#;
        let result = ServerMessage::encode_event_fast(
            "put",
            "/scores",
            "/player1",
            value_bytes,
            Some(5),
            false,
        );

        let parsed: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(parsed["ev"], "put");
        assert_eq!(parsed["tag"], 5);
    }

    #[test]
    fn test_encode_event_fast_volatile() {
        let value_bytes = br#"{"x":100,"y":200}"#;
        let result = ServerMessage::encode_event_fast(
            "put",
            "/cursors",
            "/player1",
            value_bytes,
            None,
            true,
        );

        let parsed: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(parsed["ev"], "put");
        assert_eq!(parsed["x"], true);
    }

    #[test]
    fn test_encode_event_fast_with_tag_and_volatile() {
        let value_bytes = br#"{"x":100}"#;
        let result =
            ServerMessage::encode_event_fast("put", "/data", "/item", value_bytes, Some(42), true);

        let parsed: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(parsed["ev"], "put");
        assert_eq!(parsed["tag"], 42);
        assert_eq!(parsed["x"], true);
    }

    #[test]
    fn test_prepend_lark_tag() {
        // Create a base event without tag
        let base = ServerMessage::encode_event_fast(
            "put",
            "/users",
            "/alice",
            br#"{"name":"Alice"}"#,
            None,
            false,
        );

        // Prepend a tag
        let with_tag = ServerMessage::prepend_lark_tag(&base, 7);

        let parsed: Value = serde_json::from_slice(&with_tag).unwrap();
        assert_eq!(parsed["tag"], 7);
        assert_eq!(parsed["ev"], "put");
        assert_eq!(parsed["sp"], "/users");
        assert_eq!(parsed["p"], "/alice");
        assert_eq!(parsed["v"]["name"], "Alice");
    }

    #[test]
    fn test_prepend_lark_tag_negative() {
        let base = ServerMessage::encode_event_fast("put", "/data", "/", br#"null"#, None, false);

        let with_tag = ServerMessage::prepend_lark_tag(&base, -1);

        let parsed: Value = serde_json::from_slice(&with_tag).unwrap();
        assert_eq!(parsed["tag"], -1);
        assert_eq!(parsed["v"], Value::Null);
    }
}
