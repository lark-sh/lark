//! JWT validation for Lark authentication.
//!
//! Supports multiple token types:
//! - HS256: Lark Customer tokens, Firebase Legacy tokens, Coordinator Admin tokens
//! - RS256: Firebase ID tokens, Firebase Custom tokens (handled in firebase.rs)

use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

/// Standard errors for auth operations.
#[derive(Debug, Error)]
pub enum AuthError {
    #[error("no token provided")]
    NoToken,

    #[error("invalid token: {0}")]
    InvalidToken(String),

    #[error("token expired")]
    ExpiredToken,

    #[error("invalid claims: {0}")]
    InvalidClaims(String),

    #[error("token assigned to different server: expected {expected}, got {got}")]
    WrongServer { expected: String, got: String },

    #[error("invalid token issuer: {0}")]
    InvalidIssuer(String),

    #[error("invalid token audience: {0}")]
    InvalidAudience(String),

    #[error("signing key not found: {0}")]
    KeyNotFound(String),

    #[error("missing or invalid key ID")]
    InvalidKeyId,

    #[error("invalid service account for project: {0}")]
    InvalidServiceAccount(String),
}

/// User-friendly error messages for auth errors.
pub fn user_friendly_error(err: &AuthError) -> &'static str {
    match err {
        AuthError::NoToken => "no token provided",
        AuthError::ExpiredToken => "token expired - please reauthenticate",
        AuthError::WrongServer { .. } => "token assigned to different server",
        AuthError::InvalidIssuer(_) => {
            "invalid token issuer - check firebase_project_id configuration"
        }
        AuthError::InvalidAudience(_) => {
            "invalid token audience - check firebase_project_id configuration"
        }
        AuthError::KeyNotFound(_) => {
            "signing key not found - token may be using an old or invalid key"
        }
        AuthError::InvalidKeyId => "missing or invalid key ID in token header",
        AuthError::InvalidServiceAccount(_) => "service account not authorized for this project",
        AuthError::InvalidClaims(msg) => {
            if msg.contains("missing uid") {
                "token missing required 'uid' field"
            } else if msg.contains("missing sub") {
                "token missing required 'sub' field"
            } else {
                "invalid token claims"
            }
        }
        AuthError::InvalidToken(msg) => {
            if msg.contains("signature") {
                "invalid token signature - check secret key configuration"
            } else if msg.contains("algorithm") || msg.contains("signing method") {
                "unsupported token algorithm"
            } else if msg.contains("malformed") {
                "malformed token"
            } else {
                "invalid token"
            }
        }
    }
}

/// Holds the extracted authentication information for use in rules.
#[derive(Debug, Clone)]
pub struct AuthInfo {
    /// User ID (from sub or uid claim)
    pub uid: String,
    /// Auth provider (e.g., "anonymous", "google", "custom")
    pub provider: String,
    /// Custom claims (becomes auth.token in rules)
    pub token: HashMap<String, serde_json::Value>,
    /// Database ID (from aud claim)
    pub database_id: Option<String>,
    /// Assigned server ID (from server claim)
    pub server_id: Option<String>,
    /// Project ID (from project claim or extracted from issuer)
    pub project_id: Option<String>,
    /// True if token was signed with admin_secret_key (kid: "coordinator")
    pub is_true_admin: bool,
}

impl Default for AuthInfo {
    fn default() -> Self {
        Self {
            uid: String::new(),
            provider: "custom".to_string(),
            token: HashMap::new(),
            database_id: None,
            server_id: None,
            project_id: None,
            is_true_admin: false,
        }
    }
}

/// Claims for standard Lark JWT tokens (HS256).
/// Standard claims (sub, exp, iat) are included via serde.
#[derive(Debug, Serialize, Deserialize)]
pub struct LarkClaims {
    /// Subject (user ID)
    pub sub: String,
    /// Expiration time (Unix timestamp)
    #[serde(default)]
    pub exp: Option<i64>,
    /// Issued at (Unix timestamp)
    #[serde(default)]
    pub iat: Option<i64>,
    /// Audience (database ID)
    #[serde(default)]
    pub aud: Option<StringOrArray>,
    /// Auth provider
    #[serde(default)]
    pub provider: Option<String>,
    /// Custom claims
    #[serde(default)]
    pub claims: Option<HashMap<String, serde_json::Value>>,
    /// Assigned server ID
    #[serde(default)]
    pub server: Option<String>,
    /// Project ID
    #[serde(default)]
    pub project: Option<String>,
}

/// Claims for Lark Customer Token (HS256 with uid field).
#[derive(Debug, Serialize, Deserialize)]
pub struct LarkCustomerClaims {
    /// User ID (NOT sub - this is the customer token format)
    pub uid: String,
    /// Expiration time
    #[serde(default)]
    pub exp: Option<i64>,
    /// Issued at
    #[serde(default)]
    pub iat: Option<i64>,
    /// Custom claims
    #[serde(default)]
    pub claims: Option<HashMap<String, serde_json::Value>>,
}

/// Claims for Firebase Legacy Token (HS256 with d field).
#[derive(Debug, Serialize, Deserialize)]
pub struct LegacyClaims {
    /// Version
    #[serde(default)]
    pub v: Option<String>,
    /// Legacy data field (custom claims)
    #[serde(default)]
    pub d: Option<HashMap<String, serde_json::Value>>,
    /// Expiration time
    #[serde(default)]
    pub exp: Option<i64>,
    /// Issued at
    #[serde(default)]
    pub iat: Option<i64>,
}

/// Helper type for audience which can be string or array.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StringOrArray {
    Single(String),
    Multiple(Vec<String>),
}

impl StringOrArray {
    pub fn first(&self) -> Option<&str> {
        match self {
            StringOrArray::Single(s) => Some(s),
            StringOrArray::Multiple(v) => v.first().map(|s| s.as_str()),
        }
    }
}

/// HS256 JWT Validator.
pub struct Validator {
    secret: Vec<u8>,
    server_id: Option<String>,
}

impl Validator {
    /// Create a new JWT validator with the given secret.
    pub fn new(secret: &[u8]) -> Self {
        Self {
            secret: secret.to_vec(),
            server_id: None,
        }
    }

    /// Create a validator that also checks the server claim.
    pub fn with_server(secret: &[u8], server_id: &str) -> Self {
        Self {
            secret: secret.to_vec(),
            server_id: Some(server_id.to_string()),
        }
    }

    /// Validate a JWT token and return auth info.
    pub fn validate(&self, token_string: &str) -> Result<AuthInfo, AuthError> {
        if token_string.is_empty() {
            return Err(AuthError::NoToken);
        }

        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = true;
        validation.required_spec_claims.clear(); // Don't require aud

        let key = DecodingKey::from_secret(&self.secret);

        let token_data = decode::<LarkClaims>(token_string, &key, &validation).map_err(|e| {
            if e.kind() == &jsonwebtoken::errors::ErrorKind::ExpiredSignature {
                AuthError::ExpiredToken
            } else {
                AuthError::InvalidToken(e.to_string())
            }
        })?;

        let claims = token_data.claims;

        // Extract UID from subject
        if claims.sub.is_empty() {
            return Err(AuthError::InvalidClaims("missing sub claim".to_string()));
        }

        // Check server claim if configured
        if let Some(ref expected_server) = self.server_id
            && let Some(ref token_server) = claims.server
            && token_server != expected_server
        {
            return Err(AuthError::WrongServer {
                expected: expected_server.clone(),
                got: token_server.clone(),
            });
        }

        // Extract database ID from audience
        let database_id = claims
            .aud
            .as_ref()
            .and_then(|a| a.first().map(String::from));

        // Build auth info
        let mut info = AuthInfo {
            uid: claims.sub,
            provider: claims.provider.unwrap_or_else(|| "custom".to_string()),
            token: claims.claims.unwrap_or_default(),
            database_id,
            server_id: claims.server,
            project_id: claims.project,
            is_true_admin: false,
        };

        // Default provider to "custom" if empty
        if info.provider.is_empty() {
            info.provider = "custom".to_string();
        }

        Ok(info)
    }
}

/// Validate a Lark Customer Token (HS256 with uid field).
pub fn validate_lark_customer_token(
    token_string: &str,
    secret: &[u8],
) -> Result<AuthInfo, AuthError> {
    if token_string.is_empty() {
        return Err(AuthError::NoToken);
    }

    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = true;
    validation.required_spec_claims.clear();

    let key = DecodingKey::from_secret(secret);

    let token_data =
        decode::<LarkCustomerClaims>(token_string, &key, &validation).map_err(|e| {
            if e.kind() == &jsonwebtoken::errors::ErrorKind::ExpiredSignature {
                AuthError::ExpiredToken
            } else {
                AuthError::InvalidToken(e.to_string())
            }
        })?;

    let claims = token_data.claims;

    // UID is required for Lark Customer Tokens
    if claims.uid.is_empty() {
        return Err(AuthError::InvalidClaims("missing uid".to_string()));
    }

    Ok(AuthInfo {
        uid: claims.uid,
        provider: "custom".to_string(),
        token: claims.claims.unwrap_or_default(),
        is_true_admin: false,
        ..Default::default()
    })
}

/// Validate a Firebase Legacy Token (HS256 with d field).
pub fn validate_legacy_token(token_string: &str, secret: &[u8]) -> Result<AuthInfo, AuthError> {
    if token_string.is_empty() {
        return Err(AuthError::NoToken);
    }

    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = true;
    validation.required_spec_claims.clear();

    let key = DecodingKey::from_secret(secret);

    let token_data = decode::<LegacyClaims>(token_string, &key, &validation).map_err(|e| {
        if e.kind() == &jsonwebtoken::errors::ErrorKind::ExpiredSignature {
            AuthError::ExpiredToken
        } else {
            AuthError::InvalidToken(e.to_string())
        }
    })?;

    let claims = token_data.claims;

    // Legacy tokens have empty UID - they predate user ID concept
    Ok(AuthInfo {
        uid: String::new(),
        provider: "custom".to_string(),
        token: claims.d.unwrap_or_default(),
        is_true_admin: false,
        ..Default::default()
    })
}

/// Peek at token header to get algorithm and kid without full validation.
pub fn peek_token_header(token_string: &str) -> Result<(String, Option<String>), AuthError> {
    let header = decode_header(token_string).map_err(|e| AuthError::InvalidToken(e.to_string()))?;

    let alg = match header.alg {
        Algorithm::HS256 => "HS256".to_string(),
        Algorithm::HS384 => "HS384".to_string(),
        Algorithm::HS512 => "HS512".to_string(),
        Algorithm::RS256 => "RS256".to_string(),
        Algorithm::RS384 => "RS384".to_string(),
        Algorithm::RS512 => "RS512".to_string(),
        Algorithm::ES256 => "ES256".to_string(),
        Algorithm::ES384 => "ES384".to_string(),
        Algorithm::PS256 => "PS256".to_string(),
        Algorithm::PS384 => "PS384".to_string(),
        Algorithm::PS512 => "PS512".to_string(),
        Algorithm::EdDSA => "EdDSA".to_string(),
    };

    Ok((alg, header.kid))
}

/// Peek at token payload without verification.
/// Returns the raw claims as a HashMap.
pub fn peek_token_payload(
    token_string: &str,
) -> Result<HashMap<String, serde_json::Value>, AuthError> {
    // Split the token and decode the payload (middle part)
    let parts: Vec<&str> = token_string.split('.').collect();
    if parts.len() != 3 {
        return Err(AuthError::InvalidToken("malformed token".to_string()));
    }

    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    let payload_bytes = URL_SAFE_NO_PAD
        .decode(parts[1])
        .map_err(|e| AuthError::InvalidToken(format!("invalid base64: {}", e)))?;

    let payload: HashMap<String, serde_json::Value> = serde_json::from_slice(&payload_bytes)
        .map_err(|e| AuthError::InvalidToken(format!("invalid JSON: {}", e)))?;

    Ok(payload)
}

/// Reserved claim names that should NOT be included in custom claims extraction.
const RESERVED_CLAIMS: &[&str] = &[
    // Standard JWT claims (RFC 7519)
    "iss",
    "sub",
    "aud",
    "exp",
    "iat",
    "nbf",
    "jti",
    // Firebase-specific claims
    "auth_time",
    "firebase",
    "user_id",
    "email",
    "email_verified",
    "name",
    "picture",
    "phone_number",
];

/// Extract custom claims from the root level of a JWT payload.
/// Firebase ID Tokens have custom claims at the root level (not nested).
pub fn find_root_claims(
    payload: &HashMap<String, serde_json::Value>,
) -> HashMap<String, serde_json::Value> {
    payload
        .iter()
        .filter(|(key, _)| !RESERVED_CLAIMS.contains(&key.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Generate a signed JWT token (for testing).
pub fn generate_token(
    secret: &[u8],
    uid: &str,
    provider: &str,
    custom_claims: Option<HashMap<String, serde_json::Value>>,
    expiry_secs: i64,
) -> Result<String, AuthError> {
    use jsonwebtoken::{EncodingKey, Header, encode};

    let now = chrono::Utc::now().timestamp();

    let claims = LarkClaims {
        sub: uid.to_string(),
        exp: Some(now + expiry_secs),
        iat: Some(now),
        aud: None,
        provider: Some(provider.to_string()),
        claims: custom_claims,
        server: None,
        project: None,
    };

    let key = EncodingKey::from_secret(secret);
    encode(&Header::new(Algorithm::HS256), &claims, &key)
        .map_err(|e| AuthError::InvalidToken(e.to_string()))
}

/// Generate a test token with 1 hour expiry.
pub fn generate_test_token(
    secret: &[u8],
    uid: &str,
    custom_claims: Option<HashMap<String, serde_json::Value>>,
) -> Result<String, AuthError> {
    generate_token(secret, uid, "anonymous", custom_claims, 3600)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SECRET: &[u8] = b"test-secret-key-1234567890";

    #[test]
    fn test_validator_basic() {
        let token = generate_test_token(TEST_SECRET, "user-123", None).unwrap();
        let validator = Validator::new(TEST_SECRET);
        let info = validator.validate(&token).unwrap();

        assert_eq!(info.uid, "user-123");
        assert_eq!(info.provider, "anonymous");
        assert!(!info.is_true_admin);
    }

    #[test]
    fn test_validator_with_custom_claims() {
        let mut claims = HashMap::new();
        claims.insert("role".to_string(), serde_json::json!("admin"));
        claims.insert("level".to_string(), serde_json::json!(42));

        let token = generate_test_token(TEST_SECRET, "user-456", Some(claims)).unwrap();
        let validator = Validator::new(TEST_SECRET);
        let info = validator.validate(&token).unwrap();

        assert_eq!(info.uid, "user-456");
        assert_eq!(info.token.get("role"), Some(&serde_json::json!("admin")));
        assert_eq!(info.token.get("level"), Some(&serde_json::json!(42)));
    }

    #[test]
    fn test_validator_invalid_secret() {
        let token = generate_test_token(TEST_SECRET, "user-123", None).unwrap();
        let validator = Validator::new(b"wrong-secret");
        let result = validator.validate(&token);

        assert!(matches!(result, Err(AuthError::InvalidToken(_))));
    }

    #[test]
    fn test_validator_empty_token() {
        let validator = Validator::new(TEST_SECRET);
        let result = validator.validate("");

        assert!(matches!(result, Err(AuthError::NoToken)));
    }

    #[test]
    fn test_validator_expired_token() {
        // Generate token that expired 1 hour ago
        let token = generate_token(TEST_SECRET, "user-123", "anonymous", None, -3600).unwrap();
        let validator = Validator::new(TEST_SECRET);
        let result = validator.validate(&token);

        assert!(matches!(result, Err(AuthError::ExpiredToken)));
    }

    #[test]
    fn test_peek_token_header() {
        let token = generate_test_token(TEST_SECRET, "user-123", None).unwrap();
        let (alg, kid) = peek_token_header(&token).unwrap();

        assert_eq!(alg, "HS256");
        assert!(kid.is_none()); // Our test tokens don't set kid
    }

    #[test]
    fn test_peek_token_payload() {
        let mut claims = HashMap::new();
        claims.insert("custom".to_string(), serde_json::json!("value"));

        let token = generate_test_token(TEST_SECRET, "user-123", Some(claims)).unwrap();
        let payload = peek_token_payload(&token).unwrap();

        assert_eq!(payload.get("sub"), Some(&serde_json::json!("user-123")));
    }

    #[test]
    fn test_find_root_claims() {
        let mut payload = HashMap::new();
        payload.insert("sub".to_string(), serde_json::json!("user-123"));
        payload.insert("iss".to_string(), serde_json::json!("issuer"));
        payload.insert("custom_field".to_string(), serde_json::json!("value"));
        payload.insert("role".to_string(), serde_json::json!("admin"));

        let custom = find_root_claims(&payload);

        // Reserved claims should be filtered out
        assert!(!custom.contains_key("sub"));
        assert!(!custom.contains_key("iss"));
        // Custom claims should be included
        assert_eq!(
            custom.get("custom_field"),
            Some(&serde_json::json!("value"))
        );
        assert_eq!(custom.get("role"), Some(&serde_json::json!("admin")));
    }

    #[test]
    fn test_lark_customer_token() {
        use jsonwebtoken::{EncodingKey, Header, encode};

        let claims = LarkCustomerClaims {
            uid: "customer-user-123".to_string(),
            exp: Some(chrono::Utc::now().timestamp() + 3600),
            iat: Some(chrono::Utc::now().timestamp()),
            claims: Some({
                let mut m = HashMap::new();
                m.insert("is_premium".to_string(), serde_json::json!(true));
                m
            }),
        };

        let key = EncodingKey::from_secret(TEST_SECRET);
        let token = encode(&Header::new(Algorithm::HS256), &claims, &key).unwrap();

        let info = validate_lark_customer_token(&token, TEST_SECRET).unwrap();
        assert_eq!(info.uid, "customer-user-123");
        assert_eq!(info.token.get("is_premium"), Some(&serde_json::json!(true)));
    }

    #[test]
    fn test_legacy_token() {
        use jsonwebtoken::{EncodingKey, Header, encode};

        let claims = LegacyClaims {
            v: Some("0".to_string()),
            d: Some({
                let mut m = HashMap::new();
                m.insert("legacy_field".to_string(), serde_json::json!("value"));
                m
            }),
            exp: Some(chrono::Utc::now().timestamp() + 3600),
            iat: Some(chrono::Utc::now().timestamp()),
        };

        let key = EncodingKey::from_secret(TEST_SECRET);
        let token = encode(&Header::new(Algorithm::HS256), &claims, &key).unwrap();

        let info = validate_legacy_token(&token, TEST_SECRET).unwrap();
        // Legacy tokens have empty UID
        assert!(info.uid.is_empty());
        assert_eq!(
            info.token.get("legacy_field"),
            Some(&serde_json::json!("value"))
        );
    }

    #[test]
    fn test_user_friendly_error() {
        assert_eq!(
            user_friendly_error(&AuthError::NoToken),
            "no token provided"
        );
        assert_eq!(
            user_friendly_error(&AuthError::ExpiredToken),
            "token expired - please reauthenticate"
        );
        assert_eq!(
            user_friendly_error(&AuthError::InvalidClaims("missing uid".to_string())),
            "token missing required 'uid' field"
        );
    }
}
