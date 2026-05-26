//! Authentication module for Lark.
//!
//! Supports multiple token types:
//! - HS256: Lark Customer tokens (uid field), Firebase Legacy tokens (d field),
//!   Coordinator Admin tokens (kid: "coordinator")
//!
//! Note: RS256 (Firebase ID tokens, Firebase Custom tokens) validation is handled
//! by the proxy layer. The server trusts auth info sent by the proxy.

pub mod firebase;
pub mod jwt;

use crate::auth::jwt::{
    AuthError, AuthInfo, Validator, peek_token_header, peek_token_payload,
    validate_lark_customer_token, validate_legacy_token,
};
use std::sync::atomic::{AtomicBool, Ordering};

/// MultiValidator validates Lark project tokens (HS256).
/// RS256 tokens (Firebase) are validated by the proxy layer.
pub struct MultiValidator {
    emulator_mode: AtomicBool,
}

impl MultiValidator {
    /// Create a validator that supports Lark tokens.
    /// Note: Firebase project IDs are no longer needed since proxy handles RS256 validation.
    pub fn new(_firebase_project_ids: &[String]) -> Self {
        Self {
            emulator_mode: AtomicBool::new(false),
        }
    }

    /// Enable or disable emulator mode.
    /// When enabled, the special "owner" token is accepted as an admin token.
    /// WARNING: Only enable this for local testing, never in production!
    pub fn set_emulator_mode(&self, enabled: bool) {
        self.emulator_mode.store(enabled, Ordering::SeqCst);
    }

    /// Add a Firebase project ID to the allowed list.
    /// Note: This is now a no-op since proxy handles Firebase token validation.
    pub fn add_firebase_project_id(&self, _project_id: &str) {
        // No-op - proxy handles Firebase token validation
    }

    /// Validate a token using project-specific keys.
    ///
    /// Supports HS256 tokens:
    ///   - kid: "coordinator" -> Coordinator Admin Token (use admin_secret_key, grants IsTrueAdmin)
    ///   - Has "d" field, no "uid" -> Firebase Legacy Token (use secret_key)
    ///   - Has "uid" field -> Lark Customer Token (use secret_key)
    ///
    /// RS256 tokens are NOT validated here - they should be validated by the proxy.
    ///
    /// Parameters:
    ///   - token_string: The JWT token to validate
    ///   - secret_key: Project's customer-facing secret (for customer-signed tokens)
    ///   - admin_secret_key: Coordinator's admin secret (for dashboard/admin tokens)
    ///   - firebase_project_id: Unused (kept for API compatibility)
    pub fn validate_for_project(
        &self,
        token_string: &str,
        secret_key: Option<&str>,
        admin_secret_key: Option<&str>,
        _firebase_project_id: Option<&str>,
    ) -> Result<AuthInfo, AuthError> {
        if token_string.is_empty() {
            return Err(AuthError::NoToken);
        }

        // Handle special "owner" token for emulator/testing mode
        if token_string == "owner" {
            if !self.emulator_mode.load(Ordering::SeqCst) {
                return Err(AuthError::InvalidToken(
                    "owner token only valid in emulator mode".to_string(),
                ));
            }
            let mut token_claims = std::collections::HashMap::new();
            token_claims.insert("isAdmin".to_string(), serde_json::json!(true));
            return Ok(AuthInfo {
                uid: "owner".to_string(),
                provider: "owner".to_string(),
                token: token_claims,
                is_true_admin: true,
                ..Default::default()
            });
        }

        // Peek at the token header to determine algorithm and kid
        let (alg, kid) = peek_token_header(token_string)?;

        match alg.as_str() {
            "HS256" => self.validate_hs256_token(
                token_string,
                secret_key,
                admin_secret_key,
                kid.as_deref(),
            ),
            "RS256" => {
                // RS256 tokens should be validated by the proxy layer
                Err(AuthError::InvalidToken(
                    "RS256 tokens must be validated by proxy layer".to_string(),
                ))
            }
            _ => Err(AuthError::InvalidToken(format!(
                "unsupported algorithm {}",
                alg
            ))),
        }
    }

    /// Validate an HS256 token using the appropriate key based on kid and payload.
    fn validate_hs256_token(
        &self,
        token_string: &str,
        secret_key: Option<&str>,
        admin_secret_key: Option<&str>,
        kid: Option<&str>,
    ) -> Result<AuthInfo, AuthError> {
        // Coordinator-signed admin token - must use admin_secret_key
        if kid == Some("coordinator") {
            let admin_key = admin_secret_key.ok_or_else(|| {
                AuthError::InvalidToken(
                    "no admin secret key available for coordinator token".to_string(),
                )
            })?;

            let validator = Validator::new(admin_key.as_bytes());
            let mut info = validator.validate(token_string)?;
            // Grant true admin access
            info.is_true_admin = true;
            return Ok(info);
        }

        // Customer-signed token - need secret_key
        let customer_key = secret_key
            .ok_or_else(|| AuthError::InvalidToken("no secret key available".to_string()))?;

        // Peek at payload to determine token type (Legacy vs Customer)
        let payload = peek_token_payload(token_string)?;

        // Check for Firebase Legacy Token (has "d" field, no "uid" field)
        let has_d = payload.contains_key("d");
        let has_uid = payload.contains_key("uid");

        if has_d && !has_uid {
            // Firebase Legacy Token
            let mut info = validate_legacy_token(token_string, customer_key.as_bytes())?;
            info.is_true_admin = false;
            return Ok(info);
        }

        // Check for Lark Customer Token (has "uid" field)
        if has_uid {
            let mut info = validate_lark_customer_token(token_string, customer_key.as_bytes())?;
            info.is_true_admin = false;
            return Ok(info);
        }

        // Fallback: Try the standard validator that uses "sub" field
        let validator = Validator::new(customer_key.as_bytes());
        let mut info = validator.validate(token_string)?;
        // Customer tokens never get true admin
        info.is_true_admin = false;
        Ok(info)
    }
}

// Re-export commonly used types
pub use jwt::{generate_test_token, generate_token, user_friendly_error};

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SECRET: &[u8] = b"test-secret-key-1234567890";
    const ADMIN_SECRET: &[u8] = b"admin-secret-key-1234567890";

    #[test]
    fn test_multi_validator_emulator_mode() {
        let validator = MultiValidator::new(&[]);

        // Without emulator mode, owner token should fail
        let result = validator.validate_for_project("owner", None, None, None);
        assert!(matches!(result, Err(AuthError::InvalidToken(_))));

        // Enable emulator mode
        validator.set_emulator_mode(true);

        // Now owner token should work
        let info = validator
            .validate_for_project("owner", None, None, None)
            .unwrap();
        assert_eq!(info.uid, "owner");
        assert_eq!(info.provider, "owner");
        assert!(info.is_true_admin);
    }

    #[test]
    fn test_multi_validator_hs256_standard() {
        let validator = MultiValidator::new(&[]);

        let token = generate_test_token(TEST_SECRET, "user-123", None).unwrap();

        let info = validator
            .validate_for_project(
                &token,
                Some(std::str::from_utf8(TEST_SECRET).unwrap()),
                None,
                None,
            )
            .unwrap();

        assert_eq!(info.uid, "user-123");
        assert!(!info.is_true_admin);
    }

    #[test]
    fn test_multi_validator_hs256_customer_token() {
        use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};

        let validator = MultiValidator::new(&[]);

        // Create a Lark Customer Token (has uid field)
        let claims = jwt::LarkCustomerClaims {
            uid: "customer-456".to_string(),
            exp: Some(chrono::Utc::now().timestamp() + 3600),
            iat: Some(chrono::Utc::now().timestamp()),
            claims: None,
        };

        let key = EncodingKey::from_secret(TEST_SECRET);
        let token = encode(&Header::new(Algorithm::HS256), &claims, &key).unwrap();

        let info = validator
            .validate_for_project(
                &token,
                Some(std::str::from_utf8(TEST_SECRET).unwrap()),
                None,
                None,
            )
            .unwrap();

        assert_eq!(info.uid, "customer-456");
        assert!(!info.is_true_admin);
    }

    #[test]
    fn test_multi_validator_hs256_coordinator_token() {
        use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};

        let validator = MultiValidator::new(&[]);

        // Create a Coordinator Admin Token (kid: "coordinator")
        let claims = jwt::LarkClaims {
            sub: "admin-user".to_string(),
            exp: Some(chrono::Utc::now().timestamp() + 3600),
            iat: Some(chrono::Utc::now().timestamp()),
            aud: None,
            provider: Some("admin".to_string()),
            claims: None,
            server: None,
            project: None,
        };

        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some("coordinator".to_string());

        let key = EncodingKey::from_secret(ADMIN_SECRET);
        let token = encode(&header, &claims, &key).unwrap();

        let info = validator
            .validate_for_project(
                &token,
                None,
                Some(std::str::from_utf8(ADMIN_SECRET).unwrap()),
                None,
            )
            .unwrap();

        assert_eq!(info.uid, "admin-user");
        assert!(info.is_true_admin); // Coordinator tokens get true admin
    }

    #[test]
    fn test_multi_validator_hs256_legacy_token() {
        use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};

        let validator = MultiValidator::new(&[]);

        // Create a Firebase Legacy Token (has d field, no uid)
        let claims = jwt::LegacyClaims {
            v: Some("0".to_string()),
            d: Some({
                let mut m = std::collections::HashMap::new();
                m.insert("legacy".to_string(), serde_json::json!(true));
                m
            }),
            exp: Some(chrono::Utc::now().timestamp() + 3600),
            iat: Some(chrono::Utc::now().timestamp()),
        };

        let key = EncodingKey::from_secret(TEST_SECRET);
        let token = encode(&Header::new(Algorithm::HS256), &claims, &key).unwrap();

        let info = validator
            .validate_for_project(
                &token,
                Some(std::str::from_utf8(TEST_SECRET).unwrap()),
                None,
                None,
            )
            .unwrap();

        // Legacy tokens have empty UID
        assert!(info.uid.is_empty());
        assert!(!info.is_true_admin);
        assert_eq!(info.token.get("legacy"), Some(&serde_json::json!(true)));
    }

    #[test]
    fn test_multi_validator_no_token() {
        let validator = MultiValidator::new(&[]);

        let result = validator.validate_for_project("", None, None, None);

        assert!(matches!(result, Err(AuthError::NoToken)));
    }

    #[test]
    fn test_multi_validator_no_secret_key() {
        let validator = MultiValidator::new(&[]);

        let token = generate_test_token(TEST_SECRET, "user-123", None).unwrap();

        let result = validator.validate_for_project(&token, None, None, None);

        assert!(matches!(result, Err(AuthError::InvalidToken(_))));
    }
}
