//! Firebase token types and claim structures.
//!
//! Note: Actual token validation is handled by the proxy layer.
//! This module only contains type definitions for parsing token payloads.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Firebase ID Token claims.
#[derive(Debug, Serialize, Deserialize)]
pub struct FirebaseClaims {
    /// Subject (user ID)
    #[serde(default)]
    pub sub: String,
    /// Issuer
    #[serde(default)]
    pub iss: String,
    /// Audience
    #[serde(default)]
    pub aud: Option<StringOrArray>,
    /// Expiration time
    #[serde(default)]
    pub exp: Option<i64>,
    /// Issued at
    #[serde(default)]
    pub iat: Option<i64>,
    /// Time of authentication
    #[serde(default)]
    pub auth_time: Option<i64>,
    /// Firebase-specific info
    #[serde(default)]
    pub firebase: Option<FirebaseInfo>,
    /// User ID (also in sub)
    #[serde(default)]
    pub user_id: Option<String>,
    /// User's email
    #[serde(default)]
    pub email: Option<String>,
    /// Whether email is verified
    #[serde(default)]
    pub email_verified: Option<bool>,
    /// User's display name
    #[serde(default)]
    pub name: Option<String>,
    /// User's profile picture URL
    #[serde(default)]
    pub picture: Option<String>,
}

/// Firebase-specific authentication info.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct FirebaseInfo {
    /// Sign-in provider (e.g., "google.com", "password")
    #[serde(default)]
    pub sign_in_provider: Option<String>,
    /// Linked identities
    #[serde(default)]
    pub identities: Option<HashMap<String, Vec<String>>>,
    /// Multi-tenancy tenant ID
    #[serde(default)]
    pub tenant: Option<String>,
}

/// Firebase Custom Token claims.
#[derive(Debug, Serialize, Deserialize)]
pub struct FirebaseCustomTokenClaims {
    /// Subject (service account email for custom tokens)
    #[serde(default)]
    pub sub: String,
    /// Issuer (service account email)
    #[serde(default)]
    pub iss: String,
    /// Audience
    #[serde(default)]
    pub aud: Option<StringOrArray>,
    /// Expiration time
    #[serde(default)]
    pub exp: Option<i64>,
    /// Issued at
    #[serde(default)]
    pub iat: Option<i64>,
    /// User ID (NOT sub - this is the actual user ID)
    #[serde(default)]
    pub uid: Option<String>,
    /// Custom claims
    #[serde(default)]
    pub claims: Option<HashMap<String, serde_json::Value>>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_or_array() {
        let single = StringOrArray::Single("value".to_string());
        assert_eq!(single.first(), Some("value"));

        let multiple = StringOrArray::Multiple(vec!["a".to_string(), "b".to_string()]);
        assert_eq!(multiple.first(), Some("a"));

        let empty = StringOrArray::Multiple(vec![]);
        assert_eq!(empty.first(), None);
    }
}
