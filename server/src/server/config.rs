//! Project configuration caching.
//!
//! In the Glommio model, each core has its own config cache (no shared state).
//! Configs are received from the proxy via CONFIG_PUSH messages.

use crate::rules::Evaluator;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::warn;

/// Cached project configuration.
#[derive(Clone)]
pub struct CachedProjectConfig {
    pub project_id: String,
    pub rules: Option<Arc<Evaluator>>,
    pub ephemeral: bool,
    pub secret_key: String,
    pub admin_secret_key: String,
    pub firebase_project_id: String,
    pub firebase_compat_enabled: bool,
    pub firebase_default_database: String,
}

impl Default for CachedProjectConfig {
    fn default() -> Self {
        Self {
            project_id: String::new(),
            rules: None,
            ephemeral: true,
            secret_key: String::new(),
            admin_secret_key: String::new(),
            firebase_project_id: String::new(),
            firebase_compat_enabled: true,
            firebase_default_database: "default".to_string(),
        }
    }
}

/// Project configuration cache.
/// Each core has its own cache (single-threaded, no locking needed).
pub struct ProjectConfigCache {
    cache: HashMap<String, CachedProjectConfig>,
}

impl ProjectConfigCache {
    /// Create a new empty cache.
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    /// Get project config if cached.
    pub fn get(&self, project_id: &str) -> Option<&CachedProjectConfig> {
        self.cache.get(project_id)
    }

    /// Get project config, returning default if not cached.
    pub fn get_or_default(&self, project_id: &str) -> CachedProjectConfig {
        self.cache
            .get(project_id)
            .cloned()
            .unwrap_or_else(|| CachedProjectConfig {
                project_id: project_id.to_string(),
                ..Default::default()
            })
    }

    /// Set project config directly.
    pub fn set(&mut self, project_id: &str, config: CachedProjectConfig) {
        self.cache.insert(project_id.to_string(), config);
    }

    /// Set project secrets.
    pub fn set_secrets(&mut self, project_id: &str, secret_key: &str, admin_secret_key: &str) {
        let config =
            self.cache
                .entry(project_id.to_string())
                .or_insert_with(|| CachedProjectConfig {
                    project_id: project_id.to_string(),
                    ..Default::default()
                });
        config.secret_key = secret_key.to_string();
        config.admin_secret_key = admin_secret_key.to_string();
    }

    /// Set project rules.
    pub fn set_rules(&mut self, project_id: &str, rules: Option<Arc<Evaluator>>, ephemeral: bool) {
        let config =
            self.cache
                .entry(project_id.to_string())
                .or_insert_with(|| CachedProjectConfig {
                    project_id: project_id.to_string(),
                    ..Default::default()
                });
        config.rules = rules;
        config.ephemeral = ephemeral;
    }

    /// Update config from a proxy CONFIG_PUSH.
    pub fn update_from_push(
        &mut self,
        project_id: &str,
        rules_json: Option<serde_json::Value>,
        ephemeral: bool,
        secret_key: Option<String>,
        admin_secret_key: Option<String>,
    ) {
        let rules = rules_json.and_then(|json| match crate::rules::parse_rules(&json) {
            Ok(ruleset) => Some(Arc::new(Evaluator::new(ruleset))),
            Err(e) => {
                warn!(
                    "Failed to parse pushed rules for project {}: {}",
                    project_id, e
                );
                None
            }
        });

        let config =
            self.cache
                .entry(project_id.to_string())
                .or_insert_with(|| CachedProjectConfig {
                    project_id: project_id.to_string(),
                    ..Default::default()
                });
        config.rules = rules;
        config.ephemeral = ephemeral;
        if let Some(key) = secret_key {
            config.secret_key = key;
        }
        if let Some(key) = admin_secret_key {
            config.admin_secret_key = key;
        }
    }

    /// Remove a project from cache.
    pub fn remove(&mut self, project_id: &str) {
        self.cache.remove(project_id);
    }

    /// Clear all cached configs.
    pub fn clear(&mut self) {
        self.cache.clear();
    }
}

impl Default for ProjectConfigCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_set_and_get() {
        let mut cache = ProjectConfigCache::new();

        let config = CachedProjectConfig {
            project_id: "test-project".to_string(),
            secret_key: "secret123".to_string(),
            ..Default::default()
        };

        cache.set("test-project", config);

        let cached = cache.get("test-project");
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().secret_key, "secret123");
    }

    #[test]
    fn test_set_secrets() {
        let mut cache = ProjectConfigCache::new();

        cache.set_secrets("my-project", "customer-secret", "admin-secret");

        let cached = cache.get("my-project");
        assert!(cached.is_some());
        let config = cached.unwrap();
        assert_eq!(config.secret_key, "customer-secret");
        assert_eq!(config.admin_secret_key, "admin-secret");
    }

    #[test]
    fn test_default_config() {
        let config = CachedProjectConfig::default();

        assert!(config.project_id.is_empty());
        assert!(config.ephemeral);
        assert!(config.firebase_compat_enabled);
        assert_eq!(config.firebase_default_database, "default");
    }

    #[test]
    fn test_get_or_default() {
        let cache = ProjectConfigCache::new();

        let config = cache.get_or_default("unknown-project");

        assert_eq!(config.project_id, "unknown-project");
        assert!(config.ephemeral); // Default is ephemeral
    }
}
