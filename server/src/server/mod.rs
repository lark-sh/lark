//! Server module - per-core handlers and database management for Glommio.

pub mod config;
pub mod core_handler;

pub use config::{CachedProjectConfig, ProjectConfigCache};
pub use core_handler::{CoreHandler, CoreHandlerConfig, VirtualClientSender};

/// Storage configuration for persistence.
#[derive(Clone, Debug, Default)]
pub struct StorageConfig {
    /// SharedFS root directory for persistence.
    pub shared_fs_root: Option<String>,
    /// Template directory for load testing.
    pub template_path: Option<String>,
}

/// Parse project ID from database ID.
/// Database IDs are in format "project/database", e.g., "gorilla-smash/room-123".
pub fn parse_project_id(database_id: &str) -> String {
    if let Some(idx) = database_id.find('/') {
        database_id[..idx].to_string()
    } else {
        database_id.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_project_id() {
        assert_eq!(parse_project_id("my-project/room-123"), "my-project");
        assert_eq!(parse_project_id("project/db"), "project");
        assert_eq!(parse_project_id("simple-db"), "simple-db");
        assert_eq!(parse_project_id(""), "");
    }
}
