//! Executor module for Glommio thread-per-core runtime.
//!
//! This module provides the infrastructure for running Lark on Glommio's
//! thread-per-core model with io_uring. Each core runs its own LocalExecutor
//! with dedicated task queues for TCP I/O and database processing.

pub mod pool;

pub use pool::{ExecutorPool, ExecutorPoolConfig};

use xxhash_rust::xxh64::xxh64;

/// Compute which core should handle a given database.
/// Uses xxhash64 for consistent hashing - same function used by proxy.
pub fn core_for_database(database_id: &str, nr_cores: usize) -> usize {
    let hash = xxh64(database_id.as_bytes(), 0);
    (hash as usize) % nr_cores
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_core_for_database_deterministic() {
        // Same database ID should always map to same core
        let core1 = core_for_database("my-project/room-123", 8);
        let core2 = core_for_database("my-project/room-123", 8);
        assert_eq!(core1, core2);
    }

    #[test]
    fn test_core_for_database_distribution() {
        // Different database IDs should distribute across cores
        let mut cores = std::collections::HashSet::new();
        for i in 0..100 {
            let db_id = format!("project/db-{}", i);
            cores.insert(core_for_database(&db_id, 8));
        }
        // With 100 databases across 8 cores, we should hit most cores
        assert!(
            cores.len() >= 6,
            "Expected better distribution, got {} cores",
            cores.len()
        );
    }

    #[test]
    fn test_core_for_database_bounds() {
        // Result should always be < nr_cores
        for nr_cores in 1..=64 {
            for i in 0..100 {
                let db_id = format!("test/db-{}", i);
                let core = core_for_database(&db_id, nr_cores);
                assert!(core < nr_cores, "Core {} >= nr_cores {}", core, nr_cores);
            }
        }
    }
}
