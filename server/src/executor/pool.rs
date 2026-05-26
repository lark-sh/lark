//! Executor pool for spawning per-core LocalExecutors.
//!
//! This module handles the setup of Glommio's thread-per-core model,
//! spawning one LocalExecutor per CPU core with appropriate task queues.

use glommio::{Latency, LocalExecutorBuilder, Placement, Shares, executor};
use std::time::Duration;
use tracing::{error, info};

/// Configuration for the executor pool.
#[derive(Clone, Debug)]
pub struct ExecutorPoolConfig {
    /// Number of cores to use (default: all available)
    pub nr_cores: usize,

    /// Port for TCP listener (each core binds with SO_REUSEPORT)
    pub port: u16,

    /// Whether running in emulator mode
    pub emulator: bool,

    /// Data directory for persistence (None for ephemeral)
    pub data_dir: Option<String>,
}

impl Default for ExecutorPoolConfig {
    fn default() -> Self {
        Self {
            nr_cores: std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1),
            port: 7779,
            emulator: false,
            data_dir: None,
        }
    }
}

/// Pool of per-core executors.
pub struct ExecutorPool {
    config: ExecutorPoolConfig,
}

impl ExecutorPool {
    /// Create a new executor pool with the given configuration.
    pub fn new(config: ExecutorPoolConfig) -> Self {
        Self { config }
    }

    /// Run the executor pool, blocking until shutdown.
    ///
    /// This spawns one LocalExecutor per core, each running the provided
    /// async function with its core_id and the shared config.
    pub fn run<F, Fut>(self, core_main: F)
    where
        F: Fn(usize, usize, ExecutorPoolConfig) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = ()> + 'static,
    {
        let nr_cores = self.config.nr_cores;
        info!("Starting executor pool with {} cores", nr_cores);

        let handles: Vec<_> = (0..nr_cores)
            .map(|core_id| {
                let config = self.config.clone();
                let core_main = core_main.clone();

                std::thread::spawn(move || {
                    let builder = LocalExecutorBuilder::new(Placement::Fixed(core_id))
                        .name(&format!("lark-core-{}", core_id));

                    match builder.spawn(move || async move {
                        core_main(core_id, nr_cores, config).await;
                    }) {
                        Ok(handle) => {
                            if let Err(e) = handle.join() {
                                error!("Core {} executor failed: {:?}", core_id, e);
                            }
                        }
                        Err(e) => {
                            error!("Failed to spawn executor for core {}: {}", core_id, e);
                        }
                    }
                })
            })
            .collect();

        // Wait for all cores to complete
        for (core_id, handle) in handles.into_iter().enumerate() {
            if let Err(e) = handle.join() {
                error!("Core {} thread panicked: {:?}", core_id, e);
            }
        }

        info!("Executor pool shutdown complete");
    }
}

/// Create task queues for a core.
///
/// Returns (tcp_tq, db_tq) handles.
pub fn create_task_queues() -> (glommio::TaskQueueHandle, glommio::TaskQueueHandle) {
    // TCP task queue - HIGH priority, latency-sensitive
    // Glommio will preempt other tasks to run these if they wait >5ms
    let tcp_tq = executor().create_task_queue(
        Shares::Static(100),
        Latency::Matters(Duration::from_millis(5)),
        "tcp-io",
    );

    // Database task queue - LOWER priority
    // Database processing can tolerate more latency
    let db_tq = executor().create_task_queue(Shares::Static(50), Latency::NotImportant, "database");

    (tcp_tq, db_tq)
}

/// Yield to the scheduler if needed.
///
/// This should be called periodically in long-running operations
/// to ensure TCP tasks get CPU time.
pub async fn yield_if_needed() {
    glommio::yield_if_needed().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_pool_config_default() {
        let config = ExecutorPoolConfig::default();
        assert!(config.nr_cores >= 1);
        assert_eq!(config.port, 7779);
        assert!(!config.emulator);
        assert!(config.data_dir.is_none());
    }
}
