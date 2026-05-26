// Use jemalloc for better multi-threaded allocation performance
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use clap::Parser;
use glommio::channels::local_channel;
use glommio::{Latency, Shares, executor};
use lark_server::executor::{ExecutorPool, ExecutorPoolConfig};
use lark_server::server::{CoreHandler, CoreHandlerConfig};
use lark_server::storage::StorageWorker;
use lark_server::transport::proxy::ProxyListener;
use std::rc::Rc;
use std::time::{Duration, Instant};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Lark - Fast Multiplayer Database Server (Rust + Glommio)
#[derive(Parser, Debug, Clone)]
#[command(name = "lark")]
#[command(about = "Real-time database server")]
pub struct Args {
    /// Server ID (e.g., prod-001)
    #[arg(long, env = "LARK_SERVER_ID")]
    pub id: String,

    /// Public hostname for WebSocket clients (e.g., db.example.com)
    #[arg(long, env = "LARK_HOSTNAME")]
    pub hostname: String,

    /// Public IP for UDP clients (e.g., 203.0.113.10)
    #[arg(long, env = "LARK_PUBLIC_IP", default_value = "127.0.0.1")]
    pub public_ip: String,

    /// Private IP for backend communication (e.g., 10.0.0.1)
    #[arg(long, env = "LARK_PRIVATE_IP")]
    pub private_ip: Option<String>,

    /// Public port clients use to connect
    #[arg(long, default_value = "8080", env = "LARK_PUBLIC_PORT")]
    pub public_port: u16,

    /// Proxy transport listen port (each core binds with SO_REUSEPORT)
    #[arg(long, default_value = "2727", env = "LARK_PROXY_PORT")]
    pub proxy_port: u16,

    /// Host/interface the proxy transport listener binds to. Default "0.0.0.0"
    /// (all IPv4). Use "[::]" for dual-stack IPv6 — required on IPv6-only
    /// private networks such as Fly.io's 6PN. Must be bracketed for IPv6.
    #[arg(long, default_value = "0.0.0.0", env = "LARK_PROXY_BIND")]
    pub proxy_bind: String,

    /// Max databases this server can handle
    #[arg(long, default_value = "1000", env = "LARK_CAPACITY")]
    pub capacity: u32,

    /// Number of cores to use (default: all available)
    #[arg(long, env = "LARK_NR_CORES")]
    pub nr_cores: Option<usize>,

    /// Data directory for persistence (disabled if empty)
    #[arg(long, env = "LARK_DATA_DIR")]
    pub data_dir: Option<String>,

    /// Template directory for load testing
    #[arg(long, env = "LARK_TEMPLATE_PATH")]
    pub template: Option<String>,

    /// Enable emulator mode (accepts 'owner' token)
    #[arg(long, default_value = "false", env = "LARK_EMULATOR")]
    pub emulator: bool,

    /// Enable detailed message latency tracking
    #[arg(long, default_value = "false", env = "LARK_DEBUG_TIMING")]
    pub debug_timing: bool,

    /// Idle seconds before a promoted path is evicted back to Sentinel.
    /// Default 300s. Chaos-monkey sets this low (e.g. 20s) to exercise eviction.
    #[arg(long, default_value = "300", env = "LARK_EVICTION_IDLE_SECS")]
    pub eviction_idle_secs: u64,

    /// Coordinator URL for server registration (internal endpoint, e.g., http://lark-edge:8080)
    #[arg(long, env = "LARK_COORDINATOR_URL")]
    pub coordinator: Option<String>,
}

impl Args {
    /// Get the address to register with the coordinator (private_ip:proxy_port)
    pub fn registration_address(&self) -> Option<String> {
        self.private_ip
            .as_ref()
            .map(|ip| format!("{}:{}", ip, self.proxy_port))
    }
}

fn main() {
    // Initialize logging
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Parse CLI arguments
    let args = Args::parse();

    let nr_cores = args.nr_cores.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1)
    });

    tracing::info!("Starting Lark server (ID: {})", args.id);
    tracing::info!("Runtime: Glommio thread-per-core with {} cores", nr_cores);
    tracing::info!("Proxy port: {} (SO_REUSEPORT)", args.proxy_port);
    tracing::info!("Public hostname: {}:{}", args.hostname, args.public_port);
    tracing::info!("Capacity: {} databases", args.capacity);

    if args.emulator {
        tracing::warn!("EMULATOR MODE ENABLED - accepts 'owner' token");
    }

    if let Some(ref data_dir) = args.data_dir {
        if let Some(ref template) = args.template {
            tracing::info!(
                "Persistence: template mode (load from {}, WAL to {})",
                template,
                data_dir
            );
        } else {
            tracing::info!("Persistence: {}", data_dir);
        }
    }

    if args.debug_timing {
        lark_server::metrics::set_debug_timing(true);
    }

    lark_server::db::set_eviction_idle_secs(args.eviction_idle_secs);

    // Create executor pool configuration
    let pool_config = ExecutorPoolConfig {
        nr_cores,
        port: args.proxy_port,
        emulator: args.emulator,
        data_dir: args.data_dir.clone(),
    };

    // Run the executor pool
    let pool = ExecutorPool::new(pool_config);
    pool.run(move |core_id, nr_cores, config| {
        let args = args.clone();
        async move {
            run_core(core_id, nr_cores, config, args).await;
        }
    });

    tracing::info!("Server stopped");
}

/// Register this server with the coordinator.
/// Called once on startup after listeners are ready.
///
/// Retries on transport failures (connection refused, DNS errors, etc.)
/// with exponential backoff up to [`REGISTRATION_TOTAL_TIMEOUT`]. This is
/// what makes startup order-independent: a coordinator that comes up a
/// few seconds late, restarts during a rollout, or briefly drops its
/// listener won't lose the registration.
///
/// HTTP-level errors (the server responded with a non-2xx status) are
/// treated as fatal — those signal a real configuration problem (bad
/// auth, wrong URL shape) that won't fix itself on retry.
fn register_with_coordinator(args: &Args, nr_cores: usize) {
    const REGISTRATION_TOTAL_TIMEOUT: Duration = Duration::from_secs(60);
    const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
    const MAX_BACKOFF: Duration = Duration::from_secs(10);

    let coordinator = match &args.coordinator {
        Some(url) => url,
        None => {
            tracing::debug!("No coordinator URL configured, skipping registration");
            return;
        }
    };

    let address = match args.registration_address() {
        Some(addr) => addr,
        None => {
            tracing::warn!(
                "Coordinator URL set but no private_ip configured, skipping registration"
            );
            return;
        }
    };

    tracing::info!(
        "Registering with coordinator {} (address: {}, cores: {})",
        coordinator,
        address,
        nr_cores
    );

    let url = format!("{}/internal/register", coordinator);
    let started = Instant::now();
    let mut backoff = INITIAL_BACKOFF;
    let mut attempt: u32 = 0;

    loop {
        attempt += 1;
        let result = ureq::post(&url)
            .set("Content-Type", "application/json")
            .send_json(ureq::json!({
                "server_id": &args.id,
                "address": &address,
                "nr_cores": nr_cores
            }));

        match result {
            Ok(response) if response.status() == 200 => {
                tracing::info!(
                    "Successfully registered with coordinator (attempt {})",
                    attempt
                );
                return;
            }
            Ok(response) => {
                // 2xx-but-not-200 or any other non-error response we
                // weren't expecting; treat as terminal.
                tracing::error!(
                    "Coordinator registration returned status {}: {}",
                    response.status(),
                    response.status_text()
                );
                return;
            }
            Err(ureq::Error::Status(code, response)) => {
                // Server is reachable but rejected the request. Retrying
                // won't fix a 4xx (bad payload, missing field, etc.) and
                // a 5xx after a successful TCP handshake usually means a
                // bug on the other side; not retrying.
                let body = response.into_string().unwrap_or_default();
                tracing::error!(
                    "Coordinator rejected registration with status {}: {}",
                    code,
                    body
                );
                return;
            }
            Err(e) => {
                let elapsed = started.elapsed();
                if elapsed >= REGISTRATION_TOTAL_TIMEOUT {
                    tracing::error!(
                        "Coordinator registration failed after {} attempts over {:?}: {}",
                        attempt,
                        elapsed,
                        e
                    );
                    return;
                }
                tracing::warn!(
                    "Coordinator registration attempt {} failed: {} — retrying in {:?}",
                    attempt,
                    e,
                    backoff
                );
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

/// Run the main loop for a single core.
async fn run_core(core_id: usize, nr_cores: usize, config: ExecutorPoolConfig, args: Args) {
    tracing::debug!("Core {} starting", core_id);

    // Create task queues
    let tcp_tq = executor().create_task_queue(
        Shares::Static(100),
        Latency::Matters(Duration::from_millis(5)),
        "tcp-io",
    );

    let db_tq = executor().create_task_queue(Shares::Static(50), Latency::NotImportant, "database");

    // Create compaction channel and spawn storage worker on lower-priority task queue
    let (compaction_tx, compaction_rx) = local_channel::new_bounded(256);
    let compaction_tx = Rc::new(compaction_tx);

    glommio::spawn_local_into(
        async move {
            let mut worker = StorageWorker::new(compaction_rx);
            worker.run().await;
        },
        db_tq,
    )
    .expect("Failed to spawn storage worker")
    .detach();

    // Create the per-core handler
    let handler_config = CoreHandlerConfig {
        core_id,
        nr_cores,
        port: config.port,
        emulator: config.emulator,
        data_dir: config.data_dir.clone(),
        template_path: args.template.clone(),
    };

    let handler = CoreHandler::new(handler_config, compaction_tx);

    // Create and run proxy listener on TCP task queue
    let listener = ProxyListener::new(
        handler.clone(),
        core_id,
        nr_cores,
        config.port,
        args.proxy_bind.clone(),
    );

    // Spawn the proxy listener
    glommio::spawn_local_into(
        async move {
            if let Err(e) = listener.run().await {
                tracing::error!("Core {} proxy listener error: {}", core_id, e);
            }
        },
        tcp_tq,
    )
    .expect("Failed to spawn proxy listener")
    .detach();

    // Spawn debug timing logger if enabled (only on core 0 since stats are global)
    if args.debug_timing && core_id == 0 {
        glommio::spawn_local(async move {
            loop {
                glommio::timer::Timer::new(lark_server::metrics::STATS_INTERVAL).await;
                lark_server::metrics::log_latency_stats();
                lark_server::metrics::log_wal_stats();
            }
        })
        .detach();
    }

    tracing::info!(
        "Core {} ready - listening on port {} for proxy connections",
        core_id,
        config.port
    );

    // Core 0 registers with coordinator after listener is ready
    // This is a blocking call but only happens once at startup
    if core_id == 0 {
        // Small delay to allow other cores to bind their listeners
        glommio::timer::Timer::new(Duration::from_millis(100)).await;

        // Spawn registration in a thread to avoid blocking the async runtime
        let args_clone = args.clone();
        std::thread::spawn(move || {
            register_with_coordinator(&args_clone, nr_cores);
        });
    }

    // Run forever (or until killed)
    // In production, this would be wired up to signal handling
    // Database metrics are emitted by each Database in its run loop
    loop {
        glommio::timer::Timer::new(Duration::from_secs(60)).await;
    }
}
