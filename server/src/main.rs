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

/// Bound on the metrics-push channel. Each active database enqueues one small
/// JSON line per emit (~60s); a few thousand slots is ample headroom while
/// capping memory if the shipper stalls (excess samples are dropped, not queued).
const METRICS_CHANNEL_CAPACITY: usize = 4096;

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

    /// How often (milliseconds) buffered WAL entries are flushed to disk.
    /// Default 2000ms. `0` = synchronous: every write is flushed before its ACK,
    /// so the client's ACK means the write is persisted (higher latency). Lower
    /// values shrink the window of acknowledged-but-unflushed writes lost on crash.
    #[arg(long, default_value = "2000", env = "LARK_WAL_SYNC_INTERVAL_MS")]
    pub wal_sync_interval_ms: u64,

    /// Issue a real `fdatasync` on each WAL flush (`true`) instead of only writing
    /// to the OS page cache (`false`, default). Page-cache-only writes survive a
    /// process crash but not power loss; enable this for durability across power
    /// loss. Combine with `--wal-sync-interval-ms 0` for strict per-write durability.
    ///
    /// Takes an explicit value (`--fsync-on-wal-flush=true|false`, or
    /// `LARK_FSYNC_ON_WAL_FLUSH=true|false`) rather than being a bare flag, so
    /// `=false` reliably means false via either channel.
    #[arg(
        long,
        default_value_t = false,
        action = clap::ArgAction::Set,
        env = "LARK_FSYNC_ON_WAL_FLUSH"
    )]
    pub fsync_on_wal_flush: bool,

    /// Coordinator URL for server registration (internal endpoint, e.g., http://lark-edge:8080)
    #[arg(long, env = "LARK_COORDINATOR_URL")]
    pub coordinator: Option<String>,

    /// Push per-database metrics directly to the coordinator's /internal/metrics
    /// endpoint (instead of relying only on stdout + an external log shipper).
    /// Requires --coordinator.
    #[arg(long, default_value = "false", env = "LARK_METRICS_PUSH")]
    pub metrics_push: bool,

    /// Shared secret authenticating the edge↔server proxy channel. **Required.**
    /// Must match the `SERVER_SECRET` set on every lark-edge gateway. The proxy
    /// proves knowledge of it via an HMAC over a per-connection nonce during the
    /// HELLO handshake; connections that can't are rejected before any CONNECT is
    /// processed. Generate with e.g. `openssl rand -hex 32` and set identically
    /// on both sides.
    #[arg(long, env = "SERVER_SECRET")]
    pub server_secret: String,
}

impl Args {
    /// Get the address to register with the coordinator (private_ip:proxy_port)
    pub fn registration_address(&self) -> Option<String> {
        self.private_ip
            .as_ref()
            .map(|ip| format!("{}:{}", ip, self.proxy_port))
    }
}

/// The placeholder secret shipped in docker-compose.yml / .env.example. It is
/// published with the repo, so it must never authenticate a real channel.
const DEFAULT_SERVER_SECRET: &str = "dev-secret-change-me";

/// Minimum accepted SERVER_SECRET length in bytes outside emulator mode.
/// `openssl rand -hex 32` yields 64 chars, comfortably above this.
const MIN_SERVER_SECRET_LEN: usize = 32;

/// Reject a missing, publicly-known, or too-weak shared secret. Returns a
/// human-actionable message on failure; emulator mode bypasses this entirely.
fn validate_server_secret(secret: &str) -> Result<(), String> {
    if secret.is_empty() {
        return Err("SERVER_SECRET is required: generate one with `openssl rand -hex 32` (or run `make up`, which does this automatically), or pass --emulator for local dev".to_string());
    }
    if secret == DEFAULT_SERVER_SECRET {
        return Err(format!(
            "SERVER_SECRET is set to the publicly-known default {DEFAULT_SERVER_SECRET:?}: generate a real one with `openssl rand -hex 32` (or run `make up`, which does this automatically)"
        ));
    }
    if secret.len() < MIN_SERVER_SECRET_LEN {
        return Err(format!(
            "SERVER_SECRET must be at least {MIN_SERVER_SECRET_LEN} bytes (got {}): generate one with `openssl rand -hex 32`",
            secret.len()
        ));
    }
    Ok(())
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

    // Refuse to boot with a missing, publicly-known, or too-weak shared secret
    // outside emulator (dev) mode. A clone-and-run deploy must not authenticate
    // its edge↔server channel with the published compose default. See audit H-1.
    if !args.emulator
        && let Err(e) = validate_server_secret(&args.server_secret)
    {
        tracing::error!("{e}");
        std::process::exit(1);
    }

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
    lark_server::db::set_wal_sync_interval_ms(args.wal_sync_interval_ms);
    lark_server::db::set_fsync_on_wal_flush(args.fsync_on_wal_flush);

    // Create executor pool configuration
    let pool_config = ExecutorPoolConfig {
        nr_cores,
        port: args.proxy_port,
        emulator: args.emulator,
        data_dir: args.data_dir.clone(),
    };

    // Optional direct metrics push: spawn a single off-reactor shipper thread that
    // batches emitted metrics and POSTs them to the coordinator. The cores feed it
    // through a bounded, non-blocking channel (drop-on-full), so it can never stall
    // a core.
    let metrics_tx = match (args.metrics_push, args.coordinator.clone()) {
        (true, Some(coordinator)) => {
            let (tx, rx) = std::sync::mpsc::sync_channel::<String>(METRICS_CHANNEL_CAPACITY);
            let secret = args.server_secret.clone();
            std::thread::spawn(move || metrics_shipper(coordinator, secret, rx));
            tracing::info!("Direct metrics push enabled (coordinator /internal/metrics)");
            Some(tx)
        }
        (true, None) => {
            tracing::warn!(
                "LARK_METRICS_PUSH is set but no coordinator URL configured; metrics push disabled"
            );
            None
        }
        (false, _) => None,
    };

    // Run the executor pool
    let pool = ExecutorPool::new(pool_config);
    pool.run(move |core_id, nr_cores, config| {
        let args = args.clone();
        let metrics_tx = metrics_tx.clone();
        async move {
            run_core(core_id, nr_cores, config, args, metrics_tx).await;
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
            .set("Authorization", &format!("Bearer {}", args.server_secret))
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

/// Dedicated off-reactor thread that batches per-database metrics from all
/// cores and POSTs them to the coordinator's `/internal/metrics`.
///
/// Runs blocking `ureq` on its own thread — exactly like `register_with_coordinator`
/// — so it never touches a Glommio reactor. Cores hand it JSON lines through a
/// bounded, non-blocking channel; this side simply drains and ships. Failures are
/// logged and the batch dropped (metrics are lossy-tolerant). Returns when the
/// channel closes (all senders dropped, i.e. shutdown).
fn metrics_shipper(coordinator: String, secret: String, rx: std::sync::mpsc::Receiver<String>) {
    use std::sync::mpsc::RecvTimeoutError;

    const FLUSH_INTERVAL: Duration = Duration::from_secs(10);
    const MAX_BATCH: usize = 512;

    let url = format!("{}/internal/metrics", coordinator);
    let auth = format!("Bearer {secret}");
    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(10))
        .build();

    loop {
        // Block until a line arrives (waking periodically so a closed channel is noticed).
        let first = match rx.recv_timeout(FLUSH_INTERVAL) {
            Ok(line) => line,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => return,
        };

        // Grab everything else currently queued, up to a cap, into one batch.
        let mut batch = Vec::with_capacity(16);
        batch.push(first);
        while batch.len() < MAX_BATCH {
            match rx.try_recv() {
                Ok(line) => batch.push(line),
                Err(_) => break,
            }
        }

        // Each line is already a JSON object; wrap them in an array without re-parsing.
        let body = format!("[{}]", batch.join(","));
        if let Err(e) = agent
            .post(&url)
            .set("Content-Type", "application/json")
            .set("Authorization", &auth)
            .send_string(&body)
        {
            tracing::warn!(
                "metrics push failed ({} samples dropped): {}",
                batch.len(),
                e
            );
        }
    }
}

/// Run the main loop for a single core.
async fn run_core(
    core_id: usize,
    nr_cores: usize,
    config: ExecutorPoolConfig,
    args: Args,
    metrics_tx: Option<std::sync::mpsc::SyncSender<String>>,
) {
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

    let handler = CoreHandler::new(handler_config, compaction_tx, metrics_tx);

    // Create and run proxy listener on TCP task queue
    let listener = ProxyListener::new(
        handler.clone(),
        core_id,
        nr_cores,
        config.port,
        args.proxy_bind.clone(),
        Rc::new(args.server_secret.clone()),
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

#[cfg(test)]
mod tests {
    use super::{DEFAULT_SERVER_SECRET, validate_server_secret};

    #[test]
    fn rejects_empty_default_and_short_secrets() {
        assert!(validate_server_secret("").is_err());
        assert!(validate_server_secret(DEFAULT_SERVER_SECRET).is_err());
        assert!(validate_server_secret("short").is_err());
        // 31 bytes: one below the minimum.
        assert!(validate_server_secret(&"a".repeat(31)).is_err());
    }

    #[test]
    fn accepts_sufficiently_long_secrets() {
        // 32 bytes: exactly the minimum.
        assert!(validate_server_secret(&"a".repeat(32)).is_ok());
        // `openssl rand -hex 32` shape: 64 hex chars.
        assert!(validate_server_secret(&"0".repeat(64)).is_ok());
    }
}
