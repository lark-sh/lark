use clap::Parser;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(name = "lark-chaos-monkey")]
#[command(about = "Chaos testing tool for Lark storage layer")]
pub struct CliArgs {
    /// Path to the lark server binary
    #[arg(long, default_value = "./target/debug/lark")]
    pub server_bin: PathBuf,

    /// Path to the lark-compact binary
    #[arg(long, default_value = "./target/debug/lark-compact")]
    pub compact_bin: PathBuf,

    /// Shared data directory for persistence
    #[arg(long, default_value = "/tmp/chaos-data")]
    pub data_dir: PathBuf,

    /// How long to run (e.g. "1h", "30m", "5h")
    #[arg(long, default_value = "1h", value_parser = parse_duration)]
    pub duration: Duration,

    /// TCP port for proxy connections
    #[arg(long, default_value_t = 7779)]
    pub proxy_port: u16,

    /// Project ID for test databases
    #[arg(long, default_value = "chaos-project")]
    pub project_id: String,

    /// Database ID for test database
    #[arg(long, default_value = "monkey-db")]
    pub database_id: String,

    /// Number of virtual clients
    #[arg(long, default_value_t = 4)]
    pub num_clients: u32,

    /// Minimum seconds between kill cycles
    #[arg(long, default_value_t = 30)]
    pub min_kill_interval: u64,

    /// Maximum seconds between kill cycles
    #[arg(long, default_value_t = 120)]
    pub max_kill_interval: u64,

    /// RNG seed for reproducible runs
    #[arg(long)]
    pub seed: Option<u64>,

    /// Rules mode for the test database.
    ///
    /// - `open`: permissive `{".read":true,".write":true}` — original chaos behavior.
    /// - `lookup` (default): rules that require auth and reference `data.*` /
    ///   `newData.*`, forcing the rules engine through `LazySnapshot`/
    ///   `LazyUpdateSnapshot` promotion paths on every write. A small fraction
    ///   of generated ops use a deny-sentinel value that the rule rejects;
    ///   ground-truth tracks them as `Rejected` and verify confirms they don't
    ///   appear after restart.
    #[arg(long, default_value = "lookup")]
    pub rules_mode: RulesMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum RulesMode {
    Open,
    Lookup,
}

pub struct ChaosConfig {
    pub server_bin: PathBuf,
    pub compact_bin: PathBuf,
    pub data_dir: PathBuf,
    pub duration: Duration,
    pub proxy_port: u16,
    pub project_id: String,
    pub database_id: String,
    pub num_clients: u32,
    pub min_kill_interval: Duration,
    pub max_kill_interval: Duration,
    pub seed: Option<u64>,
    pub rules_mode: RulesMode,
}

impl From<CliArgs> for ChaosConfig {
    fn from(args: CliArgs) -> Self {
        Self {
            server_bin: args.server_bin,
            compact_bin: args.compact_bin,
            data_dir: args.data_dir,
            duration: args.duration,
            proxy_port: args.proxy_port,
            project_id: args.project_id,
            database_id: args.database_id,
            num_clients: args.num_clients,
            min_kill_interval: Duration::from_secs(args.min_kill_interval),
            max_kill_interval: Duration::from_secs(args.max_kill_interval),
            seed: args.seed,
            rules_mode: args.rules_mode,
        }
    }
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty duration".into());
    }

    let (num_str, unit) = if let Some(rest) = s.strip_suffix('h') {
        (rest, "h")
    } else if let Some(rest) = s.strip_suffix('m') {
        (rest, "m")
    } else if let Some(rest) = s.strip_suffix('s') {
        (rest, "s")
    } else {
        (s, "s")
    };

    let num: u64 = num_str
        .parse()
        .map_err(|e| format!("invalid number: {}", e))?;

    match unit {
        "h" => Ok(Duration::from_secs(num * 3600)),
        "m" => Ok(Duration::from_secs(num * 60)),
        "s" => Ok(Duration::from_secs(num)),
        _ => Err(format!("unknown unit: {}", unit)),
    }
}
