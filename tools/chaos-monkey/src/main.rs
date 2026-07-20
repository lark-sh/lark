//! Lark Chaos Monkey
//!
//! A standalone chaos testing tool that continuously writes data to a real Lark server,
//! randomly kills the server, and verifies data integrity after restart.
//!
//! Any write that was acknowledged (ACK) must survive. In `--durability default`
//! mode the only acceptable loss is writes in-flight during the crash (the WAL
//! flushes every 2s, so an ACK'd write may still be in the in-process buffer);
//! the run loop tolerates this by not trusting ACKs in a grace window before the
//! kill. In `--durability strict` mode the server flushes before every ACK, so
//! the run loop trusts every ACK up to the kill and requires zero loss.

// Protocol structs mirror the server's wire format and include fields parsed
// off the wire that this tool doesn't act on; helper methods are kept for
// completeness even when unused.
#![allow(dead_code)]

mod compaction;
mod config;
mod disk;
mod ground_truth;
mod operations;
mod process;
mod protocol;
mod report;
mod verify;

use clap::Parser;
use config::{ChaosConfig, CliArgs, Durability, RulesMode};
use ground_truth::{GroundTruth, WriteOp};
use operations::{OpType, OperationGenerator, TxOpKind};
use process::{create_server_process, wait_for_server};
use protocol::client::{ProxyClient, ServerEvent};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use report::{ChaosReport, CycleReport};
use std::time::Instant;

/// Shared secret for the edge↔server proxy handshake. chaos-monkey plays the role
/// of the gateway, so it spawns lark-server with this `--server-secret` and proves
/// the same value via the HELLO_AUTH HMAC. Any fixed value works — this is a local
/// test harness, not a deployment.
pub const SERVER_SECRET: &str = "chaos-monkey-secret";
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, warn};

/// Probability of generating a deny-marker op when rules-mode is `lookup`.
/// ~5% — frequent enough to exercise the NACK path, rare enough not to
/// dominate cycle output.
const DENY_OP_PROBABILITY: f64 = 0.05;

/// Returns the rules JSON to push for the configured rules mode.
///
/// - `Open`: permissive `.read: true / .write: true` — original chaos behavior.
/// - `Lookup`: forces every write through a `data.val()` lookup (which triggers
///   blob promotion via `LazySnapshot::check_promotion`) and rejects writes
///   whose `newData.val() === '__chaos_deny__'`. The deny marker lets us
///   deliberately exercise the rule-deny path; the unconditional `data.val()`
///   on every write exercises the rules-eval promotion retry loop under chaos.
fn chaos_rules_json(mode: RulesMode) -> &'static str {
    match mode {
        RulesMode::Open => r#"{"rules": {".read": true, ".write": true}}"#,
        RulesMode::Lookup => {
            r#"{
            "rules": {
                ".read": true,
                ".write": "auth !== null && data.val() !== '__chaos_locked__' && newData.val() !== '__chaos_deny__'"
            }
        }"#
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = CliArgs::parse();
    let config: ChaosConfig = args.into();

    info!("Lark Chaos Monkey starting");
    info!("  Server binary: {}", config.server_bin.display());
    info!("  Compact binary: {}", config.compact_bin.display());
    info!("  Data directory: {}", config.data_dir.display());
    info!("  Duration: {:?}", config.duration);
    info!("  Proxy port: {}", config.proxy_port);
    info!("  Project: {}", config.project_id);
    info!("  Database: {}", config.database_id);
    info!("  Clients: {}", config.num_clients);
    info!(
        "  Kill interval: {:?} - {:?}",
        config.min_kill_interval, config.max_kill_interval
    );
    info!("  Rules mode: {:?}", config.rules_mode);
    info!("  Durability: {:?}", config.durability);
    // Resolve the seed up front so a randomly-chosen one is still printed —
    // lets the user copy it from the run header and re-run a failing
    // chaos session deterministically with `--seed N`.
    let (seed, seed_origin) = match config.seed {
        Some(s) => (s, "explicit"),
        None => (rand::thread_rng().gen(), "random"),
    };
    info!("  RNG seed: {} ({})", seed, seed_origin);

    // Create data directory
    tokio::fs::create_dir_all(&config.data_dir).await?;

    let mut rng: StdRng = StdRng::seed_from_u64(seed);

    let server_addr = format!("127.0.0.1:{}", config.proxy_port);
    let mut report = ChaosReport::new();
    let mut cycle_number = 0;
    let start = Instant::now();

    // Main chaos loop
    while start.elapsed() < config.duration {
        cycle_number += 1;
        info!("========== CYCLE {} ==========", cycle_number);

        let cycle_start = Instant::now();
        let mut ground_truth = GroundTruth::new();
        let mut op_gen = OperationGenerator::new();

        // Step 1: Start server (compaction runs in-process)
        let mut server = create_server_process(
            &config.server_bin,
            &config.data_dir,
            config.proxy_port,
            config.durability,
        );

        server.start().await?;

        // Step 2: Wait for server to accept connections
        if let Err(e) = wait_for_server(&server_addr, Duration::from_secs(30)).await {
            error!("Server failed to start: {}", e);
            server.kill().await?;
            continue;
        }

        // Step 3: Connect and handshake
        let mut client = match ProxyClient::connect(&server_addr).await {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to connect: {}", e);
                server.kill().await?;
                continue;
            }
        };

        // Step 4: Connect virtual clients
        for i in 1..=config.num_clients {
            let auth_uid = format!("chaos-{}", i);
            if let Err(e) = client
                .connect_client(i, &config.project_id, &config.database_id, &auth_uid)
                .await
            {
                error!("Failed to connect client {}: {}", i, e);
            }
        }

        // Step 5: Handle CONFIG_REQUEST and push config
        // Wait for CONFIG_REQUEST or DATABASE_LOADED events
        let config_timeout = Duration::from_secs(10);
        let config_deadline = tokio::time::Instant::now() + config_timeout;
        let mut config_pushed = false;
        let mut db_loaded = false;

        while tokio::time::Instant::now() < config_deadline && (!config_pushed || !db_loaded) {
            match client.recv_event(Duration::from_millis(500)).await {
                Some(ServerEvent::DatabaseLoaded {
                    project_id,
                    database_id,
                }) => {
                    if database_id.is_empty() {
                        // This is a CONFIG_REQUEST (we repurposed DatabaseLoaded event)
                        debug!("Pushing config for project: {}", project_id);
                        let rules = chaos_rules_json(config.rules_mode);
                        if let Err(e) = client.push_config_with_rules(&project_id, rules).await {
                            error!("Failed to push config: {}", e);
                        }
                        config_pushed = true;
                    } else {
                        debug!("Database loaded: {}/{}", project_id, database_id);
                        db_loaded = true;
                    }
                }
                Some(ServerEvent::Heartbeat) => {
                    let _ = client.send_heartbeat_ack().await;
                }
                Some(_) => {}
                None => {}
            }
        }

        if !config_pushed {
            warn!("Never received CONFIG_REQUEST — pushing config proactively");
            let rules = chaos_rules_json(config.rules_mode);
            let _ = client
                .push_config_with_rules(&config.project_id, rules)
                .await;
        }

        // Give the database a moment to fully load
        sleep(Duration::from_millis(500)).await;

        // Step 5b: Seed some initial data to exercise blob compaction.
        info!("Seeding initial data...");
        let seed_ops = op_gen.seed_collections(&mut rng);
        let seed_count = seed_ops.len();
        for op in seed_ops {
            let client_id = rng.gen_range(1..=config.num_clients);
            let req_id = client.next_request_id();
            ground_truth.record_sent(&req_id, client_id, &op.path, WriteOp::Set(op.value.clone()));
            if let Err(e) = client
                .send_set(client_id, &op.path, op.value, &req_id)
                .await
            {
                warn!("Seed write failed: {}", e);
                break;
            }
            // Drain responses periodically to avoid backpressure
            if seed_count > 0 {
                for event in client.drain_events() {
                    handle_event(&event, &mut ground_truth, &mut client).await;
                }
            }
        }
        info!("Seeded {} entries", seed_count);

        // Give compaction time to process
        sleep(Duration::from_secs(3)).await;

        // Drain any remaining responses from seeding
        for event in client.drain_events() {
            handle_event(&event, &mut ground_truth, &mut client).await;
        }

        // Step 6: Run random operations for a random duration
        let op_duration_secs =
            rng.gen_range(config.min_kill_interval.as_secs()..=config.max_kill_interval.as_secs());
        let op_duration = Duration::from_secs(op_duration_secs);
        info!("Running operations for {:?}", op_duration);

        let ops_start = Instant::now();
        let mut ops_sent = 0;

        while ops_start.elapsed() < op_duration {
            // Check if we've exceeded total duration
            if start.elapsed() >= config.duration {
                break;
            }

            // Generate and send an operation. In `lookup` rules mode, a small
            // fraction of ops are deny-marker writes that the rule will reject —
            // they exercise the NACK path end-to-end and `verify` confirms the
            // deny value never lands.
            let op = if config.rules_mode == RulesMode::Lookup && rng.gen_bool(DENY_OP_PROBABILITY)
            {
                op_gen.generate_deny(&mut rng)
            } else {
                op_gen.generate(&mut rng)
            };
            let client_id = rng.gen_range(1..=config.num_clients);
            let req_id = client.next_request_id();

            let write_op = match &op.op_type {
                OpType::Set => WriteOp::Set(op.value.clone()),
                OpType::Update => WriteOp::Update(op.value.clone()),
                OpType::Transaction => WriteOp::Transaction(op.tx_ops.clone().unwrap_or_default()),
            };
            ground_truth.record_sent(&req_id, client_id, &op.path, write_op);

            let send_result = match op.op_type {
                OpType::Set => {
                    client
                        .send_set(client_id, &op.path, op.value, &req_id)
                        .await
                }
                OpType::Update => {
                    client
                        .send_update(client_id, &op.path, op.value, &req_id)
                        .await
                }
                OpType::Transaction => {
                    // Encode each sub-op as a JSON object matching the
                    // server's TransactionOp on-wire schema.
                    let tx_ops = op.tx_ops.unwrap_or_default();
                    let wire_ops: Vec<serde_json::Value> = tx_ops
                        .into_iter()
                        .map(|sub| {
                            let kind = match sub.kind {
                                TxOpKind::Set => "s",
                                TxOpKind::Update => "u",
                                TxOpKind::Delete => "d",
                            };
                            let mut obj = serde_json::Map::new();
                            obj.insert(
                                "o".to_string(),
                                serde_json::Value::String(kind.to_string()),
                            );
                            obj.insert("p".to_string(), serde_json::Value::String(sub.path));
                            if let Some(v) = sub.value {
                                obj.insert("v".to_string(), v);
                            }
                            serde_json::Value::Object(obj)
                        })
                        .collect();
                    client.send_transaction(client_id, wire_ops, &req_id).await
                }
            };

            if let Err(e) = send_result {
                warn!("Failed to send operation: {}", e);
                break;
            }

            ops_sent += 1;

            // Process responses (non-blocking)
            for event in client.drain_events() {
                handle_event(&event, &mut ground_truth, &mut client).await;
            }

            // Small delay between operations to avoid overwhelming the server
            if ops_sent % 10 == 0 {
                sleep(Duration::from_millis(1)).await;
            }

            // Periodically do a burst
            if rng.gen_ratio(1, 200) {
                let burst = op_gen.generate_burst(&mut rng);
                for bop in burst {
                    let req_id = client.next_request_id();
                    let burst_client_id = rng.gen_range(1..=config.num_clients);
                    ground_truth.record_sent(
                        &req_id,
                        burst_client_id,
                        &bop.path,
                        WriteOp::Set(bop.value.clone()),
                    );
                    if let Err(e) = client
                        .send_set(burst_client_id, &bop.path, bop.value, &req_id)
                        .await
                    {
                        warn!("Burst send error: {}", e);
                        break;
                    }
                    ops_sent += 1;
                }
            }
        }

        // Step 7a: Stop sending new operations and drain remaining ACKs for 2s.
        // ACKs received here are trusted (committed).
        info!("Draining responses for 2s...");
        let drain_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while tokio::time::Instant::now() < drain_deadline {
            if let Some(event) = client.recv_event(Duration::from_millis(100)).await {
                handle_event(&event, &mut ground_truth, &mut client).await;
            }
        }

        // Step 7b: Grace period (4s). Keep writing to exercise "crash during WAL write".
        //
        // In `default` durability, do NOT mark ACKs as committed: the WAL syncs
        // every 2s and the kernel dirty-page flush takes up to 3s, so writes
        // ACK'd in this window may not survive a SIGKILL. Leaving them as Sent
        // means ground truth treats them as pending (survived = fine, lost = fine).
        //
        // In `strict` durability, the server flushes (and fdatasync's) before
        // every ACK, so an observed ACK is durable even under an immediate
        // SIGKILL. Keep trusting ACKs right up to the kill — any missing ACK'd
        // write is then a real violation (zero-loss contract).
        let strict = config.durability == Durability::Strict;
        if strict {
            info!("Grace period: writing for 4s (strict — ACKs trusted, zero loss required)...");
        } else {
            info!("Grace period: writing for 4s (ACKs not trusted)...");
        }
        let grace_deadline = tokio::time::Instant::now() + Duration::from_secs(4);
        let mut grace_ops = 0;
        while tokio::time::Instant::now() < grace_deadline {
            // Send a write to a unique grace-period path
            let grace_path = format!("/grace/item-{}", grace_ops);
            let grace_value = serde_json::json!({"grace": true, "n": grace_ops});
            let client_id = rng.gen_range(1..=config.num_clients);
            let req_id = client.next_request_id();
            ground_truth.record_sent(
                &req_id,
                client_id,
                &grace_path,
                WriteOp::Set(grace_value.clone()),
            );
            let _ = client
                .send_set(client_id, &grace_path, grace_value, &req_id)
                .await;
            grace_ops += 1;

            if strict {
                // Trust ACKs: mark committed so they're verified with zero tolerance.
                if let Some(event) = client.recv_event(Duration::from_millis(50)).await {
                    handle_event(&event, &mut ground_truth, &mut client).await;
                }
            } else {
                // Collect events but only handle heartbeats — ACKs/NACKs stay as
                // Sent, which ground truth treats as pending.
                if let Some(ServerEvent::Heartbeat) =
                    client.recv_event(Duration::from_millis(50)).await
                {
                    let _ = client.send_heartbeat_ack().await;
                }
            }
        }
        if strict {
            // Drain any ACKs still queued from the final grace writes so they
            // count toward the must-survive set before we stop trusting.
            let drain_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            while tokio::time::Instant::now() < drain_deadline {
                if let Some(event) = client.recv_event(Duration::from_millis(100)).await {
                    handle_event(&event, &mut ground_truth, &mut client).await;
                }
            }
        }
        info!(
            "Grace period sent {} writes ({})",
            grace_ops,
            if strict {
                "ACKs trusted"
            } else {
                "all treated as pending"
            }
        );

        let pending = ground_truth.mark_all_sent_as_pending();
        let (committed, rejected, sent) = ground_truth.stats();
        info!(
            "Pre-kill state: {} committed, {} rejected, {} pending (sent={})",
            committed, rejected, pending, sent
        );

        // Step 7c: Pre-kill verification — verify committed data is correct
        // while the server is still running (before any kill/restart).
        info!("Pre-kill verification...");
        let pre_kill_result = verify::verify_before_kill(
            &mut client,
            &ground_truth,
            1, // Use existing client_id 1
            &config.data_dir,
            &config.project_id,
            &config.database_id,
        )
        .await;
        let pre_kill_violations = pre_kill_result.violation_count();
        let pre_kill_paths_checked = pre_kill_result.paths_checked;

        // Step 8: Kill strategy
        let kill_strategy = pick_kill_strategy(
            &mut rng,
            &config.data_dir,
            &config.project_id,
            &config.database_id,
        );
        info!("Kill strategy: {}", kill_strategy);

        // Record kill time for violation timing analysis
        let kill_time = Instant::now();

        // Drop the client connection before killing
        drop(client);

        match kill_strategy.as_str() {
            "server-only" => {
                server.kill().await?;
            }
            "during-compaction" => {
                // Wait for compaction to likely be in progress, then kill.
                // Compaction runs in-process; we detect it by watching the
                // sequence file for changes.
                let db_dir = config
                    .data_dir
                    .join(&config.project_id)
                    .join(&config.database_id);
                let seq_path = db_dir.join("sequence");

                let initial_seq = tokio::fs::read_to_string(&seq_path)
                    .await
                    .unwrap_or_default()
                    .trim()
                    .parse::<i64>()
                    .unwrap_or(0);

                let poll_start = Instant::now();
                let mut caught_compaction = false;
                while poll_start.elapsed() < Duration::from_secs(30) {
                    let current_seq = tokio::fs::read_to_string(&seq_path)
                        .await
                        .unwrap_or_default()
                        .trim()
                        .parse::<i64>()
                        .unwrap_or(0);
                    if current_seq != initial_seq {
                        info!(
                            "Compaction detected (seq {} -> {}) — killing now!",
                            initial_seq, current_seq
                        );
                        caught_compaction = true;
                        break;
                    }
                    sleep(Duration::from_millis(50)).await;
                }
                if !caught_compaction {
                    info!("No compaction detected within 30s, killing anyway");
                }
                server.kill().await?;
            }
            "kill-during-replay" => {
                // Kill, restart briefly, then kill AGAIN during WAL replay.
                // Tests that WAL replay is idempotent — crashing mid-replay shouldn't lose data.
                server.kill().await?;

                sleep(Duration::from_millis(500)).await;

                info!("kill-during-replay: restarting server for first replay attempt...");
                server.start().await?;

                // Wait just long enough for the process to begin WAL replay, but NOT
                // long enough for it to finish and start accepting connections.
                let replay_kill_delay = Duration::from_millis(rng.gen_range(100..=500));
                info!(
                    "kill-during-replay: waiting {:?} then killing during replay...",
                    replay_kill_delay
                );
                sleep(replay_kill_delay).await;

                // Kill again mid-replay
                server.kill().await?;
                info!("kill-during-replay: killed server during replay, restarting properly...");
            }
            _ => unreachable!(),
        }

        // Step 9: Wait before restarting
        let restart_delay = Duration::from_secs(rng.gen_range(1..=3));
        info!("Waiting {:?} before restart", restart_delay);
        sleep(restart_delay).await;

        // Step 9b: Run compaction (force root + threshold segments) before restart.
        // Post-restart verification validates the compacted blob + segments.
        let compact_result = compaction::run_compaction(
            &config.compact_bin,
            &config.data_dir,
            &config.project_id,
            &config.database_id,
        );
        if let Some(ref err) = compact_result.error {
            error!("COMPACTION FAILED: {}", err);
        }

        // Step 10: Restart server
        if !server.is_running() {
            server.start().await?;
        }

        // Wait for server to come back
        if let Err(e) = wait_for_server(&server_addr, Duration::from_secs(30)).await {
            error!("Server failed to restart: {}", e);
            server.kill().await?;
            continue;
        }

        // Step 11: Reconnect and verify
        let mut verify_client = match ProxyClient::connect(&server_addr).await {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to reconnect for verification: {}", e);
                server.kill().await?;
                continue;
            }
        };

        // Connect a verification client
        let verify_client_id = 100;
        if let Err(e) = verify_client
            .connect_client(
                verify_client_id,
                &config.project_id,
                &config.database_id,
                "chaos-verify",
            )
            .await
        {
            error!("Failed to connect verification client: {}", e);
            server.kill().await?;
            continue;
        }

        // Handle CONFIG_REQUEST for verification
        let verify_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let mut verify_db_loaded = false;
        while tokio::time::Instant::now() < verify_deadline && !verify_db_loaded {
            match verify_client.recv_event(Duration::from_millis(500)).await {
                Some(ServerEvent::DatabaseLoaded {
                    project_id,
                    database_id,
                }) => {
                    if database_id.is_empty() {
                        let rules = chaos_rules_json(config.rules_mode);
                        let _ = verify_client
                            .push_config_with_rules(&project_id, rules)
                            .await;
                    } else {
                        verify_db_loaded = true;
                    }
                }
                Some(ServerEvent::Heartbeat) => {
                    let _ = verify_client.send_heartbeat_ack().await;
                }
                Some(_) => {}
                None => {}
            }
        }

        // Give the database time to fully load and replay WAL
        sleep(Duration::from_secs(2)).await;

        let verification = verify::verify_after_restart(
            &mut verify_client,
            &ground_truth,
            &config.data_dir,
            &config.project_id,
            &config.database_id,
            verify_client_id,
            kill_time,
        )
        .await;

        // Step 12: Record cycle results
        let cycle_report = CycleReport {
            cycle_number,
            kill_strategy: kill_strategy.clone(),
            operations_sent: ops_sent,
            committed,
            rejected,
            pending,
            pre_kill_violations,
            pre_kill_paths_checked,
            compaction: compact_result,
            verification,
            duration: cycle_start.elapsed(),
        };
        report.record_cycle(cycle_report);

        // Cleanup: kill server for a clean next cycle
        drop(verify_client);
        server.kill().await?;

        // Every 10 cycles, wipe the data directory to prevent unbounded disk growth.
        // This is safe: each cycle has its own ground truth and doesn't depend on prior data.
        // It also exercises cold-start (empty data dir) regularly.
        if cycle_number % 10 == 0 {
            info!(
                "Wiping data directory to prevent disk growth (cycle {})",
                cycle_number
            );
            if let Err(e) = tokio::fs::remove_dir_all(&config.data_dir).await {
                warn!("Failed to wipe data dir: {} (may not exist yet)", e);
            }
            tokio::fs::create_dir_all(&config.data_dir).await?;
        }

        // Brief pause between cycles
        sleep(Duration::from_secs(1)).await;
    }

    // Final report
    report.print_summary();

    if report.has_violations() {
        std::process::exit(1);
    }

    Ok(())
}

/// Handle a server event: update ground truth for ACK/NACK, respond to heartbeats.
async fn handle_event(
    event: &ServerEvent,
    ground_truth: &mut GroundTruth,
    client: &mut ProxyClient,
) {
    match event {
        ServerEvent::Ack { request_id, .. } => {
            ground_truth.mark_committed(request_id);
        }
        ServerEvent::Nack {
            request_id, error, ..
        } => {
            ground_truth.mark_rejected(request_id, error);
        }
        ServerEvent::Heartbeat => {
            let _ = client.send_heartbeat_ack().await;
        }
        _ => {}
    }
}

/// Pick a random kill strategy.
fn pick_kill_strategy<R: Rng>(
    rng: &mut R,
    _data_dir: &std::path::Path,
    _project_id: &str,
    _database_id: &str,
) -> String {
    let roll: u32 = rng.gen_range(0..100);
    match roll {
        0..=39 => "server-only".to_string(),
        40..=69 => "during-compaction".to_string(),
        70..=99 => "kill-during-replay".to_string(),
        _ => unreachable!(),
    }
}
