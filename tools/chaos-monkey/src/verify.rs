//! Post-restart verification: read back data via the server and compare
//! against ground truth, plus inspect on-disk state.

use crate::disk;
#[allow(unused_imports)]
use crate::ground_truth::AncestorOp;
use crate::ground_truth::{GroundTruth, LeafEffect, WriteState};
use crate::protocol::client::{ProxyClient, ServerEvent};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;
use tokio::time::Duration;
use tracing::{error, info, warn};

/// Info about a single data violation.
#[derive(Debug)]
pub struct ViolationInfo {
    pub path: String,
    pub expected_type: String,
    pub actual_type: String,
    /// How many seconds before the kill the ACK was received (None if unknown).
    pub ack_age_secs: Option<f64>,
}

/// Verification results for a single chaos cycle.
#[derive(Debug, Default)]
pub struct VerificationResult {
    /// Committed writes that were missing after restart (VIOLATIONS)
    pub missing_committed: Vec<ViolationInfo>,
    /// Once reads that returned wrong values (VIOLATIONS)
    pub wrong_values: Vec<ViolationInfo>,
    /// Pending writes that survived (acceptable)
    pub surviving_pending: usize,
    /// Pending writes that were lost (acceptable)
    pub lost_pending: usize,
    /// Disk inspection results
    pub disk: disk::DiskInspection,
    /// Total paths verified
    pub paths_checked: usize,
}

impl VerificationResult {
    pub fn has_violations(&self) -> bool {
        !self.missing_committed.is_empty()
            || !self.wrong_values.is_empty()
            || self.disk.has_violations()
    }

    pub fn violation_count(&self) -> usize {
        self.missing_committed.len()
            + self.wrong_values.len()
            + self.disk.wal_errors.len()
            + if self.disk.blob_error.is_some() { 1 } else { 0 }
            + if self.disk.sequence_error.is_some() {
                1
            } else {
                0
            }
    }
}

/// Verify data integrity BEFORE killing the server.
///
/// Does the same ONCE reads as post-restart verification, but while the server
/// is still running. If any committed paths return wrong values here, the problem
/// is not related to restart/promotion — it's a live server bug.
pub async fn verify_before_kill(
    client: &mut ProxyClient,
    ground_truth: &GroundTruth,
    verify_client_id: u32,
    data_dir: &Path,
    project_id: &str,
    database_id: &str,
) -> VerificationResult {
    let mut result = VerificationResult::default();

    let verification_paths = ground_truth.get_verification_paths_with_timing();
    info!(
        "PRE-KILL: Verifying {} committed paths via ONCE reads",
        verification_paths.len()
    );

    let batch_size = 50;
    let mut checked = 0;
    let mut pending_reads: HashMap<String, (String, Value)> = HashMap::new();
    // Lazy-opened fresh BlobSession used to cross-check what the blob on disk
    // actually has at violating paths.
    let mut blob_probe: Option<BlobProbe> = None;
    // Violations within this verify call — we'll probe their parent paths once
    // the main batch loop completes (a parent ONCE read could trigger promotion
    // that masks the bug, so we save it for the very end).
    let mut violations_to_probe_parent: Vec<(String, Value)> = Vec::new();

    for chunk in verification_paths.chunks(batch_size) {
        pending_reads.clear();

        for (path, expected_value, _committed_at) in chunk {
            let req_id = client.next_request_id();
            if let Err(e) = client.send_once(verify_client_id, path, &req_id).await {
                warn!("PRE-KILL: Failed to send ONCE for {}: {}", path, e);
                continue;
            }
            pending_reads.insert(req_id, (path.clone(), expected_value.clone()));
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !pending_reads.is_empty() && tokio::time::Instant::now() < deadline {
            match client.recv_event(Duration::from_millis(500)).await {
                Some(ServerEvent::Once {
                    request_id, value, ..
                }) => {
                    if let Some((path, expected)) = pending_reads.remove(&request_id) {
                        checked += 1;
                        if !values_match(&expected, &value) {
                            error!(
                                "PRE-KILL VIOLATION: {} — expected {}, got {}",
                                path,
                                value_type_summary(&expected),
                                value_type_summary(&value),
                            );
                            log_writes_affecting(ground_truth, &path);
                            // Cross-check the blob on disk: open a fresh session
                            // (no in-process cache) and see what's actually there.
                            if blob_probe.is_none() {
                                blob_probe =
                                    Some(BlobProbe::open(data_dir, project_id, database_id).await);
                            }
                            if let Some(probe) = blob_probe.as_mut() {
                                probe.report(&path, &expected).await;
                            }
                            violations_to_probe_parent.push((path.clone(), expected.clone()));
                            result.wrong_values.push(ViolationInfo {
                                path,
                                expected_type: value_type_summary(&expected),
                                actual_type: value_type_summary(&value),
                                ack_age_secs: None,
                            });
                        }
                    }
                }
                Some(ServerEvent::Nack {
                    request_id, error, ..
                }) => {
                    // The server rejected the read outright (e.g. an invalid or
                    // over-deep path). Surface it as a distinct violation rather
                    // than letting the pending entry fall through to a 10s
                    // "timeout" — a NACK on a supposedly-committed path is a real
                    // signal, and a wrong one masquerading as a timeout is worse.
                    if let Some((path, expected)) = pending_reads.remove(&request_id) {
                        warn!("PRE-KILL: ONCE read for {} was NACKed: {}", path, error);
                        result.missing_committed.push(ViolationInfo {
                            path,
                            expected_type: value_type_summary(&expected),
                            actual_type: format!("NACK: {error}"),
                            ack_age_secs: None,
                        });
                    }
                }
                Some(ServerEvent::Heartbeat) => {
                    let _ = client.send_heartbeat_ack().await;
                }
                Some(ServerEvent::Disconnected) => {
                    error!("PRE-KILL: Server disconnected during verification!");
                    break;
                }
                Some(_) => {}
                None => {}
            }
        }

        for (_, (path, expected)) in pending_reads.drain() {
            warn!("PRE-KILL: ONCE read timed out for {}", path);
            result.missing_committed.push(ViolationInfo {
                path,
                expected_type: value_type_summary(&expected),
                actual_type: "Timeout".to_string(),
                ack_age_secs: None,
            });
        }
    }

    result.paths_checked = checked;

    // Probe parent paths for violations: if `/alice/active` returned Null but the
    // blob has `Number(7600)`, what's `/alice` look like to the live server?
    // - Object missing the child key → `promote_path_deep`'s parent-Object
    //   short-circuit fired and stamped a Null marker on a path the blob has data for.
    // - Sentinel → in-memory should have promoted but didn't — sentinel-tracking bug.
    // - Primitive → `/alice` itself was clobbered to a primitive.
    if !violations_to_probe_parent.is_empty() {
        info!(
            "PRE-KILL: probing parent path for {} violation(s) to characterize tree state",
            violations_to_probe_parent.len()
        );
        for (path, expected) in &violations_to_probe_parent {
            let parent = parent_path_str(path);
            if parent.is_empty() {
                continue;
            }
            let req_id = client.next_request_id();
            if let Err(e) = client.send_once(verify_client_id, &parent, &req_id).await {
                warn!("PRE-KILL: parent probe send failed for {}: {}", parent, e);
                continue;
            }
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            let mut got = None;
            while tokio::time::Instant::now() < deadline {
                match client.recv_event(Duration::from_millis(200)).await {
                    Some(ServerEvent::Once {
                        request_id, value, ..
                    }) if request_id == req_id => {
                        got = Some(value);
                        break;
                    }
                    Some(ServerEvent::Heartbeat) => {
                        let _ = client.send_heartbeat_ack().await;
                    }
                    Some(ServerEvent::Disconnected) => break,
                    _ => {}
                }
            }
            match got {
                Some(value) => {
                    let leaf = path.rsplit('/').next().unwrap_or("");
                    let parent_has_leaf = match &value {
                        Value::Object(o) => o.contains_key(leaf),
                        _ => false,
                    };
                    error!(
                        "    PARENT PROBE: ONCE {} -> {} (leaf '{}' present in parent: {}); violating path {} expected {}",
                        parent,
                        value_type_summary(&value),
                        leaf,
                        parent_has_leaf,
                        path,
                        value_type_summary(expected),
                    );
                    if let Value::Object(o) = &value {
                        let keys: Vec<&str> = o.keys().map(|k| k.as_str()).collect();
                        let preview = if keys.len() > 20 {
                            format!("[{} keys, first 20: {:?}]", keys.len(), &keys[..20])
                        } else {
                            format!("{:?}", keys)
                        };
                        error!("    PARENT PROBE: parent keys: {}", preview);
                    }
                }
                None => {
                    warn!("    PARENT PROBE: ONCE {} timed out", parent);
                }
            }
        }
    }

    let violation_count = result.missing_committed.len() + result.wrong_values.len();
    if violation_count > 0 {
        error!(
            "PRE-KILL: {}/{} paths verified, {} VIOLATIONS (data wrong while server still running!)",
            checked,
            verification_paths.len(),
            violation_count,
        );
    } else {
        info!(
            "PRE-KILL: All {}/{} committed paths verified OK",
            checked,
            verification_paths.len(),
        );
    }

    result
}

/// Strip the last segment of a path. `/a/b/c` -> `/a/b`. `/a` -> `/`. `/` -> "".
fn parent_path_str(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    match trimmed.rfind('/') {
        Some(0) => "/".to_string(),
        Some(idx) => trimmed[..idx].to_string(),
        None => "".to_string(),
    }
}

/// Verify data integrity after a crash/restart cycle.
///
/// Steps:
/// 1. Connect to the restarted server
/// 2. For each committed path: send ONCE, compare value
/// 3. Inspect disk state
pub async fn verify_after_restart(
    client: &mut ProxyClient,
    ground_truth: &GroundTruth,
    data_dir: &Path,
    project_id: &str,
    database_id: &str,
    verify_client_id: u32,
    kill_time: Instant,
) -> VerificationResult {
    let mut result = VerificationResult::default();

    // Step 1: Verify committed writes via ONCE reads
    let verification_paths = ground_truth.get_verification_paths_with_timing();
    info!(
        "Verifying {} committed paths via ONCE reads",
        verification_paths.len()
    );

    // We need to verify in batches to avoid overwhelming the server
    let batch_size = 50;
    let mut checked = 0;
    // req_id -> (path, expected_value, committed_at)
    let mut pending_reads: HashMap<String, (String, Value, Option<Instant>)> = HashMap::new();

    for chunk in verification_paths.chunks(batch_size) {
        pending_reads.clear();

        // Send ONCE reads for this batch
        for (path, expected_value, committed_at) in chunk {
            let req_id = client.next_request_id();
            if let Err(e) = client.send_once(verify_client_id, path, &req_id).await {
                warn!("Failed to send ONCE for {}: {}", path, e);
                continue;
            }
            pending_reads.insert(
                req_id,
                (path.clone(), expected_value.clone(), *committed_at),
            );
        }

        // Collect responses with timeout
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !pending_reads.is_empty() && tokio::time::Instant::now() < deadline {
            match client.recv_event(Duration::from_millis(500)).await {
                Some(ServerEvent::Once {
                    request_id, value, ..
                }) => {
                    if let Some((path, expected, committed_at)) = pending_reads.remove(&request_id)
                    {
                        checked += 1;
                        if !values_match(&expected, &value) {
                            let ack_age_secs =
                                committed_at.map(|t| kill_time.duration_since(t).as_secs_f64());
                            error!(
                                "VIOLATION: {} — expected {}, got {} (ACK {:.1}s before kill)",
                                path,
                                value_type_summary(&expected),
                                value_type_summary(&value),
                                ack_age_secs.unwrap_or(-1.0),
                            );
                            result.wrong_values.push(ViolationInfo {
                                path,
                                expected_type: value_type_summary(&expected),
                                actual_type: value_type_summary(&value),
                                ack_age_secs,
                            });
                        }
                    }
                }
                Some(ServerEvent::Heartbeat) => {
                    let _ = client.send_heartbeat_ack().await;
                }
                Some(ServerEvent::Disconnected) => {
                    error!("Server disconnected during verification!");
                    break;
                }
                Some(_) => {} // Ignore other events
                None => {}    // Timeout, keep trying
            }
        }

        // Any remaining reads that timed out
        for (_, (path, expected, committed_at)) in pending_reads.drain() {
            let ack_age_secs = committed_at.map(|t| kill_time.duration_since(t).as_secs_f64());
            warn!(
                "ONCE read timed out for committed path: {} (ACK {:.1}s before kill)",
                path,
                ack_age_secs.unwrap_or(-1.0),
            );
            result.missing_committed.push(ViolationInfo {
                path,
                expected_type: value_type_summary(&expected),
                actual_type: "Timeout".to_string(),
                ack_age_secs,
            });
        }
    }

    result.paths_checked = checked;
    let violation_count = result.missing_committed.len() + result.wrong_values.len();
    info!(
        "Verified {}/{} paths ({} violations)",
        checked,
        verification_paths.len(),
        violation_count
    );

    // Step 1b: Diagnose violations by inspecting manifest and segment files on disk
    if violation_count > 0 {
        let all_violations: Vec<&str> = result
            .wrong_values
            .iter()
            .chain(result.missing_committed.iter())
            .map(|v| v.path.as_str())
            .collect();
        let diagnose_count = all_violations.len().min(10);
        info!(
            "Diagnosing {} of {} violations against on-disk state...",
            diagnose_count,
            all_violations.len()
        );
        for path in &all_violations[..diagnose_count] {
            let diag = disk::diagnose_missing_path(data_dir, project_id, database_id, path).await;
            error!("{}", diag);
        }
    }

    // Step 2: Inspect on-disk state
    result.disk = disk::inspect_database(data_dir, project_id, database_id).await;

    if result.disk.has_violations() {
        error!(
            "Disk inspection found violations: {}",
            result.disk.summary()
        );
    } else {
        info!("Disk inspection: {}", result.disk.summary());
    }

    result
}

/// Compare two JSON values for equality.
/// Handles the case where the server may return leaf values directly
/// or as part of a tree structure.
fn values_match(expected: &Value, actual: &Value) -> bool {
    // Null check: if expected is null, actual should also be null
    if expected.is_null() {
        return actual.is_null();
    }

    // Direct comparison
    if expected == actual {
        return true;
    }

    // For numeric comparisons, handle f64 precision
    if let (Some(e), Some(a)) = (expected.as_f64(), actual.as_f64()) {
        return (e - a).abs() < f64::EPSILON;
    }

    false
}

/// Log every primitive-leaf op that ground truth says actually touches `path`,
/// in sequence order. Object values are expanded all the way to their leaves
/// so each line is one concrete `set leaf = primitive` or `clear leaf` —
/// makes it easy to scan whether ground truth's expected value is justified.
///
/// Also lists "promote triggers" — ops whose path is at or above the target
/// but whose leaves don't directly touch target's line. These don't write
/// the target but they call `promote_path` / `handle_update` on an ancestor,
/// which can clobber in-memory state for descendants without leaving a
/// leaf-level fingerprint.
fn log_writes_affecting(ground_truth: &GroundTruth, path: &str) {
    let ops = ground_truth.ops_affecting(path);
    if ops.is_empty() {
        warn!("  (no writes touched this path according to ground truth)");
    } else {
        error!("  {} primitive-leaf ops touched this path:", ops.len());
        for op in &ops {
            let state = match &op.state {
                WriteState::Committed => "COMMITTED",
                WriteState::Sent => "PENDING",
                WriteState::Rejected(_) => "REJECTED",
            };
            let leaf = match &op.leaf {
                LeafEffect::Set { path, value } => {
                    format!("set {} = {}", path, value_type_summary(value))
                }
                LeafEffect::Clear { path } => format!("clear {}", path),
            };
            error!(
                "    seq={} {} {} (via {})",
                op.sequence, state, leaf, op.op_kind
            );
        }
    }

    let triggers = ground_truth.promote_triggers_for(path);
    if !triggers.is_empty() {
        error!(
            "  {} promote triggers (ops at/above target whose leaves don't touch target):",
            triggers.len()
        );
        for t in &triggers {
            let state = match &t.state {
                WriteState::Committed => "COMMITTED",
                WriteState::Sent => "PENDING",
                WriteState::Rejected(_) => "REJECTED",
            };
            error!(
                "    seq={} {} {} at {} (via {})",
                t.sequence, state, t.op_kind, t.path, t.op_kind
            );
        }
    }
}

/// Opens a fresh `BlobSession` against the on-disk `blob.lark` so we can
/// cross-check what the blob actually contains at a violating path. A fresh
/// session shares no in-process cache with the live server — if it returns
/// a value the live server didn't, the bug is on the read path
/// (stale CachedIO, wrong promote target, etc.). If it also returns Null,
/// the blob itself is missing data and the bug is in compaction's blob writes.
struct BlobProbe {
    session: Option<lark_blob::BlobSession<lark_blob::StdBlobIO>>,
    open_error: Option<String>,
    blob_path: std::path::PathBuf,
}

impl BlobProbe {
    async fn open(data_dir: &Path, project_id: &str, database_id: &str) -> Self {
        let blob_path = data_dir
            .join(project_id)
            .join(database_id)
            .join("blob.lark");
        if !blob_path.exists() {
            return BlobProbe {
                session: None,
                open_error: Some(format!("blob file does not exist: {}", blob_path.display())),
                blob_path,
            };
        }
        match lark_blob::StdBlobIO::open(&blob_path) {
            Ok(io) => match lark_blob::BlobSession::open(io).await {
                Ok(session) => BlobProbe {
                    session: Some(session),
                    open_error: None,
                    blob_path,
                },
                Err(e) => BlobProbe {
                    session: None,
                    open_error: Some(format!("BlobSession::open failed: {}", e)),
                    blob_path,
                },
            },
            Err(e) => BlobProbe {
                session: None,
                open_error: Some(format!("StdBlobIO::open failed: {}", e)),
                blob_path,
            },
        }
    }

    async fn report(&mut self, path: &str, expected: &Value) {
        if let Some(err) = &self.open_error {
            error!("    BLOB PROBE: cannot read fresh blob session: {}", err);
            return;
        }
        let Some(session) = self.session.as_ref() else {
            return;
        };
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        match session.read_subtree(&segments).await {
            Ok(arc_value) => {
                let blob_json = arc_value_to_json(&arc_value);
                let matches = values_match(expected, &blob_json);
                error!(
                    "    BLOB PROBE: fresh BlobSession.read_subtree({:?}) -> {} {} expected {}",
                    segments,
                    value_type_summary(&blob_json),
                    if matches { "MATCHES" } else { "DOES NOT MATCH" },
                    value_type_summary(expected),
                );
                if !matches {
                    if let Ok(s) = serde_json::to_string(&blob_json) {
                        let truncated = if s.len() > 200 {
                            format!("{}...", &s[..200])
                        } else {
                            s
                        };
                        error!("    BLOB PROBE: blob value: {}", truncated);
                    }
                    if let Ok(s) = serde_json::to_string(expected) {
                        let truncated = if s.len() > 200 {
                            format!("{}...", &s[..200])
                        } else {
                            s
                        };
                        error!("    BLOB PROBE: expected value: {}", truncated);
                    }
                }
            }
            Err(e) => {
                error!(
                    "    BLOB PROBE: read_subtree({:?}) failed: {} (blob={})",
                    segments,
                    e,
                    self.blob_path.display()
                );
            }
        }
    }
}

/// Convert an `ArcValue` (lark-blob) to a `serde_json::Value` so we can compare
/// against ground truth's `Value` representation. Sentinels surface as Null.
fn arc_value_to_json(v: &lark_blob::ArcValue) -> Value {
    match v {
        lark_blob::ArcValue::Null => Value::Null,
        lark_blob::ArcValue::Bool(b) => Value::Bool(*b),
        lark_blob::ArcValue::Number(n) => Value::Number(n.clone()),
        lark_blob::ArcValue::String(s) => Value::String(s.to_string()),
        lark_blob::ArcValue::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, child) in map.iter() {
                out.insert(k.to_string(), arc_value_to_json(child));
            }
            Value::Object(out)
        }
        lark_blob::ArcValue::Sentinel(_) => Value::Null,
    }
}

/// Produce a short type summary of a JSON value for logging.
/// e.g. "Null", "String(len=42)", "Number(3.14)", "Bool(true)", "Object(5 keys)", "Array(3 items)"
fn value_type_summary(v: &Value) -> String {
    match v {
        Value::Null => "Null".to_string(),
        Value::Bool(b) => format!("Bool({})", b),
        Value::Number(n) => format!("Number({})", n),
        Value::String(s) => {
            if s.len() > 40 {
                format!("String(len={})", s.len())
            } else {
                format!("String(\"{}\")", s)
            }
        }
        Value::Array(a) => format!("Array({} items)", a.len()),
        Value::Object(o) => format!("Object({} keys)", o.len()),
    }
}
