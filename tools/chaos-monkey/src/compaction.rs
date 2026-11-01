//! Compaction step for chaos testing.
//!
//! Between the server kill and restart, runs `lark-compact <db-dir>`
//! to force a full root re-compaction. The post-restart verification
//! then implicitly validates that the compacted blob contains all
//! committed data.

use std::path::Path;
use std::time::Instant;
use tracing::{debug, info};

/// Results from running compaction.
#[derive(Debug, Default)]
pub struct CompactionResult {
    /// Whether lark-compact ran successfully.
    pub compact_ok: bool,
    /// True if there was nothing to compact (no blob file yet).
    pub skipped: bool,
    /// Human-readable error if something went wrong.
    pub error: Option<String>,
    /// How long the compaction took (ms).
    pub elapsed_ms: u64,
}

impl CompactionResult {
    pub fn summary(&self) -> String {
        if self.skipped {
            "compact=skipped".to_string()
        } else if let Some(ref err) = self.error {
            format!("compact=FAIL ({})", err)
        } else {
            format!("compact=OK ({}ms)", self.elapsed_ms)
        }
    }
}

/// Read the blob generation number from `dir/blob.generation`.
fn read_blob_generation(dir: &Path) -> u64 {
    match std::fs::read_to_string(dir.join("blob.generation")) {
        Ok(s) => s.trim().parse::<u64>().unwrap_or(0),
        Err(_) => 0,
    }
}

/// Size of `blob.lark`, the only file full compaction shrinks.
fn blob_size(db_dir: &Path) -> u64 {
    std::fs::metadata(db_dir.join("blob.lark"))
        .map(|m| m.len())
        .unwrap_or(0)
}

/// Run full root re-compaction on the database.
///
/// Runs `lark-compact <db-dir>` and reports success or failure. The caller
/// should restart the server afterwards — the post-restart verification
/// implicitly validates the blob.
pub fn run_compaction(
    compact_bin: &Path,
    data_dir: &Path,
    project_id: &str,
    database_id: &str,
) -> CompactionResult {
    let mut result = CompactionResult::default();
    let start = Instant::now();

    let db_dir = data_dir.join(project_id).join(database_id);

    // Check if database directory exists (might not if killed before first persist)
    if !db_dir.exists() {
        info!("COMPACT: No database directory yet, skipping");
        result.skipped = true;
        result.compact_ok = true;
        result.elapsed_ms = start.elapsed().as_millis() as u64;
        return result;
    }

    // Check if there's a blob to compact
    let blob_path = db_dir.join("blob.lark");
    if !blob_path.exists() {
        info!("COMPACT: No blob file found, skipping");
        result.skipped = true;
        result.compact_ok = true;
        result.elapsed_ms = start.elapsed().as_millis() as u64;
        return result;
    }

    let old_gen = read_blob_generation(&db_dir);
    let old_size = blob_size(&db_dir);

    info!(
        "COMPACT: Running lark-compact (gen {}, blob {:.1} KB)...",
        old_gen,
        old_size as f64 / 1024.0,
    );

    // Run `lark-compact <db-dir>` — the OSS binary takes the database dir
    // as a single positional arg; there's no subcommand to dispatch on.
    let output = std::process::Command::new(compact_bin)
        .arg(db_dir.to_str().unwrap())
        .output();

    result.elapsed_ms = start.elapsed().as_millis() as u64;

    match output {
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !output.status.success() {
                result.error = Some(format!(
                    "lark-compact exited with {}: {}",
                    output.status,
                    stderr.trim()
                ));
                return result;
            }
            debug!("COMPACT: {}", stderr.trim());
            result.compact_ok = true;

            // Report before/after sizes
            let new_gen = read_blob_generation(&db_dir);
            let new_size = blob_size(&db_dir);
            let ratio = if old_size > 0 {
                (new_size as f64 / old_size as f64) * 100.0
            } else {
                0.0
            };
            info!(
                "COMPACT: OK — gen {} -> {}, blob {:.1} KB -> {:.1} KB ({:.0}%), {}ms",
                old_gen,
                new_gen,
                old_size as f64 / 1024.0,
                new_size as f64 / 1024.0,
                ratio,
                result.elapsed_ms,
            );
        }
        Err(e) => {
            result.error = Some(format!("failed to execute lark-compact: {}", e));
        }
    }

    result
}
