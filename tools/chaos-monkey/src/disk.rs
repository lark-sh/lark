//! Disk inspection: validate on-disk state after crashes.
//!
//! Checks:
//! - blob.{N}.lark files exist and are non-empty
//! - sequence file is valid (if present)
//! - WAL files are valid JSONL

use std::path::{Path, PathBuf};
use tracing::debug;

/// Results of disk inspection.
#[derive(Debug, Default)]
pub struct DiskInspection {
    pub blob_ok: bool,
    pub blob_error: Option<String>,
    pub sequence_ok: bool,
    pub sequence_error: Option<String>,
    pub wal_files_checked: usize,
    pub wal_errors: Vec<String>,
}

impl DiskInspection {
    pub fn has_violations(&self) -> bool {
        self.blob_error.is_some() || self.sequence_error.is_some() || !self.wal_errors.is_empty()
    }

    pub fn summary(&self) -> String {
        format!(
            "blob={} sequence={} wal={}/{} ok",
            if self.blob_ok { "OK" } else { "FAIL" },
            if self.sequence_ok { "OK" } else { "FAIL" },
            self.wal_files_checked - self.wal_errors.len(),
            self.wal_files_checked,
        )
    }
}

/// Inspect the on-disk state of a database.
pub async fn inspect_database(
    data_dir: &Path,
    project_id: &str,
    database_id: &str,
) -> DiskInspection {
    let db_dir = data_dir.join(project_id).join(database_id);
    let mut result = DiskInspection::default();

    if !db_dir.exists() {
        // Directory doesn't exist yet — no data has been persisted.
        // This is normal if the server was killed before its first WAL sync.
        debug!("Database directory does not exist yet (pre-persistence)");
        result.blob_ok = true;
        result.sequence_ok = true;
        return result;
    }

    // Check blob file(s)
    check_blob_files(&db_dir, &mut result).await;

    // Check sequence file
    check_sequence_file(&db_dir, &mut result).await;

    // Check WAL files
    check_wal_files(&db_dir, &mut result).await;

    debug!("Disk inspection: {}", result.summary());
    result
}

/// Check that blob.lark exists and is non-empty.
async fn check_blob_files(db_dir: &Path, result: &mut DiskInspection) {
    let blob_files = find_blob_files(db_dir).await;

    if blob_files.is_empty() {
        // No blob file yet — this is normal if the server hasn't persisted anything
        debug!("No blob files found (pre-persistence)");
        result.blob_ok = true;
        return;
    }

    // Check the highest-numbered blob file (that's the active one)
    let active_blob = &blob_files[blob_files.len() - 1];
    match tokio::fs::metadata(active_blob).await {
        Ok(meta) => {
            if meta.len() == 0 {
                result.blob_error = Some(format!(
                    "blob file {} is empty (0 bytes)",
                    active_blob.display()
                ));
            } else {
                result.blob_ok = true;
                debug!(
                    "blob file {} OK ({} bytes)",
                    active_blob.display(),
                    meta.len()
                );
            }
        }
        Err(e) => {
            result.blob_error = Some(format!(
                "blob file {} read error: {}",
                active_blob.display(),
                e
            ));
        }
    }
}

/// Check the sequence file (if present).
async fn check_sequence_file(db_dir: &Path, result: &mut DiskInspection) {
    let sequence_path = db_dir.join("sequence");
    if !sequence_path.exists() {
        // No sequence file — this is fine, means no compaction has run yet
        debug!("No sequence file found (pre-compaction)");
        result.sequence_ok = true;
        return;
    }

    match tokio::fs::read_to_string(&sequence_path).await {
        Ok(content) => {
            let trimmed = content.trim();
            if trimmed.is_empty() {
                result.sequence_error = Some("sequence file is empty".to_string());
            } else {
                match trimmed.parse::<i64>() {
                    Ok(seq) => {
                        result.sequence_ok = true;
                        debug!("sequence file OK (value={})", seq);
                    }
                    Err(e) => {
                        result.sequence_error = Some(format!(
                            "sequence file parse error: {} (content: {:?})",
                            e,
                            truncate(trimmed, 50)
                        ));
                    }
                }
            }
        }
        Err(e) => {
            result.sequence_error = Some(format!("sequence file read error: {}", e));
        }
    }
}

async fn check_wal_files(db_dir: &Path, result: &mut DiskInspection) {
    let wal_dir = db_dir.join("wal");
    if !wal_dir.exists() {
        debug!("No WAL directory found");
        return;
    }

    let mut entries = match tokio::fs::read_dir(&wal_dir).await {
        Ok(e) => e,
        Err(e) => {
            result
                .wal_errors
                .push(format!("Failed to read wal/: {}", e));
            return;
        }
    };

    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "wal") {
            result.wal_files_checked += 1;
            match validate_wal_file(&path).await {
                Ok(lines) => {
                    debug!("WAL file {} OK ({} lines)", path.display(), lines);
                }
                Err(e) => {
                    result.wal_errors.push(format!("{}: {}", path.display(), e));
                }
            }
        }
    }
}

/// Validate a WAL file: each line must be valid JSON.
async fn validate_wal_file(path: &Path) -> Result<usize, String> {
    let content = tokio::fs::read_to_string(path)
        .await
        .map_err(|e| format!("read error: {}", e))?;

    let mut line_count = 0;
    for (i, line) in content.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if serde_json::from_str::<serde_json::Value>(trimmed).is_err() {
            return Err(format!(
                "invalid JSON at line {}: {}",
                i + 1,
                truncate(trimmed, 100)
            ));
        }
        line_count += 1;
    }
    Ok(line_count)
}

/// Find all blob.{N}.lark files in a directory, sorted by sequence number.
/// Return vec with blob.lark path if it exists, empty vec otherwise.
async fn find_blob_files(db_dir: &Path) -> Vec<PathBuf> {
    let bp = db_dir.join("blob.lark");
    if tokio::fs::metadata(&bp).await.is_ok() {
        vec![bp]
    } else {
        Vec::new()
    }
}

/// Diagnose a specific path that returned Null after restart.
/// Checks blob files, sequence file, and WAL files.
pub async fn diagnose_missing_path(
    data_dir: &Path,
    project_id: &str,
    database_id: &str,
    path: &str,
) -> String {
    let db_dir = data_dir.join(project_id).join(database_id);
    let mut lines = Vec::new();

    // Derive the item path (e.g., /burst/item-85 from /burst/item-85/data)
    let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
    let item_path = if parts.len() >= 2 {
        format!("/{}/{}", parts[0], parts[1])
    } else {
        path.to_string()
    };

    // 1. Check blob files
    let blob_files = find_blob_files(&db_dir).await;
    if blob_files.is_empty() {
        lines.push(format!("  DIAG {}: no blob files found", path));
    } else {
        let active = &blob_files[blob_files.len() - 1];
        let size = tokio::fs::metadata(active)
            .await
            .map(|m| m.len())
            .unwrap_or(0);
        lines.push(format!(
            "  DIAG {}: blob={} ({} bytes), {} blob file(s) total",
            path,
            active.file_name().and_then(|n| n.to_str()).unwrap_or("?"),
            size,
            blob_files.len(),
        ));
    }

    // 2. Check sequence file
    let sequence_path = db_dir.join("sequence");
    let blob_sequence = if sequence_path.exists() {
        match tokio::fs::read_to_string(&sequence_path).await {
            Ok(content) => {
                let seq = content.trim().parse::<i64>().unwrap_or(-1);
                lines.push(format!("    sequence file: {}", seq));
                seq
            }
            Err(e) => {
                lines.push(format!("    sequence file read error: {}", e));
                -1
            }
        }
    } else {
        lines.push("    no sequence file (pre-compaction)".to_string());
        0
    };

    // 3. Scan WAL files for entries touching this path
    let wal_dir = db_dir.join("wal");
    if wal_dir.exists() {
        let wal_hits = scan_wal_for_path(&wal_dir, &item_path).await;
        if wal_hits.is_empty() {
            lines.push(format!("    WAL files: no entries for '{}'", item_path));
        } else {
            lines.push(format!(
                "    WAL files: {} entries for '{}':",
                wal_hits.len(),
                item_path
            ));
            for hit in &wal_hits {
                lines.push(format!("      - {}", hit));
            }
        }

        // Count total WAL files and their sequence range
        let wal_info = summarize_wal_files(&wal_dir).await;
        lines.push(format!(
            "    WAL summary: {} files, seq range {}-{}, blob_sequence={}",
            wal_info.count, wal_info.min_seq, wal_info.max_seq, blob_sequence
        ));
    }

    // 4. BlobSession diagnose_path (walks the blob's nav cache for this path)
    let blob_path = db_dir.join("blob.lark");
    if blob_path.exists() {
        match lark_blob::StdBlobIO::open(&blob_path) {
            Ok(io) => match lark_blob::BlobSession::open(io).await {
                Ok(session) => {
                    let path_segments: Vec<&str> = parts.to_vec();
                    let diag = session.diagnose_path(&path_segments).await;
                    lines.push(format!("    blob diagnose_path:\n{}", diag));
                }
                Err(e) => {
                    lines.push(format!(
                        "    blob diagnose_path: failed to open session: {}",
                        e
                    ));
                }
            },
            Err(e) => {
                lines.push(format!(
                    "    blob diagnose_path: failed to open blob: {}",
                    e
                ));
            }
        }
    }

    lines.join("\n")
}

struct WalSummary {
    count: usize,
    min_seq: i64,
    max_seq: i64,
}

async fn summarize_wal_files(wal_dir: &Path) -> WalSummary {
    let mut summary = WalSummary {
        count: 0,
        min_seq: i64::MAX,
        max_seq: 0,
    };
    let mut entries = match tokio::fs::read_dir(wal_dir).await {
        Ok(e) => e,
        Err(_) => return summary,
    };

    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.ends_with(".wal") {
            summary.count += 1;
            if let Some(seq) = name
                .strip_suffix(".wal")
                .and_then(|s| s.parse::<i64>().ok())
            {
                summary.min_seq = summary.min_seq.min(seq);
                summary.max_seq = summary.max_seq.max(seq);
            }
        }
    }

    if summary.min_seq == i64::MAX {
        summary.min_seq = 0;
    }
    summary
}

/// Scan WAL files for entries that touch the given path (or a parent of it).
async fn scan_wal_for_path(wal_dir: &Path, item_path: &str) -> Vec<String> {
    let mut hits = Vec::new();
    let mut entries = match tokio::fs::read_dir(wal_dir).await {
        Ok(e) => e,
        Err(_) => return hits,
    };

    let mut wal_files = Vec::new();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "wal") {
            wal_files.push(path);
        }
    }
    wal_files.sort();

    for wal_path in &wal_files {
        let content = match tokio::fs::read_to_string(wal_path).await {
            Ok(c) => c,
            Err(_) => continue,
        };
        let filename = wal_path.file_name().and_then(|f| f.to_str()).unwrap_or("?");
        for (i, line) in content.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            if let Ok(entry) = serde_json::from_str::<serde_json::Value>(trimmed) {
                if let Some(p) = entry.get("p").and_then(|p| p.as_str()) {
                    // Check if this WAL entry's path matches or is a parent of our item
                    if p == item_path
                        || item_path.starts_with(&format!("{}/", p))
                        || p.starts_with(&format!("{}/", item_path))
                    {
                        let op = entry.get("o").and_then(|o| o.as_str()).unwrap_or("?");
                        let has_value = entry.get("v").map(|v| !v.is_null()).unwrap_or(false);
                        hits.push(format!(
                            "{}:{} op={} path={} has_value={}",
                            filename,
                            i + 1,
                            op,
                            p,
                            has_value
                        ));
                    }
                }
            }
        }
    }
    hits
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() > max {
        format!("{}...", &s[..max])
    } else {
        s.to_string()
    }
}
