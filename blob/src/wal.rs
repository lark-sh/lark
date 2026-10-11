//! Write-ahead log entries and their translation into blob updates.
//!
//! WAL files are JSONL: one [`WalEntry`] per line, named `{sequence:06}.wal`.
//! This module holds the parts of the WAL that don't depend on an I/O runtime
//! (the entry types, the strict line parser, and the conversion of entries
//! into `apply_updates` tuples), so the server's StorageWorker and offline
//! tools that rebuild a blob from WAL files apply exactly the same semantics.

use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::warn;

use crate::ArcValue;

/// WAL operation types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalOp {
    #[serde(rename = "s")]
    Set,
    #[serde(rename = "u")]
    Update,
    #[serde(rename = "d")]
    Delete,
}

/// A single entry in the write-ahead log.
///
/// For SET: stores the value being set.
/// For UPDATE: stores the delta (map of updates).
/// For DELETE: value is None.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    /// Operation type.
    #[serde(rename = "o")]
    pub op: WalOp,

    /// Database path for the operation.
    #[serde(rename = "p")]
    pub path: String,

    /// Value for set/update operations.
    #[serde(rename = "v", skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,

    /// WAL file sequence this entry belongs to (not persisted to disk).
    /// Set by WalReader when loading and by Database when writing.
    #[serde(skip)]
    pub sequence: i64,
}

impl WalEntry {
    /// Create a SET entry.
    pub fn set(path: &str, value: Value) -> Self {
        Self {
            op: WalOp::Set,
            path: path.to_string(),
            value: Some(value),
            sequence: 0,
        }
    }

    /// Create an UPDATE entry.
    pub fn update(path: &str, value: Value) -> Self {
        Self {
            op: WalOp::Update,
            path: path.to_string(),
            value: Some(value),
            sequence: 0,
        }
    }

    /// Create a DELETE entry.
    pub fn delete(path: &str) -> Self {
        Self {
            op: WalOp::Delete,
            path: path.to_string(),
            value: None,
            sequence: 0,
        }
    }
}

/// Parse WAL sequence number from filename (e.g., "000001.wal" -> 1).
pub fn parse_wal_sequence(filename: &str) -> Option<i64> {
    filename
        .strip_suffix(".wal")
        .and_then(|s| s.parse::<i64>().ok())
}

/// Parse WAL entries from lines, with strict corruption detection.
///
/// If `allow_trailing_truncation` is true (only valid for the LAST WAL file
/// during replay), a malformed final line is tolerated — this handles the case
/// where the server crashed mid-write. Any malformed line that is NOT the last
/// line is always a fatal error, because it means data was lost or corrupted
/// in the middle of the file.
///
/// `path` is only used in log and error messages.
pub fn parse_wal_lines(
    content: &str,
    path: &Path,
    allow_trailing_truncation: bool,
) -> io::Result<Vec<WalEntry>> {
    let non_empty_lines: Vec<(usize, &str)> = content
        .lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .collect();

    let mut entries = Vec::new();
    let total = non_empty_lines.len();

    for (idx, (line_num, line)) in non_empty_lines.iter().enumerate() {
        let is_last_line = idx == total - 1;

        match serde_json::from_str::<WalEntry>(line) {
            Ok(entry) => entries.push(entry),
            Err(e) => {
                if is_last_line && allow_trailing_truncation {
                    // Last line of last file — likely truncated on crash, acceptable
                    warn!(
                        "[WAL Reader] Skipping truncated last line in {:?} line {}: {}",
                        path,
                        line_num + 1,
                        e
                    );
                } else {
                    // Corruption in the middle of a file — fatal
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "WAL corruption in {:?} at line {} (not last line): {}. \
                             This indicates data loss — refusing to load. \
                             Manual inspection required.",
                            path,
                            line_num + 1,
                            e
                        ),
                    ));
                }
            }
        }
    }

    Ok(entries)
}

/// Convert WAL entries into blob update tuples.
///
/// SET/DELETE entries map directly. UPDATE entries are expanded into
/// per-child-key SET operations because `apply_updates` does full replacement
/// at a path, but WAL UPDATE entries represent shallow merges.
///
/// All entries are passed through in order — no deduplication. The blob layer
/// handles applying them sequentially so every write is reflected.
pub fn coalesce_wal_entries(entries: Vec<WalEntry>) -> Vec<(Vec<String>, Option<ArcValue>)> {
    let mut result: Vec<(Vec<String>, Option<ArcValue>)> = Vec::new();

    for entry in entries {
        match entry.op {
            WalOp::Set => {
                // A SET whose value cleans to nothing (null, or only null
                // children) is a delete. serde reads {"v": null} as None.
                let segments = split_path(&entry.path);
                let value = entry.value.and_then(ArcValue::from_value_cleaned);
                result.push((segments, value));
            }
            WalOp::Delete => {
                let segments = split_path(&entry.path);
                result.push((segments, None));
            }
            WalOp::Update => {
                // Expand UPDATE into per-child-key SETs; a null child is a
                // delete of that key.
                if let Some(Value::Object(map)) = entry.value {
                    for (key, val) in map {
                        let expanded = format!("{}/{}", entry.path, key);
                        let segments = split_path(&expanded);
                        result.push((segments, ArcValue::from_value_cleaned(val)));
                    }
                }
            }
        }
    }

    result
}

#[inline]
fn split_path(path: &str) -> Vec<String> {
    path.trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_wal_sequence() {
        assert_eq!(parse_wal_sequence("000001.wal"), Some(1));
        assert_eq!(parse_wal_sequence("001284.wal"), Some(1284));
        assert_eq!(parse_wal_sequence("000001.wal_.gstmp"), None);
        assert_eq!(parse_wal_sequence("sequence"), None);
    }

    /// SET with v:null is a delete. serde reads {"v": null} as None.
    #[test]
    fn test_coalesce_set_null_is_delete() {
        let line = r#"{"o":"s","p":"/users/alice","v":null}"#;
        let entry: WalEntry = serde_json::from_str(line).unwrap();
        let updates = coalesce_wal_entries(vec![entry]);

        assert_eq!(
            updates,
            vec![(vec!["users".to_string(), "alice".to_string()], None)]
        );
    }

    /// A null child of an UPDATE deletes that key; nested nulls inside a
    /// written value are dropped rather than stored.
    #[test]
    fn test_coalesce_update_nulls_are_deletes() {
        let entries = vec![WalEntry::update(
            "/",
            json!({
                "characters/c1": null,
                "stats/c2": {"hp": 8, "mp": null},
                "empty": {"x": null}
            }),
        )];
        let mut updates = coalesce_wal_entries(entries);
        updates.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(
            updates,
            vec![
                (vec!["characters".to_string(), "c1".to_string()], None),
                (vec!["empty".to_string()], None),
                (
                    vec!["stats".to_string(), "c2".to_string()],
                    Some(ArcValue::from_value(json!({"hp": 8})))
                ),
            ]
        );
    }

    #[test]
    fn test_coalesce_set() {
        let entries = vec![WalEntry::set("/users/alice", json!({"name": "Alice"}))];
        let updates = coalesce_wal_entries(entries);
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].0, vec!["users", "alice"]);
        assert!(updates[0].1.is_some());
    }

    #[test]
    fn test_coalesce_delete() {
        let entries = vec![WalEntry::delete("/users/alice")];
        let updates = coalesce_wal_entries(entries);
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].0, vec!["users", "alice"]);
        assert!(updates[0].1.is_none());
    }

    #[test]
    fn test_coalesce_update_expands() {
        let entries = vec![WalEntry::update(
            "/users/alice",
            json!({"score": 100, "badge": "gold"}),
        )];
        let updates = coalesce_wal_entries(entries);
        assert_eq!(updates.len(), 2);
        let paths: Vec<Vec<String>> = updates.iter().map(|(p, _)| p.clone()).collect();
        assert!(paths.contains(&vec![
            "users".to_string(),
            "alice".to_string(),
            "badge".to_string()
        ]));
        assert!(paths.contains(&vec![
            "users".to_string(),
            "alice".to_string(),
            "score".to_string()
        ]));
    }

    #[test]
    fn test_coalesce_root_path() {
        let entries = vec![WalEntry::set("/", json!({"a": 1}))];
        let updates = coalesce_wal_entries(entries);
        assert_eq!(updates.len(), 1);
        assert!(updates[0].0.is_empty());
    }

    #[test]
    fn test_coalesce_mixed() {
        let entries = vec![
            WalEntry::set("/a", json!(1)),
            WalEntry::update("/b", json!({"x": 2, "y": 3})),
            WalEntry::delete("/c"),
        ];
        let updates = coalesce_wal_entries(entries);
        assert_eq!(updates.len(), 4);
    }

    #[test]
    fn test_coalesce_no_duplicates() {
        let entries = vec![WalEntry::set("/a", json!(1)), WalEntry::set("/b", json!(2))];
        let coalesced = coalesce_wal_entries(entries);
        assert_eq!(coalesced.len(), 2);
    }

    #[test]
    fn test_coalesce_same_path_passes_all() {
        let entries = vec![
            WalEntry::set("/a", json!(1)),
            WalEntry::set("/a", json!(2)),
            WalEntry::set("/a", json!(3)),
        ];
        let coalesced = coalesce_wal_entries(entries);
        assert_eq!(coalesced.len(), 3);
        assert_eq!(coalesced[0].1.as_ref().unwrap().to_value(), json!(1));
        assert_eq!(coalesced[1].1.as_ref().unwrap().to_value(), json!(2));
        assert_eq!(coalesced[2].1.as_ref().unwrap().to_value(), json!(3));
    }

    #[test]
    fn test_coalesce_preserves_order() {
        let entries = vec![
            WalEntry::set("/a", json!(1)),
            WalEntry::set("/b", json!(2)),
            WalEntry::set("/a", json!(10)),
            WalEntry::set("/c", json!(3)),
        ];
        let coalesced = coalesce_wal_entries(entries);
        assert_eq!(coalesced.len(), 4);
        assert_eq!(coalesced[0].0, vec!["a".to_string()]);
        assert_eq!(coalesced[0].1.as_ref().unwrap().to_value(), json!(1));
        assert_eq!(coalesced[1].0, vec!["b".to_string()]);
        assert_eq!(coalesced[2].0, vec!["a".to_string()]);
        assert_eq!(coalesced[2].1.as_ref().unwrap().to_value(), json!(10));
        assert_eq!(coalesced[3].0, vec!["c".to_string()]);
    }

    #[test]
    fn test_coalesce_delete_after_set() {
        let entries = vec![WalEntry::set("/a", json!(1)), WalEntry::delete("/a")];
        let coalesced = coalesce_wal_entries(entries);
        assert_eq!(coalesced.len(), 2);
        assert!(coalesced[0].1.is_some());
        assert!(coalesced[1].1.is_none());
    }

    #[test]
    fn test_coalesce_set_after_delete() {
        let entries = vec![WalEntry::delete("/a"), WalEntry::set("/a", json!(1))];
        let coalesced = coalesce_wal_entries(entries);
        assert_eq!(coalesced.len(), 2);
        assert!(coalesced[0].1.is_none());
        assert!(coalesced[1].1.is_some());
    }

    #[test]
    fn test_coalesce_all_entries_passed_through() {
        let mut entries = Vec::new();
        for i in 0..20 {
            entries.push(WalEntry::set(
                "/char-attribs/char/-xyz/-attr1/current",
                json!(format!("value-{}", i)),
            ));
        }
        entries.push(WalEntry::set("/chat/-msg1", json!({"text": "hello"})));

        let coalesced = coalesce_wal_entries(entries);
        assert_eq!(coalesced.len(), 21);
        assert_eq!(
            coalesced[19].1.as_ref().unwrap().to_value(),
            json!("value-19")
        );
    }
}
