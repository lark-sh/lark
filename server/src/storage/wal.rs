//! Write-ahead log for durability.
//!
//! The WAL stores operations in JSONL format and handles file rotation
//! when files exceed WALMaxFileSize.
//!
//! The WalWriter is async and uses Glommio's io_uring-based async I/O.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io;
use std::path::{Path, PathBuf};
use tracing::{debug, warn};

use super::fsync::AppendFile;

/// Maximum size of a single WAL file before rotation (5MB).
pub const WAL_MAX_FILE_SIZE: u64 = 5 * 1024 * 1024;

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

/// Async WAL writer that handles appending entries and file rotation.
///
/// Writes are buffered in memory. Data only touches disk during `sync()`,
/// which is called every 2 seconds.
pub struct WalWriter {
    dir: PathBuf,
    current_file: Option<AppendFile>,
    sequence: i64,
    last_append_bytes: u64,
    /// In-memory write buffer — flushed to disk on sync().
    buffer: Vec<u8>,
}

impl WalWriter {
    /// Create a new WAL writer for the given directory.
    /// The directory will be created if it doesn't exist.
    pub async fn new(dir: &Path) -> io::Result<Self> {
        Self::with_min_sequence(dir, 0).await
    }

    /// Create a new WAL writer that starts at a sequence number higher than
    /// both existing WAL files AND the given min_sequence.
    ///
    /// This is needed because compaction deletes old WAL files but updates the
    /// manifest's LastWALSequence. Without this, a restarted server might reuse
    /// sequence numbers that the compactor has already processed.
    pub async fn with_min_sequence(dir: &Path, min_sequence: i64) -> io::Result<Self> {
        // Create directory if it doesn't exist
        super::fsync::create_dir_all_async(dir).await?;

        // Find the highest existing sequence number from files (async)
        let file_sequence = find_highest_wal_sequence(dir).await?;

        // Use the higher of file sequence or manifest sequence
        let sequence = file_sequence.max(min_sequence) + 1;

        let mut writer = WalWriter {
            dir: dir.to_path_buf(),
            current_file: None,
            sequence,
            last_append_bytes: 0,
            buffer: Vec::new(),
        };

        // Open the file for writing
        writer.open_file().await?;

        debug!(
            "[WAL Writer] Initialized at {:?} (sequence={})",
            dir, writer.sequence
        );

        Ok(writer)
    }

    /// Open a new WAL file with the current sequence number.
    async fn open_file(&mut self) -> io::Result<()> {
        let filename = format!("{:06}.wal", self.sequence);
        let path = self.dir.join(&filename);

        let file = AppendFile::open(&path).await?;
        let size = file.size();

        debug!("[WAL Writer] Opened {:?} (size={})", path, size);

        self.current_file = Some(file);
        Ok(())
    }

    /// Append entries to the in-memory buffer.
    ///
    /// No file I/O occurs here — data is written to disk only during `sync()`.
    /// Always returns `Ok(false)` (rotation happens at sync time).
    pub fn append(&mut self, entries: &[WalEntry]) -> io::Result<bool> {
        self.last_append_bytes = 0;

        if entries.is_empty() {
            return Ok(false);
        }

        for entry in entries {
            let json = serde_json::to_vec(entry)?;
            self.last_append_bytes += json.len() as u64 + 1;
            self.buffer.extend_from_slice(&json);
            self.buffer.push(b'\n');
        }

        Ok(false)
    }

    /// Append a single entry to the in-memory buffer.
    pub fn append_one(&mut self, entry: &WalEntry) -> io::Result<bool> {
        self.append(std::slice::from_ref(entry))
    }

    /// Returns the number of bytes written in the last append call.
    pub fn bytes_written_last_append(&self) -> u64 {
        self.last_append_bytes
    }

    /// Flush the in-memory buffer to disk and sync.
    ///
    /// Returns true if the WAL file was rotated (size exceeded threshold).
    pub async fn sync(&mut self) -> io::Result<bool> {
        if self.buffer.is_empty() {
            return Ok(false);
        }

        if let Some(ref mut file) = self.current_file {
            file.write(&self.buffer).await?;
            file.sync().await?;
            self.buffer.clear();

            // Rotate if file is too large
            if file.size() > WAL_MAX_FILE_SIZE {
                self.rotate().await?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Flush any remaining buffer and close the current WAL file.
    pub async fn close(&mut self) -> io::Result<()> {
        if let Some(mut file) = self.current_file.take() {
            if !self.buffer.is_empty() {
                file.write(&self.buffer).await?;
                file.sync().await?;
                self.buffer.clear();
            }
            file.close().await?;
        }
        Ok(())
    }

    /// Rotate to a new WAL file.
    /// Assumes buffer has already been flushed (called from sync()).
    async fn rotate(&mut self) -> io::Result<()> {
        if let Some(file) = self.current_file.take() {
            file.close().await?;
        }

        self.sequence += 1;

        self.open_file().await
    }

    /// Returns the current WAL sequence number.
    pub fn sequence(&self) -> i64 {
        self.sequence
    }
}

/// Find the highest WAL sequence number in the directory.
async fn find_highest_wal_sequence(dir: &Path) -> io::Result<i64> {
    if !super::fsync::file_exists_async(dir).await {
        return Ok(0);
    }

    let mut highest: i64 = 0;

    for path in super::fsync::read_dir_async(dir).await? {
        if path.is_file()
            && let Some(name) = path.file_name().and_then(|n| n.to_str())
            && let Some(num_str) = name.strip_suffix(".wal")
            && let Ok(num) = num_str.parse::<i64>()
            && num > highest
        {
            highest = num;
        }
    }

    Ok(highest)
}

/// WAL reader for reading entries from WAL files.
pub struct WalReader {
    dir: PathBuf,
}

impl WalReader {
    /// Create a new WAL reader for the given directory.
    pub fn new(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
        }
    }

    /// Read all WAL entries from all files in sequence order.
    ///
    /// Uses strict parsing: only the last line of the last file may be truncated.
    pub async fn read_all(&self) -> io::Result<Vec<WalEntry>> {
        let files = self.list_files().await?;
        let total = files.len();
        let mut all_entries = Vec::new();

        for (i, filename) in files.iter().enumerate() {
            let is_last = i == total - 1;
            let entries = self.read_file(filename, is_last).await?;
            all_entries.extend(entries);
        }

        Ok(all_entries)
    }

    /// Read all WAL entries from files with sequence >= min_sequence.
    ///
    /// Uses strict parsing: only the last line of the last file may be truncated.
    /// Also validates WAL continuity — files must form a contiguous sequence
    /// starting from min_sequence with no gaps.
    pub async fn read_since(&self, min_sequence: i64) -> io::Result<Vec<WalEntry>> {
        let files = self.list_files().await?;

        // Filter to files >= min_sequence and collect with their sequences
        let filtered: Vec<(i64, &String)> = files
            .iter()
            .filter_map(|f| parse_wal_sequence(f).map(|seq| (seq, f)))
            .filter(|(seq, _)| *seq >= min_sequence)
            .collect();

        // Validate continuity
        Self::check_continuity(&filtered, min_sequence, &self.dir)?;

        let total = filtered.len();
        let mut all_entries = Vec::new();

        for (i, (seq, filename)) in filtered.iter().enumerate() {
            let is_last = i == total - 1;
            let mut entries = self.read_file(filename, is_last).await?;
            tracing::debug!(
                "WAL read {}: {} ({} entries)",
                filename,
                self.dir.display(),
                entries.len()
            );
            for entry in &mut entries {
                entry.sequence = *seq;
            }
            all_entries.extend(entries);
        }

        Ok(all_entries)
    }

    /// Read WAL entries from files with min_sequence <= sequence <= max_sequence.
    ///
    /// Uses strict parsing: only the last line of the last file may be truncated.
    pub async fn read_between(
        &self,
        min_sequence: i64,
        max_sequence: i64,
    ) -> io::Result<Vec<WalEntry>> {
        let files = self.list_files().await?;

        let filtered: Vec<(i64, &String)> = files
            .iter()
            .filter_map(|f| parse_wal_sequence(f).map(|seq| (seq, f)))
            .filter(|(seq, _)| *seq >= min_sequence && *seq <= max_sequence)
            .collect();

        let total = filtered.len();
        let mut all_entries = Vec::new();

        for (i, (seq, filename)) in filtered.iter().enumerate() {
            let is_last = i == total - 1;
            let mut entries = self.read_file(filename, is_last).await?;
            for entry in &mut entries {
                entry.sequence = *seq;
            }
            all_entries.extend(entries);
        }

        Ok(all_entries)
    }

    /// Get list of WAL files (sequence, filename) between min and max sequence.
    /// Files are returned in sequence order.
    pub async fn files_between(
        &self,
        min_sequence: i64,
        max_sequence: i64,
    ) -> io::Result<Vec<(i64, String)>> {
        let files = self.list_files().await?;
        let mut result = Vec::new();

        for filename in files {
            if let Some(seq) = parse_wal_sequence(&filename)
                && seq >= min_sequence
                && seq <= max_sequence
            {
                result.push((seq, filename));
            }
        }

        Ok(result)
    }

    /// Read all entries from a single WAL file by filename.
    ///
    /// Strict parsing: no trailing truncation allowed. Use this for completed
    /// WAL files (e.g., during compaction where files are fully written).
    pub async fn read_wal_file(&self, filename: &str) -> io::Result<Vec<WalEntry>> {
        self.read_file(filename, false).await
    }

    /// Returns the highest WAL sequence number in the directory.
    pub async fn highest_sequence(&self) -> i64 {
        find_highest_wal_sequence(&self.dir).await.unwrap_or(0)
    }

    /// Count WAL files with sequence >= min_sequence.
    pub async fn file_count_since(&self, min_sequence: i64) -> usize {
        match self.list_files().await {
            Ok(files) => files
                .iter()
                .filter(|f| parse_wal_sequence(f).is_some_and(|seq| seq >= min_sequence))
                .count(),
            Err(_) => 0,
        }
    }

    /// List WAL filenames sorted by sequence number.
    async fn list_files(&self) -> io::Result<Vec<String>> {
        if !super::fsync::file_exists_async(&self.dir).await {
            return Ok(Vec::new());
        }

        let mut files = Vec::new();

        for path in super::fsync::read_dir_async(&self.dir).await? {
            if path.is_file()
                && let Some(name) = path.file_name().and_then(|n| n.to_str())
                && name.ends_with(".wal")
            {
                files.push(name.to_string());
            }
        }

        // Sort by sequence number
        files.sort_by(|a, b| {
            let seq_a = parse_wal_sequence(a).unwrap_or(0);
            let seq_b = parse_wal_sequence(b).unwrap_or(0);
            seq_a.cmp(&seq_b)
        });

        Ok(files)
    }

    /// Read all entries from a single WAL file.
    ///
    /// `allow_trailing_truncation`: if true, a malformed last line is tolerated.
    /// Only pass true for the LAST file during replay (crash may have truncated it).
    async fn read_file(
        &self,
        filename: &str,
        allow_trailing_truncation: bool,
    ) -> io::Result<Vec<WalEntry>> {
        let path = self.dir.join(filename);
        let bytes = super::fsync::read_file_async(&path).await?;
        let content =
            String::from_utf8(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Self::parse_wal_lines(&content, &path, allow_trailing_truncation)
    }

    /// Validate that WAL files form a contiguous sequence starting from min_sequence.
    ///
    /// Rules:
    /// - If no files exist >= min_sequence, that's fine (no new data since compaction)
    /// - If files exist, the first file's sequence must equal min_sequence
    /// - Files must be contiguous: each file = previous + 1
    ///
    /// Violations indicate missing WAL files (data loss) and are fatal.
    fn check_continuity(files: &[(i64, &String)], min_sequence: i64, dir: &Path) -> io::Result<()> {
        if files.is_empty() {
            return Ok(());
        }

        // First file must be exactly min_sequence
        let first_seq = files[0].0;
        if first_seq != min_sequence {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "WAL continuity error in {:?}: expected first WAL file to be {:06}.wal (sequence {}), \
                     but first file is {:06}.wal (sequence {}). \
                     WAL files {} through {} are missing — refusing to load. \
                     Manual inspection required.",
                    dir,
                    min_sequence,
                    min_sequence,
                    first_seq,
                    first_seq,
                    min_sequence,
                    first_seq - 1
                ),
            ));
        }

        // Check for gaps between consecutive files
        for i in 1..files.len() {
            let prev_seq = files[i - 1].0;
            let curr_seq = files[i].0;
            if curr_seq != prev_seq + 1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "WAL continuity error in {:?}: gap between {:06}.wal (sequence {}) \
                         and {:06}.wal (sequence {}). \
                         WAL files {} through {} are missing — refusing to load. \
                         Manual inspection required.",
                        dir,
                        prev_seq,
                        prev_seq,
                        curr_seq,
                        curr_seq,
                        prev_seq + 1,
                        curr_seq - 1
                    ),
                ));
            }
        }

        Ok(())
    }

    /// Parse WAL entries from lines, with strict corruption detection.
    ///
    /// If `allow_trailing_truncation` is true (only valid for the LAST WAL file
    /// during replay), a malformed final line is tolerated — this handles the case
    /// where the server crashed mid-write. Any malformed line that is NOT the last
    /// line is always a fatal error, because it means data was lost or corrupted
    /// in the middle of the file.
    fn parse_wal_lines(
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

    /// Delete all WAL files with sequence < max_sequence.
    pub async fn delete_before(&self, max_sequence: i64) -> io::Result<()> {
        let files = self.list_files().await?;

        for filename in files {
            if let Some(seq) = parse_wal_sequence(&filename)
                && seq < max_sequence
            {
                let path = self.dir.join(&filename);
                super::fsync::remove_file_async(&path).await?;
            }
        }

        Ok(())
    }
}

/// Parse WAL sequence number from filename (e.g., "000001.wal" -> 1).
fn parse_wal_sequence(filename: &str) -> Option<i64> {
    filename
        .strip_suffix(".wal")
        .and_then(|s| s.parse::<i64>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        let local_ex = glommio::LocalExecutor::default();
        local_ex.run(f)
    }

    #[test]
    fn test_wal_entry_serialization() {
        let entry = WalEntry::set("/users/1", json!({"name": "Alice"}));
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains(r#""o":"s""#));
        assert!(json.contains(r#""p":"/users/1""#));

        let parsed: WalEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.op, WalOp::Set);
        assert_eq!(parsed.path, "/users/1");
    }

    #[test]
    fn test_wal_delete_entry() {
        let entry = WalEntry::delete("/users/1");
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains(r#""o":"d""#));
        assert!(!json.contains(r#""v":"#)); // No value field
    }

    #[test]
    fn test_wal_writer_and_reader() {
        block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let wal_dir = temp_dir.path().join("wal");

            // Write some entries
            {
                let mut writer = WalWriter::new(&wal_dir).await.unwrap();
                assert_eq!(writer.sequence(), 1);

                writer.append_one(&WalEntry::set("/a", json!(1))).unwrap();
                writer.append_one(&WalEntry::set("/b", json!(2))).unwrap();
                writer.sync().await.unwrap();
            }

            // Read entries back
            {
                let reader = WalReader::new(&wal_dir);
                let entries = reader.read_all().await.unwrap();
                assert_eq!(entries.len(), 2);
                assert_eq!(entries[0].path, "/a");
                assert_eq!(entries[1].path, "/b");
            }
        })
    }

    #[test]
    fn test_wal_read_since() {
        block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let wal_dir = temp_dir.path().join("wal");

            // Write some entries across multiple files
            {
                let mut writer = WalWriter::new(&wal_dir).await.unwrap();
                writer.append_one(&WalEntry::set("/a", json!(1))).unwrap();
                writer.sync().await.unwrap();
            }

            // Create new writer (new sequence)
            {
                let mut writer = WalWriter::new(&wal_dir).await.unwrap();
                assert_eq!(writer.sequence(), 2);
                writer.append_one(&WalEntry::set("/b", json!(2))).unwrap();
                writer.sync().await.unwrap();
            }

            // Read since sequence 2
            {
                let reader = WalReader::new(&wal_dir);
                let entries = reader.read_since(2).await.unwrap();
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].path, "/b");
            }
        })
    }

    #[test]
    fn test_wal_delete_before() {
        block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let wal_dir = temp_dir.path().join("wal");

            // Write entries across multiple files
            {
                let mut writer = WalWriter::new(&wal_dir).await.unwrap();
                writer.append_one(&WalEntry::set("/a", json!(1))).unwrap();
                writer.sync().await.unwrap();
            }
            {
                let mut writer = WalWriter::new(&wal_dir).await.unwrap();
                writer.append_one(&WalEntry::set("/b", json!(2))).unwrap();
                writer.sync().await.unwrap();
            }

            // Delete files before sequence 2
            let reader = WalReader::new(&wal_dir);
            reader.delete_before(2).await.unwrap();

            // Only sequence 2 should remain
            let entries = reader.read_all().await.unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].path, "/b");
        })
    }

    #[test]
    fn test_wal_min_sequence() {
        block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let wal_dir = temp_dir.path().join("wal");

            // Create writer with min sequence
            let writer = WalWriter::with_min_sequence(&wal_dir, 100).await.unwrap();
            assert_eq!(writer.sequence(), 101); // Should be min_sequence + 1
        })
    }

    #[test]
    fn test_wal_multiple_entries() {
        block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let wal_dir = temp_dir.path().join("wal");

            let mut writer = WalWriter::new(&wal_dir).await.unwrap();

            let entries = vec![
                WalEntry::set("/a", json!(1)),
                WalEntry::set("/b", json!(2)),
                WalEntry::set("/c", json!(3)),
            ];

            writer.append(&entries).unwrap();
            writer.sync().await.unwrap();

            let reader = WalReader::new(&wal_dir);
            let read_entries = reader.read_all().await.unwrap();
            assert_eq!(read_entries.len(), 3);
        })
    }

    #[test]
    fn test_wal_writer_sequence_persistence() {
        block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let wal_dir = temp_dir.path().join("wal");

            // Create first writer
            let seq1 = {
                let mut w1 = WalWriter::new(&wal_dir).await.unwrap();
                let seq = w1.sequence();
                w1.append_one(&WalEntry::set("/a", json!(1))).unwrap();
                w1.sync().await.unwrap();
                seq
            };

            // Create second writer - should pick up next sequence
            let seq2 = {
                let w2 = WalWriter::new(&wal_dir).await.unwrap();
                w2.sequence()
            };

            assert!(
                seq2 > seq1,
                "Second writer sequence ({}) should be greater than first ({})",
                seq2,
                seq1
            );
        })
    }

    #[test]
    fn test_wal_reader_empty_directory() {
        block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let wal_dir = temp_dir.path().join("wal");
            std::fs::create_dir_all(&wal_dir).unwrap();

            let reader = WalReader::new(&wal_dir);
            let entries = reader.read_all().await.unwrap();
            assert_eq!(entries.len(), 0, "Expected 0 entries for empty dir");
        })
    }

    #[test]
    fn test_wal_reader_nonexistent_directory() {
        block_on(async {
            let reader = WalReader::new(std::path::Path::new("/non/existent/path"));
            let entries = reader.read_all().await.unwrap();
            assert_eq!(entries.len(), 0, "Expected 0 entries for non-existent dir");
        })
    }

    #[test]
    fn test_wal_writer_empty_append() {
        block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let wal_dir = temp_dir.path().join("wal");

            let mut writer = WalWriter::new(&wal_dir).await.unwrap();

            // Empty append should not error
            let result = writer.append(&[]);
            assert!(result.is_ok(), "Empty append should not error");

            let result = writer.append(&Vec::new());
            assert!(result.is_ok(), "Empty vec append should not error");
        })
    }

    #[test]
    #[allow(clippy::approx_constant)] // 3.14 is test data, not PI
    fn test_wal_complex_values() {
        block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let wal_dir = temp_dir.path().join("wal");

            let complex_value = json!({
                "name": "Test",
                "nested": {"a": 1, "b": "two"},
                "array": [1, 2, 3],
                "bool": true,
                "float": 3.14
            });

            {
                let mut writer = WalWriter::new(&wal_dir).await.unwrap();
                writer
                    .append_one(&WalEntry::set("/complex", complex_value.clone()))
                    .unwrap();
                writer.sync().await.unwrap();
            }

            let reader = WalReader::new(&wal_dir);
            let entries = reader.read_all().await.unwrap();

            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].path, "/complex");

            // Verify complex value was preserved
            let val = entries[0].value.as_ref().unwrap();
            assert_eq!(val.get("name").unwrap(), "Test");
            assert_eq!(val.get("nested").unwrap().get("b").unwrap(), "two");
            assert_eq!(val.get("array").unwrap().as_array().unwrap().len(), 3);
            assert_eq!(val.get("bool").unwrap(), true);
        })
    }

    #[test]
    fn test_wal_writer_min_sequence_with_existing_files() {
        block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let wal_dir = temp_dir.path().join("wal");

            // Create a WAL file at sequence 10
            {
                let mut w1 = WalWriter::with_min_sequence(&wal_dir, 9).await.unwrap();
                assert_eq!(w1.sequence(), 10); // Should be min_sequence + 1
                w1.append_one(&WalEntry::set("/a", json!(1))).unwrap();
                w1.sync().await.unwrap();
            }

            // Now create a new writer with minSequence=5 (lower than existing file)
            // It should use sequence 11 (based on existing file 10), not 6
            {
                let w2 = WalWriter::with_min_sequence(&wal_dir, 5).await.unwrap();
                assert_eq!(
                    w2.sequence(),
                    11,
                    "Expected sequence 11 (from existing files), got {}",
                    w2.sequence()
                );
            }
        })
    }

    // =========================================================================
    // WAL Integrity Tests
    //
    // Tests for continuity validation, mid-file corruption detection,
    // and trailing truncation tolerance.
    // =========================================================================

    #[test]
    fn test_wal_continuity_gap_detected() {
        // If a WAL file is missing between manifest sequence and the latest file,
        // read_since should fail with a continuity error.
        block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let wal_dir = temp_dir.path().join("wal");
            std::fs::create_dir_all(&wal_dir).unwrap();

            // Manually create WAL files with a gap: 2, 3, 5 (missing 4)
            std::fs::write(
                wal_dir.join("000002.wal"),
                "{\"o\":\"s\",\"p\":\"/a\",\"v\":1}\n",
            )
            .unwrap();
            std::fs::write(
                wal_dir.join("000003.wal"),
                "{\"o\":\"s\",\"p\":\"/b\",\"v\":2}\n",
            )
            .unwrap();
            // Skip 000004.wal
            std::fs::write(
                wal_dir.join("000005.wal"),
                "{\"o\":\"s\",\"p\":\"/c\",\"v\":3}\n",
            )
            .unwrap();

            let reader = WalReader::new(&wal_dir);

            // read_since(2) should detect the gap between 3 and 5
            let result = reader.read_since(2).await;
            assert!(result.is_err(), "Expected error due to WAL gap");
            let err = result.unwrap_err();
            assert!(
                err.to_string().contains("gap"),
                "Error should mention gap: {}",
                err
            );
        })
    }

    #[test]
    fn test_wal_continuity_missing_first_file() {
        // If the first expected WAL file is missing, read_since should fail.
        // e.g., manifest at seq 5, files are 7,8 (missing 6)
        block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let wal_dir = temp_dir.path().join("wal");
            std::fs::create_dir_all(&wal_dir).unwrap();

            std::fs::write(
                wal_dir.join("000007.wal"),
                "{\"o\":\"s\",\"p\":\"/a\",\"v\":1}\n",
            )
            .unwrap();
            std::fs::write(
                wal_dir.join("000008.wal"),
                "{\"o\":\"s\",\"p\":\"/b\",\"v\":2}\n",
            )
            .unwrap();

            let reader = WalReader::new(&wal_dir);

            // read_since(6) — file 6 is missing
            let result = reader.read_since(6).await;
            assert!(
                result.is_err(),
                "Expected error due to missing first WAL file"
            );
            let err = result.unwrap_err();
            assert!(
                err.to_string().contains("000006"),
                "Error should reference the missing file: {}",
                err
            );
        })
    }

    #[test]
    fn test_wal_continuity_no_files_is_ok() {
        // No WAL files after the manifest sequence is fine (no new writes)
        block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let wal_dir = temp_dir.path().join("wal");
            std::fs::create_dir_all(&wal_dir).unwrap();

            let reader = WalReader::new(&wal_dir);
            let result = reader.read_since(10).await;
            assert!(result.is_ok(), "No WAL files should be OK");
            assert_eq!(result.unwrap().len(), 0);
        })
    }

    #[test]
    fn test_wal_continuity_contiguous_files_ok() {
        // Contiguous WAL files from min_sequence should pass
        block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let wal_dir = temp_dir.path().join("wal");
            std::fs::create_dir_all(&wal_dir).unwrap();

            std::fs::write(
                wal_dir.join("000003.wal"),
                "{\"o\":\"s\",\"p\":\"/a\",\"v\":1}\n",
            )
            .unwrap();
            std::fs::write(
                wal_dir.join("000004.wal"),
                "{\"o\":\"s\",\"p\":\"/b\",\"v\":2}\n",
            )
            .unwrap();
            std::fs::write(
                wal_dir.join("000005.wal"),
                "{\"o\":\"s\",\"p\":\"/c\",\"v\":3}\n",
            )
            .unwrap();

            let reader = WalReader::new(&wal_dir);
            let result = reader.read_since(3).await;
            assert!(result.is_ok(), "Contiguous WAL files should be OK");
            assert_eq!(result.unwrap().len(), 3);
        })
    }

    #[test]
    fn test_wal_continuity_ignores_older_files() {
        // Files before min_sequence should be ignored (leftover from before cleanup)
        block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let wal_dir = temp_dir.path().join("wal");
            std::fs::create_dir_all(&wal_dir).unwrap();

            // Files 1,2 are old (before manifest), 3,4 are new
            std::fs::write(
                wal_dir.join("000001.wal"),
                "{\"o\":\"s\",\"p\":\"/old1\",\"v\":1}\n",
            )
            .unwrap();
            std::fs::write(
                wal_dir.join("000002.wal"),
                "{\"o\":\"s\",\"p\":\"/old2\",\"v\":2}\n",
            )
            .unwrap();
            std::fs::write(
                wal_dir.join("000003.wal"),
                "{\"o\":\"s\",\"p\":\"/new1\",\"v\":3}\n",
            )
            .unwrap();
            std::fs::write(
                wal_dir.join("000004.wal"),
                "{\"o\":\"s\",\"p\":\"/new2\",\"v\":4}\n",
            )
            .unwrap();

            let reader = WalReader::new(&wal_dir);
            // read_since(3) should only read 3,4 and ignore 1,2
            let result = reader.read_since(3).await.unwrap();
            assert_eq!(result.len(), 2);
            assert_eq!(result[0].path, "/new1");
            assert_eq!(result[1].path, "/new2");
        })
    }

    #[test]
    fn test_wal_mid_file_corruption_detected() {
        // A malformed line in the middle of a WAL file should be fatal.
        block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let wal_dir = temp_dir.path().join("wal");
            std::fs::create_dir_all(&wal_dir).unwrap();

            // File with corruption in the middle (line 2 of 3)
            std::fs::write(
                wal_dir.join("000001.wal"),
                "{\"o\":\"s\",\"p\":\"/a\",\"v\":1}\n\
                 THIS IS CORRUPTED\n\
                 {\"o\":\"s\",\"p\":\"/c\",\"v\":3}\n",
            )
            .unwrap();

            let reader = WalReader::new(&wal_dir);
            let result = reader.read_all().await;
            assert!(result.is_err(), "Mid-file corruption should be fatal");
            let err = result.unwrap_err();
            assert!(
                err.to_string().contains("corruption") || err.to_string().contains("not last line"),
                "Error should mention corruption: {}",
                err
            );
        })
    }

    #[test]
    fn test_wal_trailing_truncation_tolerated_on_last_file() {
        // A truncated last line on the LAST WAL file should be tolerated
        // (this is the normal crash scenario).
        block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let wal_dir = temp_dir.path().join("wal");
            std::fs::create_dir_all(&wal_dir).unwrap();

            // Last file has truncated last line
            std::fs::write(
                wal_dir.join("000001.wal"),
                "{\"o\":\"s\",\"p\":\"/a\",\"v\":1}\n\
                 {\"o\":\"s\",\"p\":\"/b\",\"v\":2}\n\
                 {\"o\":\"s\",\"p\":\"/c\",\"v\":3", // truncated — missing closing }
            )
            .unwrap();

            let reader = WalReader::new(&wal_dir);
            let result = reader.read_all().await;
            assert!(
                result.is_ok(),
                "Trailing truncation on last file should be tolerated"
            );
            // Should have 2 entries (the truncated 3rd is skipped)
            assert_eq!(result.unwrap().len(), 2);
        })
    }

    #[test]
    fn test_wal_trailing_truncation_rejected_on_non_last_file() {
        // A truncated last line on a NON-LAST file should be fatal,
        // because completed files should have been fully written.
        block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let wal_dir = temp_dir.path().join("wal");
            std::fs::create_dir_all(&wal_dir).unwrap();

            // First file has truncated last line
            std::fs::write(
                wal_dir.join("000001.wal"),
                "{\"o\":\"s\",\"p\":\"/a\",\"v\":1}\n\
                 {\"o\":\"s\",\"p\":\"/b\",\"v\":", // truncated
            )
            .unwrap();
            // Second file is fine
            std::fs::write(
                wal_dir.join("000002.wal"),
                "{\"o\":\"s\",\"p\":\"/c\",\"v\":3}\n",
            )
            .unwrap();

            let reader = WalReader::new(&wal_dir);
            let result = reader.read_all().await;
            assert!(
                result.is_err(),
                "Truncation on non-last file should be fatal"
            );
        })
    }

    #[test]
    fn test_wal_single_file_corruption_in_middle() {
        // Even in a single-file scenario, corruption in the MIDDLE is fatal.
        // Only the very last line gets the truncation pass.
        block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let wal_dir = temp_dir.path().join("wal");
            std::fs::create_dir_all(&wal_dir).unwrap();

            // Single file, corruption in middle (line 2 of 3)
            std::fs::write(
                wal_dir.join("000001.wal"),
                "{\"o\":\"s\",\"p\":\"/a\",\"v\":1}\n\
                 GARBAGE\n\
                 {\"o\":\"s\",\"p\":\"/c\",\"v\":3}\n",
            )
            .unwrap();

            let reader = WalReader::new(&wal_dir);
            let result = reader.read_all().await;
            assert!(
                result.is_err(),
                "Mid-file corruption should be fatal even in single file"
            );
        })
    }

    #[test]
    fn test_wal_writer_min_sequence_after_compaction() {
        block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let wal_dir = temp_dir.path().join("wal");
            std::fs::create_dir_all(&wal_dir).unwrap();

            // Simulate: compaction has processed through sequence 5 and deleted all WAL files.
            // Directory is now empty (no .wal files).
            let manifest_last_sequence: i64 = 5;

            // Create writer with minSequence from manifest
            let mut writer = WalWriter::with_min_sequence(&wal_dir, manifest_last_sequence)
                .await
                .unwrap();

            // Writer should start at sequence 6 (manifestLastSequence + 1)
            assert_eq!(
                writer.sequence(),
                6,
                "Expected sequence 6, got {}",
                writer.sequence()
            );

            // Write an entry
            writer
                .append_one(&WalEntry::set("/test", json!("value")))
                .unwrap();
            writer.sync().await.unwrap();
            drop(writer);

            // Verify the file was created with sequence 6
            let files: Vec<_> = std::fs::read_dir(&wal_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().is_some_and(|ext| ext == "wal"))
                .collect();
            assert_eq!(files.len(), 1);
            assert_eq!(files[0].file_name().to_str().unwrap(), "000006.wal");

            // Compactor would call ReadSince(manifestLastSequence + 1) = ReadSince(6)
            let reader = WalReader::new(&wal_dir);
            let entries = reader.read_since(manifest_last_sequence + 1).await.unwrap();

            // Should find the entry we wrote
            assert_eq!(entries.len(), 1, "Expected 1 entry, got {}", entries.len());
            assert_eq!(entries[0].path, "/test");
        })
    }

    /// Verify that WalReader stamps each entry with the WAL file's sequence number.
    #[test]
    fn test_wal_reader_stamps_entry_sequence() {
        block_on(async {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let wal_dir = temp_dir.path().join("wal");
            std::fs::create_dir_all(&wal_dir).unwrap();

            // Write entries across two WAL files (manually rotate)
            let mut writer = WalWriter::new(&wal_dir).await.unwrap();
            let seq1 = writer.sequence();

            writer.append_one(&WalEntry::set("/a", json!(1))).unwrap();
            writer.append_one(&WalEntry::set("/b", json!(2))).unwrap();

            // Force rotation by syncing/closing and starting a new file
            writer.sync().await.unwrap();
            writer.close().await.unwrap();

            // Manually create second file by bumping sequence
            let mut writer2 = WalWriter::with_min_sequence(&wal_dir, seq1).await.unwrap();
            let seq2 = writer2.sequence();
            assert!(seq2 > seq1, "Second writer should have higher sequence");

            writer2.append_one(&WalEntry::set("/c", json!(3))).unwrap();
            writer2.sync().await.unwrap();
            writer2.close().await.unwrap();

            // Read all entries — each should be stamped with its file's sequence
            let reader = WalReader::new(&wal_dir);
            let entries = reader.read_since(seq1).await.unwrap();
            assert_eq!(entries.len(), 3);

            // First two entries should have seq1
            assert_eq!(
                entries[0].sequence, seq1,
                "Entry /a should have sequence {}",
                seq1
            );
            assert_eq!(entries[0].path, "/a");
            assert_eq!(
                entries[1].sequence, seq1,
                "Entry /b should have sequence {}",
                seq1
            );
            assert_eq!(entries[1].path, "/b");

            // Third entry should have seq2
            assert_eq!(
                entries[2].sequence, seq2,
                "Entry /c should have sequence {}",
                seq2
            );
            assert_eq!(entries[2].path, "/c");
        })
    }

    /// Verify that read_between also stamps sequences correctly.
    #[test]
    fn test_wal_reader_read_between_stamps_sequence() {
        block_on(async {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let wal_dir = temp_dir.path().join("wal");
            std::fs::create_dir_all(&wal_dir).unwrap();

            let mut writer = WalWriter::new(&wal_dir).await.unwrap();
            let seq1 = writer.sequence();
            writer
                .append_one(&WalEntry::set("/x", json!("val")))
                .unwrap();
            writer.sync().await.unwrap();
            writer.close().await.unwrap();

            let mut writer2 = WalWriter::with_min_sequence(&wal_dir, seq1).await.unwrap();
            let seq2 = writer2.sequence();
            writer2
                .append_one(&WalEntry::set("/y", json!("val2")))
                .unwrap();
            writer2.sync().await.unwrap();
            writer2.close().await.unwrap();

            // Read only the second file
            let reader = WalReader::new(&wal_dir);
            let entries = reader.read_between(seq2, seq2).await.unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].sequence, seq2);
            assert_eq!(entries[0].path, "/y");
        })
    }

    /// Verify that sequence field is not serialized to disk (serde skip).
    #[test]
    fn test_wal_entry_sequence_not_serialized() {
        let mut entry = WalEntry::set("/test", json!(42));
        entry.sequence = 99;

        let json = serde_json::to_string(&entry).unwrap();
        // The JSON should NOT contain "sequence"
        assert!(
            !json.contains("sequence"),
            "sequence field should not be serialized: {}",
            json
        );

        // Deserializing should default sequence to 0
        let parsed: WalEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(
            parsed.sequence, 0,
            "Deserialized sequence should be 0 (default)"
        );
        assert_eq!(parsed.path, "/test");
    }
}
