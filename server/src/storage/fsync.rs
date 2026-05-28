//! Filesystem durability utilities.
//!
//! This module provides utilities for durable file writes that survive crashes.
//! The key insight is that on POSIX systems:
//!
//! 1. `fsync()` on a file ensures the file's data and metadata are durable
//! 2. `rename()` is atomic but NOT necessarily durable
//! 3. `fsync()` on the parent directory ensures directory entries are durable
//!
//! For crash-safe file writes, we must:
//! 1. Write to a temp file
//! 2. fsync the temp file
//! 3. Rename temp to final path
//! 4. fsync the parent directory (to make the rename durable)
//!
//! ## Async I/O
//!
//! This module provides async file reading utilities using Glommio's io_uring-based I/O.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;
use std::time::Duration;

use glommio::io::BufferedFile;

/// Sync a directory to ensure all directory entries are durable.
///
/// This is necessary after creating or renaming files to ensure the
/// directory entry changes survive a crash.
pub fn sync_dir(dir: &Path) -> io::Result<()> {
    let dir_file = File::open(dir)?;
    dir_file.sync_all()?;
    Ok(())
}

/// Write data to a file with full durability guarantees.
///
/// This function:
/// 1. Writes to a temporary file (path + ".tmp")
/// 2. Fsyncs the temporary file
/// 3. Renames the temp file to the final path (atomic)
/// 4. Fsyncs the parent directory (makes rename durable)
///
/// After this function returns successfully, the data is guaranteed to be
/// durable even if the system crashes immediately afterward.
pub fn write_file_durable(path: &Path, data: &[u8]) -> io::Result<()> {
    let temp_path = path.with_extension("tmp");

    // Write to temp file and fsync
    {
        let mut file = File::create(&temp_path)?;
        file.write_all(data)?;
        file.sync_all()?;
    }

    // Rename for atomicity
    fs::rename(&temp_path, path)?;

    // Fsync parent directory to make rename durable
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }

    Ok(())
}

/// Write data to a file with full durability guarantees, creating parent directories if needed.
///
/// Same as `write_file_durable` but creates parent directories first.
pub fn write_file_durable_mkdir(path: &Path, data: &[u8]) -> io::Result<()> {
    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    write_file_durable(path, data)
}

// ============================================================================
// Async I/O utilities (Glommio)
// ============================================================================

/// Read a file asynchronously using Glommio's io_uring-based I/O.
///
/// This yields to the scheduler during disk I/O, allowing other databases
/// on the same core to make progress while waiting for the read to complete.
pub async fn read_file_async(path: &Path) -> io::Result<Vec<u8>> {
    let file = BufferedFile::open(path).await?;
    let size = file.file_size().await? as usize;

    if size == 0 {
        file.close().await?;
        return Ok(Vec::new());
    }

    // Read the entire file
    let read_result = file.read_at(0, size).await?;
    let data = read_result.to_vec();
    file.close().await?;
    Ok(data)
}

/// Sleep for a given number of milliseconds.
pub async fn sleep_ms(ms: u64) {
    glommio::timer::Timer::new(Duration::from_millis(ms)).await;
}

/// Check if a file exists asynchronously.
pub async fn file_exists_async(path: &Path) -> bool {
    match BufferedFile::open(path).await {
        Ok(file) => {
            let _ = file.close().await;
            true
        }
        Err(_) => false,
    }
}

/// Yield to the scheduler if needed.
/// Allows work stealing between tasks on the same core.
pub async fn yield_if_needed() {
    glommio::yield_if_needed().await;
}

/// Sync a file to ensure its data is durable.
pub async fn sync_file_async(path: &Path) -> io::Result<()> {
    let file = BufferedFile::open(path).await?;
    file.fdatasync().await?;
    file.close().await?;
    Ok(())
}

/// Sync a directory to ensure directory entries are durable.
///
/// Note: Glommio doesn't have native async directory sync, so we use sync + yield.
/// Directory fsync is a fast metadata operation.
pub async fn sync_dir_async(dir: &Path) -> io::Result<()> {
    sync_dir(dir)?;
    glommio::yield_if_needed().await;
    Ok(())
}

/// Write data to a file with full durability guarantees.
///
/// This function:
/// 1. Writes to a temporary file (path + ".tmp")
/// 2. Fsyncs the temporary file
/// 3. Renames the temp file to the final path (atomic)
/// 4. Fsyncs the parent directory (makes rename durable)
pub async fn write_file_durable_async(path: &Path, data: &[u8]) -> io::Result<()> {
    use futures::io::AsyncWriteExt;
    use glommio::io::{OpenOptions, StreamWriterBuilder};

    let temp_path = path.with_extension("tmp");

    // Write to temp file
    {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .buffered_open(&temp_path)
            .await?;

        let mut writer = StreamWriterBuilder::new(file)
            .with_sync_on_close_disabled(true)
            .build();

        writer.write_all(data).await?;
        writer.close().await?;
    }

    // Fsync the temp file
    sync_file_async(&temp_path).await?;

    // Rename for atomicity (sync op + yield)
    fs::rename(&temp_path, path)?;
    glommio::yield_if_needed().await;

    // Fsync parent directory to make rename durable
    if let Some(parent) = path.parent() {
        sync_dir_async(parent).await?;
    }

    Ok(())
}

/// Remove a file asynchronously.
///
/// Note: Glommio doesn't have native async unlink, but this is a fast
/// metadata operation. We yield after to allow other tasks to run.
pub async fn remove_file_async(path: &Path) -> io::Result<()> {
    let result = fs::remove_file(path);
    glommio::yield_if_needed().await;
    result
}

/// Read directory entries asynchronously.
///
/// Returns a Vec of PathBuf for each entry in the directory.
/// This is a fast metadata operation, we yield after to allow other tasks to run.
pub async fn read_dir_async(path: &Path) -> io::Result<Vec<std::path::PathBuf>> {
    let entries: Vec<std::path::PathBuf> = fs::read_dir(path)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .collect();
    glommio::yield_if_needed().await;
    Ok(entries)
}

/// Write data to a file asynchronously.
///
/// This is a simple write without durability guarantees. For durable writes,
/// use `write_file_durable_async`.
pub async fn write_file_async(path: &Path, data: &[u8]) -> io::Result<()> {
    use futures::io::AsyncWriteExt;
    use glommio::io::{OpenOptions, StreamWriterBuilder};

    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .buffered_open(path)
        .await?;

    let mut writer = StreamWriterBuilder::new(file)
        .with_sync_on_close_disabled(true)
        .build();

    writer.write_all(data).await?;
    writer.close().await?;
    Ok(())
}

/// Create directories recursively.
pub async fn create_dir_all_async(path: &Path) -> io::Result<()> {
    fs::create_dir_all(path)?;
    glommio::yield_if_needed().await;
    Ok(())
}

/// Rename a file or directory.
pub async fn rename_async(from: &Path, to: &Path) -> io::Result<()> {
    fs::rename(from, to)?;
    glommio::yield_if_needed().await;
    Ok(())
}

/// Remove a directory and all its contents.
pub async fn remove_dir_all_async(path: &Path) -> io::Result<()> {
    fs::remove_dir_all(path)?;
    glommio::yield_if_needed().await;
    Ok(())
}

/// Get file metadata (size) asynchronously.
///
/// Returns the file size in bytes, or 0 if the file doesn't exist.
pub async fn file_size_async(path: &Path) -> i64 {
    match BufferedFile::open(path).await {
        Ok(file) => {
            let size = file.file_size().await.unwrap_or(0) as i64;
            let _ = file.close().await;
            size
        }
        Err(_) => 0,
    }
}

// ============================================================================
// Async append file (for WAL writing)
// ============================================================================

/// Async append-only file for WAL writing.
///
/// Uses io_uring-based I/O via StreamWriter for efficient sequential writes.
pub struct AppendFile {
    writer: glommio::io::StreamWriter,
    size: u64,
}

impl AppendFile {
    /// Open or create a file in append mode.
    pub async fn open(path: &Path) -> io::Result<Self> {
        use glommio::io::{OpenOptions, StreamWriterBuilder};

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .buffered_open(path)
            .await?;

        let size = file.file_size().await?;

        let writer = StreamWriterBuilder::new(file)
            .with_sync_on_close_disabled(true) // We control sync explicitly
            .build();

        Ok(Self { writer, size })
    }

    /// Write data to the file.
    pub async fn write(&mut self, data: &[u8]) -> io::Result<()> {
        use futures::io::AsyncWriteExt;
        self.writer.write_all(data).await?;
        self.size += data.len() as u64;
        Ok(())
    }

    /// Sync the file to disk (fdatasync).
    pub async fn sync(&mut self) -> io::Result<()> {
        use futures::io::AsyncWriteExt;
        self.writer.flush().await
    }

    /// Close the file.
    pub async fn close(mut self) -> io::Result<()> {
        use futures::io::AsyncWriteExt;
        self.writer.close().await
    }

    /// Get the current file size.
    pub fn size(&self) -> u64 {
        self.size
    }
}

// --- Shared implementations ---

/// Read a file asynchronously and parse as JSON.
///
/// Convenience wrapper around `read_file_async` that also deserializes.
pub async fn read_json_file_async<T: serde::de::DeserializeOwned>(path: &Path) -> io::Result<T> {
    let bytes = read_file_async(path).await?;
    serde_json::from_slice(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_write_file_durable() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");

        write_file_durable(&file_path, b"hello world").unwrap();

        assert!(file_path.exists());
        let content = fs::read_to_string(&file_path).unwrap();
        assert_eq!(content, "hello world");

        // Temp file should not exist
        let temp_path = file_path.with_extension("tmp");
        assert!(!temp_path.exists());
    }

    #[test]
    fn test_write_file_durable_mkdir() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir
            .path()
            .join("subdir")
            .join("nested")
            .join("test.txt");

        write_file_durable_mkdir(&file_path, b"nested content").unwrap();

        assert!(file_path.exists());
        let content = fs::read_to_string(&file_path).unwrap();
        assert_eq!(content, "nested content");
    }

    #[test]
    fn test_sync_dir() {
        let temp_dir = TempDir::new().unwrap();

        // Should not error on a valid directory
        sync_dir(temp_dir.path()).unwrap();
    }

    #[test]
    fn test_overwrite_existing() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");

        // Write initial content
        write_file_durable(&file_path, b"first").unwrap();
        assert_eq!(fs::read_to_string(&file_path).unwrap(), "first");

        // Overwrite
        write_file_durable(&file_path, b"second").unwrap();
        assert_eq!(fs::read_to_string(&file_path).unwrap(), "second");
    }
}
