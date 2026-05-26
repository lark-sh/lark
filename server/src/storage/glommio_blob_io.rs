//! GlommioBlobIO: async BlobIO implementation using Glommio's io_uring-based I/O.
//!
//! This replaces StdBlobIO for production use in the main server, where blocking
//! file I/O would stall the entire core. Glommio's BufferedFile submits I/O
//! requests to io_uring and yields to the scheduler while waiting for completion,
//! allowing other databases on the same core to make progress.

use glommio::io::{BufferedFile, OpenOptions};
use lark_blob::BlobIO;
use std::cell::Cell;
use std::io;
use std::path::{Path, PathBuf};

/// Blob I/O backend using Glommio's io_uring-based async file I/O.
///
/// Each database holds one GlommioBlobIO with a single open file handle.
/// Reads use `read_at` (positional, no seeking). Writes use `write_at`.
/// All operations yield to the Glommio scheduler during I/O.
pub struct GlommioBlobIO {
    file: BufferedFile,
    /// Tracked locally for append — avoids an extra syscall per append.
    /// Uses Cell for interior mutability so write methods can take `&self`.
    size: Cell<u64>,
    /// Stored so clone_for_reading can reopen the same file.
    path: PathBuf,
}

impl GlommioBlobIO {
    /// Open an existing blob file for reading and writing.
    pub async fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .buffered_open(path)
            .await?;
        let size = file.file_size().await?;
        Ok(GlommioBlobIO {
            file,
            size: Cell::new(size),
            path: path.to_path_buf(),
        })
    }

    /// Open an existing file or create it if it doesn't exist (no truncation).
    pub async fn open_or_create(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .buffered_open(path)
            .await?;
        let size = file.file_size().await?;
        Ok(GlommioBlobIO {
            file,
            size: Cell::new(size),
            path: path.to_path_buf(),
        })
    }

    /// Create a new blob file (truncates if exists).
    pub async fn create(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .buffered_open(path)
            .await?;
        Ok(GlommioBlobIO {
            file,
            size: Cell::new(0),
            path: path.to_path_buf(),
        })
    }

    /// Update the stored path after an external rename (e.g., .tmp → final).
    /// The underlying file descriptor is unaffected — this only changes where
    /// `clone_for_reading` will open its new handle.
    pub fn set_path(&mut self, new_path: PathBuf) {
        self.path = new_path;
    }

    /// Derive the path for a related file (e.g., "sidecar" → "blob.sidecar.lark").
    fn related_path(&self, name: &str) -> io::Result<PathBuf> {
        let stem = self.path.file_stem().unwrap_or_default().to_string_lossy();
        let ext = self
            .path
            .extension()
            .map(|e| e.to_string_lossy().into_owned());
        let parent = self.path.parent().unwrap_or(Path::new("."));
        let new_name = match ext {
            Some(e) => format!("{}.{}.{}", stem, name, e),
            None => format!("{}.{}", stem, name),
        };
        Ok(parent.join(new_name))
    }
}

/// Cap per-read_at buffer size to avoid Glommio allocating multi-GB DMA buffers
/// for a single io_uring submission.
const MAX_READ_CHUNK: usize = 16 * 1024 * 1024; // 16 MB

impl BlobIO for GlommioBlobIO {
    async fn pread(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        // Retry short reads (matching StdBlobIO's read_exact_at behavior).
        // BufferedFile::read_at can return fewer bytes than requested.
        let mut result = Vec::with_capacity(len);
        let mut pos = 0u64;
        while result.len() < len {
            let remaining = (len - result.len()).min(MAX_READ_CHUNK);
            match self.file.read_at(offset + pos, remaining).await {
                Ok(chunk) => {
                    if chunk.is_empty() {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            format!(
                                "short read at offset {}: expected {} bytes, got {}",
                                offset,
                                len,
                                result.len()
                            ),
                        ));
                    }
                    pos += chunk.len() as u64;
                    result.extend_from_slice(&chunk);
                }
                Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(result)
    }

    async fn pread_into(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        // Retry short reads (matching StdBlobIO's read_exact_at behavior).
        let mut filled = 0usize;
        while filled < buf.len() {
            let remaining = (buf.len() - filled).min(MAX_READ_CHUNK);
            match self.file.read_at(offset + filled as u64, remaining).await {
                Ok(chunk) => {
                    if chunk.is_empty() {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            format!(
                                "short read at offset {}: expected {} bytes, got {}",
                                offset,
                                buf.len(),
                                filled
                            ),
                        ));
                    }
                    buf[filled..filled + chunk.len()].copy_from_slice(&chunk);
                    filled += chunk.len();
                }
                Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    async fn pwrite(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        loop {
            match self.file.write_at(data.to_vec(), offset).await {
                Ok(_) => break,
                Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        let end = offset + data.len() as u64;
        if end > self.size.get() {
            self.size.set(end);
        }
        Ok(())
    }

    async fn append(&self, data: &[u8]) -> io::Result<u64> {
        let offset = self.size.get();
        loop {
            match self.file.write_at(data.to_vec(), offset).await {
                Ok(_) => break,
                Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        self.size.set(self.size.get() + data.len() as u64);
        Ok(offset)
    }

    async fn sync(&self) -> io::Result<()> {
        self.file.fdatasync().await?;
        Ok(())
    }

    async fn size(&self) -> io::Result<u64> {
        Ok(self.size.get())
    }

    async fn truncate(&self, new_size: u64) -> io::Result<()> {
        self.file.truncate(new_size).await?;
        self.size.set(new_size);
        Ok(())
    }

    async fn clone_for_reading(&self) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .buffered_open(&self.path)
            .await?;
        let size = file.file_size().await?;
        Ok(GlommioBlobIO {
            file,
            size: Cell::new(size),
            path: self.path.clone(),
        })
    }

    async fn close(self) -> io::Result<()> {
        self.file
            .close()
            .await
            .map_err(|e| io::Error::other(format!("close failed: {}", e)))
    }

    async fn yield_point(&self) {
        glommio::yield_if_needed().await;
    }

    async fn open_related(&self, name: &str) -> io::Result<Self> {
        let path = self.related_path(name)?;
        GlommioBlobIO::open(&path).await
    }

    async fn create_related(&self, name: &str) -> io::Result<Self> {
        let path = self.related_path(name)?;
        GlommioBlobIO::create(&path).await
    }

    // copy_from: uses the default pread+pwrite implementation.
}
