//! BlobIO trait, in-memory implementation for testing, and std::fs implementation.

use std::cell::Cell;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

/// Read statistics from a BlobIO (or CachedIO wrapper).
///
/// Tracks both actual I/O operations (cache misses) and cache hits.
/// Returned by `take_read_stats()`, which resets counters after reading.
#[derive(Default, Debug, Clone)]
pub struct ReadStats {
    /// Number of pread calls that went to the underlying I/O (cache misses).
    pub pread_count: u64,
    /// Total bytes read from underlying I/O (cache misses).
    pub bytes_read: u64,
    /// Number of pread calls served entirely from cache.
    pub cache_hits: u64,
    /// Total bytes served from cache.
    pub cache_hit_bytes: u64,
    /// Number of cache_region calls (read_container) where the header wasn't cached and had to be fetched from disk.
    pub cache_header_misses: u64,
}

/// Abstraction over blob file I/O. Enables testing with in-memory blobs
/// and production use with real file I/O (std::fs or io_uring).
///
/// Methods are async to allow truly async implementations (e.g. glommio io_uring).
/// Synchronous implementations (MemBlobIO, StdBlobIO) return immediately.
pub trait BlobIO {
    /// Read `len` bytes starting at `offset`.
    async fn pread(&self, offset: u64, len: usize) -> io::Result<Vec<u8>>;

    /// Read exactly `buf.len()` bytes at `offset` into `buf`.
    ///
    /// Like `pread` but writes into a caller-provided buffer instead of
    /// allocating a `Vec<u8>`. Implementations should override this for
    /// zero-allocation reads (e.g. `CachedIO` copies directly from its
    /// cache into the buffer).
    ///
    /// The default implementation calls `pread` and copies into `buf`.
    async fn pread_into(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let data = self.pread(offset, buf.len()).await?;
        if data.len() != buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "pread_into: short read",
            ));
        }
        buf.copy_from_slice(&data);
        Ok(())
    }

    /// Write `data` at `offset` (overwrites existing bytes).
    async fn pwrite(&self, offset: u64, data: &[u8]) -> io::Result<()>;

    /// Write `data` at `offset`, deferring to cache in write-back mode.
    ///
    /// Like `pwrite`, but guarantees the write is captured by the write-back
    /// cache even if the target region isn't already cached. This preserves
    /// crash-safety ordering: deferred header/index writes only reach disk
    /// during `flush_write_back`, after all appended data has been synced.
    ///
    /// Use this for parent index entry updates and container header rewrites
    /// that must not hit disk before the data they reference.
    ///
    /// The default implementation just calls `pwrite` (correct for backends
    /// without write-back support like MemBlobIO and StdBlobIO).
    async fn pwrite_deferred(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.pwrite(offset, data).await
    }

    /// Append `data` at end of blob. Returns the offset where data was written.
    async fn append(&self, data: &[u8]) -> io::Result<u64>;

    /// Sync all pending writes to storage.
    async fn sync(&self) -> io::Result<()>;

    /// Current total size of the blob.
    async fn size(&self) -> io::Result<u64>;

    /// Truncate the blob to `new_size` bytes, discarding everything after.
    async fn truncate(&self, new_size: u64) -> io::Result<()>;

    /// Create a read-only clone of this handle for concurrent read+write.
    ///
    /// For file-backed implementations, this opens a new file descriptor to the
    /// same underlying file. For in-memory implementations, this snapshots the
    /// current data. Used during same-file compaction so a read handle can
    /// read from the clone while the original handle appends at EOF.
    async fn clone_for_reading(&self) -> io::Result<Self>
    where
        Self: Sized;

    /// Explicitly close the handle, releasing any OS resources.
    ///
    /// Backends like glommio require async cleanup — dropping a file handle
    /// synchronously defers FD release to the next I/O cycle and logs a warning.
    /// Call this before dropping handles that were opened via `clone_for_reading`.
    ///
    /// The default implementation is a no-op (fine for StdBlobIO, MemBlobIO).
    async fn close(self) -> io::Result<()>
    where
        Self: Sized,
    {
        Ok(())
    }

    /// Yield control back to the async runtime if needed.
    ///
    /// Called periodically during CPU-bound loops (structural_copy, tree walks)
    /// to give cooperative runtimes like glommio a chance to service other tasks.
    /// Sync implementations return immediately. Glommio should implement this as
    /// `glommio::yield_if_needed().await`.
    async fn yield_point(&self) {}

    /// Hint: pre-cache a byte region for future `pread` calls.
    ///
    /// Implementations that support read caching (e.g. `CachedIO`) should read
    /// the full region from the underlying I/O and store it. Subsequent `pread`
    /// calls within this region become memory lookups.
    ///
    /// Called by `ContainerCache::read_container` when it discovers a small
    /// container (subtree_size ≤ threshold) — one big read up front so all
    /// child reads within the container are free.
    ///
    /// The default implementation is a no-op (fine for MemBlobIO, StdBlobIO).
    async fn cache_region(&self, _offset: u64, _len: usize) -> io::Result<()> {
        Ok(())
    }

    /// Clear all cached byte regions.
    ///
    /// Called at sync boundaries (e.g. after WAL compaction) or on file rotation
    /// to ensure stale cached data isn't served. The default is a no-op.
    async fn clear_read_cache(&self) {}

    /// Remove cached byte regions overlapping [offset, offset+len).
    ///
    /// Called when a region becomes dead space so no stale cache entries
    /// exist for freed regions. The default is a no-op.
    fn clear_region(&self, _offset: u64, _len: u64) {}

    /// Enable or disable write-back mode.
    ///
    /// When write-back is enabled, `pwrite` calls to cached regions update the
    /// cache only — the underlying IO is not written until `flush_write_back`.
    /// Writes to uncached regions still go to disk immediately.
    ///
    /// This eliminates write amplification when the same region (e.g. a
    /// collection header+index) is rewritten many times in a single batch.
    /// The default is a no-op (fine for MemBlobIO, StdBlobIO).
    fn set_write_back(&self, _enabled: bool) {}

    /// Flush all dirty write-back regions to the underlying IO.
    ///
    /// Writes each dirty cached region to disk, then clears the dirty set.
    /// Must be called before the batch ends if write-back mode was enabled.
    /// The default is a no-op.
    async fn flush_write_back(&self) -> io::Result<()> {
        Ok(())
    }

    /// Discard all write-back state without flushing to disk.
    ///
    /// Clears all cached regions (including dirty ones) and disables
    /// write-back mode. Called on error recovery when a batch fails
    /// partway through — discards partial index updates so they don't
    /// contaminate the next batch's flush.
    fn discard_write_back(&self) {}

    /// Return read statistics since last reset, then reset counters.
    /// Tracks both actual I/O operations (cache misses) and cache hits.
    fn take_read_stats(&self) -> ReadStats {
        ReadStats::default()
    }

    /// Open a related file by name (e.g., "sidecar").
    ///
    /// Returns a new IO handle for a sibling file associated with this blob.
    /// For file-backed IO, this derives the path from the current file's path.
    /// For in-memory IO, this looks up a named entry in a shared registry.
    async fn open_related(&self, _name: &str) -> io::Result<Self>
    where
        Self: Sized,
    {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "open_related not supported",
        ))
    }

    /// Create a related file by name (e.g., "sidecar").
    ///
    /// Like `open_related`, but creates the file if it doesn't exist
    /// (truncates if it does). Returns a new IO handle for the sibling file.
    async fn create_related(&self, _name: &str) -> io::Result<Self>
    where
        Self: Sized,
    {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "create_related not supported",
        ))
    }
}

/// Read exactly `len` bytes from `io` at `offset`.
///
/// Returns `BlobError::UnexpectedEof` if the underlying `pread` returns fewer bytes
/// than requested (corrupt file or buggy BlobIO implementation). This prevents panics
/// from downstream array indexing on short reads.
pub(crate) async fn read_exact<IO: BlobIO>(
    io: &IO,
    offset: u64,
    len: usize,
) -> crate::Result<Vec<u8>> {
    let data = io.pread(offset, len).await?;
    if data.len() != len {
        return Err(crate::BlobError::UnexpectedEof);
    }
    Ok(data)
}

/// Read exactly `buf.len()` bytes from `io` at `offset` into a caller-provided buffer.
///
/// Zero-allocation variant of `read_exact`. Use with stack-allocated arrays for
/// small fixed-size reads to avoid heap allocation on every call.
pub(crate) async fn read_exact_into<IO: BlobIO>(
    io: &IO,
    offset: u64,
    buf: &mut [u8],
) -> crate::Result<()> {
    io.pread_into(offset, buf).await?;
    Ok(())
}

/// In-memory BlobIO backed by shared Vec<u8>. Used for tests.
///
/// Uses `Rc<RefCell<Vec<u8>>>` so that `clone_for_reading` produces a handle
/// that sees the same data — writes via one handle are visible to reads via
/// the other, matching how `StdBlobIO::clone_for_reading` shares the same
/// underlying file.
///
/// Related files (sidecars) are stored in a shared HashMap registry
/// so `create_related`/`open_related` work across clones.
#[derive(Debug, Clone, Default)]
pub struct MemBlobIO {
    data: std::rc::Rc<std::cell::RefCell<Vec<u8>>>,
    related: std::rc::Rc<std::cell::RefCell<HashMap<String, MemBlobIO>>>,
}

impl MemBlobIO {
    pub fn new() -> Self {
        MemBlobIO {
            data: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
            related: std::rc::Rc::new(std::cell::RefCell::new(HashMap::new())),
        }
    }

    /// Create a MemBlobIO pre-loaded with existing blob data.
    pub fn from_bytes(data: Vec<u8>) -> Self {
        MemBlobIO {
            data: std::rc::Rc::new(std::cell::RefCell::new(data)),
            related: std::rc::Rc::new(std::cell::RefCell::new(HashMap::new())),
        }
    }

    pub fn data(&self) -> std::cell::Ref<'_, [u8]> {
        std::cell::Ref::map(self.data.borrow(), |v| v.as_slice())
    }
}

impl BlobIO for MemBlobIO {
    async fn clone_for_reading(&self) -> io::Result<Self> {
        Ok(MemBlobIO {
            data: self.data.clone(),
            related: self.related.clone(),
        })
    }

    async fn pread(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let data = self.data.borrow();
        let offset = offset as usize;
        if offset + len > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "pread past end: offset={}, len={}, size={}",
                    offset,
                    len,
                    data.len()
                ),
            ));
        }
        Ok(data[offset..offset + len].to_vec())
    }

    async fn pread_into(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let data = self.data.borrow();
        let offset = offset as usize;
        let len = buf.len();
        if offset + len > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "pread_into past end: offset={}, len={}, size={}",
                    offset,
                    len,
                    data.len()
                ),
            ));
        }
        buf.copy_from_slice(&data[offset..offset + len]);
        Ok(())
    }

    async fn pwrite(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        let mut buf = self.data.borrow_mut();
        let offset = offset as usize;
        let end = offset + data.len();
        if end > buf.len() {
            buf.resize(end, 0);
        }
        buf[offset..end].copy_from_slice(data);
        Ok(())
    }

    async fn append(&self, data: &[u8]) -> io::Result<u64> {
        let mut buf = self.data.borrow_mut();
        let offset = buf.len() as u64;
        buf.extend_from_slice(data);
        Ok(offset)
    }

    async fn sync(&self) -> io::Result<()> {
        Ok(())
    }

    async fn size(&self) -> io::Result<u64> {
        Ok(self.data.borrow().len() as u64)
    }

    async fn truncate(&self, new_size: u64) -> io::Result<()> {
        self.data.borrow_mut().truncate(new_size as usize);
        Ok(())
    }

    async fn open_related(&self, name: &str) -> io::Result<Self> {
        let map = self.related.borrow();
        match map.get(name) {
            Some(io) => Ok(io.clone()),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("no related file: {}", name),
            )),
        }
    }

    async fn create_related(&self, name: &str) -> io::Result<Self> {
        let new_io = MemBlobIO::new();
        self.related
            .borrow_mut()
            .insert(name.to_string(), new_io.clone());
        Ok(new_io)
    }
}

/// File-backed BlobIO using std::fs::File with pread/pwrite.
///
/// Uses `FileExt::write_all_at` for positional writes (takes `&File`),
/// enabling all write methods to take `&self` via interior mutability.
/// File size is tracked in a `Cell<u64>` so `append` can atomically
/// claim space without `&mut self`.
pub struct StdBlobIO {
    file: File,
    /// Tracked file size for append positioning. Updated by append/truncate/pwrite.
    tracked_size: Cell<u64>,
    /// Path to this file, used to derive related file paths.
    /// None for clone_for_reading handles (they share the same file).
    path: Option<PathBuf>,
}

impl StdBlobIO {
    /// Open an existing blob file for reading and writing.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let size = file.metadata()?.len();
        Ok(StdBlobIO {
            file,
            tracked_size: Cell::new(size),
            path: Some(path.to_path_buf()),
        })
    }

    /// Create a new blob file (truncates if exists).
    pub fn create(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(StdBlobIO {
            file,
            tracked_size: Cell::new(0),
            path: Some(path.to_path_buf()),
        })
    }

    /// Derive a related file path from this file's path.
    ///
    /// Convention: strip extension, append `.{name}`, add extension back.
    /// e.g., `/data/blob.lark` + `"sidecar"` → `/data/blob.sidecar.lark`
    fn related_path(&self, name: &str) -> io::Result<PathBuf> {
        let path = self
            .path
            .as_ref()
            .ok_or_else(|| io::Error::other("no path available for related file"))?;
        let stem = path.file_stem().unwrap_or_default().to_string_lossy();
        let ext = path.extension().map(|e| e.to_string_lossy().into_owned());
        let parent = path.parent().unwrap_or(Path::new("."));
        let new_name = match ext {
            Some(e) => format!("{}.{}.{}", stem, name, e),
            None => format!("{}.{}", stem, name),
        };
        Ok(parent.join(new_name))
    }
}

impl BlobIO for StdBlobIO {
    async fn clone_for_reading(&self) -> io::Result<Self> {
        Ok(StdBlobIO {
            file: self.file.try_clone()?,
            tracked_size: Cell::new(self.tracked_size.get()),
            path: self.path.clone(), // preserve path for create_related/open_related
        })
    }

    async fn pread(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut buf = vec![0u8; len];
        self.file.read_exact_at(&mut buf, offset)?;
        Ok(buf)
    }

    async fn pread_into(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        self.file.read_exact_at(buf, offset)?;
        Ok(())
    }

    async fn pwrite(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        self.file.write_all_at(data, offset)?;
        // Update tracked size if this write extends the file
        let end = offset + data.len() as u64;
        if end > self.tracked_size.get() {
            self.tracked_size.set(end);
        }
        Ok(())
    }

    async fn append(&self, data: &[u8]) -> io::Result<u64> {
        use std::os::unix::fs::FileExt;
        let offset = self.tracked_size.get();
        self.file.write_all_at(data, offset)?;
        self.tracked_size.set(offset + data.len() as u64);
        Ok(offset)
    }

    async fn sync(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    async fn size(&self) -> io::Result<u64> {
        Ok(self.tracked_size.get())
    }

    async fn truncate(&self, new_size: u64) -> io::Result<()> {
        self.file.set_len(new_size)?;
        self.tracked_size.set(new_size);
        Ok(())
    }

    async fn open_related(&self, name: &str) -> io::Result<Self> {
        let path = self.related_path(name)?;
        StdBlobIO::open(&path)
    }

    async fn create_related(&self, name: &str) -> io::Result<Self> {
        let path = self.related_path(name)?;
        StdBlobIO::create(&path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        futures::executor::block_on(f)
    }

    #[test]
    fn test_mem_blob_io_basic() {
        block_on(async {
            let io = MemBlobIO::new();
            assert_eq!(io.size().await.unwrap(), 0);

            // Append some data
            let offset = io.append(b"hello").await.unwrap();
            assert_eq!(offset, 0);
            assert_eq!(io.size().await.unwrap(), 5);

            // Append more
            let offset2 = io.append(b" world").await.unwrap();
            assert_eq!(offset2, 5);
            assert_eq!(io.size().await.unwrap(), 11);

            // Read back
            assert_eq!(io.pread(0, 5).await.unwrap(), b"hello");
            assert_eq!(io.pread(5, 6).await.unwrap(), b" world");
            assert_eq!(io.pread(0, 11).await.unwrap(), b"hello world");

            // Overwrite
            io.pwrite(5, b"_WORLD").await.unwrap();
            assert_eq!(io.pread(0, 11).await.unwrap(), b"hello_WORLD");
        });
    }

    #[test]
    fn test_mem_blob_io_pread_out_of_bounds() {
        block_on(async {
            let io = MemBlobIO::new();
            assert!(io.pread(0, 1).await.is_err());
        });
    }

    #[test]
    fn test_std_blob_io_basic() {
        block_on(async {
            let dir = std::env::temp_dir().join("larkblob_test");
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join("test_basic.blob");

            // Create and write
            {
                let io = StdBlobIO::create(&path).unwrap();
                assert_eq!(io.size().await.unwrap(), 0);

                let offset = io.append(b"hello").await.unwrap();
                assert_eq!(offset, 0);
                assert_eq!(io.size().await.unwrap(), 5);

                let offset2 = io.append(b" world").await.unwrap();
                assert_eq!(offset2, 5);

                // Read back
                assert_eq!(io.pread(0, 5).await.unwrap(), b"hello");
                assert_eq!(io.pread(0, 11).await.unwrap(), b"hello world");

                // Overwrite
                io.pwrite(5, b"_WORLD").await.unwrap();
                assert_eq!(io.pread(0, 11).await.unwrap(), b"hello_WORLD");
            }

            // Re-open and verify
            {
                let io = StdBlobIO::open(&path).unwrap();
                assert_eq!(io.pread(0, 11).await.unwrap(), b"hello_WORLD");
            }

            std::fs::remove_file(&path).ok();
        });
    }

    #[test]
    fn test_std_blob_io_roundtrip() {
        block_on(async {
            use crate::arc_value::ArcValue;
            use crate::session::BlobSession;
            use crate::writer::write_blob;
            use serde_json::json;

            let dir = std::env::temp_dir().join("larkblob_test");
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join("test_roundtrip.blob");

            let tree = ArcValue::from_value(json!({
                "characters": {
                    "abc": {"hp": 100, "name": "Hero"},
                },
                "config": {"mode": "dark"}
            }));

            // Write blob to file
            {
                let io = StdBlobIO::create(&path).unwrap();
                write_blob(&io, &tree).await.unwrap();
            }

            // Read back from file
            {
                let io = StdBlobIO::open(&path).unwrap();
                let session = BlobSession::open(io).await.unwrap();
                let result = session.read_subtree(&[]).await.unwrap();
                assert_eq!(result, tree);
            }

            std::fs::remove_file(&path).ok();
        });
    }
}
