//! Journal of the file operations that decide what survives a crash.
//!
//! With the `io-trace` feature, every durable-storage call in `fsync.rs` and
//! `glommio_blob_io.rs` is recorded, so `server/tests/integration_crash.rs` can
//! rebuild what a crash at any point could leave on disk. Without the feature
//! these functions are empty and compile away.
//!
//! The journal belongs to the calling thread. A Glommio executor runs on one
//! thread, so a test's Database and StorageWorker share it, and tests running in
//! parallel don't see each other's operations.

/// One recorded operation. Paths are relative to the directory being traced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    /// A file is opened with create. `truncate` also empties an existing file.
    Create {
        path: String,
        truncate: bool,
    },
    Write {
        path: String,
        offset: u64,
        data: Vec<u8>,
    },
    Truncate {
        path: String,
        size: u64,
    },
    /// `fdatasync`: the file's data and size are on the device.
    Sync {
        path: String,
    },
    /// `fsync` of a directory: its entries are on the device.
    SyncDir {
        path: String,
    },
    Rename {
        from: String,
        to: String,
    },
    Remove {
        path: String,
    },
    /// A test is about to send a write to the server.
    WriteIssued,
    /// The server acknowledged a write.
    WriteAcked,
}

#[cfg(feature = "io-trace")]
mod enabled {
    use super::Op;
    use std::cell::{Cell, RefCell};
    use std::path::{Path, PathBuf};

    struct Recorder {
        root: PathBuf,
        ops: Vec<Op>,
    }

    thread_local! {
        static RECORDER: RefCell<Option<Recorder>> = const { RefCell::new(None) };
        static WAL_ROTATION_SIZE: Cell<Option<u64>> = const { Cell::new(None) };
        static DURABILITY: Cell<Option<(u64, bool)>> = const { Cell::new(None) };
    }

    /// Starts recording operations on paths under `root`.
    pub fn start(root: &Path) {
        // The root may be reached through a symlink (/tmp on macOS, for one).
        let root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
        RECORDER.with_borrow_mut(|r| {
            *r = Some(Recorder {
                root,
                ops: Vec::new(),
            })
        });
    }

    /// Stops recording and returns the journal.
    pub fn take() -> Vec<Op> {
        RECORDER
            .with_borrow_mut(|r| r.take())
            .map(|r| r.ops)
            .unwrap_or_default()
    }

    /// Rotates WAL files at `bytes` on this thread, so a test gets many
    /// compactions from little data.
    pub fn set_wal_rotation_size(bytes: u64) {
        WAL_ROTATION_SIZE.set(Some(bytes));
    }

    pub(crate) fn wal_rotation_size() -> Option<u64> {
        WAL_ROTATION_SIZE.get()
    }

    /// Overrides `WAL_SYNC_INTERVAL_MS` and `FSYNC_ON_WAL_FLUSH` on this thread,
    /// so tests in one process can run different durability modes side by side.
    pub fn set_durability(wal_sync_interval_ms: u64, fsync_on_wal_flush: bool) {
        DURABILITY.set(Some((wal_sync_interval_ms, fsync_on_wal_flush)));
    }

    pub(crate) fn durability() -> Option<(u64, bool)> {
        DURABILITY.get()
    }

    /// Records a marker that isn't tied to a path.
    pub fn mark(op: Op) {
        RECORDER.with_borrow_mut(|r| {
            if let Some(r) = r {
                r.ops.push(op);
            }
        });
    }

    /// `path` relative to the traced root, or None if it's outside it.
    fn relative(root: &Path, path: &Path) -> Option<String> {
        // A new file can't be canonicalized yet, but its parent can.
        let full = match (path.parent(), path.file_name()) {
            (Some(parent), Some(name)) => parent
                .canonicalize()
                .unwrap_or_else(|_| parent.to_path_buf())
                .join(name),
            _ => path.to_path_buf(),
        };
        if full == root {
            return Some(String::new());
        }
        let relative = full.strip_prefix(root).ok()?;
        Some(relative.to_string_lossy().into_owned())
    }

    fn with_path(path: &Path, build: impl FnOnce(String) -> Op) {
        RECORDER.with_borrow_mut(|r| {
            let Some(r) = r else { return };
            if let Some(relative) = relative(&r.root, path) {
                r.ops.push(build(relative));
            }
        });
    }

    pub(crate) fn create(path: &Path, truncate: bool) {
        with_path(path, |path| Op::Create { path, truncate });
    }

    pub(crate) fn write(path: &Path, offset: u64, data: &[u8]) {
        with_path(path, |path| Op::Write {
            path,
            offset,
            data: data.to_vec(),
        });
    }

    pub(crate) fn truncate(path: &Path, size: u64) {
        with_path(path, |path| Op::Truncate { path, size });
    }

    pub(crate) fn sync(path: &Path) {
        with_path(path, |path| Op::Sync { path });
    }

    pub(crate) fn sync_dir(path: &Path) {
        with_path(path, |path| Op::SyncDir { path });
    }

    pub(crate) fn rename(from: &Path, to: &Path) {
        RECORDER.with_borrow_mut(|r| {
            let Some(r) = r else { return };
            if let (Some(from), Some(to)) = (relative(&r.root, from), relative(&r.root, to)) {
                r.ops.push(Op::Rename { from, to });
            }
        });
    }

    pub(crate) fn remove(path: &Path) {
        with_path(path, |path| Op::Remove { path });
    }
}

#[cfg(feature = "io-trace")]
pub(crate) use enabled::{
    create, durability, remove, rename, sync, sync_dir, truncate, wal_rotation_size, write,
};
#[cfg(feature = "io-trace")]
pub use enabled::{mark, set_durability, set_wal_rotation_size, start, take};

#[cfg(not(feature = "io-trace"))]
mod disabled {
    use std::path::Path;

    #[inline(always)]
    pub(crate) fn wal_rotation_size() -> Option<u64> {
        None
    }
    #[inline(always)]
    pub(crate) fn durability() -> Option<(u64, bool)> {
        None
    }
    #[inline(always)]
    pub(crate) fn create(_: &Path, _: bool) {}
    #[inline(always)]
    pub(crate) fn write(_: &Path, _: u64, _: &[u8]) {}
    #[inline(always)]
    pub(crate) fn truncate(_: &Path, _: u64) {}
    #[inline(always)]
    pub(crate) fn sync(_: &Path) {}
    #[inline(always)]
    pub(crate) fn sync_dir(_: &Path) {}
    #[inline(always)]
    pub(crate) fn rename(_: &Path, _: &Path) {}
    #[inline(always)]
    pub(crate) fn remove(_: &Path) {}
}

#[cfg(not(feature = "io-trace"))]
pub(crate) use disabled::{
    create, durability, remove, rename, sync, sync_dir, truncate, wal_rotation_size, write,
};
