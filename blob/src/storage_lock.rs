//! The lock that keeps writers of a database's blob files apart.
//!
//! The storage worker in `lark-server` and `lark-compact` are separate processes
//! that both change `blob.lark`, `sidecar.lark` and `sequence`. Each holds this
//! lock for as long as it's changing them. It's a `flock` on a file in the database
//! directory, so the kernel drops it if the holder dies.
//!
//! The `.compacting` marker is separate. It says the blob shouldn't change, and it
//! outlives a crash on purpose. A process that sets it takes this lock afterwards,
//! to wait out a batch that started before the marker existed.

use std::fs::{File, TryLockError};
use std::io;
use std::path::Path;

/// File in a database directory that the lock is taken on.
pub const STORAGE_LOCK_FILE: &str = ".storage.lock";

fn open_lock_file(db_dir: &Path) -> io::Result<File> {
    File::options()
        .create(true)
        .truncate(false)
        .write(true)
        .open(db_dir.join(STORAGE_LOCK_FILE))
}

/// Takes the lock if it's free, without blocking, so it's safe on an async
/// executor. Returns `None` if something else holds it. Dropping the file releases
/// it.
pub fn try_lock_storage(db_dir: &Path) -> io::Result<Option<File>> {
    let file = open_lock_file(db_dir)?;
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(TryLockError::WouldBlock) => Ok(None),
        Err(TryLockError::Error(e)) => Err(e),
    }
}

/// Takes the lock, blocking until it's free. Dropping the file releases it.
pub fn lock_storage(db_dir: &Path) -> io::Result<File> {
    let file = open_lock_file(db_dir)?;
    file.lock()?;
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_held_lock_refuses_others_until_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let held = try_lock_storage(dir.path()).unwrap();
        assert!(held.is_some());
        assert!(try_lock_storage(dir.path()).unwrap().is_none());
        drop(held);
        assert!(try_lock_storage(dir.path()).unwrap().is_some());
    }

    #[test]
    fn test_lock_blocks_until_released() {
        let dir = tempfile::tempdir().unwrap();
        let held = lock_storage(dir.path()).unwrap();
        let path = dir.path().to_path_buf();
        let waiter = std::thread::spawn(move || lock_storage(&path).map(|_| ()));
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(!waiter.is_finished());
        drop(held);
        waiter.join().unwrap().unwrap();
    }
}
