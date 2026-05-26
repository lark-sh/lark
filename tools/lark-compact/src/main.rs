//! One-shot blob compaction for a single Lark database directory.
//!
//! ```text
//! lark-compact <database-dir>
//! ```
//!
//! Defragments the blob, absorbs pending dictionary keys, writes a clean
//! sidecar, and bumps the generation counter so any running server picks
//! up the new file on its next compaction tick.
//!
//! Used by `chaos-monkey` and by operators who want to manually trigger
//! a compaction (e.g. after a large bulk import). Lark normally runs
//! incremental compaction in-process inside `lark-server`; this binary
//! is for the cases where you want to force a full root-level rewrite
//! without restarting the server.

use lark_blob::BlobSession;
use lark_blob::io::StdBlobIO;
use lark_blob::segment::Sidecar;

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// block_on — noop-waker single-poll (StdBlobIO never returns Pending)
// ---------------------------------------------------------------------------

fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    fn noop_raw_waker() -> RawWaker {
        fn no_op(_: *const ()) {}
        fn clone(_: *const ()) -> RawWaker {
            noop_raw_waker()
        }
        RawWaker::new(
            std::ptr::null(),
            &RawWakerVTable::new(clone, no_op, no_op, no_op),
        )
    }

    let waker = unsafe { Waker::from_raw(noop_raw_waker()) };
    let mut cx = Context::from_waker(&waker);
    let mut fut = std::pin::pin!(fut);
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(val) => val,
        Poll::Pending => panic!("block_on: unexpected Pending from sync BlobIO"),
    }
}

// ---------------------------------------------------------------------------
// File-layout helpers
// ---------------------------------------------------------------------------

fn blob_path(dir: &Path) -> PathBuf {
    dir.join("blob.lark")
}

fn sidecar_path(dir: &Path) -> PathBuf {
    dir.join("sidecar.lark")
}

fn read_blob_generation(dir: &Path) -> u64 {
    match std::fs::read_to_string(dir.join("blob.generation")) {
        Ok(s) => s.trim().parse::<u64>().unwrap_or(0),
        Err(_) => 0,
    }
}

fn write_blob_generation(dir: &Path, generation: u64) -> Result<(), String> {
    std::fs::write(dir.join("blob.generation"), generation.to_string())
        .map_err(|e| format!("failed to write blob.generation: {}", e))
}

// ---------------------------------------------------------------------------
// Compaction
// ---------------------------------------------------------------------------

struct CompactResult {
    elapsed: Duration,
}

fn compact_database(db_dir: &Path) -> Result<Option<CompactResult>, String> {
    let compacting_marker = db_dir.join(".compacting");

    // Marker tells any in-process StorageWorker on the same database to
    // skip its incremental pass while we hold the blob.
    std::fs::write(&compacting_marker, b"")
        .map_err(|e| format!("failed to create .compacting: {}", e))?;

    let result = compact_database_inner(db_dir);

    let _ = std::fs::remove_file(&compacting_marker);

    result
}

fn compact_database_inner(db_dir: &Path) -> Result<Option<CompactResult>, String> {
    let bp = blob_path(db_dir);
    if !bp.exists() {
        return Ok(None);
    }

    let start = Instant::now();
    let current_gen = read_blob_generation(db_dir);

    // Open BlobSession with sidecar.
    let blob_io =
        StdBlobIO::open(&bp).map_err(|e| format!("failed to open {}: {}", bp.display(), e))?;

    let sp = sidecar_path(db_dir);
    let sidecar_io = if sp.exists() {
        Some(StdBlobIO::open(&sp).map_err(|e| format!("failed to open sidecar: {}", e))?)
    } else {
        None
    };

    let mut session = block_on(BlobSession::open_with_sidecar(blob_io, sidecar_io.as_ref()))
        .map_err(|e| format!("failed to open BlobSession: {}", e))?;

    // Root compaction (defrags blob, absorbs pending dictionary keys)
    // into a temporary file we can atomically swap into place.
    let blob_tmp = db_dir.join("blob.lark.tmp");
    let dst = StdBlobIO::create(&blob_tmp)
        .map_err(|e| format!("failed to create {}: {}", blob_tmp.display(), e))?;

    let old_io = block_on(session.root_compact(dst)).map_err(|e| {
        let _ = std::fs::remove_file(&blob_tmp);
        format!("root_compact failed: {}", e)
    })?;
    drop(old_io);

    // Sidecar reflects post-compaction state (empty free list, etc.).
    let sidecar_write_io = match sidecar_io {
        Some(io) => io,
        None => StdBlobIO::create(&sp).map_err(|e| format!("failed to create sidecar: {}", e))?,
    };
    block_on(session.apply_updates_with_sidecar(&[], Some(&sidecar_write_io)))
        .map_err(|e| format!("failed to write sidecar: {}", e))?;

    // Rename dance: blob.lark.tmp → blob.lark, keeping blob.old.lark as a
    // recovery point until the swap completes successfully.
    let blob_old = db_dir.join("blob.old.lark");
    std::fs::rename(&bp, &blob_old).map_err(|e| {
        let _ = std::fs::remove_file(&blob_tmp);
        format!("rename blob.lark -> blob.old.lark failed: {}", e)
    })?;
    std::fs::rename(&blob_tmp, &bp).map_err(|e| {
        let _ = std::fs::rename(&blob_old, &bp);
        format!("rename blob.lark.tmp -> blob.lark failed: {}", e)
    })?;
    let _ = std::fs::remove_file(&blob_old);

    // Bump generation. Any running server's StorageWorker checks this
    // value and reopens the BlobSession when it advances.
    write_blob_generation(db_dir, current_gen + 1)?;

    Ok(Some(CompactResult {
        elapsed: start.elapsed(),
    }))
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

fn print_usage() {
    eprintln!("Usage: lark-compact <database-dir>");
    eprintln!();
    eprintln!("Defragments the blob in <database-dir>, writes a clean sidecar,");
    eprintln!("and bumps blob.generation. Pairs with lark-server's in-process");
    eprintln!("compaction — the server picks up the new blob on its next tick.");
}

/// Look in `Sidecar::bytes_wasted` and stat'd file size to print a quick
/// post-run summary. Not used for any branching logic — `compact_database`
/// always runs a full pass.
fn print_pre_stats(db_dir: &Path) {
    let bp = blob_path(db_dir);
    let size = std::fs::metadata(&bp).map(|m| m.len()).unwrap_or(0);
    let sp = sidecar_path(db_dir);
    let wasted = std::fs::read(&sp)
        .ok()
        .and_then(|d| Sidecar::from_bytes(&d).ok())
        .map(|sc| sc.free_list.bytes_wasted)
        .unwrap_or(0);
    eprintln!(
        "  before: {:.1} MB blob, {:.1} MB wasted ({:.0}%)",
        size as f64 / 1024.0 / 1024.0,
        wasted as f64 / 1024.0 / 1024.0,
        if size > 0 {
            wasted as f64 / size as f64 * 100.0
        } else {
            0.0
        },
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let db_dir = match args.get(1) {
        Some(s) if s == "--help" || s == "-h" => {
            print_usage();
            std::process::exit(0);
        }
        Some(d) => PathBuf::from(d),
        None => {
            print_usage();
            std::process::exit(1);
        }
    };

    if !db_dir.exists() {
        eprintln!("error: directory does not exist: {}", db_dir.display());
        std::process::exit(1);
    }

    eprintln!("compacting {}", db_dir.display());
    print_pre_stats(&db_dir);

    match compact_database(&db_dir) {
        Ok(Some(result)) => {
            eprintln!("done in {:.1}s", result.elapsed.as_secs_f64());
        }
        Ok(None) => {
            eprintln!("skipped (no blob.lark in {})", db_dir.display());
        }
        Err(e) => {
            eprintln!("error: {}", e);
            std::process::exit(1);
        }
    }
}
