# Backup & Restore

Lark is designed so that backing up your data is just **copying files**, and restoring is **putting them back and starting the server**. This document explains what to copy, the one best-practice step that makes a copy cleanly consistent, and why it's safe.

## What makes up a backup

A complete Lark backup has two parts:

1. **lark-server data**: the per-database on-disk state under `LARK_DATA_DIR`. This is the actual database contents (your application data).
2. **lark-edge metadata**: the control-plane store that holds projects, admin users, database routing, and per-project settings (e.g. `firebase-project-id`, project secrets). This is SQLite (`lark.db`) by default, or Postgres.

Backing up only one of the two gives you an incomplete restore. The database contents without the edge metadata leaves a server that doesn't know which projects/databases exist or how to authenticate them; the edge metadata without the database contents leaves correctly-configured but empty databases.

### lark-server data layout

`LARK_DATA_DIR` contains one directory per database, keyed by project and database id:

```
{LARK_DATA_DIR}/
└── {project}/
    └── {database}/
        ├── blob.lark          # Main blob file (compact on-disk representation)
        ├── blob.generation    # u64 in text — bumped on full re-compaction (only exists after first full compaction)
        ├── sidecar.lark        # Free list + pending dictionary keys (only exists after first incremental compaction)
        ├── sequence            # Last WAL sequence applied to the blob (only exists after first incremental compaction)
        └── wal/
            ├── 000001.wal
            └── ...
```

To back up everything, copy the whole `LARK_DATA_DIR` tree. To back up a single database, copy just its `{project}/{database}/` directory.

## The procedure

### Recommended: marker-bracketed copy

For each database directory you're copying, drop a `.compacting` marker file in it for the duration of the copy:

```bash
DB_DIR="$LARK_DATA_DIR/my-project/my-database"

# 1. Freeze incremental compaction for this database, and wait for any batch
#    already running to finish.
touch "$DB_DIR/.compacting"
flock "$DB_DIR/.storage.lock" true

# 2. Copy the database files. The blob, sidecar and sequence are static now.
#    The WAL keeps growing, so the copy can end partway through a line.
cp -a "$DB_DIR" /path/to/backup/my-project/

# 3. Resume compaction.
rm "$DB_DIR/.compacting"
```

While `.compacting` is present, lark-server's storage worker skips its incremental compaction pass entirely (`server/src/storage/worker.rs`), and a batch that started before the marker holds `.storage.lock` until it finishes, so once `flock` returns, `blob.lark`, `sidecar.lark`, `blob.generation`, and `sequence` do not change during the copy. Live writes continue uninterrupted: they land in the WAL, and a restored database replays anything in it that the blob doesn't hold yet. **Remember to remove the marker afterward**; if it's left behind, compaction never resumes and the WAL grows unbounded. After an hour, lark-server logs a warning for a marker it's still skipping. (This is the same mechanism `lark-compact` uses while it holds the blob for a full re-compaction.)

To snapshot the whole deployment when there are many databases, place a `.compacting` marker in each database directory before copying, then remove it after copying that database's folder.

### Alternative: filesystem snapshot

If `LARK_DATA_DIR` lives on snapshot-capable storage (ZFS, LVM, btrfs, EBS), an atomic filesystem snapshot captures a consistent point-in-time copy of every file at once. In that case the `.compacting` marker is optional; the snapshot is already internally consistent. This is the simplest option for larger deployments that already use snapshot-capable volumes.

### Without the marker

Don't rely on a plain copy of a live database. A compaction batch can change files while they're being copied, so the blob and `sequence` can end up out of step, and the restored database can be damaged or missing writes without any warning. Use the marker, or a filesystem snapshot.

### Backing up lark-edge metadata

- **SQLite (default):** the database lives in lark-edge's data volume (e.g. `/data/lark.db` in the bundled docker-compose). Back it up with the SQLite online backup API or `sqlite3 lark.db ".backup '/path/to/backup/lark.db'"`, which is safe against a running edge. A plain file copy works too if the edge is quiesced.
- **Postgres:** use your normal Postgres backup tooling (`pg_dump`, base backups, or managed-service snapshots).

## Restore

1. Stop lark-server (and lark-edge, if restoring its metadata too).
2. Restore the lark-edge metadata store (`lark.db` or the Postgres database).
3. Copy the backed-up `LARK_DATA_DIR` tree (or individual database directories) into place. Ensure no `.compacting` markers were included in the backup; if any slipped in, delete them.
4. Start lark-server.
5. If a database refuses to load, its newest WAL file probably ends partway through a line, since the copy was taken while it was being written. Follow [If a database refuses to load](../TESTING.md#if-a-database-refuses-to-load). A filesystem snapshot can leave the same thing.

On startup, lark-server opens each `blob.lark`, reads its `blob.generation` and `sequence`, and replays any WAL entries newer than `sequence` forward (`load_from_disk` / `load_wal_entries` in `server/src/db/database/persistence.rs`). **Restore uses the exact same path as a normal startup**; there is no separate recovery mode. A backup is just a database that hasn't been opened yet.

## Why this works

The marker and lock stop compaction, so the blob, sidecar and `sequence` all hold still and match each other during the copy. The WAL is the only file still changing, and a restored database replays anything in it that the blob doesn't hold yet. Include the sidecar in every copy: it's only free-space bookkeeping unless the database crashed during a compaction, when it holds what finishes that batch.

See the [Storage section of CONTRIBUTING](../CONTRIBUTING.md#storage) for the full on-disk design and [WRITE_LIFECYCLE.md](WRITE_LIFECYCLE.md) for how writes flow from client to disk.
