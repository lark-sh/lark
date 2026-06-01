# Backup & Restore

Lark is designed so that backing up your data is just **copying files**, and
restoring is **putting them back and starting the server**. This
document explains what to copy, the one best-practice step that makes a copy
cleanly consistent, and why it's safe.

## What makes up a backup

A complete Lark backup has two parts:

1. **lark-server data** — the per-database on-disk state under `LARK_DATA_DIR`.
   This is the actual database contents (your application data).
2. **lark-edge metadata** — the control-plane store that holds projects, admin
   users, database routing, and per-project settings (e.g. `firebase-project-id`,
   project secrets). This is SQLite (`lark.db`) by default, or Postgres.

Backing up only one of the two gives you an incomplete restore. The database
contents without the edge metadata leaves a server that doesn't know which
projects/databases exist or how to authenticate them; the edge metadata without
the database contents leaves correctly-configured but empty databases.

### lark-server data layout

`LARK_DATA_DIR` contains one directory per database, keyed by project and
database id:

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

To back up everything, copy the whole `LARK_DATA_DIR` tree. To back up a single
database, copy just its `{project}/{database}/` directory.

## The procedure

### Recommended: marker-bracketed copy

For each database directory you're copying, drop a `.compacting` marker file in
it for the duration of the copy:

```bash
DB_DIR="$LARK_DATA_DIR/my-project/my-database"

# 1. Freeze incremental compaction for this database.
touch "$DB_DIR/.compacting"

# 2. Copy the database files. Order doesn't matter — they're now static
#    (the WAL keeps appending, but appends are always safe to copy).
cp -a "$DB_DIR" /path/to/backup/my-project/

# 3. Resume compaction.
rm "$DB_DIR/.compacting"
```

While `.compacting` is present, lark-server's storage worker skips its
incremental compaction pass entirely (`server/src/storage/worker.rs`), so
`blob.lark`, `sidecar.lark`, `blob.generation`, and `sequence` do not change
during the copy. Live writes continue uninterrupted — they land in the WAL,
which is append-only and safe to copy at any time. **Remember to remove the
marker afterward**; if it's left behind, compaction never resumes and the WAL
grows unbounded. (This is the same mechanism `lark-compact` uses while it holds
the blob for a full re-compaction.)

To snapshot the whole deployment when there are many databases, it is recommended
to place a `.compacting` marker in each database directory before copying, 
then remove it after copying that database folder.

### Alternative: filesystem snapshot

If `LARK_DATA_DIR` lives on snapshot-capable storage (ZFS, LVM, btrfs, EBS), an
atomic filesystem snapshot captures a consistent point-in-time copy of every
file at once. In that case the `.compacting` marker is optional — the snapshot is
already internally consistent. This is the simplest option for larger
deployments that already use snapshot-capable volumes.

### Without the marker

Even a plain `cp -a` of a live database directory restores to a valid (if
slightly stale) database — see [Data correctness guarantee](#data-correctness-guarantee) below. The
`.compacting` marker just removes the "slightly stale" window and the small risk
of free-list skew, so it's the recommended default. Filesystem snapshots and the
marker are both ways to get a *clean* point-in-time copy; the plain copy is the
fallback when neither is available.

### Backing up lark-edge metadata

- **SQLite (default):** the database lives in lark-edge's data volume (e.g.
  `/data/lark.db` in the bundled docker-compose). Back it up with the SQLite
  online backup API or `sqlite3 lark.db ".backup '/path/to/backup/lark.db'"`,
  which is safe against a running edge. A plain file copy works too if the edge
  is quiesced.
- **Postgres:** use your normal Postgres backup tooling (`pg_dump`, base backups,
  or managed-service snapshots).

## Restore

1. Stop lark-server (and lark-edge, if restoring its metadata too).
2. Restore the lark-edge metadata store (`lark.db` or the Postgres database).
3. Copy the backed-up `LARK_DATA_DIR` tree (or individual database directories)
   into place. Ensure no `.compacting` markers were included in the backup — if
   any slipped in, delete them.
4. Start lark-server.

On startup, lark-server opens each `blob.lark`, reads its `blob.generation` and
`sequence`, and replays any WAL entries newer than `sequence` forward
(`load_from_disk` / `load_wal_entries` in `server/src/db/database/persistence.rs`). **Restore
uses the exact same path as a normal startup** — there is no separate recovery
mode. A backup is just a database that hasn't been opened yet.

## Data correctness guarantee

Copying `blob.lark` while the server is running cannot produce a corrupt file,
because of how the blob is written:

1. **Data is durable before it's referenced.** When the blob is updated, newly
  appended data is fsynced to disk *before* the header that points to it is
  written, and the header is fsynced after
  (`flush_write_back` in `blob/src/cached_io.rs`). So a copy taken at any instant
  sees one of two valid states: the old header (in which case freshly-appended
  bytes are simply unreferenced waste) or the new header (whose data is already
  on disk). It never sees a header pointing at data that isn't there.

2. **A stale sidecar costs only space, never correctness.** `sidecar.lark` holds
  the free list and pending dictionary keys. Reads never depend on it — key bytes
  live inline in the blob itself; the sidecar only drives free-space reuse and
  dictionary deduplication during the next compaction. A sidecar that's slightly
  out of sync with the blob just means some reclaimable space goes untracked and
  some keys miss deduplication until the next full compaction.

3. **The WAL is append-only.** WAL files are only ever appended to and rotated, so
  copying them live is always safe. Anything in the WAL but not yet in the blob
  is replayed on the next open.

The `.compacting` marker turns "slightly stale but valid" into "exactly
consistent" by ensuring `blob.lark`/`sidecar.lark`/`sequence` don't move at all
during the copy. See the [Storage section of CONTRIBUTING](../CONTRIBUTING.md#storage)
for the full on-disk design and [WRITE_LIFECYCLE.md](WRITE_LIFECYCLE.md) for how
writes flow from client to disk.
