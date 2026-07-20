# LarkBlob: JSON-Optimized Single-Blob Storage Engine

## Background

### Referenced Reading

This design is influenced by Oracle's OSON binary format, described in:

> Z. Hua Liu et al. "Native JSON Datatype Support: Maturing SQL and NoSQL convergence in Oracle Database." PVLDB, 13(12): 3059-3071, 2020.

### Key Concepts

1. **Dictionary**: deduplicated field names, stored once per document, referenced by small integer IDs. Binary search via sorted hash codes for O(log n) field lookup.

2. **Tree-structured binary with jump offsets**: objects store a sorted field ID array and an offset array. Navigate to any child without scanning siblings. (Legacy array nodes store element offsets for O(1) indexed access; arrays are no longer written, as noted under Node Encoding.)

3. **Variable-size field IDs**: field ID size adapts based on distinct key count, either 1 byte (<256 keys), 2 bytes (<65536), or 4 bytes. Size is recorded in the header. Most documents use 1-byte field IDs, saving significant space in child index arrays.

4. **Delegate offsets for shared field structures**: objects with identical field name sets can share their sorted field ID array via a `delegate_offset` instead of repeating it. This is significant for collections of structurally identical objects (e.g., thousands of game characters all having the same fields).

5. **Depth-first layout**: children are contiguous after their parent, so reading an entire subtree is one contiguous read.

6. **Forward-offset partial updates**: when updating a value that doesn't fit in-place, tombstone the old location (write a forward pointer) and append the new value at EOF. Critically, **no forwarding chains**: subsequent updates overwrite the original tombstone's forward pointer. Re-compact when accumulated waste exceeds a threshold.

7. **Three low-level operations**: all partial updates decompose to (a) length-preserved byte replacement, (b) append at EOF, (c) truncate at EOF. These map directly to POSIX `pwrite()`, `write()`, and `ftruncate()`.

8. **Lazy block-level reading**: for large documents stored as BLOBs, data blocks are lazily read and cached in the buffer cache based on tree navigation patterns instead of linearly reading everything.

## Binary Format

### Overall Structure

```
+------------------------------------------+
| Header (64 bytes)                        |
|   magic: "LARK" (4 bytes)               |
|   version: u16                           |
|   flags: u16                             |
|     bits 0-1: field_id_size             |
|       0b00 = u8, 0b01 = u16, 0b10 = u32 |
|   dict_offset: u64                       |
|   root_offset: u64                       |
|   node_count: u64                        |
|   total_size: u64                        |
|   dict_field_count: u32                  |
|   reserved: [u8; 20]                     |
+------------------------------------------+
| Dictionary                               |
|   See "Dictionary" section below         |
+------------------------------------------+
| Tree (depth-first serialized nodes)      |
|   Root object node                       |
|     +-- Child A (contiguous subtree)     |
|     |     +-- Grandchild A1              |
|     |     +-- Grandchild A2              |
|     +-- Child B (contiguous subtree)     |
|     +-- Child C ...                      |
+------------------------------------------+
| Extended Tree (appended partial updates) |
|   New values from incremental compaction |
|   Forward pointers in tree point here    |
+------------------------------------------+
```

### Node Encoding

Every node starts with a 1-byte type tag:

```
Array:      0x02  [subtree_size:u64] [elem_count:u32] [appended_bytes:u32]   (LEGACY — read-only, see below)
                  [elem_index: (type_flags:u8, offset:u64, size:u64) x elem_count]
                  [elements: contiguous depth-first values]

String:     0x03  [len:u32] [utf8_bytes]
Number:     0x04  [f64]  (8 bytes, IEEE 754)
Bool:       0x05  [u8]   (0x00 = false, 0x01 = true)
Null:       0x06

Collection: 0x08  [subtree_size:u64] [child_count:u32] [reserved_count:u32]
                  [key_data_used:u32] [key_data_reserved:u32] [appended_bytes:u32]
                  [child_index: (key_hash:u64, type_flags:u8, offset:u64, size:u64) x child_count]
                  [reserved_index_slots: zeroed x reserved_count]
                  [key_string_area: (key_len:u16, key_bytes_or_dict_id) x child_count,
                   then zeroed reserved space to fill key_data_reserved bytes]
                  [children: contiguous depth-first subtrees]
```

Container nodes have a fixed header followed by a sorted child index and a children area:
- **Array header**: 17 bytes (`ARRAY_HEADER_SIZE`). Element index entries are 17 bytes each (`ARRAY_INDEX_ENTRY_SIZE`).
- **Collection header**: 29 bytes (`COLLECTION_HEADER_SIZE`). Child index entries are 25 bytes each (`COLLECTION_INDEX_ENTRY_SIZE`).

All containers, JSON objects **and arrays** alike, use **TYPE_COLLECTION**. Arrays carry no native type: they are stored as integer-keyed collections (`{"0":…,"1":…}`) and rendered back as JSON arrays only at the read/wire boundary (see the array contract in `arc_value.rs`). **`TYPE_ARRAY` (0x02) is legacy and read-only**: the writer never emits it, but the reader and compactor still decode pre-existing array nodes, converting them to integer-keyed objects on read. Such nodes persist on disk (compaction preserves the tag) until the path is next written, at which point they're rewritten as `TYPE_COLLECTION`.

Some collections happen to have only dictionary-resolvable keys (structural fields like `"name"`, `"score"`) and some have push-ID keys (`"-Mabc123"`); the distinction is handled at the key-encoding level (see below).

**Type tag in the index entry, not at child offset.** Each entry in a parent's `child_index` / `elem_index` carries the child's `type_flags:u8`, `offset:u64`, and `size:u64`. The child's offset is found *without* reading the child first. This means type+size live in the parent index, so navigation never reads a child node just to learn its type/size.

**Forwarding.** When an in-place update doesn't fit, the new value is appended at EOF and the parent's index entry has its `TYPE_FLAGS_FORWARDED` bit (`0x80`) set, with `offset` switched to an absolute file offset. There is no separate "forward" node type: the redirect lives entirely in the parent's index, so there are never multi-hop forwarding chains.

**Key encoding (Collection):** Each entry in the key_string_area is either an inline key string (`key_len:u16` with the high bit clear, followed by `key_len` UTF-8 bytes) or a dictionary reference (`key_len:u16` with `KEY_DICT_FLAG = 0x8000` set; the remaining 15 bits are the field_id, no inline bytes follow). The hot path for structural keys keeps them deduplicated via the dictionary; entity-ID keys are stored inline.

**Reserved space for in-place inserts:** `reserved_count` extra index slots and `key_data_reserved` extra key-string bytes let a Collection accept new children via pwrite into pre-allocated space, without rewriting the subtree. The sizing function `compute_reserved_count(child_count, total_children_size, has_push_id_keys)` reserves generously for large collections (≥1 MB → max(40, child_count/2)), moderately for medium (≥10 KB → max(20, child_count/4)), and only when needed (push-ID keys) for small ones. When reserved space is exhausted, the collection is re-serialized at EOF with fresh reserved space proportional to its new size.

**field_id_size** (in header flags, bits 0–1): `0 = u8`, `1 = u16`.

**Offsets** (`offset` in index entries) are always **u64**. Unlike OSON (which adapts offset size per-document for KB-MB documents), LarkBlob targets blobs up to hundreds of GB.

**subtree_size** (on Array/Collection): Total byte count of this node including all descendants (u64). Enables:
- Read entire subtree as one contiguous I/O: just read `subtree_size` bytes.
- Skip entire subtree without scanning: advance by `subtree_size` bytes.

**appended_bytes** (on Array/Collection): How many bytes of forwarded children live past the subtree's logical end (counted toward dead-space accounting on the sidecar's free list).

**child_index sort order:** Collection child entries are sorted by `key_hash` (xxh64 of the key string). Find a child via binary search on the hash, then verify by reading the actual key from the key_string_area (collision resolution).

### Dictionary

All unique structural field names across the database, stored once. Collection keys (push IDs) are NOT stored in the dictionary; they're stored inline in TYPE_COLLECTION nodes.

```
+--------------------------------------------------+
| field_count: u32                                 |
| sorted_count: u32                                |
| max_field_count: u32  (reserved slots)           |
| name_data_used: u32                              |
| max_name_data: u32  (reserved name bytes)        |
| sorted_hashes: [u64; max_field_count]            |
|   (first sorted_count are sorted for bsearch,    |
|    remainder are appended unsorted)               |
| sorted_to_field_id: [u32; max_field_count]       |
|   (maps sorted index -> field_id)                |
| name_lengths: [u32; max_field_count]             |
|   (byte length of each field name)               |
| name_data: [u8; max_name_data]                   |
|   (packed UTF-8 field name strings)              |
+--------------------------------------------------+
```

**Reserved space:** `max_field_count = max(500, 2×field_count)` slots. `max_name_data = max(10000, 2×name_data_used)` bytes. This allows incremental compaction to append new field names without rebuilding the dictionary or the blob. FieldIdSize (u8/u16/u32) is computed from `max_field_count` so field_ids up to reserved capacity fit in the encoding.

**Lookup:** Hash the field name → binary search in sorted_hashes[0..sorted_count] → verify against actual field name string (collision resolution) → get field_id. If not found in sorted region, linear scan sorted_hashes[sorted_count..field_count] (appended entries, typically very few). If the hash is not found at all, the field does not exist anywhere in the blob, which makes for a fast negative check.

**Growth during incremental compaction:** New structural field names are appended unsorted after the sorted region via `append_field()`, which returns pwrite patches to update the on-disk dictionary in-place. New field_ids are assigned sequentially starting from `field_count`. All existing field_ids are preserved, so no patching is needed. On full re-compaction, the dictionary is rebuilt fully sorted with all field_ids reassigned.

**Size:** Structural field names only (~200 typical for a Lark database). With reserved space: ~18KB. Negligible even for large databases.

Note: OSON's approach uses "partial dictionary rebuilding by tracking dictionary codes that have been actually changed due to insertion of new distinct field names and then only patching those changed dictionary codes." We avoid this complexity by appending new fields unsorted, which is simpler and avoids patching a potentially GB-sized blob.

### Sidecar (free list + pending keys)

Every blob has a small companion file (`sidecar.lark` on disk) that
carries state the blob itself can't represent cheaply: the **free list**
of reusable byte regions, and any **pending dictionary keys** written
inline since the last root re-compaction.

**On-disk format (v7):**

```
+---------------------------------------------+
| magic "LRKF" (4 bytes)                      |
| version: u32 = 7                            |
| region_count: u64                           |
| bytes_freed: u64    (lifetime, monotonic)   |
| bytes_reused: u64   (lifetime, monotonic)   |
| bytes_wasted: u64   (current; reset on full |
|                      compaction)            |
+---------------------------------------------+
| Free regions:                               |
|   (offset: u64, size: u64) × region_count   |
+---------------------------------------------+
| Pending keys trailer:                       |
|   pending_key_count: u32                    |
|   (key_len: u16, key_bytes: [u8]) × count   |
+---------------------------------------------+
```

Magic + version verify the file matches the format the running binary
expects; old versions are rejected outright (v1–v6 → error).

**`pending_keys`** are structural field names that were referenced in an
incremental write but aren't in the dictionary yet. Rather than rewriting
the dictionary on every incremental update (which would touch a fixed
region of the blob constantly and force a write-back), the writer stores
those keys inline in the affected collection's key-string area, marks
them in the sidecar, and the next `root_compact()` drains the pending set
into the dictionary as part of the rewrite.

### Free list and space reuse

The free list is the difference between "blob grows forever on every
write" and "blob stays roughly the same size as long as the working set
does." It's the reason `lark-server` can run for weeks without ever
triggering a full root compaction.

#### What gets freed

Any time incremental compaction makes a region of the blob unreachable,
that region's `(offset, size)` is recorded in the free list:

- **Updating a value larger than the old one.** The old bytes are freed; the new value goes wherever the free list has space (or at EOF).
- **Forwarding a child to a new location.** The pre-forwarding bytes are freed.
- **Re-serializing a collection when its reserved space is exhausted.** The old collection's bytes are freed.
- **Removing a child from a collection.** The child's bytes are freed.

Regions smaller than `MIN_FREE_REGION = 4096` bytes are not tracked,
because the index bookkeeping overhead exceeds the space savings. Those
bytes are charged to `bytes_wasted` and only recovered by a full
re-compaction.

#### Allocation policy: best-fit with splitting

When the writer wants `N` bytes of space, the free list finds the
smallest tracked region with size ≥ `N`:

1. Best-fit lookup: smallest region ≥ N (O(log n) via `BTreeMap<size,
   set<offset>>` indexed by size).
2. If the matched region is bigger than N, the remainder (`size - N`) is
   put back as its own region, *unless* the remainder is smaller than
   `MIN_FREE_REGION`, in which case it gets charged to `bytes_wasted`.
3. If no region fits, the writer appends at EOF instead.

Adjacent and overlapping regions are merged on insert via `insert_and_merge`,
so the free list doesn't fragment indefinitely.

#### Concurrent-reader safety: two-generation epoch

The free list never reuses bytes a concurrent reader might still be
mid-flight on. It does this with a two-generation epoch system:

- **`current`**: regions freed during *this* compaction cycle. **Not reusable yet.**
- **`previous`**: regions freed during the *previous* cycle. **Not reusable yet.**
- **`available`** (the indexed `by_size` / `by_offset` maps): regions freed ≥ 2 cycles ago. **Safe to reuse.**

At the start of each `apply_updates` batch, `advance_epoch()` rotates:
`previous → available`, `current → previous`. The newly-promoted regions
are returned to the caller, which clears them from the CachedIO byte
cache so the cache won't serve stale bytes after the regions are reused.

The two-cycle delay matches the worst case for an in-flight reader:
they may have already started a read using the pre-write offset/size,
and we need to wait for them to either finish or be told (via the parent
index update) about the new location before reusing the old bytes.

#### Accounting fields

| Field | Meaning | When it grows | When it resets |
|---|---|---|---|
| `bytes_freed` | Total bytes ever returned to the free list | Every `free()` | Full re-compaction |
| `bytes_reused` | Total bytes ever allocated from the free list | Every successful `allocate()` | Full re-compaction |
| `bytes_wasted` | Dead-space bytes not recoverable by the free list (too small to track, or interior to a parent's reserved area) | `free()` of < MIN_FREE_REGION, leftover splits, `waste()` calls | Full re-compaction |

`bytes_wasted` is the trigger for full re-compaction. `lark-compact`
runs root compaction when `bytes_wasted >= 500 MB AND bytes_wasted /
blob_size >= 20%`. Until then, the free list keeps the blob from
growing unboundedly even under sustained write load.

#### Crash safety

The sidecar is rewritten as a single atomic file alongside each batch
of blob writes (`apply_updates_with_sidecar`). If a crash happens
between the blob write and the sidecar write, the worst case is that
some freed regions look "live" until the next root re-compaction, i.e.
the blob keeps a little extra dead space but is still correct. The
blob never points at bytes the sidecar doesn't know about, because the
blob's parent-index entries are the source of truth for what's live;
the sidecar is just an accounting hint for "what's safe to overwrite."

### Size Overhead

Per-node overhead:
- **Collection**: 29-byte header + 25 bytes per child index entry + key-string bytes (2 bytes for a dictionary reference, or 2 + key length for an inline key) + reserved space (variable, see `compute_reserved_count`).
- **Array**: 17-byte header + 17 bytes per element index entry.
- **String**: 5 bytes header (type + length) + UTF-8 bytes.
- **Number**: 9 bytes (type + f64).
- **Bool**: 2 bytes, **Null**: 1 byte.

The dictionary deduplicates structural field names, so millions of objects sharing names like `"name"`, `"x"`, `"y"` only pay 2 bytes per key in the keystring area (a dictionary reference) plus the 25-byte child index entry. Push-ID keys are stored inline (~24 bytes per key) but don't pollute the dictionary.

## Operations

### Navigation

To read `/characters/-abc123/hp` from a 200GB blob:

```
1. Read header -> root_offset
2. Read root collection's header + child_index
   Binary search for key_hash("characters") -> entry: offset, size, type_flags
3. Seek to characters node, read header + child_index
   Binary search for key_hash("-abc123") -> entry, verify key string
4. Seek to -abc123 node, read header + child_index
   Binary search for key_hash("hp") -> entry
5. Seek to hp node at the entry's offset, read value
   -> TYPE_NUMBER, f64 = 42.0
```

Each step: one small random read (~4 KB). This means the time to lookup a value is driven by the depth of the tree, not the size of the overall blob/database.

If any entry along the way has `TYPE_FLAGS_FORWARDED` set, its `offset` is treated as an absolute file offset to the relocated child (rather than a relative offset into the parent's children area).

### Promotion (lazy load from blob into the tree)

When `lark-server` needs a subtree that's currently a Sentinel in the in-memory tree:

```
1. Navigate blob to target path (O(depth) random reads, ~1ms)
2. Read subtree:
   - If no forwarded descendants: one contiguous read of subtree_size bytes
   - If some are forwarded: extra reads (bounded by updates since
     last full re-compaction)
3. Replay any in-memory WAL entries newer than blob_sequence on top
4. Deserialize bytes to ArcValue and insert into the tree, replacing the Sentinel
```

### Eviction (drop a promoted subtree back to a Sentinel)

When the eviction policy decides a promoted path has been idle long enough:

```
1. Replace the tree node at the path with an empty Sentinel
2. No I/O needed — data is recoverable from blob + in-memory WAL
```

The data is still recoverable: the next access re-promotes the subtree from the blob and replays any pending WAL entries on top. Subscriptions deliberately don't pin paths against eviction; idle paths get evicted and re-promoted on demand.

### Batch Promotion (Startup / io_uring)

For loading multiple paths at once:

```
1. Collect paths to promote
2. Navigate each to get blob offsets (can parallelize navigations)
3. Sort by blob offset
4. Merge adjacent reads (if gap < threshold, read the gap too)
5. Submit as io_uring scatter-gather reads
6. Deserialize all subtrees from results
```

### Incremental Compaction

Applies WAL entries directly to the blob without full rewrite. Runs in the compactor process every few seconds.

**Updating an existing value:**

```
For each WAL entry (SET /characters/-abc123/hp = 42):

1. Navigate blob to find the target node + its parent's index entry (~1ms)
2. Serialize the new value to bytes
3. Compare new_size vs old_size:
   a) new_size == old_size:
      -> pwrite() at same offset (length-preserved byte replacement)
   b) new_size < old_size:
      -> pwrite() at same offset, return the unused bytes to the sidecar free list
   c) new_size > old_size:
      -> append() new value at EOF (or reuse a free-list slot of appropriate size)
      -> pwrite() the parent's index entry with new offset/size and the
         TYPE_FLAGS_FORWARDED bit set (the entry now points at the absolute
         file offset of the relocated child)
      -> If the entry was ALREADY forwarded: the old location's bytes go back
         to the free list and the parent's entry is updated to the new location.
         No multi-hop chains — the redirect always lives in the parent's index.
4. Record last-applied WAL sequence in the sidecar
```

**Inserting a new child into a collection (e.g., new chat message):**

```
For each WAL entry (SET /chat/-def456 = {author: "...", text: "..."}):

1. Navigate blob to /chat (the collection parent)
2. Ensure any new structural field names in the value subtree are in the dictionary
3. Serialize the new child value, append at EOF
4. If collection has reserved index + key string space (fast path):
   a) Shift index entries right to make room at sorted position (pwrite)
   b) Write new index entry into reserved slot (pwrite)
   c) Rebuild key string data with new key inserted (pwrite)
   d) Update header: child_count++, reserved_count--, key_data_used += new key size (pwrite)
5. If reserved space exhausted (fallback):
   a) Read entire collection subtree as ArcValue
   b) Add the new child
   c) Re-serialize at EOF with fresh reserved space proportional to new size
   d) Update the parent's index entry to point at the new location
      (TYPE_FLAGS_FORWARDED), return the old location to the free list
```

**Inserting at a path where parents don't exist (e.g., first chat message):**

```
For WAL entry (SET /chat/-msg001 = {...}) when /chat doesn't exist:

1. Navigate as deep as possible (navigate_to_depth)
2. Wrap the value in nested objects for missing path segments:
   {"-msg001": value} -> {"chat": {"-msg001": value}}
3. Insert the wrapped value at the deepest reachable level
```

**Batching optimization:** At high write rates, apply WAL entries in batches:
- Group by path (latest value per path wins, i.e. coalesce)
- Sort by blob offset (sequential pwrite is faster than random)
- Apply as batched pwrite() + append operations
- Dictionary is read once per batch (not per update)

**Cost:** Proportional to WAL size, not blob size. Applying 5MB of WAL entries to a 200GB blob takes seconds, not minutes.

### Full Re-Compaction

Rewrites the entire blob clean (no dead space, freshly sorted child indexes, dictionary rebuilt from scratch). Sequential I/O.

```
1. Walk old blob depth-first via the in-memory session (follows forwarded
   children transparently)
2. Write new blob depth-first into blob.lark.tmp (sequential writes)
3. Rebuild dictionary (compact, only fields actually referenced)
4. Rename dance: blob.lark -> blob.old.lark, blob.lark.tmp -> blob.lark
5. Bump blob.generation; remove blob.old.lark
6. Reset the sidecar (free list + bytes_wasted go to 0)
```

**Triggered when:** `bytes_wasted >= 500 MB` AND `bytes_wasted / blob_size >= 20%` (see `lark-compact`'s thresholds). Until then, incremental compaction's free-list reuse keeps blob growth bounded without rewriting the whole thing.

**Cost:** O(blob_size) read + O(blob_size) write. 200 GB = ~12 min at 300 MB/s write. Rare enough to amortize; benchmarks show concurrent reads only see ~20% throughput reduction during the rewrite.

**Optimization:** Subtrees with no forwarded descendants are bulk-copied as raw bytes without per-node decoding. For a database where only 1% of data changed between full re-compactions, CPU work scales with dirty paths while I/O is still full blob size.

## Architecture

### Project Structure

```
blob/
+-- Cargo.toml
+-- README.md           # this file
+-- src/
|   +-- lib.rs              # public API surface
|   +-- arc_value.rs        # copy-on-write JSON values (incl. Sentinel variant)
|   +-- error.rs            # error types (BlobError, Result)
|   +-- format.rs           # header + node type constants, encoding helpers
|   +-- dictionary.rs       # field-name dictionary (build, lookup, append_field)
|   +-- writer.rs           # ArcValue -> blob bytes (initial depth-first write)
|   +-- session.rs          # BlobSession: navigation + read + write + compact
|   +-- session_reader.rs   # path navigation, read_subtree, read_keys, read_shallow
|   +-- session_writer.rs   # apply_updates / serialize-and-link helpers
|   +-- session_incremental.rs  # in-place updates, free-list reuse, forwarding
|   +-- incremental.rs      # apply_updates entry point
|   +-- compact.rs          # full re-compaction (read tree, rewrite clean)
|   +-- segment.rs          # Sidecar format (free list + pending dict keys)
|                           #   — historical name; no segments anymore
|   +-- free_list.rs        # blob's reusable-range tracker (lives in the sidecar)
|   +-- nav_cache.rs        # in-memory navigation offset cache
|   +-- cached_io.rs        # CachedIO wrapper around BlobIO (byte cache)
|   +-- io.rs               # BlobIO trait (MemBlobIO for tests, StdBlobIO for files)
|   +-- test_helpers.rs     # generate_game_database, helpers for tests/benches
|   +-- bin/
|       +-- inspect.rs      # blob inspector CLI
+-- benches/
    +-- throughput.rs       # criterion: write, navigate, incremental, full compact
    +-- promotion.rs        # criterion: multiplayer-game workload, file I/O
```

Tests are inline `#[cfg(test)]` modules within each source file rather than in a separate `tests/` directory.


### I/O Abstraction

LarkBlob doesn't depend on Glommio or Tokio directly. It defines an
async `BlobIO` trait so the same engine works behind both the io_uring
runtime in `lark-server` and a plain `std::fs::File` for tests and the
standalone `lark-compact` binary.

```rust
pub trait BlobIO {
    async fn pread(&self, offset: u64, len: usize) -> io::Result<Vec<u8>>;
    async fn pwrite(&self, offset: u64, data: &[u8]) -> io::Result<()>;
    async fn append(&self, data: &[u8]) -> io::Result<u64>;  // returns new offset
    async fn truncate(&self, size: u64) -> io::Result<()>;
    async fn sync(&self) -> io::Result<()>;
    async fn size(&self) -> io::Result<u64>;
    // Plus pread_into / pwrite_deferred for zero-alloc and write-back caching.
}
```

Three concrete implementations are wired up:

- **`MemBlobIO`**: backed by a `Vec<u8>`. Fast, no disk, deterministic. Used in tests.
- **`StdBlobIO`**: `std::fs::File` with `FileExt::read_at` / `write_at`. Used by `lark-compact` and by anywhere outside the Glommio runtime.
- **`GlommioBlobIO`**: io_uring pread/pwrite. Lives in `lark-server` (`server/src/storage/glommio_blob_io.rs`), not in the blob crate itself, so the blob crate stays runtime-agnostic.

`CachedIO<IO>` wraps any `BlobIO` with a byte cache so repeated reads of the same region (and write-back of pending header/index updates) don't hit the underlying I/O twice.

### Public API

The main entry point is `BlobSession`, which owns the blob's header,
dictionary, and (optionally) sidecar across navigation + read + write
calls so the per-operation cost is just I/O.

```rust
// --- One-shot initial write ---

/// Serialize an ArcValue tree into a freshly-created blob.
pub async fn write_blob<IO: BlobIO>(io: &mut IO, tree: &ArcValue)
    -> Result<BlobStats>;

// --- Session-based access (the hot path) ---

impl<IO: BlobIO> BlobSession<IO> {
    /// Open an existing blob, read header + dictionary.
    pub async fn open(io: IO) -> Result<Self>;

    /// Open with the companion sidecar (free list + pending dictionary keys).
    pub async fn open_with_sidecar<SIO: BlobIO>(io: IO, sidecar: Option<&SIO>)
        -> Result<Self>;

    /// Initialize a fresh empty blob (Sentinel root).
    pub async fn init(io: IO) -> Result<Self>;

    /// Apply a batch of WAL-style updates to the blob.
    pub async fn apply_updates(&mut self, updates: &[(Vec<String>, Option<ArcValue>)])
        -> Result<ApplyResult>;

    /// Same, but also persist the updated sidecar at the end.
    pub async fn apply_updates_with_sidecar<SIO: BlobIO>(
        &mut self,
        updates: &[(Vec<String>, Option<ArcValue>)],
        sidecar: Option<&SIO>,
    ) -> Result<ApplyResult>;

    /// Full root re-compaction into a fresh BlobIO.
    pub async fn root_compact<DST: BlobIO>(&mut self, dst: DST) -> Result<IO>;
}

impl<IO: BlobIO> BlobSession<IO> {
    /// Read a subtree at the given path into an ArcValue.
    pub async fn read_subtree(&self, path: &[&str]) -> Result<ArcValue>;

    /// Navigate to the path and return the child's location.
    pub async fn navigate(&self, path: &[&str]) -> Result<Option<BlobLocation>>;

    /// Read just the keys of a collection (no recursive load).
    pub async fn read_keys(&self, path: &[&str]) -> Result<Vec<String>>;

    /// Read one level deep — children that are scalars come back materialized,
    /// children that are containers come back as ShallowChild references.
    pub async fn read_shallow(&self, path: &[&str]) -> Result<Option<ShallowValue>>;
}

// --- Standalone full re-compaction (no live session) ---

pub async fn full_compact<S: BlobIO, D: BlobIO>(src: &S, dst: &mut D)
    -> Result<BlobStats>;
```

Returned types:

```rust
pub struct BlobStats {
    pub total_size: u64,
    pub node_count: u64,
    pub dict_field_count: u32,
}

pub struct ApplyResult {
    pub updates_applied: u32,
    pub bytes_appended: u64,
    pub bytes_freed: u64,
    pub bytes_reused: u64,
}

pub struct BlobLocation {
    pub offset: u64,
    pub size: u64,
    pub type_flags: u8,
}
```

### Server-Compactor Safety

Invariants the compactor maintains so concurrent reads from lark-server
are always safe:

1. **The old blob is never deleted** until the new one is fully written and synced (for full re-compaction, this means atomic rename).
2. **Incremental compaction only appends and does pwrite.** It never removes data from the blob. Dead space accumulates until full re-compaction.
3. **Server reads are safe.** The blob file is always readable. Forward pointers always point to valid data. New data is appended before the forward pointer is written.
4. **Write ordering for incremental updates:**
   - First: append new value at EOF + fsync
   - Then: pwrite forward pointer at tombstone location + fsync
   - If crash between: old value is still valid (no forward pointer yet), append is orphaned dead space (reclaimed on re-compaction)

## Navigation Cache

In-memory cache to avoid re-navigating from root:

```rust
pub struct NavigationCache {
    entries: HashMap<String, BlobLocation>,  // path -> location
}
```

- Populated lazily as paths are navigated
- On incremental compaction: forwarded entries updated in-place
- On full re-compact: entire cache invalidated (offsets change)

**Size:** For active paths only. A database might have millions of paths in the blob but only thousands are navigated during a session. At ~100 bytes per entry, 10K cached paths = ~1MB.

## WAL Index

For replaying recent WAL entries during promotion:

```rust
pub struct WalIndex {
    entries: HashMap<String, Vec<WalEntry>>,  // path -> entries since last blob update
}
```

Built incrementally as WAL entries are written. Cleared when compactor confirms it has applied entries to the blob (via metadata file polling).

On promotion of `/characters/abc123`, look up all entries with path prefix `/characters/abc123/` and replay in sequence order on top of blob data.

**Size:** Between compaction cycles (a few seconds), typically small. Even at 1000 writes/sec x 5 seconds = 5000 entries x ~200 bytes = ~1MB.

## References

- [OSON Paper](https://vldb.org/pvldb/vol13/p3059-liu.pdf), Oracle VLDB 2020
