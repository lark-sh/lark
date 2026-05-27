//! Full re-compaction and subtree compaction via structural byte-level copying.
//!
//! The core `structural_copy` function walks a tree in the source blob,
//! following forwarded children (via parent index `is_forwarded` flags),
//! and writes a clean contiguous copy to the destination — without
//! deserializing to ArcValue. Container headers are patched with corrected
//! rel_offsets and subtree_sizes. All data is buffered in memory and written
//! in single operations.

use crate::error::{BlobError, Result};
use crate::format::*;
use crate::io::{BlobIO, read_exact, read_exact_into};
use crate::nav_cache::ContainerInfo;
use crate::session_reader::{read_dictionary, read_header};
use crate::writer::BlobStats;

/// Stats from a structural copy or compaction operation.
#[derive(Debug, Default, Clone)]
pub struct CopyStats {
    pub bytes_written: u64,
    pub node_count: u64,
    pub forwards_followed: u32,
}

/// Check if a collection's key_strings contain any inline push-ID keys.
/// Scans key entries: dict-ref entries (2 bytes with KEY_DICT_FLAG) are structural
/// by definition. Inline entries whose UTF-8 content starts with '-' are push IDs.
fn has_inline_push_id_key(key_strings: &[u8], child_count: usize) -> bool {
    let mut pos = 0;
    for _ in 0..child_count {
        if pos + 2 > key_strings.len() {
            break;
        }
        let raw = u16::from_le_bytes(key_strings[pos..pos + 2].try_into().unwrap());
        if raw & KEY_DICT_FLAG != 0 {
            // Dict-ref — structural key, skip
            pos += 2;
        } else {
            let key_len = raw as usize;
            if key_len > 0 && pos + 2 < key_strings.len() && key_strings[pos + 2] == b'-' {
                return true;
            }
            pos += 2 + key_len;
        }
    }
    false
}

// ---------------------------------------------------------------------------
// Container compaction (in-memory rebuild)
// ---------------------------------------------------------------------------

/// What operation to perform when compacting a container.
pub(crate) enum CompactOp<'a> {
    /// Pure defragmentation — copy all live children, no insertions or removals.
    Defrag,
    /// Insert a new child into a collection (sorted by key_hash).
    InsertCollection { key: &'a str, value_bytes: &'a [u8] },
    /// Insert multiple children into a collection (sorted by key_hash).
    /// Each entry is (key, serialized_value_bytes).
    InsertCollectionBatch { entries: &'a [(&'a str, &'a [u8])] },
}

// ---------------------------------------------------------------------------
// Streaming container compaction constants
// ---------------------------------------------------------------------------

/// Read-ahead chunk size for streaming the contiguous region.
const READ_CHUNK_SIZE: usize = 4 * 1024 * 1024; // 4MB

/// Write buffer flush threshold during streaming compaction.
const WRITE_FLUSH_SIZE: usize = 4 * 1024 * 1024; // 4MB

/// Maximum gap between forwarded children to batch into a single read.
const EXTERNAL_BATCH_GAP: u64 = 16 * 1024; // 16KB

/// Threshold for chunked flushing during structural_copy. Root frames larger
/// than this are flushed in chunks to avoid holding >1GB in memory at once.
const COMPACT_FLUSH_THRESHOLD: usize = 1 << 30; // 1GB

// ---------------------------------------------------------------------------
// Shared streaming child writer
// ---------------------------------------------------------------------------

/// Stream child data from `src` to `dst`.
///
/// Writes children in three groups, in order:
/// 1. **Contiguous**: children in the source's contiguous region, sorted by
///    source offset (disk order). Read via ~4MB chunked read-ahead.
/// 2. **Forwarded**: children outside the contiguous region, sorted by source
///    offset. Nearby children (within 16KB gap) are batched into one read.
/// 3. **Inserts**: newly inserted child data already in memory.
///
/// All writes go to `dst` via pwrite starting at `write_start`. The write
/// buffer is flushed at ~4MB intervals to bound memory usage.
///
/// `get_child(i)` returns `(src_offset, size, insert_bytes)` for child at
/// index `i`, looked up directly from the caller's children array.
#[allow(clippy::too_many_arguments)]
async fn stream_children<'a, IO: BlobIO>(
    src: &IO,
    dst: &IO,
    get_child: impl Fn(usize) -> (u64, u64, Option<&'a [u8]>),
    contiguous_idx: &[usize],
    forwarded_idx: &[usize],
    insert_idx: &[usize],
    write_start: u64,
    contiguous_region_end: u64,
) -> Result<()> {
    let mut write_cursor = write_start;
    let mut write_buf: Vec<u8> = Vec::with_capacity(WRITE_FLUSH_SIZE + READ_CHUNK_SIZE);

    // --- 1. Stream contiguous children via chunked read-ahead ---
    // Reusable read-ahead buffer — allocated once, reused across chunks.
    let mut chunk_buf: Vec<u8> = vec![0u8; READ_CHUNK_SIZE];
    let mut chunk_start: u64 = 0;
    let mut chunk_len: usize = 0;

    for &ci in contiguous_idx {
        let (child_offset, child_size, _) = get_child(ci);

        if child_size as usize >= READ_CHUNK_SIZE {
            // Large child: stream in chunks directly
            let mut remaining = child_size;
            let mut pos = child_offset;
            while remaining > 0 {
                let to_read = std::cmp::min(remaining as usize, READ_CHUNK_SIZE);
                let start = write_buf.len();
                write_buf.resize(start + to_read, 0);
                src.pread_into(pos, &mut write_buf[start..start + to_read])
                    .await?;
                pos += to_read as u64;
                remaining -= to_read as u64;

                if write_buf.len() >= WRITE_FLUSH_SIZE {
                    dst.pwrite(write_cursor, &write_buf)
                        .await
                        .map_err(BlobError::Io)?;
                    write_cursor += write_buf.len() as u64;
                    write_buf.clear();
                }
            }
            chunk_len = 0; // Invalidate chunk cache
        } else {
            // Small child: use read-ahead chunk
            let child_end = child_offset + child_size;
            let in_chunk = chunk_len > 0
                && child_offset >= chunk_start
                && child_end <= chunk_start + chunk_len as u64;

            if !in_chunk {
                let available = (contiguous_region_end - child_offset) as usize;
                let new_chunk_size = std::cmp::min(
                    std::cmp::max(child_size as usize, READ_CHUNK_SIZE),
                    available,
                );
                if new_chunk_size > chunk_buf.len() {
                    chunk_buf.resize(new_chunk_size, 0);
                }
                src.pread_into(child_offset, &mut chunk_buf[..new_chunk_size])
                    .await?;
                chunk_start = child_offset;
                chunk_len = new_chunk_size;
            }

            let local = (child_offset - chunk_start) as usize;
            write_buf.extend_from_slice(&chunk_buf[local..local + child_size as usize]);

            if write_buf.len() >= WRITE_FLUSH_SIZE {
                dst.pwrite(write_cursor, &write_buf)
                    .await
                    .map_err(BlobError::Io)?;
                write_cursor += write_buf.len() as u64;
                write_buf.clear();
            }
        }
    }

    // --- 2. Stream forwarded children with proximity batching ---
    // Reusable batch buffer — grows as needed, never shrinks.
    let mut batch_buf: Vec<u8> = Vec::new();
    let mut fi = 0;

    while fi < forwarded_idx.len() {
        let (first_offset, first_size, _) = get_child(forwarded_idx[fi]);
        let mut batch_end = first_offset + first_size;
        let mut batch_count = 1;

        // Extend batch while next child is within EXTERNAL_BATCH_GAP
        while fi + batch_count < forwarded_idx.len() {
            let (next_offset, next_size, _) = get_child(forwarded_idx[fi + batch_count]);
            if next_offset > batch_end + EXTERNAL_BATCH_GAP {
                break;
            }
            batch_end = next_offset + next_size;
            batch_count += 1;
        }

        let batch_size = (batch_end - first_offset) as usize;

        if batch_count > 1 && batch_size < READ_CHUNK_SIZE {
            // Batch read — reuse batch_buf
            if batch_buf.len() < batch_size {
                batch_buf.resize(batch_size, 0);
            }
            src.pread_into(first_offset, &mut batch_buf[..batch_size])
                .await?;
            for j in 0..batch_count {
                let (off, sz, _) = get_child(forwarded_idx[fi + j]);
                let local = (off - first_offset) as usize;
                write_buf.extend_from_slice(&batch_buf[local..local + sz as usize]);
            }
        } else {
            // Read children individually (single child or batch too large)
            for j in 0..batch_count {
                let (off, sz, _) = get_child(forwarded_idx[fi + j]);
                let start = write_buf.len();
                write_buf.resize(start + sz as usize, 0);
                src.pread_into(off, &mut write_buf[start..start + sz as usize])
                    .await?;

                if write_buf.len() >= WRITE_FLUSH_SIZE {
                    dst.pwrite(write_cursor, &write_buf)
                        .await
                        .map_err(BlobError::Io)?;
                    write_cursor += write_buf.len() as u64;
                    write_buf.clear();
                }
            }
        }

        if write_buf.len() >= WRITE_FLUSH_SIZE {
            dst.pwrite(write_cursor, &write_buf)
                .await
                .map_err(BlobError::Io)?;
            write_cursor += write_buf.len() as u64;
            write_buf.clear();
        }

        fi += batch_count;
    }

    // --- 3. Write inserted children (already in memory) ---
    for &ii in insert_idx {
        let (_, _, insert_bytes) = get_child(ii);
        if let Some(data) = insert_bytes {
            write_buf.extend_from_slice(data);

            if write_buf.len() >= WRITE_FLUSH_SIZE {
                dst.pwrite(write_cursor, &write_buf)
                    .await
                    .map_err(BlobError::Io)?;
                write_cursor += write_buf.len() as u64;
                write_buf.clear();
            }
        }
    }

    // Final flush
    if !write_buf.is_empty() {
        dst.pwrite(write_cursor, &write_buf)
            .await
            .map_err(BlobError::Io)?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// compact_collection: streaming compaction for TYPE_COLLECTION
// ---------------------------------------------------------------------------

/// Compact a TYPE_COLLECTION container via streaming I/O.
///
/// Same two-phase approach as `compact_object`: plan from the index, then
/// stream. Collection-specific handling: key string table is rebuilt in the
/// header region, reserved index slots and key string space are recomputed.
pub(crate) async fn compact_collection<IO: BlobIO>(
    src: &IO,
    dst: &IO,
    container: &ContainerInfo,
    _field_id_size: FieldIdSize,
    op: &CompactOp<'_>,
    free_list: &mut crate::free_list::FreeList,
    dict: &crate::dictionary::Dictionary,
) -> Result<(u64, CopyStats)> {
    let mut stats = CopyStats::default();
    let src_child_count = container.child_count as usize;

    // Build sorted list of new entries to insert
    struct NewEntry<'b> {
        hash: u64,
        key: &'b str,
        value_bytes: &'b [u8],
    }
    let mut new_entries: Vec<NewEntry<'_>> = Vec::new();
    match op {
        CompactOp::InsertCollection { key, value_bytes } => {
            new_entries.push(NewEntry {
                hash: crate::dictionary::hash_field_name(key),
                key,
                value_bytes,
            });
        }
        CompactOp::InsertCollectionBatch { entries } => {
            for &(key, value_bytes) in *entries {
                new_entries.push(NewEntry {
                    hash: crate::dictionary::hash_field_name(key),
                    key,
                    value_bytes,
                });
            }
            new_entries.sort_by_key(|e| e.hash);
        }
        _ => {}
    }

    // --- Phase 1: Plan ---
    // Parse source children and merge with new entries on-the-fly.
    // Source children are already in index order (sorted by hash), and
    // new_entries is sorted by hash, so a single pass produces the merged
    // output without an intermediate Vec.
    struct Child<'a> {
        hash: u64,
        type_flags: u8,
        size: u64,
        src_offset: u64, // absolute source offset
        is_contiguous: bool,
        insert_bytes: Option<&'a [u8]>,
        /// Key string bytes: (len:u16, utf8) — slice into source or built for inserts
        key_entry: KeySource<'a>,
        output_rel_offset: u64,
    }

    enum KeySource<'a> {
        /// Slice of container.key_strings[start..start+len]
        Source { start: usize, len: usize },
        /// New key for inserted children
        New(&'a str),
    }

    let mut children: Vec<Child<'_>> = Vec::with_capacity(src_child_count + new_entries.len());
    let mut key_pos = 0usize;
    let mut new_i = 0usize;

    for src_i in 0..src_child_count {
        let idx_off = src_i * COLLECTION_INDEX_ENTRY_SIZE;
        let hash = u64::from_le_bytes(
            container.child_index[idx_off..idx_off + 8]
                .try_into()
                .unwrap(),
        );
        let src_type_flags = container.child_index[idx_off + 8];
        let offset = u64::from_le_bytes(
            container.child_index[idx_off + 9..idx_off + 17]
                .try_into()
                .unwrap(),
        );
        let size = u64::from_le_bytes(
            container.child_index[idx_off + 17..idx_off + 25]
                .try_into()
                .unwrap(),
        );

        let raw_klen = u16::from_le_bytes(
            container.key_strings[key_pos..key_pos + 2]
                .try_into()
                .unwrap(),
        );
        let key_entry_start = key_pos;
        let key_entry_len = if raw_klen & KEY_DICT_FLAG != 0 {
            2 // dict-ref: just the 2-byte marker
        } else {
            2 + raw_klen as usize // inline: 2-byte len + key bytes
        };
        key_pos += key_entry_len;

        // Skip tombstones (TYPE_NULL with size=0) and zeroed-out slots.
        // These are deleted children — don't include them in the compacted output.
        let child_type = extract_type_tag(src_type_flags);
        #[allow(clippy::nonminimal_bool)] // two distinct cases: zeroed slot vs. tombstone
        if (src_type_flags == 0 && size == 0) || (child_type == TYPE_NULL && size == 0) {
            continue;
        }

        // Emit new entries with hash <= this source child's hash
        while new_i < new_entries.len() && new_entries[new_i].hash <= hash {
            let ne = &new_entries[new_i];
            new_i += 1;
            children.push(Child {
                hash: ne.hash,
                type_flags: make_type_flags(ne.value_bytes[0], false),
                size: ne.value_bytes.len() as u64,
                src_offset: 0,
                is_contiguous: false,
                insert_bytes: Some(ne.value_bytes),
                key_entry: KeySource::New(ne.key),
                output_rel_offset: 0,
            });
        }

        let is_fwd = is_forwarded_flag(src_type_flags);
        let child_abs = if is_fwd {
            offset
        } else {
            container.children_area_offset + offset
        };

        // Emit source child
        children.push(Child {
            hash,
            type_flags: make_type_flags(extract_type_tag(src_type_flags), false),
            size,
            src_offset: child_abs,
            is_contiguous: !is_fwd,
            insert_bytes: None,
            key_entry: KeySource::Source {
                start: key_entry_start,
                len: key_entry_len,
            },
            output_rel_offset: 0,
        });
    }

    // Emit remaining new entries after all source children
    while new_i < new_entries.len() {
        let ne = &new_entries[new_i];
        new_i += 1;
        children.push(Child {
            hash: ne.hash,
            type_flags: make_type_flags(ne.value_bytes[0], false),
            size: ne.value_bytes.len() as u64,
            src_offset: 0,
            is_contiguous: false,
            insert_bytes: Some(ne.value_bytes),
            key_entry: KeySource::New(ne.key),
            output_rel_offset: 0,
        });
    }

    let merged_child_count = children.len();

    // Compute key string data size and build key string buffer.
    // Also detect if any key is a push ID (for reserved space decision).
    let mut key_data_buf: Vec<u8> = Vec::new();
    let mut has_push_id_keys = false;
    for child in &children {
        match &child.key_entry {
            KeySource::Source { start, len } => {
                let entry = &container.key_strings[*start..*start + *len];
                key_data_buf.extend_from_slice(entry);
                // Check if this is an inline key starting with '-'
                if *len > 2 {
                    let raw = u16::from_le_bytes(entry[0..2].try_into().unwrap());
                    if raw & KEY_DICT_FLAG == 0 && entry.get(2) == Some(&b'-') {
                        has_push_id_keys = true;
                    }
                }
            }
            KeySource::New(key) => {
                if crate::dictionary::is_collection_key(key) {
                    has_push_id_keys = true;
                }
                if let Some(field_id) = dict.lookup(key) {
                    // Key is in the dictionary — use dict-ref encoding (2 bytes)
                    key_data_buf
                        .extend_from_slice(&(KEY_DICT_FLAG | field_id as u16).to_le_bytes());
                } else {
                    // Key not in dictionary — write inline
                    let kb = key.as_bytes();
                    key_data_buf.extend_from_slice(&(kb.len() as u16).to_le_bytes());
                    key_data_buf.extend_from_slice(kb);
                }
            }
        }
    }
    let key_data_used = key_data_buf.len() as u32;

    // Total children size for reserved space decision.
    let total_children_size: u64 = children.iter().map(|c| c.size).sum();

    let new_reserved_count = compute_reserved_count(
        merged_child_count as u32,
        total_children_size,
        has_push_id_keys,
    );
    let avg_key_entry = if merged_child_count > 0 {
        std::cmp::max(24, key_data_used / merged_child_count as u32)
    } else {
        24
    };
    let key_data_reserved = key_data_used + new_reserved_count * avg_key_entry;
    let total_slots = merged_child_count as u32 + new_reserved_count;

    // Split into physical groups
    let mut contiguous_idx: Vec<usize> = Vec::new();
    let mut forwarded_idx: Vec<usize> = Vec::new();
    let mut insert_idx: Vec<usize> = Vec::new();

    for (i, child) in children.iter().enumerate() {
        if child.insert_bytes.is_some() {
            insert_idx.push(i);
        } else if child.is_contiguous {
            contiguous_idx.push(i);
        } else {
            forwarded_idx.push(i);
        }
    }

    contiguous_idx.sort_by_key(|&i| children[i].src_offset);
    forwarded_idx.sort_by_key(|&i| children[i].src_offset);

    // Assign output rel_offsets in physical order (externals excluded — zero data contribution)
    let mut pos: u64 = 0;
    for &i in contiguous_idx
        .iter()
        .chain(forwarded_idx.iter())
        .chain(insert_idx.iter())
    {
        children[i].output_rel_offset = pos;
        pos += children[i].size;
    }
    let total_children_size = pos;

    // Build header region: header + index + reserved slots + key strings + reserved key space
    let header_region_size = COLLECTION_HEADER_SIZE
        + total_slots as usize * COLLECTION_INDEX_ENTRY_SIZE
        + key_data_reserved as usize;
    let total_output_size = header_region_size as u64 + total_children_size;

    let mut header_buf = vec![0u8; header_region_size];
    header_buf[0] = TYPE_COLLECTION;
    header_buf[1..9].copy_from_slice(&total_output_size.to_le_bytes());
    header_buf[9..13].copy_from_slice(&(merged_child_count as u32).to_le_bytes());
    header_buf[13..17].copy_from_slice(&new_reserved_count.to_le_bytes());
    header_buf[17..21].copy_from_slice(&key_data_used.to_le_bytes());
    header_buf[21..25].copy_from_slice(&key_data_reserved.to_le_bytes());
    // appended_bytes at [25..29] already 0

    // Write index entries (in index order = sorted by hash)
    let index_start = COLLECTION_HEADER_SIZE;
    for (out_i, child) in children.iter().enumerate() {
        let idx_off = index_start + out_i * COLLECTION_INDEX_ENTRY_SIZE;
        header_buf[idx_off..idx_off + 8].copy_from_slice(&child.hash.to_le_bytes());
        header_buf[idx_off + 8] = child.type_flags;
        header_buf[idx_off + 9..idx_off + 17]
            .copy_from_slice(&child.output_rel_offset.to_le_bytes());
        header_buf[idx_off + 17..idx_off + 25].copy_from_slice(&child.size.to_le_bytes());
    }

    // Write key string data into header region (after all index slots including reserved)
    let key_strings_start =
        COLLECTION_HEADER_SIZE + total_slots as usize * COLLECTION_INDEX_ENTRY_SIZE;
    header_buf[key_strings_start..key_strings_start + key_data_buf.len()]
        .copy_from_slice(&key_data_buf);

    // --- Phase 2: Stream ---
    let dst_offset = free_list
        .reserve_or_append(dst, total_output_size)
        .await
        .map_err(BlobError::Io)?;

    // Write header region
    dst.pwrite(dst_offset, &header_buf)
        .await
        .map_err(BlobError::Io)?;

    // Stream child data
    let contiguous_region_end = container.resolved_offset + container.subtree_size;
    stream_children(
        src,
        dst,
        |i| {
            (
                children[i].src_offset,
                children[i].size,
                children[i].insert_bytes,
            )
        },
        &contiguous_idx,
        &forwarded_idx,
        &insert_idx,
        dst_offset + header_region_size as u64,
        contiguous_region_end,
    )
    .await?;

    stats.bytes_written = total_output_size;
    stats.node_count = merged_child_count as u64 + 1; // +1 for the collection itself
    Ok((dst_offset, stats))
}

// ---------------------------------------------------------------------------
// compact_container: dispatcher (no upfront subtree read)
// ---------------------------------------------------------------------------

/// Compact a container (object or collection) via streaming I/O.
///
/// No upfront read of the full subtree — the index (already parsed in
/// `ContainerInfo`) provides every child's offset and size. Child data
/// is streamed in ~4MB chunks.
///
/// Returns (new_offset, stats).
pub(crate) async fn compact_container<IO: BlobIO>(
    src: &IO,
    dst: &IO,
    container: &ContainerInfo,
    field_id_size: FieldIdSize,
    op: &CompactOp<'_>,
    free_list: &mut crate::free_list::FreeList,
    dict: &crate::dictionary::Dictionary,
) -> Result<(u64, CopyStats)> {
    // Yield before potentially expensive compaction work.
    dst.yield_point().await;

    match container.tag {
        TYPE_COLLECTION => {
            compact_collection(src, dst, container, field_id_size, op, free_list, dict).await
        }
        _ => Err(BlobError::NotAContainer(
            container.resolved_offset,
            container.tag,
        )),
    }
}

// ---------------------------------------------------------------------------
// structural_copy: smart copy using v2 index format + buffered writes
// ---------------------------------------------------------------------------

/// Info about a child parsed from its parent's v2 index entry.
struct ChildEntry {
    /// Absolute offset of the child data in the source.
    src_offset: u64,
    /// Total byte size of the child.
    size: u64,
    /// The type_flags byte from the parent's index.
    type_flags: u8,
}

/// Parse child `i` from a collection's index bytes.
/// v2 format: (key_hash:8, type_flags:1, offset:8, size:8) = 25 bytes
fn parse_collection_child(index: &[u8], i: usize, children_area_offset: u64) -> ChildEntry {
    let off = i * COLLECTION_INDEX_ENTRY_SIZE;
    let type_flags = index[off + 8];
    let offset = u64::from_le_bytes(index[off + 9..off + 17].try_into().unwrap());
    let size = u64::from_le_bytes(index[off + 17..off + 25].try_into().unwrap());
    let src_offset = if is_forwarded_flag(type_flags) {
        offset
    } else {
        children_area_offset + offset
    };
    ChildEntry {
        src_offset,
        size,
        type_flags,
    }
}

/// Parse element `i` from an array's index bytes.
/// v2 format: (type_flags:1, offset:8, size:8) = 17 bytes
fn parse_array_child(index: &[u8], i: usize, children_area_offset: u64) -> ChildEntry {
    let off = i * ARRAY_INDEX_ENTRY_SIZE;
    let type_flags = index[off];
    let offset = u64::from_le_bytes(index[off + 1..off + 9].try_into().unwrap());
    let size = u64::from_le_bytes(index[off + 9..off + 17].try_into().unwrap());
    let src_offset = if is_forwarded_flag(type_flags) {
        offset
    } else {
        children_area_offset + offset
    };
    ChildEntry {
        src_offset,
        size,
        type_flags,
    }
}

/// Read a container's header, index, and key strings from source.
/// Returns everything needed to process its children during structural_copy.
struct CopyContainerInfo {
    tag: u8,
    child_count: usize,
    child_index: Vec<u8>,
    children_area_offset: u64,
    // Collection-specific
    key_strings: Vec<u8>,
    key_data_used: u32,
}

async fn read_container_info<IO: BlobIO>(src: &IO, offset: u64) -> Result<CopyContainerInfo> {
    let resolved = offset;

    let mut peek = [0u8; 1];
    read_exact_into(src, resolved, &mut peek).await?;
    let tag = peek[0];

    match tag {
        TYPE_COLLECTION => {
            let mut hdr = [0u8; COLLECTION_HEADER_SIZE];
            read_exact_into(src, resolved, &mut hdr).await?;
            let child_count = u32::from_le_bytes(hdr[9..13].try_into().unwrap()) as usize;
            let reserved_count = u32::from_le_bytes(hdr[13..17].try_into().unwrap());
            let key_data_used = u32::from_le_bytes(hdr[17..21].try_into().unwrap());
            let key_data_reserved = u32::from_le_bytes(hdr[21..25].try_into().unwrap());
            let total_slots = child_count as u32 + reserved_count;
            let idx_offset = resolved + COLLECTION_HEADER_SIZE as u64;
            let idx_size = child_count * COLLECTION_INDEX_ENTRY_SIZE;
            let child_index = if idx_size > 0 {
                read_exact(src, idx_offset, idx_size).await?
            } else {
                Vec::new()
            };
            let key_strings_offset =
                idx_offset + total_slots as u64 * COLLECTION_INDEX_ENTRY_SIZE as u64;
            let key_strings = if key_data_used > 0 {
                read_exact(src, key_strings_offset, key_data_used as usize).await?
            } else {
                Vec::new()
            };
            let children_area_offset = key_strings_offset + key_data_reserved as u64;
            Ok(CopyContainerInfo {
                tag,
                child_count,
                child_index,
                children_area_offset,
                key_strings,
                key_data_used,
            })
        }
        TYPE_ARRAY => {
            let mut hdr = [0u8; ARRAY_HEADER_SIZE];
            read_exact_into(src, resolved, &mut hdr).await?;
            let child_count = u32::from_le_bytes(hdr[9..13].try_into().unwrap()) as usize;
            let idx_offset = resolved + ARRAY_HEADER_SIZE as u64;
            let idx_size = child_count * ARRAY_INDEX_ENTRY_SIZE;
            let child_index = if idx_size > 0 {
                read_exact(src, idx_offset, idx_size).await?
            } else {
                Vec::new()
            };
            let children_area_offset = idx_offset + idx_size as u64;
            Ok(CopyContainerInfo {
                tag,
                child_count,
                child_index,
                children_area_offset,
                key_strings: Vec::new(),
                key_data_used: 0,
            })
        }
        _ => Err(BlobError::UnknownNodeType(tag)),
    }
}

/// Stack frame for the iterative structural_copy.
///
/// Each frame accumulates its output into an in-memory `buf`. When all children
/// are processed, the header+index is patched in the buffer. The root frame's
/// buffer is appended to dst; child frames return their buffer to the parent.
struct SmartCopyFrame {
    tag: u8,
    child_count: usize,
    /// Index of the next child to process.
    next_child: usize,
    /// Source child index bytes (v2 format).
    src_index: Vec<u8>,
    /// Source children area offset.
    children_area_src: u64,
    /// Size of the header+index+key_strings placeholder at the start of `buf`.
    header_region_size: usize,
    /// Output buffer: header+index placeholder, then child data appended.
    buf: Vec<u8>,
    /// Output index being built (patched into buf on finalize).
    new_index: Vec<u8>,
    /// For collections: source key strings.
    src_key_strings: Vec<u8>,
    /// For collections: current position in src_key_strings (cursor for O(n) traversal).
    src_key_pos: usize,
    /// For collections: new key strings being built.
    new_key_strings: Vec<u8>,
    /// For collections: total index slots allocated in the placeholder (child_count + reserved).
    total_index_slots: u32,
    /// For collections: total key data space reserved in the placeholder.
    total_key_data_reserved: u32,
    /// For collections: live child count (after filtering TYPE_NULL).
    live_child_count: usize,
    /// Stats accumulated for this container.
    stats: CopyStats,
    /// Bytes of this frame's output already flushed to dst (chunked flushing for >1GB).
    /// Only non-zero for the root frame when output exceeds COMPACT_FLUSH_THRESHOLD.
    bytes_flushed: u64,
}

/// Write a v2 collection index entry into the output index buffer.
fn write_collection_index_entry(
    new_index: &mut [u8],
    out_i: usize,
    src_index: &[u8],
    src_i: usize,
    rel_offset: u64,
    size: u64,
    type_flags: u8,
) {
    let dst_off = out_i * COLLECTION_INDEX_ENTRY_SIZE;
    let src_off = src_i * COLLECTION_INDEX_ENTRY_SIZE;
    // Copy key_hash from source
    new_index[dst_off..dst_off + 8].copy_from_slice(&src_index[src_off..src_off + 8]);
    new_index[dst_off + 8] = type_flags;
    new_index[dst_off + 9..dst_off + 17].copy_from_slice(&rel_offset.to_le_bytes());
    new_index[dst_off + 17..dst_off + 25].copy_from_slice(&size.to_le_bytes());
}

/// Write a v2 array index entry into the output index buffer.
fn write_array_index_entry(
    new_index: &mut [u8],
    out_i: usize,
    rel_offset: u64,
    size: u64,
    type_flags: u8,
) {
    let dst_off = out_i * ARRAY_INDEX_ENTRY_SIZE;
    new_index[dst_off] = type_flags;
    new_index[dst_off + 1..dst_off + 9].copy_from_slice(&rel_offset.to_le_bytes());
    new_index[dst_off + 9..dst_off + 17].copy_from_slice(&size.to_le_bytes());
}

/// Structurally copy a node (and all descendants) from `src` to `dst`.
///
/// Produces a fully clean output: no forward pointers, no dead space,
/// appended_bytes = 0 on all containers. All child data is buffered in
/// memory and written to dst in a single append.
///
/// All containers are descended into and processed. Leaf nodes are
/// read via `pread` from src into the parent's buffer.
///
/// Uses an explicit stack instead of recursion to avoid per-node heap
/// allocations.
pub(crate) async fn structural_copy<IO: BlobIO>(
    src: &IO,
    root_offset: u64,
    dst: &IO,
    _field_id_size: FieldIdSize,
    dict: Option<&crate::dictionary::Dictionary>,
) -> Result<CopyStats> {
    let mut total_stats = CopyStats::default();
    let mut stack: Vec<SmartCopyFrame> = Vec::new();
    let mut nodes_since_yield = 0u32;

    // For chunked flushing of the root frame: the file offset where the root
    // frame's output starts. Set on the first flush, stays None if output < 1GB.
    let mut chunked_start: Option<u64> = None;

    // Read root container and push initial frame
    let root = read_container_info(src, root_offset).await?;
    push_frame(root, &mut stack);

    loop {
        // Yield periodically for cooperative runtimes
        nodes_since_yield += 1;
        if nodes_since_yield >= 1024 {
            dst.yield_point().await;
            nodes_since_yield = 0;
        }

        let frame = match stack.last_mut() {
            None => return Ok(total_stats),
            Some(f) => f,
        };

        if frame.next_child >= frame.child_count {
            // All children processed — finalize this frame
            let mut frame = stack.pop().unwrap();
            total_stats.node_count += frame.live_child_count as u64 + 1;
            total_stats.forwards_followed += frame.stats.forwards_followed;

            if stack.is_empty() {
                // Root frame: write to dst
                if let Some(start) = chunked_start {
                    // Chunked path: flush remaining buf, then patch header
                    if !frame.buf.is_empty() {
                        dst.pwrite(start + frame.bytes_flushed, &frame.buf).await?;
                        frame.bytes_flushed += frame.buf.len() as u64;
                    }
                    let subtree_size = frame.bytes_flushed;
                    let hdr = build_header_buf(&frame, subtree_size);
                    dst.pwrite(start, &hdr).await?;
                    total_stats.bytes_written = subtree_size;
                } else {
                    // Small path: finalize in buffer, append to dst
                    let subtree_size = finalize_frame_buf(&mut frame);
                    dst.append(&frame.buf).await?;
                    total_stats.bytes_written = subtree_size;
                }
                return Ok(total_stats);
            }

            // Child frame: finalize header, then append buffer to parent's buffer
            let subtree_size = finalize_frame_buf(&mut frame);
            let parent = stack.last_mut().unwrap();
            let child_rel =
                parent.bytes_flushed + parent.buf.len() as u64 - parent.header_region_size as u64;
            match parent.tag {
                TYPE_COLLECTION => write_collection_index_entry(
                    &mut parent.new_index,
                    parent.live_child_count,
                    &parent.src_index,
                    parent.next_child - 1,
                    child_rel,
                    subtree_size,
                    make_type_flags(frame.tag, false),
                ),
                TYPE_ARRAY => write_array_index_entry(
                    &mut parent.new_index,
                    parent.live_child_count,
                    child_rel,
                    subtree_size,
                    make_type_flags(frame.tag, false),
                ),
                _ => {}
            }
            if parent.tag == TYPE_COLLECTION {
                copy_key_string_at_pos(parent, dict);
            }
            parent.live_child_count += 1;
            parent.buf.extend_from_slice(&frame.buf);

            // Check if root frame needs chunked flush after appending child
            if stack.len() == 1 {
                let root = &mut stack[0];
                if root.buf.len() >= COMPACT_FLUSH_THRESHOLD {
                    if chunked_start.is_none() {
                        chunked_start = Some(dst.size().await?);
                    }
                    let start = chunked_start.unwrap();
                    dst.pwrite(start + root.bytes_flushed, &root.buf).await?;
                    root.bytes_flushed += root.buf.len() as u64;
                    root.buf.clear();
                }
            }
            continue;
        }

        let child_i = frame.next_child;
        frame.next_child += 1;

        // Parse child from the source index — zero I/O
        let entry = match frame.tag {
            TYPE_COLLECTION => {
                parse_collection_child(&frame.src_index, child_i, frame.children_area_src)
            }
            TYPE_ARRAY => parse_array_child(&frame.src_index, child_i, frame.children_area_src),
            _ => unreachable!(),
        };

        let child_type = extract_type_tag(entry.type_flags);
        if entry.type_flags == 0 && entry.size == 0 {
            // Zeroed-out slot (unused reserved space) — skip
            if frame.tag == TYPE_COLLECTION {
                skip_key_string(frame);
            }
            continue;
        }
        if child_type == TYPE_NULL && frame.tag == TYPE_COLLECTION {
            // Deleted collection child (null = deletion) — skip
            skip_key_string(frame);
            continue;
        }

        let is_container = matches!(child_type, TYPE_COLLECTION | TYPE_ARRAY);

        if is_container {
            // Always descend into containers to resolve any forwarded children
            frame.stats.forwards_followed += if is_forwarded_flag(entry.type_flags) {
                1
            } else {
                0
            };

            let container = read_container_info(src, entry.src_offset).await?;
            push_frame(container, &mut stack);
            continue;
        }

        // Clean child (leaf or clean container): read bytes into buffer
        if is_forwarded_flag(entry.type_flags) {
            frame.stats.forwards_followed += 1;
        }

        // Record this child in the output index (clean: not forwarded)
        let rel = frame.bytes_flushed + frame.buf.len() as u64 - frame.header_region_size as u64;
        let clean_flags = make_type_flags(extract_type_tag(entry.type_flags), false);
        match frame.tag {
            TYPE_COLLECTION => write_collection_index_entry(
                &mut frame.new_index,
                frame.live_child_count,
                &frame.src_index,
                child_i,
                rel,
                entry.size,
                clean_flags,
            ),
            TYPE_ARRAY => write_array_index_entry(
                &mut frame.new_index,
                frame.live_child_count,
                rel,
                entry.size,
                clean_flags,
            ),
            _ => {}
        }
        if frame.tag == TYPE_COLLECTION {
            copy_key_string_at_pos(frame, dict);
        }
        frame.live_child_count += 1;

        // Read child data from src into buffer
        let start = frame.buf.len();
        frame.buf.resize(start + entry.size as usize, 0);
        src.pread_into(
            entry.src_offset,
            &mut frame.buf[start..start + entry.size as usize],
        )
        .await?;

        // Check if root frame needs chunked flush after adding clean child
        if stack.len() == 1 {
            let root = &mut stack[0];
            if root.buf.len() >= COMPACT_FLUSH_THRESHOLD {
                if chunked_start.is_none() {
                    chunked_start = Some(dst.size().await?);
                }
                let start = chunked_start.unwrap();
                dst.pwrite(start + root.bytes_flushed, &root.buf).await?;
                root.bytes_flushed += root.buf.len() as u64;
                root.buf.clear();
            }
        }
    }
}

/// Push a new frame for a container onto the stack.
///
/// Creates a frame with an in-memory buffer pre-sized for the header+index
/// placeholder. No writes to dst — everything stays in memory.
fn push_frame(info: CopyContainerInfo, stack: &mut Vec<SmartCopyFrame>) {
    let (header_size, index_entry_size) = match info.tag {
        TYPE_COLLECTION => (COLLECTION_HEADER_SIZE, COLLECTION_INDEX_ENTRY_SIZE),
        TYPE_ARRAY => (ARRAY_HEADER_SIZE, ARRAY_INDEX_ENTRY_SIZE),
        _ => return, // caller should have validated
    };

    if info.tag == TYPE_COLLECTION {
        // Check if any key is a push ID (inline key starting with '-').
        let has_push_id_keys = has_inline_push_id_key(&info.key_strings, info.child_count);
        // Sum child sizes to inform reserved space decision.
        let total_children_size: u64 = (0..info.child_count)
            .map(|i| {
                let eo = i * COLLECTION_INDEX_ENTRY_SIZE;
                u64::from_le_bytes(info.child_index[eo + 17..eo + 25].try_into().unwrap())
            })
            .sum();
        let new_reserved_count = compute_reserved_count(
            info.child_count as u32,
            total_children_size,
            has_push_id_keys,
        );
        let avg_key_entry = if info.child_count > 0 && info.key_data_used > 0 {
            std::cmp::max(24, info.key_data_used / info.child_count as u32)
        } else {
            24
        };
        let new_key_data_reserved = info.key_data_used + new_reserved_count * avg_key_entry;
        let total_slots = info.child_count as u32 + new_reserved_count;
        let header_region_size =
            header_size + total_slots as usize * index_entry_size + new_key_data_reserved as usize;

        let new_index_size = info.child_count * index_entry_size;
        let key_strings_cap = info.key_data_used as usize;
        let mut buf = Vec::with_capacity(header_region_size * 2);
        buf.resize(header_region_size, 0); // placeholder

        stack.push(SmartCopyFrame {
            tag: info.tag,
            child_count: info.child_count,
            next_child: 0,
            src_index: info.child_index,
            children_area_src: info.children_area_offset,
            header_region_size,
            buf,
            new_index: vec![0u8; new_index_size],
            src_key_strings: info.key_strings,
            src_key_pos: 0,
            new_key_strings: Vec::with_capacity(key_strings_cap),
            total_index_slots: total_slots,
            total_key_data_reserved: new_key_data_reserved,
            live_child_count: 0,
            stats: CopyStats::default(),
            bytes_flushed: 0,
        });
    } else {
        let header_region_size = header_size + info.child_count * index_entry_size;
        let new_index_size = info.child_count * index_entry_size;

        let mut buf = Vec::with_capacity(header_region_size * 2);
        buf.resize(header_region_size, 0); // placeholder

        stack.push(SmartCopyFrame {
            tag: info.tag,
            child_count: info.child_count,
            next_child: 0,
            src_index: info.child_index,
            children_area_src: info.children_area_offset,
            header_region_size,
            buf,
            new_index: vec![0u8; new_index_size],
            src_key_strings: Vec::new(),
            src_key_pos: 0,
            new_key_strings: Vec::new(),
            total_index_slots: 0,
            total_key_data_reserved: 0,
            live_child_count: 0,
            stats: CopyStats::default(),
            bytes_flushed: 0,
        });
    }
}

/// Advance the key string cursor past one entry without copying.
/// Used when skipping deleted children in a collection.
fn skip_key_string(frame: &mut SmartCopyFrame) {
    let pos = frame.src_key_pos;
    if pos + 2 > frame.src_key_strings.len() {
        return;
    }
    let raw = u16::from_le_bytes(frame.src_key_strings[pos..pos + 2].try_into().unwrap());
    if raw & KEY_DICT_FLAG != 0 {
        frame.src_key_pos = pos + 2; // dict-ref: no inline bytes
    } else {
        frame.src_key_pos = pos + 2 + raw as usize; // inline
    }
}

/// Copy the key string at the current cursor position and advance the cursor.
/// O(1) per call — the cursor tracks our position through the key string table.
///
/// If `dict` is Some, inline non-collection keys are converted to dict-ref
/// encoding if the key exists in the dictionary. Keys not in the dictionary
/// stay inline. Collection keys (push IDs starting with '-') always stay inline.
fn copy_key_string_at_pos(
    frame: &mut SmartCopyFrame,
    dict: Option<&crate::dictionary::Dictionary>,
) {
    let pos = frame.src_key_pos;
    if pos + 2 > frame.src_key_strings.len() {
        return;
    }
    let raw = u16::from_le_bytes(frame.src_key_strings[pos..pos + 2].try_into().unwrap());
    if raw & KEY_DICT_FLAG != 0 {
        // Already a dict-ref — copy verbatim
        frame
            .new_key_strings
            .extend_from_slice(&frame.src_key_strings[pos..pos + 2]);
        frame.src_key_pos = pos + 2;
    } else {
        // Inline key string
        let key_len = raw as usize;
        let entry_end = pos + 2 + key_len;
        if entry_end > frame.src_key_strings.len() {
            return;
        }
        let key_bytes = &frame.src_key_strings[pos + 2..entry_end];

        if let Some(d) = dict {
            let key_str = std::str::from_utf8(key_bytes).unwrap_or("");
            if !key_str.is_empty()
                && !crate::dictionary::is_collection_key(key_str)
                && let Some(field_id) = d.lookup(key_str)
            {
                frame
                    .new_key_strings
                    .extend_from_slice(&(KEY_DICT_FLAG | field_id as u16).to_le_bytes());
                frame.src_key_pos = entry_end;
                return;
            }
        }
        // Copy inline verbatim (no dict, or key not in dict, or it's a collection key)
        frame
            .new_key_strings
            .extend_from_slice(&frame.src_key_strings[pos..entry_end]);
        frame.src_key_pos = entry_end;
    }
}

/// Build the header region (header + index + key strings for collections) as
/// a standalone buffer. Used by both in-buffer finalization and chunked output.
fn build_header_buf(frame: &SmartCopyFrame, subtree_size: u64) -> Vec<u8> {
    let mut hdr = vec![0u8; frame.header_region_size];
    match frame.tag {
        TYPE_COLLECTION => {
            let live_count = frame.live_child_count as u32;
            let live_key_data_used = frame.new_key_strings.len() as u32;
            let reserved_count = frame.total_index_slots - live_count;
            let key_data_reserved = frame.total_key_data_reserved;

            hdr[0] = TYPE_COLLECTION;
            hdr[1..9].copy_from_slice(&subtree_size.to_le_bytes());
            hdr[9..13].copy_from_slice(&live_count.to_le_bytes());
            hdr[13..17].copy_from_slice(&reserved_count.to_le_bytes());
            hdr[17..21].copy_from_slice(&live_key_data_used.to_le_bytes());
            hdr[21..25].copy_from_slice(&key_data_reserved.to_le_bytes());
            // appended_bytes at [25..29] already 0

            let idx_start = COLLECTION_HEADER_SIZE;
            let used_index_size = frame.live_child_count * COLLECTION_INDEX_ENTRY_SIZE;
            if used_index_size > 0 {
                hdr[idx_start..idx_start + used_index_size]
                    .copy_from_slice(&frame.new_index[..used_index_size]);
            }

            let total_slots = frame.total_index_slots;
            let key_strings_start = idx_start + total_slots as usize * COLLECTION_INDEX_ENTRY_SIZE;
            if !frame.new_key_strings.is_empty() {
                hdr[key_strings_start..key_strings_start + frame.new_key_strings.len()]
                    .copy_from_slice(&frame.new_key_strings);
            }
        }
        TYPE_ARRAY => {
            hdr[0] = TYPE_ARRAY;
            hdr[1..9].copy_from_slice(&subtree_size.to_le_bytes());
            hdr[9..13].copy_from_slice(&(frame.live_child_count as u32).to_le_bytes());
            // appended_bytes at [13..17] already 0
            let used_index_size = frame.live_child_count * ARRAY_INDEX_ENTRY_SIZE;
            if used_index_size > 0 {
                hdr[ARRAY_HEADER_SIZE..ARRAY_HEADER_SIZE + used_index_size]
                    .copy_from_slice(&frame.new_index[..used_index_size]);
            }
        }
        _ => {}
    }
    hdr
}

/// Finalize a completed container frame: patch header + index in the buffer.
/// Returns the total subtree size.
fn finalize_frame_buf(frame: &mut SmartCopyFrame) -> u64 {
    let subtree_size = frame.bytes_flushed + frame.buf.len() as u64;
    let hdr = build_header_buf(frame, subtree_size);
    frame.buf[..hdr.len()].copy_from_slice(&hdr);
    subtree_size
}

// ---------------------------------------------------------------------------
// compact_subtree: compact one dirty subtree to EOF
// ---------------------------------------------------------------------------

/// Compact a single dirty subtree by structurally copying it to EOF.
///
/// 1. Structurally copy the subtree (following all forwards) into a buffer
/// 2. Append the clean copy at EOF of the blob
/// 3. Update the parent's child index entry to point to the new location
///
/// The old subtree bytes become dead space (reclaimed on full compaction).
///
/// `parent_offset` is the offset of the parent container node.
/// `child_index_in_parent` is which entry in the parent's child index to update.
pub async fn compact_subtree<IO: BlobIO>(
    io: &IO,
    subtree_offset: u64,
    parent_offset: u64,
    child_index_in_parent: usize,
    field_id_size: FieldIdSize,
) -> Result<CopyStats> {
    // Step 1: structurally copy to EOF
    let new_offset = io.size().await?;
    let copy_stats = {
        let read_handle = io.clone_for_reading().await?;
        let stats = structural_copy(&read_handle, subtree_offset, io, field_id_size, None).await?;
        read_handle.close().await?;
        stats
    };

    // Step 2: update parent's child index entry (v2 format)
    // Set is_forwarded=true, store absolute offset and new subtree_size
    let parent_tag = read_exact(io, parent_offset, 1).await?[0];
    let new_subtree_size = copy_stats.bytes_written;

    match parent_tag {
        TYPE_COLLECTION => {
            // v2: (key_hash:8, type_flags:1, offset:8, size:8) = 25 bytes
            let index_start = parent_offset + COLLECTION_HEADER_SIZE as u64;
            let entry_pos =
                index_start + child_index_in_parent as u64 * COLLECTION_INDEX_ENTRY_SIZE as u64;
            let mut tf_buf = [0u8; 1];
            read_exact_into(io, entry_pos + 8, &mut tf_buf).await?;
            let child_type = extract_type_tag(tf_buf[0]);
            let new_flags = make_type_flags(child_type, true);
            let mut patch = [0u8; 17]; // type_flags + offset + size
            patch[0] = new_flags;
            patch[1..9].copy_from_slice(&new_offset.to_le_bytes());
            patch[9..17].copy_from_slice(&new_subtree_size.to_le_bytes());
            io.pwrite(entry_pos + 8, &patch).await?;
        }
        TYPE_ARRAY => {
            // v2: (type_flags:1, offset:8, size:8) = 17 bytes
            let entry_pos = parent_offset
                + ARRAY_HEADER_SIZE as u64
                + (child_index_in_parent * ARRAY_INDEX_ENTRY_SIZE) as u64;
            let mut tf_buf = [0u8; 1];
            read_exact_into(io, entry_pos, &mut tf_buf).await?;
            let child_type = extract_type_tag(tf_buf[0]);
            let new_flags = make_type_flags(child_type, true);
            let mut patch = [0u8; 17]; // type_flags + offset + size
            patch[0] = new_flags;
            patch[1..9].copy_from_slice(&new_offset.to_le_bytes());
            patch[9..17].copy_from_slice(&new_subtree_size.to_le_bytes());
            io.pwrite(entry_pos, &patch).await?;
        }
        _ => return Err(BlobError::NotAContainer(parent_offset, parent_tag)),
    }

    Ok(copy_stats)
}

// ---------------------------------------------------------------------------
// full_compact: rewrite entire blob clean via structural copy
// ---------------------------------------------------------------------------

/// Rewrite a blob clean: no forward pointers, no dead space.
///
/// Deep-copies the entire tree from `src` to `dst` using `structural_copy` —
/// follows every forwarded child at every level, producing a fully clean blob.
/// This deep walk is necessary because children may contain forward pointers
/// that reference offsets in the source file; copying bytes verbatim without
/// resolving those forwards would produce a broken blob.
///
/// The dictionary is copied as-is (field_ids are preserved).
/// Collection reserved space is recomputed fresh.
pub async fn full_compact<IO: BlobIO>(src: &IO, dst: &IO) -> Result<BlobStats> {
    let src_header = read_header(src).await?;
    let dict = read_dictionary(src, &src_header).await?;
    let field_id_size = src_header.field_id_size()?;

    // Write placeholder header
    dst.append(&[0u8; HEADER_SIZE]).await?;

    // Structurally copy the tree, converting inline keys to dict-ref
    // where the key already exists in the dictionary.
    let root_offset = dst.size().await?;
    let _stats =
        structural_copy(src, src_header.root_offset, dst, field_id_size, Some(&dict)).await?;

    // Rebuild dictionary with proper sorted hashes and reserved space
    let rebuilt_dict = crate::dictionary::Dictionary::build(dict.field_names().to_vec());

    // Write dictionary after the tree (new keys discovered during structural_copy)
    let dict_offset = dst.size().await?;
    let dict_bytes = rebuilt_dict.to_bytes();
    dst.append(&dict_bytes).await?;

    let total_size = dst.size().await?;

    // Write final header
    let new_header = BlobHeader {
        version: VERSION,
        flags: field_id_size.to_flags(),
        dict_offset,
        root_offset,
        node_count: src_header.node_count,
        total_size,
        dict_field_count: rebuilt_dict.field_count(),
    };
    dst.pwrite(0, &new_header.to_bytes()).await?;

    Ok(BlobStats {
        total_size,
        node_count: src_header.node_count,
        dict_field_count: rebuilt_dict.field_count(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arc_value::ArcValue;
    use crate::incremental::apply_updates;
    use crate::io::MemBlobIO;
    use crate::session::BlobSession;
    use crate::session_reader::{read_dictionary, read_header};
    use crate::writer::write_blob;
    use serde_json::json;

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        futures::executor::block_on(f)
    }

    async fn read_root<IO: crate::io::BlobIO>(io: &IO) -> ArcValue {
        let session = BlobSession::open(io.clone_for_reading().await.unwrap())
            .await
            .unwrap();
        session.read_subtree(&[]).await.unwrap()
    }

    async fn read_at_path<IO: crate::io::BlobIO>(io: &IO, path: &[&str]) -> ArcValue {
        let session = BlobSession::open(io.clone_for_reading().await.unwrap())
            .await
            .unwrap();
        session.read_subtree(path).await.unwrap()
    }

    #[test]
    fn test_full_compact_basic() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "characters": {
                    "abc": {"hp": 100, "name": "Hero"},
                    "def": {"hp": 50, "name": "Villain"}
                },
                "config": {"mode": "dark"}
            }));
            let src = MemBlobIO::new();
            write_blob(&src, &tree).await.unwrap();

            let dst = MemBlobIO::new();
            let stats = full_compact(&src, &dst).await.unwrap();

            let result = read_root(&dst).await;

            assert_eq!(result, tree);
            assert_eq!(stats.node_count, 10);
        });
    }

    #[test]
    fn test_compact_after_updates() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "a": "short",
                "b": "short",
                "c": "short"
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();
            let original_size = io.size().await.unwrap();

            let updates = vec![
                (
                    vec!["a".to_string()],
                    Some(ArcValue::from(
                        "this is a much longer string that creates dead space",
                    )),
                ),
                (
                    vec!["b".to_string()],
                    Some(ArcValue::from(
                        "another long replacement string here for testing",
                    )),
                ),
            ];
            apply_updates(&io, &updates).await.unwrap();
            let bloated_size = io.size().await.unwrap();
            assert!(bloated_size > original_size);

            let dst = MemBlobIO::new();
            let stats = full_compact(&io, &dst).await.unwrap();

            assert!(stats.total_size < bloated_size);

            let a = read_at_path(&dst, &["a"]).await;
            assert_eq!(
                a.as_str(),
                Some("this is a much longer string that creates dead space")
            );

            let b = read_at_path(&dst, &["b"]).await;
            assert_eq!(
                b.as_str(),
                Some("another long replacement string here for testing")
            );

            let c = read_at_path(&dst, &["c"]).await;
            assert_eq!(c.as_str(), Some("short"));
        });
    }

    #[test]
    fn test_compact_no_forward_nodes() {
        block_on(async {
            let tree = ArcValue::from_value(json!({"x": "small"}));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let updates = vec![(
                vec!["x".to_string()],
                Some(ArcValue::from("a much longer string to force tombstone")),
            )];
            apply_updates(&io, &updates).await.unwrap();

            let dst = MemBlobIO::new();
            full_compact(&io, &dst).await.unwrap();

            let result = read_root(&dst).await;
            assert_eq!(
                result.get("x").unwrap().as_str(),
                Some("a much longer string to force tombstone")
            );
        });
    }

    #[test]
    fn test_compact_after_delete() {
        block_on(async {
            let tree = ArcValue::from_value(json!({"a": 1, "b": 2, "c": 3}));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let updates = vec![(vec!["b".to_string()], None)];
            apply_updates(&io, &updates).await.unwrap();

            let dst = MemBlobIO::new();
            full_compact(&io, &dst).await.unwrap();

            let result = read_root(&dst).await;

            assert_eq!(result.get("a").unwrap().as_i64(), Some(1));
            assert!(result.get("b").is_none());
            assert_eq!(result.get("c").unwrap().as_i64(), Some(3));

            // Dictionary is copied as-is (not rebuilt), so it still contains "b".
            // This is harmless — unused fields are reclaimed if we ever add
            // dictionary compaction.
        });
    }

    #[test]
    fn test_compact_compacted_is_idempotent() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "data": {"nested": {"deep": 42}}
            }));
            let src = MemBlobIO::new();
            write_blob(&src, &tree).await.unwrap();

            let dst1 = MemBlobIO::new();
            let stats1 = full_compact(&src, &dst1).await.unwrap();

            let dst2 = MemBlobIO::new();
            let stats2 = full_compact(&dst1, &dst2).await.unwrap();

            assert_eq!(stats1.total_size, stats2.total_size);
            assert_eq!(stats1.node_count, stats2.node_count);

            let result = read_root(&dst2).await;
            assert_eq!(result, tree);
        });
    }

    #[test]
    fn test_compact_with_collection() {
        block_on(async {
            // Blob with TYPE_COLLECTION (push ID keys)
            let tree = ArcValue::from_value(json!({
                "chat": {
                    "-Mabc123": {"text": "hello", "ts": 1000},
                    "-Mdef456": {"text": "world", "ts": 2000}
                }
            }));
            let src = MemBlobIO::new();
            write_blob(&src, &tree).await.unwrap();

            // Apply an update to create a forward inside the collection
            let updates = vec![(
                vec![
                    "chat".to_string(),
                    "-Mabc123".to_string(),
                    "text".to_string(),
                ],
                Some(ArcValue::from(
                    "a much longer replacement text that forces tombstone+append",
                )),
            )];
            apply_updates(&src, &updates).await.unwrap();

            let dst = MemBlobIO::new();
            let stats = full_compact(&src, &dst).await.unwrap();

            let text = read_at_path(&dst, &["chat", "-Mabc123", "text"]).await;
            assert_eq!(
                text.as_str(),
                Some("a much longer replacement text that forces tombstone+append")
            );

            let text2 = read_at_path(&dst, &["chat", "-Mdef456", "text"]).await;
            assert_eq!(text2.as_str(), Some("world"));

            assert!(stats.total_size < src.size().await.unwrap());
        });
    }

    #[test]
    fn test_compact_subtree_basic() {
        block_on(async {
            // Create a blob, apply updates to create forwards in a subtree,
            // then compact just that subtree.
            // Note: "a" subtree must be large enough that the forward doesn't
            // trigger cascading auto-compaction (appended_bytes < subtree_size/2).
            let tree = ArcValue::from_value(json!({
                "a": {
                    "x": "small_value",
                    "p1": "padding_data_padding_data_padding_data_pad",
                    "p2": "more_padding_here_for_extra_safety_margin"
                },
                "b": {"y": 42}
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            // Create a forward in the "a" subtree
            let updates = vec![(
                vec!["a".to_string(), "x".to_string()],
                Some(ArcValue::from(
                    "a much longer string to force a forward pointer here",
                )),
            )];
            apply_updates(&io, &updates).await.unwrap();

            // Navigate to find the "a" subtree offset and its position in root's child index
            let header = read_header(&io).await.unwrap();
            let dict = read_dictionary(&io, &header).await.unwrap();
            let field_id_size = header.field_id_size().unwrap();

            // Read root container (TYPE_COLLECTION) and find "a" via find_in_collection
            let root_container = crate::nav_cache::read_container(&io, header.root_offset, None)
                .await
                .unwrap();
            let (_, _type_flags, a_offset, _) = root_container
                .find_in_collection("a", &dict)
                .unwrap()
                .unwrap();

            // Find the index position of "a" in the root's child index
            let a_hash = crate::dictionary::hash_field_name("a");
            let mut a_idx = 0;
            for i in 0..root_container.child_count as usize {
                let eo = i * COLLECTION_INDEX_ENTRY_SIZE;
                let h =
                    u64::from_le_bytes(root_container.child_index[eo..eo + 8].try_into().unwrap());
                if h == a_hash {
                    a_idx = i;
                    break;
                }
            }

            // Compact the "a" subtree
            let copy_stats =
                compact_subtree(&io, a_offset, header.root_offset, a_idx, field_id_size)
                    .await
                    .unwrap();
            assert!(copy_stats.bytes_written > 0);

            // Verify the whole blob still reads correctly
            let a_val = read_at_path(&io, &["a", "x"]).await;
            assert_eq!(
                a_val.as_str(),
                Some("a much longer string to force a forward pointer here")
            );
            let b_val = read_at_path(&io, &["b", "y"]).await;
            assert_eq!(b_val.as_i64(), Some(42));
        });
    }

    /// Compaction round-trips integer-keyed objects exactly, including the gaps
    /// left by dropped null elements, so nested arrays render back unchanged.
    #[test]
    fn test_structural_copy_preserves_array_gaps() {
        block_on(async {
            let original = json!({
                "data": {
                    "root": [
                        [null, null, ["a", "b"], ["c", "d"]],
                        null,
                        [null, "x", null, "y"],
                        {"nested": [null, 1, null, 2]}
                    ]
                }
            });
            let tree = ArcValue::from_value(original.clone());
            let src = MemBlobIO::new();
            write_blob(&src, &tree).await.unwrap();

            let dst = MemBlobIO::new();
            full_compact(&src, &dst).await.unwrap();

            let result = read_root(&dst).await;
            // Compaction round-trips the stored tree exactly,
            assert_eq!(result, tree);
            // and it renders back to the original arrays, with nulls at the
            // positions of the dropped (gap) elements.
            assert_eq!(result.to_value(), original);
        });
    }

    /// Legacy on-disk arrays (TYPE_ARRAY) decode to integer-keyed objects on
    /// read, so existing blobs migrate transparently and still render as arrays.
    #[test]
    fn test_legacy_on_disk_array_migrates_to_object() {
        block_on(async {
            // A native array writes the legacy TYPE_ARRAY form on disk.
            let legacy = ArcValue::Array(std::sync::Arc::new(vec![
                ArcValue::String("a".into()),
                ArcValue::Null,
                ArcValue::String("c".into()),
            ]));
            let tree = ArcValue::Object(std::sync::Arc::new(
                [("arr".to_string(), legacy)].into_iter().collect(),
            ));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            // On read it is an integer-keyed object; the null became a gap.
            let arr = read_at_path(&io, &["arr"]).await;
            assert!(arr.is_object());
            assert_eq!(arr.get("0").unwrap().as_str(), Some("a"));
            assert!(arr.get("1").is_none());
            assert_eq!(arr.get("2").unwrap().as_str(), Some("c"));
            // ...and it renders back as the array with the gap as null.
            assert_eq!(arr.to_value(), json!(["a", null, "c"]));
        });
    }
}
