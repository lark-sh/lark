//! BlobSession: cached session for ongoing blob operations.
//!
//! Caches header, dictionary, and field_id_size across calls so the
//! compactor doesn't re-read them on every WAL batch.

use crate::arc_value::ArcValue;
use crate::dictionary::Dictionary;
use crate::error::{BlobError, Result};
use crate::format::{BlobHeader, FieldIdSize, HEADER_SIZE, VERSION};
use crate::free_list::FreeList;
use crate::incremental::IncrementalStats;
use crate::io::BlobIO;
use crate::segment::Sidecar;
use std::collections::HashSet;
use tracing::debug;

/// Result of a shallow read at a path.
pub enum ShallowValue {
    /// The path pointed to a primitive — here's the actual value.
    Primitive(ArcValue),
    /// The path pointed to a container — here are the immediate children.
    Children(Vec<ShallowChild>),
}

/// A single child entry in a shallow read result.
pub struct ShallowChild {
    /// The child's key name.
    pub key: String,
    /// The child's subtree size in bytes (from the parent's index entry).
    pub size: u64,
    /// `Some(value)` for primitive children, `None` for container children.
    pub value: Option<ArcValue>,
}

/// Result of applying a batch of WAL updates.
///
/// The dictionary is never mutated during incremental compaction — new
/// structural keys are written inline in collection key_string areas.
/// The dictionary is only rebuilt during full compaction (file rotation).
pub enum ApplyResult {
    /// Updates applied to the existing file. No changes needed by readers.
    Applied(IncrementalStats),
}

/// Session for ongoing operations on an existing blob.
///
/// Owns the IO handle and caches header, dictionary, and field_id_size
/// in memory. The dictionary is never mutated during incremental
/// compaction — new structural keys are written inline in collection
/// key_string areas. The dictionary is only rebuilt during full
/// compaction (file rotation), so reader sessions never go stale.
///
/// Root compaction is NOT performed automatically by `apply_updates`.
/// The root can accumulate fragmentation indefinitely. When the caller
/// wants to compact, call `root_compact` manually (e.g., on a schedule).
pub struct BlobSession<IO: BlobIO> {
    pub(crate) io: IO,
    pub(crate) header: BlobHeader,
    pub(crate) dict: Dictionary,
    pub(crate) field_id_size: FieldIdSize,
    pub(crate) free_list: FreeList,
    /// Non-collection keys that were written inline (not in the dictionary)
    /// during incremental updates. Accumulated across batches and drained
    /// into the dictionary during root_compact.
    pub(crate) pending_keys: HashSet<String>,
}

impl<IO: BlobIO> BlobSession<IO> {
    /// Open an existing blob — reads and caches header and dictionary.
    /// Free list starts empty. Use `open_with_sidecar` to restore it.
    pub async fn open(io: IO) -> Result<Self> {
        Self::open_with_sidecar::<IO>(io, None::<&IO>).await
    }

    /// Open an existing blob, restoring the free list from a sidecar IO.
    ///
    /// The sidecar IO should point to the free list file (e.g., `sidecar.lark`
    /// alongside the blob file). If the sidecar doesn't exist or is empty,
    /// pass `None` — the free list starts empty.
    pub async fn open_with_sidecar<SIO: BlobIO>(io: IO, sidecar: Option<&SIO>) -> Result<Self> {
        let header = crate::session_reader::read_header(&io).await?;
        let field_id_size = header.field_id_size()?;
        let dict = crate::session_reader::read_dictionary(&io, &header).await?;

        let (free_list, pending_keys) = match sidecar {
            Some(sio) => {
                let size = sio.size().await? as usize;
                if size > 0 {
                    let data = sio.pread(0, size).await.map_err(BlobError::Io)?;
                    match Sidecar::from_bytes(&data) {
                        Ok(sc) => {
                            let pk: HashSet<String> = sc.pending_keys.into_iter().collect();
                            (sc.free_list, pk)
                        }
                        Err(_) => (FreeList::new(), HashSet::new()),
                    }
                } else {
                    (FreeList::new(), HashSet::new())
                }
            }
            None => (FreeList::new(), HashSet::new()),
        };

        Ok(BlobSession {
            io,
            header,
            dict,
            field_id_size,
            free_list,
            pending_keys,
        })
    }

    /// Initialize a new empty blob and return a session for it.
    ///
    /// Writes a complete blob with an empty root object to the IO backend.
    /// Use this when creating a new database — it's an error to call `open`
    /// on an IO that doesn't already contain a valid blob.
    pub async fn init(io: IO) -> Result<Self> {
        let empty_root = ArcValue::from_value(serde_json::Value::Object(Default::default()));
        crate::writer::write_blob(&io, &empty_root).await?;
        Self::open(io).await
    }

    /// Apply a batch of WAL updates.
    ///
    /// Each update is `(path, Option<ArcValue>)`:
    /// - `Some(value)`: set the value at path
    /// - `None`: delete the value at path
    ///
    /// Returns `ApplyResult::Applied` with stats on success.
    ///
    /// Root compaction is NOT performed automatically. The root accumulates
    /// fragmentation; call `root_compact` manually when desired.
    /// Apply a batch of WAL updates (no sidecar persistence).
    /// Use `apply_updates_with_sidecar` for production use with free list persistence.
    pub async fn apply_updates(
        &mut self,
        updates: &[(Vec<String>, Option<ArcValue>)],
    ) -> Result<ApplyResult> {
        self.apply_updates_with_sidecar::<IO>(updates, None::<&IO>)
            .await
    }

    /// Apply a batch of WAL updates with sidecar persistence.
    ///
    /// If `sidecar` is provided, the free list is written and synced to it
    /// BEFORE the blob is synced. This ensures crash safety: on recovery,
    /// the worst case is losing some freed regions (leak, not corruption).
    pub async fn apply_updates_with_sidecar<SIO: BlobIO>(
        &mut self,
        updates: &[(Vec<String>, Option<ArcValue>)],
        sidecar: Option<&SIO>,
    ) -> Result<ApplyResult> {
        let mut stats = IncrementalStats::default();

        // Advance free list epoch: previous → available, current → previous.
        // Clear promoted regions from the IO cache.
        let promoted = self.free_list.advance_epoch();
        for (offset, size) in promoted {
            self.io.clear_region(offset, size);
        }

        // Build the update tree (handles coalescing internally)
        let tree = crate::incremental::UpdateNode::build(updates);

        // One read clone per batch — shared by all compactions
        let src = self.io.clone_for_reading().await?;

        // Enable write-back mode: pwrite to cached regions (e.g. collection
        // headers rewritten on each insert) only updates the cache. The
        // actual disk writes happen once at the end via flush_write_back.
        self.io.set_write_back(true);

        // Apply updates. If anything fails, discard all write-back state
        // so partial index updates don't contaminate the next batch.
        let apply_result: Result<()> = async {
            if !tree.is_empty() {
                let mut path_buf = Vec::new();
                self.apply_tree(&src, &tree, &mut path_buf, &mut stats)
                    .await?;
            }
            Ok(())
        }
        .await;

        // Close read clone — done with all compactions for this batch
        src.close().await?;

        if let Err(e) = apply_result {
            self.io.discard_write_back();
            return Err(e);
        }

        // Persist free list to sidecar BEFORE flushing the write-back cache.
        if let Some(sio) = sidecar {
            let pending: Vec<String> = self.pending_keys.iter().cloned().collect();
            let bytes = Sidecar::serialize(&self.free_list, &pending);
            sio.truncate(0).await.map_err(BlobError::Io)?;
            sio.append(&bytes).await.map_err(BlobError::Io)?;
            sio.sync().await.map_err(BlobError::Io)?;
        }

        // Flush deferred write-back regions to disk.
        self.io.flush_write_back().await.map_err(BlobError::Io)?;

        // Update header total_size on disk and in cache
        let total_size = self.io.size().await?;
        self.io.pwrite(32, &total_size.to_le_bytes()).await?;
        self.header.total_size = total_size;

        let read_stats = self.io.take_read_stats();
        stats.pread_count += read_stats.pread_count;
        stats.bytes_read += read_stats.bytes_read;
        stats.cache_hits += read_stats.cache_hits;
        stats.cache_hit_bytes += read_stats.cache_hit_bytes;
        stats.cache_header_misses += read_stats.cache_header_misses;

        // Snapshot free list stats
        stats.bytes_reused = self.free_list.bytes_reused;
        stats.free_regions_available = self.free_list.available_region_count();
        stats.bytes_wasted = self.free_list.bytes_wasted;

        debug!(
            updates_applied = stats.updates_applied,
            in_place = stats.in_place_updates,
            forwards = stats.forward_updates,
            parent_rewrites = stats.parent_rewrites,
            collection_inserts = stats.collection_inserts,
            bytes_appended = stats.bytes_appended,
            bytes_reused = stats.bytes_reused,
            bytes_wasted = stats.bytes_wasted,
            free_regions = stats.free_regions_available,
            pread_count = stats.pread_count,
            bytes_read = stats.bytes_read,
            cache_hits = stats.cache_hits,
            cache_hit_bytes = stats.cache_hit_bytes,
            cache_header_misses = stats.cache_header_misses,
            total_size,
            "apply_updates complete"
        );
        Ok(ApplyResult::Applied(stats))
    }

    /// Compact the entire blob to a new, clean file.
    ///
    /// Deep-copies the entire tree from the current file to `dst`
    /// using `structural_copy` — follows every forwarded child at every
    /// level, producing a fully clean blob with no dead space at any depth.
    ///
    /// After this call, the session owns `dst` and all reads go through it.
    /// Returns the old IO handle so the caller can manage cleanup:
    /// 1. Wait for any concurrent readers to finish with the old file
    /// 2. Close/delete the old file
    pub async fn root_compact(&mut self, dst: IO) -> Result<IO> {
        // Absorb pending keys into a cloned dictionary, then rebuild.
        // This ensures structural_copy can convert inline keys to dict-ref.
        let mut dict_clone = self.dict.clone();
        for key in &self.pending_keys {
            dict_clone.lookup_or_insert(key);
        }
        let rebuilt_dict = Dictionary::build(dict_clone.field_names().to_vec());

        // Write header placeholder, then structural_copy, then dictionary.
        dst.append(&[0u8; HEADER_SIZE]).await?;

        let root_offset = dst.size().await?;

        let copy_stats = crate::compact::structural_copy(
            &self.io,
            self.header.root_offset,
            &dst,
            self.field_id_size,
            Some(&rebuilt_dict),
        )
        .await?;

        let dict_offset = dst.size().await?;
        dst.append(&rebuilt_dict.to_bytes()).await?;

        let total_size = dst.size().await?;

        let new_header = BlobHeader {
            version: VERSION,
            flags: self.field_id_size.to_flags(),
            dict_offset,
            root_offset,
            node_count: copy_stats.node_count,
            total_size,
            dict_field_count: rebuilt_dict.field_count(),
        };
        dst.pwrite(0, &new_header.to_bytes()).await?;

        // Swap to the new file, return the old one
        let old_io = std::mem::replace(&mut self.io, dst);

        // Update cached state
        self.header = new_header;
        self.dict = rebuilt_dict;

        // New file has no dead space — clear the free list
        self.free_list.reset();

        // Pending keys have been absorbed into the rebuilt dictionary
        self.pending_keys.clear();

        Ok(old_io)
    }

    /// Diagnose a path by walking it step-by-step and returning a detailed
    /// trace of every navigation decision. Use this when a verification
    /// failure is detected to understand exactly where the read goes wrong.
    ///
    /// Returns a human-readable multi-line string with one section per
    /// path level, plus a leaf section showing the raw bytes at the final
    /// offset.
    pub async fn diagnose_path(&self, path: &[&str]) -> String {
        use crate::format::*;
        use crate::nav_cache::read_container;

        let mut lines: Vec<String> = Vec::new();
        lines.push(format!("=== DIAGNOSE PATH: /{} ===", path.join("/")));

        let file_size = self.io.size().await.unwrap_or(0);
        let root_offset = self.header.root_offset;

        lines.push(format!(
            "[root] file_size={}, root_offset={}",
            file_size, root_offset
        ));

        let mut current_offset = root_offset;
        let mut current_size_hint: Option<u64> = None;

        for (i, &key) in path.iter().enumerate() {
            lines.push(format!(
                "  step {}: looking up {:?} in container at offset={}",
                i, key, current_offset,
            ));

            let container = read_container(&self.io, current_offset, current_size_hint).await;

            let container = match container {
                Ok(c) => c,
                Err(e) => {
                    lines.push(format!("    ERROR reading container: {:?}", e));
                    let raw = self
                        .io
                        .pread(
                            current_offset,
                            32.min(file_size.saturating_sub(current_offset) as usize),
                        )
                        .await;
                    if let Ok(raw) = raw {
                        lines.push(format!(
                            "    raw bytes at offset {}: {:02x?}",
                            current_offset, raw
                        ));
                    }
                    return lines.join("\n");
                }
            };

            lines.push(format!(
                "    container: tag=0x{:02x}, child_count={}, subtree_size={}, children_area={}",
                container.tag,
                container.child_count,
                container.subtree_size,
                container.children_area_offset,
            ));

            if container.tag != TYPE_COLLECTION {
                lines.push(format!(
                    "    NOT a collection (tag=0x{:02x}), cannot look up key",
                    container.tag
                ));
                return lines.join("\n");
            }

            match container.navigate_collection_with_flags(key, &self.dict) {
                Ok(Some((type_flags, abs_offset, size))) => {
                    let type_tag = extract_type_tag(type_flags);
                    let forwarded = is_forwarded_flag(type_flags);
                    let type_name = match type_tag {
                        TYPE_COLLECTION => "COLLECTION",
                        TYPE_ARRAY => "ARRAY",
                        TYPE_STRING => "STRING",
                        TYPE_NUMBER => "NUMBER",
                        TYPE_BOOL => "BOOL",
                        TYPE_NULL => "NULL",
                        _ => "UNKNOWN",
                    };

                    lines.push(format!(
                        "    found: type_flags=0x{:02x} ({}{}), offset={}, size={}",
                        type_flags,
                        type_name,
                        if forwarded { " FORWARDED" } else { "" },
                        abs_offset,
                        size,
                    ));

                    // Tombstone check
                    if type_tag == TYPE_NULL && size == 0 {
                        lines.push("    TOMBSTONE (deleted child)".to_string());
                        return lines.join("\n");
                    }

                    // Validate offset is within file
                    if abs_offset >= file_size {
                        lines.push(format!(
                            "    WARNING: offset {} >= file_size {} — out of bounds!",
                            abs_offset, file_size,
                        ));
                    }

                    // Read raw bytes at the target offset
                    let peek_len = 32.min(file_size.saturating_sub(abs_offset) as usize);
                    if peek_len > 0 {
                        let raw = self.io.pread(abs_offset, peek_len).await;
                        if let Ok(raw) = raw {
                            let on_disk_tag = raw[0];
                            let tag_match = on_disk_tag == type_tag;
                            lines.push(format!(
                                "    raw bytes at offset {}: {:02x?}{}",
                                abs_offset,
                                raw,
                                if !tag_match {
                                    format!(
                                        " WARNING: on-disk tag 0x{:02x} != index tag 0x{:02x}",
                                        on_disk_tag, type_tag
                                    )
                                } else {
                                    String::new()
                                },
                            ));
                        }
                    }

                    current_offset = abs_offset;
                    current_size_hint = Some(size);
                }
                Ok(None) => {
                    lines.push(format!("    key {:?} NOT FOUND in collection", key));
                    let keys = self.dump_collection_keys(&container);
                    if keys.len() <= 20 {
                        lines.push(format!("    all keys: {:?}", keys));
                    } else {
                        lines.push(format!(
                            "    {} keys total, first 20: {:?}",
                            keys.len(),
                            &keys[..20]
                        ));
                    }
                    return lines.join("\n");
                }
                Err(e) => {
                    lines.push(format!("    ERROR looking up key: {:?}", e));
                    return lines.join("\n");
                }
            }
        }

        // We've consumed all path components — report on the leaf
        lines.push(format!(
            "  LEAF: offset={}, size_hint={:?}",
            current_offset, current_size_hint
        ));

        // Read raw bytes at leaf
        let peek_len = 64.min(file_size.saturating_sub(current_offset) as usize);
        if peek_len > 0 {
            let raw = self.io.pread(current_offset, peek_len).await;
            if let Ok(raw) = raw {
                lines.push(format!(
                    "  leaf raw bytes (first {}): {:02x?}",
                    peek_len, raw
                ));
                let tag = raw[0];
                match tag {
                    TYPE_NUMBER => {
                        if raw.len() >= 9 {
                            let f = f64::from_le_bytes(raw[1..9].try_into().unwrap());
                            lines.push(format!("  leaf NUMBER value: {}", f));
                        }
                    }
                    TYPE_STRING => {
                        if raw.len() >= 5 {
                            let len = u32::from_le_bytes(raw[1..5].try_into().unwrap());
                            let preview_end = 5 + (len as usize).min(50).min(raw.len() - 5);
                            let preview = String::from_utf8_lossy(&raw[5..preview_end]);
                            lines.push(format!("  leaf STRING len={}, preview={:?}", len, preview));
                        }
                    }
                    TYPE_BOOL => {
                        if raw.len() >= 2 {
                            lines.push(format!("  leaf BOOL value: {}", raw[1] != 0));
                        }
                    }
                    TYPE_NULL => {
                        lines.push("  leaf NULL".to_string());
                    }
                    TYPE_COLLECTION | TYPE_ARRAY => {
                        if raw.len() >= 9 {
                            let ss = u64::from_le_bytes(raw[1..9].try_into().unwrap());
                            lines.push(format!("  leaf container subtree_size={}", ss));
                        }
                    }
                    _ => {
                        lines.push(format!("  leaf UNKNOWN tag 0x{:02x}", tag));
                    }
                }
            }
        }

        lines.join("\n")
    }

    /// Helper: extract all key names from a collection's ContainerInfo.
    fn dump_collection_keys(&self, container: &crate::nav_cache::ContainerInfo) -> Vec<String> {
        let mut keys = Vec::new();
        for i in 0..container.child_count as usize {
            match crate::nav_cache::ContainerInfo::read_key_from_strings(
                &container.key_strings,
                i,
                &self.dict,
            ) {
                Ok(k) => keys.push(k),
                Err(e) => keys.push(format!("<error: {:?}>", e)),
            }
        }
        keys
    }

    /// Refresh the session's cached header from disk.
    ///
    /// Since the dictionary is never mutated during incremental compaction,
    /// this is mainly useful after root compaction (file rotation) when a
    /// new blob file is in use. Re-reads the header and dictionary from disk.
    pub async fn refresh(&mut self) -> Result<()> {
        self.header = crate::session_reader::read_header(&self.io).await?;
        let dict = crate::session_reader::read_dictionary(&self.io, &self.header).await?;
        self.dict = dict;
        self.field_id_size = self.header.field_id_size()?;
        Ok(())
    }

    /// Access the underlying IO handle.
    pub fn io(&self) -> &IO {
        &self.io
    }

    /// Access the cached header (read-only).
    pub fn header(&self) -> &BlobHeader {
        &self.header
    }

    /// Access the cached dictionary (read-only).
    pub fn dict(&self) -> &Dictionary {
        &self.dict
    }

    /// The field_id_size for this blob.
    pub fn field_id_size(&self) -> FieldIdSize {
        self.field_id_size
    }

    /// Clear cached state at sync boundaries.
    ///
    /// Call this on the reader session after a compaction sync boundary.
    /// With CachedIO's write-through, container reads are always fresh,
    /// but the caller should also call `io.clear_read_cache()` to flush
    /// the CachedIO byte cache when appropriate.
    pub fn clear_cache(&mut self) {
        // No-op: CachedIO handles byte-level cache coherence via write-through.
        // Kept for API compatibility — callers may still call this at sync boundaries.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::MemBlobIO;
    use crate::writer::write_blob;
    use serde_json::json;

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        futures::executor::block_on(f)
    }

    /// Helper: apply updates, return stats.
    async fn apply(
        session: &mut BlobSession<MemBlobIO>,
        updates: &[(Vec<String>, Option<ArcValue>)],
    ) -> IncrementalStats {
        match session.apply_updates(updates).await.unwrap() {
            ApplyResult::Applied(stats) => stats,
        }
    }

    #[test]
    fn test_session_init_creates_empty_blob() {
        block_on(async {
            let io = MemBlobIO::new();
            let session = BlobSession::init(io.clone()).await.unwrap();

            // Root is an empty object
            let root = session.read_subtree(&[]).await.unwrap();
            assert!(root.get("anything").is_none());

            // Blob has valid header
            assert!(session.header().total_size > 0);
            assert_eq!(session.header().node_count, 1); // just the root
        });
    }

    #[test]
    fn test_session_init_then_apply_updates() {
        block_on(async {
            let io = MemBlobIO::new();
            let mut session = BlobSession::init(io.clone()).await.unwrap();

            let updates = vec![
                (vec!["hp".to_string()], Some(ArcValue::from(100i64))),
                (vec!["name".to_string()], Some(ArcValue::from("Hero"))),
            ];
            let stats = apply(&mut session, &updates).await;
            assert_eq!(stats.updates_applied, 2);

            let hp = session.read_subtree(&["hp"]).await.unwrap();
            assert_eq!(hp.as_i64(), Some(100));

            let name = session.read_subtree(&["name"]).await.unwrap();
            assert_eq!(name.as_str(), Some("Hero"));
        });
    }

    #[test]
    fn test_session_open_and_read_root() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "characters": {
                    "-Mabc123": {"hp": 100, "name": "Hero"}
                },
                "config": {"mode": "dark"}
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let session = BlobSession::open(io.clone()).await.unwrap();

            let root = session.read_subtree(&[]).await.unwrap();
            assert_eq!(root, tree);
        });
    }

    #[test]
    fn test_session_read_subtree_at_path() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "characters": {
                    "-Mabc123": {"hp": 100, "name": "Hero"}
                }
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let session = BlobSession::open(io.clone()).await.unwrap();

            let hp = session
                .read_subtree(&["characters", "-Mabc123", "hp"])
                .await
                .unwrap();
            assert_eq!(hp.as_i64(), Some(100));

            let hero = session
                .read_subtree(&["characters", "-Mabc123"])
                .await
                .unwrap();
            assert_eq!(hero.get("name").unwrap().as_str(), Some("Hero"));
        });
    }

    #[test]
    fn test_session_navigate() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "data": {"nested": {"deep": 42}}
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let session = BlobSession::open(io.clone()).await.unwrap();

            let loc = session.navigate(&["data", "nested"]).await.unwrap();
            assert!(loc.subtree_size > 0);

            let value = session.read_subtree(&["data", "nested"]).await.unwrap();
            assert_eq!(value.get("deep").unwrap().as_i64(), Some(42));
        });
    }

    #[test]
    fn test_session_apply_updates() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "hp": 100,
                "name": "Hero"
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let mut session = BlobSession::open(io.clone()).await.unwrap();

            let updates = vec![(vec!["hp".to_string()], Some(ArcValue::from(200i64)))];
            let stats = apply(&mut session, &updates).await;
            assert_eq!(stats.updates_applied, 1);
            assert_eq!(stats.in_place_updates, 1);

            let hp = session.read_subtree(&["hp"]).await.unwrap();
            assert_eq!(hp.as_i64(), Some(200));

            let name = session.read_subtree(&["name"]).await.unwrap();
            assert_eq!(name.as_str(), Some("Hero"));
        });
    }

    #[test]
    fn test_session_cached_dict_across_batches() {
        block_on(async {
            let tree = ArcValue::from_value(json!({"hp": 100}));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let mut session = BlobSession::open(io.clone()).await.unwrap();

            let updates1 = vec![(
                vec!["stats".to_string()],
                Some(ArcValue::from_value(json!({"str": 10, "dex": 15}))),
            )];
            apply(&mut session, &updates1).await;

            let str_val = session.read_subtree(&["stats", "str"]).await.unwrap();
            assert_eq!(str_val.as_i64(), Some(10));

            let updates2 = vec![(
                vec!["stats".to_string(), "str".to_string()],
                Some(ArcValue::from(20i64)),
            )];
            let stats = apply(&mut session, &updates2).await;
            assert_eq!(stats.updates_applied, 1);

            let str_val = session.read_subtree(&["stats", "str"]).await.unwrap();
            assert_eq!(str_val.as_i64(), Some(20));
        });
    }

    #[test]
    fn test_session_multiple_batches_with_new_fields() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "characters": {
                    "-Mabc123": {"hp": 100}
                }
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let mut session = BlobSession::open(io.clone()).await.unwrap();

            let updates1 = vec![(
                vec!["chat".to_string(), "-msg001".to_string()],
                Some(ArcValue::from_value(json!({
                    "author": "Alice",
                    "content": "Hello!",
                    "timestamp": 1700000000
                }))),
            )];
            apply(&mut session, &updates1).await;

            let updates2 = vec![(
                vec!["chat".to_string(), "-msg002".to_string()],
                Some(ArcValue::from_value(json!({
                    "author": "Bob",
                    "content": "Hey!",
                    "timestamp": 1700000001
                }))),
            )];
            apply(&mut session, &updates2).await;

            let msg1 = session
                .read_subtree(&["chat", "-msg001", "author"])
                .await
                .unwrap();
            assert_eq!(msg1.as_str(), Some("Alice"));

            let msg2 = session
                .read_subtree(&["chat", "-msg002", "content"])
                .await
                .unwrap();
            assert_eq!(msg2.as_str(), Some("Hey!"));

            let hp = session
                .read_subtree(&["characters", "-Mabc123", "hp"])
                .await
                .unwrap();
            assert_eq!(hp.as_i64(), Some(100));
        });
    }

    #[test]
    fn test_session_delete() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "a": 1,
                "b": 2,
                "c": 3
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let mut session = BlobSession::open(io.clone()).await.unwrap();

            let updates = vec![(vec!["b".to_string()], None)];
            let stats = apply(&mut session, &updates).await;
            assert_eq!(stats.updates_applied, 1);

            let root = session.read_subtree(&[]).await.unwrap();
            assert!(root.get("b").is_none());
            assert_eq!(root.get("a").unwrap().as_i64(), Some(1));
            assert_eq!(root.get("c").unwrap().as_i64(), Some(3));
        });
    }

    #[test]
    fn test_frozen_dict_new_keys_written_inline() {
        block_on(async {
            // Start with a blob that has "x" in the dictionary
            let tree = ArcValue::from_value(json!({"x": 1}));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let mut session = BlobSession::open(io.clone()).await.unwrap();
            let original_dict_offset = session.header().dict_offset;
            let original_dict_count = session.dict().field_count();

            // Add new structural keys not in the original dictionary.
            // These are written inline in collection key_string areas.
            let updates: Vec<(Vec<String>, Option<ArcValue>)> = vec![
                (vec!["new_key_a".to_string()], Some(ArcValue::from(10i64))),
                (vec!["new_key_b".to_string()], Some(ArcValue::from(20i64))),
            ];
            apply(&mut session, &updates).await;

            // Dictionary should NOT have changed — frozen during incremental
            assert_eq!(
                session.header().dict_offset,
                original_dict_offset,
                "dict_offset should not move during incremental compaction"
            );
            assert_eq!(
                session.dict().field_count(),
                original_dict_count,
                "dict field count should not change during incremental compaction"
            );

            // But the new keys should still be readable (inline encoding)
            let a = session.read_subtree(&["new_key_a"]).await.unwrap();
            assert_eq!(a.as_i64(), Some(10));
            let b = session.read_subtree(&["new_key_b"]).await.unwrap();
            assert_eq!(b.as_i64(), Some(20));

            // Original data still intact
            let x = session.read_subtree(&["x"]).await.unwrap();
            assert_eq!(x.as_i64(), Some(1));

            // After root_compact, pending keys should be absorbed into the dictionary
            let dst = MemBlobIO::new();
            let _old_io = session.root_compact(dst).await.unwrap();
            assert!(
                session.dict().field_count() > original_dict_count,
                "root_compact should absorb pending inline keys into dictionary"
            );
            assert!(
                session.pending_keys.is_empty(),
                "pending_keys should be cleared after root_compact"
            );

            // Verify data is readable from the compacted blob
            let a2 = session.read_subtree(&["new_key_a"]).await.unwrap();
            assert_eq!(a2.as_i64(), Some(10));
            let b2 = session.read_subtree(&["new_key_b"]).await.unwrap();
            assert_eq!(b2.as_i64(), Some(20));
            let x2 = session.read_subtree(&["x"]).await.unwrap();
            assert_eq!(x2.as_i64(), Some(1));

            // Verify new keys are now in the dictionary
            assert!(
                session.dict().lookup("new_key_a").is_some(),
                "new_key_a should be in dict after root_compact"
            );
            assert!(
                session.dict().lookup("new_key_b").is_some(),
                "new_key_b should be in dict after root_compact"
            );
        });
    }

    // ── Test: data integrity after parent rewrite ──

    #[test]
    fn test_data_integrity_after_parent_rewrite() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "stats": {"str": 10, "dex": 15}
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let mut session = BlobSession::open(io.clone()).await.unwrap();

            // Navigate first
            let _ = session.navigate(&["stats", "str"]).await.unwrap();

            // Insert a new key into "stats" — this rewrites the parent object
            let updates = vec![(
                vec!["stats".to_string(), "con".to_string()],
                Some(ArcValue::from(12i64)),
            )];
            apply(&mut session, &updates).await;

            // Navigate through the rewritten parent — reads should return fresh data
            let str_val = session.read_subtree(&["stats", "str"]).await.unwrap();
            assert_eq!(str_val.as_i64(), Some(10));

            let con_val = session.read_subtree(&["stats", "con"]).await.unwrap();
            assert_eq!(con_val.as_i64(), Some(12));

            let dex_val = session.read_subtree(&["stats", "dex"]).await.unwrap();
            assert_eq!(dex_val.as_i64(), Some(15));
        });
    }

    // ── Test: data integrity across multiple batches ──

    #[test]
    fn test_data_integrity_across_batches() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "characters": {
                    "-Mabc123": {"hp": 100, "name": "Hero"}
                }
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let mut session = BlobSession::open(io.clone()).await.unwrap();

            // Batch 1: update hp
            let updates1 = vec![(
                vec![
                    "characters".to_string(),
                    "-Mabc123".to_string(),
                    "hp".to_string(),
                ],
                Some(ArcValue::from(200i64)),
            )];
            apply(&mut session, &updates1).await;

            // Batch 2: update name
            let updates2 = vec![(
                vec![
                    "characters".to_string(),
                    "-Mabc123".to_string(),
                    "name".to_string(),
                ],
                Some(ArcValue::from("Villain")),
            )];
            apply(&mut session, &updates2).await;

            // Verify data integrity
            let hp = session
                .read_subtree(&["characters", "-Mabc123", "hp"])
                .await
                .unwrap();
            assert_eq!(hp.as_i64(), Some(200));
            let name = session
                .read_subtree(&["characters", "-Mabc123", "name"])
                .await
                .unwrap();
            assert_eq!(name.as_str(), Some("Villain"));
        });
    }

    // ── Test: data visible after update ──

    #[test]
    fn test_data_visible_after_update() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "data": {"value": "small"}
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let mut session = BlobSession::open(io.clone()).await.unwrap();

            // Apply an update that appends data (replace small string with big string)
            let big_value = "x".repeat(100);
            let updates = vec![(
                vec!["data".to_string(), "value".to_string()],
                Some(ArcValue::from(big_value.as_str())),
            )];
            apply(&mut session, &updates).await;

            // Verify the actual value is correct
            let val = session.read_subtree(&["data", "value"]).await.unwrap();
            assert_eq!(val.as_str(), Some(big_value.as_str()));
        });
    }

    // ── Test: data integrity after rotation ──

    #[test]
    fn test_data_integrity_after_manual_root_compact() {
        block_on(async {
            let tree = ArcValue::from_value(json!({"a": "short", "b": "short", "c": "short"}));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let mut session = BlobSession::open(io.clone()).await.unwrap();

            // Fragment the root with larger values
            for round in 0..10 {
                let big_val = "x".repeat(200 + round * 100);
                let updates = vec![(
                    vec!["a".to_string()],
                    Some(ArcValue::from(big_val.as_str())),
                )];
                apply(&mut session, &updates).await;
            }

            // Manual root compact to a new file
            let dst = MemBlobIO::new();
            let _old_io = session.root_compact(dst).await.unwrap();

            // Session now points at the new file's state
            let _ = session.navigate(&["a"]).await.unwrap();

            // Verify data integrity on the compacted file
            let b = session.read_subtree(&["b"]).await.unwrap();
            assert_eq!(b.as_str(), Some("short"));
            let c = session.read_subtree(&["c"]).await.unwrap();
            assert_eq!(c.as_str(), Some("short"));
        });
    }

    #[test]
    fn test_session_sub_container_compaction_data_integrity() {
        // Mirrors production scenario: a collection with many children,
        // batched updates that forward children to EOF, then sub-container
        // compaction triggers. Verifies ALL data is correct after compaction.
        block_on(async {
            // Create a collection with 30 children (> 4KB threshold for large-container path)
            let mut items = serde_json::Map::new();
            for i in 0..30 {
                let key = format!("-Mitem{:04}", i);
                items.insert(key, json!({
                    "name": format!("Item {} with padding text to make it larger", i),
                    "description": format!("Description for item {} with more padding text here", i),
                    "value": i,
                    "active": true
                }));
            }
            let tree = ArcValue::from_value(json!({ "data": { "items": items } }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let mut session = BlobSession::open(io.clone()).await.unwrap();

            // Verify items collection is > 4KB (hits large-container compaction path)
            let items_subtree = session.read_subtree(&["data", "items"]).await.unwrap();
            let items_obj = items_subtree.as_object().unwrap();
            assert_eq!(items_obj.len(), 30);

            // Phase 1: Apply batched updates that forward children.
            // Each batch updates multiple children with larger values to cause
            // tombstone+append (forwarding). This is what production does.
            for batch in 0..10 {
                let mut updates: Vec<(Vec<String>, Option<ArcValue>)> = Vec::new();
                for i in 0..30 {
                    let key = format!("-Mitem{:04}", i);
                    let big_desc = format!(
                        "Batch {} update for item {} - {}",
                        batch,
                        i,
                        "x".repeat(300)
                    );
                    updates.push((
                        vec![
                            "data".to_string(),
                            "items".to_string(),
                            key,
                            "description".to_string(),
                        ],
                        Some(ArcValue::from(big_desc.as_str())),
                    ));
                }
                let stats = apply(&mut session, &updates).await;
                assert_eq!(stats.updates_applied, 30);

                // Verify data after each batch to find when corruption occurs
                for i in 0..30 {
                    let key = format!("-Mitem{:04}", i);
                    let val = session
                        .read_subtree(&["data", "items", &key, "value"])
                        .await
                        .unwrap_or_else(|e| {
                            panic!("batch {}: value read failed for {}: {}", batch, key, e)
                        });
                    assert_eq!(
                        val.as_i64(),
                        Some(i as i64),
                        "batch {}: value mismatch for {}",
                        batch,
                        key
                    );

                    let desc = session
                        .read_subtree(&["data", "items", &key, "description"])
                        .await
                        .unwrap_or_else(|e| {
                            panic!("batch {}: desc read failed for {}: {}", batch, key, e)
                        });
                    assert!(
                        desc.as_str().unwrap().contains(&format!("Batch {}", batch)),
                        "batch {}: desc should contain 'Batch {}' for {}, got {:?}",
                        batch,
                        batch,
                        key,
                        &desc.as_str().unwrap()[..50]
                    );
                }
            }

            // Phase 2: Verify ALL data is still correct after compaction
            for i in 0..30 {
                let key = format!("-Mitem{:04}", i);
                let val = session
                    .read_subtree(&["data", "items", &key, "value"])
                    .await
                    .unwrap();
                assert_eq!(val.as_i64(), Some(i as i64), "value mismatch for {}", key);

                let name = session
                    .read_subtree(&["data", "items", &key, "name"])
                    .await
                    .unwrap();
                assert!(
                    name.as_str().unwrap().contains(&format!("Item {}", i)),
                    "name mismatch for {}",
                    key
                );

                let desc = session
                    .read_subtree(&["data", "items", &key, "description"])
                    .await
                    .unwrap();
                assert!(
                    desc.as_str().unwrap().contains("Batch 9"),
                    "description should reflect last batch for {}",
                    key
                );
            }

            // Phase 3: Full compact should produce a clean readable blob
            let dst = MemBlobIO::new();
            crate::compact::full_compact(&io, &dst).await.unwrap();
            let dst_session = BlobSession::open(dst.clone()).await.unwrap();
            for i in 0..30 {
                let key = format!("-Mitem{:04}", i);
                let val = dst_session
                    .read_subtree(&["data", "items", &key, "value"])
                    .await
                    .unwrap();
                assert_eq!(
                    val.as_i64(),
                    Some(i as i64),
                    "full_compact: value mismatch for {}",
                    key
                );
            }
        });
    }

    #[test]
    fn test_session_sub_container_compaction_with_mixed_forwarded_children() {
        // Tests compaction of a container where SOME children are forwarded
        // (at EOF) and some are not (still inline). This tests the span-merging
        // path where contiguous inline children are batched but forwarded
        // children break the span.
        block_on(async {
            let mut items = serde_json::Map::new();
            for i in 0..20 {
                let key = format!("-Mitem{:04}", i);
                items.insert(
                    key,
                    json!({
                        "name": format!("Item {}", i),
                        "value": i
                    }),
                );
            }
            let tree = ArcValue::from_value(json!({ "items": items }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();
            let mut session = BlobSession::open(io.clone()).await.unwrap();

            // Forward only EVEN-numbered children (creates interleaved forwarded/inline)
            for batch in 0..8 {
                let mut updates: Vec<(Vec<String>, Option<ArcValue>)> = Vec::new();
                for i in (0..20).step_by(2) {
                    let key = format!("-Mitem{:04}", i);
                    let big_val =
                        format!("Batch {} big value for {} - {}", batch, i, "y".repeat(400));
                    updates.push((
                        vec!["items".to_string(), key, "name".to_string()],
                        Some(ArcValue::from(big_val.as_str())),
                    ));
                }
                let stats = apply(&mut session, &updates).await;
                assert_eq!(stats.updates_applied, 10);
            }

            // Verify ALL children — both forwarded (even) and inline (odd)
            for i in 0..20 {
                let key = format!("-Mitem{:04}", i);
                let val = session
                    .read_subtree(&["items", &key, "value"])
                    .await
                    .unwrap();
                assert_eq!(val.as_i64(), Some(i as i64), "value mismatch for {}", key);

                let name = session
                    .read_subtree(&["items", &key, "name"])
                    .await
                    .unwrap();
                if i % 2 == 0 {
                    assert!(
                        name.as_str().unwrap().contains("Batch 7"),
                        "even child {} should have latest batch",
                        i
                    );
                } else {
                    assert!(
                        name.as_str().unwrap().contains(&format!("Item {}", i)),
                        "odd child {} should be original",
                        i
                    );
                }
            }

            // Full compact verification
            let dst = MemBlobIO::new();
            crate::compact::full_compact(&io, &dst).await.unwrap();
            let dst_session = BlobSession::open(dst.clone()).await.unwrap();
            for i in 0..20 {
                let key = format!("-Mitem{:04}", i);
                let val = dst_session
                    .read_subtree(&["items", &key, "value"])
                    .await
                    .unwrap();
                assert_eq!(
                    val.as_i64(),
                    Some(i as i64),
                    "full_compact: value mismatch for {}",
                    key
                );
            }
        });
    }

    // -----------------------------------------------------------------------
    // E2E WAL integration tests — simulate the real lark-server compactor flow
    // through BlobSession::apply_updates (tree-based path)
    // -----------------------------------------------------------------------

    #[test]
    fn test_e2e_wal_batch_collection_inserts() {
        // Simulates a WAL batch with many new collection children inserted
        // at once — the primary batch-insert optimization path.
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "characters": {
                    "-Mseed001": {"hp": 100, "name": "Seed"}
                },
                "config": {"mode": "dark"}
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();
            let mut session = BlobSession::open(io.clone()).await.unwrap();

            // WAL batch: 50 new characters inserted at once
            let mut updates: Vec<(Vec<String>, Option<ArcValue>)> = Vec::new();
            for i in 0..50 {
                let key = format!("-Mchar{:04}", i);
                updates.push((
                    vec!["characters".to_string(), key],
                    Some(ArcValue::from_value(json!({
                        "hp": 100 + i,
                        "name": format!("Char {}", i),
                        "x": 0.0,
                        "y": 0.0
                    }))),
                ));
            }
            let stats = apply(&mut session, &updates).await;
            assert_eq!(stats.updates_applied, 50);

            // Verify all children
            let seed = session
                .read_subtree(&["characters", "-Mseed001", "hp"])
                .await
                .unwrap();
            assert_eq!(seed.as_i64(), Some(100));
            for i in 0..50 {
                let key = format!("-Mchar{:04}", i);
                let hp = session
                    .read_subtree(&["characters", &key, "hp"])
                    .await
                    .unwrap_or_else(|e| panic!("read hp for {} failed: {}", key, e));
                assert_eq!(hp.as_i64(), Some(100 + i as i64), "hp mismatch for {}", key);
                let name = session
                    .read_subtree(&["characters", &key, "name"])
                    .await
                    .unwrap();
                assert_eq!(name.as_str(), Some(format!("Char {}", i).as_str()));
            }

            // Config untouched
            let mode = session.read_subtree(&["config", "mode"]).await.unwrap();
            assert_eq!(mode.as_str(), Some("dark"));
        });
    }

    #[test]
    fn test_e2e_wal_batch_mixed_ops_same_collection() {
        // Single WAL batch with inserts + updates to existing + deletes,
        // all targeting the same collection. This is the realistic production pattern.
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "chat": {
                    "-Mmsg001": {"text": "hello", "author": "Alice", "ts": 1000},
                    "-Mmsg002": {"text": "world", "author": "Bob", "ts": 1001},
                    "-Mmsg003": {"text": "foo", "author": "Charlie", "ts": 1002}
                }
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();
            let mut session = BlobSession::open(io.clone()).await.unwrap();

            // WAL batch with mixed operations:
            let updates = vec![
                // Delete msg002
                (vec!["chat".to_string(), "-Mmsg002".to_string()], None),
                // Update msg001's text (existing key, deep path)
                (
                    vec![
                        "chat".to_string(),
                        "-Mmsg001".to_string(),
                        "text".to_string(),
                    ],
                    Some(ArcValue::from("updated hello")),
                ),
                // Insert new messages
                (
                    vec!["chat".to_string(), "-Mmsg004".to_string()],
                    Some(ArcValue::from_value(
                        json!({"text": "new msg 4", "author": "Dave", "ts": 1003}),
                    )),
                ),
                (
                    vec!["chat".to_string(), "-Mmsg005".to_string()],
                    Some(ArcValue::from_value(
                        json!({"text": "new msg 5", "author": "Eve", "ts": 1004}),
                    )),
                ),
                // Update msg003's author (existing key, deep path)
                (
                    vec![
                        "chat".to_string(),
                        "-Mmsg003".to_string(),
                        "author".to_string(),
                    ],
                    Some(ArcValue::from("Charles")),
                ),
            ];
            apply(&mut session, &updates).await;

            // Verify: msg001 updated
            let t1 = session
                .read_subtree(&["chat", "-Mmsg001", "text"])
                .await
                .unwrap();
            assert_eq!(t1.as_str(), Some("updated hello"));
            let a1 = session
                .read_subtree(&["chat", "-Mmsg001", "author"])
                .await
                .unwrap();
            assert_eq!(a1.as_str(), Some("Alice")); // unchanged

            // Verify: msg002 deleted (tombstone — PathNotFound)
            let m2 = session.read_subtree(&["chat", "-Mmsg002"]).await;
            assert!(m2.is_err(), "deleted entry should be PathNotFound");

            // Verify: msg003 author updated
            let a3 = session
                .read_subtree(&["chat", "-Mmsg003", "author"])
                .await
                .unwrap();
            assert_eq!(a3.as_str(), Some("Charles"));
            let t3 = session
                .read_subtree(&["chat", "-Mmsg003", "text"])
                .await
                .unwrap();
            assert_eq!(t3.as_str(), Some("foo")); // unchanged

            // Verify: new messages inserted
            let t4 = session
                .read_subtree(&["chat", "-Mmsg004", "text"])
                .await
                .unwrap();
            assert_eq!(t4.as_str(), Some("new msg 4"));
            let t5 = session
                .read_subtree(&["chat", "-Mmsg005", "text"])
                .await
                .unwrap();
            assert_eq!(t5.as_str(), Some("new msg 5"));
        });
    }

    #[test]
    fn test_e2e_wal_batch_multiple_collections() {
        // WAL batch that touches multiple collections simultaneously —
        // exactly what happens when a game has character updates + chat messages
        // + handout changes in the same batch.
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "characters": {
                    "-Mchar001": {"hp": 100, "name": "Hero"}
                },
                "chat": {
                    "-Mmsg001": {"text": "existing"}
                },
                "handouts": {
                    "-Mhand001": {"title": "Map", "content": "treasure here"}
                }
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();
            let mut session = BlobSession::open(io.clone()).await.unwrap();

            // One big WAL batch touching all three collections
            let updates = vec![
                // Characters: insert + update
                (
                    vec!["characters".to_string(), "-Mchar002".to_string()],
                    Some(ArcValue::from_value(json!({"hp": 50, "name": "Villain"}))),
                ),
                (
                    vec![
                        "characters".to_string(),
                        "-Mchar001".to_string(),
                        "hp".to_string(),
                    ],
                    Some(ArcValue::from(200i64)),
                ),
                // Chat: insert multiple
                (
                    vec!["chat".to_string(), "-Mmsg002".to_string()],
                    Some(ArcValue::from_value(
                        json!({"text": "hello", "author": "Alice"}),
                    )),
                ),
                (
                    vec!["chat".to_string(), "-Mmsg003".to_string()],
                    Some(ArcValue::from_value(
                        json!({"text": "world", "author": "Bob"}),
                    )),
                ),
                // Handouts: update + delete
                (
                    vec![
                        "handouts".to_string(),
                        "-Mhand001".to_string(),
                        "content".to_string(),
                    ],
                    Some(ArcValue::from("updated treasure map")),
                ),
            ];
            apply(&mut session, &updates).await;

            // Verify characters
            let hp1 = session
                .read_subtree(&["characters", "-Mchar001", "hp"])
                .await
                .unwrap();
            assert_eq!(hp1.as_i64(), Some(200));
            let hp2 = session
                .read_subtree(&["characters", "-Mchar002", "hp"])
                .await
                .unwrap();
            assert_eq!(hp2.as_i64(), Some(50));

            // Verify chat
            let t2 = session
                .read_subtree(&["chat", "-Mmsg002", "text"])
                .await
                .unwrap();
            assert_eq!(t2.as_str(), Some("hello"));
            let t3 = session
                .read_subtree(&["chat", "-Mmsg003", "text"])
                .await
                .unwrap();
            assert_eq!(t3.as_str(), Some("world"));

            // Verify handouts
            let content = session
                .read_subtree(&["handouts", "-Mhand001", "content"])
                .await
                .unwrap();
            assert_eq!(content.as_str(), Some("updated treasure map"));
        });
    }

    #[test]
    fn test_e2e_wal_coalescing_same_path() {
        // Simulates what happens when lark-server's WAL has multiple writes
        // to the same path in the same batch. UpdateNode::build coalesces them
        // (last write wins). Lark-server does its own dedup but this tests that
        // our internal coalescing is correct even if duplicates slip through.
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "characters": {
                    "-Mchar001": {"hp": 100, "name": "Hero"}
                }
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();
            let mut session = BlobSession::open(io.clone()).await.unwrap();

            // Same character HP updated 3 times in one batch — last write wins
            let updates = vec![
                (
                    vec![
                        "characters".to_string(),
                        "-Mchar001".to_string(),
                        "hp".to_string(),
                    ],
                    Some(ArcValue::from(150i64)),
                ),
                (
                    vec![
                        "characters".to_string(),
                        "-Mchar001".to_string(),
                        "hp".to_string(),
                    ],
                    Some(ArcValue::from(200i64)),
                ),
                (
                    vec![
                        "characters".to_string(),
                        "-Mchar001".to_string(),
                        "hp".to_string(),
                    ],
                    Some(ArcValue::from(250i64)),
                ),
            ];
            apply(&mut session, &updates).await;

            let hp = session
                .read_subtree(&["characters", "-Mchar001", "hp"])
                .await
                .unwrap();
            assert_eq!(hp.as_i64(), Some(250), "should have last-write-wins value");

            // Name should be untouched
            let name = session
                .read_subtree(&["characters", "-Mchar001", "name"])
                .await
                .unwrap();
            assert_eq!(name.as_str(), Some("Hero"));
        });
    }

    #[test]
    fn test_e2e_wal_update_op_expansion() {
        // Simulates the WAL "u" (update) operation where a partial object update
        // is expanded to individual field sets. E.g., updating a character's
        // position sends {x: 5.0, y: 3.0} which expands to two updates.
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "characters": {
                    "-Mchar001": {"hp": 100, "name": "Hero", "x": 0.0, "y": 0.0}
                }
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();
            let mut session = BlobSession::open(io.clone()).await.unwrap();

            // Expanded "u" op: update x and y for the same character
            // Plus a set on a different character and a chat insert
            let updates = vec![
                (
                    vec![
                        "characters".to_string(),
                        "-Mchar001".to_string(),
                        "x".to_string(),
                    ],
                    Some(ArcValue::from(5.5f64)),
                ),
                (
                    vec![
                        "characters".to_string(),
                        "-Mchar001".to_string(),
                        "y".to_string(),
                    ],
                    Some(ArcValue::from(3.2f64)),
                ),
                (
                    vec![
                        "characters".to_string(),
                        "-Mchar001".to_string(),
                        "hp".to_string(),
                    ],
                    Some(ArcValue::from(95i64)),
                ),
            ];
            apply(&mut session, &updates).await;

            let x = session
                .read_subtree(&["characters", "-Mchar001", "x"])
                .await
                .unwrap();
            assert_eq!(x.as_f64(), Some(5.5));
            let y = session
                .read_subtree(&["characters", "-Mchar001", "y"])
                .await
                .unwrap();
            assert_eq!(y.as_f64(), Some(3.2));
            let hp = session
                .read_subtree(&["characters", "-Mchar001", "hp"])
                .await
                .unwrap();
            assert_eq!(hp.as_i64(), Some(95));
            let name = session
                .read_subtree(&["characters", "-Mchar001", "name"])
                .await
                .unwrap();
            assert_eq!(name.as_str(), Some("Hero"));
        });
    }

    #[test]
    fn test_e2e_wal_multi_batch_accumulation() {
        // Simulates multiple sequential WAL batches through the same BlobSession,
        // each containing a realistic mix of operations. This is the core compactor
        // lifecycle: open session once, apply many WAL batches.
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "characters": {
                    "-Mchar001": {"hp": 100, "name": "Hero", "x": 0.0, "y": 0.0}
                },
                "config": {"mode": "dark"}
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();
            let mut session = BlobSession::open(io.clone()).await.unwrap();

            // Batch 1: Add chat collection + new characters
            let batch1 = vec![
                (
                    vec!["chat".to_string(), "-Mmsg001".to_string()],
                    Some(ArcValue::from_value(
                        json!({"text": "game started", "author": "System", "ts": 1000}),
                    )),
                ),
                (
                    vec!["characters".to_string(), "-Mchar002".to_string()],
                    Some(ArcValue::from_value(
                        json!({"hp": 50, "name": "Goblin", "x": 5.0, "y": 5.0}),
                    )),
                ),
                (
                    vec!["characters".to_string(), "-Mchar003".to_string()],
                    Some(ArcValue::from_value(
                        json!({"hp": 75, "name": "Elf", "x": 3.0, "y": 1.0}),
                    )),
                ),
            ];
            apply(&mut session, &batch1).await;

            // Batch 2: Movement updates + chat messages
            let batch2 = vec![
                (
                    vec![
                        "characters".to_string(),
                        "-Mchar001".to_string(),
                        "x".to_string(),
                    ],
                    Some(ArcValue::from(2.5f64)),
                ),
                (
                    vec![
                        "characters".to_string(),
                        "-Mchar001".to_string(),
                        "y".to_string(),
                    ],
                    Some(ArcValue::from(1.0f64)),
                ),
                (
                    vec!["chat".to_string(), "-Mmsg002".to_string()],
                    Some(ArcValue::from_value(
                        json!({"text": "I move north", "author": "Hero", "ts": 1001}),
                    )),
                ),
                (
                    vec!["chat".to_string(), "-Mmsg003".to_string()],
                    Some(ArcValue::from_value(
                        json!({"text": "I attack!", "author": "Hero", "ts": 1002}),
                    )),
                ),
            ];
            apply(&mut session, &batch2).await;

            // Batch 3: Combat — hp updates + delete dead character + new chat
            let batch3 = vec![
                (
                    vec![
                        "characters".to_string(),
                        "-Mchar002".to_string(),
                        "hp".to_string(),
                    ],
                    Some(ArcValue::from(0i64)),
                ),
                (
                    vec![
                        "characters".to_string(),
                        "-Mchar001".to_string(),
                        "hp".to_string(),
                    ],
                    Some(ArcValue::from(85i64)),
                ),
                (
                    vec!["chat".to_string(), "-Mmsg004".to_string()],
                    Some(ArcValue::from_value(
                        json!({"text": "Goblin defeated!", "author": "System", "ts": 1003}),
                    )),
                ),
            ];
            apply(&mut session, &batch3).await;

            // Verify final state across all batches
            // Characters
            let hero_hp = session
                .read_subtree(&["characters", "-Mchar001", "hp"])
                .await
                .unwrap();
            assert_eq!(hero_hp.as_i64(), Some(85));
            let hero_x = session
                .read_subtree(&["characters", "-Mchar001", "x"])
                .await
                .unwrap();
            assert_eq!(hero_x.as_f64(), Some(2.5));
            let hero_name = session
                .read_subtree(&["characters", "-Mchar001", "name"])
                .await
                .unwrap();
            assert_eq!(hero_name.as_str(), Some("Hero"));

            let goblin_hp = session
                .read_subtree(&["characters", "-Mchar002", "hp"])
                .await
                .unwrap();
            assert_eq!(goblin_hp.as_i64(), Some(0));

            let elf_name = session
                .read_subtree(&["characters", "-Mchar003", "name"])
                .await
                .unwrap();
            assert_eq!(elf_name.as_str(), Some("Elf"));

            // Chat messages
            let msg1 = session
                .read_subtree(&["chat", "-Mmsg001", "text"])
                .await
                .unwrap();
            assert_eq!(msg1.as_str(), Some("game started"));
            let msg4 = session
                .read_subtree(&["chat", "-Mmsg004", "text"])
                .await
                .unwrap();
            assert_eq!(msg4.as_str(), Some("Goblin defeated!"));

            // Config untouched
            let mode = session.read_subtree(&["config", "mode"]).await.unwrap();
            assert_eq!(mode.as_str(), Some("dark"));

            // Full compact should produce valid blob
            let dst = MemBlobIO::new();
            crate::compact::full_compact(&io, &dst).await.unwrap();
            let dst_session = BlobSession::open(dst.clone()).await.unwrap();
            let hp = dst_session
                .read_subtree(&["characters", "-Mchar001", "hp"])
                .await
                .unwrap();
            assert_eq!(hp.as_i64(), Some(85));
            let msg = dst_session
                .read_subtree(&["chat", "-Mmsg003", "text"])
                .await
                .unwrap();
            assert_eq!(msg.as_str(), Some("I attack!"));
        });
    }

    #[test]
    fn test_e2e_wal_large_batch_accumulation_then_compact() {
        // Simulates the production scenario: many large WAL batches that
        // accumulate fragmentation, then a manual root_compact cleans up.
        block_on(async {
            let mut chars = serde_json::Map::new();
            for i in 0..10 {
                let key = format!("-Mchar{:04}", i);
                chars.insert(key, json!({"hp": 100, "name": format!("Char {}", i)}));
            }
            let tree = ArcValue::from_value(json!({
                "characters": chars,
                "chat": { "-Mseed": {"text": "start"} }
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();
            let mut session = BlobSession::open(io.clone()).await.unwrap();

            // Apply 20 batches, each with batch inserts into chat + updates to chars
            for batch in 0..20 {
                let mut updates: Vec<(Vec<String>, Option<ArcValue>)> = Vec::new();

                // 10 new chat messages per batch
                for i in 0..10 {
                    let key = format!("-Mb{:02}m{:04}", batch, i);
                    updates.push((
                        vec!["chat".to_string(), key],
                        Some(ArcValue::from_value(json!({
                            "text": format!("Batch {} msg {} - {}", batch, i, "x".repeat(100)),
                            "ts": batch * 10 + i
                        }))),
                    ));
                }

                // Update character positions (deep path updates to same collection)
                for i in 0..10 {
                    let key = format!("-Mchar{:04}", i);
                    updates.push((
                        vec!["characters".to_string(), key, "hp".to_string()],
                        Some(ArcValue::from(100 - batch as i64)),
                    ));
                }

                apply(&mut session, &updates).await;
            }

            // Verify final state before compaction
            for i in 0..10 {
                let key = format!("-Mchar{:04}", i);
                let hp = session
                    .read_subtree(&["characters", &key, "hp"])
                    .await
                    .unwrap();
                assert_eq!(
                    hp.as_i64(),
                    Some(81),
                    "char {} hp should be 81 (100 - 19)",
                    i
                );
                let name = session
                    .read_subtree(&["characters", &key, "name"])
                    .await
                    .unwrap();
                assert_eq!(name.as_str(), Some(format!("Char {}", i).as_str()));
            }

            // Verify chat messages from last batch
            for i in 0..10 {
                let key = format!("-Mb19m{:04}", i);
                let text = session.read_subtree(&["chat", &key, "text"]).await.unwrap();
                assert!(
                    text.as_str().unwrap().contains("Batch 19"),
                    "last batch chat msg {} should contain 'Batch 19'",
                    i
                );
            }

            // Spot check earlier batches survived
            let early = session
                .read_subtree(&["chat", "-Mb00m0000", "text"])
                .await
                .unwrap();
            assert!(early.as_str().unwrap().contains("Batch 0"));

            // Manual root compact to clean file
            let dst = MemBlobIO::new();
            let _old_io = session.root_compact(dst).await.unwrap();

            // Verify data on compacted file
            let hp = session
                .read_subtree(&["characters", "-Mchar0005", "hp"])
                .await
                .unwrap();
            assert_eq!(hp.as_i64(), Some(81));
            let msg = session
                .read_subtree(&["chat", "-Mb19m0000", "text"])
                .await
                .unwrap();
            assert!(msg.as_str().unwrap().contains("Batch 19"));
        });
    }

    #[test]
    fn test_e2e_wal_batch_insert_into_new_collection() {
        // Tests the path where a batch creates a new collection (intermediate
        // objects) and inserts multiple children into it, all in one batch.
        // This is what happens on the first WAL batch for a new game database.
        block_on(async {
            let io = MemBlobIO::new();
            let mut session = BlobSession::init(io.clone()).await.unwrap();

            // First batch: create the entire game structure from scratch
            let updates = vec![
                (
                    vec!["characters".to_string(), "-Mchar001".to_string()],
                    Some(ArcValue::from_value(json!({"hp": 100, "name": "Hero"}))),
                ),
                (
                    vec!["characters".to_string(), "-Mchar002".to_string()],
                    Some(ArcValue::from_value(json!({"hp": 50, "name": "Sidekick"}))),
                ),
                (
                    vec!["config".to_string(), "mode".to_string()],
                    Some(ArcValue::from("dark")),
                ),
                (
                    vec!["config".to_string(), "grid".to_string()],
                    Some(ArcValue::from(true)),
                ),
                (
                    vec!["chat".to_string(), "-Mmsg001".to_string()],
                    Some(ArcValue::from_value(
                        json!({"text": "Welcome!", "author": "System"}),
                    )),
                ),
            ];
            apply(&mut session, &updates).await;

            // Verify everything
            let hp1 = session
                .read_subtree(&["characters", "-Mchar001", "hp"])
                .await
                .unwrap();
            assert_eq!(hp1.as_i64(), Some(100));
            let hp2 = session
                .read_subtree(&["characters", "-Mchar002", "hp"])
                .await
                .unwrap();
            assert_eq!(hp2.as_i64(), Some(50));
            let mode = session.read_subtree(&["config", "mode"]).await.unwrap();
            assert_eq!(mode.as_str(), Some("dark"));
            let grid = session.read_subtree(&["config", "grid"]).await.unwrap();
            assert_eq!(grid.as_bool(), Some(true));
            let msg = session
                .read_subtree(&["chat", "-Mmsg001", "text"])
                .await
                .unwrap();
            assert_eq!(msg.as_str(), Some("Welcome!"));
        });
    }

    #[test]
    fn test_e2e_wal_set_then_delete_same_batch() {
        // WAL batch where an entity is set and then deleted in the same batch.
        // After coalescing, the delete should win (it comes later).
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "characters": {
                    "-Mchar001": {"hp": 100}
                }
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();
            let mut session = BlobSession::open(io.clone()).await.unwrap();

            // Set a new character then delete it in the same batch
            let updates = vec![
                (
                    vec!["characters".to_string(), "-Mnew001".to_string()],
                    Some(ArcValue::from_value(json!({"hp": 50}))),
                ),
                (vec!["characters".to_string(), "-Mnew001".to_string()], None),
            ];
            apply(&mut session, &updates).await;

            // The new character should not exist — Set then Delete coalesces to Delete,
            // and deleting a non-existent key is a no-op. It may read as Null (if
            // the key was inserted then nulled) or PathNotFound (if coalescing
            // prevented the insert entirely).
            let result = session.read_subtree(&["characters", "-Mnew001"]).await;
            match result {
                Ok(ArcValue::Null) => {} // acceptable: key was inserted then nulled
                Err(crate::error::BlobError::PathNotFound(_)) => {} // acceptable: coalesced away
                other => panic!("expected Null or PathNotFound, got {:?}", other),
            }

            // Original character untouched
            let hp = session
                .read_subtree(&["characters", "-Mchar001", "hp"])
                .await
                .unwrap();
            assert_eq!(hp.as_i64(), Some(100));
        });
    }

    #[test]
    fn test_e2e_wal_delete_then_reinsert_same_batch() {
        // WAL batch where an entity is deleted and then re-inserted.
        // The re-insert should win.
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "characters": {
                    "-Mchar001": {"hp": 100, "name": "Hero"}
                }
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();
            let mut session = BlobSession::open(io.clone()).await.unwrap();

            // Delete then re-insert with different data
            let updates = vec![
                (
                    vec!["characters".to_string(), "-Mchar001".to_string()],
                    None,
                ),
                (
                    vec!["characters".to_string(), "-Mchar001".to_string()],
                    Some(ArcValue::from_value(
                        json!({"hp": 999, "name": "Reborn Hero"}),
                    )),
                ),
            ];
            apply(&mut session, &updates).await;

            let hp = session
                .read_subtree(&["characters", "-Mchar001", "hp"])
                .await
                .unwrap();
            assert_eq!(hp.as_i64(), Some(999));
            let name = session
                .read_subtree(&["characters", "-Mchar001", "name"])
                .await
                .unwrap();
            assert_eq!(name.as_str(), Some("Reborn Hero"));
        });
    }

    // ── Test: read_keys ──

    #[test]
    fn test_read_keys_object() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "hp": 100,
                "name": "Hero",
                "x": 0.0
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let session = BlobSession::open(io.clone()).await.unwrap();

            // Root is an object — keys are the field names with sizes
            let mut keys = session.read_keys(&[]).await.unwrap();
            keys.sort_by(|a, b| a.0.cmp(&b.0));
            let names: Vec<&str> = keys.iter().map(|(k, _)| k.as_str()).collect();
            assert_eq!(names, vec!["hp", "name", "x"]);
            // All sizes should be > 0
            for (_, size) in &keys {
                assert!(*size > 0);
            }
        });
    }

    #[test]
    fn test_read_keys_collection() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "characters": {
                    "-Mabc123": {"hp": 100},
                    "-Mdef456": {"hp": 50},
                    "-Mghi789": {"hp": 75}
                }
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let session = BlobSession::open(io.clone()).await.unwrap();

            // "characters" is a collection — keys are the push IDs
            let mut keys = session.read_keys(&["characters"]).await.unwrap();
            keys.sort_by(|a, b| a.0.cmp(&b.0));
            let names: Vec<&str> = keys.iter().map(|(k, _)| k.as_str()).collect();
            assert_eq!(names, vec!["-Mabc123", "-Mdef456", "-Mghi789"]);
            // Each character has {hp: N} — all same structure, same size
            assert_eq!(keys[0].1, keys[1].1);
            assert_eq!(keys[1].1, keys[2].1);
        });
    }

    #[test]
    fn test_read_keys_nested_object() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "characters": {
                    "-Mabc123": {"hp": 100, "name": "Hero", "x": 0.0}
                }
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let session = BlobSession::open(io.clone()).await.unwrap();

            // Navigate to a specific character — it's an object with field names
            let mut keys = session
                .read_keys(&["characters", "-Mabc123"])
                .await
                .unwrap();
            keys.sort_by(|a, b| a.0.cmp(&b.0));
            let names: Vec<&str> = keys.iter().map(|(k, _)| k.as_str()).collect();
            assert_eq!(names, vec!["hp", "name", "x"]);
            // All sizes should be > 0
            for (_, size) in &keys {
                assert!(*size > 0);
            }
        });
    }

    #[test]
    fn test_read_keys_leaf_returns_error() {
        block_on(async {
            let tree = ArcValue::from_value(json!({"hp": 100}));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let session = BlobSession::open(io.clone()).await.unwrap();

            // "hp" is a number, not a container
            let result = session.read_keys(&["hp"]).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_read_keys_after_updates() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "characters": {
                    "-Mabc123": {"hp": 100}
                }
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let mut session = BlobSession::open(io.clone()).await.unwrap();

            // Insert new collection children
            let updates = vec![
                (
                    vec!["characters".to_string(), "-Mdef456".to_string()],
                    Some(ArcValue::from_value(json!({"hp": 50}))),
                ),
                (
                    vec!["characters".to_string(), "-Mghi789".to_string()],
                    Some(ArcValue::from_value(json!({"hp": 75}))),
                ),
            ];
            apply(&mut session, &updates).await;

            let mut keys = session.read_keys(&["characters"]).await.unwrap();
            keys.sort_by(|a, b| a.0.cmp(&b.0));
            let names: Vec<&str> = keys.iter().map(|(k, _)| k.as_str()).collect();
            assert_eq!(names, vec!["-Mabc123", "-Mdef456", "-Mghi789"]);
        });
    }

    #[test]
    fn test_read_keys_nonexistent_path() {
        block_on(async {
            let tree = ArcValue::from_value(json!({"hp": 100}));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let session = BlobSession::open(io.clone()).await.unwrap();

            let result = session.read_keys(&["nonexistent"]).await;
            assert!(result.is_err());
        });
    }

    // ── Tests: read_shallow ──

    #[test]
    fn test_read_shallow_primitive_at_path() {
        // Shallow read on a leaf node returns the actual value
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "hp": 100,
                "name": "Hero"
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let session = BlobSession::open(io.clone()).await.unwrap();

            // Read a number
            match session.read_shallow(&["hp"]).await.unwrap() {
                ShallowValue::Primitive(v) => assert_eq!(v.as_i64(), Some(100)),
                ShallowValue::Children(_) => panic!("expected Primitive"),
            }

            // Read a string
            match session.read_shallow(&["name"]).await.unwrap() {
                ShallowValue::Primitive(v) => assert_eq!(v.as_str(), Some("Hero")),
                ShallowValue::Children(_) => panic!("expected Primitive"),
            }
        });
    }

    #[test]
    fn test_read_shallow_object_root() {
        // Shallow read on root object: primitives get values, containers get None
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "hp": 100,
                "name": "Hero",
                "characters": {
                    "-Mabc123": {"hp": 50}
                }
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let session = BlobSession::open(io.clone()).await.unwrap();

            match session.read_shallow(&[]).await.unwrap() {
                ShallowValue::Primitive(_) => panic!("expected Children"),
                ShallowValue::Children(children) => {
                    assert_eq!(children.len(), 3);

                    // Find each child by key
                    let hp = children.iter().find(|c| c.key == "hp").unwrap();
                    assert_eq!(hp.value.as_ref().unwrap().as_i64(), Some(100));
                    assert!(hp.size > 0);

                    let name = children.iter().find(|c| c.key == "name").unwrap();
                    assert_eq!(name.value.as_ref().unwrap().as_str(), Some("Hero"));

                    let chars = children.iter().find(|c| c.key == "characters").unwrap();
                    assert!(chars.value.is_none()); // container → no value
                    assert!(chars.size > 0);
                }
            }
        });
    }

    #[test]
    fn test_read_shallow_collection() {
        // Shallow read on a collection: all children are objects → all None values
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "characters": {
                    "-Mabc123": {"hp": 100},
                    "-Mdef456": {"hp": 50}
                }
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let session = BlobSession::open(io.clone()).await.unwrap();

            match session.read_shallow(&["characters"]).await.unwrap() {
                ShallowValue::Primitive(_) => panic!("expected Children"),
                ShallowValue::Children(children) => {
                    assert_eq!(children.len(), 2);
                    // Both children are objects → value is None
                    for child in &children {
                        assert!(child.value.is_none());
                        assert!(child.size > 0);
                    }
                    let mut keys: Vec<&str> = children.iter().map(|c| c.key.as_str()).collect();
                    keys.sort();
                    assert_eq!(keys, vec!["-Mabc123", "-Mdef456"]);
                }
            }
        });
    }

    #[test]
    fn test_read_shallow_mixed_children() {
        // Object with a mix of primitive and container children
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "config": {
                    "mode": "dark",
                    "grid": true,
                    "zoom": 1.5,
                    "sub": {"nested": 1}
                }
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let session = BlobSession::open(io.clone()).await.unwrap();

            match session.read_shallow(&["config"]).await.unwrap() {
                ShallowValue::Primitive(_) => panic!("expected Children"),
                ShallowValue::Children(children) => {
                    assert_eq!(children.len(), 4);

                    let mode = children.iter().find(|c| c.key == "mode").unwrap();
                    assert_eq!(mode.value.as_ref().unwrap().as_str(), Some("dark"));

                    let grid = children.iter().find(|c| c.key == "grid").unwrap();
                    assert_eq!(grid.value.as_ref().unwrap().as_bool(), Some(true));

                    let zoom = children.iter().find(|c| c.key == "zoom").unwrap();
                    assert_eq!(zoom.value.as_ref().unwrap().as_f64(), Some(1.5));

                    let sub = children.iter().find(|c| c.key == "sub").unwrap();
                    assert!(sub.value.is_none()); // container
                }
            }
        });
    }

    #[test]
    fn test_read_shallow_bool_and_null() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "flag": false,
                "nothing": null
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let session = BlobSession::open(io.clone()).await.unwrap();

            match session.read_shallow(&[]).await.unwrap() {
                ShallowValue::Primitive(_) => panic!("expected Children"),
                ShallowValue::Children(children) => {
                    let flag = children.iter().find(|c| c.key == "flag").unwrap();
                    assert_eq!(flag.value.as_ref().unwrap().as_bool(), Some(false));

                    let nothing = children.iter().find(|c| c.key == "nothing").unwrap();
                    assert_eq!(nothing.value.as_ref().unwrap(), &ArcValue::Null);
                }
            }
        });
    }

    #[test]
    fn test_read_shallow_after_updates() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "characters": {
                    "-Mabc123": {"hp": 100}
                }
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let mut session = BlobSession::open(io.clone()).await.unwrap();

            // Add a new character and a primitive sibling
            let updates = vec![
                (
                    vec!["characters".to_string(), "-Mdef456".to_string()],
                    Some(ArcValue::from_value(json!({"hp": 50}))),
                ),
                (vec!["score".to_string()], Some(ArcValue::from(999i64))),
            ];
            apply(&mut session, &updates).await;

            // Shallow read root — should see "characters" (container) and "score" (primitive)
            match session.read_shallow(&[]).await.unwrap() {
                ShallowValue::Primitive(_) => panic!("expected Children"),
                ShallowValue::Children(children) => {
                    let chars = children.iter().find(|c| c.key == "characters").unwrap();
                    assert!(chars.value.is_none());

                    let score = children.iter().find(|c| c.key == "score").unwrap();
                    assert_eq!(score.value.as_ref().unwrap().as_i64(), Some(999));
                }
            }

            // Shallow read characters — both are containers
            match session.read_shallow(&["characters"]).await.unwrap() {
                ShallowValue::Primitive(_) => panic!("expected Children"),
                ShallowValue::Children(children) => {
                    assert_eq!(children.len(), 2);
                    for child in &children {
                        assert!(child.value.is_none());
                    }
                }
            }
        });
    }

    #[test]
    fn test_read_shallow_nonexistent_path() {
        block_on(async {
            let tree = ArcValue::from_value(json!({"hp": 100}));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let session = BlobSession::open(io.clone()).await.unwrap();
            let result = session.read_shallow(&["nonexistent"]).await;
            assert!(result.is_err());
        });
    }

    // -----------------------------------------------------------------------
    // Replicas of failing lark-server integration_storage_worker tests.
    // These simulate the exact WAL→coalesce→apply_updates flow.
    // -----------------------------------------------------------------------

    /// Helper: produces large updates writing 10 × 600KB values to /bulk/item_0..9.
    fn bulk_updates() -> Vec<(Vec<String>, Option<ArcValue>)> {
        let chunk = "x".repeat(600_000);
        (0..10)
            .map(|i| {
                (
                    vec!["bulk".to_string(), format!("item_{}", i)],
                    Some(ArcValue::from(chunk.as_str())),
                )
            })
            .collect()
    }

    #[test]
    fn test_repro_compacts_updates() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "profile": {
                    "name": "Alice",
                    "bio": "Hello",
                    "score": 100
                }
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();
            let mut session = BlobSession::open(io.clone()).await.unwrap();

            let mut updates: Vec<(Vec<String>, Option<ArcValue>)> = vec![
                (
                    vec!["profile".to_string(), "score".to_string()],
                    Some(ArcValue::from(999i64)),
                ),
                (
                    vec!["profile".to_string(), "badge".to_string()],
                    Some(ArcValue::from("gold")),
                ),
            ];
            updates.extend(bulk_updates());

            apply(&mut session, &updates).await;

            let score = session.read_subtree(&["profile", "score"]).await.unwrap();
            assert_eq!(
                score.as_i64(),
                Some(999),
                "UPDATE should update score in blob"
            );

            let badge = session.read_subtree(&["profile", "badge"]).await.unwrap();
            assert_eq!(
                badge.as_str(),
                Some("gold"),
                "UPDATE should add badge in blob"
            );

            let name = session.read_subtree(&["profile", "name"]).await.unwrap();
            assert_eq!(
                name.as_str(),
                Some("Alice"),
                "Original field should be preserved"
            );

            let bio = session.read_subtree(&["profile", "bio"]).await.unwrap();
            assert_eq!(
                bio.as_str(),
                Some("Hello"),
                "Original field should be preserved"
            );

            let item0 = session.read_subtree(&["bulk", "item_0"]).await;
            assert!(item0.is_ok(), "bulk/item_0 should exist");
        });
    }

    #[test]
    fn test_repro_compacts_deletes() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "keep": "yes",
                "remove_me": "goodbye"
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();
            let mut session = BlobSession::open(io.clone()).await.unwrap();

            let mut updates: Vec<(Vec<String>, Option<ArcValue>)> =
                vec![(vec!["remove_me".to_string()], None)];
            updates.extend(bulk_updates());

            apply(&mut session, &updates).await;

            let removed = session.read_subtree(&["remove_me"]).await;
            // Err (PathNotFound) is fine; only assert when the read succeeds.
            if let Ok(v) = &removed {
                assert!(!v.exists(), "Deleted path should not exist, got {:?}", v);
            }

            let kept = session.read_subtree(&["keep"]).await.unwrap();
            assert_eq!(
                kept.as_str(),
                Some("yes"),
                "Non-deleted path should survive"
            );
        });
    }

    #[test]
    fn test_repro_compacts_new_database() {
        block_on(async {
            let io = MemBlobIO::new();
            let mut session = BlobSession::init(io.clone()).await.unwrap();

            let mut updates: Vec<(Vec<String>, Option<ArcValue>)> =
                vec![(vec!["marker".to_string()], Some(ArcValue::from("hello")))];
            updates.extend(bulk_updates());

            apply(&mut session, &updates).await;

            let marker = session.read_subtree(&["marker"]).await.unwrap();
            assert_eq!(marker.as_str(), Some("hello"), "Marker should be in blob");

            let item0 = session.read_subtree(&["bulk", "item_0"]).await;
            assert!(item0.is_ok(), "bulk/item_0 should exist");
        });
    }

    #[test]
    fn test_repro_data_survives_restart() {
        block_on(async {
            let tree = ArcValue::from_value(json!({"original": "from_blob"}));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();
            let mut session = BlobSession::open(io.clone()).await.unwrap();

            let mut updates: Vec<(Vec<String>, Option<ArcValue>)> = vec![(
                vec!["marker".to_string()],
                Some(ArcValue::from("compacted_value")),
            )];
            updates.extend(bulk_updates());

            apply(&mut session, &updates).await;

            // Simulate restart: re-open session from the IO
            let session2 = BlobSession::open(io.clone()).await.unwrap();

            let marker = session2.read_subtree(&["marker"]).await.unwrap();
            assert_eq!(
                marker.as_str(),
                Some("compacted_value"),
                "Compacted data should survive restart"
            );

            let original = session2.read_subtree(&["original"]).await.unwrap();
            assert_eq!(
                original.as_str(),
                Some("from_blob"),
                "Original data should survive"
            );

            let item0 = session2.read_subtree(&["bulk", "item_0"]).await;
            assert!(item0.is_ok(), "Bulk data should survive restart");
        });
    }

    #[test]
    fn test_repro_dictionary_full() {
        block_on(async {
            let tree = ArcValue::from_value(json!({"original": "marker"}));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();
            let mut session = BlobSession::open(io.clone()).await.unwrap();

            let padding = "x".repeat(11_000);
            let updates: Vec<(Vec<String>, Option<ArcValue>)> = (0..510)
                .map(|i| {
                    (
                        vec!["fields".to_string(), format!("field_{:04}", i)],
                        Some(ArcValue::from(padding.as_str())),
                    )
                })
                .collect();

            const BATCH_SIZE: usize = 1000;
            for chunk in updates.chunks(BATCH_SIZE) {
                apply(&mut session, chunk).await;
            }

            let original = session.read_subtree(&["original"]).await.unwrap();
            assert_eq!(
                original.as_str(),
                Some("marker"),
                "Original data should survive"
            );

            let field_0 = session
                .read_subtree(&["fields", "field_0000"])
                .await
                .unwrap();
            assert_eq!(
                field_0.as_str(),
                Some(padding.as_str()),
                "Field data should be in blob"
            );

            let field_200 = session
                .read_subtree(&["fields", "field_0200"])
                .await
                .unwrap();
            assert_eq!(
                field_200.as_str(),
                Some(padding.as_str()),
                "Mid-range field should be in blob"
            );
        });
    }

    /// Regression test: read_subtree to a leaf at the very end of the file.
    ///
    /// A small blob like {"exists": "yes"} has a ~18KB dictionary (reserved
    /// space) followed by a tiny root collection whose last child is an 8-byte
    /// string at the final bytes of the file. read_subtree_from must not do a
    /// 9-byte probe there — it would read past EOF.
    #[test]
    fn test_read_subtree_leaf_at_eof() {
        block_on(async {
            let tree = ArcValue::from_value(json!({"exists": "yes"}));
            let io = MemBlobIO::new();
            crate::writer::write_blob(&io, &tree).await.unwrap();

            let session = BlobSession::open(io).await.unwrap();

            // Read the leaf value at the end of the file
            let val = session.read_subtree(&["exists"]).await.unwrap();
            assert_eq!(val, ArcValue::from_value(json!("yes")));

            // Also exercise read_shallow on the same path
            let shallow = session.read_shallow(&["exists"]).await.unwrap();
            match shallow {
                ShallowValue::Primitive(v) => assert_eq!(v, ArcValue::from_value(json!("yes"))),
                _ => panic!("expected primitive, got children"),
            }
        });
    }

    /// Chaos monkey stress test: mimics the real chaos monkey tool.
    ///
    /// Multiple batches with a mix of:
    /// - Collection pushes with ~5KB values
    /// - Deletes of entire collections (set to null)
    /// - Writes to paths under previously deleted collections
    /// - Normal writes and updates to /data paths
    ///
    /// After EVERY batch, reads back ALL live values and verifies they match
    /// what was written. This catches subtle data corruption where the right
    /// type/size is returned but with wrong bytes.
    #[test]
    #[ignore] // Long-running stress test (~10min); run explicitly with: cargo test test_chaos_monkey -- --ignored
    fn test_chaos_monkey() {
        use crate::cached_io::CachedIO;
        use crate::io::StdBlobIO;

        block_on(async {
            // Use CachedIO<StdBlobIO> — real filesystem I/O, matching production.
            // This exercises actual pread_at/pwrite_at system calls, filesystem
            // caching, and I/O ordering that MemBlobIO can't reproduce.
            let dir = std::env::temp_dir().join("lark-blob-test-chaos-monkey");
            std::fs::create_dir_all(&dir).ok();
            let blob_path = dir.join("blob.lark");
            // Clean up any previous run
            for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
                std::fs::remove_file(entry.path()).ok();
            }
            let raw_io = StdBlobIO::create(&blob_path).unwrap();
            let io = CachedIO::new(raw_io);
            // Create sidecar before moving io into session
            let sidecar_io = io.create_related("sidecar").await.unwrap();
            let mut session = BlobSession::init(io).await.unwrap();

            let collections = ["players", "messages", "scores", "inventory", "events"];

            // Helper to generate a value with varying size (~2-8KB)
            // Production chaos monkey generates varying sizes which stress
            // forwarding and compaction at different thresholds.
            let make_push_value = |id: &str, i: usize| -> ArcValue {
                let content_len = 2000 + (i * 31) % 6000; // 2000-8000 chars
                let content: String = (0..content_len)
                    .map(|j| (b'a' + ((i + j) % 26) as u8) as char)
                    .collect();
                ArcValue::from_value(json!({
                    "id": id,
                    "content": content,
                    "author": format!("user-{}", i % 50),
                    "timestamp": 1700000000000u64 + i as u64,
                    "metadata": {
                        "type": "message",
                        "priority": i % 5,
                    }
                }))
            };

            // Helper to generate a push-ID key
            let make_push_id = |counter: usize| -> String { format!("-Mpush{:06}", counter) };

            let mut push_counter = 0usize;

            // Track all live values: path -> expected ArcValue
            // Deleting a collection removes all entries under that prefix.
            let mut expected: std::collections::HashMap<Vec<String>, ArcValue> =
                std::collections::HashMap::new();

            // --- Batch 1: Seed 2 collections ---
            eprintln!("=== Batch 1: Seeding collections ===");
            let mut updates: Vec<(Vec<String>, Option<ArcValue>)> = Vec::new();
            #[allow(clippy::needless_range_loop)]
            for coll_idx in 0..2 {
                let coll = collections[coll_idx];
                for _ in 0..60 {
                    push_counter += 1;
                    let push_id = make_push_id(push_counter);
                    let value = make_push_value(&push_id, push_counter);
                    let path = vec!["collections".to_string(), coll.to_string(), push_id];
                    expected.insert(path.clone(), value.clone());
                    updates.push((path, Some(value)));
                }
            }
            // Also add some /data writes
            for i in 0..20 {
                let val = ArcValue::from_value(json!({
                    "name": format!("-item-abcdefg-{}", i),
                    "value": i * 100,
                    "active": i % 2 == 0,
                }));
                let path = vec!["data".to_string(), format!("-item-abcdefg-{}", i)];
                expected.insert(path.clone(), val.clone());
                updates.push((path, Some(val)));
            }
            let r = session
                .apply_updates_with_sidecar(&updates, Some(&sidecar_io))
                .await
                .expect("batch 1 should succeed");
            match &r {
                ApplyResult::Applied(s) => eprintln!(
                    "batch 1: {} updates, {} rewrites",
                    s.updates_applied, s.parent_rewrites
                ),
            }

            verify_all(&session, &expected, "batch 1").await;

            // --- Batch 2: Delete a collection, push to others ---
            eprintln!("=== Batch 2: Delete + push ===");
            let mut updates2: Vec<(Vec<String>, Option<ArcValue>)> = Vec::new();

            // More pushes to the same collections
            #[allow(clippy::needless_range_loop)]
            for coll_idx in 0..3 {
                let coll = collections[coll_idx];
                for _ in 0..20 {
                    push_counter += 1;
                    let push_id = make_push_id(push_counter);
                    let value = make_push_value(&push_id, push_counter);
                    let path = vec!["collections".to_string(), coll.to_string(), push_id];
                    expected.insert(path.clone(), value.clone());
                    updates2.push((path, Some(value)));
                }
            }
            // Delete entire "players" collection
            updates2.push((vec!["collections".to_string(), "players".to_string()], None));
            // Remove all expected values under collections/players
            expected.retain(|k, _| !(k.len() >= 2 && k[0] == "collections" && k[1] == "players"));
            // Normal /data writes
            for i in 20..30 {
                let val = ArcValue::from_value(json!({"value": i * 100}));
                let path = vec!["data".to_string(), format!("-item-abcdefg-{}", i)];
                expected.insert(path.clone(), val.clone());
                updates2.push((path, Some(val)));
            }
            let r = session
                .apply_updates_with_sidecar(&updates2, Some(&sidecar_io))
                .await
                .expect("batch 2 should succeed");
            match &r {
                ApplyResult::Applied(s) => eprintln!(
                    "batch 2: {} updates, {} rewrites",
                    s.updates_applied, s.parent_rewrites
                ),
            }

            verify_all(&session, &expected, "batch 2").await;

            // --- Batch 3: Write back into the deleted "players" + more pushes ---
            eprintln!("=== Batch 3: Writes to deleted collection ===");
            let mut updates3: Vec<(Vec<String>, Option<ArcValue>)> = Vec::new();

            // Write new items into previously deleted "players"
            for _ in 0..30 {
                push_counter += 1;
                let push_id = make_push_id(push_counter);
                let value = make_push_value(&push_id, push_counter);
                let path = vec!["collections".to_string(), "players".to_string(), push_id];
                expected.insert(path.clone(), value.clone());
                updates3.push((path, Some(value)));
            }
            // Delete another collection
            updates3.push((
                vec!["collections".to_string(), "messages".to_string()],
                None,
            ));
            expected.retain(|k, _| !(k.len() >= 2 && k[0] == "collections" && k[1] == "messages"));
            // Push to remaining
            #[allow(clippy::needless_range_loop)]
            for coll_idx in 2..5 {
                let coll = collections[coll_idx];
                for _ in 0..20 {
                    push_counter += 1;
                    let push_id = make_push_id(push_counter);
                    let value = make_push_value(&push_id, push_counter);
                    let path = vec!["collections".to_string(), coll.to_string(), push_id];
                    expected.insert(path.clone(), value.clone());
                    updates3.push((path, Some(value)));
                }
            }
            // Update existing /data entries (overwrites)
            for i in 0..20 {
                let val = ArcValue::from_value(json!({"value": i * 200, "updated": true}));
                let path = vec!["data".to_string(), format!("-item-abcdefg-{}", i)];
                expected.insert(path.clone(), val.clone());
                updates3.push((path, Some(val)));
            }
            let r = session
                .apply_updates_with_sidecar(&updates3, Some(&sidecar_io))
                .await
                .expect("batch 3 should succeed");
            match &r {
                ApplyResult::Applied(s) => eprintln!(
                    "batch 3: {} updates, {} rewrites",
                    s.updates_applied, s.parent_rewrites
                ),
            }

            verify_all(&session, &expected, "batch 3").await;

            // --- Batch 4: Delete + re-create + push ---
            eprintln!("=== Batch 4: Heavy churn ===");
            let mut updates4: Vec<(Vec<String>, Option<ArcValue>)> = Vec::new();
            updates4.push((vec!["collections".to_string(), "scores".to_string()], None));
            updates4.push((
                vec!["collections".to_string(), "inventory".to_string()],
                None,
            ));
            expected.retain(|k, _| {
                !(k.len() >= 2
                    && k[0] == "collections"
                    && (k[1] == "scores" || k[1] == "inventory"))
            });
            for coll in &collections {
                for _ in 0..15 {
                    push_counter += 1;
                    let push_id = make_push_id(push_counter);
                    let value = make_push_value(&push_id, push_counter);
                    let path = vec!["collections".to_string(), coll.to_string(), push_id];
                    expected.insert(path.clone(), value.clone());
                    updates4.push((path, Some(value)));
                }
            }
            let r = session
                .apply_updates_with_sidecar(&updates4, Some(&sidecar_io))
                .await
                .expect("batch 4 should succeed");
            match &r {
                ApplyResult::Applied(s) => eprintln!(
                    "batch 4: {} updates, {} rewrites",
                    s.updates_applied, s.parent_rewrites
                ),
            }

            verify_all(&session, &expected, "batch 4").await;

            // --- Batch 5: More of everything ---
            eprintln!("=== Batch 5: Final push ===");
            let mut updates5: Vec<(Vec<String>, Option<ArcValue>)> = Vec::new();
            updates5.push((vec!["collections".to_string(), "events".to_string()], None));
            expected.retain(|k, _| !(k.len() >= 2 && k[0] == "collections" && k[1] == "events"));
            for coll in &collections {
                for _ in 0..25 {
                    push_counter += 1;
                    let push_id = make_push_id(push_counter);
                    let value = make_push_value(&push_id, push_counter);
                    let path = vec!["collections".to_string(), coll.to_string(), push_id];
                    expected.insert(path.clone(), value.clone());
                    updates5.push((path, Some(value)));
                }
            }
            let r = session
                .apply_updates_with_sidecar(&updates5, Some(&sidecar_io))
                .await
                .expect("batch 5 should succeed");
            match &r {
                ApplyResult::Applied(s) => eprintln!(
                    "batch 5: {} updates, {} rewrites",
                    s.updates_applied, s.parent_rewrites
                ),
            }

            verify_all(&session, &expected, "batch 5").await;

            // --- Batches 6-2000: Extended stress with updates to existing values ---
            // Production chaos monkey runs 60-90s per cycle. 100 batches ≈ 3.6s,
            // so 2000 batches ≈ 72s — comparable to a single cycle.
            // Verify every 10th batch (plus final) to keep runtime manageable.
            for batch_num in 6..=2000 {
                if batch_num % 100 == 0 {
                    eprintln!("=== Batch {}: Extended stress ===", batch_num);
                }
                let mut batch_updates: Vec<(Vec<String>, Option<ArcValue>)> = Vec::new();

                // Delete one collection every 3 batches
                if batch_num % 3 == 0 {
                    let coll = collections[batch_num % collections.len()];
                    batch_updates.push((vec!["collections".to_string(), coll.to_string()], None));
                    expected
                        .retain(|k, _| !(k.len() >= 2 && k[0] == "collections" && k[1] == coll));
                }

                // Push to collections
                for coll in &collections {
                    for _ in 0..10 {
                        push_counter += 1;
                        let push_id = make_push_id(push_counter);
                        let value = make_push_value(&push_id, push_counter);
                        let path = vec!["collections".to_string(), coll.to_string(), push_id];
                        expected.insert(path.clone(), value.clone());
                        batch_updates.push((path, Some(value)));
                    }
                }

                // Update individual LEAF FIELDS within containers —
                // this mimics chaos monkey's "set" operation that updates
                // updated_at (number) and data (string) fields within push-ID
                // items.
                //
                // We track FIELD-level paths (4 components) separately from
                // ITEM-level paths (3 components), so both can be verified
                // without conflicts.
                let existing_keys: Vec<(String, String)> = expected
                    .keys()
                    .filter(|k| k.len() == 3 && k[0] == "collections")
                    .take(20)
                    .map(|k| (k[1].clone(), k[2].clone()))
                    .collect();
                for (coll, push_id) in &existing_keys {
                    // Update "timestamp" field (number, in-place same-size)
                    let ts = 1700000000000u64 + (batch_num as u64) * 1000 + push_counter as u64;
                    let ts_val = ArcValue::from_value(json!(ts));
                    let ts_path = vec![
                        "collections".to_string(),
                        coll.clone(),
                        push_id.clone(),
                        "timestamp".to_string(),
                    ];
                    expected.insert(ts_path.clone(), ts_val.clone());
                    batch_updates.push((ts_path, Some(ts_val)));

                    // Update "content" field (string, may trigger forward if larger)
                    // Vary size significantly — production saw corruption at 34663 bytes
                    let content_len = 4000 + (batch_num * 17 + push_counter * 7) % 3000;
                    let content: String = (0..content_len)
                        .map(|j| (b'A' + ((batch_num + j) % 26) as u8) as char)
                        .collect();
                    let content_val = ArcValue::from_value(json!(content));
                    let content_path = vec![
                        "collections".to_string(),
                        coll.clone(),
                        push_id.clone(),
                        "content".to_string(),
                    ];
                    expected.insert(content_path.clone(), content_val.clone());
                    batch_updates.push((content_path, Some(content_val)));

                    // Remove the item-level expected (now stale since we
                    // modified individual fields, changing the object shape)
                    let item_path = vec!["collections".to_string(), coll.clone(), push_id.clone()];
                    expected.remove(&item_path);
                }

                // Update /data values (overwrite existing)
                for i in 0..10 {
                    let val = ArcValue::from_value(json!({
                        "value": (batch_num as i64) * 1000 + i,
                        "batch": batch_num,
                        "updated_at": 1700000000000u64 + (batch_num as u64) * 1000 + i as u64,
                    }));
                    let path = vec!["data".to_string(), format!("-item-abcdefg-{}", i)];
                    expected.insert(path.clone(), val.clone());
                    batch_updates.push((path, Some(val)));
                }

                // Add some deep paths (separate from /data items to avoid
                // conflicts — writing /deep/-item/sub modifies the /deep/-item
                // object, which would make our expected entry for it stale)
                for i in 0..5 {
                    let val = ArcValue::from_value(json!({
                        "deep_value": batch_num * 100 + i,
                        "data": format!("deep-data-{}-{}", batch_num, i),
                    }));
                    let path = vec![
                        "deep".to_string(),
                        format!("-item-deep-{}", batch_num),
                        format!("sub-{}", i),
                    ];
                    expected.insert(path.clone(), val.clone());
                    batch_updates.push((path, Some(val)));
                }

                let r = session
                    .apply_updates_with_sidecar(&batch_updates, Some(&sidecar_io))
                    .await
                    .unwrap_or_else(|e| panic!("batch {} failed: {:?}", batch_num, e));
                match &r {
                    ApplyResult::Applied(s) => {
                        if batch_num % 100 == 0 {
                            eprintln!(
                                "batch {}: {} updates, {} rewrites",
                                batch_num, s.updates_applied, s.parent_rewrites,
                            );
                        }
                    }
                }
                // Verify every 10th batch with the writer session (fast, cached)
                if batch_num % 10 == 0 || batch_num == 2000 {
                    verify_all(&session, &expected, &format!("batch {}", batch_num)).await;
                }

                // Every 50th batch: open a FRESH session from disk and verify.
                // This simulates what chaos monkey does — kill the process, reopen
                // from the raw file, and check all values. The fresh session has no
                // cached headers, no cached IO regions.
                // If any on-disk state is inconsistent, this will catch it.
                if batch_num % 50 == 0 || batch_num == 2000 {
                    // Sync the writer's IO to ensure all data is on disk
                    session.io.sync().await.unwrap();
                    sidecar_io.sync().await.unwrap();

                    let fresh_raw = StdBlobIO::open(&blob_path).unwrap();
                    let fresh_io = CachedIO::new(fresh_raw);
                    let fresh_sidecar = fresh_io.open_related("sidecar").await.unwrap();
                    let fresh_session =
                        BlobSession::open_with_sidecar(fresh_io, Some(&fresh_sidecar))
                            .await
                            .unwrap_or_else(|e| {
                                panic!(
                                    "fresh session open failed after batch {}: {:?}",
                                    batch_num, e
                                )
                            });
                    verify_all(
                        &fresh_session,
                        &expected,
                        &format!("FRESH batch {}", batch_num),
                    )
                    .await;
                }
            }

            eprintln!(
                "=== Chaos monkey test passed: {} paths verified across 2000 batches ===",
                expected.len(),
            );
        });
    }

    #[test]
    fn test_diagnose_path() {
        block_on(async {
            let io = MemBlobIO::new();
            let sidecar_io = MemBlobIO::new();
            let mut session = BlobSession::init(io).await.unwrap();

            // Seed some data
            let mut updates = Vec::new();
            for i in 0..20 {
                let val = ArcValue::from_value(json!({
                    "name": format!("item-{}", i),
                    "value": i * 100,
                    "content": "x".repeat(4000),
                }));
                updates.push((vec!["data".to_string(), format!("-item-{}", i)], Some(val)));
            }
            session
                .apply_updates_with_sidecar(&updates, Some(&sidecar_io))
                .await
                .unwrap();

            // Diagnose a leaf path (number)
            let diag = session.diagnose_path(&["data", "-item-5", "value"]).await;
            eprintln!("{}", diag);
            assert!(diag.contains("DIAGNOSE PATH: /data/-item-5/value"));
            assert!(diag.contains("LEAF:"));

            // Diagnose a non-existent path
            let diag = session.diagnose_path(&["data", "-item-999"]).await;
            eprintln!("{}", diag);
            assert!(diag.contains("NOT FOUND"));

            // Diagnose a container path
            let diag = session.diagnose_path(&["data", "-item-5"]).await;
            eprintln!("{}", diag);
            assert!(diag.contains("LEAF:"));
        });
    }

    #[test]
    fn test_write_amplification_bounds() {
        block_on(async {
            let io = MemBlobIO::new();
            let mut session = BlobSession::init(io.clone()).await.unwrap();

            // Generate a padding string of a given size
            fn make_value(size: usize) -> String {
                "x".repeat(size)
            }

            // Track total value bytes written
            let mut total_value_bytes: u64 = 0;

            // === Batch 1: Seed data (like chaos-monkey seeding phase) ===
            // burst: 100 items, each ~5KB (large values)
            let mut updates: Vec<(Vec<String>, Option<ArcValue>)> = Vec::new();
            for i in 0..100 {
                let key = format!("-item-{}", i);
                let val = ArcValue::from_value(json!({
                    "data": make_value(5000),
                    "seq": i,
                }));
                let val_size = 5000 + 20; // approximate
                total_value_bytes += val_size as u64;
                updates.push((vec!["burst".to_string(), key], Some(val)));
            }
            // data: 100 items, each ~80B (small values)
            for i in 0..100 {
                let key = format!("-item-{}", i);
                let val = ArcValue::from_value(json!({
                    "active": true,
                    "name": format!("-item-{}", i),
                    "value": i,
                }));
                total_value_bytes += 80;
                updates.push((vec!["data".to_string(), key], Some(val)));
            }
            // collections/players: 20 items, each ~1KB
            for i in 0..20 {
                let key = format!("-player-{}", i);
                let val = ArcValue::from_value(json!({
                    "content": make_value(1000),
                }));
                total_value_bytes += 1000;
                updates.push((
                    vec!["collections".to_string(), "players".to_string(), key],
                    Some(val),
                ));
            }
            let stats = apply(&mut session, &updates).await;
            let size_after_seed = io.size().await.unwrap();
            eprintln!(
                "After seed: blob={} bytes, updates={}, value_bytes={}",
                size_after_seed, stats.updates_applied, total_value_bytes
            );

            // === Batches 2-5: Ongoing writes (like chaos-monkey operation phase) ===
            // Each batch: some new items + overwrites of existing items
            for batch in 0..4 {
                let mut updates: Vec<(Vec<String>, Option<ArcValue>)> = Vec::new();

                // burst: 20 new items + 30 overwrites of existing items
                for i in 0..20 {
                    let key = format!("-item-{}", 100 + batch * 20 + i);
                    let val = ArcValue::from_value(json!({
                        "data": make_value(5000),
                        "seq": 100 + batch * 20 + i,
                    }));
                    total_value_bytes += 5020;
                    updates.push((vec!["burst".to_string(), key], Some(val)));
                }
                for i in 0..30 {
                    let existing_idx = (batch * 7 + i * 3) % 100;
                    let key = format!("-item-{}", existing_idx);
                    let val = ArcValue::from_value(json!({
                        "data": make_value(5000),
                        "seq": 1000 + batch * 30 + i,
                    }));
                    total_value_bytes += 5020;
                    updates.push((vec!["burst".to_string(), key], Some(val)));
                }

                // data: 20 new items + 10 overwrites
                for i in 0..20 {
                    let key = format!("-item-{}", 100 + batch * 20 + i);
                    let val = ArcValue::from_value(json!({
                        "active": true,
                        "name": format!("-item-{}", 100 + batch * 20 + i),
                        "value": 1000 + batch * 20 + i,
                    }));
                    total_value_bytes += 80;
                    updates.push((vec!["data".to_string(), key], Some(val)));
                }
                for i in 0..10 {
                    let existing_idx = (batch * 5 + i * 7) % 100;
                    let key = format!("-item-{}", existing_idx);
                    let val = ArcValue::from_value(json!({
                        "active": false,
                        "name": format!("-item-{}", existing_idx),
                        "value": 9999,
                    }));
                    total_value_bytes += 80;
                    updates.push((vec!["data".to_string(), key], Some(val)));
                }

                // collections/players: 5 new + 5 overwrites
                for i in 0..5 {
                    let key = format!("-player-{}", 20 + batch * 5 + i);
                    let val = ArcValue::from_value(json!({
                        "content": make_value(1000),
                    }));
                    total_value_bytes += 1000;
                    updates.push((
                        vec!["collections".to_string(), "players".to_string(), key],
                        Some(val),
                    ));
                }
                for i in 0..5 {
                    let existing_idx = (batch * 3 + i * 4) % 20;
                    let key = format!("-player-{}", existing_idx);
                    let val = ArcValue::from_value(json!({
                        "content": make_value(1000),
                    }));
                    total_value_bytes += 1000;
                    updates.push((
                        vec!["collections".to_string(), "players".to_string(), key],
                        Some(val),
                    ));
                }

                let stats = apply(&mut session, &updates).await;
                let blob_size = io.size().await.unwrap();
                let amplification = blob_size as f64 / total_value_bytes as f64;
                eprintln!(
                    "After batch {}: blob={} ({:.2} MB), value_bytes={} ({:.2} MB), \
                     amplification={:.1}x, in_place={}, forwards={}, rewrites={}, \
                     collection_inserts={}, appended={}",
                    batch + 1,
                    blob_size,
                    blob_size as f64 / 1048576.0,
                    total_value_bytes,
                    total_value_bytes as f64 / 1048576.0,
                    amplification,
                    stats.in_place_updates,
                    stats.forward_updates,
                    stats.parent_rewrites,
                    stats.collection_inserts,
                    stats.bytes_appended,
                );
            }

            let final_size = io.size().await.unwrap();
            let amplification = final_size as f64 / total_value_bytes as f64;

            eprintln!("\n=== SUMMARY ===");
            eprintln!(
                "Total value bytes written: {} ({:.2} MB)",
                total_value_bytes,
                total_value_bytes as f64 / 1048576.0
            );
            eprintln!(
                "Final blob size: {} ({:.2} MB)",
                final_size,
                final_size as f64 / 1048576.0
            );
            eprintln!("Write amplification: {:.1}x", amplification);

            // Verify all data is readable
            let root = session.read_subtree(&[]).await.unwrap();
            let burst = root.get("burst").unwrap().as_object().unwrap();
            eprintln!("burst children: {}", burst.len());
            let data = root.get("data").unwrap().as_object().unwrap();
            eprintln!("data children: {}", data.len());

            // The blob should NOT be more than 10x the total value bytes.
            // With perfect compaction it would be ~2-3x (format overhead).
            // With reasonable incremental behavior, should be well under 10x.
            assert!(
                amplification < 10.0,
                "Write amplification is {:.1}x — blob is {} bytes for {} value bytes. \
                 This indicates excessive compaction cascading.",
                amplification,
                final_size,
                total_value_bytes,
            );
        });
    }

    /// Test concurrent reader/writer on the same file.
    ///
    /// Simulates lark-server's architecture: a writer BlobSession applies
    /// updates while a reader BlobSession (opened on the same IO) reads
    /// data after clearing its cache.
    #[test]
    fn test_concurrent_reader_writer() {
        block_on(async {
            // Initial blob with seed data
            let tree = ArcValue::from_value(json!({
                "config": {"mode": "dark"},
                "data": {
                    "-item-0": {"active": true, "name": "item-0", "value": 0}
                }
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            // Reader session (like Database.run in lark-server)
            let mut reader = BlobSession::open(io.clone()).await.unwrap();

            // Writer session (like StorageWorker in lark-server)
            let mut writer = BlobSession::open(io.clone()).await.unwrap();

            // Writer applies batch 1: add a collection with many children
            let mut updates = Vec::new();
            for i in 0..50 {
                let key = format!("-Mchar{:04}", i);
                updates.push((
                    vec!["characters".to_string(), key],
                    Some(ArcValue::from_value(json!({
                        "hp": 100 + i,
                        "name": format!("Hero_{}", i),
                        "x": i * 10,
                        "y": i * 20,
                        "bio": format!("Character {} with padding text here", i)
                    }))),
                ));
            }
            apply(&mut writer, &updates).await;

            // Reader clears cache (what lark-server does on compaction_complete)
            reader.clear_cache();
            reader.io().clear_read_cache().await;

            // Reader reads data written by the writer
            let hp = reader
                .read_subtree(&["characters", "-Mchar0025", "hp"])
                .await;
            assert!(
                hp.is_ok(),
                "read_subtree should work after writer update: {:?}",
                hp.err()
            );
            assert_eq!(hp.unwrap().as_i64(), Some(125));

            // Original data should survive
            let config = reader.read_subtree(&["config", "mode"]).await;
            assert!(
                config.is_ok(),
                "original data should survive: {:?}",
                config.err()
            );
            assert_eq!(config.unwrap().as_str(), Some("dark"));

            let item0 = reader.read_subtree(&["data", "-item-0", "name"]).await;
            assert!(
                item0.is_ok(),
                "original data should survive: {:?}",
                item0.err()
            );
            assert_eq!(item0.unwrap().as_str(), Some("item-0"));

            // Writer applies batch 2: overwrites + new inserts
            let mut updates2 = Vec::new();
            for i in 0..10 {
                updates2.push((
                    vec![
                        "characters".to_string(),
                        format!("-Mchar{:04}", i),
                        "hp".to_string(),
                    ],
                    Some(ArcValue::from(999i64)),
                ));
            }
            for i in 50..70 {
                let key = format!("-Mchar{:04}", i);
                updates2.push((
                    vec!["characters".to_string(), key],
                    Some(ArcValue::from_value(json!({
                        "hp": 200 + i,
                        "name": format!("NewHero_{}", i),
                    }))),
                ));
            }
            apply(&mut writer, &updates2).await;

            // Reader clears cache again
            reader.clear_cache();
            reader.io().clear_read_cache().await;

            // Reader sees updated values
            let hp = reader
                .read_subtree(&["characters", "-Mchar0005", "hp"])
                .await;
            assert!(hp.is_ok(), "reader should see updated hp: {:?}", hp.err());
            assert_eq!(hp.unwrap().as_i64(), Some(999));

            // Reader sees newly inserted values
            let hp = reader
                .read_subtree(&["characters", "-Mchar0060", "hp"])
                .await;
            assert!(
                hp.is_ok(),
                "reader should see new character: {:?}",
                hp.err()
            );
            assert_eq!(hp.unwrap().as_i64(), Some(260));

            // Unchanged values still correct
            let hp = reader
                .read_subtree(&["characters", "-Mchar0030", "hp"])
                .await;
            assert!(
                hp.is_ok(),
                "unchanged character should be readable: {:?}",
                hp.err()
            );
            assert_eq!(hp.unwrap().as_i64(), Some(130));
        });
    }

    /// Test that a blob with large containers (>10MB equivalent) works correctly
    /// through multiple batches and root_compact, now that there's no segment
    /// extraction safety valve.
    #[test]
    fn test_large_blob_without_segments() {
        block_on(async {
            let io = MemBlobIO::new();
            let mut session = BlobSession::init(io.clone()).await.unwrap();

            // Build a large collection: 200 items, each ~5KB = ~1MB total.
            // (Using real 10MB+ would make the test slow; this is enough to
            // exercise the compaction path with large containers.)
            let mut updates: Vec<(Vec<String>, Option<ArcValue>)> = Vec::new();
            for i in 0..200 {
                let key = format!("-Mpush{:06}", i);
                let val = ArcValue::from_value(json!({
                    "content": "x".repeat(4000),
                    "seq": i,
                    "metadata": {"type": "message", "priority": i % 5},
                }));
                updates.push((vec!["big_collection".to_string(), key], Some(val)));
            }
            let stats = apply(&mut session, &updates).await;
            let size_after_seed = io.size().await.unwrap();
            eprintln!(
                "After seed: blob={} ({:.2} MB), {} updates",
                size_after_seed,
                size_after_seed as f64 / 1048576.0,
                stats.updates_applied
            );

            // Verify seed data
            for i in [0, 50, 100, 199] {
                let val = session
                    .read_subtree(&["big_collection", &format!("-Mpush{:06}", i), "seq"])
                    .await
                    .unwrap();
                assert_eq!(val.as_i64(), Some(i as i64));
            }

            // Batch 2: Overwrite ~50% of items + add 50 new ones
            let mut updates2: Vec<(Vec<String>, Option<ArcValue>)> = Vec::new();
            for i in 0..100 {
                let key = format!("-Mpush{:06}", i);
                let val = ArcValue::from_value(json!({
                    "content": "y".repeat(4000),
                    "seq": 1000 + i,
                    "metadata": {"type": "updated", "priority": 0},
                }));
                updates2.push((vec!["big_collection".to_string(), key], Some(val)));
            }
            for i in 200..250 {
                let key = format!("-Mpush{:06}", i);
                let val = ArcValue::from_value(json!({
                    "content": "z".repeat(4000),
                    "seq": i,
                    "metadata": {"type": "new", "priority": 1},
                }));
                updates2.push((vec!["big_collection".to_string(), key], Some(val)));
            }
            let stats2 = apply(&mut session, &updates2).await;
            let size_after_batch2 = io.size().await.unwrap();
            eprintln!(
                "After batch 2: blob={} ({:.2} MB), {} updates, {} rewrites",
                size_after_batch2,
                size_after_batch2 as f64 / 1048576.0,
                stats2.updates_applied,
                stats2.parent_rewrites
            );

            // Verify overwrites and new items
            let val = session
                .read_subtree(&["big_collection", "-Mpush000050", "seq"])
                .await
                .unwrap();
            assert_eq!(val.as_i64(), Some(1050));

            let val = session
                .read_subtree(&["big_collection", "-Mpush000225", "seq"])
                .await
                .unwrap();
            assert_eq!(val.as_i64(), Some(225));

            // Items NOT overwritten should still have original values
            let val = session
                .read_subtree(&["big_collection", "-Mpush000150", "seq"])
                .await
                .unwrap();
            assert_eq!(val.as_i64(), Some(150));

            // Root compact to a new file
            let dst = MemBlobIO::new();
            let old_io = session.root_compact(dst).await.unwrap();
            let compacted_size = session.io().size().await.unwrap();
            eprintln!(
                "After root_compact: {} ({:.2} MB), old blob was {} ({:.2} MB)",
                compacted_size,
                compacted_size as f64 / 1048576.0,
                size_after_batch2,
                size_after_batch2 as f64 / 1048576.0
            );
            old_io.close().await.unwrap();

            // Compacted blob should be significantly smaller (dead space reclaimed)
            assert!(
                compacted_size < size_after_batch2,
                "compacted blob ({}) should be smaller than pre-compact ({})",
                compacted_size,
                size_after_batch2,
            );

            // All data should survive compaction
            for i in 0..100 {
                let val = session
                    .read_subtree(&["big_collection", &format!("-Mpush{:06}", i), "seq"])
                    .await
                    .unwrap();
                assert_eq!(
                    val.as_i64(),
                    Some(1000 + i as i64),
                    "overwritten item {} should have new seq after compact",
                    i
                );
            }
            for i in 100..200 {
                let val = session
                    .read_subtree(&["big_collection", &format!("-Mpush{:06}", i), "seq"])
                    .await
                    .unwrap();
                assert_eq!(
                    val.as_i64(),
                    Some(i as i64),
                    "unchanged item {} should survive compact",
                    i
                );
            }
            for i in 200..250 {
                let val = session
                    .read_subtree(&["big_collection", &format!("-Mpush{:06}", i), "seq"])
                    .await
                    .unwrap();
                assert_eq!(
                    val.as_i64(),
                    Some(i as i64),
                    "new item {} should survive compact",
                    i
                );
            }

            // Further updates should work on the compacted blob
            let updates3 = vec![(
                vec![
                    "big_collection".to_string(),
                    "-Mpush000000".to_string(),
                    "seq".to_string(),
                ],
                Some(ArcValue::from(9999i64)),
            )];
            apply(&mut session, &updates3).await;
            let val = session
                .read_subtree(&["big_collection", "-Mpush000000", "seq"])
                .await
                .unwrap();
            assert_eq!(val.as_i64(), Some(9999));
        });
    }

    /// Summarize an ArcValue for error messages (avoid printing huge strings)
    fn summarize_value(v: &ArcValue) -> String {
        match v {
            ArcValue::String(s) => {
                if s.len() > 60 {
                    format!("String(len={}, {:?}...)", s.len(), &s[..30])
                } else {
                    format!("String({:?})", s)
                }
            }
            ArcValue::Number(n) => format!("Number({})", n),
            ArcValue::Bool(b) => format!("Bool({})", b),
            ArcValue::Null => "Null".to_string(),
            ArcValue::Object(map) => format!("Object({} keys)", map.len()),
            _ => format!("{:?}", v),
        }
    }

    /// Verify all expected values match what the session returns.
    async fn verify_all<IO: crate::io::BlobIO>(
        session: &BlobSession<IO>,
        expected: &std::collections::HashMap<Vec<String>, ArcValue>,
        batch_label: &str,
    ) {
        let mut violations = 0;
        for (path, expected_val) in expected {
            let path_refs: Vec<&str> = path.iter().map(|s| s.as_str()).collect();
            match session.read_subtree(&path_refs).await {
                Ok(actual) => {
                    if actual != *expected_val {
                        eprintln!(
                            "VIOLATION after {}: /{} — expected {:?}, got {:?}",
                            batch_label,
                            path.join("/"),
                            summarize_value(expected_val),
                            summarize_value(&actual),
                        );
                        violations += 1;
                    }
                }
                Err(e) => {
                    eprintln!(
                        "VIOLATION after {}: /{} — read error: {:?}",
                        batch_label,
                        path.join("/"),
                        e,
                    );
                    violations += 1;
                }
            }
        }
        if violations > 0 {
            panic!(
                "{} violations found after {} (checked {} paths)",
                violations,
                batch_label,
                expected.len(),
            );
        }
        eprintln!(
            "  verified {}/{} paths OK after {}",
            expected.len(),
            expected.len(),
            batch_label,
        );
    }
}
