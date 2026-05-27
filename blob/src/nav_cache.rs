//! Container reading helpers for blob traversal.
//!
//! `read_container` parses a container header + child index from IO. When
//! the IO is wrapped in `CachedIO`, byte-level caching is automatic and
//! write-through — no manual invalidation needed.
//!
//! `NavigationCache`: path -> BlobLocation mappings (unused currently, placeholder).

use crate::dictionary::hash_field_name;
use crate::error::{BlobError, Result};
use crate::format::*;
use crate::io::{BlobIO, read_exact, read_exact_into};
use crate::session_reader::BlobLocation;
use std::collections::HashMap;

/// Parsed container header + child index.
///
/// Returned by `read_container`. Contains everything needed to navigate
/// a container: type tag, structural metadata, child index bytes (for
/// binary search), and for collections, the key string area.
#[derive(Clone)]
pub struct ContainerInfo {
    /// The offset of the container on disk.
    pub resolved_offset: u64,
    /// Node type tag (TYPE_COLLECTION or TYPE_ARRAY).
    pub tag: u8,
    pub subtree_size: u64,
    pub child_count: u32,
    pub appended_bytes: u32,
    /// Absolute offset where children data begins.
    pub children_area_offset: u64,
    /// Raw child index bytes — layout depends on tag:
    /// - TYPE_COLLECTION: (key_hash:8, type_flags:1, offset:8, size:8) × child_count
    /// - TYPE_ARRAY: (type_flags:1, offset:8, size:8) × child_count
    pub child_index: Vec<u8>,

    // Collection-specific fields (zero for non-collections)
    pub reserved_count: u32,
    pub key_data_used: u32,
    pub key_data_reserved: u32,
    /// Raw key string area bytes (only for TYPE_COLLECTION).
    pub key_strings: Vec<u8>,
}

/// Threshold for pre-caching a container's full subtree via `cache_region`.
/// Containers with subtree_size ≤ this value get their full byte range cached.
const CACHE_SUBTREE_THRESHOLD: u64 = 4096;

/// Structural layout of a container, computed with checked arithmetic. All
/// counts/sizes come from the on-disk node header and may be corrupt, so naive
/// offset math can overflow u64 (and `children_area_offset - offset` underflow),
/// which panics under overflow-checks and silently wraps otherwise. These helpers
/// compute every offset with checked ops and reject genuine overflow as corrupt.
///
/// They intentionally do NOT bound offsets against the blob length:
/// `children_area_offset` (and the reserved key region) are *logical* offsets
/// that legitimately sit past the physical blob end — reserved capacity for
/// in-place growth isn't materialized, and a fully-forwarded container never
/// reads there. The blob's real extent is unreliable on the read path anyway
/// (a `size()` Cell can lag appends). Out-of-range reads are caught where they
/// matter: `pread` caps its allocation and stops at the true EOF.
struct ContainerLayout {
    index_offset: u64,
    index_size: usize,
    key_strings_offset: u64,
    children_area_offset: u64,
    structural_size: usize,
}

fn corrupt_region() -> BlobError {
    BlobError::CorruptData("container offset arithmetic overflowed")
}

/// Layout for a TYPE_COLLECTION: header, then `total_slots` index entries, then
/// key strings, then the children area.
fn collection_layout(
    offset: u64,
    child_count: u32,
    reserved_count: u32,
    key_data_reserved: u32,
) -> Result<ContainerLayout> {
    let entry = COLLECTION_INDEX_ENTRY_SIZE as u64;
    let total_slots = child_count as u64 + reserved_count as u64; // u64: no overflow
    let index_offset = offset
        .checked_add(COLLECTION_HEADER_SIZE as u64)
        .ok_or_else(corrupt_region)?;
    let key_strings_offset = total_slots
        .checked_mul(entry)
        .and_then(|n| n.checked_add(index_offset))
        .ok_or_else(corrupt_region)?;
    let children_area_offset = key_strings_offset
        .checked_add(key_data_reserved as u64)
        .ok_or_else(corrupt_region)?;
    Ok(ContainerLayout {
        index_offset,
        index_size: child_count as usize * COLLECTION_INDEX_ENTRY_SIZE,
        key_strings_offset,
        children_area_offset,
        // children_area_offset >= offset by construction, so no underflow.
        structural_size: (children_area_offset - offset) as usize,
    })
}

/// Layout for a TYPE_ARRAY: header, then `child_count` index entries, then the
/// elements area. No reserved slots or key strings.
fn array_layout(offset: u64, child_count: u32) -> Result<ContainerLayout> {
    let entry = ARRAY_INDEX_ENTRY_SIZE as u64;
    let index_offset = offset
        .checked_add(ARRAY_HEADER_SIZE as u64)
        .ok_or_else(corrupt_region)?;
    let children_area_offset = (child_count as u64)
        .checked_mul(entry)
        .and_then(|n| n.checked_add(index_offset))
        .ok_or_else(corrupt_region)?;
    Ok(ContainerLayout {
        index_offset,
        index_size: child_count as usize * ARRAY_INDEX_ENTRY_SIZE,
        key_strings_offset: 0,
        children_area_offset,
        structural_size: (children_area_offset - offset) as usize,
    })
}

/// Read and parse a container at the given offset.
///
/// Reads the header, child index, and key strings (for collections) from IO.
///
/// `subtree_size_hint`: when the caller already knows the container's subtree_size
/// from the parent's index entry, pass it here to reduce pread count:
/// - Small (≤4KB): pre-cache entire subtree via `cache_region` → 1 pread total
/// - Large (>4KB): `cache_region` header, read + parse, `cache_region` structural,
///   read index/keys → 2 preads (with or without CachedIO)
/// - None (root node or unknown): speculative peek + header + index → 2-3 preads
pub async fn read_container<IO: BlobIO>(
    io: &IO,
    offset: u64,
    subtree_size_hint: Option<u64>,
) -> Result<ContainerInfo> {
    // ── Fast path: large container with known size ──
    // We know it's a container and subtree_size > 4KB, so ≥29 bytes are available.
    // 1. cache_region(max header) — 1 pread if CachedIO, no-op otherwise
    // 2. Read header — cache hit if CachedIO, 1 pread otherwise. Parse tag + fields.
    // 3. cache_region(structural) — 1 pread if CachedIO, no-op otherwise
    // 4. Read index/keys — cache hit if CachedIO, 1 pread otherwise
    // Total: 2 preads either way. With CachedIO, structural area is cached for future visits.
    #[allow(clippy::collapsible_if)] // large body; clearer as nested
    if let Some(hint) = subtree_size_hint {
        if hint > CACHE_SUBTREE_THRESHOLD {
            // Pre-cache max header size so the header read below is a cache hit
            let _ = io.cache_region(offset, COLLECTION_HEADER_SIZE).await;

            // Read header — cache hit with CachedIO, otherwise 1 pread.
            // Always read 29 bytes (max header). For objects/arrays we only parse
            // the first 17 bytes; the extra 12 bytes are harmless index data.
            let mut hdr = [0u8; COLLECTION_HEADER_SIZE];
            read_exact_into(io, offset, &mut hdr).await?;
            let tag = hdr[0];

            return match tag {
                TYPE_COLLECTION => {
                    let subtree_size = u64::from_le_bytes(hdr[1..9].try_into().unwrap());
                    let child_count = u32::from_le_bytes(hdr[9..13].try_into().unwrap());
                    let reserved_count = u32::from_le_bytes(hdr[13..17].try_into().unwrap());
                    let key_data_used = u32::from_le_bytes(hdr[17..21].try_into().unwrap());
                    let key_data_reserved = u32::from_le_bytes(hdr[21..25].try_into().unwrap());
                    let appended_bytes = u32::from_le_bytes(hdr[25..29].try_into().unwrap());

                    let layout =
                        collection_layout(offset, child_count, reserved_count, key_data_reserved)?;

                    // Pre-cache structural region so index + key string reads are cache hits
                    let _ = io.cache_region(offset, layout.structural_size).await;

                    let child_index = if layout.index_size > 0 {
                        read_exact(io, layout.index_offset, layout.index_size).await?
                    } else {
                        Vec::new()
                    };

                    let key_strings = if key_data_used > 0 {
                        read_exact(io, layout.key_strings_offset, key_data_used as usize).await?
                    } else {
                        Vec::new()
                    };

                    let info = ContainerInfo {
                        resolved_offset: offset,
                        tag,
                        subtree_size,
                        child_count,
                        appended_bytes,
                        children_area_offset: layout.children_area_offset,
                        child_index,
                        reserved_count,
                        key_data_used,
                        key_data_reserved,
                        key_strings,
                    };
                    Ok(info)
                }
                TYPE_ARRAY => {
                    let subtree_size = u64::from_le_bytes(hdr[1..9].try_into().unwrap());
                    let child_count = u32::from_le_bytes(hdr[9..13].try_into().unwrap());
                    let appended_bytes = u32::from_le_bytes(hdr[13..17].try_into().unwrap());

                    let layout = array_layout(offset, child_count)?;

                    // Pre-cache structural region so the index read is a cache hit
                    let _ = io.cache_region(offset, layout.structural_size).await;

                    let child_index = if layout.index_size > 0 {
                        read_exact(io, layout.index_offset, layout.index_size).await?
                    } else {
                        Vec::new()
                    };

                    let info = ContainerInfo {
                        resolved_offset: offset,
                        tag,
                        subtree_size,
                        child_count,
                        appended_bytes,
                        children_area_offset: layout.children_area_offset,
                        child_index,
                        reserved_count: 0,
                        key_data_used: 0,
                        key_data_reserved: 0,
                        key_strings: Vec::new(),
                    };
                    Ok(info)
                }
                _ => Err(BlobError::NotAContainer(offset, tag)),
            };
        }
    }

    // ── Small hint or no hint path ──
    // Small hint (≤4KB): pre-cache entire subtree, then all reads are cache hits (1 pread).
    // No hint (root): speculative peek with EOF handling, then header + index reads.
    if let Some(hint) = subtree_size_hint
        && hint > 0
        && hint <= CACHE_SUBTREE_THRESHOLD
    {
        let _ = io.cache_region(offset, hint as usize).await;
    }

    // Speculative 9-byte read: tag (1) + subtree_size (8).
    // Cache hit if small hint pre-cached above; otherwise a fresh pread.
    // If the read fails (e.g. leaf node near EOF), treat as not-a-container.
    let mut peek = [0u8; 9];
    match read_exact_into(io, offset, &mut peek).await {
        Ok(()) => {}
        Err(BlobError::Io(ref e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            // Not enough bytes for a container header — must be a leaf node.
            // Read just the tag byte so we can return a proper NotAContainer error.
            let mut tag_buf = [0u8; 1];
            read_exact_into(io, offset, &mut tag_buf).await?;
            return Err(BlobError::NotAContainer(offset, tag_buf[0]));
        }
        Err(e) => return Err(e),
    }
    let tag = peek[0];

    match tag {
        TYPE_COLLECTION | TYPE_ARRAY => {
            let subtree_size = u64::from_le_bytes(peek[1..9].try_into().unwrap());
            // No hint: cache small subtrees discovered during peek
            if subtree_size_hint.is_none()
                && subtree_size > 0
                && subtree_size <= CACHE_SUBTREE_THRESHOLD
            {
                let _ = io.cache_region(offset, subtree_size as usize).await;
            }
        }
        _ => return Err(BlobError::NotAContainer(offset, tag)),
    }

    match tag {
        TYPE_COLLECTION => {
            let mut header_data = [0u8; COLLECTION_HEADER_SIZE];
            read_exact_into(io, offset, &mut header_data).await?;
            let subtree_size = u64::from_le_bytes(header_data[1..9].try_into().unwrap());
            let child_count = u32::from_le_bytes(header_data[9..13].try_into().unwrap());
            let reserved_count = u32::from_le_bytes(header_data[13..17].try_into().unwrap());
            let key_data_used = u32::from_le_bytes(header_data[17..21].try_into().unwrap());
            let key_data_reserved = u32::from_le_bytes(header_data[21..25].try_into().unwrap());
            let appended_bytes = u32::from_le_bytes(header_data[25..29].try_into().unwrap());

            let layout = collection_layout(offset, child_count, reserved_count, key_data_reserved)?;

            // For large containers without a hint (root), cache structural region
            if subtree_size > CACHE_SUBTREE_THRESHOLD {
                let _ = io.cache_region(offset, layout.structural_size).await;
            }

            let child_index = if layout.index_size > 0 {
                read_exact(io, layout.index_offset, layout.index_size).await?
            } else {
                Vec::new()
            };

            let key_strings = if key_data_used > 0 {
                read_exact(io, layout.key_strings_offset, key_data_used as usize).await?
            } else {
                Vec::new()
            };

            let info = ContainerInfo {
                resolved_offset: offset,
                tag,
                subtree_size,
                child_count,
                appended_bytes,
                children_area_offset: layout.children_area_offset,
                child_index,
                reserved_count,
                key_data_used,
                key_data_reserved,
                key_strings,
            };
            Ok(info)
        }
        TYPE_ARRAY => {
            let mut header_data = [0u8; ARRAY_HEADER_SIZE];
            read_exact_into(io, offset, &mut header_data).await?;
            let subtree_size = u64::from_le_bytes(header_data[1..9].try_into().unwrap());
            let child_count = u32::from_le_bytes(header_data[9..13].try_into().unwrap());
            let appended_bytes = u32::from_le_bytes(header_data[13..17].try_into().unwrap());

            let layout = array_layout(offset, child_count)?;

            // For large containers without a hint (root), cache structural region
            if subtree_size > CACHE_SUBTREE_THRESHOLD {
                let _ = io.cache_region(offset, layout.structural_size).await;
            }

            let child_index = if layout.index_size > 0 {
                read_exact(io, layout.index_offset, layout.index_size).await?
            } else {
                Vec::new()
            };

            let info = ContainerInfo {
                resolved_offset: offset,
                tag,
                subtree_size,
                child_count,
                appended_bytes,
                children_area_offset: layout.children_area_offset,
                child_index,
                reserved_count: 0,
                key_data_used: 0,
                key_data_reserved: 0,
                key_strings: Vec::new(),
            };
            Ok(info)
        }
        _ => Err(BlobError::NotAContainer(offset, tag)),
    }
}

impl ContainerInfo {
    /// Look up a key in this collection's child index and key strings.
    /// Returns `Some((index_entry_abs_pos, type_flags, child_abs_offset, child_size))` if found.
    /// v2 format: (key_hash:8, type_flags:1, offset:8, size:8) = 25 bytes per entry.
    pub fn find_in_collection(
        &self,
        key: &str,
        dict: &crate::dictionary::Dictionary,
    ) -> Result<Option<(u64, u8, u64, u64)>> {
        let key_hash = hash_field_name(key);

        let mut lo: u32 = 0;
        let mut hi: u32 = self.child_count;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let eo = mid as usize * COLLECTION_INDEX_ENTRY_SIZE;
            let h = u64::from_le_bytes(self.child_index[eo..eo + 8].try_into().unwrap());

            if h < key_hash {
                lo = mid + 1;
            } else if h > key_hash {
                hi = mid;
            } else {
                let type_flags = self.child_index[eo + 8];
                let offset =
                    u64::from_le_bytes(self.child_index[eo + 9..eo + 17].try_into().unwrap());
                let size =
                    u64::from_le_bytes(self.child_index[eo + 17..eo + 25].try_into().unwrap());
                let index_entry_abs_pos = self.resolved_offset
                    + COLLECTION_HEADER_SIZE as u64
                    + (mid as u64) * COLLECTION_INDEX_ENTRY_SIZE as u64;

                if Self::key_matches_at_index(&self.key_strings, mid as usize, key, dict)? {
                    let child_abs_offset = if is_forwarded_flag(type_flags) {
                        offset
                    } else {
                        self.children_area_offset + offset
                    };
                    return Ok(Some((
                        index_entry_abs_pos,
                        type_flags,
                        child_abs_offset,
                        size,
                    )));
                }

                // Hash collision — scan left and right
                let mut j = mid;
                while j > 0 {
                    j -= 1;
                    let eo2 = j as usize * COLLECTION_INDEX_ENTRY_SIZE;
                    let h2 = u64::from_le_bytes(self.child_index[eo2..eo2 + 8].try_into().unwrap());
                    if h2 != key_hash {
                        break;
                    }
                    if Self::key_matches_at_index(&self.key_strings, j as usize, key, dict)? {
                        let tf = self.child_index[eo2 + 8];
                        let off = u64::from_le_bytes(
                            self.child_index[eo2 + 9..eo2 + 17].try_into().unwrap(),
                        );
                        let sz = u64::from_le_bytes(
                            self.child_index[eo2 + 17..eo2 + 25].try_into().unwrap(),
                        );
                        let iep = self.resolved_offset
                            + COLLECTION_HEADER_SIZE as u64
                            + (j as u64) * COLLECTION_INDEX_ENTRY_SIZE as u64;
                        let cao = if is_forwarded_flag(tf) {
                            off
                        } else {
                            self.children_area_offset + off
                        };
                        return Ok(Some((iep, tf, cao, sz)));
                    }
                }
                let mut j = mid + 1;
                while j < self.child_count {
                    let eo2 = j as usize * COLLECTION_INDEX_ENTRY_SIZE;
                    let h2 = u64::from_le_bytes(self.child_index[eo2..eo2 + 8].try_into().unwrap());
                    if h2 != key_hash {
                        break;
                    }
                    if Self::key_matches_at_index(&self.key_strings, j as usize, key, dict)? {
                        let tf = self.child_index[eo2 + 8];
                        let off = u64::from_le_bytes(
                            self.child_index[eo2 + 9..eo2 + 17].try_into().unwrap(),
                        );
                        let sz = u64::from_le_bytes(
                            self.child_index[eo2 + 17..eo2 + 25].try_into().unwrap(),
                        );
                        let iep = self.resolved_offset
                            + COLLECTION_HEADER_SIZE as u64
                            + (j as u64) * COLLECTION_INDEX_ENTRY_SIZE as u64;
                        let cao = if is_forwarded_flag(tf) {
                            off
                        } else {
                            self.children_area_offset + off
                        };
                        return Ok(Some((iep, tf, cao, sz)));
                    }
                    j += 1;
                }

                return Ok(None);
            }
        }

        Ok(None)
    }

    /// Look up a child in this collection by key, returning type_flags and resolved offset.
    /// Returns `Some((type_flags, child_abs_offset, child_size))` if found.
    pub fn navigate_collection_with_flags(
        &self,
        key: &str,
        dict: &crate::dictionary::Dictionary,
    ) -> Result<Option<(u8, u64, u64)>> {
        let key_hash = hash_field_name(key);

        let mut lo: u32 = 0;
        let mut hi: u32 = self.child_count;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let eo = mid as usize * COLLECTION_INDEX_ENTRY_SIZE;
            let h = u64::from_le_bytes(self.child_index[eo..eo + 8].try_into().unwrap());

            if h < key_hash {
                lo = mid + 1;
            } else if h > key_hash {
                hi = mid;
            } else {
                let type_flags = self.child_index[eo + 8];
                let offset =
                    u64::from_le_bytes(self.child_index[eo + 9..eo + 17].try_into().unwrap());
                let size =
                    u64::from_le_bytes(self.child_index[eo + 17..eo + 25].try_into().unwrap());

                if Self::key_matches_at_index(&self.key_strings, mid as usize, key, dict)? {
                    let child_abs_offset = if is_forwarded_flag(type_flags) {
                        offset
                    } else {
                        self.children_area_offset + offset
                    };
                    return Ok(Some((type_flags, child_abs_offset, size)));
                }

                // Scan left
                let mut j = mid;
                while j > 0 {
                    j -= 1;
                    let eo2 = j as usize * COLLECTION_INDEX_ENTRY_SIZE;
                    let h2 = u64::from_le_bytes(self.child_index[eo2..eo2 + 8].try_into().unwrap());
                    if h2 != key_hash {
                        break;
                    }
                    if Self::key_matches_at_index(&self.key_strings, j as usize, key, dict)? {
                        let tf = self.child_index[eo2 + 8];
                        let off = u64::from_le_bytes(
                            self.child_index[eo2 + 9..eo2 + 17].try_into().unwrap(),
                        );
                        let sz = u64::from_le_bytes(
                            self.child_index[eo2 + 17..eo2 + 25].try_into().unwrap(),
                        );
                        let cao = if is_forwarded_flag(tf) {
                            off
                        } else {
                            self.children_area_offset + off
                        };
                        return Ok(Some((tf, cao, sz)));
                    }
                }
                // Scan right
                let mut j = mid + 1;
                while j < self.child_count {
                    let eo2 = j as usize * COLLECTION_INDEX_ENTRY_SIZE;
                    let h2 = u64::from_le_bytes(self.child_index[eo2..eo2 + 8].try_into().unwrap());
                    if h2 != key_hash {
                        break;
                    }
                    if Self::key_matches_at_index(&self.key_strings, j as usize, key, dict)? {
                        let tf = self.child_index[eo2 + 8];
                        let off = u64::from_le_bytes(
                            self.child_index[eo2 + 9..eo2 + 17].try_into().unwrap(),
                        );
                        let sz = u64::from_le_bytes(
                            self.child_index[eo2 + 17..eo2 + 25].try_into().unwrap(),
                        );
                        let cao = if is_forwarded_flag(tf) {
                            off
                        } else {
                            self.children_area_offset + off
                        };
                        return Ok(Some((tf, cao, sz)));
                    }
                    j += 1;
                }

                return Ok(None);
            }
        }

        Ok(None)
    }

    /// Look up a child in this collection by key (navigation only).
    /// Returns `Some((child_abs_offset, child_size))` if found.
    pub fn navigate_collection(
        &self,
        key: &str,
        dict: &crate::dictionary::Dictionary,
    ) -> Result<Option<(u64, u64)>> {
        let key_hash = hash_field_name(key);

        let mut lo: u32 = 0;
        let mut hi: u32 = self.child_count;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let eo = mid as usize * COLLECTION_INDEX_ENTRY_SIZE;
            let h = u64::from_le_bytes(self.child_index[eo..eo + 8].try_into().unwrap());

            if h < key_hash {
                lo = mid + 1;
            } else if h > key_hash {
                hi = mid;
            } else {
                let type_flags = self.child_index[eo + 8];
                let offset =
                    u64::from_le_bytes(self.child_index[eo + 9..eo + 17].try_into().unwrap());
                let size =
                    u64::from_le_bytes(self.child_index[eo + 17..eo + 25].try_into().unwrap());

                if Self::key_matches_at_index(&self.key_strings, mid as usize, key, dict)? {
                    let child_abs_offset = if is_forwarded_flag(type_flags) {
                        offset
                    } else {
                        self.children_area_offset + offset
                    };
                    return Ok(Some((child_abs_offset, size)));
                }

                // Scan left
                let mut j = mid;
                while j > 0 {
                    j -= 1;
                    let eo2 = j as usize * COLLECTION_INDEX_ENTRY_SIZE;
                    let h2 = u64::from_le_bytes(self.child_index[eo2..eo2 + 8].try_into().unwrap());
                    if h2 != key_hash {
                        break;
                    }
                    if Self::key_matches_at_index(&self.key_strings, j as usize, key, dict)? {
                        let tf = self.child_index[eo2 + 8];
                        let off = u64::from_le_bytes(
                            self.child_index[eo2 + 9..eo2 + 17].try_into().unwrap(),
                        );
                        let sz = u64::from_le_bytes(
                            self.child_index[eo2 + 17..eo2 + 25].try_into().unwrap(),
                        );
                        let cao = if is_forwarded_flag(tf) {
                            off
                        } else {
                            self.children_area_offset + off
                        };
                        return Ok(Some((cao, sz)));
                    }
                }
                // Scan right
                let mut j = mid + 1;
                while j < self.child_count {
                    let eo2 = j as usize * COLLECTION_INDEX_ENTRY_SIZE;
                    let h2 = u64::from_le_bytes(self.child_index[eo2..eo2 + 8].try_into().unwrap());
                    if h2 != key_hash {
                        break;
                    }
                    if Self::key_matches_at_index(&self.key_strings, j as usize, key, dict)? {
                        let tf = self.child_index[eo2 + 8];
                        let off = u64::from_le_bytes(
                            self.child_index[eo2 + 9..eo2 + 17].try_into().unwrap(),
                        );
                        let sz = u64::from_le_bytes(
                            self.child_index[eo2 + 17..eo2 + 25].try_into().unwrap(),
                        );
                        let cao = if is_forwarded_flag(tf) {
                            off
                        } else {
                            self.children_area_offset + off
                        };
                        return Ok(Some((cao, sz)));
                    }
                    j += 1;
                }

                return Ok(None);
            }
        }

        Ok(None)
    }

    /// Returns the byte size of the structural region (header + child index + reserved slots + key strings).
    /// This is the region from `resolved_offset` to `children_area_offset`.
    pub fn structural_size(&self) -> usize {
        (self.children_area_offset - self.resolved_offset) as usize
    }

    /// Serialize this ContainerInfo back to its on-disk binary format.
    /// Returns exactly `structural_size()` bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let size = self.structural_size();
        let mut buf = Vec::with_capacity(size);

        match self.tag {
            TYPE_COLLECTION => {
                buf.push(TYPE_COLLECTION);
                buf.extend_from_slice(&self.subtree_size.to_le_bytes());
                buf.extend_from_slice(&self.child_count.to_le_bytes());
                buf.extend_from_slice(&self.reserved_count.to_le_bytes());
                buf.extend_from_slice(&self.key_data_used.to_le_bytes());
                buf.extend_from_slice(&self.key_data_reserved.to_le_bytes());
                buf.extend_from_slice(&self.appended_bytes.to_le_bytes());
                // Child index (only active entries)
                buf.extend_from_slice(&self.child_index);
                // Zeroed reserved index slots
                let reserved_index_size =
                    self.reserved_count as usize * COLLECTION_INDEX_ENTRY_SIZE;
                buf.resize(buf.len() + reserved_index_size, 0);
                // Key string data (key_data_used bytes) padded to key_data_reserved
                buf.extend_from_slice(&self.key_strings);
                let padding = self.key_data_reserved as usize - self.key_data_used as usize;
                buf.resize(buf.len() + padding, 0);
            }
            TYPE_ARRAY => {
                buf.push(TYPE_ARRAY);
                buf.extend_from_slice(&self.subtree_size.to_le_bytes());
                buf.extend_from_slice(&self.child_count.to_le_bytes());
                buf.extend_from_slice(&self.appended_bytes.to_le_bytes());
                buf.extend_from_slice(&self.child_index);
            }
            _ => {}
        }

        debug_assert_eq!(buf.len(), size, "to_bytes size mismatch");
        buf
    }

    /// Insert a new child into this collection's index in-place.
    ///
    /// Returns `true` if the insert succeeded (reserved space available),
    /// `false` if there isn't enough reserved space (caller should fall back
    /// to structural copy).
    ///
    /// The child is stored with `is_forwarded=true` since its data is at an
    /// absolute offset (appended at EOF).
    pub fn insert_child(
        &mut self,
        key: &str,
        key_hash: u64,
        type_flags: u8,
        abs_offset: u64,
        size: u64,
        dict: &crate::dictionary::Dictionary,
    ) -> bool {
        let is_dict_ref = dict.lookup(key).is_some();
        let new_key_entry_size = if is_dict_ref {
            2u32
        } else {
            2 + key.len() as u32
        };
        if self.reserved_count == 0
            || self.key_data_used + new_key_entry_size > self.key_data_reserved
        {
            return false;
        }

        let child_count = self.child_count as usize;

        // Find insertion point via binary search (sorted by hash)
        let insert_pos = {
            let mut lo = 0usize;
            let mut hi = child_count;
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                let eo = mid * COLLECTION_INDEX_ENTRY_SIZE;
                let h = u64::from_le_bytes(self.child_index[eo..eo + 8].try_into().unwrap());
                if h < key_hash {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            lo
        };

        // Build new 25-byte index entry
        let mut entry = [0u8; COLLECTION_INDEX_ENTRY_SIZE];
        entry[0..8].copy_from_slice(&key_hash.to_le_bytes());
        entry[8] = type_flags;
        entry[9..17].copy_from_slice(&abs_offset.to_le_bytes());
        entry[17..25].copy_from_slice(&size.to_le_bytes());

        // Insert into child_index in-place (resize + shift + write)
        let idx_insert = insert_pos * COLLECTION_INDEX_ENTRY_SIZE;
        let idx_old_len = self.child_index.len();
        self.child_index
            .resize(idx_old_len + COLLECTION_INDEX_ENTRY_SIZE, 0);
        self.child_index.copy_within(
            idx_insert..idx_old_len,
            idx_insert + COLLECTION_INDEX_ENTRY_SIZE,
        );
        self.child_index[idx_insert..idx_insert + COLLECTION_INDEX_ENTRY_SIZE]
            .copy_from_slice(&entry);

        // Find byte offset for key insertion at insert_pos
        let key_insert_pos = {
            let mut pos = 0usize;
            for _ in 0..insert_pos {
                if pos + 2 <= self.key_strings.len() {
                    let raw =
                        u16::from_le_bytes(self.key_strings[pos..pos + 2].try_into().unwrap());
                    if raw & KEY_DICT_FLAG != 0 {
                        pos += 2; // dict-ref
                    } else {
                        pos += 2 + raw as usize; // inline
                    }
                }
            }
            pos
        };

        // Insert key string in-place (resize + shift + write)
        let new_key_total = new_key_entry_size as usize;
        let ks_old_len = self.key_strings.len();
        self.key_strings.resize(ks_old_len + new_key_total, 0);
        self.key_strings
            .copy_within(key_insert_pos..ks_old_len, key_insert_pos + new_key_total);
        if is_dict_ref {
            let field_id = dict.lookup(key).unwrap();
            self.key_strings[key_insert_pos..key_insert_pos + 2]
                .copy_from_slice(&(KEY_DICT_FLAG | field_id as u16).to_le_bytes());
        } else {
            let kb = key.as_bytes();
            self.key_strings[key_insert_pos..key_insert_pos + 2]
                .copy_from_slice(&(kb.len() as u16).to_le_bytes());
            self.key_strings[key_insert_pos + 2..key_insert_pos + new_key_total]
                .copy_from_slice(kb);
        }

        self.child_count += 1;
        self.reserved_count -= 1;
        self.key_data_used += new_key_entry_size;

        true
    }

    /// Skip to the byte position of the Nth key entry in key_strings.
    fn key_pos_at_index(key_strings: &[u8], index: usize) -> Result<usize> {
        let mut pos = 0usize;
        for _ in 0..index {
            if pos + 2 > key_strings.len() {
                return Err(BlobError::UnexpectedEof);
            }
            let raw = u16::from_le_bytes(key_strings[pos..pos + 2].try_into().unwrap());
            if raw & KEY_DICT_FLAG != 0 {
                pos += 2;
            } else {
                pos += 2 + raw as usize;
            }
        }
        Ok(pos)
    }

    /// Check if the key at a given index matches `target` WITHOUT resolving
    /// dict-ref keys to names. For dict-ref entries, compares field_ids
    /// (via dict.lookup on the target). For inline entries, compares strings.
    ///
    /// This mirrors how TYPE_OBJECT navigation worked: name→field_id direction
    /// only, so a stale reader dictionary can still match keys it already knows.
    fn key_matches_at_index(
        key_strings: &[u8],
        index: usize,
        target: &str,
        dict: &crate::dictionary::Dictionary,
    ) -> Result<bool> {
        let pos = Self::key_pos_at_index(key_strings, index)?;
        if pos + 2 > key_strings.len() {
            return Err(BlobError::UnexpectedEof);
        }
        let raw = u16::from_le_bytes(key_strings[pos..pos + 2].try_into().unwrap());
        if raw & KEY_DICT_FLAG != 0 {
            let stored_field_id = (raw & KEY_DICT_MASK) as u32;
            // Compare field_ids: lookup target in our (possibly stale) dictionary.
            // If target isn't in our dict, it can't match a dict-ref entry.
            Ok(dict.lookup(target) == Some(stored_field_id))
        } else {
            let key_len = raw as usize;
            let start = pos + 2;
            if start + key_len > key_strings.len() {
                return Err(BlobError::UnexpectedEof);
            }
            let stored = std::str::from_utf8(&key_strings[start..start + key_len])
                .map_err(|_| BlobError::UnexpectedEof)?;
            Ok(stored == target)
        }
    }

    /// Read the key string at a given index from key_strings bytes.
    /// Resolves dict-ref keys to names via the dictionary.
    ///
    /// Use this only when you need the actual key name (read_subtree, read_shallow).
    /// For navigation/matching, use key_matches_at_index instead.
    pub fn read_key_from_strings(
        key_strings: &[u8],
        index: usize,
        dict: &crate::dictionary::Dictionary,
    ) -> Result<String> {
        let pos = Self::key_pos_at_index(key_strings, index)?;
        if pos + 2 > key_strings.len() {
            return Err(BlobError::UnexpectedEof);
        }
        let raw = u16::from_le_bytes(key_strings[pos..pos + 2].try_into().unwrap());
        if raw & KEY_DICT_FLAG != 0 {
            let field_id = (raw & KEY_DICT_MASK) as u32;
            Ok(dict.get_name(field_id)?.to_string())
        } else {
            let key_len = raw as usize;
            let start = pos + 2;
            if start + key_len > key_strings.len() {
                return Err(BlobError::UnexpectedEof);
            }
            let s = std::str::from_utf8(&key_strings[start..start + key_len])
                .map_err(|_| BlobError::UnexpectedEof)?;
            Ok(s.to_string())
        }
    }
}

/// In-memory cache of path -> blob location mappings.
///
/// Populated lazily as paths are navigated. Invalidated entirely on
/// full re-compaction (all offsets change). On incremental compaction,
/// forwarded entries can be updated in-place.
pub struct NavigationCache {
    entries: HashMap<String, BlobLocation>,
}

impl NavigationCache {
    pub fn new() -> Self {
        NavigationCache {
            entries: HashMap::new(),
        }
    }

    /// Look up a cached location for a path.
    pub fn get(&self, path: &str) -> Option<&BlobLocation> {
        self.entries.get(path)
    }

    /// Cache a navigation result.
    pub fn insert(&mut self, path: String, location: BlobLocation) {
        self.entries.insert(path, location);
    }

    /// Remove a specific path from the cache.
    pub fn remove(&mut self, path: &str) {
        self.entries.remove(path);
    }

    /// Invalidate the entire cache (e.g., after full re-compaction).
    pub fn invalidate_all(&mut self) {
        self.entries.clear();
    }

    /// Invalidate all entries that start with a given prefix.
    /// Used when a subtree is updated via incremental compaction.
    pub fn invalidate_prefix(&mut self, prefix: &str) {
        self.entries.retain(|k, _| !k.starts_with(prefix));
    }

    /// Number of cached entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for NavigationCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert a path slice to a cache key string.
pub fn path_to_key(path: &[&str]) -> String {
    path.join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: a container's reserved capacity makes `children_area_offset`
    /// a *logical* offset that can sit past the physical blob end. The layout
    /// helpers must accept this — they only reject genuine u64 overflow, never a
    /// large-but-valid offset. An earlier over-strict blob-length bound here
    /// NACK'd valid reads in chaos-monkey ("container region ... exceeds blob
    /// length"). Out-of-range reads are caught at `pread` (EOF), not here.
    #[test]
    fn test_layout_allows_large_logical_offsets() {
        // Small live child_count, but large reserved key space pushes
        // children_area_offset far past any plausible physical blob end.
        let layout = collection_layout(100, 2, 0, 10_000)
            .expect("a large logical offset must be accepted, not rejected");
        assert!(layout.children_area_offset > 10_000);
        assert_eq!(
            layout.structural_size as u64,
            layout.children_area_offset - 100
        );

        // Genuine u64 overflow in the offset math is still rejected as corrupt.
        assert!(collection_layout(u64::MAX - 8, 1, 0, 0).is_err());
        assert!(array_layout(100, u32::MAX).is_ok()); // large but no overflow
        assert!(array_layout(u64::MAX, u32::MAX).is_err()); // offset+index overflows
    }

    fn make_location(offset: u64) -> BlobLocation {
        BlobLocation {
            offset,
            subtree_size: 100,
            node_type: 0x01,
        }
    }

    #[test]
    fn test_basic_operations() {
        let mut cache = NavigationCache::new();
        assert!(cache.is_empty());

        cache.insert("characters/abc/hp".to_string(), make_location(1000));
        cache.insert("characters/abc/name".to_string(), make_location(2000));
        cache.insert("config/mode".to_string(), make_location(3000));

        assert_eq!(cache.len(), 3);
        assert_eq!(cache.get("characters/abc/hp").unwrap().offset, 1000);
        assert_eq!(cache.get("config/mode").unwrap().offset, 3000);
        assert!(cache.get("nonexistent").is_none());
    }

    #[test]
    fn test_invalidate_all() {
        let mut cache = NavigationCache::new();
        cache.insert("a".to_string(), make_location(1));
        cache.insert("b".to_string(), make_location(2));

        cache.invalidate_all();
        assert!(cache.is_empty());
    }

    #[test]
    fn test_invalidate_prefix() {
        let mut cache = NavigationCache::new();
        cache.insert("characters/abc/hp".to_string(), make_location(1));
        cache.insert("characters/abc/name".to_string(), make_location(2));
        cache.insert("characters/def/hp".to_string(), make_location(3));
        cache.insert("config/mode".to_string(), make_location(4));

        // Invalidate everything under characters/abc
        cache.invalidate_prefix("characters/abc");
        assert_eq!(cache.len(), 2);
        assert!(cache.get("characters/abc/hp").is_none());
        assert!(cache.get("characters/abc/name").is_none());
        assert!(cache.get("characters/def/hp").is_some());
        assert!(cache.get("config/mode").is_some());
    }

    #[test]
    fn test_remove() {
        let mut cache = NavigationCache::new();
        cache.insert("a".to_string(), make_location(1));
        cache.insert("b".to_string(), make_location(2));

        cache.remove("a");
        assert_eq!(cache.len(), 1);
        assert!(cache.get("a").is_none());
        assert!(cache.get("b").is_some());
    }

    #[test]
    fn test_path_to_key() {
        assert_eq!(
            path_to_key(&["characters", "abc", "hp"]),
            "characters/abc/hp"
        );
        assert_eq!(path_to_key(&[]), "");
        assert_eq!(path_to_key(&["root"]), "root");
    }
}
