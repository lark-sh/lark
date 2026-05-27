//! Session-aware reader: bulk subtree reading.
//!
//! Contains standalone header/dictionary readers (used before a session exists),
//! the path navigator (`navigate_raw`), and the bulk subtree reader as a method
//! on `BlobSession`.

use crate::arc_value::ArcValue;
use crate::dictionary::Dictionary;
use crate::error::{BlobError, Result};
use crate::format::*;
use crate::io::{BlobIO, read_exact, read_exact_into};
use crate::nav_cache::read_container;
use crate::session::{BlobSession, ShallowChild, ShallowValue};
use serde_json::Number;
use std::collections::HashMap;
use std::sync::Arc;

// ── Standalone functions (no session needed) ─────────────────────────

/// Location of a node within the blob.
#[derive(Debug, Clone)]
pub struct BlobLocation {
    pub offset: u64,
    pub subtree_size: u64,
    pub node_type: u8,
}

/// Read and parse the blob header.
pub async fn read_header<IO: BlobIO>(io: &IO) -> Result<BlobHeader> {
    let mut buf = [0u8; HEADER_SIZE];
    read_exact_into(io, 0, &mut buf).await?;
    BlobHeader::from_bytes(&buf)
}

/// Read and parse the dictionary from the blob.
pub async fn read_dictionary<IO: BlobIO>(io: &IO, header: &BlobHeader) -> Result<Dictionary> {
    let dict_header = read_exact(io, header.dict_offset, 20).await?;
    let max_field_count = u32::from_le_bytes(dict_header[8..12].try_into().unwrap()) as usize;
    let max_name_data = u32::from_le_bytes(dict_header[16..20].try_into().unwrap()) as usize;
    let dict_size = 20 + max_field_count * 16 + max_name_data;
    let data = read_exact(io, header.dict_offset, dict_size).await?;
    let (dict, _) = Dictionary::from_bytes(&data)?;
    Ok(dict)
}

/// Navigate the blob to a path, returning the location of the target node.
pub async fn navigate_raw<IO: BlobIO>(
    io: &IO,
    header: &BlobHeader,
    dict: &Dictionary,
    path: &[&str],
) -> Result<BlobLocation> {
    let _field_id_size = header.field_id_size()?;
    let mut current_offset = header.root_offset;
    let mut current_size_hint: Option<u64> = None;

    for (i, &segment) in path.iter().enumerate() {
        let container = match read_container(io, current_offset, current_size_hint).await {
            Ok(c) => c,
            Err(BlobError::NotAContainer(_, _)) => {
                let remaining = path[i..].join("/");
                return Err(BlobError::PathNotFound(remaining));
            }
            Err(e) => return Err(e),
        };

        // The two `None` branches below are distinct cases (tombstone vs.
        // non-traversable intermediate segment); kept separate for clarity.
        #[allow(clippy::if_same_then_else)]
        let next: Option<(u64, u64)> = match container.tag {
            TYPE_COLLECTION => match container.navigate_collection_with_flags(segment, dict)? {
                Some((type_flags, abs_offset, size)) => {
                    let child_type = extract_type_tag(type_flags);
                    let is_last_segment = i == path.len() - 1;
                    if child_type == TYPE_NULL && size == 0 {
                        None
                    } else if !is_last_segment
                        && child_type != TYPE_COLLECTION
                        && child_type != TYPE_ARRAY
                    {
                        None
                    } else {
                        Some((abs_offset, size))
                    }
                }
                None => None,
            },
            _ => {
                let remaining_path = path[i..].join("/");
                return Err(BlobError::PathNotFound(remaining_path));
            }
        };

        match next {
            Some((offset, size)) => {
                current_offset = offset;
                current_size_hint = Some(size);
            }
            None => {
                return Err(BlobError::PathNotFound(segment.to_string()));
            }
        }
    }

    // For the final node, try read_container (it might be a container)
    match read_container(io, current_offset, current_size_hint).await {
        Ok(container) => {
            return Ok(BlobLocation {
                offset: container.resolved_offset,
                subtree_size: container.subtree_size,
                node_type: container.tag,
            });
        }
        Err(BlobError::NotAContainer(_, _)) => {
            // Not a container — read tag + size manually
        }
        Err(e) => return Err(e),
    }

    let mut tag_buf = [0u8; 1];
    read_exact_into(io, current_offset, &mut tag_buf).await?;
    let tag = tag_buf[0];

    let subtree_size = match tag {
        TYPE_ARRAY | TYPE_COLLECTION => {
            let mut ss_buf = [0u8; 8];
            read_exact_into(io, current_offset + 1, &mut ss_buf).await?;
            u64::from_le_bytes(ss_buf)
        }
        TYPE_STRING => {
            let mut len_buf = [0u8; 4];
            read_exact_into(io, current_offset + 1, &mut len_buf).await?;
            let str_len = u32::from_le_bytes(len_buf);
            1 + 4 + str_len as u64
        }
        TYPE_NUMBER => 9,
        TYPE_BOOL => 2,
        TYPE_NULL => 1,
        _ => return Err(BlobError::UnknownNodeType(tag)),
    };

    Ok(BlobLocation {
        offset: current_offset,
        subtree_size,
        node_type: tag,
    })
}

// ── Private helpers for the bulk reader ──────────────────────────────

/// Buffer pool entry.
struct BufEntry {
    base: u64,
    data: Vec<u8>,
}

/// Info about a child node carried from the parent's index entry.
#[derive(Clone, Copy)]
struct ChildInfo {
    type_tag: u8,
    is_forwarded: bool,
    abs_offset: u64,
    size: u64,
}

/// Compute children_area offset using key_data_reserved from the header.
fn compute_collection_children_area(key_strings_offset: u64, key_data_reserved: u32) -> u64 {
    key_strings_offset + key_data_reserved as u64
}

/// Parse collection key strings from a contiguous byte slice (no I/O).
fn parse_collection_keys(
    key_data: &[u8],
    child_count: usize,
    dict: &Dictionary,
) -> Result<Vec<String>> {
    let mut keys = Vec::with_capacity(child_count);
    let mut pos = 0;
    for _ in 0..child_count {
        if pos + 2 > key_data.len() {
            return Err(BlobError::UnexpectedEof);
        }
        let raw = u16::from_le_bytes(
            key_data[pos..pos + 2]
                .try_into()
                .map_err(|_| BlobError::UnexpectedEof)?,
        );
        if raw & KEY_DICT_FLAG != 0 {
            let field_id = (raw & KEY_DICT_MASK) as u32;
            keys.push(dict.get_name(field_id)?.to_string());
            pos += 2;
        } else {
            let key_len = raw as usize;
            pos += 2;
            if pos + key_len > key_data.len() {
                return Err(BlobError::UnexpectedEof);
            }
            let key = std::str::from_utf8(&key_data[pos..pos + key_len])
                .map_err(|_| BlobError::UnexpectedEof)?;
            keys.push(key.to_string());
            pos += key_len;
        }
    }
    Ok(keys)
}

/// Parse a scalar (leaf) ArcValue directly from a buffer slice.
fn parse_leaf(buf: &[u8], offset: usize, type_tag: u8) -> Result<ArcValue> {
    match type_tag {
        TYPE_STRING => {
            let len_start = offset + 1;
            if len_start + 4 > buf.len() {
                return Err(BlobError::UnexpectedEof);
            }
            let str_len =
                u32::from_le_bytes(buf[len_start..len_start + 4].try_into().unwrap()) as usize;
            let str_start = len_start + 4;
            if str_start + str_len > buf.len() {
                return Err(BlobError::UnexpectedEof);
            }
            let s = std::str::from_utf8(&buf[str_start..str_start + str_len])
                .map_err(|_| BlobError::UnexpectedEof)?;
            Ok(ArcValue::String(Arc::from(s)))
        }
        TYPE_NUMBER => {
            let start = offset + 1;
            if start + 8 > buf.len() {
                return Err(BlobError::UnexpectedEof);
            }
            let f = f64::from_le_bytes(buf[start..start + 8].try_into().unwrap());
            if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                Ok(ArcValue::Number(Number::from(f as i64)))
            } else {
                Ok(ArcValue::Number(
                    Number::from_f64(f).ok_or(BlobError::UnexpectedEof)?,
                ))
            }
        }
        TYPE_BOOL => {
            let start = offset + 1;
            if start >= buf.len() {
                return Err(BlobError::UnexpectedEof);
            }
            Ok(ArcValue::Bool(buf[start] != 0))
        }
        TYPE_NULL => Ok(ArcValue::Null),
        _ => Err(BlobError::UnknownNodeType(type_tag)),
    }
}

/// Slice into a buffer by index.
#[inline]
fn buf_slice(buffers: &[BufEntry], buf_idx: usize, abs_offset: u64, len: usize) -> Result<&[u8]> {
    let entry = &buffers[buf_idx];
    let start = local_offset(entry, abs_offset)?;
    let end = start.checked_add(len).ok_or(BlobError::UnexpectedEof)?;
    if end > entry.data.len() {
        return Err(BlobError::UnexpectedEof);
    }
    Ok(&entry.data[start..end])
}

/// Translate an absolute blob offset into an index within `entry`, validating it
/// falls inside the buffer. Child/forwarded offsets are read from on-disk node
/// headers and may be corrupt, so the naive `(abs_offset - base) as usize` can
/// underflow (offset < base) or land past the buffer — both panic. This returns
/// `UnexpectedEof` instead. The returned index is `< entry.data.len()`, so it is
/// always valid to read at least one byte there.
fn local_offset(entry: &BufEntry, abs_offset: u64) -> Result<usize> {
    let local = abs_offset
        .checked_sub(entry.base)
        .filter(|&l| l < entry.data.len() as u64)
        .ok_or(BlobError::UnexpectedEof)?;
    Ok(local as usize)
}

// ── BlobSession methods ──────────────────────────────────────────────

impl<IO: BlobIO> BlobSession<IO> {
    /// Read a subtree at a path, deserializing to ArcValue.
    ///
    /// This is the "promotion" operation: the server reads a cold subtree
    /// from the blob into memory as an ArcValue tree.
    ///
    /// Pass an empty path to read the entire blob.
    pub async fn read_subtree(&self, path: &[&str]) -> Result<ArcValue> {
        if path.is_empty() {
            return self.read_subtree_bulk(self.header.root_offset, None).await;
        }

        let location = navigate_raw(&self.io, &self.header, &self.dict, path).await?;
        self.read_subtree_bulk(location.offset, Some(location.subtree_size))
            .await
    }

    /// Navigate to a path and return the node's location (offset + subtree_size).
    pub async fn navigate(&self, path: &[&str]) -> Result<BlobLocation> {
        navigate_raw(&self.io, &self.header, &self.dict, path).await
    }

    /// Read immediate child keys and their subtree sizes at a path.
    pub async fn read_keys(&self, path: &[&str]) -> Result<Vec<(String, u64)>> {
        if path.is_empty() {
            let container = read_container(&self.io, self.header.root_offset, None).await?;
            return self.extract_keys_from_container(&container);
        }

        let location = navigate_raw(&self.io, &self.header, &self.dict, path).await?;
        let container =
            read_container(&self.io, location.offset, Some(location.subtree_size)).await?;
        self.extract_keys_from_container(&container)
    }

    /// Extract keys from a parsed container.
    fn extract_keys_from_container(
        &self,
        container: &crate::nav_cache::ContainerInfo,
    ) -> Result<Vec<(String, u64)>> {
        if container.tag != TYPE_COLLECTION {
            return Err(BlobError::NotAContainer(
                container.resolved_offset,
                container.tag,
            ));
        }
        let mut keys = Vec::with_capacity(container.child_count as usize);
        let mut pos = 0usize;
        for i in 0..container.child_count as usize {
            let eo = i * COLLECTION_INDEX_ENTRY_SIZE;
            let size =
                u64::from_le_bytes(container.child_index[eo + 17..eo + 25].try_into().unwrap());

            if pos + 2 > container.key_strings.len() {
                return Err(BlobError::UnexpectedEof);
            }
            let raw = u16::from_le_bytes(container.key_strings[pos..pos + 2].try_into().unwrap());
            let key = if raw & KEY_DICT_FLAG != 0 {
                let field_id = (raw & KEY_DICT_MASK) as u32;
                pos += 2;
                self.dict.get_name(field_id)?.to_string()
            } else {
                let key_len = raw as usize;
                pos += 2;
                if pos + key_len > container.key_strings.len() {
                    return Err(BlobError::UnexpectedEof);
                }
                let k = std::str::from_utf8(&container.key_strings[pos..pos + key_len])
                    .map_err(|_| BlobError::UnexpectedEof)?;
                pos += key_len;
                k.to_string()
            };
            keys.push((key, size));
        }
        Ok(keys)
    }

    /// Shallow-read a path: returns actual values for primitives, and
    /// key + size metadata for container children.
    pub async fn read_shallow(&self, path: &[&str]) -> Result<ShallowValue> {
        if path.is_empty() {
            return self
                .read_shallow_at_offset(&self.io, self.header.root_offset, None)
                .await;
        }

        let location = navigate_raw(&self.io, &self.header, &self.dict, path).await?;
        self.read_shallow_at_offset(&self.io, location.offset, Some(location.subtree_size))
            .await
    }

    /// Shallow-read at a resolved offset.
    async fn read_shallow_at_offset(
        &self,
        io: &IO,
        offset: u64,
        size_hint: Option<u64>,
    ) -> Result<ShallowValue> {
        let container = match read_container(io, offset, size_hint).await {
            Ok(c) => c,
            Err(BlobError::NotAContainer(_, _)) => {
                let value = Self::read_primitive_from(io, offset).await?;
                return Ok(ShallowValue::Primitive(value));
            }
            Err(e) => return Err(e),
        };

        if container.tag != TYPE_COLLECTION {
            return Err(BlobError::NotAContainer(offset, container.tag));
        }
        let mut children = Vec::with_capacity(container.child_count as usize);
        let mut key_pos = 0usize;
        for i in 0..container.child_count as usize {
            let eo = i * COLLECTION_INDEX_ENTRY_SIZE;
            let type_flags = container.child_index[eo + 8];
            let child_offset =
                u64::from_le_bytes(container.child_index[eo + 9..eo + 17].try_into().unwrap());
            let size =
                u64::from_le_bytes(container.child_index[eo + 17..eo + 25].try_into().unwrap());

            let abs_offset = if is_forwarded_flag(type_flags) {
                child_offset
            } else {
                container.children_area_offset + child_offset
            };
            let tag = extract_type_tag(type_flags);

            if key_pos + 2 > container.key_strings.len() {
                return Err(BlobError::UnexpectedEof);
            }
            let raw = u16::from_le_bytes(
                container.key_strings[key_pos..key_pos + 2]
                    .try_into()
                    .unwrap(),
            );
            let key = if raw & KEY_DICT_FLAG != 0 {
                let field_id = (raw & KEY_DICT_MASK) as u32;
                key_pos += 2;
                self.dict.get_name(field_id)?.to_string()
            } else {
                let key_len = raw as usize;
                key_pos += 2;
                if key_pos + key_len > container.key_strings.len() {
                    return Err(BlobError::UnexpectedEof);
                }
                let k = std::str::from_utf8(&container.key_strings[key_pos..key_pos + key_len])
                    .map_err(|_| BlobError::UnexpectedEof)?
                    .to_string();
                key_pos += key_len;
                k
            };

            let value = if Self::is_primitive_tag(tag) {
                Some(Self::read_primitive_from(io, abs_offset).await?)
            } else {
                None
            };

            children.push(ShallowChild { key, size, value });
        }
        Ok(ShallowValue::Children(children))
    }

    /// Whether a type tag is a primitive (not a container).
    fn is_primitive_tag(tag: u8) -> bool {
        matches!(tag, TYPE_STRING | TYPE_NUMBER | TYPE_BOOL | TYPE_NULL)
    }

    /// Read a primitive value at the given offset from the specified IO.
    async fn read_primitive_from(io: &IO, offset: u64) -> Result<ArcValue> {
        let mut tag_buf = [0u8; 1];
        read_exact_into(io, offset, &mut tag_buf).await?;
        match tag_buf[0] {
            TYPE_STRING => {
                let len_data = read_exact(io, offset + 1, 4).await?;
                let str_len = u32::from_le_bytes(len_data.try_into().unwrap()) as usize;
                let str_data = read_exact(io, offset + 5, str_len).await?;
                let s = std::str::from_utf8(&str_data).map_err(|_| BlobError::UnexpectedEof)?;
                Ok(ArcValue::String(Arc::from(s)))
            }
            TYPE_NUMBER => {
                let mut buf = [0u8; 8];
                read_exact_into(io, offset + 1, &mut buf).await?;
                let f = f64::from_le_bytes(buf);
                if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                    Ok(ArcValue::Number(Number::from(f as i64)))
                } else {
                    Ok(ArcValue::Number(
                        Number::from_f64(f).ok_or(BlobError::UnexpectedEof)?,
                    ))
                }
            }
            TYPE_BOOL => {
                let mut buf = [0u8; 1];
                read_exact_into(io, offset + 1, &mut buf).await?;
                Ok(ArcValue::Bool(buf[0] != 0))
            }
            TYPE_NULL => Ok(ArcValue::Null),
            other => Err(BlobError::UnknownNodeType(other)),
        }
    }

    // ── Bulk subtree reader ──────────────────────────────────────────

    /// Iterative stack-based bulk reader. Loads subtrees into a buffer pool
    /// for zero-copy parsing. Forwarded children are loaded from IO.
    ///
    /// `offset`: starting offset within the blob.
    /// `subtree_size`: if known, skip the 9-byte probe.
    async fn read_subtree_bulk(&self, offset: u64, subtree_size: Option<u64>) -> Result<ArcValue> {
        let _field_id_size = self.header.field_id_size()?;

        let mut buffers: Vec<BufEntry> = Vec::new();

        // Load the root buffer
        let root_tag: u8;

        if let Some(ss) = subtree_size {
            if ss > 0 {
                let data = read_exact(&self.io, offset, ss as usize).await?;
                root_tag = data[0];
                buffers.push(BufEntry { base: offset, data });
            } else {
                return Err(BlobError::UnexpectedEof);
            }
        } else {
            // Probe: read 9 bytes for tag + subtree_size
            let probe = read_exact(&self.io, offset, 9).await?;
            root_tag = probe[0];
            if matches!(root_tag, TYPE_ARRAY | TYPE_COLLECTION) {
                let ss = u64::from_le_bytes(probe[1..9].try_into().unwrap());
                if ss > 9 {
                    let mut full = probe;
                    let remaining = read_exact(&self.io, offset + 9, ss as usize - 9).await?;
                    full.extend_from_slice(&remaining);
                    buffers.push(BufEntry {
                        base: offset,
                        data: full,
                    });
                } else if ss > 0 {
                    buffers.push(BufEntry {
                        base: offset,
                        data: probe[..ss as usize].to_vec(),
                    });
                } else {
                    return Err(BlobError::UnexpectedEof);
                }
            } else {
                // Root is a scalar
                let scalar_size = match root_tag {
                    TYPE_STRING => {
                        let str_len = u32::from_le_bytes(probe[1..5].try_into().unwrap());
                        1 + 4 + str_len as usize
                    }
                    TYPE_NUMBER => 9,
                    TYPE_BOOL => 2,
                    TYPE_NULL => 1,
                    _ => return Err(BlobError::UnknownNodeType(root_tag)),
                };
                if scalar_size <= probe.len() {
                    buffers.push(BufEntry {
                        base: offset,
                        data: probe[..scalar_size].to_vec(),
                    });
                } else {
                    let data = read_exact(&self.io, offset, scalar_size).await?;
                    buffers.push(BufEntry { base: offset, data });
                }
            }
        }

        // For scalar roots, parse directly
        if !matches!(root_tag, TYPE_ARRAY | TYPE_COLLECTION) {
            return parse_leaf(&buffers[0].data, 0, root_tag);
        }

        // Stack frames for iterative tree traversal
        enum ReadFrame {
            Collection {
                children: Vec<(String, ChildInfo)>,
                next_child: usize,
                map: HashMap<String, ArcValue>,
                buf_idx: usize,
            },
            Array {
                children: Vec<ChildInfo>,
                next_elem: usize,
                arr: Vec<ArcValue>,
                buf_idx: usize,
            },
        }

        let mut stack: Vec<ReadFrame> = Vec::new();
        let mut nodes_since_yield: u32 = 0;
        let mut cur_buf_idx: usize = 0;
        let mut cur_offset = offset;

        'descend: loop {
            nodes_since_yield += 1;
            if nodes_since_yield >= 1024 {
                self.io.yield_point().await;
                nodes_since_yield = 0;
            }

            let entry = &buffers[cur_buf_idx];
            let tag = entry.data[local_offset(entry, cur_offset)?];

            let completed: ArcValue = match tag {
                TYPE_COLLECTION => {
                    let hdr = buf_slice(&buffers, cur_buf_idx, cur_offset, COLLECTION_HEADER_SIZE)?;
                    let child_count = u32::from_le_bytes(hdr[9..13].try_into().unwrap()) as usize;
                    let reserved_count = u32::from_le_bytes(hdr[13..17].try_into().unwrap());
                    let key_data_used = u32::from_le_bytes(hdr[17..21].try_into().unwrap());
                    let key_data_reserved = u32::from_le_bytes(hdr[21..25].try_into().unwrap());
                    // u64 sum: child_count and reserved_count are independent
                    // attacker-controlled u32s, so a u32 addition here can
                    // overflow (panic under overflow-checks). buf_slice below
                    // rejects values that exceed the buffer.
                    let total_slots = child_count as u64 + reserved_count as u64;

                    if child_count == 0 {
                        ArcValue::Object(Arc::new(HashMap::new()))
                    } else {
                        let child_index_offset = cur_offset + COLLECTION_HEADER_SIZE as u64;
                        let idx_len = child_count * COLLECTION_INDEX_ENTRY_SIZE;
                        let child_index_data =
                            buf_slice(&buffers, cur_buf_idx, child_index_offset, idx_len)?;
                        let key_strings_offset =
                            child_index_offset + total_slots * COLLECTION_INDEX_ENTRY_SIZE as u64;

                        let key_data = buf_slice(
                            &buffers,
                            cur_buf_idx,
                            key_strings_offset,
                            key_data_used as usize,
                        )?;
                        let keys = parse_collection_keys(key_data, child_count, &self.dict)?;

                        let children_area_offset =
                            compute_collection_children_area(key_strings_offset, key_data_reserved);

                        let mut children = Vec::with_capacity(child_count);
                        // `i` indexes both `keys` and `child_index_data` (via `eo`).
                        #[allow(clippy::needless_range_loop)]
                        for i in 0..child_count {
                            let eo = i * COLLECTION_INDEX_ENTRY_SIZE;
                            let type_flags = child_index_data[eo + 8];
                            let child_offset = u64::from_le_bytes(
                                child_index_data[eo + 9..eo + 17].try_into().unwrap(),
                            );
                            let size = u64::from_le_bytes(
                                child_index_data[eo + 17..eo + 25].try_into().unwrap(),
                            );
                            if extract_type_tag(type_flags) == TYPE_NULL && size == 0 {
                                continue;
                            }
                            let forwarded = is_forwarded_flag(type_flags);
                            let abs_offset = if forwarded {
                                child_offset
                            } else {
                                children_area_offset + child_offset
                            };
                            children.push((
                                keys[i].clone(),
                                ChildInfo {
                                    type_tag: extract_type_tag(type_flags),
                                    is_forwarded: forwarded,
                                    abs_offset,
                                    size,
                                },
                            ));
                        }

                        if children.is_empty() {
                            ArcValue::Object(Arc::new(HashMap::new()))
                        } else {
                            let first_ci = children[0].1;
                            let parent_bi = cur_buf_idx;

                            stack.push(ReadFrame::Collection {
                                children,
                                next_child: 1,
                                map: HashMap::new(),
                                buf_idx: cur_buf_idx,
                            });

                            match self
                                .resolve_child(&mut buffers, &first_ci, parent_bi)
                                .await?
                            {
                                Some(val) => val,
                                None => {
                                    if first_ci.is_forwarded {
                                        cur_buf_idx = buffers.len() - 1;
                                        cur_offset = buffers.last().unwrap().base;
                                    } else {
                                        cur_buf_idx = parent_bi;
                                        cur_offset = first_ci.abs_offset;
                                    }
                                    continue 'descend;
                                }
                            }
                        }
                    }
                }

                // TYPE_ARRAY is never written anymore — collections (including
                // arrays) are stored as TYPE_COLLECTION. This decode is retained
                // to migrate pre-existing on-disk arrays into integer-keyed
                // objects on read; such nodes self-heal to TYPE_COLLECTION on the
                // next compaction.
                TYPE_ARRAY => {
                    let hdr = buf_slice(&buffers, cur_buf_idx, cur_offset, ARRAY_HEADER_SIZE)?;
                    let elem_count = u32::from_le_bytes(hdr[9..13].try_into().unwrap()) as usize;

                    if elem_count == 0 {
                        ArcValue::Object(Arc::new(HashMap::new()))
                    } else {
                        let elem_index_start = cur_offset + ARRAY_HEADER_SIZE as u64;
                        let idx_len = elem_count * ARRAY_INDEX_ENTRY_SIZE;
                        let elem_index_data =
                            buf_slice(&buffers, cur_buf_idx, elem_index_start, idx_len)?;
                        let elements_area_start = elem_index_start + idx_len as u64;

                        let mut children = Vec::with_capacity(elem_count);
                        for i in 0..elem_count {
                            let eo = i * ARRAY_INDEX_ENTRY_SIZE;
                            let type_flags = elem_index_data[eo];
                            let elem_offset = u64::from_le_bytes(
                                elem_index_data[eo + 1..eo + 9].try_into().unwrap(),
                            );
                            let size = u64::from_le_bytes(
                                elem_index_data[eo + 9..eo + 17].try_into().unwrap(),
                            );
                            let forwarded = is_forwarded_flag(type_flags);
                            let abs_offset = if forwarded {
                                elem_offset
                            } else {
                                elements_area_start + elem_offset
                            };
                            children.push(ChildInfo {
                                type_tag: extract_type_tag(type_flags),
                                is_forwarded: forwarded,
                                abs_offset,
                                size,
                            });
                        }

                        let first_ci = children[0];
                        let parent_bi = cur_buf_idx;

                        stack.push(ReadFrame::Array {
                            children,
                            next_elem: 1,
                            arr: Vec::with_capacity(elem_count),
                            buf_idx: cur_buf_idx,
                        });

                        match self
                            .resolve_child(&mut buffers, &first_ci, parent_bi)
                            .await?
                        {
                            Some(val) => val,
                            None => {
                                if first_ci.is_forwarded {
                                    cur_buf_idx = buffers.len() - 1;
                                    cur_offset = buffers.last().unwrap().base;
                                } else {
                                    cur_buf_idx = parent_bi;
                                    cur_offset = first_ci.abs_offset;
                                }
                                continue 'descend;
                            }
                        }
                    }
                }

                // Scalar at descend level
                _ => {
                    let local_off = local_offset(&buffers[cur_buf_idx], cur_offset)?;
                    return parse_leaf(&buffers[cur_buf_idx].data, local_off, tag);
                }
            };

            // Propagate completed value up the stack
            let mut value = completed;
            loop {
                match stack.last_mut() {
                    None => return Ok(value),
                    Some(ReadFrame::Collection {
                        children,
                        next_child,
                        map,
                        buf_idx,
                    }) => {
                        let child_idx = *next_child - 1;
                        map.insert(children[child_idx].0.clone(), value);

                        if *next_child < children.len() {
                            let idx = *next_child;
                            let ci = children[idx].1;
                            let parent_bi = *buf_idx;
                            *next_child += 1;

                            match self.resolve_child(&mut buffers, &ci, parent_bi).await? {
                                Some(val) => {
                                    value = val;
                                    continue;
                                }
                                None => {
                                    if ci.is_forwarded {
                                        cur_buf_idx = buffers.len() - 1;
                                        cur_offset = buffers.last().unwrap().base;
                                    } else {
                                        cur_buf_idx = parent_bi;
                                        cur_offset = ci.abs_offset;
                                    }
                                    continue 'descend;
                                }
                            }
                        } else {
                            let final_map = std::mem::take(map);
                            value = ArcValue::Object(Arc::new(final_map));
                            stack.pop();
                        }
                    }
                    Some(ReadFrame::Array {
                        children,
                        next_elem,
                        arr,
                        buf_idx,
                    }) => {
                        arr.push(value);

                        if *next_elem < children.len() {
                            let idx = *next_elem;
                            let ci = children[idx];
                            let parent_bi = *buf_idx;
                            *next_elem += 1;

                            match self.resolve_child(&mut buffers, &ci, parent_bi).await? {
                                Some(val) => {
                                    value = val;
                                    continue;
                                }
                                None => {
                                    if ci.is_forwarded {
                                        cur_buf_idx = buffers.len() - 1;
                                        cur_offset = buffers.last().unwrap().base;
                                    } else {
                                        cur_buf_idx = parent_bi;
                                        cur_offset = ci.abs_offset;
                                    }
                                    continue 'descend;
                                }
                            }
                        } else {
                            // On-disk arrays decode to integer-keyed objects,
                            // keyed by element position; null elements become gaps.
                            let final_map: HashMap<String, ArcValue> = std::mem::take(arr)
                                .into_iter()
                                .enumerate()
                                .filter_map(|(i, v)| (!v.is_null()).then(|| (i.to_string(), v)))
                                .collect();
                            value = ArcValue::Object(Arc::new(final_map));
                            stack.pop();
                        }
                    }
                }
            }
        }
    }

    /// Resolve a child node: parse scalars inline (returns Some), or load
    /// a forwarded container buffer and return None.
    async fn resolve_child(
        &self,
        buffers: &mut Vec<BufEntry>,
        child: &ChildInfo,
        parent_buf_idx: usize,
    ) -> Result<Option<ArcValue>> {
        let is_container = matches!(child.type_tag, TYPE_ARRAY | TYPE_COLLECTION);

        // Leaf — parse from buffer or load from IO
        if !is_container {
            if child.is_forwarded {
                let data = read_exact(&self.io, child.abs_offset, child.size as usize).await?;
                return Ok(Some(parse_leaf(&data, 0, child.type_tag)?));
            } else {
                // Inline leaf — parse from parent buffer
                let local_off = local_offset(&buffers[parent_buf_idx], child.abs_offset)?;
                return Ok(Some(parse_leaf(
                    &buffers[parent_buf_idx].data,
                    local_off,
                    child.type_tag,
                )?));
            }
        }

        // Container — load forwarded containers into buffer pool
        if child.is_forwarded {
            let data = read_exact(&self.io, child.abs_offset, child.size as usize).await?;
            buffers.push(BufEntry {
                base: child.abs_offset,
                data,
            });
        }
        Ok(None)
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::MemBlobIO;
    use crate::writer::write_blob;
    use serde_json::json;

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        futures::executor::block_on(f)
    }

    async fn write_and_read_back(value: serde_json::Value) -> ArcValue {
        let tree = ArcValue::from_value(value);
        let io = MemBlobIO::new();
        write_blob(&io, &tree).await.unwrap();
        let session = BlobSession::open(io).await.unwrap();
        session.read_subtree(&[]).await.unwrap()
    }

    /// Reading a corrupted blob must never panic — only return Ok or Err. The
    /// node reader translates on-disk child/forwarded offsets into buffer
    /// indices; before bounds-checking, a corrupt offset would either index out
    /// of bounds or underflow `offset - base`. This is the in-tree counterpart
    /// to `fuzz_blob_session`: it exhaustively flips every byte of a valid blob
    /// (to several adversarial values) and drives the read paths, asserting no
    /// panic. It would have caught the original session_reader.rs OOB crash.
    #[test]
    fn test_read_does_not_panic_on_corrupted_blob() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "users": {
                    "alice": { "age": 30, "name": "Alice", "admin": true },
                    "bob": { "age": 25, "tags": ["x", "y", "z"] }
                },
                "rooms": [{ "id": 1, "members": ["alice", "bob"] }, { "id": 2 }],
                "counters": [0, 1, 2, 3, 4, 5],
                "title": "seed"
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();
            let valid = io.data().to_vec();

            for pos in 0..valid.len() {
                for &val in &[0x00u8, 0x01, 0x7f, 0x80, 0xff] {
                    let mut bytes = valid.clone();
                    if bytes[pos] == val {
                        continue;
                    }
                    bytes[pos] = val;

                    // open() may reject (corrupt header/dictionary); if it
                    // succeeds, the read traversal must also not panic.
                    if let Ok(session) = BlobSession::open(MemBlobIO::from_bytes(bytes)).await {
                        let _ = session.read_subtree(&[]).await;
                        let _ = session.read_keys(&[]).await;
                        let _ = session.read_shallow(&["users"]).await;
                        let _ = session.read_subtree(&["rooms"]).await;
                    }
                }
            }
        });
    }

    #[test]
    fn test_roundtrip_simple_object() {
        block_on(async {
            let original = json!({"hp": 42, "name": "Hero"});
            let result = write_and_read_back(original.clone()).await;
            assert_eq!(result.to_value(), original);
        });
    }

    #[test]
    fn test_roundtrip_nested() {
        block_on(async {
            let original = json!({
                "characters": {
                    "abc": {"hp": 100, "name": "Hero"},
                    "def": {"hp": 50, "name": "Villain"}
                },
                "config": {"mode": "dark"}
            });
            let result = write_and_read_back(original.clone()).await;
            assert_eq!(result.to_value(), original);
        });
    }

    #[test]
    fn test_roundtrip_array() {
        block_on(async {
            // The trailing null is dropped (null = absent), so it does not
            // round-trip; the rest of the array is preserved.
            let result = write_and_read_back(json!({"items": [1, 2, 3, "four", true, null]})).await;
            assert_eq!(result.to_value(), json!({"items": [1, 2, 3, "four", true]}));
        });
    }

    #[test]
    fn test_roundtrip_empty_object() {
        block_on(async {
            let original = json!({});
            let result = write_and_read_back(original.clone()).await;
            assert_eq!(result.to_value(), original);
        });
    }

    #[test]
    #[allow(clippy::approx_constant)] // 3.14 is test data, not an approximation of PI
    fn test_roundtrip_all_types() {
        block_on(async {
            let original = json!({
                "s": "hello",
                "n_int": 42,
                "n_float": 3.14,
                "b_true": true,
                "b_false": false,
                "null_v": null,
                "arr": [1, "two", null],
                "nested": {"x": {"y": 1}}
            });
            let result = write_and_read_back(original).await;
            // The trailing null in "arr" is dropped on read.
            let expected = json!({
                "s": "hello",
                "n_int": 42,
                "n_float": 3.14,
                "b_true": true,
                "b_false": false,
                "null_v": null,
                "arr": [1, "two"],
                "nested": {"x": {"y": 1}}
            });
            assert_eq!(result.to_value(), expected);
        });
    }

    #[test]
    fn test_navigate_to_leaf() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "characters": {
                    "abc": {"hp": 100}
                }
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let session = BlobSession::open(io).await.unwrap();
            let hp = session
                .read_subtree(&["characters", "abc", "hp"])
                .await
                .unwrap();
            assert_eq!(hp.as_i64(), Some(100));
        });
    }

    #[test]
    fn test_navigate_to_subtree() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "characters": {
                    "abc": {"hp": 100, "name": "Hero"}
                }
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let session = BlobSession::open(io).await.unwrap();
            let abc = session.read_subtree(&["characters", "abc"]).await.unwrap();
            assert_eq!(abc.get("hp").unwrap().as_i64(), Some(100));
            assert_eq!(abc.get("name").unwrap().as_str(), Some("Hero"));
        });
    }

    #[test]
    fn test_navigate_nonexistent_path() {
        block_on(async {
            let tree = ArcValue::from_value(json!({"a": 1}));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let session = BlobSession::open(io).await.unwrap();
            let result = session.read_subtree(&["nonexistent"]).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_roundtrip_large_string() {
        block_on(async {
            let big = "x".repeat(10000);
            let original = json!({"data": big});
            let result = write_and_read_back(original.clone()).await;
            assert_eq!(result.to_value(), original);
        });
    }

    #[test]
    fn test_roundtrip_unicode() {
        block_on(async {
            let original = json!({"emoji": "Hello 🌍🚀", "cjk": "你好世界"});
            let result = write_and_read_back(original.clone()).await;
            assert_eq!(result.to_value(), original);
        });
    }

    #[test]
    #[allow(clippy::approx_constant)] // -3.14 is test data, not an approximation of PI
    fn test_roundtrip_negative_numbers() {
        block_on(async {
            let original = json!({"neg": -42, "neg_float": -3.14, "zero": 0});
            let result = write_and_read_back(original.clone()).await;
            assert_eq!(result.to_value(), original);
        });
    }

    #[test]
    fn test_roundtrip_empty_array() {
        block_on(async {
            // An empty array carries no data; it is stored as an empty object,
            // not preserved as `[]`.
            let result = write_and_read_back(json!({"empty": []})).await;
            assert_eq!(result.to_value(), json!({"empty": {}}));
        });
    }

    #[test]
    fn test_roundtrip_nested_arrays() {
        block_on(async {
            let original = json!({"matrix": [[1, 2], [3, 4]]});
            let result = write_and_read_back(original.clone()).await;
            assert_eq!(result.to_value(), original);
        });
    }

    #[test]
    fn test_roundtrip_deeply_nested() {
        block_on(async {
            let mut v = json!(42);
            for i in 0..10 {
                v = json!({ format!("level{}", i): v });
            }
            let original = v;
            let result = write_and_read_back(original.clone()).await;
            assert_eq!(result.to_value(), original);
        });
    }

    #[test]
    fn test_navigate_past_leaf_returns_path_not_found() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "deep": {
                    "item": 42
                }
            }));
            let io = MemBlobIO::new();
            write_blob(&io, &tree).await.unwrap();

            let session = BlobSession::open(io).await.unwrap();
            let val = session.read_subtree(&["deep", "item"]).await.unwrap();
            assert_eq!(val.as_i64(), Some(42));

            let result = session.navigate(&["deep", "item", "d0", "d1"]).await;
            assert!(result.is_err());
            match result.unwrap_err() {
                BlobError::PathNotFound(_) => {}
                other => panic!("Expected PathNotFound, got: {:?}", other),
            }

            let result = session.read_subtree(&["deep", "item", "d0", "d1"]).await;
            assert!(result.is_err());
            match result.unwrap_err() {
                BlobError::PathNotFound(_) => {}
                other => panic!("Expected PathNotFound, got: {:?}", other),
            }
        });
    }

    #[test]
    fn test_roundtrip_many_keys() {
        block_on(async {
            let mut map = serde_json::Map::new();
            for i in 0..300 {
                map.insert(format!("key_{:04}", i), json!(i));
            }
            let original = serde_json::Value::Object(map);
            let result = write_and_read_back(original.clone()).await;
            assert_eq!(result.to_value(), original);
        });
    }
}
