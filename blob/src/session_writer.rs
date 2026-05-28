//! Writer methods on BlobSession: serialize ArcValue trees to bytes.
//!
//! Moved from `writer.rs` so that serialization has access to session state
//! (dictionary, field_id_size, pending_keys). The standalone `write_blob`
//! function remains in `writer.rs` for initial blob creation.

use crate::arc_value::ArcValue;
use crate::dictionary::{hash_field_name, is_collection_key};
use crate::error::{BlobError, Result};
use crate::format::*;
use crate::io::BlobIO;
use crate::session::BlobSession;
use std::collections::HashSet;

struct WriteContext<'a> {
    dict: &'a crate::dictionary::Dictionary,
    _field_id_size: FieldIdSize,
    node_count: u64,
    buf: Vec<u8>,
    /// True for the first collection serialized (the root). The root always
    /// gets reserved space because its position is stored in the header —
    /// moving it requires all readers to refresh.
    is_root: bool,
    /// Collects non-collection keys that were written inline (not in the
    /// dictionary). Drained into BlobSession::pending_keys after serialization.
    pending_keys: &'a mut HashSet<String>,
}

impl<IO: BlobIO> BlobSession<IO> {
    /// Serialize an ArcValue to bytes using the session's dictionary and field_id_size.
    /// Returns (bytes, node_count).
    ///
    /// Any non-collection keys not found in the dictionary are recorded in
    /// `self.pending_keys` so they can be absorbed during the next root_compact.
    pub(crate) fn serialize_value_to_bytes(&mut self, value: &ArcValue) -> Result<(Vec<u8>, u64)> {
        let mut ctx = WriteContext {
            dict: &self.dict,
            _field_id_size: self.field_id_size,
            node_count: 0,
            buf: Vec::new(),
            is_root: false,
            pending_keys: &mut self.pending_keys,
        };
        serialize_value(value, &mut ctx)?;
        Ok((ctx.buf, ctx.node_count))
    }
}

/// Serialize a single ArcValue node (and all descendants) to the write context.
/// Returns the byte offset within ctx.buf where this node starts.
fn serialize_value(value: &ArcValue, ctx: &mut WriteContext) -> Result<usize> {
    ctx.node_count += 1;
    let node_start = ctx.buf.len();

    match value {
        ArcValue::Object(map) => {
            // All objects are serialized as TYPE_COLLECTION.
            // Dictionary-known keys use dict-ref encoding (2 bytes);
            // unknown keys use inline strings (2+len bytes).
            serialize_collection(map, ctx, node_start)?;
        }

        ArcValue::String(s) => {
            ctx.buf.push(TYPE_STRING);
            let bytes = s.as_bytes();
            ctx.buf
                .extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            ctx.buf.extend_from_slice(bytes);
        }

        ArcValue::Number(n) => {
            ctx.buf.push(TYPE_NUMBER);
            let f = n.as_f64().unwrap_or(0.0);
            ctx.buf.extend_from_slice(&f.to_le_bytes());
        }

        ArcValue::Bool(b) => {
            ctx.buf.push(TYPE_BOOL);
            ctx.buf.push(if *b { 0x01 } else { 0x00 });
        }

        ArcValue::Null => {
            ctx.buf.push(TYPE_NULL);
        }

        ArcValue::Sentinel(_) => {
            return Err(BlobError::InternalError(
                "attempted to serialize ArcValue::Sentinel to blob — sentinels are in-memory only"
                    .into(),
            ));
        }
    }

    Ok(node_start)
}

/// Serialize a collection object (TYPE_COLLECTION = 0x08).
fn serialize_collection(
    map: &std::collections::HashMap<String, ArcValue>,
    ctx: &mut WriteContext,
    node_start: usize,
) -> Result<()> {
    // Type tag
    ctx.buf.push(TYPE_COLLECTION);

    // Placeholder for subtree_size (u64)
    let subtree_size_pos = ctx.buf.len();
    ctx.buf.extend_from_slice(&0u64.to_le_bytes());

    // child_count (u32)
    let child_count = map.len() as u32;
    ctx.buf.extend_from_slice(&child_count.to_le_bytes());

    // reserved_count (u32)
    let has_push_id_keys = map.keys().any(|k| is_collection_key(k));
    let reserved_count = if has_push_id_keys || ctx.is_root {
        std::cmp::max(20, child_count / 4)
    } else {
        0
    };
    ctx.is_root = false;
    ctx.buf.extend_from_slice(&reserved_count.to_le_bytes());

    // Build sorted entries: (key_hash, key, value)
    let mut children: Vec<(u64, &str, &ArcValue)> = map
        .iter()
        .map(|(k, v)| (hash_field_name(k), k.as_str(), v))
        .collect();
    children.sort_by_key(|&(hash, _, _)| hash);

    // Compute key string data size and reserved space.
    let key_data_used: u32 = children
        .iter()
        .map(|&(_, key, _)| {
            if ctx.dict.lookup(key).is_some() {
                2u32
            } else {
                2 + key.len() as u32
            }
        })
        .sum();
    #[allow(clippy::manual_checked_ops)] // explicit guard reads clearer than checked_div here
    let avg_key_entry = if child_count > 0 {
        std::cmp::max(24, key_data_used / child_count) as u32
    } else {
        24u32
    };
    let key_data_reserved = key_data_used + reserved_count * avg_key_entry;

    ctx.buf.extend_from_slice(&key_data_used.to_le_bytes());
    ctx.buf.extend_from_slice(&key_data_reserved.to_le_bytes());

    // appended_bytes (u32)
    ctx.buf.extend_from_slice(&0u32.to_le_bytes());

    // Write child_index placeholder
    let child_index_start = ctx.buf.len();
    let total_slots = child_count + reserved_count;
    ctx.buf.resize(
        child_index_start + total_slots as usize * COLLECTION_INDEX_ENTRY_SIZE,
        0,
    );

    // Write key_string_table
    let key_strings_start = ctx.buf.len();
    for &(_, key, _) in &children {
        if let Some(field_id) = ctx.dict.lookup(key) {
            ctx.buf
                .extend_from_slice(&(KEY_DICT_FLAG | field_id as u16).to_le_bytes());
        } else {
            // Inline string — record as pending key if it's not a collection key
            if !is_collection_key(key) {
                ctx.pending_keys.insert(key.to_string());
            }
            let key_bytes = key.as_bytes();
            ctx.buf
                .extend_from_slice(&(key_bytes.len() as u16).to_le_bytes());
            ctx.buf.extend_from_slice(key_bytes);
        }
    }

    // Pad key string area to key_data_reserved
    let key_strings_end = ctx.buf.len();
    let used = (key_strings_end - key_strings_start) as u32;
    debug_assert_eq!(used, key_data_used);
    ctx.buf
        .resize(key_strings_start + key_data_reserved as usize, 0);

    // Children area starts after reserved key string space
    let children_area_start = ctx.buf.len();

    // Serialize each child depth-first
    let mut child_info: Vec<(u64, u8, u64)> = Vec::with_capacity(children.len());
    for &(_, _, child_value) in &children {
        let rel_offset = (ctx.buf.len() - children_area_start) as u64;
        let child_start = ctx.buf.len();
        serialize_value(child_value, ctx)?;
        let type_tag = ctx.buf[child_start];
        let child_size = (ctx.buf.len() - child_start) as u64;
        child_info.push((rel_offset, type_tag, child_size));
    }

    // Backpatch child_index
    for (i, &(hash, _, _)) in children.iter().enumerate() {
        let entry_pos = child_index_start + i * COLLECTION_INDEX_ENTRY_SIZE;
        let (rel_offset, type_tag, size) = child_info[i];
        ctx.buf[entry_pos..entry_pos + 8].copy_from_slice(&hash.to_le_bytes());
        let type_flags = make_type_flags(type_tag, false);
        ctx.buf[entry_pos + 8] = type_flags;
        ctx.buf[entry_pos + 9..entry_pos + 17].copy_from_slice(&rel_offset.to_le_bytes());
        ctx.buf[entry_pos + 17..entry_pos + 25].copy_from_slice(&size.to_le_bytes());
    }

    // Backpatch subtree_size
    let subtree_size = (ctx.buf.len() - node_start) as u64;
    ctx.buf[subtree_size_pos..subtree_size_pos + 8].copy_from_slice(&subtree_size.to_le_bytes());

    Ok(())
}
