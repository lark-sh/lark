//! Writer: serialize an ArcValue tree to a blob.
//!
//! The writer produces a complete blob with header, dictionary, and depth-first
//! serialized tree. Objects have their children sorted by field_id for binary search.

use crate::arc_value::ArcValue;
use crate::dictionary::{Dictionary, collect_field_names, hash_field_name, is_collection_key};
use crate::error::{BlobError, Result};
use crate::format::*;
use crate::io::BlobIO;

/// Stats returned after writing a blob.
#[derive(Debug)]
pub struct BlobStats {
    pub total_size: u64,
    pub node_count: u64,
    pub dict_field_count: u32,
}

/// Serialize an ArcValue tree to a new blob.
pub async fn write_blob<IO: BlobIO>(io: &IO, tree: &ArcValue) -> Result<BlobStats> {
    // 1. Build dictionary from all field names in the tree
    let field_names = collect_field_names(tree);
    let dict = Dictionary::build(field_names);
    let field_id_size = FieldIdSize::from_field_count(dict.max_field_count());

    // 2. Write placeholder header (will be backpatched)
    let header_bytes = [0u8; HEADER_SIZE];
    io.append(&header_bytes).await?;

    // 3. Write dictionary
    let dict_offset = io.size().await?;
    let dict_bytes = dict.to_bytes();
    io.append(&dict_bytes).await?;

    // 4. Write tree depth-first
    let root_offset = io.size().await?;
    let mut ctx = WriteContext {
        dict: &dict,
        _field_id_size: field_id_size,
        node_count: 0,
        buf: Vec::new(),
        is_root: true,
    };
    serialize_value(tree, &mut ctx)?;

    // Write the serialized tree
    io.append(&ctx.buf).await?;

    let total_size = io.size().await?;

    // 5. Backpatch header
    let header = BlobHeader {
        version: VERSION,
        flags: field_id_size.to_flags(),
        dict_offset,
        root_offset,
        node_count: ctx.node_count,
        total_size,
        dict_field_count: dict.field_count(),
    };
    io.pwrite(0, &header.to_bytes()).await?;

    Ok(BlobStats {
        total_size,
        node_count: ctx.node_count,
        dict_field_count: dict.field_count(),
    })
}

struct WriteContext<'a> {
    dict: &'a Dictionary,
    _field_id_size: FieldIdSize,
    node_count: u64,
    buf: Vec<u8>,
    /// True for the first collection serialized (the root). The root always
    /// gets reserved space because its position is stored in the header —
    /// moving it requires all readers to refresh.
    is_root: bool,
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
/// Child index uses key hashes sorted by hash, with a key string table
/// for collision resolution and reading back key names.
///
/// v2 Format:
///   [0x08] [subtree_size: u64] [child_count: u32] [reserved_count: u32]
///   [key_data_used: u32] [key_data_reserved: u32] [appended_bytes: u32]
///   child_index: (key_hash: u64, type_flags: u8, offset: u64, size: u64) × child_count
///   reserved_slots: zeroed × reserved_count
///   key_string_table: (key_len: u16, key_bytes: [u8]) × child_count
///   children_area: contiguous depth-first subtrees
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

    // reserved_count (u32) — extra empty slots for future inserts.
    // Collections with push-ID keys (entity containers) get reserved space
    // because children are frequently inserted/removed. Structural collections
    // (all keys are field names) get zero reserved — they rarely change shape,
    // and if they do, compact_container rebuilds with fresh reserved space.
    // Exception: the root always gets reserved space because its position is
    // in the header — compacting it changes root_offset, requiring readers
    // to refresh.
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
    // Dictionary-known keys use dict-ref encoding: 2 bytes (KEY_DICT_FLAG | field_id).
    // Unknown keys use inline encoding: 2 bytes (key_len) + key bytes.
    let key_data_used: u32 = children
        .iter()
        .map(|&(_, key, _)| {
            if ctx.dict.lookup(key).is_some() {
                2u32 // dict-ref: just the 2-byte key_len with high bit set
            } else {
                2 + key.len() as u32 // inline: key_len + key bytes
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

    // key_data_used (u32), key_data_reserved (u32)
    ctx.buf.extend_from_slice(&key_data_used.to_le_bytes());
    ctx.buf.extend_from_slice(&key_data_reserved.to_le_bytes());

    // appended_bytes (u32) — starts at 0 for a fresh blob
    ctx.buf.extend_from_slice(&0u32.to_le_bytes());

    // Write child_index placeholder: (key_hash:8, type_flags:1, offset:8, size:8) × (child_count + reserved_count)
    let child_index_start = ctx.buf.len();
    let total_slots = child_count + reserved_count;
    ctx.buf.resize(
        child_index_start + total_slots as usize * COLLECTION_INDEX_ENTRY_SIZE,
        0,
    );

    // Write key_string_table: dict-ref or (key_len: u16, key_bytes) × child_count
    // Stored in the same sorted order as the child_index.
    let key_strings_start = ctx.buf.len();
    for &(_, key, _) in &children {
        if let Some(field_id) = ctx.dict.lookup(key) {
            // Dict-ref: high bit set, field_id in lower 15 bits, no inline bytes
            ctx.buf
                .extend_from_slice(&(KEY_DICT_FLAG | field_id as u16).to_le_bytes());
        } else {
            // Inline string: key_len + key bytes
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

    // Serialize each child depth-first, recording offsets, types, and sizes
    let mut child_info: Vec<(u64, u8, u64)> = Vec::with_capacity(children.len()); // (offset, type_tag, size)
    for &(_, _, child_value) in &children {
        let rel_offset = (ctx.buf.len() - children_area_start) as u64;
        let child_start = ctx.buf.len();
        serialize_value(child_value, ctx)?;
        let type_tag = ctx.buf[child_start]; // First byte is the type tag
        let child_size = (ctx.buf.len() - child_start) as u64;
        child_info.push((rel_offset, type_tag, child_size));
    }

    // Backpatch child_index with hashes, type_flags, offsets, and sizes
    for (i, &(hash, _, _)) in children.iter().enumerate() {
        let entry_pos = child_index_start + i * COLLECTION_INDEX_ENTRY_SIZE;
        let (rel_offset, type_tag, size) = child_info[i];
        // key_hash (8 bytes)
        ctx.buf[entry_pos..entry_pos + 8].copy_from_slice(&hash.to_le_bytes());
        // type_flags (1 byte) - fresh blob: not dirty, not forwarded
        let type_flags = make_type_flags(type_tag, false);
        ctx.buf[entry_pos + 8] = type_flags;
        // offset (8 bytes)
        ctx.buf[entry_pos + 9..entry_pos + 17].copy_from_slice(&rel_offset.to_le_bytes());
        // size (8 bytes)
        ctx.buf[entry_pos + 17..entry_pos + 25].copy_from_slice(&size.to_le_bytes());
    }
    // Reserved slots are already zeroed

    // Backpatch subtree_size
    let subtree_size = (ctx.buf.len() - node_start) as u64;
    ctx.buf[subtree_size_pos..subtree_size_pos + 8].copy_from_slice(&subtree_size.to_le_bytes());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::MemBlobIO;
    use serde_json::json;

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        futures::executor::block_on(f)
    }

    #[test]
    fn test_write_simple_object() {
        block_on(async {
            let tree = ArcValue::from_value(json!({"hp": 42, "name": "Hero"}));
            let io = MemBlobIO::new();
            let stats = write_blob(&io, &tree).await.unwrap();

            assert_eq!(stats.node_count, 3); // root object + hp + name
            assert_eq!(stats.dict_field_count, 2);
            assert!(stats.total_size > HEADER_SIZE as u64);

            // Verify header is readable
            let header_data: [u8; HEADER_SIZE] =
                io.pread(0, HEADER_SIZE).await.unwrap().try_into().unwrap();
            let header = BlobHeader::from_bytes(&header_data).unwrap();
            assert_eq!(header.version, VERSION);
            assert_eq!(header.node_count, 3);
            assert_eq!(header.dict_field_count, 2);
            assert_eq!(header.total_size, io.size().await.unwrap());
        });
    }

    #[test]
    fn test_write_nested_object() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "characters": {
                    "abc": {"hp": 100}
                }
            }));
            let io = MemBlobIO::new();
            let stats = write_blob(&io, &tree).await.unwrap();

            // root + characters + abc + hp = 4 nodes
            assert_eq!(stats.node_count, 4);
        });
    }

    #[test]
    fn test_write_array() {
        block_on(async {
            let tree = ArcValue::from_value(json!({"items": [1, 2, 3]}));
            let io = MemBlobIO::new();
            let stats = write_blob(&io, &tree).await.unwrap();

            // root + items(array) + 3 numbers = 5 nodes
            assert_eq!(stats.node_count, 5);
        });
    }

    #[test]
    fn test_write_empty_object() {
        block_on(async {
            let tree = ArcValue::from_value(json!({}));
            let io = MemBlobIO::new();
            let stats = write_blob(&io, &tree).await.unwrap();

            assert_eq!(stats.node_count, 1);
            assert_eq!(stats.dict_field_count, 0);
        });
    }

    #[test]
    fn test_write_all_types() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "str": "hello",
                "num": 42.5,
                "bool_t": true,
                "bool_f": false,
                "null_v": null,
                "arr": [1, "two"],
                "nested": {"x": 1}
            }));
            let io = MemBlobIO::new();
            let stats = write_blob(&io, &tree).await.unwrap();

            // root(1) + 7 children:
            //   str(1) + num(1) + bool_t(1) + bool_f(1) + null_v(1) + arr(1+2) + nested(1+1)
            //   = 1 + 1 + 1 + 1 + 1 + 1 + 3 + 2 = 11
            assert_eq!(stats.node_count, 11);
        });
    }

    #[test]
    fn test_write_collection_object() {
        block_on(async {
            // All objects are written as TYPE_COLLECTION
            let tree = ArcValue::from_value(json!({
                "chat": {
                    "-Mabc123": {"text": "hello"},
                    "-Mdef456": {"text": "world"}
                }
            }));
            let io = MemBlobIO::new();
            let stats = write_blob(&io, &tree).await.unwrap();

            // root(1) + chat(1) + 2 messages × (object + text) = 1 + 1 + 2*2 = 6
            assert_eq!(stats.node_count, 6);
            // Only structural fields in dict: "chat", "text" — not the push IDs
            assert_eq!(stats.dict_field_count, 2);
        });
    }

    #[test]
    fn test_write_duplicate_field_names() {
        block_on(async {
            let tree = ArcValue::from_value(json!({
                "a": {"x": 1, "y": 2},
                "b": {"x": 3, "y": 4}
            }));
            let io = MemBlobIO::new();
            let stats = write_blob(&io, &tree).await.unwrap();

            // x and y should be deduplicated in the dictionary
            assert_eq!(stats.dict_field_count, 4); // a, b, x, y
        });
    }
}
