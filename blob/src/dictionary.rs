//! Field name dictionary: build, lookup, serialize/deserialize.
//!
//! The dictionary maps field names to small integer field_ids.
//! It is stored once per blob and used by all objects for field lookup.

use crate::error::{BlobError, Result};
use std::collections::HashMap;
use xxhash_rust::xxh64::xxh64;

/// A field name dictionary. Stores deduplicated field names with hash-based lookup.
///
/// On-disk format (with reserved space for growth):
///   field_count: u32        — current number of live fields
///   sorted_count: u32       — entries 0..sorted_count are sorted (binary search region)
///   max_field_count: u32    — total allocated slots for hashes/field_ids/lengths
///   name_data_used: u32     — bytes of name_data currently used
///   max_name_data: u32      — total allocated bytes for name_data
///   sorted_hashes: [u64; max_field_count]    — first field_count are live
///   sorted_to_field_id: [u32; max_field_count]
///   field_name_lengths: [u32; max_field_count]
///   name_data: [u8; max_name_data]
///
/// On initial write, sorted_count == field_count (all sorted).
/// During incremental compaction, new fields are appended unsorted after the sorted region.
/// Lookup: binary search sorted region, then linear scan appended region.
/// Full recompact rebuilds the dictionary fully sorted.
#[derive(Debug, Clone)]
pub struct Dictionary {
    /// Hash values. First sorted_count entries are sorted for binary search.
    /// Entries sorted_count..field_count are appended unsorted.
    sorted_hashes: Vec<u64>,
    /// For each position in sorted_hashes/sorted_to_field_id, the corresponding field_id.
    sorted_to_field_id: Vec<u32>,
    /// Number of entries in the sorted region (for binary search).
    sorted_count: usize,
    /// Field names indexed by field_id.
    field_names: Vec<String>,
    /// O(1) lookup cache: field name → field_id.
    name_to_id: HashMap<String, u32>,
    /// Maximum allocated field slots (reserved capacity).
    max_field_count: u32,
    /// Current bytes used in name_data on disk.
    name_data_used: u32,
    /// Maximum allocated bytes for name_data on disk.
    max_name_data: u32,
}

impl Dictionary {
    /// Build a dictionary from a list of unique field names.
    /// The field names are sorted by hash for binary search lookup.
    /// field_id is assigned based on the original insertion order (0, 1, 2, ...).
    /// Reserves space for future growth: max(500, 2 * field_count) field slots
    /// and max(10000, 2 * name_data_size) bytes for name strings.
    pub fn build(field_names: Vec<String>) -> Self {
        let field_count = field_names.len() as u32;
        let name_data_used: u32 = field_names.iter().map(|n| n.len() as u32).sum();
        let max_field_count = std::cmp::max(500, 2 * field_count);
        let max_name_data = std::cmp::max(10000, 2 * name_data_used);

        if field_names.is_empty() {
            return Dictionary {
                sorted_hashes: vec![],
                sorted_to_field_id: vec![],
                sorted_count: 0,
                field_names: vec![],
                name_to_id: HashMap::new(),
                max_field_count,
                name_data_used: 0,
                max_name_data,
            };
        }

        // Compute hashes and pair with original index
        let mut entries: Vec<(u64, usize)> = field_names
            .iter()
            .enumerate()
            .map(|(i, name)| (hash_field_name(name), i))
            .collect();

        // Sort by hash for binary search
        entries.sort_by_key(|&(hash, _)| hash);

        let sorted_hashes: Vec<u64> = entries.iter().map(|&(h, _)| h).collect();
        let sorted_to_field_id: Vec<u32> = entries.iter().map(|&(_, i)| i as u32).collect();
        let sorted_count = sorted_hashes.len();

        let name_to_id: HashMap<String, u32> = field_names
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i as u32))
            .collect();

        Dictionary {
            sorted_hashes,
            sorted_to_field_id,
            sorted_count,
            field_names,
            name_to_id,
            max_field_count,
            name_data_used,
            max_name_data,
        }
    }

    /// Look up a field name -> field_id via O(1) HashMap lookup.
    pub fn lookup(&self, name: &str) -> Option<u32> {
        self.name_to_id.get(name).copied()
    }

    /// Look up a field name, or insert it and return the new field_id.
    ///
    /// Used during full compaction to accumulate inline keys into the
    /// dictionary. The sorted_hashes/sorted_to_field_id arrays are NOT
    /// updated here — the caller must rebuild via `Dictionary::build()`
    /// after all insertions to get proper sorted hashes and reserved space.
    pub fn lookup_or_insert(&mut self, name: &str) -> u32 {
        if let Some(id) = self.name_to_id.get(name).copied() {
            return id;
        }
        let new_id = self.field_names.len() as u32;
        self.field_names.push(name.to_string());
        self.name_to_id.insert(name.to_string(), new_id);
        new_id
    }

    /// Get field name by field_id.
    pub fn get_name(&self, field_id: u32) -> Result<&str> {
        self.field_names
            .get(field_id as usize)
            .map(|s| s.as_str())
            .ok_or(BlobError::FieldIdOutOfRange(
                field_id,
                self.field_names.len() as u32,
            ))
    }

    /// Number of live fields in the dictionary.
    pub fn field_count(&self) -> u32 {
        self.field_names.len() as u32
    }

    /// All field names in field_id order. Used to rebuild the dictionary
    /// with fresh reserved space (field_ids are preserved).
    pub fn field_names(&self) -> &[String] {
        &self.field_names
    }

    /// Maximum field capacity (reserved slots for growth).
    pub fn max_field_count(&self) -> u32 {
        self.max_field_count
    }

    /// Serialize the 20-byte dictionary header.
    ///
    /// Used by `ensure_fields_in_dict` to atomically commit new field additions
    /// by writing the header (with updated `field_count` and `name_data_used`)
    /// as a single pwrite after all field data patches have been written.
    pub fn serialize_header(&self) -> [u8; 20] {
        let mut buf = [0u8; 20];
        buf[0..4].copy_from_slice(&(self.field_names.len() as u32).to_le_bytes());
        buf[4..8].copy_from_slice(&(self.sorted_count as u32).to_le_bytes());
        buf[8..12].copy_from_slice(&self.max_field_count.to_le_bytes());
        buf[12..16].copy_from_slice(&self.name_data_used.to_le_bytes());
        buf[16..20].copy_from_slice(&self.max_name_data.to_le_bytes());
        buf
    }

    /// Serialize the dictionary to bytes with reserved space for growth.
    ///
    /// Format:
    ///   field_count: u32
    ///   sorted_count: u32
    ///   max_field_count: u32
    ///   name_data_used: u32
    ///   max_name_data: u32
    ///   sorted_hashes: [u64; max_field_count]       (first field_count live, rest zeroed)
    ///   sorted_to_field_id: [u32; max_field_count]  (first field_count live, rest zeroed)
    ///   field_name_lengths: [u32; max_field_count]   (first field_count live, rest zeroed)
    ///   name_data: [u8; max_name_data]              (first name_data_used live, rest zeroed)
    pub fn to_bytes(&self) -> Vec<u8> {
        let field_count = self.field_names.len() as u32;
        let mut buf = Vec::with_capacity(self.serialized_size());

        // Header: 5 x u32
        buf.extend_from_slice(&field_count.to_le_bytes());
        buf.extend_from_slice(&(self.sorted_count as u32).to_le_bytes());
        buf.extend_from_slice(&self.max_field_count.to_le_bytes());
        buf.extend_from_slice(&self.name_data_used.to_le_bytes());
        buf.extend_from_slice(&self.max_name_data.to_le_bytes());

        // sorted_hashes: max_field_count slots (live entries + zeroed reserved)
        for &h in &self.sorted_hashes {
            buf.extend_from_slice(&h.to_le_bytes());
        }
        let reserved_fields = self.max_field_count as usize - self.sorted_hashes.len();
        buf.resize(buf.len() + reserved_fields * 8, 0);

        // sorted_to_field_id: max_field_count slots
        for &fid in &self.sorted_to_field_id {
            buf.extend_from_slice(&fid.to_le_bytes());
        }
        buf.resize(buf.len() + reserved_fields * 4, 0);

        // field_name_lengths: max_field_count slots (indexed by field_id)
        for name in &self.field_names {
            buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
        }
        buf.resize(buf.len() + reserved_fields * 4, 0);

        // name_data: max_name_data bytes (live strings + zeroed reserved)
        for name in &self.field_names {
            buf.extend_from_slice(name.as_bytes());
        }
        let name_padding = self.max_name_data as usize - self.name_data_used as usize;
        buf.resize(buf.len() + name_padding, 0);

        buf
    }

    /// Deserialize a dictionary from bytes (new format with reserved space).
    pub fn from_bytes(data: &[u8]) -> Result<(Self, usize)> {
        if data.len() < 20 {
            return Err(BlobError::UnexpectedEof);
        }

        let field_count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let sorted_count = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
        let max_field_count = u32::from_le_bytes(data[8..12].try_into().unwrap());
        let name_data_used = u32::from_le_bytes(data[12..16].try_into().unwrap());
        let max_name_data = u32::from_le_bytes(data[16..20].try_into().unwrap());

        let mut pos = 20;

        // sorted_hashes: read field_count live entries, skip reserved slots
        let hashes_total_size = max_field_count as usize * 8;
        if data.len() < pos + hashes_total_size {
            return Err(BlobError::UnexpectedEof);
        }
        let mut sorted_hashes = Vec::with_capacity(field_count);
        for i in 0..field_count {
            let offset = pos + i * 8;
            sorted_hashes.push(u64::from_le_bytes(
                data[offset..offset + 8].try_into().unwrap(),
            ));
        }
        pos += hashes_total_size;

        // sorted_to_field_id: read field_count live entries, skip reserved
        let fids_total_size = max_field_count as usize * 4;
        if data.len() < pos + fids_total_size {
            return Err(BlobError::UnexpectedEof);
        }
        let mut sorted_to_field_id = Vec::with_capacity(field_count);
        for i in 0..field_count {
            let offset = pos + i * 4;
            sorted_to_field_id.push(u32::from_le_bytes(
                data[offset..offset + 4].try_into().unwrap(),
            ));
        }
        pos += fids_total_size;

        // field_name_lengths: read field_count entries, skip reserved
        let lengths_total_size = max_field_count as usize * 4;
        if data.len() < pos + lengths_total_size {
            return Err(BlobError::UnexpectedEof);
        }
        let mut name_lengths = Vec::with_capacity(field_count);
        for i in 0..field_count {
            let offset = pos + i * 4;
            name_lengths
                .push(u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize);
        }
        pos += lengths_total_size;

        // name_data: read live name strings, skip reserved bytes
        let mut field_names = Vec::with_capacity(field_count);
        let name_data_start = pos;
        let mut name_pos = name_data_start;
        for &len in &name_lengths {
            if data.len() < name_pos + len {
                return Err(BlobError::UnexpectedEof);
            }
            let name = std::str::from_utf8(&data[name_pos..name_pos + len])
                .map_err(|_| BlobError::UnexpectedEof)?;
            field_names.push(name.to_string());
            name_pos += len;
        }
        pos += max_name_data as usize; // skip entire name_data area including reserved

        let name_to_id: HashMap<String, u32> = field_names
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i as u32))
            .collect();

        Ok((
            Dictionary {
                sorted_hashes,
                sorted_to_field_id,
                sorted_count,
                field_names,
                name_to_id,
                max_field_count,
                name_data_used,
                max_name_data,
            },
            pos,
        ))
    }

    /// Total serialized size in bytes (including reserved space).
    pub fn serialized_size(&self) -> usize {
        let mfc = self.max_field_count as usize;
        20                        // 5 x u32 header
        + (mfc * 8)              // sorted_hashes
        + (mfc * 4)              // sorted_to_field_id
        + (mfc * 4)              // field_name_lengths
        + self.max_name_data as usize // name_data
    }

    /// Append a new field name to the dictionary (for incremental compaction).
    ///
    /// The new entry is appended unsorted after the sorted region.
    /// Returns (new_field_id, patches) where patches are (offset_within_dict, bytes)
    /// pairs for pwriting the on-disk dictionary.
    ///
    /// Returns Err(DictionaryFull) if no capacity remains.
    #[allow(clippy::type_complexity)] // (field_id, Vec<(offset, bytes)>) patch list
    pub fn append_field(&mut self, name: &str) -> Result<(u32, Vec<(usize, Vec<u8>)>)> {
        let field_count = self.field_names.len() as u32;
        if field_count >= self.max_field_count {
            return Err(BlobError::DictionaryFull);
        }
        let name_bytes = name.as_bytes();
        if self.name_data_used as usize + name_bytes.len() > self.max_name_data as usize {
            return Err(BlobError::DictionaryFull);
        }

        let new_field_id = field_count;
        let hash = hash_field_name(name);

        // Update in-memory state
        self.sorted_hashes.push(hash);
        self.sorted_to_field_id.push(new_field_id);
        self.field_names.push(name.to_string());
        self.name_to_id.insert(name.to_string(), new_field_id);

        let new_field_count = field_count + 1;
        let mfc = self.max_field_count as usize;

        // Compute pwrite patches (offsets within the serialized dictionary)
        let mut patches = Vec::new();

        // 1. Update field_count at offset 0
        patches.push((0, new_field_count.to_le_bytes().to_vec()));

        // 2. Write hash at sorted_hashes[field_count]
        let hash_offset = 20 + field_count as usize * 8;
        patches.push((hash_offset, hash.to_le_bytes().to_vec()));

        // 3. Write field_id at sorted_to_field_id[field_count]
        let fid_offset = 20 + mfc * 8 + field_count as usize * 4;
        patches.push((fid_offset, new_field_id.to_le_bytes().to_vec()));

        // 4. Write name_length at field_name_lengths[field_count]
        let len_offset = 20 + mfc * 8 + mfc * 4 + field_count as usize * 4;
        patches.push((len_offset, (name_bytes.len() as u32).to_le_bytes().to_vec()));

        // 5. Write name bytes at name_data[name_data_used]
        let name_offset = 20 + mfc * 8 + mfc * 4 + mfc * 4 + self.name_data_used as usize;
        patches.push((name_offset, name_bytes.to_vec()));

        // 6. Update name_data_used
        self.name_data_used += name_bytes.len() as u32;
        patches.push((12, self.name_data_used.to_le_bytes().to_vec()));

        Ok((new_field_id, patches))
    }
}

/// Hash a field name using xxhash64 (consistent with Lark).
pub fn hash_field_name(name: &str) -> u64 {
    xxh64(name.as_bytes(), 0)
}

/// Returns true if a key looks like a Firebase push ID (entity-ID key).
/// These start with '-' followed by Base64-like characters (alphanumeric, '-', '_').
/// Entity-ID keys are stored inline in collection objects, not in the dictionary.
pub fn is_collection_key(key: &str) -> bool {
    key.starts_with('-')
        && key.len() > 1
        && key[1..]
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// Returns true if all keys in an object are in the dictionary.
/// If any key is missing from the dictionary, the object should be written
/// as a collection (TYPE_COLLECTION) with inline keys.
pub fn all_keys_in_dict(keys: impl Iterator<Item = impl AsRef<str>>, dict: &Dictionary) -> bool {
    keys.into_iter().all(|k| dict.lookup(k.as_ref()).is_some())
}

/// Collect all unique *structural* field names from an ArcValue tree.
/// Entity-ID keys (push IDs) are excluded — they go in collection objects, not the dictionary.
///
/// An object's keys are structural if none of them look like push IDs.
/// If any key in an object looks like a push ID, ALL of that object's keys
/// are excluded (the whole object will be a collection).
pub fn collect_field_names(value: &crate::arc_value::ArcValue) -> Vec<String> {
    use std::collections::BTreeSet;

    let mut names = BTreeSet::new();
    collect_field_names_recursive(value, &mut names);
    names.into_iter().collect()
}

fn collect_field_names_recursive(
    value: &crate::arc_value::ArcValue,
    names: &mut std::collections::BTreeSet<String>,
) {
    use crate::arc_value::ArcValue;

    match value {
        ArcValue::Object(map) => {
            // If any key looks like a push ID, this is a collection —
            // none of its keys go in the dictionary.
            let is_collection = map.keys().any(|k| is_collection_key(k));
            if !is_collection {
                for key in map.keys() {
                    names.insert(key.clone());
                }
            }
            // Always recurse into children (they may contain structural objects)
            for child in map.values() {
                collect_field_names_recursive(child, names);
            }
        }
        ArcValue::Array(arr) => {
            for child in arr.iter() {
                collect_field_names_recursive(child, names);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arc_value::ArcValue;
    use serde_json::json;

    #[test]
    fn test_build_and_lookup() {
        let names = vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()];
        let dict = Dictionary::build(names);

        assert_eq!(dict.field_count(), 3);

        // Each name should resolve to a unique field_id
        let id_alpha = dict.lookup("alpha").unwrap();
        let id_beta = dict.lookup("beta").unwrap();
        let id_gamma = dict.lookup("gamma").unwrap();

        assert_ne!(id_alpha, id_beta);
        assert_ne!(id_beta, id_gamma);

        // Reverse lookup
        assert_eq!(dict.get_name(id_alpha).unwrap(), "alpha");
        assert_eq!(dict.get_name(id_beta).unwrap(), "beta");
        assert_eq!(dict.get_name(id_gamma).unwrap(), "gamma");
    }

    #[test]
    fn test_lookup_not_found() {
        let dict = Dictionary::build(vec!["x".to_string()]);
        assert!(dict.lookup("y").is_none());
    }

    #[test]
    fn test_serialization_roundtrip() {
        let names = vec![
            "characters".to_string(),
            "hp".to_string(),
            "name".to_string(),
            "x".to_string(),
            "y".to_string(),
        ];
        let dict = Dictionary::build(names);
        let bytes = dict.to_bytes();
        let (dict2, consumed) = Dictionary::from_bytes(&bytes).unwrap();

        assert_eq!(consumed, bytes.len());
        assert_eq!(dict2.field_count(), dict.field_count());

        // All lookups should match
        for name in &["characters", "hp", "name", "x", "y"] {
            let id1 = dict.lookup(name).unwrap();
            let id2 = dict2.lookup(name).unwrap();
            assert_eq!(id1, id2);
            assert_eq!(dict.get_name(id1).unwrap(), dict2.get_name(id2).unwrap());
        }
    }

    #[test]
    fn test_collect_field_names_structural() {
        // All keys here are structural (no push IDs)
        let tree = ArcValue::from_value(json!({
            "characters": {
                "abc": {"hp": 100, "name": "Hero"},
                "def": {"hp": 50, "name": "Villain"}
            },
            "config": {"mode": "dark"}
        }));
        let names = collect_field_names(&tree);
        assert!(names.contains(&"characters".to_string()));
        assert!(names.contains(&"hp".to_string()));
        assert!(names.contains(&"name".to_string()));
        assert!(names.contains(&"config".to_string()));
        assert!(names.contains(&"mode".to_string()));
        assert!(names.contains(&"abc".to_string()));
        assert!(names.contains(&"def".to_string()));
        assert_eq!(names.len(), 7);
    }

    #[test]
    fn test_collect_field_names_excludes_push_ids() {
        // The "characters" object has push ID keys — those should NOT go in the dictionary.
        // But the children's structural keys (hp, name) still should.
        let tree = ArcValue::from_value(json!({
            "characters": {
                "-Mabc123": {"hp": 100, "name": "Hero"},
                "-Mdef456": {"hp": 50, "name": "Villain"}
            },
            "config": {"mode": "dark"}
        }));
        let names = collect_field_names(&tree);
        assert!(names.contains(&"characters".to_string()));
        assert!(names.contains(&"hp".to_string()));
        assert!(names.contains(&"name".to_string()));
        assert!(names.contains(&"config".to_string()));
        assert!(names.contains(&"mode".to_string()));
        // Push IDs excluded
        assert!(!names.contains(&"-Mabc123".to_string()));
        assert!(!names.contains(&"-Mdef456".to_string()));
        assert_eq!(names.len(), 5);
    }

    #[test]
    fn test_is_collection_key() {
        assert!(is_collection_key("-Mabc123"));
        assert!(is_collection_key("-abc"));
        assert!(is_collection_key("-M_test_123"));
        assert!(is_collection_key("-LP6Tv-WFOlkkHae2Jfh")); // push ID with dash in body
        assert!(is_collection_key("-Nq-QCz8apZiRkPNCcVD")); // push ID with dash in body
        assert!(!is_collection_key("hp"));
        assert!(!is_collection_key("characters"));
        assert!(!is_collection_key("-")); // just a dash, no content
        assert!(!is_collection_key(""));
        assert!(!is_collection_key("char-attribs")); // dash in middle, not at start
    }

    #[test]
    fn test_empty_dictionary() {
        let dict = Dictionary::build(vec![]);
        assert_eq!(dict.field_count(), 0);
        assert!(dict.lookup("anything").is_none());

        let bytes = dict.to_bytes();
        let (dict2, _) = Dictionary::from_bytes(&bytes).unwrap();
        assert_eq!(dict2.field_count(), 0);
    }
}
