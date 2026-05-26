//! Binary format constants, node types, header struct, and encoding/decoding helpers.

use crate::error::{BlobError, Result};

pub const MAGIC: &[u8; 4] = b"LARK";
pub const VERSION: u16 = 2; // v2: type+size in parent index
pub const HEADER_SIZE: usize = 64;

// Node type tags (TYPE_OBJECT 0x01 removed — all objects are TYPE_COLLECTION)
pub const TYPE_ARRAY: u8 = 0x02;
pub const TYPE_STRING: u8 = 0x03;
pub const TYPE_NUMBER: u8 = 0x04;
pub const TYPE_BOOL: u8 = 0x05;
pub const TYPE_NULL: u8 = 0x06;
pub const TYPE_COLLECTION: u8 = 0x08;

/// Size of the fixed portion of an array header:
/// type(1) + subtree_size(8) + elem_count(4) + appended_bytes(4) = 17 bytes.
pub const ARRAY_HEADER_SIZE: usize = 17;

/// Size of the fixed portion of a collection object header:
/// type(1) + subtree_size(8) + child_count(4) + reserved_count(4)
/// + key_data_used(4) + key_data_reserved(4) + appended_bytes(4) = 29 bytes.
pub const COLLECTION_HEADER_SIZE: usize = 29;

/// Size of each collection child index entry: key_hash(8) + type_flags(1) + offset(8) + size(8) = 25 bytes.
pub const COLLECTION_INDEX_ENTRY_SIZE: usize = 25;

/// Size of each array element index entry: type_flags(1) + offset(8) + size(8) = 17 bytes.
pub const ARRAY_INDEX_ENTRY_SIZE: usize = 17;

// Key string encoding flags (for TYPE_COLLECTION key_strings area)
/// High bit of key_len indicates a dictionary reference (no inline bytes follow).
pub const KEY_DICT_FLAG: u16 = 0x8000;
/// Mask for the field_id when KEY_DICT_FLAG is set.
pub const KEY_DICT_MASK: u16 = 0x7FFF;

// Type flags bit masks
/// Mask for the type tag in the lower 4 bits of type_flags.
pub const TYPE_FLAGS_TYPE_MASK: u8 = 0x0F;
/// Bit 7: offset is absolute (child is forwarded), not relative to children_area.
pub const TYPE_FLAGS_FORWARDED: u8 = 0x80;

/// Compute the number of reserved index slots for a collection based on its
/// total children size. Small containers (< 10KB) only get reserved space if
/// they have push-ID keys (the old behavior). Once a container is large enough,
/// rewrites are expensive, so we always reserve space to avoid unnecessary
/// compactions that trigger rewrites.
#[inline]
pub fn compute_reserved_count(
    child_count: u32,
    total_children_size: u64,
    has_push_id_keys: bool,
) -> u32 {
    if total_children_size >= 1_000_000 {
        // >= 1MB: reserve generously — rewrites at this size are very expensive
        std::cmp::max(40, child_count / 2)
    } else if total_children_size >= 10_000 {
        // >= 10KB: reserve moderately — rewrites are starting to be costly
        std::cmp::max(20, child_count / 4)
    } else if has_push_id_keys {
        // Small push-ID collection: old behavior
        std::cmp::max(20, child_count / 4)
    } else {
        // Small structural collection: no reserved space
        0
    }
}

/// Create a type_flags byte from a type tag and flags.
#[inline]
pub fn make_type_flags(type_tag: u8, is_forwarded: bool) -> u8 {
    let mut flags = type_tag & TYPE_FLAGS_TYPE_MASK;
    if is_forwarded {
        flags |= TYPE_FLAGS_FORWARDED;
    }
    flags
}

/// Extract the type tag from a type_flags byte.
#[inline]
pub fn extract_type_tag(type_flags: u8) -> u8 {
    type_flags & TYPE_FLAGS_TYPE_MASK
}

/// Check if the forwarded flag is set in type_flags.
#[inline]
pub fn is_forwarded_flag(type_flags: u8) -> bool {
    (type_flags & TYPE_FLAGS_FORWARDED) != 0
}

/// field_id_size encoding in header flags bits 0-1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldIdSize {
    U8 = 0,
    U16 = 1,
}

impl FieldIdSize {
    pub fn byte_size(self) -> usize {
        match self {
            FieldIdSize::U8 => 1,
            FieldIdSize::U16 => 2,
        }
    }

    pub fn from_field_count(_count: u32) -> Self {
        FieldIdSize::U16
    }

    pub fn from_flags(flags: u16) -> Result<Self> {
        match flags & 0x03 {
            0 => Ok(FieldIdSize::U8),
            1 => Ok(FieldIdSize::U16),
            v => Err(BlobError::InvalidFieldIdSize(v as u8)),
        }
    }

    pub fn to_flags(self) -> u16 {
        self as u16
    }
}

/// Blob header — 64 bytes fixed.
///
/// Layout:
///   magic:            4 bytes  (offset 0)
///   version:          2 bytes  (offset 4)
///   flags:            2 bytes  (offset 6)
///   dict_offset:      8 bytes  (offset 8)
///   root_offset:      8 bytes  (offset 16)
///   node_count:       8 bytes  (offset 24)
///   total_size:       8 bytes  (offset 32)
///   dict_field_count: 4 bytes  (offset 40)
///   reserved:        20 bytes  (offset 44)
///   ---
///   Total: 64 bytes
///
/// Checksum is deferred to a future phase (will be stored in a sidecar or
/// in reserved space).
#[derive(Debug, Clone)]
pub struct BlobHeader {
    pub version: u16,
    pub flags: u16,
    pub dict_offset: u64,
    pub root_offset: u64,
    pub node_count: u64,
    pub total_size: u64,
    pub dict_field_count: u32,
}

impl BlobHeader {
    pub fn field_id_size(&self) -> Result<FieldIdSize> {
        FieldIdSize::from_flags(self.flags)
    }

    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(MAGIC);
        buf[4..6].copy_from_slice(&self.version.to_le_bytes());
        buf[6..8].copy_from_slice(&self.flags.to_le_bytes());
        buf[8..16].copy_from_slice(&self.dict_offset.to_le_bytes());
        buf[16..24].copy_from_slice(&self.root_offset.to_le_bytes());
        buf[24..32].copy_from_slice(&self.node_count.to_le_bytes());
        buf[32..40].copy_from_slice(&self.total_size.to_le_bytes());
        buf[40..44].copy_from_slice(&self.dict_field_count.to_le_bytes());
        // bytes 44..64: reserved (zeroed)
        buf
    }

    pub fn from_bytes(buf: &[u8; HEADER_SIZE]) -> Result<Self> {
        if &buf[0..4] != MAGIC {
            return Err(BlobError::InvalidMagic);
        }
        let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
        // Accept version 2 (current) only
        if version != VERSION {
            return Err(BlobError::UnsupportedVersion(version));
        }
        let flags = u16::from_le_bytes(buf[6..8].try_into().unwrap());
        let dict_offset = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let root_offset = u64::from_le_bytes(buf[16..24].try_into().unwrap());
        let node_count = u64::from_le_bytes(buf[24..32].try_into().unwrap());
        let total_size = u64::from_le_bytes(buf[32..40].try_into().unwrap());
        let dict_field_count = u32::from_le_bytes(buf[40..44].try_into().unwrap());

        Ok(BlobHeader {
            version,
            flags,
            dict_offset,
            root_offset,
            node_count,
            total_size,
            dict_field_count,
        })
    }
}

/// Write a field_id to bytes.
pub fn encode_field_id(field_id: u32, size: FieldIdSize) -> Vec<u8> {
    match size {
        FieldIdSize::U8 => vec![field_id as u8],
        FieldIdSize::U16 => (field_id as u16).to_le_bytes().to_vec(),
    }
}

/// Read a field_id from bytes.
pub fn decode_field_id(data: &[u8], size: FieldIdSize) -> Result<u32> {
    match size {
        FieldIdSize::U8 => {
            if data.is_empty() {
                return Err(BlobError::UnexpectedEof);
            }
            Ok(data[0] as u32)
        }
        FieldIdSize::U16 => {
            if data.len() < 2 {
                return Err(BlobError::UnexpectedEof);
            }
            Ok(u16::from_le_bytes(data[0..2].try_into().unwrap()) as u32)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_roundtrip() {
        let header = BlobHeader {
            version: VERSION,
            flags: FieldIdSize::U16.to_flags(),
            dict_offset: 64,
            root_offset: 1024,
            node_count: 42,
            total_size: 8192,
            dict_field_count: 10,
        };
        let bytes = header.to_bytes();
        assert_eq!(&bytes[0..4], MAGIC);
        let parsed = BlobHeader::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.version, VERSION);
        assert_eq!(parsed.dict_offset, 64);
        assert_eq!(parsed.root_offset, 1024);
        assert_eq!(parsed.node_count, 42);
        assert_eq!(parsed.total_size, 8192);
        assert_eq!(parsed.dict_field_count, 10);
        assert_eq!(parsed.field_id_size().unwrap(), FieldIdSize::U16);
    }

    #[test]
    fn test_field_id_size_from_count() {
        // Always U16 regardless of count
        assert_eq!(FieldIdSize::from_field_count(0), FieldIdSize::U16);
        assert_eq!(FieldIdSize::from_field_count(255), FieldIdSize::U16);
        assert_eq!(FieldIdSize::from_field_count(256), FieldIdSize::U16);
        assert_eq!(FieldIdSize::from_field_count(65535), FieldIdSize::U16);
        assert_eq!(FieldIdSize::from_field_count(100000), FieldIdSize::U16);
    }

    #[test]
    fn test_encode_decode_field_id() {
        for &fid_size in &[FieldIdSize::U8, FieldIdSize::U16] {
            let max_val = match fid_size {
                FieldIdSize::U8 => 255,
                FieldIdSize::U16 => 65535,
            };
            let encoded = encode_field_id(max_val, fid_size);
            let decoded = decode_field_id(&encoded, fid_size).unwrap();
            assert_eq!(decoded, max_val);
        }
    }

    #[test]
    fn test_invalid_magic() {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(b"NOPE");
        assert!(BlobHeader::from_bytes(&buf).is_err());
    }

    #[test]
    fn test_type_flags_roundtrip() {
        // Inline string
        let flags = make_type_flags(TYPE_STRING, false);
        assert_eq!(flags, 0x03);
        assert_eq!(extract_type_tag(flags), TYPE_STRING);
        assert!(!is_forwarded_flag(flags));

        // Forwarded string
        let flags = make_type_flags(TYPE_STRING, true);
        assert_eq!(flags, 0x83);
        assert_eq!(extract_type_tag(flags), TYPE_STRING);
        assert!(is_forwarded_flag(flags));

        // Inline collection
        let flags = make_type_flags(TYPE_COLLECTION, false);
        assert_eq!(flags, 0x08);
        assert_eq!(extract_type_tag(flags), TYPE_COLLECTION);
        assert!(!is_forwarded_flag(flags));

        // Forwarded collection
        let flags = make_type_flags(TYPE_COLLECTION, true);
        assert_eq!(flags, 0x88);
        assert_eq!(extract_type_tag(flags), TYPE_COLLECTION);
        assert!(is_forwarded_flag(flags));
    }

    #[test]
    fn test_index_entry_sizes() {
        // Collection: key_hash(8) + type_flags(1) + offset(8) + size(8) = 25
        assert_eq!(COLLECTION_INDEX_ENTRY_SIZE, 25);
        // Array: type_flags(1) + offset(8) + size(8) = 17
        assert_eq!(ARRAY_INDEX_ENTRY_SIZE, 17);
    }

    #[test]
    fn test_invalid_magic_rejected() {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(b"NOPE");
        assert!(BlobHeader::from_bytes(&buf).is_err());

        buf[0..4].copy_from_slice(b"LARS");
        assert!(BlobHeader::from_bytes(&buf).is_err());
    }
}
