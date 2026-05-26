//! Sidecar persistence for blob files (free list + pending keys).
//!
//! Each blob file has a companion sidecar containing its free list and
//! pending dictionary keys.
//!
//! Format v7:
//!   [magic "LRKF"] [version u32=7]
//!   [count u64] [bytes_freed u64] [bytes_reused u64] [bytes_wasted u64]
//!   [(offset u64, size u64) × count]
//!   [pending_key_count: u32]
//!   [(key_len: u16, key_bytes: [u8; key_len]) × pending_key_count]

use crate::free_list::FreeList;
use std::io;

/// Sidecar data: free list state + pending dictionary keys.
pub struct Sidecar {
    pub free_list: FreeList,
    /// Non-collection keys written inline during incremental updates.
    /// Accumulated across batches, drained into the dictionary during root_compact.
    pub pending_keys: Vec<String>,
}

impl Sidecar {
    /// Create a sidecar with no pending keys.
    pub fn new(free_list: FreeList) -> Self {
        Sidecar {
            free_list,
            pending_keys: Vec::new(),
        }
    }

    /// Serialize a sidecar to bytes from borrowed components (v7 format).
    pub fn serialize(free_list: &FreeList, pending_keys: &[String]) -> Vec<u8> {
        let regions = free_list.all_regions();
        let region_count = regions.len();

        let mut buf = Vec::with_capacity(40 + region_count * 16 + 4);

        // Header
        buf.extend_from_slice(b"LRKF");
        buf.extend_from_slice(&7u32.to_le_bytes());
        buf.extend_from_slice(&(region_count as u64).to_le_bytes());
        buf.extend_from_slice(&free_list.bytes_freed.to_le_bytes());
        buf.extend_from_slice(&free_list.bytes_reused.to_le_bytes());
        buf.extend_from_slice(&free_list.bytes_wasted.to_le_bytes());

        // Free list regions (offset, size)
        for &(offset, size) in &regions {
            buf.extend_from_slice(&offset.to_le_bytes());
            buf.extend_from_slice(&size.to_le_bytes());
        }

        // Pending keys trailer
        buf.extend_from_slice(&(pending_keys.len() as u32).to_le_bytes());
        for key in pending_keys {
            let kb = key.as_bytes();
            buf.extend_from_slice(&(kb.len() as u16).to_le_bytes());
            buf.extend_from_slice(kb);
        }

        buf
    }

    /// Serialize the sidecar to bytes (v7 format).
    pub fn to_bytes(&self) -> Vec<u8> {
        Self::serialize(&self.free_list, &self.pending_keys)
    }

    /// Deserialize a sidecar from bytes. Supports v7.
    pub fn from_bytes(data: &[u8]) -> io::Result<Self> {
        if data.len() < 8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "sidecar too short",
            ));
        }
        if &data[0..4] != b"LRKF" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad sidecar magic",
            ));
        }
        let version = u32::from_le_bytes(data[4..8].try_into().unwrap());

        match version {
            7 => Self::parse_v7(data),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "unsupported sidecar version {} (only v7 supported)",
                    version
                ),
            )),
        }
    }

    /// Parse v7 format: flat free list + pending keys.
    fn parse_v7(data: &[u8]) -> io::Result<Self> {
        if data.len() < 40 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "sidecar too short",
            ));
        }

        let count = u64::from_le_bytes(data[8..16].try_into().unwrap()) as usize;
        let bytes_freed = u64::from_le_bytes(data[16..24].try_into().unwrap());
        let bytes_reused = u64::from_le_bytes(data[24..32].try_into().unwrap());
        let bytes_wasted = u64::from_le_bytes(data[32..40].try_into().unwrap());

        let regions_end = 40 + count * 16;
        if data.len() < regions_end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "sidecar regions truncated",
            ));
        }

        let mut free_list = FreeList::new();
        free_list.bytes_freed = bytes_freed;
        free_list.bytes_reused = bytes_reused;
        free_list.bytes_wasted = bytes_wasted;

        for i in 0..count {
            let base = 40 + i * 16;
            let offset = u64::from_le_bytes(data[base..base + 8].try_into().unwrap());
            let size = u64::from_le_bytes(data[base + 8..base + 16].try_into().unwrap());
            free_list.restore_region(offset, size);
        }

        // Parse pending keys trailer
        let mut pos = regions_end;
        let mut pending_keys = Vec::new();
        if data.len() >= pos + 4 {
            let key_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            for _ in 0..key_count {
                if pos + 2 > data.len() {
                    break;
                }
                let key_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
                pos += 2;
                if pos + key_len > data.len() {
                    break;
                }
                if let Ok(key) = std::str::from_utf8(&data[pos..pos + key_len]) {
                    pending_keys.push(key.to_string());
                }
                pos += key_len;
            }
        }

        Ok(Sidecar {
            free_list,
            pending_keys,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sidecar_v7_roundtrip_empty() {
        let sidecar = Sidecar::new(FreeList::new());
        let bytes = sidecar.to_bytes();

        assert_eq!(u32::from_le_bytes(bytes[4..8].try_into().unwrap()), 7);

        let restored = Sidecar::from_bytes(&bytes).unwrap();
        assert!(restored.pending_keys.is_empty());
        assert_eq!(restored.free_list.available_region_count(), 0);
        assert_eq!(restored.free_list.bytes_freed, 0);
    }

    #[test]
    fn test_sidecar_v7_roundtrip_with_free_list() {
        let mut fl = FreeList::new();
        fl.free(1000, 8192);
        fl.free(20000, 16384);
        fl.advance_epoch();
        fl.advance_epoch();
        fl.allocate(8192); // move some stats

        let sidecar = Sidecar {
            free_list: fl,
            pending_keys: Vec::new(),
        };
        let bytes = sidecar.to_bytes();

        let restored = Sidecar::from_bytes(&bytes).unwrap();
        assert_eq!(
            restored.free_list.bytes_freed,
            sidecar.free_list.bytes_freed
        );
        assert_eq!(
            restored.free_list.bytes_reused,
            sidecar.free_list.bytes_reused
        );
        assert_eq!(
            restored.free_list.bytes_wasted,
            sidecar.free_list.bytes_wasted
        );
        assert!(restored.pending_keys.is_empty());
    }

    #[test]
    fn test_sidecar_v7_with_pending_keys() {
        let sidecar = Sidecar {
            free_list: FreeList::new(),
            pending_keys: vec!["hp".to_string(), "name".to_string(), "level".to_string()],
        };
        let bytes = sidecar.to_bytes();

        let restored = Sidecar::from_bytes(&bytes).unwrap();
        assert_eq!(restored.pending_keys, vec!["hp", "name", "level"]);
    }

    #[test]
    fn test_sidecar_bad_magic() {
        let bytes = b"NOPE\x07\x00\x00\x00";
        assert!(Sidecar::from_bytes(bytes).is_err());
    }

    #[test]
    fn test_sidecar_unsupported_version() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"LRKF");
        bytes.extend_from_slice(&99u32.to_le_bytes());
        assert!(Sidecar::from_bytes(&bytes).is_err());
    }

    #[test]
    fn test_sidecar_old_versions_rejected() {
        for version in [1u32, 2, 3, 4, 5, 6] {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(b"LRKF");
            bytes.extend_from_slice(&version.to_le_bytes());
            assert!(
                Sidecar::from_bytes(&bytes).is_err(),
                "v{} should be rejected",
                version
            );
        }
    }
}
