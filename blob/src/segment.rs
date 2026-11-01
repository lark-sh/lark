//! Sidecar persistence for blob files (free list + pending keys).
//!
//! Each blob file has a companion sidecar containing its free list and
//! pending dictionary keys.
//!
//! A sidecar describes the blob as of one WAL sequence, and is written before
//! that sequence commits. Until `sequence` reaches the same number, the blob may
//! still reference regions this free list calls free, so a sidecar only counts
//! when its sequence matches. Any other sidecar is ignored, which leaks the free
//! space it listed until the next full compaction.
//!
//! During a flush the sidecar also carries the batch's in-place writes, in a
//! section after the main one. Those writes span many pages and a crash can land
//! any of them, so they reach this file first. If a flush is interrupted,
//! [`crate::session::finish_interrupted_flush`] completes it from here. A finished
//! flush truncates the section away, so at rest a sidecar is just the main section.
//!
//! Format v8:
//!   Main section:
//!   [magic "LRKF"] [version u32=8] [sequence i64] [main_len u64]
//!   [count u64] [bytes_freed u64] [bytes_reused u64] [bytes_wasted u64]
//!   [(offset u64, size u64) × count]
//!   [pending_key_count: u32]
//!   [(key_len: u16, key_bytes: [u8; key_len]) × pending_key_count]
//!   [checksum u64]  xxh64 of the section before it
//!
//!   Flush section, when a flush is in progress:
//!   [magic "LRKW"] [blob_size u64] [flush_count u32]
//!   [(offset u64, len u32, bytes: [u8; len]) × flush_count]
//!   [checksum u64]  xxh64 of the section before it

use crate::free_list::FreeList;
use std::io;
use xxhash_rust::xxh64::xxh64;

const VERSION: u32 = 8;
/// Magic, version, sequence, main section length, region count and the three
/// byte counters.
const HEADER_SIZE: usize = 56;
const CHECKSUM_SIZE: usize = 8;

/// The in-place writes of one batch, recorded before any of them reach the blob.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PendingFlush {
    /// Blob size when the flush started. Everything below it was already synced.
    pub blob_size: u64,
    /// (offset, bytes), in the order they're written.
    pub regions: Vec<(u64, Vec<u8>)>,
}

impl PendingFlush {
    /// The sidecar's flush section. Empty if there's nothing to record.
    pub fn to_bytes(&self) -> Vec<u8> {
        if self.regions.is_empty() {
            return Vec::new();
        }
        let mut buf = Vec::new();
        buf.extend_from_slice(b"LRKW");
        buf.extend_from_slice(&self.blob_size.to_le_bytes());
        buf.extend_from_slice(&(self.regions.len() as u32).to_le_bytes());
        for (offset, data) in &self.regions {
            buf.extend_from_slice(&offset.to_le_bytes());
            buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
            buf.extend_from_slice(data);
        }
        let checksum = xxh64(&buf, 0);
        buf.extend_from_slice(&checksum.to_le_bytes());
        buf
    }

    /// Reads a flush section. None if there isn't a whole one: it's synced before
    /// the first in-place write, so a partial section means none of them started.
    fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < CHECKSUM_SIZE {
            return None;
        }
        let (body, checksum) = data.split_at(data.len() - CHECKSUM_SIZE);
        if xxh64(body, 0).to_le_bytes() != checksum {
            return None;
        }
        let mut reader = Reader { data: body, pos: 0 };
        if reader.bytes(4).ok()? != b"LRKW" {
            return None;
        }
        let blob_size = reader.u64().ok()?;
        let count = reader.u32().ok()? as usize;
        let mut regions = Vec::new();
        for _ in 0..count {
            let offset = reader.u64().ok()?;
            let len = reader.u32().ok()? as usize;
            regions.push((offset, reader.bytes(len).ok()?.to_vec()));
        }
        Some(PendingFlush { blob_size, regions })
    }
}

/// Sidecar data: free list state, pending dictionary keys, and any flush in progress.
pub struct Sidecar {
    /// The WAL sequence this sidecar describes the blob at.
    pub sequence: i64,
    pub free_list: FreeList,
    /// Non-collection keys written inline during incremental updates.
    /// Accumulated across batches, drained into the dictionary during root_compact.
    pub pending_keys: Vec<String>,
    /// The in-place writes of a flush that hasn't finished.
    pub flush: PendingFlush,
}

impl Sidecar {
    /// Create a sidecar with no pending keys.
    pub fn new(free_list: FreeList, sequence: i64) -> Self {
        Sidecar {
            sequence,
            free_list,
            pending_keys: Vec::new(),
            flush: PendingFlush::default(),
        }
    }

    /// The sidecar's main section, from borrowed components.
    pub fn serialize(free_list: &FreeList, pending_keys: &[String], sequence: i64) -> Vec<u8> {
        let regions = free_list.all_regions();
        let region_count = regions.len();

        let mut buf = Vec::with_capacity(HEADER_SIZE + region_count * 16 + 4 + CHECKSUM_SIZE);

        // Header
        buf.extend_from_slice(b"LRKF");
        buf.extend_from_slice(&VERSION.to_le_bytes());
        buf.extend_from_slice(&sequence.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes()); // main_len, filled in below
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

        let main_len = (buf.len() + CHECKSUM_SIZE) as u64;
        buf[16..24].copy_from_slice(&main_len.to_le_bytes());

        let checksum = xxh64(&buf, 0);
        buf.extend_from_slice(&checksum.to_le_bytes());
        buf
    }

    /// Serialize the sidecar to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Self::serialize(&self.free_list, &self.pending_keys, self.sequence);
        buf.extend_from_slice(&self.flush.to_bytes());
        buf
    }

    /// Deserialize a sidecar from bytes.
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

        if version != VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported sidecar version {}", version),
            ));
        }
        if data.len() < HEADER_SIZE + CHECKSUM_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "sidecar too short",
            ));
        }

        let main_len = u64::from_le_bytes(data[16..24].try_into().unwrap());
        let main_len = usize::try_from(main_len)
            .ok()
            .filter(|len| (HEADER_SIZE + CHECKSUM_SIZE..=data.len()).contains(len))
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "sidecar length out of range")
            })?;
        let (main, flush) = data.split_at(main_len);

        // A crash while the sidecar is being replaced can leave part of it.
        let (body, checksum) = main.split_at(main.len() - CHECKSUM_SIZE);
        if xxh64(body, 0).to_le_bytes() != checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "sidecar checksum mismatch",
            ));
        }
        let mut sidecar = Self::parse_body(body)?;
        sidecar.flush = PendingFlush::from_bytes(flush).unwrap_or_default();
        Ok(sidecar)
    }

    /// Parses the main section, up to its checksum.
    fn parse_body(data: &[u8]) -> io::Result<Self> {
        let sequence = i64::from_le_bytes(data[8..16].try_into().unwrap());
        let count = u64::from_le_bytes(data[24..32].try_into().unwrap()) as usize;
        let bytes_freed = u64::from_le_bytes(data[32..40].try_into().unwrap());
        let bytes_reused = u64::from_le_bytes(data[40..48].try_into().unwrap());
        let bytes_wasted = u64::from_le_bytes(data[48..56].try_into().unwrap());

        // `count` is an attacker-controlled u64. Compute the region table's end
        // with checked arithmetic: `HEADER_SIZE + count * 16` overflows usize for large
        // counts, which would either panic (overflow-checks builds) or wrap to a
        // small value that passes the truncation check and then panics on an
        // out-of-bounds slice in the loop below. Folding the bounds check into
        // the same chain rejects both overflow and truncation, bounding `count`
        // by the input length.
        let regions_end = count
            .checked_mul(16)
            .and_then(|n| n.checked_add(HEADER_SIZE))
            .filter(|&end| end <= data.len())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "sidecar regions truncated")
            })?;

        let mut free_list = FreeList::new();
        free_list.bytes_freed = bytes_freed;
        free_list.bytes_reused = bytes_reused;
        free_list.bytes_wasted = bytes_wasted;

        for i in 0..count {
            let base = HEADER_SIZE + i * 16;
            let offset = u64::from_le_bytes(data[base..base + 8].try_into().unwrap());
            let size = u64::from_le_bytes(data[base + 8..base + 16].try_into().unwrap());
            // A region whose end overflows u64 is impossible on disk. Reject the
            // whole sidecar rather than feed it to the free list: restore_region
            // would `offset + size` and either panic or (in release) wrap to a
            // bogus end, mis-merging regions and later handing out overlapping
            // space (write corruption). On Err the caller falls back to an empty
            // free list — a space leak, not corruption.
            if offset.checked_add(size).is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "sidecar region offset+size overflows",
                ));
            }
            free_list.restore_region(offset, size);
        }

        let mut reader = Reader {
            data,
            pos: regions_end,
        };

        // Pending keys
        let key_count = reader.u32()? as usize;
        let mut pending_keys = Vec::new();
        for _ in 0..key_count {
            let key_len = reader.u16()? as usize;
            if let Ok(key) = std::str::from_utf8(reader.bytes(key_len)?) {
                pending_keys.push(key.to_string());
            }
        }

        Ok(Sidecar {
            sequence,
            free_list,
            pending_keys,
            flush: PendingFlush::default(),
        })
    }
}

/// Reads fixed-size fields, failing at the end of the data.
struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn bytes(&mut self, len: usize) -> io::Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .filter(|&end| end <= self.data.len());
        let end =
            end.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "sidecar truncated"))?;
        let bytes = &self.data[self.pos..end];
        self.pos = end;
        Ok(bytes)
    }

    fn u16(&mut self) -> io::Result<u16> {
        Ok(u16::from_le_bytes(self.bytes(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.bytes(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.bytes(8)?.try_into().unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A sidecar body with the given region count, sealed with a valid checksum
    /// so parsing gets as far as the region table.
    fn sealed(count: u64, regions: &[(u64, u64)]) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(b"LRKF");
        data.extend_from_slice(&VERSION.to_le_bytes());
        data.extend_from_slice(&0i64.to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes()); // main_len
        data.extend_from_slice(&count.to_le_bytes());
        data.extend_from_slice(&[0u8; 24]); // bytes_freed/reused/wasted
        for (offset, size) in regions {
            data.extend_from_slice(&offset.to_le_bytes());
            data.extend_from_slice(&size.to_le_bytes());
        }
        data.extend_from_slice(&0u32.to_le_bytes()); // pending keys
        let main_len = (data.len() + CHECKSUM_SIZE) as u64;
        data[16..24].copy_from_slice(&main_len.to_le_bytes());
        let checksum = xxh64(&data, 0);
        data.extend_from_slice(&checksum.to_le_bytes());
        data
    }

    #[test]
    fn test_sidecar_roundtrip_empty() {
        let sidecar = Sidecar::new(FreeList::new(), 7);
        let bytes = sidecar.to_bytes();

        assert_eq!(u32::from_le_bytes(bytes[4..8].try_into().unwrap()), VERSION);

        let restored = Sidecar::from_bytes(&bytes).unwrap();
        assert_eq!(restored.sequence, 7);
        assert!(restored.pending_keys.is_empty());
        assert_eq!(restored.free_list.available_region_count(), 0);
        assert_eq!(restored.free_list.bytes_freed, 0);
    }

    /// C1 regression (fuzz_sidecar): a region `count` large enough that the
    /// region table's end overflows usize previously panicked (overflow-checks)
    /// or wrapped past the truncation check and then panicked on an OOB slice.
    /// `from_bytes` must return an error instead of panicking.
    #[test]
    fn test_from_bytes_rejects_overflowing_region_count() {
        assert!(Sidecar::from_bytes(&sealed(u64::MAX, &[])).is_err());
    }

    /// A region whose offset+size overflows u64 must be rejected, not fed to the
    /// free list (where `offset + size` would panic or wrap into a mis-merge).
    #[test]
    fn test_from_bytes_rejects_region_offset_size_overflow() {
        assert!(Sidecar::from_bytes(&sealed(1, &[(u64::MAX, 1)])).is_err());
    }

    /// A plausible-but-truncated count (no overflow, just not enough bytes) is
    /// also rejected rather than read out of bounds.
    #[test]
    fn test_from_bytes_rejects_truncated_regions() {
        assert!(Sidecar::from_bytes(&sealed(1000, &[])).is_err());
    }

    /// Partly written sidecars are rejected, whatever got cut or changed.
    #[test]
    fn test_from_bytes_rejects_a_damaged_sidecar() {
        let mut fl = FreeList::new();
        fl.free(1000, 8192);
        fl.advance_epoch();
        fl.advance_epoch();
        let bytes = Sidecar::new(fl, 3).to_bytes();
        assert!(Sidecar::from_bytes(&bytes).is_ok());

        for cut in 0..bytes.len() {
            assert!(Sidecar::from_bytes(&bytes[..cut]).is_err(), "cut at {cut}");
        }
        for at in 0..bytes.len() {
            let mut changed = bytes.clone();
            changed[at] ^= 0x40;
            assert!(Sidecar::from_bytes(&changed).is_err(), "byte {at}");
        }
    }

    #[test]
    fn test_sidecar_roundtrip_with_free_list() {
        let mut fl = FreeList::new();
        fl.free(1000, 8192);
        fl.free(20000, 16384);
        fl.advance_epoch();
        fl.advance_epoch();
        fl.allocate(8192); // move some stats

        let sidecar = Sidecar::new(fl, 0);
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
    fn test_sidecar_with_pending_keys() {
        let sidecar = Sidecar {
            sequence: 0,
            free_list: FreeList::new(),
            pending_keys: vec!["hp".to_string(), "name".to_string(), "level".to_string()],
            flush: PendingFlush::default(),
        };
        let bytes = sidecar.to_bytes();

        let restored = Sidecar::from_bytes(&bytes).unwrap();
        assert_eq!(restored.pending_keys, vec!["hp", "name", "level"]);
    }

    #[test]
    fn test_sidecar_with_pending_flush() {
        let flush = PendingFlush {
            blob_size: 9000,
            regions: vec![(64, vec![1, 2, 3]), (8000, vec![4; 900])],
        };
        let sidecar = Sidecar {
            sequence: 5,
            free_list: FreeList::new(),
            pending_keys: vec!["hp".to_string()],
            flush: flush.clone(),
        };

        let restored = Sidecar::from_bytes(&sidecar.to_bytes()).unwrap();
        assert_eq!(restored.flush, flush);
        assert_eq!(restored.pending_keys, vec!["hp"]);
    }

    /// The flush section is only used whole, and losing it leaves the main section.
    #[test]
    fn test_partial_flush_section_is_ignored() {
        let flush = PendingFlush {
            blob_size: 9000,
            regions: vec![(64, vec![1, 2, 3])],
        };
        let mut sidecar = Sidecar::new(FreeList::new(), 5);
        let main_len = sidecar.to_bytes().len();
        sidecar.flush = flush;
        let bytes = sidecar.to_bytes();

        for cut in main_len..bytes.len() {
            let restored = Sidecar::from_bytes(&bytes[..cut]).unwrap();
            assert_eq!(restored.sequence, 5);
            assert_eq!(restored.flush, PendingFlush::default(), "cut at {cut}");
        }
    }

    #[test]
    fn test_sidecar_bad_magic() {
        let bytes = b"NOPE\x08\x00\x00\x00";
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
        for version in 1u32..VERSION {
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
