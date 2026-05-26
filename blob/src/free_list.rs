//! Free list for dead space reuse.
//!
//! Maintains a set of freed byte regions and reuses them for new writes,
//! reducing file growth and delaying the need for root compaction (rotation).
//!
//! Uses a two-generation epoch system for concurrent reader safety:
//! - `current`: regions freed during this compaction cycle (not safe to reuse)
//! - `previous`: regions freed during the last cycle (becoming available)
//! - `available`: regions freed ≥2 cycles ago (safe to reuse)
//!
//! At the start of each `apply_updates`, call `advance_epoch()` to rotate
//! previous → available, current → previous. By the time a region reaches
//! `available`, the reader has had time to clear its CachedIO.

use crate::io::BlobIO;
use std::collections::{BTreeMap, BTreeSet};
use std::io;

/// Minimum region size to track. Regions smaller than this are ignored
/// (the bookkeeping overhead exceeds the space savings).
const MIN_FREE_REGION: u64 = 4096;

/// Free list managing dead space regions for reuse.
pub struct FreeList {
    /// Regions safe to reuse, indexed by size for O(log n) best-fit.
    by_size: BTreeMap<u64, BTreeSet<u64>>, // size → set of offsets
    /// Regions safe to reuse, indexed by offset for adjacency merging.
    by_offset: BTreeMap<u64, u64>, // offset → size

    /// Regions freed last cycle (not yet available).
    previous: Vec<(u64, u64)>, // (offset, size)
    /// Regions freed this cycle.
    current: Vec<(u64, u64)>, // (offset, size)

    /// Total bytes freed across all epochs.
    pub bytes_freed: u64,
    /// Total bytes reused (allocated from free list instead of appending).
    pub bytes_reused: u64,
    /// Total bytes of dead space too small to reuse (below MIN_FREE_REGION).
    pub bytes_wasted: u64,
}

impl Default for FreeList {
    fn default() -> Self {
        Self::new()
    }
}

impl FreeList {
    /// Create an empty free list.
    pub fn new() -> Self {
        FreeList {
            by_size: BTreeMap::new(),
            by_offset: BTreeMap::new(),
            previous: Vec::new(),
            current: Vec::new(),
            bytes_freed: 0,
            bytes_reused: 0,
            bytes_wasted: 0,
        }
    }

    /// Record dead space that can't be reused by the free list (e.g.,
    /// inline space interior to a parent container). Only recoverable
    /// by compacting the file.
    pub fn waste(&mut self, size: u64) {
        self.bytes_wasted += size;
    }

    /// Record a freed region.
    pub fn free(&mut self, offset: u64, size: u64) {
        self.bytes_freed += size;
        if size < MIN_FREE_REGION {
            self.bytes_wasted += size;
            return;
        }
        self.current.push((offset, size));
    }

    /// Advance the epoch: previous → available, current → previous.
    /// Returns the regions that were just promoted to available — the caller
    /// should clear these from the IO cache.
    pub fn advance_epoch(&mut self) -> Vec<(u64, u64)> {
        let prev = std::mem::take(&mut self.previous);
        let promoted = prev.clone();
        for (offset, size) in prev {
            self.insert_and_merge(offset, size);
        }
        std::mem::swap(&mut self.previous, &mut self.current);
        promoted
    }

    /// Insert a region into the available maps, merging with overlapping or adjacent regions.
    fn insert_and_merge(&mut self, offset: u64, size: u64) {
        let mut merged_offset = offset;
        let mut merged_end = offset + size;

        // Check for a left neighbor that overlaps or is adjacent
        if let Some((&left_offset, &left_size)) = self.by_offset.range(..offset).next_back()
            && left_offset + left_size >= offset
        {
            merged_offset = left_offset;
            merged_end = merged_end.max(left_offset + left_size);
        }

        // Collect all regions from merged_offset onward that overlap or are adjacent.
        let to_remove: Vec<(u64, u64)> = self
            .by_offset
            .range(merged_offset..)
            .take_while(|&(&k, _)| k <= merged_end)
            .map(|(&k, &v)| (k, v))
            .collect();

        for (k, v) in &to_remove {
            merged_end = merged_end.max(k + v);
            self.remove_from_size_map(*k, *v);
            self.by_offset.remove(k);
        }

        // Insert merged region
        let merged_size = merged_end - merged_offset;
        self.by_offset.insert(merged_offset, merged_size);
        self.by_size
            .entry(merged_size)
            .or_default()
            .insert(merged_offset);
    }

    fn remove_from_size_map(&mut self, offset: u64, size: u64) {
        if let Some(offsets) = self.by_size.get_mut(&size) {
            offsets.remove(&offset);
            if offsets.is_empty() {
                self.by_size.remove(&size);
            }
        }
    }

    /// Try to allocate a region of at least `needed` bytes (best-fit).
    pub fn allocate(&mut self, needed: u64) -> Option<u64> {
        if needed == 0 {
            return None;
        }

        // Find smallest region >= needed
        let (&found_size, offsets) = self.by_size.range(needed..).next()?;
        let &offset = offsets.iter().next()?;

        // Remove from maps
        self.remove_from_size_map(offset, found_size);
        self.by_offset.remove(&offset);

        // Put back remainder if large enough
        let remainder = found_size - needed;
        if remainder >= MIN_FREE_REGION {
            let rem_offset = offset + needed;
            self.by_offset.insert(rem_offset, remainder);
            self.by_size
                .entry(remainder)
                .or_default()
                .insert(rem_offset);
        } else {
            self.bytes_wasted += remainder;
        }

        self.bytes_reused += needed;
        Some(offset)
    }

    /// Write `data` to a free region if one fits, otherwise append to EOF.
    /// Returns the offset where data was written.
    pub async fn write_or_append<IO: BlobIO>(&mut self, io: &IO, data: &[u8]) -> io::Result<u64> {
        let needed = data.len() as u64;
        if let Some(offset) = self.allocate(needed) {
            io.pwrite(offset, data).await?;
            Ok(offset)
        } else {
            io.append(data).await
        }
    }

    /// Reserve space of `size` bytes from a free region if one fits,
    /// otherwise extend the file at EOF.
    /// Returns the offset of the reserved region.
    pub async fn reserve_or_append<IO: BlobIO>(&mut self, io: &IO, size: u64) -> io::Result<u64> {
        if let Some(offset) = self.allocate(size) {
            Ok(offset)
        } else {
            let offset = io.size().await?;
            io.truncate(offset + size).await?;
            Ok(offset)
        }
    }

    /// Collect all regions (available + previous + current) as a flat list.
    pub fn all_regions(&self) -> Vec<(u64, u64)> {
        let total = self.by_offset.len() + self.previous.len() + self.current.len();
        let mut regions = Vec::with_capacity(total);
        for (&offset, &size) in &self.by_offset {
            regions.push((offset, size));
        }
        for &(offset, size) in &self.previous {
            regions.push((offset, size));
        }
        for &(offset, size) in &self.current {
            regions.push((offset, size));
        }
        regions
    }

    /// Insert a region directly into the available maps (with merge).
    /// Used during sidecar restore — all regions go straight to available
    /// since there are no concurrent readers at startup.
    pub fn restore_region(&mut self, offset: u64, size: u64) {
        self.insert_and_merge(offset, size);
    }

    /// Clear all free list state. Called after rotation to a new file.
    pub fn reset(&mut self) {
        self.by_size.clear();
        self.by_offset.clear();
        self.previous.clear();
        self.current.clear();
        self.bytes_wasted = 0;
    }

    /// Number of available regions (for stats/testing).
    pub fn available_region_count(&self) -> usize {
        self.by_offset.len()
    }

    /// Total number of tracked regions across all epochs.
    pub fn total_region_count(&self) -> usize {
        self.by_offset.len() + self.previous.len() + self.current.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_free_list_basic_allocate() {
        let mut fl = FreeList::new();

        // Free a region, advance twice to make it available
        fl.free(1000, 8192);
        fl.advance_epoch(); // current → previous
        fl.advance_epoch(); // previous → available

        assert_eq!(fl.available_region_count(), 1);

        // Allocate a region that fits
        let offset = fl.allocate(8192);
        assert_eq!(offset, Some(1000));
        assert_eq!(fl.available_region_count(), 0);
    }

    #[test]
    fn test_free_list_best_fit() {
        let mut fl = FreeList::new();

        // Free two regions of different sizes
        fl.free(1000, 8192);
        fl.free(20000, 16384);
        fl.advance_epoch();
        fl.advance_epoch();

        // Allocate should return the smallest fit
        let offset = fl.allocate(8192);
        assert_eq!(offset, Some(1000));

        // Next allocate gets the larger region
        let offset = fl.allocate(8192);
        assert_eq!(offset, Some(20000));
    }

    #[test]
    fn test_free_list_split_remainder() {
        let mut fl = FreeList::new();

        // Free a large region
        fl.free(1000, 20000);
        fl.advance_epoch();
        fl.advance_epoch();

        // Allocate a smaller chunk — remainder should go back
        let offset = fl.allocate(8192);
        assert_eq!(offset, Some(1000));
        assert_eq!(fl.available_region_count(), 1);

        // The remainder at offset 9192, size 11808
        let offset2 = fl.allocate(8192);
        assert_eq!(offset2, Some(9192));
    }

    #[test]
    fn test_free_list_adjacent_merge() {
        let mut fl = FreeList::new();

        // Free two adjacent regions
        fl.free(1000, 5000);
        fl.free(6000, 5000);
        fl.advance_epoch();
        fl.advance_epoch();

        // Should be merged into one region of size 10000
        assert_eq!(fl.available_region_count(), 1);
        let offset = fl.allocate(10000);
        assert_eq!(offset, Some(1000));
    }

    #[test]
    fn test_free_list_min_region_filter() {
        let mut fl = FreeList::new();

        // Free a tiny region — should be ignored (counted as wasted)
        fl.free(1000, 100);
        fl.advance_epoch();
        fl.advance_epoch();

        assert_eq!(fl.available_region_count(), 0);
        assert_eq!(fl.allocate(100), None);
        assert_eq!(fl.bytes_wasted, 100);
    }

    #[test]
    fn test_free_list_epoch_safety() {
        let mut fl = FreeList::new();

        // Free a region in current cycle
        fl.free(1000, 8192);

        // Not yet available (still in current)
        assert_eq!(fl.allocate(8192), None);

        // After one advance, in previous — still not available
        fl.advance_epoch();
        assert_eq!(fl.allocate(8192), None);

        // After second advance, now available
        fl.advance_epoch();
        assert_eq!(fl.allocate(8192), Some(1000));
    }

    #[test]
    fn test_free_list_reset() {
        let mut fl = FreeList::new();

        fl.free(1000, 8192);
        fl.advance_epoch();
        fl.advance_epoch();
        assert_eq!(fl.available_region_count(), 1);

        fl.reset();
        assert_eq!(fl.available_region_count(), 0);
        assert_eq!(fl.allocate(8192), None);
    }

    #[test]
    fn test_free_list_stats() {
        let mut fl = FreeList::new();

        fl.free(1000, 8192);
        assert_eq!(fl.bytes_freed, 8192);

        fl.advance_epoch();
        fl.advance_epoch();

        fl.allocate(4096);
        assert_eq!(fl.bytes_reused, 4096);
    }

    #[test]
    fn test_free_list_allocate_small_from_large() {
        let mut fl = FreeList::new();

        fl.free(1000, 8192);
        fl.advance_epoch();
        fl.advance_epoch();

        // Small allocation should carve from the large free region
        assert_eq!(fl.allocate(100), Some(1000));
        // Remainder (8092) is >= MIN_FREE_REGION, so it's still available
        assert_eq!(fl.available_region_count(), 1);
        // Can allocate from the remainder
        assert_eq!(fl.allocate(4096), Some(1100));
    }

    #[test]
    fn test_free_list_three_way_merge() {
        let mut fl = FreeList::new();

        // Free left and right regions first
        fl.free(1000, 5000);
        fl.free(11000, 5000);
        fl.advance_epoch();
        fl.advance_epoch();
        // Two separate regions
        assert_eq!(fl.available_region_count(), 2);

        // Now free the middle region that bridges them
        fl.free(6000, 5000);
        fl.advance_epoch();
        fl.advance_epoch();
        // Should be merged into one region: [1000, 16000)
        assert_eq!(fl.available_region_count(), 1);
        let offset = fl.allocate(15000);
        assert_eq!(offset, Some(1000));
    }

    #[test]
    fn test_free_list_split_remainder_too_small() {
        let mut fl = FreeList::new();

        // Free a region that's only slightly larger than the request
        fl.free(1000, 5000);
        fl.advance_epoch();
        fl.advance_epoch();

        // Allocate 4500 — remainder is 500, below MIN_FREE_REGION
        let offset = fl.allocate(4500);
        assert_eq!(offset, Some(1000));
        // Remainder too small to track
        assert_eq!(fl.available_region_count(), 0);
    }

    #[test]
    fn test_free_list_advance_epoch_returns_promoted() {
        let mut fl = FreeList::new();

        fl.free(1000, 8192);
        fl.free(5000, 16384);
        fl.advance_epoch(); // current → previous

        fl.free(3000, 4096);
        let promoted = fl.advance_epoch(); // previous → available

        assert_eq!(promoted.len(), 2);
        assert!(promoted.contains(&(1000, 8192)));
        assert!(promoted.contains(&(5000, 16384)));
    }

    #[test]
    fn test_free_list_restore_region() {
        let mut fl = FreeList::new();

        fl.restore_region(1000, 8192);
        fl.restore_region(100000, 16384);

        assert_eq!(fl.available_region_count(), 2);
        assert_eq!(fl.allocate(8192), Some(1000));
        assert_eq!(fl.allocate(8192), Some(100000));
    }
}
