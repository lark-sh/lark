//! Byte-level read cache wrapping any BlobIO.
//!
//! `CachedIO<IO>` implements `BlobIO` and transparently serves `pread` calls
//! from cached byte regions when possible. It has no awareness of blob
//! structure — it simply remembers byte ranges that were explicitly cached
//! via `cache_region`.
//!
//! Usage:
//! ```ignore
//! let io = StdBlobIO::open(&path)?;
//! let cached = CachedIO::new(io);
//! // ... pass cached as &IO to session methods ...
//! ```

use crate::io::BlobIO;
use lru::LruCache;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashSet};
use std::io;
use std::num::NonZeroUsize;
use std::rc::Rc;

/// Maximum number of cached regions. When exceeded, the least-recently-used
/// region is evicted. At 4KB per region this caps memory at ~512MB.
/// Set to 0 to disable byte-level caching.
const MAX_CACHED_REGIONS: usize = 128_000;

/// Byte-level read cache wrapping any `BlobIO`.
///
/// All `pread` calls check cached regions first. On miss, delegates to the
/// underlying IO. Cached regions are populated explicitly via `cache_region`.
///
/// Uses a `BTreeMap<u64, Vec<u8>>` keyed by region start offset for O(log n)
/// lookup: find the largest key ≤ requested offset, check if the region covers
/// the requested range.
///
/// `pwrite` is write-through: writes go to the underlying IO first, then
/// any overlapping cached regions are patched in-place to stay coherent.
///
/// Eviction is LRU: least-recently-used region is evicted first when at capacity.
/// An `LruCache<u64, ()>` tracks access order in parallel with the BTreeMap.
/// Regions are "touched" on every pread hit and cache_region hit, so frequently
/// accessed containers (root, top-level) stay hot even when thousands of leaf
/// containers flow through the cache.
///
/// The cache is `Rc<RefCell<...>>` so `clone_for_reading` shares the same
/// cache — a read-only clone sees all regions the original has populated.
/// Uses `RefCell` for interior mutability so `pread(&self)` and
/// `cache_region(&self)` can access the cache without `&mut self`.
/// Safe because borrows never span `.await` points.
pub struct CachedIO<IO: BlobIO> {
    inner: IO,
    /// Cached byte regions keyed by start offset. O(log n) lookup via BTreeMap.
    /// Shared via Rc so clone_for_reading inherits the cache.
    regions: Rc<RefCell<BTreeMap<u64, Vec<u8>>>>,
    /// LRU eviction tracker. Parallel to `regions` — same keys, no values.
    /// Shared via Rc with clone_for_reading.
    lru_order: Rc<RefCell<LruCache<u64, ()>>>,
    /// Read stats: actual I/O (cache misses).
    /// Shared via Rc so clone_for_reading shares counters.
    pread_count: Rc<Cell<u64>>,
    bytes_read: Rc<Cell<u64>>,
    /// Read stats: cache hits.
    cache_hits: Rc<Cell<u64>>,
    cache_hit_bytes: Rc<Cell<u64>>,
    /// cache_region misses — header had to be fetched from disk (CephFS round-trip).
    cache_header_misses: Rc<Cell<u64>>,
    /// Write-back mode: when enabled, pwrite to cached regions updates the
    /// cache without writing to disk. Flushed at end of batch.
    write_back: Cell<bool>,
    /// Cache region offsets that have been modified in write-back mode and
    /// need to be flushed to the underlying IO.
    dirty_regions: Rc<RefCell<HashSet<u64>>>,
}

impl<IO: BlobIO + Clone> Clone for CachedIO<IO> {
    fn clone(&self) -> Self {
        CachedIO::new(self.inner.clone())
    }
}

impl<IO: BlobIO> CachedIO<IO> {
    /// Wrap an existing BlobIO with a read cache.
    pub fn new(inner: IO) -> Self {
        let cap = if MAX_CACHED_REGIONS > 0 {
            NonZeroUsize::new(MAX_CACHED_REGIONS).unwrap()
        } else {
            NonZeroUsize::new(1).unwrap() // LruCache requires non-zero cap
        };
        CachedIO {
            inner,
            regions: Rc::new(RefCell::new(BTreeMap::new())),
            lru_order: Rc::new(RefCell::new(LruCache::new(cap))),
            pread_count: Rc::new(Cell::new(0)),
            bytes_read: Rc::new(Cell::new(0)),
            cache_hits: Rc::new(Cell::new(0)),
            cache_hit_bytes: Rc::new(Cell::new(0)),
            cache_header_misses: Rc::new(Cell::new(0)),
            write_back: Cell::new(false),
            dirty_regions: Rc::new(RefCell::new(HashSet::new())),
        }
    }

    /// Unwrap, returning the underlying IO.
    pub fn into_inner(self) -> IO {
        self.inner
    }

    /// Borrow the underlying IO.
    pub fn inner(&self) -> &IO {
        &self.inner
    }

    /// Mutably borrow the underlying IO.
    pub fn inner_mut(&mut self) -> &mut IO {
        &mut self.inner
    }

    /// Number of cached regions (for testing).
    #[cfg(test)]
    fn region_count(&self) -> usize {
        self.regions.borrow().len()
    }
}

/// Check if a BTreeMap region covers the range [offset, offset+len).
/// Returns `(region_start_offset, data_slice)` if covered, so the caller
/// can touch the region in the LRU.
fn find_covering(
    regions: &BTreeMap<u64, Vec<u8>>,
    offset: u64,
    len: usize,
) -> Option<(u64, &[u8])> {
    // Find the last region that starts at or before `offset`
    let (&region_offset, data) = regions.range(..=offset).next_back()?;
    let region_end = region_offset + data.len() as u64;
    let read_end = offset + len as u64;
    if read_end <= region_end {
        let start = (offset - region_offset) as usize;
        // Defensive: verify slice bounds before indexing. The arithmetic
        // check above should guarantee this, but if stale cached data
        // causes callers to pass unexpected offsets, we fall through to
        // the underlying IO rather than panicking.
        if start + len > data.len() {
            return None;
        }
        Some((region_offset, &data[start..start + len]))
    } else {
        None
    }
}

/// Overlay cached regions into a read buffer after a cache miss.
///
/// When a read spans multiple small cached regions (e.g., a 4MB chunk that
/// covers many individually-cached items), `find_covering` returns None
/// because no single region covers the entire read. After reading from disk,
/// this function patches the buffer with any overlapping cached regions.
///
/// This is critical for write-back correctness: cached regions may contain
/// pwrite modifications not yet flushed to disk. Without this overlay,
/// reads that span multiple cached regions return stale disk data.
fn overlay_from_regions(regions: &BTreeMap<u64, Vec<u8>>, read_offset: u64, buf: &mut [u8]) {
    let read_end = read_offset + buf.len() as u64;

    let scan_from = regions.range(..=read_offset).next_back().map(|(&k, _)| k);
    let start = scan_from.unwrap_or(read_offset);

    for (&region_offset, region_data) in regions.range(start..read_end) {
        let region_end = region_offset + region_data.len() as u64;
        if region_end <= read_offset {
            continue;
        }

        let overlap_start = read_offset.max(region_offset);
        let overlap_end = read_end.min(region_end);

        let buf_start = (overlap_start - read_offset) as usize;
        let buf_end = (overlap_end - read_offset) as usize;
        let region_start = (overlap_start - region_offset) as usize;
        let region_end_local = (overlap_end - region_offset) as usize;

        buf[buf_start..buf_end].copy_from_slice(&region_data[region_start..region_end_local]);
    }
}

/// Patch all regions that overlap [write_offset, write_offset + write_data.len()).
fn patch_regions(regions: &mut BTreeMap<u64, Vec<u8>>, write_offset: u64, write_data: &[u8]) {
    let write_end = write_offset + write_data.len() as u64;

    // Jump directly to the first potentially overlapping region: the last one
    // starting at or before write_offset. This is O(log n) instead of scanning
    // all regions from the start of the BTreeMap.
    let scan_from = regions.range(..=write_offset).next_back().map(|(&k, _)| k);
    let start = scan_from.unwrap_or(write_offset);

    for (&region_offset, data) in regions.range_mut(start..write_end) {
        let region_end = region_offset + data.len() as u64;
        if region_end <= write_offset {
            continue; // the "before" region doesn't reach our write
        }

        let overlap_start = write_offset.max(region_offset);
        let overlap_end = write_end.min(region_end);

        let region_local_start = (overlap_start - region_offset) as usize;
        let region_local_end = (overlap_end - region_offset) as usize;
        let write_local_start = (overlap_start - write_offset) as usize;
        let write_local_end = (overlap_end - write_offset) as usize;

        data[region_local_start..region_local_end]
            .copy_from_slice(&write_data[write_local_start..write_local_end]);
    }
}

#[cfg(test)]
impl<IO: BlobIO> CachedIO<IO> {
    /// Debug: verify every cached region matches what's on disk.
    /// Only call when NOT in write-back mode (dirty regions won't match disk).
    // The borrow is held across `await`, but this is single-threaded debug-only
    // code and the regions map is never re-borrowed during the read.
    #[allow(clippy::await_holding_refcell_ref)]
    pub async fn verify_cache_consistency(&self) -> bool {
        if self.write_back.get() {
            eprintln!("[CACHE-CHECK] skipping — write-back mode is active");
            return true;
        }
        let regions = self.regions.borrow();
        let mut all_ok = true;
        for (&offset, cached_data) in regions.iter() {
            let len = cached_data.len();
            match self.inner.pread(offset, len).await {
                Ok(disk_data) => {
                    for i in 0..len {
                        if cached_data[i] != disk_data[i] {
                            eprintln!(
                                "[CACHE-MISMATCH] offset={}, byte_pos={}, cached=0x{:02x}, disk=0x{:02x}",
                                offset, i, cached_data[i], disk_data[i]
                            );
                            all_ok = false;
                            break;
                        }
                    }
                }
                Err(e) => {
                    eprintln!(
                        "[CACHE-CHECK] error reading disk at offset={}, len={}: {:?}",
                        offset, len, e
                    );
                    all_ok = false;
                }
            }
        }
        all_ok
    }
}

impl<IO: BlobIO> BlobIO for CachedIO<IO> {
    async fn pread(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        // O(log n) lookup in BTreeMap
        {
            let regions = self.regions.borrow();
            if let Some((region_offset, slice)) = find_covering(&regions, offset, len) {
                let result = slice.to_vec();
                drop(regions); // release before borrowing lru_order
                self.lru_order.borrow_mut().promote(&region_offset);
                self.cache_hits.set(self.cache_hits.get() + 1);
                self.cache_hit_bytes
                    .set(self.cache_hit_bytes.get() + len as u64);
                return Ok(result);
            }
        } // release borrow before async call

        self.pread_count.set(self.pread_count.get() + 1);
        self.bytes_read.set(self.bytes_read.get() + len as u64);
        let mut data = self.inner.pread(offset, len).await?;

        // Overlay any cached regions that partially overlap this read.
        // Cached regions may contain write-back modifications not yet on disk.
        {
            let regions = self.regions.borrow();
            overlay_from_regions(&regions, offset, &mut data);
        }

        Ok(data)
    }

    async fn pread_into(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        // O(log n) lookup — copy directly from cache into caller's buffer, no Vec
        {
            let regions = self.regions.borrow();
            if let Some((region_offset, slice)) = find_covering(&regions, offset, buf.len()) {
                buf.copy_from_slice(slice);
                drop(regions); // release before borrowing lru_order
                self.lru_order.borrow_mut().promote(&region_offset);
                self.cache_hits.set(self.cache_hits.get() + 1);
                self.cache_hit_bytes
                    .set(self.cache_hit_bytes.get() + buf.len() as u64);
                return Ok(());
            }
        } // release borrow before async call

        self.pread_count.set(self.pread_count.get() + 1);
        self.bytes_read
            .set(self.bytes_read.get() + buf.len() as u64);
        self.inner.pread_into(offset, buf).await?;

        // Overlay any cached regions that partially overlap this read.
        // Cached regions may contain write-back modifications not yet on disk.
        {
            let regions = self.regions.borrow();
            overlay_from_regions(&regions, offset, buf);
        }

        Ok(())
    }

    // The `regions` borrow is explicitly dropped before any `await`, so it is
    // never actually held across a suspend point; clippy can't see the `drop`.
    #[allow(clippy::await_holding_refcell_ref)]
    async fn pwrite(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        if self.write_back.get() {
            // Write-back mode: if a cached region covers this write, patch it
            // in-place (flushed later by flush_write_back). Otherwise write
            // directly to disk (normal for compaction writes to EOF, etc.).
            let mut regions = self.regions.borrow_mut();
            let write_end = offset + data.len() as u64;
            let mut fully_covered = false;
            let mut any_overlap = false;

            // Jump directly to the first potentially overlapping region.
            let scan_from = regions.range(..=offset).next_back().map(|(&k, _)| k);
            let start = scan_from.unwrap_or(offset);

            for (&region_offset, region_data) in regions.range_mut(start..write_end) {
                let region_end = region_offset + region_data.len() as u64;
                if region_end <= offset {
                    continue;
                }

                any_overlap = true;
                if region_offset <= offset && region_end >= write_end {
                    fully_covered = true;
                }

                let overlap_start = offset.max(region_offset);
                let overlap_end = write_end.min(region_end);

                let region_local_start = (overlap_start - region_offset) as usize;
                let region_local_end = (overlap_end - region_offset) as usize;
                let write_local_start = (overlap_start - offset) as usize;
                let write_local_end = (overlap_end - offset) as usize;

                region_data[region_local_start..region_local_end]
                    .copy_from_slice(&data[write_local_start..write_local_end]);

                self.dirty_regions.borrow_mut().insert(region_offset);
            }

            if !fully_covered {
                if any_overlap {
                    // Partial overlap: write crosses cached region boundaries.
                    // This should not happen — it indicates a write is stomping
                    // over cached container headers.
                    eprintln!(
                        "[ERROR] pwrite partial overlap with cache: offset={} len={}",
                        offset,
                        data.len()
                    );
                }
                // No cached region (or partial overlap) — write to disk.
                drop(regions);
                self.inner.pwrite(offset, data).await?;
            }
        } else {
            // Write-through to underlying IO
            self.inner.pwrite(offset, data).await?;

            // Patch overlapping cached regions (shared with clones)
            patch_regions(&mut self.regions.borrow_mut(), offset, data);
        }

        Ok(())
    }

    // The `regions` borrow is explicitly dropped before any `await`, so it is
    // never actually held across a suspend point; clippy can't see the `drop`.
    #[allow(clippy::await_holding_refcell_ref)]
    async fn pwrite_deferred(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        if self.write_back.get() {
            // Write-back mode: patch the cached region covering this write.
            // pwrite_deferred is used for parent index entry updates, which
            // always target bytes within cached container headers.
            let mut regions = self.regions.borrow_mut();
            let write_end = offset + data.len() as u64;
            let mut fully_covered = false;
            let mut any_overlap = false;

            let scan_from = regions.range(..=offset).next_back().map(|(&k, _)| k);
            let start = scan_from.unwrap_or(offset);

            for (&region_offset, region_data) in regions.range_mut(start..write_end) {
                let region_end = region_offset + region_data.len() as u64;
                if region_end <= offset {
                    continue;
                }

                any_overlap = true;
                if region_offset <= offset && region_end >= write_end {
                    fully_covered = true;
                }

                let overlap_start = offset.max(region_offset);
                let overlap_end = write_end.min(region_end);

                let region_local_start = (overlap_start - region_offset) as usize;
                let region_local_end = (overlap_end - region_offset) as usize;
                let write_local_start = (overlap_start - offset) as usize;
                let write_local_end = (overlap_end - offset) as usize;

                region_data[region_local_start..region_local_end]
                    .copy_from_slice(&data[write_local_start..write_local_end]);

                self.dirty_regions.borrow_mut().insert(region_offset);
            }

            if !fully_covered {
                if any_overlap {
                    // Partial overlap on a deferred write — this should not
                    // happen. Deferred writes target index entries within
                    // cached container headers.
                    eprintln!(
                        "[ERROR] pwrite_deferred partial overlap with cache: offset={} len={}",
                        offset,
                        data.len()
                    );
                }
                drop(regions);
                self.inner.pwrite(offset, data).await?;
            }
        } else {
            // Not in write-back mode — same as pwrite
            self.inner.pwrite(offset, data).await?;
            patch_regions(&mut self.regions.borrow_mut(), offset, data);
        }

        Ok(())
    }

    async fn append(&self, data: &[u8]) -> io::Result<u64> {
        self.inner.append(data).await
    }

    async fn sync(&self) -> io::Result<()> {
        // In write-back mode, skip per-update syncs. flush_write_back does
        // one sync before flushing headers and one after.
        if self.write_back.get() {
            return Ok(());
        }
        self.inner.sync().await
    }

    async fn size(&self) -> io::Result<u64> {
        self.inner.size().await
    }

    async fn truncate(&self, new_size: u64) -> io::Result<()> {
        let to_remove: Vec<u64>;
        {
            let mut regions = self.regions.borrow_mut();

            // Collect keys to remove (regions fully beyond new_size)
            to_remove = regions.range(new_size..).map(|(&k, _)| k).collect();
            for k in &to_remove {
                regions.remove(k);
            }

            // Trim regions that partially extend past new_size
            for (&region_offset, data) in regions.range_mut(..new_size) {
                let region_end = region_offset + data.len() as u64;
                if region_end > new_size {
                    let keep = (new_size - region_offset) as usize;
                    data.truncate(keep);
                }
            }
        }

        // Clean up LRU
        if !to_remove.is_empty() {
            let mut lru = self.lru_order.borrow_mut();
            for k in &to_remove {
                lru.pop(k);
            }
        }

        self.inner.truncate(new_size).await
    }

    async fn clone_for_reading(&self) -> io::Result<Self> {
        Ok(CachedIO {
            inner: self.inner.clone_for_reading().await?,
            regions: Rc::clone(&self.regions),
            lru_order: Rc::clone(&self.lru_order),
            pread_count: Rc::clone(&self.pread_count),
            bytes_read: Rc::clone(&self.bytes_read),
            cache_hits: Rc::clone(&self.cache_hits),
            cache_hit_bytes: Rc::clone(&self.cache_hit_bytes),
            cache_header_misses: Rc::clone(&self.cache_header_misses),
            write_back: Cell::new(false),
            dirty_regions: Rc::clone(&self.dirty_regions),
        })
    }

    async fn close(self) -> io::Result<()> {
        self.inner.close().await
    }

    async fn yield_point(&self) {
        self.inner.yield_point().await
    }

    async fn cache_region(&self, offset: u64, len: usize) -> io::Result<()> {
        if MAX_CACHED_REGIONS == 0 {
            return Ok(());
        }
        {
            let regions = self.regions.borrow();
            // Already covered? Touch it in LRU so it stays hot.
            if let Some((region_offset, _)) = find_covering(&regions, offset, len) {
                drop(regions);
                self.lru_order.borrow_mut().promote(&region_offset);
                return Ok(());
            }
        }

        // Header miss — this region wasn't cached, going to disk.
        self.cache_header_misses
            .set(self.cache_header_misses.get() + 1);

        // Evict least-recently-used regions until we're under capacity.
        // Skip eviction entirely in write-back mode — evicting a dirty
        // region would lose deferred writes.
        if !self.write_back.get() {
            let mut regions = self.regions.borrow_mut();
            let mut lru = self.lru_order.borrow_mut();
            let dirty = self.dirty_regions.borrow();
            while regions.len() >= MAX_CACHED_REGIONS {
                if let Some((lru_offset, _)) = lru.pop_lru() {
                    // Don't evict dirty regions — they have unflushed writes
                    if dirty.contains(&lru_offset) {
                        lru.put(lru_offset, ()); // put it back
                        break; // stop evicting to avoid infinite loop
                    }
                    regions.remove(&lru_offset);
                } else {
                    break;
                }
            }
        }

        self.pread_count.set(self.pread_count.get() + 1);
        self.bytes_read.set(self.bytes_read.get() + len as u64);
        let mut data = self.inner.pread(offset, len).await?;

        // Before inserting, overlay any existing dirty cached regions onto
        // the freshly-read data. pwrite_deferred may have created small
        // cache entries (e.g., a 17-byte index entry update) that this
        // larger region would otherwise stomp over with stale disk data.
        let read_end = offset + len as u64;
        let mut new_is_dirty = false;
        {
            let regions = self.regions.borrow();
            let dirty = self.dirty_regions.borrow();

            let scan_from = regions.range(..=offset).next_back().map(|(&k, _)| k);
            let start = scan_from.unwrap_or(offset);

            for (&region_offset, region_data) in regions.range(start..read_end) {
                let region_end = region_offset + region_data.len() as u64;
                if region_end <= offset {
                    continue;
                }
                if dirty.contains(&region_offset) {
                    // Overlay dirty data onto the freshly-read buffer
                    let overlap_start = offset.max(region_offset);
                    let overlap_end = read_end.min(region_end);
                    let buf_start = (overlap_start - offset) as usize;
                    let buf_end = (overlap_end - offset) as usize;
                    let region_start = (overlap_start - region_offset) as usize;
                    let region_end_local = (overlap_end - region_offset) as usize;
                    data[buf_start..buf_end]
                        .copy_from_slice(&region_data[region_start..region_end_local]);
                    new_is_dirty = true;
                }
            }
        }

        // Remove any subsumed regions and their dirty tracking
        {
            let mut regions = self.regions.borrow_mut();
            let subsumed: Vec<u64> = regions
                .range(offset..read_end)
                .filter(|(k, v)| **k >= offset && **k + v.len() as u64 <= read_end)
                .map(|(k, _)| *k)
                .collect();
            let mut dirty_mut = self.dirty_regions.borrow_mut();
            for key in subsumed {
                regions.remove(&key);
                dirty_mut.remove(&key);
            }
            if new_is_dirty {
                dirty_mut.insert(offset);
            }
            regions.insert(offset, data);
        }
        self.lru_order.borrow_mut().put(offset, ());

        Ok(())
    }

    async fn clear_read_cache(&self) {
        self.regions.borrow_mut().clear();
        self.lru_order.borrow_mut().clear();
        self.dirty_regions.borrow_mut().clear();
    }

    fn clear_region(&self, offset: u64, len: u64) {
        let end = offset + len;
        let mut regions = self.regions.borrow_mut();
        let mut dirty = self.dirty_regions.borrow_mut();
        let mut lru = self.lru_order.borrow_mut();

        let scan_from = regions.range(..offset).next_back().map(|(&k, _)| k);
        let start = scan_from.unwrap_or(offset);

        let to_remove: Vec<u64> = regions
            .range(start..end)
            .filter_map(|(&k, v)| {
                let k_end = k + v.len() as u64;
                if k_end > offset { Some(k) } else { None }
            })
            .collect();

        for k in &to_remove {
            regions.remove(k);
            dirty.remove(k);
            lru.pop(k);
        }
    }

    fn set_write_back(&self, enabled: bool) {
        self.write_back.set(enabled);
    }

    async fn flush_write_back(&self) -> io::Result<()> {
        // Collect all dirty region data, sorted by offset
        let mut dirty_data: Vec<(u64, Vec<u8>)> = {
            let dirty = self.dirty_regions.borrow();
            let regions = self.regions.borrow();
            dirty
                .iter()
                .filter_map(|&offset| regions.get(&offset).map(|data| (offset, data.clone())))
                .collect()
        };
        dirty_data.sort_by_key(|(offset, _)| *offset);

        // Merge truly adjacent regions (where one ends exactly where the next starts)
        let mut merged: Vec<(u64, Vec<u8>)> = Vec::new();
        for (offset, data) in dirty_data {
            if let Some((prev_off, prev_data)) = merged.last_mut()
                && *prev_off + prev_data.len() as u64 == offset
            {
                prev_data.extend_from_slice(&data);
                continue;
            }
            merged.push((offset, data));
        }

        // Sync all appended data to disk before writing headers that reference it.
        // This ensures that if a reader sees an updated header, the data it
        // points to is already on disk.
        self.inner.sync().await?;

        for (offset, data) in merged {
            self.inner.pwrite(offset, &data).await?;
        }

        // Sync the headers to disk so readers see a consistent state.
        self.inner.sync().await?;

        self.dirty_regions.borrow_mut().clear();
        self.write_back.set(false);
        Ok(())
    }

    fn discard_write_back(&self) {
        self.regions.borrow_mut().clear();
        self.lru_order.borrow_mut().clear();
        self.dirty_regions.borrow_mut().clear();
        self.write_back.set(false);
    }

    fn take_read_stats(&self) -> crate::io::ReadStats {
        crate::io::ReadStats {
            pread_count: self.pread_count.replace(0),
            bytes_read: self.bytes_read.replace(0),
            cache_hits: self.cache_hits.replace(0),
            cache_hit_bytes: self.cache_hit_bytes.replace(0),
            cache_header_misses: self.cache_header_misses.replace(0),
        }
    }

    async fn open_related(&self, name: &str) -> io::Result<Self> {
        let inner = self.inner.open_related(name).await?;
        Ok(CachedIO::new(inner))
    }

    async fn create_related(&self, name: &str) -> io::Result<Self> {
        let inner = self.inner.create_related(name).await?;
        Ok(CachedIO::new(inner))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arc_value::ArcValue;
    use crate::io::MemBlobIO;
    use crate::session::{ApplyResult, BlobSession};
    use serde_json::json;

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        futures::executor::block_on(f)
    }

    #[test]
    fn test_basic_cache_hit() {
        block_on(async {
            let mem = MemBlobIO::new();
            mem.append(b"hello world! this is test data.")
                .await
                .unwrap();

            let cached = CachedIO::new(mem);

            // Cache a region
            cached.cache_region(0, 12).await.unwrap();

            // Read from cached region — should hit cache
            let data = cached.pread(0, 5).await.unwrap();
            assert_eq!(&data, b"hello");

            let data = cached.pread(6, 6).await.unwrap();
            assert_eq!(&data, b"world!");

            // Read outside cached region — falls through to inner IO
            let data = cached.pread(13, 4).await.unwrap();
            assert_eq!(&data, b"this");
        });
    }

    #[test]
    fn test_write_through_patches_cache() {
        block_on(async {
            let mem = MemBlobIO::new();
            mem.append(b"hello world").await.unwrap();

            let cached = CachedIO::new(mem);

            // Cache the full region
            cached.cache_region(0, 11).await.unwrap();

            // Verify initial read
            assert_eq!(cached.pread(0, 11).await.unwrap(), b"hello world");

            // Write through — should patch cache
            cached.pwrite(6, b"WORLD").await.unwrap();

            // Read from cache — should see patched data
            assert_eq!(cached.pread(0, 11).await.unwrap(), b"hello WORLD");

            // Verify underlying IO was also updated
            assert_eq!(cached.inner().pread(0, 11).await.unwrap(), b"hello WORLD");
        });
    }

    #[test]
    fn test_cache_region_dedup() {
        if MAX_CACHED_REGIONS == 0 {
            return;
        }
        block_on(async {
            let mem = MemBlobIO::new();
            mem.append(b"abcdefghij").await.unwrap();

            let cached = CachedIO::new(mem);

            // Cache same region twice — should only store once
            cached.cache_region(0, 10).await.unwrap();
            cached.cache_region(0, 10).await.unwrap();

            assert_eq!(cached.region_count(), 1);
        });
    }

    #[test]
    fn test_clear_read_cache() {
        if MAX_CACHED_REGIONS == 0 {
            return;
        }
        block_on(async {
            let mem = MemBlobIO::new();
            mem.append(b"abcdefghij").await.unwrap();

            let cached = CachedIO::new(mem);
            cached.cache_region(0, 10).await.unwrap();
            assert_eq!(cached.region_count(), 1);

            cached.clear_read_cache().await;
            assert_eq!(cached.region_count(), 0);
        });
    }

    #[test]
    fn test_truncate_invalidates_cache() {
        block_on(async {
            let mem = MemBlobIO::new();
            mem.append(b"abcdefghij").await.unwrap();

            let cached = CachedIO::new(mem);
            cached.cache_region(0, 10).await.unwrap();

            // Truncate to 5 bytes — cached chunk should be trimmed
            cached.truncate(5).await.unwrap();

            // Reading the first 5 bytes should still work (from trimmed cache)
            assert_eq!(cached.pread(0, 5).await.unwrap(), b"abcde");

            // Reading beyond should fail
            assert!(cached.pread(0, 10).await.is_err());
        });
    }

    #[test]
    fn test_partial_write_overlap() {
        block_on(async {
            let mem = MemBlobIO::new();
            mem.append(b"0123456789").await.unwrap();

            let cached = CachedIO::new(mem);

            // Cache region [2..8)
            cached.cache_region(2, 6).await.unwrap();
            assert_eq!(cached.pread(2, 6).await.unwrap(), b"234567");

            // Write that partially overlaps the cached region
            cached.pwrite(5, b"XYZ").await.unwrap();

            // Cached region should be patched: "234XYZ" (bytes 5,6,7 overwritten)
            // Original: offset=2, data="234567"
            // Write at 5: overwrites positions 5,6,7 → "234XYZ"
            assert_eq!(cached.pread(2, 6).await.unwrap(), b"234XYZ");
        });
    }

    #[test]
    fn test_append_does_not_affect_cache() {
        block_on(async {
            let mem = MemBlobIO::new();
            mem.append(b"hello").await.unwrap();

            let cached = CachedIO::new(mem);
            cached.cache_region(0, 5).await.unwrap();

            // Append new data
            let offset = cached.append(b" world").await.unwrap();
            assert_eq!(offset, 5);

            // Cached region still valid
            assert_eq!(cached.pread(0, 5).await.unwrap(), b"hello");

            // New data readable via fallthrough
            assert_eq!(cached.pread(5, 6).await.unwrap(), b" world");
        });
    }

    // ---- Integration tests: full session workflow through CachedIO ----

    /// Helper: apply updates through CachedIO.
    async fn apply_cached(
        session: &mut BlobSession<CachedIO<MemBlobIO>>,
        updates: &[(Vec<String>, Option<ArcValue>)],
    ) -> crate::incremental::IncrementalStats {
        match session.apply_updates(updates).await.unwrap() {
            ApplyResult::Applied(stats) => stats,
        }
    }

    #[test]
    fn test_session_init_and_read_through_cached_io() {
        block_on(async {
            let io = CachedIO::new(MemBlobIO::new());
            let session = BlobSession::init(io.clone()).await.unwrap();

            // Root is an empty object
            let root = session.read_subtree(&[]).await.unwrap();
            assert!(root.get("anything").is_none());
            assert_eq!(session.header().node_count, 1);
        });
    }

    #[test]
    fn test_session_apply_updates_through_cached_io() {
        block_on(async {
            let io = CachedIO::new(MemBlobIO::new());
            let mut session = BlobSession::init(io.clone()).await.unwrap();

            let updates = vec![
                (vec!["hp".to_string()], Some(ArcValue::from(100i64))),
                (vec!["name".to_string()], Some(ArcValue::from("Hero"))),
            ];
            let stats = apply_cached(&mut session, &updates).await;
            assert_eq!(stats.updates_applied, 2);

            // Read back
            let root = session.read_subtree(&[]).await.unwrap();
            assert_eq!(root.get("hp").unwrap().as_i64(), Some(100));
            assert_eq!(root.get("name").unwrap().as_str(), Some("Hero"));
        });
    }

    #[test]
    fn test_session_nested_updates_through_cached_io() {
        block_on(async {
            let io = CachedIO::new(MemBlobIO::new());
            let mut session = BlobSession::init(io.clone()).await.unwrap();

            // Create nested structure
            let updates = vec![
                (
                    vec![
                        "characters".to_string(),
                        "abc".to_string(),
                        "hp".to_string(),
                    ],
                    Some(ArcValue::from(100i64)),
                ),
                (
                    vec![
                        "characters".to_string(),
                        "abc".to_string(),
                        "name".to_string(),
                    ],
                    Some(ArcValue::from("Hero")),
                ),
                (
                    vec!["config".to_string(), "mode".to_string()],
                    Some(ArcValue::from("dark")),
                ),
            ];
            apply_cached(&mut session, &updates).await;

            // Read subtree
            let chars = session.read_subtree(&["characters", "abc"]).await.unwrap();
            assert_eq!(chars.get("hp").unwrap().as_i64(), Some(100));
            assert_eq!(chars.get("name").unwrap().as_str(), Some("Hero"));

            // Update in place — this exercises cache coherence (pwrite patches cached bytes)
            let updates2 = vec![(
                vec![
                    "characters".to_string(),
                    "abc".to_string(),
                    "hp".to_string(),
                ],
                Some(ArcValue::from(200i64)),
            )];
            let stats = apply_cached(&mut session, &updates2).await;
            assert_eq!(stats.in_place_updates, 1);

            // Read back updated value
            let chars = session.read_subtree(&["characters", "abc"]).await.unwrap();
            assert_eq!(chars.get("hp").unwrap().as_i64(), Some(200));
        });
    }

    #[test]
    fn test_session_multiple_batches_through_cached_io() {
        block_on(async {
            let io = CachedIO::new(MemBlobIO::new());
            let mut session = BlobSession::init(io.clone()).await.unwrap();

            // Batch 1: create structure
            let updates1 = vec![(vec!["score".to_string()], Some(ArcValue::from(0i64)))];
            apply_cached(&mut session, &updates1).await;

            // Batch 2: update + add new field (tests dict caching across batches)
            let updates2 = vec![
                (vec!["score".to_string()], Some(ArcValue::from(42i64))),
                (vec!["level".to_string()], Some(ArcValue::from(5i64))),
            ];
            apply_cached(&mut session, &updates2).await;

            // Batch 3: more updates
            let updates3 = vec![(vec!["score".to_string()], Some(ArcValue::from(99i64)))];
            apply_cached(&mut session, &updates3).await;

            let root = session.read_subtree(&[]).await.unwrap();
            assert_eq!(root.get("score").unwrap().as_i64(), Some(99));
            assert_eq!(root.get("level").unwrap().as_i64(), Some(5));
        });
    }

    #[test]
    fn test_session_delete_through_cached_io() {
        block_on(async {
            let io = CachedIO::new(MemBlobIO::new());
            let mut session = BlobSession::init(io.clone()).await.unwrap();

            let updates = vec![
                (vec!["a".to_string()], Some(ArcValue::from(1i64))),
                (vec!["b".to_string()], Some(ArcValue::from(2i64))),
            ];
            apply_cached(&mut session, &updates).await;

            // Delete "a"
            let deletes = vec![(vec!["a".to_string()], None)];
            apply_cached(&mut session, &deletes).await;

            let root = session.read_subtree(&[]).await.unwrap();
            assert!(root.get("a").is_none());
            assert_eq!(root.get("b").unwrap().as_i64(), Some(2));
        });
    }

    #[test]
    fn test_write_blob_and_open_through_cached_io() {
        block_on(async {
            use crate::writer::write_blob;

            let tree = ArcValue::from_value(json!({
                "characters": {
                    "abc": {"hp": 100, "name": "Hero"},
                },
                "config": {"mode": "dark"}
            }));

            // Write blob through CachedIO
            let io = CachedIO::new(MemBlobIO::new());
            write_blob(&io, &tree).await.unwrap();

            // Open and read through CachedIO
            let session = BlobSession::open(io.clone()).await.unwrap();
            let result = session.read_subtree(&[]).await.unwrap();
            assert_eq!(result, tree);

            // Navigate to a nested path
            let hp = session
                .read_subtree(&["characters", "abc", "hp"])
                .await
                .unwrap();
            assert_eq!(hp.as_i64(), Some(100));
        });
    }

    #[test]
    fn test_write_back_defers_writes_then_flushes() {
        block_on(async {
            let mem = MemBlobIO::new();
            mem.append(b"hello world!!!!!").await.unwrap(); // 16 bytes

            let cached = CachedIO::new(mem);

            // Cache the region
            cached.cache_region(0, 16).await.unwrap();
            assert_eq!(cached.pread(0, 16).await.unwrap(), b"hello world!!!!!");

            // Enable write-back
            cached.set_write_back(true);

            // pwrite to cached region — should NOT write to underlying IO
            cached.pwrite(0, b"HELLO").await.unwrap();

            // Cache should have the updated data
            assert_eq!(cached.pread(0, 16).await.unwrap(), b"HELLO world!!!!!");

            // Underlying IO should still have old data
            assert_eq!(
                cached.inner().pread(0, 16).await.unwrap(),
                b"hello world!!!!!"
            );

            // Do another pwrite to the same region
            cached.pwrite(6, b"WORLD").await.unwrap();
            assert_eq!(cached.pread(0, 16).await.unwrap(), b"HELLO WORLD!!!!!");
            assert_eq!(
                cached.inner().pread(0, 16).await.unwrap(),
                b"hello world!!!!!"
            );

            // Flush — should write dirty regions to disk
            cached.flush_write_back().await.unwrap();
            assert_eq!(
                cached.inner().pread(0, 16).await.unwrap(),
                b"HELLO WORLD!!!!!"
            );

            // Write-back mode should be disabled after flush
            assert!(!cached.write_back.get());
            assert!(cached.dirty_regions.borrow().is_empty());
        });
    }

    #[test]
    fn test_write_back_uncached_region_writes_through() {
        block_on(async {
            let mem = MemBlobIO::new();
            mem.append(b"abcdefghijklmnop").await.unwrap();

            let cached = CachedIO::new(mem);

            // Cache only the first 8 bytes
            cached.cache_region(0, 8).await.unwrap();

            // Enable write-back
            cached.set_write_back(true);

            // pwrite to uncached region (offset 10) — should go to disk directly
            cached.pwrite(10, b"XY").await.unwrap();
            assert_eq!(cached.inner().pread(10, 2).await.unwrap(), b"XY");

            // pwrite to cached region — should NOT go to disk
            cached.pwrite(0, b"AB").await.unwrap();
            assert_eq!(cached.inner().pread(0, 2).await.unwrap(), b"ab"); // old data
            assert_eq!(cached.pread(0, 2).await.unwrap(), b"AB"); // cache has new data

            cached.flush_write_back().await.unwrap();
            assert_eq!(cached.inner().pread(0, 2).await.unwrap(), b"AB"); // now flushed
        });
    }

    #[test]
    fn test_discard_write_back_clears_all_state() {
        block_on(async {
            let mem = MemBlobIO::new();
            mem.append(b"hello world!!!!!").await.unwrap();

            let cached = CachedIO::new(mem);

            // Cache a region and enable write-back
            cached.cache_region(0, 16).await.unwrap();
            cached.set_write_back(true);

            // Make some dirty writes
            cached.pwrite(0, b"HELLO").await.unwrap();
            cached.pwrite_deferred(6, b"WORLD").await.unwrap();

            // Verify dirty state exists
            assert!(cached.write_back.get());
            assert!(!cached.dirty_regions.borrow().is_empty());
            assert!(!cached.regions.borrow().is_empty());

            // Cache says "HELLO WORLD!!!!!" but disk says "hello world!!!!!"
            assert_eq!(cached.pread(0, 16).await.unwrap(), b"HELLO WORLD!!!!!");
            assert_eq!(
                cached.inner().pread(0, 16).await.unwrap(),
                b"hello world!!!!!"
            );

            // Discard — should clear everything without flushing
            cached.discard_write_back();

            assert!(!cached.write_back.get());
            assert!(cached.dirty_regions.borrow().is_empty());
            assert!(cached.regions.borrow().is_empty());

            // Disk should still have original data (dirty writes discarded)
            assert_eq!(
                cached.inner().pread(0, 16).await.unwrap(),
                b"hello world!!!!!"
            );

            // Reads now go to disk, seeing original data
            assert_eq!(cached.pread(0, 16).await.unwrap(), b"hello world!!!!!");
        });
    }

    #[test]
    fn test_failed_batch_doesnt_corrupt_next_batch() {
        // Simulate the bug: a failed apply_updates batch leaves dirty cache
        // state. Without the fix, the next successful batch would flush those
        // stale dirty regions, permanently corrupting the blob.
        //
        // Strategy: truncate the underlying MemBlobIO (bypassing CachedIO) so
        // reads past the truncation point fail with UnexpectedEof. This IO
        // error propagates unconditionally from all code paths, unlike
        // NotAContainer which navigate_to_depth catches gracefully.
        block_on(async {
            let io = CachedIO::new(MemBlobIO::new());
            let mut session = BlobSession::init(io.clone()).await.unwrap();

            // Batch 1: create substantial data so the blob is large enough
            // that truncation will destroy some of it.
            let mut updates1: Vec<(Vec<String>, Option<ArcValue>)> = Vec::new();
            for i in 0..20 {
                let key = format!("-u{}", i);
                let val = format!("user-{}-{}", i, "x".repeat(100));
                updates1.push((
                    vec!["users".to_string(), key, "name".to_string()],
                    Some(ArcValue::from(val.as_str())),
                ));
            }
            updates1.push((
                vec!["config".to_string(), "version".to_string()],
                Some(ArcValue::from(1i64)),
            ));
            apply_cached(&mut session, &updates1).await;

            // Verify batch 1 data
            let u0 = session
                .read_subtree(&["users", "-u0", "name"])
                .await
                .unwrap();
            assert!(u0.as_str().unwrap().starts_with("user-0-"));
            let ver = session.read_subtree(&["config", "version"]).await.unwrap();
            assert_eq!(ver.as_i64(), Some(1));

            // Save a snapshot of the blob data before corruption.
            let blob_size = session.io.inner().size().await.unwrap();
            let saved_data = session
                .io
                .inner()
                .pread(0, blob_size as usize)
                .await
                .unwrap();

            // Truncate the underlying IO to just the header (64 bytes).
            // The root_offset in the header still points past 64 bytes,
            // so any navigation will fail with UnexpectedEof — an IO error
            // that propagates unconditionally from all code paths.
            // Must clear the SESSION's cache (not the test's `io` clone,
            // which has a separate cache due to CachedIO::clone).
            session.io.clear_read_cache().await;
            session.io.inner().truncate(64).await.unwrap();

            // Batch 2: try to update — should fail due to truncated data
            let updates2 = vec![
                (
                    vec!["config".to_string(), "version".to_string()],
                    Some(ArcValue::from(2i64)),
                ),
                (
                    vec!["users".to_string(), "-u0".to_string(), "name".to_string()],
                    Some(ArcValue::from("updated")),
                ),
            ];
            let result = session.apply_updates(&updates2).await;
            assert!(result.is_err(), "batch 2 should fail due to truncation");

            // Restore the blob from the saved snapshot. This simulates
            // the scenario where the blob was temporarily unreadable but
            // is now fine (e.g., a transient IO error, or the data was
            // on a briefly-unavailable NAS mount).
            session.io.inner().truncate(0).await.unwrap();
            session.io.inner().append(&saved_data).await.unwrap();
            session.io.clear_read_cache().await;

            // KEY TEST: Batch 3 applies a valid batch on the same session.
            // Without the fix, the stale dirty cache from the failed batch 2
            // would be flushed during batch 3's flush_write_back, corrupting
            // the restored blob. With the fix, the dirty cache was discarded
            // when batch 2 failed, so batch 3 starts clean.
            let updates3 = vec![(
                vec!["config".to_string(), "version".to_string()],
                Some(ArcValue::from(3i64)),
            )];
            let result3 = session.apply_updates(&updates3).await;
            assert!(
                result3.is_ok(),
                "batch 3 should succeed: {:?}",
                result3.err()
            );

            // Verify batch 3's update was applied
            let config_ver = session.read_subtree(&["config", "version"]).await.unwrap();
            assert_eq!(config_ver.as_i64(), Some(3));

            // Verify pre-failure data is still intact (batch 2's partial
            // writes were NOT flushed)
            let u0 = session
                .read_subtree(&["users", "-u0", "name"])
                .await
                .unwrap();
            assert!(
                u0.as_str().unwrap().starts_with("user-0-"),
                "pre-failure data should be intact, got: {:?}",
                u0
            );
        });
    }

    #[test]
    fn test_failed_batch_preserves_session_header() {
        // After a failed batch, the session header (root_offset, total_size)
        // must still reflect the pre-batch state, not some intermediate state.
        block_on(async {
            let io = CachedIO::new(MemBlobIO::new());
            let mut session = BlobSession::init(io.clone()).await.unwrap();

            // Apply a successful batch to establish baseline state
            let updates = vec![
                (vec!["a".to_string()], Some(ArcValue::from(1i64))),
                (vec!["b".to_string()], Some(ArcValue::from(2i64))),
            ];
            apply_cached(&mut session, &updates).await;
            let root_offset_before = session.header().root_offset;

            // Truncate the underlying IO to cause the next batch to fail.
            // Must use session.io (not the test's `io` clone) because
            // CachedIO::clone creates a separate cache.
            session.io.clear_read_cache().await;
            session.io.inner().truncate(64).await.unwrap();

            // Try a batch that should fail
            let updates2 = vec![(
                vec!["a".to_string()],
                Some(ArcValue::from(
                    "this is a much longer string value to trigger forwarding".to_string(),
                )),
            )];
            let result = session.apply_updates(&updates2).await;

            // Whether or not it failed, verify the session header is intact
            assert_eq!(
                session.header().root_offset,
                root_offset_before,
                "root_offset must be preserved after failed batch"
            );

            if result.is_err() {
                eprintln!("batch correctly failed, header preserved");
            }
        });
    }
}
