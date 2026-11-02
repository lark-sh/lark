//! Opening a read cache doesn't reserve room for its whole capacity. Every open
//! blob has one, so an up-front table costs each loaded database memory before
//! it reads anything.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use lark_blob::cached_io::CachedIO;
use lark_blob::io::MemBlobIO;

struct Counting;

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

#[test]
fn a_new_cache_reserves_little() {
    let io = MemBlobIO::new();
    let before = ALLOCATED.load(Ordering::Relaxed);
    let cache = CachedIO::new(io);
    let reserved = ALLOCATED.load(Ordering::Relaxed) - before;
    drop(cache);
    assert!(
        reserved < 64 * 1024,
        "a new CachedIO allocated {reserved} bytes"
    );
}
