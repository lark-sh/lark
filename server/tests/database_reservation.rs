//! A loaded database's fixed cost, before it holds any data, stays small. Every
//! loaded database pays it, so it multiplies across a server.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use lark_server::db::{Database, Inbox};

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
fn inbox_slots_are_a_pointer() {
    assert_eq!(std::mem::size_of::<Inbox>(), std::mem::size_of::<usize>());
}

#[test]
fn a_new_database_reserves_little() {
    let before = ALLOCATED.load(Ordering::Relaxed);
    let db = Database::new("reserve".to_string(), "test-project".to_string(), true);
    let reserved = ALLOCATED.load(Ordering::Relaxed) - before;
    drop(db);
    assert!(
        reserved < 1024 * 1024,
        "a new Database allocated {reserved} bytes"
    );
}
