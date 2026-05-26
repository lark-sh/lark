#![allow(async_fn_in_trait)]

pub mod arc_value;
pub mod cached_io;
pub mod compact;
pub mod dictionary;
pub mod error;
pub mod format;
pub mod free_list;
pub mod incremental;
pub mod io;
pub mod nav_cache;
pub mod segment;
pub mod session;
pub mod session_incremental;
pub mod session_reader;
pub mod session_writer;
pub mod test_helpers;
pub mod writer;

// Public API re-exports
pub use arc_value::ArcValue;
pub use cached_io::CachedIO;
pub use compact::full_compact;
pub use error::{BlobError, Result};
pub use format::{BlobHeader, FieldIdSize};
pub use free_list::FreeList;
pub use incremental::IncrementalStats;
pub use io::{BlobIO, MemBlobIO, ReadStats, StdBlobIO};
pub use session::{ApplyResult, BlobSession, ShallowChild, ShallowValue};
pub use session_reader::BlobLocation;
pub use writer::{BlobStats, write_blob};
