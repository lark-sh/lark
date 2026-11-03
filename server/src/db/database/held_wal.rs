//! WAL entries a database holds until compaction puts them in the blob.
//!
//! Each entry is held as the JSON line written to the WAL file. Parsed values
//! took about four times as much memory. A line is parsed only when something
//! needs the entry: replaying writes over data read from the blob, or checking
//! for a pending delete. Both go through the path index, so they parse only the
//! entries that affect the path in question.

use std::collections::{BTreeMap, VecDeque};

use lark_blob::wal::{WalEntry, WalOp};
use serde::Serialize;
use serde_json::Value;
use tracing::error;

use super::normalize_path_key;
use super::subscribe::stores_nothing;

/// Short lines are packed into blocks of this size, so a growing file doesn't
/// keep reallocating one large buffer with up to twice the space it needs.
const BLOCK_BYTES: usize = 64 * 1024;

/// A line longer than this gets an allocation of its own, sized to fit, so it
/// doesn't strand the rest of a block.
const OWN_BLOCK_BYTES: usize = BLOCK_BYTES / 4;

/// The JSON line for a WAL entry, without its newline. It's what `WalEntry`
/// serializes to, built from borrowed parts so a write isn't copied first.
pub(super) fn wal_line<V: Serialize>(
    op: &WalOp,
    path: &str,
    value: Option<&V>,
) -> serde_json::Result<Vec<u8>> {
    #[derive(Serialize)]
    struct Line<'a, V: Serialize> {
        #[serde(rename = "o")]
        op: &'a WalOp,
        #[serde(rename = "p")]
        path: &'a str,
        #[serde(rename = "v", skip_serializing_if = "Option::is_none")]
        value: Option<&'a V>,
    }
    serde_json::to_vec(&Line { op, path, value })
}

/// Whether replaying an entry could remove data at or below its path: a delete,
/// a set of nothing, or an update with a key set to nothing.
pub(super) fn may_delete(op: &WalOp, value: Option<&Value>) -> bool {
    match (op, value) {
        (WalOp::Delete, _) => true,
        (WalOp::Set, value) => value.is_none_or(stores_nothing),
        (WalOp::Update, Some(Value::Object(updates))) => updates.values().any(stores_nothing),
        (WalOp::Update, _) => false,
    }
}

struct HeldEntry {
    block: u32,
    start: u32,
    len: u32,
    may_delete: bool,
}

/// The entries of one WAL file.
struct Chunk {
    sequence: i64,
    /// Position of this chunk's first entry.
    first: usize,
    blocks: Vec<Vec<u8>>,
    /// The block short lines are going into.
    open: Option<usize>,
    entries: Vec<HeldEntry>,
    /// Size in the WAL file: the lines and their newlines.
    bytes: u64,
}

impl Chunk {
    /// Releases spare capacity once the chunk's WAL file is finished.
    fn finish(&mut self) {
        if let Some(open) = self.open.take() {
            self.blocks[open].shrink_to_fit();
        }
        self.entries.shrink_to_fit();
    }
}

/// The held entries, oldest first. Each entry has a position that stays the
/// same until it's trimmed, so the path index doesn't need rebuilding.
pub(super) struct HeldWal {
    chunks: VecDeque<Chunk>,
    /// Position of the oldest held entry.
    base: usize,
    /// Position the next entry gets.
    next: usize,
    /// Path → positions of the entries at that path, ascending. A BTreeMap so
    /// descendants can be found with a range scan.
    index: BTreeMap<String, Vec<usize>>,
}

impl HeldWal {
    pub(super) fn new() -> Self {
        Self {
            chunks: VecDeque::new(),
            base: 0,
            next: 0,
            index: BTreeMap::new(),
        }
    }

    /// Holds entries read from WAL files, in order.
    pub(super) fn from_entries(entries: Vec<WalEntry>) -> Self {
        let mut held = Self::new();
        for entry in entries {
            match wal_line(&entry.op, &entry.path, entry.value.as_ref()) {
                Ok(line) => held.push(
                    entry.sequence,
                    &entry.path,
                    &line,
                    may_delete(&entry.op, entry.value.as_ref()),
                ),
                // A parsed Value always serializes.
                Err(e) => error!("[STORAGE INTEGRITY] re-serializing a WAL entry failed: {e}"),
            }
        }
        held
    }

    pub(super) fn len(&self) -> usize {
        self.next - self.base
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.next == self.base
    }

    /// Size in the WAL files of the held entries.
    pub(super) fn bytes(&self) -> u64 {
        self.chunks.iter().map(|c| c.bytes).sum()
    }

    /// Holds an entry just appended to WAL file `sequence`. `line` is its JSON
    /// without the newline.
    pub(super) fn push(&mut self, sequence: i64, path: &str, line: &[u8], may_delete: bool) {
        if self.chunks.back().is_none_or(|c| c.sequence != sequence) {
            if let Some(previous) = self.chunks.back_mut() {
                previous.finish();
            }
            self.chunks.push_back(Chunk {
                sequence,
                first: self.next,
                blocks: Vec::new(),
                open: None,
                entries: Vec::new(),
                bytes: 0,
            });
        }
        let chunk = self.chunks.back_mut().expect("just ensured");
        let block = if line.len() > OWN_BLOCK_BYTES {
            chunk.blocks.push(Vec::with_capacity(line.len()));
            chunk.blocks.len() - 1
        } else {
            match chunk.open {
                Some(b) if chunk.blocks[b].len() + line.len() <= chunk.blocks[b].capacity() => b,
                _ => {
                    chunk.blocks.push(Vec::with_capacity(BLOCK_BYTES));
                    let b = chunk.blocks.len() - 1;
                    chunk.open = Some(b);
                    b
                }
            }
        };
        let buf = &mut chunk.blocks[block];
        chunk.entries.push(HeldEntry {
            block: block as u32,
            start: buf.len() as u32,
            len: line.len() as u32,
            may_delete,
        });
        buf.extend_from_slice(line);
        chunk.bytes += line.len() as u64 + 1;
        self.index
            .entry(normalize_path_key(path))
            .or_default()
            .push(self.next);
        self.next += 1;
    }

    fn locate(&self, pos: usize) -> Option<(&Chunk, &HeldEntry)> {
        if pos < self.base || pos >= self.next {
            return None;
        }
        let i = self
            .chunks
            .partition_point(|c| c.first <= pos)
            .checked_sub(1)?;
        let chunk = &self.chunks[i];
        Some((chunk, chunk.entries.get(pos - chunk.first)?))
    }

    /// The entry at a position, parsed.
    pub(super) fn get(&self, pos: usize) -> Option<WalEntry> {
        let (chunk, held) = self.locate(pos)?;
        let start = held.start as usize;
        let line = &chunk.blocks[held.block as usize][start..start + held.len as usize];
        match serde_json::from_slice::<WalEntry>(line) {
            Ok(mut entry) => {
                entry.sequence = chunk.sequence;
                Some(entry)
            }
            // Every held line was serialized here or parsed on load.
            Err(e) => {
                error!("[STORAGE INTEGRITY] a held WAL line didn't parse: {e}");
                None
            }
        }
    }

    pub(super) fn may_delete_at(&self, pos: usize) -> bool {
        self.locate(pos).is_some_and(|(_, held)| held.may_delete)
    }

    /// Every held entry, parsed, oldest first.
    #[cfg(test)]
    pub(super) fn entries(&self) -> Vec<WalEntry> {
        (self.base..self.next)
            .filter_map(|pos| self.get(pos))
            .collect()
    }

    /// Drops the entries of WAL files up to and including `sequence`, which
    /// compaction has put in the blob. Returns how many were dropped.
    pub(super) fn trim_through(&mut self, sequence: i64) -> usize {
        while self.chunks.front().is_some_and(|c| c.sequence <= sequence) {
            self.chunks.pop_front();
        }
        let base = self.chunks.front().map_or(self.next, |c| c.first);
        let trimmed = base - self.base;
        if trimmed > 0 {
            self.base = base;
            self.index.retain(|_, positions| {
                let gone = positions.partition_point(|&p| p < base);
                positions.drain(..gone);
                !positions.is_empty()
            });
        }
        trimmed
    }

    /// Positions of the entries that affect `path`: at it, above it or below
    /// it. Ascending, so they replay in order.
    pub(super) fn find_affecting(&self, path: &str) -> Vec<usize> {
        let path_owned = normalize_path_key(path);
        let path = path_owned.as_str();
        let mut positions = Vec::new();

        if path == "/" {
            for p in self.index.values() {
                positions.extend(p);
            }
            positions.sort_unstable();
            return positions;
        }

        if let Some(p) = self.index.get(path) {
            positions.extend(p);
        }
        if let Some(p) = self.index.get("/") {
            positions.extend(p);
        }
        let mut current = path;
        while let Some(pos) = current.rfind('/') {
            if pos == 0 {
                break;
            }
            current = &path[..pos];
            if let Some(p) = self.index.get(current) {
                positions.extend(p);
            }
        }

        use std::ops::Bound;
        for (key, p) in self
            .index
            .range::<str, _>((Bound::Excluded(path), Bound::Unbounded))
        {
            if !(key.len() > path.len()
                && key.starts_with(path)
                && key.as_bytes()[path.len()] == b'/')
            {
                break;
            }
            positions.extend(p);
        }

        positions.sort_unstable();
        positions
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    /// Counts live bytes allocated by the current thread, so a test can measure
    /// what a structure costs while other tests run in parallel.
    struct ThreadCounting;

    thread_local! {
        static LIVE: Cell<isize> = const { Cell::new(0) };
    }

    unsafe impl GlobalAlloc for ThreadCounting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let _ = LIVE.try_with(|l| l.set(l.get() + layout.size() as isize));
            unsafe { System.alloc(layout) }
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            let _ = LIVE.try_with(|l| l.set(l.get() - layout.size() as isize));
            unsafe { System.dealloc(ptr, layout) }
        }
    }

    #[global_allocator]
    static COUNTING: ThreadCounting = ThreadCounting;

    fn live() -> isize {
        LIVE.with(|l| l.get())
    }

    /// Writes like a game's: token moves, chat, presence and sheet saves.
    fn game_writes() -> Vec<WalEntry> {
        let store = "s".repeat(30_000);
        let mut out = Vec::new();
        for i in 0..3000i64 {
            let e = match i % 100 {
                0 => entry(
                    WalOp::Update,
                    "/game/char-attribs/char/-Lchar/-LStore",
                    Some(json!({"name": "store", "current": store})),
                    1 + i / 1000,
                ),
                n if n % 3 == 0 => entry(
                    WalOp::Set,
                    &format!("/game/chat/-Lmsg{i:08}"),
                    Some(json!({"who": "Player One", "type": "general",
                        "content": "an out-of-character message of ordinary length",
                        "playerid": "-LBenchPl000001", "avatar": "/users/avatar/1/30"})),
                    1 + i / 1000,
                ),
                n if n % 3 == 1 => entry(
                    WalOp::Update,
                    &format!("/game/graphics/page/-Lpage/-Ltok{:02}", i % 40),
                    Some(json!({"left": i * 7 % 5000, "top": i * 13 % 5000,
                        "lastmove": format!("{},{}", i, i + 1)})),
                    1 + i / 1000,
                ),
                _ => entry(
                    WalOp::Update,
                    &format!("/game/presence/-LPl{:02}", i % 5),
                    Some(json!({"t": i, "page": "-Lpage"})),
                    1 + i / 1000,
                ),
            };
            out.push(e);
        }
        out
    }

    #[test]
    fn held_wal_costs_little_more_than_its_bytes() {
        let entries = game_writes();

        let before = live();
        let held = HeldWal::from_entries(entries.clone());
        let held_cost = live() - before;
        let bytes = held.bytes() as isize;

        let before = live();
        let parsed: Vec<WalEntry> = entries.clone();
        let parsed_cost = live() - before;
        drop(parsed);
        println!("WAL {bytes} bytes: held {held_cost}, parsed {parsed_cost}");

        assert!(
            held_cost * 10 < bytes * 16,
            "held WAL took {held_cost} bytes for {bytes} bytes of WAL"
        );
        assert!(
            parsed_cost > held_cost,
            "parsed entries took {parsed_cost} bytes, held {held_cost}"
        );
    }

    fn entry(op: WalOp, path: &str, value: Option<Value>, sequence: i64) -> WalEntry {
        WalEntry {
            op,
            path: path.to_string(),
            value,
            sequence,
        }
    }

    fn held(entries: &[WalEntry]) -> HeldWal {
        HeldWal::from_entries(entries.to_vec())
    }

    #[test]
    fn entries_come_back_as_written() {
        let written = vec![
            entry(WalOp::Set, "/a", Some(json!({"x": 1})), 1),
            entry(WalOp::Update, "/a", Some(json!({"y": "two"})), 1),
            entry(WalOp::Delete, "/b", None, 2),
        ];
        let held = held(&written);
        let back = held.entries();
        assert_eq!(back.len(), 3);
        for (a, b) in written.iter().zip(&back) {
            assert_eq!(a.op, b.op);
            assert_eq!(a.path, b.path);
            assert_eq!(a.value, b.value);
            assert_eq!(a.sequence, b.sequence);
        }
    }

    #[test]
    fn line_matches_wal_entry_serialization() {
        let e = entry(WalOp::Update, "/p", Some(json!({"k": [1, null]})), 0);
        assert_eq!(
            wal_line(&e.op, &e.path, e.value.as_ref()).unwrap(),
            serde_json::to_vec(&e).unwrap()
        );
        let d = entry(WalOp::Delete, "/p", None, 0);
        assert_eq!(
            wal_line::<Value>(&d.op, &d.path, None).unwrap(),
            serde_json::to_vec(&d).unwrap()
        );
    }

    #[test]
    fn bytes_count_lines_and_newlines() {
        let e = entry(WalOp::Set, "/a", Some(json!(1)), 1);
        let held = held(std::slice::from_ref(&e));
        assert_eq!(
            held.bytes(),
            serde_json::to_vec(&e).unwrap().len() as u64 + 1
        );
    }

    #[test]
    fn trimming_keeps_the_index_right() {
        let held_entries = vec![
            entry(WalOp::Set, "/a", Some(json!(1)), 1),
            entry(WalOp::Set, "/a", Some(json!(2)), 2),
            entry(WalOp::Set, "/b", Some(json!(3)), 2),
            entry(WalOp::Set, "/a", Some(json!(4)), 3),
        ];
        let mut held = held(&held_entries);
        assert_eq!(held.trim_through(1), 1);
        assert_eq!(held.len(), 3);
        let at_a: Vec<_> = held
            .find_affecting("/a")
            .into_iter()
            .map(|p| held.get(p).unwrap().value)
            .collect();
        assert_eq!(at_a, vec![Some(json!(2)), Some(json!(4))]);

        assert_eq!(held.trim_through(2), 2);
        assert_eq!(held.find_affecting("/b"), Vec::<usize>::new());
        assert_eq!(held.entries()[0].value, Some(json!(4)));

        assert_eq!(held.trim_through(9), 1);
        assert!(held.is_empty());
        assert_eq!(held.bytes(), 0);
        assert!(held.find_affecting("/").is_empty());
    }

    #[test]
    fn find_affecting_covers_ancestors_descendants_and_root() {
        let held = held(&[
            entry(WalOp::Set, "/", Some(json!({"r": 1})), 1),
            entry(WalOp::Set, "/a", Some(json!(1)), 1),
            entry(WalOp::Set, "/a/b", Some(json!(1)), 1),
            entry(WalOp::Set, "/a/b/c", Some(json!(1)), 1),
            entry(WalOp::Set, "/ab", Some(json!(1)), 1),
        ]);
        assert_eq!(held.find_affecting("/a/b"), vec![0, 1, 2, 3]);
        assert_eq!(held.find_affecting("/ab"), vec![0, 4]);
        assert_eq!(held.find_affecting("/").len(), 5);
    }

    #[test]
    fn may_delete_matches_the_value() {
        let cases = [
            (WalOp::Delete, None, true),
            (WalOp::Set, None, true),
            (WalOp::Set, Some(json!(null)), true),
            (WalOp::Set, Some(json!({"a": null})), true),
            (WalOp::Set, Some(json!({"a": 1})), false),
            (WalOp::Update, Some(json!({"a": 1, "b": null})), true),
            (WalOp::Update, Some(json!({"a": 1})), false),
        ];
        for (op, value, expected) in cases {
            let held = held(&[entry(op.clone(), "/x", value.clone(), 1)]);
            assert_eq!(held.may_delete_at(0), expected, "{op:?} {value:?}");
        }
    }

    #[test]
    fn long_lines_get_their_own_block() {
        let big = "x".repeat(BLOCK_BYTES * 2);
        let held = held(&[
            entry(WalOp::Set, "/small", Some(json!(1)), 1),
            entry(WalOp::Set, "/big", Some(json!(big.clone())), 1),
            entry(WalOp::Set, "/small", Some(json!(2)), 1),
        ]);
        let back = held.entries();
        assert_eq!(back[1].value, Some(json!(big)));
        assert_eq!(back[2].value, Some(json!(2)));
    }
}
