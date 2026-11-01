//! What a crash can leave on disk, given a journal of file operations.
//!
//! [`Disk`] replays a journal and tracks, for each file, which bytes have been
//! synced. From any point it can produce the files left by a process kill
//! (everything issued survives, because the page cache does) or by a power loss
//! (synced data survives, and unsynced data lands in pieces, or not at all).

use lark_server::storage::io_trace::Op;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

pub type Files = BTreeMap<String, Vec<u8>>;

/// Blocks a power loss can land independently. ext4 writes back 4K pages; a
/// device may only guarantee single sectors.
const PAGE: usize = 4096;
const SECTOR: usize = 512;

/// The server takes this lock on every compaction; it holds no data.
const LOCK_FILE: &str = ".storage.lock";

/// When a new or renamed directory entry becomes durable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DirDurability {
    /// ext4's journal: entries commit in order, and an fsync that has to commit
    /// metadata commits every entry before it.
    Journaled,
    /// The POSIX minimum: only an fsync of the directory itself.
    Posix,
}

/// Small seeded generator, so every image can be rebuilt from its seed.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    pub fn below(&mut self, n: usize) -> usize {
        (self.next() % n.max(1) as u64) as usize
    }

    pub fn chance(&mut self, percent: usize) -> bool {
        self.below(100) < percent
    }
}

#[derive(Default)]
struct Inode {
    /// Bytes on the device as of the last sync.
    durable: Vec<u8>,
    /// Bytes as the running process sees them.
    current: Vec<u8>,
    /// What's left of `durable` if a shrink committed. ext4 journals a truncate
    /// as metadata, and later writes go to new blocks, so a crash shows either
    /// the old file or the shrunken one, never the two mixed.
    after_shrink: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
enum NameOp {
    Link(String, usize),
    Unlink(String),
    Rename(String, String),
}

impl NameOp {
    fn apply(&self, names: &mut BTreeMap<String, usize>) {
        match self {
            NameOp::Link(path, inode) => {
                names.insert(path.clone(), *inode);
            }
            NameOp::Unlink(path) => {
                names.remove(path);
            }
            NameOp::Rename(from, to) => {
                if let Some(inode) = names.remove(from) {
                    names.insert(to.clone(), inode);
                }
            }
        }
    }

    fn dir(&self) -> &str {
        let path = match self {
            NameOp::Link(path, _) | NameOp::Unlink(path) => path,
            NameOp::Rename(_, to) => to,
        };
        path.rsplit_once('/').map(|(dir, _)| dir).unwrap_or("")
    }
}

pub struct Disk {
    dirs: DirDurability,
    inodes: Vec<Inode>,
    /// Directory entries as the running process sees them.
    live: BTreeMap<String, usize>,
    /// Directory entries on the device.
    durable: BTreeMap<String, usize>,
    /// Entry changes not on the device yet, oldest first.
    pending: Vec<NameOp>,
}

impl Disk {
    /// A disk holding `base`, all of it durable.
    pub fn new(base: &Files, dirs: DirDurability) -> Self {
        let mut inodes = Vec::new();
        let mut live = BTreeMap::new();
        for (path, data) in base {
            live.insert(path.clone(), inodes.len());
            inodes.push(Inode {
                durable: data.clone(),
                current: data.clone(),
                after_shrink: None,
            });
        }
        Self {
            dirs,
            inodes,
            durable: live.clone(),
            live,
            pending: Vec::new(),
        }
    }

    fn inode(&mut self, path: &str) -> &mut Inode {
        let id = self.live[path];
        &mut self.inodes[id]
    }

    pub fn apply(&mut self, op: &Op) {
        match op {
            Op::Create { path, truncate } => {
                if self.live.contains_key(path) {
                    if *truncate {
                        self.resize(path, 0);
                    }
                } else {
                    let id = self.inodes.len();
                    self.inodes.push(Inode::default());
                    self.live.insert(path.clone(), id);
                    self.pending.push(NameOp::Link(path.clone(), id));
                }
            }
            Op::Write { path, offset, data } => {
                let inode = self.inode(path);
                let (start, end) = (*offset as usize, *offset as usize + data.len());
                if inode.current.len() < end {
                    inode.current.resize(end, 0);
                }
                inode.current[start..end].copy_from_slice(data);
            }
            Op::Truncate { path, size } => self.resize(path, *size as usize),
            Op::Sync { path } => {
                let id = self.live[path];
                let inode = &mut self.inodes[id];
                let size_changed = inode.durable.len() != inode.current.len();
                inode.durable = inode.current.clone();
                inode.after_shrink = None;
                // `fdatasync` commits metadata only if it needs to, for a new
                // file or a new size.
                let new_entry = self
                    .pending
                    .iter()
                    .any(|op| matches!(op, NameOp::Link(_, linked) if *linked == id));
                if self.dirs == DirDurability::Journaled && (size_changed || new_entry) {
                    self.commit_all();
                }
            }
            Op::SyncDir { path } => match self.dirs {
                DirDurability::Journaled => self.commit_all(),
                DirDurability::Posix => {
                    let pending = std::mem::take(&mut self.pending);
                    for op in pending {
                        if op.dir() == path {
                            op.apply(&mut self.durable);
                        } else {
                            self.pending.push(op);
                        }
                    }
                }
            },
            Op::Rename { from, to } => {
                let id = self.live.remove(from).expect("rename of a known file");
                self.live.insert(to.clone(), id);
                self.pending.push(NameOp::Rename(from.clone(), to.clone()));
            }
            Op::Remove { path } => {
                self.live.remove(path);
                self.pending.push(NameOp::Unlink(path.clone()));
            }
            Op::WriteIssued | Op::WriteAcked => {}
        }
    }

    fn resize(&mut self, path: &str, size: usize) {
        let inode = self.inode(path);
        if size < inode.current.len() {
            let keep = size.min(inode.durable.len());
            inode.after_shrink = Some(inode.durable[..keep].to_vec());
        }
        inode.current.resize(size, 0);
    }

    fn commit_all(&mut self) {
        for op in std::mem::take(&mut self.pending) {
            op.apply(&mut self.durable);
        }
    }

    /// The files a process kill leaves: everything issued, whole.
    pub fn after_kill(&self) -> Files {
        self.live
            .iter()
            .map(|(path, id)| (path.clone(), self.inodes[*id].current.clone()))
            .collect()
    }

    /// One set of files a power loss could leave at this point.
    pub fn after_power_loss(&self, rng: &mut Rng) -> Files {
        let mut names = self.durable.clone();
        match self.dirs {
            // The journal commits in order, so some prefix of the pending entries.
            DirDurability::Journaled => {
                let keep = rng.below(self.pending.len() + 1);
                for op in &self.pending[..keep] {
                    op.apply(&mut names);
                }
            }
            DirDurability::Posix => {
                for op in &self.pending {
                    if rng.chance(50) {
                        op.apply(&mut names);
                    }
                }
            }
        }
        names
            .iter()
            .map(|(path, id)| (path.clone(), torn_contents(&self.inodes[*id], rng)))
            .collect()
    }
}

/// One way a file with unsynced changes could look after a power loss.
fn torn_contents(inode: &Inode, rng: &mut Rng) -> Vec<u8> {
    if inode.durable == inode.current {
        return inode.current.clone();
    }
    let new = &inode.current;
    let old: &[u8] = match &inode.after_shrink {
        // The shrink never committed, so neither did anything after it.
        Some(_) if rng.chance(30) => return inode.durable.clone(),
        Some(shrunk) => shrunk,
        None => &inode.durable,
    };

    let block = if rng.chance(25) { SECTOR } else { PAGE };
    let len = if new.len() > old.len() {
        match rng.below(3) {
            0 => new.len(),
            1 => old.len(),
            // The size landed partway through the new data, on a block boundary.
            _ => {
                let steps = (new.len() - old.len()).div_ceil(block);
                let at = old.len().next_multiple_of(block) + rng.below(steps) * block;
                at.clamp(old.len(), new.len())
            }
        }
    } else {
        new.len().max(old.len().min(new.len()))
    };

    let blocks = len.div_ceil(block);
    let dirty: Vec<usize> = (0..blocks)
        .filter(|b| range(old, *b, block) != range(new, *b, block))
        .collect();
    let landed = pick_landed(&dirty, rng);

    let mut image = vec![0u8; len];
    for b in 0..blocks {
        let source: &[u8] = if landed.contains(&b) { new } else { old };
        let start = b * block;
        let bytes = range(source, b, block);
        let take = bytes.len().min(len - start);
        image[start..start + take].copy_from_slice(&bytes[..take]);
    }
    image
}

/// Which dirty blocks reached the device. Plain coin flips are the general
/// case; the others make contiguous tears, which land more often in practice.
fn pick_landed(dirty: &[usize], rng: &mut Rng) -> BTreeSet<usize> {
    let cut = rng.below(dirty.len() + 1);
    match rng.below(4) {
        0 => dirty.iter().copied().filter(|_| rng.chance(50)).collect(),
        1 => dirty[..cut].iter().copied().collect(),
        2 => dirty[cut..].iter().copied().collect(),
        _ => {
            let mut all: BTreeSet<usize> = dirty.iter().copied().collect();
            if let Some(missed) = dirty.get(cut) {
                all.remove(missed);
            }
            all
        }
    }
}

fn range(data: &[u8], block: usize, size: usize) -> &[u8] {
    let start = (block * size).min(data.len());
    let end = (start + size).min(data.len());
    &data[start..end]
}

/// Every file under `root`, by relative path.
pub fn read_tree(root: &Path) -> Files {
    fn walk(root: &Path, dir: &Path, files: &mut Files) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, files);
            } else if path.file_name().is_some_and(|name| name != LOCK_FILE) {
                let relative = path.strip_prefix(root).unwrap();
                let relative = relative.to_string_lossy().into_owned();
                files.insert(relative, std::fs::read(&path).unwrap());
            }
        }
    }
    let mut files = Files::new();
    walk(root, root, &mut files);
    files
}

/// Writes `files` under `root`.
pub fn write_tree(root: &Path, files: &Files) {
    for (path, data) in files {
        let path = root.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, data).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &str, offset: u64, data: &[u8]) -> Op {
        Op::Write {
            path: path.to_string(),
            offset,
            data: data.to_vec(),
        }
    }

    fn create(path: &str) -> Op {
        Op::Create {
            path: path.to_string(),
            truncate: false,
        }
    }

    fn sync(path: &str) -> Op {
        Op::Sync {
            path: path.to_string(),
        }
    }

    #[test]
    fn synced_data_always_survives_a_power_loss() {
        let mut disk = Disk::new(&Files::new(), DirDurability::Journaled);
        disk.apply(&create("f"));
        disk.apply(&write("f", 0, &[1; 9000]));
        disk.apply(&sync("f"));
        disk.apply(&write("f", 2000, &[2; 5000]));
        let mut rng = Rng::new(1);
        let mut saw_old = false;
        let mut saw_new = false;
        for _ in 0..200 {
            let image = disk.after_power_loss(&mut rng).remove("f").unwrap();
            assert_eq!(image.len(), 9000);
            // Bytes outside the unsynced write never change.
            assert!(image[..2000].iter().all(|b| *b == 1));
            assert!(image[7000..].iter().all(|b| *b == 1));
            saw_old |= image[2000..7000].contains(&1);
            saw_new |= image[2000..7000].contains(&2);
        }
        assert!(saw_old && saw_new, "a power loss should mix old and new");
    }

    #[test]
    fn a_new_entry_needs_the_directory_synced_on_posix() {
        let mut posix = Disk::new(&Files::new(), DirDurability::Posix);
        let mut ext4 = Disk::new(&Files::new(), DirDurability::Journaled);
        for disk in [&mut posix, &mut ext4] {
            disk.apply(&create("wal/1.wal"));
            disk.apply(&write("wal/1.wal", 0, b"data"));
            disk.apply(&sync("wal/1.wal"));
        }
        assert!(ext4.durable.contains_key("wal/1.wal"));
        assert!(!posix.durable.contains_key("wal/1.wal"));

        posix.apply(&Op::SyncDir {
            path: "wal".to_string(),
        });
        assert!(posix.durable.contains_key("wal/1.wal"));
    }

    #[test]
    fn a_shrink_never_mixes_with_the_old_contents() {
        let base = Files::from([("f".to_string(), vec![1u8; 8192])]);
        let mut disk = Disk::new(&base, DirDurability::Journaled);
        disk.apply(&Op::Truncate {
            path: "f".to_string(),
            size: 0,
        });
        disk.apply(&write("f", 0, &[2; 8192]));
        let mut rng = Rng::new(2);
        for _ in 0..200 {
            let image = disk.after_power_loss(&mut rng).remove("f").unwrap();
            let old_file = image == vec![1u8; 8192];
            assert!(old_file || !image.contains(&1), "old bytes after a shrink");
        }
    }
}
