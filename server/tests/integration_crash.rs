//! Crash recovery, from images of what a crash leaves on disk.
//!
//! A seeded workload runs against a real Database and StorageWorker, with
//! `io-trace` recording every durable file operation. The
//! journal then rebuilds what a crash at a chosen point could leave (see
//! `crash_model`), and each image recovers through the same server code:
//!
//! 0. If the newest WAL file ends in a torn line, the server must refuse to load
//!    and leave the WAL alone. The test then trims the line, as an operator would,
//!    and carries on. A crash may only ever tear unacknowledged lines.
//! 1. Open it and read everything. It must hold every acknowledged write, and
//!    be the state after some prefix of the writes.
//! 2. Keep writing past several compactions, which must all finish.
//! 3. Restart twice, reading everything each time.
//!
//! Step 2 and 3 matter: some damage only shows up batches or restarts later.
//!
//! Default durability can lose writes that were acknowledged, so step 1 checks
//! less there: everything the blob committed must be intact, and any other path
//! must hold a value that was written to it. The blob must always stay readable.
//!
//! ```text
//! docker run ... cargo test -p lark-server --features io-trace --test integration_crash
//! ```
//!
//! `LARK_CRASH_CASES` sets images per model, `LARK_CRASH_SEED` the seed, and
//! `LARK_CRASH_KEEP` a directory that receives each failing image.

mod common;
mod crash_model;

use common::TestServer;
use crash_model::{DirDurability, Disk, Files, Rng, read_tree, write_tree};
use glommio::{LocalExecutorBuilder, Placement};
use lark_blob::io::StdBlobIO;
use lark_blob::{ArcValue, write_blob};
use lark_server::storage::WalEntry;
use lark_server::storage::io_trace::{self, Op};
use serde_json::{Map, Value, json};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;
use tempfile::TempDir;

const PROJECT: &str = "crash-project";
const DATABASE: &str = "crash-db";

/// Small enough for a compaction every few writes.
const WAL_ROTATION_SIZE: u64 = 32 * 1024;
const WORKLOAD_WRITES: usize = 160;
const FOLLOW_UP_WRITES: usize = 40;
/// Children in the seeded collection. Its index spans several 4K pages.
const COLLECTION_SIZE: usize = 600;
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Durability {
    /// Every write is synced before its ACK.
    Strict,
    /// Writes are ACKed first and reach the page cache in batches.
    Default,
}

impl Durability {
    fn apply(self) {
        match self {
            Durability::Strict => io_trace::set_durability(0, true),
            Durability::Default => io_trace::set_durability(5, false),
        }
    }
}

/// Images that were refused for a torn WAL and then recovered once it was trimmed.
static REFUSALS: AtomicUsize = AtomicUsize::new(0);

fn db_dir(root: &Path) -> PathBuf {
    root.join(PROJECT).join(DATABASE)
}

// =============================================================================
// Workload
// =============================================================================

#[derive(Clone, Debug)]
struct Write {
    path: String,
    /// Null deletes.
    value: Value,
}

/// A string of about `size` bytes, with multibyte characters throughout.
fn text(rng: &mut Rng, size: usize) -> String {
    let unit = ["é", "日本", "x", "ñandú", "🦜"][rng.below(5)];
    unit.repeat(size / unit.len() + 1)
}

/// Writes that reach the paths recovery has to get right. Growing values move
/// to new regions and free the old ones (big enough to be reused), new children
/// shift a multi-page collection index in place, and overwrites and deletes
/// exercise the other in-place writes.
fn generate(rng: &mut Rng, count: usize, first_id: usize) -> Vec<Write> {
    (0..count)
        .map(|i| match rng.below(10) {
            0..=4 => {
                let size = 4200 + rng.below(5000);
                Write {
                    path: format!("/big/k{}", rng.below(8)),
                    value: Value::String(text(rng, size)),
                }
            }
            5..=7 => Write {
                path: format!("/items/new-{}", first_id + i),
                value: json!({ "v": text(rng, 60) }),
            },
            8 => Write {
                path: format!("/items/key{:04}", rng.below(COLLECTION_SIZE)),
                value: json!({ "v": text(rng, 30) }),
            },
            _ => Write {
                path: format!("/big/k{}", rng.below(8)),
                value: Value::Null,
            },
        })
        .collect()
}

fn base_value() -> Value {
    let items: Map<String, Value> = (0..COLLECTION_SIZE)
        .map(|i| {
            (
                format!("key{i:04}"),
                json!({ "v": format!("committed-{i:04}") }),
            )
        })
        .collect();
    let big: Map<String, Value> = (0..8).map(|i| (format!("k{i}"), json!("small"))).collect();
    json!({ "items": items, "big": big })
}

fn apply(model: &mut Value, write: &Write) {
    fn go(node: &mut Value, path: &[&str], value: &Value) {
        let map = node.as_object_mut().expect("workload paths are objects");
        if let [key] = path {
            if value.is_null() {
                map.remove(*key);
            } else {
                map.insert(key.to_string(), value.clone());
            }
            return;
        }
        let child = map.entry(path[0].to_string()).or_insert_with(|| json!({}));
        go(child, &path[1..], value);
        if child.as_object().is_some_and(|m| m.is_empty()) {
            map.remove(path[0]);
        }
    }
    let path: Vec<&str> = write.path.trim_matches('/').split('/').collect();
    go(model, &path, &write.value);
}

/// Up to a few paths where two values differ, with the total count.
fn differences(expected: &Value, got: &Value) -> (usize, Vec<String>) {
    fn walk(path: &str, a: &Value, b: &Value, found: &mut Vec<String>) {
        match (a, b) {
            (Value::Object(a), Value::Object(b)) => {
                let keys: std::collections::BTreeSet<_> = a.keys().chain(b.keys()).collect();
                for key in keys {
                    let child = format!("{path}/{key}");
                    match (a.get(key), b.get(key)) {
                        (Some(a), Some(b)) => walk(&child, a, b, found),
                        (Some(_), None) => found.push(format!("{child} missing")),
                        _ => found.push(format!("{child} unexpected")),
                    }
                }
            }
            _ if a != b => found.push(format!("{path} wrong")),
            _ => {}
        }
    }
    let mut found = Vec::new();
    walk("", expected, got, &mut found);
    let count = found.len();
    found.truncate(3);
    (count, found)
}

// =============================================================================
// Running a future on its own executor
// =============================================================================

thread_local! {
    static LAST_PANIC: RefCell<Option<String>> = const { RefCell::new(None) };
    /// The server's most recent error log on this thread: the reason behind a
    /// timeout or a stalled compaction.
    static LAST_ERROR: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Keeps the last error logged on the current thread.
struct ErrorCapture;

impl std::io::Write for ErrorCapture {
    fn write(&mut self, line: &[u8]) -> std::io::Result<usize> {
        let text = String::from_utf8_lossy(line).trim().to_string();
        if !text.is_empty() {
            LAST_ERROR.set(Some(text));
        }
        Ok(line.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn capture_errors() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::ERROR)
            .with_ansi(false)
            .without_time()
            .with_target(false)
            .with_writer(|| ErrorCapture)
            .init();
    });
}

/// A failure's cause with the numbers and temp paths taken out, so images that
/// failed the same way count together.
fn cause_of(failure: &Failure) -> String {
    let text = if failure.kind == "panic" {
        failure.detail.clone()
    } else {
        failure.log.clone().unwrap_or_default()
    };
    let paths = regex::Regex::new(r"/tmp/\.tmp\w+").unwrap();
    let numbers = regex::Regex::new(r"\d+").unwrap();
    let text = paths.replace_all(&text, "");
    numbers.replace_all(&text, "N").chars().take(170).collect()
}

fn quiet_panics() {
    capture_errors();
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let default_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if std::thread::current().name() == Some("crash-image") {
                LAST_PANIC.set(Some(info.to_string()));
            } else {
                default_hook(info);
            }
        }));
    });
}

enum Finished<T> {
    Done(T),
    Panicked(String),
    TimedOut,
}

/// Runs `make()` on a new executor. A damaged image can panic or hang the
/// server, so each gets its own thread, and a hung one is left behind.
fn on_executor<T, Fut>(
    durability: Durability,
    make: impl FnOnce() -> Fut + Send + 'static,
) -> Finished<T>
where
    T: Send + 'static,
    Fut: Future<Output = T> + 'static,
{
    quiet_panics();
    let (tx, rx) = mpsc::channel();
    std::thread::Builder::new()
        .name("crash-image".to_string())
        .spawn(move || {
            let result = catch_unwind(AssertUnwindSafe(|| {
                io_trace::set_wal_rotation_size(WAL_ROTATION_SIZE);
                durability.apply();
                let executor = LocalExecutorBuilder::new(Placement::Unbound).make();
                executor.unwrap().run(make())
            }));
            let finished = match result {
                Ok(value) => Finished::Done(value),
                Err(_) => {
                    Finished::Panicked(LAST_PANIC.take().unwrap_or_else(|| "panicked".to_string()))
                }
            };
            let _ = tx.send(finished);
        })
        .unwrap();
    rx.recv_timeout(RECOVERY_TIMEOUT)
        .unwrap_or(Finished::TimedOut)
}

// =============================================================================
// Recording a run
// =============================================================================

async fn open(root: &Path) -> (TestServer, common::TestClient) {
    let server = TestServer::with_persistence(root.to_str().unwrap());
    let rules = json!({ "rules": { ".read": true, ".write": true } });
    server
        .set_rules_with_ephemeral(PROJECT, rules, false)
        .unwrap();
    let mut client = server.client();
    client.connect(&format!("{PROJECT}/{DATABASE}")).await;
    (server, client)
}

async fn close(server: TestServer, mut client: common::TestClient) {
    client.disconnect().await;
    server.shutdown().await;
    // The database task closes its WAL once its handle is gone.
    glommio::timer::sleep(Duration::from_millis(50)).await;
}

fn read_number(path: &Path) -> i64 {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

fn highest_wal(root: &Path) -> i64 {
    std::fs::read_dir(db_dir(root).join("wal"))
        .map(|entries| {
            entries
                .flatten()
                .filter_map(|e| lark_blob::wal::parse_wal_sequence(e.file_name().to_str()?))
                .max()
                .unwrap_or(0)
        })
        .unwrap_or(0)
}

/// Waits until every finished WAL file is compacted. False if that doesn't
/// happen, which means compaction has stopped.
async fn compaction_caught_up(root: &Path) -> bool {
    let sequence = db_dir(root).join("sequence");
    for _ in 0..200 {
        if read_number(&sequence) >= highest_wal(root) - 1 {
            return true;
        }
        glommio::timer::sleep(Duration::from_millis(5)).await;
    }
    false
}

struct Recording {
    /// The directory the server really wrote, kept to compare replays against.
    root: TempDir,
    base: Files,
    ops: Vec<Op>,
    writes: Vec<Write>,
    seed: u64,
    durability: Durability,
}

fn record(seed: u64, durability: Durability) -> Recording {
    let root = TempDir::new().unwrap();
    let dir = db_dir(root.path());
    std::fs::create_dir_all(&dir).unwrap();
    let blob = ArcValue::from_value(base_value());
    let io = StdBlobIO::create(&dir.join("blob.lark")).unwrap();
    futures::executor::block_on(write_blob(&io, &blob)).unwrap();
    let base = read_tree(root.path());

    let mut rng = Rng::new(seed);
    let writes = generate(&mut rng, WORKLOAD_WRITES, 0);

    let path = root.path().to_path_buf();
    let run_writes = writes.clone();
    let ops = on_executor(durability, move || async move {
        io_trace::start(&path);
        let (server, client) = open(&path).await;
        for write in &run_writes {
            io_trace::mark(Op::WriteIssued);
            let result = client.set(&write.path, write.value.clone()).await;
            result.expect("write during recording");
            io_trace::mark(Op::WriteAcked);
        }
        assert!(compaction_caught_up(&path).await, "recording's compaction");
        close(server, client).await;
        io_trace::take()
    });
    let ops = match ops {
        Finished::Done(ops) => ops,
        Finished::Panicked(message) => panic!("recording panicked: {message}"),
        Finished::TimedOut => panic!("recording timed out"),
    };
    Recording {
        root,
        base,
        ops,
        writes,
        seed,
        durability,
    }
}

impl Recording {
    fn state_after(&self, writes: usize) -> Value {
        let mut model = base_value();
        for write in &self.writes[..writes] {
            apply(&mut model, write);
        }
        model
    }

    /// How many writes are in WAL files up to `sequence`. Each write is one line.
    fn writes_through(&self, sequence: i64) -> usize {
        self.ops
            .iter()
            .filter_map(|op| match op {
                Op::Write { path, data, .. } => {
                    let name = path.rsplit('/').next()?;
                    let wal = lark_blob::wal::parse_wal_sequence(name)?;
                    Some((wal, data.iter().filter(|b| **b == b'\n').count()))
                }
                _ => None,
            })
            .filter(|(wal, _)| *wal <= sequence)
            .map(|(_, lines)| lines)
            .sum()
    }
}

// =============================================================================
// Recovering an image
// =============================================================================

#[derive(Debug, Clone)]
struct Failure {
    kind: &'static str,
    stage: &'static str,
    detail: String,
    /// The server's last error log, when it wrote one.
    log: Option<String>,
}

fn fail(kind: &'static str, stage: &'static str, detail: String) -> Failure {
    Failure {
        kind,
        stage,
        detail,
        log: None,
    }
}

/// Reads the whole database and compares it to `expected`.
async fn check(
    client: &common::TestClient,
    expected: &Value,
    stage: &'static str,
) -> Result<(), Failure> {
    let got = client
        .once("/")
        .await
        .map_err(|e| fail("unreadable", stage, e))?;
    if &got == expected {
        return Ok(());
    }
    let (count, examples) = differences(expected, &got);
    Err(fail(
        "wrong data",
        stage,
        format!("{count} paths differ: {examples:?}"),
    ))
}

/// What a recovered database has to hold.
enum Expected {
    /// The state after some prefix of the writes. The first one holds every
    /// acknowledged write.
    Prefix(Vec<(usize, Value)>),
    /// `committed` is in the blob. Each later write may or may not have survived.
    Committed { committed: Value, later: Vec<Write> },
}

/// The paths a value can be stored at, with what each may hold.
fn allowed_values(committed: &Value, later: &[Write]) -> BTreeMap<String, Vec<Option<Value>>> {
    let mut allowed: BTreeMap<String, Vec<Option<Value>>> = BTreeMap::new();
    for (parent, children) in committed.as_object().unwrap() {
        for (key, value) in children.as_object().unwrap() {
            allowed.insert(format!("/{parent}/{key}"), vec![Some(value.clone())]);
        }
    }
    for write in later {
        let values = allowed.entry(write.path.clone()).or_insert(vec![None]);
        values.push((!write.value.is_null()).then(|| write.value.clone()));
    }
    allowed
}

/// Whether `got` keeps everything committed, apart from later writes, and every
/// other value is one that was written to that path.
fn check_committed(got: &Value, committed: &Value, later: &[Write]) -> Result<(), String> {
    let allowed = allowed_values(committed, later);
    for (path, values) in &allowed {
        if !values.contains(&got.pointer(path).cloned()) {
            return Err(format!("{path} holds a value that was never written there"));
        }
    }
    let Some(parents) = got.as_object() else {
        return Err("the root isn't an object".to_string());
    };
    for (parent, children) in parents {
        let Some(children) = children.as_object() else {
            return Err(format!("/{parent} isn't an object"));
        };
        for key in children.keys() {
            let path = format!("/{parent}/{key}");
            if !allowed.contains_key(&path) {
                return Err(format!("{path} was never written"));
            }
        }
    }
    Ok(())
}

/// Takes an image through recovery, more writes, and two restarts.
async fn recover(root: PathBuf, expected: Expected, follow_up: Vec<Write>) -> Result<(), Failure> {
    let (server, client) = open(&root).await;

    let stage = "first read";
    let got = client
        .once("/")
        .await
        .map_err(|e| fail("unreadable", stage, e))?;
    let mut model = match expected {
        Expected::Prefix(candidates) => {
            let Some((_, state)) = candidates.iter().find(|(_, state)| *state == got) else {
                // Compare against the state holding every acknowledged write.
                let (count, examples) = differences(&candidates[0].1, &got);
                return Err(fail(
                    "wrong data",
                    stage,
                    format!(
                        "not the state after any prefix of the writes; {count} paths differ \
                         from the acknowledged state: {examples:?}"
                    ),
                ));
            };
            state.clone()
        }
        Expected::Committed { committed, later } => {
            check_committed(&got, &committed, &later).map_err(|e| fail("wrong data", stage, e))?;
            got
        }
    };

    let stage = "writes after recovery";
    for write in &follow_up {
        client
            .set(&write.path, write.value.clone())
            .await
            .map_err(|e| fail("write rejected", stage, e))?;
        apply(&mut model, write);
    }
    if !compaction_caught_up(&root).await {
        return Err(fail(
            "compaction stuck",
            stage,
            format!(
                "sequence {} with WAL at {}",
                read_number(&db_dir(&root).join("sequence")),
                highest_wal(&root)
            ),
        ));
    }
    check(&client, &model, stage).await?;
    close(server, client).await;

    for stage in ["first restart", "second restart"] {
        let (server, client) = open(&root).await;
        check(&client, &model, stage).await?;
        close(server, client).await;
    }
    Ok(())
}

/// The prefixes of the workload this crash point allows: every acknowledged
/// write, and possibly the one in flight.
fn allowed_states(recording: &Recording, acked: usize, issued: usize) -> Vec<(usize, Value)> {
    (acked..=issued)
        .map(|writes| (writes, recording.state_after(writes)))
        .collect()
}

/// How many leading bytes of a WAL file hold complete entries.
fn valid_wal_prefix(content: &[u8]) -> usize {
    let mut end = 0;
    for line in content.split_inclusive(|byte| *byte == b'\n') {
        let text = line.trim_ascii();
        if !text.is_empty() && serde_json::from_slice::<WalEntry>(text).is_err() {
            break;
        }
        end += line.len();
    }
    end
}

/// The first WAL file the server would read that has a torn line, and where its
/// complete entries end. Files the blob already holds aren't read.
fn torn_wal(files: &Files) -> Option<(String, usize)> {
    let sequence = blob_sequence(files);
    files
        .iter()
        .filter(|(path, _)| wal_sequence(path).is_some_and(|wal| wal > sequence))
        .find_map(|(path, data)| {
            let valid = valid_wal_prefix(data);
            (valid < data.len()).then(|| (path.clone(), valid))
        })
}

/// The newest WAL file with entries the blob doesn't hold yet.
fn newest_uncompacted_wal(files: &Files) -> Option<String> {
    let sequence = blob_sequence(files);
    files
        .iter()
        .filter(|(path, data)| !data.is_empty() && wal_sequence(path).is_some_and(|w| w > sequence))
        .map(|(path, _)| path.clone())
        .max()
}

fn wal_sequence(path: &str) -> Option<i64> {
    lark_blob::wal::parse_wal_sequence(path.rsplit('/').next()?)
}

fn blob_sequence(files: &Files) -> i64 {
    files
        .iter()
        .find(|(path, _)| path.ends_with("/sequence"))
        .and_then(|(_, data)| std::str::from_utf8(data).ok()?.trim().parse().ok())
        .unwrap_or(0)
}

/// Waits for the server to refuse the database, and returns what it logged.
async fn refusal(root: PathBuf) -> Option<String> {
    let (server, client) = open(&root).await;
    let mut message = None;
    for _ in 0..400 {
        message = LAST_ERROR
            .take()
            .filter(|m| m.contains("Failed to initialize"));
        if message.is_some() {
            break;
        }
        glommio::timer::sleep(Duration::from_millis(5)).await;
    }
    close(server, client).await;
    message
}

/// A torn WAL must be refused and left as it is. Afterwards the image holds what
/// an operator would leave: the file cut back to its last complete entry.
fn expect_refusal(
    durability: Durability,
    root: &Path,
    files: &mut Files,
    wal: &str,
    valid: usize,
) -> Result<(), Failure> {
    let stage = "refusing a torn WAL";
    let dir = root.to_path_buf();
    let message = match on_executor(durability, move || refusal(dir)) {
        Finished::Done(message) => message,
        Finished::Panicked(message) => return Err(fail("panic", stage, message)),
        Finished::TimedOut => return Err(fail("hang", stage, String::new())),
    };
    let message =
        message.ok_or_else(|| fail("not refused", stage, format!("{wal} ends in a torn line")))?;
    if !message.contains("WAL corruption") || !message.contains(".wal") {
        return Err(fail("wrong refusal", stage, message));
    }
    let wal_files = |tree: &Files| {
        let wal = tree.iter().filter(|(path, _)| path.ends_with(".wal"));
        wal.map(|(path, data)| (path.clone(), data.clone()))
            .collect::<Files>()
    };
    if wal_files(&read_tree(root)) != wal_files(files) {
        return Err(fail("WAL changed", stage, wal.to_string()));
    }

    let trimmed = &mut files.get_mut(wal).unwrap();
    trimmed.truncate(valid);
    std::fs::write(root.join(wal), &**trimmed).unwrap();
    Ok(())
}

fn run_image(
    recording: &Recording,
    files: &Files,
    acked: usize,
    issued: usize,
    case: u64,
) -> Result<(), Failure> {
    let image = TempDir::new().unwrap();
    write_tree(image.path(), files);
    let root = image.path().to_path_buf();
    let mut files = files.clone();
    let newest = files.keys().filter(|p| p.ends_with(".wal")).max().cloned();
    while let Some((wal, valid)) = torn_wal(&files) {
        // Finished WAL files only tear if rotation can run ahead of their sync.
        if recording.durability == Durability::Strict && Some(&wal) != newest.as_ref() {
            return Err(fail("older WAL torn", "before recovery", wal));
        }
        expect_refusal(recording.durability, &root, &mut files, &wal, valid)?;
        REFUSALS.fetch_add(1, Ordering::Relaxed);
    }
    let expected = match recording.durability {
        Durability::Strict => Expected::Prefix(allowed_states(recording, acked, issued)),
        Durability::Default => {
            let committed = recording.writes_through(blob_sequence(&files));
            Expected::Committed {
                committed: recording.state_after(committed),
                later: recording.writes[committed..issued].to_vec(),
            }
        }
    };
    let mut rng = Rng::new(recording.seed ^ case.wrapping_mul(0xA5A5));
    let follow_up = generate(&mut rng, FOLLOW_UP_WRITES, 100_000);
    let run = move || async move {
        let result = recover(root, expected, follow_up).await;
        result.map_err(|mut failure| {
            failure.log = LAST_ERROR.take();
            failure
        })
    };
    match on_executor(recording.durability, run) {
        Finished::Done(result) => result,
        Finished::Panicked(message) => Err(fail("panic", "recovery", message)),
        Finished::TimedOut => Err(fail("hang", "recovery", String::new())),
    }
}

// =============================================================================
// Crash points
// =============================================================================

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Model {
    Kill(Durability),
    PowerLoss(Durability, DirDurability),
}

fn cases() -> usize {
    env_number("LARK_CRASH_CASES").unwrap_or(200) as usize
}

fn seed() -> u64 {
    env_number("LARK_CRASH_SEED").unwrap_or(1)
}

fn env_number(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.parse().ok()
}

fn is_file(op: Option<&Op>, name: &str) -> bool {
    let path = match op {
        Some(Op::Write { path, .. })
        | Some(Op::Sync { path })
        | Some(Op::Truncate { path, .. }) => path,
        _ => return false,
    };
    path.ends_with(name)
}

/// Journal positions worth crashing at, most valuable first. A crash after op
/// `i` has applied `ops[..=i]`.
fn crash_points(ops: &[Op], rng: &mut Rng) -> Vec<usize> {
    let mut windows = Vec::new();
    let mut compaction = Vec::new();
    let mut wal = Vec::new();
    for (i, op) in ops.iter().enumerate() {
        let next = ops.get(i + 1);
        let after_sidecar_sync = matches!(op, Op::Sync { .. }) && is_file(Some(op), "sidecar.lark");
        let before_blob_sync = matches!(next, Some(Op::Sync { .. })) && is_file(next, "blob.lark");
        if after_sidecar_sync || before_blob_sync {
            windows.push(i);
        } else if is_file(Some(op), "blob.lark") || is_file(Some(op), "sidecar.lark") {
            compaction.push(i);
        } else if is_file(Some(op), ".wal") {
            wal.push(i);
        }
    }
    let mut shuffle = |points: &mut Vec<usize>| {
        for i in (1..points.len()).rev() {
            points.swap(i, rng.below(i + 1));
        }
    };
    shuffle(&mut windows);
    shuffle(&mut compaction);
    shuffle(&mut wal);

    // Take turns, so a low case count still reaches every kind of point.
    let mut order = Vec::new();
    let lists = [windows, compaction, wal];
    for i in 0..lists.iter().map(Vec::len).max().unwrap_or(0) {
        for list in &lists {
            order.extend(list.get(i));
        }
    }
    order
}

#[derive(Default)]
struct Tally {
    images: usize,
    failures: BTreeMap<(&'static str, &'static str, String), (usize, String, u64)>,
}

impl Tally {
    fn add(&mut self, failure: Failure, case: u64) {
        let key = (failure.kind, failure.stage, cause_of(&failure));
        let entry = self
            .failures
            .entry(key)
            .or_insert((0, failure.detail.clone(), case));
        entry.0 += 1;
    }

    fn report(&self, model: Model) -> String {
        let failed: usize = self.failures.values().map(|f| f.0).sum();
        let mut out = format!("{model:?}: {failed} of {} images failed", self.images);
        for ((kind, stage, cause), (count, detail, case)) in &self.failures {
            let detail: String = detail.chars().take(160).collect();
            out += &format!("\n  {count} {kind} at {stage} (first: case {case})");
            out += &format!("\n      {detail}");
            if !cause.is_empty() {
                out += &format!("\n      log: {cause}");
            }
        }
        out
    }

    fn assert_clean(&self, model: Model) {
        assert!(self.failures.is_empty(), "{}", self.report(model));
        let refusals = REFUSALS.load(Ordering::Relaxed);
        println!(
            "{}; {refusals} torn WALs refused so far",
            self.report(model)
        );
    }
}

fn keep_image(files: &Files, model: Model, case: u64) {
    if let Ok(keep) = std::env::var("LARK_CRASH_KEEP") {
        let dir = Path::new(&keep).join(format!("{model:?}-{case}"));
        write_tree(&dir, files);
    }
}

fn run_model(model: Model, count: usize) -> Tally {
    let durability = match model {
        Model::Kill(durability) | Model::PowerLoss(durability, _) => durability,
    };
    let recording = record(seed(), durability);
    let mut rng = Rng::new(seed() ^ 0x5EED);
    let points = crash_points(&recording.ops, &mut rng);
    assert!(!points.is_empty(), "the workload should compact");

    // A power loss has many outcomes at each point, so revisit them.
    let per_point = match model {
        Model::Kill(_) => 1,
        Model::PowerLoss(..) => 4,
    };
    let mut plan: Vec<(usize, u64)> = Vec::new();
    for round in 0..per_point {
        plan.extend(points.iter().map(|point| (*point, round)));
    }
    plan.truncate(count);

    // Visit in journal order so each image needs only the ops since the last.
    let mut indexed: Vec<(usize, u64, u64)> = plan
        .iter()
        .enumerate()
        .map(|(case, (point, round))| (*point, *round, case as u64))
        .collect();
    indexed.sort();

    let dirs = match model {
        Model::PowerLoss(_, dirs) => dirs,
        Model::Kill(_) => DirDurability::Journaled,
    };
    let mut disk = Disk::new(&recording.base, dirs);
    let (mut position, mut acked, mut issued) = (0, 0, 0);
    let mut tally = Tally::default();

    for (point, _round, case) in indexed {
        while position <= point {
            let op = &recording.ops[position];
            disk.apply(op);
            match op {
                Op::WriteIssued => issued += 1,
                Op::WriteAcked => acked += 1,
                _ => {}
            }
            position += 1;
        }
        let mut image_rng = Rng::new(seed() ^ (case << 16) ^ 0xC0FFEE);
        let files = match model {
            Model::Kill(_) => disk.after_kill(),
            Model::PowerLoss(..) => disk.after_power_loss(&mut image_rng),
        };
        tally.images += 1;
        if let Err(failure) = run_image(&recording, &files, acked, issued, case) {
            keep_image(&files, model, case);
            tally.add(failure, case);
        }
    }
    tally
}

// =============================================================================
// Controls: these show the harness itself can be trusted
// =============================================================================

/// If this fails, some durable write isn't going through the recorder.
#[test]
fn replaying_the_whole_journal_rebuilds_the_real_directory() {
    let recording = record(seed(), Durability::Strict);
    let compactions = recording
        .ops
        .iter()
        .filter(|op| matches!(op, Op::Sync { path } if path.ends_with("sidecar.lark")))
        .count();
    assert!(compactions >= 5, "only {compactions} compactions recorded");

    let mut disk = Disk::new(&recording.base, DirDurability::Journaled);
    for op in &recording.ops {
        disk.apply(op);
    }
    let replayed = disk.after_kill();
    let real = read_tree(recording.root.path());

    let names = |files: &Files| files.keys().cloned().collect::<Vec<_>>();
    assert_eq!(names(&replayed), names(&real));
    for (path, data) in &real {
        assert!(&replayed[path] == data, "{path} differs from the replay");
    }
}

/// An image with nothing lost recovers, so failures below come from the crash
/// and not from the recovery script.
#[test]
fn an_intact_directory_recovers() {
    let recording = record(seed(), Durability::Strict);
    let files = read_tree(recording.root.path());
    let all = recording.writes.len();
    run_image(&recording, &files, all, all, 0).unwrap();
}

/// The checks can fail: an image missing an acknowledged write is reported.
#[test]
fn a_lost_acknowledged_write_is_detected() {
    // A run can end just after a rotation, with every entry already compacted and
    // nothing left to lose, so use the first one that doesn't.
    let (recording, mut files, last_wal) = (seed()..seed() + 20)
        .find_map(|seed| {
            let recording = record(seed, Durability::Strict);
            let files = read_tree(recording.root.path());
            let wal = newest_uncompacted_wal(&files)?;
            Some((recording, files, wal))
        })
        .expect("a run with an uncompacted WAL entry");
    let all = recording.writes.len();

    let data = files.get_mut(&last_wal).unwrap();
    let keep = data[..data.len() - 1]
        .iter()
        .rposition(|b| *b == b'\n')
        .map_or(0, |at| at + 1);
    data.truncate(keep);

    let failure = run_image(&recording, &files, all, all, 0).expect_err("must notice");
    assert_eq!(failure.kind, "wrong data", "{failure:?}");
}

// =============================================================================
// Crash models
// =============================================================================

/// A process kill keeps everything issued, so nothing acknowledged may be lost
/// and recovery must always finish.
#[test]
fn recovers_from_a_process_kill() {
    let model = Model::Kill(Durability::Strict);
    run_model(model, cases()).assert_clean(model);
}

/// A power loss on ext4.
#[test]
fn recovers_from_a_power_loss_on_ext4() {
    let model = Model::PowerLoss(Durability::Strict, DirDurability::Journaled);
    run_model(model, cases()).assert_clean(model);
}

/// A power loss where only an fsync of the directory saves a new entry.
#[test]
fn recovers_from_a_power_loss_with_posix_directories() {
    let model = Model::PowerLoss(Durability::Strict, DirDurability::Posix);
    run_model(model, cases()).assert_clean(model);
}

/// Default durability keeps the blob intact through a kill, whatever it loses.
#[test]
fn default_durability_recovers_from_a_process_kill() {
    let model = Model::Kill(Durability::Default);
    run_model(model, cases()).assert_clean(model);
}

/// Default durability keeps the blob intact through a power loss, whatever it loses.
#[test]
fn default_durability_recovers_from_a_power_loss_on_ext4() {
    let model = Model::PowerLoss(Durability::Default, DirDurability::Journaled);
    run_model(model, cases()).assert_clean(model);
}
