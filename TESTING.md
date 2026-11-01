# Testing

If you're considering Lark as the database behind a production app, you may have questions about our testing strategy and how we work to ensure that we a) remain compatible with Firebase SDKs and b) how we make sure that your data isn't lost. This document lays out the claims Lark makes and the test suites that back each one. Every claim comes with the command that checks it, so you can verify them yourself instead of taking our word for it.

Everything below is stated for the standard deployment described in [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md): a single `lark-server` that scales vertically, with one or more `lark-edge` gateways in front. That is the configuration the test suites exercise end to end. Running multiple `lark-server` nodes is possible (Lark Cloud does), but as with the deployment docs, validating a horizontally-scaled topology will require additional testing beyond what's here.

If you're on MacOS, note that all Rust commands run inside a Linux dev container, because the server needs `io_uring`. The Makefile handles this transparently; see [CONTRIBUTING.md](CONTRIBUTING.md#development-setup) for the one-time setup.

## Firebase Compatibility

Lark's compatibility claim is checked with the test suite Google wrote for its own SDK. The `@firebase/database-compat` package in [firebase-js-sdk](https://github.com/firebase/firebase-js-sdk) ships a mocha suite covering queries, ordering, transactions, server values, `DataSnapshot` semantics, and promise behavior. We run that suite, unmodified, against a real Lark stack:

```bash
./test/run-firebase-sdk.sh                    # full suite
./test/run-firebase-sdk.sh query              # one test file
BLOB=1 ./test/run-firebase-sdk.sh             # on blob-backed storage
```

The script clones a pinned commit of firebase-js-sdk into `.cache/`, builds the package, brings up an ephemeral Lark stack, and points the SDK's tests at it. Anywhere Lark diverges from what the SDK expects, a test fails.

If your app works against Firebase through the JS SDK, this suite is the evidence that the behaviors it depends on hold here too. Conributors should re-run for any change a client can observe: wire protocol, query evaluation, write semantics. The script header documents the options, including how to bump the pinned SDK commit.

## Durability

Lark's job is to keep the data you give it. This section covers how it does that, what happens when something fails, and the trade you're making with each durability mode.

### How your data stays safe

**The write-ahead log (WAL) comes first.** Every write is added to the end of a log file for its database before anything else happens. If the server stops for any reason, it replays the log on the way back up.

**The blob is the long-term home for your data.** In the background, Lark folds finished WAL files into `blob.lark` in batches. Each batch writes its new data and syncs it to disk first. It then saves a copy of every change to existing data before making any of those changes, and only marks the batch as done once all of it is on disk.

**A crash at any point in that process is recoverable.** If a batch is cut off, Lark finishes it from the saved copy on the next load, then replays any WAL the blob doesn't hold yet. You don't need to run anything, and there's no separate recovery mode: this is what every startup does.

**If the WAL is damaged, Lark stops and tells you.** It won't guess at damaged data, so that database refuses to load until someone has looked at it. Your other databases keep running.

### What happens when something goes wrong

| What happened | What Lark does | What you need to do |
|---|---|---|
| Normal restart or deploy (SIGTERM) | Saves everything before exiting | Nothing |
| The server process crashes | Recovers on restart | Nothing |
| The machine crashes or loses power | Recovers on restart | Usually nothing. See [If a database refuses to load](#if-a-database-refuses-to-load) |
| A disk fails or is lost | Can't recover by itself | Restore from a [backup](docs/BACKUP.md) |

How much recent data a crash can cost depends on your durability mode.

### Choosing a durability mode

Lark doesn't confirm every write the same way. By default it confirms a write immediately and saves it to the WAL a moment later, which is fast but leaves a short window where a crash can lose it. You choose where to sit on that trade with two settings, covered in [DEPLOYMENT.md](docs/DEPLOYMENT.md#durability):

| Mode | Process crash can lose | Machine crash can lose | Write speed |
|---|---|---|---|
| **Default** | Up to the last 2 seconds | Up to about 30 seconds on a default Linux install | Fastest |
| **Power-safe** | Up to the last 2 seconds | Up to the last 2 seconds | Fast |
| **Synchronous** | Nothing confirmed | Up to about 30 seconds on a default Linux install | Slower |
| **Strict** | Nothing confirmed | Nothing confirmed | Slowest |

In every mode, a crash can only cost you recent writes. Data that's already in the blob is safe.

The settings apply to the whole server. If losing a confirmed write would be a serious problem for your app, use strict. If a few seconds of lost writes is acceptable, the default gives you much faster writes.

### If a database refuses to load

Lark logs this and leaves the file exactly as it was:

```text
[STORAGE INTEGRITY] my-project/my-db: Failed to initialize: WAL corruption in ".../wal/000042.wal" at line 118 ...
```

After a machine crash, this nearly always means the last line was only partly written. In strict mode that write was never confirmed to a client, so nothing you were promised is lost.

To fix it, open the file and remove the damaged line, keep a copy first if you'd like one, and the database loads on its next connection. If the damage isn't on the last line, or you didn't just have a crash, treat it as a damaged disk: check the disk and consider restoring from a backup.

### What Lark can't protect against

Lark relies on your disk telling the truth. If a drive confirms a sync without actually saving the data, no software can recover what's gone. Lark also doesn't checksum your data in the blob or WAL, so slow corruption on a failing disk may not be noticed until something tries to read it. Use reliable storage, consider RAID or a filesystem with checksums, and keep [backups](docs/BACKUP.md).

### How we test this

We test the two things that go wrong in real life: the process dying, and the machine dying.

[`tools/chaos-monkey`](tools/chaos-monkey) plays the part of your gateway. It writes to a real `lark-server`, kills the process at random moments, restarts it, and checks every confirmed write against its own record. That includes compaction: it can run a full compaction between the kill and the restart. Runs are seeded, so a failure can be replayed.

```bash
./tools/chaos-monkey/run.sh                      # 1-hour run, default durability
./tools/chaos-monkey/run.sh --durability strict  # nothing confirmed may be lost
./tools/chaos-monkey/run.sh --duration 5h
./tools/chaos-monkey/run-seed.sh                 # short fixed-seed run for repro
```

A killed process can't show what a power cut does, so crash images cover that. This test records every file operation a real server makes, rebuilds what a machine crash could leave on disk at chosen moments, and recovers each result using the same server code. It then keeps writing and restarts twice, because some damage only appears later.

```bash
cargo test -p lark-server --features io-trace --test integration_crash
```

These run on a model of a disk, not real hardware, so they show that Lark's write ordering holds up under that model. Unit tests in `lark-blob` and the `integration_persistence` suite cover the storage engine underneath.

### For contributors

A write takes this path:

1. It's added to an in-memory buffer, which is flushed to the newest WAL file. When that happens depends on the durability mode.
2. At 5MB the WAL rotates, and the per-core `StorageWorker` picks up finished files.
3. The worker applies a batch in this order: write new data to free or appended space, sync it, write and sync the sidecar with a copy of every change to existing bytes, make those changes, sync the blob, drop the copy, then write `sequence`.

`sequence` is the commit point. Until it's written, the WAL can replay the batch, and the saved copy repairs anything a crash tore. Two rules keep this safe: a batch doesn't touch live bytes before its flush, and a sidecar only counts when its sequence matches.

Run the crash images for any change to compaction, the WAL or the order of durable writes. They aren't in CI. `LARK_CRASH_CASES` and `LARK_CRASH_SEED` change the run, and `LARK_CRASH_KEEP` saves failing images. See [WRITE_LIFECYCLE.md](docs/WRITE_LIFECYCLE.md) and [`blob/README.md`](blob/README.md#crash-safety) for detail.

## REST Conformance

The SDK suite only sees what the SDK leaves after normalizing a response, so it can't tell `{"a": null}` from `{}`. [`tools/rest-conformance`](tools/rest-conformance) checks what a strict client sees instead, by talking to a real `lark-edge` and `lark-server` over plain HTTP:

```bash
./tools/rest-conformance/run.sh                     # fixed checks, 100 random scripts
./tools/rest-conformance/run.sh --slow --cases 20   # with restarts and compaction
./tools/rest-conformance/run.sh --seed 42           # replay a failure
```

- Fixed checks cover the behavior in Firebase's public REST docs, each citing its page. Where the docs don't commit to something, the check doesn't either. Behavior we know differs is listed in `KNOWN_DIFFERENCES` in `src/spec.rs`.
- Random scripts run writes, reads, shallow reads, queries and conditional writes, and compare every response with a JSON model. Databases are evicted, restarted and compacted between requests, none of which a client should be able to see. Failures shrink to a minimal script.

The expected answers come from us reading the docs, not from Firebase itself, so this can't catch a rule we've misread in both the code and the model.

## Correctness for Edge Cases

The Firebase data model has corners that bite people: the array-to-object coercion rule (a node reads back as a JSON array only when every key is a canonical non-negative integer and `maxKey < 2 * numKeys`), mixed-type sort ordering, priorities, server values, and tainted writes (a rejected write silently invalidates later writes that depended on it). Each of these has a dedicated integration suite in `server/tests/`, 22 suites in all, covering queries and query views, incremental sort, transactions, subscriptions, onDisconnect, eviction and re-promotion of idle data, and volatile paths.

Every suite boots a real in-process server and speaks the wire protocol through the shared harness in `server/tests/common/mod.rs`. Nothing is mocked below the client.

```bash
make test-all                                  # everything
cargo test -p lark-server --test integration_arrays -j 2   # one suite, from `make shell`
```

## Security

Rules enforcement is tested where it runs, in the server (`integration_rules`), and token verification is tested where it runs, at the edge: the Go suites cover JWT validation for all four supported token formats, plus the proxy and long-poll transports and the edge-to-server wire framing.

```bash
cd edge && go test ./...
```

## What runs when

| Suite | Command | On every PR |
|---|---|---|
| Rust unit tests | `make test` | yes |
| Rust integration suites | `make test-all` | yes, except the `#[ignore]`d slow suite |
| Go edge tests | `cd edge && go test ./...` | yes |
| Firebase SDK suite | `./test/run-firebase-sdk.sh` | no; run for client-observable changes |
| REST conformance | `./tools/rest-conformance/run.sh` | yes, with more cases nightly |
| Crash images | `cargo test -p lark-server --features io-trace --test integration_crash` | no; run for compaction and WAL changes |
| Chaos monkey | `./tools/chaos-monkey/run.sh` | no; run for write-path changes |
| Randomized model tests, high case counts | `PROPTEST_CASES=1000 cargo test -p lark-server --test integration_read_model` | nightly |

CI ([.github/workflows/ci.yml](.github/workflows/ci.yml)) also enforces `cargo fmt`, `cargo clippy` with warnings denied, `gofmt`, and `go vet` on every pull request.

If you're contributing and want to know where a new test belongs, see the testing section of [CONTRIBUTING.md](CONTRIBUTING.md#testing).
