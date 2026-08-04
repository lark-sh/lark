# Testing

If you're considering Lark as the database behind a production app, you may have questions about our testing strategy and how we work to ensure that we a) remain compatible with Firebase SDKs and b) how we make sure that your data isn't lost. This document lays out the claims Lark makes and the test suites that back each one. Every claim comes with the command that checks it, so you can verify them yourself instead of taking our word for it.

Everything below is stated for the standard deployment described in [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md): a single `lark-server` that scales vertically, with one or more `lark-edge` gateways in front. That is the configuration the test suites exercise end to end. Running multiple `lark-server` nodes is possible (Lark Cloud does), but as with the deployment docs, validating a horizontally-scaled topology will require additional testing beyond what's here.

If you're on MacOS, note that all Rust commands run inside a Linux dev container, because the server needs `io_uring`. The Makefile handles this transparently; see [CONTRIBUTING.md](CONTRIBUTING.md#development-setup) for the one-time setup.

## Firebase Compatibility

Lark's compatibility claim is checked with the test suite Google wrote for its own SDK. The `@firebase/database-compat` package in [firebase-js-sdk](https://github.com/firebase/firebase-js-sdk) ships a mocha suite covering queries, ordering, transactions, server values, `DataSnapshot` semantics, and promise behavior. We run that suite, unmodified, against a real Lark stack:

```bash
./test/run-firebase-sdk.sh                    # full suite
./test/run-firebase-sdk.sh query              # one test file
```

The script clones a pinned commit of firebase-js-sdk into `.cache/`, builds the package, brings up an ephemeral Lark stack, and points the SDK's tests at it. Anywhere Lark diverges from what the SDK expects, a test fails.

If your app works against Firebase through the JS SDK, this suite is the evidence that the behaviors it depends on hold here too. Conributors should re-run for any change a client can observe: wire protocol, query evaluation, write semantics. The script header documents the options, including how to bump the pinned SDK commit.

## Durability

The durability contract has two modes, chosen per deployment:

- Default: the write-ahead log is flushed every 2 seconds. A write acknowledged inside that window may be lost if the server dies before the flush; any write acknowledged before the window must survive.
- Strict (`--durability strict` in the harness, fsync tuning in deployment): the WAL is fsync'd before the ACK is sent. No acknowledged write may be lost.

The tool that holds Lark to this contract is [`tools/chaos-monkey`](tools/chaos-monkey), a standalone binary that plays the role of the gateway: it writes continuously to a real `lark-server` over the actual wire protocol, SIGKILLs the process at random moments, restarts it, and compares what the server recovers against its own ground-truth model of every acknowledged write.

```bash
./tools/chaos-monkey/run.sh                      # 1-hour run, default durability
./tools/chaos-monkey/run.sh --durability strict  # zero-loss contract
./tools/chaos-monkey/run.sh --duration 5h
./tools/chaos-monkey/run-seed.sh                 # short fixed-seed run for repro
```

Some details of the harness:

- It verifies data before each kill as well as after the restart, so a violation can be attributed to recovery rather than to a live-server bug.
- It inspects the raw on-disk state after each crash: WAL files must be valid JSONL, the blob and sequence files must parse.
- Between kill and restart it can run `lark-compact` to force a full blob re-compaction, so recovery is verified through the compactor, the piece of a storage engine where bugs are most expensive.
- The operation mix is weighted toward writes that have actually broken things in the past: multi-path updates at the root (a real WAL-replay bug), transactions on blob-backed paths, unicode and deeply nested keys, burst writes that force WAL rotation.
- Runs are seeded. A violating run replays exactly with the same `--seed`, so a failure is a reproducible artifact rather than an anecdote.
- It can run with security rules enabled, so durability is exercised with the authorization path in play.

To be precise about the crash model: what the harness kills is the process, at an arbitrary instant, including mid-flush and mid-compaction. It does not simulate power loss or torn writes at the device layer. In strict mode the claim is that the WAL has been fsync'd before the client sees an ACK.

Under the chaos-monkey layer, the storage engine has conventional coverage of its own: the `lark-blob` crate carries roughly 200 unit tests over the on-disk format, sessions, free list, dictionary, and incremental compaction, and the `integration_persistence` suite covers WAL replay, graceful-shutdown flushing, deletion persistence, and write coalescing. A slow `#[ignore]`d suite (`integration_storage_worker`) exercises real compaction timing; it runs as part of `make test-all`.

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
| Chaos monkey | `./tools/chaos-monkey/run.sh` | no; run for write-path changes |

CI ([.github/workflows/ci.yml](.github/workflows/ci.yml)) also enforces `cargo fmt`, `cargo clippy` with warnings denied, `gofmt`, and `go vet` on every pull request.

If you're contributing and want to know where a new test belongs, see the testing section of [CONTRIBUTING.md](CONTRIBUTING.md#testing).
