# Contributing to Lark

Thanks for your interest in contributing! This guide covers how to set up a
development environment, the contribution workflow, and the conventions we follow.

By participating, you agree to abide by our
[Code of Conduct](CODE_OF_CONDUCT.md). To report a **security** vulnerability,
do **not** open an issue — follow [SECURITY.md](SECURITY.md).

## Ways to contribute

- **Bugs & features:** open an issue (templates will guide you). For questions and
  ideas, use [Discussions](https://github.com/lark-sh/lark/discussions).
- **Code & docs:** pull requests welcome. For anything large, please open an issue
  first so we can agree on the approach before you invest the time.

## Contributor License Agreement (CLA)

Lark is AGPL-licensed, and Bag of Holding, Inc. maintains the option to offer it
under commercial terms as well. To keep that possible, **all contributors must
sign a CLA** before their first contribution is merged:

- **Individuals:** the [Individual CLA](docs/cla/individual-cla.md) is signed
  automatically on your first pull request — a bot comments with instructions, and
  you sign by replying with the one-line statement it gives you. One signature
  covers all your future PRs.
- **Contributing as part of your job:** your employer should also have a
  [Corporate CLA](docs/cla/corporate-cla.md) on file (signed once, returned to
  team@lark.sh).

## Development setup

### Prerequisites

- **Rust** — latest stable (1.78+).
- **Docker** — required on macOS: Glommio uses `io_uring` (Linux-only), so the
  Makefile transparently runs Rust commands inside a Linux dev container.
- **Node.js** — for building the dashboard SPA.
- **Go** — for building `lark-edge`.

### Common Makefile targets

The root `Makefile` is the canonical surface; `make help` lists everything. The
ones you'll use most:

```bash
make dev-image     # one-time: build the Linux dev container image
make check         # cargo check --workspace inside the dev container
make test          # cargo test --lib (the fast common case)
make test-all      # full integration suite via test-everything.sh
make up            # docker compose up — brings up lark-server + lark-edge
```

### Building & running

```bash
make build-server  # release lark-server (in the dev container)
make build-edge    # cross-compile lark-edge to Linux
make build         # both
make build-spa     # dashboard SPA only

make up            # whole stack via docker compose (dashboard at :8080/admin/)
make shell         # shell inside the Linux dev container
# from inside the dev container, run the server directly in emulator mode:
cargo run -p lark-server -- --id=local-1 --hostname=localhost --proxy-port=7779 --emulator
```

### Testing

```bash
make test                                   # lib tests (fast)
make test-all                               # full integration suite
# a specific suite, from `make shell`:
cargo test -p lark-server --test integration_rules -j 2
# Firebase SDK wire-compat regression suite:
./test/run-firebase-sdk.sh
```

`-j 2` keeps parallel linking under control so the linker isn't OOM-killed on
memory-constrained machines. The Rust integration test harness
(`TestServer`/`TestClient`) lives in `server/tests/common/mod.rs`. See the
README's [Data model](README.md#data-model) and [Storage](README.md#storage)
sections for the internals you'll most often touch.

## Making changes

Common extension points (the relevant source files are noted inline):

- **A new wire operation:** define the message in
  `server/src/protocol/messages.rs`, add it to `InboxMessage` and handle it in the
  `run()` loop in `server/src/db/database.rs`, then add tests under
  `server/tests/integration_*.rs`.
- **A rules built-in:** add a method on `DataSnapshot`
  (`server/src/rules/snapshot.rs`) and dispatch it in
  `server/src/rules/expr/eval.rs`.
- **A query feature:** extend `Query` (`server/src/db/query.rs`), parsing
  (`server/src/protocol/messages.rs`), and evaluation.

### Code style

- Use `tracing` macros (`debug!`, `trace!`, `warn!`), not `println!`.
- Prefer `Option::map`/`and_then` over `if let` when transforming.
- Use `?` for error propagation; keep functions short; add `///` doc comments on
  public APIs.
- New behavior needs tests. If it's user-facing, update the docs and add a
  `CHANGELOG.md` entry under `[Unreleased]`.

## Pull request process

1. Fork and branch off `main`.
2. Make your change with tests; run `make test` (and relevant integration suites).
3. Open the PR using the template. Keep it focused — smaller PRs review faster.
4. Sign the CLA when the bot prompts you (first PR only).
5. CI must pass and a maintainer must approve before merge.

## Versioning

Lark follows [Semantic Versioning](https://semver.org). We're currently `0.x`:
while we stabilize the APIs, wire protocol, and on-disk format, minor releases may
include breaking changes (documented in `CHANGELOG.md`). The `1.0.0` line lands at
the public release.
