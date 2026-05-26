#!/bin/bash
# Run the complete Lark test suite.
#
# This script assumes it's running inside the Linux dev container — the
# Makefile target `make test-all` wraps it with the right `docker run`
# invocation. Running directly on macOS will fail because Glommio needs
# Linux/io_uring.

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

pass() { echo -e "${GREEN}PASS${NC} $1"; }
fail() { echo -e "${RED}FAIL${NC} $1"; exit 1; }

run() {
    local label="$1"
    shift
    echo ""
    echo "=== $label ==="
    if "$@"; then
        pass "$label"
    else
        fail "$label"
    fi
}

# -j 2 keeps parallel linking under control so the linker doesn't OOM on
# memory-constrained machines.

# integration_compact.rs shells out to the `lark-compact` binary, which
# `cargo test -p lark-server` doesn't transitively build. Build it
# explicitly so the integration tests find it in target/debug/.
run "Build lark-compact (needed by integration_compact)" \
    cargo build -p lark-compact -j 2

run "Lib tests" \
    cargo test --workspace --lib -j 2

run "Integration tests (all, non-ignored)" \
    cargo test --workspace --tests -j 2

run "Storage worker tests (ignored, slow)" \
    cargo test -p lark-server --test integration_storage_worker -j 2 -- --ignored --test-threads=1

echo ""
echo -e "${GREEN}All test suites passed.${NC}"
