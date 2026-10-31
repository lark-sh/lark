#!/bin/bash
# Checks the REST API of a real lark-server and lark-edge. See src/main.rs.
#
# Everything runs in the Linux dev container, because lark-server needs
# io_uring. lark-edge is built separately and copied in.
#
# Usage (from repo root):
#   ./tools/rest-conformance/run.sh                     # fixed checks, 100 scripts
#   ./tools/rest-conformance/run.sh --cases 0           # fixed checks only
#   ./tools/rest-conformance/run.sh --slow --cases 20   # restarts and compaction
#   ./tools/rest-conformance/run.sh --seed 42           # replay a failure
#
# Environment:
#   SKIP_BUILD   Set to 1 to reuse the binaries from the last run.

set -e

REPO_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
PROJECT="lark"
DEV_IMAGE="${DEV_IMAGE:-lark-dev:latest}"
GO_IMAGE="golang:$(sed -n 's/^go //p' "$REPO_DIR/edge/go.mod")-alpine"

# Shared with the Makefile, so builds reuse the same compiled state.
CACHE_MOUNTS=(
    -v "$REPO_DIR":/work
    -v "$PROJECT-cargo-registry":/root/.cargo/registry
    -v "$PROJECT-cargo-git":/root/.cargo/git
    -v "$PROJECT-cargo-target":/work/target
)

if [ "${SKIP_BUILD:-0}" != "1" ]; then
    docker run --rm -v "$REPO_DIR/edge":/src -v "$PROJECT-cargo-target":/out \
        -v "$PROJECT-go-cache":/root/.cache/go-build \
        -v "$PROJECT-go-mod":/go/pkg/mod \
        -w /src -e CGO_ENABLED=0 "$GO_IMAGE" go build -o /out/debug/lark-edge .
fi

BUILD=true
if [ "${SKIP_BUILD:-0}" != "1" ]; then
    BUILD="cargo build -p lark-server -p lark-compact -p lark-rest-conformance"
fi

docker run --rm --security-opt seccomp=unconfined "${CACHE_MOUNTS[@]}" -w /work "$DEV_IMAGE" \
    bash -c "$BUILD && exec target/debug/lark-rest-conformance \
        --server-bin target/debug/lark-server \
        --edge-bin target/debug/lark-edge \
        --compact-bin target/debug/lark-compact \
        --work-dir \$(mktemp -d) $*"
