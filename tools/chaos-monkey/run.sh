#!/bin/bash
# Run Lark Chaos Monkey.
#
# Everything runs inside the Linux dev container because:
#   - lark-server requires Linux (io_uring/Glommio)
#   - chaos monkey needs to SIGKILL the server process and inspect its files
#
# Usage (from repo root):
#   ./tools/chaos-monkey/run.sh                    # Run for 1 hour (default)
#   ./tools/chaos-monkey/run.sh --duration 5h      # Run for 5 hours
#   ./tools/chaos-monkey/run.sh --seed 42          # Reproducible run
#   ./tools/chaos-monkey/run.sh --durability strict # interval=0 + fsync, zero-loss contract
#   SKIP_BUILD=1 ./tools/chaos-monkey/run.sh       # Skip compilation
#   DEBUG=1 ./tools/chaos-monkey/run.sh            # Trace-level logging
#
# Environment:
#   SKIP_BUILD   - Set to 1 to skip building (uses cached binaries)
#   DEBUG        - Set to 1 for trace logging
#   DURATION     - Override duration (default: 1h)
#   SEED         - RNG seed for reproducible runs

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

PROJECT="lark"
DEV_IMAGE="${DEV_IMAGE:-lark-dev:latest}"
CONTAINER_NAME="lark-chaos-monkey"

# Shared cargo cache + target volumes — kept in sync with the Makefile
# so `make check` / `make build` and `./run.sh` reuse the same compiled
# state.
DOCKER_CACHE_MOUNTS=(
    -v "$REPO_DIR":/work
    -v "$PROJECT-cargo-registry":/root/.cargo/registry
    -v "$PROJECT-cargo-git":/root/.cargo/git
    -v "$PROJECT-cargo-target":/work/target
)

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log()  { echo -e "${GREEN}[CHAOS]${NC} $1"; }
warn() { echo -e "${YELLOW}[CHAOS]${NC} $1"; }
err()  { echo -e "${RED}[CHAOS]${NC} $1"; }
info() { echo -e "${CYAN}[CHAOS]${NC} $1"; }

cleanup() {
    log "Cleaning up..."
    if docker ps -q -f name=$CONTAINER_NAME 2>/dev/null | grep -q .; then
        docker stop $CONTAINER_NAME >/dev/null 2>&1 || true
        docker rm $CONTAINER_NAME >/dev/null 2>&1 || true
    fi
    docker rm $CONTAINER_NAME >/dev/null 2>&1 || true
}
trap cleanup EXIT

check_prerequisites() {
    if ! command -v docker &> /dev/null; then
        err "Docker is required but not installed"
        exit 1
    fi
    if ! docker info &> /dev/null; then
        err "Docker daemon is not running"
        exit 1
    fi
}

ensure_image() {
    if ! docker images "$DEV_IMAGE" -q 2>/dev/null | grep -q .; then
        log "Dev image $DEV_IMAGE not built yet — running 'make dev-image'..."
        (cd "$REPO_DIR" && make dev-image)
    fi
}

build_binaries() {
    if [ "${SKIP_BUILD:-0}" = "1" ]; then
        log "Skipping build (SKIP_BUILD=1), using cached binaries"
        return
    fi

    log "Building lark-server, lark-compact, lark-chaos-monkey..."
    docker run --rm \
        --security-opt seccomp=unconfined \
        "${DOCKER_CACHE_MOUNTS[@]}" \
        -w /work \
        "$DEV_IMAGE" \
        cargo build -p lark-server -p lark-compact -p lark-chaos-monkey
    log "All binaries built successfully"
}

run_chaos_monkey() {
    docker stop $CONTAINER_NAME 2>/dev/null || true
    docker rm $CONTAINER_NAME 2>/dev/null || true

    local log_level="info"
    if [ "${DEBUG:-0}" = "1" ]; then
        log_level="debug,lark_server=trace"
    fi

    local duration="${DURATION:-1h}"
    local extra_args=""

    if [ -n "$SEED" ]; then
        extra_args="$extra_args --seed $SEED"
    fi

    if [ $# -gt 0 ]; then
        extra_args="$extra_args $@"
    fi

    info ""
    info "============================================"
    info "  Lark Chaos Monkey"
    info "============================================"
    info "  Duration:    $duration"
    info "  Seed:        ${SEED:-random}"
    info "  Log level:   $log_level"
    info "  Container:   $CONTAINER_NAME"
    info "============================================"
    info ""

    log "Starting chaos monkey..."

    # --init so chaos-monkey can manage child processes.
    # --privileged + sysctl tweaks so the test exercises aggressive
    # dirty-page flushing (minimizes data loss window on SIGKILL).
    docker run \
        --name $CONTAINER_NAME \
        --init \
        --privileged \
        --security-opt seccomp=unconfined \
        -e RUST_LOG="$log_level" \
        -e LARK_COLD_TIMEOUT_SECS=5 \
        -e LARK_COLD_STORE_IDLE_SECS=10 \
        "${DOCKER_CACHE_MOUNTS[@]}" \
        -w /work \
        "$DEV_IMAGE" \
        bash -c "
            sysctl -w vm.dirty_writeback_centisecs=100 >/dev/null 2>&1 || true
            sysctl -w vm.dirty_expire_centisecs=300 >/dev/null 2>&1 || true
            sysctl -w vm.dirty_background_ratio=5 >/dev/null 2>&1 || true
            exec /work/target/debug/lark-chaos-monkey \
                --server-bin /work/target/debug/lark-server \
                --compact-bin /work/target/debug/lark-compact \
                --data-dir /tmp/chaos-data \
                --duration $duration \
                $extra_args
        "

    local exit_code=$?

    if [ $exit_code -eq 0 ]; then
        log ""
        log "Chaos monkey completed successfully — no violations found!"
    else
        err ""
        err "Chaos monkey exited with code $exit_code — violations detected!"
    fi

    return $exit_code
}

main() {
    log "Lark Chaos Monkey Runner"
    log ""

    check_prerequisites
    ensure_image
    build_binaries
    run_chaos_monkey "$@"
}

main "$@"
