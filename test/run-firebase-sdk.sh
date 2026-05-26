#!/bin/bash
# Run the Firebase JS SDK's own test suite against the Lark stack.
#
# Lark aims to be wire-compatible with the Firebase Realtime Database. This
# script clones the upstream Firebase JS SDK, builds @firebase/database-compat,
# and runs its mocha test suite against a running lark-server + lark-edge.
# Any divergence from Firebase's expected behavior shows up as a test failure.
#
# The Firebase SDK is cloned into .cache/firebase-sdk/ on first run — it's
# never checked into this repo. Subsequent runs reuse the clone.
#
# Usage (from repo root):
#
#   ./test/run-firebase-sdk.sh                          # full suite
#   ./test/run-firebase-sdk.sh query                    # one test file
#   ./test/run-firebase-sdk.sh transaction "name"       # one test, grep'd
#
# Environment:
#
#   LARK_PORT      Edge port (default 8080 — must match docker-compose.yml).
#   LARK_NS        Project namespace (default "default" — the bootstrapped
#                  first-boot project).
#   SKIP_INSTALL   Set to 1 to skip yarn install + SDK build (assume cached).
#   KEEP_RUNNING   Set to 1 to leave the stack running after tests finish.

set -e

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CACHE_DIR="$REPO_DIR/.cache"
SDK_DIR="$CACHE_DIR/firebase-sdk"
SDK_REPO="https://github.com/firebase/firebase-js-sdk.git"

# Pinned firebase-js-sdk commit.
# To bump: pick a new SHA from https://github.com/firebase/firebase-js-sdk
# (typically after the @firebase/database-compat package has cut a release),
# update here, blow away .cache/firebase-sdk and re-run.
SDK_COMMIT="${SDK_COMMIT:-37a2f6616d2b404f5c5c597afc50dfb75493e0db}"

# The test stack runs side-by-side with the normal dev stack — different
# ports, in-memory only, no persistent volumes.
LARK_PORT="${LARK_PORT:-8090}"
LARK_NS="${LARK_NS:-test-project}"
COMPOSE_FILE="$REPO_DIR/docker-compose.test.yml"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'
log()  { echo -e "${GREEN}[TEST]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
err()  { echo -e "${RED}[ERROR]${NC} $1"; }

check_prereqs() {
    for cmd in docker git node yarn curl; do
        if ! command -v "$cmd" > /dev/null; then
            err "$cmd not found in PATH"
            exit 1
        fi
    done

    if ! docker info > /dev/null 2>&1; then
        err "Docker daemon is not running"
        exit 1
    fi
}

ensure_sdk_cloned() {
    if [ -d "$SDK_DIR/.git" ]; then
        local current
        current="$(git -C "$SDK_DIR" rev-parse HEAD 2>/dev/null || echo none)"
        if [ "$current" = "$SDK_COMMIT" ]; then
            return
        fi
        log "firebase-js-sdk clone is at $current, want $SDK_COMMIT — fetching + checking out..."
        git -C "$SDK_DIR" fetch --depth=1 origin "$SDK_COMMIT"
        git -C "$SDK_DIR" checkout "$SDK_COMMIT"
        # node_modules and build outputs from the previous commit are now
        # stale — clear so the next build step rebuilds against the new tree.
        rm -rf "$SDK_DIR/node_modules" "$SDK_DIR"/packages/*/dist
        return
    fi

    log "Cloning firebase-js-sdk @ ${SDK_COMMIT:0:12} into $SDK_DIR (one-time)..."
    mkdir -p "$SDK_DIR"
    git -C "$SDK_DIR" init -q
    git -C "$SDK_DIR" remote add origin "$SDK_REPO"
    git -C "$SDK_DIR" fetch --depth=1 origin "$SDK_COMMIT"
    git -C "$SDK_DIR" checkout "$SDK_COMMIT"
}

ensure_sdk_built() {
    if [ "${SKIP_INSTALL:-0}" = "1" ]; then
        log "Skipping yarn install (SKIP_INSTALL=1)"
        return
    fi

    cd "$SDK_DIR"

    log "Running yarn install (incremental — fast after first time)..."
    # The firebase-js-sdk dev deps pull in chromedriver / geckodriver /
    # puppeteer for browser-based test suites. We only run the node-based
    # database-compat tests, so skip those downloads. They tend to fail on
    # network blips or 404 against stale Chrome versions.
    CHROMEDRIVER_SKIP_DOWNLOAD=true \
    GECKODRIVER_SKIP_DOWNLOAD=true \
    PUPPETEER_SKIP_DOWNLOAD=true \
        yarn install

    # The test helpers `require('../../../../config/project.json')` at module
    # load. The values are overridden by RTDB_EMULATOR_* env vars at runtime,
    # but the file has to exist with valid JSON for the require to succeed.
    if [ ! -f "config/project.json" ]; then
        log "Writing stub config/project.json (emulator mode overrides the values)..."
        cat > config/project.json <<'JSON'
{
  "apiKey": "fake-api-key",
  "authDomain": "lark-emulator.firebaseapp.com",
  "databaseURL": "https://lark-emulator.firebaseio.com",
  "projectId": "lark-emulator",
  "storageBucket": "lark-emulator.appspot.com",
  "messagingSenderId": "0"
}
JSON
    fi

    if [ ! -d "packages/database-compat/dist" ]; then
        log "Building @firebase/database-compat and its deps..."
        yarn --cwd packages/database-compat build:deps
    fi
}

ensure_stack_up() {
    cd "$REPO_DIR"

    log "Bringing up the ephemeral test stack (docker-compose.test.yml)..."
    # No `-d` reuse logic: the test stack has no persistent state, so a
    # fresh `up` every time is the right semantics. Each test run gets
    # a clean slate.
    docker compose -f "$COMPOSE_FILE" up -d --build

    log "Waiting for lark-edge-test to be ready..."
    # LOCAL_MODE doesn't enable /admin/, so probe a path the mock backend
    # responds to instead. A plain GET on the root returns 404 (no project
    # route), but the listener accepting connections is enough.
    for _ in $(seq 1 60); do
        if curl -s -o /dev/null --max-time 2 "http://localhost:$LARK_PORT/" 2>/dev/null; then
            log "lark-edge-test is ready"
            return
        fi
        sleep 1
    done

    err "lark-edge-test didn't come up within 60s. Last logs:"
    docker compose -f "$COMPOSE_FILE" logs --tail=50 lark-edge-test
    docker compose -f "$COMPOSE_FILE" logs --tail=50 lark-server-test
    exit 1
}

cleanup() {
    if [ "${KEEP_RUNNING:-0}" = "1" ]; then
        log "Leaving the test stack running (KEEP_RUNNING=1)."
        log "Stop it later with: docker compose -f $COMPOSE_FILE down"
        return
    fi
    log "Tearing down the test stack..."
    (cd "$REPO_DIR" && docker compose -f "$COMPOSE_FILE" down)
}

run_tests() {
    cd "$SDK_DIR/packages/database-compat"

    export RTDB_EMULATOR_PORT="$LARK_PORT"
    export RTDB_EMULATOR_NAMESPACE="$LARK_NS"

    log ""
    log "=== Firebase JS SDK Compatibility Tests ==="
    log "  Stack:     http://localhost:$LARK_PORT"
    log "  Namespace: $LARK_NS"
    log ""

    if [ -n "${1:-}" ]; then
        local test_file="test/$1.test.ts"
        if [ ! -f "$test_file" ]; then
            err "Test file not found: $SDK_DIR/packages/database-compat/$test_file"
            err "Available tests:"
            ls test/*.test.ts | sed 's|^test/|  |; s|\.test\.ts$||'
            exit 1
        fi

        # --exit forces mocha to terminate after tests run. The Firebase
        # SDK opens a persistent WebSocket and never closes it, so without
        # --exit node's event loop stays alive and the process hangs.
        if [ -n "${2:-}" ]; then
            log "Running $test_file matching '$2'..."
            TS_NODE_FILES=true TS_NODE_CACHE=NO \
            TS_NODE_COMPILER_OPTIONS='{"module":"commonjs"}' \
                npx mocha "$test_file" \
                    --file src/index.node.ts \
                    --config ../../config/mocharc.node.js \
                    --timeout 10000 \
                    --exit \
                    --grep "$2"
        else
            log "Running $test_file..."
            TS_NODE_FILES=true TS_NODE_CACHE=NO \
            TS_NODE_COMPILER_OPTIONS='{"module":"commonjs"}' \
                npx mocha "$test_file" \
                    --file src/index.node.ts \
                    --config ../../config/mocharc.node.js \
                    --timeout 10000 \
                    --exit
        fi
    else
        log "Running the full database-compat suite (yarn test:node)..."
        yarn test:node
    fi
}

main() {
    check_prereqs
    ensure_sdk_cloned
    ensure_sdk_built
    # Trap set before bringing the stack up so a partial failure (e.g.
    # build OK but the wait-for-ready times out) still tears down.
    trap cleanup EXIT
    ensure_stack_up
    run_tests "${1:-}" "${2:-}"
    log "All tests completed."
}

main "$@"
