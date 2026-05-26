#!/bin/bash
# Run the chaos monkey with a fixed seed and short cycles for repro/debugging.
#
# Defaults aim to surface violations fast: small kill intervals, a few cycles,
# fixed seed so reruns trace the same path through the operation generator.
#
# Usage:
#   ./tools/chaos-monkey/run-seed.sh                # SEED=12345, DURATION=3m
#   SEED=99 ./tools/chaos-monkey/run-seed.sh        # different seed
#   DURATION=10m ./tools/chaos-monkey/run-seed.sh   # longer run
#   SKIP_BUILD=1 ./tools/chaos-monkey/run-seed.sh   # skip rebuild
#
# Any extra args are forwarded to lark-chaos-monkey.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

export SEED="${SEED:-12345}"
export DURATION="${DURATION:-3m}"

exec "$SCRIPT_DIR/run.sh" \
    --min-kill-interval 15 \
    --max-kill-interval 25 \
    "$@"
