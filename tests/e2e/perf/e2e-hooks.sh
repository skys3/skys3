#!/usr/bin/env bash
# The performance harness's kill and start hooks for nodes the end-to-end
# suite runs (tests/e2e/scenarios/98-perf.sh writes them into its target
# file):
#
#   e2e-hooks.sh kill <node>    SIGKILLs the node, as a crash would
#   e2e-hooks.sh start <node>   starts it again with lib.sh's node_start,
#                               and returns once it reports ready
#
# It needs the environment run.sh exports (E2E_HERE, E2E_DIR, E2E_BINARY),
# which the harness passes on to its hooks.
set -euo pipefail
# shellcheck source=tests/e2e/lib.sh
. "$E2E_HERE/lib.sh"

case "${1:-}" in
kill)
    pid=$(cat "$E2E_DIR/$2/pid")
    kill -KILL "$pid"
    rm -f "$E2E_DIR/$2/pid"
    log "killed node $2 (kill -9)"
    ;;
start) node_start "$2" ;;
*) fail "usage: e2e-hooks.sh kill|start <node>" ;;
esac
