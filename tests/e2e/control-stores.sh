#!/usr/bin/env bash
# The end-to-end suite over every control store at once (plan M8-03), as
# CI's e2e job runs it:
#
#   tests/e2e/control-stores.sh
#
# Runs tests/e2e/run.sh three times side by side: every scenario over the
# file control store, and the scenarios of the single-node cluster over
# etcd with mutual TLS and over an S3 control bucket. Each run has ports
# and a directory of its own: SKYS3_E2E_PORT (default 19000) and
# SKYS3_E2E_DIR (default target/e2e-run) for the file store, and the next
# hundred ports and the directory's `-etcd` and `-s3` siblings for the
# others. The single-node runs finish within the peering scenario's five
# minutes, so the three take about as long as the file store's alone.
#
# Each run's output is printed once all three are done, in turn, with its
# own scenario groups. It exits non-zero if any run failed. Every other
# variable run.sh reads is passed to all three; SKYS3_E2E_BINARY should
# be set, or each run builds the binary.
set -uo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
port=${SKYS3_E2E_PORT:-19000}
dir=${SKYS3_E2E_DIR:-$repo/target/e2e-run}
logs=$(mktemp -d)
trap 'rm -rf "$logs"' EXIT

declare -A pids
start() {
    local control=$1 offset=$2 suffix=$3
    SKYS3_E2E_CONTROL=$control SKYS3_E2E_PORT=$((port + offset)) SKYS3_E2E_DIR=$dir$suffix \
        "$here/run.sh" >"$logs/$control.log" 2>&1 &
    pids[$control]=$!
}
start file 0 ""
start etcd 100 -etcd
start s3 200 -s3

status=0
for control in file etcd s3; do
    if wait "${pids[$control]}"; then
        result=passed
    else
        result=failed
        status=1
    fi
    echo "=== the suite over the $control control store: $result"
    cat "$logs/$control.log"
    [ "$result" = passed ] || echo "::error::the suite over the $control control store failed"
done
exit "$status"
