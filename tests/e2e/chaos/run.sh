#!/usr/bin/env bash
# The nightly kill, partition, and peering suite (plan M8-08): a 5-node
# cluster over a 3-member etcd, which a 3-node cluster peers with, under a
# seeded schedule of process kills, iptables partitions, tc netem, the loss
# of a rack, of the coordinator in the middle of a change, and of an etcd
# member, with a checked client workload (chaos.py tells what it checks).
#
#   sudo tests/e2e/chaos/run.sh
#
# It needs root (CI runs it with sudo) for the network namespaces each
# process runs in, iptables, and tc; and bash, curl, openssl, python3, and
# iproute2. The kernel needs network namespaces, veth, and bridges; without
# the netem qdisc (tc netem) the delay faults drop packets with iptables
# instead and add no delay, which the report says. The environment can
# name:
#
#   SKYS3_CHAOS_SEED     the schedule's seed; a random one by default. The
#                        run prints it, and the report names the command
#                        that replays it.
#   SKYS3_CHAOS_MINUTES  how long faults are struck (30); the run then
#                        heals, checks, and waits for the peered keys
#   SKYS3_CHAOS_NETEM    `require` to fail without tc netem (CI), `auto`
#                        (the default) to fall back to iptables loss
#   SKYS3_CHAOS_DIR      where the run's files go (target/chaos-run),
#                        emptied first: the PKI, configurations, data
#                        directories, logs, ops.jsonl (every write and its
#                        answer), report.json, and report.md
#   SKYS3_E2E_BINARY     the skys3 binary; by default `cargo build
#                        --profile e2e -p skys3` builds it
#   SKYS3_E2E_ETCD       the etcd server binary; by default the pinned
#                        release tests/e2e/etcd.sh downloads and caches
#
# It uses the subnet 10.77.0.0/24 on a bridge skc0 and the namespaces
# skc-*, which it removes when it ends, and exits non-zero if the cluster
# lost an acknowledged write, served a stale read, failed to heal, or did
# not deliver the peered keys.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../../.." && pwd)
log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }
fail() {
    printf '%s FAILED: %s\n' "$(date -u +%H:%M:%S)" "$*" >&2
    exit 1
}

[ "$(id -u)" = 0 ] || fail "the suite needs root for namespaces, iptables, and tc; run it with sudo"
for tool in ip iptables curl openssl python3; do
    command -v "$tool" >/dev/null || fail "the suite needs $tool"
done
bash "$here/net.sh" check || fail "this machine cannot make network namespaces, veths, and bridges"

dir=${SKYS3_CHAOS_DIR:-$repo/target/chaos-run}
minutes=${SKYS3_CHAOS_MINUTES:-30}
seed=${SKYS3_CHAOS_SEED:-$(od -An -N4 -tu4 /dev/urandom | tr -d ' ')}

netem=--netem
if ! bash "$here/net.sh" netem; then
    [ "${SKYS3_CHAOS_NETEM:-auto}" = require ] && fail "the kernel has no tc netem (sch_netem)"
    log "the kernel has no tc netem: the delay faults drop packets with iptables instead, without delay"
    netem=--no-netem
fi

if [ -n "${SKYS3_E2E_BINARY:-}" ]; then
    binary=$SKYS3_E2E_BINARY
else
    log "building skys3 (profile e2e)"
    (cd "$repo" && cargo build --quiet --profile e2e -p skys3)
    binary=${CARGO_TARGET_DIR:-$repo/target}/e2e/skys3
fi
binary=$(cd "$(dirname "$binary")" && pwd)/$(basename "$binary")
[ -x "$binary" ] || fail "no skys3 binary at $binary"
etcd=$(bash "$repo/tests/e2e/etcd.sh")

rm -rf "$dir"
mkdir -p "$dir"
PKI_NODES="ka/n1 ka/n2 ka/n3 ka/n4 ka/n5 kb/p1 kb/p2 kb/p3" \
    PKI_ETCD_IPS="10.77.0.21 10.77.0.22 10.77.0.23" bash "$repo/tests/e2e/pki.sh" "$dir/pki"

cleanup() {
    local status=$?
    bash "$here/net.sh" down || true
    exit "$status"
}
trap cleanup EXIT INT TERM
bash "$here/net.sh" down
bash "$here/net.sh" up n1=10.77.0.11 n2=10.77.0.12 n3=10.77.0.13 n4=10.77.0.14 \
    n5=10.77.0.15 e1=10.77.0.21 e2=10.77.0.22 e3=10.77.0.23 p1=10.77.0.31 \
    p2=10.77.0.32 p3=10.77.0.33
log "the network is up; seed $seed, $minutes minutes of faults"

python3 "$here/chaos.py" run --dir "$dir" --binary "$binary" --etcd "$etcd" \
    --seed "$seed" --minutes "$minutes" "$netem"
