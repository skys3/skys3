#!/usr/bin/env bash
# The admin listener of the single-node cluster (design sections 12 and
# 13): `/v1/health`, `/v1/buckets` against ListBuckets, and `/metrics`,
# after the scenarios before it ran.
set -euo pipefail
. "$E2E_HERE/lib.sh"

health=$(admin "$N1_ADMIN" /v1/health)
jq -e '.ready and .node_id == "n1" and .cluster_id == "e2e" and .control_store.live and
       (.disks | length) >= 1 and all(.disks[]; .in_service)' <<<"$health" >/dev/null ||
    fail "/v1/health: $health"
log "health: $(jq -c '{ready, shards_open, control_store}' <<<"$health")"

# The control bucket's versioning, which the node reads at startup from an
# S3 control store (design section 12): the SkyS3 bucket run.sh uses keeps
# no versions, so it is 0 and the node warns; -1 for any other store.
versioning=$(metric "$N1_ADMIN" skys3_control_store_versioning)
expected=-1
if [ "$E2E_CONTROL" = s3 ]; then
    expected=0
    grep -q "the control bucket's versioning is not enabled" "$E2E_DIR/logs/n1.log" ||
        fail "n1 did not warn that the control bucket keeps no versions"
fi
[ "$versioning" = "$expected" ] ||
    fail "skys3_control_store_versioning is ${versioning:-missing}, not $expected"
log "control store: $E2E_CONTROL, versioning $versioning"

n1 s3api create-bucket --bucket admin-check >/dev/null
buckets=$(admin "$N1_ADMIN" /v1/buckets)
names=$(jq -r '[.buckets[].name] | sort | join(" ")' <<<"$buckets")
listed=$(n1 s3api list-buckets --query 'Buckets[].Name' --output json | jq -r 'sort | join(" ")')
[ "$names" = "$listed" ] || fail "/v1/buckets has $names, ListBuckets $listed"
jq -e '.buckets[] | select(.name == "admin-check") | .mode == "local" and .objects == 0' \
    <<<"$buckets" >/dev/null || fail "/v1/buckets: admin-check is not an empty local bucket"
if admin "$N1_ADMIN" /v1/buckets/no-such-bucket >/dev/null 2>&1; then
    fail "/v1/buckets/no-such-bucket answered"
fi
log "buckets: $names"

curl -sS --fail --max-time 10 "http://$N1_ADMIN/metrics" >"$E2E_DIR/work/metrics.txt"
grep -q '^# TYPE skys3_' "$E2E_DIR/work/metrics.txt" || fail "/metrics has no skys3 metric family"
# Every write_back bucket the scenarios before this one flushed is clean.
for bucket in $(jq -r '.buckets[] | select(.mode == "write_back") | .name' <<<"$buckets"); do
    for series in "skys3_dirty_bytes{bucket=\"$bucket\"}" "skys3_flush_lag_seconds{bucket=\"$bucket\"}"; do
        [ -n "$(metric "$N1_ADMIN" "$series")" ] || fail "/metrics has no $series"
    done
    [ "$(metric "$N1_ADMIN" "skys3_dirty_bytes{bucket=\"$bucket\"}")" = 0 ] ||
        fail "$bucket still has dirty bytes"
done
log "/metrics: $(grep -c '^skys3_' "$E2E_DIR/work/metrics.txt") skys3 series"
