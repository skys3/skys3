#!/usr/bin/env bash
# The reconciliation scan (design section 9.1, plan M7-07): changes made to
# MinIO out of band after a bucket's keys were flushed (a new object, an
# overwrite, and a delete) show up in `GET /v1/buckets/<name>/reconcile`
# as remote_only, etag_mismatch, and index_only differences, and the scan
# changes nothing, neither in the index nor in MinIO. The node's
# configuration scans `reconciled` every 2 seconds.
set -euo pipefail
. "$E2E_HERE/lib.sh"

bucket=reconciled
remote=e2e-reconcile
work=$E2E_DIR/work/reconcile
mkdir -p "$work"

minio s3api create-bucket --bucket "$remote" >/dev/null
create_bucket "$N1_S3" "$N1_KEY" "$N1_SECRET" "$bucket" "$MINIO_URL/$remote"
for key in kept overwritten deleted; do
    printf '%s\n' "$key" >"$work/$key"
    n1 s3api put-object --bucket "$bucket" --key "$key" --body "$work/$key" >/dev/null
done
eventually 60 "bucket $bucket is flushed" flushed "$N1_ADMIN" "$bucket"
clean_scan() {
    admin "$N1_ADMIN" "/v1/buckets/$bucket/reconcile" |
        jq -e '.enabled and .last != null and .last.compared == 3 and
               (.last.differences | length) == 0'
}
eventually 30 "a scan finds no difference" clean_scan
log "a scan of the flushed bucket finds no difference"

# Out of band: a new object, an overwrite, and a delete.
printf 'outside\n' >"$work/outside"
minio s3api put-object --bucket "$remote" --key outside --body "$work/outside" >/dev/null
minio s3api put-object --bucket "$remote" --key overwritten --body "$work/outside" >/dev/null
minio s3api delete-object --bucket "$remote" --key deleted
changed_ms=$(($(date +%s%3N)))

reported() {
    admin "$N1_ADMIN" "/v1/buckets/$bucket/reconcile" >"$work/report.json"
    jq -e --argjson after "$changed_ms" '.last.started_unix_ms > $after and
        ([.last.differences[] | "\(.key) \(.kind)"] | sort) ==
        ["deleted index_only", "outside remote_only", "overwritten etag_mismatch"]' \
        "$work/report.json"
}
eventually 60 "the scan reports the three differences" reported
writer=$(jq -r '.last.differences[] | select(.key == "outside") | .remote.writer' "$work/report.json")
[ "$writer" = outside ] || fail "the new object's writer is $writer"
counts=$(jq -c .last.counts "$work/report.json")
log "reported: $counts"
for kind in remote_only etag_mismatch index_only; do
    value=$(metric "$N1_ADMIN" "skys3_reconcile_differences{bucket=\"$bucket\",kind=\"$kind\"}")
    [ "$value" = 1 ] || fail "skys3_reconcile_differences for $kind is ${value:-missing}"
done

# The scan changed nothing: the index lists and describes what it did (a
# GET could fill, and so adopt, an evicted entry, so this reads metadata
# only), and MinIO keeps the out-of-band state.
keys=$(n1 s3api list-objects-v2 --bucket "$bucket" --query 'Contents[].Key' --output text)
[ "$keys" = "$(printf 'deleted\tkept\toverwritten')" ] || fail "the index now lists $keys"
etag=$(n1 s3api head-object --bucket "$bucket" --key overwritten --query ETag --output text)
[ "$etag" = "\"$(md5sum <"$work/overwritten" | cut -d' ' -f1)\"" ] ||
    fail "the index's overwritten now has ETag $etag"
minio s3api get-object --bucket "$remote" --key overwritten "$work/out" >/dev/null
same "$work/outside" "$work/out"
if minio s3api head-object --bucket "$remote" --key deleted >/dev/null 2>&1; then
    fail "the scan restored the deleted object in MinIO"
fi
minio s3api head-object --bucket "$remote" --key outside >/dev/null
flushed "$N1_ADMIN" "$bucket" || fail "the scan made keys dirty"
log "the scan changed nothing"
