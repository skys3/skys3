#!/usr/bin/env bash
# Namespace import and read-through fill (design sections 9.1 and 9.2): a
# write_back bucket attached to a MinIO bucket that already holds 1,000
# keys imports them all; LIST and HEAD answer from the index, with the
# user metadata loaded on the first HEAD; GETs fill objects from MinIO,
# a large one across several extents; and an object overwritten in MinIO
# out of band before its first read is adopted, so the read returns the
# new bytes.
set -euo pipefail
. "$E2E_HERE/lib.sh"

bucket=imported
remote=e2e-import
work=$E2E_DIR/work/import
seed=$work/seed
mkdir -p "$seed/logs/2026" "$seed/data"

# 1,000 small objects under a few prefixes, and three more.
for i in $(seq -w 0 999); do
    case $((10#$i % 3)) in
    0) printf 'object %s\n' "$i" >"$seed/logs/2026/$i.txt" ;;
    1) printf 'object %s\n' "$i" >"$seed/data/$i.bin" ;;
    2) printf 'object %s\n' "$i" >"$seed/$i" ;;
    esac
done
random_file "$work/big" $((9 * 1024 * 1024 + 4321))
random_file "$work/changed" 100000
random_file "$work/changed.new" 120000
minio s3api create-bucket --bucket "$remote" >/dev/null
minio s3 cp --only-show-errors --recursive "$seed" "s3://$remote/"
minio s3 cp --only-show-errors "$work/big" "s3://$remote/big"
minio s3api put-object --bucket "$remote" --key changed --body "$work/changed" >/dev/null
minio s3api put-object --bucket "$remote" --key with-metadata --body "$work/changed" \
    --content-type application/x-e2e --metadata color=blue >/dev/null
count=$(minio s3api list-objects-v2 --bucket "$remote" --query 'length(Contents)')
[ "$count" = 1003 ] || fail "MinIO holds $count objects, not 1003"
log "seeded $remote with $count objects"

create_bucket "$N1_S3" "$N1_KEY" "$N1_SECRET" "$bucket" "$MINIO_URL/$remote"
imported() {
    admin "$N1_ADMIN" "/v1/buckets/$bucket" | jq -e '.flush.import.state == "done"'
}
eventually 120 "the import of $bucket is done" imported
status=$(admin "$N1_ADMIN" "/v1/buckets/$bucket")
[ "$(jq .objects <<<"$status")" = 1003 ] || fail "the index holds $(jq .objects <<<"$status") objects"
log "imported: $(jq -c .flush.import <<<"$status")"

# LIST from the index, page by page through the CLI's paginator.
listed=$(n1 s3api list-objects-v2 --bucket "$bucket" --page-size 250 --query 'length(Contents)')
[ "$listed" = 1003 ] || fail "LIST returned $listed keys"
under=$(n1 s3api list-objects-v2 --bucket "$bucket" --prefix logs/ --delimiter / \
    --query 'CommonPrefixes[].Prefix' --output text)
[ "$under" = logs/2026/ ] || fail "common prefixes under logs/: $under"

# HEAD loads the user metadata and content type from MinIO.
head=$(n1 s3api head-object --bucket "$bucket" --key with-metadata)
[ "$(jq -r .Metadata.color <<<"$head")" = blue ] || fail "HEAD with-metadata: $head"
[ "$(jq -r .ContentType <<<"$head")" = application/x-e2e ] || fail "HEAD with-metadata: $head"
log "LIST and HEAD answer from the index"

# Fills.
n1 s3api get-object --bucket "$bucket" --key logs/2026/000.txt "$work/out" >/dev/null
[ "$(cat "$work/out")" = "object 000" ] || fail "logs/2026/000.txt: $(cat "$work/out")"
n1 s3api get-object --bucket "$bucket" --key data/001.bin "$work/out" >/dev/null
[ "$(cat "$work/out")" = "object 001" ] || fail "data/001.bin: $(cat "$work/out")"
n1 s3api get-object --bucket "$bucket" --key big "$work/big.out" >/dev/null
same "$work/big" "$work/big.out"
n1 s3api get-object --bucket "$bucket" --key big "$work/big.again" >/dev/null
same "$work/big" "$work/big.again"
log "GETs fill objects from MinIO"

# An object changed in MinIO before its first read is adopted.
minio s3api put-object --bucket "$remote" --key changed --body "$work/changed.new" >/dev/null
n1 s3api get-object --bucket "$bucket" --key changed "$work/changed.out" >/dev/null
same "$work/changed.new" "$work/changed.out"
size=$(n1 s3api head-object --bucket "$bucket" --key changed --query ContentLength)
[ "$size" = 120000 ] || fail "HEAD changed after the adoption: $size bytes"
log "an out-of-band overwrite is adopted on the fill"

# The import changed nothing in MinIO.
count=$(minio s3api list-objects-v2 --bucket "$remote" --query 'length(Contents)')
[ "$count" = 1003 ] || fail "MinIO holds $count objects after the import"
