#!/usr/bin/env bash
# Write-back flush to MinIO (design section 7): objects written to a
# write_back bucket reach the target with the bytes, the user metadata,
# and the write identity (`x-amz-meta-skys3-wid`, section 7.2) of the
# bucket, a multipart object keeps its multipart ETag (section 7.4), and a
# delete reaches the target too.
set -euo pipefail
. "$E2E_HERE/lib.sh"

bucket=flushed
remote=e2e-flush
work=$E2E_DIR/work/flush
mkdir -p "$work"

minio s3api create-bucket --bucket "$remote" >/dev/null
create_bucket "$N1_S3" "$N1_KEY" "$N1_SECRET" "$bucket" "$MINIO_URL/$remote"
bucket_id=$(admin "$N1_ADMIN" "/v1/buckets/$bucket" | jq -r .bucket_id)
log "created write_back bucket $bucket ($bucket_id) on $MINIO_URL/$remote"

random_file "$work/a" 1000
random_file "$work/b" 300000
random_file "$work/large" $((17 * 1024 * 1024))
n1 s3api put-object --bucket "$bucket" --key a --body "$work/a" --metadata owner=e2e >/dev/null
n1 s3api put-object --bucket "$bucket" --key dir/b --body "$work/b" >/dev/null
n1 s3 cp --only-show-errors "$work/large" "s3://$bucket/large"
n1 s3api put-object --bucket "$bucket" --key doomed --body "$work/a" >/dev/null
local_etag=$(n1 s3api head-object --bucket "$bucket" --key large --query ETag --output text)

eventually 60 "bucket $bucket is flushed" flushed "$N1_ADMIN" "$bucket"
log "every change is at the target"

for key in a dir/b large; do
    minio s3api get-object --bucket "$remote" --key "$key" "$work/remote" >/dev/null
    same "$work/${key#dir/}" "$work/remote"
    head=$(minio s3api head-object --bucket "$remote" --key "$key")
    wid=$(jq -r '.Metadata["skys3-wid"] // empty' <<<"$head")
    [ -n "$wid" ] || fail "$remote/$key has no write identity: $head"
    case "$wid" in *"$bucket_id"*) ;; *) fail "$remote/$key's identity $wid names another bucket" ;; esac
done
[ "$(minio s3api head-object --bucket "$remote" --key a --query Metadata.owner --output text)" = e2e ] ||
    fail "the user metadata of a did not reach the target"
remote_etag=$(minio s3api head-object --bucket "$remote" --key large --query ETag --output text)
[ "$remote_etag" = "$local_etag" ] || fail "large: remote ETag $remote_etag, local $local_etag"
log "the target holds the bytes, metadata, and write identity (ETag $remote_etag)"

n1 s3api delete-object --bucket "$bucket" --key doomed
gone() { ! minio s3api head-object --bucket "$remote" --key "$1" >/dev/null 2>&1; }
eventually 60 "the delete of doomed reaches the target" gone doomed
eventually 30 "bucket $bucket is flushed again" flushed "$N1_ADMIN" "$bucket"
log "the delete reached the target"
