#!/usr/bin/env bash
# Object operations through the AWS CLI on a local bucket of the
# single-node cluster: PUT, GET, and HEAD; ListObjectsV2 page by page with
# continuation tokens; a multipart upload of 8 MiB parts; CopyObject;
# tagging; and DeleteObjects.
set -euo pipefail
. "$E2E_HERE/lib.sh"

bucket=objects
work=$E2E_DIR/work/objects
mkdir -p "$work"

n1 s3api create-bucket --bucket "$bucket" >/dev/null
log "created bucket $bucket"

# PUT, GET, and HEAD.
random_file "$work/small" 70000
n1 s3api put-object --bucket "$bucket" --key small --body "$work/small" \
    --content-type text/x-e2e --metadata origin=e2e >/dev/null
n1 s3api get-object --bucket "$bucket" --key small "$work/small.out" >/dev/null
same "$work/small" "$work/small.out"
head=$(n1 s3api head-object --bucket "$bucket" --key small)
[ "$(jq -r .ContentLength <<<"$head")" = 70000 ] || fail "HEAD length: $head"
[ "$(jq -r .ContentType <<<"$head")" = text/x-e2e ] || fail "HEAD content type: $head"
[ "$(jq -r .Metadata.origin <<<"$head")" = e2e ] || fail "HEAD metadata: $head"
etag=$(jq -r .ETag <<<"$head")
[ "$etag" = "\"$(md5sum <"$work/small" | cut -d' ' -f1)\"" ] || fail "ETag $etag is not the MD5"
if n1 s3api head-object --bucket "$bucket" --key missing >/dev/null 2>&1; then
    fail "HEAD of a missing key succeeded"
fi
log "PUT, GET, and HEAD"

# ListObjectsV2 with continuation: 25 keys in pages of 7.
mkdir -p "$work/list"
for i in $(seq -w 1 25); do
    echo "$i" >"$work/list/$i"
done
n1 s3 cp --only-show-errors --recursive "$work/list" "s3://$bucket/list/"
token=
pages=0
: >"$work/listed"
while :; do
    args=(--bucket "$bucket" --prefix list/ --max-keys 7)
    [ -n "$token" ] && args+=(--continuation-token "$token")
    page=$(n1 s3api list-objects-v2 "${args[@]}" --no-paginate)
    jq -r '.Contents[].Key' <<<"$page" >>"$work/listed"
    pages=$((pages + 1))
    [ "$(jq -r .IsTruncated <<<"$page")" = true ] || break
    token=$(jq -r .NextContinuationToken <<<"$page")
    [ "$pages" -lt 10 ] || fail "the listing does not end"
done
[ "$pages" = 4 ] || fail "25 keys in pages of 7 took $pages pages"
seq -w 1 25 | sed 's|^|list/|' | cmp -s - "$work/listed" || fail "listed $(cat "$work/listed")"
log "ListObjectsV2: 25 keys in $pages pages"

# A multipart upload of 8 MiB parts, by the CLI's transfer manager.
random_file "$work/large" $((20 * 1024 * 1024 + 12345))
n1 s3 cp --only-show-errors "$work/large" "s3://$bucket/large"
etag=$(n1 s3api head-object --bucket "$bucket" --key large --query ETag --output text)
case "$etag" in *-3\") ;; *) fail "multipart ETag $etag is not of three parts" ;; esac
n1 s3 cp --only-show-errors "s3://$bucket/large" "$work/large.out"
same "$work/large" "$work/large.out"
log "multipart upload of 8 MiB parts ($etag)"

# CopyObject, within the bucket.
n1 s3api copy-object --bucket "$bucket" --key large-copy --copy-source "$bucket/large" >/dev/null
n1 s3api get-object --bucket "$bucket" --key large-copy "$work/copy.out" >/dev/null
same "$work/large" "$work/copy.out"
log "CopyObject"

# Tagging.
n1 s3api put-object-tagging --bucket "$bucket" --key small \
    --tagging 'TagSet=[{Key=team,Value=storage},{Key=tier,Value=hot}]'
tags=$(n1 s3api get-object-tagging --bucket "$bucket" --key small --query 'TagSet' --output json)
[ "$(jq -c 'sort_by(.Key)' <<<"$tags")" = \
    '[{"Key":"team","Value":"storage"},{"Key":"tier","Value":"hot"}]' ] || fail "tags $tags"
n1 s3api delete-object-tagging --bucket "$bucket" --key small
tags=$(n1 s3api get-object-tagging --bucket "$bucket" --key small --query 'length(TagSet)')
[ "$tags" = 0 ] || fail "$tags tags left after DeleteObjectTagging"
log "tagging"

# DeleteObjects: every listed key, and one that does not exist.
delete=$(jq -cn --rawfile keys "$work/listed" \
    '{Objects: ([$keys | splits("\n") | select(. != "") | {Key: .}] + [{Key: "list/none"}]),
      Quiet: false}')
deleted=$(n1 s3api delete-objects --bucket "$bucket" --delete "$delete")
[ "$(jq '.Deleted | length' <<<"$deleted")" = 26 ] || fail "DeleteObjects: $deleted"
[ "$(jq '.Errors // [] | length' <<<"$deleted")" = 0 ] || fail "DeleteObjects: $deleted"
# shellcheck disable=SC2016 # a JMESPath literal, not a command
left=$(n1 s3api list-objects-v2 --bucket "$bucket" --prefix list/ --query 'length(Contents || `[]`)')
[ "$left" = 0 ] || fail "$left keys left after DeleteObjects"
log "DeleteObjects"
