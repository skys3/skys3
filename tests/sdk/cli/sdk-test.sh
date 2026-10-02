#!/bin/sh
# The SDK matrix's CLI client: the AWS CLI v2 against SkyS3 (plan M1-25).
#
# It runs against a matrix started by `cargo test -p skys3 --test sdk`,
# which sets the environment tests/sdk/run.sh describes, and exits
# non-zero on the first failed check. It needs aws, curl, grep, sed, and
# coreutils.
set -eu

bucket=$SKYS3_BUCKET
work=$SKYS3_WORK_DIR
part=$((5 * 1024 * 1024))

log() { echo "cli: $*"; }
fail() {
    echo "cli: FAILED: $*" >&2
    exit 1
}
same() { [ "$(cksum <"$1")" = "$(cksum <"$2")" ] || fail "$1 and $2 differ"; }

# The CLI's caches go under the client's scratch directory.
export HOME="$work/home"
mkdir -p "$HOME"

# Path-style requests (design section 11), and parts of 5 MiB.
export AWS_CONFIG_FILE="$work/config"
cat >"$AWS_CONFIG_FILE" <<EOF
[default]
s3 =
    addressing_style = path
    multipart_threshold = 5MB
    multipart_chunksize = 5MB
EOF

# The static credential, through the environment, which the default chain
# reads before the web identity.
static() {
    AWS_ACCESS_KEY_ID=$SKYS3_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$SKYS3_SECRET_ACCESS_KEY "$@"
}

aws --version
head -c 70000 /dev/urandom >"$work/default"
static aws s3api create-bucket --bucket "$bucket" >/dev/null

# The default checksum (CRC64NVME in the CLI, which bundles the Common
# Runtime), sent in an aws-chunked trailer over HTTPS.
static aws s3api put-object --bucket "$bucket" --key default --body "$work/default" \
    --debug >"$work/put.json" 2>"$work/put.debug"
grep -q 'STREAMING-UNSIGNED-PAYLOAD-TRAILER' "$work/put.debug" || fail "no aws-chunked upload"
default=$(sed -n 's/^ *"Checksum\(CRC[0-9A-Z]*\|SHA[0-9]*\)": .*/\1/p' "$work/put.json")
[ -n "$default" ] || fail "no default checksum: $(cat "$work/put.json")"
sent=$(static aws s3api head-object --bucket "$bucket" --key default --checksum-mode ENABLED \
    --query "Checksum$default" --output text)
grep -q "\"Checksum$default\": \"$sent\"" "$work/put.json" || fail "stored checksum $sent"
static aws s3api get-object --bucket "$bucket" --key default --checksum-mode ENABLED \
    "$work/default.out" >/dev/null
same "$work/default" "$work/default.out"
log "default checksum ($default) and aws-chunked upload"

for algorithm in CRC32 CRC32C CRC64NVME SHA1 SHA256; do
    head -c 5000 /dev/urandom >"$work/$algorithm"
    sent=$(static aws s3api put-object --bucket "$bucket" --key "checksum/$algorithm" \
        --body "$work/$algorithm" --checksum-algorithm "$algorithm" \
        --query "Checksum$algorithm" --output text)
    [ -n "$sent" ] && [ "$sent" != None ] || fail "$algorithm: no checksum sent"
    stored=$(static aws s3api get-object --bucket "$bucket" --key "checksum/$algorithm" \
        --checksum-mode ENABLED "$work/$algorithm.out" \
        --query "[Checksum$algorithm, ChecksumType]" --output text)
    [ "$stored" = "$(printf '%s\tFULL_OBJECT' "$sent")" ] || fail "$algorithm: stored $stored, sent $sent"
    same "$work/$algorithm" "$work/$algorithm.out"
done
log "checksums CRC32, CRC32C, CRC64NVME, SHA1, SHA256"

# `aws s3 cp` splits a large upload into parts.
head -c $((2 * part + 1000000)) /dev/urandom >"$work/large"
static aws s3 cp --only-show-errors "$work/large" "s3://$bucket/multipart"
etag=$(static aws s3api head-object --bucket "$bucket" --key multipart --query ETag --output text)
case "$etag" in *-3\") ;; *) fail "multipart ETag $etag" ;; esac
static aws s3 cp --only-show-errors "s3://$bucket/multipart" "$work/large.out"
same "$work/large" "$work/large.out"
log "multipart upload ($etag)"

# A presigned GET, used with curl. The CLI presigns only GETs.
url=$(static aws s3 presign "s3://$bucket/default" --expires-in 300)
curl -fsS --cacert "$SKYS3_CA_FILE" -o "$work/presigned.out" "$url" || fail "presigned GET"
same "$work/default" "$work/presigned.out"
log "presigned GET"

listed=$(static aws s3api list-objects-v2 --bucket "$bucket" --prefix checksum/ --delimiter / \
    --query 'length(Contents)' --output text)
[ "$listed" = 5 ] || fail "listed $listed objects"
static aws s3 cp --only-show-errors "s3://$bucket/default" "s3://$bucket/copy"
static aws s3api put-object-tagging --bucket "$bucket" --key copy \
    --tagging 'TagSet=[{Key=sdk,Value=cli}]'
tag=$(static aws s3api get-object-tagging --bucket "$bucket" --key copy \
    --query 'TagSet[0].Value' --output text)
[ "$tag" = cli ] || fail "tag $tag"
static aws s3api get-object --bucket "$bucket" --key copy --range bytes=10-19 "$work/range.out" >/dev/null
[ "$(cksum <"$work/range.out")" = "$(head -c 20 "$work/default" | tail -c 10 | cksum)" ] ||
    fail "the range returned other bytes"
static aws s3 rm --only-show-errors --recursive "s3://$bucket/"
static aws s3 rb "s3://$bucket" >/dev/null
log "listing, copy, tagging, ranges, and batch delete"

# The default chain with the web identity alone. Every command is a new
# process that assumes the role, so the load below refreshes constantly.
wi="$bucket-web-identity"
aws s3api create-bucket --bucket "$wi" >/dev/null
if aws s3api create-bucket --bucket "other-$bucket" 2>"$work/denied"; then
    fail "the role may create other buckets"
fi
grep -q AccessDenied "$work/denied" || fail "other bucket: $(cat "$work/denied")"

key_id() {
    aws configure export-credentials --format env-no-export | sed -n 's/^AWS_ACCESS_KEY_ID=//p'
}
first=$(key_id)
case "$first" in ASIA*) ;; *) fail "access key $first" ;; esac
deadline=$(($(date +%s) + SKYS3_LOAD_SECONDS))
# Each worker has its own home: the CLI caches sessions in files under
# ~/.aws/cli/cache, and concurrent processes sharing them fail with a
# KeyError when one replaces an entry another is reading.
worker() {
    n=$1
    i=0
    export HOME="$work/home-$n"
    mkdir -p "$HOME"
    head -c $((1000 + n * 100)) /dev/urandom >"$work/load-$n"
    while [ "$(date +%s)" -lt "$deadline" ]; do
        aws s3api put-object --bucket "$wi" --key "load/$n/$((i % 4))" --body "$work/load-$n" >/dev/null
        aws s3api get-object --bucket "$wi" --key "load/$n/$((i % 4))" "$work/load-$n.out" >/dev/null
        same "$work/load-$n" "$work/load-$n.out"
        key_id >>"$work/keys-$n"
        i=$((i + 1))
    done
    echo "$i" >"$work/count-$n"
}
pids=
for n in 0 1 2 3 4 5 6 7; do
    worker "$n" &
    pids="$pids $!"
done
for pid in $pids; do
    wait "$pid" || fail "a worker failed"
done
sessions=$( (echo "$first"; cat "$work"/keys-*) | sort -u | wc -l)
[ "$sessions" -ge 2 ] || fail "the session was never refreshed"
count=0
for file in "$work"/count-*; do
    count=$((count + 2 * $(cat "$file")))
done
log "web identity: $count requests under load with $sessions sessions"
log passed
