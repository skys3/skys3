#!/usr/bin/env bash
# Replication and routing across three nodes (plan M8-05b, design sections
# 5, 6.3 to 6.6, and 7.1): nodes r1, r2, and r3 of cluster rc, with
# `[cluster] multi_node = true`, share the etcd control store over mutual
# TLS (under the prefix `rc/`) and reach each other over the transport
# with mutual TLS. A write_back bucket with 4 shards of 3 members each
# flushes to a versioned MinIO bucket. The scenario checks:
#
# - that CreateBucket places every shard on all three nodes, with one
#   primary each, and refuses a bucket whose `replicas` exceed the nodes;
# - writes and reads through every node: a writer PUTs new keys, one at a
#   time, through the nodes in turn, and records each answer;
# - that after `kill -9` of shard 0's primary, writes resume within 10 s:
#   no write that fails, or takes longer than a second, ends later than
#   10 s after the kill, and writes go on succeeding after it;
# - that every acknowledged key reads back, through each surviving node,
#   with its bytes, those the new primaries no longer hold filled on them
#   from MinIO whichever node's gateway asks, and is flushed exactly once: once the bucket is
#   flushed, MinIO holds exactly one version of each key the writer
#   wrote, acknowledged or not, with its bytes, and every acknowledged key;
# - that the restarted node rejoins as a learner: no node runs the
#   coordinator in this build (plan M8-06), so the scenario adds it to the
#   learners of every shard that lost it, by a CAS on its register in
#   etcd, as the coordinator's replacement would; the shard's primary
#   backfills it and promotes it to member, and the restarted node serves
#   reads of every acknowledged key.
#
# The nodes run the default replication timing, whose failover the design
# bounds at under 10 seconds. It runs only over etcd (`# e2e: etcd`),
# beside the cluster scenario, takes about a minute and a half, and uses
# the ports E2E_PORT+21 to E2E_PORT+29.
# e2e: etcd
set -euo pipefail
. "$E2E_HERE/lib.sh"

[ -n "${E2E_ETCD_URL:-}" ] || fail "the replication scenario runs with SKYS3_E2E_CONTROL=etcd"
work=$E2E_DIR/work/replication
pki=$E2E_DIR/pki
mkdir -p "$work"
key=AKIASKYS3E2EREPLICA secret=skys3-e2e-replica-secret-0123456789
bucket=replicated
remote=e2e-replicated

# etcd's JSON gateway, with the suite's client certificate.
etcd_call() {
    curl -sS --fail-with-body --max-time 10 --cacert "$pki/e2e-ca.pem" \
        --cert "$pki/etcd-client.pem" --key "$pki/etcd-client.key" \
        -X POST "$E2E_ETCD_URL$1" -d "$2"
}
b64() { printf '%s' "$1" | base64 -w0; }

# configure <node> <port offset>: the directory $E2E_DIR/<node> and the
# configuration of node <node>, its gateway at E2E_PORT+<offset>, its
# admin listener one port up and its transport two.
configure() {
    local node=$1 offset=$2 dir=$E2E_DIR/$1
    mkdir -p "$dir"
    printf '%s' "$secret" >"$dir/secret"
    echo "127.0.0.1:$((E2E_PORT + offset + 1))" >"$dir/admin"
    echo "http://127.0.0.1:$((E2E_PORT + offset))" >"$dir/s3"
    cat >"$dir/skys3.toml" <<EOF
[cluster]
cluster_id = "rc"
multi_node = true

[node]
node_id = "$node"
data_dir = "$dir/data"
zone = "z1"
rack = "r-$node"

[gateway]
listen = "127.0.0.1:$((E2E_PORT + offset))"

[control_store]
backend = "etcd"
etcd_endpoints = ["$E2E_ETCD_URL"]
etcd_tls_ca_file = "$pki/e2e-ca.pem"
etcd_tls_cert_file = "$pki/etcd-client.pem"
etcd_tls_key_file = "$pki/etcd-client.key"
config_poll_interval_seconds = 1

[transport]
listen = "127.0.0.1:$((E2E_PORT + offset + 2))"
tls_cert_file = "$pki/rc-$node.pem"
tls_key_file = "$pki/rc-$node.key"
tls_ca_file = "$pki/rc-ca.pem"

[storage]
disk_min_free_bytes = 0

[cache]
# Keep clean payload whatever the free space of the suite's disk, as n1
# does: below the reserve, a primary evicts each version it fills for
# another node's gateway before that gateway reads it.
reserve_fraction = 0.0

[buckets.defaults]
shards_per_bucket = 4
replicas = 3
min_write_replicas = 2

[buckets.wide]
mode = "local"
replicas = 4

[identity]
sts_web_identity = false

[identity.static_credentials.admin]
access_key_id = "$key"
secret_access_key_file = "$dir/secret"
policy = '{"Version": "2012-10-17", "Statement": {"Effect": "Allow", "Action": "*", "Resource": "*"}}'

[admin]
listen = "127.0.0.1:$((E2E_PORT + offset + 1))"

[logging]
filter = "info"
EOF
    {
        echo "AWS_ACCESS_KEY_ID=$MINIO_KEY"
        echo "AWS_SECRET_ACCESS_KEY=$MINIO_SECRET"
        echo "AWS_EC2_METADATA_DISABLED=true"
        echo "AWS_CONFIG_FILE=$E2E_DIR/aws/no-config"
        echo "AWS_SHARED_CREDENTIALS_FILE=$E2E_DIR/aws/no-credentials"
    } >"$dir/env"
}

s3_of() { cat "$E2E_DIR/$1/s3"; }
admin_of() { cat "$E2E_DIR/$1/admin"; }
# shards <node>: the node's GET /v1/shards entries of the bucket.
shards() { admin "$(admin_of "$1")" /v1/shards | jq -c --arg b "$bucket" '[.shards[] | select(.bucket == $b)]'; }
body_of() { printf 'skys3 e2e replication %s' "$1"; }

# put <endpoint> <key>: PUTs the key's body, and prints the HTTP status
# (000 when the request got no answer).
put() {
    curl -sS -o /dev/null -w '%{http_code}' --max-time 30 \
        --aws-sigv4 "aws:amz:us-east-1:s3" --user "$key:$secret" \
        -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" --data-binary "$(body_of "$2")" \
        -X PUT "$1/$bucket/$2" 2>/dev/null || true
}
# get <endpoint> <key>: the object's body, or a failure.
get() {
    curl -sS --fail-with-body --max-time 30 --aws-sigv4 "aws:amz:us-east-1:s3" \
        --user "$key:$secret" -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" "$1/$bucket/$2"
}

# --- Placement --------------------------------------------------------------

nodes=(r1 r2 r3)
offsets=([1]=21 [2]=24 [3]=27)
for i in 1 2 3; do
    configure "r$i" "${offsets[$i]}"
    node_start "r$i"
done
minio s3api create-bucket --bucket "$remote" >/dev/null
minio s3api put-bucket-versioning --bucket "$remote" \
    --versioning-configuration Status=Enabled >/dev/null
create_bucket "$(s3_of r2)" "$key" "$secret" "$bucket" "$MINIO_URL/$remote"
status=$(curl -sS -o "$work/wide.xml" -w '%{http_code}' --max-time 30 \
    --aws-sigv4 "aws:amz:us-east-1:s3" --user "$key:$secret" \
    -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" -X PUT "$(s3_of r1)/wide")
[ "$status" = 400 ] && grep -q InvalidRequest "$work/wide.xml" ||
    fail "a bucket of 4 replicas on 3 nodes answered $status: $(cat "$work/wide.xml")"
log "a bucket of 4 replicas on 3 nodes is refused: $(grep -o '<Message>[^<]*' "$work/wide.xml" | cut -c10-)"

# placed <node>: whether the node holds a replica of every shard, each of
# 3 members with the same primary everywhere.
placed() {
    shards "$1" | jq -e 'length == 4 and all(.[]; .source == "replica" and
        (.members | length) == 3 and .primary != null and .epoch == 1)' >/dev/null
}
for node in "${nodes[@]}"; do
    eventually 30 "$node holds a replica of every shard" placed "$node"
done
bucket_id=$(shards r1 | jq -r '.[0].bucket_id')
victim=$(shards r1 | jq -r '.[] | select(.shard == 0) | .primary')
log "bucket $bucket ($bucket_id): primaries $(shards r1 | jq -c '[.[] | {(.shard | tostring): .primary}] | add'); shard 0's is $victim"

# --- Writes through every node, and the kill --------------------------------
#
# The writer PUTs keys k00000, k00001, ... through the live nodes in turn,
# and records `<key> <status> <start> <end> <node>` for each, the times in
# seconds since the epoch. It stops when the file `stop` appears.
#
# use_nodes <nodes...>: makes the nodes the ones the writer uses. The list
# is written aside and renamed into place: the writer reads the file at any
# time, and found empty, as `>` leaves it for a moment, it would stop
# (`read` fails at once, and the writer runs under `set -e`).
use_nodes() {
    echo "$*" >"$work/live.next"
    mv -f "$work/live.next" "$work/live"
}
use_nodes "${nodes[@]}"
writer() {
    local n=0 name status start node
    while [ ! -e "$work/stop" ]; do
        read -r -a live <"$work/live"
        node=${live[$((n % ${#live[@]}))]}
        name=$(printf 'k%05d' "$n")
        start=$EPOCHREALTIME
        status=$(put "$(s3_of "$node")" "$name")
        echo "$name $status $start $EPOCHREALTIME $node" >>"$work/writes"
        n=$((n + 1))
    done
}
: >"$work/writes"
writer &
writer_pid=$!
acked() { awk '$2 == 200' "$work/writes" | wc -l; }
some() { [ "$(acked)" -ge "$1" ]; }
eventually 60 "the writer has 150 acknowledged writes" some 150
log "before the kill: $(acked) writes acknowledged, through $(awk '$2 == 200 { print $5 }' "$work/writes" | sort -u | paste -sd, -)"

survivors=()
for node in "${nodes[@]}"; do
    [ "$node" = "$victim" ] || survivors+=("$node")
done
use_nodes "${survivors[@]}"
killed_at=$EPOCHREALTIME
kill -KILL "$(cat "$E2E_DIR/$victim/pid")"
wait "$(cat "$E2E_DIR/$victim/pid")" 2>/dev/null || true
rm -f "$E2E_DIR/$victim/pid"
log "killed $victim (kill -9)"

# Writes go on through the survivors for 20 s after the kill.
after_kill() { awk -v t="$killed_at" '$3 > t + 20' "$work/writes" | grep -q .; }
eventually 60 "the writer has run 20 s past the kill" after_kill
touch "$work/stop"
wait "$writer_pid"

# The outage: the last write that failed or took over a second, among those
# that ended after the kill.
read -r failed slow last <<<"$(awk -v t="$killed_at" '
    $4 > t && ($2 != 200 || $4 - $3 > 1) {
        if ($2 != 200) failed++; else slow++
        if ($4 > last) last = $4
    }
    END { printf "%d %d %.3f\n", failed, slow, last ? last - t : 0 }' "$work/writes")"
later=$(awk -v t="$killed_at" '$3 > t + 10 && $2 == 200' "$work/writes" | wc -l)
total=$(wc -l <"$work/writes")
awk -v t="$killed_at" -v l="$last" 'BEGIN { exit !(l < 10) }' ||
    fail "writes resumed $last s after the kill: $(awk -v t="$killed_at" '$4 > t && ($2 != 200 || $4 - $3 > 1)' "$work/writes" | tail -n 5)"
[ "$later" -ge 20 ] || fail "only $later writes succeeded from 10 s after the kill"
log "writes resumed $last s after the kill ($failed failed, $slow slow); $later acknowledged from 10 s after it; $(acked) of $total acknowledged in all"

# The takeover: shard 0 at epoch 2 led by a survivor, without the victim.
taken_over() {
    shards "${survivors[0]}" | jq -e --arg v "$victim" '.[] | select(.shard == 0) |
        .epoch >= 2 and .primary != $v and (.members | index($v) | not)' >/dev/null
}
eventually 30 "a survivor leads shard 0 without $victim" taken_over
log "after the takeover: $(shards "${survivors[0]}" | jq -c '[.[] | {shard, epoch, primary, members}]')"

# --- Every acknowledged key, read and flushed ---------------------------------

# read_back <node>: every acknowledged key through the node, with its bytes.
read_back() {
    local endpoint name
    endpoint=$(s3_of "$1")
    while read -r name _; do
        [ "$(get "$endpoint" "$name")" = "$(body_of "$name")" ] ||
            fail "$name, acknowledged, does not read back through $1"
    done < <(awk '$2 == 200' "$work/writes")
}
for node in "${survivors[@]}"; do
    read_back "$node"
done
# The new primaries' copies of what was flushed before the kill were
# evicted, as only the primary keeps clean copies (clean_copies = 1), so
# those reads were filled on the primary, for either node's gateway.
fills=0
for node in "${survivors[@]}"; do
    value=$(metric "$(admin_of "$node")" "skys3_fills_total{bucket=\"$bucket\"}")
    fills=$((fills + ${value:-0}))
done
[ "$fills" -gt 0 ] || fail "no read was filled on a new primary"
log "every acknowledged key reads back through ${survivors[*]}, $fills of them filled on their primary"

for node in "${survivors[@]}"; do
    eventually 120 "bucket $bucket is flushed on $node" flushed "$(admin_of "$node")" "$bucket"
done
minio s3api list-object-versions --bucket "$remote" --output json >"$work/versions.json"
summary=$(python3 - "$work/writes" "$work/versions.json" <<'EOF'
import hashlib, json, sys
writes = [line.split() for line in open(sys.argv[1])]
listing = json.load(open(sys.argv[2]))
if listing.get("DeleteMarkers"):
    sys.exit(f"delete markers: {listing['DeleteMarkers'][:3]}")
versions = {}
for version in listing.get("Versions", []):
    versions.setdefault(version["Key"], []).append(version)
written = {w[0] for w in writes}
for name, held in versions.items():
    if name not in written:
        sys.exit(f"{name} was never written")
    if len(held) != 1:
        sys.exit(f"{name} was flushed {len(held)} times")
    body = f"skys3 e2e replication {name}".encode()
    if held[0]["ETag"].strip('"') != hashlib.md5(body).hexdigest():
        sys.exit(f"{name} holds other bytes: {held[0]['ETag']}")
missing = [w[0] for w in writes if w[1] == "200" and w[0] not in versions]
if missing:
    sys.exit(f"acknowledged but not flushed: {missing[:5]} ({len(missing)})")
unacked = sum(1 for w in writes if w[1] != "200" and w[0] in versions)
print(f"{len(versions)} keys, one version each, {unacked} of them unacknowledged")
EOF
) || fail "the target does not hold each write exactly once: $summary"
log "the target holds every acknowledged key exactly once: $summary"

# --- The restarted node rejoins as a learner ----------------------------------

node_start "$victim"
# Restarted, it holds no replica of the shards whose registers left it out.
left_out=$(shards "${survivors[0]}" | jq -r --arg v "$victim" '.[] | select(.members | index($v) | not) | .shard')
[ -n "$left_out" ] || fail "every shard still names $victim"
for shard in $left_out; do
    shards "$victim" | jq -e --argjson s "$shard" '.[] | select(.shard == $s) | .source != "replica"' >/dev/null ||
        fail "$victim, restarted, serves shard $shard, which its register left it out of"
done
# The scenario plays the coordinator: a CAS on each register adds the node
# as a learner, at the next epoch.
for shard in $left_out; do
    register=rc/shards/$bucket_id/$shard.json
    range=$(etcd_call /v3/kv/range "{\"key\": \"$(b64 "$register")\"}")
    revision=$(jq -r '.kvs[0].mod_revision' <<<"$range")
    config=$(jq -r '.kvs[0].value | @base64d' <<<"$range" |
        jq -c --arg v "$victim" --arg p "e2e-learner-$shard" \
            '.epoch += 1 | .learners += [$v] | .proposal_id = $p')
    answer=$(etcd_call /v3/kv/txn "{
        \"compare\": [{\"key\": \"$(b64 "$register")\", \"target\": \"MOD\", \"result\": \"EQUAL\", \"mod_revision\": \"$revision\"}],
        \"success\": [{\"request_put\": {\"key\": \"$(b64 "$register")\", \"value\": \"$(b64 "$config")\"}}]}")
    jq -e .succeeded <<<"$answer" >/dev/null || fail "the CAS on $register failed: $answer"
done
log "added $victim as a learner of shards $(echo $left_out)"

# rejoined: whether the restarted node is a member of every shard again,
# each of 3 members; the roles it reports on the way are kept in `roles`.
rejoined() {
    local entries
    entries=$(shards "$victim")
    jq -r '.[] | "\(.shard) \(.role)"' <<<"$entries" >>"$work/roles"
    jq -e 'length == 4 and all(.[]; .source == "replica" and .role != "learner" and
        (.members | length) == 3)' <<<"$entries" >/dev/null
}
: >"$work/roles"
eventually 60 "$victim is a member of every shard again" rejoined
seen=$(awk '$2 == "learner" { print $1 }' "$work/roles" | sort -u | paste -sd, -)
log "$victim rejoined every shard as a member (seen as learner: shards ${seen:-none})"
read_back "$victim"
log "every acknowledged key reads back through $victim"

for node in "${nodes[@]}"; do
    node_stop "$node"
done
