#!/usr/bin/env bash
# Erasure coding in the binary (plan M8-07a, design sections 8.2 to 8.5):
# nodes ec1 to ec4 of cluster ec, with `[cluster] multi_node = true` and
# `[ec] enabled = true`, share the etcd control store over mutual TLS
# (under the prefix `ec/`). A local bucket has one shard of 3 members, and
# `[ec]` codes stripes of 2+1 from 3 eligible nodes (`min_eligible_nodes =
# 3`, `parity_fragments = 1`) as soon as an object is written
# (`ec_after_seconds = 0`). The scenario checks:
#
# - that a 16 MiB object is coded: one `EC_PUBLISH` committed
#   (`skys3_encoded_objects_total`), and three nodes hold its fragments
#   (`skys3_fragment_stored_bytes`);
# - that the members drop their replicas: the log segments holding the
#   object's bytes are compacted away on every member;
# - that it reads back with the same SHA-256 through every node, from data
#   fragments alone (no parity fragment read);
# - that with each holder of a fragment that does not lead the shard
#   killed in turn (`kill -9`), GET through another node returns the same
#   SHA-256, decoded from the parity fragment where the killed node held a
#   data fragment, as at least one of them did
#   (`skys3_fragment_reads_total{fragment="parity"}`).
#
# The gateways keep no hot cache, so every GET reads fragments. Log
# segments are 2 MiB, so the object's bytes fill closed segments that
# compaction can remove, and checkpoints, which compaction follows, come
# every second. It runs only over etcd (`# e2e: etcd`), beside the other
# cluster scenarios, takes about 25 seconds, and uses the ports
# E2E_PORT+50 to E2E_PORT+61, which only the single-node scenarios of the
# file store use otherwise.
# e2e: etcd
set -euo pipefail
. "$E2E_HERE/lib.sh"

[ -n "${E2E_ETCD_URL:-}" ] || fail "the erasure-coding scenario runs with SKYS3_E2E_CONTROL=etcd"
work=$E2E_DIR/work/erasure-coding
pki=$E2E_DIR/pki
mkdir -p "$work"
key=AKIASKYS3E2ECODING secret=skys3-e2e-coding-secret-0123456789
bucket=coded
size=$((16 << 20))

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
cluster_id = "ec"
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
tls_cert_file = "$pki/ec-$node.pem"
tls_key_file = "$pki/ec-$node.key"
tls_ca_file = "$pki/ec-ca.pem"

[storage]
segment_bytes = 2097152
index_checkpoint_interval_seconds = 1
disk_min_free_bytes = 0

[cache]
hot_cache_bytes_per_node = 0
reserve_fraction = 0.0

[ec]
enabled = true
min_eligible_nodes = 3
parity_fragments = 1
fragment_release_delay_seconds = 1

[buckets.defaults]
mode = "local"
shards_per_bucket = 1
replicas = 3
min_write_replicas = 2
ec_after_seconds = 0

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
        echo "AWS_EC2_METADATA_DISABLED=true"
        echo "AWS_CONFIG_FILE=$E2E_DIR/aws/no-config"
        echo "AWS_SHARED_CREDENTIALS_FILE=$E2E_DIR/aws/no-credentials"
    } >"$dir/env"
}

s3_of() { cat "$E2E_DIR/$1/s3"; }
admin_of() { cat "$E2E_DIR/$1/admin"; }
# shard <node>: the bucket's one shard as the node's GET /v1/shards has it.
shard() { admin "$(admin_of "$1")" /v1/shards | jq -c --arg b "$bucket" '.shards[] | select(.bucket == $b)'; }
# value <node> <series>: one series of the node's /metrics, 0 if absent.
value() { local v; v=$(metric "$(admin_of "$1")" "$2"); echo "${v:-0}"; }
# total <series>: the series summed over the running nodes.
total() {
    local node sum=0
    for node in $(cat "$work/running"); do
        sum=$(awk -v a="$sum" -v b="$(value "$node" "$1")" 'BEGIN { print a + b }')
    done
    echo "$sum"
}
# bulk <node>: the bytes of the log's bulk segments on the node's disk.
bulk() { find "$E2E_DIR/$1/data/log" -name 'bulk-*.seg' -printf '%s\n' | awk '{ s += $1 } END { print s + 0 }'; }

# put <file>: PutObject of the file as `large`, retried while the primary
# waits for its leases; prints the HTTP status.
put() {
    curl -sS -o "$work/put" -w '%{http_code}' --max-time 60 --retry 5 --retry-all-errors \
        --aws-sigv4 "aws:amz:us-east-1:s3" --user "$key:$secret" \
        -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" --data-binary "@$1" \
        -X PUT "$(s3_of ec1)/$bucket/large" 2>/dev/null || true
}
# read_back <node>: whether GET of `large` through the node returns the
# bytes written, by SHA-256.
read_back() {
    local status
    status=$(curl -sS -o "$work/got-$1" -w '%{http_code}' --max-time 60 \
        --aws-sigv4 "aws:amz:us-east-1:s3" --user "$key:$secret" \
        -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" "$(s3_of "$1")/$bucket/large" 2>/dev/null) ||
        return 1
    [ "$status" = 200 ] && [ "$(sha256sum <"$work/got-$1" | cut -d' ' -f1)" = "$(cat "$work/sha256")" ]
}

# --- The cluster and the bucket -----------------------------------------------

started=$SECONDS
nodes=(ec1 ec2 ec3 ec4)
offsets=([1]=50 [2]=53 [3]=56 [4]=59)
for i in 1 2 3 4; do
    configure "ec$i" "${offsets[$i]}"
    node_start "ec$i"
done
echo "${nodes[*]}" >"$work/running"
create_bucket "$(s3_of ec1)" "$key" "$secret" "$bucket"
# placement: the shard as the first node that has it placed reports it;
# a node that holds no replica of it reports no members.
placement() {
    local node
    for node in "${nodes[@]}"; do
        shard "$node" | jq -ce 'select((.members | length) == 3 and .primary != null)' && return
    done
    return 1
}
placed() { placement >/dev/null; }
eventually 30 "the bucket's shard has 3 members" placed
placement >"$work/shard"
primary=$(jq -r .primary "$work/shard")
members=$(jq -r '.members | join(" ")' "$work/shard")
replica() { shard "$1" | jq -e '.source == "replica"' >/dev/null; }
for node in $members; do
    eventually 30 "$node holds its replica" replica "$node"
done
log "bucket $bucket: members $members, primary $primary"

# --- A 16 MiB object is coded, and the members drop their replicas ------------

random_file "$work/large" "$size"
sha256sum <"$work/large" | cut -d' ' -f1 >"$work/sha256"
status=$(put "$work/large")
[ "$status" = 200 ] || fail "PUT of 16 MiB answered $status: $(cat "$work/put")"
put_at=$SECONDS
log "wrote 16 MiB, SHA-256 $(cat "$work/sha256")"
holders() {
    local node
    for node in "${nodes[@]}"; do
        [ "$(value "$node" skys3_fragment_stored_bytes)" != 0 ] && echo "$node"
    done
}
coded() {
    [ "$(total skys3_encoded_objects_total)" = 1 ] && [ "$(holders | wc -l)" -eq 3 ]
}
eventually 60 "the object is coded on 3 nodes" coded
holding=$(holders | paste -sd' ' -)
log "coded $((SECONDS - put_at)) s after the PUT; fragments on $holding ($(value "${holding%% *}" skys3_fragment_stored_bytes) bytes each)"
# The 16 MiB the members held are gone, but for the last segment, which
# still takes records (2 MiB at most).
dropped() {
    local node
    for node in $members; do
        [ "$(bulk "$node")" -lt $((4 << 20)) ] || return 1
    done
}
eventually 60 "the members drop their replicas" dropped
log "the members dropped their replicas $((SECONDS - put_at)) s after the PUT: bulk segments $(for n in $members; do printf '%s=%s ' "$n" "$(bulk "$n")"; done)"

# --- Reads, healthy and with a holder down ------------------------------------

parity='skys3_fragment_reads_total{fragment="parity",outcome="read"}'
for node in "${nodes[@]}"; do
    read_back "$node" || fail "GET through $node does not return the object"
done
[ "$(total "$parity")" = 0 ] || fail "a healthy read read a parity fragment"
log "GET through every node returns the same SHA-256, from data fragments alone"

decoded=0
for victim in $holding; do
    [ "$victim" = "$primary" ] && continue
    kill -KILL "$(cat "$E2E_DIR/$victim/pid")"
    wait "$(cat "$E2E_DIR/$victim/pid")" 2>/dev/null || true
    rm -f "$E2E_DIR/$victim/pid"
    killed_at=$SECONDS
    reader=
    for node in "${nodes[@]}"; do
        if [ "$node" != "$victim" ] && [ "$node" != "$primary" ]; then
            reader=$node
            break
        fi
    done
    before=$(value "$reader" "$parity")
    eventually 60 "GET through $reader returns the object with $victim down" read_back "$reader"
    after=$(value "$reader" "$parity")
    if [ "$after" != "$before" ]; then
        decoded=$((decoded + 1))
        how="decoded with the parity fragment"
    else
        how="from data fragments, $victim held the parity fragment"
    fi
    log "with $victim killed, GET through $reader returns the same SHA-256 after $((SECONDS - killed_at)) s, $how"
    node_start "$victim"
done
[ "$decoded" -gt 0 ] || fail "no holder down held a data fragment: nothing was decoded"

for node in "${nodes[@]}"; do
    node_stop "$node"
done
log "the scenario took $((SECONDS - started)) s"
