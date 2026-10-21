#!/usr/bin/env bash
# Erasure coding in the binary (plans M8-07a and M8-07b, design sections
# 8.2 to 8.7):
# nodes ec1 to ec4 of cluster ec share the etcd control store over mutual
# TLS (under the prefix `ec/`), and so run erasure coding, as every node
# of a cluster does. A local bucket has one shard of 3 members, and
# `[ec]` codes stripes of 2+1 (`parity_fragments = 1`) as soon as an
# object is written (`ec_after_seconds = 0`) and enough nodes are
# eligible. The scenario checks:
#
# - that with the default `min_eligible_nodes` of 5, the four nodes code
#   nothing: a 16 MiB object stays replicated;
# - that the runbook "Turning erasure coding on" turns it on: with
#   `min_eligible_nodes = 3` in every configuration, the nodes restart one
#   at a time, each once the one before is ready, the primary last, and
#   the object reads back between the restarts;
# - that the object is then coded: one `EC_PUBLISH` committed
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
#   (`skys3_fragment_reads_total{fragment="parity"}`);
# - that draining a holder that does not lead (`POST /v1/nodes/<id>/drain`)
#   moves its fragment to the node outside the stripe: the drained node's
#   `GET /v1/cluster` reports no unreleased fragment, the primary counts
#   the move (`skys3_moved_fragments_total`), and reads decode nothing;
# - that once a holder that does not lead is lost for good (`kill -9`, not
#   restarted), the primary counts its fragment lost
#   (`skys3_unrepaired_fragments`) after `fragment_repair_after_seconds`
#   (10), rebuilds it on the survivors (`skys3_repaired_fragments_total`,
#   `skys3_repair_bytes_total`), and the backlog that
#   `SkyS3FragmentsUnrepaired` reads clears
#   (`skys3_oldest_unrepaired_age_seconds` back to 0); the three survivors
#   then hold the stripe, and GET through each reads data fragments alone.
#
# The gateways keep no hot cache, so every GET reads fragments. Log
# segments are 2 MiB, so the object's bytes fill closed segments that
# compaction can remove, and checkpoints, which compaction follows, come
# every second. Repairs and moves are paced at 8 MiB/s, so the rebuild of
# an 8 MiB fragment (two read, one written) takes about 3 seconds, during
# which its loss is counted. It runs only over etcd (`# e2e: etcd`),
# beside the other cluster scenarios, takes about 80 seconds, and uses the
# ports E2E_PORT+50 to E2E_PORT+61, which only the single-node scenarios
# of the file store use otherwise.
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

# configure <node> <port offset> <eligible>: the directory $E2E_DIR/<node>
# and the configuration of node <node>, its gateway at E2E_PORT+<offset>,
# its admin listener one port up and its transport two, coding once
# <eligible> nodes are eligible (`min_eligible_nodes`).
configure() {
    local node=$1 offset=$2 eligible=$3 dir=$E2E_DIR/$1
    mkdir -p "$dir"
    printf '%s' "$secret" >"$dir/secret"
    echo "127.0.0.1:$((E2E_PORT + offset + 1))" >"$dir/admin"
    echo "http://127.0.0.1:$((E2E_PORT + offset))" >"$dir/s3"
    cat >"$dir/skys3.toml" <<EOF
[cluster]
cluster_id = "ec"

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
min_eligible_nodes = $eligible
parity_fragments = 1
fragment_release_delay_seconds = 1
fragment_repair_after_seconds = 10
repair_bytes_per_second_per_node = 8388608

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
# The nodes start with the default `min_eligible_nodes` of 5: four nodes
# are too few to code, so objects stay replicated until the runbook
# "Turning erasure coding on" lowers it.
for i in 1 2 3 4; do
    configure "ec$i" "${offsets[$i]}" 5
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
log "wrote 16 MiB, SHA-256 $(cat "$work/sha256")"
holders() {
    local node
    for node in "${nodes[@]}"; do
        if [ "$(value "$node" skys3_fragment_stored_bytes)" != 0 ]; then
            echo "$node"
        fi
    done
}

# --- Turning erasure coding on: a rolling restart -------------------------------

# With 4 nodes and `min_eligible_nodes = 5`, the encoders, which scan
# every second here, code nothing: the object stays replicated.
sleep 5
[ "$(total skys3_encoded_objects_total)" = 0 ] || fail "a cluster smaller than min_eligible_nodes coded an object"
[ -z "$(holders)" ] || fail "a cluster smaller than min_eligible_nodes holds fragments: $(holders | paste -sd' ' -)"
log "4 nodes under min_eligible_nodes = 5 code nothing: the object stays replicated"
# The runbook's steps: lower `min_eligible_nodes` to 3 in every node's
# configuration, and restart the nodes one at a time, each once the one
# before reports ready (node_start waits for that), the primary last. The
# object reads back between the restarts.
restarted_at=$SECONDS
for node in $(for n in "${nodes[@]}"; do [ "$n" = "$primary" ] || echo "$n"; done) "$primary"; do
    configure "$node" "${offsets[${node#ec}]}" 3
    node_stop "$node"
    node_start "$node"
    reader=ec1
    [ "$node" != ec1 ] || reader=ec2
    eventually 60 "GET through $reader returns the object once $node restarted" read_back "$reader"
done
log "restarted the 4 nodes one at a time in $((SECONDS - restarted_at)) s with min_eligible_nodes = 3; the object read back throughout"
# The restarts may move the shard's lease: the primary is the one now.
eventually 30 "the bucket's shard has 3 members after the restarts" placed
placement >"$work/shard"
primary=$(jq -r .primary "$work/shard")
members=$(jq -r '.members | join(" ")' "$work/shard")
log "after the restarts: members $members, primary $primary"
coding_at=$SECONDS
coded() {
    [ "$(total skys3_encoded_objects_total)" = 1 ] && [ "$(holders | wc -l)" -eq 3 ]
}
eventually 60 "the object is coded on 3 nodes" coded
holding=$(holders | paste -sd' ' -)
log "coded $((SECONDS - coding_at)) s after the restarts; fragments on $holding ($(value "${holding%% *}" skys3_fragment_stored_bytes) bytes each)"
# The 16 MiB the members held are gone, but for the last segment, which
# still takes records (2 MiB at most).
dropped() {
    local node
    for node in $members; do
        [ "$(bulk "$node")" -lt $((4 << 20)) ] || return 1
    done
}
eventually 60 "the members drop their replicas" dropped
log "the members dropped their replicas $((SECONDS - coding_at)) s after the restarts: bulk segments $(for n in $members; do printf '%s=%s ' "$n" "$(bulk "$n")"; done)"

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

# --- A drained holder's fragment moves off it -----------------------------------

# unreleased <node>: the fragments the node's stores hold that no primary
# released, from its GET /v1/cluster.
unreleased() { admin "$(admin_of "$1")" /v1/cluster | jq -er '.fragments.unreleased'; }
# named: the running nodes holding fragments no primary released.
named() {
    local node
    for node in $(cat "$work/running"); do
        if [ "$(unreleased "$node")" -gt 0 ]; then
            echo "$node"
        fi
    done
}
three() { [ "$(named | wc -l)" -eq 3 ]; }
# drain <method> <node>: POST or DELETE /v1/nodes/<node>/drain on ec1.
drain() {
    curl -sS -o "$work/drain" -w '%{http_code}' --max-time 10 -X "$1" \
        "http://$(admin_of ec1)/v1/nodes/$2/drain"
}
# more <a> <b>: whether number a is above b.
more() { awk -v a="$1" -v b="$2" 'BEGIN { exit !(a > b) }'; }

# The holders the restarts left (a repair may have moved a fragment while
# a holder was down), and the node outside the stripe.
eventually 60 "three nodes hold the stripe" three
drained= outside=
for node in "${nodes[@]}"; do
    if [ "$(unreleased "$node")" = 0 ]; then
        outside=$node
    elif [ "$node" != "$primary" ] && [ -z "$drained" ]; then
        drained=$node
    fi
done
moved=$(value "$primary" skys3_moved_fragments_total)
parity_before=$(total "$parity")
status=$(drain POST "$drained")
[ "$status" = 200 ] || fail "POST /v1/nodes/$drained/drain answered $status: $(cat "$work/drain")"
drained_at=$SECONDS
emptied() { [ "$(unreleased "$drained")" = 0 ]; }
eventually 60 "drained $drained holds no unreleased fragment" emptied
[ "$(unreleased "$outside")" -gt 0 ] || fail "the fragment did not move to $outside"
more "$(value "$primary" skys3_moved_fragments_total)" "$moved" ||
    fail "the primary $primary counted no move"
for node in "${nodes[@]}"; do
    read_back "$node" || fail "GET through $node does not return the object after the drain"
done
[ "$(total "$parity")" = "$parity_before" ] || fail "a read decoded after the drain"
log "drained $drained in $((SECONDS - drained_at)) s: its fragment moved to $outside, and reads decode nothing"
status=$(drain DELETE "$drained")
[ "$status" = 200 ] || fail "DELETE /v1/nodes/$drained/drain answered $status: $(cat "$work/drain")"

# --- A holder lost for good is rebuilt on the survivors -------------------------

victim=
for node in $(named); do
    if [ "$node" != "$primary" ]; then
        victim=$node
        break
    fi
done
kill -KILL "$(cat "$E2E_DIR/$victim/pid")"
wait "$(cat "$E2E_DIR/$victim/pid")" 2>/dev/null || true
rm -f "$E2E_DIR/$victim/pid"
lost_at=$SECONDS
survivors=$(for node in "${nodes[@]}"; do [ "$node" = "$victim" ] || echo "$node"; done | paste -sd' ' -)
echo "$survivors" >"$work/running"
repaired=$(value "$primary" skys3_repaired_fragments_total)
repair_bytes=$(value "$primary" skys3_repair_bytes_total)
parity_before=$(total "$parity")
# The primary counts the loss once the victim has left its checks
# unanswered for 10 s, and until the rebuild's EC_RELOCATE commits and the
# next pass finds nothing lost.
counted_at= deadline=$((SECONDS + 90))
until more "$(value "$primary" skys3_repaired_fragments_total)" "$repaired"; do
    if [ -z "$counted_at" ] && more "$(value "$primary" skys3_unrepaired_fragments)" 0; then
        counted_at=$SECONDS
    fi
    [ "$SECONDS" -lt "$deadline" ] || fail "the primary $primary did not repair $victim's fragment within 90 s"
    sleep 0.5
done
repaired_at=$SECONDS
[ -n "$counted_at" ] || fail "the primary $primary never counted $victim's fragment lost"
more "$(value "$primary" skys3_repair_bytes_total)" "$repair_bytes" || fail "the repair read and wrote nothing"
cleared() {
    [ "$(total skys3_unrepaired_fragments)" = 0 ] && [ "$(total skys3_oldest_unrepaired_age_seconds)" = 0 ]
}
eventually 30 "nothing is left to repair (SkyS3FragmentsUnrepaired clears)" cleared
[ "$(named | paste -sd' ' -)" = "$survivors" ] || fail "the survivors do not hold the stripe: $(named | paste -sd' ' -)"
for node in $survivors; do
    read_back "$node" || fail "GET through $node does not return the object after the repair"
done
[ "$(total "$parity")" = "$parity_before" ] || fail "a read decoded after the repair"
log "$victim lost: counted lost after $((counted_at - lost_at)) s, rebuilt on the survivors after $((repaired_at - lost_at)) s; reads decode nothing"

for node in $survivors; do
    node_stop "$node"
done
log "the scenario took $((SECONDS - started)) s"
