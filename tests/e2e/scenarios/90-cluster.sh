#!/usr/bin/env bash
# Three nodes of one cluster (plan M8-04, design sections 6.7 and 12):
# nodes c1, c2, and c3 of cluster c3 share the etcd control store over
# mutual TLS (under its own prefix, `c3/`, beside the single-node
# cluster's), which makes them nodes of one cluster, and authenticate each
# other over the intra-cluster transport with mutual TLS. The scenario
# checks:
#
# - that a configuration that still sets `[cluster] multi_node`, a flag
#   plan M8-09 removed, does not load, and says why;
# - each node's registration in etcd (address, zone, rack, disks, data
#   directory) and in its admin API's `cluster` object;
# - heartbeats: one node is elected coordinator (every node may be), and
#   the other two send it heartbeats over the transport, which it answers
#   as coordinator;
# - that the transport refuses a certificate from a CA the nodes do not
#   trust and one of another cluster whose CA they do trust (pb's), and
#   that a node does not start with another data directory's node ID, or
#   with a certificate that names another node;
# - a flood of 1,000 connections that never finish a handshake (threat
#   T7) against c3, whose `[transport] max_inflight_handshakes` is 4:
#   the in-flight gauge never passes 4, the node's resident memory grows
#   by less than 32 MiB, and a node gets in once the flood ends;
# - the runbook "Moving a single node into a cluster": a single node's data
#   directory that holds a bucket does not start over the cluster's store
#   and is left as it was, and the bucket's objects, copied with two
#   `aws s3 sync`s, list and read back through every node of the cluster.
#
# It runs only over etcd (`# e2e: etcd`: the default with
# SKYS3_E2E_CONTROL=etcd, beside the single-node scenarios), takes about
# a minute, and uses the ports E2E_PORT+88 to E2E_PORT+98.
# e2e: etcd
set -euo pipefail
. "$E2E_HERE/lib.sh"

[ -n "${E2E_ETCD_URL:-}" ] || fail "the cluster scenario runs with SKYS3_E2E_CONTROL=etcd"
work=$E2E_DIR/work/cluster
pki=$E2E_DIR/pki
mkdir -p "$work"
# The nodes trust their cluster's CA and pb's: a CA two clusters share,
# where the SPIFFE trust domain still tells the clusters apart.
bundle=$work/ca-bundle.pem
cat "$pki/c3-ca.pem" "$pki/pb-ca.pem" >"$bundle"
handshakes='skys3_handshakes_total{listener="transport",outcome'
in_flight='skys3_handshakes_in_flight{listener="transport"}'

# etcd's JSON gateway, with the suite's client certificate.
etcd_call() {
    curl -sS --fail-with-body --max-time 10 --cacert "$pki/e2e-ca.pem" \
        --cert "$pki/etcd-client.pem" --key "$pki/etcd-client.key" \
        -X POST "$E2E_ETCD_URL$1" -d "$2"
}
b64() { printf '%s' "$1" | base64 -w0; }
# etcd_get <key>: the value at <key>, or nothing.
etcd_get() {
    etcd_call /v3/kv/range "{\"key\": \"$(b64 "$1")\"}" | jq -r '.kvs[0].value // empty | @base64d'
}

# configure <name> <node id> <certificate> <port offset> <cap>:
# the directory $E2E_DIR/<name> and the configuration of node <node id>,
# with the certificate c3-<certificate>, its gateway at E2E_PORT+<offset>,
# its admin listener one port up and its transport two, and at most <cap>
# handshakes at once. Offset 0 puts all three on port 0, and the transport
# advertises 127.0.0.1:1, for a node that must not start.
configure() {
    local name=$1 node=$2 cert=$3 offset=$4 cap=$5 dir=$E2E_DIR/$1
    local gateway admin transport advertise=""
    if [ "$offset" = 0 ]; then
        gateway=127.0.0.1:0 admin=127.0.0.1:0 transport=127.0.0.1:0
        advertise='advertise = "127.0.0.1:1"'
    else
        gateway=127.0.0.1:$((E2E_PORT + offset))
        admin=127.0.0.1:$((E2E_PORT + offset + 1))
        transport=127.0.0.1:$((E2E_PORT + offset + 2))
    fi
    local store="backend = \"etcd\"
etcd_endpoints = [\"$E2E_ETCD_URL\"]
etcd_tls_ca_file = \"$pki/e2e-ca.pem\"
etcd_tls_cert_file = \"$pki/etcd-client.pem\"
etcd_tls_key_file = \"$pki/etcd-client.key\""
    mkdir -p "$dir"
    echo "$admin" >"$dir/admin"
    cat >"$dir/skys3.toml" <<EOF
[cluster]
cluster_id = "c3"

[node]
node_id = "$node"
data_dir = "$dir/data"
zone = "z1"
rack = "r-$node"

[gateway]
listen = "$gateway"

[control_store]
$store
config_poll_interval_seconds = 1

[transport]
listen = "$transport"
$advertise
max_inflight_handshakes = $cap
tls_cert_file = "$pki/c3-$cert.pem"
tls_key_file = "$pki/c3-$cert.key"
tls_ca_file = "$bundle"

[storage]
disk_min_free_bytes = 0
$common
[admin]
listen = "$admin"

[logging]
filter = "info"
EOF
}

# What every node here shares: local buckets by default, and a static
# credential that may do anything.
key=AKIASKYS3E2ECLUSTER secret=skys3-e2e-cluster-secret-0123456789
printf '%s' "$secret" >"$work/secret"
common="
[buckets.defaults]
mode = \"local\"

[identity]
sts_web_identity = false

[identity.static_credentials.admin]
access_key_id = \"$key\"
secret_access_key_file = \"$work/secret\"
policy = '{\"Version\": \"2012-10-17\", \"Statement\": {\"Effect\": \"Allow\", \"Action\": \"*\", \"Resource\": \"*\"}}'
"

# refused_start <name> <text>: the node of $E2E_DIR/<name> exits non-zero
# at startup, and its log says <text>.
refused_start() {
    local log=$E2E_DIR/logs/$1.log
    if timeout 60 "$E2E_BINARY" --config "$E2E_DIR/$1/skys3.toml" >"$log" 2>&1; then
        fail "node $1 started"
    fi
    grep -q "$2" "$log" || fail "node $1 failed otherwise: $(tail -n 5 "$log")"
    log "node $1 does not start: $(grep -o "$2.*" "$log" | head -n 1)"
}

# count <admin> <series>: a counter's or gauge's whole value, 0 while it
# is not exported.
count() {
    local value
    value=$(metric "$1" "$2")
    value=${value:-0}
    echo "${value%.*}"
}

# present <port> <certificate stem> [<seconds>]: kept or refused
# (cluster.py), with <seconds> for the connection and handshake (10).
present() {
    python3 "$E2E_HERE/cluster.py" present --port "$1" --ca "$pki/c3-ca.pem" \
        --cert "$pki/$2.pem" --key "$pki/$2.key" --timeout "${3:-10}" || true
}

# --- Configuration ----------------------------------------------------------

# The flags that gated the cluster until plan M8-09 are gone: a
# configuration that still sets one does not load, and says why.
configure c-old c1 c1 0 256
sed -i 's/^cluster_id = "c3"$/cluster_id = "c3"\nmulti_node = true/' "$E2E_DIR/c-old/skys3.toml"
if timeout 30 "$E2E_BINARY" --config "$E2E_DIR/c-old/skys3.toml" >"$E2E_DIR/logs/c-old.log" 2>&1; then
    fail "a configuration with [cluster] multi_node started"
fi
grep -q 'cluster.multi_node: was removed' "$E2E_DIR/logs/c-old.log" ||
    fail "the configuration with multi_node failed otherwise: $(tail -n 5 "$E2E_DIR/logs/c-old.log")"
log "a removed key does not load: $(grep -o 'cluster.multi_node: was removed[^;]*' "$E2E_DIR/logs/c-old.log" | head -n 1)"

# --- Registration -----------------------------------------------------------

offsets=([1]=90 [2]=93 [3]=96)
caps=([1]=256 [2]=256 [3]=4)
for i in 1 2 3; do
    configure "c$i" "c$i" "c$i" "${offsets[$i]}" "${caps[$i]}"
    node_start "c$i"
done
for i in 1 2 3; do
    transport=127.0.0.1:$((E2E_PORT + offsets[i] + 2))
    registration=$(etcd_get "c3/nodes/c$i.json")
    [ -n "$registration" ] || fail "c$i is not registered"
    jq -e --arg address "$transport" --arg rack "r-c$i" \
        '.address == $address and .zone == "z1" and .rack == $rack and
         (.instance_id | length) > 0 and (.disks | length) >= 1 and
         all(.disks[]; .capacity_bytes > 0) and (.departing | not)' \
        <<<"$registration" >/dev/null || fail "c$i registered $registration"
    health=$(admin "$(cat "$E2E_DIR/c$i/admin")" /v1/health)
    jq -e --arg address "$transport" --argjson cap "${caps[$i]}" \
        '.cluster.multi_node and .cluster.registration.registered and
         .cluster.registration.error == null and .cluster.transport.listen == $address and
         .cluster.transport.advertise == $address and
         .cluster.transport.max_inflight_handshakes == $cap' \
        <<<"$health" >/dev/null || fail "c$i health: $(jq -c .cluster <<<"$health")"
done
log "registrations: $(etcd_get c3/nodes/c2.json | jq -c '{address, zone, rack, instance_id}') (c2)"

# --- Coordinator and heartbeats ---------------------------------------------

# Every node may be elected (plan M8-09): one holds coordinator.lease, and
# the other two send it heartbeats over the transport, which it answers as
# coordinator.
c1_admin=$(cat "$E2E_DIR/c1/admin")
holder() { etcd_get c3/coordinator.lease | jq -r '.holder // empty'; }
elected() { [ -n "$(holder)" ]; }
eventually 60 "a node holds the coordinator lease" elected
leader=$(holder)
case "$leader" in
c1 | c2 | c3) ;;
*) fail "the coordinator lease names $leader" ;;
esac
# delivered <name>: whether the node's heartbeats reach the coordinator.
delivered() {
    admin "$(cat "$E2E_DIR/$1/admin")" /v1/health |
        jq -e --arg leader "$leader" \
            '.cluster.heartbeats.delivered >= 2 and .cluster.heartbeats.coordinator == $leader'
}
for i in 1 2 3; do
    [ "c$i" = "$leader" ] && continue
    eventually 60 "c$i's heartbeats reach $leader" delivered "c$i"
done
leader_admin=$(cat "$E2E_DIR/$leader/admin")
completed=$(count "$leader_admin" "$handshakes=\"completed\"}")
[ "$completed" -ge 2 ] || fail "$leader completed $completed transport handshakes"
log "coordinator $leader; heartbeats: $(admin "$(cat "$E2E_DIR/c$(((${leader#c} % 3) + 1))/admin")" /v1/health | jq -c .cluster.heartbeats), $leader completed $completed handshakes"

# --- Strangers --------------------------------------------------------------

c1_port=$((E2E_PORT + 92))
failed=$(count "$c1_admin" "$handshakes=\"failed\"}")
[ "$(present "$c1_port" c3-c2)" = kept ] || fail "c1 refused a certificate of c2"
result=$(present "$c1_port" c3-foreign)
[[ $result == refused* ]] || fail "c1 let in a certificate of another CA: $result"
result=$(present "$c1_port" pb-pb1)
[[ $result == refused* ]] || fail "c1 let in a node of cluster pb: $result"
refusals() { [ "$(count "$c1_admin" "$handshakes=\"failed\"}")" -ge $((failed + 2)) ]; }
eventually 10 "c1 counts both refusals" refusals
log "c1 refuses another CA and another cluster's SPIFFE ID"

registered=$(etcd_get c3/nodes/c1.json)
configure c1-twin c1 c1 0 256
refused_start c1-twin "for another data directory"
[ "$(etcd_get c3/nodes/c1.json)" = "$registered" ] || fail "c1's registration changed"
configure c4 c4 c2 0 256
refused_start c4 "certificate names"

# --- Handshake flood --------------------------------------------------------

c3_admin=$(cat "$E2E_DIR/c3/admin")
c3_port=$((E2E_PORT + 98))
c3_pid=$(cat "$E2E_DIR/c3/pid")
rss() { awk '/^VmRSS:/ { print $2 }' "/proc/$c3_pid/status"; }
before=$(rss)
timed_out=$(count "$c3_admin" "$handshakes=\"timed_out\"}")
python3 "$E2E_HERE/cluster.py" flood --port "$c3_port" --connections 1000 --seconds 15 \
    >"$work/flood.out" &
flood=$!
peak=$before most=0
for _ in $(seq 1 28); do
    sleep 0.5
    now=$(count "$c3_admin" "$in_flight")
    [ "$now" -le 4 ] || fail "c3 runs $now transport handshakes at once, over its cap of 4"
    [ "$now" -gt "$most" ] && most=$now
    current=$(rss)
    [ "$current" -gt "$peak" ] && peak=$current
done
wait "$flood"
opened=$(head -n 1 "$work/flood.out")
[ "$most" = 4 ] || fail "the flood of $opened connections never filled c3's 4 slots"
grown=$(((peak - before) / 1024))
[ "$grown" -lt 32 ] || fail "c3's resident memory grew by $grown MiB under the flood"
after=$(count "$c3_admin" "$handshakes=\"timed_out\"}")
[ "$after" -ge $((timed_out + 4)) ] ||
    fail "c3 timed out $after handshakes; the flood's first 4 should have"
# The flood's last connections reach c3 after it ends: the kernel
# completes, as their retransmissions arrive, handshakes the full accept
# queue held back, and such a connection, whose client is gone, can hold
# a slot until the handshake timeout (10 s). The scenario waits until none
# is left: no handshake in flight and c3's accept queue empty (the
# listener's receive queue in /proc/net/tcp), over four samples in a row.
accept_queue() {
    local hex
    hex=$(awk -v port="$(printf ':%04X' "$c3_port")" \
        '$4 == "0A" && substr($2, length($2) - 4) == port { split($5, q, ":"); print q[2]; exit }' \
        /proc/net/tcp)
    echo $((16#${hex:-0}))
}
quiet=0
drained() {
    if [ "$(metric "$c3_admin" "$in_flight")" = 0 ] && [ "$(accept_queue)" = 0 ]; then
        quiet=$((quiet + 1))
    else
        quiet=0
    fi
    [ "$quiet" -ge 4 ]
}
eventually 90 "the flood's connections are gone from c3" drained
# A node gets in. Should a late flood connection still take a slot, the
# node waits for it, at most one handshake timeout: 30 s is ample.
started=$SECONDS
result=$(present "$c3_port" c3-c1 30)
[ "$result" = kept ] || {
    curl -sS --max-time 10 "http://$c3_admin/metrics" | grep "^skys3_handshakes" >&2
    tail -n 20 "$E2E_DIR/logs/c3.log" >&2
    fail "c3 refused c1 after the flood: $result"
}
log "flood: $opened connections established, at most $most handshakes in flight, RSS $((before / 1024)) -> $((peak / 1024)) MiB, timed out $(count "$c3_admin" "$handshakes=\"timed_out\"}"); c1's probe took $((SECONDS - started))s after the drain, 5 of them checking it was kept"

# --- Moving a single node into the cluster ----------------------------------

# The runbook "Moving a single node into a cluster": node c1 of a single
# node, over the file control store, holds a local bucket. Its data
# directory does not start over the cluster's store, and is left as it
# was; the bucket moves with two `aws s3 sync`s through a scratch
# directory (steps 1, 3, and 4), and every cluster node lists and reads
# every object.
single=$E2E_DIR/c-single
single_s3=http://127.0.0.1:$((E2E_PORT + 88))
configure_single() {
    mkdir -p "$single"
    echo "127.0.0.1:$((E2E_PORT + 89))" >"$single/admin"
    cat >"$single/skys3.toml" <<EOF
[cluster]
cluster_id = "c3"

[node]
node_id = "c1"
data_dir = "$single/data"

[gateway]
listen = "127.0.0.1:$((E2E_PORT + 88))"

[control_store]
backend = "file"

[storage]
disk_min_free_bytes = 0
$common
[admin]
listen = "127.0.0.1:$((E2E_PORT + 89))"

[logging]
filter = "info"
EOF
}
# s3 <endpoint> <args...>: the AWS CLI against a node with the credential.
s3() {
    local endpoint=$1
    shift
    AWS_ACCESS_KEY_ID=$key AWS_SECRET_ACCESS_KEY=$secret AWS_MAX_ATTEMPTS=10 \
        aws --endpoint-url "$endpoint" "$@"
}
configure_single
node_start c-single
create_bucket "$single_s3" "$key" "$secret" moved
mkdir -p "$work/objects" "$work/scratch"
for i in 1 2 3 4 5; do
    random_file "$work/objects/object-$i" $((i * 65536))
done
s3 "$single_s3" s3 sync --only-show-errors "$work/objects" s3://moved/ ||
    fail "the single node did not take the objects"
node_stop c-single
before=$(find "$single/data" -type f -printf '%P %s\n' | sort | sha256sum)
# Over the cluster's etcd store, as c1 with its certificate.
configure c-single c1 c1 0 256
refused_start c-single "that it served as a single node"
grep -q 'Moving a single node into a cluster' "$E2E_DIR/logs/c-single.log" ||
    fail "the refusal does not name the runbook"
after=$(find "$single/data" -type f -printf '%P %s\n' | sort | sha256sum)
[ "$before" = "$after" ] || fail "the refused start changed the single node's data directory"
# Steps 3 and 4: the bucket on the cluster, and the objects copied.
configure_single
node_start c-single
c1_s3=http://127.0.0.1:$((E2E_PORT + 90))
create_bucket "$c1_s3" "$key" "$secret" moved
moved_at=$SECONDS
s3 "$single_s3" s3 sync --only-show-errors s3://moved/ "$work/scratch" ||
    fail "the copy from the single node failed"
s3 "$c1_s3" s3 sync --only-show-errors "$work/scratch" s3://moved/ ||
    fail "the copy to the cluster failed"
for i in 1 2 3; do
    endpoint=http://127.0.0.1:$((E2E_PORT + offsets[i]))
    listed=$(s3 "$endpoint" s3api list-objects-v2 --bucket moved --query 'length(Contents)' --output text)
    [ "$listed" = 5 ] || fail "c$i lists $listed objects of the moved bucket"
    rm -rf "$work/read-c$i"
    s3 "$endpoint" s3 sync --only-show-errors s3://moved/ "$work/read-c$i" ||
        fail "reading the moved bucket through c$i failed"
    diff -r "$work/objects" "$work/read-c$i" >/dev/null || fail "c$i reads other bytes"
done
log "moved a single node's bucket into the cluster in $((SECONDS - moved_at)) s: its directory is refused over etcd and left as it was, and every node lists and reads the 5 objects"
node_stop c-single

for i in 1 2 3; do
    node_stop "c$i"
done
