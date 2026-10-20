#!/usr/bin/env bash
# The coordinator, handoffs, and the member lifecycle (plan M8-06, design
# section 6.7): nodes cc1 to cc4 of cluster cc, each with `[cluster]
# multi_node = true` and `coordinator = true`, share the etcd control
# store over mutual TLS (under the prefix `cc/`). A local bucket with 4
# shards of 3 members each takes writes and reads throughout, through
# every node that serves, each request retried as an SDK retries it (3
# attempts). The scenario checks:
#
# - that one node is elected coordinator, and every node names it;
# - that a handoff moves a primary, asked through the coordinator's admin
#   API, which chooses the member, and through the primary's, which is
#   told the member; a node that is neither answers 409 naming both; and
#   the coordinator re-admits each old primary as a learner, which the
#   new primary promotes;
# - that a 4th node joins and receives shards: rebalancing moves members
#   onto it until it holds its share;
# - that a drained node is emptied: the coordinator, drained through
#   another node's admin API, moves its shards and primaries away;
# - that killing the coordinator (`kill -9`) delays only placement: reads
#   and writes go on, another node takes the lease, and placement resumes
#   under it: the killed node, restarted and no longer drained, receives
#   shards again;
# - that no request failed beyond those retries, and every acknowledged
#   key reads back through every node.
#
# Replacement removes a member at most every 3 seconds here (10 by
# default) to keep the scenario short. It runs only over etcd (`# e2e:
# etcd`), beside the other scenarios, takes about a minute and a half,
# and uses the ports E2E_PORT+33 to E2E_PORT+38 and E2E_PORT+43 to
# E2E_PORT+48, which the peering scenarios (file store only) leave free.
# e2e: etcd
set -euo pipefail
. "$E2E_HERE/lib.sh"

[ -n "${E2E_ETCD_URL:-}" ] || fail "the coordinator scenario runs with SKYS3_E2E_CONTROL=etcd"
work=$E2E_DIR/work/coordinator
pki=$E2E_DIR/pki
mkdir -p "$work"
key=AKIASKYS3E2ECOORD secret=skys3-e2e-coordinator-secret-0123456789
bucket=coordinated

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
cluster_id = "cc"
multi_node = true
coordinator = true

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
tls_cert_file = "$pki/cc-$node.pem"
tls_key_file = "$pki/cc-$node.key"
tls_ca_file = "$pki/cc-ca.pem"

[replication]
removal_interval_seconds = 3

[storage]
disk_min_free_bytes = 0

[cache]
reserve_fraction = 0.0

[buckets.defaults]
mode = "local"
shards_per_bucket = 4
replicas = 3
min_write_replicas = 2

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
body_of() { printf 'skys3 e2e coordinator %s' "$1"; }
# cluster_of <node>: the node's GET /v1/cluster.
cluster_of() { admin "$(admin_of "$1")" /v1/cluster; }
# shards <node>: the node's GET /v1/shards entries of the bucket.
shards() { admin "$(admin_of "$1")" /v1/shards | jq -c --arg b "$bucket" '[.shards[] | select(.bucket == $b)]'; }
# held <node> <field>: how many shards name <node> as <field> (members,
# learners, or primaries), as the coordinator's view reports it.
held() {
    cluster_of "$(cat "$work/coordinator")" | jq -r --arg n "$1" --arg f "$2" '.nodes[] | select(.node_id == $n) | .[$f]'
}
# api <method> <node> <path> [<body>]: an admin API call with a body,
# whose status is in $work/status and whose JSON answer is printed.
api() {
    curl -sS --max-time 30 -o "$work/answer" -w '%{http_code}' -X "$1" \
        -H 'content-type: application/json' --data "${4:-}" \
        "http://$(admin_of "$2")$3" >"$work/status"
    cat "$work/answer"
}

# put <endpoint> <key> and get <endpoint> <key> <file>: one S3 request,
# retried on 5xx answers and timeouts with backoff, 3 attempts in all, as
# the AWS SDKs do by default. They print the final HTTP status (000 for
# none). Answers go to a file: curl cannot retry into /dev/null or a pipe,
# which it cannot truncate, and gives up after the first attempt.
put() {
    curl -sS -o "$work/put" -w '%{http_code}' --max-time 10 --retry 2 \
        --aws-sigv4 "aws:amz:us-east-1:s3" --user "$key:$secret" \
        -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" --data-binary "$(body_of "$2")" \
        -X PUT "$1/$bucket/$2" 2>/dev/null || true
}
get() {
    curl -sS -o "$3" -w '%{http_code}' --max-time 10 --retry 2 \
        --aws-sigv4 "aws:amz:us-east-1:s3" --user "$key:$secret" \
        -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" "$1/$bucket/$2" 2>/dev/null || true
}

# --- Election -----------------------------------------------------------------

offsets=([1]=33 [2]=36 [3]=43 [4]=46)
for i in 1 2 3 4; do
    configure "cc$i" "${offsets[$i]}"
done
for i in 1 2 3; do
    node_start "cc$i"
done
# elected <nodes...>: whether exactly one of the nodes says it is
# coordinator, and every one names it as the lease holder; it is then in
# $work/coordinator.
elected() {
    local node holders=() serving=() view
    for node in "$@"; do
        view=$(cluster_of "$node") || return 1
        holders+=("$(jq -r '.coordinator.holder' <<<"$view")")
        [ "$(jq -r '.coordinator.this_node' <<<"$view")" = true ] && serving+=("$node")
    done
    [ ${#serving[@]} -eq 1 ] || return 1
    for holder in "${holders[@]}"; do
        [ "$holder" = "${serving[0]}" ] || return 1
    done
    echo "${serving[0]}" >"$work/coordinator"
}
started=$SECONDS
eventually 60 "one of cc1, cc2, and cc3 is elected coordinator" elected cc1 cc2 cc3
coordinator=$(cat "$work/coordinator")
log "$coordinator is coordinator after $((SECONDS - started)) s"

create_bucket "$(s3_of cc1)" "$key" "$secret" "$bucket"
# settled <nodes...>: whether the registers name 12 members (3 per shard)
# and no learner, as the coordinator counts them, and each node knows every
# shard with 3 members and no learner, and holds a replica, as a member, of
# each that names it. A replica a move took from its node stops, and keeps
# the configuration it last knew, so a stopped one is left out.
settled() {
    local node
    cluster_of "$(cat "$work/coordinator")" |
        jq -e '([.nodes[].members] | add) == 12 and ([.nodes[].learners] | add) == 0' >/dev/null ||
        return 1
    for node in "$@"; do
        shards "$node" | jq -e --arg n "$node" 'length == 4 and all(.[] | select(.role != "stopped");
            (.members | length) == 3 and (.learners | length) == 0 and
            ((.members | index($n)) == null or (.source == "replica" and .role != "learner")))' \
            >/dev/null || return 1
    done
}
eventually 30 "cc1, cc2, and cc3 hold every shard" settled cc1 cc2 cc3
log "bucket $bucket: primaries $(shards cc1 | jq -c '[.[] | {(.shard | tostring): .primary}] | add')"

# --- Writes and reads throughout --------------------------------------------
#
# The writer PUTs keys w00000, w00001, ... and the reader GETs the latest
# acknowledged key, each through the nodes in $work/live in turn, and
# record `<key> <status> <start> <end> <node>` per request. They stop when
# the file `stop` appears.
echo "cc1 cc2 cc3" >"$work/live"
writer() {
    local n=0 name status start node live
    while [ ! -e "$work/stop" ]; do
        read -r -a live <"$work/live"
        node=${live[$((n % ${#live[@]}))]}
        name=$(printf 'w%05d' "$n")
        start=$EPOCHREALTIME
        status=$(put "$(s3_of "$node")" "$name")
        echo "$name $status $start $EPOCHREALTIME $node" >>"$work/writes"
        n=$((n + 1))
    done
}
reader() {
    local n=0 name status start node live
    while [ ! -e "$work/stop" ]; do
        read -r -a live <"$work/live"
        node=${live[$((n % ${#live[@]}))]}
        name=$(awk '$2 == 200 { name = $1 } END { print name }' "$work/writes")
        if [ -z "$name" ]; then
            sleep 0.1
            continue
        fi
        start=$EPOCHREALTIME
        status=$(get "$(s3_of "$node")" "$name" "$work/read")
        if [ "$status" = 200 ] && [ "$(cat "$work/read")" != "$(body_of "$name")" ]; then
            status=bytes
        fi
        echo "$name $status $start $EPOCHREALTIME $node" >>"$work/reads"
        n=$((n + 1))
    done
}
: >"$work/writes"
: >"$work/reads"
writer &
writer_pid=$!
reader &
reader_pid=$!
# A scenario that fails stops its loops too.
trap 'kill "$writer_pid" "$reader_pid" 2>/dev/null || true' EXIT
acked() { awk '$2 == 200' "$work/writes" | wc -l; }
some() { [ "$(acked)" -ge "$1" ]; }
eventually 60 "the writer has 50 acknowledged writes" some 50
# failures: the requests of both loops that failed, after retries.
failures() { cat "$work/writes" "$work/reads" | awk '$2 != 200' | wc -l; }
# moved_on <since>: whether both loops finished a request they started
# after <since>, so neither still uses a node $work/live dropped before.
moved_on() {
    awk -v t="$1" '$3 > t { found = 1 } END { exit !found }' "$work/writes" &&
        awk -v t="$1" '$3 > t { found = 1 } END { exit !found }' "$work/reads"
}

# --- Handoffs -----------------------------------------------------------------

primary_of() { shards cc1 | jq -r --argjson s "$1" '.[] | select(.shard == $s) | .primary'; }
epoch_of() { shards cc1 | jq -r --argjson s "$1" '.[] | select(.shard == $s) | .epoch'; }
handoff_path() { echo "/v1/shards/$bucket/$1/handoff"; }

# Shard 0 through the coordinator, which chooses the member; a node that
# neither leads the shard nor coordinates refuses, naming both.
primary=$(primary_of 0)
epoch=$(epoch_of 0)
for node in cc1 cc2 cc3; do
    if [ "$node" != "$primary" ] && [ "$node" != "$coordinator" ]; then
        answer=$(api POST "$node" "$(handoff_path 0)")
        [ "$(cat "$work/status")" = 409 ] || fail "$node answered $(cat "$work/status"): $answer"
        jq -e --arg p "$primary" --arg c "$coordinator" '.primary == $p and .coordinator == $c' \
            <<<"$answer" >/dev/null || fail "$node's refusal names the wrong nodes: $answer"
        log "$node, neither primary nor coordinator, refuses the handoff: $(jq -r .error <<<"$answer")"
        break
    fi
done
handed_at=$EPOCHREALTIME
answer=$(api POST "$coordinator" "$(handoff_path 0)")
[ "$(cat "$work/status")" = 202 ] || fail "the handoff through $coordinator answered $(cat "$work/status"): $answer"
to=$(jq -r .to <<<"$answer")
log "shard 0: asked through $coordinator, $primary hands off to $to (via the $(jq -r .via <<<"$answer"))"
moved() { [ "$(primary_of "$1")" = "$2" ] && [ "$(epoch_of "$1")" -gt "$3" ]; }
eventually 30 "shard 0's primary is $to" moved 0 "$to" "$epoch"
log "shard 0 moved to $to in $(awk -v t="$handed_at" -v n="$EPOCHREALTIME" 'BEGIN { printf "%.1f", n - t }') s"

# Shard 1 through its primary, told the member.
primary=$(primary_of 1)
epoch=$(epoch_of 1)
to=$(shards cc1 | jq -r --argjson s 1 --arg p "$primary" '.[] | select(.shard == $s) | .members[] | select(. != $p)' | head -n1)
answer=$(api POST "$primary" "$(handoff_path 1)" "{\"to\": \"$to\"}")
[ "$(cat "$work/status")" = 202 ] || fail "the handoff through $primary answered $(cat "$work/status"): $answer"
jq -e '.via == "primary"' <<<"$answer" >/dev/null || fail "the primary did not start it: $answer"
eventually 30 "shard 1's primary is $to" moved 1 "$to" "$epoch"
log "shard 1: $primary handed off to $to"
# Each old primary left its shard; the coordinator adds it back as a
# learner, the only node it can, and the new primary promotes it.
eventually 60 "every shard has 3 members again" settled cc1 cc2 cc3
[ "$(failures)" -eq 0 ] || fail "requests failed during the handoffs: $(cat "$work/writes" "$work/reads" | awk '$2 != 200' | head -n 5)"
log "the old primaries are members again; no request failed ($(acked) writes acknowledged)"

# --- A fourth node joins ------------------------------------------------------

node_start cc4
echo "cc1 cc2 cc3 cc4" >"$work/live"
started=$SECONDS
share() { [ "$(held cc4 members)" -ge "$1" ] && settled cc1 cc2 cc3 cc4; }
eventually 180 "cc4 holds its share of the shards" share 3
log "cc4 joined and received $(held cc4 members) shards in $((SECONDS - started)) s: $(cluster_of cc4 | jq -c '[.nodes[] | {(.node_id): .members}] | add')"

# --- The coordinator is drained, then killed ----------------------------------

others=()
for node in cc1 cc2 cc3 cc4; do
    [ "$node" = "$coordinator" ] || others+=("$node")
done
answer=$(api POST "${others[0]}" "/v1/nodes/$coordinator/drain")
[ "$(cat "$work/status")" = 200 ] || fail "draining $coordinator answered $(cat "$work/status"): $answer"
started=$SECONDS
empty() {
    [ "$(held "$coordinator" members)" = 0 ] && [ "$(held "$coordinator" learners)" = 0 ] &&
        [ "$(held "$coordinator" primaries)" = 0 ]
}
eventually 180 "$coordinator holds no shard" empty
log "drained $coordinator through ${others[0]} in $((SECONDS - started)) s"

echo "${others[*]}" >"$work/live"
dropped_at=$EPOCHREALTIME
eventually 60 "the clients no longer use $coordinator" moved_on "$dropped_at"
killed_at=$EPOCHREALTIME
kill -KILL "$(cat "$E2E_DIR/$coordinator/pid")"
wait "$(cat "$E2E_DIR/$coordinator/pid")" 2>/dev/null || true
rm -f "$E2E_DIR/$coordinator/pid"
log "killed the coordinator, $coordinator (kill -9)"
eventually 60 "another node is elected coordinator" elected "${others[@]}"
successor=$(cat "$work/coordinator")
log "$successor is coordinator $(awk -v t="$killed_at" -v n="$EPOCHREALTIME" 'BEGIN { printf "%.1f", n - t }') s after the kill"
# Meanwhile, every request succeeded, none slower than 5 s.
more() { awk -v t="$killed_at" '$3 > t' "$work/writes" | grep -q .; }
eventually 30 "the writer goes on" more
[ "$(failures)" -eq 0 ] || fail "requests failed: $(cat "$work/writes" "$work/reads" | awk '$2 != 200' | head -n 5)"
slowest=$(cat "$work/writes" "$work/reads" |
    awk -v t="$killed_at" '$4 > t { d = $4 - $3; if (d > m) m = d } END { printf "%.2f", m }')
awk -v s="$slowest" 'BEGIN { exit !(s < 5) }' || fail "a request took $slowest s after the kill"
log "reads and writes went on: the slowest request after the kill took $slowest s"

# Placement resumes under the successor: the killed node, back and no
# longer drained, receives shards again.
node_start "$coordinator"
answer=$(api DELETE "$successor" "/v1/nodes/$coordinator/drain")
[ "$(cat "$work/status")" = 200 ] || fail "undraining $coordinator answered $(cat "$work/status"): $answer"
echo "cc1 cc2 cc3 cc4" >"$work/live"
started=$SECONDS
back() { [ "$(held "$coordinator" members)" -ge 1 ]; }
eventually 180 "$coordinator receives shards from $successor" back
eventually 120 "every shard is settled" settled cc1 cc2 cc3 cc4
log "$coordinator received $(held "$coordinator" members) shards from $successor in $((SECONDS - started)) s"

# --- Every request, and every acknowledged key --------------------------------

touch "$work/stop"
wait "$writer_pid" "$reader_pid"
[ "$(failures)" -eq 0 ] || fail "requests failed: $(cat "$work/writes" "$work/reads" | awk '$2 != 200' | head -n 5)"
# Every acknowledged key, listed and read through each node by the AWS
# CLI (with its default retries), with its bytes.
for node in cc1 cc2 cc3 cc4; do
    AWS_ACCESS_KEY_ID=$key AWS_SECRET_ACCESS_KEY=$secret aws --endpoint-url "$(s3_of "$node")" \
        s3 cp --recursive --only-show-errors "s3://$bucket" "$work/read-$node" ||
        fail "the bucket does not copy through $node"
    missing=$(python3 - "$work/writes" "$work/read-$node" <<'EOF'
import os, sys
missing = []
for line in open(sys.argv[1]):
    name, status = line.split()[:2]
    if status != "200":
        continue
    path = os.path.join(sys.argv[2], name)
    body = f"skys3 e2e coordinator {name}".encode()
    if not os.path.exists(path) or open(path, "rb").read() != body:
        missing.append(name)
print(" ".join(missing[:5]), len(missing) if missing else "")
EOF
)
    [ -z "${missing// /}" ] || fail "acknowledged keys do not read back through $node: $missing"
done
log "no request failed beyond retries: $(acked) writes and $(wc -l <"$work/reads") reads; every acknowledged key reads back through every node"

for node in cc1 cc2 cc3 cc4; do
    node_stop "$node"
done
