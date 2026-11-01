#!/usr/bin/env bash
# Native replication between two single-node clusters (design section 7.8):
# cluster pa flushes bucket `outbound` to bucket `inbound` of cluster pb
# over QUIC. With UDP to pb's QUIC port dropped, pa's flushers fall back to
# S3 REST against pb's gateway (`skys3_flush_transport{transport="s3"}`
# reads 1) and keep flushing, and once UDP passes again they are back on
# QUIC within 60 seconds. Every object written reaches pb intact.
#
# Each switch restarts pa's flushers, which wait out a quarantine of 90
# seconds before they flush on the new transport, and so does their first
# start (design section 7.8): the scenario takes about five minutes, and
# runs beside the others.
# e2e: parallel
set -euo pipefail
. "$E2E_HERE/lib.sh"

work=$E2E_DIR/work/peering
pki=$E2E_DIR/pki
mkdir -p "$work"
quic_port=7443
pa_s3=http://127.0.0.1:$((E2E_PORT + 30))
pa_admin=127.0.0.1:$((E2E_PORT + 31))
pb_s3=http://127.0.0.1:$((E2E_PORT + 40))
admin_key=AKIASKYS3E2EPEERADMIN
admin_secret=skys3-e2e-peer-admin-secret-0123456789
# The key pa's flushers sign S3 requests to pb with (s3_access_key_ids).
peer_key=AKIASKYS3E2EPAPEER
peer_secret=skys3-e2e-pa-peer-secret-0123456789abc

# configure <cluster> <node> <gateway port> <quic port> <extra>: a node's
# directory and configuration.
configure() {
    local cluster=$1 node=$2 port=$3 quic=$4 extra=$5 dir=$E2E_DIR/$1
    mkdir -p "$dir"
    printf '%s' "$admin_secret" >"$dir/admin-secret"
    printf '%s' "$peer_secret" >"$dir/peer-secret"
    echo "127.0.0.1:$((port + 1))" >"$dir/admin"
    cat >"$dir/skys3.toml" <<EOF
[cluster]
cluster_id = "$cluster"

[node]
node_id = "$node"
data_dir = "$dir/data"

[gateway]
listen = "127.0.0.1:$port"

[control_store]
backend = "file"
config_poll_interval_seconds = 1

[transport]
listen = "127.0.0.1:$((port + 2))"
tls_cert_file = "$pki/$cluster-$node.pem"
tls_key_file = "$pki/$cluster-$node.key"
tls_ca_file = "$pki/$cluster-ca.pem"

[storage]
disk_min_free_bytes = 0

[buckets.defaults]
mode = "local"
shards_per_bucket = 2
replicas = 1
min_write_replicas = 1

[identity]
sts_web_identity = false

[identity.static_credentials.admin]
access_key_id = "$admin_key"
secret_access_key_file = "$dir/admin-secret"
policy = '{"Version": "2012-10-17", "Statement": {"Effect": "Allow", "Action": "*", "Resource": "*"}}'

[admin]
listen = "127.0.0.1:$((port + 1))"

[logging]
filter = "info"

[peering]
quic_listen = "127.0.0.1:$quic"
peer_connect_timeout_ms = 2000
$extra
EOF
    {
        echo "AWS_ACCESS_KEY_ID=$peer_key"
        echo "AWS_SECRET_ACCESS_KEY=$peer_secret"
        echo "AWS_EC2_METADATA_DISABLED=true"
        echo "AWS_CONFIG_FILE=$E2E_DIR/aws/no-config"
        echo "AWS_SHARED_CREDENTIALS_FILE=$E2E_DIR/aws/no-credentials"
    } >"$dir/env"
}

at() {
    local endpoint=$1
    shift
    AWS_ACCESS_KEY_ID=$admin_key AWS_SECRET_ACCESS_KEY=$admin_secret \
        aws --endpoint-url "$endpoint" "$@"
}

transport_is() {
    [ "$(metric "$pa_admin" "skys3_flush_transport{bucket=\"outbound\",transport=\"$1\"}")" = 1 ]
}

# write <first> <last>: objects obj/<n> of 100 KB to 1 MB on pa.
write() {
    local i
    for i in $(seq "$1" "$2"); do
        random_file "$work/obj-$i" $((100000 * i))
        at "$pa_s3" s3api put-object --bucket outbound --key "obj/$i" \
            --body "$work/obj-$i" >/dev/null
    done
}

# arrived <first> <last>: whether pb holds those objects, intact.
arrived() {
    local i
    for i in $(seq "$1" "$2"); do
        at "$pb_s3" s3api get-object --bucket inbound --key "obj/$i" "$work/got" \
            >/dev/null 2>&1 || return 1
        cmp -s "$work/obj-$i" "$work/got" || return 1
    done
}

transport() { admin "$pa_admin" /v1/buckets/outbound | jq -c .flush.transport; }

# Cluster pa sends; its bucket's ID names the pair in pb's configuration.
configure pa pa1 $((E2E_PORT + 30)) 7444 "
[peering.peers.pb]
ca_file = \"$pki/pb-ca.pem\""
node_start pa
create_bucket "$pa_s3" "$admin_key" "$admin_secret" outbound "$pb_s3/inbound"
source_id=$(admin "$pa_admin" /v1/buckets/outbound | jq -r .bucket_id)

configure pb pb1 $((E2E_PORT + 40)) $quic_port "quic_advertise = [\"127.0.0.1:$quic_port\"]

[peering.peers.pa]
ca_file = \"$pki/pa-ca.pem\"
buckets = [{ source = \"$source_id\", destination = \"inbound\" }]
s3_access_key_ids = [\"$peer_key\"]

[buckets.inbound]
peer_source = \"pa\"

[identity.static_credentials.pa]
access_key_id = \"$peer_key\"
secret_access_key_file = \"$E2E_DIR/pb/peer-secret\"
policy = '{\"Version\": \"2012-10-17\", \"Statement\": {\"Effect\": \"Allow\", \"Action\": \"s3:*\", \"Resource\": [\"arn:aws:s3:::inbound\", \"arn:aws:s3:::inbound/*\"]}}'"
node_start pb
create_bucket "$pb_s3" "$admin_key" "$admin_secret" inbound
# pa's discovery found no peer while pb was down; a restart looks again.
node_stop pa
node_start pa

eventually 60 "pa flushes outbound over QUIC" transport_is quic
log "transport: $(transport)"
write 1 4
eventually 150 "objects 1 to 4 reach pb over QUIC" arrived 1 4
transport_is quic || fail "the transport changed: $(transport)"
log "objects 1 to 4 replicated over QUIC"

echo "$quic_port" >"$E2E_DIR/udp-blocked"
bash "$E2E_HERE/udp.sh" block "$quic_port"
log "UDP to port $quic_port is dropped"
write 5 7
eventually 90 "pa falls back to S3 REST" transport_is s3
log "transport: $(transport)"
eventually 150 "objects 5 to 7 reach pb over S3 REST" arrived 5 7
log "objects 5 to 7 replicated over S3 REST"

if [ "$E2E_FAULT" != udp-blocked ]; then
    bash "$E2E_HERE/udp.sh" unblock "$quic_port"
    rm -f "$E2E_DIR/udp-blocked"
    log "UDP passes again"
fi
eventually 60 "pa is back on QUIC" transport_is quic
log "transport: $(transport)"
write 8 9
eventually 150 "objects 8 and 9 reach pb over QUIC" arrived 8 9
eventually 30 "outbound is flushed" flushed "$pa_admin" outbound
switches=$(admin "$pa_admin" /v1/buckets/outbound | jq .flush.transport.switches)
[ "$switches" -ge 2 ] || fail "the transport switched $switches times"
count=$(at "$pb_s3" s3api list-objects-v2 --bucket inbound --prefix obj/ --query 'length(Contents)')
[ "$count" = 9 ] || fail "pb holds $count objects, not 9"
log "every object reached pb; the transport switched $switches times"

node_stop pa
node_stop pb
