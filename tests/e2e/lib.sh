# shellcheck shell=bash
# Shared helpers of the end-to-end suite (plan M8-01), sourced by run.sh and
# by every scenario. run.sh exports the environment they read:
#
#   E2E_DIR        the run's directory: PKI, configurations, data, logs
#   E2E_HERE       tests/e2e
#   E2E_BINARY     the skys3 binary
#   E2E_AWS        how to run the AWS CLI: `aws`, or `docker` for its image
#   E2E_PORT       the base of every port the suite listens on
#   E2E_FAULT      a seeded fault, or empty (run.sh)
#   E2E_CONTROL    the single-node cluster's control store: file, etcd,
#                  or s3 (run.sh)
#   MINIO_URL, MINIO_KEY, MINIO_SECRET
#                  the MinIO server every write_back bucket flushes to
#   N1_S3, N1_ADMIN, N1_KEY, N1_SECRET
#                  the single-node cluster's S3 and STS endpoint (HTTPS),
#                  its admin listener (host:port), and a static credential
#                  that may do anything
#   ISSUER_URL     the static OIDC issuer

# The AWS CLI image used when the host has no `aws`: the SDK matrix's
# (tests/sdk/cli/Dockerfile).
E2E_AWS_IMAGE=amazon/aws-cli:2.37.8@sha256:420ab345e847291b541b45d989535f55bcff957c27fa1100fae4aa233e86398c

log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }
fail() {
    printf '%s FAILED: %s\n' "$(date -u +%H:%M:%S)" "$*" >&2
    exit 1
}

# Runs the AWS CLI, from the host or its image. Files it reads or writes
# must be under $E2E_DIR, which the image mounts at the same path.
aws() {
    if [ "$E2E_AWS" = docker ]; then
        local args=(--rm -i --network host --volume "$E2E_DIR:$E2E_DIR" --workdir "$E2E_DIR")
        local name
        for name in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_REGION \
            AWS_CA_BUNDLE AWS_CONFIG_FILE AWS_SHARED_CREDENTIALS_FILE AWS_MAX_ATTEMPTS \
            AWS_EC2_METADATA_DISABLED AWS_PAGER; do
            [ -n "${!name+x}" ] && args+=(--env "$name")
        done
        docker run "${args[@]}" "$E2E_AWS_IMAGE" "$@"
    else
        command aws "$@"
    fi
}

# The AWS CLI against the single-node cluster with its static credential.
n1() {
    AWS_ACCESS_KEY_ID=$N1_KEY AWS_SECRET_ACCESS_KEY=$N1_SECRET \
        aws --endpoint-url "$N1_S3" "$@"
}

# The AWS CLI against MinIO, the remote target, as its root user.
minio() {
    AWS_ACCESS_KEY_ID=$MINIO_KEY AWS_SECRET_ACCESS_KEY=$MINIO_SECRET \
        aws --endpoint-url "$MINIO_URL" "$@"
}

# create_bucket <endpoint> <access-key> <secret> <bucket> [<target-url>]
#
# CreateBucket, signed with SigV4 by curl: a write_back bucket that flushes
# to <target-url> when one is given, and otherwise one of the node's default
# mode. The AWS CLI cannot send the x-skys3-bucket-* headers that choose the
# mode and the target (design section 4.1).
create_bucket() {
    local endpoint=$1 key=$2 secret=$3 bucket=$4 target=${5:-}
    local headers=()
    if [ -n "$target" ]; then
        headers=(-H "x-skys3-bucket-mode: write_back" -H "x-skys3-bucket-target: $target")
    fi
    curl -sS --fail-with-body --max-time 30 --cacert "$E2E_DIR/pki/e2e-ca.pem" \
        --aws-sigv4 "aws:amz:us-east-1:s3" --user "$key:$secret" \
        -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" "${headers[@]}" \
        -X PUT "$endpoint/$bucket" >/dev/null ||
        fail "CreateBucket $bucket at $endpoint"
}

# admin <host:port> <path>: the JSON answer of an admin API GET.
admin() {
    curl -sS --fail-with-body --max-time 10 "http://$1$2"
}

# metric <host:port> <series>: the value of one series of the node's
# /metrics, such as 'skys3_flush_transport{bucket="b",transport="quic"}',
# or nothing if it is not exported.
metric() {
    curl -sS --max-time 10 "http://$1/metrics" |
        awk -v series="$2" '$1 == series { print $2; exit }'
}

# eventually <seconds> <what> <command...>: runs the command every half
# second until it succeeds, or fails the scenario after <seconds>.
eventually() {
    local seconds=$1 what=$2
    shift 2
    local deadline=$((SECONDS + seconds))
    until "$@" >/dev/null 2>&1; do
        if [ "$SECONDS" -ge "$deadline" ]; then
            "$@" || true
            fail "timed out after ${seconds}s waiting until $what"
        fi
        sleep 0.5
    done
}

# flushed <admin> <bucket>: whether every change of the bucket is at its
# target: nothing dirty, being flushed, or held, and no tombstone left.
flushed() {
    admin "$1" "/v1/buckets/$2" |
        jq -e '.unflushed == 0 and .flush.dirty == 0 and .flush.flushing == 0' >/dev/null
}

# same <a> <b>: whether two files have the same bytes.
same() { cmp -s "$1" "$2" || fail "$1 and $2 differ"; }

# random_file <path> <bytes>
random_file() { head -c "$2" /dev/urandom >"$1"; }

# node_start <name>: starts the node configured in $E2E_DIR/<name>, with
# the environment in <name>/env if there is one, and waits until its admin
# API (`admin = "host:port"` in <name>/admin) says it is ready.
node_start() {
    local name=$1 dir=$E2E_DIR/$1
    local addr
    addr=$(cat "$dir/admin")
    (
        set -a
        # shellcheck disable=SC1091 # written by run.sh or the scenario
        [ -f "$dir/env" ] && . "$dir/env"
        set +a
        exec "$E2E_BINARY" --config "$dir/skys3.toml" >>"$E2E_DIR/logs/$name.log" 2>&1
    ) &
    echo $! >"$dir/pid"
    local deadline=$((SECONDS + 60))
    until admin "$addr" /v1/health 2>/dev/null | jq -e .ready >/dev/null 2>&1; do
        if ! kill -0 "$(cat "$dir/pid")" 2>/dev/null; then
            tail -n 30 "$E2E_DIR/logs/$name.log" >&2
            fail "node $name exited during startup"
        fi
        [ "$SECONDS" -lt "$deadline" ] || fail "node $name is not ready after 60s"
        sleep 0.2
    done
    log "node $name is ready (pid $(cat "$dir/pid"))"
}

# node_stop <name>: stops the node gracefully, and fails if it does not
# exit cleanly within 60 seconds.
node_stop() {
    local name=$1 pid
    pid=$(cat "$E2E_DIR/$name/pid" 2>/dev/null) || return 0
    kill -TERM "$pid" 2>/dev/null || return 0
    local deadline=$((SECONDS + 60))
    while kill -0 "$pid" 2>/dev/null; do
        [ "$SECONDS" -lt "$deadline" ] || fail "node $name did not stop within 60s"
        sleep 0.2
    done
    wait "$pid" 2>/dev/null || true
    rm -f "$E2E_DIR/$name/pid"
    log "node $name stopped"
}

# --- Peered clusters ---------------------------------------------------------
#
# The scenarios with two single-node clusters, one flushing to the other
# (peering, flush-refused), share a static credential that may do anything
# on every node, and the access key every node's flushers sign their S3
# requests with, which a destination lists in its peer's s3_access_key_ids.
PEER_ADMIN_KEY=AKIASKYS3E2EPEERADMIN
PEER_ADMIN_SECRET=skys3-e2e-peer-admin-secret-0123456789
PEER_FLUSH_KEY=AKIASKYS3E2EPAPEER
PEER_FLUSH_SECRET=skys3-e2e-pa-peer-secret-0123456789abc

# peer_configure <cluster> <node> <gateway port> <quic port> <extra>: the
# directory $E2E_DIR/<cluster> and the configuration of the one node of
# cluster <cluster>, with <extra> appended to its TOML. Its admin listener
# is on the gateway port plus 1 and its transport on the gateway port plus
# 2; its flushers sign with PEER_FLUSH_KEY, whose secret is also in
# <cluster>/peer-secret for a destination's credential to name.
peer_configure() {
    local cluster=$1 node=$2 port=$3 quic=$4 extra=$5 dir=$E2E_DIR/$1
    local pki=$E2E_DIR/pki
    mkdir -p "$dir"
    printf '%s' "$PEER_ADMIN_SECRET" >"$dir/admin-secret"
    printf '%s' "$PEER_FLUSH_SECRET" >"$dir/peer-secret"
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
access_key_id = "$PEER_ADMIN_KEY"
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
        echo "AWS_ACCESS_KEY_ID=$PEER_FLUSH_KEY"
        echo "AWS_SECRET_ACCESS_KEY=$PEER_FLUSH_SECRET"
        echo "AWS_EC2_METADATA_DISABLED=true"
        echo "AWS_CONFIG_FILE=$E2E_DIR/aws/no-config"
        echo "AWS_SHARED_CREDENTIALS_FILE=$E2E_DIR/aws/no-credentials"
    } >"$dir/env"
}

# peer_aws <endpoint> <args...>: the AWS CLI against a peered cluster's
# gateway, with its static credential.
peer_aws() {
    local endpoint=$1
    shift
    AWS_ACCESS_KEY_ID=$PEER_ADMIN_KEY AWS_SECRET_ACCESS_KEY=$PEER_ADMIN_SECRET \
        aws --endpoint-url "$endpoint" "$@"
}
