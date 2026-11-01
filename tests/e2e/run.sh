#!/usr/bin/env bash
# The end-to-end suite (plan M8-01, design sections 16.2 and 17): the real
# node binary against MinIO, a static OIDC issuer, the AWS CLI, and a
# second cluster it peers with, one scenario per script in scenarios/.
#
#   tests/e2e/run.sh [<scenario>...]
#
# Runs every scenario by default, or those named (`objects`, `flush`, ...:
# the script names without their number and `.sh`), in order. With an etcd
# or S3 control store (SKYS3_E2E_CONTROL), the default is the scenarios of
# the single-node cluster: those that run clusters of their own (`# e2e:
# parallel`) run them over the file store whatever the setting. A scenario
# marked `# e2e: etcd` (the three-node clusters) needs the etcd store, and
# runs, beside the others, only with SKYS3_E2E_CONTROL=etcd. It prints a
# summary and exits non-zero if any failed. Everything a run makes, the
# PKI, configurations, data directories, and logs, is in SKYS3_E2E_DIR
# (default target/e2e-run), which CI uploads when the job fails.
#
# What it needs: bash, curl, jq, openssl, python3, and the AWS CLI v2 (or
# docker, to run its image), and for the peering scenario, root (or sudo)
# with iptables. The environment can name:
#
#   SKYS3_E2E_BINARY  the skys3 binary; by default `cargo build --profile
#                     e2e -p skys3` builds it
#   SKYS3_E2E_MINIO   the MinIO server binary; by default `minio` on the
#                     PATH, or the one tests/e2e/minio.sh builds from
#                     source and caches
#   SKYS3_E2E_CONTROL the single-node cluster's control store (plan M8-03):
#                     `file` (the default); `etcd`, one etcd member over
#                     mutual TLS with the PKI's etcd certificates; or `s3`,
#                     a bucket of a second SkyS3 cluster (MinIO fails the
#                     startup probe, which this setting also checks)
#   SKYS3_E2E_ETCD    the etcd server binary for `etcd`; by default the
#                     pinned release tests/e2e/etcd.sh downloads and caches
#   SKYS3_E2E_DIR     where the run's files go; emptied first
#   SKYS3_E2E_PORT    the first of the ports the suite listens on (19000);
#                     it uses 100 from there, and UDP 7443 to 7445
#   SKYS3_E2E_PKI     a directory tests/e2e/pki.sh wrote, to use instead
#                     of a new PKI in SKYS3_E2E_DIR (CI makes it first, to
#                     install its CA)
#   SKYS3_E2E_SYSTEM_TRUST
#                     1 when the suite's CA is installed in the system
#                     trust store (CI does it); otherwise the nodes get it
#                     through SSL_CERT_FILE
#   SKYS3_E2E_SCENARIO_TIMEOUT
#                     how long a scenario may run, in seconds (600); the
#                     perf scenario's full profile needs hours
#   SKYS3_E2E_FAULT   a seeded fault, to show that the suite catches it:
#                     `flusher-off` gives the node a wrong secret for
#                     MinIO, so nothing flushes (or imports); `udp-blocked`
#                     never unblocks UDP in the peering scenario
set -euo pipefail

E2E_HERE=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$E2E_HERE/../.." && pwd)
export E2E_HERE
export E2E_DIR=${SKYS3_E2E_DIR:-$repo/target/e2e-run}
export E2E_PORT=${SKYS3_E2E_PORT:-19000}
export E2E_FAULT=${SKYS3_E2E_FAULT:-}
export E2E_CONTROL=${SKYS3_E2E_CONTROL:-file}
# shellcheck source=tests/e2e/lib.sh
. "$E2E_HERE/lib.sh"
case "$E2E_CONTROL" in
file | etcd | s3) ;;
*) fail "SKYS3_E2E_CONTROL is file, etcd, or s3, not $E2E_CONTROL" ;;
esac

# Which scenarios to run.
scenarios=()
for script in "$E2E_HERE"/scenarios/*.sh; do
    name=$(basename "$script" .sh)
    name=${name#[0-9][0-9]-}
    if grep -q '^# e2e: etcd$' "$script"; then
        if [ "$E2E_CONTROL" = etcd ] && { [ $# -eq 0 ] || [[ " $* " == *" $name "* ]]; }; then
            scenarios+=("$script")
        elif [[ " $* " == *" $name "* ]]; then
            fail "scenario $name needs SKYS3_E2E_CONTROL=etcd"
        fi
    elif [ $# -eq 0 ]; then
        if [ "$E2E_CONTROL" = file ] || ! grep -q '^# e2e: parallel$' "$script"; then
            scenarios+=("$script")
        fi
    else
        for wanted in "$@"; do
            [ "$wanted" = "$name" ] && scenarios+=("$script")
        done
    fi
done
[ ${#scenarios[@]} -gt 0 ] || fail "no scenario matches: $*"

for tool in curl jq openssl python3; do
    command -v "$tool" >/dev/null || fail "the suite needs $tool"
done
if type -P aws >/dev/null; then
    export E2E_AWS=aws
elif command -v docker >/dev/null; then
    export E2E_AWS=docker
    docker image inspect "$E2E_AWS_IMAGE" >/dev/null 2>&1 || docker pull -q "$E2E_AWS_IMAGE"
else
    fail "the suite needs the AWS CLI, or docker to run its image"
fi

rm -rf "$E2E_DIR"
mkdir -p "$E2E_DIR/logs" "$E2E_DIR/aws" "$E2E_DIR/work"

# --- Binaries ---------------------------------------------------------------

if [ -n "${SKYS3_E2E_BINARY:-}" ]; then
    E2E_BINARY=$SKYS3_E2E_BINARY
else
    log "building skys3 (profile e2e)"
    (cd "$repo" && cargo build --quiet --profile e2e -p skys3)
    E2E_BINARY=${CARGO_TARGET_DIR:-$repo/target}/e2e/skys3
fi
E2E_BINARY=$(cd "$(dirname "$E2E_BINARY")" && pwd)/$(basename "$E2E_BINARY")
export E2E_BINARY
[ -x "$E2E_BINARY" ] || fail "no skys3 binary at $E2E_BINARY"

minio_binary=$(bash "$E2E_HERE/minio.sh")

# --- Cleanup ----------------------------------------------------------------

pids=()
cleanup() {
    local status=$?
    for name in "$E2E_DIR"/*/pid; do
        [ -f "$name" ] && kill -TERM "$(cat "$name")" 2>/dev/null || true
    done
    for pid in "${pids[@]}"; do
        kill -TERM "$pid" 2>/dev/null || true
    done
    # The peering scenario's UDP block, if it failed while the block held.
    if [ -f "$E2E_DIR/udp-blocked" ]; then
        bash "$E2E_HERE/udp.sh" unblock "$(cat "$E2E_DIR/udp-blocked")" || true
    fi
    wait 2>/dev/null || true
    exit "$status"
}
trap cleanup EXIT INT TERM

# --- PKI, MinIO, and the issuer ---------------------------------------------

pki=$E2E_DIR/pki
if [ -n "${SKYS3_E2E_PKI:-}" ]; then
    cp -R "$SKYS3_E2E_PKI" "$pki"
else
    log "generating the PKI"
fi
# Fills in whatever the given directory lacks.
bash "$E2E_HERE/pki.sh" "$pki"
if [ "${SKYS3_E2E_SYSTEM_TRUST:-0}" != 1 ]; then
    # The nodes' OIDC fetcher reads the system roots, which SSL_CERT_FILE
    # replaces: the system bundle plus the suite's CA.
    system=/etc/ssl/certs/ca-certificates.crt
    { [ -f "$system" ] && cat "$system"; cat "$pki/e2e-ca.pem"; } >"$pki/trust.pem"
fi

cat >"$E2E_DIR/aws/config" <<EOF
[default]
region = us-east-1
s3 =
    addressing_style = path
    multipart_threshold = 8MB
    multipart_chunksize = 8MB
    max_concurrent_requests = 32
EOF
export AWS_CONFIG_FILE=$E2E_DIR/aws/config
export AWS_SHARED_CREDENTIALS_FILE=$E2E_DIR/aws/no-credentials
export AWS_EC2_METADATA_DISABLED=true AWS_PAGER="" AWS_MAX_ATTEMPTS=3
export AWS_CA_BUNDLE=$pki/e2e-ca.pem

export MINIO_URL=http://127.0.0.1:$((E2E_PORT + 10))
export MINIO_KEY=skys3e2eminio MINIO_SECRET=skys3-e2e-minio-secret-0123456789
mkdir -p "$E2E_DIR/minio"
MINIO_ROOT_USER=$MINIO_KEY MINIO_ROOT_PASSWORD=$MINIO_SECRET MINIO_BROWSER=off \
    "$minio_binary" server --quiet --address "127.0.0.1:$((E2E_PORT + 10))" \
    "$E2E_DIR/minio" >"$E2E_DIR/logs/minio.log" 2>&1 &
pids+=($!)

export ISSUER_URL=https://127.0.0.1:$((E2E_PORT + 20))
python3 "$E2E_HERE/oidc.py" serve --port $((E2E_PORT + 20)) --cert "$pki/issuer.pem" \
    --key "$pki/issuer.key" --signing-key "$pki/oidc-signing.key" \
    >"$E2E_DIR/logs/oidc.log" 2>&1 &
pids+=($!)

eventually 60 "MinIO is ready" curl -fsS --max-time 2 "$MINIO_URL/minio/health/ready"
eventually 30 "the OIDC issuer answers" curl -fsS --max-time 2 --cacert "$pki/e2e-ca.pem" \
    "$ISSUER_URL/.well-known/openid-configuration"
log "MinIO and the OIDC issuer are up"

# --- The single-node cluster's control store --------------------------------
#
# Its [control_store] table. etcd serves its clients over mutual TLS; the
# member's peer URL is plaintext on loopback, as it has no peers.
#
# The S3 store cannot be MinIO: it ignores If-Match on DeleteObject, so a
# conditional delete at a stale version is applied, and the startup probe
# (design section 6.1) refuses it, which the suite checks. The store is a
# bucket of a second single-node SkyS3 cluster, `ctl`, over the file store,
# which the node reaches with a profile of its own, never the flushers'
# credential.
mkdir -p "$E2E_DIR/n1"
case "$E2E_CONTROL" in
file)
    control_store='backend = "file"'
    ;;
etcd)
    etcd_binary=$(bash "$E2E_HERE/etcd.sh")
    etcd_url=https://127.0.0.1:$((E2E_PORT + 70))
    etcd_peer=http://127.0.0.1:$((E2E_PORT + 71))
    "$etcd_binary" --name e2e --data-dir "$E2E_DIR/etcd" \
        --listen-client-urls "$etcd_url" --advertise-client-urls "$etcd_url" \
        --listen-peer-urls "$etcd_peer" --initial-advertise-peer-urls "$etcd_peer" \
        --initial-cluster "e2e=$etcd_peer" \
        --cert-file "$pki/etcd-server.pem" --key-file "$pki/etcd-server.key" \
        --client-cert-auth --trusted-ca-file "$pki/e2e-ca.pem" \
        >"$E2E_DIR/logs/etcd.log" 2>&1 &
    pids+=($!)
    eventually 60 "etcd is healthy" curl -fsS --max-time 2 --cacert "$pki/e2e-ca.pem" \
        --cert "$pki/etcd-client.pem" --key "$pki/etcd-client.key" "$etcd_url/health"
    # Without a client certificate, etcd refuses the TLS handshake.
    if curl -fsS --max-time 2 --cacert "$pki/e2e-ca.pem" "$etcd_url/health" >/dev/null 2>&1; then
        fail "etcd answered a client without a certificate"
    fi
    log "etcd is up at $etcd_url (mutual TLS)"
    # For the cluster scenario, whose nodes share the store.
    export E2E_ETCD_URL=$etcd_url
    control_store="backend = \"etcd\"
etcd_endpoints = [\"$etcd_url\"]
etcd_tls_ca_file = \"$pki/e2e-ca.pem\"
etcd_tls_cert_file = \"$pki/etcd-client.pem\"
etcd_tls_key_file = \"$pki/etcd-client.key\""
    ;;
s3)
    ctl_s3=http://127.0.0.1:$((E2E_PORT + 80))
    ctl_key=AKIASKYS3E2ECONTROL ctl_secret=skys3-e2e-control-secret-0123456789
    cat >"$E2E_DIR/n1/aws-credentials" <<EOF
[skys3-control]
aws_access_key_id = $ctl_key
aws_secret_access_key = $ctl_secret

[minio-control]
aws_access_key_id = $MINIO_KEY
aws_secret_access_key = $MINIO_SECRET
EOF
    # A node whose control store is a MinIO bucket does not start.
    minio s3api create-bucket --bucket minio-control >/dev/null
    mkdir -p "$E2E_DIR/minio-control"
    cat >"$E2E_DIR/minio-control/skys3.toml" <<EOF
[cluster]
cluster_id = "minio-control"

[node]
node_id = "m1"
data_dir = "$E2E_DIR/minio-control/data"

[gateway]
listen = "127.0.0.1:$((E2E_PORT + 85))"

[control_store]
backend = "s3"
endpoint = "$MINIO_URL"
bucket = "minio-control"
profile = "minio-control"

[transport]
listen = "127.0.0.1:$((E2E_PORT + 87))"
tls_cert_file = "$pki/minio-control-m1.pem"
tls_key_file = "$pki/minio-control-m1.key"
tls_ca_file = "$pki/minio-control-ca.pem"

[storage]
disk_min_free_bytes = 0

[admin]
listen = "127.0.0.1:$((E2E_PORT + 86))"
EOF
    if AWS_CONFIG_FILE=$E2E_DIR/aws/no-config AWS_SHARED_CREDENTIALS_FILE=$E2E_DIR/n1/aws-credentials \
        AWS_EC2_METADATA_DISABLED=true timeout 120 "$E2E_BINARY" \
        --config "$E2E_DIR/minio-control/skys3.toml" >"$E2E_DIR/logs/minio-control.log" 2>&1; then
        fail "a node started over a MinIO control store"
    fi
    grep -q 'failed the startup probe' "$E2E_DIR/logs/minio-control.log" ||
        fail "a node over a MinIO control store failed otherwise: $(tail -n 5 "$E2E_DIR/logs/minio-control.log")"
    log "a node refuses a MinIO control store: $(grep -o 'control-store probe failed.*' "$E2E_DIR/logs/minio-control.log")"

    # The cluster that serves the control bucket.
    mkdir -p "$E2E_DIR/ctl"
    printf '%s' "$ctl_secret" >"$E2E_DIR/ctl/secret"
    echo "127.0.0.1:$((E2E_PORT + 81))" >"$E2E_DIR/ctl/admin"
    cat >"$E2E_DIR/ctl/skys3.toml" <<EOF
[cluster]
cluster_id = "ctl"

[node]
node_id = "ctl1"
data_dir = "$E2E_DIR/ctl/data"

[gateway]
listen = "127.0.0.1:$((E2E_PORT + 80))"

[control_store]
backend = "file"

[storage]
disk_min_free_bytes = 0

[buckets.defaults]
mode = "local"
shards_per_bucket = 1
replicas = 1
min_write_replicas = 1

[identity.static_credentials.control]
access_key_id = "$ctl_key"
secret_access_key_file = "$E2E_DIR/ctl/secret"
policy = '{"Version": "2012-10-17", "Statement": {"Effect": "Allow", "Action": "*", "Resource": "*"}}'

[admin]
listen = "127.0.0.1:$((E2E_PORT + 81))"
EOF
    node_start ctl
    create_bucket "$ctl_s3" "$ctl_key" "$ctl_secret" skys3-control
    log "the control store is the bucket skys3-control of the cluster ctl"
    # MinIO and ctl share 127.0.0.1, one failure scope, which a write_back
    # bucket's target must not share with an S3 control store unless the
    # configuration allows it.
    control_store="backend = \"s3\"
endpoint = \"$ctl_s3\"
bucket = \"skys3-control\"
profile = \"skys3-control\"
allow_correlated_control_store = true"
    ;;
esac
# Over etcd or S3, n1 is the only node of a cluster (plan M8-09): it binds
# the intra-cluster transport, registers, holds the coordinator lease, and
# serves its buckets as replicated shards of one member. Over the file
# store it is a single node, which binds no transport.
transport=""
if [ "$E2E_CONTROL" != file ]; then
    transport="[transport]
listen = \"127.0.0.1:$((E2E_PORT + 2))\"
tls_cert_file = \"$pki/e2e-n1.pem\"
tls_key_file = \"$pki/e2e-n1.key\"
tls_ca_file = \"$pki/e2e-ca.pem\""
fi

# --- The single-node cluster ------------------------------------------------

export N1_S3=https://127.0.0.1:$E2E_PORT
export N1_ADMIN=127.0.0.1:$((E2E_PORT + 1))
export N1_KEY=AKIASKYS3E2EADMIN N1_SECRET=skys3-e2e-admin-secret-0123456789abc
printf '%s' "$N1_SECRET" >"$E2E_DIR/n1/secret"
echo "$N1_ADMIN" >"$E2E_DIR/n1/admin"
cat >"$E2E_DIR/n1/skys3.toml" <<EOF
[cluster]
cluster_id = "e2e"

[node]
node_id = "n1"
data_dir = "$E2E_DIR/n1/data"

[gateway]
listen = "127.0.0.1:$E2E_PORT"
tls_cert_file = "$pki/gateway.pem"
tls_key_file = "$pki/gateway.key"

[control_store]
$control_store
config_poll_interval_seconds = 1

$transport

[storage]
index_checkpoint_interval_seconds = 2
# The suite's disk may be small; admission control is not under test.
disk_min_free_bytes = 0

[cache]
# Keep clean payload whatever the free space of the suite's disk, so that
# what is read locally and what is filled does not depend on the machine.
reserve_fraction = 0.0

[buckets.defaults]
mode = "local"
shards_per_bucket = 4
replicas = 1
min_write_replicas = 1

[buckets.reconciled]
mode = "write_back"
reconcile_interval_seconds = 2

[identity]
sts_web_identity = true
session_default_seconds = 900

[identity.static_credentials.admin]
access_key_id = "$N1_KEY"
secret_access_key_file = "$E2E_DIR/n1/secret"
policy = '{"Version": "2012-10-17", "Statement": {"Effect": "Allow", "Action": "*", "Resource": "*"}}'

[admin]
listen = "$N1_ADMIN"

[logging]
filter = "info"
EOF
minio_secret=$MINIO_SECRET
[ "$E2E_FAULT" = flusher-off ] && minio_secret=not-the-minio-secret
{
    echo "AWS_ACCESS_KEY_ID=$MINIO_KEY"
    echo "AWS_SECRET_ACCESS_KEY=$minio_secret"
    echo "AWS_EC2_METADATA_DISABLED=true"
    echo "AWS_CONFIG_FILE=$E2E_DIR/aws/no-config"
    if [ "$E2E_CONTROL" = s3 ]; then
        echo "AWS_SHARED_CREDENTIALS_FILE=$E2E_DIR/n1/aws-credentials"
    else
        echo "AWS_SHARED_CREDENTIALS_FILE=$E2E_DIR/aws/no-credentials"
    fi
    [ -f "$pki/trust.pem" ] && echo "SSL_CERT_FILE=$pki/trust.pem"
} >"$E2E_DIR/n1/env"

# The identity registers, loaded by `skys3 control identity put` with the
# node's configuration and environment. The first start creates the
# control store's registers. The file store is the node's alone, so the
# command runs while the node is stopped; an etcd or S3 store takes it
# while the node runs, and the node follows the generation it announces.
cat >"$E2E_DIR/n1/identity.json" <<EOF
{
  "providers": {
    "e2e": {"issuer": "$ISSUER_URL", "audiences": ["sts.amazonaws.com"]}
  },
  "roles": {
    "e2e-writer": {
      "trust_policy": {
        "Version": "2012-10-17",
        "Statement": {
          "Effect": "Allow",
          "Principal": {"Federated": "$ISSUER_URL"},
          "Action": "sts:AssumeRoleWithWebIdentity",
          "Condition": {
            "StringEquals": {"${ISSUER_URL#https://}:aud": "sts.amazonaws.com"},
            "StringLike": {"${ISSUER_URL#https://}:sub": "repo:skys3/*"}
          }
        }
      },
      "policies": [{
        "Version": "2012-10-17",
        "Statement": [
          {"Effect": "Allow", "Action": "s3:*",
           "Resource": ["arn:aws:s3:::sts-*", "arn:aws:s3:::sts-*/*"]},
          {"Effect": "Allow", "Action": "s3:ListAllMyBuckets", "Resource": "*"}
        ]
      }]
    }
  }
}
EOF
identity_put() {
    (
        set -a
        # shellcheck disable=SC1091 # written above
        . "$E2E_DIR/n1/env"
        set +a
        exec "$E2E_BINARY" control identity put --config "$E2E_DIR/n1/skys3.toml" \
            --file "$E2E_DIR/n1/identity.json"
    ) | tee "$E2E_DIR/logs/identity-put.log"
    grep -q '^created: identity/roles/e2e-writer.json$' "$E2E_DIR/logs/identity-put.log" ||
        fail "control identity put did not create the role"
}
# at_generation <n>: whether n1's copy is at generation <n> or later.
at_generation() {
    admin "$N1_ADMIN" /v1/health | jq -e --argjson n "$1" '.control_store.generation >= $n'
}
node_start n1
if [ "$E2E_CONTROL" = file ]; then
    node_stop n1
    identity_put
    node_start n1
else
    identity_put
    generation=$(sed -n 's/^announced at generation \([0-9]*\);.*/\1/p' "$E2E_DIR/logs/identity-put.log")
    [ -n "$generation" ] || fail "control identity put announced no generation"
    eventually 30 "n1 follows generation $generation" at_generation "$generation"
    log "n1 follows the identity registers at generation $generation"
fi

# --- Scenarios --------------------------------------------------------------
#
# A scenario whose header has the line `# e2e: parallel` or `# e2e: etcd`
# runs beside the others, which run one after another: the peering
# scenario spends most of its time waiting out the quarantines of
# transport switches (design section 7.8), and neither it nor the cluster
# scenario shares anything with the single-node cluster.

mkdir -p "$E2E_DIR/results"

# run_scenario <script>: runs it with a time limit, records its result in
# results/<name>, and keeps its output in logs/scenario-<name>.log.
run_scenario() {
    local script=$1 name started status=failed
    name=$(basename "$script" .sh)
    name=${name#[0-9][0-9]-}
    started=$SECONDS
    if timeout "${SKYS3_E2E_SCENARIO_TIMEOUT:-600}" bash "$script" >"$E2E_DIR/logs/scenario-$name.log" 2>&1; then
        status=passed
    fi
    echo "$status $name ($((SECONDS - started))s)" >"$E2E_DIR/results/$name"
}

# report <name>: prints a finished scenario's log as a group.
report() {
    echo "::group::scenario $1: $(cut -d' ' -f1 "$E2E_DIR/results/$1")"
    cat "$E2E_DIR/logs/scenario-$1.log"
    echo "::endgroup::"
    grep -q '^passed' "$E2E_DIR/results/$1" ||
        echo "::error::scenario $1 failed; its log: logs/scenario-$1.log"
}

# in_background <script>: whether the scenario runs beside the others.
in_background() { grep -q '^# e2e: \(parallel\|etcd\)$' "$1"; }

parallel=()
for script in "${scenarios[@]}"; do
    if in_background "$script"; then
        log "starting scenario $(basename "$script" .sh) in the background"
        run_scenario "$script" &
        parallel+=("$!:$script")
    fi
done
for script in "${scenarios[@]}"; do
    in_background "$script" && continue
    run_scenario "$script"
    name=$(basename "$script" .sh)
    report "${name#[0-9][0-9]-}"
done
for entry in "${parallel[@]}"; do
    wait "${entry%%:*}" || true
    name=$(basename "${entry#*:}" .sh)
    report "${name#[0-9][0-9]-}"
done

echo "passed: $(grep -h '^passed' "$E2E_DIR"/results/* | cut -d' ' -f2- | paste -sd, - || true)"
echo "failed: $(grep -h '^failed' "$E2E_DIR"/results/* | cut -d' ' -f2- | paste -sd, - || true)"
! grep -q '^failed' "$E2E_DIR"/results/*
