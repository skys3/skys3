#!/usr/bin/env bash
# The end-to-end suite (plan M8-01, design sections 16.2 and 17): the real
# node binary against MinIO, a static OIDC issuer, the AWS CLI, and a
# second cluster it peers with, one scenario per script in scenarios/.
#
#   tests/e2e/run.sh [<scenario>...]
#
# Runs every scenario by default, or those named (`objects`, `flush`, ...:
# the script names without their number and `.sh`), in order. It prints a
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
# shellcheck source=tests/e2e/lib.sh
. "$E2E_HERE/lib.sh"

# Which scenarios to run.
scenarios=()
for script in "$E2E_HERE"/scenarios/*.sh; do
    name=$(basename "$script" .sh)
    name=${name#[0-9][0-9]-}
    if [ $# -eq 0 ]; then
        scenarios+=("$script")
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

# --- The single-node cluster ------------------------------------------------

export N1_S3=https://127.0.0.1:$E2E_PORT
export N1_ADMIN=127.0.0.1:$((E2E_PORT + 1))
export N1_KEY=AKIASKYS3E2EADMIN N1_SECRET=skys3-e2e-admin-secret-0123456789abc
mkdir -p "$E2E_DIR/n1"
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
backend = "file"
config_poll_interval_seconds = 1

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
    echo "AWS_SHARED_CREDENTIALS_FILE=$E2E_DIR/aws/no-credentials"
    [ -f "$pki/trust.pem" ] && echo "SSL_CERT_FILE=$pki/trust.pem"
} >"$E2E_DIR/n1/env"

# The identity registers: the first start creates the file control store,
# and `skys3 control identity put` loads the issuer and the role into it
# while the node is stopped (the file store is the node's alone).
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
node_start n1
node_stop n1
"$E2E_BINARY" control identity put --config "$E2E_DIR/n1/skys3.toml" \
    --file "$E2E_DIR/n1/identity.json" | tee "$E2E_DIR/logs/identity-put.log"
grep -q '^created: identity/roles/e2e-writer.json$' "$E2E_DIR/logs/identity-put.log" ||
    fail "control identity put did not create the role"
node_start n1

# --- Scenarios --------------------------------------------------------------
#
# A scenario whose header has the line `# e2e: parallel` runs beside the
# others, which run one after another: the peering scenario spends most of
# its time waiting out the quarantines of transport switches (design
# section 7.8), and shares nothing with the single-node cluster.

mkdir -p "$E2E_DIR/results"

# run_scenario <script>: runs it with a time limit, records its result in
# results/<name>, and keeps its output in logs/scenario-<name>.log.
run_scenario() {
    local script=$1 name started status=failed
    name=$(basename "$script" .sh)
    name=${name#[0-9][0-9]-}
    started=$SECONDS
    if timeout 600 bash "$script" >"$E2E_DIR/logs/scenario-$name.log" 2>&1; then
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

parallel=()
for script in "${scenarios[@]}"; do
    if grep -q '^# e2e: parallel$' "$script"; then
        log "starting scenario $(basename "$script" .sh) in the background"
        run_scenario "$script" &
        parallel+=("$!:$script")
    fi
done
for script in "${scenarios[@]}"; do
    grep -q '^# e2e: parallel$' "$script" && continue
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
