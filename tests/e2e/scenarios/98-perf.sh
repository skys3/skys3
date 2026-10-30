#!/usr/bin/env bash
# The performance harness end to end (plan M7-06a, design section 16.3):
# nodes pf1, pf2, and pf3 of cluster pf share the etcd control store over
# mutual TLS, buckets have 4 shards of 3 replicas, and write_back buckets
# flush to MinIO. The scenario writes the harness's target file for them,
# with kill and start hooks (tests/e2e/perf/e2e-hooks.sh), runs
# tests/e2e/perf/perf.py with the `smoke` profile, and checks that the
# harness ran end to end: the result validates against its schema, every
# scenario ran (PUT, GET, failover, flush, import), each case has
# samples, and the values derived from the nodes' metrics, such as fsyncs
# per acknowledged PUT, are there and above 0. It checks no performance
# threshold: CI's machines are noisy, and only the release hardware's
# numbers mean anything (docs/skys3-performance.md).
#
# The same scenario launches the cluster for a longer run:
# SKYS3_PERF_PROFILE=full (or smoke) picks the profile, and
# SKYS3_PERF_SET, `name=value` pairs separated by spaces, overrides
# parameters. The result and its summary stay in work/perf/out.
#
# It runs only over etcd (`# e2e: etcd`), beside the other cluster
# scenarios, takes about a minute with the smoke profile, and uses the
# ports E2E_PORT+62 to E2E_PORT+67 and E2E_PORT+72 to E2E_PORT+74.
# e2e: etcd
set -euo pipefail
. "$E2E_HERE/lib.sh"

[ -n "${E2E_ETCD_URL:-}" ] || fail "the perf scenario runs with SKYS3_E2E_CONTROL=etcd"
work=$E2E_DIR/work/perf
pki=$E2E_DIR/pki
mkdir -p "$work"
key=AKIASKYS3E2EPERF secret=skys3-e2e-perf-secret-0123456789abcd
profile=${SKYS3_PERF_PROFILE:-smoke}

# configure <node> <port offset>: the directory $E2E_DIR/<node> and the
# configuration of node <node>, its gateway at E2E_PORT+<offset>, its
# admin listener one port up and its transport two.
configure() {
    local node=$1 offset=$2 dir=$E2E_DIR/$1
    mkdir -p "$dir"
    printf '%s' "$secret" >"$dir/secret"
    echo "127.0.0.1:$((E2E_PORT + offset + 1))" >"$dir/admin"
    cat >"$dir/skys3.toml" <<EOF
[cluster]
cluster_id = "pf"

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
tls_cert_file = "$pki/pf-$node.pem"
tls_key_file = "$pki/pf-$node.key"
tls_ca_file = "$pki/pf-ca.pem"

[storage]
disk_min_free_bytes = 0

[cache]
reserve_fraction = 0.0

[buckets.defaults]
mode = "local"
shards_per_bucket = 4
replicas = 3
min_write_replicas = 2

# The GET scenario's buckets, one per clean_copies it reads with.
[buckets.perfcc1]
clean_copies = 1

[buckets.perfcc3]
clean_copies = 3

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

nodes=(pf1 pf2 pf3)
offsets=([1]=62 [2]=65 [3]=72)
for i in 1 2 3; do
    configure "pf$i" "${offsets[$i]}"
    node_start "pf$i"
done

entries=()
for i in 1 2 3; do
    port=$((E2E_PORT + offsets[i]))
    entries+=("{\"name\": \"pf$i\", \"s3\": \"http://127.0.0.1:$port\", \"admin\": \"127.0.0.1:$((port + 1))\"}")
done
hooks=$E2E_HERE/perf/e2e-hooks.sh
cat >"$work/target.json" <<EOF
{
  "name": "e2e-pf",
  "nodes": [$(IFS=,; echo "${entries[*]}")],
  "s3": {"access_key": "$key", "secret_key": "$secret"},
  "remote": {"url": "$MINIO_URL", "access_key": "$MINIO_KEY", "secret_key": "$MINIO_SECRET"},
  "read_buckets": {"1": "perfcc1", "3": "perfcc3"},
  "cluster": {"replicas": 3, "shards_per_bucket": 4, "control_store": "etcd",
              "replica_ack_timeout_mode": "wait_through", "build": "e2e profile (unoptimized)"},
  "hooks": {"kill": ["bash", "$hooks", "kill", "{node}"],
            "start": ["bash", "$hooks", "start", "{node}"]},
  "links": [],
  "data_dir": "$E2E_DIR/pf1/data"
}
EOF

overrides=()
for pair in ${SKYS3_PERF_SET:-}; do
    overrides+=(--set "$pair")
done
started=$SECONDS
status=0
python3 "$E2E_HERE/perf/perf.py" run --target "$work/target.json" --profile "$profile" \
    --out "$work/out" "${overrides[@]}" || status=$?
log "the harness ran the $profile profile in $((SECONDS - started)) s (exit $status)"
python3 "$E2E_HERE/perf/perf.py" check "$work/out/result.json" --require all ||
    fail "the harness did not run end to end"
[ "$status" = 0 ] || fail "a scenario of the harness failed"
log "fsyncs per acknowledged PUT: $(jq -c '[.scenarios[] | select(.name == "put") | .cases[] |
    {size: .labels.size, concurrency: .labels.concurrency, fsyncs: .values.fsyncs_per_put}]' \
    "$work/out/result.json")"

for node in "${nodes[@]}"; do
    node_stop "$node"
done
