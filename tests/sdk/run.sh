#!/bin/sh
# The SDK matrix's external clients (plan M1-25, design section 16.2).
#
#   tests/sdk/run.sh build [<client>...]   build the client images (all
#                                          by default)
#   tests/sdk/run.sh client <client>       run one client
#
# Clients: python (boto3), go (AWS SDK for Go v2), javascript (AWS SDK for
# JavaScript v3), java (AWS SDK for Java 2.x), cli (AWS CLI v2), and
# s3-tests (the ceph/s3-tests subset in s3-tests/subset.txt). The AWS SDK
# for Rust runs in the test itself.
#
# `cargo test -p skys3 --test sdk` starts a node serving HTTPS, with STS
# on the same listener and an OIDC issuer it trusts, and runs `client` for
# each client SKYS3_SDK_CLIENTS names, all at once, with this environment
# and no other AWS_* variable:
#
#   SKYS3_ENDPOINT          https://127.0.0.1:<port>, S3 and STS
#   SKYS3_CA_FILE           the CA that signed the node's certificate
#   SKYS3_ACCESS_KEY_ID     a static credential that may do anything,
#   SKYS3_SECRET_ACCESS_KEY   and its secret
#   SKYS3_ALT_ACCESS_KEY_ID, SKYS3_ALT_SECRET_ACCESS_KEY
#                           a second one, for s3-tests' other users
#   SKYS3_BUCKET            the client's bucket name, sdk-<client>; the
#                           role may use buckets named sdk-* only
#   SKYS3_SESSION_SECONDS   the lifetime of STS sessions (900)
#   SKYS3_REFRESH_SECONDS   how soon after its issue a client should
#                           refresh a session, where its SDK can be told
#   SKYS3_LOAD_SECONDS      how long to send requests while refreshing
#   SKYS3_MATRIX_DIR        the matrix's temporary directory, which holds
#                           every path here
#   SKYS3_WORK_DIR          a scratch directory for this client
#   AWS_REGION, AWS_ENDPOINT_URL_S3, AWS_ENDPOINT_URL_STS, AWS_ROLE_ARN,
#   AWS_ROLE_SESSION_NAME, AWS_WEB_IDENTITY_TOKEN_FILE, AWS_CA_BUNDLE,
#   AWS_CONFIG_FILE, AWS_SHARED_CREDENTIALS_FILE (both missing files),
#   AWS_EC2_METADATA_DISABLED, NODE_EXTRA_CA_CERTS
#                           what a workload's default credential chain
#                           reads; the test rewrites the token file with a
#                           fresh token every few seconds
#
# A client runs in its image, skys3-sdk-<client>, with the host's network
# and the matrix directory mounted at its own path. With SKYS3_SDK_LOCAL=1
# it runs on the host instead, which needs its toolchain and dependencies:
# python: SKYS3_SDK_PYTHON (default python3) with python/requirements.txt;
# go: go; javascript: node, after `npm ci` in javascript/; java: a JDK and
# SKYS3_SDK_MVN (default mvn); cli: aws and curl; s3-tests: S3TESTS_DIR, a
# checkout of ceph/s3-tests at the commit in s3-tests/Dockerfile, and
# SKYS3_SDK_PYTHON with s3-tests/requirements.txt.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
all="python go javascript java cli s3-tests"

usage() {
    echo "usage: $0 build [<client>...] | client <client>" >&2
    exit 2
}

known() {
    for name in $all; do
        [ "$name" = "$1" ] && return 0
    done
    echo "unknown client: $1 (known: $all)" >&2
    exit 2
}

# Runs a client on the host, as its image's entrypoint does.
run_local() {
    python=${SKYS3_SDK_PYTHON:-python3}
    case "$1" in
    python) exec "$python" "$here/python/sdk_test.py" ;;
    go) cd "$here/go" && exec go run . ;;
    javascript) exec node "$here/javascript/sdk-test.mjs" ;;
    java)
        cd "$here/java"
        "${SKYS3_SDK_MVN:-mvn}" -q -B package
        exec java -jar target/sdk-matrix.jar
        ;;
    cli) exec sh "$here/cli/sdk-test.sh" ;;
    s3-tests)
        S3TESTS_PYTHON=$python exec sh "$here/s3-tests/run-subset.sh" \
            "${S3TESTS_DIR:?S3TESTS_DIR must name a ceph/s3-tests checkout}"
        ;;
    esac
}

# Runs a client in its image.
run_image() {
    set -- --rm --network host --user "$(id -u):$(id -g)" --env HOME=/tmp \
        --volume "$SKYS3_MATRIX_DIR:$SKYS3_MATRIX_DIR" "skys3-sdk-$1"
    for name in $(env | sed -n 's/^\(SKYS3_[A-Z0-9_]*\|AWS_[A-Z0-9_]*\|NODE_EXTRA_CA_CERTS\)=.*/\1/p'); do
        set -- --env "$name" "$@"
    done
    exec docker run "$@"
}

[ $# -ge 1 ] || usage
command=$1
shift
case "$command" in
build)
    [ $# -gt 0 ] || set -- $all
    pids=
    for client in "$@"; do
        known "$client"
        docker build --quiet --tag "skys3-sdk-$client" "$here/$client" \
            >"$here/.build-$client.log" 2>&1 &
        pids="$pids $!:$client"
    done
    failed=
    for entry in $pids; do
        if ! wait "${entry%%:*}"; then
            failed="$failed ${entry#*:}"
        fi
    done
    for client in "$@"; do
        echo "::group::image skys3-sdk-$client"
        cat "$here/.build-$client.log"
        rm -f "$here/.build-$client.log"
        echo "::endgroup::"
    done
    if [ -n "$failed" ]; then
        echo "failed to build:$failed" >&2
        exit 1
    fi
    ;;
client)
    [ $# -eq 1 ] || usage
    known "$1"
    : "${SKYS3_MATRIX_DIR:?run clients through cargo test -p skys3 --test sdk}"
    if [ "${SKYS3_SDK_LOCAL:-0}" = 1 ]; then
        run_local "$1"
    else
        run_image "$1"
    fi
    ;;
*) usage ;;
esac
