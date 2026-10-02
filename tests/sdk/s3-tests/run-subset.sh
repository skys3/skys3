#!/bin/sh
# Runs the selected ceph/s3-tests subset (subset.txt) against the SDK
# matrix's node (plan M1-25; M7-01 completes the selection).
#
#   run-subset.sh <s3-tests checkout>
#
# It runs against a matrix started by `cargo test -p skys3 --test sdk`,
# which sets the environment tests/sdk/run.sh describes. S3TESTS_PYTHON
# names the interpreter (default python3), which needs requirements.txt.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
checkout=${1:?usage: run-subset.sh <s3-tests checkout>}
python=${S3TESTS_PYTHON:-python3}

# s3-tests reads its users and endpoint from a configuration file. The
# main user is the static credential; the alternate user and the others
# s3-tests insists on are the second one. The tests that would tell them
# apart (ACLs, bucket policies, IAM) are not in the subset.
hostport=${SKYS3_ENDPOINT#https://}
conf="$SKYS3_WORK_DIR/s3tests.conf"
other() {
    cat <<EOF
display_name = $1
user_id = $1
email = $1@example.com
access_key = $SKYS3_ALT_ACCESS_KEY_ID
secret_key = $SKYS3_ALT_SECRET_ACCESS_KEY
EOF
}
{
    cat <<EOF
[DEFAULT]
host = ${hostport%:*}
port = ${hostport##*:}
is_secure = True
ssl_verify = False

[fixtures]
bucket prefix = s3tests-{random}-

[s3 main]
display_name = main
user_id = main
email = main@example.com
access_key = $SKYS3_ACCESS_KEY_ID
secret_key = $SKYS3_SECRET_ACCESS_KEY

[s3 alt]
EOF
    other alt
    printf '\n[s3 tenant]\ntenant = tenant\n'
    other tenant
    printf '\n[iam]\n'
    other iam
    printf '\n[iam root]\n'
    other root
    printf '\n[iam alt root]\n'
    other altroot
} >"$conf"

# The node IDs of the subset: subset.txt without comments and blank lines.
tests=$(sed -e 's/#.*//' -e '/^[[:space:]]*$/d' "$here/subset.txt")
cd "$checkout"
# The test's environment names the web identity and its endpoints; s3-tests
# configures its own clients, so none of that may leak into them.
unset AWS_ROLE_ARN AWS_WEB_IDENTITY_TOKEN_FILE AWS_ROLE_SESSION_NAME AWS_ENDPOINT_URL_S3 AWS_ENDPOINT_URL_STS
# shellcheck disable=SC2086 # one argument per test
S3TEST_CONF=$conf PYTHONPATH="$here${PYTHONPATH:+:$PYTHONPATH}" AWS_DEFAULT_REGION=us-east-1 \
    exec "$python" -m pytest -p skys3_s3tests -p no:cacheprovider -p no:warnings -q -rfE $tests
