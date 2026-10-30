#!/bin/sh
# Runs the selected ceph/s3-tests subset (subset.txt; excluded.txt says
# why every other test is left out) against the SDK matrix's node (plans
# M1-25 and M7-01).
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
# apart (ACLs, bucket policies, IAM) are not in the subset. The lifecycle
# tests wait three times `lc_debug_interval` seconds for an expiration;
# the node applies lifecycle rules every second.
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
lc_debug_interval = 2

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

cd "$checkout"
# The test's environment names the web identity and its endpoints; s3-tests
# configures its own clients, so none of that may leak into them.
unset AWS_ROLE_ARN AWS_WEB_IDENTITY_TOKEN_FILE AWS_ROLE_SESSION_NAME AWS_ENDPOINT_URL_S3 AWS_ENDPOINT_URL_STS
# Every test is collected and classified, and the subset runs
# (skys3_s3tests.py). The results per group go to the report, which the
# SDK matrix compares with docs/skys3-s3-compatibility.md. With
# SKYS3_S3TESTS_ALL=1 every test runs, to classify a new revision; most
# of them then fail, so their tracebacks are left out, which also saves
# pytest from parsing the 20,000 lines of test_s3.py for each.
traceback=short
if [ "${SKYS3_S3TESTS_ALL:-0}" = 1 ]; then
    traceback=no
fi
S3TEST_CONF=$conf PYTHONPATH="$here${PYTHONPATH:+:$PYTHONPATH}" AWS_DEFAULT_REGION=us-east-1 \
    SKYS3_S3TESTS_LISTS="$here" SKYS3_S3TESTS_REPORT="$SKYS3_WORK_DIR/s3-tests.md" \
    exec "$python" -m pytest -p skys3_s3tests -p no:cacheprovider -p no:warnings -q -rfE \
    --tb="$traceback" s3tests/functional
