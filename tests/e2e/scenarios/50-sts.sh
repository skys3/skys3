#!/usr/bin/env bash
# STS AssumeRoleWithWebIdentity (design section 11) against the static OIDC
# issuer run.sh serves over HTTPS, with the provider and the role loaded by
# `skys3 control identity put`: a token whose subject the trust policy
# accepts gets a session that may use buckets named sts-* and nothing
# else, and a token it does not accept, or one from a key the issuer does
# not publish, gets no session.
set -euo pipefail
. "$E2E_HERE/lib.sh"

work=$E2E_DIR/work/sts
mkdir -p "$work"
role=arn:aws:iam::000000000000:role/e2e-writer
pki=$E2E_DIR/pki

token() {
    python3 "$E2E_HERE/oidc.py" token --issuer "$ISSUER_URL" \
        --signing-key "${2:-$pki/oidc-signing.key}" --subject "$1"
}

# assume <token>: the session's credentials as JSON, or a failure.
assume() {
    aws --endpoint-url "$N1_S3" \
        sts assume-role-with-web-identity --role-arn "$role" \
        --role-session-name e2e --duration-seconds 900 --web-identity-token "$1" \
        --no-sign-request
}

assume "$(token repo:skys3/e2e)" >"$work/session.json"
expiration=$(jq -r .Credentials.Expiration "$work/session.json")
[ -n "$expiration" ] && [ "$expiration" != null ] || fail "no session: $(cat "$work/session.json")"
[ "$(jq -r .SubjectFromWebIdentityToken "$work/session.json")" = repo:skys3/e2e ] ||
    fail "the session's subject: $(cat "$work/session.json")"
log "assumed $role until $expiration"

session() {
    AWS_ACCESS_KEY_ID=$(jq -r .Credentials.AccessKeyId "$work/session.json") \
        AWS_SECRET_ACCESS_KEY=$(jq -r .Credentials.SecretAccessKey "$work/session.json") \
        AWS_SESSION_TOKEN=$(jq -r .Credentials.SessionToken "$work/session.json") \
        aws --endpoint-url "$N1_S3" "$@"
}
session s3api create-bucket --bucket sts-e2e >/dev/null
random_file "$work/object" 12345
session s3api put-object --bucket sts-e2e --key object --body "$work/object" >/dev/null
session s3api get-object --bucket sts-e2e --key object "$work/object.out" >/dev/null
same "$work/object" "$work/object.out"
session s3api list-buckets --query 'Buckets[].Name' --output text | grep -qw sts-e2e ||
    fail "the session does not list sts-e2e"
log "the session uses buckets named sts-*"

if session s3api put-object --bucket objects --key from-sts --body "$work/object" \
    2>"$work/denied.err" >/dev/null; then
    fail "the session wrote to bucket objects, which its role does not allow"
fi
grep -q AccessDenied "$work/denied.err" || fail "unexpected error: $(cat "$work/denied.err")"
log "the session is denied other buckets"

if assume "$(token repo:other/e2e)" >"$work/refused.json" 2>"$work/refused.err"; then
    fail "a subject the trust policy does not accept got a session"
fi
grep -q AccessDenied "$work/refused.err" || fail "unexpected error: $(cat "$work/refused.err")"
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$work/stranger.key" 2>/dev/null
if assume "$(token repo:skys3/e2e "$work/stranger.key")" >/dev/null 2>"$work/forged.err"; then
    fail "a token the issuer did not sign got a session"
fi
grep -q InvalidIdentityToken "$work/forged.err" || fail "unexpected error: $(cat "$work/forged.err")"
log "tokens the role does not trust get no session"
