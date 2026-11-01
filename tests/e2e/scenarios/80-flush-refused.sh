#!/usr/bin/env bash
# A refused flush (plan M8-02, design sections 7.6 and 7.8): cluster ra
# flushes bucket `outbound` to bucket `inbound` of cluster rb over S3 REST,
# signing with an access key rb lists as ra's (s3_access_key_ids). rb's
# `inbound` first lacks `peer_source = "ra"`, so rb refuses ra's writes with
# 403 AccessDenied: the flush stalls, ra counts the refusals
# (skys3_flush_refusals_total) and the keys they hold up
# (skys3_flush_refused_keys), names the last one in the bucket's status,
# and rb counts what it refused (skys3_peer_s3_refusals_total). Once rb is
# restarted with `peer_source`, the keys drain and the gauge returns to 0.
#
# ra has no [peering.peers] table, so it flushes over S3 REST only: no
# descriptor, no QUIC, and no quarantine, so the scenario takes about a
# minute or two, the probe's backoff, and runs beside the others.
# e2e: parallel
set -euo pipefail
. "$E2E_HERE/lib.sh"

work=$E2E_DIR/work/flush-refused
pki=$E2E_DIR/pki
mkdir -p "$work"
ra_s3=http://127.0.0.1:$((E2E_PORT + 50))
ra_admin=127.0.0.1:$((E2E_PORT + 51))
rb_s3=http://127.0.0.1:$((E2E_PORT + 60))
rb_admin=127.0.0.1:$((E2E_PORT + 61))
keys=3

# write: objects obj/1 to obj/$keys of 10 to 30 KB on ra.
write() {
    local i
    for i in $(seq 1 "$keys"); do
        random_file "$work/obj-$i" $((10000 * i))
        peer_aws "$ra_s3" s3api put-object --bucket outbound --key "obj/$i" \
            --body "$work/obj-$i" >/dev/null
    done
}

# arrived: whether rb holds every object, intact.
arrived() {
    local i
    for i in $(seq 1 "$keys"); do
        peer_aws "$rb_s3" s3api get-object --bucket inbound --key "obj/$i" "$work/got" \
            >/dev/null 2>&1 || return 1
        cmp -s "$work/obj-$i" "$work/got" || return 1
    done
}

# at_least <host:port> <series> <n>: whether the series reads n or more.
at_least() {
    local value
    value=$(metric "$1" "$2")
    [ -n "$value" ] && awk -v v="$value" -v n="$3" 'BEGIN { exit !(v >= n) }'
}

refused_keys() { metric "$ra_admin" 'skys3_flush_refused_keys{bucket="outbound"}'; }
refused_keys_are() { [ "$(refused_keys)" = "$1" ]; }
refusals='skys3_flush_refusals_total{bucket="outbound",code="AccessDenied"}'
peer_refusals='skys3_peer_s3_refusals_total{peer="ra",reason="peer_source"}'
refusal() { admin "$ra_admin" /v1/buckets/outbound | jq -c .flush.last_refusal; }

# rb_configure <extra>: rb's configuration, taking ra's flushes from ra's
# key and bucket pair, with <extra> appended.
rb_configure() {
    peer_configure rb rb1 $((E2E_PORT + 60)) 7445 "
[peering.peers.ra]
ca_file = \"$pki/ra-ca.pem\"
buckets = [{ source = \"$source_id\", destination = \"inbound\" }]
s3_access_key_ids = [\"$PEER_FLUSH_KEY\"]

[identity.static_credentials.ra]
access_key_id = \"$PEER_FLUSH_KEY\"
secret_access_key_file = \"$E2E_DIR/rb/peer-secret\"
policy = '{\"Version\": \"2012-10-17\", \"Statement\": {\"Effect\": \"Allow\", \"Action\": \"s3:*\", \"Resource\": [\"arn:aws:s3:::inbound\", \"arn:aws:s3:::inbound/*\"]}}'
$1"
}

# Cluster ra sends; its bucket's ID names the pair in rb's configuration.
peer_configure ra ra1 $((E2E_PORT + 50)) 7446 ""
node_start ra
create_bucket "$ra_s3" "$PEER_ADMIN_KEY" "$PEER_ADMIN_SECRET" outbound "$rb_s3/inbound"
source_id=$(admin "$ra_admin" /v1/buckets/outbound | jq -r .bucket_id)

# rb lists ra as a peer, but its bucket does not name ra as peer_source.
rb_configure ""
node_start rb
create_bucket "$rb_s3" "$PEER_ADMIN_KEY" "$PEER_ADMIN_SECRET" inbound
write
log "$keys objects written to ra"

eventually 90 "ra counts $keys refused keys" refused_keys_are "$keys"
eventually 30 "ra counts AccessDenied refusals" at_least "$ra_admin" "$refusals" 1
eventually 30 "rb counts ra's writes refused for peer_source" \
    at_least "$rb_admin" "$peer_refusals" 1
last=$(refusal)
jq -e --arg target "$rb_s3/inbound" \
    '.code == "AccessDenied" and .target == $target and .at_unix_ms > 0' <<<"$last" \
    >/dev/null || fail "last_refusal: $last"
log "refused: $last"
# The flush stalls: nothing reaches rb, and the keys stay dirty.
! arrived || fail "objects reached rb while it refuses them"
admin "$ra_admin" /v1/buckets/outbound | jq -e --argjson keys "$keys" \
    '.flush.dirty == $keys and .flush.refused_keys == $keys' >/dev/null ||
    fail "outbound: $(admin "$ra_admin" /v1/buckets/outbound | jq -c .flush)"
# Both counters keep rising while the refusal lasts.
before=$(metric "$ra_admin" "$refusals")
peer_before=$(metric "$rb_admin" "$peer_refusals")
eventually 90 "ra's refusals rise" at_least "$ra_admin" "$refusals" $((before + 1))
eventually 30 "rb's refusals rise" at_least "$rb_admin" "$peer_refusals" $((peer_before + 1))
log "refusals: ra $(metric "$ra_admin" "$refusals"), rb $(metric "$rb_admin" "$peer_refusals")"

# The fix: rb's bucket receives from ra. rb reads its configuration at
# startup, so it restarts; ra needs nothing.
node_stop rb
rb_configure "
[buckets.inbound]
peer_source = \"ra\""
node_start rb
log "rb restarted with peer_source = \"ra\""

eventually 120 "the objects reach rb" arrived
eventually 30 "outbound is flushed" flushed "$ra_admin" outbound
eventually 30 "ra's refused keys return to 0" refused_keys_are 0
log "every object reached rb; refused keys: $(refused_keys)"

node_stop ra
node_stop rb
