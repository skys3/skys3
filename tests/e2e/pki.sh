#!/usr/bin/env bash
# The end-to-end suite's throwaway PKI (plan M8-01), made with openssl.
#
#   tests/e2e/pki.sh <dir>
#
# Writes into <dir>, each key beside its certificate:
#
#   e2e-ca.pem                  the suite's CA. It signs every server
#                               certificate below; CI installs it into the
#                               system trust store, which the node's OIDC
#                               fetcher reads (design section 11).
#   gateway.pem, gateway.key    the S3 and STS listener of the single-node
#                               cluster, for 127.0.0.1 and localhost
#   issuer.pem, issuer.key      the static OIDC issuer's HTTPS listener
#   oidc-signing.key            the RSA key the issuer signs tokens with
#   <cluster>-ca.pem            one CA per cluster, for the intra-cluster
#                               transport and peering (design section 12)
#   <cluster>-<node>.pem/.key   a node certificate, which names the node
#                               with the URI subject alternative name
#                               spiffe://<cluster>/node/<node>
#   etcd-server.pem/.key,       an etcd server certificate for 127.0.0.1
#   etcd-client.pem/.key        (and the addresses of PKI_ETCD_IPS) and a
#                               client certificate, both signed by
#                               e2e-ca.pem, for the suites that run the
#                               etcd control store
#   c3-foreign.pem/.key         a node certificate naming
#                               spiffe://c3/node/c9, signed by e2e-ca.pem,
#                               a CA the nodes of cluster c3 do not trust
#                               on their transport (the cluster scenario)
#
# The clusters and nodes are those of PKI_NODES, `cluster/node` pairs
# separated by spaces (default: the single-node cluster, the node that
# must refuse a MinIO control store, the two pairs of
# peered clusters, the three nodes of cluster c3 of the suite, the three
# of cluster rc of the replication scenario, the four of cluster cc of
# the coordinator scenario, and the four of cluster ec of the
# erasure-coding scenario). Everything is EC
# P-256 except the token key, and valid for two days. Existing files are
# kept, so a rerun reuses them. PKI_ETCD_IPS, IPv4 addresses separated by
# spaces, adds the addresses of etcd members that do not listen on
# loopback (the nightly kill and partition suite, tests/e2e/chaos).
set -euo pipefail

dir=${1:?usage: pki.sh <dir>}
nodes=${PKI_NODES:-"e2e/n1 minio-control/m1 pa/pa1 pb/pb1 ra/ra1 rb/rb1 c3/c1 c3/c2 c3/c3 rc/r1 rc/r2 rc/r3 cc/cc1 cc/cc2 cc/cc3 cc/cc4 ec/ec1 ec/ec2 ec/ec3 ec/ec4"}
mkdir -p "$dir"
cd "$dir"

days=2

# A new EC key in $1.
key() {
    [ -s "$1" ] || openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1" 2>/dev/null
}

# A self-signed CA: $1 is its file name stem, $2 its common name.
ca() {
    [ -s "$1.pem" ] && return
    key "$1.key"
    openssl req -x509 -new -key "$1.key" -out "$1.pem" -days "$days" -subj "/CN=$2" \
        -addext "basicConstraints=critical,CA:TRUE" \
        -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
}

# A leaf certificate $1.pem signed by the CA $2, with the common name $3,
# the subject alternative names $4, and the extended key usages $5.
leaf() {
    [ -s "$1.pem" ] && return
    key "$1.key"
    openssl req -new -key "$1.key" -subj "/CN=$3" -out "$1.csr" 2>/dev/null
    cat >"$1.ext" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=$5
subjectAltName=$4
EOF
    openssl x509 -req -in "$1.csr" -CA "$2.pem" -CAkey "$2.key" -CAcreateserial \
        -out "$1.pem" -days "$days" -extfile "$1.ext" 2>/dev/null
    rm -f "$1.csr" "$1.ext"
}

ca e2e-ca "SkyS3 e2e CA"
leaf gateway e2e-ca "SkyS3 e2e gateway" "IP:127.0.0.1,DNS:localhost" serverAuth
leaf issuer e2e-ca "SkyS3 e2e OIDC issuer" "IP:127.0.0.1,DNS:localhost" serverAuth
# etcd's JSON gateway, which the cluster scenario writes through, reaches
# etcd's gRPC server with the server certificate, which --client-cert-auth
# then checks as a client's.
etcd_names="IP:127.0.0.1,DNS:localhost"
for ip in ${PKI_ETCD_IPS:-}; do
    etcd_names="$etcd_names,IP:$ip"
done
leaf etcd-server e2e-ca "SkyS3 e2e etcd" "$etcd_names" "serverAuth,clientAuth"
leaf etcd-client e2e-ca "SkyS3 e2e etcd client" "DNS:skys3" clientAuth
leaf c3-foreign e2e-ca c9 "URI:spiffe://c3/node/c9,IP:127.0.0.1,DNS:localhost" \
    "serverAuth,clientAuth"
[ -s oidc-signing.key ] || openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 \
    -out oidc-signing.key 2>/dev/null

for entry in $nodes; do
    cluster=${entry%%/*}
    node=${entry#*/}
    ca "$cluster-ca" "SkyS3 $cluster CA"
    # Node certificates serve and connect, on the transport and the QUIC
    # peer endpoint alike.
    leaf "$cluster-$node" "$cluster-ca" "$node" \
        "URI:spiffe://$cluster/node/$node,IP:127.0.0.1,DNS:localhost" "serverAuth,clientAuth"
done
rm -f ./*.srl
