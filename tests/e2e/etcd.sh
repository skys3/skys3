#!/usr/bin/env bash
# Prints the path of the etcd server binary the end-to-end suite runs with
# SKYS3_E2E_CONTROL=etcd (plan M8-03), downloading it first if needed.
#
#   tests/e2e/etcd.sh
#
# SKYS3_E2E_ETCD names a binary to use as is; otherwise the one unpacked
# into SKYS3_E2E_CACHE (default ~/.cache/skys3-e2e) from the etcd release
# ETCD_VERSION on GitHub, whose SHA-256 is checked first: the version the
# CI job "etcd integration" runs as an image. The release is fetched rather
# than the image so that the suite starts etcd as a process, with the
# PKI's certificates, beside MinIO and the nodes.
set -euo pipefail

ETCD_VERSION=v3.6.10
ETCD_SHA256_LINUX_AMD64=ed579fafab5701e3aaa95509969e7bc74776a4ae5269d32e3928408b406456ec

if [ -n "${SKYS3_E2E_ETCD:-}" ]; then
    echo "$SKYS3_E2E_ETCD"
    exit 0
fi
case "$(uname -s)/$(uname -m)" in
Linux/x86_64) ;;
*)
    echo "no pinned etcd release for $(uname -s)/$(uname -m); set SKYS3_E2E_ETCD" >&2
    exit 1
    ;;
esac
cache=${SKYS3_E2E_CACHE:-$HOME/.cache/skys3-e2e}
binary=$cache/etcd-$ETCD_VERSION
if [ ! -x "$binary" ]; then
    echo "downloading etcd $ETCD_VERSION into $cache" >&2
    mkdir -p "$cache"
    work=$(mktemp -d "$cache/etcd.XXXXXX")
    name=etcd-$ETCD_VERSION-linux-amd64
    curl -fsSL --retry 3 --max-time 300 -o "$work/$name.tar.gz" \
        "https://github.com/etcd-io/etcd/releases/download/$ETCD_VERSION/$name.tar.gz"
    echo "$ETCD_SHA256_LINUX_AMD64  $work/$name.tar.gz" | sha256sum -c --quiet - >&2
    tar -xzf "$work/$name.tar.gz" -C "$work" "$name/etcd"
    mv "$work/$name/etcd" "$binary"
    rm -rf "$work"
fi
echo "$binary"
