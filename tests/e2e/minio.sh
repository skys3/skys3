#!/usr/bin/env bash
# Prints the path of the MinIO server binary the end-to-end suite runs
# (plan M8-01), building it first if needed.
#
#   tests/e2e/minio.sh
#
# SKYS3_E2E_MINIO names a binary to use as is; otherwise `minio` on the
# PATH is used, or the one built into SKYS3_E2E_CACHE (default
# ~/.cache/skys3-e2e) with `go install` at MINIO_VERSION. MinIO no longer
# publishes images or binaries of its community edition, so the suite
# builds the last commit the Go module proxy serves; CI caches the result.
set -euo pipefail

MINIO_VERSION=v0.0.0-20260212201848-7aac2a2c5b7c

if [ -n "${SKYS3_E2E_MINIO:-}" ]; then
    echo "$SKYS3_E2E_MINIO"
    exit 0
fi
if type -P minio; then
    exit 0
fi
cache=${SKYS3_E2E_CACHE:-$HOME/.cache/skys3-e2e}
binary=$cache/minio-$MINIO_VERSION
if [ ! -x "$binary" ]; then
    echo "building MinIO $MINIO_VERSION into $cache" >&2
    mkdir -p "$cache"
    gopath=$(mktemp -d "$cache/go.XXXXXX")
    GOPATH=$gopath GOCACHE=$gopath/cache GOFLAGS=-modcacherw go install "github.com/minio/minio@$MINIO_VERSION" >&2
    mv "$gopath/bin/minio" "$binary"
    rm -rf "$gopath"
fi
echo "$binary"
