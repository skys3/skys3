#!/bin/sh
# Checks the Prometheus alerting rules (deploy/prometheus/skys3-alerts.yml)
# and runs their unit tests (skys3-alerts.test.yml) with promtool, taken
# from a pinned Prometheus release whose checksum is verified. CI's e2e job
# runs it; it works on any linux-amd64 host with curl.
#
#   scripts/ci/promtool.sh
#
# PROMTOOL_DIR is where the release is unpacked (default: a directory under
# RUNNER_TEMP or TMPDIR); a promtool already there is reused.
set -eu

version=3.5.0
sha256=e811827af26d822afb09a4f28314f61b618b12cff5369835a67f674d8b46f39a
release=prometheus-$version.linux-amd64

here=$(cd "$(dirname "$0")/../.." && pwd)
dir=${PROMTOOL_DIR:-${RUNNER_TEMP:-${TMPDIR:-/tmp}}/skys3-promtool-$version}
promtool=$dir/promtool

if [ ! -x "$promtool" ]; then
    mkdir -p "$dir"
    curl -sSfL --retry 3 -o "$dir/$release.tar.gz" \
        "https://github.com/prometheus/prometheus/releases/download/v$version/$release.tar.gz"
    echo "$sha256  $dir/$release.tar.gz" | sha256sum -c --quiet
    tar -xzf "$dir/$release.tar.gz" -C "$dir" --strip-components=1 "$release/promtool"
    rm "$dir/$release.tar.gz"
fi

cd "$here/deploy/prometheus"
"$promtool" check rules skys3-alerts.yml
"$promtool" test rules skys3-alerts.test.yml
