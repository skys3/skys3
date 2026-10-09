#!/usr/bin/env bash
# Runs one cargo-fuzz target from its committed corpus, with the input and
# resource bounds this file sets per target (plan M7-03). CI's smoke job,
# the nightly campaign, and local runs all start targets through it, so a
# target runs under the same bounds everywhere.
#
#   fuzz/run.sh <target> <seconds> [extra libFuzzer flags...]
#   fuzz/run.sh --cmin <target> [extra libFuzzer flags...]
#
# The first form fuzzes for <seconds>: inputs the run finds are added to
# fuzz/corpus/<target>, and crashes, timeouts, and out-of-memory inputs go to
# fuzz/artifacts/<target>. The second minimizes the corpus in place with
# `cargo fuzz cmin`, under the same bounds, so no input longer than the
# target's `max_len` is kept. Run it before committing a corpus. Set FUZZ_TARGET to
# a target triple to pass `--target` to cargo-fuzz (CI names the host,
# because the prebuilt cargo-fuzz defaults to its own musl triple).
#
# The bounds follow the parsers' limits (design section 12):
#
# - `max_len` covers every limit a parser enforces by counting the bytes
#   it reads (header lines, request heads, documents), so the fuzzer can
#   reach both sides of it. A limit checked on a declared length, before
#   the bytes arrive (frame and record lengths, the 4 MiB XML body), needs
#   no input that long: libFuzzer's default of 4 KiB applies there, and
#   unit tests cover the large limits themselves.
# - `malloc_limit_mb` fails any single allocation larger than a parser could
#   need for an input of `max_len`, which is how an allocation sized from a
#   declared length shows up. `rss_limit_mb` leaves room for AddressSanitizer
#   (its quarantine alone holds 256 MiB) and for harnesses that keep state.
# - `timeout`: every input within these lengths parses in milliseconds; a
#   unit that takes 10 s is a finding (super-linear work), not noise.

set -euo pipefail

usage() {
  echo "usage: $0 <target> <seconds> [libFuzzer flags...]" >&2
  echo "       $0 --cmin <target> [libFuzzer flags...]" >&2
  exit 2
}
if [ "${1:-}" = "--cmin" ]; then
  [ "$#" -ge 2 ] || usage
  mode=cmin target="$2"
  shift 2
else
  [ "$#" -ge 2 ] || usage
  mode=run target="$1" seconds="$2"
  shift 2
fi

# Defaults: compact parsers whose limits are declared lengths or are short.
max_len=4096
malloc_mb=64
rss_mb=1024
timeout=10

case "$target" in
  # A request head: a 16 KiB target and 16 KiB of header fields. The
  # gateway harness also keeps simulated shards for 256 inputs at a time.
  gateway_request | gateway_sigv4_canonical)
    max_len=65536 malloc_mb=128 rss_mb=2048 ;;
  # Chunk header lines of 128 bytes and a trailer section of 4 KiB.
  gateway_aws_chunked)
    max_len=16384 ;;
  # An x-amz-tagging header: 10 tags of 128 and 256 characters, URL-encoded.
  gateway_tagging)
    max_len=16384 ;;
  # Identity and trust policies: 10,240 bytes of text.
  types_policy | types_trust_policy)
    max_len=16384 ;;
  # A web identity token: 20,000 bytes.
  sts_jwt)
    max_len=32768 ;;
  # An AssumeRoleWithWebIdentity form: 64 KiB.
  sts_assume_role)
    max_len=131072 ;;
  # A key set with RSA keys of up to 8,192 bits, and node exports with
  # several registers, are a few KiB each.
  sts_jwks | control_rebuild)
    max_len=16384 ;;
  # A peer descriptor with its certificate chain of up to four certificates.
  peer_descriptor)
    max_len=16384 ;;
esac

corpus="corpus/$target"
cd "$(dirname "$0")"
mkdir -p "$corpus"

triple=()
if [ -n "${FUZZ_TARGET:-}" ]; then
  triple=(--target "$FUZZ_TARGET")
fi

bounds=(
  -max_len="$max_len"
  -malloc_limit_mb="$malloc_mb"
  -rss_limit_mb="$rss_mb"
  -timeout="$timeout"
)
if [ "$mode" = cmin ]; then
  # Edges only (`-use_counters=0`): an input stays if it reaches code no
  # smaller input reaches, which keeps the committed corpus small. Pass
  # `-use_counters=1` to also keep inputs that only run code more often.
  exec cargo +nightly fuzz cmin "${triple[@]}" "$target" "$corpus" -- \
    "${bounds[@]}" -use_counters=0 "$@"
fi
exec cargo +nightly fuzz run "${triple[@]}" "$target" "$corpus" -- \
  -max_total_time="$seconds" "${bounds[@]}" "$@"
