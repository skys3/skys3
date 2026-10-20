#!/bin/sh
# Runs every crate's `simulation` test target with each test in a process
# of its own, as many at once as there are CPUs, and fails if any test
# fails, after printing its output.
#
#   scripts/ci/simulation.sh [cargo test options, such as --release]
#
# A separate process per test is what makes a failing seed replay: Tokio
# numbers tasks with one counter per process, and the order a crashed
# simulated host drops its tasks in follows their numbers. The seeded
# runner aligns that counter before each seed (skys3_sim::tasks), which
# holds only while no other test spawns tasks in the same process, as one
# `cargo test` of a whole target does. The replay command a failure prints
# runs that one test with that one seed, as here.
#
# The seed variables (SKYS3_SIM_SEEDS and the others) pass through.
# SKYS3_SIM_JOBS sets how many tests run at once.
set -eu

if [ "${1:-}" = "--run-one" ]; then
    # One test: "<package dir><TAB><package><TAB><binary><TAB><test>".
    tab=$(printf '\t')
    IFS=$tab read -r dir package binary test <<EOF
$2
EOF
    log="$3/$(printf '%s' "$package $test" | tr -c 'A-Za-z0-9_.-' '_').log"
    started=$(date +%s)
    # As `cargo test` runs it: from the package's directory, with the
    # package's name, which the replay command names.
    if (cd "$dir" && CARGO_PKG_NAME=$package CARGO_MANIFEST_DIR=$dir \
        "$binary" --exact "$test") >"$log" 2>&1; then
        echo "ok      $package $test ($(($(date +%s) - started)) s)"
        rm -f "$log"
    else
        echo "FAILED  $package $test ($(($(date +%s) - started)) s)"
    fi
    exit 0
fi

jobs=${SKYS3_SIM_JOBS:-$(nproc)}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

cargo test --workspace --all-features --test simulation --no-run \
    --message-format=json "$@" >"$work/build.json"
jq -r 'select(.reason == "compiler-artifact" and .target.name == "simulation"
              and .executable != null)
       | "\(.manifest_path)\t\(.executable)"' "$work/build.json" >"$work/binaries"

tab=$(printf '\t')
: >"$work/tests"
while IFS=$tab read -r manifest binary; do
    dir=$(dirname "$manifest")
    package=$(sed -n 's/^name = "\(.*\)"$/\1/p' "$manifest" | head -n 1)
    "$binary" --list --format terse | sed -n 's/: test$//p' | while IFS= read -r test; do
        printf '%s\t%s\t%s\t%s\n' "$dir" "$package" "$binary" "$test"
    done >>"$work/tests"
done <"$work/binaries"

total=$(($(wc -l <"$work/tests")))
echo "Running $total simulation tests, $jobs at a time, one process each"
mkdir "$work/logs"
tr '\n' '\0' <"$work/tests" |
    xargs -0 -P "$jobs" -I {} sh "$0" --run-one {} "$work/logs" |
    tee "$work/results"

passed=$(grep -c '^ok ' "$work/results" || true)
failed=0
for log in "$work"/logs/*.log; do
    [ -e "$log" ] || continue
    failed=$((failed + 1))
    echo
    echo "==== $(basename "$log" .log)"
    cat "$log"
done
echo
if [ "$failed" -gt 0 ]; then
    echo "$failed of $total simulation tests failed"
    exit 1
fi
if [ "$passed" -ne "$total" ]; then
    echo "only $passed of $total simulation tests ran"
    exit 1
fi
echo "All $total simulation tests passed"
