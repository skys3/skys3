#!/usr/bin/env bash
# Counts the clean nights of the kill, partition, and peering suite (plan
# M8-08), whose "Done when" asks for 7 in a row: the conclusions of its job
# in the scheduled runs of the Nightly workflow on the default branch,
# newest first, as a Markdown table and the current streak.
#
#   tests/e2e/chaos/nights.sh [<owner/repo>] [<this run's result>]
#
# The repository defaults to GITHUB_REPOSITORY. <this run's result>
# (`success`, `failure`, ...) counts the run in progress first, as the
# nightly job passes its own status. Runs started by hand (workflow_dispatch)
# do not count. It needs gh, authenticated: in CI, GH_TOKEN with
# `actions: read`.
set -euo pipefail

repo=${1:-${GITHUB_REPOSITORY:?usage: nights.sh <owner/repo> [<result>]}}
current=${2:-}
job="Kill, partition, and peering suite"

branch=$(gh api "repos/$repo" --jq .default_branch)
rows=()
streak=0
counting=true
count() {
    if [ "$counting" = true ] && [ "$1" = success ]; then
        streak=$((streak + 1))
    elif [ -n "$1" ]; then
        counting=false
    fi
}
if [ -n "$current" ]; then
    rows+=("| this run | ${GITHUB_RUN_ID:-} | $current |")
    count "$current"
fi
while read -r id created; do
    [ -n "$id" ] || continue
    conclusion=$(gh api "repos/$repo/actions/runs/$id/jobs?per_page=100" \
        --jq ".jobs[] | select(.name == \"$job\") | .conclusion")
    # Runs from before the suite existed have no such job.
    [ -n "$conclusion" ] || continue
    rows+=("| ${created%T*} | $id | $conclusion |")
    count "$conclusion"
done < <(gh api "repos/$repo/actions/workflows/nightly.yml/runs?event=schedule&branch=$branch&status=completed&per_page=30" \
    --jq '.workflow_runs[] | "\(.id) \(.created_at)"')

echo "### Clean nights in a row: $streak of 7"
echo
echo "| Night | Run | $job |"
echo "|---|---|---|"
printf '%s\n' "${rows[@]}"
