#!/bin/sh
# Publishes the coverage and test-count badges for one source branch to
# the `badges` branch of the repository, under `<branch>/`, so a private
# repository can show them in its README without an external service.
#
#   scripts/ci/publish-badges.sh BRANCH SHA coverage.json coverage.log
#
# BRANCH is the source branch the figures were measured on and SHA the
# full commit they were measured at. coverage.json is the
# `cargo llvm-cov --json --summary-only` report (the badge shows its line
# percentage); coverage.log is the captured test output (its
# "test result:" lines are summed).
#
# Publications are ordered by the source branch, not by which coverage
# run finished first: before generating anything and again before every
# push attempt, the script checks that BRANCH on the remote still points
# at SHA, and exits 0 without publishing ("superseded") when it does not,
# so a slower run for an older commit can never overwrite the badges of
# a newer one. A branch that no longer exists on the remote counts as
# superseded too.
#
# Every publication is generated on top of the current tip of the badges
# branch and pushed with a plain fast-forward. If the push is rejected
# because another publication landed meanwhile, the tip is fetched again
# and the files are regenerated on top of it; nothing is ever rebased or
# merged. Environment:
#   BADGES_REMOTE    git remote to push to (default: origin)
#   BADGES_BRANCH    branch that holds the badges (default: badges)
#   BADGES_ATTEMPTS  push attempts before giving up (default: 5)
#   GIT_AUTHOR_NAME / GIT_AUTHOR_EMAIL (and COMMITTER) for the commit
set -eu

branch=${1:?branch}
source_sha=${2:?source commit}
summary_json=${3:?coverage.json}
coverage_log=${4:?coverage.log}
remote=${BADGES_REMOTE:-origin}
badges_branch=${BADGES_BRANCH:-badges}
attempts=${BADGES_ATTEMPTS:-5}
here=$(cd "$(dirname "$0")" && pwd)

# Exits 0 (nothing to do) unless the source branch on the remote still
# points at the commit these figures were measured at.
source_is_current() {
    current=$(git ls-remote "$remote" "refs/heads/$branch" | awk 'NR == 1 { print $1 }')
    if [ "$current" != "$source_sha" ]; then
        echo "publish-badges: $branch advanced from $source_sha to ${current:-nothing (branch gone)}; superseded, not publishing"
        exit 0
    fi
}
source_is_current

percent=$(jq '.data[0].totals.lines.percent' "$summary_json" | awk '{printf "%.1f", $1}')
passed=$(grep -E '^test result: ok\.' "$coverage_log" \
    | sed -E 's/^test result: ok\. ([0-9]+) passed.*/\1/' \
    | awk '{s += $1} END {print s + 0}')
whole=${percent%.*}
if [ "$whole" -ge 90 ]; then color=brightgreen
elif [ "$whole" -ge 80 ]; then color=green
elif [ "$whole" -ge 70 ]; then color=yellowgreen
elif [ "$whole" -ge 60 ]; then color=yellow
else color=red
fi
sha=$(git rev-parse --short "$source_sha")

# Every attempt gets a worktree of its own: reusing one path would reuse
# its `.git/worktrees/<name>` entry, and a `worktree add` racing with
# the removal of the previous attempt's entry has been seen to fail.
# Nothing else may prune entries meanwhile either: a fetch can leave a
# detached auto-maintenance process behind, and `git gc` prunes
# worktrees, so maintenance is off for this script's fetches; and since a
# `worktree add` that still lost its fresh entry has been seen once on a
# CI runner ("could not open .git/worktrees/<name>/locked"), the add is
# retried after a prune before the attempt is given up.
work=
cleanup() {
    if [ -n "$work" ]; then
        git worktree remove --force "$work" 2>/dev/null || rm -rf "$work"
    fi
    git worktree prune 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# `add_worktree NAME [COMMIT]`: `git worktree add --detach`, retried.
add_worktree() {
    tries=0
    while :; do
        tries=$((tries + 1))
        if git worktree add -q --detach "$@" 2>"$work.err"; then
            rm -f "$work.err"
            return 0
        fi
        if [ "$tries" -ge 3 ]; then
            cat "$work.err" >&2
            rm -f "$work.err"
            return 1
        fi
        echo "publish-badges: worktree add failed ($(tr '\n' ' ' < "$work.err")); retrying" >&2
        git worktree remove --force "$1" 2>/dev/null || rm -rf "$1"
        git worktree prune 2>/dev/null || true
        sleep 1
    done
}

n=0
while :; do
    n=$((n + 1))
    cleanup
    work=$(mktemp -d)
    rmdir "$work"
    if git -c maintenance.auto=false -c gc.auto=0 fetch -q "$remote" "$badges_branch" 2>/dev/null; then
        add_worktree "$work" FETCH_HEAD
    else
        # First publication: an empty orphan branch.
        add_worktree "$work"
        (cd "$work" && git checkout -q --orphan "$badges_branch" && git rm -rfq . >/dev/null)
    fi

    out="$work/$branch"
    mkdir -p "$out"
    sh "$here/badge.sh" coverage "${percent}%" "$color" > "$out/coverage.svg"
    sh "$here/badge.sh" tests "$passed passed" brightgreen > "$out/tests.svg"
    printf '{"branch":"%s","commit":"%s","lines_percent":%s,"tests_passed":%s}\n' \
        "$branch" "$sha" "$percent" "$passed" > "$out/summary.json"

    git -C "$work" add -A
    if git -C "$work" diff --cached --quiet; then
        echo "badges unchanged for $branch (coverage ${percent}%, ${passed} tests)"
        exit 0
    fi
    git -C "$work" commit -q -m "badges: $branch at $sha (${percent}% lines, ${passed} tests)"
    source_is_current
    if git -C "$work" push -q "$remote" "HEAD:refs/heads/$badges_branch" 2>/dev/null; then
        echo "published $branch to $remote/$badges_branch (coverage ${percent}%, ${passed} tests, commit $sha, attempt $n)"
        exit 0
    fi
    if [ "$n" -ge "$attempts" ]; then
        echo "publish-badges: push rejected $n times; giving up" >&2
        exit 1
    fi
    echo "publish-badges: push rejected (another publication landed); regenerating on the new tip"
done
