#!/bin/sh
# Exits 0 when every path that differs between two commits is
# documentation, and 1 when anything else differs or the commits cannot
# be compared. CI uses it to skip the build and test jobs for a
# docs-only change; publish-badges.sh uses it to keep publishing figures
# for a commit that its branch has moved past only by such changes.
#
#   scripts/ci/docs-only.sh BASE HEAD
#
# Documentation is any Markdown file and any file named LICENSE. Both
# commits must be present locally. The README is also the crate's rustdoc
# (src/lib.rs includes it), so CI still runs the doctests and rustdoc for
# a docs-only change; see the `docs` job in .github/workflows/ci.yml.
set -eu

base=${1:?base commit}
head=${2:?head commit}

# --no-renames lists both sides of a rename, so moving code into a .md
# file does not count as a documentation change.
paths=$(git diff --name-only --no-renames "$base" "$head") || exit 1
printf '%s\n' "$paths" | while IFS= read -r path; do
    case $path in
        '' | *.md | LICENSE | */LICENSE) ;;
        *)
            echo "docs-only: $path is not documentation"
            exit 1
            ;;
    esac
done
