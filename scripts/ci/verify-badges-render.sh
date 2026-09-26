#!/bin/sh
# Checks what CI can prove about the README badges of this (possibly
# private) repository, after a publication for BRANCH:
#
#   1. the README as GitHub renders it (the HTML the web page embeds)
#      carries exactly the two badge <img> elements, tests.svg and
#      coverage.svg, each once, and GitHub left their same-repository raw
#      URLs in place (no Camo rewrite);
#   2. the files those URLs name, re-pointed at the directory just
#      published for BRANCH, exist on the badges branch and are served to
#      an authenticated client (the workflow token, as a signed-in browser
#      sends its session) with an SVG content type.
#
# The github.com `raw/` path itself authenticates only by browser session,
# so the fetch goes through raw.githubusercontent.com, which is where that
# path redirects a browser. Whether the images finally appear in the page
# is confirmed by a signed-in viewer of the README on `main`.
#
#   GH_TOKEN=... scripts/ci/verify-badges-render.sh OWNER/REPO BRANCH
set -eu
repo=${1:?owner/repo}
branch=${2:?branch}
html=$(curl -sS -f -H "Authorization: Bearer ${GH_TOKEN:?}" \
    -H "Accept: application/vnd.github.html+json" \
    "https://api.github.com/repos/$repo/readme?ref=$branch")
urls=$(printf '%s' "$html" | grep -o '<img[^>]*src="[^"]*"' | sed -E 's/.*src="([^"]*)".*/\1/' \
    | grep -E 'coverage\.svg|tests\.svg' || true)
if [ -z "$urls" ]; then
    echo "verify-badges-render: no badge <img> in the rendered README:" >&2
    printf '%s\n' "$html" | grep -o '<img[^>]*>' >&2 || true
    exit 1
fi
status=0
seen_tests=0
seen_coverage=0
for url in $urls; do
    url=$(printf '%s' "$url" | sed 's|&amp;|\&|g')
    case "$url" in
        https://github.com/"$repo"/raw/badges/main/tests.svg) seen_tests=$((seen_tests + 1)) ;;
        https://github.com/"$repo"/raw/badges/main/coverage.svg) seen_coverage=$((seen_coverage + 1)) ;;
        *)
            echo "verify-badges-render: unexpected badge URL in the rendered README: $url" >&2
            status=1
            continue ;;
    esac
    file=${url##*/badges/main/}
    raw="https://raw.githubusercontent.com/$repo/badges/$branch/$file"
    headers=$(curl -sS -o /dev/null -D - -L -H "Authorization: token ${GH_TOKEN}" "$raw" | tr -d '\r' || true)
    code=$(printf '%s' "$headers" | grep -i '^HTTP/' | tail -1 | awk '{print $2}')
    ctype=$(printf '%s' "$headers" | grep -i '^content-type:' | tail -1 | cut -d' ' -f2-)
    echo "README embeds $url"
    echo "  published file $raw -> $code ${ctype:-?}"
    case "$code:$ctype" in
        200:*svg*) ;;
        *)
            echo "verify-badges-render: $file is not served as SVG" >&2
            status=1 ;;
    esac
done
for badge in tests coverage; do
    eval "n=\$seen_$badge"
    if [ "$n" -eq 0 ]; then
        echo "verify-badges-render: the rendered README does not embed badges/main/$badge.svg" >&2
        status=1
    elif [ "$n" -gt 1 ]; then
        echo "verify-badges-render: badges/main/$badge.svg is embedded more than once ($n)" >&2
        status=1
    fi
done
if [ "$status" -ne 0 ]; then
    echo "verify-badges-render: the README badges do not verify (see above)" >&2
fi
exit "$status"
