#!/bin/sh
# Renders a flat, shields.io-style SVG badge on stdout.
#
#   scripts/ci/badge.sh LABEL VALUE [COLOR] > badge.svg
#
# COLOR is one of the shields names (brightgreen, green, yellowgreen,
# yellow, orange, red, grey) or a hexadecimal colour (#rgb or #rrggbb);
# anything else is rejected. Text is escaped for both element content and
# attribute values. Text width is estimated at 7 px per character plus
# 10 px of padding on each side, which matches shields.io closely for the
# short ASCII labels used here. No network access, no dependencies.
set -eu

label=${1:?label}
value=${2:?value}
color=${3:-brightgreen}

case $color in
    brightgreen) color='#4c1' ;;
    green) color='#97ca00' ;;
    yellowgreen) color='#a4a61d' ;;
    yellow) color='#dfb317' ;;
    orange) color='#fe7d37' ;;
    red) color='#e05d44' ;;
    grey | gray | lightgrey | lightgray) color='#9f9f9f' ;;
    *)
        if ! printf '%s' "$color" | grep -Eq '^#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$'; then
            echo "badge.sh: colour must be a shields name or #rgb/#rrggbb, got '$color'" >&2
            exit 2
        fi
        ;;
esac

escape() {
    printf '%s' "$1" | sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g' \
        -e 's/"/\&quot;/g' -e "s/'/\&apos;/g"
}

label_width=$(( ${#label} * 7 + 10 ))
value_width=$(( ${#value} * 7 + 10 ))
width=$(( label_width + value_width ))
label_x=$(( label_width * 10 / 2 ))
value_x=$(( (label_width + value_width / 2) * 10 ))
label_len=$(( (label_width - 10) * 10 ))
value_len=$(( (value_width - 10) * 10 ))
label_text=$(escape "$label")
value_text=$(escape "$value")

cat <<SVG
<svg xmlns="http://www.w3.org/2000/svg" width="$width" height="20" role="img" aria-label="$label_text: $value_text">
<title>$label_text: $value_text</title>
<linearGradient id="s" x2="0" y2="100%"><stop offset="0" stop-color="#bbb" stop-opacity=".1"/><stop offset="1" stop-opacity=".1"/></linearGradient>
<clipPath id="r"><rect width="$width" height="20" rx="3" fill="#fff"/></clipPath>
<g clip-path="url(#r)"><rect width="$label_width" height="20" fill="#555"/><rect x="$label_width" width="$value_width" height="20" fill="$color"/><rect width="$width" height="20" fill="url(#s)"/></g>
<g fill="#fff" text-anchor="middle" font-family="Verdana,Geneva,DejaVu Sans,sans-serif" font-size="110" text-rendering="geometricPrecision">
<text x="$label_x" y="150" fill="#010101" fill-opacity=".3" transform="scale(.1)" textLength="$label_len">$label_text</text>
<text x="$label_x" y="140" transform="scale(.1)" textLength="$label_len">$label_text</text>
<text x="$value_x" y="150" fill="#010101" fill-opacity=".3" transform="scale(.1)" textLength="$value_len">$value_text</text>
<text x="$value_x" y="140" transform="scale(.1)" textLength="$value_len">$value_text</text>
</g>
</svg>
SVG
