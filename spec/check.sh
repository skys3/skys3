#!/bin/sh
# Model-checks the shard protocol (spec/ShardProtocol.tla) with TLC.
#
#   spec/check.sh model pr|nightly   Check the protocol at a profile's bounds;
#                                    fails on any invariant violation.
#   spec/check.sh bugs               Check every seeded bug; fails unless TLC
#                                    finds the expected violation for each.
#   spec/check.sh bug NAME           Check one seeded bug.
#   spec/check.sh all pr|nightly     model, then bugs.
#   spec/check.sh run KEY=VALUE...   Check the protocol with the given
#                                    constants, for exploring other bounds;
#                                    Invariants=A,B checks only those.
#
# spec/README.md explains the constants, the profiles, and the seeded bugs.
#
# Environment:
#   TLA2TOOLS_JAR   A tla2tools.jar to use. By default the pinned release is
#                   downloaded once into spec/.tools and its checksum verified.
#   TLC_WORKERS     TLC worker threads (default: auto, one per core).
#   TLC_JAVA_OPTS   Extra JVM options, for example -Xmx8g.
#
# Needs a Java 11 or later runtime and curl on PATH.
set -eu

here=$(cd "$(dirname "$0")" && pwd)

TLA2TOOLS_VERSION=1.7.4
TLA2TOOLS_SHA256=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
TLA2TOOLS_URL=https://github.com/tlaplus/tlaplus/releases/download/v$TLA2TOOLS_VERSION/tla2tools.jar

INVARIANTS="TypeOK CommittedRecordsSurvive CommittedRecordsAgree OneCommitterPerEpoch ReadsLinearizable"
DURABILITY="TypeOK CommittedRecordsSurvive CommittedRecordsAgree OneCommitterPerEpoch"

# The models each profile checks, one per line: constant overrides of the
# base constants (see defaults), then ':', then the invariants (all of them
# when empty). Every model must pass.
PROFILE_pr="
Members=3 :
Members=2 :
Nodes=2 Members=2 MaxEpoch=4 MaxRestarts=1 :
Nodes=2 Members=1 MaxEpoch=4 MaxWrites=2 MaxRestarts=1 :
"
PROFILE_nightly="
Members=3 MaxRestarts=1 :
Members=2 MaxEpoch=4 :
Members=2 MaxRestarts=1 :
Nodes=2 Members=2 MaxEpoch=5 MaxWrites=2 MaxRestarts=1 :
Members=2 MaxRestarts=1 Rates=1,4 : $DURABILITY
"

# The seeded bugs: name, the invariant TLC must find violated, and the
# constant overrides that reach the violation cheaply. Every name in
# BugNames in ShardProtocol.tla must appear here (check_bug_list enforces
# it). grace_without_drift and drift_beyond_bound are not protocol bugs but
# wrong clock settings: Grace without the drift allowance of section 5.4,
# and clocks drifting faster than Grace allows for.
BUGS="
promote_before_watermark CommittedRecordsSurvive
serve_before_grace ReadsLinearizable
candidate_keeps_granting ReadsLinearizable
accept_older_epoch CommittedRecordsSurvive Members=3
commit_on_majority CommittedRecordsSurvive
blind_register_write OneCommitterPerEpoch
truncate_by_seq_only CommittedRecordsSurvive Nodes=2 MaxEpoch=4 MaxWrites=2
drop_promoting_learner CommittedRecordsSurvive
abandon_unsettled_promotion CommittedRecordsSurvive
promotion_without_learner_lease ReadsLinearizable Nodes=2 Members=1 MaxEpoch=4
restart_forgets_grace ReadsLinearizable MaxRestarts=1
restart_forgets_proposal ReadsLinearizable MaxRestarts=1
stepped_down_keeps_reading ReadsLinearizable
restart_forgets_step_down ReadsLinearizable MaxRestarts=1
grace_without_drift ReadsLinearizable Bug=none Grace=2
drift_beyond_bound ReadsLinearizable Bug=none Rates=1,4
"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT INT TERM

die() {
    echo "check.sh: $*" >&2
    exit 2
}

sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -d' ' -f1
    else
        shasum -a 256 "$1" | cut -d' ' -f1
    fi
}

# Prints the path of tla2tools.jar, downloading the pinned release if needed.
tla2tools() {
    if [ -n "${TLA2TOOLS_JAR:-}" ]; then
        [ -f "$TLA2TOOLS_JAR" ] || die "TLA2TOOLS_JAR=$TLA2TOOLS_JAR does not exist"
        echo "$TLA2TOOLS_JAR"
        return
    fi
    jar=$here/.tools/tla2tools-$TLA2TOOLS_VERSION.jar
    if [ ! -f "$jar" ]; then
        mkdir -p "$here/.tools"
        echo "Downloading tla2tools.jar $TLA2TOOLS_VERSION" >&2
        curl -fsSL --retry 4 -o "$jar.part" "$TLA2TOOLS_URL"
        got=$(sha256 "$jar.part")
        if [ "$got" != "$TLA2TOOLS_SHA256" ]; then
            rm -f "$jar.part"
            die "tla2tools.jar checksum mismatch: got $got, expected $TLA2TOOLS_SHA256"
        fi
        mv "$jar.part" "$jar"
    fi
    echo "$jar"
}

# The base constants: the protocol's settings scaled to small integers, and
# the smallest bounds. Profiles and seeded bugs override some of them.
defaults() {
    Nodes=3
    Members=2
    MaxEpoch=3
    MaxWrites=1
    MaxRestarts=0
    MinWriteReplicas=1
    Lease=2
    Grace=4
    Rates=1,2
    Bug=none
}

set_constant() {
    value=${1#*=}
    case $1 in
        Nodes=*) Nodes=$value ;;
        Members=*) Members=$value ;;
        MaxEpoch=*) MaxEpoch=$value ;;
        MaxWrites=*) MaxWrites=$value ;;
        MaxRestarts=*) MaxRestarts=$value ;;
        MinWriteReplicas=*) MinWriteReplicas=$value ;;
        Lease=*) Lease=$value ;;
        Grace=*) Grace=$value ;;
        Rates=*) Rates=$value ;;
        Bug=*) Bug=$value ;;
        *) die "unknown constant '$1'" ;;
    esac
}

describe() {
    echo "Nodes=$Nodes Members=$Members MaxEpoch=$MaxEpoch MaxWrites=$MaxWrites" \
        "MaxRestarts=$MaxRestarts MinWriteReplicas=$MinWriteReplicas Lease=$Lease" \
        "Grace=$Grace Rates={$Rates} Bug=$Bug"
}

# Writes a TLC configuration for the current constants, checking the given
# invariants, to $1. Nodes are n1 to n$Nodes; n1 is the initial primary and
# n1 to n$Members the initial members.
write_cfg() {
    out=$1
    shift
    nodes=
    members=
    i=1
    while [ "$i" -le "$Nodes" ]; do
        nodes="$nodes${nodes:+, }n$i"
        [ "$i" -gt "$Members" ] || members="$members${members:+, }n$i"
        i=$((i + 1))
    done
    {
        echo "CONSTANTS"
        i=1
        while [ "$i" -le "$Nodes" ]; do
            echo "    n$i = n$i"
            i=$((i + 1))
        done
        echo "    Nodes = {$nodes}"
        echo "    InitPrimary = n1"
        echo "    InitMembers = {$members}"
        echo "    MaxEpoch = $MaxEpoch"
        echo "    MaxWrites = $MaxWrites"
        echo "    MaxRestarts = $MaxRestarts"
        echo "    MinWriteReplicas = $MinWriteReplicas"
        echo "    Lease = $Lease"
        echo "    Grace = $Grace"
        echo "    Rates = {$Rates}"
        echo "    Bug = \"$Bug\""
        echo "SPECIFICATION Spec"
        echo "INVARIANTS"
        for inv in "$@"; do
            echo "    $inv"
        done
        echo "SYMMETRY Symmetry"
        echo "CHECK_DEADLOCK FALSE"
    } >"$out"
}

# Runs TLC on the current constants with the given invariants. Sets
# `status` to TLC's exit status and leaves its output in $work/$1.out.
tlc() {
    name=$1
    shift
    write_cfg "$work/$name.cfg" "$@"
    cp "$here/ShardProtocol.tla" "$work/"
    rm -rf "$work/states"
    set +e
    # shellcheck disable=SC2086 # TLC_JAVA_OPTS is a list of options.
    (cd "$work" && java -XX:+UseParallelGC ${TLC_JAVA_OPTS:-} -cp "$jar" tlc2.TLC \
        -workers "${TLC_WORKERS:-auto}" -metadir states -config "$name.cfg" \
        ShardProtocol.tla) >"$work/$name.out" 2>&1
    status=$?
    set -e
}

summary() {
    grep -E '^(Error: |[0-9,]+ states generated|The depth of|Finished in)' "$1" || true
}

# Checks the current constants with the given invariants (all when none
# are given); every one must hold.
check_model() {
    # shellcheck disable=SC2086 # INVARIANTS is a list of names.
    [ $# -gt 0 ] || set -- $INVARIANTS
    echo "== Model: $(describe); invariants: $*"
    tlc model "$@"
    summary "$work/model.out"
    if [ "$status" -ne 0 ]; then
        echo "FAILED: TLC exited with status $status" >&2
        tail -n 300 "$work/model.out" >&2
        exit 1
    fi
}

check_profile() {
    case $1 in
        pr) lines=$PROFILE_pr ;;
        nightly) lines=$PROFILE_nightly ;;
        *) die "unknown profile '$1' (expected pr or nightly)" ;;
    esac
    echo "$lines" | while IFS=: read -r overrides invariants; do
        [ -n "$overrides$invariants" ] || continue
        (
            for kv in $overrides; do
                set_constant "$kv"
            done
            # shellcheck disable=SC2086 # invariants is a list of names.
            check_model $invariants
        )
    done
}

# Checks that TLC finds a violation of invariant $2 with seeded bug $1.
# Further arguments are constant overrides. Only the expected invariant is
# checked, so the bug must break that property, not just whichever one
# TLC happens to reach first.
check_bug() {
    bug=$1
    expect=$2
    shift 2
    (
        Bug=$bug
        for kv in "$@"; do
            set_constant "$kv"
        done
        echo "== Seeded bug $bug, expecting $expect to be violated: $(describe)"
        tlc "bug-$bug" TypeOK "$expect"
        summary "$work/bug-$bug.out"
        if [ "$status" -eq 12 ] && grep -q "Invariant $expect is violated" "$work/bug-$bug.out"; then
            echo "caught"
        else
            echo "FAILED: seeded bug $bug was not caught as a violation of $expect (TLC status $status)" >&2
            tail -n 300 "$work/bug-$bug.out" >&2
            exit 1
        fi
    )
}

check_bug_list() {
    declared=$(sed -n '/^BugNames == {/,/}/p' "$here/ShardProtocol.tla" |
        grep -o '"[a-z_]*"' | tr -d '"' | sort)
    listed=$(echo "$BUGS" | awk 'NF && $3 !~ /^Bug=/ { print $1 }' | sort)
    if [ "$declared" != "$listed" ]; then
        echo "FAILED: the seeded bugs in check.sh differ from BugNames in ShardProtocol.tla" >&2
        echo "BugNames: $(echo $declared)" >&2
        echo "check.sh: $(echo $listed)" >&2
        exit 1
    fi
}

check_bugs() {
    check_bug_list
    echo "$BUGS" | while read -r bug expect overrides; do
        [ -n "$bug" ] || continue
        # shellcheck disable=SC2086 # overrides is a list of KEY=VALUE words.
        check_bug "$bug" "$expect" $overrides
    done
}

[ $# -ge 1 ] || die "usage: check.sh model|all pr|nightly, check.sh bugs, check.sh bug NAME, or check.sh run KEY=VALUE..."
command -v java >/dev/null 2>&1 || die "java is not on PATH"
jar=$(tla2tools)
defaults

case $1 in
    model)
        [ $# -eq 2 ] || die "usage: check.sh model pr|nightly"
        check_profile "$2"
        ;;
    bugs)
        [ $# -eq 1 ] || die "usage: check.sh bugs"
        check_bugs
        ;;
    all)
        [ $# -eq 2 ] || die "usage: check.sh all pr|nightly"
        check_profile "$2"
        check_bugs
        ;;
    bug)
        [ $# -eq 2 ] || die "usage: check.sh bug NAME"
        line=$(echo "$BUGS" | awk -v b="$2" '$1 == b')
        [ -n "$line" ] || die "unknown seeded bug '$2'"
        # shellcheck disable=SC2086 # line is a list of words.
        check_bug $line
        ;;
    run)
        shift
        invariants=
        for kv in "$@"; do
            case $kv in
                Invariants=*) invariants=$(echo "${kv#*=}" | tr ',' ' ') ;;
                *) set_constant "$kv" ;;
            esac
        done
        # shellcheck disable=SC2086 # invariants is a list of names.
        check_model $invariants
        ;;
    *)
        die "unknown command '$1'"
        ;;
esac
