#!/usr/bin/env bash
# The network of the nightly kill, partition, and peering suite (plan
# M8-08): one network namespace per process that the suite faults, each
# with a veth on one bridge, and the faults themselves.
#
#   net.sh up <name>=<ipv4> ...   creates the bridge skc0 (10.77.0.1/24,
#                                 the host's side: the driver, the client
#                                 workload) and the namespace skc-<name>
#                                 with <ipv4>/24 on its eth0
#   net.sh down                   removes every namespace skc-*, the
#                                 bridge, and the host's FORWARD rule
#   net.sh check                  exits 0 if namespaces, veths, a bridge,
#                                 and every iptables rule the faults add
#                                 in a namespace all work
#   net.sh netem                  exits 0 if the kernel has tc netem
#   net.sh isolate <name> cluster drops every packet between <name> and
#                                 the other namespaces; the host (the
#                                 clients) still reaches it
#   net.sh isolate <name> full    drops every packet on its eth0
#   net.sh heal <name>            ends its isolation
#   net.sh shape <name> <delay ms> <jitter ms> <loss %>
#                                 delays and drops what <name> sends, with
#                                 tc netem; without netem (net.sh netem
#                                 fails), drops <loss %> of the packets
#                                 each way with iptables, and no delay
#   net.sh unshape <name>         ends its shaping
#
# Partitions and the loss fallback live in two chains of the namespace's
# own iptables, skc-part and skc-loss, so ending one never touches the
# other. Everything needs root: CI runs the suite with sudo.
set -euo pipefail

bridge=skc0
host_ip=10.77.0.1
subnet=10.77.0.0/24

ns() { echo "skc-$1"; }
in_ns() {
    local name=$1
    shift
    ip netns exec "$(ns "$name")" "$@"
}

up() {
    if ! ip link show "$bridge" >/dev/null 2>&1; then
        ip link add "$bridge" type bridge
        ip addr add "$host_ip/24" dev "$bridge"
        ip link set "$bridge" up
    fi
    # Docker's FORWARD policy (DROP) also sees bridged frames once
    # br_netfilter is loaded, as on GitHub's runners.
    iptables -C FORWARD -i "$bridge" -o "$bridge" -j ACCEPT 2>/dev/null ||
        iptables -I FORWARD 1 -i "$bridge" -o "$bridge" -j ACCEPT
    local entry name ip
    for entry in "$@"; do
        name=${entry%%=*}
        ip=${entry#*=}
        ip netns add "$(ns "$name")"
        ip link add "skc-$name" type veth peer name eth0 netns "$(ns "$name")"
        ip link set "skc-$name" master "$bridge" up
        in_ns "$name" ip addr add "$ip/24" dev eth0
        in_ns "$name" ip link set eth0 up
        in_ns "$name" ip link set lo up
        local chain
        for chain in skc-part skc-loss; do
            in_ns "$name" iptables -N "$chain"
            in_ns "$name" iptables -A INPUT -j "$chain"
            in_ns "$name" iptables -A OUTPUT -j "$chain"
        done
    done
}

down() {
    local name
    for name in $(ip netns list | awk '$1 ~ /^skc-/ { print $1 }'); do
        ip netns pids "$name" 2>/dev/null | xargs -r kill -KILL 2>/dev/null || true
        ip netns del "$name"
    done
    ip link del "$bridge" 2>/dev/null || true
    while iptables -C FORWARD -i "$bridge" -o "$bridge" -j ACCEPT 2>/dev/null; do
        iptables -D FORWARD -i "$bridge" -o "$bridge" -j ACCEPT
    done
}

check() {
    # Interface names have at most 15 bytes.
    local probe=skc-probe-$$ link=skcp$$ ok=0
    ip netns add "$probe" || return 1
    if ip link add "${link}a" type veth peer name "${link}b" 2>/dev/null; then
        ip link del "${link}a"
        # Every rule the faults add, in the probe's own iptables.
        {
            ip netns exec "$probe" iptables -N skc-part &&
                ip netns exec "$probe" iptables -N skc-loss &&
                partition_rules "$probe" cluster &&
                partition_rules "$probe" full &&
                loss_rules "$probe" 0.0100
        } >/dev/null 2>&1 || ok=1
    else
        ok=1
    fi
    if ip link add "${link}r" type bridge 2>/dev/null; then
        ip link del "${link}r"
    else
        ok=1
    fi
    ip netns del "$probe"
    return "$ok"
}

netem() {
    local probe=skc-probe-$$ ok=0
    ip netns add "$probe" || return 1
    ip netns exec "$probe" ip link set lo up
    ip netns exec "$probe" tc qdisc add dev lo root netem delay 1ms 2>/dev/null || ok=1
    ip netns del "$probe"
    return "$ok"
}

# The rules of a partition, in namespace <netns>'s chain skc-part.
partition_rules() {
    local netns=$1 mode=$2
    local -a rule=(ip netns exec "$netns" iptables -A skc-part)
    ip netns exec "$netns" iptables -F skc-part
    case "$mode" in
    cluster)
        # iptables takes one -s (and one -d) per rule: the host's packets
        # return from the chain before the subnet's are dropped.
        "${rule[@]}" -i eth0 -s "$host_ip" -j RETURN
        "${rule[@]}" -o eth0 -d "$host_ip" -j RETURN
        "${rule[@]}" -i eth0 -s "$subnet" -j DROP
        "${rule[@]}" -o eth0 -d "$subnet" -j DROP
        ;;
    full)
        "${rule[@]}" -i eth0 -j DROP
        "${rule[@]}" -o eth0 -j DROP
        ;;
    *)
        echo "isolate: the mode is cluster or full, not $mode" >&2
        return 2
        ;;
    esac
}

# The loss fallback, in namespace <netns>'s chain skc-loss.
loss_rules() {
    local netns=$1 probability=$2
    ip netns exec "$netns" iptables -F skc-loss
    ip netns exec "$netns" iptables -A skc-loss -m statistic --mode random \
        --probability "$probability" -j DROP
}

isolate() { partition_rules "$(ns "$1")" "$2"; }

heal() { in_ns "$1" iptables -F skc-part; }

shape() {
    local name=$1 delay=$2 jitter=$3 loss=$4
    if netem; then
        in_ns "$name" tc qdisc replace dev eth0 root netem \
            delay "${delay}ms" "${jitter}ms" loss "${loss}%"
    else
        local probability
        probability=$(awk -v l="$loss" 'BEGIN { printf "%.4f", l / 100 }')
        loss_rules "$(ns "$name")" "$probability"
    fi
}

unshape() {
    in_ns "$1" tc qdisc del dev eth0 root 2>/dev/null || true
    in_ns "$1" iptables -F skc-loss
}

command=${1:?usage: net.sh up|down|check|netem|isolate|heal|shape|unshape ...}
shift
case "$command" in
up | down | check | netem | isolate | heal | shape | unshape) "$command" "$@" ;;
*)
    echo "net.sh: unknown command $command" >&2
    exit 2
    ;;
esac
