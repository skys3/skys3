#!/usr/bin/env bash
# Blocks or unblocks UDP to a local port with iptables, for the peering
# scenario's fallback check (plan M8-01).
#
#   tests/e2e/udp.sh block <port>
#   tests/e2e/udp.sh unblock <port>
#
# The rule drops every UDP datagram this host sends to <port>, loopback
# included, and carries a comment so it is recognizable. It needs root, or
# sudo without a password.
set -euo pipefail

action=${1:?usage: udp.sh block|unblock <port>}
port=${2:?usage: udp.sh block|unblock <port>}
sudo=
[ "$(id -u)" = 0 ] || sudo=sudo
rule=(OUTPUT -p udp --dport "$port" -m comment --comment skys3-e2e -j DROP)
case "$action" in
block) $sudo iptables -I "${rule[@]}" ;;
unblock)
    while $sudo iptables -C "${rule[@]}" 2>/dev/null; do
        $sudo iptables -D "${rule[@]}"
    done
    ;;
*)
    echo "usage: udp.sh block|unblock <port>" >&2
    exit 2
    ;;
esac
