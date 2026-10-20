#!/usr/bin/env python3
"""Clients of a node's intra-cluster transport for the end-to-end suite's
cluster scenario (plan M8-04, design section 12).

    cluster.py present --port <port> --ca <pem> --cert <pem> --key <pem>
    cluster.py flood --port <port> --connections <n> --seconds <s>

`present` opens a TLS 1.3 connection to 127.0.0.1:<port>, verifies the
node as `localhost` against <ca>, negotiates the cluster protocol
(`skys3-cluster/1`), and presents the certificate <cert>. In TLS 1.3 the
server judges a client certificate after the client has finished, so it
then waits up to five seconds for the node to close the connection. It
prints `kept` and exits 0 if the node kept it open, or prints `refused`
and exits 1 if the handshake failed or the node closed it.

`flood` starts <n> TCP connections at once that never send a byte, the
start of a flood of handshakes that never finish (threat T7), holds them
for <s> seconds, closes them, and prints how many were established after
a second, `<established> of <n>`: those the node's accept queue has no
room for stay half-open while the kernel retries them.

Only the standard library is used.
"""

import argparse
import resource
import select
import socket
import ssl
import sys
import time

ALPN = "skys3-cluster/1"


def present(args: argparse.Namespace) -> int:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.load_verify_locations(args.ca)
    context.load_cert_chain(args.cert, args.key)
    context.set_alpn_protocols([ALPN])
    try:
        with socket.create_connection(("127.0.0.1", args.port), timeout=10) as raw:
            with context.wrap_socket(raw, server_hostname="localhost") as tls:
                tls.settimeout(5)
                try:
                    data = tls.recv(1)
                except (socket.timeout, TimeoutError):
                    print("kept")
                    return 0
                # An empty read is the node's close; anything else is not
                # what a node sends a silent client.
                print("refused" if not data else f"unexpected {data!r}")
                return 1
    except (ssl.SSLError, ConnectionError, OSError) as error:
        print(f"refused ({error})")
        return 1


def flood(args: argparse.Namespace) -> int:
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    wanted = min(hard, args.connections + 64)
    if soft < wanted:
        resource.setrlimit(resource.RLIMIT_NOFILE, (wanted, hard))
    # Every connection is started at once, without waiting: once the
    # node's accept queue is full, the kernel drops further SYNs and the
    # client retries them, as a flood's would.
    sockets = []
    for _ in range(args.connections):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)
        sock.connect_ex(("127.0.0.1", args.port))
        sockets.append(sock)
    time.sleep(min(1.0, args.seconds))
    poll = select.poll()
    for sock in sockets:
        poll.register(sock, select.POLLOUT)
    opened = sum(
        1
        for _fd, events in poll.poll(0)
        if events & select.POLLOUT and not events & (select.POLLERR | select.POLLHUP)
    )
    print(f"{opened} of {len(sockets)}", flush=True)
    time.sleep(max(0.0, args.seconds - 1.0))
    for sock in sockets:
        sock.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("present")
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--ca", required=True)
    p.add_argument("--cert", required=True)
    p.add_argument("--key", required=True)
    f = commands.add_parser("flood")
    f.add_argument("--port", type=int, required=True)
    f.add_argument("--connections", type=int, required=True)
    f.add_argument("--seconds", type=float, required=True)
    args = parser.parse_args()
    return present(args) if args.command == "present" else flood(args)


if __name__ == "__main__":
    sys.exit(main())
