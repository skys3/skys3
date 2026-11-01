#!/usr/bin/env python3
"""The nightly kill, partition, and peering suite against the node binary
(plan M8-08, covering M2-20 and the real-binary part of M3-08; design
sections 16.1 and 17).

    chaos.py run --dir <dir> --binary <skys3> --etcd <etcd> [--seed <n>]
                 [--minutes <m>] [--netem | --no-netem]
    chaos.py schedule --seed <n> [--steps <k>]

`run` expects the network `net.sh up` made (run.sh does it all): cluster
`ka`, five nodes n1 to n5 in racks ra, rb, rc, ra, rb with `failure_domain
= "rack"`; a three-member etcd, e1 to e3, which both clusters share under
their own prefixes; and cluster `kb`, three nodes p1 to p3, which bucket
`peered` of `ka` flushes to. Each process runs in a network namespace of
its own, so it can be killed, partitioned, and shaped alone.

A client workload writes throughout (PUTs, overwrites, and DELETEs, each
key written by one thread, one request at a time, retried as the AWS SDKs
retry) through the nodes the clients can reach, records every answer, and
reads a share of its acknowledged writes back through another node at
once. Meanwhile a schedule drawn from the seed strikes the cluster: n5
joins and the coordinator is killed while it moves shards onto it; then,
in a seeded order, round after round until the time is up, a kill of a
shard's primary, a kill loop, an iptables partition of a primary, tc netem
delay and loss, the loss of a rack, the coordinator's loss in the middle
of a handoff, and the loss of an etcd member. After each fault the suite
waits until the cluster heals with no operator action: every node up and
every shard with `replicas` members and no learner.

At the end, with every fault healed, it checks that every key reads back
through the nodes with its last acknowledged content, or that of a later
write, acknowledged or not, that may have applied; that no read after an
acknowledgment returned older content; that the members of every shard
applied the same records; and that the keys of `peered` arrived on `kb`.
It reports failover times, from a primary's loss to the first successful
write on its shard, as percentiles per fault, in report.json and
report.md (also appended to GITHUB_STEP_SUMMARY), and exits non-zero on
any violation or a cluster that did not heal.

The seed fixes the schedule: which fault, against which shard, rack, node,
or member, for how long. The cluster's timing is the machine's, so a replay
strikes the same targets in the same order rather than repeating the run
exactly. `schedule` prints a seed's first steps.

Only the standard library is used.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import hmac
import http.client
import itertools
import json
import os
import random
import signal
import socket
import ssl
import subprocess
import sys
import threading
import time
import urllib.parse
from dataclasses import dataclass, field

# --- Topology -----------------------------------------------------------------

HOST = "10.77.0.1"
GATEWAY, ADMIN, TRANSPORT, QUIC = 9000, 9001, 9002, 7443
ETCD_CLIENT, ETCD_PEER = 2379, 2380

SOURCE, PEER = "ka", "kb"
# Cluster ka: node -> (address, rack). n5 joins during the run.
SOURCE_NODES = {
    "n1": ("10.77.0.11", "ra"),
    "n2": ("10.77.0.12", "rb"),
    "n3": ("10.77.0.13", "rc"),
    "n4": ("10.77.0.14", "ra"),
    "n5": ("10.77.0.15", "rb"),
}
FIRST_NODES = ["n1", "n2", "n3", "n4"]
JOINING = "n5"
PEER_NODES = {"p1": "10.77.0.31", "p2": "10.77.0.32", "p3": "10.77.0.33"}
ETCD = {"e1": "10.77.0.21", "e2": "10.77.0.22", "e3": "10.77.0.23"}

# Buckets: `chaos` is local; `peered` flushes to `inbound` of kb.
LOCAL_BUCKET, PEERED_BUCKET, INBOUND = "chaos", "peered", "inbound"
SHARDS = 8
REPLICAS = 3
PEER_SHARDS = 4

ADMIN_KEY = "AKIASKYS3CHAOSADMIN"
ADMIN_SECRET = "skys3-chaos-admin-secret-0123456789abcd"
PEER_KEY = "AKIASKYS3CHAOSPEER"
PEER_SECRET = "skys3-chaos-peer-secret-0123456789abcdef"
ALLOW_ALL = '{"Version": "2012-10-17", "Statement": {"Effect": "Allow", "Action": "*", "Resource": "*"}}'

# How long the cluster may take to heal after a fault, and the keys of
# `peered` to arrive once the workload stops.
HEAL_SECONDS = 300
READ_BACK_PATIENCE_SECONDS = 120
HANDOFF_SECONDS = 60
# The fewest acknowledged writes to each bucket a run needs for its checks
# to mean anything: a few minutes of the workload acknowledge thousands.
MIN_ACKED_WRITES = 200
PEER_ARRIVAL_SECONDS = 900


def log(message: str) -> None:
    print(f"{time.strftime('%H:%M:%S', time.gmtime())} {message}", flush=True)


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round(fraction * (len(ordered) - 1)))))
    return ordered[index]


def distribution(values: list[float]) -> dict:
    return {
        "count": len(values),
        "p50": percentile(values, 0.50),
        "p90": percentile(values, 0.90),
        "p99": percentile(values, 0.99),
        "max": max(values) if values else None,
    }


def shard_of(bucket_id: str, key: str, shards: int) -> int:
    """The shard of `key` in the bucket (design section 4.1, the frozen
    hash of skys3-types `KeyHash`)."""
    digest = hashlib.sha256(
        b"skys3-shard-v1\0" + bucket_id.encode() + b"\0" + key.encode()
    ).digest()
    return int.from_bytes(digest[:8], "big") % shards


# --- S3 requests ----------------------------------------------------------------


def sign(method: str, host: str, path: str, headers: dict[str, str]) -> dict[str, str]:
    """SigV4 headers for an S3 request with an unsigned payload."""
    amz_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    date = amz_date[:8]
    signed_headers = {name.lower(): value.strip() for name, value in headers.items()}
    signed_headers.update(
        {"host": host, "x-amz-date": amz_date, "x-amz-content-sha256": "UNSIGNED-PAYLOAD"}
    )
    names = sorted(signed_headers)
    canonical = "\n".join(
        [
            method,
            urllib.parse.quote(path, safe="/-_.~"),
            "",
            "".join(f"{name}:{signed_headers[name]}\n" for name in names),
            ";".join(names),
            "UNSIGNED-PAYLOAD",
        ]
    )
    scope = f"{date}/us-east-1/s3/aws4_request"
    to_sign = "\n".join(
        ["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical.encode()).hexdigest()]
    )
    key = ("AWS4" + ADMIN_SECRET).encode()
    for part in (date, "us-east-1", "s3", "aws4_request"):
        key = hmac.new(key, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    signed_headers["authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={ADMIN_KEY}/{scope}, "
        f"SignedHeaders={';'.join(names)}, Signature={signature}"
    )
    return signed_headers


class S3:
    """One client's connections to the gateways, kept alive per address."""

    ATTEMPTS = 3
    TIMEOUT = 15.0

    def __init__(self) -> None:
        self.connections: dict[str, http.client.HTTPConnection] = {}

    def once(self, address: str, method: str, path: str, body: bytes = b"",
             headers: dict[str, str] | None = None) -> tuple[int, bytes]:
        """One attempt: the status and body, or status 0 if no answer came."""
        host = f"{address}:{GATEWAY}"
        connection = self.connections.get(address)
        if connection is None:
            connection = http.client.HTTPConnection(address, GATEWAY, timeout=self.TIMEOUT)
            self.connections[address] = connection
        try:
            connection.request(method, path, body=body if method == "PUT" else None,
                               headers=sign(method, host, path, headers or {}))
            response = connection.getresponse()
            data = response.read()
            if response.getheader("connection", "").lower() == "close":
                connection.close()
                del self.connections[address]
            return response.status, data
        except (OSError, http.client.HTTPException):
            connection.close()
            self.connections.pop(address, None)
            return 0, b""

    def call(self, pick, method: str, path: str, body: bytes = b"",
             headers: dict[str, str] | None = None) -> tuple[int, bytes, str, int]:
        """A request retried on 5xx answers and on no answer, 3 attempts in
        all with jittered backoff, as the AWS SDKs' standard retry mode does
        by default: up to 1 s before the second attempt, and up to 2 s
        before the third. `pick` chooses the node of each attempt, as a load
        balancer would. It returns the final status and body, the last node,
        and the attempts."""
        status, data, node = 0, b"", ""
        for attempt in range(self.ATTEMPTS):
            if attempt:
                time.sleep(random.uniform(0, 2 ** (attempt - 1)))
            node, address = pick()
            status, data = self.once(address, method, path, body, headers)
            if status and status < 500:
                break
        return status, data, node, attempt + 1


# --- Admin API, etcd, and processes -----------------------------------------------


class Admin:
    def __init__(self, token: str) -> None:
        self.token = token

    def call(self, address: str, method: str, path: str, body: dict | None = None,
             timeout: float = 5.0) -> tuple[int, dict | None]:
        connection = http.client.HTTPConnection(address, ADMIN, timeout=timeout)
        try:
            payload = json.dumps(body).encode() if body is not None else None
            headers = {"authorization": f"Bearer {self.token}"}
            if payload is not None:
                headers["content-type"] = "application/json"
            connection.request(method, path, body=payload, headers=headers)
            response = connection.getresponse()
            data = response.read()
            try:
                return response.status, json.loads(data) if data else None
            except ValueError:
                return response.status, None
        except (OSError, http.client.HTTPException):
            return 0, None
        finally:
            connection.close()

    def get(self, address: str, path: str) -> dict | None:
        status, answer = self.call(address, "GET", path)
        return answer if status == 200 else None


class Processes:
    """The suite's processes, each in its namespace: `ip netns exec` execs
    the program, so the child's PID is the program's."""

    def __init__(self, logs: str) -> None:
        self.logs = logs
        self.running: dict[str, subprocess.Popen] = {}
        self.commands: dict[str, tuple[list[str], dict[str, str]]] = {}

    def define(self, name: str, argv: list[str], env: dict[str, str]) -> None:
        self.commands[name] = (argv, env)

    def start(self, name: str) -> None:
        argv, env = self.commands[name]
        output = open(os.path.join(self.logs, f"{name}.log"), "ab")
        output.write(f"=== started at {time.time():.3f}\n".encode())
        output.flush()
        self.running[name] = subprocess.Popen(
            ["ip", "netns", "exec", f"skc-{name}", *argv],
            env=env, stdout=output, stderr=subprocess.STDOUT, start_new_session=True,
        )
        output.close()

    def alive(self, name: str) -> bool:
        process = self.running.get(name)
        return process is not None and process.poll() is None

    def signal(self, name: str, sig: int, timeout: float = 60.0) -> None:
        process = self.running.pop(name, None)
        if process is None:
            return
        if process.poll() is None:
            process.send_signal(sig)
        try:
            process.wait(timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()

    def kill(self, name: str) -> None:
        self.signal(name, signal.SIGKILL)

    def stop_all(self) -> None:
        for name in list(self.running):
            self.signal(name, signal.SIGTERM, timeout=20)


def run_net(*args: str) -> str:
    return subprocess.run(["bash", os.path.join(os.path.dirname(__file__), "net.sh"), *args], check=True,
                          stdout=subprocess.PIPE, text=True).stdout


# --- The clusters -------------------------------------------------------------------


@dataclass
class Suite:
    dir: str
    binary: str
    etcd_binary: str
    netem: bool
    pki: str = ""
    token: str = ""
    procs: Processes = None  # type: ignore[assignment]
    admin: Admin = None  # type: ignore[assignment]
    bucket_ids: dict[str, str] = field(default_factory=dict)
    # Nodes the clients do not reach (fully isolated), and those down.
    unreachable: set[str] = field(default_factory=set)
    # The nodes of ka that should be up once the faults heal.
    expected_up: set[str] = field(default_factory=set)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def __post_init__(self) -> None:
        self.pki = os.path.join(self.dir, "pki")
        os.makedirs(os.path.join(self.dir, "logs"), exist_ok=True)
        self.procs = Processes(os.path.join(self.dir, "logs"))
        self.token = "skys3-chaos-admin-token-" + hashlib.sha256(self.dir.encode()).hexdigest()[:24]
        with open(os.path.join(self.dir, "admin-token"), "w") as f:
            f.write(self.token)
        with open(os.path.join(self.dir, "admin-secret"), "w") as f:
            f.write(ADMIN_SECRET)
        with open(os.path.join(self.dir, "peer-secret"), "w") as f:
            f.write(PEER_SECRET)
        self.admin = Admin(self.token)

    # Addresses.
    @staticmethod
    def address(node: str) -> str:
        if node in SOURCE_NODES:
            return SOURCE_NODES[node][0]
        if node in PEER_NODES:
            return PEER_NODES[node]
        return ETCD[node]

    def up(self, node: str) -> bool:
        return self.procs.alive(node)

    def source_up(self) -> list[str]:
        return [node for node in SOURCE_NODES if self.up(node)]

    def clients(self) -> list[str]:
        """The nodes of ka the clients send requests to."""
        with self.lock:
            return [node for node in self.source_up() if node not in self.unreachable]

    # --- etcd ---
    def define_etcd(self) -> None:
        cluster = ",".join(f"{name}=http://{ip}:{ETCD_PEER}" for name, ip in ETCD.items())
        for name, ip in ETCD.items():
            client = f"https://{ip}:{ETCD_CLIENT}"
            self.procs.define(name, [
                self.etcd_binary, "--name", name, "--data-dir", os.path.join(self.dir, "etcd", name),
                "--listen-client-urls", client, "--advertise-client-urls", client,
                "--listen-peer-urls", f"http://{ip}:{ETCD_PEER}",
                "--initial-advertise-peer-urls", f"http://{ip}:{ETCD_PEER}",
                "--initial-cluster", cluster, "--initial-cluster-state", "new",
                "--initial-cluster-token", "skys3-chaos",
                "--cert-file", f"{self.pki}/etcd-server.pem", "--key-file", f"{self.pki}/etcd-server.key",
                "--client-cert-auth", "--trusted-ca-file", f"{self.pki}/e2e-ca.pem",
            ], dict(os.environ))

    def etcd_healthy(self, name: str) -> bool:
        context = ssl.create_default_context(cafile=f"{self.pki}/e2e-ca.pem")
        context.load_cert_chain(f"{self.pki}/etcd-client.pem", f"{self.pki}/etcd-client.key")
        connection = http.client.HTTPSConnection(ETCD[name], ETCD_CLIENT, context=context, timeout=3)
        try:
            connection.request("GET", "/health")
            response = connection.getresponse()
            return response.status == 200 and json.loads(response.read()).get("health") == "true"
        except (OSError, http.client.HTTPException, ValueError):
            return False
        finally:
            connection.close()

    # --- Node configurations ---
    def control_store(self) -> str:
        endpoints = ", ".join(f'"https://{ip}:{ETCD_CLIENT}"' for ip in ETCD.values())
        return f"""[control_store]
backend = "etcd"
etcd_endpoints = [{endpoints}]
etcd_tls_ca_file = "{self.pki}/e2e-ca.pem"
etcd_tls_cert_file = "{self.pki}/etcd-client.pem"
etcd_tls_key_file = "{self.pki}/etcd-client.key"
config_poll_interval_seconds = 1
"""

    def node_config(self, cluster: str, node: str, ip: str, extra: str) -> str:
        # Cluster ka keeps a shard's members in separate racks.
        domain = 'failure_domain = "rack"' if cluster == SOURCE else ""
        rack = f'rack = "{SOURCE_NODES[node][1]}"' if cluster == SOURCE else ""
        return f"""[cluster]
cluster_id = "{cluster}"
{domain}

[node]
node_id = "{node}"
data_dir = "{self.dir}/{cluster}/{node}/data"
zone = "z1"
{rack}

[gateway]
listen = "{ip}:{GATEWAY}"

{self.control_store()}
[transport]
listen = "{ip}:{TRANSPORT}"
tls_cert_file = "{self.pki}/{cluster}-{node}.pem"
tls_key_file = "{self.pki}/{cluster}-{node}.key"
tls_ca_file = "{self.pki}/{cluster}-ca.pem"

[replication]
removal_interval_seconds = 5

[storage]
disk_min_free_bytes = 0

[cache]
# Keep clean payload whatever the free space of the machine's disk.
reserve_fraction = 0.0

[identity]
sts_web_identity = false

[identity.static_credentials.admin]
access_key_id = "{ADMIN_KEY}"
secret_access_key_file = "{self.dir}/admin-secret"
policy = '{ALLOW_ALL}'

[admin]
listen = "{ip}:{ADMIN}"
token_file = "{self.dir}/admin-token"

[logging]
filter = "info"
{extra}"""

    def define_source(self) -> None:
        for node, (ip, _rack) in SOURCE_NODES.items():
            text = self.node_config(SOURCE, node, ip, f"""
[buckets.defaults]
mode = "local"
shards_per_bucket = {SHARDS}
replicas = {REPLICAS}
min_write_replicas = 2

[peering]
quic_listen = "{ip}:{QUIC}"
peer_connect_timeout_ms = 2000

[peering.peers.{PEER}]
ca_file = "{self.pki}/{PEER}-ca.pem"
""")
            self.define_node(SOURCE, node, text)

    def define_peer(self, source_bucket_id: str) -> None:
        for node, ip in PEER_NODES.items():
            text = self.node_config(PEER, node, ip, f"""
[buckets.defaults]
mode = "local"
shards_per_bucket = {PEER_SHARDS}
replicas = {REPLICAS}
min_write_replicas = 2

[buckets.{INBOUND}]
peer_source = "{SOURCE}"

[identity.static_credentials.{SOURCE}]
access_key_id = "{PEER_KEY}"
secret_access_key_file = "{self.dir}/peer-secret"
policy = '{{"Version": "2012-10-17", "Statement": {{"Effect": "Allow", "Action": "s3:*", "Resource": ["arn:aws:s3:::{INBOUND}", "arn:aws:s3:::{INBOUND}/*"]}}}}'

[peering]
quic_listen = "{ip}:{QUIC}"
quic_advertise = ["{ip}:{QUIC}"]
peer_connect_timeout_ms = 2000

[peering.peers.{SOURCE}]
ca_file = "{self.pki}/{SOURCE}-ca.pem"
buckets = [{{ source = "{source_bucket_id}", destination = "{INBOUND}" }}]
s3_access_key_ids = ["{PEER_KEY}"]
""")
            self.define_node(PEER, node, text)

    def define_node(self, cluster: str, node: str, text: str) -> None:
        directory = os.path.join(self.dir, cluster, node)
        os.makedirs(directory, exist_ok=True)
        path = os.path.join(directory, "skys3.toml")
        with open(path, "w") as f:
            f.write(text)
        env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            # The flushers' credential, for S3 REST to the peer.
            "AWS_ACCESS_KEY_ID": PEER_KEY,
            "AWS_SECRET_ACCESS_KEY": PEER_SECRET,
            "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_CONFIG_FILE": os.path.join(self.dir, "no-aws-config"),
            "AWS_SHARED_CREDENTIALS_FILE": os.path.join(self.dir, "no-aws-credentials"),
        }
        self.procs.define(node, [self.binary, "--config", path], env)

    def start_node(self, node: str, timeout: float = 90) -> None:
        self.procs.start(node)
        deadline = time.time() + timeout
        while time.time() < deadline:
            health = self.admin.get(self.address(node), "/v1/health")
            if health and health.get("ready"):
                return
            if not self.procs.alive(node):
                raise SuiteError(f"node {node} exited during startup; see logs/{node}.log")
            time.sleep(0.2)
        raise SuiteError(f"node {node} is not ready after {timeout:.0f} s")

    # --- Cluster state ---
    def shards_of(self, node: str) -> list[dict] | None:
        answer = self.admin.get(self.address(node), "/v1/shards")
        return None if answer is None else answer.get("shards", [])

    def shard_map(self, nodes: list[str]) -> dict[tuple[str, int], dict]:
        """Each shard's newest configuration among the nodes' replicas."""
        merged: dict[tuple[str, int], dict] = {}
        for node in nodes:
            for entry in self.shards_of(node) or []:
                if entry.get("source") is None:
                    continue
                key = (entry["bucket"], entry["shard"])
                if key not in merged or entry["epoch"] > merged[key]["epoch"]:
                    merged[key] = entry
        return merged

    def primaries(self) -> dict[tuple[str, int], str]:
        return {key: entry["primary"] for key, entry in self.shard_map(self.source_up()).items()}

    def coordinator(self, nodes: list[str]) -> str | None:
        """The node every one of `nodes` names as lease holder, if it
        says it is coordinator."""
        holders = set()
        serving = []
        for node in nodes:
            view = self.admin.get(self.address(node), "/v1/cluster")
            if view is None:
                return None
            holders.add(view["coordinator"]["holder"])
            if view["coordinator"]["this_node"]:
                serving.append(node)
        if len(serving) == 1 and holders == {serving[0]}:
            return serving[0]
        return None

    def settled(self, nodes: list[str], expected: dict[str, int]) -> str | None:
        """None when every shard of `expected` (bucket -> shard count) has,
        in the newest configuration any node's replica knows, `replicas`
        members, all of them up, and no learner, each member holds a
        replica of that configuration that serves as primary or member, and
        the primary serves clients; otherwise what is not yet so. A node knows only the configurations
        of the shards it holds or routed to, so no node knows every one."""
        views: dict[str, dict[tuple[str, int], dict]] = {}
        for node in nodes:
            entries = self.shards_of(node)
            if entries is None:
                return f"{node} does not answer"
            views[node] = {(entry["bucket"], entry["shard"]): entry for entry in entries
                           if entry.get("source") == "replica"}
        for bucket, count in expected.items():
            for shard in range(count):
                held = [view[(bucket, shard)] for view in views.values() if (bucket, shard) in view]
                if not held:
                    return f"no node holds a replica of {bucket}/{shard}"
                newest = max(held, key=lambda entry: entry["epoch"])
                members, learners = newest["members"], newest["learners"]
                if len(members) != REPLICAS or learners:
                    return (f"{bucket}/{shard} at epoch {newest['epoch']}: "
                            f"members {members}, learners {learners}")
                for member in members:
                    if member not in views:
                        return f"{bucket}/{shard} names {member}, which is down"
                    entry = views[member].get((bucket, shard))
                    if (entry is None or entry["epoch"] != newest["epoch"]
                            or entry["role"] not in ("primary", "member")):
                        state = entry and (entry["epoch"], entry["role"])
                        return f"{member} does not serve {bucket}/{shard} at epoch {newest['epoch']}: {state}"
                # A primary reports its role as soon as it opens or takes
                # over, and serves clients only once it has reconciled its
                # members (`serving`); a stepped-down one never does again.
                primary = views[newest["primary"]][(bucket, shard)]
                if (primary["role"] != "primary" or not primary.get("serving")
                        or primary.get("stepped_down")):
                    return (f"{newest['primary']}, primary of {bucket}/{shard} at epoch {newest['epoch']}, "
                            f"does not serve yet (role {primary['role']}, serving {primary.get('serving')}, "
                            f"stepped down {primary.get('stepped_down')})")
        return None

    def wait(self, seconds: float, what: str, check) -> float:
        """Polls `check` (None when done, or what is missing) for up to
        `seconds`; returns the time it took."""
        started = time.time()
        last = None
        while time.time() - started < seconds:
            last = check()
            if last is None:
                return time.time() - started
            time.sleep(1)
        raise SuiteError(f"timed out after {seconds:.0f} s waiting until {what}: {last}")

    def source_settled(self) -> str | None:
        up = self.source_up()
        missing = sorted(self.expected_up - set(up))
        if missing:
            return f"down: {missing}"
        if self.coordinator(up) is None:
            return "no single coordinator"
        return self.settled(up, {LOCAL_BUCKET: SHARDS, PEERED_BUCKET: SHARDS})

    def create_bucket(self, node: str, bucket: str, target: str | None = None) -> None:
        headers = {}
        if target:
            headers = {"x-skys3-bucket-mode": "write_back", "x-skys3-bucket-target": target}
        client = S3()
        address = self.address(node)
        status, body, _, _ = client.call(lambda: (node, address), "PUT", f"/{bucket}", b"", headers)
        if status != 200:
            raise SuiteError(f"CreateBucket {bucket} through {node}: {status} {body[:300]!r}")


class SuiteError(Exception):
    pass


# --- The workload -----------------------------------------------------------------


@dataclass
class Op:
    thread: int
    bucket: str
    key: str
    version: int
    kind: str  # "put" or "delete"
    sha: str | None
    start: float
    end: float = 0.0
    status: int = 0
    node: str = ""
    attempts: int = 0

    @property
    def acked(self) -> bool:
        return self.status in (200, 204)


def body_of(bucket: str, key: str, version: int, size: int) -> bytes:
    header = f"{bucket}/{key}#{version}\n".encode()
    filler = random.Random(f"{bucket}/{key}/{version}").randbytes(max(0, size - len(header)))
    return header + filler


class Workload:
    """Writer threads, each the only writer of its keys, so the writes of
    one key never overlap and their order is the thread's."""

    def __init__(self, suite: Suite, seed: int, threads: int, ops_path: str) -> None:
        self.suite = suite
        self.seed = seed
        self.threads = threads
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.ops: list[Op] = []
        self.stale: list[dict] = []
        self.reads = 0
        self.reads_unavailable = 0
        self.out = open(ops_path, "w")
        self.workers: list[threading.Thread] = []

    def pick(self, rng: random.Random, avoid: str | None = None):
        def choose():
            clients = self.suite.clients()
            if avoid and len(clients) > 1:
                clients = [node for node in clients if node != avoid]
            if not clients:
                time.sleep(0.5)
                clients = list(FIRST_NODES)
            node = rng.choice(clients)
            return node, self.suite.address(node)
        return choose

    def record(self, op: Op) -> None:
        with self.lock:
            self.ops.append(op)
            self.out.write(json.dumps(op.__dict__) + "\n")

    def writer(self, thread: int) -> None:
        rng = random.Random(f"{self.seed}/writer/{thread}")
        client = S3()
        keys: list[tuple[str, str]] = []
        versions: dict[tuple[str, str], int] = {}
        live: dict[tuple[str, str], bool] = {}
        n = 0
        while not self.stop.is_set():
            if keys and rng.random() < 0.4:
                bucket, key = rng.choice(keys[-300:])
            else:
                bucket = PEERED_BUCKET if rng.random() < 0.2 else LOCAL_BUCKET
                key = f"t{thread:02d}/k{n:06d}"
                n += 1
                keys.append((bucket, key))
            version = versions.get((bucket, key), 0) + 1
            versions[(bucket, key)] = version
            delete = live.get((bucket, key), False) and rng.random() < 0.08
            size = rng.choice([64, 512, 1024, 4096, 16384]) if rng.random() < 0.95 else 200_000
            body = b"" if delete else body_of(bucket, key, version, size)
            op = Op(thread, bucket, key, version, "delete" if delete else "put",
                    None if delete else hashlib.sha256(body).hexdigest(), time.time())
            status, _, node, attempts = client.call(
                self.pick(rng), "DELETE" if delete else "PUT", f"/{bucket}/{key}", body)
            op.end, op.status, op.node, op.attempts = time.time(), status, node, attempts
            self.record(op)
            if op.acked:
                live[(bucket, key)] = not delete
                if rng.random() < 0.3:
                    self.read_after(client, rng, op)
            time.sleep(rng.uniform(0, 0.02))

    def read_after(self, client: S3, rng: random.Random, op: Op) -> None:
        """Reads an acknowledged write back through another node: no later
        write of the key has started, so only its content is right."""
        status, data, node, _ = client.call(self.pick(rng, avoid=op.node), "GET", f"/{op.bucket}/{op.key}")
        with self.lock:
            self.reads += 1
            if status == 0 or status >= 500:
                self.reads_unavailable += 1
                return
            got = hashlib.sha256(data).hexdigest() if status == 200 else None
            if status not in (200, 404) or got != op.sha:
                self.stale.append({
                    "bucket": op.bucket, "key": op.key, "version": op.version, "node": node,
                    "status": status, "expected": op.sha, "got": got, "at": time.time(),
                })

    def start(self) -> None:
        for thread in range(self.threads):
            worker = threading.Thread(target=self.writer, args=(thread,), daemon=True)
            worker.start()
            self.workers.append(worker)

    def finish(self) -> None:
        self.stop.set()
        for worker in self.workers:
            worker.join(120)
        self.out.close()


# --- The schedule -------------------------------------------------------------------

FAULTS = ["kill-primary", "kill-loop", "partition", "netem", "rack-loss",
          "coordinator-handoff", "etcd-loss"]
RACKS = sorted({rack for _ip, rack in SOURCE_NODES.values()})


def schedule(seed: int):
    """The seed's steps: n5's join first, then every fault once per round
    in a seeded order, each with its targets and durations drawn here, so
    that nothing the run observes changes them."""
    rng = random.Random(seed)
    yield {"kind": "join", "kill_after": round(rng.uniform(0, 3), 1), "down": round(rng.uniform(5, 15), 1)}
    nodes = sorted(SOURCE_NODES)
    while True:
        kinds = list(FAULTS)
        rng.shuffle(kinds)
        for kind in kinds:
            step: dict = {"kind": kind, "pause": round(rng.uniform(5, 15), 1)}
            if kind == "kill-primary":
                step.update(bucket=rng.choice([LOCAL_BUCKET, PEERED_BUCKET]), shard=rng.randrange(SHARDS),
                            down=round(rng.uniform(5, 30), 1))
            elif kind == "kill-loop":
                step["rounds"] = [{"node": rng.choice(nodes), "down": round(rng.uniform(1, 10), 1),
                                   "up": round(rng.uniform(2, 10), 1)} for _ in range(rng.randint(3, 6))]
            elif kind == "partition":
                step.update(bucket=rng.choice([LOCAL_BUCKET, PEERED_BUCKET]), shard=rng.randrange(SHARDS),
                            mode=rng.choice(["cluster", "full"]), hold=round(rng.uniform(10, 40), 1))
            elif kind == "netem":
                targets = rng.sample(nodes + sorted(ETCD), rng.randint(1, 2))
                step.update(targets=targets, delay=rng.choice([20, 50, 100, 150]),
                            jitter=rng.choice([0, 5, 20]), loss=rng.choice([0, 1, 3, 5]),
                            hold=round(rng.uniform(20, 60), 1))
            elif kind == "rack-loss":
                step.update(rack=rng.choice(RACKS), how=rng.choice(["kill", "isolate"]),
                            hold=round(rng.uniform(20, 60), 1))
            elif kind == "coordinator-handoff":
                step.update(bucket=rng.choice([LOCAL_BUCKET, PEERED_BUCKET]), shard=rng.randrange(SHARDS),
                            kill_after=round(rng.uniform(0, 1), 2), down=round(rng.uniform(5, 15), 1))
            elif kind == "etcd-loss":
                step.update(member=rng.choice(sorted(ETCD)), how=rng.choice(["kill", "isolate"]),
                            hold=round(rng.uniform(15, 45), 1))
            yield step


# --- Faults -------------------------------------------------------------------------


@dataclass
class Loss:
    """A primary's loss: when, why, and the shards it led. `at` is when
    the fault struck its first victim (a kill sent, or a partition's rules
    in place), not when the step began: until then the primary still
    answers, and a write acknowledged meanwhile measures nothing."""
    step: int
    kind: str
    at: float
    victims: list[str]
    shards: list[tuple[str, int]]
    struck: bool = False


class Faults:
    def __init__(self, suite: Suite) -> None:
        self.suite = suite
        self.losses: list[Loss] = []
        self.steps: list[dict] = []
        self.isolated: set[str] = set()
        self.shaped: set[str] = set()

    def lose(self, index: int, kind: str, victims: list[str]) -> None:
        primaries = self.suite.primaries()
        shards = sorted(key for key, primary in primaries.items() if primary in victims)
        self.losses.append(Loss(index, kind, time.time(), victims, shards))

    def struck(self, node: str) -> None:
        """Dates the latest loss of `node` from now, if not yet dated."""
        for loss in reversed(self.losses):
            if node in loss.victims:
                if not loss.struck:
                    loss.at, loss.struck = time.time(), True
                return

    def kill(self, node: str) -> None:
        with self.suite.lock:
            self.suite.unreachable.add(node)
        time.sleep(0.2)
        self.suite.procs.kill(node)
        self.struck(node)

    def restart(self, node: str) -> None:
        if node in ETCD:
            self.suite.procs.start(node)
            self.suite.wait(60, f"{node} is healthy", lambda: None if self.suite.etcd_healthy(node) else "unhealthy")
            return
        self.suite.start_node(node)
        with self.suite.lock:
            if node not in self.isolated:
                self.suite.unreachable.discard(node)

    def isolate(self, node: str, mode: str) -> None:
        if mode == "full":
            with self.suite.lock:
                self.suite.unreachable.add(node)
            time.sleep(0.2)
        run_net("isolate", node, mode)
        self.isolated.add(node)
        self.struck(node)

    def heal(self, node: str) -> None:
        run_net("heal", node)
        self.isolated.discard(node)
        with self.suite.lock:
            if self.suite.up(node) or node in ETCD:
                self.suite.unreachable.discard(node)

    def observe_partition(self, index: int, victim: str, step: dict) -> dict:
        """Holds a partition of `victim` for the step's hold and records
        what shows it took: the packets its rules dropped, whether the
        host (the clients) still reached it, and when the other nodes named
        another primary of the shard it led. A partition that dropped
        nothing fails the run."""
        suite = self.suite
        key = (step["bucket"], step["shard"])
        started = time.time()
        moved: float | None = None
        new_primary = None
        while time.time() - started < step["hold"]:
            if moved is None:
                others = [node for node in suite.source_up() if node != victim]
                primary = suite.shard_map(others).get(key, {}).get("primary")
                if primary is not None and primary != victim:
                    moved, new_primary = round(time.time() - started, 1), primary
            time.sleep(0.5)
        # A connection, not a request: the node cut off from the cluster is
        # cut off from etcd too, and its admin answers may wait on etcd.
        try:
            socket.create_connection((suite.address(victim), GATEWAY), timeout=3.0).close()
            reached = True
        except OSError:
            reached = False
        seen = {
            "node": victim,
            "mode": step["mode"],
            "dropped_packets": int(run_net("dropped", victim).strip() or 0),
            "host_reached": reached,
            "primary_moved_after": moved,
            "new_primary": new_primary,
        }
        log(f"step {index}: {victim} cut off: {seen}")
        if seen["dropped_packets"] == 0:
            raise SuiteError(f"the partition of {victim} dropped no packets")
        return seen

    def ask_handoff(self, bucket: str, shard: int) -> tuple[str, int, dict | None]:
        """Asks the coordinator to hand `shard` of `bucket` off, until it
        answers `202`, the handoff started: the step is about losing the
        coordinator in the middle of a change, so a refused or lost request
        would test only an ordinary coordinator loss. A `409` (a change of
        the shard in flight, or a coordinator that just changed), a `503`,
        or no answer is asked again, of whichever node is coordinator then,
        for up to HANDOFF_SECONDS; then the step fails. Returns the
        coordinator, the status, and its answer."""
        suite = self.suite
        path = f"/v1/shards/{bucket}/{shard}/handoff"
        deadline = time.time() + HANDOFF_SECONDS
        last = "no single coordinator"
        while time.time() < deadline:
            coordinator = suite.coordinator(suite.source_up())
            if coordinator is not None:
                status, answer = suite.admin.call(suite.address(coordinator), "POST", path)
                if status == 202:
                    return coordinator, status, answer
                last = f"{coordinator} answered {status or 'nothing'}: {answer}"
            time.sleep(1)
        raise SuiteError(f"the coordinator did not start a handoff of {bucket}/{shard} "
                         f"in {HANDOFF_SECONDS} s: {last}")

    def primary_of(self, bucket: str, shard: int) -> str:
        primary = self.suite.primaries().get((bucket, shard))
        if primary is None:
            raise SuiteError(f"no node names a primary of {bucket}/{shard}")
        return primary

    def run(self, index: int, step: dict) -> dict:
        kind = step["kind"]
        done: dict = {"index": index, **step, "started": time.time()}
        # In the report even if the step fails.
        self.steps.append(done)
        suite = self.suite
        if kind == "join":
            coordinator = suite.coordinator(suite.source_up())
            suite.expected_up.add(JOINING)
            suite.start_node(JOINING)
            # Kill the coordinator while it moves shards onto n5.
            def moving():
                view = suite.admin.get(suite.address(coordinator), "/v1/cluster")
                if view is None:
                    return "no answer"
                held = {node["node_id"]: node for node in view["nodes"]}.get(JOINING, {})
                return None if (held.get("members") or 0) + (held.get("learners") or 0) > 0 else "nothing moved yet"
            suite.wait(120, f"{coordinator} moves shards onto {JOINING}", moving)
            time.sleep(step["kill_after"])
            done["target"] = coordinator
            self.lose(index, kind, [coordinator])
            self.kill(coordinator)
            log(f"step {index}: killed the coordinator {coordinator} while it moved shards onto {JOINING}")
            time.sleep(step["down"])
            self.restart(coordinator)
            def share():
                view = suite.admin.get(suite.address(JOINING), "/v1/cluster") or {"nodes": []}
                held = {node["node_id"]: node for node in view["nodes"]}.get(JOINING, {})
                return None if (held.get("members") or 0) >= 3 else f"{JOINING} holds {held.get('members')} members"
            suite.wait(HEAL_SECONDS, f"{JOINING} holds its share", share)
        elif kind == "kill-primary":
            victim = self.primary_of(step["bucket"], step["shard"])
            done["target"] = victim
            self.lose(index, kind, [victim])
            self.kill(victim)
            log(f"step {index}: killed {victim}, primary of {step['bucket']}/{step['shard']}")
            time.sleep(step["down"])
            self.restart(victim)
        elif kind == "kill-loop":
            done["targets"] = []
            for round_ in step["rounds"]:
                victim = round_["node"]
                done["targets"].append(victim)
                self.lose(index, kind, [victim])
                self.kill(victim)
                time.sleep(round_["down"])
                self.restart(victim)
                time.sleep(round_["up"])
            log(f"step {index}: killed and restarted {', '.join(done['targets'])}")
        elif kind == "partition":
            victim = self.primary_of(step["bucket"], step["shard"])
            done["target"] = victim
            self.lose(index, kind, [victim])
            self.isolate(victim, step["mode"])
            log(f"step {index}: partitioned {victim}, primary of {step['bucket']}/{step['shard']} ({step['mode']})")
            done["cut_off"] = self.observe_partition(index, victim, step)
            self.heal(victim)
        elif kind == "netem":
            for target in step["targets"]:
                run_net("shape", target, str(step["delay"]), str(step["jitter"]), str(step["loss"]))
                self.shaped.add(target)
            log(f"step {index}: shaped {', '.join(step['targets'])}: {step['delay']}±{step['jitter']} ms, "
                f"{step['loss']}% loss" + ("" if suite.netem else " (no netem: loss only, by iptables)"))
            time.sleep(step["hold"])
            for target in step["targets"]:
                run_net("unshape", target)
                self.shaped.discard(target)
        elif kind == "rack-loss":
            victims = [node for node, (_ip, rack) in SOURCE_NODES.items() if rack == step["rack"]]
            done["targets"] = victims
            self.lose(index, kind, victims)
            for victim in victims:
                self.kill(victim) if step["how"] == "kill" else self.isolate(victim, "full")
            log(f"step {index}: lost rack {step['rack']} ({', '.join(victims)}, {step['how']})")
            time.sleep(step["hold"])
            for victim in victims:
                self.restart(victim) if step["how"] == "kill" else self.heal(victim)
        elif kind == "coordinator-handoff":
            coordinator, status, answer = self.ask_handoff(step["bucket"], step["shard"])
            done.update(target=coordinator, handoff=status, answer=answer)
            time.sleep(step["kill_after"])
            self.lose(index, kind, [coordinator])
            self.kill(coordinator)
            log(f"step {index}: asked {coordinator} to hand {step['bucket']}/{step['shard']} off ({status}), "
                f"killed it {step['kill_after']} s later")
            time.sleep(step["down"])
            self.restart(coordinator)
        elif kind == "etcd-loss":
            member = step["member"]
            done["target"] = member
            if step["how"] == "kill":
                suite.procs.kill(member)
            else:
                run_net("isolate", member, "full")
            log(f"step {index}: lost etcd member {member} ({step['how']})")
            time.sleep(step["hold"])
            if step["how"] == "kill":
                self.restart(member)
            else:
                run_net("heal", member)
        done["faulted_until"] = time.time()
        healed = suite.wait(HEAL_SECONDS, "the cluster heals", suite.source_settled)
        done["heal_seconds"] = round(healed, 1)
        log(f"step {index}: {kind} healed {healed:.1f} s after the fault ended")
        return done

    def heal_all(self) -> None:
        for node in list(self.isolated):
            self.heal(node)
        for node in list(self.shaped):
            run_net("unshape", node)
            self.shaped.discard(node)
        for node in ETCD:
            if not self.suite.up(node):
                self.restart(node)
        for node in self.suite.expected_up:
            if not self.suite.up(node):
                self.restart(node)


# --- The checker --------------------------------------------------------------------


class Checker:
    def __init__(self, suite: Suite, workload: Workload, faults: Faults) -> None:
        self.suite = suite
        self.workload = workload
        self.faults = faults
        self.violations: list[str] = []

    def histories(self, bucket: str) -> dict[str, list[Op]]:
        keys: dict[str, list[Op]] = {}
        for op in self.workload.ops:
            if op.bucket == bucket:
                keys.setdefault(op.key, []).append(op)
        for ops in keys.values():
            ops.sort(key=lambda op: op.version)
        return keys

    @staticmethod
    def allowed(ops: list[Op]) -> set[str | None] | None:
        """The contents the key may hold: that of its last acknowledged
        write, or of any later one, which may have applied. None when no
        write of the key was acknowledged."""
        last = max((i for i, op in enumerate(ops) if op.acked), default=None)
        if last is None:
            return None
        return {op.sha for op in ops[last:]}

    def read_all(self, bucket: str, nodes: list[str], label: str,
                 read_from: str | None = None) -> tuple[int, list[str]]:
        """Reads every key of `bucket` with an acknowledged write, from
        bucket `read_from` (by default `bucket` itself) through the nodes in
        turn, 16 at a time; returns the keys checked and the violations."""
        path = read_from or bucket
        histories = {key: ops for key, ops in self.histories(bucket).items() if self.allowed(ops) is not None}
        keys = sorted(histories)
        violations: list[str] = []
        lock = threading.Lock()
        position = [0]
        # The read-back checks what the cluster holds, not that it answers
        # at once: a key whose shard is unavailable for a moment, as during
        # a planned handoff that rebalancing starts after the faults heal
        # (design 5.4), is read again until one deadline for the whole
        # read-back. A shard unavailable past it is a violation.
        patience = time.time() + READ_BACK_PATIENCE_SECONDS

        def reader() -> None:
            client = S3()
            while True:
                with lock:
                    if position[0] >= len(keys):
                        return
                    i = position[0]
                    position[0] += 1
                key = keys[i]
                node = nodes[i % len(nodes)]
                address = self.suite.address(node)
                while True:
                    status, data, _, _ = client.call(lambda n=node, a=address: (n, a), "GET", f"/{path}/{key}")
                    if (status and status < 500) or time.time() >= patience:
                        break
                    time.sleep(1)
                got = hashlib.sha256(data).hexdigest() if status == 200 else None
                allowed = self.allowed(histories[key])
                if status not in (200, 404) or got not in allowed:
                    last = [op for op in histories[key] if op.acked][-1]
                    with lock:
                        violations.append(
                            f"{label} {bucket}/{key} through {node}: {status} {got and got[:12]}, "
                            f"acknowledged v{last.version} ({last.kind}) of {len(histories[key])}")

        threads = [threading.Thread(target=reader) for _ in range(16)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        return len(keys), violations

    def converged(self) -> str | None:
        """None once the members of every shard applied the same records."""
        positions: dict[tuple[str, int], dict[str, str]] = {}
        for node in self.suite.source_up():
            for entry in self.suite.shards_of(node) or []:
                if entry.get("source") == "replica" and entry["role"] in ("primary", "member"):
                    positions.setdefault((entry["bucket"], entry["shard"]), {})[node] = entry.get("applied")
        for (bucket, shard), applied in sorted(positions.items()):
            if len(applied) != REPLICAS or len(set(applied.values())) != 1:
                return f"{bucket}/{shard}: {applied}"
        return None

    def failovers(self) -> dict:
        """Per loss and shard, the time to the first acknowledged write on
        the shard among those that started after the loss."""
        writes: dict[tuple[str, int], list[Op]] = {}
        for op in self.workload.ops:
            if op.acked:
                shard = shard_of(self.suite.bucket_ids[op.bucket], op.key, SHARDS)
                writes.setdefault((op.bucket, shard), []).append(op)
        samples: dict[str, list[float]] = {}
        missing = []
        for loss in self.faults.losses:
            for shard in loss.shards:
                after = [op.end for op in writes.get(shard, []) if op.start >= loss.at]
                if not after:
                    missing.append(f"{loss.kind} (step {loss.step}) {shard[0]}/{shard[1]}")
                    continue
                samples.setdefault(loss.kind, []).append(min(after) - loss.at)
        every = [value for values in samples.values() for value in values]
        return {
            "all": distribution(every),
            "by_fault": {kind: distribution(values) for kind, values in sorted(samples.items())},
            "samples": {kind: [round(v, 3) for v in values] for kind, values in sorted(samples.items())},
            "no_write_after": missing,
        }


# --- run ----------------------------------------------------------------------------


def run(args: argparse.Namespace) -> int:
    seed = args.seed if args.seed is not None else random.SystemRandom().randrange(2**32)
    log(f"seed {seed} (replay: SKYS3_CHAOS_SEED={seed} SKYS3_CHAOS_MINUTES={args.minutes} tests/e2e/chaos/run.sh)")
    suite = Suite(args.dir, args.binary, args.etcd, args.netem)

    # A cancelled job (SIGTERM) still stops the processes and writes what
    # it has of the report.
    def terminated(_signal, _frame):
        raise SuiteError("terminated")

    signal.signal(signal.SIGTERM, terminated)
    workload: Workload | None = None
    faults = Faults(suite)
    report: dict = {"seed": seed, "minutes": args.minutes, "netem": args.netem, "failed": [], "steps": faults.steps}
    started = time.time()
    try:
        # etcd, then ka's first four nodes and its buckets, then kb.
        suite.define_etcd()
        for member in ETCD:
            suite.procs.start(member)
        for member in ETCD:
            suite.wait(60, f"{member} is healthy", lambda m=member: None if suite.etcd_healthy(m) else "unhealthy")
        log("etcd is up: " + ", ".join(f"{name} at {ip}" for name, ip in ETCD.items()))
        suite.define_source()
        for node in FIRST_NODES:
            suite.expected_up.add(node)
            suite.start_node(node)
        suite.wait(90, "ka elects a coordinator", lambda: None if suite.coordinator(FIRST_NODES) else "none yet")
        suite.create_bucket("n1", LOCAL_BUCKET)
        suite.create_bucket("n2", PEERED_BUCKET, f"http://{PEER_NODES['p1']}:{GATEWAY}/{INBOUND}")
        suite.wait(90, "ka places every shard", suite.source_settled)
        for entry in suite.shard_map(FIRST_NODES).values():
            suite.bucket_ids[entry["bucket"]] = entry["bucket_id"]
        log(f"ka serves {LOCAL_BUCKET} and {PEERED_BUCKET} ({suite.bucket_ids}); coordinator {suite.coordinator(FIRST_NODES)}")
        suite.define_peer(suite.bucket_ids[PEERED_BUCKET])
        for node in PEER_NODES:
            suite.start_node(node)
        suite.wait(90, "kb elects a coordinator", lambda: None if suite.coordinator(list(PEER_NODES)) else "none yet")
        suite.create_bucket("p1", INBOUND)
        suite.wait(90, "kb places every shard", lambda: suite.settled(list(PEER_NODES), {INBOUND: PEER_SHARDS}))
        log("kb serves inbound")

        workload = Workload(suite, seed, args.threads, os.path.join(args.dir, "ops.jsonl"))
        workload.start()
        time.sleep(20)
        log(f"the workload runs: {len(workload.ops)} writes so far")

        deadline = started + args.minutes * 60
        for index, step in enumerate(schedule(seed)):
            if index > 0 and time.time() + 60 > deadline:
                break
            log(f"step {index}: {json.dumps(step)}")
            faults.run(index, step)
            time.sleep(step.get("pause", 5))
    except SuiteError as error:
        log(f"FAILED: {error}")
        report["failed"].append(str(error))
    except Exception as error:  # noqa: BLE001 - reported, then the run ends
        log(f"FAILED: {type(error).__name__}: {error}")
        report["failed"].append(f"{type(error).__name__}: {error}")

    try:
        if workload is not None:
            workload.finish()
            log(f"the workload stopped: {len(workload.ops)} writes")
            faults.heal_all()
            try:
                suite.wait(HEAL_SECONDS, "the cluster heals", suite.source_settled)
            except SuiteError as error:
                # Checked all the same: what reads back, and what does not.
                log(f"FAILED: {error}")
                report["failed"].append(str(error))
            checker = Checker(suite, workload, faults)
            report.update(check(suite, workload, faults, checker, report["failed"]))
    except SuiteError as error:
        log(f"FAILED: {error}")
        report["failed"].append(str(error))
    finally:
        suite.procs.stop_all()
    report["seconds"] = round(time.time() - started)
    write_report(args.dir, report)
    return 1 if report["failed"] or report.get("violations") else 0


def check(suite: Suite, workload: Workload, faults: Faults, checker: Checker,
          failed: list[str]) -> dict:
    """Every check of the run's end; one that cannot finish (a wait that
    times out) is added to `failed`, and the others still run."""
    ops = workload.ops
    acked = [op for op in ops if op.acked]
    latencies = [op.end - op.start for op in acked]
    result: dict = {
        "writes": {"total": len(ops), "acknowledged": len(acked),
                   "failed": sum(1 for op in ops if op.status and not op.acked),
                   "unanswered": sum(1 for op in ops if not op.status),
                   "deletes": sum(1 for op in ops if op.kind == "delete"),
                   "latency": distribution(latencies)},
        "reads_after_ack": {"reads": workload.reads, "unavailable": workload.reads_unavailable,
                            "stale": len(workload.stale)},
    }
    violations = [f"stale read: {json.dumps(read)}" for read in workload.stale]
    # Without acknowledged writes the read-back, convergence, and peering
    # checks below pass with nothing to check.
    for bucket in (LOCAL_BUCKET, PEERED_BUCKET):
        count = sum(1 for op in acked if op.bucket == bucket)
        result["writes"].setdefault("acknowledged_by_bucket", {})[bucket] = count
        if count < MIN_ACKED_WRITES:
            error = (f"only {count} writes to {bucket} were acknowledged, "
                     f"fewer than the {MIN_ACKED_WRITES} the checks need")
            log(f"FAILED: {error}")
            failed.append(error)
    result["failover"] = checker.failovers()

    def waited(seconds: float, what: str, until) -> float | None:
        try:
            return suite.wait(seconds, what, until)
        except SuiteError as error:
            log(f"FAILED: {error}")
            failed.append(str(error))
            return None

    took = waited(120, "the members of every shard applied the same records", checker.converged)
    result["converged_seconds"] = took and round(took, 1)
    if took is not None:
        log(f"every shard's members applied the same records ({took:.1f} s)")

    up = suite.source_up()
    for bucket in (LOCAL_BUCKET, PEERED_BUCKET):
        count, wrong = checker.read_all(bucket, up, "read-back")
        result.setdefault("read_back", {})[bucket] = count
        violations += wrong
        log(f"{count} keys of {bucket} read back through {', '.join(up)}: {len(wrong)} wrong")

    def flushed() -> str | None:
        for node in up:
            status = suite.admin.get(suite.address(node), f"/v1/buckets/{PEERED_BUCKET}")
            if status is None:
                return f"{node} does not answer"
            flush = status.get("flush") or {}
            if status.get("unflushed") or flush.get("dirty") or flush.get("flushing"):
                return f"{node}: {status.get('unflushed')} unflushed, transport {flush.get('transport')}"
        return None
    took = waited(PEER_ARRIVAL_SECONDS, f"{PEERED_BUCKET} is flushed to kb", flushed)
    transports = {}
    for node in up:
        status = suite.admin.get(suite.address(node), f"/v1/buckets/{PEERED_BUCKET}") or {}
        transports[node] = ((status.get("flush") or {}).get("transport") or {}).get("in_use")
    count, wrong = checker.read_all(PEERED_BUCKET, list(PEER_NODES), "peer", INBOUND)
    violations += wrong
    result["peering"] = {"keys": count, "flushed_after_seconds": took and round(took, 1),
                         "transport": transports, "missing_or_wrong": len(wrong)}
    log(f"{PEERED_BUCKET} flushed after {took} s; {count} keys checked on kb, "
        f"{len(wrong)} wrong; transports {transports}")
    result["violations"] = violations
    for violation in violations[:20]:
        log(f"VIOLATION: {violation}")
    return result


def seconds(value: float | None) -> str:
    return "-" if value is None else f"{value:.2f}"


def write_report(directory: str, report: dict) -> None:
    with open(os.path.join(directory, "report.json"), "w") as f:
        json.dump(report, f, indent=2, default=str)
    lines = [
        "## Kill, partition, and peering suite",
        "",
        f"Seed **{report['seed']}**, {report['minutes']} minutes of faults, "
        f"{report.get('seconds', 0)} s in all, netem {'on' if report['netem'] else 'unavailable (loss by iptables, no delay)'}. "
        f"Replay: `SKYS3_CHAOS_SEED={report['seed']} SKYS3_CHAOS_MINUTES={report['minutes']} tests/e2e/chaos/run.sh`.",
        "",
    ]
    ok = not report["failed"] and not report.get("violations")
    lines.append("**Result: " + ("passed" if ok else "FAILED") + "**")
    lines.append("")
    for failure in report["failed"]:
        lines.append(f"- failed: {failure}")
    for violation in (report.get("violations") or [])[:20]:
        lines.append(f"- violation: {violation}")
    writes = report.get("writes")
    if writes:
        lines += [
            "",
            f"Writes: {writes['acknowledged']} acknowledged of {writes['total']} "
            f"({writes['failed']} failed, {writes['unanswered']} unanswered, {writes['deletes']} deletes); "
            f"latency p50 {seconds(writes['latency']['p50'])} s, p99 {seconds(writes['latency']['p99'])} s. "
            f"Reads after acknowledgment: {report['reads_after_ack']['reads']}, "
            f"{report['reads_after_ack']['stale']} stale. Read back: {report.get('read_back')}. "
            f"Peering: {report.get('peering')}.",
        ]
    failover = report.get("failover")
    if failover:
        lines += ["", "### Failover time (primary lost to first acknowledged write on its shard), seconds", "",
                  "| Fault | Samples | p50 | p90 | p99 | max |", "|---|---:|---:|---:|---:|---:|"]
        rows = list(failover["by_fault"].items()) + [("all", failover["all"])]
        for kind, d in rows:
            lines.append(f"| {kind} | {d['count']} | {seconds(d['p50'])} | {seconds(d['p90'])} | "
                         f"{seconds(d['p99'])} | {seconds(d['max'])} |")
        if failover["no_write_after"]:
            lines.append("")
            lines.append(f"No acknowledged write after: {', '.join(failover['no_write_after'][:10])}")
    partitions = [step for step in report["steps"] if step.get("cut_off")]
    if partitions:
        lines += ["", "### Partitions", "",
                  "| # | Node | Mode | Packets dropped | Host reached it | Primary moved after (s) | Healed after (s) |",
                  "|---:|---|---|---:|---|---:|---:|"]
        for step in partitions:
            seen = step["cut_off"]
            moved = "-" if seen["primary_moved_after"] is None else f"{seen['primary_moved_after']} (to {seen['new_primary']})"
            lines.append(f"| {step['index']} | {seen['node']} | {seen['mode']} | {seen['dropped_packets']} | "
                         f"{'yes' if seen['host_reached'] else 'no'} | {moved} | {step.get('heal_seconds', '-')} |")
    if report["steps"]:
        lines += ["", "### Steps", "", "| # | Fault | Target | Healed after (s) |", "|---:|---|---|---:|"]
        for step in report["steps"]:
            target = step.get("target") or ", ".join(step.get("targets") or []) or step.get("rack", "")
            lines.append(f"| {step['index']} | {step['kind']} | {target} | {step.get('heal_seconds', '-')} |")
    text = "\n".join(lines) + "\n"
    with open(os.path.join(directory, "report.md"), "w") as f:
        f.write(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as f:
            f.write(text)
    print(text, flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    r = commands.add_parser("run")
    r.add_argument("--dir", required=True)
    r.add_argument("--binary", required=True)
    r.add_argument("--etcd", required=True)
    r.add_argument("--seed", type=int)
    r.add_argument("--minutes", type=float, default=30)
    r.add_argument("--threads", type=int, default=8)
    r.add_argument("--netem", action=argparse.BooleanOptionalAction, default=True)
    s = commands.add_parser("schedule")
    s.add_argument("--seed", type=int, required=True)
    s.add_argument("--steps", type=int, default=15)
    args = parser.parse_args()
    if args.command == "schedule":
        for index, step in enumerate(itertools.islice(schedule(args.seed), args.steps)):
            print(f"{index}: {json.dumps(step)}")
        return 0
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
