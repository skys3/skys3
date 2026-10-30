"""The cluster under test, as a target file describes it.

The harness does not launch nodes itself: whatever started the cluster
(the end-to-end suite's scenario `98-perf-smoke.sh`, or an operator on the
release hardware) writes a JSON target file, and the harness drives the
cluster through it. The file names each node's S3 endpoint and admin
listener, the credentials, the remote S3 store that write_back buckets
flush to, and the commands (hooks) that kill and start a node and shape a
link, such as tests/e2e/chaos/net.sh's netem. docs/skys3-performance.md
gives its format.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field

from .s3 import Credentials

HOOKS = ("kill", "start", "shape", "unshape")


class TargetError(ValueError):
    """A target file that does not describe a cluster the harness can use."""


def shard_of(bucket_id: str, key: str, shards: int) -> int:
    """The shard of `key` (design section 4.1, the frozen hash of
    skys3-types `KeyHash`; tests/e2e/chaos/chaos.py has the same)."""
    digest = hashlib.sha256(
        b"skys3-shard-v1\0" + bucket_id.encode() + b"\0" + key.encode()).digest()
    return int.from_bytes(digest[:8], "big") % shards


@dataclass
class Node:
    name: str
    s3: str
    admin: str


@dataclass
class Target:
    name: str
    nodes: list[Node]
    credentials: Credentials
    ca_file: str | None = None
    admin_token: str | None = None
    remote: dict | None = None
    read_buckets: dict[str, str] = field(default_factory=dict)
    cluster: dict = field(default_factory=dict)
    hooks: dict[str, list[str]] = field(default_factory=dict)
    links: list[str] = field(default_factory=list)
    data_dir: str | None = None
    down: set[str] = field(default_factory=set)

    @staticmethod
    def load(path: str) -> Target:
        with open(path) as f:
            return Target.parse(json.load(f))

    @staticmethod
    def parse(doc: dict) -> Target:
        try:
            nodes = [Node(n["name"], n["s3"].rstrip("/"), n["admin"]) for n in doc["nodes"]]
            s3 = doc["s3"]
            credentials = Credentials(s3["access_key"], s3["secret_key"],
                                      s3.get("region", "us-east-1"))
        except (KeyError, TypeError) as error:
            raise TargetError(f"the target file lacks {error}") from None
        if not nodes:
            raise TargetError("the target file names no node")
        if len({n.name for n in nodes}) != len(nodes):
            raise TargetError("two nodes of the target file have the same name")
        hooks = doc.get("hooks", {})
        for name, argv in hooks.items():
            if name not in HOOKS or not isinstance(argv, list) or not argv:
                raise TargetError(f"hook {name!r} is not one of {HOOKS} with a command list")
        remote = doc.get("remote")
        if remote is not None and not {"url", "access_key", "secret_key"} <= set(remote):
            raise TargetError("remote needs url, access_key, and secret_key")
        return Target(
            name=doc.get("name", "cluster"), nodes=nodes, credentials=credentials,
            ca_file=s3.get("ca_file"), admin_token=doc.get("admin_token"), remote=remote,
            read_buckets={str(k): v for k, v in doc.get("read_buckets", {}).items()},
            cluster=doc.get("cluster", {}), hooks=hooks, links=doc.get("links", []),
            data_dir=doc.get("data_dir"))

    # --- Nodes ---------------------------------------------------------------

    def node(self, name: str) -> Node:
        for node in self.nodes:
            if node.name == name:
                return node
        raise TargetError(f"no node {name}")

    def live(self) -> list[Node]:
        return [n for n in self.nodes if n.name not in self.down]

    def admins(self) -> dict[str, str]:
        return {n.name: n.admin for n in self.nodes}

    def remote_credentials(self) -> Credentials:
        assert self.remote is not None
        return Credentials(self.remote["access_key"], self.remote["secret_key"],
                           self.remote.get("region", "us-east-1"))

    # --- Admin API -----------------------------------------------------------

    def admin(self, node: str, path: str, timeout: float = 5.0) -> dict | None:
        """The JSON answer of an admin API GET, or None if the node does not
        answer it."""
        request = urllib.request.Request(f"http://{self.node(node).admin}{path}")
        if self.admin_token:
            request.add_header("Authorization", f"Bearer {self.admin_token}")
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.loads(response.read())
        except (OSError, urllib.error.URLError, ValueError):
            return None

    def shards(self, bucket: str) -> dict[int, dict]:
        """Each shard of the bucket in the newest configuration any live
        node reports (a node lists only the shards it holds or routed to)."""
        merged: dict[int, dict] = {}
        for node in self.live():
            answer = self.admin(node.name, "/v1/shards") or {}
            for entry in answer.get("shards", []):
                if entry.get("bucket") != bucket or entry.get("source") is None:
                    continue
                known = merged.get(entry["shard"])
                if known is None or entry.get("epoch", 0) > known.get("epoch", 0):
                    merged[entry["shard"]] = entry
        return merged

    def whole(self, bucket: str, replicas: int) -> str | None:
        """None when every shard of the bucket has `replicas` members, all
        of them up, no learner, and a serving primary; else what is not so."""
        shards = self.shards(bucket)
        if not shards:
            return f"no live node reports a shard of {bucket}"
        for shard, entry in sorted(shards.items()):
            members = entry.get("members", [])
            if len(members) != replicas or entry.get("learners"):
                return f"shard {shard}: members {members}, learners {entry.get('learners')}"
            if set(members) & self.down:
                return f"shard {shard} names a node that is down: {members}"
        return None

    # --- Hooks ---------------------------------------------------------------

    def has_hook(self, name: str) -> bool:
        return name in self.hooks

    def run_hook(self, name: str, timeout: float = 120.0, **values: object) -> None:
        """Runs a hook, its `{name}` placeholders replaced by `values`."""
        if name not in self.hooks:
            raise TargetError(f"the target file has no {name} hook")
        argv = [arg.format(**values) for arg in self.hooks[name]]
        subprocess.run(argv, check=True, timeout=timeout, env=dict(os.environ))

    def kill(self, node: str) -> None:
        """Kills the node at once (SIGKILL), and counts it down."""
        self.down.add(node)
        self.run_hook("kill", node=node)

    def start(self, node: str, ready_timeout: float = 120.0) -> None:
        """Starts the node, and waits until it reports ready."""
        self.run_hook("start", timeout=ready_timeout, node=node)
        deadline = time.time() + ready_timeout
        while time.time() < deadline:
            health = self.admin(node, "/v1/health")
            if health and health.get("ready"):
                self.down.discard(node)
                return
            time.sleep(0.2)
        raise TargetError(f"node {node} is not ready {ready_timeout:.0f} s after its start")

    def shape(self, link: str, rtt_ms: float, loss_pct: float = 0.0) -> None:
        """Gives `link` a round trip of `rtt_ms` (half of it each way) and a
        loss rate."""
        self.run_hook("shape", link=link, rtt_ms=f"{rtt_ms:g}", delay_ms=f"{rtt_ms / 2:g}",
                      loss_pct=f"{loss_pct:g}")

    def unshape(self, link: str) -> None:
        self.run_hook("unshape", link=link)

    def can_shape(self, link: str) -> bool:
        return link in self.links and self.has_hook("shape") and self.has_hook("unshape")
