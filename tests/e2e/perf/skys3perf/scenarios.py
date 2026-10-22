"""The scenarios: one per measurement of design section 16.3, each a
function that drives the cluster and returns its cases.

A case is one point of the scenario's matrix (an object size and a
concurrency, a round trip, a kill): its labels, its distributions, its
values, and the gauges' course over it. A scenario raises `Skip` when the
target lacks what it needs (a remote store, kill hooks), and an extension
point, a row this harness does not measure yet, reports `not_implemented`
rather than a number.
"""

from __future__ import annotations

import contextlib
import math
import threading
import time
import urllib.parse
from dataclasses import dataclass, field
from collections.abc import Callable

from . import metrics, stats
from .load import Generator, Task
from .params import format_size
from .relay import Relay
from .s3 import Client, Payload
from .target import Target, shard_of


class Skip(Exception):
    """The target lacks what the scenario needs; the message says what."""


@dataclass
class Context:
    target: Target
    params: dict
    generator: Generator
    run_id: str
    log: Callable[[str], None] = print
    _turn: int = 0

    def client(self, node: str | None = None) -> Client:
        """A client of `node`'s gateway, or of the live nodes' in turn."""
        live = self.target.live()
        chosen = self.target.node(node) if node else live[self._turn % len(live)]
        self._turn += 1
        return Client(chosen.s3, self.target.credentials, self.target.ca_file,
                      self.params["request_timeout"])

    def task(self, op: str, bucket: str, **fields) -> Task:
        endpoints = fields.pop("endpoints", None) or tuple(n.s3 for n in self.target.live())
        return Task(op=op, endpoints=tuple(endpoints), credentials=self.target.credentials,
                    bucket=bucket, ca_file=self.target.ca_file,
                    timeout=self.params["request_timeout"], **fields)

    def snapshot(self) -> metrics.Snapshot:
        return metrics.snapshot(self.target.admins(), self.target.admin_token)

    def sampler(self) -> metrics.Sampler:
        return metrics.Sampler(self.target.admins(), self.target.admin_token,
                               self.params["metrics_interval"])

    def summarize(self, samples: list[float], unit: str = "seconds") -> dict:
        return stats.summarize(samples, unit, self.params["samples_max"])

    def ready(self, bucket: str, timeout: float = 60.0) -> None:
        """Waits until the bucket takes a PUT: its shards are placed and
        their primaries serve."""
        deadline = time.time() + timeout
        status = 0
        while time.time() < deadline:
            status = self.client().put(bucket, f".perf-probe-{self.run_id}", Payload(1))
            if status == 200:
                return
            time.sleep(0.5)
        raise RuntimeError(f"bucket {bucket} took no PUT within {timeout:.0f} s (last {status})")

    def bucket_status(self, bucket: str) -> list[dict]:
        """The admin API's status of the bucket from every live node that
        answers it."""
        quoted = urllib.parse.quote(bucket, safe="")
        answers = [self.target.admin(n.name, f"/v1/buckets/{quoted}") for n in self.target.live()]
        return [a for a in answers if a]

    def flushed(self, bucket: str) -> bool:
        answers = self.bucket_status(bucket)
        return bool(answers) and all(
            a.get("unflushed") == 0 and a.get("flush", {}).get("dirty") == 0
            and a.get("flush", {}).get("flushing") == 0 for a in answers)


def case(labels: dict, distributions: dict | None = None, values: dict | None = None,
         series: dict | None = None) -> dict:
    return {"labels": labels, "distributions": distributions or {}, "values": values or {},
            "series": series or {}}


def wait_until(check: Callable[[], bool], timeout: float, what: str, step: float = 0.5) -> float:
    """Polls `check` until it holds, and returns when it did; fails after
    `timeout` seconds."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if check():
            return time.time()
        time.sleep(step)
    raise RuntimeError(f"timed out after {timeout:.0f} s waiting until {what}")


def remote_client(ctx: Context) -> Client:
    if not ctx.target.remote:
        raise Skip("the target names no remote S3 store for write_back buckets")
    return Client(ctx.target.remote["url"], ctx.target.remote_credentials(),
                  timeout=ctx.params["request_timeout"])


# --- PUT ----------------------------------------------------------------------


def put(ctx: Context) -> list[dict]:
    """PUT latency by object size and concurrency, with the log counters
    of every node: fsyncs and group commits per acknowledged PUT, records
    per group commit, and a bound on the serial durable rounds, beside the
    counts design section 5.3 claims."""
    p = ctx.params
    bucket = p["put.bucket"]
    ctx.client().create_bucket(bucket)
    ctx.ready(bucket)
    replicas = ctx.target.cluster.get("replicas")
    cases = []
    for size in p["put.sizes"]:
        for concurrency in p["put.concurrency"]:
            ctx.log(f"put: {format_size(size)} objects, {concurrency} at a time")
            before = ctx.snapshot()
            outcome = ctx.generator.run(ctx.task(
                "put", bucket, prefix=f"{ctx.run_id}/{format_size(size)}/c{concurrency}/",
                size=size, duration=p["put.duration"], max_bytes=p["put.max_bytes"]),
                concurrency)
            after = ctx.snapshot()
            distributions, values = outcome.summary(p["samples_max"])
            values.update(log_values(before, after, values["succeeded"],
                                     distributions["latency"]["percentiles"]["p50"]))
            values["expected"] = {"sync_participants": replicas, "serial_durable_rounds": 1,
                                  "control_store_calls": 0}
            cases.append(case({"size_bytes": size, "size": format_size(size),
                               "concurrency": concurrency}, distributions, values))
    return cases


def log_values(before: metrics.Snapshot, after: metrics.Snapshot, acked: int,
               p50: float | None) -> dict:
    """What the nodes' logs did for `acked` PUTs (docs/skys3-metrics.md
    section 3.13). Every record counts, so markers and the extents of large
    objects raise the counts above section 5.3's one record per PUT."""
    def delta(name: str) -> float | None:
        return metrics.counter_delta(before, after, name)

    commits = delta("skys3_log_group_commits_total")
    records = delta("skys3_log_records_total")
    syncs = delta("skys3_log_syncs_total")
    seconds = delta("skys3_log_commit_seconds_total")
    mean_commit = stats.ratio(seconds, commits)
    return {
        "group_commits": commits,
        "log_records": records,
        "syncs": syncs,
        "written_bytes": delta("skys3_log_written_bytes_total"),
        "fsyncs_per_put": stats.ratio(syncs, acked),
        "group_commits_per_put": stats.ratio(commits, acked),
        "records_per_group_commit": stats.ratio(records, commits),
        "mean_group_commit_seconds": mean_commit,
        # A PUT takes at least its serial rounds times a round's time; the
        # rest of its latency (network, CPU) only raises this bound.
        "serial_durable_rounds_bound": stats.ratio(p50, mean_commit),
    }


# --- GET ----------------------------------------------------------------------


def get(ctx: Context) -> list[dict]:
    """GET throughput of one hot object, by clean_copies (each through the
    bucket the target configured with it) and by the number of gateways
    the clients spread over."""
    p = ctx.params
    remote = remote_client(ctx)
    cases = []
    for copies in p["get.clean_copies"]:
        bucket = ctx.target.read_buckets.get(str(copies))
        if bucket is None:
            cases.append(case({"clean_copies": copies}, values={
                "skipped": f"the target names no bucket with clean_copies = {copies}"}))
            continue
        remote_bucket = f"perf-{ctx.run_id}-{bucket}"
        remote.create_bucket(remote_bucket)
        ctx.client().create_bucket(bucket, f"{ctx.target.remote['url']}/{remote_bucket}")
        ctx.ready(bucket)
        key = f"{ctx.run_id}/hot"
        status = ctx.client().put(bucket, key, Payload(p["get.size"], seed=1))
        if status != 200:
            raise RuntimeError(f"the hot object's PUT answered {status}")
        # Clean copies beyond clean_copies are dropped once it is flushed.
        wait_until(lambda b=bucket: ctx.flushed(b), p["request_timeout"] * 4,
                   f"{bucket} is flushed")
        for gateways in p["get.gateways"]:
            live = ctx.target.live()
            labels = {"clean_copies": copies, "gateways": gateways}
            if gateways > len(live):
                cases.append(case(labels, values={"skipped": f"only {len(live)} nodes are up"}))
                continue
            concurrency = p["get.concurrency"] * gateways
            ctx.log(f"get: clean_copies {copies}, {gateways} gateways, {concurrency} clients")
            before = ctx.snapshot()
            outcome = ctx.generator.run(ctx.task(
                "get", bucket, endpoints=tuple(n.s3 for n in live[:gateways]), keys=(key,),
                duration=p["get.duration"]), concurrency)
            after = ctx.snapshot()
            distributions, values = outcome.summary(p["samples_max"])
            for name in ("skys3_hot_cache_hits_total", "skys3_hot_cache_misses_total"):
                values[name.removeprefix("skys3_").removesuffix("_total")] = \
                    metrics.counter_delta(before, after, name)
            values["fills"] = metrics.counter_delta(before, after, "skys3_fills_total",
                                                    bucket=bucket)
            cases.append(case({**labels, "size_bytes": p["get.size"]}, distributions, values))
    return cases


# --- Failover and the durability window ----------------------------------------


@dataclass
class Writers:
    """One writer per shard, each PUTting its shard's keys one at a time
    through the live gateways in turn, recording (shard, start, end,
    status) for every request."""

    ctx: Context
    bucket: str
    keys: dict[int, list[str]]
    size: int
    records: list[tuple[int, float, float, int]] = field(default_factory=list)
    _stop: threading.Event = field(default_factory=threading.Event)
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _threads: list[threading.Thread] = field(default_factory=list)

    def start(self) -> None:
        for shard, keys in self.keys.items():
            thread = threading.Thread(target=self._write, args=(shard, keys), daemon=True)
            thread.start()
            self._threads.append(thread)

    def stop(self) -> None:
        self._stop.set()
        for thread in self._threads:
            thread.join()

    def _write(self, shard: int, keys: list[str]) -> None:
        target = self.ctx.target
        clients: dict[str, Client] = {}
        payload = Payload(self.size, seed=2)
        n = 0
        while not self._stop.is_set():
            live = target.live()
            node = live[(n + shard) % len(live)]
            client = clients.get(node.name)
            if client is None:
                client = clients[node.name] = self.ctx.client(node.name)
            start = time.time()
            status = client.put(self.bucket, keys[n % len(keys)], payload)
            end = time.time()
            with self._lock:
                self.records.append((shard, start, end, status))
            n += 1
            if status != 200:
                time.sleep(0.05)
        for client in clients.values():
            client.close()

    def acks(self, shard: int, after: float, until: float = math.inf) -> list[tuple[float, float]]:
        """(start, end) of the shard's acknowledged writes that ended in
        (after, until]."""
        with self._lock:
            return sorted((s, e) for sh, s, e, status in self.records
                          if sh == shard and status == 200 and after < e <= until)

    def failed(self, after: float) -> int:
        with self._lock:
            return sum(1 for _, _, e, status in self.records if status != 200 and e > after)

    def latencies(self, after: float, until: float) -> list[float]:
        with self._lock:
            return [e - s for _, s, e, status in self.records
                    if status == 200 and after < e <= until]


def longest_gap(start: float, ends: list[float], until: float) -> float:
    """The longest time in [start, until] without an acknowledgement: the
    largest gap between start, the ends in between, and until."""
    points = [start] + [e for e in ends if start < e < until] + [until]
    return max(b - a for a, b in zip(points, points[1:], strict=False))


def failover(ctx: Context) -> list[dict]:
    """Kills the node that leads a shard (SIGKILL), at each control-store
    round trip, and measures each shard's recovery and longest write stall
    by the killed node's role in it, the durability window (until the
    shards are whole again after the node restarts), and the stall while it
    rejoins as a learner. Every kill strikes the primary of one shard and a
    backup of the others, so one kill measures both."""
    p = ctx.params
    target = ctx.target
    if not (target.has_hook("kill") and target.has_hook("start")):
        raise Skip("the target has no kill and start hooks")
    replicas = target.cluster.get("replicas", 0)
    if len(target.nodes) < 2 or replicas < 2:
        raise Skip("the cluster has one node, or its buckets one replica")
    bucket = p["failover.bucket"]
    ctx.client().create_bucket(bucket)
    ctx.ready(bucket)
    cases = []
    for rtt in p["failover.control_rtts_ms"]:
        shaped = rtt > 0
        if shaped and not target.can_shape("control_store"):
            cases.append(case({"control_rtt_ms": rtt}, values={
                "skipped": "the target cannot shape its control_store link"}))
            continue
        if shaped:
            target.shape("control_store", rtt)
        try:
            kills = [kill_once(ctx, bucket, replicas, rtt, k) for k in range(p["failover.kills"])]
        finally:
            if shaped:
                target.unshape("control_store")
        cases.extend(kills)
        if len(kills) > 1:
            cases.append(merge_kills(ctx, rtt, kills))
    return cases


def kill_once(ctx: Context, bucket: str, replicas: int, rtt: float, kill: int) -> dict:
    p = ctx.params
    target = ctx.target
    wait_until(lambda: target.whole(bucket, replicas) is None, p["failover.heal_timeout"],
               f"every shard of {bucket} is whole")
    shards = target.shards(bucket)
    bucket_id = next(iter(shards.values()))["bucket_id"]
    keys: dict[int, list[str]] = {shard: [] for shard in shards}
    n = 0
    while any(len(k) < 8 for k in keys.values()):
        key = f"{ctx.run_id}/r{rtt:g}/k{kill}/{n}"
        keys[shard_of(bucket_id, key, len(shards))].append(key)
        n += 1
    victim = shards[sorted(shards)[kill % len(shards)]]["primary"]
    roles = {shard: "primary" if e["primary"] == victim
             else "backup" if victim in e.get("members", []) else "unaffected"
             for shard, e in shards.items()}
    writers = Writers(ctx, bucket, keys, p["failover.object_size"])
    sampler = ctx.sampler().start()
    writers.start()
    try:
        time.sleep(1.0)
        ctx.log(f"failover: killing {victim} (primary of shards "
                f"{[s for s, r in roles.items() if r == 'primary']})")
        fault = time.time()
        target.kill(victim)
        removed = None
        deadline = fault + p["failover.timeout"]
        while time.time() < deadline:
            current = target.shards(bucket)
            if removed is None and current and all(
                    victim not in e.get("members", []) for e in current.values()):
                removed = time.time()
            if all(any(s >= fault for s, _ in writers.acks(shard, fault)) for shard in shards):
                if removed is not None or time.time() > fault + p["failover.timeout"] / 2:
                    break
            time.sleep(0.25)
        time.sleep(p["failover.settle"])
        fault_end = time.time()
        restarted = restored = None
        if p["failover.restart"]:
            ctx.log(f"failover: restarting {victim}")
            restarted = time.time()
            target.start(victim)
            restored = wait_until(lambda: target.whole(bucket, replicas) is None,
                                  p["failover.heal_timeout"], f"{victim} rejoined every shard")
    finally:
        writers.stop()
        sampler.stop()

    distributions: dict[str, list[float]] = {}
    for shard, role in roles.items():
        acked_after = [(s, e) for s, e in writers.acks(shard, fault) if s >= fault]
        if acked_after:
            distributions.setdefault(f"recovery_{role}", []).append(acked_after[0][1] - fault)
        ends = [e for _, e in writers.acks(shard, fault, fault_end)]
        distributions.setdefault(f"stall_{role}", []).append(longest_gap(fault, ends, fault_end))
        if restarted is not None and restored is not None:
            ends = [e for _, e in writers.acks(shard, restarted, restored)]
            distributions.setdefault("rejoin_stall", []).append(
                longest_gap(restarted, ends, restored))
    distributions["write_latency"] = writers.latencies(fault, fault_end)
    under = sampler.series("skys3_under_replicated_bytes")
    values = {
        "victim": victim,
        "shards": {role: sum(1 for r in roles.values() if r == role)
                   for role in ("primary", "backup", "unaffected")},
        "failed_writes": writers.failed(fault),
        "removed_seconds": None if removed is None else removed - fault,
        "restarted_seconds": None if restarted is None else restarted - fault,
        "restored_seconds": None if restored is None else restored - fault,
        "under_replicated_bytes_max": metrics.peak(under),
        "replica_ack_timeout_mode": ctx.target.cluster.get("replica_ack_timeout_mode"),
    }
    return case({"control_rtt_ms": rtt, "kill": kill},
                {name: ctx.summarize(samples) for name, samples in distributions.items()},
                values, {"under_replicated_bytes": under})


def merge_kills(ctx: Context, rtt: float, kills: list[dict]) -> dict:
    """One case with the samples of every kill at a round trip."""
    merged: dict[str, list[float]] = {}
    for one in kills:
        for name, dist in one["distributions"].items():
            if dist["samples"] is None:
                continue
            merged.setdefault(name, []).extend(dist["samples"])
    windows = [k["values"]["restored_seconds"] for k in kills
               if k["values"]["restored_seconds"] is not None]
    return case({"control_rtt_ms": rtt, "kill": "all"},
                {name: ctx.summarize(samples) for name, samples in merged.items()},
                {"kills": len(kills), "restored_seconds_max": max(windows, default=None)})


# --- Flush -----------------------------------------------------------------------


def flush(ctx: Context) -> list[dict]:
    """Writes objects to a write_back bucket at each target round trip and
    measures until they are all at the target: the write throughput, the
    dirty backlog and flush lag over time, the drain time, the flush
    throughput, and how far the adaptive concurrency opened."""
    p = ctx.params
    remote = remote_client(ctx)
    url = urllib.parse.urlsplit(ctx.target.remote["url"])
    upstream = (url.hostname or "127.0.0.1", url.port or (443 if url.scheme == "https" else 80))
    cases = []
    for i, rtt in enumerate(p["flush.rtts_ms"]):
        shaper = choose_shaper(ctx, rtt)
        labels = {"target_rtt_ms": rtt, "shaper": shaper}
        if shaper.startswith("unavailable"):
            cases.append(case(labels, values={"skipped": shaper}))
            continue
        bucket = f"perf-{ctx.run_id}-flush-{i}"
        remote.create_bucket(bucket)
        with contextlib.ExitStack() as stack:
            endpoint = ctx.target.remote["url"]
            if shaper == "relay":
                relay = stack.enter_context(Relay(upstream, rtt / 2000.0))
                endpoint = f"http://{relay.address}"
            elif shaper == "hook":
                ctx.target.shape("target", rtt)
                stack.callback(ctx.target.unshape, "target")
            cases.append(flush_once(ctx, bucket, f"{endpoint}/{bucket}", labels))
    return cases


def choose_shaper(ctx: Context, rtt: float) -> str:
    wanted = ctx.params["flush.shaper"]
    if rtt <= 0:
        return "none"
    if wanted in ("auto", "hook") and ctx.target.can_shape("target"):
        return "hook"
    if wanted in ("auto", "relay"):
        # The relay forwards bytes to a plain-HTTP endpoint it names to
        # the node; it cannot carry a TLS target's name and certificate.
        if urllib.parse.urlsplit(ctx.target.remote["url"]).scheme == "https":
            return "unavailable: the relay carries plain HTTP only; shape an HTTPS target with a hook"
        return "relay"
    return "unavailable: flush.shaper is hook and the target cannot shape its target link"


def flush_once(ctx: Context, bucket: str, target_url: str, labels: dict) -> dict:
    p = ctx.params
    ctx.log(f"flush: {p['flush.objects']} objects to {target_url} ({labels})")
    ctx.client().create_bucket(bucket, target_url)
    ctx.ready(bucket)
    sampler = ctx.sampler().start()
    before = ctx.snapshot()
    started = time.time()
    concurrency = p["flush.concurrency"]
    outcome = ctx.generator.run(ctx.task(
        "put", bucket, prefix=f"{ctx.run_id}/", size=p["flush.size"],
        count=math.ceil(p["flush.objects"] / concurrency)), concurrency)
    written = time.time()
    flushed = wait_until(lambda: ctx.flushed(bucket), p["flush.timeout"], f"{bucket} is flushed")
    after = ctx.snapshot()
    sampler.stop()
    distributions, values = outcome.summary(p["samples_max"])

    def series(name: str, how: str = "total") -> list[list[float]]:
        return sampler.series(name, how, bucket=bucket)

    gauges = {
        "dirty_bytes": series("skys3_dirty_bytes"),
        "flush_lag_seconds": series("skys3_flush_lag_seconds", "highest"),
        "oldest_dirty_age_seconds": series("skys3_oldest_dirty_age_seconds", "highest"),
        "flush_concurrency": series("skys3_flush_concurrency"),
        "flush_inflight_bytes": series("skys3_flush_inflight_bytes"),
    }
    overlap_sum = metrics.counter_delta(before, after, "skys3_flush_streaming_overlap_ratio_sum",
                                        bucket=bucket)
    overlap_count = metrics.counter_delta(before, after,
                                          "skys3_flush_streaming_overlap_ratio_count",
                                          bucket=bucket)
    round_trip = sampler.series("skys3_flush_base_round_trip_seconds", "highest", bucket=bucket)
    values.update({
        "flushes": metrics.counter_delta(before, after, "skys3_flushes_total", bucket=bucket),
        "drain_seconds": flushed - written,
        "flush_seconds": flushed - started,
        "flush_throughput_bytes": stats.ratio(values["bytes"], flushed - started),
        "dirty_bytes_max": metrics.peak(gauges["dirty_bytes"]),
        "flush_lag_seconds_max": metrics.peak(gauges["flush_lag_seconds"]),
        "oldest_dirty_age_seconds_max": metrics.peak(gauges["oldest_dirty_age_seconds"]),
        "flush_concurrency_max": metrics.peak(gauges["flush_concurrency"]),
        "flush_inflight_bytes_max": metrics.peak(gauges["flush_inflight_bytes"]),
        "base_round_trip_seconds": round_trip[-1][1] if round_trip else None,
        "streaming_overlap_mean": stats.ratio(overlap_sum, overlap_count),
    })
    return case({**labels, "size_bytes": p["flush.size"]}, distributions, values, gauges)


# --- Import and fill ----------------------------------------------------------------


def import_(ctx: Context) -> list[dict]:
    """Imports a remote bucket of `import.objects` objects and measures the
    import rate, then reads some of them twice: the first GET fills each
    from the remote, the second reads it locally."""
    p = ctx.params
    remote = remote_client(ctx)
    bucket = f"perf-{ctx.run_id}-import"
    remote.create_bucket(bucket)
    url = ctx.target.remote["url"]
    concurrency = 8
    ctx.log(f"import: seeding {bucket} with {p['import.objects']} objects")
    seeded = ctx.generator.run(Task(
        op="put", endpoints=(url,), credentials=ctx.target.remote_credentials(), bucket=bucket,
        prefix="o/", size=p["import.size"], timeout=p["request_timeout"],
        count=math.ceil(p["import.objects"] / concurrency)), concurrency)
    keys = list(seeded.ops.keys)
    if len(keys) != len(seeded.ops.statuses):
        raise RuntimeError(f"seeding the remote bucket failed: {seeded.summary(0)[1]['failures']}")
    before = ctx.snapshot()
    started = time.time()
    ctx.client().create_bucket(bucket, f"{url}/{bucket}")

    def imported() -> bool:
        answers = ctx.bucket_status(bucket)
        states = [(a.get("flush") or {}).get("import") for a in answers]
        if not any(s and s.get("state") == "done" for s in states):
            return False
        if any(s and s.get("state") != "done" for s in states):
            return False
        return ctx.client().count_keys(bucket) >= len(keys)

    done = wait_until(imported, p["import.timeout"], f"{bucket} is imported", step=0.25)
    reads = keys[: p["import.reads"]]
    fill, local = [], []
    failures: dict[str, int] = {}
    for samples in (fill, local):
        for key in reads:
            # A node may not follow a new bucket's target for a moment
            # (503): retried, as an SDK would, and each failure counted.
            for attempt in range(3):
                client = ctx.client()
                start = time.time()
                status, _ = client.get(bucket, key)
                end = time.time()
                client.close()
                if status == 200:
                    samples.append(end - start)
                    break
                failures[str(status)] = failures.get(str(status), 0) + 1
                time.sleep(0.2 * (attempt + 1))
    after = ctx.snapshot()
    seconds = done - started
    return [case(
        {"objects": len(keys), "size_bytes": p["import.size"]},
        {"fill_latency": ctx.summarize(fill), "local_get_latency": ctx.summarize(local)},
        {"import_seconds": seconds, "import_rate_objects": stats.ratio(len(keys), seconds),
         "fills": metrics.counter_delta(before, after, "skys3_fills_total", bucket=bucket),
         "failed_reads": failures, "unread": 2 * len(reads) - len(fill) - len(local)})]


# --- The registry ---------------------------------------------------------------------


@dataclass(frozen=True)
class Scenario:
    name: str
    rows: tuple[str, ...]
    run: Callable[[Context], list[dict]] | None


SCENARIOS = {s.name: s for s in [
    Scenario("put", ("PUT p50/p99 by object size, with fsyncs per acknowledged PUT, records "
                     "per group commit, and serial durable rounds against section 5.3's table",),
             put),
    Scenario("get", ("Read scaling: GET throughput of one hot object by clean_copies and "
                     "gateways",), get),
    Scenario("failover", ("Failover time distribution for primary and backup failures, and the "
                          "write-unavailability window per shard, by control-store round trip; "
                          "learner promotion's write stall",
                          "Durability window: time from a member loss until every shard has "
                          "replicas members again, and p99 write latency in the cluster's "
                          "replica_ack_timeout_mode"), failover),
    Scenario("flush", ("Flush lag, dirty backlog, and sustained flush throughput by target round "
                       "trip; adaptive concurrency; streaming-flush overlap",), flush),
    Scenario("import", ("Import rate", "Fill latency"), import_),
    Scenario("cache", ("Cache hit rate under eviction, and compaction write amplification",),
             None),
    Scenario("local", ("Local buckets: stored bytes per logical byte, encoding backlog, and "
                       "repair time after a node loss",), None),
    Scenario("peer", ("Peer transport over shaped links (150-300 ms, 0-2% loss) for each "
                      "congestion controller and connection count, against S3 REST; link-flap "
                      "resume; UDP-blocked fallback (plan M6-09)",), None),
    Scenario("hot_bucket", ("Per-bucket write throughput against shards_per_bucket (section 19, "
                            "question 5)",), None),
    Scenario("metadata", ("Index bytes per entry of the namespace mirror (section 19, "
                          "question 6)",), None),
]}
