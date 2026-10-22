"""The load generator: concurrent PUTs or GETs from worker processes.

One Python process cannot drive a node hard: signing, the HTTP framing,
and copying bodies hold its interpreter lock. So a case's concurrent
clients are spread over processes (one per CPU at most), each running its
share as threads, and every process reports the CPU time it used. A
process that was busy for most of the case means the generator, not the
cluster, may have set the pace; `Outcome.generator` says so in the result.
"""

from __future__ import annotations

import dataclasses
import multiprocessing
import os
import threading
import time
from array import array
from dataclasses import dataclass, field

from . import stats
from .s3 import Client, Credentials, Payload

# A process busier than this share of the case's wall time may have been
# the bottleneck.
BUSY = 0.85


@dataclass(frozen=True)
class Task:
    """What one worker process does: `threads` clients, each sending one
    request at a time to the endpoints in turn, PUTs of `size` bytes to new
    keys under `prefix` or GETs of `keys`, until `duration` has passed or
    each client sent `count` requests (0: no limit) or `max_bytes`."""

    op: str  # "put" or "get"
    endpoints: tuple[str, ...]
    credentials: Credentials
    bucket: str
    prefix: str = ""
    keys: tuple[str, ...] = ()
    size: int = 0
    threads: int = 1
    duration: float = 0.0
    count: int = 0
    max_bytes: int = 0
    ca_file: str | None = None
    timeout: float = 30.0
    seed: int = 0
    index: int = 0


@dataclass
class Ops:
    """Every request of a task: start and end (Unix time), status, bytes."""

    starts: array = field(default_factory=lambda: array("d"))
    ends: array = field(default_factory=lambda: array("d"))
    statuses: array = field(default_factory=lambda: array("i"))
    sizes: array = field(default_factory=lambda: array("q"))
    keys: list[str] = field(default_factory=list)
    cpu_seconds: float = 0.0
    wall_seconds: float = 0.0

    def add(self, other: Ops) -> None:
        self.starts.extend(other.starts)
        self.ends.extend(other.ends)
        self.statuses.extend(other.statuses)
        self.sizes.extend(other.sizes)
        self.keys.extend(other.keys)


def _client_loop(task: Task, thread: int, out: Ops, lock: threading.Lock) -> None:
    clients = [Client(e, task.credentials, task.ca_file, task.timeout) for e in task.endpoints]
    payload = Payload(task.size, task.seed)
    deadline = time.time() + task.duration if task.duration else None
    # A positive budget, however small: 0 would mean no budget at all.
    budget = max(1, task.max_bytes // max(1, task.threads)) if task.max_bytes else 0
    sent = 0
    n = 0
    while (not task.count or n < task.count) and (deadline is None or time.time() < deadline):
        if budget and n and sent + task.size > budget:
            break
        client = clients[(thread + n) % len(clients)]
        start = time.time()
        if task.op == "put":
            key = f"{task.prefix}{task.index}-{thread}-{n}"
            status = client.put(task.bucket, key, payload)
            moved = task.size
        else:
            key = task.keys[(thread * 7919 + n) % len(task.keys)]
            status, moved = client.get(task.bucket, key)
        end = time.time()
        sent += moved
        n += 1
        with lock:
            out.starts.append(start)
            out.ends.append(end)
            out.statuses.append(status)
            out.sizes.append(moved if 200 <= status < 300 else 0)
            if task.op == "put" and 200 <= status < 300:
                out.keys.append(key)
    for client in clients:
        client.close()


def run_task(task: Task) -> Ops:
    """Runs one worker's task in this process (the pool's entry point)."""
    out = Ops()
    lock = threading.Lock()
    cpu = time.process_time()
    started = time.monotonic()
    threads = [threading.Thread(target=_client_loop, args=(task, t, out, lock))
               for t in range(task.threads)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    out.cpu_seconds = time.process_time() - cpu
    out.wall_seconds = time.monotonic() - started
    return out


def split(concurrency: int, processes: int) -> list[int]:
    """Threads per process for `concurrency` clients over at most
    `processes` processes, as evenly as possible."""
    if concurrency < 1 or processes < 1:
        raise ValueError("concurrency and processes are at least 1")
    used = min(concurrency, processes)
    base, extra = divmod(concurrency, used)
    return [base + (1 if i < extra else 0) for i in range(used)]


def tasks_for(task: Task, concurrency: int, processes: int) -> list[Task]:
    """`task` for `concurrency` clients over at most `processes` processes:
    each process gets its threads and its share of the case's byte budget,
    in proportion to its threads, so the case sends at most `max_bytes`."""
    shares = split(concurrency, processes)
    return [dataclasses.replace(task, threads=threads, index=i,
                                max_bytes=_share(task.max_bytes, threads, concurrency))
            for i, threads in enumerate(shares)]


def _share(max_bytes: int, threads: int, concurrency: int) -> int:
    """`threads`' share of `max_bytes` (0: no budget), at least 1 byte."""
    return max(1, max_bytes * threads // concurrency) if max_bytes else 0


@dataclass
class Outcome:
    """The requests of a case and what they add up to."""

    ops: Ops
    processes: int
    generator: dict

    def ok(self) -> list[int]:
        return [i for i, status in enumerate(self.ops.statuses) if 200 <= status < 300]

    def latencies(self) -> list[float]:
        return [self.ops.ends[i] - self.ops.starts[i] for i in self.ok()]

    def summary(self, samples_max: int) -> tuple[dict, dict]:
        """The case's distributions and values: the latency of the requests
        that succeeded, their throughput over the case's wall time, and the
        failures by status (0: no answer)."""
        ok = self.ok()
        ops = self.ops
        wall = (max(ops.ends) - min(ops.starts)) if len(ops.starts) else 0.0
        failures: dict[str, int] = {}
        for status in ops.statuses:
            if not 200 <= status < 300:
                failures[str(status)] = failures.get(str(status), 0) + 1
        moved = sum(ops.sizes)
        values = {
            "requests": len(ops.statuses),
            "succeeded": len(ok),
            "failed": len(ops.statuses) - len(ok),
            "wall_seconds": wall,
            "throughput_ops": stats.ratio(len(ok), wall),
            "throughput_bytes": stats.ratio(moved, wall),
            "bytes": moved,
        }
        distributions = {"latency": stats.summarize(self.latencies(), "seconds", samples_max)}
        return distributions, {**values, "failures": failures, "generator": self.generator}


class Generator:
    """A pool of worker processes, started once for a run."""

    def __init__(self, processes: int = 0) -> None:
        self.processes = processes or os.cpu_count() or 1
        # Spawned, not forked: the harness runs threads (the metrics
        # sampler) that a fork would copy mid-flight.
        self.pool = multiprocessing.get_context("spawn").Pool(self.processes)

    def close(self) -> None:
        self.pool.close()
        self.pool.join()

    def __enter__(self) -> Generator:
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    def run(self, task: Task, concurrency: int) -> Outcome:
        """`concurrency` clients doing `task`, over the pool's processes."""
        tasks = tasks_for(task, concurrency, self.processes)
        results = self.pool.map(run_task, tasks, chunksize=1)
        ops = Ops()
        for result in results:
            ops.add(result)
        busiest = max((r.cpu_seconds / r.wall_seconds for r in results if r.wall_seconds),
                      default=0.0)
        generator = {
            "processes": len(tasks),
            "threads": concurrency,
            "cpu_seconds": sum(r.cpu_seconds for r in results),
            "busiest_process_utilization": busiest,
            "may_have_limited": busiest >= BUSY,
        }
        return Outcome(ops=ops, processes=len(tasks), generator=generator)
