"""Node metrics: parsing /metrics, snapshots, counter deltas, and a sampler.

Each node serves its own metrics in the OpenMetrics text format on its
admin listener (docs/skys3-metrics.md). The harness takes a snapshot of
every node before and after a case, and the difference of each counter is
what the case did; a `Sampler` polls the nodes meanwhile, for the gauges
whose course matters (dirty bytes, flush lag, under-replicated bytes).
"""

from __future__ import annotations

import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field

Labels = tuple[tuple[str, str], ...]
# A node's metrics: sample name -> labels -> value.
Parsed = dict[str, dict[Labels, float]]


def parse(text: str) -> Parsed:
    """The samples of an OpenMetrics or Prometheus text exposition."""
    samples: Parsed = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "{" in line:
            name, rest = line.split("{", 1)
            labels_text, _, rest = rest.rpartition("}")
            labels = _labels(labels_text)
        else:
            name, _, rest = line.partition(" ")
            labels = ()
        fields = rest.split()
        if not fields:
            continue
        try:
            value = float(fields[0])
        except ValueError:
            continue
        samples.setdefault(name.strip(), {})[labels] = value
    return samples


def _labels(text: str) -> Labels:
    """Label pairs from `a="x",b="y"`, with \\" and \\\\ escapes."""
    pairs = []
    i = 0
    while i < len(text):
        equals = text.index("=", i)
        name = text[i:equals].strip().lstrip(",").strip()
        i = equals + 2  # past ="
        value = []
        while text[i] != '"':
            if text[i] == "\\" and i + 1 < len(text):
                i += 1
                value.append({"n": "\n"}.get(text[i], text[i]))
            else:
                value.append(text[i])
            i += 1
        pairs.append((name, "".join(value)))
        i += 1
        while i < len(text) and text[i] in ", ":
            i += 1
    return tuple(sorted(pairs))


def value(parsed: Parsed | None, name: str, **labels: str) -> float | None:
    """The sum of the series of `name` whose labels include `labels`, or
    None if the node exports none."""
    if not parsed or name not in parsed:
        return None
    wanted = set(labels.items())
    matched = [v for series, v in parsed[name].items() if wanted <= set(series)]
    return sum(matched) if matched else None


@dataclass
class Snapshot:
    """Every node's metrics at one moment; None for a node that did not
    answer."""

    at: float
    nodes: dict[str, Parsed | None] = field(default_factory=dict)

    def total(self, name: str, **labels: str) -> float | None:
        """The sum over the nodes, or None if no node exports it."""
        values = [value(p, name, **labels) for p in self.nodes.values()]
        known = [v for v in values if v is not None]
        return sum(known) if known else None

    def highest(self, name: str, **labels: str) -> float | None:
        values = [value(p, name, **labels) for p in self.nodes.values()]
        known = [v for v in values if v is not None]
        return max(known) if known else None

    def label_value(self, name: str, label: str) -> str | None:
        """A label of the first series of `name` any node exports, such as
        the version of skys3_build_info."""
        for parsed in self.nodes.values():
            for series in (parsed or {}).get(name, {}):
                for key, text in series:
                    if key == label:
                        return text
        return None


def counter_delta(before: Snapshot, after: Snapshot, name: str, **labels: str) -> float | None:
    """How much counter `name` grew between the snapshots, summed over the
    nodes. A node whose counter went down restarted in between, and counts
    from 0, as Prometheus's increase() does. None if no node exports it."""
    total = None
    for node, parsed in after.nodes.items():
        now = value(parsed, name, **labels)
        if now is None:
            continue
        then = value(before.nodes.get(node), name, **labels)
        grown = now if then is None or now < then else now - then
        total = grown if total is None else total + grown
    return total


def fetch(address: str, token: str | None, timeout: float = 5.0) -> Parsed | None:
    """One node's metrics, or None if it does not answer."""
    request = urllib.request.Request(f"http://{address}/metrics")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return parse(response.read().decode())
    except (OSError, urllib.error.URLError, ValueError):
        return None


def snapshot(admins: dict[str, str], token: str | None) -> Snapshot:
    """Every node's metrics now: `admins` maps node names to admin
    listener addresses."""
    taken = Snapshot(at=time.time())
    for node, address in admins.items():
        taken.nodes[node] = fetch(address, token)
    return taken


class Sampler:
    """Takes a snapshot of every node each `interval` seconds, in a thread,
    from `start()` until `stop()`."""

    def __init__(self, admins: dict[str, str], token: str | None, interval: float) -> None:
        self.admins = admins
        self.token = token
        self.interval = interval
        self.snapshots: list[Snapshot] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> Sampler:
        self.snapshots = []
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="metrics-sampler", daemon=True)
        self._thread.start()
        return self

    def _run(self) -> None:
        while not self._stop.is_set():
            started = time.monotonic()
            self.snapshots.append(snapshot(self.admins, self.token))
            self._stop.wait(max(0.0, self.interval - (time.monotonic() - started)))

    def stop(self) -> list[Snapshot]:
        self._stop.set()
        if self._thread:
            self._thread.join()
        return self.snapshots

    def series(self, name: str, how: str = "total", **labels: str) -> list[list[float]]:
        """[time, value] pairs of `name` over the samples, summed over the
        nodes (`total`) or their maximum (`highest`); samples where no node
        exports it are left out."""
        points = []
        for taken in self.snapshots:
            v = taken.total(name, **labels) if how == "total" else taken.highest(name, **labels)
            if v is not None:
                points.append([round(taken.at, 3), v])
        return points


def peak(points: list[list[float]]) -> float | None:
    """The largest value of a series, or None if it is empty."""
    return max((v for _, v in points), default=None)
