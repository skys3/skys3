"""The harness's parameters, their defaults per profile, and their parsing.

Every size, count, duration, and round trip a scenario uses is a parameter
named `<scenario>.<name>` (or a bare name for the whole run). A profile
gives each its default: `smoke` takes seconds and tiny objects and is safe
for CI, `full` is the design section 16.3 matrix for release hardware.
`--set name=value` overrides one; lists are comma-separated. Sizes take
binary units (`64KiB`, `1GiB`), durations `ms`, `s`, `m`, or `h`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

PROFILES = ("smoke", "full")

_SIZE_UNITS = {"": 1, "B": 1, "KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30, "TiB": 1 << 40}
_DURATION_UNITS = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0}


class ParamError(ValueError):
    """A parameter that is unknown or whose value does not parse."""


def parse_size(text: str) -> int:
    """Bytes from `4096`, `64KiB`, or `1.5GiB`."""
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([KMGT]iB|B)?\s*", text)
    if not match:
        raise ParamError(f"not a size (such as 4096, 64KiB, or 1GiB): {text!r}")
    return int(float(match.group(1)) * _SIZE_UNITS[match.group(2) or ""])


def format_size(size: int) -> str:
    """The shortest exact form of a size: 1KiB, 1536B, 16MiB."""
    for unit in ("TiB", "GiB", "MiB", "KiB"):
        if size and size % _SIZE_UNITS[unit] == 0:
            return f"{size // _SIZE_UNITS[unit]}{unit}"
    return f"{size}B"


def parse_duration(text: str) -> float:
    """Seconds from `500ms`, `2s`, `1.5m`, or a bare number of seconds."""
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h)?\s*", text)
    if not match:
        raise ParamError(f"not a duration (such as 500ms, 30s, or 2m): {text!r}")
    return float(match.group(1)) * _DURATION_UNITS[match.group(2) or "s"]


def parse_bool(text: str) -> bool:
    lowered = text.strip().lower()
    if lowered in ("1", "true", "yes", "on"):
        return True
    if lowered in ("0", "false", "no", "off"):
        return False
    raise ParamError(f"not a boolean: {text!r}")


_PARSERS = {
    "int": int,
    "float": float,
    "size": parse_size,
    "duration": parse_duration,
    "bool": parse_bool,
    "str": str,
}


@dataclass(frozen=True)
class Param:
    name: str
    kind: str  # a key of _PARSERS
    smoke: object
    full: object
    doc: str
    is_list: bool = False

    def parse(self, text: str) -> object:
        parser = _PARSERS[self.kind]
        try:
            if self.is_list:
                return [parser(item) for item in text.split(",") if item.strip()]
            return parser(text)
        except ParamError:
            raise
        except ValueError as error:
            raise ParamError(f"{self.name}: {error}") from None


KiB, MiB, GiB = 1 << 10, 1 << 20, 1 << 30

IMPLEMENTED = ["put", "get", "failover", "flush", "import"]
EXTENSION_POINTS = ["cache", "local", "peer", "hot_bucket", "metadata"]

PARAMS = [
    Param("scenarios", "str", IMPLEMENTED, IMPLEMENTED + EXTENSION_POINTS,
          "The scenarios to run, in order.", is_list=True),
    Param("metrics_interval", "duration", 0.5, 1.0,
          "How often every node's /metrics is sampled during a scenario."),
    Param("samples_max", "int", 2000, 10000,
          "A distribution keeps its raw samples while it has at most this many."),
    Param("processes", "int", 0, 0,
          "Load-generator processes at most; 0 is one per CPU."),
    Param("request_timeout", "duration", 30.0, 300.0,
          "How long a request may take before it counts as unanswered."),
    # PUT p50/p99 by size, with the log counters (section 16.3, row 1).
    Param("put.sizes", "size", [KiB, 64 * KiB],
          [KiB, 16 * KiB, 128 * KiB, MiB, 16 * MiB, 128 * MiB, GiB],
          "Object sizes, one case each with every concurrency.", is_list=True),
    Param("put.concurrency", "int", [1, 4], [1, 16, 64],
          "Concurrent PUTs, one case each.", is_list=True),
    Param("put.duration", "duration", 2.0, 30.0, "How long each case sends PUTs."),
    Param("put.max_bytes", "size", 64 * MiB, 16 * GiB,
          "The most bytes a case sends, split among its writers; each sends one object "
          "at least."),
    Param("put.bucket", "str", "perf-put", "perf-put",
          "The bucket, of the cluster's default mode."),
    # GET throughput by clean_copies and gateways (row 7).
    Param("get.size", "size", 64 * KiB, MiB, "The size of the hot object."),
    Param("get.clean_copies", "int", [1], [1, 2, 3],
          "clean_copies values, each through the bucket the target names for it.", is_list=True),
    Param("get.gateways", "int", [1, 3], [1, 2, 3, 5, 8],
          "Gateway counts; counts above the live nodes are skipped.", is_list=True),
    Param("get.concurrency", "int", 4, 16, "Concurrent GETs per gateway."),
    Param("get.duration", "duration", 2.0, 30.0, "How long each case sends GETs."),
    # Failover, write unavailability, and the durability window (rows 2 and 5).
    Param("failover.control_rtts_ms", "float", [0.0], [1.0, 100.0],
          "Control-store round trips, shaped with the target's control_store "
          "link hooks; 0 leaves the link as it is.", is_list=True),
    Param("failover.kills", "int", 1, 5, "Kills of a shard primary's node per round trip."),
    Param("failover.object_size", "size", KiB, 4 * KiB, "The size of the writers' objects."),
    Param("failover.settle", "duration", 2.0, 5.0,
          "How long writers go on once every shard took a write after the kill."),
    Param("failover.timeout", "duration", 60.0, 180.0,
          "How long every shard may take to take a write after the kill."),
    Param("failover.restart", "bool", True, True,
          "Restart the killed node, and measure until every shard is whole again."),
    Param("failover.heal_timeout", "duration", 120.0, 600.0,
          "How long the shards may take to have `replicas` members and no learner."),
    Param("failover.bucket", "str", "perf-failover", "perf-failover", "The bucket."),
    # Flush lag, dirty backlog, and flush throughput by target round trip (row 3).
    Param("flush.rtts_ms", "float", [0.0, 20.0], [1.0, 10.0, 50.0, 100.0, 150.0],
          "Round trips to the remote target; 0 leaves the link as it is.", is_list=True),
    Param("flush.objects", "int", 100, 5000, "Objects written per round trip."),
    Param("flush.size", "size", 16 * KiB, MiB, "Their size."),
    Param("flush.concurrency", "int", 4, 32, "Concurrent PUTs writing them."),
    Param("flush.shaper", "str", "auto", "auto",
          "How a round trip is made: `hook` (the target's `target` link hooks, such as "
          "tc netem), `relay` (a delaying TCP relay in the harness), or `auto` (hook if "
          "the target has one, else relay)."),
    Param("flush.timeout", "duration", 120.0, 3600.0, "How long the bucket may take to flush."),
    # Import rate and fill latency (rows 3 and 6).
    Param("import.objects", "int", 100, 20000, "Objects in the remote bucket imported."),
    Param("import.size", "size", KiB, 4 * KiB, "Their size."),
    Param("import.reads", "int", 50, 1000,
          "Objects read twice after the import: filled, then local."),
    Param("import.timeout", "duration", 120.0, 3600.0, "How long the import may take."),
]

BY_NAME = {param.name: param for param in PARAMS}


def resolve(profile: str, overrides: list[str] | None = None) -> dict:
    """The parameters of `profile` with `overrides` (`name=value`) applied."""
    if profile not in PROFILES:
        raise ParamError(f"the profile is one of {', '.join(PROFILES)}, not {profile!r}")
    values = {param.name: getattr(param, profile) for param in PARAMS}
    for override in overrides or []:
        name, sep, text = override.partition("=")
        if not sep:
            raise ParamError(f"an override is name=value, not {override!r}")
        param = BY_NAME.get(name.strip())
        if param is None:
            raise ParamError(f"unknown parameter {name.strip()!r}; `perf.py params` lists them")
        values[param.name] = param.parse(text)
    unknown = set(values["scenarios"]) - set(IMPLEMENTED + EXTENSION_POINTS)
    if unknown:
        raise ParamError(f"unknown scenarios: {', '.join(sorted(unknown))}")
    return values


def describe() -> str:
    """The parameter table, in Markdown, for the documentation."""
    lines = ["| Parameter | Kind | smoke | full | Meaning |", "|---|---|---|---|---|"]
    for param in PARAMS:
        kind = f"list of {param.kind}" if param.is_list else param.kind
        lines.append(f"| `{param.name}` | {kind} | {_show(param, param.smoke)} | "
                     f"{_show(param, param.full)} | {param.doc} |")
    return "\n".join(lines)


def _show(param: Param, value: object) -> str:
    def one(item: object) -> str:
        if param.kind == "size":
            return format_size(int(item))  # type: ignore[arg-type]
        if param.kind == "duration":
            return f"{item:g}s"
        if isinstance(item, float):
            return f"{item:g}"
        return str(item).lower() if isinstance(item, bool) else str(item)

    if isinstance(value, list):
        return ", ".join(one(item) for item in value)
    return one(value)
