"""The human-readable side of a result: a Markdown summary of one run, and
a comparison of two runs case by case."""

from __future__ import annotations

import json

# The values the summary shows for each scenario's cases; the result has
# them all.
KEY_VALUES = {
    "put": ["throughput_ops", "fsyncs_per_put", "group_commits_per_put",
            "records_per_group_commit", "mean_group_commit_seconds",
            "serial_durable_rounds_bound"],
    "get": ["throughput_ops", "throughput_bytes", "hot_cache_hits", "fills"],
    "failover": ["victim", "failed_writes", "removed_seconds", "restored_seconds",
                 "under_replicated_bytes_max", "restored_seconds_max"],
    "flush": ["throughput_ops", "drain_seconds", "flush_throughput_bytes", "dirty_bytes_max",
              "flush_lag_seconds_max", "flush_concurrency_max", "base_round_trip_seconds"],
    "import": ["import_rate_objects", "import_seconds", "fills", "failed_reads", "unread"],
}
# What `compare` sets side by side: percentiles of every distribution,
# and these values.
COMPARED_VALUES = ["throughput_ops", "throughput_bytes", "fsyncs_per_put",
                   "records_per_group_commit", "flush_throughput_bytes", "drain_seconds",
                   "import_rate_objects", "restored_seconds"]


def show(value: object, unit: str = "") -> str:
    """A value for people: seconds in ms below 1 s, bytes in binary units,
    other numbers with 3 significant digits."""
    if value is None:
        return "-"
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return str(value)
    if unit == "seconds" or "_seconds" in unit:
        return f"{value * 1000:.3g} ms" if abs(value) < 1 else f"{value:.3g} s"
    if "bytes" in unit:
        for name, size in (("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)):
            if abs(value) >= size:
                return f"{value / size:.3g} {name}{'/s' if 'throughput' in unit else ''}"
        return f"{value:.0f} B{'/s' if 'throughput' in unit else ''}"
    if isinstance(value, int) or value.is_integer():
        return str(int(value))
    return f"{value:.3g}"


def labels_text(labels: dict) -> str:
    return " ".join(f"{k}={v}" for k, v in labels.items() if k != "size_bytes") or "-"


def summary(result: dict) -> str:
    git = result["git"]
    host = result["host"]
    disk = host["disk"]
    disk_text = "unknown disk" if not disk else (
        f"{disk['device']} ({'rotational' if disk['rotational'] else 'solid-state'}"
        f"{', ' + disk['model'] if disk['model'] else ''})")
    lines = [
        f"# SkyS3 performance run {result['run_id']} ({result['profile']})",
        "",
        f"- Commit {git['sha'] or 'unknown'}{' with local changes' if git['dirty'] else ''}; "
        f"binary {result['cluster']['binary_version'] or 'unknown'}",
        f"- Cluster {result['cluster']['name']}: {len(result['cluster']['nodes'])} nodes, "
        f"{json.dumps(result['cluster']['facts'], sort_keys=True)}",
        f"- Host: {host['cpus']} CPUs, {host['machine']}, Linux {host['kernel']}, {disk_text}",
        f"- {result['started_at']} to {result['finished_at']}",
        "",
        "Numbers mean something only on the fixed release hardware, compared with runs there.",
    ]
    for scenario in result["scenarios"]:
        lines += ["", f"## {scenario['name']}: {scenario['status']} "
                      f"({scenario['duration_seconds']:.1f} s)"]
        if scenario["reason"]:
            lines.append(f"\n{scenario['reason']}")
        for case in scenario["cases"]:
            parts = []
            if "skipped" in case["values"]:
                parts.append(f"skipped: {case['values']['skipped']}")
            for name, dist in case["distributions"].items():
                if dist["count"]:
                    pct = dist["percentiles"]
                    parts.append(f"{name} n={dist['count']} p50={show(pct['p50'], dist['unit'])} "
                                 f"p99={show(pct['p99'], dist['unit'])} "
                                 f"max={show(dist['max'], dist['unit'])}")
            for key in KEY_VALUES.get(scenario["name"], []):
                if key in case["values"]:
                    parts.append(f"{key}={show(case['values'][key], key)}")
            lines.append(f"- {labels_text(case['labels'])}: " + "; ".join(parts))
    return "\n".join(lines) + "\n"


def _cases(result: dict) -> dict[tuple[str, str], dict]:
    return {(s["name"], json.dumps(c["labels"], sort_keys=True)): c
            for s in result["scenarios"] for c in s["cases"]}


def compare(base: dict, new: dict) -> str:
    """A Markdown table of every measure two results share, with the
    change from `base` to `new`."""
    rows = ["| Scenario | Case | Measure | Base | New | Change |", "|---|---|---|---|---|---|"]
    old_cases = _cases(base)
    for key, case in _cases(new).items():
        before = old_cases.get(key)
        if before is None:
            continue
        name, labels = key[0], labels_text(json.loads(key[1]))
        pairs = []
        for dist_name, dist in case["distributions"].items():
            other = before["distributions"].get(dist_name)
            if other:
                for p in ("p50", "p99"):
                    pairs.append((f"{dist_name} {p}", other["percentiles"][p],
                                  dist["percentiles"][p], dist["unit"]))
        for value_name in COMPARED_VALUES:
            if value_name in case["values"] and value_name in before["values"]:
                pairs.append((value_name, before["values"][value_name],
                              case["values"][value_name], value_name))
        for measure, a, b, unit in pairs:
            change = "-"
            if isinstance(a, (int, float)) and isinstance(b, (int, float)) and a:
                change = f"{(b - a) / a * 100:+.1f}%"
            rows.append(f"| {name} | {labels} | {measure} | {show(a, unit)} | "
                        f"{show(b, unit)} | {change} |")
    if len(rows) == 2:
        rows.append("| - | - | the results share no case | - | - | - |")
    return "\n".join(rows) + "\n"
