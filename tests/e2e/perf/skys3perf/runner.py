"""A run: the host and git facts, every scenario in turn, the result
document, and the checks a result must pass."""

from __future__ import annotations

import datetime
import os
import platform
import secrets
import subprocess
import time
import traceback

from . import SCHEMA_NAME, SCHEMA_VERSION, schema
from .load import Generator
from .scenarios import SCENARIOS, Context, Skip
from .target import Target

# Values each case of a scenario that ran must report, and above 0: what
# the metrics and the clients measured, so a run whose counters or
# samples went missing fails its check (`perf.py check`).
REQUIRED_POSITIVE = {
    "put": ["throughput_ops", "fsyncs_per_put", "group_commits_per_put",
            "records_per_group_commit", "mean_group_commit_seconds"],
    "get": ["throughput_ops", "throughput_bytes"],
    "failover": [],
    "flush": ["throughput_ops", "flushes", "flush_throughput_bytes"],
    "import": ["import_rate_objects", "fills"],
}
# Values each such case must report, possibly 0.
REQUIRED_PRESENT = {
    "flush": ["dirty_bytes_max", "flush_lag_seconds_max", "drain_seconds"],
    "failover": ["removed_seconds", "under_replicated_bytes_max"],
}


def now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def git_facts(directory: str) -> dict:
    """The commit the harness runs from, and whether the tree differs."""
    def git(*args: str) -> str | None:
        try:
            return subprocess.run(["git", "-C", directory, *args], check=True, text=True,
                                  capture_output=True, timeout=30).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            return None

    sha = os.environ.get("SKYS3_PERF_GIT_SHA") or git("rev-parse", "HEAD")
    status = git("status", "--porcelain")
    return {"sha": sha, "dirty": None if status is None else bool(status)}


def disk_facts(path: str | None) -> dict | None:
    """The block device holding `path`, whether it spins, and its model,
    from sysfs; None if it cannot be told (another OS, a container)."""
    if not path or not os.path.exists(path):
        return None
    device = os.stat(path).st_dev
    sysfs = f"/sys/dev/block/{os.major(device)}:{os.minor(device)}"
    if not os.path.exists(sysfs):
        return None
    real = os.path.realpath(sysfs)
    # A partition's queue/ and device/ are its disk's, one level up.
    disk = real if os.path.exists(os.path.join(real, "queue")) else os.path.dirname(real)

    def read(name: str) -> str | None:
        try:
            with open(os.path.join(disk, name)) as f:
                return f.read().strip()
        except OSError:
            return None

    rotational = read("queue/rotational")
    return {"device": os.path.basename(disk),
            "rotational": None if rotational is None else rotational == "1",
            "model": read("device/model")}


def host_facts(data_dir: str | None) -> dict:
    memory = None
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    memory = int(line.split()[1]) * 1024
    except OSError:
        pass
    return {"cpus": os.cpu_count() or 1, "kernel": platform.release(),
            "machine": platform.machine(), "python": platform.python_version(),
            "memory_bytes": memory, "disk": disk_facts(data_dir)}


def execute(target: Target, profile: str, params: dict, log=print) -> dict:
    """Runs the scenarios `params` names against the target, in order, and
    returns the result document."""
    run_id = secrets.token_hex(3)
    started = now()
    with Generator(params["processes"]) as generator:
        context = Context(target=target, params=params, generator=generator, run_id=run_id,
                          log=log)
        first = context.snapshot()
        result = {
            "schema": SCHEMA_NAME, "version": SCHEMA_VERSION, "run_id": run_id,
            "started_at": started, "finished_at": started,
            "git": git_facts(os.path.dirname(os.path.abspath(__file__))),
            "profile": profile, "parameters": params, "host": host_facts(target.data_dir),
            "cluster": {"name": target.name, "nodes": [n.name for n in target.nodes],
                        "binary_version": first.label_value("skys3_build_info", "version"),
                        "facts": target.cluster},
            "scenarios": [],
        }
        for name in params["scenarios"]:
            result["scenarios"].append(run_scenario(context, name, log))
    result["finished_at"] = now()
    return result


def run_scenario(context: Context, name: str, log=print) -> dict:
    scenario = SCENARIOS[name]
    entry = {"name": name, "rows": list(scenario.rows), "status": "ok", "reason": None,
             "duration_seconds": 0.0, "cases": []}
    started = time.monotonic()
    if scenario.run is None:
        entry["status"] = "not_implemented"
        entry["reason"] = "an extension point: this harness does not measure the row yet"
        return entry
    log(f"scenario {name}: started")
    try:
        entry["cases"] = scenario.run(context)
    except Skip as skip:
        entry["status"], entry["reason"] = "skipped", str(skip)
    except Exception as error:  # noqa: BLE001 - a failed scenario must not stop the run
        entry["status"], entry["reason"] = "failed", f"{type(error).__name__}: {error}"
        log(traceback.format_exc())
    entry["duration_seconds"] = time.monotonic() - started
    log(f"scenario {name}: {entry['status']} in {entry['duration_seconds']:.1f} s"
        + (f" ({entry['reason']})" if entry["reason"] else ""))
    return entry


def check(result: dict, require: list[str]) -> list[str]:
    """What is wrong with a result: schema errors; a required scenario that
    did not run (`ok`); a case of a scenario that ran without samples in
    any distribution; and a metric-derived value that is missing, or not
    above 0 where it must be. No performance threshold is checked."""
    problems = schema.validate(result)
    if problems:
        return problems
    by_name = {s["name"]: s for s in result["scenarios"]}
    for name in require:
        scenario = by_name.get(name)
        if scenario is None:
            problems.append(f"{name}: not in the result")
        elif scenario["status"] != "ok":
            problems.append(f"{name}: {scenario['status']} ({scenario['reason']})")
    for scenario in result["scenarios"]:
        if scenario["status"] != "ok":
            continue
        name = scenario["name"]
        measured = [c for c in scenario["cases"] if "skipped" not in c["values"]]
        if not measured:
            problems.append(f"{name}: no case was measured")
        for case in measured:
            where = f"{name} {case['labels']}"
            if not any(d["count"] > 0 for d in case["distributions"].values()):
                problems.append(f"{where}: no distribution has samples")
            for key in REQUIRED_POSITIVE.get(name, []):
                value = case["values"].get(key)
                if not isinstance(value, (int, float)) or value <= 0:
                    problems.append(f"{where}: {key} is {value!r}, not above 0")
            for key in REQUIRED_PRESENT.get(name, []):
                if case["labels"].get("kill") == "all":
                    continue
                if not isinstance(case["values"].get(key), (int, float)):
                    problems.append(f"{where}: {key} is missing")
        if name == "failover" and not any(
                d.startswith("recovery_") and v["count"] > 0
                for c in measured for d, v in c["distributions"].items()):
            problems.append("failover: no shard recovered after a kill")
    return problems
