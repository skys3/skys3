#!/usr/bin/env python3
"""The SkyS3 performance harness (plan M7-06a; design section 16.3).

    perf.py run --target <target.json> [--profile smoke|full]
                [--set <name>=<value>]... [--out <dir>]
    perf.py check <result.json> [--require <scenario>,...|all]
    perf.py compare <base.json> <new.json>
    perf.py params

`run` drives the cluster a target file describes through the scenarios of
the profile (or `--set scenarios=...`), samples every node's metrics
meanwhile, and writes `result.json` (schema: result.schema.json) and
`summary.md` to <dir> (default ./perf-out), printing the summary. It exits
1 if a scenario failed. `check` validates a result against the schema and
checks that the scenarios ran end to end, with samples and metric-derived
values; it checks no performance threshold. `--require all` requires every
implemented scenario the run named. `compare` prints the change
of each shared measure between two results. `params` prints every
parameter with its defaults.

docs/skys3-performance.md is the runbook: the target file, the profiles,
shaping links, and how M6-09 and M7-06 use it. Only the standard library
is used.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from skys3perf import SCHEMA_VERSION, params, report, runner  # noqa: E402
from skys3perf.target import Target, TargetError  # noqa: E402


def log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def load_result(path: str) -> dict:
    with open(path) as f:
        result = json.load(f)
    if result.get("version") != SCHEMA_VERSION:
        raise SystemExit(f"{path} is a result of schema version {result.get('version')}, "
                         f"not {SCHEMA_VERSION}")
    return result


def run(args: argparse.Namespace) -> int:
    try:
        target = Target.load(args.target)
        values = params.resolve(args.profile, args.set)
    except (TargetError, params.ParamError, OSError, ValueError) as error:
        log(f"perf.py: {error}")
        return 2
    os.makedirs(args.out, exist_ok=True)
    result = runner.execute(target, args.profile, values, log)
    with open(os.path.join(args.out, "result.json"), "w") as f:
        json.dump(result, f, indent=1, allow_nan=False)
        f.write("\n")
    text = report.summary(result)
    with open(os.path.join(args.out, "summary.md"), "w") as f:
        f.write(text)
    print(text)
    return 1 if any(s["status"] == "failed" for s in result["scenarios"]) else 0


def check(args: argparse.Namespace) -> int:
    result = load_result(args.result)
    require = [name for name in (args.require or "").split(",") if name]
    if require == ["all"]:
        # Every scenario the run asked for that this harness implements.
        require = [name for name in result["parameters"].get("scenarios", [])
                   if name in params.IMPLEMENTED]
    problems = runner.check(result, require)
    for problem in problems:
        print(f"FAILED: {problem}")
    if not problems:
        ran = [s["name"] for s in result["scenarios"] if s["status"] == "ok"]
        print(f"{args.result}: valid; ran end to end: {', '.join(ran) or 'nothing'}")
    return 1 if problems else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    r = commands.add_parser("run", help="run the scenarios against a cluster")
    r.add_argument("--target", required=True, help="the target file")
    r.add_argument("--profile", choices=params.PROFILES, default="smoke")
    r.add_argument("--set", action="append", default=[], metavar="NAME=VALUE",
                   help="override a parameter (repeatable)")
    r.add_argument("--out", default="perf-out", help="where result.json and summary.md go")
    c = commands.add_parser("check", help="validate a result and check it ran end to end")
    c.add_argument("result")
    c.add_argument("--require", help="scenarios that must have run, comma-separated, or "
                   "`all` for every implemented scenario the run named")
    d = commands.add_parser("compare", help="compare two results")
    d.add_argument("base")
    d.add_argument("new")
    commands.add_parser("params", help="list the parameters and their defaults")
    args = parser.parse_args()
    if args.command == "run":
        return run(args)
    if args.command == "check":
        return check(args)
    if args.command == "compare":
        print(report.compare(load_result(args.base), load_result(args.new)), end="")
        return 0
    print(params.describe())
    return 0


if __name__ == "__main__":
    sys.exit(main())
