"""A pytest plugin that adapts ceph/s3-tests to SkyS3 (plans M1-25 and M7-01).

It does three things.

1. Cleanup. s3-tests empties its buckets before and after every test by
   listing object versions, which SkyS3 rejects with 501 NotImplemented
   (design section 11: no local versioning APIs). Without versions,
   ListObjectsV2 lists the same objects, so the cleanup lists them that
   way. Nothing else about the tests changes.

2. Selection. Every test s3-tests has must be classified: selected in
   subset.txt, or excluded by a rule of excluded.txt, which says why. The
   run fails before any test starts if a test is in neither, or if a
   line of either file matches no test, so a new pinned revision cannot
   add or rename tests unnoticed. Only the selected tests run, unless
   SKYS3_S3TESTS_ALL=1 runs every test (to classify a new revision).

3. Results. At the end it writes the results per group, as the
   compatibility matrix's s3-tests section (docs/skys3-s3-compatibility.md),
   to SKYS3_S3TESTS_REPORT, and every test's outcome, one per line, to
   SKYS3_S3TESTS_OUTCOMES, when those are set.

The list files are read from SKYS3_S3TESTS_LISTS (default: this file's
directory). Both are UTF-8 text in which a line starting with `## ` opens
a group, a line starting with `#` is a comment, and any other non-blank
line names tests:

- subset.txt: `## <group>`, then pytest node IDs.
- excluded.txt: `## <kind> | <group> | <why>`, where the kind is
  `unsupported`, `divergence`, `assumption`, or `open` (a gap in a
  supported feature, listed until it is fixed), then rules: a node ID, a
  node ID pattern in which `*` matches any text, a file path (every test
  in that file), or `marker:<name>` (every test with that pytest marker).
  A test belongs to the first rule that matches it.
"""

import os
import re
from collections import Counter

import pytest
import s3tests.functional as functional

KINDS = {
    "unsupported": "Not supported",
    "divergence": "Known divergence",
    "assumption": "Test-suite assumption",
    "open": "Open point",
}


def _list_objects(client, bucket, batch_size):
    """Yields the bucket's keys in batches, as `list_versions` does."""
    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, PaginationConfig={"PageSize": batch_size})
    for page in pages:
        objects = page.get("Contents", [])
        if objects:
            yield [{"Key": o["Key"]} for o in objects]


functional.list_versions = _list_objects


class Group:
    """A group of subset.txt or excluded.txt and the lines under it."""

    def __init__(self, name, kind=None, why=None):
        self.name = name
        self.kind = kind
        self.why = why
        self.lines = []


def _read_groups(path, excluded):
    groups = []
    with open(path, encoding="utf-8") as lines:
        for number, line in enumerate(lines, 1):
            line = line.strip()
            if line.startswith("## "):
                heading = line[3:].strip()
                if excluded:
                    fields = [field.strip() for field in heading.split("|")]
                    if len(fields) != 3 or fields[0] not in KINDS or not all(fields):
                        raise pytest.UsageError(
                            f"{path}:{number}: expected `## <kind> | <group> | <why>` "
                            f"with a kind of {', '.join(KINDS)}"
                        )
                    groups.append(Group(fields[1], fields[0], fields[2]))
                else:
                    groups.append(Group(heading))
            elif line and not line.startswith("#"):
                if not groups:
                    raise pytest.UsageError(f"{path}:{number}: a test before any `## ` group")
                groups[-1].lines.append((number, line))
    return groups


UNCLASSIFIED = Group("Unclassified")


class Rule:
    """One line of excluded.txt."""

    def __init__(self, group, number, text):
        self.group = group
        self.number = number
        self.text = text
        self.hits = 0
        if text.startswith("marker:"):
            marker = text[len("marker:"):]
            self.matches = lambda item: item.get_closest_marker(marker) is not None
        elif "::" not in text:
            self.matches = lambda item: item.nodeid.startswith(text + "::")
        else:
            pattern = re.compile(".*".join(re.escape(part) for part in text.split("*")))
            self.matches = lambda item: pattern.fullmatch(item.nodeid) is not None


class Selection:
    """Which group each collected test belongs to, and its outcome."""

    def __init__(self, lists):
        self.subset = _read_groups(os.path.join(lists, "subset.txt"), excluded=False)
        self.excluded = _read_groups(os.path.join(lists, "excluded.txt"), excluded=True)
        self.group_of = {}
        self.outcomes = {}

    def classify(self, items):
        """Assigns every item a group; returns the problems found."""
        problems = []
        selected = {}
        for group in self.subset:
            for number, nodeid in group.lines:
                if nodeid in selected:
                    problems.append(f"subset.txt:{number}: {nodeid} is listed twice")
                selected[nodeid] = group
        rules = [Rule(group, number, text) for group in self.excluded for number, text in group.lines]
        collected = set()
        for item in items:
            collected.add(item.nodeid)
            group = selected.get(item.nodeid)
            if group is None:
                rule = next((rule for rule in rules if rule.matches(item)), None)
                if rule is None:
                    problems.append(f"{item.nodeid} is neither selected nor excluded")
                    self.group_of[item.nodeid] = UNCLASSIFIED
                    continue
                rule.hits += 1
                group = rule.group
            self.group_of[item.nodeid] = group
        for nodeid, group in selected.items():
            if nodeid not in collected:
                problems.append(f"subset.txt: {nodeid} ({group.name}) is not a test")
        for rule in rules:
            if rule.hits == 0:
                problems.append(f"excluded.txt:{rule.number}: {rule.text} matches no test")
        return problems

    def report(self):
        """The compatibility matrix's s3-tests section, in Markdown."""
        tests = Counter(self.group_of.values())
        lines = [
            "| Group | Tests | Passed |",
            "|---|---:|---:|",
        ]
        totals = Counter()
        for group in self.subset:
            outcomes = Counter(
                self.outcomes.get(nodeid, "not run")
                for nodeid, of in self.group_of.items()
                if of is group
            )
            totals.update(outcomes)
            lines.append(f"| {group.name} | {tests[group]} | {outcomes['passed']} |")
        selected = sum(tests[group] for group in self.subset)
        lines.append(f"| **Selected** | **{selected}** | **{totals['passed']}** |")
        lines += [
            "",
            "| Excluded group | Kind | Why | Tests |",
            "|---|---|---|---:|",
        ]
        for group in self.excluded:
            lines.append(f"| {group.name} | {KINDS[group.kind]} | {group.why} | {tests[group]} |")
        excluded = sum(tests[group] for group in self.excluded)
        lines.append(f"| **Excluded** | | | **{excluded}** |")
        return "\n".join(lines) + "\n"

    # Hooks, as a plugin registered by `pytest_configure`.

    def pytest_collection_modifyitems(self, config, items):
        problems = self.classify(items)
        run_all = os.environ.get("SKYS3_S3TESTS_ALL") == "1"
        if problems and run_all:
            print("\nthe s3-tests lists do not match the tests:\n  " + "\n  ".join(problems))
        elif problems:
            raise pytest.UsageError(
                "the s3-tests lists do not match the tests:\n  " + "\n  ".join(problems)
            )
        if run_all:
            return
        subset = set(self.subset)
        keep = [item for item in items if self.group_of[item.nodeid] in subset]
        dropped = [item for item in items if self.group_of[item.nodeid] not in subset]
        if dropped:
            config.hook.pytest_deselected(items=dropped)
        items[:] = keep

    def pytest_runtest_logreport(self, report):
        if report.failed:
            self.outcomes[report.nodeid] = "failed"
        elif report.skipped:
            self.outcomes.setdefault(report.nodeid, "skipped")
        elif report.when == "call":
            self.outcomes.setdefault(report.nodeid, "passed")

    def pytest_sessionfinish(self, session):
        path = os.environ.get("SKYS3_S3TESTS_REPORT")
        if path and self.group_of:
            with open(path, "w", encoding="utf-8") as report:
                report.write(self.report())
        path = os.environ.get("SKYS3_S3TESTS_OUTCOMES")
        if path:
            with open(path, "w", encoding="utf-8") as outcomes:
                for nodeid in sorted(self.group_of):
                    group = self.group_of[nodeid].name
                    outcome = self.outcomes.get(nodeid, "not run")
                    outcomes.write(f"{outcome}\t{group}\t{nodeid}\n")


def pytest_configure(config):
    lists = os.environ.get("SKYS3_S3TESTS_LISTS", os.path.dirname(os.path.abspath(__file__)))
    config.pluginmanager.register(Selection(lists), "skys3-selection")
