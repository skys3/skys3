import copy
import json
import unittest

from skys3perf import schema, stats


def result() -> dict:
    """A small result that conforms."""
    return {
        "schema": "skys3-perf-result", "version": 1, "run_id": "abc123",
        "started_at": "2026-10-21T00:00:00+00:00", "finished_at": "2026-10-21T00:01:00+00:00",
        "git": {"sha": "0" * 40, "dirty": False}, "profile": "smoke",
        "parameters": {"put.sizes": [1024]},
        "host": {"cpus": 4, "kernel": "6.1", "machine": "x86_64", "python": "3.12.1",
                 "memory_bytes": 1 << 34, "disk": {"device": "nvme0n1", "rotational": False,
                                                   "model": None}},
        "cluster": {"name": "c", "nodes": ["n1"], "binary_version": "0.1.0", "facts": {}},
        "scenarios": [
            {"name": "put", "rows": ["PUT"], "status": "ok", "reason": None,
             "duration_seconds": 1.5,
             "cases": [{"labels": {"size": "1KiB"},
                        "distributions": {"latency": stats.summarize([0.1, 0.2], "seconds", 10)},
                        "values": {"fsyncs_per_put": 3.0, "throughput_ops": 10.0,
                                   "group_commits_per_put": 3.0, "records_per_group_commit": 1.0,
                                   "mean_group_commit_seconds": 0.001},
                        "series": {"dirty_bytes": [[1.0, 2.0]]}}]},
            {"name": "peer", "rows": ["peer"], "status": "not_implemented", "reason": "later",
             "duration_seconds": 0, "cases": []},
        ],
    }


class Validation(unittest.TestCase):
    def test_the_schema_is_json_with_a_version(self):
        loaded = schema.load()
        self.assertEqual(loaded["properties"]["version"], {"const": 1})
        json.dumps(loaded)

    def test_a_conforming_result(self):
        self.assertEqual(schema.validate(result()), [])

    def test_each_kind_of_breach(self):
        breaches = {
            "missing": (lambda r: r.pop("git"), "$: git is missing"),
            "extra": (lambda r: r.update(color="red"), "$: color is not allowed"),
            "const": (lambda r: r.update(version=2), "$.version: 2 is not 1"),
            "enum": (lambda r: r["scenarios"][0].update(status="great"),
                     "$.scenarios[0].status: 'great' is not one of"),
            "type": (lambda r: r["host"].update(cpus="four"), "$.host.cpus: str is not integer"),
            "bool is no integer": (lambda r: r["host"].update(cpus=True),
                                   "$.host.cpus: bool is not integer"),
            "minimum": (lambda r: r["host"].update(cpus=0), "$.host.cpus: 0 is below 1"),
            "ref": (lambda r: r["scenarios"][0]["cases"][0]["distributions"]["latency"]
                    .update(count=-1),
                    "$.scenarios[0].cases[0].distributions.latency.count: -1 is below 0"),
            "items": (lambda r: r["scenarios"][0]["cases"][0]["series"]["dirty_bytes"]
                      .append(["x"]),
                      "$.scenarios[0].cases[0].series.dirty_bytes[1][0]: str is not number"),
        }
        for name, (breach, expected) in breaches.items():
            broken = copy.deepcopy(result())
            breach(broken)
            errors = schema.validate(broken)
            self.assertTrue(any(e.startswith(expected) for e in errors), (name, errors))

    def test_only_local_references(self):
        with self.assertRaises(ValueError):
            schema.validate({}, {"$ref": "http://example.com/schema"})


if __name__ == "__main__":
    unittest.main()
