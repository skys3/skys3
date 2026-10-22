import copy
import os
import tempfile
import unittest

from fakes import FakeNode
from test_schema import result

from skys3perf import report, runner
from skys3perf.scenarios import SCENARIOS, Context, Skip, longest_gap
from skys3perf.target import Target


def target(node: FakeNode, **extra) -> Target:
    return Target.parse({"nodes": [{"name": "n1", "s3": node.url, "admin": node.address}],
                         "s3": {"access_key": "a", "secret_key": "s"}, **extra})


class Checks(unittest.TestCase):
    def test_a_good_result_passes(self):
        self.assertEqual(runner.check(result(), ["put"]), [])

    def test_schema_errors_come_first(self):
        broken = result()
        broken["version"] = 2
        self.assertEqual(runner.check(broken, []), ["$.version: 2 is not 1"])

    def test_required_scenarios_must_have_run(self):
        problems = runner.check(result(), ["put", "peer", "flush"])
        self.assertEqual(problems, ["peer: not_implemented (later)", "flush: not in the result"])

    def test_samples_and_metric_values(self):
        broken = result()
        case = broken["scenarios"][0]["cases"][0]
        case["distributions"]["latency"]["count"] = 0
        case["values"]["fsyncs_per_put"] = 0
        del case["values"]["throughput_ops"]
        problems = runner.check(broken, [])
        self.assertIn("put {'size': '1KiB'}: no distribution has samples", problems)
        self.assertIn("put {'size': '1KiB'}: fsyncs_per_put is 0, not above 0", problems)
        self.assertIn("put {'size': '1KiB'}: throughput_ops is None, not above 0", problems)

    def test_skipped_cases_and_empty_scenarios(self):
        broken = result()
        broken["scenarios"][0]["cases"] = [{"labels": {}, "distributions": {}, "series": {},
                                           "values": {"skipped": "no remote"}}]
        self.assertEqual(runner.check(broken, []), ["put: no case was measured"])

    def test_failover_needs_a_recovery_and_its_values(self):
        broken = result()
        broken["scenarios"][0]["name"] = "failover"
        problems = runner.check(broken, [])
        self.assertIn("failover: no shard recovered after a kill", problems)
        self.assertIn("failover {'size': '1KiB'}: removed_seconds is missing", problems)


class Scenarios(unittest.TestCase):
    def test_every_scenario_names_its_rows(self):
        for scenario in SCENARIOS.values():
            self.assertTrue(scenario.rows, scenario.name)

    def test_extension_points_and_skips_and_failures(self):
        with FakeNode() as node:
            context = Context(target=target(node), params={"request_timeout": 5},
                              generator=None, run_id="r", log=lambda _: None)  # type: ignore[arg-type]
            entry = runner.run_scenario(context, "peer", log=lambda _: None)
            self.assertEqual(entry["status"], "not_implemented")
            for name in ("flush", "import", "get"):
                entry = runner.run_scenario(context, name, log=lambda _: None)
                self.assertEqual(entry["status"], "skipped", name)
                self.assertIn("no remote", entry["reason"])
            entry = runner.run_scenario(context, "failover", log=lambda _: None)
            self.assertEqual((entry["status"], entry["reason"]),
                             ("skipped", "the target has no kill and start hooks"))
            entry = runner.run_scenario(context, "put", log=lambda _: None)
            self.assertEqual(entry["status"], "failed")
            self.assertIn("KeyError", entry["reason"])

    def test_skip_is_an_exception(self):
        self.assertTrue(issubclass(Skip, Exception))

    def test_longest_gap(self):
        self.assertEqual(longest_gap(10.0, [], 15.0), 5.0)
        self.assertEqual(longest_gap(10.0, [11.0, 14.0, 9.0, 16.0], 15.0), 3.0)
        self.assertAlmostEqual(longest_gap(10.0, [10.5, 11.0], 11.2), 0.5)


class Facts(unittest.TestCase):
    def test_host_and_git(self):
        host = runner.host_facts("/")
        self.assertGreaterEqual(host["cpus"], 1)
        self.assertIn("kernel", host)
        self.assertIsNone(runner.disk_facts("/no/such/path"))
        self.assertIsNone(runner.disk_facts(None))
        git = runner.git_facts(os.path.dirname(__file__))
        self.assertTrue(git["sha"] is None or len(git["sha"]) == 40)
        with tempfile.TemporaryDirectory() as empty:
            self.assertEqual(runner.git_facts(empty)["sha"], None)
        os.environ["SKYS3_PERF_GIT_SHA"] = "f" * 40
        try:
            self.assertEqual(runner.git_facts(os.path.dirname(__file__))["sha"], "f" * 40)
        finally:
            del os.environ["SKYS3_PERF_GIT_SHA"]


class Reports(unittest.TestCase):
    def test_summary(self):
        text = report.summary(result())
        self.assertIn("# SkyS3 performance run abc123 (smoke)", text)
        self.assertIn("## put: ok (1.5 s)", text)
        self.assertIn("- size=1KiB: latency n=2 p50=150 ms p99=199 ms max=200 ms; "
                      "throughput_ops=10; fsyncs_per_put=3;", text)
        self.assertIn("## peer: not_implemented", text)

    def test_show(self):
        self.assertEqual(report.show(None), "-")
        self.assertEqual(report.show(0.0015, "seconds"), "1.5 ms")
        self.assertEqual(report.show(12.0, "drain_seconds"), "12 s")
        self.assertEqual(report.show(3 << 20, "dirty_bytes_max"), "3 MiB")
        self.assertEqual(report.show(2048.0, "throughput_bytes"), "2 KiB/s")
        self.assertEqual(report.show(10, "throughput_bytes"), "10 B/s")
        self.assertEqual(report.show(7), "7")
        self.assertEqual(report.show("n2"), "n2")
        self.assertEqual(report.show(True), "True")

    def test_compare(self):
        base, new = result(), copy.deepcopy(result())
        new["scenarios"][0]["cases"][0]["values"]["fsyncs_per_put"] = 1.5
        table = report.compare(base, new)
        self.assertIn("| put | size=1KiB | latency p50 | 150 ms | 150 ms | +0.0% |", table)
        self.assertIn("| put | size=1KiB | fsyncs_per_put | 3 | 1.5 | -50.0% |", table)
        new["scenarios"][0]["cases"][0]["labels"] = {"size": "2KiB"}
        self.assertIn("share no case", report.compare(base, new))


if __name__ == "__main__":
    unittest.main()
