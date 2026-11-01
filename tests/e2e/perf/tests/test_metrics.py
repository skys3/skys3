import time
import unittest

from fakes import FakeNode

from skys3perf import metrics

TEXT = """# HELP skys3_log_syncs File syncs.
# TYPE skys3_log_syncs counter
skys3_log_syncs_total 12
skys3_dirty_bytes{bucket="a"} 100
skys3_dirty_bytes{bucket="b"} 50.5
skys3_build_info{version="0.1.0"} 1
skys3_flush_lag_seconds{bucket="a",target="x\\"y"} 2.5 1700000000
odd{a="1"} not-a-number
# EOF
"""


class Parsing(unittest.TestCase):
    def test_samples_and_labels(self):
        parsed = metrics.parse(TEXT)
        self.assertEqual(parsed["skys3_log_syncs_total"], {(): 12.0})
        self.assertEqual(parsed["skys3_dirty_bytes"][(("bucket", "a"),)], 100.0)
        self.assertEqual(
            parsed["skys3_flush_lag_seconds"][(("bucket", "a"), ("target", 'x"y'))], 2.5)
        self.assertNotIn("odd", parsed)
        self.assertNotIn("#", "".join(parsed))

    def test_value_sums_matching_series(self):
        parsed = metrics.parse(TEXT)
        self.assertEqual(metrics.value(parsed, "skys3_dirty_bytes"), 150.5)
        self.assertEqual(metrics.value(parsed, "skys3_dirty_bytes", bucket="b"), 50.5)
        self.assertIsNone(metrics.value(parsed, "skys3_dirty_bytes", bucket="c"))
        self.assertIsNone(metrics.value(parsed, "skys3_missing"))
        self.assertIsNone(metrics.value(None, "skys3_dirty_bytes"))


def snapshot(**nodes: str | None) -> metrics.Snapshot:
    return metrics.Snapshot(at=0.0, nodes={name: None if text is None else metrics.parse(text)
                                           for name, text in nodes.items()})


class Deltas(unittest.TestCase):
    def test_counters_grow_by_the_difference_summed_over_nodes(self):
        before = snapshot(n1="c_total 10\n", n2="c_total 5\n")
        after = snapshot(n1="c_total 15\n", n2="c_total 6\n")
        self.assertEqual(metrics.counter_delta(before, after, "c_total"), 6.0)

    def test_a_restarted_node_counts_from_zero(self):
        before = snapshot(n1="c_total 10\n", n2="c_total 50\n")
        after = snapshot(n1="c_total 12\n", n2="c_total 3\n")
        self.assertEqual(metrics.counter_delta(before, after, "c_total"), 5.0)

    def test_a_node_missing_before_counts_all_and_one_missing_after_nothing(self):
        before = snapshot(n1="c_total 10\n", n2=None)
        after = snapshot(n1=None, n2="c_total 4\n")
        self.assertEqual(metrics.counter_delta(before, after, "c_total"), 4.0)
        self.assertIsNone(metrics.counter_delta(before, snapshot(n1=None), "c_total"))

    def test_labels_filter(self):
        before = snapshot(n1='f_total{bucket="a"} 1\nf_total{bucket="b"} 1\n')
        after = snapshot(n1='f_total{bucket="a"} 4\nf_total{bucket="b"} 9\n')
        self.assertEqual(metrics.counter_delta(before, after, "f_total", bucket="a"), 3.0)

    def test_totals_highest_and_labels(self):
        taken = snapshot(n1=TEXT, n2='skys3_dirty_bytes{bucket="a"} 1\n', n3=None)
        self.assertEqual(taken.total("skys3_dirty_bytes", bucket="a"), 101.0)
        self.assertEqual(taken.highest("skys3_dirty_bytes", bucket="a"), 100.0)
        self.assertIsNone(taken.total("nope"))
        self.assertIsNone(taken.highest("nope"))
        self.assertEqual(taken.label_value("skys3_build_info", "version"), "0.1.0")
        self.assertIsNone(taken.label_value("skys3_build_info", "commit"))

    def test_peak(self):
        self.assertEqual(metrics.peak([[0, 1.0], [1, 3.0], [2, 2.0]]), 3.0)
        self.assertIsNone(metrics.peak([]))


class Sampling(unittest.TestCase):
    def test_samples_nodes_until_stopped(self):
        with FakeNode() as node:
            count = {"n": 0}

            def text() -> str:
                count["n"] += 1
                return f'skys3_dirty_bytes{{bucket="a"}} {count["n"]}\n'

            node.metrics = text
            sampler = metrics.Sampler({"n1": node.address, "gone": "127.0.0.1:9"}, None, 0.05)
            sampler.start()
            time.sleep(0.3)
            taken = sampler.stop()
        self.assertGreaterEqual(len(taken), 2)
        self.assertTrue(all(s.nodes["gone"] is None for s in taken))
        series = sampler.series("skys3_dirty_bytes", bucket="a")
        self.assertEqual([v for _, v in series], sorted(v for _, v in series))
        self.assertEqual(sampler.series("skys3_dirty_bytes", "highest", bucket="a"), series)
        self.assertEqual(sampler.series("skys3_missing"), [])

    def test_fetch_sends_the_token(self):
        with FakeNode() as node:
            node.metrics = lambda: "x 1\n"
            self.assertEqual(metrics.fetch(node.address, "secret"), {"x": {(): 1.0}})


if __name__ == "__main__":
    unittest.main()
