import math
import unittest

from skys3perf import stats


class Percentiles(unittest.TestCase):
    def test_interpolates_between_the_closest_ranks(self):
        ordered = [1.0, 2.0, 3.0, 4.0]
        self.assertEqual(stats.percentile(ordered, 0), 1.0)
        self.assertEqual(stats.percentile(ordered, 100), 4.0)
        self.assertAlmostEqual(stats.percentile(ordered, 50), 2.5)
        self.assertAlmostEqual(stats.percentile(ordered, 90), 3.7)

    def test_matches_a_known_series(self):
        ordered = [float(i) for i in range(1, 101)]
        self.assertAlmostEqual(stats.percentile(ordered, 99), 99.01)
        self.assertAlmostEqual(stats.percentile(ordered, 99.9), 99.901)

    def test_one_sample_is_every_percentile(self):
        for p in (0, 50, 99.9, 100):
            self.assertEqual(stats.percentile([7.0], p), 7.0)

    def test_no_samples_have_none(self):
        self.assertIsNone(stats.percentile([], 50))

    def test_refuses_a_percentile_out_of_range(self):
        for p in (-1, 100.5):
            with self.assertRaises(ValueError):
                stats.percentile([1.0], p)

    def test_keys(self):
        self.assertEqual([stats.percentile_key(p) for p in stats.PERCENTILES],
                         ["p50", "p90", "p99", "p99.9"])


class Histogram(unittest.TestCase):
    def test_the_125_scale(self):
        cases = {0.0003: 0.0005, 0.001: 0.001, 0.0011: 0.002, 0.3: 0.5, 0.2 * 1.5: 0.5,
                 3: 5, 5: 5, 7: 10, 1000: 1000, 1001: 2000}
        for value, bound in cases.items():
            self.assertEqual(stats.scale_125(value), bound, value)
        with self.assertRaises(ValueError):
            stats.scale_125(0)

    def test_buckets_cover_the_samples_and_count_each_once(self):
        samples = [0.0012, 0.003, 0.004, 0.02, 0.5, 0.5]
        h = stats.histogram(samples)
        self.assertEqual(h["bounds"], [0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5])
        self.assertEqual(h["counts"], [1, 2, 0, 1, 0, 0, 0, 2])
        self.assertEqual(sum(h["counts"]), len(samples))

    def test_a_sample_on_a_bound_falls_in_its_bucket(self):
        h = stats.histogram([1.0, 2.0, 5.0])
        self.assertEqual(h["bounds"], [1.0, 2.0, 5.0])
        self.assertEqual(h["counts"], [1, 1, 1])

    def test_zero_and_empty(self):
        self.assertEqual(stats.histogram([]), {"bounds": [], "counts": []})
        self.assertEqual(stats.histogram([0.0, 0.0]), {"bounds": [1.0], "counts": [2]})
        self.assertEqual(stats.histogram([0.0, 3.0]), {"bounds": [5.0], "counts": [2]})


class Summaries(unittest.TestCase):
    def test_summary_fields(self):
        d = stats.summarize([3.0, 1.0, 2.0], "seconds", samples_max=10)
        self.assertEqual(d["count"], 3)
        self.assertEqual((d["min"], d["max"], d["mean"]), (1.0, 3.0, 2.0))
        self.assertEqual(d["percentiles"]["p50"], 2.0)
        self.assertEqual(d["samples"], [3.0, 1.0, 2.0])
        self.assertEqual(sum(d["histogram"]["counts"]), 3)

    def test_samples_are_dropped_past_the_limit(self):
        d = stats.summarize([1.0] * 11, "seconds", samples_max=10)
        self.assertIsNone(d["samples"])
        self.assertEqual(d["count"], 11)

    def test_empty(self):
        d = stats.summarize([], "bytes", samples_max=10)
        self.assertEqual(d["count"], 0)
        self.assertIsNone(d["mean"])
        self.assertTrue(all(v is None for v in d["percentiles"].values()))
        self.assertEqual(d["samples"], [])

    def test_ratio(self):
        self.assertEqual(stats.ratio(6, 3), 2)
        self.assertIsNone(stats.ratio(None, 3))
        self.assertIsNone(stats.ratio(1, 0))
        self.assertIsNone(stats.ratio(1, None))
        self.assertTrue(math.isfinite(stats.ratio(1, 1e-9)))


if __name__ == "__main__":
    unittest.main()
