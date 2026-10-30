import unittest

from skys3perf import params
from skys3perf.params import ParamError


class Parsing(unittest.TestCase):
    def test_sizes(self):
        self.assertEqual(params.parse_size("4096"), 4096)
        self.assertEqual(params.parse_size("4096B"), 4096)
        self.assertEqual(params.parse_size("64KiB"), 65536)
        self.assertEqual(params.parse_size(" 1GiB "), 1 << 30)
        self.assertEqual(params.parse_size("1.5MiB"), 3 << 19)
        for bad in ("", "1KB", "-1", "lots", "1 kib"):
            with self.assertRaises(ParamError, msg=bad):
                params.parse_size(bad)

    def test_sizes_print_back(self):
        for size in (1, 1536, 1 << 10, 16 << 20, 1 << 30, 1 << 40):
            self.assertEqual(params.parse_size(params.format_size(size)), size)
        self.assertEqual(params.format_size(1536), "1536B")
        self.assertEqual(params.format_size(0), "0B")

    def test_durations(self):
        self.assertEqual(params.parse_duration("500ms"), 0.5)
        self.assertEqual(params.parse_duration("30"), 30.0)
        self.assertEqual(params.parse_duration("2m"), 120.0)
        self.assertEqual(params.parse_duration("1h"), 3600.0)
        with self.assertRaises(ParamError):
            params.parse_duration("soon")

    def test_booleans(self):
        self.assertTrue(params.parse_bool("yes"))
        self.assertFalse(params.parse_bool("0"))
        with self.assertRaises(ParamError):
            params.parse_bool("maybe")


class Profiles(unittest.TestCase):
    def test_every_parameter_has_both_defaults_of_its_kind(self):
        for profile in params.PROFILES:
            values = params.resolve(profile)
            self.assertEqual(set(values), set(params.BY_NAME))
            for param in params.PARAMS:
                value = values[param.name]
                self.assertEqual(isinstance(value, list), param.is_list, param.name)
                self.assertTrue(param.doc.endswith("."), param.name)

    def test_smoke_is_smaller_than_full(self):
        smoke, full = params.resolve("smoke"), params.resolve("full")
        self.assertLess(max(smoke["put.sizes"]), max(full["put.sizes"]))
        self.assertLess(smoke["put.duration"], full["put.duration"])
        self.assertEqual(full["put.sizes"][-1], 1 << 30)
        self.assertIn(150.0, full["flush.rtts_ms"])
        self.assertEqual(full["failover.control_rtts_ms"], [1.0, 100.0])

    def test_overrides(self):
        values = params.resolve("smoke", ["put.sizes=1KiB,1MiB", "put.duration=500ms",
                                          "failover.restart=false", "scenarios=put"])
        self.assertEqual(values["put.sizes"], [1024, 1 << 20])
        self.assertEqual(values["put.duration"], 0.5)
        self.assertFalse(values["failover.restart"])
        self.assertEqual(values["scenarios"], ["put"])

    def test_bad_overrides(self):
        for bad in (["nothing"], ["no.such=1"], ["put.concurrency=a"], ["scenarios=put,nope"],
                    ["put.sizes=1KB"]):
            with self.assertRaises(ParamError, msg=bad):
                params.resolve("smoke", bad)
        with self.assertRaises(ParamError):
            params.resolve("huge")

    def test_described(self):
        table = params.describe()
        self.assertIn("| `put.sizes` | list of size | 1KiB, 64KiB |", table)
        self.assertIn("| `put.duration` | duration | 2s | 30s |", table)
        self.assertIn("| `failover.restart` | bool | true | true |", table)
        self.assertEqual(len(table.splitlines()), len(params.PARAMS) + 2)


if __name__ == "__main__":
    unittest.main()
