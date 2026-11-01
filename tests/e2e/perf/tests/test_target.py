import os
import sys
import tempfile
import unittest

from fakes import FakeNode

from skys3perf.target import Target, TargetError, shard_of

BASE = {"nodes": [{"name": "n1", "s3": "http://127.0.0.1:1/", "admin": "127.0.0.1:2"}],
        "s3": {"access_key": "a", "secret_key": "s"}}


class Parsing(unittest.TestCase):
    def test_a_minimal_target(self):
        target = Target.parse(BASE)
        self.assertEqual(target.nodes[0].s3, "http://127.0.0.1:1")
        self.assertEqual(target.credentials.region, "us-east-1")
        self.assertEqual(target.admins(), {"n1": "127.0.0.1:2"})
        self.assertFalse(target.has_hook("kill"))
        self.assertFalse(target.can_shape("control_store"))
        with self.assertRaises(TargetError):
            target.node("n9")

    def test_errors(self):
        bad = [
            {"s3": BASE["s3"]},
            {**BASE, "nodes": []},
            {**BASE, "nodes": BASE["nodes"] * 2},
            {**BASE, "s3": {"access_key": "a"}},
            {**BASE, "hooks": {"explode": ["true"]}},
            {**BASE, "hooks": {"kill": "true"}},
            {**BASE, "remote": {"url": "http://x"}},
        ]
        for doc in bad:
            with self.assertRaises(TargetError, msg=doc):
                Target.parse(doc)

    def test_load_from_a_file(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            f.write('{"nodes": [{"name": "x", "s3": "http://h:1", "admin": "h:2"}], '
                    '"s3": {"access_key": "a", "secret_key": "s", "region": "eu"}, '
                    '"read_buckets": {"1": "cc1"}, "remote": {"url": "http://m", '
                    '"access_key": "k", "secret_key": "t"}}')
        try:
            target = Target.load(f.name)
        finally:
            os.unlink(f.name)
        self.assertEqual(target.credentials.region, "eu")
        self.assertEqual(target.read_buckets, {"1": "cc1"})
        self.assertEqual(target.remote_credentials().access_key, "k")

    def test_shard_hash_matches_the_frozen_vectors(self):
        # crates/skys3-types/tests/golden.rs, for 1, 7, 8, and 256 shards.
        vectors = {("b-7f3a", "photos/cat.jpg"): [0, 1, 7, 87],
                   ("b-7f3a", "photos/dog.jpg"): [0, 5, 2, 186],
                   ("b-0001", "photos/cat.jpg"): [0, 4, 7, 31]}
        for (bucket, key), shards in vectors.items():
            self.assertEqual([shard_of(bucket, key, n) for n in (1, 7, 8, 256)], shards)


class Hooks(unittest.TestCase):
    def test_hooks_get_their_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            log = os.path.join(tmp, "log")
            hook = [sys.executable, "-c",
                    "import sys; open(sys.argv[1], 'a').write(' '.join(sys.argv[2:]) + '\\n')",
                    log]
            target = Target.parse({**BASE, "links": ["target"], "hooks": {
                "kill": hook + ["kill", "{node}"],
                "shape": hook + ["shape", "{link}", "{rtt_ms}", "{delay_ms}", "{loss_pct}"],
                "unshape": hook + ["unshape", "{link}"]}})
            target.kill("n1")
            self.assertEqual(target.live(), [])
            self.assertTrue(target.can_shape("target"))
            target.shape("target", 100, 0.5)
            target.unshape("target")
            with open(log) as f:
                self.assertEqual(f.read().splitlines(),
                                 ["kill n1", "shape target 100 50 0.5", "unshape target"])
            with self.assertRaises(TargetError):
                target.run_hook("start", node="n1")


class Admin(unittest.TestCase):
    def test_shards_take_the_newest_configuration(self):
        with FakeNode() as one, FakeNode() as two:
            target = Target.parse({"nodes": [
                {"name": "n1", "s3": one.url, "admin": one.address},
                {"name": "n2", "s3": two.url, "admin": two.address}],
                "s3": BASE["s3"], "hooks": {"start": [sys.executable, "-c", "pass"]}})
            entry = {"bucket": "b", "bucket_id": "b-1", "source": "replica", "shard": 0,
                     "primary": "n1", "members": ["n1", "n2"], "learners": []}
            one.admin["/v1/shards"] = {"shards": [{**entry, "epoch": 1}]}
            two.admin["/v1/shards"] = {"shards": [
                {**entry, "epoch": 2, "primary": "n2", "members": ["n2"]},
                {**entry, "bucket": "other", "epoch": 9}]}
            self.assertEqual(target.shards("b")[0]["primary"], "n2")
            self.assertEqual(target.whole("b", 1), None)
            self.assertIn("members ['n2']", target.whole("b", 2))
            self.assertIn("no live node", target.whole("nothing", 2))
            two.admin["/v1/shards"] = {"shards": [{**entry, "epoch": 3}]}
            target.down.add("n2")
            self.assertIn("is down", target.whole("b", 2) or "")
            self.assertIsNone(target.admin("n1", "/v1/missing"))
            one.admin["/v1/health"] = {"ready": True}
            target.down.add("n1")
            target.start("n1")
            self.assertNotIn("n1", target.down)
            with self.assertRaises(TargetError):
                target.start("n2", ready_timeout=0.5)


if __name__ == "__main__":
    unittest.main()
