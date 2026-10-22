import time
import unittest

from fakes import FakeNode

from skys3perf import load
from skys3perf.load import Generator, Outcome, Ops, Task
from skys3perf.relay import Relay
from skys3perf.s3 import Client, Credentials, Payload

CREDENTIALS = Credentials("a", "s")


class Splitting(unittest.TestCase):
    def test_threads_spread_evenly_over_processes(self):
        self.assertEqual(load.split(1, 4), [1])
        self.assertEqual(load.split(4, 4), [1, 1, 1, 1])
        self.assertEqual(load.split(10, 4), [3, 3, 2, 2])
        self.assertEqual(load.split(64, 3), [22, 21, 21])
        for bad in ((0, 4), (4, 0)):
            with self.assertRaises(ValueError):
                load.split(*bad)

    def test_processes_share_the_byte_budget(self):
        task = Task(op="put", endpoints=(), credentials=CREDENTIALS, bucket="b", size=10, max_bytes=1000)
        tasks = load.tasks_for(task, 10, 4)
        self.assertEqual([t.threads for t in tasks], [3, 3, 2, 2])
        self.assertEqual([t.max_bytes for t in tasks], [300, 300, 200, 200])
        self.assertEqual([t.index for t in tasks], [0, 1, 2, 3])
        # A tiny budget stays a budget in every process, and none stays none.
        self.assertEqual([t.max_bytes for t in load.tasks_for(
            Task(op="put", endpoints=(), credentials=CREDENTIALS, bucket="b", max_bytes=2), 8, 4)], [1] * 4)
        self.assertEqual([t.max_bytes for t in load.tasks_for(
            Task(op="put", endpoints=(), credentials=CREDENTIALS, bucket="b"), 8, 4)], [0] * 4)


class Running(unittest.TestCase):
    def test_a_task_in_this_process(self):
        with FakeNode() as node:
            ops = load.run_task(Task(op="put", endpoints=(node.url,), credentials=CREDENTIALS,
                                     bucket="b", prefix="p/", size=100, threads=3, count=4))
            self.assertEqual(len(ops.statuses), 12)
            self.assertEqual(node.puts, 12)
            self.assertEqual(len(set(ops.keys)), 12)
            ops = load.run_task(Task(op="get", endpoints=(node.url,), credentials=CREDENTIALS,
                                     bucket="b", keys=("k",), threads=2, duration=0.2))
            self.assertGreater(len(ops.statuses), 2)
            self.assertEqual(set(ops.sizes), {5})
            self.assertEqual(ops.keys, [])

    def test_the_byte_budget_stops_writers(self):
        with FakeNode() as node:
            ops = load.run_task(Task(op="put", endpoints=(node.url,), credentials=CREDENTIALS,
                                     bucket="b", size=100, threads=2, duration=10,
                                     max_bytes=1000))
        self.assertEqual(len(ops.statuses), 10)

    def test_the_pool_runs_a_case_over_processes(self):
        with FakeNode() as node, Generator(2) as generator:
            outcome = generator.run(Task(op="put", endpoints=(node.url,), credentials=CREDENTIALS,
                                         bucket="b", prefix="x/", size=10, count=5), 3)
            self.assertEqual(node.puts, 15)
        self.assertEqual(outcome.processes, 2)
        self.assertEqual(outcome.generator["threads"], 3)
        distributions, values = outcome.summary(samples_max=100)
        self.assertEqual(distributions["latency"]["count"], 15)
        self.assertEqual((values["succeeded"], values["failed"], values["bytes"]), (15, 0, 150))
        self.assertGreater(values["throughput_ops"], 0)

    def test_failures_by_status(self):
        ops = Ops()
        for status, start in ((200, 1.0), (503, 1.5), (0, 2.0), (503, 2.5)):
            ops.starts.append(start)
            ops.ends.append(start + 0.5)
            ops.statuses.append(status)
            ops.sizes.append(10 if status == 200 else 0)
        outcome = Outcome(ops=ops, processes=1, generator={})
        _, values = outcome.summary(samples_max=10)
        self.assertEqual(values["failures"], {"503": 2, "0": 1})
        self.assertEqual(values["wall_seconds"], 2.0)
        self.assertEqual(values["throughput_ops"], 0.5)
        self.assertEqual(outcome.latencies(), [0.5])


class Relaying(unittest.TestCase):
    def test_the_relay_adds_its_delay_each_way(self):
        with FakeNode() as node:
            direct = Client(node.url, CREDENTIALS)
            started = time.monotonic()
            self.assertEqual(direct.put("b", "k", Payload(10)), 200)
            baseline = time.monotonic() - started
            with Relay(("127.0.0.1", int(node.address.split(":")[1])), 0.1) as relay:
                client = Client(f"http://{relay.address}", CREDENTIALS)
                started = time.monotonic()
                self.assertEqual(client.put("b", "k", Payload(3 << 20)), 200)
                self.assertEqual(client.get("b", "k"), (200, 5))
                took = time.monotonic() - started
                client.close()
            self.assertEqual(node.put_bytes, 10 + (3 << 20))
            self.assertEqual(relay.connections, 1)
        # Two requests on one connection: two round trips of 0.2 s.
        self.assertGreater(took, 0.4)
        self.assertLess(baseline, 0.4)

    def test_an_unreachable_upstream_closes_the_connection(self):
        with Relay(("127.0.0.1", 9), 0.0) as relay:
            client = Client(f"http://{relay.address}", CREDENTIALS, timeout=5)
            self.assertEqual(client.get("b", "k"), (0, 0))


if __name__ == "__main__":
    unittest.main()
