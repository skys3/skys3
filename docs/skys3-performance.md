# SkyS3: Running the Performance Suite

**Implements:** plan M7-06a, the harness for the measurements of the [SkyS3 design](skys3-design.md), section 16.3, which plan M6-09 and M7-06 complete on fixed hardware.

The performance harness, `tests/e2e/perf/perf.py`, drives a running SkyS3
cluster through a named set of scenarios, samples every node's metrics while
they run, and writes a machine-readable result (`result.json`) and a short
summary (`summary.md`). This runbook tells how to run it on the release
hardware, how to shape links, how to read and compare results, and how M6-09
and M7-06 are completed with it.

Numbers mean something only when they come from the fixed release hardware
and are compared with runs on the same hardware. CI runs the harness on every
pull request (section 2.1) only to show that it works end to end: it checks no
threshold, and its numbers are not evidence for section 16.3.

## Contents

- [1. What it measures](#1-what-it-measures)
- [2. Running it](#2-running-it)
  - [2.1 In CI](#21-in-ci)
  - [2.2 On the release hardware](#22-on-the-release-hardware)
  - [2.3 Against a cluster started otherwise](#23-against-a-cluster-started-otherwise)
- [3. The target file](#3-the-target-file)
- [4. Profiles and parameters](#4-profiles-and-parameters)
- [5. Shaping links](#5-shaping-links)
- [6. Reading results](#6-reading-results)
- [7. Completing M6-09 and M7-06](#7-completing-m6-09-and-m7-06)
- [8. Adding a scenario](#8-adding-a-scenario)

## 1. What it measures

Each scenario covers rows of design section 16.3. A row the harness does not
measure yet is an extension point: a named scenario that reports
`not_implemented` in the result, never a number.

| Scenario | Section 16.3 row | What it reports |
|---|---|---|
| `put` | PUT p50/p99 by size, fsyncs per acknowledged PUT, records per group commit, serial durable rounds (section 5.3's table) | PUT latency by object size and concurrency; from the nodes' log counters (metrics reference section 3.13), fsyncs and group commits per acknowledged PUT, records per group commit, the mean time of a group commit, and an upper bound on the serial durable rounds; section 5.3's claims beside them (`expected`) |
| `get` | Read scaling | GET throughput and latency of one hot object, by `clean_copies` and by the number of gateways; hot-cache hits and fills |
| `failover` | Failover distribution and per-shard write unavailability, at control-store round trips of 1 ms and 100 ms; learner promotion's stall. Durability window | Kills (SIGKILL) a node that leads a shard while one writer per shard writes; per shard, by the killed node's role in it (primary or backup), the recovery time (from the kill to the end of the first acknowledged write that started after it) and the longest write stall; the time until the killed node is out of every configuration; after its restart, the time until every shard has `replicas` members and no learner, and the stall while it rejoins; `skys3_under_replicated_bytes` over time; the write latency in the cluster's `replica_ack_timeout_mode` |
| `flush` | Flush lag, dirty backlog, sustained flush throughput at target round trips from 1 ms to 150 ms; adaptive concurrency; streaming overlap | At each round trip, the write throughput, the drain time, the flush throughput, and over time the dirty bytes, flush lag, oldest dirty age, adaptive concurrency, and in-flight bytes; the measured base round trip; the streaming-overlap mean when objects are streamed |
| `import` | Import rate; fill latency (cache row) | The import rate of a remote bucket, and the latency of a first GET (a fill) and of a second (local) |
| `cache` | Cache hit rate and compaction write amplification | Extension point |
| `local` | Local buckets: stored bytes per logical byte, encoding backlog, repair time | Extension point |
| `peer` | Peer transport over shaped links (plan M6-09) | Extension point |
| `hot_bucket` | Section 19, question 5: per-bucket write throughput against `shards_per_bucket` | Extension point |
| `metadata` | Section 19, question 6: index bytes per entry | Extension point |

The load generator is Python, beside the end-to-end suite and the chaos
suite, and uses only the standard library. A case's concurrent clients are
spread over worker processes, one per CPU at most, each running its share as
threads, so the generator is not bound to one interpreter lock. Each case
records the generator's CPU time and the utilization of its busiest process
(`values.generator`), and sets `may_have_limited` when a process was busy for
85% of the case or more: the generator, not the cluster, may then have set
the pace, and the case should be run with more `processes` or from another
host. Measured in a 4-CPU container, a PUT or GET costs the generator about
0.5 to 0.65 ms of CPU, about 2,000 requests a second per core.

## 2. Running it

### 2.1 In CI

The end-to-end scenario `tests/e2e/scenarios/98-perf.sh` runs on every pull
request, in the etcd leg of the suite (`# e2e: etcd`), beside the other
cluster scenarios. It starts nodes `pf1` to `pf3` of cluster `pf` over the
suite's etcd, buckets of 4 shards of 3 replicas, writes the target file, runs
the `smoke` profile (about 40 seconds), and checks the result with
`perf.py check --require all`: it validates against the schema, every
implemented scenario ran (`ok`), each measured case has samples, and the
metric-derived values are there and above 0 (fsyncs, group commits, and
records per PUT; flushes and flush throughput; the import rate and fills).
The harness's unit tests run in the same job (`python3 -m unittest discover
-s tests` in `tests/e2e/perf`).

To run it locally: `SKYS3_E2E_CONTROL=etcd tests/e2e/run.sh perf`. The
result and summary are in `target/e2e-run/work/perf/out/`.

### 2.2 On the release hardware

The same scenario launches the cluster for the release run, on one host:

```sh
cargo build --release -p skys3
SKYS3_E2E_CONTROL=etcd SKYS3_E2E_BINARY=target/release/skys3 \
SKYS3_PERF_PROFILE=full SKYS3_E2E_SCENARIO_TIMEOUT=28800 \
SKYS3_E2E_DIR=/fast-disk/skys3-perf tests/e2e/run.sh perf
```

- Build with `--release`: the suite's own `e2e` profile is unoptimized.
- `SKYS3_E2E_DIR` puts the data directories on the disk to be measured; the
  result records that disk (`host.disk`) from the first node's data
  directory.
- `SKYS3_PERF_SET` overrides parameters, `name=value` pairs separated by
  spaces, such as `SKYS3_PERF_SET="scenarios=put,get put.duration=60s"`.
- `SKYS3_E2E_SCENARIO_TIMEOUT` (seconds) lifts the suite's 10-minute limit
  per scenario; the `full` profile takes hours, most of it the 1 GiB PUTs and
  the flush round trips.
- Run nothing else on the host, and keep its CPU frequency governor fixed.
- Give it disk: nothing the harness writes is deleted, and each `put` case
  writes up to `put.max_bytes` (at least one object per writer: 64 GiB for
  64 writers of 1 GiB), times `replicas`. The `full` profile needs about
  half a terabyte across the nodes' disks; lower `put.max_bytes` and
  `put.sizes` for less.

The result is in `$SKYS3_E2E_DIR/work/perf/out/`. To measure across hosts,
or with links shaped by netem, start the cluster otherwise and give the
harness a target file (section 2.3).

### 2.3 Against a cluster started otherwise

```sh
tests/e2e/perf/perf.py run --target target.json --profile full --out results/
tests/e2e/perf/perf.py check results/result.json --require all
```

`run` exits 1 if a scenario failed, and 2 if the target file or a parameter
is wrong. `perf.py params` lists every parameter with its defaults.

## 3. The target file

A JSON document describing the cluster. The harness never launches nodes
itself: whatever started them writes it (`98-perf.sh` writes one).

```json
{
  "name": "release-r1",
  "nodes": [{"name": "n1", "s3": "https://10.0.0.11:9000", "admin": "10.0.0.11:7490"}],
  "s3": {"access_key": "AKIA...", "secret_key": "...", "region": "us-east-1",
         "ca_file": "/etc/skys3/ca.pem"},
  "admin_token": "...",
  "remote": {"url": "http://10.0.1.5:9000", "access_key": "...", "secret_key": "..."},
  "read_buckets": {"1": "perfcc1", "3": "perfcc3"},
  "cluster": {"replicas": 3, "shards_per_bucket": 4, "control_store": "etcd",
              "replica_ack_timeout_mode": "wait_through"},
  "hooks": {
    "kill": ["ssh", "{node}", "pkill", "-KILL", "-x", "skys3"],
    "start": ["ssh", "{node}", "systemctl", "start", "skys3"],
    "shape": ["sudo", "/opt/perf/shape-link.sh", "{link}", "{delay_ms}", "{loss_pct}"],
    "unshape": ["sudo", "/opt/perf/shape-link.sh", "{link}", "0", "0"]
  },
  "links": ["control_store", "target"],
  "data_dir": "/var/lib/skys3"
}
```

| Field | Required | Meaning |
|---|---|---|
| `nodes` | yes | Each node's name, S3 endpoint (`http://` or `https://`), and admin listener (`host:port`). |
| `s3` | yes | A static credential that may do anything, and the CA bundle for an HTTPS gateway. |
| `admin_token` | no | The admin listener's bearer token, for listeners off loopback. |
| `remote` | for `get`, `flush`, `import` | The S3 store write_back buckets flush to, and its root credential: the harness creates buckets in it and seeds the import. Without it, those scenarios report `skipped`. |
| `read_buckets` | for `get` | For each `clean_copies` value, the name of a bucket the nodes configure with it (`[buckets.<name>] clean_copies = ...`); the harness creates it as a write_back bucket. |
| `cluster` | no | Facts the harness cannot read and records in the result: `replicas` (needed by `put`'s `expected` and by `failover`), `replica_ack_timeout_mode`, how the binary was built. |
| `hooks` | for `failover`, shaping | Commands, as argument lists. `{node}`, `{link}`, `{rtt_ms}`, `{delay_ms}` (half the round trip), and `{loss_pct}` are replaced. `kill` must kill the node at once (SIGKILL); `start` must start it, and the harness then waits until `/v1/health` reports ready. |
| `links` | for shaping | The links `shape` and `unshape` can shape: `control_store` (every node's link to the control store) and `target` (every node's link to `remote`). |
| `data_dir` | no | A data directory, whose disk the result records. |

## 4. Profiles and parameters

`smoke` takes seconds per case and tiny objects, and is what CI runs. `full`
is the section 16.3 matrix for the release hardware. `--set name=value`
overrides any parameter; lists are comma-separated, sizes take binary units
(`64KiB`, `1GiB`), and durations `ms`, `s`, `m`, or `h`. The table is
`perf.py params`'s output.

| Parameter | Kind | smoke | full | Meaning |
|---|---|---|---|---|
| `scenarios` | list of str | put, get, failover, flush, import | put, get, failover, flush, import, cache, local, peer, hot_bucket, metadata | The scenarios to run, in order. |
| `metrics_interval` | duration | 0.5s | 1s | How often every node's /metrics is sampled during a scenario. |
| `samples_max` | int | 2000 | 10000 | A distribution keeps its raw samples while it has at most this many. |
| `processes` | int | 0 | 0 | Load-generator processes at most; 0 is one per CPU. |
| `request_timeout` | duration | 30s | 300s | How long a request may take before it counts as unanswered. |
| `put.sizes` | list of size | 1KiB, 64KiB | 1KiB, 16KiB, 128KiB, 1MiB, 16MiB, 128MiB, 1GiB | Object sizes, one case each with every concurrency. |
| `put.concurrency` | list of int | 1, 4 | 1, 16, 64 | Concurrent PUTs, one case each. |
| `put.duration` | duration | 2s | 30s | How long each case sends PUTs. |
| `put.max_bytes` | size | 64MiB | 16GiB | The most bytes a case sends, split among its writers; each sends one object at least. |
| `put.bucket` | str | perf-put | perf-put | The bucket, of the cluster's default mode. |
| `get.size` | size | 64KiB | 1MiB | The size of the hot object. |
| `get.clean_copies` | list of int | 1 | 1, 2, 3 | clean_copies values, each through the bucket the target names for it. |
| `get.gateways` | list of int | 1, 3 | 1, 2, 3, 5, 8 | Gateway counts; counts above the live nodes are skipped. |
| `get.concurrency` | int | 4 | 16 | Concurrent GETs per gateway. |
| `get.duration` | duration | 2s | 30s | How long each case sends GETs. |
| `failover.control_rtts_ms` | list of float | 0 | 1, 100 | Control-store round trips, shaped with the target's control_store link hooks; 0 leaves the link as it is. |
| `failover.kills` | int | 1 | 5 | Kills of a shard primary's node per round trip. |
| `failover.object_size` | size | 1KiB | 4KiB | The size of the writers' objects. |
| `failover.settle` | duration | 2s | 5s | How long writers go on once every shard took a write after the kill. |
| `failover.timeout` | duration | 60s | 180s | How long every shard may take to take a write after the kill. |
| `failover.restart` | bool | true | true | Restart the killed node, and measure until every shard is whole again. |
| `failover.heal_timeout` | duration | 120s | 600s | How long the shards may take to have `replicas` members and no learner. |
| `failover.bucket` | str | perf-failover | perf-failover | The bucket. |
| `flush.rtts_ms` | list of float | 0, 20 | 1, 10, 50, 100, 150 | Round trips to the remote target; 0 leaves the link as it is. |
| `flush.objects` | int | 100 | 5000 | Objects written per round trip. |
| `flush.size` | size | 16KiB | 1MiB | Their size. |
| `flush.concurrency` | int | 4 | 32 | Concurrent PUTs writing them. |
| `flush.shaper` | str | auto | auto | How a round trip is made: `hook` (the target's `target` link hooks, such as tc netem), `relay` (a delaying TCP relay in the harness), or `auto` (hook if the target has one, else relay). |
| `flush.timeout` | duration | 120s | 3600s | How long the bucket may take to flush. |
| `import.objects` | int | 100 | 20000 | Objects in the remote bucket imported. |
| `import.size` | size | 1KiB | 4KiB | Their size. |
| `import.reads` | int | 50 | 1000 | Objects read twice after the import: filled, then local. |
| `import.timeout` | duration | 120s | 3600s | How long the import may take. |

Counts are per client and rounded up: `flush.objects` and `import.objects`
are split evenly over the concurrent clients, so a run may write a few more.

## 5. Shaping links

Section 16.3 measures failover at control-store round trips of 1 and 100 ms,
and flushing at target round trips of 1 to 150 ms. The harness shapes a link
only through the target file's `shape` and `unshape` hooks, so the tool is
the operator's:

- **Network namespaces on one host.** The chaos suite's
  `tests/e2e/chaos/net.sh` runs each process in a namespace of its own and
  shapes what a namespace sends with `tc netem` (`net.sh shape <name>
  <delay ms> <jitter ms> <loss %>`, `net.sh unshape <name>`). A hook script
  (`shape-link.sh` above, the operator's) maps each link to namespaces,
  `control_store` to the etcd members' and `target` to the remote store's,
  and runs `net.sh shape` on each. netem delays what a namespace sends, one
  direction, so shaping both ends of a link with `{delay_ms}` gives the
  round trip.
- **Several hosts.** `tc qdisc add dev <nic> root netem delay <ms>` on the
  hosts of the control store or the remote store, through `ssh`.
- **Without netem** (containers, loopback, CI): the `flush` scenario puts a
  delaying TCP relay between the nodes and the remote store
  (`flush.shaper=relay`, the fallback of `auto`): every byte waits half the
  round trip each way. It limits no bandwidth and loses nothing, unlike
  netem, and each flush case records its shaper (`labels.shaper`), so relay
  and netem cases are never compared by mistake. Failover at a shaped
  control-store round trip needs the hooks; without them its shaped cases
  report `skipped`.

Check a shaped round trip before trusting it: the `flush` cases report the
flushers' own measure, `base_round_trip_seconds`, which should be the shaped
round trip plus the store's service time.

## 6. Reading results

`result.json` follows `tests/e2e/perf/result.schema.json`, version 1; any
change to its shape raises `version`, and `check` and `compare` refuse a
result of another version. Its fields:

- `run_id`, `started_at`, `finished_at`; `git` (`sha`, and `dirty` when the
  tree had local changes; `SKYS3_PERF_GIT_SHA` names the commit when the
  harness runs outside the checkout); `profile`; `parameters`, every
  parameter as resolved.
- `host`: CPU count, kernel, machine, Python, memory, and the disk of
  `data_dir` (device, whether it is rotational, model) from sysfs, or `null`.
  A virtual disk may report itself rotational.
- `cluster`: the target's name and nodes, the binary's version from
  `skys3_build_info`, and the target file's `cluster` facts.
- `scenarios`: each with its `rows` of section 16.3, `status` (`ok`,
  `failed`, `skipped`, `not_implemented`), `reason`, duration, and `cases`.
  A case has `labels` (its point in the matrix), `distributions`, `values`,
  and `series` (`[unix time, value]` pairs of gauges summed over the nodes,
  or their maximum for ages and lags). A case the target could not run has a
  `skipped` value and nothing else.
- A distribution has its unit, count, minimum, maximum, mean, the
  percentiles p50, p90, p99, and p99.9 (interpolated between the closest
  ranks), a histogram on a 1-2-5 scale (`bounds` are upper bounds, `counts`
  the samples above the previous bound), and the raw samples while there are
  at most `samples_max`.

How to read the `put` values against section 5.3's table:

- `fsyncs_per_put` counts every file sync of every node per acknowledged PUT.
  For a small PUT without batching, it should equal the sync participants,
  `expected.sync_participants` (`replicas`). Group commit lowers it under
  concurrency, which `records_per_group_commit` shows. Objects above
  `inline_max_bytes` add extent records, and a group that creates a segment
  adds a directory sync, so large objects count more.
- `serial_durable_rounds_bound` is the PUT's median latency over the mean
  group commit time: an upper bound on the serial durable rounds, close to
  section 5.3's 1 only when syncs dominate the latency, as on a disk with
  real flushes. On a fast disk or an unoptimized build it means nothing.
- Control-store calls on the write path are not counted: the claim of 0 is
  not measured by this harness.

For `failover`, one kill measures both failure kinds: the killed node led
some shards (`recovery_primary`, `stall_primary`) and was a backup of others
(`recovery_backup`, `stall_backup`). With several kills per round trip, a
case labelled `kill=all` merges their samples into the distribution section
16.3 asks for. A run measures the `replica_ack_timeout_mode` the cluster
runs; the other mode needs a second run with the nodes reconfigured.

`summary.md` prints each case on one line. `perf.py compare base.json
new.json` prints, for every case the two results share, the p50 and p99 of
each distribution and the main values, with the change in percent.

## 7. Completing M6-09 and M7-06

M7-06's "Done when" is that the section 16.3 targets are met, or the design
is revised with the measurements attached. With this harness, that is:

1. Fix the release hardware and record it in the PR: CPU, memory, disks, NIC,
   kernel. Use the same hardware for every later comparison.
2. Implement the extension points the run needs: `cache`, `local`, and
   `hot_bucket` and `metadata` for section 19's questions 5 and 6. Each is a
   scenario of its own (section 8).
3. Run the `full` profile on a release build (section 2.2), once with
   `replica_ack_timeout_mode = "wait_through"` and once with `"fail_fast"`,
   with the control-store and target links shaped by netem (section 5).
4. Attach `result.json` and `summary.md` of each run to the PR, set each
   section 16.3 row against the claim it checks (section 5.3's table, the
   failover bound of section 6.4, adaptive concurrency reaching the
   bandwidth-delay product of section 7.7), and revise the design where a
   claim does not hold.

M6-09 is the `peer` scenario: two clusters over links shaped to 150 to
300 ms and 0 to 2% loss, QUIC with each congestion controller and connection
count against S3 REST, link-flap resume, and the UDP-blocked fallback. It
needs a second cluster in the target file and the peering settings to vary,
then the same steps; its report decides the peer default (section 19, item
9).

## 8. Adding a scenario

A scenario is a function in `tests/e2e/perf/skys3perf/scenarios.py` that takes
the run's `Context` and returns its cases, registered in `SCENARIOS` with the
section 16.3 rows it covers. Replacing an extension point's `None` with the
function makes it run.

- Every size, count, duration, and round trip it uses is a parameter in
  `params.py`, with a `smoke` and a `full` default and a description.
- It reads node counters as differences of snapshots
  (`metrics.counter_delta`, which counts a restarted node from 0) and gauges
  through a `Sampler`.
- It raises `Skip` when the target lacks what it needs, and reports a case
  it cannot measure with a `skipped` value; it never invents a number.
- The values `perf.py check` requires of it go in `REQUIRED_POSITIVE` or
  `REQUIRED_PRESENT` (`runner.py`), and its logic gets unit tests under
  `tests/e2e/perf/tests`. A metric it needs that no node exports is added to
  the node and to the metrics reference first.
