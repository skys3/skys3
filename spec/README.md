# Shard protocol model

`ShardProtocol.tla` is a TLA+ specification of the SkyS3 shard replication
protocol: design sections 5 and 6.3 to 6.8 in
[`docs/skys3-design.md`](../docs/skys3-design.md). TLC, the TLA+ model
checker, explores every behavior of a small shard within the bounds below
and checks the three properties of section 6.8:

| Invariant | Property |
|---|---|
| `CommittedRecordsSurvive` | Every committed record is at its `seq` in the log of every member of the current configuration, so of every node that can become primary. |
| `CommittedRecordsAgree` | No two primaries commit different records at the same `seq`. |
| `OneCommitterPerEpoch` | At most one node commits records in any epoch. |
| `ReadsLinearizable` | No read returns state older than a write already acknowledged to a client, or than another read already returned. |

`TypeOK` checks the shape of the state.

## Running the checks

Needs a Java 11 or later runtime and `curl`. The first run downloads the
pinned `tla2tools.jar` release (1.7.4) into `spec/.tools/` and verifies its
SHA-256; set `TLA2TOOLS_JAR` to use another copy.

```sh
spec/check.sh model pr        # the protocol at the PR bounds; must pass
spec/check.sh bugs            # every seeded bug must be caught
spec/check.sh all pr          # both: what CI runs on changes under spec/
spec/check.sh all nightly     # the nightly bounds, then the seeded bugs
spec/check.sh bug serve_before_grace          # one seeded bug
spec/check.sh run MaxEpoch=4 MaxRestarts=2    # any bounds, for exploring
```

A failing check prints TLC's counterexample: the sequence of states, each
labeled with the action that produced it.

`TLC_WORKERS` sets the worker threads (default: one per core) and
`TLC_JAVA_OPTS` passes JVM options such as `-Xmx8g`. TLC keeps its state
queue on disk in a temporary directory; large bounds need a few gigabytes.

CI runs `spec/check.sh all pr` in the `Protocol model check` job of
`.github/workflows/ci.yml` when anything under `spec/` changes, and
`spec/check.sh all nightly` in `.github/workflows/nightly.yml` every night.

## Constants and bounds

`check.sh` generates the TLC configuration. Nodes are `n1` to `nN`; `n1` is
the initial primary, the first `Members` nodes form the initial
configuration, and the rest are spares that can join as learners. A node
removed from the shard can also come back as a learner with its old log.

| Constant | Meaning | Base value |
|---|---|---|
| `Nodes` | Nodes that can hold the shard | 3 |
| `Members` | Members of the initial configuration (epoch 1) | 2 |
| `MaxEpoch` | Bound: the register's epoch never exceeds it, so a behavior has at most `MaxEpoch - 1` configuration changes (takeovers, removals, learner additions, promotions) | 3 |
| `MaxWrites` | Bound: client writes, so also the longest log | 1 |
| `MaxRestarts` | Bound: node restarts in a behavior | 0 |
| `MinWriteReplicas` | `min_write_replicas` | 1 |
| `Lease` | `primary_lease`, in local clock units | 2 |
| `Grace` | `primary_grace`, in local clock units | 4 |
| `Rates` | Local clock units a node's clock may advance in one real tick | `1,2` |
| `Bug` | `none`, or a seeded bug | `none` |

Each profile is a list of models, each the base values with some
overrides, and every model must pass every invariant unless the table says
otherwise. Small models
with different shapes find more per second than one large model: two-node
models reach four epochs and a restart cheaply, and three-node models cover
a third member and a spare.

| Profile | Model (overrides of the base values) | Distinct states | Time on 4 cores |
|---|---|---:|---:|
| `pr` | `Members=3` | 1,214,333 | 1 min 48 s |
| `pr` | (none) | 372,452 | 37 s |
| `pr` | `Nodes=2 Members=2 MaxEpoch=4 MaxRestarts=1` | 617,749 | 26 s |
| `pr` | `Nodes=2 Members=1 MaxEpoch=4 MaxWrites=2 MaxRestarts=1` | 433,805 | 14 s |
| `nightly` | `Members=3 MaxRestarts=1` | 9,678,448 | 22 min |
| `nightly` | `MaxEpoch=4` | 6,159,012 | 9 min |
| `nightly` | `MaxRestarts=1` | 3,688,860 | 4 min |
| `nightly` | `Nodes=2 Members=2 MaxEpoch=5 MaxWrites=2 MaxRestarts=1` | 18,389,112 | 15 min |
| `nightly` | `MaxRestarts=1 Rates=1,4`, durability invariants only | 3,739,924 | 8 min |

The seeded bugs add about two minutes, so `all pr` takes about five
minutes and `all nightly` about an hour. Larger three-node models grow
fast: `Members=3 MaxWrites=2 MaxRestarts=1` had passed 29.5 million states
with no violation, and was still growing, when it was stopped after
45 minutes.

`MinWriteReplicas = 1` lets a shard that has shrunk to one member keep
writing, which reaches more states than the default of 2; the setting only
gates writes, so it cannot hide a safety violation.

## How time is modeled

Leases are the only part of the protocol that depends on time, and they
only matter for read linearizability (design section 5.4). The model uses
discrete real-time ticks with bounded clock-rate drift:

- The `Tick` action advances real time by one tick. In each tick, every
  node's monotonic clock advances by an amount chosen from `Rates`,
  independently per node and per tick. With `Rates = {lo..hi}`, every clock
  runs between `lo` and `hi` units per tick: the drift bound `ρ` of design
  section 2.3 around the nominal rate `(lo + hi) / 2`, with
  `(1 + ρ) / (1 − ρ) = hi / lo`. All other actions take no time.
- Timers are kept as the time remaining, in the owning node's local units:
  the primary's lease from each member, and each member's `primary_grace`
  since it last granted a lease. Remaining time saturates at zero, so the
  state space stays finite without an absolute clock.
- A lease round (beacon, acknowledgement, grant) is one action. In the
  protocol the primary's lease counts from when it sent the beacon and the
  member's grace from when it acknowledged it, so any message delay only
  shortens the lease and postpones the grace. The atomic round is the worst
  case for reads.
- Choosing the extreme rates in every tick is the worst case for the
  section 5.4 inequality `primary_grace ≥ primary_lease × (1+ρ)/(1−ρ)`,
  which with integer rates reads `Grace × lo ≥ Lease × hi`. The defaults,
  `Rates = {1, 2}` (so `ρ = 1/3`, far above the 1% the design assumes),
  `Lease = 2`, and `Grace = 4`, meet it with equality.
- The 500 ms margin covers the time between a lease check and the index
  read it admits. A read in the model is atomic with its lease check, so
  the model needs no margin; the implementation does.

Three checks show that the clocks matter. The `grace_without_drift` run
sets `Grace = Lease`, as if the inequality had no drift allowance, and the
`drift_beyond_bound` run widens `Rates` to `{1, 4}` with the base lease and
grace; TLC must find a stale read in both. A nightly model checks the
durability invariants with `Rates = {1, 4}` and must pass: the design's
claim that clocks affect read linearizability only (section 13).

Integer ticks resolve the inequality coarsely: timers expire only at tick
boundaries, so a grace slightly short of `Lease × hi / lo` can still pass
(`Rates = {1, 3}` does, with the base lease and grace). The model shows the
inequality is sufficient and that a missing drift allowance is caught; it
cannot measure a small shortfall.

## Other abstractions

- **One shard, one key.** A read returns the state at the primary's commit
  watermark, and is stale if any acknowledged write is newer. Treating every
  write as a write to the same key is the worst case for per-key
  linearizability.
- **Messages.** Sending, delivering, and acting on a message is one action,
  taken at any time or never, so loss, delay, and reordering are covered by
  the interleavings. A deposed primary keeps sending until it learns of the
  new epoch, which covers stragglers from old epochs. Configurations reach a
  node from the register or from any other node, in any order.
- **Control store.** A compare-and-swap is atomic. A proposer learns the
  outcome only when it later adopts a newer configuration, so a lost
  response is a delayed adoption; the lost-response rule of section 6.1 is
  assumed.
- **Gateways.** Any node that believes it is the serving primary may be
  asked to serve a read, which is what a gateway with an arbitrarily stale
  shard map can do. The epoch a gateway sends can only make a node reject a
  read, so it is not modeled.
- **Restarts** keep the log, the latest `CONFIG` record, an outstanding
  proposal, and a step-down, and lose leases, the grace timer, and a
  primary's view of its members' acknowledgements. A restarted primary
  re-aligns its members before it serves again.
- **Members' commit watermarks** are not tracked: only a primary serves, and
  a new primary recommits its whole log during reconciliation.
- **Not modeled:** disk loss (losing every member loses data by design),
  `replica_ack_timeout` and other timeouts that only affect liveness,
  the flusher, erasure coding, and the coordinator's own lease. TLC checks
  safety only; no liveness property is checked.

## Seeded bugs

Each seeded bug removes one guard the design relies on. `check.sh bugs`
runs every one at the base bounds (3 nodes, 2 initial members,
`MaxEpoch = 3`, `MaxWrites = 1`, no restarts) plus the overrides listed,
checks only the invariant named, and fails unless TLC finds a violation of
it. It also fails if its list and `BugNames` in the specification disagree.
The whole set runs in about two minutes on four cores.

| Bug | Guard removed (design section) | Must violate | Overrides |
|---|---|---|---|
| `promote_before_watermark` | R3: promote a learner before it has backfilled and is durable up to the commit watermark (6.3, 6.7) | `CommittedRecordsSurvive` | |
| `serve_before_grace` | Take over, and so serve, without waiting for `primary_grace` or a step-down (5.4) | `ReadsLinearizable` | |
| `candidate_keeps_granting` | R1: a candidate keeps granting leases after proposing (6.3) | `ReadsLinearizable` | |
| `accept_older_epoch` | R2: a member accepts appends from an older epoch (6.3) | `CommittedRecordsSurvive` | `Members=3` |
| `commit_on_majority` | The all-member commit rule: commit on a majority (5.1) | `CommittedRecordsSurvive` | |
| `blind_register_write` | The register CAS: write without comparing the version (6.1) | `OneCommitterPerEpoch` | |
| `truncate_by_seq_only` | Reconciliation and re-admission compare `(epoch, seq)`: compare `seq` only (6.6, 6.7) | `CommittedRecordsSurvive` | `Nodes=2 MaxEpoch=4 MaxWrites=2` |
| `drop_promoting_learner` | Keep waiting for a learner while its promotion CAS is outstanding (6.7) | `CommittedRecordsSurvive` | |
| `promotion_without_learner_lease` | Need the learner's lease while its promotion is outstanding (5.4, 6.7) | `ReadsLinearizable` | `Nodes=2 Members=1 MaxEpoch=4` |
| `restart_forgets_grace` | Count a restart as a lease grant (5.4) | `ReadsLinearizable` | `MaxRestarts=1` |
| `restart_forgets_proposal` | Keep an outstanding proposal across a restart (6.3) | `ReadsLinearizable` | `MaxRestarts=1` |
| `stepped_down_keeps_reading` | A stepped-down primary stops serving reads (5.4) | `ReadsLinearizable` | |
| `restart_forgets_step_down` | Keep a step-down across a restart (5.4) | `ReadsLinearizable` | `MaxRestarts=1` |
| `grace_without_drift` | Clock setting: `Grace = Lease`, no drift allowance (5.4) | `ReadsLinearizable` | `Grace=2` |
| `drift_beyond_bound` | Clock setting: clocks drift beyond what `Grace` allows for (13) | `ReadsLinearizable` | `Rates=1,4` |

The plan's two examples are `promote_before_watermark` (promoting a learner
before it holds the commit watermark) and `serve_before_grace` (a new
primary serving before `primary_grace` has passed).

## Design changes found by the model

Writing and checking the model changed the design in four places, recorded
in `docs/skys3-design.md`:

- **Restarts and `primary_grace` (section 5.4).** A restarted node does not
  know when it last granted a lease, so it counts the restart as a grant.
  Seeded bug `restart_forgets_grace`.
- **Durable proposals (section 6.3).** A node records a proposal before the
  CAS and, until it learns the outcome, across restarts too, acts as if the
  CAS succeeded. Seeded bugs `restart_forgets_proposal` and
  `drop_promoting_learner`.
- **Leases during promotion (sections 5.4 and 6.7).** Learners in the
  acknowledgement set grant leases, and a primary with an outstanding
  promotion needs the learner's lease to serve reads. Without it, a
  promotion that succeeded unseen lets the new member take over while the
  primary still serves reads on the old members' leases. Seeded bug
  `promotion_without_learner_lease`.
- **Reconciliation compares `(epoch, seq)` (section 6.6).** Truncating only
  past the new primary's last `seq` keeps a member's different record at a
  `seq` the new primary also holds. Seeded bug `truncate_by_seq_only`.

The step-down of a planned handoff is also durable, and counts only for the
epoch it names (section 5.4; seeded bug `restart_forgets_step_down`).

## Why TLA+

The design names TLA+ first, and a reviewer can read the specification
next to the design without knowing an implementation language. TLC checks
it exhaustively with no project code, so the model does not depend on
implementation types that are still being written (M2-07 onward). A Rust
checker such as `stateright` could share types with the implementation,
but that ties the model to the code's structure, and checking the real code
is the simulation harness's job (M2-03). The model stays outside the Cargo
workspace, so it adds no crates.
