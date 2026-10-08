//! Index snapshots and the restore drill of a lost shard (§6.9, §8.9,
//! plan M4-11).
//!
//! With [`ClusterConfig::snapshots`](crate::ClusterConfig::snapshots),
//! every node writes index snapshots of the shards it leads to the remote
//! store: a `write_back` bucket's under `snapshots/<bucket>/`, a `local`
//! bucket's to its backup target if it has one (the default), and
//! otherwise under `snapshots/<bucket>/` too. At random moments of the
//! workload the driver assumes every member of every shard lost: it forks
//! the remote store as it is then ([`Loss`]), which keeps the snapshots
//! and the durable homes, the remote targets and backups, and notes the
//! history's moment. After the run, a restore drill builds each shard's
//! lost-key report from the latest snapshot in each fork, and [`audit`]
//! checks it against the ground truth of the clients' history:
//!
//! - **The window.** A key with a write that may have taken effect after
//!   the snapshot was taken (one acknowledged after the moment the
//!   report's window starts, or never answered for sure, and called before
//!   the loss) may be lost without being reported. The window's start, on
//!   the nodes' clock, maps to the history's moments through the moments
//!   the driver noted at each step, with two steps of margin.
//! - **Every other key** has its final value from its acknowledged writes
//!   (a key whose last writes overlap, with more than one possible final
//!   value, is left out). It is lost if its durable home does not hold
//!   that value, or, in a `local` bucket without a backup, if it holds an
//!   object at all. The report must name exactly the keys that are lost,
//!   each with its final value: an ETag, or a delete.
//!
//! Seeded bugs ([`SnapshotBug`]) show that the drill catches a snapshot
//! that leaves out dirty entries, deltas that forget removed rows, and a
//! restore that dates an older state as the latest.

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::time::Duration;

use skys3_flush::snapshot::{self, DurableHome, LostKeyReport};
use skys3_flush::test_hooks::SnapshotBug;
use skys3_gateway::ShardRef as KeyShard;
use skys3_log::ShardRef;
use skys3_sim::SimS3;
use skys3_sim::check::Violation;
use skys3_sim::history::{History, Operation, Outcome};
use skys3_types::{BucketDocument, BucketMode, ClusterId};

use crate::backup::{Backup, backup_prefix};
use crate::cluster::{remote_prefix, written};
use crate::workload::Routes;

/// Index snapshots on every node, and the drill that checks their reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Snapshots {
    /// `index_snapshot_interval_seconds` of every bucket.
    pub interval_seconds: u64,
    /// How many moments of the workload the drill assumes every member
    /// lost at.
    pub losses: usize,
    /// The earliest of those moments, after the clients start.
    pub earliest: Duration,
    /// The latest of those moments, after the clients start.
    pub latest: Duration,
    /// A seeded bug the nodes and the drill run.
    pub bug: SnapshotBug,
}

impl Default for Snapshots {
    /// A snapshot every second, and three losses between the 2nd and
    /// the 8th second of the workload.
    fn default() -> Self {
        Self {
            interval_seconds: 1,
            losses: 3,
            earliest: Duration::from_secs(2),
            latest: Duration::from_secs(8),
            bug: SnapshotBug::None,
        }
    }
}

/// The prefix of `bucket`'s snapshot target in the remote store.
fn snapshot_prefix(bucket: &BucketDocument, backup: Backup) -> String {
    match backup_prefix(bucket) {
        Some(prefix) if backup.enabled() => prefix,
        _ => format!("snapshots/{}/", bucket.name),
    }
}

/// The prefix of `bucket`'s durable home in the remote store, if it has
/// one besides its members.
fn home_prefix(bucket: &BucketDocument, backup: Backup) -> Option<String> {
    match bucket.mode {
        BucketMode::WriteBack => Some(remote_prefix(bucket).to_owned()),
        _ if backup.enabled() => backup_prefix(bucket),
        _ => None,
    }
}

impl Snapshots {
    /// The configuration lines that give the buckets named `local` (the
    /// `local` ones) and `write_back` their snapshot targets, beside the
    /// backup tables, which already make a backup target the default: a
    /// line for `[buckets.defaults]`, and the tables.
    pub(crate) fn tables(
        &self,
        local: &[String],
        write_back: &[String],
        backup: Backup,
    ) -> (String, String) {
        let defaults = format!(
            "index_snapshot_interval_seconds = {}\n",
            self.interval_seconds
        );
        let mut tables = String::new();
        let mut table = |name: &str, mode: &str| {
            let _ = write!(
                tables,
                "[buckets.{name}]\nmode = \"{mode}\"\n\
                 snapshot_target = \"https://remote.sim.internal/remote/snapshots/{name}/\"\n"
            );
        };
        if !backup.enabled() {
            local.iter().for_each(|name| table(name, "local"));
        }
        write_back.iter().for_each(|name| table(name, "write_back"));
        (defaults, tables)
    }
}

/// The remote store as it was when the drill assumed every member lost.
#[derive(Debug)]
pub(crate) struct Loss {
    /// When, on the nodes' clock, in milliseconds since the Unix epoch.
    at_ms: u64,
    /// The history's moment then.
    moment: u64,
    /// The remote store as it was.
    store: SimS3,
}

/// What the drill notes during a run: the losses still to come and those
/// taken, and the history's moment at each step.
#[derive(Debug)]
pub(crate) struct Drill {
    /// When the losses come, after the clients start, in order.
    due: VecDeque<Duration>,
    /// Each step's time on the nodes' clock, in milliseconds since the
    /// Unix epoch, and the history's moment then.
    trace: Vec<(u64, u64)>,
    losses: Vec<Loss>,
}

impl Drill {
    /// A drill whose losses come at `due`, after the clients start.
    pub(crate) fn new(mut due: Vec<Duration>) -> Self {
        due.sort();
        Self {
            due: due.into(),
            trace: Vec::new(),
            losses: Vec::new(),
        }
    }

    /// Notes a step at `since_epoch` on the nodes' clock, `elapsed` since
    /// the clients started at `origin`, and takes the losses due.
    pub(crate) fn step(
        &mut self,
        since_epoch: Duration,
        since_origin: Option<Duration>,
        history: &History,
        remote: &SimS3,
    ) {
        let at_ms = millis(since_epoch);
        let moment = history.now();
        if self.trace.last().is_none_or(|(ms, _)| *ms < at_ms) {
            self.trace.push((at_ms, moment));
        }
        while since_origin.is_some_and(|elapsed| self.due.front().is_some_and(|at| *at <= elapsed))
        {
            self.due.pop_front();
            self.take(at_ms, moment, remote);
        }
    }

    /// Takes every loss still due, now: the clients are done.
    pub(crate) fn finish(&mut self, since_epoch: Duration, history: &History, remote: &SimS3) {
        while self.due.pop_front().is_some() {
            self.take(millis(since_epoch), history.now(), remote);
        }
    }

    fn take(&mut self, at_ms: u64, moment: u64, remote: &SimS3) {
        self.losses.push(Loss {
            at_ms,
            moment,
            store: remote.fork(),
        });
    }

    /// The history's moment before every event at or after `ms` on the
    /// nodes' clock: the moment noted two steps before the step of `ms`.
    fn moment_before(&self, ms: u64) -> u64 {
        let before = self.trace.partition_point(|(at, _)| *at + 2 <= ms);
        before.checked_sub(1).map_or(0, |index| self.trace[index].1)
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// What the drill found.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SnapshotAudit {
    /// The losses drilled.
    pub losses: usize,
    /// Reports built, one per shard per loss.
    pub reports: usize,
    /// Reports built from a snapshot.
    pub from_snapshots: usize,
    /// Reports built from a snapshot with deltas applied.
    pub from_deltas: usize,
    /// Keys reported lost, over every report.
    pub lost: usize,
    /// Keys checked against the history, over every report.
    pub checked: usize,
    /// Keys left to the window: written, or perhaps written, after the
    /// snapshot.
    pub in_window: usize,
}

/// Builds every shard's lost-key report from each loss's fork of the
/// remote store, and checks it against the history.
///
/// # Errors
///
/// The first key reported that is not lost, or lost and neither reported
/// nor in the window, or reported with another value than its last.
pub(crate) fn audit(
    drill: &Drill,
    routes: &Routes,
    keys: usize,
    backup: Backup,
    cluster: &ClusterId,
    operations: &[Operation],
) -> Result<SnapshotAudit, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .map_err(|error| error.to_string())?;
    let mut audit = SnapshotAudit::default();
    for loss in &drill.losses {
        audit.losses += 1;
        for bucket in &routes.buckets {
            for shard in bucket.shards.shards() {
                let shard = ShardRef::new(bucket.bucket_id.clone(), shard);
                let report = runtime
                    .block_on(report(loss, bucket, &shard, backup, cluster))
                    .map_err(|error| format!("the drill of {shard} failed: {error}"))?;
                audit.reports += 1;
                audit.from_snapshots += usize::from(report.snapshot.is_some());
                audit.from_deltas +=
                    usize::from(report.snapshot.is_some_and(|info| info.number > 0));
                audit.lost += report.lost.len();
                let start = report
                    .window
                    .from_ms
                    .map_or(0, |from| drill.moment_before(from));
                let check = Check {
                    loss,
                    bucket,
                    backup,
                    start,
                    operations,
                };
                for n in 0..keys {
                    let key = format!("key-{n}");
                    if KeyShard::for_key(bucket, &key).shard != shard.shard {
                        continue;
                    }
                    check
                        .key(&key, &report, &mut audit)
                        .map_err(|violation| violation.to_string())?;
                }
            }
        }
    }
    Ok(audit)
}

/// The lost-key report of `shard` from the loss's fork.
async fn report(
    loss: &Loss,
    bucket: &BucketDocument,
    shard: &ShardRef,
    backup: Backup,
    cluster: &ClusterId,
) -> Result<LostKeyReport, snapshot::SnapshotError> {
    let prefix = snapshot_prefix(bucket, backup);
    let restored = snapshot::latest(&loss.store, &prefix, shard).await?;
    let home = home_prefix(bucket, backup);
    let home = home.as_deref().map(|prefix| DurableHome {
        store: &loss.store,
        prefix,
        cluster,
    });
    snapshot::lost_keys(shard, restored.as_ref(), home, loss.at_ms).await
}

/// One shard's report against the history.
struct Check<'a> {
    loss: &'a Loss,
    bucket: &'a BucketDocument,
    backup: Backup,
    /// The history's moment before the window starts.
    start: u64,
    operations: &'a [Operation],
}

impl Check<'_> {
    /// Checks the report on `key`.
    fn key(
        &self,
        key: &str,
        report: &LostKeyReport,
        audit: &mut SnapshotAudit,
    ) -> Result<(), Violation> {
        let name = format!("{}/{key}", self.bucket.name);
        let writes: Vec<&Operation> = self
            .operations
            .iter()
            .filter(|op| op.key == name && op.called <= self.loss.moment && op.may_have_written())
            .collect();
        let reported = report.lost.iter().find(|lost| lost.key == key);
        let violation = |reason: String| Violation {
            key: name.clone(),
            reason: format!("{reason} (report: {report:?})"),
            operations: writes.iter().map(ToString::to_string).collect(),
        };
        let in_window = writes.iter().any(|op| match op.outcome {
            Outcome::Done => op.answered.is_none_or(|answered| answered >= self.start),
            _ => true,
        });
        // The values the key may hold last: those of the acknowledged
        // writes no other acknowledged write follows.
        let done: Vec<&&Operation> = writes
            .iter()
            .filter(|op| op.outcome == Outcome::Done)
            .collect();
        let mut finals: Vec<Option<&str>> = done
            .iter()
            .filter(|op| !done.iter().any(|later| op.precedes(later)))
            .map(|op| op.written())
            .collect();
        finals.sort_unstable();
        finals.dedup();
        if done.is_empty() {
            finals.push(None);
        }
        if in_window || finals.len() > 1 {
            audit.in_window += 1;
            return Ok(());
        }
        audit.checked += 1;
        let last = finals[0];
        let lost = match home_prefix(self.bucket, self.backup) {
            Some(prefix) => {
                let held = self
                    .loss
                    .store
                    .object(&format!("{prefix}{key}"))
                    .map(|object| written(&object.body, &object.info.etag));
                held.as_deref() != last
            }
            None => last.is_some(),
        };
        match (lost, reported) {
            (true, None) => Err(violation(format!(
                "holds {last:?} only on the lost members, written before the window, \
                 and is not reported"
            ))),
            (false, Some(_)) => Err(violation(format!(
                "is reported lost, but its last value {last:?} survives"
            ))),
            (true, Some(reported)) => {
                let value = reported
                    .object
                    .as_ref()
                    .map(|object| object.etag.to_string());
                if value.as_deref() == last {
                    Ok(())
                } else {
                    Err(violation(format!(
                        "is reported lost as {value:?}, but its last value is {last:?}"
                    )))
                }
            }
            (false, None) => Ok(()),
        }
    }
}
