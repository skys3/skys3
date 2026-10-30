//! Conflict policies (§7.2, plan M4-06): out-of-band writers at the remote
//! store, and the final remote state audited against the `write_back`
//! buckets' `flush_conflict_policy`.
//!
//! With [`ClusterConfig::conflicts`](crate::ClusterConfig::conflicts), the
//! `write_back` buckets take the policy of [`Conflicts::policy`], and an
//! out-of-band writer puts objects straight into the remote store at the
//! workload's keys while the clients run and the faults strike, so that
//! flushes find foreign objects before and after primaries change hands.
//! The history records each out-of-band write as a `PUT` that never gets
//! an answer: it may take effect at any time after its call, or never,
//! which is what a policy may make of it.
//!
//! Once every fault healed and the history is read back, a final phase
//! makes each key's last flush meet a known out-of-band write:
//!
//! 1. A drain waits until the flushers have nothing left to send but
//!    keys held in conflict.
//! 2. The out-of-band writer writes about half the keys again: the
//!    *burst*. No flush is in flight, so the next flush of each finds it.
//! 3. A client writes every key once more, its *final* write, and the
//!    drain waits again.
//! 4. Under `hold`, every burst key must be held, with the burst's object
//!    still at the remote. An operator then resolves each held key through
//!    [`FlushService::resolve`], with `overwrite` and `discard_local` in
//!    turn, and the drain waits for nothing to be held.
//!
//! After the final power loss and recovery, the audit checks each key: the
//! remote holds what every member holds, and that is what the policy says
//! it must be. Under `overwrite` it is the final write, flushed by SkyS3;
//! under `discard_local` it is the burst's object for a burst key, adopted
//! by every member, and the final write for the others; under `hold` it is
//! what the operator chose, and the final write for keys never held.
//!
//! Two seeded bugs show the audit catches what it should
//! ([`ConflictBug`]): flushers that discard local writes in a bucket that
//! did not opt in, and an operator's resolution that marks the key clean
//! instead of returning it to dirty.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use bytes::Bytes;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use skys3_config::ConflictPolicy;
use skys3_flush::FlushService;
use skys3_flush::test_hooks::ConflictBug;
use skys3_index::ImportCheckpoint;
use skys3_io::SimMount;
use skys3_remote::{ObjectStore, PutObject, UserMetadata};
use skys3_sim::SimS3;
use skys3_sim::check::{Survivors, Violation};
use skys3_sim::history::{Call, Condition, History, Operation};
use skys3_types::{BucketDocument, BucketMode, ClusterId, WriteIdentity};

use crate::backup::Flushers;
use crate::cluster::{remote_prefix, written};
use crate::workload::{Client, Routes, Workload, etag_of};

/// The conflict policy of the `write_back` buckets, the out-of-band
/// writes, and the seeded bug, if any.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Conflicts {
    /// The `write_back` buckets' `flush_conflict_policy`: in `[flush]` for
    /// `hold` and `overwrite`, in each bucket's own table for
    /// `discard_local`.
    pub policy: ConflictPolicy,
    /// How many out-of-band writes the writer makes while the clients run.
    pub writes: usize,
    /// A seeded bug the flushers run.
    pub bug: ConflictBug,
}

impl Conflicts {
    /// `policy`, with `writes` out-of-band writes and no bug.
    #[must_use]
    pub fn new(policy: ConflictPolicy, writes: usize) -> Self {
        Self {
            policy,
            writes,
            bug: ConflictBug::None,
        }
    }

    /// The same, running the seeded `bug`.
    #[must_use]
    pub fn with_bug(mut self, bug: ConflictBug) -> Self {
        self.bug = bug;
        self
    }

    /// The `[flush]` key and the `[buckets.<name>]` tables that give the
    /// `write_back` buckets `names` the policy.
    pub(crate) fn configuration<'a>(
        self,
        names: impl IntoIterator<Item = &'a str>,
    ) -> (String, String) {
        let name = policy_name(self.policy);
        if self.policy != ConflictPolicy::DiscardLocal {
            return (
                format!("flush_conflict_policy = \"{name}\"\n"),
                String::new(),
            );
        }
        let mut tables = String::new();
        for bucket in names {
            let _ = write!(
                tables,
                "[buckets.{bucket}]\nmode = \"write_back\"\nflush_conflict_policy = \"{name}\"\n"
            );
        }
        (String::new(), tables)
    }
}

/// The policy's name in configuration.
fn policy_name(policy: ConflictPolicy) -> &'static str {
    match policy {
        ConflictPolicy::Hold => "hold",
        ConflictPolicy::Overwrite => "overwrite",
        ConflictPolicy::DiscardLocal => "discard_local",
    }
}

/// What the audit found.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConflictAudit {
    /// Out-of-band writes the remote store applied, the burst's included.
    pub out_of_band: usize,
    /// Keys the burst wrote.
    pub burst: usize,
    /// Keys held in conflict once the final writes were flushed.
    pub held: usize,
    /// Keys whose remote object is the final write at the end.
    pub final_writes: usize,
    /// Keys whose remote object is an out-of-band write at the end, which
    /// every member adopted.
    pub adopted: usize,
}

/// What the final phase did, for the audit.
#[derive(Debug, Default)]
pub(crate) struct FinalPhase {
    /// The value of each key's burst write.
    burst: BTreeMap<String, String>,
    /// The value of each key's final write.
    finals: BTreeMap<String, String>,
    /// The keys held in conflict once the final writes were flushed, and
    /// what the remote held for each then.
    held: BTreeMap<String, Option<Remote>>,
    /// How the operator resolved each held key.
    resolved: BTreeMap<String, ConflictPolicy>,
    /// Out-of-band writes the store applied.
    out_of_band: usize,
    /// The value of every out-of-band write of each key, applied or not.
    written: BTreeMap<String, BTreeSet<String>>,
    /// Why the phase failed, if it did.
    failure: Option<String>,
}

/// The final phase's record, shared with the driver.
pub(crate) type Shared = Arc<Mutex<FinalPhase>>;

/// A remote object as the audit sees it: the value its write wrote, and
/// whether SkyS3 wrote it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Remote {
    value: String,
    ours: bool,
}

/// The object the remote store holds for `bucket`'s `key`.
fn remote(
    store: &SimS3,
    cluster: &ClusterId,
    bucket: &BucketDocument,
    key: &str,
) -> Option<Remote> {
    let object = store.object(&format!("{}{key}", remote_prefix(bucket)))?;
    let ours = object
        .info
        .metadata
        .write_identity()
        .and_then(|value| value.parse::<WriteIdentity>().ok())
        .is_some_and(|identity| identity.cluster == *cluster);
    Some(Remote {
        value: written(&object.body, &object.info.etag),
        ours,
    })
}

/// The keys of the `write_back` buckets of `routes`.
fn write_back_keys(routes: &Routes, keys: usize) -> Vec<(String, BucketDocument, String)> {
    routes
        .keys(keys)
        .into_iter()
        .filter(|(_, bucket, _)| bucket.mode == BucketMode::WriteBack)
        .collect()
}

/// Writes `body` straight into the remote store at `bucket`'s `key`, as
/// another writer would, and records it in `history` as a `PUT` that never
/// gets an answer. Retries until the store applies it if `until_applied`.
/// Returns the value written, and whether the store applied it.
async fn out_of_band(
    store: &SimS3,
    history: &History,
    phase: &Shared,
    (name, bucket, key): &(String, BucketDocument, String),
    body: String,
    until_applied: bool,
) -> String {
    let value = etag_of(body.as_bytes());
    lock(phase)
        .written
        .entry(name.clone())
        .or_default()
        .insert(value.clone());
    let call = Call::Put {
        value: value.clone(),
        condition: Condition::None,
    };
    // Never answered: the write may take effect in the cluster at any time
    // after its call, or never, as the policy decides.
    drop(history.call("out-of-band", name, call));
    let mut metadata = UserMetadata::new();
    metadata
        .insert("writer", "out-of-band")
        .expect("the metadata is valid");
    let request = PutObject::new(format!("{}{key}", remote_prefix(bucket)), Bytes::from(body))
        .with_metadata(metadata);
    loop {
        match store.put_object(request.clone()).await {
            Ok(_) => {
                lock(phase).out_of_band += 1;
                return value;
            }
            Err(_) if until_applied => tokio::time::sleep(Duration::from_millis(50)).await,
            Err(_) => return value,
        }
    }
}

/// What the out-of-band writer works with.
pub(crate) struct Writer {
    pub store: SimS3,
    pub routes: Routes,
    pub history: History,
    pub workload: Workload,
    pub flushers: Arc<Flushers>,
    /// The cluster's nodes, every one of which runs an import.
    pub nodes: usize,
}

/// How long the out-of-band writer waits for the namespace imports.
const IMPORT_LIMIT: Duration = Duration::from_secs(30);

/// The out-of-band writer that runs beside the clients: `writes` writes to
/// keys of the `write_back` buckets, each after a pause of up to
/// `think_time`. Counts the writes the store applied in `phase`.
///
/// It starts once every node's namespace import of the `write_back`
/// buckets is done, or after [`IMPORT_LIMIT`]. A node imports only into
/// the shards it leads, so an import that finds an object of a shard
/// another node leads waits for it forever (plan M2-17); the remote holds
/// only SkyS3's flushes, which no import meets, until the imports end.
pub(crate) async fn writer(
    writer: Writer,
    writes: usize,
    seed: u64,
    phase: Shared,
) -> turmoil::Result {
    let mut rng = SmallRng::seed_from_u64(seed);
    let keys = write_back_keys(&writer.routes, writer.workload.keys);
    let buckets = write_back_buckets(&writer.routes);
    let deadline = tokio::time::Instant::now() + IMPORT_LIMIT;
    while !imported(&writer.flushers, &buckets, writer.nodes)
        && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(DRAIN_POLL).await;
    }
    for n in 0..writes {
        let pause = rng.random_range(Duration::ZERO..=writer.workload.think_time);
        tokio::time::sleep(pause).await;
        let target = &keys[rng.random_range(0..keys.len())];
        let body = format!("out-of-band write {n} of {}", target.0);
        out_of_band(&writer.store, &writer.history, &phase, target, body, false).await;
    }
    Ok(())
}

/// The `write_back` buckets of `routes`.
pub(crate) fn write_back_buckets(routes: &Routes) -> Vec<BucketDocument> {
    routes
        .buckets
        .iter()
        .filter(|bucket| bucket.mode == BucketMode::WriteBack)
        .cloned()
        .collect()
}

/// Whether at least `nodes` live flush services finished the import of
/// every bucket of `buckets`.
fn imported(flushers: &Flushers, buckets: &[BucketDocument], nodes: usize) -> bool {
    let done = live(flushers)
        .iter()
        .filter(|service| {
            buckets.iter().all(|bucket| {
                service.status(&bucket.bucket_id).is_some_and(|status| {
                    status
                        .import
                        .is_some_and(|import| import.checkpoint == ImportCheckpoint::Done)
                })
            })
        })
        .count();
    done >= nodes
}

/// How long each drain waits for the flushers.
const DRAIN_LIMIT: Duration = Duration::from_secs(60);
/// How often a drain looks at the flushers.
const DRAIN_POLL: Duration = Duration::from_millis(100);
/// Attempts of each final write.
const FINAL_ATTEMPTS: usize = 40;

/// Everything the final phase works with.
pub(crate) struct Final {
    pub conflicts: Conflicts,
    pub store: SimS3,
    pub routes: Routes,
    pub history: History,
    pub workload: Workload,
    pub flushers: Arc<Flushers>,
    pub cluster: ClusterId,
    pub seed: u64,
    pub phase: Shared,
}

/// Runs the final phase (see the module documentation), recording what it
/// did, or why it failed, in `phase`.
pub(crate) async fn finish(run: Final) -> turmoil::Result {
    let phase = Arc::clone(&run.phase);
    if let Err(error) = run.steps().await {
        lock(&phase).failure = Some(error);
    }
    Ok(())
}

impl Final {
    async fn steps(self) -> Result<(), String> {
        let keys = write_back_keys(&self.routes, self.workload.keys);
        let buckets = write_back_buckets(&self.routes);
        drain(&self.flushers, &buckets).await?;

        // The burst: about half the keys, at least one.
        let mut rng = SmallRng::seed_from_u64(self.seed);
        let mut burst = BTreeMap::new();
        for (n, target) in keys.iter().enumerate() {
            if rng.random_bool(0.5) || (n + 1 == keys.len() && burst.is_empty()) {
                let body = format!("out-of-band burst of {}", target.0);
                let value =
                    out_of_band(&self.store, &self.history, &self.phase, target, body, true).await;
                burst.insert(target.0.clone(), value);
            }
        }

        let mut client = Client::new(
            "finisher".to_owned(),
            self.routes.clone(),
            self.history.clone(),
            self.workload.clone(),
            self.seed.rotate_left(13),
        );
        let finals = client
            .write_each(&keys, FINAL_ATTEMPTS)
            .await
            .map_err(|error| error.to_string())?;
        let held: BTreeMap<String, Option<Remote>> = drain(&self.flushers, &buckets)
            .await?
            .into_iter()
            .map(|name| {
                let (_, bucket, key) = keys
                    .iter()
                    .find(|(listed, ..)| *listed == name)
                    .ok_or_else(|| format!("{name} is not a key of the workload"))?;
                let found = remote(&self.store, &self.cluster, bucket, key);
                Ok((name, found))
            })
            .collect::<Result<_, String>>()?;

        // An operator resolves every held key, with each policy in turn.
        let mut resolved = BTreeMap::new();
        if self.conflicts.policy == ConflictPolicy::Hold {
            for (n, name) in held.keys().enumerate() {
                let policy = if n % 2 == 0 {
                    ConflictPolicy::Overwrite
                } else {
                    ConflictPolicy::DiscardLocal
                };
                let (_, bucket, key) = keys
                    .iter()
                    .find(|(listed, ..)| listed == name)
                    .ok_or_else(|| format!("{name} is not a key of the workload"))?;
                resolve(&self.flushers, bucket, key, policy).await?;
                resolved.insert(name.clone(), policy);
            }
            let still = drain(&self.flushers, &buckets).await?;
            if !still.is_empty() {
                return Err(format!(
                    "keys held after the operator resolved every key: {still:?}"
                ));
            }
        }
        let mut phase = lock(&self.phase);
        phase.burst = burst;
        phase.finals = finals;
        phase.held = held;
        phase.resolved = resolved;
        Ok(())
    }
}

/// The live flush services.
fn live(flushers: &Flushers) -> Vec<Arc<FlushService<SimS3, SimMount>>> {
    flushers
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .filter_map(Weak::upgrade)
        .collect()
}

/// Waits until the live flushers cover every shard of `buckets` and have
/// nothing left to send but keys held in conflict, and returns the keys
/// held, as the history names them.
pub(crate) async fn drain(
    flushers: &Flushers,
    buckets: &[BucketDocument],
) -> Result<BTreeSet<String>, String> {
    let deadline = tokio::time::Instant::now() + DRAIN_LIMIT;
    loop {
        match idle(flushers, buckets) {
            Ok(held) => return Ok(held),
            Err(busy) if tokio::time::Instant::now() >= deadline => {
                return Err(format!("the flushers did not drain: {busy}"));
            }
            Err(_) => tokio::time::sleep(DRAIN_POLL).await,
        }
    }
}

/// The keys held in conflict, if the live flushers of every shard of
/// `buckets` have nothing else left to send, or what is still busy.
fn idle(flushers: &Flushers, buckets: &[BucketDocument]) -> Result<BTreeSet<String>, String> {
    let services = live(flushers);
    let mut held = BTreeSet::new();
    for bucket in buckets {
        let mut covered = BTreeSet::new();
        for service in &services {
            let Some(status) = service.status(&bucket.bucket_id) else {
                continue;
            };
            for (shard, flusher) in &status.shards {
                if flusher.stopped || flusher.dirty > 0 || flusher.flushing > 0 {
                    return Err(format!(
                        "{} shard {shard}: {flusher:?}, import {:?}",
                        bucket.name, status.import
                    ));
                }
                covered.insert(*shard);
                for conflict in &flusher.conflicts {
                    held.insert(format!("{}/{}", bucket.name, conflict.key));
                }
            }
        }
        if let Some(shard) = bucket
            .shards
            .shards()
            .find(|shard| !covered.contains(shard))
        {
            return Err(format!("{} shard {shard} has no flusher", bucket.name));
        }
    }
    Ok(held)
}

/// Resolves the conflict `bucket`'s `key` is held in under `policy`, on
/// the node whose flusher holds it.
async fn resolve(
    flushers: &Flushers,
    bucket: &BucketDocument,
    key: &str,
    policy: ConflictPolicy,
) -> Result<(), String> {
    let mut refusals = Vec::new();
    for service in live(flushers) {
        match service.resolve(&bucket.bucket_id, key, policy).await {
            Ok(()) => return Ok(()),
            Err(refusal) => refusals.push(refusal.to_string()),
        }
    }
    Err(format!(
        "no node resolved {}/{key}: {refusals:?}",
        bucket.name
    ))
}

fn lock(phase: &Shared) -> std::sync::MutexGuard<'_, FinalPhase> {
    phase.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Checks every key of the `write_back` buckets of `routes` against the
/// policy's rule, with `survivors` as recovery found the members' copies,
/// and returns what it found.
///
/// # Errors
///
/// The final phase's failure, or the first key whose remote object, or a
/// member's copy, is not what the policy says.
#[allow(
    clippy::too_many_arguments,
    reason = "the parts of the run the audit reads"
)]
pub(crate) fn audit(
    conflicts: Conflicts,
    phase: &Shared,
    store: &SimS3,
    cluster: &ClusterId,
    routes: &Routes,
    keys: usize,
    survivors: &BTreeMap<String, Survivors>,
    operations: &[Operation],
) -> Result<ConflictAudit, Violation> {
    let phase = lock(phase);
    let violation = |key: &str, reason: String| Violation {
        operations: operations
            .iter()
            .filter(|operation| operation.key == key)
            .map(ToString::to_string)
            .collect(),
        key: key.to_owned(),
        reason,
    };
    if let Some(failure) = &phase.failure {
        return Err(violation("", format!("the final phase failed: {failure}")));
    }
    let mut audit = ConflictAudit {
        out_of_band: phase.out_of_band,
        burst: phase.burst.len(),
        held: phase.held.len(),
        ..ConflictAudit::default()
    };
    // Under `hold`, the final write of every burst key found the burst's
    // object and was held, and nothing replaced that object.
    if conflicts.policy == ConflictPolicy::Hold {
        for (name, value) in &phase.burst {
            let expected = Some(Remote {
                value: value.clone(),
                ours: false,
            });
            match phase.held.get(name) {
                Some(found) if *found == expected => {}
                Some(found) => {
                    let reason = format!(
                        "under hold, the remote held {found:?} instead of the out-of-band write \
                         {value} it was held for"
                    );
                    return Err(violation(name, reason));
                }
                None => {
                    let reason = format!(
                        "under hold, the final write was not held over the out-of-band write \
                         {value}"
                    );
                    return Err(violation(name, reason));
                }
            }
        }
    }
    for (name, bucket, key) in write_back_keys(routes, keys) {
        let found = remote(store, cluster, &bucket, &key);
        let Some(last) = phase.finals.get(&name) else {
            return Err(violation(&name, "no final write was recorded".to_owned()));
        };
        let final_write = Remote {
            value: last.clone(),
            ours: true,
        };
        let adopted = |found: &Option<Remote>| found.as_ref().is_some_and(|f| !f.ours);
        // An out-of-band write of the key that no flush followed before the
        // final write's: that flush found it, and dropped the final write.
        let earlier = adopted(&found)
            && phase
                .written
                .get(&name)
                .zip(found.as_ref())
                .is_some_and(|(written, found)| written.contains(&found.value));
        let (rule, ok) = match conflicts.policy {
            ConflictPolicy::Overwrite => ("overwrite: the final write", found == Some(final_write)),
            ConflictPolicy::DiscardLocal => match phase.burst.get(&name) {
                Some(value) => (
                    "discard_local: the burst's out-of-band write",
                    found.as_ref().is_some_and(|f| !f.ours && f.value == *value),
                ),
                None => (
                    "discard_local: the final write, or an out-of-band write of the key",
                    found == Some(final_write) || earlier,
                ),
            },
            ConflictPolicy::Hold => match phase.resolved.get(&name) {
                Some(ConflictPolicy::DiscardLocal) => (
                    "hold, resolved with discard_local: the out-of-band write it was held for",
                    adopted(&found) && found == phase.held[&name],
                ),
                Some(_) => (
                    "hold, resolved with overwrite: the final write",
                    found == Some(final_write),
                ),
                None => (
                    "hold, never held: the final write",
                    found == Some(final_write),
                ),
            },
        };
        if !ok {
            let reason = format!("the remote store holds {found:?}, but the rule is {rule}");
            return Err(violation(&name, reason));
        }
        let value = found.as_ref().map(|found| found.value.clone());
        let copies = survivors
            .get(&name)
            .map_or(&[][..], |survivor| &survivor.copies[..]);
        if let Some(copy) = copies.iter().find(|copy| **copy != value) {
            let reason = format!(
                "the remote store holds {value:?} once the flushers are done, but a member holds \
                 {copy:?} (every member: {copies:?}) under {rule}"
            );
            return Err(violation(&name, reason));
        }
        if adopted(&found) {
            audit.adopted += 1;
        } else {
            audit.final_writes += 1;
        }
    }
    Ok(audit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_configuration_names_the_policy_where_it_is_allowed() {
        let names = ["bucket-1", "bucket-2"];
        let (flush, tables) = Conflicts::new(ConflictPolicy::Overwrite, 4).configuration(names);
        assert_eq!(flush, "flush_conflict_policy = \"overwrite\"\n");
        assert_eq!(tables, "");
        let (flush, tables) = Conflicts::new(ConflictPolicy::DiscardLocal, 4).configuration(names);
        assert_eq!(flush, "");
        assert!(tables.contains("[buckets.bucket-2]\nmode = \"write_back\"\n"));
        assert!(tables.contains("flush_conflict_policy = \"discard_local\""));
        let seeded = Conflicts::new(ConflictPolicy::Hold, 0).with_bug(ConflictBug::ResolvesClean);
        assert_eq!(seeded.bug, ConflictBug::ResolvesClean);
    }
}
