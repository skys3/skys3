//! Backup targets of `local` buckets (§8.9, plan M4-09): every committed
//! change of a `local` bucket must reach its backup target, and with
//! `backup_ack = "write_through"` a write is acknowledged only once the
//! backup holds it.
//!
//! With [`ClusterConfig::backup`](crate::ClusterConfig::backup), every
//! `local` bucket names a backup target in the remote store, under the key
//! prefix `backup/<bucket>/`, and the run audits it:
//!
//! - **Eventually.** Once the clients are done and every fault healed, the
//!   driver waits until the backup flushers of every live node have nothing
//!   left to send ([`drain`]). After the final power loss and recovery,
//!   the backup must hold, for every key, exactly what every member of its
//!   shard holds: the version they hold, or nothing for a key they deleted
//!   ([`audit`]). A change that never reached the backup, a delete
//!   included, fails it.
//! - **At each acknowledgement**, with `backup_ack = "write_through"`: the
//!   audit of write-through buckets (`RemoteAudit`) checks each
//!   acknowledged write against the backup alone, as if every node's disks
//!   were destroyed, and so it checks every key once the clients are done,
//!   before any fault heals.
//!
//! The members' own copies are checked as before: a `local` bucket's
//! replicas are its durable home, so an entry a backup made clean must
//! still hold its bytes on every member.
//!
//! Three seeded bugs show that the checks catch what they should:
//! [`Backup::EvictsLocal`] evicts backed-up payload as a `write_back`
//! bucket's cache would, [`Backup::AckedLocally`] acknowledges before the
//! backup has the write, and [`Backup::ForgetsDeletes`] records a delete
//! as flushed without deleting the key at the backup.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use skys3_flush::FlushService;
use skys3_io::SimMount;
use skys3_sim::SimS3;
use skys3_sim::check::{Survivors, Violation};
use skys3_sim::history::Operation;
use skys3_types::{BucketDocument, BucketMode, ShardId};

use crate::cluster::written;
use crate::workload::Routes;

/// Whether the `local` buckets have backup targets (§8.9), how writes to
/// them are acknowledged, and which seeded bug, if any, the nodes run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Backup {
    /// No backup targets.
    #[default]
    Off,
    /// A backup target with `backup_ack = "local"`: writes are answered
    /// after their local commit and reach the backup later.
    Asynchronous,
    /// A backup target with `backup_ack = "write_through"`: writes are
    /// answered once the backup holds them.
    WriteThrough,
    /// A seeded bug: as [`Backup::Asynchronous`], but the nodes' clean
    /// caches are told the `local` buckets' `clean_copies` too, so members
    /// evict the payload a backup made clean.
    EvictsLocal,
    /// A seeded bug: audited as [`Backup::WriteThrough`], but the gateways
    /// acknowledge every write after its local commit.
    AckedLocally,
    /// A seeded bug: as [`Backup::Asynchronous`], but the flushers record
    /// each delete as flushed, removing its tombstone, without deleting
    /// the key at the backup.
    ForgetsDeletes,
}

impl Backup {
    /// Whether the `local` buckets have backup targets.
    pub(crate) fn enabled(self) -> bool {
        self != Self::Off
    }

    /// Whether each acknowledged write is checked against the backup alone.
    pub(crate) fn audited_at_ack(self) -> bool {
        matches!(self, Self::WriteThrough | Self::AckedLocally)
    }

    /// Whether the nodes' clean caches evict `local` buckets (a seeded
    /// bug).
    pub(crate) fn evicts_local(self) -> bool {
        self == Self::EvictsLocal
    }

    /// Whether the flushers never delete a key at the backup (a seeded
    /// bug).
    pub(crate) fn forgets_deletes(self) -> bool {
        self == Self::ForgetsDeletes
    }

    /// The `[buckets.<name>]` tables of the `local` buckets `names`, as the
    /// flushers see them, or, with `gateway`, as the gateways do.
    pub(crate) fn tables<'a>(
        self,
        names: impl IntoIterator<Item = &'a str>,
        gateway: bool,
    ) -> String {
        let mut tables = String::new();
        if !self.enabled() {
            return tables;
        }
        let ack = match self {
            Self::WriteThrough => "write_through",
            Self::AckedLocally if !gateway => "write_through",
            _ => "local",
        };
        for name in names {
            let _ = write!(
                tables,
                "[buckets.{name}]\nmode = \"local\"\n\
                 backup_target = \"https://remote.sim.internal/remote/{}\"\n\
                 backup_ack = \"{ack}\"\n",
                prefix(name)
            );
        }
        tables
    }
}

/// The key prefix of the `local` bucket `name`'s backup target in the
/// remote store.
fn prefix(name: &str) -> String {
    format!("backup/{name}/")
}

/// The key prefix of `bucket`'s backup target in the remote store, if it
/// is a `local` bucket.
pub(crate) fn backup_prefix(bucket: &BucketDocument) -> Option<String> {
    (bucket.mode == BucketMode::Local).then(|| prefix(bucket.name.as_str()))
}

/// The flush services of every node's every life, which the drain asks
/// how far their backups are.
pub(crate) type Flushers = Mutex<Vec<Weak<FlushService<SimS3, SimMount>>>>;

/// How often the drain looks at the flushers.
const DRAIN_POLL: Duration = Duration::from_millis(100);

/// Waits, for at most `limit`, until the backup flushers of the live nodes
/// cover every shard of the `local` buckets of `routes` and have nothing
/// left to send: no key dirty, being flushed, or held in conflict. Returns
/// whether they did; if not, the audit says what the backup lacks.
pub(crate) async fn drain(flushers: Arc<Flushers>, routes: Routes, limit: Duration) -> bool {
    let buckets: Vec<BucketDocument> = routes
        .buckets
        .iter()
        .filter(|bucket| bucket.mode == BucketMode::Local)
        .cloned()
        .collect();
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        if drained(&flushers, &buckets) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(DRAIN_POLL).await;
    }
}

/// Whether the live flushers of every shard of `buckets` are idle.
fn drained(flushers: &Flushers, buckets: &[BucketDocument]) -> bool {
    let live: Vec<_> = flushers
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .filter_map(Weak::upgrade)
        .collect();
    buckets.iter().all(|bucket| {
        let mut idle = BTreeSet::<ShardId>::new();
        for service in &live {
            let Some(status) = service.status(&bucket.bucket_id) else {
                continue;
            };
            for (shard, flusher) in &status.shards {
                let quiet = !flusher.stopped
                    && flusher.dirty == 0
                    && flusher.flushing == 0
                    && flusher.conflicts.is_empty();
                if quiet {
                    idle.insert(*shard);
                } else {
                    return false;
                }
            }
        }
        bucket.shards.shards().all(|shard| idle.contains(&shard))
    })
}

/// Checks that the backup target of every `local` bucket in `routes` holds
/// exactly what every member holds of each of its `keys` keys, as
/// `survivors` found them after recovery, and returns how many keys the
/// backups hold an object for.
///
/// # Errors
///
/// The first key whose backup holds another version, or nothing, or an
/// object the members deleted.
pub(crate) fn audit(
    remote: &SimS3,
    routes: &Routes,
    keys: usize,
    survivors: &BTreeMap<String, Survivors>,
    operations: &[Operation],
) -> Result<usize, Violation> {
    let mut held = 0;
    for (name, bucket, key) in routes.keys(keys) {
        let Some(prefix) = backup_prefix(&bucket) else {
            continue;
        };
        let backup = remote
            .object(&format!("{prefix}{key}"))
            .map(|object| written(&object.body, &object.info.etag));
        held += usize::from(backup.is_some());
        let copies = survivors
            .get(&name)
            .map_or(&[][..], |survivor| &survivor.copies[..]);
        if let Some(copy) = copies.iter().find(|copy| **copy != backup) {
            let reason = format!(
                "the backup target holds {backup:?} once the flushers are done, but a member \
                 holds {copy:?} (every member: {copies:?})"
            );
            return Err(Violation {
                operations: operations
                    .iter()
                    .filter(|operation| operation.key == name)
                    .map(ToString::to_string)
                    .collect(),
                key: name,
                reason,
            });
        }
    }
    Ok(held)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_tables_say_what_each_side_sees() {
        let names = ["bucket-0", "bucket-1"];
        assert_eq!(Backup::Off.tables(names, false), "");
        let tables = Backup::Asynchronous.tables(names, false);
        assert!(tables.contains("[buckets.bucket-1]"), "{tables}");
        assert!(
            tables.contains(
                "backup_target = \"https://remote.sim.internal/remote/backup/bucket-0/\""
            ),
            "{tables}"
        );
        assert!(tables.contains("backup_ack = \"local\""), "{tables}");
        let through = Backup::WriteThrough.tables(names, true);
        assert!(
            through.contains("backup_ack = \"write_through\""),
            "{through}"
        );
        // The seeded bugs change only what the gateways see.
        let flushers = Backup::AckedLocally.tables(names, false);
        assert!(
            flushers.contains("backup_ack = \"write_through\""),
            "{flushers}"
        );
        let gateways = Backup::AckedLocally.tables(names, true);
        assert!(gateways.contains("backup_ack = \"local\""), "{gateways}");
        assert_eq!(
            Backup::ForgetsDeletes.tables(names, true),
            Backup::Asynchronous.tables(names, true)
        );
        assert!(Backup::AckedLocally.audited_at_ack() && Backup::WriteThrough.audited_at_ack());
        assert!(!Backup::Asynchronous.audited_at_ack());
        assert!(Backup::EvictsLocal.evicts_local() && !Backup::Asynchronous.evicts_local());
        assert!(Backup::ForgetsDeletes.forgets_deletes() && !Backup::Off.forgets_deletes());
    }
}
