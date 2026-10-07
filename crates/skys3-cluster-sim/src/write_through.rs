//! Write-through buckets (§7.5, plan M4-08): the remote store alone must
//! hold every acknowledged write, so that losing the whole cluster loses
//! none.
//!
//! With [`ClusterConfig::write_through`](crate::ClusterConfig::write_through),
//! every `write_back` bucket acknowledges writes only once they are
//! flushed, and the `RemoteAudit` checks it twice, each time with the
//! remote store as the only survivor, as if every node's disks were
//! destroyed: when a client records an acknowledged write, for its key,
//! and once the clients are done, before any fault heals, for every key.
//! A seeded bug, [`WriteThrough::AckedLocally`], acknowledges writes after
//! their local commit only, as `ack_policy = "local"` does, while the
//! audit still runs.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use skys3_sim::SimS3;
use skys3_sim::check::{Survivors, Violation, check_durable};
use skys3_sim::history::{History, Operation};
use skys3_types::BucketMode;

use crate::cluster::{remote_prefix, written};
use crate::workload::{OnAck, Routes};

/// How `write_back` buckets acknowledge writes (§7.5), and whether the
/// run audits them: the remote store alone must then hold every
/// acknowledged write of such a bucket, or one that may have come after
/// it, when a client records its acknowledgement and once the clients are
/// done, as if every node's disks were destroyed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum WriteThrough {
    /// `ack_policy = "local"`, unaudited.
    #[default]
    Off,
    /// `ack_policy = "write_through"`, audited.
    On,
    /// A seeded bug: audited as `write_through`, but every write is
    /// acknowledged after its local commit only.
    AckedLocally,
}

impl WriteThrough {
    /// Whether the gateways wait for the remote flush.
    pub(crate) fn waits(self) -> bool {
        self == Self::On
    }

    /// Whether the audit runs.
    pub(crate) fn audited(self) -> bool {
        self != Self::Off
    }
}

/// How long a write-through write waits for its flush in the simulation
/// (`write_through_timeout_seconds`), well within a client's timeout.
pub(crate) const TIMEOUT: Duration = Duration::from_secs(2);

/// Checks that the remote store holds every acknowledged write of the
/// `write_back` buckets, or a write that may have come after it. Clones
/// share what they found.
#[derive(Clone)]
pub(crate) struct RemoteAudit {
    remote: SimS3,
    /// The remote key of each key of a `write_back` bucket, by the name
    /// the history gives it.
    keys: Arc<BTreeMap<String, String>>,
    history: History,
    violation: Arc<Mutex<Option<Violation>>>,
    checked: Arc<AtomicU64>,
}

impl RemoteAudit {
    /// An audit of the `keys` keys of each bucket in `routes` against
    /// `remote`, over `history`.
    pub(crate) fn new(remote: SimS3, routes: &Routes, keys: usize, history: History) -> Self {
        let keys = routes
            .keys(keys)
            .into_iter()
            .filter(|(_, bucket, _)| bucket.mode == BucketMode::WriteBack)
            .map(|(name, bucket, key)| {
                let remote_key = format!("{}{key}", remote_prefix(&bucket));
                (name, remote_key)
            })
            .collect();
        Self {
            remote,
            keys: Arc::new(keys),
            history,
            violation: Arc::default(),
            checked: Arc::default(),
        }
    }

    /// What clients call with each acknowledged write's key.
    pub(crate) fn on_ack(&self) -> OnAck {
        let audit = self.clone();
        Arc::new(move |name: &str| audit.acknowledged(name))
    }

    /// Checks the key the history names `name` against the remote store
    /// alone, right after a write of it was acknowledged.
    fn acknowledged(&self, name: &str) {
        if !self.keys.contains_key(name) {
            return;
        }
        self.checked.fetch_add(1, Ordering::Relaxed);
        let operations: Vec<Operation> = self
            .history
            .operations()
            .into_iter()
            .filter(|operation| operation.key == name)
            .collect();
        if let Err(violation) = check_durable(&operations, &self.survivors(Some(name))) {
            let mut found = self
                .violation
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            found.get_or_insert(violation);
        }
    }

    /// Checks every key against the remote store alone: what survives if
    /// every node's disks are lost now.
    pub(crate) fn lose_every_node(&self) -> Result<(), Violation> {
        let operations: Vec<Operation> = self
            .history
            .operations()
            .into_iter()
            .filter(|operation| self.keys.contains_key(&operation.key))
            .collect();
        check_durable(&operations, &self.survivors(None))
    }

    /// What the remote store holds of the key named `only`, or of every
    /// key; a key of a `local` bucket has no survivor.
    fn survivors(&self, only: Option<&str>) -> BTreeMap<String, Survivors> {
        self.keys
            .iter()
            .filter(|(name, _)| only.is_none_or(|only| only == name.as_str()))
            .map(|(name, remote_key)| {
                let flushed = self
                    .remote
                    .object(remote_key)
                    .map(|object| written(&object.body, &object.info.etag));
                let survivor = Survivors {
                    flushed: Some(flushed),
                    ..Survivors::default()
                };
                (name.clone(), survivor)
            })
            .collect()
    }

    /// The first acknowledged write the remote store did not hold when it
    /// was acknowledged, if any.
    pub(crate) fn violation(&self) -> Option<Violation> {
        self.violation
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    /// How many acknowledged writes were checked when acknowledged.
    pub(crate) fn checked(&self) -> u64 {
        self.checked.load(Ordering::Relaxed)
    }
}
