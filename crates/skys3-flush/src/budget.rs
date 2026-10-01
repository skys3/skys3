//! The dirty-data budget (§7.6): how many bytes of committed writes may wait
//! for the remote, per bucket and per cluster, and this node's share of
//! each.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use skys3_config::BucketsConfig;
use skys3_types::{BucketDocument, BucketId, BucketName};

/// Which budget turned a write away.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Exhausted {
    /// The bucket's `max_dirty_bytes`.
    Bucket,
    /// The cluster's `flush.max_dirty_bytes`.
    Cluster,
}

/// Dirty bytes against a share of a budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Usage {
    /// Bytes of versions not yet at the remote, held conflicts included.
    pub dirty: u64,
    /// This node's share of the budget.
    pub share: u64,
}

/// One budget's dirty bytes, kept by the flushers that count against it.
#[derive(Debug)]
pub(crate) struct Account {
    dirty: AtomicU64,
    share: AtomicU64,
}

impl Account {
    fn new(share: u64) -> Self {
        Self {
            dirty: AtomicU64::new(0),
            share: AtomicU64::new(share),
        }
    }

    fn usage(&self) -> Usage {
        Usage {
            dirty: self.dirty.load(Ordering::Relaxed),
            share: self.share.load(Ordering::Relaxed),
        }
    }

    fn exhausted(&self) -> bool {
        let usage = self.usage();
        usage.dirty >= usage.share
    }

    fn adjust(&self, from: u64, to: u64) {
        if to >= from {
            self.dirty.fetch_add(to - from, Ordering::Relaxed);
        } else {
            let less = from - to;
            // Every byte added is subtracted once; saturating keeps an
            // accounting slip from refusing every later write.
            let _ = self
                .dirty
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |dirty| {
                    Some(dirty.saturating_sub(less))
                });
        }
    }
}

/// What one shard flusher counts its dirty bytes against: its bucket's
/// budget and the cluster's.
#[derive(Debug, Clone)]
pub(crate) struct Charge {
    bucket: Arc<Account>,
    cluster: Arc<Account>,
}

impl Charge {
    /// Moves the flusher's dirty bytes from `from` to `to`.
    pub(crate) fn adjust(&self, from: u64, to: u64) {
        self.bucket.adjust(from, to);
        self.cluster.adjust(from, to);
    }
}

/// The dirty-data budgets of a node (§7.6).
///
/// Every shard flusher counts the bytes of the versions it has not yet put
/// at the remote, held conflicts included, against its bucket's
/// `max_dirty_bytes` and the cluster's `flush.max_dirty_bytes`.
/// [`DirtyBudget::check`] tells the gateway whether a write that adds data
/// may proceed: once either budget is used up, it may not, and the client
/// gets `503 SlowDown` until flushing drains the dirty set. During a remote
/// outage nothing drains, so writes stop at the budget instead of filling
/// the disks.
///
/// **Shares.** A bucket's shards have their primaries, and so their
/// flushers, on different nodes, and each node admits writes alone. Each
/// node therefore enforces a share of every budget in proportion to the
/// shards whose primary it is: `max_dirty_bytes × held ÷ shards` of a
/// bucket's budget, and of the cluster's the same fraction over the shards
/// of every `write_back` bucket. The shares never add up to more than the
/// budget, and no write waits for another node. With all primaries on one
/// node, as in a single-node cluster, its share is the whole budget.
///
/// The check is made when a write arrives and the bytes are counted once
/// it commits, so writes in flight when a budget fills may overshoot it,
/// as may the writes to a bucket in the moments before its flushers
/// start. A bucket without flushers here is not limited.
pub struct DirtyBudget {
    cluster_limit: u64,
    buckets_config: Option<BucketsConfig>,
    cluster: Arc<Account>,
    buckets: Mutex<HashMap<BucketId, Arc<Account>>>,
}

impl fmt::Debug for DirtyBudget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DirtyBudget")
            .field("cluster_limit", &self.cluster_limit)
            .field("cluster", &self.cluster.usage())
            .finish_non_exhaustive()
    }
}

impl DirtyBudget {
    /// A budget of `cluster_limit` bytes for the cluster, which also limits
    /// each bucket until [`DirtyBudget::with_buckets`] says otherwise.
    #[must_use]
    pub fn new(cluster_limit: u64) -> Self {
        Self {
            cluster_limit,
            buckets_config: None,
            cluster: Arc::new(Account::new(cluster_limit)),
            buckets: Mutex::default(),
        }
    }

    /// A budget that never refuses a write.
    #[must_use]
    pub fn unlimited() -> Self {
        Self::new(u64::MAX)
    }

    /// Takes each bucket's `max_dirty_bytes` from `buckets`.
    #[must_use]
    pub fn with_buckets(mut self, buckets: BucketsConfig) -> Self {
        self.buckets_config = Some(buckets);
        self
    }

    /// The budget of the bucket named `name`.
    #[must_use]
    pub fn bucket_limit(&self, name: &BucketName) -> u64 {
        self.buckets_config
            .as_ref()
            .map_or(self.cluster_limit, |buckets| {
                buckets.get(name).max_dirty_bytes
            })
    }

    /// Whether a write that adds data to `bucket` may proceed now.
    ///
    /// # Errors
    ///
    /// Which budget is used up: the bucket's share is checked first.
    pub fn check(&self, bucket: &BucketId) -> Result<(), Exhausted> {
        let Some(account) = self.lock().get(bucket).cloned() else {
            return Ok(());
        };
        if account.exhausted() {
            Err(Exhausted::Bucket)
        } else if self.cluster.exhausted() {
            Err(Exhausted::Cluster)
        } else {
            Ok(())
        }
    }

    /// `bucket`'s dirty bytes and this node's share of its budget, if it
    /// has flushers here.
    #[must_use]
    pub fn usage(&self, bucket: &BucketId) -> Option<Usage> {
        self.lock().get(bucket).map(|account| account.usage())
    }

    /// The dirty bytes of every bucket flushed here, and this node's share
    /// of the cluster's budget.
    #[must_use]
    pub fn cluster_usage(&self) -> Usage {
        self.cluster.usage()
    }

    /// Sets the shares from the `write_back` buckets flushed here, each with
    /// the number of its shards whose flusher runs on this node, and
    /// forgets every other bucket.
    pub(crate) fn plan<'a>(&self, buckets: impl IntoIterator<Item = (&'a BucketDocument, u32)>) {
        let (mut held, mut total) = (0u64, 0u64);
        let mut accounts = self.lock();
        let mut kept = HashMap::with_capacity(accounts.len());
        for (bucket, open) in buckets {
            let shards = bucket.shards.get();
            let share = share(self.bucket_limit(&bucket.name), open, shards);
            let account = accounts
                .remove(&bucket.bucket_id)
                .unwrap_or_else(|| Arc::new(Account::new(share)));
            account.share.store(share, Ordering::Relaxed);
            kept.insert(bucket.bucket_id.clone(), account);
            held += u64::from(open.min(shards));
            total += u64::from(shards);
        }
        *accounts = kept;
        let cluster = if total == 0 {
            self.cluster_limit
        } else {
            scale(self.cluster_limit, held, total)
        };
        self.cluster.share.store(cluster, Ordering::Relaxed);
    }

    /// What a flusher of `bucket` counts against.
    pub(crate) fn charge(&self, bucket: &BucketId) -> Charge {
        let account = self
            .lock()
            .entry(bucket.clone())
            .or_insert_with(|| Arc::new(Account::new(self.cluster_limit)))
            .clone();
        Charge {
            bucket: account,
            cluster: Arc::clone(&self.cluster),
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<BucketId, Arc<Account>>> {
        // Every update leaves the map consistent.
        self.buckets.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A node's share of a `limit`-byte budget over `shards` shards, `held` of
/// which have their primary on the node: `limit × held ÷ shards`, rounded
/// down, and the whole budget when it holds them all.
#[must_use]
pub fn share(limit: u64, held: u32, shards: u32) -> u64 {
    if shards == 0 {
        return 0;
    }
    scale(limit, u64::from(held.min(shards)), u64::from(shards))
}

fn scale(limit: u64, part: u64, whole: u64) -> u64 {
    let scaled = u128::from(limit) * u128::from(part) / u128::from(whole);
    // `part` is at most `whole`, so the result fits.
    u64::try_from(scaled).unwrap_or(limit)
}

#[cfg(test)]
mod tests {
    use skys3_types::{BucketMode, ProposalId, ShardCount};

    use super::*;

    fn bucket(id: &str, name: &str, shards: u32) -> BucketDocument {
        BucketDocument {
            bucket_id: BucketId::new(id).unwrap(),
            name: name.parse().unwrap(),
            mode: BucketMode::WriteBack,
            shards: ShardCount::new(shards).unwrap(),
            replicas: 1,
            min_write_replicas: 1,
            clean_copies: 1,
            target: None,
            created_unix_ms: 0,
            proposal_id: ProposalId::new("p").unwrap(),
        }
    }

    #[test]
    fn shares_are_proportional_to_the_primaries_held() {
        assert_eq!(share(1000, 8, 8), 1000);
        assert_eq!(share(1000, 3, 8), 375);
        assert_eq!(share(1000, 0, 8), 0);
        assert_eq!(share(1000, 9, 8), 1000, "never more than the budget");
        assert_eq!(share(1000, 1, 0), 0);
        assert_eq!(share(u64::MAX, 1, 2), u64::MAX / 2, "no overflow");
        // The shares of every node add up to at most the budget.
        let split = [3, 3, 2].map(|held| share(1001, held, 8));
        assert!(split.iter().sum::<u64>() <= 1001);
    }

    #[test]
    fn writes_are_refused_once_a_share_is_used() {
        let config: skys3_config::Config = r#"
            [cluster]
            cluster_id = "c"
            [control_store]
            etcd_endpoints = ["https://e:2379"]
            [flush]
            max_dirty_bytes = 1000
            [buckets.small]
            max_dirty_bytes = 100
        "#
        .parse()
        .unwrap();
        let budget =
            DirtyBudget::new(config.flush().max_dirty_bytes).with_buckets(config.buckets().clone());
        let (small, large) = (bucket("b-1", "small", 4), bucket("b-2", "large", 4));
        assert_eq!(budget.bucket_limit(&small.name), 100);
        assert_eq!(budget.bucket_limit(&large.name), 1000);
        // Half of the small bucket's primaries and all of the large one's.
        budget.plan([(&small, 2), (&large, 4)]);
        assert_eq!(budget.usage(&small.bucket_id).unwrap().share, 50);
        assert_eq!(budget.cluster_usage().share, 750);

        let charge = budget.charge(&small.bucket_id);
        charge.adjust(0, 49);
        assert_eq!(budget.check(&small.bucket_id), Ok(()));
        charge.adjust(49, 50);
        assert_eq!(budget.check(&small.bucket_id), Err(Exhausted::Bucket));
        charge.adjust(50, 10);
        assert_eq!(budget.check(&small.bucket_id), Ok(()));

        let other = budget.charge(&large.bucket_id);
        other.adjust(0, 740);
        assert_eq!(budget.check(&large.bucket_id), Err(Exhausted::Cluster));
        assert_eq!(budget.check(&small.bucket_id), Err(Exhausted::Cluster));
        other.adjust(740, 0);
        assert_eq!(
            budget.cluster_usage(),
            Usage {
                dirty: 10,
                share: 750
            }
        );

        // A bucket no flusher counts for is not limited.
        let unknown = BucketId::new("b-3").unwrap();
        assert_eq!(budget.check(&unknown), Ok(()));
        assert_eq!(budget.usage(&unknown), None);
        // A bucket dropped from the plan is forgotten.
        budget.plan([(&large, 4)]);
        assert_eq!(budget.usage(&small.bucket_id), None);
        assert_eq!(budget.cluster_usage().share, 1000);
        budget.plan([]);
        assert_eq!(budget.cluster_usage().share, 1000);
    }

    #[test]
    fn accounting_slips_saturate_at_zero() {
        let budget = DirtyBudget::new(10);
        let id = BucketId::new("b-1").unwrap();
        let charge = budget.charge(&id);
        charge.adjust(5, 0);
        assert_eq!(budget.usage(&id).unwrap().dirty, 0);
        assert_eq!(budget.cluster_usage().dirty, 0);
        assert!(format!("{budget:?}").contains("cluster_limit: 10"));
        assert_eq!(DirtyBudget::unlimited().check(&id), Ok(()));
    }
}
