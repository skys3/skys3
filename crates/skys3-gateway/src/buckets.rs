//! Bucket records and the bucket operations that change them (design
//! §4.1, §6.1, §11).
//!
//! Bucket registers live in the control store, keyed by S3 name
//! (`buckets/<name>.json`). The gateway answers HeadBucket, ListBuckets,
//! and GetBucketLocation from its local copy, so those requests never
//! touch the control store (§6.2). CreateBucket and DeleteBucket write the
//! register, then increment the generation in `cluster.json`, which
//! announces the change to every node.
//!
//! **Creation.** A request picks a bucket's mode with the
//! [`MODE_HEADER`] header, defaulting to the `mode` of the bucket's
//! `[buckets.<name>]` table or `[buckets.defaults]`, and a `write_back`
//! bucket's target with [`TARGET_HEADER`], a path-style URL. The shard count
//! and replication settings come from the configuration. The bucket gets a
//! fresh random ID, and its shards are opened before its register is
//! created with `If-None-Match: *`, so a bucket is never visible without
//! its shards.
//!
//! **Deletion detaches.** The remote is never touched. DeleteBucket seals
//! every shard, so no client write commits while it decides, and refuses
//! with `409 BucketNotEmpty` while a `write_back` bucket has entries not
//! yet flushed, or a `local` bucket has any object. Otherwise it deletes
//! the register at the version it read, and then drops the shards.
//!
//! **Lost answers.** A creation or deletion that lands while every answer
//! is lost reports `503` and announces nothing. A retried CreateBucket that
//! finds the register announces it. A deletion of unknown outcome keeps its
//! seals and is remembered; a retried DeleteBucket that finds the register
//! gone, or naming a new bucket, drops the old shards, announces the
//! deletion, and answers `204` (design §4.1).

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Mutex, MutexGuard, PoisonError, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use s3s::{S3Error, s3_error};
use skys3_config::{BucketsConfig, Config, ControlStoreConfig, IdentityConfig, parse_target};
use skys3_control::{
    ControlError, ControlStore, DeletionOutcome, Expected, KeyPrefix, ProposalIds, ProposalOutcome,
    RegisterKind, RetryPolicy, TypedKey, Version, Versioned, bump_generation, propose_delete,
    propose_document, read, read_with_retries,
};
use skys3_io::BlockingPool;
use skys3_types::{BucketDocument, BucketId, BucketMode, BucketName, ClusterId, ProposalId};

use crate::authz::Permissions;
use crate::limits::RequestLimits;
use crate::shard::{ShardError, ShardRef, ShardSummary, Shards};

/// The CreateBucket header that sets the bucket's mode: `write_back`,
/// `local`, or `read_only`. S3 has no field for it.
pub const MODE_HEADER: &str = "x-skys3-bucket-mode";

/// The CreateBucket header that names a `write_back` bucket's target, as a
/// path-style URL: `https://host[:port]/bucket` or
/// `https://host[:port]/bucket/prefix`.
pub const TARGET_HEADER: &str = "x-skys3-bucket-target";

/// What the gateway needs from the node's configuration.
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    /// The cluster, whose generation bucket changes increment.
    pub cluster_id: ClusterId,
    /// Per-bucket settings that bucket creation applies.
    pub buckets: BucketsConfig,
    /// The control store, which a `write_back` bucket's target must not
    /// share a failure scope with.
    pub control_store: ControlStoreConfig,
    /// Request limits.
    pub limits: RequestLimits,
    /// Retries of control-store requests.
    pub retry: RetryPolicy,
    /// What unsigned requests may do: `anonymous_policy` when
    /// `anonymous_access` is on, and `None`, which refuses them with
    /// `403 AccessDenied`, when it is off.
    pub anonymous: Option<Permissions>,
    /// `inline_max_bytes`: an object body up to this size is stored in its
    /// `PUT` record; a larger one is streamed as `EXTENT` records (§5.1).
    /// It must not exceed the log's own `inline_max_bytes`.
    pub inline_max_bytes: u64,
    /// `extent_bytes`: the size of the `EXTENT` records a large body is
    /// streamed in.
    pub extent_bytes: u64,
    /// The pool that hashes object bodies (§7.4). Without one, bodies are
    /// hashed on the request's task, which only tests should do; the node
    /// sets one.
    pub hashing_pool: Option<BlockingPool>,
}

impl GatewayConfig {
    /// The gateway settings of a node configuration, with the default
    /// limits and retries, and no hashing pool.
    #[must_use]
    pub fn new(config: &Config) -> Self {
        Self {
            cluster_id: config.cluster().cluster_id.clone(),
            buckets: config.buckets().clone(),
            control_store: config.control_store().clone(),
            limits: RequestLimits::default(),
            retry: RetryPolicy::default(),
            anonymous: anonymous_permissions(config.identity()),
            inline_max_bytes: config.storage().inline_max_bytes,
            extent_bytes: config.storage().extent_bytes,
            hashing_pool: None,
        }
    }
}

/// What `[identity]` lets unsigned requests do.
fn anonymous_permissions(identity: &IdentityConfig) -> Option<Permissions> {
    if identity.anonymous_access {
        identity
            .anonymous_policy
            .clone()
            .map(Permissions::from_policy)
    } else {
        None
    }
}

/// A source of fresh bucket and proposal IDs.
///
/// A bucket ID is `b-` and 23 random base-32 characters (115 bits), the
/// 25 bytes `BucketId` allows. IDs are never reused because they are never
/// derived from anything: a collision needs about 2^57 buckets for even
/// odds, and the control store, which keys shards by bucket ID, has no
/// other way to tell two buckets apart.
#[derive(Debug, Clone)]
pub struct IdSource {
    rng: SmallRng,
}

impl IdSource {
    const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";

    /// A source seeded from the operating system, for nodes.
    #[must_use]
    pub fn from_os_rng() -> Self {
        Self {
            rng: SmallRng::from_os_rng(),
        }
    }

    /// A source seeded from `seed`, for simulation and tests.
    #[must_use]
    pub fn seeded(seed: u64) -> Self {
        Self {
            rng: SmallRng::seed_from_u64(seed),
        }
    }

    /// A fresh bucket ID.
    pub fn bucket_id(&mut self) -> BucketId {
        let mut bits: u128 = self.rng.random();
        let mut id = String::with_capacity(BucketId::MAX_LEN);
        id.push_str("b-");
        for _ in 0..BucketId::MAX_LEN - 2 {
            id.push(char::from(Self::ALPHABET[(bits % 32) as usize]));
            bits /= 32;
        }
        BucketId::new(id).expect("the alphabet and length make a valid bucket ID")
    }

    /// A fresh proposal ID.
    pub fn proposal_id(&mut self) -> ProposalId {
        ProposalId::from_u128(self.rng.random())
    }

    /// A proposal-ID source of its own, for a series of writes.
    pub fn proposal_ids(&mut self) -> ProposalIds {
        ProposalIds::seeded(self.rng.random())
    }
}

/// The most deletions of unknown outcome a gateway remembers. Beyond it the
/// oldest is forgotten: its shards stay sealed until a restart, and startup
/// recovery reclaims them if the register is gone.
const MAX_PENDING_DELETIONS: usize = 256;

/// A DeleteBucket whose register delete may or may not have applied. Its
/// seals are still held, so no write is acknowledged into a bucket that may
/// be gone.
#[derive(Debug)]
struct PendingDeletion {
    bucket: BucketDocument,
    /// The register version the delete was conditional on: while the
    /// register is at it, a late delete may still apply.
    version: Version,
    /// Seals held on every shard of the bucket.
    seals: u32,
}

/// The bucket records and the operations on them.
#[derive(Debug)]
pub(crate) struct Buckets<C, H> {
    store: C,
    shards: H,
    config: GatewayConfig,
    /// The local copy of every bucket register.
    catalog: RwLock<BTreeMap<BucketName, BucketDocument>>,
    /// Deletions of unknown outcome, oldest first.
    pending: Mutex<VecDeque<PendingDeletion>>,
    ids: Mutex<IdSource>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl<C: ControlStore, H: Shards> Buckets<C, H> {
    /// Loads every bucket register.
    pub(crate) async fn load(
        store: C,
        shards: H,
        config: GatewayConfig,
        ids: IdSource,
    ) -> Result<Self, ControlError> {
        let buckets = Self {
            store,
            shards,
            config,
            catalog: RwLock::default(),
            pending: Mutex::default(),
            ids: Mutex::new(ids),
        };
        buckets.reload().await?;
        Ok(buckets)
    }

    /// Replaces the local copy with the bucket registers in the control
    /// store.
    pub(crate) async fn reload(&self) -> Result<(), ControlError> {
        let mut catalog = BTreeMap::new();
        for (key, _) in self.store.list(&KeyPrefix::buckets()).await? {
            let RegisterKind::Bucket(name) = key.kind() else {
                continue;
            };
            let typed = TypedKey::bucket(&name);
            if let Some(bucket) = read_with_retries(&self.store, &typed, &self.config.retry).await?
            {
                catalog.insert(name, bucket.value);
            }
        }
        *self.catalog.write().unwrap_or_else(PoisonError::into_inner) = catalog;
        Ok(())
    }

    /// The bucket named `name`, from the local copy.
    pub(crate) fn get(&self, name: &BucketName) -> Option<BucketDocument> {
        self.catalog().get(name).cloned()
    }

    /// The bucket named `name`, or `NoSuchBucket`.
    pub(crate) fn require(&self, name: &BucketName) -> Result<BucketDocument, S3Error> {
        self.get(name).ok_or_else(no_such_bucket)
    }

    /// Every bucket, in name order, from the local copy.
    pub(crate) fn list(&self) -> Vec<BucketDocument> {
        self.catalog().values().cloned().collect()
    }

    fn catalog(&self) -> std::sync::RwLockReadGuard<'_, BTreeMap<BucketName, BucketDocument>> {
        self.catalog.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn remember(&self, name: &BucketName, bucket: Option<BucketDocument>) {
        let mut catalog = self.catalog.write().unwrap_or_else(PoisonError::into_inner);
        match bucket {
            Some(bucket) => catalog.insert(name.clone(), bucket),
            None => catalog.remove(name),
        };
    }

    /// Creates a bucket. `mode` and `target` are the values of
    /// [`MODE_HEADER`] and [`TARGET_HEADER`], if the request has them.
    pub(crate) async fn create(
        &self,
        name: BucketName,
        mode: Option<&str>,
        target: Option<&str>,
    ) -> Result<BucketDocument, S3Error> {
        if self.get(&name).is_some() {
            return Err(already_owned());
        }
        let settings = self.config.buckets.get(&name);
        let mode = match mode {
            Some(mode) => parse_mode(mode)?,
            None => settings.mode,
        };
        let target = match (mode, target) {
            (BucketMode::ReadOnly, _) => {
                return Err(s3_error!(
                    NotImplemented,
                    "read_only buckets are not supported yet"
                ));
            }
            (BucketMode::WriteBack, None) => {
                return Err(s3_error!(
                    InvalidArgument,
                    "A write_back bucket needs a target: set the {TARGET_HEADER} header to \
                     https://host[:port]/bucket or https://host[:port]/bucket/prefix"
                ));
            }
            (BucketMode::WriteBack, Some(url)) => {
                let target = parse_target(url).map_err(|e| s3_error!(InvalidArgument, "{e}"))?;
                self.config
                    .control_store
                    .check_target_independence(&target)
                    .map_err(|e| s3_error!(InvalidArgument, "{e}"))?;
                Some(target)
            }
            (BucketMode::Local, Some(_)) => {
                return Err(s3_error!(
                    InvalidArgument,
                    "A local bucket has no target; remove the {TARGET_HEADER} header"
                ));
            }
            (BucketMode::Local, None) => None,
        };
        let (bucket_id, proposal_id) = {
            let mut ids = lock(&self.ids);
            (ids.bucket_id(), ids.proposal_id())
        };
        let bucket = BucketDocument {
            bucket_id,
            name: name.clone(),
            mode,
            shards: settings.shards_per_bucket,
            replicas: settings.replication.replicas,
            min_write_replicas: settings.replication.min_write_replicas,
            clean_copies: settings.replication.clean_copies,
            target,
            created_unix_ms: unix_ms(SystemTime::now()),
            proposal_id,
        };

        let shards: Vec<_> = ShardRef::all(&bucket).collect();
        for (opened, shard) in shards.iter().enumerate() {
            if let Err(error) = self.shards.open(shard, &bucket).await {
                self.remove_shards(&shards[..opened]).await;
                return Err(shard_error(error));
            }
        }
        let key = TypedKey::bucket(&name);
        let retry = &self.config.retry;
        match propose_document(&self.store, &key, Expected::Absent, &bucket, retry).await {
            Ok(ProposalOutcome::Accepted(_)) => {
                self.remember(&name, Some(bucket.clone()));
                self.announce().await;
                Ok(bucket)
            }
            Ok(ProposalOutcome::Rejected) => {
                self.remove_shards(&shards).await;
                if let Ok(current) = read(&self.store, &key).await {
                    let found = current.is_some();
                    self.remember(&name, current.map(|current| current.value));
                    // The bucket may be one whose creation landed but whose
                    // answers were lost, here or on another gateway, before
                    // it was announced. Announcing again costs other nodes
                    // only a re-read.
                    if found {
                        self.announce().await;
                    }
                }
                Err(already_owned())
            }
            Err(error) => {
                // A write that may still land keeps its shards, so the
                // bucket works if it does; otherwise startup recovery
                // reclaims them.
                if !error.may_have_applied() {
                    self.remove_shards(&shards).await;
                }
                Err(control_error(&error))
            }
        }
    }

    /// Deletes (detaches) a bucket.
    pub(crate) async fn delete(&self, name: &BucketName) -> Result<(), S3Error> {
        let key = TypedKey::bucket(name);
        let retry = &self.config.retry;
        let current = read_with_retries(&self.store, &key, retry)
            .await
            .map_err(|error| control_error(&error))?;
        let pending = self.take_pending(name);
        let Some(current) = current else {
            self.remember(name, None);
            if let Some(pending) = pending {
                // An earlier DeleteBucket of this gateway applied, but its
                // answers were lost: finish what it started.
                self.finish_detach(&pending.bucket).await;
                return Ok(());
            }
            return Err(no_such_bucket());
        };
        // Seals an earlier DeleteBucket of unknown outcome still holds on
        // this bucket's shards.
        let mut held = 0;
        if let Some(pending) = pending {
            if pending.bucket.bucket_id != current.value.bucket_id {
                // The name now belongs to a new bucket: the earlier delete
                // applied.
                self.finish_detach(&pending.bucket).await;
            } else if pending.version != current.version {
                // The register moved on, so the earlier delete can no
                // longer apply.
                self.unseal_times(&pending.bucket, pending.seals).await;
            } else {
                held = pending.seals;
            }
        }
        let bucket = &current.value;
        let shards: Vec<_> = ShardRef::all(bucket).collect();
        let mut summary = ShardSummary::default();
        for (sealed, shard) in shards.iter().enumerate() {
            match self.shards.seal(shard).await {
                Ok(holds) => summary = summary.add(holds),
                Err(error) => {
                    self.unseal(&shards[..sealed]).await;
                    self.keep_pending(&current, held);
                    return Err(shard_error(error));
                }
            }
        }
        if let Some(refusal) = refuse_detach(bucket.mode, summary) {
            // Held seals came from a check that passed, and no client write
            // has committed since, so they cannot lead here; they are kept
            // all the same while the earlier delete may apply.
            self.unseal(&shards).await;
            self.keep_pending(&current, held);
            return Err(refusal);
        }
        match propose_delete(&self.store, key.key(), &current.version, retry).await {
            Ok(DeletionOutcome::Deleted) => {
                self.remember(name, None);
                self.finish_detach(bucket).await;
                Ok(())
            }
            Ok(DeletionOutcome::Rejected) => {
                // The register changed since it was read, so no delete
                // conditional on that version can apply. Lifting the seals
                // is safe whatever it holds now: the same bucket keeps
                // these shards, and another one has shards of its own.
                self.unseal_times(bucket, held + 1).await;
                match read(&self.store, &key).await {
                    Ok(None) => {
                        self.remember(name, None);
                        Err(no_such_bucket())
                    }
                    Ok(Some(now)) => {
                        self.remember(name, Some(now.value));
                        Err(s3_error!(
                            OperationAborted,
                            "A conflicting conditional operation is currently in progress \
                             against this resource. Please try again."
                        ))
                    }
                    Err(error) => Err(control_error(&error)),
                }
            }
            Err(error) => {
                // A delete that may still land keeps the shards sealed, so
                // no write is acknowledged into a bucket about to vanish,
                // and is remembered: a retried DeleteBucket resolves it.
                if error.may_have_applied() {
                    held += 1;
                } else {
                    self.unseal(&shards).await;
                }
                self.keep_pending(&current, held);
                Err(control_error(&error))
            }
        }
    }

    /// Drops a deleted bucket's shards and announces the deletion.
    async fn finish_detach(&self, bucket: &BucketDocument) {
        let shards: Vec<_> = ShardRef::all(bucket).collect();
        self.remove_shards(&shards).await;
        self.announce().await;
    }

    fn take_pending(&self, name: &BucketName) -> Option<PendingDeletion> {
        let mut pending = lock(&self.pending);
        let index = pending.iter().position(|p| p.bucket.name == *name)?;
        pending.remove(index)
    }

    /// Remembers that `held` seals stay on `current`'s shards because a
    /// delete conditional on its version may still apply.
    fn keep_pending(&self, current: &Versioned<BucketDocument>, held: u32) {
        if held == 0 {
            return;
        }
        let mut pending = lock(&self.pending);
        pending.push_back(PendingDeletion {
            bucket: current.value.clone(),
            version: current.version.clone(),
            seals: held,
        });
        if pending.len() > MAX_PENDING_DELETIONS
            && let Some(forgotten) = pending.pop_front()
        {
            tracing::warn!(
                bucket = %forgotten.bucket.name,
                "a deletion of unknown outcome was forgotten; its shards stay sealed until a restart"
            );
        }
    }

    async fn unseal_times(&self, bucket: &BucketDocument, times: u32) {
        let shards: Vec<_> = ShardRef::all(bucket).collect();
        for _ in 0..times {
            self.unseal(&shards).await;
        }
    }

    /// Increments the generation, which announces a bucket change to every
    /// node. A failure is only logged: the next increment announces every
    /// change before it (design §6.2).
    async fn announce(&self) {
        let mut ids = lock(&self.ids).proposal_ids();
        let cluster = &self.config.cluster_id;
        if let Err(error) =
            bump_generation(&self.store, cluster, &mut ids, &self.config.retry).await
        {
            tracing::warn!(%error, "a bucket change was not announced; the next change announces it");
        }
    }

    async fn unseal(&self, shards: &[ShardRef]) {
        for shard in shards {
            if let Err(error) = self.shards.unseal(shard).await {
                tracing::warn!(%error, "a shard seal was not lifted");
            }
        }
    }

    async fn remove_shards(&self, shards: &[ShardRef]) {
        for shard in shards {
            if let Err(error) = self.shards.remove(shard).await {
                tracing::warn!(%error, "a shard was not removed; startup recovery reclaims it");
            }
        }
    }
}

/// Why a bucket with these contents cannot be detached, if it cannot.
fn refuse_detach(mode: BucketMode, summary: ShardSummary) -> Option<S3Error> {
    match mode {
        BucketMode::Local if summary.objects > 0 => Some(s3_error!(
            BucketNotEmpty,
            "The bucket you tried to delete is not empty: it holds {} objects",
            summary.objects
        )),
        BucketMode::WriteBack | BucketMode::ReadOnly if summary.unflushed > 0 => Some(s3_error!(
            BucketNotEmpty,
            "The bucket has {} changes not yet flushed to its target; delete it once they \
                 are flushed",
            summary.unflushed
        )),
        _ => None,
    }
}

fn parse_mode(text: &str) -> Result<BucketMode, S3Error> {
    match text.trim() {
        "write_back" => Ok(BucketMode::WriteBack),
        "local" => Ok(BucketMode::Local),
        "read_only" => Ok(BucketMode::ReadOnly),
        other => Err(s3_error!(
            InvalidArgument,
            "{MODE_HEADER} is {other:?}; it must be write_back, local, or read_only"
        )),
    }
}

fn unix_ms(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).map_or(0, |since| {
        u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
    })
}

pub(crate) fn no_such_bucket() -> S3Error {
    s3_error!(NoSuchBucket, "The specified bucket does not exist")
}

fn already_owned() -> S3Error {
    s3_error!(
        BucketAlreadyOwnedByYou,
        "Your previous request to create the named bucket succeeded and you already own it."
    )
}

/// The S3 error for a failed control-store request: unavailable for
/// anything a retry may fix, internal otherwise.
fn control_error(error: &ControlError) -> S3Error {
    tracing::warn!(%error, "a control-store request failed");
    let transient = error.is_retryable()
        || matches!(
            error,
            ControlError::RetriesExhausted { .. } | ControlError::NotBootstrapped
        );
    if transient {
        s3_error!(
            ServiceUnavailable,
            "The cluster's control store is unavailable; retry later"
        )
    } else {
        s3_error!(InternalError, "The cluster's control store failed")
    }
}

pub(crate) fn shard_error(error: ShardError) -> S3Error {
    match error {
        ShardError::Sealed(_) => s3_error!(
            OperationAborted,
            "The bucket is being deleted; retry once the deletion has finished"
        ),
        ShardError::Invalid { .. } => {
            tracing::error!(%error, "a shard refused a record");
            s3_error!(InternalError)
        }
        other => {
            tracing::warn!(error = %other, "a shard request failed");
            s3_error!(
                ServiceUnavailable,
                "A shard of the bucket is unavailable; retry later"
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use s3s::S3ErrorCode;
    use skys3_control::MemoryControlStore;

    use super::*;
    use crate::stub::MemoryShards;

    #[tokio::test]
    async fn pending_deletions_are_bounded() {
        let config: Config = "[cluster]\ncluster_id = \"c\"\n[control_store]\n\
                              etcd_endpoints = [\"https://e:2379\"]"
            .parse()
            .unwrap();
        let buckets = Buckets::load(
            MemoryControlStore::new(),
            MemoryShards::new().await,
            GatewayConfig::new(&config),
            IdSource::seeded(1),
        )
        .await
        .unwrap();
        let bucket = |n: usize| Versioned {
            value: BucketDocument {
                bucket_id: format!("b-{n}").parse().unwrap(),
                name: format!("bucket-{n}").parse().unwrap(),
                mode: BucketMode::Local,
                shards: skys3_types::ShardCount::new(1).unwrap(),
                replicas: 1,
                min_write_replicas: 1,
                clean_copies: 0,
                target: None,
                created_unix_ms: 0,
                proposal_id: "p".parse().unwrap(),
            },
            version: Version::new("1"),
        };
        buckets.keep_pending(&bucket(0), 0);
        assert!(lock(&buckets.pending).is_empty());
        for n in 0..=MAX_PENDING_DELETIONS {
            buckets.keep_pending(&bucket(n), 1);
        }
        assert_eq!(lock(&buckets.pending).len(), MAX_PENDING_DELETIONS);
        assert!(buckets.take_pending(&bucket(0).value.name).is_none());
        let kept = buckets.take_pending(&bucket(1).value.name).unwrap();
        assert_eq!(kept.seals, 1);
    }

    #[test]
    fn bucket_ids_are_valid_distinct_and_seedable() {
        let mut ids = IdSource::seeded(1);
        let first = ids.bucket_id();
        assert_eq!(first.as_str().len(), BucketId::MAX_LEN);
        assert!(first.as_str().starts_with("b-"));
        assert_ne!(ids.bucket_id(), first);
        assert_eq!(IdSource::seeded(1).bucket_id(), first);
        assert_ne!(IdSource::from_os_rng().bucket_id(), first);
        assert_ne!(ids.proposal_id(), ids.proposal_id());
        let mut a = ids.clone().proposal_ids();
        let mut b = ids.proposal_ids();
        assert_eq!(a.next_id(), b.next_id());
    }

    #[test]
    fn modes_parse() {
        assert_eq!(parse_mode("write_back").unwrap(), BucketMode::WriteBack);
        assert_eq!(parse_mode(" local ").unwrap(), BucketMode::Local);
        assert_eq!(parse_mode("read_only").unwrap(), BucketMode::ReadOnly);
        let error = parse_mode("Local").unwrap_err();
        assert_eq!(*error.code(), S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn detach_refusals_follow_the_mode() {
        let summary = |objects, unflushed| ShardSummary { objects, unflushed };
        assert!(refuse_detach(BucketMode::Local, summary(0, 0)).is_none());
        assert!(refuse_detach(BucketMode::Local, summary(1, 1)).is_some());
        assert!(refuse_detach(BucketMode::WriteBack, summary(5, 0)).is_none());
        let refusal = refuse_detach(BucketMode::WriteBack, summary(5, 2)).unwrap();
        assert_eq!(*refusal.code(), S3ErrorCode::BucketNotEmpty);
        assert!(refusal.message().unwrap().contains("2 changes"));
    }

    #[test]
    fn times_are_milliseconds_since_the_epoch() {
        assert_eq!(unix_ms(UNIX_EPOCH + Duration::from_millis(1500)), 1500);
        assert_eq!(unix_ms(UNIX_EPOCH - Duration::from_secs(1)), 0);
    }

    #[test]
    fn errors_map_to_s3_codes() {
        let unavailable = control_error(&ControlError::Unavailable("down".into()));
        assert_eq!(*unavailable.code(), S3ErrorCode::ServiceUnavailable);
        let internal = control_error(&ControlError::Io(std::io::Error::other("disk")));
        assert_eq!(*internal.code(), S3ErrorCode::InternalError);
        let shard = ShardRef {
            bucket: "b-1".parse().unwrap(),
            shard: skys3_types::ShardId::new(0),
        };
        let sealed = shard_error(ShardError::Sealed(shard.clone()));
        assert_eq!(*sealed.code(), S3ErrorCode::OperationAborted);
        let missing = shard_error(ShardError::NotFound(shard));
        assert_eq!(*missing.code(), S3ErrorCode::ServiceUnavailable);
    }
}
