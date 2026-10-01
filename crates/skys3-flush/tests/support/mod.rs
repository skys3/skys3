//! Shared test support: a node with one shard on a simulated disk, a
//! simulated remote, and helpers to write and to wait for the flush.

#![allow(dead_code, reason = "each test target uses part of the support")]

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use skys3_flush::{FlushSettings, ShardFlusher, Target};
use skys3_index::{Entry, EntryState, Index, IndexConfig};
use skys3_io::{BlockingPool, MonotonicClock, SimDisk, SimMount};
use skys3_log::record::{Delete, Put, PutData, Tags};
use skys3_log::{LogConfig, RecordBody, SegmentLog, ShardRef};
use skys3_remote::probe::{ConditionalWrites, OperationSupport, PreconditionSupport};
use skys3_shard::{Shard, ShardSet};
use skys3_sim::SimS3;
use skys3_sim::s3::SimS3Config;
use skys3_types::{BucketId, ClusterId, ETag, Epoch, NodeId, ProposalId, ShardConfig, ShardId};

/// The cluster every test writes as.
pub fn cluster() -> ClusterId {
    ClusterId::new("c-test").unwrap()
}

/// Shard 0 of bucket `b-flush`.
pub fn shard_ref() -> ShardRef {
    ShardRef::new(BucketId::new("b-flush").unwrap(), ShardId::new(0))
}

/// A paused current-thread runtime: timers fire as soon as nothing else
/// can run.
pub fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap()
}

/// What a target honors, operation by operation.
pub fn writes(put: bool, complete: bool, delete: bool) -> ConditionalWrites {
    let support = |honored: bool| {
        if honored {
            PreconditionSupport::Honored
        } else {
            PreconditionSupport::Ignored
        }
    };
    ConditionalWrites {
        put_object: OperationSupport {
            if_none_match: Some(support(put)),
            if_match: support(put),
        },
        complete_multipart_upload: OperationSupport {
            if_none_match: Some(support(complete)),
            if_match: support(complete),
        },
        delete_object: OperationSupport {
            if_none_match: None,
            if_match: support(delete),
        },
    }
}

/// Settings with short backoffs.
pub fn settings() -> FlushSettings {
    FlushSettings {
        concurrency: 4,
        min_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(200),
        ..FlushSettings::default()
    }
}

/// A node with one open shard.
pub struct Node {
    pub disk: SimDisk,
    pub set: ShardSet<SimMount>,
    pub shard: Shard<SimMount>,
    pub pool: BlockingPool,
}

impl Node {
    /// Opens a node on a new simulated disk, seeded with `seed`.
    pub async fn open(seed: u64) -> Node {
        let disk = SimDisk::new(seed);
        let mount = disk.mount();
        let log_config = LogConfig {
            inline_max_bytes: 4096,
            segment_bytes: 1 << 20,
            group_commit_max_delay: Duration::ZERO,
            group_commit_max_bytes: 1 << 20,
        };
        let (log, _) = SegmentLog::open(mount.clone(), log_config, Arc::new(MonotonicClock::new()))
            .await
            .unwrap();
        let index_config = IndexConfig {
            checkpoint_interval: Duration::from_secs(10),
            cache_bytes: 1 << 20,
        };
        let index = Arc::new(Index::open_sim(&mount, "index.redb", &index_config).unwrap());
        let pool = BlockingPool::new("index", NonZeroUsize::MIN).unwrap();
        let set = ShardSet::new(index, log, pool.clone());
        let shard = set.open(&config()).await.unwrap();
        Node {
            disk,
            set,
            shard,
            pool,
        }
    }

    /// Starts a flusher to `target`.
    pub fn flusher(&self, target: &Arc<Target<SimS3>>) -> ShardFlusher {
        ShardFlusher::spawn(self.shard.clone(), Arc::clone(target))
    }

    /// Commits a PUT of `key` with `body` and returns its `seq`.
    pub async fn put(&self, key: &str, body: &str) -> u64 {
        let committed = self.shard.commit(put(key, body)).await.unwrap();
        committed.position.seq.get()
    }

    /// Commits a DELETE of `key`.
    pub async fn delete(&self, key: &str) -> u64 {
        let body = RecordBody::Delete(Delete {
            key: key.to_owned(),
        });
        self.shard.commit(body).await.unwrap().position.seq.get()
    }

    /// Commits a TAGS of `key`.
    pub async fn tag(&self, key: &str, tags: &[(&str, &str)]) -> u64 {
        let body = RecordBody::Tags(Tags {
            key: key.to_owned(),
            tags: tags
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
        });
        self.shard.commit(body).await.unwrap().position.seq.get()
    }

    /// The entry of `key`.
    pub async fn entry(&self, key: &str) -> Option<Entry> {
        self.shard.entry(key).await.unwrap()
    }

    /// Every entry that is not clean.
    pub async fn unclean(&self) -> Vec<(String, Entry)> {
        let entries = self.shard.entries(None, usize::MAX).await.unwrap();
        entries
            .into_iter()
            .filter(|(_, e)| !matches!(e.state, EntryState::Clean | EntryState::Evicted))
            .collect()
    }

    /// Waits until the flusher has nothing left to do but hold conflicts,
    /// and the index shows only the conflicted keys unclean.
    pub async fn settle(&self, flusher: &ShardFlusher) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3600);
        loop {
            let status = flusher.status();
            let unclean = self.unclean().await;
            if status.dirty == 0 && unclean.len() == status.conflicts.len() {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the flush did not settle: {status:?}, unclean: {unclean:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// The single-member configuration of the test shard.
pub fn config() -> ShardConfig {
    let shard = shard_ref();
    let node: NodeId = "node-1".parse().unwrap();
    ShardConfig {
        bucket_id: shard.bucket.clone(),
        shard: shard.shard,
        epoch: Epoch::new(1),
        primary: node.clone(),
        members: vec![node],
        learners: Vec::new(),
        min_write_replicas: 1,
        replicas: 1,
        proposal_id: ProposalId::new("p-1").unwrap(),
    }
}

/// The MD5 ETag of `body`.
pub fn md5_etag(body: &[u8]) -> ETag {
    use md5::{Digest, Md5};
    let hex: String = Md5::digest(body)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    ETag::new(hex).unwrap()
}

/// A simulated remote that behaves like AWS S3, with versioning if asked.
pub fn remote(seed: u64, versioning: bool) -> SimS3 {
    SimS3::new(
        seed,
        SimS3Config {
            versioning,
            ..SimS3Config::default()
        },
    )
}

/// A target on `store` honoring every precondition.
pub fn target(store: &SimS3) -> Arc<Target<SimS3>> {
    target_with(store, writes(true, true, true))
}

/// A target on `store` honoring `writes`.
pub fn target_with(store: &SimS3, writes: ConditionalWrites) -> Arc<Target<SimS3>> {
    Arc::new(Target::new(
        Arc::new(store.clone()),
        "",
        writes,
        cluster(),
        settings(),
    ))
}

/// The write identity of the record at `seq` of the test shard.
pub fn identity(seq: u64) -> String {
    format!("c-test/b-flush/0/1.{seq}")
}

/// A `PUT` of `key` with an inline `body`, user metadata, and a tag.
pub fn put(key: &str, body: &str) -> RecordBody {
    RecordBody::Put(Put {
        key: key.to_owned(),
        size: body.len() as u64,
        last_modified_ms: 1_700_000_000_000,
        etag: md5_etag(body.as_bytes()),
        inherited_identity: None,
        metadata: BTreeMap::from([
            ("content-type".to_owned(), "text/plain".to_owned()),
            ("cache-control".to_owned(), "no-cache".to_owned()),
            ("x-amz-meta-owner".to_owned(), "team-a".to_owned()),
        ]),
        tags: BTreeMap::from([("kind".to_owned(), "test".to_owned())]),
        checksums: BTreeMap::new(),
        copy_source: None,
        data: PutData::Inline(Bytes::copy_from_slice(body.as_bytes())),
    })
}
