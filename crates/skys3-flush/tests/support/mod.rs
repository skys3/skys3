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
use skys3_log::record::{
    CompletedPart, Delete, MpuComplete, MpuCreate, MpuPart, Put, PutData, Tags, UploadBegin,
};
use skys3_log::{LogConfig, RecordBody, SegmentLog, ShardRef};
use skys3_remote::probe::{ConditionalWrites, OperationSupport, PreconditionSupport};
use skys3_shard::{Shard, ShardSet};
use skys3_sim::SimS3;
use skys3_sim::s3::SimS3Config;
use skys3_types::{
    BucketId, ClusterId, ETag, Epoch, EpochSeq, NodeId, ProposalId, Seq, ShardConfig, ShardId,
};

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
        Self::open_with(seed, BlockingPool::new("index", NonZeroUsize::MIN).unwrap()).await
    }

    /// Opens a node whose index runs on the caller's thread, so that a
    /// paused clock never advances while the index works: simulated
    /// durations then depend on the remote's delays only.
    pub async fn open_inline(seed: u64) -> Node {
        Self::open_with(seed, BlockingPool::inline("index")).await
    }

    async fn open_with(seed: u64, pool: BlockingPool) -> Node {
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

    /// Commits the `UPLOAD_BEGIN` of a streamed PUT of `key` and returns
    /// its `seq`, which the PUT's write identity names (§7.2).
    pub async fn begin(&self, key: &str) -> u64 {
        let body = RecordBody::UploadBegin(UploadBegin {
            key: key.to_owned(),
        });
        let committed = self.shard.commit(body).await.unwrap();
        committed.position.seq.get()
    }

    /// Commits the PUT of `key` with `body` that completes the streamed PUT
    /// begun at `begun`, and returns its `seq`.
    pub async fn complete(&self, key: &str, body: &str, begun: u64) -> u64 {
        let RecordBody::Put(mut streamed) = put(key, body) else {
            unreachable!("put makes a PUT")
        };
        streamed.inherited_identity = Some(EpochSeq::new(Epoch::new(1), Seq::new(begun)));
        let committed = self.shard.commit(RecordBody::Put(streamed)).await;
        committed.unwrap().position.seq.get()
    }

    /// Uploads `parts` of `key` as a multipart upload, with user metadata,
    /// standard headers, and a tag, and completes it.
    pub async fn multipart(&self, key: &str, parts: &[&str]) -> Multipart {
        let create = RecordBody::MpuCreate(MpuCreate {
            key: key.to_owned(),
            initiated_ms: 1_700_000_000_000,
            metadata: BTreeMap::from([
                ("content-type".to_owned(), "video/mp4".to_owned()),
                ("cache-control".to_owned(), "no-cache".to_owned()),
                ("x-amz-meta-owner".to_owned(), "team-b".to_owned()),
            ]),
            tags: BTreeMap::from([("kind".to_owned(), "video".to_owned())]),
            checksum: None,
        });
        let upload = self.shard.commit(create).await.unwrap().position;
        let mut completed = Vec::new();
        for (number, body) in (1..).zip(parts) {
            let part = RecordBody::MpuPart(MpuPart {
                key: key.to_owned(),
                upload,
                part_number: number,
                size: body.len() as u64,
                last_modified_ms: 1_700_000_000_001,
                etag: md5_etag(body.as_bytes()),
                checksums: BTreeMap::new(),
                data: PutData::Inline(Bytes::copy_from_slice(body.as_bytes())),
            });
            let position = self.shard.commit(part).await.unwrap().position;
            completed.push(CompletedPart { number, position });
        }
        let etag = multipart_etag(parts);
        let complete = RecordBody::MpuComplete(MpuComplete {
            key: key.to_owned(),
            upload,
            last_modified_ms: 1_700_000_000_002,
            size: parts.iter().map(|body| body.len() as u64).sum(),
            etag: etag.clone(),
            checksums: BTreeMap::new(),
            parts: completed,
        });
        let committed = self.shard.commit(complete).await.unwrap();
        assert!(committed.outcome.is_applied(), "{:?}", committed.outcome);
        Multipart {
            upload: upload.seq.get(),
            complete: committed.position.seq.get(),
            etag,
        }
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
    /// and the index shows only those unclean.
    pub async fn settle(&self, flusher: &ShardFlusher) {
        let patience = Patience::new();
        loop {
            let status = flusher.status();
            let unclean = self.unclean().await;
            let held = status.conflicts.len();
            if status.dirty == 0 && unclean.len() == held {
                return;
            }
            assert!(
                !patience.is_exhausted(),
                "the flush did not settle: {status:?}, unclean: {unclean:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// How long a test waits for a condition, in real time. The runtime's
/// paused clock jumps ahead whenever the runtime idles, also while the
/// index work runs on real blocking threads, so a bound in virtual time
/// could expire before that work is done.
pub struct Patience(std::time::Instant);

impl Patience {
    /// A minute from now.
    pub fn new() -> Patience {
        Patience(std::time::Instant::now() + Duration::from_secs(60))
    }

    /// Whether the wait should give up.
    pub fn is_exhausted(&self) -> bool {
        std::time::Instant::now() >= self.0
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

/// A multipart object [`Node::multipart`] committed.
pub struct Multipart {
    /// The `seq` of its `MPU_CREATE`, which its write identity names.
    pub upload: u64,
    /// The `seq` of its `MPU_COMPLETE`.
    pub complete: u64,
    /// Its multipart ETag.
    pub etag: ETag,
}

/// The MD5 ETag of `body`.
pub fn md5_etag(body: &[u8]) -> ETag {
    use md5::{Digest, Md5};
    ETag::new(hex(&Md5::digest(body))).unwrap()
}

/// The ETag of a multipart object of `parts`: the MD5 of the parts' MD5
/// digests, then `-` and the number of parts (§7.4).
pub fn multipart_etag(parts: &[&str]) -> ETag {
    use md5::{Digest, Md5};
    let digests: Vec<u8> = parts
        .iter()
        .flat_map(|part| Md5::digest(part.as_bytes()))
        .collect();
    ETag::new(format!("{}-{}", hex(&Md5::digest(digests)), parts.len())).unwrap()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A simulated remote that behaves like AWS S3, with versioning if asked,
/// and any part size.
pub fn remote(seed: u64, versioning: bool) -> SimS3 {
    SimS3::new(
        seed,
        SimS3Config {
            versioning,
            min_part_size: 1,
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
