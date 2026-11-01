//! The admin API's conflict calls (design §7.2, plan M4-06) against a real
//! shard and flush service, whose remote is a simulated store that takes
//! out-of-band writes: listing the keys held in conflict, and resolving
//! them under each policy, which returns them to dirty.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http::{Method, StatusCode};
use http_body_util::BodyExt;
use md5::{Digest, Md5};
use serde_json::Value;
use skys3::admin::{ControlState, NodeAdmin};
use skys3_config::Config;
use skys3_flush::{FlushMetrics, FlushService, FlushSettings, ProbeStatus};
use skys3_gateway::LocalShards;
use skys3_index::{EntryState, ImportCheckpoint, Index, IndexConfig};
use skys3_io::{BlockingPool, MonotonicClock, SimDisk, SimMount};
use skys3_log::record::{Put, PutData};
use skys3_log::{LogConfig, RecordBody, SegmentLog, ShardRef};
use skys3_obs::{AdminApi, Health};
use skys3_remote::{ObjectStore, PutObject, UserMetadata};
use skys3_shard::{Shard, ShardSet};
use skys3_sim::SimS3;
use skys3_sim::s3::SimS3Config;
use skys3_types::{
    BucketDocument, BucketId, BucketMode, BucketName, ClusterId, Epoch, NodeId, ProposalId,
    RemoteTarget, ShardConfig, ShardCount, ShardId,
};

const CONFIG: &str = "[cluster]\ncluster_id = \"c-test\"\n\
    [control_store]\netcd_endpoints = [\"https://etcd.example:2379\"]\n\
    [buckets.photos]\nmode = \"write_back\"\n\
    [buckets.vault]\nmode = \"local\"\nbackup_target = \"https://backup.example/vault\"\n";

fn node_id() -> NodeId {
    NodeId::new("node-1").unwrap()
}

fn bucket(name: &str, mode: BucketMode) -> BucketDocument {
    BucketDocument {
        bucket_id: BucketId::new(format!("b-{name}")).unwrap(),
        name: BucketName::new(name).unwrap(),
        mode,
        shards: ShardCount::new(1).unwrap(),
        replicas: 1,
        min_write_replicas: 1,
        clean_copies: 1,
        target: (mode == BucketMode::WriteBack).then(|| RemoteTarget {
            endpoint: "https://s3.example".to_owned(),
            bucket: "remote".to_owned(),
            prefix: Some("team/".to_owned()),
        }),
        created_unix_ms: 0,
        lifecycle: None,
        proposal_id: ProposalId::new("p-1").unwrap(),
    }
}

/// The single-member configuration of `bucket`'s shard, led by this node.
fn shard_config(bucket: &BucketDocument) -> ShardConfig {
    ShardConfig {
        bucket_id: bucket.bucket_id.clone(),
        shard: ShardId::new(0),
        epoch: Epoch::new(1),
        primary: node_id(),
        members: vec![node_id()],
        learners: Vec::new(),
        min_write_replicas: 1,
        replicas: 1,
        proposal_id: ProposalId::new("p-2").unwrap(),
    }
}

/// A node's shards on a fresh simulated disk.
async fn shard_set() -> ShardSet<SimMount> {
    let mount = SimDisk::new(1).mount();
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
    let index = Index::open_sim(&mount, "index.redb", &index_config).unwrap();
    ShardSet::new(Arc::new(index), log, BlockingPool::inline("index"))
}

/// A `PUT` of `key` with `body`.
fn put(key: &str, body: &str) -> RecordBody {
    let etag = Md5::digest(body.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    RecordBody::Put(Put {
        key: key.to_owned(),
        size: body.len() as u64,
        last_modified_ms: 1_700_000_000_000,
        etag: etag.parse().unwrap(),
        inherited_identity: None,
        metadata: BTreeMap::from([("content-type".to_owned(), "text/plain".to_owned())]),
        tags: BTreeMap::new(),
        checksums: BTreeMap::new(),
        copy_source: None,
        data: PutData::Inline(Bytes::copy_from_slice(body.as_bytes())),
    })
}

/// Writes `body` to `key` at the remote, as another writer would.
async fn out_of_band(store: &SimS3, key: &str, body: &str) {
    let mut metadata = UserMetadata::new();
    metadata.insert("writer", "someone-else").unwrap();
    let request = PutObject::new(format!("team/{key}"), body.to_owned()).with_metadata(metadata);
    store.put_object(request).await.unwrap();
}

/// Calls the admin API and returns the status and the JSON answer.
async fn call(admin: &NodeAdmin<SimMount>, method: Method, path: &str) -> (StatusCode, Value) {
    let Some(response) = admin.call(&method, path).await else {
        return (StatusCode::NOT_FOUND, Value::Null);
    };
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

/// Waits until `check` holds, for at most a minute.
async fn eventually(what: &str, mut check: impl AsyncFnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while !check().await {
        assert!(std::time::Instant::now() < deadline, "{what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The keys the admin API lists as held in conflict.
async fn held(admin: &NodeAdmin<SimMount>) -> Vec<String> {
    let (status, body) = call(admin, Method::GET, "/v1/buckets/photos/conflicts").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body["conflicts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|conflict| conflict["key"].as_str().unwrap().to_owned())
        .collect()
}

/// The body and write identity of the remote object at `key`.
fn remote(store: &SimS3, key: &str) -> Option<(String, Option<String>)> {
    store.object(&format!("team/{key}")).map(|object| {
        let body = String::from_utf8(object.body.to_vec()).unwrap();
        (
            body,
            object.info.metadata.write_identity().map(str::to_owned),
        )
    })
}

#[test]
fn conflicts_are_listed_and_resolved_under_each_policy() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let config: Config = CONFIG.parse().unwrap();
        let store = SimS3::new(
            1,
            SimS3Config {
                min_part_size: 1,
                ..SimS3Config::default()
            },
        );
        let photos = bucket("photos", BucketMode::WriteBack);
        let vault = bucket("vault", BucketMode::Local);
        let set = shard_set().await;
        let shard: Shard<SimMount> = set.open(&shard_config(&photos)).await.unwrap();
        set.open(&shard_config(&vault)).await.unwrap();
        let connected = store.clone();
        let service = FlushService::new(
            ClusterId::new("c-test").unwrap(),
            FlushSettings {
                min_backoff: Duration::from_millis(10),
                max_backoff: Duration::from_millis(100),
                ..FlushSettings::default()
            },
            Box::new(move |_: &RemoteTarget| connected.clone()),
            FlushMetrics::default(),
        )
        .with_buckets(config.buckets().clone());
        let service = Arc::new(service);
        let buckets = vec![photos.clone(), vault.clone()];
        eventually("the probe and the import finish", async || {
            service.reconcile(&buckets, &set).await;
            service.status(&photos.bucket_id).is_some_and(|status| {
                matches!(status.probe, ProbeStatus::Done { .. })
                    && status
                        .import
                        .is_some_and(|import| import.checkpoint == ImportCheckpoint::Done)
            })
        })
        .await;
        // Keys another writer creates once the import has passed them:
        // SkyS3 believes them absent, so their flushes find the conflict.
        let mut seqs = BTreeMap::new();
        for key in ["a/one", "b key", "c"] {
            out_of_band(&store, key, &format!("{key} theirs")).await;
            let committed = shard.commit(put(key, &format!("{key} ours"))).await;
            seqs.insert(key, committed.unwrap().position.seq.get());
        }
        let listed = buckets.clone();
        let admin = NodeAdmin {
            node_id: node_id(),
            cluster_id: ClusterId::new("c-test").unwrap(),
            buckets: Arc::new(move || listed.clone()),
            shards: LocalShards::new(set.clone(), node_id()),
            disks: Vec::new(),
            control: Arc::new(Mutex::new(ControlState::default())),
            health: Health::new(),
            flush: service.clone(),
        };

        // Listing: each key, its shard, its local `seq`, and the remote's
        // write; the bucket's status shows the same, with its policy.
        eventually("every key is held", async || held(&admin).await.len() == 3).await;
        let (_, list) = call(&admin, Method::GET, "/v1/buckets/photos/conflicts").await;
        assert_eq!(list["bucket"], "photos");
        assert_eq!(list["conflict_policy"], "hold");
        let first = &list["conflicts"][0];
        assert_eq!(first["key"], "a/one");
        assert_eq!(first["shard"], 0);
        assert_eq!(first["seq"], seqs["a/one"]);
        assert_eq!(first["remote_identity"], Value::Null);
        assert!(first["remote_etag"].is_string());
        let (_, status) = call(&admin, Method::GET, "/v1/buckets/photos").await;
        assert_eq!(status["flush"]["conflict_policy"], "hold");
        assert_eq!(status["flush"]["conflicts"], list["conflicts"]);
        let (_, vault_list) = call(&admin, Method::GET, "/v1/buckets/vault/conflicts").await;
        assert_eq!(vault_list["conflicts"], Value::Array(Vec::new()));

        // Refusals.
        for (method, path, expected) in [
            (
                Method::POST,
                "/v1/buckets/photos/conflicts/bogus/c",
                StatusCode::BAD_REQUEST,
            ),
            (
                Method::POST,
                "/v1/buckets/photos/conflicts/hold/%zz",
                StatusCode::BAD_REQUEST,
            ),
            (
                Method::POST,
                "/v1/buckets/photos/conflicts/hold/%ff",
                StatusCode::BAD_REQUEST,
            ),
            (
                Method::POST,
                "/v1/buckets/photos/conflicts/hold/",
                StatusCode::BAD_REQUEST,
            ),
            (
                Method::POST,
                "/v1/buckets/photos/conflicts/hold/missing",
                StatusCode::NOT_FOUND,
            ),
            (
                Method::POST,
                "/v1/buckets/photos/conflicts/hold",
                StatusCode::NOT_FOUND,
            ),
            (
                Method::POST,
                "/v1/buckets/other/conflicts/hold/c",
                StatusCode::NOT_FOUND,
            ),
            (
                Method::GET,
                "/v1/buckets/photos/conflicts/hold/c",
                StatusCode::METHOD_NOT_ALLOWED,
            ),
            (
                Method::POST,
                "/v1/buckets/photos/conflicts",
                StatusCode::METHOD_NOT_ALLOWED,
            ),
            (
                Method::GET,
                "/v1/buckets/photos/conflictsx",
                StatusCode::NOT_FOUND,
            ),
            // A backup target never discards.
            (
                Method::POST,
                "/v1/buckets/vault/conflicts/discard_local/c",
                StatusCode::CONFLICT,
            ),
            (
                Method::POST,
                "/v1/buckets/vault/conflicts/overwrite/c",
                StatusCode::NOT_FOUND,
            ),
        ] {
            let (status, body) = call(&admin, method.clone(), path).await;
            assert_eq!(status, expected, "{method} {path}: {body}");
        }
        assert_eq!(held(&admin).await.len(), 3);

        // `overwrite`: the local version replaces the other writer's.
        let (status, body) = call(
            &admin,
            Method::POST,
            "/v1/buckets/photos/conflicts/overwrite/a/one",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["key"], "a/one");
        assert_eq!(body["policy"], "overwrite");
        assert_eq!(body["state"], "dirty");
        let identity = format!("c-test/b-photos/0/1.{}", seqs["a/one"]);
        eventually("the overwrite lands", async || {
            remote(&store, "a/one") == Some(("a/one ours".to_owned(), Some(identity.clone())))
        })
        .await;

        // `discard_local`: the other writer's object is adopted, and the
        // local version dropped. The key is percent-encoded.
        let (status, body) = call(
            &admin,
            Method::POST,
            "/v1/buckets/photos/conflicts/discard_local/b%20key",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["key"], "b key");
        eventually("the discard lands", async || {
            let entry = shard.entry("b key").await.unwrap().unwrap();
            entry.state == EntryState::Evicted
        })
        .await;
        assert_eq!(
            remote(&store, "b key"),
            Some(("b key theirs".to_owned(), None))
        );

        // `hold` retries the conditional flush, which finds the other
        // writer's object again; once it is gone, the retry flushes.
        let (status, _) = call(&admin, Method::POST, "/v1/buckets/photos/conflicts/hold/c").await;
        assert_eq!(status, StatusCode::OK);
        eventually("only c is held", async || held(&admin).await == ["c"]).await;
        store
            .delete_object(skys3_remote::DeleteObject::new("team/c"))
            .await
            .unwrap();
        let (status, _) = call(&admin, Method::POST, "/v1/buckets/photos/conflicts/hold/c").await;
        assert_eq!(status, StatusCode::OK);
        eventually("nothing is held", async || held(&admin).await.is_empty()).await;
        eventually("c is flushed", async || {
            remote(&store, "c").is_some_and(|(body, _)| body == "c ours")
        })
        .await;
        service.shutdown().await;
        let shard_ref = ShardRef::new(photos.bucket_id.clone(), ShardId::new(0));
        assert!(set.get(&shard_ref).await.is_some());
    });
}
