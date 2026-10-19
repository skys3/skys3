//! Backup targets of `local` buckets (§8.9), end to end: through the
//! gateway, real shards on a simulated disk with a clean cache, and the
//! flush service. Every committed change reaches the backup with its write
//! identity, deletes included; nothing the backup holds is evicted
//! locally; and with `backup_ack = "write_through"` a write is answered
//! only once the backup holds it.

mod support;

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http::{Method, Request};
use skys3_config::Config;
use skys3_control::{MemoryControlStore, ProposalIds, RetryPolicy, bootstrap};
use skys3_flush::{FlushMetrics, FlushService, FlushSettings, ProbeStatus};
use skys3_gateway::stub::MemoryShards;
use skys3_gateway::{Gateway, GatewayConfig, IdSource, MODE_HEADER, ShardRef, TrustAll};
use skys3_index::EntryState;
use skys3_io::SimMount;
use skys3_obs::MetricsRegistry;
use skys3_remote::{ObjectStore, PutObject};
use skys3_shard::{CacheMetrics, CacheSettings, CleanCache};
use skys3_sim::SimS3;
use skys3_sim::s3::{SimS3Config, SimS3Faults};
use skys3_types::{BucketDocument, RemoteTarget};
use support::{Patience, cluster, settings};
use tokio::sync::oneshot;

/// The key prefix of the backup target of the bucket `photos`.
const PREFIX: &str = "backup/photos/";

struct World {
    gateway: Gateway<TrustAll>,
    shards: MemoryShards,
    service: FlushService<SimS3, SimMount>,
    store: SimS3,
    /// A clean cache with no room, told nothing of the `local` bucket, as
    /// a node tells its cache only of the buckets it evicts.
    cache: CleanCache,
}

impl World {
    /// A gateway with the `local` bucket `photos`, whose backup target in
    /// `store` acknowledges by `ack`, and whose writes wait at most
    /// `timeout` for it.
    async fn new(store: SimS3, ack: &str, timeout: Duration) -> World {
        let config: Config = format!(
            "[cluster]\ncluster_id = \"c-test\"\n\
             [control_store]\netcd_endpoints = [\"https://etcd.invalid:2379\"]\n\
             [buckets.photos]\nmode = \"local\"\n\
             backup_target = \"https://s3.example/remote/{PREFIX}\"\n\
             backup_ack = \"{ack}\"\n\
             [buckets.plain]\nmode = \"local\"\n"
        )
        .parse()
        .unwrap();
        let control = MemoryControlStore::new();
        let ids = ProposalIds::seeded(1).next_id();
        bootstrap(&control, &cluster(), ids, &RetryPolicy::default())
            .await
            .unwrap();
        let shards = MemoryShards::new().await;
        let cache = CleanCache::new(
            CacheSettings {
                max_bytes: 0,
                reserve_fraction: 0.0,
            },
            CacheMetrics::default(),
        );
        shards.local().set().use_cache(&cache).await;
        let mut gateway_config = GatewayConfig::new(&config);
        gateway_config.write_through_timeout = timeout;
        // Bodies of 1 KiB and more stream to the backup while they arrive.
        gateway_config.streaming_flush_min_bytes = Some(1024);
        gateway_config.flush_part_bytes = 512;
        gateway_config.inline_max_bytes = 256;
        gateway_config.extent_bytes = 256;
        let gateway = Gateway::new(
            gateway_config,
            control,
            shards.clone(),
            IdSource::seeded(3),
            TrustAll,
        )
        .await
        .unwrap();
        let connect = {
            let store = store.clone();
            move |_: &RemoteTarget| store.clone()
        };
        let settings = FlushSettings {
            streaming: true,
            part_bytes: 512,
            ..settings()
        };
        let service = FlushService::new(
            cluster(),
            settings,
            Box::new(connect),
            FlushMetrics::register(&MetricsRegistry::new()),
        )
        .with_buckets(config.buckets().clone());
        let world = World {
            gateway,
            shards,
            service,
            store,
            cache,
        };
        for bucket in ["/photos", "/plain"] {
            let (status, _) = world
                .call(Method::PUT, bucket, &[(MODE_HEADER, "local")], "")
                .await;
            assert_eq!(status, 200);
        }
        world.reconcile().await;
        world
            .until("the backup's probe is done", |w| {
                w.service
                    .status(&w.photos().bucket_id)
                    .is_some_and(|status| matches!(status.probe, ProbeStatus::Done { .. }))
            })
            .await;
        world
    }

    fn photos(&self) -> BucketDocument {
        self.bucket("photos")
    }

    fn bucket(&self, name: &str) -> BucketDocument {
        self.gateway
            .buckets()
            .into_iter()
            .find(|bucket| bucket.name.as_str() == name)
            .unwrap()
    }

    /// Starts the flushers of the bucket's shards, as the node does every
    /// second.
    async fn reconcile(&self) {
        let buckets = self.gateway.buckets();
        self.service
            .reconcile(&buckets, self.shards.local().set())
            .await;
    }

    /// Sends a request and returns its status and body.
    async fn call(
        &self,
        method: Method,
        uri: &str,
        headers: &[(&str, &str)],
        body: impl Into<Bytes>,
    ) -> (u16, String) {
        let body = s3s::Body::from(body.into());
        call(&self.gateway, method, uri, headers, body).await
    }

    /// PUTs `body` at `key` of `photos` as a client that sends its first
    /// `first` bytes, then waits until the backup's multipart upload of the
    /// key is open before it sends the rest, so that the body streams.
    ///
    /// Otherwise the `PUT` may commit before the flusher's open of the
    /// remote upload starts, as on a busy runner, and the body is then
    /// rightly sent as one `PutObject` under the same write identity.
    async fn put_streamed(&self, key: &str, body: &str, first: usize) -> (u16, String) {
        let (gate, opened) = oneshot::channel();
        let gated = Gated {
            first: Some(Bytes::copy_from_slice(&body.as_bytes()[..first])),
            opened: Some(opened),
            rest: Some(Bytes::copy_from_slice(&body.as_bytes()[first..])),
        };
        let uri = format!("/photos/{key}");
        let put = call(
            &self.gateway,
            Method::PUT,
            &uri,
            &[],
            s3s::Body::http_body(gated),
        );
        let remote = format!("{PREFIX}{key}");
        let open = async {
            self.until("the backup's upload of the body is open", |w| {
                w.store.uploads().iter().any(|(_, key)| *key == remote)
            })
            .await;
            let _ = gate.send(());
        };
        tokio::join!(put, open).0
    }

    /// The bytes the backup holds at `key`.
    fn backup(&self, key: &str) -> Option<Vec<u8>> {
        self.store
            .object(&format!("{PREFIX}{key}"))
            .map(|object| object.body.to_vec())
    }

    /// The state of `key`'s entry in `bucket`, if it has one.
    async fn state(&self, bucket: &BucketDocument, key: &str) -> Option<EntryState> {
        let shard = ShardRef::for_key(bucket, key);
        let replica = self.shards.local().set().get(&(&shard).into()).await?;
        replica.entry(key).await.unwrap().map(|entry| entry.state)
    }

    /// Waits, letting the flushers run, until `done` holds.
    async fn until(&self, what: &str, done: impl Fn(&World) -> bool) {
        let patience = Patience::new();
        while !done(self) {
            assert!(!patience.is_exhausted(), "gave up waiting until {what}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Waits until the backup holds `body` at `key`, or nothing.
    async fn until_backup(&self, key: &str, body: Option<&str>) {
        let what = format!("the backup holds {body:?} at {key}");
        self.until(&what, |w| {
            w.backup(key).as_deref() == body.map(str::as_bytes)
        })
        .await;
    }
}

async fn call(
    gateway: &Gateway<TrustAll>,
    method: Method,
    uri: &str,
    headers: &[(&str, &str)],
    body: s3s::Body,
) -> (u16, String) {
    let mut request = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let request = request.body(body).unwrap();
    let response = gateway.handle(request).await;
    let status = response.status().as_u16();
    let mut body = response.into_body();
    let body = body.store_all_limited(64 << 20).await.unwrap();
    (status, String::from_utf8_lossy(&body).into_owned())
}

/// A request body that sends `first`, then waits until `opened` fires or
/// is dropped before it sends `rest`.
struct Gated {
    first: Option<Bytes>,
    opened: Option<oneshot::Receiver<()>>,
    rest: Option<Bytes>,
}

impl http_body::Body for Gated {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, std::io::Error>>> {
        if let Some(first) = self.first.take() {
            return Poll::Ready(Some(Ok(http_body::Frame::data(first))));
        }
        if let Some(opened) = &mut self.opened {
            if Pin::new(opened).poll(cx).is_pending() {
                return Poll::Pending;
            }
            self.opened = None;
        }
        Poll::Ready(
            self.rest
                .take()
                .map(|rest| Ok(http_body::Frame::data(rest))),
        )
    }
}

/// A store whose every request takes 5 to 20 ms each way, so that a flush
/// takes longer than a local commit.
fn slow_store(seed: u64) -> SimS3 {
    let store = SimS3::new(
        seed,
        SimS3Config {
            min_part_size: 1,
            ..SimS3Config::default()
        },
    );
    store.set_faults(SimS3Faults {
        min_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(20),
        ..SimS3Faults::NONE
    });
    store
}

/// A runtime whose clock runs in real time: the shards' index pool runs
/// on threads of its own, which a paused clock does not wait for.
fn real_time() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_time()
        .build()
        .unwrap()
}

fn large_body() -> String {
    (0..3000)
        .map(|i| char::from(b'a' + (i % 26) as u8))
        .collect()
}

#[test]
fn every_change_reaches_the_backup_and_nothing_is_evicted() {
    real_time().block_on(async {
        let world = World::new(slow_store(11), "local", Duration::from_secs(30)).await;
        let photos = world.photos();
        let status = world.service.status(&photos.bucket_id).unwrap();
        assert!(status.backup);
        assert_eq!(status.import, None, "a backup imports nothing");
        assert_eq!(status.dirty_budget, u64::MAX, "and counts no budget");
        assert!(world.service.filler(&photos.bucket_id).is_none());
        assert!(world.service.remote(&photos.bucket_id).is_none());
        assert!(world.service.target_of(&photos).is_some());
        // A local bucket without a backup target is not flushed.
        let plain = world.bucket("plain");
        assert!(world.service.status(&plain.bucket_id).is_none());
        assert!(world.service.target_of(&plain).is_none());

        // A PUT, an overwrite, and a streamed PUT.
        for body in ["first", "second"] {
            let (status, _) = world.call(Method::PUT, "/photos/a", &[], body).await;
            assert_eq!(status, 200);
        }
        let large = large_body();
        let (status, _) = world.put_streamed("large", &large, 1536).await;
        assert_eq!(status, 200);
        world.until_backup("a", Some("second")).await;
        world.until_backup("large", Some(&large)).await;
        // Streamed in 512-byte parts, with the write identity of its
        // `UPLOAD_BEGIN`.
        let object = world.store.object(&format!("{PREFIX}large")).unwrap();
        assert!(object.info.etag.as_str().ends_with("-6"), "{object:?}");
        let identity = object.info.metadata.get("skys3-wid").unwrap();
        let prefix = format!("c-test/{}/", photos.bucket_id);
        assert!(identity.starts_with(&prefix), "{identity}");

        // A copy and a change of tags.
        let source = [("x-amz-copy-source", "/photos/a")];
        let (status, copied) = world.call(Method::PUT, "/photos/b", &source, "").await;
        assert_eq!(status, 200, "{copied}");
        let tagging = "<Tagging><TagSet><Tag><Key>team</Key><Value>blue</Value></Tag>\
                       </TagSet></Tagging>";
        let (status, _) = world
            .call(Method::PUT, "/photos/b?tagging", &[], tagging)
            .await;
        assert_eq!(status, 200);
        world
            .until("the backup has the tags", |w| {
                w.store
                    .tags(&format!("{PREFIX}b"))
                    .is_some_and(|tags| tags.get("team").map(String::as_str) == Some("blue"))
            })
            .await;

        // Backed up, the entries are clean, and the cache, which has no
        // room, evicts none of them: a local bucket's replicas are its
        // durable home.
        for key in ["a", "large", "b"] {
            let patience = Patience::new();
            while world.state(&photos, key).await != Some(EntryState::Clean) {
                assert!(!patience.is_exhausted(), "{key} never became clean");
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        assert_eq!(world.cache.reclaim(world.shards.local().set()).await, 0);
        assert_eq!(world.cache.usage().bytes, 0);
        let (status, body) = world.call(Method::GET, "/photos/large", &[], "").await;
        assert_eq!((status, body), (200, large));

        // A delete keeps its tombstone until the backup deleted the key.
        world.store.set_faults(SimS3Faults::OUTAGE);
        let (status, _) = world.call(Method::DELETE, "/photos/a", &[], "").await;
        assert_eq!(status, 204);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(world.state(&photos, "a").await, Some(EntryState::Dirty));
        assert_eq!(world.backup("a").as_deref(), Some(&b"second"[..]));
        world.store.set_faults(SimS3Faults::NONE);
        world.until_backup("a", None).await;
        let patience = Patience::new();
        while world.state(&photos, "a").await.is_some() {
            assert!(!patience.is_exhausted(), "the tombstone stayed");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // In a local bucket without a backup, the tombstone goes at once.
        let (status, _) = world.call(Method::PUT, "/plain/k", &[], "v").await;
        assert_eq!(status, 200);
        let (status, _) = world.call(Method::DELETE, "/plain/k", &[], "").await;
        assert_eq!(status, 204);
        let patience = Patience::new();
        while world.state(&plain, "k").await.is_some() {
            assert!(!patience.is_exhausted(), "the tombstone stayed");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
}

#[test]
fn a_bucket_is_detached_only_once_its_deletes_reached_the_backup() {
    real_time().block_on(async {
        let world = World::new(slow_store(12), "local", Duration::from_secs(30)).await;
        let (status, _) = world.call(Method::PUT, "/photos/k", &[], "v").await;
        assert_eq!(status, 200);
        world.until_backup("k", Some("v")).await;
        world.store.set_faults(SimS3Faults::OUTAGE);
        let (status, _) = world.call(Method::DELETE, "/photos/k", &[], "").await;
        assert_eq!(status, 204);
        // No object is left, but the delete exists only here.
        let (status, error) = world.call(Method::DELETE, "/photos", &[], "").await;
        assert_eq!(status, 409, "{error}");
        assert!(error.contains("BucketNotEmpty"), "{error}");
        assert!(error.contains("1 deletes"), "{error}");
        world.store.set_faults(SimS3Faults::NONE);
        world.until_backup("k", None).await;
        let patience = Patience::new();
        loop {
            let (status, _) = world.call(Method::DELETE, "/photos", &[], "").await;
            if status == 204 {
                break;
            }
            assert!(!patience.is_exhausted(), "the bucket was never detached");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
}

#[test]
fn with_write_through_a_write_is_answered_once_the_backup_holds_it() {
    real_time().block_on(async {
        let world = World::new(slow_store(13), "write_through", Duration::from_secs(2)).await;
        let (status, _) = world.call(Method::PUT, "/photos/k", &[], "v1").await;
        assert_eq!(status, 200);
        assert_eq!(world.backup("k").as_deref(), Some(&b"v1"[..]));
        let large = large_body();
        let (status, _) = world
            .call(Method::PUT, "/photos/large", &[], large.clone())
            .await;
        assert_eq!(status, 200);
        assert_eq!(world.backup("large"), Some(large.into_bytes()));
        let (status, _) = world.call(Method::DELETE, "/photos/large", &[], "").await;
        assert_eq!(status, 204);
        assert_eq!(world.backup("large"), None);

        // An outage: the write is committed, answered `503 SlowDown`, and
        // reaches the backup once it is back.
        world.store.set_faults(SimS3Faults::OUTAGE);
        let (status, error) = world.call(Method::PUT, "/photos/k", &[], "v2").await;
        assert_eq!(status, 503, "{error}");
        assert!(error.contains("<Code>SlowDown</Code>"), "{error}");
        let (status, body) = world.call(Method::GET, "/photos/k", &[], "").await;
        assert_eq!((status, body.as_str()), (200, "v2"));
        world.store.set_faults(SimS3Faults::NONE);
        world.until_backup("k", Some("v2")).await;

        // Another writer at the backup holds the key in conflict.
        world
            .store
            .put_object(PutObject::new(format!("{PREFIX}k"), "theirs"))
            .await
            .unwrap();
        let (status, error) = world.call(Method::PUT, "/photos/k", &[], "v3").await;
        assert_eq!(status, 409, "{error}");
        assert!(error.contains("<Code>OperationAborted</Code>"), "{error}");
        let (status, _) = world.call(Method::PUT, "/photos/other", &[], "x").await;
        assert_eq!(status, 200);
        // A local bucket without a backup answers at once.
        let (status, _) = world.call(Method::PUT, "/plain/k", &[], "v").await;
        assert_eq!(status, 200);
    });
}
