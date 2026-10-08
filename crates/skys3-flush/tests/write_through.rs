//! Write-through buckets (§7.5), end to end: through the gateway, real
//! shards on a simulated disk, and the flush service. Every write that
//! makes a version is answered only once the remote holds it; a write
//! whose flush does not finish in time is answered `503 SlowDown`, one
//! whose key is held in conflict `409 OperationAborted`, and a write whose
//! flusher stops while it waits is answered by the next flusher.

mod support;

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use base64::Engine as _;
use bytes::Bytes;
use http::{Method, Request};
use md5::{Digest as _, Md5};
use skys3_config::Config;
use skys3_control::{MemoryControlStore, ProposalIds, RetryPolicy, bootstrap};
use skys3_flush::{FlushMetrics, FlushService, FlushSettings, ProbeStatus};
use skys3_gateway::stub::MemoryShards;
use skys3_gateway::{Gateway, GatewayConfig, IdSource, MODE_HEADER, TARGET_HEADER, TrustAll};
use skys3_io::SimMount;
use skys3_obs::MetricsRegistry;
use skys3_remote::{ObjectStore, PutObject};
use skys3_sim::SimS3;
use skys3_sim::s3::{SimS3Config, SimS3Faults};
use skys3_types::RemoteTarget;
use support::{Patience, cluster, settings};
use tokio::sync::oneshot;

/// The remote prefix of the bucket `photos`.
const PREFIX: &str = "team/";

struct World {
    gateway: Gateway<TrustAll>,
    shards: MemoryShards,
    service: FlushService<SimS3, SimMount>,
    store: SimS3,
}

impl World {
    /// A gateway with the `write_through` bucket `photos`, flushed to
    /// `store` with streaming on, whose writes wait at most `timeout`.
    async fn new(store: SimS3, timeout: Duration) -> World {
        let config: Config = "[cluster]\ncluster_id = \"c-test\"\n\
             [control_store]\netcd_endpoints = [\"https://etcd.invalid:2379\"]\n\
             [flush]\nack_policy = \"write_through\""
            .parse()
            .unwrap();
        let control = MemoryControlStore::new();
        let ids = ProposalIds::seeded(1).next_id();
        bootstrap(&control, &cluster(), ids, &RetryPolicy::default())
            .await
            .unwrap();
        let shards = MemoryShards::new().await;
        let mut gateway_config = GatewayConfig::new(&config);
        gateway_config.write_through_timeout = timeout;
        // Bodies of 1 KiB and more stream to the remote while they arrive.
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
        );
        let world = World {
            gateway,
            shards,
            service,
            store,
        };
        let target = format!("https://s3.example/remote/{PREFIX}");
        let mode = [
            (MODE_HEADER, "write_back"),
            (TARGET_HEADER, target.as_str()),
        ];
        let (status, _) = world.call(Method::PUT, "/photos", &mode, "").await;
        assert_eq!(status, 200);
        world.reconcile().await;
        world
            .until("the target's probe is done", |w| {
                w.service
                    .status(&w.gateway.buckets()[0].bucket_id)
                    .is_some_and(|status| matches!(status.probe, ProbeStatus::Done { .. }))
            })
            .await;
        world
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
    /// `first` bytes, then waits until the remote's multipart upload of the
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
            self.until("the remote upload of the body is open", |w| {
                w.store.uploads().iter().any(|(_, key)| *key == remote)
            })
            .await;
            let _ = gate.send(());
        };
        tokio::join!(put, open).0
    }

    /// The bytes the remote holds at `key`.
    fn remote(&self, key: &str) -> Option<Vec<u8>> {
        self.store
            .object(&format!("{PREFIX}{key}"))
            .map(|object| object.body.to_vec())
    }

    /// The ETag the remote holds at `key`.
    fn remote_etag(&self, key: &str) -> Option<String> {
        self.store
            .object(&format!("{PREFIX}{key}"))
            .map(|object| object.info.etag.to_string())
    }

    /// Waits, letting the flushers run, until `done` holds.
    async fn until(&self, what: &str, done: impl Fn(&World) -> bool) {
        let patience = Patience::new();
        while !done(self) {
            assert!(!patience.is_exhausted(), "gave up waiting until {what}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
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

/// A store like AWS S3 whose every request takes 5 to 20 ms each way, so
/// that a flush takes longer than a local commit.
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

/// A runtime whose clock runs in real time: a paused clock would jump to
/// the write-through deadline whenever every task waits for the index's
/// pool.
fn real_time() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_time()
        .build()
        .unwrap()
}

/// The text of the first `<tag>` element of `xml`.
fn element<'a>(xml: &'a str, tag: &str) -> &'a str {
    let open = format!("<{tag}>");
    let start = xml
        .find(&open)
        .unwrap_or_else(|| panic!("no {tag} in {xml}"))
        + open.len();
    let end = start + xml[start..].find("</").unwrap();
    &xml[start..end]
}

fn content_md5(body: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(Md5::digest(body.as_bytes()))
}

#[test]
fn every_write_is_answered_once_the_remote_holds_it() {
    real_time().block_on(async {
        let world = World::new(slow_store(5), Duration::from_secs(30)).await;

        // A small PUT, an overwrite, and a streamed one.
        for body in ["first", "second"] {
            let (status, _) = world.call(Method::PUT, "/photos/a", &[], body).await;
            assert_eq!(status, 200);
            assert_eq!(world.remote("a").as_deref(), Some(body.as_bytes()));
        }
        let large: String = (0..3000)
            .map(|i| char::from(b'a' + (i % 26) as u8))
            .collect();
        let (status, _) = world.put_streamed("large", &large, 1536).await;
        assert_eq!(status, 200);
        assert_eq!(world.remote("large").as_deref(), Some(large.as_bytes()));
        // Streamed in 512-byte parts: a multipart ETag at the remote.
        let etag = world.remote_etag("large").unwrap();
        assert!(etag.ends_with("-6"), "{etag}");

        // A copy, and a change of tags.
        let source = [("x-amz-copy-source", "/photos/a")];
        let (status, copied) = world.call(Method::PUT, "/photos/b", &source, "").await;
        assert_eq!(status, 200, "{copied}");
        assert_eq!(world.remote("b").as_deref(), Some(&b"second"[..]));
        let tagging = "<Tagging><TagSet><Tag><Key>team</Key><Value>blue</Value></Tag>\
                       </TagSet></Tagging>";
        let (status, _) = world
            .call(Method::PUT, "/photos/b?tagging", &[], tagging)
            .await;
        assert_eq!(status, 200);
        let tags = world.store.tags(&format!("{PREFIX}b")).unwrap();
        assert_eq!(tags.get("team").map(String::as_str), Some("blue"));

        // A multipart upload: its parts are answered after their local
        // commit, and its completion once the remote object is there.
        let (status, created) = world.call(Method::POST, "/photos/m?uploads", &[], "").await;
        assert_eq!(status, 200, "{created}");
        let id = element(&created, "UploadId").to_owned();
        let (status, _) = world
            .call(
                Method::PUT,
                &format!("/photos/m?partNumber=1&uploadId={id}"),
                &[],
                "part one",
            )
            .await;
        assert_eq!(status, 200);
        let (_, listed) = world
            .call(Method::GET, &format!("/photos/m?uploadId={id}"), &[], "")
            .await;
        let etag = element(&listed, "ETag").replace("&quot;", "\"");
        let complete = format!(
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>\
             <ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
        );
        let (status, completed) = world
            .call(
                Method::POST,
                &format!("/photos/m?uploadId={id}"),
                &[],
                complete,
            )
            .await;
        assert_eq!(status, 200, "{completed}");
        assert_eq!(world.remote("m").as_deref(), Some(&b"part one"[..]));

        // Deletes are answered once the remote no longer holds the key.
        let (status, _) = world.call(Method::DELETE, "/photos/a", &[], "").await;
        assert_eq!(status, 204);
        assert_eq!(world.remote("a"), None);
        let delete = "<Delete><Object><Key>b</Key></Object><Object><Key>m</Key></Object>\
                      </Delete>";
        let md5 = content_md5(delete);
        let (status, deleted) = world
            .call(
                Method::POST,
                "/photos?delete",
                &[("content-md5", md5.as_str())],
                delete,
            )
            .await;
        assert_eq!(status, 200, "{deleted}");
        assert!(!deleted.contains("<Error>"), "{deleted}");
        assert_eq!((world.remote("b"), world.remote("m")), (None, None));
    });
}

#[test]
fn a_write_the_remote_does_not_take_in_time_is_answered_slow_down() {
    real_time().block_on(async {
        let world = World::new(slow_store(6), Duration::from_millis(300)).await;
        let (status, _) = world.call(Method::PUT, "/photos/k", &[], "v1").await;
        assert_eq!(status, 200);

        world.store.set_faults(SimS3Faults::OUTAGE);
        let started = std::time::Instant::now();
        let (status, error) = world.call(Method::PUT, "/photos/k", &[], "v2").await;
        assert_eq!(status, 503, "{error}");
        assert!(error.contains("<Code>SlowDown</Code>"), "{error}");
        assert!(started.elapsed() >= Duration::from_millis(300));
        // The write is committed in the cluster, and the remote gets it
        // once it is back.
        let (status, body) = world.call(Method::GET, "/photos/k", &[], "").await;
        assert_eq!((status, body.as_str()), (200, "v2"));
        assert_eq!(world.remote("k").as_deref(), Some(&b"v1"[..]));
        world.store.set_faults(SimS3Faults::NONE);
        world
            .until("the remote holds v2", |w| {
                w.remote("k").as_deref() == Some(&b"v2"[..])
            })
            .await;
        // Writes are answered at once again.
        let (status, _) = world.call(Method::PUT, "/photos/k", &[], "v3").await;
        assert_eq!(status, 200);
        assert_eq!(world.remote("k").as_deref(), Some(&b"v3"[..]));
    });
}

#[test]
fn a_write_held_in_conflict_is_answered_operation_aborted() {
    real_time().block_on(async {
        let world = World::new(slow_store(7), Duration::from_secs(30)).await;
        let (status, _) = world.call(Method::PUT, "/photos/k", &[], "mine").await;
        assert_eq!(status, 200);
        // Another writer replaces the object at the remote.
        world
            .store
            .put_object(PutObject::new(format!("{PREFIX}k"), "theirs"))
            .await
            .unwrap();

        let (status, error) = world.call(Method::PUT, "/photos/k", &[], "again").await;
        assert_eq!(status, 409, "{error}");
        assert!(error.contains("<Code>OperationAborted</Code>"), "{error}");
        // A later write of the held key gets the same answer at once.
        let (status, _) = world.call(Method::DELETE, "/photos/k", &[], "").await;
        assert_eq!(status, 409);
        assert_eq!(world.remote("k").as_deref(), Some(&b"theirs"[..]));
        // Other keys are not held.
        let (status, _) = world.call(Method::PUT, "/photos/other", &[], "x").await;
        assert_eq!(status, 200);
    });
}

#[test]
fn a_write_waits_through_a_flusher_restart() {
    real_time().block_on(async {
        let world = World::new(slow_store(8), Duration::from_secs(30)).await;
        world.store.set_faults(SimS3Faults::OUTAGE);
        let gateway = world.gateway.clone();
        let put = tokio::spawn(async move {
            call(
                &gateway,
                Method::PUT,
                "/photos/k",
                &[],
                s3s::Body::from(Bytes::from("v1")),
            )
            .await
        });
        // The write is committed, and waits for the remote.
        world
            .until("the write commits", |w| {
                w.service
                    .status(&w.gateway.buckets()[0].bucket_id)
                    .is_some_and(|status| status.shards.iter().any(|(_, s)| s.dirty > 0))
            })
            .await;
        // The bucket's flushers stop, as on a primary that loses its
        // shards; the write's wait ends unanswered, and it asks again.
        world
            .service
            .reconcile(&[], world.shards.local().set())
            .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!put.is_finished());
        world.store.set_faults(SimS3Faults::NONE);
        world.reconcile().await;
        let (status, _) = put.await.unwrap();
        assert_eq!(status, 200);
        assert_eq!(world.remote("k").as_deref(), Some(&b"v1"[..]));
    });
}
