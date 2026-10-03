//! Streaming multipart flush (§7.3) of parts copied with UploadPartCopy,
//! end to end: through the gateway, real shards on a simulated disk, and
//! the flush service. A copied part commits as an `MPU_PART`, as an
//! uploaded one does, so it streams to the remote while the upload is open,
//! and the remote object completes after the local completion with the
//! local ETag.

mod support;

use std::time::Duration;

use bytes::Bytes;
use http::{Method, Request};
use skys3_config::Config;
use skys3_control::{MemoryControlStore, ProposalIds, RetryPolicy, bootstrap};
use skys3_flush::{FlushMetrics, FlushService, FlushSettings};
use skys3_gateway::stub::MemoryShards;
use skys3_gateway::{Gateway, GatewayConfig, IdSource, MODE_HEADER, TARGET_HEADER, TrustAll};
use skys3_io::SimMount;
use skys3_obs::MetricsRegistry;
use skys3_remote::{ListParts, ObjectStore};
use skys3_sim::SimS3;
use skys3_sim::s3::SimS3Config;
use skys3_types::{BucketDocument, RemoteTarget};
use support::{Patience, cluster, runtime, settings};

/// The source object's size: more than the smallest part S3 accepts
/// before the last.
const SOURCE: usize = 6 << 20;

struct World {
    gateway: Gateway<TrustAll>,
    shards: MemoryShards,
    service: FlushService<SimS3, SimMount>,
    store: SimS3,
}

impl World {
    /// A gateway with the `write_back` bucket `photos`, flushed to `store`
    /// with streaming on.
    async fn new(store: SimS3) -> World {
        let config: Config = "[cluster]\ncluster_id = \"c-test\"\n\
             [control_store]\netcd_endpoints = [\"https://etcd.invalid:2379\"]"
            .parse()
            .unwrap();
        let control = MemoryControlStore::new();
        let ids = ProposalIds::seeded(1).next_id();
        bootstrap(&control, &cluster(), ids, &RetryPolicy::default())
            .await
            .unwrap();
        let shards = MemoryShards::new().await;
        let gateway = Gateway::new(
            GatewayConfig::new(&config),
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
        let target = "https://s3.example/remote/team/";
        let mode = [(MODE_HEADER, "write_back"), (TARGET_HEADER, target)];
        let (status, _) = world.call(Method::PUT, "/photos", &mode, "").await;
        assert_eq!(status, 200);
        let buckets = world.gateway.buckets();
        world
            .service
            .reconcile(&buckets, world.shards.local().set())
            .await;
        world
    }

    /// Sends a request and returns its status and body.
    async fn call(
        &self,
        method: Method,
        uri: &str,
        headers: &[(&str, &str)],
        body: impl Into<Bytes>,
    ) -> (u16, String) {
        let mut request = Request::builder().method(method).uri(uri);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        let request = request.body(s3s::Body::from(body.into())).unwrap();
        let response = self.gateway.handle(request).await;
        let status = response.status().as_u16();
        let mut body = response.into_body();
        let body = body.store_all_limited(1 << 20).await.unwrap();
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    fn bucket(&self) -> BucketDocument {
        self.gateway.buckets().pop().unwrap()
    }

    /// The open streams of the bucket's flushers.
    fn streams(&self) -> u64 {
        let status = self.service.status(&self.bucket().bucket_id).unwrap();
        status.shards.iter().map(|(_, s)| s.streams).sum()
    }

    /// The parts the store's open upload of `key` holds: number and size.
    async fn remote_parts(&self, key: &str) -> Vec<(u32, u64)> {
        let uploads = self.store.uploads();
        let Some((id, _)) = uploads.iter().find(|(_, k)| k == key) else {
            return Vec::new();
        };
        let listed = self.store.list_parts(ListParts::new(key, id.clone())).await;
        listed
            .map(|output| {
                output
                    .parts
                    .iter()
                    .map(|p| (p.part_number, p.size))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Waits, letting the flushers run, until `done` holds.
    async fn until(&self, what: &str, done: impl AsyncFn(&World) -> bool) {
        let patience = Patience::new();
        while !done(self).await {
            assert!(!patience.is_exhausted(), "gave up waiting until {what}");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
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

#[test]
fn copied_parts_stream_while_the_upload_is_open() {
    runtime().block_on(async {
        let world = World::new(SimS3::new(41, SimS3Config::default())).await;
        let source: Vec<u8> = (0..SOURCE).map(|i| (i % 251) as u8).collect();
        let (status, _) = world
            .call(Method::PUT, "/photos/src", &[], source.clone())
            .await;
        assert_eq!(status, 200);
        world
            .until("the source is flushed", async |w| {
                w.store.object("team/src").is_some()
            })
            .await;

        // The remote upload opens with the local one.
        let (status, created) = world
            .call(Method::POST, "/photos/dst?uploads", &[], "")
            .await;
        assert_eq!(status, 200, "{created}");
        let id = element(&created, "UploadId").to_owned();
        world
            .until("the remote upload opens", async |w| {
                w.store.uploads().iter().any(|(_, key)| key == "team/dst")
            })
            .await;

        // Part 1 is the whole source, part 2 a range of it.
        let copy = |number: u16, range: Option<&'static str>| {
            let world = &world;
            let id = id.clone();
            async move {
                let mut headers = vec![("x-amz-copy-source", "photos/src")];
                if let Some(range) = range {
                    headers.push(("x-amz-copy-source-range", range));
                }
                let uri = format!("/photos/dst?partNumber={number}&uploadId={id}");
                let (status, body) = world.call(Method::PUT, &uri, &headers, "").await;
                assert_eq!(status, 200, "{body}");
                element(&body, "ETag").to_owned()
            }
        };
        let first = copy(1, None).await;
        let second = copy(2, Some("bytes=10-109")).await;

        // Both stream before the client completes.
        world
            .until("the copied parts reach the remote", async |w| {
                w.remote_parts("team/dst").await == [(1, SOURCE as u64), (2, 100)]
            })
            .await;
        assert!(world.store.object("team/dst").is_none());

        let requests = world.store.stats().requests;
        let complete = format!(
            "<CompleteMultipartUpload>\
             <Part><PartNumber>1</PartNumber><ETag>{first}</ETag></Part>\
             <Part><PartNumber>2</PartNumber><ETag>{second}</ETag></Part>\
             </CompleteMultipartUpload>"
        );
        let uri = format!("/photos/dst?uploadId={id}");
        let (status, completed) = world.call(Method::POST, &uri, &[], complete).await;
        assert_eq!(status, 200, "{completed}");
        let etag = element(&completed, "ETag").replace("&quot;", "\"");
        world
            .until("the object is flushed", async |w| {
                w.store.object("team/dst").is_some() && w.streams() == 0
            })
            .await;

        // The completion sent only the Complete, and the remote object is
        // the local one.
        assert_eq!(world.store.stats().requests - requests, 1);
        let remote = world.store.object("team/dst").unwrap();
        assert_eq!(format!("\"{}\"", remote.info.etag), etag);
        assert!(etag.ends_with("-2\""), "{etag}");
        let mut expected = source.clone();
        expected.extend_from_slice(&source[10..110]);
        assert_eq!(remote.body, expected);
        assert!(world.store.uploads().is_empty());
    });
}
