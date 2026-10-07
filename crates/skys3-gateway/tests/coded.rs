//! GETs and copies of coded objects (§8.5): once a version's `EC_PUBLISH`
//! commits, the gateway reads its bytes from the fragments the entry's
//! coded layout names, decoding a stripe that lost a fragment, and
//! CopyObject copies a coded source from its fragments too, never from the
//! replicas, which drop their copies once the object is coded.

mod common;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use common::signing::request as with_body;
use common::{Answer, Setup, config, setup_with};
use http::Method;
use skys3_ec::{
    CodecId, CodedStripe, FragmentBytes, FragmentId, FragmentLocation, FragmentReadError,
    FragmentRequest, FragmentSource, Geometry, current_codec,
};
use skys3_gateway::{GatewayConfig, Precondition, ShardRef, Shards};
use skys3_log::RecordBody;
use skys3_log::record::EcPublish;
use skys3_types::{AttemptId, BucketDocument, NodeId};

/// The object's size, and its stripes' data length: three stripes, the
/// last one shorter.
const SIZE: usize = 5_000;
const STRIPE_LEN: usize = 2_000;

/// Fragments held in memory by node and ID, with every request recorded.
#[derive(Debug, Default)]
struct Fragments {
    held: Mutex<BTreeMap<(NodeId, FragmentId), Bytes>>,
    requests: Mutex<Vec<FragmentRequest>>,
}

impl Fragments {
    fn requests(&self) -> Vec<FragmentRequest> {
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Loses fragment `index` of every stripe of `publish`.
    fn lose(&self, publish: &EcPublish, index: usize) {
        let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        for stripe in &publish.stripes {
            let location = &stripe.fragments()[index];
            held.remove(&(location.node.clone(), location.fragment));
        }
    }
}

impl FragmentSource for Fragments {
    fn read(&self, request: FragmentRequest) -> skys3_ec::read::ReadFuture<'_> {
        Box::pin(async move {
            self.requests
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(request.clone());
            let held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
            let Some(fragment) = held.get(&(request.node.clone(), request.fragment)) else {
                return Err(FragmentReadError::NotHeld {
                    node: request.node,
                    reason: "no such fragment".to_owned(),
                });
            };
            let data = fragment.slice(request.range.start as usize..request.range.end as usize);
            let crc32c = crc32c::crc32c(&data);
            Ok(FragmentBytes { data, crc32c })
        })
    }
}

/// `len` bytes of lowercase letters.
fn letters(len: usize) -> Bytes {
    (0..len).map(|i| b'a' + (i * 7 % 26) as u8).collect()
}

/// A gateway that reads fragments from `fragments`, if given.
async fn gateway(fragments: Option<&Arc<Fragments>>) -> Setup {
    let mut config: GatewayConfig = config("");
    config.inline_max_bytes = 1024;
    config.extent_bytes = 1000;
    config.fragments = fragments.map(|f| Arc::clone(f) as Arc<dyn FragmentSource>);
    setup_with(config).await
}

impl Setup {
    async fn put(&self, uri: &str, body: Bytes) -> Answer {
        self.send(with_body(Method::PUT, uri, &[], body)).await
    }

    async fn copy(&self, to: &str, from: &str) -> Answer {
        self.call(Method::PUT, to, &[("x-amz-copy-source", from)], "")
            .await
    }

    /// Codes `data`, the current version of `key`, as 2+1 stripes held by
    /// `fragments`, and commits its `EC_PUBLISH`.
    async fn code(
        &self,
        bucket: &BucketDocument,
        key: &str,
        data: &[u8],
        fragments: &Fragments,
    ) -> EcPublish {
        let shard = ShardRef::for_key(bucket, key);
        let entry = self.shards.entry(&shard, key).await.unwrap().unwrap();
        let object = entry.object.unwrap();
        assert_eq!(object.size, data.len() as u64);
        let geometry = Geometry::new(2, 1).unwrap();
        let chunks: Vec<&[u8]> = data.chunks(STRIPE_LEN).collect();
        let mut stripes = Vec::new();
        let mut next = 1u128;
        for (number, chunk) in chunks.iter().enumerate() {
            let encoded = current_codec().encode(geometry, chunk).unwrap();
            let mut locations = Vec::new();
            for (index, fragment) in encoded.into_iter().enumerate() {
                let node: NodeId = format!("n{index}").parse().unwrap();
                let id = FragmentId::new(next);
                next += 1;
                fragments
                    .held
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert((node.clone(), id), fragment.into());
                locations.push(FragmentLocation { node, fragment: id });
            }
            let stripe = CodedStripe::new(
                number as u32,
                (number * STRIPE_LEN) as u64,
                chunk.len() as u64,
                geometry,
                CodecId::CURRENT,
                locations,
            )
            .unwrap();
            stripes.push(stripe);
        }
        let publish = EcPublish {
            key: key.to_owned(),
            version: entry.version,
            etag: object.local_etag,
            attempt: AttemptId::new(entry.version.epoch, 1),
            size: object.size,
            stripes,
        };
        self.shards
            .write(
                &shard,
                RecordBody::EcPublish(publish.clone()),
                Precondition::None,
            )
            .await
            .unwrap()
            .unwrap();
        let coded = self.shards.entry(&shard, key).await.unwrap().unwrap();
        assert!(coded.object.unwrap().coded.is_some(), "the publish applied");
        publish
    }
}

#[tokio::test]
async fn gets_and_copies_of_a_coded_object_read_its_fragments() {
    let fragments = Arc::new(Fragments::default());
    let setup = gateway(Some(&fragments)).await;
    let bucket = setup.create_local("coded").await;
    let data = letters(SIZE);
    setup
        .put("/coded/src", data.clone())
        .await
        .assert(200, None);
    let publish = setup.code(&bucket, "src", &data, &fragments).await;
    assert!(fragments.requests().is_empty(), "nothing was coded yet");

    // A range reads the data fragments that cover it.
    let got = setup
        .call(
            Method::GET,
            "/coded/src",
            &[("range", "bytes=1900-4199")],
            "",
        )
        .await;
    got.assert(206, None);
    assert_eq!(got.body.as_bytes(), &data[1900..4200]);
    let read = fragments.requests();
    assert!(!read.is_empty(), "the range came from the fragments");
    assert!(
        read.iter().all(|r| r.identity.index < 2),
        "data fragments only"
    );

    // With fragment 0 of every stripe lost, the copy decodes each stripe
    // from the other two, and the copy holds the source's bytes and ETag.
    fragments.lose(&publish, 0);
    let before = fragments.requests().len();
    let copied = setup.copy("/coded/copy", "coded/src").await;
    copied.assert(200, None);
    let requests = fragments.requests();
    let copy_reads = &requests[before..];
    assert!(
        copy_reads.iter().any(|r| r.identity.index == 2),
        "the copy decoded from parity: {copy_reads:?}"
    );
    assert!(
        copy_reads
            .iter()
            .all(|r| r.identity.key == "src" && r.identity.version == publish.version),
        "the copy names the source's version"
    );
    let source_etag = format!("\"{}\"", publish.etag.as_str());
    assert!(copied.body.contains(&source_etag), "{copied:?}");

    // The copy is a new, replicated object with the source's bytes.
    let shard = ShardRef::for_key(&bucket, "copy");
    let entry = setup.shards.entry(&shard, "copy").await.unwrap().unwrap();
    assert!(entry.object.unwrap().coded.is_none());
    let reads = fragments.requests().len();
    let got = setup.call(Method::GET, "/coded/copy", &[], "").await;
    got.assert(200, None);
    assert_eq!(got.body.as_bytes(), &data[..]);
    assert_eq!(fragments.requests().len(), reads, "the copy is not coded");

    // A whole GET of the source decodes too.
    let reads = fragments.requests().len();
    let got = setup.call(Method::GET, "/coded/src", &[], "").await;
    got.assert(200, None);
    assert_eq!(got.body.as_bytes(), &data[..]);
    assert!(fragments.requests().len() > reads, "the GET read fragments");
}

#[tokio::test]
async fn a_coded_source_that_cannot_be_read_answers_503() {
    // A gateway that reads no fragments cannot copy a coded source, even
    // though this shard still holds the replicated bytes: it never reads
    // them, as a member that dropped them could not serve them.
    let fragments = Arc::new(Fragments::default());
    let blind = gateway(None).await;
    let bucket = blind.create_local("coded").await;
    blind
        .put("/coded/src", letters(SIZE))
        .await
        .assert(200, None);
    blind.code(&bucket, "src", &letters(SIZE), &fragments).await;
    blind
        .copy("/coded/copy", "coded/src")
        .await
        .assert(503, Some("ServiceUnavailable"));
    blind
        .call(Method::GET, "/coded/src", &[], "")
        .await
        .assert(503, Some("ServiceUnavailable"));

    // A stripe that lost more than `m` fragments cannot be decoded.
    let fragments = Arc::new(Fragments::default());
    let setup = gateway(Some(&fragments)).await;
    let bucket = setup.create_local("coded").await;
    setup
        .put("/coded/src", letters(SIZE))
        .await
        .assert(200, None);
    let publish = setup.code(&bucket, "src", &letters(SIZE), &fragments).await;
    fragments.lose(&publish, 0);
    fragments.lose(&publish, 1);
    let failed = setup.copy("/coded/copy", "coded/src").await;
    failed.assert(503, Some("ServiceUnavailable"));
    assert!(
        failed.body.contains("fragments could not be read"),
        "{failed:?}"
    );
    let shard = ShardRef::for_key(&bucket, "copy");
    assert!(setup.shards.entry(&shard, "copy").await.unwrap().is_none());
}
