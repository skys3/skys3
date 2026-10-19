//! Refused flushes (§7.6): a target that answers `403` to the capability
//! probe or to a flush. The keys stay dirty and are retried, each refusal
//! is counted by its code, the bucket's status names the latest, and the
//! refused-keys gauge counts the keys held up until they flush.

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use skys3_flush::{BucketStatus, FlushMetrics, FlushService, ProbeStatus, RefusalCode, Refusals};
use skys3_io::{ManualWallClock, SimMount};
use skys3_obs::MetricsRegistry;
use skys3_remote::{
    AbortMultipartUpload, CompleteMultipartUpload, CopyObject, CreateMultipartUpload, DeleteObject,
    DeleteOutput, GetObject, GetOutput, HeadObject, ListObjectsV2, ListObjectsV2Output, ListParts,
    ListPartsOutput, ObjectInfo, ObjectStore, PutObject, S3Error, S3ErrorKind, S3Result, UploadId,
    UploadPart, WriteOutput,
};
use skys3_sim::SimS3;
use skys3_types::{
    BucketDocument, BucketMode, BucketName, ETag, ProposalId, RemoteTarget, ShardCount,
};
use support::{Node, Patience, cluster, remote, runtime, settings, shard_ref};

/// When the test's wall clock reads.
const NOW: Duration = Duration::from_secs(1_750_000_000);

/// A remote that refuses every write with the error it is given, as a
/// store whose policy or credentials changed does, and reads as usual.
#[derive(Debug, Clone)]
struct Refusing {
    inner: SimS3,
    refusal: Arc<Mutex<Option<S3Error>>>,
    /// Refuses `CopyObject` alone, as a policy without the copy's
    /// permissions does.
    copy_refusal: Arc<Mutex<Option<S3Error>>>,
}

impl Refusing {
    fn new(inner: SimS3) -> Self {
        Self {
            inner,
            refusal: Arc::default(),
            copy_refusal: Arc::default(),
        }
    }

    /// Refuses writes with `403 <code>` from now on, or takes them again.
    fn refuse(&self, code: Option<&str>) {
        *self.refusal.lock().unwrap() = code.map(forbidden);
    }

    /// Refuses `CopyObject` with `403 <code>` from now on, or takes it
    /// again.
    fn refuse_copies(&self, code: Option<&str>) {
        *self.copy_refusal.lock().unwrap() = code.map(forbidden);
    }

    fn admit(&self) -> S3Result<()> {
        self.refusal.lock().unwrap().clone().map_or(Ok(()), Err)
    }
}

impl ObjectStore for Refusing {
    async fn put_object(&self, request: PutObject) -> S3Result<WriteOutput> {
        self.admit()?;
        self.inner.put_object(request).await
    }

    async fn get_object(&self, request: GetObject) -> S3Result<GetOutput> {
        self.inner.get_object(request).await
    }

    async fn head_object(&self, request: HeadObject) -> S3Result<ObjectInfo> {
        self.inner.head_object(request).await
    }

    async fn delete_object(&self, request: DeleteObject) -> S3Result<DeleteOutput> {
        self.admit()?;
        self.inner.delete_object(request).await
    }

    async fn list_objects_v2(&self, request: ListObjectsV2) -> S3Result<ListObjectsV2Output> {
        self.inner.list_objects_v2(request).await
    }

    async fn copy_object(&self, request: CopyObject) -> S3Result<WriteOutput> {
        self.admit()?;
        self.copy_refusal
            .lock()
            .unwrap()
            .clone()
            .map_or(Ok(()), Err)?;
        self.inner.copy_object(request).await
    }

    async fn create_multipart_upload(&self, request: CreateMultipartUpload) -> S3Result<UploadId> {
        self.admit()?;
        self.inner.create_multipart_upload(request).await
    }

    async fn upload_part(&self, request: UploadPart) -> S3Result<ETag> {
        self.admit()?;
        self.inner.upload_part(request).await
    }

    async fn complete_multipart_upload(
        &self,
        request: CompleteMultipartUpload,
    ) -> S3Result<WriteOutput> {
        self.admit()?;
        self.inner.complete_multipart_upload(request).await
    }

    async fn abort_multipart_upload(&self, request: AbortMultipartUpload) -> S3Result<()> {
        self.inner.abort_multipart_upload(request).await
    }

    async fn list_parts(&self, request: ListParts) -> S3Result<ListPartsOutput> {
        self.inner.list_parts(request).await
    }
}

/// A `403` answer with `code`.
fn forbidden(code: &str) -> S3Error {
    S3Error::new(S3ErrorKind::Other, "refused by the test")
        .with_status(403)
        .with_code(code)
}

fn bucket() -> BucketDocument {
    BucketDocument {
        bucket_id: shard_ref().bucket,
        name: BucketName::new("photos").unwrap(),
        mode: BucketMode::WriteBack,
        shards: ShardCount::new(1).unwrap(),
        replicas: 1,
        min_write_replicas: 1,
        clean_copies: 1,
        target: Some(RemoteTarget {
            endpoint: "https://s3.example/".to_owned(),
            bucket: "remote".to_owned(),
            prefix: Some("team/".to_owned()),
        }),
        created_unix_ms: 0,
        lifecycle: None,
        proposal_id: ProposalId::new("p-1").unwrap(),
    }
}

fn service(store: &Refusing, registry: &MetricsRegistry) -> FlushService<Refusing, SimMount> {
    let store = store.clone();
    FlushService::new(
        cluster(),
        settings(),
        Box::new(move |_: &RemoteTarget| store.clone()),
        FlushMetrics::register(registry),
    )
    .with_wall_clock(Arc::new(ManualWallClock::new(NOW)))
}

/// Reconciles until `done` holds for the bucket's status, and returns it.
async fn until(
    service: &FlushService<Refusing, SimMount>,
    node: &Node,
    what: &str,
    done: impl Fn(&BucketStatus) -> bool,
) -> BucketStatus {
    let bucket = bucket();
    let patience = Patience::new();
    while !patience.is_exhausted() {
        service
            .reconcile(std::slice::from_ref(&bucket), &node.set)
            .await;
        let status = service.status(&bucket.bucket_id).unwrap();
        if done(&status) {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("never: {what}");
}

/// The value of the exported series `series`, if there is one.
fn sample(registry: &MetricsRegistry, series: &str) -> Option<f64> {
    let text = registry.encode().unwrap();
    text.lines()
        .find_map(|line| line.strip_prefix(series)?.strip_prefix(' ')?.parse().ok())
}

fn refused_keys(registry: &MetricsRegistry) -> Option<f64> {
    sample(registry, "skys3_flush_refused_keys{bucket=\"photos\"}")
}

fn refusals(registry: &MetricsRegistry, code: &str) -> f64 {
    let series = format!("skys3_flush_refusals_total{{bucket=\"photos\",code=\"{code}\"}}");
    sample(registry, &series).unwrap_or_else(|| panic!("no {series}"))
}

#[test]
fn a_refused_probe_holds_every_dirty_key_until_the_target_takes_writes() {
    runtime().block_on(async {
        let node = Node::open(61).await;
        let store = Refusing::new(remote(61, false));
        store.refuse(Some("AccessDenied"));
        let registry = MetricsRegistry::new();
        let service = service(&store, &registry);
        node.put("cat.jpg", "meow").await;
        node.put("dog.jpg", "woof").await;

        let status = until(&service, &node, "the probe is refused", |status| {
            matches!(status.probe, ProbeStatus::Running { refused: true, .. })
                && status.shards.iter().map(|(_, s)| s.dirty).sum::<u64>() == 2
        })
        .await;
        assert_eq!(status.gauges(NOW).refused_keys, 2);
        let refusal = status.last_refusal.clone().unwrap();
        assert_eq!(refusal.code, RefusalCode::AccessDenied);
        assert_eq!(refusal.target, "https://s3.example/remote");
        assert!(refusal.error.starts_with("probe: "), "{}", refusal.error);
        assert!(
            refusal.error.contains("403 AccessDenied"),
            "{}",
            refusal.error
        );
        assert_eq!(refusal.at, NOW);
        service.refresh_metrics();
        assert_eq!(refused_keys(&registry), Some(2.0));
        assert!(refusals(&registry, "AccessDenied") >= 1.0);
        // Every code has its series, at 0 until counted.
        for code in [
            "InvalidAccessKeyId",
            "SignatureDoesNotMatch",
            "refused",
            "other",
        ] {
            assert_eq!(refusals(&registry, code), 0.0, "{code}");
        }

        // The probe keeps retrying, and passes once the target takes writes
        // again: the keys flush, and the gauge falls back to 0.
        store.refuse(None);
        let status = until(&service, &node, "the keys flush", |status| {
            matches!(status.probe, ProbeStatus::Done { .. })
                && status
                    .shards
                    .iter()
                    .all(|(_, s)| s.dirty == 0 && s.flushing == 0)
        })
        .await;
        assert_eq!(status.gauges(NOW).refused_keys, 0);
        assert!(store.inner.object("team/cat.jpg").is_some());
        assert!(store.inner.object("team/dog.jpg").is_some());
        // The status still names the latest refusal since the node started.
        assert_eq!(status.last_refusal, Some(refusal));
        service.refresh_metrics();
        assert_eq!(refused_keys(&registry), Some(0.0));

        // The bucket's gauge goes with the bucket.
        service.reconcile(&[], &node.set).await;
        service.refresh_metrics();
        assert_eq!(refused_keys(&registry), None);
    });
}

#[test]
fn refused_flushes_are_retried_and_counted_until_they_succeed() {
    runtime().block_on(async {
        let node = Node::open(62).await;
        let store = Refusing::new(remote(62, false));
        let registry = MetricsRegistry::new();
        let service = service(&store, &registry);
        until(&service, &node, "the probe is done", |status| {
            matches!(status.probe, ProbeStatus::Done { .. }) && !status.shards.is_empty()
        })
        .await;
        assert_eq!(
            service.status(&bucket().bucket_id).unwrap().last_refusal,
            None
        );

        // The credentials stop working after the probe passed.
        store.refuse(Some("InvalidAccessKeyId"));
        node.put("cat.jpg", "meow").await;
        let status = until(&service, &node, "the flush is refused", |status| {
            status.shards[0].1.refused == 1
        })
        .await;
        assert_eq!(status.gauges(NOW).refused_keys, 1);
        let shard = &status.shards[0].1;
        assert_eq!(shard.dirty, 1, "a refused key stays dirty");
        assert!(shard.conflicts.is_empty(), "a refusal is not a conflict");
        let refusal = status.last_refusal.clone().unwrap();
        assert_eq!(refusal.code, RefusalCode::InvalidAccessKeyId);
        assert!(refusal.error.starts_with("cat.jpg: "), "{}", refusal.error);
        assert!(
            shard.last_error.as_deref().unwrap().contains("403"),
            "{shard:?}"
        );

        // It is retried with the backoff, and each attempt counts.
        until(&service, &node, "a retry is refused", |_| {
            service.refresh_metrics();
            refusals(&registry, "InvalidAccessKeyId") >= 3.0
        })
        .await;
        assert_eq!(refusals(&registry, "AccessDenied"), 0.0);
        assert!(sample(&registry, "skys3_flush_retries_total{bucket=\"photos\"}").unwrap() >= 3.0);

        // Another error is a failure, not a refusal: the key no longer
        // counts as refused once an attempt ends that way.
        store.inner.set_faults(skys3_sim::s3::SimS3Faults::OUTAGE);
        store.refuse(None);
        until(&service, &node, "the refusal is past", |status| {
            status.shards[0].1.refused == 0 && status.shards[0].1.dirty == 1
        })
        .await;

        store.inner.set_faults(skys3_sim::s3::SimS3Faults::NONE);
        let status = until(&service, &node, "the key flushes", |status| {
            status.shards[0].1.dirty == 0 && status.shards[0].1.flushing == 0
        })
        .await;
        assert_eq!(status.gauges(NOW).refused_keys, 0);
        assert!(store.inner.object("team/cat.jpg").is_some());
        service.refresh_metrics();
        assert_eq!(refused_keys(&registry), Some(0.0));
    });
}

/// A target that refuses only `CopyObject`: the copy probe's refusals are
/// counted and named, but hold up no key, since copies are uploaded until
/// that probe succeeds.
#[test]
fn a_refused_copy_probe_is_counted_but_holds_up_no_key() {
    runtime().block_on(async {
        let node = Node::open(63).await;
        let store = Refusing::new(remote(63, false));
        store.refuse_copies(Some("AccessDenied"));
        let registry = MetricsRegistry::new();
        let service = service(&store, &registry);
        node.put("cat.jpg", "meow").await;

        let status = until(&service, &node, "the copy probe is refused", |status| {
            status
                .last_refusal
                .as_ref()
                .is_some_and(|refusal| refusal.error.starts_with("copy probe: "))
        })
        .await;
        let refusal = status.last_refusal.unwrap();
        assert_eq!(refusal.code, RefusalCode::AccessDenied);
        assert!(refusal.error.contains("CopyObject"), "{}", refusal.error);
        // Flushing goes on meanwhile, without server-side copies.
        let status = until(&service, &node, "the key flushes", |status| {
            store.inner.object("team/cat.jpg").is_some()
                && status
                    .shards
                    .iter()
                    .all(|(_, s)| s.dirty == 0 && s.flushing == 0)
        })
        .await;
        assert_eq!(
            status.probe,
            ProbeStatus::Done {
                unprotected: Vec::new(),
                server_side_copy: false,
            }
        );
        assert_eq!(status.gauges(NOW).refused_keys, 0);
        assert!(store.inner.object("team/cat.jpg").is_some());
        service.refresh_metrics();
        assert!(refusals(&registry, "AccessDenied") >= 1.0);
        assert_eq!(refused_keys(&registry), Some(0.0));

        // Once the target takes copies, the probe passes.
        store.refuse_copies(None);
        until(&service, &node, "the copy probe passes", |status| {
            matches!(
                status.probe,
                ProbeStatus::Done {
                    server_side_copy: true,
                    ..
                }
            )
        })
        .await;
    });
}

#[test]
fn unregistered_refusals_count_and_keep_the_latest() {
    let refusals = Refusals::default();
    refusals.record(RefusalCode::Other, "k: 401".to_owned(), NOW);
    assert_eq!(refusals.count(RefusalCode::Other), 1);
    let latest = refusals.latest().unwrap();
    assert_eq!((latest.code, latest.at), (RefusalCode::Other, NOW));
    assert_eq!(latest.target, "", "the default names no target");
}
