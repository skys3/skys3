//! The capability probe against simulated stores configured like AWS S3,
//! like Cloudflare R2, and like providers that reject or ignore every
//! precondition (design §7.2).
//!
//! These are integration tests because `skys3-sim` depends on this crate: a
//! unit test would see two copies of the `ObjectStore` trait.

use std::sync::Mutex;
use std::time::Duration;

use skys3_remote::probe::{
    ConditionalOperation, ConditionalProbe, ConditionalWrites, OperationSupport,
    PreconditionSupport,
};
use skys3_remote::{
    AbortMultipartUpload, CompleteMultipartUpload, CopyObject, CreateMultipartUpload, DeleteObject,
    DeleteOutput, GetObject, GetOutput, HeadObject, ListObjectsV2, ListObjectsV2Output, ListParts,
    ListPartsOutput, ObjectInfo, ObjectStore, PutObject, S3Error, S3ErrorKind, S3Result, UploadId,
    UploadPart, VersionId, WriteOutput,
};
use skys3_sim::s3::{ConditionalSupport, Conditionals, Fault, Operation, SimS3, SimS3Config};
use skys3_types::ETag;

use ConditionalOperation::{CompleteMultipartUpload as Complete, DeleteObject as Delete};
use PreconditionSupport::{Honored, Ignored, Rejected};

const PREFIX: &str = "tenant/";

fn store(conditionals: Conditionals, versioning: bool) -> SimS3 {
    SimS3::new(
        7,
        SimS3Config {
            versioning,
            conditionals,
            ..SimS3Config::default()
        },
    )
}

fn probe() -> ConditionalProbe {
    ConditionalProbe::new(PREFIX, 0xfeed)
}

fn uniform(support: PreconditionSupport) -> ConditionalWrites {
    let both = OperationSupport {
        if_none_match: Some(support),
        if_match: support,
    };
    ConditionalWrites {
        put_object: both,
        complete_multipart_upload: both,
        delete_object: OperationSupport {
            if_none_match: None,
            if_match: support,
        },
    }
}

/// Checks that the probe left nothing in `store`: no object, no version or
/// delete marker, and no upload.
fn assert_clean(store: &SimS3) {
    assert_eq!(store.keys(), Vec::<String>::new());
    assert_eq!(store.versions(), Vec::new());
    assert_eq!(store.uploads(), Vec::new());
}

/// A scratch key of [`probe`].
fn scratch(name: &str) -> String {
    format!("tenant/.skys3-probe/000000000000feed/{name}")
}

#[tokio::test]
async fn aws_s3_protects_every_operation() {
    let store = store(Conditionals::AWS_S3, false);
    let writes = probe().run(&store).await.unwrap();
    assert_eq!(writes, uniform(Honored));
    assert!(writes.unprotected().is_empty());
    assert_clean(&store);
}

#[tokio::test]
async fn r2_leaves_complete_and_delete_unprotected() {
    let store = store(Conditionals::R2, false);
    let writes = probe().run(&store).await.unwrap();
    assert_eq!(writes.put_object, uniform(Honored).put_object);
    assert_eq!(
        writes.complete_multipart_upload,
        uniform(Ignored).complete_multipart_upload
    );
    assert_eq!(writes.delete_object, uniform(Ignored).delete_object);
    assert_eq!(writes.unprotected(), [Complete, Delete]);
    assert_eq!(
        writes.to_string(),
        "PutObject: If-None-Match honored, If-Match honored; \
         CompleteMultipartUpload: If-None-Match ignored, If-Match ignored; \
         DeleteObject: If-Match ignored"
    );
    assert_clean(&store);
}

#[tokio::test]
async fn rejected_and_ignored_headers_are_told_apart() {
    for (support, expected) in [
        (ConditionalSupport::Rejected, Rejected),
        (ConditionalSupport::Ignored, Ignored),
    ] {
        let store = store(Conditionals::all(support), false);
        let writes = probe().run(&store).await.unwrap();
        assert_eq!(writes, uniform(expected), "{support:?}");
        assert_eq!(writes.unprotected(), ConditionalOperation::ALL);
        assert_clean(&store);
    }
}

#[tokio::test]
async fn operations_are_probed_independently() {
    let conditionals = Conditionals {
        put_object: ConditionalSupport::Rejected,
        complete_multipart_upload: ConditionalSupport::Honored,
        delete_object: ConditionalSupport::Rejected,
        ..Conditionals::AWS_S3
    };
    let store = store(conditionals, false);
    let writes = probe().run(&store).await.unwrap();
    assert_eq!(writes.put_object, uniform(Rejected).put_object);
    assert!(writes.complete_multipart_upload.is_protected());
    assert_eq!(
        writes.unprotected(),
        [ConditionalOperation::PutObject, Delete]
    );
    assert_clean(&store);
}

#[tokio::test]
async fn versioned_buckets_lose_every_probe_version() {
    for conditionals in [Conditionals::AWS_S3, Conditionals::R2] {
        let recording = Recording::new(store(conditionals, true));
        probe().run(&recording).await.unwrap();
        assert_clean(&recording.inner);

        let versions = recording.log().versions.clone();
        assert!(!versions.is_empty());
        for (key, version) in &versions {
            let get = GetObject::new(key.clone()).with_version_id(version.clone());
            let error = recording.inner.get_object(get).await.unwrap_err();
            assert!(
                matches!(
                    error.kind(),
                    S3ErrorKind::NoSuchVersion | S3ErrorKind::MethodNotAllowed
                ),
                "{key} {version}: {error}"
            );
        }
    }
}

#[tokio::test]
async fn the_probe_stays_under_its_scratch_prefix() {
    let probe = probe();
    assert_eq!(
        probe.scratch_prefix(),
        "tenant/.skys3-probe/000000000000feed/"
    );
    let recording = Recording::new(store(Conditionals::R2, false));
    probe.run(&recording).await.unwrap();
    let log = recording.log();
    assert!(log.keys.len() > 10);
    for key in &log.keys {
        assert!(key.starts_with(probe.scratch_prefix()), "{key}");
    }

    let fresh = ConditionalProbe::with_fresh_nonce("");
    assert!(
        fresh
            .scratch_prefix()
            .starts_with(ConditionalProbe::SCRATCH_DIR)
    );
    assert_ne!(fresh, ConditionalProbe::with_fresh_nonce(""));
}

#[tokio::test]
async fn a_failed_step_fails_the_probe_and_cleans_up() {
    let store = store(Conditionals::AWS_S3, false);
    store.inject(Operation::CompleteMultipartUpload, Fault::InternalError);
    let error = probe().run(&store).await.unwrap_err();
    assert_eq!(
        error.step,
        "CompleteMultipartUpload with If-None-Match: * on a missing key"
    );
    assert_eq!(error.error.kind(), S3ErrorKind::InternalError);
    assert!(error.leftovers.is_empty());
    assert!(
        error
            .to_string()
            .starts_with("capability probe failed at CompleteMultipartUpload")
    );
    assert_clean(&store);

    // A lost response hides a write that was applied; cleanup removes it.
    let store = self::store(Conditionals::AWS_S3, false);
    store.inject(Operation::PutObject, Fault::LostResponse);
    let error = probe().run(&store).await.unwrap_err();
    assert!(error.error.may_have_applied());
    assert_clean(&store);

    let store = self::store(Conditionals::AWS_S3, false);
    store.inject(Operation::UploadPart, Fault::SlowDown);
    let error = probe().run(&store).await.unwrap_err();
    assert_eq!(error.error.kind(), S3ErrorKind::SlowDown);
    assert_clean(&store);

    let store = self::store(Conditionals::AWS_S3, false);
    store.inject(Operation::CreateMultipartUpload, Fault::SlowDown);
    let error = probe().run(&store).await.unwrap_err();
    assert_eq!(error.error.kind(), S3ErrorKind::SlowDown);
    assert_clean(&store);
}

#[tokio::test]
async fn failed_steps_of_every_kind_are_reported() {
    // Fail each request of the probe in turn: the probe reports the error
    // and removes everything, whichever step it was.
    let operations = [
        (Operation::PutObject, 5),
        (Operation::CompleteMultipartUpload, 4),
        (Operation::DeleteObject, 2),
    ];
    for (operation, requests) in operations {
        for skip in 0..requests {
            let store = store(Conditionals::AWS_S3, false);
            for _ in 0..skip {
                store.inject(operation, Fault::Delay(Duration::ZERO));
            }
            store.inject(operation, Fault::SlowDown);
            let error = probe().run(&store).await.unwrap_err();
            assert_eq!(
                error.error.kind(),
                S3ErrorKind::SlowDown,
                "{operation:?} {skip}"
            );
            assert_clean(&store);
        }
    }
}

#[tokio::test]
async fn cleanup_failures_are_reported_with_leftovers() {
    // The AWS S3 probe sends two conditional deletes; the third delete is
    // the first of the cleanup.
    let store = store(Conditionals::AWS_S3, false);
    store.inject(Operation::DeleteObject, Fault::Delay(Duration::ZERO));
    store.inject(Operation::DeleteObject, Fault::Delay(Duration::ZERO));
    store.inject(Operation::DeleteObject, Fault::InternalError);
    let error = probe().run(&store).await.unwrap_err();
    assert_eq!(error.step, "cleanup");
    assert_eq!(
        error.leftovers,
        ["tenant/.skys3-probe/000000000000feed/put-object"]
    );
    assert!(
        error
            .to_string()
            .ends_with("left behind: tenant/.skys3-probe/000000000000feed/put-object")
    );
    assert_eq!(
        store.keys(),
        ["tenant/.skys3-probe/000000000000feed/put-object"]
    );

    // A failed step and a failed cleanup: the step's error, and what is
    // left.
    let store = self::store(Conditionals::AWS_S3, false);
    store.inject(Operation::DeleteObject, Fault::SlowDown);
    store.inject(Operation::DeleteObject, Fault::InternalError);
    let error = probe().run(&store).await.unwrap_err();
    assert_eq!(
        error.step,
        "DeleteObject with If-Match with a different ETag"
    );
    assert_eq!(error.error.kind(), S3ErrorKind::SlowDown);
    assert_eq!(error.leftovers.len(), 1);
}

#[tokio::test]
async fn unaborted_uploads_are_aborted_by_the_cleanup() {
    let store = store(Conditionals::AWS_S3, false);
    // The first complete fails and its abort fails; the cleanup aborts it.
    store.inject(Operation::CompleteMultipartUpload, Fault::InternalError);
    store.inject(Operation::AbortMultipartUpload, Fault::SlowDown);
    let error = probe().run(&store).await.unwrap_err();
    assert!(error.leftovers.is_empty());
    assert_clean(&store);

    // If the cleanup's abort fails too, the upload is reported.
    let store = self::store(Conditionals::AWS_S3, false);
    store.inject(Operation::CompleteMultipartUpload, Fault::InternalError);
    store.inject(Operation::AbortMultipartUpload, Fault::SlowDown);
    store.inject(Operation::AbortMultipartUpload, Fault::SlowDown);
    let error = probe().run(&store).await.unwrap_err();
    assert_eq!(error.leftovers.len(), 1);
    assert!(
        error.leftovers[0].contains("(upload "),
        "{:?}",
        error.leftovers
    );
    assert_eq!(store.uploads().len(), 1);
}

#[tokio::test]
async fn lost_responses_on_versioned_buckets_are_found() {
    // Lose the response to each write of the probe in turn, on a versioned
    // bucket: whether or not the write was applied, the cleanup finds what
    // it created and leaves no version behind.
    // R2 ignores the completes' preconditions, so the probe sends it one
    // complete fewer.
    for (conditionals, completes) in [(Conditionals::AWS_S3, 4), (Conditionals::R2, 3)] {
        let operations = [
            (Operation::PutObject, 5),
            (Operation::CompleteMultipartUpload, completes),
        ];
        for (operation, requests) in operations {
            for skip in 0..requests {
                let store = store(conditionals, true);
                for _ in 0..skip {
                    store.inject(operation, Fault::Delay(Duration::ZERO));
                }
                store.inject(operation, Fault::LostResponse);
                let error = probe().run(&store).await.unwrap_err();
                assert!(error.error.may_have_applied(), "{operation:?} {skip}");
                assert!(error.leftovers.is_empty(), "{operation:?} {skip}: {error}");
                assert_clean(&store);
            }
        }
    }
}

#[tokio::test]
async fn unfound_versions_are_reported() {
    // The fourth complete, with If-Match with the current ETag, is applied
    // and its response lost. The cleanup's delete of the first complete's
    // version fails (after the two put-object versions), so the key's
    // current object is the lost version: the cleanup deletes it, then
    // finds the version it reported and stops.
    let store = store(Conditionals::AWS_S3, true);
    for _ in 0..3 {
        store.inject(
            Operation::CompleteMultipartUpload,
            Fault::Delay(Duration::ZERO),
        );
    }
    store.inject(Operation::CompleteMultipartUpload, Fault::LostResponse);
    store.inject(Operation::DeleteObject, Fault::Delay(Duration::ZERO));
    store.inject(Operation::DeleteObject, Fault::Delay(Duration::ZERO));
    store.inject(Operation::DeleteObject, Fault::InternalError);
    let error = probe().run(&store).await.unwrap_err();
    assert_eq!(
        error.step,
        "CompleteMultipartUpload with If-Match with the current ETag"
    );
    let key = scratch("complete-multipart-upload");
    let versions = store.versions();
    assert_eq!(versions.len(), 1, "{versions:?}");
    let (left, Some(version), false) = &versions[0] else {
        panic!("{versions:?}");
    };
    assert_eq!(left, &key);
    assert_eq!(error.leftovers, [format!("{key} (version {version})")]);

    // If HeadObject fails, the version is reported as unfound.
    let store = self::store(Conditionals::AWS_S3, true);
    store.inject(Operation::CompleteMultipartUpload, Fault::LostResponse);
    store.inject(Operation::HeadObject, Fault::InternalError);
    let error = probe().run(&store).await.unwrap_err();
    assert_eq!(
        error.leftovers,
        [format!("{key} (a version the probe could not find)")]
    );
    assert_eq!(store.keys(), [key]);
}

#[tokio::test]
async fn lost_deletes_may_leave_a_delete_marker() {
    // On a versioned bucket, the probe cannot find the delete marker a lost
    // delete may have added, so it reports it, whether or not the delete
    // was applied.
    let key = scratch("delete-object");
    let reported = [format!(
        "{key} (a delete marker, if the delete was applied)"
    )];
    for applied in [false, true] {
        let store = store(Conditionals::AWS_S3, true);
        if applied {
            // The second delete, with the current ETag, is applied.
            store.inject(Operation::DeleteObject, Fault::Delay(Duration::ZERO));
        }
        store.inject(Operation::DeleteObject, Fault::LostResponse);
        let error = probe().run(&store).await.unwrap_err();
        assert_eq!(error.leftovers, reported, "applied: {applied}");
        let markers: Vec<_> = store.versions().into_iter().map(|v| (v.0, v.2)).collect();
        if applied {
            assert_eq!(markers, [(key.clone(), true)]);
        } else {
            assert_eq!(markers, []);
        }
        assert!(store.keys().is_empty());
    }

    // On an unversioned bucket, a delete leaves nothing behind.
    let store = store(Conditionals::AWS_S3, false);
    store.inject(Operation::DeleteObject, Fault::Delay(Duration::ZERO));
    store.inject(Operation::DeleteObject, Fault::LostResponse);
    let error = probe().run(&store).await.unwrap_err();
    assert!(error.leftovers.is_empty(), "{error}");
    assert_clean(&store);
}

#[tokio::test]
async fn lost_upload_ids_are_reported() {
    // The upload a lost CreateMultipartUpload response may have started
    // cannot be found without its ID, so the probe reports it.
    let key = scratch("complete-multipart-upload");
    for fault in [Fault::LostResponse, Fault::LostRequest] {
        let store = store(Conditionals::AWS_S3, false);
        store.inject(Operation::CreateMultipartUpload, fault);
        let error = probe().run(&store).await.unwrap_err();
        assert_eq!(
            error.step,
            "CompleteMultipartUpload with If-None-Match: * on a missing key"
        );
        assert_eq!(
            error.leftovers,
            [format!("{key} (an upload, if it was created)")]
        );
        assert!(
            error
                .to_string()
                .ends_with("(an upload, if it was created)")
        );
        assert!(store.keys().is_empty());
        let uploads = store.uploads();
        if fault == Fault::LostResponse {
            assert_eq!(uploads.len(), 1);
            assert_eq!(uploads[0].1, key);
        } else {
            assert!(uploads.is_empty());
        }
    }
}

#[tokio::test]
async fn writes_without_versions_on_versioned_buckets_are_found() {
    // A store that reports versions for PutObject but not for
    // CompleteMultipartUpload: the cleanup finds the completes' versions.
    for conditionals in [Conditionals::AWS_S3, Conditionals::R2] {
        let mut recording = Recording::new(store(conditionals, true));
        recording.hide_complete_versions = true;
        probe().run(&recording).await.unwrap();
        assert_clean(&recording.inner);
    }
}

#[tokio::test]
async fn other_refusals_count_as_rejections() {
    // A 400 without a known code, and a 412 even when the precondition
    // holds, both reject the header.
    let bad_request = S3Error::new(S3ErrorKind::Other, "unsupported header")
        .with_status(400)
        .with_code("UnsupportedHeader");
    let always_412 = S3Error::new(S3ErrorKind::PreconditionFailed, "always");
    for refusal in [bad_request, always_412] {
        let mut recording = Recording::new(store(Conditionals::AWS_S3, false));
        recording.refuse_conditionals = Some(refusal);
        let writes = probe().run(&recording).await.unwrap();
        assert_eq!(writes, uniform(Rejected));
        assert_clean(&recording.inner);
    }

    // A 501 without a known code rejects the header too, and is not taken
    // for a write that may have been applied.
    let not_implemented = S3Error::new(S3ErrorKind::Other, "unsupported")
        .with_status(501)
        .with_code("Unsupported");
    assert!(not_implemented.may_have_applied());
    for versioning in [false, true] {
        let mut recording = Recording::new(store(Conditionals::AWS_S3, versioning));
        recording.refuse_conditionals = Some(not_implemented.clone());
        let writes = probe().run(&recording).await.unwrap();
        assert_eq!(writes, uniform(Rejected));
        assert_clean(&recording.inner);
    }

    // An error that says nothing about the header fails the probe.
    let mut recording = Recording::new(store(Conditionals::AWS_S3, false));
    recording.refuse_conditionals = Some(
        S3Error::new(S3ErrorKind::Other, "denied")
            .with_status(403)
            .with_code("AccessDenied"),
    );
    let error = probe().run(&recording).await.unwrap_err();
    assert_eq!(error.error.code(), "AccessDenied");
    assert_eq!(
        error.step,
        "PutObject with If-None-Match: * on a missing key"
    );
    assert_clean(&recording.inner);
}

#[test]
fn support_formats_and_protects() {
    let writes = uniform(Honored);
    for operation in ConditionalOperation::ALL {
        assert!(writes.operation(operation).is_protected());
        assert_eq!(operation.to_string(), operation.as_str());
    }
    assert_eq!(writes.delete_object.to_string(), "If-Match honored");
    let partial = OperationSupport {
        if_none_match: Some(Honored),
        if_match: Rejected,
    };
    assert!(!partial.is_protected());
    assert_eq!(
        partial.to_string(),
        "If-None-Match honored, If-Match rejected"
    );
    assert!(Honored.is_honored() && !Ignored.is_honored() && !Rejected.is_honored());
}

/// What a [`Recording`] saw.
#[derive(Debug, Default)]
struct Log {
    /// Every key a request named.
    keys: Vec<String>,
    /// Every version a write or delete created.
    versions: Vec<(String, VersionId)>,
}

/// A store that records requests, and can refuse every conditional write
/// with a fixed error.
#[derive(Debug)]
struct Recording {
    inner: SimS3,
    log: Mutex<Log>,
    refuse_conditionals: Option<S3Error>,
    /// Whether `CompleteMultipartUpload` answers without the version.
    hide_complete_versions: bool,
}

impl Recording {
    fn new(inner: SimS3) -> Self {
        Recording {
            inner,
            log: Mutex::default(),
            refuse_conditionals: None,
            hide_complete_versions: false,
        }
    }

    fn log(&self) -> std::sync::MutexGuard<'_, Log> {
        self.log.lock().unwrap()
    }

    fn saw(&self, key: &str) {
        self.log().keys.push(key.to_owned());
    }

    fn created(&self, key: &str, version: Option<&VersionId>) {
        if let Some(version) = version {
            self.log().versions.push((key.to_owned(), version.clone()));
        }
    }

    fn refuse(&self, conditional: bool) -> S3Result<()> {
        match &self.refuse_conditionals {
            Some(error) if conditional => Err(error.clone()),
            _ => Ok(()),
        }
    }
}

impl ObjectStore for Recording {
    async fn put_object(&self, request: PutObject) -> S3Result<WriteOutput> {
        self.saw(&request.key);
        self.refuse(request.precondition.is_some())?;
        let key = request.key.clone();
        let output = self.inner.put_object(request).await?;
        self.created(&key, output.version_id.as_ref());
        Ok(output)
    }

    async fn get_object(&self, request: GetObject) -> S3Result<GetOutput> {
        self.saw(&request.key);
        self.inner.get_object(request).await
    }

    async fn head_object(&self, request: HeadObject) -> S3Result<ObjectInfo> {
        self.saw(&request.key);
        self.inner.head_object(request).await
    }

    async fn delete_object(&self, request: DeleteObject) -> S3Result<DeleteOutput> {
        self.saw(&request.key);
        self.refuse(request.if_match.is_some())?;
        let key = request.key.clone();
        let output = self.inner.delete_object(request).await?;
        if output.delete_marker {
            self.created(&key, output.version_id.as_ref());
        }
        Ok(output)
    }

    async fn list_objects_v2(&self, request: ListObjectsV2) -> S3Result<ListObjectsV2Output> {
        self.saw(&request.prefix);
        self.inner.list_objects_v2(request).await
    }

    async fn copy_object(&self, request: CopyObject) -> S3Result<WriteOutput> {
        self.saw(&request.key);
        self.inner.copy_object(request).await
    }

    async fn create_multipart_upload(&self, request: CreateMultipartUpload) -> S3Result<UploadId> {
        self.saw(&request.key);
        self.inner.create_multipart_upload(request).await
    }

    async fn upload_part(&self, request: UploadPart) -> S3Result<ETag> {
        self.saw(&request.key);
        self.inner.upload_part(request).await
    }

    async fn complete_multipart_upload(
        &self,
        request: CompleteMultipartUpload,
    ) -> S3Result<WriteOutput> {
        self.saw(&request.key);
        self.refuse(request.precondition.is_some())?;
        let key = request.key.clone();
        let mut output = self.inner.complete_multipart_upload(request).await?;
        self.created(&key, output.version_id.as_ref());
        if self.hide_complete_versions {
            output.version_id = None;
        }
        Ok(output)
    }

    async fn abort_multipart_upload(&self, request: AbortMultipartUpload) -> S3Result<()> {
        self.saw(&request.key);
        self.inner.abort_multipart_upload(request).await
    }

    async fn list_parts(&self, request: ListParts) -> S3Result<ListPartsOutput> {
        self.saw(&request.key);
        self.inner.list_parts(request).await
    }
}
