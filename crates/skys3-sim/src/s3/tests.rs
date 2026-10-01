use std::fmt::Debug;

use bytes::Bytes;
use md5::{Digest, Md5};
use skys3_remote::{
    ByteRange, CompletedPart, ListedObject, MAX_KEY_LEN, MetadataDirective, VersionId,
};
use tokio::time::Instant;

use super::*;

const MIN_PART: u64 = 4;

fn config() -> SimS3Config {
    SimS3Config {
        min_part_size: MIN_PART,
        ..SimS3Config::default()
    }
}

fn store() -> SimS3 {
    SimS3::new(1, config())
}

fn store_with(conditionals: Conditionals) -> SimS3 {
    SimS3::new(
        1,
        SimS3Config {
            conditionals,
            ..config()
        },
    )
}

fn versioned() -> SimS3 {
    SimS3::new(
        1,
        SimS3Config {
            versioning: true,
            ..config()
        },
    )
}

fn etag(value: &str) -> ETag {
    ETag::new(value).unwrap()
}

fn md5_hex(data: &[u8]) -> String {
    Md5::digest(data)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn kind<T: Debug>(result: S3Result<T>) -> S3ErrorKind {
    result.unwrap_err().kind()
}

fn body(store: &SimS3, key: &str) -> Option<Bytes> {
    store.object(key).map(|object| object.body)
}

async fn put(store: &SimS3, key: &str, data: &'static str) -> WriteOutput {
    store.put_object(PutObject::new(key, data)).await.unwrap()
}

fn if_match(etag: &ETag) -> WritePrecondition {
    WritePrecondition::IfMatch(etag.clone())
}

fn stale() -> WritePrecondition {
    WritePrecondition::IfMatch(etag("0123"))
}

// Objects: PUT, GET, HEAD, DELETE.

#[tokio::test]
async fn put_get_and_head_round_trip() {
    let store = store();
    let mut metadata = UserMetadata::new();
    metadata.insert("owner", "team-a").unwrap();
    let written = store
        .put_object(
            PutObject::new("a/b", "hello")
                .with_metadata(metadata.clone())
                .with_content_type("text/plain"),
        )
        .await
        .unwrap();
    assert_eq!(written.etag.as_str(), md5_hex(b"hello"));
    assert_eq!(written.version_id, None);

    let got = store.get_object(GetObject::new("a/b")).await.unwrap();
    assert_eq!(got.body, Bytes::from("hello"));
    assert_eq!(got.range, None);
    let expected = ObjectInfo {
        etag: written.etag.clone(),
        size: 5,
        version_id: None,
        metadata,
        content_type: Some("text/plain".into()),
    };
    assert_eq!(got.info, expected);
    assert_eq!(
        store.head_object(HeadObject::new("a/b")).await.unwrap(),
        expected
    );

    // An overwrite replaces everything.
    let rewritten = put(&store, "a/b", "bye").await;
    let head = store.head_object(HeadObject::new("a/b")).await.unwrap();
    assert_eq!(head.etag, rewritten.etag);
    assert!(head.metadata.is_empty());
    assert_eq!(head.content_type, None);
}

#[tokio::test]
async fn missing_objects_are_404() {
    let store = store();
    assert_eq!(
        kind(store.get_object(GetObject::new("nope")).await),
        S3ErrorKind::NoSuchKey
    );
    assert_eq!(
        kind(store.head_object(HeadObject::new("nope")).await),
        S3ErrorKind::NoSuchKey
    );
    // Unconditional deletes of missing keys succeed.
    assert_eq!(
        store.delete_object(DeleteObject::new("nope")).await,
        Ok(DeleteOutput::default())
    );
}

#[tokio::test]
async fn deletes_remove_objects() {
    let store = store();
    put(&store, "k", "v").await;
    store.delete_object(DeleteObject::new("k")).await.unwrap();
    assert_eq!(store.object("k"), None);
    assert!(store.keys().is_empty());
}

#[tokio::test]
async fn ranged_gets() {
    let store = store();
    put(&store, "k", "0123456789").await;
    let get = |range| store.get_object(GetObject::new("k").with_range(range));
    let inclusive = |first, last| ByteRange::inclusive(first, last).unwrap();

    let part = get(inclusive(2, 4)).await.unwrap();
    assert_eq!(part.body, Bytes::from("234"));
    assert_eq!(part.range, Some(2..5));
    assert_eq!(part.info.size, 10);
    assert_eq!(
        get(inclusive(8, 100)).await.unwrap().body,
        Bytes::from("89")
    );
    assert_eq!(
        get(ByteRange::from_offset(7)).await.unwrap().body,
        Bytes::from("789")
    );
    assert_eq!(
        get(ByteRange::suffix(3)).await.unwrap().body,
        Bytes::from("789")
    );
    assert_eq!(
        get(ByteRange::suffix(30)).await.unwrap().body,
        Bytes::from("0123456789")
    );
    assert_eq!(
        kind(get(inclusive(10, 12)).await),
        S3ErrorKind::InvalidRange
    );
    assert_eq!(
        kind(get(ByteRange::suffix(0)).await),
        S3ErrorKind::InvalidRange
    );
}

#[tokio::test]
async fn read_conditions() {
    let store = store();
    let written = put(&store, "k", "v").await;
    let other = etag("0123");

    let get = |request: GetObject| store.get_object(request);
    assert!(
        get(GetObject::new("k").with_if_match(written.etag.clone()))
            .await
            .is_ok()
    );
    assert_eq!(
        kind(get(GetObject::new("k").with_if_match(other.clone())).await),
        S3ErrorKind::PreconditionFailed
    );
    assert_eq!(
        kind(get(GetObject::new("k").with_if_none_match(written.etag.clone())).await),
        S3ErrorKind::NotModified
    );
    assert!(
        get(GetObject::new("k").with_if_none_match(other.clone()))
            .await
            .is_ok()
    );
    assert_eq!(
        kind(
            store
                .head_object(HeadObject::new("k").with_if_none_match(written.etag.clone()))
                .await
        ),
        S3ErrorKind::NotModified
    );
    let head = HeadObject {
        if_match: Some(other),
        ..HeadObject::new("k")
    };
    assert_eq!(
        kind(store.head_object(head).await),
        S3ErrorKind::PreconditionFailed
    );
}

#[tokio::test]
async fn keys_must_be_1_to_1024_bytes() {
    let store = store();
    assert_eq!(
        kind(store.put_object(PutObject::new("", "v")).await),
        S3ErrorKind::InvalidArgument
    );
    let long = "k".repeat(MAX_KEY_LEN + 1);
    assert_eq!(
        kind(store.put_object(PutObject::new(long.clone(), "v")).await),
        S3ErrorKind::InvalidArgument
    );
    assert_eq!(
        kind(
            store
                .create_multipart_upload(CreateMultipartUpload::new(long))
                .await
        ),
        S3ErrorKind::InvalidArgument
    );
    assert!(
        store
            .put_object(PutObject::new("k".repeat(MAX_KEY_LEN), "v"))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn user_metadata_is_limited_to_2_kib() {
    let store = store();
    let metadata = |size: usize| {
        let mut metadata = UserMetadata::new();
        metadata.insert("k", "v".repeat(size - 1)).unwrap();
        metadata
    };
    let at_limit = metadata(UserMetadata::S3_LIMIT);
    let over = metadata(UserMetadata::S3_LIMIT + 1);

    let put = |metadata| store.put_object(PutObject::new("k", "v").with_metadata(metadata));
    assert!(put(at_limit.clone()).await.is_ok());
    assert_eq!(kind(put(over.clone()).await), S3ErrorKind::MetadataTooLarge);
    assert_eq!(
        kind(
            store
                .create_multipart_upload(
                    CreateMultipartUpload::new("m").with_metadata(over.clone())
                )
                .await
        ),
        S3ErrorKind::MetadataTooLarge
    );
    let copy = CopyObject::new("k", "c").with_metadata_directive(MetadataDirective::Replace {
        metadata: over,
        content_type: None,
    });
    assert_eq!(
        kind(store.copy_object(copy).await),
        S3ErrorKind::MetadataTooLarge
    );
    assert_eq!(store.keys(), ["k"]);
}

// Write preconditions, honored.

#[tokio::test]
async fn put_if_absent() {
    let store = store();
    let create = || PutObject::new("k", "first").with_precondition(WritePrecondition::IfAbsent);
    store.put_object(create()).await.unwrap();
    assert_eq!(
        kind(store.put_object(create()).await),
        S3ErrorKind::PreconditionFailed
    );
    assert_eq!(body(&store, "k"), Some(Bytes::from("first")));
}

#[tokio::test]
async fn put_if_match() {
    let store = store();
    let first = put(&store, "k", "first").await;
    let second = store
        .put_object(PutObject::new("k", "second").with_precondition(if_match(&first.etag)))
        .await
        .unwrap();
    // The first ETag is stale now.
    assert_eq!(
        kind(
            store
                .put_object(PutObject::new("k", "third").with_precondition(if_match(&first.etag)))
                .await
        ),
        S3ErrorKind::PreconditionFailed
    );
    assert_eq!(store.object("k").unwrap().info.etag, second.etag);
    // If-Match on a missing key is 404.
    assert_eq!(
        kind(
            store
                .put_object(PutObject::new("missing", "v").with_precondition(stale()))
                .await
        ),
        S3ErrorKind::NoSuchKey
    );
    assert_eq!(store.object("missing"), None);
}

#[tokio::test]
async fn delete_if_match() {
    let store = store();
    let written = put(&store, "k", "v").await;
    let delete = |etag: ETag| store.delete_object(DeleteObject::new("k").with_if_match(etag));
    assert_eq!(
        kind(delete(etag("0123")).await),
        S3ErrorKind::PreconditionFailed
    );
    assert!(store.object("k").is_some());
    delete(written.etag.clone()).await.unwrap();
    assert_eq!(store.object("k"), None);
    // The object is gone: a retry of the same delete is 404.
    assert_eq!(kind(delete(written.etag).await), S3ErrorKind::NoSuchKey);
}

/// Uploads `parts` to a new upload of `key` and returns the upload and the
/// parts to complete it with.
async fn upload(
    store: &SimS3,
    key: &str,
    parts: &[&'static str],
) -> (UploadId, Vec<CompletedPart>) {
    let upload_id = store
        .create_multipart_upload(CreateMultipartUpload::new(key))
        .await
        .unwrap();
    let mut completed = Vec::new();
    for (part_number, body) in (1..).zip(parts) {
        let etag = store
            .upload_part(UploadPart {
                key: key.into(),
                upload_id: upload_id.clone(),
                part_number,
                body: Bytes::from_static(body.as_bytes()),
            })
            .await
            .unwrap();
        completed.push(CompletedPart { part_number, etag });
    }
    (upload_id, completed)
}

fn complete(
    key: &str,
    upload_id: &UploadId,
    parts: Vec<CompletedPart>,
    precondition: WritePrecondition,
) -> CompleteMultipartUpload {
    CompleteMultipartUpload {
        key: key.into(),
        upload_id: upload_id.clone(),
        parts,
        precondition,
    }
}

#[tokio::test]
async fn complete_multipart_upload_if_absent() {
    let store = store();
    let (upload_id, parts) = upload(&store, "k", &["aaaa", "b"]).await;
    let request = complete("k", &upload_id, parts.clone(), WritePrecondition::IfAbsent);
    let written = store
        .complete_multipart_upload(request.clone())
        .await
        .unwrap();
    assert!(written.etag.as_str().ends_with("-2"));

    let (upload_id, parts) = upload(&store, "k", &["cccc", "d"]).await;
    let request = complete("k", &upload_id, parts, WritePrecondition::IfAbsent);
    assert_eq!(
        kind(store.complete_multipart_upload(request).await),
        S3ErrorKind::PreconditionFailed
    );
    assert_eq!(body(&store, "k"), Some(Bytes::from("aaaab")));
    // A failed precondition leaves the upload open.
    assert_eq!(store.uploads(), [(upload_id, "k".to_owned())]);
}

#[tokio::test]
async fn complete_multipart_upload_if_match() {
    let store = store();
    let first = put(&store, "k", "v").await;
    let (upload_id, parts) = upload(&store, "k", &["aaaa", "b"]).await;
    let request = |precondition| complete("k", &upload_id, parts.clone(), precondition);
    assert_eq!(
        kind(store.complete_multipart_upload(request(stale())).await),
        S3ErrorKind::PreconditionFailed
    );
    store
        .complete_multipart_upload(request(if_match(&first.etag)))
        .await
        .unwrap();
    assert_eq!(body(&store, "k"), Some(Bytes::from("aaaab")));

    let (upload_id, parts) = upload(&store, "missing", &["x"]).await;
    let request = complete("missing", &upload_id, parts, stale());
    assert_eq!(
        kind(store.complete_multipart_upload(request).await),
        S3ErrorKind::NoSuchKey
    );
}

#[tokio::test]
async fn copy_destination_preconditions() {
    let store = store();
    put(&store, "src", "data").await;
    let dst = put(&store, "dst", "old").await;
    let copy = |precondition| {
        store.copy_object(CopyObject::new("src", "dst").with_precondition(precondition))
    };
    assert_eq!(
        kind(copy(WritePrecondition::IfAbsent).await),
        S3ErrorKind::PreconditionFailed
    );
    assert_eq!(kind(copy(stale()).await), S3ErrorKind::PreconditionFailed);
    assert_eq!(body(&store, "dst"), Some(Bytes::from("old")));
    copy(if_match(&dst.etag)).await.unwrap();
    assert_eq!(body(&store, "dst"), Some(Bytes::from("data")));

    let to_missing = CopyObject::new("src", "new").with_precondition(stale());
    assert_eq!(
        kind(store.copy_object(to_missing).await),
        S3ErrorKind::NoSuchKey
    );
    let to_new = CopyObject::new("src", "new").with_precondition(WritePrecondition::IfAbsent);
    assert!(store.copy_object(to_new).await.is_ok());
}

#[tokio::test]
async fn copy_source_if_match() {
    let store = store();
    let source = put(&store, "src", "data").await;
    let copy = |etag| store.copy_object(CopyObject::new("src", "dst").with_source_if_match(etag));
    assert_eq!(
        kind(copy(etag("0123")).await),
        S3ErrorKind::PreconditionFailed
    );
    assert_eq!(store.object("dst"), None);
    let copied = copy(source.etag.clone()).await.unwrap();
    assert_eq!(copied.etag, source.etag);
}

// Write preconditions, ignored or rejected.

#[tokio::test]
async fn ignored_preconditions_write_unconditionally() {
    let store = store_with(Conditionals::all(ConditionalSupport::Ignored));
    put(&store, "k", "v").await;
    let put_stale = PutObject::new("k", "put").with_precondition(stale());
    store.put_object(put_stale).await.unwrap();
    assert_eq!(body(&store, "k"), Some(Bytes::from("put")));

    let (upload_id, parts) = upload(&store, "k", &["mpu"]).await;
    let request = complete("k", &upload_id, parts, WritePrecondition::IfAbsent);
    store.complete_multipart_upload(request).await.unwrap();
    assert_eq!(body(&store, "k"), Some(Bytes::from("mpu")));

    let copy = CopyObject::new("k", "c")
        .with_source_if_match(etag("0123"))
        .with_precondition(stale());
    store.copy_object(copy).await.unwrap();
    assert_eq!(body(&store, "c"), Some(Bytes::from("mpu")));

    let delete = DeleteObject::new("k").with_if_match(etag("0123"));
    store.delete_object(delete).await.unwrap();
    assert_eq!(store.object("k"), None);
}

#[tokio::test]
async fn rejected_preconditions_fail_with_501() {
    let store = store_with(Conditionals::all(ConditionalSupport::Rejected));
    let written = put(&store, "k", "v").await;
    let current = if_match(&written.etag);
    let not_implemented = S3ErrorKind::NotImplemented;

    let conditional_put = PutObject::new("k", "put").with_precondition(current.clone());
    assert_eq!(
        kind(store.put_object(conditional_put).await),
        not_implemented
    );

    let (upload_id, parts) = upload(&store, "k", &["mpu"]).await;
    let request = complete("k", &upload_id, parts.clone(), current.clone());
    assert_eq!(
        kind(store.complete_multipart_upload(request).await),
        not_implemented
    );

    let copy = CopyObject::new("k", "c").with_precondition(WritePrecondition::IfAbsent);
    assert_eq!(kind(store.copy_object(copy).await), not_implemented);
    let copy = CopyObject::new("k", "c").with_source_if_match(written.etag.clone());
    assert_eq!(kind(store.copy_object(copy).await), not_implemented);

    let delete = DeleteObject::new("k").with_if_match(written.etag.clone());
    assert_eq!(kind(store.delete_object(delete).await), not_implemented);

    // Nothing was written, and unconditional requests still work.
    assert_eq!(store.keys(), ["k"]);
    assert_eq!(body(&store, "k"), Some(Bytes::from("v")));
    let request = complete("k", &upload_id, parts, WritePrecondition::None);
    store.complete_multipart_upload(request).await.unwrap();
    store.copy_object(CopyObject::new("k", "c")).await.unwrap();
    store.delete_object(DeleteObject::new("k")).await.unwrap();
    assert_eq!(store.keys(), ["c"]);
}

#[tokio::test]
async fn r2_profile_honors_only_put_and_copy_source() {
    let store = store_with(Conditionals::R2);
    put(&store, "k", "v").await;
    let create = PutObject::new("k", "x").with_precondition(WritePrecondition::IfAbsent);
    assert_eq!(
        kind(store.put_object(create).await),
        S3ErrorKind::PreconditionFailed
    );
    let copy = CopyObject::new("k", "c").with_source_if_match(etag("0123"));
    assert_eq!(
        kind(store.copy_object(copy).await),
        S3ErrorKind::PreconditionFailed
    );

    // A delete and a complete with stale preconditions both apply.
    let (upload_id, parts) = upload(&store, "k", &["mpu"]).await;
    let request = complete("k", &upload_id, parts, WritePrecondition::IfAbsent);
    store.complete_multipart_upload(request).await.unwrap();
    let copy = CopyObject::new("k", "c").with_precondition(stale());
    store.copy_object(copy).await.unwrap();
    let delete = DeleteObject::new("k").with_if_match(etag("0123"));
    store.delete_object(delete).await.unwrap();
    assert_eq!(store.keys(), ["c"]);
}

// 409 ConditionalRequestConflict.

#[tokio::test(start_paused = true)]
async fn a_write_during_a_conditional_write_makes_it_conflict() {
    let store = store();
    let first = put(&store, "k", "first").await;
    store.inject(
        Operation::PutObject,
        Fault::Delay(Duration::from_millis(10)),
    );
    let conditional = PutObject::new("k", "conditional").with_precondition(if_match(&first.etag));
    let (conditional, plain) = tokio::join!(
        store.put_object(conditional),
        store.put_object(PutObject::new("k", "plain")),
    );
    assert_eq!(kind(conditional), S3ErrorKind::ConditionalRequestConflict);
    plain.unwrap();
    assert_eq!(body(&store, "k"), Some(Bytes::from("plain")));
    assert_eq!(store.stats().conflicts, 1);
}

#[tokio::test(start_paused = true)]
async fn racing_creates_one_wins_and_the_other_conflicts() {
    let store = store();
    store.inject(
        Operation::PutObject,
        Fault::Delay(Duration::from_millis(10)),
    );
    store.inject(
        Operation::PutObject,
        Fault::Delay(Duration::from_millis(20)),
    );
    let create = |body| PutObject::new("k", body).with_precondition(WritePrecondition::IfAbsent);
    let (a, b) = tokio::join!(store.put_object(create("a")), store.put_object(create("b")));
    a.unwrap();
    assert_eq!(kind(b), S3ErrorKind::ConditionalRequestConflict);
    // The loser re-reads and retries: now it is a plain 412.
    assert_eq!(
        kind(store.put_object(create("b")).await),
        S3ErrorKind::PreconditionFailed
    );
    assert_eq!(body(&store, "k"), Some(Bytes::from("a")));
}

#[tokio::test(start_paused = true)]
async fn conflicts_need_a_successful_write_to_the_same_key() {
    let store = store();
    let written = put(&store, "k", "v").await;
    store.inject(
        Operation::DeleteObject,
        Fault::Delay(Duration::from_millis(10)),
    );
    let delete = store.delete_object(DeleteObject::new("k").with_if_match(written.etag));
    let others = async {
        // Another key, and a failed write to the same key.
        put(&store, "other", "v").await;
        let failed = PutObject::new("k", "x").with_precondition(WritePrecondition::IfAbsent);
        assert!(store.put_object(failed).await.is_err());
    };
    let (deleted, ()) = tokio::join!(delete, others);
    deleted.unwrap();
    assert_eq!(store.keys(), ["other"]);
}

#[tokio::test(start_paused = true)]
async fn every_conditional_write_can_conflict() {
    let store = store();
    let delay = Fault::Delay(Duration::from_millis(10));
    let written = put(&store, "k", "v").await;
    let (upload_id, parts) = upload(&store, "k", &["mpu"]).await;
    let plain = || store.put_object(PutObject::new("k", "plain"));

    store.inject(Operation::CompleteMultipartUpload, delay);
    let request = complete("k", &upload_id, parts, if_match(&written.etag));
    let (result, _) = tokio::join!(store.complete_multipart_upload(request), plain());
    assert_eq!(kind(result), S3ErrorKind::ConditionalRequestConflict);

    let current = store.object("k").unwrap().info.etag;
    store.inject(Operation::CopyObject, delay);
    put(&store, "src", "v").await;
    let copy = CopyObject::new("src", "k").with_precondition(if_match(&current));
    let (result, _) = tokio::join!(store.copy_object(copy), plain());
    assert_eq!(kind(result), S3ErrorKind::ConditionalRequestConflict);

    let current = store.object("k").unwrap().info.etag;
    store.inject(Operation::DeleteObject, delay);
    let delete = DeleteObject::new("k").with_if_match(current);
    let (result, _) = tokio::join!(store.delete_object(delete), plain());
    assert_eq!(kind(result), S3ErrorKind::ConditionalRequestConflict);
    assert_eq!(store.stats().conflicts, 3);
}

#[tokio::test(start_paused = true)]
async fn unconditional_and_ignored_writes_never_conflict() {
    let store = store_with(Conditionals::R2);
    store.inject(
        Operation::DeleteObject,
        Fault::Delay(Duration::from_millis(10)),
    );
    let written = put(&store, "k", "v").await;
    // R2 ignores If-Match on DeleteObject, so the delete is unconditional.
    let delete = DeleteObject::new("k").with_if_match(written.etag);
    let (deleted, _) = tokio::join!(store.delete_object(delete), put(&store, "k", "w"));
    deleted.unwrap();
    assert_eq!(store.object("k"), None);
    assert_eq!(store.stats().conflicts, 0);
}

#[tokio::test(start_paused = true)]
async fn abandoned_conditional_writes_are_forgotten() {
    let store = store();
    store.inject(Operation::PutObject, Fault::Delay(Duration::from_secs(10)));
    let create = PutObject::new("k", "v").with_precondition(WritePrecondition::IfAbsent);
    let abandoned = tokio::time::timeout(Duration::from_secs(1), store.put_object(create)).await;
    assert!(abandoned.is_err());
    assert!(store.state().in_flight.is_empty());
    assert_eq!(store.object("k"), None);
}

// Injected faults.

#[tokio::test]
async fn scripted_conflicts_apply_nothing() {
    let store = store();
    store.inject(Operation::PutObject, Fault::Conflict);
    let result = store.put_object(PutObject::new("k", "v")).await;
    assert_eq!(kind(result), S3ErrorKind::ConditionalRequestConflict);
    assert_eq!(store.object("k"), None);
}

#[tokio::test]
async fn server_errors_apply_nothing() {
    let store = store();
    store.inject(Operation::PutObject, Fault::InternalError);
    store.inject(Operation::PutObject, Fault::SlowDown);
    let internal = kind(store.put_object(PutObject::new("k", "v")).await);
    assert_eq!(internal, S3ErrorKind::InternalError);
    let slow_down = kind(store.put_object(PutObject::new("k", "v")).await);
    assert_eq!(slow_down, S3ErrorKind::SlowDown);
    assert_eq!(slow_down.status(), Some(503));
    assert_eq!(store.object("k"), None);
    let stats = store.stats();
    assert_eq!((stats.internal_errors, stats.slow_downs), (1, 1));
}

#[tokio::test]
async fn lost_requests_apply_nothing() {
    let store = store();
    store.inject(Operation::DeleteObject, Fault::LostRequest);
    put(&store, "k", "v").await;
    let result = store.delete_object(DeleteObject::new("k")).await;
    assert_eq!(kind(result), S3ErrorKind::Timeout);
    assert!(store.object("k").is_some());
    assert_eq!(store.stats().lost_requests, 1);
}

#[tokio::test]
async fn lost_responses_hide_applied_writes() {
    let store = store();
    let mut kinds = Vec::new();
    for attempt in 0..16 {
        store.inject(Operation::PutObject, Fault::LostResponse);
        let key = format!("k{attempt}");
        let error = store
            .put_object(PutObject::new(key.clone(), "v"))
            .await
            .unwrap_err();
        assert!(error.kind().may_have_applied());
        kinds.push(error.kind());
        // Applied anyway.
        assert_eq!(body(&store, &key), Some(Bytes::from("v")));
    }
    // Lost responses surface both as timeouts and as server errors.
    assert!(kinds.contains(&S3ErrorKind::Timeout));
    assert!(kinds.contains(&S3ErrorKind::InternalError));
    assert_eq!(store.stats().lost_responses, 16);
}

#[tokio::test]
async fn a_lost_conditional_create_is_recognized_by_its_write_identity() {
    // The §7.2 recovery rule: the retry fails its precondition, and a HEAD
    // finds the flush's own write identity.
    let store = store();
    let mut metadata = UserMetadata::new();
    metadata.insert("skys3-wid", "c/b/1/2.3").unwrap();
    let create = PutObject::new("k", "v")
        .with_metadata(metadata)
        .with_precondition(WritePrecondition::IfAbsent);
    store.inject(Operation::PutObject, Fault::LostResponse);
    assert!(store.put_object(create.clone()).await.is_err());
    assert_eq!(
        kind(store.put_object(create).await),
        S3ErrorKind::PreconditionFailed
    );
    let head = store.head_object(HeadObject::new("k")).await.unwrap();
    assert_eq!(head.metadata.write_identity(), Some("c/b/1/2.3"));
}

#[tokio::test]
async fn stale_reads_answer_from_before_the_latest_write() {
    let store = store();
    // A key never written reads the same, stale or not.
    store.inject(Operation::GetObject, Fault::StaleRead);
    assert_eq!(
        kind(store.get_object(GetObject::new("k")).await),
        S3ErrorKind::NoSuchKey
    );
    let first = put(&store, "k", "one").await;
    store.inject(Operation::GetObject, Fault::StaleRead);
    assert_eq!(
        kind(store.get_object(GetObject::new("k")).await),
        S3ErrorKind::NoSuchKey,
        "a stale read of a new key finds nothing"
    );
    put(&store, "k", "two").await;
    store.inject(Operation::GetObject, Fault::StaleRead);
    let stale = store.get_object(GetObject::new("k")).await.unwrap();
    assert_eq!(
        (stale.body, stale.info.etag),
        (Bytes::from("one"), first.etag.clone())
    );
    store.inject(Operation::HeadObject, Fault::StaleRead);
    let head = store.head_object(HeadObject::new("k")).await.unwrap();
    assert_eq!(head.etag, first.etag);
    // Without the fault, reads are current.
    assert_eq!(
        store.get_object(GetObject::new("k")).await.unwrap().body,
        Bytes::from("two")
    );
    // A failed write leaves the previous state alone.
    let failed = store
        .put_object(PutObject::new("k", "three").with_precondition(WritePrecondition::IfAbsent))
        .await;
    assert_eq!(kind(failed), S3ErrorKind::PreconditionFailed);
    store.inject(Operation::GetObject, Fault::StaleRead);
    let stale = store.get_object(GetObject::new("k")).await.unwrap();
    assert_eq!(stale.body, Bytes::from("one"));
    // A deleted key still reads, stale.
    store.delete_object(DeleteObject::new("k")).await.unwrap();
    store.inject(Operation::GetObject, Fault::StaleRead);
    let stale = store.get_object(GetObject::new("k")).await.unwrap();
    assert_eq!(stale.body, Bytes::from("two"));
}

#[tokio::test]
async fn stale_reads_on_a_versioned_bucket_name_the_old_version() {
    let store = versioned();
    let first = put(&store, "k", "one").await;
    put(&store, "k", "two").await;
    store.inject(Operation::GetObject, Fault::StaleRead);
    let stale = store.get_object(GetObject::new("k")).await.unwrap();
    assert_eq!(stale.info.version_id, first.version_id);
    // Reads of a specific version are never stale.
    let second = store.versions()[1].1.clone().unwrap();
    store.inject(Operation::GetObject, Fault::StaleRead);
    let read = store
        .get_object(GetObject::new("k").with_version_id(second.clone()))
        .await
        .unwrap();
    assert_eq!(
        (read.body, read.info.version_id),
        (Bytes::from("two"), Some(second))
    );
}

#[tokio::test]
async fn random_stale_reads_follow_their_probability() {
    let store = store();
    put(&store, "k", "one").await;
    put(&store, "k", "two").await;
    store.set_faults(SimS3Faults {
        stale_read_probability: 0.5,
        ..SimS3Faults::NONE
    });
    let mut stale = 0;
    for _ in 0..200 {
        let read = store.get_object(GetObject::new("k")).await.unwrap();
        stale += usize::from(read.body == "one");
    }
    assert!((60..140).contains(&stale), "{stale}");
}

#[tokio::test(start_paused = true)]
async fn delays_hold_both_legs_of_a_request() {
    let store = store();
    store.set_faults(SimS3Faults {
        min_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(5),
        ..SimS3Faults::NONE
    });
    let start = Instant::now();
    put(&store, "k", "v").await;
    assert_eq!(start.elapsed(), Duration::from_millis(10));

    store.inject(
        Operation::GetObject,
        Fault::Delay(Duration::from_millis(50)),
    );
    let start = Instant::now();
    store.get_object(GetObject::new("k")).await.unwrap();
    assert_eq!(start.elapsed(), Duration::from_millis(55));
}

#[tokio::test]
async fn random_faults_hit_every_operation() {
    let store = store();
    put(&store, "k", "v").await;
    store.set_faults(SimS3Faults::OUTAGE);
    assert_eq!(store.faults(), SimS3Faults::OUTAGE);
    let internal = S3ErrorKind::InternalError;
    let upload_id = UploadId("upload".into());
    assert_eq!(
        kind(store.put_object(PutObject::new("k", "w")).await),
        internal
    );
    assert_eq!(kind(store.get_object(GetObject::new("k")).await), internal);
    assert_eq!(
        kind(store.head_object(HeadObject::new("k")).await),
        internal
    );
    assert_eq!(
        kind(store.delete_object(DeleteObject::new("k")).await),
        internal
    );
    assert_eq!(
        kind(store.list_objects_v2(ListObjectsV2::new("")).await),
        internal
    );
    assert_eq!(
        kind(store.copy_object(CopyObject::new("k", "c")).await),
        internal
    );
    assert_eq!(
        kind(
            store
                .create_multipart_upload(CreateMultipartUpload::new("m"))
                .await
        ),
        internal
    );
    let part = UploadPart {
        key: "m".into(),
        upload_id: upload_id.clone(),
        part_number: 1,
        body: Bytes::new(),
    };
    assert_eq!(kind(store.upload_part(part).await), internal);
    let request = complete("m", &upload_id, Vec::new(), WritePrecondition::None);
    assert_eq!(
        kind(store.complete_multipart_upload(request).await),
        internal
    );
    let abort = AbortMultipartUpload {
        key: "m".into(),
        upload_id: upload_id.clone(),
    };
    assert_eq!(kind(store.abort_multipart_upload(abort).await), internal);
    let list = ListParts::new("m", upload_id);
    assert_eq!(kind(store.list_parts(list).await), internal);
    assert_eq!(store.stats().internal_errors, 11);
    assert_eq!(body(&store, "k"), Some(Bytes::from("v")));

    // The outage ends.
    store.set_faults(SimS3Faults::NONE);
    assert!(store.get_object(GetObject::new("k")).await.is_ok());
    assert_eq!(store.stats().requests, 13);
}

#[tokio::test]
async fn random_faults_replay_with_the_seed() {
    let outcomes = |seed| async move {
        let store = SimS3::new(seed, config());
        store.set_faults(SimS3Faults {
            internal_error_probability: 0.1,
            slow_down_probability: 0.1,
            lost_request_probability: 0.1,
            lost_response_probability: 0.1,
            ..SimS3Faults::NONE
        });
        let mut outcomes = Vec::new();
        for i in 0..64 {
            let result = store.put_object(PutObject::new(format!("k{i}"), "v")).await;
            outcomes.push(result.err().map(|e| e.kind()));
        }
        (outcomes, store.keys(), store.stats())
    };
    let (first, keys, stats) = outcomes(3).await;
    assert_eq!(outcomes(3).await, (first.clone(), keys, stats));
    assert_ne!(outcomes(4).await.0, first);
    assert!(stats.internal_errors > 0 && stats.slow_downs > 0);
    assert!(stats.lost_requests > 0 && stats.lost_responses > 0);
}

#[test]
#[should_panic(expected = "fault probabilities")]
fn invalid_faults_are_refused() {
    store().set_faults(SimS3Faults {
        lost_request_probability: 2.0,
        ..SimS3Faults::NONE
    });
}

// Multipart uploads.

#[tokio::test]
async fn multipart_uploads_have_s3_etags() {
    let store = store();
    let mut metadata = UserMetadata::new();
    metadata.insert("skys3-wid", "c/b/1/2.3").unwrap();
    let upload_id = store
        .create_multipart_upload(
            CreateMultipartUpload::new("k")
                .with_metadata(metadata.clone())
                .with_content_type("a/b"),
        )
        .await
        .unwrap();
    let mut parts = Vec::new();
    for (part_number, body) in [(1, "aaaa"), (2, "bbbb"), (3, "c")] {
        let etag = store
            .upload_part(UploadPart {
                key: "k".into(),
                upload_id: upload_id.clone(),
                part_number,
                body: Bytes::from(body),
            })
            .await
            .unwrap();
        assert_eq!(etag.as_str(), md5_hex(body.as_bytes()));
        parts.push(CompletedPart { part_number, etag });
    }
    let written = store
        .complete_multipart_upload(complete("k", &upload_id, parts, WritePrecondition::None))
        .await
        .unwrap();

    let mut digests = Vec::new();
    for body in ["aaaa", "bbbb", "c"] {
        digests.extend_from_slice(&Md5::digest(body.as_bytes()));
    }
    assert_eq!(written.etag.as_str(), format!("{}-3", md5_hex(&digests)));
    let object = store.object("k").unwrap();
    assert_eq!(object.body, Bytes::from("aaaabbbbc"));
    assert_eq!(object.info.etag, written.etag);
    assert_eq!(object.info.metadata, metadata);
    assert_eq!(object.info.content_type.as_deref(), Some("a/b"));
    assert!(store.uploads().is_empty());
}

#[tokio::test]
async fn completing_uses_only_the_named_parts_and_the_latest_upload_of_each() {
    let store = store();
    let (upload_id, mut parts) = upload(&store, "k", &["aaaa", "skip", "c"]).await;
    let replaced = store
        .upload_part(UploadPart {
            key: "k".into(),
            upload_id: upload_id.clone(),
            part_number: 1,
            body: Bytes::from("AAAA"),
        })
        .await
        .unwrap();
    parts[0].etag = replaced;
    parts.remove(1);
    store
        .complete_multipart_upload(complete("k", &upload_id, parts, WritePrecondition::None))
        .await
        .unwrap();
    assert_eq!(body(&store, "k"), Some(Bytes::from("AAAAc")));
}

#[tokio::test]
async fn completing_checks_the_parts() {
    let store = store();
    let (upload_id, parts) = upload(&store, "k", &["aaaa", "bb", "c"]).await;
    let attempt = |parts: Vec<CompletedPart>| {
        store.complete_multipart_upload(complete("k", &upload_id, parts, WritePrecondition::None))
    };
    assert_eq!(kind(attempt(Vec::new()).await), S3ErrorKind::InvalidRequest);
    let reversed = vec![parts[1].clone(), parts[0].clone()];
    assert_eq!(kind(attempt(reversed).await), S3ErrorKind::InvalidPartOrder);
    let duplicated = vec![parts[0].clone(), parts[0].clone()];
    assert_eq!(
        kind(attempt(duplicated).await),
        S3ErrorKind::InvalidPartOrder
    );
    let wrong_etag = vec![CompletedPart {
        part_number: 1,
        etag: parts[1].etag.clone(),
    }];
    assert_eq!(kind(attempt(wrong_etag).await), S3ErrorKind::InvalidPart);
    let missing = vec![CompletedPart {
        part_number: 9,
        etag: parts[0].etag.clone(),
    }];
    assert_eq!(kind(attempt(missing).await), S3ErrorKind::InvalidPart);
    // Part 2 is below the minimum and is not last.
    assert_eq!(
        kind(attempt(parts.clone()).await),
        S3ErrorKind::EntityTooSmall
    );
    // As the last part it is fine.
    attempt(parts[..2].to_vec()).await.unwrap();
    assert_eq!(body(&store, "k"), Some(Bytes::from("aaaabb")));
}

#[tokio::test]
async fn uploads_are_found_by_id_and_key() {
    let store = store();
    let (upload_id, parts) = upload(&store, "k", &["a"]).await;
    let part = |key: &str, part_number| UploadPart {
        key: key.into(),
        upload_id: upload_id.clone(),
        part_number,
        body: Bytes::from("x"),
    };
    assert_eq!(
        kind(store.upload_part(part("other", 1)).await),
        S3ErrorKind::NoSuchUpload
    );
    assert_eq!(
        kind(store.upload_part(part("k", 0)).await),
        S3ErrorKind::InvalidArgument
    );
    assert_eq!(
        kind(store.upload_part(part("k", 10_001)).await),
        S3ErrorKind::InvalidArgument
    );
    assert!(store.upload_part(part("k", 10_000)).await.is_ok());

    store
        .complete_multipart_upload(complete(
            "k",
            &upload_id,
            parts.clone(),
            WritePrecondition::None,
        ))
        .await
        .unwrap();
    // A completed upload is gone, so a retried completion is 404.
    assert_eq!(
        kind(
            store
                .complete_multipart_upload(complete(
                    "k",
                    &upload_id,
                    parts,
                    WritePrecondition::None
                ))
                .await
        ),
        S3ErrorKind::NoSuchUpload
    );
}

#[tokio::test]
async fn aborting_discards_the_upload() {
    let store = store();
    let (upload_id, _) = upload(&store, "k", &["a"]).await;
    let (other, _) = upload(&store, "j", &["b"]).await;
    assert_eq!(
        store.uploads(),
        [(upload_id.clone(), "k".into()), (other, "j".into())]
    );
    let abort = |key: &str| AbortMultipartUpload {
        key: key.into(),
        upload_id: upload_id.clone(),
    };
    assert_eq!(
        kind(store.abort_multipart_upload(abort("j")).await),
        S3ErrorKind::NoSuchUpload
    );
    store.abort_multipart_upload(abort("k")).await.unwrap();
    assert_eq!(store.uploads().len(), 1);
    assert_eq!(
        kind(store.abort_multipart_upload(abort("k")).await),
        S3ErrorKind::NoSuchUpload
    );
    assert_eq!(
        kind(store.list_parts(ListParts::new("k", upload_id)).await),
        S3ErrorKind::NoSuchUpload
    );
    assert!(store.keys().is_empty());
}

#[tokio::test]
async fn list_parts_pages() {
    let store = store();
    let (upload_id, parts) = upload(&store, "k", &["a", "bb", "ccc"]).await;
    let page = |marker, max_parts| {
        store.list_parts(ListParts {
            part_number_marker: marker,
            max_parts,
            ..ListParts::new("k", upload_id.clone())
        })
    };
    let all = page(None, 1000).await.unwrap();
    assert!(!all.is_truncated);
    assert_eq!(all.next_part_number_marker, None);
    let listed: Vec<_> = all.parts.iter().map(|p| (p.part_number, p.size)).collect();
    assert_eq!(listed, [(1, 1), (2, 2), (3, 3)]);
    assert_eq!(all.parts[1].etag, parts[1].etag);

    let first = page(None, 2).await.unwrap();
    assert!(first.is_truncated);
    assert_eq!(first.parts.len(), 2);
    assert_eq!(first.next_part_number_marker, Some(2));
    let rest = page(Some(2), 2).await.unwrap();
    assert!(!rest.is_truncated);
    assert_eq!(rest.parts[0].part_number, 3);
}

// Copies.

#[tokio::test]
async fn copies_keep_or_replace_metadata() {
    let store = store();
    let mut metadata = UserMetadata::new();
    metadata.insert("skys3-wid", "c/b/1/2.3").unwrap();
    let source = store
        .put_object(
            PutObject::new("src", "data")
                .with_metadata(metadata.clone())
                .with_content_type("a/b"),
        )
        .await
        .unwrap();

    let copied = store
        .copy_object(CopyObject::new("src", "kept"))
        .await
        .unwrap();
    assert_eq!(copied.etag, source.etag);
    let kept = store.object("kept").unwrap();
    assert_eq!(kept.info.metadata, metadata);
    assert_eq!(kept.info.content_type.as_deref(), Some("a/b"));

    let mut replacement = UserMetadata::new();
    replacement.insert("skys3-wid", "c/b/1/2.4").unwrap();
    let replace =
        CopyObject::new("src", "replaced").with_metadata_directive(MetadataDirective::Replace {
            metadata: replacement.clone(),
            content_type: None,
        });
    store.copy_object(replace).await.unwrap();
    let replaced = store.object("replaced").unwrap();
    assert_eq!(replaced.info.metadata, replacement);
    assert_eq!(replaced.info.content_type, None);
    assert_eq!(replaced.body, Bytes::from("data"));

    assert_eq!(
        kind(store.copy_object(CopyObject::new("missing", "x")).await),
        S3ErrorKind::NoSuchKey
    );
}

#[tokio::test]
async fn copying_an_object_to_itself_must_replace_metadata() {
    let store = store();
    put(&store, "k", "v").await;
    assert_eq!(
        kind(store.copy_object(CopyObject::new("k", "k")).await),
        S3ErrorKind::InvalidRequest
    );
    let replace = CopyObject::new("k", "k").with_metadata_directive(MetadataDirective::Replace {
        metadata: UserMetadata::new(),
        content_type: Some("a/b".into()),
    });
    store.copy_object(replace).await.unwrap();
    let head = store.head_object(HeadObject::new("k")).await.unwrap();
    assert_eq!(head.content_type.as_deref(), Some("a/b"));
}

#[tokio::test]
async fn copies_of_multipart_objects_get_a_single_part_etag() {
    let store = store();
    let (upload_id, parts) = upload(&store, "mpu", &["aaaa", "b"]).await;
    store
        .complete_multipart_upload(complete("mpu", &upload_id, parts, WritePrecondition::None))
        .await
        .unwrap();
    let copied = store
        .copy_object(CopyObject::new("mpu", "copy"))
        .await
        .unwrap();
    assert_eq!(copied.etag.as_str(), md5_hex(b"aaaab"));
}

// Listing.

async fn populate(store: &SimS3, keys: &[&str]) {
    for key in keys {
        store
            .put_object(PutObject::new(*key, Bytes::copy_from_slice(key.as_bytes())))
            .await
            .unwrap();
    }
}

/// Lists everything under `prefix` in pages of `page` entries, and returns
/// the keys and common prefixes in order.
async fn list_all(
    store: &SimS3,
    request: ListObjectsV2,
    page: u32,
) -> (Vec<String>, Vec<String>, usize) {
    let mut keys = Vec::new();
    let mut prefixes = Vec::new();
    let mut pages = 0;
    let mut request = request.with_max_keys(page);
    loop {
        let output = store.list_objects_v2(request.clone()).await.unwrap();
        pages += 1;
        assert!(output.objects.len() + output.common_prefixes.len() <= page as usize);
        keys.extend(output.objects.into_iter().map(|o| o.key));
        prefixes.extend(output.common_prefixes);
        match output.next_continuation_token {
            Some(token) => {
                assert!(output.is_truncated);
                request.continuation_token = Some(token);
            }
            None => {
                assert!(!output.is_truncated);
                return (keys, prefixes, pages);
            }
        }
    }
}

const KEYS: &[&str] = &[
    "a", "b/1", "b/2", "b/3/x", "b/4", "c/1", "c/2", "c0", "d/e/f", "z",
];

#[tokio::test]
async fn listing_rolls_keys_up_by_delimiter() {
    let store = store();
    populate(&store, KEYS).await;
    let output = store
        .list_objects_v2(ListObjectsV2::new("").with_delimiter("/"))
        .await
        .unwrap();
    let keys: Vec<_> = output.objects.iter().map(|o| o.key.as_str()).collect();
    assert_eq!(keys, ["a", "c0", "z"]);
    assert_eq!(output.common_prefixes, ["b/", "c/", "d/"]);
    assert_eq!(
        output.objects[0],
        ListedObject {
            key: "a".into(),
            etag: etag(&md5_hex(b"a")),
            size: 1,
        }
    );

    let output = store
        .list_objects_v2(ListObjectsV2::new("b/").with_delimiter("/"))
        .await
        .unwrap();
    let keys: Vec<_> = output.objects.iter().map(|o| o.key.as_str()).collect();
    assert_eq!(keys, ["b/1", "b/2", "b/4"]);
    assert_eq!(output.common_prefixes, ["b/3/"]);

    // An empty delimiter is no delimiter.
    let output = store
        .list_objects_v2(ListObjectsV2::new("d").with_delimiter(""))
        .await
        .unwrap();
    assert_eq!(output.objects.len(), 1);
}

#[tokio::test]
async fn listing_pages_through_everything_exactly_once() {
    let store = store();
    populate(&store, KEYS).await;
    for (prefix, delimiter) in [("", None), ("", Some("/")), ("b/", Some("/")), ("c", None)] {
        let mut request = ListObjectsV2::new(prefix);
        request.delimiter = delimiter.map(str::to_owned);
        let expected = list_all(&store, request.clone(), 1000).await;
        assert_eq!(expected.2, 1);
        for page in 1..=11 {
            let paged = list_all(&store, request.clone(), page).await;
            assert_eq!((&paged.0, &paged.1), (&expected.0, &expected.1));
            let entries = expected.0.len() + expected.1.len();
            assert_eq!(paged.2, entries.div_ceil(page as usize).max(1));
        }
    }
}

#[tokio::test]
async fn listing_starts_after_a_key() {
    let store = store();
    populate(&store, KEYS).await;
    let (keys, _, _) = list_all(&store, ListObjectsV2::new("").with_start_after("c0"), 3).await;
    assert_eq!(keys, ["d/e/f", "z"]);
    // A start before the prefix starts at the prefix.
    let (keys, _, _) = list_all(&store, ListObjectsV2::new("c/").with_start_after("a"), 3).await;
    assert_eq!(keys, ["c/1", "c/2"]);
    // A token overrides start_after.
    let first = store
        .list_objects_v2(ListObjectsV2::new("").with_max_keys(1))
        .await
        .unwrap();
    let token = first.next_continuation_token.unwrap();
    let next = store
        .list_objects_v2(
            ListObjectsV2::new("")
                .with_max_keys(1)
                .with_start_after("y")
                .with_continuation_token(token),
        )
        .await
        .unwrap();
    assert_eq!(next.objects[0].key, "b/1");
}

#[tokio::test]
async fn listing_limits() {
    let store = store();
    populate(&store, KEYS).await;
    let empty = store
        .list_objects_v2(ListObjectsV2::new("").with_max_keys(0))
        .await
        .unwrap();
    assert_eq!(empty, ListObjectsV2Output::default());
    let all = store
        .list_objects_v2(ListObjectsV2::new("").with_max_keys(u32::MAX))
        .await
        .unwrap();
    assert_eq!(all.objects.len(), KEYS.len());
    assert_eq!(
        kind(
            store
                .list_objects_v2(ListObjectsV2::new("").with_continuation_token("forged"))
                .await
        ),
        S3ErrorKind::InvalidArgument
    );
}

#[tokio::test]
async fn start_after_inside_a_common_prefix_leaves_the_prefix_out() {
    let store = store();
    populate(&store, &["a", "b/1", "b/3", "c"]).await;
    let request = ListObjectsV2::new("")
        .with_delimiter("/")
        .with_start_after("b/2");
    let output = store.list_objects_v2(request.clone()).await.unwrap();
    // "b/" sorts before "b/2", so S3 leaves it out although "b/3" follows.
    let keys: Vec<_> = output.objects.iter().map(|o| o.key.as_str()).collect();
    assert_eq!(keys, ["c"]);
    assert!(output.common_prefixes.is_empty());
    // A start before the prefix keeps it.
    let output = store
        .list_objects_v2(request.with_start_after("b"))
        .await
        .unwrap();
    assert_eq!(output.common_prefixes, ["b/"]);
}

#[tokio::test]
async fn a_common_prefix_is_never_returned_on_two_pages() {
    let store = store();
    populate(&store, &["a/1", "a/2", "b/1", "b/2", "c"]).await;
    let request = ListObjectsV2::new("").with_delimiter("/");
    let (keys, prefixes, pages) = list_all(&store, request, 1).await;
    assert_eq!(keys, ["c"]);
    assert_eq!(prefixes, ["a/", "b/"]);
    assert_eq!(pages, 3);
}

#[tokio::test]
async fn list_parts_with_no_room_is_an_empty_final_page() {
    let store = store();
    let (upload_id, _) = upload(&store, "k", &["a", "b"]).await;
    let request = ListParts {
        max_parts: 0,
        ..ListParts::new("k", upload_id)
    };
    let output = store.list_parts(request).await.unwrap();
    assert_eq!(output, ListPartsOutput::default());
}

#[tokio::test]
async fn listing_caps_pages_at_1000_keys() {
    let store = store();
    for i in 0..1001 {
        put(&store, &format!("k{i:04}"), "").await;
    }
    let page = store
        .list_objects_v2(ListObjectsV2::new("").with_max_keys(5000))
        .await
        .unwrap();
    assert_eq!(page.objects.len(), 1000);
    assert!(page.is_truncated);
    let (keys, _, pages) = list_all(&store, ListObjectsV2::new(""), 1000).await;
    assert_eq!((keys.len(), pages), (1001, 2));
}

// Versioning.

#[tokio::test]
async fn versioned_buckets_keep_every_version() {
    let store = versioned();
    let v1 = put(&store, "k", "one").await;
    let v2 = put(&store, "k", "two").await;
    let (Some(id1), Some(id2)) = (v1.version_id.clone(), v2.version_id.clone()) else {
        panic!("a versioned bucket returns version IDs");
    };
    assert_ne!(id1, id2);

    let current = store.get_object(GetObject::new("k")).await.unwrap();
    assert_eq!(current.body, Bytes::from("two"));
    assert_eq!(current.info.version_id.as_ref(), Some(&id2));
    let old = store
        .get_object(GetObject::new("k").with_version_id(id1.clone()))
        .await
        .unwrap();
    assert_eq!(old.body, Bytes::from("one"));
    assert_eq!(old.info.version_id.as_ref(), Some(&id1));
    let head = store
        .head_object(HeadObject::new("k").with_version_id(id1.clone()))
        .await
        .unwrap();
    assert_eq!(head.etag, v1.etag);
    assert_eq!(
        kind(
            store
                .get_object(GetObject::new("k").with_version_id(VersionId("nope".into())))
                .await
        ),
        S3ErrorKind::NoSuchVersion
    );
}

#[tokio::test]
async fn versioned_deletes_add_and_remove_markers() {
    let store = versioned();
    let written = put(&store, "k", "v").await;
    let deleted = store.delete_object(DeleteObject::new("k")).await.unwrap();
    assert!(deleted.delete_marker);
    let marker = deleted.version_id.unwrap();

    assert_eq!(
        kind(store.get_object(GetObject::new("k")).await),
        S3ErrorKind::NoSuchKey
    );
    assert_eq!(
        kind(
            store
                .get_object(GetObject::new("k").with_version_id(marker.clone()))
                .await
        ),
        S3ErrorKind::MethodNotAllowed
    );
    assert!(store.keys().is_empty());
    assert_eq!(
        store.versions(),
        [
            ("k".to_owned(), written.version_id.clone(), false),
            ("k".to_owned(), Some(marker.clone()), true),
        ]
    );
    let listed = store.list_objects_v2(ListObjectsV2::new("")).await.unwrap();
    assert!(listed.objects.is_empty());
    // A delete marker is no object for If-Match.
    let delete = DeleteObject::new("k").with_if_match(written.etag.clone());
    assert_eq!(
        kind(store.delete_object(delete).await),
        S3ErrorKind::NoSuchKey
    );
    let create = PutObject::new("k", "w").with_precondition(WritePrecondition::IfAbsent);
    let recreated = store.put_object(create).await.unwrap();

    // Removing the newest version makes the marker current again, and
    // removing the marker restores the first object.
    let newest = recreated.version_id.unwrap();
    let removed = store
        .delete_object(DeleteObject::new("k").with_version_id(newest.clone()))
        .await
        .unwrap();
    assert_eq!(removed.version_id.as_ref(), Some(&newest));
    assert!(!removed.delete_marker);
    assert!(store.object("k").is_none());
    let removed = store
        .delete_object(DeleteObject::new("k").with_version_id(marker))
        .await
        .unwrap();
    assert!(removed.delete_marker);
    assert_eq!(body(&store, "k"), Some(Bytes::from("v")));
}

#[tokio::test]
async fn deleting_a_specific_version() {
    let store = versioned();
    let v1 = put(&store, "k", "one").await;
    let id1 = v1.version_id.clone().unwrap();
    let delete = |etag| {
        store.delete_object(
            DeleteObject::new("k")
                .with_version_id(id1.clone())
                .with_if_match(etag),
        )
    };
    assert_eq!(
        kind(delete(etag("0123")).await),
        S3ErrorKind::PreconditionFailed
    );
    delete(v1.etag.clone()).await.unwrap();
    assert!(store.object("k").is_none());
    // Removing a version that is gone changes nothing.
    let again = store
        .delete_object(DeleteObject::new("k").with_version_id(id1.clone()))
        .await
        .unwrap();
    assert!(!again.delete_marker);
    let unknown = store
        .delete_object(DeleteObject::new("other").with_version_id(id1))
        .await
        .unwrap();
    assert!(!unknown.delete_marker);
}

#[tokio::test]
async fn versioned_copies_read_a_specific_source_version() {
    let store = versioned();
    let v1 = put(&store, "src", "one").await;
    let id1 = v1.version_id.clone().unwrap();
    put(&store, "src", "two").await;
    let copy = CopyObject::new("src", "dst").with_source_version_id(id1.clone());
    let copied = store.copy_object(copy).await.unwrap();
    assert_eq!(copied.etag, v1.etag);
    assert!(copied.version_id.is_some());
    assert_eq!(body(&store, "dst"), Some(Bytes::from("one")));
    // Copying an old version onto its own key restores it, keeping its
    // metadata: only a copy of the current version must replace it.
    let restore = CopyObject::new("src", "src").with_source_version_id(id1);
    store.copy_object(restore).await.unwrap();
    assert_eq!(body(&store, "src"), Some(Bytes::from("one")));
}

#[tokio::test]
async fn version_ids_need_a_versioned_bucket() {
    let store = store();
    put(&store, "k", "v").await;
    let version = VersionId("v".into());
    let invalid = S3ErrorKind::InvalidArgument;
    assert_eq!(
        kind(
            store
                .get_object(GetObject::new("k").with_version_id(version.clone()))
                .await
        ),
        invalid
    );
    assert_eq!(
        kind(
            store
                .delete_object(DeleteObject::new("k").with_version_id(version))
                .await
        ),
        invalid
    );
}
