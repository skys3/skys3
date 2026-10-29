//! Bucket operations against the control store and the shard stub.

mod common;

use bytes::Bytes;
use common::{config, setup, setup_with};
use http::Method;
use skys3_control::faults::Fault;
use skys3_control::{ControlStore, Expected, PutOutcome, RegisterKey, TypedKey};
use skys3_gateway::stub::EntryState;
use skys3_gateway::{Gateway, IdSource, ShardError, ShardRef, Shards, TrustAll};
use skys3_types::{BucketMode, RegisterDocument};

#[tokio::test]
async fn a_local_bucket_is_created_listed_and_located() {
    let setup = setup("").await;
    let before = setup.generation().await;
    let created = setup.create("photos", "local", None).await;
    created.assert(200, None);
    assert_eq!(created.headers["location"], "/photos");

    let bucket = setup.register("photos").await.unwrap();
    assert_eq!(bucket.mode, BucketMode::Local);
    assert_eq!(bucket.shards.get(), 8);
    assert_eq!((bucket.replicas, bucket.min_write_replicas), (3, 2));
    assert!(bucket.target.is_none());
    assert!(bucket.created_unix_ms > 1_700_000_000_000);
    assert_eq!(bucket.bucket_id.as_str().len(), 25);
    assert!(
        setup.generation().await > before,
        "the creation was not announced"
    );
    assert_eq!(setup.shards.open_shards(&bucket.bucket_id).len(), 8);

    setup
        .call(Method::HEAD, "/photos", &[], "")
        .await
        .assert(200, None);
    let location = setup.call(Method::GET, "/photos?location", &[], "").await;
    location.assert(200, None);
    assert!(location.body.contains("LocationConstraint"), "{location:?}");
    let list = setup.call(Method::GET, "/", &[], "").await;
    list.assert(200, None);
    assert!(list.body.contains("<Name>photos</Name>"), "{list:?}");
    assert!(list.body.contains("<CreationDate>"), "{list:?}");

    let again = setup.create("photos", "local", None).await;
    again.assert(409, Some("BucketAlreadyOwnedByYou"));
}

#[tokio::test]
async fn missing_buckets_are_reported() {
    let setup = setup("").await;
    let head = setup.call(Method::HEAD, "/nothing", &[], "").await;
    assert_eq!(head.status, 404);
    for (method, uri) in [
        (Method::GET, "/nothing?location"),
        (Method::DELETE, "/nothing"),
        (Method::GET, "/nothing?versioning"),
        (Method::GET, "/nothing?encryption"),
        (Method::GET, "/nothing?ownershipControls"),
        (Method::GET, "/nothing/key?retention"),
    ] {
        let answer = setup.call(method.clone(), uri, &[], "").await;
        answer.assert(404, Some("NoSuchBucket"));
    }
}

#[tokio::test]
async fn bucket_names_are_validated() {
    let setup = setup("").await;
    for name in ["Bad", "a", "192.168.1.1"] {
        let answer = setup.create(name, "local", None).await;
        assert_eq!(answer.status, 400, "{name}: {answer:?}");
        assert_eq!(answer.code(), Some("InvalidBucketName"), "{name}");
    }
}

#[tokio::test]
async fn modes_and_targets_come_from_headers_or_configuration() {
    let setup = setup(
        "[buckets.defaults]\nshards_per_bucket = 4\nreplicas = 1\nmin_write_replicas = 1\n\
         [buckets.configured]\nmode = \"local\"\n",
    )
    .await;
    let bucket = setup.create_write_back("remote").await;
    let target = bucket.target.unwrap();
    assert_eq!(bucket.mode, BucketMode::WriteBack);
    assert_eq!(target.endpoint, "https://s3.example.com");
    assert_eq!(target.bucket, "remote");
    assert_eq!(target.prefix.as_deref(), Some("prefix/"));
    assert_eq!(bucket.shards.get(), 4);
    assert_eq!((bucket.replicas, bucket.clean_copies), (1, 1));

    // No mode header: the bucket's table says local.
    let answer = setup.call(Method::PUT, "/configured", &[], "").await;
    answer.assert(200, None);
    assert_eq!(
        setup.register("configured").await.unwrap().mode,
        BucketMode::Local
    );
    // The default mode is write_back, which needs a target.
    let answer = setup.call(Method::PUT, "/unconfigured", &[], "").await;
    answer.assert(400, Some("InvalidArgument"));
    assert!(answer.body.contains("x-skys3-bucket-target"), "{answer:?}");

    let cases = [
        (
            "write_back",
            Some("ftp://host/bucket"),
            400,
            "InvalidArgument",
        ),
        ("write_back", Some("https://host"), 400, "InvalidArgument"),
        ("local", Some("https://host/bucket"), 400, "InvalidArgument"),
        ("cache", None, 400, "InvalidArgument"),
        (
            "read_only",
            Some("https://host/bucket"),
            501,
            "NotImplemented",
        ),
    ];
    for (mode, target, status, code) in cases {
        let answer = setup.create("other", mode, target).await;
        assert_eq!(
            (answer.status.as_u16(), answer.code()),
            (status, Some(code)),
            "{mode}"
        );
    }
    let non_ascii = common::request(Method::PUT, "/other", &[], "");
    let mut non_ascii = non_ascii;
    non_ascii.headers_mut().insert(
        skys3_gateway::MODE_HEADER,
        http::HeaderValue::from_bytes(b"loc\xe4l").unwrap(),
    );
    setup
        .send(non_ascii)
        .await
        .assert(400, Some("InvalidArgument"));
    assert!(setup.register("other").await.is_none());
    // Refused creations opened no shards.
    assert_eq!(setup.shards.len(), 8);
}

#[tokio::test]
async fn a_target_in_the_control_stores_failure_scope_is_refused() {
    let config = config(
        "[control_store]\nbackend = \"s3\"\nendpoint = \"https://s3.us-west-2.amazonaws.com\"\n\
         bucket = \"control\"\n",
    );
    let setup = setup_with(config).await;
    let answer = setup
        .create(
            "data",
            "write_back",
            Some("https://s3.us-west-2.amazonaws.com/data"),
        )
        .await;
    answer.assert(400, Some("InvalidArgument"));
    assert!(answer.body.contains("failure scope"), "{answer:?}");
    setup
        .create(
            "data",
            "write_back",
            Some("https://s3.eu-west-1.amazonaws.com/data"),
        )
        .await
        .assert(200, None);
}

#[tokio::test]
async fn deleting_an_empty_bucket_detaches_it() {
    let setup = setup("").await;
    let bucket = setup.create_local("photos").await;
    let before = setup.generation().await;
    setup
        .call(Method::DELETE, "/photos", &[], "")
        .await
        .assert(204, None);
    assert!(setup.register("photos").await.is_none());
    assert!(setup.shards.open_shards(&bucket.bucket_id).is_empty());
    assert!(
        setup.generation().await > before,
        "the deletion was not announced"
    );
    assert_eq!(
        setup.call(Method::HEAD, "/photos", &[], "").await.status,
        404
    );
    let list = setup.call(Method::GET, "/", &[], "").await;
    assert!(!list.body.contains("photos"), "{list:?}");

    // A recreated bucket gets a new ID, and so new shards.
    let recreated = setup.create_local("photos").await;
    assert_ne!(recreated.bucket_id, bucket.bucket_id);
}

#[tokio::test]
async fn a_local_bucket_with_objects_is_not_deleted() {
    let setup = setup("").await;
    let bucket = setup.create_local("photos").await;
    let shard = setup
        .shards
        .put(&bucket, "cat.jpg", EntryState::Clean)
        .unwrap();
    assert_eq!(shard, ShardRef::for_key(&bucket, "cat.jpg"));
    let answer = setup.call(Method::DELETE, "/photos", &[], "").await;
    answer.assert(409, Some("BucketNotEmpty"));
    assert!(setup.register("photos").await.is_some());
    // The seals were lifted: writes go on.
    assert_eq!(setup.shards.is_sealed(&shard), Some(false));
    setup
        .shards
        .put(&bucket, "dog.jpg", EntryState::Dirty)
        .unwrap();
}

#[tokio::test]
async fn a_write_back_bucket_detaches_once_flushed() {
    let setup = setup("").await;
    let bucket = setup.create_write_back("remote").await;
    setup.shards.put(&bucket, "a", EntryState::Dirty).unwrap();
    setup
        .shards
        .put(&bucket, "b", EntryState::Tombstone)
        .unwrap();
    setup.shards.put(&bucket, "c", EntryState::Clean).unwrap();
    let answer = setup.call(Method::DELETE, "/remote", &[], "").await;
    answer.assert(409, Some("BucketNotEmpty"));
    assert!(answer.body.contains("2 changes"), "{answer:?}");

    // Clean entries live at the target, which detaching leaves alone.
    setup.shards.flush(&bucket.bucket_id);
    setup
        .call(Method::DELETE, "/remote", &[], "")
        .await
        .assert(204, None);
    assert!(setup.register("remote").await.is_none());
}

#[tokio::test]
async fn writes_are_refused_while_a_detach_decides() {
    let setup = setup("").await;
    let bucket = setup.create_local("photos").await;
    let (shards, sealed_bucket) = (setup.shards.clone(), bucket.clone());
    // The read, then a write attempted while the delete is in flight.
    setup.store.script([
        Fault::Pass,
        Fault::before(move || {
            let (shards, bucket) = (shards.clone(), sealed_bucket.clone());
            async move {
                let refused = shards.put(&bucket, "late", EntryState::Dirty);
                assert!(matches!(refused, Err(ShardError::Sealed(_))), "{refused:?}");
            }
        }),
    ]);
    setup
        .call(Method::DELETE, "/photos", &[], "")
        .await
        .assert(204, None);
    assert!(setup.register("photos").await.is_none());
}

#[tokio::test]
async fn a_creation_whose_answers_were_lost_is_announced_on_retry() {
    let setup = setup("").await;
    let before = setup.generation().await;
    // The create lands; its answer and the answers of both retries are lost.
    setup.store.script(vec![Fault::LoseResponse; 3]);
    let answer = setup.create("photos", "local", None).await;
    answer.assert(503, Some("ServiceUnavailable"));
    assert!(setup.register("photos").await.is_some());
    assert_eq!(setup.generation().await, before, "announced too early");

    let retry = setup.create("photos", "local", None).await;
    retry.assert(409, Some("BucketAlreadyOwnedByYou"));
    assert!(
        setup.generation().await > before,
        "the bucket was never announced"
    );
    setup
        .call(Method::HEAD, "/photos", &[], "")
        .await
        .assert(200, None);
}

#[tokio::test]
async fn a_deletion_whose_answers_were_lost_is_finished_on_retry() {
    let setup = setup("").await;
    let bucket = setup.create_local("photos").await;
    let other = Gateway::new(
        config(""),
        setup.memory.clone(),
        setup.shards.clone(),
        IdSource::seeded(8),
        TrustAll,
    )
    .await
    .unwrap();
    let head = || common::request(Method::HEAD, "/photos", &[], "");
    assert_eq!(other.handle(head()).await.status(), 200);
    let before = setup.generation().await;
    // The read passes; the delete lands, and every answer is lost.
    setup.store.script([
        Fault::Pass,
        Fault::LoseResponse,
        Fault::LoseResponse,
        Fault::LoseResponse,
    ]);
    let answer = setup.call(Method::DELETE, "/photos", &[], "").await;
    answer.assert(503, Some("ServiceUnavailable"));
    assert!(setup.register("photos").await.is_none());
    assert_eq!(setup.generation().await, before);
    let shard = ShardRef::for_key(&bucket, "k");
    assert_eq!(setup.shards.is_sealed(&shard), Some(true));

    // The retry finds the register gone, and finishes the detach.
    setup
        .call(Method::DELETE, "/photos", &[], "")
        .await
        .assert(204, None);
    assert!(setup.shards.open_shards(&bucket.bucket_id).is_empty());
    assert!(
        setup.generation().await > before,
        "the deletion was never announced"
    );
    other.reload_buckets().await.unwrap();
    assert_eq!(other.handle(head()).await.status(), 404);
    let again = setup.call(Method::DELETE, "/photos", &[], "").await;
    again.assert(404, Some("NoSuchBucket"));
}

#[tokio::test]
async fn a_lost_deletion_superseded_by_a_new_bucket_is_finished() {
    let setup = setup("").await;
    let old = setup.create_local("photos").await;
    setup.store.script([
        Fault::Pass,
        Fault::LoseResponse,
        Fault::LoseResponse,
        Fault::LoseResponse,
    ]);
    let answer = setup.call(Method::DELETE, "/photos", &[], "").await;
    answer.assert(503, Some("ServiceUnavailable"));
    // Another gateway creates a new bucket under the name.
    let mut new = old.clone();
    new.bucket_id = "b-new".parse().unwrap();
    new.proposal_id = "new".parse().unwrap();
    for shard in ShardRef::all(&new) {
        setup.shards.open(&shard, &new).await.unwrap();
    }
    let key = TypedKey::bucket(&new.name);
    let value = Bytes::from(new.to_json().unwrap());
    let written = setup.memory.put_if(key.key(), Expected::Absent, value);
    assert!(matches!(written.await.unwrap(), PutOutcome::Written(_)));

    setup
        .call(Method::DELETE, "/photos", &[], "")
        .await
        .assert(204, None);
    assert!(setup.shards.open_shards(&old.bucket_id).is_empty());
    assert!(setup.shards.open_shards(&new.bucket_id).is_empty());
}

#[tokio::test]
async fn seals_of_a_lost_deletion_are_lifted_once_it_cannot_apply() {
    let setup = setup("").await;
    let bucket = setup.create_local("photos").await;
    let shard = ShardRef::for_key(&bucket, "k");
    // The delete never lands, but nothing says so.
    setup.store.script([
        Fault::Pass,
        Fault::LoseRequest,
        Fault::LoseRequest,
        Fault::LoseRequest,
    ]);
    let answer = setup.call(Method::DELETE, "/photos", &[], "").await;
    answer.assert(503, Some("ServiceUnavailable"));
    assert_eq!(setup.shards.is_sealed(&shard), Some(true));

    // A retry that applies nothing keeps the earlier seal: the earlier
    // delete may still land.
    setup.store.script([
        Fault::Pass,
        Fault::Unavailable,
        Fault::Unavailable,
        Fault::Unavailable,
    ]);
    let answer = setup.call(Method::DELETE, "/photos", &[], "").await;
    answer.assert(503, Some("ServiceUnavailable"));
    assert_eq!(setup.shards.is_sealed(&shard), Some(true));

    // Once the register moves on, no earlier delete can apply.
    let key = TypedKey::bucket(&bucket.name);
    let current = setup.memory.get(key.key()).await.unwrap().unwrap();
    let mut rewritten = bucket.clone();
    rewritten.proposal_id = "rewritten".parse().unwrap();
    let value = Bytes::from(rewritten.to_json().unwrap());
    let written = setup
        .memory
        .put_if(key.key(), Expected::Version(current.version), value);
    assert!(matches!(written.await.unwrap(), PutOutcome::Written(_)));
    setup.store.script([
        Fault::Pass,
        Fault::Unavailable,
        Fault::Unavailable,
        Fault::Unavailable,
    ]);
    let answer = setup.call(Method::DELETE, "/photos", &[], "").await;
    answer.assert(503, Some("ServiceUnavailable"));
    assert_eq!(setup.shards.is_sealed(&shard), Some(false));
    setup.shards.put(&bucket, "k", EntryState::Dirty).unwrap();
    let refused = setup.call(Method::DELETE, "/photos", &[], "").await;
    refused.assert(409, Some("BucketNotEmpty"));
}

#[tokio::test]
async fn a_lost_delete_answer_still_detaches() {
    let setup = setup("").await;
    let bucket = setup.create_local("photos").await;
    setup.store.script([Fault::Pass, Fault::LoseResponse]);
    setup
        .call(Method::DELETE, "/photos", &[], "")
        .await
        .assert(204, None);
    assert!(setup.register("photos").await.is_none());
    assert!(setup.shards.open_shards(&bucket.bucket_id).is_empty());
}

#[tokio::test]
async fn a_bucket_changed_during_a_detach_is_kept() {
    let setup = setup("").await;
    let bucket = setup.create_local("photos").await;
    let memory = setup.memory.clone();
    // Another writer rewrites the register between the read and the delete.
    setup.store.script([
        Fault::Pass,
        Fault::before(move || {
            let memory = memory.clone();
            async move {
                let key = RegisterKey::bucket(&"photos".parse().unwrap());
                let current = memory.get(&key).await.unwrap().unwrap();
                let mut document = skys3_types::BucketDocument::from_json(&current.value).unwrap();
                document.proposal_id = "rewritten".parse().unwrap();
                let value = Bytes::from(document.to_json().unwrap());
                let written = memory.put_if(&key, Expected::Version(current.version), value);
                assert!(matches!(written.await.unwrap(), PutOutcome::Written(_)));
            }
        }),
    ]);
    let answer = setup.call(Method::DELETE, "/photos", &[], "").await;
    answer.assert(409, Some("OperationAborted"));
    assert!(setup.register("photos").await.is_some());
    let shard = ShardRef::for_key(&bucket, "k");
    assert_eq!(setup.shards.is_sealed(&shard), Some(false));
}

#[tokio::test]
async fn a_bucket_deleted_by_another_node_is_missing() {
    let setup = setup("").await;
    setup.create_local("photos").await;
    let memory = setup.memory.clone();
    setup.store.script([
        Fault::Pass,
        Fault::before(move || {
            let memory = memory.clone();
            async move {
                let key = RegisterKey::bucket(&"photos".parse().unwrap());
                let current = memory.get(&key).await.unwrap().unwrap();
                let deleted = memory.delete_if(&key, &current.version).await.unwrap();
                assert_eq!(deleted, skys3_control::DeleteOutcome::Deleted);
            }
        }),
    ]);
    let answer = setup.call(Method::DELETE, "/photos", &[], "").await;
    answer.assert(404, Some("NoSuchBucket"));
}

#[tokio::test]
async fn a_lost_creation_race_removes_its_shards() {
    let setup = setup("").await;
    let winner = setup.create_local("winner").await;
    let memory = setup.memory.clone();
    let mut theirs = winner.clone();
    theirs.name = "photos".parse().unwrap();
    theirs.bucket_id = "b-theirs".parse().unwrap();
    setup.store.script([Fault::before(move || {
        let (memory, theirs) = (memory.clone(), theirs.clone());
        async move {
            let key = TypedKey::bucket(&theirs.name);
            let value = Bytes::from(theirs.to_json().unwrap());
            let written = memory.put_if(key.key(), Expected::Absent, value).await;
            assert!(matches!(written.unwrap(), PutOutcome::Written(_)));
        }
    })]);
    let answer = setup.create("photos", "local", None).await;
    answer.assert(409, Some("BucketAlreadyOwnedByYou"));
    let bucket = setup.register("photos").await.unwrap();
    assert_eq!(bucket.bucket_id.as_str(), "b-theirs");
    // Only the first bucket's shards remain, and the gateway learned the
    // winner.
    assert_eq!(setup.shards.len(), 8);
    assert_eq!(setup.shards.open_shards(&winner.bucket_id).len(), 8);
    setup
        .call(Method::HEAD, "/photos", &[], "")
        .await
        .assert(200, None);
}

#[tokio::test]
async fn control_store_outages_are_service_unavailable() {
    let setup = setup("").await;
    setup.store.script(vec![Fault::Unavailable; 3]);
    let answer = setup.create("photos", "local", None).await;
    answer.assert(503, Some("ServiceUnavailable"));
    assert!(setup.register("photos").await.is_none());

    // A creation that may have landed keeps its shards; here it did land.
    setup.store.script(vec![
        Fault::LoseResponse,
        Fault::LoseResponse,
        Fault::LoseResponse,
    ]);
    let answer = setup.create("maybe", "local", None).await;
    answer.assert(503, Some("ServiceUnavailable"));
    let landed = setup.register("maybe").await.unwrap();
    assert_eq!(setup.shards.open_shards(&landed.bucket_id).len(), 8);
    setup.gateway.reload_buckets().await.unwrap();
    setup
        .call(Method::HEAD, "/maybe", &[], "")
        .await
        .assert(200, None);

    let bucket = setup.create_local("kept").await;
    // The read fails.
    setup.store.script(vec![Fault::Unavailable; 3]);
    let answer = setup.call(Method::DELETE, "/kept", &[], "").await;
    answer.assert(503, Some("ServiceUnavailable"));
    // The delete cannot have landed, so the seals are lifted.
    setup.store.script([Fault::Pass]);
    setup.store.script(vec![Fault::Unavailable; 3]);
    let answer = setup.call(Method::DELETE, "/kept", &[], "").await;
    answer.assert(503, Some("ServiceUnavailable"));
    assert_eq!(
        setup.shards.is_sealed(&ShardRef::for_key(&bucket, "k")),
        Some(false)
    );

    // A delete that may have landed keeps the shards sealed.
    setup.store.script([
        Fault::Pass,
        Fault::LoseRequest,
        Fault::LoseRequest,
        Fault::LoseRequest,
    ]);
    let answer = setup.call(Method::DELETE, "/kept", &[], "").await;
    answer.assert(503, Some("ServiceUnavailable"));
    assert_eq!(
        setup.shards.is_sealed(&ShardRef::for_key(&bucket, "k")),
        Some(true)
    );
    // A retry resolves it.
    setup
        .call(Method::DELETE, "/kept", &[], "")
        .await
        .assert(204, None);

    setup.store.script([Fault::Fail]);
    let answer = setup.call(Method::DELETE, "/maybe", &[], "").await;
    answer.assert(500, Some("InternalError"));
}

#[tokio::test]
async fn shard_outages_are_service_unavailable() {
    let setup = setup("").await;
    let bucket = setup.create_local("photos").await;
    setup.shards.set_unavailable(true);
    setup
        .create("other", "local", None)
        .await
        .assert(503, Some("ServiceUnavailable"));
    assert!(setup.register("other").await.is_none());
    let answer = setup.call(Method::DELETE, "/photos", &[], "").await;
    answer.assert(503, Some("ServiceUnavailable"));
    setup.shards.set_unavailable(false);
    assert_eq!(
        setup.shards.is_sealed(&ShardRef::for_key(&bucket, "k")),
        Some(false)
    );
    // A missing shard fails the delete and lifts the seals already taken.
    let last = ShardRef::all(&bucket).last().unwrap();
    setup.shards.remove(&last).await.unwrap();
    let answer = setup.call(Method::DELETE, "/photos", &[], "").await;
    answer.assert(503, Some("ServiceUnavailable"));
    assert_eq!(
        setup
            .shards
            .is_sealed(&ShardRef::all(&bucket).next().unwrap()),
        Some(false)
    );
}

#[tokio::test]
async fn list_buckets_pages_and_filters() {
    let setup = setup("").await;
    for name in ["alpha", "beta", "bravo", "charlie"] {
        setup.create_local(name).await;
    }
    let names = |body: &str| -> Vec<String> {
        body.split("<Name>")
            .skip(1)
            .map(|rest| rest.split("</Name>").next().unwrap().to_owned())
            .collect()
    };
    let page = setup.call(Method::GET, "/?max-buckets=2", &[], "").await;
    page.assert(200, None);
    assert_eq!(names(&page.body), ["alpha", "beta"]);
    assert!(
        page.body
            .contains("<ContinuationToken>beta</ContinuationToken>"),
        "{page:?}"
    );
    let rest = setup
        .call(
            Method::GET,
            "/?max-buckets=2&continuation-token=beta",
            &[],
            "",
        )
        .await;
    assert_eq!(names(&rest.body), ["bravo", "charlie"]);
    assert!(!rest.body.contains("ContinuationToken"), "{rest:?}");
    let prefixed = setup.call(Method::GET, "/?prefix=b", &[], "").await;
    assert_eq!(names(&prefixed.body), ["beta", "bravo"]);
    let huge = setup
        .call(Method::GET, "/?max-buckets=1000000", &[], "")
        .await;
    assert_eq!(names(&huge.body).len(), 4);
    let zero = setup.call(Method::GET, "/?max-buckets=0", &[], "").await;
    zero.assert(400, Some("InvalidArgument"));
}

#[tokio::test]
async fn gateways_share_buckets_through_the_control_store() {
    let setup = setup("").await;
    let other = Gateway::new(
        config(""),
        setup.memory.clone(),
        setup.shards.clone(),
        IdSource::seeded(8),
        TrustAll,
    )
    .await
    .unwrap();
    setup.create_local("photos").await;
    let head = || common::request(Method::HEAD, "/photos", &[], "");
    assert_eq!(other.handle(head()).await.status(), 404);
    other.reload_buckets().await.unwrap();
    assert_eq!(other.handle(head()).await.status(), 200);

    // A register that is not a valid document stops a gateway from loading.
    let key = RegisterKey::new("buckets/broken.json").unwrap();
    let written = setup
        .memory
        .put_if(&key, Expected::Absent, Bytes::from_static(b"{}"));
    assert!(matches!(written.await.unwrap(), PutOutcome::Written(_)));
    assert!(other.reload_buckets().await.is_err());
    let loaded = Gateway::new(
        config(""),
        setup.memory.clone(),
        setup.shards.clone(),
        IdSource::seeded(9),
        TrustAll,
    )
    .await;
    assert!(loaded.is_err());
    // Registers outside the bucket layout are ignored.
    let stray = RegisterKey::new("buckets/x/y.json").unwrap();
    let written = setup
        .memory
        .put_if(&stray, Expected::Absent, Bytes::from_static(b"{}"));
    assert!(matches!(written.await.unwrap(), PutOutcome::Written(_)));
}
