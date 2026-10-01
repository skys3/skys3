//! The S3 backend over the simulated S3 store: object keys, how S3 answers
//! map to the control-store contract, lost responses, listing, and the
//! conditional poll of `cluster.json`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use skys3_control::faults::{Fault as ControlFault, FaultyStore};
use skys3_control::{
    ChangeStream, ControlError, ControlStore, DeleteOutcome, DeletionOutcome, Expected, KeyPrefix,
    ProposalIds, ProposalOutcome, PutOutcome, RegisterKey, RetryPolicy, S3ControlStore,
    S3StoreConfig, Version, bootstrap, bump_generation, proposal_id_of, propose, propose_delete,
};
use skys3_remote::{
    AbortMultipartUpload, CompleteMultipartUpload, CopyObject, CreateMultipartUpload, DeleteObject,
    DeleteOutput, GetObject, GetOutput, HeadObject, ListObjectsV2, ListObjectsV2Output, ListParts,
    ListPartsOutput, ObjectInfo, ObjectStore, PutObject, S3Result, UploadId, UploadPart,
    WriteOutput,
};
use skys3_sim::SimS3;
use skys3_sim::s3::{ConditionalSupport, Conditionals, Fault, Operation, SimS3Config};
use skys3_types::{ETag, Generation, ProposalId};

const PREFIX: &str = "skys3-prod-a/";

fn config() -> S3StoreConfig {
    S3StoreConfig {
        prefix: PREFIX.to_owned(),
        poll_interval: Duration::from_secs(30),
    }
}

fn store_with(conditionals: Conditionals) -> S3ControlStore<SimS3> {
    let objects = SimS3::new(
        7,
        SimS3Config {
            conditionals,
            ..SimS3Config::default()
        },
    );
    S3ControlStore::new(objects, config()).unwrap()
}

fn store() -> S3ControlStore<SimS3> {
    store_with(Conditionals::AWS_S3)
}

fn key(key: &str) -> RegisterKey {
    RegisterKey::new(key).unwrap()
}

fn value(proposal: &ProposalId) -> Bytes {
    Bytes::from(format!(r#"{{"proposal_id":"{proposal}"}}"#))
}

async fn create(store: &S3ControlStore<SimS3>, k: &str) -> Version {
    let id = ProposalId::from_u128(k.len() as u128);
    match store
        .put_if(&key(k), Expected::Absent, value(&id))
        .await
        .unwrap()
    {
        PutOutcome::Written(version) => version,
        PutOutcome::PreconditionFailed => panic!("{k} exists"),
    }
}

#[test]
fn prefixes_must_end_with_a_slash_and_leave_room_for_keys() {
    let objects = SimS3::new(0, SimS3Config::default());
    for prefix in ["", "a/", "skys3-prod-a/control/"] {
        let config = S3StoreConfig {
            prefix: prefix.to_owned(),
            ..config()
        };
        assert!(
            S3ControlStore::new(objects.clone(), config).is_ok(),
            "{prefix}"
        );
    }
    for prefix in ["a".to_owned(), format!("{}/", "p".repeat(512))] {
        let config = S3StoreConfig {
            prefix: prefix.clone(),
            ..config()
        };
        let error = S3ControlStore::new(objects.clone(), config).unwrap_err();
        assert_eq!(error.key(), prefix);
    }
}

#[tokio::test]
async fn registers_are_json_objects_under_the_prefix() {
    let store = store();
    let register = key("buckets/photos.json");
    assert_eq!(
        store.object_key(&register),
        "skys3-prod-a/buckets/photos.json"
    );
    let version = create(&store, "buckets/photos.json").await;
    let object = store
        .objects()
        .object(&store.object_key(&register))
        .unwrap();
    assert_eq!(object.info.etag.as_str(), version.as_str());
    assert_eq!(
        object.info.content_type.as_deref(),
        Some("application/json")
    );
    assert_eq!(store.objects().keys(), ["skys3-prod-a/buckets/photos.json"]);
    assert_eq!(store.config(), &config());
    assert!(format!("{store:?}").contains("S3ControlStore"));
}

#[tokio::test]
async fn s3_answers_map_to_the_contract() {
    let store = store();
    let register = key("nodes/node-1.json");
    let id = ProposalId::from_u128(1);
    let cases = [
        (Operation::PutObject, Fault::Conflict, "conflict"),
        (Operation::PutObject, Fault::InternalError, "indeterminate"),
        (Operation::PutObject, Fault::LostRequest, "indeterminate"),
        (Operation::PutObject, Fault::SlowDown, "unavailable"),
    ];
    for (operation, fault, expected) in cases {
        store.objects().inject(operation, fault);
        let error = store
            .put_if(&register, Expected::Absent, value(&id))
            .await
            .unwrap_err();
        let found = match error {
            ControlError::Conflict(ref k) if *k == register => "conflict",
            ControlError::Indeterminate(_) => "indeterminate",
            ControlError::Unavailable(_) => "unavailable",
            _ => "other",
        };
        assert_eq!(found, expected, "{fault:?}: {error}");
    }
    let version = create(&store, "nodes/node-1.json").await;
    for operation in [Operation::GetObject, Operation::ListObjectsV2] {
        // A 409 to a request that is not a conditional write is transient.
        store.objects().inject(operation, Fault::Conflict);
    }
    assert!(matches!(
        store.get(&register).await,
        Err(ControlError::Unavailable(_))
    ));
    assert!(matches!(
        store.list(&KeyPrefix::root()).await,
        Err(ControlError::Unavailable(_))
    ));
    store
        .objects()
        .inject(Operation::DeleteObject, Fault::Conflict);
    assert!(matches!(
        store.delete_if(&register, &version).await,
        Err(ControlError::Conflict(_))
    ));
    // A version that is not an entity tag matches no object.
    let bad = Version::new("not an etag");
    let written = store
        .put_if(&register, Expected::Version(bad.clone()), value(&id))
        .await;
    assert_eq!(written.unwrap(), PutOutcome::PreconditionFailed);
    let deleted = store.delete_if(&register, &bad).await;
    assert_eq!(deleted.unwrap(), DeleteOutcome::PreconditionFailed);
    // Neither sent a request.
    assert_eq!(store.objects().stats().requests, 8);
}

#[tokio::test]
async fn unsupported_preconditions_are_rejected() {
    let store = store_with(Conditionals::all(ConditionalSupport::Rejected));
    let register = key("cluster.json");
    let error = store
        .put_if(
            &register,
            Expected::Absent,
            value(&ProposalId::from_u128(1)),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, ControlError::Rejected(_)), "{error}");
    assert!(!error.is_retryable() && !error.may_have_applied());
    // propose gives up at once.
    let id = ProposalId::from_u128(2);
    let outcome = propose(
        &store,
        &register,
        Expected::Absent,
        value(&id),
        &id,
        &RetryPolicy::default(),
    )
    .await;
    assert!(matches!(outcome, Err(ControlError::Rejected(_))));
}

#[tokio::test(start_paused = true)]
async fn lost_s3_responses_resolve_by_proposal_id() {
    let store = store();
    let register = key("coordinator.lease");
    let policy = RetryPolicy::default();
    let mut ids = ProposalIds::seeded(1);

    // The create lands and its answer is lost; the retry gets 412, and the
    // re-read finds the proposal.
    store
        .objects()
        .inject(Operation::PutObject, Fault::LostResponse);
    let proposal = ids.next_id();
    let outcome = propose(
        &store,
        &register,
        Expected::Absent,
        value(&proposal),
        &proposal,
        &policy,
    );
    let ProposalOutcome::Accepted(v1) = outcome.await.unwrap() else {
        panic!("a landed create was rejected");
    };
    // The same for an update, with the re-read's answer lost too.
    store
        .objects()
        .inject(Operation::PutObject, Fault::LostResponse);
    store
        .objects()
        .inject(Operation::GetObject, Fault::InternalError);
    let proposal = ids.next_id();
    let expected = Expected::Version(v1.clone());
    let outcome = propose(
        &store,
        &register,
        expected,
        value(&proposal),
        &proposal,
        &policy,
    );
    let ProposalOutcome::Accepted(v2) = outcome.await.unwrap() else {
        panic!("a landed update was rejected");
    };
    let stored = store.get(&register).await.unwrap().unwrap();
    assert_eq!(stored.version, v2);
    assert_eq!(proposal_id_of(&stored.value), Some(proposal));

    // A lost request applied nothing and is sent again.
    store
        .objects()
        .inject(Operation::PutObject, Fault::LostRequest);
    let proposal = ids.next_id();
    let outcome = propose(
        &store,
        &register,
        Expected::Version(v2.clone()),
        value(&proposal),
        &proposal,
        &policy,
    );
    let ProposalOutcome::Accepted(v3) = outcome.await.unwrap() else {
        panic!("a resent update was rejected");
    };

    // A delete whose answer is lost resolves by the register's absence.
    store
        .objects()
        .inject(Operation::DeleteObject, Fault::LostResponse);
    let deleted = propose_delete(&store, &register, &v3, &policy).await;
    assert_eq!(deleted.unwrap(), DeletionOutcome::Deleted);
    assert_eq!(store.get(&register).await.unwrap(), None);
    assert!(store.objects().stats().lost_responses >= 3);
}

#[tokio::test(start_paused = true)]
async fn faults_around_the_s3_store_resolve_like_any_backend() {
    let faulty = FaultyStore::new(store());
    let register = key("buckets/b.json");
    let policy = RetryPolicy::default();
    faulty.script([
        ControlFault::LoseResponse,
        ControlFault::LateRequest(Duration::from_millis(5)),
        ControlFault::Conflict,
    ]);
    let proposal = ProposalId::from_u128(9);
    let outcome = propose(
        &faulty,
        &register,
        Expected::Absent,
        value(&proposal),
        &proposal,
        &policy,
    );
    assert!(matches!(
        outcome.await.unwrap(),
        ProposalOutcome::Accepted(_)
    ));
    let stored = faulty.inner().get(&register).await.unwrap().unwrap();
    assert_eq!(proposal_id_of(&stored.value), Some(proposal));
}

#[tokio::test]
async fn listings_page_through_every_register_under_the_prefix() {
    let store = store();
    let mut written = Vec::new();
    for n in 0..1_005 {
        let k = format!("shards/b-1/{n}.json");
        written.push((key(&k), create(&store, &k).await));
    }
    create(&store, "nodes/node-1.json").await;
    // An object outside the prefix is not a register.
    store
        .objects()
        .put_object(PutObject::new("elsewhere/shards/b-1/0.json", "{}"))
        .await
        .unwrap();
    written.sort_by(|a, b| a.0.cmp(&b.0));
    let listed = store.list(&KeyPrefix::shards()).await.unwrap();
    assert_eq!(listed, written);
    assert_eq!(store.list(&KeyPrefix::root()).await.unwrap().len(), 1_006);
}

#[tokio::test]
async fn an_object_that_is_not_a_register_fails_the_listing() {
    let store = store();
    store
        .objects()
        .put_object(PutObject::new(format!("{PREFIX}nodes/.hidden"), "{}"))
        .await
        .unwrap();
    let error = store.list(&KeyPrefix::nodes()).await.unwrap_err();
    assert!(matches!(error, ControlError::InvalidKey(_)), "{error}");
}

/// The key and `If-None-Match` of each `GetObject`.
type Gets = Arc<Mutex<Vec<(String, Option<ETag>)>>>;

/// An object store that records the `If-None-Match` of every `GetObject`.
#[derive(Debug)]
struct Recording {
    inner: SimS3,
    gets: Gets,
}

impl ObjectStore for Recording {
    async fn put_object(&self, request: PutObject) -> S3Result<WriteOutput> {
        self.inner.put_object(request).await
    }

    async fn get_object(&self, request: GetObject) -> S3Result<GetOutput> {
        let entry = (request.key.clone(), request.if_none_match.clone());
        self.gets.lock().unwrap().push(entry);
        self.inner.get_object(request).await
    }

    async fn head_object(&self, request: HeadObject) -> S3Result<ObjectInfo> {
        self.inner.head_object(request).await
    }

    async fn delete_object(&self, request: DeleteObject) -> S3Result<DeleteOutput> {
        self.inner.delete_object(request).await
    }

    async fn list_objects_v2(&self, request: ListObjectsV2) -> S3Result<ListObjectsV2Output> {
        self.inner.list_objects_v2(request).await
    }

    async fn copy_object(&self, request: CopyObject) -> S3Result<WriteOutput> {
        self.inner.copy_object(request).await
    }

    async fn create_multipart_upload(&self, request: CreateMultipartUpload) -> S3Result<UploadId> {
        self.inner.create_multipart_upload(request).await
    }

    async fn upload_part(&self, request: UploadPart) -> S3Result<ETag> {
        self.inner.upload_part(request).await
    }

    async fn complete_multipart_upload(
        &self,
        request: CompleteMultipartUpload,
    ) -> S3Result<WriteOutput> {
        self.inner.complete_multipart_upload(request).await
    }

    async fn abort_multipart_upload(&self, request: AbortMultipartUpload) -> S3Result<()> {
        self.inner.abort_multipart_upload(request).await
    }

    async fn list_parts(&self, request: ListParts) -> S3Result<ListPartsOutput> {
        self.inner.list_parts(request).await
    }
}

#[tokio::test(start_paused = true)]
async fn change_streams_poll_cluster_json_with_if_none_match() {
    let gets = Arc::new(Mutex::new(Vec::new()));
    let objects = Recording {
        inner: SimS3::new(1, SimS3Config::default()),
        gets: Arc::clone(&gets),
    };
    let store = S3ControlStore::new(objects, config()).unwrap();
    let cluster = "skys3-prod-a".parse().unwrap();
    let policy = RetryPolicy::default();
    let mut ids = ProposalIds::seeded(3);
    bootstrap(&store, &cluster, ids.next_id(), &policy)
        .await
        .unwrap();
    let mut changes = store.changes(Generation::ZERO).await.unwrap();
    let first = changes.next().await.unwrap();
    assert!(first.snapshot);
    gets.lock().unwrap().clear();

    // Three polls find nothing new, each with one conditional GET.
    let waited = tokio::time::timeout(Duration::from_secs(95), changes.next()).await;
    assert!(waited.is_err());
    let cluster_key = format!("{PREFIX}cluster.json");
    let etag = store
        .objects()
        .inner
        .object(&cluster_key)
        .unwrap()
        .info
        .etag;
    assert_eq!(
        *gets.lock().unwrap(),
        vec![(cluster_key.clone(), Some(etag)); 3]
    );

    // A write and an increment are reported at the next poll.
    let version = create_on(&store, "buckets/b.json").await;
    let generation = bump_generation(&store, &cluster, &mut ids, &policy)
        .await
        .unwrap();
    let start = tokio::time::Instant::now();
    let change = changes.next().await.unwrap();
    assert!(start.elapsed() <= Duration::from_secs(30));
    assert_eq!(change.generation, generation);
    assert_eq!(change.registers, [(key("buckets/b.json"), Some(version))]);

    // A poll that finds cluster.json gone reports it.
    let current = store.get(&RegisterKey::cluster()).await.unwrap().unwrap();
    let deleted = store
        .delete_if(&RegisterKey::cluster(), &current.version)
        .await;
    assert_eq!(deleted.unwrap(), DeleteOutcome::Deleted);
    let error = changes.next().await.unwrap_err();
    assert!(matches!(error, ControlError::NotBootstrapped), "{error}");
}

async fn create_on<O: ObjectStore>(store: &S3ControlStore<O>, k: &str) -> Version {
    let id = ProposalId::from_u128(42);
    match store
        .put_if(&key(k), Expected::Absent, value(&id))
        .await
        .unwrap()
    {
        PutOutcome::Written(version) => version,
        PutOutcome::PreconditionFailed => panic!("{k} exists"),
    }
}
