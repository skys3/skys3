//! The etcd backend against the in-process [`Fake`]: what it does when
//! etcd answers with errors, does not answer, breaks the protocol, or ends
//! a watch. The tests against a real etcd are in `tests/etcd.rs`.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use skys3_types::{ClusterId, Generation};

use super::fake::{Fake, Fault, WatchFault};
use super::*;
use crate::cluster::{bootstrap, bump_generation};
use crate::conformance::{self, Backend};
use crate::propose::{ProposalIds, RetryPolicy};
use crate::store::ChangeStream;

/// Bounds every test, so a hang fails it.
async fn bounded<T>(test: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(60), test)
        .await
        .expect("the test finished in time")
}

/// A short request timeout, for faults that never answer.
const TIMEOUT: Duration = Duration::from_secs(1);

fn config(urls: &[String]) -> EtcdStoreConfig {
    EtcdStoreConfig {
        request_timeout: TIMEOUT,
        ..EtcdStoreConfig::new(urls.to_vec(), "c/")
    }
}

fn store(fake: &Fake) -> EtcdControlStore {
    EtcdControlStore::new(config(&[fake.url("http")])).unwrap()
}

fn key() -> RegisterKey {
    RegisterKey::new("buckets/b.json").unwrap()
}

fn value() -> Bytes {
    Bytes::from_static(br#"{"proposal_id":"p"}"#)
}

fn cluster() -> ClusterId {
    "c".parse().unwrap()
}

struct Fakes {
    fake: Fake,
    stores: u64,
}

impl Backend for Fakes {
    type Store = EtcdControlStore;

    async fn store(&mut self) -> EtcdControlStore {
        self.stores += 1;
        let config =
            EtcdStoreConfig::new(vec![self.fake.url("http")], format!("s{}/", self.stores));
        EtcdControlStore::new(config).unwrap()
    }
}

#[tokio::test]
async fn the_store_conforms_over_the_fake() {
    bounded(async {
        let mut fakes = Fakes {
            fake: Fake::start(None).await,
            stores: 0,
        };
        conformance::run(&mut fakes).await;
    })
    .await;
}

#[tokio::test]
async fn write_errors_follow_the_lost_response_rule() {
    bounded(async {
        let fake = Fake::start(None).await;
        let store = store(&fake);
        let cases = [
            (Fault::AppliedThenHang, "indeterminate", true),
            (Fault::Hang, "indeterminate", false),
            (Fault::AppliedThenStatus(14), "indeterminate", true),
            (Fault::AppliedThenStatus(4), "indeterminate", true),
            (Fault::Status(14), "indeterminate", false),
            (Fault::Status(8), "unavailable", false),
            (Fault::Status(7), "rejected", false),
            (Fault::Status(16), "rejected", false),
            (Fault::Oversized, "indeterminate", false),
            (Fault::Compressed, "indeterminate", false),
            (Fault::Http(503), "indeterminate", false),
        ];
        for (n, (fault, expected, applied)) in cases.into_iter().enumerate() {
            let key = RegisterKey::new(format!("nodes/n{n}.json")).unwrap();
            fake.fail([fault.clone()]);
            let error = store
                .put_if(&key, Expected::Absent, value())
                .await
                .unwrap_err();
            let kind = match error {
                ControlError::Indeterminate(_) => "indeterminate",
                ControlError::Unavailable(_) => "unavailable",
                ControlError::Rejected(_) => "rejected",
                _ => "other",
            };
            assert_eq!(kind, expected, "{fault:?}: {error}");
            let read = store.get(&key).await.unwrap();
            assert_eq!(read.is_some(), applied, "{fault:?}");
        }
    })
    .await;
}

#[tokio::test]
async fn deletes_and_reads_map_errors() {
    bounded(async {
        let fake = Fake::start(None).await;
        let store = store(&fake);
        let PutOutcome::Written(version) = store
            .put_if(&key(), Expected::Absent, value())
            .await
            .unwrap()
        else {
            panic!("the create failed");
        };
        fake.fail([Fault::AppliedThenHang]);
        let error = store.delete_if(&key(), &version).await.unwrap_err();
        assert!(error.may_have_applied(), "{error}");
        assert_eq!(store.get(&key()).await.unwrap(), None);
        for (code, retryable) in [(14, true), (8, true), (7, false), (16, false)] {
            fake.fail([Fault::Status(code)]);
            let error = store.get(&key()).await.unwrap_err();
            assert_eq!(error.is_retryable(), retryable, "{code}: {error}");
            assert!(!error.may_have_applied(), "{code}: {error}");
            fake.fail([Fault::Status(code)]);
            let error = store.list(&KeyPrefix::root()).await.unwrap_err();
            assert_eq!(error.is_retryable(), retryable, "{code}: {error}");
        }
        // Versions that are not revisions fail without a request.
        let requests = fake.requests();
        let stale = Version::new("\"etag\"");
        let put = store
            .put_if(&key(), Expected::Version(stale.clone()), value())
            .await;
        assert_eq!(put.unwrap(), PutOutcome::PreconditionFailed);
        let delete = store.delete_if(&key(), &stale).await.unwrap();
        assert_eq!(delete, DeleteOutcome::PreconditionFailed);
        assert_eq!(fake.requests(), requests);
    })
    .await;
}

#[tokio::test]
async fn an_unanswered_call_moves_to_the_next_endpoint() {
    bounded(async {
        let (first, second) = (Fake::start(None).await, Fake::start(None).await);
        let store =
            EtcdControlStore::new(config(&[first.url("http"), second.url("http")])).unwrap();
        assert_eq!(store.get(&key()).await.unwrap(), None);
        assert_eq!((first.requests(), second.requests()), (1, 0));
        first.fail([Fault::Hang]);
        let error = store.get(&key()).await.unwrap_err();
        assert!(error.is_retryable(), "{error}");
        assert_eq!(store.get(&key()).await.unwrap(), None);
        assert_eq!((first.requests(), second.requests()), (2, 1));
        // An answer with an error status keeps the connection.
        second.fail([Fault::Status(7)]);
        assert!(store.get(&key()).await.is_err());
        assert_eq!(store.get(&key()).await.unwrap(), None);
        assert_eq!((first.requests(), second.requests()), (2, 3));
    })
    .await;
}

#[tokio::test]
async fn unreachable_endpoints_are_passed_over() {
    bounded(async {
        let fake = Fake::start(None).await;
        let closed = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port())
        };
        let store = EtcdControlStore::new(config(&[closed.clone(), fake.url("http")])).unwrap();
        assert_eq!(store.get(&key()).await.unwrap(), None);
        let store = EtcdControlStore::new(config(&[closed])).unwrap();
        let error = store
            .put_if(&key(), Expected::Absent, value())
            .await
            .unwrap_err();
        assert!(matches!(error, ControlError::Unavailable(_)), "{error}");
        assert!(!error.may_have_applied());
    })
    .await;
}

#[tokio::test]
async fn listings_start_over_when_their_revision_is_compacted() {
    bounded(async {
        let fake = Fake::start(None).await;
        let store = store(&fake);
        let count = usize::try_from(EtcdControlStore::LIST_PAGE).unwrap() + 1;
        for n in 0..count {
            fake.put(&format!("c/nodes/n{n:05}.json"), b"{}");
        }
        fake.put("c/buckets/b.json", b"{}");
        fake.put("other/nodes/x.json", b"{}");
        let requests = fake.requests();
        fake.fail([Fault::Pass, Fault::Status(11)]);
        let listed = store.list(&KeyPrefix::nodes()).await.unwrap();
        assert_eq!(listed.len(), count);
        assert_eq!(fake.requests(), requests + 4);
        // A store that keeps compacting is given up on.
        fake.fail([
            Fault::Pass,
            Fault::Status(11),
            Fault::Pass,
            Fault::Status(11),
            Fault::Pass,
            Fault::Status(11),
        ]);
        let error = store.list(&KeyPrefix::nodes()).await.unwrap_err();
        assert!(matches!(error, ControlError::Rejected(_)), "{error}");
        // A key under the prefix outside the register grammar.
        fake.put("c/buckets/bad key.json", b"{}");
        let error = store.list(&KeyPrefix::buckets()).await.unwrap_err();
        assert!(matches!(error, ControlError::InvalidKey(_)), "{error}");
    })
    .await;
}

/// Waits until the feed waits for a wake-up, then bumps the generation,
/// and returns what the feed reports. Only the watch can wake the feed,
/// since it reads the generation before it waits.
async fn report_after_bump(
    fake: &Fake,
    changes: ChangeFeed<EtcdControlStore>,
    store: &EtcdControlStore,
    ids: &mut ProposalIds,
) -> (ChangeFeed<EtcdControlStore>, Generation) {
    let mut changes = changes;
    let read = fake.requests();
    let next = tokio::spawn(async move {
        let change = changes.next().await.unwrap();
        (changes, change.generation)
    });
    // The feed's read of cluster.json, which finds the generation it
    // already reported.
    while fake.requests() == read {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    bump_generation(store, &cluster(), ids, &RetryPolicy::default())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), next)
        .await
        .expect("the watch woke the feed")
        .unwrap()
}

/// A watch etcd cancels, compacts, or ends is opened again, and the feed
/// then reports what was written while no watch was open.
#[tokio::test]
async fn watches_reopen_and_report_what_they_missed() {
    bounded(async {
        for fault in [
            WatchFault::CanceledAfterCreate,
            WatchFault::CompactedAfterCreate,
            WatchFault::EndedAfterCreate,
        ] {
            let fake = Fake::start(None).await;
            let store = store(&fake);
            let mut ids = ProposalIds::seeded(3);
            bootstrap(&store, &cluster(), ids.next_id(), &RetryPolicy::default())
                .await
                .unwrap();
            fake.fail_watch(fault.clone());
            let changes = store.changes(Generation::new(1)).await.unwrap();
            // The first watch ends at once; the bump comes while the feed
            // waits and no watch is open.
            let (changes, generation) = report_after_bump(&fake, changes, &store, &mut ids).await;
            assert_eq!(generation, Generation::new(2), "{fault:?}");
            assert_eq!(fake.watches(), 2, "{fault:?}");
            // The reopened watch reports the next increment.
            let (_, generation) = report_after_bump(&fake, changes, &store, &mut ids).await;
            assert_eq!(generation, Generation::new(3), "{fault:?}");
            assert_eq!(fake.watches(), 2, "{fault:?}");
        }
    })
    .await;
}

#[tokio::test]
async fn a_refused_watch_is_an_error() {
    bounded(async {
        let fake = Fake::start(None).await;
        let store = store(&fake);
        fake.fail_watch(WatchFault::Refused);
        let error = store.changes(Generation::ZERO).await.unwrap_err();
        assert!(matches!(error, ControlError::Unavailable(_)), "{error}");
        let closed = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port())
        };
        let store = EtcdControlStore::new(config(&[closed])).unwrap();
        let error = store.changes(Generation::ZERO).await.unwrap_err();
        assert!(error.is_retryable(), "{error}");
    })
    .await;
}

/// A CA and a certificate for `localhost` it issued.
fn pki() -> (
    CertificateDer<'static>,
    CertificateDer<'static>,
    PrivateKeyDer<'static>,
) {
    let ca_key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = params.self_signed(&ca_key).unwrap().der().clone();
    let issuer = Issuer::new(params, ca_key);
    let key = KeyPair::generate().unwrap();
    let params = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
    let cert = params.signed_by(&key, &issuer).unwrap().der().clone();
    (
        ca,
        cert,
        PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
    )
}

fn client_tls(ca: CertificateDer<'static>) -> Arc<rustls::ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(ca).unwrap();
    Arc::new(
        rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth(),
    )
}

#[tokio::test]
async fn https_endpoints_speak_tls_with_alpn() {
    bounded(async {
        let (ca, cert, cert_key) = pki();
        let mut server = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert], cert_key)
        .unwrap();
        // etcd refuses HTTP/2 over TLS without ALPN.
        server.alpn_protocols = vec![b"h2".to_vec()];
        let fake = Fake::start(Some(Arc::new(server))).await;
        let config = EtcdStoreConfig {
            tls: Some(client_tls(ca)),
            ..config(&[fake.url("https")])
        };
        let store = EtcdControlStore::new(config.clone()).unwrap();
        conformance::conditional_writes(&store).await;

        let (other_ca, _, _) = pki();
        let untrusted = EtcdStoreConfig {
            tls: Some(client_tls(other_ca)),
            ..config
        };
        let store = EtcdControlStore::new(untrusted).unwrap();
        let error = store.get(&key()).await.unwrap_err();
        assert!(
            error.to_string().contains("TLS handshake failed"),
            "{error}"
        );
    })
    .await;
}
