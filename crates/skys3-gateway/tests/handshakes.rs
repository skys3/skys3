//! The gateway's cap on in-flight TLS handshakes (design §12): connections
//! beyond it wait in the kernel's accept queue until a handshake ends.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common::setup;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use skys3_gateway::GatewayListener;
use skys3_obs::{HandshakeLimit, HandshakeMetrics, HandshakeOutcome, MetricsRegistry};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

/// A server configuration for `localhost`, and a client one that trusts it.
fn tls_pair() -> (Arc<rustls::ServerConfig>, Arc<rustls::ClientConfig>) {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let cert: CertificateDer<'static> = certified.cert.der().clone();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        certified.signing_key.serialize_der(),
    ));
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let server = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert.clone()], key)
        .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert).unwrap();
    let client = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    (Arc::new(server), Arc::new(client))
}

/// Waits up to five seconds for `condition`.
async fn eventually(what: &str, condition: impl Fn() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !condition() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Connects to `addr` and completes a TLS handshake.
async fn handshake(addr: SocketAddr, client: Arc<rustls::ClientConfig>) {
    let stream = TcpStream::connect(addr).await.unwrap();
    let name = ServerName::try_from("localhost").unwrap();
    let mut stream = TlsConnector::from(client)
        .connect(name, stream)
        .await
        .unwrap();
    stream.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connections_beyond_the_cap_wait_for_a_handshake_to_end() {
    let setup = setup("").await;
    let (server, client) = tls_pair();
    let registry = MetricsRegistry::new();
    let metrics = HandshakeMetrics::register(&registry);
    let limit = HandshakeLimit::new(1).with_metrics(&metrics, "gateway");
    let listener = GatewayListener::bind("127.0.0.1:0".parse().unwrap(), setup.gateway.clone())
        .await
        .unwrap()
        .with_tls(server)
        .with_handshake_limit(limit.clone());
    let addr = listener.local_addr().unwrap();
    assert!(format!("{listener:?}").contains("cap: 1"));
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let serving = tokio::spawn(listener.serve(async {
        let _ = stopped.await;
    }));

    // A client that connects and never says a word holds the only slot.
    let silent = TcpStream::connect(addr).await.unwrap();
    eventually("the silent connection takes the slot", || {
        metrics.in_flight("gateway") == 1
    })
    .await;
    let waiting = tokio::spawn(handshake(addr, Arc::clone(&client)));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !waiting.is_finished(),
        "a handshake beyond the cap must wait for the slot"
    );
    assert_eq!(limit.in_flight(), 1);

    // Its end frees the slot, and the waiting client is served.
    drop(silent);
    tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("the waiting handshake ran once the slot was free")
        .unwrap();
    eventually("both handshakes ended", || {
        metrics.ended("gateway", HandshakeOutcome::Completed) == 1
            && metrics.ended("gateway", HandshakeOutcome::Failed) == 1
    })
    .await;
    assert_eq!(metrics.in_flight("gateway"), 0);

    let _ = stop.send(());
    tokio::time::timeout(Duration::from_secs(15), serving)
        .await
        .unwrap()
        .unwrap();
}
