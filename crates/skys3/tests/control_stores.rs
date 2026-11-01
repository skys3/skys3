//! A node on each control-store backend (design §6.1, §6.2, §12): etcd
//! over mutual TLS, against the in-process stand-in for etcd; an S3 store,
//! another SkyS3 node's bucket, which passes the startup probe; the
//! single-node guard; `skys3 control identity` against a shared store
//! while the node runs; and a file store reopened after a failed write.

mod support;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair};
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use rustls::server::WebPkiClientVerifier;
use skys3::identity::{self, IdentityFile, Request};
use skys3::{Node, StartError};
use skys3_config::Config;
use skys3_control::etcd_fake::Fake;
use support::process::{Process, fresh_addresses};
use support::{admin_json, client, config_text, eventually, get, http_get, put};

/// `config_text` with the file control store replaced by `backend`, the
/// lines of another `[control_store]` backend.
fn config_with(dir: &Path, gateway: &str, admin: &str, backend: &str) -> String {
    config_text(dir, gateway, admin, "").replace("backend = \"file\"", backend)
}

/// Starts a node in `dir` on ephemeral ports with the control store
/// `backend`.
async fn start(dir: &Path, backend: &str) -> Result<Node, StartError> {
    let text = config_with(dir, "127.0.0.1:0", "127.0.0.1:0", backend);
    Node::start(text.parse().unwrap()).await
}

fn endpoint(node: &Node) -> String {
    format!("http://{}", node.gateway_addr())
}

/// The admin API's `control_store` object.
async fn control_state(node: &Node) -> serde_json::Value {
    admin_json(&node.admin_addr().to_string(), "/v1/health").await["control_store"].clone()
}

/// `der` as a PEM block labeled `label`.
fn pem(label: &str, der: &[u8]) -> String {
    use base64::Engine as _;
    let text = base64::engine::general_purpose::STANDARD.encode(der);
    let lines: Vec<&str> = text
        .as_bytes()
        .chunks(64)
        .map(|line| std::str::from_utf8(line).unwrap())
        .collect();
    format!(
        "-----BEGIN {label}-----\n{}\n-----END {label}-----\n",
        lines.join("\n")
    )
}

/// A throwaway PKI in `dir`: the stand-in's TLS configuration, which
/// requires a client certificate from the CA, and the `[control_store]`
/// lines of a client that has one.
fn mutual_tls(dir: &Path) -> (Arc<rustls::ServerConfig>, String) {
    let ca_key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::new(params, ca_key);
    let leaf = |name: &str, usage: ExtendedKeyUsagePurpose| {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec![name.to_owned()]).unwrap();
        params.extended_key_usages = vec![usage];
        (params.signed_by(&key, &issuer).unwrap(), key)
    };
    let (server, server_key) = leaf("localhost", ExtendedKeyUsagePurpose::ServerAuth);
    let (client, client_key) = leaf("skys3-node", ExtendedKeyUsagePurpose::ClientAuth);
    let write = |name: &str, pem: String| -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, pem).unwrap();
        path
    };
    let ca_file = write("etcd-ca.pem", pem("CERTIFICATE", ca.der()));
    let cert_file = write("etcd-client.pem", pem("CERTIFICATE", client.der()));
    let key_file = write(
        "etcd-client.key",
        pem("PRIVATE KEY", &client_key.serialize_der()),
    );

    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut roots = RootCertStore::empty();
    roots.add(ca.der().clone()).unwrap();
    let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
        .build()
        .unwrap();
    let mut config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_client_cert_verifier(verifier)
        .with_single_cert(
            vec![CertificateDer::from(server.der().to_vec())],
            PrivatePkcs8KeyDer::from(server_key.serialize_der()).into(),
        )
        .unwrap();
    config.alpn_protocols = vec![b"h2".to_vec()];
    let lines = format!(
        "etcd_tls_ca_file = \"{}\"\netcd_tls_cert_file = \"{}\"\netcd_tls_key_file = \"{}\"",
        ca_file.display(),
        cert_file.display(),
        key_file.display()
    );
    (Arc::new(config), lines)
}

/// An identity file with one provider.
fn identity_file() -> IdentityFile {
    IdentityFile::parse(
        br#"{"providers": {"ci": {"issuer": "https://token.example", "audiences": ["sts"]}}}"#,
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_runs_on_etcd_over_mutual_tls() {
    let dir = tempfile::tempdir().unwrap();
    let (server, tls) = mutual_tls(dir.path());
    let fake = Fake::start(Some(server)).await;
    let backend = format!(
        "backend = \"etcd\"\netcd_endpoints = [\"{}\"]\n{tls}",
        fake.url("https")
    );
    let node = start(dir.path(), &backend).await.unwrap();
    assert_eq!(control_state(&node).await["live"], true);
    let s3 = client(&endpoint(&node));
    s3.create_bucket().bucket("data").send().await.unwrap();
    put(&s3, "data", "k", b"on etcd").await;
    assert!(fake.get("node-test/cluster.json").is_some());
    assert!(fake.get("node-test/buckets/data.json").is_some());

    // The store is shared, so the identity command runs beside the node,
    // which follows the change.
    let config: Config = config_with(dir.path(), "127.0.0.1:0", "127.0.0.1:0", &backend)
        .parse()
        .unwrap();
    let applied = identity::run(&config, &Request::Put(identity_file()))
        .await
        .unwrap();
    assert!(fake.get("node-test/identity/providers/ci.json").is_some());
    let generation = applied.generation.get();
    eventually("the node syncs the identity change", || async {
        control_state(&node).await["generation"] == generation
    })
    .await;
    node.shutdown().await.unwrap();

    // A restart finds everything on etcd.
    let node = start(dir.path(), &backend).await.unwrap();
    let s3 = client(&endpoint(&node));
    assert_eq!(get(&s3, "data", "k").await, b"on etcd");
    node.shutdown().await.unwrap();

    // Without its client certificate, etcd refuses the node, which runs
    // from its copy.
    let without = backend
        .lines()
        .filter(|line| !line.starts_with("etcd_tls_cert_file") && !line.starts_with("etcd_tls_key"))
        .collect::<Vec<_>>()
        .join("\n");
    let node = start(dir.path(), &without).await.unwrap();
    assert_eq!(control_state(&node).await["live"], false);
    assert_eq!(control_state(&node).await["generation"], generation);
    let s3 = client(&endpoint(&node));
    assert_eq!(get(&s3, "data", "k").await, b"on etcd");
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_refuses_a_store_that_registers_another_node() {
    let dir = tempfile::tempdir().unwrap();
    let fake = Fake::start(None).await;
    let backend = format!(
        "backend = \"etcd\"\netcd_endpoints = [\"{}\"]",
        fake.url("http")
    );
    // A node on its own store starts, and leaves no registration in this
    // build.
    let node = start(dir.path(), &backend).await.unwrap();
    node.shutdown().await.unwrap();

    fake.put("node-test/nodes/node-elsewhere.json", b"{}");
    let error = start(dir.path(), &backend).await.unwrap_err();
    let StartError::ControlStore(message) = &error else {
        panic!("{error}");
    };
    assert!(message.contains("node-elsewhere"), "{message}");
    assert!(message.contains("one node per control store"), "{message}");
    assert!(message.contains("[control_store] prefix"), "{message}");
    // So does one that never ran.
    let fresh = tempfile::tempdir().unwrap();
    let error = start(fresh.path(), &backend).await.unwrap_err();
    assert!(matches!(error, StartError::ControlStore(_)), "{error}");
}

/// Another SkyS3 node serves the control bucket: its gateway honors
/// conditional writes and deletes, so the node under test passes the
/// startup probe against it. The node under test is the binary, so that
/// the default credential chain reads its own environment.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_runs_on_an_s3_store_that_passes_the_probe() {
    let store_dir = tempfile::tempdir().unwrap();
    let text = config_text(store_dir.path(), "127.0.0.1:0", "127.0.0.1:0", "")
        .replace("cluster_id = \"node-test\"", "cluster_id = \"store\"");
    let store = Node::start(text.parse().unwrap()).await.unwrap();
    let objects = client(&endpoint(&store));
    objects
        .create_bucket()
        .bucket("control")
        .send()
        .await
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let (gateway, admin) = fresh_addresses();
    let backend = format!(
        "backend = \"s3\"\nendpoint = \"{}\"\nbucket = \"control\"\nprefix = \"c1/\"",
        endpoint(&store)
    );
    let config = dir.path().join("skys3.toml");
    std::fs::write(&config, config_with(dir.path(), &gateway, &admin, &backend)).unwrap();
    let missing = dir.path().join("missing");
    let env = [
        ("AWS_ACCESS_KEY_ID", Path::new(support::ACCESS_KEY)),
        ("AWS_SECRET_ACCESS_KEY", Path::new(support::SECRET_KEY)),
        ("AWS_EC2_METADATA_DISABLED", Path::new("true")),
        ("AWS_CONFIG_FILE", missing.as_path()),
        ("AWS_SHARED_CREDENTIALS_FILE", missing.as_path()),
    ];
    let log = dir.path().join("node.log");
    let node = Process::start_with_env(&config, &log, &env).await;
    let s3 = node.s3();
    s3.create_bucket().bucket("data").send().await.unwrap();
    put(&s3, "data", "k", b"on s3").await;
    for key in ["c1/cluster.json", "c1/buckets/data.json"] {
        objects
            .head_object()
            .bucket("control")
            .key(key)
            .send()
            .await
            .unwrap_or_else(|error| panic!("{key}: {error:?}"));
    }
    let log_text = std::fs::read_to_string(&log).unwrap();
    assert!(log_text.contains("passed the startup probe"), "{log_text}");
    // The probe leaves no scratch register.
    let listed = objects
        .list_objects_v2()
        .bucket("control")
        .prefix("c1/probe/")
        .send()
        .await
        .unwrap();
    assert_eq!(listed.key_count(), Some(0));
    // The versioning gauge reads what the store answered.
    eventually("the versioning gauge is set", || async {
        let (_, metrics) = http_get(&node.admin, "/metrics").await.unwrap();
        metrics.contains("skys3_control_store_versioning 0\n")
            || metrics.contains("skys3_control_store_versioning 1\n")
            || metrics.contains("skys3_control_store_versioning -1\n")
    })
    .await;
    assert!(node.terminate().await.success());

    let node = Process::start_with_env(&config, &log, &env).await;
    assert_eq!(get(&node.s3(), "data", "k").await, b"on s3");
    assert!(node.terminate().await.success());
    store.shutdown().await.unwrap();
}

/// A failed write takes the file store out of service; the node detaches
/// it, serves its copy, and opens the store again once it can (K1).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_file_store_is_opened_again() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), "backend = \"file\"").await.unwrap();
    let s3 = client(&endpoint(&node));
    s3.create_bucket().bucket("first").send().await.unwrap();
    // A directory where the next register write puts its temporary file:
    // the write fails, and so does every open of the store while it is
    // there, since opening removes temporary files.
    let blocker = dir.path().join("data/control/buckets/.second.json.tmp");
    std::fs::create_dir_all(&blocker).unwrap();
    let refused = s3.create_bucket().bucket("second").send().await;
    assert!(refused.is_err(), "{refused:?}");
    eventually("the node serves its copy", || async {
        control_state(&node).await["live"] == false
    })
    .await;
    // Reads come from the copy meanwhile.
    s3.head_bucket().bucket("first").send().await.unwrap();

    std::fs::remove_dir(&blocker).unwrap();
    eventually("the store is open again", || async {
        control_state(&node).await["live"] == true
    })
    .await;
    s3.create_bucket().bucket("second").send().await.unwrap();
    let listed = s3.list_buckets().send().await.unwrap();
    assert_eq!(listed.buckets().len(), 2);
    node.shutdown().await.unwrap();
}
