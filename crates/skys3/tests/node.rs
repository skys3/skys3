//! A node started in process: recovery across restarts, the local copy of
//! control state, reclaiming orphaned shards, TLS, STS, and the admin API.

mod support;

use std::path::Path;

use skys3::{Node, StartError};
use skys3_config::Config;
use support::{
    admin_json, body, client, config_text, eventually, get, http_get, http_request, put,
};

/// Starts a node in `dir` on ephemeral ports.
async fn start(dir: &Path, extra: &str) -> Node {
    let text = config_text(dir, "127.0.0.1:0", "127.0.0.1:0", extra);
    let config: Config = text.parse().unwrap();
    Node::start(config).await.unwrap()
}

fn endpoint(node: &Node) -> String {
    format!("http://{}", node.gateway_addr())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn objects_and_buckets_survive_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), "").await;
    let node_id = node.node_id().clone();
    let s3 = client(&endpoint(&node));
    s3.create_bucket().bucket("photos").send().await.unwrap();
    let small = body(1, 100);
    let large = body(2, 200_000);
    put(&s3, "photos", "small.txt", &small).await;
    put(&s3, "photos", "large.bin", &large).await;
    let admin = node.admin_addr().to_string();
    let health = admin_json(&admin, "/v1/health").await;
    assert_eq!(health["ready"], true, "{health}");
    assert_eq!(health["control_store"]["live"], true);
    assert_eq!(health["disks"].as_array().unwrap().len(), 2);
    // Four shards of the bucket and the sessions shard.
    assert_eq!(health["shards_open"], 5);
    node.shutdown().await.unwrap();

    let node = start(dir.path(), "").await;
    assert_eq!(node.node_id(), &node_id, "the node keeps its ID");
    let s3 = client(&endpoint(&node));
    assert_eq!(get(&s3, "photos", "small.txt").await, small);
    assert_eq!(get(&s3, "photos", "large.bin").await, large);
    let listed = s3.list_buckets().send().await.unwrap();
    assert_eq!(listed.buckets().len(), 1);

    let admin = node.admin_addr().to_string();
    let buckets = admin_json(&admin, "/v1/buckets").await;
    let photos = &buckets["buckets"][0];
    assert_eq!(photos["name"], "photos");
    assert_eq!(photos["mode"], "local");
    assert_eq!(photos["shards_open"], 4);
    assert_eq!(photos["objects"], 2);
    assert_eq!(photos["unflushed"], 2);
    assert_eq!(admin_json(&admin, "/v1/buckets/photos").await, *photos);
    let (status, _) = http_get(&admin, "/v1/buckets/other").await.unwrap();
    assert_eq!(status, 404);
    let (status, _) = http_get(&admin, "/v1/unknown").await.unwrap();
    assert_eq!(status, 404);
    let (status, _) = http_request(&admin, "POST", "/v1/health").await.unwrap();
    assert_eq!(status, 405);
    let (status, _) = http_get(&admin, "/readyz").await.unwrap();
    assert_eq!(status, 200);
    let (status, metrics) = http_get(&admin, "/metrics").await.unwrap();
    assert_eq!(status, 200);
    assert!(metrics.contains("skys3_control_store_live 1"), "{metrics}");
    assert!(
        metrics.contains("skys3_disks_out_of_service 0"),
        "{metrics}"
    );

    // A deleted object stays deleted after a restart.
    s3.delete_object()
        .bucket("photos")
        .key("small.txt")
        .send()
        .await
        .unwrap();
    node.shutdown().await.unwrap();
    let node = start(dir.path(), "").await;
    let s3 = client(&endpoint(&node));
    let missing = s3
        .get_object()
        .bucket("photos")
        .key("small.txt")
        .send()
        .await;
    assert!(missing.is_err());
    assert_eq!(get(&s3, "photos", "large.bin").await, large);
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_runs_from_its_copy_while_the_control_store_fails() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), "").await;
    let s3 = client(&endpoint(&node));
    s3.create_bucket().bucket("kept").send().await.unwrap();
    put(&s3, "kept", "a", b"alpha").await;
    node.shutdown().await.unwrap();

    // A control store whose cluster.json cannot be read.
    let cluster = dir.path().join("data/control/cluster.json");
    let original = std::fs::read(&cluster).unwrap();
    std::fs::write(&cluster, b"not json").unwrap();
    let node = start(dir.path(), "").await;
    let s3 = client(&endpoint(&node));
    assert_eq!(get(&s3, "kept", "a").await, b"alpha");
    put(&s3, "kept", "b", b"beta").await;
    let refused = s3.create_bucket().bucket("new").send().await;
    assert!(refused.is_err(), "bucket changes need the control store");
    let admin = node.admin_addr().to_string();
    let health = admin_json(&admin, "/v1/health").await;
    assert_eq!(health["control_store"]["live"], false, "{health}");
    assert_eq!(health["shards_open"], 5, "no shard is reclaimed");

    node.shutdown().await.unwrap();

    // Once the store answers again, the node uses it.
    std::fs::write(&cluster, original).unwrap();
    let node = start(dir.path(), "").await;
    let s3 = client(&endpoint(&node));
    let admin = node.admin_addr().to_string();
    assert_eq!(
        admin_json(&admin, "/v1/health").await["control_store"]["live"],
        true
    );
    assert_eq!(get(&s3, "kept", "b").await, b"beta");
    s3.create_bucket().bucket("new").send().await.unwrap();
    put(&s3, "new", "c", b"gamma").await;
    // The copy follows the change.
    eventually("the copy holds the new bucket's generation", || async {
        admin_json(&admin, "/v1/health").await["control_store"]["generation"] == 3
    })
    .await;
    node.shutdown().await.unwrap();
}

/// Health's `control_store.live`.
async fn live(node: &Node) -> bool {
    let health = admin_json(&node.admin_addr().to_string(), "/v1/health").await;
    health["control_store"]["live"] == true
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reset_control_store_is_never_bootstrapped_again() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), "").await;
    let s3 = client(&endpoint(&node));
    s3.create_bucket().bucket("kept").send().await.unwrap();
    put(&s3, "kept", "a", b"alpha").await;
    node.shutdown().await.unwrap();

    // The control store's volume is not mounted: its directory is gone.
    let control = dir.path().join("data/control");
    let moved = dir.path().join("control-elsewhere");
    std::fs::rename(&control, &moved).unwrap();
    let node = start(dir.path(), "").await;
    assert!(!live(&node).await);
    assert!(!control.exists(), "the directory is not created again");
    let s3 = client(&endpoint(&node));
    assert_eq!(get(&s3, "kept", "a").await, b"alpha");
    assert!(s3.create_bucket().bucket("new").send().await.is_err());
    let health = admin_json(&node.admin_addr().to_string(), "/v1/health").await;
    assert_eq!(health["shards_open"], 5, "no shard is reclaimed");
    node.shutdown().await.unwrap();

    // An empty directory in its place is not claimed or bootstrapped.
    std::fs::create_dir(&control).unwrap();
    let node = start(dir.path(), "").await;
    assert!(!live(&node).await);
    assert!(!control.join("cluster.json").exists());
    assert!(!control.join(".owner.json").exists());
    let s3 = client(&endpoint(&node));
    assert_eq!(get(&s3, "kept", "a").await, b"alpha");
    node.shutdown().await.unwrap();

    // Nor is the node's own store once its registers are gone.
    std::fs::remove_dir_all(&control).unwrap();
    std::fs::rename(&moved, &control).unwrap();
    let cluster = control.join("cluster.json");
    let original = std::fs::read(&cluster).unwrap();
    std::fs::remove_file(&cluster).unwrap();
    let node = start(dir.path(), "").await;
    assert!(!live(&node).await);
    assert!(!cluster.exists());
    node.shutdown().await.unwrap();

    // The real store, back in place, is used again with every bucket.
    std::fs::write(&cluster, original).unwrap();
    let node = start(dir.path(), "").await;
    assert!(live(&node).await);
    let s3 = client(&endpoint(&node));
    assert_eq!(get(&s3, "kept", "a").await, b"alpha");
    s3.create_bucket().bucket("new").send().await.unwrap();
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_runs_from_its_copy_until_the_store_opens() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), "").await;
    let s3 = client(&endpoint(&node));
    s3.create_bucket().bucket("kept").send().await.unwrap();
    put(&s3, "kept", "a", b"alpha").await;
    node.shutdown().await.unwrap();

    // A path the store cannot be opened at: an I/O error, not a reset.
    let control = dir.path().join("data/control");
    let moved = dir.path().join("control-elsewhere");
    std::fs::rename(&control, &moved).unwrap();
    std::fs::write(&control, b"not a directory").unwrap();
    let node = start(dir.path(), "").await;
    assert!(!live(&node).await);
    let s3 = client(&endpoint(&node));
    assert_eq!(get(&s3, "kept", "a").await, b"alpha");
    assert!(s3.create_bucket().bucket("new").send().await.is_err());

    // Once the store can be opened, the running node uses it.
    std::fs::remove_file(&control).unwrap();
    std::fs::rename(&moved, &control).unwrap();
    eventually("the node opens the control store", || live(&node)).await;
    s3.create_bucket().bucket("new").send().await.unwrap();
    put(&s3, "new", "b", b"beta").await;
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_file_control_store_serves_only_its_node() {
    let first = tempfile::tempdir().unwrap();
    let node = start(first.path(), "").await;
    let node_id = node.node_id().clone();
    let s3 = client(&endpoint(&node));
    s3.create_bucket().bucket("mine").send().await.unwrap();
    node.shutdown().await.unwrap();
    let control = first.path().join("data/control");

    // Another node, with its own data directory, pointed at the store.
    let second = tempfile::tempdir().unwrap();
    let pointed = |node_id: Option<&str>| {
        let text = config_text(second.path(), "127.0.0.1:0", "127.0.0.1:0", "").replace(
            "backend = \"file\"",
            &format!("backend = \"file\"\ndirectory = \"{}\"", control.display()),
        );
        let text = match node_id {
            Some(id) => text.replace("[node]\n", &format!("[node]\nnode_id = \"{id}\"\n")),
            None => text,
        };
        text.parse::<Config>().unwrap()
    };
    let refused = Node::start(pointed(None)).await.unwrap_err();
    assert!(matches!(refused, StartError::ControlStore(_)), "{refused}");
    // The same node ID with another data directory is another node too.
    for created in ["data", "disk-a", "disk-b"] {
        std::fs::remove_dir_all(second.path().join(created)).unwrap();
    }
    let refused = Node::start(pointed(Some(node_id.as_str())))
        .await
        .unwrap_err();
    assert!(refused.to_string().contains("instance"), "{refused}");
    // A store that names no owner is claimed only while empty.
    std::fs::remove_file(control.join(".owner.json")).unwrap();
    let refused = Node::start(pointed(None)).await.unwrap_err();
    assert!(refused.to_string().contains("names no owner"), "{refused}");
    assert!(!control.join(".owner.json").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shards_of_a_bucket_without_a_register_are_reclaimed() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), "").await;
    let s3 = client(&endpoint(&node));
    s3.create_bucket().bucket("gone").send().await.unwrap();
    put(&s3, "gone", "k", b"v").await;
    node.shutdown().await.unwrap();

    // A deletion whose answer was lost after the register went.
    std::fs::remove_file(dir.path().join("data/control/buckets/gone.json")).unwrap();
    let node = start(dir.path(), "").await;
    let admin = node.admin_addr().to_string();
    assert_eq!(admin_json(&admin, "/v1/health").await["shards_open"], 1);
    let buckets = admin_json(&admin, "/v1/buckets").await;
    assert_eq!(buckets["buckets"].as_array().unwrap().len(), 0);
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_back_buckets_get_flushers() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), "").await;
    let s3 = client(&endpoint(&node));
    // A target nothing listens on: its capability probe keeps failing, so
    // nothing is flushed and the write stays dirty.
    s3.create_bucket()
        .bucket("archive")
        .customize()
        .mutate_request(|request| {
            let headers = request.headers_mut();
            headers.insert("x-skys3-bucket-mode", "write_back");
            headers.insert("x-skys3-bucket-target", "http://127.0.0.1:9/remote/team");
        })
        .send()
        .await
        .unwrap();
    put(&s3, "archive", "k", b"v").await;
    let admin = node.admin_addr().to_string();
    eventually("the bucket has a flusher", || async {
        let status = admin_json(&admin, "/v1/buckets/archive").await;
        status["flush"]["probe"] == "running"
    })
    .await;
    let status = admin_json(&admin, "/v1/buckets/archive").await;
    assert_eq!(status["unflushed"], 1);
    assert_eq!(status["flush"]["conflicts"], serde_json::json!([]));
    assert_eq!(status["flush"]["awaiting_multipart"], serde_json::json!([]));
    let (_, metrics) = http_get(&admin, "/metrics").await.unwrap();
    assert!(
        metrics.contains("skys3_dirty_bytes{bucket=\"archive\"}"),
        "{metrics}"
    );
    // A bucket with unflushed writes cannot be detached.
    assert!(s3.delete_bucket().bucket("archive").send().await.is_err());
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_gateway_serves_https_and_sts() {
    let dir = tempfile::tempdir().unwrap();
    let cert = support::tls_file("server.pem");
    let key = support::tls_file("server.key");
    let text = config_text(dir.path(), "127.0.0.1:0", "127.0.0.1:0", "")
        .replace(
            "listen = \"127.0.0.1:0\"\n\n[control_store]",
            &format!(
                "listen = \"127.0.0.1:0\"\ntls_cert_file = \"{}\"\ntls_key_file = \"{}\"\n\n\
                 [control_store]",
                cert.display(),
                key.display()
            ),
        )
        .replace("sts_web_identity = false", "sts_web_identity = true");
    let node = Node::start(text.parse().unwrap()).await.unwrap();
    let s3 = client(&format!("https://localhost:{}", node.gateway_addr().port()));
    s3.create_bucket().bucket("secure").send().await.unwrap();
    put(&s3, "secure", "k", b"over tls").await;
    assert_eq!(get(&s3, "secure", "k").await, b"over tls");

    // Plain HTTP is refused by the TLS listener.
    let plain = http_get(&node.gateway_addr().to_string(), "/").await;
    assert!(plain.map_or(true, |(status, _)| status == 0));
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_refuses_what_it_cannot_run() {
    let dir = tempfile::tempdir().unwrap();
    let text = config_text(dir.path(), "127.0.0.1:0", "127.0.0.1:0", "").replace(
        "backend = \"file\"",
        "backend = \"etcd\"\netcd_endpoints = [\"https://etcd.invalid:2379\"]",
    );
    let error = Node::start(text.parse().unwrap()).await.unwrap_err();
    assert!(matches!(error, StartError::Unsupported(_)), "{error}");

    // A second node cannot use a data directory in use.
    let node = start(dir.path(), "").await;
    let error = Node::start(
        config_text(dir.path(), "127.0.0.1:0", "127.0.0.1:0", "")
            .parse()
            .unwrap(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("in use"), "{error}");
    node.shutdown().await.unwrap();

    // A missing secret file stops the start.
    let text = config_text(dir.path(), "127.0.0.1:0", "127.0.0.1:0", "");
    std::fs::remove_file(dir.path().join("secret")).unwrap();
    let error = Node::start(text.parse().unwrap()).await.unwrap_err();
    assert!(matches!(error, StartError::Credentials(_)), "{error}");

    // So does a gateway address that is taken.
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let text = config_text(
        dir.path(),
        &taken.local_addr().unwrap().to_string(),
        "127.0.0.1:0",
        "",
    );
    let error = Node::start(text.parse().unwrap()).await.unwrap_err();
    assert!(matches!(error, StartError::Io { .. }), "{error}");
}
