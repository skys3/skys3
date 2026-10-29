//! The real `skys3` binary on a temporary directory: data written with the
//! AWS SDK survives a graceful restart (`SIGTERM`), and every acknowledged
//! write survives `SIGKILL`.

mod support;

use std::process::Command;

use support::process::{BINARY, Process, configure};
use support::{body, get, put};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_survives_graceful_restarts_and_kills() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("node.log");
    let (config, gateway, admin) = configure(dir.path());

    let node = Process::start(&config, &gateway, &admin, &log).await;
    let s3 = node.s3();
    s3.create_bucket().bucket("data").send().await.unwrap();
    let mut written = Vec::new();
    for (i, len) in [10_usize, 5_000, 150_000].into_iter().enumerate() {
        let key = format!("before-restart/{i}");
        let data = body(i as u8, len);
        put(&s3, "data", &key, &data).await;
        written.push((key, data));
    }
    let status = node.terminate().await;
    assert!(status.success(), "{status:?}");

    // A graceful restart keeps everything.
    let node = Process::start(&config, &gateway, &admin, &log).await;
    let s3 = node.s3();
    for (key, data) in &written {
        assert_eq!(&get(&s3, "data", key).await, data, "{key}");
    }
    // Writes acknowledged just before a SIGKILL survive it.
    for i in 0..20_u8 {
        let key = format!("before-kill/{i}");
        let data = body(100 + i, 300 + usize::from(i) * 7_000);
        put(&s3, "data", &key, &data).await;
        written.push((key, data));
    }
    node.kill();

    let node = Process::start(&config, &gateway, &admin, &log).await;
    let s3 = node.s3();
    for (key, data) in &written {
        assert_eq!(&get(&s3, "data", key).await, data, "{key}");
    }
    let listed = s3.list_buckets().send().await.unwrap();
    assert_eq!(listed.buckets().len(), 1);
    let status = node.terminate().await;
    assert!(status.success(), "{status:?}");
}

#[test]
fn the_command_line_checks_configurations() {
    let dir = tempfile::tempdir().unwrap();
    let (config, _, _) = configure(dir.path());
    let output = Command::new(BINARY)
        .arg("--config")
        .arg(&config)
        .arg("--check-config")
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stdout).contains("valid"));

    let invalid = dir.path().join("invalid.toml");
    std::fs::write(&invalid, "[cluster]\ncluster_id = \"Not Valid\"\n").unwrap();
    let output = Command::new(BINARY)
        .args(["-c"])
        .arg(&invalid)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("cluster.cluster_id"));

    let output = Command::new(BINARY).arg("--version").output().unwrap();
    assert!(output.status.success());
    let output = Command::new(BINARY).arg("--nonsense").output().unwrap();
    assert_eq!(output.status.code(), Some(2));
}

#[test]
fn a_node_that_cannot_start_exits_with_failure() {
    let dir = tempfile::tempdir().unwrap();
    let (config, _, _) = configure(dir.path());
    let text = std::fs::read_to_string(&config).unwrap().replace(
        "backend = \"file\"",
        "backend = \"etcd\"\netcd_endpoints = [\"https://etcd.invalid:2379\"]",
    );
    std::fs::write(&config, text).unwrap();
    let output = Command::new(BINARY)
        .arg("--config")
        .arg(&config)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stderr).contains("backend"));
}
