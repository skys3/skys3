//! Drills of the operator runbooks (`docs/skys3-runbooks.md`, plan M7-05).
//!
//! Each test breaks something the way a runbook's failure does, follows
//! the runbook's diagnosis steps (the series its alerts read, the admin
//! API, the node's log, the command line), carries out its remediation,
//! and checks what its verification step checks. The runbooks' drill
//! table names the test of each.
//!
//! Most drills run the real `skys3` binary. The flush and conflict drills
//! need a remote S3 target that takes the flusher's writes; a second node
//! cannot be one, since a gateway refuses the write-identity metadata that
//! every flush sends (`x-amz-meta-skys3-wid`). They run the node's flush
//! service, metrics, and admin API in process instead, over a shard on a
//! simulated disk and a simulated S3 store whose link an outage drops.
//!
//! The alerts fire on thresholds of minutes; the drills check the series
//! the alerts read, not the alerts, which would need a Prometheus server.

mod support;

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_sdk_s3::error::ProvideErrorMetadata;
use bytes::Bytes;
use http::{Method, StatusCode};
use http_body_util::BodyExt;
use md5::{Digest, Md5};
use serde_json::Value;
use skys3::admin::{ControlState, NodeAdmin};
use skys3::datadir::{DiskDir, boot_id, fence};
use skys3_config::Config;
use skys3_flush::{FlushMetrics, FlushService, FlushSettings, ProbeStatus};
use skys3_gateway::LocalShards;
use skys3_index::{EntryState, ImportCheckpoint, Index, IndexConfig};
use skys3_io::{BlockingPool, MonotonicClock, SimDisk, SimMount};
use skys3_log::record::{Put, PutData};
use skys3_log::{LogConfig, RecordBody, SegmentLog};
use skys3_obs::{AdminApi, Health, MetricsRegistry};
use skys3_remote::{ObjectStore, PutObject, UserMetadata};
use skys3_shard::{Shard, ShardSet};
use skys3_sim::SimS3;
use skys3_sim::s3::SimS3Config;
use skys3_types::{
    BucketDocument, BucketId, BucketMode, BucketName, ClusterId, Epoch, Label, NodeId, ProposalId,
    RemoteTarget, ShardConfig, ShardCount, ShardId,
};
use support::process::{BINARY, Process, configure};
use support::{admin_json, body, get, http_get, put};

/// How long a drill waits for a node to react.
const PATIENCE: Duration = Duration::from_secs(60);

/// A target nothing listens on: the remote is unreachable from the start.
const UNREACHABLE: &str = "http://127.0.0.1:9/remote";

/// Polls `condition` until it holds, for up to [`PATIENCE`].
async fn until(what: &str, mut condition: impl AsyncFnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + PATIENCE;
    while !condition().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting until {what}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The samples of an OpenMetrics text, by series (name and labels).
fn samples(metrics: &str) -> BTreeMap<String, f64> {
    metrics
        .lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| {
            let (series, value) = line.rsplit_once(' ')?;
            Some((series.to_owned(), value.parse().ok()?))
        })
        .collect()
}

/// A series' value in `samples`, or 0 if it has none (a labeled family
/// has no sample before its first event).
fn value(samples: &BTreeMap<String, f64>, series: &str) -> f64 {
    samples.get(series).copied().unwrap_or(0.0)
}

/// A node's metrics, as Prometheus scrapes them; `None` if the admin
/// listener does not answer, as `up` records it.
async fn scrape(admin: &str) -> Option<BTreeMap<String, f64>> {
    let (status, metrics) = http_get(admin, "/metrics").await.ok()?;
    (status == 200).then(|| samples(&metrics))
}

/// The value of `series` in a node's metrics.
async fn metric(node: &Process, series: &str) -> f64 {
    value(
        &scrape(&node.admin).await.expect("the node answers"),
        series,
    )
}

/// The text the node logged so far.
fn log_text(dir: &Path) -> String {
    std::fs::read_to_string(dir.join("node.log")).unwrap_or_default()
}

/// Starts the binary with the configuration at `config`, logging to
/// `node.log` beside it. The default credential chain finds nothing, and
/// does not ask instance metadata.
async fn start(config: &Path) -> Process {
    let log = config.with_file_name("node.log");
    let env = [("AWS_EC2_METADATA_DISABLED", Path::new("true"))];
    Process::start_with_env(config, &log, &env).await
}

/// Rewrites the configuration at `config`, replacing `from` with `to`.
fn edit_config(config: &Path, from: &str, to: &str) {
    let text = std::fs::read_to_string(config).unwrap();
    assert!(text.contains(from), "{from:?} is not in the configuration");
    std::fs::write(config, text.replacen(from, to, 1)).unwrap();
}

/// Creates `bucket` as a `write_back` bucket of `target`.
async fn attach(s3: &aws_sdk_s3::Client, bucket: &str, target: &'static str) {
    s3.create_bucket()
        .bucket(bucket)
        .customize()
        .mutate_request(move |request| {
            let headers = request.headers_mut();
            headers.insert("x-skys3-bucket-mode", "write_back");
            headers.insert("x-skys3-bucket-target", target);
        })
        .send()
        .await
        .unwrap();
}

/// The S3 error code and HTTP status of a PUT that must fail.
async fn refused_put(s3: &aws_sdk_s3::Client, bucket: &str, key: &str) -> (String, u16) {
    let error = s3
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(b"refused".to_vec().into())
        .send()
        .await
        .expect_err("the PUT is refused");
    let status = error.raw_response().map(|r| r.status().as_u16());
    (error.code().unwrap_or_default().to_owned(), status.unwrap())
}

/// `dirty-budget`: writes to a bucket whose target is down fill its dirty
/// budget and are refused; the operator raises the budget for the outage
/// with a restart, and writes are admitted while the data stays dirty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drill_dirty_budget() {
    let dir = tempfile::tempdir().unwrap();
    let config = configure(dir.path());
    std::fs::write(
        &config,
        std::fs::read_to_string(&config).unwrap() + "\n[flush]\nmax_dirty_bytes = 4096\n",
    )
    .unwrap();
    let node = start(&config).await;
    let s3 = node.s3();
    attach(&s3, "archive", UNREACHABLE).await;
    put(&s3, "archive", "a", &body(1, 4096)).await;

    // Symptoms: the share is used up, and writes are refused.
    let dirty = "skys3_dirty_bytes{bucket=\"archive\"}";
    let budget = "skys3_dirty_budget_bytes{bucket=\"archive\"}";
    until("the write is counted", async || {
        metric(&node, dirty).await == 4096.0
    })
    .await;
    assert_eq!(metric(&node, budget).await, 4096.0);
    assert_eq!(
        refused_put(&s3, "archive", "b").await,
        ("SlowDown".to_owned(), 503)
    );
    let refusals = "skys3_admission_refusals_total{reason=\"bucket_budget\"}";
    assert_eq!(metric(&node, refusals).await, 1.0);
    // Diagnosis: the bucket's flush status names why nothing drains.
    let status = admin_json(&node.admin, "/v1/buckets/archive").await;
    let flush = &status["flush"];
    assert_eq!(flush["dirty_bytes"], 4096, "{status}");
    assert_eq!(flush["dirty_budget_bytes"], 4096, "{status}");
    assert_eq!(flush["probe"], "running", "{status}");
    assert!(flush["probe_error"].is_string(), "{status}");
    // Reads of what the node holds go on.
    assert_eq!(get(&s3, "archive", "a").await, body(1, 4096));

    // Remediation: a larger budget for the outage, with a restart.
    assert!(node.terminate().await.success());
    edit_config(
        &config,
        "max_dirty_bytes = 4096",
        "max_dirty_bytes = 1048576",
    );
    let node = start(&config).await;
    let s3 = node.s3();
    until("the budget is raised", async || {
        metric(&node, budget).await == 1_048_576.0
    })
    .await;
    put(&s3, "archive", "b", b"admitted").await;
    // The data written before stays dirty until the target returns.
    until("both writes are counted", async || {
        metric(&node, dirty).await == 4104.0
    })
    .await;
    assert!(node.terminate().await.success());
}

/// `control-store-unreachable` and `identity-staleness`: the control
/// store's volume goes away under a running node, which serves from its
/// copy and refuses bucket changes; the volume returns, and a restart
/// reopens the store and refreshes the identity copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drill_control_store_outage() {
    let dir = tempfile::tempdir().unwrap();
    let config = configure(dir.path());
    let node = start(&config).await;
    let s3 = node.s3();
    s3.create_bucket().bucket("kept").send().await.unwrap();
    put(&s3, "kept", "a", b"alpha").await;
    let read = "skys3_control_store_last_success_timestamp_seconds";
    let synced = "skys3_identity_synced_timestamp_seconds";
    assert!(metric(&node, read).await > 0.0);
    let synced_before = metric(&node, synced).await;
    assert!(synced_before > 0.0);
    let staleness = metric(&node, "skys3_identity_max_staleness_seconds").await;
    assert_eq!(staleness, 24.0 * 3600.0);

    // The store's volume goes away under the running node. The file store
    // answers reads from memory, so the first write finds it gone, and the
    // store refuses everything after it.
    let control_dir = dir.path().join("data/control");
    let away = dir.path().join("control-away");
    std::fs::rename(&control_dir, &away).unwrap();
    // A store that fails a write answers 500, not the 503 of a node that
    // runs from its copy.
    let refused = s3.create_bucket().bucket("new").send().await.unwrap_err();
    let status = refused.raw_response().map(|r| r.status().as_u16());
    assert_eq!(status, Some(500), "{refused:?}");
    assert_eq!(
        refused.message(),
        Some("The cluster's control store failed"),
        "{refused:?}"
    );
    // Symptoms: the last successful read stops moving.
    let mut last = metric(&node, read).await;
    until("the node stops reading the store", async || {
        tokio::time::sleep(Duration::from_millis(2500)).await;
        let now = metric(&node, read).await;
        std::mem::replace(&mut last, now) == now
    })
    .await;
    // `skys3_control_store_live` stays 1: it shows only whether the store
    // answered since startup.
    assert_eq!(metric(&node, "skys3_control_store_live").await, 1.0);
    // Diagnosis: the log says why, and the data path goes on.
    assert!(log_text(dir.path()).contains("cannot read the control store's generation"));
    put(&s3, "kept", "b", b"beta").await;
    assert_eq!(get(&s3, "kept", "a").await, b"alpha");
    assert_eq!(metric(&node, synced).await, synced_before);

    // Remediation: the volume returns. The file store stays refused until
    // the node opens it again, at its next start.
    std::fs::rename(&away, &control_dir).unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(metric(&node, read).await, last);
    assert!(s3.create_bucket().bucket("new").send().await.is_err());
    assert!(node.terminate().await.success());
    let node = start(&config).await;
    // Verification: the store is read again, the identity copy is fresh,
    // and bucket changes work.
    assert!(metric(&node, read).await > last);
    assert!(metric(&node, synced).await > synced_before);
    assert_eq!(metric(&node, "skys3_control_store_live").await, 1.0);
    let s3 = node.s3();
    s3.create_bucket().bucket("new").send().await.unwrap();
    assert_eq!(get(&s3, "kept", "b").await, b"beta");
    assert!(node.terminate().await.success());
}

/// Runs `skys3 control <args>` with the configuration at `config`.
fn control(config: &Path, args: &[&OsStr]) -> std::process::Output {
    Command::new(BINARY)
        .arg("control")
        .args(args)
        .arg("--config")
        .arg(config)
        .output()
        .unwrap()
}

/// `control-store-loss`: the control store's registers are lost; the node
/// serves from its copy; the operator stops it, exports its copy, checks
/// the plan, rebuilds the store, and starts it again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drill_control_store_loss() {
    let dir = tempfile::tempdir().unwrap();
    let config = configure(dir.path());
    let node = start(&config).await;
    let s3 = node.s3();
    for bucket in ["one", "two"] {
        s3.create_bucket().bucket(bucket).send().await.unwrap();
        put(&s3, bucket, "k", bucket.as_bytes()).await;
    }
    let health = admin_json(&node.admin, "/v1/health").await;
    let generation = health["control_store"]["generation"].as_u64().unwrap();
    assert!(node.terminate().await.success());

    // The store loses its registers, and the node starts without them.
    let control_dir = dir.path().join("data/control");
    std::fs::remove_dir_all(&control_dir).unwrap();
    std::fs::create_dir(&control_dir).unwrap();
    let node = start(&config).await;
    // Symptoms and diagnosis: the node runs from its copy, and says the
    // store looks reset.
    assert_eq!(metric(&node, "skys3_control_store_live").await, 0.0);
    let health = admin_json(&node.admin, "/v1/health").await;
    assert_eq!(health["control_store"]["live"], false, "{health}");
    assert_eq!(health["control_store"]["generation"], generation);
    assert!(log_text(dir.path()).contains("the control store looks reset"));
    assert!(!control_dir.join("cluster.json").exists());
    assert_eq!(get(&node.s3(), "one", "k").await, b"one");
    // An export needs the node stopped.
    let export = dir.path().join("export.json");
    let args = [
        OsStr::new("export"),
        OsStr::new("--output"),
        export.as_os_str(),
    ];
    assert!(!control(&config, &args).status.success());
    assert!(node.terminate().await.success());

    // Remediation: export, check the plan, rebuild.
    let output = control(&config, &args);
    assert!(output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(&format!("a copy at generation {generation}")),
        "{stderr}"
    );
    let rebuild = |dry_run: bool| {
        let mut args = vec![
            OsStr::new("rebuild"),
            OsStr::new("--from"),
            export.as_os_str(),
        ];
        if dry_run {
            args.push(OsStr::new("--dry-run"));
        }
        control(&config, &args)
    };
    let output = rebuild(true);
    assert!(output.status.success(), "{output:?}");
    let plan = String::from_utf8_lossy(&output.stdout);
    assert!(
        plan.contains("2 bucket, 0 identity, and 0 shard registers"),
        "{plan}"
    );
    assert!(plan.ends_with("dry run: nothing written\n"), "{plan}");
    let output = rebuild(false);
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("rebuilt: wrote 3 registers"), "{stdout}");

    // Verification: the store answers at a newer generation, with every
    // bucket, and bucket changes work again.
    let node = start(&config).await;
    let health = admin_json(&node.admin, "/v1/health").await;
    assert_eq!(health["control_store"]["live"], true, "{health}");
    assert_eq!(health["control_store"]["generation"], generation + 1);
    assert_eq!(metric(&node, "skys3_control_store_live").await, 1.0);
    let s3 = node.s3();
    for bucket in ["one", "two"] {
        assert_eq!(get(&s3, bucket, "k").await, bucket.as_bytes());
    }
    s3.create_bucket().bucket("three").send().await.unwrap();
    // A store that is not lost is never rebuilt over.
    assert!(node.terminate().await.success());
    let output = control(&config, &args);
    assert!(output.status.success(), "{output:?}");
    assert_eq!(rebuild(false).status.code(), Some(1));
}

/// `node-down` and `cluster-power-loss`: the node is killed, and comes
/// back while the control store cannot be opened; it serves from its copy
/// and opens the store once it can.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drill_node_down() {
    let dir = tempfile::tempdir().unwrap();
    let config = configure(dir.path());
    let node = start(&config).await;
    let s3 = node.s3();
    s3.create_bucket().bucket("data").send().await.unwrap();
    let written: Vec<(String, Vec<u8>)> = (0..12_u8)
        .map(|i| (format!("k{i}"), body(i, 100 + usize::from(i) * 9_000)))
        .collect();
    for (key, data) in &written {
        put(&s3, "data", key, data).await;
    }
    let admin = node.admin.clone();
    node.kill();

    // Symptoms: scrapes fail (`up` is 0), and so do the probes.
    assert!(scrape(&admin).await.is_none());
    assert!(http_get(&admin, "/healthz").await.is_err());

    // The power returns before the control store's volume: the path holds
    // something the store cannot be opened from.
    let control_dir = dir.path().join("data/control");
    let away = dir.path().join("control-away");
    std::fs::rename(&control_dir, &away).unwrap();
    std::fs::write(&control_dir, b"not mounted").unwrap();
    let node = start(&config).await;
    let health = admin_json(&node.admin, "/v1/health").await;
    assert_eq!(health["ready"], true, "{health}");
    assert_eq!(health["control_store"]["live"], false, "{health}");
    assert!(
        health["disks"]
            .as_array()
            .unwrap()
            .iter()
            .all(|disk| disk["in_service"] == true),
        "{health}"
    );
    let s3 = node.s3();
    for (key, data) in &written {
        assert_eq!(&get(&s3, "data", key).await, data, "{key}");
    }
    // The store's volume returns, and the running node opens it.
    std::fs::remove_file(&control_dir).unwrap();
    std::fs::rename(&away, &control_dir).unwrap();
    until("the node opens the control store", async || {
        metric(&node, "skys3_control_store_live").await == 1.0
    })
    .await;
    assert!(log_text(dir.path()).contains("opened the control store"));
    s3.create_bucket().bucket("more").send().await.unwrap();
    assert!(node.terminate().await.success());
}

/// The bytes the file system holding `path` has free.
fn available(path: &Path) -> u64 {
    skys3_io::disk::available_bytes(path).unwrap()
}

/// `disk-space`: the node's file system falls below `disk_min_free_bytes`;
/// writes are refused and deletes admitted; the operator frees space, and
/// writes are admitted again without a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drill_disk_space() {
    const MIB: u64 = 1 << 20;
    // What the drill fills: room enough that other writers on the same
    // file system do not change the outcome.
    const BALLAST: u64 = 256 * MIB;
    let dir = tempfile::tempdir().unwrap();
    if available(dir.path()) < 4 * BALLAST {
        eprintln!("skipping drill_disk_space: the file system has too little free space");
        return;
    }
    let config = configure(dir.path());
    // Small segments, so the node's own files take little of the margin.
    edit_config(
        &config,
        "index_checkpoint_interval_seconds = 1",
        "index_checkpoint_interval_seconds = 1\nsegment_bytes = 1048576",
    );
    let node = start(&config).await;
    let s3 = node.s3();
    s3.create_bucket().bucket("data").send().await.unwrap();
    put(&s3, "data", "kept", b"kept").await;
    assert!(node.terminate().await.success());

    // Something else fills the file system, and the margin is set halfway
    // into what it wrote.
    let ballast = dir.path().join("ballast");
    {
        use std::io::Write;
        let mut file = std::fs::File::create(&ballast).unwrap();
        let chunk = vec![0x5a_u8; MIB as usize];
        for _ in 0..BALLAST / MIB {
            file.write_all(&chunk).unwrap();
        }
        file.sync_all().unwrap();
    }
    let margin = available(dir.path()) + BALLAST / 2;
    edit_config(
        &config,
        "segment_bytes = 1048576",
        &format!("segment_bytes = 1048576\ndisk_min_free_bytes = {margin}"),
    );
    let node = start(&config).await;
    let s3 = node.s3();

    // Symptoms: writes are refused, and counted.
    assert_eq!(
        refused_put(&s3, "data", "new").await,
        ("SlowDown".to_owned(), 503)
    );
    let refusals = "skys3_admission_refusals_total{reason=\"disk_space\"}";
    assert_eq!(metric(&node, refusals).await, 1.0);
    // Diagnosis: the log names the place that is low.
    assert!(log_text(dir.path()).contains("low on disk space"));
    // Reads and deletes go on.
    assert_eq!(get(&s3, "data", "kept").await, b"kept");
    s3.delete_object()
        .bucket("data")
        .key("kept")
        .send()
        .await
        .unwrap();

    // Remediation: space is freed; the node checks every second.
    std::fs::remove_file(&ballast).unwrap();
    until("writes are admitted", async || {
        s3.put_object()
            .bucket("data")
            .key("new")
            .body(b"new".to_vec().into())
            .send()
            .await
            .is_ok()
    })
    .await;
    assert!(log_text(dir.path()).contains("disk space recovered; writes are admitted"));
    assert_eq!(get(&s3, "data", "new").await, b"new");
    assert!(node.terminate().await.success());
}

/// The entry of `/v1/health`'s `disks` whose path is `path`.
fn disk_entry(health: &Value, path: &Path) -> Value {
    health["disks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|disk| disk["path"] == path.display().to_string())
        .unwrap_or_else(|| panic!("no disk at {}: {health}", path.display()))
        .clone()
}

/// `disk-out-of-service`: a disk's directory goes away under a running
/// node, and its next segment cannot be created; the node takes the disk
/// out of service. The operator stops the node, brings the disk back,
/// clears the fence once the disk is checked, and starts the node.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drill_disk_out_of_service() {
    let dir = tempfile::tempdir().unwrap();
    let config = configure(dir.path());
    // Small segments, so a few writes need a new one.
    edit_config(
        &config,
        "index_checkpoint_interval_seconds = 1",
        "index_checkpoint_interval_seconds = 1\nsegment_bytes = 131072",
    );
    let node = start(&config).await;
    let s3 = node.s3();
    s3.create_bucket().bucket("data").send().await.unwrap();
    let mut acknowledged = BTreeMap::new();
    for i in 0..4_u8 {
        let (key, data) = (format!("before/{i}"), body(i, 2_000));
        put(&s3, "data", &key, &data).await;
        acknowledged.insert(key, data);
    }

    // The disk goes away: its directory is no longer where the node
    // creates segments.
    let disk = dir.path().join("disk-a");
    let away = dir.path().join("disk-a-away");
    std::fs::rename(&disk, &away).unwrap();
    for i in 0..40_u8 {
        let (key, data) = (format!("during/{i}"), body(100 + i, 20_000));
        let answer = s3
            .put_object()
            .bucket("data")
            .key(&key)
            .body(data.clone().into())
            .send()
            .await;
        if answer.is_ok() {
            acknowledged.insert(key, data);
        }
    }

    // Symptoms: the gauge, readiness, and the disk's state.
    until("the disk is out of service", async || {
        metric(&node, "skys3_disks_out_of_service").await == 1.0
    })
    .await;
    let (status, _) = http_get(&node.admin, "/readyz").await.unwrap();
    assert_eq!(status, 503);
    let health = admin_json(&node.admin, "/v1/health").await;
    assert_eq!(health["ready"], false, "{health}");
    assert_eq!(
        health["not_ready"],
        serde_json::json!(["storage"]),
        "{health}"
    );
    let failed = disk_entry(&health, &disk);
    assert_eq!(failed["in_service"], false, "{health}");
    assert!(failed["error"].is_string(), "{health}");
    assert_eq!(
        disk_entry(&health, &dir.path().join("disk-b"))["in_service"],
        true
    );
    let label = failed["label"].as_str().unwrap().to_owned();
    let log = log_text(dir.path());
    assert!(log.contains("a disk was taken out of service"), "{log}");
    // The fence could not be written into the missing directory.
    assert!(log.contains("cannot fence the disk"), "{log}");

    // Remediation: stop the node, bring the disk back, and check it.
    assert!(node.terminate().await.success());
    std::fs::rename(&away, &disk).unwrap();
    // A disk that failed in place carries a fence, which keeps the node
    // from starting until the host restarts or an operator removes it.
    let disk_dir = DiskDir {
        label: Label::new(label.as_str()).unwrap(),
        path: disk.clone(),
    };
    fence(&disk_dir, boot_id().as_deref(), "sync failed (drill)").unwrap();
    let refused = Command::new(BINARY)
        .arg("--config")
        .arg(&config)
        .output()
        .unwrap();
    assert!(!refused.status.success());
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(stderr.contains("was taken out of service"), "{stderr}");
    assert!(stderr.contains("out-of-service.json"), "{stderr}");
    std::fs::remove_file(disk.join("out-of-service.json")).unwrap();

    // Verification: the node is ready, every disk in service, and every
    // acknowledged write readable.
    let node = start(&config).await;
    let health = admin_json(&node.admin, "/v1/health").await;
    assert_eq!(health["ready"], true, "{health}");
    assert_eq!(metric(&node, "skys3_disks_out_of_service").await, 0.0);
    let s3 = node.s3();
    for (key, data) in &acknowledged {
        assert_eq!(&get(&s3, "data", key).await, data, "{key}");
    }
    put(&s3, "data", "after", b"after").await;
    assert!(node.terminate().await.success());
}

// The flush and conflict drills, in process.

const NODE: &str = "node-1";
const CLUSTER: &str = "c-test";

fn node_id() -> NodeId {
    NodeId::new(NODE).unwrap()
}

/// The `write_back` bucket `photos`, with one shard, flushed to `team/` in
/// the remote bucket.
fn photos() -> BucketDocument {
    BucketDocument {
        bucket_id: BucketId::new("b-photos").unwrap(),
        name: BucketName::new("photos").unwrap(),
        mode: BucketMode::WriteBack,
        shards: ShardCount::new(1).unwrap(),
        replicas: 1,
        min_write_replicas: 1,
        clean_copies: 1,
        target: Some(RemoteTarget {
            endpoint: "https://s3.example".to_owned(),
            bucket: "remote".to_owned(),
            prefix: Some("team/".to_owned()),
        }),
        created_unix_ms: 0,
        lifecycle: None,
        proposal_id: ProposalId::new("p-1").unwrap(),
    }
}

/// A `PUT` of `key` with `body`, made now.
fn put_record(key: &str, body: &str) -> RecordBody {
    let etag = Md5::digest(body.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    RecordBody::Put(Put {
        key: key.to_owned(),
        size: body.len() as u64,
        last_modified_ms: u64::try_from(now.as_millis()).unwrap(),
        etag: etag.parse().unwrap(),
        inherited_identity: None,
        metadata: BTreeMap::from([("content-type".to_owned(), "text/plain".to_owned())]),
        tags: BTreeMap::new(),
        checksums: BTreeMap::new(),
        copy_source: None,
        data: PutData::Inline(Bytes::copy_from_slice(body.as_bytes())),
    })
}

/// A node's flush side: the shard of `photos` on a simulated disk, the
/// flush service with its metrics in a registry, followed as the node
/// follows it, and the admin API, over a simulated S3 store whose link
/// from this node an outage drops.
struct FlushRig {
    store: SimS3,
    shard: Shard<SimMount>,
    service: Arc<FlushService<SimS3, SimMount>>,
    registry: MetricsRegistry,
    admin: NodeAdmin<SimMount>,
    follower: tokio::task::JoinHandle<()>,
}

impl FlushRig {
    async fn new() -> Self {
        let store = SimS3::new(1, SimS3Config::default());
        let mount = SimDisk::new(1).mount();
        let log_config = LogConfig {
            inline_max_bytes: 4096,
            segment_bytes: 1 << 20,
            group_commit_max_delay: Duration::ZERO,
            group_commit_max_bytes: 1 << 20,
        };
        let (log, _) = SegmentLog::open(mount.clone(), log_config, Arc::new(MonotonicClock::new()))
            .await
            .unwrap();
        let index_config = IndexConfig {
            checkpoint_interval: Duration::from_secs(10),
            cache_bytes: 1 << 20,
        };
        let index = Index::open_sim(&mount, "index.redb", &index_config).unwrap();
        let set = ShardSet::new(Arc::new(index), log, BlockingPool::inline("index"));
        let bucket = photos();
        let shard = set
            .open(&ShardConfig {
                bucket_id: bucket.bucket_id.clone(),
                shard: ShardId::new(0),
                epoch: Epoch::new(1),
                primary: node_id(),
                members: vec![node_id()],
                learners: Vec::new(),
                min_write_replicas: 1,
                replicas: 1,
                proposal_id: ProposalId::new("p-2").unwrap(),
            })
            .await
            .unwrap();
        let registry = MetricsRegistry::new();
        let config: Config = format!(
            "[cluster]\ncluster_id = \"{CLUSTER}\"\n\
             [control_store]\netcd_endpoints = [\"https://etcd.example:2379\"]\n\
             [buckets.photos]\nmode = \"write_back\"\n"
        )
        .parse()
        .unwrap();
        let connected = store.from_source(NODE);
        let service = FlushService::new(
            ClusterId::new(CLUSTER).unwrap(),
            FlushSettings {
                min_backoff: Duration::from_millis(10),
                max_backoff: Duration::from_millis(200),
                ..FlushSettings::default()
            },
            Box::new(move |_: &RemoteTarget| connected.clone()),
            FlushMetrics::register(&registry),
        )
        .with_buckets(config.buckets().clone());
        let service = Arc::new(service);
        // As the node's flush follower does, more often.
        let follower = tokio::spawn({
            let (service, set, buckets) = (Arc::clone(&service), set.clone(), vec![bucket.clone()]);
            async move {
                loop {
                    service.reconcile(&buckets, &set).await;
                    service.refresh_metrics();
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        });
        let admin = NodeAdmin {
            node_id: node_id(),
            cluster_id: ClusterId::new(CLUSTER).unwrap(),
            buckets: Arc::new(move || vec![bucket.clone()]),
            shards: LocalShards::new(set, node_id()),
            disks: Vec::new(),
            control: Arc::new(Mutex::new(ControlState::default())),
            health: Health::new(),
            flush: service.clone(),
        };
        let rig = Self {
            store,
            shard,
            service,
            registry,
            admin,
            follower,
        };
        until("the probe and the import finish", async || {
            rig.service
                .status(&photos().bucket_id)
                .is_some_and(|status| {
                    matches!(status.probe, ProbeStatus::Done { .. })
                        && status
                            .import
                            .is_some_and(|import| import.checkpoint == ImportCheckpoint::Done)
                })
        })
        .await;
        rig
    }

    /// The value of the series `name{bucket="photos"}` in the node's
    /// metrics.
    fn bucket_metric(&self, name: &str) -> f64 {
        let samples = samples(&self.registry.encode().unwrap());
        value(&samples, &format!("{name}{{bucket=\"photos\"}}"))
    }

    /// Calls the admin API.
    async fn call(&self, method: Method, path: &str) -> (StatusCode, Value) {
        let response = self.admin.call(&method, path).await.expect("a known route");
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&body).unwrap())
    }

    /// `GET /v1/buckets/photos`'s `flush` object.
    async fn flush_status(&self) -> Value {
        let (status, body) = self.call(Method::GET, "/v1/buckets/photos").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body["flush"].clone()
    }

    /// Commits a write of `key`, as a client's PUT does.
    async fn write(&self, key: &str, body: &str) {
        self.shard.commit(put_record(key, body)).await.unwrap();
    }

    /// The remote's body of `key` and its metadata `writer`, if it holds
    /// the key.
    fn remote(&self, key: &str) -> Option<(String, Option<String>)> {
        self.store.object(&format!("team/{key}")).map(|object| {
            (
                String::from_utf8(object.body.to_vec()).unwrap(),
                object.info.metadata.get("writer").map(str::to_owned),
            )
        })
    }

    /// Writes `body` to `key` at the remote, as another writer would.
    async fn out_of_band(&self, key: &str, body: &str) {
        let mut metadata = UserMetadata::new();
        metadata.insert("writer", "someone-else").unwrap();
        let request =
            PutObject::new(format!("team/{key}"), body.to_owned()).with_metadata(metadata);
        self.store.put_object(request).await.unwrap();
    }

    /// Waits until nothing is dirty or being flushed.
    async fn drained(&self) {
        until("the bucket is flushed", async || {
            let flush = self.flush_status().await;
            flush["dirty"] == 0 && flush["flushing"] == 0 && flush["dirty_bytes"] == 0
        })
        .await;
    }

    async fn shutdown(self) {
        self.follower.abort();
        self.service.shutdown().await;
    }
}

/// Runs `drill` on a single-threaded runtime: the simulated disk's index
/// runs its I/O inline.
fn in_process<F: Future<Output = ()>>(drill: impl FnOnce() -> F) {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(drill());
}

/// `dirty-data-age` and `flush-stalled`: the node's link to the remote
/// target drops; the dirty data ages, flushes are retried, and the bucket
/// status names the error; the link returns, and the backlog drains with
/// no operator action.
#[test]
fn drill_remote_outage() {
    in_process(async || {
        let rig = FlushRig::new().await;
        rig.write("before", "flushed before the outage").await;
        rig.drained().await;

        rig.store.set_source_down(NODE, true);
        rig.write("during", "written during the outage").await;
        // Symptoms: the series the alerts read.
        until("flushes are retried", async || {
            rig.bucket_metric("skys3_flush_retries_total") >= 3.0
        })
        .await;
        until("the dirty data ages", async || {
            rig.bucket_metric("skys3_oldest_dirty_age_seconds") >= 1.0
                && rig.bucket_metric("skys3_flush_lag_seconds") >= 1.0
        })
        .await;
        assert!(rig.bucket_metric("skys3_dirty_bytes") > 0.0);
        // Diagnosis: the bucket status names the error.
        let flush = rig.flush_status().await;
        assert_eq!(flush["dirty"], 1, "{flush}");
        let errors = flush["errors"].as_array().unwrap();
        assert_eq!(errors.len(), 1, "{flush}");
        assert!(
            errors[0].as_str().unwrap().starts_with("during:"),
            "{flush}"
        );
        assert!(rig.remote("during").is_none());

        // Remediation: the link returns; the flushers need nothing else.
        rig.store.set_source_down(NODE, false);
        rig.drained().await;
        until("the gauges return to 0", async || {
            rig.bucket_metric("skys3_oldest_dirty_age_seconds") == 0.0
                && rig.bucket_metric("skys3_flush_lag_seconds") == 0.0
        })
        .await;
        let flush = rig.flush_status().await;
        assert_eq!(flush["errors"], Value::Array(Vec::new()), "{flush}");
        assert_eq!(
            rig.remote("during"),
            Some(("written during the outage".to_owned(), None))
        );
        // The retries stop.
        let retries = rig.bucket_metric("skys3_flush_retries_total");
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(rig.bucket_metric("skys3_flush_retries_total"), retries);
        rig.shutdown().await;
    });
}

/// `held-conflicts` and `discarded-conflicts`: another writer changes keys
/// at the remote; the flushes find it and hold the keys; the operator
/// inspects them, keeps the local version of one, and the remote's of the
/// other, which the discard counter records.
#[test]
fn drill_out_of_band_writes() {
    in_process(async || {
        let rig = FlushRig::new().await;
        for key in ["kept", "dropped"] {
            rig.write(key, &format!("{key}: ours, first")).await;
        }
        rig.drained().await;
        for key in ["kept", "dropped"] {
            rig.out_of_band(key, &format!("{key}: theirs")).await;
            rig.write(key, &format!("{key}: ours, second")).await;
        }

        // Symptoms: keys held, and the conflicts counted.
        until("both keys are held", async || {
            rig.bucket_metric("skys3_conflicted_keys") == 2.0
        })
        .await;
        assert_eq!(rig.bucket_metric("skys3_flush_conflicts_total"), 2.0);
        // Held keys never drain: they age, but are not flush lag.
        until("the held keys age", async || {
            rig.bucket_metric("skys3_oldest_dirty_age_seconds") >= 1.0
        })
        .await;
        assert_eq!(rig.bucket_metric("skys3_flush_lag_seconds"), 0.0);
        // Diagnosis: the conflicts, each with the remote's write.
        let (status, list) = rig.call(Method::GET, "/v1/buckets/photos/conflicts").await;
        assert_eq!(status, StatusCode::OK, "{list}");
        assert_eq!(list["conflict_policy"], "hold");
        let conflicts = list["conflicts"].as_array().unwrap();
        let keys: Vec<&str> = conflicts
            .iter()
            .map(|c| c["key"].as_str().unwrap())
            .collect();
        assert_eq!(keys, ["dropped", "kept"], "{list}");
        for conflict in conflicts {
            // Not a SkyS3 write: no write identity.
            assert_eq!(conflict["remote_identity"], Value::Null, "{list}");
            assert!(conflict["remote_etag"].is_string(), "{list}");
        }
        assert_eq!(
            rig.remote("kept"),
            Some(("kept: theirs".to_owned(), Some("someone-else".to_owned())))
        );

        // Remediation: keep the local version of one key ...
        let (status, body) = rig
            .call(Method::POST, "/v1/buckets/photos/conflicts/overwrite/kept")
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["state"], "dirty");
        until("the overwrite lands", async || {
            rig.remote("kept") == Some(("kept: ours, second".to_owned(), None))
        })
        .await;
        assert_eq!(
            rig.bucket_metric("skys3_flush_conflicts_overwritten_total"),
            1.0
        );
        // ... and adopt the other writer's version of the other.
        let (status, body) = rig
            .call(
                Method::POST,
                "/v1/buckets/photos/conflicts/discard_local/dropped",
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        until("the discard lands", async || {
            let entry = rig.shard.entry("dropped").await.unwrap().unwrap();
            entry.state == EntryState::Evicted
        })
        .await;
        // `discarded-conflicts` symptoms: the counter its alert reads.
        assert_eq!(
            rig.bucket_metric("skys3_flush_conflicts_discarded_total"),
            1.0
        );
        assert_eq!(
            rig.remote("dropped"),
            Some((
                "dropped: theirs".to_owned(),
                Some("someone-else".to_owned())
            ))
        );

        // Verification: nothing held, nothing dirty.
        until("nothing is held", async || {
            rig.bucket_metric("skys3_conflicted_keys") == 0.0
        })
        .await;
        rig.drained().await;
        let (_, list) = rig.call(Method::GET, "/v1/buckets/photos/conflicts").await;
        assert_eq!(list["conflicts"], Value::Array(Vec::new()));
        // A key no flusher holds is refused.
        let (status, _) = rig
            .call(Method::POST, "/v1/buckets/photos/conflicts/hold/kept")
            .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        rig.shutdown().await;
    });
}
