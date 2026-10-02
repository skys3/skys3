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

/// Runs `skys3 control <args>` with the configuration at `config`.
fn control(config: &std::path::Path, args: &[&std::ffi::OsStr]) -> std::process::Output {
    Command::new(BINARY)
        .arg("control")
        .arg(args[0])
        .arg("--config")
        .arg(config)
        .args(&args[1..])
        .output()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_command_line_rebuilds_a_lost_control_store() {
    use std::ffi::OsStr;

    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("node.log");
    let (config, gateway, admin) = configure(dir.path());
    let node = Process::start(&config, &gateway, &admin, &log).await;
    let s3 = node.s3();
    s3.create_bucket().bucket("data").send().await.unwrap();
    put(&s3, "data", "k", b"kept").await;
    assert!(node.terminate().await.success());
    std::fs::remove_dir_all(dir.path().join("data/control")).unwrap();

    let export = dir.path().join("export.json");
    let output = control(
        &config,
        &[
            OsStr::new("export"),
            OsStr::new("--output"),
            export.as_os_str(),
        ],
    );
    assert!(output.status.success(), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stderr).contains("a copy at generation"));
    // Without --output, the export goes to standard output.
    let output = control(&config, &[OsStr::new("export")]);
    assert!(output.status.success(), "{output:?}");
    let printed = skys3_control::ControlExport::from_json(&output.stdout).unwrap();
    let written =
        skys3_control::ControlExport::from_json(&std::fs::read(&export).unwrap()).unwrap();
    assert_eq!(printed, written);

    let from = [OsStr::new("--from"), export.as_os_str()];
    let rebuild = |extra: &[&OsStr]| {
        let args: Vec<&OsStr> = [OsStr::new("rebuild")]
            .into_iter()
            .chain(from)
            .chain(extra.iter().copied())
            .collect();
        control(&config, &args)
    };
    let output = rebuild(&[OsStr::new("--dry-run")]);
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("1 bucket, 0 identity, and 0 shard registers"),
        "{stdout}"
    );
    assert!(stdout.ends_with("dry run: nothing written\n"), "{stdout}");
    // Mistakes on the command line.
    let output = rebuild(&[OsStr::new("--lost"), OsStr::new("Not Valid")]);
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    let output = rebuild(&[OsStr::new("--prefer"), OsStr::new("Not Valid")]);
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    let output = control(
        &config,
        &[
            OsStr::new("rebuild"),
            OsStr::new("--from"),
            OsStr::new("/missing.json"),
        ],
    );
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    // A rebuild that is refused fails.
    let output = rebuild(&[OsStr::new("--lost"), written.node_id.as_str().as_ref()]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stderr).contains("refused"));
    // The chosen copy must be one of the newest generation.
    let output = rebuild(&[OsStr::new("--prefer"), OsStr::new("node-9")]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("of node node-9, does not exist"),
        "{output:?}"
    );

    let output = rebuild(&[OsStr::new("--prefer"), written.node_id.as_str().as_ref()]);
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("rebuilt: wrote 2 registers"), "{stdout}");

    let node = Process::start(&config, &gateway, &admin, &log).await;
    let s3 = node.s3();
    assert_eq!(get(&s3, "data", "k").await, b"kept");
    s3.create_bucket().bucket("more").send().await.unwrap();
    assert!(node.terminate().await.success());
}
