//! The illustrative configuration of design §14 and the configuration
//! reference, read from the documents themselves so the tests track them.

use std::collections::BTreeSet;

use skys3_config::{
    AckPolicy, AckTimeoutMode, Config, ControlStoreBackend, FailureDomain, LogFormat,
    TargetTransport,
};
use skys3_types::{BucketMode, BucketName, ShardCount};

const DESIGN: &str = include_str!("../../../docs/skys3-design.md");
const REFERENCE: &str = include_str!("../../../docs/skys3-config.md");

/// The first TOML code block of design section 14.
fn design_example() -> &'static str {
    let section = DESIGN
        .split_once("\n## 14. ")
        .expect("the design has a section 14")
        .1;
    let block = section
        .split_once("```toml\n")
        .expect("section 14 has a TOML example")
        .1;
    block
        .split_once("\n```")
        .expect("the TOML example is closed")
        .0
}

/// The dotted path of every key in `text`, with bucket tables collapsed to
/// `buckets.<name>`.
fn keys(text: &str) -> BTreeSet<String> {
    fn walk(prefix: &str, table: &toml::Table, out: &mut BTreeSet<String>) {
        for (key, value) in table {
            let path = if prefix.is_empty() {
                key.clone()
            } else if prefix == "buckets" && key != "defaults" {
                "buckets.<name>".to_owned()
            } else {
                format!("{prefix}.{key}")
            };
            match value {
                toml::Value::Table(inner) => walk(&path, inner, out),
                _ => {
                    out.insert(path);
                }
            }
        }
    }
    let table: toml::Table = text.parse().expect("valid TOML");
    let mut out = BTreeSet::new();
    walk("", &table, &mut out);
    out
}

#[test]
fn the_design_example_loads_unchanged() {
    let config: Config = design_example().parse().unwrap_or_else(|e| panic!("{e}"));

    assert_eq!(config.cluster().cluster_id.as_str(), "skys3-prod-a");
    assert_eq!(config.cluster().failure_domain, FailureDomain::Node);
    let ControlStoreBackend::Etcd { endpoints } = &config.control_store().backend else {
        panic!("the example uses etcd");
    };
    assert_eq!(endpoints.len(), 3);
    assert_eq!(config.control_store().prefix, "skys3-prod-a/");
    assert_eq!(
        config.replication().replica_ack_timeout_mode,
        AckTimeoutMode::WaitThrough
    );

    let buckets = config.buckets();
    assert_eq!(buckets.defaults.mode, BucketMode::WriteBack);
    assert_eq!(
        buckets.defaults.shards_per_bucket,
        ShardCount::new(8).unwrap()
    );
    assert_eq!(buckets.defaults.target_transport, TargetTransport::Auto);

    let archive = buckets.get(&BucketName::new("archive").unwrap());
    assert_eq!(archive.mode, BucketMode::Local);
    assert_eq!(archive.replication, buckets.defaults.replication);
    let backup = archive.backup_target.as_ref().unwrap();
    assert_eq!(backup.endpoint, "https://s3.skys3-prod-eu.example.internal");
    assert_eq!(backup.bucket, "archive");
    let snapshots = archive.snapshot_target.as_ref().unwrap();
    assert_eq!(snapshots.bucket, "example-skys3-snapshots");
    assert_eq!(snapshots.prefix.as_deref(), Some("archive/"));

    let receiver = buckets.get(&BucketName::new("archive-from-eu").unwrap());
    assert_eq!(
        receiver.peer_source.as_ref().unwrap().as_str(),
        "skys3-prod-eu"
    );
    assert_eq!(receiver.snapshot_target, None);
}

/// The example's values are the defaults, so a configuration with only the
/// required keys resolves to the same settings.
#[test]
fn the_defaults_are_the_design_example_values() {
    let example: Config = design_example().parse().unwrap();
    let minimal: Config = r#"
        [cluster]
        cluster_id = "skys3-prod-a"
        [control_store]
        etcd_endpoints = ["https://etcd-1.example.internal:2379"]
    "#
    .parse()
    .unwrap();

    assert_eq!(
        minimal.control_store().prefix,
        example.control_store().prefix
    );
    assert_eq!(minimal.replication(), example.replication());
    assert_eq!(minimal.storage(), example.storage());
    assert_eq!(minimal.cache(), example.cache());
    assert_eq!(minimal.flush(), example.flush());
    assert_eq!(minimal.ec(), example.ec());
    assert_eq!(minimal.buckets().defaults, example.buckets().defaults);
    assert!(minimal.buckets().named.is_empty());
    assert_eq!(minimal.peering(), example.peering());
    let mut identity = example.identity().clone();
    // The example's static credential has no default.
    assert!(identity.static_credentials.remove("bootstrap").is_some());
    assert_eq!(minimal.identity(), &identity);

    assert_eq!(minimal.admin().listen.to_string(), "127.0.0.1:7490");
    assert_eq!(minimal.admin().token_file, None);
    assert_eq!(minimal.logging().filter, "info");
    assert_eq!(minimal.logging().format, LogFormat::Text);
    assert_eq!(minimal.buckets().defaults.ack_policy, AckPolicy::Local);
}

/// A configuration that sets every key the schema accepts, each to a
/// non-default value where one is allowed.
const EVERY_KEY: &str = r#"
[cluster]
cluster_id = "c1"
failure_domain = "zone"

[control_store]
backend = "s3"
endpoint = "https://control.example"
bucket = "control"
prefix = "c1-registers/"
allow_correlated_control_store = true
coordinator_lease_seconds = 12
config_poll_interval_seconds = 20

[replication]
replica_ack_timeout_ms = 2000
replica_ack_timeout_mode = "fail_fast"
member_suspect_after_ms = 3500
lease_renew_interval_ms = 900
primary_lease_ms = 3000
primary_grace_ms = 5000
assumed_clock_drift = 0.02
node_forget_after_hours = 48

[storage]
inline_max_bytes = 65536
extent_bytes = 2097152
segment_bytes = 536870912
group_commit_max_delay_us = 250
group_commit_max_bytes = 8388608
index_checkpoint_interval_seconds = 5
compaction_live_threshold = 0.4
read_registration_ttl_seconds = 60
read_registration_renew_interval_seconds = 20

[cache]
hot_cache_bytes_per_node = 1073741824
cache_max_bytes_per_node = 10737418240
reserve_fraction = 0.2

[flush]
ack_policy = "write_through"
flush_min_concurrency_per_shard = 2
flush_max_concurrency_per_shard = 32
flush_max_inflight_bytes_per_target = 536870912
streaming_flush_min_bytes = 33554432
flush_part_bytes = 16777216
flush_conflict_policy = "overwrite"
max_dirty_bytes = 1099511627776

[ec]
parity_fragments = 3
max_data_fragments = 6
min_eligible_nodes = 7
fragment_release_delay_seconds = 30
fragment_orphan_after_seconds = 7200
repair_bytes_per_second_per_node = 52428800

[buckets.defaults]
mode = "local"
shards_per_bucket = 16
replicas = 5
min_write_replicas = 3
clean_copies = 2
import_parallel_streams = 8
ec_min_object_bytes = 8388608
ec_stripe_data_bytes = 33554432
ec_after_seconds = 300
backup_ack = "write_through"
index_snapshot_interval_seconds = 600
target_transport = "s3"

[buckets."logs.example"]
mode = "write_back"
shards_per_bucket = 4
replicas = 3
min_write_replicas = 2
clean_copies = 3
import_parallel_streams = 64
ec_min_object_bytes = 1048576
ec_stripe_data_bytes = 1048576
ec_after_seconds = 60
backup_ack = "local"
index_snapshot_interval_seconds = 60
target_transport = "native"
ack_policy = "local"
flush_conflict_policy = "discard_local"
snapshot_target = "https://snapshots.example/snaps/logs/"

[buckets.mirror]
backup_target = "https://peer.example/mirror"
peer_source = "c2"

[peering]
quic_listen = "[::]:8443"
congestion_control = "bbr"
peer_frame_bytes = 131072
peer_connect_timeout_ms = 1000
peer_connections_per_shard = 16
peer_max_inflight_bytes = 134217728
peer_staging_quota_bytes = 10737418240
peer_staging_ttl_seconds = 3600

[identity]
anonymous_access = true
anonymous_policy = '{"Version": "2012-10-17", "Statement": {"Effect": "Allow", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::public/*"}}'
sts_web_identity = false
session_default_seconds = 900
session_maximum_seconds = 43200
identity_max_staleness_hours = 12
oidc_clock_skew_seconds = 0

[identity.static_credentials.bootstrap]
access_key_id = "AKIASKYS3BOOTSTRAP"
secret_access_key_file = "/etc/skys3/bootstrap.secret"
policy = '{"Version": "2012-10-17", "Statement": {"Effect": "Allow", "Action": "*", "Resource": "*"}}'

[admin]
listen = "0.0.0.0:9000"
token_file = "/etc/skys3/admin-token"

[logging]
filter = "info,skys3_shard=debug"
format = "json"
"#;

#[test]
fn every_key_parses_and_is_resolved() {
    let config: Config = EVERY_KEY.parse().unwrap_or_else(|e| panic!("{e}"));
    let ControlStoreBackend::S3 { endpoint, bucket } = &config.control_store().backend else {
        panic!("s3 backend expected");
    };
    assert_eq!(
        (endpoint.as_str(), bucket.as_str()),
        ("https://control.example", "control")
    );
    assert_eq!(config.control_store().prefix, "c1-registers/");
    assert_eq!(config.control_store().coordinator_lease().as_secs(), 12);
    assert_eq!(config.control_store().config_poll_interval().as_secs(), 20);
    assert_eq!(config.identity().oidc_clock_skew().as_secs(), 0);
    assert!(config.identity().anonymous_policy.is_some());
    let bootstrap = &config.identity().static_credentials["bootstrap"];
    assert_eq!(bootstrap.access_key_id, "AKIASKYS3BOOTSTRAP");
    assert_eq!(bootstrap.policy, skys3_types::policy::Policy::allow_all());

    let defaults = &config.buckets().defaults;
    assert_eq!(defaults.mode, BucketMode::Local);
    assert_eq!(defaults.ack_policy, AckPolicy::WriteThrough, "from [flush]");
    assert_eq!(defaults.ec_after().as_secs(), 300);
    assert_eq!(defaults.index_snapshot_interval().as_secs(), 600);

    let logs = config
        .buckets()
        .get(&BucketName::new("logs.example").unwrap());
    assert_eq!(logs.mode, BucketMode::WriteBack);
    assert_eq!(logs.replication.clean_copies, 3);
    assert_eq!(logs.ack_policy, AckPolicy::Local);
    assert_eq!(logs.backup_target, None);
    assert_eq!(logs.snapshot_target.as_ref().unwrap().bucket, "snaps");

    let mirror = config.buckets().get(&BucketName::new("mirror").unwrap());
    assert_eq!(
        mirror.mode,
        BucketMode::Local,
        "inherited from the defaults"
    );
    assert_eq!(mirror.replication.replicas, 5);
    assert_eq!(
        mirror.snapshot_target, mirror.backup_target,
        "defaults to the backup target"
    );
    assert_eq!(mirror.peer_source.as_ref().unwrap().as_str(), "c2");

    let unnamed = config.buckets().get(&BucketName::new("other").unwrap());
    assert_eq!(unnamed, defaults);
}

#[test]
fn the_reference_documents_every_key() {
    let mut all = keys(design_example());
    all.extend(keys(EVERY_KEY));
    let missing: Vec<_> = all
        .iter()
        .filter(|path| {
            let key = path.rsplit('.').next().unwrap();
            !REFERENCE.contains(&format!("`{key}`"))
        })
        .collect();
    assert!(
        missing.is_empty(),
        "missing from docs/skys3-config.md: {missing:?}"
    );
    for section in [
        "[cluster]",
        "[control_store]",
        "[replication]",
        "[storage]",
        "[cache]",
        "[flush]",
        "[ec]",
        "[buckets.defaults]",
        "[buckets.<name>]",
        "[peering]",
        "[identity]",
        "[admin]",
        "[logging]",
    ] {
        assert!(
            REFERENCE.contains(&format!("`{section}`")),
            "{section} is not documented"
        );
    }
}

#[test]
fn the_every_key_fixture_covers_the_design_example() {
    let fixture = keys(EVERY_KEY);
    let uncovered: Vec<_> = keys(design_example())
        .into_iter()
        .filter(|path| !fixture.contains(path))
        .filter(|path| path != "control_store.etcd_endpoints")
        .collect();
    assert!(uncovered.is_empty(), "add to EVERY_KEY: {uncovered:?}");
}
