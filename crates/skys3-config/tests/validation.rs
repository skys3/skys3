//! Each load-time rule, broken by a configuration.

use skys3_config::{Config, ConfigError, ControlStoreBackend, ReplicationConfig, Violations};
use skys3_types::BucketName;

const BASE: &str = r#"
[cluster]
cluster_id = "skys3-prod-a"

[control_store]
etcd_endpoints = ["https://etcd-1.example.internal:2379"]
"#;

/// The base configuration plus `extra` sections.
fn load(extra: &str) -> Result<Config, ConfigError> {
    format!("{BASE}\n{extra}").parse()
}

fn violations_of(text: &str) -> Violations {
    match text.parse::<Config>() {
        Err(ConfigError::Invalid(violations)) => violations,
        Err(other) => panic!("expected rule violations, got: {other}"),
        Ok(_) => panic!("the configuration was accepted:\n{text}"),
    }
}

/// Asserts that `extra` breaks exactly the rules reported at `keys`.
#[track_caller]
fn assert_violations(extra: &str, keys: &[&str]) {
    let violations = violations_of(&format!("{BASE}\n{extra}"));
    let reported: Vec<_> = violations
        .as_slice()
        .iter()
        .map(|v| v.key.as_str())
        .collect();
    assert_eq!(reported, keys, "{violations}");
}

#[track_caller]
fn parse_error(text: &str) -> String {
    match text.parse::<Config>() {
        Err(ConfigError::Parse(error)) => error.to_string(),
        Err(other) => panic!("expected a parse error, got: {other}"),
        Ok(_) => panic!("the configuration was accepted:\n{text}"),
    }
}

#[test]
fn the_base_configuration_is_valid() {
    load("").unwrap();
}

// Rules the plan names (M0-03).

#[test]
fn primary_grace_must_cover_the_drift_adjusted_lease() {
    // 4000 × 1.01 / 0.99 + 500 = 4581 (rounded up).
    assert_violations(
        "[replication]\nprimary_grace_ms = 4580",
        &["replication.primary_grace_ms"],
    );
    load("[replication]\nprimary_grace_ms = 4581").unwrap();
    // More drift raises the bound: 4000 × 1.1 / 0.9 + 500 = 5389.
    assert_violations(
        "[replication]\nassumed_clock_drift = 0.1\nprimary_grace_ms = 5388",
        &["replication.primary_grace_ms"],
    );
    let violations = violations_of(&format!("{BASE}\n[replication]\nprimary_grace_ms = 4000"));
    let message = &violations.as_slice()[0].message;
    assert!(message.contains("at least 4581"), "{message}");
}

#[test]
fn clock_drift_must_be_a_fraction_below_one() {
    for drift in ["1.0", "-0.01", "nan", "inf"] {
        assert_violations(
            &format!("[replication]\nassumed_clock_drift = {drift}"),
            &["replication.assumed_clock_drift"],
        );
    }
}

#[test]
fn wait_through_timeout_must_outlast_member_removal() {
    // 3000 ms suspicion + 1000 ms CAS allowance; the timeout must exceed it.
    assert_violations(
        "[replication]\nreplica_ack_timeout_ms = 4000",
        &["replication.replica_ack_timeout_ms"],
    );
    load("[replication]\nreplica_ack_timeout_ms = 4001").unwrap();
    assert_violations(
        "[replication]\nmember_suspect_after_ms = 4500",
        &["replication.replica_ack_timeout_ms"],
    );
    // Fail-fast mode has no such bound.
    load("[replication]\nreplica_ack_timeout_mode = \"fail_fast\"\nreplica_ack_timeout_ms = 2000")
        .unwrap();
    assert_eq!(ReplicationConfig::CAS_ALLOWANCE.as_millis(), 1000);
}

#[test]
fn min_write_replicas_must_not_exceed_replicas() {
    assert_violations(
        "[buckets.defaults]\nreplicas = 2\nmin_write_replicas = 3\nclean_copies = 1",
        &["buckets.defaults.min_write_replicas"],
    );
    assert_violations(
        "[buckets.defaults]\nmin_write_replicas = 0",
        &["buckets.defaults.min_write_replicas"],
    );
    assert_violations(
        "[buckets.defaults]\nreplicas = 0",
        &["buckets.defaults.replicas", "buckets.defaults.clean_copies"],
    );
}

#[test]
fn clean_copies_must_not_exceed_replicas() {
    assert_violations(
        "[buckets.defaults]\nclean_copies = 4",
        &["buckets.defaults.clean_copies"],
    );
    // Both replication rules are reported together.
    assert_violations(
        "[buckets.defaults]\nreplicas = 1\nclean_copies = 2",
        &[
            "buckets.defaults.min_write_replicas",
            "buckets.defaults.clean_copies",
        ],
    );
    load("[buckets.defaults]\nclean_copies = 0").unwrap();
    load("[buckets.defaults]\nclean_copies = 3").unwrap();
}

#[test]
fn shards_per_bucket_is_at_most_256() {
    assert_violations(
        "[buckets.defaults]\nshards_per_bucket = 257",
        &["buckets.defaults.shards_per_bucket"],
    );
    assert_violations(
        "[buckets.defaults]\nshards_per_bucket = 0",
        &["buckets.defaults.shards_per_bucket"],
    );
    load("[buckets.defaults]\nshards_per_bucket = 256").unwrap();
}

#[test]
fn cluster_id_must_fit_the_write_identity() {
    let with_id = |id: &str| {
        format!(
            "[cluster]\ncluster_id = \"{id}\"\n[control_store]\netcd_endpoints = [\"http://e:2379\"]"
        )
    };
    with_id(&"a".repeat(24)).parse::<Config>().unwrap();
    for bad in [
        "a".repeat(25),
        String::new(),
        "Prod".to_owned(),
        "prod.a".to_owned(),
    ] {
        let violations = violations_of(&with_id(&bad));
        assert!(
            violations.contains_key("cluster.cluster_id"),
            "{bad:?}: {violations}"
        );
        assert_eq!(violations.len(), 1, "{violations}");
    }
}

#[test]
fn lease_renewal_must_be_shorter_than_the_lease() {
    assert_violations(
        "[replication]\nprimary_lease_ms = 1000",
        &["replication.lease_renew_interval_ms"],
    );
}

#[test]
fn read_registration_renewal_must_be_shorter_than_its_ttl() {
    assert_violations(
        "[storage]\nread_registration_renew_interval_seconds = 30",
        &["storage.read_registration_renew_interval_seconds"],
    );
}

// Reporting.

#[test]
fn every_violation_is_reported_at_once() {
    let violations = violations_of(
        r#"
        [cluster]
        cluster_id = "this-cluster-id-is-much-too-long"
        [control_store]
        etcd_endpoints = ["https://etcd-1:2379"]
        [replication]
        primary_grace_ms = 4000
        lease_renew_interval_ms = 5000
        member_suspect_after_ms = 6000
        [storage]
        read_registration_renew_interval_seconds = 40
        [buckets.defaults]
        shards_per_bucket = 300
        clean_copies = 4
        "#,
    );
    let keys: Vec<_> = violations
        .as_slice()
        .iter()
        .map(|v| v.key.as_str())
        .collect();
    assert_eq!(
        keys,
        [
            "cluster.cluster_id",
            "replication.primary_grace_ms",
            "replication.lease_renew_interval_ms",
            "replication.replica_ack_timeout_ms",
            "storage.read_registration_renew_interval_seconds",
            "buckets.defaults.shards_per_bucket",
            "buckets.defaults.clean_copies",
        ]
    );
    let text = ConfigError::from(violations).to_string();
    assert!(
        text.starts_with("invalid configuration (7 problems):"),
        "{text}"
    );
    assert!(
        text.contains("\n  - replication.primary_grace_ms: is 4000;"),
        "{text}"
    );
}

// Unknown keys and parse errors.

#[test]
fn unknown_keys_are_rejected_everywhere() {
    for (text, key) in [
        (
            format!("{BASE}\n[replication]\nprimary_lease = 4000"),
            "primary_lease",
        ),
        (
            format!("{BASE}\n[replicaton]\nprimary_lease_ms = 4000"),
            "replicaton",
        ),
        (
            format!("{BASE}\n[buckets.defaults]\nreplica = 3"),
            "replica",
        ),
        (
            format!("{BASE}\n[buckets.archive]\nbackup_targt = \"x\""),
            "backup_targt",
        ),
        (format!("{BASE}\n[admin]\ntoken = \"secret\""), "token"),
        (format!("{BASE}\n[logging]\nlevel = \"debug\""), "level"),
        (
            "[cluster]\ncluster_id = \"a\"\nregion = \"x\"\n[control_store]\netcd_endpoints = []"
                .to_owned(),
            "region",
        ),
        (
            "[cluster]\ncluster_id = \"a\"\n[control_store]\netcd_endpoint = [\"http://e\"]"
                .to_owned(),
            "etcd_endpoint",
        ),
    ] {
        let error = parse_error(&text);
        assert!(error.contains("unknown field"), "{error}");
        assert!(error.contains(key), "{error}");
    }
}

#[test]
fn malformed_values_are_parse_errors() {
    let error = parse_error("[control_store]\netcd_endpoints = []");
    assert!(error.contains("cluster"), "{error}");
    let error = parse_error(&format!("{BASE}\n[buckets.defaults]\nmode = \"cached\""));
    assert!(error.contains("write_back"), "{error}");
    let error = parse_error(&format!("{BASE}\n[buckets.defaults]\nreplicas = 300"));
    assert!(error.contains("replicas"), "{error}");
    let error = parse_error(&format!("{BASE}\n[admin]\nlisten = \"localhost\""));
    assert!(error.contains("listen"), "{error}");
    parse_error("not toml");
}

#[test]
fn load_reads_a_file() {
    let dir = std::env::temp_dir().join(format!("skys3-config-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("skys3.toml");
    std::fs::write(&path, BASE).unwrap();
    let config = Config::load(&path).unwrap();
    assert_eq!(config.cluster().cluster_id.as_str(), "skys3-prod-a");

    let missing = dir.join("missing.toml");
    let error = Config::load(&missing).unwrap_err();
    assert!(matches!(error, ConfigError::Read { .. }));
    assert!(error.to_string().contains("missing.toml"), "{error}");
    std::fs::remove_dir_all(&dir).unwrap();
}

// The other rules of each section.

#[test]
fn durations_and_sizes_must_be_positive() {
    for (section, key) in [
        ("replication", "replica_ack_timeout_ms"),
        ("replication", "node_forget_after_hours"),
        ("storage", "extent_bytes"),
        ("storage", "group_commit_max_bytes"),
        ("storage", "index_checkpoint_interval_seconds"),
        ("flush", "flush_max_inflight_bytes_per_target"),
        ("flush", "streaming_flush_min_bytes"),
        ("flush", "max_dirty_bytes"),
        ("ec", "max_data_fragments"),
        ("ec", "fragment_orphan_after_seconds"),
        ("ec", "repair_bytes_per_second_per_node"),
        ("peering", "peer_connect_timeout_ms"),
        ("peering", "peer_connections_per_shard"),
        ("peering", "peer_staging_ttl_seconds"),
        ("identity", "identity_max_staleness_hours"),
        ("buckets.defaults", "import_parallel_streams"),
        ("buckets.defaults", "ec_min_object_bytes"),
        ("buckets.defaults", "ec_stripe_data_bytes"),
        ("buckets.defaults", "index_snapshot_interval_seconds"),
    ] {
        let violations = violations_of(&format!("{BASE}\n[{section}]\n{key} = 0"));
        assert!(
            violations.contains_key(&format!("{section}.{key}")),
            "{violations}"
        );
    }
    for section in ["control_store"] {
        for key in ["coordinator_lease_seconds", "config_poll_interval_seconds"] {
            let text = format!(
                "[cluster]\ncluster_id = \"a\"\n[{section}]\netcd_endpoints = [\"http://e\"]\n{key} = 0"
            );
            assert!(violations_of(&text).contains_key(&format!("{section}.{key}")));
        }
    }
}

#[test]
fn replication_timing_rules() {
    assert_violations(
        "[replication]\nmember_suspect_after_ms = 1000\nreplica_ack_timeout_mode = \"fail_fast\"",
        &["replication.member_suspect_after_ms"],
    );
    let violations = violations_of(&format!("{BASE}\n[replication]\nprimary_lease_ms = 0"));
    assert!(violations.contains_key("replication.primary_lease_ms"));
}

#[test]
fn storage_rules() {
    assert_violations(
        "[storage]\nsegment_bytes = 1048576",
        &["storage.segment_bytes"],
    );
    assert_violations(
        "[storage]\nsegment_bytes = 16777216\ninline_max_bytes = 16777216",
        &["storage.segment_bytes"],
    );
    // Record payloads are bounded by the log format.
    assert_violations(
        "[storage]\ninline_max_bytes = 16777217",
        &["storage.inline_max_bytes"],
    );
    for extent_bytes in ["65535", "16777217"] {
        assert_violations(
            &format!("[storage]\nextent_bytes = {extent_bytes}"),
            &["storage.extent_bytes"],
        );
    }
    let config = load("[storage]\nextent_bytes = 65536\ninline_max_bytes = 16777216").unwrap();
    assert_eq!(config.storage().extent_bytes, 65536);
    for threshold in ["0.0", "1.0"] {
        assert_violations(
            &format!("[storage]\ncompaction_live_threshold = {threshold}"),
            &["storage.compaction_live_threshold"],
        );
    }
    assert_violations(
        "[cache]\nreserve_fraction = 1.0",
        &["cache.reserve_fraction"],
    );
}

#[test]
fn flush_rules() {
    assert_violations(
        "[flush]\nflush_min_concurrency_per_shard = 0",
        &["flush.flush_min_concurrency_per_shard"],
    );
    assert_violations(
        "[flush]\nflush_min_concurrency_per_shard = 65",
        &["flush.flush_max_concurrency_per_shard"],
    );
    for bytes in ["5242879", "5368709121"] {
        assert_violations(
            &format!("[flush]\nflush_part_bytes = {bytes}"),
            &["flush.flush_part_bytes"],
        );
    }
    load("[flush]\nflush_part_bytes = 5242880").unwrap();
    assert_violations(
        "[flush]\nflush_conflict_policy = \"discard_local\"",
        &["flush.flush_conflict_policy"],
    );
}

#[test]
fn ec_rules() {
    assert_violations("[ec]\nmin_eligible_nodes = 2", &["ec.min_eligible_nodes"]);
    assert_violations("[ec]\nparity_fragments = 0", &["ec.parity_fragments"]);
}

#[test]
fn peering_rules() {
    assert_violations(
        "[peering]\npeer_max_inflight_bytes = 1000",
        &["peering.peer_max_inflight_bytes"],
    );
    assert_violations(
        "[peering]\npeer_staging_quota_bytes = 1000",
        &["peering.peer_staging_quota_bytes"],
    );
    assert_violations(
        "[peering]\npeer_frame_bytes = 0",
        &["peering.peer_frame_bytes"],
    );
}

#[test]
fn identity_rules() {
    assert_violations(
        "[identity]\nsession_default_seconds = 7200",
        &["identity.session_default_seconds"],
    );
    assert_violations(
        "[identity]\nsession_default_seconds = 600\nsession_maximum_seconds = 86400",
        &[
            "identity.session_default_seconds",
            "identity.session_maximum_seconds",
        ],
    );
    assert_violations(
        "[identity]\noidc_clock_skew_seconds = 301",
        &["identity.oidc_clock_skew_seconds"],
    );
}

const READ_ONLY: &str =
    r#"{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:Get*","Resource":"*"}}"#;

#[test]
fn anonymous_access_needs_a_policy() {
    assert!(load("").unwrap().identity().anonymous_policy.is_none());
    assert_violations(
        "[identity]\nanonymous_access = true",
        &["identity.anonymous_policy"],
    );
    assert_violations(
        &format!("[identity]\nanonymous_policy = '{READ_ONLY}'"),
        &["identity.anonymous_policy"],
    );
    let config = load(&format!(
        "[identity]\nanonymous_access = true\nanonymous_policy = '{READ_ONLY}'"
    ))
    .unwrap();
    assert_eq!(
        config.identity().anonymous_policy,
        Some(READ_ONLY.parse().unwrap())
    );
    let error = parse_error(&format!(
        "{BASE}\n[identity]\nanonymous_access = true\nanonymous_policy = '{{\"Version\":\"2012-10-17\"}}'"
    ));
    assert!(error.contains("Statement is required"), "{error}");
}

fn static_credential(name: &str, key_id: &str, file: &str) -> String {
    format!(
        "[identity.static_credentials.{name}]\naccess_key_id = \"{key_id}\"\n\
         secret_access_key_file = \"{file}\"\npolicy = '{READ_ONLY}'\n"
    )
}

#[test]
fn static_credentials_are_checked() {
    let both = format!(
        "{}{}",
        static_credential(
            "bootstrap",
            "AKIABOOTSTRAP0001",
            "/etc/skys3/bootstrap.secret"
        ),
        static_credential(
            "backup-svc",
            "AKIABACKUPSERVICE",
            "/etc/skys3/backup.secret"
        ),
    );
    let config = load(&both).unwrap();
    let credentials = &config.identity().static_credentials;
    assert_eq!(credentials.len(), 2);
    assert_eq!(credentials["bootstrap"].access_key_id, "AKIABOOTSTRAP0001");
    assert_eq!(
        credentials["backup-svc"].secret_access_key_file,
        std::path::Path::new("/etc/skys3/backup.secret")
    );

    assert_violations(
        &static_credential("\"bad/name\"", "AKIABOOTSTRAP0001", "/s"),
        &["identity.static_credentials.\"bad/name\""],
    );
    assert_violations(
        &static_credential(&"n".repeat(65), "AKIABOOTSTRAP0001", "/s"),
        &[&format!("identity.static_credentials.{}", "n".repeat(65))],
    );
    for key_id in ["AKIASHORT", "AKIA-BOOTSTRAP-0001", &"A".repeat(129)] {
        assert_violations(
            &static_credential("a", key_id, "/s"),
            &["identity.static_credentials.a.access_key_id"],
        );
    }
    assert_violations(
        &static_credential("a", "AKIABOOTSTRAP0001", ""),
        &["identity.static_credentials.a.secret_access_key_file"],
    );
    assert_violations(
        &format!(
            "{}{}",
            static_credential("a", "AKIABOOTSTRAP0001", "/a"),
            static_credential("b", "AKIABOOTSTRAP0001", "/b"),
        ),
        &["identity.static_credentials.b.access_key_id"],
    );
    let error = parse_error(&format!(
        "{BASE}\n[identity.static_credentials.a]\naccess_key_id = \"AKIABOOTSTRAP0001\"\n\
         secret_access_key_file = \"/s\"\npolicy = '{{\"Version\":\"2012-10-17\",\"Statement\":{{\"Effect\":\"Allow\",\"Action\":\"*\",\"Resource\":\"*\",\"Condition\":{{}}}}}}'"
    ));
    assert!(error.contains("Condition is not supported"), "{error}");
    let error = parse_error(&format!(
        "{BASE}\n[identity.static_credentials.a]\naccess_key_id = \"AKIABOOTSTRAP0001\"\n\
         secret_access_key = \"inline\"\nsecret_access_key_file = \"/s\"\npolicy = '{READ_ONLY}'"
    ));
    assert!(error.contains("unknown field"), "{error}");
}

#[test]
fn admin_listener_off_loopback_needs_a_token() {
    assert_violations("[admin]\nlisten = \"0.0.0.0:7490\"", &["admin.token_file"]);
    assert_violations("[admin]\nlisten = \"10.0.0.5:7490\"", &["admin.token_file"]);
    assert_violations("[admin]\ntoken_file = \"\"", &["admin.token_file"]);
    load("[admin]\nlisten = \"[::1]:7490\"").unwrap();
    let config = load("[admin]\nlisten = \"0.0.0.0:7490\"\ntoken_file = \"/run/token\"").unwrap();
    assert_eq!(
        config.admin().token_file.as_deref(),
        Some("/run/token".as_ref())
    );
}

#[test]
fn transport_certificate_files_are_set_together() {
    let config = load("").unwrap();
    assert_eq!(config.transport().listen.to_string(), "0.0.0.0:7400");
    assert_eq!(config.transport().tls_files(), None);

    let all = "tls_cert_file = \"/etc/skys3/node.crt\"\n\
               tls_key_file = \"/etc/skys3/node.key\"\n\
               tls_ca_file = \"/etc/skys3/ca.crt\"";
    let config = load(&format!("[transport]\nlisten = \"10.0.0.5:7400\"\n{all}")).unwrap();
    let files = config.transport().tls_files().unwrap();
    assert_eq!(files.cert, std::path::Path::new("/etc/skys3/node.crt"));
    assert_eq!(files.key, std::path::Path::new("/etc/skys3/node.key"));
    assert_eq!(files.ca, std::path::Path::new("/etc/skys3/ca.crt"));

    assert_violations(
        "[transport]\ntls_cert_file = \"/etc/skys3/node.crt\"",
        &["transport.tls_key_file", "transport.tls_ca_file"],
    );
    assert_violations(
        &format!("[transport]\n{}", all.replace("/etc/skys3/node.key", "")),
        &["transport.tls_key_file"],
    );
}

// [control_store]

fn control_store(body: &str) -> String {
    format!("[cluster]\ncluster_id = \"c1\"\n[control_store]\n{body}")
}

#[track_caller]
fn assert_control_store_violations(body: &str, keys: &[&str]) {
    let violations = violations_of(&control_store(body));
    let reported: Vec<_> = violations
        .as_slice()
        .iter()
        .map(|v| v.key.as_str())
        .collect();
    assert_eq!(reported, keys, "{violations}");
}

#[test]
fn etcd_backend_rules() {
    assert_control_store_violations("", &["control_store.etcd_endpoints"]);
    assert_control_store_violations("etcd_endpoints = []", &["control_store.etcd_endpoints"]);
    assert_control_store_violations(
        "etcd_endpoints = [\"etcd-1:2379\", \"https://etcd-2:2379/v3\"]",
        &[
            "control_store.etcd_endpoints",
            "control_store.etcd_endpoints",
        ],
    );
    assert_control_store_violations(
        "etcd_endpoints = [\"http://e\"]\nendpoint = \"https://s3\"\nbucket = \"b\"",
        &["control_store.endpoint", "control_store.bucket"],
    );
}

#[test]
fn s3_backend_rules() {
    let config: Config =
        control_store("backend = \"s3\"\nendpoint = \"https://r2.example/\"\nbucket = \"ctl\"")
            .parse()
            .unwrap();
    assert_eq!(
        config.control_store().backend,
        ControlStoreBackend::S3 {
            endpoint: "https://r2.example".to_owned(),
            bucket: "ctl".to_owned(),
        }
    );
    assert_control_store_violations(
        "backend = \"s3\"",
        &["control_store.endpoint", "control_store.bucket"],
    );
    assert_control_store_violations(
        "backend = \"s3\"\nendpoint = \"ftp://x\"\nbucket = \"a/b\"\netcd_endpoints = []",
        &[
            "control_store.etcd_endpoints",
            "control_store.endpoint",
            "control_store.bucket",
        ],
    );
}

#[test]
fn file_backend_rules() {
    let config: Config = control_store("backend = \"file\"").parse().unwrap();
    assert_eq!(
        config.control_store().backend,
        ControlStoreBackend::File {
            directory: "/var/lib/skys3/control".into(),
        }
    );
    let config: Config = format!(
        "{}\n[node]\ndata_dir = \"/data\"",
        control_store("backend = \"file\"")
    )
    .parse()
    .unwrap();
    assert_eq!(
        config.control_store().backend,
        ControlStoreBackend::File {
            directory: "/data/control".into(),
        },
        "the default follows data_dir"
    );
    let config: Config = control_store("backend = \"file\"\ndirectory = \"/ctl\"")
        .parse()
        .unwrap();
    assert_eq!(
        config.control_store().backend,
        ControlStoreBackend::File {
            directory: "/ctl".into(),
        }
    );
    assert_control_store_violations(
        "backend = \"file\"\ndirectory = \"\"\netcd_endpoints = [\"http://e\"]\nbucket = \"b\"",
        &[
            "control_store.etcd_endpoints",
            "control_store.bucket",
            "control_store.directory",
        ],
    );
    assert_control_store_violations(
        "etcd_endpoints = [\"http://e\"]\ndirectory = \"/ctl\"",
        &["control_store.directory"],
    );
}

// [node] and [gateway]

#[test]
fn node_rules() {
    let config = load("[node]\nnode_id = \"node-2\"\ndisks = [\"/a\", \"/b\"]").unwrap();
    assert_eq!(config.node().node_id.as_ref().unwrap().as_str(), "node-2");
    assert_eq!(config.node().disks.len(), 2);
    assert_violations(
        "[node]\nnode_id = \"Node_2\"\ndata_dir = \"\"\ndisks = [\"/a\", \"\", \"/a\"]",
        &["node.node_id", "node.data_dir", "node.disks", "node.disks"],
    );
    assert_violations("[node]\ndisks = []", &["node.disks"]);
    let many: Vec<_> = (0..65).map(|i| format!("\"/d{i}\"")).collect();
    assert_violations(
        &format!("[node]\ndisks = [{}]", many.join(", ")),
        &["node.disks"],
    );
}

#[test]
fn gateway_rules() {
    let config = load("[gateway]\nlisten = \"0.0.0.0:443\"").unwrap();
    assert_eq!(config.gateway().listen.port(), 443);
    assert_violations(
        "[gateway]\ntls_key_file = \"/k\"",
        &["gateway.tls_cert_file"],
    );
    assert_violations(
        "[gateway]\ntls_cert_file = \"\"\ntls_key_file = \"/k\"",
        &["gateway.tls_cert_file"],
    );
}

#[test]
fn prefix_rules() {
    for prefix in ["", "/", "no-slash", "/abs/", "has space/"] {
        assert_control_store_violations(
            &format!("etcd_endpoints = [\"http://e\"]\nprefix = \"{prefix}\""),
            &["control_store.prefix"],
        );
    }
    let config: Config = control_store("etcd_endpoints = [\"http://e\"]\nprefix = \"a/b/\"")
        .parse()
        .unwrap();
    assert_eq!(config.control_store().prefix, "a/b/");
}

#[test]
fn an_s3_control_store_must_not_share_a_backup_targets_scope() {
    let with = |endpoint: &str, allow: bool| {
        control_store(&format!(
            "backend = \"s3\"\nendpoint = \"{endpoint}\"\nbucket = \"ctl\"\n\
             allow_correlated_control_store = {allow}\n\
             [buckets.archive]\nmode = \"local\"\n\
             backup_target = \"https://s3.us-west-2.amazonaws.com/backup\""
        ))
    };
    let violations = violations_of(&with("https://bucket.s3.us-west-2.amazonaws.com", false));
    assert_eq!(violations.as_slice()[0].key, "control_store.endpoint");
    assert!(violations.as_slice()[0].message.contains("archive"));
    with("https://s3.us-west-2.amazonaws.com", true)
        .parse::<Config>()
        .unwrap();
    with("https://s3.eu-central-1.amazonaws.com", false)
        .parse::<Config>()
        .unwrap();
}

// [buckets]

#[test]
fn bucket_names_must_be_valid() {
    assert_violations("[buckets.Archive]\nmode = \"local\"", &["buckets.Archive"]);
    assert_violations(
        "[buckets.\"a..b\"]\nmode = \"local\"",
        &["buckets.\"a..b\""],
    );
}

#[test]
fn per_bucket_keys_are_not_defaults() {
    assert_violations(
        "[buckets.defaults]\nbackup_target = \"https://h/b\"\nsnapshot_target = \"https://h/s\"\n\
         peer_source = \"c2\"\nack_policy = \"local\"\nflush_conflict_policy = \"hold\"",
        &[
            "buckets.defaults.backup_target",
            "buckets.defaults.snapshot_target",
            "buckets.defaults.peer_source",
            "buckets.defaults.ack_policy",
            "buckets.defaults.flush_conflict_policy",
        ],
    );
}

#[test]
fn bucket_tables_override_the_defaults() {
    let config = load(
        "[buckets.defaults]\nreplicas = 5\nmin_write_replicas = 3\n\
         [buckets.hot]\nclean_copies = 5\nshards_per_bucket = 32\n\
         flush_conflict_policy = \"discard_local\"",
    )
    .unwrap();
    let hot = config.buckets().get(&BucketName::new("hot").unwrap());
    assert_eq!(hot.replication.replicas, 5);
    assert_eq!(hot.replication.min_write_replicas, 3);
    assert_eq!(hot.replication.clean_copies, 5);
    assert_eq!(hot.shards_per_bucket.get(), 32);
    assert_eq!(
        hot.flush_conflict_policy,
        skys3_config::ConflictPolicy::DiscardLocal
    );
    assert_eq!(
        config.buckets().defaults.flush_conflict_policy,
        skys3_config::ConflictPolicy::Hold
    );
}

#[test]
fn bucket_overrides_are_checked_against_inherited_values() {
    assert_violations(
        "[buckets.small]\nreplicas = 1\nclean_copies = 2",
        &[
            "buckets.small.min_write_replicas",
            "buckets.small.clean_copies",
        ],
    );
    assert_violations(
        "[buckets.small]\nshards_per_bucket = 1000\nimport_parallel_streams = 0",
        &[
            "buckets.small.shards_per_bucket",
            "buckets.small.import_parallel_streams",
        ],
    );
}

#[test]
fn a_bad_default_is_reported_once() {
    assert_violations(
        "[buckets.defaults]\nshards_per_bucket = 1000\nclean_copies = 9\nec_stripe_data_bytes = 0\n\
         [buckets.alpha]\nmode = \"local\"\n[buckets.beta]\nmode = \"local\"",
        &[
            "buckets.defaults.shards_per_bucket",
            "buckets.defaults.clean_copies",
            "buckets.defaults.ec_stripe_data_bytes",
        ],
    );
}

#[test]
fn backup_targets_belong_to_local_buckets() {
    assert_violations(
        "[buckets.cache]\nmode = \"write_back\"\nbackup_target = \"https://h/b\"",
        &["buckets.cache.backup_target"],
    );
    assert_violations(
        "[buckets.defaults]\nbackup_ack = \"write_through\"\n[buckets.vault]\nmode = \"local\"",
        &["buckets.vault.backup_ack"],
    );
    load(
        "[buckets.defaults]\nbackup_ack = \"write_through\"\n\
         [buckets.vault]\nmode = \"local\"\nbackup_target = \"https://h/b\"",
    )
    .unwrap();
}

#[test]
fn target_urls_are_checked() {
    assert_violations(
        "[buckets.alpha]\nmode = \"local\"\nbackup_target = \"s3://bucket\"\n\
         snapshot_target = \"https://h\"",
        &[
            "buckets.alpha.backup_target",
            "buckets.alpha.snapshot_target",
        ],
    );
}

#[test]
fn peer_source_must_be_another_cluster() {
    assert_violations(
        "[buckets.alpha]\nmode = \"local\"\npeer_source = \"skys3-prod-a\"",
        &["buckets.alpha.peer_source"],
    );
    assert_violations(
        "[buckets.alpha]\nmode = \"local\"\npeer_source = \"Other_Cluster\"",
        &["buckets.alpha.peer_source"],
    );
}

// Rules keep running after an early failure.

#[test]
fn an_invalid_cluster_id_does_not_hide_other_violations() {
    let violations = violations_of(
        r#"
        [cluster]
        cluster_id = "Not_A_Valid_Cluster"
        [control_store]
        backend = "s3"
        endpoint = "https://[fd00::1]:9000"
        bucket = "ctl"
        coordinator_lease_seconds = 0
        config_poll_interval_seconds = 0
        [replication]
        lease_renew_interval_ms = 4000
        [buckets.archive]
        mode = "local"
        backup_target = "http://[fd00:0:0:0:0:0:0:1]/backup"
        [buckets.broken]
        clean_copies = 9
        "#,
    );
    let keys: Vec<_> = violations
        .as_slice()
        .iter()
        .map(|v| v.key.as_str())
        .collect();
    assert_eq!(
        keys,
        [
            "cluster.cluster_id",
            "control_store.coordinator_lease_seconds",
            "control_store.config_poll_interval_seconds",
            "replication.lease_renew_interval_ms",
            "replication.member_suspect_after_ms",
            "buckets.broken.clean_copies",
            "control_store.endpoint",
        ]
    );
}

// Endpoint and target authorities.

#[test]
fn endpoint_authorities_are_validated() {
    for (endpoints, reason) in [
        (r#"["https://:2379"]"#, "invalid host"),
        (r#"["https://etcd-1:bad"]"#, "invalid port"),
        (r#"["https://[fd00::1:2379"]"#, "invalid host"),
        (r#"["https://etcd-1:70000"]"#, "invalid port"),
    ] {
        let violations = violations_of(&control_store(&format!("etcd_endpoints = {endpoints}")));
        assert_eq!(violations.len(), 1, "{violations}");
        let violation = &violations.as_slice()[0];
        assert_eq!(violation.key, "control_store.etcd_endpoints");
        assert!(violation.message.contains(reason), "{violation}");
    }
    assert_violations(
        "[buckets.archive]\nmode = \"local\"\nbackup_target = \"https://peer:bad/archive\"",
        &["buckets.archive.backup_target"],
    );
}

#[test]
fn endpoint_hosts_are_normalized() {
    let config = load(
        "[buckets.archive]\nmode = \"local\"\n\
         backup_target = \"https://Peer.Example:8443/archive\"\n\
         snapshot_target = \"http://[FD00:0::1]/snaps/\"",
    )
    .unwrap();
    let archive = config.buckets().get(&BucketName::new("archive").unwrap());
    let endpoint =
        |target: &Option<skys3_types::RemoteTarget>| target.as_ref().unwrap().endpoint.clone();
    assert_eq!(
        endpoint(&archive.backup_target),
        "https://peer.example:8443"
    );
    assert_eq!(endpoint(&archive.snapshot_target), "http://[fd00::1]");
}

#[test]
fn an_s3_control_store_must_not_share_a_snapshot_targets_scope() {
    let with = |snapshot: &str| {
        control_store(&format!(
            "backend = \"s3\"\nendpoint = \"https://r2.example\"\nbucket = \"ctl\"\n\
             [buckets.archive]\nmode = \"local\"\n\
             backup_target = \"https://s3.us-west-2.amazonaws.com/backup\"\n\
             snapshot_target = \"{snapshot}\""
        ))
    };
    let violations = violations_of(&with("https://R2.example/snaps"));
    let [violation] = violations.as_slice() else {
        panic!("{violations}");
    };
    assert_eq!(violation.key, "control_store.endpoint");
    assert!(
        violation
            .message
            .contains("snapshot target of bucket archive"),
        "{violation:?}"
    );
    with("https://s3.us-east-2.amazonaws.com/snaps")
        .parse::<Config>()
        .unwrap();
}

#[test]
fn a_target_must_not_overlap_the_control_prefix_even_when_correlation_is_allowed() {
    let with = |backup: &str| {
        control_store(&format!(
            "backend = \"s3\"\nendpoint = \"https://minio.example\"\nbucket = \"shared\"\n\
             prefix = \"control/\"\nallow_correlated_control_store = true\n\
             [buckets.archive]\nmode = \"local\"\nbackup_target = \"{backup}\""
        ))
    };
    for backup in [
        "https://minio.example/shared",
        "https://minio.example/shared/control/",
        "https://minio.example/shared/control/archive/",
        "https://minio.example/shared/con",
    ] {
        let violations = violations_of(&with(backup));
        assert!(
            violations.as_slice()[0].message.contains("overlap"),
            "{backup}: {violations}"
        );
    }
    for backup in [
        "https://minio.example/shared/data/",
        "https://minio.example/other/control/",
    ] {
        with(backup).parse::<Config>().unwrap();
    }
}

#[test]
fn correlated_ip_endpoints_are_compared_as_addresses() {
    let with = |control: &str, backup: &str| {
        control_store(&format!(
            "backend = \"s3\"\nendpoint = \"{control}\"\nbucket = \"ctl\"\n\
             [buckets.archive]\nmode = \"local\"\nbackup_target = \"{backup}/archive\""
        ))
    };
    for (control, backup) in [
        ("http://[fd00::1]:9000", "http://[fd00:0:0:0:0:0:0:1]:9001"),
        ("http://10.0.0.5:9000", "http://[::ffff:10.0.0.5]"),
        ("https://minio.example", "https://MINIO.example:9443"),
    ] {
        let violations = violations_of(&with(control, backup));
        assert_eq!(
            violations.as_slice()[0].key,
            "control_store.endpoint",
            "{violations}"
        );
    }
    with("http://[fd00::1]", "http://[fd00::2]")
        .parse::<Config>()
        .unwrap();
}

#[test]
fn attached_targets_must_not_share_the_control_stores_scope() {
    let config = |body: &str| {
        control_store(body)
            .parse::<Config>()
            .unwrap()
            .control_store()
            .clone()
    };
    let s3 = |allow: bool| {
        config(&format!(
            "backend = \"s3\"\nendpoint = \"https://bucket.s3.us-west-2.amazonaws.com\"\n\
             bucket = \"ctl\"\nallow_correlated_control_store = {allow}"
        ))
    };
    let target = skys3_config::parse_target("https://s3.us-west-2.amazonaws.com/data").unwrap();
    let error = s3(false).check_target_independence(&target).unwrap_err();
    assert!(error.contains("aws:us-west-2"), "{error}");
    s3(true).check_target_independence(&target).unwrap();
    let elsewhere = skys3_config::parse_target("https://s3.eu-west-1.amazonaws.com/d").unwrap();
    s3(false).check_target_independence(&elsewhere).unwrap();
    let etcd = config("etcd_endpoints = [\"https://s3.us-west-2.amazonaws.com\"]");
    etcd.check_target_independence(&target).unwrap();
    // The control bucket itself, under the control prefix, is never a
    // target.
    let control = skys3_config::parse_target("https://s3.us-west-2.amazonaws.com/ctl").unwrap();
    let error = s3(true).check_target_independence(&control).unwrap_err();
    assert!(error.contains("overlap"), "{error}");
}

#[test]
fn target_regions_are_names() {
    for bad in ["", "us east", "us-east-1/x"] {
        let violations = violations_of(&format!("{BASE}\n[flush]\ntarget_region = {bad:?}"));
        assert!(
            violations.contains_key("flush.target_region"),
            "{violations}"
        );
    }
    let config: skys3_config::Config = format!("{BASE}\n[flush]\ntarget_region = \"auto\"")
        .parse()
        .unwrap();
    assert_eq!(config.flush().target_region, "auto");
}
