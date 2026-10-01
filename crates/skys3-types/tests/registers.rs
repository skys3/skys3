//! Register documents against the design's example and their invariants.

use skys3_types::{
    BucketDocument, BucketMode, ClusterDocument, CoordinatorLease, DiskInfo, InvalidRegister,
    NodeId, NodeRegistration, RegisterDocument, RegisterError, RemoteTarget, ReplicationError,
    ReplicationSettings, ShardConfig,
};

/// The shard register example of design section 6.1, verbatim.
const SHARD_EXAMPLE: &str = r#"{
  "bucket_id": "b-7f3a",
  "shard": 5,
  "epoch": 42,
  "primary": "node-3",
  "members": ["node-3", "node-7", "node-9"],
  "learners": [],
  "min_write_replicas": 2,
  "replicas": 3,
  "proposal_id": "01J8Z6K3V2Q4"
}"#;

fn compact(json: &str) -> String {
    json.split_whitespace().collect()
}

fn node(id: &str) -> NodeId {
    id.parse().unwrap()
}

fn shard_example() -> ShardConfig {
    ShardConfig::from_json(SHARD_EXAMPLE.as_bytes()).unwrap()
}

fn invalid<T: RegisterDocument>(document: &T) -> InvalidRegister {
    match document.to_json().unwrap_err() {
        RegisterError::Invalid { source, .. } => source,
        other => panic!("expected an invariant error, got {other}"),
    }
}

#[test]
fn shard_example_parses_with_every_field() {
    let config = shard_example();
    assert_eq!(config.bucket_id.as_str(), "b-7f3a");
    assert_eq!(config.shard.get(), 5);
    assert_eq!(config.epoch.get(), 42);
    assert_eq!(config.primary, node("node-3"));
    assert_eq!(
        config.members,
        [node("node-3"), node("node-7"), node("node-9")]
    );
    assert!(config.learners.is_empty());
    assert_eq!(config.min_write_replicas, 2);
    assert_eq!(config.replicas, 3);
    assert_eq!(config.proposal_id().as_str(), "01J8Z6K3V2Q4");
    assert!(config.is_member(&node("node-9")));
    assert!(!config.is_learner(&node("node-9")));
}

#[test]
fn shard_example_serializes_back_to_the_same_bytes() {
    let json = shard_example().to_json().unwrap();
    assert_eq!(String::from_utf8(json).unwrap(), compact(SHARD_EXAMPLE));
}

#[test]
fn shard_config_rejects_malformed_values() {
    let malformed = [
        SHARD_EXAMPLE.replace("\"b-7f3a\"", "\"B-7F3A\""),
        SHARD_EXAMPLE.replace("\"shard\": 5", "\"shard\": 256"),
        SHARD_EXAMPLE.replace("\"epoch\": 42", "\"epoch\": -1"),
        SHARD_EXAMPLE.replace("\"learners\": [],", ""),
        SHARD_EXAMPLE.replace("\"learners\": []", "\"learners\": [], \"extra\": 1"),
        SHARD_EXAMPLE.replace("01J8Z6K3V2Q4", "01J8 Z6K3"),
        "[]".to_owned(),
        "{".to_owned(),
    ];
    for json in malformed {
        let err = ShardConfig::from_json(json.as_bytes()).unwrap_err();
        assert!(matches!(err, RegisterError::Json { .. }), "{json}: {err}");
        assert!(
            err.to_string()
                .starts_with("malformed shard configuration document:")
        );
        assert!(std::error::Error::source(&err).is_some());
    }
}

#[test]
fn shard_config_checks_its_invariants() {
    let mut config = shard_example();
    config.primary = node("node-4");
    assert_eq!(
        invalid(&config),
        InvalidRegister::PrimaryNotMember(node("node-4"))
    );

    let mut config = shard_example();
    config.members.clear();
    assert_eq!(invalid(&config), InvalidRegister::NoMembers);

    let mut config = shard_example();
    config.members.push(node("node-7"));
    assert_eq!(
        invalid(&config),
        InvalidRegister::DuplicateNode(node("node-7"))
    );

    let mut config = shard_example();
    config.learners = vec![node("node-1"), node("node-1")];
    assert_eq!(
        invalid(&config),
        InvalidRegister::DuplicateNode(node("node-1"))
    );

    let mut config = shard_example();
    config.learners = vec![node("node-9")];
    assert_eq!(
        invalid(&config),
        InvalidRegister::LearnerIsMember(node("node-9"))
    );

    let mut config = shard_example();
    config.min_write_replicas = 4;
    assert_eq!(
        invalid(&config),
        InvalidRegister::Replication(ReplicationError::MinWriteReplicas {
            min_write_replicas: 4,
            replicas: 3
        })
    );

    let mut config = shard_example();
    config.replicas = 0;
    assert_eq!(
        invalid(&config),
        InvalidRegister::Replication(ReplicationError::NoReplicas)
    );
}

#[test]
fn shard_config_allows_extra_members_during_a_rebalance() {
    let mut config = shard_example();
    config.members.push(node("node-11"));
    config.learners.push(node("node-12"));
    assert!(config.validate().is_ok());
    let json = config.to_json().unwrap();
    assert_eq!(ShardConfig::from_json(&json).unwrap(), config);
}

#[test]
fn invalid_documents_are_refused_when_read() {
    let json = SHARD_EXAMPLE.replace("\"primary\": \"node-3\"", "\"primary\": \"node-4\"");
    let err = ShardConfig::from_json(json.as_bytes()).unwrap_err();
    assert_eq!(
        err.to_string(),
        "invalid shard configuration document: primary node-4 is not a member"
    );
}

#[test]
fn replication_settings_follow_the_design_bounds() {
    let settings = |replicas, min_write_replicas, clean_copies| ReplicationSettings {
        replicas,
        min_write_replicas,
        clean_copies,
    };
    assert_eq!(settings(3, 2, 1).validate(), Ok(()));
    assert_eq!(settings(3, 3, 3).validate(), Ok(()));
    assert_eq!(settings(1, 1, 0).validate(), Ok(()));
    assert_eq!(
        settings(0, 0, 0).validate(),
        Err(ReplicationError::NoReplicas)
    );
    assert_eq!(
        settings(3, 0, 1).validate(),
        Err(ReplicationError::MinWriteReplicas {
            min_write_replicas: 0,
            replicas: 3
        })
    );
    let err = settings(3, 2, 4).validate().unwrap_err();
    assert_eq!(
        err,
        ReplicationError::CleanCopies {
            clean_copies: 4,
            replicas: 3
        }
    );
    assert_eq!(
        err.to_string(),
        "clean_copies is 4; it must be from 0 to replicas (3)"
    );
}

const CLUSTER_EXAMPLE: &str = r#"{
  "cluster_id": "skys3-prod-a",
  "format_version": 1,
  "generation": 17,
  "proposal_id": "01J8Z6K3V2Q5"
}"#;

#[test]
fn cluster_document_round_trips_and_checks_its_version() {
    let document = ClusterDocument::from_json(CLUSTER_EXAMPLE.as_bytes()).unwrap();
    assert_eq!(document.cluster_id.as_str(), "skys3-prod-a");
    assert_eq!(document.generation.get(), 17);
    assert_eq!(document.proposal_id().as_str(), "01J8Z6K3V2Q5");
    let json = document.to_json().unwrap();
    assert_eq!(String::from_utf8(json).unwrap(), compact(CLUSTER_EXAMPLE));

    let mut newer = document;
    newer.format_version = 2;
    assert_eq!(
        invalid(&newer),
        InvalidRegister::UnsupportedFormatVersion {
            found: 2,
            supported: ClusterDocument::FORMAT_VERSION
        }
    );
}

#[test]
fn coordinator_lease_round_trips() {
    let json = r#"{"holder":"node-7","proposal_id":"01J8Z6K3V2Q6"}"#;
    let lease = CoordinatorLease::from_json(json.as_bytes()).unwrap();
    assert_eq!(lease.holder, node("node-7"));
    assert_eq!(lease.proposal_id().as_str(), "01J8Z6K3V2Q6");
    assert_eq!(lease.to_json().unwrap(), json.as_bytes());
}

const NODE_EXAMPLE: &str = r#"{
  "node_id": "node-3",
  "address": "10.0.3.17:7400",
  "zone": "us-east-1a",
  "rack": "r12",
  "disks": [
    {"disk_id": "nvme0", "capacity_bytes": 3840755982336},
    {"disk_id": "nvme1", "capacity_bytes": 3840755982336}
  ],
  "proposal_id": "01J8Z6K3V2Q7"
}"#;

#[test]
fn node_registration_round_trips() {
    let registration = NodeRegistration::from_json(NODE_EXAMPLE.as_bytes()).unwrap();
    assert_eq!(registration.node_id, node("node-3"));
    assert_eq!(registration.disks.len(), 2);
    assert_eq!(registration.proposal_id().as_str(), "01J8Z6K3V2Q7");
    let json = registration.to_json().unwrap();
    assert_eq!(String::from_utf8(json).unwrap(), compact(NODE_EXAMPLE));

    // Zone and rack are optional and omitted when absent.
    let mut bare = registration;
    bare.zone = None;
    bare.rack = None;
    let json = String::from_utf8(bare.to_json().unwrap()).unwrap();
    assert!(!json.contains("zone") && !json.contains("rack"), "{json}");
    assert_eq!(NodeRegistration::from_json(json.as_bytes()).unwrap(), bare);
}

#[test]
fn node_registration_checks_address_and_disks() {
    for address in [
        "",
        "not-an-address",
        "a b:1",
        "é:1",
        "10.0.3.17:0",
        "::1:7400",
    ] {
        let json = NODE_EXAMPLE.replace("10.0.3.17:7400", address);
        let err = NodeRegistration::from_json(json.as_bytes()).unwrap_err();
        assert!(
            matches!(err, RegisterError::Json { .. }),
            "{address:?}: {err}"
        );
        assert!(err.to_string().contains("node address"), "{err}");
    }
    let json = NODE_EXAMPLE.replace("10.0.3.17:7400", "[fd00::17]:7400");
    let registration = NodeRegistration::from_json(json.as_bytes()).unwrap();
    assert_eq!(registration.address.to_string(), "[fd00::17]:7400");

    let mut bad = registration;
    bad.disks.push(DiskInfo {
        disk_id: "nvme0".parse().unwrap(),
        capacity_bytes: 1,
    });
    assert_eq!(
        invalid(&bad),
        InvalidRegister::DuplicateDisk("nvme0".parse().unwrap())
    );
    assert_eq!(
        bad.validate().unwrap_err().to_string(),
        "disk nvme0 is listed twice"
    );
}

const BUCKET_EXAMPLE: &str = r#"{
  "bucket_id": "b-7f3a",
  "name": "photos",
  "mode": "write_back",
  "shards": 8,
  "replicas": 3,
  "min_write_replicas": 2,
  "clean_copies": 1,
  "target": {
    "endpoint": "https://s3.us-east-1.amazonaws.com",
    "bucket": "example-photos",
    "prefix": "skys3/"
  },
  "created_unix_ms": 1790812800000,
  "proposal_id": "01J8Z6K3V2Q8"
}"#;

fn bucket_example() -> BucketDocument {
    BucketDocument::from_json(BUCKET_EXAMPLE.as_bytes()).unwrap()
}

#[test]
fn bucket_document_round_trips() {
    let bucket = bucket_example();
    assert_eq!(bucket.name.as_str(), "photos");
    assert_eq!(bucket.mode, BucketMode::WriteBack);
    assert_eq!(bucket.shards.get(), 8);
    assert_eq!(bucket.created_unix_ms, 1_790_812_800_000);
    assert_eq!(
        bucket.replication(),
        ReplicationSettings {
            replicas: 3,
            min_write_replicas: 2,
            clean_copies: 1
        }
    );
    assert_eq!(bucket.proposal_id().as_str(), "01J8Z6K3V2Q8");
    let json = bucket.to_json().unwrap();
    assert_eq!(String::from_utf8(json).unwrap(), compact(BUCKET_EXAMPLE));

    let mut local = bucket;
    local.mode = BucketMode::Local;
    local.target = None;
    let json = local.to_json().unwrap();
    assert_eq!(BucketDocument::from_json(&json).unwrap(), local);
}

#[test]
fn bucket_document_checks_its_target_binding() {
    let mut bucket = bucket_example();
    bucket.target = None;
    assert_eq!(
        invalid(&bucket),
        InvalidRegister::TargetRequired(BucketMode::WriteBack)
    );
    bucket.mode = BucketMode::ReadOnly;
    assert_eq!(
        invalid(&bucket),
        InvalidRegister::TargetRequired(BucketMode::ReadOnly)
    );

    let mut bucket = bucket_example();
    bucket.mode = BucketMode::Local;
    assert_eq!(
        invalid(&bucket),
        InvalidRegister::TargetNotAllowed(BucketMode::Local)
    );
    assert_eq!(
        bucket.validate().unwrap_err().to_string(),
        "a local bucket has no target"
    );

    let blank = |endpoint: &str, bucket: &str| RemoteTarget {
        endpoint: endpoint.to_owned(),
        bucket: bucket.to_owned(),
        prefix: None,
    };
    let mut bucket = bucket_example();
    bucket.target = Some(blank("", "b"));
    assert_eq!(invalid(&bucket), InvalidRegister::InvalidTarget("endpoint"));
    bucket.target = Some(blank("https://e", "a b"));
    assert_eq!(invalid(&bucket), InvalidRegister::InvalidTarget("bucket"));

    let mut bucket = bucket_example();
    bucket.clean_copies = 4;
    assert!(matches!(
        invalid(&bucket),
        InvalidRegister::Replication(ReplicationError::CleanCopies { .. })
    ));
}

#[test]
fn bucket_modes_use_their_configuration_spelling() {
    for (mode, text) in [
        (BucketMode::WriteBack, "write_back"),
        (BucketMode::Local, "local"),
        (BucketMode::ReadOnly, "read_only"),
    ] {
        assert_eq!(mode.to_string(), text);
        assert_eq!(serde_json::to_string(&mode).unwrap(), format!("\"{text}\""));
        assert_eq!(
            serde_json::from_str::<BucketMode>(&format!("\"{text}\"")).unwrap(),
            mode
        );
    }
    assert!(serde_json::from_str::<BucketMode>("\"writeback\"").is_err());
}

#[test]
fn role_documents_hold_policies_as_strings() {
    use skys3_types::RoleDocument;
    use skys3_types::policy::{Policy, PolicyDocument, RequestContext};

    let trust = r#"{"Version":"2012-10-17","Statement":{"Effect":"Allow","Principal":{"Federated":"idp.example"},"Action":"sts:AssumeRoleWithWebIdentity"}}"#;
    let allow =
        r#"{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:*","Resource":"*"}}"#;
    let json = serde_json::json!({
        "trust_policy": trust,
        "policies": [allow],
        "proposal_id": "01J8Z6K3V2Q4",
    })
    .to_string();
    let role = RoleDocument::from_json(json.as_bytes()).unwrap();
    assert_eq!(role.trust_policy.text(), trust);
    assert_eq!(role.policies[0].text(), allow);
    let request = RequestContext::new("s3:GetObject", "arn:aws:s3:::b/k");
    assert!(role.policies[0].policy().evaluate(&request).is_allowed());
    assert_eq!(
        RoleDocument::from_json(&role.to_json().unwrap()).unwrap(),
        role
    );
    assert_eq!(role.proposal_id().to_string(), "01J8Z6K3V2Q4");

    // Without policies, a role may do nothing.
    let bare = serde_json::json!({"trust_policy": trust, "proposal_id": "01J8Z6K3V2Q4"});
    let bare = RoleDocument::from_json(bare.to_string().as_bytes()).unwrap();
    assert!(bare.policies.is_empty());

    let mut crowded = role.clone();
    crowded.policies = vec![role.policies[0].clone(); RoleDocument::MAX_POLICIES + 1];
    let error = crowded.to_json().unwrap_err();
    assert!(matches!(
        error,
        RegisterError::Invalid {
            source: InvalidRegister::TooManyPolicies(11),
            ..
        }
    ));
    assert!(error.to_string().contains("at most 10"), "{error}");

    for bad in [
        serde_json::json!({"trust_policy": allow, "proposal_id": "01J8Z6K3V2Q4"}),
        serde_json::json!({"trust_policy": trust, "policies": [trust], "proposal_id": "01J8Z6K3V2Q4"}),
        serde_json::json!({"trust_policy": {"Version": "2012-10-17"}, "proposal_id": "01J8Z6K3V2Q4"}),
        serde_json::json!({"trust_policy": trust, "proposal_id": "01J8Z6K3V2Q4", "extra": 1}),
    ] {
        assert!(
            RoleDocument::from_json(bad.to_string().as_bytes()).is_err(),
            "{bad}"
        );
    }
    assert!(PolicyDocument::<Policy>::parse("{}").is_err());
}

#[test]
fn role_names_fit_register_keys() {
    use skys3_types::RoleDocument;

    let longest = "r".repeat(64);
    for good in ["deployer", "ci.deploy-1", "A_b", &longest] {
        assert!(RoleDocument::valid_name(good), "{good}");
    }
    let too_long = "r".repeat(65);
    for bad in ["", ".hidden", "a/b", "a+b", "a@b", &too_long] {
        assert!(!RoleDocument::valid_name(bad), "{bad}");
    }
}
