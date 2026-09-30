//! Property tests: text and JSON round trips, and total parsers.

use proptest::collection::vec;
use proptest::prelude::*;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::num::NonZeroU16;

use skys3_types::{
    BucketDocument, BucketId, BucketMode, BucketName, ClusterDocument, ClusterId, CoordinatorLease,
    DiskInfo, DnsName, ETag, Epoch, EpochSeq, Generation, Host, KeyHash, Label, NodeAddress,
    NodeId, NodeRegistration, ProposalId, RegisterDocument, RemoteTarget, Seq, ShardConfig,
    ShardCount, ShardId, VersionIdentity, WriteIdentity,
};

/// A lowercase DNS-label identifier of 1 to `max` bytes.
fn label(max: usize) -> impl Strategy<Value = String> {
    let middle = max.saturating_sub(2);
    prop_oneof![
        "[a-z0-9]".boxed(),
        proptest::string::string_regex(&format!("[a-z0-9][a-z0-9-]{{0,{middle}}}[a-z0-9]"))
            .unwrap()
            .boxed(),
    ]
}

fn cluster_id() -> impl Strategy<Value = ClusterId> {
    label(ClusterId::MAX_LEN).prop_map(|s| ClusterId::new(s).unwrap())
}

fn bucket_id() -> impl Strategy<Value = BucketId> {
    label(BucketId::MAX_LEN).prop_map(|s| BucketId::new(s).unwrap())
}

fn node_id() -> impl Strategy<Value = NodeId> {
    label(NodeId::MAX_LEN).prop_map(|s| NodeId::new(s).unwrap())
}

fn proposal_id() -> impl Strategy<Value = ProposalId> {
    prop_oneof![
        "[A-Za-z0-9_-]{1,64}".prop_map(|s| ProposalId::new(s).unwrap()),
        any::<u128>().prop_map(ProposalId::from_u128),
    ]
}

fn epoch_seq() -> impl Strategy<Value = EpochSeq> {
    (any::<u64>(), any::<u64>()).prop_map(|(e, s)| EpochSeq::new(Epoch::new(e), Seq::new(s)))
}

fn write_identity() -> impl Strategy<Value = WriteIdentity> {
    (cluster_id(), bucket_id(), any::<u8>(), epoch_seq()).prop_map(
        |(cluster, bucket, shard, position)| {
            WriteIdentity::new(cluster, bucket, ShardId::new(shard), position)
        },
    )
}

fn distinct_nodes(max: usize) -> impl Strategy<Value = Vec<NodeId>> {
    proptest::collection::btree_set(node_id(), 0..=max).prop_map(|set| set.into_iter().collect())
}

/// A valid shard configuration: distinct members and learners, the primary
/// among the members, and consistent replica counts.
fn shard_config() -> impl Strategy<Value = ShardConfig> {
    (
        bucket_id(),
        any::<u8>(),
        any::<u64>(),
        distinct_nodes(12),
        any::<prop::sample::Index>(),
        1..=u8::MAX,
        any::<prop::sample::Index>(),
        proposal_id(),
    )
        .prop_filter("needs a member", |t| !t.3.is_empty())
        .prop_map(
            |(bucket_id, shard, epoch, nodes, split, replicas, min, proposal_id)| {
                let members_len = split.index(nodes.len()) + 1;
                let (members, learners) = nodes.split_at(members_len);
                let primary = members[split.index(members.len())].clone();
                ShardConfig {
                    bucket_id,
                    shard: ShardId::new(shard),
                    epoch: Epoch::new(epoch),
                    primary,
                    members: members.to_vec(),
                    learners: learners.to_vec(),
                    min_write_replicas: u8::try_from(min.index(usize::from(replicas)) + 1).unwrap(),
                    replicas,
                    proposal_id,
                }
            },
        )
}

fn bucket_document() -> impl Strategy<Value = BucketDocument> {
    let target = (
        "https://[a-z0-9.-]{1,40}",
        "[a-zA-Z0-9._-]{3,63}",
        proptest::option::of("[ -~]{0,100}"),
    )
        .prop_map(|(endpoint, bucket, prefix)| RemoteTarget {
            endpoint,
            bucket,
            prefix,
        });
    (
        bucket_id(),
        "[a-z0-9][a-z0-9-]{1,61}[a-z0-9]",
        prop_oneof![
            Just(BucketMode::WriteBack),
            Just(BucketMode::Local),
            Just(BucketMode::ReadOnly)
        ],
        1..=ShardCount::MAX,
        (
            1..=u8::MAX,
            any::<prop::sample::Index>(),
            any::<prop::sample::Index>(),
        ),
        target,
        proposal_id(),
    )
        .prop_map(
            |(bucket_id, name, mode, shards, (replicas, min, clean), target, proposal_id)| {
                BucketDocument {
                    bucket_id,
                    name: BucketName::new(name).unwrap(),
                    mode,
                    shards: ShardCount::new(shards).unwrap(),
                    replicas,
                    min_write_replicas: u8::try_from(min.index(usize::from(replicas)) + 1).unwrap(),
                    clean_copies: u8::try_from(clean.index(usize::from(replicas) + 1)).unwrap(),
                    target: mode.has_target().then_some(target),
                    proposal_id,
                }
            },
        )
}

/// A DNS name of 1 to 4 labels whose last label has a letter.
fn dns_name() -> impl Strategy<Value = DnsName> {
    (vec(label(63), 0..4), "[a-z]([a-z0-9-]{0,61}[a-z0-9])?").prop_filter_map(
        "at most 253 bytes",
        |(mut labels, last)| {
            labels.push(last);
            DnsName::new(labels.join(".")).ok()
        },
    )
}

fn node_address() -> impl Strategy<Value = NodeAddress> {
    let host = prop_oneof![
        dns_name().prop_map(Host::Dns),
        any::<u32>().prop_map(|ip| Host::Ipv4(Ipv4Addr::from(ip))),
        any::<u128>().prop_map(|ip| Host::Ipv6(Ipv6Addr::from(ip))),
    ];
    (host, 1..=u16::MAX)
        .prop_map(|(host, port)| NodeAddress::new(host, NonZeroU16::new(port).unwrap()))
}

fn node_registration() -> impl Strategy<Value = NodeRegistration> {
    let label = || label(Label::MAX_LEN).prop_map(|s| Label::new(s).unwrap());
    (
        node_id(),
        node_address(),
        proptest::option::of(label()),
        proptest::option::of(label()),
        proptest::collection::btree_map(label(), any::<u64>(), 0..8),
        proposal_id(),
    )
        .prop_map(
            |(node_id, address, zone, rack, disks, proposal_id)| NodeRegistration {
                node_id,
                address,
                zone,
                rack,
                disks: disks
                    .into_iter()
                    .map(|(disk_id, capacity_bytes)| DiskInfo {
                        disk_id,
                        capacity_bytes,
                    })
                    .collect(),
                proposal_id,
            },
        )
}

fn round_trip<T: RegisterDocument + PartialEq + std::fmt::Debug>(document: &T) {
    let json = document.to_json().unwrap();
    assert_eq!(&T::from_json(&json).unwrap(), document);
}

proptest! {
    #[test]
    fn node_addresses_round_trip(address in node_address()) {
        let text = address.to_string();
        prop_assert!(text.len() <= NodeAddress::MAX_LEN);
        prop_assert_eq!(text.parse::<NodeAddress>().unwrap(), address.clone());
        let json = serde_json::to_string(&address).unwrap();
        prop_assert_eq!(serde_json::from_str::<NodeAddress>(&json).unwrap(), address);
    }

    #[test]
    fn node_address_parse_is_total_and_canonical(
        s in "[a-z0-9.:\\[\\]-]{0,40}|\\PC{0,60}",
    ) {
        // Whatever parses prints in canonical form, which re-parses equal and
        // is the input itself unless an IPv6 literal was normalized.
        if let Ok(address) = s.parse::<NodeAddress>() {
            let canonical = address.to_string();
            prop_assert_eq!(canonical.parse::<NodeAddress>().unwrap(), address.clone());
            if !matches!(address.host(), Host::Ipv6(_)) {
                prop_assert_eq!(canonical, s);
            }
        }
    }

    #[test]
    fn ids_round_trip_through_text_and_json(cluster in cluster_id(), node in node_id()) {
        prop_assert_eq!(cluster.to_string().parse::<ClusterId>().unwrap(), cluster.clone());
        let json = serde_json::to_string(&node).unwrap();
        prop_assert_eq!(serde_json::from_str::<NodeId>(&json).unwrap(), node);
    }

    #[test]
    fn id_constructors_are_total(s in any::<String>()) {
        // Accepting and rejecting are both fine; panicking is not, and an
        // accepted value must be unchanged.
        if let Ok(id) = BucketId::new(s.clone()) {
            prop_assert_eq!(id.as_str(), s.as_str());
            prop_assert!(id.as_str().len() <= BucketId::MAX_LEN);
        }
        if let Ok(name) = BucketName::new(s.clone()) {
            prop_assert_eq!(name.as_str(), s.as_str());
        }
        if let Ok(id) = ProposalId::new(s.clone()) {
            prop_assert_eq!(id.as_str(), s.as_str());
        }
    }

    #[test]
    fn epoch_seq_round_trips_and_orders_like_tuples(a in epoch_seq(), b in epoch_seq()) {
        prop_assert_eq!(a.to_string().parse::<EpochSeq>().unwrap(), a);
        let json = serde_json::to_string(&a).unwrap();
        prop_assert_eq!(serde_json::from_str::<EpochSeq>(&json).unwrap(), a);
        prop_assert_eq!(a.cmp(&b), (a.epoch.get(), a.seq.get()).cmp(&(b.epoch.get(), b.seq.get())));
    }

    #[test]
    fn counters_round_trip_through_text(n in any::<u64>()) {
        prop_assert_eq!(n.to_string().parse::<Seq>().unwrap(), Seq::new(n));
        prop_assert_eq!(n.to_string().parse::<Generation>().unwrap(), Generation::new(n));
    }

    #[test]
    fn write_identity_round_trips_within_its_limit(wid in write_identity()) {
        let text = wid.to_string();
        prop_assert!(text.len() <= WriteIdentity::MAX_LEN);
        prop_assert!(wid.matches(&text));
        prop_assert_eq!(text.parse::<WriteIdentity>().unwrap(), wid.clone());
        let json = serde_json::to_string(&wid).unwrap();
        prop_assert_eq!(serde_json::from_str::<WriteIdentity>(&json).unwrap(), wid);
    }

    #[test]
    fn write_identity_parse_is_total_and_canonical(s in "[a-z0-9./-]{0,100}|\\PC{0,100}") {
        // Whatever parses must print back as exactly the input.
        if let Ok(wid) = s.parse::<WriteIdentity>() {
            prop_assert_eq!(wid.to_string(), s);
        }
    }

    #[test]
    fn distinct_write_identities_have_distinct_text(a in write_identity(), b in write_identity()) {
        prop_assert_eq!(a == b, a.to_string() == b.to_string());
        prop_assert_eq!(a == b, a.matches(&b.to_string()));
    }

    #[test]
    fn etags_round_trip_quoted(value in "[!#-~]{1,256}") {
        let etag = ETag::new(value.clone()).unwrap();
        prop_assert_eq!(ETag::from_quoted(&etag.to_quoted()).unwrap(), etag.clone());
        let version = VersionIdentity::new(Seq::new(3), etag);
        let json = serde_json::to_string(&version).unwrap();
        prop_assert_eq!(serde_json::from_str::<VersionIdentity>(&json).unwrap(), version);
    }

    #[test]
    fn etag_parse_is_total(s in any::<String>()) {
        if let Ok(etag) = ETag::from_quoted(&s) {
            prop_assert_eq!(etag.to_quoted(), s);
        }
    }

    #[test]
    fn shards_are_in_range_and_stable(bucket in bucket_id(), key in vec(any::<u8>(), 0..1100), count in 1..=ShardCount::MAX) {
        let count = ShardCount::new(count).unwrap();
        let hash = KeyHash::of(&bucket, &key);
        prop_assert_eq!(hash, KeyHash::of(&bucket, &key));
        let shard = hash.shard(count);
        prop_assert!(count.contains(shard));
        prop_assert_eq!(u64::from(shard.get()), hash.get() % u64::from(count.get()));
    }

    #[test]
    fn shard_configs_round_trip(config in shard_config()) {
        round_trip(&config);
    }

    #[test]
    fn bucket_documents_round_trip(bucket in bucket_document()) {
        round_trip(&bucket);
    }

    #[test]
    fn node_registrations_round_trip(registration in node_registration()) {
        round_trip(&registration);
    }

    #[test]
    fn small_documents_round_trip(
        cluster_id in cluster_id(),
        generation in any::<u64>(),
        holder in node_id(),
        a in proposal_id(),
        b in proposal_id(),
    ) {
        round_trip(&ClusterDocument {
            cluster_id,
            format_version: ClusterDocument::FORMAT_VERSION,
            generation: Generation::new(generation),
            proposal_id: a,
        });
        round_trip(&CoordinatorLease { holder, proposal_id: b });
    }

    #[test]
    fn register_parsers_are_total(bytes in vec(any::<u8>(), 0..300)) {
        let _ = ShardConfig::from_json(&bytes);
        let _ = BucketDocument::from_json(&bytes);
        let _ = NodeRegistration::from_json(&bytes);
    }
}
