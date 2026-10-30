//! Lifecycle expiration and multipart cleanup (plan M5-10, design §8.7)
//! across primary changes: each shard primary evaluates its bucket's rules
//! over its index, and a primary that crashes, loses power, or is cut off
//! is taken over while its passes commit expirations.
//!
//! The cluster runs replicated shards with takeovers, the M1 workload on
//! keys no rule matches, and a lifecycle client (`ClusterConfig::lifecycle`)
//! that writes objects with prefixes, sizes, and tags, and opens uploads,
//! for rules that expire them at chosen lifecycle days, each a second of
//! simulated time, while the faults fall. The checks: the client finds,
//! through the gateways, exactly the objects and uploads the rules expire
//! gone; and in the final log of each shard's primary every `DELETE` of
//! the client's keys follows a `PUT` of a version the rules expire, so no
//! version is expired twice, or expired that no rule expires. The M1
//! workload's history stays linearizable and durable around them.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use skys3_cluster_sim::{
    Cluster, ClusterConfig, Endpoint, Fault, FaultPlan, FaultProfile, Lifecycle, LifecycleObject,
    LifecycleStart, ReplicatedServices, RoutedServices, View, Workload,
};
use skys3_gateway::routing::RoutingConfig;
use skys3_shard::replication::ReplicationConfig;
use skys3_sim::{Runner, SimContext};
use skys3_types::lifecycle::{Expiration, LifecycleConfiguration, LifecycleRule, RuleFilter};

use crate::COST;

fn rule(id: &str, prefix: &str, expiration: Option<Expiration>) -> LifecycleRule {
    LifecycleRule {
        id: id.to_owned(),
        enabled: true,
        filter: RuleFilter {
            prefix: prefix.to_owned(),
            ..RuleFilter::default()
        },
        expiration,
        abort_upload_days: None,
    }
}

fn tags(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect()
}

/// Rules that expire objects at lifecycle days 0 (by age), 3, 5, and 7,
/// by prefix, tag, and size, and abort uploads under `up/`; and objects
/// and uploads that each rule matches or just misses.
fn lifecycle() -> Lifecycle {
    let date = |day| Some(Expiration::DateMs(Lifecycle::date(day)));
    let mut temp_logs = rule("temp-logs", "logs/", date(3));
    temp_logs.filter.tags = tags(&[("class", "temp")]);
    let mut big_data = rule("big-data", "data/", date(7));
    big_data.filter.size_greater_than = Some(100);
    let mut uploads = rule("uploads", "up/", None);
    uploads.abort_upload_days = Some(1);
    let mut off = rule("off", "keep/", date(1));
    off.enabled = false;
    let configuration = LifecycleConfiguration {
        rules: vec![
            temp_logs,
            rule("tmp", "tmp/", date(5)),
            big_data,
            rule("old", "old/", Some(Expiration::Days(1))),
            uploads,
            off,
        ],
    };
    configuration.validate().unwrap();
    let object = |key: &str, size, pairs: &[(&str, &str)]| LifecycleObject {
        key: key.to_owned(),
        size,
        tags: tags(pairs),
    };
    let temp = [("class", "temp")];
    Lifecycle {
        configuration,
        interval: Duration::from_millis(200),
        day: Duration::from_secs(1),
        objects: vec![
            object("logs/a", 10, &temp),
            object("logs/b", 10, &[("class", "kept")]),
            object("logs/c", 10, &[]),
            object("logs/d", 600, &temp),
            object("tmp/a", 10, &[]),
            object("tmp/b", 700, &temp),
            object("data/small", 50, &[]),
            object("data/big", 200, &[]),
            object("old/a", 10, &[]),
            object("keep/a", 10, &temp),
        ],
        uploads: vec![
            "up/a".to_owned(),
            "up/b".to_owned(),
            "keep/upload".to_owned(),
        ],
        start: LifecycleStart::default(),
    }
}

/// Three nodes, every shard on all of them, and one `local` bucket with
/// the lifecycle rules.
fn config() -> ClusterConfig {
    ClusterConfig {
        replicas: 3,
        every_member_durable: true,
        buckets: 1,
        shards_per_bucket: 2,
        lifecycle: Some(lifecycle()),
        ..ClusterConfig::default()
    }
}

/// Clients that send each request to any node and wait out a takeover.
fn workload(context: &SimContext) -> Workload {
    Workload {
        clients: 2,
        operations: 30 * context.scale() as usize,
        keys: 4,
        think_time: Duration::from_millis(150),
        timeout: Duration::from_secs(6),
        any_gateway: true,
        ..Workload::default()
    }
}

/// Routing over replicated shards whose members take over from a primary
/// that is down, with the timings of the takeover scenarios.
fn services() -> RoutedServices {
    let replication = ReplicationConfig {
        lease_renew_interval: Duration::from_millis(200),
        primary_lease: Duration::from_millis(800),
        primary_grace: Duration::from_millis(1400),
        member_suspect_after: Duration::from_millis(700),
        ..ReplicationConfig::default()
    };
    let routing = RoutingConfig {
        connect_timeout: Duration::from_millis(300),
        request_timeout: Duration::from_secs(4),
        register_interval: Duration::from_millis(200),
        ..RoutingConfig::default()
    };
    RoutedServices::new(
        ReplicatedServices::new(replication).with_takeover(),
        routing,
    )
}

/// The cluster, checked against the invariants of §6.8 after every step,
/// recording the most shards served by a primary other than their first.
fn cluster(services: &RoutedServices, taken: &Arc<Mutex<usize>>) -> Cluster<RoutedServices> {
    let taken = Arc::clone(taken);
    Cluster::with_services(config(), services.clone()).invariant(
        move |view: &View<'_, RoutedServices>| {
            let replicated = view.services.replicated();
            replicated.check_commits()?;
            view.services.check_served()?;
            let mut most = taken.lock().unwrap_or_else(PoisonError::into_inner);
            *most = (*most).max(replicated.taken_over().len());
            Ok(())
        },
    )
}

/// What the rules expire of the client's objects in one bucket.
fn expiring() -> usize {
    let lifecycle = lifecycle();
    lifecycle
        .objects
        .iter()
        .filter(|object| lifecycle.expiry(object).is_some())
        .count()
}

/// A primary loses power or crashes while its passes expire objects, and
/// the other primary later, so that new primaries evaluate the rules
/// again. For half the seeds the first primary's messages to its members
/// are held from just before a rule's date until after the crash, so
/// that its pass appends `DELETE`s that never commit, and the members
/// truncate them; for the others it crashes at a moment that varies with
/// the seed. Every version is expired exactly once.
#[test]
fn expirations_commit_once_across_primary_changes() {
    Runner::with_cost(4, COST).run(|context| {
        let seed = context.seed();
        // Nodes 0 and 1 are the first primaries of the two shards.
        let first = (seed % 2) as usize;
        let mut plan = FaultPlan::none();
        let at = if (seed / 2) % 2 == 0 {
            // Lifecycle day `date` begins `date` seconds into the workload.
            let date = 3 + 2 * ((seed / 4) % 2);
            let begins = Duration::from_secs(date);
            for other in [0, 1, 2].into_iter().filter(|node| *node != first) {
                plan.push(
                    begins - Duration::from_millis(100),
                    Fault::Hold {
                        a: Endpoint::Node(first),
                        b: Endpoint::Node(other),
                        duration: Duration::from_millis(1500),
                    },
                );
            }
            // Before the primary would remove the silent members (§6.4).
            begins + Duration::from_millis(250 + 40 * (seed % 8))
        } else {
            Duration::from_millis(1500 + 250 * (seed % 16))
        };
        plan.push(
            at,
            Fault::Crash {
                node: first,
                power_loss: seed % 8 == 1,
                downtime: Duration::from_secs(3),
            },
        );
        plan.push(
            at + Duration::from_millis(3200),
            Fault::Crash {
                node: 1 - first,
                power_loss: seed % 4 < 2,
                downtime: Duration::from_secs(2),
            },
        );
        let taken = Arc::new(Mutex::new(0));
        let report = cluster(&services(), &taken).run(context, &workload(context), &plan)?;
        let taken = *taken.lock().unwrap_or_else(PoisonError::into_inner);
        eprintln!(
            "seed {seed}: {:?}; {taken} shards taken over",
            report.lifecycle
        );
        assert!(
            report.lifecycle.expired >= expiring(),
            "{:?}",
            report.lifecycle
        );
        assert!(taken > 0, "no shard was taken over");
        Ok(())
    });
}

/// Random crashes, partitions, message loss, and control-store faults
/// while the rules expire objects.
#[test]
fn expirations_commit_once_under_random_faults() {
    Runner::with_cost(4, COST).run(|context| {
        let workload = workload(context);
        let profile = FaultProfile {
            end: Duration::from_secs(8) * context.scale(),
            crashes: 2,
            partitions: 2,
            message_loss: 1,
            control: 1,
            max_duration: Duration::from_secs(3),
            ..FaultProfile::default()
        };
        let mut plan = FaultPlan::random(context.rng(), &profile, 3, 2, workload.clients);
        // Two nodes cut off from each other while the first dates pass.
        plan.push(
            Duration::from_secs(2),
            Fault::Partition {
                a: Endpoint::Node((context.seed() % 3) as usize),
                b: Endpoint::Node((context.seed() as usize + 1) % 3),
                duration: Duration::from_secs(2),
            },
        );
        let taken = Arc::new(Mutex::new(0));
        let report = cluster(&services(), &taken).run(context, &workload, &plan)?;
        eprintln!("seed {}: {:?}", context.seed(), report.lifecycle);
        assert!(
            report.lifecycle.expired >= expiring(),
            "{:?}",
            report.lifecycle
        );
        Ok(())
    });
}
