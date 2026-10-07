//! Erasure coding under crashes (plan M5-04): a shard primary encodes its
//! objects while the primary, a member, or fragment holders are killed or
//! lose power at each step of an attempt, and every object stays readable
//! in full from its replicas or from `k` fragments of each stripe. Seeded
//! bugs show that the checks catch publishing before every fragment is
//! durable, dropping replicas before `EC_PUBLISH` commits, and applying an
//! `EC_PUBLISH` of a superseded version. See
//! `skys3_cluster_sim::coding`.

use std::time::Duration;

use rand::Rng;
use rand::seq::IndexedRandom;
use skys3_cluster_sim::coding::{
    self, CodingConfig, Crash, CrashKind, CrashPoint, CrashTarget, seed_shard_bug,
};
use skys3_ec::SeededBug as StoreBug;
use skys3_shard::SeededBug;
use skys3_sim::{Runner, SimContext};

/// What one seed of these scenarios costs, in seeds of a typical scenario:
/// two to five runs of a six-node cluster, about a second each in a debug
/// build. CI's 256 seeds run 16 of each.
const COST: u64 = 16;

/// A crash at `point` of a target and kind the seed draws.
fn drawn_crash(context: &mut SimContext, point: CrashPoint) -> CodingConfig {
    let targets = [
        CrashTarget::Primary,
        CrashTarget::Member(1),
        CrashTarget::Member(2),
        CrashTarget::Holder,
        CrashTarget::Holders,
    ];
    let kinds = [
        CrashKind::Kill,
        CrashKind::PowerLoss,
        CrashKind::PowerAtNextSync,
    ];
    let crash = Crash {
        delay: Duration::ZERO,
        target: *targets.choose(context.rng()).expect("targets"),
        kind: *kinds.choose(context.rng()).expect("kinds"),
        downtime: Duration::from_millis(context.rng().random_range(200..2000)),
    };
    CodingConfig {
        crashes: Some((point, vec![crash])),
        ..CodingConfig::default()
    }
}

#[test]
fn crashes_at_every_encoding_step_keep_every_object_readable() {
    Runner::with_cost(4, COST).run(|context| {
        for point in CrashPoint::ALL {
            let config = drawn_crash(context, point);
            let report = coding::run(context, &config)
                .map_err(|error| format!("{point:?} with {:?}: {error}", config.crashes))?;
            tracing::debug!(?point, ?report, "a crash run passed");
        }
        Ok(())
    });
}

/// Runs `config` without a bug, which must pass, then with `seed`
/// seeding a bug, which a check must catch with an error containing one
/// of `caught`.
fn caught(
    context: &mut SimContext,
    config: &CodingConfig,
    seed: impl FnOnce(&mut CodingConfig),
    caught: &[&str],
) -> turmoil::Result {
    let mut replay = SimContext::with_scale(context.seed(), context.scale());
    coding::run(&mut replay, config).map_err(|error| format!("without the bug: {error}"))?;
    let mut bugged = config.clone();
    seed(&mut bugged);
    let outcome = coding::run(context, &bugged);
    seed_shard_bug(None);
    match outcome {
        Err(error) if caught.iter().any(|reason| error.contains(reason)) => Ok(()),
        Err(error) => Err(format!("caught for another reason: {error}").into()),
        Ok(report) => Err(format!("the bug went unnoticed: {report:?}").into()),
    }
}

#[test]
fn publishing_before_every_fragment_is_durable_is_caught() {
    Runner::with_cost(4, COST).run(|context| {
        // Every holder of a stripe but the primary loses power at its next
        // sync: the sync that would make its fragment durable.
        let crash = Crash {
            delay: Duration::ZERO,
            target: CrashTarget::Holders,
            kind: CrashKind::PowerAtNextSync,
            downtime: Duration::from_millis(500),
        };
        let config = CodingConfig {
            crashes: Some((CrashPoint::LastStripeStarted, vec![crash])),
            ..CodingConfig::default()
        };
        caught(
            context,
            &config,
            |config| config.store_bug = Some(StoreBug::AcknowledgeBeforeSync),
            &["readable neither", "does not decode"],
        )
    });
}

#[test]
fn dropping_replicas_before_the_publish_commits_is_caught() {
    Runner::with_cost(4, COST).run(|context| {
        // One member is down long enough that the EC_PUBLISH cannot
        // commit; the other restarts at once and replays it, uncommitted.
        let kill = |delay, member, downtime| Crash {
            delay: Duration::from_millis(delay),
            target: CrashTarget::Member(member),
            kind: CrashKind::Kill,
            downtime: Duration::from_millis(downtime),
        };
        let config = CodingConfig {
            crashes: Some((
                CrashPoint::Appended,
                vec![kill(0, 2, 3000), kill(20, 1, 300)],
            )),
            ..CodingConfig::default()
        };
        caught(
            context,
            &config,
            |_| seed_shard_bug(Some(SeededBug::DropBeforeCommit)),
            &["before the EC_PUBLISH"],
        )
    });
}

#[test]
fn applying_a_superseded_publish_is_caught() {
    Runner::with_cost(4, COST).run(|context| {
        // The first object is overwritten, with as many bytes, while it is
        // encoded.
        let config = CodingConfig {
            overwrite: true,
            object_bytes: 48 * 1024..64 * 1024,
            ..CodingConfig::default()
        };
        caught(
            context,
            &config,
            |_| seed_shard_bug(Some(SeededBug::PublishSuperseded)),
            &["not the", "does not decode"],
        )
    });
}
