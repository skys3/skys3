//! Erasure coding under crashes (plan M5-04): a shard primary encodes its
//! objects while the primary, a member, or fragment holders are killed or
//! lose power at each step of an attempt, and every object stays readable
//! in full from its replicas or from `k` fragments of each stripe. Seeded
//! bugs show that the checks catch publishing before every fragment is
//! durable, dropping replicas before `EC_PUBLISH` commits, and applying an
//! `EC_PUBLISH` of a superseded version.
//!
//! Orphan reclamation (plan M5-05) runs on every node in the scenarios
//! that race it with publication, slow encodings past
//! `fragment_orphan_after_seconds`, and change the primary by takeover,
//! with the primary dead or cut off; no committed layout may name a
//! reclaimed fragment, and every orphan must be reclaimed. Seeded bugs
//! show that the checks catch a judge that confirms orphans of attempts in
//! progress, one that answers before reconciliation, and a deposed one
//! that judges its successor's attempts. See `skys3_cluster_sim::coding`.

use std::time::Duration;

use rand::Rng;
use rand::seq::IndexedRandom;
use skys3_cluster_sim::coding::{
    self, CodingConfig, Crash, CrashKind, CrashPoint, CrashTarget, OrphanConfig, seed_shard_bug,
};
use skys3_ec::SeededBug as StoreBug;
use skys3_ec::orphans::JudgeBug;
use skys3_shard::SeededBug;
use skys3_sim::{Runner, SimContext};

/// What one run of the six-node cluster costs, in seeds of a typical
/// scenario: about a second in a debug build. A seed of the crash
/// scenario makes thirty runs, one of a seeded-bug scenario two.
const RUN_COST: u64 = 8;

/// Every node a crash can hit.
const TARGETS: [CrashTarget; 5] = [
    CrashTarget::Primary,
    CrashTarget::Member(1),
    CrashTarget::Member(2),
    CrashTarget::Holder,
    CrashTarget::Holders,
];

/// A crash of `target` at `point`, of a kind and downtime the seed draws.
fn drawn_crash(context: &mut SimContext, point: CrashPoint, target: CrashTarget) -> CodingConfig {
    let kinds = [
        CrashKind::Kill,
        CrashKind::PowerLoss,
        CrashKind::PowerAtNextSync,
    ];
    let crash = Crash {
        delay: Duration::ZERO,
        target,
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
    Runner::with_cost(1, 30 * RUN_COST).run(|context| {
        for point in CrashPoint::ALL {
            for target in TARGETS {
                let config = drawn_crash(context, point, target);
                let report = coding::run(context, &config)
                    .map_err(|error| format!("{:?}: {error}", config.crashes))?;
                tracing::debug!(?point, ?report, "a crash run passed");
            }
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
    Runner::with_cost(4, 2 * RUN_COST).run(|context| {
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
    Runner::with_cost(4, 2 * RUN_COST).run(|context| {
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
    Runner::with_cost(4, 2 * RUN_COST).run(|context| {
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

/// Orphan reclamation on every node, asking about fragments once they are
/// `orphan_after` old.
fn reclaiming(orphan_after: Duration, bug: Option<JudgeBug>) -> Option<OrphanConfig> {
    Some(OrphanConfig {
        orphan_after,
        sweep: Duration::from_millis(20),
        bug,
    })
}

#[test]
fn reclamation_racing_publication_reclaims_only_orphans() {
    Runner::with_cost(4, 2 * RUN_COST).run(|context| {
        // Fragments are asked about while their attempts write and
        // publish. A crash at a drawn step leaves orphans, as does the
        // first object's overwrite, which supersedes its attempt.
        let point = *CrashPoint::ALL.choose(context.rng()).expect("points");
        let target = *TARGETS.choose(context.rng()).expect("targets");
        let orphan_after = Duration::from_millis(context.rng().random_range(0..60));
        let crashed = CodingConfig {
            orphans: reclaiming(orphan_after, None),
            ..drawn_crash(context, point, target)
        };
        let report = coding::run(context, &crashed)
            .map_err(|error| format!("{:?}: {error}", crashed.crashes))?;
        tracing::debug!(?report, "a crash run passed");
        let superseded = CodingConfig {
            overwrite: true,
            orphans: reclaiming(orphan_after, None),
            ..CodingConfig::default()
        };
        let report = coding::run(context, &superseded)?;
        if report.rejected == 0 || report.reclaimed == 0 {
            return Err(format!("the superseded attempt left no orphan: {report:?}").into());
        }
        Ok(())
    });
}

/// Fragment writes slow enough that every attempt outlasts
/// `orphan_after`.
fn outlasting(bug: Option<JudgeBug>) -> CodingConfig {
    CodingConfig {
        write_delay: Duration::from_millis(150),
        orphans: reclaiming(Duration::from_millis(100), bug),
        ..CodingConfig::default()
    }
}

#[test]
fn encodings_outlasting_orphan_after_keep_their_fragments() {
    Runner::with_cost(4, 2 * RUN_COST).run(|context| {
        let report = coding::run(context, &outlasting(None))?;
        if report.published < 4 {
            return Err(format!("objects were left unpublished: {report:?}").into());
        }
        Ok(())
    });
}

#[test]
fn confirming_an_orphan_of_an_attempt_in_progress_is_caught() {
    Runner::with_cost(4, 2 * RUN_COST).run(|context| {
        caught(
            context,
            &outlasting(None),
            |config| *config = outlasting(Some(JudgeBug::IgnoreAttempts)),
            &["which was reclaimed"],
        )
    });
}

/// A primary change: the members take over once `crashes` struck at
/// `point`.
fn primary_change(point: CrashPoint, crashes: Vec<Crash>, bug: Option<JudgeBug>) -> CodingConfig {
    CodingConfig {
        crashes: Some((point, crashes)),
        orphans: reclaiming(Duration::from_millis(300), bug),
        takeover: true,
        ..CodingConfig::default()
    }
}

/// `kind` hitting `target` `delay` milliseconds after the point, for
/// `downtime` milliseconds.
fn strike(target: CrashTarget, kind: CrashKind, delay: u64, downtime: u64) -> Crash {
    Crash {
        delay: Duration::from_millis(delay),
        target,
        kind,
        downtime: Duration::from_millis(downtime),
    }
}

#[test]
fn reclamation_across_a_primary_change_reclaims_only_orphans() {
    Runner::with_cost(2, 4 * RUN_COST).run(|context| {
        // The primary dies, or is cut off from its members and the shard
        // register while fragment holders still reach it, at a drawn
        // step, for longer than the members' grace: one of them takes
        // over and encodes what is left, while every node asks the first
        // members about its fragments, in a drawn order.
        for kind in [CrashKind::Kill, CrashKind::Isolate] {
            let point = *CrashPoint::ALL.choose(context.rng()).expect("points");
            let downtime = context.rng().random_range(9_000..12_000);
            let primary = strike(CrashTarget::Primary, kind, 0, downtime);
            let config = primary_change(point, vec![primary], None);
            let report = coding::run(context, &config)
                .map_err(|error| format!("{kind:?} at {point:?}: {error}"))?;
            if report.epoch < 2 {
                return Err(format!("no primary change: {report:?}").into());
            }
        }
        Ok(())
    });
}

#[test]
fn answering_before_reconciliation_is_caught() {
    Runner::with_cost(4, 4 * RUN_COST).run(|context| {
        // A member dies as an attempt's EC_PUBLISH is appended, so the
        // record cannot commit; the primary dies once the other member
        // holds it. That member takes over while the first is still down,
        // and rolls the record forward only once it serves, when the
        // first is back or removed.
        let crashes = vec![
            strike(CrashTarget::Member(2), CrashKind::Kill, 0, 12_000),
            strike(CrashTarget::Primary, CrashKind::Kill, 5, 16_000),
        ];
        let point = CrashPoint::Appended;
        let config = primary_change(point, crashes.clone(), None);
        let bugged = primary_change(point, crashes, Some(JudgeBug::BeforeServing));
        caught(
            context,
            &config,
            |config| *config = bugged,
            &["which was reclaimed"],
        )
    });
}

#[test]
fn a_deposed_primary_judging_a_later_epoch_is_caught() {
    Runner::with_cost(4, 4 * RUN_COST).run(|context| {
        // The primary is cut off from its members and the register, and
        // fragment holders ask it about its successor's fragments.
        let crashes = vec![strike(CrashTarget::Primary, CrashKind::Isolate, 0, 12_000)];
        let config = primary_change(CrashPoint::StripeStarted, crashes.clone(), None);
        let bugged = primary_change(
            CrashPoint::StripeStarted,
            crashes,
            Some(JudgeBug::IgnoreLaterEpochs),
        );
        caught(
            context,
            &config,
            |config| *config = bugged,
            &["which was reclaimed"],
        )
    });
}
