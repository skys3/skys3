//! The erasure-coding simulation suite (plan M5-12, design §16.1): what
//! the scenarios of plans M5-04 to M5-09 leave open, on the same coding
//! cluster (see `skys3_cluster_sim::coding`).
//!
//! - **Loss combinations.** For the layouts 2+1, 3+2 (the default), and
//!   4+2, every combination of up to `m` lost fragment indices and of up
//!   to `m` lost holders reads every object back through the gateways,
//!   and every combination of `m + 1` fails cleanly, never with other
//!   bytes. Every run reads every combination.
//! - **Crashes at each step.** A process kill, a power loss, and a power
//!   cut at the next sync, at each step of encoding (the fragment writes,
//!   the `EC_PUBLISH`, the members dropping their replicas) and of repair
//!   (the rebuilt fragments' writes, the `EC_RELOCATE`, the release of the
//!   fragments it replaced), with orphan reclamation and fragment release
//!   on every node: no object is lost, and the run settles only once every
//!   fragment a node holds is named by a layout.
//! - **Races.** Overwrites and deletes of a key racing its repair, and a
//!   holder lost for good in the middle of a drain: nothing resurrects or
//!   is lost, and the repairers leave the stripes alone once they are
//!   whole.
//! - **Self-healing.** A holder lost for good is rebuilt on the survivors
//!   with no operator action, and the metrics behind
//!   `SkyS3FragmentsUnrepaired` clear.
//!
//! Each scenario has a seeded bug that its checks catch.

use std::time::Duration;

use rand::Rng;
use rand::seq::IndexedRandom;
use skys3_cluster_sim::coding::{
    self, CodingConfig, CodingReport, Crash, CrashKind, CrashPoint, Loss, MoveCrash, MovePoint,
    MoveTarget, RaceConfig, RaceOp, ReadConfig, RebalanceConfig, ReleaseConfig, RepairConfig,
    RepairCrash, RepairOutcome, RepairPoint, RepairTarget, seed_shard_bug,
};
use skys3_config::EcConfig;
use skys3_ec::orphans::JudgeBug;
use skys3_ec::read::seeded::ReadBug;
use skys3_ec::repair::RepairBug;
use skys3_shard::SeededBug;
use skys3_sim::{Runner, SimContext};

use super::coding::{TARGETS, caught, reclaiming};

/// What one run of the coding cluster costs, in seeds of a typical
/// scenario: about a second in a debug build.
const RUN_COST: u64 = 8;

/// What one run that also reads every loss combination costs: about five
/// seconds, the 4+2 runs a little more.
const COMBINATIONS_COST: u64 = 40;

/// What one run that loses holders and repairs them costs: the encoding,
/// then a few seconds of repairs, with fragment release and a quiet second
/// at the end.
const REPAIR_COST: u64 = 32;

/// What a seed of a crash matrix declares: what makes CI's 256 seeds run
/// the three consecutive seeds that strike every step with each kind of
/// crash. A seed of the encoding matrix makes seven runs, about 15 seconds
/// in a debug build, and one of the repair matrix eleven, about 45.
const MATRIX_COST: u64 = 85;

/// The crash kinds, one per seed in turn: three consecutive seeds strike
/// every step with each.
const KINDS: [CrashKind; 3] = [
    CrashKind::Kill,
    CrashKind::PowerLoss,
    CrashKind::PowerAtNextSync,
];

/// The kind of crash seed `seed` strikes with.
fn kind_of(context: &SimContext) -> CrashKind {
    KINDS[(context.seed() % KINDS.len() as u64) as usize]
}

/// The `[ec]` policies whose stripes six nodes code as 2+1, 3+2, and 4+2.
fn layouts() -> [(usize, usize, EcConfig); 3] {
    [
        (
            2,
            1,
            EcConfig {
                parity_fragments: 1,
                min_eligible_nodes: 3,
                max_data_fragments: 2,
                ..EcConfig::default()
            },
        ),
        (3, 2, EcConfig::default()),
        (
            4,
            2,
            EcConfig {
                min_eligible_nodes: 6,
                max_data_fragments: 4,
                ..EcConfig::default()
            },
        ),
    ]
}

/// A run that reads every loss combination once every object is coded,
/// under `ec`, with `bug` seeded into the reads.
fn combining(ec: EcConfig, bug: Option<ReadBug>) -> CodingConfig {
    CodingConfig {
        reads: Some(ReadConfig {
            clients: 0,
            gets: 0,
            lossy: 0,
            corrupt_only: false,
            bug,
        }),
        combinations: true,
        ec,
        ..CodingConfig::default()
    }
}

/// The number of sets of 1 to `most` of `count` items.
fn sets(count: usize, most: usize) -> usize {
    let mut total = 0;
    let mut choose = 1;
    for size in 1..=most.min(count) {
        choose = choose * (count + 1 - size) / size;
        total += choose;
    }
    total
}

#[test]
fn every_loss_of_up_to_m_fragments_or_holders_reads_back() {
    Runner::with_cost(1, 3 * COMBINATIONS_COST).run(|context| {
        for (k, m, ec) in layouts() {
            let report = coding::run(context, &combining(ec, None))
                .map_err(|error| format!("{k}+{m}: {error}"))?;
            let combinations = report.combinations.ok_or("no combinations were read")?;
            tracing::info!(?combinations, "the loss combinations passed");
            let expected = ((k, m), 1 + sets(k + m, m + 1), sets(6, m + 1));
            let read = (
                combinations.geometry,
                combinations.fragment_sets,
                combinations.holder_sets,
            );
            if read != expected {
                return Err(format!("read {read:?} combinations, not {expected:?}").into());
            }
            if combinations.served == 0 || combinations.refused == 0 {
                return Err(format!("no read was served or refused: {combinations:?}").into());
            }
        }
        Ok(())
    });
}

#[test]
fn decoding_a_stripe_short_of_k_fragments_is_caught() {
    Runner::with_cost(1, 2 * COMBINATIONS_COST).run(|context| {
        caught(
            context,
            &combining(EcConfig::default(), None),
            |config| {
                if let Some(reads) = &mut config.reads {
                    reads.bug = Some(ReadBug::FillMissing);
                }
            },
            &["not those of the version written"],
        )
    });
}

/// Orphan reclamation, asking about fragments as soon as it sees them, and
/// fragment release on every node.
fn reclaiming_and_releasing(config: CodingConfig, bug: Option<JudgeBug>) -> CodingConfig {
    CodingConfig {
        orphans: reclaiming(Duration::ZERO, bug),
        release: Some(ReleaseConfig::default()),
        ..config
    }
}

/// Every step of encoding: those of an attempt, then the members dropping
/// their replicas.
fn encoding_steps() -> impl Iterator<Item = CrashPoint> {
    CrashPoint::ALL
        .into_iter()
        .chain([CrashPoint::ReplicaDropped])
}

/// The kind a crash matrix strikes `point` with in a seed of `kind`: the
/// members drop their replicas last, and no node reliably syncs after
/// they did (a cut armed there fired in 7 of 40 runs), so a power cut at
/// the next sync becomes a power loss there.
fn encoding_kind(point: CrashPoint, kind: CrashKind) -> CrashKind {
    match (point, kind) {
        (CrashPoint::ReplicaDropped, CrashKind::PowerAtNextSync) => CrashKind::PowerLoss,
        _ => kind,
    }
}

/// Checks that a crash matrix's run struck: its planned crash resolved to
/// at least one node, every one of which it struck and which came back.
fn struck(report: &CodingReport) -> Result<(), String> {
    if report.strikes.is_empty() {
        return Err("the crash resolved to no node".to_owned());
    }
    for strike in &report.strikes {
        strike.check()?;
    }
    Ok(())
}

#[test]
fn crashes_of_each_kind_at_each_encoding_step_lose_and_leak_nothing() {
    Runner::with_cost(1, MATRIX_COST).run(|context| {
        let kind = kind_of(context);
        for point in encoding_steps() {
            let crash = Crash {
                delay: Duration::ZERO,
                target: *TARGETS.choose(context.rng()).expect("targets"),
                kind: encoding_kind(point, kind),
                downtime: Duration::from_millis(context.rng().random_range(200..2000)),
            };
            let config = reclaiming_and_releasing(
                CodingConfig {
                    crashes: Some((point, vec![crash])),
                    ..CodingConfig::default()
                },
                None,
            );
            let report = coding::run(context, &config)
                .map_err(|error| format!("{point:?}, {crash:?}: {error}"))?;
            struck(&report).map_err(|error| format!("{point:?}, {crash:?}: {error}"))?;
        }
        Ok(())
    });
}

#[test]
fn keeping_fragments_judged_in_progress_for_good_is_caught() {
    Runner::with_cost(1, 14 * RUN_COST).run(|context| {
        // Slow fragment writes: the holders of an attempt's first stripe
        // ask about its fragments while the attempt still writes the next.
        // The primary then dies, which abandons the attempt.
        let crash = Crash {
            delay: Duration::from_millis(60),
            target: coding::CrashTarget::Primary,
            kind: CrashKind::Kill,
            downtime: Duration::from_millis(500),
        };
        let config = CodingConfig {
            crashes: Some((CrashPoint::StripeWritten, vec![crash])),
            write_delay: Duration::from_millis(150),
            ..CodingConfig::default()
        };
        caught(
            context,
            &reclaiming_and_releasing(config.clone(), None),
            |bugged| {
                *bugged = reclaiming_and_releasing(config, Some(JudgeBug::InProgressReferenced));
            },
            &["which no member's layout names"],
        )
    });
}

/// A run that loses holders in `losses` once every object is coded and
/// repairs them, with orphan reclamation and fragment release, and a quiet
/// second once every stripe is whole.
fn repairing(losses: Vec<Loss>) -> CodingConfig {
    reclaiming_and_releasing(
        CodingConfig {
            repair: Some(RepairConfig {
                losses,
                quiet: Duration::from_secs(1),
                ..RepairConfig::default()
            }),
            ..CodingConfig::default()
        },
        None,
    )
}

/// The repairs of a run, which every repair run reports.
fn repairs(report: &CodingReport) -> Result<&RepairOutcome, String> {
    let repair = report.repair.as_ref().ok_or("no repairs")?;
    if repair.relocated == 0 {
        return Err(format!("nothing was relocated: {repair:?}"));
    }
    tracing::info!(?repair, released = report.released, "repairs");
    Ok(repair)
}

/// Every step of a repair, and whom a crash there strikes. A repair has no
/// new holder until it placed its fragments, and a crash of the new holder
/// at `Started` would wait for the placement and strike as one at `Placed`
/// does, so the new holder is struck from `Placed` on.
fn repair_steps() -> Vec<(RepairPoint, RepairTarget)> {
    let mut steps = Vec::new();
    for point in RepairPoint::ALL {
        steps.push((point, RepairTarget::Primary));
        if point != RepairPoint::Started {
            steps.push((point, RepairTarget::NewHolder));
        }
    }
    steps.push((RepairPoint::Released, RepairTarget::Primary));
    steps.push((RepairPoint::Released, RepairTarget::OldHolder));
    steps
}

/// The kind a crash matrix strikes `target` at `point` with in a seed of
/// `kind`: a power cut at the new holder's next sync is armed as the
/// repair's write to it leaves, which it did before `Written`, so it
/// becomes a power loss from there on.
fn repair_kind(point: RepairPoint, target: RepairTarget, kind: CrashKind) -> CrashKind {
    let written = matches!(
        point,
        RepairPoint::Written | RepairPoint::Appended | RepairPoint::Committed
    );
    match (target, kind) {
        (RepairTarget::NewHolder, CrashKind::PowerAtNextSync) if written => CrashKind::PowerLoss,
        _ => kind,
    }
}

#[test]
fn crashes_of_each_kind_at_each_repair_step_lose_and_leak_nothing() {
    Runner::with_cost(1, MATRIX_COST).run(|context| {
        let kind = kind_of(context);
        for (point, target) in repair_steps() {
            // A lost disk: its node answers again, and notes the release
            // of what it lost.
            let mut config = repairing(vec![Loss::Disk]);
            let crash = RepairCrash {
                point,
                delay: Duration::ZERO,
                target,
                kind: repair_kind(point, target, kind),
                downtime: Duration::from_millis(context.rng().random_range(200..2000)),
            };
            if let Some(repair) = &mut config.repair {
                repair.crash = Some(crash);
            }
            let report =
                coding::run(context, &config).map_err(|error| format!("{crash:?}: {error}"))?;
            if report.strikes.len() != 1 {
                return Err(format!("{crash:?}: struck {:?}", report.strikes).into());
            }
            struck(&report).map_err(|error| format!("{crash:?}: {error}"))?;
            repairs(&report).map_err(|error| format!("{crash:?}: {error}"))?;
            if report.released == 0 {
                return Err(format!("{crash:?}: nothing was released: {report:?}").into());
            }
        }
        Ok(())
    });
}

#[test]
fn keeping_a_crashed_repair_s_fragments_for_good_is_caught() {
    Runner::with_cost(1, 3 * REPAIR_COST).run(|context| {
        // Each rebuilt fragment is durable 300 ms before the repairer
        // learns so, and its node asks about it meanwhile; the primary dies
        // 150 ms after placing the first, which abandons that repair.
        let mut config = repairing(vec![Loss::Disk]);
        if let Some(repair) = &mut config.repair {
            repair.ack_delay = Duration::from_millis(300);
            repair.crash = Some(RepairCrash {
                point: RepairPoint::Placed,
                delay: Duration::from_millis(150),
                target: RepairTarget::Primary,
                kind: CrashKind::Kill,
                downtime: Duration::from_millis(500),
            });
        }
        let bugged = CodingConfig {
            orphans: reclaiming(Duration::ZERO, Some(JudgeBug::InProgressReferenced)),
            ..config.clone()
        };
        caught(
            context,
            &config,
            |config| *config = bugged,
            &["which no member's layout names"],
        )
    });
}

/// A run in which `op` races the first repair to reach `point`.
fn racing(op: RaceOp, point: RepairPoint, loss: Loss) -> CodingConfig {
    CodingConfig {
        race: Some(RaceConfig { op, point }),
        ..repairing(vec![loss])
    }
}

#[test]
fn overwrites_and_deletes_racing_repairs_resurrect_and_lose_nothing() {
    Runner::with_cost(1, 2 * REPAIR_COST).run(|context| {
        let point = *RepairPoint::ALL.choose(context.rng()).expect("points");
        let loss = *[Loss::ForGood, Loss::Disk]
            .choose(context.rng())
            .expect("losses");
        for op in [RaceOp::Overwrite, RaceOp::Delete] {
            let report = coding::run(context, &racing(op, point, loss))
                .map_err(|error| format!("{op:?} at {point:?}, {loss:?}: {error}"))?;
            let race = report.race.as_ref().ok_or("no race")?;
            tracing::info!(?race, "the race passed");
            if race.key.is_none() || !race.written {
                return Err(format!("{op:?} at {point:?}: nothing raced: {race:?}").into());
            }
        }
        Ok(())
    });
}

#[test]
fn releasing_the_fragments_a_relocation_moves_to_is_caught() {
    Runner::with_cost(1, 2 * REPAIR_COST).run(|context| {
        caught(
            context,
            &racing(RaceOp::Overwrite, RepairPoint::Placed, Loss::Disk),
            |_| seed_shard_bug(Some(SeededBug::ReleaseRelocatedTarget)),
            &["which lost it", "readable neither", "started a repair"],
        )
    });
}

/// A run in which one node is drained as another joins, and a holder is
/// lost for good at `point` of the first move, the drained node itself
/// (`OldHolder`) or the copy's new holder.
fn draining(point: MovePoint, target: MoveTarget) -> CodingConfig {
    let mut config = repairing(Vec::new());
    if let Some(repair) = &mut config.repair {
        repair.bound = Duration::from_secs(20);
    }
    config.rebalance = Some(RebalanceConfig {
        joins: 1,
        drains: 1,
        crash: Some(MoveCrash {
            point,
            target,
            kind: CrashKind::Lose,
            downtime: Duration::ZERO,
        }),
        ..RebalanceConfig::default()
    });
    config
}

#[test]
fn a_holder_lost_in_the_middle_of_a_drain_is_repaired_and_moves_settle() {
    Runner::with_cost(1, 2 * REPAIR_COST).run(|context| {
        let point = *MovePoint::ALL.choose(context.rng()).expect("points");
        let target = *[MoveTarget::OldHolder, MoveTarget::NewHolder]
            .choose(context.rng())
            .expect("targets");
        let report = coding::run(context, &draining(point, target))
            .map_err(|error| format!("{target:?} lost at {point:?}: {error}"))?;
        let moves = report.moves.as_ref().ok_or("no moves")?;
        let repair = report.repair.as_ref().ok_or("no repairs")?;
        tracing::info!(?moves, ?repair, "the drain passed");
        // A copy's new holder may be lost before it holds anything, which
        // leaves nothing to repair; the drained node always holds some.
        let repaired = repair.relocated > 0 || target == MoveTarget::NewHolder;
        if moves.moved == 0 || !repaired || moves.drained.len() != 1 {
            return Err(format!("the drain or the repair did nothing: {report:?}").into());
        }
        Ok(())
    });
}

#[test]
fn leaving_a_lost_drained_node_to_its_moves_is_caught() {
    Runner::with_cost(1, 5 * REPAIR_COST).run(|context| {
        // The drained node is lost for good once its first fragment's copy
        // is durable: its other fragments can only be rebuilt.
        caught(
            context,
            &draining(MovePoint::Written, MoveTarget::OldHolder),
            |config| {
                if let Some(repair) = &mut config.repair {
                    repair.bug = Some(RepairBug::LeaveDepartingToMoves);
                }
            },
            &["did not end within"],
        )
    });
}

/// A holder lost for good at 3+2, whose fragments are rebuilt on the
/// survivors; then a lost disk at 4+2, whose node is the only one left to
/// take its stripes' fragments, so they are rebuilt in place.
#[test]
fn lost_holders_heal_with_no_operator_action() {
    Runner::with_cost(1, 2 * REPAIR_COST).run(|context| {
        let mut refilled = repairing(vec![Loss::Disk]);
        refilled.ec = layouts()[2].2.clone();
        for (what, config) in [
            ("lost for good", repairing(vec![Loss::ForGood])),
            ("refilled", refilled),
        ] {
            let report =
                coding::run(context, &config).map_err(|error| format!("{what}: {error}"))?;
            let repair = report.repair.as_ref().ok_or("no repairs")?;
            let healing = &repair.healing;
            tracing::info!(?repair, what, "healed");
            if repair.started == 0 || healing.peak_unrepaired == 0 {
                return Err(format!("{what}: the metrics saw no repair: {repair:?}").into());
            }
            if healing.unrepaired_at_end != 0 || healing.oldest_unrepaired_ms_at_end != 0 {
                return Err(
                    format!("{what}: the unrepaired fragments did not clear: {healing:?}").into(),
                );
            }
        }
        Ok(())
    });
}

#[test]
fn trusting_a_silent_holder_is_caught() {
    Runner::with_cost(1, 4 * REPAIR_COST).run(|context| {
        caught(
            context,
            &repairing(vec![Loss::ForGood]),
            |config| {
                if let Some(repair) = &mut config.repair {
                    repair.bug = Some(RepairBug::TrustSilent);
                }
            },
            &["did not end within"],
        )
    });
}
