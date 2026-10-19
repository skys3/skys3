//! Fragment moves (plan M5-09): once every object of the coding cluster is
//! coded, nodes join or are drained while holders are lost or lose their
//! fragment disk and clients read the objects through the gateways. The
//! shard primary's repairer must repair first, then move fragments onto
//! the new nodes and off the drained ones, each move publishing its copy
//! before the old fragment is retired, so that every stripe keeps `k`
//! readable fragments throughout, no failure domain ever holds more than
//! `m` of a stripe, and every stripe is whole and the nodes balanced
//! within a bound, through crashes of the primary and of a moved
//! fragment's old and new holders at each step of a move, and when a move
//! outlasts `fragment_orphan_after_seconds`. Seeded bugs show that the
//! checks catch a move the orphan fence does not know, an old fragment
//! retired before its move commits, a copy relocated before it is
//! durable, moves ahead of repairs, and moves planned at the wrong
//! failure-domain level. See `skys3_cluster_sim::coding`.

use std::time::Duration;

use rand::Rng;
use rand::seq::IndexedRandom;
use skys3_cluster_sim::coding::{
    self, CodingConfig, CodingReport, CrashKind, Loss, MoveCrash, MoveOutcome, MovePoint,
    MoveTarget, OrphanConfig, ReadConfig, RebalanceBug, RebalanceConfig, RepairConfig,
};
use skys3_ec::SeededBug as StoreBug;
use skys3_ec::repair::RepairBug;
use skys3_sim::{Runner, SimContext};

use super::coding::caught;

/// What one run costs, in seeds of a typical scenario: the encoding of the
/// coding scenarios, then a few seconds of repairs and moves, about two
/// seconds in a debug build. Runs with reads take about five, and a
/// scenario that runs its seed with and without a bug runs twice or more,
/// so those declare more; the joins and drains with reads declare more
/// still, to keep CI's seed set to a few runs of each.
const RUN_COST: u64 = 16;

/// A run in which `rebalance` joins and drains nodes as the holders in
/// `losses` are lost, with reads going on if `reads`.
fn rebalancing(rebalance: RebalanceConfig, losses: Vec<Loss>, reads: bool) -> CodingConfig {
    CodingConfig {
        reads: reads.then_some(ReadConfig {
            clients: 2,
            gets: 30,
            lossy: 0,
            corrupt_only: false,
            bug: None,
        }),
        repair: Some(RepairConfig {
            losses,
            bound: Duration::from_secs(20),
            ..RepairConfig::default()
        }),
        rebalance: Some(rebalance),
        ..CodingConfig::default()
    }
}

/// No loss, a holder lost for good, or a lost fragment disk, as the seed
/// draws.
fn drawn_losses(context: &mut SimContext) -> Vec<Loss> {
    [vec![], vec![Loss::ForGood], vec![Loss::Disk]]
        .choose(context.rng())
        .expect("losses")
        .clone()
}

/// The moves of a run, which every rebalancing run reports.
fn moved(report: &CodingReport) -> Result<&MoveOutcome, String> {
    let moves = report.moves.as_ref().ok_or("no moves")?;
    if moves.moved == 0 {
        return Err(format!("nothing was moved: {moves:?}"));
    }
    tracing::info!(?moves, repair = ?report.repair, "moves");
    Ok(moves)
}

#[test]
fn joined_nodes_receive_fragments_while_reads_and_losses_go_on() {
    Runner::with_cost(4, 4 * RUN_COST).run(|context| {
        let losses = drawn_losses(context);
        let rebalance = RebalanceConfig {
            joins: 2,
            ..RebalanceConfig::default()
        };
        let config = rebalancing(rebalance, losses.clone(), true);
        let report = coding::run(context, &config).map_err(|e| format!("{losses:?}: {e}"))?;
        let moves = moved(&report)?;
        if moves.joined.len() != 2 {
            return Err(format!("the nodes did not join: {moves:?}").into());
        }
        let reads = report.reads.as_ref().ok_or("no reads")?;
        if reads.served == 0 {
            return Err(format!("no read was served: {reads:?}").into());
        }
        Ok(())
    });
}

/// A node is drained and another joins, as when a node is replaced: with
/// a holder lost for good besides, the five eligible nodes left still hold
/// a 3+2 stripe.
#[test]
fn a_drained_node_is_emptied_while_reads_and_losses_go_on() {
    Runner::with_cost(4, 4 * RUN_COST).run(|context| {
        let losses = drawn_losses(context);
        let rebalance = RebalanceConfig {
            joins: 1,
            drains: 1,
            ..RebalanceConfig::default()
        };
        let config = rebalancing(rebalance, losses.clone(), true);
        let report = coding::run(context, &config).map_err(|e| format!("{losses:?}: {e}"))?;
        let moves = moved(&report)?;
        if moves.drained.len() != 1 {
            return Err(format!("no node was drained: {moves:?}").into());
        }
        Ok(())
    });
}

/// Six nodes in three racks hold 3+2 stripes, two fragments at most per
/// rack; one node joins the first rack and another a new one.
#[test]
fn moves_keep_each_rack_within_its_cap() {
    Runner::with_cost(4, 2 * RUN_COST).run(|context| {
        let losses = if context.rng().random_bool(0.5) {
            vec![Loss::Disk]
        } else {
            Vec::new()
        };
        let rebalance = RebalanceConfig {
            joins: 2,
            racks: true,
            ..RebalanceConfig::default()
        };
        let report = coding::run(context, &rebalancing(rebalance, losses.clone(), false))
            .map_err(|e| format!("{losses:?}: {e}"))?;
        moved(&report)?;
        Ok(())
    });
}

#[test]
fn crashes_during_moves_keep_k_fragments_readable() {
    Runner::with_cost(4, RUN_COST).run(|context| {
        let point = *MovePoint::ALL.choose(context.rng()).expect("points");
        let target = *[
            MoveTarget::Primary,
            MoveTarget::OldHolder,
            MoveTarget::NewHolder,
        ]
        .choose(context.rng())
        .expect("targets");
        let kind = *[
            CrashKind::Kill,
            CrashKind::PowerLoss,
            CrashKind::PowerAtNextSync,
        ]
        .choose(context.rng())
        .expect("kinds");
        let crash = MoveCrash {
            point,
            target,
            kind,
            downtime: Duration::from_millis(context.rng().random_range(200..2000)),
        };
        let rebalance = RebalanceConfig {
            joins: 2,
            drains: 1,
            crash: Some(crash),
            ..RebalanceConfig::default()
        };
        let report = coding::run(context, &rebalancing(rebalance, Vec::new(), false))
            .map_err(|e| format!("{crash:?}: {e}"))?;
        moved(&report)?;
        Ok(())
    });
}

/// Moves that outlast `orphan_after`: each copy is durable on its node for
/// 300 ms before the mover learns so, with orphan reclamation asking about
/// fragments 100 ms old on every node.
fn outlasting(bug: Option<RepairBug>) -> CodingConfig {
    let rebalance = RebalanceConfig {
        joins: 1,
        drains: 1,
        ..RebalanceConfig::default()
    };
    let mut config = rebalancing(rebalance, Vec::new(), false);
    config.orphans = Some(OrphanConfig {
        orphan_after: Duration::from_millis(100),
        sweep: Duration::from_millis(20),
        bug: None,
    });
    if let Some(repair) = &mut config.repair {
        repair.bug = bug;
        repair.ack_delay = Duration::from_millis(300);
        repair.bound = Duration::from_secs(40);
    }
    config
}

#[test]
fn moves_outlasting_orphan_after_keep_their_fragments() {
    Runner::with_cost(4, 2 * RUN_COST).run(|context| {
        let report = coding::run(context, &outlasting(None))?;
        moved(&report)?;
        Ok(())
    });
}

#[test]
fn a_move_unknown_to_the_orphan_fence_is_caught() {
    Runner::with_cost(2, 4 * RUN_COST).run(|context| {
        caught(
            context,
            &outlasting(None),
            |config| *config = outlasting(Some(RepairBug::UnfencedMove)),
            &["which was reclaimed", "which lost it"],
        )
    });
}

#[test]
fn retiring_a_fragment_before_its_move_commits_is_caught() {
    Runner::with_cost(2, 4 * RUN_COST).run(|context| {
        // The primary dies once a copy is placed: that move never commits.
        let rebalance = RebalanceConfig {
            joins: 2,
            crash: Some(MoveCrash {
                point: MovePoint::Placed,
                target: MoveTarget::Primary,
                kind: CrashKind::Kill,
                downtime: Duration::from_millis(500),
            }),
            ..RebalanceConfig::default()
        };
        let config = rebalancing(rebalance, Vec::new(), false);
        caught(
            context,
            &config,
            |config| {
                if let Some(rebalance) = &mut config.rebalance {
                    rebalance.bug = Some(RebalanceBug::RetireEarly);
                }
            },
            &["which was reclaimed"],
        )
    });
}

#[test]
fn relocating_a_copy_before_it_is_durable_is_caught() {
    Runner::with_cost(2, 4 * RUN_COST).run(|context| {
        // The first copy's new holder loses power at its next sync: the
        // one that would make the copy durable.
        let rebalance = RebalanceConfig {
            joins: 2,
            crash: Some(MoveCrash {
                point: MovePoint::Started,
                target: MoveTarget::NewHolder,
                kind: CrashKind::PowerAtNextSync,
                downtime: Duration::from_millis(500),
            }),
            ..RebalanceConfig::default()
        };
        let config = rebalancing(rebalance, Vec::new(), false);
        caught(
            context,
            &config,
            |config| config.store_bug = Some(StoreBug::AcknowledgeBeforeSync),
            &["which lost it", "readable neither", "does not decode"],
        )
    });
}

#[test]
fn moves_ahead_of_repairs_or_across_racks_are_caught() {
    Runner::with_cost(2, 8 * RUN_COST).run(|context| {
        // A disk is lost as two nodes join: the passes that find its
        // fragments lost must repair them before any move.
        let rebalance = RebalanceConfig {
            joins: 2,
            ..RebalanceConfig::default()
        };
        let config = rebalancing(rebalance, vec![Loss::Disk], false);
        caught(
            context,
            &config,
            |config| {
                if let Some(repair) = &mut config.repair {
                    repair.bug = Some(RepairBug::MovesBeforeRepairs);
                }
            },
            &["repairs come first"],
        )?;
        let rebalance = RebalanceConfig {
            joins: 2,
            racks: true,
            ..RebalanceConfig::default()
        };
        caught(
            context,
            &rebalancing(rebalance, Vec::new(), false),
            |config| {
                if let Some(rebalance) = &mut config.rebalance {
                    rebalance.bug = Some(RebalanceBug::NodeLevel);
                }
            },
            &["over its cap"],
        )
    });
}
