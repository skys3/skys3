//! Repair (plan M5-08): once every object of the coding cluster is coded
//! and its replicas are dropped, one or two fragment holders are lost, for
//! good or by losing their fragment disk, while clients read the objects
//! through every node's gateway. The shard primary's repairer must make
//! every stripe whole again within a bound, with no operator action,
//! starting the stripes that lost two fragments first and keeping to the
//! bandwidth cap, and no stripe may drop below `k` readable fragments
//! when the primary or a rebuilt fragment's new holder crashes during the
//! repairs, or when a repair outlasts `fragment_orphan_after_seconds`.
//! Seeded bugs show that the checks catch a relocation of a fragment that
//! was not durable, a repair the orphan fence does not know, repairs out
//! of order, and repairs past the cap. See `skys3_cluster_sim::coding`.

use std::time::Duration;

use rand::Rng;
use rand::seq::IndexedRandom;
use skys3_cluster_sim::coding::{
    self, CodingConfig, CodingReport, CrashKind, Loss, OrphanConfig, ReadConfig, RepairConfig,
    RepairCrash, RepairOutcome, RepairPoint, RepairTarget,
};
use skys3_config::EcConfig;
use skys3_ec::SeededBug as StoreBug;
use skys3_ec::repair::RepairBug;
use skys3_sim::{Runner, SimContext};

use super::coding::caught;

/// What one run costs, in seeds of a typical scenario: the encoding of
/// the coding scenarios, then a few seconds of repairs.
const RUN_COST: u64 = 16;

/// A run that loses a holder in each of `losses` once every object is
/// coded, with `reads` going on, under the `[ec]` policy `ec`.
fn losing(losses: Vec<Loss>, reads: bool, ec: EcConfig) -> CodingConfig {
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
            ..RepairConfig::default()
        }),
        ec,
        ..CodingConfig::default()
    }
}

/// 2+2 stripes, which six nodes can make whole again after losing two.
fn two_plus_two() -> EcConfig {
    EcConfig {
        min_eligible_nodes: 4,
        max_data_fragments: 2,
        ..EcConfig::default()
    }
}

/// One loss of a kind the seed draws.
fn drawn_loss(context: &mut SimContext) -> Loss {
    *[Loss::ForGood, Loss::Disk]
        .choose(context.rng())
        .expect("losses")
}

/// The repairs of a run, which every repair run reports.
fn repairs(report: &CodingReport) -> Result<&RepairOutcome, String> {
    let repair = report.repair.as_ref().ok_or("no repairs")?;
    if repair.relocated == 0 {
        return Err(format!("nothing was relocated: {repair:?}"));
    }
    tracing::info!(?repair, "repairs");
    Ok(repair)
}

/// Three of the four objects are retagged before the loss, which moves
/// their entries' versions past their coded layouts': checks, reads, and
/// rebuilt fragments must name the version the fragments were written for.
#[test]
fn a_lost_holder_is_repaired_while_reads_go_on() {
    Runner::with_cost(4, 2 * RUN_COST).run(|context| {
        let loss = drawn_loss(context);
        let mut config = losing(vec![loss], true, EcConfig::default());
        if let Some(repair) = &mut config.repair {
            repair.retag = 3;
        }
        let report = coding::run(context, &config).map_err(|error| format!("{loss:?}: {error}"))?;
        let repair = repairs(&report)?;
        if repair.retagged != 3 || repair.retagged_relocated == 0 {
            return Err(format!("no retagged object was repaired: {repair:?}").into());
        }
        let reads = report.reads.as_ref().ok_or("no reads")?;
        if reads.served == 0 {
            return Err(format!("no read was served: {reads:?}").into());
        }
        Ok(())
    });
}

/// Two holders lost for good fall silent together, and their losses are
/// found in one pass, which must start the stripes that lost both first.
/// A lost disk's losses are found once its node answers again, which may
/// be passes apart from the other's.
#[test]
fn stripes_missing_two_fragments_are_repaired_first() {
    Runner::with_cost(4, 2 * RUN_COST).run(|context| {
        let losses = vec![Loss::ForGood, drawn_loss(context)];
        let report = coding::run(context, &losing(losses.clone(), true, two_plus_two()))
            .map_err(|error| format!("{losses:?}: {error}"))?;
        let repair = repairs(&report)?;
        if losses == [Loss::ForGood; 2] && repair.most_lost < 2 {
            return Err(format!("no stripe lost two fragments: {repair:?}").into());
        }
        Ok(())
    });
}

#[test]
fn crashes_during_repairs_keep_k_fragments_readable() {
    Runner::with_cost(4, RUN_COST).run(|context| {
        let point = *RepairPoint::ALL.choose(context.rng()).expect("points");
        let target = *[RepairTarget::Primary, RepairTarget::NewHolder]
            .choose(context.rng())
            .expect("targets");
        let kind = *[
            CrashKind::Kill,
            CrashKind::PowerLoss,
            CrashKind::PowerAtNextSync,
        ]
        .choose(context.rng())
        .expect("kinds");
        let crash = RepairCrash {
            point,
            target,
            kind,
            downtime: Duration::from_millis(context.rng().random_range(200..2000)),
        };
        let mut config = losing(vec![drawn_loss(context)], false, EcConfig::default());
        if let Some(repair) = &mut config.repair {
            repair.crash = Some(crash);
        }
        let report =
            coding::run(context, &config).map_err(|error| format!("{crash:?}: {error}"))?;
        repairs(&report)?;
        Ok(())
    });
}

/// Repairs that outlast `orphan_after`: each rebuilt fragment is durable
/// on its node for 300 ms before the repairer learns so, with orphan
/// reclamation asking about fragments 100 ms old on every node.
fn outlasting(bug: Option<RepairBug>) -> CodingConfig {
    let mut config = losing(vec![Loss::Disk], false, EcConfig::default());
    config.orphans = Some(OrphanConfig {
        orphan_after: Duration::from_millis(100),
        sweep: Duration::from_millis(20),
        bug: None,
    });
    if let Some(repair) = &mut config.repair {
        repair.bug = bug;
        repair.ack_delay = Duration::from_millis(300);
        repair.bound = Duration::from_secs(30);
    }
    config
}

#[test]
fn repairs_outlasting_orphan_after_keep_their_fragments() {
    Runner::with_cost(4, 2 * RUN_COST).run(|context| {
        let report = coding::run(context, &outlasting(None))?;
        repairs(&report)?;
        Ok(())
    });
}

#[test]
fn a_repair_unknown_to_the_orphan_fence_is_caught() {
    Runner::with_cost(2, 4 * RUN_COST).run(|context| {
        caught(
            context,
            &outlasting(None),
            |config| *config = outlasting(Some(RepairBug::Unfenced)),
            &["which was reclaimed", "which lost it"],
        )
    });
}

#[test]
fn relocating_a_fragment_before_it_is_durable_is_caught() {
    Runner::with_cost(2, 2 * RUN_COST).run(|context| {
        // Each rebuilt fragment's new holder loses power at its next sync:
        // the one that would make the fragment durable.
        let mut config = losing(vec![drawn_loss(context)], false, EcConfig::default());
        if let Some(repair) = &mut config.repair {
            repair.crash = Some(RepairCrash {
                point: RepairPoint::Placed,
                target: RepairTarget::NewHolder,
                kind: CrashKind::PowerAtNextSync,
                downtime: Duration::from_millis(500),
            });
        }
        caught(
            context,
            &config,
            |config| config.store_bug = Some(StoreBug::AcknowledgeBeforeSync),
            &["which lost it", "readable neither", "does not decode"],
        )
    });
}

#[test]
fn repairs_out_of_order_or_past_the_cap_are_caught() {
    Runner::with_cost(2, 4 * RUN_COST).run(|context| {
        let seeded = |bug| {
            move |config: &mut CodingConfig| {
                if let Some(repair) = &mut config.repair {
                    repair.bug = Some(bug);
                }
            }
        };
        let silent = losing(vec![Loss::ForGood, Loss::ForGood], false, two_plus_two());
        caught(
            context,
            &silent,
            seeded(RepairBug::LeastLostFirst),
            &["fragments, before"],
        )?;
        let disks = losing(vec![Loss::Disk, Loss::Disk], false, two_plus_two());
        caught(
            context,
            &disks,
            seeded(RepairBug::Unthrottled),
            &["past the bandwidth cap"],
        )
    });
}
