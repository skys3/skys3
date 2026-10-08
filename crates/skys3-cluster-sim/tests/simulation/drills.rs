//! Restore drills of a shard whose members are all lost (plan M5-11):
//! while coded and replicated objects are written, retagged, overwritten,
//! deleted, and repaired, the drill assumes the shard's member lost, with
//! up to two other nodes, at random moments, and restores the shard from
//! its latest index snapshot and the fragment headers of the surviving
//! nodes. Every restored object must read back with the bytes and tags
//! the history gives it, coded objects written after the snapshot
//! included, and the lost-key report must match the history. Seeded bugs
//! of re-indexing are caught. See `skys3_cluster_sim::drill`.

use std::time::Duration;

use skys3_cluster_sim::drill::{self, DrillConfig, DrillReport};
use skys3_ec::reindex::seeded::ReindexBug;
use skys3_sim::{Runner, SimContext};

/// What one run costs, in seeds of a typical scenario.
const RUN_COST: u64 = 4;

#[test]
fn restore_drills_match_the_history() {
    let mut total = DrillReport::default();
    Runner::with_cost(8, RUN_COST).run(|context| {
        let report = drill::run(context, &DrillConfig::default())?;
        tracing::info!(?report, "drills");
        if report.drills == 0 || report.restored == 0 {
            return Err(format!("nothing was restored: {report:?}").into());
        }
        add(&mut total, &report);
        Ok(())
    });
    // Over the seeds, every kind of outcome the drill reports occurs.
    let DrillReport {
        written_after,
        unrecoverable,
        lost,
        repaired,
        reclaimed,
        with_snapshot,
        retags_from_headers,
        ..
    } = total;
    for (what, count) in [
        ("restored objects written after the snapshot", written_after),
        ("unrecoverable coded objects", unrecoverable),
        ("replicated objects reported lost", lost),
        ("repairs", repaired),
        ("reclaimed orphans", reclaimed),
        ("drills from a snapshot", with_snapshot),
        ("tags restored from a later header", retags_from_headers),
    ] {
        assert!(count > 0, "no {what}: {total:?}");
    }
}

fn add(total: &mut DrillReport, report: &DrillReport) {
    total.drills += report.drills;
    total.with_snapshot += report.with_snapshot;
    total.restored += report.restored;
    total.written_after += report.written_after;
    total.retags_from_headers += report.retags_from_headers;
    total.unrecoverable += report.unrecoverable;
    total.lost += report.lost;
    total.repaired += report.repaired;
    total.reclaimed += report.reclaimed;
}

/// A run for a seeded bug: `losses` losses, each a chance to catch it.
fn hunting(losses: usize) -> DrillConfig {
    DrillConfig {
        losses,
        ..DrillConfig::default()
    }
}

/// Runs the seed without `bug`, which must pass, then with it, which must
/// fail with one of `caught`.
fn caught(
    context: &mut SimContext,
    config: &DrillConfig,
    bug: ReindexBug,
    caught: &[&str],
) -> turmoil::Result {
    let mut replay = SimContext::with_scale(context.seed(), context.scale());
    drill::run(&mut replay, config).map_err(|error| format!("without the bug: {error}"))?;
    let bugged = DrillConfig {
        bug: Some(bug),
        ..config.clone()
    };
    match drill::run(context, &bugged) {
        Err(error) if caught.iter().any(|reason| error.contains(reason)) => Ok(()),
        Err(error) => Err(format!("{bug:?} was caught for another reason: {error}").into()),
        Ok(report) => Err(format!("{bug:?} went unnoticed: {report:?}").into()),
    }
}

#[test]
fn preferring_an_older_attempt_s_headers_is_caught() {
    Runner::with_cost(4, RUN_COST).run(|context| {
        caught(
            context,
            &hunting(12),
            ReindexBug::OlderAttempt,
            &["is restored with the tags"],
        )
    });
}

#[test]
fn preferring_the_headers_tags_to_a_later_snapshot_is_caught() {
    Runner::with_cost(4, RUN_COST).run(|context| {
        caught(
            context,
            &hunting(8),
            ReindexBug::HeaderTags,
            &["is restored with the tags"],
        )
    });
}

#[test]
fn ignoring_writes_after_the_snapshot_is_caught() {
    Runner::with_cost(4, RUN_COST).run(|context| {
        caught(
            context,
            &hunting(8),
            ReindexBug::PrefersSnapshot,
            &["is not restored", "written after the snapshot"],
        )
    });
}

#[test]
fn restoring_a_key_deleted_before_the_snapshot_is_caught() {
    Runner::with_cost(4, RUN_COST).run(|context| {
        caught(
            context,
            // Orphans are never reclaimed, so a deleted version's fragments
            // stay.
            &DrillConfig {
                orphan_after: Duration::from_secs(600),
                ..hunting(8)
            },
            ReindexBug::Resurrects,
            &["was deleted before the snapshot", "before the snapshot"],
        )
    });
}

#[test]
fn falling_back_to_an_older_version_is_caught() {
    Runner::with_cost(4, RUN_COST).run(|context| {
        caught(
            context,
            // Each loss takes enough holders of the latest version that it
            // cannot be rebuilt, and as few of the previous one's as it can.
            &DrillConfig {
                lose_latest: true,
                ..hunting(12)
            },
            ReindexBug::FallsBack,
            &[
                "written after the snapshot",
                "before the snapshot",
                "must be reported lost",
            ],
        )
    });
}
