//! Coded reads through the gateway (plan M5-06): once every object of the
//! coding cluster is coded and its replicas are dropped, clients read
//! whole objects and ranges through every node's gateway while up to `m`
//! fragment holders crash, lose power, lose their fragment disk for good,
//! or send corrupted fragment bytes, and copy some of them to new keys
//! with CopyObject. Every GET that answers, of an object or of a copy,
//! must return its version's bytes. Seeded bugs show that the checks catch a reader
//! that trusts bytes failing their CRC32C, and one that decodes with the
//! wrong fragment indices. See `skys3_cluster_sim::coding`.

use skys3_cluster_sim::coding::{self, CodingConfig, ReadConfig};
use skys3_ec::read::seeded::ReadBug;
use skys3_sim::{Runner, SimContext};

use super::coding::caught;

/// What one run costs, in seeds of a typical scenario: the encoding of
/// the coding scenarios, then about four seconds of reads under faults.
const RUN_COST: u64 = 32;

/// A run whose readers send `gets` GETs each while `lossy` nodes fail.
fn reading(lossy: usize, corrupt_only: bool, bug: Option<ReadBug>) -> CodingConfig {
    CodingConfig {
        reads: Some(ReadConfig {
            clients: 3,
            gets: 40,
            lossy,
            corrupt_only,
            bug,
        }),
        ..CodingConfig::default()
    }
}

/// Two runs of a seed in one process see the same reads. The run has no
/// crash: a crashed host's sockets close in the order its runtime drops
/// its tasks, which follows task IDs that are global to the process, so
/// the closes, and the latencies drawn after them, differ between two runs
/// of a seed in one process (a run in a fresh process replays exactly).
#[test]
fn a_seed_of_coded_reads_replays_exactly() {
    Runner::with_cost(1, 4 * RUN_COST).run(|context| {
        let config = reading(2, true, None);
        let mut replay = SimContext::with_scale(context.seed(), context.scale());
        let first = coding::run(&mut replay, &config)?;
        let second = coding::run(context, &config)?;
        if format!("{first:?}") != format!("{second:?}") {
            return Err(format!("two runs differ: {first:?} and {second:?}").into());
        }
        Ok(())
    });
}

#[test]
fn degraded_reads_through_the_gateway_return_the_written_bytes() {
    Runner::with_cost(4, RUN_COST).run(|context| {
        let lossy = if context.seed() % 2 == 0 { 2 } else { 1 };
        let report = coding::run(context, &reading(lossy, false, None))?;
        let reads = report.reads.ok_or("no reads")?;
        tracing::debug!(?reads, "the reads passed");
        if reads.served == 0 || reads.parity_reads == 0 {
            return Err(format!("no degraded read was served: {reads:?}").into());
        }
        if reads.copied == 0 {
            return Err(format!("no coded object was copied: {reads:?}").into());
        }
        Ok(())
    });
}

/// Runs `config` clean and then with `bug`, which must be caught.
fn read_bug_caught(context: &mut SimContext, corrupt_only: bool, bug: ReadBug) -> turmoil::Result {
    caught(
        context,
        &reading(2, corrupt_only, None),
        |config| {
            if let Some(reads) = &mut config.reads {
                reads.bug = Some(bug);
            }
        },
        &["not those of version"],
    )
}

#[test]
fn trusting_bytes_that_fail_their_crc_is_caught() {
    Runner::with_cost(4, 2 * RUN_COST)
        .run(|context| read_bug_caught(context, true, ReadBug::TrustCrc));
}

#[test]
fn decoding_with_the_wrong_fragment_indices_is_caught() {
    Runner::with_cost(4, 2 * RUN_COST)
        .run(|context| read_bug_caught(context, false, ReadBug::WrongIndices));
}
