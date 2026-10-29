//! Simulation scenarios for the disk and clock abstractions.
//!
//! A node appends checksummed records to a log on a simulated disk and
//! acknowledges them once a sync covers them. The driver crashes the node and
//! its disk at random steps and restarts it. After every restart, the node's
//! recovery must find every acknowledged record: synced data survives the
//! crash, while unsynced records and torn tails are cut away.

use std::cell::Cell;
use std::error::Error;
use std::io::{self, ErrorKind};
use std::rc::Rc;
use std::time::Duration;

use bytes::Bytes;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use skys3_io::disk::SimFile;
use skys3_io::{Clock, Disk, Drift, MonotonicClock, SegmentFile, SimDisk, SimDiskFaults, SimMount};
use skys3_sim::{NodeClock, Runner, SeedSet, SimContext};

const LOG: &str = "log";
const RECORD_LEN: usize = 16;
const CHECK: u64 = 0x5eed_5eed_5eed_5eed;

/// The design's default drift bound `ρ`, 1%.
const RHO: Drift = match Drift::from_ppm(10_000) {
    Some(drift) => drift,
    None => panic!("valid drift"),
};

/// Sync errors and torn writes, as rare and as common as they need to be to
/// show up in most seeds.
const FAULTS: SimDiskFaults = SimDiskFaults {
    sync_error_probability: 0.01,
    torn_write_probability: 0.5,
    capacity: None,
};

fn record(seq: u64) -> Bytes {
    let mut record = Vec::with_capacity(RECORD_LEN);
    record.extend_from_slice(&seq.to_le_bytes());
    record.extend_from_slice(&(seq ^ CHECK).to_le_bytes());
    Bytes::from(record)
}

fn parse(record: &[u8; RECORD_LEN]) -> Option<u64> {
    let (seq, check) = record.split_at(8);
    let seq = u64::from_le_bytes(seq.try_into().ok()?);
    let check = u64::from_le_bytes(check.try_into().ok()?);
    (check == seq ^ CHECK).then_some(seq)
}

/// State that outlives node crashes: what the node acknowledged, which is
/// what its clients were told.
#[derive(Default)]
struct Observed {
    acknowledged: Cell<u64>,
    restarts: Cell<u64>,
}

/// Opens the log, or creates it, and counts the valid records at its start.
async fn open_log(mount: &SimMount) -> io::Result<(SimFile, u64)> {
    let log = match mount.open(LOG).await {
        Ok(log) => log,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            let log = mount.create(LOG).await?;
            mount.sync_dir().await?;
            log
        }
        Err(error) => return Err(error),
    };
    let contents = log.read_at(0, log.len() as usize).await?;
    let mut records = 0;
    for chunk in contents.as_chunks::<RECORD_LEN>().0 {
        if parse(chunk) != Some(records) {
            break;
        }
        records += 1;
    }
    Ok((log, records))
}

/// Cuts the log back to its valid records, durably.
async fn cut_torn_tail(log: &SimFile, records: u64) -> io::Result<()> {
    let valid = records * RECORD_LEN as u64;
    if log.len() > valid {
        log.truncate(valid).await?;
        log.sync_data().await?;
    }
    Ok(())
}

/// Appends records and acknowledges each batch once a sync covers it.
async fn append_forever(
    log: &SimFile,
    mut records: u64,
    clock: &MonotonicClock,
    rng: &mut SmallRng,
    observed: &Observed,
) -> io::Result<()> {
    loop {
        for _ in 0..rng.random_range(1..=4) {
            log.append(record(records)).await?;
            records += 1;
        }
        log.sync_data().await?;
        observed.acknowledged.set(records);
        clock
            .sleep(Duration::from_millis(rng.random_range(1..=3)))
            .await;
    }
}

/// The node: recovers, checks that no acknowledged record was lost, then
/// appends and syncs until it crashes. After an I/O error, such as a failed
/// sync, it acknowledges nothing more: the log takes the disk out of service
/// (design §10.4) until the node restarts.
async fn node(
    disk: SimDisk,
    clock: NodeClock,
    seed: u64,
    observed: Rc<Observed>,
) -> Result<(), Box<dyn Error>> {
    let restarts = observed.restarts.get();
    observed.restarts.set(restarts + 1);
    let mut rng = SmallRng::seed_from_u64(seed ^ restarts);
    let clock = clock.start();
    let mount = disk.mount();

    let Ok((log, records)) = open_log(&mount).await else {
        return out_of_service().await;
    };
    let acknowledged = observed.acknowledged.get();
    if records < acknowledged {
        return Err(
            format!("recovered {records} records, but {acknowledged} were acknowledged").into(),
        );
    }
    if cut_torn_tail(&log, records).await.is_ok() {
        // Returns only on an I/O error.
        let _ = append_forever(&log, records, &clock, &mut rng, &observed).await;
    }
    out_of_service().await
}

async fn out_of_service() -> Result<(), Box<dyn Error>> {
    std::future::pending().await
}

/// Crashes the node's host and its disk together, then restarts the host.
fn crash_and_restart(sim: &mut turmoil::Sim<'_>, disk: &SimDisk) {
    sim.crash("node");
    disk.crash();
    sim.bounce("node");
}

/// Runs the crash-and-restart scenario and returns how many records were
/// acknowledged and how often the node started.
fn crash_restart(
    context: &mut SimContext,
    faults: SimDiskFaults,
) -> Result<(u64, u64), Box<dyn Error>> {
    let disk = context.disk_with_faults(faults);
    let clock = context.node_clock(RHO);
    let node_seed = context.fork_seed();
    let observed = Rc::new(Observed::default());
    let mut sim = context.builder().build();

    let host_disk = disk.clone();
    let host_observed = Rc::clone(&observed);
    sim.host("node", move || {
        node(
            host_disk.clone(),
            clock,
            node_seed,
            Rc::clone(&host_observed),
        )
    });

    for _ in 0..2_000 {
        sim.step()?;
        if context.rng().random_bool(0.01) {
            crash_and_restart(&mut sim, &disk);
        }
    }
    // A final crash and restart checks the last acknowledgements too.
    crash_and_restart(&mut sim, &disk);
    sim.step()?;

    if observed.acknowledged.get() == 0 {
        return Err("the node acknowledged nothing".into());
    }
    Ok((observed.acknowledged.get(), observed.restarts.get()))
}

#[test]
fn crashes_lose_no_synced_record() {
    Runner::new().run(|context| crash_restart(context, SimDiskFaults::default()).map(drop));
}

#[test]
fn crashes_with_torn_writes_and_sync_errors_lose_no_acknowledged_record() {
    Runner::new().run(|context| crash_restart(context, FAULTS).map(drop));
}

#[test]
fn a_seed_replays_exactly() {
    let run = |seed| crash_restart(&mut SimContext::new(seed), FAULTS).unwrap();
    let first = run(1);
    assert_eq!(run(1), first);
    assert!(first.1 > 1, "the node never restarted");
    assert!((2..6).any(|seed| run(seed) != first));
}

#[test]
#[should_panic(
    expected = "replay with: SKYS3_SIM_SEED=4 cargo test -p skys3-sim --test simulation"
)]
fn a_node_that_acknowledges_unsynced_records_fails_with_its_seed() {
    Runner::with_seeds(SeedSet::One(4)).run(|context| {
        let disk = context.disk();
        let mut sim = context.builder().build();
        let observed = Rc::new(Observed::default());
        let host_disk = disk.clone();
        let host_observed = Rc::clone(&observed);
        sim.host("node", move || {
            let mount = host_disk.mount();
            let observed = Rc::clone(&host_observed);
            async move {
                let (log, records) = open_log(&mount).await?;
                if records < observed.acknowledged.get() {
                    return Err("lost an acknowledged record".into());
                }
                // Acknowledges without a sync.
                log.append(record(records)).await?;
                observed.acknowledged.set(records + 1);
                out_of_service().await
            }
        });
        sim.step()?;
        crash_and_restart(&mut sim, &disk);
        sim.step()?;
        Ok(())
    });
}

#[test]
fn drifting_clocks_stay_within_the_bound() {
    Runner::new().run(|context| {
        let tick = Duration::from_millis(5);
        let mut builder = context.builder();
        builder
            .tick_duration(tick)
            .simulation_duration(Duration::from_secs(20));
        let mut sim = builder.build();
        for name in ["a", "b", "c"] {
            let node = context.node_clock(RHO);
            sim.client(name, async move {
                let clock = node.start();
                let real = tokio::time::Instant::now();
                clock.sleep(Duration::from_secs(10)).await;
                let local = clock.now() - node.start;
                let real = real.elapsed();
                // Ten local seconds take 10 / (1 + d) real seconds, give or
                // take a simulation tick. A 1% drift is 100 ms.
                let expected = 10.0 / (1.0 + f64::from(node.drift.ppm()) / 1e6);
                let slack = 2 * tick;
                if local < Duration::from_secs(10)
                    || local > Duration::from_secs(10) + slack
                    || (real.as_secs_f64() - expected).abs() > slack.as_secs_f64()
                {
                    return Err(format!("drift {}: {local:?} local in {real:?}", node.drift).into());
                }
                Ok(())
            });
        }
        sim.run()
    });
}
