//! Simulation scenarios for the segment log, run by CI's simulation job
//! with a larger seed set.
//!
//! - Crash points: for each seed's workload, cut the power or kill the
//!   process at every write and sync boundary, and fail every sync in turn,
//!   then recover. No acknowledged record may be lost, and none may be
//!   acknowledged unless a successful sync covered it after any failed sync
//!   of its file.
//! - A node under `turmoil` that appends from several tasks while the
//!   driver cuts its power or kills its process at random, with sync errors
//!   and torn writes injected. Every restart must find every acknowledged
//!   record.

mod harness;

use std::error::Error;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use harness::{
    Audit, AuditedMount, Outcome, check_recovered, clock, delete, extent, inline_put, run_writers,
    runtime, small_config,
};
use rand::Rng;
use skys3_io::{SimDisk, SimDiskFaults, SimMount};
use skys3_log::{LogConfig, LogError, LogRecord, RecordLocation, RecoveryError, SegmentLog};
use skys3_sim::{Runner, SimContext};

/// Torn writes on half of the crashes, so recovery meets both clean and
/// torn tails.
const TORN: SimDiskFaults = SimDiskFaults {
    sync_error_probability: 0.0,
    torn_write_probability: 0.5,
    capacity: None,
};

/// A seed's workload: how many appenders, how many records each, and how
/// long a group commit waits.
#[derive(Clone, Copy, Debug)]
struct Workload {
    disk_seed: u64,
    writers: u8,
    records: u64,
    delay: Duration,
}

impl Workload {
    fn draw(context: &mut SimContext) -> Self {
        let delays = [0, 200, 2000];
        Self {
            disk_seed: context.fork_seed(),
            writers: context.rng().random_range(1..=4),
            records: context.rng().random_range(3..=8),
            delay: Duration::from_micros(delays[context.rng().random_range(0..delays.len())]),
        }
    }

    fn config(&self) -> LogConfig {
        LogConfig {
            group_commit_max_delay: self.delay,
            ..small_config()
        }
    }
}

/// What one run with planned faults saw.
#[derive(Debug)]
struct Run {
    ops: u64,
    syncs: u64,
}

/// Runs `writers` of `workload` against a log on `mount`, checks that
/// nothing was acknowledged unless a sync covered it, and returns what the
/// appenders were told.
async fn run_audited(workload: Workload, mount: &AuditedMount, writers: Range<u8>) -> Outcome {
    let Ok((log, _)) = harness::open(mount.clone(), workload.config()).await else {
        // The plan cut recovery short.
        return Outcome::default();
    };
    let outcome = run_writers(&log, writers, workload.records).await;
    mount.check_acknowledged(&outcome.acknowledged);
    let failed_sync = mount.audit().sync_failed;
    if failed_sync {
        // Nothing is acknowledged after a failed sync, ever.
        assert!(!log.is_in_service());
        let error = log.append(&delete(0, 1)).await.unwrap_err();
        assert!(matches!(error, LogError::OutOfService(_)), "{error}");
    }
    outcome
}

/// Recovers the log on `disk` after a power loss, and checks that every
/// acknowledged record survived and that the recovered log works.
async fn recover_and_check(workload: Workload, disk: &SimDisk, outcome: &Outcome) {
    let mount = AuditedMount::new(disk, Audit::default());
    let (log, _) = harness::open(mount.clone(), workload.config())
        .await
        .expect("recovery after a crash succeeds");
    check_recovered(&log, outcome).await;
    let more = [delete(70, 1), extent(70, 2, 900), inline_put(70, 3, 200)];
    let mut acknowledged = Vec::new();
    for record in more {
        let location = log.append(&record).await.unwrap();
        assert_eq!(log.read(location).await.unwrap(), record);
        acknowledged.push((record, location));
    }
    mount.check_acknowledged(&acknowledged);
}

/// Runs `workload` on a fresh disk with the faults `plan` schedules, then
/// cuts the power and recovers.
fn run_with_plan(workload: Workload, plan: Audit) -> Run {
    runtime().block_on(async move {
        let disk = SimDisk::with_faults(workload.disk_seed, TORN);
        let mount = AuditedMount::new(&disk, plan);
        let outcome = run_audited(workload, &mount, 0..workload.writers).await;
        let run = {
            let audit = mount.audit();
            Run {
                ops: audit.ops,
                syncs: audit.syncs,
            }
        };
        disk.crash();
        recover_and_check(workload, &disk, &outcome).await;
        run
    })
}

/// Runs `workload` on a fresh disk and kills the process before operation
/// `kill_at`, so the page cache survives. A restarted process recovers,
/// checks, and appends more; then the power fails, and every record either
/// process acknowledged must survive.
fn run_with_kill(workload: Workload, kill_at: u64) {
    runtime().block_on(async move {
        let disk = SimDisk::with_faults(workload.disk_seed, TORN);
        let plan = Audit {
            kill_at: Some(kill_at),
            ..Audit::default()
        };
        let first = AuditedMount::new(&disk, plan);
        let mut outcome = run_audited(workload, &first, 0..workload.writers).await;

        let second = AuditedMount::new(&disk, Audit::default());
        outcome.extend(run_audited(workload, &second, 10..10 + workload.writers).await);
        let (log, _) = harness::open(second.clone(), workload.config())
            .await
            .expect("recovery after a process crash succeeds");
        check_recovered(&log, &outcome).await;
        drop(log);

        disk.crash();
        recover_and_check(workload, &disk, &outcome).await;
    });
}

#[test]
fn power_losses_at_every_write_and_sync_boundary_lose_no_acknowledged_record() {
    Runner::new().run(|context| {
        let workload = Workload::draw(context);
        let total = run_with_plan(workload, Audit::default()).ops;
        assert!(total > 4, "the workload did too little: {total} operations");
        for crash_at in 0..=total {
            let plan = Audit {
                crash_at: Some(crash_at),
                ..Audit::default()
            };
            run_with_plan(workload, plan);
        }
        Ok(())
    })
}

#[test]
fn process_crashes_at_every_write_and_sync_boundary_lose_no_acknowledged_record() {
    Runner::new().run(|context| {
        let workload = Workload::draw(context);
        let total = run_with_plan(workload, Audit::default()).ops;
        for kill_at in 0..=total {
            run_with_kill(workload, kill_at);
        }
        Ok(())
    })
}

#[test]
fn no_acknowledgement_follows_a_failed_sync() {
    Runner::new().run(|context| {
        let workload = Workload::draw(context);
        let syncs = run_with_plan(workload, Audit::default()).syncs;
        for fail_sync_at in 0..syncs {
            let plan = Audit {
                fail_sync_at: Some(fail_sync_at),
                ..Audit::default()
            };
            run_with_plan(workload, plan);
        }
        Ok(())
    })
}

/// State that outlives the node's restarts: what it acknowledged.
#[derive(Debug, Default)]
struct Observed {
    acknowledged: Mutex<Vec<(LogRecord, RecordLocation)>>,
    next_seq: AtomicU64,
    starts: AtomicU64,
    /// Whether the disk went out of service since the host last restarted:
    /// recovery failed, or the running log reports a failure.
    out_of_service: AtomicBool,
    log: Mutex<Option<SegmentLog<SimMount>>>,
}

impl Observed {
    fn take_out_of_service(&self) -> bool {
        let log = self.log.lock().unwrap().take();
        let failed = log.is_some_and(|log| !log.is_in_service());
        self.out_of_service.swap(false, Ordering::Relaxed) || failed
    }
}

/// Sync errors and torn writes, frequent enough to show up in most seeds.
const FAULTS: SimDiskFaults = SimDiskFaults {
    sync_error_probability: 0.005,
    torn_write_probability: 0.5,
    capacity: None,
};

/// The node: recovers, checks every acknowledged record, then appends from
/// several tasks until it crashes or its disk goes out of service.
async fn node(disk: SimDisk, seed: u64, observed: Arc<Observed>) -> turmoil::Result {
    let starts = observed.starts.fetch_add(1, Ordering::Relaxed);
    let log = match SegmentLog::open(disk.mount(), small_config(), clock()).await {
        Ok((log, _)) => log,
        // An injected sync error during recovery: wait for a restart.
        Err(RecoveryError::Io(_)) => {
            observed.out_of_service.store(true, Ordering::Relaxed);
            return std::future::pending().await;
        }
        Err(error) => return Err(format!("recovery refused to start: {error}").into()),
    };
    *observed.log.lock().unwrap() = Some(log.clone());
    let acknowledged = observed.acknowledged.lock().unwrap().clone();
    for (record, location) in acknowledged {
        match log.read(location).await {
            Ok(read) if read == record => {}
            other => {
                return Err(format!("acknowledged record at {location} lost: {other:?}").into());
            }
        }
    }
    for writer in 0..3_u8 {
        let log = log.clone();
        let observed = Arc::clone(&observed);
        let mut rng = <rand::rngs::SmallRng as rand::SeedableRng>::seed_from_u64(
            seed ^ (starts << 8) ^ u64::from(writer),
        );
        tokio::spawn(async move {
            loop {
                let seq = observed.next_seq.fetch_add(1, Ordering::Relaxed);
                let record = match rng.random_range(0..3) {
                    0 => delete(writer, seq),
                    1 => inline_put(writer, seq, rng.random_range(0..400)),
                    _ => extent(writer, seq, rng.random_range(1..3000)),
                };
                let Ok(location) = log.append(&record).await else {
                    return;
                };
                observed
                    .acknowledged
                    .lock()
                    .unwrap()
                    .push((record, location));
                tokio::time::sleep(Duration::from_micros(rng.random_range(0..2000))).await;
            }
        });
    }
    std::future::pending().await
}

fn crash_restart(context: &mut SimContext) -> Result<(u64, u64), Box<dyn Error>> {
    let disk = context.disk_with_faults(FAULTS);
    let seed = context.fork_seed();
    let observed = Arc::new(Observed::default());
    let mut sim = context.builder().build();
    let host_disk = disk.clone();
    let host_observed = Arc::clone(&observed);
    sim.host("node", move || {
        node(host_disk.clone(), seed, Arc::clone(&host_observed))
    });

    // After a sync error, the page cache may show bytes the disk lost, so
    // the disk is used again only after the host restarts, which loses the
    // page cache as a power loss does (design §10.4). Otherwise a restart
    // is a process crash or a power loss.
    let restart = |sim: &mut turmoil::Sim<'_>, power_loss: bool| {
        sim.crash("node");
        if observed.take_out_of_service() || power_loss {
            disk.crash();
        }
        sim.bounce("node");
    };
    for _ in 0..1_000 {
        sim.step()?;
        if context.rng().random_bool(0.01) {
            let power_loss = context.rng().random_bool(0.7);
            restart(&mut sim, power_loss);
        }
    }
    // A final power loss checks the last acknowledgements too.
    restart(&mut sim, true);
    sim.step()?;

    let acknowledged = observed.acknowledged.lock().unwrap().len() as u64;
    if acknowledged == 0 {
        return Err("the node acknowledged nothing".into());
    }
    Ok((acknowledged, observed.starts.load(Ordering::Relaxed)))
}

#[test]
fn power_losses_and_process_crashes_lose_no_acknowledged_record() {
    Runner::new().run(|context| crash_restart(context).map(drop));
}

#[test]
fn a_seed_replays_exactly() {
    let run = |seed| crash_restart(&mut SimContext::new(seed)).unwrap();
    let first = run(3);
    assert_eq!(run(3), first);
    assert!(first.1 > 1, "the node never restarted");
}
