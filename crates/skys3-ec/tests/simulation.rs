//! Crash simulation of the fragment store (plan M5-02), run by CI's
//! simulation job with a larger seed set.
//!
//! Each seed draws a workload: concurrent writers of fragments of mixed
//! sizes, on segments and group commits small enough that the fragments
//! span several of each. A first run counts the workload's syncs. Then:
//!
//! - **Power cuts.** One run per sync and side cuts the power just before
//!   or just after that sync, with torn writes on half the crashes, and
//!   recovers. Every fragment the store acknowledged must read back whole,
//!   with its header and a valid checksum, and the recovered store must
//!   take new fragments that survive the next power cut.
//! - **Process crashes.** One run per sync kills the process at that sync,
//!   keeping the page cache. A second life checks every acknowledged
//!   fragment and writes more; then the power fails, and every fragment
//!   either life acknowledged must survive.
//!
//! A further test seeds bugs (acknowledging before the sync, never syncing
//! the directory, and skipping recovery's syncs) and checks that the
//! scenarios catch each of them.

mod support;

use std::ops::Range;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use rand::Rng;
use skys3_ec::fragment::FragmentHeader;
use skys3_ec::{FragmentId, FragmentStore, SeededBug};
use skys3_io::{Disk, SimDisk, SimDiskFaults, SimPower, SyncCut};
use skys3_sim::{Runner, SimContext};
use support::{KillMount, fragment, runtime, small_config};

/// Torn writes on half of the crashes, so recovery meets clean and torn
/// tails.
const TORN: SimDiskFaults = SimDiskFaults {
    sync_error_probability: 0.0,
    torn_write_probability: 0.5,
    capacity: None,
};

/// Fragment lengths: within one checksum block, exactly one, just over
/// one, and several.
const SIZES: [u64; 7] = [64, 1024, 4096, 65_536, 65_600, 131_136, 199_936];

/// Each seed's runs cost about this many typical simulation seeds, so CI's
/// seed count is divided by it.
const COST: u64 = 8;

/// A seed's workload.
#[derive(Clone, Debug)]
struct Workload {
    disk_seed: u64,
    writers: u8,
    /// The lengths of each writer's fragments, in order.
    sizes: Vec<Vec<u64>>,
}

impl Workload {
    fn draw(context: &mut SimContext) -> Self {
        let disk_seed = context.fork_seed();
        let writers = context.rng().random_range(1..=4);
        let sizes = (0..writers)
            .map(|_| {
                let count = context.rng().random_range(1..=4);
                (0..count)
                    .map(|_| SIZES[context.rng().random_range(0..SIZES.len())])
                    .collect()
            })
            .collect();
        Self {
            disk_seed,
            writers,
            sizes,
        }
    }
}

/// A fragment the store acknowledged.
#[derive(Clone, Debug)]
struct Acked {
    id: FragmentId,
    header: FragmentHeader,
    payload: Bytes,
}

type Acks = Arc<Mutex<Vec<Acked>>>;

async fn open<D: Disk>(disk: D, bug: Option<SeededBug>) -> Option<FragmentStore<D>> {
    let opened = match bug {
        Some(bug) => FragmentStore::open_with_bug(disk, small_config(), bug).await,
        None => FragmentStore::open(disk, small_config()).await,
    };
    opened.ok().map(|(store, _)| store)
}

/// Runs writers `writers` of `workload` against `store` until each is done
/// or the store fails, recording what was acknowledged. Writers from 10 on
/// repeat the workload's writers under other keys.
async fn write_all<D: Disk>(
    store: &FragmentStore<D>,
    workload: &Workload,
    writers: Range<u8>,
    acks: &Acks,
) {
    let mut tasks = Vec::new();
    for writer in writers {
        let store = store.clone();
        let acks = Arc::clone(acks);
        let sizes = workload.sizes[usize::from(writer % 10)].clone();
        tasks.push(tokio::spawn(async move {
            for (n, len) in sizes.into_iter().enumerate() {
                let seed = u64::from(writer) << 8 | n as u64;
                let (header, payload) = fragment(&format!("w{writer}/{n}"), len, seed);
                let Ok(id) = store.write(&header, payload.clone()).await else {
                    return;
                };
                let acked = Acked {
                    id,
                    header,
                    payload,
                };
                acks.lock().unwrap().push(acked);
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
}

/// Checks that every acknowledged fragment reads back whole, with its
/// header and a checksum that matches its bytes.
async fn check<D: Disk>(store: &FragmentStore<D>, acks: &[Acked]) -> Result<(), String> {
    for acked in acks {
        let len = acked.payload.len() as u64;
        let read = store
            .read(acked.id, 0..len)
            .await
            .map_err(|error| format!("acknowledged fragment {} lost: {error}", acked.id))?;
        if read.data != acked.payload || read.header != acked.header {
            return Err(format!("fragment {} reads back wrong", acked.id));
        }
        if read.crc32c != crc32c::crc32c(&acked.payload) {
            return Err(format!("fragment {} has a wrong checksum", acked.id));
        }
    }
    Ok(())
}

/// Recovers the store on `disk` after a crash and checks every fragment in
/// `acks`; then writes one more, cuts the power again, and checks them all
/// once more.
async fn recover_and_check(disk: &SimDisk, mut acks: Vec<Acked>) -> Result<(), String> {
    let (store, _) = FragmentStore::open(disk.mount(), small_config())
        .await
        .map_err(|error| format!("recovery refused to start: {error}"))?;
    check(&store, &acks).await?;
    let (header, payload) = fragment("after/0", 4096, 99);
    let id = store
        .write(&header, payload.clone())
        .await
        .map_err(|error| format!("the recovered store failed a write: {error}"))?;
    acks.push(Acked {
        id,
        header,
        payload,
    });
    drop(store);
    disk.crash();
    let (store, _) = FragmentStore::open(disk.mount(), small_config())
        .await
        .map_err(|error| format!("the second recovery refused to start: {error}"))?;
    check(&store, &acks).await
}

/// Runs `workload` with the power cut at `cut`, if any, then cuts it at
/// the end, recovers, and checks. Returns the syncs the run made.
fn power_cut_run(
    workload: &Workload,
    cut: Option<(u64, SyncCut)>,
    bug: Option<SeededBug>,
) -> Result<u64, String> {
    runtime().block_on(async {
        let disk = SimDisk::with_faults(workload.disk_seed, TORN);
        let power = SimPower::new();
        disk.set_power(&power);
        if let Some((at, side)) = cut {
            power.cut_at_sync(at, side);
        }
        let acks = Acks::default();
        if let Some(store) = open(disk.mount(), bug).await {
            write_all(&store, workload, 0..workload.writers, &acks).await;
        }
        let syncs = power.syncs();
        // No cut during the checks.
        power.cut_at_sync(u64::MAX, SyncCut::Before);
        disk.crash();
        let acks = acks.lock().unwrap().clone();
        recover_and_check(&disk, acks).await?;
        Ok(syncs)
    })
}

/// Runs `workload` and kills the process at sync `kill_at`, if any, or
/// after the writers finish. A second life checks and writes more; then the
/// power fails, and a third life checks everything acknowledged. Returns
/// the syncs of the first life.
fn kill_run(
    workload: &Workload,
    kill_at: Option<u64>,
    bug: Option<SeededBug>,
) -> Result<u64, String> {
    runtime().block_on(async {
        let disk = SimDisk::with_faults(workload.disk_seed, TORN);
        let acks = Acks::default();
        let first = KillMount::new(&disk, kill_at);
        if let Some(store) = open(first.clone(), bug).await {
            write_all(&store, workload, 0..workload.writers, &acks).await;
        }
        let syncs = first.syncs();
        disk.kill();

        let Some(store) = open(disk.mount(), bug).await else {
            return Err("recovery after a process crash failed".to_owned());
        };
        let before = acks.lock().unwrap().clone();
        check(&store, &before).await?;
        write_all(&store, workload, 10..10 + workload.writers, &acks).await;
        drop(store);
        disk.crash();
        let acks = acks.lock().unwrap().clone();
        recover_and_check(&disk, acks).await?;
        Ok(syncs)
    })
}

/// Cuts the power at every sync of `context`'s workload, before and after
/// it.
fn power_cuts(context: &mut SimContext, bug: Option<SeededBug>) -> Result<(), String> {
    let workload = Workload::draw(context);
    let syncs = power_cut_run(&workload, None, bug)?;
    if syncs < 3 {
        return Err(format!("the workload made only {syncs} syncs"));
    }
    for at in 0..syncs {
        for side in [SyncCut::Before, SyncCut::After] {
            power_cut_run(&workload, Some((at, side)), bug)
                .map_err(|error| format!("power cut {side:?} sync {at}: {error}"))?;
        }
    }
    Ok(())
}

/// Kills the process at every sync of `context`'s workload.
fn process_kills(context: &mut SimContext, bug: Option<SeededBug>) -> Result<(), String> {
    let workload = Workload::draw(context);
    let syncs = kill_run(&workload, None, bug)?;
    for at in 0..syncs {
        kill_run(&workload, Some(at), bug)
            .map_err(|error| format!("process crash at sync {at}: {error}"))?;
    }
    Ok(())
}

#[test]
fn power_cuts_at_every_sync_lose_no_acknowledged_fragment() {
    Runner::with_cost(4, COST).run(|context| power_cuts(context, None).map_err(Into::into));
}

#[test]
fn process_crashes_at_every_sync_lose_no_acknowledged_fragment() {
    Runner::with_cost(4, COST).run(|context| process_kills(context, None).map_err(Into::into));
}

#[test]
fn the_scenarios_catch_seeded_bugs() {
    type Scenario = fn(&mut SimContext, Option<SeededBug>) -> Result<(), String>;
    let cases: [(SeededBug, Scenario); 3] = [
        (SeededBug::AcknowledgeBeforeSync, power_cuts),
        (SeededBug::SkipDirectorySync, power_cuts),
        (SeededBug::SkipRecoverySync, process_kills),
    ];
    for (bug, scenario) in cases {
        let caught = (0..8).find_map(|seed| scenario(&mut SimContext::new(seed), Some(bug)).err());
        assert!(caught.is_some(), "{bug:?} was not caught");
    }
}

#[test]
fn a_seed_replays_exactly() {
    let workload = Workload::draw(&mut SimContext::new(5));
    let syncs = power_cut_run(&workload, None, None).unwrap();
    assert_eq!(power_cut_run(&workload, None, None).unwrap(), syncs);
    assert_eq!(
        kill_run(&workload, None, None).unwrap(),
        kill_run(&workload, None, None).unwrap()
    );
}
