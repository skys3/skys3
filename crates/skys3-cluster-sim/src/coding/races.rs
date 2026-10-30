//! An overwrite or a delete racing the repair of its key (plan M5-12,
//! design §8.6, §8.7): once a repair after the losses reaches a chosen
//! step, the shard primary overwrites the repaired key with new bytes, or
//! deletes it.
//!
//! The repair's `EC_RELOCATE` then commits before the write, and its
//! fragments are released with the version it relocated, or after, and is
//! rejected, which leaves its rebuilt fragments orphans. Either way nothing
//! may resurrect: from the moment a delete commits, no replica that leads
//! the shard and serves has an object at the key, and at the end no member
//! has. An overwritten key reads back its new bytes, coded in turn. With
//! release, the run also settles only once the replaced version's
//! fragments and the rebuilt ones are gone.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use skys3_ec::RepairStep;
use skys3_index::IndexReader;
use skys3_io::SimMount;
use skys3_log::record::Delete;
use skys3_log::{RecordBody, ShardRef};
use skys3_shard::{Role, Shard};

use super::{Driver, RepairPoint, View, World, bytes, lock, moves, put, roll};

/// What races a repair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaceOp {
    /// The key is overwritten with as many new bytes.
    Overwrite,
    /// The key is deleted.
    Delete,
}

/// An overwrite or a delete of the key of the first repair to reach
/// `point`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaceConfig {
    /// The write.
    pub op: RaceOp,
    /// The step of the repair it races.
    pub point: RepairPoint,
}

/// What the race did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RaceOutcome {
    /// The key raced, once a repair reached the point.
    pub key: Option<String>,
    /// Whether the overwrite or the delete committed.
    pub written: bool,
    /// Repairs of the key whose `EC_RELOCATE` was applied.
    pub relocated: usize,
    /// Repairs of the key whose `EC_RELOCATE` was rejected: the version had
    /// moved on.
    pub rejected: usize,
}

/// The race as the nodes see it.
#[derive(Debug, Default)]
pub(super) struct RaceLog {
    /// The key to overwrite or delete, until the primary takes it.
    key: Mutex<Option<String>>,
    /// The keys deleted, once each delete committed.
    pub(super) deleted: Mutex<BTreeSet<String>>,
    /// Whether the write committed.
    written: AtomicBool,
}

/// The race's phase of the driver.
#[derive(Debug, Default)]
pub(super) struct Phase {
    /// The key raced.
    raced: Option<String>,
    /// How many repair events the point was looked for in.
    seen: usize,
}

/// Overwrites or deletes the key the driver names, once, on the primary
/// replica `shard`; an overwritten key is coded again, and its replicas
/// rolled out of the last segment.
pub(super) async fn serve(world: Arc<World>, shard: Shard<SimMount>) {
    let Some(config) = world.run.race else {
        return;
    };
    let key = loop {
        if let Some(key) = lock(&world.races.key).take() {
            break key;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    };
    match config.op {
        RaceOp::Overwrite => {
            let size = lock(&world.expected).get(&key).map_or(1, Vec::len);
            put(&world, &shard, &key, bytes(world.seed ^ 0x0ace, 1, size)).await;
            world.races.written.store(true, Ordering::SeqCst);
            loop {
                tokio::time::sleep(Duration::from_millis(200)).await;
                let entry = shard.entry(&key).await.ok().flatten();
                if entry
                    .and_then(|entry| entry.object)
                    .is_some_and(|object| object.coded.is_some())
                {
                    roll(&shard).await;
                    return;
                }
            }
        }
        RaceOp::Delete => {
            // The key leaves the objects checked before the delete can
            // apply, and joins the deleted ones once it committed.
            lock(&world.expected).remove(&key);
            let delete = RecordBody::Delete(Delete { key: key.clone() });
            while shard.commit(delete.clone()).await.is_err() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            lock(&world.races.deleted).insert(key);
            world.races.written.store(true, Ordering::SeqCst);
        }
    }
}

/// Checks that `reader` has no object at `key`, which was deleted.
pub(super) fn stays_deleted(
    reader: &IndexReader,
    shard: &ShardRef,
    key: &str,
) -> Result<(), String> {
    let entry = reader.entry(shard, key).map_err(|e| e.to_string())?;
    match entry.and_then(|entry| entry.object) {
        None => Ok(()),
        Some(object) => Err(format!(
            "{key}, deleted, has an object again, of {} bytes{}",
            object.size,
            if object.coded.is_some() {
                ", coded"
            } else {
                ""
            }
        )),
    }
}

impl Driver<'_> {
    /// Names the key of the first repair to reach the race's point, for
    /// the primary to overwrite or delete.
    pub(super) fn race_progress(&mut self) {
        let Some(config) = self.world.run.race else {
            return;
        };
        if self.race.raced.is_some() {
            return;
        }
        let events = lock(&self.world.repair.events);
        let moves = moves::move_attempts(&events);
        let reached = events[self.race.seen..]
            .iter()
            .find(|(_, _, event)| {
                !moves.contains(&event.attempt) && config.point.reached(&event.step)
            })
            .map(|(_, _, event)| event.key.clone());
        self.race.seen = events.len();
        drop(events);
        if let Some(key) = reached {
            tracing::debug!(key, ?config, "the race point is reached");
            *lock(&self.world.races.key) = Some(key.clone());
            self.race.raced = Some(key);
        }
    }

    /// Checks that no replica that leads the shard and serves has an object
    /// at a key whose delete committed.
    pub(super) fn check_deleted(&self, views: &[Option<View>]) -> Result<(), String> {
        let deleted = lock(&self.world.races.deleted).clone();
        if deleted.is_empty() {
            return Ok(());
        }
        for (n, view) in views.iter().enumerate() {
            let Some(view) = view else { continue };
            let leads = view.shard.as_ref().is_some_and(|shard| {
                matches!(shard.role(), Role::Primary | Role::Alone) && shard.is_serving()
            });
            if !leads {
                continue;
            }
            let reader = view.index.read().map_err(|e| e.to_string())?;
            for key in &deleted {
                stays_deleted(&reader, &self.world.shard, key)
                    .map_err(|e| format!("the serving primary n{n}: {e}"))?;
            }
        }
        Ok(())
    }

    /// Checks that no member has an object at a deleted key; or says which
    /// has.
    pub(super) fn deletes_settled(&self) -> Result<(), String> {
        let deleted = lock(&self.world.races.deleted).clone();
        if deleted.is_empty() {
            return Ok(());
        }
        let views = self.views();
        for member in self.world.members() {
            let Some(view) = &views[member] else {
                return Err(format!("member n{member} has not recovered"));
            };
            let reader = view.index.read().map_err(|e| e.to_string())?;
            for key in &deleted {
                stays_deleted(&reader, &self.world.shard, key)
                    .map_err(|e| format!("member n{member}: {e}"))?;
            }
        }
        Ok(())
    }

    /// What the race did, in a run that races a repair.
    pub(super) fn race_outcome(&self) -> Option<RaceOutcome> {
        self.world.run.race?;
        let mut outcome = RaceOutcome {
            key: self.race.raced.clone(),
            written: self.world.races.written.load(Ordering::SeqCst),
            ..RaceOutcome::default()
        };
        let events = lock(&self.world.repair.events);
        for (_, _, event) in events.iter() {
            if Some(&event.key) != outcome.key.as_ref() {
                continue;
            }
            match event.step {
                RepairStep::Committed {
                    relocated: true, ..
                } => outcome.relocated += 1,
                RepairStep::Committed { .. } => outcome.rejected += 1,
                _ => {}
            }
        }
        Some(outcome)
    }
}
