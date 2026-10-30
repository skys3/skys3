//! Fragment release in the coding cluster (plan M5-07, M5-12, design
//! §8.7): every node keeps the fragments its shard primary releases in its
//! [`FragmentHolds`] for the release delay and then reclaims them, orphan
//! reclamation releases its orphans the same way, and every replica that
//! leads the shard runs a [`Releaser`].
//!
//! A run with release settles only once every fragment a node that is up
//! holds is named by some member's coded layout: what encodings, repairs,
//! and moves left behind, and what overwrites, deletes, and relocations
//! replaced, must all be gone. Fragments that outlive their layouts for
//! [`LEAK_LIMIT`] once everything else settled fail the run.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use skys3_ec::{
    FragmentHolds, FragmentServer, FragmentStore, HoldSettings, ReleaseClient, ReleaseSettings,
    ReleaseSink, ReleasedFragment, Releaser,
};
use skys3_io::{BlockingPool, Clock, SimMount};
use skys3_log::ShardRef;
use skys3_net::{Transport, TurmoilNetwork};
use skys3_shard::Shard;
use skys3_types::NodeId;

use super::{Driver, World, lock, moves};

/// How long the nodes may hold fragments that no layout names once
/// everything else settled: well past the orphan age, the release delay,
/// and a restart of the node, which forgets what it was told.
pub(super) const LEAK_LIMIT: Duration = Duration::from_secs(15);

/// Fragment release on every node.
#[derive(Debug, Clone, Copy)]
pub struct ReleaseConfig {
    /// `fragment_release_delay_seconds`.
    pub delay: Duration,
    /// How often each node reclaims the released fragments that waited the
    /// delay.
    pub sweep: Duration,
    /// How often the releaser of a replica that leads runs a pass.
    pub interval: Duration,
}

impl Default for ReleaseConfig {
    fn default() -> Self {
        Self {
            delay: Duration::from_millis(300),
            sweep: Duration::from_millis(50),
            interval: Duration::from_millis(100),
        }
    }
}

/// A release a node noted: the position of the releasing primary, the
/// node told, and the fragments.
#[derive(Debug, Clone)]
pub(super) struct Told {
    pub(super) releaser: usize,
    pub(super) node: NodeId,
    pub(super) fragments: Vec<ReleasedFragment>,
}

/// Every release the nodes noted, in order.
#[derive(Debug, Default)]
pub(super) struct ReleaseLog {
    pub(super) told: Mutex<Vec<Told>>,
}

impl ReleaseLog {
    /// How many fragments the nodes noted released, counting each time.
    pub(super) fn noted(&self) -> usize {
        lock(&self.told)
            .iter()
            .map(|told| told.fragments.len())
            .sum()
    }
}

/// The holds of a node of a run with release, timed by `clock`.
pub(super) fn holds(world: &World, clock: &Arc<dyn Clock>) -> Option<FragmentHolds> {
    let config = world.run.release?;
    let settings = HoldSettings {
        release_delay: config.delay,
        registration_ttl: Duration::from_secs(30),
    };
    Some(FragmentHolds::new(Arc::clone(clock), settings))
}

/// Reclaims the released fragments of `store` that waited the delay, for
/// the rest of the node's life.
pub(super) fn sweep(world: &World, holds: FragmentHolds, store: FragmentStore<SimMount>) {
    if let Some(config) = world.run.release {
        tokio::spawn(holds.run(vec![store], config.sweep, 0.5));
    }
}

/// Starts the releaser of node `index`'s replica `shard`, which tells nodes
/// through `transport`, and the node itself through `fragments`.
pub(super) fn start(
    world: &Arc<World>,
    index: usize,
    shard: Shard<SimMount>,
    transport: &Transport<TurmoilNetwork>,
    fragments: &FragmentServer<SimMount>,
) {
    let Some(config) = world.run.release else {
        return;
    };
    let client = ReleaseClient::new(
        world.nodes[index].id.clone(),
        transport.clone(),
        world.peers.clone(),
        Some(fragments.clone()),
    );
    let sink = Observed {
        client,
        world: Arc::clone(world),
        releaser: index,
    };
    let settings = ReleaseSettings {
        interval: config.interval,
        batch: ReleaseSettings::BATCH,
    };
    let releaser = Releaser::new(
        shard,
        Arc::new(sink),
        moves::planner_source(Arc::clone(world)),
        BlockingPool::inline("release"),
        settings,
    );
    tokio::spawn(releaser.run());
}

/// A releaser's tells, of which the driver learns those noted.
struct Observed {
    client: ReleaseClient<TurmoilNetwork, SimMount>,
    world: Arc<World>,
    releaser: usize,
}

impl ReleaseSink for Observed {
    async fn release(
        &self,
        node: &NodeId,
        shard: &ShardRef,
        fragments: &[ReleasedFragment],
    ) -> Result<(), String> {
        self.client.release(node, shard, fragments).await?;
        lock(&self.world.release.told).push(Told {
            releaser: self.releaser,
            node: node.clone(),
            fragments: fragments.to_vec(),
        });
        Ok(())
    }
}

impl Driver<'_> {
    /// Checks that every fragment a node that is up holds is named by some
    /// member's coded layout, of any key; or says which is not.
    pub(super) fn unnamed(&self) -> Result<(), String> {
        let views = self.views();
        let mut named = BTreeSet::new();
        for member in self.world.members() {
            let Some(view) = &views[member] else {
                continue;
            };
            let reader = view.index.read().map_err(|e| e.to_string())?;
            for key in moves::coded_keys(&reader, &self.world.shard)? {
                let entry = reader
                    .entry(&self.world.shard, &key)
                    .map_err(|e| e.to_string())?;
                let Some(coded) = entry.and_then(|e| e.object).and_then(|o| o.coded) else {
                    continue;
                };
                for stripe in &coded.stripes {
                    for location in stripe.fragments() {
                        named.insert((location.node.clone(), location.fragment));
                    }
                }
            }
        }
        let written = lock(&self.world.fragments);
        for (n, (view, slot)) in views.iter().zip(&self.world.nodes).enumerate() {
            let Some(view) = view else { continue };
            let generation = self.world.generation_of(&slot.id);
            for id in view.fragments.stores()[0].ids() {
                if named.contains(&(slot.id.clone(), id)) {
                    continue;
                }
                let attempt = written
                    .get(&(slot.id.clone(), generation, id))
                    .map(|written| written.attempt);
                let released = view
                    .fragments
                    .holds()
                    .is_some_and(|holds| holds.is_released(id));
                return Err(format!(
                    "n{n} holds fragment {id} of attempt {attempt:?}, released {released}, \
                     which no member's layout names"
                ));
            }
        }
        Ok(())
    }
}
