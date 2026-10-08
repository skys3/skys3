//! Fragment moves (design §8.3, plan M5-09): a repairer's pass that found
//! no fragment lost moves fragments of the shard's stripes, as
//! [`FragmentPlanner::moves`] plans them, publish before retire.

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Bytes;
use skys3_coord::{FragmentPlanner, MoveRequest, PlannedMove, ShardStripe};
use skys3_index::{Entry, Index};
use skys3_io::Disk;
use skys3_log::record::FragmentMove;
use skys3_types::{AttemptId, FragmentLocation, NodeId};

use super::{
    Inventory, RepairBug, RepairError, RepairReport, RepairStep, Repairer, Transfer, coded_len,
    coded_stripe, coded_version, stripe_info,
};
use crate::codec;
use crate::encoder::{Writing, fragment_header, leads};
use crate::transfer::FragmentWriter;

impl<D: Disk, W: FragmentWriter> Repairer<D, W> {
    /// Moves up to [`RepairSettings::moves_per_pass`] fragments of the
    /// shard's stripes, one at a time, as `planner` plans them around the
    /// nodes `silent` at their last check, while this replica leads.
    ///
    /// [`RepairSettings::moves_per_pass`]: super::RepairSettings::moves_per_pass
    pub(super) async fn rebalance(
        &self,
        pass: u64,
        inventory: &Inventory,
        planner: &FragmentPlanner,
        silent: &BTreeSet<NodeId>,
        report: &mut RepairReport,
    ) {
        if self.settings.moves_per_pass == 0 {
            return;
        }
        let stripes: Vec<ShardStripe<'_>> = inventory
            .entries
            .iter()
            .filter_map(|(key, entry)| {
                let coded = entry.object.as_ref()?.coded.as_ref()?;
                Some(
                    coded
                        .stripes
                        .iter()
                        .map(move |stripe| ShardStripe { key, stripe }),
                )
            })
            .flatten()
            .collect();
        let avoid: Vec<NodeId> = silent.iter().cloned().collect();
        let shard = self.shard.shard();
        let planned = planner.moves(&MoveRequest {
            bucket: &shard.bucket,
            shard: shard.shard,
            stripes: &stripes,
            avoid: &avoid,
            limit: self.settings.moves_per_pass,
        });
        for planned in planned {
            if !leads(&self.shard) {
                break;
            }
            // The entry as it is now, not as the pass found it: a write or
            // a retag since would reject the move's record.
            let entry = match self.current(&planned.key).await {
                Ok(Some(entry)) => entry,
                Ok(None) => continue,
                Err(error) => {
                    tracing::warn!(shard = %shard, %error, "moves cannot read the index");
                    break;
                }
            };
            let located = coded_stripe(&entry, planned.stripe)
                .and_then(|stripe| stripe.fragments().get(usize::from(planned.index)));
            if located != Some(&planned.from) {
                // The layout changed since the plan: the next pass plans
                // again.
                continue;
            }
            match self.make_move(pass, &entry, &planned).await {
                Ok(true) => {
                    report.moved += 1;
                    self.metrics.moved_fragment();
                }
                Ok(false) => report.unmoved += 1,
                Err(error) => {
                    report.unmoved += 1;
                    tracing::info!(
                        shard = %self.shard.shard(),
                        key = planned.key,
                        stripe = planned.stripe,
                        index = planned.index,
                        %error,
                        "a fragment was not moved"
                    );
                }
            }
        }
    }

    /// The entry of `key` in the shard's index now.
    async fn current(&self, key: &str) -> Result<Option<Entry>, RepairError> {
        let index: Arc<Index> = Arc::clone(self.shard.index());
        let shard = self.shard.shard().clone();
        let key = key.to_owned();
        self.pool
            .run(move || index.read()?.entry(&shard, &key))
            .await
            .map_err(|_| RepairError::PoolClosed)?
            .map_err(RepairError::from)
    }

    /// Moves one fragment as a fragment-writing attempt: whether its
    /// `EC_RELOCATE` was applied.
    async fn make_move(
        &self,
        pass: u64,
        entry: &Entry,
        planned: &PlannedMove,
    ) -> Result<bool, RepairError> {
        let attempt = self.numbers.next(&self.shard)?;
        let unfenced = self.has_bug(RepairBug::UnfencedMove);
        if !unfenced {
            self.attempts.begin(attempt);
        }
        // Forgets the attempt if this future is dropped before it appends.
        let _writing = Writing {
            attempts: &self.attempts,
            attempt,
        };
        let key = planned.key.as_str();
        self.emit(
            key,
            planned.stripe,
            attempt,
            RepairStep::MoveStarted {
                pass,
                index: planned.index,
                from: planned.from.node.clone(),
                to: planned.to.clone(),
                reason: planned.reason,
            },
        );
        let to = match self.copy(entry, planned, attempt, unfenced).await {
            Ok(to) => to,
            Err(error) => {
                self.attempts.finish(attempt);
                self.emit(key, planned.stripe, attempt, RepairStep::Abandoned);
                return Err(error);
            }
        };
        if unfenced {
            self.attempts.begin(attempt);
        }
        let moved = FragmentMove {
            stripe: planned.stripe,
            index: planned.index,
            from: planned.from.clone(),
            to,
        };
        self.relocate(key, planned.stripe, entry, attempt, vec![moved])
            .await
    }

    /// Reads the fragment `planned` moves whole and writes it to its new
    /// node, with a header of `attempt`: where it is durable now.
    async fn copy(
        &self,
        entry: &Entry,
        planned: &PlannedMove,
        attempt: AttemptId,
        unfenced: bool,
    ) -> Result<FragmentLocation, RepairError> {
        let missing = || RepairError::Unreadable { read: 0, needed: 1 };
        let key = planned.key.as_str();
        let (number, index) = (planned.stripe, planned.index);
        let stripe = coded_stripe(entry, number).ok_or_else(missing)?;
        // The copy names the version the stripe's fragments were written
        // for, which a retag leaves behind the entry's.
        let version = coded_version(entry).ok_or_else(missing)?;
        let len = codec(stripe.codec())?.fragment_len(stripe.geometry(), stripe.data_len())?;
        let identity = self
            .identity(key, entry, number, index)
            .ok_or_else(missing)?;

        self.check_writing(attempt, unfenced)?;
        let data = self
            .read_whole(&planned.from, identity, len, Transfer::Move)
            .await
            .map_err(RepairError::Source)?;
        let node = planned.from.node.clone();
        self.emit(key, number, attempt, RepairStep::Read { index, node });

        self.check_writing(attempt, unfenced)?;
        let to = planned.to.clone();
        let nodes = vec![(index, to.clone())];
        self.emit(key, number, attempt, RepairStep::Placed { nodes });
        let info = stripe_info(stripe, coded_len(entry));
        let header = fragment_header(
            self.shard.shard(),
            key,
            entry,
            version,
            attempt,
            info,
            index,
        );
        self.pace(Transfer::Move, len).await;
        let data = Bytes::from(data);
        let written = self.writer.write(&to, &header, data).await;
        self.metrics.transferred(len);
        let fragment = written.map_err(RepairError::Fragments)?;
        let node = to.clone();
        self.emit(key, number, attempt, RepairStep::Written { index, node });
        Ok(FragmentLocation { node: to, fragment })
    }
}
