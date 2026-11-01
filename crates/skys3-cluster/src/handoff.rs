//! [`Handoffs`]: starting the planned handoffs a coordinator asks a node
//! for (§5.4, §6.7).

use std::fmt;
use std::sync::Arc;

use skys3_coord::{Handoff, HandoffFuture, HandoffSink};
use skys3_gateway::ShardRef;
use skys3_io::Disk;
use skys3_net::Network;
use skys3_shard::replication::Replication;

use crate::observe::{ReplicaObserver, Unobserved};

/// Starts the planned handoffs a coordinator asks a node for (§6.7,
/// *Asking for a handoff*): only on the node's serving primary of the
/// shard, in the epoch the request names, to another member of it. A
/// request planned from an older register, or by a node that is no longer
/// coordinator, so does nothing once the shard has moved on.
///
/// A request is advice: the handoff itself
/// ([`Replication::hand_off`]) checks the register and every
/// configuration it leads to is a compare-and-swap, so a lost, late,
/// repeated, or forged request costs at most a handoff. Who may ask is
/// for the endpoint that receives requests to decide.
///
/// Clones share the replication.
pub struct Handoffs<N: Network, D: Disk> {
    replication: Replication<N, D>,
    observer: Arc<dyn ReplicaObserver<D>>,
}

impl<N: Network, D: Disk> Clone for Handoffs<N, D> {
    fn clone(&self) -> Self {
        Self {
            replication: self.replication.clone(),
            observer: Arc::clone(&self.observer),
        }
    }
}

impl<N: Network, D: Disk> fmt::Debug for Handoffs<N, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Handoffs")
            .field("node", self.replication.node())
            .finish_non_exhaustive()
    }
}

impl<N: Network, D: Disk> Handoffs<N, D> {
    /// Starts handoffs on the replicas of `replication`.
    #[must_use]
    pub fn new(replication: Replication<N, D>) -> Self {
        Self {
            replication,
            observer: Arc::new(Unobserved),
        }
    }

    /// The same, reporting the handoffs it starts to `observer`.
    #[must_use]
    pub fn with_observer(self, observer: Arc<dyn ReplicaObserver<D>>) -> Self {
        Self { observer, ..self }
    }

    /// Starts `handoff` on a task of its own, if the node's replica of its
    /// shard is the shard's serving primary in `handoff.epoch` and
    /// `handoff.to` is another member.
    async fn start(&self, handoff: Handoff) -> Result<(), String> {
        let shard = skys3_log::ShardRef::new(handoff.bucket.clone(), handoff.shard);
        let replica = self
            .replication
            .set()
            .get(&shard)
            .await
            .ok_or_else(|| format!("shard {shard} is not open here"))?;
        let config = replica.config();
        if config.epoch != handoff.epoch {
            return Err(format!(
                "the replica is in epoch {}, not {}",
                config.epoch, handoff.epoch
            ));
        }
        let node = self.replication.node();
        if config.primary != *node || !replica.is_serving() || replica.is_stopped() {
            return Err("the replica is not the shard's serving primary".to_owned());
        }
        if handoff.to == *node || !config.is_member(&handoff.to) {
            return Err(format!("{} is not another member", handoff.to));
        }
        let named = ShardRef {
            bucket: handoff.bucket,
            shard: handoff.shard,
        };
        self.observer.handoff_began(&named, config.epoch);
        let (replication, observer, to) = (
            self.replication.clone(),
            Arc::clone(&self.observer),
            handoff.to,
        );
        tokio::spawn(async move {
            let handed = replication.hand_off(&shard, &to).await;
            if let Err(error) = &handed {
                tracing::info!(%shard, %error, "an asked handoff failed");
            }
            observer.handoff_ended(&named, &handed);
        });
        Ok(())
    }
}

impl<N: Network, D: Disk> HandoffSink for Handoffs<N, D> {
    fn hand_off(&self, handoff: Handoff) -> HandoffFuture<'_> {
        Box::pin(self.start(handoff))
    }
}
