//! Resuming a node's replicas when it starts (§6.2): each in its shard's
//! register, or, while the register cannot be read, in the configuration
//! of the latest `CONFIG` record the node applied, which its index keeps.
//!
//! A configuration kept that way may be stale: the shard's membership may
//! have changed while the node was down. Epochs fence a stale replica
//! (rule R2): its members, or its primary, have moved to a newer epoch and
//! refuse it, so it commits nothing and serves nothing, and the worst it
//! costs is a redirect. A shard whose membership did not change resumes
//! serving without the control store. A replica opened from its kept
//! configuration reads its register every `link_timeout` until it can,
//! and then follows it.
//!
//! Two kinds of proposal the node may have made before it went down
//! survive the restart in its index, so that opening from the kept
//! configuration loses neither: a primary's promotion of a learner (§6.7)
//! and a member's takeover (§6.3). A member whose takeover may have landed
//! unseen follows no primary of the epoch it proposed over, and sends the
//! proposal again, until it learns the outcome.

use skys3_control::ControlError;
use skys3_index::IndexError;
use skys3_io::Disk;
use skys3_log::ShardRef;
use skys3_net::Network;
use skys3_types::ShardConfig;
use tokio::task::JoinSet;

use super::Replication;
use crate::error::ShardError;
use crate::shard::Shard;

impl<N: Network, D: Disk> Replication<N, D> {
    /// Opens this node's replica of `shard` in the newest configuration the
    /// node can find, unless it is open already, and returns it, or `None`
    /// if that configuration does not name the node.
    ///
    /// The node reads the shard's register, through the registers
    /// [`Replication::with_removal`] gave, and the configuration of the
    /// latest `CONFIG` record of the shard it applied
    /// ([`ShardSet::kept_config`](crate::ShardSet::kept_config)), and opens
    /// the replica in the newer of the two. If the register cannot be read,
    /// it opens the replica in the kept configuration (§6.2); the replica
    /// then reads the register every `link_timeout` until it can, and
    /// follows it: it adopts a newer configuration that names the node
    /// where it can ([`Shard::reconfigure`]), and otherwise stops, serving
    /// nothing, until the node opens it again. A register that is gone
    /// means the shard's bucket was deleted, and nothing is opened.
    ///
    /// # Errors
    ///
    /// [`ShardError::Unavailable`] if the index fails, and otherwise as
    /// [`Replication::open`].
    pub async fn resume(&self, shard: &ShardRef) -> Result<Option<Shard<D>>, ShardError> {
        let inner = &self.inner;
        if let Some(open) = inner.set.get(shard).await {
            return Ok(Some(open));
        }
        let kept = inner.set.kept_config(shard).await?;
        let read = match inner.removal.get() {
            Some(removal) => removal.registers.read(shard).await,
            None => Err(ControlError::Unavailable(
                "this node has no shard registers".to_owned(),
            )),
        };
        let (config, confirmed) = match (read, kept) {
            // A stale read of the register: the node applied a newer
            // configuration than it shows.
            (Ok(Some(held)), Some(kept)) if kept.epoch > held.epoch => (kept, false),
            (Ok(Some(held)), _) => (held, true),
            (Ok(None), _) => return Ok(None),
            (Err(error), Some(kept)) => {
                tracing::info!(%shard, epoch = %kept.epoch, %error,
                    "the shard's register cannot be read; resuming from the local copy");
                (kept, false)
            }
            (Err(error), None) => {
                tracing::debug!(%shard, %error, "the shard's register cannot be read");
                return Ok(None);
            }
        };
        let node = &inner.node;
        if !config.is_member(node) && !config.is_learner(node) {
            return Ok(None);
        }
        let replica = self.open(&config).await?;
        if !confirmed {
            self.confirm(&replica);
        }
        Ok(Some(replica))
    }

    /// Resumes every shard whose configuration the node keeps, as
    /// [`Replication::resume`] does, reading their registers concurrently:
    /// a node that restarts while the control store is slow or unreachable
    /// waits for it about once, not once per shard. Returns each shard's
    /// result, in shard order.
    ///
    /// # Errors
    ///
    /// An [`IndexError`] if the index fails.
    #[allow(clippy::type_complexity, reason = "one result per shard")]
    pub async fn resume_kept(
        &self,
    ) -> Result<Vec<(ShardRef, Result<Option<Shard<D>>, ShardError>)>, IndexError> {
        let kept = self.inner.set.kept_configs().await?;
        let mut resuming = JoinSet::new();
        for shard in kept.into_keys() {
            let replication = self.clone();
            resuming.spawn(async move {
                let resumed = replication.resume(&shard).await;
                (shard, resumed)
            });
        }
        let mut resumed = Vec::new();
        while let Some(joined) = resuming.join_next().await {
            match joined {
                Ok(one) => resumed.push(one),
                Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
                // Only the runtime shutting down cancels a task.
                Err(_) => {}
            }
        }
        resumed.sort_by(|(a, _), (b, _)| a.cmp(b));
        Ok(resumed)
    }

    /// Reads the register of `replica`'s shard, which opened in its kept
    /// configuration, every `link_timeout` until it can, then follows it
    /// ([`Replication::follow_register`]); a read older than the replica's
    /// configuration is stale, and the register is read again. It stops
    /// early once the node no longer has that replica open.
    fn confirm(&self, replica: &Shard<D>) {
        let Some(removal) = self.inner.removal.get().cloned() else {
            return;
        };
        let (replication, replica) = (self.clone(), replica.clone());
        let interval = self.inner.config.link_timeout;
        tokio::spawn(async move {
            let shard = replica.shard().clone();
            loop {
                tokio::time::sleep(interval).await;
                let open = replication.inner.set.get(&shard).await;
                if !open.is_some_and(|open| open.durable().same_channel(&replica.durable())) {
                    return;
                }
                match removal.registers.read(&shard).await {
                    Ok(held) => {
                        if replication.follow_register(&replica, held.as_ref()).await {
                            return;
                        }
                    }
                    Err(error) => {
                        tracing::debug!(%shard, %error, "the shard's register still cannot be read");
                    }
                }
            }
        });
    }

    /// Brings `replica` to `held`, what its shard's register holds, and
    /// returns whether that settled the replica, or `false` if `held` is a
    /// stale read, older than the replica's configuration.
    ///
    /// The replica's own configuration is confirmed. A newer configuration
    /// that names the node is adopted in order with the replica's writes
    /// where the replica can change in place
    /// ([`ShardSet::open_replica`](crate::ShardSet::open_replica)), and one
    /// that re-admits the node as a learner stops the replica and opens the
    /// shard again as a learner from its log; one that makes this node the
    /// primary is left to the takeover the member proposed, which it
    /// settles itself. A replica that cannot adopt `held`, because the
    /// register no longer names the node or gives it another role, stops:
    /// it serves and grants nothing, answers with `held` as a redirect, and
    /// opens in the register's configuration at the node's next start. So
    /// does a replica whose register is gone, as the shard's bucket was
    /// deleted, which opens nothing at the next start, and one whose
    /// register holds another configuration of its epoch.
    async fn follow_register(&self, replica: &Shard<D>, held: Option<&ShardConfig>) -> bool {
        let shard = replica.shard();
        let config = replica.config();
        let Some(held) = held else {
            tracing::info!(%shard, "the shard's register is gone; the replica stops");
            replica.depose("the shard's register is gone", None);
            return true;
        };
        if held.epoch < config.epoch {
            tracing::debug!(%shard, epoch = %held.epoch, "a stale read of the shard's register");
            return false;
        }
        if held.epoch == config.epoch {
            if *held == config {
                tracing::debug!(%shard, "the shard's register confirms the local copy");
            } else {
                tracing::warn!(%shard, ?held, "the shard's register holds another configuration");
                replica.depose(
                    "the shard's register holds another configuration of the replica's epoch",
                    None,
                );
            }
            return true;
        }
        let node = &self.inner.node;
        let named = held.is_member(node) || held.is_learner(node);
        if named && held.primary == *node && replica.outstanding_takeover().is_some() {
            return true;
        }
        let adopted = if named {
            self.open(held).await.map(drop)
        } else {
            Err(ShardError::configuration(
                shard,
                "the shard's register no longer names this node",
            ))
        };
        if let Err(error) = adopted {
            tracing::info!(%shard, epoch = %held.epoch, %error,
                "the shard changed while the node was away; the replica stops");
            replica.depose(
                "the shard's register holds a configuration the replica cannot adopt",
                Some(held),
            );
        }
        true
    }
}
