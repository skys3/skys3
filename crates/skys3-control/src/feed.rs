//! [`ChangeFeed`]: the change stream built from `get` and `list`, which
//! every backend without a richer native watch uses.

use std::collections::BTreeMap;
use std::time::Duration;

use skys3_types::Generation;
use tokio::sync::watch;

use crate::key::{KeyPrefix, RegisterKey, TypedKey};
use crate::propose::read;
use crate::store::{Change, ChangeStream, ControlError, ControlStore, Version};

/// A [`ChangeStream`] that reads the generation in `cluster.json` and, when
/// it moves, lists the registers and reports the difference from its
/// previous report.
///
/// It observes when woken: by an in-process notification of every write
/// ([`ChangeFeed::notified`], the in-memory and file backends), or by a
/// timer ([`ChangeFeed::polling`], for S3 backends, which poll every
/// `config_poll_interval`). Listing only after reading the generation
/// guarantees that every write announced by that generation is in the
/// listing.
///
/// [`ChangeStream::next`] is cancel-safe: the feed updates its state only
/// when it returns a report, so a dropped call loses nothing.
#[derive(Debug)]
pub struct ChangeFeed<S> {
    store: S,
    wake: Wake,
    /// The generation last reported, or the stream's `after` before the
    /// first report.
    generation: Generation,
    /// The versions last reported, or `None` before the first report.
    reported: Option<BTreeMap<RegisterKey, Version>>,
}

#[derive(Debug)]
enum Wake {
    /// Woken by every write.
    Notified(watch::Receiver<()>),
    /// Woken by a timer; `due` is set once the first observation is made.
    Polling { interval: Duration, due: bool },
}

impl<S: ControlStore> ChangeFeed<S> {
    /// A feed that observes whenever `written` is notified, which the
    /// backend does after every write.
    #[must_use]
    pub fn notified(store: S, after: Generation, written: watch::Receiver<()>) -> Self {
        Self::new(store, after, Wake::Notified(written))
    }

    /// A feed that observes at once and then every `interval`.
    #[must_use]
    pub fn polling(store: S, after: Generation, interval: Duration) -> Self {
        Self::new(
            store,
            after,
            Wake::Polling {
                interval,
                due: false,
            },
        )
    }

    fn new(store: S, after: Generation, wake: Wake) -> Self {
        Self {
            store,
            wake,
            generation: after,
            reported: None,
        }
    }

    /// Reads the generation and, if it moved, lists the registers and
    /// builds a report.
    async fn observe(&mut self) -> Result<Option<Change>, ControlError> {
        let cluster = read(&self.store, &TypedKey::cluster())
            .await?
            .ok_or(ControlError::NotBootstrapped)?;
        let generation = cluster.value.generation;
        if generation == self.generation {
            return Ok(None);
        }
        let current: BTreeMap<_, _> = self
            .store
            .list(&KeyPrefix::root())
            .await?
            .into_iter()
            .filter(|(key, _)| reported(key))
            .collect();
        let registers = match &self.reported {
            None => current
                .iter()
                .map(|(key, version)| (key.clone(), Some(version.clone())))
                .collect(),
            Some(previous) => diff(previous, &current),
        };
        let snapshot = self.reported.is_none();
        self.reported = Some(current);
        self.generation = generation;
        Ok(Some(Change {
            generation,
            snapshot,
            registers,
        }))
    }
}

/// Whether the feed reports `key`: every register but `cluster.json`,
/// whose generation the report carries, and `coordinator.lease`, which is
/// rewritten on every renewal.
fn reported(key: &RegisterKey) -> bool {
    *key != RegisterKey::cluster() && *key != RegisterKey::coordinator_lease()
}

/// The registers whose version differs between two listings.
fn diff(
    previous: &BTreeMap<RegisterKey, Version>,
    current: &BTreeMap<RegisterKey, Version>,
) -> Vec<(RegisterKey, Option<Version>)> {
    let mut changed: Vec<_> = current
        .iter()
        .filter(|(key, version)| previous.get(*key) != Some(*version))
        .map(|(key, version)| (key.clone(), Some(version.clone())))
        .chain(
            previous
                .keys()
                .filter(|key| !current.contains_key(*key))
                .map(|key| (key.clone(), None)),
        )
        .collect();
    changed.sort_by(|a, b| a.0.cmp(&b.0));
    changed
}

impl<S: ControlStore> ChangeStream for ChangeFeed<S> {
    async fn next(&mut self) -> Result<Change, ControlError> {
        loop {
            match &mut self.wake {
                Wake::Notified(written) => {
                    // Mark every write so far as seen before observing, so
                    // a write during the observation wakes the next wait.
                    written.borrow_and_update();
                }
                Wake::Polling { interval, due } => {
                    if *due {
                        tokio::time::sleep(*interval).await;
                    }
                    *due = true;
                }
            }
            if let Some(change) = self.observe().await? {
                return Ok(change);
            }
            if let Wake::Notified(written) = &mut self.wake
                && written.changed().await.is_err()
            {
                // The backend dropped its sender, which it does not do
                // while the feed holds a clone of it; fall back to polling.
                self.wake = Wake::Polling {
                    interval: Duration::from_secs(1),
                    due: true,
                };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{bootstrap, bump_generation};
    use crate::memory::MemoryControlStore;
    use crate::propose::{ProposalIds, RetryPolicy};

    fn key(key: &str) -> RegisterKey {
        RegisterKey::new(key).unwrap()
    }

    #[test]
    fn differences_include_new_changed_and_removed_registers() {
        let version = |v: &str| Version::new(v);
        let previous = BTreeMap::from([
            (key("buckets/a.json"), version("1")),
            (key("buckets/b.json"), version("2")),
            (key("nodes/n.json"), version("3")),
        ]);
        let current = BTreeMap::from([
            (key("buckets/a.json"), version("1")),
            (key("buckets/b.json"), version("4")),
            (key("shards/b-1/0.json"), version("5")),
        ]);
        assert_eq!(
            diff(&previous, &current),
            vec![
                (key("buckets/b.json"), Some(version("4"))),
                (key("nodes/n.json"), None),
                (key("shards/b-1/0.json"), Some(version("5"))),
            ]
        );
        assert!(diff(&current, &current).is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_store_without_cluster_json_is_an_error() {
        let store = MemoryControlStore::new();
        let mut feed = ChangeFeed::polling(store, Generation::ZERO, Duration::from_secs(1));
        let error = feed.next().await.unwrap_err();
        assert!(matches!(error, ControlError::NotBootstrapped), "{error}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_feed_without_notifications_falls_back_to_polling() {
        let store = MemoryControlStore::new();
        let (policy, mut ids) = (RetryPolicy::default(), ProposalIds::seeded(0));
        let cluster = "c".parse().unwrap();
        bootstrap(&store, &cluster, ids.next_id(), &policy)
            .await
            .unwrap();
        let (sender, receiver) = watch::channel(());
        drop(sender);
        let mut feed = ChangeFeed::notified(store.clone(), Generation::new(1), receiver);
        let waited = tokio::time::timeout(Duration::from_secs(5), feed.next()).await;
        assert!(waited.is_err(), "{waited:?}");
        bump_generation(&store, &cluster, &mut ids, &policy)
            .await
            .unwrap();
        let change = feed.next().await.unwrap();
        assert_eq!(change.generation, Generation::new(2));
    }
}
