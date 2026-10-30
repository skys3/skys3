//! The `BATCH`es a target's flushers share (§7.8): small objects and
//! deletes from every shard flushing to the target travel together, one
//! round trip a batch.
//!
//! A flush hands its `COMMIT` to the target's [`Batcher`] and waits for
//! its `APPLIED`. The batcher sends a batch as soon as it has items and
//! fewer than [`BATCHES_IN_FLIGHT`] batches are unanswered, so an idle
//! target sends each item at once, and a busy one gathers the items that
//! arrive while its batches are in flight. Each batch is packed by
//! [`BatchBuilder`]: an item that does not fit, or whose key or identity
//! the batch already holds, waits for the next one.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use skys3_peer::{Applied, Batch, BatchBuilder, Commit, Message, Outcome, Refusal, Write};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

use super::PeerBug;
use super::hooks::peer_bug;
use super::link::{LinkError, PeerLink, Stream};

/// The most batches of a target sent and not yet answered.
pub(crate) const BATCHES_IN_FLIGHT: usize = 4;

/// Why an item got no `APPLIED`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Unbatched {
    /// The session does not accept `BATCH`: stage the object instead.
    NotSupported,
    /// The batch failed or went unanswered: whether the item applied is
    /// unknown, and the flush is retried.
    Lost(LinkError),
}

/// One `COMMIT` waiting for its batch.
struct Item {
    commit: Commit,
    reply: oneshot::Sender<Result<Outcome, Unbatched>>,
}

/// The batches of one target, sent by a task of their own.
#[derive(Debug, Clone)]
pub(crate) struct Batcher {
    items: mpsc::UnboundedSender<Item>,
    /// Whether a session refused `BATCH`.
    unsupported: Arc<AtomicBool>,
}

impl Batcher {
    /// Starts the batches of a target reached through `link`, each stream
    /// bounded by `timeout`, on the current Tokio runtime. The task ends
    /// once every handle is dropped and every batch is answered.
    pub(crate) fn spawn(link: Arc<dyn PeerLink>, timeout: Duration) -> Self {
        let (items, received) = mpsc::unbounded_channel();
        let unsupported = Arc::new(AtomicBool::new(false));
        tokio::spawn(run(link, timeout, received, Arc::clone(&unsupported)));
        Self { items, unsupported }
    }

    /// Whether objects may travel in batches: no session has refused them.
    pub(crate) fn supported(&self) -> bool {
        !self.unsupported.load(Ordering::Relaxed)
    }

    /// Queues `commit`, a delete or a version with its bytes inline, for
    /// the next batch, and returns the wait for its result.
    pub(crate) fn submit(&self, commit: Commit) -> Result<Queued, Unbatched> {
        if !self.supported() {
            return Err(Unbatched::NotSupported);
        }
        let (reply, answer) = oneshot::channel();
        self.items
            .send(Item { commit, reply })
            .map_err(|_| stopped())?;
        Ok(Queued(answer))
    }
}

/// A `COMMIT` queued for a batch.
pub(crate) struct Queued(oneshot::Receiver<Result<Outcome, Unbatched>>);

impl Queued {
    /// Its result, once its batch is answered.
    pub(crate) async fn applied(self) -> Result<Outcome, Unbatched> {
        self.0.await.unwrap_or_else(|_| Err(stopped()))
    }
}

/// The failure of an item whose batcher stopped.
fn stopped() -> Unbatched {
    Unbatched::Lost(LinkError::new("the target's batches stopped"))
}

/// Gathers items and sends them in batches until every handle is dropped.
async fn run(
    link: Arc<dyn PeerLink>,
    timeout: Duration,
    mut received: mpsc::UnboundedReceiver<Item>,
    unsupported: Arc<AtomicBool>,
) {
    let mut pending = VecDeque::new();
    let mut sending = JoinSet::new();
    let mut open = true;
    loop {
        while sending.len() < BATCHES_IN_FLIGHT && !pending.is_empty() {
            let items = pack(&mut pending);
            if !items.is_empty() {
                let (link, unsupported) = (Arc::clone(&link), Arc::clone(&unsupported));
                sending.spawn(send(link, timeout, items, unsupported));
            }
        }
        if !open && pending.is_empty() && sending.is_empty() {
            return;
        }
        tokio::select! {
            item = received.recv(), if open => match item {
                Some(item) => {
                    pending.push_back(item);
                    while let Ok(item) = received.try_recv() {
                        pending.push_back(item);
                    }
                }
                None => open = false,
            },
            Some(_) = sending.join_next(), if !sending.is_empty() => {}
        }
    }
}

/// Takes the items of the next batch from `pending`, oldest first. Items
/// that do not fit stay, in order; an item that can never travel in a
/// batch is answered at once.
fn pack(pending: &mut VecDeque<Item>) -> Vec<Item> {
    let mut builder = BatchBuilder::new();
    let mut taken = Vec::new();
    let mut kept = VecDeque::new();
    for item in pending.drain(..) {
        match builder.push(&item.commit) {
            Ok(()) => taken.push(item),
            Err(Refusal::Full | Refusal::Repeated) => kept.push_back(item),
            Err(refusal) => {
                let error = LinkError::new(format!("the object cannot be batched: {refusal}"));
                let _ = item.reply.send(Err(Unbatched::Lost(error)));
            }
        }
    }
    *pending = kept;
    taken
}

/// Sends `items` as one `BATCH` on a stream of its own and answers each
/// with its `APPLIED`; an item without one gets the stream's failure.
async fn send(
    link: Arc<dyn PeerLink>,
    timeout: Duration,
    items: Vec<Item>,
    unsupported: Arc<AtomicBool>,
) {
    let batch = Batch {
        items: items.iter().map(|item| item.commit.clone()).collect(),
    };
    let mut replies: Vec<_> = items.into_iter().map(|item| Some(item.reply)).collect();
    let result = exchange(&*link, timeout, &batch, &mut replies, &unsupported).await;
    let failure = match result {
        Ok(()) => Unbatched::Lost(LinkError::new("the batch ended without an APPLIED for it")),
        Err(failure) => failure,
    };
    for reply in replies.into_iter().flatten() {
        let _ = reply.send(Err(failure.clone()));
    }
}

/// Sends `batch` and answers each item's reply in `replies` as its
/// `APPLIED` arrives.
async fn exchange(
    link: &dyn PeerLink,
    timeout: Duration,
    batch: &Batch,
    replies: &mut [Option<oneshot::Sender<Result<Outcome, Unbatched>>>],
    unsupported: &AtomicBool,
) -> Result<(), Unbatched> {
    let mut stream = Stream::open(link, timeout).await.map_err(Unbatched::Lost)?;
    if !stream.batches() {
        unsupported.store(true, Ordering::Relaxed);
        return Err(Unbatched::NotSupported);
    }
    stream
        .send(&Message::Batch(batch.clone()))
        .await
        .map_err(Unbatched::Lost)?;
    stream.finish().map_err(Unbatched::Lost)?;
    if peer_bug() == PeerBug::FlushedOnCommit {
        // The seeded bug: every item counts as committed once sent.
        for (item, reply) in batch.items.iter().zip(replies.iter_mut()) {
            if let Some(reply) = reply.take() {
                let _ = reply.send(Ok(assumed_committed(item)));
            }
        }
    }
    while let Some(message) = stream.recv().await.map_err(Unbatched::Lost)? {
        let Message::Applied(Applied { identity, outcome }) = message else {
            continue;
        };
        let at = batch
            .items
            .iter()
            .position(|item| item.identity == identity);
        if let Some(reply) = at.and_then(|at| replies[at].take()) {
            let _ = reply.send(Ok(outcome));
        }
        if replies.iter().all(Option::is_none) {
            break;
        }
    }
    Ok(())
}

/// The result the seeded bug [`PeerBug::FlushedOnCommit`] assumes for a
/// `COMMIT` it sent.
pub(crate) fn assumed_committed(commit: &Commit) -> Outcome {
    let etag = match &commit.write {
        Write::Put(put) => Some(put.etag.clone()),
        Write::Delete => None,
    };
    Outcome::Committed { etag }
}
