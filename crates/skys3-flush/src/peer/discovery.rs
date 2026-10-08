//! Discovery and fallback (§7.8, plan M6-07): which transport the
//! flushers of a target that may be a SkyS3 peer use, and when that
//! changes.
//!
//! A [`Discovery`] runs for each such target. It reads the peer descriptor
//! from the target's S3 endpoint ([`DESCRIPTOR_KEY`], through the target's
//! own store), verifies it, and tries a QUIC handshake with the addresses
//! it names within `peer_connect_timeout`:
//!
//! - **QUIC** when the handshake succeeds ([`Choice::Native`]). A flush
//!   whose link fails tells the discovery ([`Discovery::link_failed`]),
//!   which tries a fresh handshake at once and falls back if it fails.
//! - **S3 REST to the peer** when the descriptor verifies but the handshake
//!   does not ([`Choice::PeerS3`]): every precondition is honored, as on
//!   any SkyS3 gateway, so nothing is probed. Another handshake is tried
//!   after a backoff that doubles from the transport's least re-probe
//!   interval to its most, and the target returns to QUIC once one
//!   succeeds.
//! - **Plain S3 REST** when the target has no descriptor, or one that
//!   does not verify ([`Choice::PlainS3`]): the target is probed and
//!   flushed as any S3 store. With no descriptor at all (`404`, or any
//!   other `4xx`), the target is not a peer, and the discovery ends; a
//!   refused descriptor is read again after the backoff. A target that
//!   served a descriptor, refused or not, may be a peer that took native
//!   commits, so its shard flushers wait out the quarantine as they
//!   start.
//! - **Waiting** ([`Choice::Waiting`]) until the descriptor can be read,
//!   and for a target whose `target_transport` is `native` for as long as
//!   QUIC cannot be used: nothing is flushed meanwhile.
//!
//! Each change of choice is a new generation; the flush service rebuilds
//! the target and restarts its shard flushers for it, as a restart or a
//! takeover would (see [`FlushService`](crate::FlushService)).

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use skys3_config::TargetTransport;
use skys3_io::WallClock;
use skys3_peer::{DESCRIPTOR_KEY, Expected, MAX_DESCRIPTOR_LEN, PeerDescriptor};
use skys3_remote::{ByteRange, GetObject, ObjectStore, S3ErrorKind};
use skys3_types::{BucketName, ClusterId};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use super::hooks::peer_bug;
use super::{PeerBug, PeerLink, PeerTransport};

/// The transport a target's flushers use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Transport {
    /// The native peer protocol over QUIC.
    Quic,
    /// S3 REST.
    S3,
}

impl Transport {
    /// The transport's name in the admin API and the metric's label.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Transport::Quic => "quic",
            Transport::S3 => "s3",
        }
    }
}

/// What a target's discovery found, as the admin API reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportStatus {
    /// The bucket's `target_transport`.
    pub configured: TargetTransport,
    /// The transport its flushers use now, or `None` while they wait for
    /// one.
    pub in_use: Option<Transport>,
    /// Whether the target is a SkyS3 peer: it served a descriptor that
    /// verified.
    pub peer: bool,
    /// Why, in words: the latest decision, or what the latest attempt to
    /// change it found.
    pub reason: String,
    /// How many times the transport changed since the bucket was followed.
    pub switches: u64,
}

/// What the flushers of a target use.
#[derive(Clone)]
pub(crate) enum Choice {
    /// Nothing yet: flushing waits.
    Waiting,
    /// QUIC, over this link.
    Native(Arc<dyn PeerLink>),
    /// S3 REST to a verified SkyS3 peer, which honors every precondition.
    PeerS3,
    /// S3 REST to a store that is not a verified SkyS3 peer: probed.
    PlainS3 {
        /// Whether the store served a descriptor, which did not verify:
        /// it may be a peer that took native commits, so the shard
        /// flushers wait out the quarantine.
        found: bool,
    },
}

impl Choice {
    fn transport(&self) -> Option<Transport> {
        match self {
            Choice::Waiting => None,
            Choice::Native(_) => Some(Transport::Quic),
            Choice::PeerS3 | Choice::PlainS3 { .. } => Some(Transport::S3),
        }
    }

    /// Whether `other` is the same choice, a link aside: moving between
    /// them needs no new target.
    fn same(&self, other: &Choice) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

impl std::fmt::Debug for Choice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Choice::Waiting => "Waiting",
            Choice::Native(_) => "Native",
            Choice::PeerS3 => "PeerS3",
            Choice::PlainS3 { .. } => "PlainS3",
        })
    }
}

/// The current choice and its generation, shared with the flush service.
#[derive(Debug)]
struct Selection {
    generation: u64,
    choice: Choice,
    status: TransportStatus,
}

#[derive(Debug)]
struct Shared {
    selection: Mutex<Selection>,
    /// Raised by flushes whose link failed.
    failed: Notify,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, Selection> {
        // Every update leaves the selection consistent.
        self.selection
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Makes `choice` the target's choice for `reason`: a new generation,
    /// unless it is the choice already made, of which only the reason
    /// changes.
    fn choose(&self, choice: Choice, peer: bool, reason: String) {
        let mut selection = self.lock();
        selection.status.peer = peer;
        if selection.choice.same(&choice) {
            if selection.status.reason != reason {
                tracing::debug!(reason, "the target's transport stays");
            }
            selection.status.reason = reason;
            return;
        }
        if selection.choice.transport().is_some() {
            selection.status.switches += 1;
        }
        tracing::info!(from = ?selection.choice, to = ?choice, reason,
            "the target's transport changes");
        selection.status.in_use = choice.transport();
        selection.status.reason = reason;
        selection.generation += 1;
        selection.choice = choice;
    }

    /// Records why the choice was not changed.
    fn note(&self, reason: String) {
        self.lock().status.reason = reason;
    }
}

/// The transport selection of one target that may be a SkyS3 peer.
#[derive(Debug)]
pub(crate) struct Discovery {
    shared: Arc<Shared>,
    task: Option<JoinHandle<()>>,
}

impl Drop for Discovery {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

/// What a discovery needs to run.
pub(crate) struct Probe<S> {
    /// The target's store, which serves the descriptor.
    pub(crate) store: Arc<S>,
    /// The destination bucket.
    pub(crate) bucket: BucketName,
    /// The bucket's `target_transport`: `auto` or `native`.
    pub(crate) configured: TargetTransport,
    /// How peers are reached.
    pub(crate) peers: Arc<PeerTransport>,
    /// This cluster, which the bucket must receive from.
    pub(crate) cluster: ClusterId,
    /// The clock descriptors are checked on.
    pub(crate) wall: Arc<dyn WallClock>,
}

impl Discovery {
    /// Starts the discovery of the target `probe` describes, on the current
    /// Tokio runtime.
    pub(crate) fn spawn<S: ObjectStore>(probe: Probe<S>) -> Self {
        let shared = Arc::new(Shared {
            selection: Mutex::new(Selection {
                generation: 0,
                choice: Choice::Waiting,
                status: TransportStatus {
                    configured: probe.configured,
                    in_use: None,
                    peer: false,
                    reason: "looking for a peer descriptor".to_owned(),
                    switches: 0,
                },
            }),
            failed: Notify::new(),
        });
        let task = tokio::spawn(run(probe, Arc::clone(&shared)));
        Self {
            shared,
            task: Some(task),
        }
    }

    /// A target that waits for good: `native`, on a node that cannot reach
    /// peers, for `reason`.
    pub(crate) fn stuck(configured: TargetTransport, reason: &str) -> Self {
        Self {
            shared: Arc::new(Shared {
                selection: Mutex::new(Selection {
                    generation: 0,
                    choice: Choice::Waiting,
                    status: TransportStatus {
                        configured,
                        in_use: None,
                        peer: false,
                        reason: reason.to_owned(),
                        switches: 0,
                    },
                }),
                failed: Notify::new(),
            }),
            task: None,
        }
    }

    /// The current choice and its generation.
    pub(crate) fn current(&self) -> (u64, Choice) {
        let selection = self.shared.lock();
        (selection.generation, selection.choice.clone())
    }

    /// What the discovery found.
    pub(crate) fn status(&self) -> TransportStatus {
        self.shared.lock().status.clone()
    }

    /// What a flush calls when its link failed: the discovery checks the
    /// path with a fresh handshake.
    pub(crate) fn alarm(&self) -> LinkAlarm {
        LinkAlarm(Arc::clone(&self.shared))
    }
}

/// Tells a target's discovery that a link failed.
#[derive(Clone, Debug)]
pub(crate) struct LinkAlarm(Arc<Shared>);

impl LinkAlarm {
    /// A flush's link failed.
    pub(crate) fn link_failed(&self) {
        self.0.failed.notify_one();
    }
}

/// What reading the descriptor found.
enum Fetched {
    /// The store answered with these bytes.
    Found(bytes::Bytes),
    /// The store has none: the target is not a peer.
    Absent(String),
    /// The store did not answer usefully; ask again later.
    Unreachable(String),
}

/// Runs the discovery until the target turns out not to be a peer, or the
/// discovery is dropped.
async fn run<S: ObjectStore>(probe: Probe<S>, shared: Arc<Shared>) {
    let native_only = probe.configured == TargetTransport::Native;
    // Failed rounds since the last handshake that succeeded.
    let mut failures = 0u32;
    loop {
        let wait = match fetch(&probe).await {
            Fetched::Unreachable(error) => {
                shared.note(format!("the peer descriptor could not be read: {error}"));
                failures += 1;
                Some(probe.peers.reprobe_after(failures))
            }
            Fetched::Absent(reason) if native_only => {
                shared.choose(Choice::Waiting, false, reason);
                failures += 1;
                Some(probe.peers.reprobe_after(failures))
            }
            Fetched::Absent(reason) => {
                shared.choose(Choice::PlainS3 { found: false }, false, reason);
                return;
            }
            Fetched::Found(bytes) => match verify(&probe, &bytes) {
                Err(reason) => {
                    let choice = if native_only {
                        Choice::Waiting
                    } else {
                        Choice::PlainS3 { found: true }
                    };
                    tracing::warn!(reason, "the target's peer descriptor is refused");
                    shared.choose(choice, false, reason);
                    failures += 1;
                    Some(probe.peers.reprobe_after(failures))
                }
                Ok(descriptor) => match handshake(&probe, &descriptor).await {
                    Ok(link) => {
                        failures = 0;
                        let reason = format!(
                            "QUIC to cluster {} at {}",
                            descriptor.cluster,
                            descriptor.addresses.join(", ")
                        );
                        shared.choose(Choice::Native(link), true, reason);
                        None
                    }
                    Err(reason) => {
                        let choice = if native_only {
                            Choice::Waiting
                        } else {
                            Choice::PeerS3
                        };
                        shared.choose(choice, true, reason);
                        failures += 1;
                        Some(probe.peers.reprobe_after(failures))
                    }
                },
            },
        };
        match wait {
            Some(wait) => tokio::time::sleep(wait).await,
            // On QUIC: check the path again once a flush's link fails.
            None => shared.failed.notified().await,
        }
    }
}

/// Reads the target's descriptor, at most [`MAX_DESCRIPTOR_LEN`] bytes of
/// it.
async fn fetch<S: ObjectStore>(probe: &Probe<S>) -> Fetched {
    let last = u64::try_from(MAX_DESCRIPTOR_LEN).unwrap_or(u64::MAX);
    let request = GetObject {
        range: ByteRange::inclusive(0, last),
        ..GetObject::new(DESCRIPTOR_KEY)
    };
    match probe.store.get_object(request).await {
        Ok(output) => Fetched::Found(output.body),
        Err(error) if error.kind() == S3ErrorKind::NoSuchKey => {
            Fetched::Absent("the target serves no peer descriptor".to_owned())
        }
        Err(error)
            if error
                .status()
                .is_some_and(|code| (400..500).contains(&code)) =>
        {
            Fetched::Absent(format!(
                "the target refused the peer descriptor's GET: {error}"
            ))
        }
        Err(error) => Fetched::Unreachable(error.to_string()),
    }
}

/// The descriptor `bytes` hold, if it verifies for this source and bucket.
fn verify<S>(probe: &Probe<S>, bytes: &[u8]) -> Result<PeerDescriptor, String> {
    if peer_bug() == PeerBug::UnverifiedDescriptor {
        return PeerDescriptor::decode_unverified(bytes)
            .map_err(|error| format!("the peer descriptor is refused: {error}"));
    }
    let expected = Expected {
        bucket: &probe.bucket,
        source: &probe.cluster,
    };
    probe
        .peers
        .verifier
        .verify(bytes, probe.wall.now(), expected)
        .map_err(|error| format!("the peer descriptor is refused: {error}"))
}

/// A link to the destination `descriptor` names, once a fresh handshake
/// with it finished within `peer_connect_timeout`.
async fn handshake<S>(
    probe: &Probe<S>,
    descriptor: &PeerDescriptor,
) -> Result<Arc<dyn PeerLink>, String> {
    let timeout = probe.peers.connect_timeout;
    match tokio::time::timeout(timeout, (probe.peers.connect)(descriptor)).await {
        Ok(Ok(link)) => Ok(link),
        Ok(Err(error)) => Err(format!(
            "the QUIC handshake with cluster {} failed: {error}",
            descriptor.cluster
        )),
        Err(_) => Err(format!(
            "the QUIC handshake with cluster {} did not finish within {timeout:?}",
            descriptor.cluster
        )),
    }
}

/// Re-probe intervals: `min`, doubling per failed round, at most `max`.
pub(crate) fn backoff(min: Duration, max: Duration, failures: u32) -> Duration {
    let factor = 1u32 << failures.saturating_sub(1).min(20);
    min.saturating_mul(factor).min(max).max(min.min(max))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn re_probes_back_off_to_the_most() {
        let (min, max) = (Duration::from_secs(15), Duration::from_secs(300));
        let waits: Vec<u64> = (1..=7).map(|n| backoff(min, max, n).as_secs()).collect();
        assert_eq!(waits, [15, 30, 60, 120, 240, 300, 300]);
        assert_eq!(backoff(min, max, 0), min);
        assert_eq!(backoff(min, max, u32::MAX), max);
    }

    #[test]
    fn choices_change_generations_only_when_they_differ() {
        let discovery = Discovery::stuck(TargetTransport::Native, "no transport");
        let shared = &discovery.shared;
        assert_eq!(discovery.current().0, 0);
        assert_eq!(discovery.status().reason, "no transport");
        shared.choose(Choice::Waiting, false, "still".to_owned());
        assert_eq!(discovery.current().0, 0);
        assert_eq!(discovery.status().reason, "still");
        shared.choose(Choice::PeerS3, true, "UDP is blocked".to_owned());
        let status = discovery.status();
        assert_eq!(
            (status.in_use, status.peer, status.switches),
            (Some(Transport::S3), true, 0)
        );
        shared.choose(Choice::PlainS3 { found: true }, false, "refused".to_owned());
        assert_eq!(discovery.current().0, 2);
        assert_eq!(discovery.status().switches, 1);
        shared.note("a later round failed".to_owned());
        assert_eq!(discovery.status().reason, "a later round failed");
        assert!(format!("{:?}", discovery.current().1).contains("PlainS3"));
        assert_eq!(Transport::Quic.as_str(), "quic");
        assert_eq!(Transport::S3.as_str(), "s3");
        discovery.alarm().link_failed();
    }
}
