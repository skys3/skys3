//! Which nodes a node's fragment traffic finds silent (design §8.3; plan
//! M8-07b).
//!
//! Placement ranks suspect nodes after live ones (§8.3), but the binary
//! has no node-wide judge of liveness: heartbeats go only to the
//! coordinator (§6.7), whose registry other nodes cannot read, and a
//! registration says nothing of whether its node runs. So each node judges
//! from its own fragment traffic instead ([`Silence`]): the writes of its
//! encoders, repairers, and moves, and the reads of its gateway and
//! repairers, whose checks reach every node holding a fragment of a shard
//! the node leads at each pass (§8.6). A node that did not answer one of
//! them is suspect for [`SUSPECT_FOR`], unless it answers another
//! meanwhile; a refusal or a fragment it does not hold is an answer.
//!
//! [`Watched`] wraps a fragment writer or source to report what it sees,
//! and the node's fragment planner marks the nodes [`Silence::judge`]s
//! silent [`NodeState::Suspect`]. So an encoder pays one failed write for a
//! node that is down, not one per stripe it places there, until the
//! suspicion lapses and the next write tries the node again; and moves
//! never target it.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use skys3_coord::{Candidate, NodeState};
use skys3_ec::fragment::FragmentHeader;
use skys3_ec::read::{HoldFuture, ReadFuture};
use skys3_ec::{
    FragmentBytes, FragmentId, FragmentReadError, FragmentRequest, FragmentSource, FragmentWriter,
    TransferError,
};
use skys3_types::NodeId;
use tokio::time::Instant;

/// How long a node that did not answer stays suspect, unless it answers
/// meanwhile: long enough that an encoder placing stripes on a node that
/// is down pays one failed write a minute, short enough that a node back
/// from a restart, which nothing else may write to while it is suspect,
/// receives fragments again within a minute.
pub(crate) const SUSPECT_FOR: Duration = Duration::from_secs(60);

/// The nodes the node's fragment traffic last found silent, and when.
/// Clones share them.
#[derive(Debug, Clone, Default)]
pub(crate) struct Silence {
    unanswered: Arc<Mutex<BTreeMap<NodeId, Instant>>>,
}

impl Silence {
    /// Records that `node` did not answer.
    pub(crate) fn unanswered(&self, node: &NodeId) {
        let now = Instant::now();
        let mut unanswered = self.lock();
        // Suspicions that lapsed go, so the map holds only nodes silent
        // within the last minute.
        unanswered.retain(|_, at| now.duration_since(*at) < SUSPECT_FOR);
        if unanswered.insert(node.clone(), now).is_none() {
            tracing::info!(
                %node,
                "a fragment write or read found the node silent; placement ranks it after live \
                 nodes until it answers"
            );
        }
    }

    /// Records that `node` answered.
    pub(crate) fn answered(&self, node: &NodeId) {
        if let Some(at) = self.lock().remove(node)
            && at.elapsed() < SUSPECT_FOR
        {
            tracing::info!(%node, "a node found silent answers fragment traffic again");
        }
    }

    /// Whether `node` did not answer within the last [`SUSPECT_FOR`], and
    /// has not answered since.
    pub(crate) fn is_silent(&self, node: &NodeId) -> bool {
        self.lock()
            .get(node)
            .is_some_and(|at| at.elapsed() < SUSPECT_FOR)
    }

    /// `candidate`, suspect if it is live by its registration but silent.
    pub(crate) fn judge(&self, mut candidate: Candidate) -> Candidate {
        if candidate.state == NodeState::Live && self.is_silent(&candidate.node) {
            candidate.state = NodeState::Suspect;
        }
        candidate
    }

    /// Records what a fragment write to `node` found.
    fn wrote(&self, node: &NodeId, written: &Result<FragmentId, TransferError>) {
        match written {
            Ok(_) | Err(TransferError::Refused { .. }) => self.answered(node),
            Err(TransferError::Transport { .. } | TransferError::NoAnswer(_)) => {
                self.unanswered(node);
            }
            // Nothing reached the node.
            Err(_) => {}
        }
    }

    /// Records what a fragment read from `node` found.
    fn read(&self, node: &NodeId, read: &Result<FragmentBytes, FragmentReadError>) {
        match read {
            Err(FragmentReadError::Unreachable { .. }) => self.unanswered(node),
            // The node served the read, or answered why not.
            _ => self.answered(node),
        }
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<NodeId, Instant>> {
        // Plain inserts and removals, which a panic cannot leave half done.
        self.unanswered
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// A fragment writer or source that reports to a [`Silence`] whether each
/// node it reaches answers.
#[derive(Debug)]
pub(crate) struct Watched<T> {
    inner: T,
    silence: Silence,
}

impl<T> Watched<T> {
    /// `inner`, reporting to `silence`.
    pub(crate) fn new(inner: T, silence: Silence) -> Self {
        Self { inner, silence }
    }
}

impl<W: FragmentWriter> FragmentWriter for Watched<W> {
    async fn write(
        &self,
        node: &NodeId,
        header: &FragmentHeader,
        data: Bytes,
    ) -> Result<FragmentId, TransferError> {
        let written = self.inner.write(node, header, data).await;
        self.silence.wrote(node, &written);
        written
    }
}

impl<S: FragmentSource> FragmentSource for Watched<S> {
    fn read(&self, request: FragmentRequest) -> ReadFuture<'_> {
        Box::pin(async move {
            let node = request.node.clone();
            let read = self.inner.read(request).await;
            self.silence.read(&node, &read);
            read
        })
    }

    fn register(&self) -> Option<(u64, Duration)> {
        self.inner.register()
    }

    fn hold(
        &self,
        node: NodeId,
        registration: u64,
        fragments: Vec<FragmentId>,
        end: bool,
    ) -> HoldFuture<'_> {
        self.inner.hold(node, registration, fragments, end)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use skys3_ec::fragment::{ObjectMeta, StripeInfo};
    use skys3_ec::read::FragmentIdentity;
    use skys3_log::ShardRef;
    use skys3_net::TransportError;
    use skys3_types::{
        AttemptId, BucketId, CodecId, ETag, Epoch, EpochSeq, Geometry, Seq, ShardId,
    };

    use super::*;

    fn node(name: &str) -> NodeId {
        NodeId::new(name).unwrap()
    }

    fn candidate(name: &str, state: NodeState) -> Candidate {
        Candidate {
            node: node(name),
            zone: None,
            rack: None,
            capacity_bytes: 1 << 30,
            state,
            shards: 0,
            primaries: 0,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_node_is_suspect_until_it_answers_or_a_minute_passes() {
        let silence = Silence::default();
        let (a, b) = (node("a"), node("b"));
        assert!(!silence.is_silent(&a));
        silence.unanswered(&a);
        silence.unanswered(&b);
        assert!(silence.is_silent(&a) && silence.is_silent(&b));
        assert_eq!(
            silence.judge(candidate("a", NodeState::Live)).state,
            NodeState::Suspect
        );
        // A departing node stays departing; a node not silent stays live.
        assert_eq!(
            silence.judge(candidate("b", NodeState::Departing)).state,
            NodeState::Departing
        );
        assert_eq!(
            silence.judge(candidate("c", NodeState::Live)).state,
            NodeState::Live
        );

        silence.answered(&a);
        assert!(!silence.is_silent(&a));
        tokio::time::sleep(SUSPECT_FOR).await;
        assert!(!silence.is_silent(&b), "the suspicion lapses");
        // A lapsed suspicion is dropped at the next one, and answering
        // after it changes nothing.
        silence.answered(&b);
        silence.unanswered(&a);
        assert_eq!(silence.lock().len(), 1);
    }

    /// A writer that answers with what `outcome` makes of each call.
    struct Scripted {
        calls: AtomicUsize,
        outcome: fn(&NodeId) -> Result<FragmentId, TransferError>,
    }

    impl FragmentWriter for Scripted {
        async fn write(
            &self,
            node: &NodeId,
            _header: &FragmentHeader,
            _data: Bytes,
        ) -> Result<FragmentId, TransferError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            (self.outcome)(node)
        }
    }

    fn header() -> FragmentHeader {
        let position = EpochSeq::new(Epoch::new(1), Seq::new(1));
        FragmentHeader {
            shard: ShardRef::new(BucketId::new("b-1").unwrap(), ShardId::new(0)),
            key: "k".to_owned(),
            version: position,
            attempt: AttemptId::new(Epoch::new(1), 1),
            stripe: stripe(),
            index: 0,
            object: ObjectMeta {
                size: 128,
                last_modified_ms: 0,
                etag: ETag::new("9b2cf535f27731c974343645a3985328").unwrap(),
                identity: position,
                metadata: BTreeMap::new(),
                tags: BTreeMap::new(),
                checksums: BTreeMap::new(),
                parts: Vec::new(),
            },
        }
    }

    fn stripe() -> StripeInfo {
        StripeInfo {
            number: 0,
            count: 1,
            offset: 0,
            data_len: 128,
            geometry: Geometry::new(2, 1).unwrap(),
            codec: CodecId::REED_SOLOMON_V1,
        }
    }

    #[tokio::test]
    async fn writes_report_who_answered() {
        let silence = Silence::default();
        let outcome = |node: &NodeId| match node.as_str() {
            "up" => Ok(FragmentId::new(1)),
            "refusing" => Err(TransferError::Refused {
                node: node.clone(),
                reason: "full".to_owned(),
            }),
            "down" => Err(TransferError::Transport {
                node: node.clone(),
                source: TransportError::HandshakeTimeout,
            }),
            "slow" => Err(TransferError::NoAnswer(node.clone())),
            _ => Err(TransferError::UnknownNode(node.clone())),
        };
        let writer = Watched::new(
            Scripted {
                calls: AtomicUsize::new(0),
                outcome,
            },
            silence.clone(),
        );
        for name in ["refusing", "up", "down", "slow", "unknown"] {
            silence.unanswered(&node(name));
        }
        silence.answered(&node("unknown"));
        for name in ["up", "refusing", "down", "slow", "unknown"] {
            let _ = writer
                .write(&node(name), &header(), Bytes::from_static(b"x"))
                .await;
        }
        assert_eq!(writer.inner.calls.load(Ordering::Relaxed), 5);
        let silent: Vec<&str> = ["up", "refusing", "down", "slow", "unknown"]
            .into_iter()
            .filter(|name| silence.is_silent(&node(name)))
            .collect();
        assert_eq!(silent, ["down", "slow"]);
    }

    /// A source that answers reads by the node they name, and takes holds.
    #[derive(Debug)]
    struct Answers;

    impl FragmentSource for Answers {
        fn read(&self, request: FragmentRequest) -> ReadFuture<'_> {
            let node = request.node;
            let reason = String::from("test");
            Box::pin(async move {
                match node.as_str() {
                    "up" => Ok(FragmentBytes {
                        data: Bytes::from_static(b"x"),
                        crc32c: 0,
                    }),
                    "empty" => Err(FragmentReadError::NotHeld { node, reason }),
                    "damaged" => Err(FragmentReadError::Damaged { node, reason }),
                    _ => Err(FragmentReadError::Unreachable { node, reason }),
                }
            })
        }

        fn register(&self) -> Option<(u64, Duration)> {
            Some((7, Duration::from_secs(1)))
        }

        fn hold(
            &self,
            _node: NodeId,
            registration: u64,
            _fragments: Vec<FragmentId>,
            end: bool,
        ) -> HoldFuture<'_> {
            Box::pin(async move {
                if end {
                    Err(registration.to_string())
                } else {
                    Ok(())
                }
            })
        }
    }

    fn request(name: &str) -> FragmentRequest {
        let position = EpochSeq::new(Epoch::new(1), Seq::new(1));
        FragmentRequest {
            node: node(name),
            fragment: FragmentId::new(2),
            identity: FragmentIdentity {
                shard: ShardRef::new(BucketId::new("b-1").unwrap(), ShardId::new(0)),
                key: "k".to_owned(),
                version: position,
                etag: ETag::new("9b2cf535f27731c974343645a3985328").unwrap(),
                stripe: stripe(),
                index: 0,
            },
            range: 0..1,
        }
    }

    #[tokio::test]
    async fn reads_report_who_answered_and_holds_pass_through() {
        let silence = Silence::default();
        let source = Watched::new(Answers, silence.clone());
        for name in ["up", "empty", "damaged"] {
            silence.unanswered(&node(name));
        }
        for name in ["up", "empty", "damaged", "down"] {
            let _ = source.read(request(name)).await;
        }
        let silent: Vec<&str> = ["up", "empty", "damaged", "down"]
            .into_iter()
            .filter(|name| silence.is_silent(&node(name)))
            .collect();
        assert_eq!(silent, ["down"]);
        assert_eq!(source.register(), Some((7, Duration::from_secs(1))));
        assert!(source.hold(node("up"), 7, Vec::new(), false).await.is_ok());
        assert_eq!(
            source.hold(node("up"), 7, Vec::new(), true).await,
            Err("7".to_owned())
        );
    }
}
