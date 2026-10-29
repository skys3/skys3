//! The bodies of the replication messages: `prost` messages in a frame's
//! header, with an encoded log record as the payload where one travels.
//! Members and primaries decode them from authenticated but untrusted
//! peers, so every field is checked before it is used.
//!
//! A link carries one shard between its primary and one member:
//!
//! ```text
//! primary                                  member
//!   Sync(config, primary's last seq, sequencing epoch, lineage) ->
//!                                          <- SyncAck(last, record)*   records past the primary's last seq
//!                                          <- SyncAck(last, done)      after truncating, if it had to
//!                                          <- SyncAck(refused)         a member that cannot reconcile
//!   Append(epoch, commit, lazy, lease) + record ->
//!   Beacon(epoch, commit, lease)          ->
//!                                          <- AppendAck(durable, lease) as durability grows, and per beacon
//!                                          <- AppendAck(rejected)      an append from an older epoch (R2)
//! ```
//!
//! `lease` is a lease stamp (§5.4): the primary's clock reading when it sent
//! the frame. An acknowledgement echoes the latest stamp the member received
//! in the session, which grants the primary a lease until the stamp plus
//! `primary_lease`, on the primary's clock.
//!
//! A `Sync` may carry a newer configuration than the member's, after a
//! member removal (§6.4) or a takeover (§6.5). Its `sequencing` epoch tells
//! the member whether the primary has appended that configuration's
//! `CONFIG` record, and so where the member appends its own (§5.1). Its
//! lineage, the epochs of the primary's records, tells the member which of
//! its own records the primary holds too (§6.6): the member sends back
//! those past the primary's log that the primary sequenced itself, and
//! truncates any other it holds past the prefix they share.

use bytes::Bytes;
use prost::Message;
use skys3_net::{Frame, Header, MessageKind};
use skys3_types::{Epoch, RegisterDocument, Seq, ShardConfig};

use crate::lineage::{self, Lineage};

/// Primary to member: open a session for one shard.
#[derive(Clone, PartialEq, Message)]
pub struct Sync {
    /// The primary's configuration of the shard, as the JSON of its
    /// register (design §6.1).
    #[prost(bytes = "vec", tag = "1")]
    pub config: Vec<u8>,
    /// The last `seq` the primary holds: the member returns the records it
    /// holds after it.
    #[prost(uint64, tag = "2")]
    pub primary_last: u64,
    /// The epoch the primary sequences records in: the configuration's
    /// once it has appended its `CONFIG` record, an earlier one before.
    #[prost(uint64, tag = "3")]
    pub sequencing: u64,
    /// The `seq` after which the primary's lineage is known.
    #[prost(uint64, tag = "4")]
    pub known_after: u64,
    /// The primary's lineage: the epochs of its log's records (§6.6).
    #[prost(message, repeated, tag = "5")]
    pub lineage: Vec<Run>,
}

/// One run of a [`Lineage`]: the log holds records of `epoch` up to
/// `last`.
#[derive(Clone, Copy, PartialEq, Eq, Message)]
pub struct Run {
    /// The epoch.
    #[prost(uint64, tag = "1")]
    pub epoch: u64,
    /// The last `seq` of the run.
    #[prost(uint64, tag = "2")]
    pub last: u64,
}

impl Sync {
    /// The primary's configuration, checked as a shard register is.
    ///
    /// # Errors
    ///
    /// Why the configuration is not a valid shard register.
    pub fn configuration(&self) -> Result<ShardConfig, String> {
        ShardConfig::from_json(&self.config).map_err(|error| error.to_string())
    }

    /// The primary's lineage, checked.
    ///
    /// # Errors
    ///
    /// Why it is not a lineage ([`Lineage::from_parts`]).
    pub fn lineage(&self) -> Result<Lineage, String> {
        let runs = self
            .lineage
            .iter()
            .map(|run| lineage::Run {
                epoch: Epoch::new(run.epoch),
                last: Seq::new(run.last),
            })
            .collect();
        Lineage::from_parts(Seq::new(self.known_after), runs)
    }

    /// Sets the primary's lineage.
    #[must_use]
    pub fn with_lineage(mut self, lineage: &Lineage) -> Self {
        self.known_after = lineage.known_after().get();
        self.lineage = lineage
            .runs()
            .iter()
            .map(|run| Run {
                epoch: run.epoch.get(),
                last: run.last.get(),
            })
            .collect();
        self
    }
}

/// Member to primary: the answer to a [`Sync`], one frame per record the
/// member holds past the primary's last `seq`, then one marked `done`.
#[derive(Clone, PartialEq, Message)]
pub struct SyncAck {
    /// The last `seq` the member holds durably.
    #[prost(uint64, tag = "1")]
    pub last: u64,
    /// Whether this is the last frame of the answer.
    #[prost(bool, tag = "2")]
    pub done: bool,
    /// The member's epoch.
    #[prost(uint64, tag = "3")]
    pub epoch: u64,
    /// Why the member refused the session, if it did.
    #[prost(string, tag = "4")]
    pub refused: String,
}

/// Primary to member: one record, with the commit watermark.
#[derive(Clone, PartialEq, Message)]
pub struct Append {
    /// The primary's epoch.
    #[prost(uint64, tag = "1")]
    pub epoch: u64,
    /// The commit watermark.
    #[prost(uint64, tag = "2")]
    pub commit: u64,
    /// Whether the record may wait for another to start a group commit.
    #[prost(bool, tag = "3")]
    pub lazy: bool,
    /// The lease stamp, in nanoseconds on the primary's clock.
    #[prost(uint64, optional, tag = "4")]
    pub lease: Option<u64>,
}

/// Primary to member: the commit watermark and a lease stamp, while there
/// is nothing to append and at least every `lease_renew_interval` while
/// there is. The member answers with an [`AppendAck`].
#[derive(Clone, PartialEq, Message)]
pub struct Beacon {
    /// The primary's epoch.
    #[prost(uint64, tag = "1")]
    pub epoch: u64,
    /// The commit watermark.
    #[prost(uint64, tag = "2")]
    pub commit: u64,
    /// The lease stamp, in nanoseconds on the primary's clock.
    #[prost(uint64, optional, tag = "3")]
    pub lease: Option<u64>,
}

/// Member to primary: the last `seq` of the run of records it holds
/// durably, or the newer epoch it refused an append for.
#[derive(Clone, PartialEq, Message)]
pub struct AppendAck {
    /// The last `seq` the member holds durably.
    #[prost(uint64, tag = "1")]
    pub durable: u64,
    /// The member's newer epoch, if it refused the append (rule R2), or 0.
    #[prost(uint64, tag = "2")]
    pub rejected: u64,
    /// The latest lease stamp the member received in the session, if it
    /// grants a lease with this acknowledgement.
    #[prost(uint64, optional, tag = "3")]
    pub lease: Option<u64>,
}

/// A frame of `kind` with `body` and `payload`.
#[must_use]
pub fn frame(kind: MessageKind, body: &impl Message, payload: Bytes) -> Frame {
    Frame::new(Header::new(kind).with_body(body.encode_to_vec()), payload)
}

/// Decodes the body of `frame`, which must be of `kind`.
///
/// # Errors
///
/// Why the frame is of another kind or its body does not decode.
pub fn body<M: Message + Default>(frame: &Frame, kind: MessageKind) -> Result<M, String> {
    if frame.header.kind != kind {
        return Err(format!(
            "expected a {kind:?} message, got {:?}",
            frame.header.kind
        ));
    }
    M::decode(frame.header.body.clone()).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use skys3_types::EpochSeq;

    use super::*;

    proptest! {
        /// Whatever a peer sends, decoding never panics, and an append's
        /// fields come back as they were sent.
        #[test]
        fn bodies_from_any_bytes(body in proptest::collection::vec(any::<u8>(), 0..256),
                                 epoch: u64, commit: u64, lazy: bool,
                                 lease: Option<u64>) {
            let frame = Frame::new(Header::new(MessageKind::Sync).with_body(body.clone()), "");
            if let Ok(sync) = super::body::<Sync>(&frame, MessageKind::Sync) {
                let _ = sync.configuration();
            }
            for kind in [MessageKind::SyncAck, MessageKind::Append, MessageKind::AppendAck,
                         MessageKind::Beacon] {
                let frame = Frame::new(Header::new(kind).with_body(body.clone()), "");
                let _ = super::body::<SyncAck>(&frame, kind);
                let _ = super::body::<Append>(&frame, kind);
                let _ = super::body::<AppendAck>(&frame, kind);
                let _ = super::body::<Beacon>(&frame, kind);
            }
            let append = Append { epoch, commit, lazy, lease };
            let sent = super::frame(MessageKind::Append, &append, Bytes::new());
            prop_assert_eq!(super::body::<Append>(&sent, MessageKind::Append), Ok(append));
            let beacon = Beacon { epoch, commit, lease };
            let sent = super::frame(MessageKind::Beacon, &beacon, Bytes::new());
            prop_assert_eq!(super::body::<Beacon>(&sent, MessageKind::Beacon), Ok(beacon));
        }
    }

    #[test]
    fn bodies_round_trip_through_frames() {
        let sync = Sync {
            config: b"{}".to_vec(),
            primary_last: 7,
            sequencing: 2,
            ..Sync::default()
        };

        assert!(sync.configuration().is_err());
        // No lineage: refused.
        assert!(sync.lineage().is_err());
        let mut lineage = Lineage::new(EpochSeq::new(Epoch::new(1), Seq::new(3)));
        lineage.push(EpochSeq::new(Epoch::new(2), Seq::new(7)));
        let sync = sync.with_lineage(&lineage);
        assert_eq!(sync.lineage(), Ok(lineage));
        let sent = frame(MessageKind::Sync, &sync, Bytes::new());
        assert_eq!(body::<Sync>(&sent, MessageKind::Sync), Ok(sync));
        let error = body::<SyncAck>(&sent, MessageKind::SyncAck).unwrap_err();
        assert!(error.contains("expected a SyncAck"), "{error}");

        let ack = AppendAck {
            durable: 9,
            rejected: 0,
            lease: Some(0),
        };
        let frame = frame(MessageKind::AppendAck, &ack, Bytes::from_static(b"x"));
        assert_eq!(body::<AppendAck>(&frame, MessageKind::AppendAck), Ok(ack));
        assert_eq!(frame.payload, Bytes::from_static(b"x"));

        let mut broken = frame.clone();
        broken.header.body = Bytes::from_static(&[0xff]);
        assert!(body::<AppendAck>(&broken, MessageKind::AppendAck).is_err());
    }
}
