//! Message kinds, the classes they belong to, and which roles may send
//! each class.
//!
//! The kinds follow the protocol of design §5 and §6 as modeled in
//! `spec/ShardProtocol.tla`. This crate carries them and checks who sends
//! them; the layers that own a kind define its body (a `prost` message in
//! [`Header::body`](crate::Header::body)) and its payload, and check
//! shard-level roles such as "only the primary of this epoch appends".

use crate::identity::Role;

/// What a frame carries. The numeric values are part of the wire format:
/// a value is never reused for another meaning.
///
/// Kinds are grouped by [`MessageClass`] in blocks of 16 values, so a
/// class can grow without renumbering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum MessageKind {
    /// Not a valid kind; a frame that carries it is refused.
    Unspecified = 0,

    /// Primary to member or learner: one log record at `(epoch, seq)`,
    /// with the commit watermark (§5.1). The payload is the encoded record.
    Append = 1,
    /// Member or learner to primary: durable up to a `seq`, or the newer
    /// epoch that made it refuse the append (rule R2).
    AppendAck = 2,
    /// Primary to member: collect the member's last `(epoch, seq)` and the
    /// prefix it shares with the primary's log, for reconciliation (§6.6)
    /// and for verifying a re-admitted learner's old records (§6.7).
    Sync = 3,
    /// Member to primary: the answer to [`MessageKind::Sync`], sent after
    /// the member has made its `TRUNCATE` durable.
    SyncAck = 4,
    /// Primary to learner: a chunk of the shard's index snapshot or
    /// payload during backfill (§6.4, §6.7). The payload is the chunk.
    Backfill = 5,
    /// Learner to primary: a backfill chunk is durable.
    BackfillAck = 6,

    /// Primary to member or learner: a lease beacon sent at a primary-local
    /// time, carrying the commit watermark as a heartbeat (§5.4).
    Beacon = 16,
    /// Member or learner to primary: the acknowledgement that grants the
    /// lease for that beacon.
    BeaconAck = 17,
    /// Old primary to candidate: the durable step-down of a planned
    /// handoff, naming its epoch and last `seq` (§5.4).
    StepDown = 18,

    /// Gateway to shard primary: a client request with the shard epoch the
    /// gateway knows (§5.1, step 1). The payload is the request body.
    Forward = 32,
    /// Shard primary to gateway: the result of a forwarded request, or a
    /// redirect hint naming a newer configuration.
    ForwardReply = 33,

    /// Node to coordinator: a liveness heartbeat, advice only (§6.7).
    NodeHeartbeat = 48,
    /// A hint that control-store registers changed, so the receiver reads
    /// them before its next poll (§6.2).
    ControlChanged = 49,
    /// Coordinator or operator tool to a primary: hand the primary role of
    /// a shard to a named member by planned handoff (§6.7).
    Handoff = 50,
    /// The answer to an admin message.
    AdminReply = 51,
}

/// A group of message kinds that the same roles may send.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MessageClass {
    /// Log replication, reconciliation, and backfill (§5.1, §6.4, §6.6).
    Replication,
    /// Leases and planned handoff (§5.4).
    Lease,
    /// Client requests a gateway forwards to a shard primary (§5.1).
    Request,
    /// Cluster administration: heartbeats to the coordinator, change hints,
    /// and placement commands (§6.2, §6.7).
    Admin,
}

impl MessageKind {
    /// The class of the kind, or `None` for [`MessageKind::Unspecified`].
    #[must_use]
    pub const fn class(self) -> Option<MessageClass> {
        Some(match self {
            Self::Unspecified => return None,
            Self::Append
            | Self::AppendAck
            | Self::Sync
            | Self::SyncAck
            | Self::Backfill
            | Self::BackfillAck => MessageClass::Replication,
            Self::Beacon | Self::BeaconAck | Self::StepDown => MessageClass::Lease,
            Self::Forward | Self::ForwardReply => MessageClass::Request,
            Self::NodeHeartbeat | Self::ControlChanged | Self::Handoff | Self::AdminReply => {
                MessageClass::Admin
            }
        })
    }
}

impl Role {
    /// Whether a peer with this role may send messages of `class`.
    ///
    /// A node may send every class. An operator tool may send only admin
    /// messages, so a stolen tool certificate cannot append records or
    /// grant leases.
    #[must_use]
    pub const fn may_send(self, class: MessageClass) -> bool {
        match self {
            Self::Node => true,
            Self::Admin => matches!(class, MessageClass::Admin),
        }
    }

    /// Whether a peer with this role may send `kind`. No role may send
    /// [`MessageKind::Unspecified`].
    #[must_use]
    pub const fn may_send_kind(self, kind: MessageKind) -> bool {
        match kind.class() {
            Some(class) => self.may_send(class),
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [MessageKind; 16] = [
        MessageKind::Unspecified,
        MessageKind::Append,
        MessageKind::AppendAck,
        MessageKind::Sync,
        MessageKind::SyncAck,
        MessageKind::Backfill,
        MessageKind::BackfillAck,
        MessageKind::Beacon,
        MessageKind::BeaconAck,
        MessageKind::StepDown,
        MessageKind::Forward,
        MessageKind::ForwardReply,
        MessageKind::NodeHeartbeat,
        MessageKind::ControlChanged,
        MessageKind::Handoff,
        MessageKind::AdminReply,
    ];

    #[test]
    fn kinds_are_grouped_by_class_in_blocks_of_sixteen() {
        for kind in ALL {
            let block = kind as i32 / 16;
            let expected = match block {
                0 if kind == MessageKind::Unspecified => None,
                0 => Some(MessageClass::Replication),
                1 => Some(MessageClass::Lease),
                2 => Some(MessageClass::Request),
                3 => Some(MessageClass::Admin),
                _ => panic!("{kind:?} is outside the known blocks"),
            };
            assert_eq!(kind.class(), expected, "{kind:?}");
            assert_eq!(MessageKind::try_from(kind as i32), Ok(kind));
        }
        assert!(MessageKind::try_from(7).is_err());
    }

    #[test]
    fn admins_may_send_only_admin_messages() {
        for kind in ALL {
            let class = kind.class();
            assert_eq!(Role::Node.may_send_kind(kind), class.is_some(), "{kind:?}");
            assert_eq!(
                Role::Admin.may_send_kind(kind),
                class == Some(MessageClass::Admin),
                "{kind:?}"
            );
        }
    }
}
