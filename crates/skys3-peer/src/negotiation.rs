//! Protocol versions, capabilities, and what two `HELLO`s agree on.
//!
//! Each end sends one `HELLO` when a connection opens: its cluster, the
//! range of protocol versions it speaks, and its capabilities. Each end
//! then computes [`negotiate`] from its own `HELLO` and the peer's. The
//! result does not depend on which end computes it, so the two agree
//! without a further round trip: the highest version both speak, and the
//! capabilities both have.

use std::fmt;

use skys3_types::ClusterId;

use crate::message::{Hello, Message};

/// The protocol version this implementation speaks best.
pub const PROTOCOL_VERSION: u16 = 2;

/// The first protocol version whose `COMMIT`s and `BATCH` items must
/// carry an apply-by time ([`Commit::apply_by_ms`](crate::Commit), §7.8).
/// A source that falls back to S3 REST relies on it to bound when a
/// `COMMIT` it sent can still apply, so no session is older.
pub const APPLY_BY_VERSION: u16 = 2;

/// The protocol versions this implementation speaks: only those whose
/// commits carry an apply-by time. Version 1 was never released.
pub const SUPPORTED_VERSIONS: VersionRange = VersionRange {
    min: APPLY_BY_VERSION,
    max: PROTOCOL_VERSION,
};

/// An inclusive range of protocol versions, `min..=max`, with
/// `1 <= min <= max`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VersionRange {
    min: u16,
    max: u16,
}

impl VersionRange {
    /// The versions `min..=max`, or `None` unless `1 <= min <= max`.
    #[must_use]
    pub const fn new(min: u16, max: u16) -> Option<Self> {
        if min >= 1 && min <= max {
            Some(Self { min, max })
        } else {
            None
        }
    }

    /// The oldest version in the range.
    #[must_use]
    pub const fn min(self) -> u16 {
        self.min
    }

    /// The newest version in the range.
    #[must_use]
    pub const fn max(self) -> u16 {
        self.max
    }

    /// Whether `version` is in the range.
    #[must_use]
    pub const fn contains(self, version: u16) -> bool {
        self.min <= version && version <= self.max
    }
}

impl fmt::Display for VersionRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}..={}", self.min, self.max)
    }
}

/// Optional protocol features, as a set of bits. Bits this
/// implementation does not know are kept, so a set round-trips, but they
/// mean nothing to it: a feature is used only when both ends have it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Capabilities(u64);

impl Capabilities {
    /// No capabilities.
    pub const NONE: Self = Self(0);

    /// The destination accepts `BATCH` (plan M6-05). Without it, a source
    /// sends small objects as it sends large ones.
    pub const BATCH: Self = Self(1);

    /// Every capability this implementation knows.
    pub const KNOWN: Self = Self::BATCH;

    /// The capabilities of `bits`, known or not.
    #[must_use]
    pub const fn from_bits(bits: u64) -> Self {
        Self(bits)
    }

    /// The set's bits.
    #[must_use]
    pub const fn bits(self) -> u64 {
        self.0
    }

    /// Whether the set holds every capability of `other`.
    #[must_use]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    /// The capabilities in both sets.
    #[must_use]
    pub const fn intersection(self, other: Self) -> Self {
        Self(self.0 & other.0)
    }

    /// The capabilities in either set.
    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

/// The two ends of a peer connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    /// The cluster that flushes: it opens the connection and sends objects.
    Source,
    /// The cluster that receives the objects.
    Destination,
}

/// What two `HELLO`s agree on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    /// The version every later message is in: the highest both speak.
    pub version: u16,
    /// The capabilities both ends have and this implementation knows.
    pub capabilities: Capabilities,
    /// The peer's cluster, as its `HELLO` names it. The transport checks it
    /// against the peer's certificate (plan M6-02).
    pub peer: ClusterId,
}

/// Why two `HELLO`s agree on no version.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("no common protocol version: this end speaks {local}, the peer {remote}")]
pub struct NegotiationError {
    /// The versions this end speaks.
    pub local: VersionRange,
    /// The versions the peer speaks.
    pub remote: VersionRange,
}

/// What `local`'s and `remote`'s `HELLO`s agree on: the highest version
/// both speak, and the capabilities both have. The version and
/// capabilities do not depend on which `HELLO` is which.
///
/// ```
/// use skys3_peer::{Capabilities, Hello, VersionRange, negotiate};
/// use skys3_types::ClusterId;
///
/// let ours = Hello {
///     cluster: ClusterId::new("prod-us")?,
///     versions: VersionRange::new(1, 3).unwrap(),
///     capabilities: Capabilities::BATCH,
/// };
/// let theirs = Hello {
///     cluster: ClusterId::new("prod-eu")?,
///     versions: VersionRange::new(2, 5).unwrap(),
///     capabilities: Capabilities::NONE,
/// };
/// let session = negotiate(&ours, &theirs)?;
/// assert_eq!(session.version, 3);
/// assert_eq!(session.capabilities, Capabilities::NONE);
/// assert_eq!(session.peer.as_str(), "prod-eu");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// # Errors
///
/// [`NegotiationError`] if the version ranges do not overlap.
pub fn negotiate(local: &Hello, remote: &Hello) -> Result<Session, NegotiationError> {
    let version = local.versions.max.min(remote.versions.max);
    if version < local.versions.min.max(remote.versions.min) {
        return Err(NegotiationError {
            local: local.versions,
            remote: remote.versions,
        });
    }
    Ok(Session {
        version,
        capabilities: local
            .capabilities
            .intersection(remote.capabilities)
            .intersection(Capabilities::KNOWN),
        peer: remote.cluster.clone(),
    })
}

/// Why a message is not allowed in a session.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ProtocolError {
    /// The message is sent only once, before the session exists.
    #[error("HELLO is sent once, when the connection opens")]
    RepeatedHello,
    /// The other side sends this message.
    #[error("a {side:?} does not send {message}")]
    WrongSide {
        /// The side that sent it.
        side: Side,
        /// The message's name.
        message: &'static str,
    },
    /// The message needs a capability the session lacks.
    #[error("{message} needs a capability the peers did not agree on")]
    NotNegotiated {
        /// The message's name.
        message: &'static str,
    },
    /// A `COMMIT` or `BATCH` item without the apply-by time its session's
    /// version requires (§7.8).
    #[error("{message} carries no apply-by time")]
    NoApplyBy {
        /// The message's name.
        message: &'static str,
    },
}

impl Session {
    /// Whether `sender` may send `message` in this session.
    ///
    /// # Errors
    ///
    /// [`ProtocolError`] for a second `HELLO`, a message from the wrong
    /// side, a `BATCH` without [`Capabilities::BATCH`], or, from
    /// [`APPLY_BY_VERSION`] on, a `COMMIT` or `BATCH` item without an
    /// apply-by time.
    pub fn check(&self, message: &Message, sender: Side) -> Result<(), ProtocolError> {
        let name = message.name();
        let from = match message {
            Message::Hello(_) => return Err(ProtocolError::RepeatedHello),
            Message::Abort(_) => return Ok(()),
            Message::Begin(_) | Message::Data(_) | Message::Commit(_) | Message::Batch(_) => {
                Side::Source
            }
            Message::Durable(_) | Message::Resume(_) | Message::Applied(_) => Side::Destination,
        };
        if from != sender {
            return Err(ProtocolError::WrongSide {
                side: sender,
                message: name,
            });
        }
        if matches!(message, Message::Batch(_)) && !self.capabilities.contains(Capabilities::BATCH)
        {
            return Err(ProtocolError::NotNegotiated { message: name });
        }
        let unbounded = match message {
            Message::Commit(commit) => commit.apply_by_ms.is_none(),
            Message::Batch(batch) => batch.items.iter().any(|item| item.apply_by_ms.is_none()),
            _ => false,
        };
        if unbounded && self.version >= APPLY_BY_VERSION {
            return Err(ProtocolError::NoApplyBy { message: name });
        }
        Ok(())
    }
}
