//! Epochs, sequence numbers, log positions, and configuration generations.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Why a string was rejected as a number or a log position.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ParseNumberError {
    /// The string is not a canonical decimal `u64`.
    #[error("{value:?} is not a canonical decimal number from 0 to 18446744073709551615")]
    NotCanonical {
        /// The rejected string.
        value: String,
    },
    /// The string is not of the form `<epoch>.<seq>`.
    #[error("{value:?} is not of the form <epoch>.<seq>")]
    NotEpochSeq {
        /// The rejected string.
        value: String,
    },
}

/// Parses the canonical decimal form of a `u64`: ASCII digits only, no sign,
/// and no leading zeros except in `"0"` itself.
///
/// Rejecting every other spelling keeps one text form per value, so encoded
/// identities compare equal exactly when their values do.
pub(crate) fn parse_canonical_u64(s: &str) -> Result<u64, ParseNumberError> {
    let canonical =
        !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) && (s == "0" || !s.starts_with('0'));
    canonical
        .then(|| s.parse().ok())
        .flatten()
        .ok_or_else(|| ParseNumberError::NotCanonical {
            value: s.to_owned(),
        })
}

/// Defines a `u64` counter newtype with the common trait impls.
macro_rules! counter {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(u64);

        impl $name {
            /// The smallest value, 0.
            pub const ZERO: Self = Self(0);
            /// The largest value, `u64::MAX`.
            pub const MAX: Self = Self(u64::MAX);

            /// Wraps a raw value.
            #[must_use]
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            /// The raw value.
            #[must_use]
            pub const fn get(self) -> u64 {
                self.0
            }

            /// The next value, or `None` at [`Self::MAX`].
            #[must_use]
            pub const fn checked_next(self) -> Option<Self> {
                match self.0.checked_add(1) {
                    Some(next) => Some(Self(next)),
                    None => None,
                }
            }
        }

        impl From<u64> for $name {
            fn from(value: u64) -> Self {
                Self(value)
            }
        }

        impl From<$name> for u64 {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }

        impl FromStr for $name {
            type Err = ParseNumberError;

            /// Parses the canonical decimal form that [`Display`](fmt::Display)
            /// writes.
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                parse_canonical_u64(s).map(Self)
            }
        }
    };
}

counter! {
    /// A shard configuration epoch (§4.1, §6.3).
    ///
    /// Every configuration change writes epoch `e + 1` with a compare-and-swap
    /// over epoch `e`. Members reject appends stamped with an epoch older than
    /// the newest one they have seen (rule R2).
    Epoch
}

counter! {
    /// A per-shard log sequence number, assigned by the primary (§5.1).
    Seq
}

counter! {
    /// The configuration generation in `cluster.json` (§6.2). Every
    /// control-store change the coordinator makes increments it.
    Generation
}

/// A position in a shard's log: the `(epoch, seq)` of a record.
///
/// Positions order by epoch first, then by sequence number, which is the
/// order reconciliation compares members' logs in (§6.6). The text form is
/// `<epoch>.<seq>`, the tail of a write identity (§7.2).
///
/// ```
/// use skys3_types::{Epoch, EpochSeq, Seq};
///
/// let a = EpochSeq::new(Epoch::new(42), Seq::new(100));
/// let b = EpochSeq::new(Epoch::new(43), Seq::new(7));
/// assert!(a < b);
/// assert_eq!(a.to_string(), "42.100");
/// assert_eq!("42.100".parse::<EpochSeq>()?, a);
/// # Ok::<(), skys3_types::ParseNumberError>(())
/// ```
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct EpochSeq {
    /// The epoch the record was appended in.
    pub epoch: Epoch,
    /// The record's sequence number.
    pub seq: Seq,
}

impl EpochSeq {
    /// The longest text form: two 20-digit numbers and a `.`.
    pub const MAX_TEXT_LEN: usize = 41;

    /// Pairs an epoch and a sequence number.
    #[must_use]
    pub const fn new(epoch: Epoch, seq: Seq) -> Self {
        Self { epoch, seq }
    }
}

impl fmt::Display for EpochSeq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.epoch, self.seq)
    }
}

impl FromStr for EpochSeq {
    type Err = ParseNumberError;

    /// Parses `<epoch>.<seq>`, with both numbers in canonical decimal form.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let not_epoch_seq = || ParseNumberError::NotEpochSeq {
            value: s.to_owned(),
        };
        let (epoch, seq) = s.split_once('.').ok_or_else(not_epoch_seq)?;
        let epoch = parse_canonical_u64(epoch).map_err(|_| not_epoch_seq())?;
        let seq = parse_canonical_u64(seq).map_err(|_| not_epoch_seq())?;
        Ok(Self::new(Epoch(epoch), Seq(seq)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_decimal_accepts_exactly_one_spelling() {
        assert_eq!(parse_canonical_u64("0"), Ok(0));
        assert_eq!(parse_canonical_u64("42"), Ok(42));
        assert_eq!(parse_canonical_u64("18446744073709551615"), Ok(u64::MAX));
        for bad in [
            "",
            "00",
            "042",
            "+1",
            "-1",
            " 1",
            "1 ",
            "1_000",
            "18446744073709551616",
            "１",
        ] {
            assert_eq!(
                parse_canonical_u64(bad),
                Err(ParseNumberError::NotCanonical {
                    value: bad.to_owned()
                }),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn counters_step_and_convert() {
        assert_eq!(Epoch::ZERO.checked_next(), Some(Epoch::new(1)));
        assert_eq!(Epoch::MAX.checked_next(), None);
        assert_eq!(Seq::new(7).get(), 7);
        assert_eq!(u64::from(Generation::from(9)), 9);
        assert_eq!(Seq::default(), Seq::ZERO);
        assert_eq!("12".parse::<Generation>(), Ok(Generation::new(12)));
        assert!("012".parse::<Seq>().is_err());
        assert_eq!(Epoch::MAX.to_string(), "18446744073709551615");
    }

    #[test]
    fn counters_serialize_as_json_numbers() {
        assert_eq!(serde_json::to_string(&Epoch::new(42)).unwrap(), "42");
        assert_eq!(serde_json::from_str::<Seq>("100").unwrap(), Seq::new(100));
        assert!(serde_json::from_str::<Seq>("-1").is_err());
        assert!(serde_json::from_str::<Seq>("\"1\"").is_err());
    }

    #[test]
    fn positions_order_by_epoch_then_seq() {
        let pos = |e, s| EpochSeq::new(Epoch::new(e), Seq::new(s));
        assert!(pos(1, 900) < pos(2, 0));
        assert!(pos(2, 0) < pos(2, 1));
        assert_eq!(pos(3, 3).max(pos(3, 2)), pos(3, 3));
    }

    #[test]
    fn positions_parse_only_their_canonical_form() {
        assert_eq!(
            "0.0".parse::<EpochSeq>(),
            Ok(EpochSeq::new(Epoch::ZERO, Seq::ZERO))
        );
        let max = EpochSeq::new(Epoch::MAX, Seq::MAX);
        assert_eq!(max.to_string().len(), EpochSeq::MAX_TEXT_LEN);
        assert_eq!(max.to_string().parse::<EpochSeq>(), Ok(max));
        for bad in ["", "1", "1.", ".1", "1.2.3", "01.2", "1.02", "1,2", "a.b"] {
            assert_eq!(
                bad.parse::<EpochSeq>(),
                Err(ParseNumberError::NotEpochSeq {
                    value: bad.to_owned()
                }),
                "{bad:?}"
            );
        }
        assert_eq!(
            "x".parse::<EpochSeq>().unwrap_err().to_string(),
            "\"x\" is not of the form <epoch>.<seq>"
        );
    }

    #[test]
    fn positions_serialize_as_objects() {
        let pos = EpochSeq::new(Epoch::new(4), Seq::new(5));
        let json = serde_json::to_string(&pos).unwrap();
        assert_eq!(json, r#"{"epoch":4,"seq":5}"#);
        assert_eq!(serde_json::from_str::<EpochSeq>(&json).unwrap(), pos);
        assert!(serde_json::from_str::<EpochSeq>(r#"{"epoch":4,"seq":5,"x":1}"#).is_err());
    }
}
