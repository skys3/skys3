//! Node addresses: the `host:port` other nodes connect to.

use std::fmt;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::num::NonZeroU16;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::position::parse_canonical_u64;

/// Why a string was rejected as a node address.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum AddressError {
    /// The address is longer than [`NodeAddress::MAX_LEN`].
    #[error("node address is {0} bytes long; the limit is {max}", max = NodeAddress::MAX_LEN)]
    TooLong(usize),
    /// The address has no `:port`.
    #[error("node address {0:?} has no port; expected host:port or [ipv6]:port")]
    MissingPort(String),
    /// The port is not a canonical decimal number from 1 to 65535.
    #[error("node address has an invalid port {0:?}; it must be from 1 to 65535")]
    InvalidPort(String),
    /// The host is not a DNS name, an IPv4 address, or a bracketed IPv6
    /// address.
    #[error("node address has an invalid host {host:?}: {reason}")]
    InvalidHost {
        /// The rejected host.
        host: String,
        /// What is wrong with it.
        reason: &'static str,
    },
}

/// A DNS host name: dot-separated labels of lowercase ASCII letters, digits,
/// and `-`, each 1 to 63 bytes and not starting or ending with `-`, at most
/// [`DnsName::MAX_LEN`] bytes in all, with no trailing dot. The last label
/// is not all digits, so a malformed IPv4 address is never taken for a name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DnsName(String);

impl DnsName {
    /// The longest name, in bytes.
    pub const MAX_LEN: usize = 253;

    /// Validates a DNS name.
    ///
    /// # Errors
    ///
    /// Returns [`AddressError::InvalidHost`] naming the rule `name` breaks.
    pub fn new(name: impl Into<String>) -> Result<Self, AddressError> {
        let name = name.into();
        let invalid = |reason| AddressError::InvalidHost {
            host: name.clone(),
            reason,
        };
        if name.is_empty() {
            return Err(invalid("the host is empty"));
        }
        if name.len() > Self::MAX_LEN {
            return Err(invalid("a DNS name is at most 253 bytes"));
        }
        for label in name.split('.') {
            if label.is_empty() || label.len() > 63 {
                return Err(invalid("each DNS label must be 1 to 63 bytes"));
            }
            if !label
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            {
                return Err(invalid(
                    "a DNS name has only lowercase ASCII letters, digits, '-', and '.'",
                ));
            }
            if label.starts_with('-') || label.ends_with('-') {
                return Err(invalid("a DNS label must not start or end with '-'"));
            }
        }
        if name
            .rsplit('.')
            .next()
            .is_some_and(|last| last.bytes().all(|b| b.is_ascii_digit()))
        {
            return Err(invalid("not an IPv4 address, and not a DNS name"));
        }
        Ok(Self(name))
    }

    /// The name as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DnsName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for DnsName {
    type Err = AddressError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

/// The host part of a [`NodeAddress`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Host {
    /// A DNS name.
    Dns(DnsName),
    /// An IPv4 address in dotted-decimal form.
    Ipv4(Ipv4Addr),
    /// An IPv6 address, written in brackets.
    Ipv6(Ipv6Addr),
}

impl Host {
    /// Parses a host written without brackets: an IPv4 address or a DNS
    /// name.
    fn parse_unbracketed(host: &str) -> Result<Self, AddressError> {
        if host.contains(':') {
            return Err(AddressError::InvalidHost {
                host: host.to_owned(),
                reason: "an IPv6 address must be in brackets",
            });
        }
        match host.parse() {
            Ok(ip) => Ok(Self::Ipv4(ip)),
            Err(_) => DnsName::new(host).map(Self::Dns),
        }
    }
}

impl fmt::Display for Host {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Dns(name) => fmt::Display::fmt(name, f),
            Self::Ipv4(ip) => write!(f, "{ip}"),
            Self::Ipv6(ip) => write!(f, "[{ip}]"),
        }
    }
}

/// The `host:port` a node is reached at by other nodes (§6.1).
///
/// The host is a DNS name, an IPv4 address, or an IPv6 address in brackets;
/// the port is required and from 1 to 65535. The text form is canonical:
/// DNS names are lowercase, ports have no leading zeros, and IPv6 addresses
/// are written in their RFC 5952 form, to which parsing normalizes them.
///
/// ```
/// use skys3_types::NodeAddress;
///
/// let address: NodeAddress = "node-3.example.internal:7400".parse()?;
/// assert_eq!(address.port(), 7400);
/// assert_eq!("[0:0::1]:7000".parse::<NodeAddress>()?.to_string(), "[::1]:7000");
/// assert!("not-an-address".parse::<NodeAddress>().is_err());
/// # Ok::<(), skys3_types::AddressError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeAddress {
    host: Host,
    port: NonZeroU16,
}

impl NodeAddress {
    /// The longest text form accepted, in bytes: a 253-byte DNS name, `:`,
    /// and a 5-digit port.
    pub const MAX_LEN: usize = DnsName::MAX_LEN + 6;

    /// Pairs a host and a port.
    #[must_use]
    pub const fn new(host: Host, port: NonZeroU16) -> Self {
        Self { host, port }
    }

    /// The host.
    #[must_use]
    pub const fn host(&self) -> &Host {
        &self.host
    }

    /// The port.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port.get()
    }
}

impl fmt::Display for NodeAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl FromStr for NodeAddress {
    type Err = AddressError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() > Self::MAX_LEN {
            return Err(AddressError::TooLong(s.len()));
        }
        let (host, port) = if let Some(bracketed) = s.strip_prefix('[') {
            let (literal, rest) =
                bracketed
                    .split_once(']')
                    .ok_or_else(|| AddressError::InvalidHost {
                        host: s.to_owned(),
                        reason: "a '[' must be closed by ']'",
                    })?;
            let port = rest
                .strip_prefix(':')
                .ok_or_else(|| AddressError::MissingPort(s.to_owned()))?;
            let ip = literal.parse().map_err(|_| AddressError::InvalidHost {
                host: format!("[{literal}]"),
                reason: "not an IPv6 address",
            })?;
            (Host::Ipv6(ip), port)
        } else {
            // The port follows the last ':'.
            let (host, port) = s
                .rsplit_once(':')
                .ok_or_else(|| AddressError::MissingPort(s.to_owned()))?;
            (Host::parse_unbracketed(host)?, port)
        };
        let port = parse_canonical_u64(port)
            .ok()
            .and_then(|n| u16::try_from(n).ok())
            .and_then(NonZeroU16::new)
            .ok_or_else(|| AddressError::InvalidPort(port.to_owned()))?;
        Ok(Self::new(host, port))
    }
}

impl Serialize for NodeAddress {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for NodeAddress {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Result<NodeAddress, AddressError> {
        s.parse()
    }

    #[test]
    fn addresses_accept_each_host_kind() {
        let dns = parse("node-3.example.internal:7400").unwrap();
        assert_eq!(
            dns.host(),
            &Host::Dns(DnsName::new("node-3.example.internal").unwrap())
        );
        assert_eq!(DnsName::new("a.b").unwrap().as_str(), "a.b");
        assert_eq!("a.b".parse::<DnsName>().unwrap().to_string(), "a.b");
        assert_eq!(dns.port(), 7400);
        assert_eq!(dns.to_string(), "node-3.example.internal:7400");

        let v4 = parse("10.0.3.17:1").unwrap();
        assert_eq!(v4.host(), &Host::Ipv4(Ipv4Addr::new(10, 0, 3, 17)));
        assert_eq!(v4.to_string(), "10.0.3.17:1");

        let v6 = parse("[::1]:65535").unwrap();
        assert_eq!(v6.host(), &Host::Ipv6(Ipv6Addr::LOCALHOST));
        assert_eq!(v6.to_string(), "[::1]:65535");

        assert_eq!(parse("localhost:80").unwrap().to_string(), "localhost:80");
        assert_eq!(parse("a1:80").unwrap().to_string(), "a1:80");
        let longest = format!(
            "{}.{}:65535",
            vec!["b".repeat(63); 3].join("."),
            "c".repeat(61)
        );
        assert_eq!(longest.len(), NodeAddress::MAX_LEN);
        assert!(parse(&longest).is_ok());
    }

    #[test]
    fn ipv6_addresses_are_normalized() {
        assert_eq!(parse("[0:0::1]:7000").unwrap().to_string(), "[::1]:7000");
        assert_eq!(
            parse("[2001:DB8::1]:7000").unwrap().to_string(),
            "[2001:db8::1]:7000"
        );
    }

    #[test]
    fn addresses_need_a_valid_port() {
        for bad in [
            "not-an-address",
            "10.0.0.1",
            "[::1]",
            "[::1]80",
            "[::1]x:80",
        ] {
            assert_eq!(
                parse(bad),
                Err(AddressError::MissingPort(bad.into())),
                "{bad:?}"
            );
        }
        for port in ["", "0", "65536", "080", "+80", "8o"] {
            assert_eq!(
                parse(&format!("host:{port}")),
                Err(AddressError::InvalidPort(port.into())),
                "{port:?}"
            );
        }
    }

    #[test]
    fn addresses_reject_malformed_hosts() {
        let reason = |s: &str| match parse(s) {
            Err(AddressError::InvalidHost { reason, .. }) => reason,
            other => panic!("{s:?}: {other:?}"),
        };
        assert_eq!(reason(":80"), "the host is empty");
        assert_eq!(reason("::1:80"), "an IPv6 address must be in brackets");
        assert_eq!(reason("[1.2.3.4]:80"), "not an IPv6 address");
        assert_eq!(reason("[::1:80"), "a '[' must be closed by ']'");
        assert_eq!(reason("::1"), "an IPv6 address must be in brackets");
        assert_eq!(reason("a..b:80"), "each DNS label must be 1 to 63 bytes");
        assert_eq!(reason("a.:80"), "each DNS label must be 1 to 63 bytes");
        assert_eq!(
            reason(&format!("{}:80", "a".repeat(64))),
            "each DNS label must be 1 to 63 bytes"
        );
        assert_eq!(
            reason(&format!(
                "{}.{}:80",
                vec!["a".repeat(63); 3].join("."),
                "c".repeat(62)
            )),
            "a DNS name is at most 253 bytes"
        );
        for bad in ["Node:80", "a_b:80", "a b:80", "é:80"] {
            assert!(reason(bad).starts_with("a DNS name has only"), "{bad:?}");
        }
        assert_eq!(
            reason("-a:80"),
            "a DNS label must not start or end with '-'"
        );
        assert_eq!(
            reason("a.b-:80"),
            "a DNS label must not start or end with '-'"
        );
        for bad in ["1.2.3.256:80", "1.2.3:80", "01.2.3.4:80", "123:80"] {
            assert_eq!(
                reason(bad),
                "not an IPv4 address, and not a DNS name",
                "{bad:?}"
            );
        }
    }

    #[test]
    fn addresses_bound_their_length() {
        let long = format!("{}:1", "a".repeat(NodeAddress::MAX_LEN));
        assert_eq!(parse(&long), Err(AddressError::TooLong(long.len())));
        assert_eq!(
            AddressError::TooLong(300).to_string(),
            "node address is 300 bytes long; the limit is 259"
        );
    }

    #[test]
    fn addresses_serialize_as_strings() {
        let address = NodeAddress::new(Host::Ipv6(Ipv6Addr::LOCALHOST), NonZeroU16::MIN);
        let json = serde_json::to_string(&address).unwrap();
        assert_eq!(json, "\"[::1]:1\"");
        assert_eq!(serde_json::from_str::<NodeAddress>(&json).unwrap(), address);
        let err = serde_json::from_str::<NodeAddress>("\"not-an-address\"").unwrap_err();
        assert!(err.to_string().contains("has no port"), "{err}");
    }
}
