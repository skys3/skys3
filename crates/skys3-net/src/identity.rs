//! Who a peer is: the identity its certificate binds, as a SPIFFE ID.
//!
//! Every certificate of the cluster PKI names its holder in exactly one URI
//! subject alternative name of the form
//!
//! ```text
//! spiffe://<cluster-id>/<role>/<name>
//! ```
//!
//! The trust domain is the cluster ID, so a certificate issued for another
//! cluster under the same CA is refused. The role is `node` for a SkyS3
//! node, whose name is its node ID, or `admin` for an operator tool, whose
//! name is a [`Label`]. The role decides which messages the holder may send
//! (see [`MessageClass`](crate::MessageClass)).

use std::fmt;

use skys3_types::{ClusterId, IdError, Label, NodeId};

/// The URI scheme and separator of a SPIFFE ID.
const SPIFFE_SCHEME: &str = "spiffe://";

/// What a certificate's holder is allowed to be in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Role {
    /// A SkyS3 node: it serves the transport and may send every message
    /// kind. Shard-level roles (primary, member, learner, coordinator)
    /// change at run time and are checked by the protocol, not here.
    Node,
    /// An operator tool: it connects to nodes and may send only admin
    /// messages.
    Admin,
}

impl Role {
    /// The role's path segment in a SPIFFE ID.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Admin => "admin",
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The authenticated identity of a peer, from its verified certificate.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PeerIdentity {
    /// A node, by its node ID.
    Node(NodeId),
    /// An operator tool, by the name its certificate gives.
    Admin(Label),
}

impl PeerIdentity {
    /// The holder's role.
    #[must_use]
    pub const fn role(&self) -> Role {
        match self {
            Self::Node(_) => Role::Node,
            Self::Admin(_) => Role::Admin,
        }
    }

    /// The node ID, if the holder is a node.
    #[must_use]
    pub const fn node_id(&self) -> Option<&NodeId> {
        match self {
            Self::Node(id) => Some(id),
            Self::Admin(_) => None,
        }
    }

    /// The name the identity carries: a node ID or an admin label.
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::Node(id) => id.as_str(),
            Self::Admin(label) => label.as_str(),
        }
    }

    /// The SPIFFE ID that names this identity in `cluster`.
    ///
    /// ```
    /// use skys3_net::PeerIdentity;
    /// use skys3_types::{ClusterId, NodeId};
    ///
    /// let cluster = ClusterId::new("prod-a")?;
    /// let node = PeerIdentity::Node(NodeId::new("node-3")?);
    /// let uri = node.spiffe_id(&cluster);
    /// assert_eq!(uri, "spiffe://prod-a/node/node-3");
    /// assert_eq!(PeerIdentity::from_spiffe_id(&uri, &cluster)?, node);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn spiffe_id(&self, cluster: &ClusterId) -> String {
        format!(
            "{SPIFFE_SCHEME}{cluster}/{}/{}",
            self.role().as_str(),
            self.name()
        )
    }

    /// Parses a SPIFFE ID of `cluster`.
    ///
    /// # Errors
    ///
    /// [`IdentityError`] if `uri` is not a SPIFFE ID, names another trust
    /// domain, or does not have exactly the path `/<role>/<name>` with a
    /// known role and a valid name.
    pub fn from_spiffe_id(uri: &str, cluster: &ClusterId) -> Result<Self, IdentityError> {
        let invalid = |reason| IdentityError::Malformed {
            uri: truncated(uri),
            reason,
        };
        let rest = uri
            .strip_prefix(SPIFFE_SCHEME)
            .ok_or_else(|| invalid("not a spiffe:// URI"))?;
        let (domain, path) = rest
            .split_once('/')
            .ok_or_else(|| invalid("no path after the trust domain"))?;
        if domain != cluster.as_str() {
            return Err(IdentityError::WrongCluster {
                uri: truncated(uri),
                expected: cluster.clone(),
            });
        }
        let (role, name) = path
            .split_once('/')
            .ok_or_else(|| invalid("the path is not /<role>/<name>"))?;
        let identity = match role {
            "node" => NodeId::new(name).map(Self::Node),
            "admin" => Label::new(name).map(Self::Admin),
            _ => return Err(invalid("the role is neither node nor admin")),
        };
        identity.map_err(|source| IdentityError::InvalidName {
            uri: truncated(uri),
            source,
        })
    }

    /// Finds the identity in a certificate's URI subject alternative names:
    /// exactly one must be a SPIFFE ID, and it must be one of `cluster`.
    /// URIs of other schemes are ignored.
    ///
    /// # Errors
    ///
    /// [`IdentityError::Missing`] or [`IdentityError::Ambiguous`] unless
    /// exactly one URI is a SPIFFE ID, and the errors of
    /// [`PeerIdentity::from_spiffe_id`].
    pub fn from_uri_names<'a>(
        uris: impl IntoIterator<Item = &'a str>,
        cluster: &ClusterId,
    ) -> Result<Self, IdentityError> {
        let mut spiffe = uris
            .into_iter()
            .filter(|uri| uri.starts_with(SPIFFE_SCHEME));
        let first = spiffe.next().ok_or(IdentityError::Missing)?;
        if spiffe.next().is_some() {
            return Err(IdentityError::Ambiguous);
        }
        Self::from_spiffe_id(first, cluster)
    }
}

impl fmt::Display for PeerIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.role(), self.name())
    }
}

/// Why a certificate's identity was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum IdentityError {
    /// The certificate has no `spiffe://` URI subject alternative name.
    #[error("the certificate has no spiffe:// URI subject alternative name")]
    Missing,
    /// The certificate has more than one SPIFFE ID.
    #[error("the certificate has more than one spiffe:// URI subject alternative name")]
    Ambiguous,
    /// The SPIFFE ID's trust domain is not this cluster's ID.
    #[error("{uri:?} is not an identity of cluster {expected}")]
    WrongCluster {
        /// The SPIFFE ID, truncated for display.
        uri: String,
        /// The cluster ID the trust domain must equal.
        expected: ClusterId,
    },
    /// The URI is not a SPIFFE ID of the form this cluster uses.
    #[error("{uri:?} is not a SkyS3 identity: {reason}")]
    Malformed {
        /// The URI, truncated for display.
        uri: String,
        /// What is wrong with it.
        reason: &'static str,
    },
    /// The identity has a role the context does not accept, such as an
    /// operator tool's certificate presented by a server.
    #[error("a peer with role {found} is not accepted here; expected {expected}")]
    WrongRole {
        /// The role the certificate names.
        found: Role,
        /// The role required.
        expected: Role,
    },
    /// The name in the SPIFFE ID is not a valid node ID or label.
    #[error("{uri:?} has an invalid name: {source}")]
    InvalidName {
        /// The SPIFFE ID, truncated for display.
        uri: String,
        /// Why the name is invalid.
        source: IdError,
    },
}

/// At most 128 bytes of `uri`, cut at a character boundary, so a hostile
/// certificate cannot fill logs.
fn truncated(uri: &str) -> String {
    const MAX: usize = 128;
    if uri.len() <= MAX {
        return uri.to_owned();
    }
    let mut end = MAX;
    while !uri.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &uri[..end])
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn valid_identities_round_trip(admin in any::<bool>(), name in "[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?") {
            let identity = if admin {
                PeerIdentity::Admin(Label::new(name).unwrap())
            } else {
                PeerIdentity::Node(NodeId::new(name).unwrap())
            };
            let uri = identity.spiffe_id(&cluster());
            prop_assert_eq!(PeerIdentity::from_spiffe_id(&uri, &cluster()).unwrap(), identity);
        }

        #[test]
        fn accepted_ids_are_canonical(uri in "(spiffe://prod-a/(node|admin)/)?[a-z0-9/A-Z.:%-]{0,20}") {
            if let Ok(identity) = PeerIdentity::from_spiffe_id(&uri, &cluster()) {
                prop_assert_eq!(identity.spiffe_id(&cluster()), uri);
            }
        }
    }

    fn cluster() -> ClusterId {
        ClusterId::new("prod-a").unwrap()
    }

    #[test]
    fn round_trips_both_roles() {
        let node = PeerIdentity::Node(NodeId::new("n1").unwrap());
        let admin = PeerIdentity::Admin(Label::new("ops-cli").unwrap());
        for identity in [node, admin] {
            let uri = identity.spiffe_id(&cluster());
            assert_eq!(
                PeerIdentity::from_spiffe_id(&uri, &cluster()).unwrap(),
                identity
            );
        }
        let admin = PeerIdentity::from_spiffe_id("spiffe://prod-a/admin/ops", &cluster()).unwrap();
        assert_eq!(admin.role(), Role::Admin);
        assert_eq!(admin.node_id(), None);
        assert_eq!(admin.name(), "ops");
        assert_eq!(admin.to_string(), "admin ops");
        assert_eq!(Role::Node.to_string(), "node");
    }

    #[test]
    fn refuses_other_clusters_and_malformed_ids() {
        let err = PeerIdentity::from_spiffe_id("spiffe://prod-b/node/n1", &cluster()).unwrap_err();
        assert!(matches!(err, IdentityError::WrongCluster { .. }), "{err}");
        for bad in [
            "https://prod-a/node/n1",
            "spiffe://prod-a",
            "spiffe://prod-a/node",
            "spiffe://prod-a/gateway/n1",
            "spiffe://prod-a/Node/n1",
        ] {
            let err = PeerIdentity::from_spiffe_id(bad, &cluster()).unwrap_err();
            assert!(
                matches!(err, IdentityError::Malformed { .. }),
                "{bad}: {err}"
            );
        }
        for bad in [
            "spiffe://prod-a/node/n1/extra",
            "spiffe://prod-a/node/",
            "spiffe://prod-a/node/N1",
            "spiffe://prod-a/admin/a?b",
            "spiffe://prod-a/node/n1#x",
        ] {
            let err = PeerIdentity::from_spiffe_id(bad, &cluster()).unwrap_err();
            assert!(
                matches!(err, IdentityError::InvalidName { .. }),
                "{bad}: {err}"
            );
        }
        // The trust domain is compared exactly: no case folding, no port.
        for bad in ["spiffe://PROD-A/node/n1", "spiffe://prod-a:1/node/n1"] {
            let err = PeerIdentity::from_spiffe_id(bad, &cluster()).unwrap_err();
            assert!(matches!(err, IdentityError::WrongCluster { .. }), "{bad}");
        }
    }

    #[test]
    fn a_certificate_has_exactly_one_spiffe_id() {
        let c = cluster();
        let found =
            PeerIdentity::from_uri_names(["https://example.com/x", "spiffe://prod-a/node/n2"], &c)
                .unwrap();
        assert_eq!(found, PeerIdentity::Node(NodeId::new("n2").unwrap()));
        assert_eq!(
            PeerIdentity::from_uri_names(["https://example.com/x"], &c),
            Err(IdentityError::Missing)
        );
        assert_eq!(
            PeerIdentity::from_uri_names(
                ["spiffe://prod-a/node/n2", "spiffe://prod-a/admin/ops"],
                &c
            ),
            Err(IdentityError::Ambiguous)
        );
    }

    #[test]
    fn long_uris_are_truncated_in_errors() {
        let long = format!("spiffe://prod-b/{}", "é".repeat(200));
        let IdentityError::WrongCluster { uri, .. } =
            PeerIdentity::from_spiffe_id(&long, &cluster()).unwrap_err()
        else {
            panic!("expected WrongCluster");
        };
        assert!(uri.len() <= 131 && uri.ends_with("..."), "{uri}");
    }
}
