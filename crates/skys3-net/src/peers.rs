//! [`Peers`]: where the other nodes of a cluster are.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard};

use skys3_types::{NodeAddress, NodeId};

/// The transport address of each node of a cluster, as the services that
/// connect to other nodes look them up: the replication links of shard
/// primaries, their backfills, and a gateway's forwarded requests.
///
/// The set may change while they run. A node of a cluster in the binary
/// learns the addresses from the registrations under `nodes/` (§6.7) and
/// replaces them as they change ([`Peers::replace`]); a service looks an
/// address up each time it connects, so a node that registers later is
/// reached from then on. A fixed set, such as a simulation's, converts
/// from a map ([`Peers::from`]).
///
/// Clones share the set.
#[derive(Clone, Default)]
pub struct Peers {
    addresses: Arc<RwLock<BTreeMap<NodeId, NodeAddress>>>,
}

impl Peers {
    /// An empty set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn read(&self) -> RwLockReadGuard<'_, BTreeMap<NodeId, NodeAddress>> {
        // Every update replaces whole values, so the map is consistent.
        self.addresses
            .read()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The address of `node`, if it is known.
    #[must_use]
    pub fn get(&self, node: &NodeId) -> Option<NodeAddress> {
        self.read().get(node).cloned()
    }

    /// Every node and its address, in node order.
    #[must_use]
    pub fn all(&self) -> BTreeMap<NodeId, NodeAddress> {
        self.read().clone()
    }

    /// How many nodes have an address.
    #[must_use]
    pub fn len(&self) -> usize {
        self.read().len()
    }

    /// Whether no node has an address.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.read().is_empty()
    }

    /// Sets the address of `node`, and returns whether it changed.
    pub fn insert(&self, node: NodeId, address: NodeAddress) -> bool {
        let mut addresses = self
            .addresses
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        addresses.insert(node, address.clone()) != Some(address)
    }

    /// Replaces the whole set with `addresses`, and returns whether it
    /// changed.
    pub fn replace(&self, addresses: BTreeMap<NodeId, NodeAddress>) -> bool {
        let mut current = self
            .addresses
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let changed = *current != addresses;
        *current = addresses;
        changed
    }
}

impl From<BTreeMap<NodeId, NodeAddress>> for Peers {
    fn from(addresses: BTreeMap<NodeId, NodeAddress>) -> Self {
        Self {
            addresses: Arc::new(RwLock::new(addresses)),
        }
    }
}

impl fmt::Debug for Peers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.read().iter()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(name: &str) -> NodeId {
        NodeId::new(name).unwrap()
    }

    fn address(text: &str) -> NodeAddress {
        text.parse().unwrap()
    }

    #[test]
    fn clones_share_one_set_that_changes_as_nodes_register() {
        let peers = Peers::new();
        assert!(peers.is_empty());
        let shared = peers.clone();
        assert!(peers.insert(node("n1"), address("10.0.0.1:7400")));
        assert!(!peers.insert(node("n1"), address("10.0.0.1:7400")));
        assert_eq!(shared.get(&node("n1")), Some(address("10.0.0.1:7400")));
        assert_eq!(shared.get(&node("n2")), None);

        let mut moved = BTreeMap::new();
        moved.insert(node("n1"), address("10.0.0.9:7400"));
        moved.insert(node("n2"), address("10.0.0.2:7400"));
        assert!(shared.replace(moved.clone()));
        assert!(!shared.replace(moved.clone()));
        assert_eq!(peers.all(), moved);
        assert_eq!(peers.len(), 2);
        let shown = format!("{peers:?}");
        assert!(shown.contains("n1") && shown.contains("n2"), "{shown}");
    }

    #[test]
    fn a_fixed_set_converts_from_a_map() {
        let mut fixed = BTreeMap::new();
        fixed.insert(node("a"), address("127.0.0.1:1"));
        let peers = Peers::from(fixed.clone());
        assert_eq!(peers.all(), fixed);
        assert!(!peers.is_empty());
    }
}
