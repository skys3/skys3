//! Where re-indexing finds fragment headers: every node of the cluster.

use std::collections::BTreeMap;
use std::future::Future;

use skys3_io::Disk;
use skys3_log::record::ShardRef;
use skys3_types::{FragmentLocation, NodeId};

use crate::layout::FoundFragment;
use crate::store::FragmentError;
use crate::transfer::FragmentServer;

/// Why a node's fragment headers could not be listed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the fragment headers of node {node} cannot be listed: {reason}")]
pub struct HeaderError {
    /// The node.
    pub node: NodeId,
    /// Why.
    pub reason: String,
}

/// Lists the headers of the fragments a node holds of a shard.
///
/// Fragments may be on any eligible node, not only on the shard's members
/// (§8.3), and repairs and moves after the latest snapshot put them on
/// nodes its layouts never named, so a drill asks every node of the
/// cluster. A node's own fragment servers answer with
/// [`FragmentServer::found`]; a map from node to server lists each node's.
pub trait HeaderSource: Send + Sync {
    /// The fragments `node` holds of `shard`, with their headers.
    fn headers(
        &self,
        node: &NodeId,
        shard: &ShardRef,
    ) -> impl Future<Output = Result<Vec<FoundFragment>, HeaderError>> + Send;
}

impl<D: Disk> FragmentServer<D> {
    /// The fragments of `shard` that this server's stores hold, with their
    /// headers, located on `node`, this server's node. A fragment whose
    /// header fails its checksum is left out, as a read finds it missing.
    ///
    /// # Errors
    ///
    /// [`FragmentError::OutOfService`] if a store's disk is out of service:
    /// its fragments cannot be listed.
    pub async fn found(
        &self,
        node: &NodeId,
        shard: &ShardRef,
    ) -> Result<Vec<FoundFragment>, FragmentError> {
        let mut found = Vec::new();
        for store in self.stores() {
            for id in store.ids() {
                let header = match store.header(id).await {
                    Ok(header) => header,
                    Err(error @ FragmentError::OutOfService(_)) => return Err(error),
                    Err(error) => {
                        tracing::warn!(%node, %id, %error, "a fragment header is left out");
                        continue;
                    }
                };
                if header.shard == *shard {
                    found.push(FoundFragment {
                        location: FragmentLocation {
                            node: node.clone(),
                            fragment: id,
                        },
                        header,
                    });
                }
            }
        }
        Ok(found)
    }
}

impl<D: Disk> HeaderSource for BTreeMap<NodeId, FragmentServer<D>> {
    async fn headers(
        &self,
        node: &NodeId,
        shard: &ShardRef,
    ) -> Result<Vec<FoundFragment>, HeaderError> {
        let server = self.get(node).ok_or_else(|| HeaderError {
            node: node.clone(),
            reason: "no fragment server is known for it".to_owned(),
        })?;
        server
            .found(node, shard)
            .await
            .map_err(|error| HeaderError {
                node: node.clone(),
                reason: error.to_string(),
            })
    }
}
