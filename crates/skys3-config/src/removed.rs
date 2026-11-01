//! Keys that earlier builds read and this one no longer does.
//!
//! Plan M8-09 removed the flags that gated the cluster in the node binary:
//! `[cluster] multi_node`, `[cluster] coordinator`, and `[ec] enabled`.
//! What they chose now follows from the configuration itself: a node over
//! an etcd or S3 control store is one of a cluster's nodes, every node of a
//! cluster may be elected coordinator, and every node of a cluster runs
//! erasure coding, which codes objects only while `ec.min_eligible_nodes`
//! nodes are eligible (§6.2, §6.7, §8.2).
//!
//! A configuration that still sets one is refused, as one with any unknown
//! key is (§14), but with the reason and what to do instead, rather than
//! serde's list of the keys the table accepts. It is never ignored with a
//! warning: an old `multi_node = false` over etcd would then start a node
//! of a cluster, and `[ec] enabled = false` would start coding, both
//! against what the file says.

use serde::Deserialize;
use serde::de::IgnoredAny;

use crate::error::Checker;

/// The removed keys, as dotted paths, each with why it is gone and what to
/// do instead.
pub(crate) const REMOVED_KEYS: [(&str, &str); 3] = [
    (
        "cluster.multi_node",
        "was removed: a node over an etcd or S3 control store is always one of the cluster's \
         nodes, and a node over the file store a single node (§6.2); delete the key, and use \
         backend = \"file\" for a single node",
    ),
    (
        "cluster.coordinator",
        "was removed: every node of a cluster may be elected coordinator (§6.7); delete the key",
    ),
    (
        "ec.enabled",
        "was removed: every node of a cluster runs erasure coding, which codes the objects of \
         local buckets only while at least ec.min_eligible_nodes nodes are eligible (§8.2); \
         delete the key, and keep min_eligible_nodes above the cluster's node count to keep \
         objects replicated",
    ),
];

/// The tables that held removed keys, read leniently: any other key, and a
/// removed key of any type, is accepted here, since only whether a removed
/// key is present matters.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct Tables {
    cluster: ClusterTable,
    ec: EcTable,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct ClusterTable {
    multi_node: Option<IgnoredAny>,
    coordinator: Option<IgnoredAny>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct EcTable {
    enabled: Option<IgnoredAny>,
}

/// Reports every removed key `text` sets. Text that is not TOML, or whose
/// `[cluster]` or `[ec]` is not a table, reports nothing: parsing the
/// whole configuration then says what is wrong with it.
pub(crate) fn check(text: &str, checker: &mut Checker) {
    let Ok(tables) = toml::from_str::<Tables>(text) else {
        return;
    };
    let set = [
        tables.cluster.multi_node.is_some(),
        tables.cluster.coordinator.is_some(),
        tables.ec.enabled.is_some(),
    ];
    for ((key, reason), set) in REMOVED_KEYS.iter().zip(set) {
        if set {
            checker.report(*key, reason);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reported(text: &str) -> Vec<String> {
        let mut checker = Checker::default();
        check(text, &mut checker);
        match checker.finish() {
            Ok(()) => Vec::new(),
            Err(violations) => violations.into_iter().map(|v| v.key).collect(),
        }
    }

    #[test]
    fn each_removed_key_is_reported_whatever_its_value() {
        assert_eq!(
            reported(
                "[cluster]\ncluster_id = \"c\"\nmulti_node = false\ncoordinator = \"yes\"\n\
                 [ec]\nenabled = 1\nparity_fragments = 2"
            ),
            ["cluster.multi_node", "cluster.coordinator", "ec.enabled"]
        );
        assert_eq!(
            reported("[cluster]\nmulti_node = true"),
            ["cluster.multi_node"]
        );
        assert_eq!(reported("[ec]\nenabled = false"), ["ec.enabled"]);
    }

    #[test]
    fn other_keys_and_malformed_text_report_nothing() {
        assert!(reported("[cluster]\ncluster_id = \"c\"\n[ec]\nparity_fragments = 1").is_empty());
        assert!(reported("").is_empty());
        assert!(reported("[cluster\nmulti_node = true").is_empty());
        assert!(reported("cluster = 3\nec = \"x\"").is_empty());
        // A key of that name elsewhere is not the removed one.
        assert!(reported("[node]\nenabled = true\n[gateway]\ncoordinator = 1").is_empty());
    }
}
