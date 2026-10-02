//! Rebuilding a lost control store from the nodes' local copies (design
//! §6.2, §6.9).
//!
//! Every node keeps what it uses of the control state: each replica the
//! configuration of its shard's newest `CONFIG` record, and each node the
//! registers under `buckets/` and `identity/`. If the store is lost, an
//! operator rebuilds it from those copies. Nothing does so automatically:
//! a node that rebuilt a store it merely could not reach would give the
//! cluster a second referee (§6.10).
//!
//! 1. Every node stops, and each exports what it keeps as a
//!    [`ControlExport`] (`skys3 control export`). The export recovers the
//!    node's logs first, so it holds the newest `CONFIG` record of each
//!    shard the node has durably, applied or not: a node makes a
//!    configuration's record durable before it acts on it.
//! 2. [`RebuildPlan::new`] merges the exports and refuses what it cannot
//!    rebuild safely.
//! 3. [`RebuildPlan::apply`] writes the plan into the empty store, with
//!    `cluster.json` last.
//! 4. The nodes start again, and sync from the rebuilt store.
//!
//! What the plan holds, and why:
//!
//! - **Shard registers hold the newest configuration, unchanged.** Of
//!   every configuration the exports hold for a shard, the one with the
//!   highest epoch is written as it is, `proposal_id` included, so each
//!   replica finds its register equal to its own configuration and adopts
//!   nothing. A configuration that landed in the lost store after it was
//!   acted on by no node is not rebuilt, and nothing needs it: its epoch
//!   was never used, and a proposal still outstanding for it is sent
//!   again over the rebuilt register like any other. Two different
//!   configurations of one epoch cannot both have landed, so the plan
//!   refuses them.
//! - **Every member of a rebuilt configuration exported.** The primary of
//!   any later configuration was a member of the one before it (rule R1),
//!   or its primary, and appended that configuration's record before it
//!   acted, so a configuration newer than every export would show on a
//!   member. A member whose disks are lost for good can be declared lost
//!   ([`RebuildOptions::lost`]) instead; it must never start again with
//!   its old data directory.
//! - **Buckets and identity from the newest copy.** Each node's copy is a
//!   whole listing at its generation, so the copy with the highest
//!   generation is the newest whole state, deletions included; merging
//!   copies would bring back deleted roles and buckets. Its registers are
//!   written byte for byte. Copies of the newest generation that differ
//!   saw a write no increment had announced yet, and nothing in them
//!   shows which saw the later state: a sync's start time is read from
//!   the node's own clock, and a sync that started later can still have
//!   listed a register earlier. So the plan refuses them, naming the
//!   copies and the registers they differ in, until the operator chooses
//!   one ([`RebuildOptions::prefer`]). Shards that hold objects but belong to no
//!   bucket of that copy would be dropped by their nodes (§4.1), so the
//!   plan refuses them unless [`RebuildOptions::allow_unnamed`] says so.
//! - **Generations move forward.** `cluster.json` is written at the
//!   generation after the newest copy's, so every exported node sees it
//!   move and syncs, and a node never follows a store backwards
//!   ([`ControlError::GenerationBehind`]).
//!
//! [`RebuildPlan::apply`] writes only into a store that holds neither
//! `cluster.json` nor a bucket, shard, or identity register the plan does
//! not write: a store that answers with them is not lost. Node
//! registrations and a coordinator lease that nodes wrote while the store
//! was lost are left alone, since nodes register again when they start.
//! Every register is created with `If-None-Match: *` and checked byte for
//! byte when it exists, and `cluster.json`, whose `proposal_id` derives
//! from the plan, goes last: until it lands, nodes treat the store as
//! reset, and an interrupted rebuild run again with the same exports
//! completes it.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use skys3_types::{
    BucketDocument, BucketId, BucketName, ClusterDocument, ClusterId, Epoch, Generation, NodeId,
    ProposalId, RegisterDocument, ShardConfig, ShardCount, ShardId,
};

use crate::key::{KeyPrefix, RegisterKey};
use crate::propose::{RetryPolicy, get_with_retries};
use crate::store::{ControlError, ControlStore, Expected, PutOutcome};

/// The format of a [`ControlExport`] this build writes and reads.
pub const EXPORT_FORMAT: u32 = 1;

/// What one stopped node keeps of the control state, for a rebuild.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlExport {
    /// [`EXPORT_FORMAT`].
    pub format: u32,
    /// The node's cluster.
    pub cluster_id: ClusterId,
    /// The node.
    pub node_id: NodeId,
    /// The instance ID of the node's data directory, which the file
    /// backend records as its owner.
    pub instance_id: String,
    /// The node's copy of the registers under `buckets/` and `identity/`,
    /// or `None` if it never synced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub copy: Option<ExportedCopy>,
    /// The configuration of each shard's newest `CONFIG` record the node
    /// holds.
    #[serde(default)]
    pub configs: Vec<ShardConfig>,
    /// The shards the node holds state of.
    #[serde(default)]
    pub held: Vec<HeldShard>,
}

impl ControlExport {
    /// Parses an export.
    ///
    /// # Errors
    ///
    /// The parser's error if `bytes` is not an export.
    pub fn from_json(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }

    /// The export as indented JSON.
    #[must_use]
    pub fn to_json(&self) -> Vec<u8> {
        // Every field serializes: keys are strings, values plain data.
        serde_json::to_vec_pretty(self).unwrap_or_default()
    }
}

/// A node's copy of the registers under `buckets/` and `identity/`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportedCopy {
    /// The generation `cluster.json` held when the copy was read.
    pub generation: Generation,
    /// When the sync that read the copy started, in milliseconds since the
    /// Unix epoch, by the node's clock. It is shown to an operator and
    /// never orders copies: the clocks of different nodes are not
    /// comparable, and a sync that started later can still have listed a
    /// register earlier.
    pub synced_at_ms: u64,
    /// Each register's value, by key, as the store held it.
    pub registers: BTreeMap<String, String>,
}

/// A shard a node holds state of.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeldShard {
    /// The shard's bucket.
    pub bucket_id: BucketId,
    /// The shard.
    pub shard: ShardId,
    /// Whether the node's index lists an object in it.
    pub objects: bool,
}

/// What an operator decides for a rebuild.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RebuildOptions {
    /// Nodes whose disks are lost for good, which export nothing. A
    /// configuration that names one as a member is rebuilt without its
    /// export. Such a node must never start again with its old data
    /// directory: it may hold records of a configuration no other node
    /// knew.
    pub lost: BTreeSet<NodeId>,
    /// Rebuild even though shards that hold objects belong to no bucket of
    /// the newest copy. Their nodes drop them at their next start (§4.1).
    pub allow_unnamed: bool,
    /// The node whose copy of the bucket and identity registers is rebuilt
    /// when the copies of the newest generation differ, which the plan
    /// otherwise refuses. Its copy must be of the newest generation: a
    /// copy of an older one misses changes a newer one announced.
    pub prefer: Option<NodeId>,
}

/// Why a rebuild was refused.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RebuildError {
    /// No export was given.
    #[error("no node's export was given")]
    NoExports,
    /// An export of another format.
    #[error(
        "the export of node {node} has format {found}; this build reads format {EXPORT_FORMAT}"
    )]
    ExportFormat {
        /// The node.
        node: NodeId,
        /// The format found.
        found: u32,
    },
    /// An export of another cluster.
    #[error("the export of node {node} belongs to cluster {found}, not {expected}")]
    ClusterMismatch {
        /// The node.
        node: NodeId,
        /// The cluster being rebuilt.
        expected: ClusterId,
        /// The export's cluster.
        found: ClusterId,
    },
    /// A node was exported twice.
    #[error("node {0} was exported twice")]
    DuplicateExport(NodeId),
    /// A node was exported and declared lost.
    #[error("node {0} was exported and declared lost")]
    ExportedAndLost(NodeId),
    /// No export holds a copy of the bucket and identity registers.
    #[error("no export holds a copy of the bucket and identity registers")]
    NoCopy,
    /// The copies of the newest generation differ, and no copy was
    /// chosen.
    #[error(
        "the copies of the bucket and identity registers at generation {generation} differ, \
         between {}, in {}: nothing shows which is newer, so choose the copy to rebuild from",
        groups(.copies), .registers.join(", ")
    )]
    DivergentCopies {
        /// The newest generation.
        generation: Generation,
        /// The nodes holding a copy of it, grouped by equal copies.
        copies: Vec<Vec<NodeId>>,
        /// The registers whose values, or presence, differ between them.
        registers: Vec<String>,
    },
    /// The chosen copy is not of the newest generation.
    #[error(
        "the chosen copy, of node {node}, {}; the newest copies are at generation {newest}, and \
         only one of those can be chosen",
        .found.map_or_else(|| "does not exist".to_owned(), |found| format!("is at generation {found}"))
    )]
    PreferredNotNewest {
        /// The chosen node.
        node: NodeId,
        /// The generation of its copy, if it exported one.
        found: Option<Generation>,
        /// The newest generation.
        newest: Generation,
    },
    /// An export holds something no node writes.
    #[error("the export of node {node} is invalid: {reason}")]
    InvalidExport {
        /// The node.
        node: NodeId,
        /// What is wrong.
        reason: String,
    },
    /// Two exports hold different configurations of one epoch of a shard.
    #[error(
        "shard register {shard} has two configurations of epoch {epoch}, from {first} and {second}"
    )]
    ConflictingConfigs {
        /// The shard's register.
        shard: RegisterKey,
        /// The epoch.
        epoch: Epoch,
        /// One node holding a configuration.
        first: NodeId,
        /// A node holding another.
        second: NodeId,
    },
    /// Members of rebuilt configurations were neither exported nor declared
    /// lost.
    #[error("members of rebuilt configurations were not exported: {}", list(.0))]
    MissingExports(BTreeMap<NodeId, Vec<RegisterKey>>),
    /// Shards hold objects but belong to no bucket of the newest copy.
    #[error(
        "shards hold objects but no bucket of the newest copy names them, so their nodes would \
         drop them: {}", keys(.0)
    )]
    UnnamedShards(Vec<RegisterKey>),
    /// The store holds `cluster.json`: it is not lost.
    #[error(
        "the control store holds cluster.json: it is not lost, or another cluster or rebuild \
         uses its prefix"
    )]
    StoreInUse,
    /// The store holds registers the rebuild does not write.
    #[error("the control store holds registers the rebuild does not write: {}", keys(.0))]
    StoreNotEmpty(Vec<RegisterKey>),
    /// A register holds another value than the one the rebuild writes.
    #[error("register {0} holds another value than the rebuild writes")]
    RegisterDiffers(RegisterKey),
    /// The store failed.
    #[error(transparent)]
    Control(#[from] ControlError),
}

fn keys(keys: &[RegisterKey]) -> String {
    let names: Vec<&str> = keys.iter().map(RegisterKey::as_str).collect();
    names.join(", ")
}

fn groups(copies: &[Vec<NodeId>]) -> String {
    let groups: Vec<String> = copies
        .iter()
        .map(|nodes| {
            let nodes: Vec<&str> = nodes.iter().map(NodeId::as_str).collect();
            format!("[{}]", nodes.join(", "))
        })
        .collect();
    groups.join(" and ")
}

fn list(missing: &BTreeMap<NodeId, Vec<RegisterKey>>) -> String {
    let nodes: Vec<String> = missing
        .iter()
        .map(|(node, shards)| format!("{node} ({} shards)", shards.len()))
        .collect();
    nodes.join(", ")
}

/// Something about a plan an operator should know, which does not stop
/// the rebuild.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RebuildNote {
    /// A node holds an older configuration of a shard than the one
    /// rebuilt; it follows the register once it starts.
    Behind {
        /// The shard's register.
        shard: RegisterKey,
        /// The node.
        node: NodeId,
        /// The epoch it holds.
        epoch: Epoch,
    },
    /// A rebuilt configuration names a member declared lost; the shard's
    /// primary removes it, or its members take over, once they run.
    LostMember {
        /// The shard's register.
        shard: RegisterKey,
        /// The member.
        node: NodeId,
    },
    /// Shards of a bucket that no export configures. On a single node,
    /// which keeps no shard registers, that is every shard; in a cluster,
    /// the coordinator places them as new, empty shards.
    Unconfigured {
        /// The bucket.
        bucket: BucketName,
        /// How many of its shards.
        shards: u32,
    },
    /// A shard of a bucket the newest copy does not name: left behind by
    /// a deleted bucket when it holds no object. It is not rebuilt.
    Unnamed {
        /// The shard's register.
        shard: RegisterKey,
        /// Whether a node lists objects in it.
        objects: bool,
    },
    /// Another copy of the same generation as the one rebuilt from holds
    /// other registers: a write between the two listings that no
    /// increment announced. The operator chose the copy rebuilt from
    /// ([`RebuildOptions::prefer`]).
    CopyDiffers {
        /// The node of the other copy.
        node: NodeId,
    },
}

impl fmt::Display for RebuildNote {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Behind { shard, node, epoch } => write!(
                f,
                "{node} holds epoch {epoch} of {shard}, older than the one rebuilt; it follows \
                 the register when it starts"
            ),
            Self::LostMember { shard, node } => write!(
                f,
                "{shard} names {node}, declared lost, as a member; the shard removes it once it runs"
            ),
            Self::Unconfigured { bucket, shards } => write!(
                f,
                "{shards} shards of bucket {bucket} have no configuration in any export; a \
                 coordinator places them as new shards"
            ),
            Self::Unnamed { shard, objects } => write!(
                f,
                "{shard} belongs to no bucket of the newest copy{}; it is not rebuilt",
                if *objects { " but holds objects" } else { "" }
            ),
            Self::CopyDiffers { node } => write!(
                f,
                "the copy of {node} has the rebuilt generation but other registers; the chosen \
                 copy is rebuilt"
            ),
        }
    }
}

/// The registers a rebuild writes, merged from the nodes' exports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebuildPlan {
    cluster_id: ClusterId,
    generation: Generation,
    copy_from: NodeId,
    registers: BTreeMap<RegisterKey, Bytes>,
    cluster: Bytes,
    notes: Vec<RebuildNote>,
}

/// One shard's newest configuration among the exports.
struct Newest {
    config: ShardConfig,
    from: NodeId,
}

impl RebuildPlan {
    /// Merges `exports`, one per stopped node, into the registers of
    /// `cluster`'s rebuilt store: every shard's newest configuration,
    /// the newest copy's bucket and identity registers, and a
    /// `cluster.json` one generation past that copy's.
    ///
    /// # Errors
    ///
    /// [`RebuildError`] for exports that cannot be rebuilt from safely.
    pub fn new(
        cluster: &ClusterId,
        exports: &[ControlExport],
        options: &RebuildOptions,
    ) -> Result<Self, RebuildError> {
        let exported = check_exports(cluster, exports, options)?;
        let (copy_from, copy, differing) = newest_copy(exports, options.prefer.as_ref())?;
        let mut notes: Vec<RebuildNote> = differing
            .into_iter()
            .map(|node| RebuildNote::CopyDiffers { node })
            .collect();
        let (mut registers, buckets) = copied_registers(&copy_from.node_id, copy)?;
        let newest = newest_configs(exports, &mut notes)?;
        let shards = shard_registers(&newest, &buckets, &exported, options, &mut notes)?;
        registers.extend(shards);
        let unnamed = unnamed_shards(exports, &newest, &buckets);
        let holding: Vec<RegisterKey> = unnamed
            .iter()
            .filter(|(_, objects)| **objects)
            .map(|(key, _)| key.clone())
            .collect();
        if !holding.is_empty() && !options.allow_unnamed {
            return Err(RebuildError::UnnamedShards(holding));
        }
        notes.extend(
            unnamed
                .into_iter()
                .map(|(shard, objects)| RebuildNote::Unnamed { shard, objects }),
        );

        let generation =
            copy.generation
                .checked_next()
                .ok_or_else(|| RebuildError::InvalidExport {
                    node: copy_from.node_id.clone(),
                    reason: "the copy's generation is at its maximum".to_owned(),
                })?;
        let document = ClusterDocument {
            cluster_id: cluster.clone(),
            format_version: ClusterDocument::FORMAT_VERSION,
            generation,
            proposal_id: derived_proposal(cluster, generation, &registers),
        };
        let cluster_json = document
            .to_json()
            .map_err(|error| ControlError::InvalidRegister {
                key: RegisterKey::cluster(),
                source: error,
            })?;
        Ok(Self {
            cluster_id: cluster.clone(),
            generation,
            copy_from: copy_from.node_id.clone(),
            registers,
            cluster: Bytes::from(cluster_json),
            notes,
        })
    }

    /// The cluster.
    #[must_use]
    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    /// The generation `cluster.json` is written at.
    #[must_use]
    pub fn generation(&self) -> Generation {
        self.generation
    }

    /// The node whose copy of the bucket and identity registers is
    /// rebuilt.
    #[must_use]
    pub fn copy_from(&self) -> &NodeId {
        &self.copy_from
    }

    /// Every register the rebuild writes before `cluster.json`, in the
    /// order it writes them.
    #[must_use]
    pub fn registers(&self) -> &BTreeMap<RegisterKey, Bytes> {
        &self.registers
    }

    /// The value of `cluster.json`, written last.
    #[must_use]
    pub fn cluster_json(&self) -> &Bytes {
        &self.cluster
    }

    /// The configurations of the shard registers the rebuild writes.
    pub fn shard_configs(&self) -> impl Iterator<Item = ShardConfig> + '_ {
        self.registers
            .iter()
            .filter(|(key, _)| key.starts_with(&KeyPrefix::shards()))
            .filter_map(|(_, value)| ShardConfig::from_json(value).ok())
    }

    /// What the operator should know about the plan.
    #[must_use]
    pub fn notes(&self) -> &[RebuildNote] {
        &self.notes
    }

    /// Writes the plan into `store`, retrying each request under `policy`,
    /// and returns what it wrote.
    ///
    /// The store must hold no `cluster.json`, and no register under
    /// `buckets/`, `shards/`, or `identity/` that the plan does not write.
    /// A register the plan writes that exists must hold the plan's value:
    /// an earlier run of the same plan wrote it. `cluster.json` goes last;
    /// a store that already holds the plan's `cluster.json` is complete.
    ///
    /// # Errors
    ///
    /// [`RebuildError::StoreInUse`], [`RebuildError::StoreNotEmpty`], or
    /// [`RebuildError::RegisterDiffers`] for a store that is not lost, and
    /// [`RebuildError::Control`] if the store fails. Running the plan again
    /// completes an interrupted rebuild.
    pub async fn apply<S: ControlStore>(
        &self,
        store: &S,
        policy: &RetryPolicy,
    ) -> Result<Applied, RebuildError> {
        let cluster = RegisterKey::cluster();
        if let Some(held) = get_with_retries(store, &cluster, policy).await? {
            if held.value == self.cluster {
                return Ok(Applied {
                    written: 0,
                    present: self.registers.len() + 1,
                });
            }
            return Err(RebuildError::StoreInUse);
        }
        let mut unexpected = Vec::new();
        for prefix in [
            KeyPrefix::buckets(),
            KeyPrefix::identity(),
            KeyPrefix::shards(),
        ] {
            let listed = retrying(policy, || store.list(&prefix)).await?;
            unexpected.extend(
                listed
                    .into_iter()
                    .map(|(key, _)| key)
                    .filter(|key| !self.registers.contains_key(key)),
            );
        }
        if !unexpected.is_empty() {
            return Err(RebuildError::StoreNotEmpty(unexpected));
        }
        let mut applied = Applied::default();
        for (key, value) in self.registers.iter().chain([(&cluster, &self.cluster)]) {
            if create(store, key, value, policy).await? {
                applied.written += 1;
            } else {
                applied.present += 1;
            }
        }
        Ok(applied)
    }
}

/// What [`RebuildPlan::apply`] did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Applied {
    /// Registers it wrote.
    pub written: usize,
    /// Registers an earlier run had written.
    pub present: usize,
}

/// Checks that every export is of `cluster` and this format, and that no
/// node is exported twice or both exported and lost. Returns the exported
/// nodes.
fn check_exports(
    cluster: &ClusterId,
    exports: &[ControlExport],
    options: &RebuildOptions,
) -> Result<BTreeSet<NodeId>, RebuildError> {
    if exports.is_empty() {
        return Err(RebuildError::NoExports);
    }
    let mut exported = BTreeSet::new();
    for export in exports {
        let node = &export.node_id;
        if export.format != EXPORT_FORMAT {
            return Err(RebuildError::ExportFormat {
                node: node.clone(),
                found: export.format,
            });
        }
        if export.cluster_id != *cluster {
            return Err(RebuildError::ClusterMismatch {
                node: node.clone(),
                expected: cluster.clone(),
                found: export.cluster_id.clone(),
            });
        }
        if !exported.insert(node.clone()) {
            return Err(RebuildError::DuplicateExport(node.clone()));
        }
        if options.lost.contains(node) {
            return Err(RebuildError::ExportedAndLost(node.clone()));
        }
    }
    Ok(exported)
}

/// The newest copy of the bucket and identity registers, and the nodes
/// whose copies of its generation differ from it.
///
/// A copy of a higher generation always wins: its listing started after
/// every change announced up to its generation. Copies of the newest
/// generation must be equal, or one of them `prefer`red: nothing orders
/// them (see [`ExportedCopy::synced_at_ms`]).
fn newest_copy<'a>(
    exports: &'a [ControlExport],
    prefer: Option<&NodeId>,
) -> Result<(&'a ControlExport, &'a ExportedCopy, Vec<NodeId>), RebuildError> {
    let copies: Vec<(&ControlExport, &ExportedCopy)> = exports
        .iter()
        .filter_map(|export| export.copy.as_ref().map(|copy| (export, copy)))
        .collect();
    let generation = copies
        .iter()
        .map(|(_, copy)| copy.generation)
        .max()
        .ok_or(RebuildError::NoCopy)?;
    let mut newest: Vec<(&ControlExport, &ExportedCopy)> = copies
        .iter()
        .copied()
        .filter(|(_, copy)| copy.generation == generation)
        .collect();
    newest.sort_by(|(a, _), (b, _)| a.node_id.cmp(&b.node_id));
    let chosen = match prefer {
        Some(node) => *newest
            .iter()
            .find(|(export, _)| export.node_id == *node)
            .ok_or_else(|| RebuildError::PreferredNotNewest {
                node: node.clone(),
                found: copies
                    .iter()
                    .find(|(export, _)| export.node_id == *node)
                    .map(|(_, copy)| copy.generation),
                newest: generation,
            })?,
        // Every copy is of `generation`'s; the lowest node ID, so that every
        // run of equal copies names the same node.
        None => newest[0],
    };
    let differing: Vec<NodeId> = newest
        .iter()
        .filter(|(_, copy)| copy.registers != chosen.1.registers)
        .map(|(export, _)| export.node_id.clone())
        .collect();
    if prefer.is_none() && !differing.is_empty() {
        return Err(divergent(generation, &newest));
    }
    Ok((chosen.0, chosen.1, differing))
}

/// The refusal of `copies`, all of `generation`, which differ.
fn divergent(generation: Generation, copies: &[(&ControlExport, &ExportedCopy)]) -> RebuildError {
    let mut groups: Vec<(&BTreeMap<String, String>, Vec<NodeId>)> = Vec::new();
    for (export, copy) in copies {
        let node = export.node_id.clone();
        match groups
            .iter_mut()
            .find(|(registers, _)| **registers == copy.registers)
        {
            Some((_, nodes)) => nodes.push(node),
            None => groups.push((&copy.registers, vec![node])),
        }
    }
    let keys: BTreeSet<&String> = groups
        .iter()
        .flat_map(|(registers, _)| registers.keys())
        .collect();
    let registers = keys
        .into_iter()
        .filter(|key| {
            let first = groups[0].0.get(*key);
            groups
                .iter()
                .any(|(registers, _)| registers.get(*key) != first)
        })
        .cloned()
        .collect();
    RebuildError::DivergentCopies {
        generation,
        copies: groups.into_iter().map(|(_, nodes)| nodes).collect(),
        registers,
    }
}

/// Each shard's newest configuration among the exports, noting the nodes
/// that hold older ones.
fn newest_configs(
    exports: &[ControlExport],
    notes: &mut Vec<RebuildNote>,
) -> Result<BTreeMap<(BucketId, ShardId), Newest>, RebuildError> {
    let mut newest: BTreeMap<(BucketId, ShardId), Newest> = BTreeMap::new();
    let mut held: Vec<((BucketId, ShardId), NodeId, Epoch)> = Vec::new();
    for export in exports {
        for config in &export.configs {
            config
                .validate()
                .map_err(|error| RebuildError::InvalidExport {
                    node: export.node_id.clone(),
                    reason: format!(
                        "the configuration of {}: {error}",
                        RegisterKey::shard(&config.bucket_id, config.shard)
                    ),
                })?;
            let slot = (config.bucket_id.clone(), config.shard);
            held.push((slot.clone(), export.node_id.clone(), config.epoch));
            match newest.get(&slot) {
                Some(known) if known.config.epoch > config.epoch => {}
                Some(known) if known.config.epoch == config.epoch => {
                    if known.config != *config {
                        return Err(RebuildError::ConflictingConfigs {
                            shard: RegisterKey::shard(&config.bucket_id, config.shard),
                            epoch: config.epoch,
                            first: known.from.clone(),
                            second: export.node_id.clone(),
                        });
                    }
                }
                _ => {
                    newest.insert(
                        slot,
                        Newest {
                            config: config.clone(),
                            from: export.node_id.clone(),
                        },
                    );
                }
            }
        }
    }
    for ((bucket, shard), node, epoch) in held {
        if newest
            .get(&(bucket.clone(), shard))
            .is_some_and(|newest| newest.config.epoch > epoch)
        {
            let shard = RegisterKey::shard(&bucket, shard);
            notes.push(RebuildNote::Behind { shard, node, epoch });
        }
    }
    Ok(newest)
}

/// The buckets of a copy: each bucket's name and shard count, by ID.
type Buckets = BTreeMap<BucketId, (BucketName, ShardCount)>;

/// The bucket and identity registers of `copy`, the copy of node `from`,
/// and the buckets they name.
fn copied_registers(
    from: &NodeId,
    copy: &ExportedCopy,
) -> Result<(BTreeMap<RegisterKey, Bytes>, Buckets), RebuildError> {
    let invalid = |reason: String| RebuildError::InvalidExport {
        node: from.clone(),
        reason,
    };
    let mut registers = BTreeMap::new();
    let mut buckets = Buckets::new();
    for (key, value) in &copy.registers {
        let key = RegisterKey::new(key.as_str()).map_err(|error| invalid(error.to_string()))?;
        if key.starts_with(&KeyPrefix::buckets()) {
            let bucket = BucketDocument::from_json(value.as_bytes())
                .map_err(|error| invalid(format!("{key}: {error}")))?;
            if key != RegisterKey::bucket(&bucket.name) {
                return Err(invalid(format!("{key} holds bucket {}", bucket.name)));
            }
            buckets.insert(bucket.bucket_id, (bucket.name, bucket.shards));
        } else if !key.starts_with(&KeyPrefix::identity()) {
            return Err(invalid(format!(
                "{key} is not a bucket or identity register"
            )));
        }
        registers.insert(key, Bytes::from(value.clone()));
    }
    Ok((registers, buckets))
}

/// The shard registers of `buckets`: each shard's newest configuration,
/// whose members must all be `exported` or declared lost. Notes members
/// declared lost, and buckets with shards no export configures.
fn shard_registers(
    newest: &BTreeMap<(BucketId, ShardId), Newest>,
    buckets: &Buckets,
    exported: &BTreeSet<NodeId>,
    options: &RebuildOptions,
    notes: &mut Vec<RebuildNote>,
) -> Result<BTreeMap<RegisterKey, Bytes>, RebuildError> {
    let mut registers = BTreeMap::new();
    let mut missing: BTreeMap<NodeId, Vec<RegisterKey>> = BTreeMap::new();
    let mut configured: BTreeMap<&BucketId, u32> = BTreeMap::new();
    for ((bucket, shard), newest) in newest {
        let key = RegisterKey::shard(bucket, *shard);
        let Some((_, count)) = buckets.get(bucket) else {
            continue;
        };
        if !count.contains(*shard) {
            return Err(RebuildError::InvalidExport {
                node: newest.from.clone(),
                reason: format!("{key} is past the bucket's {} shards", count.get()),
            });
        }
        *configured.entry(bucket).or_default() += 1;
        for member in &newest.config.members {
            if exported.contains(member) {
                continue;
            }
            if options.lost.contains(member) {
                notes.push(RebuildNote::LostMember {
                    shard: key.clone(),
                    node: member.clone(),
                });
            } else {
                missing.entry(member.clone()).or_default().push(key.clone());
            }
        }
        let value = newest
            .config
            .to_json()
            .map_err(|error| RebuildError::InvalidExport {
                node: newest.from.clone(),
                reason: format!("{key}: {error}"),
            })?;
        registers.insert(key, Bytes::from(value));
    }
    if !missing.is_empty() {
        return Err(RebuildError::MissingExports(missing));
    }
    for (bucket, (name, count)) in buckets {
        let unconfigured = count.get() - configured.get(bucket).copied().unwrap_or(0);
        if unconfigured > 0 {
            notes.push(RebuildNote::Unconfigured {
                bucket: name.clone(),
                shards: unconfigured,
            });
        }
    }
    Ok(registers)
}

/// The shards the exports hold state or configurations of whose bucket is
/// not in `buckets`, with whether any node lists objects in them.
fn unnamed_shards(
    exports: &[ControlExport],
    newest: &BTreeMap<(BucketId, ShardId), Newest>,
    buckets: &Buckets,
) -> BTreeMap<RegisterKey, bool> {
    let mut unnamed: BTreeMap<RegisterKey, bool> = BTreeMap::new();
    for (bucket, shard) in newest.keys() {
        if !buckets.contains_key(bucket) {
            unnamed
                .entry(RegisterKey::shard(bucket, *shard))
                .or_default();
        }
    }
    for held in exports.iter().flat_map(|export| &export.held) {
        if !buckets.contains_key(&held.bucket_id) {
            *unnamed
                .entry(RegisterKey::shard(&held.bucket_id, held.shard))
                .or_default() |= held.objects;
        }
    }
    unnamed
}

/// The `proposal_id` of the rebuilt `cluster.json`: a digest of the plan,
/// so that running the same plan again writes the same `cluster.json`, and
/// a store that holds it is known to be complete.
fn derived_proposal(
    cluster: &ClusterId,
    generation: Generation,
    registers: &BTreeMap<RegisterKey, Bytes>,
) -> ProposalId {
    let mut digest = Sha256::new();
    digest.update(b"skys3 control-store rebuild\0");
    digest.update(cluster.as_str().as_bytes());
    digest.update(generation.get().to_be_bytes());
    for (key, value) in registers {
        digest.update((key.as_str().len() as u64).to_be_bytes());
        digest.update(key.as_str().as_bytes());
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value);
    }
    let digest = digest.finalize();
    let mut first = [0; 16];
    first.copy_from_slice(&digest[..16]);
    ProposalId::from_u128(u128::from_be_bytes(first))
}

/// Creates `key` with `value` under `If-None-Match: *`, and returns whether
/// this call wrote it: `false` if the register already holds `value`, from
/// an attempt whose answer was lost or an earlier run.
///
/// An attempt that got no answer is sent again until one is answered.
/// Once the register exists, a late attempt can no longer land, so a
/// register this returns for never changes afterwards.
async fn create<S: ControlStore>(
    store: &S,
    key: &RegisterKey,
    value: &Bytes,
    policy: &RetryPolicy,
) -> Result<bool, RebuildError> {
    loop {
        let outcome = retrying(policy, || {
            store.put_if(key, Expected::Absent, value.clone())
        })
        .await?;
        match outcome {
            PutOutcome::Written(_) => return Ok(true),
            PutOutcome::PreconditionFailed => match get_with_retries(store, key, policy).await? {
                Some(held) if held.value == *value => return Ok(false),
                Some(_) => return Err(RebuildError::RegisterDiffers(key.clone())),
                // Deleted between the two requests: create it again.
                None => {}
            },
        }
    }
}

/// Runs `request` until it is answered, fails for good, or `policy` is
/// exhausted, backing off between attempts.
async fn retrying<T, F, Fut>(policy: &RetryPolicy, mut request: F) -> Result<T, ControlError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, ControlError>>,
{
    let mut backoff = policy.initial_backoff;
    let mut attempts = 1;
    loop {
        match request().await {
            Err(error) if error.is_retryable() && attempts < policy.max_attempts => {
                tokio::time::sleep(backoff).await;
                backoff = backoff.saturating_mul(2).min(policy.max_backoff);
                attempts += 1;
            }
            result => return result,
        }
    }
}

#[cfg(test)]
mod tests;
