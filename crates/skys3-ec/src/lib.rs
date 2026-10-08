#![forbid(unsafe_code)]
//! Erasure coding for `local` buckets (design §8).
//!
//! Section numbers (§) refer to the [SkyS3 design](https://github.com/skys3/skys3/blob/main/docs/skys3-design.md).
//!
//! A large object in a `local` bucket is stored as a sequence of stripes.
//! Each stripe splits its data into `k` data fragments and adds `m` parity
//! fragments, and any `k` of the `k + m` fragments rebuild it (§8.3, §8.4).
//!
//! - **Geometry** ([`Geometry`]): the `k+m` shape of a stripe, recorded per
//!   stripe and never recomputed.
//! - **Codecs** ([`EcCodec`], [`CodecId`], [`codec`], [`current_codec`]):
//!   the versioned interface that encodes, decodes, and repairs one stripe
//!   in memory. Every stripe stores the ID of the codec that encoded it and
//!   is always decoded by that codec, so a new codec never changes how an
//!   existing stripe reads (§15).
//! - **Reed-Solomon** ([`ReedSolomonV1`]): codec 1, systematic Reed-Solomon
//!   from `reed-solomon-simd`, frozen by the golden vectors in
//!   `tests/golden.rs`.
//!
//! - **Fragments** ([`fragment`]): the fragment record format. A fragment's
//!   header names its object version, stripe, geometry, codec, and the
//!   attempt that wrote it, so headers alone rebuild a lost index's coded
//!   objects.
//! - **Fragment store** ([`FragmentStore`]): a disk's fragment segments. It
//!   makes each fragment durable before it returns the fragment's ID, reads
//!   ranges verified against block checksums, and recovers from crashes
//!   without losing an acknowledged fragment.
//! - **Layouts** ([`rebuild_layouts`]): each stripe's layout and codec,
//!   rebuilt from the fragment headers found on the nodes.
//!
//! - **Fragment writes** ([`FragmentClient`], [`FragmentServer`]): a
//!   fragment sent to its node over the cluster transport, as the
//!   `FragmentWrite` and `FragmentWritten` messages, and acknowledged
//!   only once the node's fragment store made it durable.
//! - **Encoding** ([`Encoder`]): on a shard's primary, objects that
//!   qualify (§8.2) are read from the local replica, encoded stripe by
//!   stripe, placed by `skys3_coord::FragmentPlanner`, written through a
//!   [`FragmentWriter`], and published with an `EC_PUBLISH` record once
//!   every fragment is durable. Each attempt has an [`AttemptId`] drawn
//!   from numbers the index reserves durably, and [`Attempts`] tracks
//!   those in progress, the fence orphan reclamation needs (§8.4).
//! - **Orphan reclamation** ([`orphans`]): fragment nodes ask the shard
//!   primary about fragments they have held for
//!   `fragment_orphan_after_seconds` ([`OrphanReclaimer`],
//!   [`OrphanClient`]), and reclaim those its [`OrphanJudge`] finds that
//!   no committed layout references and none ever will.
//! - **Coded reads** ([`read`]): a range of a coded object read from the
//!   data fragments that cover it ([`read_coded`]), and decoded from any
//!   `k` fragments of a stripe that lost one, missing or corrupt, through
//!   a [`FragmentSource`] ([`FragmentReadClient`] over the cluster
//!   transport, served by [`FragmentServer::serve_reads`]).
//! - **Re-indexing** ([`reindex`]): a lost shard's coded objects
//!   restored from its latest index snapshot and the fragment headers its
//!   surviving nodes hold ([`HeaderSource`]), the restore drill's core
//!   (§6.9, §8.9).
//! - **Repair** ([`repair`]): a shard primary finds the fragments its
//!   coded objects lost, rebuilds each damaged stripe's on other nodes
//!   from `k` survivors, most damaged stripes first and within the node's
//!   repair bandwidth, and relocates them with an `EC_RELOCATE` record
//!   ([`Repairer`]).
//!
//! ```
//! use skys3_ec::{CodecId, EcCodec, Geometry, codec, current_codec};
//!
//! let geometry = Geometry::RS_4_2;
//! let data = b"an object's stripe, at least one byte".to_vec();
//! let fragments = current_codec().encode(geometry, &data)?;
//! assert_eq!(fragments.len(), 6);
//!
//! // Lose any two fragments; the stripe's codec ID finds the codec again.
//! let mut slots: Vec<Option<&[u8]>> = fragments.iter().map(|f| Some(f.as_slice())).collect();
//! slots[0] = None;
//! slots[5] = None;
//! let codec = codec(CodecId::CURRENT)?;
//! assert_eq!(codec.decode(geometry, data.len() as u64, &slots)?, data);
//! # Ok::<(), skys3_ec::EcError>(())
//! ```

mod codec;
mod encoder;
mod error;
pub mod fragment;
mod layout;
pub mod orphans;
pub mod read;
mod reed_solomon;
pub mod reindex;
pub mod repair;
mod store;
mod transfer;

pub use codec::{EcCodec, codec, current_codec};
pub use encoder::{
    AttemptState, Attempts, EncodeError, EncodeEvent, EncodeObserver, EncodeStep, Encoded, Encoder,
    EncoderSettings, PlannerSource, ScanReport, Skip,
};
pub use error::EcError;
pub use layout::{
    FoundFragment, ObjectLayout, ObjectVersion, RebuildError, StripeLayout, rebuild_layouts,
};
pub use orphans::{
    OrphanClient, OrphanConfirmer, OrphanJudge, OrphanReclaimer, OrphanServer, Suspect, Verdict,
};
pub use read::{
    CodedBody, CodedRead, CodedReadError, FragmentBytes, FragmentIdentity, FragmentReadClient,
    FragmentReadError, FragmentRequest, FragmentSource, read_coded,
};
pub use reed_solomon::ReedSolomonV1;
pub use reindex::{HeaderError, HeaderSource, Reindexed, SnapshotState, reindex};
pub use repair::{
    RepairBandwidth, RepairError, RepairEvent, RepairMetrics, RepairObserver, RepairReport,
    RepairSettings, RepairStep, Repairer,
};
pub use skys3_types::{
    AttemptId, CodecId, CodedStripe, FragmentId, FragmentLocation, Geometry, GeometryError,
};
#[cfg(feature = "test-util")]
#[doc(hidden)]
pub use store::SeededBug;
pub use store::{
    FragmentError, FragmentRange, FragmentSegment, FragmentStore, FragmentStoreConfig,
    RecoveryError, RecoveryReport, TornTail,
};
pub use transfer::{
    CHUNK_LEN, FragmentClient, FragmentServer, FragmentWrite, FragmentWriter, FragmentWritten,
    TransferError,
};
