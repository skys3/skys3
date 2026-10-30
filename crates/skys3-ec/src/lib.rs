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
//! The fragment store and fragment segments (M5-02), placement (M5-03), and
//! the encoder that drives a codec stripe by stripe and publishes the result
//! (M5-04) build on this crate. A codec works on whole stripes in memory;
//! those layers decide where fragments live and how they are checksummed.
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
mod error;
mod geometry;
mod reed_solomon;

pub use codec::{CodecId, EcCodec, codec, current_codec};
pub use error::EcError;
pub use geometry::Geometry;
pub use reed_solomon::ReedSolomonV1;
