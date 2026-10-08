//! Rebuilding stripe layouts from fragment headers alone (design §8.4).
//!
//! An object's `EC_PUBLISH` record (M5-04) holds, for each stripe, its
//! geometry, its codec, its place in the object, and the location of each
//! fragment: a [`StripeLayout`]. Every fragment header carries all of that
//! for its own stripe except the other fragments' locations, which come
//! from where the headers were found. So if a shard's index is lost, the
//! headers gathered from the nodes rebuild every coded object's layout
//! ([`rebuild_layouts`]), even with up to `m` fragments of each stripe
//! missing, and its index entry from [`ObjectMeta`].
//!
//! Headers of several attempts may describe one object version: an
//! abandoned encoding whose fragments are not reclaimed yet, a repair, or a
//! move. Fragments of a stripe that agree on its layout are
//! interchangeable, because a codec's output is fixed by its ID, geometry,
//! and data. The rebuild therefore prefers, for each fragment index, the
//! copy from the latest attempt, and for each stripe the layout of the
//! latest attempt that has enough fragments to decode.

use std::collections::BTreeMap;

use skys3_log::record::{IDENTITY_METADATA, ShardRef};
use skys3_types::{CodecId, CodedStripe, EpochSeq, FragmentLocation, Geometry};

use crate::fragment::{FragmentHeader, ObjectMeta};

/// A fragment header found on a node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoundFragment {
    /// Where the fragment is.
    pub location: FragmentLocation,
    /// Its header.
    pub header: FragmentHeader,
}

/// One stripe's layout, as rebuilt from fragment headers: what an
/// `EC_PUBLISH` record holds as a [`CodedStripe`], with a slot left empty
/// for each fragment no header was found for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StripeLayout {
    /// The stripe's number within the object.
    pub number: u32,
    /// Where the stripe's data starts in the object.
    pub offset: u64,
    /// The stripe's data length.
    pub data_len: u64,
    /// The stripe's geometry.
    pub geometry: Geometry,
    /// The codec that decodes the stripe.
    pub codec: CodecId,
    /// Each fragment's location by index, `k + m` slots; `None` for a
    /// fragment no header was found for.
    pub fragments: Vec<Option<FragmentLocation>>,
}

impl StripeLayout {
    /// The number of fragments located.
    #[must_use]
    pub fn located(&self) -> usize {
        self.fragments.iter().flatten().count()
    }
}

impl From<CodedStripe> for StripeLayout {
    /// The layout of a published stripe, every fragment located.
    fn from(stripe: CodedStripe) -> Self {
        Self {
            number: stripe.number(),
            offset: stripe.offset(),
            data_len: stripe.data_len(),
            geometry: stripe.geometry(),
            codec: stripe.codec(),
            fragments: stripe.fragments().iter().cloned().map(Some).collect(),
        }
    }
}

/// An object version's layout: its stripes, in order, and the metadata its
/// index entry needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectLayout {
    /// The object version.
    pub version: ObjectVersion,
    /// The object's metadata, with the tags, write identity, and identity
    /// metadata of the latest attempt, which a `TAGS` record may have
    /// changed since earlier ones.
    pub object: ObjectMeta,
    /// The stripes, which cover the object's bytes in order.
    pub stripes: Vec<StripeLayout>,
}

/// An object version that fragment headers name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectVersion {
    /// The object's shard.
    pub shard: ShardRef,
    /// The object key.
    pub key: String,
    /// The position of the record that committed the version.
    pub version: EpochSeq,
}

/// Why an object's layout could not be rebuilt from the headers found.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum RebuildError {
    /// Two headers of one object version disagree on what the version is:
    /// its size, ETag, metadata, or another field fixed for its life.
    #[error("the fragments at {first:?} and {other:?} disagree on the object version")]
    InconsistentObject {
        /// A fragment of the latest attempt.
        first: FragmentLocation,
        /// A fragment that disagrees with it.
        other: FragmentLocation,
    },
    /// No header of a stripe was found.
    #[error("no fragment of stripe {stripe} was found")]
    MissingStripe {
        /// The stripe's number.
        stripe: u32,
    },
    /// Fewer than `k` fragments of a stripe were found.
    #[error("stripe {stripe} has {available} of the {needed} fragments it needs")]
    NotEnoughFragments {
        /// The stripe's number.
        stripe: u32,
        /// `k`.
        needed: usize,
        /// The distinct fragments found.
        available: usize,
    },
    /// The stripes found do not cover the object's bytes exactly once.
    #[error("stripe {stripe} does not start where the previous one ends")]
    Gap {
        /// The first stripe out of place, or the stripe count if the
        /// stripes end before the object does.
        stripe: u32,
    },
}

/// Rebuilds the layout of every object version that `found` names.
///
/// Each object's result is independent: an object with too few fragments
/// gets an error, the others their layouts.
pub fn rebuild_layouts(
    found: impl IntoIterator<Item = FoundFragment>,
) -> BTreeMap<ObjectVersion, Result<ObjectLayout, RebuildError>> {
    let mut objects: BTreeMap<ObjectVersion, Vec<FoundFragment>> = BTreeMap::new();
    for fragment in found {
        let header = &fragment.header;
        let version = ObjectVersion {
            shard: header.shard.clone(),
            key: header.key.clone(),
            version: header.version,
        };
        objects.entry(version).or_default().push(fragment);
    }
    objects
        .into_iter()
        .map(|(version, fragments)| {
            let layout = rebuild_object(version.clone(), fragments);
            (version, layout)
        })
        .collect()
}

/// The values that place a stripe in an object and fix its fragments.
type StripeKey = (u32, u64, u64, Geometry, CodecId);

/// Rebuilds one object version from its fragments.
fn rebuild_object(
    version: ObjectVersion,
    mut fragments: Vec<FoundFragment>,
) -> Result<ObjectLayout, RebuildError> {
    // Latest attempt first, then by location, so every choice below is
    // deterministic.
    fragments.sort_by(|a, b| (b.header.attempt, &a.location).cmp(&(a.header.attempt, &b.location)));
    let reference = &fragments[0];
    // A `TAGS` record changes the tags, and makes itself the version's
    // write identity in place of an inherited one or one carried from
    // another cluster: a later attempt, such as a repair, holds the newer
    // ones, so only the rest of the metadata must agree.
    let fixed = |object: &ObjectMeta| {
        let mut metadata = object.metadata.clone();
        metadata.remove(IDENTITY_METADATA);
        ObjectMeta {
            tags: Default::default(),
            identity: EpochSeq::default(),
            metadata,
            ..object.clone()
        }
    };
    let expected = fixed(&reference.header.object);
    if let Some(other) = fragments
        .iter()
        .find(|f| fixed(&f.header.object) != expected)
    {
        return Err(RebuildError::InconsistentObject {
            first: reference.location.clone(),
            other: other.location.clone(),
        });
    }

    // Attempts may have cut the object into different stripes: try each
    // stripe count, latest attempt first.
    let mut counts: Vec<u32> = Vec::new();
    for fragment in &fragments {
        if !counts.contains(&fragment.header.stripe.count) {
            counts.push(fragment.header.stripe.count);
        }
    }
    let mut first_error = None;
    for count in counts {
        let plan = fragments.iter().filter(|f| f.header.stripe.count == count);
        match rebuild_stripes(plan, count, reference.header.object.size) {
            Ok(stripes) => {
                return Ok(ObjectLayout {
                    version,
                    object: reference.header.object.clone(),
                    stripes,
                });
            }
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
    }
    // There is at least one fragment, so at least one count was tried.
    Err(first_error.unwrap_or(RebuildError::MissingStripe { stripe: 0 }))
}

/// Picks a decodable layout for each of `count` stripes from `fragments`,
/// which are sorted latest attempt first, and checks that they cover the
/// object's `size` bytes in order.
fn rebuild_stripes<'a>(
    fragments: impl Iterator<Item = &'a FoundFragment>,
    count: u32,
    size: u64,
) -> Result<Vec<StripeLayout>, RebuildError> {
    // Candidate layouts in order of their latest attempt, each with the
    // first (latest) copy of each fragment index.
    let mut candidates: Vec<(StripeKey, BTreeMap<u8, FragmentLocation>)> = Vec::new();
    for fragment in fragments {
        let stripe = &fragment.header.stripe;
        let key = (
            stripe.number,
            stripe.offset,
            stripe.data_len,
            stripe.geometry,
            stripe.codec,
        );
        let position = match candidates.iter().position(|(k, _)| *k == key) {
            Some(position) => position,
            None => {
                candidates.push((key, BTreeMap::new()));
                candidates.len() - 1
            }
        };
        candidates[position]
            .1
            .entry(fragment.header.index)
            .or_insert_with(|| fragment.location.clone());
    }

    let mut stripes = Vec::new();
    let mut next_offset = 0;
    for number in 0..count {
        let of_stripe = candidates.iter().filter(|((n, ..), _)| *n == number);
        // The candidate with the most fragments, for the error if none
        // can be decoded: its `k` and its fragment count.
        let mut most: Option<(usize, usize)> = None;
        let mut chosen = None;
        for ((_, offset, data_len, geometry, codec), located) in of_stripe {
            if located.len() >= geometry.data_fragments() {
                chosen = Some((*offset, *data_len, *geometry, *codec, located));
                break;
            }
            if most.is_none_or(|(_, available)| located.len() > available) {
                most = Some((geometry.data_fragments(), located.len()));
            }
        }
        let Some((offset, data_len, geometry, codec, located)) = chosen else {
            return Err(match most {
                Some((needed, available)) => RebuildError::NotEnoughFragments {
                    stripe: number,
                    needed,
                    available,
                },
                None => RebuildError::MissingStripe { stripe: number },
            });
        };
        if offset != next_offset {
            return Err(RebuildError::Gap { stripe: number });
        }
        next_offset = offset + data_len;
        let mut slots = vec![None; geometry.total_fragments()];
        for (&index, location) in located {
            slots[usize::from(index)] = Some(location.clone());
        }
        stripes.push(StripeLayout {
            number,
            offset,
            data_len,
            geometry,
            codec,
            fragments: slots,
        });
    }
    if next_offset != size {
        return Err(RebuildError::Gap { stripe: count });
    }
    Ok(stripes)
}
