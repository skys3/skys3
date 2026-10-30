//! What a fragment header says about its fragment: the object version and
//! stripe it belongs to, how the stripe was coded, and which attempt wrote
//! it.

use skys3_log::record::{
    FieldError, MAX_KEY_LEN, MAX_METADATA_LEN, MAX_TAG_KEY_LEN, MAX_TAG_VALUE_LEN, MAX_TAGS,
    Metadata, ShardRef, TagSet,
};
use skys3_types::checksum::{Checksum, ChecksumAlgorithm, Checksums};
use skys3_types::limits::MAX_PARTS;
use skys3_types::{
    AttemptId, BucketId, CodecId, ETag, Epoch, EpochSeq, Geometry, ShardId, VersionIdentity,
};

use super::record::{FragmentDecodeError, FragmentEncodeError};
use super::wire::{Reader, Writer, inconsistent, insert_sorted, invalid};

/// A fragment's header: enough on its own to rebuild its stripe's layout
/// in the object's `EC_PUBLISH` record, and the object's index entry, if a
/// shard's index is ever lost (design §8.4).
///
/// The writer of a fragment (an encoder, a repair, or a move) fills it in;
/// the fragment store adds the [`FragmentId`](crate::FragmentId) it assigns
/// and checksums both header and payload. The module documentation of
/// [`fragment`](crate::fragment) gives the encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FragmentHeader {
    /// The shard whose primary wrote the fragment: the object's bucket and
    /// shard.
    pub shard: ShardRef,
    /// The object key.
    pub key: String,
    /// The log position of the record that committed the object version.
    /// With the ETag in [`ObjectMeta::etag`], it is the version identity
    /// ([`FragmentHeader::version_identity`]).
    pub version: EpochSeq,
    /// The attempt that wrote the fragment (§8.4).
    pub attempt: AttemptId,
    /// The stripe the fragment belongs to.
    pub stripe: StripeInfo,
    /// The fragment's index within its stripe: `0..k` for data fragments,
    /// `k..k+m` for parity fragments.
    pub index: u8,
    /// The object the stripe is part of.
    pub object: ObjectMeta,
}

/// The stripe a fragment belongs to, and how it was coded (§8.3, §8.4).
///
/// Every fragment of a stripe carries the same values, so any one of them
/// gives the stripe's layout and codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StripeInfo {
    /// The stripe's number within the object, from 0.
    pub number: u32,
    /// How many stripes the object was encoded as, at least 1.
    pub count: u32,
    /// Where the stripe's data starts in the object.
    pub offset: u64,
    /// The stripe's data length in bytes, at least 1.
    pub data_len: u64,
    /// The stripe's geometry, `k+m`.
    pub geometry: Geometry,
    /// The codec that encoded the stripe, and must decode it.
    pub codec: CodecId,
}

/// The object version a stripe is part of: what its index entry holds
/// besides the payload's location (§8.4).
///
/// A `TAGS` record changes a version's tags without a new version, and
/// makes itself the version's write identity, dropping an inherited one and
/// the identity metadata of another cluster's write. So a header holds the
/// tags, `identity`, and identity metadata the version had when the
/// attempt that wrote it read the object; everything else is fixed for the
/// life of the version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    /// The object's size in bytes.
    pub size: u64,
    /// The object's `Last-Modified`, in milliseconds since the Unix epoch.
    pub last_modified_ms: u64,
    /// The ETag clients see (`local_etag`, §4.2).
    pub etag: ETag,
    /// The log position the object's write identity names (§7.2): the
    /// committing record's own position, or that of the `UPLOAD_BEGIN` or
    /// `MPU_CREATE` it inherits.
    pub identity: EpochSeq,
    /// The stored metadata: standard headers and `x-amz-meta-*`.
    pub metadata: Metadata,
    /// The object's tags when the fragment was written.
    pub tags: TagSet,
    /// The client checksums (§7.4).
    pub checksums: Checksums,
    /// For a multipart object, each part's number and size in part order;
    /// empty otherwise. The sizes add up to the object's size.
    pub parts: Vec<PartSize>,
}

/// The number and size of one part of a multipart object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PartSize {
    /// The part number, from 1 to 10,000.
    pub number: u16,
    /// The part's size in bytes.
    pub size: u64,
}

impl FragmentHeader {
    /// Encodes the header's fields alone, as a fragment record holds them
    /// after its fixed header: what a writer sends a fragment node with the
    /// fragment's bytes.
    ///
    /// # Errors
    ///
    /// [`FragmentEncodeError`] if a field breaks the format.
    pub fn to_bytes(&self) -> Result<Vec<u8>, FragmentEncodeError> {
        let mut out = Vec::new();
        self.encode(&mut out)?;
        Ok(out)
    }

    /// Decodes fields that [`FragmentHeader::to_bytes`] encoded, which must
    /// fill `bytes` exactly.
    ///
    /// # Errors
    ///
    /// [`FragmentDecodeError::Malformed`] if a field breaks the format.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, FragmentDecodeError> {
        let mut r = Reader::new(bytes);
        let header = Self::decode(&mut r)?;
        r.finish("header")?;
        Ok(header)
    }

    /// The identity of the object version the fragment belongs to (§9.2).
    #[must_use]
    pub fn version_identity(&self) -> VersionIdentity {
        VersionIdentity::new(self.version.seq, self.object.etag.clone())
    }

    /// Checks the rules the encoding cannot express: the fragment index is
    /// within the geometry, the stripe lies within the object, and parts
    /// add up to the object.
    pub(crate) fn check(&self) -> Result<(), FieldError> {
        let stripe = &self.stripe;
        if stripe.count == 0 {
            return Err(inconsistent("stripe.count", "an object has a stripe"));
        }
        if stripe.number >= stripe.count {
            return Err(inconsistent("stripe.number", "is not below the count"));
        }
        if stripe.data_len == 0 {
            return Err(inconsistent("stripe.data_len", "a stripe holds data"));
        }
        if stripe.codec.get() == 0 {
            return Err(invalid("stripe.codec", "codec ID 0 is never assigned"));
        }
        let end = stripe.offset.checked_add(stripe.data_len);
        if end.is_none_or(|end| end > self.object.size) {
            return Err(inconsistent(
                "stripe.offset",
                "the stripe ends past the object",
            ));
        }
        if usize::from(self.index) >= stripe.geometry.total_fragments() {
            return Err(inconsistent("index", "is not below k+m"));
        }
        let parts = &self.object.parts;
        if !parts.is_empty() {
            let increasing = parts.windows(2).all(|w| w[0].number < w[1].number);
            if !increasing
                || parts
                    .iter()
                    .any(|p| p.number == 0 || u32::from(p.number) > MAX_PARTS)
            {
                return Err(invalid(
                    "object.parts",
                    "part numbers increase from 1 to 10000",
                ));
            }
            let total = parts
                .iter()
                .try_fold(0u64, |total, part| total.checked_add(part.size));
            if total != Some(self.object.size) {
                return Err(inconsistent("object.parts", "sizes do not add up"));
            }
        }
        Ok(())
    }

    /// Appends the header's fields, in declaration order.
    pub(crate) fn encode(&self, out: &mut Vec<u8>) -> Result<(), FieldError> {
        self.check()?;
        let mut w = Writer::new(out);
        w.str8(
            "shard.bucket",
            self.shard.bucket.as_str(),
            1,
            BucketId::MAX_LEN,
        )?;
        w.u8(self.shard.shard.get());
        w.str16("key", &self.key, 1, MAX_KEY_LEN)?;
        w.position(self.version);
        w.u64(self.attempt.epoch.get());
        w.u64(self.attempt.number);
        let stripe = &self.stripe;
        w.u32(stripe.number);
        w.u32(stripe.count);
        w.u64(stripe.offset);
        w.u64(stripe.data_len);
        // A geometry has at most 255 fragments, so each count fits a byte.
        w.u8(stripe.geometry.data_fragments() as u8);
        w.u8(stripe.geometry.parity_fragments() as u8);
        w.u16(stripe.codec.get());
        w.u8(self.index);
        let object = &self.object;
        w.u64(object.size);
        w.u64(object.last_modified_ms);
        w.str16("object.etag", object.etag.as_str(), 1, ETag::MAX_LEN)?;
        w.position(object.identity);
        write_metadata(&mut w, &object.metadata)?;
        w.count8("object.tags", object.tags.len(), 0, MAX_TAGS)?;
        for (key, value) in &object.tags {
            w.str16("object.tags", key, 1, MAX_TAG_KEY_LEN)?;
            w.str16("object.tags", value, 0, MAX_TAG_VALUE_LEN)?;
        }
        w.count8(
            "object.checksums",
            object.checksums.len(),
            0,
            ChecksumAlgorithm::ALL.len(),
        )?;
        for (&algorithm, checksum) in &object.checksums {
            checksum
                .check(algorithm)
                .map_err(|e| invalid("object.checksums", e))?;
            w.u8(algorithm.code());
            w.raw(checksum.digest());
            w.u16(checksum.parts().unwrap_or(0));
        }
        w.count16("object.parts", object.parts.len(), 0, MAX_PARTS as usize)?;
        for part in &object.parts {
            w.u16(part.number);
            w.u64(part.size);
        }
        Ok(())
    }

    /// Reads the header's fields, leaving the rest of `r` unread.
    pub(crate) fn decode(r: &mut Reader<'_>) -> Result<Self, FieldError> {
        let bucket = r.str8("shard.bucket", 1, BucketId::MAX_LEN)?;
        let bucket = BucketId::new(bucket).map_err(|e| invalid("shard.bucket", e))?;
        let shard = ShardRef::new(bucket, ShardId::new(r.u8("shard.number")?));
        let key = r.str16("key", 1, MAX_KEY_LEN)?;
        let version = r.position("version")?;
        let attempt = AttemptId::new(Epoch::new(r.u64("attempt")?), r.u64("attempt")?);
        let number = r.u32("stripe.number")?;
        let count = r.u32("stripe.count")?;
        let offset = r.u64("stripe.offset")?;
        let data_len = r.u64("stripe.data_len")?;
        let data = r.u8("stripe.geometry")?;
        let parity = r.u8("stripe.geometry")?;
        let geometry =
            Geometry::new(data.into(), parity.into()).map_err(|e| invalid("stripe.geometry", e))?;
        let codec = CodecId::new(r.u16("stripe.codec")?);
        let index = r.u8("index")?;
        let size = r.u64("object.size")?;
        let last_modified_ms = r.u64("object.last_modified")?;
        let etag = r.str16("object.etag", 1, ETag::MAX_LEN)?;
        let etag = ETag::new(etag).map_err(|e| invalid("object.etag", e))?;
        let identity = r.position("object.identity")?;
        let metadata = read_metadata(r)?;
        let mut tags = TagSet::new();
        for _ in 0..r.count8("object.tags", 0, MAX_TAGS)? {
            let key = r.str16("object.tags", 1, MAX_TAG_KEY_LEN)?;
            let value = r.str16("object.tags", 0, MAX_TAG_VALUE_LEN)?;
            insert_sorted("object.tags", &mut tags, key, value)?;
        }
        let mut checksums = Checksums::new();
        for _ in 0..r.count8("object.checksums", 0, ChecksumAlgorithm::ALL.len())? {
            let code = r.u8("object.checksums")?;
            let algorithm = ChecksumAlgorithm::from_code(code)
                .ok_or_else(|| invalid("object.checksums", format!("unknown algorithm {code}")))?;
            let digest = r.take("object.checksums", algorithm.digest_len())?;
            let checksum = match r.u16("object.checksums")? {
                0 => Checksum::full_object(algorithm, digest),
                parts => Checksum::composite(algorithm, digest, parts.into()),
            }
            .map_err(|e| invalid("object.checksums", e))?;
            insert_sorted("object.checksums", &mut checksums, algorithm, checksum)?;
        }
        let part_count = r.count16("object.parts", 0, MAX_PARTS as usize)?;
        let mut parts = Vec::with_capacity(part_count);
        for _ in 0..part_count {
            let number = r.u16("object.parts")?;
            let size = r.u64("object.parts")?;
            parts.push(PartSize { number, size });
        }
        let header = Self {
            shard,
            key,
            version,
            attempt,
            stripe: StripeInfo {
                number,
                count,
                offset,
                data_len,
                geometry,
                codec,
            },
            index,
            object: ObjectMeta {
                size,
                last_modified_ms,
                etag,
                identity,
                metadata,
                tags,
                checksums,
                parts,
            },
        };
        header.check()?;
        Ok(header)
    }
}

/// Whether `name` is a lowercase HTTP header name, as stored metadata
/// names are.
fn is_metadata_name(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || b"!#$%&'*+-.^_`|~".contains(&b)
        })
}

/// Adds an entry's bytes to the running total of a metadata map.
fn add_metadata_len(total: &mut usize, name: &str, value: &str) -> Result<(), FieldError> {
    *total = total.saturating_add(name.len()).saturating_add(value.len());
    if *total > MAX_METADATA_LEN {
        return Err(FieldError {
            field: "object.metadata",
            problem: skys3_log::record::Problem::TooLong {
                len: *total as u64,
                max: MAX_METADATA_LEN as u64,
            },
        });
    }
    if !is_metadata_name(name) {
        return Err(invalid(
            "object.metadata",
            format!("{name:?} is not a lowercase header name"),
        ));
    }
    Ok(())
}

fn write_metadata(w: &mut Writer<'_>, metadata: &Metadata) -> Result<(), FieldError> {
    const FIELD: &str = "object.metadata";
    w.count16(FIELD, metadata.len(), 0, MAX_METADATA_LEN)?;
    let mut total = 0;
    for (name, value) in metadata {
        add_metadata_len(&mut total, name, value)?;
        w.str16(FIELD, name, 1, MAX_METADATA_LEN)?;
        w.str16(FIELD, value, 0, MAX_METADATA_LEN)?;
    }
    Ok(())
}

fn read_metadata(r: &mut Reader<'_>) -> Result<Metadata, FieldError> {
    const FIELD: &str = "object.metadata";
    let mut metadata = Metadata::new();
    let mut total = 0;
    for _ in 0..r.count16(FIELD, 0, MAX_METADATA_LEN)? {
        let name = r.str16(FIELD, 1, MAX_METADATA_LEN)?;
        let value = r.str16(FIELD, 0, MAX_METADATA_LEN)?;
        add_metadata_len(&mut total, &name, &value)?;
        insert_sorted(FIELD, &mut metadata, name, value)?;
    }
    Ok(metadata)
}
