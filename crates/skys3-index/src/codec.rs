//! The byte encodings of the index's keys and values.
//!
//! redb stores raw bytes; this module defines what they mean. Every value
//! starts with a format byte ([`VALUE_FORMAT`]) so a later build can change
//! a layout without guessing. This build writes format 3 and reads formats
//! 1 to 3 ([`MIN_VALUE_FORMAT`]); any other format byte is rejected. Format
//! 2 adds a `u16` part count after each checksum digest of an [`Entry`],
//! zero for a `FULL_OBJECT` checksum; a format 1 checksum has none and is
//! `FULL_OBJECT`. Format 3 adds the payload of a multipart object (tag 3:
//! the upload's position, a `u16` part count, and each part's number
//! (`u16`) and size (`u64`)), and the [`Upload`] and [`Part`] values, which
//! only it has. Every other value is the same in all three. Integers in
//! values are little-endian. Keys use big-endian integers so that redb's
//! byte order sorts them numerically:
//!
//! | Table | Key | Value |
//! |---|---|---|
//! | namespace | shard, then the object key's bytes | [`Entry`] |
//! | locations | shard, epoch, seq | [`RecordLocation`]: segment, offset, length |
//! | shards | shard | applied position: epoch, seq |
//! | coverage | disk label (`u8` length and bytes), segment id | [`SegmentSummary`] from offset zero |
//! | control | register key (UTF-8) | [`ControlEntry`] |
//! | uploads | shard, the object key's bytes, a zero byte, then the upload's epoch and seq | [`Upload`] |
//! | parts | shard, the upload's epoch and seq, part number (`u16`) | [`Part`] |
//! | shard_map | shard | the gateway's [`ShardConfig`], as its register's JSON |
//!
//! An upload key ends with a fixed 17 bytes, so it decodes whatever bytes
//! the object key holds, and a shard's uploads sort by key, then by age.
//!
//! A shard is encoded as the bucket ID's length (`u8`), its bytes, and the
//! shard number (`u8`), so all of a shard's keys share a prefix and sort
//! together, objects in S3 key order.
//!
//! Variable-length fields follow the log record format (§10.1): text has a
//! `u16` length (a `u8` length for bucket IDs), an option has a presence
//! byte, and a map has a count followed by its entries in strictly
//! increasing key order. Decoders check every length against its bound and
//! against the remaining bytes before allocating, and reject trailing
//! bytes, so every value of the current format has exactly one encoding. A
//! value of an older format re-encodes in the current one.

use std::collections::BTreeMap;
use std::fmt;

use skys3_log::record::{
    Checksum, ChecksumAlgorithm, ChecksumType, Checksums, CopySource, ExtentRef, MAX_EXTENTS,
    MAX_KEY_LEN, MAX_METADATA_LEN, MAX_PAYLOAD_LEN, MAX_STORAGE_CLASS_LEN, MAX_TAG_KEY_LEN,
    MAX_TAG_VALUE_LEN, MAX_TAGS, MAX_VERSION_ID_LEN, Metadata, ShardRef, TagSet, UploadChecksum,
};
use skys3_log::{RecordLocation, SegmentId, SegmentSummary};
use skys3_types::limits::MAX_PARTS;
use skys3_types::{
    BucketId, ETag, Epoch, EpochSeq, Generation, Label, RegisterDocument, Seq, ShardConfig,
    ShardId, VersionIdentity,
};

use crate::entry::{
    ControlEntry, Entry, EntryState, ObjectPart, ObjectVersion, Part, Payload, Upload,
};

/// The format byte every value this build writes starts with, and the
/// newest format it reads.
pub const VALUE_FORMAT: u8 = 3;

/// The oldest value format this build reads.
pub const MIN_VALUE_FORMAT: u8 = 1;

/// The first value format with a part count after each checksum digest.
const PART_COUNTS_SINCE: u8 = 2;

/// The first value format with multipart uploads.
const UPLOADS_SINCE: u8 = 3;

/// The longest control-state value kept locally, in bytes.
pub const MAX_CONTROL_VALUE_LEN: usize = 1 << 20;

/// The longest control-store version text kept locally, in bytes.
pub const MAX_CONTROL_VERSION_LEN: usize = 1024;

/// The most shards a coverage summary names: far more than share a disk.
const MAX_SUMMARY_SHARDS: usize = 1 << 20;

const PAYLOAD_NONE: u8 = 0;
const PAYLOAD_INLINE: u8 = 1;
const PAYLOAD_EXTENTS: u8 = 2;
const PAYLOAD_PARTS: u8 = 3;

/// The bytes after an upload key's object key: a zero byte, then the
/// upload's position.
const UPLOAD_KEY_SUFFIX_LEN: usize = 1 + 16;

/// Bytes the index cannot decode, or a value it cannot encode.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{field}: {reason}")]
pub struct CodecError {
    field: &'static str,
    reason: String,
}

impl CodecError {
    fn new(field: &'static str, reason: impl fmt::Display) -> Self {
        Self {
            field,
            reason: reason.to_string(),
        }
    }

    /// The field that broke the format.
    #[must_use]
    pub fn field(&self) -> &'static str {
        self.field
    }
}

type Result<T> = std::result::Result<T, CodecError>;

/// Appends primitive values, checking each bound the decoder checks.
#[derive(Default)]
struct Writer(Vec<u8>);

impl Writer {
    fn value() -> Self {
        Self(vec![VALUE_FORMAT])
    }

    fn u8(&mut self, value: u8) {
        self.0.push(value);
    }

    fn u16(&mut self, value: u16) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }

    fn u32(&mut self, value: u32) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }

    fn position(&mut self, position: EpochSeq) {
        self.u64(position.epoch.get());
        self.u64(position.seq.get());
    }

    fn present(&mut self, present: bool) {
        self.u8(u8::from(present));
    }

    fn len<T: TryFrom<usize>>(field: &'static str, len: usize, max: usize) -> Result<T> {
        if len > max {
            return Err(CodecError::new(field, format_args!("{len} exceeds {max}")));
        }
        T::try_from(len).map_err(|_| CodecError::new(field, "length out of range"))
    }

    fn len8(&mut self, field: &'static str, len: usize, max: usize) -> Result<()> {
        let len = Self::len(field, len, max)?;
        self.u8(len);
        Ok(())
    }

    fn len16(&mut self, field: &'static str, len: usize, max: usize) -> Result<()> {
        let len = Self::len(field, len, max)?;
        self.u16(len);
        Ok(())
    }

    fn len32(&mut self, field: &'static str, len: usize, max: usize) -> Result<()> {
        let len = Self::len(field, len, max)?;
        self.u32(len);
        Ok(())
    }

    fn str16(&mut self, field: &'static str, text: &str, max: usize) -> Result<()> {
        self.len16(field, text.len(), max)?;
        self.0.extend_from_slice(text.as_bytes());
        Ok(())
    }

    fn option_str(&mut self, field: &'static str, text: Option<&str>, max: usize) -> Result<()> {
        self.present(text.is_some());
        text.map_or(Ok(()), |text| self.str16(field, text, max))
    }

    fn shard(&mut self, shard: &ShardRef) {
        let bucket = shard.bucket.as_str();
        // A bucket ID is at most `BucketId::MAX_LEN` bytes.
        self.u8(bucket.len() as u8);
        self.0.extend_from_slice(bucket.as_bytes());
        self.u8(shard.shard.get());
    }
}

/// Reads primitive values, checking every length before using it. The
/// second field is the format of the value being read; keys have none and
/// read as the current format.
struct Reader<'a>(&'a [u8], u8);

impl<'a> Reader<'a> {
    /// Starts reading a value, checking its format byte.
    fn value(bytes: &'a [u8], field: &'static str) -> Result<Self> {
        let mut reader = Self(bytes, VALUE_FORMAT);
        match reader.u8(field)? {
            format @ MIN_VALUE_FORMAT..=VALUE_FORMAT => {
                reader.1 = format;
                Ok(reader)
            }
            format => Err(CodecError::new(
                field,
                format_args!("unsupported value format {format}"),
            )),
        }
    }

    fn take(&mut self, field: &'static str, len: usize) -> Result<&'a [u8]> {
        if len > self.0.len() {
            return Err(CodecError::new(field, "truncated"));
        }
        let (taken, rest) = self.0.split_at(len);
        self.0 = rest;
        Ok(taken)
    }

    fn array<const N: usize>(&mut self, field: &'static str) -> Result<[u8; N]> {
        let mut array = [0; N];
        array.copy_from_slice(self.take(field, N)?);
        Ok(array)
    }

    fn u8(&mut self, field: &'static str) -> Result<u8> {
        Ok(self.array::<1>(field)?[0])
    }

    fn u16(&mut self, field: &'static str) -> Result<u16> {
        self.array(field).map(u16::from_le_bytes)
    }

    fn u32(&mut self, field: &'static str) -> Result<u32> {
        self.array(field).map(u32::from_le_bytes)
    }

    fn u64(&mut self, field: &'static str) -> Result<u64> {
        self.array(field).map(u64::from_le_bytes)
    }

    fn position(&mut self, field: &'static str) -> Result<EpochSeq> {
        let epoch = Epoch::new(self.u64(field)?);
        Ok(EpochSeq::new(epoch, Seq::new(self.u64(field)?)))
    }

    fn present(&mut self, field: &'static str) -> Result<bool> {
        match self.u8(field)? {
            0 => Ok(false),
            1 => Ok(true),
            tag => Err(CodecError::new(field, format_args!("invalid tag {tag}"))),
        }
    }

    fn check(field: &'static str, len: usize, max: usize) -> Result<usize> {
        if len > max {
            return Err(CodecError::new(field, format_args!("{len} exceeds {max}")));
        }
        Ok(len)
    }

    fn len8(&mut self, field: &'static str, max: usize) -> Result<usize> {
        Self::check(field, self.u8(field)?.into(), max)
    }

    fn len16(&mut self, field: &'static str, max: usize) -> Result<usize> {
        let len = self.u16(field)?;
        Self::check(field, len.into(), max)
    }

    fn len32(&mut self, field: &'static str, max: usize) -> Result<usize> {
        let len = usize::try_from(self.u32(field)?).unwrap_or(usize::MAX);
        Self::check(field, len, max)
    }

    /// Checks that `count` entries of at least `min_len` bytes fit in what
    /// remains, so a hostile count cannot reserve memory.
    fn check_count(&self, field: &'static str, count: usize, min_len: usize) -> Result<()> {
        match count.checked_mul(min_len) {
            Some(needed) if needed <= self.0.len() => Ok(()),
            _ => Err(CodecError::new(field, "truncated")),
        }
    }

    fn text(field: &'static str, bytes: &[u8]) -> Result<String> {
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| CodecError::new(field, "not UTF-8"))
    }

    fn str16(&mut self, field: &'static str, max: usize) -> Result<String> {
        let len = self.len16(field, max)?;
        Self::text(field, self.take(field, len)?)
    }

    fn option_str(&mut self, field: &'static str, max: usize) -> Result<Option<String>> {
        if self.present(field)? {
            self.str16(field, max).map(Some)
        } else {
            Ok(None)
        }
    }

    fn etag(&mut self, field: &'static str) -> Result<ETag> {
        ETag::new(self.str16(field, ETag::MAX_LEN)?).map_err(|e| CodecError::new(field, e))
    }

    fn option_etag(&mut self, field: &'static str) -> Result<Option<ETag>> {
        if self.present(field)? {
            self.etag(field).map(Some)
        } else {
            Ok(None)
        }
    }

    fn shard(&mut self, field: &'static str) -> Result<ShardRef> {
        let len = self.len8(field, BucketId::MAX_LEN)?;
        let bucket = Self::text(field, self.take(field, len)?)?;
        let bucket = BucketId::new(bucket).map_err(|e| CodecError::new(field, e))?;
        Ok(ShardRef::new(bucket, ShardId::new(self.u8(field)?)))
    }

    fn finish(self, field: &'static str) -> Result<()> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(CodecError::new(
                field,
                format_args!("{} trailing bytes", self.0.len()),
            ))
        }
    }
}

/// Inserts a decoded map entry, requiring strictly increasing keys.
fn insert_sorted<K: Ord, V>(
    field: &'static str,
    map: &mut BTreeMap<K, V>,
    key: K,
    value: V,
) -> Result<()> {
    if map.last_key_value().is_some_and(|(last, _)| *last >= key) {
        return Err(CodecError::new(field, "keys out of order"));
    }
    map.insert(key, value);
    Ok(())
}

/// Returns the key prefix every key of `shard` starts with.
#[must_use]
pub fn shard_key(shard: &ShardRef) -> Vec<u8> {
    let mut w = Writer::default();
    w.shard(shard);
    w.0
}

/// Decodes a key of the shards table.
///
/// # Errors
///
/// Returns a [`CodecError`] if `bytes` is not exactly one shard.
pub fn decode_shard_key(bytes: &[u8]) -> Result<ShardRef> {
    let mut r = Reader(bytes, VALUE_FORMAT);
    let shard = r.shard("shard")?;
    r.finish("shard")?;
    Ok(shard)
}

/// Returns the namespace key of `key` in `shard`.
#[must_use]
pub fn entry_key(shard: &ShardRef, key: &str) -> Vec<u8> {
    let mut bytes = shard_key(shard);
    bytes.extend_from_slice(key.as_bytes());
    bytes
}

/// Decodes a namespace key into its shard and object key.
///
/// # Errors
///
/// Returns a [`CodecError`] if the shard is malformed or the object key is
/// empty, too long, or not UTF-8.
pub fn decode_entry_key(bytes: &[u8]) -> Result<(ShardRef, String)> {
    let mut r = Reader(bytes, VALUE_FORMAT);
    let shard = r.shard("entry key")?;
    let key = Reader::check("entry key", r.0.len(), MAX_KEY_LEN)?;
    if key == 0 {
        return Err(CodecError::new("entry key", "empty"));
    }
    Ok((shard, Reader::text("entry key", r.0)?))
}

/// Returns the location-map key of the record at `position` in `shard`.
#[must_use]
pub fn location_key(shard: &ShardRef, position: EpochSeq) -> Vec<u8> {
    let mut bytes = shard_key(shard);
    bytes.extend_from_slice(&position.epoch.get().to_be_bytes());
    bytes.extend_from_slice(&position.seq.get().to_be_bytes());
    bytes
}

/// Decodes a location-map key into its shard and position.
///
/// # Errors
///
/// Returns a [`CodecError`] if `bytes` is not a shard and a position.
pub fn decode_location_key(bytes: &[u8]) -> Result<(ShardRef, EpochSeq)> {
    let mut r = Reader(bytes, VALUE_FORMAT);
    let shard = r.shard("location key")?;
    let epoch = u64::from_be_bytes(r.array("location key")?);
    let seq = u64::from_be_bytes(r.array("location key")?);
    r.finish("location key")?;
    Ok((shard, EpochSeq::new(Epoch::new(epoch), Seq::new(seq))))
}

/// Returns the coverage key of segment `segment` on disk `disk`.
#[must_use]
pub fn coverage_key(disk: &Label, segment: SegmentId) -> Vec<u8> {
    let mut bytes = coverage_prefix(disk);
    bytes.extend_from_slice(&segment.get().to_be_bytes());
    bytes
}

/// Returns the prefix of every coverage key of disk `disk`.
#[must_use]
pub fn coverage_prefix(disk: &Label) -> Vec<u8> {
    let label = disk.as_str();
    let mut bytes = Vec::with_capacity(label.len() + 9);
    // A label is at most `Label::MAX_LEN` bytes.
    bytes.push(label.len() as u8);
    bytes.extend_from_slice(label.as_bytes());
    bytes
}

/// Decodes a coverage key into its disk and segment.
///
/// # Errors
///
/// Returns a [`CodecError`] if `bytes` is not a label and a segment id.
pub fn decode_coverage_key(bytes: &[u8]) -> Result<(Label, SegmentId)> {
    let mut r = Reader(bytes, VALUE_FORMAT);
    let len = r.len8("coverage key", Label::MAX_LEN)?;
    let label = Reader::text("coverage key", r.take("coverage key", len)?)?;
    let label = Label::new(label).map_err(|e| CodecError::new("coverage key", e))?;
    let segment = SegmentId::new(u64::from_be_bytes(r.array("coverage key")?));
    r.finish("coverage key")?;
    Ok((label, segment))
}

/// Encodes a record location.
#[must_use]
pub fn encode_location(location: &RecordLocation) -> Vec<u8> {
    let mut w = Writer::value();
    w.u64(location.segment.get());
    w.u64(location.offset);
    w.u32(location.len);
    w.0
}

/// Decodes a record location.
///
/// # Errors
///
/// Returns a [`CodecError`] if `bytes` is not an encoded location.
pub fn decode_location(bytes: &[u8]) -> Result<RecordLocation> {
    let mut r = Reader::value(bytes, "location")?;
    let location = RecordLocation {
        segment: SegmentId::new(r.u64("location.segment")?),
        offset: r.u64("location.offset")?,
        len: r.u32("location.len")?,
    };
    r.finish("location")?;
    Ok(location)
}

/// Encodes a shard's applied position.
#[must_use]
pub fn encode_applied(position: EpochSeq) -> Vec<u8> {
    let mut w = Writer::value();
    w.position(position);
    w.0
}

/// Decodes a shard's applied position.
///
/// # Errors
///
/// Returns a [`CodecError`] if `bytes` is not an encoded position.
pub fn decode_applied(bytes: &[u8]) -> Result<EpochSeq> {
    let mut r = Reader::value(bytes, "applied")?;
    let position = r.position("applied")?;
    r.finish("applied")?;
    Ok(position)
}

/// Encodes a coverage summary. Its run starts at offset zero, so only its
/// end and positions are stored.
///
/// # Errors
///
/// Returns a [`CodecError`] if the summary does not start at zero or names
/// too many shards.
pub fn encode_summary(summary: &SegmentSummary) -> Result<Vec<u8>> {
    if summary.start != 0 {
        return Err(CodecError::new("coverage", "does not start at zero"));
    }
    let mut w = Writer::value();
    w.u64(summary.end);
    w.len32(
        "coverage.positions",
        summary.positions.len(),
        MAX_SUMMARY_SHARDS,
    )?;
    for (shard, &position) in &summary.positions {
        w.shard(shard);
        w.position(position);
    }
    Ok(w.0)
}

/// Decodes a coverage summary.
///
/// # Errors
///
/// Returns a [`CodecError`] if `bytes` is not an encoded summary.
pub fn decode_summary(bytes: &[u8]) -> Result<SegmentSummary> {
    let mut r = Reader::value(bytes, "coverage")?;
    let end = r.u64("coverage.end")?;
    let count = r.len32("coverage.positions", MAX_SUMMARY_SHARDS)?;
    // A shard takes at least 3 bytes and a position 16.
    r.check_count("coverage.positions", count, 19)?;
    let mut positions = BTreeMap::new();
    for _ in 0..count {
        let shard = r.shard("coverage.positions")?;
        let position = r.position("coverage.positions")?;
        insert_sorted("coverage.positions", &mut positions, shard, position)?;
    }
    r.finish("coverage")?;
    Ok(SegmentSummary {
        start: 0,
        end,
        positions,
    })
}

/// Encodes a control-state copy.
///
/// # Errors
///
/// Returns a [`CodecError`] if the version or value is too long.
pub fn encode_control(entry: &ControlEntry) -> Result<Vec<u8>> {
    let mut w = Writer::value();
    w.u64(entry.generation.get());
    w.str16("control.version", &entry.version, MAX_CONTROL_VERSION_LEN)?;
    w.len32("control.value", entry.value.len(), MAX_CONTROL_VALUE_LEN)?;
    w.0.extend_from_slice(&entry.value);
    Ok(w.0)
}

/// Encodes a configuration of the gateway's shard map (§6.2).
///
/// # Errors
///
/// Returns a [`CodecError`] if its JSON is longer than
/// [`MAX_CONTROL_VALUE_LEN`].
pub fn encode_route(config: &ShardConfig) -> Result<Vec<u8>> {
    let json = config
        .to_json()
        .map_err(|error| CodecError::new("shard_map.config", error))?;
    let mut w = Writer::value();
    w.len32("shard_map.config", json.len(), MAX_CONTROL_VALUE_LEN)?;
    w.0.extend_from_slice(&json);
    Ok(w.0)
}

/// Decodes a configuration of the gateway's shard map, checked as a shard
/// register is.
///
/// # Errors
///
/// Returns a [`CodecError`] if `bytes` is not an encoded configuration.
pub fn decode_route(bytes: &[u8]) -> Result<ShardConfig> {
    let mut r = Reader::value(bytes, "shard_map")?;
    let len = r.len32("shard_map.config", MAX_CONTROL_VALUE_LEN)?;
    let json = r.take("shard_map.config", len)?;
    r.finish("shard_map")?;
    ShardConfig::from_json(json).map_err(|error| CodecError::new("shard_map.config", error))
}

/// Decodes a control-state copy.
///
/// # Errors
///
/// Returns a [`CodecError`] if `bytes` is not an encoded copy.
pub fn decode_control(bytes: &[u8]) -> Result<ControlEntry> {
    let mut r = Reader::value(bytes, "control")?;
    let generation = Generation::new(r.u64("control.generation")?);
    let version = r.str16("control.version", MAX_CONTROL_VERSION_LEN)?;
    let len = r.len32("control.value", MAX_CONTROL_VALUE_LEN)?;
    let value = r.take("control.value", len)?.to_vec();
    r.finish("control")?;
    Ok(ControlEntry {
        generation,
        version,
        value,
    })
}

/// Encodes a namespace entry.
///
/// # Errors
///
/// Returns a [`CodecError`] if a field is out of the bounds the log record
/// format sets (§10.1), exactly when decoding would reject the result.
pub fn encode_entry(entry: &Entry) -> Result<Vec<u8>> {
    let mut w = Writer::value();
    w.position(entry.version);
    w.u8(entry.state.code());
    write_option_etag(&mut w, "entry.remote_etag", entry.remote_etag.as_ref())?;
    w.option_str(
        "entry.remote_version_id",
        entry.remote_version_id.as_deref(),
        MAX_VERSION_ID_LEN,
    )?;
    w.present(entry.object.is_some());
    if let Some(object) = &entry.object {
        write_object(&mut w, object)?;
    }
    Ok(w.0)
}

/// Decodes a namespace entry.
///
/// # Errors
///
/// Returns a [`CodecError`] if `bytes` is not an encoded entry.
pub fn decode_entry(bytes: &[u8]) -> Result<Entry> {
    let mut r = Reader::value(bytes, "entry")?;
    let version = r.position("entry.version")?;
    let code = r.u8("entry.state")?;
    let state = EntryState::from_code(code)
        .ok_or_else(|| CodecError::new("entry.state", format_args!("invalid state {code}")))?;
    let remote_etag = r.option_etag("entry.remote_etag")?;
    let remote_version_id = r.option_str("entry.remote_version_id", MAX_VERSION_ID_LEN)?;
    let object = if r.present("entry.object")? {
        Some(read_object(&mut r)?)
    } else {
        None
    };
    r.finish("entry")?;
    Ok(Entry {
        version,
        state,
        object,
        remote_etag,
        remote_version_id,
    })
}

/// Returns the uploads-table key of the upload of `key` opened at `upload`
/// in `shard`.
#[must_use]
pub fn upload_key(shard: &ShardRef, key: &str, upload: EpochSeq) -> Vec<u8> {
    let mut bytes = entry_key(shard, key);
    bytes.push(0);
    push_position(&mut bytes, upload);
    bytes
}

/// Decodes an uploads-table key into its shard, object key, and upload.
///
/// # Errors
///
/// Returns a [`CodecError`] if the shard is malformed, the object key is
/// empty, too long, or not UTF-8, or the suffix is not a zero byte and a
/// position.
pub fn decode_upload_key(bytes: &[u8]) -> Result<(ShardRef, String, EpochSeq)> {
    let field = "upload key";
    let mut r = Reader(bytes, VALUE_FORMAT);
    let shard = r.shard(field)?;
    let Some(key_len) = r.0.len().checked_sub(UPLOAD_KEY_SUFFIX_LEN) else {
        return Err(CodecError::new(field, "truncated"));
    };
    if key_len == 0 {
        return Err(CodecError::new(field, "empty"));
    }
    Reader::check(field, key_len, MAX_KEY_LEN)?;
    let key = Reader::text(field, r.take(field, key_len)?)?;
    if r.u8(field)? != 0 {
        return Err(CodecError::new(field, "no separator"));
    }
    let upload = read_key_position(&mut r, field)?;
    r.finish(field)?;
    Ok((shard, key, upload))
}

/// Returns the parts-table key of part `number` of the upload opened at
/// `upload` in `shard`.
#[must_use]
pub fn part_key(shard: &ShardRef, upload: EpochSeq, number: u16) -> Vec<u8> {
    let mut bytes = upload_parts_prefix(shard, upload);
    bytes.extend_from_slice(&number.to_be_bytes());
    bytes
}

/// Returns the prefix of every parts-table key of the upload opened at
/// `upload` in `shard`.
#[must_use]
pub fn upload_parts_prefix(shard: &ShardRef, upload: EpochSeq) -> Vec<u8> {
    let mut bytes = shard_key(shard);
    push_position(&mut bytes, upload);
    bytes
}

/// Decodes a parts-table key into its shard, upload, and part number.
///
/// # Errors
///
/// Returns a [`CodecError`] if `bytes` is not a shard, a position, and a
/// part number.
pub fn decode_part_key(bytes: &[u8]) -> Result<(ShardRef, EpochSeq, u16)> {
    let field = "part key";
    let mut r = Reader(bytes, VALUE_FORMAT);
    let shard = r.shard(field)?;
    let upload = read_key_position(&mut r, field)?;
    let number = u16::from_be_bytes(r.array(field)?);
    r.finish(field)?;
    Ok((shard, upload, number))
}

fn push_position(bytes: &mut Vec<u8>, position: EpochSeq) {
    bytes.extend_from_slice(&position.epoch.get().to_be_bytes());
    bytes.extend_from_slice(&position.seq.get().to_be_bytes());
}

fn read_key_position(r: &mut Reader<'_>, field: &'static str) -> Result<EpochSeq> {
    let epoch = u64::from_be_bytes(r.array(field)?);
    let seq = u64::from_be_bytes(r.array(field)?);
    Ok(EpochSeq::new(Epoch::new(epoch), Seq::new(seq)))
}

/// Encodes an open multipart upload.
///
/// # Errors
///
/// Returns a [`CodecError`] if a field is out of the bounds the log record
/// format sets (§10.1), exactly when decoding would reject the result.
pub fn encode_upload(upload: &Upload) -> Result<Vec<u8>> {
    let mut w = Writer::value();
    w.u64(upload.initiated_ms);
    write_metadata(&mut w, &upload.metadata)?;
    write_tags(&mut w, &upload.tags)?;
    w.present(upload.checksum.is_some());
    if let Some(checksum) = upload.checksum {
        if !checksum.is_valid() {
            return Err(CodecError::new(
                "upload.checksum",
                "not a multipart checksum",
            ));
        }
        w.u8(checksum.algorithm.code());
        w.u8(u8::from(checksum.checksum_type == ChecksumType::Composite));
    }
    Ok(w.0)
}

/// Decodes an open multipart upload.
///
/// # Errors
///
/// Returns a [`CodecError`] if `bytes` is not an encoded upload of a format
/// that has uploads.
pub fn decode_upload(bytes: &[u8]) -> Result<Upload> {
    let mut r = Reader::value(bytes, "upload")?;
    check_has_uploads(&r, "upload")?;
    let initiated_ms = r.u64("upload.initiated_ms")?;
    let metadata = read_metadata(&mut r)?;
    let tags = read_tags(&mut r)?;
    let checksum = if r.present("upload.checksum")? {
        let code = r.u8("upload.checksum")?;
        let algorithm = ChecksumAlgorithm::from_code(code).ok_or_else(|| {
            CodecError::new("upload.checksum", format_args!("invalid algorithm {code}"))
        })?;
        let checksum_type = match r.u8("upload.checksum")? {
            0 => ChecksumType::FullObject,
            1 => ChecksumType::Composite,
            tag => {
                return Err(CodecError::new(
                    "upload.checksum",
                    format_args!("invalid type {tag}"),
                ));
            }
        };
        let checksum = UploadChecksum {
            algorithm,
            checksum_type,
        };
        if !checksum.is_valid() {
            return Err(CodecError::new(
                "upload.checksum",
                "not a multipart checksum",
            ));
        }
        Some(checksum)
    } else {
        None
    };
    r.finish("upload")?;
    Ok(Upload {
        initiated_ms,
        metadata,
        tags,
        checksum,
    })
}

/// Encodes a part of a multipart upload.
///
/// # Errors
///
/// Returns a [`CodecError`] if a field is out of bounds, or the payload is
/// neither inline nor in extents.
pub fn encode_part(part: &Part) -> Result<Vec<u8>> {
    if !matches!(part.payload, Payload::Inline(_) | Payload::Extents(_)) {
        return Err(CodecError::new(
            "part.payload",
            "a part's bytes are inline or in extents",
        ));
    }
    let mut w = Writer::value();
    w.position(part.position);
    w.u64(part.size);
    w.u64(part.last_modified_ms);
    write_etag(&mut w, "part.etag", &part.etag)?;
    write_checksums(&mut w, &part.checksums)?;
    write_payload(&mut w, &part.payload)?;
    Ok(w.0)
}

/// Decodes a part of a multipart upload.
///
/// # Errors
///
/// Returns a [`CodecError`] if `bytes` is not an encoded part of a format
/// that has uploads.
pub fn decode_part(bytes: &[u8]) -> Result<Part> {
    let mut r = Reader::value(bytes, "part")?;
    check_has_uploads(&r, "part")?;
    let part = Part {
        position: r.position("part.position")?,
        size: r.u64("part.size")?,
        last_modified_ms: r.u64("part.last_modified_ms")?,
        etag: r.etag("part.etag")?,
        checksums: read_checksums(&mut r)?,
        payload: read_payload(&mut r)?,
    };
    if !matches!(part.payload, Payload::Inline(_) | Payload::Extents(_)) {
        return Err(CodecError::new(
            "part.payload",
            "a part's bytes are inline or in extents",
        ));
    }
    r.finish("part")?;
    Ok(part)
}

fn check_has_uploads(r: &Reader<'_>, field: &'static str) -> Result<()> {
    if r.1 < UPLOADS_SINCE {
        return Err(CodecError::new(
            field,
            format_args!("value format {} has no uploads", r.1),
        ));
    }
    Ok(())
}

fn write_etag(w: &mut Writer, field: &'static str, etag: &ETag) -> Result<()> {
    w.str16(field, etag.as_str(), ETag::MAX_LEN)
}

fn write_option_etag(w: &mut Writer, field: &'static str, etag: Option<&ETag>) -> Result<()> {
    w.present(etag.is_some());
    etag.map_or(Ok(()), |etag| write_etag(w, field, etag))
}

fn write_object(w: &mut Writer, object: &ObjectVersion) -> Result<()> {
    w.u64(object.size);
    w.u64(object.last_modified_ms);
    write_etag(w, "object.local_etag", &object.local_etag)?;
    w.present(object.write_identity.is_some());
    if let Some(identity) = object.write_identity {
        w.position(identity);
    }
    write_metadata(w, &object.metadata)?;
    write_tags(w, &object.tags)?;
    write_checksums(w, &object.checksums)?;
    w.option_str(
        "object.storage_class",
        object.storage_class.as_deref(),
        MAX_STORAGE_CLASS_LEN,
    )?;
    w.present(object.copy_source.is_some());
    if let Some(source) = &object.copy_source {
        let bucket = source.bucket.as_str();
        w.len8("object.copy_source.bucket", bucket.len(), BucketId::MAX_LEN)?;
        w.0.extend_from_slice(bucket.as_bytes());
        w.str16("object.copy_source.key", &source.key, MAX_KEY_LEN)?;
        w.u64(source.version.seq.get());
        write_etag(w, "object.copy_source.etag", &source.version.etag)?;
        write_option_etag(
            w,
            "object.copy_source.remote_etag",
            source.remote_etag.as_ref(),
        )?;
    }
    write_payload(w, &object.payload)
}

fn write_payload(w: &mut Writer, payload: &Payload) -> Result<()> {
    match payload {
        Payload::None => w.u8(PAYLOAD_NONE),
        Payload::Inline(position) => {
            w.u8(PAYLOAD_INLINE);
            w.position(*position);
        }
        Payload::Extents(extents) => {
            w.u8(PAYLOAD_EXTENTS);
            w.len32("object.extents", extents.len(), MAX_EXTENTS)?;
            for extent in extents {
                check_extent_len(extent.len)?;
                w.position(extent.position);
                w.u32(extent.len);
            }
        }
        Payload::Parts { upload, parts } => {
            w.u8(PAYLOAD_PARTS);
            w.position(*upload);
            w.len16("object.parts", parts.len(), MAX_PARTS as usize)?;
            let mut previous = 0;
            for part in parts {
                check_part_number(part.number, previous)?;
                previous = part.number;
                w.u16(part.number);
                w.u64(part.size);
            }
        }
    }
    Ok(())
}

fn read_payload(r: &mut Reader<'_>) -> Result<Payload> {
    Ok(match r.u8("object.payload")? {
        PAYLOAD_NONE => Payload::None,
        PAYLOAD_INLINE => Payload::Inline(r.position("object.payload")?),
        PAYLOAD_EXTENTS => {
            let count = r.len32("object.extents", MAX_EXTENTS)?;
            r.check_count("object.extents", count, 20)?;
            let mut extents = Vec::with_capacity(count);
            for _ in 0..count {
                let position = r.position("object.extents")?;
                let len = r.u32("object.extents")?;
                check_extent_len(len)?;
                extents.push(ExtentRef { position, len });
            }
            Payload::Extents(extents)
        }
        PAYLOAD_PARTS if r.1 >= UPLOADS_SINCE => {
            let upload = r.position("object.parts")?;
            let count = r.len16("object.parts", MAX_PARTS as usize)?;
            // A part is a number and a size.
            r.check_count("object.parts", count, 10)?;
            let mut parts = Vec::with_capacity(count);
            let mut previous = 0;
            for _ in 0..count {
                let number = r.u16("object.parts")?;
                check_part_number(number, previous)?;
                previous = number;
                parts.push(ObjectPart {
                    number,
                    size: r.u64("object.parts")?,
                });
            }
            Payload::Parts { upload, parts }
        }
        tag => {
            return Err(CodecError::new(
                "object.payload",
                format_args!("invalid tag {tag}"),
            ));
        }
    })
}

/// Checks that a part number is valid and follows `previous`.
fn check_part_number(number: u16, previous: u16) -> Result<()> {
    if number <= previous || u32::from(number) > MAX_PARTS {
        return Err(CodecError::new(
            "object.parts",
            format_args!("part {number} after part {previous}"),
        ));
    }
    Ok(())
}

fn read_object(r: &mut Reader<'_>) -> Result<ObjectVersion> {
    let size = r.u64("object.size")?;
    let last_modified_ms = r.u64("object.last_modified_ms")?;
    let local_etag = r.etag("object.local_etag")?;
    let write_identity = if r.present("object.write_identity")? {
        Some(r.position("object.write_identity")?)
    } else {
        None
    };
    let metadata = read_metadata(r)?;
    let tags = read_tags(r)?;
    let checksums = read_checksums(r)?;
    let storage_class = r.option_str("object.storage_class", MAX_STORAGE_CLASS_LEN)?;
    let copy_source = if r.present("object.copy_source")? {
        let len = r.len8("object.copy_source.bucket", BucketId::MAX_LEN)?;
        let bucket = Reader::text(
            "object.copy_source.bucket",
            r.take("object.copy_source.bucket", len)?,
        )?;
        let bucket =
            BucketId::new(bucket).map_err(|e| CodecError::new("object.copy_source.bucket", e))?;
        let key = r.str16("object.copy_source.key", MAX_KEY_LEN)?;
        let seq = Seq::new(r.u64("object.copy_source.seq")?);
        let etag = r.etag("object.copy_source.etag")?;
        let remote_etag = r.option_etag("object.copy_source.remote_etag")?;
        Some(CopySource {
            bucket,
            key,
            version: VersionIdentity::new(seq, etag),
            remote_etag,
        })
    } else {
        None
    };
    let payload = read_payload(r)?;
    Ok(ObjectVersion {
        size,
        last_modified_ms,
        local_etag,
        write_identity,
        metadata,
        tags,
        checksums,
        storage_class,
        copy_source,
        payload,
    })
}

fn check_extent_len(len: u32) -> Result<()> {
    if len == 0 || len > MAX_PAYLOAD_LEN {
        return Err(CodecError::new(
            "object.extents",
            format_args!("an extent of {len} bytes"),
        ));
    }
    Ok(())
}

fn write_metadata(w: &mut Writer, metadata: &Metadata) -> Result<()> {
    let field = "object.metadata";
    w.len16(field, metadata.len(), MAX_METADATA_LEN)?;
    let mut total = 0usize;
    for (name, value) in metadata {
        total = total.saturating_add(name.len() + value.len());
        Reader::check(field, total, MAX_METADATA_LEN)?;
        w.str16(field, name, MAX_METADATA_LEN)?;
        w.str16(field, value, MAX_METADATA_LEN)?;
    }
    Ok(())
}

fn read_metadata(r: &mut Reader<'_>) -> Result<Metadata> {
    let field = "object.metadata";
    let count = r.len16(field, MAX_METADATA_LEN)?;
    let mut metadata = Metadata::new();
    let mut total = 0usize;
    for _ in 0..count {
        let name = r.str16(field, MAX_METADATA_LEN)?;
        let value = r.str16(field, MAX_METADATA_LEN)?;
        total = total.saturating_add(name.len() + value.len());
        Reader::check(field, total, MAX_METADATA_LEN)?;
        insert_sorted(field, &mut metadata, name, value)?;
    }
    Ok(metadata)
}

fn write_tags(w: &mut Writer, tags: &TagSet) -> Result<()> {
    let field = "object.tags";
    w.len8(field, tags.len(), MAX_TAGS)?;
    for (key, value) in tags {
        w.str16(field, key, MAX_TAG_KEY_LEN)?;
        w.str16(field, value, MAX_TAG_VALUE_LEN)?;
    }
    Ok(())
}

fn read_tags(r: &mut Reader<'_>) -> Result<TagSet> {
    let field = "object.tags";
    let count = r.len8(field, MAX_TAGS)?;
    let mut tags = TagSet::new();
    for _ in 0..count {
        let key = r.str16(field, MAX_TAG_KEY_LEN)?;
        let value = r.str16(field, MAX_TAG_VALUE_LEN)?;
        insert_sorted(field, &mut tags, key, value)?;
    }
    Ok(tags)
}

fn write_checksums(w: &mut Writer, checksums: &Checksums) -> Result<()> {
    let field = "object.checksums";
    w.len8(field, checksums.len(), ChecksumAlgorithm::ALL.len())?;
    for (&algorithm, checksum) in checksums {
        checksum
            .check(algorithm)
            .map_err(|e| CodecError::new(field, e))?;
        w.u8(algorithm.code());
        w.0.extend_from_slice(checksum.digest());
        w.u16(checksum.parts().unwrap_or(0));
    }
    Ok(())
}

/// Reads checksums: a count, then for each its algorithm's code, the
/// digest, and, from format 2, the part count of a composite checksum
/// (zero for `FULL_OBJECT`).
fn read_checksums(r: &mut Reader<'_>) -> Result<Checksums> {
    let field = "object.checksums";
    let part_counts = r.1 >= PART_COUNTS_SINCE;
    let count = r.len8(field, ChecksumAlgorithm::ALL.len())?;
    let mut checksums = Checksums::new();
    for _ in 0..count {
        let code = r.u8(field)?;
        let algorithm = ChecksumAlgorithm::from_code(code)
            .ok_or_else(|| CodecError::new(field, format_args!("invalid algorithm {code}")))?;
        let digest = r.take(field, algorithm.digest_len())?;
        let parts = if part_counts { r.u16(field)? } else { 0 };
        let checksum = match parts {
            0 => Checksum::full_object(algorithm, digest),
            parts => Checksum::composite(algorithm, digest, parts.into()),
        }
        .map_err(|e| CodecError::new(field, e))?;
        insert_sorted(field, &mut checksums, algorithm, checksum)?;
    }
    Ok(checksums)
}
