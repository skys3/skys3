//! The `prost` encoding of message headers, and its conversion to and from
//! [`Message`]. Field numbers are part of the wire format and are never
//! reused for another meaning.

use std::collections::BTreeMap;

use bytes::Bytes;
use prost::{Message as _, Oneof};
use skys3_types::checksum::{Checksum, ChecksumAlgorithm, Checksums};
use skys3_types::{BucketName, ClusterId, ETag, WriteIdentity};

use crate::error::{MessageError, ensure};
use crate::message::{
    Abort, AbortReason, Applied, ApplyError, Batch, Begin, Commit, Data, Hello, Message, Outcome,
    Precondition, Put, PutData, StagedPart, StagedRanges, Write,
};
use crate::negotiation::{Capabilities, VersionRange};
use crate::ranges::ByteRanges;

/// A message header: exactly one message.
#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct Envelope {
    #[prost(oneof = "WireMessage", tags = "1, 2, 3, 4, 5, 6, 7, 8, 9")]
    body: Option<WireMessage>,
}

#[derive(Clone, PartialEq, Oneof)]
enum WireMessage {
    #[prost(message, tag = "1")]
    Hello(WireHello),
    #[prost(message, tag = "2")]
    Begin(WireBegin),
    #[prost(message, tag = "3")]
    Data(WireData),
    #[prost(message, tag = "4")]
    Durable(WireRanges),
    #[prost(message, tag = "5")]
    Resume(WireRanges),
    #[prost(message, tag = "6")]
    Commit(WireCommit),
    #[prost(message, tag = "7")]
    Applied(WireApplied),
    #[prost(message, tag = "8")]
    Batch(WireBatch),
    #[prost(message, tag = "9")]
    Abort(WireAbort),
}

#[derive(Clone, PartialEq, prost::Message)]
struct WireHello {
    #[prost(string, tag = "1")]
    cluster: String,
    #[prost(uint32, tag = "2")]
    min_version: u32,
    #[prost(uint32, tag = "3")]
    max_version: u32,
    #[prost(uint64, tag = "4")]
    capabilities: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct WireBegin {
    #[prost(string, tag = "1")]
    identity: String,
    #[prost(string, tag = "2")]
    bucket: String,
    #[prost(string, tag = "3")]
    key: String,
}

/// `DATA`'s header; its bytes are the frame's payload.
#[derive(Clone, PartialEq, prost::Message)]
struct WireData {
    #[prost(uint64, tag = "1")]
    piece: u64,
    #[prost(uint64, tag = "2")]
    offset: u64,
    #[prost(fixed32, tag = "3")]
    crc32c: u32,
}

#[derive(Clone, PartialEq, prost::Message)]
struct WireRanges {
    #[prost(string, tag = "1")]
    identity: String,
    #[prost(message, repeated, tag = "2")]
    pieces: Vec<WirePiece>,
}

/// A piece's ranges, flattened as `start, end, start, end, ...`.
#[derive(Clone, PartialEq, prost::Message)]
struct WirePiece {
    #[prost(uint64, tag = "1")]
    piece: u64,
    #[prost(uint64, repeated, tag = "2")]
    bounds: Vec<u64>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct WireCommit {
    #[prost(string, tag = "1")]
    identity: String,
    #[prost(string, tag = "2")]
    bucket: String,
    #[prost(string, tag = "3")]
    key: String,
    #[prost(oneof = "WirePrecondition", tags = "4, 5, 6")]
    precondition: Option<WirePrecondition>,
    #[prost(oneof = "WireWrite", tags = "7, 8")]
    write: Option<WireWrite>,
}

#[derive(Clone, PartialEq, Oneof)]
enum WirePrecondition {
    #[prost(bool, tag = "4")]
    Absent(bool),
    #[prost(string, tag = "5")]
    Matches(String),
    #[prost(bool, tag = "6")]
    Unconditional(bool),
}

#[derive(Clone, PartialEq, Oneof)]
enum WireWrite {
    #[prost(message, tag = "7")]
    Put(WirePut),
    #[prost(bool, tag = "8")]
    Delete(bool),
}

#[derive(Clone, PartialEq, prost::Message)]
struct WirePut {
    #[prost(uint64, tag = "1")]
    size: u64,
    #[prost(string, tag = "2")]
    etag: String,
    #[prost(uint64, tag = "3")]
    last_modified_ms: u64,
    #[prost(message, repeated, tag = "4")]
    metadata: Vec<WireEntry>,
    #[prost(message, repeated, tag = "5")]
    tags: Vec<WireEntry>,
    #[prost(message, repeated, tag = "6")]
    checksums: Vec<WireChecksum>,
    #[prost(oneof = "WirePutData", tags = "7, 8, 9")]
    data: Option<WirePutData>,
}

/// Where a `WirePut`'s bytes are. An inline body is the next `len` bytes of
/// the `BATCH` payload.
#[derive(Clone, PartialEq, Oneof)]
enum WirePutData {
    #[prost(uint64, tag = "7")]
    StagedPiece(u64),
    #[prost(message, tag = "8")]
    Multipart(WireParts),
    #[prost(uint64, tag = "9")]
    InlineLen(u64),
}

#[derive(Clone, PartialEq, prost::Message)]
struct WireParts {
    #[prost(message, repeated, tag = "1")]
    parts: Vec<WirePart>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct WirePart {
    #[prost(uint32, tag = "1")]
    number: u32,
    #[prost(uint64, tag = "2")]
    piece: u64,
    #[prost(uint64, tag = "3")]
    size: u64,
    #[prost(bytes = "vec", tag = "4")]
    md5: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct WireEntry {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    value: String,
}

/// A checksum: the algorithm's code (as in log records), the digest, and
/// the part count of a composite checksum, 0 for `FULL_OBJECT`.
#[derive(Clone, PartialEq, prost::Message)]
struct WireChecksum {
    #[prost(uint32, tag = "1")]
    algorithm: u32,
    #[prost(bytes = "vec", tag = "2")]
    digest: Vec<u8>,
    #[prost(uint32, tag = "3")]
    parts: u32,
}

#[derive(Clone, PartialEq, prost::Message)]
struct WireApplied {
    #[prost(string, tag = "1")]
    identity: String,
    #[prost(oneof = "WireOutcome", tags = "2, 3, 4")]
    outcome: Option<WireOutcome>,
}

#[derive(Clone, PartialEq, Oneof)]
enum WireOutcome {
    #[prost(message, tag = "2")]
    Committed(WireCommitted),
    #[prost(message, tag = "3")]
    PreconditionFailed(WirePreconditionFailed),
    #[prost(message, tag = "4")]
    Failed(WireFailed),
}

#[derive(Clone, PartialEq, prost::Message)]
struct WireCommitted {
    #[prost(string, optional, tag = "1")]
    etag: Option<String>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct WirePreconditionFailed {
    #[prost(string, optional, tag = "1")]
    current: Option<String>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct WireFailed {
    #[prost(uint32, tag = "1")]
    error: u32,
    #[prost(string, tag = "2")]
    reason: String,
}

/// `BATCH`'s header; the items' inline bytes, in item order, are the
/// frame's payload.
#[derive(Clone, PartialEq, prost::Message)]
struct WireBatch {
    #[prost(message, repeated, tag = "1")]
    items: Vec<WireCommit>,
    #[prost(fixed32, tag = "2")]
    crc32c: u32,
}

#[derive(Clone, PartialEq, prost::Message)]
struct WireAbort {
    #[prost(string, tag = "1")]
    identity: String,
    #[prost(uint32, tag = "2")]
    reason: u32,
    #[prost(string, tag = "3")]
    detail: String,
}

/// The wire codes of [`ApplyError`].
const APPLY_ERRORS: [(u32, ApplyError); 4] = [
    (1, ApplyError::Incomplete),
    (2, ApplyError::ChecksumMismatch),
    (3, ApplyError::Refused),
    (4, ApplyError::Unavailable),
];

/// The wire codes of [`AbortReason`].
const ABORT_REASONS: [(u32, AbortReason); 4] = [
    (1, AbortReason::Cancelled),
    (2, AbortReason::Expired),
    (3, AbortReason::QuotaExceeded),
    (4, AbortReason::Refused),
];

fn code_of<T: PartialEq>(table: &[(u32, T)], value: &T) -> u32 {
    table
        .iter()
        .find(|(_, v)| v == value)
        .map(|(code, _)| *code)
        .expect("every value has a code")
}

fn value_of<T: Copy>(
    table: &[(u32, T)],
    field: &'static str,
    code: u32,
) -> Result<T, MessageError> {
    table
        .iter()
        .find(|(c, _)| *c == code)
        .map(|(_, value)| *value)
        .ok_or_else(|| MessageError::invalid(field, format_args!("unknown code {code}")))
}

impl Envelope {
    /// The header of `message`, and its payload as the chunks it is made
    /// of.
    pub(crate) fn encode(message: &Message) -> (Self, Vec<Bytes>) {
        let mut payload = Vec::new();
        let body = match message {
            Message::Hello(hello) => WireMessage::Hello(WireHello {
                cluster: hello.cluster.as_str().to_owned(),
                min_version: hello.versions.min().into(),
                max_version: hello.versions.max().into(),
                capabilities: hello.capabilities.bits(),
            }),
            Message::Begin(begin) => WireMessage::Begin(WireBegin {
                identity: begin.identity.to_string(),
                bucket: begin.bucket.as_str().to_owned(),
                key: begin.key.clone(),
            }),
            Message::Data(data) => {
                payload.push(data.bytes.clone());
                WireMessage::Data(WireData {
                    piece: data.piece,
                    offset: data.offset,
                    crc32c: crc32c::crc32c(&data.bytes),
                })
            }
            Message::Durable(ranges) => WireMessage::Durable(encode_ranges(ranges)),
            Message::Resume(ranges) => WireMessage::Resume(encode_ranges(ranges)),
            Message::Commit(commit) => WireMessage::Commit(encode_commit(commit, &mut payload)),
            Message::Applied(applied) => WireMessage::Applied(encode_applied(applied)),
            Message::Batch(batch) => {
                let items = batch
                    .items
                    .iter()
                    .map(|item| encode_commit(item, &mut payload))
                    .collect();
                let crc32c = payload
                    .iter()
                    .fold(0, |crc, chunk| crc32c::crc32c_append(crc, chunk));
                WireMessage::Batch(WireBatch { items, crc32c })
            }
            Message::Abort(abort) => WireMessage::Abort(WireAbort {
                identity: abort.identity.to_string(),
                reason: code_of(&ABORT_REASONS, &abort.reason),
                detail: abort.detail.clone(),
            }),
        };
        (Self { body: Some(body) }, payload)
    }

    /// Decodes a header, before its payload has arrived.
    pub(crate) fn decode_header(header: &[u8]) -> Result<Self, MessageError> {
        let envelope = Self::decode(header)?;
        if envelope.body.is_none() {
            return Err(MessageError::UnknownMessage);
        }
        Ok(envelope)
    }

    /// The message of this header and `payload`, checked.
    pub(crate) fn into_message(self, payload: Bytes) -> Result<Message, MessageError> {
        let mut rest = payload.clone();
        let message = match self.body.ok_or(MessageError::UnknownMessage)? {
            WireMessage::Hello(hello) => Message::Hello(decode_hello(hello)?),
            WireMessage::Begin(begin) => Message::Begin(Begin {
                identity: identity("begin.identity", &begin.identity)?,
                bucket: bucket("begin.bucket", begin.bucket)?,
                key: begin.key,
            }),
            WireMessage::Data(data) => {
                check_crc(data.crc32c, &payload)?;
                rest.clear();
                Message::Data(Data {
                    piece: data.piece,
                    offset: data.offset,
                    bytes: payload,
                })
            }
            WireMessage::Durable(ranges) => Message::Durable(decode_ranges(ranges)?),
            WireMessage::Resume(ranges) => Message::Resume(decode_ranges(ranges)?),
            WireMessage::Commit(commit) => Message::Commit(decode_commit(commit, &mut rest)?),
            WireMessage::Applied(applied) => Message::Applied(decode_applied(applied)?),
            WireMessage::Batch(batch) => {
                check_crc(batch.crc32c, &payload)?;
                let items = batch
                    .items
                    .into_iter()
                    .map(|item| decode_commit(item, &mut rest))
                    .collect::<Result<_, _>>()?;
                Message::Batch(Batch { items })
            }
            WireMessage::Abort(abort) => Message::Abort(Abort {
                identity: identity("abort.identity", &abort.identity)?,
                reason: value_of(&ABORT_REASONS, "abort.reason", abort.reason)?,
                detail: abort.detail,
            }),
        };
        ensure(rest.is_empty(), "payload", || {
            format!(
                "{} bytes are left over after the {}",
                rest.len(),
                message.name()
            )
        })?;
        message.validate()?;
        Ok(message)
    }
}

fn check_crc(expected: u32, payload: &[u8]) -> Result<(), MessageError> {
    let actual = crc32c::crc32c(payload);
    if actual == expected {
        Ok(())
    } else {
        Err(MessageError::ChecksumMismatch { expected, actual })
    }
}

fn identity(field: &'static str, text: &str) -> Result<WriteIdentity, MessageError> {
    text.parse()
        .map_err(|error| MessageError::invalid(field, error))
}

fn bucket(field: &'static str, name: String) -> Result<BucketName, MessageError> {
    BucketName::new(name).map_err(|error| MessageError::invalid(field, error))
}

fn etag(field: &'static str, value: String) -> Result<ETag, MessageError> {
    ETag::new(value).map_err(|error| MessageError::invalid(field, error))
}

fn decode_hello(hello: WireHello) -> Result<Hello, MessageError> {
    let versions = u16::try_from(hello.min_version)
        .ok()
        .zip(u16::try_from(hello.max_version).ok())
        .and_then(|(min, max)| VersionRange::new(min, max))
        .ok_or_else(|| {
            MessageError::invalid(
                "hello.versions",
                format_args!("{}..={}", hello.min_version, hello.max_version),
            )
        })?;
    Ok(Hello {
        cluster: ClusterId::new(hello.cluster)
            .map_err(|error| MessageError::invalid("hello.cluster", error))?,
        versions,
        capabilities: Capabilities::from_bits(hello.capabilities),
    })
}

fn encode_ranges(ranges: &StagedRanges) -> WireRanges {
    WireRanges {
        identity: ranges.identity.to_string(),
        pieces: ranges
            .pieces
            .iter()
            .map(|(&piece, ranges)| WirePiece {
                piece,
                bounds: ranges
                    .as_slice()
                    .iter()
                    .flat_map(|r| [r.start, r.end])
                    .collect(),
            })
            .collect(),
    }
}

fn decode_ranges(wire: WireRanges) -> Result<StagedRanges, MessageError> {
    const FIELD: &str = "ranges.pieces";
    let mut pieces = BTreeMap::new();
    for piece in wire.pieces {
        let (pairs, []) = piece.bounds.as_chunks::<2>() else {
            return Err(MessageError::invalid(FIELD, "an odd number of bounds"));
        };
        let ranges = pairs.iter().map(|&[start, end]| start..end).collect();
        let ranges = ByteRanges::from_canonical(ranges).map_err(|ranges| {
            MessageError::invalid(FIELD, format_args!("{ranges:?} are not canonical"))
        })?;
        ensure(pieces.insert(piece.piece, ranges).is_none(), FIELD, || {
            format!("piece {} is listed twice", piece.piece)
        })?;
    }
    Ok(StagedRanges {
        identity: identity("ranges.identity", &wire.identity)?,
        pieces,
    })
}

fn encode_commit(commit: &Commit, payload: &mut Vec<Bytes>) -> WireCommit {
    let precondition = match &commit.precondition {
        Precondition::Absent => WirePrecondition::Absent(true),
        Precondition::Matches(identity) => WirePrecondition::Matches(identity.to_string()),
        Precondition::Unconditional => WirePrecondition::Unconditional(true),
    };
    let write = match &commit.write {
        Write::Put(put) => WireWrite::Put(encode_put(put, payload)),
        Write::Delete => WireWrite::Delete(true),
    };
    WireCommit {
        identity: commit.identity.to_string(),
        bucket: commit.bucket.as_str().to_owned(),
        key: commit.key.clone(),
        precondition: Some(precondition),
        write: Some(write),
    }
}

fn encode_put(put: &Put, payload: &mut Vec<Bytes>) -> WirePut {
    let entries = |map: &BTreeMap<String, String>| {
        map.iter()
            .map(|(name, value)| WireEntry {
                name: name.clone(),
                value: value.clone(),
            })
            .collect()
    };
    let data = match &put.data {
        PutData::Staged { piece } => WirePutData::StagedPiece(*piece),
        PutData::Multipart(parts) => WirePutData::Multipart(WireParts {
            parts: parts
                .iter()
                .map(|part| WirePart {
                    number: part.number.into(),
                    piece: part.piece,
                    size: part.size,
                    md5: part.md5.to_vec(),
                })
                .collect(),
        }),
        PutData::Inline(bytes) => {
            payload.push(bytes.clone());
            WirePutData::InlineLen(bytes.len() as u64)
        }
    };
    WirePut {
        size: put.size,
        etag: put.etag.as_str().to_owned(),
        last_modified_ms: put.last_modified_ms,
        metadata: entries(&put.metadata),
        tags: entries(&put.tags),
        checksums: put
            .checksums
            .iter()
            .map(|(algorithm, checksum)| WireChecksum {
                algorithm: algorithm.code().into(),
                digest: checksum.digest().to_vec(),
                parts: checksum.parts().map_or(0, u32::from),
            })
            .collect(),
        data: Some(data),
    }
}

/// Decodes a `COMMIT` whose inline bytes, if any, are the first bytes of
/// `payload`, and takes them from it.
fn decode_commit(wire: WireCommit, payload: &mut Bytes) -> Result<Commit, MessageError> {
    let precondition = match wire.precondition {
        Some(WirePrecondition::Absent(_)) => Precondition::Absent,
        Some(WirePrecondition::Matches(text)) => {
            Precondition::Matches(identity("commit.precondition", &text)?)
        }
        Some(WirePrecondition::Unconditional(_)) => Precondition::Unconditional,
        None => return Err(MessageError::invalid("commit.precondition", "is missing")),
    };
    let write = match wire.write {
        Some(WireWrite::Put(put)) => Write::Put(decode_put(put, payload)?),
        Some(WireWrite::Delete(_)) => Write::Delete,
        None => return Err(MessageError::invalid("commit.write", "is missing")),
    };
    Ok(Commit {
        identity: identity("commit.identity", &wire.identity)?,
        bucket: bucket("commit.bucket", wire.bucket)?,
        key: wire.key,
        precondition,
        write,
    })
}

fn decode_put(wire: WirePut, payload: &mut Bytes) -> Result<Put, MessageError> {
    let data = match wire.data {
        Some(WirePutData::StagedPiece(piece)) => PutData::Staged { piece },
        Some(WirePutData::Multipart(parts)) => PutData::Multipart(
            parts
                .parts
                .into_iter()
                .map(decode_part)
                .collect::<Result<_, _>>()?,
        ),
        Some(WirePutData::InlineLen(len)) => {
            let len = usize::try_from(len)
                .ok()
                .filter(|&len| len <= payload.len())
                .ok_or_else(|| {
                    MessageError::invalid(
                        "put.data",
                        format_args!("{len} inline bytes, but {} are left", payload.len()),
                    )
                })?;
            PutData::Inline(payload.split_to(len))
        }
        None => return Err(MessageError::invalid("put.data", "is missing")),
    };
    Ok(Put {
        size: wire.size,
        etag: etag("put.etag", wire.etag)?,
        last_modified_ms: wire.last_modified_ms,
        metadata: decode_map("put.metadata", wire.metadata)?,
        tags: decode_map("put.tags", wire.tags)?,
        checksums: decode_checksums(wire.checksums)?,
        data,
    })
}

fn decode_part(wire: WirePart) -> Result<StagedPart, MessageError> {
    const FIELD: &str = "put.parts";
    Ok(StagedPart {
        number: u16::try_from(wire.number).map_err(|_| {
            MessageError::invalid(FIELD, format_args!("part number {}", wire.number))
        })?,
        piece: wire.piece,
        size: wire.size,
        md5: wire.md5.try_into().map_err(|md5: Vec<u8>| {
            MessageError::invalid(FIELD, format_args!("an MD5 of {} bytes", md5.len()))
        })?,
    })
}

fn decode_map(
    field: &'static str,
    entries: Vec<WireEntry>,
) -> Result<BTreeMap<String, String>, MessageError> {
    let mut map = BTreeMap::new();
    for entry in entries {
        let name = entry.name;
        ensure(!map.contains_key(&name), field, || {
            format!("{name:?} is listed twice")
        })?;
        map.insert(name, entry.value);
    }
    Ok(map)
}

fn decode_checksums(wire: Vec<WireChecksum>) -> Result<Checksums, MessageError> {
    const FIELD: &str = "put.checksums";
    let mut checksums = Checksums::new();
    for entry in wire {
        let algorithm = u8::try_from(entry.algorithm)
            .ok()
            .and_then(ChecksumAlgorithm::from_code)
            .ok_or_else(|| {
                MessageError::invalid(FIELD, format_args!("unknown algorithm {}", entry.algorithm))
            })?;
        let checksum = match entry.parts {
            0 => Checksum::full_object(algorithm, &entry.digest),
            parts => Checksum::composite(algorithm, &entry.digest, parts),
        }
        .map_err(|error| MessageError::invalid(FIELD, error))?;
        ensure(
            checksums.insert(algorithm, checksum).is_none(),
            FIELD,
            || format!("{algorithm} is listed twice"),
        )?;
    }
    Ok(checksums)
}

fn encode_applied(applied: &Applied) -> WireApplied {
    let outcome = match &applied.outcome {
        Outcome::Committed { etag } => WireOutcome::Committed(WireCommitted {
            etag: etag.as_ref().map(|etag| etag.as_str().to_owned()),
        }),
        Outcome::PreconditionFailed { current } => {
            WireOutcome::PreconditionFailed(WirePreconditionFailed {
                current: current.as_ref().map(ToString::to_string),
            })
        }
        Outcome::Failed { error, reason } => WireOutcome::Failed(WireFailed {
            error: code_of(&APPLY_ERRORS, error),
            reason: reason.clone(),
        }),
    };
    WireApplied {
        identity: applied.identity.to_string(),
        outcome: Some(outcome),
    }
}

fn decode_applied(wire: WireApplied) -> Result<Applied, MessageError> {
    let outcome = match wire.outcome {
        Some(WireOutcome::Committed(committed)) => Outcome::Committed {
            etag: committed
                .etag
                .map(|value| etag("applied.etag", value))
                .transpose()?,
        },
        Some(WireOutcome::PreconditionFailed(failed)) => Outcome::PreconditionFailed {
            current: failed
                .current
                .map(|text| identity("applied.current", &text))
                .transpose()?,
        },
        Some(WireOutcome::Failed(failed)) => Outcome::Failed {
            error: value_of(&APPLY_ERRORS, "applied.error", failed.error)?,
            reason: failed.reason,
        },
        None => return Err(MessageError::invalid("applied.outcome", "is missing")),
    };
    Ok(Applied {
        identity: identity("applied.identity", &wire.identity)?,
        outcome,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{MAX_HEADER_LEN, MAX_PAYLOAD_LEN};

    const IDENTITY: &str = "prod-us/b-7f3a/5/42.1001";

    /// A frame of `body` and `payload`, encoded without any checks.
    fn frame(body: Option<WireMessage>, payload: &[u8]) -> Vec<u8> {
        let header = Envelope { body }.encode_to_vec();
        let mut out = (header.len() as u32).to_be_bytes().to_vec();
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(&header);
        out.extend_from_slice(payload);
        out
    }

    #[track_caller]
    fn invalid(body: WireMessage, payload: &[u8], field: &str) {
        match Message::decode(&frame(Some(body), payload)) {
            Err(MessageError::Invalid { field: f, problem }) => assert_eq!(f, field, "{problem}"),
            other => panic!("expected an invalid {field}, got {other:?}"),
        }
    }

    #[track_caller]
    fn valid(body: WireMessage, payload: &[u8]) -> Message {
        let (message, _) = Message::decode(&frame(Some(body), payload))
            .unwrap()
            .unwrap();
        message
    }

    fn hello(min_version: u32, max_version: u32) -> WireMessage {
        WireMessage::Hello(WireHello {
            cluster: "prod-eu".to_owned(),
            min_version,
            max_version,
            capabilities: 0,
        })
    }

    fn commit(precondition: Option<WirePrecondition>, write: Option<WireWrite>) -> WireCommit {
        WireCommit {
            identity: IDENTITY.to_owned(),
            bucket: "archive".to_owned(),
            key: "k".to_owned(),
            precondition,
            write,
        }
    }

    fn put(data: Option<WirePutData>) -> WirePut {
        WirePut {
            size: 3,
            etag: "abc".to_owned(),
            data,
            ..WirePut::default()
        }
    }

    fn with_put(put: WirePut) -> WireMessage {
        WireMessage::Commit(commit(
            Some(WirePrecondition::Absent(true)),
            Some(WireWrite::Put(put)),
        ))
    }

    #[test]
    fn lengths_and_headers_are_checked_before_the_payload_arrives() {
        let mut prefix = (MAX_HEADER_LEN + 1).to_be_bytes().to_vec();
        prefix.extend_from_slice(&0u32.to_be_bytes());
        assert!(matches!(
            Message::decode(&prefix),
            Err(MessageError::HeaderTooLong(_))
        ));
        let mut prefix = 0u32.to_be_bytes().to_vec();
        prefix.extend_from_slice(&(MAX_PAYLOAD_LEN + 1).to_be_bytes());
        assert!(matches!(
            Message::decode(&prefix),
            Err(MessageError::PayloadTooLong(_))
        ));
        // An empty envelope is refused before its declared payload arrives.
        let mut empty = frame(None, b"");
        empty[4..8].copy_from_slice(&100u32.to_be_bytes());
        assert_eq!(Message::decode(&empty), Err(MessageError::UnknownMessage));
        // A message with a truncated length-delimited value.
        let mut bytes = 2u32.to_be_bytes().to_vec();
        bytes.extend_from_slice(&0u32.to_be_bytes());
        bytes.extend_from_slice(&[0x0a, 0x05]);
        let error = Message::decode(&bytes).unwrap_err();
        assert!(matches!(error, MessageError::Malformed(_)), "{error}");
        assert!(error.to_string().starts_with("malformed message header"));
        // A message of a later version is skipped by protobuf, and refused.
        assert_eq!(
            Message::decode_parts(&[0x50, 0x01], Bytes::new()),
            Err(MessageError::UnknownMessage)
        );
        assert_eq!(
            MessageError::UnknownMessage.to_string(),
            "the header holds no known message"
        );
    }

    #[test]
    fn hellos_need_a_valid_cluster_and_version_range() {
        for (min, max) in [(0, 1), (2, 1), (1, 65_536)] {
            invalid(hello(min, max), b"", "hello.versions");
        }
        let mut bad_cluster = hello(1, 1);
        if let WireMessage::Hello(hello) = &mut bad_cluster {
            hello.cluster = "Prod".to_owned();
        }
        invalid(bad_cluster, b"", "hello.cluster");
        assert!(matches!(valid(hello(1, 65_535), b""), Message::Hello(_)));
        // Only DATA and BATCH carry a payload.
        invalid(hello(1, 1), b"x", "payload");
    }

    #[test]
    fn identities_buckets_and_codes_are_parsed() {
        let begin = |identity: &str, bucket: &str| {
            WireMessage::Begin(WireBegin {
                identity: identity.to_owned(),
                bucket: bucket.to_owned(),
                key: "k".to_owned(),
            })
        };
        invalid(begin("prod-us/b/5/01.1", "archive"), b"", "begin.identity");
        invalid(begin(IDENTITY, "Archive"), b"", "begin.bucket");
        let abort = |reason: u32| {
            WireMessage::Abort(WireAbort {
                identity: IDENTITY.to_owned(),
                reason,
                detail: String::new(),
            })
        };
        invalid(abort(0), b"", "abort.reason");
        invalid(abort(5), b"", "abort.reason");
        let mut bad_identity = abort(1);
        if let WireMessage::Abort(abort) = &mut bad_identity {
            abort.identity.clear();
        }
        invalid(bad_identity, b"", "abort.identity");
    }

    #[test]
    fn payload_checksums_are_verified() {
        let data = WireMessage::Data(WireData {
            piece: 1,
            offset: 0,
            crc32c: crc32c::crc32c(b"abc"),
        });
        assert!(matches!(valid(data.clone(), b"abc"), Message::Data(_)));
        let error = Message::decode(&frame(Some(data), b"abd")).unwrap_err();
        assert!(matches!(error, MessageError::ChecksumMismatch { .. }));
        assert!(error.to_string().contains("does not match"), "{error}");

        let batch = |crc32c| {
            WireMessage::Batch(WireBatch {
                items: vec![commit(
                    Some(WirePrecondition::Unconditional(true)),
                    Some(WireWrite::Put(put(Some(WirePutData::InlineLen(3))))),
                )],
                crc32c,
            })
        };
        assert!(matches!(
            valid(batch(crc32c::crc32c(b"abc")), b"abc"),
            Message::Batch(_)
        ));
        assert!(matches!(
            Message::decode(&frame(Some(batch(0)), b"abc")),
            Err(MessageError::ChecksumMismatch { .. })
        ));
        // Inline lengths must account for the payload exactly.
        invalid(batch(crc32c::crc32c(b"ab")), b"ab", "put.data");
        invalid(batch(crc32c::crc32c(b"abcd")), b"abcd", "payload");
    }

    #[test]
    fn ranges_must_be_canonical_pairs() {
        let ranges = |pieces: Vec<WirePiece>| {
            WireMessage::Durable(WireRanges {
                identity: IDENTITY.to_owned(),
                pieces,
            })
        };
        let piece = |piece, bounds: &[u64]| WirePiece {
            piece,
            bounds: bounds.to_vec(),
        };
        invalid(ranges(vec![piece(1, &[0, 5, 9])]), b"", "ranges.pieces");
        invalid(ranges(vec![piece(1, &[0, 5, 5, 9])]), b"", "ranges.pieces");
        invalid(ranges(vec![piece(1, &[5, 5])]), b"", "ranges.pieces");
        invalid(
            ranges(vec![piece(1, &[0, 5]), piece(1, &[7, 9])]),
            b"",
            "ranges.pieces",
        );
        let mut bad_identity = ranges(vec![]);
        if let WireMessage::Durable(ranges) = &mut bad_identity {
            ranges.identity.clear();
        }
        invalid(bad_identity, b"", "ranges.identity");
    }

    #[test]
    fn commits_need_a_precondition_and_a_write() {
        invalid(
            WireMessage::Commit(commit(None, Some(WireWrite::Delete(true)))),
            b"",
            "commit.precondition",
        );
        invalid(
            WireMessage::Commit(commit(Some(WirePrecondition::Absent(true)), None)),
            b"",
            "commit.write",
        );
        invalid(
            WireMessage::Commit(commit(
                Some(WirePrecondition::Matches("x".to_owned())),
                Some(WireWrite::Delete(true)),
            )),
            b"",
            "commit.precondition",
        );
        let mut bad_bucket = commit(
            Some(WirePrecondition::Absent(true)),
            Some(WireWrite::Delete(true)),
        );
        bad_bucket.bucket = "a".to_owned();
        invalid(
            WireMessage::Commit(bad_bucket.clone()),
            b"",
            "commit.bucket",
        );
        bad_bucket.bucket = "archive".to_owned();
        bad_bucket.identity.clear();
        invalid(WireMessage::Commit(bad_bucket), b"", "commit.identity");
        invalid(with_put(put(None)), b"", "put.data");
        // A standalone COMMIT has no inline bytes to take.
        invalid(
            with_put(put(Some(WirePutData::InlineLen(3)))),
            b"",
            "put.data",
        );
        let mut bad_etag = put(Some(WirePutData::StagedPiece(1)));
        bad_etag.etag = String::new();
        invalid(with_put(bad_etag), b"", "put.etag");
    }

    #[test]
    fn put_fields_are_checked() {
        let part = |number, md5: Vec<u8>| WirePart {
            number,
            piece: 1,
            size: 3,
            md5,
        };
        let multipart = |parts| with_put(put(Some(WirePutData::Multipart(WireParts { parts }))));
        invalid(multipart(vec![part(65_536, vec![0; 16])]), b"", "put.parts");
        invalid(multipart(vec![part(1, vec![0; 15])]), b"", "put.parts");
        assert!(matches!(
            valid(multipart(vec![part(1, vec![0; 16])]), b""),
            Message::Commit(_)
        ));

        let entry = |name: &str| WireEntry {
            name: name.to_owned(),
            value: "v".to_owned(),
        };
        let mut twice = put(Some(WirePutData::StagedPiece(1)));
        twice.metadata = vec![entry("a"), entry("a")];
        invalid(with_put(twice.clone()), b"", "put.metadata");
        twice.metadata.clear();
        twice.tags = vec![entry("t"), entry("t")];
        invalid(with_put(twice), b"", "put.tags");

        let checksum = |algorithm, len, parts| WireChecksum {
            algorithm,
            digest: vec![0; len],
            parts,
        };
        let with_checksums = |checksums| {
            let mut put = put(Some(WirePutData::StagedPiece(1)));
            put.checksums = checksums;
            with_put(put)
        };
        for checksums in [
            vec![checksum(0, 4, 0)],
            vec![checksum(257, 4, 0)],
            vec![checksum(1, 3, 0)],
            vec![checksum(3, 8, 2)],
            vec![checksum(1, 4, 0), checksum(1, 4, 0)],
        ] {
            invalid(with_checksums(checksums), b"", "put.checksums");
        }
        assert!(matches!(
            valid(with_checksums(vec![checksum(5, 32, 4)]), b""),
            Message::Commit(_)
        ));
    }

    #[test]
    fn applied_needs_a_known_outcome() {
        let applied = |outcome| {
            WireMessage::Applied(WireApplied {
                identity: IDENTITY.to_owned(),
                outcome,
            })
        };
        invalid(applied(None), b"", "applied.outcome");
        invalid(
            applied(Some(WireOutcome::Failed(WireFailed {
                error: 9,
                reason: String::new(),
            }))),
            b"",
            "applied.error",
        );
        invalid(
            applied(Some(WireOutcome::Committed(WireCommitted {
                etag: Some("\"".to_owned()),
            }))),
            b"",
            "applied.etag",
        );
        invalid(
            applied(Some(WireOutcome::PreconditionFailed(
                WirePreconditionFailed {
                    current: Some("nope".to_owned()),
                },
            ))),
            b"",
            "applied.current",
        );
        let mut bad_identity = applied(Some(WireOutcome::Committed(WireCommitted::default())));
        if let WireMessage::Applied(applied) = &mut bad_identity {
            applied.identity.clear();
        }
        invalid(bad_identity, b"", "applied.identity");
    }
}
