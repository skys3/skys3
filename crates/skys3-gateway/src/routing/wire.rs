//! The forwarding messages between a gateway and a shard replica (§5.1,
//! step 1): `prost` messages in a frame's header, with a log record or an
//! answer as the payload.
//!
//! ```text
//! gateway                                         replica
//!   Forward(shard, epoch, operation) + record?   ->
//!                                                <- ForwardReply(outcome, epoch, config?) + Answer?
//! ```
//!
//! A request carries the shard epoch the gateway knows. A write's record
//! (`PUT`, `DELETE`, `TAGS`, a multipart record, or an `EXTENT`) travels
//! in the log record format, at the last position `(2⁶⁴−1, 2⁶⁴−1)`, after
//! every position it may name, and the replica sequences it at the next
//! position of its own log. The reply's header says
//! whether the replica served the request, and its payload holds the
//! answer: [`Answer`] for a served request. A read registration carries
//! the plan's layout as its payload instead, a [`Layout`]. Both ends decode frames from
//! authenticated but untrusted peers, so every field is checked before it
//! is used.

use std::time::Duration;

use bytes::Bytes;
use prost::{Message, Oneof};
use skys3_index::codec;
use skys3_index::{Entry, EntryState, ListItem, ListPage, ListQuery, Upload};
use skys3_log::record::{ExtentRef, MAX_EXTENTS, MAX_KEY_LEN};
use skys3_log::{LogRecord, RecordBody};
use skys3_net::{Frame, Header, MessageKind};
use skys3_shard::{FlushState, ReadPlan, Registered, StreamedBody};
use skys3_types::{
    BucketId, ClusterId, Epoch, EpochSeq, NodeId, RegisterDocument, Seq, ShardConfig, ShardId,
};

use super::{Reply, Request, Response};
use crate::conditions::{ConditionFailed, PeerCondition, Precondition};
use crate::shard::{ShardError, ShardRef, ShardSummary};

/// The longest refusal reason a reply carries, in bytes.
const MAX_REASON_LEN: usize = 1024;

/// The longest a forwarded flush wait may ask its replica to wait. A
/// gateway asks for less than its request timeout, and asks again.
pub const MAX_FLUSH_WAIT: Duration = Duration::from_secs(60);

/// Gateway to replica: one call of the gateway's shard interface.
#[derive(Clone, PartialEq, Message)]
pub struct Forward {
    /// The shard's bucket ID.
    #[prost(string, tag = "1")]
    pub bucket_id: String,
    /// The shard's number.
    #[prost(uint32, tag = "2")]
    pub shard: u32,
    /// The epoch of the configuration the gateway knows.
    #[prost(uint64, tag = "3")]
    pub epoch: u64,
    /// The call.
    #[prost(
        oneof = "Op",
        tags = "4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20"
    )]
    pub op: Option<Op>,
}

/// The call a [`Forward`] makes.
#[derive(Clone, PartialEq, Oneof)]
pub enum Op {
    /// The entry of this key.
    #[prost(string, tag = "4")]
    Entry(String),
    /// A page of the shard's listing.
    #[prost(message, tag = "5")]
    List(Query),
    /// An open upload and a page of its parts.
    #[prost(message, tag = "6")]
    Upload(UploadQuery),
    /// A page of the shard's open uploads.
    #[prost(message, tag = "7")]
    Uploads(UploadsQuery),
    /// A page of an upload's parts.
    #[prost(message, tag = "8")]
    Parts(PartsQuery),
    /// The payload of the record at this position.
    #[prost(message, tag = "9")]
    Payload(Position),
    /// Commit the `EXTENT` record in the frame's payload.
    #[prost(bool, tag = "10")]
    AppendExtent(bool),
    /// Commit the record in the frame's payload if the condition holds.
    #[prost(message, tag = "11")]
    Write(Condition),
    /// Seal the shard.
    #[prost(bool, tag = "12")]
    Seal(bool),
    /// Lift one seal.
    #[prost(bool, tag = "13")]
    Unseal(bool),
    /// The read plan of this key.
    #[prost(string, tag = "14")]
    Plan(String),
    /// Register a read of a version, whose plan's layout is the frame's
    /// payload, a [`Layout`].
    #[prost(message, tag = "15")]
    Register(RegisterQuery),
    /// Renew this registration.
    #[prost(uint64, tag = "16")]
    Renew(u64),
    /// Release this registration.
    #[prost(uint64, tag = "17")]
    Release(u64),
    /// The payload at a position for a registered read.
    #[prost(message, tag = "18")]
    Fetch(FetchQuery),
    /// Announce more of a streamed single PUT's body.
    #[prost(message, tag = "19")]
    Announce(Announcement),
    /// Wait for a version of a key to reach the remote target.
    #[prost(message, tag = "20")]
    Flushed(FlushQuery),
}

/// A write-through write's wait for its version's flush (design §7.5).
#[derive(Clone, PartialEq, Message)]
pub struct FlushQuery {
    /// The key.
    #[prost(string, tag = "1")]
    pub key: String,
    /// The version's position.
    #[prost(message, optional, tag = "2")]
    pub version: Option<Position>,
    /// How long the replica may wait, in milliseconds: at most
    /// [`MAX_FLUSH_WAIT`].
    #[prost(uint64, tag = "3")]
    pub wait_ms: u64,
}

/// More of a streamed single PUT's body (design §7.3).
#[derive(Clone, PartialEq, Message)]
pub struct Announcement {
    /// The key.
    #[prost(string, tag = "1")]
    pub key: String,
    /// The position of the body's `UPLOAD_BEGIN`.
    #[prost(message, optional, tag = "2")]
    pub upload: Option<Position>,
    /// The metadata and tags the `PUT` will store, as the index encodes
    /// an open upload's, whose other fields are unused.
    #[prost(bytes = "vec", tag = "3")]
    pub headers: Vec<u8>,
    /// Applied extents of the body.
    #[prost(message, repeated, tag = "4")]
    pub extents: Vec<BodyExtent>,
}

/// An extent of a streamed body.
#[derive(Clone, PartialEq, Message)]
pub struct BodyExtent {
    /// Its offset in the body.
    #[prost(uint64, tag = "1")]
    pub offset: u64,
    /// The `EXTENT` record.
    #[prost(message, optional, tag = "2")]
    pub extent: Option<ExtentAnswer>,
}

/// A read registration of a key's version.
#[derive(Clone, PartialEq, Message)]
pub struct RegisterQuery {
    /// The key.
    #[prost(string, tag = "1")]
    pub key: String,
    /// The version's position.
    #[prost(message, optional, tag = "2")]
    pub version: Option<Position>,
}

/// A fetch under a registered read.
#[derive(Clone, PartialEq, Message)]
pub struct FetchQuery {
    /// The registration.
    #[prost(uint64, tag = "1")]
    pub read: u64,
    /// The payload's position.
    #[prost(message, optional, tag = "2")]
    pub position: Option<Position>,
}

/// A version's bytes as log positions, in body order.
#[derive(Clone, PartialEq, Message)]
pub struct Layout {
    /// The extents.
    #[prost(message, repeated, tag = "1")]
    pub extents: Vec<ExtentAnswer>,
}

/// A log position.
#[derive(Clone, Copy, PartialEq, Eq, Message)]
pub struct Position {
    /// The epoch.
    #[prost(uint64, tag = "1")]
    pub epoch: u64,
    /// The sequence number.
    #[prost(uint64, tag = "2")]
    pub seq: u64,
}

/// A listing query.
#[derive(Clone, PartialEq, Message)]
pub struct Query {
    /// The prefix.
    #[prost(string, tag = "1")]
    pub prefix: String,
    /// The delimiter, if any.
    #[prost(string, optional, tag = "2")]
    pub delimiter: Option<String>,
    /// The item to list after, if any.
    #[prost(string, optional, tag = "3")]
    pub start_after: Option<String>,
    /// The most items in the page.
    #[prost(uint64, tag = "4")]
    pub max_items: u64,
}

/// An upload of a key and a page of its parts.
#[derive(Clone, PartialEq, Message)]
pub struct UploadQuery {
    /// The key.
    #[prost(string, tag = "1")]
    pub key: String,
    /// The upload's position.
    #[prost(message, optional, tag = "2")]
    pub upload: Option<Position>,
    /// The part number to list after.
    #[prost(uint32, tag = "3")]
    pub after: u32,
    /// The most parts.
    #[prost(uint64, tag = "4")]
    pub limit: u64,
}

/// A page of open uploads.
#[derive(Clone, PartialEq, Message)]
pub struct UploadsQuery {
    /// The key prefix.
    #[prost(string, tag = "1")]
    pub prefix: String,
    /// The key to list after, if any.
    #[prost(string, optional, tag = "2")]
    pub after_key: Option<String>,
    /// The upload of `after_key` to list after; every upload of it if
    /// absent.
    #[prost(message, optional, tag = "3")]
    pub after_upload: Option<Position>,
    /// The most uploads.
    #[prost(uint64, tag = "4")]
    pub limit: u64,
}

/// A page of an upload's parts.
#[derive(Clone, PartialEq, Message)]
pub struct PartsQuery {
    /// The upload's position.
    #[prost(message, optional, tag = "1")]
    pub upload: Option<Position>,
    /// The part number to list after.
    #[prost(uint32, tag = "2")]
    pub after: u32,
    /// The most parts.
    #[prost(uint64, tag = "3")]
    pub limit: u64,
}

/// A write's precondition.
#[derive(Clone, PartialEq, Message)]
pub struct Condition {
    /// 0: none, 1: absent, 2: exists, 3: matches `etag`; for a peer
    /// cluster's write (design §7.8), 4: the key has no current identity,
    /// 5: its current identity is `expected`, 6: unconditional.
    #[prost(uint32, tag = "1")]
    pub kind: u32,
    /// The ETag of a `matches` condition.
    #[prost(string, tag = "2")]
    pub etag: String,
    /// A peer write's identity.
    #[prost(string, tag = "3")]
    pub identity: String,
    /// The identity a peer write expects, for kind 5.
    #[prost(string, tag = "4")]
    pub expected: String,
    /// The cluster of a peer write's destination: this cluster.
    #[prost(string, tag = "5")]
    pub cluster: String,
}

/// Replica to gateway: whether the replica served a [`Forward`].
#[derive(Clone, PartialEq, Message)]
pub struct ForwardReply {
    /// What happened: see the `OUTCOME_*` constants.
    #[prost(uint32, tag = "1")]
    pub outcome: u32,
    /// The epoch of the replica's configuration.
    #[prost(uint64, tag = "2")]
    pub epoch: u64,
    /// The replica's configuration, as its register's JSON: the redirect
    /// hint of a refusal, or a newer configuration than the gateway's.
    #[prost(bytes = "vec", tag = "3")]
    pub config: Vec<u8>,
    /// Why the replica refused, if it did.
    #[prost(string, tag = "4")]
    pub reason: String,
    /// The position a write that was not acknowledged took, if it got one.
    #[prost(message, optional, tag = "5")]
    pub position: Option<Position>,
}

/// The replica served the request; the payload holds the [`Answer`].
pub const OUTCOME_SERVED: u32 = 0;
/// The replica is not the primary: `config` is its configuration.
pub const OUTCOME_REDIRECT: u32 = 1;
/// The replica's epoch is older than the gateway's.
pub const OUTCOME_BEHIND: u32 = 2;
/// The shard is not open on the replica's node.
pub const OUTCOME_NOT_FOUND: u32 = 3;
/// The shard is sealed.
pub const OUTCOME_SEALED: u32 = 4;
/// The shard refused the record.
pub const OUTCOME_INVALID: u32 = 5;
/// The shard could not serve the request.
pub const OUTCOME_UNAVAILABLE: u32 = 6;
/// The write was not acknowledged in time, and may still commit at
/// `position`, if it has one.
pub const OUTCOME_NOT_ACKNOWLEDGED: u32 = 7;

/// The answer to a served request, in the reply's payload.
#[derive(Clone, PartialEq, Message)]
pub struct Answer {
    /// The answer, by the call it answers.
    #[prost(
        oneof = "Result",
        tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16"
    )]
    pub result: Option<Result>,
}

/// An [`Answer`], by the call it answers.
#[derive(Clone, PartialEq, Oneof)]
pub enum Result {
    /// The encoded entry, empty for none.
    #[prost(bytes = "vec", tag = "1")]
    Entry(Vec<u8>),
    /// A listing page.
    #[prost(message, tag = "2")]
    List(Page),
    /// An upload and a page of its parts.
    #[prost(message, tag = "3")]
    Upload(UploadAnswer),
    /// A page of open uploads.
    #[prost(message, tag = "4")]
    Uploads(UploadsAnswer),
    /// A page of parts.
    #[prost(message, tag = "5")]
    Parts(PartsAnswer),
    /// A record's payload.
    #[prost(bytes = "bytes", tag = "6")]
    Payload(Bytes),
    /// The committed extent.
    #[prost(message, tag = "7")]
    Extent(ExtentAnswer),
    /// The written record's position, or why its condition failed.
    #[prost(message, tag = "8")]
    Written(Written),
    /// What the sealed shard holds.
    #[prost(message, tag = "9")]
    Sealed(Summary),
    /// The seal was lifted.
    #[prost(bool, tag = "10")]
    Unsealed(bool),
    /// A read plan.
    #[prost(message, tag = "11")]
    Plan(PlanAnswer),
    /// A read registration, or that the holder does not hold the version.
    #[prost(message, tag = "12")]
    Registered(RegisteredAnswer),
    /// Whether the registration was renewed.
    #[prost(bool, tag = "13")]
    Renewed(bool),
    /// The registration was released.
    #[prost(bool, tag = "14")]
    Released(bool),
    /// The announcement was passed on.
    #[prost(bool, tag = "15")]
    Announced(bool),
    /// Where the version waited for is: see the `FLUSH_*` constants.
    #[prost(uint32, tag = "16")]
    Flushed(u32),
}

/// The remote holds the version waited for, or a later one.
pub const FLUSH_FLUSHED: u32 = 0;
/// The key is held in conflict.
pub const FLUSH_CONFLICT: u32 = 1;
/// Neither, within the wait.
pub const FLUSH_PENDING: u32 = 2;

/// A read plan.
#[derive(Clone, PartialEq, Message)]
pub struct PlanAnswer {
    /// The encoded entry, empty for none.
    #[prost(bytes = "vec", tag = "1")]
    pub entry: Vec<u8>,
    /// The version's layout.
    #[prost(message, optional, tag = "2")]
    pub layout: Option<Layout>,
    /// The holders, by node ID.
    #[prost(string, repeated, tag = "3")]
    pub holders: Vec<String>,
}

/// A read registration.
#[derive(Clone, PartialEq, Message)]
pub struct RegisteredAnswer {
    /// The registration, absent if the holder does not hold the version.
    #[prost(uint64, optional, tag = "1")]
    pub read: Option<u64>,
    /// Where the holder serves the version from.
    #[prost(message, optional, tag = "2")]
    pub layout: Option<Layout>,
}

/// A listing page.
#[derive(Clone, PartialEq, Message)]
pub struct Page {
    /// The items.
    #[prost(message, repeated, tag = "1")]
    pub items: Vec<Item>,
    /// Whether the shard has more.
    #[prost(bool, tag = "2")]
    pub truncated: bool,
}

/// A listing item: an object, carried as an encoded entry, or a common
/// prefix.
#[derive(Clone, PartialEq, Message)]
pub struct Item {
    /// The key or the common prefix.
    #[prost(string, tag = "1")]
    pub name: String,
    /// The object's latest version as an encoded entry; absent for a
    /// common prefix.
    #[prost(bytes = "vec", optional, tag = "2")]
    pub object: Option<Vec<u8>>,
}

/// An upload and a page of its parts.
#[derive(Clone, PartialEq, Message)]
pub struct UploadAnswer {
    /// The encoded upload; absent if it is not open.
    #[prost(bytes = "vec", optional, tag = "1")]
    pub upload: Option<Vec<u8>>,
    /// The parts.
    #[prost(message, repeated, tag = "2")]
    pub parts: Vec<PartRow>,
}

/// A page of parts.
#[derive(Clone, PartialEq, Message)]
pub struct PartsAnswer {
    /// The parts.
    #[prost(message, repeated, tag = "1")]
    pub parts: Vec<PartRow>,
}

/// One part.
#[derive(Clone, PartialEq, Message)]
pub struct PartRow {
    /// The part number.
    #[prost(uint32, tag = "1")]
    pub number: u32,
    /// The encoded part.
    #[prost(bytes = "vec", tag = "2")]
    pub part: Vec<u8>,
}

/// A page of open uploads.
#[derive(Clone, PartialEq, Message)]
pub struct UploadsAnswer {
    /// The uploads.
    #[prost(message, repeated, tag = "1")]
    pub uploads: Vec<UploadRow>,
}

/// One open upload.
#[derive(Clone, PartialEq, Message)]
pub struct UploadRow {
    /// The key.
    #[prost(string, tag = "1")]
    pub key: String,
    /// The upload's position.
    #[prost(message, optional, tag = "2")]
    pub position: Option<Position>,
    /// The encoded upload.
    #[prost(bytes = "vec", tag = "3")]
    pub upload: Vec<u8>,
}

/// A committed extent.
#[derive(Clone, PartialEq, Message)]
pub struct ExtentAnswer {
    /// The `EXTENT` record's position.
    #[prost(message, optional, tag = "1")]
    pub position: Option<Position>,
    /// The payload length.
    #[prost(uint32, tag = "2")]
    pub len: u32,
}

/// A write's outcome.
#[derive(Clone, PartialEq, Message)]
pub struct Written {
    /// The record's position, if it was written.
    #[prost(message, optional, tag = "1")]
    pub position: Option<Position>,
    /// Why not: 1 no such key, 2 precondition failed, 3 no such upload,
    /// 4 invalid part.
    #[prost(uint32, tag = "2")]
    pub failed: u32,
}

/// What a sealed shard holds.
#[derive(Clone, Copy, PartialEq, Eq, Message)]
pub struct Summary {
    /// Live objects.
    #[prost(uint64, tag = "1")]
    pub objects: u64,
    /// Unflushed entries.
    #[prost(uint64, tag = "2")]
    pub unflushed: u64,
}

/// Why a frame does not decode.
type Decoded<T> = std::result::Result<T, String>;

/// The frame of a forwarded `request` to `shard`, in the gateway's `epoch`.
///
/// # Errors
///
/// Why a write's record does not encode.
pub fn request_frame(shard: &ShardRef, epoch: Epoch, request: &Request) -> Decoded<Frame> {
    let record = |body: RecordBody| {
        let record = LogRecord {
            shard: shard.into(),
            position: EpochSeq::new(Epoch::MAX, Seq::MAX),
            body,
        };
        record.to_bytes().map_err(|error| error.to_string())
    };
    let mut payload = Bytes::new();
    let op = match request {
        Request::Entry { key } => Op::Entry(key.clone()),
        Request::List(query) => Op::List(Query {
            prefix: query.prefix.clone(),
            delimiter: query.delimiter.clone(),
            start_after: query.start_after.clone(),
            max_items: query.max_items as u64,
        }),
        Request::Upload {
            key,
            upload,
            after,
            limit,
        } => Op::Upload(UploadQuery {
            key: key.clone(),
            upload: Some(position(*upload)),
            after: u32::from(*after),
            limit: *limit as u64,
        }),
        Request::Uploads {
            prefix,
            after,
            limit,
        } => Op::Uploads(UploadsQuery {
            prefix: prefix.clone(),
            after_key: after.as_ref().map(|(key, _)| key.clone()),
            after_upload: after.as_ref().and_then(|(_, upload)| upload.map(position)),
            limit: *limit as u64,
        }),
        Request::Parts {
            upload,
            after,
            limit,
        } => Op::Parts(PartsQuery {
            upload: Some(position(*upload)),
            after: u32::from(*after),
            limit: *limit as u64,
        }),
        Request::Payload(at) => Op::Payload(position(*at)),
        Request::Plan { key } => Op::Plan(key.clone()),
        Request::Register {
            key,
            version,
            layout,
        } => {
            payload = Bytes::from(layout_of(layout).encode_to_vec());
            Op::Register(RegisterQuery {
                key: key.clone(),
                version: Some(position(*version)),
            })
        }
        Request::Renew(read) => Op::Renew(*read),
        Request::Release(read) => Op::Release(*read),
        Request::Fetch { read, position: at } => Op::Fetch(FetchQuery {
            read: *read,
            position: Some(position(*at)),
        }),
        Request::AppendExtent(extent) => {
            payload = record(RecordBody::Extent(extent.clone()))?;
            Op::AppendExtent(true)
        }
        Request::Announce(body) => Op::Announce(announcement(body)?),
        Request::Flushed { key, version, wait } => Op::Flushed(FlushQuery {
            key: key.clone(),
            version: Some(position(*version)),
            wait_ms: u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
        }),
        Request::Write { body, condition } => {
            payload = record(body.clone())?;
            Op::Write(match condition {
                Precondition::None => condition_of(0, ""),
                Precondition::Absent => condition_of(1, ""),
                Precondition::Exists => condition_of(2, ""),
                Precondition::Matches(etag) => condition_of(3, etag),
                Precondition::Peer(peer) => peer_condition(peer),
            })
        }
        Request::Seal => Op::Seal(true),
        Request::Unseal => Op::Unseal(true),
    };
    let forward = Forward {
        bucket_id: shard.bucket.as_str().to_owned(),
        shard: u32::from(shard.shard.get()),
        epoch: epoch.get(),
        op: Some(op),
    };
    Ok(Frame::new(
        Header::new(MessageKind::Forward).with_body(forward.encode_to_vec()),
        payload,
    ))
}

fn condition_of(kind: u32, etag: &str) -> Condition {
    Condition {
        kind,
        etag: etag.to_owned(),
        ..Condition::default()
    }
}

fn peer_condition(peer: &PeerCondition) -> Condition {
    let (kind, expected) = match &peer.expected {
        skys3_peer::Precondition::Absent => (4, String::new()),
        skys3_peer::Precondition::Matches(expected) => (5, expected.to_string()),
        skys3_peer::Precondition::Unconditional => (6, String::new()),
    };
    Condition {
        kind,
        etag: String::new(),
        identity: peer.identity.to_string(),
        expected,
        cluster: peer.cluster.to_string(),
    }
}

/// The precondition of a write to `shard`.
fn decode_condition(condition: Condition, shard: &ShardRef) -> Decoded<Precondition> {
    let Condition {
        kind,
        etag,
        identity,
        expected,
        cluster,
    } = condition;
    let plain = identity.is_empty() && expected.is_empty() && cluster.is_empty();
    let peer = |expected| -> Decoded<Precondition> {
        Ok(Precondition::Peer(PeerCondition {
            identity: identity.parse().map_err(|error| format!("{error}"))?,
            expected,
            cluster: ClusterId::new(cluster).map_err(|error| error.to_string())?,
            shard: shard.clone(),
        }))
    };
    match (kind, etag.is_empty(), plain) {
        (0, true, true) => Ok(Precondition::None),
        (1, true, true) => Ok(Precondition::Absent),
        (2, true, true) => Ok(Precondition::Exists),
        (3, _, true) => Ok(Precondition::Matches(etag)),
        (4, true, _) if expected.is_empty() => peer(skys3_peer::Precondition::Absent),
        (5, true, _) => peer(skys3_peer::Precondition::Matches(
            expected.parse().map_err(|error| format!("{error}"))?,
        )),
        (6, true, _) if expected.is_empty() => peer(skys3_peer::Precondition::Unconditional),
        _ => Err(format!("condition {kind}")),
    }
}

/// Decodes a forwarded request: its shard, the gateway's epoch, and the
/// call.
///
/// # Errors
///
/// Why the frame is not a valid request.
pub fn decode_request(frame: &Frame) -> Decoded<(ShardRef, Epoch, Request)> {
    let forward: Forward = body(frame, MessageKind::Forward)?;
    let shard = ShardRef {
        bucket: BucketId::new(forward.bucket_id).map_err(|error| error.to_string())?,
        shard: ShardId::new(
            u8::try_from(forward.shard).map_err(|_| format!("shard {}", forward.shard))?,
        ),
    };
    let record = || -> Decoded<RecordBody> {
        let (record, len) = LogRecord::decode(&frame.payload).map_err(|error| error.to_string())?;
        if len != frame.payload.len() || record.shard != (&shard).into() {
            return Err("the record is not the request's alone".to_owned());
        }
        Ok(record.body)
    };
    let request = match forward.op.ok_or("the request names no call")? {
        Op::Entry(key) => Request::Entry { key },
        Op::List(query) => Request::List(ListQuery {
            prefix: query.prefix,
            delimiter: query.delimiter,
            start_after: query.start_after,
            max_items: size(query.max_items),
        }),
        Op::Upload(query) => Request::Upload {
            key: query.key,
            upload: epoch_seq(query.upload)?,
            after: part_number(query.after)?,
            limit: size(query.limit),
        },
        Op::Uploads(query) => Request::Uploads {
            prefix: query.prefix,
            after: match (query.after_key, query.after_upload) {
                (Some(key), upload) => {
                    Some((key, upload.map(|at| epoch_seq(Some(at))).transpose()?))
                }
                (None, None) => None,
                (None, Some(_)) => return Err("an upload to list after without its key".into()),
            },
            limit: size(query.limit),
        },
        Op::Parts(query) => Request::Parts {
            upload: epoch_seq(query.upload)?,
            after: part_number(query.after)?,
            limit: size(query.limit),
        },
        Op::Payload(at) => Request::Payload(epoch_seq(Some(at))?),
        Op::AppendExtent(_) => match record()? {
            RecordBody::Extent(extent) => Request::AppendExtent(extent),
            other => return Err(format!("an extent request carries a {:?}", other.kind())),
        },
        Op::Write(condition) => Request::Write {
            body: match record()? {
                body @ (RecordBody::Put(_)
                | RecordBody::Delete(_)
                | RecordBody::Tags(_)
                | RecordBody::Flushed(_)
                | RecordBody::MpuCreate(_)
                | RecordBody::MpuPart(_)
                | RecordBody::MpuComplete(_)
                | RecordBody::MpuAbort(_)
                | RecordBody::UploadBegin(_)
                // What a read records of a remote: lazily loaded metadata
                // (§9.1), and the versions of an origin (§9.5).
                | RecordBody::Import(_)
                | RecordBody::Adopt(_)) => body,
                other => return Err(format!("a write request carries a {:?}", other.kind())),
            },
            condition: decode_condition(condition, &shard)?,
        },
        Op::Seal(_) => Request::Seal,
        Op::Unseal(_) => Request::Unseal,
        Op::Plan(key) => Request::Plan { key },
        Op::Register(query) => Request::Register {
            key: query.key,
            version: epoch_seq(query.version)?,
            layout: decode_layout(
                Layout::decode(frame.payload.clone()).map_err(|error| error.to_string())?,
            )?,
        },
        Op::Renew(read) => Request::Renew(read),
        Op::Release(read) => Request::Release(read),
        Op::Fetch(query) => Request::Fetch {
            read: query.read,
            position: epoch_seq(query.position)?,
        },
        Op::Announce(announced) => Request::Announce(decode_announcement(announced)?),
        Op::Flushed(query) => {
            if query.key.is_empty() || query.key.len() > MAX_KEY_LEN {
                return Err(format!("a key of {} bytes", query.key.len()));
            }
            let wait = Duration::from_millis(query.wait_ms);
            if wait > MAX_FLUSH_WAIT {
                return Err(format!("a flush wait of {} ms", query.wait_ms));
            }
            Request::Flushed {
                key: query.key,
                version: epoch_seq(query.version)?,
                wait,
            }
        }
    };
    if !matches!(
        request,
        Request::AppendExtent(_) | Request::Write { .. } | Request::Register { .. }
    ) && !frame.payload.is_empty()
    {
        return Err("a request without a record carries a payload".to_owned());
    }
    Ok((shard, Epoch::new(forward.epoch), request))
}

/// The frame of `reply`, answering the request numbered `request_id`.
#[must_use]
pub fn reply_frame(reply: &Reply, request_id: u64) -> Frame {
    let config = |config: &ShardConfig| config.to_json().unwrap_or_default();
    let refused = |outcome, epoch: Epoch, reason: &str| ForwardReply {
        outcome,
        epoch: epoch.get(),
        config: Vec::new(),
        reason: truncate(reason),
        position: None,
    };
    let (header, payload) = match reply {
        Reply::Served {
            response,
            epoch,
            hint,
        } => match answer(response) {
            Ok(answer) => (
                ForwardReply {
                    outcome: OUTCOME_SERVED,
                    epoch: epoch.get(),
                    config: hint.as_ref().map(config).unwrap_or_default(),
                    reason: String::new(),
                    position: None,
                },
                Bytes::from(answer.encode_to_vec()),
            ),
            Err(error) => {
                let reason = format!("the answer does not encode: {error}");
                (refused(OUTCOME_UNAVAILABLE, *epoch, &reason), Bytes::new())
            }
        },
        Reply::Redirect(hint) => (
            ForwardReply {
                outcome: OUTCOME_REDIRECT,
                epoch: hint.epoch.get(),
                config: config(hint),
                reason: String::new(),
                position: None,
            },
            Bytes::new(),
        ),
        Reply::Behind(epoch) => (refused(OUTCOME_BEHIND, *epoch, ""), Bytes::new()),
        Reply::Refused(error) => {
            let outcome = match error {
                ShardError::NotFound(_) => OUTCOME_NOT_FOUND,
                ShardError::Sealed(_) => OUTCOME_SEALED,
                ShardError::Invalid { .. } => OUTCOME_INVALID,
                ShardError::NotAcknowledged { .. } => OUTCOME_NOT_ACKNOWLEDGED,
                _ => OUTCOME_UNAVAILABLE,
            };
            let reason = match error {
                ShardError::Invalid { reason, .. }
                | ShardError::Unavailable { reason, .. }
                | ShardError::NotAcknowledged { reason, .. } => reason.clone(),
                other => other.to_string(),
            };
            let mut refusal = refused(outcome, Epoch::ZERO, &reason);
            if let ShardError::NotAcknowledged {
                position: Some(at), ..
            } = error
            {
                refusal.position = Some(position(*at));
            }
            (refusal, Bytes::new())
        }
    };
    Frame::new(
        Header::new(MessageKind::ForwardReply)
            .with_request_id(request_id)
            .with_body(header.encode_to_vec()),
        payload,
    )
}

fn truncate(reason: &str) -> String {
    let mut end = reason.len().min(MAX_REASON_LEN);
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    reason[..end].to_owned()
}

/// The answer to a served request, or why one of its values does not
/// encode. Every value came from the index, which holds only values that
/// encode, but an empty value must never stand in for one that failed.
fn answer(response: &Response) -> std::result::Result<Answer, codec::CodecError> {
    let failed = std::cell::RefCell::new(None);
    let encoded = |value: std::result::Result<Vec<u8>, codec::CodecError>| {
        value.unwrap_or_else(|error| {
            failed.borrow_mut().get_or_insert(error);
            Vec::new()
        })
    };
    let parts = |parts: &[(u16, skys3_index::Part)]| {
        parts
            .iter()
            .map(|(number, part)| PartRow {
                number: u32::from(*number),
                part: encoded(codec::encode_part(part)),
            })
            .collect()
    };
    let result = match response {
        Response::Entry(entry) => Result::Entry(
            entry
                .as_ref()
                .map(|entry| encoded(codec::encode_entry(entry)))
                .unwrap_or_default(),
        ),
        Response::List(page) => Result::List(Page {
            items: page
                .items
                .iter()
                .map(|item| match item {
                    ListItem::Object { key, object } => Item {
                        name: key.clone(),
                        object: Some(encoded(codec::encode_entry(&Entry {
                            version: EpochSeq::new(Epoch::ZERO, Seq::ZERO),
                            state: EntryState::Dirty,
                            object: Some((**object).clone()),
                            remote_etag: None,
                            remote_version_id: None,
                        }))),
                    },
                    ListItem::Prefix(prefix) => Item {
                        name: prefix.clone(),
                        object: None,
                    },
                })
                .collect(),
            truncated: page.truncated,
        }),
        Response::Upload(found) => Result::Upload(match found {
            Some((upload, page)) => UploadAnswer {
                upload: Some(encoded(codec::encode_upload(upload))),
                parts: parts(page),
            },
            None => UploadAnswer::default(),
        }),
        Response::Uploads(uploads) => Result::Uploads(UploadsAnswer {
            uploads: uploads
                .iter()
                .map(|(key, at, upload)| UploadRow {
                    key: key.clone(),
                    position: Some(position(*at)),
                    upload: encoded(codec::encode_upload(upload)),
                })
                .collect(),
        }),
        Response::Parts(page) => Result::Parts(PartsAnswer { parts: parts(page) }),
        Response::Payload(bytes) => Result::Payload(bytes.clone()),
        Response::Extent(extent) => Result::Extent(ExtentAnswer {
            position: Some(position(extent.position)),
            len: extent.len,
        }),
        Response::Written(written) => Result::Written(match written {
            Ok(at) => Written {
                position: Some(position(*at)),
                failed: 0,
            },
            Err(failed) => Written {
                position: None,
                failed: match failed {
                    ConditionFailed::NoSuchKey => 1,
                    ConditionFailed::PreconditionFailed => 2,
                    ConditionFailed::NoSuchUpload => 3,
                    ConditionFailed::InvalidPart => 4,
                },
            },
        }),
        Response::Sealed(summary) => Result::Sealed(Summary {
            objects: summary.objects,
            unflushed: summary.unflushed,
        }),
        Response::Unsealed => Result::Unsealed(true),
        Response::Plan(plan) => Result::Plan(PlanAnswer {
            entry: plan
                .entry
                .as_ref()
                .map(|entry| encoded(codec::encode_entry(entry)))
                .unwrap_or_default(),
            layout: Some(layout_of(&plan.layout)),
            holders: plan.holders.iter().map(ToString::to_string).collect(),
        }),
        Response::Registered(registered) => Result::Registered(match registered {
            Some(registered) => RegisteredAnswer {
                read: Some(registered.id),
                layout: Some(layout_of(&registered.layout)),
            },
            None => RegisteredAnswer::default(),
        }),
        Response::Renewed(renewed) => Result::Renewed(*renewed),
        Response::Released => Result::Released(true),
        Response::Announced => Result::Announced(true),
        Response::Flushed(state) => Result::Flushed(match state {
            FlushState::Flushed => FLUSH_FLUSHED,
            FlushState::Conflict => FLUSH_CONFLICT,
            // A state added later is reported as not flushed yet.
            _ => FLUSH_PENDING,
        }),
    };
    match failed.into_inner() {
        Some(error) => Err(error),
        None => Ok(Answer {
            result: Some(result),
        }),
    }
}

/// Decodes the reply to a request to `shard`.
///
/// # Errors
///
/// Why the frame is not a valid reply.
pub fn decode_reply(frame: &Frame, shard: &ShardRef) -> Decoded<Reply> {
    let reply: ForwardReply = body(frame, MessageKind::ForwardReply)?;
    let hint = || -> Decoded<ShardConfig> {
        let config = ShardConfig::from_json(&reply.config).map_err(|error| error.to_string())?;
        if config.bucket_id != shard.bucket || config.shard != shard.shard {
            return Err("the configuration is of another shard".to_owned());
        }
        Ok(config)
    };
    let reason = reply.reason.clone();
    if reply.outcome != OUTCOME_SERVED && !frame.payload.is_empty() {
        return Err("a refusal carries a payload".to_owned());
    }
    if reply.outcome != OUTCOME_NOT_ACKNOWLEDGED && reply.position.is_some() {
        return Err("only a write that was not acknowledged has a position".to_owned());
    }
    let shard = shard.clone();
    Ok(match reply.outcome {
        OUTCOME_SERVED => Reply::Served {
            response: decode_answer(&frame.payload)?,
            epoch: Epoch::new(reply.epoch),
            hint: if reply.config.is_empty() {
                None
            } else {
                Some(hint()?)
            },
        },
        OUTCOME_REDIRECT => Reply::Redirect(hint()?),
        OUTCOME_BEHIND => Reply::Behind(Epoch::new(reply.epoch)),
        OUTCOME_NOT_FOUND => Reply::Refused(ShardError::NotFound(shard)),
        OUTCOME_SEALED => Reply::Refused(ShardError::Sealed(shard)),
        OUTCOME_INVALID => Reply::Refused(ShardError::Invalid { shard, reason }),
        OUTCOME_UNAVAILABLE => Reply::Refused(ShardError::Unavailable { shard, reason }),
        OUTCOME_NOT_ACKNOWLEDGED => Reply::Refused(ShardError::NotAcknowledged {
            shard,
            position: reply.position.map(|at| epoch_seq(Some(at))).transpose()?,
            reason,
        }),
        other => return Err(format!("outcome {other}")),
    })
}

fn decode_answer(payload: &Bytes) -> Decoded<Response> {
    let answer = Answer::decode(payload.clone()).map_err(|error| error.to_string())?;
    let codec = |error: codec::CodecError| error.to_string();
    let parts = |rows: Vec<PartRow>| -> Decoded<Vec<(u16, skys3_index::Part)>> {
        rows.into_iter()
            .map(|row| {
                Ok((
                    part_number(row.number)?,
                    codec::decode_part(&row.part).map_err(codec)?,
                ))
            })
            .collect()
    };
    Ok(match answer.result.ok_or("the answer is empty")? {
        Result::Entry(bytes) if bytes.is_empty() => Response::Entry(None),
        Result::Entry(bytes) => Response::Entry(Some(codec::decode_entry(&bytes).map_err(codec)?)),
        Result::List(page) => Response::List(ListPage {
            items: page
                .items
                .into_iter()
                .map(|item| {
                    Ok(match item.object {
                        None => ListItem::Prefix(item.name),
                        Some(bytes) => ListItem::Object {
                            key: item.name,
                            object: Box::new(
                                codec::decode_entry(&bytes)
                                    .map_err(codec)?
                                    .object
                                    .ok_or("a listed object without its version")?,
                            ),
                        },
                    })
                })
                .collect::<Decoded<_>>()?,
            truncated: page.truncated,
        }),
        Result::Upload(found) => Response::Upload(match found.upload {
            Some(upload) => Some((
                codec::decode_upload(&upload).map_err(codec)?,
                parts(found.parts)?,
            )),
            None if found.parts.is_empty() => None,
            None => return Err("parts of an upload that is not open".to_owned()),
        }),
        Result::Uploads(page) => Response::Uploads(
            page.uploads
                .into_iter()
                .map(|row| {
                    Ok((
                        row.key,
                        epoch_seq(row.position)?,
                        codec::decode_upload(&row.upload).map_err(codec)?,
                    ))
                })
                .collect::<Decoded<_>>()?,
        ),
        Result::Parts(page) => Response::Parts(parts(page.parts)?),
        Result::Payload(bytes) => Response::Payload(bytes),
        Result::Extent(extent) => Response::Extent(ExtentRef {
            position: epoch_seq(extent.position)?,
            len: extent.len,
        }),
        Result::Written(written) => Response::Written(match (written.position, written.failed) {
            (Some(at), 0) => Ok(epoch_seq(Some(at))?),
            (None, 1) => Err(ConditionFailed::NoSuchKey),
            (None, 2) => Err(ConditionFailed::PreconditionFailed),
            (None, 3) => Err(ConditionFailed::NoSuchUpload),
            (None, 4) => Err(ConditionFailed::InvalidPart),
            (_, failed) => return Err(format!("write outcome {failed}")),
        }),
        Result::Sealed(summary) => Response::Sealed(ShardSummary {
            objects: summary.objects,
            unflushed: summary.unflushed,
        }),
        Result::Unsealed(_) => Response::Unsealed,
        Result::Plan(plan) => {
            let entry = if plan.entry.is_empty() {
                None
            } else {
                Some(codec::decode_entry(&plan.entry).map_err(codec)?)
            };
            let holders = plan
                .holders
                .iter()
                .map(|node| {
                    node.parse()
                        .map_err(|error| format!("holder {node}: {error}"))
                })
                .collect::<Decoded<Vec<NodeId>>>()?;
            if entry.is_none() && !holders.is_empty() {
                return Err("holders of a key with no entry".to_owned());
            }
            Response::Plan(ReadPlan {
                entry,
                layout: decode_layout(plan.layout.unwrap_or_default())?,
                holders,
            })
        }
        Result::Registered(registered) => Response::Registered(match registered.read {
            Some(id) => Some(Registered {
                id,
                layout: decode_layout(registered.layout.unwrap_or_default())?,
            }),
            None if registered.layout.is_none() => None,
            None => return Err("a layout without a registration".to_owned()),
        }),
        Result::Renewed(renewed) => Response::Renewed(renewed),
        Result::Released(_) => Response::Released,
        Result::Announced(_) => Response::Announced,
        Result::Flushed(state) => Response::Flushed(match state {
            FLUSH_FLUSHED => FlushState::Flushed,
            FLUSH_CONFLICT => FlushState::Conflict,
            FLUSH_PENDING => FlushState::Pending,
            other => return Err(format!("flush state {other}")),
        }),
    })
}

/// The wire form of a streamed body's announcement.
fn announcement(body: &StreamedBody) -> Decoded<Announcement> {
    let headers = Upload {
        initiated_ms: 0,
        metadata: body.metadata.clone(),
        tags: body.tags.clone(),
        checksum: None,
    };
    Ok(Announcement {
        key: body.key.clone(),
        upload: Some(position(body.upload)),
        headers: codec::encode_upload(&headers).map_err(|error| error.to_string())?,
        extents: body
            .extents
            .iter()
            .map(|(offset, extent)| BodyExtent {
                offset: *offset,
                extent: Some(ExtentAnswer {
                    position: Some(position(extent.position)),
                    len: extent.len,
                }),
            })
            .collect(),
    })
}

/// An announcement from its wire form: a key, an upload, metadata and tags
/// within their limits, and at most a `PUT`'s extents, each with bytes.
fn decode_announcement(announced: Announcement) -> Decoded<StreamedBody> {
    if announced.key.is_empty() || announced.key.len() > MAX_KEY_LEN {
        return Err(format!("a key of {} bytes", announced.key.len()));
    }
    if announced.extents.len() > MAX_EXTENTS {
        return Err(format!("{} extents", announced.extents.len()));
    }
    let headers = codec::decode_upload(&announced.headers).map_err(|error| error.to_string())?;
    let extents = announced
        .extents
        .into_iter()
        .map(|extent| {
            let offset = extent.offset;
            let layout = Layout {
                extents: extent.extent.into_iter().collect(),
            };
            match decode_layout(layout)?.as_slice() {
                [extent] => Ok((offset, *extent)),
                _ => Err("an announced extent is missing".to_owned()),
            }
        })
        .collect::<Decoded<_>>()?;
    Ok(StreamedBody {
        key: announced.key,
        upload: epoch_seq(announced.upload)?,
        metadata: headers.metadata,
        tags: headers.tags,
        extents,
    })
}

/// The wire form of a layout.
fn layout_of(layout: &[ExtentRef]) -> Layout {
    Layout {
        extents: layout
            .iter()
            .map(|extent| ExtentAnswer {
                position: Some(position(extent.position)),
                len: extent.len,
            })
            .collect(),
    }
}

/// A layout from its wire form: every extent has a position and bytes.
fn decode_layout(layout: Layout) -> Decoded<Vec<ExtentRef>> {
    layout
        .extents
        .into_iter()
        .map(|extent| {
            if extent.len == 0 {
                return Err("an empty extent in a layout".to_owned());
            }
            Ok(ExtentRef {
                position: epoch_seq(extent.position)?,
                len: extent.len,
            })
        })
        .collect()
}

/// Decodes the body of `frame`, which must be of `kind`.
fn body<M: Message + Default>(frame: &Frame, kind: MessageKind) -> Decoded<M> {
    if frame.header.kind != kind {
        return Err(format!(
            "expected a {kind:?} message, got {:?}",
            frame.header.kind
        ));
    }
    M::decode(frame.header.body.clone()).map_err(|error| error.to_string())
}

fn position(at: EpochSeq) -> Position {
    Position {
        epoch: at.epoch.get(),
        seq: at.seq.get(),
    }
}

fn epoch_seq(at: Option<Position>) -> Decoded<EpochSeq> {
    let at = at.ok_or("a position is missing")?;
    Ok(EpochSeq::new(Epoch::new(at.epoch), Seq::new(at.seq)))
}

fn part_number(number: u32) -> Decoded<u16> {
    u16::try_from(number).map_err(|_| format!("part number {number}"))
}

/// A requested count, which the index bounds by what it holds.
fn size(count: u64) -> usize {
    usize::try_from(count).unwrap_or(usize::MAX)
}
