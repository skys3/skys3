//! The etcd v3 messages the backend exchanges: a field-for-field subset of
//! etcd v3.6's `api/etcdserverpb/rpc.proto` and `api/mvccpb/kv.proto`.
//!
//! Only the messages and fields the backend sends or reads are declared.
//! Field numbers and types follow the upstream files exactly; Protobuf
//! decoding skips the fields left out. A test decodes responses captured
//! from etcd 3.6.10, so a wrong field number is caught.

use bytes::Bytes;

/// `etcdserverpb.KV/Range`.
pub(crate) const RANGE: &str = "/etcdserverpb.KV/Range";
/// `etcdserverpb.KV/Txn`.
pub(crate) const TXN: &str = "/etcdserverpb.KV/Txn";
/// `etcdserverpb.Watch/Watch`, a bidirectional stream.
pub(crate) const WATCH: &str = "/etcdserverpb.Watch/Watch";

/// `etcdserverpb.ResponseHeader`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ResponseHeader {
    /// The ID of the cluster that answered.
    #[prost(uint64, tag = "1")]
    pub cluster_id: u64,
    /// The ID of the member that answered.
    #[prost(uint64, tag = "2")]
    pub member_id: u64,
    /// The store's revision when the request was applied.
    #[prost(int64, tag = "3")]
    pub revision: i64,
    /// The member's Raft term.
    #[prost(uint64, tag = "4")]
    pub raft_term: u64,
}

/// `mvccpb.KeyValue`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct KeyValue {
    /// The key.
    #[prost(bytes = "bytes", tag = "1")]
    pub key: Bytes,
    /// The revision of the key's latest creation.
    #[prost(int64, tag = "2")]
    pub create_revision: i64,
    /// The revision of the key's latest modification: a register's
    /// version.
    #[prost(int64, tag = "3")]
    pub mod_revision: i64,
    /// The number of writes since the key's latest creation.
    #[prost(int64, tag = "4")]
    pub version: i64,
    /// The value.
    #[prost(bytes = "bytes", tag = "5")]
    pub value: Bytes,
    /// The lease attached to the key, or 0.
    #[prost(int64, tag = "6")]
    pub lease: i64,
}

/// `mvccpb.Event.EventType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum EventType {
    /// A put.
    Put = 0,
    /// A delete.
    Delete = 1,
}

/// `mvccpb.Event`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Event {
    /// What happened.
    #[prost(enumeration = "EventType", tag = "1")]
    pub r#type: i32,
    /// The key after the event; for a delete, only the key and the
    /// deletion's revision.
    #[prost(message, optional, tag = "2")]
    pub kv: Option<KeyValue>,
}

/// `etcdserverpb.RangeRequest`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct RangeRequest {
    /// The first key.
    #[prost(bytes = "bytes", tag = "1")]
    pub key: Bytes,
    /// The end of the range, exclusive; empty for the key alone.
    #[prost(bytes = "bytes", tag = "2")]
    pub range_end: Bytes,
    /// The most keys to return, or 0 for no limit.
    #[prost(int64, tag = "3")]
    pub limit: i64,
    /// The revision to read at, or 0 for the latest.
    #[prost(int64, tag = "4")]
    pub revision: i64,
    /// Whether a member may answer from its own state, which can be stale.
    /// The backend always reads linearizably.
    #[prost(bool, tag = "7")]
    pub serializable: bool,
    /// Whether to leave the values out.
    #[prost(bool, tag = "8")]
    pub keys_only: bool,
}

/// `etcdserverpb.RangeResponse`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct RangeResponse {
    /// The header.
    #[prost(message, optional, tag = "1")]
    pub header: Option<ResponseHeader>,
    /// The keys found, in key order.
    #[prost(message, repeated, tag = "2")]
    pub kvs: Vec<KeyValue>,
    /// Whether the range holds more keys than `limit` let through.
    #[prost(bool, tag = "3")]
    pub more: bool,
    /// The number of keys in the range.
    #[prost(int64, tag = "4")]
    pub count: i64,
}

/// `etcdserverpb.PutRequest`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PutRequest {
    /// The key.
    #[prost(bytes = "bytes", tag = "1")]
    pub key: Bytes,
    /// The value.
    #[prost(bytes = "bytes", tag = "2")]
    pub value: Bytes,
}

/// `etcdserverpb.DeleteRangeRequest`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct DeleteRangeRequest {
    /// The key.
    #[prost(bytes = "bytes", tag = "1")]
    pub key: Bytes,
}

/// `etcdserverpb.RequestOp`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct RequestOp {
    /// The operation.
    #[prost(oneof = "Request", tags = "2, 3")]
    pub request: Option<Request>,
}

/// `etcdserverpb.RequestOp.request`, the operations the backend uses.
#[derive(Clone, PartialEq, prost::Oneof)]
pub enum Request {
    /// `request_put`.
    #[prost(message, tag = "2")]
    Put(PutRequest),
    /// `request_delete_range`.
    #[prost(message, tag = "3")]
    DeleteRange(DeleteRangeRequest),
}

/// `etcdserverpb.Compare.CompareResult`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum CompareResult {
    /// The target equals the given value.
    Equal = 0,
}

/// `etcdserverpb.Compare.CompareTarget`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum CompareTarget {
    /// The key's `create_revision`.
    Create = 1,
    /// The key's `mod_revision`.
    Mod = 2,
}

/// `etcdserverpb.Compare`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Compare {
    /// The comparison.
    #[prost(enumeration = "CompareResult", tag = "1")]
    pub result: i32,
    /// What to compare.
    #[prost(enumeration = "CompareTarget", tag = "2")]
    pub target: i32,
    /// The key.
    #[prost(bytes = "bytes", tag = "3")]
    pub key: Bytes,
    /// The value to compare with.
    #[prost(oneof = "TargetUnion", tags = "5, 6")]
    pub target_union: Option<TargetUnion>,
}

/// `etcdserverpb.Compare.target_union`, the targets the backend uses.
#[derive(Clone, PartialEq, prost::Oneof)]
pub enum TargetUnion {
    /// `create_revision`: 0 for a key that does not exist.
    #[prost(int64, tag = "5")]
    CreateRevision(i64),
    /// `mod_revision`.
    #[prost(int64, tag = "6")]
    ModRevision(i64),
}

/// `etcdserverpb.TxnRequest`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct TxnRequest {
    /// Every comparison must hold for `success` to run.
    #[prost(message, repeated, tag = "1")]
    pub compare: Vec<Compare>,
    /// The operations if every comparison holds.
    #[prost(message, repeated, tag = "2")]
    pub success: Vec<RequestOp>,
    /// The operations otherwise.
    #[prost(message, repeated, tag = "3")]
    pub failure: Vec<RequestOp>,
}

/// `etcdserverpb.TxnResponse`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct TxnResponse {
    /// The header; its revision is the transaction's.
    #[prost(message, optional, tag = "1")]
    pub header: Option<ResponseHeader>,
    /// Whether every comparison held.
    #[prost(bool, tag = "2")]
    pub succeeded: bool,
}

/// `etcdserverpb.WatchRequest`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct WatchRequest {
    /// The request.
    #[prost(oneof = "WatchRequestUnion", tags = "1")]
    pub request_union: Option<WatchRequestUnion>,
}

/// `etcdserverpb.WatchRequest.request_union`, the requests the backend
/// sends. A watch ends when its stream does.
#[derive(Clone, PartialEq, prost::Oneof)]
pub enum WatchRequestUnion {
    /// `create_request`.
    #[prost(message, tag = "1")]
    Create(WatchCreateRequest),
}

/// `etcdserverpb.WatchCreateRequest`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct WatchCreateRequest {
    /// The key.
    #[prost(bytes = "bytes", tag = "1")]
    pub key: Bytes,
    /// The end of the range, exclusive; empty for the key alone.
    #[prost(bytes = "bytes", tag = "2")]
    pub range_end: Bytes,
    /// The first revision to report, or 0 for changes after the watch is
    /// created.
    #[prost(int64, tag = "3")]
    pub start_revision: i64,
}

/// `etcdserverpb.WatchResponse`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct WatchResponse {
    /// The header.
    #[prost(message, optional, tag = "1")]
    pub header: Option<ResponseHeader>,
    /// The watch the response is for.
    #[prost(int64, tag = "2")]
    pub watch_id: i64,
    /// Whether this answers a create request.
    #[prost(bool, tag = "3")]
    pub created: bool,
    /// Whether the watch was canceled; no events follow.
    #[prost(bool, tag = "4")]
    pub canceled: bool,
    /// Set when the requested start revision was compacted away: the
    /// oldest revision still available. The watch is canceled.
    #[prost(int64, tag = "5")]
    pub compact_revision: i64,
    /// Why the watch was canceled.
    #[prost(string, tag = "6")]
    pub cancel_reason: String,
    /// The events.
    #[prost(message, repeated, tag = "11")]
    pub events: Vec<Event>,
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;

    fn hex(text: &str) -> Vec<u8> {
        (0..text.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
            .collect()
    }

    /// The header every captured response carries.
    fn captured_header(revision: i64) -> Option<ResponseHeader> {
        Some(ResponseHeader {
            cluster_id: 324_952_591_200_643_719,
            member_id: 3_319_814_642_761_637_952,
            revision,
            raft_term: 2,
        })
    }

    // The fixtures are `etcdctl -w protobuf` output from etcd 3.6.10,
    // after `put fx/cluster.json '{"g":1}'` and `put fx/buckets/a.json va`.

    #[test]
    fn decodes_a_captured_range_response() {
        // etcdctl get fx/cluster.json
        let bytes = hex(
            "0a180887d5edba8fd59dc10410c0c0caafc1ba96892e1803200212200a0f66782f636c75737465722e6a\
             736f6e1002180220012a077b2267223a317d2001",
        );
        let response = RangeResponse::decode(bytes.as_slice()).unwrap();
        assert_eq!(response.header, captured_header(3));
        assert_eq!(
            response.kvs,
            [KeyValue {
                key: Bytes::from_static(b"fx/cluster.json"),
                create_revision: 2,
                mod_revision: 2,
                version: 1,
                value: Bytes::from_static(br#"{"g":1}"#),
                lease: 0,
            }]
        );
        assert_eq!((response.more, response.count), (false, 1));
    }

    #[test]
    fn decodes_a_captured_listing() {
        // etcdctl get fx/ --prefix --keys-only
        let bytes = hex(
            "0a180887d5edba8fd59dc10410c0c0caafc1ba96892e1803200212190a1166782f6275636b6574732f61\
             2e6a736f6e10031803200112170a0f66782f636c75737465722e6a736f6e1002180220012002",
        );
        let response = RangeResponse::decode(bytes.as_slice()).unwrap();
        let keys: Vec<_> = response
            .kvs
            .iter()
            .map(|kv| (kv.key.as_ref(), kv.mod_revision, kv.value.is_empty()))
            .collect();
        assert_eq!(
            keys,
            [
                (b"fx/buckets/a.json".as_slice(), 3, true),
                (b"fx/cluster.json".as_slice(), 2, true),
            ]
        );
        assert_eq!(response.count, 2);
    }

    #[test]
    fn decodes_captured_transactions() {
        // etcdctl txn: mod("fx/cluster.json") = "2", then put, run twice.
        let won = hex("0a180887d5edba8fd59dc10410c0c0caafc1ba96892e1804200210011a0612040a021804");
        let response = TxnResponse::decode(won.as_slice()).unwrap();
        assert_eq!(response.header, captured_header(4));
        assert!(response.succeeded);
        let lost = hex("0a180887d5edba8fd59dc10410c0c0caafc1ba96892e18042002");
        let response = TxnResponse::decode(lost.as_slice()).unwrap();
        assert_eq!(response.header, captured_header(4));
        assert!(!response.succeeded);
    }

    #[test]
    fn decodes_a_captured_watch_event() {
        // etcdctl watch fx/cluster.json, during put fx/cluster.json g3.
        let bytes = hex(
            "0a180887d5edba8fd59dc10410c0c0caafc1ba96892e180520025a1d121b0a0f66782f636c7573746572\
             2e6a736f6e1002180520032a026733",
        );
        let response = WatchResponse::decode(bytes.as_slice()).unwrap();
        assert_eq!(response.header, captured_header(5));
        assert!(!response.created && !response.canceled);
        let [event] = response.events.as_slice() else {
            panic!("one event: {response:?}");
        };
        assert_eq!(event.r#type, EventType::Put as i32);
        let kv = event.kv.as_ref().unwrap();
        assert_eq!(kv.key.as_ref(), b"fx/cluster.json");
        assert_eq!((kv.mod_revision, kv.version), (5, 3));
        assert_eq!(kv.value.as_ref(), b"g3");
    }

    #[test]
    fn requests_encode_with_the_upstream_field_numbers() {
        // A conditional put: Compare (field 1) of mod_revision (target 2,
        // field 2) on the key (field 3) at revision 7 (field 6), then a
        // put (RequestOp field 2) on success (field 2).
        let request = TxnRequest {
            compare: vec![Compare {
                result: CompareResult::Equal as i32,
                target: CompareTarget::Mod as i32,
                key: Bytes::from_static(b"k"),
                target_union: Some(TargetUnion::ModRevision(7)),
            }],
            success: vec![RequestOp {
                request: Some(Request::Put(PutRequest {
                    key: Bytes::from_static(b"k"),
                    value: Bytes::from_static(b"v"),
                })),
            }],
            failure: Vec::new(),
        };
        assert_eq!(
            request.encode_to_vec(),
            hex("0a0710021a016b3007120812060a016b120176")
        );
        let watch = WatchRequest {
            request_union: Some(WatchRequestUnion::Create(WatchCreateRequest {
                key: Bytes::from_static(b"k"),
                range_end: Bytes::new(),
                start_revision: 0,
            })),
        };
        assert_eq!(watch.encode_to_vec(), hex("0a030a016b"));
    }
}
