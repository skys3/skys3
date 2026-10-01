//! Shared helpers: proptest strategies for records, and a record builder
//! written from the documented layout rather than from the encoder.

#![allow(dead_code)]

use bytes::Bytes;
use proptest::collection::{btree_map, btree_set, vec};
use proptest::prelude::*;
use proptest::sample::{Index, subsequence};

use skys3_log::record::{
    Adopt, ChecksumAlgorithm, Checksums, CopySource, Delete, Extent, ExtentRef, Flushed, Import,
    LogRecord, MAX_PAYLOAD_LEN, Metadata, Put, PutData, RecordBody, ShardRef, TagSet, Tags,
};
use skys3_types::{
    BucketId, ETag, Epoch, EpochSeq, KeyHash, NodeId, ProposalId, Seq, ShardConfig, ShardId,
    VersionIdentity,
};

/// A lowercase DNS-label identifier of 1 to `max` bytes.
pub fn label(max: usize) -> impl Strategy<Value = String> {
    let middle = max - 2;
    prop_oneof![
        "[a-z0-9]".boxed(),
        proptest::string::string_regex(&format!("[a-z0-9][a-z0-9-]{{0,{middle}}}[a-z0-9]"))
            .unwrap()
            .boxed(),
    ]
}

pub fn bucket_id() -> impl Strategy<Value = BucketId> {
    label(BucketId::MAX_LEN).prop_map(|s| BucketId::new(s).unwrap())
}

pub fn shard_ref() -> impl Strategy<Value = ShardRef> {
    (bucket_id(), any::<u8>())
        .prop_map(|(bucket, shard)| ShardRef::new(bucket, ShardId::new(shard)))
}

pub fn epoch_seq() -> impl Strategy<Value = EpochSeq> {
    (any::<u64>(), any::<u64>()).prop_map(|(e, s)| EpochSeq::new(Epoch::new(e), Seq::new(s)))
}

/// A position after the start of the log, so earlier positions exist.
pub fn later_position() -> impl Strategy<Value = EpochSeq> {
    (1..=u64::MAX, any::<u64>()).prop_map(|(e, s)| EpochSeq::new(Epoch::new(e), Seq::new(s)))
}

/// A position strictly before `position`.
pub fn position_before(position: EpochSeq) -> impl Strategy<Value = EpochSeq> {
    (0..position.epoch.get(), any::<u64>())
        .prop_map(|(e, s)| EpochSeq::new(Epoch::new(e), Seq::new(s)))
}

pub fn key() -> impl Strategy<Value = String> {
    prop_oneof![
        8 => "(?s).{1,40}",
        1 => "[a-z/]{1000,1024}",
    ]
}

pub fn etag() -> impl Strategy<Value = ETag> {
    "[!#-~]{1,40}".prop_map(|s| ETag::new(s).unwrap())
}

pub fn metadata() -> impl Strategy<Value = Metadata> {
    btree_map(
        prop_oneof![
            "content-[a-z]{1,10}",
            "x-amz-meta-[a-z0-9!#$%&'*+.^_`|~-]{1,20}"
        ],
        "(?s).{0,40}",
        0..6,
    )
}

pub fn tags() -> impl Strategy<Value = TagSet> {
    btree_map("(?s).{1,20}", "(?s).{0,40}", 0..=10)
}

pub fn checksums() -> impl Strategy<Value = Checksums> {
    subsequence(
        ChecksumAlgorithm::ALL.to_vec(),
        0..=ChecksumAlgorithm::ALL.len(),
    )
    .prop_flat_map(|algorithms| {
        algorithms
            .into_iter()
            .map(|a| vec(any::<u8>(), a.digest_len()).prop_map(move |digest| (a, digest)))
            .collect::<Vec<_>>()
            .prop_map(|entries| entries.into_iter().collect::<Checksums>())
    })
}

pub fn version_id() -> impl Strategy<Value = Option<String>> {
    proptest::option::of("[A-Za-z0-9._-]{1,64}")
}

fn copy_source() -> impl Strategy<Value = CopySource> {
    (
        bucket_id(),
        key(),
        any::<u64>(),
        etag(),
        proptest::option::of(etag()),
    )
        .prop_map(|(bucket, key, seq, etag, remote_etag)| CopySource {
            bucket,
            key,
            version: VersionIdentity::new(Seq::new(seq), etag),
            remote_etag,
        })
}

fn put_data(position: EpochSeq) -> impl Strategy<Value = PutData> {
    let extent = (position_before(position), 1..=MAX_PAYLOAD_LEN)
        .prop_map(|(position, len)| ExtentRef { position, len });
    prop_oneof![
        vec(any::<u8>(), 0..300).prop_map(|data| PutData::Inline(Bytes::from(data))),
        vec(extent, 1..12).prop_map(PutData::Extents),
    ]
}

fn put(position: EpochSeq) -> impl Strategy<Value = Put> {
    (
        (key(), any::<u64>(), etag()),
        proptest::option::of(position_before(position)),
        (metadata(), tags(), checksums()),
        proptest::option::of(copy_source()),
        put_data(position),
    )
        .prop_map(
            |(
                (key, last_modified_ms, etag),
                inherited_identity,
                (metadata, tags, checksums),
                copy_source,
                data,
            )| {
                let size = match &data {
                    PutData::Inline(bytes) => bytes.len() as u64,
                    PutData::Extents(extents) => extents.iter().map(|e| u64::from(e.len)).sum(),
                };
                Put {
                    key,
                    size,
                    last_modified_ms,
                    etag,
                    inherited_identity,
                    metadata,
                    tags,
                    checksums,
                    copy_source,
                    data,
                }
            },
        )
}

fn node_id() -> impl Strategy<Value = NodeId> {
    label(NodeId::MAX_LEN).prop_map(|s| NodeId::new(s).unwrap())
}

fn proposal_id() -> impl Strategy<Value = ProposalId> {
    prop_oneof![
        "[A-Za-z0-9_-]{1,64}".prop_map(|s| ProposalId::new(s).unwrap()),
        any::<u128>().prop_map(ProposalId::from_u128),
    ]
}

/// A valid configuration of `shard` at `epoch`.
fn shard_config(shard: ShardRef, epoch: Epoch) -> impl Strategy<Value = ShardConfig> {
    (
        btree_set(node_id(), 1..=8),
        any::<Index>(),
        1..=u8::MAX,
        any::<Index>(),
        proposal_id(),
    )
        .prop_map(move |(nodes, split, replicas, min, proposal_id)| {
            let nodes: Vec<_> = nodes.into_iter().collect();
            let (members, learners) = nodes.split_at(split.index(nodes.len()) + 1);
            ShardConfig {
                bucket_id: shard.bucket.clone(),
                shard: shard.shard,
                epoch,
                primary: members[split.index(members.len())].clone(),
                members: members.to_vec(),
                learners: learners.to_vec(),
                min_write_replicas: u8::try_from(min.index(usize::from(replicas)) + 1).unwrap(),
                replicas,
                proposal_id,
            }
        })
}

fn body(shard: ShardRef, position: EpochSeq) -> impl Strategy<Value = RecordBody> {
    prop_oneof![
        put(position).prop_map(RecordBody::Put),
        key().prop_map(|key| RecordBody::Delete(Delete { key })),
        (key(), any::<u64>(), vec(any::<u8>(), 1..300))
            .prop_filter("fits below the largest offset", |(_, offset, data)| {
                offset.checked_add(data.len() as u64).is_some()
            })
            .prop_map(|(key, offset, data)| RecordBody::Extent(Extent {
                key,
                offset,
                data: data.into(),
            })),
        (key(), tags()).prop_map(|(key, tags)| RecordBody::Tags(Tags { key, tags })),
        (
            key(),
            any::<u64>(),
            proptest::option::of(etag()),
            version_id()
        )
            .prop_map(
                |(key, seq, remote_etag, remote_version_id)| RecordBody::Flushed(Flushed {
                    key,
                    seq: Seq::new(seq),
                    remote_etag,
                    remote_version_id,
                })
            ),
        (
            key(),
            any::<u64>(),
            any::<u64>(),
            etag(),
            proptest::option::of("[A-Z_]{1,20}")
        )
            .prop_map(|(key, size, last_modified_ms, etag, storage_class)| {
                RecordBody::Import(Import {
                    key,
                    size,
                    last_modified_ms,
                    etag,
                    storage_class,
                })
            }),
        (
            (key(), any::<u64>(), any::<u64>(), any::<u64>()),
            (etag(), version_id(), metadata(), checksums())
        )
            .prop_map(
                |(
                    (key, seq, size, last_modified_ms),
                    (remote_etag, remote_version_id, metadata, checksums),
                )| {
                    RecordBody::Adopt(Adopt {
                        key,
                        expected_seq: Seq::new(seq),
                        size,
                        last_modified_ms,
                        remote_etag,
                        remote_version_id,
                        metadata,
                        checksums,
                    })
                }
            ),
        shard_config(shard, position.epoch).prop_map(RecordBody::Config),
        Just(RecordBody::Truncate),
    ]
}

/// Any valid record of a defined kind.
pub fn record() -> impl Strategy<Value = LogRecord> {
    (shard_ref(), later_position()).prop_flat_map(|(shard, position)| {
        body(shard.clone(), position).prop_map(move |body| LogRecord {
            shard: shard.clone(),
            position,
            body,
        })
    })
}

/// Recomputes the CRC32C of a record that `record` holds exactly, as the
/// format specifies: over every byte from offset 8.
pub fn reseal(record: &mut [u8]) {
    let crc = crc32c::crc32c(&record[8..]);
    record[4..8].copy_from_slice(&crc.to_le_bytes());
}

/// The fields of a hand-built fixed header.
#[derive(Clone)]
pub struct Frame {
    pub kind: u16,
    pub bucket: &'static str,
    pub shard: u8,
    pub epoch: u64,
    pub seq: u64,
    pub key_hash: u64,
}

impl Frame {
    /// A frame of `kind` in shard 3 of bucket `b1`, at position 5.9.
    pub fn new(kind: u16) -> Self {
        Self {
            kind,
            bucket: "b1",
            shard: 3,
            epoch: 5,
            seq: 9,
            key_hash: 0,
        }
    }

    /// Sets the key hash to the hash of `key` in the frame's bucket.
    pub fn keyed(mut self, key: &str) -> Self {
        self.key_hash = KeyHash::of(&BucketId::new(self.bucket).unwrap(), key.as_bytes()).get();
        self
    }

    /// Builds a sealed record with this fixed header, following the layout
    /// in the `record` module documentation.
    pub fn build(&self, body: &[u8], payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(b"SKYL");
        out.extend_from_slice(&[0; 4]);
        out.extend_from_slice(&1u16.to_le_bytes());
        out.extend_from_slice(&self.kind.to_le_bytes());
        out.extend_from_slice(&u32::try_from(80 + body.len()).unwrap().to_le_bytes());
        out.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_le_bytes());
        out.push(self.shard);
        out.push(u8::try_from(self.bucket.len()).unwrap());
        out.extend_from_slice(&[0, 0]);
        out.extend_from_slice(&self.epoch.to_le_bytes());
        out.extend_from_slice(&self.seq.to_le_bytes());
        out.extend_from_slice(&self.key_hash.to_le_bytes());
        let mut bucket = [0; 32];
        bucket[..self.bucket.len()].copy_from_slice(self.bucket.as_bytes());
        out.extend_from_slice(&bucket);
        assert_eq!(out.len(), 80);
        out.extend_from_slice(body);
        out.extend_from_slice(payload);
        reseal(&mut out);
        out
    }
}

/// Builds a kind-specific header from its encoded fields.
#[derive(Default)]
pub struct Body(pub Vec<u8>);

impl Body {
    pub fn u8(mut self, v: u8) -> Self {
        self.0.push(v);
        self
    }
    pub fn u16(mut self, v: u16) -> Self {
        self.0.extend_from_slice(&v.to_le_bytes());
        self
    }
    pub fn u32(mut self, v: u32) -> Self {
        self.0.extend_from_slice(&v.to_le_bytes());
        self
    }
    pub fn u64(mut self, v: u64) -> Self {
        self.0.extend_from_slice(&v.to_le_bytes());
        self
    }
    pub fn raw(mut self, bytes: &[u8]) -> Self {
        self.0.extend_from_slice(bytes);
        self
    }
    /// Text with a `u8` length.
    pub fn str8(self, s: &str) -> Self {
        self.u8(u8::try_from(s.len()).unwrap()).raw(s.as_bytes())
    }
    /// Text with a `u16` length.
    pub fn str16(self, s: &str) -> Self {
        self.u16(u16::try_from(s.len()).unwrap()).raw(s.as_bytes())
    }
}
