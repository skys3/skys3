//! What names a key's payload: the positions of the records that hold the
//! bytes of its entry and of its open multipart uploads, which compaction
//! must keep or evict and may otherwise drop (§10.3).

use std::collections::BTreeMap;

use redb::ReadableTable;
use skys3_log::record::ShardRef;
use skys3_types::EpochSeq;

use crate::codec;
use crate::entry::{EntryState, Payload};
use crate::error::IndexError;
use crate::tables::{self, Bytes};

/// What names a record that holds payload of a key on this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Holder {
    /// The key's entry, in `state` at `version`: the record holds the
    /// entry's bytes, or a part of its multipart object's.
    Entry {
        /// The entry's state. An evicted entry names no payload, so it is
        /// never a holder.
        state: EntryState,
        /// The entry's version.
        version: EpochSeq,
    },
    /// The key's entry, whose version is coded by the `EC_PUBLISH` at
    /// `publish` (§8.4): the record holds the version's replicated bytes,
    /// which a replica keeps only until it knows that record committed.
    Coded {
        /// The position of the `EC_PUBLISH` record.
        publish: EpochSeq,
    },
    /// A part of an open multipart upload of the key.
    Upload,
}

/// The positions of the records that hold a key's bytes on this node, and
/// what names each: see [`IndexReader::holders`](crate::IndexReader::holders).
pub type Holders = BTreeMap<EpochSeq, Holder>;

/// The holders of `key` in `shard`, from the namespace, uploads, and parts
/// tables of one transaction.
pub(crate) fn holders<N, U, P>(
    namespace: &N,
    uploads: &U,
    parts: &P,
    shard: &ShardRef,
    key: &str,
) -> Result<Holders, IndexError>
where
    N: ReadableTable<Bytes, Bytes>,
    U: ReadableTable<Bytes, Bytes>,
    P: ReadableTable<Bytes, Bytes>,
{
    let mut held = Holders::new();
    if let Some(entry) = tables::entry(namespace, shard, key)?
        && let Some(object) = &entry.object
    {
        let holder = match &object.coded {
            Some(coded) => Holder::Coded {
                publish: coded.publish,
            },
            None => Holder::Entry {
                state: entry.state,
                version: entry.version,
            },
        };
        match &object.payload {
            Payload::Parts { upload, .. } => {
                for (_, part) in tables::parts(parts, shard, *upload, 0, usize::MAX)? {
                    add(&mut held, &part.payload, holder);
                }
            }
            payload => add(&mut held, payload, holder),
        }
    }
    // An upload's rows are keyed by the key's namespace key, a zero byte,
    // and the upload's position.
    let start = [codec::entry_key(shard, key).as_slice(), &[0]].concat();
    let end = tables::prefix_end(&start);
    for row in uploads.range(start.as_slice()..end.as_slice())? {
        let (row_key, _) = row?;
        let (_, name, upload) =
            codec::decode_upload_key(row_key.value()).map_err(IndexError::codec("uploads"))?;
        if name != key {
            continue;
        }
        for (_, part) in tables::parts(parts, shard, upload, 0, usize::MAX)? {
            add(&mut held, &part.payload, Holder::Upload);
        }
    }
    Ok(held)
}

/// Adds the positions `payload` names, held by `holder`.
fn add(held: &mut Holders, payload: &Payload, holder: Holder) {
    let positions: Vec<EpochSeq> = match payload {
        Payload::None | Payload::Parts { .. } => Vec::new(),
        Payload::Inline(position) => vec![*position],
        Payload::Extents(extents) => extents.iter().map(|extent| extent.position).collect(),
    };
    for position in positions {
        held.entry(position).or_insert(holder);
    }
}
