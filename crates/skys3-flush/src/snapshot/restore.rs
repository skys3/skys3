//! Reading a shard's latest snapshot back from its snapshot target.

use std::collections::{BTreeMap, BTreeSet};

use skys3_index::{Entry, ShardTable, Upload, codec};
use skys3_log::ShardRef;
use skys3_remote::{GetObject, ListObjectsV2, ObjectStore};
use skys3_types::EpochSeq;

use super::SnapshotError;
use super::format::{
    ChainId, Contents, Snapshot, object_key, parse_object_key, row_digest, shard_dir,
};
use super::hooks::{self, SnapshotBug};

/// A shard's index as its latest snapshot holds it: a chain's base with
/// every delta after it applied, up to the first that is missing or does
/// not decode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Restored {
    /// The shard.
    pub shard: ShardRef,
    /// What the snapshots hold of the shard.
    pub contents: Contents,
    /// The chain restored.
    pub chain: ChainId,
    /// The last snapshot of the chain applied: 0 for the base alone.
    pub number: u32,
    /// The applied position its rows were read at.
    pub position: EpochSeq,
    /// When it was taken, on the primary's clock, in milliseconds since
    /// the Unix epoch: every write acknowledged before it is in the rows.
    pub taken_ms: u64,
    /// The rows, by table code and key.
    rows: BTreeMap<(u32, Vec<u8>), Vec<u8>>,
}

impl Restored {
    fn base(snapshot: Snapshot) -> Self {
        let rows = snapshot
            .rows
            .into_iter()
            .map(|(table, (key, value))| ((table.code(), key), value))
            .collect();
        Self {
            shard: snapshot.shard,
            contents: snapshot.contents,
            chain: snapshot.chain,
            number: 0,
            position: snapshot.position,
            taken_ms: snapshot.taken_ms,
            rows,
        }
    }

    /// Applies the next delta of the chain.
    fn apply(&mut self, delta: Snapshot) {
        if !delta.removed.is_empty() {
            let removed: BTreeSet<_> = delta.removed.into_iter().collect();
            self.rows
                .retain(|(code, key), _| match ShardTable::from_code(*code) {
                    Some(table) => !removed.contains(&row_digest(table, key)),
                    None => true,
                });
        }
        for (table, (key, value)) in delta.rows {
            self.rows.insert((table.code(), key), value);
        }
        self.number = delta.number;
        self.position = delta.position;
        self.taken_ms = delta.taken_ms;
    }

    /// The rows of `table`, in key order, as the index stores them: what
    /// re-indexing starts from (plan M5-11).
    pub fn rows(&self, table: ShardTable) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.rows
            .range((table.code(), Vec::new())..)
            .take_while(move |((code, _), _)| *code == table.code())
            .map(|((_, key), value)| (key.as_slice(), value.as_slice()))
    }

    /// The entries, by object key.
    ///
    /// # Errors
    ///
    /// [`SnapshotError::Row`] if a row does not decode.
    pub fn entries(&self) -> Result<BTreeMap<String, Entry>, SnapshotError> {
        self.rows(ShardTable::Namespace)
            .map(|(key, value)| {
                let (_, name) = codec::decode_entry_key(key).map_err(row)?;
                Ok((name, codec::decode_entry(value).map_err(row)?))
            })
            .collect()
    }

    /// The open multipart uploads: key, upload position, the upload, and
    /// how many parts it holds.
    ///
    /// # Errors
    ///
    /// [`SnapshotError::Row`] if a row does not decode.
    pub fn uploads(&self) -> Result<Vec<(String, EpochSeq, Upload, usize)>, SnapshotError> {
        let mut parts = BTreeMap::<EpochSeq, usize>::new();
        for (key, _) in self.rows(ShardTable::Parts) {
            let (_, upload, _) = codec::decode_part_key(key).map_err(row)?;
            *parts.entry(upload).or_default() += 1;
        }
        self.rows(ShardTable::Uploads)
            .map(|(key, value)| {
                let (_, name, at) = codec::decode_upload_key(key).map_err(row)?;
                let upload = codec::decode_upload(value).map_err(row)?;
                Ok((name, at, upload, parts.get(&at).copied().unwrap_or(0)))
            })
            .collect()
    }
}

fn row(error: codec::CodecError) -> SnapshotError {
    SnapshotError::Row(error.to_string())
}

/// Reads `shard`'s latest snapshot from the target `store`, whose prefix is
/// `prefix`: the greatest chain whose base decodes, with each delta after
/// it applied in order, up to the first that is missing or does not
/// decode. Returns `None` if the target holds no usable snapshot of the
/// shard.
///
/// # Errors
///
/// [`SnapshotError::Remote`] if listing or reading the target fails.
pub async fn latest<S: ObjectStore>(
    store: &S,
    prefix: &str,
    shard: &ShardRef,
) -> Result<Option<Restored>, SnapshotError> {
    let dir = shard_dir(prefix, shard);
    let mut chains = BTreeMap::<ChainId, BTreeSet<u32>>::new();
    let mut token = None;
    loop {
        let mut request = ListObjectsV2::new(dir.clone());
        if let Some(token) = token.take() {
            request = request.with_continuation_token(token);
        }
        let page = store.list_objects_v2(request).await?;
        for object in page.objects {
            if let Some((chain, number)) = parse_object_key(&dir, &object.key) {
                chains.entry(chain).or_default().insert(number);
            }
        }
        match page.next_continuation_token {
            Some(next) if page.is_truncated => token = Some(next),
            _ => break,
        }
    }
    for (chain, numbers) in chains.iter().rev() {
        let Some(mut restored) = read(store, &dir, shard, chain, 0)
            .await?
            .map(Restored::base)
        else {
            continue;
        };
        let mut latest = (restored.number, restored.position, restored.taken_ms);
        for number in (1..).take_while(|number| numbers.contains(number)) {
            let Some(delta) = read(store, &dir, shard, chain, number).await? else {
                break;
            };
            latest = (delta.number, delta.position, delta.taken_ms);
            if hooks::snapshot_bug() != SnapshotBug::BaseOnly {
                restored.apply(delta);
            }
        }
        if hooks::snapshot_bug() == SnapshotBug::BaseOnly {
            (restored.number, restored.position, restored.taken_ms) = latest;
        }
        return Ok(Some(restored));
    }
    Ok(None)
}

/// Reads snapshot `number` of `chain`, or `None` if it is gone, does not
/// decode, or is not what its key says.
async fn read<S: ObjectStore>(
    store: &S,
    dir: &str,
    shard: &ShardRef,
    chain: &ChainId,
    number: u32,
) -> Result<Option<Snapshot>, SnapshotError> {
    let key = object_key(dir, chain, number);
    let body = match store.get_object(GetObject::new(key.clone())).await {
        Ok(object) => object.body,
        Err(error) if error.kind() == skys3_remote::S3ErrorKind::NoSuchKey => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    match Snapshot::decode(&body) {
        Ok(snapshot)
            if snapshot.shard == *shard
                && snapshot.chain == *chain
                && snapshot.number == number =>
        {
            Ok(Some(snapshot))
        }
        Ok(_) => {
            tracing::warn!(
                key,
                "a snapshot object is not what its key names; it is skipped"
            );
            Ok(None)
        }
        Err(error) => {
            tracing::warn!(key, %error, "a snapshot object does not decode; it is skipped");
            Ok(None)
        }
    }
}
