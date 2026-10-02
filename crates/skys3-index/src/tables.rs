//! The index's redb tables and typed access to them.
//!
//! The [`codec`](crate::codec) module documents each table's key and value
//! encoding.

use std::collections::BTreeMap;
use std::time::Duration;

use redb::{ReadOnlyTable, ReadableTable, Table, TableDefinition};
use skys3_log::RecordLocation;
use skys3_log::record::ShardRef;
use skys3_types::{Epoch, EpochSeq, Generation, ShardConfig};

use crate::codec;
use crate::entry::{ControlEntry, Entry, Part, Upload};
use crate::error::IndexError;
use crate::listing::{self, ListPage, ListQuery};

type Bytes = &'static [u8];

/// Each shard's namespace index, keyed by shard and object key.
pub(crate) const NAMESPACE: TableDefinition<Bytes, Bytes> = TableDefinition::new("namespace");
/// The node-local location map, keyed by shard and record position.
pub(crate) const LOCATIONS: TableDefinition<Bytes, Bytes> = TableDefinition::new("locations");
/// Each shard's applied position.
pub(crate) const SHARDS: TableDefinition<Bytes, Bytes> = TableDefinition::new("shards");
/// Each shard's open multipart uploads, keyed by shard, key, and upload.
pub(crate) const UPLOADS: TableDefinition<Bytes, Bytes> = TableDefinition::new("uploads");
/// The parts of open uploads and of multipart objects, keyed by shard,
/// upload, and part number.
pub(crate) const PARTS: TableDefinition<Bytes, Bytes> = TableDefinition::new("parts");
/// What each log segment holds, as of the durable checkpoint.
pub(crate) const COVERAGE: TableDefinition<Bytes, Bytes> = TableDefinition::new("coverage");
/// The node's local copy of control state, keyed by register key.
pub(crate) const CONTROL: TableDefinition<&str, Bytes> = TableDefinition::new("control");
/// Each bucket's namespace import checkpoint, keyed by bucket ID.
pub(crate) const IMPORTS: TableDefinition<&str, Bytes> = TableDefinition::new("imports");
/// The gateway's shard map (§6.2): the newest configuration the node knows
/// of each shard, keyed by shard. It is a cache that is safe when stale,
/// so a build that does not know the table loses nothing by ignoring it,
/// and the table needs no new format version.
pub(crate) const SHARD_MAP: TableDefinition<Bytes, Bytes> = TableDefinition::new("shard_map");
/// The epoch in which this node's replica of each shard stepped down as
/// primary for a planned handoff (§5.4), keyed by shard. Unlike the shard
/// map it is not a cache: a build that ignored it could serve again in
/// that epoch, so it came with a new format version.
pub(crate) const STEP_DOWNS: TableDefinition<Bytes, u64> = TableDefinition::new("step_downs");
/// The promotion of a learner that this node's replica of each shard
/// proposed as primary and has not seen the outcome of (§6.3, §6.7): the
/// configuration it proposed, keyed by shard. A build that ignored it
/// could stop waiting for a learner the register has made a member, so it
/// came with a new format version.
pub(crate) const PROMOTIONS: TableDefinition<Bytes, Bytes> = TableDefinition::new("promotions");
/// Single values: the format version and the control generation.
pub(crate) const META: TableDefinition<&str, u64> = TableDefinition::new("meta");

/// The key of the index format version in [`META`].
pub(crate) const FORMAT_VERSION_KEY: &str = "format_version";
/// The key of the control generation in [`META`].
const CONTROL_GENERATION_KEY: &str = "control_generation";
/// The key in [`META`] of when the sync that produced the control copy
/// started, in milliseconds since the Unix epoch.
const CONTROL_SYNCED_AT_KEY: &str = "control_synced_at_ms";

fn entry<T: ReadableTable<Bytes, Bytes>>(
    table: &T,
    shard: &ShardRef,
    key: &str,
) -> Result<Option<Entry>, IndexError> {
    let Some(value) = table.get(codec::entry_key(shard, key).as_slice())? else {
        return Ok(None);
    };
    codec::decode_entry(value.value())
        .map(Some)
        .map_err(IndexError::codec("namespace"))
}

fn location<T: ReadableTable<Bytes, Bytes>>(
    table: &T,
    shard: &ShardRef,
    position: EpochSeq,
) -> Result<Option<RecordLocation>, IndexError> {
    let Some(value) = table.get(codec::location_key(shard, position).as_slice())? else {
        return Ok(None);
    };
    codec::decode_location(value.value())
        .map(Some)
        .map_err(IndexError::codec("locations"))
}

fn upload<T: ReadableTable<Bytes, Bytes>>(
    table: &T,
    shard: &ShardRef,
    key: &str,
    upload: EpochSeq,
) -> Result<Option<Upload>, IndexError> {
    let Some(value) = table.get(codec::upload_key(shard, key, upload).as_slice())? else {
        return Ok(None);
    };
    codec::decode_upload(value.value())
        .map(Some)
        .map_err(IndexError::codec("uploads"))
}

/// Up to `limit` parts of the upload opened at `upload`, in part order,
/// after part `after` (0 for the first).
fn parts<T: ReadableTable<Bytes, Bytes>>(
    table: &T,
    shard: &ShardRef,
    upload: EpochSeq,
    after: u16,
    limit: usize,
) -> Result<Vec<(u16, Part)>, IndexError> {
    let prefix = codec::upload_parts_prefix(shard, upload);
    let end = prefix_end(&prefix);
    let Some(first) = after.checked_add(1) else {
        return Ok(Vec::new());
    };
    let start = codec::part_key(shard, upload, first);
    let mut page = Vec::new();
    for row in table.range(start.as_slice()..end.as_slice())? {
        if page.len() == limit {
            break;
        }
        let (key, value) = row?;
        let (_, _, number) =
            codec::decode_part_key(key.value()).map_err(IndexError::codec("parts"))?;
        let part = codec::decode_part(value.value()).map_err(IndexError::codec("parts"))?;
        page.push((number, part));
    }
    Ok(page)
}

fn applied<T: ReadableTable<Bytes, Bytes>>(
    table: &T,
    shard: &ShardRef,
) -> Result<Option<EpochSeq>, IndexError> {
    let Some(value) = table.get(codec::shard_key(shard).as_slice())? else {
        return Ok(None);
    };
    codec::decode_applied(value.value())
        .map(Some)
        .map_err(IndexError::codec("shards"))
}

/// Reads every row of a table keyed and valued by bytes, decoding both.
fn read_all<T, K, V>(
    table: &T,
    name: &'static str,
    key: impl Fn(&[u8]) -> Result<K, codec::CodecError>,
    value: impl Fn(&[u8]) -> Result<V, codec::CodecError>,
) -> Result<BTreeMap<K, V>, IndexError>
where
    T: ReadableTable<Bytes, Bytes>,
    K: Ord,
{
    let mut rows = BTreeMap::new();
    for row in table.iter()? {
        let (k, v) = row?;
        let k = key(k.value()).map_err(IndexError::codec(name))?;
        let v = value(v.value()).map_err(IndexError::codec(name))?;
        rows.insert(k, v);
    }
    Ok(rows)
}

/// Returns the smallest key greater than every key that starts with
/// `prefix`. A shard key ends with its shard number and starts with a
/// bucket ID length below 255, so it never consists of `0xff` bytes alone.
pub(crate) fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < u8::MAX {
            end.push(last + 1);
            break;
        }
    }
    end
}

/// A table of a shard's state that a snapshot carries (§6.7): what a learner
/// needs besides its applied position, which the snapshot names, and the
/// location map, which is node-local.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShardTable {
    /// The namespace index.
    Namespace,
    /// The open multipart uploads.
    Uploads,
    /// The parts of uploads and multipart objects.
    Parts,
}

impl ShardTable {
    /// Every table, in the order a snapshot sends them.
    pub const ALL: [Self; 3] = [Self::Namespace, Self::Uploads, Self::Parts];

    /// The table's code in a snapshot.
    #[must_use]
    pub const fn code(self) -> u32 {
        match self {
            Self::Namespace => 1,
            Self::Uploads => 2,
            Self::Parts => 3,
        }
    }

    /// The table with `code`, if any.
    #[must_use]
    pub fn from_code(code: u32) -> Option<Self> {
        Self::ALL.into_iter().find(|table| table.code() == code)
    }

    /// Checks that a row of the table, as a peer sent it, belongs to
    /// `shard` and decodes.
    pub(crate) fn check(self, shard: &ShardRef, key: &[u8], value: &[u8]) -> Result<(), String> {
        let theirs = match self {
            Self::Namespace => {
                codec::decode_entry(value).map_err(|e| e.to_string())?;
                codec::decode_entry_key(key).map(|(shard, _)| shard)
            }
            Self::Uploads => {
                codec::decode_upload(value).map_err(|e| e.to_string())?;
                codec::decode_upload_key(key).map(|(shard, ..)| shard)
            }
            Self::Parts => {
                codec::decode_part(value).map_err(|e| e.to_string())?;
                codec::decode_part_key(key).map(|(shard, ..)| shard)
            }
        }
        .map_err(|e| e.to_string())?;
        if theirs == *shard {
            Ok(())
        } else {
            Err(format!("a row of shard {theirs} in a snapshot of {shard}"))
        }
    }
}

/// Write access to the tables that applying records changes, inside one
/// write transaction (see [`Applier`](crate::Applier)).
///
/// Changes become visible to readers when the transaction commits, and
/// durable at the next checkpoint.
pub struct IndexWriter<'txn> {
    namespace: Table<'txn, Bytes, Bytes>,
    locations: Table<'txn, Bytes, Bytes>,
    shards: Table<'txn, Bytes, Bytes>,
    uploads: Table<'txn, Bytes, Bytes>,
    parts: Table<'txn, Bytes, Bytes>,
}

impl<'txn> IndexWriter<'txn> {
    pub(crate) fn open(txn: &'txn redb::WriteTransaction) -> Result<Self, IndexError> {
        Ok(Self {
            namespace: txn.open_table(NAMESPACE)?,
            locations: txn.open_table(LOCATIONS)?,
            shards: txn.open_table(SHARDS)?,
            uploads: txn.open_table(UPLOADS)?,
            parts: txn.open_table(PARTS)?,
        })
    }

    /// Returns the open upload of `key` in `shard` opened at `upload`.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading or decoding fails.
    pub fn upload(
        &self,
        shard: &ShardRef,
        key: &str,
        upload: EpochSeq,
    ) -> Result<Option<Upload>, IndexError> {
        self::upload(&self.uploads, shard, key, upload)
    }

    /// Stores the open upload of `key` in `shard` opened at `position`.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if the upload cannot be encoded or written.
    pub fn put_upload(
        &mut self,
        shard: &ShardRef,
        key: &str,
        position: EpochSeq,
        upload: &Upload,
    ) -> Result<(), IndexError> {
        let value = codec::encode_upload(upload).map_err(IndexError::codec("uploads"))?;
        self.uploads.insert(
            codec::upload_key(shard, key, position).as_slice(),
            value.as_slice(),
        )?;
        Ok(())
    }

    /// Removes an open upload, but not its parts, returning whether it
    /// existed.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if the write fails.
    pub fn remove_upload(
        &mut self,
        shard: &ShardRef,
        key: &str,
        upload: EpochSeq,
    ) -> Result<bool, IndexError> {
        Ok(self
            .uploads
            .remove(codec::upload_key(shard, key, upload).as_slice())?
            .is_some())
    }

    /// Returns part `number` of the upload opened at `upload`.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading or decoding fails.
    pub fn part(
        &self,
        shard: &ShardRef,
        upload: EpochSeq,
        number: u16,
    ) -> Result<Option<Part>, IndexError> {
        let key = codec::part_key(shard, upload, number);
        let Some(value) = self.parts.get(key.as_slice())? else {
            return Ok(None);
        };
        codec::decode_part(value.value())
            .map(Some)
            .map_err(IndexError::codec("parts"))
    }

    /// Returns every part of the upload opened at `upload`, in part order.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading or decoding fails.
    pub fn parts(
        &self,
        shard: &ShardRef,
        upload: EpochSeq,
    ) -> Result<Vec<(u16, Part)>, IndexError> {
        parts(&self.parts, shard, upload, 0, usize::MAX)
    }

    /// Stores part `number` of the upload opened at `upload`, replacing any
    /// other.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if the part cannot be encoded or written.
    pub fn put_part(
        &mut self,
        shard: &ShardRef,
        upload: EpochSeq,
        number: u16,
        part: &Part,
    ) -> Result<(), IndexError> {
        let value = codec::encode_part(part).map_err(IndexError::codec("parts"))?;
        self.parts.insert(
            codec::part_key(shard, upload, number).as_slice(),
            value.as_slice(),
        )?;
        Ok(())
    }

    /// Removes part `number` of the upload opened at `upload`, returning
    /// whether it existed.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if the write fails.
    pub fn remove_part(
        &mut self,
        shard: &ShardRef,
        upload: EpochSeq,
        number: u16,
    ) -> Result<bool, IndexError> {
        Ok(self
            .parts
            .remove(codec::part_key(shard, upload, number).as_slice())?
            .is_some())
    }

    /// Returns the entry of `key` in `shard`.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading or decoding fails.
    pub fn entry(&self, shard: &ShardRef, key: &str) -> Result<Option<Entry>, IndexError> {
        entry(&self.namespace, shard, key)
    }

    /// Stores the entry of `key` in `shard`, replacing any other.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if the entry cannot be encoded or written.
    pub fn put_entry(
        &mut self,
        shard: &ShardRef,
        key: &str,
        entry: &Entry,
    ) -> Result<(), IndexError> {
        let value = codec::encode_entry(entry).map_err(IndexError::codec("namespace"))?;
        self.namespace
            .insert(codec::entry_key(shard, key).as_slice(), value.as_slice())?;
        Ok(())
    }

    /// Removes the entry of `key` in `shard`, returning whether it existed.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if the write fails.
    pub fn remove_entry(&mut self, shard: &ShardRef, key: &str) -> Result<bool, IndexError> {
        Ok(self
            .namespace
            .remove(codec::entry_key(shard, key).as_slice())?
            .is_some())
    }

    /// Returns the location of the record at `position` in `shard`.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading or decoding fails.
    pub fn location(
        &self,
        shard: &ShardRef,
        position: EpochSeq,
    ) -> Result<Option<RecordLocation>, IndexError> {
        location(&self.locations, shard, position)
    }

    /// Records where the record at `position` in `shard` is on this node.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if the write fails.
    pub fn put_location(
        &mut self,
        shard: &ShardRef,
        position: EpochSeq,
        location: &RecordLocation,
    ) -> Result<(), IndexError> {
        self.locations.insert(
            codec::location_key(shard, position).as_slice(),
            codec::encode_location(location).as_slice(),
        )?;
        Ok(())
    }

    /// Removes the location of the record at `position` in `shard`,
    /// returning whether it existed.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if the write fails.
    pub fn remove_location(
        &mut self,
        shard: &ShardRef,
        position: EpochSeq,
    ) -> Result<bool, IndexError> {
        Ok(self
            .locations
            .remove(codec::location_key(shard, position).as_slice())?
            .is_some())
    }

    /// Returns the position of the last record applied to `shard`, or
    /// `None` if it has applied none.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading or decoding fails.
    pub fn applied(&self, shard: &ShardRef) -> Result<Option<EpochSeq>, IndexError> {
        applied(&self.shards, shard)
    }

    /// Removes everything the index holds for `shard`: its entries, its
    /// record locations, its uploads and parts, and its applied position.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if the write fails.
    pub fn remove_shard(&mut self, shard: &ShardRef) -> Result<(), IndexError> {
        let prefix = codec::shard_key(shard);
        let end = prefix_end(&prefix);
        for table in [
            &mut self.namespace,
            &mut self.locations,
            &mut self.uploads,
            &mut self.parts,
        ] {
            let range = prefix.as_slice()..end.as_slice();
            table.retain_in(range, |key, _| !key.starts_with(&prefix))?;
        }
        self.shards.remove(prefix.as_slice())?;
        Ok(())
    }

    pub(crate) fn applied_positions(&self) -> Result<BTreeMap<ShardRef, EpochSeq>, IndexError> {
        read_all(
            &self.shards,
            "shards",
            codec::decode_shard_key,
            codec::decode_applied,
        )
    }

    pub(crate) fn set_applied(
        &mut self,
        shard: &ShardRef,
        position: EpochSeq,
    ) -> Result<(), IndexError> {
        self.shards.insert(
            codec::shard_key(shard).as_slice(),
            codec::encode_applied(position).as_slice(),
        )?;
        Ok(())
    }
}

/// Write access to the node's local copy of control state (§6.2), inside
/// one durable write transaction (see
/// [`Index::update_control`](crate::Index::update_control)).
pub struct ControlWriter<'txn> {
    control: Table<'txn, &'static str, Bytes>,
    meta: Table<'txn, &'static str, u64>,
}

impl<'txn> ControlWriter<'txn> {
    pub(crate) fn open(txn: &'txn redb::WriteTransaction) -> Result<Self, IndexError> {
        Ok(Self {
            control: txn.open_table(CONTROL)?,
            meta: txn.open_table(META)?,
        })
    }

    /// Returns the copy of register `key`.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading or decoding fails.
    pub fn get(&self, key: &str) -> Result<Option<ControlEntry>, IndexError> {
        control(&self.control, key)
    }

    /// Stores the copy of register `key`.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if the copy cannot be encoded or written.
    pub fn put(&mut self, key: &str, entry: &ControlEntry) -> Result<(), IndexError> {
        let value = codec::encode_control(entry).map_err(IndexError::codec("control"))?;
        self.control.insert(key, value.as_slice())?;
        Ok(())
    }

    /// Removes the copy of register `key`, returning whether it existed.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if the write fails.
    pub fn remove(&mut self, key: &str) -> Result<bool, IndexError> {
        Ok(self.control.remove(key)?.is_some())
    }

    /// Removes the copy of every register, before a sync stores a complete
    /// new copy.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if the write fails.
    pub fn clear(&mut self) -> Result<(), IndexError> {
        self.control.retain(|_, _| false)?;
        Ok(())
    }

    /// Records when the sync that produced the copy started, as time since
    /// the Unix epoch, so a restarted node knows the copy's age (§6.2).
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if the write fails.
    pub fn set_synced_at(&mut self, since_epoch: Duration) -> Result<(), IndexError> {
        let millis = u64::try_from(since_epoch.as_millis()).unwrap_or(u64::MAX);
        self.meta.insert(CONTROL_SYNCED_AT_KEY, millis)?;
        Ok(())
    }

    /// Records that the local copy is complete up to `generation`.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if the write fails.
    pub fn set_generation(&mut self, generation: Generation) -> Result<(), IndexError> {
        self.meta.insert(CONTROL_GENERATION_KEY, generation.get())?;
        Ok(())
    }
}

fn control<T: ReadableTable<&'static str, Bytes>>(
    table: &T,
    key: &str,
) -> Result<Option<ControlEntry>, IndexError> {
    let Some(value) = table.get(key)? else {
        return Ok(None);
    };
    codec::decode_control(value.value())
        .map(Some)
        .map_err(IndexError::codec("control"))
}

/// Everything the index holds except its own bookkeeping, for comparing two
/// indexes, as the crash tests do, and for inspection tools.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IndexDump {
    /// Namespace entries by shard and key.
    pub entries: BTreeMap<(ShardRef, String), Entry>,
    /// Record locations by shard and position.
    pub locations: BTreeMap<(ShardRef, EpochSeq), RecordLocation>,
    /// Each shard's applied position.
    pub applied: BTreeMap<ShardRef, EpochSeq>,
    /// Open multipart uploads by shard, key, and upload.
    pub uploads: BTreeMap<(ShardRef, String, EpochSeq), Upload>,
    /// Parts by shard, upload, and part number.
    pub parts: BTreeMap<(ShardRef, EpochSeq, u16), Part>,
    /// The local copy of control state, by register key.
    pub control: BTreeMap<String, ControlEntry>,
    /// The generation the control-state copy is complete up to.
    pub control_generation: Option<Generation>,
}

/// A consistent read-only view of the index: a redb read transaction.
pub struct IndexReader {
    namespace: ReadOnlyTable<Bytes, Bytes>,
    locations: ReadOnlyTable<Bytes, Bytes>,
    shards: ReadOnlyTable<Bytes, Bytes>,
    uploads: ReadOnlyTable<Bytes, Bytes>,
    parts: ReadOnlyTable<Bytes, Bytes>,
    control: ReadOnlyTable<&'static str, Bytes>,
    shard_map: ReadOnlyTable<Bytes, Bytes>,
    step_downs: ReadOnlyTable<Bytes, u64>,
    promotions: ReadOnlyTable<Bytes, Bytes>,
    meta: ReadOnlyTable<&'static str, u64>,
}

impl IndexReader {
    pub(crate) fn open(txn: &redb::ReadTransaction) -> Result<Self, IndexError> {
        Ok(Self {
            namespace: txn.open_table(NAMESPACE)?,
            locations: txn.open_table(LOCATIONS)?,
            shards: txn.open_table(SHARDS)?,
            uploads: txn.open_table(UPLOADS)?,
            parts: txn.open_table(PARTS)?,
            control: txn.open_table(CONTROL)?,
            shard_map: txn.open_table(SHARD_MAP)?,
            step_downs: txn.open_table(STEP_DOWNS)?,
            promotions: txn.open_table(PROMOTIONS)?,
            meta: txn.open_table(META)?,
        })
    }

    /// Returns the epoch in which this node's replica of `shard` stepped
    /// down as primary for a planned handoff (§5.4), if it ever did.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading fails.
    pub fn step_down(&self, shard: &ShardRef) -> Result<Option<Epoch>, IndexError> {
        let value = self.step_downs.get(codec::shard_key(shard).as_slice())?;
        Ok(value.map(|epoch| Epoch::new(epoch.value())))
    }

    /// Returns the configuration this node's replica of `shard` last
    /// proposed to promote a learner in, as primary, if it recorded one
    /// (see [`Index::store_promotion`](crate::Index::store_promotion)).
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading or decoding fails.
    pub fn promotion(&self, shard: &ShardRef) -> Result<Option<ShardConfig>, IndexError> {
        let Some(value) = self.promotions.get(codec::shard_key(shard).as_slice())? else {
            return Ok(None);
        };
        codec::decode_route(value.value())
            .map(Some)
            .map_err(IndexError::codec("promotions"))
    }

    /// Returns the gateway's shard map: the configuration kept for each
    /// shard.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading or decoding fails.
    pub fn shard_map(&self) -> Result<BTreeMap<ShardRef, ShardConfig>, IndexError> {
        read_all(
            &self.shard_map,
            "shard_map",
            codec::decode_shard_key,
            codec::decode_route,
        )
    }

    /// Returns the open upload of `key` in `shard` opened at `upload`.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading or decoding fails.
    pub fn upload(
        &self,
        shard: &ShardRef,
        key: &str,
        upload: EpochSeq,
    ) -> Result<Option<Upload>, IndexError> {
        self::upload(&self.uploads, shard, key, upload)
    }

    /// Returns up to `limit` open uploads of `shard` whose keys start with
    /// `prefix`, in key order and, for one key, in the order they were
    /// opened: one page of ListMultipartUploads. With `after`, the page
    /// starts after that key and upload, or after every upload of that key
    /// if the upload is `None`.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading or decoding fails.
    pub fn uploads(
        &self,
        shard: &ShardRef,
        prefix: &str,
        after: Option<(&str, Option<EpochSeq>)>,
        limit: usize,
    ) -> Result<Vec<(String, EpochSeq, Upload)>, IndexError> {
        let shard_prefix = codec::shard_key(shard);
        let mut start = codec::entry_key(shard, prefix);
        // Whether the row at `start` itself is excluded.
        let mut exclusive = false;
        if let Some((key, upload)) = after {
            let after_start = match upload {
                Some(upload) => codec::upload_key(shard, key, upload),
                // Past every upload of `key`: its zero byte, then any
                // position, sort before this.
                None => [codec::entry_key(shard, key).as_slice(), &[1]].concat(),
            };
            if after_start > start {
                exclusive = upload.is_some();
                start = after_start;
            }
        }
        let mut page = Vec::new();
        for row in self.uploads.range(start.as_slice()..)? {
            if page.len() == limit {
                break;
            }
            let (row_key, value) = row?;
            let row_key = row_key.value();
            if !row_key.starts_with(&shard_prefix) {
                break;
            }
            if exclusive && row_key == start.as_slice() {
                continue;
            }
            let (_, key, upload) =
                codec::decode_upload_key(row_key).map_err(IndexError::codec("uploads"))?;
            if !key.starts_with(prefix) {
                break;
            }
            let value =
                codec::decode_upload(value.value()).map_err(IndexError::codec("uploads"))?;
            page.push((key, upload, value));
        }
        Ok(page)
    }

    /// Returns up to `limit` parts of the upload opened at `upload`, in
    /// part order, after part `after` (0 for the first): one page of
    /// ListParts, or the parts a read of a multipart object needs.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading or decoding fails.
    pub fn parts(
        &self,
        shard: &ShardRef,
        upload: EpochSeq,
        after: u16,
        limit: usize,
    ) -> Result<Vec<(u16, Part)>, IndexError> {
        parts(&self.parts, shard, upload, after, limit)
    }

    /// Returns the entry of `key` in `shard`.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading or decoding fails.
    pub fn entry(&self, shard: &ShardRef, key: &str) -> Result<Option<Entry>, IndexError> {
        entry(&self.namespace, shard, key)
    }

    /// Returns up to `limit` entries of `shard` in key order, starting
    /// after `start_after` if given: one page of a listing (§9.4).
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading or decoding fails.
    pub fn entries(
        &self,
        shard: &ShardRef,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, Entry)>, IndexError> {
        let prefix = codec::shard_key(shard);
        let start = match start_after {
            Some(key) => codec::entry_key(shard, key),
            None => prefix.clone(),
        };
        let mut page = Vec::new();
        for row in self.namespace.range(start.as_slice()..)? {
            if page.len() == limit {
                break;
            }
            let (key, value) = row?;
            let key = key.value();
            if !key.starts_with(&prefix) {
                break;
            }
            if key == start.as_slice() && start_after.is_some() {
                continue;
            }
            let (_, name) = codec::decode_entry_key(key).map_err(IndexError::codec("namespace"))?;
            let entry =
                codec::decode_entry(value.value()).map_err(IndexError::codec("namespace"))?;
            page.push((name, entry));
        }
        Ok(page)
    }

    /// Returns one page of `shard`'s listing (§9.4): its live objects and
    /// common prefixes in order, as [`ListQuery`] describes, skipping
    /// delete tombstones.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading or decoding fails.
    pub fn list(&self, shard: &ShardRef, query: &ListQuery) -> Result<ListPage, IndexError> {
        listing::list(&self.namespace, shard, query)
    }

    /// Returns the location of the record at `position` in `shard`.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading or decoding fails.
    pub fn location(
        &self,
        shard: &ShardRef,
        position: EpochSeq,
    ) -> Result<Option<RecordLocation>, IndexError> {
        location(&self.locations, shard, position)
    }

    /// Returns the position of the last record applied to `shard`.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading or decoding fails.
    pub fn applied(&self, shard: &ShardRef) -> Result<Option<EpochSeq>, IndexError> {
        applied(&self.shards, shard)
    }

    /// Returns every shard's applied position.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading or decoding fails.
    pub fn applied_positions(&self) -> Result<BTreeMap<ShardRef, EpochSeq>, IndexError> {
        read_all(
            &self.shards,
            "shards",
            codec::decode_shard_key,
            codec::decode_applied,
        )
    }

    /// Returns the copy of register `key`.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading or decoding fails.
    pub fn control(&self, key: &str) -> Result<Option<ControlEntry>, IndexError> {
        control(&self.control, key)
    }

    /// Returns the generation the control-state copy is complete up to, or
    /// `None` if the node has none.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading fails.
    pub fn control_generation(&self) -> Result<Option<Generation>, IndexError> {
        Ok(self
            .meta
            .get(CONTROL_GENERATION_KEY)?
            .map(|value| Generation::new(value.value())))
    }

    /// Returns the copy of every register, by register key.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading or decoding fails.
    pub fn control_entries(&self) -> Result<BTreeMap<String, ControlEntry>, IndexError> {
        let mut control = BTreeMap::new();
        for row in self.control.iter()? {
            let (key, value) = row?;
            let entry =
                codec::decode_control(value.value()).map_err(IndexError::codec("control"))?;
            control.insert(key.value().to_owned(), entry);
        }
        Ok(control)
    }

    /// Returns when the sync that produced the control copy started, as
    /// time since the Unix epoch, or `None` if it was never recorded.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading fails.
    pub fn control_synced_at(&self) -> Result<Option<Duration>, IndexError> {
        Ok(self
            .meta
            .get(CONTROL_SYNCED_AT_KEY)?
            .map(|value| Duration::from_millis(value.value())))
    }

    /// Returns rows of `shard` in `table`, in key order after the key
    /// `after` (from the first if `None`), as stored: at least one row if
    /// any is left, and then rows while their keys and values add up to no
    /// more than `max_bytes`. A primary sends them to a learner as a
    /// snapshot of the shard (§6.7, [`Index::install_rows`](crate::Index::install_rows)).
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading fails.
    pub fn shard_rows(
        &self,
        table: ShardTable,
        shard: &ShardRef,
        after: Option<&[u8]>,
        max_bytes: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, IndexError> {
        let prefix = codec::shard_key(shard);
        let end = prefix_end(&prefix);
        let table = match table {
            ShardTable::Namespace => &self.namespace,
            ShardTable::Uploads => &self.uploads,
            ShardTable::Parts => &self.parts,
        };
        let (mut rows, mut bytes) = (Vec::new(), 0);
        for row in table.range(prefix.as_slice()..end.as_slice())? {
            let (key, value) = row?;
            let (key, value) = (key.value(), value.value());
            if after.is_some_and(|after| key <= after) {
                continue;
            }
            bytes += key.len() + value.len();
            if !rows.is_empty() && bytes > max_bytes {
                break;
            }
            rows.push((key.to_vec(), value.to_vec()));
        }
        Ok(rows)
    }

    /// Returns everything the index holds except its bookkeeping.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading or decoding fails.
    pub fn dump(&self) -> Result<IndexDump, IndexError> {
        let control = self.control_entries()?;
        Ok(IndexDump {
            entries: read_all(
                &self.namespace,
                "namespace",
                codec::decode_entry_key,
                codec::decode_entry,
            )?,
            locations: read_all(
                &self.locations,
                "locations",
                codec::decode_location_key,
                codec::decode_location,
            )?,
            applied: self.applied_positions()?,
            uploads: read_all(
                &self.uploads,
                "uploads",
                codec::decode_upload_key,
                codec::decode_upload,
            )?,
            parts: read_all(
                &self.parts,
                "parts",
                codec::decode_part_key,
                codec::decode_part,
            )?,
            control,
            control_generation: self.control_generation()?,
        })
    }
}
