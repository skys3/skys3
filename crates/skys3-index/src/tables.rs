//! The index's redb tables and typed access to them.
//!
//! The [`codec`](crate::codec) module documents each table's key and value
//! encoding.

use std::collections::BTreeMap;
use std::time::Duration;

use redb::{ReadOnlyTable, ReadableTable, Table, TableDefinition};
use skys3_log::RecordLocation;
use skys3_log::record::ShardRef;
use skys3_types::{EpochSeq, Generation};

use crate::codec;
use crate::entry::{ControlEntry, Entry};
use crate::error::IndexError;
use crate::listing::{self, ListPage, ListQuery};

type Bytes = &'static [u8];

/// Each shard's namespace index, keyed by shard and object key.
pub(crate) const NAMESPACE: TableDefinition<Bytes, Bytes> = TableDefinition::new("namespace");
/// The node-local location map, keyed by shard and record position.
pub(crate) const LOCATIONS: TableDefinition<Bytes, Bytes> = TableDefinition::new("locations");
/// Each shard's applied position.
pub(crate) const SHARDS: TableDefinition<Bytes, Bytes> = TableDefinition::new("shards");
/// What each log segment holds, as of the durable checkpoint.
pub(crate) const COVERAGE: TableDefinition<Bytes, Bytes> = TableDefinition::new("coverage");
/// The node's local copy of control state, keyed by register key.
pub(crate) const CONTROL: TableDefinition<&str, Bytes> = TableDefinition::new("control");
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

/// Write access to the tables that applying records changes, inside one
/// write transaction (see [`Applier`](crate::Applier)).
///
/// Changes become visible to readers when the transaction commits, and
/// durable at the next checkpoint.
pub struct IndexWriter<'txn> {
    namespace: Table<'txn, Bytes, Bytes>,
    locations: Table<'txn, Bytes, Bytes>,
    shards: Table<'txn, Bytes, Bytes>,
}

impl<'txn> IndexWriter<'txn> {
    pub(crate) fn open(txn: &'txn redb::WriteTransaction) -> Result<Self, IndexError> {
        Ok(Self {
            namespace: txn.open_table(NAMESPACE)?,
            locations: txn.open_table(LOCATIONS)?,
            shards: txn.open_table(SHARDS)?,
        })
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
    /// record locations, and its applied position.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if the write fails.
    pub fn remove_shard(&mut self, shard: &ShardRef) -> Result<(), IndexError> {
        let prefix = codec::shard_key(shard);
        let end = prefix_end(&prefix);
        for table in [&mut self.namespace, &mut self.locations] {
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
    control: ReadOnlyTable<&'static str, Bytes>,
    meta: ReadOnlyTable<&'static str, u64>,
}

impl IndexReader {
    pub(crate) fn open(txn: &redb::ReadTransaction) -> Result<Self, IndexError> {
        Ok(Self {
            namespace: txn.open_table(NAMESPACE)?,
            locations: txn.open_table(LOCATIONS)?,
            shards: txn.open_table(SHARDS)?,
            control: txn.open_table(CONTROL)?,
            meta: txn.open_table(META)?,
        })
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
            control,
            control_generation: self.control_generation()?,
        })
    }
}
