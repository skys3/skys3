//! STS session records in the node's system bucket (design §11).
//!
//! Session records live in an internal, local-only bucket that is never
//! flushed and never listed: its document is fixed, it has no register in
//! the control store, and its ID, `sys-sessions`, cannot collide with a
//! generated bucket ID (which starts with `b-`). It has one shard. Each
//! record is a `PUT` keyed by access key ID, whose body is the session's
//! JSON and whose metadata carries its expiry, so expired sessions are
//! found from the index alone. Records therefore survive restarts like any
//! object, and replicate with the shard once replication exists (plan M2).

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use skys3_gateway::{LocalShards, Precondition, ShardRef, Shards};
use skys3_io::{BlockingPool, Disk};
use skys3_log::RecordBody;
use skys3_log::record::{Delete, Extent, Flushed, Put, PutData};
use skys3_sts::{Session, SessionStore, SessionStoreError};
use skys3_types::{BucketDocument, BucketId, BucketMode, BucketName, ETag, ProposalId, ShardCount};

/// The ID of the system bucket that holds sessions.
pub const SESSIONS_BUCKET_ID: &str = "sys-sessions";

/// The metadata key that holds a record's expiry, in seconds since the
/// Unix epoch.
const EXPIRES_AT: &str = "expires-at";

/// The ETag of every record. Records are never served over S3 nor
/// flushed, so it carries no meaning.
const RECORD_ETAG: &str = "skys3-session";

/// How many entries an expiry sweep reads at a time.
const SWEEP_PAGE: usize = 1024;

/// The fixed document of the sessions bucket.
#[must_use]
pub fn sessions_bucket() -> BucketDocument {
    BucketDocument {
        bucket_id: BucketId::new(SESSIONS_BUCKET_ID).expect("the ID is valid"),
        name: BucketName::new("skys3-system-sessions").expect("the name is valid"),
        mode: BucketMode::Local,
        shards: ShardCount::new(1).expect("one shard is valid"),
        replicas: 1,
        min_write_replicas: 1,
        clean_copies: 0,
        target: None,
        created_unix_ms: 0,
        proposal_id: ProposalId::from_u128(0),
    }
}

/// Session records in the system bucket. Clones share the shard.
pub struct SystemSessions<D: Disk> {
    shards: LocalShards<D>,
    shard: ShardRef,
    pool: BlockingPool,
    inline_max_bytes: u64,
}

impl<D: Disk> Clone for SystemSessions<D> {
    fn clone(&self) -> Self {
        Self {
            shards: self.shards.clone(),
            shard: self.shard.clone(),
            pool: self.pool.clone(),
            inline_max_bytes: self.inline_max_bytes,
        }
    }
}

impl<D: Disk> fmt::Debug for SystemSessions<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SystemSessions")
            .field("shard", &self.shard)
            .finish_non_exhaustive()
    }
}

fn failed(error: impl fmt::Display) -> SessionStoreError {
    SessionStoreError(error.to_string())
}

impl<D: Disk> SystemSessions<D> {
    /// Opens the sessions bucket's shard among `shards`. Index reads run on
    /// `pool`; a record larger than `inline_max_bytes` goes in an extent.
    ///
    /// # Errors
    ///
    /// If the shard cannot be opened.
    pub async fn open(
        shards: LocalShards<D>,
        pool: BlockingPool,
        inline_max_bytes: u64,
    ) -> Result<Self, SessionStoreError> {
        let bucket = sessions_bucket();
        let shard = ShardRef::all(&bucket)
            .next()
            .expect("the bucket has a shard");
        shards.open(&shard, &bucket).await.map_err(failed)?;
        Ok(Self {
            shards,
            shard,
            pool,
            inline_max_bytes,
        })
    }

    async fn commit(&self, body: RecordBody) -> Result<u64, SessionStoreError> {
        match self
            .shards
            .write(&self.shard, body, Precondition::None)
            .await
            .map_err(failed)?
        {
            Ok(position) => Ok(position.seq.get()),
            Err(failed) => unreachable!("an unconditional write failed its condition: {failed}"),
        }
    }

    /// The keys of the records whose expiry is at or before `now`.
    async fn expired(&self, now: u64) -> Result<Vec<String>, SessionStoreError> {
        let local = self
            .shards
            .set()
            .get(&(&self.shard).into())
            .await
            .ok_or_else(|| failed("the sessions shard is not open"))?;
        let (index, shard) = (Arc::clone(local.index()), (&self.shard).into());
        self.pool
            .run(move || {
                let reader = index.read()?;
                let mut expired = Vec::new();
                let mut after: Option<String> = None;
                loop {
                    let page = reader.entries(&shard, after.as_deref(), SWEEP_PAGE)?;
                    let Some((last, _)) = page.last() else {
                        return Ok(expired);
                    };
                    after = Some(last.clone());
                    for (key, entry) in page {
                        let expires_at = entry
                            .object
                            .as_ref()
                            .and_then(|object| object.metadata.get(EXPIRES_AT))
                            .and_then(|value| value.parse::<u64>().ok());
                        if expires_at.is_some_and(|expires_at| expires_at <= now) {
                            expired.push(key);
                        }
                    }
                }
            })
            .await
            .map_err(failed)?
            .map_err(|error: skys3_index::IndexError| failed(error))
    }
}

impl<D: Disk> SessionStore for SystemSessions<D> {
    async fn insert(&self, session: Session) -> Result<(), SessionStoreError> {
        let json = Bytes::from(serde_json::to_vec(&session).map_err(failed)?);
        let size = json.len() as u64;
        let data = if size <= self.inline_max_bytes {
            PutData::Inline(json)
        } else {
            let extent = Extent {
                key: session.access_key_id.clone(),
                offset: 0,
                data: json,
            };
            let reference = self
                .shards
                .append_extent(&self.shard, extent)
                .await
                .map_err(failed)?;
            PutData::Extents(vec![reference])
        };
        let put = Put {
            key: session.access_key_id.clone(),
            size,
            last_modified_ms: session.issued_at.saturating_mul(1000),
            etag: ETag::new(RECORD_ETAG).expect("the ETag is valid"),
            inherited_identity: None,
            metadata: BTreeMap::from([(EXPIRES_AT.to_owned(), session.expires_at.to_string())]),
            tags: BTreeMap::new(),
            checksums: BTreeMap::new(),
            copy_source: None,
            data,
        };
        self.commit(RecordBody::Put(put)).await.map(drop)
    }

    async fn get(&self, access_key_id: &str) -> Result<Option<Session>, SessionStoreError> {
        let entry = self
            .shards
            .entry(&self.shard, access_key_id)
            .await
            .map_err(failed)?;
        let Some(object) = entry.and_then(|entry| entry.object) else {
            return Ok(None);
        };
        let positions = match object.payload {
            skys3_index::Payload::Inline(position) => vec![position],
            skys3_index::Payload::Extents(extents) => {
                extents.iter().map(|extent| extent.position).collect()
            }
            skys3_index::Payload::None => return Err(failed("a session record has no bytes")),
        };
        let mut json = Vec::new();
        for position in positions {
            let payload = self
                .shards
                .payload(&self.shard, position)
                .await
                .map_err(failed)?;
            json.extend_from_slice(&payload);
        }
        serde_json::from_slice(&json).map(Some).map_err(failed)
    }

    async fn remove_expired(&self, now: Duration) -> Result<usize, SessionStoreError> {
        let expired = self.expired(now.as_secs()).await?;
        for key in &expired {
            let seq = self
                .commit(RecordBody::Delete(Delete { key: key.clone() }))
                .await?;
            // The bucket is never flushed: a FLUSHED of the tombstone
            // removes the entry, unless the key was written again.
            let flushed = Flushed {
                key: key.clone(),
                seq: skys3_types::Seq::new(seq),
                remote_etag: None,
                remote_version_id: None,
            };
            self.commit(RecordBody::Flushed(flushed)).await?;
        }
        Ok(expired.len())
    }
}

#[cfg(test)]
mod tests {
    use skys3_gateway::stub::MemoryShards;

    use super::*;

    /// A session record, built from its JSON form.
    fn session(access_key_id: &str, expires_at: u64) -> Session {
        let zeros = |bytes: usize| "A".repeat((bytes * 4).div_ceil(3));
        serde_json::from_value(serde_json::json!({
            "access_key_id": access_key_id,
            "token_hash": zeros(32),
            "sealed_secret": zeros(30),
            "role": "reader",
            "session_name": "ci",
            "issuer": "https://issuer.example",
            "subject": "repo:x",
            "issued_at": 100,
            "expires_at": expires_at,
        }))
        .unwrap()
    }

    async fn sessions(inline_max_bytes: u64) -> (MemoryShards, SystemSessions<skys3_io::SimMount>) {
        let shards = MemoryShards::new().await;
        let pool = BlockingPool::new("sessions", std::num::NonZeroUsize::MIN).unwrap();
        let store = SystemSessions::open(shards.local().clone(), pool, inline_max_bytes)
            .await
            .unwrap();
        (shards, store)
    }

    #[tokio::test]
    async fn sessions_are_stored_read_and_expired() {
        for inline_max_bytes in [0, 128 << 10] {
            let (_shards, store) = sessions(inline_max_bytes).await;
            let first = session("ASIAFIRST", 1_000);
            let second = session("ASIASECOND", 2_000);
            store.insert(first.clone()).await.unwrap();
            store.insert(second.clone()).await.unwrap();
            assert_eq!(store.get("ASIAFIRST").await.unwrap(), Some(first));
            assert_eq!(store.get("ASIAMISSING").await.unwrap(), None);
            assert!(format!("{store:?}").contains("sys-sessions"));

            assert_eq!(
                store
                    .remove_expired(Duration::from_secs(999))
                    .await
                    .unwrap(),
                0
            );
            assert_eq!(
                store
                    .clone()
                    .remove_expired(Duration::from_secs(1_000))
                    .await
                    .unwrap(),
                1
            );
            assert_eq!(store.get("ASIAFIRST").await.unwrap(), None);
            assert_eq!(store.get("ASIASECOND").await.unwrap(), Some(second));
            // The tombstone is gone too.
            assert_eq!(
                store
                    .remove_expired(Duration::from_secs(1_000))
                    .await
                    .unwrap(),
                0
            );
        }
    }

    #[tokio::test]
    async fn sessions_survive_a_restart() {
        let (shards, store) = sessions(128 << 10).await;
        store.insert(session("ASIAKEPT", 5_000)).await.unwrap();
        drop(store);
        let disk = shards.disk().clone();
        disk.crash();
        let reopened = MemoryShards::open(disk).await.unwrap();
        let pool = BlockingPool::new("sessions", std::num::NonZeroUsize::MIN).unwrap();
        let store = SystemSessions::open(reopened.local().clone(), pool, 128 << 10)
            .await
            .unwrap();
        assert!(store.get("ASIAKEPT").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn failures_are_reported() {
        let (shards, store) = sessions(128 << 10).await;
        shards.local().remove(&store.shard).await.unwrap();
        assert!(store.remove_expired(Duration::ZERO).await.is_err());
        assert!(store.get("ASIAX").await.is_err());
        assert!(store.insert(session("ASIAX", 1)).await.is_err());
    }
}
