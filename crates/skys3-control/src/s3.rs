//! The S3 control-store backend (design §6.1): registers are objects in a
//! control bucket, under the cluster's prefix.

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use skys3_remote::{
    DeleteObject, GetObject, ListObjectsV2, ObjectStore, PutObject, S3Error, S3ErrorKind,
    WritePrecondition,
};
use skys3_types::{ETag, Generation};

use crate::feed::ChangeFeed;
use crate::key::{KeyError, KeyPrefix, RegisterKey};
use crate::store::{
    Change, ChangeStream, ControlError, ControlStore, DeleteOutcome, Expected, PutOutcome, Version,
    Versioned,
};

/// Where an [`S3ControlStore`] keeps its registers, and how often its
/// change streams poll.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3StoreConfig {
    /// The key prefix of every register in the control bucket:
    /// `[control_store] prefix`, empty or ending in `/`.
    pub prefix: String,
    /// How often a change stream reads `cluster.json`:
    /// `config_poll_interval`.
    pub poll_interval: Duration,
}

/// A control store held in one bucket of an S3-compatible object store:
/// registers are objects under the cluster's prefix.
///
/// Clones share the object store. Like every backend, it answers each
/// call with one request and leaves retries to
/// [`propose`](crate::propose).
///
/// | Operation | S3 request |
/// |---|---|
/// | `get` | `GetObject` |
/// | `put_if` | `PutObject` with `If-None-Match: *` or `If-Match: <etag>` |
/// | `delete_if` | `DeleteObject` with `If-Match: <etag>` |
/// | `list` | `ListObjectsV2` under the prefix, page by page |
/// | `changes` | `GetObject` of `cluster.json` with `If-None-Match: <etag>` every poll interval |
///
/// A register's version is its object's ETag. Answers map to the
/// [`ControlStore`] contract:
///
/// - `412 Precondition Failed`, and the `404 NoSuchKey` S3 answers to an
///   `If-Match` on a key without a current object, mean another writer
///   won: [`PutOutcome::PreconditionFailed`] or
///   [`DeleteOutcome::PreconditionFailed`].
/// - `409 ConditionalRequestConflict` applied nothing:
///   [`ControlError::Conflict`], which [`propose`](crate::propose) sends
///   again.
/// - A lost response, a timeout, or a `500` may hide an applied write:
///   [`ControlError::Indeterminate`], resolved by the lost-response rule.
/// - `503 SlowDown` and other transient errors applied nothing:
///   [`ControlError::Unavailable`].
/// - Anything else, such as `403 AccessDenied`, a missing bucket, or `501
///   NotImplemented` for a header the store does not support:
///   [`ControlError::Rejected`].
///
/// The backend trusts the store to honor its preconditions and to read its
/// own writes. [`ControlProbe`](crate::ControlProbe) checks both before a
/// node uses the store, and refuses a store that fails.
///
/// # Scope
///
/// Every request names a key under the configured prefix, and nothing else:
/// object keys are the prefix followed by a [`RegisterKey`], whose grammar
/// has no way out of it, and listings ask only for keys under it. Limiting
/// the control-store credential to that prefix is the operator's part
/// (design §6.1).
pub struct S3ControlStore<O> {
    shared: Arc<Shared<O>>,
}

struct Shared<O> {
    objects: O,
    config: S3StoreConfig,
}

impl<O> Clone for S3ControlStore<O> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<O: fmt::Debug> fmt::Debug for S3ControlStore<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3ControlStore")
            .field("objects", &self.shared.objects)
            .field("config", &self.shared.config)
            .finish()
    }
}

/// What a conditional read of a register found.
enum Polled {
    /// The register still has the version the reader holds.
    Unchanged,
    /// The register's current value, or `None` if it no longer exists.
    Changed(Option<Versioned>),
}

impl<O: ObjectStore> S3ControlStore<O> {
    /// The longest prefix, in bytes: S3's object-key limit less the
    /// longest register key, so that every register fits. Configuration
    /// validation applies the same bound to `[control_store] prefix`.
    pub const MAX_PREFIX_LEN: usize = skys3_remote::MAX_KEY_LEN - RegisterKey::MAX_LEN;

    /// A store whose registers are the objects of `objects` under
    /// `config.prefix`.
    ///
    /// # Errors
    ///
    /// A [`KeyError`] if the prefix is not empty and does not end in `/`,
    /// or is so long that a register key could exceed S3's 1,024-byte key
    /// limit.
    pub fn new(objects: O, config: S3StoreConfig) -> Result<Self, KeyError> {
        let prefix = &config.prefix;
        let reason = if !prefix.is_empty() && !prefix.ends_with('/') {
            Some("an S3 control-store prefix must be empty or end with '/'")
        } else if prefix.len() > Self::MAX_PREFIX_LEN {
            Some("an S3 control-store prefix is at most 512 bytes")
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(KeyError {
                key: prefix.clone(),
                reason,
            });
        }
        Ok(Self {
            shared: Arc::new(Shared { objects, config }),
        })
    }

    /// The object store.
    #[must_use]
    pub fn objects(&self) -> &O {
        &self.shared.objects
    }

    /// The configuration.
    #[must_use]
    pub fn config(&self) -> &S3StoreConfig {
        &self.shared.config
    }

    /// The object key of a register.
    #[must_use]
    pub fn object_key(&self, key: &RegisterKey) -> String {
        format!("{}{key}", self.shared.config.prefix)
    }

    /// Reads a register unless it is still at `version`: a `GetObject`
    /// with `If-None-Match`, which S3 answers with `304 Not Modified`
    /// while nothing changed.
    async fn get_if_changed(
        &self,
        key: &RegisterKey,
        version: &Version,
    ) -> Result<Polled, ControlError> {
        let Ok(etag) = ETag::new(version.as_str()) else {
            return self.get(key).await.map(Polled::Changed);
        };
        let request = GetObject::new(self.object_key(key)).with_if_none_match(etag);
        match self.shared.objects.get_object(request).await {
            Ok(output) => Ok(Polled::Changed(Some(Versioned {
                value: output.body,
                version: version_of(&output.info.etag),
            }))),
            Err(error) if error.kind() == S3ErrorKind::NotModified => Ok(Polled::Unchanged),
            Err(error) if error.kind() == S3ErrorKind::NoSuchKey => Ok(Polled::Changed(None)),
            Err(error) => Err(control_error(None, error)),
        }
    }
}

/// The version of an object with `etag`.
fn version_of(etag: &ETag) -> Version {
    Version::new(etag.as_str())
}

/// The `If-Match` tag for a version, or `None` for a version no object can
/// have, whose precondition fails without a request.
fn if_match(version: &Version) -> Option<ETag> {
    ETag::new(version.as_str()).ok()
}

/// Maps an error that is not a precondition failure. `key` is the
/// register a conditional write was for; a `409` to anything else is
/// treated as a transient error.
fn control_error(key: Option<&RegisterKey>, error: S3Error) -> ControlError {
    let conflict = error.kind() == S3ErrorKind::ConditionalRequestConflict;
    if let (true, Some(key)) = (conflict, key) {
        ControlError::Conflict(key.clone())
    } else if error.may_have_applied() {
        ControlError::Indeterminate(error.to_string())
    } else if error.is_transient() || conflict {
        ControlError::Unavailable(error.to_string())
    } else {
        ControlError::Rejected(error.to_string())
    }
}

/// Whether a conditional write's error means its precondition failed: a
/// `412`, or the `404` S3 answers to `If-Match` on a key without a current
/// object.
fn precondition_failed(error: &S3Error, if_match: bool) -> bool {
    match error.kind() {
        S3ErrorKind::PreconditionFailed => true,
        S3ErrorKind::NoSuchKey => if_match,
        _ => false,
    }
}

impl<O: ObjectStore> ControlStore for S3ControlStore<O> {
    type Changes = S3Changes<O>;

    async fn get(&self, key: &RegisterKey) -> Result<Option<Versioned>, ControlError> {
        let request = GetObject::new(self.object_key(key));
        match self.shared.objects.get_object(request).await {
            Ok(output) => Ok(Some(Versioned {
                value: output.body,
                version: version_of(&output.info.etag),
            })),
            Err(error) if error.kind() == S3ErrorKind::NoSuchKey => Ok(None),
            Err(error) => Err(control_error(None, error)),
        }
    }

    async fn put_if(
        &self,
        key: &RegisterKey,
        expected: Expected,
        value: Bytes,
    ) -> Result<PutOutcome, ControlError> {
        let precondition = match &expected {
            Expected::Absent => WritePrecondition::IfAbsent,
            Expected::Version(version) => match if_match(version) {
                Some(etag) => WritePrecondition::IfMatch(etag),
                None => return Ok(PutOutcome::PreconditionFailed),
            },
        };
        let is_if_match = matches!(precondition, WritePrecondition::IfMatch(_));
        let request = PutObject::new(self.object_key(key), value)
            .with_content_type("application/json")
            .with_precondition(precondition);
        match self.shared.objects.put_object(request).await {
            Ok(output) => Ok(PutOutcome::Written(version_of(&output.etag))),
            Err(error) if precondition_failed(&error, is_if_match) => {
                Ok(PutOutcome::PreconditionFailed)
            }
            Err(error) => Err(control_error(Some(key), error)),
        }
    }

    async fn delete_if(
        &self,
        key: &RegisterKey,
        expected: &Version,
    ) -> Result<DeleteOutcome, ControlError> {
        let Some(etag) = if_match(expected) else {
            return Ok(DeleteOutcome::PreconditionFailed);
        };
        let request = DeleteObject::new(self.object_key(key)).with_if_match(etag);
        match self.shared.objects.delete_object(request).await {
            Ok(_) => Ok(DeleteOutcome::Deleted),
            Err(error) if precondition_failed(&error, true) => {
                Ok(DeleteOutcome::PreconditionFailed)
            }
            Err(error) => Err(control_error(Some(key), error)),
        }
    }

    async fn list(&self, prefix: &KeyPrefix) -> Result<Vec<(RegisterKey, Version)>, ControlError> {
        let root = &self.shared.config.prefix;
        let mut request = ListObjectsV2::new(format!("{root}{prefix}"));
        let mut registers = Vec::new();
        loop {
            let page = self
                .shared
                .objects
                .list_objects_v2(request.clone())
                .await
                .map_err(|error| control_error(None, error))?;
            for object in page.objects {
                let Some(key) = object.key.strip_prefix(root.as_str()) else {
                    return Err(ControlError::Unavailable(format!(
                        "the listing of {root}{prefix} returned {:?}",
                        object.key
                    )));
                };
                registers.push((RegisterKey::new(key)?, version_of(&object.etag)));
            }
            match page.next_continuation_token {
                Some(token) if page.is_truncated => request.continuation_token = Some(token),
                None if page.is_truncated => {
                    return Err(ControlError::Unavailable(
                        "a truncated listing had no continuation token".into(),
                    ));
                }
                _ => break,
            }
        }
        // S3 lists in UTF-8 byte order, which is the key order; sorting
        // keeps the contract with a store that does not.
        registers.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(registers)
    }

    async fn changes(&self, after: Generation) -> Result<Self::Changes, ControlError> {
        let polled = PolledStore {
            store: self.clone(),
            cluster: Arc::new(Mutex::new(None)),
        };
        let interval = self.shared.config.poll_interval;
        Ok(S3Changes(ChangeFeed::polling(polled, after, interval)))
    }
}

/// The change stream of an [`S3ControlStore`]: a polling [`ChangeFeed`]
/// that reads `cluster.json` with `If-None-Match: <etag>` every
/// `poll_interval`, so a poll that finds nothing new costs a `304 Not
/// Modified` (design §6.2).
#[derive(Debug)]
pub struct S3Changes<O: ObjectStore>(ChangeFeed<PolledStore<O>>);

impl<O: ObjectStore> ChangeStream for S3Changes<O> {
    async fn next(&mut self) -> Result<Change, ControlError> {
        self.0.next().await
    }
}

/// The store a change feed reads through: `cluster.json` conditionally on
/// the copy it last read, everything else as the store does.
#[derive(Debug)]
struct PolledStore<O> {
    store: S3ControlStore<O>,
    /// `cluster.json` as last read.
    cluster: Arc<Mutex<Option<Versioned>>>,
}

impl<O> Clone for PolledStore<O> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            cluster: Arc::clone(&self.cluster),
        }
    }
}

impl<O: ObjectStore> PolledStore<O> {
    fn cached(&self) -> Option<Versioned> {
        self.cluster
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn remember(&self, cluster: Option<Versioned>) {
        *self.cluster.lock().unwrap_or_else(PoisonError::into_inner) = cluster;
    }
}

impl<O: ObjectStore> ControlStore for PolledStore<O> {
    type Changes = S3Changes<O>;

    async fn get(&self, key: &RegisterKey) -> Result<Option<Versioned>, ControlError> {
        if *key != RegisterKey::cluster() {
            return self.store.get(key).await;
        }
        let current = match self.cached() {
            Some(cached) => match self.store.get_if_changed(key, &cached.version).await? {
                Polled::Unchanged => return Ok(Some(cached)),
                Polled::Changed(current) => current,
            },
            None => self.store.get(key).await?,
        };
        self.remember(current.clone());
        Ok(current)
    }

    async fn put_if(
        &self,
        key: &RegisterKey,
        expected: Expected,
        value: Bytes,
    ) -> Result<PutOutcome, ControlError> {
        self.store.put_if(key, expected, value).await
    }

    async fn delete_if(
        &self,
        key: &RegisterKey,
        expected: &Version,
    ) -> Result<DeleteOutcome, ControlError> {
        self.store.delete_if(key, expected).await
    }

    async fn list(&self, prefix: &KeyPrefix) -> Result<Vec<(RegisterKey, Version)>, ControlError> {
        self.store.list(prefix).await
    }

    async fn changes(&self, after: Generation) -> Result<Self::Changes, ControlError> {
        self.store.changes(after).await
    }
}

#[cfg(test)]
mod tests {
    use skys3_sim::SimS3;
    use skys3_sim::s3::SimS3Config;

    use super::*;

    fn polled() -> PolledStore<SimS3> {
        let config = S3StoreConfig {
            prefix: "c/".to_owned(),
            poll_interval: Duration::from_secs(1),
        };
        let store = S3ControlStore::new(SimS3::new(0, SimS3Config::default()), config).unwrap();
        PolledStore {
            store,
            cluster: Arc::new(Mutex::new(None)),
        }
    }

    #[tokio::test]
    async fn the_poll_view_passes_everything_but_cluster_json_through() {
        let polled = polled();
        let key = RegisterKey::new("nodes/node-1.json").unwrap();
        let value = Bytes::from_static(br#"{"proposal_id":"p"}"#);
        let PutOutcome::Written(version) = polled
            .put_if(&key, Expected::Absent, value.clone())
            .await
            .unwrap()
        else {
            panic!("the create failed");
        };
        let read = polled.get(&key).await.unwrap().unwrap();
        assert_eq!((read.value, &read.version), (value, &version));
        let listed = polled.list(&KeyPrefix::nodes()).await.unwrap();
        assert_eq!(listed, [(key.clone(), version.clone())]);
        let deleted = polled.delete_if(&key, &version).await.unwrap();
        assert_eq!(deleted, DeleteOutcome::Deleted);
        assert!(polled.changes(Generation::ZERO).await.is_ok());
        assert!(polled.cached().is_none(), "only cluster.json is kept");
    }

    #[tokio::test]
    async fn a_cached_version_that_is_not_an_etag_is_read_again() {
        let polled = polled();
        let cluster = RegisterKey::cluster();
        let value = Bytes::from_static(br#"{"proposal_id":"p"}"#);
        let written = polled.put_if(&cluster, Expected::Absent, value).await;
        assert!(matches!(written.unwrap(), PutOutcome::Written(_)));
        let current = polled.store.get(&cluster).await.unwrap();
        polled.remember(Some(Versioned {
            value: Bytes::new(),
            version: Version::new("not an etag"),
        }));
        assert_eq!(polled.get(&cluster).await.unwrap(), current);
        assert_eq!(polled.cached(), current);
    }
}
