//! The etcd control-store backend (design §6.1): registers are etcd keys
//! under the cluster's prefix, written by transactions that compare the
//! key's revision.
//!
//! The client is the small part of etcd's v3 gRPC API the backend needs
//! (`KV.Range`, `KV.Txn`, and `Watch.Watch`), on `hyper`'s HTTP/2 client,
//! `prost`, and `rustls` with `aws-lc-rs`, which the workspace already
//! uses. The `etcd-client` crate was not used: its build script compiles
//! etcd's `.proto` files and needs `protoc` on every machine that builds
//! the workspace, and it brings about 25 more crates, among them a second
//! `base64`, `hashbrown`, and `foldhash`.

#[cfg(test)]
mod fake;
#[cfg(test)]
mod fake_tests;
pub(crate) mod grpc;
pub(crate) mod proto;

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use skys3_types::Generation;
use tokio::sync::watch;

use self::grpc::{CallError, Channel, Code, Endpoint, Streaming};
use self::proto::{
    Compare, CompareResult, CompareTarget, DeleteRangeRequest, PutRequest, RangeRequest,
    RangeResponse, Request, RequestOp, TargetUnion, TxnRequest, TxnResponse, WatchCreateRequest,
    WatchRequest, WatchRequestUnion, WatchResponse,
};
use crate::feed::ChangeFeed;
use crate::key::{KeyPrefix, RegisterKey};
use crate::store::{
    ControlError, ControlStore, DeleteOutcome, Expected, PutOutcome, Version, Versioned,
};

/// Where an [`EtcdControlStore`] keeps its registers, and how it reaches
/// etcd.
#[derive(Clone)]
pub struct EtcdStoreConfig {
    /// The cluster's client URLs, `http://` or `https://`, without a path:
    /// `[control_store] etcd_endpoints`.
    pub endpoints: Vec<String>,
    /// The key prefix of every register: `[control_store] prefix`, visible
    /// ASCII ending in `/`.
    pub prefix: String,
    /// The TLS configuration for `https://` endpoints: the CA certificates
    /// that verify etcd's, and the client certificate if etcd asks for one
    /// (`--client-cert-auth`). The store sets the ALPN protocol to `h2`.
    pub tls: Option<Arc<rustls::ClientConfig>>,
    /// How long a connection attempt, and each request, may take before it
    /// is given up: [`EtcdStoreConfig::DEFAULT_REQUEST_TIMEOUT`] unless set.
    pub request_timeout: Duration,
}

impl EtcdStoreConfig {
    /// The default for [`EtcdStoreConfig::request_timeout`]. etcd itself
    /// gives up on a write its cluster has not committed after about
    /// 5 seconds, with its default election timeout.
    pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(7);

    /// A configuration for `endpoints` and `prefix`, without TLS, with the
    /// default request timeout.
    #[must_use]
    pub fn new(endpoints: Vec<String>, prefix: impl Into<String>) -> Self {
        Self {
            endpoints,
            prefix: prefix.into(),
            tls: None,
            request_timeout: Self::DEFAULT_REQUEST_TIMEOUT,
        }
    }
}

impl fmt::Debug for EtcdStoreConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EtcdStoreConfig")
            .field("endpoints", &self.endpoints)
            .field("prefix", &self.prefix)
            .field("tls", &self.tls.is_some())
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}

/// An [`EtcdStoreConfig`] that cannot be used.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid etcd control-store configuration: {0}")]
pub struct EtcdConfigError(String);

/// A control store held in an etcd v3 cluster: registers are keys under
/// the cluster's prefix.
///
/// Clones share one HTTP/2 connection. Like every backend, it answers each
/// call with one request and leaves retries to
/// [`propose`](crate::propose).
///
/// | Operation | etcd request |
/// |---|---|
/// | `get` | `Range` of the key, linearizable |
/// | `put_if` | `Txn`: if the key's `create_revision` is 0 (absent) or its `mod_revision` is the expected version, `Put` |
/// | `delete_if` | `Txn`: if the key's `mod_revision` is the expected version, `DeleteRange` of the key |
/// | `list` | `Range` of the prefix, keys only, in pages of [`EtcdControlStore::LIST_PAGE`] keys at one revision |
/// | `changes` | A watch on `cluster.json` that wakes the shared [`ChangeFeed`] |
///
/// A register's version is its key's `mod_revision`, in decimal. etcd's
/// revisions grow with every write to the cluster, so every value a
/// register holds, across deletions and re-creations, has a version of its
/// own. Answers map to the [`ControlStore`] contract:
///
/// - A transaction whose comparison fails wrote nothing:
///   [`PutOutcome::PreconditionFailed`] or
///   [`DeleteOutcome::PreconditionFailed`].
/// - No connection to any endpoint: nothing was sent,
///   [`ControlError::Unavailable`].
/// - A timeout, a broken connection, a malformed answer, or the statuses
///   `UNAVAILABLE` (etcd's "request timed out", "leader changed", "no
///   leader"), `DEADLINE_EXCEEDED`, `CANCELLED`, `ABORTED`, `INTERNAL`,
///   and `UNKNOWN` may hide an applied write: [`ControlError::Indeterminate`]
///   for writes, resolved by the lost-response rule, and
///   [`ControlError::Unavailable`] for reads.
/// - `RESOURCE_EXHAUSTED` ("too many requests", "database space
///   exceeded") applied nothing: [`ControlError::Unavailable`].
/// - Anything else, such as `PERMISSION_DENIED`, `UNAUTHENTICATED`, or
///   `INVALID_ARGUMENT` ("request is too large"): [`ControlError::Rejected`].
///
/// Reads are linearizable, so etcd needs no startup probe, but it passes
/// [`ControlProbe`](crate::ControlProbe) like every backend.
#[derive(Clone)]
pub struct EtcdControlStore {
    shared: Arc<Shared>,
}

struct Shared {
    channel: Channel,
    prefix: String,
    request_timeout: Duration,
}

impl fmt::Debug for EtcdControlStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EtcdControlStore")
            .field("channel", &self.shared.channel)
            .field("prefix", &self.shared.prefix)
            .finish()
    }
}

/// How long a change stream waits before it opens its watch again after
/// etcd ended it or could not be reached.
const WATCH_RETRY_DELAY: Duration = Duration::from_secs(1);

impl EtcdControlStore {
    /// The most keys one page of a listing asks for.
    pub const LIST_PAGE: i64 = 1000;

    /// How often a listing starts over when the revision its first page
    /// read at is compacted away before its last page.
    const LIST_ATTEMPTS: u32 = 3;

    /// A store whose registers are the keys of the etcd cluster at
    /// `config.endpoints` under `config.prefix`. It connects at the first
    /// request.
    ///
    /// # Errors
    ///
    /// An [`EtcdConfigError`] if there is no endpoint, an endpoint is not
    /// an `http://` or `https://` URL without a path, an `https://`
    /// endpoint has no TLS configuration, or the prefix is empty, does not
    /// end in `/`, or is not visible ASCII.
    pub fn new(config: EtcdStoreConfig) -> Result<Self, EtcdConfigError> {
        if config.endpoints.is_empty() {
            return Err(EtcdConfigError("no endpoint".to_owned()));
        }
        let endpoints = config
            .endpoints
            .iter()
            .map(|url| Endpoint::parse(url))
            .collect::<Result<Vec<_>, _>>()
            .map_err(EtcdConfigError)?;
        if config.tls.is_none() && endpoints.iter().any(Endpoint::tls) {
            return Err(EtcdConfigError(
                "https:// endpoints need a TLS configuration".to_owned(),
            ));
        }
        let prefix = &config.prefix;
        if !prefix.ends_with('/') || !prefix.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(EtcdConfigError(format!(
                "the prefix {prefix:?} must be visible ASCII ending with '/'"
            )));
        }
        Ok(Self {
            shared: Arc::new(Shared {
                channel: Channel::new(endpoints, config.tls, config.request_timeout),
                prefix: config.prefix,
                request_timeout: config.request_timeout,
            }),
        })
    }

    /// The prefix of every register key.
    #[must_use]
    pub fn prefix(&self) -> &str {
        &self.shared.prefix
    }

    /// The etcd key of a register.
    #[must_use]
    pub fn etcd_key(&self, key: &RegisterKey) -> String {
        format!("{}{key}", self.shared.prefix)
    }

    async fn txn(&self, compare: Compare, success: Request) -> Result<TxnResponse, ControlError> {
        let request = TxnRequest {
            compare: vec![compare],
            success: vec![RequestOp {
                request: Some(success),
            }],
            failure: Vec::new(),
        };
        self.shared
            .channel
            .unary(proto::TXN, &request)
            .await
            .map_err(write_error)
    }

    /// Opens a watch on `cluster.json` and waits for etcd to confirm it,
    /// so that every later write to it is reported.
    async fn watch_cluster(&self) -> Result<Streaming, ControlError> {
        let request = WatchRequest {
            request_union: Some(WatchRequestUnion::Create(WatchCreateRequest {
                key: Bytes::from(self.etcd_key(&RegisterKey::cluster())),
                range_end: Bytes::new(),
                start_revision: 0,
            })),
        };
        let channel = &self.shared.channel;
        let mut stream = channel
            .open(proto::WATCH, &request)
            .await
            .map_err(read_error)?;
        let created = tokio::time::timeout(
            self.shared.request_timeout,
            stream.message::<WatchResponse>(),
        )
        .await
        .map_err(|_| ControlError::Unavailable("etcd did not confirm a watch".to_owned()))?
        .map_err(read_error)?;
        if !created.created || created.canceled {
            return Err(ControlError::Unavailable(format!(
                "etcd refused a watch: {}",
                created.cancel_reason
            )));
        }
        Ok(stream)
    }

    /// Wakes a change feed whenever `cluster.json` changes, until the feed
    /// is dropped.
    ///
    /// When etcd ends or cancels the watch, for example because the
    /// connection broke, its member stopped, or a slow watch fell behind a
    /// compaction, the watch is opened again from the current revision,
    /// and the feed is woken once it is: the feed then reads the
    /// generation again, which covers every write made while no watch was
    /// open.
    async fn follow(self, mut stream: Streaming, wake: watch::Sender<()>) {
        loop {
            loop {
                tokio::select! {
                    () = wake.closed() => return,
                    response = stream.message::<WatchResponse>() => match response {
                        Ok(response) if !response.canceled && response.compact_revision == 0 => {
                            if !response.events.is_empty() {
                                wake.send_replace(());
                            }
                        }
                        _ => break,
                    },
                }
            }
            stream = loop {
                tokio::select! {
                    () = wake.closed() => return,
                    () = tokio::time::sleep(WATCH_RETRY_DELAY) => {}
                }
                tokio::select! {
                    () = wake.closed() => return,
                    watched = self.watch_cluster() => if let Ok(stream) = watched {
                        break stream;
                    },
                }
            };
            wake.send_replace(());
        }
    }
}

/// The version of a value written at `revision`.
fn version_of(revision: i64) -> Version {
    Version::new(revision.to_string())
}

/// The revision a version names, or `None` for a version no key can have,
/// whose precondition fails without a request.
fn revision_of(version: &Version) -> Option<i64> {
    let revision: i64 = version.as_str().parse().ok()?;
    (revision > 0 && revision.to_string() == version.as_str()).then_some(revision)
}

/// The end of the key range that holds every key starting with `prefix`,
/// which is visible ASCII.
fn range_end(prefix: &str) -> Bytes {
    let mut end = prefix.as_bytes().to_vec();
    if let Some(last) = end.last_mut() {
        *last += 1;
    }
    Bytes::from(end)
}

/// Maps a failed read.
fn read_error(error: CallError) -> ControlError {
    match error {
        CallError::Status(status) if status.code.is_transient() => {
            ControlError::Unavailable(CallError::Status(status).to_string())
        }
        CallError::Status(_) => ControlError::Rejected(error.to_string()),
        CallError::NotSent(_) | CallError::NoAnswer(_) => {
            ControlError::Unavailable(error.to_string())
        }
    }
}

/// Maps a failed write.
fn write_error(error: CallError) -> ControlError {
    match &error {
        CallError::NoAnswer(_) => ControlError::Indeterminate(error.to_string()),
        CallError::Status(status) if status.code.may_have_applied() => {
            ControlError::Indeterminate(error.to_string())
        }
        CallError::NotSent(_) => ControlError::Unavailable(error.to_string()),
        CallError::Status(status) if status.code == Code::RESOURCE_EXHAUSTED => {
            ControlError::Unavailable(error.to_string())
        }
        CallError::Status(_) => ControlError::Rejected(error.to_string()),
    }
}

/// The revision a write was applied at, from its answer's header.
fn written_at(response: &TxnResponse) -> Result<i64, ControlError> {
    response
        .header
        .as_ref()
        .map(|header| header.revision)
        .filter(|revision| *revision > 0)
        .ok_or_else(|| {
            ControlError::Indeterminate("etcd answered a write without its revision".to_owned())
        })
}

impl ControlStore for EtcdControlStore {
    type Changes = ChangeFeed<Self>;

    async fn get(&self, key: &RegisterKey) -> Result<Option<Versioned>, ControlError> {
        let etcd_key = Bytes::from(self.etcd_key(key));
        let request = RangeRequest {
            key: etcd_key.clone(),
            ..RangeRequest::default()
        };
        let response: RangeResponse = self
            .shared
            .channel
            .unary(proto::RANGE, &request)
            .await
            .map_err(read_error)?;
        let Some(kv) = response.kvs.into_iter().next() else {
            return Ok(None);
        };
        if kv.key != etcd_key || kv.mod_revision <= 0 {
            return Err(ControlError::Unavailable(format!(
                "etcd answered a read of {key} with key {:?} at revision {}",
                String::from_utf8_lossy(&kv.key),
                kv.mod_revision
            )));
        }
        Ok(Some(Versioned {
            value: kv.value,
            version: version_of(kv.mod_revision),
        }))
    }

    async fn put_if(
        &self,
        key: &RegisterKey,
        expected: Expected,
        value: Bytes,
    ) -> Result<PutOutcome, ControlError> {
        let etcd_key = Bytes::from(self.etcd_key(key));
        let (target, target_union) = match &expected {
            Expected::Absent => (CompareTarget::Create, TargetUnion::CreateRevision(0)),
            Expected::Version(version) => match revision_of(version) {
                Some(revision) => (CompareTarget::Mod, TargetUnion::ModRevision(revision)),
                None => return Ok(PutOutcome::PreconditionFailed),
            },
        };
        let compare = Compare {
            result: CompareResult::Equal as i32,
            target: target as i32,
            key: etcd_key.clone(),
            target_union: Some(target_union),
        };
        let put = Request::Put(PutRequest {
            key: etcd_key,
            value,
        });
        let response = self.txn(compare, put).await?;
        if !response.succeeded {
            return Ok(PutOutcome::PreconditionFailed);
        }
        Ok(PutOutcome::Written(version_of(written_at(&response)?)))
    }

    async fn delete_if(
        &self,
        key: &RegisterKey,
        expected: &Version,
    ) -> Result<DeleteOutcome, ControlError> {
        let Some(revision) = revision_of(expected) else {
            return Ok(DeleteOutcome::PreconditionFailed);
        };
        let etcd_key = Bytes::from(self.etcd_key(key));
        let compare = Compare {
            result: CompareResult::Equal as i32,
            target: CompareTarget::Mod as i32,
            key: etcd_key.clone(),
            target_union: Some(TargetUnion::ModRevision(revision)),
        };
        let delete = Request::DeleteRange(DeleteRangeRequest { key: etcd_key });
        let response = self.txn(compare, delete).await?;
        Ok(if response.succeeded {
            DeleteOutcome::Deleted
        } else {
            DeleteOutcome::PreconditionFailed
        })
    }

    async fn list(&self, prefix: &KeyPrefix) -> Result<Vec<(RegisterKey, Version)>, ControlError> {
        let root = &self.shared.prefix;
        let start = format!("{root}{prefix}");
        let end = range_end(&start);
        let mut attempts = 0;
        'listing: loop {
            attempts += 1;
            let mut registers = Vec::new();
            let mut from = Bytes::from(start.clone());
            // Every page after the first reads at the first one's revision,
            // so the listing is one consistent view.
            let mut revision = 0;
            loop {
                let request = RangeRequest {
                    key: from,
                    range_end: end.clone(),
                    limit: Self::LIST_PAGE,
                    revision,
                    keys_only: true,
                    ..RangeRequest::default()
                };
                let page: RangeResponse =
                    match self.shared.channel.unary(proto::RANGE, &request).await {
                        Ok(page) => page,
                        Err(CallError::Status(status))
                            if status.code == Code::OUT_OF_RANGE
                                && revision != 0
                                && attempts < Self::LIST_ATTEMPTS =>
                        {
                            continue 'listing;
                        }
                        Err(error) => return Err(read_error(error)),
                    };
                if revision == 0 {
                    revision = page.header.as_ref().map_or(0, |header| header.revision);
                }
                let Some(last) = page.kvs.last().map(|kv| kv.key.clone()) else {
                    break;
                };
                for kv in page.kvs {
                    let key = std::str::from_utf8(&kv.key)
                        .ok()
                        .and_then(|key| key.strip_prefix(root.as_str()))
                        .ok_or_else(|| {
                            ControlError::Unavailable(format!(
                                "the listing of {start} returned {:?}",
                                String::from_utf8_lossy(&kv.key)
                            ))
                        })?;
                    registers.push((RegisterKey::new(key)?, version_of(kv.mod_revision)));
                }
                if !page.more {
                    break;
                }
                let mut next = last.to_vec();
                next.push(0);
                from = Bytes::from(next);
            }
            // etcd lists in byte order, which is the key order; sorting
            // keeps the contract regardless.
            registers.sort_by(|a, b| a.0.cmp(&b.0));
            return Ok(registers);
        }
    }

    async fn changes(&self, after: Generation) -> Result<Self::Changes, ControlError> {
        let stream = self.watch_cluster().await?;
        let (wake, woken) = watch::channel(());
        tokio::spawn(self.clone().follow(stream, wake));
        Ok(ChangeFeed::notified(self.clone(), after, woken))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::etcd::grpc::Status;

    fn status(code: Code) -> CallError {
        CallError::Status(Status {
            code,
            message: "m".to_owned(),
        })
    }

    #[test]
    fn versions_are_canonical_positive_revisions() {
        assert_eq!(revision_of(&version_of(42)), Some(42));
        for bad in ["0", "-3", "042", "+42", "", "x", "\"etag\""] {
            assert_eq!(revision_of(&Version::new(bad)), None, "{bad}");
        }
    }

    #[test]
    fn ranges_end_after_the_prefix() {
        assert_eq!(range_end("c/nodes/"), Bytes::from_static(b"c/nodes0"));
        assert_eq!(range_end(""), Bytes::new());
    }

    #[test]
    fn errors_map_onto_the_lost_response_rule() {
        let writes_unknown = [
            Code::CANCELLED,
            Code::UNKNOWN,
            Code::DEADLINE_EXCEEDED,
            Code::ABORTED,
            Code::INTERNAL,
            Code::UNAVAILABLE,
        ];
        for code in writes_unknown {
            assert!(matches!(
                write_error(status(code)),
                ControlError::Indeterminate(_)
            ));
            assert!(matches!(
                read_error(status(code)),
                ControlError::Unavailable(_)
            ));
        }
        let no_answer = CallError::NoAnswer("timeout".to_owned());
        assert!(matches!(
            write_error(no_answer.clone()),
            ControlError::Indeterminate(_)
        ));
        assert!(matches!(
            read_error(no_answer),
            ControlError::Unavailable(_)
        ));
        let not_sent = CallError::NotSent("refused".to_owned());
        assert!(matches!(
            write_error(not_sent.clone()),
            ControlError::Unavailable(_)
        ));
        assert!(matches!(read_error(not_sent), ControlError::Unavailable(_)));
        let busy = status(Code::RESOURCE_EXHAUSTED);
        assert!(matches!(
            write_error(busy.clone()),
            ControlError::Unavailable(_)
        ));
        assert!(matches!(read_error(busy), ControlError::Unavailable(_)));
        // INVALID_ARGUMENT, PERMISSION_DENIED, FAILED_PRECONDITION,
        // OUT_OF_RANGE, UNIMPLEMENTED, UNAUTHENTICATED.
        for code in [3, 7, 9, 11, 12, 16] {
            let error = write_error(status(Code(code)));
            assert!(matches!(error, ControlError::Rejected(_)), "{error}");
            let error = read_error(status(Code(code)));
            assert!(matches!(error, ControlError::Rejected(_)), "{error}");
        }
    }

    #[test]
    fn a_write_without_a_revision_is_indeterminate() {
        let error = written_at(&TxnResponse {
            header: None,
            succeeded: true,
        })
        .unwrap_err();
        assert!(error.may_have_applied(), "{error}");
    }

    #[test]
    fn configurations_are_checked() {
        let valid = EtcdStoreConfig::new(vec!["http://127.0.0.1:2379".to_owned()], "c/");
        let store = EtcdControlStore::new(valid.clone()).unwrap();
        assert_eq!(store.prefix(), "c/");
        assert_eq!(
            store.etcd_key(&RegisterKey::cluster()),
            "c/cluster.json".to_owned()
        );
        assert!(format!("{store:?}").contains("127.0.0.1"));
        assert!(format!("{valid:?}").contains("tls: false"));
        let invalid = [
            EtcdStoreConfig {
                endpoints: Vec::new(),
                ..valid.clone()
            },
            EtcdStoreConfig {
                endpoints: vec!["etcd:2379".to_owned()],
                ..valid.clone()
            },
            EtcdStoreConfig {
                endpoints: vec!["https://etcd:2379".to_owned()],
                ..valid.clone()
            },
            EtcdStoreConfig {
                prefix: String::new(),
                ..valid.clone()
            },
            EtcdStoreConfig {
                prefix: "c".to_owned(),
                ..valid.clone()
            },
            EtcdStoreConfig {
                prefix: "a b/".to_owned(),
                ..valid
            },
        ];
        for config in invalid {
            let error = EtcdControlStore::new(config.clone()).unwrap_err();
            assert!(
                error.to_string().starts_with("invalid etcd control-store"),
                "{config:?}: {error}"
            );
        }
    }
}
