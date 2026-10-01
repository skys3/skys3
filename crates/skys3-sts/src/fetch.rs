//! Fetching discovery documents and key sets.
//!
//! The validator reaches issuers only through [`DocumentFetcher`], so tests
//! and the simulation serve documents from memory ([`MemoryFetcher`]) and a
//! node uses [`HttpsFetcher`](crate::HttpsFetcher).

use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;

use bytes::Bytes;
use thiserror::Error;

/// Why a document could not be fetched.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum FetchError {
    /// The URL is malformed or not allowed, such as `http` in production.
    #[error("URL {url:?} is not allowed: {reason}")]
    UrlRejected {
        /// The URL.
        url: String,
        /// Why it was rejected.
        reason: &'static str,
    },
    /// The document is larger than the limit.
    #[error("the document is larger than {limit} bytes")]
    TooLarge {
        /// The limit, in bytes.
        limit: usize,
    },
    /// The server answered with a status other than 200.
    #[error("the server answered with status {0}")]
    Status(u16),
    /// The request did not finish within the timeout.
    #[error("the request timed out")]
    Timeout,
    /// The connection or TLS handshake failed.
    #[error("transport error: {0}")]
    Transport(String),
}

/// Fetches a document by URL.
///
/// An implementation must stop reading once a body exceeds `max_bytes` and
/// return [`FetchError::TooLarge`], so a hostile server cannot make the node
/// buffer an unbounded response. The validator checks the length again.
pub trait DocumentFetcher: Send + Sync + 'static {
    /// Fetches the document at `url`, of at most `max_bytes` bytes.
    fn fetch(
        &self,
        url: &str,
        max_bytes: usize,
    ) -> impl Future<Output = Result<Bytes, FetchError>> + Send;
}

/// A [`DocumentFetcher`] that serves documents from memory and counts
/// requests, for tests and the simulation.
#[derive(Debug, Default)]
pub struct MemoryFetcher {
    state: Mutex<MemoryState>,
}

#[derive(Debug, Default)]
struct MemoryState {
    documents: HashMap<String, Result<Bytes, FetchError>>,
    requests: HashMap<String, usize>,
}

impl MemoryFetcher {
    /// Returns a fetcher that serves nothing.
    pub fn new() -> Self {
        Self::default()
    }

    /// Serves `document` at `url`, replacing what was there.
    pub fn insert(&self, url: impl Into<String>, document: impl Into<Bytes>) {
        self.lock()
            .documents
            .insert(url.into(), Ok(document.into()));
    }

    /// Makes requests for `url` fail with `error`.
    pub fn fail(&self, url: impl Into<String>, error: FetchError) {
        self.lock().documents.insert(url.into(), Err(error));
    }

    /// Returns how many times `url` has been requested.
    pub fn requests(&self, url: &str) -> usize {
        self.lock().requests.get(url).copied().unwrap_or(0)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, MemoryState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl DocumentFetcher for MemoryFetcher {
    async fn fetch(&self, url: &str, max_bytes: usize) -> Result<Bytes, FetchError> {
        let mut state = self.lock();
        *state.requests.entry(url.to_owned()).or_default() += 1;
        match state.documents.get(url) {
            None => Err(FetchError::Status(404)),
            Some(Ok(document)) if document.len() > max_bytes => {
                Err(FetchError::TooLarge { limit: max_bytes })
            }
            Some(result) => result.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn serves_counts_and_limits() {
        let fetcher = MemoryFetcher::new();
        fetcher.insert("https://a/doc", "hello");
        fetcher.fail("https://a/down", FetchError::Timeout);

        assert_eq!(fetcher.fetch("https://a/doc", 5).await.unwrap(), "hello");
        assert_eq!(
            fetcher.fetch("https://a/doc", 4).await,
            Err(FetchError::TooLarge { limit: 4 })
        );
        assert_eq!(
            fetcher.fetch("https://a/down", 5).await,
            Err(FetchError::Timeout)
        );
        assert_eq!(
            fetcher.fetch("https://a/none", 5).await,
            Err(FetchError::Status(404))
        );
        assert_eq!(fetcher.requests("https://a/doc"), 2);
        assert_eq!(fetcher.requests("https://a/other"), 0);
    }

    #[test]
    fn errors_display() {
        let rejected = FetchError::UrlRejected {
            url: "http://a".into(),
            reason: "not https",
        };
        assert_eq!(
            rejected.to_string(),
            "URL \"http://a\" is not allowed: not https"
        );
        assert_eq!(
            FetchError::TooLarge { limit: 3 }.to_string(),
            "the document is larger than 3 bytes"
        );
        assert_eq!(
            FetchError::Status(500).to_string(),
            "the server answered with status 500"
        );
        assert_eq!(FetchError::Timeout.to_string(), "the request timed out");
        assert_eq!(
            FetchError::Transport("reset".into()).to_string(),
            "transport error: reset"
        );
    }
}
