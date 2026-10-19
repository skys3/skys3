//! Refused flushes (§7.6): a target that answers a flush, or the capability
//! probe that flushing waits for, with `401` or `403`, or a SkyS3 peer that
//! answers a native `COMMIT` or `BEGIN` with `refused` (§7.8).
//!
//! A refusal is about the path to the target, not about the key: wrong or
//! revoked credentials, a bucket policy, or a peer whose bucket does not
//! name this cluster as its `peer_source`. So a refused key is neither held
//! as a conflict nor given up: it stays dirty and is retried with the usual
//! backoff, and flushing resumes by itself once the operator fixes the
//! path. What this module adds is visibility: each refusal is counted by
//! its code in `skys3_flush_refusals_total`, the bucket's flush status
//! names the latest one, and `skys3_flush_refused_keys` counts the keys a
//! refusal holds up.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use prometheus_client::metrics::counter::Counter;
use skys3_remote::S3Error;

/// Why a target refused a flush, as the `code` label of
/// `skys3_flush_refusals_total` names it. The set is fixed, so the label's
/// cardinality is bounded whatever a target answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RefusalCode {
    /// `403 AccessDenied`: the credentials are valid, but a policy, or a
    /// SkyS3 peer's bucket configuration (§7.8), forbids the write.
    AccessDenied,
    /// `403 InvalidAccessKeyId`: the target does not know the access key.
    InvalidAccessKeyId,
    /// `403 SignatureDoesNotMatch`: the secret key is wrong.
    SignatureDoesNotMatch,
    /// A SkyS3 peer answered a native `COMMIT`, a `BATCH` item, or a
    /// `BEGIN` with `refused` (§7.8), as it does for a bucket that does not
    /// receive from this cluster or a bucket pair it is not authorized for.
    Refused,
    /// Any other `401` or `403`, such as `ExpiredToken` or
    /// `RequestTimeTooSkewed`, or one without an error code, as answers to
    /// `HEAD` are.
    Other,
}

impl RefusalCode {
    /// Every code, in label order.
    pub const ALL: [RefusalCode; 5] = [
        RefusalCode::AccessDenied,
        RefusalCode::InvalidAccessKeyId,
        RefusalCode::SignatureDoesNotMatch,
        RefusalCode::Refused,
        RefusalCode::Other,
    ];

    /// The label value: the S3 error code, `refused` for a native refusal,
    /// or `other`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            RefusalCode::AccessDenied => "AccessDenied",
            RefusalCode::InvalidAccessKeyId => "InvalidAccessKeyId",
            RefusalCode::SignatureDoesNotMatch => "SignatureDoesNotMatch",
            RefusalCode::Refused => "refused",
            RefusalCode::Other => "other",
        }
    }

    /// The code of `error` if it is a refusal: a `401` or `403` answer.
    /// Any other error, a throttle or a server error for example, is not.
    #[must_use]
    pub fn of(error: &S3Error) -> Option<RefusalCode> {
        if !matches!(error.status(), Some(401 | 403)) {
            return None;
        }
        Some(match error.code() {
            "AccessDenied" => RefusalCode::AccessDenied,
            "InvalidAccessKeyId" => RefusalCode::InvalidAccessKeyId,
            "SignatureDoesNotMatch" => RefusalCode::SignatureDoesNotMatch,
            _ => RefusalCode::Other,
        })
    }

    fn index(self) -> usize {
        self as usize
    }
}

impl std::fmt::Display for RefusalCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The latest refusal of a bucket's target, as its flush status in the
/// admin API names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    /// Why the target refused.
    pub code: RefusalCode,
    /// The target that refused: its endpoint and bucket, as
    /// `<endpoint>/<bucket>`.
    pub target: String,
    /// What was refused and the target's answer: `probe: <error>` for the
    /// capability probe, `<key>: <error>` for a key's flush.
    pub error: String,
    /// When, on the node's wall clock, since the Unix epoch.
    pub at: Duration,
}

/// Where the flushers and the probe of one bucket's target report its
/// refusals: the bucket's `skys3_flush_refusals_total` counters, one per
/// [`RefusalCode`], and the latest refusal. Clones share them, so they
/// outlive the target being rebuilt for a new transport. The default
/// counts in no registry and names no target.
#[derive(Debug, Clone, Default)]
pub struct Refusals {
    counters: [Counter; RefusalCode::ALL.len()],
    target: Arc<str>,
    latest: Arc<Mutex<Option<Refusal>>>,
}

impl Refusals {
    /// Refusals of `target` counted in `counters`, indexed as
    /// [`RefusalCode::ALL`].
    pub(crate) fn new(counters: [Counter; RefusalCode::ALL.len()], target: &str) -> Self {
        Self {
            counters,
            target: Arc::from(target),
            latest: Arc::default(),
        }
    }

    /// Counts a refusal with `code` at wall-clock time `at`, and keeps it as
    /// the latest; `error` says what was refused and how.
    pub fn record(&self, code: RefusalCode, error: String, at: Duration) {
        self.counters[code.index()].inc();
        let refusal = Refusal {
            code,
            target: self.target.to_string(),
            error,
            at,
        };
        *self.latest.lock().unwrap_or_else(PoisonError::into_inner) = Some(refusal);
    }

    /// The latest refusal, if the target refused anything since the node
    /// started.
    #[must_use]
    pub fn latest(&self) -> Option<Refusal> {
        self.latest
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// How many refusals with `code` were counted.
    #[must_use]
    pub fn count(&self, code: RefusalCode) -> u64 {
        self.counters[code.index()].get()
    }
}

#[cfg(test)]
mod tests {
    use skys3_remote::S3ErrorKind;

    use super::*;

    fn answer(status: u16, code: Option<&str>) -> S3Error {
        let error = S3Error::new(S3ErrorKind::Other, "refused").with_status(status);
        match code {
            Some(code) => error.with_code(code),
            None => error,
        }
    }

    #[test]
    fn only_401_and_403_answers_are_refusals() {
        let cases = [
            (403, Some("AccessDenied"), Some(RefusalCode::AccessDenied)),
            (
                403,
                Some("InvalidAccessKeyId"),
                Some(RefusalCode::InvalidAccessKeyId),
            ),
            (
                403,
                Some("SignatureDoesNotMatch"),
                Some(RefusalCode::SignatureDoesNotMatch),
            ),
            // Codes outside the fixed set, and none at all, keep the
            // label bounded.
            (403, Some("RequestTimeTooSkewed"), Some(RefusalCode::Other)),
            (400, Some("ExpiredToken"), None),
            (401, Some("Unauthorized"), Some(RefusalCode::Other)),
            (403, None, Some(RefusalCode::Other)),
            (404, Some("NoSuchBucket"), None),
            (500, Some("AccessDenied"), None),
            (503, Some("SlowDown"), None),
        ];
        for (status, code, expected) in cases {
            assert_eq!(
                RefusalCode::of(&answer(status, code)),
                expected,
                "{status} {code:?}"
            );
        }
        // Errors with no status are no refusals either.
        for kind in [S3ErrorKind::Timeout, S3ErrorKind::NotSent] {
            assert_eq!(RefusalCode::of(&S3Error::new(kind, "")), None);
        }
        assert_eq!(
            RefusalCode::of(&S3Error::new(S3ErrorKind::PreconditionFailed, "")),
            None
        );
    }

    #[test]
    fn labels_are_distinct_and_in_order() {
        let labels: Vec<_> = RefusalCode::ALL.iter().map(|c| c.to_string()).collect();
        assert_eq!(
            labels,
            [
                "AccessDenied",
                "InvalidAccessKeyId",
                "SignatureDoesNotMatch",
                "refused",
                "other"
            ]
        );
        for (index, code) in RefusalCode::ALL.into_iter().enumerate() {
            assert_eq!(code.index(), index);
        }
    }

    #[test]
    fn the_latest_refusal_is_kept_and_each_is_counted() {
        let refusals = Refusals::new(Default::default(), "http://peer:9000/inbound");
        assert_eq!(refusals.latest(), None);
        let shared = refusals.clone();
        refusals.record(
            RefusalCode::AccessDenied,
            "probe: 403 AccessDenied".to_owned(),
            Duration::from_secs(1),
        );
        shared.record(
            RefusalCode::Refused,
            "k: refused".to_owned(),
            Duration::from_secs(2),
        );
        assert_eq!(refusals.count(RefusalCode::AccessDenied), 1);
        assert_eq!(refusals.count(RefusalCode::Refused), 1);
        assert_eq!(refusals.count(RefusalCode::Other), 0);
        assert_eq!(
            refusals.latest(),
            Some(Refusal {
                code: RefusalCode::Refused,
                target: "http://peer:9000/inbound".to_owned(),
                error: "k: refused".to_owned(),
                at: Duration::from_secs(2),
            })
        );
    }
}
