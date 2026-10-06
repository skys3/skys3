//! Write-through acknowledgement (design §7.5).
//!
//! A write to a `write_back` bucket whose `ack_policy` is `write_through`
//! is answered only once its version has reached the bucket's remote
//! target as well as every member: PutObject, CopyObject,
//! CompleteMultipartUpload, DeleteObject, each key of DeleteObjects, and
//! the tagging writes. Multipart parts are not objects and are answered
//! after their local commit; their completion waits.
//!
//! After the local commit the gateway asks the shard's primary, whose
//! flusher answers once the remote holds the version or a later one of the
//! key ([`Shards::flushed`]). The ask is bounded by [`FLUSH_WAIT_ROUND`]
//! and repeated until `write_through_timeout_seconds` have passed, and an
//! ask that fails, such as while a new primary's flusher starts after a
//! primary change, is repeated after [`RETRY_DELAY`]. A write is never
//! undone: the client's error says that it is committed in the cluster,
//! and the flusher keeps sending it.
//!
//! - **Not flushed in time** (a remote outage, slow transfers, a primary
//!   change that outlasts the timeout): `503 SlowDown`.
//! - **Held in conflict** (§7.2): `409 OperationAborted`. The key is not
//!   flushed until the conflict is resolved, so retrying cannot help.

use std::time::Duration;

use s3s::{S3Error, S3Result, s3_error};
use skys3_config::{AckPolicy, BucketsConfig};
use skys3_shard::FlushState;
use skys3_types::{BucketDocument, BucketMode, EpochSeq};
use tokio::time::Instant;

use crate::shard::{ShardError, ShardRef, Shards};

/// The longest one ask of the shard's primary waits before the gateway
/// asks again: well within a forwarded request's timeout.
pub(crate) const FLUSH_WAIT_ROUND: Duration = Duration::from_secs(5);

/// How long the gateway waits before it asks again after an ask failed.
pub(crate) const RETRY_DELAY: Duration = Duration::from_millis(100);

/// Which buckets acknowledge writes only once they are flushed, and how
/// long such a write waits.
#[derive(Debug, Clone)]
pub(crate) struct WriteThrough {
    buckets: BucketsConfig,
    timeout: Duration,
}

impl WriteThrough {
    pub(crate) fn new(buckets: BucketsConfig, timeout: Duration) -> Self {
        Self { buckets, timeout }
    }

    /// How long a write to `bucket` waits for its flush, or `None` if the
    /// bucket acknowledges writes after their local commit.
    pub(crate) fn wait_of(&self, bucket: &BucketDocument) -> Option<Duration> {
        let write_through = bucket.mode == BucketMode::WriteBack
            && self.buckets.get(&bucket.name).ack_policy == AckPolicy::WriteThrough;
        write_through.then_some(self.timeout)
    }
}

/// Waits up to `timeout` until the remote target holds `version` of
/// `key`, committed in `shard`, or a later version of the key.
///
/// # Errors
///
/// `503 SlowDown` if the remote does not hold it in time, and
/// `409 OperationAborted` if the key is held in conflict.
pub(crate) async fn reach_remote<H: Shards>(
    shards: &H,
    shard: &ShardRef,
    key: &str,
    version: EpochSeq,
    timeout: Duration,
) -> S3Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(not_flushed(shard, key, timeout));
        }
        match shards
            .flushed(shard, key, version, left.min(FLUSH_WAIT_ROUND))
            .await
        {
            Ok(FlushState::Flushed) => return Ok(()),
            Ok(FlushState::Conflict) => return Err(conflict()),
            // Not yet: ask again.
            Ok(_) => {}
            Err(error) => {
                log_failed_ask(shard, key, &error);
                tokio::time::sleep(RETRY_DELAY.min(left)).await;
            }
        }
    }
}

fn log_failed_ask(shard: &ShardRef, key: &str, error: &ShardError) {
    tracing::debug!(%shard, key, %error, "asking for a write-through flush failed; asking again");
}

/// The answer to a write whose flush did not finish in time.
fn not_flushed(shard: &ShardRef, key: &str, timeout: Duration) -> S3Error {
    tracing::warn!(%shard, key, ?timeout,
        "a write-through write did not reach the remote target in time");
    s3_error!(
        SlowDown,
        "The write is committed in the cluster but did not reach the bucket's remote target \
         in time; it is sent there later. Retry later"
    )
}

/// The answer to a write whose key is held in conflict.
fn conflict() -> S3Error {
    s3_error!(
        OperationAborted,
        "The bucket's remote target holds a write this cluster did not make, so the key is \
         held in conflict; the write is committed in the cluster but reaches the target only \
         once the conflict is resolved"
    )
}

#[cfg(test)]
mod tests {
    use s3s::S3ErrorCode;
    use skys3_config::Config;
    use skys3_types::{Epoch, Seq};

    use super::*;
    use crate::shard::tests::bucket;
    use crate::stub::MemoryShards;

    fn config(text: &str) -> Config {
        format!(
            "[cluster]\ncluster_id = \"c\"\n\
             [control_store]\netcd_endpoints = [\"https://e:2379\"]\n{text}"
        )
        .parse()
        .unwrap()
    }

    #[test]
    fn only_write_back_buckets_with_the_policy_wait() {
        let through = config("[flush]\nack_policy = \"write_through\"");
        let timeout = Duration::from_secs(7);
        let policy = WriteThrough::new(through.buckets().clone(), timeout);
        let mut photos = bucket("b-1", 1);
        assert_eq!(policy.wait_of(&photos), None, "a local bucket");
        photos.mode = BucketMode::WriteBack;
        assert_eq!(policy.wait_of(&photos), Some(timeout));

        // A bucket's own table overrides the default.
        let named = config(
            "[flush]\nack_policy = \"write_through\"\n\
             [buckets.photos]\nack_policy = \"local\"",
        );
        let policy = WriteThrough::new(named.buckets().clone(), timeout);
        assert_eq!(policy.wait_of(&photos), None);
        let local = config("");
        let policy = WriteThrough::new(local.buckets().clone(), timeout);
        assert_eq!(policy.wait_of(&photos), None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_wait_without_a_flusher_ends_in_slow_down() {
        let shards = MemoryShards::new().await;
        let mut document = bucket("b-1", 1);
        document.mode = BucketMode::WriteBack;
        let shard = ShardRef::for_key(&document, "k");
        let version = EpochSeq::new(Epoch::new(1), Seq::new(1));
        // The shard is not open, and then has no flusher: every ask fails,
        // and the write is answered once the timeout passed.
        let started = Instant::now();
        let timeout = Duration::from_secs(2);
        let error = reach_remote(&shards, &shard, "k", version, timeout)
            .await
            .unwrap_err();
        assert_eq!(*error.code(), S3ErrorCode::SlowDown);
        assert!(started.elapsed() >= timeout);

        shards.open(&shard, &document).await.unwrap();
        let error = reach_remote(&shards, &shard, "k", version, timeout)
            .await
            .unwrap_err();
        assert_eq!(*error.code(), S3ErrorCode::SlowDown);
        assert_eq!(*conflict().code(), S3ErrorCode::OperationAborted);
    }
}
