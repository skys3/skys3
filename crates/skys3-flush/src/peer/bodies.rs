//! Staging streamed single PUTs at a native target while the client
//! uploads (§7.3, §7.8).
//!
//! The gateway receiving a body announces its extents to the primary once
//! they are durable on every member ([`StreamedBody`]). For a native
//! target, each announcement's extents are staged at once, on a stream of
//! their own: `BEGIN` under the body's `UPLOAD_BEGIN` identity, the
//! `RESUME`, and the frames the destination lacks. The stream then ends,
//! and its end waits for the destination's appends, so the next `RESUME`
//! of the identity reports them. A stream that fails is tried again after
//! a backoff; what it staged stays staged.
//!
//! The flush of the body's `PUT` claims the body, which stops its stream
//! if one is running, and stages and commits the version as any other
//! (see the `upload` module): its `RESUME` finds what was staged here, so
//! only the rest is sent after the local commit. A body whose `PUT` does
//! not commit within `body_timeout` of its last announcement, or whose
//! `PUT` is superseded before it is flushed, is told to the destination
//! with an `ABORT`; staging the destination never hears of again expires
//! after `peer_staging_ttl_seconds`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use skys3_io::Disk;
use skys3_log::record::ExtentRef;
use skys3_peer::{Abort, AbortReason, Begin, Message};
use skys3_shard::{Shard, StreamedBody};
use skys3_types::EpochSeq;
use tokio::task::{AbortHandle, JoinSet};
use tokio::time::Instant;

use super::Native;
use super::link::Stream;
use super::upload::{Ended, Layout, Staged};
use crate::attempt::Failure;
use crate::target::{FlushSettings, Target};

/// One streamed body, by the position of its `UPLOAD_BEGIN`.
#[derive(Debug)]
struct Body {
    key: String,
    /// Announced extents not staged yet, by offset in the body.
    extents: BTreeMap<u64, ExtentRef>,
    /// Whether its `PUT` may still commit.
    open: bool,
    /// When it was last announced.
    announced: Instant,
    /// The stream staging it.
    staging: Option<AbortHandle>,
    /// Failed streams since the last that staged.
    failures: u32,
    /// When a stream may be tried again after a failure.
    retry_at: Option<Instant>,
}

/// What a body's task did.
#[derive(Debug)]
pub(crate) enum BodyStep {
    /// Its stream staged the announced extents at these offsets.
    Staged(EpochSeq, Vec<u64>),
    /// Its stream failed; it is tried again after a backoff.
    Failed(EpochSeq, String),
    /// An `ABORT` of a body's staging was sent, or could not be.
    Aborted,
}

/// The streamed bodies of one shard flusher with a native target.
#[derive(Debug, Default)]
pub(crate) struct Bodies {
    bodies: Mutex<BTreeMap<EpochSeq, Body>>,
    /// Bodies whose staging the destination is to discard, by key and
    /// `UPLOAD_BEGIN`.
    doomed: Mutex<Vec<(String, EpochSeq)>>,
}

impl Bodies {
    fn lock(&self) -> MutexGuard<'_, BTreeMap<EpochSeq, Body>> {
        // Every update leaves the map consistent.
        self.bodies.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn doom(&self, key: String, upload: EpochSeq) {
        self.doomed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((key, upload));
    }

    /// The gateway receiving a body announced more of it. Once its `PUT`
    /// committed, announcements are ignored.
    pub(crate) fn announced(&self, announced: StreamedBody) {
        let mut bodies = self.lock();
        let body = bodies.entry(announced.upload).or_insert_with(|| Body {
            key: announced.key,
            extents: BTreeMap::new(),
            open: true,
            announced: Instant::now(),
            staging: None,
            failures: 0,
            retry_at: None,
        });
        if body.open {
            body.announced = Instant::now();
            body.extents.extend(announced.extents);
        }
    }

    /// The `PUT` of the body begun at `upload` committed: nothing more of
    /// it comes.
    pub(crate) fn completed(&self, upload: EpochSeq) {
        if let Some(body) = self.lock().get_mut(&upload) {
            body.open = false;
        }
    }

    /// The flush of the `PUT` of the body begun at `upload` takes over:
    /// the body's stream, if any, stops, and the flush stages the rest.
    pub(crate) fn claim(&self, upload: EpochSeq) {
        if let Some(body) = self.lock().remove(&upload)
            && let Some(task) = body.staging
        {
            task.abort();
        }
    }

    /// `key` is clean, or its latest version is flushed some other way:
    /// the bodies of its committed `PUT`s that were not claimed will never
    /// be, and their staging is discarded.
    pub(crate) fn reap(&self, key: &str) {
        let mut bodies = self.lock();
        let reaped: Vec<EpochSeq> = bodies
            .iter()
            .filter(|(_, body)| body.key == key && !body.open)
            .map(|(&upload, _)| upload)
            .collect();
        for upload in reaped {
            if let Some(body) = bodies.remove(&upload) {
                if let Some(task) = body.staging {
                    task.abort();
                }
                self.doom(body.key, upload);
            }
        }
    }

    /// How many bodies are being staged.
    pub(crate) fn len(&self) -> usize {
        self.lock().len()
    }

    /// When the flusher must look at its bodies again: a stream to retry,
    /// or a body that times out.
    pub(crate) fn next_wake(&self, settings: &FlushSettings) -> Option<Instant> {
        self.lock()
            .values()
            .flat_map(|body| {
                let timeout = body.open.then(|| body.announced + settings.body_timeout);
                let retry = body.retry_at.filter(|_| body.staging.is_none());
                timeout.into_iter().chain(retry)
            })
            .min()
    }

    /// Records what a body's task did.
    pub(crate) fn finish(&self, step: BodyStep, settings: &FlushSettings) {
        let mut bodies = self.lock();
        match step {
            BodyStep::Staged(upload, offsets) => {
                if let Some(body) = bodies.get_mut(&upload) {
                    body.staging = None;
                    body.failures = 0;
                    body.retry_at = None;
                    for offset in offsets {
                        body.extents.remove(&offset);
                    }
                }
            }
            BodyStep::Failed(upload, error) => {
                if let Some(body) = bodies.get_mut(&upload) {
                    tracing::debug!(key = body.key, %error, "a streamed body will be staged again");
                    body.staging = None;
                    body.failures += 1;
                    body.retry_at = Some(Instant::now() + settings.backoff(body.failures));
                }
            }
            BodyStep::Aborted => {}
        }
    }

    /// Starts what the bodies need: a stream for each body with announced
    /// extents to stage, while fewer than `max_concurrency` stream, and an
    /// `ABORT` for each body that timed out or was reaped.
    pub(crate) fn pump<S: Send + Sync + 'static, D: Disk>(
        &self,
        shard: &Shard<D>,
        target: &Arc<Target<S>>,
        tasks: &mut JoinSet<BodyStep>,
    ) {
        let settings = &target.settings;
        let now = Instant::now();
        let mut bodies = self.lock();
        let timed_out: Vec<EpochSeq> = bodies
            .iter()
            .filter(|(_, body)| body.open && body.announced + settings.body_timeout <= now)
            .map(|(&upload, _)| upload)
            .collect();
        for upload in timed_out {
            if let Some(body) = bodies.remove(&upload) {
                tracing::debug!(key = body.key, %upload,
                    "a streamed body's PUT did not commit in time; its staging is discarded");
                if let Some(task) = body.staging {
                    task.abort();
                }
                self.doom(body.key, upload);
            }
        }
        let doomed =
            std::mem::take(&mut *self.doomed.lock().unwrap_or_else(PoisonError::into_inner));
        for (key, upload) in doomed {
            let (shard, target) = (shard.clone(), Arc::clone(target));
            tasks.spawn(async move {
                discard(&shard, &target, key, upload).await;
                BodyStep::Aborted
            });
        }
        if !settings.streaming {
            return;
        }
        let mut staging = bodies.values().filter(|b| b.staging.is_some()).count();
        for (&upload, body) in bodies.iter_mut() {
            if staging >= settings.max_concurrency as usize {
                break;
            }
            let due = body.retry_at.is_none_or(|at| at <= now);
            if body.staging.is_some() || body.extents.is_empty() || !due {
                continue;
            }
            let layout = Layout::of_extents(&body.extents);
            let (shard, target, key) = (shard.clone(), Arc::clone(target), body.key.clone());
            let task = tasks.spawn(async move {
                match stage(&shard, &target, key, upload, &layout).await {
                    Ok(()) => BodyStep::Staged(upload, layout.0.iter().map(|s| s.offset).collect()),
                    Err(failure) => BodyStep::Failed(upload, failure.to_string()),
                }
            });
            body.staging = Some(task);
            staging += 1;
        }
    }
}

/// The `BEGIN` of the body of `key` begun at `upload`, if `target` is
/// native.
fn begin<'t, S, D: Disk>(
    shard: &Shard<D>,
    target: &'t Target<S>,
    key: String,
    upload: EpochSeq,
) -> Option<(&'t Native, Begin)> {
    let native = target.native.as_ref()?;
    let begin = Begin {
        identity: target.identity(shard.shard(), upload),
        bucket: native.bucket.clone(),
        key: format!("{}{key}", target.prefix),
    };
    Some((native, begin))
}

/// Stages `layout`, announced extents of the body of `key` begun at
/// `upload`, and waits until the destination ended the stream, by when
/// every frame it accepted has settled in its staging.
async fn stage<S, D: Disk>(
    shard: &Shard<D>,
    target: &Target<S>,
    key: String,
    upload: EpochSeq,
    layout: &Layout,
) -> Result<(), Failure> {
    let Some((native, begin)) = begin(shard, target, key, upload) else {
        return Ok(());
    };
    let staged = Staged {
        shard,
        target,
        native,
        begin,
    };
    let (mut stream, durable) = match staged.open().await.map_err(Failure::link)? {
        Ok(opened) => opened,
        Err(Ended::Aborted(reason, detail)) => {
            return Err(Failure::Peer(format!("{reason:?}: {detail}")));
        }
        Err(Ended::Applied(outcome)) => return Err(Failure::Peer(format!("{outcome:?}"))),
    };
    staged.send(&mut stream, layout, &durable).await?;
    stream.finish().map_err(Failure::link)?;
    while stream.recv().await.map_err(Failure::link)?.is_some() {}
    Ok(())
}

/// Tells the destination to discard the staging of the body of `key`
/// begun at `upload`. A failure leaves it to expire there.
async fn discard<S, D: Disk>(shard: &Shard<D>, target: &Target<S>, key: String, upload: EpochSeq) {
    let Some((native, begin)) = begin(shard, target, key, upload) else {
        return;
    };
    let abort = Message::Abort(Abort {
        identity: begin.identity,
        reason: AbortReason::Cancelled,
        detail: "the body's PUT will not be flushed".to_owned(),
    });
    let sent = async {
        let mut stream = Stream::open(&*native.link, native.timeout).await?;
        stream.send(&abort).await?;
        stream.finish()
    };
    if let Err(error) = sent.await {
        tracing::debug!(%error, "a streamed body's staging is left to expire");
    }
}
