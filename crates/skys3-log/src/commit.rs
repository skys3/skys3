//! The group committer: one task per disk that writes queued records,
//! syncs them, and acknowledges them (§10.4).

use std::io;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use skys3_io::{Clock, Disk, MonoTime, SegmentFile};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, oneshot};

use crate::config::LogConfig;
use crate::log::Shared;
use crate::segment::{RecordLocation, SegmentClass, SegmentId, file_name};

/// What the committer tells an appender: the record's location once a sync
/// covers it, or the error that took the disk out of service.
pub(crate) type Reply = Result<RecordLocation, Arc<io::Error>>;

/// One encoded record waiting for a group commit.
#[derive(Debug)]
pub(crate) struct Request {
    pub(crate) class: SegmentClass,
    pub(crate) bytes: Bytes,
    /// When the record was queued, on the log's clock.
    pub(crate) arrival: MonoTime,
    pub(crate) reply: oneshot::Sender<Reply>,
}

/// The segment a class appends to.
#[derive(Debug)]
struct Active<F> {
    id: SegmentId,
    file: Arc<F>,
    /// The segment's length: every byte written by a group commit that
    /// succeeded, or recovered.
    len: u64,
}

/// The task that owns the disk's write path.
///
/// It takes queued records in arrival order and commits them in groups:
/// it waits from a group's first record until `group_commit_max_delay`
/// passes or the group reaches `group_commit_max_bytes`, then appends each
/// class's records to its segment with one write, syncs every file written
/// (and the directory, if it created a file), and only then acknowledges
/// the group. Records that arrive meanwhile wait for the next group.
///
/// The first I/O error ends the write path for good: the committer fails
/// the group it was committing and every record queued after it, and the
/// log reports the disk out of service. It never retries, so no record a
/// failed sync covered is acknowledged later.
#[derive(Debug)]
pub(crate) struct Committer<D: Disk> {
    disk: D,
    shared: Arc<Shared<D::File>>,
    requests: mpsc::Receiver<Request>,
    clock: Arc<dyn Clock>,
    config: LogConfig,
    /// The segment each class appends to, by [`SegmentClass::index`].
    active: [Option<Active<D::File>>; 2],
    /// The id of the next new segment, or `None` once ids are exhausted.
    next_id: Option<SegmentId>,
    /// Whether a file was created since the last directory sync.
    dir_dirty: bool,
}

impl<D: Disk> Committer<D> {
    pub(crate) fn new(
        disk: D,
        shared: Arc<Shared<D::File>>,
        requests: mpsc::Receiver<Request>,
        clock: Arc<dyn Clock>,
        config: LogConfig,
        next_id: Option<SegmentId>,
    ) -> Self {
        let active = SegmentClass::ALL.map(|class| {
            shared.last_segment(class).map(|(id, file)| Active {
                id,
                len: file.len(),
                file,
            })
        });
        Self {
            disk,
            shared,
            requests,
            clock,
            config,
            active,
            next_id,
            dir_dirty: false,
        }
    }

    /// Commits groups until every log handle is dropped, or until the
    /// first I/O error.
    pub(crate) async fn run(mut self) {
        let mut group = Vec::new();
        while let Some(first) = self.requests.recv().await {
            self.gather(first, &mut group).await;
            match self.commit(&group).await {
                Ok(locations) => {
                    let bytes = group.iter().map(|r| r.bytes.len() as u64).sum();
                    self.shared.stats.record_commit(group.len() as u64, bytes);
                    for (request, location) in group.drain(..).zip(locations) {
                        // The appender may have stopped waiting; the record
                        // is durable either way.
                        let _ = request.reply.send(Ok(location));
                    }
                }
                Err(error) => {
                    let error = self.shared.take_out_of_service(error);
                    for request in group.drain(..) {
                        let _ = request.reply.send(Err(Arc::clone(&error)));
                    }
                    self.requests.close();
                    while let Some(request) = self.requests.recv().await {
                        let _ = request.reply.send(Err(Arc::clone(&error)));
                    }
                    return;
                }
            }
        }
    }

    /// Collects a group starting with `first`: records already queued, and
    /// those that arrive before the group's deadline, until it holds
    /// `group_commit_max_bytes`.
    async fn gather(&mut self, first: Request, group: &mut Vec<Request>) {
        let deadline = first
            .arrival
            .saturating_add(self.config.group_commit_max_delay);
        let mut bytes = first.bytes.len() as u64;
        group.push(first);
        while bytes < self.config.group_commit_max_bytes {
            let next = match self.requests.try_recv() {
                Ok(request) => Some(request),
                Err(TryRecvError::Disconnected) => None,
                Err(TryRecvError::Empty) if self.clock.now() >= deadline => None,
                Err(TryRecvError::Empty) => {
                    let at = self.clock.runtime_deadline(deadline);
                    tokio::time::timeout_at(at, self.requests.recv())
                        .await
                        .ok()
                        .flatten()
                }
            };
            let Some(request) = next else { break };
            bytes += request.bytes.len() as u64;
            group.push(request);
        }
    }

    /// Writes and syncs `group`, and returns each record's location.
    async fn commit(&mut self, group: &[Request]) -> io::Result<Vec<RecordLocation>> {
        let mut class_bytes = [0u64; 2];
        for request in group {
            class_bytes[request.class.index()] += request.bytes.len() as u64;
        }
        for class in SegmentClass::ALL {
            let len = class_bytes[class.index()];
            if len > 0 {
                self.prepare_segment(class, len).await?;
            }
        }

        let mut ends = self
            .active
            .each_ref()
            .map(|a| a.as_ref().map_or(0, |a| a.len));
        let mut buffers: [Vec<Bytes>; 2] = Default::default();
        let mut locations = Vec::with_capacity(group.len());
        for request in group {
            let index = request.class.index();
            let active = self.active[index]
                .as_ref()
                .expect("prepare_segment opened a segment for every class in the group");
            let len = u32::try_from(request.bytes.len())
                .expect("records are at most MAX_RECORD_LEN bytes");
            locations.push(RecordLocation {
                segment: active.id,
                offset: ends[index],
                len,
            });
            ends[index] += u64::from(len);
            buffers[index].push(request.bytes.clone());
        }

        let [hot, bulk] = buffers;
        let [hot_segment, bulk_segment] = self.active.each_ref();
        let dir_dirty = self.dir_dirty;
        let disk = &self.disk;
        let (hot, bulk, dir) = tokio::join!(
            write_and_sync(hot_segment.as_ref(), hot),
            write_and_sync(bulk_segment.as_ref(), bulk),
            async move {
                if dir_dirty {
                    disk.sync_dir().await
                } else {
                    Ok(())
                }
            },
        );
        hot?;
        bulk?;
        dir?;

        self.dir_dirty = false;
        for (active, end) in self.active.iter_mut().zip(ends) {
            if let Some(active) = active {
                active.len = end;
            }
        }
        Ok(locations)
    }

    /// Makes sure `class` has a segment to append `len` bytes to, starting
    /// a new one if it has none, or if the bytes would take a non-empty
    /// segment past `segment_bytes`.
    ///
    /// Starting segments only here, between groups, means every segment
    /// but the last of its class was fully synced before the next was
    /// created, so only the last can have a torn tail.
    async fn prepare_segment(&mut self, class: SegmentClass, len: u64) -> io::Result<()> {
        let index = class.index();
        let full = self.active[index]
            .as_ref()
            .is_none_or(|a| a.len > 0 && a.len.saturating_add(len) > self.config.segment_bytes);
        if !full {
            return Ok(());
        }
        let id = self
            .next_id
            .ok_or_else(|| io::Error::other("segment ids are exhausted"))?;
        let file = Arc::new(self.disk.create(&file_name(class, id)).await?);
        self.next_id = id.next();
        self.dir_dirty = true;
        self.shared.add_segment(id, class, Arc::clone(&file));
        self.active[index] = Some(Active { id, file, len: 0 });
        Ok(())
    }
}

/// Appends a class's records to its segment with one write, then syncs the
/// segment. Does nothing if the class has no records in the group.
async fn write_and_sync<F: SegmentFile>(
    segment: Option<&Active<F>>,
    records: Vec<Bytes>,
) -> io::Result<()> {
    let Some(segment) = segment.filter(|_| !records.is_empty()) else {
        return Ok(());
    };
    let data = concat(records);
    let offset = segment.file.append(data).await?;
    if offset != segment.len {
        return Err(io::Error::other(format!(
            "segment {} was written at offset {offset}, not at its end {}",
            segment.id, segment.len
        )));
    }
    segment.file.sync_data().await
}

/// Joins records into one buffer, without copying a lone record.
fn concat(mut records: Vec<Bytes>) -> Bytes {
    if records.len() == 1 {
        return records.pop().unwrap_or_default();
    }
    let len = records.iter().map(Bytes::len).sum();
    let mut data = BytesMut::with_capacity(len);
    for record in records {
        data.extend_from_slice(&record);
    }
    data.freeze()
}
