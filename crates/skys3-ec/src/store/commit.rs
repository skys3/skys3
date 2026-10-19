//! The fragment committer: one task per disk that writes queued fragments,
//! syncs them, and only then acknowledges them (§10.4).

use std::io;
use std::sync::Arc;

use bytes::Bytes;
use skys3_io::{Disk, SegmentFile};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, oneshot};

use super::{Bugs, FragmentStoreConfig, Located, Shared, compose_id, segment_name};
use crate::FragmentId;
use crate::fragment::{FIXED_LEN, seal};

/// The last segment number a fragment ID can name: 56 bits.
pub(crate) const MAX_SEGMENT: u64 = (1 << 56) - 1;

/// What the committer tells a writer: the fragment's ID once a sync covers
/// it, or the error that took the disk out of service.
pub(crate) type Reply = Result<FragmentId, Arc<io::Error>>;

/// One fragment waiting for a group commit: its header after the fixed
/// header ([`encode_tail`](crate::fragment::encode_tail)) and its payload.
#[derive(Debug)]
pub(crate) struct Request {
    pub(crate) tail: Vec<u8>,
    pub(crate) payload: Bytes,
    pub(crate) reply: oneshot::Sender<Reply>,
}

impl Request {
    fn record_len(&self) -> u64 {
        (FIXED_LEN + self.tail.len()) as u64 + self.payload.len() as u64
    }
}

/// The segment fragments are appended to.
#[derive(Debug)]
struct Active<F> {
    id: u64,
    file: Arc<F>,
    /// Every byte written by a group commit that succeeded, or recovered.
    len: u64,
}

/// The task that owns the store's write path.
///
/// It takes the first queued fragment and every fragment already queued
/// behind it, up to `group_commit_max_bytes`, without waiting for more:
/// fragments are large, so a group gains little from a delay. It appends
/// them to the last segment, syncs it (and the directory, if it created the
/// segment), and only then records and acknowledges them. A new segment is
/// started only before a group, so only the last segment can have a torn
/// tail. The first I/O error ends the write path: the group and every
/// fragment queued after it fail, and nothing is ever acknowledged again.
pub(crate) struct Committer<D: Disk> {
    pub(crate) disk: Arc<D>,
    pub(crate) shared: Arc<Shared<D::File>>,
    pub(crate) requests: mpsc::Receiver<Request>,
    pub(crate) config: FragmentStoreConfig,
    pub(crate) next_segment: Option<u64>,
    pub(crate) bugs: Bugs,
    active: Option<Active<D::File>>,
    dir_dirty: bool,
}

impl<D: Disk> Committer<D> {
    pub(crate) fn new(
        disk: Arc<D>,
        shared: Arc<Shared<D::File>>,
        requests: mpsc::Receiver<Request>,
        config: FragmentStoreConfig,
        next_segment: Option<u64>,
        bugs: Bugs,
    ) -> Self {
        let active = shared.last_segment().map(|(id, file)| Active {
            id,
            len: file.len(),
            file,
        });
        Self {
            disk,
            shared,
            requests,
            config,
            next_segment,
            bugs,
            active,
            dir_dirty: false,
        }
    }

    /// Commits groups until every store handle is dropped, or until the
    /// first I/O error.
    pub(crate) async fn run(mut self) {
        let mut group = Vec::new();
        while let Some(first) = self.requests.recv().await {
            let mut bytes = first.record_len();
            group.push(first);
            while bytes < self.config.group_commit_max_bytes {
                match self.requests.try_recv() {
                    Ok(request) => {
                        bytes = bytes.saturating_add(request.record_len());
                        group.push(request);
                    }
                    Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
                }
            }
            if let Err(error) = self.commit(&mut group).await {
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

    /// Writes and syncs `group`, then records and acknowledges each
    /// fragment, draining `group`. On an error, the fragments left in
    /// `group` were not acknowledged.
    async fn commit(&mut self, group: &mut Vec<Request>) -> io::Result<()> {
        let total = group.iter().map(Request::record_len).sum();
        self.prepare_segment(total).await?;
        let Some(active) = &self.active else {
            return Err(io::Error::other("no fragment segment to append to"));
        };
        let mut end = active.len;
        let mut placed = Vec::with_capacity(group.len());
        for request in group.iter() {
            let id = compose_id(self.config.disk, active.id, end);
            let header = seal(id, request.payload.len() as u64, &request.tail);
            let located = Located {
                segment: active.id,
                offset: end,
                header_len: header.len() as u32,
                payload_len: request.payload.len() as u64,
            };
            end = append(active.file.as_ref(), header.into(), end).await?;
            end = append(active.file.as_ref(), request.payload.clone(), end).await?;
            placed.push((id, located));
        }
        if self.bugs.acknowledge_before_sync {
            self.acknowledge(group, &placed);
        }
        active.file.sync_data().await?;
        if self.dir_dirty && !self.bugs.skip_directory_sync {
            self.disk.sync_dir().await?;
        }
        self.dir_dirty = false;
        if let Some(active) = &mut self.active {
            active.len = end;
        }
        self.acknowledge(group, &placed);
        Ok(())
    }

    /// Records the fragments of `group` in the fragment map, then answers
    /// their writers.
    fn acknowledge(&self, group: &mut Vec<Request>, placed: &[(FragmentId, Located)]) {
        self.shared.insert(placed.iter().copied());
        for (request, (id, _)) in group.drain(..).zip(placed) {
            // The writer may have stopped waiting; the fragment is durable
            // either way.
            let _ = request.reply.send(Ok(*id));
        }
    }

    /// Makes sure there is a segment to append `len` bytes to: a new one
    /// if there is none, or if the bytes would take a non-empty segment
    /// past `segment_bytes`.
    async fn prepare_segment(&mut self, len: u64) -> io::Result<()> {
        let full = self.active.as_ref().is_none_or(|active| {
            active.len > 0 && active.len.saturating_add(len) > self.config.segment_bytes
        });
        if !full {
            return Ok(());
        }
        let id = self
            .next_segment
            .filter(|&id| id <= MAX_SEGMENT)
            .ok_or_else(|| io::Error::other("fragment segment numbers are exhausted"))?;
        let file = Arc::new(self.disk.create(&segment_name(id)).await?);
        self.next_segment = id.checked_add(1);
        self.dir_dirty = true;
        self.shared.add_segment(id, Arc::clone(&file));
        self.active = Some(Active { id, file, len: 0 });
        Ok(())
    }
}

/// Appends `data` to `file`, which must end at `at`, and returns the new
/// end.
async fn append<F: SegmentFile>(file: &F, data: Bytes, at: u64) -> io::Result<u64> {
    let len = data.len() as u64;
    let offset = file.append(data).await?;
    if offset != at {
        return Err(io::Error::other(format!(
            "a fragment segment was written at offset {offset}, not at its end {at}"
        )));
    }
    Ok(at + len)
}
