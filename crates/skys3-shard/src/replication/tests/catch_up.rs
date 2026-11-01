//! A primary's link to a member that is behind what the primary holds in
//! memory: it reads the records back from the log, and beacons while it
//! does, so a slow read does not end the session (plan M8-08).

use std::sync::atomic::{AtomicBool, AtomicU64};

use skys3_io::{Disk, SegmentFile};

use super::*;

/// How long each read of a segment takes while the disk is slow: longer
/// than the link timeout by itself.
const SLOW_READ: Duration = Duration::from_millis(1500);

/// A simulated disk whose reads are slow while `slow` is set.
#[derive(Debug, Clone)]
pub(super) struct SlowMount {
    inner: SimMount,
    slow: Arc<AtomicBool>,
}

impl SlowMount {
    pub(super) fn new(inner: SimMount, slow: Arc<AtomicBool>) -> Self {
        Self { inner, slow }
    }
}

/// A file of a [`SlowMount`].
#[derive(Debug)]
pub(super) struct SlowFile {
    inner: <SimMount as Disk>::File,
    slow: Arc<AtomicBool>,
}

impl Disk for SlowMount {
    type File = SlowFile;

    async fn create(&self, name: &str) -> std::io::Result<SlowFile> {
        let inner = self.inner.create(name).await?;
        Ok(SlowFile {
            inner,
            slow: Arc::clone(&self.slow),
        })
    }

    async fn open(&self, name: &str) -> std::io::Result<SlowFile> {
        let inner = self.inner.open(name).await?;
        Ok(SlowFile {
            inner,
            slow: Arc::clone(&self.slow),
        })
    }

    async fn remove(&self, name: &str) -> std::io::Result<()> {
        self.inner.remove(name).await
    }

    async fn list(&self) -> std::io::Result<Vec<String>> {
        self.inner.list().await
    }

    async fn sync_dir(&self) -> std::io::Result<()> {
        self.inner.sync_dir().await
    }
}

impl SegmentFile for SlowFile {
    fn len(&self) -> u64 {
        self.inner.len()
    }

    async fn append(&self, data: Bytes) -> std::io::Result<u64> {
        self.inner.append(data).await
    }

    async fn sync_data(&self) -> std::io::Result<()> {
        self.inner.sync_data().await
    }

    async fn read_at(&self, offset: u64, len: usize) -> std::io::Result<Bytes> {
        if self.slow.load(Ordering::SeqCst) {
            tokio::time::sleep(SLOW_READ).await;
        }
        self.inner.read_at(offset, len).await
    }

    async fn truncate(&self, len: u64) -> std::io::Result<()> {
        self.inner.truncate(len).await
    }
}

/// What the hand-written member saw.
#[derive(Default)]
struct Seen {
    /// Sessions the primary opened.
    sessions: AtomicUsize,
    /// The last `seq` appended in the latest session.
    last: AtomicU64,
}

/// Node 2, hand-written: it reports an empty log at every session,
/// acknowledges every record it is sent and answers every beacon, and
/// ends its first session once `forget` is set, as a member that lost its
/// log would.
async fn forgetful_member(pki: &Pki, seen: Arc<Seen>, forget: Arc<AtomicBool>) -> SocketAddr {
    let listener = pki
        .transport(&node(2))
        .bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok(incoming) = listener.accept().await {
            let Ok(mut link) = incoming.handshake().await else {
                continue;
            };
            let session = seen.sessions.fetch_add(1, Ordering::SeqCst) + 1;
            seen.last.store(0, Ordering::SeqCst);
            let (seen, forget) = (Arc::clone(&seen), Arc::clone(&forget));
            tokio::spawn(async move {
                if next(&mut link).await.is_none() {
                    return;
                }
                let answer = SyncAck {
                    last: 0,
                    done: true,
                    epoch: 2,
                    ..SyncAck::default()
                };
                let frame = wire::frame(MessageKind::SyncAck, &answer, Bytes::new());
                if link.send(&frame).await.is_err() {
                    return;
                }
                loop {
                    if session == 1 && forget.load(Ordering::SeqCst) {
                        return;
                    }
                    let frame =
                        match tokio::time::timeout(Duration::from_millis(50), link.recv()).await {
                            Err(_) => continue,
                            Ok(Ok(Some(frame))) => frame,
                            Ok(_) => return,
                        };
                    let lease =
                        if let Ok(beacon) = wire::body::<Beacon>(&frame, MessageKind::Beacon) {
                            beacon.lease
                        } else {
                            let append: Append = wire::body(&frame, MessageKind::Append).unwrap();
                            let (record, _) = LogRecord::decode(&frame.payload).unwrap();
                            seen.last.store(record.position.seq.get(), Ordering::SeqCst);
                            append.lease
                        };
                    let ack = AppendAck {
                        durable: seen.last.load(Ordering::SeqCst),
                        rejected: 0,
                        lease,
                    };
                    let frame = wire::frame(MessageKind::AppendAck, &ack, Bytes::new());
                    if link.send(&frame).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    address
}

/// A member that is behind every record the primary holds in memory is
/// sent them from the log. Reading the log scans every segment, which on a
/// large log took longer than the link timeout; the link sent nothing
/// meanwhile, so the session timed out, and the next one read the log
/// again, for good: on the binary, a re-admitted learner never caught up.
/// The link now beacons while it reads: one session, slow reads and all,
/// delivers every record.
#[test]
fn a_link_that_reads_the_log_back_beacons_meanwhile() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::time::timeout(WAIT, slow_catch_up())
            .await
            .expect("the test finished in time");
    });
}

async fn slow_catch_up() {
    let pki = Pki::new();
    let (seen, forget) = (Arc::new(Seen::default()), Arc::new(AtomicBool::new(false)));
    let address = forgetful_member(&pki, Arc::clone(&seen), Arc::clone(&forget)).await;

    let disk = SimDisk::new(17);
    let slow = Arc::new(AtomicBool::new(false));
    let mount = SlowMount {
        inner: disk.mount(),
        slow: Arc::clone(&slow),
    };
    let log_config = LogConfig {
        inline_max_bytes: 512,
        segment_bytes: 8192,
        group_commit_max_delay: Duration::ZERO,
        group_commit_max_bytes: 16 * 1024,
    };
    let clock = Arc::new(MonotonicClock::new());
    let (log, _) = SegmentLog::open(mount, log_config, Arc::clone(&clock) as _)
        .await
        .unwrap();
    let index = Index::open_sim(&disk.mount(), "index.redb", &IndexConfig::default()).unwrap();
    let pool = BlockingPool::new("index", NonZeroUsize::MIN).unwrap();
    let set = ShardSet::new(Arc::new(index), log, pool);
    let timing = ReplicationConfig::default();
    let peers = BTreeMap::from([(node(2), address.to_string().parse().unwrap())]);
    let primary = Replication::new(node(1), set, pki.transport(&node(1)), peers, clock, timing);
    let shard = primary.open(&config()).await.unwrap();
    while !shard.is_serving() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Records the member acknowledged, so they committed and left memory.
    for i in 0..20 {
        let body = RecordBody::Delete(Delete {
            key: format!("k{i}"),
        });
        shard.commit(body).await.unwrap();
    }
    let committed = shard.leader().unwrap().commit().get();
    assert_eq!(committed, 20);

    // The member forgets them; the next session reads them from the log,
    // slowly.
    slow.store(true, Ordering::SeqCst);
    forget.store(true, Ordering::SeqCst);
    while seen.sessions.load(Ordering::SeqCst) < 2 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    while seen.last.load(Ordering::SeqCst) < committed {
        assert_eq!(
            seen.sessions.load(Ordering::SeqCst),
            2,
            "the session ended while the link read the log"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    slow.store(false, Ordering::SeqCst);
}
