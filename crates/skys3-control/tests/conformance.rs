//! Runs the conformance suite against the in-memory and file backends.

use std::num::NonZeroUsize;
use std::time::Duration;

use bytes::Bytes;
use skys3_control::conformance::{self, Backend};
use skys3_control::{
    ChangeFeed, ChangeStream, ControlError, ControlStore, DeleteOutcome, Expected,
    FileControlStore, FileStoreConfig, KeyPrefix, MemoryControlStore, ProposalIds, PutOutcome,
    RegisterKey, RetryPolicy, Version, Versioned, bootstrap, bump_generation,
};
use skys3_io::BlockingPool;
use skys3_types::Generation;
use tempfile::TempDir;

struct Memory;

impl Backend for Memory {
    type Store = MemoryControlStore;

    async fn store(&mut self) -> MemoryControlStore {
        MemoryControlStore::new()
    }
}

/// The in-memory store with a change stream that polls, as the S3 backend
/// will.
#[derive(Debug, Clone)]
struct Polling(MemoryControlStore);

impl ControlStore for Polling {
    type Changes = ChangeFeed<MemoryControlStore>;

    async fn get(&self, key: &RegisterKey) -> Result<Option<Versioned>, ControlError> {
        self.0.get(key).await
    }

    async fn put_if(
        &self,
        key: &RegisterKey,
        expected: Expected,
        value: Bytes,
    ) -> Result<PutOutcome, ControlError> {
        self.0.put_if(key, expected, value).await
    }

    async fn delete_if(
        &self,
        key: &RegisterKey,
        expected: &Version,
    ) -> Result<DeleteOutcome, ControlError> {
        self.0.delete_if(key, expected).await
    }

    async fn list(&self, prefix: &KeyPrefix) -> Result<Vec<(RegisterKey, Version)>, ControlError> {
        self.0.list(prefix).await
    }

    async fn changes(&self, after: Generation) -> Result<Self::Changes, ControlError> {
        Ok(ChangeFeed::polling(
            self.0.clone(),
            after,
            Duration::from_secs(30),
        ))
    }
}

struct PollingBackend;

impl Backend for PollingBackend {
    type Store = Polling;

    async fn store(&mut self) -> Polling {
        Polling(MemoryControlStore::new())
    }
}

struct File {
    pool: BlockingPool,
    dirs: Vec<TempDir>,
}

impl Backend for File {
    type Store = FileControlStore;

    async fn store(&mut self) -> FileControlStore {
        let dir = tempfile::tempdir().unwrap();
        let config = FileStoreConfig {
            root: dir.path().join("control"),
            node: "node-1".parse().unwrap(),
        };
        self.dirs.push(dir);
        FileControlStore::open(config, self.pool.clone())
            .await
            .unwrap()
    }
}

#[tokio::test(start_paused = true)]
async fn the_memory_store_conforms() {
    conformance::run(&mut Memory).await;
}

#[tokio::test(start_paused = true)]
async fn a_polling_change_stream_conforms() {
    conformance::run(&mut PollingBackend).await;
}

// Real time: the file store's I/O runs on pool threads, and a paused clock
// would jump ahead while the runtime waits for them.
#[tokio::test]
async fn the_file_store_conforms() {
    let pool = BlockingPool::new("control", NonZeroUsize::new(2).unwrap()).unwrap();
    conformance::run(&mut File {
        pool,
        dirs: Vec::new(),
    })
    .await;
}

#[tokio::test]
async fn the_file_store_keeps_generations_across_reopening() {
    let dir = tempfile::tempdir().unwrap();
    let pool = BlockingPool::new("control", NonZeroUsize::MIN).unwrap();
    let config = FileStoreConfig {
        root: dir.path().to_path_buf(),
        node: "node-1".parse().unwrap(),
    };
    let cluster = "dev".parse().unwrap();
    let policy = RetryPolicy::default();
    let mut ids = ProposalIds::seeded(0);
    let store = FileControlStore::open(config.clone(), pool.clone())
        .await
        .unwrap();
    bootstrap(&store, &cluster, ids.next_id(), &policy)
        .await
        .unwrap();
    bump_generation(&store, &cluster, &mut ids, &policy)
        .await
        .unwrap();
    drop(store);
    let store = FileControlStore::open(config, pool).await.unwrap();
    let mut changes = store.changes(Generation::ZERO).await.unwrap();
    let change = changes.next().await.unwrap();
    assert_eq!(change.generation, Generation::new(2));
    assert!(change.snapshot);
}
