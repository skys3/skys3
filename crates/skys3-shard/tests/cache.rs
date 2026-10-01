//! The node-local cache transitions of §4.2, which no record makes:
//! Evicted → Clean by a read-through fill (§9.2), and Clean → Evicted by
//! eviction (§9.3), each refused once the entry holds another version.

mod support;

use std::sync::Arc;

use skys3_index::{EntryState, Index, Payload};
use skys3_io::SimDisk;
use skys3_log::RecordBody;
use skys3_log::record::ExtentRef;
use skys3_shard::cache::{self, CacheRefusal};
use skys3_shard::{Effect, Outcome, Shard, ShardError};
use skys3_types::EpochSeq;
use support::{
    adopt, apply, at, delete, dump, entry, extent, flushed, import, index_config, mpu_complete,
    mpu_create, mpu_part, new_index, open_log, pool, put, record, runtime, shard,
};

/// Applies the body at `seq` and returns its outcome.
fn step(index: &Index, seq: u64, body: RecordBody) -> Outcome {
    apply(index, &record(at(seq), body)).expect("a new position is applied")
}

/// Fills `version` of `k` with `extents`.
fn fill(index: &Index, version: EpochSeq, extents: Vec<ExtentRef>) -> Result<(), CacheRefusal> {
    index
        .update_local(|writer| {
            cache::fill(writer, &shard(0), "k", version, Payload::Extents(extents))
        })
        .unwrap()
}

/// Evicts `version` of `k`.
fn evict(index: &Index, version: EpochSeq) -> Result<(), CacheRefusal> {
    index
        .update_local(|writer| cache::evict(writer, &shard(0), "k", version))
        .unwrap()
}

/// The `EXTENT` records at `seqs` holding 30 and 12 bytes: the 42 bytes
/// of an imported stub.
fn extents(index: &Index, seqs: [u64; 2]) -> Vec<ExtentRef> {
    let lens = [30, 12];
    for (seq, (offset, len)) in seqs.into_iter().zip([(0, 30), (30, 12)]) {
        step(index, seq, RecordBody::Extent(extent("k", offset, len)));
    }
    seqs.into_iter()
        .zip(lens)
        .map(|(seq, len)| ExtentRef {
            position: at(seq),
            len,
        })
        .collect()
}

#[test]
fn a_fill_makes_an_evicted_entry_clean_with_its_extents() {
    let (_, index) = new_index();
    step(&index, 1, import("k", 5));
    let filled = extents(&index, [2, 3]);
    let before = dump(&index);
    fill(&index, at(1), filled.clone()).unwrap();

    let entry = entry(&index, "k").unwrap();
    assert_eq!((entry.version, entry.state), (at(1), EntryState::Clean));
    assert_eq!(entry.object.unwrap().payload, Payload::Extents(filled));
    let after = dump(&index);
    assert_eq!(after.applied, before.applied, "no record was applied");

    // The filled entry is clean like any other: a duplicate `FLUSHED` is
    // rejected, an `ADOPT` of its version applies, and so does a write.
    assert_eq!(
        step(&index, 4, flushed("k", 1, false)),
        Outcome::Rejected(skys3_shard::Rejection::AlreadyClean)
    );
    assert_eq!(
        step(&index, 5, adopt("k", 1, 9)),
        Outcome::Applied(Effect::Adopted)
    );
    assert_eq!(entry_state(&index), EntryState::Evicted);
}

fn entry_state(index: &Index) -> EntryState {
    entry(index, "k").unwrap().state
}

#[test]
fn a_fill_is_refused_once_the_entry_changed() {
    let (_, index) = new_index();
    assert_eq!(fill(&index, at(1), Vec::new()), Err(CacheRefusal::NoEntry));
    step(&index, 1, import("k", 5));
    let filled = extents(&index, [2, 3]);
    // The fill named another version.
    assert_eq!(
        fill(&index, at(4), filled.clone()),
        Err(CacheRefusal::VersionChanged {
            expected: at(4),
            current: at(1),
        })
    );
    // Its bytes do not add up to the object.
    assert_eq!(
        fill(&index, at(1), filled[..1].to_vec()),
        Err(CacheRefusal::Payload)
    );
    let inline = index
        .update_local(|writer| cache::fill(writer, &shard(0), "k", at(1), Payload::Inline(at(2))))
        .unwrap();
    assert_eq!(inline, Err(CacheRefusal::Payload));
    // Another fill was first.
    fill(&index, at(1), filled.clone()).unwrap();
    assert_eq!(
        fill(&index, at(1), filled.clone()),
        Err(CacheRefusal::State(EntryState::Clean))
    );
    // A local write committed while the fill read the remote.
    step(&index, 4, put("k", 10, 4));
    assert_eq!(
        fill(&index, at(4), filled.clone()),
        Err(CacheRefusal::State(EntryState::Dirty))
    );
    step(&index, 5, delete("k"));
    assert_eq!(
        fill(&index, at(5), filled),
        Err(CacheRefusal::State(EntryState::Dirty))
    );
}

#[test]
fn eviction_drops_a_clean_payload_and_a_fill_restores_it() {
    let (_, index) = new_index();
    step(&index, 1, put("k", 42, 1));
    // Dirty payload is never evicted.
    assert_eq!(
        evict(&index, at(1)),
        Err(CacheRefusal::State(EntryState::Dirty))
    );
    step(&index, 2, flushed("k", 1, false));
    assert_eq!(
        evict(&index, at(2)),
        Err(CacheRefusal::VersionChanged {
            expected: at(2),
            current: at(1),
        })
    );
    evict(&index, at(1)).unwrap();
    let stub = entry(&index, "k").unwrap();
    assert_eq!((stub.version, stub.state), (at(1), EntryState::Evicted));
    assert_eq!(stub.remote_etag, Some(support::etag(1001)));
    assert_eq!(stub.object.as_ref().unwrap().payload, Payload::None);
    assert_eq!(
        evict(&index, at(1)),
        Err(CacheRefusal::State(EntryState::Evicted))
    );

    let filled = extents(&index, [3, 4]);
    fill(&index, at(1), filled.clone()).unwrap();
    let entry = entry(&index, "k").unwrap();
    assert_eq!((entry.version, entry.state), (at(1), EntryState::Clean));
    assert_eq!(entry.object.unwrap().payload, Payload::Extents(filled));
    assert_eq!(evict(&index, at(1)), Ok(()));

    let (_, index) = new_index();
    assert_eq!(evict(&index, at(1)), Err(CacheRefusal::NoEntry));
}

#[test]
fn a_multipart_object_is_not_evicted() {
    let (_, index) = new_index();
    step(&index, 1, mpu_create("k"));
    step(&index, 2, mpu_part("k", at(1), 1, 5, Vec::new()));
    step(&index, 3, mpu_complete("k", at(1), &[(1, at(2))], 5));
    step(&index, 4, flushed("k", 3, false));
    assert_eq!(evict(&index, at(3)), Err(CacheRefusal::Multipart));
    assert_eq!(entry_state(&index), EntryState::Clean);
}

#[test]
fn a_shard_fills_and_evicts_its_entries() {
    runtime().block_on(async {
        let disk = SimDisk::new(3);
        let log = open_log(disk.mount()).await;
        let index =
            Arc::new(Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap());
        let shard = Shard::open(&support::config(&shard(0), 1), log, index, pool())
            .await
            .unwrap();
        let stub = shard.commit(import("k", 5)).await.unwrap().position;
        let mut filled = Vec::new();
        for (offset, len) in [(0, 30), (30, 12)] {
            filled.push(shard.append_extent(extent("k", offset, len)).await.unwrap());
        }
        let payload = Payload::Extents(filled.clone());
        shard
            .fill("k", stub, payload.clone())
            .await
            .unwrap()
            .unwrap();
        let entry = shard.entry("k").await.unwrap().unwrap();
        assert_eq!(entry.state, EntryState::Clean);
        assert_eq!(entry.object.unwrap().payload, payload);
        assert_eq!(
            shard.fill("k", stub, payload).await.unwrap(),
            Err(CacheRefusal::State(EntryState::Clean))
        );
        shard.evict("k", stub).await.unwrap().unwrap();
        assert_eq!(
            shard.entry("k").await.unwrap().unwrap().state,
            EntryState::Evicted
        );
        // The filled bytes stay readable until compaction reclaims them.
        assert_eq!(
            shard.payload(filled[1].position).await.unwrap(),
            support::fill(12)
        );

        shard.close().await.unwrap();
        assert!(matches!(
            shard.evict("k", stub).await,
            Err(ShardError::Unavailable { .. })
        ));
    });
}

#[test]
fn refusals_describe_themselves() {
    assert_eq!(
        CacheRefusal::VersionChanged {
            expected: at(2),
            current: at(1),
        }
        .to_string(),
        "the entry is at 1.1, not 1.2"
    );
    assert_eq!(
        CacheRefusal::State(EntryState::Dirty).to_string(),
        "the entry is Dirty"
    );
    assert_eq!(
        CacheRefusal::Multipart.to_string(),
        "multipart objects are not evicted"
    );
}
