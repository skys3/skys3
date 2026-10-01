//! Measures group commit on a real disk: how many records share each
//! `fdatasync`, and the append throughput and latency that follow.
//!
//! ```text
//! cargo run --release -p skys3-log --example group_commit_bench -- \
//!     [--dir PATH] [--writers N] [--records N] [--payload BYTES] [--delay-us N]
//! ```
//!
//! Each of `writers` tasks appends `records` inline `PUT` records of
//! `payload` bytes, one at a time, as concurrent clients of one disk would.
//! The segment directory is a temporary directory unless `--dir` names one,
//! whose disk is then the one measured.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use skys3_io::{BlockingPool, MonotonicClock, RealDisk};
use skys3_log::record::{Put, PutData};
use skys3_log::{LogConfig, LogRecord, RecordBody, SegmentLog, ShardRef};
use skys3_types::{BucketId, ETag, Epoch, EpochSeq, Seq, ShardId};

struct Options {
    dir: Option<PathBuf>,
    writers: u64,
    records: u64,
    payload: usize,
    delay: Duration,
}

impl Options {
    fn parse() -> Result<Self, Box<dyn Error>> {
        let mut options = Options {
            dir: None,
            writers: 64,
            records: 200,
            payload: 4096,
            delay: LogConfig::default().group_commit_max_delay,
        };
        let mut args = env::args().skip(1);
        while let Some(flag) = args.next() {
            let value = args.next().ok_or(format!("{flag} needs a value"))?;
            match flag.as_str() {
                "--dir" => options.dir = Some(value.into()),
                "--writers" => options.writers = value.parse()?,
                "--records" => options.records = value.parse()?,
                "--payload" => options.payload = value.parse()?,
                "--delay-us" => options.delay = Duration::from_micros(value.parse()?),
                _ => return Err(format!("unknown flag {flag}").into()),
            }
        }
        Ok(options)
    }
}

fn record(writer: u64, seq: u64, payload: &Bytes) -> Result<LogRecord, Box<dyn Error>> {
    let shard_number = u8::try_from(writer % 256)?;
    Ok(LogRecord {
        shard: ShardRef::new(BucketId::new("bench")?, ShardId::new(shard_number)),
        position: EpochSeq::new(Epoch::new(1), Seq::new(seq)),
        body: RecordBody::Put(Put {
            key: format!("writer-{writer}/object-{seq}"),
            size: payload.len() as u64,
            last_modified_ms: 1_700_000_000_000,
            etag: ETag::new("d41d8cd98f00b204e9800998ecf8427e")?,
            inherited_identity: None,
            metadata: BTreeMap::new(),
            tags: BTreeMap::new(),
            checksums: BTreeMap::new(),
            copy_source: None,
            data: PutData::Inline(payload.clone()),
        }),
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let options = Options::parse()?;
    let temp = tempfile::tempdir()?;
    let dir = options.dir.clone().unwrap_or_else(|| temp.path().into());
    let pool = BlockingPool::new("bench-disk", NonZeroUsize::new(4).ok_or("zero threads")?)?;
    let disk = RealDisk::open(dir.join("segments"), pool).await?;
    let config = LogConfig {
        group_commit_max_delay: options.delay,
        ..LogConfig::default()
    };
    let (log, _) = SegmentLog::open(disk, config, Arc::new(MonotonicClock::new())).await?;

    let payload = Bytes::from(vec![0x5a; options.payload]);
    let start = Instant::now();
    let mut tasks = Vec::new();
    for writer in 0..options.writers {
        let log = log.clone();
        let payload = payload.clone();
        let records = options.records;
        tasks.push(tokio::spawn(async move {
            let mut latency = Duration::ZERO;
            for seq in 0..records {
                let record = record(writer, seq, &payload).map_err(|e| e.to_string())?;
                let appended = Instant::now();
                log.append(&record).await.map_err(|e| e.to_string())?;
                latency += appended.elapsed();
            }
            Ok::<_, String>(latency)
        }));
    }
    let mut latency = Duration::ZERO;
    for task in tasks {
        latency += task.await??;
    }
    let elapsed = start.elapsed();

    let stats = log.stats();
    let records = stats.records as f64;
    println!(
        "writers {}, records {}, payload {} B, group_commit_max_delay {:?}",
        options.writers, stats.records, options.payload, options.delay
    );
    println!("group commits        {}", stats.group_commits);
    println!("records per commit   {:.2}", stats.records_per_commit());
    println!(
        "bytes per commit     {:.0}",
        stats.bytes as f64 / stats.group_commits.max(1) as f64
    );
    println!(
        "records per second   {:.0}",
        records / elapsed.as_secs_f64()
    );
    println!(
        "mean append latency  {:?}",
        latency.div_f64(records.max(1.0))
    );
    Ok(())
}
