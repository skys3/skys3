//! Index snapshot objects: their encoding and their keys at the snapshot
//! target (§8.9).
//!
//! See [`Snapshot`] for the layout of an object and its key.

use md5::{Digest, Md5};
use skys3_index::{ShardRow, ShardTable};
use skys3_log::ShardRef;
use skys3_types::{BucketId, Epoch, EpochSeq, Seq, ShardId};

/// The directory of every shard's snapshots inside a snapshot target's
/// prefix. Namespace imports skip it (§9.1).
pub const SNAPSHOT_DIR: &str = ".skys3-snapshots/";

/// The format version this build writes and reads.
pub const FORMAT: u16 = 1;

const MAGIC: &[u8; 8] = b"SKYS3IXS";
const TRAILER: usize = 16;

/// What a snapshot holds of its shard (§8.9).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Contents {
    /// A `write_back` bucket's: the entries that are not clean (dirty,
    /// being flushed, or in conflict, tombstones included), the open
    /// multipart uploads and the parts of those and of unclean multipart
    /// objects, and the streaming flush's remote uploads. The rest of the
    /// index can be imported from the remote again (§9.1).
    Unflushed,
    /// A `local` bucket's: every row of the shard.
    Full,
}

impl Contents {
    /// The code in an encoded snapshot.
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::Unflushed => 1,
            Self::Full => 2,
        }
    }

    fn from_code(code: u8) -> Option<Self> {
        [Self::Unflushed, Self::Full]
            .into_iter()
            .find(|contents| contents.code() == code)
    }
}

/// A chain of snapshots: a base and the deltas after it.
///
/// Chains order by the epoch their primary sequenced in, then by the
/// base's position and time: the latest chain of a shard is the greatest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChainId {
    /// The epoch the primary that wrote the chain sequenced in.
    pub epoch: Epoch,
    /// The applied position the base was taken at.
    pub base: EpochSeq,
    /// When the base was taken, in milliseconds since the Unix epoch.
    pub taken_ms: u64,
}

/// A row's identity in a delta's list of removed rows: the MD5 of its
/// table's code (`u32`, big-endian) and its key.
pub type RowDigest = [u8; 16];

/// The digest of the row of `table` at `key`.
#[must_use]
pub fn row_digest(table: ShardTable, key: &[u8]) -> RowDigest {
    let mut md5 = Md5::new();
    md5.update(table.code().to_be_bytes());
    md5.update(key);
    md5.finalize().into()
}

/// A digest of a row's value, to tell whether the row changed.
pub(crate) fn value_digest(value: &[u8]) -> u64 {
    let digest: [u8; 16] = Md5::digest(value).into();
    let mut first = [0; 8];
    first.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(first)
}

/// One snapshot object: a base, or a delta of its chain.
///
/// # Keys
///
/// A shard's snapshots live under
/// `<prefix>.skys3-snapshots/<bucket ID>/<shard>/`, where `<prefix>` is
/// the snapshot target's. Each object is one snapshot of a **chain**: a
/// base, numbered 0, and the deltas a primary wrote after it, numbered
/// from 1. The key is `<chain>/<number>`, in fixed-width lowercase hex, so
/// keys sort as chains and numbers do:
///
/// ```text
/// <dir><epoch:016x>-<base epoch:016x>-<base seq:016x>-<taken ms:016x>/<number:08x>
/// ```
///
/// [`ChainId`] names a chain by the epoch its primary sequenced in, the
/// position its base was taken at, and when. A later primary sequences in
/// a later epoch, so its chains sort after every chain of an earlier one.
///
/// # Encoding
///
/// An object is a header, the rows, the digests of removed rows, and a
/// trailer, every integer big-endian:
///
/// | Field | Encoding |
/// |---|---|
/// | magic | the 8 bytes `SKYS3IXS` |
/// | format | `u16`, [`FORMAT`] |
/// | index format | `u64`: the index format of the rows (`skys3_index::FORMAT_VERSION`) |
/// | contents | `u8`: [`Contents::code`] |
/// | bucket ID | `u16` length, then UTF-8 |
/// | shard | `u8` |
/// | chain | epoch `u64`, base epoch `u64`, base seq `u64`, taken ms `u64` |
/// | number | `u32` |
/// | position | epoch `u64`, seq `u64`: the applied position the rows were read at |
/// | taken | `u64`: milliseconds since the Unix epoch, read before the rows |
/// | rows | `u64` count; each: table `u32` ([`ShardTable::code`]), key `u32` length and bytes, value `u32` length and bytes |
/// | removed | `u64` count; each: a 16-byte [`RowDigest`] |
/// | trailer | the MD5 of every byte before it |
///
/// Rows are stored as the index stores them (`skys3_index::codec`), as a
/// learner's snapshot sends them (§6.7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// The shard.
    pub shard: ShardRef,
    /// What the snapshot holds of the shard.
    pub contents: Contents,
    /// The chain.
    pub chain: ChainId,
    /// 0 for the base; the deltas count from 1.
    pub number: u32,
    /// The shard's applied position the rows were read at.
    pub position: EpochSeq,
    /// When the snapshot was taken, in milliseconds since the Unix epoch,
    /// on the primary's clock: read before the rows, so that every write
    /// acknowledged before it is in them.
    pub taken_ms: u64,
    /// A base's rows, or a delta's rows that are new or changed since the
    /// previous snapshot of the chain, in table and key order.
    pub rows: Vec<(ShardTable, ShardRow)>,
    /// The digests of the rows a delta's predecessor held that are gone.
    pub removed: Vec<RowDigest>,
}

/// Why a snapshot object does not decode.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FormatError {
    /// The object ends early, or a length runs past its end.
    #[error("the snapshot is truncated")]
    Truncated,
    /// The object does not start with the magic bytes.
    #[error("not a snapshot object")]
    Magic,
    /// The object's format, or its rows' index format, is newer than this
    /// build reads.
    #[error("unsupported snapshot format {format} (index format {index})")]
    Unsupported {
        /// The snapshot format.
        format: u16,
        /// The rows' index format.
        index: u64,
    },
    /// The trailer does not match the contents.
    #[error("the snapshot's checksum does not match")]
    Checksum,
    /// A field holds a value it cannot.
    #[error("invalid snapshot: {0}")]
    Invalid(&'static str),
    /// Bytes follow the removed rows.
    #[error("the snapshot has trailing bytes")]
    Trailing,
}

impl Snapshot {
    /// The object's bytes.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let rows: usize = self
            .rows
            .iter()
            .map(|(_, (key, value))| 12 + key.len() + value.len())
            .sum();
        let mut out = Vec::with_capacity(128 + rows + 16 * self.removed.len());
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&FORMAT.to_be_bytes());
        out.extend_from_slice(&skys3_index::FORMAT_VERSION.to_be_bytes());
        out.push(self.contents.code());
        let bucket = self.shard.bucket.as_str().as_bytes();
        // Bucket IDs are far shorter than 64 KiB.
        out.extend_from_slice(
            &u16::try_from(bucket.len())
                .unwrap_or(u16::MAX)
                .to_be_bytes(),
        );
        out.extend_from_slice(bucket);
        out.push(self.shard.shard.get());
        out.extend_from_slice(&self.chain.epoch.get().to_be_bytes());
        put_position(&mut out, self.chain.base);
        out.extend_from_slice(&self.chain.taken_ms.to_be_bytes());
        out.extend_from_slice(&self.number.to_be_bytes());
        put_position(&mut out, self.position);
        out.extend_from_slice(&self.taken_ms.to_be_bytes());
        out.extend_from_slice(&(self.rows.len() as u64).to_be_bytes());
        for (table, (key, value)) in &self.rows {
            out.extend_from_slice(&table.code().to_be_bytes());
            put_bytes(&mut out, key);
            put_bytes(&mut out, value);
        }
        out.extend_from_slice(&(self.removed.len() as u64).to_be_bytes());
        for digest in &self.removed {
            out.extend_from_slice(digest);
        }
        let trailer: [u8; 16] = Md5::digest(&out).into();
        out.extend_from_slice(&trailer);
        out
    }

    /// Decodes an object's bytes.
    ///
    /// # Errors
    ///
    /// [`FormatError`] if the bytes are not a whole snapshot of a format
    /// this build reads. The rows themselves are not decoded.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let body_len = bytes
            .len()
            .checked_sub(TRAILER)
            .ok_or(FormatError::Truncated)?;
        let (body, trailer) = bytes.split_at(body_len);
        let mut reader = Reader(body);
        if reader.take(MAGIC.len())? != MAGIC {
            return Err(FormatError::Magic);
        }
        let format = reader.u16()?;
        let index = reader.u64()?;
        if format != FORMAT || index > skys3_index::FORMAT_VERSION {
            return Err(FormatError::Unsupported { format, index });
        }
        if <[u8; 16]>::from(Md5::digest(body)) != trailer {
            return Err(FormatError::Checksum);
        }
        let contents = Contents::from_code(reader.u8()?).ok_or(FormatError::Invalid("contents"))?;
        let bucket_len = usize::from(reader.u16()?);
        let bucket = std::str::from_utf8(reader.take(bucket_len)?)
            .ok()
            .and_then(|bucket| BucketId::new(bucket).ok())
            .ok_or(FormatError::Invalid("bucket ID"))?;
        let shard = ShardRef::new(bucket, ShardId::new(reader.u8()?));
        let chain = ChainId {
            epoch: Epoch::new(reader.u64()?),
            base: reader.position()?,
            taken_ms: reader.u64()?,
        };
        let number = reader.u32()?;
        let position = reader.position()?;
        let taken_ms = reader.u64()?;
        let count = reader.count(12)?;
        let mut rows = Vec::with_capacity(count);
        for _ in 0..count {
            let table =
                ShardTable::from_code(reader.u32()?).ok_or(FormatError::Invalid("table"))?;
            let key = reader.bytes()?.to_vec();
            let value = reader.bytes()?.to_vec();
            rows.push((table, (key, value)));
        }
        let count = reader.count(16)?;
        let mut removed = Vec::with_capacity(count);
        for _ in 0..count {
            let mut digest = [0; 16];
            digest.copy_from_slice(reader.take(16)?);
            removed.push(digest);
        }
        if !reader.0.is_empty() {
            return Err(FormatError::Trailing);
        }
        let snapshot = Self {
            shard,
            contents,
            chain,
            number,
            position,
            taken_ms,
            rows,
            removed,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    /// Checks what one snapshot can say about itself.
    fn validate(&self) -> Result<(), FormatError> {
        let base = self.number == 0;
        if base && (self.position != self.chain.base || self.taken_ms != self.chain.taken_ms) {
            return Err(FormatError::Invalid(
                "a base taken elsewhere than its chain",
            ));
        }
        if base && !self.removed.is_empty() {
            return Err(FormatError::Invalid("a base with removed rows"));
        }
        if self.position < self.chain.base || self.taken_ms < self.chain.taken_ms {
            return Err(FormatError::Invalid("a delta taken before its base"));
        }
        Ok(())
    }
}

fn put_position(out: &mut Vec<u8>, position: EpochSeq) {
    out.extend_from_slice(&position.epoch.get().to_be_bytes());
    out.extend_from_slice(&position.seq.get().to_be_bytes());
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    // Index keys and values are far shorter than 4 GiB.
    out.extend_from_slice(&u32::try_from(bytes.len()).unwrap_or(u32::MAX).to_be_bytes());
    out.extend_from_slice(bytes);
}

/// Reads fields from the front of a byte slice.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], FormatError> {
        if self.0.len() < len {
            return Err(FormatError::Truncated);
        }
        let (taken, rest) = self.0.split_at(len);
        self.0 = rest;
        Ok(taken)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], FormatError> {
        let mut array = [0; N];
        array.copy_from_slice(self.take(N)?);
        Ok(array)
    }

    fn u8(&mut self) -> Result<u8, FormatError> {
        Ok(self.array::<1>()?[0])
    }

    fn u16(&mut self) -> Result<u16, FormatError> {
        self.array().map(u16::from_be_bytes)
    }

    fn u32(&mut self) -> Result<u32, FormatError> {
        self.array().map(u32::from_be_bytes)
    }

    fn u64(&mut self) -> Result<u64, FormatError> {
        self.array().map(u64::from_be_bytes)
    }

    fn position(&mut self) -> Result<EpochSeq, FormatError> {
        Ok(EpochSeq::new(
            Epoch::new(self.u64()?),
            Seq::new(self.u64()?),
        ))
    }

    fn bytes(&mut self) -> Result<&'a [u8], FormatError> {
        let len = usize::try_from(self.u32()?).map_err(|_| FormatError::Truncated)?;
        self.take(len)
    }

    /// A count of items that take at least `min_len` bytes each, checked
    /// against what is left, so that no count allocates more than the
    /// object could hold.
    fn count(&mut self, min_len: usize) -> Result<usize, FormatError> {
        let count = usize::try_from(self.u64()?).map_err(|_| FormatError::Truncated)?;
        if count.saturating_mul(min_len) > self.0.len() {
            return Err(FormatError::Truncated);
        }
        Ok(count)
    }
}

/// The key prefix of `shard`'s snapshots in a target whose prefix is
/// `prefix`.
#[must_use]
pub fn shard_dir(prefix: &str, shard: &ShardRef) -> String {
    format!("{prefix}{SNAPSHOT_DIR}{}/{}/", shard.bucket, shard.shard)
}

/// The key of snapshot `number` of `chain` under `dir` ([`shard_dir`]).
#[must_use]
pub fn object_key(dir: &str, chain: &ChainId, number: u32) -> String {
    format!(
        "{dir}{:016x}-{:016x}-{:016x}-{:016x}/{number:08x}",
        chain.epoch.get(),
        chain.base.epoch.get(),
        chain.base.seq.get(),
        chain.taken_ms
    )
}

/// The chain and number a key under `dir` names, or `None` for a key that
/// is not a snapshot's.
#[must_use]
pub fn parse_object_key(dir: &str, key: &str) -> Option<(ChainId, u32)> {
    let (chain, number) = key.strip_prefix(dir)?.split_once('/')?;
    let hex = |text: &str, width: usize| {
        (text.len() == width && text.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')))
            .then(|| u64::from_str_radix(text, 16).ok())
            .flatten()
    };
    let mut fields = chain.split('-');
    let mut next = || hex(fields.next()?, 16);
    let id = ChainId {
        epoch: Epoch::new(next()?),
        base: EpochSeq::new(Epoch::new(next()?), Seq::new(next()?)),
        taken_ms: next()?,
    };
    if fields.next().is_some() {
        return None;
    }
    let number = u32::try_from(hex(number, 8)?).ok()?;
    Some((id, number))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn shard() -> ShardRef {
        ShardRef::new(BucketId::new("b-snap").unwrap(), ShardId::new(3))
    }

    fn at(epoch: u64, seq: u64) -> EpochSeq {
        EpochSeq::new(Epoch::new(epoch), Seq::new(seq))
    }

    fn sample() -> Snapshot {
        let chain = ChainId {
            epoch: Epoch::new(4),
            base: at(3, 17),
            taken_ms: 1_000,
        };
        Snapshot {
            shard: shard(),
            contents: Contents::Full,
            chain,
            number: 2,
            position: at(4, 9),
            taken_ms: 3_000,
            rows: vec![
                (ShardTable::Namespace, (b"k".to_vec(), b"v".to_vec())),
                (ShardTable::Parts, (Vec::new(), vec![0; 300])),
            ],
            removed: vec![row_digest(ShardTable::Uploads, b"gone")],
        }
    }

    #[test]
    fn a_snapshot_round_trips() {
        let snapshot = sample();
        assert_eq!(Snapshot::decode(&snapshot.encode()), Ok(snapshot));
    }

    #[test]
    fn damage_is_refused() {
        let bytes = sample().encode();
        for cut in 0..bytes.len() {
            assert!(Snapshot::decode(&bytes[..cut]).is_err(), "cut at {cut}");
        }
        let mut flipped = bytes.clone();
        flipped[40] ^= 1;
        assert_eq!(Snapshot::decode(&flipped), Err(FormatError::Checksum));
        let mut magic = bytes.clone();
        magic[0] = b'X';
        assert_eq!(Snapshot::decode(&magic), Err(FormatError::Magic));
        let mut newer = bytes;
        newer[9] = 2;
        assert!(matches!(
            Snapshot::decode(&newer),
            Err(FormatError::Unsupported { format: 2, .. })
        ));
    }

    #[test]
    fn a_base_must_be_its_chain_base() {
        let mut base = sample();
        base.number = 0;
        assert!(matches!(
            Snapshot::decode(&base.encode()),
            Err(FormatError::Invalid(_))
        ));
        base.position = base.chain.base;
        base.taken_ms = base.chain.taken_ms;
        assert!(matches!(
            Snapshot::decode(&base.encode()),
            Err(FormatError::Invalid(_))
        ));
        base.removed.clear();
        assert_eq!(Snapshot::decode(&base.encode()), Ok(base.clone()));
        let mut early = sample();
        early.taken_ms = 10;
        assert!(Snapshot::decode(&early.encode()).is_err());
    }

    #[test]
    fn keys_sort_as_chains_and_numbers() {
        let dir = shard_dir("backup/", &shard());
        assert_eq!(dir, "backup/.skys3-snapshots/b-snap/3/");
        let chain = sample().chain;
        let key = object_key(&dir, &chain, 7);
        assert_eq!(parse_object_key(&dir, &key), Some((chain, 7)));
        let later = ChainId {
            epoch: Epoch::new(5),
            ..chain
        };
        assert!(object_key(&dir, &later, 0) > object_key(&dir, &chain, 7));
        assert!(object_key(&dir, &chain, 0x10) > object_key(&dir, &chain, 0xf));
        for bad in [
            "elsewhere/x",
            &format!("{dir}0000000000000004-0/00000007"),
            &format!("{key}0"),
            &format!("{}A", &key[..key.len() - 1]),
            &format!(
                "{dir}0000000000000004-0000000000000003-0000000000000011-00000000000003e8-1/00000007"
            ),
        ] {
            assert_eq!(parse_object_key(&dir, bad), None, "{bad}");
        }
    }

    fn row() -> impl Strategy<Value = (ShardTable, ShardRow)> {
        (
            prop::sample::select(ShardTable::ALL.to_vec()),
            prop::collection::vec(any::<u8>(), 0..40),
            prop::collection::vec(any::<u8>(), 0..80),
        )
            .prop_map(|(table, key, value)| (table, (key, value)))
    }

    proptest! {
        #[test]
        fn every_snapshot_round_trips(
            full in any::<bool>(),
            epoch in any::<u64>(),
            base in (any::<u64>(), any::<u64>()),
            number in 1..u32::MAX,
            ahead in (0..1000u64, 0..1000u64),
            rows in prop::collection::vec(row(), 0..20),
            removed in prop::collection::vec(any::<[u8; 16]>(), 0..20),
        ) {
            let base = at(base.0 / 2, base.1 / 2);
            let chain = ChainId { epoch: Epoch::new(epoch), base, taken_ms: 5 };
            let snapshot = Snapshot {
                shard: shard(),
                contents: if full { Contents::Full } else { Contents::Unflushed },
                chain,
                number,
                position: at(base.epoch.get() + ahead.0, base.seq.get()),
                taken_ms: 5 + ahead.1,
                rows,
                removed,
            };
            prop_assert_eq!(Snapshot::decode(&snapshot.encode()), Ok(snapshot));
        }

        #[test]
        fn decoding_any_bytes_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..200)) {
            let _ = Snapshot::decode(&bytes);
        }
    }
}
