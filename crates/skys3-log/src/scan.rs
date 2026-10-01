//! Reading a segment's records in order.

use std::io;
use std::sync::Arc;

use bytes::Bytes;
use skys3_io::SegmentFile;

use crate::record::{DecodeError, LogRecord, MAGIC, RecordHeader};
use crate::segment::{RecordLocation, SegmentId};

/// How much a scanner reads at a time, unless a record needs more.
const CHUNK_LEN: usize = 1 << 20;

/// One record found by a [`SegmentScanner`], verified up to its fixed
/// header and CRC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScannedRecord {
    /// Where the record is stored.
    pub location: RecordLocation,
    /// The record's fixed header, decoded and verified, with the CRC of the
    /// whole record checked.
    pub header: RecordHeader,
    /// The record's bytes, header and payload.
    pub bytes: Bytes,
}

impl ScannedRecord {
    /// Decodes the record's body.
    ///
    /// # Errors
    ///
    /// Returns the [`DecodeError`] if the body breaks the format. The CRC
    /// already verified, so the error's class is
    /// [`ErrorClass::Invalid`](crate::record::ErrorClass::Invalid): a faulty
    /// writer, not a torn write.
    pub fn decode(&self) -> Result<LogRecord, DecodeError> {
        LogRecord::decode(&self.bytes).map(|(record, _)| record)
    }
}

/// Why a scan stopped before the end of a segment.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ScanError {
    /// Reading the segment failed.
    #[error("cannot read segment {segment}: {source}")]
    Io {
        /// The segment.
        segment: SegmentId,
        /// The read error.
        source: io::Error,
    },
    /// The bytes at `offset` are not a record this build can verify.
    #[error("segment {segment} has no valid record at offset {offset}: {source}")]
    Decode {
        /// The segment.
        segment: SegmentId,
        /// Where the bad record starts.
        offset: u64,
        /// What is wrong with it.
        source: DecodeError,
    },
}

/// Reads the records of one segment from its start, in order.
///
/// A scanner covers a range fixed when it is created: by default from the
/// segment's start to its length then.
/// Every record it returns has a verified fixed header and CRC. It stops at
/// the first bytes that are not such a record and reports them as a
/// [`ScanError::Decode`] whose offset is where the valid records end.
#[derive(Debug)]
pub struct SegmentScanner<F> {
    file: Arc<F>,
    segment: SegmentId,
    /// Where the next record starts.
    offset: u64,
    /// The end of the scan: the segment's length when the scan started.
    end: u64,
    /// Bytes read ahead, starting at `buf_start`.
    buf: Bytes,
    buf_start: u64,
}

impl<F: SegmentFile> SegmentScanner<F> {
    /// Scans `file`, which holds segment `segment`, from offset `start` to
    /// offset `end`, at most its length.
    pub(crate) fn new(file: Arc<F>, segment: SegmentId, start: u64, end: u64) -> Self {
        Self {
            file,
            segment,
            offset: start,
            end,
            buf: Bytes::new(),
            buf_start: start,
        }
    }

    /// Returns the segment being scanned.
    #[must_use]
    pub fn segment(&self) -> SegmentId {
        self.segment
    }

    /// Returns the offset just past the last record returned: the length
    /// of the segment's valid prefix so far.
    #[must_use]
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Returns the offset where the scan ends.
    #[must_use]
    pub fn end(&self) -> u64 {
        self.end
    }

    /// Returns the next record, or `None` at the end of the segment.
    ///
    /// # Errors
    ///
    /// Returns a [`ScanError`] if a read fails or the next bytes are not a
    /// verifiable record. The scanner does not move past them, so every
    /// later call returns an error too.
    pub async fn next(&mut self) -> Result<Option<ScannedRecord>, ScanError> {
        if self.offset >= self.end {
            return Ok(None);
        }
        let fixed = self.fill(RecordHeader::LEN).await?;
        let record_len = RecordHeader::peek_len(&fixed).map_err(|e| self.decode_error(e))?;
        let record = self.fill(record_len).await?;
        let header = RecordHeader::decode(&record).map_err(|e| self.decode_error(e))?;
        let location = RecordLocation {
            segment: self.segment,
            offset: self.offset,
            // A record is at most `MAX_RECORD_LEN` bytes, which fits a `u32`.
            len: u32::try_from(record_len).expect("record lengths fit a u32"),
        };
        let bytes = record.slice(..record_len);
        self.offset = location.end();
        Ok(Some(ScannedRecord {
            location,
            header,
            bytes,
        }))
    }

    /// Returns up to `len` bytes at the scan offset, fewer only at the end
    /// of the scan, reading ahead as needed.
    async fn fill(&mut self, len: usize) -> Result<Bytes, ScanError> {
        let remaining = self.end - self.offset;
        let len = usize::try_from(remaining).map_or(len, |remaining| len.min(remaining));
        let start = usize::try_from(self.offset - self.buf_start).unwrap_or(usize::MAX);
        if start <= self.buf.len() && self.buf.len() - start >= len {
            return Ok(self.buf.slice(start..start + len));
        }
        let read_len =
            usize::try_from(remaining).map_or(len.max(CHUNK_LEN), |r| len.max(CHUNK_LEN).min(r));
        self.buf = self
            .file
            .read_at(self.offset, read_len)
            .await
            .map_err(|source| ScanError::Io {
                segment: self.segment,
                source,
            })?;
        self.buf_start = self.offset;
        Ok(self.buf.slice(..len))
    }

    fn decode_error(&self, source: DecodeError) -> ScanError {
        ScanError::Decode {
            segment: self.segment,
            offset: self.offset,
            source,
        }
    }
}

/// Returns the offset of the first record at or after `from` in `file`
/// whose fixed header and CRC verify, or `None` if there is none.
///
/// Every offset where the record magic appears is tried, so this reads the
/// whole range; recovery calls it only past a tear, where the range is
/// normally empty.
pub(crate) async fn find_verified_record<F: SegmentFile>(
    file: &F,
    from: u64,
) -> io::Result<Option<u64>> {
    let end = file.len();
    let mut chunk_start = from;
    while chunk_start < end {
        let len = usize::try_from(end - chunk_start).map_or(CHUNK_LEN, |r| r.min(CHUNK_LEN));
        let chunk = file.read_at(chunk_start, len).await?;
        // A candidate may start in the last bytes of this chunk; the next
        // chunk starts after the last position tried here.
        let tried = chunk.len().saturating_sub(MAGIC.len() - 1).max(1);
        for at in 0..tried {
            if !chunk[at..].starts_with(&MAGIC) {
                continue;
            }
            let offset = chunk_start + at as u64;
            if verifies_at(file, offset, end, &chunk[at..]).await? {
                return Ok(Some(offset));
            }
        }
        chunk_start += tried as u64;
    }
    Ok(None)
}

/// Returns whether a record whose fixed header and CRC verify starts at
/// `offset`. `ahead` holds the bytes from `offset` already read.
async fn verifies_at<F: SegmentFile>(
    file: &F,
    offset: u64,
    end: u64,
    ahead: &[u8],
) -> io::Result<bool> {
    let fixed;
    let fixed = if ahead.len() >= RecordHeader::LEN {
        ahead
    } else if end - offset >= RecordHeader::LEN as u64 {
        fixed = file.read_at(offset, RecordHeader::LEN).await?;
        &fixed[..]
    } else {
        return Ok(false);
    };
    let Ok(len) = RecordHeader::peek_len(fixed) else {
        return Ok(false);
    };
    if end - offset < len as u64 {
        return Ok(false);
    }
    let record = file.read_at(offset, len).await?;
    Ok(RecordHeader::decode(&record).is_ok())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use skys3_io::{Disk, SimDisk};
    use skys3_types::{BucketId, Epoch, EpochSeq, Seq, ShardId};

    use super::*;
    use crate::record::{Delete, RecordBody, ShardRef};

    fn record() -> Bytes {
        LogRecord {
            shard: ShardRef::new(BucketId::new("b").unwrap(), ShardId::new(0)),
            position: EpochSeq::new(Epoch::new(1), Seq::new(1)),
            body: RecordBody::Delete(Delete { key: "key".into() }),
        }
        .to_bytes()
        .unwrap()
    }

    /// Returns a file holding `parts` back to back.
    async fn file(parts: &[&[u8]]) -> impl SegmentFile {
        let file = SimDisk::new(0).mount().create("f").await.unwrap();
        for part in parts {
            file.append(Bytes::copy_from_slice(part)).await.unwrap();
        }
        file
    }

    #[tokio::test]
    async fn finds_records_across_chunk_boundaries() {
        let record = record();
        // The magic straddles the end of the first chunk.
        let padding = vec![0; CHUNK_LEN - 2];
        let found = find_verified_record(&file(&[&padding, &record]).await, 0).await;
        assert_eq!(found.unwrap(), Some(padding.len() as u64));
        // The search starts at `from`.
        let file = file(&[&record, &record]).await;
        assert_eq!(find_verified_record(&file, 0).await.unwrap(), Some(0));
        let second = record.len() as u64;
        assert_eq!(find_verified_record(&file, 1).await.unwrap(), Some(second));
    }

    #[tokio::test]
    async fn skips_magic_that_is_not_a_record() {
        let record = record();
        let mut bad_crc = record.to_vec();
        bad_crc[30] ^= 1;
        let cases: [&[&[u8]]; 4] = [
            // Too short for a fixed header.
            &[&[0; 10], &MAGIC, &[0; 20]],
            // A whole fixed header, but lengths past the end of the file.
            &[&record[..RecordHeader::LEN + 1]],
            // An impossible header.
            &[&MAGIC, &[0xff; 100]],
            // A CRC mismatch.
            &[&bad_crc],
        ];
        for parts in cases {
            let file = file(parts).await;
            assert_eq!(find_verified_record(&file, 0).await.unwrap(), None);
        }
        // The fixed header is read separately when a chunk ends inside it.
        let padding = vec![0; CHUNK_LEN - 40];
        let file = file(&[&padding, &record]).await;
        assert_eq!(
            find_verified_record(&file, 0).await.unwrap(),
            Some(padding.len() as u64)
        );
    }
}
