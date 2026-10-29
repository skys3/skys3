//! The index's error type.

use std::io;

use skys3_io::PoolClosed;
use skys3_log::{LogError, RecordLocation, ScanError};

use crate::codec::CodecError;

/// Why an index operation failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum IndexError {
    /// redb failed: an I/O error, or a database it cannot use.
    #[error("index storage: {0}")]
    Storage(#[from] redb::Error),
    /// Creating or opening the index file failed.
    #[error("index file: {0}")]
    Io(#[from] io::Error),
    /// The index holds bytes it cannot decode, or was given a value it
    /// cannot encode.
    #[error("index {table} table: {source}")]
    Codec {
        /// The table.
        table: &'static str,
        /// What is wrong.
        source: CodecError,
    },
    /// The index was written by a build with another format version.
    #[error("index format version {found} is not supported (this build reads {supported})")]
    UnsupportedFormat {
        /// The version found.
        found: u64,
        /// The version this build reads.
        supported: u64,
    },
    /// Reading the log failed.
    #[error(transparent)]
    Log(#[from] LogError),
    /// Replay found a segment it cannot read to its end. Recovery has already
    /// cut torn tails, so this is damage.
    #[error("replay: {0}")]
    Scan(#[from] ScanError),
    /// A record that replay must apply does not decode: its CRC verified,
    /// so it was written damaged (§10.1).
    #[error("replay: the record at {location} is damaged: {source}")]
    Damaged {
        /// Where the record is.
        location: RecordLocation,
        /// What is wrong with it.
        source: LogError,
    },
    /// The blocking pool that runs index I/O has shut down.
    #[error(transparent)]
    Pool(#[from] PoolClosed),
}

impl IndexError {
    pub(crate) fn codec(table: &'static str) -> impl FnOnce(CodecError) -> Self {
        move |source| Self::Codec { table, source }
    }
}

/// Converts each of redb's error types into [`IndexError::Storage`].
macro_rules! from_redb {
    ($($error:ty),* $(,)?) => {
        $(
            impl From<$error> for IndexError {
                fn from(error: $error) -> Self {
                    Self::Storage(error.into())
                }
            }
        )*
    };
}

from_redb!(
    redb::CommitError,
    redb::DatabaseError,
    redb::SetDurabilityError,
    redb::StorageError,
    redb::TableError,
    redb::TransactionError,
);
