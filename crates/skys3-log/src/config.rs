//! The log's settings, from the `[storage]` configuration section.

use std::time::Duration;

use skys3_config::StorageConfig;

use crate::log::MAX_RECORD_LEN;

/// How the log sizes segments and group commits (§10.1, §10.4).
///
/// Build it from the validated configuration with
/// [`LogConfig::from_storage`]. The fields are public so tests and tools can
/// set them directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogConfig {
    /// `inline_max_bytes`: the largest payload a record in a hot segment
    /// may carry. Larger bodies are streamed as `EXTENT` records, which go
    /// to bulk segments.
    pub inline_max_bytes: u64,
    /// `segment_bytes`: the size at which the log starts a new segment of a
    /// class. A segment exceeds it only when a single group commit's
    /// records for the class do (see [`SegmentLog`](crate::SegmentLog)).
    pub segment_bytes: u64,
    /// `group_commit_max_delay_us`: how long a group commit waits, from the
    /// arrival of its first record, for more records before it writes and
    /// syncs. Zero commits whatever is queued at once.
    pub group_commit_max_delay: Duration,
    /// `group_commit_max_bytes`: the bytes after which a group commit stops
    /// waiting for more records. A commit holds less than this plus one
    /// record.
    pub group_commit_max_bytes: u64,
}

impl LogConfig {
    /// Takes the log's settings from the `[storage]` section.
    #[must_use]
    pub fn from_storage(storage: &StorageConfig) -> Self {
        Self {
            inline_max_bytes: storage.inline_max_bytes,
            segment_bytes: storage.segment_bytes,
            group_commit_max_delay: storage.group_commit_max_delay(),
            group_commit_max_bytes: storage.group_commit_max_bytes,
        }
    }

    /// The most bytes a crash can leave unsynced at the end of a segment:
    /// one group commit's worth. Recovery uses it to tell a torn tail from
    /// damage to synced records (see [`recovery`](crate::recovery)).
    #[must_use]
    pub fn tear_window(&self) -> u64 {
        self.group_commit_max_bytes
            .saturating_add(u64::from(MAX_RECORD_LEN))
    }
}

impl Default for LogConfig {
    /// The settings of the default configuration.
    fn default() -> Self {
        Self::from_storage(&StorageConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn takes_the_storage_settings() {
        let storage = StorageConfig {
            inline_max_bytes: 4096,
            segment_bytes: 1 << 20,
            group_commit_max_delay_us: 250,
            group_commit_max_bytes: 1 << 16,
            ..StorageConfig::default()
        };
        let config = LogConfig::from_storage(&storage);
        assert_eq!(
            config,
            LogConfig {
                inline_max_bytes: 4096,
                segment_bytes: 1 << 20,
                group_commit_max_delay: Duration::from_micros(250),
                group_commit_max_bytes: 1 << 16,
            }
        );
        assert_eq!(config.tear_window(), (1 << 16) + u64::from(MAX_RECORD_LEN));
        assert_eq!(
            LogConfig::default().group_commit_max_delay,
            Duration::from_micros(500)
        );
    }
}
