//! Size limits shared by the storage engine and configuration loading.
//!
//! The log record format (`skys3-log`, design §10.1) bounds its payloads,
//! and configuration loading (`skys3-config`) keeps `inline_max_bytes` and
//! `extent_bytes` within those bounds, so a valid configuration never asks
//! for a record the log rejects. Both crates read the limits from here, so
//! neither depends on the other.

/// The largest single PUT S3 accepts, in bytes (5 GiB).
pub const MAX_SINGLE_PUT_BYTES: u64 = 5 * 1024 * 1024 * 1024;

/// The most parts a multipart upload can have (the S3 limit), and so the
/// largest part count of a multipart ETag or composite checksum.
pub const MAX_PARTS: u32 = 10_000;

/// The largest log record payload, in bytes (16 MiB): one inline body or one
/// extent. `inline_max_bytes` and `extent_bytes` are at most this.
pub const MAX_RECORD_PAYLOAD_LEN: u32 = 16 * 1024 * 1024;

/// The smallest `extent_bytes`, in bytes (64 KiB).
pub const MIN_EXTENT_LEN: u32 = 64 * 1024;

/// The least a peer destination charges against `peer_staging_quota_bytes`
/// for each staging it opens and each extent it stages (64 KiB, the
/// smallest `extent_bytes`, design §7.8). The memory its staging index
/// takes then grows with the quota, as staging in the smallest extents
/// would, however small the frames a source sends.
pub const MIN_STAGING_CHARGE: u64 = MIN_EXTENT_LEN as u64;

/// The most extents one `PUT` record references: a
/// [`MAX_SINGLE_PUT_BYTES`] object in extents of [`MIN_EXTENT_LEN`].
pub const MAX_EXTENTS_PER_PUT: usize = (MAX_SINGLE_PUT_BYTES / MIN_EXTENT_LEN as u64) as usize;

const _: () = assert!(MAX_EXTENTS_PER_PUT == 81_920);
const _: () = assert!(MIN_EXTENT_LEN <= MAX_RECORD_PAYLOAD_LEN);

/// Whether an XML 1.0 document can carry `text`: it holds no control
/// character below U+0020 other than tab, line feed, and carriage return.
///
/// S3 answers in XML and echoes keys, prefixes, markers, and rule IDs, and
/// `s3s` refuses to send a response that holds such a character, so the
/// gateway refuses them where a client brings them in (design §12).
#[must_use]
pub fn is_xml_text(text: &str) -> bool {
    !text
        .bytes()
        .any(|b| b < b' ' && !matches!(b, b'\t' | b'\n' | b'\r'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xml_text_has_no_control_characters_but_whitespace() {
        assert!(is_xml_text(""));
        assert!(is_xml_text("photos/2024/cat.jpg"));
        assert!(is_xml_text("tab\tnew line\ncarriage return\r\u{7f}\u{e9}"));
        for b in (0_u8..0x20).filter(|b| !matches!(b, b'\t' | b'\n' | b'\r')) {
            let text = format!("a{}b", char::from(b));
            assert!(!is_xml_text(&text), "{text:?}");
        }
    }
}
