//! Fuzzes `ETag` parsing, which reads entity tags from remote responses.

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_types::ETag;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    // Parsing must never panic. A quoted tag that is accepted prints back as
    // the input and re-parses equal.
    if let Ok(etag) = ETag::from_quoted(text) {
        let quoted = etag.to_quoted();
        assert_eq!(quoted, text);
        assert!(etag.as_str().len() <= ETag::MAX_LEN);
        assert_eq!(ETag::from_quoted(&quoted).ok(), Some(etag));
    }
    // The same holds for the unquoted form.
    if let Ok(etag) = ETag::new(text) {
        assert_eq!(etag.as_str(), text);
        assert_eq!(ETag::from_quoted(&etag.to_quoted()).ok(), Some(etag));
    }
});
