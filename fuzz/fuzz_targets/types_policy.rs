//! Fuzzes policy parsing and evaluation. Policies come from configuration
//! now, and from the control store and STS callers' session policies later
//! (plan M1-24).
//!
//! The input is a resource key, a newline, and a policy document; without
//! a newline, all of it is the document.

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_types::policy::{MAX_POLICY_BYTES, Policy, RequestContext, S3_ARN_PREFIX};

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let (key, document) = text.split_once('\n').unwrap_or(("bucket/key", text));
    // Parsing never panics, accepts only bounded documents, and is
    // deterministic.
    let Ok(policy) = Policy::parse(document) else {
        return;
    };
    assert!(document.len() <= MAX_POLICY_BYTES);
    assert_eq!(Policy::parse(document).ok().as_ref(), Some(&policy));
    // Evaluation never panics, and matching stays fast for any pattern
    // against any key (libFuzzer's timeout catches a blow-up).
    let resource = format!("{S3_ARN_PREFIX}{key}");
    for action in ["s3:GetObject", "s3:PutObject", "s3:ListAllMyBuckets"] {
        let request = RequestContext::new(action, &resource);
        assert_eq!(policy.evaluate(&request), policy.evaluate(&request));
    }
});
