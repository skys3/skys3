//! Fuzzes trust-policy parsing and evaluation. Trust policies come from
//! role registers in the control store.
//!
//! The input is a token subject, a newline, and a trust policy document;
//! without a newline, all of it is the document.

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_types::policy::trust::{TrustPolicy, WebIdentity};

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let (subject, document) = text.split_once('\n').unwrap_or(("repo:a/b:*", text));
    // Parsing never panics and is deterministic, and a document parses
    // exactly as it deserializes when embedded in another document.
    let parsed = TrustPolicy::parse(document).ok();
    assert_eq!(TrustPolicy::parse(document).ok(), parsed);
    if serde_json::from_str::<serde_json::Value>(document).is_ok() {
        let embedded = serde_json::from_str::<Vec<TrustPolicy>>(&format!("[{document}]"))
            .ok()
            .map(|mut policies| policies.remove(0));
        assert_eq!(embedded, parsed);
    } else {
        assert!(parsed.is_none());
    }
    let Some(policy) = parsed else {
        return;
    };
    // Evaluation never panics, and matching stays fast for any pattern
    // against any subject (libFuzzer's timeout catches a blow-up).
    for issuer in ["https://idp.example", "http://127.0.0.1:8080"] {
        let identity = WebIdentity {
            issuer,
            audience: subject,
            subject,
            authorized_party: Some(subject),
        };
        assert_eq!(policy.evaluate(&identity), policy.evaluate(&identity));
    }
});
