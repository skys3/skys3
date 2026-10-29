//! Entry points for the fuzz targets in the repository's `fuzz/` crate.
//!
//! This module is hidden from the documentation and is not a stable API.

use crate::endpoint::request::{self, MAX_SESSION_POLICY_BYTES};
use crate::jwk::{KeySet, MAX_KEYS};
use crate::jwt::{MAX_TOKEN_BYTES, UnverifiedToken};

/// Parses `data` as a token and checks the parser's invariants. Returns
/// whether it parsed.
///
/// # Panics
///
/// Panics if an invariant does not hold: that is a bug the fuzzer found.
pub fn parse_token(data: &[u8]) -> bool {
    let Ok(text) = std::str::from_utf8(data) else {
        return false;
    };
    let Ok(token) = UnverifiedToken::parse(text) else {
        return false;
    };
    assert!(text.len() <= MAX_TOKEN_BYTES);
    // The signature covers exactly the first two parts.
    assert!(text.as_bytes().starts_with(token.signing_input));
    assert_eq!(text.as_bytes()[token.signing_input.len()], b'.');
    assert_eq!(
        token.signing_input.iter().filter(|b| **b == b'.').count(),
        1
    );
    assert!(token.header.alg.name().parse::<crate::Algorithm>() == Ok(token.header.alg));
    true
}

/// Parses `data` as a JWKS. Returns the number of usable keys.
///
/// # Panics
///
/// Panics if an invariant does not hold.
pub fn parse_jwks(data: &[u8]) -> Option<usize> {
    let keys = KeySet::parse(data).ok()?;
    assert!(keys.len() <= MAX_KEYS);
    Some(keys.len())
}

/// Parses `data` as an `AssumeRoleWithWebIdentity` request: the query
/// string up to the first newline, and the form body after it. Returns
/// whether it parsed.
///
/// # Panics
///
/// Panics if an accepted request breaks a rule the parser enforces.
pub fn parse_assume_role(data: &[u8]) -> bool {
    let (query, body) = match data.iter().position(|b| *b == b'\n') {
        Some(at) => (&data[..at], &data[at + 1..]),
        None => (&[][..], data),
    };
    let Ok(request) = request::parse(query, body) else {
        return false;
    };
    assert!(skys3_types::RoleDocument::valid_name(&request.role));
    assert!(request.role_arn.ends_with(&request.role));
    assert!(request.account.bytes().all(|b| b.is_ascii_digit()));
    assert!((2..=64).contains(&request.session_name.len()));
    assert!((4..=MAX_TOKEN_BYTES).contains(&request.token.len()));
    assert!(
        request
            .policy
            .as_ref()
            .is_none_or(|policy| (1..=MAX_SESSION_POLICY_BYTES).contains(&policy.len()))
    );
    // Parsing is deterministic.
    assert!(request::parse(query, body).is_ok_and(|again| again == request));
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_points() {
        assert!(parse_token(b"eyJhbGciOiJSUzI1NiJ9.e30.c2ln"));
        assert!(!parse_token(b"eyJhbGciOiJub25lIn0.e30."));
        assert!(!parse_token(&[0xff, b'.', b'.']));
        assert_eq!(parse_jwks(br#"{"keys":[{"kty":"oct","k":"AA"}]}"#), Some(0));
        assert_eq!(parse_jwks(b"[]"), None);
        assert!(parse_assume_role(
            b"Action=AssumeRoleWithWebIdentity\nVersion=2011-06-15&RoleArn=arn:aws:iam::1:role/r\
              &RoleSessionName=ci&WebIdentityToken=abcd"
        ));
        assert!(!parse_assume_role(b"Action=AssumeRole"));
    }
}
