//! Entry points for the fuzz targets in the repository's `fuzz/` crate.
//!
//! This module is hidden from the documentation and is not a stable API.

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
    }
}
