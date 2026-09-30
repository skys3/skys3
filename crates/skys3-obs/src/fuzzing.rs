//! Entry points for the fuzz targets in the repository's `fuzz/` crate.
//!
//! This module is hidden from the documentation and is not a stable API. It
//! exposes parsers of untrusted input without making them part of the
//! crate's public surface.

use http::HeaderMap;
use http::header::{AUTHORIZATION, HeaderValue};

use crate::admin::{self, AdminToken};

/// The token [`authorize`] checks against, at the minimum length.
pub const TOKEN: &str = "fuzz-token-0123456789abcdefghijk";

/// Parses an `Authorization` header value as bearer credentials.
#[must_use]
pub fn bearer_credentials(value: &[u8]) -> Option<&[u8]> {
    admin::bearer_credentials(value)
}

/// Runs the admin listener's authorization check on a raw `Authorization`
/// header value against [`TOKEN`]. Returns `None` if the bytes are not a
/// valid header value, which the HTTP parser would have rejected already.
#[must_use]
pub fn authorize(value: &[u8]) -> Option<bool> {
    let value = HeaderValue::from_bytes(value).ok()?;
    let token = AdminToken::new(TOKEN).expect("TOKEN is a valid admin token");
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, value);
    Some(admin::authorized(Some(&token), &headers))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_points() {
        assert_eq!(bearer_credentials(b"Bearer x"), Some(&b"x"[..]));
        assert_eq!(authorize(format!("Bearer {TOKEN}").as_bytes()), Some(true));
        assert_eq!(authorize(b"Bearer nope"), Some(false));
        assert_eq!(authorize(b"Bearer \n"), None);
    }
}
