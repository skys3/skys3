//! ListObjectsV2 continuation tokens (§9.4): the last item of a page,
//! authenticated with HMAC-SHA256.
//!
//! A token is `base64url(version ‖ last item ‖ tag)`, without padding, where
//! `tag` is `HMAC-SHA256(key, context ‖ version ‖ bucket ID ‖ prefix ‖
//! delimiter ‖ last item)` and each variable-length field of the MAC input
//! is length-prefixed. A token is therefore valid only for the listing that
//! issued it: the same bucket, prefix, and delimiter. Its contents are the
//! gateway's, not the client's; the version byte lets a later format carry
//! more, such as per-shard positions.
//!
//! The keys are [`ListTokenKeys`]: one that signs, and older ones that still
//! verify, so a key can be rotated without failing listings in progress.

use std::fmt;

use aws_lc_rs::hmac;
use aws_lc_rs::rand::SystemRandom;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use skys3_types::BucketId;

use crate::limits::MAX_KEY_BYTES;

/// The format version of the tokens this build issues.
const VERSION: u8 = 1;

/// Domain separation for the MAC.
const CONTEXT: &[u8] = b"skys3 list continuation token";

/// The bytes of a tag: a whole HMAC-SHA256.
const TAG_BYTES: usize = 32;

/// The longest token, in characters, that is decoded at all: the base64
/// of a version byte, an item of [`MAX_KEY_BYTES`], and a tag.
const MAX_TOKEN_CHARS: usize = (1 + MAX_KEY_BYTES + TAG_BYTES).div_ceil(3) * 4;

/// The keys that sign and verify continuation tokens.
///
/// Where they come from is the node's decision (design §9.4): a node
/// generates a key at startup ([`ListTokenKeys::generate`]) unless it is
/// given one, so that its tokens are accepted by that node until it
/// restarts. Gateways that must accept each other's tokens share keys
/// ([`ListTokenKeys::new`]).
#[derive(Clone)]
pub struct ListTokenKeys {
    /// The key that signs new tokens, and verifies first.
    current: hmac::Key,
    /// Keys that still verify tokens they signed.
    previous: Vec<hmac::Key>,
}

impl fmt::Debug for ListTokenKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ListTokenKeys")
            .field("previous", &self.previous.len())
            .finish_non_exhaustive()
    }
}

/// A key given to [`ListTokenKeys::new`] that is too short.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "a continuation-token key must be at least {} bytes",
    ListTokenKeys::MIN_KEY_BYTES
)]
pub struct ShortTokenKey;

/// A continuation token that this gateway did not issue for this listing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InvalidToken;

/// What a token is bound to: one listing of one bucket.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TokenScope<'a> {
    pub(crate) bucket: &'a BucketId,
    pub(crate) prefix: &'a str,
    pub(crate) delimiter: Option<&'a str>,
}

impl ListTokenKeys {
    /// The shortest key accepted, in bytes: the strength of HMAC-SHA256.
    pub const MIN_KEY_BYTES: usize = 32;

    /// A fresh random key, with no previous ones.
    ///
    /// # Panics
    ///
    /// If the operating system's random number generator fails, which
    /// leaves the node unable to make any secret.
    #[must_use]
    pub fn generate() -> Self {
        let current = hmac::Key::generate(hmac::HMAC_SHA256, &SystemRandom::new())
            .expect("the system random number generator works");
        Self {
            current,
            previous: Vec::new(),
        }
    }

    /// Keys from secrets: `current` signs, and it and every one of
    /// `previous` verify.
    ///
    /// # Errors
    ///
    /// [`ShortTokenKey`] if a secret is shorter than
    /// [`ListTokenKeys::MIN_KEY_BYTES`].
    pub fn new(current: &[u8], previous: &[&[u8]]) -> Result<Self, ShortTokenKey> {
        let key = |secret: &[u8]| {
            if secret.len() < Self::MIN_KEY_BYTES {
                return Err(ShortTokenKey);
            }
            Ok(hmac::Key::new(hmac::HMAC_SHA256, secret))
        };
        Ok(Self {
            current: key(current)?,
            previous: previous
                .iter()
                .map(|secret| key(secret))
                .collect::<Result<_, _>>()?,
        })
    }

    /// A token that resumes the listing `scope` after the item `last`.
    pub(crate) fn seal(&self, scope: TokenScope<'_>, last: &str) -> String {
        let tag = hmac::sign(&self.current, &message(scope, last.as_bytes()));
        let mut token = Vec::with_capacity(1 + last.len() + TAG_BYTES);
        token.push(VERSION);
        token.extend_from_slice(last.as_bytes());
        token.extend_from_slice(tag.as_ref());
        URL_SAFE_NO_PAD.encode(token)
    }

    /// The item a token issued for the listing `scope` resumes after.
    pub(crate) fn open(&self, scope: TokenScope<'_>, token: &str) -> Result<String, InvalidToken> {
        if token.len() > MAX_TOKEN_CHARS {
            return Err(InvalidToken);
        }
        let bytes = URL_SAFE_NO_PAD.decode(token).map_err(|_| InvalidToken)?;
        let Some((&VERSION, rest)) = bytes.split_first() else {
            return Err(InvalidToken);
        };
        let Some(split) = rest.len().checked_sub(TAG_BYTES).filter(|&n| n > 0) else {
            return Err(InvalidToken);
        };
        let (last, tag) = rest.split_at(split);
        let message = message(scope, last);
        std::iter::once(&self.current)
            .chain(&self.previous)
            .find(|key| hmac::verify(key, &message, tag).is_ok())
            .ok_or(InvalidToken)?;
        // Only this gateway's keys sign, and they sign only items, so a
        // verified token holds one.
        String::from_utf8(last.to_vec()).map_err(|_| InvalidToken)
    }
}

/// The MAC input of a token for `scope` that resumes after `last`.
fn message(scope: TokenScope<'_>, last: &[u8]) -> Vec<u8> {
    let mut message = Vec::with_capacity(CONTEXT.len() + 64 + last.len());
    let mut field = |bytes: &[u8]| {
        let len = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
        message.extend_from_slice(&len.to_be_bytes());
        message.extend_from_slice(bytes);
    };
    field(CONTEXT);
    field(&[VERSION]);
    field(scope.bucket.as_str().as_bytes());
    field(scope.prefix.as_bytes());
    match scope.delimiter {
        Some(delimiter) => {
            field(&[1]);
            field(delimiter.as_bytes());
        }
        None => field(&[0]),
    }
    field(last);
    message
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn bucket() -> BucketId {
        BucketId::new("b-7f3a").unwrap()
    }

    fn scope<'a>(
        bucket: &'a BucketId,
        prefix: &'a str,
        delimiter: Option<&'a str>,
    ) -> TokenScope<'a> {
        TokenScope {
            bucket,
            prefix,
            delimiter,
        }
    }

    #[test]
    fn a_token_opens_only_for_its_listing() {
        let keys = ListTokenKeys::generate();
        let b = bucket();
        let token = keys.seal(scope(&b, "photos/", Some("/")), "photos/2024/");
        assert!(token.len() <= MAX_TOKEN_CHARS);
        assert_eq!(
            keys.open(scope(&b, "photos/", Some("/")), &token),
            Ok("photos/2024/".to_owned())
        );
        let other = BucketId::new("b-other").unwrap();
        for wrong in [
            scope(&other, "photos/", Some("/")),
            scope(&b, "photos", Some("/")),
            scope(&b, "photos/", None),
            scope(&b, "photos/", Some("")),
            scope(&b, "photos/", Some("|")),
        ] {
            assert_eq!(keys.open(wrong, &token), Err(InvalidToken), "{wrong:?}");
        }
        // Another key, as another node's, does not verify it.
        let other_keys = ListTokenKeys::generate();
        assert_eq!(
            other_keys.open(scope(&b, "photos/", Some("/")), &token),
            Err(InvalidToken)
        );
        assert!(format!("{keys:?}").contains("previous: 0"));
    }

    #[test]
    fn rotated_keys_still_verify() {
        let old = [7u8; 32];
        let new = [9u8; 40];
        let b = bucket();
        let s = scope(&b, "", None);
        let before = ListTokenKeys::new(&old, &[]).unwrap();
        let token = before.seal(s, "k");
        let after = ListTokenKeys::new(&new, &[&old]).unwrap();
        assert_eq!(after.open(s, &token), Ok("k".to_owned()));
        // Tokens are signed with the current key only.
        let fresh = after.seal(s, "k");
        assert_ne!(fresh, token);
        assert_eq!(before.open(s, &fresh), Err(InvalidToken));
        assert_eq!(
            ListTokenKeys::new(&old[..31], &[]).unwrap_err(),
            ShortTokenKey
        );
        assert!(ListTokenKeys::new(&new, &[&old[..1]]).is_err());
        assert_eq!(
            ShortTokenKey.to_string(),
            "a continuation-token key must be at least 32 bytes"
        );
    }

    #[test]
    fn malformed_tokens_are_refused() {
        let keys = ListTokenKeys::new(&[1; 32], &[]).unwrap();
        let b = bucket();
        let s = scope(&b, "", None);
        let valid = keys.seal(s, "key");
        let mut versioned = URL_SAFE_NO_PAD.decode(&valid).unwrap();
        versioned[0] = 2;
        // A token for an empty item, which no listing has.
        let tag_only = keys.seal(s, "");
        for token in [
            String::new(),
            "*".to_owned(),
            "A".repeat(MAX_TOKEN_CHARS + 1),
            URL_SAFE_NO_PAD.encode([VERSION]),
            URL_SAFE_NO_PAD.encode(versioned),
            format!("{valid}="),
            tag_only,
        ] {
            assert_eq!(keys.open(s, &token), Err(InvalidToken), "{token:?}");
        }
        // A tag over bytes that are not UTF-8 is not something the gateway
        // signs, and is refused after it verifies.
        let mut bytes = vec![VERSION, 0xff];
        let tag = hmac::sign(&keys.current, &message(s, &[0xff]));
        bytes.extend_from_slice(tag.as_ref());
        assert_eq!(
            keys.open(s, &URL_SAFE_NO_PAD.encode(bytes)),
            Err(InvalidToken)
        );
    }

    proptest! {
        #[test]
        fn tampered_tokens_are_refused(
            last in "\\PC{1,40}",
            prefix in "\\PC{0,8}",
            delimiter in proptest::option::of("\\PC{0,3}"),
            at in any::<prop::sample::Index>(),
            with in "[A-Za-z0-9_-]",
        ) {
            let keys = ListTokenKeys::generate();
            let b = bucket();
            let s = scope(&b, &prefix, delimiter.as_deref());
            let token = keys.seal(s, &last);
            prop_assert_eq!(keys.open(s, &token), Ok(last));
            let at = at.index(token.len());
            prop_assume!(token[at..=at] != with);
            let mut tampered = token.clone();
            tampered.replace_range(at..=at, &with);
            prop_assert_eq!(keys.open(s, &tampered), Err(InvalidToken));
            let truncated = &token[..at];
            prop_assert_eq!(keys.open(s, truncated), Err(InvalidToken));
        }
    }
}
