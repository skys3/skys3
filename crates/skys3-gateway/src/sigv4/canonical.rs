//! The canonical request, the string to sign, and signatures.
//!
//! The canonical request is built from the bytes the client sent. Its URI
//! and query components are never decoded and re-encoded, which would turn
//! `%2F` into `/` and lose what the client signed:
//!
//! - a `%XX` escape is kept as it was received, with its hexadecimal
//!   digits in uppercase, as every signer writes them;
//! - an unreserved character (`A-Z a-z 0-9 - . _ ~`), and `/` in the path,
//!   is kept;
//! - in the query, a `+` becomes `%20`: `s3s` decodes the query as a form,
//!   where `+` is a space, and so does the AWS SDKs' signer;
//! - any other byte, such as a literal space, `$`, or a `+` in the path, is
//!   percent-encoded, as the client did when it signed.
//!
//! The rule that matters is that two requests `s3s` reads differently never
//! share a canonical form: each canonical `%XX` stands for the byte `XX`
//! whether it came from an escape (in either case), from the byte itself,
//! or (for `%20` in the query) from a `+`, which is how `s3s` decodes them
//! too, and an escape that is not one stays `%25`. Repeated query
//! parameters are the exception, since signing sorts them by value; the
//! authenticator refuses them.
//!
//! S3 does not normalize paths: `/a/./b` and `//` are signed as they are.

use aws_lc_rs::{constant_time, digest, hmac};
use http::HeaderMap;
use http::request::Parts;

/// SHA-256 of the empty string, in hexadecimal.
pub(crate) const EMPTY_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// The head of a request, as far as signing goes.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Head<'a> {
    pub(crate) method: &'a str,
    /// The path as received, `%XX` escapes and all.
    pub(crate) path: &'a str,
    /// The query string as received, without its `?`.
    pub(crate) query: &'a str,
    pub(crate) headers: &'a HeaderMap,
    /// The request target's authority, which stands in for a missing
    /// `host` header (HTTP/2 sends `:authority` instead).
    pub(crate) authority: Option<&'a str>,
}

impl<'a> Head<'a> {
    pub(crate) fn new(parts: &'a Parts) -> Self {
        Self {
            method: parts.method.as_str(),
            path: parts.uri.path(),
            query: parts.uri.query().unwrap_or(""),
            headers: &parts.headers,
            authority: parts.uri.authority().map(http::uri::Authority::as_str),
        }
    }
}

/// A signed header the request does not have.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MissingHeader(pub(crate) String);

/// Builds the canonical request.
///
/// `signed_headers` is the signed header list as the client sent it,
/// already checked to be sorted lowercase names. With `presigned`, the
/// `X-Amz-Signature` query parameter is left out.
///
/// # Errors
///
/// [`MissingHeader`] if a signed header is not in the request.
pub(crate) fn canonical_request(
    head: &Head<'_>,
    signed_headers: &str,
    payload_hash: &str,
    presigned: bool,
) -> Result<Vec<u8>, MissingHeader> {
    let mut out = Vec::with_capacity(
        head.method.len() + head.path.len() + head.query.len() + 512 + payload_hash.len(),
    );
    out.extend_from_slice(head.method.as_bytes());
    out.push(b'\n');
    canonical_uri(head.path, &mut out);
    out.push(b'\n');
    canonical_query(head.query, presigned, &mut out);
    out.push(b'\n');
    for name in signed_headers.split(';') {
        out.extend_from_slice(name.as_bytes());
        out.push(b':');
        let mut values = head.headers.get_all(name).iter().peekable();
        if values.peek().is_none() {
            match (name, head.authority) {
                ("host", Some(authority)) => push_header_value(authority.as_bytes(), &mut out),
                _ => return Err(MissingHeader(name.to_owned())),
            }
        }
        for (i, value) in values.enumerate() {
            if i > 0 {
                out.push(b',');
            }
            push_header_value(value.as_bytes(), &mut out);
        }
        out.push(b'\n');
    }
    out.push(b'\n');
    out.extend_from_slice(signed_headers.as_bytes());
    out.push(b'\n');
    out.extend_from_slice(payload_hash.as_bytes());
    Ok(out)
}

/// Appends the canonical form of the path: `/` for an empty one.
pub(crate) fn canonical_uri(path: &str, out: &mut Vec<u8>) {
    if path.is_empty() {
        out.push(b'/');
    } else {
        push_canonical(path.as_bytes(), Component::Path, out);
    }
}

/// Appends the canonical query string: each `name=value` pair in canonical
/// form, sorted by name and then value, joined with `&`. A parameter
/// without `=` has an empty value.
pub(crate) fn canonical_query(query: &str, presigned: bool, out: &mut Vec<u8>) {
    let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .filter(|pair| !(presigned && super::params::decode_pair(pair).0 == "X-Amz-Signature"))
        .map(|pair| pair.split_once('=').unwrap_or((pair, "")))
        .map(|(name, value)| {
            let mut canonical_name = Vec::with_capacity(name.len());
            push_canonical(name.as_bytes(), Component::Query, &mut canonical_name);
            let mut canonical_value = Vec::with_capacity(value.len());
            push_canonical(value.as_bytes(), Component::Query, &mut canonical_value);
            (canonical_name, canonical_value)
        })
        .collect();
    pairs.sort_unstable();
    for (i, (name, value)) in pairs.iter().enumerate() {
        if i > 0 {
            out.push(b'&');
        }
        out.extend_from_slice(name);
        out.push(b'=');
        out.extend_from_slice(value);
    }
}

/// Which part of the request target a component is from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Component {
    /// The path: `/` is kept, and `+` is a plus.
    Path,
    /// A query parameter's name or value: `+` is a space.
    Query,
}

/// Appends `component` in canonical form (see the module documentation).
fn push_canonical(component: &[u8], kind: Component, out: &mut Vec<u8>) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut i = 0;
    while i < component.len() {
        let byte = component[i];
        if byte == b'%'
            && let Some(&[high, low]) = component.get(i + 1..i + 3)
            && high.is_ascii_hexdigit()
            && low.is_ascii_hexdigit()
        {
            out.extend_from_slice(&[b'%', high.to_ascii_uppercase(), low.to_ascii_uppercase()]);
            i += 3;
            continue;
        }
        if byte.is_ascii_alphanumeric()
            || matches!(byte, b'-' | b'.' | b'_' | b'~')
            || (kind == Component::Path && byte == b'/')
        {
            out.push(byte);
        } else if kind == Component::Query && byte == b'+' {
            out.extend_from_slice(b"%20");
        } else {
            out.extend_from_slice(&[
                b'%',
                HEX[usize::from(byte >> 4)],
                HEX[usize::from(byte & 15)],
            ]);
        }
        i += 1;
    }
}

/// Appends a header value with leading and trailing spaces removed and
/// each run of spaces collapsed to one. S3 treats only spaces this way.
fn push_header_value(value: &[u8], out: &mut Vec<u8>) {
    let (mut started, mut space) = (false, false);
    for &byte in value {
        if byte == b' ' {
            space = started;
            continue;
        }
        if space {
            out.push(b' ');
            space = false;
        }
        started = true;
        out.push(byte);
    }
}

/// The SHA-256 of `data`.
pub(crate) fn sha256(data: &[u8]) -> [u8; 32] {
    let digest = digest::digest(&digest::SHA256, data);
    let mut out = [0; 32];
    out.copy_from_slice(digest.as_ref());
    out
}

/// Lowercase hexadecimal.
pub(crate) fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 15)]));
    }
    out
}

/// The string to sign for a request.
pub(crate) fn string_to_sign(timestamp: &str, scope: &str, canonical_request: &[u8]) -> String {
    format!(
        "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
        hex(&sha256(canonical_request))
    )
}

/// Derives the signing key for a credential scope.
pub(crate) fn signing_key(secret: &[u8], date: &str, region: &str, service: &str) -> hmac::Key {
    let mut seed = Vec::with_capacity(4 + secret.len());
    seed.extend_from_slice(b"AWS4");
    seed.extend_from_slice(secret);
    let mut key = hmac::Key::new(hmac::HMAC_SHA256, &seed);
    seed.fill(0);
    for part in [date, region, service, "aws4_request"] {
        let tag = hmac::sign(&key, part.as_bytes());
        key = hmac::Key::new(hmac::HMAC_SHA256, tag.as_ref());
    }
    key
}

/// The signature of `string_to_sign`.
pub(crate) fn sign(key: &hmac::Key, string_to_sign: &[u8]) -> [u8; 32] {
    let tag = hmac::sign(key, string_to_sign);
    let mut out = [0; 32];
    out.copy_from_slice(tag.as_ref());
    out
}

/// Whether `signature` signs `string_to_sign`, compared in constant time.
pub(crate) fn verify(key: &hmac::Key, string_to_sign: &[u8], signature: &[u8; 32]) -> bool {
    constant_time::verify_slices_are_equal(&sign(key, string_to_sign), signature).is_ok()
}

/// What a query means to `s3s`: its pairs decoded as a form, in order of
/// name and value. A canonical query must mean what its query means.
#[cfg(any(test, feature = "test-util"))]
pub(crate) fn query_meaning(query: &str) -> Vec<(String, String)> {
    let mut pairs = serde_urlencoded::from_str::<Vec<(String, String)>>(query).unwrap_or_default();
    pairs.sort();
    pairs
}

#[cfg(test)]
mod tests {
    use http::Request;
    use proptest::prelude::*;

    use super::*;

    /// The secret key of the examples in the S3 documentation.
    const SECRET: &[u8] = b"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

    fn canonical(
        method: &str,
        uri: &str,
        headers: &[(&str, &str)],
        signed: &str,
        payload: &str,
    ) -> String {
        let mut request = Request::builder().method(method).uri(uri);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        let parts = request.body(()).unwrap().into_parts().0;
        let bytes = canonical_request(&Head::new(&parts), signed, payload, false).unwrap();
        String::from_utf8(bytes).unwrap()
    }

    fn signature(canonical: &str) -> String {
        let key = signing_key(SECRET, "20130524", "us-east-1", "s3");
        let to_sign = string_to_sign(
            "20130524T000000Z",
            "20130524/us-east-1/s3/aws4_request",
            canonical.as_bytes(),
        );
        hex(&sign(&key, to_sign.as_bytes()))
    }

    /// The examples of
    /// <https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html>.
    #[test]
    fn s3_documentation_examples() {
        let get = canonical(
            "GET",
            "/test.txt",
            &[
                ("host", "examplebucket.s3.amazonaws.com"),
                ("range", "bytes=0-9"),
                ("x-amz-content-sha256", EMPTY_SHA256),
                ("x-amz-date", "20130524T000000Z"),
            ],
            "host;range;x-amz-content-sha256;x-amz-date",
            EMPTY_SHA256,
        );
        assert_eq!(
            signature(&get),
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
        let body_hash = "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072";
        assert_eq!(hex(&sha256(b"Welcome to Amazon S3.")), body_hash);
        let put = canonical(
            "PUT",
            "/test$file.text",
            &[
                ("date", "Fri, 24 May 2013 00:00:00 GMT"),
                ("host", "examplebucket.s3.amazonaws.com"),
                ("x-amz-content-sha256", body_hash),
                ("x-amz-date", "20130524T000000Z"),
                ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
            ],
            "date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class",
            body_hash,
        );
        assert!(put.starts_with("PUT\n/test%24file.text\n\n"), "{put}");
        assert_eq!(
            signature(&put),
            "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
        );
        let headers = [
            ("host", "examplebucket.s3.amazonaws.com"),
            ("x-amz-content-sha256", EMPTY_SHA256),
            ("x-amz-date", "20130524T000000Z"),
        ];
        let signed = "host;x-amz-content-sha256;x-amz-date";
        let lifecycle = canonical("GET", "/?lifecycle", &headers, signed, EMPTY_SHA256);
        assert!(lifecycle.starts_with("GET\n/\nlifecycle=\n"), "{lifecycle}");
        assert_eq!(
            signature(&lifecycle),
            "fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543"
        );
        let list = canonical(
            "GET",
            "/?prefix=J&max-keys=2",
            &headers,
            signed,
            EMPTY_SHA256,
        );
        assert_eq!(
            signature(&list),
            "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
        );
    }

    /// The presigned URL example of
    /// <https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html>.
    #[test]
    fn s3_presigned_url_example() {
        let request = Request::get(
            "/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256\
             &X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request\
             &X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host\
             &X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404",
        )
        .header("host", "examplebucket.s3.amazonaws.com")
        .body(())
        .unwrap();
        let parts = request.into_parts().0;
        let canonical =
            canonical_request(&Head::new(&parts), "host", "UNSIGNED-PAYLOAD", true).unwrap();
        let canonical = String::from_utf8(canonical).unwrap();
        assert_eq!(
            canonical,
            "GET\n/test.txt\nX-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=\
             AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=\
             20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host\n\
             host:examplebucket.s3.amazonaws.com\n\nhost\nUNSIGNED-PAYLOAD"
        );
        assert_eq!(
            signature(&canonical),
            "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
        );
    }

    #[test]
    fn components_are_canonicalized_without_decoding() {
        let uri = |path: &str| {
            let mut out = Vec::new();
            canonical_uri(path, &mut out);
            String::from_utf8(out).unwrap()
        };
        assert_eq!(uri(""), "/");
        assert_eq!(uri("/a%2fb/c%2Fd"), "/a%2Fb/c%2Fd");
        assert_eq!(uri("/a+b$c d"), "/a%2Bb%24c%20d");
        assert_eq!(uri("/./a/../b//"), "/./a/../b//");
        assert_eq!(uri("/50%/%zz"), "/50%25/%25zz");
        assert_eq!(uri("/\u{1234}"), "/%E1%88%B4");
        let query = |query: &str, presigned: bool| {
            let mut out = Vec::new();
            canonical_query(query, presigned, &mut out);
            String::from_utf8(out).unwrap()
        };
        assert_eq!(query("", false), "");
        assert_eq!(query("b=2&a=1&&a=0&c", false), "a=0&a=1&b=2&c=");
        assert_eq!(
            query("k=a+b&k2=a%2bb&k3=a/b", false),
            "k=a%20b&k2=a%2Bb&k3=a%2Fb"
        );
        assert_eq!(query("a+b=%20+", false), "a%20b=%20%20");
        assert_eq!(query("a=1&X-Amz-Signature=s", true), "a=1");
        assert_eq!(query("a=1&X-Amz-%53ignature=s", true), "a=1");
        assert_eq!(
            query("a=1&x-amz-signature=s", true),
            "a=1&x-amz-signature=s"
        );
        assert_eq!(
            query("a=1&X-Amz-Signature=s", false),
            "X-Amz-Signature=s&a=1"
        );
        assert_eq!(query("x==y", false), "x=%3Dy");
        // Pairs sort by name, not by their joined text.
        assert_eq!(query("=&%20", false), "=&%20=");
    }

    #[test]
    fn header_values_are_trimmed_and_joined() {
        let mut out = Vec::new();
        push_header_value(b"  a   b \t c  ", &mut out);
        assert_eq!(out, b"a b \t c");
        out.clear();
        push_header_value(b"\tx\t", &mut out);
        assert_eq!(out, b"\tx\t");
        let canonical = canonical(
            "GET",
            "http://example.com:8080/k",
            &[("x-a", "1"), ("x-a", " 2  3 ")],
            "host;x-a",
            "UNSIGNED-PAYLOAD",
        );
        assert_eq!(
            canonical,
            "GET\n/k\n\nhost:example.com:8080\nx-a:1,2 3\n\nhost;x-a\nUNSIGNED-PAYLOAD"
        );
        let parts = Request::get("/k").body(()).unwrap().into_parts().0;
        assert_eq!(
            canonical_request(&Head::new(&parts), "host", "", false),
            Err(MissingHeader("host".to_owned()))
        );
    }

    #[test]
    fn verification_is_exact() {
        let key = signing_key(SECRET, "20130524", "us-east-1", "s3");
        let mut signature = sign(&key, b"message");
        assert!(verify(&key, b"message", &signature));
        assert!(!verify(&key, b"messagE", &signature));
        signature[31] ^= 1;
        assert!(!verify(&key, b"message", &signature));
    }

    /// Whether `text` is a canonical component: unreserved characters,
    /// optionally `/`, and `%XX` escapes with uppercase digits.
    fn is_canonical(text: &[u8], slash: bool) -> bool {
        let mut i = 0;
        while i < text.len() {
            match text[i] {
                b'%' => {
                    let escape = text.get(i + 1..i + 3);
                    if !escape.is_some_and(|e| {
                        e.iter()
                            .all(|d| d.is_ascii_digit() || (b'A'..=b'F').contains(d))
                    }) {
                        return false;
                    }
                    i += 3;
                }
                b'/' if slash => i += 1,
                b if b.is_ascii_alphanumeric() || b"-._~".contains(&b) => i += 1,
                _ => return false,
            }
        }
        true
    }

    proptest! {
        #[test]
        fn canonical_paths_are_canonical_and_stable(path in "\\PC*") {
            let mut once = Vec::new();
            canonical_uri(&path, &mut once);
            prop_assert!(is_canonical(&once, true));
            let mut twice = Vec::new();
            canonical_uri(std::str::from_utf8(&once).unwrap(), &mut twice);
            prop_assert_eq!(&once, &twice);
        }

        /// The canonical query means what the query means to `s3s`, so two
        /// queries `s3s` reads differently never share a canonical form.
        #[test]
        fn canonical_queries_mean_what_the_query_means(
            tokens in proptest::collection::vec(
                proptest::sample::select(&[
                    "a", "B", "+", "%2B", "%2b", "%20", " ", "%", "%zz", "%2", "=", "&", ";",
                    "%3D", "%26", "%61", "%FF", "%C3%A9", "\u{e9}", "/", "%2F", "~",
                ][..]),
                0..24,
            ),
        ) {
            let query = tokens.concat();
            let mut canonical = Vec::new();
            canonical_query(&query, false, &mut canonical);
            prop_assert_eq!(query_meaning(std::str::from_utf8(&canonical).unwrap()), query_meaning(&query));
        }

        #[test]
        fn canonical_paths_mean_what_the_path_means(
            tokens in proptest::collection::vec(
                proptest::sample::select(&[
                    "a", "+", "%2B", "%20", " ", "%", "%zz", "/", "%2F", "%2f", "%FF", "\u{e9}",
                ][..]),
                0..24,
            ),
        ) {
            let path = tokens.concat();
            let mut canonical = Vec::new();
            canonical_uri(&path, &mut canonical);
            let decoded = crate::limits::percent_decode(&path);
            let canonical = crate::limits::percent_decode(std::str::from_utf8(&canonical).unwrap());
            // An empty path is `/`.
            prop_assert_eq!(canonical, if decoded.is_empty() { b"/".to_vec() } else { decoded });
        }

        #[test]
        fn canonical_queries_are_sorted_and_stable(query in "[a-zA-Z0-9%=&+ ~._/-]{0,64}") {
            let mut once = Vec::new();
            canonical_query(&query, false, &mut once);
            let text = String::from_utf8(once.clone()).unwrap();
            let pairs: Vec<(&str, &str)> = text
                .split('&')
                .filter(|p| !p.is_empty())
                .map(|p| p.split_once('=').unwrap())
                .collect();
            prop_assert!(pairs.windows(2).all(|w| w[0] <= w[1]));
            for (name, value) in &pairs {
                prop_assert!(is_canonical(name.as_bytes(), false));
                prop_assert!(is_canonical(value.as_bytes(), false));
            }
            let mut twice = Vec::new();
            canonical_query(&text, false, &mut twice);
            prop_assert_eq!(once, twice);
        }

        #[test]
        fn unreserved_bytes_and_escapes_are_kept(bytes in proptest::collection::vec(any::<u8>(), 0..64)) {
            // Fully percent-encoded input is already canonical.
            let encoded: String = bytes.iter().map(|b| format!("%{b:02X}")).collect();
            let mut out = Vec::new();
            push_canonical(encoded.as_bytes(), Component::Query, &mut out);
            prop_assert_eq!(out, encoded.into_bytes());
        }
    }
}
