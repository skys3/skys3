//! Request limits (design §12): header sizes, URI and key lengths, part
//! numbers, ranges, and the size and nesting depth of XML bodies.
//!
//! The gateway checks every request against [`RequestLimits`] before
//! authentication and routing, so an oversized request costs a bounded
//! amount of memory and never reaches `s3s`. HTTP/1 framing bounds the
//! header block first ([`GatewayListener`](crate::GatewayListener) sets
//! hyper's limits from the same values), and the checks here hold for any
//! transport.
//!
//! The gateway serves path-style requests (`/bucket/key`); a request's
//! path tells whether its body is object data, which streams on unbuffered,
//! or a small XML document, which is buffered up to
//! [`RequestLimits::max_xml_body_bytes`] and scanned for depth before `s3s`
//! parses it. `s3s` parses each XML body against its operation's schema
//! and skips unknown elements without recursion, so the depth bound is
//! defense in depth; the size bound is what bounds memory.

use std::sync::Arc;

use http::request::Parts;
use http::{HeaderMap, Method};
use quick_xml::Reader;
use quick_xml::events::Event;
use s3s::config::{S3Config, StaticConfigProvider};
use s3s::{S3Error, s3_error};
use skys3_types::limits::MAX_SINGLE_PUT_BYTES;

/// The longest object key, in bytes of UTF-8 (the S3 limit).
pub const MAX_KEY_BYTES: usize = 1024;

/// The highest part number of a multipart upload, which is also the most
/// parts one upload can have (the S3 limit).
pub const MAX_PART_NUMBER: u32 = 10_000;

/// The longest `Range` header value. One range of two 19-digit offsets
/// needs 45 bytes; S3 serves one range per request.
pub const MAX_RANGE_HEADER_BYTES: usize = 64;

/// Bounds on what a request may carry before the gateway buffers or parses
/// it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestLimits {
    /// The most header fields (hyper's HTTP/1 default).
    pub max_header_count: usize,
    /// The most bytes in header names and values together. S3 allows 8 KiB
    /// of headers plus 2 KiB of user metadata; this leaves room for long
    /// session tokens.
    pub max_header_bytes: usize,
    /// The longest request target, path and query, in bytes. A presigned
    /// URL for a 1,024-byte key, percent-encoded, with a session token fits.
    pub max_uri_bytes: usize,
    /// The largest XML request body. A `CompleteMultipartUpload` of 10,000
    /// parts with checksums is about 2.5 MiB.
    pub max_xml_body_bytes: usize,
    /// The deepest element nesting in an XML request body. The deepest S3
    /// request schema nests about 8 levels.
    pub max_xml_depth: usize,
}

impl Default for RequestLimits {
    fn default() -> Self {
        Self {
            max_header_count: 100,
            max_header_bytes: 16 * 1024,
            max_uri_bytes: 16 * 1024,
            max_xml_body_bytes: 4 * 1024 * 1024,
            max_xml_depth: 32,
        }
    }
}

/// What a request addresses, read from its path-style target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Target {
    /// `/`: ListBuckets.
    Service,
    /// `/bucket`: a bucket operation.
    Bucket,
    /// `/bucket/key`: an object operation.
    Object,
}

/// How the gateway treats a request's body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BodyKind {
    /// Object data (PutObject, UploadPart) or a POST form, streamed to
    /// `s3s` under its own size limits.
    Stream,
    /// Empty or an XML document: buffered and scanned before `s3s` parses
    /// it.
    Xml,
}

/// A request's head, as the limit checks classified it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RequestShape {
    pub(crate) target: Target,
    pub(crate) body: BodyKind,
}

impl RequestShape {
    /// Whether the request is CreateBucket: `PUT /bucket` with no query.
    pub(crate) fn is_create_bucket(&self, parts: &Parts) -> bool {
        self.target == Target::Bucket && parts.method == Method::PUT && parts.uri.query().is_none()
    }
}

/// Query parameters whose PUT body on an object is XML, not object data.
const XML_OBJECT_SUBRESOURCES: [&str; 4] = ["acl", "legal-hold", "retention", "tagging"];

impl RequestLimits {
    /// Checks a request's head and classifies it.
    ///
    /// # Errors
    ///
    /// The S3 error for the first limit the request breaks:
    /// `RequestHeaderSectionTooLarge`, `InvalidURI`, `KeyTooLongError`,
    /// `InvalidArgument` (part number or range), or
    /// `MaxMessageLengthExceeded` for an XML body that declares a length
    /// above the limit.
    pub(crate) fn check_head(&self, parts: &Parts) -> Result<RequestShape, S3Error> {
        self.check_headers(&parts.headers)?;
        let target_len = parts.uri.path_and_query().map_or(0, |pq| pq.as_str().len());
        if target_len > self.max_uri_bytes {
            return Err(s3_error!(
                InvalidURI,
                "The request target is {target_len} bytes; the limit is {}.",
                self.max_uri_bytes
            ));
        }
        let target = match parts.uri.path().trim_start_matches('/').split_once('/') {
            None if parts.uri.path().trim_start_matches('/').is_empty() => Target::Service,
            None | Some((_, "")) => Target::Bucket,
            Some((_, key)) => {
                let len = percent_decoded_len(key);
                if len > MAX_KEY_BYTES {
                    return Err(s3_error!(
                        KeyTooLongError,
                        "The key is {len} bytes; the limit is {MAX_KEY_BYTES}."
                    ));
                }
                Target::Object
            }
        };
        let query = parts.uri.query().unwrap_or("");
        for (name, value) in query_pairs(query) {
            if name == "partNumber" && !valid_part_number(value) {
                return Err(s3_error!(
                    InvalidArgument,
                    "Part number must be an integer between 1 and {MAX_PART_NUMBER}, inclusive"
                ));
            }
        }
        if let Some(range) = parts.headers.get(http::header::RANGE)
            && range.len() > MAX_RANGE_HEADER_BYTES
        {
            return Err(s3_error!(
                InvalidArgument,
                "The Range header is longer than {MAX_RANGE_HEADER_BYTES} bytes; \
                 one byte range is served per request"
            ));
        }
        let streams = match (&parts.method, &target) {
            (&Method::PUT, Target::Object) => {
                !query_pairs(query).any(|(name, _)| XML_OBJECT_SUBRESOURCES.contains(&name))
            }
            (&Method::POST, Target::Bucket) => is_form(&parts.headers),
            _ => false,
        };
        let body = if streams {
            BodyKind::Stream
        } else {
            let declared = parts
                .headers
                .get(http::header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok()?.parse::<u64>().ok());
            if declared.is_some_and(|len| len > self.max_xml_body_bytes as u64) {
                return Err(self.xml_too_large());
            }
            BodyKind::Xml
        };
        Ok(RequestShape { target, body })
    }

    fn check_headers(&self, headers: &HeaderMap) -> Result<(), S3Error> {
        let count = headers.len();
        let bytes: usize = headers
            .iter()
            .map(|(name, value)| name.as_str().len() + value.len())
            .sum();
        if count > self.max_header_count || bytes > self.max_header_bytes {
            return Err(s3_error!(
                RequestHeaderSectionTooLarge,
                "The request has {count} header fields of {bytes} bytes; the limits are {} \
                 fields and {} bytes.",
                self.max_header_count,
                self.max_header_bytes
            ));
        }
        Ok(())
    }

    pub(crate) fn xml_too_large(&self) -> S3Error {
        s3_error!(
            MaxMessageLengthExceeded,
            "The request body is longer than {} bytes.",
            self.max_xml_body_bytes
        )
    }

    /// Checks that an XML request body nests at most
    /// [`RequestLimits::max_xml_depth`] elements deep, is well formed as
    /// far as nesting goes, and has no document type declaration.
    ///
    /// # Errors
    ///
    /// `MalformedXML`.
    pub(crate) fn check_xml(&self, body: &[u8]) -> Result<(), S3Error> {
        let malformed = |why: &str| {
            s3_error!(
                MalformedXML,
                "The XML you provided was not well formed: {why}"
            )
        };
        let mut reader = Reader::from_reader(body);
        let mut depth = 0_usize;
        loop {
            let opened = match reader.read_event() {
                Ok(Event::Start(_)) => {
                    depth += 1;
                    depth
                }
                Ok(Event::Empty(_)) => depth + 1,
                Ok(Event::End(_)) => {
                    depth = depth.saturating_sub(1);
                    continue;
                }
                Ok(Event::DocType(_)) => return Err(malformed("it declares a document type")),
                Ok(Event::Eof) if depth == 0 => return Ok(()),
                Ok(Event::Eof) => return Err(malformed("an element is not closed")),
                Ok(_) => continue,
                // The parser's message may quote the body, which need not be text.
                Err(_) => return Err(malformed("it is not well-formed XML")),
            };
            if opened > self.max_xml_depth {
                return Err(malformed(&format!(
                    "elements nest more than {} deep",
                    self.max_xml_depth
                )));
            }
        }
    }

    /// The `s3s` configuration that applies these limits inside `s3s`.
    pub(crate) fn s3s_config(&self) -> Arc<StaticConfigProvider> {
        let mut config = S3Config::default();
        config.xml_max_body_size = self.max_xml_body_bytes;
        config.put_object_max_size = Some(MAX_SINGLE_PUT_BYTES);
        config.post_object_max_file_size = MAX_SINGLE_PUT_BYTES;
        // A POST form's fields other than the file: the policy, signature,
        // and metadata, each a few KiB at most.
        config.form_max_field_size = 64 * 1024;
        config.form_max_fields_size = 1024 * 1024;
        config.form_max_parts = 100;
        Arc::new(StaticConfigProvider::new(Arc::new(config)))
    }
}

/// Whether the request body is an HTML form (POST Object).
fn is_form(headers: &HeaderMap) -> bool {
    headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .trim_start()
                .get(..19)
                .is_some_and(|start| start.eq_ignore_ascii_case("multipart/form-data"))
        })
}

/// The `name=value` pairs of a query string, undecoded. A pair without `=`
/// has an empty value.
pub(crate) fn query_pairs(query: &str) -> impl Iterator<Item = (&str, &str)> {
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| pair.split_once('=').unwrap_or((pair, "")))
}

fn valid_part_number(value: &str) -> bool {
    value.len() <= 5
        && value.bytes().all(|b| b.is_ascii_digit())
        && value
            .parse::<u32>()
            .is_ok_and(|n| (1..=MAX_PART_NUMBER).contains(&n))
}

/// The length of `text` once its `%XX` escapes are decoded. A `%` not
/// followed by two hex digits counts as itself.
pub(crate) fn percent_decoded_len(text: &str) -> usize {
    let bytes = text.as_bytes();
    let mut len = 0;
    let mut i = 0;
    while i < bytes.len() {
        let escaped = bytes[i] == b'%'
            && bytes.get(i + 1).is_some_and(u8::is_ascii_hexdigit)
            && bytes.get(i + 2).is_some_and(u8::is_ascii_hexdigit);
        i += if escaped { 3 } else { 1 };
        len += 1;
    }
    len
}

#[cfg(test)]
mod tests {
    use http::Request;
    use proptest::prelude::*;
    use s3s::S3ErrorCode;

    use super::*;

    fn parts(method: Method, uri: &str, headers: &[(&str, &str)]) -> Parts {
        let mut request = Request::builder().method(method).uri(uri);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        request.body(()).unwrap().into_parts().0
    }

    fn shape(method: Method, uri: &str, headers: &[(&str, &str)]) -> Result<RequestShape, S3Error> {
        RequestLimits::default().check_head(&parts(method, uri, headers))
    }

    fn code(result: Result<impl std::fmt::Debug, S3Error>) -> S3ErrorCode {
        result.unwrap_err().code().clone()
    }

    #[test]
    fn targets_and_bodies_are_classified() {
        let cases = [
            (Method::GET, "/", Target::Service, BodyKind::Xml),
            (Method::PUT, "/b", Target::Bucket, BodyKind::Xml),
            (Method::PUT, "/b/", Target::Bucket, BodyKind::Xml),
            (Method::PUT, "/b/k", Target::Object, BodyKind::Stream),
            (
                Method::PUT,
                "/b/a/b/c?partNumber=3&uploadId=x",
                Target::Object,
                BodyKind::Stream,
            ),
            (Method::PUT, "/b/k?tagging", Target::Object, BodyKind::Xml),
            (Method::PUT, "/b/k?acl", Target::Object, BodyKind::Xml),
            (Method::POST, "/b?delete", Target::Bucket, BodyKind::Xml),
            (
                Method::POST,
                "/b/k?uploadId=1",
                Target::Object,
                BodyKind::Xml,
            ),
        ];
        for (method, uri, target, body) in cases {
            let shape = shape(method.clone(), uri, &[]).unwrap();
            assert_eq!((shape.target, shape.body), (target, body), "{method} {uri}");
        }
        let form = [("content-type", "Multipart/Form-Data; boundary=x")];
        assert_eq!(
            shape(Method::POST, "/b", &form).unwrap().body,
            BodyKind::Stream
        );
        assert_eq!(
            shape(Method::POST, "/b/k", &form).unwrap().body,
            BodyKind::Xml
        );
        let create = parts(Method::PUT, "/b", &[]);
        assert!(
            shape(Method::PUT, "/b", &[])
                .unwrap()
                .is_create_bucket(&create)
        );
        let acl = parts(Method::PUT, "/b?acl", &[]);
        assert!(
            !shape(Method::PUT, "/b?acl", &[])
                .unwrap()
                .is_create_bucket(&acl)
        );
    }

    #[test]
    fn header_sections_are_bounded() {
        let limits = RequestLimits {
            max_header_count: 2,
            max_header_bytes: 20,
            ..RequestLimits::default()
        };
        let check = |headers: &[(&str, &str)]| limits.check_head(&parts(Method::GET, "/", headers));
        check(&[("a", "1"), ("b", "2")]).unwrap();
        assert_eq!(
            code(check(&[("a", "1"), ("b", "2"), ("c", "3")])),
            S3ErrorCode::RequestHeaderSectionTooLarge
        );
        assert_eq!(
            code(check(&[("a", "0123456789abcdefghij")])),
            S3ErrorCode::RequestHeaderSectionTooLarge
        );
    }

    #[test]
    fn uris_and_keys_are_bounded() {
        let key = "k".repeat(MAX_KEY_BYTES);
        shape(Method::GET, &format!("/b/{key}"), &[]).unwrap();
        let long = format!("/b/{key}k");
        assert_eq!(
            code(shape(Method::GET, &long, &[])),
            S3ErrorCode::KeyTooLongError
        );
        // Escapes count once decoded: 342 three-byte characters.
        let encoded = "%E2%82%AC".repeat(342);
        assert_eq!(
            code(shape(Method::GET, &format!("/b/{encoded}"), &[])),
            S3ErrorCode::KeyTooLongError
        );
        shape(Method::GET, &format!("/b/{}", "%E2%82%AC".repeat(341)), &[]).unwrap();
        let limits = RequestLimits {
            max_uri_bytes: 8,
            ..RequestLimits::default()
        };
        let result = limits.check_head(&parts(Method::GET, "/b/k?x=123", &[]));
        assert_eq!(code(result), S3ErrorCode::InvalidURI);
    }

    #[test]
    fn part_numbers_and_ranges_are_bounded() {
        for good in ["1", "10000", "00001"] {
            shape(
                Method::PUT,
                &format!("/b/k?partNumber={good}&uploadId=u"),
                &[],
            )
            .unwrap();
        }
        for bad in ["0", "10001", "", "-1", "1.5", "99999999999", "+1"] {
            let uri = format!("/b/k?partNumber={bad}&uploadId=u");
            assert_eq!(
                code(shape(Method::PUT, &uri, &[])),
                S3ErrorCode::InvalidArgument,
                "{bad}"
            );
        }
        shape(
            Method::GET,
            "/b/k",
            &[("range", "bytes=0-9223372036854775806")],
        )
        .unwrap();
        let long = format!("bytes=0-{}", "9".repeat(60));
        assert_eq!(
            code(shape(Method::GET, "/b/k", &[("range", &long)])),
            S3ErrorCode::InvalidArgument
        );
    }

    #[test]
    fn declared_xml_bodies_are_bounded() {
        let limits = RequestLimits::default();
        let too_long = (limits.max_xml_body_bytes + 1).to_string();
        let result = shape(Method::POST, "/b?delete", &[("content-length", &too_long)]);
        assert_eq!(code(result), S3ErrorCode::MaxMessageLengthExceeded);
        // Object data is bounded by s3s's object size limit instead.
        shape(Method::PUT, "/b/k", &[("content-length", &too_long)]).unwrap();
    }

    #[test]
    fn xml_depth_and_doctypes_are_checked() {
        let limits = RequestLimits {
            max_xml_depth: 3,
            ..RequestLimits::default()
        };
        limits.check_xml(b"").unwrap();
        limits
            .check_xml(b"<?xml version=\"1.0\"?><a><b><c/></b></a>")
            .unwrap();
        limits
            .check_xml(b"<a><b><c>text</c></b><!-- x --></a>")
            .unwrap();
        for bad in [
            &b"<a><b><c><d/></c></b></a>"[..],
            b"<a><b><c><d>",
            b"<a><b></c></a>",
            b"<a>",
            b"<!DOCTYPE a [<!ENTITY x \"y\">]><a/>",
        ] {
            let error = limits.check_xml(bad).unwrap_err();
            assert_eq!(*error.code(), S3ErrorCode::MalformedXML, "{bad:?}");
        }
    }

    #[test]
    fn s3s_gets_the_same_limits() {
        let limits = RequestLimits::default();
        let config = s3s::config::S3ConfigProvider::snapshot(&*limits.s3s_config());
        assert_eq!(config.xml_max_body_size, limits.max_xml_body_bytes);
        assert_eq!(config.put_object_max_size, Some(MAX_SINGLE_PUT_BYTES));
    }

    /// `depth` nested elements.
    fn nested(depth: usize) -> String {
        format!("{}{}", "<e>".repeat(depth), "</e>".repeat(depth))
    }

    proptest! {
        #[test]
        fn nesting_is_accepted_up_to_the_limit(depth in 0_usize..80, max in 1_usize..64) {
            let limits = RequestLimits { max_xml_depth: max, ..RequestLimits::default() };
            prop_assert_eq!(limits.check_xml(nested(depth).as_bytes()).is_ok(), depth <= max);
        }

        #[test]
        fn arbitrary_bodies_never_panic(body in proptest::collection::vec(any::<u8>(), 0..512)) {
            let _ = RequestLimits::default().check_xml(&body);
        }

        #[test]
        fn decoded_lengths_match_a_decoder(bytes in proptest::collection::vec(any::<u8>(), 0..64)) {
            let encoded: String = bytes.iter().map(|b| format!("%{b:02X}")).collect();
            prop_assert_eq!(percent_decoded_len(&encoded), bytes.len());
            let plain = String::from_utf8_lossy(&bytes).replace('%', "");
            prop_assert_eq!(percent_decoded_len(&plain), plain.len());
        }
    }
}
