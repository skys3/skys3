//! The SigV4 test suite of the AWS Common Runtime, vendored in
//! `tests/data/aws-sigv4-test-suite` (Apache-2.0, see its `NOTICE`).
//!
//! Each vector is a request signed twice, with an `Authorization` header
//! and as a presigned URL, with its canonical request, string to sign, and
//! signature. The vendored vectors are the ones whose rules S3 shares: the
//! suite's path-normalization and double-encoding vectors are for other
//! services, and its folded-header vector cannot occur over HTTP/1.1, which
//! forbids folding. The suite signs for the service `service` and hashes
//! the body itself instead of sending `x-amz-content-sha256`, so the
//! vectors exercise parsing, canonicalization, and signing directly rather
//! than through the authenticator's S3 rules.

use std::path::{Path, PathBuf};
use std::time::Duration;

use http::{HeaderMap, HeaderName, HeaderValue};

use super::AuthMethod;
use super::canonical::{self, Head, hex, sha256};
use super::params::{self, Signed};

fn suite() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/aws-sigv4-test-suite")
}

/// A signed request from the suite: method, target, headers, and body.
struct Vector {
    method: String,
    target: String,
    headers: HeaderMap,
    body: Vec<u8>,
}

impl Vector {
    fn read(path: &Path) -> Self {
        let text = std::fs::read_to_string(path).unwrap();
        let (head, body) = text.split_once("\n\n").unwrap_or((&text, ""));
        let mut lines = head.lines();
        let request_line = lines.next().unwrap();
        let (method, rest) = request_line.split_once(' ').unwrap();
        let target = rest.strip_suffix(" HTTP/1.1").unwrap();
        let mut headers = HeaderMap::new();
        for line in lines {
            let (name, value) = line.split_once(':').unwrap();
            headers.append(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                // As an HTTP/1.1 parser delivers it.
                HeaderValue::from_str(value.trim_matches([' ', '\t'])).unwrap(),
            );
        }
        Self {
            method: method.to_owned(),
            target: target.to_owned(),
            headers,
            body: body.as_bytes().to_vec(),
        }
    }

    fn head(&self) -> Head<'_> {
        let (path, query) = self.target.split_once('?').unwrap_or((&self.target, ""));
        Head {
            method: &self.method,
            path,
            query,
            headers: &self.headers,
            authority: None,
        }
    }

    /// The signing parameters, read the way the authenticator reads them.
    fn signed(&self, method: AuthMethod) -> Signed {
        let (path, _) = self.target.split_once('?').unwrap_or((&self.target, ""));
        // The parameter parser needs a valid URI. The signing parameters
        // are ASCII; other parameters may not be, and are escaped here.
        let query: String = self
            .head()
            .query
            .bytes()
            .map(|b| {
                if b.is_ascii_graphic() {
                    char::from(b).to_string()
                } else {
                    format!("%{b:02X}")
                }
            })
            .collect();
        let uri = format!("/?{query}");
        let mut request = http::Request::builder()
            .method(self.method.as_str())
            .uri(uri);
        for (name, value) in &self.headers {
            request = request.header(name, value);
        }
        let parts = request.body(()).unwrap().into_parts().0;
        let signed = params::parse(&parts, "service")
            .unwrap_or_else(|error| panic!("{path}: {error:?}"))
            .unwrap();
        assert_eq!(signed.method, method);
        signed
    }
}

/// Checks one form of a vector: canonical request, string to sign, and
/// signature.
fn check(dir: &Path, form: &str, method: AuthMethod) {
    let name = dir.file_name().unwrap().to_string_lossy().into_owned();
    let read = |file: &str| std::fs::read_to_string(dir.join(format!("{form}-{file}"))).unwrap();
    let vector = Vector::read(&dir.join(format!("{form}-signed-request.txt")));
    let signed = vector.signed(method);
    let context: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(dir.join("context.json")).unwrap()).unwrap();
    // 2015-08-30T12:36:00Z.
    assert_eq!(signed.time, Duration::from_secs(1_440_938_160), "{name}");
    assert_eq!(
        signed.access_key_id, context["credentials"]["access_key_id"],
        "{name}"
    );
    let expected_canonical = read("canonical-request.txt");
    // The suite's presigned URLs sign the hash of the empty body, where S3
    // uses UNSIGNED-PAYLOAD.
    let payload_hash = match method {
        AuthMethod::Header => hex(&sha256(&vector.body)),
        AuthMethod::Presigned => expected_canonical.lines().last().unwrap().to_owned(),
    };
    let presigned = method == AuthMethod::Presigned;
    let canonical = canonical::canonical_request(
        &vector.head(),
        &signed.signed_headers,
        &payload_hash,
        presigned,
    )
    .unwrap();
    assert_eq!(
        String::from_utf8(canonical.clone()).unwrap(),
        expected_canonical,
        "{name} {form}"
    );
    let string_to_sign = canonical::string_to_sign(&signed.timestamp, &signed.scope(), &canonical);
    assert_eq!(string_to_sign, read("string-to-sign.txt"), "{name} {form}");
    let secret = context["credentials"]["secret_access_key"]
        .as_str()
        .unwrap();
    let key = canonical::signing_key(
        secret.as_bytes(),
        &signed.date,
        &signed.region,
        &signed.service,
    );
    assert!(
        canonical::verify(&key, string_to_sign.as_bytes(), &signed.signature),
        "{name} {form}"
    );
    if let Some(token) = context["credentials"]["token"].as_str()
        && presigned
    {
        assert_eq!(signed.session_token.as_deref(), Some(token), "{name}");
    }
}

#[test]
fn the_aws_signing_test_suite_passes() {
    let mut count = 0;
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(suite())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.is_dir())
        .collect();
    dirs.sort();
    for dir in dirs {
        check(&dir, "header", AuthMethod::Header);
        // This vector adds the session token to the presigned URL after
        // signing, as STS allows; S3 signs it with the rest of the query.
        if !dir.ends_with("post-sts-header-after") {
            check(&dir, "query", AuthMethod::Presigned);
        }
        count += 1;
    }
    assert_eq!(count, 31, "every vendored vector ran");
}
