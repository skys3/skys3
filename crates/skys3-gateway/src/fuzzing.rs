//! Entry points for the fuzz targets in the repository's `fuzz/` crate.
//!
//! This module is hidden from the documentation and is not a stable API.
//! [`Harness`] drives arbitrary bytes, as a whole request or as the XML
//! body of a bucket or object operation, through the full request
//! pipeline: limits, rejected features, XML bounds, `s3s` parsing, and the
//! operations, over a fresh in-memory control store and shard stub.

use http::{Method, Request};
use s3s::Body;
use skys3_config::Config;
use skys3_control::{MemoryControlStore, ProposalIds, RetryPolicy, bootstrap};

use crate::buckets::{GatewayConfig, IdSource, MODE_HEADER};
use crate::service::{Gateway, Unauthenticated};
use crate::stub::MemoryShards;

/// The largest response the gateway may give to any request in the
/// harness. Every response to a bucket operation is small, so a larger one
/// means a limit failed.
pub const MAX_RESPONSE_BYTES: usize = 64 * 1024;

/// The bucket every harness gateway starts with.
pub const BUCKET: &str = "fuzz";

/// Requests whose body the structured mode fills with the input.
const XML_REQUESTS: [(Method, &str); 14] = [
    (Method::PUT, "/fuzz-new"),
    (Method::PUT, "/fuzz?versioning"),
    (Method::PUT, "/fuzz?acl"),
    (Method::PUT, "/fuzz?ownershipControls"),
    (Method::PUT, "/fuzz?object-lock"),
    (Method::PUT, "/fuzz?encryption"),
    (Method::PUT, "/fuzz?tagging"),
    (Method::PUT, "/fuzz?lifecycle"),
    (Method::POST, "/fuzz?delete"),
    (Method::PUT, "/fuzz/key?tagging"),
    (Method::PUT, "/fuzz/key?retention"),
    (Method::PUT, "/fuzz/key?legal-hold"),
    (Method::PUT, "/fuzz/key?acl"),
    (Method::POST, "/fuzz/key?uploadId=u"),
];

const METHODS: [Method; 6] = [
    Method::GET,
    Method::PUT,
    Method::POST,
    Method::DELETE,
    Method::HEAD,
    Method::OPTIONS,
];

/// Runs fuzz inputs through fresh gateways.
#[derive(Debug)]
pub struct Harness {
    runtime: tokio::runtime::Runtime,
}

impl Default for Harness {
    fn default() -> Self {
        Self::new()
    }
}

impl Harness {
    /// A harness with its own single-threaded runtime.
    ///
    /// # Panics
    ///
    /// If the runtime cannot be built.
    #[must_use]
    pub fn new() -> Self {
        Self {
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("a current-thread runtime builds"),
        }
    }

    /// Answers the request `data` encodes, and returns the response's
    /// status code and body length, or `None` for bytes that are not a
    /// valid HTTP request, which hyper would have refused.
    ///
    /// If the first byte has its high bit set, the rest is the XML body of
    /// one of a fixed set of operations, chosen by the low bits. Otherwise
    /// the first byte picks the method, and the rest is the request target,
    /// a newline, `name: value` header lines, an empty line, and the body.
    ///
    /// # Panics
    ///
    /// If the gateway cannot be built, or its response body is longer
    /// than [`MAX_RESPONSE_BYTES`].
    #[must_use]
    pub fn request(&self, data: &[u8]) -> Option<(u16, usize)> {
        let request = decode(data)?;
        self.runtime.block_on(async {
            let gateway = gateway().await;
            let response = gateway.handle(request).await;
            let status = response.status().as_u16();
            let mut body = response.into_body();
            let bytes = body
                .store_all_limited(MAX_RESPONSE_BYTES)
                .await
                .expect("responses are small and complete");
            Some((status, bytes.len()))
        })
    }
}

/// A gateway with bucket [`BUCKET`] over fresh in-memory state.
async fn gateway() -> Gateway<Unauthenticated> {
    let config: Config = "[cluster]\ncluster_id = \"fuzz\"\n[control_store]\netcd_endpoints = [\"https://etcd.invalid:2379\"]"
        .parse()
        .expect("the configuration is valid");
    let config = GatewayConfig::new(&config);
    let store = MemoryControlStore::new();
    let mut ids = ProposalIds::seeded(0);
    bootstrap(
        &store,
        &config.cluster_id,
        ids.next_id(),
        &RetryPolicy::default(),
    )
    .await
    .expect("the memory store bootstraps");
    let gateway = Gateway::new(
        config,
        store,
        MemoryShards::new(),
        IdSource::seeded(0),
        Unauthenticated,
    )
    .await
    .expect("the gateway loads");
    let create = Request::put(format!("/{BUCKET}"))
        .header(MODE_HEADER, "local")
        .body(Body::empty())
        .expect("the request is valid");
    assert!(gateway.handle(create).await.status().is_success());
    gateway
}

fn decode(data: &[u8]) -> Option<Request<Body>> {
    let (&first, rest) = data.split_first()?;
    if first & 0x80 != 0 {
        let (method, target) = &XML_REQUESTS[usize::from(first & 0x7f) % XML_REQUESTS.len()];
        return Request::builder()
            .method(method)
            .uri(*target)
            .header(http::header::CONTENT_LENGTH, rest.len())
            .body(Body::from(rest.to_vec()))
            .ok();
    }
    let method = &METHODS[usize::from(first) % METHODS.len()];
    let (head, body) = match rest.windows(2).position(|w| w == b"\n\n") {
        Some(end) => (&rest[..end], &rest[end + 2..]),
        None => (rest, &[][..]),
    };
    let mut lines = head.split(|&b| b == b'\n');
    let target = lines.next()?;
    let mut request = Request::builder().method(method).uri(target);
    for line in lines {
        let colon = line.iter().position(|&b| b == b':')?;
        let value = line[colon + 1..].trim_ascii();
        request = request.header(&line[..colon], value);
    }
    request.body(Body::from(body.to_vec())).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inputs_decode_into_requests() {
        let harness = Harness::default();
        assert_eq!(harness.request(b""), None);
        let (status, len) = harness.request(b"\x00/").unwrap();
        assert_eq!(status, 200, "ListBuckets");
        assert!(len > 0);
        assert_eq!(harness.request(b"\x04/fuzz").unwrap().0, 200, "HeadBucket");
        let versioning = b"\x01/fuzz?versioning\nx-amz-acl: private\n\n<VersioningConfiguration/>";
        assert_eq!(harness.request(versioning).unwrap().0, 501);
        assert_eq!(harness.request(b"\x00/\nno colon"), None);
        let enable =
            b"\x81<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
        assert_eq!(harness.request(enable).unwrap().0, 501);
        assert_eq!(harness.request(b"\x88<Delete><Object>").unwrap().0, 400);
    }
}
