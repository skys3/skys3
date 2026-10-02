//! Creating buckets through the S3 API (plan M3-04): a client sends
//! CreateBucket for each bucket to a different node's gateway, which
//! places the bucket's shards on the registered nodes, and then waits
//! until every node's gateway knows every bucket.

use std::time::Duration;

use ::http::{Method, Request, Response, StatusCode};
use bytes::Bytes;
use http_body_util::Full;
use skys3_gateway::{MODE_HEADER, TARGET_HEADER};
use skys3_types::{BucketDocument, BucketMode, NodeId};

use crate::s3::{self, NoAnswer};

/// Attempts of each request: a node may still be starting, or not have
/// learned of a bucket yet.
const ATTEMPTS: usize = 120;

/// The pause between attempts.
const RETRY_DELAY: Duration = Duration::from_millis(250);

/// Creates every bucket of `planned`, the bucket at position `n` through
/// the gateway of node `n mod nodes`, and waits until each node's gateway
/// answers HeadBucket for each bucket. Planned bucket IDs are ignored: the
/// gateway gives each bucket a fresh one.
pub(crate) async fn create(
    planned: Vec<BucketDocument>,
    nodes: Vec<NodeId>,
    timeout: Duration,
) -> turmoil::Result {
    for (n, bucket) in planned.iter().enumerate() {
        let host = nodes[n % nodes.len()].to_string();
        let mut request = Request::put(format!("/{}", bucket.name)).header(
            MODE_HEADER,
            match bucket.mode {
                BucketMode::WriteBack => "write_back",
                _ => "local",
            },
        );
        if let Some(target) = &bucket.target {
            let prefix = target.prefix.as_deref().unwrap_or_default();
            let url = format!("{}/{}/{prefix}", target.endpoint, target.bucket);
            request = request.header(TARGET_HEADER, url);
        }
        let request = request.body(Full::default())?;
        until(&host, request, timeout, |response| {
            // A 409 answers an attempt after one whose answer was lost.
            response.status() == StatusCode::OK
                || (response.status() == StatusCode::CONFLICT
                    && String::from_utf8_lossy(response.body()).contains("BucketAlreadyOwnedByYou"))
        })
        .await
        .map_err(|error| format!("creating {} through {host}: {error}", bucket.name))?;
    }
    for bucket in &planned {
        for node in &nodes {
            let request = Request::builder()
                .method(Method::HEAD)
                .uri(format!("/{}", bucket.name))
                .body(Full::default())?;
            let host = node.to_string();
            until(&host, request, timeout, |response| {
                response.status() == StatusCode::OK
            })
            .await
            .map_err(|error| format!("{host} never learned of {}: {error}", bucket.name))?;
        }
    }
    Ok(())
}

/// Sends `request` to `host` until an answer `done` accepts. A server
/// error, a `404`, or no answer is tried again; any other answer fails.
async fn until(
    host: &str,
    request: Request<Full<Bytes>>,
    timeout: Duration,
    done: impl Fn(&Response<Bytes>) -> bool,
) -> Result<(), String> {
    let mut last = String::from("no attempt");
    for _ in 0..ATTEMPTS {
        let mut attempt = Request::builder()
            .method(request.method().clone())
            .uri(request.uri().clone());
        for (name, value) in request.headers() {
            attempt = attempt.header(name, value);
        }
        let attempt = attempt
            .body(request.body().clone())
            .map_err(|error| error.to_string())?;
        let answer = match s3::connect(host, timeout).await {
            Ok(connection) => connection.send(attempt, timeout).await,
            Err(error) => Err(error),
        };
        match answer {
            Ok(response) if done(&response) => return Ok(()),
            Ok(response)
                if response.status().is_server_error()
                    || response.status() == StatusCode::NOT_FOUND =>
            {
                last = format!("{}", response.status());
            }
            Ok(response) => {
                return Err(format!(
                    "{}: {}",
                    response.status(),
                    String::from_utf8_lossy(response.body())
                ));
            }
            Err(NoAnswer::Io(error) | NoAnswer::Broken(error)) => last = error,
            Err(NoAnswer::Timeout) => last = "no answer in time".to_owned(),
        }
        tokio::time::sleep(RETRY_DELAY).await;
    }
    Err(format!(
        "no success in {ATTEMPTS} attempts; the last: {last}"
    ))
}
