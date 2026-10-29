//! Shared helpers: node configurations in a temporary directory, an S3
//! client, plain HTTP requests to the admin listener, and the binary as a
//! child process ([`process`]).

#![allow(dead_code)]

pub mod process;

use std::path::{Path, PathBuf};
use std::time::Duration;

use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_smithy_http_client::tls::{self, TlsContext, TrustStore, rustls_provider::CryptoMode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// The static access key the test configurations define.
pub const ACCESS_KEY: &str = "AKIASKYS3NODETEST";
/// Its secret.
pub const SECRET_KEY: &str = "skys3-node-test-secret-0123456789abcdef";

/// The test certificates: a CA, and a server certificate for 127.0.0.1
/// and `localhost` that it signed.
pub fn tls_file(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data")
        .join(name)
}

/// A node configuration rooted at `dir`, with two disks, the file control
/// store, local buckets of four shards, small inline bodies so larger ones
/// go in extents, and fast checkpoints and polls. `gateway` and `admin`
/// are listen addresses; `extra` is appended.
pub fn config_text(dir: &Path, gateway: &str, admin: &str, extra: &str) -> String {
    let secret = dir.join("secret");
    std::fs::write(&secret, SECRET_KEY).unwrap();
    let path = |name: &str| dir.join(name).display().to_string();
    format!(
        r#"
[cluster]
cluster_id = "node-test"

[node]
data_dir = "{data}"
disks = ["{disk_a}", "{disk_b}"]

[gateway]
listen = "{gateway}"

[control_store]
backend = "file"
config_poll_interval_seconds = 1

[storage]
inline_max_bytes = 1024
extent_bytes = 65536
group_commit_max_delay_us = 0
index_checkpoint_interval_seconds = 1

[buckets.defaults]
mode = "local"
shards_per_bucket = 4

[identity]
sts_web_identity = false

[identity.static_credentials.tester]
access_key_id = "{ACCESS_KEY}"
secret_access_key_file = "{secret}"
policy = '{{"Version": "2012-10-17", "Statement": {{"Effect": "Allow", "Action": "*", "Resource": "*"}}}}'

[admin]
listen = "{admin}"

[logging]
filter = "info"
{extra}
"#,
        data = path("data"),
        disk_a = path("disk-a"),
        disk_b = path("disk-b"),
        secret = secret.display(),
    )
}

/// An S3 client for the gateway at `endpoint` (`http://...` or
/// `https://...`), trusting the test CA.
pub fn client(endpoint: &str) -> aws_sdk_s3::Client {
    let ca = std::fs::read(tls_file("ca.pem")).unwrap();
    let context = TlsContext::builder()
        .with_trust_store(TrustStore::empty().with_pem_certificate(ca))
        .build()
        .unwrap();
    let http = aws_smithy_http_client::Builder::new()
        .tls_provider(tls::Provider::Rustls(CryptoMode::AwsLc))
        .tls_context(context)
        .build_https();
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .http_client(http)
        .endpoint_url(endpoint)
        .force_path_style(true)
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "test"))
        .retry_config(RetryConfig::disabled())
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

/// A body of `len` bytes that differs from any other `seed`'s.
pub fn body(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| seed.wrapping_add((i % 251) as u8))
        .collect()
}

/// Uploads `data` as `bucket/key`.
pub async fn put(client: &aws_sdk_s3::Client, bucket: &str, key: &str, data: &[u8]) {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(data.to_vec().into())
        .send()
        .await
        .unwrap_or_else(|error| panic!("PUT {bucket}/{key}: {error:?}"));
}

/// Downloads `bucket/key`.
pub async fn get(client: &aws_sdk_s3::Client, bucket: &str, key: &str) -> Vec<u8> {
    let object = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap_or_else(|error| panic!("GET {bucket}/{key}: {error:?}"));
    object.body.collect().await.unwrap().into_bytes().to_vec()
}

/// Sends `GET path` to `addr` over plain HTTP and returns the status and
/// body.
pub async fn http_get(addr: &str, path: &str) -> std::io::Result<(u16, String)> {
    http_request(addr, "GET", path).await
}

/// Sends a bodiless `method path` to `addr` over plain HTTP and returns the
/// status and body.
pub async fn http_request(addr: &str, method: &str, path: &str) -> std::io::Result<(u16, String)> {
    let mut stream = tokio::net::TcpStream::connect(addr).await?;
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: skys3\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    let status = response
        .get(9..12)
        .and_then(|code| code.parse().ok())
        .unwrap_or(0);
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_owned())
        .unwrap_or_default();
    Ok((status, body))
}

/// The JSON body of `GET path` on the admin listener at `addr`.
pub async fn admin_json(addr: &str, path: &str) -> serde_json::Value {
    let (status, body) = http_get(addr, path).await.unwrap();
    assert_eq!(status, 200, "{path}: {body}");
    serde_json::from_str(&body).unwrap_or_else(|error| panic!("{path}: {error}: {body}"))
}

/// Polls `condition` until it holds, for up to 30 seconds.
pub async fn eventually<F, Fut>(what: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    for _ in 0..300 {
        if condition().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting until {what}");
}
