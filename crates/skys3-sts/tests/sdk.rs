//! The AWS SDK for Rust's web-identity provider against the gateway's
//! listener: configured as a workload is, with `AWS_ENDPOINT_URL_STS`,
//! `AWS_ROLE_ARN`, and `AWS_WEB_IDENTITY_TOKEN_FILE`, the default
//! credential chain obtains session credentials from SkyS3's STS endpoint,
//! signs S3 requests with them, and refreshes them once they expire.

mod common;

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_config::{BehaviorVersion, SdkConfig};
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider, future};
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::{Credentials, SharedHttpClient};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_smithy_async::time::TimeSource;
use aws_smithy_http_client::tls::{self, rustls_provider::CryptoMode};
use aws_types::os_shim_internal::Env;
use common::{Node, ROLE_ARN, now};
use serde_json::json;
use skys3_gateway::GatewayListener;
use skys3_io::{ManualWallClock, WallClock};
use tokio::sync::oneshot;

/// The node's manual clock as the SDK's time source, so the SDK signs and
/// caches credentials on the node's time.
#[derive(Debug, Clone)]
struct NodeTime(ManualWallClock);

impl TimeSource for NodeTime {
    fn now(&self) -> SystemTime {
        UNIX_EPOCH + self.0.now()
    }
}

/// A credentials provider that counts how often it is asked.
#[derive(Debug)]
struct Counting {
    inner: SharedCredentialsProvider,
    calls: Arc<AtomicUsize>,
}

impl ProvideCredentials for Counting {
    fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.provide_credentials()
    }
}

fn http_client() -> SharedHttpClient {
    aws_smithy_http_client::Builder::new()
        .tls_provider(tls::Provider::Rustls(CryptoMode::AwsLc))
        .build_https()
}

/// The SDK configuration a workload has, with its environment pointing
/// STS at `addr` and its web identity at `token_file`.
async fn workload_config(addr: SocketAddr, dir: &Path, token_file: &Path) -> SdkConfig {
    let endpoint = format!("http://{addr}");
    let missing = dir.join("no-such-file");
    let (token_file, missing) = (
        token_file.to_str().unwrap().to_owned(),
        missing.to_str().unwrap().to_owned(),
    );
    let env = Env::from_slice(&[
        ("AWS_REGION", "us-east-1"),
        ("AWS_ENDPOINT_URL_STS", endpoint.as_str()),
        ("AWS_ROLE_ARN", ROLE_ARN),
        ("AWS_ROLE_SESSION_NAME", "sdk-test"),
        ("AWS_WEB_IDENTITY_TOKEN_FILE", token_file.as_str()),
        // No shared configuration files: the web identity is all there is.
        ("AWS_CONFIG_FILE", missing.as_str()),
        ("AWS_SHARED_CREDENTIALS_FILE", missing.as_str()),
        ("AWS_EC2_METADATA_DISABLED", "true"),
    ]);
    aws_config::defaults(BehaviorVersion::latest())
        .env(env)
        .http_client(http_client())
        .load()
        .await
}

/// An S3 client for the gateway at `addr` that signs on `clock`'s time.
fn s3_client(
    sdk: &SdkConfig,
    addr: SocketAddr,
    clock: &ManualWallClock,
    credentials: impl ProvideCredentials + 'static,
) -> aws_sdk_s3::Client {
    let config = aws_sdk_s3::config::Builder::from(sdk)
        .endpoint_url(format!("http://{addr}"))
        .force_path_style(true)
        .time_source(NodeTime(clock.clone()))
        .credentials_provider(credentials)
        .retry_config(RetryConfig::disabled())
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_sdk_web_identity_provider_obtains_and_refreshes_credentials() {
    let node = Node::start(now()).await;
    let listener = GatewayListener::bind("127.0.0.1:0".parse().unwrap(), node.gateway.clone())
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let serving = tokio::spawn(listener.serve(async {
        let _ = stopped.await;
    }));

    let dir = tempfile::tempdir().unwrap();
    let token_file = dir.path().join("token");
    let subject = "system:serviceaccount:ci:builder";
    std::fs::write(&token_file, node.token(subject, json!({}))).unwrap();
    let sdk = workload_config(addr, dir.path(), &token_file).await;
    let chain = sdk.credentials_provider().unwrap();

    // The default chain finds the web identity and gets a session.
    let first = chain.provide_credentials().await.unwrap();
    assert!(first.access_key_id().starts_with("ASIA"), "{first:?}");
    assert!(first.session_token().is_some());
    let expiry = first.expiry().unwrap().duration_since(UNIX_EPOCH).unwrap();
    assert_eq!(expiry.as_secs(), node.clock.now().as_secs() + 1800);

    // An S3 client gets its credentials from the chain. The role's policy
    // allows `deploy-*` buckets and listing, and nothing else (M1-07b's
    // authorization).
    let calls = Arc::new(AtomicUsize::new(0));
    let counting = Counting {
        inner: chain.clone(),
        calls: Arc::clone(&calls),
    };
    let s3 = s3_client(&sdk, addr, &node.clock, counting);
    s3.create_bucket()
        .bucket("deploy-app")
        .send()
        .await
        .unwrap();
    let listed = s3.list_buckets().send().await.unwrap();
    let names: Vec<_> = listed.buckets().iter().filter_map(|b| b.name()).collect();
    assert_eq!(names, ["deploy-app"]);
    let denied = s3.create_bucket().bucket("other").send().await.unwrap_err();
    assert_eq!(denied.code(), Some("AccessDenied"), "{denied:?}");
    assert_eq!(calls.load(Ordering::SeqCst), 1, "the session is cached");

    // The session expires. The SDK's identity cache notices on the node's
    // clock and asks the chain again, which reads a fresh token from the
    // file and gets a new session.
    node.clock.advance(Duration::from_secs(1801));
    node.sync().await.unwrap();
    std::fs::write(&token_file, node.token(subject, json!({}))).unwrap();
    s3.head_bucket().bucket("deploy-app").send().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2, "the session was refreshed");
    assert_eq!(node.sessions.len(), 3);

    // The first session, used past its expiry, is refused.
    let stale = Credentials::new(
        first.access_key_id(),
        first.secret_access_key(),
        first.session_token().map(str::to_owned),
        None,
        "expired",
    );
    let expired = s3_client(&sdk, addr, &node.clock, stale);
    let error = expired.list_buckets().send().await.unwrap_err();
    assert_eq!(error.code(), Some("ExpiredToken"), "{error:?}");

    // A token whose subject the trust policy does not accept gets no
    // session.
    std::fs::write(
        &token_file,
        node.token("system:serviceaccount:prod:builder", json!({})),
    )
    .unwrap();
    let error = chain.provide_credentials().await.unwrap_err();
    assert!(format!("{error:?}").contains("AccessDenied"), "{error:?}");

    let _ = stop.send(());
    serving.await.unwrap();
}
