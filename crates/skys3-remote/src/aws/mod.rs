//! The remote-target client: an [`ObjectStore`](crate::ObjectStore) over
//! the AWS SDK for S3 (design §7, §11).
//!
//! [`AwsS3`] talks to one bucket of AWS S3 or of an S3-compatible provider.
//! It is built from the bucket's target (`RemoteTarget`, the parsed form of a
//! configured target URL), a region, and a credentials provider:
//!
//! - **Endpoints and addressing.** Requests go to the target's endpoint.
//!   AWS S3 endpoints (`*.amazonaws.com`) use virtual-hosted-style
//!   addressing, and every other endpoint path-style addressing, which
//!   S3-compatible providers support; [`Addressing`] overrides the choice.
//! - **Credentials** come from `aws-config` providers:
//!   [`default_credentials`] is the SDK's default chain (environment,
//!   profiles, web identity, container and instance metadata), and
//!   [`web_identity_credentials`] assumes a role with a web-identity token
//!   file, as a workload does (§11).
//! - **No retries.** The SDK's retries are off: every method is one
//!   request, and the caller retries (the [`ObjectStore`](crate::ObjectStore)
//!   contract).
//! - **Errors** map to [`S3Error`](crate::S3Error) with the store's status
//!   and code. A request that timed out or lost its connection is
//!   [`S3ErrorKind::Timeout`](crate::S3ErrorKind::Timeout), which may have
//!   been applied.
//! - **Checksums.** The SDK adds flexible checksums only where S3 requires
//!   them, since not every S3-compatible provider accepts them. Bodies are
//!   signed (`x-amz-content-sha256`), so the store still verifies what it
//!   receives.
//! - **TLS** is rustls with the `aws-lc-rs` provider, the SDK's default and
//!   the one the rest of SkyS3 uses, and the platform's root certificates.
//!   Credential providers share the same HTTP client.
//!
//! Keys are keys in the bucket: a target's key prefix is the caller's to
//! apply, as it is for the probe
//! ([`ConditionalProbe`](crate::probe::ConditionalProbe)).
//!
//! ```
//! use skys3_remote::aws::{AwsS3, Credentials, SharedCredentialsProvider};
//! use skys3_types::RemoteTarget;
//!
//! let target = RemoteTarget {
//!     endpoint: "https://s3.us-west-2.amazonaws.com".to_owned(),
//!     bucket: "example-bucket".to_owned(),
//!     prefix: None,
//! };
//! let credentials = Credentials::new("AKIDEXAMPLE", "secret", None, None, "example");
//! let store = AwsS3::new(&target, "us-west-2", SharedCredentialsProvider::new(credentials));
//! assert_eq!(store.bucket(), "example-bucket");
//! ```

mod error;
mod store;

use std::path::PathBuf;
use std::time::Duration;

use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::provider_config::ProviderConfig;
use aws_config::web_identity_token::{StaticConfiguration, WebIdentityTokenCredentialsProvider};
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::timeout::TimeoutConfig;
use aws_sdk_s3::config::{
    BehaviorVersion, Region, RequestChecksumCalculation, ResponseChecksumValidation,
    SharedHttpClient,
};
use aws_smithy_http_client::tls::{self, rustls_provider::CryptoMode};
use skys3_types::RemoteTarget;

pub use aws_sdk_s3::config::{Credentials, ProvideCredentials, SharedCredentialsProvider};

/// How requests name the bucket.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Addressing {
    /// Virtual-hosted style for AWS S3 endpoints (`*.amazonaws.com`) and
    /// buckets whose names are valid host labels, path style otherwise.
    #[default]
    Auto,
    /// `https://<endpoint>/<bucket>/<key>`, which S3-compatible providers
    /// support.
    Path,
    /// `https://<bucket>.<endpoint host>/<key>`.
    VirtualHosted,
}

impl Addressing {
    /// Whether requests to `bucket` at `endpoint` use path style.
    fn path_style(self, endpoint: &str, bucket: &str) -> bool {
        match self {
            Addressing::Path => true,
            Addressing::VirtualHosted => false,
            Addressing::Auto => !(is_aws_endpoint(endpoint) && is_dns_compatible(bucket)),
        }
    }
}

/// Whether `endpoint`'s host is an AWS domain.
fn is_aws_endpoint(endpoint: &str) -> bool {
    let authority = endpoint
        .split_once("://")
        .map_or(endpoint, |(_, rest)| rest)
        .split('/')
        .next()
        .unwrap_or_default();
    let host = match authority.rsplit_once(':') {
        Some((host, port)) if port.bytes().all(|b| b.is_ascii_digit()) => host,
        _ => authority,
    };
    let host = host.to_ascii_lowercase();
    host.ends_with(".amazonaws.com") || host.ends_with(".amazonaws.com.cn")
}

/// Whether `bucket` can be a host label under TLS: no dots, which a
/// wildcard certificate does not cover, and no characters outside DNS
/// labels.
fn is_dns_compatible(bucket: &str) -> bool {
    !bucket.is_empty()
        && bucket.len() <= 63
        && bucket
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !bucket.starts_with('-')
        && !bucket.ends_with('-')
}

/// Builds an [`AwsS3`] with settings beyond the target, region, and
/// credentials.
#[derive(Clone, Debug)]
pub struct AwsS3Builder {
    endpoint: String,
    bucket: String,
    region: String,
    credentials: SharedCredentialsProvider,
    addressing: Addressing,
    connect_timeout: Duration,
    attempt_timeout: Option<Duration>,
}

impl AwsS3Builder {
    /// The default connect timeout.
    pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

    /// Sets how requests name the bucket.
    pub fn addressing(mut self, addressing: Addressing) -> Self {
        self.addressing = addressing;
        self
    }

    /// Sets the time allowed to establish a connection, 10 seconds by
    /// default.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Bounds each request, from sending it to reading its whole response.
    /// There is no bound by default: a part upload over a slow link can
    /// take long, and the SDK's stalled-stream protection already fails a
    /// transfer that stops making progress.
    pub fn attempt_timeout(mut self, timeout: Duration) -> Self {
        self.attempt_timeout = Some(timeout);
        self
    }

    /// Builds the client.
    pub fn build(self) -> AwsS3 {
        let path_style = self.addressing.path_style(&self.endpoint, &self.bucket);
        let mut timeouts = TimeoutConfig::builder().connect_timeout(self.connect_timeout);
        if let Some(timeout) = self.attempt_timeout {
            timeouts = timeouts.operation_attempt_timeout(timeout);
        }
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .http_client(https_client())
            .endpoint_url(&self.endpoint)
            .force_path_style(path_style)
            .region(Region::new(self.region))
            .credentials_provider(self.credentials)
            .retry_config(RetryConfig::disabled())
            .timeout_config(timeouts.build())
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
            .build();
        AwsS3 {
            client: aws_sdk_s3::Client::from_conf(config),
            bucket: self.bucket,
            path_style,
        }
    }
}

/// One bucket of AWS S3 or of an S3-compatible provider. See the [module
/// documentation](self).
///
/// Clones share the client and its connection pool.
#[derive(Clone, Debug)]
pub struct AwsS3 {
    client: aws_sdk_s3::Client,
    bucket: String,
    path_style: bool,
}

impl AwsS3 {
    /// A client for `target`'s bucket in `region`, with default settings.
    /// For S3-compatible providers, `region` is the one they sign with,
    /// such as `auto` for Cloudflare R2 or `us-east-1` for most others.
    pub fn new(
        target: &RemoteTarget,
        region: impl Into<String>,
        credentials: SharedCredentialsProvider,
    ) -> Self {
        Self::builder(target, region, credentials).build()
    }

    /// A builder for `target`'s bucket in `region`.
    pub fn builder(
        target: &RemoteTarget,
        region: impl Into<String>,
        credentials: SharedCredentialsProvider,
    ) -> AwsS3Builder {
        AwsS3Builder {
            endpoint: target.endpoint.clone(),
            bucket: target.bucket.clone(),
            region: region.into(),
            credentials,
            addressing: Addressing::Auto,
            connect_timeout: AwsS3Builder::DEFAULT_CONNECT_TIMEOUT,
            attempt_timeout: None,
        }
    }

    /// The bucket's name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Whether requests use path-style addressing.
    pub fn is_path_style(&self) -> bool {
        self.path_style
    }
}

/// The HTTP client of the S3 client and the credential providers: rustls
/// with the `aws-lc-rs` provider.
fn https_client() -> SharedHttpClient {
    aws_smithy_http_client::Builder::new()
        .tls_provider(tls::Provider::Rustls(CryptoMode::AwsLc))
        .build_https()
}

/// The configuration credential providers load with: `region` for STS, and
/// the shared HTTP client.
fn provider_config(region: &str) -> ProviderConfig {
    ProviderConfig::without_region()
        .with_region(Some(Region::new(region.to_owned())))
        .with_http_client(https_client())
}

/// The SDK's default credential chain: environment variables, the shared
/// config and credentials files (including `web_identity_token_file` and
/// `credential_process` profiles), a web identity from
/// `AWS_WEB_IDENTITY_TOKEN_FILE` and `AWS_ROLE_ARN`, and container and
/// instance metadata. `region` is the STS region for providers that assume
/// roles.
///
/// Building the chain resolves nothing; credentials are loaded and cached
/// on first use.
pub async fn default_credentials(region: &str) -> SharedCredentialsProvider {
    // Without an explicit region, the chain would look one up, from
    // instance metadata if need be, and use it over the configured one.
    let chain = DefaultCredentialsChain::builder()
        .region(Region::new(region.to_owned()))
        .configure(provider_config(region))
        .build()
        .await;
    SharedCredentialsProvider::new(chain)
}

/// Credentials from STS `AssumeRoleWithWebIdentity`, with the token read
/// from `token_file` on every refresh, as a workload identity provider
/// rotates it (§11). `region` is the STS region.
pub fn web_identity_credentials(
    token_file: impl Into<PathBuf>,
    role_arn: impl Into<String>,
    session_name: impl Into<String>,
    region: &str,
) -> SharedCredentialsProvider {
    let provider = WebIdentityTokenCredentialsProvider::builder()
        .configure(&provider_config(region))
        .static_configuration(StaticConfiguration {
            web_identity_token_file: token_file.into(),
            role_arn: role_arn.into(),
            session_name: session_name.into(),
        })
        .build();
    SharedCredentialsProvider::new(provider)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_addressing_uses_virtual_hosts_only_on_aws() {
        let auto = Addressing::Auto;
        assert!(!auto.path_style("https://s3.us-west-2.amazonaws.com", "bucket"));
        assert!(!auto.path_style("https://S3.AMAZONAWS.COM:443/", "bucket-1"));
        assert!(!auto.path_style("https://s3.cn-north-1.amazonaws.com.cn", "b"));
        assert!(auto.path_style("https://s3.us-west-2.amazonaws.com", "my.bucket"));
        assert!(auto.path_style("https://s3.amazonaws.com", "Upper"));
        assert!(auto.path_style("https://s3.amazonaws.com", "-dash"));
        assert!(auto.path_style("https://s3.amazonaws.com", ""));
        assert!(auto.path_style("https://account.r2.cloudflarestorage.com", "bucket"));
        assert!(auto.path_style("http://10.0.0.5:9000", "bucket"));
        assert!(auto.path_style("http://[fd00::1]:9000", "bucket"));
        assert!(auto.path_style("s3.amazonaws.com.example", "bucket"));
        assert!(Addressing::Path.path_style("https://s3.amazonaws.com", "bucket"));
        assert!(!Addressing::VirtualHosted.path_style("http://minio:9000", "bucket"));
    }

    #[test]
    fn builder_applies_settings() {
        let target = RemoteTarget {
            endpoint: "https://s3.eu-central-1.amazonaws.com".to_owned(),
            bucket: "bucket".to_owned(),
            prefix: Some("team/".to_owned()),
        };
        let credentials =
            SharedCredentialsProvider::new(Credentials::new("AKID", "secret", None, None, "test"));
        let store = AwsS3::builder(&target, "eu-central-1", credentials.clone())
            .connect_timeout(Duration::from_secs(1))
            .attempt_timeout(Duration::from_secs(2))
            .build();
        assert!(!store.is_path_style());
        let config = store.client.config();
        assert_eq!(config.region().map(Region::as_ref), Some("eu-central-1"));
        let timeouts = config.timeout_config().unwrap();
        assert_eq!(timeouts.connect_timeout(), Some(Duration::from_secs(1)));
        assert_eq!(
            timeouts.operation_attempt_timeout(),
            Some(Duration::from_secs(2))
        );
        assert_eq!(config.retry_config().unwrap().max_attempts(), 1);

        let path = AwsS3::builder(&target, "eu-central-1", credentials)
            .addressing(Addressing::Path)
            .build();
        assert!(path.is_path_style());
        assert_eq!(path.bucket(), "bucket");
    }
}
