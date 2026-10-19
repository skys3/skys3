//! The SDK matrix (plan M1-25, design §16.2): AWS SDK clients against the
//! real binary, serving HTTPS with STS on the same listener, and an OIDC
//! issuer the node trusts.
//!
//! Every client checks the same features: SDK-default checksums and every
//! other checksum algorithm, `aws-chunked` uploads, multipart uploads,
//! multipart copies by UploadPartCopy (through the SDK's copy helper where
//! it has one), presigned URLs, and web-identity credentials from the default chain,
//! configured only by `AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT_URL_STS`,
//! `AWS_ROLE_ARN`, and `AWS_WEB_IDENTITY_TOKEN_FILE`, refreshed while
//! requests run. Sessions last 900 seconds, the shortest STS allows, and
//! each client sets its SDK's refresh window so that it refreshes within
//! seconds (`SKYS3_REFRESH_SECONDS`). The token file gets a new token
//! every two seconds, and a token stops being accepted seven seconds after
//! its issue at most, so a client that kept a token instead of rereading
//! the file would fail its next refresh within the load phase.
//!
//! The AWS SDK for Rust always runs, in this test, and also checks bucket
//! lifecycle rules, which the node applies every second here. The other clients run
//! when `SKYS3_SDK_CLIENTS` names them (such as `python go javascript java
//! cli s3-tests`): each runs `tests/sdk/run.sh client <name>` with the
//! environment `tests/sdk/run.sh` documents, all at once, in containers
//! unless `SKYS3_SDK_LOCAL=1` runs them on the host. CI's `sdk` job runs
//! them all.

mod support;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aws_config::identity::IdentityCache;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider, future};
use aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextRef;
use aws_sdk_s3::config::{BehaviorVersion, ConfigBag, Intercept, RuntimeComponents};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, ChecksumType, CompletedMultipartUpload, CompletedPart, Delete,
    ObjectIdentifier, Tag, Tagging,
};
use aws_smithy_http_client::tls::{self, TlsContext, TrustStore, rustls_provider::CryptoMode};
use aws_types::os_shim_internal::Env;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use skys3_sts::{OidcProvider, ProviderDocument};
use skys3_types::policy::PolicyDocument;
use skys3_types::{ProposalId, RoleDocument};
use support::issuer::Issuer;
use support::process::{Process, fresh_addresses};
use support::{ACCESS_KEY, SECRET_KEY, body, config_text, tls_file};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinHandle;

/// The role every client's web identity assumes.
const ROLE_ARN: &str = "arn:aws:iam::123456789012:role/sdk-matrix";
/// The audience of the issuer's tokens, which the provider accepts.
const AUDIENCE: &str = "sts.amazonaws.com";
/// The subject of the issuer's tokens; the trust policy accepts
/// `system:serviceaccount:sdk:*`.
const SUBJECT: &str = "system:serviceaccount:sdk:matrix";
/// A second static credential, which s3-tests uses as its other user.
const ALT_ACCESS_KEY: &str = "AKIASKYS3SDKALTKEY";
/// Its secret.
const ALT_SECRET_KEY: &str = "skys3-sdk-alt-secret-0123456789abcdefgh";
/// The session lifetime, `session_default_seconds`: the STS minimum.
const SESSION_SECONDS: u64 = 900;
/// How soon after its issue a client should refresh a session.
const REFRESH_SECONDS: u64 = 5;
/// How long a token is valid. Its `iat` and `exp` are whole seconds, so
/// it lasts between 5 and 6 seconds from its issue.
const TOKEN_LIFETIME: Duration = Duration::from_secs(6);
/// How far past a token's `exp` the node accepts it,
/// `oidc_clock_skew_seconds`: the issuer and the node share a clock.
const CLOCK_SKEW_SECONDS: u64 = 1;
/// How often the token file gets a new token. A token read from the file
/// is at most this old, so it has at least 4 seconds left; one kept from
/// an earlier read fails within `TOKEN_LIFETIME` and the skew.
const TOKEN_ROTATION: Duration = Duration::from_secs(2);
/// How long a part of a multipart upload is, except the last: the S3
/// minimum.
const PART: usize = 5 << 20;
/// How long the load phase of the web-identity checks lasts, unless
/// `SKYS3_LOAD_SECONDS` says otherwise.
const LOAD_SECONDS: u64 = 20;
/// How long an external client may run, unless
/// `SKYS3_SDK_TIMEOUT_SECONDS` says otherwise.
const CLIENT_TIMEOUT_SECONDS: u64 = 900;

/// A node serving HTTPS and STS, and the token file. The issuer serves for
/// as long as the task that rotates the token runs.
struct Matrix {
    dir: tempfile::TempDir,
    node: Process,
    endpoint: String,
    token_file: PathBuf,
    rotation: JoinHandle<()>,
}

impl Matrix {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let ca = dir.path().join("ca.pem");
        std::fs::copy(tls_file("ca.pem"), &ca).unwrap();
        let (gateway, admin) = fresh_addresses();
        let config = dir.path().join("skys3.toml");
        std::fs::write(&config, matrix_config(dir.path(), &gateway, &admin)).unwrap();
        let log = dir.path().join("node.log");
        // The binary's issuer fetcher trusts the system roots, which
        // `SSL_CERT_FILE` replaces with the test CA.
        let env = [("SSL_CERT_FILE", ca.as_path())];

        // The first start creates the control store. The identity registers
        // go in while the node is down: the file store is read when opened.
        // Either start may move the node to other ports (`Process::start`).
        let node = Process::start_with_env(&config, &log, &env).await;
        assert!(node.terminate().await.success());
        let issuer = Arc::new(Issuer::start().await);
        write_identity(&dir.path().join("data/control"), &issuer);
        let node = Process::start_with_env(&config, &log, &env).await;

        let token_dir = dir.path().join("web-identity");
        std::fs::create_dir(&token_dir).unwrap();
        let token_file = token_dir.join("token");
        write_token(&token_file, &issuer);
        let rotation = tokio::spawn({
            let (issuer, token_file) = (Arc::clone(&issuer), token_file.clone());
            async move {
                loop {
                    tokio::time::sleep(TOKEN_ROTATION).await;
                    write_token(&token_file, &issuer);
                }
            }
        });
        Matrix {
            endpoint: format!("https://{}", node.gateway),
            dir,
            node,
            token_file,
            rotation,
        }
    }

    fn ca(&self) -> PathBuf {
        self.dir.path().join("ca.pem")
    }

    /// An S3 client with the static credential, trusting the test CA.
    fn s3(&self) -> aws_sdk_s3::Client {
        support::client(&self.endpoint)
    }

    /// The environment an external client gets (`tests/sdk/run.sh`).
    fn client_env(&self, client: &str, work: &Path) -> Vec<(String, String)> {
        let path = |p: &Path| p.display().to_string();
        let missing = path(&self.dir.path().join("no-such-file"));
        let ca = path(&self.ca());
        [
            ("SKYS3_ENDPOINT", self.endpoint.clone()),
            ("SKYS3_CA_FILE", ca.clone()),
            ("SKYS3_ACCESS_KEY_ID", ACCESS_KEY.to_owned()),
            ("SKYS3_SECRET_ACCESS_KEY", SECRET_KEY.to_owned()),
            ("SKYS3_ALT_ACCESS_KEY_ID", ALT_ACCESS_KEY.to_owned()),
            ("SKYS3_ALT_SECRET_ACCESS_KEY", ALT_SECRET_KEY.to_owned()),
            ("SKYS3_BUCKET", format!("sdk-{client}")),
            ("SKYS3_SESSION_SECONDS", SESSION_SECONDS.to_string()),
            ("SKYS3_REFRESH_SECONDS", REFRESH_SECONDS.to_string()),
            ("SKYS3_LOAD_SECONDS", load_seconds().to_string()),
            ("SKYS3_MATRIX_DIR", path(self.dir.path())),
            ("SKYS3_WORK_DIR", path(work)),
            ("AWS_REGION", "us-east-1".to_owned()),
            ("AWS_ENDPOINT_URL_S3", self.endpoint.clone()),
            ("AWS_ENDPOINT_URL_STS", self.endpoint.clone()),
            ("AWS_ROLE_ARN", ROLE_ARN.to_owned()),
            ("AWS_ROLE_SESSION_NAME", format!("sdk-{client}")),
            ("AWS_WEB_IDENTITY_TOKEN_FILE", path(&self.token_file)),
            ("AWS_CA_BUNDLE", ca.clone()),
            ("AWS_CONFIG_FILE", missing.clone()),
            ("AWS_SHARED_CREDENTIALS_FILE", missing),
            ("AWS_EC2_METADATA_DISABLED", "true".to_owned()),
            ("NODE_EXTRA_CA_CERTS", ca),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
    }

    async fn stop(self) {
        self.rotation.abort();
        let status = self.node.terminate().await;
        assert!(status.success(), "{status:?}");
    }
}

/// The node's configuration: HTTPS with the test certificate, STS with
/// 900-second sessions, and the static and alternate credentials.
fn matrix_config(dir: &Path, gateway: &str, admin: &str) -> String {
    let alt_secret = dir.join("alt-secret");
    std::fs::write(&alt_secret, ALT_SECRET_KEY).unwrap();
    let alt = format!(
        r#"
[identity.static_credentials.alt]
access_key_id = "{ALT_ACCESS_KEY}"
secret_access_key_file = "{}"
policy = '{{"Version": "2012-10-17", "Statement": {{"Effect": "Allow", "Action": "*", "Resource": "*"}}}}'
"#,
        alt_secret.display()
    );
    config_text(dir, gateway, admin, &alt)
        // A lifecycle pass every second (`rust_lifecycle`).
        .replace(
            "index_checkpoint_interval_seconds = 1\n",
            "index_checkpoint_interval_seconds = 1\nlifecycle_interval_seconds = 1\n",
        )
        .replace(
            &format!("listen = \"{gateway}\"\n"),
            &format!(
                "listen = \"{gateway}\"\ntls_cert_file = \"{}\"\ntls_key_file = \"{}\"\n",
                tls_file("server.pem").display(),
                tls_file("server.key").display()
            ),
        )
        .replace(
            "sts_web_identity = false",
            &format!(
                "sts_web_identity = true\nsession_default_seconds = {SESSION_SECONDS}\n\
                 oidc_clock_skew_seconds = {CLOCK_SKEW_SECONDS}"
            ),
        )
}

/// Writes the issuer's provider register and the `sdk-matrix` role into
/// the file control store at `control`. The role may do anything to
/// buckets named `sdk-*` and list buckets, and nothing else.
fn write_identity(control: &Path, issuer: &Issuer) {
    let key = issuer.key();
    let trust = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": {
            "Effect": "Allow",
            "Principal": {"Federated": issuer.url},
            "Action": "sts:AssumeRoleWithWebIdentity",
            "Condition": {
                "StringEquals": {format!("{key}:aud"): AUDIENCE},
                "StringLike": {format!("{key}:sub"): "system:serviceaccount:sdk:*"},
            }
        }
    });
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": "s3:*", "Resource": ["arn:aws:s3:::sdk-*", "arn:aws:s3:::sdk-*/*"]},
            {"Effect": "Allow", "Action": "s3:ListAllMyBuckets", "Resource": "*"},
        ]
    });
    let provider = ProviderDocument::new(
        OidcProvider::new(issuer.url.clone(), [AUDIENCE]),
        ProposalId::from_u128(1),
    );
    let role = RoleDocument {
        trust_policy: PolicyDocument::parse(&trust.to_string()).unwrap(),
        policies: vec![PolicyDocument::parse(&policy.to_string()).unwrap()],
        proposal_id: ProposalId::from_u128(2),
    };
    let provider_key = skys3_sts::identity::provider_key("sdk-matrix").unwrap();
    let role_key = skys3_sts::identity::role_key("sdk-matrix").unwrap();
    for (key, value) in [
        (provider_key.key(), serde_json::to_vec(&provider).unwrap()),
        (role_key.key(), serde_json::to_vec(&role).unwrap()),
    ] {
        let path = control.join(key.as_str());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, value).unwrap();
    }
}

/// Replaces the token file with a new token, atomically, since clients
/// read it at any moment.
fn write_token(token_file: &Path, issuer: &Issuer) {
    let next = token_file.with_extension("next");
    std::fs::write(&next, issuer.token(SUBJECT, AUDIENCE, TOKEN_LIFETIME)).unwrap();
    std::fs::rename(&next, token_file).unwrap();
}

fn env_number(name: &str, default: u64) -> u64 {
    std::env::var(name).map_or(default, |value| {
        value
            .trim()
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be a number, not {value:?}"))
    })
}

fn load_seconds() -> u64 {
    env_number("SKYS3_LOAD_SECONDS", LOAD_SECONDS)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_sdk_matrix_passes() {
    let matrix = Matrix::start().await;
    let clients: Vec<String> = std::env::var("SKYS3_SDK_CLIENTS")
        .unwrap_or_default()
        .split([' ', ',', '\n'])
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .collect();
    // Every external client runs in its own process, concurrently with the
    // others and with the Rust client.
    let external: Vec<_> = clients
        .iter()
        .map(|client| {
            let work = matrix.dir.path().join("clients").join(client);
            std::fs::create_dir_all(&work).unwrap();
            let env = matrix.client_env(client, &work);
            tokio::spawn(run_external(client.clone(), env))
        })
        .collect();
    rust_features(&matrix).await;
    rust_web_identity(&matrix).await;
    let mut results = Vec::new();
    for handle in external {
        results.push(handle.await.unwrap());
    }

    let mut failed = Vec::new();
    for result in &results {
        println!(
            "::group::{} ({}, {:.0?})",
            result.client,
            if result.passed { "passed" } else { "FAILED" },
            result.elapsed
        );
        println!("{}", result.output);
        println!("::endgroup::");
        if !result.passed {
            failed.push(result.client.clone());
        }
    }
    println!("rust: passed");
    for result in &results {
        println!(
            "{}: {} in {:.0?}",
            result.client,
            if result.passed { "passed" } else { "FAILED" },
            result.elapsed
        );
    }
    if !failed.is_empty() {
        let log = std::fs::read_to_string(matrix.dir.path().join("node.log")).unwrap_or_default();
        println!("node log, warnings and errors:");
        for line in log
            .lines()
            .filter(|line| line.contains(" WARN ") || line.contains(" ERROR "))
        {
            println!("{line}");
        }
        panic!("SDK matrix clients failed: {failed:?}");
    }
    matrix.stop().await;
}

/// How an external client ended.
struct ClientResult {
    client: String,
    passed: bool,
    elapsed: Duration,
    output: String,
}

/// Runs `tests/sdk/run.sh client <client>` with `env` and no inherited
/// `AWS_*` variable, under a timeout.
async fn run_external(client: String, env: Vec<(String, String)>) -> ClientResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut command = tokio::process::Command::new("sh");
    command
        .arg(root.join("tests/sdk/run.sh"))
        .arg("client")
        .arg(&client)
        .current_dir(&root)
        .kill_on_drop(true);
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("AWS_") {
            command.env_remove(name);
        }
    }
    command.envs(env);
    let timeout = Duration::from_secs(env_number(
        "SKYS3_SDK_TIMEOUT_SECONDS",
        CLIENT_TIMEOUT_SECONDS,
    ));
    let started = Instant::now();
    let (passed, output) = match tokio::time::timeout(timeout, command.output()).await {
        Ok(Ok(output)) => (
            output.status.success(),
            format!(
                "{}{}\nexit: {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
                output.status
            ),
        ),
        Ok(Err(error)) => (false, format!("could not run the client: {error}")),
        Err(_) => (false, format!("timed out after {timeout:?}")),
    };
    ClientResult {
        client,
        passed,
        elapsed: started.elapsed(),
        output,
    }
}

/// Records the payload hash (`x-amz-content-sha256`) of every request a
/// client sends, which names the `aws-chunked` form when it is one.
#[derive(Debug, Clone, Default)]
struct PayloadForms(Arc<Mutex<Vec<String>>>);

impl PayloadForms {
    fn streaming(&self) -> Vec<String> {
        let forms = self.0.lock().unwrap();
        forms
            .iter()
            .filter(|form| form.starts_with("STREAMING-"))
            .cloned()
            .collect()
    }
}

impl Intercept for PayloadForms {
    fn name(&self) -> &'static str {
        "PayloadForms"
    }

    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), aws_sdk_s3::error::BoxError> {
        if let Some(form) = context.request().headers().get("x-amz-content-sha256") {
            self.0.lock().unwrap().push(form.to_owned());
        }
        Ok(())
    }
}

/// The checksum algorithms SkyS3 supports (design section 11). The SDK
/// offers more, which SkyS3 answers with `501 NotImplemented`.
const ALGORITHMS: [ChecksumAlgorithm; 5] = [
    ChecksumAlgorithm::Crc32,
    ChecksumAlgorithm::Crc32C,
    ChecksumAlgorithm::Crc64Nvme,
    ChecksumAlgorithm::Sha1,
    ChecksumAlgorithm::Sha256,
];

/// The checksums an object answer carries, by algorithm.
fn checksums(
    crc32: Option<&str>,
    crc32c: Option<&str>,
    crc64nvme: Option<&str>,
    sha1: Option<&str>,
    sha256: Option<&str>,
) -> Vec<(ChecksumAlgorithm, String)> {
    [
        (ChecksumAlgorithm::Crc32, crc32),
        (ChecksumAlgorithm::Crc32C, crc32c),
        (ChecksumAlgorithm::Crc64Nvme, crc64nvme),
        (ChecksumAlgorithm::Sha1, sha1),
        (ChecksumAlgorithm::Sha256, sha256),
    ]
    .into_iter()
    .filter_map(|(algorithm, value)| Some((algorithm, value?.to_owned())))
    .collect()
}

/// The whole body of a GetObject answer.
async fn bytes(object: aws_sdk_s3::operation::get_object::GetObjectOutput) -> Vec<u8> {
    object.body.collect().await.unwrap().into_bytes().to_vec()
}

/// The AWS SDK for Rust with a static credential: checksums, `aws-chunked`
/// uploads, multipart, presigned URLs, and the everyday operations.
async fn rust_features(matrix: &Matrix) {
    let forms = PayloadForms::default();
    let s3 = matrix.s3();
    let s3 =
        aws_sdk_s3::Client::from_conf(s3.config().to_builder().interceptor(forms.clone()).build());
    let bucket = "sdk-rust";
    s3.create_bucket().bucket(bucket).send().await.unwrap();

    // The SDK's default checksum, CRC32, comes back on PUT and GET.
    let data = body(1, 70_000);
    let put = s3
        .put_object()
        .bucket(bucket)
        .key("default")
        .body(data.clone().into())
        .send()
        .await
        .unwrap();
    let crc32 = put.checksum_crc32().expect("the default checksum");
    let object = s3
        .get_object()
        .bucket(bucket)
        .key("default")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap();
    assert_eq!(object.checksum_crc32(), Some(crc32));
    assert_eq!(bytes(object).await, data);

    // Every algorithm of design section 11 is stored and returned.
    for (i, algorithm) in ALGORITHMS.into_iter().enumerate() {
        let key = format!("checksum/{algorithm}");
        let data = body(10 + i as u8, 3_000 + i * 1_000);
        let put = s3
            .put_object()
            .bucket(bucket)
            .key(&key)
            .checksum_algorithm(algorithm.clone())
            .body(data.clone().into())
            .send()
            .await
            .unwrap_or_else(|error| panic!("PUT with {algorithm}: {error:?}"));
        let sent = checksums(
            put.checksum_crc32(),
            put.checksum_crc32_c(),
            put.checksum_crc64_nvme(),
            put.checksum_sha1(),
            put.checksum_sha256(),
        );
        assert_eq!(sent.len(), 1, "{algorithm}: {sent:?}");
        assert_eq!(sent[0].0, algorithm);
        let object = s3
            .get_object()
            .bucket(bucket)
            .key(&key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        let stored = checksums(
            object.checksum_crc32(),
            object.checksum_crc32_c(),
            object.checksum_crc64_nvme(),
            object.checksum_sha1(),
            object.checksum_sha256(),
        );
        assert_eq!(stored, sent, "{algorithm}");
        assert_eq!(object.checksum_type(), Some(&ChecksumType::FullObject));
        assert_eq!(bytes(object).await, data, "{algorithm}");
    }

    // A body streamed from a file goes as `aws-chunked` with a trailing
    // checksum.
    let file = matrix.dir.path().join("rust-upload");
    let data = body(30, 300_000);
    std::fs::write(&file, &data).unwrap();
    s3.put_object()
        .bucket(bucket)
        .key("streamed")
        .body(ByteStream::from_path(&file).await.unwrap())
        .send()
        .await
        .unwrap();
    let object = s3
        .get_object()
        .bucket(bucket)
        .key("streamed")
        .send()
        .await
        .unwrap();
    assert_eq!(bytes(object).await, data);
    let streamed = forms.streaming();
    assert!(
        streamed
            .iter()
            .any(|form| form == "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
        "no aws-chunked upload: {streamed:?}"
    );

    rust_multipart(&s3, bucket).await;
    rust_part_copy(&s3, bucket).await;
    rust_presigned(&s3, bucket, &matrix.ca()).await;
    rust_lifecycle(&s3).await;

    // Listing, copies, tags, ranges, and batch deletes.
    let listed = s3
        .list_objects_v2()
        .bucket(bucket)
        .prefix("checksum/")
        .delimiter("/")
        .send()
        .await
        .unwrap();
    assert_eq!(listed.contents().len(), ALGORITHMS.len());
    s3.copy_object()
        .bucket(bucket)
        .key("copy")
        .copy_source(format!("{bucket}/default"))
        .send()
        .await
        .unwrap();
    let tags = Tagging::builder()
        .tag_set(Tag::builder().key("sdk").value("rust").build().unwrap())
        .build()
        .unwrap();
    s3.put_object_tagging()
        .bucket(bucket)
        .key("copy")
        .tagging(tags)
        .send()
        .await
        .unwrap();
    let tagged = s3
        .get_object_tagging()
        .bucket(bucket)
        .key("copy")
        .send()
        .await
        .unwrap();
    assert_eq!(tagged.tag_set()[0].value(), "rust");
    let range = s3
        .get_object()
        .bucket(bucket)
        .key("copy")
        .range("bytes=10-19")
        .send()
        .await
        .unwrap();
    assert_eq!(bytes(range).await, body(1, 70_000)[10..20]);
    let all = s3.list_objects_v2().bucket(bucket).send().await.unwrap();
    let keys: Vec<_> = all
        .contents()
        .iter()
        .map(|object| {
            ObjectIdentifier::builder()
                .key(object.key().unwrap())
                .build()
                .unwrap()
        })
        .collect();
    let deleted = s3
        .delete_objects()
        .bucket(bucket)
        .delete(Delete::builder().set_objects(Some(keys)).build().unwrap())
        .send()
        .await
        .unwrap();
    assert!(deleted.errors().is_empty(), "{:?}", deleted.errors());
    s3.delete_bucket().bucket(bucket).send().await.unwrap();
}

/// Lifecycle rules on a `local` bucket (design section 8.7): a rule with a
/// prefix and a tag filter and one with a prefix only expire, at a date
/// already past, exactly the objects they match, on the node's next pass;
/// a rule whose days have not passed and an upload-cleanup rule leave
/// their objects and uploads alone. The configuration round-trips through
/// the SDK, and is removed.
async fn rust_lifecycle(s3: &aws_sdk_s3::Client) {
    use aws_sdk_s3::primitives::DateTime;
    use aws_sdk_s3::types::{
        AbortIncompleteMultipartUpload, BucketLifecycleConfiguration, ExpirationStatus,
        LifecycleExpiration, LifecycleRule, LifecycleRuleAndOperator, LifecycleRuleFilter,
    };

    let bucket = "sdk-lifecycle";
    s3.create_bucket().bucket(bucket).send().await.unwrap();
    let temp = || Tag::builder().key("class").value("temp").build().unwrap();
    for key in ["logs/tagged", "logs/plain", "keep/tagged", "tmp/scratch"] {
        let mut put = s3
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body(7, 100).into());
        if key.ends_with("tagged") {
            put = put.tagging("class=temp");
        }
        put.send().await.unwrap();
    }
    let upload = s3
        .create_multipart_upload()
        .bucket(bucket)
        .key("logs/upload")
        .send()
        .await
        .unwrap();

    let past = DateTime::from_secs(1_577_836_800); // 2020-01-01
    let rule = |id: &str, filter: LifecycleRuleFilter| {
        LifecycleRule::builder()
            .id(id)
            .status(ExpirationStatus::Enabled)
            .filter(filter)
    };
    let prefix = |prefix: &str| LifecycleRuleFilter::builder().prefix(prefix).build();
    let tagged_logs = LifecycleRuleFilter::builder()
        .and(
            LifecycleRuleAndOperator::builder()
                .prefix("logs/")
                .tags(temp())
                .build(),
        )
        .build();
    let rules = vec![
        rule("tagged-logs", tagged_logs)
            .expiration(LifecycleExpiration::builder().date(past).build())
            .build()
            .unwrap(),
        rule("tmp", prefix("tmp/"))
            .expiration(LifecycleExpiration::builder().date(past).build())
            .build()
            .unwrap(),
        rule("keep", prefix("keep/"))
            .expiration(LifecycleExpiration::builder().days(30).build())
            .build()
            .unwrap(),
        rule("uploads", prefix(""))
            .abort_incomplete_multipart_upload(
                AbortIncompleteMultipartUpload::builder()
                    .days_after_initiation(1)
                    .build(),
            )
            .build()
            .unwrap(),
    ];
    let configuration = BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .unwrap();
    s3.put_bucket_lifecycle_configuration()
        .bucket(bucket)
        .lifecycle_configuration(configuration)
        .send()
        .await
        .unwrap();
    let stored = s3
        .get_bucket_lifecycle_configuration()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    let ids: Vec<_> = stored.rules().iter().filter_map(|rule| rule.id()).collect();
    assert_eq!(ids, ["tagged-logs", "tmp", "keep", "uploads"]);
    let and = stored.rules()[0]
        .filter()
        .and_then(|filter| filter.and())
        .unwrap();
    assert_eq!((and.prefix(), and.tags().len()), (Some("logs/"), 1));
    let expiration = stored.rules()[0].expiration().unwrap();
    assert_eq!(expiration.date(), Some(&past));

    // The node runs a pass every second; wait for the expirations.
    let started = Instant::now();
    let keys = || async {
        let listed = s3.list_objects_v2().bucket(bucket).send().await.unwrap();
        let keys: Vec<String> = listed
            .contents()
            .iter()
            .filter_map(|object| object.key().map(str::to_owned))
            .collect();
        keys
    };
    loop {
        let left = keys().await;
        if left == ["keep/tagged", "logs/plain"] {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "the expired objects are still there: {left:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let missing = s3
        .head_object()
        .bucket(bucket)
        .key("tmp/scratch")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        missing.raw_response().map(|r| r.status().as_u16()),
        Some(404)
    );
    let uploads = s3
        .list_multipart_uploads()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    assert_eq!(uploads.uploads().len(), 1, "the upload has a day to go");

    s3.delete_bucket_lifecycle()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    let gone = s3
        .get_bucket_lifecycle_configuration()
        .bucket(bucket)
        .send()
        .await
        .unwrap_err();
    assert_eq!(gone.code(), Some("NoSuchLifecycleConfiguration"));
    s3.abort_multipart_upload()
        .bucket(bucket)
        .key("logs/upload")
        .upload_id(upload.upload_id().unwrap())
        .send()
        .await
        .unwrap();
}

/// A three-part upload with CRC32 part checksums: a composite checksum,
/// a multipart ETag, and parts served by number.
async fn rust_multipart(s3: &aws_sdk_s3::Client, bucket: &str) {
    let data = body(40, 2 * PART + 1_000_000);
    let upload = s3
        .create_multipart_upload()
        .bucket(bucket)
        .key("multipart")
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .send()
        .await
        .unwrap();
    let id = upload.upload_id().unwrap();
    let mut parts = Vec::new();
    for (i, chunk) in data.chunks(PART).enumerate() {
        let number = i32::try_from(i).unwrap() + 1;
        let part = s3
            .upload_part()
            .bucket(bucket)
            .key("multipart")
            .upload_id(id)
            .part_number(number)
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .body(chunk.to_vec().into())
            .send()
            .await
            .unwrap();
        parts.push(
            CompletedPart::builder()
                .part_number(number)
                .e_tag(part.e_tag().unwrap())
                .checksum_crc32(part.checksum_crc32().unwrap())
                .build(),
        );
    }
    let done = s3
        .complete_multipart_upload()
        .bucket(bucket)
        .key("multipart")
        .upload_id(id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(parts))
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(done.e_tag().unwrap().ends_with("-3\""), "{done:?}");
    let head = s3
        .head_object()
        .bucket(bucket)
        .key("multipart")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap();
    assert_eq!(head.checksum_type(), Some(&ChecksumType::Composite));
    assert!(head.checksum_crc32().unwrap().ends_with("-3"), "{head:?}");
    let object = s3
        .get_object()
        .bucket(bucket)
        .key("multipart")
        .send()
        .await
        .unwrap();
    assert_eq!(bytes(object).await, data);
    let second = s3
        .get_object()
        .bucket(bucket)
        .key("multipart")
        .part_number(2)
        .send()
        .await
        .unwrap();
    assert_eq!(second.parts_count(), Some(3));
    assert_eq!(bytes(second).await, data[PART..2 * PART]);
}

/// UploadPartCopy of `multipart`, range by range at its own part
/// boundaries, under `x-amz-copy-source-if-match`: each part's ETag is the
/// MD5 of its bytes, so the copy's multipart ETag is the source's, as in
/// S3. A failed source condition answers `412`.
async fn rust_part_copy(s3: &aws_sdk_s3::Client, bucket: &str) {
    use md5::Digest as _;

    let data = body(40, 2 * PART + 1_000_000);
    let source = s3
        .head_object()
        .bucket(bucket)
        .key("multipart")
        .send()
        .await
        .unwrap();
    let source_etag = source.e_tag().unwrap();
    let upload = s3
        .create_multipart_upload()
        .bucket(bucket)
        .key("part-copy")
        .send()
        .await
        .unwrap();
    let id = upload.upload_id().unwrap();
    let refused = s3
        .upload_part_copy()
        .bucket(bucket)
        .key("part-copy")
        .upload_id(id)
        .part_number(1)
        .copy_source(format!("{bucket}/multipart"))
        .copy_source_if_none_match(source_etag)
        .send()
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Some("PreconditionFailed"), "{refused:?}");
    let mut parts = Vec::new();
    for (i, chunk) in data.chunks(PART).enumerate() {
        let number = i32::try_from(i).unwrap() + 1;
        let first = i * PART;
        let copied = s3
            .upload_part_copy()
            .bucket(bucket)
            .key("part-copy")
            .upload_id(id)
            .part_number(number)
            .copy_source(format!("{bucket}/multipart"))
            .copy_source_range(format!("bytes={first}-{}", first + chunk.len() - 1))
            .copy_source_if_match(source_etag)
            .send()
            .await
            .unwrap();
        let etag = copied.copy_part_result().unwrap().e_tag().unwrap();
        let md5: String = md5::Md5::digest(chunk)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(etag, format!("\"{md5}\""));
        parts.push(
            CompletedPart::builder()
                .part_number(number)
                .e_tag(etag)
                .build(),
        );
    }
    let done = s3
        .complete_multipart_upload()
        .bucket(bucket)
        .key("part-copy")
        .upload_id(id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(parts))
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(done.e_tag(), Some(source_etag));
    let object = s3
        .get_object()
        .bucket(bucket)
        .key("part-copy")
        .send()
        .await
        .unwrap();
    assert_eq!(bytes(object).await, data);
}

/// Presigned GET and PUT URLs, used by a plain HTTPS client.
async fn rust_presigned(s3: &aws_sdk_s3::Client, bucket: &str, ca: &Path) {
    let presigning = PresigningConfig::expires_in(Duration::from_secs(300)).unwrap();
    let get = s3
        .get_object()
        .bucket(bucket)
        .key("default")
        .presigned(presigning.clone())
        .await
        .unwrap();
    let (status, got) = https_request(ca, "GET", get.uri(), &[], &[]).await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&got));
    assert_eq!(got, body(1, 70_000));

    let data = body(50, 20_000);
    let put = s3
        .put_object()
        .bucket(bucket)
        .key("presigned")
        .presigned(presigning)
        .await
        .unwrap();
    let headers: Vec<_> = put.headers().collect();
    let (status, answer) = https_request(ca, "PUT", put.uri(), &headers, &data).await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&answer));
    let object = s3
        .get_object()
        .bucket(bucket)
        .key("presigned")
        .send()
        .await
        .unwrap();
    assert_eq!(bytes(object).await, data);
}

/// Sends one HTTP/1.1 request over TLS to the `https://` `url`, trusting
/// `ca`, and returns the status and body.
async fn https_request(
    ca: &Path,
    method: &str,
    url: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> (u16, Vec<u8>) {
    let rest = url.strip_prefix("https://").expect("an https URL");
    let (authority, target) = rest.split_at(rest.find('/').unwrap_or(rest.len()));
    let host = authority
        .rsplit_once(':')
        .map_or(authority, |(host, _)| host);
    let mut roots = rustls::RootCertStore::empty();
    for certificate in CertificateDer::pem_file_iter(ca).unwrap() {
        roots.add(certificate.unwrap()).unwrap();
    }
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let exchange = async {
        let tcp = tokio::net::TcpStream::connect(authority).await.unwrap();
        let name = ServerName::try_from(host.to_owned()).unwrap();
        let mut stream = connector.connect(name, tcp).await.unwrap();
        let mut head = format!(
            "{method} {target} HTTP/1.1\r\nhost: {authority}\r\ncontent-length: {}\r\n\
             connection: close\r\n",
            body.len()
        );
        for (name, value) in headers {
            if !name.eq_ignore_ascii_case("host") {
                head.push_str(&format!("{name}: {value}\r\n"));
            }
        }
        head.push_str("\r\n");
        // Write and read at once, so a large body cannot stall on a full
        // socket buffer while the server answers early.
        let (mut reader, mut writer) = tokio::io::split(&mut stream);
        let write = async {
            writer.write_all(head.as_bytes()).await.unwrap();
            writer.write_all(body).await.unwrap();
            writer.flush().await.unwrap();
        };
        let mut response = Vec::new();
        let read = reader.read_to_end(&mut response);
        let ((), read) = tokio::join!(write, read);
        read.unwrap();
        response
    };
    let response = tokio::time::timeout(Duration::from_secs(30), exchange)
        .await
        .expect("the request answers in time");
    let split = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("a response head");
    let head = String::from_utf8_lossy(&response[..split]).to_ascii_lowercase();
    let status = head[9..12].parse().unwrap();
    let mut body = response[split + 4..].to_vec();
    if head.contains("transfer-encoding: chunked") {
        body = dechunk(&body);
    }
    (status, body)
}

/// Decodes a chunked HTTP/1.1 body.
fn dechunk(mut data: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    loop {
        let line_end = data.windows(2).position(|w| w == b"\r\n").unwrap();
        let size_text = String::from_utf8_lossy(&data[..line_end]);
        let size = usize::from_str_radix(size_text.split(';').next().unwrap().trim(), 16).unwrap();
        data = &data[line_end + 2..];
        if size == 0 {
            return body;
        }
        body.extend_from_slice(&data[..size]);
        data = &data[size + 2..];
    }
}

/// A credentials provider that records the access key IDs it hands out.
#[derive(Debug)]
struct Recording {
    inner: SharedCredentialsProvider,
    keys: Arc<Mutex<BTreeSet<String>>>,
}

impl ProvideCredentials for Recording {
    fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        future::ProvideCredentials::new(async move {
            let credentials = self.inner.provide_credentials().await?;
            self.keys
                .lock()
                .unwrap()
                .insert(credentials.access_key_id().to_owned());
            Ok(credentials)
        })
    }
}

/// The AWS SDK for Rust's default credential chain, configured only by
/// the environment a workload has: it assumes the role with the token
/// file's web identity at `AWS_ENDPOINT_URL_STS`, and sends S3 requests to
/// `AWS_ENDPOINT_URL_S3`. Then eight tasks send requests for the load
/// phase while the SDK refreshes the session.
///
/// The SDK's identity cache refreshes a session `buffer_time` before it
/// expires, less a random jitter of up to half of it, so no buffer makes
/// it refresh at a fixed short age. A buffer of twice the session lifetime
/// makes every request due for a refresh, as botocore's 15-minute window
/// does with 900-second sessions; the cache still loads one session at a
/// time for all the requests that wait on it.
async fn rust_web_identity(matrix: &Matrix) {
    let ca = std::fs::read(matrix.ca()).unwrap();
    let context = TlsContext::builder()
        .with_trust_store(TrustStore::empty().with_pem_certificate(ca))
        .build()
        .unwrap();
    let http = aws_smithy_http_client::Builder::new()
        .tls_provider(tls::Provider::Rustls(CryptoMode::AwsLc))
        .tls_context(context)
        .build_https();
    let work = matrix.dir.path().join("clients/rust");
    std::fs::create_dir_all(&work).unwrap();
    let vars = matrix.client_env("rust", &work);
    let pairs: Vec<(&str, &str)> = vars
        .iter()
        .filter(|(name, _)| name.starts_with("AWS_"))
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();
    let sdk = aws_config::defaults(BehaviorVersion::latest())
        .env(Env::from_slice(&pairs))
        .http_client(http)
        .identity_cache(
            IdentityCache::lazy()
                .buffer_time(Duration::from_secs(2 * SESSION_SECONDS))
                .build(),
        )
        .load()
        .await;
    let keys = Arc::new(Mutex::new(BTreeSet::new()));
    let recording = Recording {
        inner: sdk.credentials_provider().unwrap(),
        keys: Arc::clone(&keys),
    };
    let s3 = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&sdk)
            .force_path_style(true)
            .credentials_provider(recording)
            .build(),
    );

    // The session may use `sdk-*` buckets and nothing else.
    let bucket = "sdk-rust-web-identity";
    s3.create_bucket().bucket(bucket).send().await.unwrap();
    let denied = s3
        .create_bucket()
        .bucket("other-rust")
        .send()
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Some("AccessDenied"), "{denied:?}");
    let first = keys.lock().unwrap().clone();
    assert!(
        first.iter().all(|key| key.starts_with("ASIA")),
        "session keys: {first:?}"
    );

    // A token every client has seen, which is superseded during the load.
    let superseded = std::fs::read_to_string(&matrix.token_file).unwrap();
    let read_at = Instant::now();

    let deadline = Instant::now() + Duration::from_secs(load_seconds());
    let operations = Arc::new(AtomicUsize::new(0));
    let workers: Vec<_> = (0..8)
        .map(|worker| {
            let (s3, operations) = (s3.clone(), Arc::clone(&operations));
            tokio::spawn(async move {
                let mut round = 0_usize;
                while Instant::now() < deadline {
                    let key = format!("load/{worker}/{}", round % 4);
                    let data = body(worker, 1_000 + round % 7 * 100);
                    s3.put_object()
                        .bucket(bucket)
                        .key(&key)
                        .body(data.clone().into())
                        .send()
                        .await
                        .unwrap_or_else(|error| panic!("PUT {key}: {error:?}"));
                    let object = s3
                        .get_object()
                        .bucket(bucket)
                        .key(&key)
                        .send()
                        .await
                        .unwrap_or_else(|error| panic!("GET {key}: {error:?}"));
                    assert_eq!(bytes(object).await, data);
                    operations.fetch_add(2, Ordering::Relaxed);
                    round += 1;
                }
            })
        })
        .collect();
    for worker in workers {
        worker.await.unwrap();
    }
    let sessions = keys.lock().unwrap().len();
    println!(
        "rust: {} requests under load with {sessions} sessions",
        operations.load(Ordering::Relaxed)
    );
    assert!(sessions >= 2, "the SDK never refreshed its session");

    // The load outlasted the superseded token: every client that kept
    // working through it reread the token file to refresh.
    let expired = read_at + TOKEN_LIFETIME + Duration::from_secs(CLOCK_SKEW_SECONDS + 1);
    tokio::time::sleep_until(expired.into()).await;
    let (status, answer) = assume_role(matrix, &superseded).await;
    assert_eq!(status, 400, "{answer}");
    assert!(answer.contains("ExpiredTokenException"), "{answer}");
    let current = std::fs::read_to_string(&matrix.token_file).unwrap();
    let (status, answer) = assume_role(matrix, &current).await;
    assert_eq!(status, 200, "{answer}");
}

/// Calls `AssumeRoleWithWebIdentity` with `token`, without the SDK.
async fn assume_role(matrix: &Matrix, token: &str) -> (u16, String) {
    let form = format!(
        "Action=AssumeRoleWithWebIdentity&Version=2011-06-15&RoleArn={ROLE_ARN}\
         &RoleSessionName=rust-check&WebIdentityToken={}",
        token.trim()
    );
    let (status, body) = https_request(
        &matrix.ca(),
        "POST",
        &format!("{}/", matrix.endpoint),
        &[("content-type", "application/x-www-form-urlencoded")],
        form.as_bytes(),
    )
    .await;
    (status, String::from_utf8_lossy(&body).into_owned())
}
