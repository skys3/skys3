//! A node's identity stack for STS tests: a gateway with the STS endpoint,
//! SigV4 over static credentials and sessions, an in-memory control store
//! (wrapped for fault injection) holding the identity registers, and an
//! OIDC issuer whose documents are served from memory.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use http::{Method, Request, StatusCode};
use s3s::Body;
use serde_json::{Value, json};
use skys3_config::Config;
use skys3_control::faults::{FaultRates, FaultyStore};
use skys3_control::{
    ControlStore, Expected, MemoryControlStore, ProposalIds, ProposalOutcome, PutOutcome,
    RegisterKey, RetryPolicy, TypedKey, bootstrap, propose_document,
};
use skys3_gateway::stub::MemoryShards;
use skys3_gateway::{Gateway, GatewayConfig, IdSource, SigV4Authenticator, StaticCredentials};
use skys3_io::{ManualWallClock, WallClock};
use skys3_sts::identity::{provider_key, role_key};
use skys3_sts::testkit::{TestKey, jwks};
use skys3_sts::{
    Algorithm, DocumentFetcher, FetchError, IdentityCopy, MemoryFetcher, MemorySessionStore,
    NodeCredentials, OidcProvider, OidcValidator, ProviderDocument, StsEndpoint, StsSettings,
    ValidatorSettings,
};
use skys3_types::policy::PolicyDocument;
use skys3_types::{RegisterDocument, RoleDocument};

pub const ISSUER: &str = "https://idp.example";
pub const ISSUER_KEY: &str = "idp.example";
pub const AUDIENCE: &str = "sts.amazonaws.com";
pub const JWKS: &str = "https://idp.example/keys";
pub const ROLE_ARN: &str = "arn:aws:iam::123456789012:role/deployer";

pub type Store = FaultyStore<MemoryControlStore>;
pub type Lookup = NodeCredentials<MemorySessionStore>;
pub type Sts = StsEndpoint<GatedFetcher, MemorySessionStore>;

/// The issuer's documents, served from memory through a gate that a test
/// can close to hold fetches open, as a slow issuer would.
pub struct GatedFetcher {
    documents: MemoryFetcher,
    gate: Arc<tokio::sync::RwLock<()>>,
    waiting: Arc<tokio::sync::Notify>,
}

impl DocumentFetcher for GatedFetcher {
    async fn fetch(&self, url: &str, max_bytes: usize) -> Result<Bytes, FetchError> {
        self.waiting.notify_one();
        let _open = self.gate.read().await;
        self.documents.fetch(url, max_bytes).await
    }
}

/// The `[identity]` settings and buckets every node in these tests uses.
const CONFIG: &str = r#"
[cluster]
cluster_id = "test"
[control_store]
etcd_endpoints = ["https://etcd.invalid:2379"]
[buckets.defaults]
mode = "local"
[identity]
session_default_seconds = 1800
session_maximum_seconds = 7200
identity_max_staleness_hours = 1
"#;

/// The trust policy of the `deployer` role: tokens for `AUDIENCE` whose
/// subject is in the `ci` namespace.
pub fn trust_policy() -> String {
    json!({
        "Version": "2012-10-17",
        "Statement": {
            "Effect": "Allow",
            "Principal": {"Federated": format!("arn:aws:iam::123456789012:oidc-provider/{ISSUER_KEY}")},
            "Action": "sts:AssumeRoleWithWebIdentity",
            "Condition": {
                "StringEquals": {format!("{ISSUER_KEY}:aud"): AUDIENCE},
                "StringLike": {format!("{ISSUER_KEY}:sub"): "system:serviceaccount:ci:*"},
            }
        }
    })
    .to_string()
}

/// The `deployer` role's policy: everything on buckets named `deploy-*`.
pub fn role_policy() -> String {
    json!({
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": "s3:*", "Resource": ["arn:aws:s3:::deploy-*", "arn:aws:s3:::deploy-*/*"]},
            {"Effect": "Allow", "Action": "s3:ListAllMyBuckets", "Resource": "*"},
        ]
    })
    .to_string()
}

/// One node's identity stack.
pub struct Node {
    pub gateway: Gateway<SigV4Authenticator<Lookup>>,
    pub sts: Arc<Sts>,
    pub store: Store,
    pub sessions: MemorySessionStore,
    pub clock: ManualWallClock,
    pub key: TestKey,
    pub gate: Arc<tokio::sync::RwLock<()>>,
    pub fetching: Arc<tokio::sync::Notify>,
    pub retry: RetryPolicy,
    ids: std::sync::Mutex<ProposalIds>,
}

/// Seconds since the Unix epoch, now.
pub fn now() -> Duration {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
}

impl Node {
    /// A node whose clock reads `start`, with the `deployer` role and the
    /// issuer registered and synced.
    pub async fn start(start: Duration) -> Self {
        let config: Config = CONFIG.parse().unwrap();
        let identity_config = config.identity();
        let clock = ManualWallClock::new(start);
        let wall: Arc<dyn WallClock> = Arc::new(clock.clone());
        let memory = MemoryControlStore::new();
        let retry = RetryPolicy {
            max_attempts: 2,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
        };
        let mut ids = ProposalIds::seeded(5);
        let gateway_config = GatewayConfig::new(&config);
        bootstrap(&memory, &gateway_config.cluster_id, ids.next_id(), &retry)
            .await
            .unwrap();
        let store = FaultyStore::new(memory);

        let key = TestKey::rsa("k1");
        let fetcher = MemoryFetcher::new();
        fetcher.insert(
            format!("{ISSUER}/.well-known/openid-configuration"),
            json!({"issuer": ISSUER, "jwks_uri": JWKS}).to_string(),
        );
        fetcher.insert(JWKS, jwks(&[&key]));
        let (gate, fetching) = (Arc::default(), Arc::default());
        let fetcher = GatedFetcher {
            documents: fetcher,
            gate: Arc::clone(&gate),
            waiting: Arc::clone(&fetching),
        };
        let validator =
            OidcValidator::new(fetcher, Arc::clone(&wall), ValidatorSettings::default());
        let identity = Arc::new(IdentityCopy::new(
            Arc::clone(&wall),
            identity_config.identity_max_staleness(),
        ));
        let sessions = MemorySessionStore::new();
        let settings = StsSettings::from_config(identity_config).unwrap();
        let sts = Arc::new(StsEndpoint::new(
            validator,
            Arc::clone(&identity),
            sessions.clone(),
            Arc::clone(&wall),
            settings,
        ));
        let lookup = NodeCredentials::new(
            StaticCredentials::new(),
            sessions.clone(),
            identity,
            Arc::clone(&wall),
        );
        let gateway = Gateway::new(
            gateway_config,
            store.clone(),
            MemoryShards::new().await,
            IdSource::seeded(3),
            SigV4Authenticator::new(lookup, wall),
        )
        .await
        .unwrap()
        .with_sts(sts.clone());

        let node = Node {
            gateway,
            sts,
            store,
            sessions,
            clock,
            key,
            gate,
            fetching,
            retry,
            ids: std::sync::Mutex::new(ids),
        };
        node.put_provider("idp", OidcProvider::new(ISSUER, [AUDIENCE]))
            .await;
        node.put_role("deployer", &trust_policy(), &[&role_policy()])
            .await;
        node.sync().await.unwrap();
        node
    }

    fn next_id(&self) -> skys3_types::ProposalId {
        self.ids.lock().unwrap().next_id()
    }

    /// Writes a provider register.
    pub async fn put_provider(&self, name: &str, provider: OidcProvider) {
        let document = ProviderDocument::new(provider, self.next_id());
        self.put(&provider_key(name).unwrap(), &document).await;
    }

    /// Writes a role register.
    pub async fn put_role(&self, name: &str, trust_policy: &str, policies: &[&str]) {
        let document = RoleDocument {
            trust_policy: PolicyDocument::parse(trust_policy).unwrap(),
            policies: policies
                .iter()
                .map(|policy| PolicyDocument::parse(policy).unwrap())
                .collect(),
            proposal_id: self.next_id(),
        };
        self.put(&role_key(name).unwrap(), &document).await;
    }

    /// Writes a register, whatever it holds now.
    async fn put<D: RegisterDocument>(&self, key: &TypedKey<D>, document: &D) {
        let store = self.store.inner();
        let expected = match store.get(key.key()).await.unwrap() {
            Some(current) => Expected::Version(current.version),
            None => Expected::Absent,
        };
        let outcome = propose_document(store, key, expected, document, &self.retry)
            .await
            .unwrap();
        assert!(matches!(outcome, ProposalOutcome::Accepted(_)));
    }

    /// Writes a raw value to a register, whatever it holds now.
    pub async fn put_raw(&self, key: &str, value: &str) {
        let store = self.store.inner();
        let key = RegisterKey::new(key).unwrap();
        let expected = match store.get(&key).await.unwrap() {
            Some(current) => Expected::Version(current.version),
            None => Expected::Absent,
        };
        let outcome = store
            .put_if(&key, expected, Bytes::copy_from_slice(value.as_bytes()))
            .await
            .unwrap();
        assert!(matches!(outcome, PutOutcome::Written(_)));
    }

    /// Syncs the node's identity copy.
    pub async fn sync(&self) -> Result<(), skys3_control::ControlError> {
        self.sts.sync_identity(&self.store, &self.retry).await
    }

    /// Makes every control-store request fail, or succeed again.
    pub fn set_outage(&self, outage: bool) {
        let unavailable = if outage { 1.0 } else { 0.0 };
        self.store.set_rates(FaultRates {
            unavailable,
            ..FaultRates::default()
        });
    }

    /// A token from the issuer, for `subject`, valid for an hour from the
    /// node's clock, with `extra` claims added or replaced.
    pub fn token(&self, subject: &str, extra: Value) -> String {
        let now = self.clock.now().as_secs();
        let mut claims = json!({
            "iss": ISSUER,
            "sub": subject,
            "aud": AUDIENCE,
            "iat": now,
            "exp": now + 3600,
        });
        if let (Some(claims), Value::Object(extra)) = (claims.as_object_mut(), extra) {
            claims.extend(extra);
        }
        self.key.sign(Algorithm::Rs256, &claims)
    }

    /// Sends an `AssumeRoleWithWebIdentity` form with `params` added to,
    /// or replacing, the defaults; a parameter with an empty value is left
    /// out.
    pub async fn assume(&self, token: &str, params: &[(&str, &str)]) -> Answer {
        let mut pairs = vec![
            ("Action", "AssumeRoleWithWebIdentity"),
            ("Version", "2011-06-15"),
            ("RoleArn", ROLE_ARN),
            ("RoleSessionName", "ci-run"),
            ("WebIdentityToken", token),
        ];
        pairs.retain(|(name, _)| !params.iter().any(|(other, _)| other == name));
        pairs.extend(params.iter().filter(|(_, value)| !value.is_empty()));
        let body = serde_urlencoded::to_string(pairs).unwrap();
        self.post("application/x-www-form-urlencoded", body).await
    }

    /// Sends a `POST /` with `body`.
    pub async fn post(&self, content_type: &str, body: impl Into<Bytes>) -> Answer {
        let request = Request::builder()
            .method(Method::POST)
            .uri("/")
            .header("content-type", content_type)
            .body(Body::from(body.into()))
            .unwrap();
        let response = self.gateway.handle(request).await;
        let (parts, mut body) = response.into_parts();
        let bytes = body.store_all_limited(1 << 20).await.unwrap();
        Answer {
            status: parts.status,
            headers: parts.headers,
            body: String::from_utf8(bytes.to_vec()).unwrap(),
        }
    }
}

/// A response with its body read.
#[derive(Debug)]
pub struct Answer {
    pub status: StatusCode,
    pub headers: http::HeaderMap,
    pub body: String,
}

impl Answer {
    /// The text of the first `<name>` element in the body.
    pub fn element(&self, name: &str) -> Option<&str> {
        let open = format!("<{name}>");
        let start = self.body.find(&open)? + open.len();
        let end = self.body[start..].find(&format!("</{name}>"))? + start;
        Some(&self.body[start..end])
    }

    /// The error code in the body, if any.
    pub fn code(&self) -> Option<&str> {
        self.element("Code")
    }

    #[track_caller]
    pub fn assert(&self, status: u16, code: Option<&str>) {
        assert_eq!(self.status.as_u16(), status, "{self:?}");
        assert_eq!(self.code(), code, "{self:?}");
    }
}
