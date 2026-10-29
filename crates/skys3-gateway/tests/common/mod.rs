//! Shared setup for the gateway's integration tests: a gateway over an
//! in-memory control store, wrapped for fault injection, and the shard
//! stub.

#![allow(dead_code)]

pub mod signing;

use std::time::Duration;

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use s3s::Body;
use skys3_config::Config;
use skys3_control::faults::FaultyStore;
use skys3_control::{
    MemoryControlStore, ProposalIds, RetryPolicy, TypedKey, bootstrap, read, read_cluster,
};
use skys3_gateway::stub::MemoryShards;
use skys3_gateway::{
    Authenticator, Gateway, GatewayConfig, IdSource, MODE_HEADER, TARGET_HEADER, TrustAll,
};
use skys3_types::{BucketDocument, BucketName, Generation};

pub type Store = FaultyStore<MemoryControlStore>;

pub struct Setup {
    pub gateway: Gateway<TrustAll>,
    pub store: Store,
    pub memory: MemoryControlStore,
    pub shards: MemoryShards,
}

/// A response with its body read.
#[derive(Debug)]
pub struct Answer {
    pub status: StatusCode,
    pub headers: http::HeaderMap,
    pub body: String,
}

impl Answer {
    /// The S3 error code in the body, if any.
    pub fn code(&self) -> Option<&str> {
        let start = self.body.find("<Code>")? + "<Code>".len();
        let end = self.body[start..].find("</Code>")? + start;
        Some(&self.body[start..end])
    }

    #[track_caller]
    pub fn assert(&self, status: u16, code: Option<&str>) {
        assert_eq!(self.status.as_u16(), status, "{self:?}");
        assert_eq!(self.code(), code, "{self:?}");
    }
}

pub fn config(extra: &str) -> GatewayConfig {
    let control_store = if extra.contains("[control_store]") {
        ""
    } else {
        "[control_store]\netcd_endpoints = [\"https://etcd.invalid:2379\"]\n"
    };
    let text = format!("[cluster]\ncluster_id = \"test\"\n{control_store}{extra}");
    let config: Config = text.parse().unwrap();
    let mut config = GatewayConfig::new(&config);
    config.retry = RetryPolicy {
        max_attempts: 3,
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(2),
    };
    config
}

pub async fn setup(extra: &str) -> Setup {
    setup_with(config(extra)).await
}

pub async fn setup_with(config: GatewayConfig) -> Setup {
    let memory = MemoryControlStore::new();
    let store = FaultyStore::new(memory.clone());
    let policy = RetryPolicy::default();
    bootstrap(
        &memory,
        &config.cluster_id,
        ProposalIds::seeded(1).next_id(),
        &policy,
    )
    .await
    .unwrap();
    let shards = MemoryShards::new().await;
    let gateway = Gateway::new(
        config,
        store.clone(),
        shards.clone(),
        IdSource::seeded(7),
        TrustAll,
    )
    .await
    .unwrap();
    Setup {
        gateway,
        store,
        memory,
        shards,
    }
}

/// A gateway over fresh in-memory state that authenticates with `auth`.
pub async fn gateway_with<A: Authenticator>(config: GatewayConfig, auth: A) -> Gateway<A> {
    let memory = MemoryControlStore::new();
    bootstrap(
        &memory,
        &config.cluster_id,
        ProposalIds::seeded(1).next_id(),
        &RetryPolicy::default(),
    )
    .await
    .unwrap();
    Gateway::new(
        config,
        memory,
        MemoryShards::new().await,
        IdSource::seeded(7),
        auth,
    )
    .await
    .unwrap()
}

impl Setup {
    pub async fn send(&self, request: Request<Body>) -> Answer {
        answer(self.gateway.handle(request).await).await
    }

    pub async fn call(
        &self,
        method: Method,
        uri: &str,
        headers: &[(&str, &str)],
        body: &str,
    ) -> Answer {
        self.send(request(method, uri, headers, body)).await
    }

    pub async fn create(&self, name: &str, mode: &str, target: Option<&str>) -> Answer {
        let mut headers = vec![(MODE_HEADER, mode)];
        if let Some(target) = target {
            headers.push((TARGET_HEADER, target));
        }
        self.call(Method::PUT, &format!("/{name}"), &headers, "")
            .await
    }

    pub async fn create_local(&self, name: &str) -> BucketDocument {
        self.create(name, "local", None).await.assert(200, None);
        self.register(name).await.unwrap()
    }

    pub async fn create_write_back(&self, name: &str) -> BucketDocument {
        let target = "https://s3.example.com/remote/prefix/";
        self.create(name, "write_back", Some(target))
            .await
            .assert(200, None);
        self.register(name).await.unwrap()
    }

    /// The bucket register in the control store.
    pub async fn register(&self, name: &str) -> Option<BucketDocument> {
        let name = BucketName::new(name).unwrap();
        read(&self.memory, &TypedKey::bucket(&name))
            .await
            .unwrap()
            .map(|register| register.value)
    }

    pub async fn generation(&self) -> Generation {
        let cluster = "test".parse().unwrap();
        read_cluster(&self.memory, &cluster, &RetryPolicy::default())
            .await
            .unwrap()
            .value
            .generation
    }
}

pub fn request(method: Method, uri: &str, headers: &[(&str, &str)], body: &str) -> Request<Body> {
    let mut request = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    request
        .body(Body::from(Bytes::copy_from_slice(body.as_bytes())))
        .unwrap()
}

pub async fn answer(response: Response<Body>) -> Answer {
    let (parts, mut body) = response.into_parts();
    let bytes = body.store_all_limited(1 << 20).await.unwrap();
    Answer {
        status: parts.status,
        headers: parts.headers,
        body: String::from_utf8(bytes.to_vec()).unwrap(),
    }
}
