//! End-to-end tests of the admin listener over real TCP connections.

use std::net::SocketAddr;

use skys3_obs::prometheus_client::metrics::gauge::Gauge;
use skys3_obs::prometheus_client::registry::Unit;
use skys3_obs::{AdminConfig, AdminError, AdminListener, AdminToken, Health, MetricsRegistry};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const TOKEN: &str = "c0ffee00c0ffee00c0ffee00c0ffee00c0ffee00";

/// A listener serving on an ephemeral loopback port.
struct Running {
    addr: SocketAddr,
    stop: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

impl Running {
    async fn start(token: Option<&str>, metrics: MetricsRegistry, health: Health) -> Self {
        let config = AdminConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            token: token.map(|t| AdminToken::new(t).unwrap()),
        };
        let listener = AdminListener::bind(config, metrics, health).await.unwrap();
        let addr = listener.local_addr().unwrap();
        assert!(format!("{listener:?}").contains(&addr.to_string()));
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(listener.serve(async {
            let _ = stopped.await;
        }));
        Self { addr, stop, task }
    }

    async fn shutdown(self) {
        self.stop.send(()).unwrap();
        self.task.await.unwrap();
    }
}

/// Sends one HTTP/1.1 request and returns the status code and the body.
async fn get(addr: SocketAddr, path: &str, token: Option<&str>) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let auth = token.map_or(String::new(), |t| format!("Authorization: Bearer {t}\r\n"));
    let request = format!("GET {path} HTTP/1.1\r\nHost: skys3\r\n{auth}Connection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    let status = head.split(' ').nth(1).unwrap().parse().unwrap();
    (status, body.to_owned())
}

#[tokio::test]
async fn scrapes_a_registered_metric() {
    let metrics = MetricsRegistry::new();
    let dirty: Gauge = Gauge::default();
    metrics.register_with_unit(
        "dirty",
        "Bytes not yet flushed.",
        Unit::Bytes,
        dirty.clone(),
    );
    let running = Running::start(None, metrics, Health::new()).await;

    dirty.set(4096);
    let (status, body) = get(running.addr, "/metrics", None).await;
    assert_eq!(status, 200);
    assert!(body.contains("# TYPE skys3_dirty_bytes gauge\n"), "{body}");
    assert!(body.contains("skys3_dirty_bytes 4096\n"), "{body}");
    assert!(body.contains("skys3_build_info{version="), "{body}");
    assert!(body.ends_with("# EOF\n"), "{body}");

    // The first scrape is counted by the next one.
    let (_, body) = get(running.addr, "/metrics", None).await;
    assert!(
        body.contains(r#"skys3_admin_requests_total{endpoint="metrics",code="200"} 1"#),
        "{body}"
    );

    running.shutdown().await;
}

#[tokio::test]
async fn token_protects_metrics_but_not_health_checks() {
    let health = Health::new();
    let recovery = health.register("recovery");
    let running = Running::start(Some(TOKEN), MetricsRegistry::new(), health).await;

    assert_eq!(get(running.addr, "/metrics", None).await.0, 401);
    assert_eq!(
        get(running.addr, "/metrics", Some("not-the-token")).await.0,
        401
    );
    assert_eq!(get(running.addr, "/metrics", Some(TOKEN)).await.0, 200);

    assert_eq!(
        get(running.addr, "/healthz", None).await,
        (200, "ok\n".to_owned())
    );
    assert_eq!(
        get(running.addr, "/readyz", None).await,
        (503, "not ready: recovery\n".to_owned())
    );
    recovery.set_ready(true);
    assert_eq!(
        get(running.addr, "/readyz", None).await,
        (200, "ready\n".to_owned())
    );

    running.shutdown().await;
}

#[tokio::test]
async fn refuses_a_non_loopback_address_without_a_token() {
    let config = AdminConfig {
        listen: "0.0.0.0:0".parse().unwrap(),
        token: None,
    };
    let err = AdminListener::bind(config, MetricsRegistry::new(), Health::new())
        .await
        .unwrap_err();
    assert!(matches!(err, AdminError::TokenRequired { .. }), "{err}");
}

#[tokio::test]
async fn reports_an_address_in_use() {
    let running = Running::start(None, MetricsRegistry::new(), Health::new()).await;
    let config = AdminConfig {
        listen: running.addr,
        token: None,
    };
    let err = AdminListener::bind(config, MetricsRegistry::new(), Health::new())
        .await
        .unwrap_err();
    assert!(matches!(err, AdminError::Bind { .. }), "{err}");
    running.shutdown().await;
}

#[tokio::test]
async fn stops_accepting_after_shutdown() {
    let running = Running::start(None, MetricsRegistry::new(), Health::new()).await;
    let addr = running.addr;
    running.shutdown().await;
    assert!(TcpStream::connect(addr).await.is_err());
}
