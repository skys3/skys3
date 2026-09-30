#![forbid(unsafe_code)]
//! Observability for a SkyS3 node: `tracing` setup, the metrics registry,
//! and the admin HTTP listener that serves metrics and health checks.
//!
//! - [`logging`] installs the process-wide `tracing` subscriber.
//! - [`metrics`] holds the node's [`MetricsRegistry`] and enforces the
//!   metric naming conventions. Every metric is listed in the metrics
//!   reference, `docs/skys3-metrics.md`.
//! - [`health`] tracks the readiness of the node's components.
//! - [`admin`] serves `/metrics`, `/healthz`, and `/readyz`, and decides
//!   how callers authenticate (design section 12).
//!
//! # Example
//!
//! ```no_run
//! use prometheus_client::metrics::gauge::Gauge;
//! use prometheus_client::registry::Unit;
//! use skys3_obs::{AdminConfig, AdminListener, Health, LogConfig, MetricsRegistry};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! skys3_obs::init_tracing(&LogConfig::default())?;
//!
//! let metrics = MetricsRegistry::new();
//! let dirty: Gauge = Gauge::default();
//! metrics.register_with_unit("dirty", "Bytes not yet flushed.", Unit::Bytes, dirty.clone());
//!
//! let health = Health::new();
//! let recovery = health.register("recovery");
//! let listener = AdminListener::bind(AdminConfig::default(), metrics, health).await?;
//! tokio::spawn(listener.serve(std::future::pending()));
//! recovery.set_ready(true);
//! # Ok(())
//! # }
//! ```

pub mod admin;
pub mod health;
pub mod logging;
pub mod metrics;

pub use admin::{AdminConfig, AdminError, AdminListener, AdminToken, AdminTokenError};
pub use health::{Health, Readiness};
pub use logging::{LogConfig, LogFormat, TracingInitError, init_tracing};
pub use metrics::MetricsRegistry;
pub use prometheus_client;
