//! `[admin]` and `[logging]`: the admin HTTP listener and log output.
//!
//! The binary maps these sections to the observability crate; this crate
//! only defines and checks them.

use std::net::SocketAddr;
use std::path::PathBuf;

use serde::Deserialize;

use crate::error::Checker;

/// `[admin]`: the admin HTTP listener (metrics, health, and the admin API).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AdminConfig {
    /// `listen`: the TCP address of the admin listener.
    pub listen: SocketAddr,
    /// `token_file`: a file holding the bearer token callers must present.
    /// Required when `listen` is not a loopback address.
    pub token_file: Option<PathBuf>,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 7490)),
            token_file: None,
        }
    }
}

impl AdminConfig {
    pub(crate) fn check(&self, checker: &mut Checker) {
        match &self.token_file {
            Some(path) => checker.require(!path.as_os_str().is_empty(), "admin.token_file", || {
                "must not be empty".to_owned()
            }),
            None => checker.require(self.listen.ip().is_loopback(), "admin.token_file", || {
                format!(
                    "is required because admin.listen ({}) is not a loopback address",
                    self.listen
                )
            }),
        }
    }
}

/// The format of log lines.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    /// Human-readable text.
    #[default]
    Text,
    /// One JSON object per line.
    Json,
}

/// `[logging]`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LoggingConfig {
    /// `filter`: `tracing` `EnvFilter` directives, such as
    /// `"info,skys3_shard=debug"`. The binary parses them.
    pub filter: String,
    /// `format`.
    pub format: LogFormat,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            filter: "info".to_owned(),
            format: LogFormat::Text,
        }
    }
}
