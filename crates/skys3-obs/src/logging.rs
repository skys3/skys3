//! Process-wide `tracing` setup.
//!
//! The node binary calls [`init_tracing`] once at startup, before anything
//! logs. Events go to standard error, as text for people or as one JSON
//! object per line for log collectors. Records from dependencies that use
//! the `log` crate are forwarded into `tracing`.

use std::fmt;
use std::io::{self, IsTerminal};
use std::str::FromStr;

use tracing::Subscriber;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::ParseError;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::util::{SubscriberInitExt, TryInitError};

/// The default event filter: `info` and above from every target.
pub const DEFAULT_LOG_FILTER: &str = "info";

/// How log events are written.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LogFormat {
    /// Human-readable lines, colored when standard error is a terminal.
    #[default]
    Text,
    /// One JSON object per event, for log collectors.
    Json,
}

impl LogFormat {
    /// Returns the configuration value naming this format.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Json => "json",
        }
    }
}

impl fmt::Display for LogFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for LogFormat {
    type Err = UnknownLogFormat;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            _ => Err(UnknownLogFormat(s.to_owned())),
        }
    }
}

/// A log format name other than `text` or `json`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnknownLogFormat(String);

impl fmt::Display for UnknownLogFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "unknown log format {:?}, expected \"text\" or \"json\"",
            self.0
        )
    }
}

impl std::error::Error for UnknownLogFormat {}

/// Logging settings: the `[logging]` section of the configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogConfig {
    /// Which events to record, in `tracing_subscriber::EnvFilter` directive
    /// syntax, such as `info` or `info,skys3_flush=debug`.
    pub filter: String,
    /// How events are written.
    pub format: LogFormat,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            filter: DEFAULT_LOG_FILTER.to_owned(),
            format: LogFormat::default(),
        }
    }
}

/// Why [`init_tracing`] failed.
#[derive(Debug)]
pub enum TracingInitError {
    /// The filter is not valid directive syntax.
    InvalidFilter(ParseError),
    /// A global subscriber or `log` logger is already installed.
    AlreadyInitialized(TryInitError),
}

impl fmt::Display for TracingInitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFilter(err) => write!(f, "invalid log filter: {err}"),
            Self::AlreadyInitialized(err) => write!(f, "tracing is already initialized: {err}"),
        }
    }
}

impl std::error::Error for TracingInitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidFilter(err) => Some(err),
            Self::AlreadyInitialized(err) => Some(err),
        }
    }
}

/// Installs the process-wide subscriber that writes events to standard
/// error, and forwards `log` records into it.
///
/// # Errors
///
/// Returns [`TracingInitError::InvalidFilter`] if `config.filter` does not
/// parse, and [`TracingInitError::AlreadyInitialized`] if a subscriber is
/// already installed, for example by an earlier call.
pub fn init_tracing(config: &LogConfig) -> Result<(), TracingInitError> {
    let ansi = io::stderr().is_terminal();
    build_subscriber(config, io::stderr, ansi)?
        .try_init()
        .map_err(TracingInitError::AlreadyInitialized)
}

/// Builds the subscriber that [`init_tracing`] installs, writing to
/// `writer`. Tests use it to capture output without a global subscriber.
///
/// # Errors
///
/// Returns [`TracingInitError::InvalidFilter`] if `config.filter` does not
/// parse.
pub fn build_subscriber<W>(
    config: &LogConfig,
    writer: W,
    ansi: bool,
) -> Result<Box<dyn Subscriber + Send + Sync>, TracingInitError>
where
    W: for<'w> MakeWriter<'w> + Send + Sync + 'static,
{
    let filter = EnvFilter::builder()
        .parse(&config.filter)
        .map_err(TracingInitError::InvalidFilter)?;
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(writer);
    Ok(match config.format {
        LogFormat::Text => Box::new(builder.with_ansi(ansi).finish()),
        LogFormat::Json => Box::new(builder.json().finish()),
    })
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    use super::*;

    /// A `MakeWriter` that collects everything written into one buffer.
    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl Capture {
        fn contents(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    impl Write for Capture {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'w> MakeWriter<'w> for Capture {
        type Writer = Self;

        fn make_writer(&'w self) -> Self::Writer {
            self.clone()
        }
    }

    fn capture(config: &LogConfig, log: impl FnOnce()) -> String {
        let out = Capture::default();
        let subscriber = build_subscriber(config, out.clone(), false).unwrap();
        tracing::subscriber::with_default(subscriber, log);
        out.contents()
    }

    #[test]
    fn text_format_applies_the_filter() {
        let out = capture(&LogConfig::default(), || {
            tracing::info!(shard = 3, "shard opened");
            tracing::debug!("hidden at info");
        });
        assert!(out.contains("INFO"), "{out}");
        assert!(out.contains("shard opened shard=3"), "{out}");
        assert!(!out.contains("hidden at info"), "{out}");
    }

    #[test]
    fn json_format_writes_one_object_per_event() {
        let config = LogConfig {
            filter: "debug".to_owned(),
            format: LogFormat::Json,
        };
        let out = capture(&config, || {
            tracing::debug!(bucket = "photos", "flushed");
        });
        let line = out.lines().next().unwrap();
        assert!(line.starts_with('{') && line.ends_with('}'), "{line}");
        assert!(line.contains(r#""level":"DEBUG""#), "{line}");
        assert!(line.contains(r#""message":"flushed""#), "{line}");
        assert!(line.contains(r#""bucket":"photos""#), "{line}");
    }

    #[test]
    fn invalid_filter_is_rejected() {
        let config = LogConfig {
            filter: "info,[".to_owned(),
            format: LogFormat::Text,
        };
        let err = build_subscriber(&config, io::sink, false).err().unwrap();
        assert!(matches!(err, TracingInitError::InvalidFilter(_)));
        assert!(err.to_string().starts_with("invalid log filter"), "{err}");
        assert!(std::error::Error::source(&err).is_some());
        assert!(matches!(
            init_tracing(&config),
            Err(TracingInitError::InvalidFilter(_))
        ));
    }

    #[test]
    fn global_init_succeeds_once() {
        // The only test in this binary that installs a global subscriber.
        init_tracing(&LogConfig::default()).unwrap();
        let err = init_tracing(&LogConfig::default()).unwrap_err();
        assert!(matches!(err, TracingInitError::AlreadyInitialized(_)));
        assert!(
            err.to_string()
                .starts_with("tracing is already initialized"),
            "{err}"
        );
        assert!(std::error::Error::source(&err).is_some());
    }

    #[test]
    fn log_format_round_trips_through_strings() {
        for format in [LogFormat::Text, LogFormat::Json] {
            assert_eq!(format.to_string().parse::<LogFormat>(), Ok(format));
        }
        let err = "yaml".parse::<LogFormat>().unwrap_err();
        assert_eq!(
            err.to_string(),
            r#"unknown log format "yaml", expected "text" or "json""#
        );
        assert_eq!(LogFormat::default(), LogFormat::Text);
    }
}
