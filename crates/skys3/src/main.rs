#![forbid(unsafe_code)]
//! The `skys3` node binary.
//!
//! ```text
//! skys3 [--config <path>] [--check-config]
//! ```
//!
//! It loads and validates the configuration (default
//! `/etc/skys3/skys3.toml`), sets up logging from `[logging]`, starts the
//! node, and serves until `SIGTERM` or `SIGINT`, then shuts down
//! gracefully. `--check-config` only validates the configuration.

use std::path::PathBuf;
use std::process::ExitCode;

use skys3::{Node, log_config};
use skys3_config::Config;

/// The configuration file read without `--config`.
const DEFAULT_CONFIG: &str = "/etc/skys3/skys3.toml";

const USAGE: &str = "usage: skys3 [--config <path>] [--check-config]";

/// What the command line asks for.
struct Args {
    config: PathBuf,
    check_only: bool,
}

/// Parses the command line, or returns the text to print and the exit
/// code for `--help`, `--version`, or a mistake.
fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, (String, ExitCode)> {
    let mut parsed = Args {
        config: PathBuf::from(DEFAULT_CONFIG),
        check_only: false,
    };
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" | "-c" => match args.next() {
                Some(path) => parsed.config = PathBuf::from(path),
                None => return Err((format!("--config needs a path\n{USAGE}"), ExitCode::from(2))),
            },
            "--check-config" => parsed.check_only = true,
            "--help" | "-h" => return Err((USAGE.to_owned(), ExitCode::SUCCESS)),
            "--version" | "-V" => {
                return Err((
                    format!("skys3 {}", env!("CARGO_PKG_VERSION")),
                    ExitCode::SUCCESS,
                ));
            }
            other => {
                return Err((
                    format!("unknown argument {other:?}\n{USAGE}"),
                    ExitCode::from(2),
                ));
            }
        }
    }
    Ok(parsed)
}

/// Completes on the first `SIGTERM` or `SIGINT`.
async fn termination() {
    let mut terminate =
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(signal) => signal,
            Err(error) => {
                tracing::error!(%error, "cannot listen for SIGTERM");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
    tokio::select! {
        _ = terminate.recv() => tracing::info!("received SIGTERM"),
        _ = tokio::signal::ctrl_c() => tracing::info!("received SIGINT"),
    }
}

fn main() -> ExitCode {
    let args = match parse_args(std::env::args().skip(1)) {
        Ok(args) => args,
        Err((text, code)) => {
            if code == ExitCode::SUCCESS {
                println!("{text}");
            } else {
                eprintln!("{text}");
            }
            return code;
        }
    };
    let config = match Config::load(&args.config) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{}: {error}", args.config.display());
            return ExitCode::from(2);
        }
    };
    if args.check_only {
        println!("{}: valid", args.config.display());
        return ExitCode::SUCCESS;
    }
    if let Err(error) = skys3_obs::init_tracing(&log_config(&config)) {
        eprintln!("cannot set up logging: {error}");
        return ExitCode::from(2);
    }
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("skys3")
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            tracing::error!(%error, "cannot start the runtime");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(async {
        let node = match Node::start(config).await {
            Ok(node) => node,
            Err(error) => {
                tracing::error!(%error, "the node cannot start");
                return ExitCode::FAILURE;
            }
        };
        match node.run_until(termination()).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                tracing::error!(%error, "the node stopped with an error");
                ExitCode::FAILURE
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Args, (String, ExitCode)> {
        parse_args(args.iter().map(|arg| (*arg).to_owned()))
    }

    #[test]
    fn arguments_are_parsed() {
        let args = parse(&[]).ok().unwrap();
        assert_eq!(args.config, PathBuf::from(DEFAULT_CONFIG));
        assert!(!args.check_only);
        let args = parse(&["--config", "/x.toml", "--check-config"])
            .ok()
            .unwrap();
        assert_eq!(args.config, PathBuf::from("/x.toml"));
        assert!(args.check_only);
        for (args, code) in [
            (&["--help"][..], ExitCode::SUCCESS),
            (&["-V"][..], ExitCode::SUCCESS),
            (&["--config"][..], ExitCode::from(2)),
            (&["--bogus"][..], ExitCode::from(2)),
        ] {
            let Err((text, exit)) = parse(args) else {
                panic!("{args:?} parsed");
            };
            assert_eq!(exit, code, "{args:?}");
            assert!(!text.is_empty());
        }
    }
}
