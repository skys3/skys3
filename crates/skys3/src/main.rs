#![forbid(unsafe_code)]
//! The `skys3` node binary.
//!
//! ```text
//! skys3 [--config <path>] [--check-config]
//! skys3 control export [--config <path>] [--output <path>]
//! skys3 control rebuild [--config <path>] --from <export>... [--lost <node-id>]...
//!                       [--allow-unnamed] [--dry-run]
//! ```
//!
//! It loads and validates the configuration (default
//! `/etc/skys3/skys3.toml`), sets up logging from `[logging]`, starts the
//! node, and serves until `SIGTERM` or `SIGINT`, then shuts down
//! gracefully. `--check-config` only validates the configuration.
//!
//! `control export` and `control rebuild` rebuild a lost control store
//! from the nodes' local copies (design §6.2, §6.9; `skys3::rebuild`):
//! with every node stopped, each node's export is written as JSON to
//! `--output` or standard output, and the rebuild merges the exports and
//! writes them into the empty store. `--dry-run` shows the plan and
//! writes nothing; `--lost` names a node whose disks are lost for good;
//! `--allow-unnamed` rebuilds although shards holding objects belong to
//! no bucket of the newest copy.

use std::collections::BTreeSet;
use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use skys3::{Node, log_config};
use skys3_config::Config;
use skys3_control::{ControlExport, RebuildOptions};
use skys3_types::NodeId;

/// The configuration file read without `--config`.
const DEFAULT_CONFIG: &str = "/etc/skys3/skys3.toml";

const USAGE: &str = "usage: skys3 [--config <path>] [--check-config]
       skys3 control export [--config <path>] [--output <path>]
       skys3 control rebuild [--config <path>] --from <export>... [--lost <node-id>]...
                             [--allow-unnamed] [--dry-run]";

/// What the command line asks for.
#[derive(Debug, PartialEq, Eq)]
struct Args {
    config: PathBuf,
    command: Command,
}

/// The command to run.
#[derive(Debug, PartialEq, Eq)]
enum Command {
    /// Run the node, or with `check_only` only validate the configuration.
    Serve { check_only: bool },
    /// Export the stopped node's control state.
    Export { output: Option<PathBuf> },
    /// Rebuild the control store from exports.
    Rebuild {
        from: Vec<PathBuf>,
        lost: Vec<String>,
        allow_unnamed: bool,
        dry_run: bool,
    },
}

/// A usage mistake: the message and the usage, with exit code 2.
fn usage(message: impl std::fmt::Display) -> (String, ExitCode) {
    (format!("{message}\n{USAGE}"), ExitCode::from(2))
}

/// The command a command line starts with: `control export`, `control
/// rebuild`, or serving.
fn command(
    args: &mut std::iter::Peekable<impl Iterator<Item = String>>,
) -> Result<Command, (String, ExitCode)> {
    if args.peek().map(String::as_str) != Some("control") {
        return Ok(Command::Serve { check_only: false });
    }
    args.next();
    match args.next().as_deref() {
        Some("export") => Ok(Command::Export { output: None }),
        Some("rebuild") => Ok(Command::Rebuild {
            from: Vec::new(),
            lost: Vec::new(),
            allow_unnamed: false,
            dry_run: false,
        }),
        Some(other) => Err(usage(format!("unknown control command {other:?}"))),
        None => Err(usage("control needs a command: export or rebuild")),
    }
}

/// Parses the command line, or returns the text to print and the exit
/// code for `--help`, `--version`, or a mistake.
fn parse_args(args: impl Iterator<Item = String>) -> Result<Args, (String, ExitCode)> {
    let mut args = args.peekable();
    let mut command = command(&mut args)?;
    let mut config = PathBuf::from(DEFAULT_CONFIG);
    while let Some(arg) = args.next() {
        let mut value = |name: &str| {
            args.next()
                .ok_or_else(|| usage(format!("{name} needs a value")))
        };
        match (arg.as_str(), &mut command) {
            ("--config" | "-c", _) => config = PathBuf::from(value("--config")?),
            ("--check-config", Command::Serve { check_only }) => *check_only = true,
            ("--output" | "-o", Command::Export { output }) => {
                *output = Some(PathBuf::from(value("--output")?));
            }
            ("--from", Command::Rebuild { from, .. }) => from.push(PathBuf::from(value("--from")?)),
            ("--lost", Command::Rebuild { lost, .. }) => lost.push(value("--lost")?),
            ("--allow-unnamed", Command::Rebuild { allow_unnamed, .. }) => *allow_unnamed = true,
            ("--dry-run", Command::Rebuild { dry_run, .. }) => *dry_run = true,
            ("--help" | "-h", _) => return Err((USAGE.to_owned(), ExitCode::SUCCESS)),
            ("--version" | "-V", _) => {
                return Err((
                    format!("skys3 {}", env!("CARGO_PKG_VERSION")),
                    ExitCode::SUCCESS,
                ));
            }
            (other, _) => return Err(usage(format!("unknown argument {other:?}"))),
        }
    }
    if let Command::Rebuild { from, .. } = &command
        && from.is_empty()
    {
        return Err(usage("control rebuild needs at least one --from <export>"));
    }
    Ok(Args { config, command })
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
    if args.command == (Command::Serve { check_only: true }) {
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
    match args.command {
        Command::Serve { .. } => runtime.block_on(serve(config)),
        Command::Export { output } => runtime.block_on(export(&config, output)),
        Command::Rebuild {
            from,
            lost,
            allow_unnamed,
            dry_run,
        } => runtime.block_on(rebuild(&config, &from, &lost, allow_unnamed, dry_run)),
    }
}

/// Runs the node until `SIGTERM` or `SIGINT`.
async fn serve(config: Config) -> ExitCode {
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
}

/// `skys3 control export`: writes the stopped node's export to `output`,
/// or to standard output.
async fn export(config: &Config, output: Option<PathBuf>) -> ExitCode {
    let exported = match skys3::rebuild::export_data_dir(config).await {
        Ok(exported) => exported,
        Err(error) => {
            eprintln!("cannot export the node's control state: {error}");
            return ExitCode::FAILURE;
        }
    };
    let json = exported.to_json();
    let written = match &output {
        Some(path) => std::fs::write(path, &json),
        None => {
            let mut stdout = std::io::stdout().lock();
            stdout
                .write_all(&json)
                .and_then(|()| stdout.write_all(b"\n"))
        }
    };
    if let Err(error) = written {
        eprintln!("cannot write the export: {error}");
        return ExitCode::FAILURE;
    }
    eprintln!(
        "exported node {}: {} shard configurations, {} shards held, {}",
        exported.node_id,
        exported.configs.len(),
        exported.held.len(),
        exported.copy.as_ref().map_or_else(
            || "no copy of the bucket and identity registers".to_owned(),
            |copy| format!("a copy at generation {}", copy.generation)
        )
    );
    ExitCode::SUCCESS
}

/// `skys3 control rebuild`: rebuilds the control store from the exports in
/// `from`, or with `dry_run` shows the plan.
async fn rebuild(
    config: &Config,
    from: &[PathBuf],
    lost: &[String],
    allow_unnamed: bool,
    dry_run: bool,
) -> ExitCode {
    let mut exports = Vec::with_capacity(from.len());
    for path in from {
        let parsed = std::fs::read(path)
            .map_err(|error| error.to_string())
            .and_then(|bytes| ControlExport::from_json(&bytes).map_err(|error| error.to_string()));
        match parsed {
            Ok(export) => exports.push(export),
            Err(error) => {
                eprintln!("{}: {error}", path.display());
                return ExitCode::from(2);
            }
        }
    }
    let mut options = RebuildOptions {
        lost: BTreeSet::new(),
        allow_unnamed,
    };
    for node in lost {
        match NodeId::new(node.as_str()) {
            Ok(node) => {
                options.lost.insert(node);
            }
            Err(error) => {
                eprintln!("--lost {node}: {error}");
                return ExitCode::from(2);
            }
        }
    }
    match skys3::rebuild::rebuild(config, &exports, &options, dry_run).await {
        Ok(rebuilt) => {
            for line in skys3::rebuild::summary(&rebuilt.plan) {
                println!("{line}");
            }
            match rebuilt.applied {
                Some(applied) => println!(
                    "rebuilt: wrote {} registers, found {} written before",
                    applied.written, applied.present
                ),
                None => println!("dry run: nothing written"),
            }
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("the rebuild was refused: {error}");
            ExitCode::FAILURE
        }
    }
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
        assert_eq!(args.command, Command::Serve { check_only: false });
        let args = parse(&["--config", "/x.toml", "--check-config"])
            .ok()
            .unwrap();
        assert_eq!(args.config, PathBuf::from("/x.toml"));
        assert_eq!(args.command, Command::Serve { check_only: true });
        for (args, code) in [
            (&["--help"][..], ExitCode::SUCCESS),
            (&["-V"][..], ExitCode::SUCCESS),
            (&["--config"][..], ExitCode::from(2)),
            (&["--bogus"][..], ExitCode::from(2)),
            (&["control"][..], ExitCode::from(2)),
            (&["control", "bogus"][..], ExitCode::from(2)),
            (&["control", "rebuild"][..], ExitCode::from(2)),
            (
                &["control", "export", "--check-config"][..],
                ExitCode::from(2),
            ),
            (&["control", "export", "--output"][..], ExitCode::from(2)),
            (&["--output", "x"][..], ExitCode::from(2)),
            (&["control", "rebuild", "--help"][..], ExitCode::SUCCESS),
        ] {
            let Err((text, exit)) = parse(args) else {
                panic!("{args:?} parsed");
            };
            assert_eq!(exit, code, "{args:?}");
            assert!(!text.is_empty());
        }
    }

    #[test]
    fn control_commands_are_parsed() {
        let args = parse(&["control", "export", "-c", "/x.toml", "-o", "/e.json"])
            .ok()
            .unwrap();
        assert_eq!(args.config, PathBuf::from("/x.toml"));
        assert_eq!(
            args.command,
            Command::Export {
                output: Some(PathBuf::from("/e.json"))
            }
        );
        let args = parse(&[
            "control",
            "rebuild",
            "--from",
            "a.json",
            "--from",
            "b.json",
            "--lost",
            "node-3",
            "--allow-unnamed",
            "--dry-run",
        ])
        .ok()
        .unwrap();
        assert_eq!(
            args.command,
            Command::Rebuild {
                from: vec![PathBuf::from("a.json"), PathBuf::from("b.json")],
                lost: vec!["node-3".to_owned()],
                allow_unnamed: true,
                dry_run: true,
            }
        );
    }
}
