//! The real `skys3` binary as a child process.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::Duration;

use super::{client, config_text, http_get};

/// The binary under test.
pub const BINARY: &str = env!("CARGO_BIN_EXE_skys3");

/// How many times [`Process::start`] starts the binary when it cannot bind
/// the ports its configuration names.
pub const START_ATTEMPTS: usize = 5;

/// What the binary logs when an address is taken: the `Display` of
/// `io::ErrorKind::AddrInUse` on Linux and macOS.
const ADDRESS_IN_USE: &str = "Address already in use";

/// How long a readiness probe may wait for an answer.
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// `N` distinct free TCP ports on loopback. The listeners that find them
/// are held until all are chosen, so none repeats.
pub fn free_ports<const N: usize>() -> [u16; N] {
    let listeners: [std::net::TcpListener; N] =
        std::array::from_fn(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap());
    listeners.map(|listener| listener.local_addr().unwrap().port())
}

/// A free TCP port on loopback. It is free when chosen only: anything may
/// bind it before the binary does, which [`Process::start`] recovers from.
pub fn free_port() -> u16 {
    let [port] = free_ports();
    port
}

/// Fresh loopback addresses for a gateway and an admin listener.
pub fn fresh_addresses() -> (String, String) {
    let [gateway, admin] = free_ports().map(|port| format!("127.0.0.1:{port}"));
    (gateway, admin)
}

/// Writes a configuration for `dir` with fresh ports, and returns its
/// path. Where the node serves is in the [`Process`] that starts it.
pub fn configure(dir: &Path) -> PathBuf {
    let (gateway, admin) = fresh_addresses();
    let path = dir.join("skys3.toml");
    std::fs::write(&path, config_text(dir, &gateway, &admin, "")).unwrap();
    path
}

/// The `listen` address of a configuration's `section` (`"[gateway]"`).
pub fn listen_address(text: &str, section: &str) -> String {
    let mut current = "";
    for line in text.lines().map(str::trim) {
        if line.starts_with('[') {
            current = line;
        } else if current == section
            && let Some(value) = line.strip_prefix("listen = ")
        {
            return value.trim_matches('"').to_owned();
        }
    }
    panic!("the configuration has no listen address in {section}:\n{text}")
}

/// Moves the gateway and the admin listener of the configuration at
/// `config` to fresh ports.
fn move_to_fresh_ports(config: &Path) {
    let text = std::fs::read_to_string(config).unwrap();
    let old = |section| format!("listen = \"{}\"", listen_address(&text, section));
    let (gateway, admin) = fresh_addresses();
    let moved = text
        .replace(&old("[gateway]"), &format!("listen = \"{gateway}\""))
        .replace(&old("[admin]"), &format!("listen = \"{admin}\""));
    std::fs::write(config, moved).unwrap();
}

/// A node process and where it serves.
pub struct Process {
    child: Child,
    pub gateway: String,
    pub admin: String,
}

/// How one start of the binary ended.
enum Started {
    Ready(Process),
    /// The binary exited because a port was taken.
    AddressInUse,
}

impl Process {
    /// Starts the binary with the configuration at `config`, appending its
    /// log to `log`, and waits until it is ready.
    ///
    /// The node serves where the configuration's `[gateway]` and `[admin]`
    /// sections say, unless a port there is taken. Test ports are free
    /// only when chosen ([`free_port`]), and a test running in parallel,
    /// or anything else, can bind one before the binary does, also while a
    /// node is down between restarts. When the binary exits because an
    /// address is in use, the configuration is rewritten with fresh ports
    /// and the binary started again, up to [`START_ATTEMPTS`] times in
    /// all. The returned process names the addresses it serves on. Any
    /// other exit before the node is ready panics with the log.
    pub async fn start(config: &Path, log: &Path) -> Self {
        Self::start_with_env(config, log, &[]).await
    }

    /// Like [`Process::start`], with `env` added to the binary's
    /// environment.
    pub async fn start_with_env(config: &Path, log: &Path, env: &[(&str, &Path)]) -> Self {
        for attempt in 1..=START_ATTEMPTS {
            match Self::try_start(config, log, env).await {
                Started::Ready(process) => return process,
                Started::AddressInUse if attempt < START_ATTEMPTS => {
                    eprintln!(
                        "a port of {} was taken; moving to fresh ones",
                        config.display()
                    );
                    move_to_fresh_ports(config);
                }
                Started::AddressInUse => {}
            }
        }
        let log = std::fs::read_to_string(log).unwrap_or_default();
        panic!("the node found a port taken in each of {START_ATTEMPTS} starts; its log:\n{log}")
    }

    /// Starts the binary once and waits until it is ready or has exited.
    async fn try_start(config: &Path, log_path: &Path, env: &[(&str, &Path)]) -> Started {
        let text = std::fs::read_to_string(config).unwrap();
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
            .unwrap();
        // Where this start's lines begin: every start appends to the log.
        let offset = usize::try_from(log.metadata().unwrap().len()).unwrap();
        let child = Command::new(BINARY)
            .arg("--config")
            .arg(config)
            .envs(env.iter().copied())
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .unwrap();
        let mut process = Self {
            child,
            gateway: listen_address(&text, "[gateway]"),
            admin: listen_address(&text, "[admin]"),
        };
        // The node logs `serving` once it holds both ports. Until then, the
        // admin port may be someone else's, which may answer `/readyz`
        // itself, or accept and never answer, so each probe has a deadline.
        let serving = format!("admin_addr={}", process.admin);
        for _ in 0..300 {
            let log = std::fs::read(log_path).unwrap_or_default();
            let ours = String::from_utf8_lossy(log.get(offset..).unwrap_or_default());
            if let Some(exited) = process.child.try_wait().unwrap() {
                if ours.contains(ADDRESS_IN_USE) {
                    return Started::AddressInUse;
                }
                let log = String::from_utf8_lossy(&log);
                panic!("the node exited: {exited:?}; its log:\n{log}");
            }
            let bound = ours
                .lines()
                .any(|line| line.contains(" serving ") && line.contains(&serving));
            if bound {
                let probe = http_get(&process.admin, "/readyz");
                if let Ok(Ok((200, _))) = tokio::time::timeout(PROBE_TIMEOUT, probe).await {
                    return Started::Ready(process);
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let log = std::fs::read_to_string(log_path).unwrap_or_default();
        panic!("timed out waiting until the node is ready; its log:\n{log}");
    }

    /// An S3 client for the node's gateway.
    pub fn s3(&self) -> aws_sdk_s3::Client {
        client(&format!("http://{}", self.gateway))
    }

    /// Sends `SIGTERM` and waits for the process to exit.
    pub async fn terminate(mut self) -> ExitStatus {
        let status = Command::new("kill")
            .arg("-TERM")
            .arg(self.child.id().to_string())
            .status()
            .unwrap();
        assert!(status.success());
        for _ in 0..300 {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        self.child.kill().unwrap();
        panic!("the node did not exit after SIGTERM");
    }

    /// Kills the process with `SIGKILL` and waits until it is gone.
    pub fn kill(mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
