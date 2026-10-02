//! The real `skys3` binary as a child process.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::Duration;

use super::{client, config_text, eventually, http_get};

/// The binary under test.
pub const BINARY: &str = env!("CARGO_BIN_EXE_skys3");

/// A free TCP port on loopback.
pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Writes a configuration for `dir` with fresh ports, and returns its
/// path, the gateway's address, and the admin listener's.
pub fn configure(dir: &Path) -> (PathBuf, String, String) {
    let gateway = format!("127.0.0.1:{}", free_port());
    let admin = format!("127.0.0.1:{}", free_port());
    let path = dir.join("skys3.toml");
    std::fs::write(&path, config_text(dir, &gateway, &admin, "")).unwrap();
    (path, gateway, admin)
}

/// A node process and where it serves.
pub struct Process {
    child: Child,
    pub gateway: String,
    pub admin: String,
}

impl Process {
    /// Starts the binary with the configuration at `config`, appending its
    /// log to `log`, and waits until it is ready.
    pub async fn start(config: &Path, gateway: &str, admin: &str, log: &Path) -> Self {
        Self::start_with_env(config, gateway, admin, log, &[]).await
    }

    /// Like [`Process::start`], with `env` added to the binary's
    /// environment.
    pub async fn start_with_env(
        config: &Path,
        gateway: &str,
        admin: &str,
        log: &Path,
        env: &[(&str, &Path)],
    ) -> Self {
        let log_path = log.to_owned();
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log)
            .unwrap();
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
            gateway: gateway.to_owned(),
            admin: admin.to_owned(),
        };
        let admin = process.admin.clone();
        eventually("the node is ready", || {
            let admin = admin.clone();
            let exited = process.child.try_wait().unwrap();
            let log_path = log_path.clone();
            async move {
                if exited.is_some() {
                    let log = std::fs::read_to_string(&log_path).unwrap_or_default();
                    panic!("the node exited: {exited:?}; its log:\n{log}");
                }
                matches!(http_get(&admin, "/readyz").await, Ok((200, _)))
            }
        })
        .await;
        process
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
