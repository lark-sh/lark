//! Process management: spawn, kill, and restart the Lark server.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::process::{Child, Command};
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, warn};

/// Manages a child process.
pub struct ManagedProcess {
    pub name: String,
    child: Option<Child>,
    bin_path: PathBuf,
    args: Vec<String>,
}

impl ManagedProcess {
    pub fn new(name: &str, bin_path: PathBuf, args: Vec<String>) -> Self {
        Self {
            name: name.to_string(),
            child: None,
            bin_path,
            args,
        }
    }

    /// Start the process. Returns error if already running.
    pub async fn start(&mut self) -> anyhow::Result<()> {
        if self.is_running() {
            anyhow::bail!("{} is already running", self.name);
        }

        info!(
            "Starting {}: {} {}",
            self.name,
            self.bin_path.display(),
            self.args.join(" ")
        );

        let child = Command::new(&self.bin_path)
            .args(&self.args)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| anyhow::anyhow!("Failed to start {}: {}", self.name, e))?;

        info!("{} started with PID {}", self.name, child.id().unwrap_or(0));
        self.child = Some(child);
        Ok(())
    }

    /// SIGKILL the process immediately.
    pub async fn kill(&mut self) -> anyhow::Result<()> {
        if let Some(ref child) = self.child {
            if let Some(pid) = child.id() {
                info!("SIGKILL {} (PID {})", self.name, pid);
                // Use libc::kill for SIGKILL (more reliable than child.kill() which sends SIGTERM first)
                unsafe {
                    libc::kill(pid as i32, libc::SIGKILL);
                }
            }
        }

        // Wait for the process to actually exit
        if let Some(mut child) = self.child.take() {
            match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
                Ok(Ok(status)) => {
                    debug!("{} exited with status: {}", self.name, status);
                }
                Ok(Err(e)) => {
                    warn!("{} wait error: {}", self.name, e);
                }
                Err(_) => {
                    error!("{} did not exit within 5s after SIGKILL", self.name);
                }
            }
        }

        Ok(())
    }

    /// Check if the process is still running.
    pub fn is_running(&mut self) -> bool {
        if let Some(ref mut child) = self.child {
            match child.try_wait() {
                Ok(None) => true, // Still running
                Ok(Some(_)) => {
                    self.child = None;
                    false // Exited
                }
                Err(_) => false,
            }
        } else {
            false
        }
    }

    /// Get the PID if running.
    pub fn pid(&self) -> Option<u32> {
        self.child.as_ref().and_then(|c| c.id())
    }
}

/// Create a ManagedProcess for the Lark server.
///
/// Sets `--eviction-idle-secs=20` so promoted paths get evicted aggressively
/// during chaos runs — this exercises the lazy-tree promote/evict code paths
/// that prod hits but the default 5-minute timeout would never fire in a short
/// chaos session.
pub fn create_server_process(bin_path: &Path, data_dir: &Path, proxy_port: u16) -> ManagedProcess {
    let args = vec![
        "--id=chaos-1".to_string(),
        "--hostname=localhost".to_string(),
        format!("--proxy-port={}", proxy_port),
        format!("--server-secret={}", crate::SERVER_SECRET),
        "--emulator".to_string(),
        format!("--data-dir={}", data_dir.display()),
        "--nr-cores=1".to_string(),
        "--eviction-idle-secs=20".to_string(),
    ];
    ManagedProcess::new("lark-server", bin_path.to_path_buf(), args)
}

/// Wait for the server to accept TCP connections.
pub async fn wait_for_server(addr: &str, max_wait: Duration) -> anyhow::Result<()> {
    let start = tokio::time::Instant::now();
    loop {
        match tokio::net::TcpStream::connect(addr).await {
            Ok(_) => {
                debug!("Server is accepting connections on {}", addr);
                return Ok(());
            }
            Err(_) => {
                if start.elapsed() > max_wait {
                    anyhow::bail!(
                        "Server did not start accepting connections on {} within {:?}",
                        addr,
                        max_wait
                    );
                }
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
}
