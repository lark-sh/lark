//! Runs a real lark-server and lark-edge, and talks to the edge over HTTP.

use serde_json::Value;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

const SECRET: &str = "rest-conformance-test-secret-0123456789";
const READY_TIMEOUT: Duration = Duration::from_secs(30);
/// EVICT_DATABASE is asynchronous: the database stops at its next loop. A client
/// that connects before then is stranded, so give it time.
const EVICT_SETTLE: Duration = Duration::from_millis(400);

pub const PROJECT: &str = "test-project";
pub const DATABASE: &str = "default";

/// The whole database is open except `/private`.
pub const RULES: &str = r#"{"rules":{"$top":{
    ".read":"$top !== 'private'",
    ".write":"$top !== 'private'"
}}}"#;

pub struct Binaries {
    pub server: PathBuf,
    pub edge: PathBuf,
    pub compact: PathBuf,
}

#[derive(Debug, Clone)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    pub fn json(&self) -> Result<Value, String> {
        serde_json::from_slice(&self.body)
            .map_err(|e| format!("not valid JSON ({e}): {:?}", self.text()))
    }
}

pub struct Stack {
    binaries: Binaries,
    work_dir: PathBuf,
    data_dir: PathBuf,
    proxy_port: u16,
    edge_port: u16,
    server: Option<Child>,
    edge: Option<Child>,
    agent: ureq::Agent,
}

impl Stack {
    pub fn start(
        binaries: Binaries,
        work_dir: PathBuf,
        proxy_port: u16,
        edge_port: u16,
    ) -> Result<Self, String> {
        let data_dir = work_dir.join("data");
        std::fs::create_dir_all(&data_dir).map_err(|e| e.to_string())?;
        let mut stack = Self {
            binaries,
            work_dir,
            data_dir,
            proxy_port,
            edge_port,
            server: None,
            edge: None,
            agent: ureq::AgentBuilder::new().redirects(0).build(),
        };
        stack.spawn_server()?;
        stack.spawn_edge()?;
        stack.wait_ready()?;
        Ok(stack)
    }

    fn log_file(&self, name: &str) -> Result<File, String> {
        File::options()
            .create(true)
            .append(true)
            .open(self.work_dir.join(name))
            .map_err(|e| e.to_string())
    }

    fn spawn_server(&mut self) -> Result<(), String> {
        let log = self.log_file("server.log")?;
        let child = Command::new(&self.binaries.server)
            .args([
                "--id=rest-conformance".to_string(),
                "--hostname=localhost".to_string(),
                format!("--proxy-port={}", self.proxy_port),
                format!("--server-secret={SECRET}"),
                format!("--data-dir={}", self.data_dir.display()),
                "--nr-cores=1".to_string(),
            ])
            .stdout(log.try_clone().map_err(|e| e.to_string())?)
            .stderr(log)
            .spawn()
            .map_err(|e| format!("start lark-server: {e}"))?;
        self.server = Some(child);
        Ok(())
    }

    fn spawn_edge(&mut self) -> Result<(), String> {
        let log = self.log_file("edge.log")?;
        let child = Command::new(&self.binaries.edge)
            .env("LOCAL_MODE", "true")
            .env("LOCAL_PERSISTENT", "true")
            .env("LOCAL_RULES", RULES)
            .env(
                "LOCAL_BACKEND_ADDR",
                format!("127.0.0.1:{}", self.proxy_port),
            )
            .env("LOCAL_PROJECT_ID", PROJECT)
            .env("HTTPS_LISTEN_ADDR", format!("127.0.0.1:{}", self.edge_port))
            .env(
                "INTERNAL_LISTEN_ADDR",
                format!("127.0.0.1:{}", self.edge_port + 1),
            )
            .env("LARKDB_DOMAIN", "localhost")
            .env("SERVER_SECRET", SECRET)
            .env("SESSION_SECRET", SECRET)
            .stdin(Stdio::null())
            .stdout(log.try_clone().map_err(|e| e.to_string())?)
            .stderr(log)
            .spawn()
            .map_err(|e| format!("start lark-edge: {e}"))?;
        self.edge = Some(child);
        Ok(())
    }

    fn wait_ready(&self) -> Result<(), String> {
        let started = Instant::now();
        let mut last = String::new();
        while started.elapsed() < READY_TIMEOUT {
            match self.request("GET", "/.ready-probe.json", &[], None) {
                Ok(r) if r.status == 200 => return Ok(()),
                Ok(r) => last = format!("{} {}", r.status, r.text()),
                Err(e) => last = e,
            }
            sleep(Duration::from_millis(50));
        }
        Err(format!("stack not ready after {READY_TIMEOUT:?}: {last}"))
    }

    /// Sends a request and returns whatever came back. Only a failure to get a
    /// response is an error.
    pub fn request(
        &self,
        method: &str,
        path_and_query: &str,
        headers: &[(&str, &str)],
        body: Option<&[u8]>,
    ) -> Result<Response, String> {
        let url = format!("http://127.0.0.1:{}{path_and_query}", self.edge_port);
        let mut request = self.agent.request(method, &url);
        for (name, value) in headers {
            request = request.set(name, value);
        }
        let result = match body {
            Some(body) => request.send_bytes(body),
            None => request.call(),
        };
        let response = match result {
            Ok(response) | Err(ureq::Error::Status(_, response)) => response,
            Err(e) => return Err(format!("{method} {path_and_query}: {e}")),
        };
        let status = response.status();
        let headers = response
            .headers_names()
            .iter()
            .map(|n| (n.clone(), response.header(n).unwrap_or("").to_string()))
            .collect();
        let mut body = Vec::new();
        response
            .into_reader()
            .read_to_end(&mut body)
            .map_err(|e| format!("{method} {path_and_query}: read body: {e}"))?;
        Ok(Response {
            status,
            headers,
            body,
        })
    }

    /// Port for tools that need a raw connection.
    pub fn edge_port(&self) -> u16 {
        self.edge_port
    }

    /// Whether the server has died since it was started, such as from a failed
    /// debug assertion.
    pub fn server_exit(&mut self) -> Option<String> {
        match self.server.as_mut()?.try_wait() {
            Ok(Some(status)) => Some(status.to_string()),
            _ => None,
        }
    }

    pub fn server_log_tail(&self) -> String {
        let log = std::fs::read(self.work_dir.join("server.log")).unwrap_or_default();
        let start = log.len().saturating_sub(4000);
        String::from_utf8_lossy(&log[start..]).into_owned()
    }

    /// Unloads the database, so the next read loads from blob and WAL.
    pub fn evict(&self) -> Result<(), String> {
        let response = self.request("POST", "/.local/evict", &[], Some(b""))?;
        if response.status != 204 {
            return Err(format!("evict: {} {}", response.status, response.text()));
        }
        sleep(EVICT_SETTLE);
        self.wait_ready()
    }

    fn stop_server(&mut self) -> Result<(), String> {
        let Some(mut child) = self.server.take() else {
            return Ok(());
        };
        unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
        let status = child.wait().map_err(|e| e.to_string())?;
        if !status.success() {
            return Err(format!("lark-server exited with {status}"));
        }
        Ok(())
    }

    /// Stops the server cleanly and starts it again. If it's already dead, this
    /// just starts it.
    pub fn restart(&mut self) -> Result<(), String> {
        self.stop_server()?;
        self.spawn_server()?;
        self.wait_ready()
    }

    /// Runs a full compaction with the server stopped.
    pub fn full_compact(&mut self) -> Result<(), String> {
        self.stop_server()?;
        let db_dir = self.database_dir();
        let output = Command::new(&self.binaries.compact)
            .arg(&db_dir)
            .output()
            .map_err(|e| format!("run lark-compact: {e}"))?;
        if !output.status.success() {
            return Err(format!(
                "lark-compact: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        self.spawn_server()?;
        self.wait_ready()
    }

    /// Forces a server back up after a crash, ignoring how it died.
    pub fn recover(&mut self) -> Result<(), String> {
        if let Some(mut child) = self.server.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        self.spawn_server()?;
        self.wait_ready()
    }

    fn database_dir(&self) -> PathBuf {
        Path::new(&self.data_dir).join(PROJECT).join(DATABASE)
    }
}

impl Drop for Stack {
    fn drop(&mut self) {
        for child in [self.edge.take(), self.server.take()].into_iter().flatten() {
            let mut child = child;
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
