use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

pub struct ServerGuard {
    child: tokio::process::Child,
    url: String,
}

impl ServerGuard {
    pub fn url(&self) -> &str {
        &self.url
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

fn extract_listening_address(line: &str) -> Option<String> {
    let marker = "listening on ";
    if let Some(idx) = line.find(marker) {
        let addr_part = &line[idx + marker.len()..];
        let addr = addr_part.trim().to_string();
        if !addr.is_empty() {
            return Some(addr);
        }
    }
    None
}

pub async fn spawn_server() -> ServerGuard {
    // Start the server with --port=0. Setting the port to 0 in tonic tells the OS 
    // to dynamically allocate any available ephemeral port, preventing port collisions.
    let current_exe = std::env::current_exe().expect("The current test executable path should be retrievable");
    let server_binary_path = current_exe
        .parent().expect("The deps directory should exist")
        .parent().expect("The target profile directory should exist")
        .join("jj-cc-server");

    let mut child = Command::new(server_binary_path)
        .arg("--port=0")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("The jj-cc-server process should have spawned");

    let stdout = child.stdout.take().expect("The stdout pipe should have been captured from the child process");

    let mut reader = BufReader::new(stdout).lines();

    // Timeout of 2 seconds to find the listening address in stdout. 
    // 2 seconds is arbitrary but serves as a safe defensive timeout to prevent 
    // the test from hanging indefinitely if the server hangs.
    let timeout = tokio::time::sleep(Duration::from_secs(2));
    tokio::pin!(timeout);

    let server_addr = loop {
        tokio::select! {
            line_result = reader.next_line() => {
                match line_result {
                    Ok(Some(line)) => {
                        if let Some(addr) = extract_listening_address(&line) {
                            break addr;
                        }
                    }
                    Ok(None) => panic!("The stdout pipe should not have closed before the server listening address was printed"),
                    Err(e) => panic!("A line from the server stdout should have been read successfully: {:?}", e),
                }
            }
            _ = &mut timeout => {
                panic!("The server listening address should have appeared in stdout before timeout");
            }
        }
    };

    let url = format!("http://{}", server_addr);

    ServerGuard { child, url }
}

/// Struct to hold the jj-cc-server instance and the temporary test directory where changes are made to the working directory for cli integration tests
pub struct TestWorkspace {
    server: ServerGuard,
    temp_dir: tempfile::TempDir,
}

impl TestWorkspace {
    /// Spawns a new dynamic-port `jj-cc-server` instance and initializes a temporary 
    /// Commit Cloud repository workspace using `jj cc init`.
    pub async fn init() -> Self {
        let server = spawn_server().await;
        let temp_dir = tempfile::tempdir().expect("temporary directory should have been created for testing");
        let repo_path = temp_dir.path();

        let mut init_cmd = assert_cmd::Command::cargo_bin("jj")
            .expect("The jj CLI binary should have compiled");

        init_cmd
            .current_dir(repo_path)
            .args([
                "cc",
                "init",
                "--server",
                server.url(),
                "--create",
                ".",
            ]);

        init_cmd.assert().success();

        TestWorkspace { server, temp_dir }
    }

    pub fn repo_path(&self) -> &std::path::Path {
        self.temp_dir.path()
    }

    pub fn server_url(&self) -> &str {
        self.server.url()
    }

    pub fn jj_cmd(&self) -> assert_cmd::Command {
        let mut cmd = assert_cmd::Command::cargo_bin("jj")
            .expect("The jj CLI binary should have compiled");
        cmd.current_dir(self.repo_path());
        cmd
    }
}
