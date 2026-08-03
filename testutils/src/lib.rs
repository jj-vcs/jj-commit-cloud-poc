
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

pub type TestResult = Result<(), Box<dyn std::error::Error>>;

pub fn hermetic_git() {}

pub fn new_temp_dir() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

pub static HERMETIC_GIT_CONFIGS: &[(&str, &str)] = &[
    ("user.name", "Test User"),
    ("user.email", "test.user@example.com"),
];

pub struct ServerGuard {

    child: Option<tokio::process::Child>,
    url: String,
}

impl ServerGuard {
    pub fn url(&self) -> &str {
        &self.url
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.start_kill();
        }
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
    if let Ok(ext_url) = std::env::var("JJ_TEST_SERVER_URL") {
        if !ext_url.is_empty() {
            let mut url = ext_url;
            if !url.starts_with("http://") && !url.starts_with("https://") {
                url = format!("http://{}", url);
            }
            return ServerGuard {
                child: None,
                url,
            };
        }
    }

    let current_exe = std::env::current_exe().expect("The current test executable path should be retrievable");
    let server_binary_path = current_exe
        .parent().expect("The deps directory should exist")
        .parent().expect("The target profile directory should exist")
        .join("jj-cc-server");

    let mut child = Command::new(server_binary_path)
        .arg("--port=0")
        .arg("--db-backend=memory")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("The jj-cc-server process should have spawned");

    let stdout = child.stdout.take().expect("The stdout pipe should have been captured from the child process");

    let mut reader = BufReader::new(stdout).lines();

    let timeout = tokio::time::sleep(Duration::from_secs(10));

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

    ServerGuard {
        child: Some(child),
        url,
    }
}

