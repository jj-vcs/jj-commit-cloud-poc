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
    spawn_server_with_args(&[]).await
}

fn find_server_bin() -> std::path::PathBuf {
    let current_exe = std::env::current_exe().expect("The current test executable path should be retrievable");
    let target_dir = current_exe
        .parent().expect("The deps directory should exist")
        .parent().expect("The target profile directory should exist");
    let direct = target_dir.join("jj-cc-server");
    if direct.exists() {
        return direct;
    }
    let poc_server = target_dir.parent().unwrap().join("jj-commit-cloud-poc/target/debug/jj-cc-server");
    if poc_server.exists() {
        return poc_server;
    }
    direct
}

fn find_jj_cmd() -> assert_cmd::Command {
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_jj") {
        return assert_cmd::Command::new(path);
    }
    let current_exe = std::env::current_exe().unwrap();
    let target_dir = current_exe.parent().unwrap().parent().unwrap();
    let direct = target_dir.join("jj");
    if direct.exists() {
        return assert_cmd::Command::new(direct);
    }
    let poc_jj = target_dir.parent().unwrap().join("jj-commit-cloud-poc/target/debug/jj");
    if poc_jj.exists() {
        return assert_cmd::Command::new(poc_jj);
    }
    let fallback = std::path::PathBuf::from("/usr/local/google/home/srachaba/Projects/jj-commit-cloud-poc/target/debug/jj");
    if fallback.exists() {
        return assert_cmd::Command::new(fallback);
    }
    assert_cmd::Command::cargo_bin("jj").expect("The jj CLI binary should have compiled")
}

pub async fn spawn_server_with_args(extra_args: &[&str]) -> ServerGuard {
    let server_binary_path = find_server_bin();

    let mut cmd = Command::new(server_binary_path);
    cmd.arg("--port=0");
    for arg in extra_args {
        cmd.arg(arg);
    }

    let mut child = cmd
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

pub async fn spawn_sqlite_server(db_path: &std::path::Path) -> ServerGuard {
    let sqlite_arg = format!("--sqlite-path={}", db_path.display());
    spawn_server_with_args(&["--store-type=sqlite", &sqlite_arg]).await
}

/// Struct to hold the jj-cc-server instance and the temporary test directory where changes are made to the working directory for cli integration tests
pub struct TestWorkspace {
    server: ServerGuard,
    temp_dir: tempfile::TempDir,
    _db_dir: Option<tempfile::TempDir>,
    db_path: Option<std::path::PathBuf>,
}

impl TestWorkspace {
    /// Spawns a new dynamic-port `jj-cc-server` instance and initializes a temporary 
    /// Commit Cloud repository workspace using `jj cc init`.
    pub async fn init() -> Self {
        let server = spawn_server().await;
        let temp_dir = tempfile::tempdir().expect("temporary directory should have been created for testing");
        let repo_path = temp_dir.path();

        let mut init_cmd = find_jj_cmd();

        init_cmd
            .current_dir(repo_path)
            .env("JJ_USER", "Test User")
            .env("JJ_EMAIL", "test.user@example.com")
            .args([
                "cc",
                "init",
                "--server",
                server.url(),
                "--create",
                ".",
            ]);

        init_cmd.assert().success();

        TestWorkspace {
            server,
            temp_dir,
            _db_dir: None,
            db_path: None,
        }
    }

    pub async fn init_sqlite() -> Self {
        let db_dir = tempfile::tempdir().expect("temporary directory should have been created for sqlite db");
        let db_path = db_dir.path().join("commit_cloud.db");
        let server = spawn_sqlite_server(&db_path).await;
        let temp_dir = tempfile::tempdir().expect("temporary directory should have been created for testing");
        let repo_path = temp_dir.path();

        let mut init_cmd = find_jj_cmd();

        init_cmd
            .current_dir(repo_path)
            .env("JJ_USER", "Test User")
            .env("JJ_EMAIL", "test.user@example.com")
            .args([
                "cc",
                "init",
                "--server",
                server.url(),
                "--create",
                ".",
            ]);

        init_cmd.assert().success();

        TestWorkspace {
            server,
            temp_dir,
            _db_dir: Some(db_dir),
            db_path: Some(db_path),
        }
    }

    pub fn db_path(&self) -> &std::path::Path {
        self.db_path
            .as_deref()
            .expect("db_path should only be called on workspaces initialized with SQLite")
    }

    pub fn repo_path(&self) -> &std::path::Path {
        self.temp_dir.path()
    }

    pub fn server_url(&self) -> &str {
        self.server.url()
    }

    pub fn jj_cmd(&self) -> assert_cmd::Command {
        let mut cmd = find_jj_cmd();
        cmd.current_dir(self.repo_path());
        cmd.env("JJ_USER", "Test User");
        cmd.env("JJ_EMAIL", "test.user@example.com");
        cmd
    }

    pub async fn mount_vfs(&self) -> VfsMountGuard {
        spawn_vfs_mount(self.repo_path()).await
    }
}

pub struct VfsMountGuard {
    child: Option<tokio::process::Child>,
    mount_dir: tempfile::TempDir,
}

impl VfsMountGuard {
    pub fn mountpoint(&self) -> &std::path::Path {
        self.mount_dir.path()
    }
}

impl Drop for VfsMountGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.start_kill();
        }
        #[cfg(target_os = "linux")]
        {
            let _ = std::process::Command::new("fusermount")
                .arg("-u")
                .arg(self.mount_dir.path())
                .status();
        }
    }
}

fn find_jjfsd_bin() -> std::path::PathBuf {
    let current_exe = std::env::current_exe().unwrap_or_default();
    let target_dir = current_exe
        .parent()
        .unwrap_or(std::path::Path::new(""))
        .parent()
        .unwrap_or(std::path::Path::new(""));
    let direct = target_dir.join("jjfsd");
    if direct.exists() {
        return direct;
    }
    let poc_vfs = std::path::PathBuf::from("/usr/local/google/home/srachaba/Projects/jj-vfs-poc/target/debug/jjfsd");
    if poc_vfs.exists() {
        return poc_vfs;
    }
    direct
}

pub async fn spawn_vfs_mount(workspace_path: &std::path::Path) -> VfsMountGuard {
    let mount_dir = tempfile::tempdir().expect("Failed to create tempdir for vfs mount");
    let vfs_bin = find_jjfsd_bin();
    let mut cmd = tokio::process::Command::new(vfs_bin);
    cmd.arg(mount_dir.path());
    cmd.arg(workspace_path);
    cmd.env("JJ_USER", "Test User");
    cmd.env("JJ_EMAIL", "test.user@example.com");
    let mut child = cmd.spawn().expect("Failed to spawn jjfsd");

    let commits_dir = mount_dir.path().join("commits");
    let mut ready = false;
    for _ in 0..50 {
        if commits_dir.exists() {
            ready = true;
            break;
        }
        if let Ok(Some(status)) = child.try_wait() {
            panic!("jjfsd exited prematurely with status: {:?}", status);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(ready, "VFS mount point did not become ready in time");

    VfsMountGuard {
        child: Some(child),
        mount_dir,
    }
}
