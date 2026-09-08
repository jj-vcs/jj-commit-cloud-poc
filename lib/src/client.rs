use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use hyper_util::rt::TokioIo;
use crate::util::CommitCloudConfig;

pub fn get_default_daemon_socket(server_url: &str) -> PathBuf {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    server_url.hash(&mut hasher);
    let hash = hasher.finish();

    if let Ok(user) = std::env::var("USER") {
        PathBuf::from(format!("/tmp/jj-cc-daemon-{user}-{hash:016x}.sock"))
    } else {
        PathBuf::from(format!("/tmp/jj-cc-daemon-{hash:016x}.sock"))
    }
}

pub fn find_daemon_binary() -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    if let Ok(path) = std::env::var("JJ_CC_DAEMON_BIN") {
        let p = PathBuf::from(path);
        if p.exists() {
            return Ok(p);
        }
    }
    if let Ok(current_exe) = std::env::current_exe() {
        if let Some(parent) = current_exe.parent() {
            let candidate = parent.join("jj-cc-daemon");
            if candidate.exists() {
                return Ok(candidate);
            }
            if let Some(profile_dir) = parent.parent() {
                let candidate2 = profile_dir.join("jj-cc-daemon");
                if candidate2.exists() {
                    return Ok(candidate2);
                }
            }
        }
    }
    if let Ok(path_var) = std::env::var("PATH") {
        for dir in std::env::split_paths(&path_var) {
            let candidate = dir.join("jj-cc-daemon");
            if candidate.is_file() {
                return Ok(candidate);
            }
        }
    }
    let fallback = PathBuf::from("/usr/local/google/home/srachaba/Projects/jj-commit-cloud-poc/target/debug/jj-cc-daemon");
    if fallback.exists() {
        return Ok(fallback);
    }
    Err("Could not locate 'jj-cc-daemon' binary. Ensure it is built and in PATH.".into())
}

pub async fn spawn_daemon(
    server_url: &str,
    socket_path: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if socket_path.exists() {
        let _ = std::fs::remove_file(socket_path);
    }

    if let Some(parent) = socket_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    let daemon_bin = find_daemon_binary()?;

    let mut cmd = tokio::process::Command::new(daemon_bin);
    cmd.arg("--server").arg(server_url);
    cmd.arg("--socket").arg(socket_path);
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::null());

    #[cfg(unix)]
    {
        cmd.process_group(0);
    }

    let mut child = cmd.spawn()?;

    // Wait until daemon process binds to the socket
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(3);
    while start.elapsed() < timeout {
        if let Ok(Some(status)) = child.try_wait() {
            return Err(format!("Daemon exited prematurely with status: {:?}", status).into());
        }
        if UnixStream::connect(socket_path).await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    Err(format!("Timed out waiting for daemon to start at {}", socket_path.display()).into())
}

pub async fn connect_channel(
    config: &CommitCloudConfig,
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    if config.use_daemon {
        let socket_path = config
            .daemon_socket
            .as_deref()
            .map(PathBuf::from)
            .unwrap_or_else(|| get_default_daemon_socket(&config.server_url));

        // Directly attempt to connect to the daemon socket. If it fails, spawn daemon synchronously.
        if UnixStream::connect(&socket_path).await.is_err() {
            if let Err(e) = spawn_daemon(&config.server_url, &socket_path).await {
                eprintln!("Warning: failed to start daemon ({e}), falling back to direct gRPC");
                return Ok(Channel::from_shared(config.server_url.clone())?
                    .connect()
                    .await?);
            }
        }

        let path_clone = socket_path.clone();
        let channel = Endpoint::try_from("http://[::]:50051")?
            .connect_with_connector(service_fn(move |_: Uri| {
                let path = path_clone.clone();
                async move {
                    let stream = UnixStream::connect(path).await?;
                    Ok::<_, std::io::Error>(TokioIo::new(stream))
                }
            }))
            .await?;
        Ok(channel)
    } else {
        Ok(Channel::from_shared(config.server_url.clone())?
            .connect()
            .await?)
    }
}

pub async fn connect_backend_client(
    config: &CommitCloudConfig,
) -> Result<cc_common::backend::backend_service_client::BackendServiceClient<Channel>, Box<dyn std::error::Error + Send + Sync>> {
    let channel = connect_channel(config).await?;
    Ok(cc_common::backend::backend_service_client::BackendServiceClient::new(channel))
}

pub async fn connect_op_store_client(
    config: &CommitCloudConfig,
) -> Result<cc_common::op_store::op_store_service_client::OpStoreServiceClient<Channel>, Box<dyn std::error::Error + Send + Sync>> {
    let channel = connect_channel(config).await?;
    Ok(cc_common::op_store::op_store_service_client::OpStoreServiceClient::new(channel))
}

pub async fn connect_workspace_client(
    config: &CommitCloudConfig,
) -> Result<cc_common::workspace::workspace_service_client::WorkspaceServiceClient<Channel>, Box<dyn std::error::Error + Send + Sync>> {
    let channel = connect_channel(config).await?;
    Ok(cc_common::workspace::workspace_service_client::WorkspaceServiceClient::new(channel))
}
