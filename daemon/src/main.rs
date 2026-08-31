use clap::Parser;
use std::fs;
use std::path::PathBuf;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;
use tracing::info;

mod connection;
mod proxy_backend;
mod proxy_op_store;
mod proxy_workspace;

use connection::RemoteConnectionManager;
use proxy_backend::DaemonBackendProxy;
use proxy_op_store::DaemonOpStoreProxy;
use proxy_workspace::DaemonWorkspaceProxy;

use cc_common::backend::backend_service_server::BackendServiceServer;
use cc_common::op_store::op_store_service_server::OpStoreServiceServer;
use cc_common::workspace::workspace_service_server::WorkspaceServiceServer;

#[derive(Parser, Debug)]
#[command(name = "jj-cc-daemon", about = "Commit Cloud Local Daemon")]
struct Args {
    /// Remote Commit Cloud server address (e.g. http://127.0.0.1:8080)
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    server: String,

    /// Unix domain socket path for local IPC
    #[arg(long)]
    socket: PathBuf,

    /// Optional idle timeout in seconds before daemon exits
    #[arg(long)]
    idle_timeout: Option<u64>,
}

struct SocketGuard {
    path: PathBuf,
}

impl SocketGuard {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Drop for SocketGuard {
    fn drop(&mut self) {
        if self.path.exists() {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    if let Some(parent) = args.socket.parent() {
        fs::create_dir_all(parent)?;
    }

    if args.socket.exists() {
        info!("Removing existing stale socket file at '{}'", args.socket.display());
        let _ = fs::remove_file(&args.socket);
    }

    let _socket_guard = SocketGuard::new(args.socket.clone());

    let listener = UnixListener::bind(&args.socket)?;
    info!(
        "Commit Cloud Daemon listening on Unix socket '{}', proxying to '{}'",
        args.socket.display(),
        args.server
    );

    let stream = UnixListenerStream::new(listener);

    let conn_manager = RemoteConnectionManager::new(args.server);

    let backend_proxy = DaemonBackendProxy::new(conn_manager.clone());
    let op_store_proxy = DaemonOpStoreProxy::new(conn_manager.clone());
    let workspace_proxy = DaemonWorkspaceProxy::new(conn_manager);

    let res = Server::builder()
        .add_service(BackendServiceServer::new(backend_proxy))
        .add_service(OpStoreServiceServer::new(op_store_proxy))
        .add_service(WorkspaceServiceServer::new(workspace_proxy))
        .serve_with_incoming_shutdown(stream, async {
            let _ = tokio::signal::ctrl_c().await;
            info!("Shutting down Commit Cloud Daemon...");
        })
        .await;

    if let Err(e) = res {
        eprintln!("Daemon server error: {e}");
        return Err(e.into());
    }

    Ok(())
}
