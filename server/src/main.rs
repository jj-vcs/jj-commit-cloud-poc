use clap::{Parser, ValueEnum};
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use cc_common::backend::backend_service_server::BackendServiceServer;
use cc_common::op_store::op_store_service_server::OpStoreServiceServer;
use cc_common::workspace::workspace_service_server::WorkspaceServiceServer;

mod backend;
pub mod error_util;
mod hash_utils;
mod op_store;
pub mod store;
mod workspace;

use backend::CommitCloudBackendService;
use op_store::CommitCloudOpStoreService;
use store::{MemoryStore, Store};
use workspace::CommitCloudWorkspaceService;

// Storage backend to run the server against.
#[derive(Copy, Clone, Debug, ValueEnum)]
enum StoreType {
    Memory,
    Sqlite,
}

#[derive(Parser, Debug)]
#[command(name = "jj-cc-server", about = "Jujutsu Commit Cloud Server")]
struct Args {
    /// Host interface to bind to
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Port to listen on (use 0 for ephemeral port assignment)
    #[arg(short, long, default_value_t = 8080)]
    port: u16,

    /// Storage backend type
    #[arg(long, value_enum, default_value_t = StoreType::Memory)]
    store_type: StoreType,

    /// Path to SQLite database file (defaults to ~/.jj-cc-server/commit_cloud.db)
    #[arg(long)]
    sqlite_path: Option<std::path::PathBuf>,
}

pub struct CommitCloudServerImpl {
    pub store: Arc<dyn Store>,
}

impl Default for CommitCloudServerImpl {
    fn default() -> Self {
        Self {
            store: Arc::new(MemoryStore::default()),
        }
    }
}

fn get_default_sqlite_path() -> std::path::PathBuf {
    if let Some(home) = std::env::var_os("HOME") {
        let dir = std::path::PathBuf::from(home).join(".jj-cc-server");
        let _ = std::fs::create_dir_all(&dir);
        dir.join("commit_cloud.db")
    } else {
        std::path::PathBuf::from("commit_cloud.db")
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("jj_cc_server=info".parse()?),
        )
        .init();

    let args = Args::parse();
    let store: Arc<dyn Store> = match args.store_type {
        StoreType::Memory => Arc::new(MemoryStore::default()),
        StoreType::Sqlite => {
            let path = args.sqlite_path.unwrap_or_else(get_default_sqlite_path);
            info!("Opening SQLite Database Store at '{}'", path.display());
            Arc::new(store::SqliteStore::open(path)?)
        }
    };

    let addr: SocketAddr = format!("{}:{}", args.host, args.port).parse()?;

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_service_status("", tonic_health::ServingStatus::Serving)
        .await;
    health_reporter
        .set_serving::<OpStoreServiceServer<CommitCloudOpStoreService>>()
        .await;

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    info!("jj-cc-server listening on {}", local_addr);

    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let backend_service = CommitCloudBackendService::new(store.clone());
    let op_store_service = CommitCloudOpStoreService::new(store.clone());
    let workspace_service = CommitCloudWorkspaceService::new(store.clone());

    Server::builder()
        .add_service(health_service)
        .add_service(BackendServiceServer::new(backend_service))
        .add_service(OpStoreServiceServer::new(op_store_service))
        .add_service(WorkspaceServiceServer::new(workspace_service))
        .serve_with_incoming_shutdown(incoming, async {
            match tokio::signal::ctrl_c().await {
                Ok(()) => {
                    info!("Shutdown signal received, shutting down gracefully...");
                }
                Err(e) => {
                    warn!(
                        "Failed to listen for shutdown signal: {}, shutting down...",
                        e
                    );
                }
            }
        })
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_sqlite_path() {
        let path = get_default_sqlite_path();
        assert!(path.ends_with("commit_cloud.db"));
    }
}
