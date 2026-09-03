use clap::{Parser, ValueEnum};
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use cc_common::backend::backend_service_server::BackendServiceServer;
use cc_common::op_store::op_store_service_server::OpStoreServiceServer;

mod backend;
pub mod error_util;
mod hash_utils;
pub mod jj_lib_adapters;
mod op_store;
pub mod store;

use backend::CommitCloudBackendService;
use op_store::CommitCloudOpStoreService;
use store::{MemoryStore, Store};

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

pub fn get_default_sqlite_path_from_home(
    home: Option<&std::path::Path>,
) -> Result<std::path::PathBuf, std::io::Error> {
    if let Some(home_dir) = home {
        let dir = home_dir.join(".jj-cc-server");
        std::fs::create_dir_all(&dir).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!(
                    "Failed to create default SQLite directory '{}': {}",
                    dir.display(),
                    e
                ),
            )
        })?;
        Ok(dir.join("commit_cloud.db"))
    } else {
        warn!("HOME environment variable is not set; using local directory for SQLite database");
        Ok(std::path::PathBuf::from("commit_cloud.db"))
    }
}

pub fn get_default_sqlite_path() -> Result<std::path::PathBuf, std::io::Error> {
    let home = std::env::var_os("HOME").map(std::path::PathBuf::from);
    get_default_sqlite_path_from_home(home.as_deref())
}

pub fn resolve_sqlite_path(
    custom_path: Option<std::path::PathBuf>,
) -> Result<std::path::PathBuf, std::io::Error> {
    if let Some(path) = custom_path {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    std::io::Error::new(
                        e.kind(),
                        format!(
                            "Failed to create parent directory '{}' for SQLite database: {}",
                            parent.display(),
                            e
                        ),
                    )
                })?;
            }
        }
        Ok(path)
    } else {
        get_default_sqlite_path()
    }
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
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
            let path = resolve_sqlite_path(args.sqlite_path)?;
            info!("Opening SQLite Database Store at '{}'", path.display());
            Arc::new(store::SqliteStore::open(&path).map_err(|e| {
                format!(
                    "Failed to open SQLite database at '{}': {}",
                    path.display(),
                    e
                )
            })?)
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

    Server::builder()
        .add_service(health_service)
        .add_service(BackendServiceServer::new(backend_service))
        .add_service(OpStoreServiceServer::new(op_store_service))
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
    use tempfile::tempdir;

    #[test]
    fn test_default_sqlite_path_happy_path() {
        let temp_dir = tempdir().unwrap();
        let path = get_default_sqlite_path_from_home(Some(temp_dir.path())).unwrap();
        assert!(path.ends_with("commit_cloud.db"));
        assert!(temp_dir.path().join(".jj-cc-server").is_dir());
    }

    #[test]
    fn test_default_sqlite_path_without_home() {
        let path = get_default_sqlite_path_from_home(None).unwrap();
        assert_eq!(path, std::path::PathBuf::from("commit_cloud.db"));
    }

    #[test]
    fn test_default_sqlite_path_failure_when_dir_creation_fails() {
        let temp_dir = tempdir().unwrap();
        let blocker_file = temp_dir.path().join(".jj-cc-server");
        // Create a regular file where the directory should be, causing create_dir_all to fail
        std::fs::write(&blocker_file, "blocking file").unwrap();

        let result = get_default_sqlite_path_from_home(Some(temp_dir.path()));
        assert!(result.is_err());
        let err = result.unwrap_err();
        let expected_io_err = std::fs::create_dir_all(&blocker_file).unwrap_err();
        let expected_msg = format!(
            "Failed to create default SQLite directory '{}': {}",
            blocker_file.display(),
            expected_io_err
        );
        assert_eq!(err.to_string(), expected_msg);
    }

    #[test]
    fn test_resolve_sqlite_path_custom_happy_path() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("nested").join("custom.db");
        let resolved = resolve_sqlite_path(Some(db_path.clone())).unwrap();
        assert_eq!(resolved, db_path);
        assert!(temp_dir.path().join("nested").is_dir());
    }

    #[test]
    fn test_resolve_sqlite_path_custom_failure() {
        let temp_dir = tempdir().unwrap();
        let blocker_file = temp_dir.path().join("nested");
        std::fs::write(&blocker_file, "blocking file").unwrap();

        let db_path = blocker_file.join("custom.db");
        let result = resolve_sqlite_path(Some(db_path));
        assert!(result.is_err());
        let err = result.unwrap_err();
        let expected_io_err = std::fs::create_dir_all(&blocker_file).unwrap_err();
        let expected_msg = format!(
            "Failed to create parent directory '{}' for SQLite database: {}",
            blocker_file.display(),
            expected_io_err
        );
        assert_eq!(err.to_string(), expected_msg);
    }
}
