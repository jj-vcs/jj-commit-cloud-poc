use clap::Parser;
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
mod op_store;
pub mod store;

use backend::CommitCloudBackendService;
use op_store::CommitCloudOpStoreService;
use store::{MemoryStore, Store};

#[derive(Parser, Debug)]
#[command(name = "jj-cc-server", about = "Jujutsu Commit Cloud Server")]
struct Args {
    /// Host interface to bind to
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Port to listen on (use 0 for ephemeral port assignment)
    #[arg(short, long, default_value_t = 8080)]
    port: u16,

    /// Storage backend type: "memory", "sqlite", or "spanner"
    #[arg(long, default_value = "memory")]
    store_type: String,

    /// Path to SQLite database file (required if store-type is "sqlite")
    #[arg(long)]
    sqlite_path: Option<std::path::PathBuf>,

    /// Google Cloud Spanner database resource name (required if store-type is "spanner")
    #[arg(long)]
    spanner_database: Option<String>,
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
    let store: Arc<dyn Store> = match args.store_type.as_str() {
        "memory" => Arc::new(MemoryStore::default()),
        "sqlite" => {
            let path = args
                .sqlite_path
                .expect("--sqlite-path is required when --store-type=sqlite");
            Arc::new(store::SqliteStore::open(path)?)
        }
        "spanner" => {
            let db_name = args
                .spanner_database
                .as_deref()
                .unwrap_or("projects/test/instances/test/databases/test");
            Arc::new(store::SpannerStore::open_spanner(db_name)?)
        }
        _ => return Err(format!("Unknown store type: {}", args.store_type).into()),
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
