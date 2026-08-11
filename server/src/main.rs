use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use cc_common::backend::backend_service_server::BackendServiceServer;
use cc_common::op_store::op_store_service_server::OpStoreServiceServer;

mod backend;
mod hash_utils;
mod op_store;
pub mod store;

use backend::CommitCloudBackendService;
use op_store::CommitCloudOpStoreService;
use store::{MemoryStore, Store};

#[derive(Parser, Debug)]
#[command(name = "jj-cc-server", about = "Jujutsu Commit Cloud Server")]
struct Args {
    #[arg(short, long, default_value_t = 50051)]
    port: u16,
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
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,jj_commit_cloud_server=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = Args::parse();
    let addr: SocketAddr = format!("[::1]:{}", args.port).parse()?;

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<BackendServiceServer<CommitCloudBackendService>>()
        .await;
    health_reporter
        .set_serving::<OpStoreServiceServer<CommitCloudOpStoreService>>()
        .await;

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    info!("jj-cc-server listening on {}", local_addr);

    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let server_impl = CommitCloudServerImpl::default();
    let backend_service = CommitCloudBackendService::new(server_impl.store.clone());
    let op_store_service = CommitCloudOpStoreService::new(server_impl.store.clone());

    Server::builder()
        .add_service(health_service)
        .add_service(BackendServiceServer::new(backend_service))
        .add_service(OpStoreServiceServer::new(op_store_service))
        .serve_with_incoming_shutdown(incoming, async {
            match tokio::signal::ctrl_c().await {
                Ok(()) => {
                    info!("Received shutdown signal, terminating server");
                }
                Err(err) => {
                    warn!("Error listening for shutdown signal: {}", err);
                }
            }
        })
        .await?;

    Ok(())
}
