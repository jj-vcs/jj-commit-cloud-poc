mod db;

use cc_proto::backend::backend_service_server::{BackendService, BackendServiceServer};
use cc_proto::backend::*;
use cc_proto::op_heads_store::op_heads_store_service_server::{OpHeadsStoreService, OpHeadsStoreServiceServer};
use cc_proto::op_heads_store::*;
use cc_proto::op_store::op_store_service_server::{OpStoreService, OpStoreServiceServer};
use cc_proto::op_store::*;
use clap::{Parser, ValueEnum};
use db::{DatabaseStore, MemoryStore, SpannerStore, SqliteStore};
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(ValueEnum, Clone, Debug, Default, PartialEq, Eq)]
pub enum DbBackendType {
    Memory,
    #[default]
    Sqlite,
    Spanner,
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

    /// Database backend to use (sqlite, memory, spanner)
    #[arg(long, value_enum, default_value_t = DbBackendType::Sqlite)]
    db_backend: DbBackendType,

    /// Path to SQLite database file (defaults to ~/.jj-cc-server/commit_cloud.db)
    #[arg(long)]
    db_path: Option<String>,

    /// Full GCP Spanner database path (projects/<proj>/instances/<inst>/databases/<db>)
    #[arg(long, default_value = "projects/srachaba-jj-poc-sandbox/instances/jj-cc-spanner/databases/commit_cloud")]
    spanner_db: String,
}


fn get_default_db_path() -> String {
    if let Some(home) = std::env::var_os("HOME") {
        let dir = std::path::PathBuf::from(home).join(".jj-cc-server");
        let _ = std::fs::create_dir_all(&dir);
        dir.join("commit_cloud.db").to_string_lossy().to_string()
    } else {
        "commit_cloud.db".to_string()
    }
}


#[derive(Clone)]
pub struct CcBackendService {
    store: Arc<dyn DatabaseStore>,
}

#[tonic::async_trait]
impl BackendService for CcBackendService {
    async fn register_repository(
        &self,
        request: Request<RegisterRepositoryRequest>,
    ) -> Result<Response<RegisterRepositoryResponse>, Status> {
        let req = request.into_inner();
        let requested_id = if req.repo_id.is_empty() { None } else { Some(req.repo_id.as_str()) };
        let repo_id = self.store.register_repository(requested_id).await?;
        info!("Registering repository with repo_id '{}'", repo_id);
        Ok(Response::new(RegisterRepositoryResponse { repo_id }))
    }


    async fn read_commit(
        &self,
        request: Request<ReadCommitRequest>,
    ) -> Result<Response<ReadCommitResponse>, Status> {
        let req = request.into_inner();
        info!("Read commit for repo_id '{}', commit_id hex = {}", req.repo_id, hex::encode(&req.commit_id));
        let commit = self.store.read_commit(&req.repo_id, &req.commit_id).await?;
        if let Some(commit) = commit {
            Ok(Response::new(ReadCommitResponse { commit: Some(commit) }))
        } else {
            Err(Status::not_found(format!("Commit {:?} not found", req.commit_id)))
        }
    }


    async fn write_commit(
        &self,
        request: Request<WriteCommitRequest>,
    ) -> Result<Response<WriteCommitResponse>, Status> {
        let req = request.into_inner();
        if let Some(commit) = req.commit {
            let commit_id = self.store.write_commit(&req.repo_id, commit).await?;
            info!("Written commit for repo_id '{}', commit_id hex = {}", req.repo_id, hex::encode(&commit_id));
            Ok(Response::new(WriteCommitResponse { commit_id }))
        } else {
            Err(Status::invalid_argument("Commit object missing"))
        }
    }

    async fn read_tree(
        &self,
        request: Request<ReadTreeRequest>,
    ) -> Result<Response<ReadTreeResponse>, Status> {
        let req = request.into_inner();
        info!("Read tree for repo_id '{}', tree_id hex = {}", req.repo_id, hex::encode(&req.tree_id));
        let entries = self.store.read_tree(&req.repo_id, &req.tree_id).await?.unwrap_or_default();
        Ok(Response::new(ReadTreeResponse {
            tree_id: req.tree_id,
            entries,
        }))
    }


    async fn write_tree(
        &self,
        request: Request<WriteTreeRequest>,
    ) -> Result<Response<WriteTreeResponse>, Status> {
        let req = request.into_inner();
        let tree_id = self.store.write_tree(&req.repo_id, req.entries).await?;
        info!("Written tree for repo_id '{}', tree_id hex = {}", req.repo_id, hex::encode(&tree_id));
        Ok(Response::new(WriteTreeResponse { tree_id }))
    }

    type ReadFileStream = std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<ReadFileResponse, Status>> + Send>>;

    async fn read_file(
        &self,
        request: Request<ReadFileRequest>,
    ) -> Result<Response<Self::ReadFileStream>, Status> {
        let req = request.into_inner();
        info!("Read file for repo_id '{}', file_id hex = {}", req.repo_id, hex::encode(&req.file_id));
        let content = self.store.read_file(&req.repo_id, &req.file_id).await?;
        if let Some(bytes) = content {
            let chunk = ReadFileResponse { chunk: bytes };
            let stream = tokio_stream::once(Ok(chunk));
            Ok(Response::new(Box::pin(stream)))
        } else {
            Err(Status::not_found("File content not found"))
        }
    }


    async fn write_file(
        &self,
        request: Request<WriteFileRequest>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        let req = request.into_inner();
        let file_id = self.store.write_file(&req.repo_id, &req.content).await?;
        info!("Written file for repo_id '{}', file_id hex = {}", req.repo_id, hex::encode(&file_id));
        Ok(Response::new(WriteFileResponse { file_id }))
    }

    async fn read_symlink(
        &self,
        request: Request<ReadSymlinkRequest>,
    ) -> Result<Response<ReadSymlinkResponse>, Status> {
        let req = request.into_inner();
        let target = self.store.read_symlink(&req.repo_id, &req.symlink_id).await?;
        if let Some(target) = target {
            Ok(Response::new(ReadSymlinkResponse { target }))
        } else {
            Err(Status::not_found("Symlink target not found"))
        }
    }

    async fn write_symlink(
        &self,
        request: Request<WriteSymlinkRequest>,
    ) -> Result<Response<WriteSymlinkResponse>, Status> {
        let req = request.into_inner();
        let symlink_id = self.store.write_symlink(&req.repo_id, &req.target).await?;
        Ok(Response::new(WriteSymlinkResponse { symlink_id }))
    }
}

#[derive(Clone)]
pub struct CcOpStoreService {
    store: Arc<dyn DatabaseStore>,
}

#[tonic::async_trait]
impl OpStoreService for CcOpStoreService {
    async fn read_operation(
        &self,
        request: Request<ReadOperationRequest>,
    ) -> Result<Response<ReadOperationResponse>, Status> {
        let req = request.into_inner();
        info!("Read operation for repo_id '{}', op_id hex = {}", req.repo_id, hex::encode(&req.operation_id));
        let op = self.store.read_operation(&req.repo_id, &req.operation_id).await?;
        if let Some(op) = op {
            Ok(Response::new(ReadOperationResponse { operation: Some(op) }))
        } else {
            Err(Status::not_found("Operation not found"))
        }
    }


    async fn write_operation(
        &self,
        request: Request<WriteOperationRequest>,
    ) -> Result<Response<WriteOperationResponse>, Status> {
        let req = request.into_inner();
        if let Some(op) = req.operation {
            let operation_id = self.store.write_operation(&req.repo_id, op).await?;
            info!("Written operation for repo_id '{}', op_id hex = {}", req.repo_id, hex::encode(&operation_id));
            Ok(Response::new(WriteOperationResponse { operation_id }))
        } else {
            Err(Status::invalid_argument("Operation object missing"))
        }
    }

    async fn read_view(
        &self,
        request: Request<ReadViewRequest>,
    ) -> Result<Response<ReadViewResponse>, Status> {
        let req = request.into_inner();
        info!("Read view for repo_id '{}', view_id hex = {}", req.repo_id, hex::encode(&req.view_id));
        let view = self.store.read_view(&req.repo_id, &req.view_id).await?;
        if let Some(view) = view {
            Ok(Response::new(ReadViewResponse { view: Some(view) }))
        } else {
            Err(Status::not_found("View not found"))
        }
    }


    async fn write_view(
        &self,
        request: Request<WriteViewRequest>,
    ) -> Result<Response<WriteViewResponse>, Status> {
        let req = request.into_inner();
        if let Some(view) = req.view {
            let view_id = self.store.write_view(&req.repo_id, view).await?;
            info!("Written view for repo_id '{}', view_id hex = {}", req.repo_id, hex::encode(&view_id));
            Ok(Response::new(WriteViewResponse { view_id }))
        } else {
            Err(Status::invalid_argument("View object missing"))
        }
    }
}

#[derive(Clone)]
pub struct CcOpHeadsStoreService {
    store: Arc<dyn DatabaseStore>,
}

#[tonic::async_trait]
impl OpHeadsStoreService for CcOpHeadsStoreService {
    async fn get_op_heads(
        &self,
        request: Request<GetOpHeadsRequest>,
    ) -> Result<Response<GetOpHeadsResponse>, Status> {
        let req = request.into_inner();
        info!("Get op heads for repo_id '{}'", req.repo_id);
        let heads = self.store.get_op_heads(&req.repo_id).await?;
        Ok(Response::new(GetOpHeadsResponse { op_head_ids: heads }))
    }

    async fn add_op_head(
        &self,
        request: Request<AddOpHeadRequest>,
    ) -> Result<Response<AddOpHeadResponse>, Status> {
        let req = request.into_inner();
        info!("Add op head for repo_id '{}'", req.repo_id);
        let current_op_head_ids = self.store.add_op_head(&req.repo_id, &req.op_head_id).await?;
        Ok(Response::new(AddOpHeadResponse {
            success: true,
            current_op_head_ids,
        }))
    }


    async fn remove_op_head(
        &self,
        request: Request<RemoveOpHeadRequest>,
    ) -> Result<Response<RemoveOpHeadResponse>, Status> {
        let req = request.into_inner();
        let current_op_head_ids = self.store.remove_op_head(&req.repo_id, &req.op_head_id).await?;
        Ok(Response::new(RemoveOpHeadResponse {
            success: true,
            current_op_head_ids,
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(tracing_subscriber::EnvFilter::from_default_env().add_directive("jj_cc_server=info".parse()?))
        .init();


    let args = Args::parse();
    let addr: SocketAddr = format!("{}:{}", args.host, args.port).parse()?;

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_service_status("", tonic_health::ServingStatus::Serving)
        .await;

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;

    let store: Arc<dyn DatabaseStore> = match args.db_backend {
        DbBackendType::Memory => {
            info!("Initializing In-Memory Database Store");
            Arc::new(MemoryStore::new())
        }
        DbBackendType::Sqlite => {
            let path = args.db_path.unwrap_or_else(get_default_db_path);
            info!("Opening SQLite Database Store at '{}'", path);
            Arc::new(SqliteStore::open(&path)?)
        }
        DbBackendType::Spanner => {
            info!("Connecting to Google Cloud Spanner Store at '{}'", args.spanner_db);
            Arc::new(SpannerStore::connect(&args.spanner_db).await?)
        }
    };


    info!("jj-cc-server listening on {}", local_addr);
    println!("jj-cc-server listening on {}", local_addr);


    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let backend_service = CcBackendService { store: store.clone() };
    let op_store_service = CcOpStoreService { store: store.clone() };
    let op_heads_store_service = CcOpHeadsStoreService { store: store.clone() };

    Server::builder()
        .add_service(health_service)
        .add_service(BackendServiceServer::new(backend_service))
        .add_service(OpStoreServiceServer::new(op_store_service))
        .add_service(OpHeadsStoreServiceServer::new(op_heads_store_service))
        .serve_with_incoming_shutdown(incoming, async {
            match tokio::signal::ctrl_c().await {
                Ok(()) => {
                    info!("Shutdown signal received, shutting down gracefully...");
                }
                Err(e) => {
                    warn!("Failed to listen for shutdown signal: {}, shutting down...", e);
                }
            }
        })
        .await?;

    Ok(())
}
