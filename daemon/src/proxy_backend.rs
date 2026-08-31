use crate::connection::RemoteConnectionManager;
use cc_common::backend::backend_service_server::BackendService;
use cc_common::backend::*;
use tonic::{Request, Response, Status, Streaming};

#[derive(Clone)]
pub struct DaemonBackendProxy {
    conn_manager: RemoteConnectionManager,
}

impl DaemonBackendProxy {
    pub fn new(conn_manager: RemoteConnectionManager) -> Self {
        Self { conn_manager }
    }
}

#[tonic::async_trait]
impl BackendService for DaemonBackendProxy {
    async fn register_repository(
        &self,
        request: Request<RegisterRepositoryRequest>,
    ) -> Result<Response<RegisterRepositoryResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.register_repository(request).await })
            .await
    }

    async fn read_commit(
        &self,
        request: Request<ReadCommitRequest>,
    ) -> Result<Response<ReadCommitResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.read_commit(request).await })
            .await
    }

    async fn write_commit(
        &self,
        request: Request<WriteCommitRequest>,
    ) -> Result<Response<WriteCommitResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.write_commit(request).await })
            .await
    }

    async fn read_tree(
        &self,
        request: Request<ReadTreeRequest>,
    ) -> Result<Response<ReadTreeResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.read_tree(request).await })
            .await
    }

    async fn write_tree(
        &self,
        request: Request<WriteTreeRequest>,
    ) -> Result<Response<WriteTreeResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.write_tree(request).await })
            .await
    }

    type ReadFileStream = Streaming<ReadFileResponse>;

    async fn read_file(
        &self,
        request: Request<ReadFileRequest>,
    ) -> Result<Response<Self::ReadFileStream>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.read_file(request).await })
            .await
    }

    async fn write_file(
        &self,
        request: Request<WriteFileRequest>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.write_file(request).await })
            .await
    }

    async fn read_symlink(
        &self,
        request: Request<ReadSymlinkRequest>,
    ) -> Result<Response<ReadSymlinkResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.read_symlink(request).await })
            .await
    }

    async fn write_symlink(
        &self,
        request: Request<WriteSymlinkRequest>,
    ) -> Result<Response<WriteSymlinkResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.write_symlink(request).await })
            .await
    }
}
