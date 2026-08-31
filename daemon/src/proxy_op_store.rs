use crate::connection::RemoteConnectionManager;
use cc_common::op_store::op_store_service_server::OpStoreService;
use cc_common::op_store::*;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct DaemonOpStoreProxy {
    conn_manager: RemoteConnectionManager,
}

impl DaemonOpStoreProxy {
    pub fn new(conn_manager: RemoteConnectionManager) -> Self {
        Self { conn_manager }
    }
}

#[tonic::async_trait]
impl OpStoreService for DaemonOpStoreProxy {
    async fn read_operation(
        &self,
        request: Request<ReadOperationRequest>,
    ) -> Result<Response<ReadOperationResponse>, Status> {
        self.conn_manager
            .with_op_store(|mut c| async move { c.read_operation(request).await })
            .await
    }

    async fn write_operation(
        &self,
        request: Request<WriteOperationRequest>,
    ) -> Result<Response<WriteOperationResponse>, Status> {
        self.conn_manager
            .with_op_store(|mut c| async move { c.write_operation(request).await })
            .await
    }

    async fn read_view(
        &self,
        request: Request<ReadViewRequest>,
    ) -> Result<Response<ReadViewResponse>, Status> {
        self.conn_manager
            .with_op_store(|mut c| async move { c.read_view(request).await })
            .await
    }

    async fn write_view(
        &self,
        request: Request<WriteViewRequest>,
    ) -> Result<Response<WriteViewResponse>, Status> {
        self.conn_manager
            .with_op_store(|mut c| async move { c.write_view(request).await })
            .await
    }

    async fn get_op_heads(
        &self,
        request: Request<GetOpHeadsRequest>,
    ) -> Result<Response<GetOpHeadsResponse>, Status> {
        self.conn_manager
            .with_op_store(|mut c| async move { c.get_op_heads(request).await })
            .await
    }

    async fn update_op_heads(
        &self,
        request: Request<UpdateOpHeadsRequest>,
    ) -> Result<Response<UpdateOpHeadsResponse>, Status> {
        self.conn_manager
            .with_op_store(|mut c| async move { c.update_op_heads(request).await })
            .await
    }
}
