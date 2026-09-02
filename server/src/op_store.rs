use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

use cc_common::op_store::op_store_service_server::OpStoreService;
use cc_common::op_store::*;

use crate::error_util::ensure_repo_registered_error;
use crate::hash_utils::{hash_operation, hash_view};
use crate::store::Store;

#[derive(Clone)]
pub struct CommitCloudOpStoreService {
    store: Arc<dyn Store>,
}

impl CommitCloudOpStoreService {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl OpStoreService for CommitCloudOpStoreService {
    async fn read_operation(
        &self,
        request: tonic::Request<ReadOperationRequest>,
    ) -> Result<tonic::Response<ReadOperationResponse>, tonic::Status> {
        let req = request.into_inner();
        info!(
            "Reading operation: {} for repo: {}",
            hex::encode(&req.operation_id),
            req.repo_id
        );

        ensure_repo_registered_error(self.store.as_ref(), &req.repo_id, "reading operations")
            .await?;

        if let Some(op) = self
            .store
            .get_operation(&req.repo_id, &req.operation_id)
            .await?
        {
            return Ok(tonic::Response::new(ReadOperationResponse {
                operation: Some(op),
            }));
        }

        if req.operation_id == cc_common::ROOT_OPERATION_ID_BYTES {
            let root_op = Operation {
                view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
                parents: vec![],
                metadata: Some(OperationMetadata {
                    start_time_millis: 0,
                    end_time_millis: 0,
                    description: "root()".to_string(),
                    hostname: "".to_string(),
                    username: "".to_string(),
                    is_snapshot: false,
                    workspace_name: None,
                    attributes: HashMap::new(),
                }),
                commit_predecessors: vec![],
                commit_predecessors_set: true,
            };
            return Ok(tonic::Response::new(ReadOperationResponse {
                operation: Some(root_op),
            }));
        }

        Err(tonic::Status::not_found(format!(
            "operation should have been present in cloud database: {} in repo: {}",
            hex::encode(&req.operation_id),
            req.repo_id
        )))
    }

    async fn write_operation(
        &self,
        request: tonic::Request<WriteOperationRequest>,
    ) -> Result<tonic::Response<WriteOperationResponse>, tonic::Status> {
        let req = request.into_inner();
        let op = req.operation.ok_or_else(|| {
            tonic::Status::invalid_argument("request should have contained an operation object")
        })?;

        info!("Writing operation for repo: {}", req.repo_id);

        ensure_repo_registered_error(self.store.as_ref(), &req.repo_id, "writing operations")
            .await?;

        let op_id = hash_operation(&op);

        self.store
            .put_operation(req.repo_id, op_id.clone(), op)
            .await?;

        Ok(tonic::Response::new(WriteOperationResponse {
            operation_id: op_id,
        }))
    }

    async fn read_view(
        &self,
        request: tonic::Request<ReadViewRequest>,
    ) -> Result<tonic::Response<ReadViewResponse>, tonic::Status> {
        let req = request.into_inner();
        info!(
            "Reading view: {} for repo: {}",
            hex::encode(&req.view_id),
            req.repo_id
        );

        ensure_repo_registered_error(self.store.as_ref(), &req.repo_id, "reading views").await?;

        if let Some(v) = self.store.get_view(&req.repo_id, &req.view_id).await? {
            return Ok(tonic::Response::new(ReadViewResponse {
                view_id: req.view_id.clone(),
                view: Some(v),
            }));
        }

        if req.view_id == cc_common::ROOT_VIEW_ID_BYTES {
            let root_view = View {
                head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                wc_commit_ids: HashMap::new(),
                local_bookmarks: HashMap::new(),
                remote_bookmarks: HashMap::new(),
            };
            return Ok(tonic::Response::new(ReadViewResponse {
                view_id: req.view_id.clone(),
                view: Some(root_view),
            }));
        }

        Err(tonic::Status::not_found(format!(
            "view should have been present in cloud database: {} in repo: {}",
            hex::encode(&req.view_id),
            req.repo_id
        )))
    }

    async fn write_view(
        &self,
        request: tonic::Request<WriteViewRequest>,
    ) -> Result<tonic::Response<WriteViewResponse>, tonic::Status> {
        let req = request.into_inner();
        let view = req.view.ok_or_else(|| {
            tonic::Status::invalid_argument("request should have contained a view object")
        })?;

        info!("Writing view for repo: {}", req.repo_id);

        ensure_repo_registered_error(self.store.as_ref(), &req.repo_id, "writing views").await?;

        let view_id = hash_view(&view);

        self.store
            .put_view(req.repo_id, view_id.clone(), view)
            .await?;

        Ok(tonic::Response::new(WriteViewResponse { view_id }))
    }

    async fn get_op_heads(
        &self,
        request: tonic::Request<GetOpHeadsRequest>,
    ) -> Result<tonic::Response<GetOpHeadsResponse>, tonic::Status> {
        let req = request.into_inner();
        info!("Get op heads for repo: {}", req.repo_id);

        ensure_repo_registered_error(self.store.as_ref(), &req.repo_id, "requesting op heads")
            .await?;

        let heads = self
            .store
            .get_op_heads(&req.repo_id)
            .await?
            .unwrap_or_else(|| vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()]);

        Ok(tonic::Response::new(GetOpHeadsResponse {
            op_head_ids: heads,
        }))
    }

    async fn update_op_heads(
        &self,
        request: tonic::Request<UpdateOpHeadsRequest>,
    ) -> Result<tonic::Response<UpdateOpHeadsResponse>, tonic::Status> {
        let req = request.into_inner();
        info!("Update op heads for repo: {}", req.repo_id);

        ensure_repo_registered_error(self.store.as_ref(), &req.repo_id, "updating op heads")
            .await?;

        let current_heads = self
            .store
            .update_op_heads(req.repo_id, &req.old_op_head_ids, req.new_op_head_id)
            .await?;

        Ok(tonic::Response::new(UpdateOpHeadsResponse {
            current_op_head_ids: current_heads,
        }))
    }
}
