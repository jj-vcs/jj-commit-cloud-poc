use async_trait::async_trait;
use cc_common::op_store::op_store_service_client::OpStoreServiceClient;
use cc_common::op_store::*;
use jj_lib::object_id::{HexPrefix, ObjectId, PrefixResolution};
use jj_lib::op_store::{OpStore, OpStoreResult, Operation, OperationId, View, ViewId};
use std::fmt::Debug;
use std::path::Path;
use std::time::SystemTime;

use crate::util::{CommitCloudConfig, run_async};

pub use cc_common::conversions::op_store::*;

#[derive(Debug)]
pub struct CommitCloudOpStore {
    server_url: String,
    repo_id: String,
    root_operation_id: OperationId,
}

impl CommitCloudOpStore {
    pub fn name() -> &'static str {
        "commit_cloud"
    }

    pub fn load(store_path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config = CommitCloudConfig::load_from_store(store_path)?;
        Ok(Self {
            server_url: config.server_url,
            repo_id: config.repo_id,
            root_operation_id: OperationId::from_bytes(&cc_common::ROOT_OPERATION_ID_BYTES),
        })
    }
}

#[async_trait]
impl OpStore for CommitCloudOpStore {
    fn name(&self) -> &str {
        Self::name()
    }

    fn root_operation_id(&self) -> &OperationId {
        &self.root_operation_id
    }

    async fn read_view(&self, id: &ViewId) -> OpStoreResult<View> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let view_id_bytes = id.as_bytes().to_vec();

        run_async(move || async move {
            let mut client = OpStoreServiceClient::connect(server_url).await?;
            let response = client
                .read_view(ReadViewRequest {
                    repo_id,
                    view_id: view_id_bytes,
                })
                .await?;
            let proto_view = response
                .into_inner()
                .view
                .ok_or_else(|| "server response should have contained a view".to_string())?;
            Ok(view_from_proto(proto_view))
        })
        .map_err(|e| jj_lib::op_store::OpStoreError::Other(e.into()))
    }

    async fn write_view(&self, contents: &View) -> OpStoreResult<ViewId> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let proto_view = view_to_proto(contents);

        run_async(move || async move {
            let mut client = OpStoreServiceClient::connect(server_url).await?;
            let response = client
                .write_view(WriteViewRequest {
                    repo_id,
                    view: Some(proto_view),
                })
                .await?;
            let view_id_bytes = response.into_inner().view_id;
            Ok(ViewId::from_bytes(&view_id_bytes))
        })
        .map_err(|e| jj_lib::op_store::OpStoreError::Other(e.into()))
    }

    async fn read_operation(&self, id: &OperationId) -> OpStoreResult<Operation> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let op_id_bytes = id.as_bytes().to_vec();

        run_async(move || async move {
            let mut client = OpStoreServiceClient::connect(server_url).await?;
            let response = client
                .read_operation(ReadOperationRequest {
                    repo_id,
                    operation_id: op_id_bytes,
                })
                .await?;
            let proto_op = response
                .into_inner()
                .operation
                .ok_or_else(|| "server response should have contained an operation".to_string())?;
            Ok(operation_from_proto(proto_op))
        })
        .map_err(|e| jj_lib::op_store::OpStoreError::Other(e.into()))
    }

    async fn write_operation(&self, contents: &Operation) -> OpStoreResult<OperationId> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let proto_op = operation_to_proto(contents);

        run_async(move || async move {
            let mut client = OpStoreServiceClient::connect(server_url).await?;
            let response = client
                .write_operation(WriteOperationRequest {
                    repo_id,
                    operation: Some(proto_op),
                })
                .await?;
            let op_id_bytes = response.into_inner().operation_id;
            Ok(OperationId::from_bytes(&op_id_bytes))
        })
        .map_err(|e| jj_lib::op_store::OpStoreError::Other(e.into()))
    }

    // TODO: Implement server RPC to resolve short operation ID prefixes for commands like `jj op show <prefix>`
    // (returning SingleMatch, AmbiguousMatch, or NoMatch).
    async fn resolve_operation_id_prefix(
        &self,
        prefix: &HexPrefix,
    ) -> OpStoreResult<PrefixResolution<OperationId>> {
        if prefix.matches(&self.root_operation_id) {
            Ok(PrefixResolution::SingleMatch(
                self.root_operation_id.clone(),
            ))
        } else {
            Ok(PrefixResolution::NoMatch)
        }
    }

    async fn gc(&self, _head_ids: &[OperationId], _keep_newer: SystemTime) -> OpStoreResult<()> {
        Ok(())
    }
}
