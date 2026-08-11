use std::fmt::Debug;
use std::path::Path;
use async_trait::async_trait;
use cc_common::op_store::op_store_service_client::OpStoreServiceClient;
use cc_common::op_store::*;
use jj_lib::object_id::ObjectId;
use jj_lib::op_heads_store::{OpHeadsStore, OpHeadsStoreError, OpHeadsStoreLock};
use jj_lib::op_store::OperationId;

use crate::util::{run_async, CommitCloudConfig};

#[derive(Debug)]
pub struct CommitCloudOpHeadsStore {
    server_url: String,
    repo_id: String,
}

struct CommitCloudOpHeadsStoreNoLock;
impl OpHeadsStoreLock for CommitCloudOpHeadsStoreNoLock {}

impl CommitCloudOpHeadsStore {
    pub fn name() -> &'static str {
        "commit_cloud"
    }

    pub fn load(store_path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config = CommitCloudConfig::load_from_store(store_path)?;
        Ok(Self {
            server_url: config.server_url,
            repo_id: config.repo_id,
        })
    }
}

#[async_trait]
impl OpHeadsStore for CommitCloudOpHeadsStore {
    fn name(&self) -> &str {
        Self::name()
    }

    // TODO: Add support for concurrency control with locking to prevent race conditions
    // during concurrent client operations from overwriting operation heads.
    async fn update_op_heads(
        &self,
        old_ids: &[OperationId],
        new_id: &OperationId,
    ) -> Result<(), OpHeadsStoreError> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let old_op_head_ids = old_ids.iter().map(|id| id.as_bytes().to_vec()).collect();
        let new_op_head_id = new_id.as_bytes().to_vec();
        let target_new_id = new_id.clone();

        run_async(move || async move {
            let mut client = OpStoreServiceClient::connect(server_url).await?;
            client
                .update_op_heads(UpdateOpHeadsRequest {
                    repo_id,
                    old_op_head_ids,
                    new_op_head_id,
                })
                .await?;
            Ok(())
        })
        .map_err(|e| OpHeadsStoreError::Write {
            new_op_id: target_new_id,
            source: e.into(),
        })
    }

    async fn get_op_heads(&self) -> Result<Vec<OperationId>, OpHeadsStoreError> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();

        run_async(move || async move {
            let mut client = OpStoreServiceClient::connect(server_url).await?;
            let response = client
                .get_op_heads(GetOpHeadsRequest { repo_id })
                .await?;
            let head_ids: Vec<OperationId> = response
                .into_inner()
                .op_head_ids
                .into_iter()
                .map(|b| OperationId::from_bytes(&b))
                .collect();
            Ok(head_ids)
        })
        .map_err(|e| OpHeadsStoreError::Read(e.into()))
    }

    async fn lock(&self) -> Result<Box<dyn OpHeadsStoreLock + '_>, OpHeadsStoreError> {
        Ok(Box::new(CommitCloudOpHeadsStoreNoLock))
    }
}
