use std::fmt::Debug;
use std::path::Path;
use async_trait::async_trait;
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

    async fn update_op_heads(
        &self,
        _old_ids: &[OperationId],
        _new_id: &OperationId,
    ) -> Result<(), OpHeadsStoreError> {
        unimplemented!()
    }

    async fn get_op_heads(&self) -> Result<Vec<OperationId>, OpHeadsStoreError> {
        unimplemented!()
    }

    async fn lock(&self) -> Result<Box<dyn OpHeadsStoreLock + '_>, OpHeadsStoreError> {
        Ok(Box::new(CommitCloudOpHeadsStoreNoLock))
    }
}
