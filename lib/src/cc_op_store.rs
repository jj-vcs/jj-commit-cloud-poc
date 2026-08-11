use std::fmt::Debug;
use std::path::Path;
use std::time::SystemTime;
use async_trait::async_trait;
use jj_lib::object_id::{HexPrefix, ObjectId, PrefixResolution};
use jj_lib::op_store::{OpStore, OpStoreResult, Operation, OperationId, View, ViewId};

use crate::util::{run_async, CommitCloudConfig};

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

    async fn read_view(&self, _id: &ViewId) -> OpStoreResult<View> {
        unimplemented!()
    }

    async fn write_view(&self, _contents: &View) -> OpStoreResult<ViewId> {
        unimplemented!()
    }

    async fn read_operation(&self, _id: &OperationId) -> OpStoreResult<Operation> {
        unimplemented!()
    }

    async fn write_operation(&self, _contents: &Operation) -> OpStoreResult<OperationId> {
        unimplemented!()
    }

    async fn resolve_operation_id_prefix(
        &self,
        prefix: &HexPrefix,
    ) -> OpStoreResult<PrefixResolution<OperationId>> {
        if prefix.matches(&self.root_operation_id) {
            Ok(PrefixResolution::SingleMatch(self.root_operation_id.clone()))
        } else {
            Ok(PrefixResolution::NoMatch)
        }
    }

    async fn gc(&self, _head_ids: &[OperationId], _keep_newer: SystemTime) -> OpStoreResult<()> {
        Ok(())
    }
}
