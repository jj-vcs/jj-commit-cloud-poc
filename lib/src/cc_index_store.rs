use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use jj_lib::backend::CommitId;
use jj_lib::default_index::DefaultIndexStore;
use jj_lib::index::{IndexStore, IndexStoreError, IndexStoreResult, MutableIndex, ReadonlyIndex};
use jj_lib::operation::Operation;
use jj_lib::store::Store;

use crate::util::{run_async, CommitCloudConfig};

#[derive(Debug)]
pub struct CommitCloudIndexStore {
    inner: DefaultIndexStore,
    server_url: String,
    project_id: String,
}

impl CommitCloudIndexStore {
    pub fn init(store_path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let inner = DefaultIndexStore::init(store_path)?;
        let config = CommitCloudConfig::load_from_store(store_path)?;
        Ok(Self {
            inner,
            server_url: config.server_url,
            project_id: config.project_id,
        })
    }

    pub fn load(store_path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let inner = DefaultIndexStore::load(store_path);
        let config = CommitCloudConfig::load_from_store(store_path)?;
        Ok(Self {
            inner,
            server_url: config.server_url,
            project_id: config.project_id,
        })
    }
}

#[async_trait(?Send)]
impl IndexStore for CommitCloudIndexStore {
    fn name(&self) -> &str {
        "commit_cloud"
    }

    async fn get_index_at_op(
        &self,
        op: &Operation,
        store: &Arc<Store>,
    ) -> IndexStoreResult<Box<dyn ReadonlyIndex>> {
        let index = self.inner.get_index_at_op(op, store).await?;

        let server_url = self.server_url.clone();
        let project_id = self.project_id.clone();
        let project_commit_ids: Vec<CommitId> = run_async(move || async move {
            let mut client =
                cc_common::backend::backend_service_client::BackendServiceClient::connect(
                    server_url,
                )
                .await?;
            let res = client
                .list_project_commits(tonic::Request::new(
                    cc_common::backend::ListProjectCommitsRequest { project_id },
                ))
                .await?
                .into_inner();
            Ok(res
                .commit_ids
                .into_iter()
                .map(|b| CommitId::from_bytes(&b))
                .collect())
        })
        .map_err(IndexStoreError::Read)?;

        let mut missing_ids = Vec::new();
        for cid in &project_commit_ids {
            if !index
                .as_index()
                .has_id(cid)
                .map_err(|e| IndexStoreError::Read(e.into()))?
            {
                missing_ids.push(cid.clone());
            }
        }

        if missing_ids.is_empty() {
            return Ok(index);
        }

        let mut mut_index = index.start_modification();

        for cid in &missing_ids {
            add_commit_recursive(&mut *mut_index, store, cid).await?;
        }

        self.inner.write_index(mut_index, op)
    }

    fn write_index(
        &self,
        index: Box<dyn MutableIndex>,
        op: &Operation,
    ) -> IndexStoreResult<Box<dyn ReadonlyIndex>> {
        self.inner.write_index(index, op)
    }
}

async fn add_commit_recursive(
    mut_index: &mut dyn MutableIndex,
    store: &Arc<Store>,
    commit_id: &CommitId,
) -> IndexStoreResult<()> {
    if mut_index
        .as_index()
        .has_id(commit_id)
        .map_err(|e| IndexStoreError::Read(e.into()))?
    {
        return Ok(());
    }
    let commit = store
        .get_commit_async(commit_id)
        .await
        .map_err(|e| IndexStoreError::Read(e.into()))?;
    for parent_id in commit.parent_ids() {
        Box::pin(add_commit_recursive(mut_index, store, parent_id)).await?;
    }
    mut_index
        .add_commit(&commit)
        .await
        .map_err(|e| IndexStoreError::Write(e.into()))?;
    Ok(())
}
