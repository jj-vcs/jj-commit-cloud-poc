#![allow(elided_lifetimes_in_paths)]

use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use futures::StreamExt as _;
use jj_lib::backend::*;
use jj_lib::index::Index;
use jj_lib::repo_path::{RepoPath, RepoPathBuf};
use std::fmt::Debug;
use std::fs;
use std::path::Path;
use std::pin::Pin;
use std::time::SystemTime;
use uuid::Uuid;

// git standard hash sizes, see upstream /lib/src/git_backend.rs
const HASH_LENGTH: usize = 20;
const CHANGE_ID_LENGTH: usize = 16;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CommitCloudConfig {
    pub server_url: String,
    pub repo_id: String,
}

impl CommitCloudConfig {
    pub fn load_from_store(store_path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config_path = store_path.join("config.toml");
        let content = fs::read_to_string(&config_path)?;
        Ok(toml::from_str(&content)?)
    }
}

#[derive(Debug)]
pub struct CommitCloudBackend {
    root_commit_id: CommitId,
    root_change_id: ChangeId,
    empty_tree_id: TreeId,
}

impl CommitCloudBackend {
    pub fn name() -> &'static str {
        "commit_cloud"
    }

    pub fn init(
        store_path: &Path,
        server_url: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let repo_id = Uuid::new_v4().to_string();
        let root_commit_id = CommitId::from_bytes(&[0u8; HASH_LENGTH]);
        let root_change_id = ChangeId::from_bytes(&[0u8; CHANGE_ID_LENGTH]);
        let empty_tree_id = TreeId::from_hex("4b825dc642cb6eb9a060e54bf8d69288fbee4904");

        // Open a synchronous gRPC connection and register the repository UUID in the cloud!
        let register_future = async {
            let mut client = cc_common::backend::backend_service_client::BackendServiceClient::connect(server_url.to_string()).await?;
            client.register_repository(tonic::Request::new(cc_common::backend::RegisterRepositoryRequest {
                repo_id: repo_id.clone(),
            })).await?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        };

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(register_future))?;
        } else {
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(register_future)?;
        }

        // Write local config toml
        let config_path = store_path.join("config.toml");
        let config = CommitCloudConfig {
            server_url: server_url.to_string(),
            repo_id: repo_id.clone(),
        };
        fs::write(&config_path, toml::to_string_pretty(&config)?)?;

        Ok(Self {
            root_commit_id,
            root_change_id,
            empty_tree_id,
        })
    }
}

#[async_trait]
impl Backend for CommitCloudBackend {
    fn name(&self) -> &str {
        Self::name()
    }

    fn commit_id_length(&self) -> usize {
        HASH_LENGTH
    }

    fn change_id_length(&self) -> usize {
        CHANGE_ID_LENGTH
    }

    fn root_commit_id(&self) -> &CommitId {
        &self.root_commit_id
    }

    fn root_change_id(&self) -> &ChangeId {
        &self.root_change_id
    }

    fn empty_tree_id(&self) -> &TreeId {
        &self.empty_tree_id
    }

    fn concurrency(&self) -> usize {
        1
    }

    async fn read_file(
        &self,
        _path: &RepoPath,
        _id: &FileId,
    ) -> BackendResult<Pin<Box<dyn futures::AsyncRead + Send>>> {
        Err(BackendError::Unsupported("read_file not supported".to_string()))
    }

    async fn write_file(
        &self,
        _path: &RepoPath,
        _contents: &mut (dyn futures::AsyncRead + Send + Unpin),
    ) -> BackendResult<FileId> {
        Err(BackendError::Unsupported("write_file not supported".to_string()))
    }

    async fn read_symlink(&self, _path: &RepoPath, _id: &SymlinkId) -> BackendResult<String> {
        Err(BackendError::Unsupported("read_symlink not supported".to_string()))
    }

    async fn write_symlink(&self, _path: &RepoPath, _target: &str) -> BackendResult<SymlinkId> {
        Err(BackendError::Unsupported("write_symlink not supported".to_string()))
    }

    async fn read_copy(&self, _id: &CopyId) -> BackendResult<CopyHistory> {
        Err(BackendError::Unsupported("copies not supported".to_string()))
    }

    async fn write_copy(&self, _contents: &CopyHistory) -> BackendResult<CopyId> {
        Err(BackendError::Unsupported("copies not supported".to_string()))
    }

    async fn get_related_copies(&self, _copy_id: &CopyId) -> BackendResult<Vec<RelatedCopy>> {
        Err(BackendError::Unsupported("copies not supported".to_string()))
    }

    async fn read_tree(&self, _path: &RepoPath, _id: &TreeId) -> BackendResult<Tree> {
        Err(BackendError::Unsupported("read_tree not supported".to_string()))
    }

    async fn write_tree(&self, _path: &RepoPath, _tree: &Tree) -> BackendResult<TreeId> {
        Ok(self.empty_tree_id.clone())
    }

    async fn read_commit(&self, id: &CommitId) -> BackendResult<Commit> {
        if *id == self.root_commit_id {
            return Ok(make_root_commit(
                self.root_change_id().clone(),
                self.empty_tree_id.clone(),
            ));
        }
        Err(BackendError::Unsupported("read_commit not supported".to_string()))
    }

    async fn write_commit(
        &self,
        commit: Commit,
        _sign_with: Option<&mut SigningFn>,
    ) -> BackendResult<(CommitId, Commit)> {
        let id = CommitId::from_bytes(&[1u8; HASH_LENGTH]);
        Ok((id, commit))
    }

    fn get_copy_records(
        &self,
        _paths: Option<&[RepoPathBuf]>,
        _root: &CommitId,
        _head: &CommitId,
    ) -> BackendResult<BoxStream<'_, BackendResult<CopyRecord>>> {
        Ok(stream::empty().boxed())
    }

    fn gc(&self, _index: &dyn Index, _keep_newer: SystemTime) -> BackendResult<()> {
        Ok(())
    }
}
