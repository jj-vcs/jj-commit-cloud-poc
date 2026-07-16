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

        // Write local config toml
        let config_path = store_path.join("config.toml");
        let config_content = format!(
            "server_url = \"{}\"\nrepo_id = \"{}\"\n",
            server_url, repo_id
        );
        fs::write(&config_path, config_content)?;

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
        println!("CommitCloudBackend::name() called");
        Self::name()
    }

    fn commit_id_length(&self) -> usize {
        println!("CommitCloudBackend::commit_id_length() called");
        HASH_LENGTH
    }

    fn change_id_length(&self) -> usize {
        println!("CommitCloudBackend::change_id_length() called");
        CHANGE_ID_LENGTH
    }

    fn root_commit_id(&self) -> &CommitId {
        println!("CommitCloudBackend::root_commit_id() called");
        &self.root_commit_id
    }

    fn root_change_id(&self) -> &ChangeId {
        println!("CommitCloudBackend::root_change_id() called");
        &self.root_change_id
    }

    fn empty_tree_id(&self) -> &TreeId {
        println!("CommitCloudBackend::empty_tree_id() called");
        &self.empty_tree_id
    }

    fn concurrency(&self) -> usize {
        println!("CommitCloudBackend::concurrency() called");
        1
    }

    async fn read_file(
        &self,
        path: &RepoPath,
        id: &FileId,
    ) -> BackendResult<Pin<Box<dyn futures::AsyncRead + Send>>> {
        println!("CommitCloudBackend::read_file() called for path = {:?}, id = {:?}", path, id);
        Err(BackendError::Unsupported("read_file not supported".to_string()))
    }

    async fn write_file(
        &self,
        path: &RepoPath,
        _contents: &mut (dyn futures::AsyncRead + Send + Unpin),
    ) -> BackendResult<FileId> {
        println!("CommitCloudBackend::write_file() called for path = {:?}", path);
        Err(BackendError::Unsupported("write_file not supported".to_string()))
    }

    async fn read_symlink(&self, path: &RepoPath, id: &SymlinkId) -> BackendResult<String> {
        println!("CommitCloudBackend::read_symlink() called for path = {:?}, id = {:?}", path, id);
        Err(BackendError::Unsupported("read_symlink not supported".to_string()))
    }

    async fn write_symlink(&self, path: &RepoPath, target: &str) -> BackendResult<SymlinkId> {
        println!("CommitCloudBackend::write_symlink() called for path = {:?}, target = {:?}", path, target);
        Err(BackendError::Unsupported("write_symlink not supported".to_string()))
    }

    async fn read_copy(&self, id: &CopyId) -> BackendResult<CopyHistory> {
        println!("CommitCloudBackend::read_copy() called for id = {:?}", id);
        Err(BackendError::Unsupported("copies not supported".to_string()))
    }

    async fn write_copy(&self, _contents: &CopyHistory) -> BackendResult<CopyId> {
        println!("CommitCloudBackend::write_copy() called");
        Err(BackendError::Unsupported("copies not supported".to_string()))
    }

    async fn get_related_copies(&self, copy_id: &CopyId) -> BackendResult<Vec<RelatedCopy>> {
        println!("CommitCloudBackend::get_related_copies() called for copy_id = {:?}", copy_id);
        Err(BackendError::Unsupported("copies not supported".to_string()))
    }

    async fn read_tree(&self, path: &RepoPath, id: &TreeId) -> BackendResult<Tree> {
        println!("CommitCloudBackend::read_tree() called for path = {:?}, id = {:?}", path, id);
        Err(BackendError::Unsupported("read_tree not supported".to_string()))
    }

    async fn write_tree(&self, path: &RepoPath, _tree: &Tree) -> BackendResult<TreeId> {
        println!("CommitCloudBackend::write_tree() called for path = {:?}", path);
        Ok(self.empty_tree_id.clone())
    }

    async fn read_commit(&self, id: &CommitId) -> BackendResult<Commit> {
        println!("CommitCloudBackend::read_commit() called for id = {:?}", id);
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
        println!("CommitCloudBackend::write_commit() called for change_id = {:?}", commit.change_id);
        let id = CommitId::from_bytes(&[1u8; HASH_LENGTH]);
        Ok((id, commit))
    }

    fn get_copy_records(
        &self,
        _paths: Option<&[RepoPathBuf]>,
        root: &CommitId,
        head: &CommitId,
    ) -> BackendResult<BoxStream<'_, BackendResult<CopyRecord>>> {
        println!("CommitCloudBackend::get_copy_records() called from root = {:?} to head = {:?}", root, head);
        Ok(stream::empty().boxed())
    }

    fn gc(&self, _index: &dyn Index, _keep_newer: SystemTime) -> BackendResult<()> {
        println!("CommitCloudBackend::gc() called");
        Ok(())
    }
}
