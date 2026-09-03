#![allow(elided_lifetimes_in_paths)]

use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use futures::StreamExt as _;
use jj_lib::backend::*;
use jj_lib::index::Index;
use jj_lib::object_id::ObjectId;
use jj_lib::repo_path::{RepoPath, RepoPathBuf};
use std::fmt::Debug;
use std::fs;
use std::path::Path;
use std::pin::Pin;
use std::time::SystemTime;

use crate::util::{run_async, CommitCloudConfig};

#[derive(Debug)]
pub struct CommitCloudBackend {
    server_url: String,
    repo_id: String,
    root_commit_id: CommitId,
    root_change_id: ChangeId,
    empty_tree_id: TreeId,
}

impl CommitCloudBackend {
    pub fn name() -> &'static str {
        "commit_cloud"
    }
    
    // TODO: Add logic to pass the optional repository 'name' field during initialization.
    // The field exists in the RPC and can be stored by the server for repository display and aliasing. 
    pub fn init(
        store_path: &Path,
        server_url: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let root_commit_id = CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES);
        let root_change_id = ChangeId::from_bytes(&cc_common::ROOT_CHANGE_ID_BYTES);
        let empty_tree_id = TreeId::from_hex(cc_common::EMPTY_TREE_ID_HEX);

        let server_url_cloned = server_url.to_string();
        let repo_id = run_async(move || async move {
            let mut client = cc_common::backend::backend_service_client::BackendServiceClient::connect(server_url_cloned).await?;
            let register_repo_response = client.register_repository(tonic::Request::new(cc_common::backend::RegisterRepositoryRequest {
                name: None,
            })).await?.into_inner();
            Ok(register_repo_response.repo_id)
        })?;

        // Write local config toml
        let config_path = store_path.join("config.toml");
        let config = CommitCloudConfig {
            server_url: server_url.to_string(),
            repo_id: repo_id.clone(),
        };
        fs::write(&config_path, toml::to_string_pretty(&config)?)?;

        Ok(Self {
            server_url: server_url.to_string(),
            repo_id,
            root_commit_id,
            root_change_id,
            empty_tree_id,
        })
    }

    pub fn load(store_path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config = CommitCloudConfig::load_from_store(store_path)?;
        let root_commit_id = CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES);
        let root_change_id = ChangeId::from_bytes(&cc_common::ROOT_CHANGE_ID_BYTES);
        let empty_tree_id = TreeId::from_hex(cc_common::EMPTY_TREE_ID_HEX);
        Ok(Self {
            server_url: config.server_url,
            repo_id: config.repo_id,
            root_commit_id,
            root_change_id,
            empty_tree_id,
        })
    }
}

pub use cc_common::conversions::backend::*;

#[async_trait]
impl Backend for CommitCloudBackend {

    fn name(&self) -> &str {
        Self::name()
    }

    fn commit_id_length(&self) -> usize {
        cc_common::COMMIT_ID_LENGTH
    }

    fn change_id_length(&self) -> usize {
        cc_common::CHANGE_ID_LENGTH
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
        id: &FileId,
    ) -> BackendResult<Pin<Box<dyn futures::AsyncRead + Send>>> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let file_id_bytes = id.to_bytes().to_vec();
        let file_id_hex = id.hex();

        let content = run_async(move || async move {
            let mut client = cc_common::backend::backend_service_client::BackendServiceClient::connect(server_url).await?;
            let res = client.read_file(tonic::Request::new(cc_common::backend::ReadFileRequest {
                repo_id,
                file_id: file_id_bytes,
            })).await;

            let mut stream = match res {
                Ok(r) => r.into_inner(),
                Err(status) if status.code() == tonic::Code::NotFound => {
                    return Err(Box::new(BackendError::ObjectNotFound {
                        object_type: "file".into(),
                        hash: file_id_hex,
                        source: status.into(),
                    }) as Box<dyn std::error::Error + Send + Sync>);
                }
                Err(status) => return Err(Box::new(status) as Box<dyn std::error::Error + Send + Sync>),
            };

            let mut content = Vec::new();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|e| BackendError::Other(e.into()))?;
                content.extend_from_slice(&chunk.chunk);
            }
            Ok(content)
        }).map_err(|e| match e.downcast::<BackendError>() {
            Ok(err) => *err,
            Err(e) => BackendError::Other(e),
        })?;

        let cursor = futures::io::Cursor::new(content);
        Ok(Box::pin(cursor) as Pin<Box<dyn futures::AsyncRead + Send>>)
    }

    // TODO: Upgrade write_file to stream 64KB chunks directly from AsyncRead to gRPC
    // instead of buffering the whole file payload into memory in a single unary RPC.
    async fn write_file(
        &self,
        _path: &RepoPath,
        contents: &mut (dyn futures::AsyncRead + Send + Unpin),
    ) -> BackendResult<FileId> {
        let mut buffer = Vec::new();
        futures::AsyncReadExt::read_to_end(contents, &mut buffer)
            .await
            .map_err(|e| BackendError::Other(e.into()))?;

        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();

        let file_id_bytes = run_async(move || async move {
            let mut client = cc_common::backend::backend_service_client::BackendServiceClient::connect(server_url).await?;
            let res = client.write_file(tonic::Request::new(cc_common::backend::WriteFileRequest {
                repo_id,
                content: buffer,
            })).await?;
            Ok(res.into_inner().file_id)
        }).map_err(|e| BackendError::Other(e.into()))?;

        Ok(FileId::from_bytes(&file_id_bytes))
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

    async fn read_tree(&self, path: &RepoPath, id: &TreeId) -> BackendResult<Tree> {
        if *id == self.empty_tree_id {
            return Ok(Tree::from_sorted_entries(vec![]));
        }

        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let tree_id_bytes = id.to_bytes().to_vec();
        let tree_id_hex = id.hex();
        let path_str = path.as_internal_file_string().to_string();

        let proto_entries = run_async(move || async move {
            let mut client = cc_common::backend::backend_service_client::BackendServiceClient::connect(server_url).await?;
            let res = client.read_tree(tonic::Request::new(cc_common::backend::ReadTreeRequest {
                repo_id,
                tree_id: tree_id_bytes,
                path: path_str,
            })).await;

            match res {
                Ok(r) => Ok(r.into_inner().entries),
                Err(status) if status.code() == tonic::Code::NotFound => {
                    Err(Box::new(BackendError::ObjectNotFound {
                        object_type: "tree".into(),
                        hash: tree_id_hex,
                        source: status.into(),
                    }) as Box<dyn std::error::Error + Send + Sync>)
                }
                Err(status) => Err(Box::new(status) as Box<dyn std::error::Error + Send + Sync>),
            }

        }).map_err(|e| match e.downcast::<BackendError>() {
            Ok(err) => *err,
            Err(e) => BackendError::Other(e),
        })?;

        let mut jj_entries = Vec::new();
        for entry in proto_entries {
            let (comp, val) = tree_entry_from_proto(entry).map_err(|e| BackendError::Other(e.into()))?;
            jj_entries.push((comp, val));
        }

        Ok(Tree::from_sorted_entries(jj_entries))
    }

    async fn write_tree(&self, path: &RepoPath, tree: &Tree) -> BackendResult<TreeId> {
        if tree.entries().next().is_none() {
            return Ok(self.empty_tree_id.clone());
        }

        let proto_entries: Result<Vec<_>, _> = tree.entries().map(|e| tree_entry_to_proto(&e)).collect();
        let proto_entries = proto_entries?;

        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let path_str = path.as_internal_file_string().to_string();

        let tree_id_bytes = run_async(move || async move {
            let mut client = cc_common::backend::backend_service_client::BackendServiceClient::connect(server_url).await?;
            let res = client.write_tree(tonic::Request::new(cc_common::backend::WriteTreeRequest {
                repo_id,
                path: path_str,
                entries: proto_entries,
            })).await?;
            Ok(res.into_inner().tree_id)
        }).map_err(|e| BackendError::Other(e.into()))?;

        Ok(TreeId::from_bytes(&tree_id_bytes))
    }

    async fn read_commit(&self, id: &CommitId) -> BackendResult<Commit> {
        if *id == self.root_commit_id {
            return Ok(make_root_commit(
                self.root_change_id().clone(),
                self.empty_tree_id.clone(),
            ));
        }

        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let commit_id_bytes = id.to_bytes().to_vec();
        let commit_id_hex = id.hex();

        let proto_commit = run_async(move || async move {
            let mut client = cc_common::backend::backend_service_client::BackendServiceClient::connect(server_url).await?;
            let res = client.read_commit(tonic::Request::new(cc_common::backend::ReadCommitRequest {
                repo_id,
                commit_id: commit_id_bytes,
            })).await;

            match res {
                Ok(r) => r.into_inner().commit.ok_or_else(|| {
                    Box::new(std::io::Error::new(std::io::ErrorKind::NotFound, "server response should have contained commit data"))
                        as Box<dyn std::error::Error + Send + Sync>
                }),
                Err(status) if status.code() == tonic::Code::NotFound => {
                    Err(Box::new(BackendError::ObjectNotFound {
                        object_type: "commit".into(),
                        hash: commit_id_hex,
                        source: status.into(),
                    }) as Box<dyn std::error::Error + Send + Sync>)
                }
                Err(status) => Err(Box::new(status) as Box<dyn std::error::Error + Send + Sync>),
            }
        }).map_err(|e| match e.downcast::<BackendError>() {
            Ok(err) => *err,
            Err(e) => BackendError::Other(e),
        })?;

        Ok(commit_from_proto(proto_commit))
    }

    async fn write_commit(
        &self,
        commit: Commit,
        _sign_with: Option<&mut SigningFn>,
    ) -> BackendResult<(CommitId, Commit)> {
        let proto_commit = commit_to_proto(&commit);
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();

        let returned_id_bytes = run_async(move || async move {
            let mut client = cc_common::backend::backend_service_client::BackendServiceClient::connect(server_url).await?;
            let res = client.write_commit(tonic::Request::new(cc_common::backend::WriteCommitRequest {
                repo_id,
                commit: Some(proto_commit),
            })).await?;
            Ok(res.into_inner().commit_id)
        }).map_err(|e| BackendError::Other(e.into()))?;

        let returned_id = CommitId::from_bytes(&returned_id_bytes);
        Ok((returned_id, commit))
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
