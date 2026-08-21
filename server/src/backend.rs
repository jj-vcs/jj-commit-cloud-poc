use std::sync::Arc;
use tracing::info;

use cc_common::backend::backend_service_server::BackendService;
use cc_common::backend::*;

use crate::hash_utils::{compute_git_blob_hash, compute_git_commit_hash, compute_git_tree_hash};
use crate::store::Store;

#[derive(Clone)]
pub struct CommitCloudBackendService {
    store: Arc<dyn Store>,
}

impl CommitCloudBackendService {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self { store }
    }

    async fn ensure_repo_registered_error(
        &self,
        repo_id: &str,
        action: &str,
    ) -> Result<(), tonic::Status> {
        if !self.store.is_repo_registered(repo_id).await {
            return Err(tonic::Status::not_found(format!(
                "repository should have been registered before {action}"
            )));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl BackendService for CommitCloudBackendService {
    async fn register_repository(
        &self,
        request: tonic::Request<RegisterRepositoryRequest>,
    ) -> Result<tonic::Response<RegisterRepositoryResponse>, tonic::Status> {
        let req = request.into_inner();
        info!("Registering repository: {}", req.repo_id);
        self.store.register_repo(req.repo_id.clone()).await;
        Ok(tonic::Response::new(RegisterRepositoryResponse {
            repo_id: req.repo_id,
        }))
    }

    async fn read_commit(
        &self,
        request: tonic::Request<ReadCommitRequest>,
    ) -> Result<tonic::Response<ReadCommitResponse>, tonic::Status> {
        let req = request.into_inner();
        let repo_id = req.repo_id;
        let commit_id = req.commit_id;

        self.ensure_repo_registered_error(&repo_id, "requesting commits").await?;

        if let Some(commit) = self.store.get_commit(&repo_id, &commit_id).await {
            return Ok(tonic::Response::new(ReadCommitResponse {
                commit: Some(commit),
            }));
        }
        Err(tonic::Status::not_found(
            "commit should have been present in cloud database",
        ))
    }

    async fn write_commit(
        &self,
        request: tonic::Request<WriteCommitRequest>,
    ) -> Result<tonic::Response<WriteCommitResponse>, tonic::Status> {
        let req = request.into_inner();
        let repo_id = req.repo_id;

        self.ensure_repo_registered_error(&repo_id, "requesting commits").await?;

        let mut commit = req.commit.ok_or_else(|| {
            tonic::Status::invalid_argument("request should have contained commit data")
        })?;
        let commit_id = if commit.commit_id.is_empty() {
            compute_git_commit_hash(&commit)
        } else {
            commit.commit_id.clone()
        };
        commit.commit_id = commit_id.clone();
        info!("Writing commit {:?} for repo {}", commit_id, repo_id);

        self.store.put_commit(repo_id, commit_id.clone(), commit).await;

        Ok(tonic::Response::new(WriteCommitResponse { commit_id }))
    }

    async fn read_tree(
        &self,
        request: tonic::Request<ReadTreeRequest>,
    ) -> Result<tonic::Response<ReadTreeResponse>, tonic::Status> {
        let req = request.into_inner();
        let repo_id = req.repo_id;
        let tree_id = req.tree_id;

        self.ensure_repo_registered_error(&repo_id, "requesting trees").await?;

        if tree_id == cc_common::EMPTY_TREE_ID_BYTES {
            return Ok(tonic::Response::new(ReadTreeResponse {
                tree_id,
                entries: vec![],
            }));
        }

        if let Some(entries) = self.store.get_tree(&repo_id, &tree_id).await {
            return Ok(tonic::Response::new(ReadTreeResponse {
                tree_id,
                entries,
            }));
        }
        Err(tonic::Status::not_found(
            "tree should have been present in cloud database",
        ))
    }

    async fn write_tree(
        &self,
        request: tonic::Request<WriteTreeRequest>,
    ) -> Result<tonic::Response<WriteTreeResponse>, tonic::Status> {
        let req = request.into_inner();
        let repo_id = req.repo_id;

        self.ensure_repo_registered_error(&repo_id, "writing trees").await?;

        let tree_id = compute_git_tree_hash(&req.entries);

        self.store.put_tree(repo_id, tree_id.clone(), req.entries).await;

        Ok(tonic::Response::new(WriteTreeResponse { tree_id }))
    }

    type ReadFileStream =
        tokio_stream::wrappers::ReceiverStream<Result<ReadFileResponse, tonic::Status>>;

    async fn read_file(
        &self,
        request: tonic::Request<ReadFileRequest>,
    ) -> Result<tonic::Response<Self::ReadFileStream>, tonic::Status> {
        let req = request.into_inner();
        let repo_id = req.repo_id;
        let file_id = req.file_id;

        self.ensure_repo_registered_error(&repo_id, "reading files").await?;

        let content = self.store.get_file(&repo_id, &file_id).await.ok_or_else(|| {
            tonic::Status::not_found("file should have been present in cloud database")
        })?;

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let _ = tx.send(Ok(ReadFileResponse { chunk: content })).await;
        });

        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ))
    }

    // TODO: Upgrade write_file RPC handler to consume tonic::Streaming<WriteFileRequest>
    // to handle chunked streaming uploads for large files (>4MB) without hitting gRPC limits.
    async fn write_file(
        &self,
        request: tonic::Request<WriteFileRequest>,
    ) -> Result<tonic::Response<WriteFileResponse>, tonic::Status> {
        let req = request.into_inner();
        let repo_id = req.repo_id;

        self.ensure_repo_registered_error(&repo_id, "writing files").await?;

        let file_id = compute_git_blob_hash(&req.content);

        self.store.put_file(repo_id, file_id.clone(), req.content).await;

        Ok(tonic::Response::new(WriteFileResponse { file_id }))
    }

    async fn read_symlink(
        &self,
        _request: tonic::Request<ReadSymlinkRequest>,
    ) -> Result<tonic::Response<ReadSymlinkResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet"))
    }

    async fn write_symlink(
        &self,
        _request: tonic::Request<WriteSymlinkRequest>,
    ) -> Result<tonic::Response<WriteSymlinkResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet"))
    }
}
