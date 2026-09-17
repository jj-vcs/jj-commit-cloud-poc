// TODO: add server-side error logging for file and operation ids for debugging
use std::sync::Arc;
use tracing::info;

use cc_common::backend::backend_service_server::BackendService;
use cc_common::backend::*;

use crate::error_util::ensure_project_registered_error;
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
}

#[tonic::async_trait]
impl BackendService for CommitCloudBackendService {
    async fn register_project(
        &self,
        request: tonic::Request<RegisterProjectRequest>,
    ) -> Result<tonic::Response<RegisterProjectResponse>, tonic::Status> {
        let req = request.into_inner();
        let project_id = req
            .project_id
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        info!("Registering project: {} (name: {:?})", project_id, req.name);
        self.store
            .register_project(project_id.clone(), req.name)
            .await?;
        Ok(tonic::Response::new(RegisterProjectResponse { project_id }))
    }

    async fn register_repository(
        &self,
        request: tonic::Request<RegisterRepositoryRequest>,
    ) -> Result<tonic::Response<RegisterRepositoryResponse>, tonic::Status> {
        let req = request.into_inner();
        let project_id = req.project_id.trim().to_string();
        if project_id.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "project_id is required when registering a repository",
            ));
        }

        if let Some(repo_id) = req.repo_id.filter(|s| !s.is_empty()) {
            if let Some(existing_project_id) = self.store.get_repo_project_id(&repo_id).await? {
                if existing_project_id != project_id {
                    return Err(tonic::Status::invalid_argument(format!(
                        "Repository '{}' is already registered under project '{}', not '{}'",
                        repo_id, existing_project_id, project_id
                    )));
                }
                return Ok(tonic::Response::new(RegisterRepositoryResponse {
                    repo_id,
                    project_id: existing_project_id,
                }));
            }
            info!(
                "Registering explicit repository: {} in project {} (name: {:?})",
                repo_id, project_id, req.name
            );
            self.store
                .register_repo(repo_id.clone(), project_id.clone(), req.name)
                .await?;
            return Ok(tonic::Response::new(RegisterRepositoryResponse {
                repo_id,
                project_id,
            }));
        }

        let repo_id = uuid::Uuid::new_v4().to_string();
        info!(
            "Registering repository: {} in project {} (name: {:?})",
            repo_id, project_id, req.name
        );
        self.store
            .register_repo(repo_id.clone(), project_id.clone(), req.name)
            .await?;
        Ok(tonic::Response::new(RegisterRepositoryResponse {
            repo_id,
            project_id,
        }))
    }

    async fn read_commit(
        &self,
        request: tonic::Request<ReadCommitRequest>,
    ) -> Result<tonic::Response<ReadCommitResponse>, tonic::Status> {
        let req = request.into_inner();
        let project_id = req.project_id;
        let commit_id = req.commit_id;

        ensure_project_registered_error(self.store.as_ref(), &project_id, "requesting commits")
            .await?;

        if let Some(commit) = self.store.get_commit(&project_id, &commit_id).await? {
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
        let project_id = req.project_id;

        ensure_project_registered_error(self.store.as_ref(), &project_id, "requesting commits")
            .await?;

        let mut commit = req.commit.ok_or_else(|| {
            tonic::Status::invalid_argument("request should have contained commit data")
        })?;
        if commit.parent_commit_ids.len() > 1
            && commit
                .parent_commit_ids
                .iter()
                .any(|p| p.as_slice() == cc_common::ROOT_COMMIT_ID_BYTES)
        {
            return Err(tonic::Status::invalid_argument(
                "The Commit Cloud backend does not support creating merge commits with the root commit as one of the parents.",
            ));
        }
        let commit_id = if commit.commit_id.is_empty() {
            compute_git_commit_hash(&commit)
        } else {
            commit.commit_id.clone()
        };
        commit.commit_id = commit_id.clone();
        info!("Writing commit {:?} for project {}", commit_id, project_id);

        self.store
            .put_commit(project_id, commit_id.clone(), commit)
            .await?;

        Ok(tonic::Response::new(WriteCommitResponse { commit_id }))
    }

    async fn list_project_commits(
        &self,
        request: tonic::Request<ListProjectCommitsRequest>,
    ) -> Result<tonic::Response<ListProjectCommitsResponse>, tonic::Status> {
        let req = request.into_inner();
        let project_id = req.project_id;

        ensure_project_registered_error(self.store.as_ref(), &project_id, "requesting commits")
            .await?;

        let commit_ids = self.store.list_project_commit_ids(&project_id).await?;
        Ok(tonic::Response::new(ListProjectCommitsResponse {
            commit_ids,
        }))
    }

    async fn read_tree(
        &self,
        request: tonic::Request<ReadTreeRequest>,
    ) -> Result<tonic::Response<ReadTreeResponse>, tonic::Status> {
        let req = request.into_inner();
        let project_id = req.project_id;
        let tree_id = req.tree_id;

        ensure_project_registered_error(self.store.as_ref(), &project_id, "requesting trees")
            .await?;

        if tree_id == cc_common::EMPTY_TREE_ID_BYTES {
            return Ok(tonic::Response::new(ReadTreeResponse {
                tree_id,
                entries: vec![],
            }));
        }

        if let Some(entries) = self.store.get_tree(&project_id, &tree_id).await? {
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
        let project_id = req.project_id;

        ensure_project_registered_error(self.store.as_ref(), &project_id, "writing trees").await?;

        let tree_id = compute_git_tree_hash(&req.entries);

        self.store
            .put_tree(project_id, tree_id.clone(), req.entries)
            .await?;

        Ok(tonic::Response::new(WriteTreeResponse { tree_id }))
    }

    type ReadFileStream =
        tokio_stream::wrappers::ReceiverStream<Result<ReadFileResponse, tonic::Status>>;

    async fn read_file(
        &self,
        request: tonic::Request<ReadFileRequest>,
    ) -> Result<tonic::Response<Self::ReadFileStream>, tonic::Status> {
        let req = request.into_inner();
        let project_id = req.project_id;
        let file_id = req.file_id;

        ensure_project_registered_error(self.store.as_ref(), &project_id, "reading files").await?;

        let content = self
            .store
            .get_file(&project_id, &file_id)
            .await?
            .ok_or_else(|| {
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
        let project_id = req.project_id;

        ensure_project_registered_error(self.store.as_ref(), &project_id, "writing files").await?;

        let file_id = compute_git_blob_hash(&req.content);

        self.store
            .put_file(project_id, file_id.clone(), req.content)
            .await?;

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
