use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use tracing::info;

use cc_common::backend::backend_service_server::BackendService;
use cc_common::backend::*;

use crate::hash_utils::{compute_git_blob_hash, compute_git_commit_hash, compute_git_tree_hash};

#[derive(Debug, Default)]
pub struct CommitCloudServerImpl {
    repos: Mutex<HashSet<String>>,
    commits: Mutex<HashMap<String, HashMap<Vec<u8>, Commit>>>,
    trees: Mutex<HashMap<String, HashMap<Vec<u8>, Vec<TreeEntry>>>>,
    files: Mutex<HashMap<String, HashMap<Vec<u8>, Vec<u8>>>>,
}

impl CommitCloudServerImpl {
    fn ensure_repo_registered_error(&self, repo_id: &str, action: &str) -> Result<(), tonic::Status> {
        if !self.repos.lock().unwrap().contains(repo_id) {
            return Err(tonic::Status::not_found(format!(
                "repository should have been registered before {action}"
            )));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl BackendService for CommitCloudServerImpl {
    async fn register_repository(
        &self,
        request: tonic::Request<RegisterRepositoryRequest>,
    ) -> Result<tonic::Response<RegisterRepositoryResponse>, tonic::Status> {
        let req = request.into_inner();
        info!("Registering repository: {}", req.repo_id);
        let mut repos = self.repos.lock().unwrap();
        repos.insert(req.repo_id.clone());
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

        self.ensure_repo_registered_error(&repo_id, "requesting commits")?;

        let commits = self.commits.lock().unwrap();
        if let Some(repo_commits) = commits.get(&repo_id) {
            if let Some(commit) = repo_commits.get(&commit_id) {
                return Ok(tonic::Response::new(ReadCommitResponse {
                    commit: Some(commit.clone()),
                }));
            }
        }
        Err(tonic::Status::not_found("commit should have been present in cloud database"))
    }

    async fn write_commit(
        &self,
        request: tonic::Request<WriteCommitRequest>,
    ) -> Result<tonic::Response<WriteCommitResponse>, tonic::Status> {
        let req = request.into_inner();
        let repo_id = req.repo_id;

        self.ensure_repo_registered_error(&repo_id, "requesting commits")?;

        let mut commit = req.commit.ok_or_else(|| tonic::Status::invalid_argument("request should have contained commit data"))?;
        let commit_id = if commit.commit_id.is_empty() {
            compute_git_commit_hash(&commit)
        } else {
            commit.commit_id.clone()
        };
        commit.commit_id = commit_id.clone();
        info!("Writing commit {:?} for repo {}", commit_id, repo_id);

        let mut commits = self.commits.lock().unwrap();
        let repo_commits = commits.entry(repo_id).or_default();
        repo_commits.insert(commit_id.clone(), commit);

        Ok(tonic::Response::new(WriteCommitResponse { commit_id }))
    }

    async fn read_tree(
        &self,
        request: tonic::Request<ReadTreeRequest>,
    ) -> Result<tonic::Response<ReadTreeResponse>, tonic::Status> {
        let req = request.into_inner();
        let repo_id = req.repo_id;
        let tree_id = req.tree_id;

        self.ensure_repo_registered_error(&repo_id, "requesting trees")?;

        if tree_id == cc_common::EMPTY_TREE_ID_BYTES {
            return Ok(tonic::Response::new(ReadTreeResponse {
                tree_id,
                entries: vec![],
            }));
        }

        let trees = self.trees.lock().unwrap();
        if let Some(repo_trees) = trees.get(&repo_id) {
            if let Some(entries) = repo_trees.get(&tree_id) {
                return Ok(tonic::Response::new(ReadTreeResponse {
                    tree_id,
                    entries: entries.clone(),
                }));
            }
        }
        Err(tonic::Status::not_found("tree should have been present in cloud database"))
    }

    async fn write_tree(
        &self,
        request: tonic::Request<WriteTreeRequest>,
    ) -> Result<tonic::Response<WriteTreeResponse>, tonic::Status> {
        let req = request.into_inner();
        let repo_id = req.repo_id;

        self.ensure_repo_registered_error(&repo_id, "writing trees")?;

        let tree_id = compute_git_tree_hash(&req.entries);

        let mut trees = self.trees.lock().unwrap();
        let repo_trees = trees.entry(repo_id).or_default();
        repo_trees.insert(tree_id.clone(), req.entries);

        Ok(tonic::Response::new(WriteTreeResponse { tree_id }))
    }

    type ReadFileStream = tokio_stream::wrappers::ReceiverStream<Result<ReadFileResponse, tonic::Status>>;

    async fn read_file(
        &self,
        request: tonic::Request<ReadFileRequest>,
    ) -> Result<tonic::Response<Self::ReadFileStream>, tonic::Status> {
        let req = request.into_inner();
        let repo_id = req.repo_id;
        let file_id = req.file_id;

        self.ensure_repo_registered_error(&repo_id, "reading files")?;

        let files = self.files.lock().unwrap();
        let content = files
            .get(&repo_id)
            .and_then(|repo_files| repo_files.get(&file_id))
            .cloned()
            .ok_or_else(|| tonic::Status::not_found("file should have been present in cloud database"))?;

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let _ = tx.send(Ok(ReadFileResponse { chunk: content })).await;
        });

        Ok(tonic::Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    // TODO: Upgrade write_file RPC handler to consume tonic::Streaming<WriteFileRequest>
    // to handle chunked streaming uploads for large files (>4MB) without hitting gRPC limits.
    async fn write_file(
        &self,
        request: tonic::Request<WriteFileRequest>,
    ) -> Result<tonic::Response<WriteFileResponse>, tonic::Status> {
        let req = request.into_inner();
        let repo_id = req.repo_id;

        self.ensure_repo_registered_error(&repo_id, "writing files")?;

        let file_id = compute_git_blob_hash(&req.content);

        let mut files = self.files.lock().unwrap();
        let repo_files = files.entry(repo_id).or_default();
        repo_files.insert(file_id.clone(), req.content);

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
