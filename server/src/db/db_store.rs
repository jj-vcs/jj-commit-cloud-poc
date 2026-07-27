use cc_proto::backend::{Commit, TreeEntry};
use cc_proto::op_store::{Operation, View};
use std::fmt;

#[derive(Debug)]
pub enum DbError {
    NotFound(String),
    AlreadyExists(String),
    InvalidData(String),
    Internal(String),
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::NotFound(msg) => write!(f, "Not found: {msg}"),
            DbError::AlreadyExists(msg) => write!(f, "Already exists: {msg}"),
            DbError::InvalidData(msg) => write!(f, "Invalid data: {msg}"),
            DbError::Internal(msg) => write!(f, "Internal database error: {msg}"),
        }
    }
}

impl std::error::Error for DbError {}

impl From<DbError> for tonic::Status {
    fn from(err: DbError) -> Self {
        match err {
            DbError::NotFound(msg) => tonic::Status::not_found(msg),
            DbError::AlreadyExists(msg) => tonic::Status::already_exists(msg),
            DbError::InvalidData(msg) => tonic::Status::invalid_argument(msg),
            DbError::Internal(msg) => tonic::Status::internal(msg),
        }
    }
}

#[tonic::async_trait]
pub trait DatabaseStore: Send + Sync {
    async fn register_repository(&self, requested_repo_id: Option<&str>) -> Result<String, DbError>;


    // Commits & Trees
    async fn read_commit(&self, repo_id: &str, commit_id: &[u8]) -> Result<Option<Commit>, DbError>;
    async fn write_commit(&self, repo_id: &str, commit: Commit) -> Result<Vec<u8>, DbError>;

    async fn read_tree(&self, repo_id: &str, tree_id: &[u8]) -> Result<Option<Vec<TreeEntry>>, DbError>;
    async fn write_tree(&self, repo_id: &str, entries: Vec<TreeEntry>) -> Result<Vec<u8>, DbError>;

    // Files & Symlinks
    async fn read_file(&self, repo_id: &str, file_id: &[u8]) -> Result<Option<Vec<u8>>, DbError>;
    async fn write_file(&self, repo_id: &str, content: &[u8]) -> Result<Vec<u8>, DbError>;

    async fn read_symlink(&self, repo_id: &str, symlink_id: &[u8]) -> Result<Option<String>, DbError>;
    async fn write_symlink(&self, repo_id: &str, target: &str) -> Result<Vec<u8>, DbError>;

    // Operations, Views, and Op Heads
    async fn read_operation(&self, repo_id: &str, op_id: &[u8]) -> Result<Option<Operation>, DbError>;
    async fn write_operation(&self, repo_id: &str, op: Operation) -> Result<Vec<u8>, DbError>;

    async fn read_view(&self, repo_id: &str, view_id: &[u8]) -> Result<Option<View>, DbError>;
    async fn write_view(&self, repo_id: &str, view: View) -> Result<Vec<u8>, DbError>;

    async fn get_op_heads(&self, repo_id: &str) -> Result<Vec<Vec<u8>>, DbError>;
    async fn add_op_head(&self, repo_id: &str, op_id: &[u8]) -> Result<Vec<Vec<u8>>, DbError>;
    async fn remove_op_head(&self, repo_id: &str, op_id: &[u8]) -> Result<Vec<Vec<u8>>, DbError>;
}
