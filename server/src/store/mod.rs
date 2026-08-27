use async_trait::async_trait;
use cc_common::backend::*;
use cc_common::op_store::*;

pub mod memorystore;
pub mod sql_dialect;

pub use memorystore::{CommitId, FileId, MemoryStore, OpId, RepoId, TreeId, ViewId};
pub use sql_dialect::SqlDialect;

// Use async fn for storage functions to not block server threads for read/write operations as current and future storage backends are implemented
#[async_trait]
pub trait Store: Send + Sync {
    async fn is_repo_registered(&self, repo_id: &str) -> bool;
    async fn register_repo(&self, repo_id: String);

    async fn get_commit(&self, repo_id: &str, commit_id: &[u8]) -> Option<Commit>;
    async fn put_commit(&self, repo_id: String, commit_id: Vec<u8>, commit: Commit);

    async fn get_tree(&self, repo_id: &str, tree_id: &[u8]) -> Option<Vec<TreeEntry>>;
    async fn put_tree(&self, repo_id: String, tree_id: Vec<u8>, entries: Vec<TreeEntry>);

    async fn get_file(&self, repo_id: &str, file_id: &[u8]) -> Option<Vec<u8>>;
    async fn put_file(&self, repo_id: String, file_id: Vec<u8>, content: Vec<u8>);

    async fn get_operation(&self, repo_id: &str, op_id: &[u8]) -> Option<Operation>;
    async fn put_operation(&self, repo_id: String, op_id: Vec<u8>, op: Operation);

    async fn get_view(&self, repo_id: &str, view_id: &[u8]) -> Option<View>;
    async fn put_view(&self, repo_id: String, view_id: Vec<u8>, view: View);

    async fn get_op_heads(&self, repo_id: &str) -> Option<Vec<OpId>>;
    async fn update_op_heads(
        &self,
        repo_id: String,
        old_ids: &[Vec<u8>],
        new_id: Vec<u8>,
    ) -> Vec<OpId>;
}
