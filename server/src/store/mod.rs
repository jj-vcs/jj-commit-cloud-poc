use async_trait::async_trait;
use cc_common::backend::*;
use cc_common::op_store::*;

pub mod memorystore;
pub mod sqlitestore;

pub use memorystore::{CommitId, FileId, MemoryStore, OpId, RepoId, TreeId, ViewId};
pub use sqlitestore::SqliteStore;

// Error returned by fallible store operations.
// Kept independent of any transport type so store implementations
// Note: a read that finds nothing is not an error — that is reported as
// `Ok(None)`. `Read` is reserved for the backend actually failing.
#[derive(Debug)]
pub enum StoreError {
    // A read from the storage backend failed (e.g. query/statement error or I/O failure).
    Read(String),
    // A write to the storage backend failed.
    Write(String),
    // A Compare-And-Swap (CAS) optimistic concurrency conflict occurred.
    CasConflict(String),
    // Data failed to be encoded into protobuf format.
    Encode(String),
    // Data was retrieved but could not be decoded, indicating corruption.
    Decode(String),
    // The blocking storage task failed to run to completion (e.g. panicked or was cancelled).
    Task(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::Read(msg) => write!(f, "storage read error: {msg}"),
            StoreError::Write(msg) => write!(f, "storage write error: {msg}"),
            StoreError::CasConflict(msg) => write!(f, "CAS conflict: {msg}"),
            StoreError::Encode(msg) => write!(f, "storage encode error: {msg}"),
            StoreError::Decode(msg) => write!(f, "stored data is corrupt: {msg}"),
            StoreError::Task(msg) => write!(f, "storage task error: {msg}"),
        }
    }
}

impl std::error::Error for StoreError {}

pub type StoreResult<T> = Result<T, StoreError>;

// Use async fn for storage functions to not block server threads for read/write operations as current and future storage backends are implemented
#[async_trait]
pub trait Store: Send + Sync {
    async fn is_repo_registered(&self, repo_id: &str) -> StoreResult<bool>;
    async fn register_repo(&self, repo_id: String, name: Option<String>) -> StoreResult<()>;

    async fn get_commit(&self, repo_id: &str, commit_id: &[u8]) -> StoreResult<Option<Commit>>;
    async fn put_commit(
        &self,
        repo_id: String,
        commit_id: Vec<u8>,
        commit: Commit,
    ) -> StoreResult<()>;

    async fn get_tree(&self, repo_id: &str, tree_id: &[u8]) -> StoreResult<Option<Vec<TreeEntry>>>;
    async fn put_tree(
        &self,
        repo_id: String,
        tree_id: Vec<u8>,
        entries: Vec<TreeEntry>,
    ) -> StoreResult<()>;

    async fn get_file(&self, repo_id: &str, file_id: &[u8]) -> StoreResult<Option<Vec<u8>>>;
    async fn put_file(&self, repo_id: String, file_id: Vec<u8>, content: Vec<u8>) -> StoreResult<()>;

    async fn get_operation(&self, repo_id: &str, op_id: &[u8]) -> StoreResult<Option<Operation>>;
    async fn put_operation(&self, repo_id: String, op_id: Vec<u8>, op: Operation) -> StoreResult<()>;

    async fn get_view(&self, repo_id: &str, view_id: &[u8]) -> StoreResult<Option<View>>;
    async fn put_view(&self, repo_id: String, view_id: Vec<u8>, view: View) -> StoreResult<()>;

    async fn get_op_heads(&self, repo_id: &str) -> StoreResult<Option<Vec<Vec<u8>>>>;
    // Updates op heads by removing `old_ids` (if present) and appending `new_id`.
    // Validates that `old_ids` are currently active to prevent stale client overwrites.
    async fn update_op_heads_append_row(
        &self,
        repo_id: String,
        old_ids: &[Vec<u8>],
        new_id: Vec<u8>,
    ) -> StoreResult<Vec<Vec<u8>>>;

    // Atomically compares the full set of currently active op heads against `expected_exact_heads`
    // and replaces them entirely with `[new_id]`. Fails immediately with `StoreError::CasConflict`
    // if the active set does not match `expected_exact_heads`. Used during server-side reconciliation.
    async fn update_op_heads_compare_and_swap(
        &self,
        repo_id: String,
        expected_exact_heads: &[Vec<u8>],
        new_id: Vec<u8>,
    ) -> StoreResult<Vec<Vec<u8>>>;
}
