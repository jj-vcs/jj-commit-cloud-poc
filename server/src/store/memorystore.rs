use async_trait::async_trait;
use cc_common::backend::*;
use cc_common::op_store::*;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use super::{Store, StoreError, StoreResult};

pub type RepoId = String;
pub type CommitId = Vec<u8>;
pub type TreeId = Vec<u8>;
pub type FileId = Vec<u8>;
pub type OpId = Vec<u8>;
pub type ViewId = Vec<u8>;

#[derive(Debug, Default)]
pub struct MemoryStore {
    pub repos: Mutex<HashSet<RepoId>>,
    pub commits: Mutex<HashMap<RepoId, HashMap<CommitId, Commit>>>,
    pub trees: Mutex<HashMap<RepoId, HashMap<TreeId, Vec<TreeEntry>>>>,
    pub files: Mutex<HashMap<RepoId, HashMap<FileId, Vec<u8>>>>,
    pub ops: Mutex<HashMap<RepoId, HashMap<OpId, Operation>>>,
    pub views: Mutex<HashMap<RepoId, HashMap<ViewId, View>>>,
    pub op_heads: Mutex<HashMap<RepoId, Vec<OpId>>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Store for MemoryStore {
    async fn is_repo_registered(&self, repo_id: &str) -> StoreResult<bool> {
        Ok(self.repos.lock().unwrap().contains(repo_id))
    }

    async fn register_repo(&self, repo_id: String, _name: Option<String>) -> StoreResult<()> {
        self.repos.lock().unwrap().insert(repo_id);
        Ok(())
    }

    async fn get_commit(&self, repo_id: &str, commit_id: &[u8]) -> StoreResult<Option<Commit>> {
        let commits = self.commits.lock().unwrap();
        Ok(commits.get(repo_id).and_then(|m| m.get(commit_id).cloned()))
    }

    async fn put_commit(
        &self,
        repo_id: String,
        commit_id: Vec<u8>,
        commit: Commit,
    ) -> StoreResult<()> {
        let mut commits = self.commits.lock().unwrap();
        commits.entry(repo_id).or_default().insert(commit_id, commit);
        Ok(())
    }

    async fn get_tree(&self, repo_id: &str, tree_id: &[u8]) -> StoreResult<Option<Vec<TreeEntry>>> {
        let trees = self.trees.lock().unwrap();
        Ok(trees.get(repo_id).and_then(|m| m.get(tree_id).cloned()))
    }

    async fn put_tree(
        &self,
        repo_id: String,
        tree_id: Vec<u8>,
        entries: Vec<TreeEntry>,
    ) -> StoreResult<()> {
        let mut trees = self.trees.lock().unwrap();
        trees.entry(repo_id).or_default().insert(tree_id, entries);
        Ok(())
    }

    async fn get_file(&self, repo_id: &str, file_id: &[u8]) -> StoreResult<Option<Vec<u8>>> {
        let files = self.files.lock().unwrap();
        Ok(files.get(repo_id).and_then(|m| m.get(file_id).cloned()))
    }

    async fn put_file(
        &self,
        repo_id: String,
        file_id: Vec<u8>,
        content: Vec<u8>,
    ) -> StoreResult<()> {
        let mut files = self.files.lock().unwrap();
        files.entry(repo_id).or_default().insert(file_id, content);
        Ok(())
    }

    async fn get_operation(&self, repo_id: &str, op_id: &[u8]) -> StoreResult<Option<Operation>> {
        let ops = self.ops.lock().unwrap();
        Ok(ops.get(repo_id).and_then(|m| m.get(op_id).cloned()))
    }

    async fn put_operation(
        &self,
        repo_id: String,
        op_id: Vec<u8>,
        op: Operation,
    ) -> StoreResult<()> {
        let mut ops = self.ops.lock().unwrap();
        ops.entry(repo_id).or_default().insert(op_id, op);
        Ok(())
    }

    async fn get_view(&self, repo_id: &str, view_id: &[u8]) -> StoreResult<Option<View>> {
        let views = self.views.lock().unwrap();
        Ok(views.get(repo_id).and_then(|m| m.get(view_id).cloned()))
    }

    async fn put_view(&self, repo_id: String, view_id: Vec<u8>, view: View) -> StoreResult<()> {
        let mut views = self.views.lock().unwrap();
        views.entry(repo_id).or_default().insert(view_id, view);
        Ok(())
    }

    async fn get_op_heads(&self, repo_id: &str) -> StoreResult<Option<Vec<OpId>>> {
        let op_heads = self.op_heads.lock().unwrap();
        Ok(op_heads.get(repo_id).cloned())
    }

    async fn update_op_heads_append_row(
        &self,
        repo_id: String,
        old_ids: &[Vec<u8>],
        new_id: Vec<u8>,
    ) -> StoreResult<Vec<OpId>> {
        let mut op_heads = self.op_heads.lock().unwrap();
        let current_heads = op_heads
            .entry(repo_id.clone())
            .or_insert_with(|| vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()]);

        if old_ids.is_empty() {
            if !current_heads.contains(&new_id) {
                current_heads.push(new_id);
            }
        } else {
            for old in old_ids {
                if !current_heads.contains(old) {
                    return Err(StoreError::CasConflict(format!(
                        "write race detected on stale old_op_head_ids: head {} no longer active in repo {}",
                        hex::encode(old),
                        repo_id
                    )));
                }
            }

            current_heads.retain(|head| !old_ids.contains(head));
            if !current_heads.contains(&new_id) {
                current_heads.push(new_id);
            }
        }

        Ok(current_heads.clone())
    }

    async fn update_op_heads_compare_and_swap(
        &self,
        repo_id: String,
        expected_exact_heads: &[Vec<u8>],
        new_id: Vec<u8>,
    ) -> StoreResult<Vec<OpId>> {
        let mut op_heads = self.op_heads.lock().unwrap();
        let current_heads = op_heads
            .entry(repo_id.clone())
            .or_insert_with(|| vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()]);

        let current_set: std::collections::HashSet<Vec<u8>> =
            current_heads.iter().cloned().collect();
        let expected_set: std::collections::HashSet<Vec<u8>> =
            expected_exact_heads.iter().cloned().collect();

        if current_set != expected_set {
            return Err(StoreError::CasConflict(format!(
                "reconciliation CAS conflict for repo {repo_id}: expected heads {expected_set:?}, found {current_set:?}"
            )));
        }

        *current_heads = vec![new_id.clone()];
        Ok(vec![new_id])
    }
}
