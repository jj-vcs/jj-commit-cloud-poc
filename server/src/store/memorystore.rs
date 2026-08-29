use async_trait::async_trait;
use cc_common::backend::*;
use cc_common::op_store::*;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use super::Store;

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
    async fn is_repo_registered(&self, repo_id: &str) -> bool {
        self.repos.lock().unwrap().contains(repo_id)
    }

    async fn register_repo(&self, repo_id: String, _name: Option<String>) {
        self.repos.lock().unwrap().insert(repo_id);
    }

    async fn get_commit(&self, repo_id: &str, commit_id: &[u8]) -> Option<Commit> {
        let commits = self.commits.lock().unwrap();
        commits.get(repo_id)?.get(commit_id).cloned()
    }

    async fn put_commit(&self, repo_id: String, commit_id: Vec<u8>, commit: Commit) {
        let mut commits = self.commits.lock().unwrap();
        commits.entry(repo_id).or_default().insert(commit_id, commit);
    }

    async fn get_tree(&self, repo_id: &str, tree_id: &[u8]) -> Option<Vec<TreeEntry>> {
        let trees = self.trees.lock().unwrap();
        trees.get(repo_id)?.get(tree_id).cloned()
    }

    async fn put_tree(&self, repo_id: String, tree_id: Vec<u8>, entries: Vec<TreeEntry>) {
        let mut trees = self.trees.lock().unwrap();
        trees.entry(repo_id).or_default().insert(tree_id, entries);
    }

    async fn get_file(&self, repo_id: &str, file_id: &[u8]) -> Option<Vec<u8>> {
        let files = self.files.lock().unwrap();
        files.get(repo_id)?.get(file_id).cloned()
    }

    async fn put_file(&self, repo_id: String, file_id: Vec<u8>, content: Vec<u8>) {
        let mut files = self.files.lock().unwrap();
        files.entry(repo_id).or_default().insert(file_id, content);
    }

    async fn get_operation(&self, repo_id: &str, op_id: &[u8]) -> Option<Operation> {
        let ops = self.ops.lock().unwrap();
        ops.get(repo_id)?.get(op_id).cloned()
    }

    async fn put_operation(&self, repo_id: String, op_id: Vec<u8>, op: Operation) {
        let mut ops = self.ops.lock().unwrap();
        ops.entry(repo_id).or_default().insert(op_id, op);
    }

    async fn get_view(&self, repo_id: &str, view_id: &[u8]) -> Option<View> {
        let views = self.views.lock().unwrap();
        views.get(repo_id)?.get(view_id).cloned()
    }

    async fn put_view(&self, repo_id: String, view_id: Vec<u8>, view: View) {
        let mut views = self.views.lock().unwrap();
        views.entry(repo_id).or_default().insert(view_id, view);
    }

    async fn get_op_heads(&self, repo_id: &str) -> Option<Vec<OpId>> {
        let op_heads = self.op_heads.lock().unwrap();
        op_heads.get(repo_id).cloned()
    }

    // Updates op heads by removing old superseded heads and appending new_id, while preserving concurrent sibling heads.
    // This is required for divergent op head resolution down the road—if we wiped out sibling heads here, client-side reconciliation wouldn't receive the divergent heads it needs to merge them.
    async fn update_op_heads(
        &self,
        repo_id: String,
        old_ids: &[Vec<u8>],
        new_id: Vec<u8>,
    ) -> Vec<OpId> {
        let mut op_heads = self.op_heads.lock().unwrap();
        let current_heads = op_heads
            .entry(repo_id)
            .or_insert_with(|| vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()]);

        if old_ids.is_empty() {
            if !current_heads.contains(&new_id) {
                current_heads.push(new_id);
            }
        } else {
            let mut removed_any = false;
            current_heads.retain(|head| {
                if old_ids.contains(head) {
                    removed_any = true;
                    false
                } else {
                    true
                }
            });

            if removed_any && !current_heads.contains(&new_id) {
                current_heads.push(new_id);
            }
        }

        current_heads.clone()
    }
}
