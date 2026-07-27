use crate::db::db_store::{DatabaseStore, DbError};
use cc_proto::backend::{Commit, TreeEntry};
use cc_proto::op_store::{Operation, View};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Default)]
struct InnerStore {
    commits: HashMap<String, HashMap<Vec<u8>, Commit>>,
    trees: HashMap<String, HashMap<Vec<u8>, Vec<TreeEntry>>>,
    files: HashMap<String, HashMap<Vec<u8>, Vec<u8>>>,
    symlinks: HashMap<String, HashMap<Vec<u8>, String>>,
    operations: HashMap<String, HashMap<Vec<u8>, Operation>>,
    views: HashMap<String, HashMap<Vec<u8>, View>>,
    op_heads: HashMap<String, Vec<Vec<u8>>>,
}


#[derive(Clone, Default)]
pub struct MemoryStore {
    inner: Arc<Mutex<InnerStore>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[tonic::async_trait]
impl DatabaseStore for MemoryStore {
    async fn register_repository(&self, requested_repo_id: Option<&str>) -> Result<String, DbError> {
        let mut store = self.inner.lock().unwrap();
        let repo_id = requested_repo_id
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        store.commits.entry(repo_id.clone()).or_default();
        store.trees.entry(repo_id.clone()).or_default();
        store.files.entry(repo_id.clone()).or_default();
        store.symlinks.entry(repo_id.clone()).or_default();
        store.operations.entry(repo_id.clone()).or_default();
        store.views.entry(repo_id.clone()).or_default();
        store.op_heads.entry(repo_id.clone()).or_default();

        Ok(repo_id)
    }


    async fn read_commit(&self, repo_id: &str, commit_id: &[u8]) -> Result<Option<Commit>, DbError> {
        let store = self.inner.lock().unwrap();
        if let Some(repo_commits) = store.commits.get(repo_id) {
            Ok(repo_commits.get(commit_id).cloned())
        } else {
            Ok(None)
        }
    }

    async fn write_commit(&self, repo_id: &str, mut commit: Commit) -> Result<Vec<u8>, DbError> {
        let mut store = self.inner.lock().unwrap();
        let commit_id = if commit.commit_id.is_empty() {
            let mut hasher = blake3::Hasher::new();
            hasher.update(&commit.change_id);
            hasher.update(&commit.root_tree_id);
            for p in &commit.parent_commit_ids {
                hasher.update(p);
            }
            hasher.update(commit.description.as_bytes());
            let hash = hasher.finalize();
            hash.as_bytes()[0..20].to_vec()
        } else {
            commit.commit_id.clone()
        };

        commit.commit_id = commit_id.clone();
        let repo_commits = store.commits.entry(repo_id.to_string()).or_default();
        repo_commits.insert(commit_id.clone(), commit);
        Ok(commit_id)
    }

    async fn read_tree(&self, repo_id: &str, tree_id: &[u8]) -> Result<Option<Vec<TreeEntry>>, DbError> {
        let store = self.inner.lock().unwrap();
        if let Some(repo_trees) = store.trees.get(repo_id) {
            Ok(repo_trees.get(tree_id).cloned())
        } else {
            Ok(None)
        }
    }

    async fn write_tree(&self, repo_id: &str, entries: Vec<TreeEntry>) -> Result<Vec<u8>, DbError> {
        let mut store = self.inner.lock().unwrap();
        let mut hasher = blake3::Hasher::new();
        for entry in &entries {
            hasher.update(entry.name.as_bytes());
            hasher.update(&entry.entry_id);
        }
        let hash = hasher.finalize();
        let tree_id = hash.as_bytes()[0..20].to_vec();

        let repo_trees = store.trees.entry(repo_id.to_string()).or_default();
        repo_trees.insert(tree_id.clone(), entries);
        Ok(tree_id)
    }

    async fn read_file(&self, repo_id: &str, file_id: &[u8]) -> Result<Option<Vec<u8>>, DbError> {
        let store = self.inner.lock().unwrap();
        if let Some(repo_files) = store.files.get(repo_id) {
            Ok(repo_files.get(file_id).cloned())
        } else {
            Ok(None)
        }
    }

    async fn write_file(&self, repo_id: &str, content: &[u8]) -> Result<Vec<u8>, DbError> {
        let mut store = self.inner.lock().unwrap();
        let hash = blake3::hash(content);
        let file_id = hash.as_bytes()[0..20].to_vec();

        let repo_files = store.files.entry(repo_id.to_string()).or_default();
        repo_files.insert(file_id.clone(), content.to_vec());
        Ok(file_id)
    }

    async fn read_symlink(&self, repo_id: &str, symlink_id: &[u8]) -> Result<Option<String>, DbError> {
        let store = self.inner.lock().unwrap();
        if let Some(repo_symlinks) = store.symlinks.get(repo_id) {
            Ok(repo_symlinks.get(symlink_id).cloned())
        } else {
            Ok(None)
        }
    }

    async fn write_symlink(&self, repo_id: &str, target: &str) -> Result<Vec<u8>, DbError> {
        let mut store = self.inner.lock().unwrap();
        let hash = blake3::hash(target.as_bytes());
        let symlink_id = hash.as_bytes()[0..20].to_vec();

        let repo_symlinks = store.symlinks.entry(repo_id.to_string()).or_default();
        repo_symlinks.insert(symlink_id.clone(), target.to_string());
        Ok(symlink_id)
    }

    async fn read_operation(&self, repo_id: &str, op_id: &[u8]) -> Result<Option<Operation>, DbError> {
        let store = self.inner.lock().unwrap();
        if let Some(repo_ops) = store.operations.get(repo_id) {
            let found_op = if repo_ops.contains_key(op_id) {
                repo_ops.get(op_id).cloned()
            } else {
                repo_ops
                    .iter()
                    .find(|(k, _)| k.starts_with(op_id))
                    .map(|(_, v)| v.clone())
            };
            Ok(found_op)
        } else {
            Ok(None)
        }
    }

    async fn write_operation(&self, repo_id: &str, mut op: Operation) -> Result<Vec<u8>, DbError> {
        let mut store = self.inner.lock().unwrap();
        let op_id = if op.operation_id.is_empty() {
            let mut hasher = blake3::Hasher::new();
            hasher.update(&op.view_id);
            for p in &op.parent_op_ids {
                hasher.update(p);
            }
            let hash = hasher.finalize();
            hash.as_bytes()[0..20].to_vec()
        } else {
            op.operation_id.clone()
        };

        op.operation_id = op_id.clone();
        let repo_ops = store.operations.entry(repo_id.to_string()).or_default();
        repo_ops.insert(op_id.clone(), op);
        Ok(op_id)
    }

    async fn read_view(&self, repo_id: &str, view_id: &[u8]) -> Result<Option<View>, DbError> {
        let store = self.inner.lock().unwrap();
        if let Some(repo_views) = store.views.get(repo_id) {
            Ok(repo_views.get(view_id).cloned())
        } else {
            Ok(None)
        }
    }

    async fn write_view(&self, repo_id: &str, mut view: View) -> Result<Vec<u8>, DbError> {
        let mut store = self.inner.lock().unwrap();
        let view_id = if view.view_id.is_empty() {
            let mut hasher = blake3::Hasher::new();
            for h in &view.head_commit_ids {
                hasher.update(h);
            }
            let hash = hasher.finalize();
            hash.as_bytes()[0..20].to_vec()
        } else {
            view.view_id.clone()
        };

        view.view_id = view_id.clone();
        let repo_views = store.views.entry(repo_id.to_string()).or_default();
        repo_views.insert(view_id.clone(), view);
        Ok(view_id)
    }

    async fn get_op_heads(&self, repo_id: &str) -> Result<Vec<Vec<u8>>, DbError> {
        let store = self.inner.lock().unwrap();
        Ok(store.op_heads.get(repo_id).cloned().unwrap_or_default())
    }

    async fn add_op_head(&self, repo_id: &str, op_id: &[u8]) -> Result<Vec<Vec<u8>>, DbError> {
        let mut store = self.inner.lock().unwrap();
        let is_root_op = store
            .operations
            .get(repo_id)
            .and_then(|ops| ops.get(op_id))
            .map(|op| {
                op.parent_op_ids.is_empty()
                    || op.parent_op_ids.iter().all(|id| id.is_empty() || id.iter().all(|&b| b == 0))
            })
            .unwrap_or(false);

        let heads = store.op_heads.entry(repo_id.to_string()).or_default();
        if !heads.is_empty() && is_root_op {
            return Ok(heads.clone());
        }

        if !heads.contains(&op_id.to_vec()) {
            heads.push(op_id.to_vec());
        }
        Ok(heads.clone())
    }

    async fn remove_op_head(&self, repo_id: &str, op_id: &[u8]) -> Result<Vec<Vec<u8>>, DbError> {
        let mut store = self.inner.lock().unwrap();
        let heads = store.op_heads.entry(repo_id.to_string()).or_default();
        heads.retain(|h| h != op_id);
        Ok(heads.clone())
    }
}
