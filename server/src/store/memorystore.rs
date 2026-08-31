use async_trait::async_trait;
use cc_common::backend::*;
use cc_common::op_store::*;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use super::{Store, StoreError, StoreResult};

pub type ProjectId = String;
pub type RepoId = String;
pub type CommitId = Vec<u8>;
pub type TreeId = Vec<u8>;
pub type FileId = Vec<u8>;
pub type OpId = Vec<u8>;
pub type ViewId = Vec<u8>;

use cc_common::workspace::WorkspaceState;

#[derive(Debug, Default)]
pub struct MemoryStore {
    pub projects: Mutex<HashSet<ProjectId>>,
    pub repos: Mutex<HashMap<RepoId, ProjectId>>,
    pub commits: Mutex<HashMap<ProjectId, HashMap<CommitId, Commit>>>,
    pub trees: Mutex<HashMap<ProjectId, HashMap<TreeId, Vec<TreeEntry>>>>,
    pub files: Mutex<HashMap<ProjectId, HashMap<FileId, Vec<u8>>>>,
    pub ops: Mutex<HashMap<RepoId, HashMap<OpId, Operation>>>,
    pub views: Mutex<HashMap<RepoId, HashMap<ViewId, View>>>,
    pub op_heads: Mutex<HashMap<RepoId, Vec<OpId>>>,
    pub workspaces: Mutex<HashMap<RepoId, HashMap<(String, String), WorkspaceState>>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Store for MemoryStore {
    async fn is_project_registered(&self, project_id: &str) -> StoreResult<bool> {
        Ok(self.projects.lock().unwrap().contains(project_id))
    }

    async fn register_project(&self, project_id: String, _name: Option<String>) -> StoreResult<()> {
        self.projects.lock().unwrap().insert(project_id);
        Ok(())
    }

    async fn is_repo_registered(&self, repo_id: &str) -> StoreResult<bool> {
        Ok(self.repos.lock().unwrap().contains_key(repo_id))
    }

    async fn register_repo(
        &self,
        repo_id: String,
        project_id: String,
        _name: Option<String>,
    ) -> StoreResult<()> {
        self.projects.lock().unwrap().insert(project_id.clone());
        self.repos.lock().unwrap().insert(repo_id, project_id);
        Ok(())
    }

    async fn get_repo_project_id(&self, repo_id: &str) -> StoreResult<Option<String>> {
        Ok(self.repos.lock().unwrap().get(repo_id).cloned())
    }

    async fn get_commit(&self, project_id: &str, commit_id: &[u8]) -> StoreResult<Option<Commit>> {
        let commits = self.commits.lock().unwrap();
        Ok(commits.get(project_id).and_then(|m| m.get(commit_id).cloned()))
    }

    async fn list_project_commit_ids(&self, project_id: &str) -> StoreResult<Vec<CommitId>> {
        let commits = self.commits.lock().unwrap();
        Ok(commits
            .get(project_id)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default())
    }

    async fn put_commit(
        &self,
        project_id: String,
        commit_id: Vec<u8>,
        commit: Commit,
    ) -> StoreResult<()> {
        let mut commits = self.commits.lock().unwrap();
        commits.entry(project_id).or_default().insert(commit_id, commit);
        Ok(())
    }

    async fn get_tree(&self, project_id: &str, tree_id: &[u8]) -> StoreResult<Option<Vec<TreeEntry>>> {
        let trees = self.trees.lock().unwrap();
        Ok(trees.get(project_id).and_then(|m| m.get(tree_id).cloned()))
    }

    async fn put_tree(
        &self,
        project_id: String,
        tree_id: Vec<u8>,
        entries: Vec<TreeEntry>,
    ) -> StoreResult<()> {
        let mut trees = self.trees.lock().unwrap();
        trees.entry(project_id).or_default().insert(tree_id, entries);
        Ok(())
    }

    async fn get_file(&self, project_id: &str, file_id: &[u8]) -> StoreResult<Option<Vec<u8>>> {
        let files = self.files.lock().unwrap();
        Ok(files.get(project_id).and_then(|m| m.get(file_id).cloned()))
    }

    async fn put_file(
        &self,
        project_id: String,
        file_id: Vec<u8>,
        content: Vec<u8>,
    ) -> StoreResult<()> {
        let mut files = self.files.lock().unwrap();
        files.entry(project_id).or_default().insert(file_id, content);
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

    async fn update_op_heads_append_new_remove_old(
        &self,
        repo_id: String,
        ids_to_remove: &[Vec<u8>],
        id_to_add: Vec<u8>,
    ) -> StoreResult<Vec<OpId>> {
        let mut op_heads = self.op_heads.lock().unwrap();
        let current_heads = op_heads
            .entry(repo_id.clone())
            .or_insert_with(|| vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()]);

        if ids_to_remove.is_empty() {
            if !current_heads.contains(&id_to_add) {
                current_heads.push(id_to_add);
            }
        } else {
            for old in ids_to_remove {
                if !current_heads.contains(old) {
                    return Err(StoreError::CasConflict(format!(
                        "write race detected on stale old_op_head_ids: head {} no longer active in repo {}",
                        hex::encode(old),
                        repo_id
                    )));
                }
            }

            current_heads.retain(|head| !ids_to_remove.contains(head));
            if !current_heads.contains(&id_to_add) {
                current_heads.push(id_to_add);
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

    async fn get_workspace(
        &self,
        repo_id: &str,
        user: &str,
        workspace_name: &str,
    ) -> StoreResult<Option<WorkspaceState>> {
        let workspaces = self.workspaces.lock().unwrap();
        Ok(workspaces
            .get(repo_id)
            .and_then(|ws| ws.get(&(user.to_string(), workspace_name.to_string())).cloned()))
    }

    async fn put_workspace(&self, workspace: WorkspaceState) -> StoreResult<()> {
        let mut workspaces = self.workspaces.lock().unwrap();
        let repo_workspaces = workspaces
            .entry(workspace.repo_id.clone())
            .or_default();
        repo_workspaces.insert(
            (workspace.user.clone(), workspace.workspace_name.clone()),
            workspace,
        );
        Ok(())
    }

    async fn list_workspaces(&self, repo_id: &str) -> StoreResult<Vec<WorkspaceState>> {
        let workspaces = self.workspaces.lock().unwrap();
        Ok(workspaces
            .get(repo_id)
            .map(|ws| ws.values().cloned().collect())
            .unwrap_or_default())
    }
}
