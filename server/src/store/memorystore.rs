use async_trait::async_trait;
use cc_common::backend::*;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use super::Store;

pub type RepoId = String;
pub type CommitId = Vec<u8>;
pub type TreeId = Vec<u8>;
pub type FileId = Vec<u8>;

#[derive(Debug, Default)]
pub struct MemoryStore {
    pub repos: Mutex<HashSet<RepoId>>,
    pub commits: Mutex<HashMap<RepoId, HashMap<CommitId, Commit>>>,
    pub trees: Mutex<HashMap<RepoId, HashMap<TreeId, Vec<TreeEntry>>>>,
    pub files: Mutex<HashMap<RepoId, HashMap<FileId, Vec<u8>>>>,
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

    async fn register_repo(&self, repo_id: String) {
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
}
