// Copyright 2024-2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::fmt::Debug;
use std::path::Path;
use std::pin::Pin;
use std::time::SystemTime;
use async_trait::async_trait;
use futures::io::AsyncRead;
use futures::stream::BoxStream;
use jj_lib::backend::{
    Backend, BackendInitError, BackendResult, ChangeId,
    Commit, CommitId, CopyHistory, CopyId, CopyRecord, FileId, RelatedCopy,
    SigningFn, SymlinkId, Tree, TreeId,
};
use jj_lib::index::Index;
use jj_lib::repo_path::{RepoPath, RepoPathBuf};

#[derive(Debug)]
pub struct SqliteBackend {}

impl SqliteBackend {
    pub fn init(_store_path: &Path) -> Result<Self, BackendInitError> {
        todo!("SqliteBackend::init not implemented")
    }

    pub fn load(_store_path: &Path) -> Result<Self, jj_lib::backend::BackendLoadError> {
        todo!("SqliteBackend::load not implemented")
    }
}


#[async_trait]
impl Backend for SqliteBackend {
    fn name(&self) -> &str { "sqlite" }

    fn commit_id_length(&self) -> usize { 20 }
    fn change_id_length(&self) -> usize { 16 }
    fn root_commit_id(&self) -> &CommitId { todo!() }
    fn root_change_id(&self) -> &ChangeId { todo!() }
    fn empty_tree_id(&self) -> &TreeId { todo!() }
    fn concurrency(&self) -> usize { 1 }

    async fn read_file(&self, _path: &RepoPath, _id: &FileId) -> BackendResult<Pin<Box<dyn AsyncRead + Send>>> {
        todo!("read_file")
    }
    async fn write_file(&self, _path: &RepoPath, _contents: &mut (dyn AsyncRead + Send + Unpin)) -> BackendResult<FileId> {
        todo!("write_file")
    }
    async fn read_symlink(&self, _path: &RepoPath, _id: &SymlinkId) -> BackendResult<String> {
        todo!("read_symlink")
    }
    async fn write_symlink(&self, _path: &RepoPath, _target: &str) -> BackendResult<SymlinkId> {
        todo!("write_symlink")
    }
    async fn read_copy(&self, _id: &CopyId) -> BackendResult<CopyHistory> {
        todo!("read_copy")
    }
    async fn write_copy(&self, _copy: &CopyHistory) -> BackendResult<CopyId> {
        todo!("write_copy")
    }
    async fn get_related_copies(&self, _copy_id: &CopyId) -> BackendResult<Vec<RelatedCopy>> {
        todo!("get_related_copies")
    }
    async fn read_tree(&self, _path: &RepoPath, _id: &TreeId) -> BackendResult<Tree> {
        todo!("read_tree")
    }
    async fn write_tree(&self, _path: &RepoPath, _contents: &Tree) -> BackendResult<TreeId> {
        todo!("write_tree")
    }
    async fn read_commit(&self, _id: &CommitId) -> BackendResult<Commit> {
        todo!("read_commit")
    }
    async fn write_commit(&self, _contents: Commit, _sign_with: Option<&mut SigningFn>) -> BackendResult<(CommitId, Commit)> {
        todo!("write_commit")
    }
    fn get_copy_records(&self, _paths: Option<&[RepoPathBuf]>, _root: &CommitId, _head: &CommitId) -> BackendResult<BoxStream<'_, BackendResult<CopyRecord>>> {
        todo!("get_copy_records")
    }
    fn gc(&self, _index: &dyn Index, _keep_newer: SystemTime) -> BackendResult<()> {
        Ok(())
    }
}
