#![allow(elided_lifetimes_in_paths)]

use std::fmt::{self, Debug};
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use futures::stream::{self, BoxStream, LocalBoxStream};
use futures::StreamExt as _;
use jj_lib::backend::*;
use jj_lib::commit::Commit as JjCommit;
use jj_lib::graph::GraphNode;
use jj_lib::index::{
    ChangeIdIndex, Index, IndexError, IndexResult, IndexStore, IndexStoreResult, MutableIndex,
    ReadonlyIndex, ResolvedChangeTargets,
};
use jj_lib::object_id::{HexPrefix, ObjectId, PrefixResolution};
use jj_lib::op_heads_store::{OpHeadsStore, OpHeadsStoreError, OpHeadsStoreLock};
use jj_lib::op_store::{
    OpStore, OpStoreError, OpStoreResult, Operation, OperationId, View, ViewId,
};
use jj_lib::operation::Operation as JjOperation;
use jj_lib::repo_path::{RepoPath, RepoPathBuf};
use jj_lib::revset::{ResolvedExpression, Revset, RevsetContainingFn, RevsetEvaluationError};
use jj_lib::store::Store as JjStore;
use jj_lib::submodule_store::SubmoduleStore;

use crate::hash_utils::{
    compute_git_blob_hash, compute_git_commit_hash, compute_git_tree_hash, hash_operation,
    hash_view,
};
use crate::store::Store;
use cc_common::conversions::backend::{
    commit_from_proto, commit_to_proto, tree_entry_from_proto, tree_entry_to_proto,
};
use cc_common::conversions::op_store::{
    operation_from_proto, operation_to_proto, view_from_proto, view_to_proto,
};

pub struct ServerBackend {
    store: Arc<dyn Store>,
    repo_id: String,
    root_commit_id: CommitId,
    root_change_id: ChangeId,
    empty_tree_id: TreeId,
}

impl Debug for ServerBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerBackend")
            .field("repo_id", &self.repo_id)
            .finish()
    }
}

impl ServerBackend {
    pub fn new(store: Arc<dyn Store>, repo_id: String) -> Self {
        let root_commit_id = CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES);
        let root_change_id = ChangeId::from_bytes(&cc_common::ROOT_CHANGE_ID_BYTES);
        let empty_tree_id = TreeId::from_hex(cc_common::EMPTY_TREE_ID_HEX);
        Self {
            store,
            repo_id,
            root_commit_id,
            root_change_id,
            empty_tree_id,
        }
    }
}

#[async_trait]
impl Backend for ServerBackend {
    fn name(&self) -> &str {
        "server_backend"
    }

    fn commit_id_length(&self) -> usize {
        cc_common::COMMIT_ID_LENGTH
    }

    fn change_id_length(&self) -> usize {
        cc_common::CHANGE_ID_LENGTH
    }

    fn root_commit_id(&self) -> &CommitId {
        &self.root_commit_id
    }

    fn root_change_id(&self) -> &ChangeId {
        &self.root_change_id
    }

    fn empty_tree_id(&self) -> &TreeId {
        &self.empty_tree_id
    }

    fn concurrency(&self) -> usize {
        1
    }

    async fn read_file(
        &self,
        _path: &RepoPath,
        id: &FileId,
    ) -> BackendResult<Pin<Box<dyn futures::AsyncRead + Send>>> {
        let file_id_bytes = id.to_bytes().to_vec();
        let content = self
            .store
            .get_file(&self.repo_id, &file_id_bytes)
            .await
            .map_err(|e| BackendError::Other(e.into()))?
            .ok_or_else(|| BackendError::ObjectNotFound {
                object_type: "file".into(),
                hash: id.hex(),
                source: "file not found in server store".into(),
            })?;
        Ok(Box::pin(futures::io::Cursor::new(content)))
    }

    async fn write_file(
        &self,
        _path: &RepoPath,
        contents: &mut (dyn futures::AsyncRead + Send + Unpin),
    ) -> BackendResult<FileId> {
        let mut buffer = Vec::new();
        futures::AsyncReadExt::read_to_end(contents, &mut buffer)
            .await
            .map_err(|e| BackendError::Other(e.into()))?;
        let file_id_bytes = compute_git_blob_hash(&buffer);
        self.store
            .put_file(self.repo_id.clone(), file_id_bytes.clone(), buffer)
            .await
            .map_err(|e| BackendError::Other(e.into()))?;
        Ok(FileId::from_bytes(&file_id_bytes))
    }

    async fn read_symlink(&self, _path: &RepoPath, _id: &SymlinkId) -> BackendResult<String> {
        Err(BackendError::Unsupported("read_symlink not supported".to_string()))
    }

    async fn write_symlink(&self, _path: &RepoPath, _target: &str) -> BackendResult<SymlinkId> {
        Err(BackendError::Unsupported("write_symlink not supported".to_string()))
    }

    async fn read_copy(&self, _id: &CopyId) -> BackendResult<CopyHistory> {
        Err(BackendError::Unsupported("copies not supported".to_string()))
    }

    async fn write_copy(&self, _contents: &CopyHistory) -> BackendResult<CopyId> {
        Err(BackendError::Unsupported("copies not supported".to_string()))
    }

    async fn get_related_copies(&self, _copy_id: &CopyId) -> BackendResult<Vec<RelatedCopy>> {
        Err(BackendError::Unsupported("copies not supported".to_string()))
    }

    async fn read_tree(&self, _path: &RepoPath, id: &TreeId) -> BackendResult<Tree> {
        if *id == self.empty_tree_id {
            return Ok(Tree::from_sorted_entries(vec![]));
        }

        let proto_entries = self
            .store
            .get_tree(&self.repo_id, id.as_bytes())
            .await
            .map_err(|e| BackendError::Other(e.into()))?
            .ok_or_else(|| BackendError::ObjectNotFound {
                object_type: "tree".into(),
                hash: id.hex(),
                source: "tree not found in server store".into(),
            })?;

        let mut jj_entries = Vec::new();
        for entry in proto_entries {
            let (comp, val) =
                tree_entry_from_proto(entry).map_err(|e| BackendError::Other(e.into()))?;
            jj_entries.push((comp, val));
        }

        Ok(Tree::from_sorted_entries(jj_entries))
    }

    async fn write_tree(&self, _path: &RepoPath, tree: &Tree) -> BackendResult<TreeId> {
        if tree.entries().next().is_none() {
            return Ok(self.empty_tree_id.clone());
        }

        let proto_entries: Result<Vec<_>, _> =
            tree.entries().map(|e| tree_entry_to_proto(&e)).collect();
        let proto_entries = proto_entries?;
        let tree_id_bytes = compute_git_tree_hash(&proto_entries);

        self.store
            .put_tree(self.repo_id.clone(), tree_id_bytes.clone(), proto_entries)
            .await
            .map_err(|e| BackendError::Other(e.into()))?;

        Ok(TreeId::from_bytes(&tree_id_bytes))
    }

    async fn read_commit(&self, id: &CommitId) -> BackendResult<Commit> {
        if *id == self.root_commit_id {
            return Ok(make_root_commit(
                self.root_change_id().clone(),
                self.empty_tree_id.clone(),
            ));
        }

        let proto_commit = self
            .store
            .get_commit(&self.repo_id, id.as_bytes())
            .await
            .map_err(|e| BackendError::Other(e.into()))?
            .ok_or_else(|| BackendError::ObjectNotFound {
                object_type: "commit".into(),
                hash: id.hex(),
                source: "commit not found in server store".into(),
            })?;

        Ok(commit_from_proto(proto_commit))
    }

    async fn write_commit(
        &self,
        commit: Commit,
        _sign_with: Option<&mut SigningFn>,
    ) -> BackendResult<(CommitId, Commit)> {
        let proto_commit = commit_to_proto(&commit);
        let commit_id_bytes = compute_git_commit_hash(&proto_commit);
        self.store
            .put_commit(
                self.repo_id.clone(),
                commit_id_bytes.clone(),
                proto_commit,
            )
            .await
            .map_err(|e| BackendError::Other(e.into()))?;
        Ok((CommitId::from_bytes(&commit_id_bytes), commit))
    }

    fn get_copy_records(
        &self,
        _paths: Option<&[RepoPathBuf]>,
        _root: &CommitId,
        _head: &CommitId,
    ) -> BackendResult<BoxStream<'_, BackendResult<CopyRecord>>> {
        Ok(stream::empty().boxed())
    }

    fn gc(&self, _index: &dyn Index, _keep_newer: SystemTime) -> BackendResult<()> {
        Ok(())
    }
}

pub struct ServerOpStore {
    store: Arc<dyn Store>,
    repo_id: String,
    root_operation_id: OperationId,
}

impl Debug for ServerOpStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerOpStore")
            .field("repo_id", &self.repo_id)
            .finish()
    }
}

impl ServerOpStore {
    pub fn new(store: Arc<dyn Store>, repo_id: String) -> Self {
        Self {
            store,
            repo_id,
            root_operation_id: OperationId::from_bytes(&cc_common::ROOT_OPERATION_ID_BYTES),
        }
    }
}

#[async_trait]
impl OpStore for ServerOpStore {
    fn name(&self) -> &str {
        "server_op_store"
    }

    fn root_operation_id(&self) -> &OperationId {
        &self.root_operation_id
    }

    async fn read_operation(&self, id: &OperationId) -> OpStoreResult<Operation> {
        if id.as_bytes() == cc_common::ROOT_OPERATION_ID_BYTES {
            let root_op = cc_common::op_store::Operation {
                view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
                parents: vec![],
                metadata: Some(cc_common::op_store::OperationMetadata {
                    start_time_millis: 0,
                    end_time_millis: 0,
                    description: "root()".to_string(),
                    hostname: "".to_string(),
                    username: "".to_string(),
                    is_snapshot: false,
                    workspace_name: None,
                    attributes: std::collections::HashMap::new(),
                }),
                commit_predecessors: vec![],
                commit_predecessors_set: true,
            };
            return Ok(operation_from_proto(root_op));
        }

        let op = self
            .store
            .get_operation(&self.repo_id, id.as_bytes())
            .await
            .map_err(|e| OpStoreError::Other(e.into()))?
            .ok_or_else(|| OpStoreError::ObjectNotFound {
                object_type: "operation".into(),
                hash: id.hex(),
                source: "operation not found in server store".into(),
            })?;

        Ok(operation_from_proto(op))
    }

    async fn write_operation(&self, operation: &Operation) -> OpStoreResult<OperationId> {
        let op_proto = operation_to_proto(operation);
        let op_id_bytes = hash_operation(&op_proto);

        self.store
            .put_operation(self.repo_id.clone(), op_id_bytes.clone(), op_proto)
            .await
            .map_err(|e| OpStoreError::Other(e.into()))?;

        Ok(OperationId::from_bytes(&op_id_bytes))
    }

    async fn read_view(&self, id: &ViewId) -> OpStoreResult<View> {
        if id.as_bytes() == cc_common::ROOT_VIEW_ID_BYTES {
            let root_view = cc_common::op_store::View {
                head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                wc_commit_ids: std::collections::HashMap::new(),
                local_bookmarks: std::collections::HashMap::new(),
                remote_bookmarks: std::collections::HashMap::new(),
            };
            return Ok(view_from_proto(root_view));
        }

        let view = self
            .store
            .get_view(&self.repo_id, id.as_bytes())
            .await
            .map_err(|e| OpStoreError::Other(e.into()))?
            .ok_or_else(|| OpStoreError::ObjectNotFound {
                object_type: "view".into(),
                hash: id.hex(),
                source: "view not found in server store".into(),
            })?;

        Ok(view_from_proto(view))
    }

    async fn write_view(&self, view: &View) -> OpStoreResult<ViewId> {
        let view_proto = view_to_proto(view);
        let view_id_bytes = hash_view(&view_proto);

        self.store
            .put_view(self.repo_id.clone(), view_id_bytes.clone(), view_proto)
            .await
            .map_err(|e| OpStoreError::Other(e.into()))?;

        Ok(ViewId::from_bytes(&view_id_bytes))
    }

    async fn resolve_operation_id_prefix(
        &self,
        prefix: &HexPrefix,
    ) -> OpStoreResult<PrefixResolution<OperationId>> {
        if prefix.matches(&self.root_operation_id) {
            Ok(PrefixResolution::SingleMatch(
                self.root_operation_id.clone(),
            ))
        } else {
            Ok(PrefixResolution::NoMatch)
        }
    }

    async fn gc(&self, _target_op_ids: &[OperationId], _keep_newer: SystemTime) -> OpStoreResult<()> {
        Ok(())
    }
}

pub struct ServerOpHeadsStore {
    store: Arc<dyn Store>,
    repo_id: String,
}

impl Debug for ServerOpHeadsStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerOpHeadsStore")
            .field("repo_id", &self.repo_id)
            .finish()
    }
}

pub struct ServerOpHeadsStoreNoLock;
impl OpHeadsStoreLock for ServerOpHeadsStoreNoLock {}

impl ServerOpHeadsStore {
    pub fn new(store: Arc<dyn Store>, repo_id: String) -> Self {
        Self { store, repo_id }
    }
}

#[async_trait]
impl OpHeadsStore for ServerOpHeadsStore {
    fn name(&self) -> &str {
        "server_op_heads_store"
    }

    async fn update_op_heads(
        &self,
        old_ids: &[OperationId],
        new_id: &OperationId,
    ) -> Result<(), OpHeadsStoreError> {
        let old_vec: Vec<Vec<u8>> = old_ids.iter().map(|id| id.as_bytes().to_vec()).collect();
        let new_vec = new_id.as_bytes().to_vec();
        let target_id = new_id.clone();

        self.store
            .update_op_heads_compare_and_swap(
                self.repo_id.clone(),
                &old_vec,
                new_vec,
            )
            .await
            .map_err(|e| OpHeadsStoreError::Write {
                new_op_id: target_id,
                source: e.into(),
            })?;

        Ok(())
    }

    async fn get_op_heads(&self) -> Result<Vec<OperationId>, OpHeadsStoreError> {
        let heads = self
            .store
            .get_op_heads(&self.repo_id)
            .await
            .map_err(|e| OpHeadsStoreError::Read(e.into()))?;

        let head_ids: Vec<OperationId> = heads
            .unwrap_or_else(|| vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()])
            .into_iter()
            .map(|b| OperationId::from_bytes(&b))
            .collect();

        Ok(head_ids)
    }

    async fn lock(&self) -> Result<Box<dyn OpHeadsStoreLock + '_>, OpHeadsStoreError> {
        Ok(Box::new(ServerOpHeadsStoreNoLock))
    }
}

#[derive(Debug)]
pub struct EmptyRevset;

impl Revset for EmptyRevset {
    fn stream<'a>(&self) -> LocalBoxStream<'a, Result<CommitId, RevsetEvaluationError>>
    where
        Self: 'a,
    {
        Box::pin(futures::stream::empty())
    }

    fn commit_change_ids<'a>(
        &self,
    ) -> LocalBoxStream<'a, Result<(CommitId, ChangeId), RevsetEvaluationError>>
    where
        Self: 'a,
    {
        Box::pin(futures::stream::empty())
    }

    fn stream_graph<'a>(
        &self,
    ) -> LocalBoxStream<'a, Result<GraphNode<CommitId>, RevsetEvaluationError>>
    where
        Self: 'a,
    {
        Box::pin(futures::stream::empty())
    }

    fn is_empty(&self) -> bool {
        true
    }

    fn count_estimate(&self) -> Result<(usize, Option<usize>), RevsetEvaluationError> {
        Ok((0, Some(0)))
    }

    fn containing_fn<'a>(&self) -> Box<RevsetContainingFn<'a>>
    where
        Self: 'a,
    {
        Box::new(|_id| Ok(false))
    }
}

/// In-memory implementation of [`Index`], [`ReadonlyIndex`], and [`MutableIndex`]
/// for server-side operations that only manage operation graph metadata.
#[derive(Clone, Debug, Default)]
pub struct ServerIndex;

impl Index for ServerIndex {
    fn shortest_unique_commit_id_prefix_len(
        &self,
        _commit_id: &CommitId,
    ) -> IndexResult<usize> {
        Ok(12)
    }

    fn resolve_commit_id_prefix(
        &self,
        _prefix: &HexPrefix,
    ) -> IndexResult<PrefixResolution<CommitId>> {
        Ok(PrefixResolution::NoMatch)
    }

    fn has_id(&self, _commit_id: &CommitId) -> IndexResult<bool> {
        Ok(false)
    }

    fn is_ancestor(
        &self,
        ancestor_id: &CommitId,
        descendant_id: &CommitId,
    ) -> IndexResult<bool> {
        Ok(ancestor_id == descendant_id)
    }

    fn common_ancestors(
        &self,
        _set1: &[CommitId],
        _set2: &[CommitId],
    ) -> IndexResult<Vec<CommitId>> {
        Ok(vec![])
    }

    fn all_heads_for_gc(&self) -> IndexResult<Box<dyn Iterator<Item = CommitId> + '_>> {
        Err(IndexError::AllHeadsForGcUnsupported)
    }

    fn heads(
        &self,
        candidates: &mut dyn Iterator<Item = &CommitId>,
    ) -> IndexResult<Vec<CommitId>> {
        Ok(candidates.cloned().collect())
    }

    fn changed_paths_in_commit(
        &self,
        _commit_id: &CommitId,
    ) -> IndexResult<Option<Box<dyn Iterator<Item = RepoPathBuf> + '_>>> {
        Ok(None)
    }

    fn evaluate_revset(
        &self,
        _expression: &ResolvedExpression,
        _store: &Arc<JjStore>,
    ) -> Result<Box<dyn Revset + '_>, RevsetEvaluationError> {
        Ok(Box::new(EmptyRevset))
    }
}

pub struct ServerChangeIdIndex;

impl ChangeIdIndex for ServerChangeIdIndex {
    fn resolve_prefix(
        &self,
        _prefix: &HexPrefix,
    ) -> IndexResult<PrefixResolution<ResolvedChangeTargets>> {
        Ok(PrefixResolution::NoMatch)
    }

    fn shortest_unique_prefix_len(&self, _change_id: &ChangeId) -> IndexResult<usize> {
        Ok(12)
    }
}

impl ReadonlyIndex for ServerIndex {
    fn as_index(&self) -> &dyn Index {
        self
    }

    fn change_id_index(
        &self,
        _heads: &mut dyn Iterator<Item = &CommitId>,
    ) -> Box<dyn ChangeIdIndex> {
        Box::new(ServerChangeIdIndex)
    }

    fn start_modification(&self) -> Box<dyn MutableIndex> {
        Box::new(self.clone())
    }
}

#[async_trait]
impl MutableIndex for ServerIndex {
    fn as_index(&self) -> &dyn Index {
        self
    }

    fn change_id_index(
        &self,
        _heads: &mut dyn Iterator<Item = &CommitId>,
    ) -> Box<dyn ChangeIdIndex + '_> {
        Box::new(ServerChangeIdIndex)
    }

    async fn add_commit(&mut self, _commit: &JjCommit) -> IndexResult<()> {
        Ok(())
    }

    fn merge_in(&mut self, _other: &dyn ReadonlyIndex) -> IndexResult<()> {
        Ok(())
    }
}

/// In-memory [`IndexStore`] for server-side operations with no filesystem dependencies.
#[derive(Debug, Default)]
pub struct ServerIndexStore;

#[async_trait(?Send)]
impl IndexStore for ServerIndexStore {
    fn name(&self) -> &str {
        "server"
    }

    async fn get_index_at_op(
        &self,
        _op: &JjOperation,
        _store: &Arc<JjStore>,
    ) -> IndexStoreResult<Box<dyn ReadonlyIndex>> {
        Ok(Box::new(ServerIndex))
    }

    fn write_index(
        &self,
        _index: Box<dyn MutableIndex>,
        _op: &JjOperation,
    ) -> IndexStoreResult<Box<dyn ReadonlyIndex>> {
        Ok(Box::new(ServerIndex))
    }
}

/// In-memory [`SubmoduleStore`] for server-side operations with no filesystem dependencies.
#[derive(Debug, Default)]
pub struct ServerSubmoduleStore;

impl SubmoduleStore for ServerSubmoduleStore {
    fn name(&self) -> &str {
        "server"
    }
}
