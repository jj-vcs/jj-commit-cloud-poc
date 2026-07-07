// Copyright 2020 The Jujutsu Authors
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

use std::collections::BTreeMap;
use itertools::Itertools as _;
use smallvec::SmallVec;
use thiserror::Error;

use jj_lib::backend::{CommitId, MillisSinceEpoch, Timestamp};
use jj_lib::op_store::{
    Operation, OperationId, OperationMetadata, View, ViewId, RemoteView, RemoteRef,
    RemoteRefState, RefTarget, LocalRemoteRefTarget, TimestampRange,
};
use jj_lib::ref_name::{
    WorkspaceName, WorkspaceNameBuf, RefNameBuf, RemoteNameBuf, GitRefNameBuf,
    RemoteRefSymbol, RefName, RemoteName,
};
use jj_lib::merge::Merge;
use jj_lib::object_id::ObjectId;

const OPERATION_ID_LENGTH: usize = 64;
const VIEW_ID_LENGTH: usize = 64;

#[derive(Debug, Error)]
pub enum PostDecodeError {
    #[error("Invalid hash length (expected {expected} bytes, got {actual} bytes)")]
    InvalidHashLength { expected: usize, actual: usize },
    #[error("Invalid remote ref state value {0}")]
    InvalidRemoteRefStateValue(i32),
    #[error("Invalid number of ref target terms {0}")]
    EvenNumberOfRefTargetTerms(usize),
}

fn operation_id_from_proto(bytes: Vec<u8>) -> Result<OperationId, PostDecodeError> {
    if bytes.len() != OPERATION_ID_LENGTH {
        Err(PostDecodeError::InvalidHashLength {
            expected: OPERATION_ID_LENGTH,
            actual: bytes.len(),
        })
    } else {
        Ok(OperationId::new(bytes))
    }
}

fn view_id_from_proto(bytes: Vec<u8>) -> Result<ViewId, PostDecodeError> {
    if bytes.len() != VIEW_ID_LENGTH {
        Err(PostDecodeError::InvalidHashLength {
            expected: VIEW_ID_LENGTH,
            actual: bytes.len(),
        })
    } else {
        Ok(ViewId::new(bytes))
    }
}

fn timestamp_to_proto(timestamp: &Timestamp) -> jj_lib::protos::simple_op_store::Timestamp {
    jj_lib::protos::simple_op_store::Timestamp {
        millis_since_epoch: timestamp.timestamp.0,
        tz_offset: timestamp.tz_offset,
    }
}

fn timestamp_from_proto(proto: jj_lib::protos::simple_op_store::Timestamp) -> Timestamp {
    Timestamp {
        timestamp: MillisSinceEpoch(proto.millis_since_epoch),
        tz_offset: proto.tz_offset,
    }
}

fn operation_metadata_to_proto(
    metadata: &OperationMetadata,
) -> jj_lib::protos::simple_op_store::OperationMetadata {
    jj_lib::protos::simple_op_store::OperationMetadata {
        start_time: Some(timestamp_to_proto(&metadata.time.start)),
        end_time: Some(timestamp_to_proto(&metadata.time.end)),
        description: metadata.description.clone(),
        hostname: metadata.hostname.clone(),
        username: metadata.username.clone(),
        is_snapshot: metadata.is_snapshot,
        workspace_name: metadata.workspace_name.clone().map(Into::into),
        attributes: metadata
            .attributes
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    }
}

fn operation_metadata_from_proto(
    proto: jj_lib::protos::simple_op_store::OperationMetadata,
) -> OperationMetadata {
    let time = TimestampRange {
        start: timestamp_from_proto(proto.start_time.unwrap_or_default()),
        end: timestamp_from_proto(proto.end_time.unwrap_or_default()),
    };
    let workspace_name = proto.workspace_name.map(Into::into);
    OperationMetadata {
        time,
        description: proto.description,
        hostname: proto.hostname,
        username: proto.username,
        is_snapshot: proto.is_snapshot,
        workspace_name,
        attributes: proto.attributes.into_iter().collect(),
    }
}

fn commit_predecessors_map_to_proto(
    map: &BTreeMap<CommitId, Vec<CommitId>>,
) -> Vec<jj_lib::protos::simple_op_store::CommitPredecessors> {
    map.iter()
        .map(
            |(commit_id, predecessor_ids)| jj_lib::protos::simple_op_store::CommitPredecessors {
                commit_id: commit_id.to_bytes(),
                predecessor_ids: predecessor_ids.iter().map(|id| id.to_bytes()).collect(),
            },
        )
        .collect()
}

fn commit_predecessors_map_from_proto(
    proto: Vec<jj_lib::protos::simple_op_store::CommitPredecessors>,
) -> BTreeMap<CommitId, Vec<CommitId>> {
    proto
        .into_iter()
        .map(|entry| {
            let commit_id = CommitId::new(entry.commit_id);
            let predecessor_ids = entry
                .predecessor_ids
                .into_iter()
                .map(CommitId::new)
                .collect();
            (commit_id, predecessor_ids)
        })
        .collect()
}

pub fn operation_to_proto(operation: &Operation) -> jj_lib::protos::simple_op_store::Operation {
    let (commit_predecessors, stores_commit_predecessors) = match &operation.commit_predecessors {
        Some(map) => (commit_predecessors_map_to_proto(map), true),
        None => (vec![], false),
    };
    let parents = operation.parents.iter().map(|id| id.to_bytes()).collect();
    jj_lib::protos::simple_op_store::Operation {
        view_id: operation.view_id.as_bytes().to_vec(),
        parents,
        metadata: Some(operation_metadata_to_proto(&operation.metadata)),
        commit_predecessors,
        stores_commit_predecessors,
    }
}

pub fn operation_from_proto(
    proto: jj_lib::protos::simple_op_store::Operation,
) -> Result<Operation, PostDecodeError> {
    let parents = proto
        .parents
        .into_iter()
        .map(operation_id_from_proto)
        .try_collect()?;
    let view_id = view_id_from_proto(proto.view_id)?;
    let metadata = operation_metadata_from_proto(proto.metadata.unwrap_or_default());
    let commit_predecessors = proto
        .stores_commit_predecessors
        .then(|| commit_predecessors_map_from_proto(proto.commit_predecessors));
    Ok(Operation {
        view_id,
        parents,
        metadata,
        commit_predecessors,
    })
}

pub fn view_to_proto(view: &View) -> jj_lib::protos::simple_op_store::View {
    let wc_commit_ids = view
        .wc_commit_ids
        .iter()
        .map(|(name, id)| (name.into(), id.to_bytes()))
        .collect();
    let head_ids = view.head_ids.iter().map(|id| id.to_bytes()).collect();

    let bookmarks = bookmark_views_to_proto_legacy(&view.local_bookmarks, &view.remote_views);

    let local_tags = view
        .local_tags
        .iter()
        .map(|(name, target)| jj_lib::protos::simple_op_store::Tag {
            name: name.into(),
            target: ref_target_to_proto(target),
        })
        .collect();

    let remote_views = remote_views_to_proto(&view.remote_views);

    let git_refs = view
        .git_refs
        .iter()
        .map(|(name, target)| {
            #[expect(deprecated)]
            jj_lib::protos::simple_op_store::GitRef {
                name: name.into(),
                commit_id: Default::default(),
                target: ref_target_to_proto(target),
            }
        })
        .collect();

    let git_head = ref_target_to_proto(&view.git_head);

    #[expect(deprecated)]
    jj_lib::protos::simple_op_store::View {
        head_ids,
        wc_commit_id: Default::default(),
        wc_commit_ids,
        bookmarks,
        local_tags,
        remote_views,
        git_refs,
        git_head_legacy: Default::default(),
        git_head,
        // New/loaded view should have been migrated to the latest format
        has_git_refs_migrated_to_remote_tags: true,
    }
}

pub fn view_from_proto(proto: jj_lib::protos::simple_op_store::View) -> Result<View, PostDecodeError> {
    let mut wc_commit_ids = BTreeMap::new();
    #[expect(deprecated)]
    if !proto.wc_commit_id.is_empty() {
        wc_commit_ids.insert(
            WorkspaceName::DEFAULT.to_owned(),
            CommitId::new(proto.wc_commit_id),
        );
    }
    for (name, commit_id) in proto.wc_commit_ids {
        wc_commit_ids.insert(WorkspaceNameBuf::from(name), CommitId::new(commit_id));
    }
    let head_ids = proto.head_ids.into_iter().map(CommitId::new).collect();

    let (local_bookmarks, mut remote_views) = bookmark_views_from_proto_legacy(proto.bookmarks)?;

    let local_tags = proto
        .local_tags
        .into_iter()
        .map(|tag_proto| {
            let name: RefNameBuf = tag_proto.name.into();
            (name, ref_target_from_proto(tag_proto.target))
        })
        .collect();

    let git_refs: BTreeMap<_, _> = proto
        .git_refs
        .into_iter()
        .map(|git_ref| {
            let name: GitRefNameBuf = git_ref.name.into();
            let target = if git_ref.target.is_some() {
                ref_target_from_proto(git_ref.target)
            } else {
                // Legacy format
                #[expect(deprecated)]
                RefTarget::normal(CommitId::new(git_ref.commit_id))
            };
            (name, target)
        })
        .collect();

    if !proto.remote_views.is_empty() {
        remote_views = remote_views_from_proto(proto.remote_views)?;
    }

    #[cfg(feature = "git")]
    if !proto.has_git_refs_migrated_to_remote_tags {
        tracing::info!("migrating Git-tracking tags");
        let git_tags: BTreeMap<_, _> = git_refs
            .iter()
            .filter_map(|(full_name, target)| {
                let name = full_name.as_str().strip_prefix("refs/tags/")?;
                assert!(!name.is_empty());
                let name: RefNameBuf = name.into();
                let remote_ref = RemoteRef {
                    target: target.clone(),
                    state: RemoteRefState::Tracked,
                };
                Some((name, remote_ref))
            })
            .collect();
        if !git_tags.is_empty() {
            let git_view = remote_views
                .entry(jj_lib::git::REMOTE_NAME_FOR_LOCAL_GIT_REPO.to_owned())
                .or_default();
            assert!(git_view.tags.is_empty());
            git_view.tags = git_tags;
        }
    }

    #[expect(deprecated)]
    let git_head = if proto.git_head.is_some() {
        ref_target_from_proto(proto.git_head)
    } else if !proto.git_head_legacy.is_empty() {
        RefTarget::normal(CommitId::new(proto.git_head_legacy))
    } else {
        RefTarget::absent()
    };

    Ok(View {
        head_ids,
        local_bookmarks,
        local_tags,
        remote_views,
        git_refs,
        git_head,
        wc_commit_ids,
    })
}

fn bookmark_views_to_proto_legacy(
    local_bookmarks: &BTreeMap<RefNameBuf, RefTarget>,
    remote_views: &BTreeMap<RemoteNameBuf, RemoteView>,
) -> Vec<jj_lib::protos::simple_op_store::Bookmark> {
    merge_join_ref_views(local_bookmarks, remote_views, |view| &view.bookmarks)
        .map(|(name, bookmark_target)| {
            let local_target = ref_target_to_proto(bookmark_target.local_target);
            let remote_bookmarks = bookmark_target
                .remote_refs
                .iter()
                .map(
                    |&(remote_name, remote_ref)| jj_lib::protos::simple_op_store::RemoteBookmark {
                        remote_name: remote_name.into(),
                        target: ref_target_to_proto(&remote_ref.target),
                        state: Some(remote_ref_state_to_proto(remote_ref.state)),
                    },
                )
                .collect();
            #[expect(deprecated)]
            jj_lib::protos::simple_op_store::Bookmark {
                name: name.into(),
                local_target,
                remote_bookmarks,
            }
        })
        .collect()
}

type BookmarkViews = (
    BTreeMap<RefNameBuf, RefTarget>,
    BTreeMap<RemoteNameBuf, RemoteView>,
);

fn bookmark_views_from_proto_legacy(
    bookmarks_legacy: Vec<jj_lib::protos::simple_op_store::Bookmark>,
) -> Result<BookmarkViews, PostDecodeError> {
    let mut local_bookmarks: BTreeMap<RefNameBuf, RefTarget> = BTreeMap::new();
    let mut remote_views: BTreeMap<RemoteNameBuf, RemoteView> = BTreeMap::new();
    for bookmark_proto in bookmarks_legacy {
        let bookmark_name: RefNameBuf = bookmark_proto.name.into();
        let local_target = ref_target_from_proto(bookmark_proto.local_target);
        #[expect(deprecated)]
        let remote_bookmarks = bookmark_proto.remote_bookmarks;
        for remote_bookmark in remote_bookmarks {
            let remote_name: RemoteNameBuf = remote_bookmark.remote_name.into();
            let state = match remote_bookmark.state {
                Some(n) => remote_ref_state_from_proto(n)?,
                None => RemoteRefState::New,
            };
            let remote_view = remote_views.entry(remote_name).or_default();
            let remote_ref = RemoteRef {
                target: ref_target_from_proto(remote_bookmark.target),
                state,
            };
            remote_view
                .bookmarks
                .insert(bookmark_name.clone(), remote_ref);
        }
        if local_target.is_present() {
            local_bookmarks.insert(bookmark_name, local_target);
        }
    }
    Ok((local_bookmarks, remote_views))
}

fn remote_views_to_proto(
    remote_views: &BTreeMap<RemoteNameBuf, RemoteView>,
) -> Vec<jj_lib::protos::simple_op_store::RemoteView> {
    remote_views
        .iter()
        .map(|(name, view)| jj_lib::protos::simple_op_store::RemoteView {
            name: name.into(),
            bookmarks: remote_refs_to_proto(&view.bookmarks),
            tags: remote_refs_to_proto(&view.tags),
        })
        .collect()
}

fn remote_views_from_proto(
    remote_views_proto: Vec<jj_lib::protos::simple_op_store::RemoteView>,
) -> Result<BTreeMap<RemoteNameBuf, RemoteView>, PostDecodeError> {
    remote_views_proto
        .into_iter()
        .map(|proto| {
            let name: RemoteNameBuf = proto.name.into();
            let view = RemoteView {
                bookmarks: remote_refs_from_proto(proto.bookmarks)?,
                tags: remote_refs_from_proto(proto.tags)?,
            };
            Ok((name, view))
        })
        .collect()
}

fn remote_refs_to_proto(
    remote_refs: &BTreeMap<RefNameBuf, RemoteRef>,
) -> Vec<jj_lib::protos::simple_op_store::RemoteRef> {
    remote_refs
        .iter()
        .map(
            |(name, remote_ref)| jj_lib::protos::simple_op_store::RemoteRef {
                name: name.into(),
                target_terms: ref_target_to_terms_proto(&remote_ref.target),
                state: remote_ref_state_to_proto(remote_ref.state),
            },
        )
        .collect()
}

fn remote_refs_from_proto(
    remote_refs_proto: Vec<jj_lib::protos::simple_op_store::RemoteRef>,
) -> Result<BTreeMap<RefNameBuf, RemoteRef>, PostDecodeError> {
    remote_refs_proto
        .into_iter()
        .map(|proto| {
            let name: RefNameBuf = proto.name.into();
            let remote_ref = RemoteRef {
                target: ref_target_from_terms_proto(proto.target_terms)?,
                state: remote_ref_state_from_proto(proto.state)?,
            };
            Ok((name, remote_ref))
        })
        .collect()
}

fn ref_target_to_terms_proto(
    value: &RefTarget,
) -> Vec<jj_lib::protos::simple_op_store::RefTargetTerm> {
    value
        .as_merge()
        .iter()
        .map(|term| term.as_ref().map(|id| id.to_bytes()))
        .map(|value| jj_lib::protos::simple_op_store::RefTargetTerm { value })
        .collect()
}

fn ref_target_from_terms_proto(
    proto: Vec<jj_lib::protos::simple_op_store::RefTargetTerm>,
) -> Result<RefTarget, PostDecodeError> {
    let terms: SmallVec<[_; 1]> = proto
        .into_iter()
        .map(|jj_lib::protos::simple_op_store::RefTargetTerm { value }| value.map(CommitId::new))
        .collect();
    if terms.len() % 2 == 0 {
        Err(PostDecodeError::EvenNumberOfRefTargetTerms(terms.len()))
    } else {
        Ok(RefTarget::from_merge(Merge::from_vec(terms)))
    }
}

fn ref_target_to_proto(value: &RefTarget) -> Option<jj_lib::protos::simple_op_store::RefTarget> {
    let term_to_proto =
        |term: &Option<CommitId>| jj_lib::protos::simple_op_store::ref_conflict::Term {
            value: term.as_ref().map(|id| id.to_bytes()),
        };
    let merge = value.as_merge();
    let conflict_proto = jj_lib::protos::simple_op_store::RefConflict {
        removes: merge.removes().map(term_to_proto).collect(),
        adds: merge.adds().map(term_to_proto).collect(),
    };
    let proto = jj_lib::protos::simple_op_store::RefTarget {
        value: Some(jj_lib::protos::simple_op_store::ref_target::Value::Conflict(
            conflict_proto,
        )),
    };
    Some(proto)
}

fn ref_target_from_proto(
    maybe_proto: Option<jj_lib::protos::simple_op_store::RefTarget>,
) -> RefTarget {
    let Some(proto) = maybe_proto else {
        return RefTarget::absent();
    };
    match proto.value.unwrap() {
        #[expect(deprecated)]
        jj_lib::protos::simple_op_store::ref_target::Value::CommitId(id) => {
            RefTarget::normal(CommitId::new(id))
        }
        #[expect(deprecated)]
        jj_lib::protos::simple_op_store::ref_target::Value::ConflictLegacy(conflict) => {
            let removes = conflict.removes.into_iter().map(CommitId::new);
            let adds = conflict.adds.into_iter().map(CommitId::new);
            RefTarget::from_legacy_form(removes, adds)
        }
        jj_lib::protos::simple_op_store::ref_target::Value::Conflict(conflict) => {
            let term_from_proto = |term: jj_lib::protos::simple_op_store::ref_conflict::Term| {
                term.value.map(CommitId::new)
            };
            let removes = conflict.removes.into_iter().map(term_from_proto);
            let adds = conflict.adds.into_iter().map(term_from_proto);
            RefTarget::from_merge(Merge::from_removes_adds(removes, adds))
        }
    }
}

fn remote_ref_state_to_proto(state: RemoteRefState) -> i32 {
    let proto_state = match state {
        RemoteRefState::New => jj_lib::protos::simple_op_store::RemoteRefState::New,
        RemoteRefState::Tracked => jj_lib::protos::simple_op_store::RemoteRefState::Tracked,
    };
    proto_state as i32
}

fn remote_ref_state_from_proto(proto_value: i32) -> Result<RemoteRefState, PostDecodeError> {
    let proto_state = proto_value
        .try_into()
        .map_err(|prost::UnknownEnumValue(n)| PostDecodeError::InvalidRemoteRefStateValue(n))?;
    let state = match proto_state {
        jj_lib::protos::simple_op_store::RemoteRefState::New => RemoteRefState::New,
        jj_lib::protos::simple_op_store::RemoteRefState::Tracked => RemoteRefState::Tracked,
    };
    Ok(state)
}

pub fn merge_join_ref_views<'a>(
    local_refs: &'a BTreeMap<RefNameBuf, RefTarget>,
    remote_views: &'a BTreeMap<RemoteNameBuf, RemoteView>,
    get_remote_refs: impl FnMut(&RemoteView) -> &BTreeMap<RefNameBuf, RemoteRef>,
) -> impl Iterator<Item = (&'a RefName, LocalRemoteRefTarget<'a>)> {
    let mut local_refs_iter = local_refs
        .iter()
        .map(|(name, target)| (&**name, target))
        .peekable();
    let mut remote_refs_iter = flatten_remote_refs(remote_views, get_remote_refs).peekable();

    std::iter::from_fn(move || {
        let (name, local_target) = if let Some((symbol, _)) = remote_refs_iter.peek() {
            local_refs_iter
                .next_if(|&(local_name, _)| local_name <= symbol.name)
                .unwrap_or((symbol.name, RefTarget::absent_ref()))
        } else {
            local_refs_iter.next()?
        };
        let remote_refs = remote_refs_iter
            .peeking_take_while(|(symbol, _)| symbol.name == name)
            .map(|(symbol, remote_ref)| (symbol.remote, remote_ref))
            .collect();
        let local_remote_target = LocalRemoteRefTarget {
            local_target,
            remote_refs,
        };
        Some((name, local_remote_target))
    })
}

pub fn flatten_remote_refs(
    remote_views: &BTreeMap<RemoteNameBuf, RemoteView>,
    mut get_remote_refs: impl FnMut(&RemoteView) -> &BTreeMap<RefNameBuf, RemoteRef>,
) -> impl Iterator<Item = (RemoteRefSymbol<'_>, &RemoteRef)> {
    remote_views
        .iter()
        .map(|(remote, remote_view)| {
            get_remote_refs(remote_view)
                .iter()
                .map(move |(name, remote_ref)| (name.to_remote_symbol(remote), remote_ref))
        })
        .kmerge_by(|(symbol1, _), (symbol2, _)| symbol1 < symbol2)
}
