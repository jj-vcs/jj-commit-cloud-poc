use std::collections::{BTreeMap, HashMap, HashSet};

use jj_lib::backend::{CommitId, MillisSinceEpoch, Timestamp};
use jj_lib::object_id::ObjectId;
use jj_lib::op_store::{
    Operation, OperationId, OperationMetadata, RefTarget, RemoteRef, RemoteRefState, RemoteView,
    TimestampRange, View, ViewId,
};
use jj_lib::ref_name::{RefNameBuf, RemoteNameBuf, WorkspaceNameBuf};

use crate::op_store as pb;

pub fn ref_target_to_proto(target: &RefTarget) -> pb::RefTarget {
    let removes = target
        .removed_ids()
        .map(|id| pb::RefTargetTerm {
            commit_id: id.as_bytes().to_vec(),
        })
        .collect();
    let adds = target
        .added_ids()
        .map(|id| pb::RefTargetTerm {
            commit_id: id.as_bytes().to_vec(),
        })
        .collect();
    pb::RefTarget { removes, adds }
}

pub fn ref_target_from_proto(target: &pb::RefTarget) -> RefTarget {
    let removed_ids = target
        .removes
        .iter()
        .map(|t| CommitId::from_bytes(&t.commit_id));
    let added_ids = target
        .adds
        .iter()
        .map(|t| CommitId::from_bytes(&t.commit_id));
    RefTarget::from_legacy_form(removed_ids, added_ids)
}

pub fn operation_to_proto(op: &Operation) -> pb::Operation {
    let metadata = pb::OperationMetadata {
        start_time_millis: op.metadata.time.start.timestamp.0,
        end_time_millis: op.metadata.time.end.timestamp.0,
        description: op.metadata.description.clone(),
        hostname: op.metadata.hostname.clone(),
        username: op.metadata.username.clone(),
        is_snapshot: op.metadata.is_snapshot,
        workspace_name: op
            .metadata
            .workspace_name
            .as_ref()
            .map(|w| w.as_str().to_string()),
        attributes: op
            .metadata
            .attributes
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    };
    let commit_predecessors = match &op.commit_predecessors {
        Some(map) => map
            .iter()
            .map(|(k, v)| pb::CommitPredecessors {
                commit_id: k.as_bytes().to_vec(),
                predecessor_ids: v.iter().map(|id| id.as_bytes().to_vec()).collect(),
            })
            .collect(),
        None => vec![],
    };

    pb::Operation {
        view_id: op.view_id.as_bytes().to_vec(),
        parents: op.parents.iter().map(|p| p.as_bytes().to_vec()).collect(),
        metadata: Some(metadata),
        commit_predecessors,
        commit_predecessors_set: op.commit_predecessors.is_some(),
    }
}

pub fn operation_from_proto(mut op: pb::Operation) -> Operation {
    let meta = op.metadata.take().unwrap_or_default();
    let start_ts = Timestamp {
        timestamp: MillisSinceEpoch(meta.start_time_millis),
        tz_offset: 0,
    };
    let end_ts = Timestamp {
        timestamp: MillisSinceEpoch(meta.end_time_millis),
        tz_offset: 0,
    };
    let metadata = OperationMetadata {
        time: TimestampRange {
            start: start_ts,
            end: end_ts,
        },
        description: meta.description,
        hostname: meta.hostname,
        username: meta.username,
        is_snapshot: meta.is_snapshot,
        workspace_name: meta.workspace_name.map(WorkspaceNameBuf::from),
        attributes: meta.attributes.into_iter().collect(),
    };
    let commit_predecessors = if op.commit_predecessors_set {
        let mut map = BTreeMap::new();
        for p in op.commit_predecessors {
            let key = CommitId::from_bytes(&p.commit_id);
            let val = p
                .predecessor_ids
                .iter()
                .map(|id| CommitId::from_bytes(id))
                .collect();
            map.insert(key, val);
        }
        Some(map)
    } else {
        None
    };

    Operation {
        view_id: ViewId::from_bytes(&op.view_id),
        parents: op
            .parents
            .iter()
            .map(|p| OperationId::from_bytes(p))
            .collect(),
        metadata,
        commit_predecessors,
    }
}

pub fn view_to_proto(view: &View) -> pb::View {
    let head_ids = view
        .head_ids
        .iter()
        .map(|id| id.as_bytes().to_vec())
        .collect();
    let wc_commit_ids = view
        .wc_commit_ids
        .iter()
        .map(|(k, v)| (k.as_str().to_string(), v.as_bytes().to_vec()))
        .collect();
    let local_bookmarks = view
        .local_bookmarks
        .iter()
        .map(|(k, v)| (k.as_str().to_string(), ref_target_to_proto(v)))
        .collect();
    let mut remote_bookmarks = HashMap::new();
    for (remote_name, remote_view) in &view.remote_views {
        for (bookmark_name, remote_ref) in &remote_view.bookmarks {
            let key = format!("{}@{}", bookmark_name.as_str(), remote_name.as_str());
            remote_bookmarks.insert(
                key,
                pb::RemoteRef {
                    target: Some(ref_target_to_proto(&remote_ref.target)),
                    is_tracked: remote_ref.state == RemoteRefState::Tracked,
                },
            );
        }
    }

    pb::View {
        head_ids,
        wc_commit_ids,
        local_bookmarks,
        remote_bookmarks,
    }
}

pub fn view_from_proto(view: pb::View) -> View {
    let head_ids = view
        .head_ids
        .iter()
        .map(|id| CommitId::from_bytes(id))
        .collect::<HashSet<_>>();
    let wc_commit_ids = view
        .wc_commit_ids
        .into_iter()
        .map(|(k, v)| (WorkspaceNameBuf::from(k), CommitId::from_bytes(&v)))
        .collect();
    let local_bookmarks = view
        .local_bookmarks
        .iter()
        .map(|(k, v)| (RefNameBuf::from(k.as_str()), ref_target_from_proto(v)))
        .collect();
    let mut remote_views: BTreeMap<RemoteNameBuf, RemoteView> = BTreeMap::new();
    for (key, remote_ref_proto) in &view.remote_bookmarks {
        let remote_ref = RemoteRef {
            target: remote_ref_proto
                .target
                .as_ref()
                .map(ref_target_from_proto)
                .unwrap_or_else(RefTarget::absent),
            state: if remote_ref_proto.is_tracked {
                RemoteRefState::Tracked
            } else {
                RemoteRefState::New
            },
        };
        if let Some((b_str, r_str)) = key.rsplit_once('@') {
            let r_name = RemoteNameBuf::from(r_str);
            let b_name = RefNameBuf::from(b_str);
            remote_views
                .entry(r_name)
                .or_default()
                .bookmarks
                .insert(b_name, remote_ref);
        }
    }

    View {
        head_ids,
        wc_commit_ids,
        local_bookmarks,
        local_tags: BTreeMap::new(),
        remote_views,
        git_refs: BTreeMap::new(),
        git_head: RefTarget::absent(),
    }
}
